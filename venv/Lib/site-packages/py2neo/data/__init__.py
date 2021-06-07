#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2021, Nigel Small
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import absolute_import


__all__ = [
    "Subgraph",
    "Walkable",
    "Entity",
    "Node",
    "Relationship",
    "Path",
    "walk",
    "UniquenessError",
]


from collections import OrderedDict
from itertools import chain
from uuid import uuid4

from py2neo.collections import SetView, PropertyDict
from py2neo.compat import string_types, ustr, xstr
from py2neo.cypher import cypher_escape, cypher_repr, cypher_join
from py2neo.cypher.encoding import CypherEncoder, LabelSetView
from py2neo.cypher.queries import (
    unwind_create_nodes_query,
    unwind_merge_nodes_query,
    unwind_merge_relationships_query,
)


class Subgraph(object):
    """ A :class:`.Subgraph` is an arbitrary collection of nodes and
    relationships. It is also the base class for :class:`.Node`,
    :class:`.Relationship` and :class:`.Path`.

    By definition, a subgraph must contain at least one node;
    `null subgraphs <http://mathworld.wolfram.com/NullGraph.html>`_
    should be represented by :const:`None`. To test for
    `emptiness <http://mathworld.wolfram.com/EmptyGraph.html>`_ the
    built-in :func:`bool` function can be used.

    The simplest way to construct a subgraph is by combining nodes and
    relationships using standard set operations. For example::

        >>> s = ab | ac
        >>> s
        {(alice:Person {name:"Alice"}),
         (bob:Person {name:"Bob"}),
         (carol:Person {name:"Carol"}),
         (Alice)-[:KNOWS]->(Bob),
         (Alice)-[:WORKS_WITH]->(Carol)}
        >>> s.nodes()
        frozenset({(alice:Person {name:"Alice"}),
                   (bob:Person {name:"Bob"}),
                   (carol:Person {name:"Carol"})})
        >>> s.relationships()
        frozenset({(Alice)-[:KNOWS]->(Bob),
                   (Alice)-[:WORKS_WITH]->(Carol)})

    .. describe:: subgraph | other | ...

        Union.
        Return a new subgraph containing all nodes and relationships from *subgraph* as well as all those from *other*.
        Any entities common to both will only be included once.

    .. describe:: subgraph & other & ...

        Intersection.
        Return a new subgraph containing all nodes and relationships common to both *subgraph* and *other*.

    .. describe:: subgraph - other - ...

        Difference.
        Return a new subgraph containing all nodes and relationships that exist in *subgraph* but do not exist in *other*,
        as well as all nodes that are connected by the relationships in *subgraph* regardless of whether or not they exist in *other*.

    .. describe:: subgraph ^ other ^ ...

        Symmetric difference.
        Return a new subgraph containing all nodes and relationships that exist in *subgraph* or *other*, but not in both,
        as well as all nodes that are connected by those relationships regardless of whether or not they are common to *subgraph* and *other*.

    """

    def __init__(self, nodes=None, relationships=None):
        self.__nodes = frozenset(nodes or [])
        self.__relationships = frozenset(relationships or [])
        self.__nodes |= frozenset(chain.from_iterable(r.nodes for r in self.__relationships))
        if not self.__nodes:
            raise ValueError("Subgraphs must contain at least one node")

    def __repr__(self):
        return "Subgraph({%s}, {%s})" % (", ".join(map(repr, self.nodes)),
                                         ", ".join(map(repr, self.relationships)))

    def __eq__(self, other):
        try:
            return self.nodes == other.nodes and self.relationships == other.relationships
        except (AttributeError, TypeError):
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        value = 0
        for entity in self.__nodes:
            value ^= hash(entity)
        for entity in self.__relationships:
            value ^= hash(entity)
        return value

    def __len__(self):
        return len(self.__relationships)

    def __iter__(self):
        return iter(self.__relationships)

    def __bool__(self):
        return bool(self.__relationships)

    def __nonzero__(self):
        return bool(self.__relationships)

    def __or__(self, other):
        return Subgraph(set(self.nodes) | set(other.nodes), set(self.relationships) | set(other.relationships))

    def __and__(self, other):
        return Subgraph(set(self.nodes) & set(other.nodes), set(self.relationships) & set(other.relationships))

    def __sub__(self, other):
        r = set(self.relationships) - set(other.relationships)
        n = (set(self.nodes) - set(other.nodes)) | set().union(*(set(rel.nodes) for rel in r))
        return Subgraph(n, r)

    def __xor__(self, other):
        r = set(self.relationships) ^ set(other.relationships)
        n = (set(self.nodes) ^ set(other.nodes)) | set().union(*(set(rel.nodes) for rel in r))
        return Subgraph(n, r)

    def __db_create__(self, tx):
        """ Create new data in a remote :class:`.Graph` from this
        :class:`.Subgraph`.

        :param tx:
        """
        graph = tx.graph

        # Convert nodes into a dictionary of
        #   {frozenset(labels): [Node, Node, ...]}
        node_dict = {}
        for node in self.nodes:
            if node.graph is None:
                key = frozenset(node.labels)
                node_dict.setdefault(key, []).append(node)

        # Convert relationships into a dictionary of
        #   {rel_type: [Rel, Rel, ...]}
        rel_dict = {}
        for relationship in self.relationships:
            if relationship.graph is None:
                key = type(relationship).__name__
                rel_dict.setdefault(key, []).append(relationship)

        for labels, nodes in node_dict.items():
            pq = unwind_create_nodes_query(list(map(dict, nodes)), labels=labels)
            pq = cypher_join(pq, "RETURN id(_)")
            records = tx.run(*pq)
            for i, record in enumerate(records):
                node = nodes[i]
                node.graph = graph
                node.identity = record[0]
                node._remote_labels = labels
        for r_type, relationships in rel_dict.items():
            data = map(lambda r: [r.start_node.identity, dict(r), r.end_node.identity],
                       relationships)
            pq = unwind_merge_relationships_query(data, r_type)
            pq = cypher_join(pq, "RETURN id(_)")
            for i, record in enumerate(tx.run(*pq)):
                relationship = relationships[i]
                relationship.graph = graph
                relationship.identity = record[0]

    def __db_delete__(self, tx):
        """ Delete data in a remote :class:`.Graph` based on this
        :class:`.Subgraph`.

        :param tx:
        """
        graph = tx.graph
        node_identities = []
        for relationship in self.relationships:
            if relationship.graph is graph:
                relationship.graph = None
                relationship.identity = None
        for node in self.nodes:
            if node.graph is graph:
                node_identities.append(node.identity)
                node.graph = None
                node.identity = None
        # TODO: this might delete remote relationships that aren't
        #  represented in the local subgraph - is this OK?
        list(tx.run("MATCH (_) WHERE id(_) IN $x DETACH DELETE _", x=node_identities))

    def __db_exists__(self, tx):
        """ Determine whether one or more graph entities all exist
        within the database. Note that if any nodes or relationships in
        this :class:`.Subgraph` are not bound to remote counterparts,
        this method will return ``False``.

        :param tx:
        :returns: ``True`` if all entities exist remotely, ``False``
            otherwise
        """
        graph = tx.graph
        node_ids = set()
        relationship_ids = set()
        for i, node in enumerate(self.nodes):
            if node.graph is graph:
                node_ids.add(node.identity)
            else:
                return False
        for i, relationship in enumerate(self.relationships):
            if relationship.graph is graph:
                relationship_ids.add(relationship.identity)
            else:
                return False
        statement = ("OPTIONAL MATCH (a) WHERE id(a) IN $x "
                     "OPTIONAL MATCH ()-[r]->() WHERE id(r) IN $y "
                     "RETURN count(DISTINCT a) + count(DISTINCT r)")
        parameters = {"x": list(node_ids), "y": list(relationship_ids)}
        return tx.evaluate(statement, parameters) == len(node_ids) + len(relationship_ids)

    def __db_merge__(self, tx, primary_label=None, primary_key=None):
        """ Merge data into a remote :class:`.Graph` from this
        :class:`.Subgraph`.

        :param tx:
        :param primary_label:
        :param primary_key:
        """
        graph = tx.graph

        # Convert nodes into a dictionary of
        #   {(p_label, p_key, frozenset(labels)): [Node, Node, ...]}
        node_dict = {}
        for node in self.nodes:
            if node.graph is None:
                p_label = getattr(node, "__primarylabel__", None) or primary_label
                p_key = getattr(node, "__primarykey__", None) or primary_key
                key = (p_label, p_key, frozenset(node.labels))
                node_dict.setdefault(key, []).append(node)

        # Convert relationships into a dictionary of
        #   {rel_type: [Rel, Rel, ...]}
        rel_dict = {}
        for relationship in self.relationships:
            if relationship.graph is None:
                key = type(relationship).__name__
                rel_dict.setdefault(key, []).append(relationship)

        for (pl, pk, labels), nodes in node_dict.items():
            if pl is None or pk is None:
                raise ValueError("Primary label and primary key are required for MERGE operation")
            pq = unwind_merge_nodes_query(map(dict, nodes), (pl, pk), labels)
            pq = cypher_join(pq, "RETURN id(_)")
            identities = [record[0] for record in tx.run(*pq)]
            if len(identities) > len(nodes):
                raise UniquenessError("Found %d matching nodes for primary label %r and primary "
                                      "key %r with labels %r but merging requires no more than "
                                      "one" % (len(identities), pl, pk, set(labels)))
            for i, identity in enumerate(identities):
                node = nodes[i]
                node.graph = graph
                node.identity = identity
                node._remote_labels = labels
        for r_type, relationships in rel_dict.items():
            data = map(lambda r: [r.start_node.identity, dict(r), r.end_node.identity],
                       relationships)
            pq = unwind_merge_relationships_query(data, r_type)
            pq = cypher_join(pq, "RETURN id(_)")
            for i, record in enumerate(tx.run(*pq)):
                relationship = relationships[i]
                relationship.graph = graph
                relationship.identity = record[0]

    def __db_pull__(self, tx):
        """ Copy data from a remote :class:`.Graph` into this
        :class:`.Subgraph`.

        :param tx:
        """
        # Pull nodes
        nodes = {}
        for node in self.nodes:
            if node.graph != tx.graph:
                raise ValueError("Node %r does not belong to graph %r" % (node, tx.graph))
            nodes[node.identity] = node
        query = tx.run("MATCH (_) WHERE id(_) in $x "
                       "RETURN id(_), labels(_), properties(_)", x=list(nodes.keys()))
        for identity, new_labels, new_properties in query:
            node = nodes[identity]
            node.clear_labels()
            node.update_labels(new_labels)
            node.clear()
            node.update(new_properties)
        # Pull relationships
        relationships = {}
        for relationship in self.relationships:
            if relationship.graph != tx.graph:
                raise ValueError(
                    "Relationship %r does not belong to graph %r" % (relationship, tx.graph))
            relationships[relationship.identity] = relationship
        query = tx.run("MATCH ()-[_]->() WHERE id(_) in $x "
                       "RETURN id(_), properties(_)", x=list(relationships.keys()))
        for identity, new_properties in query:
            relationship = relationships[identity]
            relationship.clear()
            relationship.update(new_properties)

    def __db_push__(self, tx):
        """ Copy data into a remote :class:`.Graph` from this
        :class:`.Subgraph`.

        :param tx:
        """
        graph = tx.graph
        for node in self.nodes:
            if node.graph is graph:
                clauses = ["MATCH (_) WHERE id(_) = $x", "SET _ = $y"]
                parameters = {"x": node.identity, "y": dict(node)}
                old_labels = node._remote_labels - node._labels
                if old_labels:
                    clauses.append("REMOVE _:%s" % ":".join(map(cypher_escape, old_labels)))
                new_labels = node._labels - node._remote_labels
                if new_labels:
                    clauses.append("SET _:%s" % ":".join(map(cypher_escape, new_labels)))
                tx.run("\n".join(clauses), parameters)
        for relationship in self.relationships:
            if relationship.graph is graph:
                clauses = ["MATCH ()-[_]->() WHERE id(_) = $x", "SET _ = $y"]
                parameters = {"x": relationship.identity, "y": dict(relationship)}
                tx.run("\n".join(clauses), parameters)

    def __db_separate__(self, tx):
        """ Delete relationships in a remote :class:`.Graph` based on
        those present in this :class:`.Subgraph`.

        :param tx:
        :return:
        """
        graph = tx.graph
        relationship_identities = []
        for relationship in self.relationships:
            if relationship.graph is graph:
                relationship_identities.append(relationship.identity)
                relationship.graph = None
                relationship.identity = None
        list(tx.run("MATCH ()-[_]->() WHERE id(_) IN $x DELETE _", x=relationship_identities))

    @property
    def graph(self):
        assert self.__nodes     # assume there is at least one node
        return set(self.__nodes).pop().graph

    @property
    def nodes(self):
        """ The set of all nodes in this subgraph.
        """
        return SetView(self.__nodes)

    @property
    def relationships(self):
        """ The set of all relationships in this subgraph.
        """
        return SetView(self.__relationships)

    def labels(self):
        """ Return the set of all node labels in this subgraph.

        *Changed in version 2020.0: this is now a method rather than a
        property, as in previous versions.*
        """
        return frozenset(chain.from_iterable(node.labels for node in self.__nodes))

    def types(self):
        """ Return the set of all relationship types in this subgraph.
        """
        return frozenset(type(rel).__name__ for rel in self.__relationships)

    def keys(self):
        """ Return the set of all property keys used by the nodes and
        relationships in this subgraph.
        """
        return (frozenset(chain.from_iterable(node.keys() for node in self.__nodes)) |
                frozenset(chain.from_iterable(rel.keys() for rel in self.__relationships)))


class Walkable(Subgraph):
    """ A subgraph with added traversal information.
    """

    def __init__(self, iterable):
        self.__sequence = tuple(iterable)
        nodes = self.__sequence[0::2]
        for node in nodes:
            _ = node.labels  # ensure not stale
        Subgraph.__init__(self, nodes, self.__sequence[1::2])

    def __repr__(self):
        return "%s(subgraph=%s, sequence=%r)" % (self.__class__.__name__,
                                                 Subgraph.__repr__(self),
                                                 self.__sequence)

    def __eq__(self, other):
        try:
            other_walk = tuple(walk(other))
        except TypeError:
            return False
        else:
            return tuple(walk(self)) == other_walk

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        value = 0
        for item in self.__sequence:
            value ^= hash(item)
        return value

    def __len__(self):
        return (len(self.__sequence) - 1) // 2

    def __getitem__(self, index):
        if isinstance(index, slice):
            start, stop = index.start, index.stop
            if start is not None:
                if start < 0:
                    start += len(self)
                start *= 2
            if stop is not None:
                if stop < 0:
                    stop += len(self)
                stop = 2 * stop + 1
            return Path(*self.__sequence[start:stop])
        elif index < 0:
            return self.__sequence[2 * index]
        else:
            return self.__sequence[2 * index + 1]

    def __iter__(self):
        for relationship in self.__sequence[1::2]:
            yield relationship

    def __add__(self, other):
        if other is None:
            return self
        return Path(*walk(self, other))

    def __walk__(self):
        """ Traverse and yield all nodes and relationships in this
        object in order.
        """
        return iter(self.__sequence)

    @property
    def start_node(self):
        """ The first node encountered on a :func:`.walk` of this object.
        """
        return self.__sequence[0]

    @property
    def end_node(self):
        """ The last node encountered on a :func:`.walk` of this object.
        """
        return self.__sequence[-1]

    @property
    def nodes(self):
        """ The sequence of nodes over which a :func:`.walk` of this
        object will traverse.
        """
        return self.__sequence[0::2]

    @property
    def relationships(self):
        """ The sequence of relationships over which a :func:`.walk`
        of this object will traverse.
        """
        return self.__sequence[1::2]


class Entity(PropertyDict, Walkable):
    """ Base class for objects that can be optionally bound to a remote resource. This
    class is essentially a container for a :class:`.Resource` instance.
    """

    _graph = None
    identity = None

    @classmethod
    def ref(cls, graph, identity):
        raise NotImplementedError

    def __init__(self, iterable, properties):
        Walkable.__init__(self, iterable)
        PropertyDict.__init__(self, properties)
        uuid = str(uuid4())
        while "0" <= uuid[-7] <= "9":
            uuid = str(uuid4())
        self.__uuid__ = uuid
        self._stale = set()

    def __bool__(self):
        return len(self) > 0

    def __nonzero__(self):
        return len(self) > 0

    @property
    def __name__(self):
        name = None
        if name is None and "__name__" in self:
            name = self["__name__"]
        if name is None and "name" in self:
            name = self["name"]
        if name is None and self.identity is not None:
            name = u"_" + ustr(self.identity)
        return name or u""

    def __or__(self, other):
        # Python 3.9 added the | and |= operators to the dict
        # class (PEP584). This broke Entity union operations by
        # picking up the __or__ handler in PropertyDict before
        # the one in Walkable. The hack below forces Entity to
        # use the Walkable implementation.
        return Walkable.__or__(self, other)

    @property
    def graph(self):
        return self._graph

    @graph.setter
    def graph(self, value):
        self._graph = value

    def clear(self):
        self._stale.discard("properties")
        super(Entity, self).clear()


class Node(Entity):
    """ A node is a fundamental unit of data storage within a property
    graph that may optionally be connected, via relationships, to
    other nodes.

    Node objects can either be created implicitly, by returning nodes
    in a Cypher query such as ``CREATE (a) RETURN a``, or can be
    created explicitly through the constructor. In the former case, the
    local Node object is *bound* to the remote node in the database; in
    the latter case, the Node object remains unbound until
    :meth:`created <.Transaction.create>` or
    :meth:`merged <.Transaction.merge>` into a Neo4j database.

    It possible to combine nodes (along with relationships and other
    graph data objects) into :class:`.Subgraph` objects using set
    operations. For more details, look at the documentation for the
    :class:`.Subgraph` class.

    All positional arguments passed to the constructor are interpreted
    as labels and all keyword arguments as properties::

        >>> from py2neo import Node
        >>> a = Node("Person", name="Alice")

    """

    @classmethod
    def ref(cls, graph, identity):
        obj = cls()
        obj.graph = graph
        obj.identity = identity
        obj._stale.add("labels")
        obj._stale.add("properties")
        return obj

    def __init__(self, *labels, **properties):
        self._remote_labels = frozenset()
        self._labels = set(labels)
        Entity.__init__(self, (self,), properties)

    def __repr__(self):
        args = list(map(repr, sorted(self.labels)))
        kwargs = OrderedDict()
        d = dict(self)
        for key in sorted(d):
            if CypherEncoder.is_safe_key(key):
                args.append("%s=%r" % (key, d[key]))
            else:
                kwargs[key] = d[key]
        if kwargs:
            args.append("**{%s}" % ", ".join("%r: %r" % (k, kwargs[k]) for k in kwargs))
        return "Node(%s)" % ", ".join(args)

    def __str__(self):
        return xstr(cypher_repr(self))

    def __eq__(self, other):
        if self is other:
            return True
        try:
            if any(x is None for x in [self.graph, other.graph, self.identity, other.identity]):
                return False
            return (issubclass(type(self), Node) and issubclass(type(other), Node) and
                    self.graph == other.graph and self.identity == other.identity)
        except (AttributeError, TypeError):
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        if self.graph and self.identity:
            return hash(self.graph.service) ^ hash(self.graph.name) ^ hash(self.identity)
        else:
            return hash(id(self))

    def __getitem__(self, item):
        if self.graph is not None and self.identity is not None and "properties" in self._stale:
            self.graph.pull(self)
        return Entity.__getitem__(self, item)

    def __ensure_labels(self):
        if self.graph is not None and self.identity is not None and "labels" in self._stale:
            self.graph.pull(self)

    def keys(self):
        if self.graph is not None and self.identity is not None and "properties" in self._stale:
            self.graph.pull(self)
        return Entity.keys(self)

    @property
    def labels(self):
        """ The full set of labels associated with with this *node*.

        This set is immutable and cannot be used to add or remove
        labels. Use methods such as :meth:`.add_label` and
        :meth:`.remove_label` for that instead.
        """
        self.__ensure_labels()
        return LabelSetView(self._labels)

    def has_label(self, label):
        """ Return :const:`True` if this node has the label `label`,
        :const:`False` otherwise.
        """
        self.__ensure_labels()
        return label in self._labels

    def add_label(self, label):
        """ Add the label `label` to this node.
        """
        self.__ensure_labels()
        self._labels.add(label)

    def remove_label(self, label):
        """ Remove the label `label` from this node, if it exists.
        """
        self.__ensure_labels()
        self._labels.discard(label)

    def clear_labels(self):
        """ Remove all labels from this node.
        """
        self._stale.discard("labels")
        self._labels.clear()

    def update_labels(self, labels):
        """ Add multiple labels to this node from the iterable
        `labels`.
        """
        self.__ensure_labels()
        self._labels.update(labels)


class Relationship(Entity):
    """ A relationship represents a typed connection between a pair of nodes.

    The positional arguments passed to the constructor identify the nodes to
    relate and the type of the relationship. Keyword arguments describe the
    properties of the relationship::

        >>> from py2neo import Node, Relationship
        >>> a = Node("Person", name="Alice")
        >>> b = Node("Person", name="Bob")
        >>> a_knows_b = Relationship(a, "KNOWS", b, since=1999)

    This class may be extended to allow relationship types names to be
    derived from the class name. For example::

        >>> WORKS_WITH = Relationship.type("WORKS_WITH")
        >>> a_works_with_b = WORKS_WITH(a, b)
        >>> a_works_with_b
        (Alice)-[:WORKS_WITH {}]->(Bob)

    """

    @staticmethod
    def type(name):
        """ Return the :class:`.Relationship` subclass corresponding to a
        given name.

        :param name: relationship type name
        :returns: `type` object

        Example::

            >>> KNOWS = Relationship.type("KNOWS")
            >>> KNOWS(a, b)
            KNOWS(Node('Person', name='Alice'), Node('Person', name='Bob')

        """
        for s in Relationship.__subclasses__():
            if s.__name__ == name:
                return s
        return type(xstr(name), (Relationship,), {})

    @classmethod
    def ref(cls, graph, identity, *nodes):
        obj = cls(*nodes)
        obj.graph = graph
        obj.identity = identity
        obj._stale.add("properties")
        return obj

    def __init__(self, *nodes, **properties):
        n = []
        for value in nodes:
            if value is None:
                n.append(None)
            elif isinstance(value, string_types):
                n.append(value)
            elif isinstance(value, Node):
                n.append(value)
            else:
                raise TypeError("Unknown node type for %r" % value)

        num_args = len(n)
        if num_args == 0:
            raise TypeError("Relationships must specify at least one endpoint")
        elif num_args == 1:
            # Relationship(a)
            n = (n[0], n[0])
        elif num_args == 2:
            if n[1] is None or isinstance(n[1], string_types):
                # Relationship(a, "TO")
                self.__class__ = Relationship.type(n[1])
                n = (n[0], n[0])
            else:
                # Relationship(a, b)
                n = (n[0], n[1])
        elif num_args == 3:
            # Relationship(a, "TO", b)
            self.__class__ = Relationship.type(n[1])
            n = (n[0], n[2])
        else:
            raise TypeError("Hyperedges not supported")
        Entity.__init__(self, (n[0], self, n[1]), properties)

    def __repr__(self):
        args = [repr(self.nodes[0]), repr(self.nodes[-1])]
        kwargs = OrderedDict()
        d = dict(self)
        for key in sorted(d):
            if CypherEncoder.is_safe_key(key):
                args.append("%s=%r" % (key, d[key]))
            else:
                kwargs[key] = d[key]
        if kwargs:
            args.append("**{%s}" % ", ".join("%r: %r" % (k, kwargs[k]) for k in kwargs))
        return "%s(%s)" % (self.__class__.__name__, ", ".join(args))

    def __str__(self):
        return xstr(cypher_repr(self))

    def __eq__(self, other):
        if self is other:
            return True
        try:
            if any(x is None for x in [self.graph, other.graph, self.identity, other.identity]):
                try:
                    return type(self) is type(other) and list(self.nodes) == list(other.nodes) and dict(self) == dict(other)
                except (AttributeError, TypeError):
                    return False
            return issubclass(type(self), Relationship) and issubclass(type(other), Relationship) and self.graph == other.graph and self.identity == other.identity
        except (AttributeError, TypeError):
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.nodes) ^ hash(type(self))


class Path(Walkable):
    """ A path represents a walk through a graph, starting on a node
    and visiting alternating relationships and nodes thereafter.
    Paths have a "overlaid" direction separate to that of the
    relationships they contain, and the nodes and relationships
    themselves may each be visited multiple times, in any order,
    within the same path.

    Paths can be returned from Cypher queries or can be constructed
    locally via the constructor or by using the addition operator.

    The `entities` provided to the constructor are walked in order to
    build up the new path. This is only possible if the end node of
    each entity is the same as either the start node or the end node
    of the next entity; in the latter case, the second entity will be
    walked in reverse. Nodes that overlap from one argument onto
    another are not duplicated.

        >>> from py2neo import Node, Path
        >>> alice, bob, carol = Node(name="Alice"), Node(name="Bob"), Node(name="Carol")
        >>> abc = Path(alice, "KNOWS", bob, Relationship(carol, "KNOWS", bob), carol)
        >>> abc
        <Path order=3 size=2>
        >>> abc.nodes
        (<Node labels=set() properties={'name': 'Alice'}>,
         <Node labels=set() properties={'name': 'Bob'}>,
         <Node labels=set() properties={'name': 'Carol'}>)
        >>> abc.relationships
        (<Relationship type='KNOWS' properties={}>,
         <Relationship type='KNOWS' properties={}>)
        >>> dave, eve = Node(name="Dave"), Node(name="Eve")
        >>> de = Path(dave, "KNOWS", eve)
        >>> de
        <Path order=2 size=1>
        >>> abcde = Path(abc, "KNOWS", de)
        >>> abcde
        <Path order=5 size=4>
        >>> for relationship in abcde.relationships:
        ...     print(relationship)
        ({name:"Alice"})-[:KNOWS]->({name:"Bob"})
        ({name:"Carol"})-[:KNOWS]->({name:"Bob"})
        ({name:"Carol"})-[:KNOWS]->({name:"Dave"})
        ({name:"Dave"})-[:KNOWS]->({name:"Eve"})

    """

    @classmethod
    def hydrate(cls, graph, nodes, u_rels, sequence):
        last_node = nodes[0]
        steps = [last_node]
        for i, rel_index in enumerate(sequence[::2]):
            next_node = nodes[sequence[2 * i + 1]]
            if rel_index > 0:
                u_rel = u_rels[rel_index - 1]
                start_node = Node.ref(graph, last_node.identity)
                end_node = Node.ref(graph, next_node.identity)
            else:
                u_rel = u_rels[-rel_index - 1]
                start_node = Node.ref(graph, next_node.identity)
                end_node = Node.ref(graph, last_node.identity)
            rel = Relationship.ref(graph, u_rel.id, start_node, u_rel.type, end_node)
            rel.clear()
            rel.update(u_rel.properties)
            steps.append(rel)
            last_node = next_node
        return cls(*steps)

    def __init__(self, *entities):
        entities = list(entities)
        for i, entity in enumerate(entities):
            if isinstance(entity, Entity):
                continue
            elif entity is None:
                entities[i] = Node()
            elif isinstance(entity, dict):
                entities[i] = Node(**entity)
        for i, entity in enumerate(entities):
            try:
                start_node = entities[i - 1].end_node
                end_node = entities[i + 1].start_node
            except (IndexError, AttributeError):
                pass
            else:
                if isinstance(entity, string_types):
                    entities[i] = Relationship(start_node, entity, end_node)
                elif isinstance(entity, tuple) and len(entity) == 2:
                    t, properties = entity
                    entities[i] = Relationship(start_node, t, end_node, **properties)
        Walkable.__init__(self, walk(*entities))

    def __str__(self):
        return xstr(cypher_repr(self))

    def __repr__(self):
        entities = [self.start_node] + list(self.relationships)
        return "Path(%s)" % ", ".join(map(repr, entities))

    @staticmethod
    def walk(*walkables):
        """ Traverse over the arguments supplied, in order, yielding
        alternating :class:`.Node` and :class:`.Relationship` objects.
        Any node or relationship may be traversed one or more times in
        any direction.

        :arg walkables: sequence of walkable objects
        """
        if not walkables:
            return
        walkable = walkables[0]
        try:
            entities = walkable.__walk__()
        except AttributeError:
            raise TypeError("Object %r is not walkable" % walkable)
        for entity in entities:
            yield entity
        end_node = walkable.end_node
        for walkable in walkables[1:]:
            try:
                if end_node == walkable.start_node:
                    entities = walkable.__walk__()
                    end_node = walkable.end_node
                elif end_node == walkable.end_node:
                    entities = reversed(list(walkable.__walk__()))
                    end_node = walkable.start_node
                else:
                    raise ValueError("Cannot append walkable %r "
                                     "to node %r" % (walkable, end_node))
            except AttributeError:
                raise TypeError("Object %r is not walkable" % walkable)
            for i, entity in enumerate(entities):
                if i > 0:
                    yield entity


walk = Path.walk


# TODO: find a better home for this class
class UniquenessError(Exception):
    """ Raised when a condition assumed to be unique is determined
    non-unique.
    """
