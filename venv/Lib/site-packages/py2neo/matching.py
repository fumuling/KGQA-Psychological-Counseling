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


"""
The ``py2neo.matching`` module provides functionality to match nodes
and relationships according to certain criteria. For each entity type,
a ``Matcher`` class and a ``Match`` class are provided. The ``Matcher``
can be used to perform a basic selection, returning a ``Match`` that
itself can be evaluated or further refined.

The underlying query is only evaluated when the selection undergoes
iteration or when a specific evaluation method is called (such as
:meth:`.NodeMatch.first`). This means that a :class:`.NodeMatch`
object may be reused before and after data changes for different
results.
"""


__all__ = [

    "Predicate",
    "IsNull",
    "IsNotNull",
    "Predicate1",
    "EqualTo",
    "NotEqualTo",
    "LessThan",
    "LessThanOrEqualTo",
    "GreaterThan",
    "GreaterThanOrEqualTo",
    "StartsWith",
    "EndsWith",
    "Contains",
    "Like",
    "In",
    "Connective",
    "And",
    "Or",
    "EitherOr",

    "IS_NULL",
    "IS_NOT_NULL",
    "EQ", "EQUAL_TO",
    "NE", "NOT_EQUAL_TO",
    "LT", "LESS_THAN",
    "LE", "LESS_THAN_OR_EQUAL_TO",
    "GT", "GREATER_THAN",
    "GE", "GREATER_THAN_OR_EQUAL_TO",
    "STARTS_WITH",
    "ENDS_WITH",
    "CONTAINS",
    "LIKE",
    "IN",
    "AND",
    "OR",
    "XOR",

    "NodeMatch",
    "NodeMatcher",
    "RelationshipMatch",
    "RelationshipMatcher",

]


from py2neo.collections import is_collection
from py2neo.compat import Sequence, Set
from py2neo.cypher import cypher_escape, cypher_repr


class Predicate(object):

    @classmethod
    def cast(cls, value):
        if value is None:
            return IsNull()
        elif isinstance(value, Predicate):
            return value
        elif isinstance(value, (tuple, set, frozenset)):
            return In(value)
        else:
            return EqualTo(value)

    def compile(self, key, _):
        return "", {}


class IsNull(Predicate):
    """ Null value predicate.

    This is equivalent to the Cypher expression ``x IS NULL``.
    """

    def compile(self, key, _):
        return "_.%s IS NULL" % cypher_escape(key), {}


class IsNotNull(Predicate):
    """ Non-null value predicate.

    This is equivalent to the Cypher expression ``x IS NOT NULL``.
    """

    def compile(self, key, _):
        return "_.%s IS NOT NULL" % cypher_escape(key), {}


class Predicate1(Predicate):

    def __init__(self, value):
        self.value = value


class EqualTo(Predicate1):
    """ Equal value predicate.

    This is equivalent to the Cypher expression ``x = value``.
    """

    def compile(self, key, i):
        return "_.%s = $%s" % (cypher_escape(key), i), {"%s" % i: self.value}


class NotEqualTo(Predicate1):
    """ Unequal value predicate.

    This is equivalent to the Cypher expression ``x <> value``.
    """

    def compile(self, key, i):
        return "_.%s <> $%s" % (cypher_escape(key), i), {"%s" % i: self.value}


class LessThan(Predicate1):
    """ Lesser value predicate.

    This is equivalent to the Cypher expression ``x < value``.
    """

    def compile(self, key, i):
        return "_.%s < $%s" % (cypher_escape(key), i), {"%s" % i: self.value}


class LessThanOrEqualTo(Predicate1):
    """ Lesser or equal value predicate.

    This is equivalent to the Cypher expression ``x <= value``.
    """

    def compile(self, key, i):
        return "_.%s <= $%s" % (cypher_escape(key), i), {"%s" % i: self.value}


class GreaterThan(Predicate1):
    """ Greater value predicate.

    This is equivalent to the Cypher expression ``x > value``.
    """

    def compile(self, key, i):
        return "_.%s > $%s" % (cypher_escape(key), i), {"%s" % i: self.value}


class GreaterThanOrEqualTo(Predicate1):
    """ Greater or equal value predicate.

    This is equivalent to the Cypher expression ``x >= value``.
    """

    def compile(self, key, i):
        return "_.%s >= $%s" % (cypher_escape(key), i), {"%s" % i: self.value}


class StartsWith(Predicate1):
    """ String prefix predicate.

    This is equivalent to the Cypher expression ``s STARTS WITH value``.

        >>> nodes.match("Person", name=STARTS_WITH("Kevin")).all()
        [Node('Person', born=1958, name='Kevin Bacon'),
         Node('Person', born=1957, name='Kevin Pollak')]

    """

    def compile(self, key, i):
        return "_.%s STARTS WITH $%s" % (cypher_escape(key), i), {"%s" % i: self.value}


class EndsWith(Predicate1):
    """ String suffix predicate.

    This is equivalent to the Cypher expression ``s ENDS WITH value``.

        >>> nodes.match("Person", name=ENDS_WITH("Wachowski")).all()
        [Node('Person', born=1967, name='Andy Wachowski'),
         Node('Person', born=1965, name='Lana Wachowski')]
    """

    def compile(self, key, i):
        return "_.%s ENDS WITH $%s" % (cypher_escape(key), i), {"%s" % i: self.value}


class Contains(Predicate1):
    """ Substring predicate.

        >>> nodes.match("Person", name=CONTAINS("eve")).all()
        [Node('Person', born=1967, name='Steve Zahn'),
         Node('Person', born=1964, name='Keanu Reeves')]

    This is equivalent to the Cypher expression ``s CONTAINS value``.
    """

    def compile(self, key, i):
        return "_.%s CONTAINS $%s" % (cypher_escape(key), i), {"%s" % i: self.value}


class Like(Predicate1):
    """ Regular expression matching predicate.
    The `regex` can be a string or an pre-existing compiled
    Python ``re`` pattern.

        >>> nodes.match("Person", name=LIKE("Ke.*n")).all()
        [Node('Person', born=1958, name='Kevin Bacon'),
         Node('Person', born=1962, name='Kelly Preston')]

    This is equivalent to the Cypher expression ``s =~ regex``.
    """

    def __init__(self, regex):
        try:
            value = regex.pattern
        except AttributeError:
            value = regex
        super(Like, self).__init__(value)

    def compile(self, key, i):
        return "_.%s =~ $%s" % (cypher_escape(key), i), {"%s" % i: self.value}


class In(Predicate1):
    """ List membership predicate.

        >>> nodes.match("Person", born=IN([1962, 1964, 1966])).all()
        [Node('Person', born=1964, name='Keanu Reeves'),
         Node('Person', born=1962, name='Tom Cruise'),
         Node('Person', born=1962, name='Demi Moore'),
         Node('Person', born=1966, name='Kiefer Sutherland'),
         Node('Person', born=1962, name='Anthony Edwards'),
         Node('Person', born=1962, name='Kelly Preston'),
         Node('Person', born=1966, name='John Cusack'),
         Node('Person', born=1962, name="Rosie O'Donnell"),
         Node('Person', born=1966, name='Halle Berry'),
         Node('Person', born=1966, name='Matthew Fox')]

    This is equivalent to the Cypher expression ``x IN list``.
    """

    def compile(self, key, i):
        return "_.%s IN $%s" % (cypher_escape(key), i), {"%s" % i: list(self.value)}


class Connective(Predicate):

    def __init__(self, *values):
        self.values = tuple(map(Predicate.cast, values))


class And(Connective):
    """ Connective wherein all predicates must evaluate true.

        >>> nodes.match("Person", born=AND(GE(1964), LE(1966))).all()
        [Node('Person', born=1965, name='John C. Reilly'),
         Node('Person', born=1964, name='Keanu Reeves'),
         Node('Person', born=1965, name='Lana Wachowski'),
         Node('Person', born=1966, name='Kiefer Sutherland'),
         Node('Person', born=1966, name='John Cusack'),
         Node('Person', born=1966, name='Halle Berry'),
         Node('Person', born=1965, name='Tom Tykwer'),
         Node('Person', born=1966, name='Matthew Fox')]

    This is equivalent to the Cypher expression ``(pred1 AND pred2 AND ...)``.
    """

    def compile(self, key, i):
        predicates = []
        parameters = {}
        for j, value in enumerate(self.values, start=1):
            c, p = value.compile(key, "%s_%s" % (i, j))
            predicates.append(c)
            parameters.update(p)
        return "(%s)" % " AND ".join(predicates), parameters


class Or(Connective):
    """ Connective wherein at least one predicate must evaluate true.

        >>> nodes.match("Person", name=OR(STARTS_WITH("H"), ENDS_WITH("h"))).all()
        [Node('Person', born=1960, name='Hugo Weaving'),
         Node('Person', born=1943, name='J.T. Walsh'),
         Node('Person', born=1941, name='Jim Cash'),
         Node('Person', born=1963, name='Helen Hunt'),
         Node('Person', born=1950, name='Howard Deutch'),
         Node('Person', born=1966, name='Halle Berry'),
         Node('Person', born=1985, name='Emile Hirsch')]

    This is equivalent to the Cypher expression ``(pred1 OR pred2 OR ...)``.
    """

    def compile(self, key, i):
        predicates = []
        parameters = {}
        for j, value in enumerate(self.values, start=1):
            c, p = value.compile(key, "%s_%s" % (i, j))
            predicates.append(c)
            parameters.update(p)
        return "(%s)" % " OR ".join(predicates), parameters


class EitherOr(Connective):
    """ Connective wherein exactly one predicate must evaluate true.

        >>> nodes.match("Person", name=XOR(STARTS_WITH("H"), ENDS_WITH("h"))).all()
        [Node('Person', born=1960, name='Hugo Weaving'),
         Node('Person', born=1943, name='J.T. Walsh'),
         Node('Person', born=1941, name='Jim Cash'),
         Node('Person', born=1963, name='Helen Hunt'),
         Node('Person', born=1966, name='Halle Berry'),
         Node('Person', born=1985, name='Emile Hirsch')]

    This is equivalent to the Cypher expression ``(pred1 XOR pred2 XOR ...)``.
    """

    def compile(self, key, i):
        predicates = []
        parameters = {}
        for j, value in enumerate(self.values, start=1):
            c, p = value.compile(key, "%s_%s" % (i, j))
            predicates.append(c)
            parameters.update(p)
        return "(%s)" % " XOR ".join(predicates), parameters


IS_NULL = IsNull
IS_NOT_NULL = IsNotNull

EQ = EQUAL_TO = EqualTo
NE = NOT_EQUAL_TO = NotEqualTo

LT = LESS_THAN = LessThan
LE = LESS_THAN_OR_EQUAL_TO = LessThanOrEqualTo
GT = GREATER_THAN = GreaterThan
GE = GREATER_THAN_OR_EQUAL_TO = GreaterThanOrEqualTo

STARTS_WITH = StartsWith
ENDS_WITH = EndsWith
CONTAINS = Contains
LIKE = Like

IN = In

AND = And
OR = Or
XOR = EitherOr


def _property_predicates(properties, offset=1):
    for i, (key, value) in enumerate(properties.items(), start=offset):
        yield Predicate.cast(value).compile(key, i)


class NodeMatch(object):
    """ Immutable set of node selection criteria.

    .. describe:: iter(match)

        Iterate through all matching nodes.

    .. describe:: len(match)

        Return the number of nodes matched.

    """

    def __init__(self, graph, labels=frozenset(), predicates=tuple(), order_by=tuple(), skip=None, limit=None):
        self.graph = graph
        self._labels = frozenset(labels)
        self._predicates = tuple(predicates)
        self._order_by = tuple(order_by)
        self._skip = skip
        self._limit = limit

    def __len__(self):
        """ Return the number of nodes matched.
        """
        return self.graph.evaluate(*self._query_and_parameters(count=True))

    def __iter__(self):
        """ Iterate through all matching nodes.
        """
        for record in self.graph.run(*self._query_and_parameters()):
            yield record[0]

    def all(self):
        """ Evaluate the selection and return a list of all matched
        :class:`.Node` objects.

        :return: list of matching :class:`.Node` objects

        *New in version 2020.0.*
        """
        return list(self)

    def count(self):
        """ Evaluate the selection and return a count of the number
        of matches.

        :return: number of nodes matched

        *New in version 2020.0.*
        """
        return len(self)

    def exists(self):
        """ Evaluate the selection and return :py:const:`True` if at
        least one matched node exists.

        :return: boolean indicating presence or absence of a match

        *New in version 2020.0.*
        """
        return len(self) > 0

    def first(self):
        """ Evaluate the match and return the first :class:`.Node`
        matched or :const:`None` if no matching nodes are found.

        :return: a single matching :class:`.Node` or :const:`None`
        """
        return self.graph.evaluate(*self._query_and_parameters())

    def _query_and_parameters(self, count=False):
        """ A tuple of the Cypher query and parameters used to select
        the nodes that match the criteria for this selection.

        :return: Cypher query string
        """
        clauses = ["MATCH (_%s)" % "".join(":%s" % cypher_escape(label) for label in self._labels)]
        parameters = {}
        if self._predicates:
            predicates = []
            for predicate in self._predicates:
                if isinstance(predicate, tuple):
                    predicate, param = predicate
                    parameters.update(param)
                predicates.append(predicate)
            clauses.append("WHERE %s" % " AND ".join(predicates))
        if count:
            clauses.append("RETURN count(_)")
        else:
            clauses.append("RETURN _")
            if self._order_by:
                clauses.append("ORDER BY %s" % (", ".join(self._order_by)))
            if self._skip:
                clauses.append("SKIP %d" % self._skip)
            if self._limit is not None:
                clauses.append("LIMIT %d" % self._limit)
        return " ".join(clauses), parameters

    def where(self, *predicates, **properties):
        """ Refine this match to create a new match. The criteria specified
        for refining the match consist of predicates and properties.
        Conditions are individual Cypher expressions that would be found
        in a `WHERE` clause; properties are used as exact matches for
        property values.

        To refer to the current node within a predicate expression, use
        the underscore character ``_``. For example::

            match.where("_.name =~ 'J.*'")

        Simple property equalities can also be specified::

            match.where(born=1976)

        :param predicates: Cypher expressions to add to the `WHERE` clause
        :param properties: exact property match keys and values
        :return: refined :class:`.NodeMatch` object
        """
        return self.__class__(self.graph, self._labels,
                              self._predicates + predicates + tuple(_property_predicates(properties)),
                              self._order_by, self._skip, self._limit)

    def order_by(self, *fields):
        """ Order by the fields or field expressions specified.

        To refer to the current node within a field or field expression,
        use the underscore character ``_``. For example::

            match.order_by("_.name", "max(_.a, _.b)")

        :param fields: fields or field expressions to order by
        :return: refined :class:`.NodeMatch` object
        """
        return self.__class__(self.graph, self._labels, self._predicates,
                              fields, self._skip, self._limit)

    def skip(self, amount):
        """ Skip the first `amount` nodes in the result.

        :param amount: number of nodes to skip
        :return: refined :class:`.NodeMatch` object
        """
        return self.__class__(self.graph, self._labels, self._predicates,
                              self._order_by, amount, self._limit)

    def limit(self, amount):
        """ Limit to at most `amount` nodes.

        :param amount: maximum number of nodes to return
        :return: refined :class:`.NodeMatch` object
        """
        return self.__class__(self.graph, self._labels, self._predicates,
                              self._order_by, self._skip, amount)


class NodeMatcher(object):
    """ Matcher for selecting nodes.

    A :class:`.NodeMatcher` can be used to locate nodes that fulfil a
    specific set of criteria. Typically, a single node can be
    identified passing a specific label and property key-value pair.
    However, any number of labels and predicates supported by the
    Cypher `WHERE` clause are allowed.

    For a simple equality match by label and property::

        >>> from py2neo import Graph
        >>> from py2neo.matching import *
        >>> g = Graph()
        >>> nodes = NodeMatcher(g)
        >>> keanu = nodes.match("Person", name="Keanu Reeves").first()
        >>> keanu
        Node('Person', born=1964, name='Keanu Reeves')

    :param graph: :class:`.Graph` object on which to perform matches

    .. describe:: iter(matcher)

        Iterate through the matches, yielding the node ID for each one in turn.

    .. describe:: len(matcher)

        Count the matched nodes and return the number matched.

    .. describe:: node_id in matcher

        Determine whether a given node ID exists.

    .. describe:: matcher[node_id]

        Match and return a specific node by ID.
        This raises a :py:exc:`KeyError` if no such node can be found.

    """

    _match_class = NodeMatch

    def __init__(self, graph):
        self.graph = graph

    def __iter__(self):
        for node in self.match():
            yield node.identity

    def __len__(self):
        return len(self.match())

    def __contains__(self, identity):
        return self.match().where("id(_) = %d" % identity).exists()

    def __getitem__(self, identity):
        """ Return a node by identity.
        """
        entity = self.get(identity)
        if entity is None:
            raise KeyError("Node %d not found" % identity)
        return entity

    def get(self, identity):
        """ Create a new :class:`.NodeMatch` that filters by identity
        and returns the first matched :class:`.Node`. This can be used
        to match and return a :class:`.Node` by ID.

            >>> matches.get(1234)
            Node('Person', name='Alice')

        If no such :class:`.Node` is found, :py:const:`None` is
        returned instead. Contrast with ``matcher[1234]`` which raises
        a :py:exc:`KeyError` if no entity is found.
        """
        t = type(identity)
        if issubclass(t, (list, tuple, set, frozenset)):
            matches = self.match().where("id(_) in %s" % cypher_repr(list(identity)))
            return t(matches)
        else:
            return self.match().where("id(_) = %d" % identity).first()

    def match(self, *labels, **properties):
        """ Describe a basic node match using labels and property
        equality.

        :param labels: node labels to match
        :param properties: set of property keys and values to match
        :return: :class:`.NodeMatch` instance
        """
        criteria = {}
        if labels:
            criteria["labels"] = frozenset(labels)
        if properties:
            criteria["predicates"] = tuple(_property_predicates(properties))
        return self._match_class(self.graph, **criteria)


class RelationshipMatch(object):
    """ Immutable set of relationship selection criteria.

    .. describe:: iter(match)

        Iterate through all matching relationships.

    .. describe:: len(match)

        Return the number of relationships matched.
    """

    def __init__(self, graph, nodes=None, r_type=None,
                 predicates=tuple(), order_by=tuple(), skip=None, limit=None):
        if nodes is not None and not isinstance(nodes, (Sequence, Set)):
            raise ValueError("Nodes must be supplied as a Sequence or a Set")
        self.graph = graph
        self._nodes = nodes
        self._r_type = r_type
        self._predicates = tuple(predicates)
        self._order_by = tuple(order_by)
        self._skip = skip
        self._limit = limit

    def __len__(self):
        """ Return the number of relationships matched.
        """
        return self.graph.evaluate(*self._query_and_parameters(count=True))

    def __iter__(self):
        """ Iterate through all matching relationships.
        """
        query, parameters = self._query_and_parameters()
        for record in self.graph.run(query, parameters):
            yield record[0]

    def all(self):
        """ Evaluate the selection and return a list of all matched
        :class:`.Relationship` objects.

        :return: list of matching :class:`.Relationship` objects

        *New in version 2020.0.*
        """
        return list(self)

    def count(self):
        """ Evaluate the selection and return a count of the number
        of matches.

        :return: number of relationships matched

        *New in version 2020.0.*
        """
        return len(self)

    def exists(self):
        """ Evaluate the selection and return :py:const:`True` if at
        least one matched relationship exists.

        :return: boolean indicating presence or absence of a match

        *New in version 2020.0.*
        """
        return len(self) > 0

    def first(self):
        """ Evaluate the selection and return the first
        :class:`.Relationship` selected or :const:`None` if no matching
        relationships are found.

        :return: a single matching :class:`.Relationship` or :const:`None`
        """
        return self.graph.evaluate(*self._query_and_parameters())

    def _query_and_parameters(self, count=False):
        """ A tuple of the Cypher query and parameters used to select
        the relationships that match the criteria for this selection.

        :return: Cypher query string
        """

        def verify_node(n):
            if n.graph != self.graph:
                raise ValueError("Node %r does not belong to this graph" % n)
            if n.identity is None:
                raise ValueError("Node %r is not bound to a graph" % n)

        def r_type_name(r):
            try:
                return r.__name__
            except AttributeError:
                return r

        clauses = []
        parameters = {}
        if self._r_type is None:
            relationship_detail = ""
        elif is_collection(self._r_type):
            relationship_detail = ":" + "|".join(cypher_escape(r_type_name(t))
                                                 for t in self._r_type)
        else:
            relationship_detail = ":%s" % cypher_escape(r_type_name(self._r_type))
        if not self._nodes:
            clauses.append("MATCH (a)-[_" + relationship_detail + "]->(b)")
        elif isinstance(self._nodes, Sequence):
            if len(self._nodes) >= 1 and self._nodes[0] is not None:
                start_node = self._nodes[0]
                verify_node(start_node)
                clauses.append("MATCH (a) WHERE id(a) = $x")
                parameters["x"] = start_node.identity
            if len(self._nodes) >= 2 and self._nodes[1] is not None:
                end_node = self._nodes[1]
                verify_node(end_node)
                clauses.append("MATCH (b) WHERE id(b) = $y")
                parameters["y"] = end_node.identity
            if len(self._nodes) >= 3:
                raise ValueError("Node sequence cannot be longer than two")
            clauses.append("MATCH (a)-[_" + relationship_detail + "]->(b)")
        elif isinstance(self._nodes, Set):
            nodes = {node for node in self._nodes if node is not None}
            if len(nodes) >= 1:
                start_node = nodes.pop()
                verify_node(start_node)
                clauses.append("MATCH (a) WHERE id(a) = $x")
                parameters["x"] = start_node.identity
            if len(nodes) >= 1:
                end_node = nodes.pop()
                verify_node(end_node)
                clauses.append("MATCH (b) WHERE id(b) = $y")
                parameters["y"] = end_node.identity
            if len(nodes) >= 1:
                raise ValueError("Node set cannot be larger than two")
            clauses.append("MATCH (a)-[_" + relationship_detail + "]-(b)")
        else:
            raise ValueError("Nodes must be passed as a Sequence or a Set")
        if self._predicates:
            predicates = []
            for predicate in self._predicates:
                if isinstance(predicate, tuple):
                    predicate, param = predicate
                    parameters.update(param)
                predicates.append(predicate)
            clauses.append("WHERE %s" % " AND ".join(predicates))
        if count:
            clauses.append("RETURN count(_)")
        else:
            clauses.append("RETURN _")
            if self._order_by:
                clauses.append("ORDER BY %s" % (", ".join(self._order_by)))
            if self._skip:
                clauses.append("SKIP %d" % self._skip)
            if self._limit is not None:
                clauses.append("LIMIT %d" % self._limit)
        return " ".join(clauses), parameters

    def where(self, *predicates, **properties):
        """ Refine this match to create a new match. The criteria specified
        for refining the match consist of predicates and properties.
        Conditions are individual Cypher expressions that would be found
        in a `WHERE` clause; properties are used as exact matches for
        property values.

        To refer to the current relationship within a predicate expression,
        use the underscore character ``_``. For example::

            match.where("_.weight >= 30")

        Simple property equalities can also be specified::

            match.where(since=1999)

        :param predicates: Cypher expressions to add to the `WHERE` clause
        :param properties: exact property match keys and values
        :return: refined :class:`.RelationshipMatch` object
        """
        return self.__class__(self.graph,
                              nodes=self._nodes,
                              r_type=self._r_type,
                              predicates=self._predicates + predicates + tuple(_property_predicates(properties)),
                              order_by=self._order_by,
                              skip=self._skip,
                              limit=self._limit)

    def order_by(self, *fields):
        """ Order by the fields or field expressions specified.

        To refer to the current relationship within a field or field
        expression, use the underscore character ``_``. For example::

            match.order_by("_.weight", "max(_.a, _.b)")

        :param fields: fields or field expressions to order by
        :return: refined :class:`.RelationshipMatch` object
        """
        return self.__class__(self.graph,
                              nodes=self._nodes,
                              r_type=self._r_type,
                              predicates=self._predicates,
                              order_by=fields,
                              skip=self._skip,
                              limit=self._limit)

    def skip(self, amount):
        """ Skip the first `amount` relationships in the result.

        :param amount: number of relationships to skip
        :return: refined :class:`.RelationshipMatch` object
        """
        return self.__class__(self.graph,
                              nodes=self._nodes,
                              r_type=self._r_type,
                              predicates=self._predicates,
                              order_by=self._order_by,
                              skip=amount,
                              limit=self._limit)

    def limit(self, amount):
        """ Limit to at most `amount` relationships.

        :param amount: maximum number of relationships to return
        :return: refined :class:`.RelationshipMatch` object
        """
        return self.__class__(self.graph,
                              nodes=self._nodes,
                              r_type=self._r_type,
                              predicates=self._predicates,
                              order_by=self._order_by,
                              skip=self._skip,
                              limit=amount)


class RelationshipMatcher(object):
    """ Matcher for selecting relationships that fulfil a specific
    set of criteria.

    :param graph: :class:`.Graph` object on which to perform matches

    .. describe:: iter(matcher)

        Iterate through the matches, yielding the relationship ID for each one in turn.

    .. describe:: len(matcher)

        Count the matched relationships and return the number matched.

    .. describe:: relationship_id in matcher

        Determine whether a given relationship ID exists.

    .. describe:: matcher[relationship_id]

        Match and return a specific relationship by ID.
        This raises a :exc:`KeyError` if no such relationship can be found.

    """

    _match_class = RelationshipMatch

    def __init__(self, graph):
        self.graph = graph

    def __iter__(self):
        for relationship in self.match():
            yield relationship.identity

    def __len__(self):
        return len(self.match())

    def __contains__(self, identity):
        return self.match().where("id(_) = %d" % identity).exists()

    def __getitem__(self, identity):
        """ Return a relationship by identity.
        """
        entity = self.get(identity)
        if entity is None:
            raise KeyError("Relationship %d not found" % identity)
        return entity

    def get(self, identity):
        """ Create a new :class:`.RelationshipMatch` that filters by
        identity and returns the first matched :class:`.Relationship`.
        This can be used to match and return a :class:`.Relationship`
        by ID.

            >>> relationships.get(1234)
            Relationship(...)

        If no such :class:`.Relationship` is found, :py:const:`None` is
        returned instead. Contrast with `matcher[1234]` which raises a
        :py:exc:`KeyError` if no entity is found.
        """
        t = type(identity)
        if issubclass(t, (list, tuple, set, frozenset)):
            matches = self.match().where("id(_) in %s" % cypher_repr(list(identity)))
            return t(matches)
        else:
            return self.match().where("id(_) = %d" % identity).first()

    def match(self, nodes=None, r_type=None, **properties):
        """ Describe a basic relationship match using start and end
        nodes plus relationship type.

        :param nodes: Sequence or Set of start and end nodes (:const:`None` means any node);
                a Set implies a match in any direction
        :param r_type:
        :param properties: set of property keys and values to match
        :return: :class:`.RelationshipMatch` instance
        """
        criteria = {}
        if nodes is not None:
            criteria["nodes"] = nodes
        if r_type is not None:
            criteria["r_type"] = r_type
        if properties:
            criteria["predicates"] = tuple(_property_predicates(properties))
        return self._match_class(self.graph, **criteria)
