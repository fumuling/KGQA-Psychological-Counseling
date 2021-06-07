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


from __future__ import absolute_import, print_function


__all__ = [
    "GraphService",
    "Graph",
    "SystemGraph",
    "Schema",
    "Transaction",
]


from inspect import isgenerator
from time import sleep

from py2neo.compat import (deprecated,
                           Sequence,
                           Mapping)
from py2neo.cypher import Cursor, cypher_escape
from py2neo.cypher.proc import ProcedureLibrary
from py2neo.errors import (Neo4jError,
                           ConnectionUnavailable,
                           ConnectionBroken,
                           ConnectionLimit,
                           ServiceUnavailable,
                           WriteServiceUnavailable)
from py2neo.matching import NodeMatcher, RelationshipMatcher


class GraphService(object):
    """ The :class:`.GraphService` class is the top-level accessor for
    an entire Neo4j graph database management system (DBMS). Within the
    py2neo object hierarchy, a :class:`.GraphService` contains one or
    more :class:`.Graph` objects in which data storage and retrieval
    activity chiefly occurs.

    An explicit URI can be passed to the constructor::

        >>> from py2neo import GraphService
        >>> gs = GraphService("bolt://camelot.example.com:7687")

    Alternatively, the default value of ``bolt://localhost:7687`` is
    used::

        >>> default_gs = GraphService()
        >>> default_gs
        <GraphService uri='bolt://localhost:7687'>

    .. note::

        Some attributes of this class available in earlier versions of
        py2neo are no longer available, specifically
        ``kernel_start_time``, ``primitive_counts``,
        ``store_creation_time``, ``store_file_sizes`` and ``store_id``,
        along with the ``query_jmx`` method. This is due to a change in
        Neo4j 4.0 relating to how certain system metadata is exposed.
        Replacement functionality may be reintroduced in a future
        py2neo release.

    *Changed in 2020.0: this class was formerly known as 'Database',
    but was renamed to avoid confusion with the concept of the same
    name introduced with the multi-database feature of Neo4j 4.0.*

    .. describe:: iter(graph_service)

        Yield all named graphs.

        For Neo4j 4.0 and above, this yields the names returned by a
        ``SHOW DATABASES`` query. For earlier versions, this yields no
        entries, since the one and only graph in these versions is not
        named.

        *New in version 2020.0.*

    .. describe:: graph_service[name]

        Access a :class:`.Graph` by name.

        *New in version 2020.0.*

    """

    _connector = None

    _graphs = None

    def __init__(self, profile=None, **settings):
        from py2neo import ServiceProfile
        from py2neo.client import Connector
        connector_settings = {
            "user_agent": settings.pop("user_agent", None),
            "init_size": settings.pop("init_size", None),
            "max_size": settings.pop("max_size", None),
            "max_age": settings.pop("max_age", None),
            "routing_refresh_ttl": settings.pop("routing_refresh_ttl", None),
        }
        profile = ServiceProfile(profile, **settings)
        if connector_settings["init_size"] is None and not profile.routing:
            # Ensures credentials are checked on construction
            connector_settings["init_size"] = 1
        self._connector = Connector(profile, **connector_settings)
        self._graphs = {}

    def __repr__(self):
        class_name = self.__class__.__name__
        profile = self._connector.profile
        return "<%s uri=%r secure=%r user_agent=%r>" % (
            class_name, profile.uri, profile.secure, self._connector.user_agent)

    def __eq__(self, other):
        try:
            return self.uri == other.uri
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self._connector)

    def __getitem__(self, graph_name):
        if graph_name is None:
            graph_name = self._connector.default_graph_name()
        elif graph_name not in self._connector.graph_names():
            raise KeyError("Graph {!r} does not exist for "
                           "service {!r}".format(graph_name, self._connector.profile.uri))
        if graph_name not in self._graphs:
            graph_class = SystemGraph if graph_name == "system" else Graph
            self._graphs[graph_name] = graph_class(self.profile, name=graph_name)
        return self._graphs[graph_name]

    def __iter__(self):
        return iter(self._connector.graph_names())

    @property
    def connector(self):
        """ The :class:`.Connector` providing communication for this
        graph service.

        *New in version 2020.0.*
        """
        return self._connector

    @property
    def profile(self):
        """ The :class:`.ConnectionProfile` for which this graph
        service is configured. This attribute is simply a shortcut
        for ``connector.profile``.

        *New in version 2020.0.*
        """
        return self.connector.profile

    @property
    def uri(self):
        """ The URI to which this graph service is connected. This
        attribute is simply a shortcut for ``connector.profile.uri``.
        """
        return self.profile.uri

    @property
    def default_graph(self):
        """ The default :class:`.Graph` exposed by this graph service.
        """
        return self[None]

    @property
    def system_graph(self):
        """ The :class:`.SystemGraph` exposed by this graph service.

        *New in version 2020.0.*
        """
        return self["system"]

    def keys(self):
        """ Return a list of all :class:`.Graph` names exposed by this
        graph service.

        *New in version 2020.0.*
        """
        return list(self)

    @property
    def kernel_version(self):
        """ The :class:`~packaging.version.Version` of Neo4j running.
        """
        from packaging.version import Version
        components = self.default_graph.call("dbms.components").data()
        kernel_component = [component for component in components
                            if component["name"] == "Neo4j Kernel"][0]
        version_string = kernel_component["versions"][0]
        return Version(version_string)

    @property
    def product(self):
        """ The product name.
        """
        record = next(self.default_graph.call("dbms.components"))
        return "%s %s (%s)" % (record[0], " ".join(record[1]), record[2].title())

    @property
    def config(self):
        """ A dictionary of the configuration parameters used to
        configure Neo4j.

            >>> gs.config['dbms.connectors.default_advertised_address']
            'localhost'

        """
        return {record["name"]: record["value"]
                for record in self.default_graph.call("dbms.listConfig")}


class Graph(object):
    """ The `Graph` class provides a handle to an individual named
    graph database exposed by a Neo4j graph database service.

    Connection details are provided using either a URI or a
    :class:`.ConnectionProfile`, plus individual settings, if required.

    The `name` argument allows selection of a graph database by name.
    When working with Neo4j 4.0 and above, this can be any name defined
    in the system catalogue, a full list of which can be obtained
    through the Cypher ``SHOW DATABASES`` command. Passing `None` here
    will select the default database, as defined on the server. For
    earlier versions of Neo4j, the `name` must be set to `None`.

        >>> from py2neo import Graph
        >>> sales = Graph("bolt+s://g.example.com:7687", name="sales")
        >>> sales.run("MATCH (c:Customer) RETURN c.name")
         c.name
        ---------------
         John Smith
         Amy Pond
         Rory Williams

    The `system graph`, which is available in all 4.x+ product editions,
    can also be accessed via the :class:`.SystemGraph` class.

        >>> from py2neo import SystemGraph
        >>> sg = SystemGraph("bolt+s://g.example.com:7687")
        >>> sg.call("dbms.security.listUsers")
         username | roles | flags
        ----------|-------|-------
         neo4j    |  null | []

    In addition to the core `connection details <#getting-connected>`_
    that can be passed to the constructor, the :class:`.Graph` class
    can accept several other settings:

    ===================  ========================================================  ==============  =========================
    Keyword              Description                                               Type            Default
    ===================  ========================================================  ==============  =========================
    ``user_agent``       User agent to send for all connections                    str             `(depends on URI scheme)`
    ``max_connections``  The maximum number of simultaneous connections permitted  int             40
    ===================  ========================================================  ==============  =========================

    Once obtained, the `Graph` instance provides direct or indirect
    access to most of the functionality available within py2neo.
    """

    #: The :class:`.GraphService` to which this :class:`.Graph` belongs.
    service = None

    #: The :class:`.Schema` resource for this :class:`.Graph`.
    schema = None

    def __init__(self, profile=None, name=None, **settings):
        self.service = GraphService(profile, **settings)
        self.__name__ = name
        self.schema = Schema(self)
        self._procedures = ProcedureLibrary(self)

    def __repr__(self):
        if self.name is None:
            return "%s(%r)" % (self.__class__.__name__, self.service.uri)
        else:
            return "%s(%r, name=%r)" % (self.__class__.__name__, self.service.uri, self.name)

    def __eq__(self, other):
        try:
            return self.service == other.service and self.__name__ == other.__name__
        except (AttributeError, TypeError):
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __len__(self):
        return len(self.relationships)

    def __bool__(self):
        return True

    __nonzero__ = __bool__

    @property
    def name(self):
        """ The name of this graph.

        *New in version 2020.0.*
        """
        return self.__name__

    # TRANSACTION MANAGEMENT #

    def auto(self, readonly=False,
             # after=None, metadata=None, timeout=None
             ):
        """ Create a new auto-commit :class:`~py2neo.Transaction`.

        :param readonly: if :py:const:`True`, will begin a readonly
            transaction, otherwise will begin as read-write

        *New in version 2020.0.*
        """
        return Transaction(self, autocommit=True, readonly=readonly,
                           # after, metadata, timeout
                           )

    def begin(self, readonly=False,
              # after=None, metadata=None, timeout=None
              ):
        """ Begin a new :class:`~py2neo.Transaction`.

        :param readonly: if :py:const:`True`, will begin a readonly
            transaction, otherwise will begin as read-write

        *Changed in version 2021.1: the 'autocommit' argument has been
        removed. Use the 'auto' method instead.*
        """
        return Transaction(self, autocommit=False, readonly=readonly,
                           # after, metadata, timeout
                           )

    def commit(self, tx):
        """ Commit a transaction.

        *New in version 2021.1.*
        """
        if tx is None:
            return
        if not isinstance(tx, Transaction):
            raise TypeError("Bad transaction %r" % tx)
        if tx.closed:
            raise TypeError("Cannot commit closed transaction")
        try:
            summary = self.service.connector.commit(tx.ref)
            tx._bookmark = summary["bookmark"]
            tx._profile = summary["profile"]
            tx._time = summary["time"]
        finally:
            tx._closed = True

    def rollback(self, tx):
        """ Rollback a transaction.

        *New in version 2021.1.*
        """
        if tx is None or tx.closed:
            return
        if not isinstance(tx, Transaction):
            raise TypeError("Bad transaction %r" % tx)
        try:
            summary = self.service.connector.rollback(tx.ref)
            tx._bookmark = summary["bookmark"]
            tx._profile = summary["profile"]
            tx._time = summary["time"]
        except (ConnectionUnavailable, ConnectionBroken):
            pass
        finally:
            tx._closed = True

    # CYPHER EXECUTION #

    def run(self, cypher, parameters=None, **kwparameters):
        """ Run a single read/write query within an auto-commit
        :class:`~py2neo.Transaction`.

        :param cypher: Cypher statement
        :param parameters: dictionary of parameters
        :param kwparameters: extra parameters supplied as keyword
            arguments
        :return:
        """
        return self.auto().run(cypher, parameters, **kwparameters)

    def evaluate(self, cypher, parameters=None, **kwparameters):
        """ Run a :meth:`~py2neo.Transaction.evaluate` operation within an
        auto-commit :class:`~py2neo.Transaction`.

        :param cypher: Cypher statement
        :param parameters: dictionary of parameters
        :return: first value from the first record returned or
                 :py:const:`None`.
        """
        return self.run(cypher, parameters, **kwparameters).evaluate()

    def update(self, cypher, parameters=None, timeout=None):
        """ Call a function representing a transactional unit of work.

        The function must always accept a :class:`~py2neo.Transaction`
        object as its first argument. Additional arguments can be
        passed though the `args` and `kwargs` arguments of this method.

        The unit of work may be called multiple times if earlier
        attempts fail due to connectivity or other transient errors.
        As such, the function should have no non-idempotent side
        effects.

        :param cypher: cypher string or transaction function containing
            a unit of work
        :param parameters: cypher parameter map or function arguments
        :param timeout:
        :raises WriteServiceUnavailable: if the update does not
            successfully complete
        """
        if callable(cypher):
            if parameters is None:
                self._update(cypher, timeout=timeout)
            elif (isinstance(parameters, tuple) and len(parameters) == 2 and
                    isinstance(parameters[0], Sequence) and isinstance(parameters[1], Mapping)):
                self._update(lambda tx: cypher(tx, *parameters[0], **parameters[1]),
                             timeout=timeout)
            elif isinstance(parameters, Sequence):
                self._update(lambda tx: cypher(tx, *parameters), timeout=timeout)
            elif isinstance(parameters, Mapping):
                self._update(lambda tx: cypher(tx, **parameters), timeout=timeout)
            else:
                raise TypeError("Unrecognised parameter type")
        else:
            self._update(lambda tx: tx.update(cypher, parameters), timeout=timeout)

    def _update(self, f, timeout=None):
        from py2neo.timing import Timer
        # TODO: logging
        n = 0
        for _ in Timer.repeat(at_least=3, timeout=timeout):
            n += 1
            tx = None
            try:
                tx = self.begin(
                                # after=after, metadata=metadata, timeout=timeout
                                )
                value = f(tx)
                if isgenerator(value):
                    _ = list(value)     # exhaust the generator
                self.commit(tx)
            except (ConnectionUnavailable, ConnectionBroken, ConnectionLimit):
                self.rollback(tx)
                continue
            except Neo4jError as error:
                self.rollback(tx)
                if error.should_invalidate_routing_table():
                    self.service.connector.invalidate_routing_table(self.name)
                if error.should_retry():
                    continue
                else:
                    raise
            except Exception:
                self.rollback(tx)
                raise
            else:
                return
        raise WriteServiceUnavailable("Failed to execute update after %r tries" % n)

    def query(self, cypher, parameters=None, timeout=None):
        """ Run a single readonly query within an auto-commit
        :class:`~py2neo.Transaction`.

        :param cypher: Cypher statement
        :param parameters: dictionary of parameters
        :param timeout:
        :returns:
        :raises TypeError: if the underlying connection profile does not
            support readonly transactions
        :raises ServiceUnavailable: if the query does not successfully
            complete

        *Refactored from read to query in version 2021.1*
        """
        from py2neo.timing import Timer
        # TODO: logging
        n = 0
        for _ in Timer.repeat(at_least=3, timeout=timeout):
            n += 1
            try:
                result = self.auto(readonly=True).run(cypher, parameters)
            except (ConnectionUnavailable, ConnectionBroken, ConnectionLimit):
                continue
            except Neo4jError as error:
                if error.should_invalidate_routing_table():
                    self.service.connector.invalidate_routing_table(self.name)
                if error.should_retry():
                    continue
                else:
                    raise
            else:
                return result
        raise ServiceUnavailable("Failed to execute query after %r tries" % n)

    @property
    def call(self):
        """ Accessor for listing and calling procedures.

        This property contains a :class:`.ProcedureLibrary` object tied
        to this graph, which provides links to Cypher procedures in
        the underlying implementation.

        Calling a procedure requires only the regular Python function
        call syntax::

            >>> g = Graph()
            >>> g.call.dbms.components()
             name         | versions   | edition
            --------------|------------|-----------
             Neo4j Kernel | ['3.5.12'] | community

        The object returned from the call is a
        :class:`~py2neo.Cursor` object, identical to
        that obtained from running a normal Cypher query, and can
        therefore be consumed in a similar way.

        Procedure names can alternatively be supplied as a string::

            >>> g.call["dbms.components"]()
             name         | versions   | edition
            --------------|------------|-----------
             Neo4j Kernel | ['3.5.12'] | community

        Using :func:`dir` or :func:`iter` on the `call` attribute will
        yield a list of available procedure names.

        *New in version 2020.0.*
        """
        return self._procedures

    def delete_all(self):
        """ Delete all nodes and relationships from this :class:`.Graph`.

        .. warning::
            This method will permanently remove **all** nodes and relationships
            from the graph and cannot be undone.
        """
        self.run("MATCH (a) DETACH DELETE a")

    @deprecated("The graph.read(...) method is deprecated, "
                "use graph.query(...) instead")
    def read(self, cypher, parameters=None, **kwparameters):
        return self.query(cypher, dict(parameters or {}, **kwparameters))

    # SUBGRAPH OPERATIONS #

    def create(self, subgraph):
        """ Run a :meth:`~py2neo.Transaction.create` operation within a
        :class:`~py2neo.Transaction`.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph`
        """
        self.update(lambda tx: tx.create(subgraph))

    def delete(self, subgraph):
        """ Run a :meth:`~py2neo.Transaction.delete` operation within an
        auto-commit :class:`~py2neo.Transaction`. To delete only the
        relationships, use the :meth:`.separate` method.

        Note that only entities which are bound to corresponding
        remote entities though the ``graph`` and ``identity``
        attributes will trigger a deletion.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph` object
        """
        self.update(lambda tx: tx.delete(subgraph))

    def exists(self, subgraph):
        """ Run a :meth:`~py2neo.Transaction.exists` operation within an
        auto-commit :class:`~py2neo.Transaction`.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph` object
        :return:
        """
        return self.auto(readonly=True).exists(subgraph)

    def match(self, nodes=None, r_type=None, limit=None):
        """ Match and return all relationships with specific criteria.

        For example, to find all of Alice's friends::

            for rel in graph.match((alice, ), r_type="FRIEND"):
                print(rel.end_node["name"])

        :param nodes: Sequence or Set of start and end nodes (:const:`None` means any node);
                a Set implies a match in any direction
        :param r_type: type of relationships to match (:const:`None` means any type)
        :param limit: maximum number of relationships to match (:const:`None` means unlimited)
        """
        return RelationshipMatcher(self).match(nodes=nodes, r_type=r_type).limit(limit)

    def match_one(self, nodes=None, r_type=None):
        """ Match and return one relationship with specific criteria.

        :param nodes: Sequence or Set of start and end nodes (:const:`None` means any node);
                a Set implies a match in any direction
        :param r_type: type of relationships to match (:const:`None` means any type)
        """
        matches = self.match(nodes=nodes, r_type=r_type, limit=1)
        rels = list(matches)
        if rels:
            return rels[0]
        else:
            return None

    def merge(self, subgraph, label=None, *property_keys):
        """ Run a :meth:`~py2neo.Transaction.merge` operation within an
        auto-commit :class:`~py2neo.Transaction`.

        The example code below shows a simple merge for a new relationship
        between two new nodes:

            >>> from py2neo import Graph, Node, Relationship
            >>> g = Graph()
            >>> a = Node("Person", name="Alice", age=33)
            >>> b = Node("Person", name="Bob", age=44)
            >>> KNOWS = Relationship.type("KNOWS")
            >>> g.merge(KNOWS(a, b), "Person", "name")

        Following on, we then create a third node (of a different type) to
        which both the original nodes connect:

            >>> c = Node("Company", name="ACME")
            >>> c.__primarylabel__ = "Company"
            >>> c.__primarykey__ = "name"
            >>> WORKS_FOR = Relationship.type("WORKS_FOR")
            >>> g.merge(WORKS_FOR(a, c) | WORKS_FOR(b, c))

        For details of how the merge algorithm works, see the
        :meth:`~py2neo.Transaction.merge` method. Note that this is different
        to a Cypher MERGE.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph` object
        :param label: label on which to match any existing nodes
        :param property_keys: property keys on which to match any existing nodes
        """
        self.update(lambda tx: tx.merge(subgraph, label, *property_keys))

    @property
    def nodes(self):
        """ A :class:`.NodeMatcher` for this graph.

        This can be used to find nodes that match given criteria:

            >>> graph = Graph()
            >>> graph.nodes[1234]
            (_1234:Person {name: 'Alice'})
            >>> graph.nodes.get(1234)
            (_1234:Person {name: 'Alice'})
            >>> graph.nodes.match("Person", name="Alice").first()
            (_1234:Person {name: 'Alice'})

        Nodes can also be efficiently counted using this attribute:

            >>> len(graph.nodes)
            55691
            >>> len(graph.nodes.match("Person", age=33))
            12

        """
        return NodeMatcher(self)

    def pull(self, subgraph):
        """ Pull data to one or more entities from their remote counterparts.

        :param subgraph: the collection of nodes and relationships to pull
        """
        self.update(lambda tx: tx.pull(subgraph))

    def push(self, subgraph):
        """ Push data from one or more entities to their remote counterparts.

        :param subgraph: the collection of nodes and relationships to push
        """
        self.update(lambda tx: tx.push(subgraph))

    @property
    def relationships(self):
        """ A :class:`.RelationshipMatcher` for this graph.

        This can be used to find relationships that match given criteria
        as well as efficiently count relationships.
        """
        return RelationshipMatcher(self)

    def separate(self, subgraph):
        """ Run a :meth:`~py2neo.Transaction.separate`
        operation within an auto-commit :class:`~py2neo.Transaction`.

        Note that only relationships which are bound to corresponding
        remote relationships though the ``graph`` and ``identity``
        attributes will trigger a deletion.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph`
        """
        self.update(lambda tx: tx.separate(subgraph))


class SystemGraph(Graph):
    """ A subclass of :class:`.Graph` that provides access to the
    system database for the remote DBMS. This is only available in
    Neo4j 4.0 and above.

    *New in version 2020.0.*
    """

    def __init__(self, profile=None, **settings):
        settings["name"] = "system"
        super(SystemGraph, self).__init__(profile, **settings)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.service.uri)


class Schema(object):
    """ The schema resource attached to a `Graph` instance.
    """

    def __init__(self, graph):
        self.graph = graph

    @property
    def node_labels(self):
        """ The set of node labels currently defined within the graph.
        """
        return frozenset(record[0] for record in
                         self.graph.run("CALL db.labels"))

    @property
    def relationship_types(self):
        """ The set of relationship types currently defined within the graph.
        """
        return frozenset(record[0] for record in
                         self.graph.run("CALL db.relationshipTypes"))

    def create_index(self, label, *property_keys):
        """ Create a schema index for a label and property
        key combination.
        """
        cypher = "CREATE INDEX ON :{}({})".format(
            cypher_escape(label), ", ".join(map(cypher_escape, property_keys)))
        self.graph.update(cypher)
        while property_keys not in self.get_indexes(label):
            sleep(0.1)

    def create_uniqueness_constraint(self, label, property_key):
        """ Create a node uniqueness constraint for a given label and property
        key.

        While indexes support the use of composite keys, unique constraints may
        only be tied to a single property key.
        """
        cypher = "CREATE CONSTRAINT ON (_:{}) ASSERT _.{} IS UNIQUE".format(
            cypher_escape(label), cypher_escape(property_key))
        self.graph.update(cypher)
        while property_key not in self.get_uniqueness_constraints(label):
            sleep(0.1)

    def drop_index(self, label, *property_keys):
        """ Remove label index for a given property key.
        """
        cypher = "DROP INDEX ON :{}({})".format(
            cypher_escape(label), ", ".join(map(cypher_escape, property_keys)))
        self.graph.update(cypher)

    def drop_uniqueness_constraint(self, label, property_key):
        """ Remove the node uniqueness constraint for a given label and
        property key.
        """
        cypher = "DROP CONSTRAINT ON (_:{}) ASSERT _.{} IS UNIQUE".format(
            cypher_escape(label), cypher_escape(property_key))
        self.graph.update(cypher)

    def _get_indexes(self, label, unique_only=False):
        indexes = []
        result = self.graph.run("CALL db.indexes")
        for record in result:
            properties = []
            # The code branches here depending on the format of the response
            # from the `db.indexes` procedure, which has varied enormously
            # since 3.0.
            if len(record) == 10:
                if "labelsOrTypes" in result.keys():
                    # 4.0.0
                    # ['id', 'name', 'state', 'populationPercent',
                    # 'uniqueness', 'type', 'entityType', 'labelsOrTypes',
                    #  'properties', 'provider']
                    (id_, name, state, population_percent, uniqueness, type_,
                     entity_type, token_names, properties, provider) = record
                    description = None
                    # The 'type' field has randomly changed its meaning in 4.0,
                    # holding for example 'BTREE' instead of for example
                    # 'node_unique_property'. To check for uniqueness, we now
                    # need to look at the new 'uniqueness' field.
                    is_unique = uniqueness == "UNIQUE"
                else:
                    # 3.5.3
                    # ['description', 'indexName', 'tokenNames', 'properties',
                    #  'state', 'type', 'progress', 'provider', 'id',
                    #  'failureMessage']
                    (description, index_name, token_names, properties, state,
                     type_, progress, provider, id_, failure_message) = record
                    is_unique = type_ == "node_unique_property"
            elif len(record) == 7:
                # 3.4.10
                (description, lbl, properties, state,
                 type_, provider, failure_message) = record
                is_unique = type_ == "node_unique_property"
                token_names = [lbl]
            elif len(record) == 6:
                # 3.4.7
                description, lbl, properties, state, type_, provider = record
                is_unique = type_ == "node_unique_property"
                token_names = [lbl]
            elif len(record) == 3:
                # 3.0.10
                description, state, type_ = record
                is_unique = type_ == "node_unique_property"
                token_names = []
            else:
                raise RuntimeError("Unexpected response from procedure "
                                   "db.indexes (%d fields)" % len(record))
            if state not in (u"ONLINE", u"online"):
                continue
            if unique_only and not is_unique:
                continue
            if not token_names or not properties:
                if description:
                    from py2neo.cypher.lexer import CypherLexer
                    from pygments.token import Token
                    tokens = list(CypherLexer().get_tokens(description))
                    for token_type, token_value in tokens:
                        if token_type is Token.Name.Label:
                            token_names.append(token_value.strip("`"))
                        elif token_type is Token.Name.Variable:
                            properties.append(token_value.strip("`"))
            if not token_names or not properties:
                continue
            if label in token_names:
                indexes.append(tuple(properties))
        return indexes

    def get_indexes(self, label):
        """ Fetch a list of indexed property keys for a label.
        """
        return self._get_indexes(label)

    def get_uniqueness_constraints(self, label):
        """ Fetch a list of unique constraints for a label. Each constraint is
        the name of a single property key.
        """
        return [k[0] for k in self._get_indexes(label, unique_only=True)]


class Transaction(object):
    """ Logical context for one or more graph operations.

    Transaction objects are typically constructed by the
    :meth:`.Graph.auto` and :meth:`.Graph.begin` methods.
    Likewise, the :meth:`.Graph.commit` and :meth:`.Graph.rollback`
    methods can be used to finish a transaction.
    """

    def __init__(self, graph, autocommit=False, readonly=False,
                 # after=None, metadata=None, timeout=None
                 ):
        self._graph = graph
        self._autocommit = autocommit
        self._connector = self.graph.service.connector
        if autocommit:
            self._ref = None
        else:
            self._ref = self._connector.begin(self.graph.name, readonly=readonly,
                                              # after, metadata, timeout
                                              )
        self._readonly = readonly
        self._closed = False
        self._bookmark = None
        self._profile = None
        self._time = None

    @property
    def graph(self):
        """ Graph to which this transaction belongs.
        """
        return self._graph

    @property
    def ref(self):
        """ Transaction reference.
        """
        return self._ref

    @property
    def readonly(self):
        """ :py:const:`True` if this is a readonly transaction,
        :py:const:`False` otherwise.
        """
        return self._readonly

    @property
    def closed(self):
        """ :py:const:`True` if this transaction is closed,
        :py:const:`False` otherwise.
        """
        return self._closed

    @property
    def bookmark(self):
        """ The closing bookmark for this transaction, populated
        on commit.
        """
        return self._bookmark

    @property
    def profile(self):
        """ The connection profile under which this transaction was
        carried out.
        """
        return self._profile

    @property
    def time(self):
        """ The total time taken to carry out this transaction
        """
        return self._time

    def run(self, cypher, parameters=None, **kwparameters):
        """ Send a Cypher query to the server for execution and return
        a :py:class:`~.cypher.Cursor` for navigating its result.

        :param cypher: Cypher query
        :param parameters: dictionary of parameters
        :returns: :py:class:`~.cypher.Cursor` object
        """
        from py2neo.client import Connection

        if self.closed:
            raise TypeError("Cannot run query in closed transaction")

        try:
            hydrant = Connection.default_hydrant(self._connector.profile, self.graph)
            parameters = dict(parameters or {}, **kwparameters)
            if self.ref:
                result = self._connector.run(self.ref, cypher, parameters)
            else:
                result = self._connector.auto_run(cypher, parameters,
                                                  graph_name=self.graph.name,
                                                  readonly=self.readonly)
            return Cursor(result, hydrant)
        finally:
            if not self.ref:
                self._closed = True

    def evaluate(self, cypher, parameters=None, **kwparameters):
        """ Execute a single Cypher query and return the value from
        the first column of the first record.

        :param cypher: Cypher statement
        :param parameters: dictionary of parameters
        :returns: single return value or :const:`None`
        """
        return self.run(cypher, parameters, **kwparameters).evaluate(0)

    def update(self, cypher, parameters=None, **kwparameters):
        """ Execute a single Cypher statement and discard any result
        returned.

        :param cypher: Cypher statement
        :param parameters: dictionary of parameters
        """
        self.run(cypher, parameters, **kwparameters)

    @deprecated("The transaction.commit() method is deprecated, "
                "use graph.commit(transaction) instead")
    def commit(self):
        """ Commit the transaction.
        """
        return self.graph.commit(self)

    @deprecated("The transaction.rollback() method is deprecated, "
                "use graph.rollback(transaction) instead")
    def rollback(self):
        """ Roll back the current transaction, undoing all actions
        previously taken.
        """
        return self.graph.rollback(self)

    def create(self, subgraph):
        """ Create remote nodes and relationships that correspond to those in a
        local subgraph. Any entities in *subgraph* that are already bound to
        remote entities will remain unchanged, those which are not will become
        bound to their newly-created counterparts.

        For example::

            >>> from py2neo import Graph, Node, Relationship
            >>> g = Graph()
            >>> tx = g.begin()
            >>> a = Node("Person", name="Alice")
            >>> tx.create(a)
            >>> b = Node("Person", name="Bob")
            >>> ab = Relationship(a, "KNOWS", b)
            >>> tx.create(ab)
            >>> tx.commit()
            >>> g.exists(ab)
            True

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                    creatable object
        """
        if self._autocommit:
            raise TypeError("Create operations are not supported inside "
                            "auto-commit transactions")
        try:
            create = subgraph.__db_create__
        except AttributeError:
            raise TypeError("No method defined to create object %r" % subgraph)
        else:
            create(self)

    def delete(self, subgraph):
        """ Delete the remote nodes and relationships that correspond to
        those in a local subgraph. To delete only the relationships, use
        the :meth:`.separate` method.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph`
        """
        try:
            delete = subgraph.__db_delete__
        except AttributeError:
            raise TypeError("No method defined to delete object %r" % subgraph)
        else:
            delete(self)

    def exists(self, subgraph):
        """ Determine whether one or more entities all exist within the
        graph. Note that if any nodes or relationships in *subgraph* are not
        bound to remote counterparts, this method will return ``False``.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph`
        :returns: ``True`` if all entities exist remotely, ``False`` otherwise
        """
        try:
            exists = subgraph.__db_exists__
        except AttributeError:
            raise TypeError("No method defined to check existence of object %r" % subgraph)
        else:
            return exists(self)

    def merge(self, subgraph, primary_label=None, primary_key=None):
        """ Create or update the nodes and relationships of a local
        subgraph in the remote database. Note that the functionality of
        this operation is not strictly identical to the Cypher MERGE
        clause, although there is some overlap.

        Each node and relationship in the local subgraph is merged
        independently, with nodes merged first and relationships merged
        second.

        For each node, the merge is carried out by comparing that node with
        a potential remote equivalent on the basis of a single label and
        property value. If no remote match is found, a new node is created;
        if a match is found, the labels and properties of the remote node
        are updated. The label and property used for comparison are determined
        by the `primary_label` and `primary_key` arguments but may be
        overridden for individual nodes by the of `__primarylabel__` and
        `__primarykey__` attributes on the node itself.

        For each relationship, the merge is carried out by comparing that
        relationship with a potential remote equivalent on the basis of matching
        start and end nodes plus relationship type. If no remote match is found,
        a new relationship is created; if a match is found, the properties of
        the remote relationship are updated.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph` object
        :param primary_label: label on which to match any existing nodes
        :param primary_key: property key(s) on which to match any existing
                            nodes
        """
        try:
            merge = subgraph.__db_merge__
        except AttributeError:
            raise TypeError("No method defined to merge object %r" % subgraph)
        else:
            merge(self, primary_label, primary_key)

    def pull(self, subgraph):
        """ Update local entities from their remote counterparts.

        For any nodes and relationships that exist in both the local
        :class:`.Subgraph` and the remote :class:`.Graph`, pull properties
        and node labels into the local copies. This operation does not
        create or delete any entities.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph`
        """
        try:
            pull = subgraph.__db_pull__
        except AttributeError:
            raise TypeError("No method defined to pull object %r" % subgraph)
        else:
            return pull(self)

    def push(self, subgraph):
        """ Update remote entities from their local counterparts.

        For any nodes and relationships that exist in both the local
        :class:`.Subgraph` and the remote :class:`.Graph`, push properties
        and node labels into the remote copies. This operation does not
        create or delete any entities.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph`
        """
        try:
            push = subgraph.__db_push__
        except AttributeError:
            raise TypeError("No method defined to push object %r" % subgraph)
        else:
            return push(self)

    def separate(self, subgraph):
        """ Delete the remote relationships that correspond to those in a local
        subgraph. This leaves any nodes untouched.

        :param subgraph: a :class:`.Node`, :class:`.Relationship` or other
                       :class:`.Subgraph`
        """
        try:
            separate = subgraph.__db_separate__
        except AttributeError:
            raise TypeError("No method defined to separate object %r" % subgraph)
        else:
            separate(self)
