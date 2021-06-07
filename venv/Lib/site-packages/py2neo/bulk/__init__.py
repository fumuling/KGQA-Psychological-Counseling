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
This module contains facilities to carry out bulk data operations
such as creating or merging nodes and relationships.
"""


__all__ = [
    "create_nodes",
    "merge_nodes",
    "create_relationships",
    "merge_relationships",
]


from logging import getLogger

from py2neo.cypher.queries import (
    unwind_create_nodes_query,
    unwind_merge_nodes_query,
    unwind_create_relationships_query,
    unwind_merge_relationships_query,
)


log = getLogger(__name__)


def create_nodes(tx, data, labels=None, keys=None):
    """ Create nodes from an iterable sequence of raw node data.

    The raw node `data` is supplied as either a list of lists or a list
    of dictionaries. If the former, then a list of `keys` must also be
    provided in the same order as the values. This option will also
    generally require fewer bytes to be sent to the server, since key
    duplication is removed. An iterable of extra `labels` can also be
    supplied, which will be attached to all new nodes.

    The example code below shows how to pass raw node data as a list of
    lists:

        >>> from py2neo import Graph
        >>> from py2neo.bulk import create_nodes
        >>> g = Graph()
        >>> keys = ["name", "age"]
        >>> data = [
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
        ]
        >>> create_nodes(g.auto(), data, labels={"Person"}, keys=keys)
        >>> g.nodes.match("Person").count()
        3

    This second example shows how to pass raw node data as a list of
    dictionaries. This alternative can be particularly useful if the
    fields are not uniform across records.

        >>> data = [
            {"name": "Dave", "age": 66},
            {"name": "Eve", "date_of_birth": "1943-10-01"},
            {"name": "Frank"},
        ]
        >>> create_nodes(g.auto(), data, labels={"Person"})
        >>> g.nodes.match("Person").count()
        6

    There are obviously practical limits to the amount of data that
    should be included in a single bulk load of this type. For that
    reason, it is advisable to batch the input data into chunks, and
    carry out each in a separate transaction.

    The code below shows how batching can be achieved using a simple
    loop. This assumes that `data` is an iterable of raw node data
    (lists of values) and steps through that data in chunks of size
    `batch_size` until everything has been consumed.

        >>> from itertools import islice
        >>> stream = iter(data)
        >>> batch_size = 10000
        >>> while True:
        ...     batch = islice(stream, batch_size)
        ...     if batch:
        ...         create_nodes(g.auto(), batch, labels={"Person"})
        ...     else:
        ...         break

    There is no universal `batch_size` that performs optimally for all
    use cases. It is recommended to experiment with this value to
    discover what size works best.

    :param tx: :class:`.Transaction` in which to carry out this
        operation
    :param data: node data supplied as a list of lists (if `keys` are
        provided) or a list of dictionaries (if `keys` is :const:`None`)
    :param labels: labels to apply to the created nodes
    :param keys: optional set of keys for the supplied `data` (if
        supplied as value lists)
    """
    list(tx.run(*unwind_create_nodes_query(data, labels, keys)))


def merge_nodes(tx, data, merge_key, labels=None, keys=None):
    """ Merge nodes from an iterable sequence of raw node data.

    In a similar way to :func:`.create_nodes`, the raw node `data` can
    be supplied as either lists (with field `keys`) or as dictionaries.
    This method however uses an ``UNWIND ... MERGE`` construct in the
    underlying Cypher query to create or update nodes depending
    on what already exists.

    The merge is performed on the basis of the label and keys
    represented by the `merge_key`, updating a node if that combination
    is already present in the graph, and creating a new node otherwise.
    The value of this argument may take one of several forms and is
    used internally to construct an appropriate ``MERGE`` pattern. The
    table below gives examples of the values permitted, and how each is
    interpreted, using ``x`` as the input value from the source data.

    .. table::
        :widths: 40 60

        =================================================  ===========================================================
        Argument                                           ``MERGE`` Clause
        =================================================  ===========================================================
        ``("Person", "name")``                             ``MERGE (a:Person {name:x})``
        ``("Person", "name", "family name")``              ``MERGE (a:Person {name:x[0], `family name`:x[1]})``
        ``(("Person", "Female"), "name")``                 ``MERGE (a:Female:Person {name:x})``
        ``(("Person", "Female"), "name", "family name")``  ``MERGE (a:Female:Person {name:x[0], `family name`:x[1]})``
        =================================================  ===========================================================

    As with :func:`.create_nodes`, extra `labels` may also be
    specified; these will be applied to all nodes, pre-existing or new.
    The label included in the `merge_key` does not need to be
    separately included here.

    The example code below shows a simple merge based on a `Person`
    label and a `name` property:

        >>> from py2neo import Graph
        >>> from py2neo.bulk import merge_nodes
        >>> g = Graph()
        >>> keys = ["name", "age"]
        >>> data = [
            ["Alice", 33],
            ["Bob", 44],
            ["Carol", 55],
            ["Carol", 66],
            ["Alice", 77],
        ]
        >>> merge_nodes(g.auto(), data, ("Person", "name"), keys=keys)
        >>> g.nodes.match("Person").count()
        3

    :param tx: :class:`.Transaction` in which to carry out this
        operation
    :param data: node data supplied as a list of lists (if `keys` are
        provided) or a list of dictionaries (if `keys` is :const:`None`)
    :param merge_key: tuple of (label, key1, key2...) on which to merge
    :param labels: additional labels to apply to the merged nodes
    :param keys: optional set of keys for the supplied `data` (if
        supplied as value lists)
    """
    list(tx.run(*unwind_merge_nodes_query(data, merge_key, labels, keys)))


def create_relationships(tx, data, rel_type, start_node_key=None, end_node_key=None, keys=None):
    """ Create relationships from an iterable sequence of raw
    relationship data.

    The raw relationship `data` is supplied as a list of triples (or
    3-item lists), each representing (start_node, detail, end_node).
    The `rel_type` specifies the type of relationship to create, and is
    fixed for the entire data set.

    Start and end node information can either be provided as an
    internal node ID or, in conjunction with a `start_node_key` or
    `end_node_key`, a tuple or list of property values to ``MATCH``.
    For example, to link people to their place of work, the code below
    could be used:

        >>> from py2neo import Graph
        >>> from py2neo.bulk import create_relationships
        >>> g = Graph()
        >>> data = [
            (("Alice", "Smith"), {"since": 1999}, "ACME"),
            (("Bob", "Jones"), {"since": 2002}, "Bob Corp"),
            (("Carol", "Singer"), {"since": 1981}, "The Daily Planet"),
        ]
        >>> create_relationships(g.auto(), data, "WORKS_FOR", \\
            start_node_key=("Person", "name", "family name"), end_node_key=("Company", "name"))

    If the company node IDs were already known by other means, the code
    could instead look like this:

        >>> data = [
            (("Alice", "Smith"), {"since": 1999}, 123),
            (("Bob", "Jones"), {"since": 2002}, 124),
            (("Carol", "Singer"), {"since": 1981}, 201),
        ]
        >>> create_relationships(g.auto(), data, "WORKS_FOR", \\
            start_node_key=("Person", "name", "family name"))

    These `start_node_key` and `end_node_key` arguments are interpreted
    in a similar way to the `merge_key` of :func:`merge_nodes`, except
    that the values are instead used to construct ``MATCH`` patterns.
    Additionally, passing :py:const:`None` indicates that a match by
    node ID should be used. The table below shows example combinations,
    where ``x`` is the input value drawn from the source data.

    .. table::
        :widths: 40 60

        =================================================  ===========================================================
        Argument                                           ``MATCH`` Clause
        =================================================  ===========================================================
        :py:const:`None`                                   ``MATCH (a) WHERE id(a) = x``
        ``("Person", "name")``                             ``MATCH (a:Person {name:x})``
        ``("Person", "name", "family name")``              ``MATCH (a:Person {name:x[0], `family name`:x[1]})``
        ``(("Person", "Female"), "name")``                 ``MATCH (a:Female:Person {name:x})``
        ``(("Person", "Female"), "name", "family name")``  ``MATCH (a:Female:Person {name:x[0], `family name`:x[1]})``
        =================================================  ===========================================================


    As with other methods, such as :func:`.create_nodes`, the
    relationship `data` can also be supplied as a list of property
    values, indexed by `keys`. This can avoid sending duplicated key
    names over the network, and alters the method call as follows:

        >>> data = [
            (("Alice", "Smith"), [1999], 123),
            (("Bob", "Jones"), [2002], 124),
            (("Carol", "Singer"), [1981], 201),
        ]
        >>> create_relationships(g.auto(), data, "WORKS_FOR" \\
            start_node_key=("Person", "name", "family name")), keys=["since"])

    :param tx: :class:`.Transaction` in which to carry out this
        operation
    :param data: relationship data supplied as a list of triples of
        `(start_node, detail, end_node)`
    :param rel_type: relationship type name to create
    :param start_node_key: optional tuple of (label, key1, key2...) on
        which to match relationship start nodes, matching by node ID
        if not provided
    :param end_node_key: optional tuple of (label, key1, key2...) on
        which to match relationship end nodes, matching by node ID
        if not provided
    :param keys: optional set of field names for the relationship
        `detail` (if supplied as value lists)
    :return:
    """
    list(tx.run(*unwind_create_relationships_query(
        data, rel_type, start_node_key, end_node_key, keys)))


def merge_relationships(tx, data, merge_key, start_node_key=None, end_node_key=None, keys=None):
    """ Merge relationships from an iterable sequence of raw
    relationship data.

    The `merge_key` argument operates according to the the same general
    principle as its namesake in :func:`.merge_nodes`, but instead of a
    variable number of labels, exactly one relationship type must be
    specified. This allows for the following input options:

    .. table::
        :widths: 40 60

        =======================================  ============================================================
        Argument                                 ``MERGE`` Clause
        =======================================  ============================================================
        ``"KNOWS"``                              ``MERGE (a)-[ab:KNOWS]->(b)``
        ``("KNOWS",)``                           ``MERGE (a)-[ab:KNOWS]->(b)``
        ``("KNOWS", "since")``                   ``MERGE (a)-[ab:KNOWS {since:$x}]->(b)``
        ``("KNOWS", "since", "introduced by")``  ``MERGE (a)-[ab:KNOWS {since:$x, `introduced by`:$y}]->(b)``
        =======================================  ============================================================

    For details on how the `start_node_key` and `end_node_key`
    arguments can be used, see :func:`.create_relationships`.

    :param tx: :class:`.Transaction` in which to carry out this
        operation
    :param data: relationship data supplied as a list of triples of
        `(start_node, detail, end_node)`
    :param merge_key: tuple of (rel_type, key1, key2...) on which to
        merge
    :param start_node_key: optional tuple of (label, key1, key2...) on
        which to match relationship start nodes, matching by node ID
        if not provided
    :param end_node_key: optional tuple of (label, key1, key2...) on
        which to match relationship end nodes, matching by node ID
        if not provided
    :param keys: optional set of field names for the relationship
        `detail` (if supplied as value lists)
    :return:
    """
    list(tx.run(*unwind_merge_relationships_query(
        data, merge_key, start_node_key, end_node_key, keys)))
