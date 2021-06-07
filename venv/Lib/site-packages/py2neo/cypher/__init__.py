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


__all__ = [
    "Cursor",
    "Record",
    "CypherExpression",
    "cypher_escape",
    "cypher_join",
    "cypher_repr",
    "cypher_str",
]

from functools import reduce
from operator import xor as xor_operator

from py2neo.cypher.encoding import CypherEncoder
from py2neo.compat import Mapping, string_types, unicode_types, ustr


class Cursor(object):
    """ A `Cursor` is a navigator for a stream of records.

    A cursor can be thought of as a window onto an underlying data
    stream. All cursors in py2neo are "forward-only", meaning that
    navigation starts before the first record and may proceed only in a
    forward direction.

    It is not generally necessary for application code to instantiate a
    cursor directly as one will be returned by any Cypher execution method.
    However, cursor creation requires only a :class:`.DataSource` object
    which contains the logic for how to access the source data that the
    cursor navigates.

    Many simple cursor use cases require only the :meth:`.forward` method
    and the :attr:`.current` attribute. To navigate through all available
    records, a `while` loop can be used::

        while cursor.forward():
            print(cursor.current["name"])

    If only the first record is of interest, a similar `if` structure will
    do the job::

        if cursor.forward():
            print(cursor.current["name"])

    To combine `forward` and `current` into a single step, use the built-in
    py:func:`next` function::

        print(next(cursor)["name"])

    Cursors are also iterable, so can be used in a loop::

        for record in cursor:
            print(record["name"])

    For queries that are expected to return only a single value within a
    single record, use the :meth:`.evaluate` method. This will return the
    first value from the next record or :py:const:`None` if neither the
    field nor the record are present::

        print(cursor.evaluate())

    """

    def __init__(self, result, hydrant=None):
        self._result = result
        self._fields = self._result.fields()
        self._hydrant = hydrant
        self._current = None

    def __repr__(self):
        preview = self.preview(3)
        if preview:
            return repr(preview)
        else:
            return "(No data)"

    def __next__(self):
        if self.forward():
            return self._current
        else:
            raise StopIteration()

    # Exists only for Python 2 iteration compatibility
    next = __next__

    def __iter__(self):
        while self.forward():
            yield self._current

    def __getitem__(self, key):
        return self._current[key]

    @property
    def profile(self):
        return self._result.profile

    @property
    def current(self):
        """ Returns the current record or :py:const:`None` if no record
        has yet been selected.
        """
        return self._current

    @property
    def closed(self):
        return self._result.offline

    def keys(self):
        """ Return the field names for the records in the stream.
        """
        return self._fields

    def summary(self):
        """ Return the result summary.
        """
        return self._result.summary()

    def plan(self):
        """ Return the execution plan returned by this query, if any.
        """
        metadata = self._result.summary()
        try:
            return metadata["plan"]
        except KeyError:
            try:
                return metadata["profile"]
            except KeyError:
                return None

    def stats(self):
        """ Return the execution statistics for this query.

        This contains details of the activity undertaken by the database
        kernel for the query, such as the number of entities created or
        deleted.

        >>> from py2neo import Graph
        >>> g = Graph()
        >>> g.run("CREATE (a:Person) SET a.name = 'Alice'").stats()
        {'labels_added': 1, 'nodes_created': 1, 'properties_set': 1}

        """
        metadata = self._result.summary()
        stats = {}
        for key, value in metadata.get("stats", {}).items():
            key = key.replace("-", "_")
            if key.startswith("relationship_"):
                # hack for server bug
                key = "relationships_" + key[13:]
            stats[key] = value
        return stats

    def forward(self, amount=1):
        """ Attempt to move the cursor one position forward (or by
        another amount if explicitly specified). The cursor will move
        position by up to, but never more than, the amount specified.
        If not enough scope for movement remains, only that remainder
        will be consumed. The total amount moved is returned.

        :param amount: the amount to move the cursor
        :returns: the amount that the cursor was able to move
        """
        if amount == 0:
            return 0
        if amount < 0:
            raise ValueError("Cursor can only move forwards")
        amount = int(amount)
        moved = 0
        while moved != amount:
            values = self._result.take()
            if values is None:
                break
            if self._hydrant:
                values = self._hydrant.hydrate_list(values)
            self._current = Record(self._fields, values)
            moved += 1
        return moved

    def preview(self, limit=1):
        """ Construct a :class:`.Table` containing a preview of
        upcoming records, including no more than the given `limit`.

        :param limit: maximum number of records to include in the
            preview
        :returns: :class:`.Table` containing the previewed records
        """
        from py2neo.export import Table
        if limit < 0:
            raise ValueError("Illegal preview size")
        records = []
        if self._fields:
            for values in self._result.peek(int(limit)):
                if self._hydrant:
                    values = self._hydrant.hydrate_list(values)
                records.append(values)
            return Table(records, self._fields)
        else:
            return None

    def evaluate(self, field=0):
        """ Return the value of the first field from the next record
        (or the value of another field if explicitly specified).

        This method attempts to move the cursor one step forward and,
        if successful, selects and returns an individual value from
        the new current record. By default, this value will be taken
        from the first value in that record but this can be overridden
        with the `field` argument, which can represent either a
        positional index or a textual key.

        If the cursor cannot be moved forward or if the record contains
        no values, :py:const:`None` will be returned instead.

        This method is particularly useful when it is known that a
        Cypher query returns only a single value.

        :param field: field to select value from (optional)
        :returns: value of the field or :py:const:`None`

        Example:
            >>> from py2neo import Graph
            >>> g = Graph()
            >>> g.run("MATCH (a) WHERE a.email=$x RETURN a.name", x="bob@acme.com").evaluate()
            'Bob Robertson'
        """
        if self.forward():
            try:
                return self[field]
            except IndexError:
                return None
        else:
            return None

    def data(self, *keys):
        """ Consume and extract the entire result as a list of
        dictionaries.

        ::

            >>> from py2neo import Graph
            >>> graph = Graph()
            >>> graph.run("MATCH (a:Person) RETURN a.name, a.born LIMIT 4").data()
            [{'a.born': 1964, 'a.name': 'Keanu Reeves'},
             {'a.born': 1967, 'a.name': 'Carrie-Anne Moss'},
             {'a.born': 1961, 'a.name': 'Laurence Fishburne'},
             {'a.born': 1960, 'a.name': 'Hugo Weaving'}]

        :param keys: indexes or keys of the items to include; if none
                     are provided, all values will be included
        :returns: list of dictionary of values, keyed by field name
        :raises IndexError: if an out-of-bounds index is specified
        """
        return [record.data(*keys) for record in self]

    def to_table(self):
        """ Consume and extract the entire result as a :class:`.Table`
        object.

        :return: the full query result
        """
        from py2neo.export import Table
        return Table(self)

    def to_subgraph(self):
        """ Consume and extract the entire result as a :class:`.Subgraph`
        containing the union of all the graph structures within.

        :return: :class:`.Subgraph` object
        """
        s = None
        for record in self:
            s_ = record.to_subgraph()
            if s_ is not None:
                if s is None:
                    s = s_
                else:
                    s |= s_
        return s

    def to_ndarray(self, dtype=None, order='K'):
        """ Consume and extract the entire result as a
        `numpy.ndarray <https://docs.scipy.org/doc/numpy/reference/generated/numpy.ndarray.html>`_.

        .. note::
           This method requires `numpy` to be installed.

        :param dtype:
        :param order:
        :warns: If `numpy` is not installed
        :returns: `ndarray
            <https://docs.scipy.org/doc/numpy/reference/generated/numpy.ndarray.html>`__ object.
        """
        from py2neo.export import to_numpy_ndarray
        return to_numpy_ndarray(self, dtype, order)

    def to_series(self, field=0, index=None, dtype=None):
        """ Consume and extract one field of the entire result as a
        `pandas.Series <http://pandas.pydata.org/pandas-docs/stable/dsintro.html#series>`_.

        .. note::
           This method requires `pandas` to be installed.

        :param field:
        :param index:
        :param dtype:
        :warns: If `pandas` is not installed
        :returns: `Series
            <http://pandas.pydata.org/pandas-docs/stable/dsintro.html#series>`__ object.
        """
        from py2neo.export import to_pandas_series
        return to_pandas_series(self, field, index, dtype)

    def to_data_frame(self, index=None, columns=None, dtype=None):
        """ Consume and extract the entire result as a
        `pandas.DataFrame <http://pandas.pydata.org/pandas-docs/stable/dsintro.html#dataframe>`_.

        ::

            >>> from py2neo import Graph
            >>> graph = Graph()
            >>> graph.run("MATCH (a:Person) RETURN a.name, a.born LIMIT 4").to_data_frame()
               a.born              a.name
            0    1964        Keanu Reeves
            1    1967    Carrie-Anne Moss
            2    1961  Laurence Fishburne
            3    1960        Hugo Weaving

        .. note::
           This method requires `pandas` to be installed.

        :param index: Index to use for resulting frame.
        :param columns: Column labels to use for resulting frame.
        :param dtype: Data type to force.
        :warns: If `pandas` is not installed
        :returns: `DataFrame
            <http://pandas.pydata.org/pandas-docs/stable/dsintro.html#series>`__ object.
        """
        from py2neo.export import to_pandas_data_frame
        return to_pandas_data_frame(self, index, columns, dtype)

    def to_matrix(self, mutable=False):
        """ Consume and extract the entire result as a
        `sympy.Matrix <http://docs.sympy.org/latest/tutorial/matrices.html>`_.

        .. note::
           This method requires `sympy` to be installed.

        :param mutable:
        :returns: `Matrix
            <http://docs.sympy.org/latest/tutorial/matrices.html>`_ object.
        """
        from py2neo.export import to_sympy_matrix
        return to_sympy_matrix(self, mutable)


class Record(tuple, Mapping):
    """ A :class:`.Record` object holds an ordered, keyed collection of
    values. It is in many ways similar to a :class:`namedtuple` but
    allows field access only through bracketed syntax, and provides
    more functionality. :class:`.Record` extends both :class:`tuple`
    and :class:`Mapping`.

    .. describe:: record[index]
                  record[key]

        Return the value of *record* with the specified *key* or *index*.

    .. describe:: len(record)

        Return the number of fields in *record*.

    .. describe:: dict(record)

        Return a `dict` representation of *record*.

    """

    __keys = None

    def __new__(cls, keys, values):
        inst = tuple.__new__(cls, values)
        inst.__keys = keys
        return inst

    def __repr__(self):
        return "Record({%s})" % ", ".join("%r: %r" % (field, self[i])
                                          for i, field in enumerate(self.__keys))

    def __str__(self):
        return "\t".join(map(repr, (self[i] for i, _ in enumerate(self.__keys))))

    def __eq__(self, other):
        return dict(self) == dict(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return reduce(xor_operator, map(hash, self.items()))

    def __getitem__(self, key):
        if isinstance(key, slice):
            keys = self.__keys[key]
            values = super(Record, self).__getitem__(key)
            return self.__class__(zip(keys, values))
        index = self.index(key)
        if 0 <= index < len(self):
            return super(Record, self).__getitem__(index)
        else:
            return None

    def __getslice__(self, start, stop):
        key = slice(start, stop)
        keys = self.__keys[key]
        values = tuple(self)[key]
        return self.__class__(zip(keys, values))

    def get(self, key, default=None):
        """ Obtain a single value from the record by index or key. If the
        specified item does not exist, the default value is returned.

        :param key: index or key
        :param default: default value to be returned if `key` does not exist
        :return: selected value
        """
        try:
            index = self.__keys.index(ustr(key))
        except ValueError:
            return default
        if 0 <= index < len(self):
            return super(Record, self).__getitem__(index)
        else:
            return default

    def index(self, key):
        """ Return the index of the given item.
        """
        from six import integer_types, string_types
        if isinstance(key, integer_types):
            if 0 <= key < len(self.__keys):
                return key
            raise IndexError(key)
        elif isinstance(key, string_types):
            try:
                return self.__keys.index(key)
            except ValueError:
                raise KeyError(key)
        else:
            raise TypeError(key)

    def keys(self):
        """ Return the keys of the record.

        :return: list of key names
        """
        return list(self.__keys)

    def values(self, *keys):
        """ Return the values of the record, optionally filtering to
        include only certain values by index or key.

        :param keys: indexes or keys of the items to include; if none
                     are provided, all values will be included
        :return: list of values
        """
        if keys:
            d = []
            for key in keys:
                try:
                    i = self.index(key)
                except KeyError:
                    d.append(None)
                else:
                    d.append(self[i])
            return d
        return list(self)

    def items(self, *keys):
        """ Return the fields of the record as a list of key and value tuples

        :param keys: indexes or keys of the items to include; if none
                     are provided, all values will be included
        :return: list of (key, value) tuples
        """
        if keys:
            d = []
            for key in keys:
                try:
                    i = self.index(key)
                except KeyError:
                    d.append((key, None))
                else:
                    d.append((self.__keys[i], self[i]))
            return d
        return list((self.__keys[i], super(Record, self).__getitem__(i)) for i in range(len(self)))

    def data(self, *keys):
        """ Return the keys and values of this record as a dictionary,
        optionally including only certain values by index or key. Keys
        provided that do not exist within the record will be included
        but with a value of :py:const:`None`; indexes provided
        that are out of bounds will trigger an :exc:`IndexError`.

        :param keys: indexes or keys of the items to include; if none
                     are provided, all values will be included
        :return: dictionary of values, keyed by field name
        :raises: :exc:`IndexError` if an out-of-bounds index is specified
        """
        if keys:
            d = {}
            for key in keys:
                try:
                    i = self.index(key)
                except KeyError:
                    d[key] = None
                else:
                    d[self.__keys[i]] = self[i]
            return d
        return dict(self)

    def to_subgraph(self):
        """ Return a :class:`.Subgraph` containing the union of all the
        graph structures within this :class:`.Record`.

        :return: :class:`.Subgraph` object
        """
        from py2neo.data import Subgraph
        s = None
        for value in self.values():
            if isinstance(value, Subgraph):
                if s is None:
                    s = value
                else:
                    s |= value
        return s


class CypherExpression(object):

    def __init__(self, value):
        self.__value = value

    @property
    def value(self):
        return self.__value


def cypher_escape(identifier):
    """ Return a Cypher identifier, with escaping if required.

    Simple Cypher identifiers, which just contain alphanumerics
    and underscores, can be represented as-is in expressions.
    Any which contain more esoteric characters, such as spaces
    or punctuation, must be escaped in backticks. Backticks
    themselves are escaped by doubling.

    ::

        >>> cypher_escape("simple_identifier")
        'simple_identifier'
        >>> cypher_escape("identifier with spaces")
        '`identifier with spaces`'
        >>> cypher_escape("identifier with `backticks`")
        '`identifier with ``backticks```'

    Identifiers are used in Cypher to denote named values, labels,
    relationship types and property keys. This function will typically
    be used to construct dynamic Cypher queries in places where
    parameters cannot be used.

        >>> "MATCH (a:{label}) RETURN id(a)".format(label=cypher_escape("Employee of the Month"))
        'MATCH (a:`Employee of the Month`) RETURN id(a)'

    :param identifier: any non-empty string
    """
    if not isinstance(identifier, string_types):
        raise TypeError(type(identifier).__name__)
    encoder = CypherEncoder()
    return encoder.encode_key(identifier)


def cypher_join(*clauses, **parameters):
    """ Join multiple Cypher clauses, returning a (query, parameters)
    tuple. Each clause may either be a simple string query or a
    (query, parameters) tuple. Additional `parameters` may also be
    supplied as keyword arguments.

    :param clauses:
    :param parameters:
    :return: (query, parameters) tuple
    """
    query = []
    params = {}
    for clause in clauses:
        if clause is None:
            continue
        if isinstance(clause, tuple):
            try:
                q, p = clause
            except ValueError:
                raise ValueError("Expected query or (query, parameters) tuple "
                                 "for clause %r" % clause)
        else:
            q = clause
            p = None
        query.append(q)
        if p:
            params.update(p)
    params.update(parameters)
    return "\n".join(query), params


def cypher_repr(value, **kwargs):
    """ Return the Cypher representation of a value.

    This function attempts to convert the supplied value into a Cypher
    literal form, as used in expressions.

    """
    encoder = CypherEncoder(**kwargs)
    return encoder.encode_value(value)


def cypher_str(value, **kwargs):
    """ Convert a Cypher value to a Python Unicode string.

    This function converts the supplied value into a string form, as
    used for human-readable output. This is generally identical to
    :meth:`.cypher_repr` except for with string values, which are
    returned as-is, instead of being enclosed in quotes with certain
    characters escaped.

    """
    if isinstance(value, unicode_types):
        return value
    elif isinstance(value, string_types):
        return value.decode(kwargs.get("encoding", "utf-8"))
    else:
        return cypher_repr(value, **kwargs)
