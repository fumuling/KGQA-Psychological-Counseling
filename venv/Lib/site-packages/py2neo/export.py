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
This module allows exporting data to external formats.

Example:

    >>> from py2neo import Graph
    >>> from py2neo.export import to_pandas_data_frame
    >>> graph = Graph()
    >>> to_pandas_data_frame(graph.run("MATCH (a:Person) RETURN a.name, a.born LIMIT 4"))
       a.born              a.name
    0    1964        Keanu Reeves
    1    1967    Carrie-Anne Moss
    2    1961  Laurence Fishburne
    3    1960        Hugo Weaving

"""


from __future__ import absolute_import, print_function, unicode_literals

from io import StringIO
from warnings import warn

from py2neo.compat import numeric_types, ustr
from py2neo.cypher import cypher_repr, cypher_str


def to_numpy_ndarray(cursor, dtype=None, order='K'):
    """ Consume and extract the entire result as a
    `numpy.ndarray <https://docs.scipy.org/doc/numpy/reference/generated/numpy.ndarray.html>`_.

    .. note::
       This method requires `numpy` to be installed.

    :param cursor:
    :param dtype:
    :param order:
    :warns: If `numpy` is not installed
    :returns: `ndarray
        <https://docs.scipy.org/doc/numpy/reference/generated/numpy.ndarray.html>`__ object.
    """
    try:
        # noinspection PyPackageRequirements
        from numpy import array
    except ImportError:
        warn("Numpy is not installed.")
        raise
    else:
        return array(list(map(list, cursor)), dtype=dtype, order=order)


def to_pandas_series(cursor, field=0, index=None, dtype=None):
    """ Consume and extract one field of the entire result as a
    `pandas.Series <http://pandas.pydata.org/pandas-docs/stable/dsintro.html#series>`_.

    .. note::
       This method requires `pandas` to be installed.

    :param cursor:
    :param field:
    :param index:
    :param dtype:
    :warns: If `pandas` is not installed
    :returns: `Series
        <http://pandas.pydata.org/pandas-docs/stable/dsintro.html#series>`__ object.
    """
    try:
        # noinspection PyPackageRequirements
        from pandas import Series
    except ImportError:
        warn("Pandas is not installed.")
        raise
    else:
        return Series([record[field] for record in cursor], index=index, dtype=dtype)


def to_pandas_data_frame(cursor, index=None, columns=None, dtype=None):
    """ Consume and extract the entire result as a
    `pandas.DataFrame <http://pandas.pydata.org/pandas-docs/stable/dsintro.html#dataframe>`_.

    .. note::
       This method requires `pandas` to be installed.

    :param cursor:
    :param index: Index to use for resulting frame.
    :param columns: Column labels to use for resulting frame.
    :param dtype: Data type to force.
    :warns: If `pandas` is not installed
    :returns: `DataFrame
        <http://pandas.pydata.org/pandas-docs/stable/dsintro.html#series>`__ object.
    """
    try:
        # noinspection PyPackageRequirements
        from pandas import DataFrame
    except ImportError:
        warn("Pandas is not installed.")
        raise
    else:
        return DataFrame(list(map(dict, cursor)), index=index, columns=columns, dtype=dtype)


def to_sympy_matrix(cursor, mutable=False):
    """ Consume and extract the entire result as a
    `sympy.Matrix <http://docs.sympy.org/latest/tutorial/matrices.html>`_.

    .. note::
       This method requires `sympy` to be installed.

    :param cursor:
    :param mutable:
    :returns: `Matrix
        <http://docs.sympy.org/latest/tutorial/matrices.html>`_ object.
    """
    try:
        # noinspection PyPackageRequirements
        from sympy import MutableMatrix, ImmutableMatrix
    except ImportError:
        warn("Sympy is not installed.")
        raise
    else:
        if mutable:
            return MutableMatrix(list(map(list, cursor)))
        else:
            return ImmutableMatrix(list(map(list, cursor)))


class Table(list):
    """ Immutable list of records.

    A :class:`.Table` holds a list of :class:`.Record` objects, typically received as the result of a Cypher query.
    It provides a convenient container for working with a result in its entirety and provides methods for conversion into various output formats.
    :class:`.Table` extends ``list``.

    .. describe:: repr(table)

        Return a string containing an ASCII art representation of this table.
        Internally, this method calls :meth:`.write` with `header=True`, writing the output into an ``io.StringIO`` instance.

    """

    def __init__(self, records, keys=None):
        super(Table, self).__init__(map(tuple, records))
        if keys:
            k = list(map(ustr, keys))
        else:
            try:
                k = records.keys()
            except AttributeError:
                raise ValueError("Missing keys")
        width = len(k)
        t = [set() for _ in range(width)]
        o = [False] * width
        for record in self:
            for i, value in enumerate(record):
                if value is None:
                    o[i] = True
                else:
                    t[i].add(type(value))
        f = []
        for i, _ in enumerate(k):
            f.append({
                "type": t[i].copy().pop() if len(t[i]) == 1 else tuple(t[i]),
                "numeric": all(t_ in numeric_types for t_ in t[i]),
                "optional": o[i],
            })
        self._keys = k
        self._fields = f

    def __repr__(self):
        s = StringIO()
        self.write(file=s, header=True)
        return s.getvalue()

    def _repr_html_(self):
        """ Return a string containing an HTML representation of this table.
        This method is used by Jupyter notebooks to display the table natively within a browser.
        Internally, this method calls :meth:`.write_html` with `header=True`, writing the output into an ``io.StringIO`` instance.
        """
        s = StringIO()
        self.write_html(file=s, header=True)
        return s.getvalue()

    def keys(self):
        """ Return a list of field names for this table.
        """
        return list(self._keys)

    def field(self, key):
        """ Return a dictionary of metadata for a given field.
        The metadata includes the following values:

        `type`
            Single class or tuple of classes representing the
            field values.

        `numeric`
            :const:`True` if all field values are of a numeric
            type, :const:`False` otherwise.

        `optional`
            :const:`True` if any field values are :const:`None`,
            :const:`False` otherwise.

        """
        from six import integer_types, string_types
        if isinstance(key, integer_types):
            return self._fields[key]
        elif isinstance(key, string_types):
            try:
                index = self._keys.index(key)
            except ValueError:
                raise KeyError(key)
            else:
                return self._fields[index]
        else:
            raise TypeError(key)

    def _range(self, skip, limit):
        if skip is None:
            skip = 0
        if limit is None or skip + limit > len(self):
            return range(skip, len(self))
        else:
            return range(skip, skip + limit)

    def write(self, file=None, header=None, skip=None, limit=None, auto_align=True,
              padding=1, separator=u"|", newline=u"\r\n"):
        """ Write data to a human-readable ASCII art table.

        :param file: file-like object capable of receiving output
        :param header: boolean flag for addition of column headers
        :param skip: number of records to skip before beginning output
        :param limit: maximum number of records to include in output
        :param auto_align: if :const:`True`, right-justify numeric values
        :param padding: number of spaces to include between column separator and value
        :param separator: column separator character
        :param newline: newline character sequence
        :return: the number of records included in output
        """

        space = u" " * padding
        widths = [1 if header else 0] * len(self._keys)

        def calc_widths(values, **_):
            strings = [cypher_str(value).splitlines(False) for value in values]
            for i, s in enumerate(strings):
                w = max(map(len, s)) if s else 0
                if w > widths[i]:
                    widths[i] = w

        def write_line(values, underline=u""):
            strings = [cypher_str(value).splitlines(False) for value in values]
            height = max(map(len, strings)) if strings else 1
            for y in range(height):
                line_text = u""
                underline_text = u""
                for x, _ in enumerate(values):
                    try:
                        text = strings[x][y]
                    except IndexError:
                        text = u""
                    if auto_align and self._fields[x]["numeric"]:
                        text = space + text.rjust(widths[x]) + space
                        u_text = underline * len(text)
                    else:
                        text = space + text.ljust(widths[x]) + space
                        u_text = underline * len(text)
                    if x > 0:
                        text = separator + text
                        u_text = separator + u_text
                    line_text += text
                    underline_text += u_text
                if underline:
                    line_text += newline + underline_text
                line_text += newline
                print(line_text, end=u"", file=file)

        def apply(f):
            count = 0
            for count, index in enumerate(self._range(skip, limit), start=1):
                if count == 1 and header:
                    f(self.keys(), underline=u"-")
                f(self[index])
            return count

        apply(calc_widths)
        return apply(write_line)

    def write_html(self, file=None, header=None, skip=None, limit=None, auto_align=True):
        """ Write data to an HTML table.

        :param file: file-like object capable of receiving output
        :param header: boolean flag for addition of column headers
        :param skip: number of records to skip before beginning output
        :param limit: maximum number of records to include in output
        :param auto_align: if :const:`True`, right-justify numeric values
        :return: the number of records included in output
        """

        def html_escape(s):
            return (s.replace(u"&", u"&amp;")
                     .replace(u"<", u"&lt;")
                     .replace(u">", u"&gt;")
                     .replace(u'"', u"&quot;")
                     .replace(u"'", u"&#039;"))

        def write_tr(values, tag):
            print(u"<tr>", end="", file=file)
            for i, value in enumerate(values):
                if tag == "th":
                    template = u'<{}>{}</{}>'
                elif auto_align and self._fields[i]["numeric"]:
                    template = u'<{} style="text-align:right">{}</{}>'
                else:
                    template = u'<{} style="text-align:left">{}</{}>'
                print(template.format(tag, html_escape(cypher_str(value)), tag), end="", file=file)
            print(u"</tr>", end="", file=file)

        count = 0
        print(u"<table>", end="", file=file)
        for count, index in enumerate(self._range(skip, limit), start=1):
            if count == 1 and header:
                write_tr(self.keys(), u"th")
            write_tr(self[index], u"td")
        print(u"</table>", end="", file=file)
        return count

    def write_separated_values(self, separator, file=None, header=None, skip=None, limit=None,
                               newline=u"\r\n", quote=u"\""):
        """ Write data to a delimiter-separated file.

        :param separator: field separator character
        :param file: file-like object capable of receiving output
        :param header: boolean flag or string style tag, such as 'i' or 'cyan',
            for addition of column headers
        :param skip: number of records to skip before beginning output
        :param limit: maximum number of records to include in output
        :param newline: newline character sequence
        :param quote: quote character
        :return: the number of records included in output
        """
        from pansi import ansi
        from six import string_types

        escaped_quote = quote + quote
        quotable = separator + newline + quote

        def header_row(names):
            if isinstance(header, string_types):
                if hasattr(ansi, header):
                    template = "{%s}{}{_}" % header
                else:
                    t = [tag for tag in dir(ansi) if
                         not tag.startswith("_") and isinstance(getattr(ansi, tag), str)]
                    raise ValueError("Unknown style tag %r\n"
                                     "Available tags are: %s" % (header, ", ".join(map(repr, t))))
            else:
                template = "{}"
            for name in names:
                yield template.format(name, **ansi)

        def data_row(values):
            for value in values:
                if value is None:
                    yield ""
                    continue
                if isinstance(value, string_types):
                    value = ustr(value)
                    if any(ch in value for ch in quotable):
                        value = quote + value.replace(quote, escaped_quote) + quote
                else:
                    value = cypher_repr(value)
                yield value

        count = 0
        for count, index in enumerate(self._range(skip, limit), start=1):
            if count == 1 and header:
                print(*header_row(self.keys()), sep=separator, end=newline, file=file)
            print(*data_row(self[index]), sep=separator, end=newline, file=file)
        return count

    def write_csv(self, file=None, header=None, skip=None, limit=None):
        """ Write the data as RFC4180-compatible comma-separated values.
        This is a customised call to :meth:`.write_separated_values`.
        """
        return self.write_separated_values(u",", file, header, skip, limit)

    def write_tsv(self, file=None, header=None, skip=None, limit=None):
        """ Write the data as tab-separated values.
        This is a customised call to :meth:`.write_separated_values`.
        """
        return self.write_separated_values(u"\t", file, header, skip, limit)
