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

from py2neo.compat import Set, bytes_types, string_types


def is_collection(obj):
    """ Returns true for any iterable which is not a string or byte sequence.
    """
    if isinstance(obj, bytes_types) or isinstance(obj, string_types):
        return False
    try:
        iter(obj)
    except TypeError:
        return False
    else:
        return True


def iter_items(iterable):
    """ Iterate through all items (key-value pairs) within an iterable
    dictionary-like object. If the object has a `keys` method, this is
    used along with `__getitem__` to yield each pair in turn. If no
    `keys` method exists, each iterable element is assumed to be a
    2-tuple of key and value.
    """
    if hasattr(iterable, "keys"):
        for key in iterable.keys():
            yield key, iterable[key]
    else:
        for key, value in iterable:
            yield key, value


class SetView(Set):

    def __init__(self, collection):
        self.__collection = collection

    def __eq__(self, other):
        return frozenset(self) == frozenset(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __len__(self):
        return len(self.__collection)

    def __iter__(self):
        return iter(self.__collection)

    def __contains__(self, element):
        return element in self.__collection

    def difference(self, other):
        cls = self.__class__
        return cls(frozenset(self).difference(frozenset(other)))


class PropertyDict(dict):
    """ Mutable key-value property store.

    A dictionary for property values that treats :const:`None`
    and missing values as semantically identical.

    PropertyDict instances can be created and used in a similar way
    to a standard dictionary. For example::

        >>> fruit = PropertyDict({"name": "banana", "colour": "yellow"})
        >>> fruit["name"]
        'banana'

    The key difference with a PropertyDict is in how it handles
    missing values. Instead of raising a :py:class:`KeyError`,
    attempts to access a missing value will simply return
    :py:const:`None` instead.

    These are the operations that the PropertyDict can support:

    .. describe:: len(d)

        Return the number of items in the PropertyDict `d`.

    .. describe:: d[key]

        Return the item of `d` with key `key`. Returns :py:const:`None`
        if key is not in the map.

    .. describe:: d == other

        Return ``True`` if ``d`` is equal to ``other`` after all ``None`` values have been removed from ``other``.

    .. describe:: properties != other

        Return ``True`` if ``d`` is unequal to ``other`` after all ``None`` values have been removed from ``other``.

    .. describe:: d[key]

        Return the value of *d* with key *key* or ``None`` if the key is missing.

    .. describe:: d[key] = value

        Set the value of *d* with key *key* to *value* or remove the item if *value* is ``None``.

    .. method:: setdefault(key, default=None)

        If *key* is in the PropertyDict, return its value.
        If not, insert *key* with a value of *default* and return *default* unless *default* is ``None``, in which case do nothing.
        The value of *default* defaults to ``None``.

    .. method:: update(iterable=None, **kwargs)

        Update the PropertyDict with the key-value pairs from *iterable* followed by the keyword arguments from *kwargs*.
        Individual properties already in the PropertyDict will be overwritten by those in *iterable* and subsequently by those in *kwargs* if the keys match.
        Any value of ``None`` will effectively delete the property with that key, should it exist.

    """

    def __init__(self, iterable=None, **kwargs):
        dict.__init__(self)
        self.update(iterable, **kwargs)

    def __eq__(self, other):
        return dict.__eq__(self, {key: value for key, value in other.items() if value is not None})

    def __ne__(self, other):
        return not self.__eq__(other)

    def __getitem__(self, key):
        return dict.get(self, key)

    def __setitem__(self, key, value):
        if value is None:
            try:
                dict.__delitem__(self, key)
            except KeyError:
                pass
        else:
            dict.__setitem__(self, key, value)

    def setdefault(self, key, default=None):
        if key in self:
            value = self[key]
        elif default is None:
            value = None
        else:
            value = dict.setdefault(self, key, default)
        return value

    def update(self, iterable=None, **kwargs):
        for key, value in dict(iterable or {}, **kwargs).items():
            self[key] = value
