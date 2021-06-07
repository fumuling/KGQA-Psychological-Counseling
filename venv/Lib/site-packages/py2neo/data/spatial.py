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
This module defines spatial data types.
"""


__all__ = [
    "Point",
    "CartesianPoint",
    "WGS84Point",
]


# SRID to subclass mappings
_srid_table = {}


class Point(tuple):
    """ A point within a geometric space. This type is generally used
    via its subclasses and should not be instantiated directly unless
    there is no subclass defined for the required SRID.
    """

    @classmethod
    def class_for_srid(cls, srid):
        point_class, dim = _srid_table[srid]
        return point_class, dim

    srid = None

    def __new__(cls, iterable):
        return tuple.__new__(cls, iterable)

    def __repr__(self):
        return "POINT(%s)" % " ".join(map(str, self))

    def __eq__(self, other):
        try:
            return type(self) is type(other) and tuple(self) == tuple(other)
        except (AttributeError, TypeError):
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(type(self)) ^ hash(tuple(self))


def _point_subclass(name, fields, srid_map):
    """ Dynamically create a Point subclass.
    """

    def srid(self):
        try:
            return srid_map[len(self)]
        except KeyError:
            return None

    attributes = {"srid": property(srid)}

    for index, subclass_field in enumerate(fields):

        def accessor(self, i=index, f=subclass_field):
            try:
                return self[i]
            except IndexError:
                raise AttributeError(f)

        for field_alias in {subclass_field, "xyz"[index]}:
            attributes[field_alias] = property(accessor)

    cls = type(name, (Point,), attributes)

    for dim, srid in srid_map.items():
        _srid_table[srid] = (cls, dim)

    return cls


# Point subclass definitions
CartesianPoint = _point_subclass("CartesianPoint", ["x", "y", "z"], {2: 7203, 3: 9157})
WGS84Point = _point_subclass("WGS84Point", ["longitude", "latitude", "height"], {2: 4326, 3: 4979})
