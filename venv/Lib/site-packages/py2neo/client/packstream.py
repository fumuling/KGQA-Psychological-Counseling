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


from __future__ import division

from codecs import decode
from collections import namedtuple
from io import BytesIO
from struct import pack as struct_pack, unpack as struct_unpack

from py2neo.client import Hydrant
from py2neo.compat import bytes_types, integer_types, UNICODE
from py2neo.data import Node, Relationship, Path
from py2neo.data.spatial import Point

from neotime import Duration, Date, Time, DateTime
from pytz import FixedOffset, timezone
from six import PY2


PACKED_UINT_8 = [struct_pack(">B", value) for value in range(0x100)]
PACKED_UINT_16 = [struct_pack(">H", value) for value in range(0x10000)]

UNPACKED_UINT_8 = {bytes(bytearray([x])): x for x in range(0x100)}
UNPACKED_UINT_16 = {struct_pack(">H", x): x for x in range(0x10000)}

UNPACKED_MARKERS = {b"\xC0": None, b"\xC2": False, b"\xC3": True}
UNPACKED_MARKERS.update({bytes(bytearray([z])): z for z in range(0x00, 0x80)})
UNPACKED_MARKERS.update({bytes(bytearray([z + 256])): z for z in range(-0x10, 0x00)})


INT64_MIN = -(2 ** 63)
INT64_MAX = 2 ** 63


UNIX_EPOCH_DATE_ORDINAL = Date(1970, 1, 1).to_ordinal()


unbound_relationship = namedtuple("UnboundRelationship", ["id", "type", "properties"])


class Structure(object):

    def __init__(self, tag, *fields):
        self.tag = tag
        self.fields = list(fields)

    def __repr__(self):
        return "Structure[#%02X](%s)" % (self.tag, ", ".join(map(repr, self.fields)))

    def __eq__(self, other):
        try:
            return self.tag == other.tag and self.fields == other.fields
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __len__(self):
        return len(self.fields)

    def __getitem__(self, key):
        return self.fields[key]

    def __setitem__(self, key, value):
        self.fields[key] = value


def pack_into(buffer, *values, **kwargs):
    """ Pack values into a buffer.

    :param buffer:
    :param values:
    :param kwargs:
    :return:
    """
    from datetime import date, time, datetime, timedelta
    from neotime import Date, Time, DateTime, Duration
    from pytz import utc
    from py2neo.data.spatial import Point

    version = kwargs.get("version", ())

    unix_epoch_date = Date(1970, 1, 1)

    write_bytes = buffer.write

    def write_header(size, tiny, small=None, medium=None, large=None):
        if 0x0 <= size <= 0xF and tiny is not None:
            write_bytes(bytearray([tiny + size]))
        elif size < 0x100 and small is not None:
            write_bytes(bytearray([small]))
            write_bytes(PACKED_UINT_8[size])
        elif size < 0x10000 and medium is not None:
            write_bytes(bytearray([medium]))
            write_bytes(PACKED_UINT_16[size])
        elif size < 0x100000000 and large is not None:
            write_bytes(bytearray([large]))
            write_bytes(struct_pack(">I", size))
        else:
            raise ValueError("Collection too large")

    def write_time(t):
        try:
            nanoseconds = int(t.ticks * 1000000000)
        except AttributeError:
            nanoseconds = (3600000000000 * t.hour + 60000000000 * t.minute +
                           1000000000 * t.second + 1000 * t.microsecond)
        if t.tzinfo:
            write_bytes(b"\xB2T")
            pack_into(buffer, nanoseconds, t.tzinfo.utcoffset(t).seconds)
        else:
            write_bytes(b"\xB1t")
            pack_into(buffer, nanoseconds)

    def seconds_and_nanoseconds(dt):
        if isinstance(dt, datetime):
            dt = DateTime.from_native(dt)
        zone_epoch = DateTime(1970, 1, 1, tzinfo=dt.tzinfo)
        t = dt.to_clock_time() - zone_epoch.to_clock_time()
        return t.seconds, t.nanoseconds

    def write_datetime(dt):
        tz = dt.tzinfo
        if tz is None:
            # without time zone
            local_dt = utc.localize(dt)
            seconds, nanoseconds = seconds_and_nanoseconds(local_dt)
            write_bytes(b"\xB2d")
            pack_into(buffer, seconds, nanoseconds)
        elif hasattr(tz, "zone") and tz.zone:
            # with named time zone
            seconds, nanoseconds = seconds_and_nanoseconds(dt)
            write_bytes(b"\xB3f")
            pack_into(buffer, seconds, nanoseconds, tz.zone)
        else:
            # with time offset
            seconds, nanoseconds = seconds_and_nanoseconds(dt)
            write_bytes(b"\xB3F")
            pack_into(buffer, seconds, nanoseconds, tz.utcoffset(dt).seconds)

    def write_point(p):
        dim = len(p)
        write_bytes(bytearray([0xB1 + dim]))
        if dim == 2:
            write_bytes(b"X")
        elif dim == 3:
            write_bytes(b"Y")
        else:
            raise ValueError("Cannot dehydrate Point with %d dimensions" % dim)
        pack_into(buffer, p.srid, *p)

    for value in values:

        # None
        if value is None:
            write_bytes(b"\xC0")  # NULL

        # Boolean
        elif value is True:
            write_bytes(b"\xC3")
        elif value is False:
            write_bytes(b"\xC2")

        # Float (only double precision is supported)
        elif isinstance(value, float):
            write_bytes(b"\xC1")
            write_bytes(struct_pack(">d", value))

        # Integer
        elif isinstance(value, integer_types):
            if -0x10 <= value < 0x80:
                write_bytes(PACKED_UINT_8[value % 0x100])
            elif -0x80 <= value < -0x10:
                write_bytes(b"\xC8")
                write_bytes(PACKED_UINT_8[value % 0x100])
            elif -0x8000 <= value < 0x8000:
                write_bytes(b"\xC9")
                write_bytes(PACKED_UINT_16[value % 0x10000])
            elif -0x80000000 <= value < 0x80000000:
                write_bytes(b"\xCA")
                write_bytes(struct_pack(">i", value))
            elif INT64_MIN <= value < INT64_MAX:
                write_bytes(b"\xCB")
                write_bytes(struct_pack(">q", value))
            else:
                raise ValueError("Integer %s out of range" % value)

        # String (from bytes)
        elif isinstance(value, bytes):
            write_header(len(value), 0x80, 0xD0, 0xD1, 0xD2)
            write_bytes(value)

        # String (from unicode)
        elif isinstance(value, UNICODE):
            encoded = value.encode("utf-8")
            write_header(len(encoded), 0x80, 0xD0, 0xD1, 0xD2)
            write_bytes(encoded)

        # Byte array
        elif isinstance(value, bytes_types):
            write_header(len(value), None, 0xCC, 0xCD, 0xCE)
            write_bytes(bytes(value))

        # List
        elif isinstance(value, list) or type(value) is tuple:
            write_header(len(value), 0x90, 0xD4, 0xD5, 0xD6)
            pack_into(buffer, *value, version=version)

        # Dictionary
        elif isinstance(value, dict):
            write_header(len(value), 0xA0, 0xD8, 0xD9, 0xDA)
            for key, item in value.items():
                if isinstance(key, (bytes, UNICODE)):
                    pack_into(buffer, key, item, version=version)
                else:
                    raise TypeError("Dictionary key {!r} is not a string".format(key))

        # Bolt 2 introduced temporal and spatial types
        elif version < (2, 0):
            raise TypeError("Values of type %s are not supported "
                            "by Bolt %s" % (type(value), ".".join(version)))

        # DateTime
        #
        # Note: The built-in datetime.datetime class extends the
        # datetime.date class, so this needs to be listed first
        # to avoid objects being encoded incorrectly.
        #
        elif isinstance(value, (datetime, DateTime)):
            write_datetime(value)

        # Date
        elif isinstance(value, (date, Date)):
            write_bytes(b"\xB1D")
            pack_into(buffer, value.toordinal() - unix_epoch_date.toordinal())

        # Time
        elif isinstance(value, (time, Time)):
            write_time(value)

        # TimeDelta
        elif isinstance(value, timedelta):
            write_bytes(b"\xB4E")
            pack_into(buffer,
                      0,                                    # months
                      value.days,                           # days
                      value.seconds,                        # seconds
                      1000 * value.microseconds)            # nanoseconds

        # Duration
        elif isinstance(value, Duration):
            write_bytes(b"\xB4E")
            pack_into(buffer,
                      value.months,                         # months
                      value.days,                           # days
                      value.seconds,                        # seconds
                      int(1000000000 * value.subseconds))   # nanoseconds

        # Point
        elif isinstance(value, Point):
            write_point(value)

        # Other
        else:
            raise TypeError("Values of type %s are not supported" % type(value))


def pack(*values, **kwargs):
    buffer = BytesIO()
    pack_into(buffer, *values, **kwargs)
    return buffer.getvalue()


class UnpackStream(object):

    def __init__(self, data, offset=0):
        if PY2:
            self._data = bytearray(data)
        else:
            self._data = data
        self._offset = offset

    def unpack(self):
        marker = self._data[self._offset]
        self._offset += 1

        # Tiny collections
        if marker == 0x80:
            return ""
        elif 0x81 <= marker <= 0x8F:  # TINY_STRING
            start = self._offset
            self._offset = start + (marker & 0x0F)
            return decode(self._data[start:self._offset], "utf-8")
        elif marker == 0x90:
            return []
        elif marker == 0x91:
            return [self.unpack()]
        elif marker == 0x92:
            return [self.unpack(), self.unpack()]
        elif marker == 0x93:
            return [self.unpack(), self.unpack(), self.unpack()]
        elif 0x94 <= marker <= 0x9F:    # TINY_LIST
            return [self.unpack() for _ in range(marker & 0x0F)]
        elif 0xA0 <= marker <= 0xAF:    # TINY_DICT
            size = marker & 0x0F
            value = {}
            for _ in range(size):
                key = self.unpack()
                value[key] = self.unpack()
            return value

        # Integer
        elif 0x00 <= marker <= 0x7F:
            return marker
        elif 0xF0 <= marker <= 0xFF:
            return marker - 0x100
        elif marker == 0xC8:
            return self._read_i8()
        elif marker == 0xC9:
            return self._read_i16be()
        elif marker == 0xCA:
            return self._read_i32be()
        elif marker == 0xCB:
            return self._read_i64be()

        # String
        elif marker == 0xD0:  # STRING_8:
            size = self._read_u8()
            return decode(self._read(size), "utf-8")
        elif marker == 0xD1:  # STRING_16:
            size = self._read_u16be()
            return decode(self._read(size), "utf-8")
        elif marker == 0xD2:  # STRING_32:
            size = self._read_u32be()
            return decode(self._read(size), "utf-8")

        # Structure
        elif 0xB0 <= marker <= 0xBF:    # TINY_STRUCT
            tag = self._read_u8()
            fields = [self.unpack() for _ in range(marker & 0x0F)]
            if tag == 68:  # 'D'
                return self._hydrate_date(*fields)
            elif tag in (84, 116):  # 'T' and 't'
                return self._hydrate_time(*fields)
            elif tag in (70, 100, 102):  # b"F", b"f", b"d"
                return self._hydrate_datetime(*fields)
            elif tag == 69:  # b"E"
                return self._hydrate_duration(*fields)
            elif tag in (88, 89):  # b"X", b"Y"
                return self._hydrate_point(*fields)
            else:
                return Structure(tag, *fields)

        # Float
        elif marker == 0xC1:
            return self._read_f64be()

        # Boolean
        elif marker == 0xC2:
            return False
        elif marker == 0xC3:
            return True

        # Null
        elif marker == 0xC0:
            return None

        # Bytes
        elif marker == 0xCC:
            size = self._read_u8()
            return bytes(self._read(size))
        elif marker == 0xCD:
            size = self._read_u16be()
            return bytes(self._read(size))
        elif marker == 0xCE:
            size = self._read_u32be()
            return bytes(self._read(size))

        # List
        elif marker == 0xD4:  # LIST_8:
            size = self._read_u8()
            return [self.unpack() for _ in range(size)]
        elif marker == 0xD5:  # LIST_16:
            size = self._read_u16be()
            return [self.unpack() for _ in range(size)]
        elif marker == 0xD6:  # LIST_32:
            size = self._read_u32be()
            return [self.unpack() for _ in range(size)]

        # Dictionary
        elif marker == 0xD8:  # MAP_8:
            size = self._read_u8()
            value = {}
            for _ in range(size):
                key = self.unpack()
                value[key] = self.unpack()
            return value
        elif marker == 0xD9:  # MAP_16:
            size = self._read_u16be()
            value = {}
            for _ in range(size):
                key = self.unpack()
                value[key] = self.unpack()
            return value
        elif marker == 0xDA:  # MAP_32:
            size = self._read_u32be()
            value = {}
            for _ in range(size):
                key = self.unpack()
                value[key] = self.unpack()
            return value

        else:
            raise ValueError("Unknown PackStream marker %02X" % marker)

    def _hydrate_date(self, days):
        """ Hydrator for `Date` values.

        :param days:
        :return: Date
        """
        return Date.from_ordinal(UNIX_EPOCH_DATE_ORDINAL + days)

    def _hydrate_time(self, nanoseconds, tz=None):
        """ Hydrator for `Time` and `LocalTime` values.

        :param nanoseconds:
        :param tz:
        :return: Time
        """
        seconds, nanoseconds = map(int, divmod(nanoseconds, 1000000000))
        minutes, seconds = map(int, divmod(seconds, 60))
        hours, minutes = map(int, divmod(minutes, 60))
        seconds = (1000000000 * seconds + nanoseconds) / 1000000000
        t = Time(hours, minutes, seconds)
        if tz is None:
            return t
        tz_offset_minutes, tz_offset_seconds = divmod(tz, 60)
        zone = FixedOffset(tz_offset_minutes)
        return zone.localize(t)

    def _hydrate_datetime(self, seconds, nanoseconds, tz=None):
        """ Hydrator for `DateTime` and `LocalDateTime` values.

        :param seconds:
        :param nanoseconds:
        :param tz:
        :return: datetime
        """
        minutes, seconds = map(int, divmod(seconds, 60))
        hours, minutes = map(int, divmod(minutes, 60))
        days, hours = map(int, divmod(hours, 24))
        seconds = (1000000000 * seconds + nanoseconds) / 1000000000
        t = DateTime.combine(Date.from_ordinal(UNIX_EPOCH_DATE_ORDINAL + days),
                             Time(hours, minutes, seconds))
        if tz is None:
            return t
        if isinstance(tz, int):
            tz_offset_minutes, tz_offset_seconds = divmod(tz, 60)
            zone = FixedOffset(tz_offset_minutes)
        else:
            zone = timezone(tz)
        return zone.localize(t)

    def _hydrate_duration(self, months, days, seconds, nanoseconds):
        """ Hydrator for `Duration` values.

        :param months:
        :param days:
        :param seconds:
        :param nanoseconds:
        :return: `duration` namedtuple
        """
        return Duration(months=months, days=days, seconds=seconds, nanoseconds=nanoseconds)

    def _hydrate_point(self, srid, *coordinates):
        """ Create a new instance of a Point subclass from a raw
        set of fields. The subclass chosen is determined by the
        given SRID; a ValueError will be raised if no such
        subclass can be found.
        """
        try:
            point_class, dim = Point.class_for_srid(srid)
        except KeyError:
            point = Point(coordinates)
            point.srid = srid
            return point
        else:
            if len(coordinates) != dim:
                raise ValueError("SRID %d requires %d coordinates (%d provided)" % (srid, dim, len(coordinates)))
            return point_class(coordinates)

    def _read(self, n=1):
        q = self._offset + n
        m = self._data[self._offset:q]
        self._offset = q
        return m

    def _read_u8(self):
        q = self._offset + 1
        n, = struct_unpack(">B", self._data[self._offset:q])
        self._offset = q
        return n

    def _read_u16be(self):
        q = self._offset + 2
        n, = struct_unpack(">H", self._data[self._offset:q])
        self._offset = q
        return n

    def _read_u32be(self):
        q = self._offset + 4
        n, = struct_unpack(">I", self._data[self._offset:q])
        self._offset = q
        return n

    def _read_i8(self):
        q = self._offset + 1
        z, = struct_unpack(">b", self._data[self._offset:q])
        self._offset = q
        return z

    def _read_i16be(self):
        q = self._offset + 2
        z, = struct_unpack(">h", self._data[self._offset:q])
        self._offset = q
        return z

    def _read_i32be(self):
        q = self._offset + 4
        z, = struct_unpack(">i", self._data[self._offset:q])
        self._offset = q
        return z

    def _read_i64be(self):
        q = self._offset + 8
        z, = struct_unpack(">q", self._data[self._offset:q])
        self._offset = q
        return z

    def _read_f64be(self):
        q = self._offset + 8
        r, = struct_unpack(">d", self._data[self._offset:q])
        self._offset = q
        return r


class PackStreamHydrant(Hydrant):

    def __init__(self, graph):
        self.graph = graph

    def hydrate_list(self, obj):
        for i, value in enumerate(obj):
            t = type(value)
            if t is list:
                obj[i] = self.hydrate_list(value)
            elif t is dict:
                obj[i] = self.hydrate_dict(value)
            elif t is Structure:
                obj[i] = self.hydrate_structure(value)
        return obj

    def hydrate_dict(self, obj):
        for key, value in obj.items():
            t = type(value)
            if t is list:
                obj[key] = self.hydrate_list(value)
            elif t is dict:
                obj[key] = self.hydrate_dict(value)
            elif t is Structure:
                obj[key] = self.hydrate_structure(value)
        return obj

    def hydrate_structure(self, obj):
        tag = obj.tag
        if tag == 78:
            return self._hydrate_node(*obj.fields)
        elif tag == 82:
            return self._hydrate_relationship(*obj.fields)
        elif tag == 80:
            return self._hydrate_path(*obj.fields)
        else:
            return obj

    def _hydrate_node(self, identity, labels, properties):
        node = Node.ref(self.graph, identity)
        node.clear_labels()
        node.update_labels(labels)
        node.clear()
        node.update(properties)
        return node

    def _hydrate_relationship(self, identity, start_node_id, end_node_id, r_type, properties):
        start_node = Node.ref(self.graph, start_node_id)
        end_node = Node.ref(self.graph, end_node_id)
        rel = Relationship.ref(self.graph, identity, start_node, r_type, end_node)
        rel.clear()
        rel.update(properties)
        return rel

    def _hydrate_path(self, nodes, relationships, sequence):
        nodes = [self._hydrate_node(n_id, n_label, n_properties)
                 for n_id, n_label, n_properties in nodes]
        u_rels = []
        for r_id, r_type, r_properties in relationships:
            u_rel = unbound_relationship(r_id, r_type, r_properties)
            u_rels.append(u_rel)
        return Path.hydrate(self.graph, nodes, u_rels, sequence)
