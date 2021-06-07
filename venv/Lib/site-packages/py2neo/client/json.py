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


from collections import namedtuple
from logging import getLogger

from py2neo.client import Hydrant
from py2neo.client.packstream import Structure
from py2neo.compat import Sequence, Mapping, integer_types, string_types
from py2neo.matching import RelationshipMatcher


log = getLogger(__name__)


INT64_MIN = -(2 ** 63)
INT64_MAX = 2 ** 63 - 1


class JSONHydrant(Hydrant):

    unbound_relationship = namedtuple("UnboundRelationship", ["id", "type", "properties"])

    def __init__(self, graph):
        self.graph = graph
        self.hydration_functions = {}

    @classmethod
    def _uri_to_id(cls, uri):
        """ Utility function to convert entity URIs into numeric identifiers.
        """
        _, _, identity = uri.rpartition("/")
        return int(identity)

    @classmethod
    def json_to_packstream(cls, data):
        """ This converts from JSON format into PackStream prior to
        proper hydration. This code needs to die horribly in a freak
        yachting accident.
        """
        # TODO: other partial hydration
        if "self" in data:
            if "type" in data:
                return Structure(ord(b"R"),
                                 cls._uri_to_id(data["self"]),
                                 cls._uri_to_id(data["start"]),
                                 cls._uri_to_id(data["end"]),
                                 data["type"],
                                 data["data"])
            else:
                return Structure(ord(b"N"),
                                 cls._uri_to_id(data["self"]),
                                 data["metadata"]["labels"],
                                 data["data"])
        elif "nodes" in data and "relationships" in data:
            nodes = [Structure(ord(b"N"), i, None, None) for i in map(cls._uri_to_id, data["nodes"])]
            relps = [Structure(ord(b"r"), i, None, None) for i in map(cls._uri_to_id, data["relationships"])]
            seq = [i // 2 + 1 for i in range(2 * len(data["relationships"]))]
            for i, direction in enumerate(data["directions"]):
                if direction == "<-":
                    seq[2 * i] *= -1
            return Structure(ord(b"P"), nodes, relps, seq)
        else:
            # from warnings import warn
            # warn("Map literals returned over the Neo4j HTTP interface are ambiguous "
            #      "and may be unintentionally hydrated as graph objects")
            return data

    def hydrate_list(self, values):
        """ Convert JSON values into native values. This is the other half
        of the HTTP hydration process, and is basically a copy of the
        Bolt/PackStream hydration code. It needs to be combined with the
        code in `json_to_packstream` so that hydration is done in a single
        pass.
        """
        assert isinstance(values, list)
        for i, value in enumerate(values):
            if isinstance(value, (list, dict, Structure)):
                values[i] = self.hydrate_object(value)
        return values

    def hydrate_object(self, obj):
        log.debug("Hydrating object %r", obj)
        from py2neo.data import Node, Relationship, Path
        if isinstance(obj, Structure):
            tag = obj.tag
            fields = obj.fields
            if tag == ord(b"N"):
                obj = Node.ref(self.graph, fields[0])
                if fields[1] is not None:
                    obj._remote_labels = frozenset(fields[1])
                    obj.clear_labels()
                    obj.update_labels(fields[1])
                if fields[2] is not None:
                    obj.clear()
                    obj.update(self.hydrate_object(fields[2]))
                return obj
            elif tag == ord(b"R"):
                start_node = Node.ref(self.graph, fields[1])
                end_node = Node.ref(self.graph, fields[2])
                obj = Relationship.ref(self.graph, fields[0], start_node, fields[3], end_node)
                if fields[4] is not None:
                    obj.clear()
                    obj.update(self.hydrate_object(fields[4]))
                return obj
            elif tag == ord(b"P"):
                # Herein lies a dirty hack to retrieve missing relationship
                # detail for paths received over HTTP.
                nodes = [self.hydrate_object(node) for node in fields[0]]
                u_rels = []
                typeless_u_rel_ids = []
                for r in fields[1]:
                    u_rel = self.unbound_relationship(*map(self.hydrate_object, r))
                    assert u_rel.type is None
                    typeless_u_rel_ids.append(u_rel.id)
                    u_rels.append(u_rel)
                if typeless_u_rel_ids:
                    r_dict = {r.identity: r for r in RelationshipMatcher(self.graph).get(typeless_u_rel_ids)}
                    for i, u_rel in enumerate(u_rels):
                        if u_rel.type is None:
                            u_rels[i] = self.unbound_relationship(
                                u_rel.id,
                                type(r_dict[u_rel.id]).__name__,
                                u_rel.properties
                            )
                sequence = fields[2]
                return Path.hydrate(self.graph, nodes, u_rels, sequence)
            else:
                try:
                    f = self.hydration_functions[tag]
                except KeyError:
                    # If we don't recognise the structure type, just return it as-is
                    return obj
                else:
                    return f(*map(self.hydrate_object, obj.fields))
        elif isinstance(obj, list):
            return list(map(self.hydrate_object, obj))
        elif isinstance(obj, dict):
            return {key: self.hydrate_object(value) for key, value in obj.items()}
        else:
            return obj

    def dehydrate(self, data, version=None):
        """ Dehydrate to JSON.
        """
        if data is None or data is True or data is False or isinstance(data, float) or isinstance(data, string_types):
            return data
        elif isinstance(data, integer_types):
            if data < INT64_MIN or data > INT64_MAX:
                raise ValueError("Integers must be within the signed 64-bit range")
            return data
        elif isinstance(data, bytearray):
            return list(data)
        elif isinstance(data, Mapping):
            d = {}
            for key in data:
                if not isinstance(key, string_types):
                    raise TypeError("Dictionary keys must be strings")
                d[key] = self.dehydrate(data[key])
            return d
        elif isinstance(data, Sequence):
            return list(map(self.dehydrate, data))
        else:
            raise TypeError("Neo4j does not support JSON parameters of type %s" % type(data).__name__)


def dehydrate(data):
    """ Dehydrate to JSON.
    """
    if data is None or data is True or data is False or isinstance(data, float) or isinstance(data, string_types):
        return data
    elif isinstance(data, integer_types):
        if data < INT64_MIN or data > INT64_MAX:
            raise ValueError("Integers must be within the signed 64-bit range")
        return data
    elif isinstance(data, bytearray):
        return list(data)
    elif isinstance(data, Mapping):
        d = {}
        for key in data:
            if not isinstance(key, string_types):
                raise TypeError("Dictionary keys must be strings")
            d[key] = dehydrate(data[key])
        return d
    elif isinstance(data, Sequence):
        return list(map(dehydrate, data))
    else:
        raise TypeError("Neo4j does not support JSON parameters of type %s" % type(data).__name__)
