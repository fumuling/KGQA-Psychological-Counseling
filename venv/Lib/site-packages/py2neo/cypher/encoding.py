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
This module provides facilities for encoding values into Cypher
identifiers and literals.
"""

from __future__ import absolute_import

from collections import OrderedDict
from re import compile as re_compile
from unicodedata import category

from py2neo.collections import SetView
from py2neo.compat import uchr, ustr, numeric_types, string_types, unicode_types


ID_START = {u"_"} | {uchr(x) for x in range(0xFFFF)
                     if category(uchr(x)) in ("LC", "Ll", "Lm", "Lo", "Lt", "Lu", "Nl")}
ID_CONTINUE = ID_START | {uchr(x) for x in range(0xFFFF)
                          if category(uchr(x)) in ("Mn", "Mc", "Nd", "Pc", "Sc")}

DOUBLE_QUOTE = u'"'
SINGLE_QUOTE = u"'"

ESCAPED_DOUBLE_QUOTE = u'\\"'
ESCAPED_SINGLE_QUOTE = u"\\'"

X_ESCAPE = re_compile(r"(\\x([0-9a-f]{2}))")
DOUBLE_QUOTED_SAFE = re_compile(r"([ -!#-\[\]-~]+)")
SINGLE_QUOTED_SAFE = re_compile(r"([ -&(-\[\]-~]+)")


class LabelSetView(SetView):

    def __init__(self, elements=(), selected=(), **kwargs):
        super(LabelSetView, self).__init__(frozenset(elements))
        self.__selected = tuple(selected)
        self.__kwargs = kwargs
        self.__encoder = CypherEncoder(**kwargs)

    def __repr__(self):
        if self.__selected:
            return "".join(":%s" % self.__encoder.encode_key(e) for e in self.__selected if e in self)
        else:
            return "".join(":%s" % self.__encoder.encode_key(e) for e in sorted(self))

    def __getattr__(self, element):
        if element in self.__selected:
            return self.__class__(self, self.__selected)
        else:
            return self.__class__(self, self.__selected + (element,))


class PropertyDictView(object):

    def __init__(self, items=(), selected=(), **kwargs):
        self.__items = dict(items)
        self.__selected = tuple(selected)
        self.__encoder = CypherEncoder(**kwargs)

    def __repr__(self):
        if self.__selected:
            properties = OrderedDict((key, self.__items[key]) for key in self.__selected if key in self.__items)
        else:
            properties = OrderedDict((key, self.__items[key]) for key in sorted(self.__items))
        return self.__encoder.encode_value(properties)

    def __getattr__(self, key):
        if key in self.__selected:
            return self.__class__(self.__items, self.__selected)
        else:
            return self.__class__(self.__items, self.__selected + (key,))

    def __len__(self):
        return len(self.__items)

    def __iter__(self):
        return iter(self.__items)

    def __contains__(self, key):
        return key in self.__items


class PropertySelector(object):

    def __init__(self, items=(), default_value=None, **kwargs):
        self.__items = dict(items)
        self.__default_value = default_value
        self.__encoder = CypherEncoder(**kwargs)
        self.__encoding = kwargs.get("encoding", "utf-8")

    def __getattr__(self, key):
        value = self.__items.get(key, self.__default_value)
        if isinstance(value, unicode_types):
            return value
        elif isinstance(value, string_types):
            return value.decode(self.__encoding)
        else:
            return self.__encoder.encode_value(value)


class CypherEncoder(object):

    __default_instance = None

    def __new__(cls, *args, **kwargs):
        if not kwargs:
            if cls.__default_instance is None:
                cls.__default_instance = super(CypherEncoder, cls).__new__(cls)
            return cls.__default_instance
        return super(CypherEncoder, cls).__new__(cls)

    encoding = "utf-8"
    quote = None
    sequence_separator = u", "
    key_value_separator = u": "
    node_template = u"{id}{labels} {properties}"
    related_node_template = u"{name}"
    relationship_template = u"{type} {properties}"

    def __init__(self, encoding=None, quote=None, sequence_separator=None, key_value_separator=None,
                 node_template=None, related_node_template=None, relationship_template=None):
        if encoding:
            self.encoding = encoding
        if quote:
            self.quote = quote
        if sequence_separator:
            self.sequence_separator = sequence_separator
        if key_value_separator:
            self.key_value_separator = key_value_separator
        if node_template:
            self.node_template = node_template
        if related_node_template:
            self.related_node_template = related_node_template
        if relationship_template:
            self.relationship_template = relationship_template

    @classmethod
    def is_safe_key(cls, key):
        key = ustr(key)
        return key[0] in ID_START and all(key[i] in ID_CONTINUE for i in range(1, len(key)))

    @classmethod
    def encode_key(cls, key):
        key = ustr(key)
        if not key:
            raise ValueError("Keys cannot be empty")
        if cls.is_safe_key(key):
            return key
        else:
            return u"`" + key.replace(u"`", u"``") + u"`"

    def encode_value(self, value):
        from py2neo.cypher import CypherExpression
        from py2neo.data import Node, Relationship, Path
        from neotime import Date, Time, DateTime, Duration
        if value is None:
            return u"null"
        if value is True:
            return u"true"
        if value is False:
            return u"false"
        if isinstance(value, CypherExpression):
            return value.value
        if isinstance(value, numeric_types):
            return ustr(value)
        if isinstance(value, string_types):
            return self.encode_string(value)
        if isinstance(value, Node):
            return self.encode_node(value)
        if isinstance(value, Relationship):
            return self.encode_relationship(value)
        if isinstance(value, Path):
            return self.encode_path(value)
        if isinstance(value, list):
            return self.encode_list(value)
        if isinstance(value, dict):
            return self.encode_map(value)
        if isinstance(value, Date):
            return u"date({})".format(self.encode_string(value.iso_format()))
        if isinstance(value, Time):
            return u"time({})".format(self.encode_string(value.iso_format()))
        if isinstance(value, DateTime):
            return u"datetime({})".format(self.encode_string(value.iso_format()))
        if isinstance(value, Duration):
            return u"duration({})".format(self.encode_string(value.iso_format()))
        raise TypeError("Cypher literal values of type %s.%s are not supported" %
                        (type(value).__module__, type(value).__name__))

    def encode_string(self, value):
        value = ustr(value)

        quote = self.quote
        if quote is None:
            num_single = value.count(u"'")
            num_double = value.count(u'"')
            quote = SINGLE_QUOTE if num_single <= num_double else DOUBLE_QUOTE

        if quote == SINGLE_QUOTE:
            escaped_quote = ESCAPED_SINGLE_QUOTE
            safe = SINGLE_QUOTED_SAFE
        elif quote == DOUBLE_QUOTE:
            escaped_quote = ESCAPED_DOUBLE_QUOTE
            safe = DOUBLE_QUOTED_SAFE
        else:
            raise ValueError("Unsupported quote character %r" % quote)

        if not value:
            return quote + quote

        parts = safe.split(value)
        for i in range(0, len(parts), 2):
            parts[i] = (X_ESCAPE.sub(u"\\\\u00\\2", parts[i].encode("unicode-escape").decode("utf-8")).
                        replace(quote, escaped_quote).replace(u"\\u0008", u"\\b").replace(u"\\u000c", u"\\f"))
        return quote + u"".join(parts) + quote

    def encode_list(self, values):
        return u"[" + self.sequence_separator.join(map(self.encode_value, values)) + u"]"

    def encode_map(self, values):
        return u"{" + self.sequence_separator.join(self.encode_key(key) + self.key_value_separator +
                                                   self.encode_value(value) for key, value in values.items()) + u"}"

    def encode_node(self, node):
        return self._encode_node(node, self.node_template)

    def encode_relationship(self, relationship):
        nodes = relationship.nodes
        return u"{}-{}->{}".format(
            self._encode_node(nodes[0], self.related_node_template),
            self._encode_relationship_detail(relationship, self.relationship_template),
            self._encode_node(nodes[-1], self.related_node_template),
        )

    def encode_path(self, path):
        encoded = []
        append = encoded.append
        nodes = path.nodes
        for i, relationship in enumerate(path.relationships):
            append(self._encode_node(nodes[i], self.related_node_template))
            related_nodes = relationship.nodes
            if self._node_id(related_nodes[0]) == self._node_id(nodes[i]):
                append(u"-")
                append(self._encode_relationship_detail(relationship, self.relationship_template))
                append(u"->")
            else:
                append(u"<-")
                append(self._encode_relationship_detail(relationship, self.relationship_template))
                append(u"-")
        append(self._encode_node(nodes[-1], self.related_node_template))
        return u"".join(encoded)

    @classmethod
    def _node_id(cls, node):
        return node.identity if hasattr(node, "identity") else node

    def _encode_node(self, node, template):
        return u"(" + template.format(
            id=u"" if node.identity is None else (u"_" + ustr(node.identity)),
            labels=LabelSetView(node.labels, encoding=self.encoding, quote=self.quote),
            properties=PropertyDictView(node, encoding=self.encoding, quote=self.quote),
            property=PropertySelector(node, u""),
            name=node.__name__,
        ).strip() + u")"

    def _encode_relationship_detail(self, relationship, template):
        return u"[" + template.format(
            id=u"" if relationship.identity is None else (u"_" + ustr(relationship.identity)),
            type=u":" + ustr(type(relationship).__name__),
            properties=PropertyDictView(relationship, encoding=self.encoding, quote=self.quote),
            property=PropertySelector(relationship, u""),
            name=relationship.__name__,
        ).strip() + u"]"
