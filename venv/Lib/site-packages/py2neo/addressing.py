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
This module provides classes for modelling IP addresses, based on tuples.
"""


from socket import AF_INET, AF_INET6

from py2neo.compat import xstr


class Address(tuple):
    """ Address of a machine on a network.
    """

    @classmethod
    def parse(cls, s, default_host=None, default_port=None):
        s = xstr(s)
        if s.startswith("["):
            # IPv6
            host, _, port = s[1:].rpartition("]")
            port = port.lstrip(":")
            try:
                port = int(port)
            except (TypeError, ValueError):
                pass
            return cls((host or default_host or "localhost",
                        port or default_port or 0, 0, 0))
        else:
            # IPv4
            host, _, port = s.partition(":")
            try:
                port = int(port)
            except (TypeError, ValueError):
                pass
            return cls((host or default_host or "localhost",
                        port or default_port or 0))

    def __new__(cls, iterable):
        if isinstance(iterable, cls):
            return iterable
        n_parts = len(iterable)
        inst = tuple.__new__(cls, iterable)
        if n_parts == 2:
            inst.__class__ = IPv4Address
        elif n_parts == 4:
            inst.__class__ = IPv6Address
        else:
            raise ValueError("Addresses must consist of either "
                             "two parts (IPv4) or four parts (IPv6)")
        return inst

    #: Address family (AF_INET or AF_INET6)
    family = None

    def __repr__(self):
        return "{}({!r})".format(self.__class__.__name__, tuple(self))

    @property
    def host(self):
        return self[0]

    @property
    def port(self):
        return self[1]

    @property
    def port_number(self):
        from socket import getservbyname
        if self.port == "bolt":
            # Special case, just because. The regular /etc/services
            # file doesn't contain this, but it can be found in
            # /usr/share/nmap/nmap-services if nmap is installed.
            from py2neo import DEFAULT_BOLT_PORT
            return DEFAULT_BOLT_PORT
        try:
            return getservbyname(self.port)
        except (IOError, OSError, TypeError):
            # OSError: service/proto not found
            # TypeError: getservbyname() argument 1 must be str, not X
            try:
                return int(self.port)
            except (TypeError, ValueError) as e:
                raise type(e)("Unknown port value %r" % self.port)


class IPv4Address(Address):
    """ Address subclass, specifically for IPv4 addresses.
    """

    family = AF_INET

    def __str__(self):
        return "{}:{}".format(*self)


class IPv6Address(Address):
    """ Address subclass, specifically for IPv6 addresses.
    """

    family = AF_INET6

    def __str__(self):
        return "[{}]:{}".format(*self)
