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
Py2neo consists of several distinct regions of API, the heart of which
is the Graph API. This has evolved from the original, foundational API
included with early versions of the library, and remains relevant for
general purpose use today.

The :class:`.Graph` class represents a graph database exposed by a
Neo4j service running on a single instance or cluster, and which
provides access to a large portion of the most commonly used py2neo
features. The full DBMS is represented by a :class:`.GraphService`
object.

For convenience, all core functions and classes are exported from the
``py2neo`` root namespace. This includes all connectivity and database
management functionality as well as entity matching and core errors.
"""

from __future__ import absolute_import, print_function


__all__ = [

    # Project metadata
    "__author__",
    "__copyright__",
    "__email__",
    "__license__",
    "__package__",
    "__version__",

    # Basic connectivity
    "DEFAULT_PROTOCOL",
    "DEFAULT_SECURE",
    "DEFAULT_VERIFY",
    "DEFAULT_USER",
    "DEFAULT_PASSWORD",
    "DEFAULT_HOST",
    "DEFAULT_BOLT_PORT",
    "DEFAULT_HTTP_PORT",
    "DEFAULT_HTTPS_PORT",
    "ConnectionProfile",
    "ServiceProfile",

]


from os import getenv

from py2neo.addressing import Address
from py2neo.compat import Mapping, string_types, urlsplit
from py2neo.meta import get_metadata

from py2neo.database import *
from py2neo.errors import *
from py2neo.matching import *
from py2neo.data import *


__all__ += database.__all__
__all__ += errors.__all__
__all__ += matching.__all__
__all__ += data.__all__


metadata = get_metadata()

__author__ = metadata["author"]
__copyright__ = "2011, {}".format(metadata["author"])
__email__ = metadata["author_email"]
__license__ = metadata["license"]
__package__ = metadata["name"]
__version__ = metadata["version"]


NEO4J_URI = getenv("NEO4J_URI")
NEO4J_AUTH = getenv("NEO4J_AUTH")
NEO4J_SECURE = getenv("NEO4J_SECURE")
NEO4J_VERIFY = getenv("NEO4J_VERIFY")


DEFAULT_PROTOCOL = "bolt"
DEFAULT_SECURE = False
DEFAULT_VERIFY = True
DEFAULT_USER = "neo4j"
DEFAULT_PASSWORD = "password"
DEFAULT_HOST = "localhost"
DEFAULT_BOLT_PORT = 7687
DEFAULT_HTTP_PORT = 7474
DEFAULT_HTTPS_PORT = 7473


class ConnectionProfile(Mapping):
    """ Connection details for a Neo4j server.

    A connection profile holds a set of values that describe how to
    connect to, and authorise against, a particular Neo4j server.
    The set of values held within a profile are available as either
    object attributes (e.g. ``profile.uri``) or sub-items (e.g.
    ``profile["uri"]``).

    Profile instances are immutable, so can be safely hashed for
    inclusion within a set or as dictionary keys.

    :param profile:
        The base connection information, provided as a dictionary of
        settings, an existing :class:`.ConnectionProfile` object or a
        string URI. This value can also be :const:`None`, in which case
        default base settings are used.

    :param settings:
        Optional set of individual overrides.

    The full set of attributes and operations are described below.

    .. describe:: profile == other

        Return :const:`True` if `profile` and `other` are equal.

    .. describe:: profile != other

        Return :const:`True` if `profile` and `other` are unequal.

    .. describe:: hash(profile)

        Return a hash of `profile` based on its contained values.

    .. describe:: profile[key]

        Return a profile value using a string key.
        Key names are identical to the corresponding attribute names.

    .. describe:: len(profile)

        Return the number of values encoded within this profile.

    .. describe:: dict(profile)

        Coerce the profile into a dictionary of key-value pairs.

    """

    _keys = ("secure", "verify", "scheme", "user", "password", "address",
             "auth", "host", "port", "port_number", "protocol", "uri")

    _hash_keys = ("protocol", "secure", "verify", "user", "password", "address")

    def __init__(self, profile=None, **settings):
        # TODO: recognise IPv6 addresses explicitly
        self.__protocol = DEFAULT_PROTOCOL
        self.__secure = DEFAULT_SECURE
        self.__verify = DEFAULT_VERIFY
        self.__user = DEFAULT_USER
        self.__password = DEFAULT_PASSWORD
        self.__address = Address.parse("")

        self._apply_env_vars()

        if profile is None:
            pass
        elif isinstance(profile, string_types):
            self._apply_uri(profile)
        elif isinstance(profile, self.__class__):
            self._apply_settings(**{k: profile[k] for k in self._hash_keys})
        elif isinstance(profile, Mapping):
            self._apply_settings(**profile)
        else:
            raise TypeError("Profile %r is neither a ConnectionProfile "
                            "nor a string URI" % profile)

        self._apply_settings(**settings)

        if not self.address.port:
            addr = list(self.address)
            if self.protocol == "http":
                addr[1] = DEFAULT_HTTPS_PORT if self.secure else DEFAULT_HTTP_PORT
            else:
                addr[1] = DEFAULT_BOLT_PORT
            self.__address = Address(addr)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.uri)

    def __str__(self):
        return "«{}»".format(self.uri)

    def __getitem__(self, key):
        if key in self._keys:
            return getattr(self, key)
        else:
            raise KeyError(key)

    def __len__(self):
        return len(self._keys)

    def __iter__(self):
        return iter(self._keys)

    def _apply_env_vars(self):
        if NEO4J_URI:
            self._apply_uri(NEO4J_URI)
        if NEO4J_AUTH:
            self._apply_settings(auth=NEO4J_AUTH)
        if NEO4J_SECURE:
            self._apply_settings(secure=(NEO4J_SECURE == "1"))
        if NEO4J_VERIFY:
            self._apply_settings(verify=(NEO4J_VERIFY == "1"))

    def _apply_uri(self, uri):
        settings = {}
        parsed = urlsplit(uri)
        if parsed.scheme is not None:
            self._apply_scheme(parsed.scheme)
        if "@" in parsed.netloc:
            settings["address"] = parsed.netloc.partition("@")[-1]
        else:
            settings["address"] = parsed.netloc
        if parsed.username:
            settings["user"] = parsed.username
        if parsed.password:
            settings["password"] = parsed.password
        self._apply_settings(**settings)

    def _apply_scheme(self, scheme):
        if scheme == "https":
            protocol, ext = "http", "s"
        else:
            protocol, _, ext = scheme.partition("+")
        if ext == "":
            self._apply_settings(protocol=protocol, secure=False, verify=True)
        elif ext == "s":
            self._apply_settings(protocol=protocol, secure=True, verify=True)
        elif ext == "ssc":
            self._apply_settings(protocol=protocol, secure=True, verify=False)
        else:
            raise ValueError("Unknown scheme extension %r" % ext)

    def _apply_settings(self, uri=None, scheme=None, protocol=None, secure=None, verify=None,
                        address=None, host=None, port=None, port_number=None,
                        auth=None, user=None, password=None, **other):
        if uri:
            self._apply_uri(uri)

        if scheme:
            self._apply_scheme(scheme)
        if protocol:
            self._apply_protocol(protocol)
        if secure is not None:
            self.__secure = secure
        if verify is not None:
            self.__verify = verify

        if isinstance(address, tuple):
            self.__address = Address(address)
        elif address:
            self.__address = Address.parse(address)
        if host and port:
            self.__address = Address.parse("%s:%s" % (host, port))
        elif host:
            self.__address = Address.parse("%s:%s" % (host, self.port))
        elif port:
            self.__address = Address.parse("%s:%s" % (self.host, port))

        if isinstance(auth, tuple):
            self.__user, self.__password = auth
        elif auth:
            self.__user, _, self.__password = auth.partition(":")
        if user:
            self.__user = user
        if password:
            self.__password = password

        if other:
            raise ValueError("The following settings are not supported: %r" % other)

    def _apply_protocol(self, protocol):
        if protocol not in ("bolt", "http"):
            raise ValueError("Unknown protocol %r" % protocol)
        self.__protocol = protocol

    def __hash__(self):
        values = tuple(getattr(self, key) for key in self._hash_keys)
        return hash(values)

    def __eq__(self, other):
        self_values = tuple(getattr(self, key) for key in self._hash_keys)
        try:
            other_values = tuple(getattr(other, key) for key in self._hash_keys)
        except AttributeError:
            return False
        else:
            return self_values == other_values

    @property
    def secure(self):
        """ A flag for whether or not to apply security to the
        connection. If unspecified, and uninfluenced by environment
        variables, this will default to :const:`True`.
        """
        return self.__secure

    @property
    def verify(self):
        """ A flag for verification of remote server certificates.
        If unspecified, and uninfluenced by environment variables, this
        will default to :const:`True`.
        """
        return self.__verify

    @property
    def scheme(self):
        """ The URI scheme for contacting the remote server.
        If unspecified, and uninfluenced by environment variables, this
        will default to ``'bolt'``.
        """
        if self.secure and self.verify:
            return "https" if self.protocol == "http" else self.protocol + "+s"
        elif self.secure:
            return self.protocol + "+ssc"
        else:
            return self.protocol

    @property
    def user(self):
        """ The user as whom to authorise.
        If unspecified, and uninfluenced by environment variables, this
        will default to ``'neo4j'``.
        """
        return self.__user

    @property
    def password(self):
        """ The password which with to authorise.
        If unspecified, and uninfluenced by environment variables, this
        will default to ``'password'``.
        """
        return self.__password

    @property
    def address(self):
        """ The full socket :class:`.Address` of the remote server.
        If unspecified, and uninfluenced by environment variables, this
        will default to ``IPv4Address(('localhost', 7687))``.
        """
        return self.__address

    @property
    def auth(self):
        """ A 2-tuple of `(user, password)` representing the combined
        auth details. If unspecified, and uninfluenced by environment
        variables, this will default to ``('neo4j', 'password')``.
        """
        return self.user, self.password

    @property
    def host(self):
        """ The host name or IP address of the remote server.
        If unspecified, and uninfluenced by environment variables, this
        will default to ``'localhost'``.
        """
        return self.address.host

    @property
    def port(self):
        """ The port to which to connect on the remote server. This
        will be the correct port for the given :attr:`.protocol`.
        If unspecified, and uninfluenced by environment variables, this
        will default to ``7687`` (for Bolt traffic).
        """
        return self.address.port

    @property
    def port_number(self):
        """ A variant of :attr:`.port` guaranteed to be returned as a
        number. In some cases, the regular port value can be a string,
        this attempts to resolve or convert that value into a number.
        If unspecified, and uninfluenced by environment variables, this
        will default to ``7687`` (for Bolt traffic).
        """
        return self.address.port_number

    @property
    def protocol(self):
        """ The name of the underlying point-to-point protocol, derived
        from the URI scheme. This will either be ``'bolt'`` or
        ``'http'``, regardless of security and verification settings.
        If unspecified, and uninfluenced by environment variables, this
        will default to ``'bolt'``.
        """
        return self.__protocol

    @property
    def uri(self):
        """ A full URI for the profile. This generally includes all
        other information, excluding the password (for security
        reasons). If unspecified, and uninfluenced by environment
        variables, this will default to ``'bolt://neo4j@localhost:7687'``.
        """
        return "%s://%s@%s:%s" % (self.scheme, self.user, self.host, self.port)

    @classmethod
    def from_config_parser(cls, parser, section, prefix=None):
        prefix = str(prefix or "")
        settings = {}
        for name, value in parser.items(section):
            if name.startswith(prefix):
                settings[name[len(prefix):]] = value
        uri = settings.pop("uri", None)
        return cls(uri, **settings)

    @classmethod
    def from_file(cls, filenames, section, prefix=None):
        """ Load profile information from a configuration file.

        The required file format is described in the standard library
        ``configparser`` module, and is similar to that used in Windows
        INI files.

        :param filenames:
        :param section:
        :param prefix:
        :returns: :class:`ConnectionProfile` object created from the
            loaded configuration
        """
        from py2neo.compat import ConfigParser
        parser = ConfigParser()
        parser.read(filenames)
        return cls.from_config_parser(parser, section, prefix)

    def to_dict(self, include_password=False):
        """ Convert this profile to a dictionary, optionally including
        password information.

        :param include_password: if True then include the password in
            the return value, otherwise omit this information (default)
        """
        if include_password:
            return dict(self)
        else:
            return {key: value for key, value in self.items()
                    if key not in ("auth", "password")}


class ServiceProfile(ConnectionProfile):
    """ Connection details for a full Neo4j service, such as a cluster
    or single instance. This class extends :class:`.ConnectionProfile`
    so also inherits all of its attributes.
    """

    _keys = ConnectionProfile._keys + ("routing",)

    _hash_keys = ConnectionProfile._hash_keys + ("routing",)

    def __init__(self, profile=None, **settings):
        self.__routing = False
        super(ServiceProfile, self).__init__(profile, **settings)

    @property
    def scheme(self):
        if self.protocol == "bolt" and self.routing:
            protocol = "neo4j"
        else:
            protocol = self.protocol
        if self.secure and self.verify:
            return "https" if protocol == "http" else protocol + "+s"
        elif self.secure:
            return protocol + "+ssc"
        else:
            return protocol

    @property
    def routing(self):
        """ Routing flag
        """
        return self.__routing

    def _apply_protocol(self, protocol):
        if protocol == "neo4j":
            self.__routing = True
            super(ServiceProfile, self)._apply_protocol("bolt")
        else:
            super(ServiceProfile, self)._apply_protocol(protocol)

    def _apply_settings(self, uri=None, scheme=None, protocol=None, secure=None, verify=None,
                        address=None, host=None, port=None, port_number=None,
                        auth=None, user=None, password=None, **other):
        try:
            self.__routing = other.pop("routing")
        except KeyError:
            pass
        return super(ServiceProfile, self)._apply_settings(uri, scheme, protocol, secure, verify,
                                                           address, host, port, port_number,
                                                           auth, user, password, **other)
