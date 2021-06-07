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
This module provides a convenience socket wrapper class
(:class:`.Wire`) for low-level network communication.
"""


from socket import SHUT_WR

from monotonic import monotonic
from six import raise_from

from py2neo.addressing import Address
from py2neo.compat import BaseRequestHandler


class Wire(object):
    """ Buffered socket wrapper for reading and writing bytes.
    """

    __closed = False

    __broken = False

    @classmethod
    def open(cls, address, timeout=None, keep_alive=False, on_broken=None):
        """ Open a connection to a given network :class:`.Address`.

        :param address:
        :param timeout:
        :param keep_alive:
        :param on_broken: callback for when the wire is broken after a
            successful connection has first been established (this does
            not trigger if the connection never opens successfully)
        :returns: :class:`.Wire` object
        :raises WireError: if connection fails to open
        """
        from socket import socket, SOL_SOCKET, SO_KEEPALIVE
        address = Address(address)
        s = socket(family=address.family)
        if keep_alive:
            s.setsockopt(SOL_SOCKET, SO_KEEPALIVE, 1)
        s.settimeout(timeout)
        try:
            s.connect(address)
        except (IOError, OSError) as error:
            raise_from(WireError("Cannot connect to %r" % (address,)), error)
        return cls(s, on_broken=on_broken)

    def __init__(self, s, on_broken=None):
        s.settimeout(None)  # ensure wrapped socket is in blocking mode
        self.__socket = s
        self.__active_time = monotonic()
        self.__bytes_received = 0
        self.__bytes_sent = 0
        self.__input = bytearray()
        self.__input_len = 0
        self.__output = bytearray()
        self.__on_broken = on_broken

    def secure(self, verify=True, hostname=None):
        """ Apply a layer of security onto this connection.
        """
        from ssl import SSLContext, SSLError
        try:
            # noinspection PyUnresolvedReferences
            from ssl import PROTOCOL_TLS
        except ImportError:
            from ssl import PROTOCOL_SSLv23
            context = SSLContext(PROTOCOL_SSLv23)
        else:
            context = SSLContext(PROTOCOL_TLS)
        if verify:
            from ssl import CERT_REQUIRED
            context.verify_mode = CERT_REQUIRED
            context.check_hostname = bool(hostname)
        else:
            from ssl import CERT_NONE
            context.verify_mode = CERT_NONE
        context.load_default_certs()
        try:
            self.__socket = context.wrap_socket(self.__socket, server_hostname=hostname)
        except (IOError, OSError) as error:
            # TODO: add connection failure/diagnostic callback
            if error.errno == 0:
                raise BrokenWireError("Peer closed connection during TLS handshake; "
                                      "server may not be configured for secure connections")
            else:
                raise WireError("Unable to establish secure connection with remote peer")
        else:
            self.__active_time = monotonic()

    def read(self, n):
        """ Read bytes from the network.
        """
        while self.__input_len < n:
            required = n - self.__input_len
            requested = max(required, 16384)
            try:
                received = self.__socket.recv(requested)
            except (IOError, OSError):
                self.__mark_broken("Wire broken")
            else:
                if received:
                    self.__active_time = monotonic()
                    new_bytes_received = len(received)
                    self.__input.extend(received)
                    self.__input_len += new_bytes_received
                    self.__bytes_received += new_bytes_received
                else:
                    self.__mark_broken("Network read incomplete "
                                       "(received %d of %d bytes)" %
                                       (self.__input_len, n))
        data = self.__input[:n]
        self.__input[:n] = []
        self.__input_len -= n
        return data

    def peek(self):
        """ Return any buffered unread data.
        """
        return self.__input

    def write(self, b):
        """ Write bytes to the output buffer.
        """
        self.__output.extend(b)

    def send(self, final=False):
        """ Send the contents of the output buffer to the network.
        """
        if self.__closed:
            raise WireError("Closed")
        sent = 0
        while self.__output:
            try:
                n = self.__socket.send(self.__output)
            except (IOError, OSError):
                self.__mark_broken("Wire broken")
            else:
                self.__active_time = monotonic()
                self.__bytes_sent += n
                self.__output[:n] = []
                sent += n
        if final:
            try:
                self.__socket.shutdown(SHUT_WR)
            except (IOError, OSError):
                self.__mark_broken("Wire broken")
        return sent

    def close(self):
        """ Close the connection.
        """
        try:
            self.__socket.close()
        except (IOError, OSError):
            self.__mark_broken("Wire broken")
        else:
            self.__closed = True

    @property
    def closed(self):
        """ Flag indicating whether this connection has been closed locally.
        """
        return self.__closed

    @property
    def broken(self):
        """ Flag indicating whether this connection has been closed remotely.
        """
        return self.__broken

    @property
    def local_address(self):
        """ The local :class:`.Address` to which this connection is bound.
        """
        return Address(self.__socket.getsockname())

    @property
    def remote_address(self):
        """ The remote :class:`.Address` to which this connection is bound.
        """
        return Address(self.__socket.getpeername())

    @property
    def bytes_sent(self):
        return self.__bytes_sent

    @property
    def bytes_received(self):
        return self.__bytes_received

    def __mark_broken(self, message):
        idle_time = monotonic() - self.__active_time
        message += (" after %.01fs idle (%r bytes sent, "
                    "%r bytes received)" % (idle_time,
                                            self.__bytes_sent,
                                            self.__bytes_received))
        if callable(self.__on_broken):
            self.__on_broken(message)
        self.__broken = True
        raise BrokenWireError(message, idle_time=idle_time,
                              bytes_sent=self.__bytes_sent,
                              bytes_received=self.__bytes_received)


class WireRequestHandler(BaseRequestHandler):
    """ Base handler for use with the `socketserver` module that wraps
    the request attribute as a :class:`.Wire` object.
    """

    __wire = None

    @property
    def wire(self):
        if self.__wire is None:
            self.__wire = Wire(self.request)
        return self.__wire


class WireError(OSError):
    """ Raised when a connection error occurs.

    :param idle_time:
    :param bytes_sent:
    :param bytes_received:
    """

    def __init__(self, *args, **kwargs):
        super(WireError, self).__init__(*args)
        self.idle_time = kwargs.get("idle_time", None)
        self.bytes_sent = kwargs.get("bytes_sent", 0)
        self.bytes_received = kwargs.get("bytes_received", 0)


class BrokenWireError(WireError):
    """ Raised when a connection is broken by the network or remote peer.
    """
