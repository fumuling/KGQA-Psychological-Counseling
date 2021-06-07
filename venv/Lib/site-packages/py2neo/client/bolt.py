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
This module contains client implementations for the Bolt messaging
protocol. It contains a base :class:`.Bolt` class, which is a type of
:class:`~py2neo.client.Connection`, and which is further extended by a
separate class for each protocol version. :class:`.Bolt1` extends
:class:`.Bolt`, :class:`.Bolt2` extends :class:`.Bolt1`, and so on.

Each subclass therefore introduces deltas atop the previous protocol
version. This reduces duplication of client code at the expense of more
work should an old protocol version be removed.

As of Bolt 4.0, the protocol versioning scheme aligned directly with
that of Neo4j itself. Prior to this, the protocol was versioned with a
single integer that did not necessarily increment in line with each
Neo4j product release.
"""


__all__ = [
    "BoltMessageReader",
    "BoltMessageWriter",
    "Bolt",
    "Bolt1",
    "Bolt2",
    "Bolt3",
    "Bolt4x0",
    "Bolt4x1",
    "BoltTransactionRef",
    "BoltResult",
    "BoltResponse",
]


from collections import deque
from io import BytesIO
from itertools import islice
from logging import getLogger
from struct import pack as struct_pack, unpack as struct_unpack

from six import PY2, raise_from

from py2neo import ConnectionProfile
from py2neo.client import bolt_user_agent, Connection, TransactionRef, Result, Bookmark
from py2neo.client.packstream import pack_into, UnpackStream, PackStreamHydrant
from py2neo.errors import (Neo4jError,
                           ConnectionUnavailable,
                           ConnectionBroken,
                           ProtocolError)
from py2neo.wiring import Wire, WireError, BrokenWireError


BOLT_SIGNATURE = b"\x60\x60\xB0\x17"


log = getLogger(__name__)


class BoltMessageReader(object):

    def __init__(self, wire):
        self.wire = wire
        if PY2:
            self.read_message = self.read_message_py2

    def read_message(self):
        chunks = []
        while True:
            try:
                hi, lo = self.wire.read(2)
            except WireError as error:
                raise_from(ConnectionBroken("Failed to read message"), error)
            else:
                if hi == lo == 0:
                    break
                size = hi << 8 | lo
                chunks.append(self.wire.read(size))
        message = b"".join(chunks)
        _, n = divmod(message[0], 0x10)
        try:
            unpacker = UnpackStream(message, offset=2)
            fields = [unpacker.unpack() for _ in range(n)]
        except ValueError as error:
            raise_from(ProtocolError("Bad message content"), error)
        else:
            return message[1], fields

    def read_message_py2(self):
        chunks = []
        while True:
            try:
                hi, lo = self.wire.read(2)
            except WireError as error:
                raise_from(ConnectionBroken("Failed to read message"), error)
            else:
                if hi == lo == 0:
                    break
                size = hi << 8 | lo
                chunks.append(self.wire.read(size))
        message = bytearray(b"".join(map(bytes, chunks)))
        _, n = divmod(message[0], 0x10)
        try:
            unpacker = UnpackStream(message, offset=2)
            fields = [unpacker.unpack() for _ in range(n)]
        except ValueError as error:
            raise_from(ProtocolError("Bad message content"), error)
        else:
            return message[1], fields

    def peek_message(self):
        """ If another complete message exists, return the tag for
        that message, otherwise return `None`.
        """
        data = self.wire.peek()
        p = 0

        def peek_chunk():
            q = p + 2
            if q < len(data):
                size, = struct_unpack(">H", data[p:q])
                r = q + size
                if r < len(data):
                    return size
            return -1

        while True:
            chunk_size = peek_chunk()
            if chunk_size == -1:
                return None
            elif chunk_size == 0:
                return data[3]
            else:
                p += 2 + chunk_size


class BoltMessageWriter(object):

    def __init__(self, wire, protocol_version):
        self.wire = wire
        self.protocol_version = protocol_version
        self.buffer = BytesIO()

    def _write_chunk(self, data):
        size = len(data)
        self.wire.write(struct_pack(">H", size))
        if size > 0:
            self.wire.write(data)
        return size

    def write_message(self, tag, fields):
        buffer = self.buffer
        buffer.seek(0)
        buffer.write(bytearray([0xB0 + len(fields), tag]))
        pack_into(buffer, *fields, version=self.protocol_version)
        buffer.truncate()
        buffer.seek(0)
        while self._write_chunk(buffer.read(0x7FFF)):
            pass

    def send(self, final=False):
        try:
            return self.wire.send(final=final)
        except WireError as error:
            raise_from(ConnectionBroken("Failed to send Bolt messages"), error)


class Bolt(Connection):
    """ This is the base class for Bolt client connections. This class
    is not intended to be instantiated directly, but contains an
    :meth:`~Bolt.open` factory method that returns an instance of the
    appropriate subclass, once a connection has been successfully
    established.
    """

    protocol_version = ()

    messages = {}

    __local_port = 0

    @classmethod
    def _walk_subclasses(cls):
        for subclass in cls.__subclasses__():
            assert issubclass(subclass, cls)  # for the benefit of the IDE
            yield subclass
            for k in subclass._walk_subclasses():
                yield k

    @classmethod
    def _get_subclass(cls, protocol_version):
        for subclass in cls._walk_subclasses():
            if subclass.protocol_version == protocol_version:
                return subclass
        raise TypeError("Unsupported protocol version %d.%d" % protocol_version)

    @classmethod
    def default_hydrant(cls, profile, graph):
        return PackStreamHydrant(graph)

    @classmethod
    def _proposed_versions(cls, data, offset=0):
        candidates = []
        for i in range(offset, offset + 16, 4):
            delta = data[i + 1]
            minor = data[i + 2]
            major = data[i + 3]
            for v in range(minor, minor - delta - 1, -1):
                candidates.append((major, v))
        return candidates

    @classmethod
    def accept(cls, wire, min_protocol_version=None):
        data = wire.read(20)
        if data[0:4] != BOLT_SIGNATURE:
            raise ProtocolError("Incoming connection did not provide Bolt signature")
        for major, minor in cls._proposed_versions(data, offset=4):
            if min_protocol_version and (major, minor) < min_protocol_version:
                continue
            try:
                subclass = Bolt._get_subclass((major, minor))
            except TypeError:
                continue
            else:
                wire.write(bytearray([0, 0, minor, major]))
                wire.send()
                return subclass(wire, ConnectionProfile(address=wire.remote_address))
        raise TypeError("Unable to agree supported protocol version")

    @classmethod
    def open(cls, profile=None, user_agent=None, on_release=None, on_broken=None):
        """ Open and authenticate a Bolt connection.

        :param profile: :class:`.ConnectionProfile` detailing how and
            where to connect
        :param user_agent:
        :param on_release:
        :param on_broken:
        :returns: :class:`.Bolt` connection object
        :raises: :class:`.ConnectionUnavailable` if a connection cannot
            be opened, or a protocol version cannot be agreed
        """
        if profile is None:
            profile = ConnectionProfile(scheme="bolt")
        try:
            wire = cls._connect(profile, on_broken=on_broken)
            protocol_version = cls._handshake(wire)
            subclass = cls._get_subclass(protocol_version)
            if subclass is None:
                raise TypeError("Unable to agree supported protocol version")
            bolt = subclass(wire, profile, on_release=on_release)
            bolt._hello(user_agent or bolt_user_agent())
            return bolt
        except (TypeError, WireError) as error:
            raise_from(ConnectionUnavailable("Cannot open connection to %r" % profile), error)

    @classmethod
    def _connect(cls, profile, on_broken):
        log.debug("[#%04X] C: (Dialing <%s>)", 0, profile.address)
        wire = Wire.open(profile.address, keep_alive=True, on_broken=on_broken)
        local_port = wire.local_address.port_number
        log.debug("[#%04X] S: (Accepted)", local_port)
        if profile.secure:
            log.debug("[#%04X] C: (Securing connection)", local_port)
            wire.secure(verify=profile.verify, hostname=profile.host)
        return wire

    @classmethod
    def _handshake(cls, wire):
        local_port = wire.local_address.port_number
        log.debug("[#%04X] C: <BOLT>", local_port)
        wire.write(BOLT_SIGNATURE)
        log.debug("[#%04X] C: <PROTOCOL> 4.3~4.0 | 4.0 | 3.0 | 2.0", local_port)
        wire.write(b"\x00\x03\x03\x04"      # Neo4j 4.3.x and Neo4j 4.2, 4.1, 4.0 (patched)
                   b"\x00\x00\x00\x04"      # Neo4j 4.2, 4.1, 4.0 (unpatched)
                   b"\x00\x00\x00\x03"      # Neo4j 3.5.x
                   b"\x00\x00\x00\x02")     # Neo4j 3.4.x
        wire.send()
        v = bytearray(wire.read(4))
        if v == bytearray([0, 0, 0, 0]):
            raise TypeError("Unable to negotiate compatible protocol version")
        log.debug("[#%04X] S: <PROTOCOL> %d.%d", local_port, v[-1], v[-2])
        return v[-1], v[-2]

    def __init__(self, wire, profile, on_release=None):
        super(Bolt, self).__init__(profile, on_release=on_release)
        self._wire = wire
        self.__local_port = wire.local_address.port_number

    def read_message(self):
        """ Read a response message from the input queue.

        :returns: 2-tuple of (tag, fields)
        """
        raise NotImplementedError

    def write_message(self, tag, fields):
        """ Write a request to the output queue.

        :param tag:
            Unique message type identifier.

        :param fields:
            Message payload.

        """
        raise NotImplementedError

    def send(self):
        """ Send all messages in the output queue to the network.
        """
        raise NotImplementedError

    def close(self):
        """ Close the connection.
        """
        if self.closed or self.broken:
            return
        try:
            self._goodbye()
            self._wire.close()
        except BrokenWireError:
            return
        log.debug("[#%04X] C: (Hanging up)", self.local_port)

    @property
    def closed(self):
        return self._wire.closed

    @property
    def broken(self):
        return self._wire.broken

    @property
    def local_port(self):
        return self.__local_port

    @property
    def bytes_sent(self):
        return self._wire.bytes_sent

    @property
    def bytes_received(self):
        return self._wire.bytes_received

    def _assert_open(self):
        if self.closed:
            raise ConnectionUnavailable("Connection has been closed")
        if self.broken:
            raise ConnectionUnavailable("Connection is broken")

    def supports_multi(self):
        return self.protocol_version >= (4, 0)


class Bolt1(Bolt):

    protocol_version = (1, 0)

    messages = {
        0x01: "INIT",
        0x0E: "ACK_FAILURE",
        0x0F: "RESET",
        0x10: "RUN",
        0x2F: "DISCARD_ALL",
        0x3F: "PULL_ALL",
        0x70: "SUCCESS",
        0x71: "RECORD",
        0x7E: "IGNORED",
        0x7F: "FAILURE",
    }

    def __init__(self, wire, profile, on_release=None):
        super(Bolt1, self).__init__(wire, profile, on_release=on_release)
        self._reader = BoltMessageReader(wire)
        self._writer = BoltMessageWriter(wire, self.protocol_version)
        self._responses = deque()
        self._transaction = None
        self._metadata = {}

    @property
    def transaction(self):
        return self._transaction

    def bookmark(self):
        return self._metadata.get("bookmark")

    def _hello(self, user_agent):
        self._assert_open()
        extra = {"scheme": "basic",
                 "principal": self.profile.user,
                 "credentials": self.profile.password}
        clean_extra = dict(extra)
        clean_extra.update({"credentials": "*******"})
        response = self.append_message(0x01, user_agent, extra, vital=True)
        self.send()
        self._fetch()
        self._audit(response)
        self.connection_id = response.metadata.get("connection_id")
        self.server_agent = response.metadata.get("server")
        if not self.server_agent.startswith("Neo4j/"):
            raise ProtocolError("Unexpected server agent {!r}".format(self.server_agent))

    def reset(self, force=False):
        self._assert_open()
        if force or self._transaction:
            response = self.append_message(0x0F, vital=True)
            self._sync(response)
            self._audit(response)
            self._transaction = None

    def _set_transaction(self, graph_name=None, readonly=False, after=None, metadata=None, timeout=None):
        self._assert_open()
        self._assert_no_transaction()
        if graph_name and not self.supports_multi():
            raise TypeError("This Neo4j installation does not support "
                            "named graphs")
        if metadata:
            raise TypeError("Transaction metadata not supported until Bolt v3")
        if timeout:
            raise TypeError("Transaction timeout not supported until Bolt v3")
        self._transaction = BoltTransactionRef(self, graph_name, readonly, after)

    def auto_run(self, cypher, parameters=None, graph_name=None, readonly=False,
                 # after=None, metadata=None, timeout=None
                 ):
        self._set_transaction(graph_name, readonly=readonly,
                              # after, metadata, timeout
                              )
        return self._run(graph_name, cypher, parameters or {}, final=True)

    def begin(self, graph_name, readonly=False,
              # after=None, metadata=None, timeout=None
              ):
        self._set_transaction(graph_name, readonly=readonly,
                              # after, metadata, timeout
                              )
        responses = (self.append_message(0x10, "BEGIN", self._transaction.extra),
                     self.append_message(0x2F))
        try:
            self._sync(*responses)
        except BrokenWireError as error:
            raise_from(ConnectionBroken("Transaction could not begin "
                                        "due to disconnection"), error)
        else:
            self._audit(self._transaction)
            return self._transaction

    def commit(self, tx):
        self._assert_open()
        self._assert_transaction_open(tx)
        self._transaction.set_complete()
        try:
            self._sync(self.append_message(0x10, "COMMIT", {}),
                       self.append_message(0x2F))
        except BrokenWireError as error:
            tx.mark_broken()
            raise_from(ConnectionBroken("Transaction broken by disconnection "
                                        "during commit"), error)
        else:
            try:
                self._audit(self._transaction)
            except Neo4jError as error:
                tx.mark_broken()
                raise_from(ConnectionBroken("Failed to commit transaction"), error)
            else:
                return Bookmark()

    def rollback(self, tx):
        self._assert_open()
        self._assert_transaction_open(tx)
        self._transaction.set_complete()
        try:
            self._sync(self.append_message(0x10, "ROLLBACK", {}),
                       self.append_message(0x2F))
        except BrokenWireError as error:
            tx.mark_broken()
            raise_from(ConnectionBroken("Transaction broken by disconnection "
                                        "during rollback"), error)
        else:
            try:
                self._audit(self._transaction)
            except Neo4jError as error:
                tx.mark_broken()
                raise_from(ConnectionBroken("Failed to rollback transaction"), error)
            else:
                return Bookmark()

    def run(self, tx, cypher, parameters=None):
        self._assert_open()
        if tx is None:
            raise ValueError("Transaction is None")
        self._assert_transaction_open(tx)
        return self._run(tx.graph_name, cypher, parameters or {})

    def _run(self, graph_name, cypher, parameters, extra=None, final=False):
        # TODO: limit logging for big parameter sets (e.g. bulk import)
        response = self.append_message(0x10, cypher, parameters)
        result = BoltResult(self._transaction, self, response)
        self._transaction.append(result, final=final)
        return result

    def pull(self, result, n=-1, capacity=-1):
        print(result, n, capacity)
        self._assert_open()
        self._assert_result_consumable(result)
        if n != -1:
            raise IndexError("Flow control is not available in this version of Neo4j")
        response = self.append_message(0x3F, capacity=capacity)
        result.append(response, final=True)
        try:
            self._sync(response)
        except BrokenWireError as error:
            if self._transaction:
                self._transaction.mark_broken()
            raise_from(ConnectionBroken("Transaction broken by disconnection "
                                        "during pull"), error)
        else:
            self._audit(self._transaction)
            return response

    def discard(self, result):
        self._assert_open()
        self._assert_result_consumable(result)
        response = self.append_message(0x2F)
        result.append(response, final=True)
        try:
            self._sync(response)
        except BrokenWireError as error:
            if self._transaction:
                self._transaction.mark_broken()
            raise_from(ConnectionBroken("Transaction broken by disconnection "
                                        "during discard"), error)
        else:
            self._audit(self._transaction)
            return response

    def _get_routing_info(self, graph_name, query, parameters):
        try:
            result = self.auto_run(query, parameters, graph_name)
            self.pull(result)
            while True:
                record = result.take()
                if record is None:
                    break
                addresses = {}
                for a in record[1]:
                    addresses[a["role"]] = [ConnectionProfile(self.profile, address=address)
                                            for address in a["addresses"]]
                return (addresses.get("ROUTE", []),
                        addresses.get("READ", []),
                        addresses.get("WRITE", []),
                        record[0])
        except Neo4jError as error:
            if error.title == "ProcedureNotFound":
                raise_from(TypeError("Neo4j service does not support routing"), error)
            else:
                raise

    def route(self, graph_name=None, context=None):
        #
        # Bolt 1 (< Neo4j 3.2) (Clusters only)
        #     cx.run("CALL dbms.cluster.routing.getServers"
        #
        #
        # Bolt 1 (>= Neo4j 3.2) / Bolt 2 / Bolt 3 (Clusters only)
        #     cx.run("CALL dbms.cluster.routing.getRoutingTable($context)",
        #    {"context": self.routing_context}
        #
        if graph_name is not None:
            raise TypeError("Multiple graph databases are not available prior to Bolt v4")
        query = "CALL dbms.cluster.routing.getRoutingTable($context)"
        parameters = {"context": context or {}}
        return self._get_routing_info(None, query, parameters)

    def sync(self, result):
        self.send()
        self._wait(result.last())
        self._audit(result)

    def fetch(self, result):
        return result.take()

    def _assert_no_transaction(self):
        if self._transaction:
            raise TypeError("Cannot open multiple simultaneous transactions "
                            "on a Bolt connection")

    def _assert_transaction_open(self, tx):
        if tx is not self._transaction:
            raise ValueError("Transaction %r is not open on this connection", tx)
        if tx.broken:
            raise ValueError("Transaction is broken")

    def _assert_result_consumable(self, result):
        try:
            tx = result.transaction
        except AttributeError:
            raise TypeError("Result object is unusable")
        if result is not tx.last():
            raise TypeError("Random query access is not supported before Bolt 4.0")
        if result.complete():
            raise IndexError("Result is fully consumed")

    def _log_message(self, port, tag, fields):
        try:
            name = self.messages[tag]
        except KeyError:
            log.debug("[#%04X] ?: (Unexpected protocol message #%02X)",
                      port, tag)
        else:
            peer = "C" if tag < 0x70 else "S"
            n_fields = len(fields)
            if n_fields == 0:
                log.debug("[#%04X] %s: %s", port, peer, name)
            elif n_fields == 1:
                log.debug("[#%04X] %s: %s %r", port, peer, name, fields[0])
            elif n_fields == 2:
                log.debug("[#%04X] %s: %s %r %r", port, peer, name,
                          fields[0], fields[1])
            elif n_fields == 3:
                log.debug("[#%04X] %s: %s %r %r %r", port, peer, name,
                          fields[0], fields[1], fields[2])
            else:
                log.debug("[#%04X] %s: %s %r %r %r ...", port, peer, name,
                          fields[0], fields[1], fields[2])

    def read_message(self):
        tag, fields = self._reader.read_message()
        if tag == 0x71:
            # If a RECORD is received, check for more records
            # in the buffer immediately following, and log and
            # add them all at the same time
            while self._reader.peek_message() == 0x71:
                _, extra_fields = self._reader.read_message()
                fields.extend(extra_fields)
        self._log_message(self.local_port, tag, fields)
        return tag, fields

    def write_message(self, tag, fields=()):
        if tag == 0x01:
            self._writer.write_message(tag, fields)
            meadows = []
            for field in fields:
                if isinstance(field, dict) and "credentials" in field:
                    meadows.append(dict(field, credentials="*******"))
                else:
                    meadows.append(field)
            self._log_message(self.local_port, tag, meadows)
        elif tag == 0x71:
            preview = []
            for field in fields:
                self._writer.write_message(tag, [field])
                if len(preview) < 4:
                    preview.append(field)
            self._log_message(self.local_port, tag, preview)
        else:
            self._writer.write_message(tag, fields)
            self._log_message(self.local_port, tag, fields)

    def append_message(self, tag, *fields, **kwargs):
        """ Write a request to the output queue.

        :param tag:
            Unique message type identifier.

        :param fields:
            Message payload.

        :param kwargs
            - capacity: The preferred max number of records that a
              response can hold.
            - vital: If true, this should trigger the connection to
              close on failure. Vital responses cannot be ignored.

        """
        capacity = kwargs.get("capacity", None)
        if capacity is None:
            capacity = -1
        vital = kwargs.get("vital", None)
        if vital is None:
            vital = False
        self.write_message(tag, fields)
        response = BoltResponse(capacity=capacity, vital=vital)
        self._responses.append(response)
        return response

    def send(self, final=False):
        sent = self._writer.send(final=final)
        if sent:
            log.debug("[#%02X] C: (Sent %r bytes)", self.local_port, sent)

    def _fetch(self):
        """ Fetch and process the next incoming message.

        This method does not raise an exception on receipt of a
        FAILURE message. Instead, it sets the response (and
        consequently the parent query and transaction) to a failed
        state. It is the responsibility of the caller to convert this
        failed state into an exception.
        """
        tag, fields = self.read_message()
        if tag == 0x70:
            self._responses.popleft().set_success(**fields[0])
            self._metadata.update(fields[0])
        elif tag == 0x71:
            self._responses[0].add_records(fields)
        elif tag == 0x7F:
            rs = self._responses.popleft()
            rs.set_failure(**fields[0])
            if rs.vital:
                self._wire.close()
        elif tag == 0x7E and not self._responses[0].vital:
            self._responses.popleft().set_ignored()
        else:
            self._wire.close()
            raise ProtocolError("Unexpected protocol message #%02X", tag)

    def _wait(self, response):
        """ Read all incoming responses up to and including a
        particular response.

        This method calls fetch, but does not raise an exception on
        FAILURE.
        """
        while not response.full() and not response.done():
            self._fetch()

    def _sync(self, *responses):
        self.send()
        for response in responses:
            self._wait(response)

    def _audit(self, task):
        """ Checks a specific task (response, result or transaction)
        for failure, raising an exception if one is found.

        :raise BoltFailure:
        """
        if task is None:
            return
        try:
            task.audit()
        except Neo4jError:
            self.reset(force=True)
            raise
        finally:
            # On 1 Apr 2021, this was moved here from _wait. Because
            # _wait is generally called before _audit, the post-failure
            # reset would previously happen *after* the connection was
            # released back into the pool. If a new thread picked up
            # that connection *before* this has been processed, the
            # write buffer was muddied by the competing activities.
            # Putting it here ensures that release is only ever done
            # after such a reset.
            if not self._transaction:
                self.release()


class Bolt2(Bolt1):

    protocol_version = (2, 0)

    messages = Bolt1.messages


class Bolt3(Bolt2):

    protocol_version = (3, 0)

    messages = {
        0x01: "HELLO",
        0x02: "GOODBYE",
        0x0F: "RESET",
        0x10: "RUN",
        0x11: "BEGIN",
        0x12: "COMMIT",
        0x13: "ROLLBACK",
        0x2F: "DISCARD_ALL",
        0x3F: "PULL_ALL",
        0x70: "SUCCESS",
        0x71: "RECORD",
        0x7E: "IGNORED",
        0x7F: "FAILURE",
    }

    def __init__(self, wire, profile, on_release=None):
        super(Bolt3, self).__init__(wire, profile, on_release)
        self._polite = False

    def _hello(self, user_agent):
        self._assert_open()
        extra = {"user_agent": user_agent,
                 "scheme": "basic",
                 "principal": self.profile.user,
                 "credentials": self.profile.password}
        response = self.append_message(0x01, extra, vital=True)
        self.send()
        self._fetch()
        self._audit(response)
        self.server_agent = response.metadata.get("server")
        self.connection_id = response.metadata.get("connection_id")
        self._polite = True

    def _goodbye(self):
        if self._polite:
            self.write_message(0x02)
            self.send(final=True)

    def auto_run(self, cypher, parameters=None, graph_name=None, readonly=False,
                 # after=None, metadata=None, timeout=None
                 ):
        self._assert_open()
        self._assert_no_transaction()
        self._transaction = BoltTransactionRef(self, graph_name, readonly,
                                               # after, metadata, timeout
                                               )
        return self._run(graph_name, cypher, parameters or {},
                           self._transaction.extra, final=True)

    def begin(self, graph_name, readonly=False,
              # after=None, metadata=None, timeout=None
              ):
        self._assert_open()
        self._assert_no_transaction()
        self._transaction = BoltTransactionRef(self, graph_name, readonly,
                                               # after, metadata, timeout
                                               )
        response = self.append_message(0x11, self._transaction.extra)
        try:
            self._sync(response)
        except BrokenWireError as error:
            raise_from(ConnectionBroken("Transaction could not begin "
                                        "due to disconnection"), error)
        else:
            self._audit(self._transaction)
            return self._transaction

    def commit(self, tx):
        self._assert_open()
        self._assert_transaction_open(tx)
        self._transaction.set_complete()
        response = self.append_message(0x12)
        try:
            self._sync(response)
        except BrokenWireError as error:
            tx.mark_broken()
            raise_from(ConnectionBroken("Transaction broken by disconnection "
                                        "during commit"), error)
        else:
            try:
                self._audit(self._transaction)
            except Neo4jError as error:
                tx.mark_broken()
                raise_from(ConnectionBroken("Failed to commit transaction"), error)
            else:
                return Bookmark(response.metadata.get("bookmark"))

    def rollback(self, tx):
        self._assert_open()
        self._assert_transaction_open(tx)
        self._transaction.set_complete()
        response = self.append_message(0x13)
        try:
            self._sync(response)
        except BrokenWireError as error:
            tx.mark_broken()
            raise_from(ConnectionBroken("Transaction broken by disconnection "
                                        "during rollback"), error)
        else:
            try:
                self._audit(self._transaction)
            except Neo4jError as error:
                tx.mark_broken()
                raise_from(ConnectionBroken("Failed to rollback transaction"), error)
            else:
                return Bookmark(response.metadata.get("bookmark"))

    def _run(self, graph_name, cypher, parameters, extra=None, final=False):
        response = self.append_message(0x10, cypher, parameters, extra or {})
        result = BoltResult(self._transaction, self, response)
        self._transaction.append(result, final=final)
        return result


class Bolt4x0(Bolt3):

    protocol_version = (4, 0)

    messages = {
        0x01: "HELLO",
        0x02: "GOODBYE",
        0x0F: "RESET",
        0x10: "RUN",
        0x11: "BEGIN",
        0x12: "COMMIT",
        0x13: "ROLLBACK",
        0x2F: "DISCARD",
        0x3F: "PULL",
        0x70: "SUCCESS",
        0x71: "RECORD",
        0x7E: "IGNORED",
        0x7F: "FAILURE",
    }

    def _assert_result_consumable(self, result):
        try:
            tx = result.transaction
        except AttributeError:
            raise TypeError("Result object is unusable")
        if result.complete():
            raise IndexError("Result is fully consumed")
        if result.has_more_records():
            if result is tx.last():
                return -1
            else:
                return tx.index(result)
        else:
            raise IndexError("Result is fully consumed")

    def pull(self, result, n=-1, capacity=-1):
        self._assert_open()
        qid = self._assert_result_consumable(result)
        args = {"n": n, "qid": qid}
        response = self.append_message(0x3F, args, capacity=capacity)
        result.append(response, final=(n == -1))
        try:
            self._sync(response)
        except BrokenWireError as error:
            result.transaction.mark_broken()
            raise_from(ConnectionBroken("Transaction broken by disconnection "
                                        "during pull"), error)
        else:
            self._audit(self._transaction)
            return response

    def discard(self, result):
        self._assert_open()
        qid = self._assert_result_consumable(result)
        args = {"n": -1, "qid": qid}
        response = self.append_message(0x2F, args)
        result.append(response, final=True)
        try:
            self._sync(response)
        except BrokenWireError as error:
            result.transaction.mark_broken()
            raise_from(ConnectionBroken("Transaction broken by disconnection "
                                        "during discard"), error)
        else:
            self._audit(self._transaction)
            return response

    def route(self, graph_name=None, context=None):
        # In Neo4j 4.0 and above, routing is available for all
        # topologies, standalone or clustered.
        context = dict(context or {})
        context["address"] = str(self.profile.address)
        if graph_name is None:
            # Default database
            query = "CALL dbms.routing.getRoutingTable($context)"
            parameters = {"context": context}
        else:
            # Named database
            query = "CALL dbms.routing.getRoutingTable($context, $database)"
            parameters = {"context": context, "database": graph_name}
        return self._get_routing_info("system", query, parameters)


class Bolt4x1(Bolt4x0):

    protocol_version = (4, 1)

    messages = Bolt4x0.messages


class Bolt4x2(Bolt4x1):

    protocol_version = (4, 2)

    messages = Bolt4x1.messages


class Bolt4x3(Bolt4x2):

    protocol_version = (4, 3)

    messages = {
        0x01: "HELLO",
        0x02: "GOODBYE",
        0x0F: "RESET",
        0x10: "RUN",
        0x11: "BEGIN",
        0x12: "COMMIT",
        0x13: "ROLLBACK",
        0x2F: "DISCARD",
        0x3F: "PULL",
        0x66: "ROUTE",
        0x70: "SUCCESS",
        0x71: "RECORD",
        0x7E: "IGNORED",
        0x7F: "FAILURE",
    }


class Task(object):

    def done(self):
        raise NotImplementedError

    def failed(self):
        raise NotImplementedError

    def audit(self):
        raise NotImplementedError


class ItemizedTask(Task):
    """ This class represents a form of dynamic checklist. Items may
    be added, up to a "final" item which marks the list as complete.
    Each item may then be marked as done.
    """

    def __init__(self):
        self._items = []
        self._items_len = 0
        self._complete = False

    def __bool__(self):
        return not self.done() and not self.failed()

    __nonzero__ = __bool__

    def index(self, item):
        return self._items.index(item)

    def items(self):
        return iter(self._items)

    def append(self, item, final=False):
        self._items.append(item)
        self._items_len += 1
        if final:
            self.set_complete()

    def set_complete(self):
        self._complete = True

    def complete(self):
        """ Flag to indicate whether all items have been appended to
        this task, whether or not they are done.
        """
        return self._complete

    def first(self):
        try:
            return self._items[0]
        except IndexError:
            return None

    def last(self):
        try:
            return self._items[-1]
        except IndexError:
            return None

    def done(self):
        """ Flag to indicate whether the list of items is complete and
        all items are done.
        """
        if self._complete:
            try:
                last = self._items[-1]
            except IndexError:
                return True
            else:
                return last.done()
        else:
            return False

    def failed(self):
        return any(item.failed() for item in self._items)

    def audit(self):
        for item in self._items:
            item.audit()


class BoltTransactionRef(ItemizedTask, TransactionRef):

    def __init__(self, connection, graph_name,
                 readonly=False, after=None, metadata=None, timeout=None
                 ):
        self.connection = connection
        if graph_name and connection.protocol_version < (4, 0):
            raise TypeError("Database selection is not supported "
                            "prior to Neo4j 4.0")
        ItemizedTask.__init__(self)
        TransactionRef.__init__(self, graph_name, readonly=readonly)
        self.after = after
        self.metadata = metadata
        self.timeout = timeout

    @property
    def extra(self):
        extra = {}
        if self.graph_name:
            extra["db"] = self.graph_name
        if self.readonly:
            extra["mode"] = "r"
        if self.after:
            extra["bookmarks"] = list(Bookmark(self.after))
        if self.metadata:
            extra["metadata"] = self.metadata
        if self.timeout:
            extra["timeout"] = self.timeout
        return extra


class BoltResult(ItemizedTask, Result):
    """ The result of a query carried out over a Bolt connection.

    Implementation-wise, this form of query is comprised of a number of
    individual message exchanges. Each of these exchanges may succeed
    or fail in its own right, but contribute to the overall success or
    failure of the query.
    """

    def __init__(self, tx, cx, response):
        ItemizedTask.__init__(self)
        Result.__init__(self, tx)
        self.__record_type = None
        self.__cx = cx
        self._profile = cx.profile
        self.append(response)
        self._last_taken = 0

    @property
    def offline(self):
        return self.done() or self.__cx.closed or self.__cx.broken

    @property
    def profile(self):
        return self._profile

    def header(self):
        return self._items[0]

    def fields(self):
        return self.header().metadata.get("fields")

    def summary(self):
        d = {}
        for item in self.items():
            d.update(item.metadata)
        return d

    def take(self):
        i = self._last_taken
        while i < self._items_len:
            response = self._items[i]
            try:
                record = response.records.popleft()
            except IndexError:
                i += 1
            else:
                self._last_taken = i
                return record
        return None

    def peek(self, limit):
        records = []
        i = self._last_taken
        while i < self._items_len:
            response = self._items[i]
            records.extend(response.peek_records(limit - len(records)))
            if len(records) == limit:
                break
            i += 1
        return records

    def has_more_records(self):
        for item in self._items[1:]:
            has_more = item.metadata.get("has_more", False)
            if not has_more:
                return False
        return True


class BoltResponse(Task):

    # status values:
    #   0 = not done
    #   1 = success
    #   2 = failure
    #   3 = ignored

    def __init__(self, capacity=-1, vital=False):
        super(BoltResponse, self).__init__()
        self.capacity = capacity
        self.vital = vital
        self.records = deque()
        self._status = 0
        self._metadata = {}
        self._failure = None

    def __repr__(self):
        if self._status == 1:
            return "<BoltResponse SUCCESS %r>" % self._metadata
        elif self._status == 2:
            return "<BoltResponse FAILURE %r>" % self._metadata
        elif self._status == 3:
            return "<BoltResponse IGNORED>"
        else:
            return "<BoltResponse ?>"

    def add_records(self, value_lists):
        self.records.extend(value_lists)

    def peek_records(self, n):
        return islice(self.records, 0, n)

    def set_success(self, **metadata):
        self._status = 1
        self._metadata.update(metadata)

    def set_failure(self, **metadata):
        self._status = 2
        self._failure = Neo4jError.hydrate(metadata)

    def set_ignored(self):
        self._status = 3

    def full(self):
        if self.capacity >= 0:
            return len(self.records) >= self.capacity
        else:
            return False

    def done(self):
        return self._status != 0

    def failed(self):
        return self._status >= 2

    def audit(self):
        if self._failure:
            self.set_ignored()
            raise self._failure

    @property
    def metadata(self):
        return self._metadata
