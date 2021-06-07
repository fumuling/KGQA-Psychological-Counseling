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

from collections import OrderedDict
from logging import getLogger
from json import dumps as json_dumps, loads as json_loads

from packaging.version import Version
from six import raise_from
from urllib3 import HTTPConnectionPool, HTTPSConnectionPool, make_headers
from urllib3.exceptions import HTTPError

from py2neo import ConnectionProfile
from py2neo.compat import urlsplit
from py2neo.client import http_user_agent, Connection, TransactionRef, Result, Bookmark
from py2neo.client.json import JSONHydrant, dehydrate
from py2neo.errors import Neo4jError, ConnectionUnavailable, ProtocolError


log = getLogger(__name__)


class HTTP(Connection):

    @classmethod
    def default_hydrant(cls, profile, graph):
        return JSONHydrant(graph)

    @classmethod
    def open(cls, profile=None, user_agent=None, on_release=None, on_broken=None):
        """ Open an HTTP connection to a server.

        :param profile: :class:`.ConnectionProfile` detailing how and
            where to connect
        :param user_agent:
        :param on_release:
        :param on_broken:
        :returns: :class:`.HTTP` connection object
        :raises: :class:`.ConnectionUnavailable` if a connection cannot
            be opened
        """
        if profile is None:
            profile = ConnectionProfile(scheme="http")
        try:
            http = cls(profile, on_release=on_release)
            http._hello(user_agent or http_user_agent())
            return http
        except HTTPError as error:
            raise_from(ConnectionUnavailable("Cannot open connection to %r", profile), error)

    def __init__(self, profile, on_release=None):
        super(HTTP, self).__init__(profile, on_release=on_release)
        self.http_pool = None
        self.headers = {}
        self.__closed = False
        self._make_pool(profile)

    def _make_pool(self, profile):
        if profile.secure:
            from ssl import CERT_NONE, CERT_REQUIRED
            from certifi import where as cert_where
            self.http_pool = HTTPSConnectionPool(
                host=profile.host,
                port=profile.port_number,
                maxsize=1,
                block=True,
                cert_reqs=CERT_REQUIRED if profile.verify else CERT_NONE,
                ca_certs=cert_where()
            )
        else:
            self.http_pool = HTTPConnectionPool(
                host=profile.host,
                port=profile.port_number,
                maxsize=1,
                block=True,
            )

    def close(self):
        self.http_pool.close()
        self.__closed = True

    @property
    def closed(self):
        return self.__closed

    @property
    def broken(self):
        return False

    @property
    def local_port(self):
        raise NotImplementedError

    def _hello(self, user_agent):
        self.headers.update(make_headers(basic_auth=":".join(self.profile.auth),
                                         user_agent=user_agent))
        r = self.http_pool.request(method="GET",
                                   url="/",
                                   headers=dict(self.headers))
        metadata = json_loads(r.data.decode("utf-8"))
        if "neo4j_version" in metadata:
            # {
            #   "bolt_routing" : "neo4j://localhost:7687",
            #   "transaction" : "http://localhost:7474/db/{databaseName}/tx",
            #   "bolt_direct" : "bolt://localhost:7687",
            #   "neo4j_version" : "4.0.0",
            #   "neo4j_edition" : "community"
            # }
            self._neo4j_version = Version(metadata["neo4j_version"])  # Neo4j 4.x
            self._neo4j_edition = metadata["neo4j_edition"]
        else:                               # Neo4j 3.x
            # {
            #   "data" : "http://localhost:7474/db/data/",
            #   "management" : "http://localhost:7474/db/manage/",
            #   "bolt" : "bolt://localhost:7687"
            # }
            r = self.http_pool.request(method="GET",
                                       url="/db/data/",
                                       headers=dict(self.headers))
            metadata = json_loads(r.data.decode("utf-8"))
            # {
            #   "extensions" : { },
            #   "node" : "http://localhost:7474/db/data/node",
            #   "relationship" : "http://localhost:7474/db/data/relationship",
            #   "node_index" : "http://localhost:7474/db/data/index/node",
            #   "relationship_index" : "http://localhost:7474/db/data/index/relationship",
            #   "extensions_info" : "http://localhost:7474/db/data/ext",
            #   "relationship_types" : "http://localhost:7474/db/data/relationship/types",
            #   "batch" : "http://localhost:7474/db/data/batch",
            #   "cypher" : "http://localhost:7474/db/data/cypher",
            #   "indexes" : "http://localhost:7474/db/data/schema/index",
            #   "constraints" : "http://localhost:7474/db/data/schema/constraint",
            #   "transaction" : "http://localhost:7474/db/data/transaction",
            #   "node_labels" : "http://localhost:7474/db/data/labels",
            #   "neo4j_version" : "3.5.12"
            # }
            self._neo4j_version = Version(metadata["neo4j_version"])  # Neo4j 3.x
        self.server_agent = "Neo4j/{}".format(self._neo4j_version)

        # Given the root discovery endpoint isn't authenticated, we don't
        # catch incorrect passwords here, and this wouldn't then be signalled
        # to the user until later on. So here, we make a second call to a
        # different URL for that reason only.
        r = self.http_pool.request(method="GET",
                                   url="/db/data/",
                                   headers=dict(self.headers))
        data = r.data.decode("utf-8")
        rs = HTTPResponse.from_json(r.status, data or "{}")
        rs.audit()

    def fast_forward(self, bookmark):
        raise NotImplementedError("Bookmarking is not yet supported over HTTP")

    def auto_run(self, cypher, parameters=None, graph_name=None, readonly=False,
                 # after=None, metadata=None, timeout=None
                 ):
        if graph_name and not self.supports_multi():
            raise TypeError("Neo4j {} does not support "
                            "named graphs".format(self.neo4j_version))
        if readonly:
            log.warning("Readonly transactions are not supported over HTTP")
        r = self._post(HTTPTransactionRef.autocommit_uri(graph_name), cypher, parameters)
        rs = HTTPResponse.from_json(r.status, r.data.decode("utf-8"))
        self.release()
        rs.audit()
        return HTTPResult(HTTPTransactionRef(graph_name), rs.result(), profile=self.profile)

    def begin(self, graph_name, readonly=False,
              # after=None, metadata=None, timeout=None
              ):
        if graph_name and not self.supports_multi():
            raise TypeError("Neo4j {} does not support "
                            "named graphs".format(self.neo4j_version))
        if readonly:
            log.warning("Readonly transactions are not supported over HTTP")
        # if after:
        #     raise TypeError("Bookmarks are not supported over HTTP")
        # if metadata:
        #     raise TypeError("Transaction metadata is not supported over HTTP")
        # if timeout:
        #     raise TypeError("Transaction timeouts are not supported over HTTP")
        r = self._post(HTTPTransactionRef.begin_uri(graph_name))
        rs = HTTPResponse.from_json(r.status, r.data.decode("utf-8"))
        location_path = urlsplit(r.headers["Location"]).path
        tx = HTTPTransactionRef(graph_name, location_path.rpartition("/")[-1])
        self.release()
        rs.audit(tx)
        return tx

    def commit(self, tx):
        if tx.broken:
            raise ValueError("Transaction is broken")
        try:
            r = self._post(tx.commit_uri())
        except ProtocolError:
            tx.mark_broken()
            raise
        else:
            rs = HTTPResponse.from_json(r.status, r.data.decode("utf-8"))
            self.release()
            rs.audit(tx)
            return Bookmark()

    def rollback(self, tx):
        if tx.broken:
            raise ValueError("Transaction is broken")
        try:
            r = self._delete(tx.uri())
        except ProtocolError:
            tx.mark_broken()
            raise
        else:
            rs = HTTPResponse.from_json(r.status, r.data.decode("utf-8"))
            self.release()
            rs.audit(tx)
            return Bookmark()

    def run(self, tx, cypher, parameters=None):
        try:
            r = self._post(tx.uri(), cypher, parameters)
        except ProtocolError:
            tx.mark_broken()
            raise
        else:
            rs = HTTPResponse.from_json(r.status, r.data.decode("utf-8"))
            self.release()
            rs.audit(tx)
            return HTTPResult(tx, rs.result(), profile=self.profile)

    def pull(self, result, n=-1):
        # TODO: lower-memory algorithm that doesn't require
        #  duplication of potentially large data sets
        if n == -1:
            result._buffer.extend(result._data)
            result._data[:] = []
        else:
            result._buffer.extend(result._data[:n])
            result._data[:n] = []

    def discard(self, result):
        result._data[:] = []

    def sync(self, result):
        pass

    def fetch(self, result):
        record = result.take()
        return record

    def _post(self, url, statement=None, parameters=None):
        log.debug("POST %r %r %r", url, statement, parameters)
        if statement:
            statements = [
                OrderedDict([
                    ("statement", statement),
                    ("parameters", dehydrate(parameters or {})),
                    ("resultDataContents", ["REST"]),
                    ("includeStats", True),
                ])
            ]
        else:
            statements = []
        try:
            return self.http_pool.request(method="POST",
                                          url=url,
                                          headers=dict(self.headers, **{"Content-Type": "application/json"}),
                                          body=json_dumps({"statements": statements}))
        except HTTPError as error:
            raise_from(ProtocolError("Failed to POST to %r" % url), error)

    def _delete(self, url):
        log.debug("DELETE %r", url)
        try:
            return self.http_pool.request(method="DELETE",
                                          url=url,
                                          headers=dict(self.headers))
        except HTTPError as error:
            raise_from(ProtocolError("Failed to DELETE %r" % url), error)

    def supports_multi(self):
        return self._neo4j_version >= Version("4.0")


class HTTPTransactionRef(TransactionRef):

    def __init__(self, graph_name, txid=None, readonly=False):
        super(HTTPTransactionRef, self).__init__(graph_name, txid, readonly)
        self.failure = None

    def __bool__(self):
        return not self.broken

    __nonzero__ = __bool__

    @classmethod
    def autocommit_uri(cls, graph_name):
        if graph_name:
            return "/db/{}/tx/commit".format(graph_name)
        else:
            return "/db/data/transaction/commit"

    @classmethod
    def begin_uri(cls, graph_name):
        if graph_name:
            return "/db/{}/tx".format(graph_name)
        else:
            return "/db/data/transaction"

    def uri(self):
        if self.graph_name:
            return "/db/{}/tx/{}".format(self.graph_name, self.txid)
        else:
            return "/db/data/transaction/{}".format(self.txid)

    def commit_uri(self):
        if self.graph_name:
            return "/db/{}/tx/{}/commit".format(self.graph_name, self.txid)
        else:
            return "/db/data/transaction/{}/commit".format(self.txid)


class HTTPResult(Result):

    def __init__(self, tx, result, profile):
        Result.__init__(self, tx)
        self._profile = profile
        self._columns = result.get("columns", ())
        self._data = result.get("data", [])
        self._buffer = []
        self._summary = {}
        if "stats" in result:
            self._summary["stats"] = result["stats"]
        self._cursor = 0

    @property
    def offline(self):
        return True

    @property
    def profile(self):
        return self._profile

    def fields(self):
        return self._columns

    def summary(self):
        return self._summary

    def take(self):
        try:
            record = self._buffer[self._cursor]["rest"]
        except IndexError:
            return None
        else:
            self._cursor += 1
            return record

    def peek(self, limit):
        records = []
        for i in range(limit):
            try:
                records.append(self._buffer[self._cursor + i]["rest"])
            except IndexError:
                break
        return records


class HTTPResponse(object):

    @classmethod
    def from_json(cls, status, data):
        try:
            content = json_loads(data, object_hook=JSONHydrant.json_to_packstream)
        except ValueError as error:
            raise_from(ProtocolError("Cannot decode response content as JSON"), error)
        else:
            return cls(status, content)

    def __init__(self, status, content):
        self._status = status
        self._content = content

    @property
    def status(self):
        return self._status

    def columns(self):
        return tuple(self._content.get("columns", ()))

    def result(self, index=0):
        try:
            results = self._content["results"]
        except KeyError:
            return {}  # TODO return None, somehow
        else:
            try:
                return results[index]
            except IndexError:
                return {}  # TODO return None, somehow

    def stats(self):
        return self._content.get("stats", {})

    def errors(self):
        return self._content.get("errors", [])

    def audit(self, tx=None):
        if self.errors():
            failure = Neo4jError.hydrate(self.errors().pop(0))
            if tx is not None:
                tx.mark_broken()
            raise failure
