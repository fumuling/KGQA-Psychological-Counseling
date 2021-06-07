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


from argparse import ArgumentParser

from py2neo import ConnectionBroken, ServiceProfile
from py2neo.client import Connector
from py2neo.client.bolt import Bolt
from py2neo.compat import TCPServer, ThreadingMixIn
from py2neo.wiring import WireRequestHandler

from logging import basicConfig, DEBUG


basicConfig(level=DEBUG)


target = "neo4j://localhost:17601"  # TODO: hardcoded


class BoltRouter(WireRequestHandler):

    def __init__(self, *args, **kwargs):
        self.client = None
        self.target = None
        self.user_agent = None
        self.auth = {}
        self.transaction = None
        self.results = {}
        WireRequestHandler.__init__(self, *args, **kwargs)

    def setup(self):
        self.client = Bolt.accept(self.wire, min_protocol_version=(4, 0))

    def handle(self):
        try:
            while True:
                tag, args = self.client.read_message()
                try:
                    handler = self.handlers[tag]
                except KeyError:
                    raise Exception("Unknown message tag %r" % tag)
                else:
                    handler(self, *args)
        except ConnectionBroken as error:
            print("[%s] Client gone" % (self.wire.remote_address,))

    def finish(self):
        self.client.close()
        self.target.close()

    def process_01(self, *args):
        for arg in args:
            if isinstance(arg, dict):
                self.auth = arg
                if "user_agent" in self.auth:
                    self.user_agent = self.auth.pop("user_agent")
            else:
                self.user_agent = arg
        self.target = Connector(ServiceProfile(target, user=self.auth["principal"],
                                               password=self.auth["credentials"]), init_size=1)
        self.client.write_message(0x70, [{"server": self.target.server_agent,
                                          "proxy": "py2neo.proxy/0.0.0",  # TODO: hardcoded
                                          "connection_id": "proxy-0"}])  # TODO: hardcoded
        self.client.send()

    def process_02(self, *_):
        self.target.close()
        self.client.close()

    def process_0f(self, *_):
        self.results.clear()
        self.transaction = None
        # TODO: self.error = None
        self.client.write_message(0x70, [{}])
        self.client.send()

    def process_10(self, cypher, parameters, metadata=None):
        try:
            graph_name = metadata["db"]
        except (KeyError, TypeError):
            graph_name = None
        try:
            mode = metadata["mode"]
        except (KeyError, TypeError):
            mode = None
        self.results[-1] = result = self.target.auto_run(cypher, parameters,
                                                         graph_name=graph_name,
                                                         readonly=(mode == "r"))
        self.client.write_message(0x70, [{"fields": result.fields()}])
        self.client.send()

    def process_2f(self, args=None, *_):
        result = self.results[-1]
        while result.take() is not None:
            pass
        self.client.write_message(0x70, [{}])
        self.client.send()

    def process_3f(self, args=None, *_):
        result = self.results[-1]
        while True:
            record = result.take()
            if record is None:
                break
            self.client.write_message(0x71, record)
        self.client.write_message(0x70, [{}])
        self.client.send()

    handlers = {0x01: process_01,
                0x02: process_02,
                0x0F: process_0f,
                0x10: process_10,
                0x2F: process_2f,
                0x3F: process_3f}


class BoltServer(ThreadingMixIn, TCPServer):

    allow_reuse_address = True

    def __init__(self, server_address, bind_and_activate=True):
        TCPServer.__init__(self, server_address, BoltRouter, bind_and_activate)


def run():
    parser = ArgumentParser()
    parser.add_argument("-b", "--bind-address", help="bind address for the proxy server", default="0.0.0.0:7687")
    parser.add_argument("-s", "--server-address", help="Neo4j server address", default="localhost:17687")

    #args = parser.parse_args()
    #bind_host, bind_port = args.bind_address.split(":")
    #server_host, server_port = args.server_address.split(":")

    server = BoltServer(("localhost", 7687))
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    run()
