#!/usr/bin/env python
# coding: utf-8

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


from __future__ import print_function

from argparse import ArgumentParser
from inspect import getdoc
from sys import stderr, exit

from py2neo import __version__
from py2neo.client.__main__ import console
from py2neo.compat import argument
from py2neo.server import Neo4jService
from py2neo.server.console import Neo4jConsole
from py2neo.server.security import make_auth, make_self_signed_certificate


def version():
    """ Display the current library version.
    """
    print(__version__)


@argument("-a", "--auth", type=make_auth,
          help="Credentials with which to bootstrap the service. "
               "These must be specified as a 'user:password' pair.")
@argument("-n", "--name",
          help="A Docker network name to which all servers will be "
               "attached. If omitted, an auto-generated name will be "
               "used.")
@argument("-v", "--verbose", action="count", default=0,
          help="Increase verbosity.")
@argument("-z", "--self-signed-certificate", action="store_true",
          help="Generate and use a self-signed certificate")
@argument("image", nargs="?", default="latest",
          help="Docker image to use (defaults to 'latest')")
def server(name, image, auth, self_signed_certificate, verbose):
    """ Start a Neo4j service in a Docker container.
    """
    con = Neo4jConsole()
    con.verbosity = verbose
    try:
        if self_signed_certificate:
            cert_key_pair = make_self_signed_certificate()
        else:
            cert_key_pair = None
        with Neo4jService.single_instance(name, image, auth, cert_key_pair) as neo4j:
            con.service = neo4j
            con.env()
            con.loop()
    except KeyboardInterrupt:
        exit(130)
    except Exception as e:
        message = " ".join(map(str, e.args))
        if hasattr(e, 'explanation'):
            message += "\n" + e.explanation
        print(message, file=stderr)
        exit(1)


def movies():
    """ Start the demo 'movies' web server.
    """
    from py2neo.vendor.bottle import load_app
    load_app("py2neo.movies").run()


def main():
    parser = ArgumentParser("py2neo")
    subparsers = parser.add_subparsers(title="commands")

    def add_command(func, name):
        subparser = subparsers.add_parser(name, help=getdoc(func))
        subparser.set_defaults(f=func)
        if hasattr(func, "arguments"):
            for a, kw in func.arguments:
                subparser.add_argument(*a, **kw)

    add_command(console, "console")
    add_command(movies, "movies")
    add_command(console, "run")
    add_command(server, "server")
    add_command(version, "version")

    args = parser.parse_args()
    kwargs = vars(args)
    try:
        f = kwargs.pop("f")
    except KeyError:
        parser.print_help()
    else:
        f(**kwargs)


if __name__ == "__main__":
    main()
