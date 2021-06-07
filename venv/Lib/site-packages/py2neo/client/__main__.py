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

from py2neo.compat import argument


@argument("-u", "--uri",
          help="Set the connection URI.")
@argument("-a", "--auth", metavar="USER:PASSWORD",
          help="Set the user and password.")
@argument("-q", "--quiet", action="count", default=0,
          help="Reduce verbosity.")
@argument("-r", "--routing", action="store_true", default=False,
          help="Enable connection routing.")
@argument("-s", "--secure", action="store_true", default=False,
          help="Use encrypted communication (TLS).")
@argument("-v", "--verbose", action="count", default=0,
          help="Increase verbosity.")
@argument("-x", "--times", type=int, default=1,
          help="Number of times to repeat.")
@argument("cypher", nargs="*")
def console(cypher, uri, auth=None, routing=False, secure=False, verbose=False, quiet=False, times=1):
    """ Run one or more Cypher queries through the client console, or
    open the console for interactive use if no queries are specified.
    """
    from py2neo.client.console import ClientConsole
    con = ClientConsole(uri, auth=auth, routing=routing, secure=secure,
                        verbosity=(verbose - quiet), welcome=False)
    if cypher:
        con.process_all(cypher, times)
    else:
        con.loop()


def main():
    parser = ArgumentParser(description=getdoc(console))
    for a, kw in console.arguments:
        parser.add_argument(*a, **kw)

    args = parser.parse_args()
    kwargs = vars(args)
    console(**kwargs)


if __name__ == "__main__":
    main()
