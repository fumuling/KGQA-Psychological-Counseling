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


from py2neo.cypher import cypher_escape


class ProcedureLibrary(object):
    """ Accessor for listing and calling procedures.

    This object is typically constructed and accessed via the
    :meth:`.Graph.call` attribute. See the documentation for that
    attribute for usage information.

    *New in version 2020.0.*
    """

    def __init__(self, graph):
        self.graph = graph

    def __getattr__(self, name):
        return Procedure(self.graph, name)

    def __getitem__(self, name):
        return Procedure(self.graph, name)

    def __dir__(self):
        return list(self)

    def __iter__(self):
        proc = Procedure(self.graph, "dbms.procedures")
        for record in proc(keys=["name"]):
            yield record[0]

    def __call__(self, procedure, *args):
        """ Call a procedure by name.

        For example:
            >>> from py2neo import Graph
            >>> g = Graph()
            >>> g.call("dbms.components")
             name         | versions  | edition
            --------------|-----------|-----------
             Neo4j Kernel | ['4.0.2'] | community

        :param procedure: fully qualified procedure name
        :param args: positional arguments to pass to the procedure
        :returns: :class:`.Cursor` object wrapping the result
        """
        return Procedure(self.graph, procedure)(*args)


class Procedure(object):
    """ Represents an individual procedure.

    *New in version 2020.0.*
    """

    def __init__(self, graph, name):
        self.graph = graph
        self.name = name

    def __getattr__(self, name):
        return Procedure(self.graph, self.name + "." + name)

    def __getitem__(self, name):
        return Procedure(self.graph, self.name + "." + name)

    def __dir__(self):
        proc = Procedure(self.graph, "dbms.procedures")
        prefix = self.name + "."
        return [record[0][len(prefix):] for record in proc(keys=["name"])
                if record[0].startswith(prefix)]

    def __call__(self, *args, **kwargs):
        """ Call a procedure by name.

        For example:
            >>> from py2neo import Graph
            >>> g = Graph()
            >>> g.call("dbms.components")
             name         | versions  | edition
            --------------|-----------|-----------
             Neo4j Kernel | ['4.0.2'] | community

        :param procedure: fully qualified procedure name
        :param args: positional arguments to pass to the procedure
        :returns: :class:`.Cursor` object wrapping the result
        """
        procedure_name = ".".join(cypher_escape(part) for part in self.name.split("."))
        arg_list = [(str(i), arg) for i, arg in enumerate(args)]
        cypher = "CALL %s(%s)" % (procedure_name, ", ".join("$" + a[0] for a in arg_list))
        keys = kwargs.get("keys")
        if keys:
            cypher += " YIELD %s" % ", ".join(keys)
        return self.graph.run(cypher, dict(arg_list))
