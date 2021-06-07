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
from inspect import getdoc
from logging import getLogger
from shlex import split as shlex_split
from sys import stdout
from textwrap import wrap
from webbrowser import open as open_browser

from english.text import first_sentence
from pansi.console import Console


log = getLogger(__name__)


class _ConsoleArgumentParser(ArgumentParser):

    def __init__(self, prog=None, **kwargs):
        kwargs["add_help"] = False
        super(_ConsoleArgumentParser, self).__init__(prog, **kwargs)

    def exit(self, status=0, message=None):
        pass    # TODO

    def error(self, message):
        raise _ConsoleCommandError(message)


class _ConsoleCommandError(Exception):

    pass


class _CommandConsole(Console):

    def __init__(self, name, out=stdout, verbosity=None, history=None, time_format=None):
        super(_CommandConsole, self).__init__(name, out=out, verbosity=verbosity,
                                              history=history, time_format=time_format)
        self.__parser = _ConsoleArgumentParser(self.name)
        self.__command_parsers = self.__parser.add_subparsers()
        self.__commands = {}
        self.__usages = {}
        self.add_command("help", self.help)
        self.add_command("exit", self.exit)

    def __del__(self):
        try:
            self._Console__log.removeHandler(self._Console__handler)
        except ValueError:
            pass

    def add_command(self, name, f):
        parser = self.__command_parsers.add_parser(name, add_help=False)
        try:
            from inspect import getfullargspec
        except ImportError:
            # Python 2
            from inspect import getargspec
            spec = getargspec(f)
        else:
            # Python 3
            spec = getfullargspec(f)
        args, default_values = spec[0], spec[3]
        if default_values:
            n_defaults = len(default_values)
            defaults = dict(zip(args[-n_defaults:], default_values))
        else:
            defaults = {}
        usage = []
        for i, arg in enumerate(args):
            if i == 0 and arg == "self":
                continue
            if arg in defaults:
                parser.add_argument(arg, nargs="?", default=defaults[arg])
                usage.append("[%s]" % arg)
            else:
                parser.add_argument(arg)
                usage.append(arg)
        parser.set_defaults(f=f)
        self.__commands[name] = parser
        self.__usages[name] = usage

    def process(self, line):
        """ Handle input.
        """

        # Lex
        try:
            tokens = shlex_split(line)
        except ValueError as error:
            self.error("Syntax error (%s)", error.args[0])
            return 2

        # Parse
        try:
            args = self.__parser.parse_args(tokens)
        except _ConsoleCommandError as error:
            if tokens[0] in self.__commands:
                # misused
                self.error(error)
                return 1
            else:
                # unknown
                self.error(error)
                return 127

        # Dispatch
        kwargs = vars(args)
        f = kwargs.pop("f")
        return f(**kwargs) or 0

    def help(self, command=None):
        """ Show general or command-specific help.
        """
        if command:
            try:
                parser = self.__commands[command]
            except KeyError:
                self.error("No such command %r", command)
                raise RuntimeError('No such command "%s".' % command)
            else:
                parts = ["usage:", command] + self.__usages[command]
                self.write(" ".join(parts))
                self.write()
                f = parser.get_default("f")
                doc = getdoc(f)
                self.write(doc.rstrip())
                self.write()
        else:
            self.write("Commands:")
            command_width = max(map(len, self.__commands))
            template = "  {:<%d}   {}" % command_width
            for name in sorted(self.__commands):
                parser = self.__commands[name]
                f = parser.get_default("f")
                doc = getdoc(f)
                lines = wrap(first_sentence(doc), 73 - command_width)
                for i, line in enumerate(lines):
                    if i == 0:
                        self.write(template.format(name, line))
                    else:
                        self.write(template.format("", line))
            self.write()

    def exit(self):
        """ Exit the console.
        """
        super(_CommandConsole, self).exit()
        self._Console__log.removeHandler(self._Console__handler)


class Neo4jConsole(_CommandConsole):

    args = None

    service = None

    def __init__(self, out=stdout):
        super(Neo4jConsole, self).__init__("py2neo.server", out=out)
        self.add_command("browser", self.browser)
        self.add_command("env", self.env)
        self.add_command("ls", self.ls)
        self.add_command("logs", self.logs)

    def _iter_instances(self, name):
        if not name:
            name = "a"
        for instance in self.service.instances:
            if name in (instance.name, instance.fq_name):
                yield instance

    def _for_each_instance(self, name, f):
        found = 0
        for instance_obj in self._iter_instances(name):
            f(instance_obj)
            found += 1
        return found

    def browser(self, instance="a"):
        """ Start the Neo4j browser.

        A machine name may optionally be passed, which denotes the server to
        which the browser should be tied. If no machine name is given, 'a' is
        assumed.
        """

        def f(i):
            try:
                uri = "https://{}".format(i.addresses["https"])
            except KeyError:
                uri = "http://{}".format(i.addresses["http"])
            log.info("Opening web browser for machine %r at %r", i.fq_name, uri)
            open_browser(uri)

        if not self._for_each_instance(instance, f):
            raise RuntimeError("Machine {!r} not found".format(instance))

    def env(self):
        """ Show available environment variables.

        Each service exposes several environment variables which contain
        information relevant to that service. These are:

          BOLT_SERVER_ADDR   space-separated string of router addresses
          NEO4J_AUTH         colon-separated user and password

        """
        for key, value in sorted(self.service.env().items()):
            log.info("%s=%r", key, value)

    def ls(self):
        """ Show server details.
        """
        self.write("CONTAINER   NAME        "
                   "BOLT PORT   HTTP PORT   HTTPS PORT   MODE")
        for instance in self.service.instances:
            if instance is None:
                continue
            self.write("{:<12}{:<12}{:<12}{:<12}{:<13}{:<15}".format(
                instance.container.short_id,
                instance.fq_name,
                instance.bolt_port,
                instance.http_port,
                instance.https_port or 0,
                instance.config.get("dbms.mode", "SINGLE"),
            ))

    def logs(self, instance="a"):
        """ Display server logs.

        If no server name is provided, 'a' is used as a default.
        """

        def f(m):
            self.write(m.container.logs().decode("utf-8"))

        if not self._for_each_instance(instance, f):
            self.error("Machine %r not found", instance)
            return 1
