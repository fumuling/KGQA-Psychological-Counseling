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


from __future__ import division, print_function

import shlex
from datetime import datetime
from os import environ, makedirs
from os.path import expanduser, join as path_join
from subprocess import call
from tempfile import NamedTemporaryFile
from textwrap import dedent
from timeit import default_timer as timer

from pansi.console import Console
from prompt_toolkit import prompt
from prompt_toolkit.history import FileHistory
from prompt_toolkit.lexers import PygmentsLexer
from prompt_toolkit.styles import merge_styles, style_from_pygments_cls, style_from_pygments_dict
from pygments.styles.native import NativeStyle
from pygments.token import Token

from py2neo import __version__, ServiceProfile
from py2neo.cypher.lexer import CypherLexer
from py2neo.database import GraphService
from py2neo.errors import Neo4jError
from py2neo.export import Table


EDITOR = environ.get("EDITOR", "vim")

HISTORY_FILE_DIR = expanduser(path_join("~", ".py2neo"))

HISTORY_FILE = "console_history"

TITLE = "Py2neo console v{}".format(__version__)

DESCRIPTION = "Py2neo console is a Cypher runner and interactive tool for Neo4j."

QUICK_HELP = """\
  //  to enter multi-line mode (press [Alt]+[Enter] to run)
  /e  to launch external editor
  /?  for help
  /x  to exit\
"""

FULL_HELP = """\
If command line arguments are provided, these are executed in order as
statements. If no arguments are provided, an interactive console is
presented.

Statements entered at the interactive prompt or as arguments can be
regular Cypher, transaction control keywords or slash commands. Multiple
Cypher statements can be entered on the same line separated by semicolons.
These will be executed within a single transaction.

For a handy Cypher reference, see:

  https://neo4j.com/docs/cypher-refcard/current/

Transactions can be managed interactively. To do this, use the transaction
control keywords BEGIN, COMMIT and ROLLBACK.

Slash commands provide access to supplementary functionality.

\b
{}

\b
Execution commands:
  /use      select a graph database by name (~ for default)
  /play     run a query from a file

\b
Formatting commands:
  /csv      format output as comma-separated values
  /table    format output in a table
  /tsv      format output as tab-separated values

\b
Information commands:
  /config   show Neo4j server configuration
  /kernel   show Neo4j kernel information

Report bugs to py2neo@nige.tech\
""".format(QUICK_HELP)


def is_command(source):
    if source == "//":
        return True
    if source.startswith("//"):
        return False
    if source.startswith("/*"):
        return False
    return source.startswith("/") or source.startswith(":")


class ClientConsole(Console):

    multi_line = False

    def __init__(self, profile=None, *_, **settings):
        super(ClientConsole, self).__init__("py2neo", verbosity=settings.pop("verbosity", 0))
        self.output_file = settings.pop("file", None)

        welcome = settings.pop("welcome", True)
        if welcome:
            self.write(TITLE)
            self.write()
            self.write(dedent(QUICK_HELP))
            self.write()

        self.profile = ServiceProfile(profile, **settings)
        self.graph_name = None
        try:
            self.dbms = GraphService(self.profile)    # TODO: use Connector instead
        except OSError as error:
            self.critical("Could not connect to <%s> (%s)",
                          self.profile.uri, " ".join(map(str, error.args)))
            raise
        else:
            self.debug("Connected to <%s>", self.dbms.uri)
        try:
            makedirs(HISTORY_FILE_DIR)
        except OSError:
            pass
        self.history = FileHistory(path_join(HISTORY_FILE_DIR, HISTORY_FILE))
        self.lexer = CypherLexer()
        self.result_writer = Table.write

        self.commands = {

            "//": self.set_multi_line,
            "::": self.set_multi_line,
            "/e": self.edit,
            "/edit": self.edit,
            ":e": self.edit,
            ":edit": self.edit,

            "/?": self.help,
            "/h": self.help,
            "/help": self.help,
            ":?": self.help,
            ":h": self.help,
            ":help": self.help,

            "/x": self.exit,
            "/exit": self.exit,
            ":x": self.exit,
            ":q": self.exit,
            ":q!": self.exit,
            ":wq": self.exit,
            ":exit": self.exit,

            "/use": self.use,
            ":use": self.use,
            "/play": self.play,
            ":play": self.play,

            "/csv": self.set_csv_result_writer,
            ":csv": self.set_csv_result_writer,
            "/table": self.set_tabular_result_writer,
            ":table": self.set_tabular_result_writer,
            "/tsv": self.set_tsv_result_writer,
            ":tsv": self.set_tsv_result_writer,

            "/config": self.config,
            ":config": self.config,
            "/kernel": self.kernel,
            ":kernel": self.kernel,

        }
        self.tx = None
        self.qid = 0

    def __del__(self):
        try:
            self._Console__log.removeHandler(self._Console__handler)
        except ValueError:
            pass

    @property
    def graph(self):
        return self.dbms[self.graph_name]

    def process_all(self, lines, times=1):
        gap = False
        for _ in range(times):
            for line in lines:
                if gap:
                    self.write("")
                self.process(line)
                if not is_command(line):
                    gap = True
        return 0

    def process(self, line):
        line = line.strip()
        if not line:
            return
        try:
            if is_command(line):
                self.run_command(line)
            else:
                self.run_source(line)
        except Neo4jError as error:
            if hasattr(error, "title") and hasattr(error, "message"):
                self.error("%s: %s", error.title, error.message)
            else:
                self.error("%s: %s", error.__class__.__name__, " ".join(map(str, error.args)))
        except OSError as error:
            self.critical("Service Unavailable (%s)", error.args[0])
        except Exception as error:
            self.exception(*error.args)

    def begin_transaction(self):
        if self.tx is None:
            self.tx = self.graph.begin()
            self.qid = 1
        else:
            self.warning("Transaction already open")

    def commit_transaction(self):
        if self.tx:
            try:
                self.graph.commit(self.tx)
                self.info("Transaction committed")
            finally:
                self.tx = None
                self.qid = 0
        else:
            self.warning("No current transaction")

    def rollback_transaction(self):
        if self.tx:
            try:
                self.graph.rollback(self.tx)
                self.info("Transaction rolled back")
            finally:
                self.tx = None
                self.qid = 0
        else:
            self.warning("No current transaction")

    def read(self):
        prompt_args = {
            "history": self.history,
            "lexer": PygmentsLexer(CypherLexer),
            "style": merge_styles([
                style_from_pygments_cls(NativeStyle),
                style_from_pygments_dict({
                    Token.Prompt.User: "#ansigreen",
                    Token.Prompt.At: "#ansigreen",
                    Token.Prompt.Host: "#ansigreen",
                    Token.Prompt.Slash: "#ansigreen",
                    Token.Prompt.Graph: "#ansiblue",
                    Token.Prompt.QID: "#ansiyellow",
                    Token.Prompt.Arrow: "#808080",
                })
            ])
        }

        if self.multi_line:
            self.multi_line = False
            return prompt(u"", multiline=True, **prompt_args)

        def get_prompt_tokens():
            graph_name = "~" if self.graph_name is None else self.graph_name
            tokens = [
                ("class:pygments.prompt.user", self.profile.user),
                ("class:pygments.prompt.at", "@"),
                ("class:pygments.prompt.host", self.profile.host),
                ("class:pygments.prompt.slash", "/"),
                ("class:pygments.prompt.graph", graph_name),
            ]
            if self.tx is None:
                tokens.append(("class:pygments.prompt.arrow", " -> "))
            else:
                tokens.append(("class:pygments.prompt.arrow", " "))
                tokens.append(("class:pygments.prompt.qid", str(self.qid)))
                tokens.append(("class:pygments.prompt.arrow", "> "))
            return tokens

        return prompt(get_prompt_tokens, **prompt_args)

    def run_source(self, source):
        for i, statement in enumerate(self.lexer.get_statements(source)):
            if i > 0:
                self.write(u"")
            if statement.upper() == "BEGIN":
                self.begin_transaction()
            elif statement.upper() == "COMMIT":
                self.commit_transaction()
            elif statement.upper() == "ROLLBACK":
                self.rollback_transaction()
            elif self.tx is None:
                self.run_cypher(self.graph.run, statement, {})
            else:
                self.run_cypher(self.tx.run, statement, {}, query_id=self.qid)
                self.qid += 1

    def run_cypher(self, runner, statement, parameters, query_id=0):
        t0 = timer()
        result = runner(statement, parameters)
        record_count = self.write_result(result)
        if result.profile:
            uri = result.profile["uri"]
        else:
            uri = self.graph.service.uri

        msg = "Fetched %r %s from %r in %rs"
        args = [
            record_count,
            "record" if record_count == 1 else "records",
            uri,
            timer() - t0,
        ]
        if query_id:
            msg += " for query (%r)"
            args.append(query_id)
        self.debug(msg, *args)

    def write_result(self, result, page_size=50):
        table = Table(result)
        table_size = len(table)
        if self.verbosity >= 0:
            for skip in range(0, table_size, page_size):
                self.result_writer(table, file=self.output_file, header="cyan", skip=skip, limit=page_size)
                self.write("\r\n", end='')
        return table_size

    def run_command(self, source):
        source = source.lstrip()
        assert source
        terms = shlex.split(source)
        command_name = terms[0]
        try:
            command = self.commands[command_name]
        except KeyError:
            self.info("Unknown command: " + command_name)
        else:
            args = []
            kwargs = {}
            for term in terms[1:]:
                if "=" in term:
                    key, _, value = term.partition("=")
                    kwargs[key] = value
                else:
                    args.append(term)
            command(*args, **kwargs)

    def set_multi_line(self, **kwargs):
        self.multi_line = True

    def edit(self, **kwargs):
        initial_message = b""
        with NamedTemporaryFile(suffix=".cypher") as f:
            f.write(initial_message)
            f.flush()
            call([EDITOR, f.name])
            f.seek(0)
            source = f.read().decode("utf-8")
            self.write(source)
            self.process(source)

    def help(self, **kwargs):
        self.info(DESCRIPTION)
        self.info(u"")
        self.info(FULL_HELP.replace("\b\n", ""))

    def use(self, graph_name):
        if graph_name == "~":
            graph_name = None
        try:
            _ = self.dbms[graph_name]
        except KeyError:
            self.error("Graph database %r not found", graph_name)
        else:
            self.graph_name = graph_name

    def play(self, file_name):
        work = self.load_unit_of_work(file_name=file_name)
        with self.graph.begin() as tx:
            work(tx)

    def load_unit_of_work(self, file_name):
        """ Load a transaction function from a cypher source file.
        """
        with open(expanduser(file_name)) as f:
            source = f.read()

        def unit_of_work(tx):
            for line_no, statement in enumerate(self.lexer.get_statements(source), start=1):
                if line_no > 0:
                    self.write(u"")
                self.run_cypher(tx.run, statement, {}, query_id=line_no)

        return unit_of_work

    def set_csv_result_writer(self, **kwargs):
        self.result_writer = Table.write_csv

    def set_tabular_result_writer(self, **kwargs):
        self.result_writer = Table.write

    def set_tsv_result_writer(self, **kwargs):
        self.result_writer = Table.write_tsv

    def config(self, **kwargs):
        result = self.graph.run("CALL dbms.listConfig")
        records = None
        last_category = None
        for record in result:
            name = record["name"]
            category, _, _ = name.partition(".")
            if category != last_category:
                if records is not None:
                    Table(records, ["name", "value"]).write(auto_align=False, padding=0, separator=u" = ")
                    self.write(u"")
                records = []
            records.append((name, record["value"]))
            last_category = category
        if records is not None:
            Table(records, ["name", "value"]).write(auto_align=False, padding=0, separator=u" = ")

    def kernel(self, **kwargs):
        result = self.graph.run("CALL dbms.queryJmx", {"query": "org.neo4j:instance=kernel#0,name=Kernel"})
        records = []
        for record in result:
            attributes = record["attributes"]
            for key, value_dict in sorted(attributes.items()):
                value = value_dict["value"]
                if key.endswith("Date") or key.endswith("Time"):
                    try:
                        value = datetime.fromtimestamp(value / 1000).isoformat(" ")
                    except:
                        pass
                records.append((key, value))
        Table(records, ["key", "value"]).write(auto_align=False, padding=0, separator=u" = ")

    def exit(self):
        """ Exit the console.
        """
        super(ClientConsole, self).exit()
        self._Console__log.removeHandler(self._Console__handler)
