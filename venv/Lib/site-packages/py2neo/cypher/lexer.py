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
This module contains a `Cypher <https://neo4j.com/developer/cypher/>`_ language lexer based on the
`Pygments <http://pygments.org/>`_ `lexer framework <http://pygments.org/docs/lexerdevelopment/>`_.
This can be used to parse statements and expressions for the Cypher variant available in Neo4j 3.4.

To parse a Cypher statement, create a :class:`.CypherLexer` or select by the name `py2neo.cypher`,
then invoke the :meth:`.get_tokens` method::

    >>> from pygments.lexers import get_lexer_by_name
    >>> lexer = get_lexer_by_name("py2neo.cypher")
    >>> list(lexer.get_tokens("MATCH (a:Person)-[:KNOWS]->(b) RETURN a"))
    [(Token.Keyword, 'MATCH'),
     (Token.Text.Whitespace, ' '),
     (Token.Punctuation, '('),
     (Token.Name.Variable, 'a'),
     (Token.Punctuation, ':'),
     (Token.Name.Label, 'Person'),
     (Token.Punctuation, ')-['),
     (Token.Punctuation, ':'),
     (Token.Name.Label, 'KNOWS'),
     (Token.Punctuation, ']->('),
     (Token.Name.Variable, 'b'),
     (Token.Punctuation, ')'),
     (Token.Text.Whitespace, ' '),
     (Token.Keyword, 'RETURN'),
     (Token.Text.Whitespace, ' '),
     (Token.Name.Variable, 'a'),
     (Token.Text.Whitespace, '\\n')]

To split multiple semicolon-separated statements within a single string, use instead the :meth:`.get_statements` method::

    >>> list(lexer.get_statements("CREATE (:Person {name:'Alice'}); MATCH (a:Person {name:'Alice'}) RETURN id(a)"))
    ["CREATE (:Person {name:'Alice'})",
     "MATCH (a:Person {name:'Alice'}) RETURN id(a)"]

"""

import re

from pygments.lexer import RegexLexer, include, bygroups
from pygments.token import Keyword, Punctuation, Comment, Operator, Name, \
    String, Number, Whitespace


__all__ = [
    "cypher_keywords",
    "cypher_pseudo_keywords",
    "cypher_operator_symbols",
    "cypher_operator_words",
    "cypher_constants",
    "neo4j_built_in_functions",
    "neo4j_user_defined_functions",
    "CypherLexer",
]


cypher_keywords = [
    "ACCESS",
    "ALL",
    "ALTER",
    "AS",
    "AS COPY OF",
    "ASC",
    "ASCENDING",
    "ASSERT",
    "ASSERT EXISTS",
    "ASSIGN",
    "CALL",
    "CHANGE REQUIRED",
    "CHANGE NOT REQUIRED",
    "CONSTRAINT",
    "CREATE",
    "CREATE OR REPLACE",
    "CREATE UNIQUE",
    "CURRENT USER",
    "CYPHER",
    "DATABASE",
    "DATABASES",
    "DBMS",
    "DEFAULT DATABASE",
    "DELETE",
    "DENY",
    "DESC",
    "DESCENDING",
    "DETACH DELETE",
    "DO",
    "DROP",
    "ELEMENTS",
    "EXPLAIN",
    "FIELDTERMINATOR",
    "FOREACH",
    "FROM",
    "GRANT",
    "GRAPH",
    "GRAPH AT",
    "GRAPH OF",
    "IF EXISTS",
    "IF NOT EXISTS",
    "INDEX",
    "INTO",
    "IS NODE KEY",
    "IS UNIQUE",
    "LIMIT",
    "LOAD",
    "LOAD CSV",
    "MANAGEMENT",
    "MATCH",
    "MERGE",
    "NAME",
    "NEW LABELS",
    "NEW TYPES",
    "NEW PROPERTY NAMES",
    "NODE",
    "NODES",
    "ON",
    "ON CREATE SET",
    "ON MATCH SET",
    "OPTIONAL MATCH",
    "ORDER BY",
    "PERSIST",
    "POPULATED ROLES",
    "_PRAGMA",
    "PRIVILEGE",
    "PRIVILEGES",
    "PROFILE",
    "REMOVE",
    "RELATIONSHIP",
    "RELATIONSHIPS",
    "RELOCATE",
    "RETURN",
    "RETURN DISTINCT",
    "REVOKE",
    "ROLE",
    "ROLES",
    "SET",
    "SET PASSWORD",
    "SET PASSWORDS",
    "SET USER STATUS",
    "SET STATUS ACTIVE",
    "SET STATUS SUSPENDED",
    "SHOW",
    "SKIP",
    "SNAPSHOT",
    "SOURCE",
    "START",
    "STOP",
    "TARGET",
    "TERMINATE",
    "TO",
    "TRANSACTION",
    "TRAVERSE",
    "UNION",
    "UNION ALL",
    "UNWIND",
    "USE",
    "USER",
    "USERS",
    "USING INDEX",
    "USING JOIN",
    "USING PERIODIC COMMIT",
    "USING SCAN",
    "WHERE",
    "WITH",
    "WITH DISTINCT",
    "WITH HEADERS",
    "YIELD",
    ">>",
]
cypher_pseudo_keywords = [
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
]
cypher_operator_symbols = [
    "!=",
    "%",
    "*",
    "+",
    "+=",
    "-",
    ".",
    "/",
    "<",
    "<=",
    "<>",
    "=",
    "=~",
    ">",
    ">=",
    "^",
]
cypher_operator_words = [
    'AND',
    'CASE',
    'CONTAINS',
    'DISTINCT',
    'ELSE',
    'END',
    'ENDS WITH',
    'IN',
    'IS NOT NULL',
    'IS NULL',
    'NOT',
    'OR',
    'STARTS WITH',
    'THEN',
    'WHEN',
    'XOR',
]
cypher_constants = [
    'null',
    'true',
    'false',
]

neo4j_built_in_functions = [
    "abs",
    "acos",
    "all",
    "allShortestPaths",
    "any",
    "asin",
    "atan",
    "atan2",
    "avg",
    "ceil",
    "coalesce",
    "collect",
    "cos",
    "cot",
    "count",
    "degrees",
    "distance",
    "e",
    "endNode",
    "exists",
    "exp",
    "extract",
    "filter",
    "floor",
    "haversin",
    "head",
    "id",
    "keys",
    "labels",
    "last",
    "left",
    "length",
    "log",
    "log10",
    "lTrim",
    "max",
    "min",
    "nodes",
    "none",
    "percentileCont",
    "percentileDisc",
    "pi",
    "point",
    "properties",
    "radians",
    "rand",
    "range",
    "reduce",
    "relationships",
    "replace",
    "reverse",
    "right",
    "round",
    "rTrim",
    "shortestPath",
    "sign",
    "sin",
    "single",
    "size",
    "split",
    "sqrt",
    "startNode",
    "stdDev",
    "stdDevP",
    "substring",
    "sum",
    "tail",
    "tan",
    "timestamp",
    "toBoolean",
    "toFloat",
    "toInteger",
    "toLower",
    "toString",
    "toUpper",
    "trim",
    "type",
]

neo4j_user_defined_functions = [
    "date",
    "date.realtime",
    "date.statement",
    "date.transaction",
    "date.truncate",
    "datetime",
    "datetime.fromepoch",
    "datetime.fromepochmillis",
    "datetime.realtime",
    "datetime.statement",
    "datetime.transaction",
    "datetime.truncate",
    "duration",
    "duration.between",
    "duration.inDays",
    "duration.inMonths",
    "duration.inSeconds",
    "localdatetime",
    "localdatetime.realtime",
    "localdatetime.statement",
    "localdatetime.transaction",
    "localdatetime.truncate",
    "localtime",
    "localtime.realtime",
    "localtime.statement",
    "localtime.transaction",
    "localtime.truncate",
    "randomUUID",
    "time",
    "time.realtime",
    "time.statement",
    "time.transaction",
    "time.truncate"
]


def word_list(words, token_type):
    return list(reversed(sorted((word.replace(" ", r"\s+") + r"\b", token_type) for word in words)))


def symbol_list(symbols, token_type):
    return list(reversed(sorted(("".join("\\" + ch for ch in symbol), token_type) for symbol in symbols)))


class CypherLexer(RegexLexer):
    """ Pygments lexer for the `Cypher Query Language
    <https://neo4j.com/docs/cypher-refcard/current/>`_
    as available in Neo4j 4.2.
    """
    name = "Cypher"
    aliases = ["cypher", "py2neo.cypher"]
    filenames = ["*.cypher", "*.cyp"]

    flags = re.IGNORECASE | re.MULTILINE | re.UNICODE

    tokens = {

        'root': [
            include('strings'),
            include('comments'),
            include('keywords'),
            include('pseudo-keywords'),
            include('escape-commands'),
            (r'[,;]', Punctuation),
            include('labels'),
            include('operators'),
            include('expressions'),
            include('whitespace'),
            (r'\(', Punctuation, 'in-()'),
            (r'\[', Punctuation, 'in-[]'),
            (r'\{', Punctuation, 'in-{}'),
        ],
        'in-()': [
            include('strings'),
            include('comments'),
            include('keywords'),        # keywords used in FOREACH
            (r'[,|]', Punctuation),
            include('labels'),
            include('operators'),
            include('expressions'),
            include('whitespace'),
            (r'\(', Punctuation, '#push'),
            (r'\)\s*<?-+>?\s*\(', Punctuation),
            (r'\)\s*<?-+\s*\[', Punctuation, ('#pop', 'in-[]')),
            (r'\)', Punctuation, '#pop'),
            (r'\[', Punctuation, 'in-[]'),
            (r'\{', Punctuation, 'in-{}'),
        ],
        'in-[]': [
            include('strings'),
            include('comments'),
            (r'WHERE\b', Keyword),      # used in list comprehensions
            (r'[,|]', Punctuation),
            include('labels'),
            include('operators'),
            include('expressions'),
            include('whitespace'),
            (r'\(', Punctuation, 'in-()'),
            (r'\[', Punctuation, '#push'),
            (r'\]\s*-+>?\s*\(', Punctuation, ('#pop', 'in-()')),
            (r'\]', Punctuation, '#pop'),
            (r'\{', Punctuation, 'in-{}'),
        ],
        'in-{}': [
            include('strings'),
            include('comments'),
            (r'[,:]', Punctuation),
            include('operators'),
            include('expressions'),
            include('whitespace'),
            (r'\(', Punctuation, 'in-()'),
            (r'\[', Punctuation, 'in-[]'),
            (r'\{', Punctuation, '#push'),
            (r'\}', Punctuation, '#pop'),
        ],

        'comments': [
            (r'//', Comment.Single, 'single-comments'),
            (r'/\*', Comment.Multiline, 'multiline-comments'),
        ],
        'single-comments': [
            (r'.*$', Comment.Single, '#pop'),
        ],
        'multiline-comments': [
            (r'/\*', Comment.Multiline, 'multiline-comments'),
            (r'\*/', Comment.Multiline, '#pop'),
            (r'[^/*]+', Comment.Multiline),
            (r'[/*]', Comment.Multiline)
        ],

        'strings': [
            # TODO: highlight escape sequences
            (r"'(?:\\[bfnrt\"'\\]|\\u[0-9A-Fa-f]{4}|\\U[0-9A-Fa-f]{8}|[^\\'])*'", String),
            (r'"(?:\\[bfnrt\'"\\]|\\u[0-9A-Fa-f]{4}|\\U[0-9A-Fa-f]{8}|[^\\"])*"', String),
        ],

        'keywords': word_list(cypher_keywords, Keyword),
        'pseudo-keywords': word_list(cypher_pseudo_keywords, Keyword),

        'escape-commands': [
            (r'^\s*\/(?!\/).*$', Comment.Single),
            (r'^\s*:.*$', Comment.Single),
        ],

        'labels': [
            (r'(:)(\s*)(`(?:``|[^`])+`)', bygroups(Punctuation, Whitespace, Name.Label)),
            (r'(:)(\s*)([A-Za-z_][0-9A-Za-z_]*)', bygroups(Punctuation, Whitespace, Name.Label)),
        ],

        'operators': (word_list(cypher_operator_words, Operator) +
                      symbol_list(cypher_operator_symbols, Operator)),

        'expressions': [
            include('procedures'),
            include('functions'),
            include('constants'),
            include('aliases'),
            include('variables'),
            include('parameters'),
            include('numbers'),
        ],
        'procedures': [
            (r'(CALL)(\s+)([A-Za-z_][0-9A-Za-z_\.]*)', bygroups(Keyword, Whitespace, Name.Function)),
        ],
        'functions': [
            (r'([A-Za-z_][0-9A-Za-z_\.]*)(\s*)(\()', bygroups(Name.Function, Whitespace, Punctuation), "in-()"),
        ],
        'aliases': [
            (r'(AS)(\s+)(`(?:``|[^`])+`)', bygroups(Keyword, Whitespace, Name.Variable)),
            (r'(AS)(\s+)([A-Za-z_][0-9A-Za-z_]*)', bygroups(Keyword, Whitespace, Name.Variable)),
        ],
        'variables': [
            (r'`(?:``|[^`])+`', Name.Variable),
            (r'[A-Za-z_][0-9A-Za-z_]*', Name.Variable),
        ],
        'parameters': [
            (r'(\$)(`(?:``|[^`])+`)', bygroups(Punctuation, Name.Variable.Global)),
            (r'(\$)([A-Za-z_][0-9A-Za-z_]*)', bygroups(Punctuation, Name.Variable.Global)),
        ],
        'constants': word_list(cypher_constants, Name.Constant),
        'numbers': [
            (r'[0-9]*\.[0-9]*(e[+-]?[0-9]+)?', Number.Float),
            (r'[0-9]+e[+-]?[0-9]+', Number.Float),
            (r'[0-9]+', Number.Integer),
        ],

        'whitespace': [
            (r'\s+', Whitespace),
        ],

    }

    def get_statements(self, text):
        """ Split the text into statements delimited by semicolons and
        yield each statement in turn. Yielded statements are stripped
        of both leading and trailing whitespace. Empty statements are
        skipped.
        """
        fragments = []
        for index, token_type, value in self.get_tokens_unprocessed(text):
            if token_type == Punctuation and value == ";":
                statement = "".join(fragments).strip()
                fragments[:] = ()
                if statement:
                    yield statement
            else:
                fragments.append(value)
        statement = "".join(fragments).strip()
        if statement:
            yield statement
