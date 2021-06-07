#!/usr/bin/env python
# coding: utf-8

# Copyright 2020, Nigel Small
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

from logging import getLogger, Formatter, StreamHandler, \
    DEBUG, INFO, WARNING, ERROR, CRITICAL
from os.path import expanduser, join, isfile
from readline import read_history_file, write_history_file
from sys import stdout

from pansi import ansi
from six import PY2
from six.moves import input


class ConsoleLogFormatter(Formatter):
    """ Colour formatter for pretty log output.
    """

    def formatTime(self, record, datefmt=None):
        s = super(ConsoleLogFormatter, self).formatTime(record, datefmt)
        return "{BLACK}{}{_}".format(s, **ansi)

    @classmethod
    def _paint_message(cls, message, level):
        if level == DEBUG:
            return "{cyan}{}{_}".format(message, **ansi)
        elif level == WARNING:
            return "{yellow}{}{_}".format(message, **ansi)
        elif level == ERROR:
            return "{red}{}{_}".format(message, **ansi)
        elif level == CRITICAL:
            return "{RED}{}{_}".format(message, **ansi)
        else:
            return message

    def formatMessage(self, record):
        record.message = self._paint_message(record.message, record.levelno)
        return super(ConsoleLogFormatter, self).formatMessage(record)

    def format(self, record):
        if PY2:
            old_fmt = self._fmt
            try:
                self._fmt = self._paint_message(self._fmt, record.levelno)
                return super(ConsoleLogFormatter, self).format(record)
            finally:
                self._fmt = old_fmt
        else:
            return super(ConsoleLogFormatter, self).format(record)


class Console(object):
    """ Basic interactive command line console.
    """

    prompt = "{cyan}->{_} ".format(**ansi)

    def __init__(self, name, out=stdout, verbosity=None, history=None, time_format=None):
        self.name = name
        self.__out = out
        self.__history = history or expanduser(join("~", ".%s.history" % name))
        self.__looping = False
        self.__status = 0
        self.__verbosity = 0

        if time_format is None:
            self.__formatter = ConsoleLogFormatter("%(message)s")
        else:
            self.__formatter = ConsoleLogFormatter("%(asctime)s  %(message)s", time_format)
        self.__handler = StreamHandler(self.__out)
        self.__handler.setFormatter(self.__formatter)
        self.__log = getLogger(self.name)
        self.__log.addHandler(self.__handler)

        if verbosity is not None:
            self.verbosity = verbosity

    def __del__(self):
        self.__log.removeHandler(self.__handler)

    @property
    def verbosity(self):
        return self.__verbosity

    @verbosity.setter
    def verbosity(self, value):
        self.__verbosity = value
        if self.__verbosity >= 1:
            self.__log.setLevel(DEBUG)
        elif self.__verbosity == 0:
            self.__log.setLevel(INFO)
        elif self.__verbosity == -1:
            self.__log.setLevel(WARNING)
        elif self.__verbosity == -2:
            self.__log.setLevel(ERROR)
        else:
            self.__log.setLevel(CRITICAL)

    def write(self, *values, **kwargs):
        sep = kwargs.get("sep", " ")
        end = kwargs.get("end", "\n")
        if self.verbosity >= 0:
            print(*values, sep=sep, end=end, file=self.__out)

    def debug(self, msg, *args, **kwargs):
        self.__log.debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self.__log.info(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self.__log.warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.__log.error(msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self.__log.critical(msg, *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        self.__log.exception(msg, *args, **kwargs)

    def loop(self, process_empty_input=False):
        if isfile(self.__history):
            read_history_file(self.__history)
        try:
            self.__looping = True
            while self.__looping:
                try:
                    line = self.read()
                except KeyboardInterrupt:   # Ctrl+C
                    print()
                    continue
                except EOFError:            # Ctrl+D
                    print()
                    self.exit()
                else:
                    if line or process_empty_input:
                        self.__status = self.process(line)
        finally:
            write_history_file(self.__history)

    def exit(self):
        self.__looping = False

    def read(self):
        """ Get input.
        """
        return input(self.prompt)

    def process(self, line):
        """ Handle input.
        """
        self.write("OK")
        return 0


def watch(name, verbosity=1, time_format="%H:%M:%S"):
    return Console(name, verbosity=verbosity, time_format=time_format)


if __name__ == "__main__":
    Console(__name__).loop()
