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
This module provides an implementation of the Python Database API v2.0
(as specified in PEP 249) for Neo4j. The classes here are thin wrappers
over the regular py2neo functionality, so will provide a very similar
behaviour and experience to the rest of the library.

To get started, create a :class:`Connection` object using the
:func:`connect` function::

>>> from py2neo.pep249 import connect
>>> con = connect()

The arguments accepted by this function are identical to those accepted
by the :class:`.Graph` class in the core API. Therefore, a URI and any
combination of individual settings may be specified.

The :class:`Connection` object represents a single
client-server connection and can maintain a single transaction at any
point in time. Transactions are implicitly created through using a
method like :meth:`Cursor.execute` or can be explicitly
created using the :meth:`Connection.begin` method.

"""


from datetime import date, time, datetime
from time import localtime

from six import raise_from

from py2neo import Neo4jError
from py2neo.client import (Connection as _Connection,
                           ConnectionProfile as _ConnectionProfile)
from py2neo.errors import ConnectionUnavailable as _ConnectionUnavailable


apilevel = "2.0"
threadsafety = 0        # TODO
paramstyle = "cypher"   # TODO


def _date_from_ticks(ticks):
    return Date(*localtime(ticks)[:3])


def _time_from_ticks(ticks):
    return Time(*localtime(ticks)[3:6])


def _timestamp_from_ticks(ticks):
    return Timestamp(*localtime(ticks)[:6])


Date = date
Time = time
Timestamp = datetime
DateFromTicks = _date_from_ticks
TimeFromTicks = _time_from_ticks
TimestampFromTicks = _timestamp_from_ticks
Binary = memoryview


# noinspection PyShadowingBuiltins
class Warning(Exception):
    """ Exception raised for important warnings like data truncations
    while inserting, etc.
    """


class Error(Exception):
    """ Exception that is the base class of all other error exceptions.
    You can use this to catch all errors with one single except
    statement. Warnings are not considered errors and thus should not
    use this class as base.
    """


class InterfaceError(Error):
    """ Exception raised for errors that are related to the database
    interface rather than the database itself.
    """


class DatabaseError(Error):
    """ Exception raised for errors that are related to the database.
    """


class DataError(DatabaseError):
    """ Exception raised for errors that are due to problems with the
    processed data like division by zero, numeric value out of range,
    etc.
    """


class OperationalError(DatabaseError):
    """ Exception raised for errors that are related to the database's
    operation and not necessarily under the control of the programmer,
    e.g. an unexpected disconnect occurs, the data source name is not
    found, a transaction could not be processed, a memory allocation
    error occurred during processing, etc.
    """


class IntegrityError(DatabaseError):
    """ Exception raised when the relational integrity of the database
    is affected, e.g. a foreign key check fails.
    """


class InternalError(DatabaseError):
    """ Exception raised when the database encounters an internal
    error, e.g. the cursor is not valid anymore, the transaction is
    out of sync, etc.
    """


class ProgrammingError(DatabaseError):
    """ Exception raised for programming errors, e.g. table not found
    or already exists, syntax error in the SQL statement, wrong number
    of parameters specified, etc.
    """


class NotSupportedError(DatabaseError):
    """ Exception raised in case a method or database API was used
    which is not supported by the database, e.g. requesting a
    .rollback() on a connection that does not support transaction or
    has transactions turned off.
    """


class Connection(object):
    """ PEP249-compliant connection to a Neo4j server.
    """

    Warning = Warning
    Error = Error
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError

    def __init__(self, profile=None, **settings):
        profile = _ConnectionProfile(profile, **settings)
        try:
            self._cx = _Connection.open(profile)
        except _ConnectionUnavailable as error:
            raise_from(self.OperationalError("Connection unavailable"), error)
        self._tx = None
        self._db = None

    def __check__(self):
        if self._cx is None or self._cx.closed:
            raise self.ProgrammingError("Connection is closed")
        if self._cx.broken:
            raise self.OperationalError("Connection is broken")

    def __execute__(self, query, parameters=None):
        try:
            result = self._cx.run(self._tx, query, parameters)
            self._cx.pull(result)
        except Neo4jError as error:
            self._tx = None
            raise_from(self.OperationalError("Failed to execute query"), error)
        else:
            return result

    def begin(self):
        """ Begin a transaction.

        :raises ProgrammingError: if the connection is closed
        :raises OperationalError: if the connection is broken or
            if the transaction fails to begin
        """
        self.rollback()
        try:
            self._tx = self._cx.begin(self._db)
        except Neo4jError as error:
            raise_from(self.OperationalError("Failed to begin transaction"), error)

    def commit(self):
        """ Commit any pending transaction to the database.

        :raises ProgrammingError: if the connection is closed
        :raises OperationalError: if the connection is broken or
            if the transaction fails to commit
        """
        self.__check__()
        if self._tx is not None:
            try:
                self._cx.commit(self._tx)
            except Neo4jError as error:
                raise_from(self.OperationalError("Failed to commit transaction"), error)
            finally:
                self._tx = None

    def rollback(self):
        """ Rollback any pending transaction.

        :raises ProgrammingError: if the connection is closed
        :raises OperationalError: if the connection is broken or
            if the transaction fails to rollback
        """
        self.__check__()
        if self._tx is not None:
            try:
                self._cx.rollback(self._tx)
            except Neo4jError as error:
                raise_from(self.OperationalError("Failed to rollback transaction"), error)
            finally:
                self._tx = None

    @property
    def in_transaction(self):
        """ True if a transaction is active, False otherwise.
        """
        return self._tx is not None

    def cursor(self):
        """ Construct a new :class:`.Cursor` object for this connection.
        """
        self.__check__()
        return Cursor(self)

    def execute(self, query, parameters=None):
        """ Execute a query on this connection.
        """
        self.__check__()
        cursor = self.cursor()
        cursor.execute(query, parameters)
        return cursor

    def executemany(self, query, seq_of_parameters):
        """ Execute a query on this connection once for each parameter
        set.
        """
        self.__check__()
        cursor = self.cursor()
        cursor.executemany(query, seq_of_parameters)

    def close(self):
        """ Close the connection immediately.

        The connection will be unusable from this point forward; a
        :class:`.ProgrammingError` exception will be raised if any
        operation is attempted with the connection. The same applies to
        all cursor objects trying to use the connection. Note that
        closing a connection without committing the changes first will
        cause an implicit rollback to be performed.
        """
        if self._cx is not None and not self._cx.closed and not self._cx.broken:
            if self._tx is not None:
                try:
                    self._cx.rollback(self._tx)
                except Neo4jError:
                    pass
                finally:
                    self._tx = None
            self._cx.close()
            self._cx = None


class Cursor(object):
    """ PEP249-compliant cursor attached to a Neo4j server.
    """

    arraysize = 1

    def __init__(self, connection):
        self._connection = connection
        self._result = None
        self._closed = False

    def __iter__(self):
        self.__check__()
        if self._result is None:
            return
        while True:
            record = self._result.take()
            if record is None:
                break
            yield tuple(record)

    def __check__(self):
        if self._closed:
            raise self._connection.ProgrammingError("Cursor is closed")
        self.connection.__check__()

    @property
    def connection(self):
        """ Connection to which this cursor is bound.
        """
        return self._connection

    @property
    def description(self):
        """ Field details, each represented as a 7-tuple.
        """
        if self._result is None:
            return None
        return [(name, None, None, None, None, None, None)
                for name in self._result.fields()]

    @property
    def rowcount(self):
        """ Number of rows affected by the last query executed.
        """
        return -1

    @property
    def summary(self):
        """ Dictionary of summary information relating to the last
        query executed.
        """
        if self._result is None:
            return None
        return self._result.summary()

    def close(self):
        """ Close the cursor immediately.
        """
        self._closed = True

    def execute(self, query, parameters=None):
        """ Execute a query.

        :raises ProgrammingError: if the cursor or the connection is closed
        :raises OperationalError: if the connection is broken
        """
        self.__check__()
        if not self.connection.in_transaction:
            self.connection.begin()
        self._result = self.connection.__execute__(query, parameters)
        return self

    def executemany(self, query, seq_of_parameters):
        """ Execute query multiple times with different parameter sets.

        :raises ProgrammingError: if the cursor or the connection is closed
        :raises OperationalError: if the connection is broken
        """
        self.__check__()
        for parameters in seq_of_parameters:
            self.execute(query, parameters)

    def fetchone(self):
        """ Fetch the next record, if available.

        :returns: record tuple or :py:const:`None`
        :raises ProgrammingError: if the cursor or the connection is closed
        :raises OperationalError: if the connection is broken
        """
        self.__check__()
        if self._result is None:
            return None
        record = self._result.take()
        if record is None:
            return None
        return tuple(record)

    def fetchmany(self, size=None):
        """ Fetch up to `size` records.

        :param size:
        :returns: list of record tuples
        :raises ProgrammingError: if the cursor or the connection is closed
        :raises OperationalError: if the connection is broken
        """
        self.__check__()
        if self._result is None:
            return []
        if size is None:
            size = self.arraysize
        records = []
        for _ in range(size):
            record = self._result.take()
            if record is None:
                break
            records.append(tuple(record))
        return records

    def fetchall(self):
        """ Fetch all remaining records.

        :returns: list of record tuples
        :raises ProgrammingError: if the cursor or the connection is closed
        :raises OperationalError: if the connection is broken
        """
        self.__check__()
        if self._result is None:
            return []
        records = []
        while True:
            record = self._result.take()
            if record is None:
                break
            records.append(tuple(record))
        return records

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass


def connect(profile=None, **settings):
    """ Constructor for creating a connection to the database.
    """
    return Connection(profile, **settings)
