#!/usr/bin/env python3
"""
Generic database connector class for PostgresQL databases. It includes a built-in logging system (logger)

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 4/10/23
"""

from .logger import LoggerSuperclass, PRL
import psycopg2
import time
import pandas as pd
import traceback


class Connection(LoggerSuperclass):
    def __init__(self, host, port, db_name, db_user, db_password, timeout, logger, count):
        LoggerSuperclass.__init__(self, logger, f"PGCON{count}", colour=PRL)
        self.info("Creating connection")
        self.__host = host
        self.__port = port
        self.__name = db_name
        self.__user = db_user
        self.__pwd = db_password
        self.__timeout = timeout

        self.__connection_count = count

        self.available = True  # flag that determines if this connection is available or not
        # Create a connection
        self.connection = psycopg2.connect(host=self.__host, port=self.__port, dbname=self.__name, user=self.__user,
                                           password=self.__pwd, connect_timeout=self.__timeout)
        self.cursor = self.connection.cursor()
        self.last_used = -1

        self.index = 0
        self.__closing = False

    def run_query(self, query, description=False, debug=False, fetch=True):
        """
        Executes a query and returns the result. If description=True the desription will also be returned
        """
        self.available = False
        if debug:
            self.debug(query)
        self.cursor.execute(query)
        self.connection.commit()
        if fetch:
            resp = self.cursor.fetchall()
            self.available = True
            if description:
                return resp, self.cursor.description
            return resp
        else:
            self.available = True
            return

    def close(self):
        if not self.__closing:
            self.__closing = True
            self.info(f"Closing connection")
            self.connection.close()
        else:
            self.error(f"Someone else is closing connection {self.__connection_count}!")


class PgDatabaseConnector(LoggerSuperclass):
    """
    Interface to access a PostgresQL database
    """

    def __init__(self, host, port, db_name, db_user, db_password, logger, timeout=5):
        LoggerSuperclass.__init__(self, logger, "PostgresQL")
        self.conn_count = 0
        self.__host = host
        self.__port = port
        self.__name = db_name
        self.__user = db_user
        self.__pwd = db_password
        self.__timeout = timeout
        self.__logger = logger

        self.query_time = -1  # stores here the execution time of the last query
        self.db_initialized = False
        self.connections = []  # list of connections, starts with one
        self.max_connections = 50

    def new_connection(self) -> Connection:
        self.conn_count += 1
        c = Connection(self.__host, self.__port, self.__name, self.__user, self.__pwd, self.__timeout, self.__logger,
                       self.conn_count)
        self.connections.append(c)
        return c

    def get_available_connection(self):
        """
        Loops through the connections and gets the first available. If there isn't any available create a new one (or
        wait if connections reached the limit).
        """

        for i in range(len(self.connections)):
            c = self.connections[i]
            if c.available:
                return c

        while len(self.connections) >= self.max_connections:
            time.sleep(0.5)
            self.debug("waiting for conn")

        self.info(f"Creating DB connection {len(self.connections)}..")
        return self.new_connection()

    def exec_query(self, query, description=False, debug=False, fetch=True):
        """
        Runs a query in a free connection
        """
        c = self.get_available_connection()
        results = None
        try:
            results = c.run_query(query, description=description, debug=debug, fetch=fetch)

        except psycopg2.errors.UniqueViolation as e:
            # most likely a duplicated key, raise it again
            c.connection.rollback()
            c.available = True  # set it to available
            self.error(f"Exception caught!:\n{traceback.format_exc()}")
            raise e

        except Exception as e:
            self.warning(f"Exception caught!:\n{traceback.format_exc()}")
            self.error(f"Exception in exec_query {e}")
            try:
                self.warning("closing db connection due to exception")
                c.close()
            except:  # ignore errors
                pass
            self.error(f"Removing connection")
            self.connections.remove(c)
        return results

    def list_from_query(self, query, debug=False):
        """
        Makes a query to the database using a cursor object and returns a DataFrame object
        with the reponse
        :param query: string with the query
        :param debug:
        :returns list with the query result
        """
        return self.exec_query(query, debug=debug)

    def dataframe_from_query(self, query, debug=False):
        """
        Makes a query to the database using a cursor object and returns a DataFrame object
        with the reponse
        :param cursor: database cursor
        :param query: string with the query
        :param debug:
        :returns DataFrame with the query result
        """
        response, description = self.exec_query(query, debug=debug, description=True)
        colnames = [desc[0] for desc in description]  # Get the Column names
        df = pd.DataFrame(response, columns=colnames)
        return df

    def close(self):
        for c in self.connections:
            c.close()
