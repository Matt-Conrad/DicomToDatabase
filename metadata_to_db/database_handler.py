"""Module contains class for handling PostgreSQL DB."""
import os
import sys
import json
import logging
import psycopg2
import psycopg2.extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
# This line is so modules using this package as a submodule can use this.
sys.path.append(os.path.dirname(os.path.abspath(__file__)).replace('\\', '/'))
#

class DatabaseHandler:
    def __init__(self, configHandler):
        self.configHandler = configHandler
        self.dbInfo = self.configHandler.getDbInfo()
        
        # Open cursor to the default server (named postgres)
        self.default_connection = self.openConnection(open_default=True)
        self.default_connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        if not self.db_exists(self.dbInfo["database"]):
            self.create_new_db(self.dbInfo["database"])

        # Open cursor to the server specified in the config file
        self.connection = self.openConnection()
        self.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    def __del__(self):
        self.closeConnection(self.default_connection)
        self.closeConnection(self.connection)

    def openConnection(self, open_default=False):
        """Opens a connection to DB.
        
        Parameters
        ----------
        open_default : bool
            Open a connection to the DB named postgres, otherwise to the DB specified in config file
        """
        params = self.dbInfo.copy()
        if open_default:
            params['database'] = 'postgres'
        logging.info("Opening connection to DB: %s", params['database'])
        return psycopg2.connect(**params)

    def openCursor(self, connection):
        logging.info("Opening cursor in given connection")
        return connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def closeConnection(self, connection):
        logging.info('Closing connection')
        if connection is not None:
            connection.close()

    def closeCursor(self, cursor):
        logging.info('Closing cursor')
        try:
            cursor.close()
        except (psycopg2.DatabaseError) as error:
            logging.warning(error)

    def check_server_connection(self):
        logging.info('Checking connection to Postgres server')
        if self.executeQuery(self.connection, 'SELECT version()', fetchHowMany="one") is not None:
            logging.info('Server connection confirmed')

    def db_exists(self, db_name):
        result = None
        sql_query = 'SELECT datname FROM pg_catalog.pg_database WHERE datname=\'' + db_name + '\''
        if self.executeQuery(self.default_connection, sql_query, fetchHowMany="one") is None:
            result = False
        else:
            result = True
        logging.debug("DB named %s existence: %s", db_name, str(result))
        return result

    def table_exists(self, table_name):
        result = None
        sql_query = "SELECT * FROM information_schema.tables WHERE table_name=\'" + table_name + "\';"
        if self.executeQuery(self.connection, sql_query, fetchHowMany="one") is None:
            result = False
        else:
            result = True
        logging.info('Table %s exists: %s', table_name, str(result))
        return result

    def count_records(self, table_name):
        """Checks the count of records in a table."""
        sql_query = 'SELECT COUNT(*) FROM ' + table_name + ';'
        return self.executeQuery(self.connection, sql_query, fetchHowMany="one")[0]

    def drop_table(table_name):
        logging.info('Attempting to drop table: %s', table_name)
        self.executeQuery(self.connection, 'DROP TABLE ' + table_name + ';')
        logging.info("Dropped table: %s", table_name)

    def add_table_to_db(self, table_name, columns_info_path, section_name):
        logging.info('Attempting to add table')

        # Open the json with the list of columns we're interested in
        with open(columns_info_path) as file_reader:
            columns_info = json.load(file_reader)
        columns = columns_info[section_name]

        # Make the SQL query
        sql_query = 'CREATE TABLE ' + table_name + ' (' + os.linesep + 'file_name VARCHAR(255) PRIMARY KEY,' + \
            os.linesep + 'file_path VARCHAR(255),' + os.linesep
        for column_name in columns:
            if not columns[column_name]['calculation_only']:
                sql_query = sql_query + column_name + ' ' + columns[column_name]['db_datatype'] + ',' + os.linesep
        margin_to_remove = -1 * (len(os.linesep) + 1)
        sql_query = sql_query[:margin_to_remove] + ');'
        self.executeQuery(self.connection, sql_query)
        self.table_exists(table_name)

    def create_new_db(self, db_name):
        logging.info('Attempting to create a new DB')
        self.executeQuery(self.default_connection, 'CREATE DATABASE ' + db_name + ';')
        self.db_exists(db_name)
    
    def executeQuery(self, connection, query, values=None, fetchHowMany=None):
        """Executes a query in the desired cursor."""
        result = None
        cursor = self.openCursor(connection)
        try:
            if values is None:
                cursor.execute(query)
            else:
                cursor.execute(query, values)

            if fetchHowMany == "all":
                result = cursor.fetchall()
            elif fetchHowMany == "one":
                result = cursor.fetchone()
            else:
                pass

        except (psycopg2.DatabaseError) as error:
            logging.warning(error)
        
        return result

    def executeQuery2(self, cursor, query, values=None):
        """Executes a query in the desired cursor."""
        try:
            if values is None:
                cursor.execute(query)
            else:
                cursor.execute(query, values)

        except (psycopg2.DatabaseError) as error:
            logging.warning(error)

