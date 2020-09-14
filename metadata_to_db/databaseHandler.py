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
        self.defaultConnection = self.openConnection(openDefault=True)
        self.defaultConnection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        if not self.dbExists(self.dbInfo["database"]):
            self.createNewDb(self.dbInfo["database"])

        # Open cursor to the server specified in the config file
        self.connection = self.openConnection()
        self.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    def openConnection(self, openDefault=False):
        """Opens a connection to DB.
        
        Parameters
        ----------
        openDefault : bool
            Open a connection to the DB named postgres, otherwise to the DB specified in config file
        """
        params = self.dbInfo.copy()
        if openDefault:
            params['database'] = 'postgres'
        logging.info("Opening connection to DB: %s", params['database'])
        return psycopg2.connect(**params)

    def closeConnection(self, connection):
        logging.info('Closing connection')
        if connection is not None:
            connection.close()

    def openCursor(self, connection):
        return connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def closeCursor(self, cursor):
        logging.info('Closing cursor')
        try:
            cursor.close()
        except (psycopg2.DatabaseError) as error:
            logging.warning(error)

    def checkServerConnection(self):
        logging.info('Checking connection to Postgres server')
        if self.executeQuery(self.connection, 'SELECT version()').fetchone() is not None:
            logging.info('Server connection confirmed')

    def dbExists(self, dbName):
        result = None
        sqlQuery = 'SELECT datname FROM pg_catalog.pg_database WHERE datname=\'' + dbName + '\''
        if self.executeQuery(self.defaultConnection, sqlQuery).fetchone() is None:
            result = False
        else:
            result = True
        logging.debug("DB named %s existence: %s", dbName, str(result))
        return result

    def tableExists(self, tableName):
        result = None
        sqlQuery = "SELECT * FROM information_schema.tables WHERE table_name=\'" + tableName + "\';"
        if self.executeQuery(self.connection, sqlQuery).fetchone() is None:
            result = False
        else:
            result = True
        logging.info('Table %s exists: %s', tableName, str(result))
        return result

    def countRecords(self, tableName):
        """Checks the count of records in a table."""
        sqlQuery = 'SELECT COUNT(*) FROM ' + tableName + ';'
        return self.executeQuery(self.connection, sqlQuery).fetchone()[0]

    def dropTable(tableName):
        logging.info('Attempting to drop table: %s', tableName)
        self.executeQuery(self.connection, 'DROP TABLE ' + tableName + ';')
        logging.info("Dropped table: %s", tableName)

    def addTableToDb(self, tableName, columnsInfoPath, sectionName):
        logging.info('Attempting to add table')

        # Open the json with the list of columns we're interested in
        with open(columnsInfoPath) as fileReader:
            columnsInfo = json.load(fileReader)
        columns = columnsInfo[sectionName]

        # Make the SQL query
        sqlQuery = 'CREATE TABLE ' + tableName + ' (' + os.linesep + 'file_name VARCHAR(255) PRIMARY KEY,' + \
            os.linesep + 'file_path VARCHAR(255),' + os.linesep
        for columnName in columns:
            if not columns[columnName]['calculation_only']:
                sqlQuery = sqlQuery + columnName + ' ' + columns[columnName]['db_datatype'] + ',' + os.linesep
        marginToRemove = -1 * (len(os.linesep) + 1)
        sqlQuery = sqlQuery[:marginToRemove] + ');'
        self.executeQuery(self.connection, sqlQuery)
        self.tableExists(tableName)

    def createNewDb(self, dbName):
        logging.info('Attempting to create a new DB')
        self.executeQuery(self.defaultConnection, 'CREATE DATABASE ' + dbName + ';')
        self.dbExists(dbName)

    def dropDb(self, dbName):
        logging.info('Attempting to drop a new DB')
        self.closeConnection(self.connection)
        self.executeQuery(self.defaultConnection, 'DROP DATABASE ' + dbName + ';')
        self.dbExists(dbName)

    def executeQuery(self, connection, query, values=None):
        cursor = self.openCursor(connection)
        try:
            if values is None:
                cursor.execute(query)
            else:
                cursor.execute(query, values)
        except (psycopg2.DatabaseError) as error:
            logging.warning(error)
        return cursor
