"""Module contains class for handling PostgreSQL DB."""
import os
import sys
import json
import logging
import psycopg2
import psycopg2.extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import atexit

class DatabaseHandler:
    def __init__(self, configHandler):
        self.configHandler = configHandler
        self.dbInfo = self.configHandler.getDbInfo()
        
        # Open cursor to the default server (named postgres)
        self.defaultConnection = self.openConnection(openDefault=True)

        if not self.dbExists(self.dbInfo["database"]):
            self.createNewDb(self.dbInfo["database"])

        # Open cursor to the server specified in the config file
        self.connection = self.openConnection()
        atexit.register(self.closeAllConnections)

    def closeAllConnections(self):
        self.closeConnection(self.defaultConnection)
        self.closeConnection(self.connection)

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
        connection = psycopg2.connect(**params)
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return connection

    def closeConnection(self, connection):
        logging.info('Closing connection')
        if connection is not None:
            connection.close()

    def openCursor(self, connection):
        return connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def closeCursor(self, cursor):
        logging.debug('Closing cursor')
        try:
            cursor.close()
        except (psycopg2.DatabaseError) as error:
            logging.warning(error)

    def checkServerConnection(self):
        logging.info('Checking connection to Postgres server')
        if self.executeQuery(self.connection, 'SELECT version()').fetchone() is not None:
            logging.info('Server is connected')
        else:
            logging.info("Server is not connected")

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
        result = None
        if self.tableExists(tableName):
            sqlQuery = 'SELECT COUNT(*) FROM \"' + tableName + '\";'
            result = self.executeQuery(self.connection, sqlQuery).fetchone()[0]
        else:
            result = 0
        return result

    def dropTable(self, tableName):
        logging.info('Attempting to drop table: %s', tableName)
        self.executeQuery(self.connection, 'DROP TABLE ' + tableName + ';')
        logging.info("Dropped table: %s", tableName)

    def addTableToDb(self, tableName, columnsInfoPath, nonElementSectionName, elementSectionName):
        logging.info('Attempting to add table')

        # Open the json with the list of columns we're interested in
        with open(columnsInfoPath) as fileReader:
            columnsInfo = json.load(fileReader)
        
        # Make the SQL query
        sqlQuery = 'CREATE TABLE \"' + tableName + '\" ('

        nonElementsColumns = columnsInfo[nonElementSectionName]
        for columnName in nonElementsColumns:
            sqlQuery = sqlQuery + "\"" + columnName + '\" ' + nonElementsColumns[columnName]['db_datatype'] + " " + nonElementsColumns[columnName]['constraints'] + ','

        elementColumns = columnsInfo[elementSectionName]
        for columnName in elementColumns:
            if not elementColumns[columnName]['calculation_only']:
                sqlQuery = sqlQuery + "\"" + columnName + '\" ' + elementColumns[columnName]['db_datatype'] + ','

        sqlQuery = sqlQuery[:-1] + ');'
        self.executeQuery(self.connection, sqlQuery)
        self.tableExists(tableName)

    def createNewDb(self, dbName):
        logging.info('Attempting to create a new DB')
        self.executeQuery(self.defaultConnection, 'CREATE DATABASE \"' + dbName + '\";')
        self.dbExists(dbName)

    def dropDb(self, dbName):
        logging.info('Attempting to drop a new DB')
        self.closeConnection(self.connection)
        self.executeQuery(self.defaultConnection, 'DROP DATABASE \"' + dbName + '\";')
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