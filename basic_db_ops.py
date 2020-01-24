"""Module contains functions that are basic and common DB operations."""
import os
import json
import logging
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from config import config

def check_server_connection(db_config_file_name):
    """Check the connection to a PostgreSQL DB server.

    Parameters
    ----------
    db_config_file_name : string
        The file name of the INI file that contains the information on the DB server
    """
    logging.info('Running test for server connection')
    conn = None
    try:
        # read connection parameters
        relevant_section = 'postgresql'
        logging.info('Reading configuration information from %s of %s', relevant_section,
                     db_config_file_name)
        params = config(filename=db_config_file_name, section=relevant_section)
        params['database'] = 'postgres'
        logging.info(params)

        # connect to the PostgreSQL server
        logging.info('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        logging.info('Connection established')

        # execute a statement
        logging.info('Checking for database version')
        cur.execute('SELECT version()')
        db_version = cur.fetchone()
        logging.info('PostgreSQL database version: %s', db_version)

       # close the cursor with the PostgreSQL
        cur.close()

    # If an exception is raised along the way, report it
    except (psycopg2.DatabaseError) as error:
        logging.warning(error)

    # At the end, if the connection still exists then close it
    finally:
        if conn is not None:
            logging.info('Attempting to close connection')
            conn.close()
            logging.info('Database connection closed.')

def db_exists(db_config_file_name, db_name):
    """Check the existence of a DB in a PostgreSQL DB server.

    Parameters
    ----------
    db_config_file_name : string
        The file name of the INI file that contains the information on the DB server
    db_name : string
        The name of the database we wish to check the existence of
    """
    logging.info('Checking for existence of DB')
    conn = None
    result = None
    try:
        # read connection parameters
        relevant_section = 'postgresql'
        logging.info('Reading configuration information from %s of %s', relevant_section,
                     db_config_file_name)
        params = config(filename=db_config_file_name, section=relevant_section)
        params['database'] = 'postgres'
        logging.info(params)

        # connect to the PostgreSQL server
        logging.info('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        logging.info('Connection established')

        # execute a statement
        logging.info('Checking for database version')
        sql_query = 'SELECT datname FROM pg_catalog.pg_database WHERE datname=\'' + db_name + '\''
        cur.execute(sql_query)
        if cur.fetchone() is None:
            result = False
        else:
            result = True

       # close the cursor with the PostgreSQL
        cur.close()

    # If an exception is raised along the way, report it
    except (psycopg2.DatabaseError) as error:
        logging.warning(error)

    # At the end, if the connection still exists then close it
    finally:
        if conn is not None:
            logging.info('Attempting to close connection')
            conn.close()
            logging.info('Database connection closed.')
    return result

def table_exists(db_config_file_name, db_name, table_name):
    """Check the existence of a table in a DB in a PostgreSQL DB server.

    Parameters
    ----------
    db_config_file_name : string
        The file name of the INI file that contains the information on the DB server
    db_name : string
        The name of the database we wish to check
    table_name : string
        The name of the table we wish to check the existence of
    """
    logging.info('Checking for existence of table in DB')
    conn = None
    result = None
    try:
        # read connection parameters
        relevant_section = 'postgresql'
        logging.info('Reading configuration information from %s of %s', relevant_section,
                     db_config_file_name)
        params = config(filename=db_config_file_name, section=relevant_section)
        params['database'] = db_name
        logging.info(params)

        # connect to the PostgreSQL server
        logging.info('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        logging.info('Connection established')

        # execute a statement
        logging.info('Checking for database version')
        sql_query = "SELECT * FROM information_schema.tables WHERE table_name=%s"
        cur.execute(sql_query, (table_name,))
        if cur.fetchone() is None:
            result = False
        else:
            result = True

       # close the cursor with the PostgreSQL
        cur.close()

    # If an exception is raised along the way, report it
    except (psycopg2.DatabaseError) as error:
        logging.warning(error)

    # At the end, if the connection still exists then close it
    finally:
        if conn is not None:
            logging.info('Attempting to close connection')
            conn.close()
            logging.info('Database connection closed.')
    return result

def drop_table(table_name, db_config_file_name):
    """Drop a table in the desired DB.

    Parameters
    ----------
    table_name : string
        The name of the table in the DB specified in db_config_file_name
    db_config_file_name : string
        The file name of the INI file that contains the information on the DB server
    """
    logging.info('Attempting to drop table')
    conn = None
    try:
        # read the connection parameters
        relevant_section = 'postgresql'
        logging.info('Reading configuration information from %s of %s', relevant_section,
                     db_config_file_name)
        params = config(filename=db_config_file_name, section='postgresql')
        logging.info(params)

        # connect to the PostgreSQL server
        logging.info('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        logging.info('Connection established')

        # drop table
        cur.execute('DROP TABLE ' + table_name + ';')

        # close communication with the PostgreSQL database server
        cur.close()

        # commit the changes
        conn.commit()

        logging.info('Table successfully dropped')
    except (psycopg2.DatabaseError) as error:
        logging.warning(error)
    finally:
        if conn is not None:
            logging.info('Attempting to close connection')
            conn.close()
            logging.info('Database connection closed.')

def add_table_to_db(table_name, elements_json, db_config_file_name, section_name):
    """Add a table to the desired DB.

    Parameters
    ----------
    table_name : string
        Name of the table to be added to the DB
    elements_json : string
        Name of the JSON containing the list of elements. Each element name that is not
        calculation_only will be a column in the new table
    db_config_file_name : string
        The file name of the INI file that contains the information on the DB server
    section_name : string
        Name of the section in the elements_json that has the column info for that table
    """
    logging.info('Attempting to add table to DB')

    # Open the json with the list of elements we're interested in
    logging.info('Reading the desired column info in from section %s in the %s',
                 section_name, elements_json)
    with open(elements_json) as file_reader:
        elements_json = json.load(file_reader)
    elements = elements_json[section_name]

    # Make the SQL query
    sql_query = 'CREATE TABLE ' + table_name + ' (' + os.linesep + \
        'file_path VARCHAR(255) PRIMARY KEY,' + os.linesep
    for element_name in elements:
        if not elements[element_name]['calculation_only']:
            sql_query = sql_query + element_name + ' ' + elements[element_name]['db_datatype'] \
                + ',' + os.linesep
    sql_query = sql_query[:-2] + ');'

    conn = None
    try:
        # read the connection parameters
        relevant_section = 'postgresql'
        logging.info('Reading configuration information from %s of %s', relevant_section,
                     db_config_file_name)
        params = config(filename=db_config_file_name, section='postgresql')
        logging.info(params)

        # connect to the PostgreSQL server
        logging.info('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        logging.info('Connection established')

        # create table one by one
        cur.execute(sql_query)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
        logging.info('Table successfully added.')
    except (psycopg2.DatabaseError) as error:
        logging.warning(error)
    finally:
        if conn is not None:
            logging.info('Attempting to close connection')
            conn.close()
            logging.info('Database connection closed.')

# TODO: Rewrite this function to be more flexible
def create_new_db(db_name):
    """Create a new DB.

    Parameters
    ----------
    db_name : string
        Name of the new DB
    """
    logging.info('Attempting to create a new DB')
    try:
        # connect to the PostgreSQL server
        logging.info('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(dbname='postgres', user='postgres', host='localhost',
                                password='postgres')
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        logging.info('Connection established')

        # Create database
        cur.execute('CREATE DATABASE ' + db_name + ';')
        cur.close()
        conn.commit()
        logging.info('Database successfully created')
    except (psycopg2.DatabaseError) as error:
        logging.warning(error)
    finally:
        if conn is not None:
            logging.info('Attempting to close connection')
            conn.close()
            logging.info('Database connection closed.')

if __name__ == "__main__":
    logging.basicConfig(filename='basic_db_ops.log', level=logging.DEBUG)
    print(db_exists('config.ini', 'nifti_test'))