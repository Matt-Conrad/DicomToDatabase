"""Module contains functions that are basic and common DB operations."""
import os
import sys
import json
import logging
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
# This line is so modules using this package as a submodule can use this.
sys.path.append(os.path.dirname(os.path.abspath(__file__)).replace('\\', '/'))
#
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
        params = config(filename=db_config_file_name, section='postgresql')
        params['database'] = 'postgres'

        # connect to the PostgreSQL server
        logging.debug('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        logging.debug('Connection established')

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
            logging.debug('Attempting to close connection')
            conn.close()
            logging.debug('Database connection closed.')

def db_exists(db_config_file_name, db_name):
    """Check the existence of a DB in a PostgreSQL DB server.

    Parameters
    ----------
    db_config_file_name : string
        The file name of the INI file that contains the information on the DB server
    db_name : string
        The name of the database we wish to check the existence of

    Returns
    -------
    bool
        Return True if the DB exists or False if the DB doesn't exist
    """
    logging.info('Checking for existence of DB: %s', db_name)
    conn = None
    result = None
    try:
        # read connection parameters
        params = config(filename=db_config_file_name, section='postgresql')
        params['database'] = 'postgres'

        # connect to the PostgreSQL server
        logging.debug('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        logging.debug('Connection established')

        # execute a statement
        sql_query = 'SELECT datname FROM pg_catalog.pg_database WHERE datname=\'' + db_name + '\''
        cur.execute(sql_query)
        if cur.fetchone() is None:
            result = False
        else:
            result = True

        logging.info('Database %s exists: %s', db_name, result)

       # close the cursor with the PostgreSQL
        cur.close()

    # If an exception is raised along the way, report it
    except (psycopg2.DatabaseError) as error:
        logging.warning(error)

    # At the end, if the connection still exists then close it
    finally:
        if conn is not None:
            logging.debug('Attempting to close connection')
            conn.close()
            logging.debug('Database connection closed.')
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

    Returns
    -------
    bool
        Return True if the table exists or False if the table doesn't exist
    """
    logging.debug('Checking for existence of table %s in DB %s ', table_name, db_name)
    conn = None
    result = None
    try:
        # read connection parameters
        params = config(filename=db_config_file_name, section='postgresql')
        params['database'] = db_name

        # connect to the PostgreSQL server
        logging.debug('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        logging.debug('Connection established')

        # execute a statement
        sql_query = "SELECT * FROM information_schema.tables WHERE table_name=%s"
        cur.execute(sql_query, (table_name,))
        if cur.fetchone() is None:
            result = False
        else:
            result = True

        logging.info('Table %s exists: %s', table_name, result)

       # close the cursor with the PostgreSQL
        cur.close()

    # If an exception is raised along the way, report it
    except (psycopg2.DatabaseError) as error:
        logging.debug(str(error).rstrip())

    # At the end, if the connection still exists then close it
    finally:
        if conn is not None:
            logging.debug('Attempting to close connection')
            conn.close()
            logging.debug('Database connection closed.')
    return result

def count_records(db_config_file_name, db_name, table_name):
    """Checks the count of records in the table in a DB in a PostgreSQL DB server.

    Parameters
    ----------
    db_config_file_name : string
        The file name of the INI file that contains the information on the DB server
    db_name : string
        The name of the database we wish to check
    table_name : string
        The name of the table we wish to check the existence of

    Returns
    -------
    int
        Return the count of records in the table
    """
    logging.debug('Counting the number of records in table %s in DB %s ', table_name, db_name)
    conn = None
    result = None
    try:
        # read connection parameters
        params = config(filename=db_config_file_name, section='postgresql')
        params['database'] = db_name

        # connect to the PostgreSQL server
        logging.debug('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        logging.debug('Connection established')

        # execute a statement
        sql_query = 'SELECT COUNT(*) FROM ' + table_name + ';'
        cur.execute(sql_query, (table_name,))
        result = cur.fetchone()[0]

       # close the cursor with the PostgreSQL
        cur.close()

    # If an exception is raised along the way, report it
    except (psycopg2.DatabaseError) as error:
        logging.warning(error)

    # At the end, if the connection still exists then close it
    finally:
        if conn is not None:
            logging.debug('Attempting to close connection')
            conn.close()
            logging.debug('Database connection closed.')
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
        params = config(filename=db_config_file_name, section='postgresql')

        # connect to the PostgreSQL server
        logging.debug('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        logging.debug('Connection established')

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
            logging.debug('Attempting to close connection')
            conn.close()
            logging.debug('Database connection closed.')

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
    with open(elements_json) as file_reader:
        elements_json = json.load(file_reader)
    elements = elements_json[section_name]

    # Make the SQL query
    sql_query = 'CREATE TABLE ' + table_name + ' (' + os.linesep + \
        'file_name VARCHAR(255) PRIMARY KEY,' + os.linesep + \
        'file_path VARCHAR(255),' + os.linesep
    for element_name in elements:
        if not elements[element_name]['calculation_only']:
            sql_query = sql_query + element_name + ' ' + elements[element_name]['db_datatype'] \
                + ',' + os.linesep
    margin_to_remove = -1 * (len(os.linesep) + 1)
    sql_query = sql_query[:margin_to_remove] + ');'

    conn = None
    try:
        # read the connection parameters
        params = config(filename=db_config_file_name, section='postgresql')

        # connect to the PostgreSQL server
        logging.debug('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        logging.debug('Connection established')

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
            logging.debug('Attempting to close connection')
            conn.close()
            logging.debug('Database connection closed.')

# TODO: Rewrite this function to be more flexible
def create_new_db(db_name):
    """Create a new DB.

    Parameters
    ----------
    db_name : string
        Name of the new DB
    """
    logging.info('Attempting to create a new DB')
    conn = None
    try:
        # connect to the PostgreSQL server
        logging.debug('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(dbname='postgres', user='postgres', host='localhost',
                                password='postgres')
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        logging.debug('Connection established')

        # Create database
        cur.execute('CREATE DATABASE ' + db_name + ';')
        cur.close()
        conn.commit()
        logging.info('Database successfully created')
    except (psycopg2.DatabaseError) as error:
        logging.warning(error)
    finally:
        if conn is not None:
            logging.debug('Attempting to close connection')
            conn.close()
            logging.debug('Database connection closed.')

def import_image_label_data(table_name, csv_full_path, elements_json, db_config_file_name, section_name):
    """Import data into a table in the desired DB.

    Parameters
    ----------
    table_name : string
        Name of the table to add the data to
    csv_full_path : string
        Name of the CSV containing the image labels
    elements_json : string
        Name of the JSON containing the list of elements. Each element name that is not
        calculation_only will be a column in the new table
    db_config_file_name : string
        The file name of the INI file that contains the information on the DB server
    section_name : string
        Name of the section in the elements_json that has the column info for that table
    """
    logging.info('Attempting to import table to DB')

    # Add table
    add_table_to_db(table_name, elements_json, db_config_file_name, section_name)

    # Open the json with the list of elements we're interested in
    with open(elements_json) as file_reader:
        elements_json = json.load(file_reader)
    elements = elements_json[section_name]

    # Make the SQL query
    sql_query = 'COPY ' + table_name + '(file_name, file_path, '
    for element_name in elements:
        if not elements[element_name]['calculation_only']:
            sql_query = sql_query + element_name + ','
    sql_query = sql_query[:-1] + ') FROM \'' + csv_full_path + '\' DELIMITER \',\' CSV HEADER;'

    conn = None
    try:
        # read the connection parameters
        params = config(filename=db_config_file_name, section='postgresql')

        # connect to the PostgreSQL server
        logging.debug('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        logging.debug('Connection established')

        # create table one by one
        cur.execute(sql_query)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
        logging.info('Import successful')
    except (psycopg2.DatabaseError) as error:
        logging.warning(error)
    finally:
        if conn is not None:
            logging.debug('Attempting to close connection')
            conn.close()
            logging.debug('Database connection closed.')

if __name__ == "__main__":
    logging.basicConfig(filename='basic_db_ops.log', level=logging.DEBUG)
