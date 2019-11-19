"""Module contains functions that are basic and common DB operations."""
import os
import json
import psycopg2
from config import config

def check_db_connection(db_config_file_name):
    """Check the connection to a PostgreSQL DB server.

    Parameters
    ----------
    db_config_file_name : string
        The file name of the INI file that contains the information on the DB server
    """
    conn = None
    try:
        # read connection parameters
        params = config(filename=db_config_file_name, section='postgresql')

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)

       # close the cursor with the PostgreSQL
        cur.close()

    # If an exception is raised along the way, report it
    except (psycopg2.DatabaseError) as error:
        print(error)

    # At the end, if the connection still exists then close it
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def drop_table(table_name, db_config_file_name):
    """Drop a table in the desired DB.

    Parameters
    ----------
    table_name : string
        The name of the table in the DB specified in db_config_file_name
    db_config_file_name : string
        The file name of the INI file that contains the information on the DB server
    """
    conn = None
    try:
        # read the connection parameters
        params = config(filename=db_config_file_name, section='postgresql')
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        cur.execute('DROP TABLE ' + table_name + ';')
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def add_table_to_db(table_name, elements_json, db_config_file_name):
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
    """
    # Open the json with the list of elements we're interested in
    with open(elements_json) as file_reader:
        elements_json = json.load(file_reader)
    elements = elements_json['elements']

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
        params = config(filename=db_config_file_name, section='postgresql')
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        cur.execute(sql_query)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
