"""
This module contains functions that are basic and common DB operations
"""
import os
import json
import psycopg2
from config import config

def check_db_connection(db_config_file_name, config_section):
    """
    Checks that a connection to the PostgreSQL database server is possible by retrieving
    the version from the DB
    """
    conn = None
    try:
        # read connection parameters
        params = config(filename=db_config_file_name, section=config_section)

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

def drop_table(table_name, db_config_file_name, config_section):
    """ This function drops a table you specify """
    conn = None
    try:
        # read the connection parameters
        params = config(filename=db_config_file_name, section=config_section)
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

def add_table_to_db(db_name, elements_json, db_config_file_name, config_section):
    """
    This function will add a table to the DB
    """
    # Open the json with the list of elements we're interested in
    with open(elements_json) as file_reader:
        elements_json = json.load(file_reader)
    elements = elements_json['elements']

    # Make the SQL query
    sql_query = 'CREATE TABLE ' + db_name + ' (' + os.linesep + \
        'file_path VARCHAR(255) PRIMARY KEY,' + os.linesep
    for element_name in elements:
        if not elements[element_name]['calculation_only']:
            sql_query = sql_query + element_name + ' ' + elements[element_name]['db_datatype'] \
                + ',' + os.linesep
    sql_query = sql_query[:-2] + ');'

    conn = None
    try:
        # read the connection parameters
        params = config(filename=db_config_file_name, section=config_section)
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
