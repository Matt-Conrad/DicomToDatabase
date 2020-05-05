"""Contains script that moves all DCM tag-values from a directory of DCMs into a PostgreSQL DB."""
import logging
import sys
import os
import json
from pathlib import Path
import psycopg2
import nibabel as nib
import numpy as np
# This line is so modules using this package as a submodule can use this.
sys.path.append(os.path.dirname(os.path.abspath(__file__)).replace('\\', '/'))
#
from config import config
import basic_db_ops as bdo

# Types of files we want from the dataset
DESIRED_SUFFIXES = ['injured.nii', 'uninjured.nii']

def nifti_to_db(elements_json, config_file_name, section_name):
    """Move all desired NIFTI metadata from a directory full of NIFTIs into a PostgreSQL DB.

    This function goes through all of the NIFTI files in the directory specified in the 'postgresql'
    section of the config file specified by config_file_name. For each NIFTI, the function reads the
    values of the tags specified in the JSON named in elements_json and stores them all in a
    PostgreSQL DB table where the columns are the tags and each row is a NIFTI.

    Parameters
    ----------
    elements_json : string
        The name of the JSON that contains the list of elements we want to read from the DICOM
    config_file_name : string
        The name of the file that contains DB and DICOM folder info
    section_name : string
        Name of the section in the elements_json that has the column info for that table
    """
    logging.info('Attempting to store NIFTI metadata from DCMs in a folder to Postgres DB')
    
    # Create the database if it isn't already there
    db_name = config(filename=config_file_name, section='postgresql')['database']
    if not bdo.db_exists(config_file_name, db_name):
        bdo.create_new_db(db_name)

    # Create table if it isn't already there
    db_name = config(filename=config_file_name, section='postgresql')['database']
    table_name = config(filename=config_file_name, section='table_info')['metadata_table_name']
    if not bdo.table_exists(config_file_name, db_name, table_name):
        bdo.add_table_to_db(table_name, elements_json, config_file_name, section_name)

    # Open the json with the list of elements we're interested in
    with open(elements_json) as file_reader:
        elements_dict = json.load(file_reader)
    elements_original = elements_dict[section_name]

    folder_path = config(filename=config_file_name, section='nifti_folder')['folder_path']
    pathlist = Path(folder_path).glob('**/*.nii')
    for path in pathlist:
        # read each image in the subdirectories
        file_path = str(path)

        # Only want image files that contain suffixes in the DESIRED_SUFFIXES list
        suffix_exists_in_path = [suffix for suffix in DESIRED_SUFFIXES if(suffix in file_path)] 
        if not bool(suffix_exists_in_path):
            continue

        elements = elements_original.copy()

        logging.info('Storing: ' + file_path)

        # Insert the DICOM metadata as a new record in the Postgres DB
        conn = None
        try:
            # read the connection parameters
            params = config(filename=config_file_name, section='postgresql')
            table_name = config(filename=config_file_name, section='table_info')['metadata_table_name']
            # # connect to the PostgreSQL server
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            # Create the SQL query to be used
            sql_query, values = create_sql_query(table_name, elements, file_path)
            logging.debug('SQL Query: %s', sql_query)
            # create table one by one
            cur.execute(sql_query, values)
            # close communication with the PostgreSQL database server
            cur.close()
            # commit the changes
            conn.commit()
            logging.info('Stored')
        except (psycopg2.DatabaseError) as error:
            logging.warning(error)
            logging.warning('Not stored')
        finally:
            if conn is not None:
                conn.close()

# TODO: Create a more specific name for this function
def create_sql_query(table_name, elements, file_path):
    """Create the SQL query for inserting a record.

    This function reads each element in elements_dict from the NIFTI specified by file_path and
    formats the data into a SQL query to be later executed by psycopg2.

    Parameters
    ----------
    table_name : string
        The name of the table that the SQL query is to be aimed at
    elements : dict
        The dictionary containing all of the info from the elements_json
    file_path : [type]
        The file path of the DCM we're interested in

    Returns
    -------
    (string, list)
        Return the completed SQL query and a list of the values that will be formatted
        into the SQL query by the psycopg2 execute function.
    """
    img = nib.load(file_path)
    # Go through the list of elements and try to read the value
    for element_name in elements.keys():
        try:
            value = img.header[element_name]
            if isinstance(value, np.ndarray):
                value = value.tolist()
            elements[element_name]['value'] = value
        except (KeyError) as tag: # if the value isn't there, then set it as None
            logging.warning('Cannot read the following NIFTI tag: ' + element_name)
            elements[element_name]['value'] = None
            continue

    # Adjust the data as necessary before storing
    # data_adjustments(elements)

    # Create the list of values that we're going to use to build the query
    names = ['file_path']
    values = [file_path]
    placeholders = ['%s']
    # Append any value to this list that isn't birth_date or study_date
    for element_name in elements.keys():
        if not elements[element_name]['calculation_only']:
            names.append(element_name)
            values.append(elements[element_name]['value'])
            placeholders.append('%s')

    # Build the SQL query
    sql_query = 'INSERT INTO ' + table_name + ' (' + ', '.join(names) + ')' + os.linesep \
        + 'VALUES (' + ', '.join(placeholders) + ');'

    return (sql_query, values)

if __name__ == "__main__":
    logging.basicConfig(filename='nifti_to_db.log', level=logging.INFO)
    nifti_to_db('elements.json', 'config.ini', 'nifti_elements')
