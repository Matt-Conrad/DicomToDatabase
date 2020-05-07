"""Contains script that moves all DCM tag-values from a directory of DCMs into a PostgreSQL DB."""
import logging
import os
import sys
import json
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta
import psycopg2
import pydicom as pdm
# This line is so modules using this package as a submodule can use this.
sys.path.append(os.path.dirname(os.path.abspath(__file__)).replace('\\', '/'))
#
from config import config
import basic_db_ops as bdo

def dicom_to_db(elements_json, config_file_name, section_name):
    """Move all desired DCM tag-values from a directory full of DCMs into a PostgreSQL DB.

    This function goes through all of the DCM files in the directory specified in the 'postgresql'
    section of the config file specified by config_file_name. For each DCM, the function reads the
    values of the tags specified in the JSON named in elements_json and stores them all in a
    PostgreSQL DB table where the columns are the tags and each row is a DCM.

    Parameters
    ----------
    elements_json : string
        The name of the JSON that contains the list of elements we want to read from the DICOM
    config_file_name : string
        The name of the file that contains DB and DICOM folder info
    section_name : string
        Name of the section in the elements_json that has the column info for that table
    """
    logging.info('Attempting to store DICOM metadata from DCMs in a folder to Postgres DB')

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

    # Read images one at a time
    folder_path = config(filename=config_file_name, section='dicom_folder')['folder_path']
    pathlist = Path(folder_path).glob('**/*.dcm')
    for path in pathlist:
        elements = elements_original.copy()

        file_path = str(path)
        logging.debug('Storing: ' + file_path)

        # Insert the DICOM metadata as a new record in the Postgres DB
        conn = None
        try:
            # read the connection parameters
            params = config(filename=config_file_name, section='postgresql')
            # connect to the PostgreSQL server
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
            logging.debug('Stored')
        except (psycopg2.DatabaseError) as error:
            logging.debug(error)
        finally:
            if conn is not None:
                conn.close()
    logging.info('Done storing metadata')

# TODO: Create a more specific name for this function
def create_sql_query(table_name, elements, file_path):
    """Create the SQL query for inserting a record.

    This function reads each element in elements_dict from the DCM specified by file_path and
    formats the data into a SQL query to be later executed by psycopg2.

    Parameters
    ----------
    table_name : string
        The name of the table that the SQL query is to be aimed at
    elements : dict
        The dictionary containing all of the info from the elements_json
    file_path : string
        The file path of the DCM we're interested in

    Returns
    -------
    (string, list)
        Return the completed SQL query and a list of the values that will be formatted
        into the SQL query by the psycopg2 execute function.
    """
    dcm = pdm.dcmread(file_path)
    # Go through the list of elements and try to read the value
    for element_name in elements.keys():
        tag = elements[element_name]['tag']
        [group_num, element_num] = tag.split(',')
        try:
            element = dcm[int(group_num, 16), int(element_num, 16)]
            if element.VR == 'DS':
                if element.VM == 1:
                    value = int(float(element.value))
                elif element.VM > 1:
                    value = int(float(element.value[0]))
            else:
                value = element.value
            elements[element_name]['value'] = value
        except (KeyError) as tag: # if the value isn't there, then set it as None
            logging.debug('Cannot read the following DICOM tag: ' + str(tag))
            elements[element_name]['value'] = None
            continue

    # Adjust the data as necessary before storing
    data_adjustments(elements)

    # Create the list of values that we're going to use to build the query
    names = ['file_name', 'file_path']
    values = [file_path.split(os.sep)[-1], file_path]
    placeholders = ['%s', '%s']
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

def data_adjustments(elements):
    """Make adjustments to the data if they are needed.

    In some situations, the data in elements may need to be adjusted before we put them into a
    query.

    Parameters
    ----------
    elements : dict
        The dict of elements containing data read from the dcm and other associated info
    """
    # Calculate patient_age if it's in the list of elements but wasn't read from the DCM
    if ('patient_age' in elements) and (elements['patient_age']['value'] is None):
        if (elements['patient_birth_date']['value'] is not None) \
            and (elements['study_date']['value'] is not None):

            logging.info('Calculating the patient age (0x0010, 0x1010)')
            patient_birth_date = elements['patient_birth_date']['value']
            study_date = elements['study_date']['value']

            patient_birth_date = datetime.strptime(patient_birth_date, '%Y%m%d')
            study_date = datetime.strptime(study_date, '%Y%m%d')
            elements['patient_age']['value'] = relativedelta(study_date, patient_birth_date).years

    # Combine patient_orientation into a string, otherwise it is treated as a list
    if ('patient_orientation' in elements) and \
        (elements['patient_orientation']['value'] is not None):
        elements['patient_orientation']['value'] = \
            '\\'.join(elements['patient_orientation']['value'])

if __name__ == "__main__":
    logging.basicConfig(filename='dicom_to_db.log', level=logging.INFO)
    dicom_to_db('elements.json', 'config.ini', 'dicom_elements')