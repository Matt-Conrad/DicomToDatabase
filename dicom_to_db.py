"""
This script will store specific tag/value pairs for all of the DICOM files in a folder (including
 all sub-folders) into a PostgreSQL DB table
"""

import os
import json
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta
import psycopg2
import pydicom as pdm
from config import config
import basic_db_ops as db

def dicom_to_db(elements_json, db_config_file_name, config_section, dcm_section):
    """
    This function will take a foldername and DB details and move the metadata in that folder
    to the DB
    """
    # Open the json with the list of elements we're interested in
    with open('elements.json') as file_reader:
        elements_json = json.load(file_reader)

    folder_path = config(filename=db_config_file_name, section=dcm_section)['folder_path']
    pathlist = Path(folder_path).glob('**/*.dcm')
    for path in pathlist:
        # read each image in the subdirectories
        file_path = str(path)
        dcm = pdm.dcmread(file_path)

        print('Starting to read ' + file_path)

        sql_query, values = create_sql_query(elements_json, dcm, file_path)

        # Insert the DICOM metadata as a new record in the Postgres DB
        conn = None
        try:
            # read the connection parameters
            params = config(filename=db_config_file_name, section=config_section)
            # connect to the PostgreSQL server
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            # create table one by one
            cur.execute(sql_query, values)
            # close communication with the PostgreSQL database server
            cur.close()
            # commit the changes
            conn.commit()
        except (psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

        print('Done reading ' + file_path)

def create_sql_query(elements_json, dcm, file_path):
    """
    Attempts to read the elements specified in the elements_json from the dcm file
    """
    # Go through the list of elements and try to read the value
    elements = elements_json['elements']
    for element_name in elements.keys():
        tag = elements[element_name]['tag']
        [group_num, element_num] = tag.split(',')
        try:
            value = dcm[int(group_num, 16), int(element_num, 16)].value
            elements[element_name]['value'] = value
        except (KeyError) as error: # if the value isn't there, then skip it
            print(error)
            continue

    # Calculate patient_age if it isn't there already
    calculate_age(elements)

    # Combine patient_orientation, otherwise it is treated as a list
    if 'patient_orientation' in elements:
        elements['patient_orientation']['value'] = ''.join(elements['patient_orientation']['value'])

    # Create the list of values that we're going to insert
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
    sql_query = 'INSERT INTO image_metadata (' + ', '.join(names) + ')' + os.linesep \
        + 'VALUES (' + ', '.join(placeholders) + ');'

    return (sql_query, values)

def calculate_age(elements):
    """
    Calculates the patient_age if it isn't already filled out
    """
    if ('patient_age' in elements) and ('value' not in elements['patient_age']):
        if ('value' in elements['patient_birth_date']) and ('value' in elements['study_date']):
            patient_birth_date = elements['patient_birth_date']['value']
            study_date = elements['study_date']['value']

            patient_birth_date = datetime.strptime(patient_birth_date, '%Y%m%d')
            study_date = datetime.strptime(study_date, '%Y%m%d')
            elements['patient_age']['value'] = relativedelta(
                study_date, patient_birth_date).years
        else:
            print('Cannot calculate patient age, leaving blank')
            elements['patient_age']['value'] = None

if __name__ == '__main__':
    db.drop_table('image_metadata', 'config.ini', 'postgresql')
    db.add_table_to_db('image_metadata', 'elements.json', 'config.ini', 'postgresql')
    dicom_to_db('elements.json', 'config.ini', 'postgresql', 'dicom_folder')

    # db.check_db_connection('config.ini', 'postgresql')
