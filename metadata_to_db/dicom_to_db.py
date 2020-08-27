"""Contains script that moves all DCM tag-values from a directory of DCMs into a PostgreSQL DB."""
import os
import logging
import requests
import sys
import json
from datetime import datetime
import psycopg2
from pathlib import Path
import pydicom as pdm
# This line is so modules using this package as a submodule can use this.
sys.path.append(os.path.dirname(os.path.abspath(__file__)).replace('\\', '/'))
#

class DicomToDatabase:
    def __init__(self, configHandler, dbHandler):
        self.configHandler = configHandler
        self.dbHandler = dbHandler

    def dicomToDb(self, db_name, metaTableName, columnsInfoPath):
        logging.info('Attempting to store DICOM metadata from DCMs in a folder to Postgres DB')
        
        config_file_name =  self.configHandler.getConfigFilename()
        section_name = "elements"

        if not self.dbHandler.table_exists(metaTableName):
            self.dbHandler.add_table_to_db(metaTableName, columnsInfoPath, section_name)

        with open(columnsInfoPath) as file_reader:
            elements_dict = json.load(file_reader)
        elements_original = elements_dict[section_name]

        folderRelPath = "./" + self.configHandler.getDatasetName()
        pathlist = Path(folderRelPath).glob('**/*.dcm')
        for path in pathlist:
            elements = elements_original.copy()

            file_path = str(path)
            logging.debug('Storing: ' + file_path)

            # Insert the DICOM metadata as a new record in the Postgres DB
            sql_query, values = self.create_sql_query(metaTableName, elements, file_path)
            self.dbHandler.executeQuery(self.dbHandler.storeCursor, sql_query, values)
                    
        logging.info('Done storing metadata')

    def create_sql_query(self, metaTableName, elements, file_path):
        """Create the SQL query for inserting a record."""
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
        self.data_adjustments(elements)

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
        sql_query = 'INSERT INTO ' + metaTableName + ' (' + ', '.join(names) + ')' + os.linesep + 'VALUES (' + ', '.join(placeholders) + ');'

        return (sql_query, values)

    def data_adjustments(self, elements):
        """Make adjustments to the data if they are needed."""
        # Calculate patient_age if it's in the list of elements but wasn't read from the DCM
        if ('patient_age' in elements) and (elements['patient_age']['value'] is None):
            if (elements['patient_birth_date']['value'] is not None) and (elements['study_date']['value'] is not None):
                logging.info('Calculating the patient age (0x0010, 0x1010)')
                patient_birth_date = elements['patient_birth_date']['value']
                study_date = elements['study_date']['value']

                patient_birth_date = datetime.strptime(patient_birth_date, '%Y%m%d')
                study_date = datetime.strptime(study_date, '%Y%m%d')
                elements['patient_age']['value'] = relativedelta(study_date, patient_birth_date).years

        # Combine patient_orientation into a string, otherwise it is treated as a list
        if ('patient_orientation' in elements) and (elements['patient_orientation']['value'] is not None):
            elements['patient_orientation']['value'] = '\\'.join(elements['patient_orientation']['value'])