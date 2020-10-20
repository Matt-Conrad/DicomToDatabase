"""Contains script that moves all DCM tag-values from a directory of DCMs into a PostgreSQL DB."""
import os
import logging
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
import pydicom as pdm

class DicomToDatabase:
    def __init__(self, configHandler, dbHandler):
        self.configHandler = configHandler
        self.dbHandler = dbHandler

    def dicomToDb(self, dbName, metaTableName, columnsInfoPath):
        logging.info('Attempting to store DICOM metadata from DCMs in a folder to Postgres DB')
        
        with open(columnsInfoPath) as fileReader:
            elementsDict = json.load(fileReader)
        elementsOriginal = elementsDict["elements"]

        pathlist = Path(self.configHandler.getUnpackFolderPath()).glob('**/*.dcm')
        for path in pathlist:
            elements = elementsOriginal.copy()

            filePath = str(path)
            logging.debug('Storing: ' + filePath)

            # Insert the DICOM metadata as a new record in the Postgres DB
            sqlQuery, values = self.createSqlQuery(metaTableName, elements, filePath)
            self.dbHandler.executeQuery(self.dbHandler.connection, sqlQuery, values)
                    
        logging.info('Done storing metadata')

    def createSqlQuery(self, metaTableName, elements, filePath):
        """Create the SQL query for inserting a record."""
        dcm = pdm.dcmread(filePath)
        # Go through the list of elements and try to read the value
        for elementName in elements.keys():
            tag = elements[elementName]['tag']
            [groupNum, elementNum] = tag.split(',')
            try:
                element = dcm[int(groupNum, 16), int(elementNum, 16)]
                if element.VR == 'DS':
                    if element.VM == 1:
                        value = int(float(element.value))
                    elif element.VM > 1:
                        value = int(float(element.value[0]))
                else:
                    value = element.value
                elements[elementName]['value'] = value
            except (KeyError) as tag: # if the value isn't there, then set it as None
                logging.debug('Cannot read the following DICOM tag: ' + str(tag))
                elements[elementName]['value'] = None
                continue

        # Adjust the data as necessary before storing
        self.dataAdjustments(elements)

        # Create the list of values that we're going to use to build the query
        names = ['file_name', 'file_path']
        fileRelPath = filePath.replace(self.configHandler.getParentFolder() + os.path.sep, "")
        values = [filePath.split(os.sep)[-1], fileRelPath]
        placeholders = ['%s', '%s']
        # Append any value to this list that isn't birth_date or study_date
        for elementName in elements.keys():
            if not elements[elementName]['calculation_only']:
                names.append(elementName)
                values.append(elements[elementName]['value'])
                placeholders.append('%s')

        # Build the SQL query
        sqlQuery = 'INSERT INTO ' + metaTableName + ' (' + ', '.join(names) + ') VALUES (' + ', '.join(placeholders) + ');'

        return (sqlQuery, values)

    def dataAdjustments(self, elements):
        """Make adjustments to the data if they are needed."""
        # Calculate patient_age if it's in the list of elements but wasn't read from the DCM
        if ('patient_age' in elements) and (elements['patient_age']['value'] is None):
            if (elements['patient_birth_date']['value'] is not None) and (elements['study_date']['value'] is not None):
                logging.info('Calculating the patient age (0x0010, 0x1010)')
                patientBirthDate = elements['patient_birth_date']['value']
                studyDate = elements['study_date']['value']

                patientBirthDate = datetime.strptime(patientBirthDate, '%Y%m%d')
                studyDate = datetime.strptime(studyDate, '%Y%m%d')
                elements['patient_age']['value'] = relativedelta(studyDate, patientBirthDate).years

        # Combine patient_orientation into a string, otherwise it is treated as a list
        if ('patient_orientation' in elements) and (elements['patient_orientation']['value'] is not None):
            elements['patient_orientation']['value'] = '\\'.join(elements['patient_orientation']['value'])
