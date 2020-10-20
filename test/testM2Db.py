import shutil
import os, sys
projectDir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(projectDir, "metadata_to_db"))
from metaToDbConfigHandler import MetaToDbConfigHandler
from databaseHandler import DatabaseHandler
from dicomToDb import DicomToDatabase

parentFolder = os.path.dirname(os.path.abspath(__file__))
miscFolderRelPath = os.path.join("..", "misc")

configFilename = "config.ini"
configFileRelPath = os.path.join(miscFolderRelPath, configFilename)
configFileFullPath = os.path.join(parentFolder, configFilename)

columnsInfoFilename = "columns_info.json"
columnsInfoRelPath = os.path.join(miscFolderRelPath, columnsInfoFilename)
columnsInfoFullPath = os.path.join(parentFolder, columnsInfoFilename)

shutil.copyfile(os.path.join(parentFolder, configFileRelPath), configFileFullPath)
shutil.copyfile(os.path.join(parentFolder, columnsInfoRelPath), columnsInfoFullPath)

configHandler = MetaToDbConfigHandler(configFileFullPath)
dbHandler = DatabaseHandler(configHandler)
m2db = DicomToDatabase(configHandler, dbHandler)

metaTableName = configHandler.getTableName("metadata")

if not dbHandler.tableExists(metaTableName):
    dbHandler.addTableToDb(metaTableName, columnsInfoFullPath, "nonElementColumns", "elements")

m2db.dicomToDb(dbHandler.dbInfo['database'], metaTableName, columnsInfoFullPath)