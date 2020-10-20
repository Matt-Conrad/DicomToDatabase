from configHandler import ConfigHandler
import os

class MetaToDbConfigHandler(ConfigHandler):
    """Config handler specifically for CXR project."""
    def __init__(self, configFilePath):
        ConfigHandler.__init__(self, configFilePath)
        self.prepConfigIni()

    def prepConfigIni(self):
        self.setParentFolder()
        self.setColumnsInfoName()

    def getDbInfo(self):
        return self.getSection('postgresql')

    def getParentFolder(self):
        return self.getSetting('misc', "parent_folder")

    def getColumnsInfoName(self):
        return self.getSetting("misc", 'columns_info_name')

    def getColumnsInfoFullPath(self):
        return os.path.join(self.getParentFolder(), self.getColumnsInfoName())
        
    def getTableName(self, table):
        return self.getSetting('tableNames', table)

    def getLogLevel(self):
        return self.getSetting("logging", "level")

    def getUnpackFolderPath(self):
        return os.path.join(self.getParentFolder(), "NLMCXR_subset_dataset")

    def setParentFolder(self):
        self.setSetting("misc", "parent_folder", os.path.dirname(self.getConfigFilePath()))

    def setColumnsInfoName(self):
        self.setSetting("misc", "columns_info_name", "columns_info.json")
