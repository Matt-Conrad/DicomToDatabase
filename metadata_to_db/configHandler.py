"""Module contains class for handling config files of the INI format."""
from configparser import ConfigParser
import logging
import atexit
import os

class ConfigHandler:
    def __init__(self, configFilePath):
        self.configFilePath = configFilePath

        self.parser = ConfigParser()
        self.readConfigFile()
        atexit.register(self.writeConfigFile)   

    # Functions for external use
    def getConfigFilename(self):
        return os.path.basename(self.configFilePath)

    def getConfigFilePath(self):
        return self.configFilePath

    # Functions for internal use
    def readConfigFile(self):
        logging.info("Reading config file: %s", self.getConfigFilePath())
        if os.path.exists(self.getConfigFilePath()):
            self.parser.read(self.getConfigFilePath())
            logging.info("Config file read")
        else:
            raise OSError('File {0} not found'.format(self.getConfigFilePath()))

    def writeConfigFile(self):
        logging.info("Writing config file: %s", self.getConfigFilePath())
        with open(self.getConfigFilePath(), 'w') as configfile:
            self.parser.write(configfile)

    def getSection(self, sectionName):
        section = {}
        if self.parser.has_section(sectionName):
            params = self.parser.items(sectionName)
            for param in params:
                section[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(sectionName, self.getConfigFilename()))
        return section
    
    def getSetting(self, sectionName, settingName):
        return self.getSection(sectionName)[settingName]

    def setSetting(self, sectionName, settingName, value):
        self.parser[sectionName][settingName] = value
        self.writeConfigFile()

