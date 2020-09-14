"""Module contains class for handling config files of the INI format."""
from configparser import ConfigParser
import logging
import os

class ConfigHandler:
    def __init__(self, configFilename):
        self.configFilename = configFilename

        self.parser = ConfigParser()
        self.readConfigFile()

    def __del__(self):
        self.writeConfigFile()

    # Functions for external use
    def getConfigFilename(self):
        return self.configFilename

    # Functions for internal use
    def readConfigFile(self):
        logging.info("Reading config file: %s", self.getConfigFilename())
        if os.path.exists(self.configFilename):
            self.parser.read(self.configFilename)
            logging.info("Config file read")
        else:
            raise OSError('File {0} not found'.format(self.getConfigFilename()))

    def writeConfigFile(self):
        logging.info("Writing config file: %s", self.getConfigFilename())
        with open(self.configFilename, 'w') as configfile:
            self.parser.write(configfile)

    def getSection(self, sectionName):
        section = {}
        if self.parser.has_section(sectionName):
            params = self.parser.items(sectionName)
            for param in params:
                section[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(sectionName, filename))
        return section
    
    def getSetting(self, sectionName, settingName):
        return self.getSection(sectionName)[settingName]

    def setSetting(self, sectionName, settingName, value):
        self.parser[sectionName][settingName] = value

    
    