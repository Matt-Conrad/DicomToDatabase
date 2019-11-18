"""
This module contains a function that will read parameters from a config file (INI format)
"""
from configparser import ConfigParser

def config(filename, section):
    """ This function reads the parameters from the specified section of the desired file """
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db
