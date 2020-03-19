"""Contains a function that will read parameters from a config file (INI format)."""
import logging
import os
from configparser import ConfigParser

def config(filename, section):
    """Read the parameters from the specified section of the desired config file.

    Parameters
    ----------
    filename : string
        Name of the config file
    section : string
        Name of the section in the config file

    Returns
    -------
    dict
        Dictionary containing the params and values from the config file

    Raises
    ------
    Exception
        Raised when section is not found in the config file
    """
    # create a parser
    parser = ConfigParser()
    # read config file
    if os.path.exists(filename):
        parser.read(filename)
    else:
        raise OSError('File {0} not found'.format(filename))

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    logging.debug('Successfully read section: %s in config file: %s,  here is the result:', section, filename)
    logging.debug(db)

    return db

def update_config_file(filename, section, key, value):
    """Update a key-value pair in the specified config file.

    Parameters
    ----------
    filename : string
        Name of the config file
    section : string
        Name of the section in the config file
    key : string
        Name of the key in the section in the config file
    value : string
        Value associated with the key

    Raises
    ------
    Exception
        Raised when section is not found in the config file
    """
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    if parser.has_section(section):
        parser[section][key] = value
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    with open(filename, 'w') as configfile:
        parser.write(configfile)
    logging.debug('Updated key %s in section %s of config file %s', key, section, filename)

def get_config_setting(filename, section, key):
    """Get the database name in the specified config file.

    Parameters
    ----------
    filename : string
        Name of the config file
    section : string
        Name of the section in the config file
    key : string
        Name of the key in the section in the config file

    Returns
    -------
    value
        The value of the key in the desired section

    Raises
    ------
    Exception
        Raised when section is not found in the config file
    """
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    if parser.has_section(section):
        return parser[section][key]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

if __name__ == "__main__":
    logging.basicConfig(filename='config.log', level=logging.INFO)
