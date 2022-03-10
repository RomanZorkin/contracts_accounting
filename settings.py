"""Docstring."""

import configparser
from pathlib import Path

config_file = Path('config.ini')
config = configparser.ConfigParser()
config.read(config_file)

BOOL_NAME = 'boolean'
DATE_NAME = 'date'
FLOAT_NAME = 'numeric'
INT_NAME = 'integer'
STR_NAME = 'text'

DB_CONFIGURATION: str = config['DB_connection']['rule']