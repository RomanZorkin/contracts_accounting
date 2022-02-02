"""Docstring."""

import configparser
from pathlib import Path

config_file = Path('config.ini')
config = configparser.ConfigParser()
config.read(config_file)

DB_CONFIGURATION: str = config['DB_connection']['rule']