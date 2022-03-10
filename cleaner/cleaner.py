"""Corrects the original data before writing them to a database or frame."""

import datetime
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import yaml

import sqlorm.word_module as wm

config_file = Path('configuration/table.yaml')
with open(config_file) as fh:
    table_config = yaml.load(fh, Loader=yaml.FullLoader)  # noqa: S506

BOOL_NAME = 'boolean'
DATE_NAME = 'date'
FLOAT_NAME = 'numeric'
INT_NAME = 'integer'
STR_NAME = 'text'


class Cleaner(object):
    """Base class for bringing objects to the established formats.

    Atributes:
        table_name (str): the name of the table for which we apply the\
            corresponding rules from configuration file "table.yaml".
        table_scheme (Dict): a dictionary with all the rules, parameters and\
            settings for the table. The following parameters are passed:\
            the name of the table, the name of the file to read,\
            the sheet to read, the column settings (names, data format,\
            which are used for sql, the range of cells in excel, etc.).
        columns (Dict): dictionary with column parameters\
            key - column name, value - dictionary with parameters.
        id_sql (str): name of PRIMARY KEY sql column.
    """

    def __init__(self, table_name: str):
        """Init Cleaner class.

        Args:
            table_name (str): the name of the table for which we apply the
                corresponding rules from configuration file "table.yaml".
        """
        self.table_name: str = table_name
        self.table_scheme: Dict[str, Any] = table_config[self.table_name]
        self.columns: Dict[str, Dict[str, Any]] = self.table_scheme['columns']
        self.id_sql: str = self._sql_id()

    def format(self, column: str) -> str:
        """Generate the data format for the corresponding column.

        The data format for each column is recorded in the
        configuration file.

        Arguments:
            column (str): column name for wich we find format

        Returns:
            str:  data format for the corresponding column.
        """
        column_format = self.columns[column]['format'].split(' ')
        return column_format[0]

    def _sql_id(self) -> str:
        """Form name of PRIMARY KEY sql column.

        Returns:
            str: PRIMARY KEY sql column name.
        """
        for column in list(self.columns):
            # it may seem that the object for checking the condition
            # is similar to the result from the sel.format() method,
            # but the result of the method is string and not a list
            if len(self.columns[column]['format'].split(' ')) > 1:
                return column

