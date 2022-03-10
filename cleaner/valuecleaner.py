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


class ValueCleaner(Cleaner):
    """Cleaner subclus to bring the list to the set configuration.

    To handle the data run "columns_cleaner()" method.
    The adjustment takes place for each column in turn

    Attributes:
        list_to_clean (List[Any]): list for correct
        column (str): column name for adjustment
        function_dict (Dict[str, Any]): a dictionary in which the\
            correspondences between the format and the correction method\
            are established
    """

    def __init__(
            self,
            list_to_clean: List[Any],
            table_name: str,
            column: str,
    ):
        """Init ValueCleaner class.

        Args:
            list_to_clean (List[Any]): list for correct
            table_name (str): the name of the table for which we apply the\
                corresponding rules from configuration file "table.yaml".
            column (str): column name for adjustment.
        """
        super().__init__(table_name)
        self.list_to_clean: List[Any] = list_to_clean
        self.column: str = column
        self.function_dict: Dict[str, Any] = {
            BOOL_NAME: self._boolean_handler,
            DATE_NAME: self._date_handler,
            INT_NAME: self._integer_handler,
            FLOAT_NAME: self._numeric_handler,
            STR_NAME: self._text_handler,
        }
        self.true_list = [
            'да',
        ]

    def columns_cleaner(self) -> List[Any]:
        """A method to start adjusting a list.

        Returns:
            List[Any]: clean list after correction
        """
        self.function_dict[self.format(self.column)]()
        return self.list_to_clean

    def _boolean_handler(self) -> None:
        """Correction method for bool data."""
        logging.debug('start _to_bool ValueCleaner')
        new_list = []
        for position in self.list_to_clean:
            if position in self.true_list:
                new_list.append(True)
            else:
                new_list.append(True)
        self.list_to_clean = new_list

    def _date_handler(self) -> None:
        """Correction method for date data."""
        logging.debug('start _to_date ValueCleaner')

    def _integer_handler(self) -> None:
        """Correction method for integer data."""
        logging.debug('start _to_int ValueCleaner')
        new_list = []
        for position in self.list_to_clean:
            try:
                pad = int(position)
            except TypeError:
                pad = 0
            new_list.append(int(pad))
        self.list_to_clean = new_list

    def _numeric_handler(self) -> None:
        """Correction method for float data."""
        logging.debug('start _to_float ValueCleaner')
        new_list = []
        for position in self.list_to_clean:
            try:
                pad = float(position)
            except (TypeError, ValueError):
                pad = float(0)
            new_list.append(float(pad))
        self.list_to_clean = new_list

    def _text_handler(self) -> None:
        """Correction method for string or text data."""
        logging.debug('start _to_str ValueCleaner')
        new_list = []
        for position in self.list_to_clean:
            try:
                pad = str(position)
            except TypeError:
                pad = ''
            new_list.append(str(pad))
        self.list_to_clean = new_list
