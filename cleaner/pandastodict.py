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


class PandasToDict(PandasCleaner):
    """PandasCleaner subclas to clean dictionary.

    This subclass works with dicts. A dictionary is a representation of a
    single row in a table or frame. Each dictionary key is the column
    name, and the value is the value for this column in the current row.
    The class provides methods for processing each column in accordance
    with the format specified by the configuration file.The start of
    processing is started by the method 'clean_rows()'.
    The class can be used in two cases:
    - when data from the frame is transferred for writing to the database,
    they must be represented as a dictionary. At the same time, the
    dictionary is pre-checked for correctness.
    - the second case is when the information from the source is extracted
    in the form of a dictionary and it needs to be transferred to a dataframe.

    Arguments:
        function_dict (Dict[str, Any]): a dictionary in which the
            correspondences between the format and the correction method
            are established
    """

    def __init__(
            self,
            table_name: str = None,
            dict_for_clean: Dict[str, Any] = None,
    ):
        """Init PandasToDict class.

        Args:
            table_name (str): the name of the table for which we apply the\
                corresponding rules from configuration file "table.yaml".
            dict_for_clean (Dict[str, Any]): dict for correct. Dict format\
                {'table_name':'deals', 'reg_number':1, 'KBK':"'22'", ... }.

        """
        super().__init__(table_name, dict_for_clean)
        self.function_dict: Dict[str, Any] = {
            BOOL_NAME: self._boolean_handler,
            DATE_NAME: self._date_handler,
            INT_NAME: self._integer_handler,
            FLOAT_NAME: self._numeric_handler,
            STR_NAME: self._text_handler,
        }

    def clean_rows(self):
        """A method to start clean the dict.

        The method alternately reads the dictionary values and launches
        the appropriate methods to verify the correctness of the available
        data. The scheme of methods is spelled out in self.function_dict.
        Returns a clean dictionary.

        Returns:
            Dict[str, Any]:
        """
        # sequentially iterate through the dictionary keys (columns names)
        for column in list(self.dict_for_clean)[1:]:
            # for each column, we extract the data format
            # from the configuratuon file
            # skip first row with table name
            if self.format == self.table_name:
                continue
            # we call the corresponding function for each format,
            # to correct values
            logging.debug(f'clean_rows in PandasToDict {column}')
            self.function_dict[self.format(column)](column)
        return self.dict_for_clean

    def _boolean_handler(self, column_name: str):
        """Correction method for bool data.

        Correction dict row with data like bool. The method returns
        nothing. It corrects the specified dictionary string directly
        in the self.dict passed to the class.

        Arguments:
            column_name (str): this is the dictionary key\
                and the column name
        """
        if isinstance(self.dict_for_clean[column_name], bool):
            pass  # noqa: WPS420
        elif self.dict_for_clean[column_name] == 'да':
            self.dict_for_clean[column_name] = True
        else:
            self.dict_for_clean[column_name] = False

    def _date_handler(self, column_name: str):
        """Correction method for date data.

        Correction dict row with data like date. The method returns
        nothing. It corrects the specified dictionary string directly
        in the self.dict passed to the class.

        Arguments:
            column_name (str): this is the dictionary key\
                and the column name
        """
        input_value = self.dict_for_clean[column_name]
        # input value have a pandas format, thats why
        # we use pandas method to get good string
        if pd.isnull(input_value):
            self.dict_for_clean[column_name] = "'1999-01-01'"
        elif isinstance(input_value, str):
            input_value = datetime.datetime.strptime(input_value, '%d.%m.%Y')
            input_value = input_value.strftime('%Y-%m-%d')
            self.dict_for_clean[column_name] = f"'{input_value}'"
        else:
            # we convert pandas datestamp format to python
            # datetime library format
            input_value = input_value.to_pydatetime().date()
            # convert date format to string
            input_value = input_value.strftime('%Y-%m-%d')
            self.dict_for_clean[column_name] = f"'{input_value}'"

    def _integer_handler(self, column_name: str):
        """Correction method for integer data.

        Correction dict row with data like integer. The method returns
        nothing. It corrects the specified dictionary string directly
        in the self.dict passed to the class.

        Arguments:
            column_name (str): this is the dictionary key\
                and the column name
        """
        input_value = self.dict_for_clean[column_name]
        if isinstance(input_value, (int, float)):
            self.dict_for_clean[column_name] = int(input_value)
        else:
            self._error_incident('_integer_handler')

    def _numeric_handler(self, column_name: str):
        """Correction method for float data.

        Correction dict row with data like float. The method returns
        nothing. It corrects the specified dictionary string directly
        in the self.dict passed to the class.

        Arguments:
            column_name (str): this is the dictionary key\
                and the column name
        """
        input_value = self.dict_for_clean[column_name]
        if isinstance(input_value, (int, float)):
            if pd.isnull(input_value):
                self.dict_for_clean[column_name] = float(0)
            else:
                self.dict_for_clean[column_name] = float(input_value)
        else:
            self.dict_for_clean[column_name] = float(0)
            self._error_incident('_numeric_handler')

    def _text_handler(self, column_name: str):
        """Correction method for string data.

        Correction dict row with data like string. The method returns
        nothing. It corrects the specified dictionary string directly
        in the self.dict passed to the class.

        Arguments:
            column_name (str): this is the dictionary key\
                and the column name
        """
        self.dict_for_clean[column_name] = "'{0}'".format(
            str(self.dict_for_clean[column_name]),
        )

    def _error_incident(self, text: str):
        logging.critical(f'PandasToDict_err {text}')
