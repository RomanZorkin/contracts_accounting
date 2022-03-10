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



class PandasFormat(Cleaner):
    """Cleaner subclus to bring the DataFrame to the set configuration.

    To handle the data, one of two methods is run. "format_corrector()" -
    for Series or DataFrame with one column and
    "big_frame_format_corrector()" for DataFrame with multiple columns.
    The adjustment takes place for each column in turn
    Also we can delete rows from DataFrame, wich dont have any useful
    information. Rule for deleting rows, also set by the configuration

    Attributes:
        frame (pd.DataFrame): frame for adjustments
        column (str): curent column for adjustments
        function_dict (Dict[str, Any]): a dictionary in which the\
            correspondences between the format and the correction method\
            are established
    """

    def __init__(self, table_name: str = '', frame: pd.DataFrame = None):
        """Init PandasFormat class.

        Args:
            table_name (str): the name of the table for which we apply the\
                corresponding rules from configuration file "table.yaml".
            frame (pd.DataFrame): frame for adjustments.

        """
        super().__init__(table_name)
        self.frame: pd.DataFrame = frame
        self.column: str = ''
        self.function_dict: Dict[str, Any] = {
            BOOL_NAME: self._to_bool,
            DATE_NAME: self._to_date,
            INT_NAME: self._to_int,
            FLOAT_NAME: self._to_float,
            STR_NAME: self._to_str,
        }
        logging.debug('start PandasFormat')

    def empty_rows(self) -> pd.DataFrame:
        """Method for deleting empty rows."""
        pass  # noqa: WPS420

    def format_corrector(self) -> pd.DataFrame:
        """A method to start adjusting a series or frame from a single column.

        The first value in the value list should be the column name.
        This is necessary to determine the column format

        Returns:
            pd.DataFrame: correct DataFrame.
        """
        logging.debug('start format_corrector')
        self.column = list(self.frame)[0]
        return self.function_dict[self.format(self.column)]()

    def big_frame_format_corrector(self, big_frame) -> pd.DataFrame:
        """A method to start adjusting a frame of multiple columns.

        The process of verifying the correctness of data in the class
        is based on checking the correctness of a separate column with
        a given format. To do this, the columns are extracted from
        the frame in turn in the method, the data in them are
        checked and corrected. After that, the column with the correct
        data is written back to the original frame.

        Arguments:
            big_frame (pd.DataFrame): DataFrame for adjustment

        Returns:
            pd.DataFrame: correct DataFrame.
        """
        for column in big_frame.columns.values.tolist():
            self.column = str(column)
            self.frame = pd.DataFrame(
                big_frame[self.column],
                columns=[self.column],
            )
            big_frame[self.column] = self.function_dict[
                self.format(self.column)
            ]()
        return big_frame

    def _to_str(self, parametrs=None) -> pd.DataFrame:
        """Correction method for string or text data.

        Correction list with data like text

        Arguments:
            parametrs (None): some parametr

        Returns:
            pd.DataFrame: DataFrame with formatted text.
        """
        logging.debug('start _to_str PandasFormat')
        return self.frame.fillna('')

    def _to_int(self, parametrs=None) -> pd.DataFrame:
        """Correction method for integer data.

        Correction list with data like integer.

        Arguments:
            parametrs (None): some parametr

        Returns:
            pd.DataFrame: DataFrame with formatted integer.
        """
        logging.debug('start _to_int PandasForma')
        float_df = pd.DataFrame()
        float_df[self.column] = pd.to_numeric(
            self.frame[self.column],
            errors='coerce',
        ).fillna(0)
        return float_df.astype('int')

    def _to_float(self, parametrs=None) -> pd.DataFrame:
        """Correction method for float data.

        Correction list with data like float.

        Arguments:
            parametrs (None): some parametr

        Returns:
            pd.DataFrame: DataFrame with formatted float.
        """
        logging.debug('start _to_float PandasFormat')
        float_df = pd.DataFrame()
        float_df[self.column] = pd.to_numeric(
            self.frame[self.column],
            errors='coerce',
        ).fillna(0)
        return float_df.astype('float')

    def _to_date(self, parametrs=0) -> pd.DataFrame:
        """Correction method for date data.

        Correction list with data like date.

        Arguments:
            parametrs (int): Timestamp parametr, default - 0\
                need for position with err or None data

        Returns:
            pd.DataFrame: DataFrame with formatted data.
        """
        logging.debug('start _to_date PandasFormat')
        date_df = pd.DataFrame()
        date_df[self.column] = pd.to_datetime(
            self.frame[self.column],
            errors='coerce',
        ).fillna(pd.Timestamp(parametrs))
        # записывает данные в формате даты parametr = '19990101' если Nat
        return date_df

    def _to_bool(self, parametrs=None) -> pd.DataFrame:
        """Correction method for bool data.

        Correction list with data like bool.

        Arguments:
            parametrs (None): some parametr

        Returns:
            pd.DataFrame: DataFrame with formatted bool.
        """
        logging.debug('start _to_bool PandasFormat')
        return self.frame.astype('boolean').fillna(False)
