"""Transfers information from a given file to a database."""

import logging
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List

import openpyxl
import pandas as pd
import yaml

from sqlorm import cleaner, sql_admin

start_time = time.time()
config_file = Path('configuration/table.yaml')

with open(config_file) as fh:
    table_config = yaml.safe_load(fh)


class Reader(object):
    """Transfers information from a given file to a database.

    The name of the table (rules) is passed to the class object.
    The rules for each table are written in the configuration file
    "table.yaml". In accordance with the established rules, the necessary
    file is read. The received data is transmitted to the Pandas Frame,
    after which it is converted to the specified format. The data format
    for each column is also specified in the configuration file. Clean and
    verified data is recorded in the database.

    Attributes:
        table_name (str): the name of the table for which we apply the\
            corresponding rules from configuration file "table.yaml".
        table_scheme (Dict): a dictionary with all the rules, parameters and\
            settings for the table. The following parameters are passed:\
            the name of the table, the name of the file to read,\
            the sheet to read, the column settings (names, data format,\
            which are used for sql, the range of cells in excel, etc.).
        columns_scheme (Dict): dictionary with column parameters\
            key - column name, value - dictionary with parameters\
            (data format, whether it is part of SQL tables (true or false),\
            whether it is part of excel tables and a range of cells).
        columns (List): list of columns names
        id_sql (str): name of PRIMARY KEY sql column.
        sql_scheme: sql_admin.Scheme object. Required for\
            interaction with postgresql.
        budget_year (int): finance year.
    """

    def __init__(self, table: str):
        """Init Reader class.

        Args:
            table (str): the name of the table for which we apply the
                corresponding rules from configuration file "table.yaml".
        """
        self.table_name: str = table
        self.table_scheme: Dict[str, Any] = table_config[table]
        self.columns_scheme: Dict[
            str,
            Dict[str, Any],
        ] = self.table_scheme['columns']
        self.columns: List[str] = list(self.columns_scheme)
        self.id_sql: str = self._sql_id()
        self.sql_scheme = sql_admin.Scheme(self.table_name)
        self.budget_year: int = None

    def _sql_id(self) -> str:
        """Form name of PRIMARY KEY sql column.

        Returns:
            str: PRIMARY KEY sql column name.
        """
        for column in self.columns:
            formating = self.columns_scheme[column]['format'].split(' ')
            if len(formating) > 1:
                return column


class FrameHandler(Reader):
    """Reader subclas for work with pandas DataFrame.

    The class processes the DataFRAME, controls the correctness of
    the data contained in them, and corrects them if necessary
    """

    def frame_corrector(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Format dataframe data.

        For each table in the configuration file, the rules for forming
        columns are set depending on the data type (int, float, date ...).
        Each column is checked for compliance with the established rules.
        If necessary, the data is adjusted.

        Args:
            frame (pd.DataFrame): DataFrame for control.

        Returns:
            pd.DataFrame: DataFrame with formatted data.
        """
        return cleaner.StructureCleaner(
            table_name=self.table_name,
            inframe=frame,
        ).structure_corrector()

    def frame_to_dict(self, frame: pd.DataFrame) -> Dict[int, Dict[str, Any]]:
        """Convert pandas DataFrame to dictionary.

        The method converts the pandaы DataFrame into a dictionary.
        The DataFrame is checked for the correctness of the data,
        if necessary, the data is formatted. The DataFrame is converted
        to a dictionary with values line by line - one entry in the
        dictionary is the values of all columns of the DataFrame on one
        line. The key in the dictionary is int from 0 to the last line in
        DataFrame. The table name is added to the dictionary values.
        The dictionary is checked line by line for compliance with the data
        available in it, the formats set for this table. The checked strings
        are overwritten into the primary dictionary.

        Args:
            frame (pd.DataFrame): DataFrame with file data.

        Returns:
            Dict[int, Dict[str, Any]]: dict format
                {0:{'table_name':'deals', 'reg_number':1, 'KBK':"'22'", ... },
                1:{'table_name':'deals', 'reg_number': 1, 'KBK':"'22'", ... }}
        """
        # the "index" value forms the dictionary line by line,
        # if we take "dict", then the key will be the columns
        frame_dict = self.frame_corrector(
            frame,
        ).to_dict('index')
        for key, record in frame_dict.items():
            clean_dict = cleaner.PandasToDict(
                self.table_name,
                {
                    **{'table_name': self.table_name},
                    **record,
                },
            ).clean_rows()
            frame_dict[key] = clean_dict
        return frame_dict


class ExcelReader(FrameHandler):
    """FrameHandler subclass for reading excel files.

    Read information from excel files and writes it
    to the postgresql database. The read data is transferred to the Dataframe.
    It is more convenient to work with data in this form. The Dataframe is
    checked for compliance with the established formats and correctness of
    the received data. After checking and converting the data, they are
    transferred to the database for writing.

    Attributes:
        excel_file (Path): path to excel file. The path is determined by\
            the configuration file "table.yaml". The internal\
            directory is used - "external_data_source".
        work_sheet (str): name excel workshit, where the\
            information is located.
        excel_rows (List[str]): the range of rows where\
            the information is placed.
        excel_columns (List[str]): excel columns names.
        excel_cells (Dict[str, List[str]]): dictionary containing: key -\
            the name of the excel column; value - the range of the\
            column to read in the form of a list ['a1', 'a10'].
        wb_obj (openpyxl): openpyxl object - excel workbook (excel file).
        ws (openpyxl): openpyxl object - excel worksheet in workbook.
    """

    def __init__(self, table: str):
        """Init Reader class.

        Args:
            table (str): the name of the table for which we apply the
                corresponding rules from configuration file "table.yaml".
        """
        super().__init__(table)
        self.excel_file: Path = Path(self.table_scheme['excel_file'])
        self._work_sheet: str = self.table_scheme['work_sheet']
        self.excel_rows: List[str] = self.table_scheme['excel_rows']
        self.excel_columns: List[str] = self._excel_columns()
        self.excel_cells: Dict[str, List[str]] = self._excel_cells()
        self.wb_obj = openpyxl.load_workbook(self.excel_file)
        self.ws = self.wb_obj[self._work_sheet]

    def read(self, column: str) -> List[Any]:
        """Generate a list of values by column.

        The method reads the values of the column cells and passes them
        to the list. The list is processed using the Cleaner module to
        correct the values and bring them to the specified formats.

        Args:
            column (str): name of the column with extracted values.

        Returns:
            List[Any]: list of column values.
        """
        clean_list = cleaner.ValueCleaner(
            [
                cell[0].value for cell in self.ws[
                    self.excel_cells[column][0]: self.excel_cells[column][1]
                ]
            ],
            self.table_name,
            column,
        )
        return clean_list.columns_cleaner()

    def list_to_frame(self) -> pd.DataFrame:
        """Convert data received from a file into a Dataframe.

        The method receives data from each column in turn in the form
        of a list. The lists are written to the Dataframe.

        Returns:
            pd.DataFrame: DataFrame with formatted data.
        """
        frame = pd.DataFrame(columns=self.excel_columns)
        for column in self.excel_columns:
            clean_frame = cleaner.PandasFormat(
                self.table_name,
                pd.DataFrame(self.read(column), columns=[column]),
            ).format_corrector()
            frame[column] = clean_frame[column]
        return frame

    def big_dict_to_sql(self, dict_to_sql: Dict[int, Dict[str, Any]]) -> None:
        """Start the procedure of writing to the database.

        Args:
            dict_to_sql ( Dict[int, Dict]): dictionary with data to write
                to the database
        """
        self.sql_scheme.insert_big_data(dict_to_sql, self.id_sql)

    def table_to_sql(self) -> None:
        """Run a method to write data to the database."""
        self.big_dict_to_sql(self.frame_to_dict(self.list_to_frame()))
        logging.debug('finish file_reader.table_to_sql')

    def _excel_cells(self) -> Dict[str, List[str]]:
        """Return the range of readable cells in the column.

        Returns:
            Dict[str, List[str]]: dictionary containing: key -
            the name of the excel column; value - the range of the
            column to read in the form of a list ['a1', 'a10'].
        """
        return {
            column: self.columns_scheme[
                column
            ]['excel'][1] for column in self.excel_columns
        }

    def _excel_columns(self) -> List[str]:
        """Form list with columns names according to the configuration file.

        Returns:
            List[str]: list of columns names
        """
        return [
            col for col in self.columns if self.columns_scheme[col]['excel'][0]
        ]


class XmlReader(FrameHandler):
    """FrameHandler subclass for reading xml files.

    Read information from xml files and writes it
    to the postgresql database. The read data is transferred to the Dataframe.
    It is more convenient to work with data in this form. The Dataframe is
    checked for compliance with the established formats and correctness of
    the received data. After checking and converting the data, they are
    transferred to the database for writing.

    Attributes:
        xml_file (Path): path to xml file. The path is determined by\
            the configuration file "table.yaml". The internal\
            directory is used - "external_data_source".
        xml_obj (ElementTree): ElementTree object
        base_tag (str):

    """

    def __init__(self, table: str):
        """Init Reader class.

        Args:
            table (str): the name of the table for which we apply the
                corresponding rules from configuration file "table.yaml".
        """
        super().__init__(table)
        self.xml_file: Path = Path(self.table_scheme['xml_file'])
        self.xml_obj: ElementTree = ET.parse(self.xml_file).getroot()
        self.base_tag = self.table_scheme['base_tag']
        self.budget_year = self.xml_obj.find(
            self.table_scheme['budget_year']
        ).text

    def read(self) -> List[Dict[str, Any]]:
        data_list = []
        position = 0
        #перебор по базовым однотипным позициям
        for level in self.xml_obj.iter(self.base_tag):
            position += 1
            data_dict = {}
            #перебор внутри позиции по заданным элементам
            for column in self.columns:
                xml_tag = self.table_scheme['columns'][column]['xml'][1]
                curent_level = level.find(xml_tag)
                try:
                    data_dict[column] = curent_level.text
                except:
                    data_dict[column] = None
            data_dict['budget_year'] = self.budget_year
            data_list.append(data_dict)
        return data_list

    def list_to_frame(self)  -> pd.DataFrame:       
        return self.frame_corrector(
            cleaner.PandasFormat(
                self.table_name,
            ).big_frame_format_corrector(pd.DataFrame(self.read()))
        )

    def xml_to_excel(self):
        self.list_to_frame().to_excel("tmp/output.xlsx")
    
    def big_dict_to_sql(self, dict_to_sql: Dict[int, Dict[str, Any]]) -> None:
        """Start the procedure of writing to the database.

        Args:
            dict_to_sql ( Dict[int, Dict]): dictionary with data to write
                to the database
        """
        self.sql_scheme.insert_big_data(dict_to_sql, self.id_sql)

    def table_to_sql(self) -> None:
        """Run a method to write data to the database."""
        self.big_dict_to_sql(self.frame_to_dict(self.list_to_frame()))
        logging.debug('finish file_reader.table_to_sql')
        