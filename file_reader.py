"""Transfers information from a given file to a database."""

import logging
import time
from pathlib import Path
from typing import Any, Dict, List

import openpyxl
import pandas as pd
import yaml

from sqlorm import cleaner, sql_admin

start_time = time.time()
print('время выполнения import%s' % (time.time() - start_time))

config_file = Path('configuration/table.yaml')

with open(config_file) as fh:
    table_config = yaml.load(fh, Loader=yaml.FullLoader)

logging.debug(
    "Scheme class inicialisation %s" % (time.time() - start_time),
)

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
        table_name (str): the name of the table for which we apply the
            corresponding rules from configuration file "table.yaml".
        table_scheme (Dict): a dictionary with all the rules, parameters and
            settings for the table. The following parameters are passed:
            the name of the table, the name of the file to read,
            the sheet to read, the column settings (names, data format,
            which are used for sql, the range of cells in excel, etc.).
        columns_scheme (Dict): dictionary with column parameters:
            key - column name, value - dictionary with parameters
            (data format, whether it is part of SQL tables (true or false),
            whether it is part of excel tables and a range of cells).
        columns (List): list of columns names.
        id_sql (str): name of PRIMARY KEY sql column.
        sql_scheme: sql_admin.Scheme object. Required for
            interaction with postgresql.
    """

    def __init__(self, table: str):
        """Inits Reader class.

        Args:
            table (str): the name of the table for which we apply the
                corresponding rules from configuration file "table.yaml".
        """
        self.table_name: str = table
        self.table_scheme: Dict[str, Any] = table_config[table]
        self.columns_scheme: Dict[str, Dict[str, Any]] = self.table_scheme['columns']
        self.columns: List[str] = list(self.columns_scheme)
        self.id_sql: str = self._sql_id()
        self.sql_scheme = sql_admin.Scheme(self.table_name)

    def _sql_id(self) -> str:
        """Form name of PRIMARY KEY sql column."""
        for column in self.columns:
            format = self.columns_scheme[column]['format'].split(' ')
            if len(format) > 1:
                return column


class Excel_reader(Reader):
    """Reader subclass for reading excel files.

    Read information from excel files and writes it
    to the postgresql database

    Attributes:
        excel_file (Path): path to excel file. The path is determined by
            the configuration file "table.yaml". The internal
            directory is used - "external_data_source".
        work_sheet (str): name excel workshit, where the
            information is located.
        excel_rows (List[str]): the range of rows where
            the information is placed.
        excel_columns (List[str]): excel columns names.
        excel_cells (Dict[str, List[str]]): dictionary containing: key -
            the name of the excel column; value - the range of the
            column to read in the form of a list ['a1', 'a10'].
        wb_obj (openpyxl): openpyxl object - excel workbook (excel file).
        ws (openpyxl): openpyxl object - excel worksheet in workbook.
    """

    def __init__(self, table: str):
        """Inits Reader class.

        Args:
            table (str): the name of the table for which we apply the
                corresponding rules from configuration file "table.yaml".
        """
        super().__init__(table)
        self.excel_file: Path = Path(self.table_scheme['excel_file'])
        self.work_sheet: str = self.table_scheme['work_sheet']
        self.excel_rows: List[str] = self.table_scheme['excel_rows']
        self.excel_columns: List[str] = self._excel_columns()
        self.excel_cells: Dict[str, List[str]] = self._excel_cells()
        logging.debug(
            "start excel inicialisation %s" % (time.time() - start_time),
            self.excel_file,
        )
        self.wb_obj = openpyxl.load_workbook(self.excel_file)
        self.ws = self.wb_obj[self.work_sheet]
        print("class inicialisation %s" % (time.time() - start_time))

    def _excel_columns(self) -> List[str]:
        """Form list with columns names according to the configuration file.

        Returns:
            List[str]: list of columns names
        """
        return [
            c for c in self.columns if self.columns_scheme[c]['excel'][0]
        ]

    def _excel_cells(self) -> Dict[str, List[str]]:
        """Return the range of readable cells in the column.

        Returns:
            Dict[str, List[str]]: dictionary containing: key -
            the name of the excel column; value - the range of the
            column to read in the form of a list ['a1', 'a10'].
        """
        return {
            c: self.columns_scheme[c]['excel'][1] for c in self.excel_columns
        }

    def read(self, column: str) -> List[Any]:
        """Generate a list of values by column.

        The function reads the values of the column cells and passes them
        to the list. The list is processed using the Cleaner module to
        correct the values and bring them to the specified formats.

        Args:
            column (str): name of the column with extracted values.
        Returns:
            List[Any]: list of column values.
        """
        print(f"start read {column} column {(time.time() - start_time)}")
        clean_list = cleaner.ValueCleaner(
            [
                cell[0].value for cell in self.ws[
                    self.excel_cells[column][0] : self.excel_cells[column][1]
                ]
            ],
            self.table_name,
            column,
        )
        return clean_list.columns_cleaner()


    def dict_to_frame(self) -> pd.DataFrame:
        """Convert dictionary to pandas DataFrame.



        Returns:
            pd.DataFrame:
        """
        # отловить ошибку формирует словарь в  независимости от знчений redaer
        # формирует, даже если пусто
        df_dict = {
            column : self.read(column) for column in self.excel_columns
        }
        logging.debug("finish dict_to_frame %s" % (time.time() - start_time))
        # frame = pd.DataFrame(df_dict)
        # print(cleaner.PandasCleaner(self.table_name).format_corrector(frame))
        return pd.DataFrame(df_dict)

    def list_to_frame(self) -> pd.DataFrame:
        print('start list_to frame')
        frame = pd.DataFrame(columns=self.excel_columns)
        for column in self.excel_columns:
            clean_frame = cleaner.PandasFormat(
                self.table_name,
                pd.DataFrame(self.read(column), columns=[column]),
            ).format_corrector()
            frame[column] = clean_frame[column]
        #создает фрейм из колоноко фреймов, которфеы деланм из колоноко ексел и чистим, счала лист, потом, пандас
        print("finish list_to_frame %s" % (time.time() - start_time))
        return frame

    def frame_corrector(self, frame: pd.DataFrame) -> pd.DataFrame:
        return cleaner.StructureCleaner(
            table_name = self.table_name,
            inframe = frame
        ).structure_corrector()

    def frame_to_dict(self)-> Dict:
        # значение index формирует словарь построчно, если взять dict,
        # то клю будут колонки
        frame_dict = self.frame_corrector(
            self.list_to_frame()
        ).to_dict('index')
        for key, value in frame_dict.items():
            clean_dict = cleaner.PandasToDict(
                self.table_name,
                {
                    **{'table_name' : self.table_name},
                    **value
                },
            ).clean_rows()
            frame_dict[key] = clean_dict
        # dict format
        # {0:{'table_name': 'deals', 'reg_number': 1, 'KBK': "'2223'", ... },
        #  1:{'table_name': 'deals', 'reg_number': 1, 'KBK': "'2223'", ... }}
        return frame_dict

    def big_dict_to_sql(self, dict_to_sql) -> None:
        print("старт запись в базу %s" % (time.time() - start_time))
        self.sql_scheme.insert_big_data(dict_to_sql, self.id_sql)
        print("время выполнения %s" % (time.time() - start_time))

    def table_to_sql(self) -> None:
        self.big_dict_to_sql(self.frame_to_dict())


#Excel_reader(table).frame_to_sql()
