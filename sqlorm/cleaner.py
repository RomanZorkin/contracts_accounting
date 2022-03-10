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


class PandasCleaner(Cleaner):
    """Cleaner subclass.

    This class is bathe class to works with dicts.
    The class can be used in two cases:
    - when data from the frame is transferred for writing to the database,
    they must be represented as a dictionary. At the same time, the
    dictionary is pre-checked for correctness.
    - the second case is when the information from the source is extracted
    in the form of a dictionary and it needs to be transferred to a dataframe.

    Arguments:
        dict_for_clean (Dict[Any, Any]): dict for correct.
        pandas_type (Dict[str, str]):
    """

    def __init__(self, table_name=None, dict_for_clean=None):
        """Init PandasCleaner class.

        Args:
            table_name (str): the name of the table for which we apply the\
                corresponding rules from configuration file "table.yaml".
            dict_for_clean (Dict[Any, Any]): dict for correct.

        """
        super().__init__(table_name)
        self.dict_for_clean: Dict[Any, Any] = dict_for_clean
        self.pandas_type: Dict[str, str] = {
            BOOL_NAME: 'bool',
            DATE_NAME: 'datetime64[ns]',
            INT_NAME: 'int',
            FLOAT_NAME: 'float',
            STR_NAME: 'str',
        }


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


class StructureCleaner(Cleaner):
    """Fills the date frame with information missing from the source file.

    The structure of sql tables may contain columns that are not in the file
    from where the data is being read. This class implements methods that
    allow you to fill such columns with data. The data is obtained by the
    calculation method.

    Atributes:
        frame (pd.DataFrame):
        function_dict (Dict[str, Any]):
    """

    def __init__(
            self,
            table_name: str,
            inframe: pd.DataFrame,
            budget_year: Any,
    ):
        """Init StructureCleaner class.

        Args:
            table_name (str): table name.
            inframe (pd.DataFrame): frame wich we correct.
        """
        super().__init__(table_name)
        self.budget_year = budget_year
        self.frame: pd.DataFrame = inframe
        self.function_dict: Dict[str, Any] = {
            'budget_commitment': self._budget_commitment,
            'purchases': self._purchases,
            'deals': self._deals,
            'payments_short': self._payments,
            'payments_full': self._payments,
            'commitment_treasury': self._commitment_treasury,
            'plan': self._plan,
            'internal_plan': self._internal_plan,
            'limits': self._limits,
        }
        self.reg_number_pattern = {
            'year': '0',
            'reg_number': 0,
            'reg_number_full': '0/0',
        }

    def none_row_deleter(self) -> pd.DataFrame:
        """Method for deleting rows without useful information."""
        self.frame = self.frame[
            (self.frame.one > 0) | (self.frame.two > 0) | (self.frame.three > 0)  # noqa F821, WPS465
            ]

    def structure_corrector(self) -> pd.DataFrame:
        """A method to start structure correcting.

        Each table has its own rules. We can form new records\
        for new columns, or adjust existing values.

        Returns:
            pd.DataFrame: correct DataFrame.
        """
        return self.function_dict[self.table_name]()

    def _deals(self) -> pd.DataFrame:
        """Correct the data structure of the "deals" table.

        Method create sql_id column as summ of "reg_number + / + two last
        digits of budget year" and insert new column - budget year.

        Returns:
            pd.DataFrame: new DataFrame
        """
        logging.debug('start Structure corrector _deals')
        clean_frame = pd.DataFrame()

        clean_frame['budget_year'] = [
            self.budget_year
        ] * len(self.frame['reg_number'])

        clean_frame['slash'] = ['/'] * len(self.frame['reg_number'])
        clean_frame['short_year'] = [self.budget_year - 2000] * len(self.frame['reg_number'])

        clean_frame['reg_number_full'] = self.frame['reg_number'].astype(str) \
            + clean_frame['slash'] + clean_frame['short_year'].astype(str)

        self.frame['budget_year'] = PandasFormat(
            self.table_name,
            clean_frame,
        ).format_corrector()

        clean_frame = clean_frame.drop('budget_year', 1)
        clean_frame = clean_frame.drop('slash', 1)
        clean_frame = clean_frame.drop('short_year', 1)
        self.frame['reg_number_full'] = PandasFormat(
            self.table_name,
            clean_frame,
        ).format_corrector()
        self.frame['additional_agreement'] = '' * len(self.frame)
        for i in range(len(self.frame)):
            self.frame['additional_agreement'].iloc[i] = self._additional(
                self.frame['subject'].iloc[i],
            )
        return self.frame

    def _payments(self) -> pd.DataFrame:
        """Correct the data structure of the "payments" table.

        Returns:
            pd.DataFrame: new DataFrame
        """
        for column in list(self.reg_number_pattern):
            clean_frame = pd.DataFrame()
            # information is extracted line by line from the
            # "contract_identificator" column
            for deal_object in self.frame['contract_identificator']:
                clean_frame = clean_frame.append(
                    {
                        column: self._find_reg_number(deal_object)[column],
                    },
                    ignore_index=True,
                )
            self.frame[column] = PandasFormat(
                self.table_name,
                clean_frame,
            ).format_corrector()
        # self.frame.to_excel('платеж из 1с.xlsx')
        # формирую индекс как сумму номера зкр и года
        for i in range(len(self.frame)):
            self.frame['order_number'].iloc[i] = '{0}_{1}'.format(
                str(self.frame['order_date'].dt.year.iloc[i]),
                self.frame['order_number'].iloc[i],
            )
        # self.frame['order_number'] = self.frame['order_number'] + f'_{}'
        return self.frame

    def _commitment_treasury(self) -> pd.DataFrame:
        """Correct the data structure of the "commitment_treasury" table.

        Generates a unique value for the SQL prmery ID column. In the source
        file, the values can be repeated in each column. To create a unique
        value, the values document number and document date are summed up

        Returns:
            pd.DataFrame: new DataFrame
        """
        logging.debug('start Structure corrector _commitment_treasury')
        clean_frame = pd.DataFrame()
        clean_frame[self.id_sql] = self.frame['doc_number'].astype(str) \
                                   + self.frame['doc_date'].astype(str)
        self.frame[self.id_sql] = PandasFormat(
            self.table_name,
            clean_frame,
        ).format_corrector()
        self.frame = self.frame[
            self.frame['status'] == 'Отражен на ЛС'
            ]
        return self.frame

    def _plan(self) -> pd.DataFrame:
        """Correct the data structure of the "plan" table.

        Returns:
            pd.DataFrame: new DataFrame
        """
        # обрезает IKZ до знаков т.к. хвост мешает и вносит путаницу при загрузке и
        # форимровании IKZ в других таблицах там че попало,
        # поэтому и там тоже обрезаем
        for row in self.frame['ikz']:
            location = pd.Index(self.frame['ikz']).get_loc(row)
            self.frame['ikz'].iloc[location] = row[:26]
        return self.frame

    def _purchases(self) -> pd.DataFrame:
        """Correct the data structure of the "purchases" table.

        There is no short purchase number in the source file.
        The number is calculated by extracting 3 digits from the unique
        purchase number - from 24 to 26 characters.
        Short numbers can be repeated.

        Returns:
            pd.DataFrame: new DataFrame
        """
        short_num_start = 23
        short_num_finish = 26
        logging.debug('start Structure corrector _purchases')
        clean_frame = pd.DataFrame()
        clean_frame['state_register_number'] = [
            int(order_number[short_num_start:short_num_finish:])
            for order_number in self.frame['order_number']
        ]
        self.frame['state_register_number'] = PandasFormat(
            self.table_name,
            clean_frame,
        ).format_corrector()
        return self.frame

    def _budget_commitment(self) -> pd.DataFrame:
        """Correct the data structure of the "budget_commitment" table.

        The columns "year", "reg_numbers", "reg_number_full" are missing
        in the source file. These columns are filled in by calculation.
        From the "contract_identificator" column, the contract attribute
        is extracted in the format "25/21", where 25 is the registration
        number, and 21 is the budget year of the contract.

        Returns:
            pd.DataFrame: new DataFrame
        """
        # list of columns
        logging.debug('start _budget_commitment in StructureCleaner')
        for column in list(self.reg_number_pattern):
            clean_frame = pd.DataFrame()
            # information is extracted line by line from the
            # "contract_identificator" column
            for deal_object in self.frame['contract_identificator']:
                clean_frame = clean_frame.append(
                    {
                        column: self._find_reg_number(deal_object)[column],
                    },
                    ignore_index=True,
                )
            self.frame[column] = PandasFormat(
                self.table_name,
                clean_frame,
            ).format_corrector()
        # self.frame.to_excel('договры из 1с.xlsx')
        return self.frame

    def _find_reg_number(self, deal_object: str) -> Dict[str, Any]:
        """Methods forms reg number information.

        Method forms dictionary with a set of values associated with the
        registration number of the contract:
        - Year of registration;
        - short registration number of the contract;
        - full registration number of the contract.
        For example, the contract registration number is 4/21, the data will
        be transferred to the dictionary:
        {'year': '2021', 'reg_number': 4, 'reg_number_full': '4/21'}.
        If there are some errors methods return default
        self.reg_number_pattern dict.

        Arguments:
            deal_object (str): record from 'contract_identificator' column

        Returns:
            Dict[str, Any]: dictionary with data for filling columns:\
                'year', 'reg_number', 'reg_number_full'.
        """
        deal_object_list = deal_object.replace(
            "'",
            "",  # noqa: Q000
        ).split(' ')[-1].split('/')
        work_len = len(deal_object_list)
        if work_len > 1:
            num_list = deal_object_list[work_len - 2].split(' ')
            try:  # noqa: WPS229
                return self._reg_number_maker(
                    deal_object_list,
                    work_len,
                    num_list,
                )
            except TypeError:
                logging.critical('StructureCleaner find_reg_number incident')
                return self.reg_number_pattern
        return self.reg_number_pattern

    def _reg_number_maker(
            self,
            deal_object: List[str],
            work_len: int,
            num_list: List[str],
    ) -> Dict[str, Any]:
        """Methods forms reg number information.

        Method forms dictionary with a set of values associated with the
        registration number of the contract:
        - Year of registration;
        - short registration number of the contract;
        - full registration number of the contract.
        For example, the contract registration number is 4/21, the data will
        be transferred to the dictionary:
        {'year': '2021', 'reg_number': 4, 'reg_number_full': '4/21'}.

        Arguments:
            deal_object (List[str]): a list with two values: reg.contract\
                number and year of its registration
            work_len (int): len of deal_object list
            num_list (List[str]): reg_number

        Returns:
            Dict[str, Any]: dictionary with data for filling columns:\
                'year', 'reg_number', 'reg_number_full'.
        """
        millennium = 2000
        external_dict = {
            'year': str(int(deal_object[work_len - 1]) + millennium),
            'reg_number': int(num_list[-1]),
            'reg_number_full': None,
        }
        external_dict['reg_number_full'] = str(
            '{0}/{1}'.format(
                external_dict['reg_number'],
                int(external_dict['year']) - millennium,
            ),
        )
        return external_dict

    def _internal_plan(self):

        print('start_internal plan')
        names_dict = {}
        for column in self.columns:
            word_name = self.table_scheme['columns'][column]['word'][1]
            if len(word_name) > 0:
                names_dict[word_name[0]] = column

        self.frame = wm.make_new_frame(self.frame, names_dict)
        numeric_columns = ['amount_new', 'amount']
        for column in numeric_columns:
            self.frame[column] = pd.to_numeric(self.frame[column])
        self.frame.to_excel('tmp/Закупки 2022.xlsx', index=False)
        print(f'create file Закупки.xlsx')
        return self.frame

    def _limits(self):
        # заполняем пустые строки
        self.frame['curent_limit'] = round(self.frame['curent_limit']*1000)
        self.frame['head'] = '202'
        self.frame['budget_year'] = 2022
        self.frame['kbk'] = ''
        self.frame['kbk_index'] = ''
        self.frame['detalisation'].str.lower()
        # формирую индекс как сумму номера зкр и года
        for i in range(len(self.frame)):
            if len(self.frame['rpr'].iloc[i]) < 4:
                self.frame['rpr'].iloc[i] = f"0{self.frame['rpr'].iloc[i]}"
            self.frame['kbk'].iloc[i] = '{0}{1}{2}{3}{4}'.format(
                self.frame['head'].iloc[i],
                self.frame['rpr'].iloc[i],
                self.frame['csr'].iloc[i],
                self.frame['vr'].iloc[i],
                self.frame['kosgu'].iloc[i],
            )
            self.frame['kbk_index'].iloc[i] = '{0}_{1}_{2}'.format(
                self.frame['kbk'].iloc[i],
                self.frame['detalisation'].iloc[i],
                self.frame['budget_year'].iloc[i],
            )
        return self.frame

    def _additional(self, subject: str) -> str:
        subject_list = subject.lower().split(' ')
        #print(subject_list)
        if subject_list[0] == 'д/с' and subject_list[1] == 'к':
            #print(subject_list[2])
            return subject_list[2]
        return 'None'
