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
        # self.frame.to_excel('???????????? ???? 1??.xlsx')
        # ???????????????? ???????????? ?????? ?????????? ???????????? ?????? ?? ????????
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
            self.frame['status'] == '?????????????? ???? ????'
            ]
        return self.frame

    def _plan(self) -> pd.DataFrame:
        """Correct the data structure of the "plan" table.

        Returns:
            pd.DataFrame: new DataFrame
        """
        # ???????????????? IKZ ???? ???????????? ??.??. ?????????? ???????????? ?? ???????????? ???????????????? ?????? ???????????????? ??
        # ???????????????????????? IKZ ?? ???????????? ???????????????? ?????? ???? ????????????,
        # ?????????????? ?? ?????? ???????? ????????????????
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
        # self.frame.to_excel('?????????????? ???? 1??.xlsx')
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
        self.frame.to_excel('otcheti/?????????????? 2022.xlsx', index=False)
        print(f'create file ??????????????.xlsx')
        return self.frame

    def _limits(self):
        # ?????????????????? ???????????? ????????????
        self.frame['curent_limit'] = round(self.frame['curent_limit']*1000)
        self.frame['head'] = '202'
        self.frame['budget_year'] = 2022
        self.frame['kbk'] = ''
        self.frame['kbk_index'] = ''
        self.frame['detalisation'].str.lower()
        # ???????????????? ???????????? ?????? ?????????? ???????????? ?????? ?? ????????
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
