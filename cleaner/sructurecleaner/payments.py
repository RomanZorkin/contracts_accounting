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
