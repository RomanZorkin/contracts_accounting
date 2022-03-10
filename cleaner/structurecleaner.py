"""Corrects the original data before writing them to a database or frame."""

import datetime
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import yaml

from cleaner.cleaner import Cleaner
import sqlorm.word_module as wm

config_file = Path('configuration/table.yaml')
with open(config_file) as fh:
    table_config = yaml.load(fh, Loader=yaml.FullLoader)  # noqa: S506

BOOL_NAME = 'boolean'
DATE_NAME = 'date'
FLOAT_NAME = 'numeric'
INT_NAME = 'integer'
STR_NAME = 'text'


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

    def structure_corrector(self) -> pd.DataFrame:
        """A method to start structure correcting.

        Each table has its own rules. We can form new records\
        for new columns, or adjust existing values.

        Returns:
            pd.DataFrame: correct DataFrame.
        """
        return self.function_dict[self.table_name]()
        