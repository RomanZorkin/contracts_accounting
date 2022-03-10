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


class Deals()
    def deals(self) -> pd.DataFrame:
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
