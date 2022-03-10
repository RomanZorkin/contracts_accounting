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
