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
