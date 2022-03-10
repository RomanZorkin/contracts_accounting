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
