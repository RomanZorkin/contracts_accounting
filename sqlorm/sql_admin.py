from pathlib import *
import asyncio
import yaml
import logging

from sqlorm import run


config_file = Path('configuration/table.yaml')

with open(config_file) as fh:
    table_config = yaml.load(fh, Loader=yaml.FullLoader)

class Scheme():

    def __init__(self, table):
        self.table_config = table_config[table]
        self.table_name = table
        # list of columns names, that use for creating sql_table and pandas data frame
        self.columns = list(self.table_config['columns'])
        # the dict with rules for each column, as for sql, as for pandas, as for excel
        self.columns_rule = self.table_config['columns']
        # scheme of sql table
        self.sql_scheme = self._sql_scheme_creator()
        self.sql_table = run.DBTable(self.sql_scheme)
        self.is_table()

    def _sql_scheme_creator(self):
        _sql_scheme = {'table_name' : self.table_name}
        for column in self.columns:
            if self.columns_rule[column]['SQL']:
                _sql_scheme[column] = self.columns_rule[column]['format']
        return _sql_scheme

    def is_table(self):
        loop = asyncio.get_event_loop()
        status = loop.run_until_complete(self.sql_table.is_table())
        if len(status) == 0:
            print('start_creator')
            self.create_table()

    def create_table(self):
        print('create_function')
        # table_scheme pattern:
        # {'table_name: '', 'id' : 'integer PRIMARY KEY,', 'column1' : 'text', ...}
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.sql_table.create_table())

    def insert_data(self, excel_dict, index):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.sql_table.edit_table(excel_dict))

    def insert_big_data(self, excel_dict, index):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.sql_table.edit_bigdata_table(excel_dict, index))
    
    def read_row(self, query):        
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self.sql_table.read_row(query))

    def read_row_list(self, query):        
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self.sql_table.read_row_list(query))

    def read_all_column(self, column):
        loop = asyncio.get_event_loop()        
        return loop.run_until_complete(self.sql_table.one_column_read(column))