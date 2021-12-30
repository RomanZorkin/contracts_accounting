from pathlib import Path
import asyncio
import yaml
import logging

from sqlorm import run


config_file = Path('configuration/table.yaml')

with open(config_file) as fh:
    table_config = yaml.load(fh, Loader=yaml.FullLoader)

class Scheme(object):
    """Class docstring.
    
    Ful class description

    Args:
        table (str):

    Atributes:
        table_config:
        table_name:
        columns:
        columns_rule:
        sql_scheme:
        sql_table:
        is_table:
    """

    def __init__(self, table: str):
        """Init Scheme class."""

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
        """Method docstring."""
        one_sql_scheme = {'table_name' : self.table_name}
        for column in self.columns:
            if self.columns_rule[column]['SQL']:
                one_sql_scheme[column] = self.columns_rule[column]['format']
        return one_sql_scheme

    def is_table(self) -> None:
        """Checks the table for presence in the database.
        
        If there is no corresponding table in the database. 
        Creates a table according to the established scheme (configuration).
        """
        logging.debug('start table in DB cheker')
        loop = asyncio.get_event_loop()
        status = loop.run_until_complete(self.sql_table.is_table())
        if len(status) == 0:            
            self.create_table()

    def create_table(self) -> None:
        """Create a new table in the database.
        
        
        """
        logging.debug('start table creator function')
        # table_scheme pattern:
        # {'table_name: '', 'id' : 'integer PRIMARY KEY,', 'column1' : 'text', ...}
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.sql_table.create_table())

    def insert_data(self, excel_dict, index):
        """Method docstring."""
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.sql_table.edit_table(excel_dict))

    def insert_big_data(self, excel_dict, index):
        """Method docstring."""
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.sql_table.edit_bigdata_table(excel_dict, index))

    def read_row(self, query):
        """Method docstring."""
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self.sql_table.read_row(query))

    def read_row_list(self, query):
        """Method docstring."""
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self.sql_table.read_row_list(query))

    def read_all_column(self, column):
        """Method docstring."""
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self.sql_table.one_column_read(column))