"""The module is used to work with the PostgreSQL database via asyncpg

with the help of the module, you can:
create, fill in, edit, read, delete
database tables
"""

import os
import asyncio
import configparser
from pathlib import *
import logging

import asyncpg

from sqlorm import query_builder


config_file = Path('config.ini')
config = configparser.ConfigParser()
config.read(config_file)

connection_rule = config['DB_connection']['rule']

class DBTable(object):
    """
    Class rule by Db tables
    """

    def __init__(self, sheme=None):
        """sheme is a dict in format:
        {'table_name: '', 'id' : 'integer PRIMARY KEY,', 'column1' : 'text', ...}
        """
        self.table_name = sheme['table_name']
        self.table_sheme = sheme

    async def is_table(self):
        conn = await asyncpg.connect(connection_rule)
        status = await conn.fetch(
            "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{0}';".format(
                self.table_name,
            )
        )
        return [value[0] for value in status]

    async def create_table(self):
        """
        Method creat new table in DB
        """
        print('start creater')
        conn = await asyncpg.connect(connection_rule)
        sql_query, columns_list = await query_builder.create_query_builder(self.table_sheme)
        print(sql_query, columns_list)
        await conn.execute(sql_query)
        await conn.close()
        self.columns = columns_list

    async def edit_table(self, insert_data, index):
        """
        Method insert into table information
        """
        print('start edit_table')
        conn = await asyncpg.connect(connection_rule)
        sql_query = await query_builder.insert_query_builder(insert_data, index)
        #print(sql_query)
        await conn.execute(sql_query)
        await conn.close()

    async def edit_bigdata_table(self, insert_data, index):
        #insert data format:
        #{1:{'table_name': 'deals', 'reg_number': 1, 'KBK': "'2223'", ... },
        # 2:{'table_name': 'deals', 'reg_number': 1, 'KBK': "'2223'", ... }}
        conn = await asyncpg.connect(connection_rule)        
        for row in insert_data.values():                      
            sql_query = await query_builder.insert_query_builder(row, index)
            #print(sql_query)
            await conn.execute(sql_query)
        await conn.close()

    async def one_column_read(self, column):
        """
        Method read all row from column
        """
        print('start one_column_read')
        conn = await asyncpg.connect(connection_rule)
        sql_query = await query_builder.all_column_query_builder(self.table_name, column)
        #print(sql_query)
        rows = await conn.fetch(sql_query)
        await conn.close()
        return [value[0] for value in rows]

    async def read_row(self, query):
        """
        Method read row information
        """
        print('start_read_row')
        conn = await asyncpg.connect(connection_rule)
        sql_query = await query_builder.read_row_query_builder(self.table_name, query)
        print(sql_query)
        row = await conn.fetchrow(sql_query)
        await conn.close()
        return row[0]

    async def read_row_list(self, query_list):
        """
        Method read row information
        """
        print('start_read_row_list')
        row_dict = {}
        conn = await asyncpg.connect(connection_rule)
        for query in query_list:            
            sql_query = await query_builder.read_row_query_builder(self.table_name, query)
            print(sql_query)
            row = await conn.fetchrow(sql_query)
            row_dict[query[list(query)[2]]] = row[0]
        await conn.close()
        return row_dict

    async def delete_table(self):
        """
        Method delete table in DB
        """
        conn = await asyncpg.connect(connection_rule)
        await conn.execute("""
            DROP TABLE {0}
        """.format(self.table_name,)
        )
        await conn.close()
