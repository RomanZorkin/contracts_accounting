import asyncio
import logging

async def create_query_builder(table_sheme):
    """
    {'table_name' : 'name', 'id' : 'integer PRIMARY KEY,', 'column1' : 'text', ...}
    """
    print('start create_query_builder')
    table_name = ''
    columns = ''
    columns_list = []
    counter = 1
    dict_length = len(table_sheme)
    for key, value in table_sheme.items():
        if counter == 1:
            table_name = value
            counter += 1
        elif counter == dict_length:
            columns = '{0}{1} {2}\n'.format(columns, key, value)
            columns_list.append(key)
            counter += 1
        else:
            columns = '{0}{1} {2},\n'.format(columns, key, value)
            columns_list.append(key)
            counter += 1

    query = 'CREATE TABLE IF NOT EXISTS {0} ({1})'.format(table_name, columns)
    return query, columns_list

async def insert_query_builder(insert_data, index):
    """
    {'table_name' : 'name', 'id' : 1, 'column1' : 'text', ...}
    """
    # наименование колонок
    columns = ''
    # значения колнок
    values = ''
    # колонки = значение - формат для обновления
    update_data = ''
    counter = 1
    dict_length = len(insert_data)
    # первая строка словаря содержит название таблицы его пропускаем
    # последнию строку заполняем по особому правилу - без запятой на конце
    # остальные строки формируем простым переборовм оставляем запятую на конце
    for key, value in insert_data.items():
        if counter == 1:
            table_name = value
            counter += 1
        # значения для конца запроса, без запятой на конце
        elif counter == dict_length:
            columns = '{0}{1}\n'.format(columns, key)
            values = '{0}{1}\n'.format(values, value)
            update_data ='{}{}={}\n'.format(update_data, key, value)
            counter += 1
        else:
            # значения с запятой на конце, для середины запроса
            columns = '{0}{1},\n'.format(columns, key)
            values = '{0}{1},\n'.format(values, value)
            update_data ='{}{}={},\n'.format(update_data, key, value)
            counter += 1

    query = '''INSERT INTO {0}({1}) VALUES({2})
    ON CONFLICT ({3}) DO UPDATE SET {4};'''.format(
        table_name, columns, values, index, update_data,
    )
    return query

async def read_row_query_builder(table, insert_query):

    query = 'SELECT {0} FROM {1} WHERE {2} = {3}'.format(
        insert_query['search_column'],
        table,
        insert_query['index'],
        insert_query['index_value'],
    )
    return query

async def all_column_query_builder(table, column):
    return 'SELECT {0} FROM {1};'.format(column, table)

async def find_max_date_row():
    query = '''
    SELECT * FROM {0} 
    WHERE commitment_number = '0015271220000000214' 
    AND registration_date = 
    (SELECT MAX(registration_date) FROM {0} 
    WHERE commitment_number = '0015271220000000214');'''.format(
        commitment_treasury,
        comitment,
        registration_date,
    )
    
'''
INSERT INTO directories_struct (id, html, css)
VALUES ($3::int, $1::text, $2::text)
ON CONFLICT (id) DO UPDATE SET
html=$1::text,css=$2::text;'''