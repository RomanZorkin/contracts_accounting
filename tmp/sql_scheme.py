import yaml
from pathlib import Path
import pandas as pd

config_file = Path('configuration/table.yaml')

with open(config_file) as fh:
    table_config = yaml.safe_load(fh)

print(list(table_config))
tables = list(table_config)
table_dict = {}
max_len = 0
for table in tables:
    table_dict[table] = {}
    for column in list(table_config[table]['columns']):
        table_dict[table][column] = table_config[table]['columns'][column]['format']
    sorted(table_dict[table])
    table_len = len(table_dict[table])
    if table_len > max_len:
        max_len = table_len    
print(table_dict)
print(max_len)

def plus_len(curent_dict):
    delta = max_len - len(curent_dict)
    for x in range(delta):
        curent_dict[x] = None
    return curent_dict
len_list = []
for table in tables:
    print(table_dict[table])
    table_dict[table] = plus_len(table_dict[table])
    print(table_dict[table])
    len_list.append(len( table_dict[table]))

print(len_list)


new_dict = {}
for table in tables:
    format = f'{table}_format'
    key_list = []
    value_list = []
    for key, value in table_dict[table].items():
        key_list.append(key)
        value_list.append(value)
    new_dict[table] = key_list
    new_dict[format] = value_list



frame = pd.DataFrame(new_dict)
frame.to_excel('tmp/sql_scheme.xlsx')