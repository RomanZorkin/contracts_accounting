import os
import json
import time
from pathlib import *
import yaml

import file_reader
from sqlorm import cleaner, sql_admin

start_time = time.time()

table = 'deals' # table scheme name
sql_scheme = sql_admin.Scheme(table)

config_file = Path('configuration/config.yaml')
with open(config_file) as fh:
    scan_config = yaml.load(fh, Loader=yaml.FullLoader)

class Counter():

    def __init__(self):
        self.scan_dir = Path(scan_config['scans'], str(scan_config['year']))
        self.csv_file = Path(self.scan_dir, 'scan.csv')

    def scan_list(self):
        return [
            int(file.stem) for file in sorted(Path(self.scan_dir).glob('*.pdf'))
        ]

    def deals_list(self):
        return sorted(sql_scheme.read_all_column('reg_number'))

    def lost_list(self):
        return sorted(
            list(
                set(self.deals_list()) - set(self.scan_list())
            )
        )

    def find_subject(self):
        lost_dict = {}
        for reg_number in self.lost_list():
            print(reg_number)
            query  = {
            'search_column' : 'subject',
            'index' : 'reg_number',
            'index_value' : reg_number,
            }
            lost_dict[reg_number] = sql_scheme.read_row(query)
        return lost_dict

    def find_subject_all(self):
        lost_dict = {}
        query_list = []
        for reg_number in self.lost_list():
            print(reg_number)
            query  = {
            'search_column' : 'subject',
            'index' : 'reg_number',
            'index_value' : reg_number,
            }
            query_list.append(query)  
        return sql_scheme.read_row_list(query_list)

    def make_otchet(self):
        with open (self.csv_file, 'w') as excel:
            excel.write ("Список отсутсвующих сканов государственных контрактов\n")
            for number, subject in self.find_subject_all().items():
                excel.write(f' Дог № {number};{subject}\n')
            excel.write("--- %s время выполнения PYTHON скрипта ---" % (time.time() - start_time))
        print('Записан файл ', self.csv_file)

choice = str(input(
    """Введите вариант подсчета сканов:\n
    1. введите - 1, если требуется обновить базу данных\n
    время обработки запроса - не более 9 секунд
    2. введите - 2, если обновление не требуется\n
    время обработки запроса - не более 1,5 секунд
    """
))

if choice == '1':
    file_reader.Excel_reader(table).table_to_sql()
test = Counter()
test.make_otchet()
#print(test.find_subject_1())
