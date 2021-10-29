"""Module for scan counter starting."""

import file_reader
from scan_counter import Counter
from sqlorm import sql_admin

question = """
    Введите вариант подсчета сканов:\n
    1. введите - 1, если требуется обновить базу данных\n
    время обработки запроса - не более 9 секунд
    2. введите - 2, если обновление не требуется\n
    время обработки запроса - не более 1,5 секунд
"""
choice = str(input(question))

if choice == '1':
    file_reader.Excel_reader('deals').table_to_sql()

run = Counter(sql_admin.Scheme('deals'), '2021')
run.make_report()
