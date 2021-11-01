"""Module for start database writing."""

import logging
from pathlib import Path
import time

import file_reader

start_time1 = time.time()
#logfile = Path('debug_table_admin.log')

logging.basicConfig(filename='test.log', filemode='w', level=logging.DEBUG)
logging.debug('table_admin%s' % (time.time() - start_time1))
#file_reader.ExcelReader('deals').table_to_sql()
#file_reader.ExcelReader('purchases').table_to_sql()
#file_reader.ExcelReader('budget_commitment').table_to_sql()
#file_reader.ExcelReader('payments_full').table_to_sql()
#file_reader.ExcelReader('payments_short').table_to_sql()
file_reader.ExcelReader('commitment_treasury').table_to_sql()

print('Время выполнения table_admin%s' % (time.time() - start_time1))
