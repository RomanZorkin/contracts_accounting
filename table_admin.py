"""Module for start database writing."""

import logging
from pathlib import Path
import time

import file_reader

import api.sql_scheme

start_time1 = time.time()
#logfile = Path('debug_table_admin.log')

#logging.basicConfig(filename='test.log', filemode='w', level=logging.DEBUG)
#logging.debug('table_admin%s' % (time.time() - start_time1))
#file_reader.ExcelReader('deals', 2022).table_to_sql()  # договоры
#file_reader.ExcelReader('purchases').table_to_sql() # старый вариант плана графика из еис print form.xls
#file_reader.ExcelReader('budget_commitment').table_to_sql() # Принятые обязательства по договрам из 1С
file_reader.ExcelReader('payments_full').table_to_sql()
#file_reader.ExcelReader('payments_short').table_to_sql()
#file_reader.ExcelReader('commitment_treasury').table_to_sql() # Обязательства ЭБ
#file_reader.XmlReader('plan').table_to_sql() # result.xml  плна график из еис
#file_reader.WordReader('internal_plan', 2022).table_to_sql() # Легендарная таблица
#file_reader.ExcelReader('limits', 2022).table_to_sql()

##############################################################

from tmp import tmp_sql_hader as tsh
import pandas as pd

commitments = tsh.Commitments()
#print(commitments.commitments_index())
reports = [
    {'frame': commitments.report(2022), 'filename': 'otcheti/извещения.xlsx'},
    {'frame': tsh.plan_graphic(), 'filename': 'otcheti/план-график 2022.xlsx'},     
]

for report in reports:    
    frame = pd.DataFrame(report['frame'])
    frame.to_excel(report['filename'])

frame = pd.DataFrame(tsh.union_plan_deals())
frame['pay_total']=frame.loc[:,['pay_short' , 'pay_full']].sum(axis = 1)
frame.to_excel('otcheti/контракты 2022.xlsx')

print('Время выполнения table_admin%s' % (time.time() - start_time1))
