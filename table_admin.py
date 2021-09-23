import time
start_time1 = time.time()

import file_reader 


#file_reader.Excel_reader('deals').table_to_sql()
#file_reader.Excel_reader('purchases').table_to_sql()
#file_reader.Excel_reader('budget_commitment').table_to_sql()
#file_reader.Excel_reader('payments_full').table_to_sql()
#file_reader.Excel_reader('payments_short').table_to_sql()
file_reader.Excel_reader('commitment_treasury').table_to_sql()

print("Время выполнения table_admin%s" % (time.time() - start_time1))