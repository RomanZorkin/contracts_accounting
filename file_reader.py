"""This module read excel file План-закупок 2021.xlsm,
create dataframe with clean information
and write it to postgresql database"""
import time
start_time = time.time()

import pandas as pd
import openpyxl
from pathlib import *
import yaml



from sqlorm import cleaner, sql_admin

print("время выполнения import%s" % (time.time() - start_time))

#start_time = time.time()
config_file = Path('configuration/table.yaml')

with open(config_file) as fh:
    table_config = yaml.load(fh, Loader=yaml.FullLoader)

print("Scheme class inicialisation %s" % (time.time() - start_time))

class Reader():

    def __init__(self, table):
        self.table_name = table
        self.table_scheme = table_config[table]
        self.columns_scheme = self.table_scheme['columns']
        self.columns = list(self.columns_scheme)
        self.id_sql = self._sql_id()
        self.sql_scheme = sql_admin.Scheme(self.table_name)

    def _sql_id(self):
        for column in self.columns:
            format = self.columns_scheme[column]['format'].split(' ')
            if len(format) > 1:
                return column


class Excel_reader(Reader):

    def __init__(self, table):
        super().__init__(table)
        self.excel_file = Path(self.table_scheme['excel_file'])
        self.work_sheet = self.table_scheme['work_sheet']
        self.excel_rows = self.table_scheme['excel_rows']
        self.excel_columns = self._excel_columns()
        self.excel_cells = self._excel_cells()
        print("start excel inicialisation %s" % (time.time() - start_time), self.excel_file)
        self.wb_obj = openpyxl.load_workbook(self.excel_file)
        self.ws = self.wb_obj[self.work_sheet]
        print("class inicialisation %s" % (time.time() - start_time))

    def _excel_columns(self):
        return [
            c for c in self.columns if self.columns_scheme[c]['excel'][0]
        ]

    def _excel_cells(self):
        return {
            c: self.columns_scheme[c]['excel'][1] for c in self.excel_columns
        }

    def read(self, column):
        print(f"start read {column} column {(time.time() - start_time)}")
        clean_list = cleaner.ValueCleaner(
            [
                cell[0].value for cell in self.ws[
                    self.excel_cells[column][0] : self.excel_cells[column][1]
                ]
            ],
            self.table_name,
            column,
        )
        return clean_list.columns_cleaner()


    def dict_to_frame(self):
        #отловить ошибку формирует словарь в  независимости от знчений redaer
        #формирует, даже если пусто
        df_dict = {
            column : self.read(column) for column in self.excel_columns
        }
        print("finish dict_to_frame %s" % (time.time() - start_time))
        #frame = pd.DataFrame(df_dict)
        #print(cleaner.PandasCleaner(self.table_name).format_corrector(frame))
        return pd.DataFrame(df_dict)

    def list_to_frame(self):
        print('start list_to frame')
        frame = pd.DataFrame(columns=self.excel_columns)
        for column in self.excel_columns:
            clean_frame = cleaner.PandasFormat(
                self.table_name,
                pd.DataFrame(self.read(column), columns=[column]),
            ).format_corrector()
            frame[column] = clean_frame[column]
        #создает фрейм из колоноко фреймов, которфеы деланм из колоноко ексел и чистим, счала лист, потом, пандас
        print("finish list_to_frame %s" % (time.time() - start_time))
        return frame

    def frame_corrector(self, frame):
        return cleaner.StructureCleaner(
            table_name = self.table_name,
            inframe = frame
        ).structure_corrector()

    def frame_to_dict(self):
        # значение index формирует словарь построчно, если взять dict,
        # то клю будут колонки
        frame_dict = self.frame_corrector(
            self.list_to_frame()
        ).to_dict('index')
        for key, value in frame_dict.items():
            clean_dict = cleaner.PandasToDict(
                self.table_name,
                {
                    **{'table_name' : self.table_name},
                    **value
                },
            ).clean_rows()
            frame_dict[key] = clean_dict
        # dict format
        # {0:{'table_name': 'deals', 'reg_number': 1, 'KBK': "'2223'", ... },
        #  1:{'table_name': 'deals', 'reg_number': 1, 'KBK': "'2223'", ... }}
        return frame_dict

    def big_dict_to_sql(self, dict_to_sql):
        print("старт запись в базу %s" % (time.time() - start_time))
        self.sql_scheme.insert_big_data(dict_to_sql, self.id_sql)
        print("время выполнения %s" % (time.time() - start_time))

    def table_to_sql(self):
        self.big_dict_to_sql(self.frame_to_dict())
    

#Excel_reader(table).frame_to_sql()
