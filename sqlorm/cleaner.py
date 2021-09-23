import datetime
import pandas as pd
from pathlib import *
import yaml


config_file = Path('configuration/table.yaml')
with open(config_file) as fh:
    table_config = yaml.load(fh, Loader=yaml.FullLoader)

class Cleaner():

    def __init__(self, table_name = None):
        self.table_name = table_name
        self.table_scheme = table_config[self.table_name]        
        self.columns = self.table_scheme['columns']
        self.id_sql = self._sql_id()

    def _sql_id(self):
        for column in self.columns:
            format = self.columns[column]['format'].split(' ')
            if len(format) > 1:
                return column

    def format(self, column):
        format = self.columns[column]['format'].split(' ')
        return format[0]


class PandasCleaner(Cleaner):
    '''Class used for clean pandas dataframe values
    '''

    def __init__(self, table_name = None, dict_for_clean = None):
        super().__init__(table_name)
        self.dict_for_clean = dict_for_clean
        self.pandas_type = {
            'boolean' : 'bool',
            'date' : 'datetime64[ns]',
            'integer' : 'int',
            'numeric' : 'float',
            'text' : 'str',
        }


class PandasToDict(PandasCleaner):


    def __init__(self, table_name = None, dict_for_clean = None):
        super().__init__(table_name, dict_for_clean)
        self.function_dict = {
            'boolean' : self._boolean_handler,
            'date' : self._date_handler,
            'integer' : self._integer_handler,
            'numeric' : self._numeric_handler,
            'text' : self._text_handler,
        }



    def _boolean_handler(self, column_name):
        if type(self.dict_for_clean[column_name]) == bool:
            pass
        elif self.dict_for_clean[column_name] == 'да':
            self.dict_for_clean[column_name] = True
        else:
            self.dict_for_clean[column_name] = False

    def _date_handler(self, column_name):
        input_value = self.dict_for_clean[column_name]
        # input value have a pandas format, thats why we use pandas method to get good string
        if pd.isnull(input_value):
            self.dict_for_clean[column_name] = "'1999-01-01'"
        elif type(input_value) is str:
            input_value = datetime.datetime.strptime(input_value, '%d.%m.%Y')
            input_value = input_value.strftime('%Y-%m-%d')
            self.dict_for_clean[column_name] = "'" + input_value + "'"
        else:
            print(input_value)
            # we convert pandas datestamp format to python datetime library format
            # input_value = input_value.dt.strftime('%Y-%m-%d')
            input_value = input_value.to_pydatetime().date()
            # convert date format to string
            input_value = input_value.strftime('%Y-%m-%d')
            self.dict_for_clean[column_name] = "'" + input_value + "'"


    def _integer_handler(self, column_name):
        input_value = self.dict_for_clean[column_name]
        if type(input_value) == int or type(input_value) == float:
            self.dict_for_clean[column_name] = int(input_value)
        else:
            self._error_incident('_integer_handler')

    def _numeric_handler(self, column_name):
        input_value = self.dict_for_clean[column_name]
        if type(input_value) == int or type(input_value) == float:
            if pd.isnull(input_value):
                self.dict_for_clean[column_name] = 0.0
            else:
                self.dict_for_clean[column_name] = float(input_value)
        else:
            self.dict_for_clean[column_name] = 0.0
            self._error_incident('_numeric_handler')

    def _text_handler(self, column_name):
        self.dict_for_clean[column_name] = "'" + str(self.dict_for_clean[column_name]) + "'"

    def _error_incident(self, text):
        print('error in ' + text)

    def clean_rows(self):
        # sequentially iterate through the dictionary keys (columns names)
        for column in list(self.dict_for_clean)[1:]:
            # for each column, we extract the data format from the configuratuon file
            format = self.columns[column]['format'].split(' ')
            # skip first row with table name
            if format[0] == self.table_name:
                continue
            # we call the corresponding function for each format, to correct values
            self.function_dict[format[0]](column)
        return self.dict_for_clean


class PandasFormat(Cleaner):


    def __init__(self, table_name = None, frame=None):
        super().__init__(table_name)
        self.frame = frame
        self.function_dict = {
            'boolean' : self._to_bool,
            'date' : self._to_date,
            'integer' : self._to_int,
            'numeric' : self._to_float,
            'text' : self._to_str,
        }

    def _to_str(self, parametrs=None):
        print('start _to_str PandasFormat')
        return self.frame.fillna('')

    def _to_int(self, parametrs=None):
        print('start _to_int PandasFormat')
        float_df = pd.DataFrame()
        float_df[list(self.frame)[0]] = pd.to_numeric(
            self.frame[list(self.frame)[0]],
            errors='coerce'
        ).fillna(0)
        return float_df.astype('int')

    def _to_float(self, parametrs=None):
        print('start _to_float PandasFormat')
        float_df = pd.DataFrame()
        float_df[list(self.frame)[0]] = pd.to_numeric(
            self.frame[list(self.frame)[0]],
            errors='coerce'
        ).fillna(0)
        return  float_df.astype('float')

    def _to_date(self, parametrs=0):
        print('start _to_date PandasFormat')
        date_df = pd.DataFrame()
        date_df[list(self.frame)[0]] = pd.to_datetime(
            self.frame[list(self.frame)[0]],
            #ускоряет процедуру обработки
            #infer_datetime_format=True,
            errors='coerce',
        #записывает данные в формате даты parametr = '19990101' если Nat
        ).fillna(pd.Timestamp(parametrs))
        return date_df

    def _to_bool(self, parametrs=None):
        print('start _to_bool PandasFormat')
        return self.frame.astype('boolean').fillna(False)

    def format_corrector(self):
        return  self.function_dict[
            self.format(
                list(self.frame)[0]
            )
        ]()


class ValueCleaner(Cleaner):

    def __init__(self, list_to_clean, table_name, column):
        super().__init__(table_name)
        self.list_to_clean = list_to_clean
        self.column = column
        self.function_dict = {
            'boolean' : self._boolean_handler,
            'date' : self._date_handler,
            'integer' : self._integer_handler,
            'numeric' : self._numeric_handler,
            'text' : self._text_handler,
        }
        self.true_list = [
            'да',
        ]

    def _boolean_handler(self):
        print('_boolean_handler')
        new_list = []
        for value in self.list_to_clean:
            if value in self.true_list:
                new_list.append(True)
            else:
                new_list.append(True)
        self.list_to_clean = new_list

    def _date_handler(self):
        print('_date_handler')
        print(self.list_to_clean)

    def _integer_handler(self):
        print('_integer_handler')

    def _numeric_handler(self):
        '''
        '''
        new_list = []
        for value in self.list_to_clean:
            try:
                value = float(value)
            except:
                value = 0.0
            new_list.append(float(value))
        self.list_to_clean = new_list
        print('_numeric_handler')

    def _text_handler(self):
        print('_text_handler')

    def columns_cleaner(self):
        print(self.column)
        format = self.columns[self.column]['format'].split(' ')
        self.function_dict[format[0]]()
        return self.list_to_clean


class StructureCleaner(Cleaner):

    def __init__(self, table_name = None, inframe=None):
        super().__init__(table_name)
        self.frame = inframe        
        self.function_dict = {
            'budget_commitment' : self._budget_commitment,
            'purchases' : self._purchases,
            'deals' : self._deals,
            'payments_short' : self._payments,
            'payments_full' : self._payments,
        }

    def _deals(self):
        return self.frame

    def _payments(self):
        return self.frame

    def _purchases(self):
        print('start Structure corrector _purchases') 
        clean_frame = pd.DataFrame()   
        clean_frame[self.id_sql] = [
            int(value[23:26:]) for value in self.frame['order_number']
        ]
        self.frame[self.id_sql] = PandasFormat(
            self.table_name,
            clean_frame,
        ).format_corrector()     
        return self.frame

    def _budget_commitment(self):
        print('start corrector')
        for column in ['year', 'reg_number', 'reg_number_full']:
            clean_frame = pd.DataFrame() 
            for object in self.frame['contract_identificator']:                    
                clean_frame = clean_frame.append(
                    {
                        column : self.find_reg_number(object)[column]
                    }, ignore_index=True
                )
            self.frame[column] = PandasFormat(
                self.table_name,
                clean_frame,
            ).format_corrector()
        self.frame.to_excel('ihj.xlsx')
        return self.frame
    
    def find_reg_number(self, object):
        #print('start find_reg_number')
        extra_dict = {
            'year' : 0,
            'reg_number' : 0,
            'reg_number_full' : '0/0',
        }
        object = object.replace("'", "")
        work_list = object.split('/')
        work_len = len(work_list)
        if work_len > 1:
            num_list = work_list[work_len-2].split(' ')
            num_len = len(num_list)
            try:
                #print('work_list', work_list[work_len-1], 'num_list', num_list[num_len-1])
                extra_dict['year'] = int(work_list[work_len-1])+2000
                extra_dict['reg_number'] = int(num_list[num_len-1])
                extra_dict['reg_number_full'] = str(
                    '{0}/{1}'.format(
                        extra_dict['reg_number'],
                        extra_dict['year']-2000,
                    )
                )
                return extra_dict
            except:
                #print(num_list)
                print('!!!! !!!!\nDescription.corrector except incident\n!!!!!')
                return extra_dict
        else:
            return extra_dict

    def none_row_deleter(self):
        self.frame = self.frame[(frame.one > 0)|(frame.two > 0)|(frame.three > 0)]

    def structure_corrector(self):
        return self.function_dict[self.table_name]()

