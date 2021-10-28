import pandas as pd
from sqlorm import cleaner
import logging

logging.basicConfig(filename='debug.log', level=logging.DEBUG)
#logging.basicConfig(filename='info.log', level=logging.INFO)

logging.debug('Start data_dict')
data_dict: list = [
    {'subject' : ['string', 1, 2.3,'2021.01.03', True, None, []],},
    {'reg_number' : [1, 2, 3, 3.3, '4', '4.4', True, None, [], '2021.02.05'],},
    {'amount' : [1.2, 3.2, 3.3, 1, '1', 'vasy', None,  '2021.02.05'],},
    {'contract_date' : ['2021.01.03', '2021/01/02','2021-02-05', '15/03/21', '15.01.99', '12-03-21',None, 3.2, 'vasy', True, 1, 2, '3', 0, -2],},
    {'mark_perfomance' : [True, False, None],},
]


def start(): 
    logging.info('start function "start"\n')
    for data in data_dict:
        logging.debug(f'start FOR data: {data}')
        for key, value in data.items():
            logging.debug(f'start FOR key: {key}, value: {value}')
            frame = pd.DataFrame(value, columns=[key])
            logging.debug(f'ORIGINAL FRAME\n{frame}\n{frame.dtypes}\n')

            newframe = cleaner.PandasFormat('deals', frame).format_corrector()     
            logging.debug(f'CORRECT FRAME\n{newframe}\n{newframe.dtypes}\n\n')
start()
