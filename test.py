import pandas as pd
from sqlorm import cleaner


data_dict = [
    {'subject' : ['string', 1, 2.3,'2021.01.03', True, None, []],},
    {'reg_number' : [1, 2, 3, 3.3, '4', '4.4', True, None, [], '2021.02.05'],},
    {'amount' : [1.2, 3.2, 3.3, 1, '1', 'vasy', None,  '2021.02.05'],},
    {'contract_date' : ['2021.01.03', '2021/01/02','2021-02-05', '15/03/21', '15.01.99', '12-03-21',None, 3.2, 'vasy', True, 1, 2, '3', 0, -2],},
    {'mark_perfomance' : [True, False, None],},
]


def start():
    for data in data_dict:
        for key, value in data.items():
            print(data)
            frame = pd.DataFrame(value, columns=[key])
            print('ORIGINAL FRAME')
            print(frame)
            print(frame.dtypes, '\n')
           
            newframe = cleaner.PandasFormat('deals', frame).format_corrector()
            #newframe = function_dict[key](frame, parametrs)
            print('CORRECT FRAME')
            print(newframe)
            print(newframe.dtypes, '\n\n')


start()