import yaml
from pathlib import *
import logging


config_file = {
    'table' : Path('configuration/table.yaml'),
    'config' : Path('configuration/config.yaml'),
}
# Declare a python object with data
table = {
    'tables' : [
        'deals',
        'purchases',
        'budget_commitment',
        'payments_short',
        'payments_full',
        'commitment_treasury',
    ],
    'deals' : {
        'table_name' : 'deals',
        'work_sheet' : 'Договоры',
        'excel_file' : 'external_data_source/План-закупок 2021.xlsm',
        'excel_rows' : ['81', '303'],
        'columns' : {
            'reg_number' : {
                'format' : 'integer PRIMARY KEY',
                'SQL' : True,
                'excel' : [True, ['B81', 'B303'],],
            },
            'contract_number' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['C81','C303'],],
            },
            'subject' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['D81','D303'],],
            },
            'partner' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['H81','H303'],],
            },
            'INN' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [False, ['81','303'],],
            },
            'contract_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [True, ['I81','I303'],],
            },
            'state_register_number' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['J81','J303'],],
            },
            'KBK' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['K81','K303'],],
            },
            'period' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['L81','L303'],],
            },
            'detalisation' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['M81','M303'],],
            },
            'amount' : {
                'format' : 'numeric',
                'SQL' : True,
                'excel' : [True, ['N81','N303'],],
            },
            'purchase_procedure' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['O81','O303'],],
            },
            'responsible' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['P81','P303'],],
            },
            'mark_perfomance' : {
                'format' : 'boolean',
                'SQL' : True,
                'excel' : [True, ['Q81','Q303'],],
            },
            'execution_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [True, ['S81','S303'],],
            },
            'budget_commitment' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [False, ['81','303'],],
            },
            'total_amount' :{
                'format' : 'numeric',
                'SQL' : True,
                'excel' : [False, ['81','303'],],
            },
            'buget_year' :{
                'format' : 'text',
                'SQL' : True,
                'excel' : [False, ['81','303'],],
            },
        },
    },
    'purchases' : {
        'table_name' : 'purchases',
        'work_sheet' : 'Page 1',
        'excel_file' : 'external_data_source/print form.xlsx',
        'excel_rows' : ['22', '206'],
        'columns' : {
            'order_number' : {
                'format' : 'text PRIMARY KEY',
                'SQL' : True,
                'excel' : [True, ['B22', 'B206'],],
            },
            'OKPD' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['C22', 'C206'],],
            },
            'subject' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['F22', 'F206'],],
            },
            'budget_year' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['H22', 'H206'],],
            },
            'amount' : {
                'format' : 'numeric',
                'SQL' : True,
                'excel' : [True, ['J22', 'J206'],],
            },
            'state_register_number' : {
                'format' : 'integer',
                'SQL' : True,
                'excel' : [False, ['', ''],],
            },
            'reg_number' : {
                'format' : 'integer',
                'SQL' : True,
                'excel' : [False, ['', ''],],
            },
        },
    },
    'budget_commitment' : {
        'table_name' : 'budget_commitment',
        'work_sheet' : 'TDSheet',
        'excel_file' : 'external_data_source/Обязательства.xlsx',
        'excel_rows' : ['5', '168'],
        'columns' : {
            'reg_number_full' : {
                'format' : 'text PRIMARY KEY',                
                'SQL' : True,
                'excel' : [False, ['',''],],
            },
            'reg_number' : {
                'format' : 'integer',
                'SQL' : True,
                'excel' : [False, ['',''],],
            },
            'contract_identificator' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['A5', 'A168'],],                
            },
            'contract_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [True, ['B5', 'B168'],],
            },
            'contract_number' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['C5', 'C168'],],
            },
            'subject' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['D5', 'D168'],],
            },
            'notification_number' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['E5', 'E168'],],
            },
            'notification_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [True, ['F5', 'F168'],],
            },
            'notification_amount' : {
                'format' : 'numeric',
                'SQL' : True,
                'excel' : [True, ['G5', 'G168'],],
            },
            'commitment_number' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['H5', 'H168'],],
            },
            'commitment_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [False, ['', ''],],
            },
            'execution_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [True, ['I5', 'I168'],],
            },
            'amount' : {
                'format' : 'numeric',
                'SQL' : True,
                'excel' : [True, ['J5', 'J168'],],
            },
            'advance_value' : {
                'format' : 'numeric',
                'SQL' : True,
                'excel' : [True, ['K5', 'K168'],],
            },
            'advance_amount' : {
                'format' : 'numeric',
                'SQL' : True,
                'excel' : [True, ['L5', 'L168'],],
            },
            'year' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [False, ['', ''],],
            },
        },
    },
    'payments_short' : {
        'table_name' : 'payments_short',
        'work_sheet' : 'TDSheet',
        'excel_file' : 'external_data_source/ЗКРсокращенная.xlsx',
        'excel_rows' : ['7', '696'],
        'columns' : {
            'order_number' : {
                'format' : 'integer PRIMARY KEY',
                'SQL' : True,
                'excel' : [True, ['A7', 'A696'],],
            },
            'order_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [True, ['C7', 'C696'],],
            },
            'subjekt' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['D7', 'D696'],],
            },
            'detalization' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['F7', 'F696'],],
            },
            'CSR' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['G7', 'G696'],],
            },
            'KOSGU' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['H7', 'H696'],],
            },
            'KBK' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [False, ['',''],],
            },
            'contract_identificator' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['I7', 'I696'],],
            },
            'reg_number' : {
                'format' : 'integer',
                'SQL' : True,
                'excel' : [False, ['',''],],
            },
            'reg_number_full' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [False, ['I7', 'I696'],],
            },
            'partner_INN' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['J7', 'J696'],],
            },
            'partner' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [False, ['',''],],
            },
            'INN' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [False, ['',''],],
            },
            'payment_order_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [True, ['K7', 'K696'],],
            },
            'payment_order_number' : {
                'format' : 'integer',
                'SQL' : True,
                'excel' : [True, ['L7', 'L696'],],
            },
            'primary_doc_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [True, ['M7', 'M696'],],
            },
            'primary_doc_number' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['N7', 'N696'],],
            },
            'primary_doc_form' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['O7', 'O696'],],
            },
            'amount' : {
                'format' : 'numeric',
                'SQL' : True,
                'excel' : [True, ['Q7', 'Q696'],],
            },
            'year' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [False, ['', ''],],
            },
        },
    },
    'payments_full' : {
        'table_name' : 'payments_full',
        'work_sheet' : 'TDSheet',
        'excel_file' : 'external_data_source/ЗКРполная.xlsx',
        'excel_rows' : ['7', '916'],
        'columns' : {
            'order_number' : {
                'format' : 'integer PRIMARY KEY',
                'SQL' : True,
                'excel' : [True, ['A7', 'A916'],],
            },
            'order_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [True, ['C7', 'C916'],],
            },
            'subjekt' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['D7', 'D916'],],
            },
            'detalization' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['F7', 'F916'],],
            },
            'CSR' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['G7', 'G916'],],
            },
            'KOSGU' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['H7', 'H916'],],
            },
            'KBK' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [False, ['',''],],
            },
            'contract_identificator' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['I7', 'I916'],],
            },
            'reg_number' : {
                'format' : 'integer',
                'SQL' : True,
                'excel' : [False, ['',''],],
            },
            'reg_number_full' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [False, ['I7', 'I916'],],
            },
            'partner_INN' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['J7', 'J916'],],
            },
            'partner' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [False, ['',''],],
            },
            'INN' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [False, ['',''],],
            },
            'payment_order_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [True, ['K7', 'K916'],],
            },
            'payment_order_number' : {
                'format' : 'integer',
                'SQL' : True,
                'excel' : [True, ['L7', 'L916'],],
            },
            'primary_doc_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [True, ['M7', 'M916'],],
            },
            'primary_doc_number' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['N7', 'N916'],],
            },
            'primary_doc_form' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['O7', 'O916'],],
            },
            'amount' : {
                'format' : 'numeric',
                'SQL' : True,
                'excel' : [True, ['Q7', 'Q916'],],
            },
            'year' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [False, ['', ''],],
            },
        },
    },
    'commitment_treasury' : {
        'table_name' : 'deals',
        'work_sheet' : 'Scroller',
        'excel_file' : 'external_data_source/Обязательства_ЭБ.xlsx',
        'excel_rows' : ['5', '2000'],
        'columns' : {
            'status' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['E5', 'E2000'],],
            },
            'doc_number' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['F5', 'F2000'],],
            },
            'doc_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [True, ['G5', 'G2000'],],
            },
            'change_number' : {
                'format' : 'integer',
                'SQL' : True,
                'excel' : [True, ['H5', 'H2000'],],
            },
            'type' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['I5', 'I2000'],],
            },
            'basic_doc_name' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['K5', 'K2000'],],
            },
            'basic_doc_number' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['L5', 'L2000'],],
            },
            'execution_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [True, ['M5', 'M2000'],],
            },
            'amount' : {
                'format' : 'numeric',
                'SQL' : True,
                'excel' : [True, ['N5', 'N2000'],],
            },
            'commitment_number' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['P5', 'P2000'],],
            },
            'subject' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['S5', 'S2000'],],
            },
            'registration_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [True, ['T5', 'T2000'],],
            },
            'commitment_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [True, ['V5', 'V2000'],],
            },
            'index' : {
                'format' : 'text PRIMARY KEY',
                'SQL' : True,
                'excel' : [False, ['', ''],],
            },           
        },
    },
}

config = {
    'year' : 2021,
    'scans' : 'D:/Работа/Войсковая часть 68895/Договоры/Договоры',
}
# Convert and print the JSON data in YAML stream
#print(yaml.dump(table))

def insert_structure(yaml_dict, yaml_dir):
    with open(yaml_dir, 'w') as f:
        yaml.dump(yaml_dict, f, default_flow_style=False)

def chenge_range_row():
    with open(config_file) as fh:
        config = yaml.load(fh, Loader=yaml.FullLoader)
    head = ['81' , '81',]
    floor = ['303', '258',]
    columns = list(config['deals']['columns'])
    for column in columns:
        scheme = config['deals']['columns'][column]
        scheme['excel'][1][0] = scheme['excel'][1][0].replace(head[0], head[1])
        scheme['excel'][1][1] = scheme['excel'][1][1].replace(floor[0], floor[1])
    with open(config_file, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)


insert_structure(table, config_file['table'])

#chenge_range_row()

# Load YAML data from the file
# Print YAML data before sorting
# Sort YAML data based on keys
#sorted_data = yaml.dump(read_data)
# Print YAML data after sorting
#print(sorted_data)