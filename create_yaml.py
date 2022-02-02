import yaml
from pathlib import *
import logging


config_file = {
    'table' : Path('configuration/table.yaml'),
    'config' : Path('configuration/config.yaml'),
}
# Declare a python object with data
table = {
    'deals' : {
        'table_name' : 'deals',
        'file_format':'excel',
        'work_sheet' : 'Договоры',
        'file_path' : 'external_data_source/План-закупок .xlsm',
        'excel_rows' : ['81', '303'],
        'budget_year':'',
        'columns' : {
            'reg_number_full' : {
                'format' : 'text PRIMARY KEY',
                'SQL' : True,
                'excel' : [False, ['', ''],],
                'links': ['budget_commitment', 'payments_full', 'payments_short', 'budget_commitment']
            },            
            'reg_number' : {
                'format' : 'integer',
                'SQL' : True,
                'excel' : [True, ['B81', 'B303'],],
                'links': [],
            },
            'contract_number' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['C81','C303'],],
                'links': [],
            },
            'subject' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['D81','D303'],],
                'links': [],
            },
            'partner' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['H81','H303'],],
                'links': [],
            },
            'inn' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [False, ['81','303'],],
                'links': ['payments_full', 'payments_short',],
            },
            'contract_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [True, ['I81','I303'],],
                'links': [],
            },
            'state_register_number' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['J81','J303'],],
                'links': ['plan',],
            },
            'kbk' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['K81','K303'],],
                'links': [],
            },
            'period' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['L81','L303'],],
                'links': [],
            },
            'detalisation' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['M81','M303'],],
                'links': ['payments_full', 'payments_short',],
            },
            'amount' : {
                'format' : 'numeric',
                'SQL' : True,
                'excel' : [True, ['N81','N303'],],
                'links': [],
            },
            'purchase_procedure' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['O81','O303'],],
                'links': [],
            },
            'responsible' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['P81','P303'],],
                'links': [],
            },
            'mark_perfomance' : {
                'format' : 'boolean',
                'SQL' : True,
                'excel' : [True, ['Q81','Q303'],],
                'links': [],
            },
            'execution_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [True, ['S81','S303'],],
                'links': [],
            },
            'commitment_number' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [False, ['81','303'],],
                'links': [],
            },
            'total_amount' :{
                'format' : 'numeric',
                'SQL' : True,
                'excel' : [False, ['81','303'],],
                'links': [],
            },
            'budget_year' :{
                'format' : 'integer',
                'SQL' : True,
                'excel' : [False, ['81','303'],],
                'links': [],
            },
            'ikz': {                
                'format':'text',
                'SQL':True,
                'excel' : [False, ['',''],],
                'links': ['plan',],
            },
            'change_number': {
                'format' : 'text',
                'SQL' : True,
                'excel' : [False, ['', ''],],
                'links': [],
            },
            'paid_for':{
                'format' : 'numeric',
                'SQL' : True,
                'excel' : [False, ['',''],],
                'links': [],
            },            
        },
    },
    'purchases' : {
        'table_name' : 'purchases',
        'file_format':'excel',
        'work_sheet' : 'Page 1',
        'file_path' : 'external_data_source/print form.xlsx',
        'excel_rows' : ['22', '206'],
        'budget_year':'',
        'columns' : {
            'order_number' : {
                'format' : 'text PRIMARY KEY',
                'SQL' : True,
                'excel' : [True, ['B22', 'B206'],],
                'links': [],
            },
            'okpd' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['C22', 'C206'],],
                'links': [],
            },
            'subject' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['F22', 'F206'],],
                'links': [],
            },
            'budget_year' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['H22', 'H206'],],
                'links': [],
            },
            'amount' : {
                'format' : 'numeric',
                'SQL' : True,
                'excel' : [True, ['J22', 'J206'],],
                'links': [],
            },
            'state_register_number' : {
                'format' : 'integer',
                'SQL' : True,
                'excel' : [False, ['', ''],],
                'links': [],
            },
            'reg_number' : {
                'format' : 'integer',
                'SQL' : True,
                'excel' : [False, ['', ''],],
                'links': [],
            },
        },
    },
    'budget_commitment' : {
        'table_name' : 'budget_commitment',
        'file_format':'excel',
        'work_sheet' : 'TDSheet',
        'file_path' : 'external_data_source/Обязательства.xlsx',
        'excel_rows' : ['5', '168'],
        'budget_year':'',
        'columns' : {
            'reg_number_full' : {
                'format' : 'text PRIMARY KEY',
                'SQL' : True,
                'excel' : [False, ['',''],],
                'links': [],
            },
            'reg_number' : {
                'format' : 'integer',
                'SQL' : True,
                'excel' : [False, ['',''],],
                'links': [],
            },
            'contract_identificator' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['A5', 'A168'],],
                'links': [],
            },
            'contract_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [True, ['B5', 'B168'],],
                'links': [],
            },
            'contract_number' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['C5', 'C168'],],
                'links': [],
            },
            'subject' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['D5', 'D168'],],
                'links': [],
            },
            'notification_number' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['E5', 'E168'],],
                'links': [],
            },
            'notification_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [True, ['F5', 'F168'],],
                'links': [],
            },
            'notification_amount' : {
                'format' : 'numeric',
                'SQL' : True,
                'excel' : [True, ['G5', 'G168'],],
                'links': [],
            },
            'commitment_number' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['H5', 'H168'],],
                'links': [],
            },
            'commitment_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [False, ['', ''],],
                'links': [],
            },
            'execution_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [True, ['I5', 'I168'],],
                'links': [],
            },
            'amount' : {
                'format' : 'numeric',
                'SQL' : True,
                'excel' : [True, ['J5', 'J168'],],
                'links': [],
            },
            'advance_value' : {
                'format' : 'numeric',
                'SQL' : True,
                'excel' : [True, ['K5', 'K168'],],
                'links': [],
            },
            'advance_amount' : {
                'format' : 'numeric',
                'SQL' : True,
                'excel' : [True, ['L5', 'L168'],],
                'links': [],
            },
            'year' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [False, ['', ''],],
                'links': [],
            },
        },
    },
    'payments_short' : {
        'table_name' : 'payments_short',
        'file_format':'excel',
        'work_sheet' : 'TDSheet',
        'file_path' : 'external_data_source/ЗКРсокращенная.xlsx',
        'excel_rows' : ['7', '696'],
        'budget_year':'',
        'columns' : {
            'order_number' : {
                'format' : 'text PRIMARY KEY',
                'SQL' : True,
                'excel' : [True, ['A7', 'A1070'],],
                'links': [],
            },
            'order_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [True, ['C7', 'C1070'],],
                'links': [],
            },
            'subjekt' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['D7', 'D1070'],],
                'links': [],
            },
            'detalisation' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['F7', 'F1070'],],
                'links': [],
            },
            'csr' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['G7', 'G1070'],],
                'links': [],
            },
            'kosgu' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['H7', 'H1070'],],
                'links': [],
            },
            'kbk' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [False, ['',''],],
                'links': [],
            },
            'contract_identificator' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['I7', 'I1070'],],
                'links': [],
            },
            'reg_number' : {
                'format' : 'integer',
                'SQL' : True,
                'excel' : [False, ['',''],],
                'links': [],
            },
            'reg_number_full' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [False, ['I7', 'I1070'],],
                'links': [],
            },
            'partner_inn' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['J7', 'J1070'],],
                'links': [],
            },
            'partner' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [False, ['',''],],
                'links': [],
            },
            'inn' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [False, ['',''],],
                'links': [],
            },
            'payment_order_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [True, ['K7', 'K1070'],],
                'links': [],
            },
            'payment_order_number' : {
                'format' : 'integer',
                'SQL' : True,
                'excel' : [True, ['L7', 'L1070'],],
                'links': [],
            },
            'primary_doc_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [True, ['M7', 'M1070'],],
                'links': [],
            },
            'primary_doc_number' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['N7', 'N1070'],],
                'links': [],
            },
            'primary_doc_form' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['O7', 'O1070'],],
                'links': [],
            },
            'amount' : {
                'format' : 'numeric',
                'SQL' : True,
                'excel' : [True, ['Q7', 'Q1070'],],
                'links': [],
            },
            'year' : {
                'format' : 'integer',
                'SQL' : True,
                'excel' : [False, ['', ''],],
                'links': [],
            },
        },
    },
    'payments_full' : {
        'table_name' : 'payments_full',
        'file_format':'excel',
        'work_sheet' : 'TDSheet',
        'file_path' : 'external_data_source/ЗКРполная.xlsx',
        'excel_rows' : ['7', '916'],
        'budget_year':'',
        'columns' : {
            'order_number' : {
                'format' : 'text PRIMARY KEY',
                'SQL' : True,
                'excel' : [True, ['A7', 'A1372'],],
                'links': [],
            },
            'order_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [True, ['C7', 'C1372'],],
                'links': [],
            },
            'subjekt' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['D7', 'D1372'],],
                'links': [],
            },
            'detalisation' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['F7', 'F1372'],],
                'links': [],
            },
            'csr' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['G7', 'G1372'],],
                'links': [],
            },
            'kosgu' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['H7', 'H1372'],],
                'links': [],
            },
            'kbk' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [False, ['',''],],
                'links': [],
            },
            'contract_identificator' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['I7', 'I1372'],],
                'links': [],
            },
            'reg_number' : {
                'format' : 'integer',
                'SQL' : True,
                'excel' : [False, ['',''],],
                'links': [],
            },
            'reg_number_full' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [False, ['I7', 'I1372'],],
                'links': [],
            },
            'partner_inn' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['J7', 'J1372'],],
                'links': [],
            },
            'partner' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [False, ['',''],],
                'links': [],
            },
            'inn' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [False, ['',''],],
                'links': [],
            },
            'payment_order_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [True, ['K7', 'K1372'],],
                'links': [],
            },
            'payment_order_number' : {
                'format' : 'integer',
                'SQL' : True,
                'excel' : [True, ['L7', 'L1372'],],
                'links': [],
            },
            'primary_doc_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [True, ['M7', 'M1372'],],
                'links': [],
            },
            'primary_doc_number' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['N7', 'N1372'],],
                'links': [],
            },
            'primary_doc_form' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['O7', 'O1372'],],
                'links': [],
            },
            'amount' : {
                'format' : 'numeric',
                'SQL' : True,
                'excel' : [True, ['Q7', 'Q1372'],],
                'links': [],
            },
            'year' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [False, ['', ''],],
                'links': [],
            },
        },
    },
    'commitment_treasury' : {
        'table_name' : 'commitment_treasury',
        'file_format':'excel',
        'work_sheet' : 'Scroller',
        'file_path' : 'external_data_source/Обязательства_ЭБ.xlsx',
        'excel_rows' : ['5', '2000'],
        'budget_year':'',
        'columns' : {
            'status' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['E5', 'E2000'],],
                'links': [],
            },
            'doc_number' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['F5', 'F2000'],],
                'links': [],
            },
            'doc_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [True, ['G5', 'G2000'],],
                'links': [],
            },
            'change_number' : {
                'format' : 'integer',
                'SQL' : True,
                'excel' : [True, ['H5', 'H2000'],],
                'links': [],
            },
            'type' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['I5', 'I2000'],],
                'links': [],
            },
            'basic_doc_name' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['K5', 'K2000'],],
                'links': [],
            },
            'basic_doc_number' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['L5', 'L2000'],],
                'links': [],
            },
            'execution_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [True, ['M5', 'M2000'],],
                'links': [],
            },
            'amount' : {
                'format' : 'numeric',
                'SQL' : True,
                'excel' : [True, ['N5', 'N2000'],],
                'links': [],
            },
            'commitment_number' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['P5', 'P2000'],],
                'links': [],
            },
            'subject' : {
                'format' : 'text',
                'SQL' : True,
                'excel' : [True, ['S5', 'S2000'],],
                'links': [],
            },
            'registration_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [True, ['T5', 'T2000'],],
                'links': [],
            },
            'commitment_date' : {
                'format' : 'date',
                'SQL' : True,
                'excel' : [True, ['V5', 'V2000'],],
                'links': [],
            },
            'index' : {
                'format' : 'text PRIMARY KEY',
                'SQL' : True,
                'excel' : [False, ['', ''],],
                'links': [],
            },
        },
    },
    'plan' : {
        'table_name':'plan',
        'file_format':'xml',
        'file_path':'external_data_source/result.xml',
        'base_tag':'{http://zakupki.gov.ru/oos/TPtypes/1}position',
        'budget_year':'.//{http://zakupki.gov.ru/oos/TPtypes/1}planYear',
        'columns':{
            'state_register_number':{
                'format' : 'text',
                'SQL' : True,
                'xml' : [
                    True,
                    './/{http://zakupki.gov.ru/oos/TPtypes/1}purchaseNumber',
                ],
                'links': [],
            },
            'ikz':{
                'format':'text PRIMARY KEY',
                'SQL':True,
                'xml':[
                    True,
                    './/{http://zakupki.gov.ru/oos/TPtypes/1}IKZ',
                ],
                'links': [],
            },
            'subject':{
                'format' : 'text',
                'SQL' : True,
                'xml' : [
                    True,
                    './/{http://zakupki.gov.ru/oos/TPtypes/1}purchaseObjectInfo',
                ],
                'links': [],
            },
            'amount':{
                'format' : 'numeric',
                'SQL' : True,
                'xml' : [
                    True,
                    './/{http://zakupki.gov.ru/oos/TPtypes/1}currentYear',
                ],
                'links': [],
            },
            'amount_next_year':{
                'format' : 'numeric',
                'SQL' : True,
                'xml' : [
                    True,
                    './/{http://zakupki.gov.ru/oos/TPtypes/1}firstYear',
                ],
                'links': [],
            },
            'publish_year':{
                'format' : 'numeric',
                'SQL' : True,
                'xml' : [
                    True,
                    './/{http://zakupki.gov.ru/oos/TPtypes/1}publishYear',
                ],
                'links': [],
            },
            'okpd':{
                'format' : 'text',
                'SQL' : True,
                'xml' : [
                    True,
                    './/{http://zakupki.gov.ru/oos/base/1}OKPDCode',
                ],
                'links': [],
            },
            'budget_year':{
                'format' : 'numeric',
                'SQL' : True,
                'xml' : [
                    None,
                    './/{http://zakupki.gov.ru/oos/base/1}OKPDCode',
                ],
                'links': [],
            },
        },
    },
    'internal_plan': {
        'table_name':'internal_plan',
        'file_format':'docx',
        'file_path':'external_data_source/Легендарная таблица .docx',        
        'budget_year':'',
        'columns':{
            'ikz':{
                'format': 'text PRIMARY KEY',
                'SQL': True,
                'word': [True, ['ИКЗ'],],
                'links': [],
            },
            'number':{
                'format': 'text',
                'SQL': True,
                'word':[True, ['№'],],
                'links': [],
            },
            'subject':{
                'format': 'text',
                'SQL': True,
                'word':[True, ['Наименование закупки'],],
                'links': [],
            },
            'okpd':{
                'format': 'text',
                'SQL': True,
                'word':[True, ['ОКПД'],],
                'links': [],
            },
            'limitations':{
                'format': 'text',
                'SQL': True,
                'word':[True, ['ограничен.'],],
                'links': [],
            },
            'method':{
                'format': 'text',
                'SQL': True,
                'word':[True, ['Способ'],],
                'links': [],
            },
            'contract_year':{
                'format': 'text',
                'SQL': True,
                'word':[True, ['год нач.'],],
                'links': [],
            },
            'execution_date':{
                'format': 'date',
                'SQL': True,
                'word':[True, ['поставка'],],
                'links': [],
            },
            'responsible':{
                'format': 'text',
                'SQL': True,
                'word':[True, ['Ответст'],],
                'links': [],
            },
            'law':{
                'format': 'text',
                'SQL': True,
                'word':[True, ['НПА'],],
                'links': [],
            },
            'amount_old':{
                'format': 'numeric',
                'SQL': True,
                'word':[True, ['НМЦК ст'],],
                'links': [],
            },
            'amount_new':{
                'format': 'numeric',
                'SQL': True,
                'word':[True, ['НМЦК нов'],],
                'links': [],
            },
            'amount':{
                'format': 'numeric',
                'SQL': True,
                'word':[True, ['2022 ЦК'],],
                'links': [],
            },
            'contract_number':{
                'format': 'text',
                'SQL': True,
                'word':[True, ['№ контракта'],],
                'links': [],
            },
            'partner':{
                'format': 'text',
                'SQL': True,
                'word':[True, ['Исполнитель'],],
                'links': [],
            },
            'next_year':{
                'format': 'numeric',
                'SQL': True,
                'word':[True, ['2023'],],
                'links': [],
            },
            'next_next_year':{
                'format': 'numeric',
                'SQL': True,
                'word':[True, ['2024'],],
                'links': [],
            },
            'status':{
                'format': 'text',
                'SQL': True,
                'word':[True, ['Стадия'],],
                'links': [],
            },
            'kbk':{
                'format': 'text',
                'SQL': True,
                'word':[False, [''],],
                'links': [],
            },
            'detalisation':{
                'format': 'text',
                'SQL': True,
                'word':[False, [''],],
                'links': [],
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