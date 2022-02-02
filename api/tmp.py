from sqlalchemy import ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, Date, Float, Boolean
from sqlalchemy.orm import sessionmaker

import configparser
from pathlib import Path
import sql_scheme

config_file = Path('config.ini')
config = configparser.ConfigParser()
config.read(config_file)


engine = create_engine('postgresql://owner:1111@localhost:5432/contracts', echo=False)
Base = declarative_base()


def filter_query(number: str) -> str:
    Session = sessionmaker(bind=engine)
    session = Session()
    
    for row in session.query(sql_scheme.Deals).filter(sql_scheme.Deals.reg_number_full == number):
        #print(row.reg_number, row.subject)
        answer = {
            'reg_number_full': row.reg_number_full,
            'reg_number':row.reg_number,
            'contract_number':row.contract_number,
            'subject':row.subject,
            'partner':row.partner,
            'contract_date':row.contract_date,
            'state_register_number':row.state_register_number,
            'period':row.period,
            'detalisation':row.detalisation,
            'amount':row.amount,
            'purchase_procedure':row.purchase_procedure,
            'responsible':row.responsible,
            'mark_perfomance':row.mark_perfomance,
            'execution_date':row.execution_date,
            'budget_commitment':row.commitment_number,
            'total_amount':row.total_amount,
            'budget_year':row.budget_year,
            'inn':row.inn,
            'kbk':row.kbk,
            'pays': [],            
        }
        pay_sum = 0
        for second_row in row.pays_short:
            pay_sum = pay_sum + second_row.amount
            answer['pays'].append(
                {
                    'amount': second_row.amount,
                    'date': second_row.order_date,
                    'subject': second_row.subjekt,
                }
            )
        for third_row in row.pays_full:
            pay_sum = pay_sum + third_row.amount
            answer['pays'].append(
                {
                    'amount': third_row.amount,
                    'date': third_row.order_date,
                    'subject': third_row.subjekt,
                }
            )
        
        answer['pay_summ'] = pay_sum
        
    session.commit()
    return answer

def filter_all():
    print('start')
    Session = sessionmaker(bind=engine)
    session = Session()
    big_answer = {}
    
    #list_kbk =[]
    empty_kbk = {}
    epmty_list = []
    for row in session.query(sql_scheme.Plan):
        #print(row.reg_number, row.subject)
        #print(row)
        empty_kbk[row.ikz] = [None, row.subject]
        for second_row in row.int_plan:
            empty_kbk[row.ikz][0] = second_row.kbk
            #print(f'{second_row.kbk} {second_row.detalisation}')
            #print(list(big_answer))
            if f'{second_row.kbk} {second_row.detalisation}' in big_answer:              
                big_answer[f'{second_row.kbk} {second_row.detalisation}'] += row.amount                
            else:
                big_answer[f'{second_row.kbk} {second_row.detalisation}'] = row.amount
            #print(second_row.kbk)
    
    for ikz, kbk in empty_kbk.items():        
        if kbk[0] is None:
            epmty_list.append([ikz, kbk[1]])
    session.commit()
    print(big_answer)
    #print(empty_kbk)
    #print(epmty_list)

    #return big_answer

filter_all()
"""
g = '21122220195742222010010121000203000'
print(g[:26])
print(g[29:36])
d ='211222201957422220100101210002030000'
c = '{}'.format(
    g[:26],
    #'000',
    #g[29:36]
)
print(c, len(c))
print(d, len(d))
if g == d:
    print('ok')

"""