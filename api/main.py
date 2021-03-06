import operator

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

DB_CONFIGURATION: str = 'postgres://owner:1111@localhost:5432/contracts'

engine = create_engine('postgresql://owner:1111@localhost:5432/contracts', echo=True)
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
        #answer['pays'] = sorted(
        #    answer['pays'],
        #    key=operator.itemgetter('date'),
        #)
        answer['pay_summ'] = pay_sum
        
    session.commit()
    return answer

def all(number):
    Session = sessionmaker(bind=engine)
    session = Session()
    query = session.query(sql_scheme.Deals).filter(sql_scheme.Deals.reg_number_full == number)
    print(query.all()[0].subject)
    session.commit()


def filter_all():
    print('start')
    Session = sessionmaker(bind=engine)
    session = Session()
    big_answer = []
    for row in session.query(sql_scheme.Deals):
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
        }
        big_answer.append(answer)
        print(answer)
    session.commit()
    return big_answer

#filter_all()