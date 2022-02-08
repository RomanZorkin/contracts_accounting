import operator

from sqlalchemy import ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, Date, Float, Boolean
from sqlalchemy.orm import sessionmaker

import configparser
from pathlib import Path

import api.sql_scheme as sql_scheme

config_file = Path('config.ini')
config = configparser.ConfigParser()
config.read(config_file)

DB_CONFIGURATION: str = config['DB_connection']['alchemy_rule']

engine = create_engine(DB_CONFIGURATION, echo=True)
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

def notifications():
    """
    запрос для получения списка извещений. выборка извещений делается по условию:
    сичтаем что извещение существует в случае если позиция из плана графика
    по номеру икз существует в позиции выгрузки из казначейства бюджетных обязательств
    по столбцу номер документа основания (при формировании извещений туда попадает номер икз)
    далее проверяем не заключен ли по данному икз договор если нет то включаем строку в ответ
    """
    from sqlalchemy.sql import func, cast, and_
    Session = sessionmaker(bind=engine)
    session = Session()
    subquery = session.query(sql_scheme.Deals.state_register_number).filter(
        # func.substr обрезает стоку по указанному правилу
        func.substr(sql_scheme.Plan.ikz, 1, 2) == func.substr(
            # .cast() преобразует формат ячейки
            sql_scheme.Deals.budget_year.cast(Text), 3, 4
        )
    )
    curent_query = session.query(
        sql_scheme.Plan.ikz,
        # .label() усанавливает имя для колонки
        func.substr(sql_scheme.Plan.state_register_number, 2, 4).label('ikz_short'),
        sql_scheme.Plan.amount,
        sql_scheme.Internal_plan.kbk,
        sql_scheme.Internal_plan.detalisation,
        sql_scheme.Commitment_treasury.doc_date,
        sql_scheme.Plan.subject,
    ).join(
        # отбор по таблице Commitment_treasury, в ней по столбцу номер документа основания (basic_doc_number)
        # обязательно должен быть номер ИКЗ, это признак опубликованного извещения
        sql_scheme.Commitment_treasury,
        sql_scheme.Plan.ikz == func.substr(
            sql_scheme.Commitment_treasury.basic_doc_number, 1, 26
        )
    ).join(
        # отбор по таблице Internal_plan, в данном случае нас интересуют кбк и детализация,
        # в случае отсутсвия позиции в данной таблицы, строку все равно включаем в запрос (left join)
        sql_scheme.Internal_plan,
        # isouter= True - условие left join запроса
        sql_scheme.Plan.ikz == sql_scheme.Internal_plan.ikz, isouter=True
    ).filter(and_(
        #sql_scheme.Plan.budget_year == 2021,
        sql_scheme.Plan.amount > 0,
        # короткий номер икз не должен быть по соответствующему году в таблтце договоро
        func.substr(
            sql_scheme.Plan.state_register_number, 2, 4
        ).not_in(subquery)
    ))

    answer = []
    for row in curent_query:
        # итерация по запросу возвращает картеж со значениями, 
        # в порядке установленном в запросе колонок
        answer_dict = {}
        position = 0
        # извлекаем названия колонко - возвращается список словарей ,
        # где по ключу name получаем название колокни
        for column_property in curent_query.column_descriptions:
            #print(row, column_property, column_property['name'], position)
            answer_dict[column_property['name']] = row[position]
            position += 1
        answer.append(answer_dict)
   
    print(answer)

    return answer

    """
    from sqlalchemy import inspect
    mapper = inspect(sql_scheme.Deals)
    for column in mapper.attrs:
        print(column.key)
    """