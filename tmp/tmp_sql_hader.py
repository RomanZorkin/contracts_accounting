import operator

import sqlalchemy
from sqlalchemy import ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, Date, Float, Boolean
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func, cast, and_

import configparser
from pathlib import Path

import api.sql_scheme as sql_scheme

config_file = Path('config.ini')
config = configparser.ConfigParser()
config.read(config_file)

DB_CONFIGURATION: str = config['DB_connection']['alchemy_rule']

engine = create_engine(DB_CONFIGURATION, echo=True)
Base = declarative_base()

def plan_graphic():
    """
    запрос для получения списка извещений. выборка извещений делается по условию:
    сичтаем что извещение существует в случае если позиция из плана графика
    по номеру икз существует в позиции выгрузки из казначейства бюджетных обязательств
    по столбцу номер документа основания (при формировании извещений туда попадает номер икз)
    далее проверяем не заключен ли по данному икз договор если нет то включаем строку в ответ
    """
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
        sql_scheme.Plan.amount,
        sql_scheme.Internal_plan.kbk,
        sql_scheme.Internal_plan.detalisation,
        sql_scheme.Plan.subject,
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
        func.substr(sql_scheme.Plan.ikz, 1, 2) == '22'
    )).order_by(sql_scheme.Internal_plan.kbk, sql_scheme.Internal_plan.detalisation)

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

def curent_contracts(budget_year=2022):
    """метод выводит текущие контракты с их исполнением
    """
    Session = sessionmaker(bind=engine)
    session = Session()
    curent_query = session.query(
        sql_scheme.Deals.kbk.label('kbk'),
        sql_scheme.Deals.detalisation.label('dfo'),
        sql_scheme.Deals.subject.label('predmet'),
        sql_scheme.Deals.purchase_procedure.label('procedur'),
        sql_scheme.Deals.amount.label('summa'),
        sql_scheme.Deals.reg_number_full.label('id_num'),
        sql_scheme.Deals.contract_date.label('start_date'),
        sql_scheme.Deals.execution_date.label('finish_date'),
        func.sum(sql_scheme.Payments_short.amount).label('pay_short'),
        func.sum(sql_scheme.Payments_full.amount).label('pay_full'),
        sqlalchemy.sql.literal_column('""').label('table'),
        # sqlalchemy.sql.null().label('c2'), 
    ).join(
        sql_scheme.Payments_short,
        # isouter= True - условие left join запроса
        sql_scheme.Deals.reg_number_full == sql_scheme.Payments_short.reg_number_full, isouter=True
    ).join(
        sql_scheme.Payments_full,
        # isouter= True - условие left join запроса
        sql_scheme.Deals.reg_number_full == sql_scheme.Payments_full.reg_number_full, isouter=True
    ).filter(and_(
        sql_scheme.Deals.budget_year == budget_year,
        sql_scheme.Deals.subject != 'None'
    )).group_by(
        sql_scheme.Deals.kbk,
        sql_scheme.Deals.detalisation,
        sql_scheme.Deals.subject,
        sql_scheme.Deals.purchase_procedure,
        sql_scheme.Deals.amount,
        sql_scheme.Deals.reg_number_full,
        sql_scheme.Deals.execution_date,
        sql_scheme.Deals.contract_date,
    )

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

    return answer

def union_plan_deals(budget_year=2022):
    """
    выводит совместную таблицу по заключенным контрактам и закупкма планируемым по ПГЗ
    """
    Session = sessionmaker(bind=engine)
    session = Session()
    deals_query = session.query(sql_scheme.Deals.kbk,
        sql_scheme.Deals.kbk.label('kbk'),
        sql_scheme.Deals.detalisation.label('dfo'),
        sql_scheme.Deals.subject.label('predmet'),
        sql_scheme.Deals.purchase_procedure.label('procedur'),
        sql_scheme.Deals.amount.label('summa'),
        sql_scheme.Deals.reg_number_full.label('id_num'),
        sql_scheme.Deals.contract_date.label('start_date'),
        sql_scheme.Deals.execution_date.label('finish_date'),
        func.sum(sql_scheme.Payments_short.amount).label('pay_short'),
        func.sum(sql_scheme.Payments_full.amount).label('pay_full'),
        sqlalchemy.sql.literal_column("'договоры'").label('table'),
        # sqlalchemy.sql.null().label('c2'), 
    ).join(
        sql_scheme.Payments_short,
        # isouter= True - условие left join запроса
        sql_scheme.Deals.reg_number_full == sql_scheme.Payments_short.reg_number_full, isouter=True
    ).join(
        sql_scheme.Payments_full,
        # isouter= True - условие left join запроса
        sql_scheme.Deals.reg_number_full == sql_scheme.Payments_full.reg_number_full, isouter=True
    ).filter(and_(
        sql_scheme.Deals.budget_year == budget_year,
        sql_scheme.Deals.subject != 'None'
    )).group_by(
        sql_scheme.Deals.kbk,
        sql_scheme.Deals.detalisation,
        sql_scheme.Deals.subject,
        sql_scheme.Deals.purchase_procedure,
        sql_scheme.Deals.amount,
        sql_scheme.Deals.reg_number_full,
        sql_scheme.Deals.contract_date,
        sql_scheme.Deals.execution_date,        
    ).order_by(
        sql_scheme.Deals.kbk,
        sql_scheme.Deals.detalisation,
    )

    plan_query = session.query(sql_scheme.Plan.ikz,
        # .label() усанавливает имя для колонки
        sql_scheme.Internal_plan.kbk.label('kbk'),
        sql_scheme.Internal_plan.detalisation.label('dfo'),
        sql_scheme.Plan.subject.label('predmet'),
        sqlalchemy.sql.literal_column("'аукцион'").label('procedur'),
        sql_scheme.Plan.amount.label('summa'),
        sql_scheme.Plan.ikz.label('id_num'),
        sqlalchemy.sql.null().label('start_date'),
        sqlalchemy.sql.null().label('finish_date'),
        sqlalchemy.sql.null().label('pay_short'),
        sqlalchemy.sql.null().label('pay_full'),
        sqlalchemy.sql.literal_column("'план'").label('table'),
        
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
        sql_scheme.Plan.budget_year == budget_year,
    ))
    
    both = deals_query.union_all(plan_query).order_by('kbk', 'dfo', 'table')
    
    answer = []
    for row in both:
        # итерация по запросу возвращает картеж со значениями,
        # в порядке установленном в запросе колонок
        answer_dict = {}
        position = 0
        # извлекаем названия колонко - возвращается список словарей ,
        # где по ключу name получаем название колокни
        for column_property in both.column_descriptions:
            #print(row, column_property, column_property['name'], position)
            answer_dict[column_property['name']] = row[position]
            position += 1
        answer.append(answer_dict)

    return answer