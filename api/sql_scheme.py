from sqlalchemy import ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, Date, Float, Boolean
from sqlalchemy.orm import relationship, sessionmaker

import configparser
from pathlib import Path


engine = create_engine('postgresql://owner:1111@localhost:5432/contracts', echo=False)
Base = declarative_base()

class Budget_commitment(Base):
    __tablename__ = 'budget_commitment'

    advance_amount = Column(Float)
    advance_value = Column(Float)
    amount = Column(Float)
    commitment_date = Column(Date)
    commitment_number = Column(Text, ForeignKey('commitment_treasury.commitment_number'))
    contract_date = Column(Date)
    contract_identificator = Column(Text)
    contract_number = Column(Text)
    execution_date = Column(Date)
    notification_amount = Column(Float)
    notification_date = Column(Date)
    notification_number = Column(Text)
    reg_number = Column(Integer)
    reg_number_full = Column(Text, primary_key=True)
    subject = Column(Text)
    year = Column(Text)


class Commitment_treasury(Base):
    __tablename__ = 'commitment_treasury'

    amount = Column(Float)
    basic_doc_name = Column(Text)
    basic_doc_number = Column(Text)
    change_number = Column(Integer)
    commitment_date = Column(Date)
    commitment_number = Column(Text)
    doc_date = Column(Date)
    doc_number = Column(Text)
    execution_date = Column(Date)
    index = Column(Text, primary_key=True)
    registration_date = Column(Date)
    status = Column(Text)
    subject = Column(Text)
    type = Column(Text)
    commitments = relationship('Budget_commitment', backref='commitment_treasury')


class Deals(Base):
    __tablename__ = 'deals'

    amount = Column(Float)
    budget_year = Column(Integer)
    change_number = Column(Text)
    commitment_number = Column(Text)
    contract_date = Column(Date)
    contract_number = Column(Text)
    detalisation = Column(Text)
    execution_date = Column(Date)
    ikz = Column(Text)
    inn = Column(Text)
    kbk = Column(Text)
    mark_perfomance = Column(Boolean)
    paid_for = Column(Float)
    partner = Column(Text)
    period = Column(Text)
    purchase_procedure = Column(Text)
    reg_number = Column(Integer)
    reg_number_full = Column(Text, primary_key=True)
    responsible = Column(Text)
    state_register_number = Column(Text)
    subject = Column(Text)
    total_amount = Column(Float)
    pays_short = relationship('Payments_short', backref='deals')
    pays_full = relationship('Payments_full', backref='deals')


class Internal_plan(Base):
    __tablename__ = 'internal_plan'

    amount = Column(Float)
    amount_new = Column(Float)
    amount_old = Column(Float)
    contract_number = Column(Text)
    contract_year = Column(Text)
    detalisation = Column(Text)
    execution_date = Column(Date)
    ikz = Column(Text, ForeignKey('plan.ikz'), primary_key=True)
    kbk = Column(Text)
    law = Column(Text)
    limitations = Column(Text)
    method = Column(Text)
    next_next_year = Column(Float)
    next_year = Column(Float)
    number = Column(Text)
    okpd = Column(Text)
    partner = Column(Text)
    responsible = Column(Text)
    status = Column(Text)
    subject = Column(Text)


class Payments_full(Base):
    __tablename__ = 'payments_full'

    amount = Column(Float)
    contract_identificator = Column(Text)
    csr = Column(Text)
    detalisation = Column(Text)
    inn = Column(Text)
    kbk = Column(Text)
    kosgu = Column(Text)
    order_date = Column(Date)
    order_number = Column(Text, primary_key=True)
    partner = Column(Text)
    partner_inn = Column(Text)
    payment_order_date = Column(Date)
    payment_order_number = Column(Integer)
    primary_doc_date = Column(Date)
    primary_doc_form = Column(Text)
    primary_doc_number = Column(Text)
    reg_number = Column(Integer)
    reg_number_full = Column(Text, ForeignKey('deals.reg_number_full'))
    subjekt = Column(Text)
    year = Column(Integer)


class Payments_short(Base):
    __tablename__ = 'payments_short'

    amount = Column(Float)
    contract_identificator = Column(Text)
    csr = Column(Text)
    detalisation = Column(Text)
    inn = Column(Text)
    kbk = Column(Text)
    kosgu = Column(Text)
    order_date = Column(Date)
    order_number = Column(Text, primary_key=True)
    partner = Column(Text)
    partner_inn = Column(Text)
    payment_order_date = Column(Date)
    payment_order_number = Column(Integer)
    primary_doc_date = Column(Date)
    primary_doc_form = Column(Text)
    primary_doc_number = Column(Text)
    reg_number = Column(Integer)
    reg_number_full = Column(Text, ForeignKey('deals.reg_number_full'))
    subjekt = Column(Text)
    year = Column(Integer)


class Plan(Base):
    __tablename__ = 'plan'

    amount = Column(Float)
    amount_next_year = Column(Float)
    budget_year = Column(Float)
    ikz = Column(Text, primary_key=True)
    okpd = Column(Text)
    publish_year = Column(Float)
    state_register_number = Column(Text)
    subject = Column(Text)
    cancel_tag = Column(Boolean)
    int_plan = relationship('Internal_plan', backref='plan')



class Purchases(Base):
    __tablename__ = 'purchases'

    amount = Column(Float)
    budget_year = Column(Text)
    okpd = Column(Text)
    order_number = Column(Text, primary_key=True)
    reg_number = Column(Integer)
    state_register_number = Column(Integer)
    subject = Column(Text)
