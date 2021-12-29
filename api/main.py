from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, Date, Float, Boolean
from sqlalchemy.orm import sessionmaker

engine = create_engine('postgresql://owner:1111@localhost:5432/contracts', echo=True)
Base = declarative_base()

class Deals(Base):
    __tablename__ = 'deals'
    
    reg_number = Column(Integer, primary_key=True)
    contract_number = Column(Text)
    subject = Column(Text)
    partner = Column(Text)
    inn = Column(Text)
    contract_date = Column(Date)
    state_register_number = Column(Text)
    kbk = Column(Text)
    period = Column(Text)
    detalisation = Column(Text)
    amount = Column(Float)
    purchase_procedure = Column(Text)
    responsible = Column(Text)
    mark_perfomance = Column(Boolean)
    execution_date = Column(Date)
    budget_commitment = Column(Text)
    total_amount = Column(Float)
    buget_year = Column(Text)

def filter_query(number: int) -> str:
    Session = sessionmaker(bind=engine)
    session = Session()

    for row in session.query(Deals).filter(Deals.reg_number == number):
        #print(row.reg_number, row.subject)
        answer = {
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
            'budget_commitment':row.budget_commitment,
            'total_amount':row.total_amount,
            'buget_year':row.buget_year,
            'inn':row.inn,
            'kbk':row.kbk,
        }
        #print(answer)
    session.commit()
    return answer

def all(number):
    Session = sessionmaker(bind=engine)
    session = Session()

    query = session.query(Deals).filter(Deals.reg_number == number)
    print(query.all()[0].subject)


    session.commit()    



def filter_all():
    print('start')
    Session = sessionmaker(bind=engine)
    session = Session()
    big_answer = []
    for row in session.query(Deals):
        #print(row.reg_number, row.subject)
        answer = {
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
            'budget_commitment':row.budget_commitment,
            'total_amount':row.total_amount,
            'buget_year':row.buget_year,
            'inn':row.inn,
            'kbk':row.kbk,
        }
        big_answer.append(answer)
        print(answer)
    session.commit()
    return big_answer

#filter_all()