from typing import Optional

from fastapi import FastAPI

import api.sql_handler as sql_handler

app = FastAPI()


@app.get("/items/")
def read_item(num: str):  
    #reg_num = f'{item_id}/{str(year)[2:4]}'  
    query_sd = sql_handler.filter_query(num)
    #print('query', query_sd)
    return query_sd

@app.get("/all")
def read_all():
    query_sd = sql_handler.filter_all()
    #print('query', query_sd)
    return query_sd

@app.get("/notifications")
def notifications():
    #sql_handler.notifications()
    return sql_handler.notifications()

@app.get("/budget_commitment")
def budget_commitment():
    return {'func': 'budget_commitment'}

@app.get("/commitment_treasury")
def commitment_treasury():
    return {'func': 'commitment_treasury'}

@app.get("/deals")
def deals():
    answer = {'func': 'deals'}
    print(answer)
    return answer

@app.get("/limits")
def limits():
    return {'func': 'limits'}

@app.get("/payment_schedule")
def payment_schedule():
    return {'func': 'payment_schedule'}

@app.get("/payments_full")
def payments_full():
    return {'func': 'payments_full'}

@app.get("/payments_short")
def payments_short():
    return {'func': 'payments_short'}

@app.get("/payments")
def payments():
    return {'func': 'payments'}

@app.get("/plan")
def plan():
    return {'func': 'plan'}

@app.get("/purchase_plan")
def purchase_plan():
    return {'func': 'purchase_plan'}

@app.get("/spending")
def spending():
    return {'func': 'spending'}