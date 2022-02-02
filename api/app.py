from typing import Optional

from fastapi import FastAPI

import main

app = FastAPI()


@app.get("/deals")
def read_root():
    return {"Hello": "World"}


@app.get("/items/")
def read_item(num: str):  
    #reg_num = f'{item_id}/{str(year)[2:4]}'  
    query_sd = main.filter_query(num)
    #print('query', query_sd)
    return query_sd

@app.get("/all")
def read_all():
    query_sd = main.filter_all()
    #print('query', query_sd)
    return query_sd