from typing import Optional

from fastapi import FastAPI

import main

app = FastAPI()


@app.get("/deals")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Optional[str] = None):
    query_sd = main.filter_query(item_id)
    #print('query', query_sd)
    return query_sd

@app.get("/all")
def read_all():
    query_sd = main.filter_all()
    #print('query', query_sd)
    return query_sd