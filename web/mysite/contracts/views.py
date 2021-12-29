from django.shortcuts import render
from django.http import HttpResponse
import requests

# Create your views here.
def index(request):

    return render(request, 'contracts/index.html', {'num':3})

def deal(request):
    if dict(request.GET):
        num = int(dict(request.GET)['num'][0])        
        r = requests.get(f"http://127.0.0.1:8000/items/{num}")
        data_dict = {'table1':[r.json()],}
        return render(request, 'contracts/deal.html', data_dict)
    else:
        return first_html(request)    

def all_deals_list(request):
    print('start request to fastapi')
    r = requests.get("http://127.0.0.1:8000/all")
    print('finish request to fastapi')
    data_dict = {'table1':r.json(),}    
    return render(request, 'contracts/ab.html', data_dict)