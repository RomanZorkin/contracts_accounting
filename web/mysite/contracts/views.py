from django.shortcuts import render
from django.http import HttpResponse
from django.conf.urls.static import static
import requests

# Create your views here.
def index(request):

    return render(request, 'contracts/index.html', {'num':3})

def deal(request):
    if dict(request.GET):
        num = str(dict(request.GET)['num'][0])  
        #year = str(dict(request.GET)['year'][0])     
        r = requests.get(f"http://127.0.0.1:8000/items?num={num}")
        data_dict = {
            'table1':[r.json()],
            'pay':{'fine':3}
        }
        print(data_dict)
        return render(request, 'contracts/deal.html', data_dict)
    else:
        return first_html(request)    

def all_deals_list(request):
    r = requests.get("http://127.0.0.1:8000/all")    
    data_dict = {'table1':r.json(),}    
    return render(request, 'contracts/ab.html', data_dict)