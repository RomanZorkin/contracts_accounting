from django.shortcuts import render
from django.http import HttpResponse
from django.conf.urls.static import static
import requests

HOST = 'http://127.0.0.1:8000/'

# Create your views here.
def index(request):
    return render(request, 'contracts/index.html', {'num':3})

def contract_card(request):
    if dict(request.GET):
        num = str(dict(request.GET)['num'][0])
        #year = str(dict(request.GET)['year'][0])
        r = requests.get(f"http://127.0.0.1:8000/items?num={num}")
        data_dict = {
            'table1':[r.json()],
            'pay':36,
        }
        print(data_dict)
        return render(request, 'contracts/contract_card.html', data_dict)
    else:
        return contracts(request)

def contracts(request):
    r = requests.get("http://127.0.0.1:8000/all")
    data_dict = {'table1':r.json(),}
    return render(request, 'contracts/contracts.html', data_dict)

def notifications(request):
    req_dict = {
        'table': requests.get(f'{HOST}notifications').json()
    }
    print(req_dict)
    return render(request, 'contracts/notifications.html', req_dict)

def budget_commitment(request):
    req_dict = requests.get(f'{HOST}budget_commitment').json()
    return render(request, 'contracts/budget_commitment.html', req_dict)

def commitment_treasury(request):
    req_dict = requests.get(f'{HOST}commitment_treasury').json()
    return render(request, 'contracts/commitment_treasury.html', req_dict)

def deals(request):
    req_dict = requests.get(f'{HOST}deals').json()
    req_dict['pusy'] = 'ty'
    print(req_dict)
    return render(request, 'contracts/deals.html', req_dict)

def limits(request):
    req_dict = requests.get(f'{HOST}limits').json()
    return render(request, 'contracts/limits.html', req_dict)

def payment_schedule(request):
    req_dict = requests.get(f'{HOST}payment_schedule').json()
    return render(request, 'contracts/payment_schedule.html', req_dict)

def payments_full(request):
    req_dict = requests.get(f'{HOST}payments_full').json()
    return render(request, 'contracts/payments_full.html', req_dict)

def payments_short(request):
    req_dict = requests.get(f'{HOST}payments_short').json()
    return render(request, 'contracts/payments_short.html', req_dict)

def payments(request):
    req_dict = requests.get(f'{HOST}payments').json()
    return render(request, 'contracts/payments.html', req_dict)

def plan(request):
    req_dict = requests.get(f'{HOST}plan').json()
    return render(request, 'contracts/plan.html', req_dict)

def purchase_plan(request):
    req_dict = requests.get(f'{HOST}purchase_plan').json()
    return render(request, 'contracts/purchase_plan.html', req_dict)

def spending(request):
    req_dict = requests.get(f'{HOST}spending').json()
    return render(request, 'contracts/spending.html', req_dict)
