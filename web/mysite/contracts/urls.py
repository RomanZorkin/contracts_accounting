"""mysite URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.urls import path, include
from . import views

urlpatterns = [    
    path('', views.index, name='index'),
    path('contract_card', views.contract_card, name='contract_card'),
    path('contracts', views.contracts, name='contracts'),
    path('notifications', views.notifications, name='notifications'),
    path('budget_commitment', views.budget_commitment, name='budget_commitment'),
    path('commitment_treasury', views.commitment_treasury, name='commitment_treasury'),
    path('deals', views.deals, name='deals'),
    path('limits', views.limits, name='limits'),
    path('payment_schedule', views.payment_schedule, name='payment_schedule'),
    path('payments_full', views.payments_full, name='payments_full'),
    path('payments_short', views.payments_short, name='payments_short'),
    path('payments', views.payments, name='payments'),
    path('plan', views.plan, name='plan'),
    path('purchase_plan', views.purchase_plan, name='purchase_plan'),
    path('spending', views.spending, name='spending'),
]
