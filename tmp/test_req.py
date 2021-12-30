import requests
from sqlorm.cleaner import Cleaner



r = requests.get("http://127.0.0.1:8000/all")
print(r.text)