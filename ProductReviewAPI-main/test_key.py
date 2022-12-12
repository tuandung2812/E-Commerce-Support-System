import requests

result = requests.post("http://localhost:5000/new_api_key", data={'device_name': 'test1'})
print(result.json())
