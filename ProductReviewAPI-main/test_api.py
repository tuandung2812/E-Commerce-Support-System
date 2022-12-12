import requests
import json
import pprint

bs_api_key = 'DITF>NGE'
legit_api_key = 'ab837f0cf3f14b9a9fce78265dc45076'

url = "https://www.sendo.vn/op-lung-iphone-6-plus-6s-plus-chong-soc-360-17878754.html"
result = requests.get("http://localhost:5000/review/byname", params={"api_key":legit_api_key,
                                                                     "name": "tủ lạnh lg",
                                                                     "site": "all",
                                                                     "maxreview": 5,
                                                                     "productnum": 5})
# result = requests.get("http://localhost:5000/review/byurl", params={"api_key":legit_api_key,
#                                                                      "url": url,
#                                                                      "site": "sendo",
#                                                                      "maxreview": 5
#                                                                     })

pprint.pprint(result.json())
with open('result.json', 'w', encoding='utf-8') as f:
    json.dump(result.json(), f, indent=4, ensure_ascii=False)
