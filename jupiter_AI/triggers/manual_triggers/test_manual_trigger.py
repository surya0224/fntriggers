import requests
import json

url = "http://127.0.0.1:5000/raiseManualTrigger"

parameters = [{
    "fromDate": "2018-05-01",
    "toDate": "2018-05-31",
    "pos": "KWI",
    "origin": "KWI",
    "destination": "DXB",
    "compartment": "Y",
    "reason": "R1",
    "work_package_name": "WP1",
    "flag": "M"
}]

url1 = "http://127.0.0.1:5000/raiseSystemTriggers"

parameters1 = ["CMBCMBDXBY", "KTMKTMDXBY"]

headers = {"Connection": "keep-alive",
           "Content-Length": "257",
           "X-Requested-With": "XMLHttpRequest",
           "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.52 Safari/536.5",
           "Content-Type": "application/json",
           "Accept": "*/*",
           "Accept-Encoding": "gzip,deflate",
           "Accept-Language": "q=0.8,en-US",
           "Accept-Charset": "utf-8"
           }

parameters2 = {
    'strategy_name': 'top 10 markets',
    'collection_name': 'JUP_DB_Market_Significance',
    'query': '[{"$match": {"online": true}},{"$sort": {"rank": -1}},{"$group": {"_id": {"market": "$market"}}},{"$project": {"_id": 0, "market": "$_id.market"}}]'
}

parameters2 = [{
    'user': 'l.kumarasamy',
    'name': 'forecast sf',
    'collection_name': 'JUP_DB_Market_Significance'
    # ,
    # 'query': '[{"$match": {"online": true}},{"$sort": {"rank": -1}},{"$group": {"_id": {"market": "$market"}}},{"$project": {"_id": 0, "market": "$_id.market"}}]'
}]

url2 = "http://127.0.01:5000/runStrategicQuery"

response = requests.post(url, data=json.dumps(parameters), headers=headers)
print response.text