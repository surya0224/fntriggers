"""
Author: Anamika Jaiswal
Date Created: 2019-02-20
File Name: YQYR_celery.py
Every month JUP_DB_Exchange_Rate gets updated. This code is run to update the Reference_Rate in JUP_DB_ATPCO_Fares_Rules,JUP_DB_ATPCO_YQYR_RecS1
and to update calculated YQ,YR values using latest exchange rate

"""

import pymongo
from pymongo import MongoClient

client = MongoClient()
import time
import re
import datetime


from jupiter_AI import mongo_client, JUPITER_DB, ATPCO_DB, JUPITER_LOGGER
from jupiter_AI.logutils import measure



count = 1
@measure(JUPITER_LOGGER)

def exchange_rate_main(client,system_date,carrier_list):
    count = 0
    carrier=carrier_list[0]
    #here system_date is actual system_date -1
    db_ATPCO = client[ATPCO_DB]
    db_fzDB = client[JUPITER_DB]


    coll = db_ATPCO.JUP_DB_ATPCO_YQYR_RecS1
    collf = db_fzDB.JUP_DB_ATPCO_Fares_Rules
    exr = db_fzDB.JUP_DB_Exchange_Rate
    st = time.time()

    curx = exr.find({"reference_factor":{'$nin':[None,1]}})
    for i in curx:
        count += 1
        coll.update({"CXR_CODE":carrier,"SERVICE_FEE_CUR": i["code"]}, {'$set': {'Reference_Rate': i["Reference_Rate"]}}, multi=True)
        collf.update({"carrier":carrier,'$or': [{'effective_to': None}, {'effective_to': {'$gt': system_date}}],"currency": i["code"]}, {'$set': {'Reference_Rate': i["Reference_Rate"]}}, multi=True)
        print (count)

    print ("Updated Reference Rates", time.time() - st)

    # sty = time.time()
    # curx = exr.find({'reference_factor': {'$nin':[None,1]}})
    # for i in curx:
    #     count += 1
    #     print count
    #     a = i['reference_factor']
    #     coll.update(
    #         {"carrier": carrier, "currency": i["code"]},
    #         {'$mul': {'YQ': a,"YR":a}}, multi=True)
    # print ("Updated YQ YR in ", time.time() - sty)

    print ("Done in ", time.time() - st)


if __name__=='__main__':
    client = mongo_client()
    system_date="2019-01-24"
    carrier=["FZ"]
    exchange_rate_main(client,system_date,carrier)
