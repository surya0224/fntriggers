import json
import numpy as np
import pandas as pd
import datetime
import math
import time
from jupiter_AI.batch.atpco_automation.Automation_tasks import run_yqyr_complete
from celery import group, chord, chain
from jupiter_AI import client, JUPITER_DB, SYSTEM_DATE, today, parameters, RABBITMQ_HOST, RABBITMQ_PASSWORD, \
    RABBITMQ_PORT, RABBITMQ_USERNAME, ATPCO_DB, JUPITER_LOGGER
from jupiter_AI.logutils import measure
import rabbitpy

# from jupiter_AI.batch.fbmapping_batch.JUP_AI_Batch_Fare_Ladder_Mapping import get_od_list

url = 'amqp://' + RABBITMQ_USERNAME + \
      ":" + RABBITMQ_PASSWORD + \
      "@" + RABBITMQ_HOST + ":" + str(RABBITMQ_PORT) + "/"

db = client[JUPITER_DB]


# def intersection(lst1, lst2):
#     # Use of hybrid method
#     temp = set(lst2)
#     lst3 = [value for value in lst1 if value in temp]
#     return lst3


@measure(JUPITER_LOGGER)
def yqyr(system_date):
    # system date is current date -1
    #run type define whether one wants to process YQYR for the complete collection or just for current system date
    run_type="Daily"
    #run_type="Complete"
    sysdate = datetime.datetime.strptime(system_date, '%Y-%m-%d')

    file_date = datetime.datetime.strftime(sysdate, "%Y%m%d")
    st = time.time()
    coll = db.JUP_DB_ATPCO_Fares_Rules
    od_list = {}
    cxr = coll.distinct('carrier')
    # cxr.remove('FZ')
    cxr=['EK']

    for i in cxr:
        od_list.update({i: []})
        cod = db.Temp_fzDB_tbl_002.distinct('OD')
        #cod=["QYGMLE","QYGMRU","QYGSEZ"]
        od_list[i] = cod
        # od_list[i]=(intersection(ods, cod))
        # print od_list[i]

    carrier_od = []
    for carrier in list(od_list.keys()):

        curx = db.JUP_DB_Carrier_hubs.find({"carrier": carrier})
        if curx.count() == 0:
            print "no hub found"
            continue
        for x in curx:
            hub = x["hub"]
            hub_country = x["hub_country"]
            hub_zone = x["hub_zone"]
            hub_area = x["hub_area"]

        for OD in od_list[carrier]:

            origin = OD[:3]
            destination = OD[3:]

            curz = db.JUP_DB_ATPCO_Zone_Master.find({"CITY_CODE": {"$in": [origin, destination]}})
            for j in curz:
                if j["CITY_CODE"] == origin:
                    origin_area = j["CITY_AREA"]
                    origin_zone = j["CITY_ZONE"]
                    origin_country = j["CITY_CNTRY"]

                elif j["CITY_CODE"] == destination:
                    destination_area = j["CITY_AREA"]
                    destination_zone = j["CITY_ZONE"]
                    destination_country = j["CITY_CNTRY"]
            carrier_od.append(
                run_yqyr_complete.s(system_date=system_date, carrier=[carrier], OD=[OD],hub= hub,hub_area= hub_area,hub_country= hub_country,hub_zone= hub_zone, origin_area=origin_area,
                                         origin_country=origin_country, origin_zone=origin_zone, destination_area=destination_area,destination_country= destination_country,destination_zone= destination_zone))


    group_y = group(carrier_od)
    resy = group_y()
    grpy_res = resy.get()
    print 'yqyr done in', time.time() - st

    db.JUP_DB_ATPCO_Fares_Rules.update({'carrier': "FZ", 'channel': "TA", 'origin_country': "IN"},
                                       {'$set': {"YR": 0.0}}, multi=True)

    db.JUP_DB_ATPCO_Fares_Rules.update({'carrier': "FZ", 'channel': "web"}, {'$set': {"YR": 0.0}}, multi=True)

    db.JUP_DB_ATPCO_Fares_Rules.update({'carrier': "FZ", 'Rule_id': "GP08", 'private_fare': True},
                                       {'$set': {"YR": 0.0}}, multi=True)


if __name__ == '__main__':
    print("yqyr start")
    system_date = datetime.datetime.strftime(today - datetime.timedelta(days=1), "%Y-%m-%d")
    #system_date = "2018-12-27"
    yqyr(system_date)