import copy
import json
import pymongo
import calendar
from pymongo.errors import BulkWriteError
from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne
import collections
import math
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
import datetime as dt
import time
import sys
from jupiter_AI import client, JUPITER_DB, JUPITER_LOGGER
from jupiter_AI.logutils import measure
from jupiter_AI.network_level_params import SYSTEM_DATE

# Connect mongodb db business layer
# db = client[JUPITER_DB]

# Network parameter #
script_start_time = time.clock()
cur_date = datetime.strptime(SYSTEM_DATE, '%Y-%m-%d')
dateformat = '%Y-%m-%d'
dateformat_y_m = '%Y-%m'
yearformat = '%Y'
monthformat = '%m'

if len(str(cur_date.day)) == 2:
    date = str(cur_date.day)
else:
    date = '0'+str(cur_date.day)

if len(str(cur_date.month)) == 2:
    month = str(cur_date.month)
else:
    month = '0'+str(cur_date.month)

next_month = cur_date.month+1
year = cur_date.year
str_date = str(year)+'-'+ str(month)+'-'+date

@measure(JUPITER_LOGGER)
def main(od,client):
    copyList = copy.deepcopy(od)
    db = client[JUPITER_DB]
    num = 1
    Bulk = db.JUP_DB_Manual_Triggers_Module.initialize_unordered_bulk_op()
    cursor = db.JUP_DB_OD_Distance_Master.find({'od': {'$in' : od},'distance' : {'$ne' : None}},no_cursor_timeout=True)
    for x in cursor:
        try:
            Distance = x['distance']
            if np.isnan(Distance):
                Distance = 0
            Bulk.find({
                'origin.City': x['origin'],
                'destination.City': x['destination'],
                "dep_date_ISO" : {"$ne": None},
                "trx_date": str_date
            }).update(
            {
              "$set":{
                      "distance" : Distance,
                      "psuedo_od" : x["psuedo_od"]
                    }
            })
            copyList.remove(x['od'])
            if num % 50 == 0:
                try:
                    Bulk.execute()
                    Bulk = db.JUP_DB_Manual_Triggers_Module.initialize_unordered_bulk_op()
                except Exception as bwe:
                    print(bwe)
            num = num + 1
        except Exception as error:
            print(error)

    # print copyList
    # break the remaining od and get from OD distance master sum the leg value and update to Manual triggers
    for each_od in copyList:
        distance = 0
        if "DXB" in each_od:
            print "No data ",each_od
        else:
            try:
                leg_1 = each_od[:3]+"DXB"
                cursor_leg_1 = db.JUP_DB_OD_Distance_Master.find_one({'od': leg_1, 'distance': {'$ne': None}},no_cursor_timeout=True)
                distance = cursor_leg_1["distance"]
                # print "leg_1", leg_1, distance

                leg_2 = "DXB" + each_od[3:]
                cursor_leg_2 = db.JUP_DB_OD_Distance_Master.find_one({'od': leg_2, 'distance': {'$ne': None}},no_cursor_timeout=True)
                distance = distance + cursor_leg_2["distance"]
                # print "leg_2", leg_2, distance
            except Exception as error:
                print "No data ", each_od
                distance = 0
        try:
            db.JUP_DB_Manual_Triggers_Module.update({
                'origin.City': each_od[:3],
                'destination.City': each_od[3:],
                "dep_date_ISO": {"$ne": None},
                "trx_date": str_date
            },{
                "$set": {
                    "distance": distance
                }
            },multi = True)
            # pass
        except Exception as error:
            pass
    try:
        Bulk.execute()
    except Exception as bwe:
        print(bwe)


if __name__ == '__main__':
    db = client[JUPITER_DB]
    od_list = list(db.JUP_DB_Manual_Triggers_Module.distinct('od', {'pos': {'$ne': None}, 'trx_date': SYSTEM_DATE}))
    numb = 0
    mtt = []
    od_arr = []
    for od in od_list:
        od_arr.append(od)
        if numb == 100:
            print od_arr
            main(od_arr, client)
            od_arr = []
            numb = 0
        else:
            numb = numb + 1

    main(od_arr, client)
    # od_list = list(db.JUP_DB_Manual_Triggers_Module.distinct('od', {'pos': {'$ne': None}, 'trx_date': SYSTEM_DATE}))
    # for od in od_list:
    #main(client)
