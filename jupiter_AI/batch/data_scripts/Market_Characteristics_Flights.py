import copy
import json
import pymongo
import calendar
from pymongo.errors import BulkWriteError
from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne
import collections
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from datetime import date
import datetime as dt
import time
from dateutil.relativedelta import relativedelta
from jupiter_AI import client, JUPITER_DB, JUPITER_LOGGER
from jupiter_AI.logutils import measure
from jupiter_AI.network_level_params import SYSTEM_DATE

# Connect mongodb db business layer
db = client[JUPITER_DB]


startTime = datetime.now()
# cur_date = datetime.strptime("2018-04-20", '%Y-%m-%d')
cur_date = datetime.strptime(SYSTEM_DATE, '%Y-%m-%d')

if len(str(cur_date.day)) == 2:
    date = str(cur_date.day)
else:
    date = '0' + str(cur_date.day)

if len(str(cur_date.month)) == 2:
    month = str(cur_date.month)
else:
    month = '0' + str(cur_date.month)

year = cur_date.year
this_month = cur_date.month
next_month = cur_date.month +1
system_date = str(year)+'-'+str(month)+'-'+str(date)

cur_date_start = cur_date - relativedelta(months=1)

# print cur_date_start
if len(str(cur_date_start.day)) == 2:
    date_1wkLTR = str(cur_date_start.day)
else:
    date_1wkLTR = '0' + str(cur_date_start.day)

if len(str(cur_date_start.month)) == 2:
    month_1wkLTR = str(cur_date_start.month)
else:
    month_1wkLTR = '0' + str(cur_date.month)

year_1wkLTR = cur_date_start.year
# dep_date_start = str(year_1wkLTR)+'-'+month_1wkLTR+'-'+'01'
dep_date_start = datetime.strftime((cur_date - relativedelta(months=1)), '%Y-%m') +"-01"


cur_date_end = cur_date + relativedelta(months=+3)
# cur_date_end = datetime(cur_date.year, cur_date.month+4, calendar.monthrange(cur_date.year, cur_date.month+4)[1])

if len(str(cur_date_end.day)) == 2:
    date_1wkLTR = str(cur_date_end.day)
else:
    date_1wkLTR = '0' + str(cur_date_end.day)

if len(str(cur_date.month)) == 2:
    month_1wkLTR = str(cur_date_end.month)
else:
    month_1wkLTR = '0' + str(cur_date_end.month)

year_1wkLTR = cur_date_end.year
dep_date_end = datetime.strftime(cur_date_end, '%Y-%m-')+""+str(calendar.monthrange(cur_date_end.year, cur_date_end.month)[1])

print dep_date_start,dep_date_end
date2_ms = datetime.now()


@measure(JUPITER_LOGGER)
def main(client):
    db = client[JUPITER_DB]
    pipeline = [
    {"$match":{"dep_date": {"$gte":dep_date_start,"$lte":dep_date_end}}},

    {"$sort":{'origin': 1, "destination": 1, "od": 1, "compartment": 1, "flight_number": 1, "dep_date": 1, "snap_date": -1}},

    {"$group":{
        "_id": {
            "origin": '$origin',
            "destination": '$destination',
            "compartment": '$compartment',
            "flight_number": '$flight_number',
            "dep_date": '$dep_date'
        },

        "pax": {"$first":'$pax'},
        "pax_1":{"$first":'$pax_1'},
        "bookings":{"$first":'$bookings'},
        "bookings_1":{"$first":'$bookings_1'},
        "forecast_pax":{"$first":'$pax_forecast'},
        "forecast_pax_1":{"$first":'$pax_forecast_1'},
        "capacity":{"$first":'$capacity'},
        "capacity_1":{"$first":'$capacity_1'},
    }},

    {"$group":{
        "_id": {
            "origin": '$_id.origin',
            "destination": '$_id.destination',
            "compartment": '$_id.compartment',
            "dep_date": '$_id.dep_date'
        },
        "pax": {"$sum":'$pax'},
        "pax_1":{"$sum":'$pax_1'},
        "bookings":{"$sum":'$bookings'},
        "bookings_1":{"$sum":'$bookings_1'},
        "forecast_pax":{"$sum":'$forecast_pax'},
        "forecast_pax_1":{"$sum":'$forecast_pax_1'},
        "capacity":{"$sum":'$capacity'},
        "capacity_1":{"$sum":'$capacity_1'},
    }}
    ]

    cursor = db.JUP_DB_Market_Characteristics_Flights.aggregate(pipeline,allowDiskUse = True)

    num = 1;
    bulk1 = db.JUP_DB_Flight_Leg_Characteristics.initialize_unordered_bulk_op()
    for x in cursor:
        dateformat = '%Y-%m-%d'
        dep_date_ISO = datetime.strptime(x['_id']["dep_date"], dateformat)

        od = x['_id']["origin"] + "" + x['_id']["destination"];
        combine_column = od + "" + x['_id']["compartment"] + "" + x['_id']["dep_date"];
        try:
            bulk1.find({
                "dep_date":x['_id']['dep_date'],
                'od':od,
                'compartment':x['_id']['compartment'],
            }).upsert().update(
                {
            "$set":{
                "dep_date_ISO": dep_date_ISO,
                "dep_date": x["_id"]["dep_date"],
                'od': od,
                'compartment': x["_id"]["compartment"],
                "origin": x["_id"]["origin"],
                "destination": x["_id"]["destination"],
                "combine_column": combine_column,
                "pax": x["pax"],
                "pax_1": x["pax_1"],
                "bookings": x["bookings"],
                "bookings_1": x["bookings_1"],
                "forecast_pax": x["forecast_pax"],
                "forecast_pax_1": x["forecast_pax_1"],
                "capacity": x["capacity"],
                "capacity_1": x["capacity_1"]
            }
            }
            )

            if((num % 100) == 0):
                try :
                    bulk1.execute()
                    bulk1 = db.JUP_DB_Flight_Leg_Characteristics.initialize_unordered_bulk_op()
                except BulkWriteError as bwe:
                    print(bwe.details)
        except Exception as error:
            print error

        num = num + 1

    try :
        bulk1.execute();
    except Exception as error:
        print error


if __name__ == '__main__':
    db = client[JUPITER_DB]
    main(client)
