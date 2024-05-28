import copy
import json
import pymongo
import calendar
from pymongo.errors import BulkWriteError
from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne
import collections
#import pandas as pd
import numpy as np
from datetime import datetime
from datetime import date
from datetime import time
import datetime as dt
import time
from dateutil.relativedelta import relativedelta
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
def main(od, client):
    db = client[JUPITER_DB]
    num = 1
    Bulk = db.JUP_DB_Manual_Triggers_Module.initialize_unordered_bulk_op()
    cursor = db.JUP_DB_Host_OD_Capacity.find({'dep_date':{"$gte" : "2021-01-01","$lte" : "2023-12-31"}, 'od':{'$in' : od}}, no_cursor_timeout=True)
    for x in cursor:
        try:
            # print(x)
            dep_date_ISO = datetime.strptime(x['dep_date'], '%Y-%m-%d')
            dep_date = x['dep_date']
            dep_month = int(x['month'])
            dep_year = int(x['year'])
            od = x['origin']+ x['destination']
            origin = dict()
            # origin['Network'] = x['origin'],['network'],
            # origin['Cluster'] = x['origin'],['cluster'],
            # origin['Region'] = x['origin'],['region'],
            # origin['Country'] = x['origin'],['country'],
            origin['City'] = str(x['origin']),
            destination = dict()
            # destination['Network'] = x['destination'],['network']
            # destination['Cluster'] = x['destination'],['cluster']
            # destination['Region'] = x['destination'],['region']
            # destination['Country'] = x['destination'],['country']
            destination['City'] = x['destination']
            compartment_J = dict()
            compartment_J['compartment'] = "J"
            compartment_J['all'] = 'all'
            inventory_J = dict()
            inventory_J["booking"] = x['leg1_j_bookings']
            # print(origin)
            if x['leg1_j_bookings_1'] is None or np.isnan(x['leg1_j_bookings_1']):
                inventory_J["booking_1"] = 0
            else:
                inventory_J["booking_1"] = x['leg1_j_bookings_1']

            inventory_J["capacity"] = x['j_cap']

            if x['j_cap_1'] is None or np.isnan(x['j_cap_1']):
                inventory_J["capacity_1"] = 0
            else:
                inventory_J["capacity_1"] = x['j_cap_1']
            # print(compartment)
            # print(dep_date_ISO)
            Bulk.find({
                'origin.City': x['origin'],
                'destination.City': x['destination'],
                'compartment.compartment': compartment_J['compartment'],
                "dep_date": dep_date,
                # "dep_date_ISO": dep_date_ISO,
            }).update(
                {
            "$set": {
                #"od":od,
                #"origin": origin,
                #"destination": destination,
                #"compartment": compartment_J,
                "dep_date": dep_date,
                # "dep_date_ISO": dep_date_ISO,
                "dep_month": dep_month,
                "dep_year": dep_year,
                "inventory": inventory_J
                    }
                }
                )
            # for Y compartment
            compartment_Y = dict()
            compartment_Y['compartment'] = "Y"
            compartment_Y['all'] = 'all'
            inventory_Y = dict()
            inventory_Y["booking"] = x['leg1_y_bookings']

            if x['leg1_y_bookings_1'] is None or np.isnan(x['leg1_y_bookings_1']):
                inventory_Y["booking_1"] = 0
            else:
                inventory_Y["booking_1"]= x['leg1_y_bookings_1']

            inventory_Y["capacity"] = x['y_cap']

            if x['y_cap_1'] is None or np.isnan(x['y_cap_1']):
                inventory_Y["capacity_1"] =  0
            else:
                inventory_Y["capacity_1"] = x['y_cap_1']

            Bulk.find({
                'origin.City': x['origin'],
                'destination.City': x['destination'],
                'compartment.compartment': compartment_Y['compartment'],
                "dep_date": dep_date,
                # "dep_date_ISO": dep_date_ISO,
            }).update(
                {
                    "$set": {
                    #'od':od,
                    #"origin": origin,
                    #"destination": destination,
                    #"compartment": compartment_Y,
                    "dep_date": dep_date,
                    # "dep_date_ISO": dep_date_ISO,
                    "dep_month": dep_month,
                    "dep_year": dep_year,
                    "inventory": inventory_Y
                    }
                }
                    )

            if num % 1000 == 0:
                try:
                    doc = Bulk.execute()
                    # print("Insideside")
                    # print(x['origin'],destination['City'],compartment_Y['compartment'],dep_date,dep_date_ISO)
                    print(doc)
                    Bulk = db.JUP_DB_Manual_Triggers_Module.initialize_unordered_bulk_op()
                except Exception as bwe:
                    print(bwe)
            num = num + 1

        except Exception as error:
            print(error)

    try:
        doc = Bulk.execute()
        # print("outside")
        print(doc)
    except Exception as bwe:
        print(bwe)


if __name__=='__main__':

    db = client[JUPITER_DB]
    od_list = list(db.JUP_DB_Manual_Triggers_Module.distinct('od', {'pos.City': {'$ne': None}, 'trx_date': SYSTEM_DATE}))
    # od_list = list(db.JUP_DB_Manual_Triggers_Module.distinct('od', {'pos': {'$ne': None}, 'trx_date': "2018-04-17"}))
    numb = 0
    mtt = []
    od_arr = []
    # '''
    try:
        for od in od_list:
            #print(od)
            od_arr.append(od)
            if numb == 100:
                main(od_arr, client)
                od_arr = []
                numb = 0
            else:
                numb = numb + 1
        if not od_arr:
            print("final list is empty")
        else:
            main(od_arr, client)
    except Exception as error:
        print(error)

    # main(["DXBAMM"], client)