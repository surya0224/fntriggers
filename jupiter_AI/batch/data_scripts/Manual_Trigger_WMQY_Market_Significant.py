
import copy
import json
import pymongo
import calendar
from pymongo.errors import BulkWriteError
from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne
import collections
#import pandas as pd
import numpy as np
import math
import calendar

from datetime import datetime, timedelta
import time
from dateutil.relativedelta import relativedelta
from jupiter_AI import client, JUPITER_DB, JUPITER_LOGGER
from jupiter_AI.logutils import measure
from jupiter_AI.network_level_params import SYSTEM_DATE

# Connect mongodb db business layer
# db = client[JUPITER_DB]

startTime = datetime.now()
# cur_date = datetime.strptime("2018-04-20", '%Y-%m-%d')
cur_date = datetime.strptime(SYSTEM_DATE, '%Y-%m-%d')
# cur_date = datetime.strptime("2018-04-14", '%Y-%m-%d')

if len(str(cur_date.day)) == 2:
    date = str(cur_date.day)
else:
    date = '0'+str(cur_date.day)

if len(str(cur_date.month)) == 2:
    month = str(cur_date.month)
else:
    month = '0'+str(cur_date.month)
year = cur_date.year
this_month = cur_date.month
next_month = cur_date.month +1
str_date = str(year)+'-'+month+'-'+ date

# cur_date_1 = cur_date - relativedelta(month = cur_date.month-1)
cur_date_1 = cur_date - relativedelta(month = cur_date.month-1)

if len(str(cur_date_1.day)) == 2:
    date_1wkLTR = str(cur_date_1.day)
else:
    date_1wkLTR = '0'+ str(cur_date_1.day)

if len(str(cur_date_1.month)) == 2:
    month_1wkLTR = str(cur_date_1.month)
else:
    month_1wkLTR = '0' + str(cur_date_1.month)
year_1wkLTR = cur_date_1.year

# dep_date_start = str(year_1wkLTR)+'-'+str(month_1wkLTR)+'-'+ '01'
dep_date_start = datetime.strftime((cur_date - relativedelta(months=1)), '%Y-%m') +"-01"

# print Calendar.monthrange(cur_date.year, cur_date.month+3)[1]
# cur_date_end = datetime(cur_date.year, cur_date.month+2, Calendar.monthrange(cur_date.year, cur_date.month+2)[1])
cur_date_end = cur_date + relativedelta(months=+4)

if len(str(cur_date_end.day)) == 2:
    date_1wkLTR = str(cur_date_end.day)
else:
    date_1wkLTR ='0' + str(cur_date_end.day)

if len(str(cur_date_end.month)) == 2:
    month_1wkLTR = str(cur_date_end.month)
else:
    month_1wkLTR ='0' + str(cur_date_end.month)

year_1wkLTR = cur_date_end.year
# dep_date_end = str(year_1wkLTR) +'-'+ month_1wkLTR + '-'+ date_1wkLTR
# dep_date_end = datetime.strftime(cur_date_end, '%Y-%m-%d')

dt_range = cur_date + relativedelta(months=+3)
dep_date_end = datetime.strftime(dt_range, '%Y-%m') + '-'+str(calendar.monthrange(dt_range.year, dt_range.month)[1])

print dep_date_end, dep_date_start, str_date

#ThirtyDate_year = cur_date_end.year
#dep_date_end = str(ThirtyDate_year)+'-'+ThirtyDate_month+'-'+ThirtyDate_date

@measure(JUPITER_LOGGER)
def main(pos,client):
    db = client[JUPITER_DB]
    # Monthly
        # == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == =
    # Updation of Significant / Non Sig Markets in both summary collections

    try:
        num = 1
        bulk_Summary = db.JUP_DB_Manual_Triggers_Module_Summary.initialize_unordered_bulk_op()
        cursor = list(db.JUP_DB_Market_Significance.find({"significance.name": "l.kumarasamy", 'pos' : pos },no_cursor_timeout=True))

        for sig in cursor:
            for x in sig['significance']:
                if x['name'] == "l.kumarasamy":
                    # print num
                    # print x
                    pos = sig['pos']
                    origin = sig['od'][0:3]
                    destination = sig['od'][3:6]
                    compartment = sig['compartment']
                    # print 1
                    bulk_Summary.find({
                        'dep_date': {'$gte': dep_date_start, '$lte': dep_date_end},
                        'pos.City': pos,
                        'origin.City': origin,
                        'destination.City': destination,
                        'compartment': compartment
                    }).update({'$set': {
                        'significant_flag' : x['significant_flag']
                    }})


                    if num % 1000 == 0:
                        try:
                            bulk_Summary.execute()
                            bulk_Summary = db.JUP_DB_Manual_Triggers_Module_Summary.initialize_unordered_bulk_op()
                        except Exception as error:
                            print error
                    # print 3
                    num= num +1
        try:
            bulk_Summary.execute()
        except Exception as error:
            print error
    except Exception as error:
        print error
    endTime = datetime.now()
    #'''

if __name__ == '__main__':
    db = client[JUPITER_DB]
    pos_list = db.JUP_DB_Manual_Triggers_Module_Summary.distinct('pos.City')
    for each_market in pos_list:
        main(each_market, client)
        # pass
        # print each_market
    # main("CMB")
