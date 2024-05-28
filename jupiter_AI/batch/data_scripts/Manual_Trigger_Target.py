from __future__ import division
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
import datetime as dt
import time
from dateutil.relativedelta import relativedelta
import global_variable as var
import sys
from jupiter_AI import client, JUPITER_DB, JUPITER_LOGGER
from jupiter_AI.logutils import measure
from jupiter_AI.network_level_params import SYSTEM_DATE

# Connect mongodb db business layer
# db = client[JUPITER_DB]

script_start_time = time.clock()
cur_date = datetime.strptime(SYSTEM_DATE, '%Y-%m-%d')

if len(str(cur_date.day)) ==2:
    date = str(cur_date.day)
else:
    date = '0'+str(cur_date.day)

if len(str(cur_date.month)) ==2:
    month = str(cur_date.month)
else:
    month = '0'+str(cur_date.month)

this_month = cur_date.month
next_month = cur_date.month +1
year = cur_date.year
str_date = datetime.strftime(cur_date, '%Y-%m-%d')
print str_date

# Target
@measure(JUPITER_LOGGER)
def getDaysInMonth(month, year): # by default month convert with +1, So we can use -1 for get actual month

    date = datetime(year, month, 1)
    dates = dict()
    days =[]
    daysISO =[]
    while date.month == month :
        daysISO.append(date)
        day = date.day
        months = date.month # add 1 as getMonth returns 0 - 11
        year = date.year
        if months < 10 :
            months = "0" + str(months)
        if  day < 10 :
            day = "0" + str(day)
        days.append(str(year) + "-" + str(months) + "-" + str(day))
        date = date + relativedelta(days=1)
    dates['days'] = days
    dates['daysISO'] = daysISO
    return dates

dateformat = '%Y-%m-%d'

@measure(JUPITER_LOGGER)
def main(pos,snap_date,doc,client):
    db = client[JUPITER_DB]
    num = 1
    Bulk = db.JUP_DB_Manual_Triggers_Module.initialize_unordered_bulk_op()
    Bulk_summary = db.JUP_DB_Manual_Triggers_Module_Summary.initialize_unordered_bulk_op()
    str_date = snap_date
    print pos ,str_date
    cursor = db.JUP_DB_Target_OD.find({'pos':pos,'snap_date': str_date, 'month': {"$in":[this_month-1,this_month, next_month,next_month + 1]}},no_cursor_timeout=True)
    for x in cursor:
        combine_column  = "M"+str(int(x['departureMonth'][0:4]))+""+str(int(x['departureMonth'][4:6]));
        duration = doc[combine_column];
        tpax = x['pax']/duration;
        trev = x['revenue']/duration;
        tpax_1 = 0;
        trev_1 = 0;
        target = dict()
        target['pax'] = x['pax']
        target['avgFare'] = x['average_fare']
        target['revenue'] = x['revenue']
        target['pax_1'] = x['pax_1']
        target['avgFare_1'] = x['average_fare_1']
        target['revenue_1'] = x['revenue_1']
        target['significant_target'] = x['significant_target']
        snap_month = int(x['snap_date'][5:7])
        snap_year = int(x['snap_date'][0:4])
        dep_month = int(x['month'])
        dep_year = int(x['year'])
        od = x['origin']+ x['destination']
        pos =  dict()
        pos['Network'] = 'Network'
        pos['Cluster'] = x['Cluster']
        pos['Region'] = x['region']
        pos['Country'] = x['country']
        pos['City'] = x['pos']
        origin =  dict()
        origin['Network'] = x['Network_Origin']
        origin['Cluster'] = x['Cluster_Origin']
        origin['Region'] = x['Region_Origin']
        origin['Country'] = x['Country_Origin']
        origin['City'] = x['origin']
        destination = dict()
        destination['Network'] = x['Network_Dest']
        destination['Cluster'] = x['Cluster_Dest']
        destination['Region'] = x['Region_Dest']
        destination['Country'] = x['Country_Dest']
        destination['City'] = x['destination']
        compartment = dict()
        compartment['compartment'] = x['compartment']
        compartment['all'] = 'all'
        trx_date_ISO = datetime.strptime(x['snap_date'], dateformat)
        dates = getDaysInMonth(dep_month, dep_year)
        market_combined = x['pos'] + x['od'] + x['compartment']
        for i in range(len(dates['days'])):

            Bulk.find({
                      'pos.City': pos['City'],
                      'origin.City': origin['City'],
                      'destination.City': destination['City'],
                      'compartment.compartment': compartment['compartment'],
                      'dep_date': dates['days'][i],
                      'dep_date_ISO': dates['daysISO'][i],
                      # trx_date: x.snap_date,
            }).upsert().update(
            {
            '$set': {
              'od': od,
              'pos': pos,
              'origin': origin,
              'destination': destination,
              'compartment': compartment,
              'dep_date': dates['days'][i],
              'dep_date_ISO': dates['daysISO'][i],
              'dep_month': dep_month,
              'dep_year': dep_year,
              'target': target,
              'market_combined':market_combined
            }})

            Bulk_summary.find({
                'dep_date' : dates['days'][i],
                'pos.City':pos['City'],
                'origin.City':origin['City'],
                'destination.City':destination['City'],
                'compartment':compartment['compartment'],
            }).upsert().update(
            {
            '$set': {
                'dep_date' : dates['days'][i],
                'dep_month': dep_month,
                'dep_year': dep_year,
                "od": od,
                "pos": pos,
                "origin": origin,
                "destination": destination,
                "compartment": compartment['compartment'],
                "target_pax":x['pax'],
                "target_revenue" : x['revenue'],
                "target_avgFare" : x['average_fare'],
                "target_pax_1":x['pax_1'],
                "target_revenue_1" : x['revenue_1'],
                "target_avgFare_1" : x['average_fare_1'],
                "significant_target" : x['significant_target'],
                "prorate_target_pax" : tpax,
                "prorate_target_revenue" : trev,
                "prorate_target_pax_1" : tpax_1,
                "prorate_target_revenue_1" : trev_1,
            }})
            if num % 1000 == 0:
                try:
                    Bulk.execute()
                    Bulk_summary.execute()
                    Bulk = db.JUP_DB_Manual_Triggers_Module.initialize_unordered_bulk_op()
                    Bulk_summary = db.JUP_DB_Manual_Triggers_Module_Summary.initialize_unordered_bulk_op()
                except Exception as bwe:
                    print(bwe)
            num=num+1
        dates = None
    try:
        Bulk.execute()
        Bulk_summary.execute()
    except Exception as bwe:
        print(bwe)


if __name__ == '__main__':
    db = client[JUPITER_DB]
    doc = db.JUP_DB_Target_OD.distinct('snap_date')
    str_date = doc[len(doc) - 1]
    pos_list = list(db.JUP_DB_Target_OD.distinct('pos', {'snap_date': str_date}))
    doc_1 = dict()
    cal_cursor = db.JUP_DB_Calendar_Master.find({'duration' : {'$ne' : None}})
    for x in cal_cursor:
        doc_1[x['combine_column']] =  x['duration']
    for pos in pos_list:
        main(pos,str_date,doc_1,client)
