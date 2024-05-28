from __future__ import division
import copy
import json
import pymongo
import calendar
from pymongo.errors import BulkWriteError
from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne
import collections
# import pandas as pd
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

# Network parameter #
script_start_time = time.clock()
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
str_date = str(year) + '-' + str(month) + date

dateformat = '%Y-%m-%d'
dateformat_y_m = '%Y-%m'
yearformat = '%Y'
monthformat = '%m'


@measure(JUPITER_LOGGER)
def getDaysInMonth(month, year):
    date = datetime(year, month, 1)
    dates = dict()
    days = []
    daysISO = []
    while date.month == month:
        daysISO.append(date)
        day = date.day
        # print (day)
        months = date.month
        year = date.year
        if months < 10:
            months = "0" + str(months)
        if day < 10:
            day = "0" + str(day)
        days.append(str(year) + "-" + str(months) + "-" + str(day))
        date = date + relativedelta(days=1)
    dates['days'] = days
    dates['daysISO'] = daysISO
    # print dates
    return dates


@measure(JUPITER_LOGGER)
def main(pos, snap_date, doc, client):
    print pos
    db = client[JUPITER_DB]
    num = 1
    str_date = snap_date
    # doc = db.JUP_DB_Forecast_OD.distinct('snap_date')
    # str_date = doc[len(doc)-1]
    Bulk = db.JUP_DB_Manual_Triggers_Module.initialize_unordered_bulk_op()
    Bulk_summary = db.JUP_DB_Manual_Triggers_Module_Summary.initialize_unordered_bulk_op()
    cursor = db.JUP_DB_Forecast_OD.find({"snap_date": str_date, 'pos': pos}, no_cursor_timeout=True)
    # print list(cursor)

    for x in cursor:
        # print x
        try:
            combine_column = "M" + str(int(x['departureMonth'][0:4])) + "" + str(int(x['departureMonth'][4:6]));
            duration = doc[combine_column];
            # print x
            fpax = x['pax'] / duration;
            frev = x['revenue'] / duration;
            forecast = dict()
            forecast['pax'] = x['pax']
            forecast['avgFare'] = x['avgFare']
            forecast['revenue'] = x['revenue']
            forecast['pax_1'] = x['pax_1']
            forecast['avgFare_1'] = x['avgFare_1']
            forecast['revenue_1'] = x['revenue_1']
            snap_month = int(x['snap_date'][5:7])
            snap_year = int(x['snap_date'][0:4])
            dep_month = int(x['Month'])
            dep_year = int(x['Year'])
            pos = dict()
            od = x['origin'] + x['destination']
            pos['Network'] = 'Network'
            pos['Cluster'] = x['Cluster']
            pos['Region'] = x['region']
            pos['Country'] = x['country']
            pos['City'] = x['pos']
            origin = dict()
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
            # print dates
            for i in range(len(dates['days'])):
                # print("od", od,
                #    "pos",pos,
                #    "origin", origin,
                #    "destination", destination,
                #    "compartment", compartment,
                #    "dep_date", dates['days'][i],
                #    "dep_date_ISO", dates['daysISO'][i],
                #    "dep_month", dep_month,
                #    "dep_year", dep_year,
                #    "forecast", forecast)

                # db.Test.insert({"date" : dates['daysISO'][i]})
                Bulk.find({
                    'pos.City': pos['City'],
                    'od': od,
                    'compartment.compartment': compartment['compartment'],
                    "dep_date": dates['days'][i],
                    "dep_date_ISO": dates['daysISO'][i],
                    # trx_date: x.snap_date,
                }).upsert().update(
                    {
                        "$set": {
                            "od": od,
                            "pos": pos,
                            "origin": origin,
                            "destination": destination,
                            "compartment": compartment,
                            "dep_date": dates['days'][i],
                            "dep_date_ISO": dates['daysISO'][i],
                            # trx_date: x.snap_date,
                            # trx_date_ISO: trx_date_ISO,
                            # trx_month: snap_month,
                            # trx_year: snap_year,
                            "dep_month": dep_month,
                            "dep_year": dep_year,
                            "forecast": forecast,
                            'market_combined': market_combined
                        }
                    }
                )

                Bulk_summary.find({
                    "dep_date": dates['days'][i],
                    'pos.City': pos['City'],
                    'origin.City': origin['City'],
                    'destination.City': destination['City'],
                    'compartment': compartment['compartment'],
                    # trx_date: x.snap_date,
                }).upsert().update(
                    {
                        "$set": {
                            'dep_date': dates['days'][i],
                            "dep_month": dep_month,
                            "dep_year": dep_year,
                            "od": od,
                            "pos": pos,
                            "origin": origin,
                            "destination": destination,
                            "compartment": compartment['compartment'],
                            "forecast_pax": x['pax'],
                            "forecast_avgFare": x['avgFare'],
                            "forecast_revenue": x['revenue'],
                            "forecast_pax_1": x['pax_1'],
                            "forecast_avgFare_1": x['avgFare_1'],
                            "forecast_revenue_1": x['revenue_1'],
                            "prorate_forecast_pax": fpax,
                            "prorate_forecast_revenue": frev,
                        }
                    }
                )
            if num % 1000 == 0:
                try:
                    Bulk.execute()
                    Bulk_summary.execute()
                    Bulk = db.JUP_DB_Manual_Triggers_Module.initialize_unordered_bulk_op()
                    Bulk_summary = db.JUP_DB_Manual_Triggers_Module_Summary.initialize_unordered_bulk_op()
                except Exception as bwe:
                    print(bwe)
            num = num + 1
            dates = None
        except Exception as bwe:
            print(bwe)
    try:
        Bulk.execute()
        Bulk_summary.execute()
    except Exception as bwe:
        print(bwe)


if __name__ == '__main__':
    db = client[JUPITER_DB]
    doc = db.JUP_DB_Forecast_OD.distinct('snap_date')
    str_date = doc[len(doc) - 1]
    pos_list = list(db.JUP_DB_Forecast_OD.distinct('pos', {'snap_date': str_date}))
    doc_1 = dict()
    cal_cursor = db.JUP_DB_Calendar_Master.find({'duration': {'$ne': None}})

    for x in cal_cursor:
        doc_1[x['combine_column']] = x['duration']
    for pos in pos_list:
        main(pos, str_date, doc_1, client)
