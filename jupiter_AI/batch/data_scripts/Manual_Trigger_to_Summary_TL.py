import copy
import json
import pymongo
import calendar
from pymongo.errors import BulkWriteError
from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne
import collections
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
import calendar as Calendar
from datetime import date
from datetime import timedelta
import datetime as dt
import time

from jupiter_AI import client, JUPITER_DB, JUPITER_LOGGER
from jupiter_AI.logutils import measure
from jupiter_AI.network_level_params import SYSTEM_DATE

# Connect mongodb db business layer
# db = client[JUPITER_DB]


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
cur_date_end = cur_date_1 + relativedelta(months=+3)

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

# var str_date = "2018-04-20";
# var dep_date_start = "2018-03-01";
# var dep_date_end = "2018-06-31";"""
@measure(JUPITER_LOGGER)
def main(pos, client):
    db = client[JUPITER_DB]
    print pos
    summary_YJ_pipeline = ''
    summary_MT_pipeline = ''
    summary_YJ1_pipeline = ''
    summary_MT1_pipeline = ''

    summary_MT_pipeline = [
        {'$match': {
            'dep_date': {'$gte': dep_date_start, '$lte': dep_date_end},
            'pos.City': pos,
            'compartment' : {'$ne' : 'TL'}
            # "origin.City": od[:3],
            # "destination.City": od[3:]
        }},
        {'$group': {
            '_id': {
                'dep_month': '$dep_month',
                'dep_date': '$dep_date',
                #'dep_date_ISO': '$dep_date_ISO',
                'dep_year': '$dep_year',
                'pos': '$pos.City',
                'od': '$od',
                #psuedo_od: '$psuedo_od',
                'origin': '$origin.City',
                'destination': '$destination.City'
                },
                'pos': {'$first': '$pos'},'dep_date_ISO': {'$first': '$dep_date_ISO'},
                'origin': {'$first': '$origin'},
                'destination': {'$first': '$destination'},
                'psuedo_od': {'$first': '$psuedo_od'},
                'book_pax': {'$sum': '$book_pax'},
                'book_ticket': {'$sum': '$book_ticket'},
                'book_pax_1': {'$sum': '$book_pax_1'},
                'book_ticket_1': {'$sum': '$book_ticket_1'},
                'sale_pax': {'$sum': '$sale_pax'},
                'sale_revenue': {'$sum': '$sale_revenue'},
                'sale_pax_1': {'$sum': '$sale_pax_1'},
                'sale_revenue_1': {'$sum': '$sale_revenue_1'},
                'flown_pax': {'$sum': '$flown_pax'},
                'flown_revenue': {'$sum': '$flown_revenue'},
                'flown_pax_1': {'$sum': '$flown_pax_1'},
                'flown_revenue_1': {'$sum': '$flown_revenue_1'},
                'book_snap_pax': {'$sum': '$book_snap_pax'},
                'sale_snap_pax': {'$sum': '$sale_snap_pax'},
                'flown_snap_pax': {'$sum': '$flown_snap_pax'},
                'book_snap_revenue': {'$sum': '$book_snap_revenue'},
                'sale_snap_revenue': {'$sum': '$sale_snap_revenue'},
                'flown_snap_revenue': {'$sum': '$flown_snap_revenue'},
                'target_pax': {'$sum': '$target_pax'},
                'target_avgFare': {'$sum': '$target_avgFare'},
                'target_revenue': {'$sum': '$target_revenue'},
                'target_pax_1': {'$sum': '$target_pax_1'},
                'target_avgFare_1': {'$sum': '$target_avgFare_1'},
                'target_revenue_1': {'$sum': '$target_revenue_1'},
                'distance': {'$max': '$distance'},
                'distance_sale': {'$sum': '$distance_sale'},
                'distance_sale_1': {'$sum': '$distance_sale_1'},
                'distance_flown': {'$sum': '$distance_flown'},
                'distance_flown_1': {'$sum': '$distance_flown_1'},
                'distance_Target_Pax': {'$sum': '$distance_Target_Pax'},
                'capacity': {'$sum': '$capacity'},
                'capacity_1': {'$sum': '$capacity_1'},
                'forecast_pax': {'$sum': '$forecast_pax'},
                'forecast_avgFare': {'$sum': '$forecast_avgFare'},
                'forecast_revenue': {'$sum': '$forecast_revenue'},
                'book_paxCapaAdj': {'$sum': '$book_paxCapaAdj'},
                'book_ticketCapaAdj': {'$sum': '$book_ticketCapaAdj'},
                'sale_paxCapaAdj': {'$sum': '$sale_paxCapaAdj'},
                'sale_revenueCapaAdj': {'$sum': '$sale_revenueCapaAdj'},
                'flown_paxCapaAdj': {'$sum': '$flown_paxCapaAdj'},
                'flown_revenueCapaAdj': {'$sum': '$flown_revenueCapaAdj'},
                "prorate_target_pax": {'$sum': '$prorate_target_pax'},
                "prorate_target_revenue": {'$sum': '$prorate_target_revenue'},
                "prorate_target_pax_1": {'$sum': '$prorate_target_pax_1'},
                "prorate_target_revenue_1": {'$sum': '$prorate_target_revenue_1'},
                "prorate_forecast_pax": {'$sum': '$prorate_forecast_pax'},
                "prorate_forecast_revenue": {'$sum': '$prorate_forecast_revenue'},
                'significant_target': {'$max': '$significant_target'},
                "leg1_pax": {'$sum': "$leg1_pax"},
                "leg1_pax_1": {'$sum': "$leg1_pax_1"},
                "leg1_capacity": {'$sum': "$leg1_capacity"},
                "leg1_capacity_1": {'$sum': "$leg1_capacity_1"},
                "leg1_bookings": {'$sum': "$leg1_bookings"},
                "leg1_bookings_1": {'$sum': "$leg1_bookings_1"},
                "leg2_pax": {'$sum': "$leg2_pax"},
                "leg2_pax_1": {'$sum': "$leg2_pax_1"},
                "leg2_capacity": {'$sum': "$leg2_capacity"},
                "leg2_capacity_1": {'$sum': "$leg2_capacity_1"},
                "leg2_bookings": {'$sum': "$leg2_bookings"},
                "leg2_bookings_1": {'$sum': "$leg2_bookings_1"},
                "leg1_forecastpax": {'$sum': "$leg1_forecastpax"},
                "leg1_forecastpax_1": {'$sum': "$leg1_forecastpax_1"},
                "leg2_forecastpax": {'$sum': "$leg2_forecastpax"},
                "leg2_forecastpax_1": {'$sum': "$leg2_forecastpax_1"}
        }},
        {'$project': {
        'combine_column_mt': {'$concat': ['$_id.dep_date', '$pos.City', '$_id.od', 'TL']},
        'dep_month': '$_id.dep_month',
        'dep_date': '$_id.dep_date',
        'dep_date_ISO': '$dep_date_ISO',
        'dep_year': '$_id.dep_year',
        'pos': '$pos',
        'od': '$_id.od',
        'psuedo_od': '$psuedo_od',
        'origin': '$origin',
        'destination': '$destination',
        'compartment': "TL",
        'book_pax': '$book_pax',
        'book_ticket': '$book_ticket',
        'book_pax_1': '$book_pax_1',
        'book_ticket_1': '$book_ticket_1',
        'sale_pax': '$sale_pax',
        'sale_revenue': '$sale_revenue',
        'sale_pax_1': '$sale_pax_1',
        'sale_revenue_1': '$sale_revenue_1',
        'flown_pax': '$flown_pax',
        'flown_revenue': '$flown_revenue',
        'flown_pax_1': '$flown_pax_1',
        'flown_revenue_1': '$flown_revenue_1',
        'book_snap_pax': '$book_snap_pax',
        'sale_snap_pax': '$sale_snap_pax',
        'flown_snap_pax': '$flown_snap_pax',
        'book_snap_revenue': '$book_snap_revenue',
        'sale_snap_revenue': '$sale_snap_revenue',
        'flown_snap_revenue': '$flown_snap_revenue',
        'target_pax': '$target_pax',
        'target_avgFare_1': {'$cond': [{'$ne': ["$target_pax_1", 0]}, {'$divide': ['$target_revenue_1', '$target_pax_1']}, 0]},
        'target_avgFare': {'$cond': [{'$ne': ["$target_pax", 0]}, {'$divide': ['$target_revenue', '$target_pax']}, 0]},
        # target_avgFare: '$target_avgFare',
        'target_revenue': '$target_revenue',
        'target_pax_1': '$target_pax_1',
        'significant_target': '$significant_target',
        # target_avgFare_1: '$target_avgFare_1',
        'target_revenue_1': '$target_revenue_1',
        'distance': '$distance',
        'distance_sale': '$distance_sale',
        'distance_sale_1': '$distance_sale_1',
        'distance_flown': '$distance_flown',
        'distance_flown_1': '$distance_flown_1',
        'distance_Target_Pax': '$distance_Target_Pax',
        'capacity': '$capacity',
        'capacity_1': '$capacity_1',
        'forecast_pax': '$forecast_pax',
        'forecast_avgFare': {'$cond': [{'$ne': ["$forecast_pax", 0]}, {'$divide': ['$forecast_revenue', '$forecast_pax']}, 0]},
        # forecast_avgFare: '$forecast_avgFare',
        'forecast_revenue': '$forecast_revenue',
        'book_paxCapaAdj': '$book_paxCapaAdj',
        'book_ticketCapaAdj': '$book_ticketCapaAdj',
        'sale_paxCapaAdj': '$sale_paxCapaAdj',
        'sale_revenueCapaAdj': '$sale_revenueCapaAdj',
        'flown_paxCapaAdj': '$flown_paxCapaAdj',
        'flown_revenueCapaAdj': '$flown_revenueCapaAdj',
        "prorate_target_pax": '$prorate_target_pax',
        "prorate_target_revenue": '$prorate_target_revenue',
        "prorate_target_pax_1": '$prorate_target_pax_1',
        "prorate_target_revenue_1": '$prorate_target_revenue_1',
        "prorate_forecast_pax": '$prorate_forecast_pax',
        "prorate_forecast_revenue": '$prorate_forecast_revenue',
        "leg1_pax": "$leg1_pax",
        "leg1_pax_1": "$leg1_pax_1",
        "leg1_capacity": "$leg1_capacity",
        "leg1_capacity_1": "$leg1_capacity_1",
        "leg1_bookings": "$leg1_bookings",
        "leg1_bookings_1": "$leg1_bookings_1",
        "leg2_pax": "$leg2_pax",
        "leg2_pax_1": "$leg2_pax_1",
        "leg2_capacity": "$leg2_capacity",
        "leg2_capacity_1": "$leg2_capacity_1",
        "leg2_bookings": "$leg2_bookings",
        "leg2_bookings_1": "$leg2_bookings_1",
        "leg1_forecastpax": "$leg1_forecastpax",
        "leg1_forecastpax_1": "$leg1_forecastpax_1",
        "leg2_forecastpax": "$leg2_forecastpax",
        "leg2_forecastpax_1": "$leg2_forecastpax_1"
    }},
    # {'$out': 'Temp_Summary_TL'}
    ]
    cursor = db.JUP_DB_Manual_Triggers_Module_Summary.aggregate(summary_MT_pipeline, allowDiskUse =True)

    num = 1
    Bulk = db.JUP_DB_Manual_Triggers_Module_Summary.initialize_ordered_bulk_op()
    # cursor = db.Temp_Summary_TL.find(no_cursor_timeout=True)
    print "Comp TL"
    for x in cursor:
        # print x
        try:
            del x['_id']
            Bulk.find({
                "dep_date": x['dep_date'],
                "pos.City": x['pos']['City'],
                "origin.City": x['origin']['City'],
                "destination.City": x['destination']['City'],
                "compartment": x['compartment'],
            }).upsert().update({'$set': x})

            if num % 1000 == 0:
                Bulk.execute()
                Bulk = db.JUP_DB_Manual_Triggers_Module_Summary.initialize_ordered_bulk_op()
            num = num + 1
        except Exception as bwe:
            print(bwe)
    try:
        Bulk.execute()
        pass
    except Exception as bwe:
        print(bwe)



if __name__ == '__main__':
    # doc = db.JUP_DB_Target_OD.distinct('snap_date')
    # str_date = doc[len(doc) - 1]
    # pos_list = list(db.JUP_DB_Target_OD.distinct('pos', {'snap_date': str_date}))
    # for pos in pos_list:
    #     main(pos,od)
    db = client[JUPITER_DB]
    market_list = db.JUP_DB_Manual_Triggers_Module.distinct('pos.City')
    # market_list = ["RUH"]
    for each_market in market_list:
        main(each_market,client)
        # pass
        # print each_market

