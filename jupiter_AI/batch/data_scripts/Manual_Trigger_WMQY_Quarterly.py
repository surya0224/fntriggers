
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

#dd = datetime.now()
#dd = dd + relativedelta(days =+30)

#if len(str(cur_date_end.day)) == 2:
#    ThirtyDate_date = str(cur_date_end.day)
#else:
#    ThirtyDate_date = '0'+str(cur_date_end.day)

#if len(str(cur_date_end.month)) ==2:
#    ThirtyDate_month = str(cur_date_end.month)
#else:
#    ThirtyDate_month = '0'+str(cur_date_end.month)

#ThirtyDate_year = cur_date_end.year
#dep_date_end = str(ThirtyDate_year)+'-'+ThirtyDate_month+'-'+ThirtyDate_date

@measure(JUPITER_LOGGER)
def main(pos,client):
    db = client[JUPITER_DB]
    #    #########################################################################################################
    # quarter
    try:

        quarter_pipeline = [
        {'$match': {
            'dep_date': {'$gte': dep_date_start, '$lte': dep_date_end},
            'pos.City': pos,
            'compartment': {'$ne': ['TL']}
        }},
        {'$addFields': {"quarter": {'$cond':[{'$lte':[{'$month':"$dep_date_ISO"}, 3]}, 1,{'$cond':[{'$lte':[{'$month':"$dep_date_ISO"}, 6]}, 2, {'$cond':[{'$lte':[{'$month':"$dep_date_ISO"}, 9]}, 3, 4]}]}]}
        }},
        {'$group': {
            '_id': {
                "dep_year": '$dep_year',
                "dep_month": '$dep_month',
                "quarter": '$quarter',
                "pos": "$pos.City",
                "od": "$od",
                "destination": "$destination.City",
                "origin": "$origin.City",
                "compartment": "$compartment",
            },
            'popular_fare_detail': {'$addToSet':"$popular_fare_detail"},
            'farebasis': {'$addToSet':"$farebasis"},
            'pos': {'$first':'$pos'},
            'origin': {'$first':'$origin'},
            'destination': {'$first':'$destination'},
            'psuedo_od': {'$first':'$psuedo_od'},
            "pax": {'$sum': {'$cond': [{'$gte':['$dep_date', system_date]},
            '$sale_pax',
            '$flown_pax'
            ]
            }},
            "revenue": {'$sum': {'$cond': [{'$gte':['$dep_date', system_date]},
            '$sale_revenue',
            '$flown_revenue'
            ]
        }},
        "pax_1": {'$sum': {'$cond': [{'$gte':['$dep_date', system_date]},
        '$sale_pax_1',
        '$flown_pax_1'
        ]
        }},
        "revenue_1": {'$sum': {'$cond': [{'$gte':['$dep_date', system_date]},
        '$sale_revenue_1',
        '$flown_revenue_1'
        ]
        }},
        'paxCapaAdj':{'$sum': {'$multiply':[{'$cond': [{'$gte':['$dep_date', system_date]},
        '$sale_pax',
        '$flown_pax'
        ]
        }, {'$cond': [{ '$and':[{'$ne': ["$capacity", 0]}, {'$ne': ["$capacity_1", 0]}]},
        {'$divide':["$capacity_1", "$capacity"]},
        1]}]}},

        'revenueCapaAdj':{'$sum': {'$multiply':[{'$cond': [{'$gte':['$dep_date', system_date]},
        '$sale_revenue',
        '$flown_revenue'
        ]
        }, {'$cond': [{ '$and':[{'$ne': ["$capacity", 0]}, {'$ne': ["$capacity_1", 0]}]},
        {'$divide':["$capacity_1", "$capacity"]},
        1]}]}},
        "pax_distance": {'$sum': {'$multiply': [{'$cond': [{'$gte':['$dep_date', system_date]},
        '$sale_pax',
        '$flown_pax'
        ]}
        , '$distance']
        }},

        "pax_distance_1": {'$sum': {'$multiply': [{'$cond': [{'$gte':['$dep_date', system_date]},
        '$sale_pax_1',
        '$flown_pax_1'
        ]}
        , '$distance']
        }},

        "dep_date_start": {'$min': "$dep_date"},
        "dep_date_end": {'$max': "$dep_date"},
        "book_pax":{'$sum':"$book_pax"},
        "book_ticket":{'$sum':"$book_ticket"},
        "book_pax_1":{'$sum':"$book_pax_1"},
        "book_ticket_1":{'$sum':"$book_ticket_1"},
        "sale_pax":{'$sum':"$sale_pax"},
        "sale_revenue":{'$sum':"$sale_revenue"},
        "sale_pax_1":{'$sum':"$sale_pax_1"},
        "sale_revenue_1":{'$sum':"$sale_revenue_1"},
        "flown_pax":{'$sum':"$flown_pax"},
        "flown_revenue":{'$sum':"$flown_revenue"},
        "flown_pax_1":{'$sum':"$flown_pax_1"},
        "flown_revenue_1":{'$sum':"$flown_revenue_1"},
        "book_snap_pax":{'$sum':"$book_snap_pax"},
        "sale_snap_pax":{'$sum':"$sale_snap_pax"},
        "flown_snap_pax":{'$sum':"$flown_snap_pax"},
        "book_snap_revenue":{'$sum':"$book_snap_revenue"},
        "sale_snap_revenue":{'$sum':"$sale_snap_revenue"},
        "flown_snap_revenue":{'$sum':"$flown_snap_revenue"},
        'significant_target':{'$max': '$significant_target'},
        'significant_flag':{'$max': '$significant_flag'},
        "target_pax":{'$max':"$target_pax"},
        "target_avgFare":{'$max':"$target_avgFare"},
        "target_revenue":{'$max':"$target_revenue"},
        "target_pax_1":{'$max':"$target_pax_1"},
        "target_avgFare_1":{'$max':"$target_avgFare_1"},
        "target_revenue_1":{'$max':"$target_revenue_1"},
        "distance_only":  {'$max': "$distance"},
        "capacity":{'$sum':"$capacity"},
        "capacity_1":{'$sum':"$capacity_1"},
        "forecast_pax":{'$max':"$forecast_pax"},
        "forecast_avgFare":{'$max':"$forecast_avgFare"},
        "forecast_revenue":{'$max':"$forecast_revenue"},
        # "book_paxCapaAdj":{'$sum':"$book_paxCapaAdj"},
        # "book_ticketCapaAdj":{'$sum':"$book_ticketCapaAdj"},
        # "sale_paxCapaAdj":{'$sum':"$sale_paxCapaAdj"},
        # "sale_revenueCapaAdj":{'$sum':"$sale_revenueCapaAdj"},
        # "flown_paxCapaAdj":{'$sum':"$flown_paxCapaAdj"},
        # "flown_revenueCapaAdj":{'$sum':"$flown_revenueCapaAdj"},
        "leg1_pax": {'$sum':"$leg1_pax"},
        "leg1_pax_1": {'$sum':"$leg1_pax_1"},
        "leg1_bookings": {'$sum':"$leg1_bookings"},
        "leg1_bookings_1": {'$sum':"$leg1_bookings_1"},
        "leg1_forecast_pax": {'$sum':"$leg1_forecastpax"},
        "leg1_forecast_pax_1": {'$sum':"$leg1_forecastpax_1"},
        "leg1_capacity": {'$sum':"$leg1_capacity"},
        "leg1_capacity_1": {'$sum':"$leg1_capacity_1"},
        "leg2_pax": {'$sum':"$leg2_pax"},
        "leg2_pax_1": {'$sum':"$leg2_pax_1"},
        "leg2_bookings": {'$sum':"$leg2_bookings"},
        "leg2_bookings_1": {'$sum':"$leg2_bookings_1"},
        "leg2_forecast_pax": {'$sum':"$leg2_forecastpax"},
        "leg2_forecast_pax_1": {'$sum':"$leg2_forecastpax_1"},
        "leg2_capacity": {'$sum':"$leg2_capacity"},
        "leg2_capacity_1": {'$sum':"$leg2_capacity_1"},
        }},

        { '$addFields': {
            'farebasis': {'$reduce': {'input': '$farebasis',
                                     'initialValue': [],
                                    'in': { '$concatArrays': ["$$value", "$$this"]}
        }}
        }},
        { '$addFields': {
            'popular_fare_detail': {'$reduce': {
                                               'input': '$popular_fare_detail',
                                               'initialValue': [],
                                                'in': { '$concatArrays': ["$$value", "$$this"]}
        }}
        }},
        {'$group': {
            '_id': {
                "dep_year": '$_id.dep_year',
                "quarter": '$_id.quarter',
                "pos": "$pos.City",
                "od": "$_id.od",
                "destination": "$destination.City",
                "origin": "$origin.City",
                "compartment": "$_id.compartment",
            },
            "dep_month": {'$min': '$_id.dep_month'},
        'popular_fare_detail': {'$addToSet':"$popular_fare_detail"},
        'farebasis': {'$addToSet':"$farebasis"},
        'pos': {'$first':'$pos'},
        'origin': {'$first':'$origin'},
        'destination': {'$first':'$destination'},
        'psuedo_od': {'$first':'$psuedo_od'},
        'pax': {'$sum': '$pax'},
        'pax_1': {'$sum': '$pax_1'},
        'revenue': {'$sum': '$revenue'},
        'revenue_1': {'$sum': '$revenue_1'},
        "dep_date_start": {'$min': "$dep_date_start"},
        "dep_date_end": {'$max': "$dep_date_end"},
        "book_pax":{'$sum':"$book_pax"},
        "book_ticket":{'$sum':"$book_ticket"},
        "book_pax_1":{'$sum':"$book_pax_1"},
        "book_ticket_1":{'$sum':"$book_ticket_1"},
        "sale_pax":{'$sum':"$sale_pax"},
        "sale_revenue":{'$sum':"$sale_revenue"},
        "sale_pax_1":{'$sum':"$sale_pax_1"},
        "sale_revenue_1":{'$sum':"$sale_revenue_1"},
        "flown_pax":{'$sum':"$flown_pax"},
        "flown_revenue":{'$sum':"$flown_revenue"},
        "flown_pax_1":{'$sum':"$flown_pax_1"},
        "flown_revenue_1":{'$sum':"$flown_revenue_1"},
        "book_snap_pax":{'$sum':"$book_snap_pax"},
        "sale_snap_pax":{'$sum':"$sale_snap_pax"},
        "flown_snap_pax":{'$sum':"$flown_snap_pax"},
        "book_snap_revenue":{'$sum':"$book_snap_revenue"},
        "sale_snap_revenue":{'$sum':"$sale_snap_revenue"},
        "flown_snap_revenue":{'$sum':"$flown_snap_revenue"},
        'significant_target':{'$max': '$significant_target'},
        'significant_flag':{'$max': '$significant_flag'},
        "target_pax":{'$sum':"$target_pax"},
        "target_avgFare":{'$avg':"$target_avgFare"},
        "target_revenue":{'$sum':"$target_revenue"},
        "target_pax_1":{'$sum':"$target_pax_1"},
        "target_avgFare_1":{'$avg':"$target_avgFare_1"},
        "target_revenue_1":{'$sum':"$target_revenue_1"},
        "distance_only": {'$max': '$distance_only'},
        "capacity":{'$sum':"$capacity"},
        "capacity_1":{'$sum':"$capacity_1"},
        "forecast_pax":{'$sum':"$forecast_pax"},
        "forecast_avgFare":{'$avg':"$forecast_avgFare"},
        "forecast_revenue":{'$sum':"$forecast_revenue"},
        # "book_paxCapaAdj":{'$sum':"$book_paxCapaAdj"},
        # "book_ticketCapaAdj":{'$sum':"$book_ticketCapaAdj"},
        # "sale_paxCapaAdj":{'$sum':"$sale_paxCapaAdj"},
        # "sale_revenueCapaAdj":{'$sum':"$sale_revenueCapaAdj"},
        # "flown_paxCapaAdj":{'$sum':"$flown_paxCapaAdj"},
        # "flown_revenueCapaAdj":{'$sum':"$flown_revenueCapaAdj"},
        'pax_distance': {'$sum': '$pax_distance'},
        'pax_distance_1': {'$sum': '$pax_distance_1'},
        "paxCapaAdj":{'$sum':"$paxCapaAdj"},
        "revenueCapaAdj":{'$sum':"$revenueCapaAdj"},
        "leg1_pax": {'$sum':"$leg1_pax"},
        "leg1_pax_1": {'$sum':"$leg1_pax_1"},
        "leg1_bookings": {'$sum':"$leg1_bookings"},
        "leg1_bookings_1": {'$sum':"$leg1_bookings_1"},
        "leg1_forecast_pax": {'$sum':"$leg1_forecast_pax"},
        "leg1_forecast_pax_1": {'$sum':"$leg1_forecast_pax_1"},
        "leg1_capacity": {'$sum':"$leg1_capacity"},
        "leg1_capacity_1": {'$sum':"$leg1_capacity_1"},
        "leg2_pax": {'$sum':"$leg2_pax"},
        "leg2_pax_1": {'$sum':"$leg2_pax_1"},
        "leg2_bookings": {'$sum':"$leg2_bookings"},
        "leg2_bookings_1": {'$sum':"$leg2_bookings_1"},
        "leg2_forecast_pax": {'$sum':"$leg2_forecast_pax"},
        "leg2_forecast_pax_1": {'$sum':"$leg2_forecast_pax_1"},
        "leg2_capacity": {'$sum':"$leg2_capacity"},
        "leg2_capacity_1": {'$sum':"$leg2_capacity_1"},
        }},

        { '$addFields': {
            'farebasis': {'$reduce': {
                                     'input': '$farebasis',
                                     'initialValue': [],
                                    'in': { '$concatArrays': ["$$value", "$$this"]}
        }}
        }},
        { '$addFields': {
            'popular_fare_detail': {'$reduce': {
                                               'input': '$popular_fare_detail',
                                               'initialValue': [],
                                                'in': { '$concatArrays': ["$$value", "$$this"]}
        }}
        }},

        { '$unwind': {'path': "$popular_fare_detail", 'preserveNullAndEmptyArrays': True}},

        {'$sort': {
            'popular_fare_detail.carrier': 1, 'popular_fare_detail.currency': 1, 'popular_fare_detail.is_one_way': 1,
            'popular_fare_detail.frequency': -1, 'popular_fare_detail.fare': 1, 'popular_fare_detail.stop_over': 1
        }},

        {'$group':{
            '_id': {
                 # fare: '$popular_fare_detail.fare',
                          "dep_year": '$_id.dep_year',
                           "dep_month": '$_id.dep_month',
                          "pos": "$_id.pos",
                           "od": "$_id.od",
                           "destination": "$_id.destination",
                           "origin": "$_id.origin",
                            "compartment": "$_id.compartment",
                            'is_one_way': '$popular_fare_detail.is_one_way',
                            'carrier': '$popular_fare_detail.carrier',
                            'currency': '$popular_fare_detail.currency',
        },
        'outbound_fare_basis': { '$first':'$popular_fare_detail.outbound_fare_basis'},
        'tax': { '$first':'$popular_fare_detail.tax'},
        'observation_date': { '$first':'$popular_fare_detail.observation_date'},
        'frequency':{ '$first': '$popular_fare_detail.frequency'},
        'stop_over':{ '$first': '$popular_fare_detail.stop_over'},
        'fare': { '$first':'$popular_fare_detail.fare'},
        'doc': {'$first': '$$ROOT'}
        }},

        {'$group':{
            '_id': {
                 # fare: '$popular_fare_detail.fare',
                          "dep_year": '$_id.dep_year',
                    # "dep_month": '$_id.dep_month',
                        "pos": "$_id.pos",
                         "od": "$_id.od",
                         "destination": "$_id.destination",
                         "origin": "$_id.origin",
                         "compartment": "$_id.compartment",
        },
        'doc': {'$first': '$doc'},
        'popular_fare_detail': {'$push': {
            'is_one_way': '$_id.is_one_way',
            'carrier': '$_id.carrier',
            'outbound_fare_basis': '$outbound_fare_basis',
            'observation_date': '$observation_date',
            'tax': '$tax',
            'currency': '$_id.currency',
            'frequency': '$frequency',
            'stop_over': '$stop_over',
            'fare': '$fare'
        }}
        # doc: {'$first': '$$ROOT'}
        }},
        { '$unwind': {'path': "$doc.farebasis", 'preserveNullAndEmptyArrays': True}},
        {'$group': {
            '_id': {
                "dep_year": '$_id.dep_year',
                "dep_month": '$_id.dep_month',
                "pos": "$_id.pos",
                "od": "$_id.od",
                "destination": "$_id.destination",
                "origin": "$_id.origin",
                "compartment": "$_id.compartment",
                "farebasis": "$doc.farebasis.farebasis",
                "RBD": "$doc.farebasis.RBD",
                "currency": "$doc.farebasis.currency",
                "fareId": "$doc.farebasis.fareId",
            },
            "pax": {'$sum': "$doc.farebasis.pax"},
        "pax_1": {'$sum': "$doc.farebasis.pax_1"},
        "rev": {'$sum': "$doc.farebasis.rev"},
        "rev_1": {'$sum': "$doc.farebasis.rev_1"},
        "AIR_CHARGE": {'$sum': "$doc.farebasis.AIR_CHARGE"},
        'doc': {'$first': '$doc'},
        'popular_fare_detail': {'$first': '$popular_fare_detail'},
        }},
        {'$group': {
            '_id': {
                "dep_year": '$_id.dep_year',
                "dep_month": '$_id.dep_month',
                "pos": "$_id.pos",
                "od": "$_id.od",
                "destination": "$_id.destination",
                "origin": "$_id.origin",
                "compartment": "$_id.compartment"
            },
            'farebasis': {'$push': {
            'farebasis': '$_id.farebasis',
            "RBD": "$_id.RBD",
            "currency": "$_id.currency",
            "fareId": "$_id.fareId",
            "pax": "$pax",
            "pax_1": "$pax_1",
            "rev": "$rev",
            "rev_1": "$rev_1",
            "AIR_CHARGE": "$AIR_CHARGE",
        }},

        'doc': {'$first': '$doc'},
        'popular_fare_detail': {'$first': '$popular_fare_detail'},
        }},
        {'$project': {
            '_id': 0,
            'farebasis': '$farebasis',
            'popular_fare_detail': {'$cond': [{'$eq': [{'$arrayElemAt': ["$popular_fare_detail.tax", 0]}, None]}, None, '$popular_fare_detail']},
            "combine_column": {'$concat': ["Q", {
                "$substr": [
                    {
                        "$mod": [
                            "$doc._id.dep_year",
                            10000
                        ]
                    },
                    0,
                    4
                ]
                },
                   {
                       "$substr": [
                           {
                               "$mod": [
                                   "$doc._id.quarter",
                                   10000
                               ]
                           },
                           0,
                           2
                       ]
                   }]
            },
            "dep_year": '$doc._id.dep_year',
            "dep_month": '$doc.dep_month',
            "dep_date_start":'$doc.dep_date_start',
            "dep_date_end": '$doc.dep_date_end',
            "pos": "$doc.pos",
            "od": "$doc._id.od",
            "destination": "$doc.destination",
            "origin": "$doc.origin",
            "compartment": "$doc._id.compartment",
            "book_pax":"$doc.book_pax",
            'pax': '$doc.pax',
            'pax_1': '$doc.pax_1',
            'revenue': '$doc.revenue',
            'revenue_1': '$doc.revenue_1',
            'psuedo_od': '$doc.psuedo_od',
            "book_ticket":"$doc.book_ticket",
            "book_pax_1":"$doc.book_pax_1",
            "book_ticket_1":"$doc.book_ticket_1",
            "sale_pax":"$doc.sale_pax",
            "sale_revenue":"$doc.sale_revenue",
            "sale_pax_1":"$doc.sale_pax_1",
            "sale_revenue_1":"$doc.sale_revenue_1",
            "flown_pax":"$doc.flown_pax",
            "flown_revenue":"$doc.flown_revenue",
            "flown_pax_1":"$doc.flown_pax_1",
            "flown_revenue_1":"$doc.flown_revenue_1",
            "book_snap_pax":"$doc.book_snap_pax",
            "sale_snap_pax":"$doc.sale_snap_pax",
            "flown_snap_pax":"$doc.flown_snap_pax",
            "book_snap_revenue":"$doc.book_snap_revenue",
            "sale_snap_revenue":"$doc.sale_snap_revenue",
            "flown_snap_revenue":"$doc.flown_snap_revenue",
            'significant_target':'$doc.significant_target',
            'significant_flag':'$doc.significant_flag',
            "target_pax":"$doc.target_pax",
            "target_avgFare":"$doc.target_avgFare",
            "target_revenue":"$doc.target_revenue",
            "target_pax_1":"$doc.target_pax_1",
            "target_avgFare_1":"$doc.target_avgFare_1",
            "target_revenue_1":"$doc.target_revenue_1",
            "distance": "$doc.distance_only",
            "pax_distance":'$doc.pax_distance',
            "pax_distance_1":'$doc.pax_distance_1',
            "distance_Target_Pax":{'$multiply': [
            "$doc.distance_only", '$doc.target_pax']},
            'paxVLYR': {'$multiply': [{'$cond': [{ '$ne': ['$doc.pax_1', 0]}, {'$divide': [{'$subtract': ["$doc.paxCapaAdj", '$doc.pax_1']}, '$doc.pax_1']}, 0]}, 100]}, 'revenueVLYR': {'$multiply': [{'$cond': [{ '$ne': ['$doc.revenue_1', 0]}, {'$divide': [{'$subtract': ["$doc.revenueCapaAdj",
                                                                          '$doc.revenue_1']}, '$doc.revenue_1']}, 0]}, 100]},
            'yield': {'$cond': [{'$ne': ['$doc.pax_distance', 0]}, {'$multiply': [{'$divide': ['$doc.revenue', '$doc.pax_distance']}, 100]}, 0]},
            'yield_1': {'$cond': [{'$ne': ['$doc.pax_distance_1', 0]}, {'$multiply': [{'$divide': ['$doc.revenue_1', '$doc.pax_distance_1']}, 100]}, 0]},
            'paxVTGT': {'$multiply': [{'$cond': [{ '$ne': ["$doc.target_pax", 0]}, {'$divide': [{'$subtract': ["$doc.pax", "$doc.target_pax"]}, "$doc.target_pax"]}, 0]}, 100]},
            'revenueVTGT': {'$multiply': [{'$cond': [{ '$ne': ["$doc.target_revenue", 0]}, {'$divide': [{'$subtract': ["$doc.revenue", "$doc.target_revenue"]}, "$doc.target_revenue"]}, 0]}, 100]},
            "capacity":"$doc.capacity", "capacity_1":"$doc.capacity_1", "forecast_pax":"$doc.forecast_pax", "forecast_avgFare":"$doc.forecast_avgFare", "forecast_revenue":"$doc.forecast_revenue",
            "paxCapaAdj":"$doc.paxCapaAdj",
            "revenueCapaAdj":"$doc.revenueCapaAdj",
            "prorate_target_pax": "$doc.target_pax",
            "prorate_target_revenue": "$doc.target_revenue",
            "prorate_target_pax_1": "$doc.target_pax_1",
            "prorate_target_revenue_1": "$doc.target_revenue_1",
            "prorate_forecast_pax": "$doc.forecast_pax",
            "prorate_forecast_revenue": "$doc.forecast_revenue",
            "leg1_pax": "$doc.leg1_pax",
            "leg1_pax_1": "$doc.leg1_pax_1",
            "leg1_bookings": "$doc.leg1_bookings",
            "leg1_bookings_1": "$doc.leg1_bookings_1",
            "leg1_forecastpax": "$doc.leg1_forecast_pax",
            "leg1_forecastpax_1": "$doc.leg1_forecast_pax_1",
            "leg1_capacity": "$doc.leg1_capacity",
            "leg1_capacity_1": "$doc.leg1_capacity_1",
            "leg2_pax": "$doc.leg2_pax",
            "leg2_pax_1": "$doc.leg2_pax_1",
            "leg2_bookings": "$doc.leg2_bookings",
            "leg2_bookings_1": "$doc.leg2_bookings_1",
            "leg2_forecastpax": "$doc.leg2_forecast_pax",
            "leg2_forecastpax_1": "$doc.leg2_forecast_pax_1",
            "leg2_capacity": "$doc.leg2_capacity",
            "leg2_capacity_1": "$doc.leg2_capacity_1",
        }},
        # {'$out': 'Temp_MT_Summary_YJ'}
        ]

        cursor = db.JUP_DB_Manual_Triggers_Module_Summary.aggregate(quarter_pipeline, allowDiskUse=True)
        num = 1

        bulkSummary = db.JUP_DB_Manual_Triggers_Module_Monthly_Weekly_Summary.initialize_unordered_bulk_op()
        # cursor = db.Temp_MT_Summary_YJ.find(no_cursor_timeout=True)

        for x in cursor:
            # del x['_id']
            bulkSummary.find({
                "combine_column": x['combine_column'],
                "pos.City": x['pos']['City'],
                "origin.City": x['origin']['City'],
                "destination.City": x['destination']['City'],
                "compartment": x['compartment']
            }).upsert().update({'$set': x})
            if num % 1000 == 0:
                try:
                    program_starts = time.time()
                    bulkSummary.execute()
                    bulkSummary = db.JUP_DB_Manual_Triggers_Module_Monthly_Weekly_Summary.initialize_unordered_bulk_op()
                    now = time.time()
                    print("time for update " + str(num) + " rows {0}".format(now - program_starts))
                except Exception as bwe:
                    print(bwe.details)
            num = num + 1
        bulkSummary.execute()

    except Exception as error:
        print(error)
    #'''

if __name__ == '__main__':
    db = client[JUPITER_DB]
    pos_list = db.JUP_DB_Manual_Triggers_Module_Summary.distinct('pos.City')
    for each_market in pos_list:
        main(each_market, client)
        # pass
        # print each_market
    # main("CMB")