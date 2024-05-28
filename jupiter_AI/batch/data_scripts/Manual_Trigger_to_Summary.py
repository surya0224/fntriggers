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

    # print(cur_date.weekday())
    summary_YJ_pipeline = [{
        '$match':{
                'dep_date': {'$gte': dep_date_start, '$lte': dep_date_end},
                '$or':[{'trx_date': {'$lte': str_date}},
                {'trx_date': {'$eq': None}}],
                'pos.City': pos,
                # 'od': od
                # 'compartment.compartment': 'Y'
                }
        },
        {'$unwind': '$dep_date'},
        {'$unwind': '$pos.City'},
        {
        '$group': {
            '_id': {
                'dep_date': "$dep_date",
                # 'dep_date_ISO': "$dep_date_ISO",
                'dep_month': '$dep_month',
                'dep_year': '$dep_year',
                'pos': '$pos.City',
                'origin': '$origin.City',
                'destination': '$destination.City',
                'od': '$od',
                'compartment': '$compartment.compartment',
                },

            'farebasis': {'$addToSet': {'$ifNull': [{'$cond': [{'$gte': ['$dep_date',str_date]}, '$sale_farebasis', '$flown_farebasis']}, []]}},
            'pos': {'$first': '$pos'},
            'dep_date_ISO': {'$first': '$dep_date_ISO'},
            'origin': {'$first': '$origin'},
            'destination': {'$first': '$destination'},
            'psuedo_od': {'$first': '$psuedo_od'},
            'book_pax': {'$sum': "$book_pax.value"},
            'book_ticket': {'$sum': "$book_revenue.value"},
            'book_pax_1': {'$sum': "$book_pax.value_1"},
            'book_snap_pax': {'$sum': "$book_pax.value"},
            'sale_snap_pax': {'$sum': "$sale_pax.value"},
            'flown_snap_pax': {'$sum': "$flown_pax.value"},
            'book_snap_revenue': {'$sum': "$book_revenue.value"},
            'sale_snap_revenue': {'$sum': "$sale_revenue.value"},
            'flown_snap_revenue': {'$sum': "$flown_revenue.value"},
            'book_ticket_1': {'$sum': "$book_revenue.value_1"},
            'sale_pax': {'$sum': "$sale_pax.value"},
            'sale_revenue': {'$sum': "$sale_revenue.value"},
            'sale_pax_1': {'$sum': "$sale_pax.value_1"},
            'sale_revenue_1': {'$sum': "$sale_revenue.value_1"},
            'flown_pax': {'$sum': "$flown_pax.value"},
            'flown_revenue': {'$sum': "$flown_revenue.value"},
            'flown_pax_1': {'$max': "$flown_pax.value_1"},
            'flown_revenue_1': {'$max': "$flown_revenue.value_1"},
            'capacity': {'$max': {'$ifNull': ['$inventory.capacity', 0]}},
            'capacity_1': {'$max': {'$ifNull': ['$inventory.capacity_1', 0]}},
            'target_pax': {'$max': '$target.pax'},
            'target_revenue': {'$max': '$target.revenue'},
            'target_pax_1': {'$max': '$target.pax_1'},
            'target_revenue_1': {'$max': '$target.revenue_1'},
            'significant_target': {'$max': '$target.significant_target'},
            'distance': {'$max': '$distance'},
            'forecast_pax': {'$max': '$forecast.pax'},
            'forecast_avgFare': {'$max': '$forecast.avgFare'},
            'forecast_revenue': {'$max': '$forecast.revenue'},
        }
        },

        {'$addFields': {
            'farebasis': {'$reduce': {
                            'input': '$farebasis',
                            'initialValue': [],
                            'in': { '$concatArrays': ["$$value", "$$this"]}
            }}
        }},

        {
        '$project': {
            'farebasis': '$farebasis',
            'dep_month': '$_id.dep_month',
            'dep_date': '$_id.dep_date',
            'dep_date_ISO': '$dep_date_ISO',
            'compartment': '$_id.compartment',
            'dep_year': '$_id.dep_year',
            'pos': '$pos',
            'duration': '$Cal.duration',
            'od': '$_id.od',
            'psuedo_od': {'$ifNull': ['$psuedo_od', '$_id.od']},
            'origin': '$origin',
            'destination': '$destination',
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
            'target_pax_1': '$target_pax_1',
            'significant_target': '$significant_target',
            'target_revenue_1': '$target_revenue_1',
            'od_capacity': '$capacity',
            'od_capacity_1': '$capacity_1',
            'forecast_pax': '$forecast_pax',
            'forecast_avgFare': '$forecast_avgFare',
            'forecast_revenue': '$forecast_revenue',
            'target_avgFare_1': {
            '$cond': [{ '$ne': ["$target_pax_1", 0]}, {'$divide': ['$target_revenue_1', '$target_pax_1']}, 0]
                },

            'target_avgFare': {
                '$cond': [{ '$ne': ["$target_pax", 0]}, {'$divide': ['$target_revenue', '$target_pax']}, 0]
                },
            'target_revenue': '$target_revenue',
            'distance': '$distance',
            'distance_sale': {'$multiply': ['$distance', '$sale_pax']},
            'distance_sale_1': {'$multiply': ['$distance', '$sale_pax_1']},
            'distance_flown': {'$multiply': ['$distance', '$flown_pax']},
            'distance_flown_1': {'$multiply': ['$distance', '$flown_pax_1']},
            #target_flown_1: {$multiply: ['$distance', '$flown_pax_1']},
            'distance_Target_Pax': {'$multiply': ['$distance', '$target_pax']},
            'book_paxCapaAdj': {'$multiply': ['$book_pax', {'$cond': [{ '$and':[{'$ne': ["$capacity", 0]}, {'$ne': ["$capacity_1", 0]}]},
            {'$divide': ["$capacity_1", "$capacity"]}, 1]}]},

            'book_ticketCapaAdj': {'$multiply': ['$book_ticket', {'$cond': [{'$and':[{'$ne': ["$capacity", 0]}, {'$ne': ["$capacity_1", 0]}]},
            {'$divide': ["$capacity_1", "$capacity"]}, 1]}]},

            'sale_paxCapaAdj': {'$multiply': ['$sale_pax', {'$cond': [{ '$and':[{'$ne': ["$capacity", 0]}, {'$ne': ["$capacity_1", 0]}]},
            {'$divide': ["$capacity_1", "$capacity"]}, 1]}]},

            'sale_revenueCapaAdj': {'$multiply': ['$sale_revenue', {'$cond': [{'$and':[{'$ne': ["$capacity", 0]}, {'$ne': ["$capacity_1", 0]}]},
            {'$divide': ["$capacity_1", "$capacity"]}, 1]}]},

            'flown_paxCapaAdj': {'$multiply': ['$flown_pax', {'$cond': [{'$and':[{'$ne': ["$capacity", 0]}, {'$ne': ["$capacity_1", 0]}]},
            {'$divide': ["$capacity_1", "$capacity"]}, 1]}]},

            'flown_revenueCapaAdj': {'$multiply': ['$flown_revenue', {'$cond': [{'$and':[{'$ne': ["$capacity", 0]}, {'$ne': ["$capacity_1", 0]}]},
            {'$divide': ["$capacity_1", "$capacity"]}, 1]}]},

        }
        },
        {'$group': {
            '_id': {
            'dep_month': '$dep_month',
            'dep_year': '$dep_year',
            'dep_date': '$dep_date',
            # 'dep_date_ISO': '$dep_date_ISO',
            'compartment': '$compartment',
            'pos': '$pos.City',
            'od': '$od',
            'origin': '$origin.City',
            'destination': '$destination.City',
            },
            'farebasis': {'$push': '$farebasis'},
            'pos': {'$first': '$pos'},
            'dep_date_ISO': {'$first': '$dep_date_ISO'},
            'origin': {'$first': '$origin'},
            'destination': {'$first': '$destination'},
            'psuedo_od': {'$first': '$psuedo_od'},
            'book_paxCapaAdj': {'$sum': '$book_paxCapaAdj'},
            'book_ticketCapaAdj': {'$sum': '$book_ticketCapaAdj'},
            'sale_paxCapaAdj': {'$sum': '$sale_paxCapaAdj'},
            'sale_revenueCapaAdj': {'$sum': '$sale_revenueCapaAdj'},
            'flown_paxCapaAdj': {'$sum': '$flown_paxCapaAdj'},
            'flown_revenueCapaAdj': {'$sum': '$flown_revenueCapaAdj'},
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
            'book_snap_pax': {'$sum': "$book_snap_pax"},
            'sale_snap_pax': {'$sum': "$sale_snap_pax"},
            'flown_snap_pax': {'$sum': "$flown_snap_pax"},
            'book_snap_revenue': {'$sum': "$book_snap_revenue"},
            'sale_snap_revenue': {'$sum': "$sale_snap_revenue"},
            'flown_snap_revenue': {'$sum': "$flown_snap_revenue"},
            'target_pax': {'$sum': '$target_pax'},
            'target_avgFare': {'$avg': '$target_avgFare'},
            'significant_target': {'$max': '$significant_target'},
            'target_revenue': {'$sum': '$target_revenue'},
            'distance': {'$max': '$distance'},
            'distance_sale': {'$sum': '$distance_sale'},
            'distance_sale_1': {'$sum': '$distance_sale_1'},
            'distance_flown': {'$sum': '$distance_flown'},
            'distance_flown_1': {'$sum': '$distance_flown_1'},
            'distance_Target_Pax': {'$sum': '$distance_Target_Pax'},
            'od_capacity': {'$max': '$od_capacity'},
            'od_capacity_1': {'$max': '$od_capacity_1'},
            'target_pax_1': {'$sum': '$target_pax_1'},
            'target_avgFare_1': {'$avg': '$target_avgFare_1'},
            'target_revenue_1': {'$sum': '$target_revenue_1'},
            'forecast_pax': {'$sum': '$forecast_pax'},
            'forecast_avgFare': {'$sum': '$forecast_avgFare'},
            'forecast_revenue': {'$sum': '$forecast_revenue'},
            'list_of_pos': {'$addToSet': '$pos'},
        }},

        {'$addFields': {
             'farebasis': {'$reduce': {
                            'input': '$farebasis',
                            'initialValue': [],
                            'in': {'$concatArrays': ["$$value", "$$this"]}
            }}
        }},

        {'$unwind': {'path': "$farebasis", 'preserveNullAndEmptyArrays': True}},
        {'$group': {
            '_id': {
                'dep_month': '$_id.dep_month',
                'dep_year': '$_id.dep_year',
                'dep_date': '$_id.dep_date',
                'dep_date_ISO': '$dep_date_ISO',
                'compartment': '$_id.compartment',
                'pos': '$pos.City',
                'od': '$_id.od',
                'origin': '$origin.City',
                'destination': '$destination.City',
                'fare_basis': '$farebasis.fare_basis',
                'RBD': '$farebasis.RBD',
                'currency': '$farebasis.currency',
                'fareId': '$farebasis.fareId',
            },

            'pax': {'$sum': '$farebasis.pax'},
            'pax_1': {'$sum': '$farebasis.pax_1'},
            'rev': {'$sum': '$farebasis.rev'},
            'rev_1': {'$sum': '$farebasis.rev_1'},
            'AIR_CHARGE': {'$sum': '$farebasis.AIR_CHARGE'},
            'doc': {'$first': '$$ROOT'}
        }},
        {'$group': {
            '_id': {
                'dep_month': '$_id.dep_month',
                'dep_year': '$_id.dep_year',
                'dep_date': '$_id.dep_date',
                'dep_date_ISO': '$_id.dep_date_ISO',
                'compartment': '$_id.compartment',
                'pos': '$_id.pos',
                'od': '$_id.od',
                'origin': '$_id.origin',
                'destination': '$_id.destination',

            },
        'farebasis': {'$push': {
            'farebasis': '$_id.fare_basis',
            'RBD': '$_id.RBD',
            'currency': '$_id.currency',
            'fareId': '$_id.fareId',
            'pax': '$pax',
            'pax_1': '$pax_1',
            'rev': '$rev',
            'rev_1': '$rev_1',
            'AIR_CHARGE': '$AIR_CHARGE',
        }},
        'doc': {'$first': '$doc'}
        }},

        {'$project': {
            '_id': 0,
            'farebasis': '$farebasis',
            'dep_month': '$_id.dep_month',
            'dep_date': '$_id.dep_date',
            'dep_date_ISO': '$_id.dep_date_ISO',
            'compartment': '$_id.compartment',
            'dep_year': '$_id.dep_year',
            'pos': '$doc.pos',
            'psuedo_od': '$doc.psuedo_od',
            'od': '$_id.od',
            'origin': '$doc.origin',
            'destination': '$doc.destination',
            'book_pax': '$doc.book_pax',
            'book_ticket': '$doc.book_ticket',
            'book_pax_1': '$doc.book_pax_1',
            'book_ticket_1': '$doc.book_ticket_1',
            'sale_pax': '$doc.sale_pax',
            'sale_revenue': '$doc.sale_revenue',
            'sale_pax_1': '$doc.sale_pax_1',
            'sale_revenue_1': '$doc.sale_revenue_1',
            'flown_pax': '$doc.flown_pax',
            'flown_revenue': '$doc.flown_revenue',
            'flown_pax_1': '$doc.flown_pax_1',
            'flown_revenue_1': '$doc.flown_revenue_1',
            'book_snap_pax': '$doc.book_snap_pax',
            'sale_snap_pax': '$doc.sale_snap_pax',
            'flown_snap_pax': '$doc.flown_snap_pax',
            'book_snap_revenue': '$doc.book_snap_revenue',
            'sale_snap_revenue': '$doc.sale_snap_revenue',
            'flown_snap_revenue': '$doc.flown_snap_revenue',
            'target_pax': '$doc.target_pax',
            'target_avgFare': '$doc.target_avgFare',
            'target_revenue': '$doc.target_revenue',
            'target_pax_1': '$doc.target_pax_1',
            'target_avgFare_1': '$doc.target_avgFare_1',
            'target_revenue_1': '$doc.target_revenue_1',
            'distance': '$doc.distance',
            'distance_sale': '$doc.distance_sale',
            'distance_sale_1': '$doc.distance_sale_1',
            'distance_flown': '$doc.distance_flown',
            'distance_flown_1': '$doc.distance_flown_1',
            'distance_Target_Pax': '$doc.distance_Target_Pax',
            'significant_target': '$doc.significant_target',
            'capacity': '$doc.od_capacity',
            'capacity_1': '$doc.od_capacity_1',
            'forecast_pax': '$doc.forecast_pax',
            'forecast_avgFare': '$doc.forecast_avgFare',
            'forecast_revenue': '$doc.forecast_revenue',
            # list_of_pos:'$list_of_pos',
            'book_paxCapaAdj': '$doc.book_paxCapaAdj',
            'book_ticketCapaAdj': '$doc.book_ticketCapaAdj',
            'sale_paxCapaAdj': '$doc.sale_paxCapaAdj',
            'sale_revenueCapaAdj': '$doc.sale_revenueCapaAdj',
            'flown_paxCapaAdj': '$doc.flown_paxCapaAdj',
            'flown_revenueCapaAdj': '$doc.flown_revenueCapaAdj',

            # doc: '$doc'
        }},

        {"$addFields": {
            "FlightLegCombine": {"$cond": {"if": {"$eq": ["$destination.City", "DXB"]}, "then": { "$concat": ["$origin.City", "$destination.City", "$compartment", "$dep_date"]},
                                   "else": {
                                       "$cond": {
                                           "if": {"$and": [{
                                                    "$ne": [
                                                    "$origin.City", "DXB"]}, {"$ne": ["$destination.City", "DXB"]}]},
                                           "then": {
                                                "$concat": [
                                                   "$origin.City",
                                                   "DXB",
                                                   "$compartment",
                                                   "$dep_date"
                                               ]},
                                           "else": "0"
                                       }}}
                                },
            "FlightLegCombine_2": {"$cond": {"if": {"$eq": ["$origin.City", "DXB"]}, "then": {"$concat": ["$origin.City", "$destination.City", "$compartment", "$dep_date"]},
                    "else": {"$cond": {
                        "if": {"$and": [{"$ne": ["$origin.City", "DXB"]}, {"$ne": ["$destination.City", "DXB"]}]},
                        "then": {"$concat": ["DXB", "$destination.City", "$compartment", "$dep_date"]}, "else": "0"}}}
                    }
        }},

        {
        "$lookup": {
            "from": "JUP_DB_Flight_Leg_Characteristics",
            "localField": "FlightLegCombine",
            "foreignField": "combine_column",
            "as": "leg1"
        }
        },
        {
        "$lookup": {
            "from": "JUP_DB_Flight_Leg_Characteristics",
            "localField": "FlightLegCombine_2",
            "foreignField": "combine_column",
            "as": "legs2"
        }},
        # Updating Psudo od
        {
            "$lookup": {
                "from": "JUP_DB_OD_Distance_Master",
                "localField": "od",
                "foreignField": "od",
                "as": "psuedo"
        }},

        {"$addFields": {
            "psuedo_od_lookup": {"$arrayElemAt": ["$psuedo", 0]},
        }},

        {"$addFields": {
            "leg2": {"$arrayElemAt": ["$legs2", 0]},
        }},
        {"$addFields": {
            "leg1": {"$arrayElemAt": ["$leg1", 0]},
        }},
        {'$addFields': {
            'Month': "Month"
        }},
        {
        '$lookup': {
            'from': 'JUP_DB_Calendar_Master',
            'localField': 'Month',
            'foreignField': 'period',
            'as': 'Cal'
        }},

          {"$addFields": {

              "combine_column": {'$concat': ["M", {
                  "$substr": [
                      {
                          "$mod": [
                              "$dep_year",
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
                             "$dep_month",
                             10000
                         ]
                     },
                     0,
                     2
                 ]
             }]},
          }},
      {"$addFields": {
          "Cal": {
              "$filter": {
                  "input": "$Cal",
                  "as": "item",
                  "cond": {"$eq": ['$combine_column', "$$item.combine_column"]}
              }
          }
      }},
       {'$unwind': {'path': "$Cal", 'preserveNullAndEmptyArrays': True}},
        # {'$unwind': '$Cal'},
        {'$project': {
            'combine_column_mt': {'$concat': ['$dep_date', '$pos.City', '$od', '$compartment']},
            'farebasis': '$farebasis',
            'dep_month': '$dep_month',
            'dep_date': '$dep_date',
            'dep_date_ISO': '$dep_date_ISO',
            'compartment': '$compartment',
            'dep_year': '$dep_year',
            'pos': '$pos',
            'duration': '$Cal.duration',
            'od': '$od',
            'psuedo_od': { '$ifNull': ['$psuedo_od_lookup.psuedo_od', '$psuedo_od']},
            'origin': '$origin',
            'destination': '$destination',
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
            'target_avgFare': '$target_avgFare',
            'target_revenue': '$target_revenue',
            'target_pax_1': '$target_pax_1',
            'target_avgFare_1': '$target_avgFare_1',
            'target_revenue_1': '$target_revenue_1',
            'distance': '$distance',
            'distance_sale': '$distance_sale',
            'distance_sale_1': '$distance_sale_1',
            'distance_flown': '$distance_flown',
            'distance_flown_1': '$distance_flown_1',
            'distance_Target_Pax': '$distance_Target_Pax',
            'significant_target': { '$ifNull': ['$significant_target', False]},
            'capacity': '$capacity',
            'capacity_1': '$capacity_1',
            'forecast_pax': '$forecast_pax',
            'forecast_avgFare': '$forecast_avgFare',
            'forecast_revenue': '$forecast_revenue',
            'book_paxCapaAdj': '$book_paxCapaAdj',
            'book_ticketCapaAdj': '$book_ticketCapaAdj',
            'sale_paxCapaAdj': '$sale_paxCapaAdj',
            'sale_revenueCapaAdj': '$sale_revenueCapaAdj',
            'flown_paxCapaAdj': '$flown_paxCapaAdj',
            'flown_revenueCapaAdj': '$flown_revenueCapaAdj',
            "prorate_target_pax": {'$cond': [{'$ne': ["$target_pax", 0]}, {'$divide': ["$target_pax", '$Cal.duration']}, 0]},
            "prorate_target_revenue": {'$cond': [{'$ne': ["$target_revenue", 0]}, {'$divide': ["$target_revenue", '$Cal.duration']}, 0]},
            "prorate_target_pax_1": {'$cond': [{'$ne': ["$target_pax_1", 0]}, {'divide': ["$target_pax_1", '$Cal.duration']}, 0]},
            "prorate_target_revenue_1": {'$cond': [{'$ne': ["$target_revenue_1", 0]}, {'$divide': ["$target_revenue_1", '$Cal.duration']}, 0]},
            "prorate_forecast_pax": {'$cond': [{'$ne': ["$forecast_pax", 0]}, {'$divide': ['$forecast_pax', '$Cal.duration']}, 0]},
            "prorate_forecast_revenue": {'$cond': [{'$ne': ["$forecast_revenue", 0]}, {'$divide': ["$forecast_revenue", '$Cal.duration']}, 0]},
            "leg1_pax": { '$ifNull': ["$leg1.pax", 0]},
            "leg1_pax_1": { '$ifNull': ["$leg1.pax_1", 0]},
            "leg1_capacity": { '$ifNull': ["$leg1.capacity", 0]},
            "leg1_capacity_1": {'$ifNull': ["$leg1.capacity_1", 0]},
            "leg1_bookings": { '$ifNull': ["$leg1.bookings", 0]},
            "leg1_bookings_1": { '$ifNull': ["$leg1.bookings_1", 0]},
            "leg2_pax": { '$ifNull': ["$leg2.pax", 0]},
            "leg2_pax_1": { '$ifNull': ["$leg2.pax_1", 0]},
            "leg2_capacity": { '$ifNull': ["$leg2.capacity", 0]},
            "leg2_capacity_1": { '$ifNull': ["$leg2.capacity_1", 0]},
            "leg2_bookings": { '$ifNull': ["$leg2.bookings", 0]},
            "leg2_bookings_1": { '$ifNull': ["$leg2.bookings_1", 0]},
            "leg1_forecastpax": { '$ifNull': ["$leg1.forecast_pax", 0]},
            "leg1_forecastpax_1": { '$ifNull': ["$leg1.forecast_pax_1", 0]},
            "leg2_forecastpax": { '$ifNull': ["$leg2.forecast_pax", 0]},
            "leg2_forecastpax_1": { '$ifNull': ["$leg2.forecast_pax_1", 0]}
        }},
    # {'$out': 'Temp_Summary_YJ'},
    ]
    cursor = db.JUP_DB_Manual_Triggers_Module.aggregate(summary_YJ_pipeline,allowDiskUse=True)
    num = 1
    Bulk = db.JUP_DB_Manual_Triggers_Module_Summary.initialize_ordered_bulk_op()
    # cursor = db.Temp_Summary_YJ.find(no_cursor_timeout=True)
    #print "Comp YJ"
    for x in cursor:
        # print x['psuedo_od']
        # print ("dep_date"+ x['dep_date']+"pos.City"+ x['pos']['City']+"origin.City"+ x['origin']['City']+"destination.City"+ x['destination']['City']+"compartment"+ x['compartment'])
        try:
            # del x['_id']
            Bulk.find({
                "dep_date": x['dep_date'],
                "pos.City": x['pos']['City'],
                "origin.City": x['origin']['City'],
                "destination.City": x['destination']['City'],
                "compartment": x['compartment'],
            }).upsert().update(
                {
                    '$set': x
                })

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
    market_list = db.JUP_DB_Manual_Triggers_Module.distinct('market_combined')
    market_list.remove(None)
    actual_list = list()
    for each_market in market_list:
        actual_list.append({
            'pos' : each_market[:3],
            'od' : each_market[3:9]
        })
    for each_market in actual_list:
        # main(each_market['pos'], each_market['od'],client)
        pass
        # print each_market
    main("CMB", client)
