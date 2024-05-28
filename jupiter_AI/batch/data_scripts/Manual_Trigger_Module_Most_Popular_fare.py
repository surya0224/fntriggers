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
# db = client[JUPITER_DB]


def main(client, ods):
    db = client[JUPITER_DB]
    cur_date = datetime.strptime(SYSTEM_DATE, '%Y-%m-%d')
    if len(str(cur_date.day)) == 2:
        date = str(cur_date.day)
    else:
        date = '0'+ str(cur_date.day)

    if len(str(cur_date.month)) ==2:
        month = str(cur_date.month)
    else:
        month = '0'+ str(cur_date.month)

    year = cur_date.year
    this_month = cur_date.month
    next_month = cur_date.month +1
    str_date = str(year)+'-'+str(month)+'-'+date
    doc = db.JUP_DB_Config_ETL_frequency.find_one({"item" : "Infare"})
    try:
        date_range = doc["action"]["Popular_fare_consume_range_of_Observation_date"]
    except KeyError:
        date_range = 3
    print(date_range)
    cur_date = cur_date - relativedelta(days=(date_range-1))
    if len(str(cur_date.day)) ==2:
        date_1wkLTR = str(cur_date.day)
    else:
        date_1wkLTR = '0'+str(cur_date.day)

    if len(str(cur_date.month)) ==2:
        month_1wkLTR = str(cur_date.month)
    else:
        month_1wkLTR = '0'+ str(cur_date.month)

    year_1wkLTR = cur_date.year
    str_date_1wkLTR = str(year_1wkLTR) + '-' + str(month_1wkLTR) + '-' + str(date_1wkLTR)
    print str_date, str_date_1wkLTR
    MPF_pipeline = [
    # {"$match": {
    #     "snap_date": {'$gte': "2020-06-14", '$lte': str_date}, "od" : {"$in" : ods}}},

    {'$sort': {'pos': 1, 'od': 1, 'carrier': 1, 'compartment': 1, 'outbound_departure_date': 1, 'is_one_way': 1, 'observation_date': -1, 'observation_time': -1}},
    {'$group': {'_id': {'pos': '$pos', 'od': '$od', 'carrier': '$carrier', 'is_one_way': '$is_one_way', 'compartment': '$compartment',
                        'outbound_travel_stop_over': 'outbound_travel_stop_over',
                        'inbound_travel_stop_over': 'inbound_travel_stop_over',
                        'outbound_departure_date': '$outbound_departure_date',
                        'outbound_flight_no': '$outbound_flight_no'
                        },
                        'observation_date': {'$first': '$observation_date'},
                        'observation_time': {'$first': '$observation_time'},
    'doc': {'$push': {
        'observation_date': '$observation_date',
        'observation_time': '$observation_time',
        'pos': '$pos',
        'od': '$od',
        'compartment': '$compartment',
        'carrier': '$carrier',
        'is_one_way': '$is_one_way',
        'outbound_departure_date': '$outbound_departure_date',
        'outbound_travel_stop_over': '$outbound_travel_stop_over',
        'inbound_travel_stop_over': '$inbound_travel_stop_over',
        'outbound_flight_no': '$outbound_flight_no',
        'outbound_fare_basis': '$outbound_fare_basis',
        'tax': '$tax',
        'fare': '$price_inc',
        'currency': '$currency'
    }}
    }},
    {'$unwind': '$doc'},
    { '$redact': {
    '$cond': {
    'if': { '$and':[
        {'$eq': ['$observation_date', '$doc.observation_date']},
        {'$eq': ['$observation_time', '$doc.observation_time']}]},
    'then': "$$KEEP",
    'else': "$$PRUNE"
    }
    }
    },
    {
    '$lookup':
    {
    'from': 'JUP_DB_Pos_Currency_Master'
        ,
          'localField': "_id.pos",
          'foreignField': "country",
          'as': "inventory_docs"
    }
    },
    {
    '$lookup':
    {
    'from': 'JUP_DB_Exchange_Rate',
          'localField': "doc.currency",
          'foreignField': "code",
            'as': "cur_exch"
    }
    },
    {'$addFields': {'webcur': {'$arrayElemAt': ["$inventory_docs.web", 0]}}},
    {
    '$lookup':
    {
    'from': "JUP_DB_Exchange_Rate",
          'localField': "webcur",
        'foreignField': "code",
        'as': "web_exch"
    }
    },
    {'$addFields': {'cur_exch_rate': {'$arrayElemAt': ["$cur_exch.Reference_Rate", 0]}}},
    {'$addFields': {'web_exch_rate': {'$arrayElemAt': ["$web_exch.Reference_Rate", 0]}}},
    {'$project': {
                    'inf_currency': '$doc.currency',
                    'web_cur_from_master': '$webcur',

                    'fare': {'$cond': [{'$and':[{'$ne': ['$web_exch_rate', 0]}, {'$ne': ['$web_exch_rate', None]},{'$ne': ['$cur_exch_rate', 0]}, {'$ne': ['$cur_exch_rate', None]}]}, {'$divide': [{'$multiply': [
                    '$doc.fare', '$cur_exch_rate']}, '$web_exch_rate']}, '$doc.fare']},
                    'tax': {'$cond': [{'$and':[{'$ne': ['$web_exch_rate', 0]}, {'$ne': ['$web_exch_rate', None]},{'$ne': ['$cur_exch_rate', 0]}, {'$ne': ['$cur_exch_rate', None]}]}, {'$divide': [{'$multiply': [
                    '$doc.tax', '$cur_exch_rate']}, '$web_exch_rate']}, '$doc.tax']},
                    'observation_date': '$observation_date', 'observation_time': '$observation_time',
                    'doc': '$doc'
    }},

    {'$group':{
                '_id': {
                    'pos': '$_id.pos', 'od': '$_id.od', 'compartment': '$_id.compartment', 'carrier': '$_id.carrier',
                    'is_one_way': '$_id.is_one_way',
                    'outbound_departure_date': '$_id.outbound_departure_date', 'observation_date': '$observation_date',
                    'observation_time': '$observation_time',
                    'outbound_flight_no': '$doc.outbound_flight_no', 'fare': '$fare', 'currency': '$web_cur_from_master',
                    'outbound_fare_basis': '$doc.outbound_fare_basis',
                    'outbound_travel_stop_over': '$doc.outbound_travel_stop_over',
                    'inbound_travel_stop_over': '$doc.inbound_travel_stop_over',
                    'tax': '$tax',
                },

        }},
    {'$addFields': {'frequency': 1}},
    {'$group': {
        '_id': {'pos': '$_id.pos', 'od': '$_id.od', 'carrier': '$_id.carrier', 'is_one_way': '$_id.is_one_way',
                'compartment': '$_id.compartment',
                'outbound_departure_date': '$_id.outbound_departure_date', 'observation_date': '$_id.observation_date',
                'fare': '$_id.fare', 'currency': '$_id.currency', 'outbound_fare_basis': '$_id.outbound_fare_basis',
                'tax': '$_id.tax',
                'outbound_travel_stop_over': '$_id.outbound_travel_stop_over',
                'inbound_travel_stop_over': '$_id.inbound_travel_stop_over',
    },
    'frequency': {'$sum': '$frequency'}
    }},
    {'$addFields': {'frequency': 1}},
        {"$addFields": {
            "outbound_travel_stop_over":
                {"$cond": [

                    {"$eq": [{"$size": {"$split": ["$_id.outbound_travel_stop_over", ","]}}, 1]},
                    {'$cond': [
                        {'$eq': ["$_id.outbound_travel_stop_over", ""]}, 0, 1
                    ]},
                    {"$size": {"$split": ["$_id.outbound_travel_stop_over", ","]}}
                ]},
            "inbound_travel_stop_over": {"$cond": [

                {"$eq": [{"$size": {"$split": ["$_id.inbound_travel_stop_over", ","]}}, 1]},
                {'$cond': [
                    {'$eq': ["$_id.inbound_travel_stop_over", ""]}, 0, 1
                ]},
                {"$size": {"$split": ["$_id.inbound_travel_stop_over", ","]}}
            ]}
        }},
    {"$addFields": {
        "stop_over": {"$cond": [
            {"$eq": ["$_id.is_one_way", 1]},
            "$outbound_travel_stop_over",
            {"$min": ['$outbound_travel_stop_over', '$inbound_travel_stop_over']}
        ]}
    }},
    {'$sort': {'_id.pos': 1,
             '_id.od': 1,
             '_id.carrier': 1,
             '_id.is_one_way': 1,
             '_id.compartment': 1,
             '_id.outbound_departure_date': 1,
             '_id.observation_date': 1,
             '_id.currency': 1,
             '_id.fare': 1,
             'stop_over': 1,
             'frequency': -1,

             }},
    {'$group': {
        '_id': {'pos': '$_id.pos', 'od': '$_id.od', 'carrier': '$_id.carrier', 'is_one_way': '$_id.is_one_way',
              'compartment': '$_id.compartment',
              'outbound_departure_date': '$_id.outbound_departure_date', 'observation_date': '$_id.observation_date',
              'observation_time': '$_id.observation_time',
              'currency': '$_id.currency',
              'stop_over': '$stop_over',
              },

        'frequency': {'$first': '$frequency'},
        'stop_over': {'$first': '$stop_over'},
    'outbound_fare_basis': {'$first': '$_id.outbound_fare_basis'},
    'tax': {'$first': '$_id.tax'},
    'fare': {'$first': '$_id.fare'},
    }},

    {'$group': {
        '_id': {'pos': '$_id.pos', 'od': '$_id.od', 'carrier': '$_id.carrier', 'is_one_way': '$_id.is_one_way',
              'compartment': '$_id.compartment',
              'outbound_departure_date': '$_id.outbound_departure_date', 'observation_date': '$_id.observation_date',
              'observation_time': '$_id.observation_time',
              'currency': '$_id.currency', 'outbound_fare_basis': '$outbound_fare_basis',
              'tax': '$tax',
                'stop_over': '$stop_over',

              },
        'frequency': {'$max': '$frequency'},
    'doc': {'$push': {
        'pos': '$_id.pos', 'od': '$_id.od', 'carrier': '$_id.carrier', 'is_one_way': '$_id.is_one_way',
        'compartment': '$_id.compartment',
        'outbound_departure_date': '$_id.outbound_departure_date', 'observation_date': '$_id.observation_date',
        'observation_time': '$_id.observation_time',
        'fare': '$fare', 'currency': '$_id.currency', 'frequency': '$frequency', 'outbound_fare_basis': '$outbound_fare_basis',
        'tax': '$tax',

        'stop_over': '$_id.stop_over',
    }}
    }},

    {'$unwind': '$doc'},
    { '$redact': {
    '$cond': {
    'if':{'$eq': ['$frequency', '$doc.frequency']},
    'then': "$$KEEP",
    'else': "$$PRUNE"
    }
    }
    },

    {'$sort': {'frequency': -1, 'doc.fare': 1,'doc.stop_over': 1}},
    {'$project': {
        'pos': '$_id.pos', 'od': '$_id.od', 'carrier': '$_id.carrier', 'is_one_way': '$_id.is_one_way',
        'compartment': '$_id.compartment',
        'outbound_departure_date': '$_id.outbound_departure_date', 'observation_date': '$_id.observation_date',
        'observation_time': '$_id.observation_time', 'fare': '$doc.fare', 'frequency': '$frequency', 'currency': '$_id.currency',
        'outbound_fare_basis': '$doc.outbound_fare_basis',
        'tax': '$doc.tax',
        'stop_over': '$doc.stop_over',
    }},
    {'$group': {
        '_id': {'pos': '$pos', 'od': '$od', 'compartment': '$compartment', 'outbound_departure_date':
            '$outbound_departure_date'},
        'fare': {'$first': '$fare'},
    'frequency': {'$first': '$frequency'},
    'currency': {'$first': '$currency'},
    'carrier': {'$first': '$carrier'},
    'is_one_way': {'$first': '$is_one_way'},
    'popular_fare_detail': {
    '$push': {
        'fare': '$fare',
        'frequency': '$frequency',
        'is_one_way': '$is_one_way',
        'carrier': '$carrier',
        'currency': '$currency',
        'outbound_departure_date': '$outbound_departure_date',
        'observation_time': '$observation_time',
        'observation_date': '$observation_date',
        'outbound_fare_basis': '$outbound_fare_basis',
        'tax': '$tax',
        'stop_over': '$stop_over',
    }}
    }},
    {'$project': {
        '_id': 0,
        'pos': '$_id.pos', 'od': '$_id.od', 'compartment': '$_id.compartment',
        'outbound_departure_date': '$_id.outbound_departure_date',
        'popular_fare': {
            'fare': '$fare',
            'frequency': '$frequency',
            'currency': '$currency',
            'carrier': '$carrier',
            'is_one_way': '$is_one_way',
        },
        'popular_fare_detail': '$popular_fare_detail'
    }}
    ]
    cursor = db.JUP_DB_Infare.aggregate(MPF_pipeline, allowDiskUse = True)
    # for i in cursor:
    #     print i


    # update temp table data to Manual trigger table x
    num = 1
    Bulk = db.JUP_DB_Manual_Triggers_Module.initialize_unordered_bulk_op()
    BulkSummary = db.JUP_DB_Manual_Triggers_Module_Summary.initialize_unordered_bulk_op()
    #cursor = db.Temp_MPF.find(no_cursor_timeout=True)
    for x in cursor:
        print x['outbound_departure_date'],x['pos'], x['od'],x['compartment']
        dep_date_ISO = datetime.strptime(x['outbound_departure_date'], '%Y-%m-%d')

        Bulk.find({
            'dep_date': x['outbound_departure_date'],
            # 'dep_date_ISO': dep_date_ISO,
            #   'trx_date': str_date,
            # 'pos.Country': x['pos'],
            'od': x['od'],
            'compartment.compartment': x['compartment'],

        }).update(
            {
        '$set': {
            'popular_fare': x['popular_fare'],
            'popular_fare_detail': x['popular_fare_detail'],

        }
        }
        )

        BulkSummary.find({

            'dep_date': x['outbound_departure_date'],
             # 'pos.Country': x['pos'],
            'od': x['od'],
            'compartment': x['compartment'],

        }).update(
            {
        '$set': {
            'popular_fare': x['popular_fare'],
            'popular_fare_detail': x['popular_fare_detail'],

        }})
        if num % 1000 == 0:
            Bulk.execute()
            Bulk = db.JUP_DB_Manual_Triggers_Module.initialize_unordered_bulk_op()
            BulkSummary.execute()
            BulkSummary = db.JUP_DB_Manual_Triggers_Module_Summary.initialize_unordered_bulk_op()
        num = num +1
    try:
        Bulk.execute()
        BulkSummary.execute()
    except Exception:
        pass

if __name__ == '__main__':
    db = client[JUPITER_DB]
    od_list = list(db.JUP_DB_OD_Master.distinct('pseudo_od'))
    main(client, od_list)
    main(client,
        [
            "DELYVR",
"DELYOW",
"DELYYC",
"DELYTO",
"DELTPE",
"DELSAL",
"DELJED",
"DELBOG",
"DELYHZ",
"DELYMQ",
"DELWAS",
"DELYYZ",
"DELMNL",
"DELEWR",
"DELSFO",
"DELJFK",
"DELJKT",
"DELTYO",
"DELCHI",
"DELKUL"
    ]
    )
