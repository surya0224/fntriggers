"""
File Name              :   common_RnA_functions
Author                 :   Ashwin Kumar
Date Created           :   2016-12-16
Description            : The current program is a repository for common functions that can be used in
 the RnA business layer logic and  data access logic codes.

MODIFICATIONS LOG         :
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :
"""

import ast
import datetime
import json
import random
import string
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, date, timedelta as td

import pandas as pd
from jupiter_AI import client
from jupiter_AI.network_level_params import Host_Airline_Code as host
from jupiter_AI.network_level_params import Host_Airline_Hub as host_hub
from jupiter_AI.network_level_params import JUPITER_DB
#db = client[JUPITER_DB]
"""
the following code get_od_list_leg_level will break the given origin destination combo
to leg level origin destination if the hub of the host airline is neither the origin
nor the destination

example: origin - BLR and destination - DOH

neither the origin nor the destination is the hub for the host airline in the above example.

therefore, the od - BLRDOH will be broken down to BLR(hub) and (hub)DOH
"""

def get_od_list_leg_level(dict_scr_filter):
    od_build = []
    for idx, item in enumerate(dict_scr_filter['origin']):
        if item != host_hub and dict_scr_filter['destination'][idx] != host_hub:
            od1 = ''.join(item + host_hub)
            od_build.append({'od': od1})
            od2 = ''.join(host_hub + dict_scr_filter['destination'][idx])
            od_build.append({'od': od2})
        else:
            od = ''.join(item + dict_scr_filter['destination'][idx])
            od_build.append({'od': od})
        od_build = [i for n, i in enumerate(od_build) if i not in od_build[n + 1:]]
    return od_build

"""
the following code build_query_schedule_col is a query builder that can be used in the data access logic of RnA codes
the following code does not have OD level query built in as collections may have different OD level
(OD level or leg level)

Example:
Certain collections may have documents at an OD level.
example collection(s) - Sales_Flown, OD_distance
However, certain collections only have documents at a leg level
example collection(s) - Flight_leg_details

therefore, it is better to have separate functions for OD level query

"""

def build_query_schedule_col(dict_scr_filter):
    dict_scr_filter = deepcopy(defaultdict(list, dict_scr_filter))
    query_schedule = dict()

    if dict_scr_filter['region']:
        query_schedule['region'] = {'$in': dict_scr_filter['region']}
    if dict_scr_filter['country']:
        query_schedule['country'] = {'$in': dict_scr_filter['country']}
    if dict_scr_filter['pos']:
        query_schedule['pos'] = {'$in': dict_scr_filter['pos']}
    if dict_scr_filter['compartment']:
        query_schedule['compartment'] = {'$in': dict_scr_filter['compartment']}
    if dict_scr_filter['origin'] and dict_scr_filter['destination']:
        query_schedule['$or'] = get_od_list_leg_level(dict_scr_filter)
    query_schedule['effective_from'] = {'$lte': dict_scr_filter['toDate']}
    query_schedule['effective_to'] = {'$gte': dict_scr_filter['fromDate']}

    return query_schedule

"""
The following function gen_collection_name() generates a random name. This random name will be used to name a collection
created by the aggregate function. The aggregate function of mongodb writes all of its outputs to
a collection which has a name generated by this function.

The name is a combination of random characters and the time when the function is being called. This is to
ensure there are no duplications while naming the collections
"""

def gen_collection_name():
    time_stamp = datetime.now().isoformat()
    random_string = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))

    collection_name = 'JUP_Temp_' + time_stamp +'_'+ random_string
    return collection_name

"""
This function returns the bookings, capacity, ticketed values for a given filter. Additionally, it also
provides the ratio between ticketed and capacity for each OD and a flag with value 1 if the above mentioned
ratio is greater than equal to 0.9 or a flag value of 0 if it is lesser than 0.9.

This flag helps in identifying whether RnAs for a given OD requires an action or not. Generally, if this ratio
is more than 0.9 an action is not required
"""

def get_btc_data(filter):
    query = build_query_schedule_col(filter)
    get_date_range = enumerate_dates(filter['fromDate'], filter['toDate'])
    results_collection = gen_collection_name()

    runrate_pipeline = [
        {
            '$match': query

        }
        ,
        {
            '$group':
                {
                    '_id': {'od':'$od', 'compartment': '$compartment'},
                    'bookings': {"$sum": "$pax"},
                    'ticketed': {"$sum": "$ticketed"}
                }
        }
        ,
        {
            "$project":
                {
                    "_id.none": 1,
                    'od':'$_id.od',
                    'compartment': '$_id.compartment',
                    "bookings": '$bookings',
                    "ticketed": '$ticketed'
                 }
        }
        ,
        {
            '$lookup':
                {
                    'from':'JUP_DB_Schedule_Capacity',
                    'localField':'od',
                    'foreignField':'od',
                    'as': 'capacity_collection'
                }
        }
        ,
        {
            '$unwind': '$capacity_collection'
        }
        ,
        {
            '$redact':
                {
                    '$cond':
                        {
                            'if': {'$eq': [host, '$capacity_collection.airline']},
                            'then': '$$DESCEND',
                            'else': '$$PRUNE'
                        }
                }
        }
        ,
        {
            '$project':
                {
                    '_id': 0,
                    'od':'$od',
                    'bookings':'$bookings',
                    'ticketed': '$ticketed',
                    'effective_from':'$capacity_collection.effective_from',
                    'effective_to': '$capacity_collection.effective_to',
                    'frequency' : '$capacity_collection.frequency',
                    'capacity': '$capacity_collection.capacity',
                    'compartment': '$compartment',
                    'compartment_sc': '$capacity_collection.compartment'
                }
        }
        ,
        {
            '$redact':
                {
                    '$cond':
                        {
                            'if': {'$eq':['$compartment','$compartment_sc']},
                            'then': '$$DESCEND',
                            'else': '$$PRUNE'
                        }
                }
        }
        ,
        {
            '$redact':
                {
                    '$cond':
                        {
                            'if':
                                {'$and':
                                     [{'$lte':['$effective_from',filter['toDate']]},
                                      {'$gte':['effective_to',filter['fromDate']]}]},
                            'then': '$$DESCEND',
                            'else': '$$PRUNE'
                        }
                }
        }
        ,
        {
            '$project':
                {
                    '_id': 0,
                    'od': '$od',
                    'bookings': '$bookings',
                    'ticketed': '$ticketed',
                    'effective_from': '$effective_from',
                    'effective_to': '$effective_to',
                    'capacity': '$capacity',
                    'compartment': '$compartment',
                    'frequency' : '$frequency'
                }
        }
        ,
        {
            '$project':
                {
                    '_id': 0,
                    'od': '$od',
                    'bookings': '$bookings',
                    'ticketed': '$ticketed',
                    'effective_from': '$effective_from',
                    'effective_to': '$effective_to',
                    'new_effective_from':
                        {
                            '$cond':
                                {
                                    'if':{'$lte':['$effective_from',filter['fromDate']]},
                                    'then': filter['fromDate'],
                                    'else': '$effective_from'
                                }
                        },
                    'new_effective_to':
                        {
                            '$cond':
                                {
                                    'if':{'$gt':['$effective_to',filter['toDate']]},
                                    'then': filter['toDate'],
                                    'else': '$effective_to'
                                }
                        },
                    'capacity': '$capacity',
                    'compartment': '$compartment',
                    'frequency': '$frequency'
                }
        }
        ,
        {
            '$project':
                {
                    '_id': 0,
                    'od': '$od',
                    'bookings': '$bookings',
                    'ticketed': '$ticketed',
                    'effective_from': '$new_effective_from',
                    'effective_to': '$new_effective_to',
                    'capacity': '$capacity',
                    'compartment': '$compartment',
                    'frequency': '$frequency'
                }
        }
        ,
        {
            '$group':
                {
                    '_id':
                        {
                    'od': '$od',
                    'compartment': '$compartment',
                    'bookings': '$bookings',
                    'ticketed': '$ticketed',
                        },
                    'capacity_details':
                        {
                            '$push':
                                {
                                    'effective_from': '$new_effective_from',
                                    'effective_to': '$new_effective_to',
                                    'capacity': '$capacity',
                                    'frequency': '$frequency'
                                }
                        }

                }
        }
        ,
        {

            '$project':
                {
                    'od': '$_id.od',
                    'compartment': '$_id.compartment',
                    'bookings':'$_id.bookings',
                    'ticketed': '$_id.ticketed',
                    'capacity_details':'$capacity_details'
                }
        }
        ,
        {
            '$out': results_collection
        }

        ]
    perform_aggregate = db.JUP_DB_Booking_DepDate.aggregate(runrate_pipeline, allowDiskUse=True)
    get_collection = db.get_collection(results_collection)
    btc_data = get_collection.find()
    btc_data_count = get_collection.find().count()
    btc_data_list = list(btc_data)

    if btc_data_count != 0:
        for documents in btc_data_list:
            tot_capacity = 0
            for capacity in documents['capacity_details']:
                daterange = pd.date_range(datetime.datetime.strptime(capacity['effective_from'], "%Y-%m-%d"),
                                          datetime.datetime.strptime(capacity['effective_to'], "%Y-%m-%d"))
                frequency = capacity['frequency']
                frequency = ast.literal_eval(json.dumps(frequency))
                frequency = list(frequency)
                for single_date in daterange:
                    if str(single_date.isoweekday()) in frequency:
                        tot_capacity += capacity['capacity']
            ticketed_capacity = (float(documents['ticketed'])/tot_capacity) * 100
            if ticketed_capacity > 90:
                flag_for_action = 0
            else:
                flag_for_action = 1
            documents['capacity'] = tot_capacity
            documents['ticketed_capacity'] = ticketed_capacity
            documents['flag_for_action'] = flag_for_action

    return btc_data_list

"""
the following function enumerate dates will return a list of dates between two given date values
"""

def enumerate_dates(d1, d2):
    dates_list = []

    # d1 = '2008-10-15'
    # d2 = '2008-10-20'

    date_obj = datetime.strptime(d1, '%Y-%m-%d')
    date_month = date_obj.month
    date_year = date_obj.year
    date_day = date_obj.day

    d1 = date(date_year, date_month, date_day)

    date_obj = datetime.strptime(d2, '%Y-%m-%d')
    date_month = date_obj.month
    date_year = date_obj.year
    date_day = date_obj.day

    d2 = date(date_year, date_month, date_day)

    delta = d2 - d1

    for i in range(delta.days + 1):
        # dates_list.append((d1 + td(days=i)).strftime('%Y-%m-%d'))
        dates_list.append(d1 + td(days=i))

    return dates_list

