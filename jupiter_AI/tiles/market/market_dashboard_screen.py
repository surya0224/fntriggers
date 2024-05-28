"""
File Name              : market_dashboard_screen
Author                 : Ashwin Kumar
Date Created           : 2016-12-29
Description            : The tiles present in the market dashboard are calulated over here.
                         the tiles are host revenue and pax, host rank, deployed capacity and capacity vlyr,
                         competitor rank and market share.

MODIFICATIONS LOG
    S.No                   : 1
    Date Modified          : 2017-02-09
    By                     : Shamail Mulla
    Modification Details   : Code Optimisation
"""

import os
import sys

root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir, os.path.pardir))
sys.path.append(root_dir)

from collections import defaultdict
from copy import deepcopy
import datetime
import time
import inspect
from jupiter_AI.common import ClassErrorObject as error_class
try:
    from jupiter_AI import client, JUPITER_DB, Host_Airline_Code
    db = client[JUPITER_DB]
except:
    pass
from jupiter_AI.network_level_params import Host_Airline_Code as host
from jupiter_AI.tiles.jup_common_functions_tiles import gen_collection_name as gen
from jupiter_AI.tiles.jup_common_functions_tiles import query_month_year_builder
from jupiter_AI.tiles.jup_common_functions_tiles import prev_year_range

result_collection_capacity = gen()

result_collection_ms = gen()
result_host_rank_coll = gen()
result_collection_demo_capacity = 'JUP_Demo_Host_Capacity'
result_collection_rev_pax = gen()


def get_module_name():
    return inspect.stack()[1][3]


def get_arg_lists(frame):
    """
    function used to get the list of arguments of the function
    where it is called
    """
    args, _, _, values = inspect.getargvalues(frame)
    argument_name_list = []
    argument_value_list = []
    for k in args:
        argument_name_list.append(k)
        argument_value_list.append(values[k])
    return argument_name_list, argument_value_list


def query_builder(dict_filter):
    """
    Creates a query to be fired on the db
    :param dict_filter: dictionary of user inputs
    :return: dictionary
    """
    query = defaultdict(list)
    dict_filter = deepcopy(defaultdict(list, dict_filter))
    if dict_filter['region']:
        query['$and'].append({'region': {'$in': dict_filter['region']}})
    if dict_filter['country']:
        query['$and'].append({'country': {'$in': dict_filter['country']}})
    if dict_filter['pos']:
        query['$and'].append({'pos': {'$in': dict_filter['pos']}})
    if dict_filter['origin'] and dict_filter['destination']:
        od_build = []
        od = []
        for idx, item in enumerate(dict_filter['origin']):
            od_build.append({'origin': item, 'destination': dict_filter['destination'][idx]})
        for i in od_build:
            a = i['origin'] + i['destination']
            od.append({'od': a})
        query['$and'].append({'$or': od})
    from_obj = datetime.datetime.strptime(dict_filter['fromDate'], '%Y-%m-%d')
    to_obj = datetime.datetime.strptime(dict_filter['toDate'], '%Y-%m-%d')
    month_year = query_month_year_builder(from_obj.month, from_obj.year, to_obj.month, to_obj.year)
    query['$and'].append({'$or': month_year})
    # query for travel date
    # if dict_filter['region']:
    #     query['region'] = {'$in': dict_filter['region']}
    # if dict_filter['country']:
    #     query['country'] = {'$in': dict_filter['country']}
    # if dict_filter['pos']:
    #     query['pos'] = {'$in': dict_filter['pos']}
    # if dict_filter['compartment']:
    #     query['compartment'] = {'$in': dict_filter['compartment']}
    # if dict_filter['channel']:
    #     query['channel'] = {'$in': dict_filter['channel']}
    #
    # from_month = datetime.datetime.strptime(dict_filter['fromDate'], '%Y-%m-%d').month
    # from_year = datetime.datetime.strptime(dict_filter['fromDate'], '%Y-%m-%d').year
    # to_month = datetime.datetime.strptime(dict_filter['toDate'], '%Y-%m-%d').month
    # to_year = datetime.datetime.strptime(dict_filter['toDate'], '%Y-%m-%d').year
    # month_year = month_builder(from_month, from_year, to_month, to_year)
    # query['$or'] = month_year
    # print query
    return query


def get_revenue_pax(afilter):
    try:
        if 'JUP_DB_Market_Share' in db.collection_names():
            afilter = deepcopy(defaultdict(list, afilter))
            query = query_builder(afilter)
            response = dict()
            query['MarketingCarrier1'] = host
            # print query
            ppln_rev_pax = [
                {
                    '$match': query
                }
                ,
                {
                    '$group':
                        {
                            '_id': None,
                            'revenue': {'$sum': '$revenue'},
                            'pax': {'$sum': '$pax'}
                        }
                }
                ,
                {
                    '$out': result_collection_rev_pax
                }

            ]
            # print result_collection_rev_pax
            db.JUP_DB_Market_Share.aggregate(ppln_rev_pax, allowDiskUse=True)

            if result_collection_rev_pax in db.collection_names():
                # creating a cursor for mongodb result document
                rev_pax = db.get_collection(result_collection_rev_pax)
                # print rev_pax.count()
                if rev_pax.count() == 0: # Empty collection returned by the database
                    rev_pax.drop()
                    response = {
                        'tot_pax': 'NA',
                        'tot_revenue': 'NA'
                    }
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                        'jupter_AI/tiles/market/market_dashboard_screen.py method: get_revenue_pax',
                                                        get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list('There is no data for that query.')
                    obj_error.write_error_logs(datetime.datetime.now())

                elif rev_pax.count() == 1: # There must be exactly 1 document as the output of the pipeline
                    # print rev_pax[0]
                    rev = list(rev_pax.find(projection={'revenue': 1, '_id': 0}))
                    pax = list(rev_pax.find(projection={'pax': 1, '_id': 0}))
                    rev_pax.drop()
                    response = \
                        {
                            'tot_rev': rev[0][u'revenue'],
                            'tot_pax': pax[0][u'pax']
                        }

                else: # If multiple documents are returned by the database --> error
                    rev_pax.drop()
                    response = \
                        {
                        'tot_rev': 'NA',
                        'tot_pax': 'NA'
                    }
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                        'jupter_AI/tiles/market/market_dashboard_screen.py method: get_revenue_pax',
                                                        get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list('Expected 1 document, received ', rev_pax.count(), 'documents. Check pipeline.')
                    obj_error.write_error_logs(datetime.datetime.now())
            else:
                response = {
                    'tot_pax': 'NA',
                    'tot_revenue': 'NA'
                }
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/market/market_dashboard_screen.py method: get_revenue_pax',
                                                get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list('result_collection_rev_pax not created in the database. Check aggregate pipeline.')
                obj_error.write_error_logs(datetime.datetime.now())
            return response
        else: # Collection to query does not exist
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/market/market_dashboard_screen.py method: get_revenue_pax',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Cumulative_Market_share cannot be found in the database.')
            obj_error.write_error_logs(datetime.datetime.now())
    # to handle any other pythonic exception that may have occurred
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/market/market_dashboard_screen.py method: get_revenue_pax',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


def host_comp_market_share(afilter):
    try:
        if 'JUP_DB_Market_Share' in db.collection_names():
            afilter = deepcopy(defaultdict(list, afilter))  # use proper variable name
            query = query_builder(afilter)
            pipeline_user = [
                {
                    '$match': query
                }
                ,
                {

                    '$group':
                        {
                            '_id':
                                {
                                    'MarketingCarrier': '$MarketingCarrier1'
                                },
                            'pax': {'$sum': '$pax'}
                        }
                }
                ,
                # Getting market size for each airline
                {
                    '$project':
                        {
                            'airline': '$_id.MarketingCarrier',
                            'pax': '$pax'
                        }
                }
                ,
                {
                    '$sort': {'pax': -1}
                }
                ,
                # Getting entire market size irrespective of airline
                {
                    '$group':
                        {
                            '_id': None,
                            'market_size': {'$sum': '$pax'},
                            'al_details':
                                {
                                    '$push':
                                        {
                                            'airline': '$airline',
                                            'pax': '$pax'
                                        }
                                }
                        }
                }
                ,
                {
                    '$project':
                        {
                            '_id': 0,
                            'al_details': '$al_details',
                            'market_size': '$market_size'
                        }
                }
                ,
                {
                    '$unwind':
                            {
                                'path': '$al_details',
                                'includeArrayIndex': 'rank'
                            }
                }
                ,
                {
                    '$addFields':
                        {
                            'market_share':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$gt': ['$market_size',0]
                                                },
                                            'then':
                                                {
                                                    '$divide':
                                                        [
                                                            {
                                                                '$multiply':
                                                                    ['$al_details.pax', 100]
                                                            },
                                                            '$market_size'
                                                        ]
                                                },
                                            'else': 'NA'
                                        }
                                },
                            'rank': {'$add': ['$rank',1]}
                        }
                }
                ,
                {
                    '$project':
                        {
                            '_id': 0,
                            'airline': '$al_details.airline',
                            'market_share': '$market_share',
                            'rank': '$rank'
                        }
                }
                ,
                {
                    '$out': result_collection_ms
                }
            ]
            # print 'pipeline'
            # print query
            db.JUP_DB_Market_Share.aggregate(pipeline_user, allowDiskUse=True)
            if result_collection_ms in db.collection_names(): # check if resultant collection has been created in the db
                ms_collection = db.get_collection(result_collection_ms)
                lst_ms = list(ms_collection.find(projection={'airline': 1, 'market_share': 1, 'rank': 1, '_id': 0}))
                db[result_collection_ms].drop()
                if len(lst_ms) > 0: # check if resultant collection has data
                    response = dict()
                    response['host'] = dict()
                    response['competitor'] = dict()
                    for doc in lst_ms:
                        if doc['airline'] == Host_Airline_Code:
                            response['host'] = dict()
                            response['host'][Host_Airline_Code] = {
                                'market_share': doc['market_share'],
                                'rank': int(doc['rank'])
                            }
                        else:
                            response['competitor'][doc['airline']] = {
                                'market_share': doc['market_share'],
                                'rank': int(doc['rank'])
                            }
                    return response

                    # ms_collection.drop()
                    # response = \
                    #     {
                    #         'host_comp_market_share': lst_ms
                    #     }
                    # return response
                else: # If pipeline has returned an empty collection
                    ms_collection.drop()
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/market/market_dashboard_screen.py method: host_comp_market_share',
                                                get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list('There is no data.')
                    obj_error.write_error_logs(datetime.datetime.now())
            else:
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/market/market_dashboard_screen.py method: host_comp_market_share',
                                                get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list(str(result_collection_ms + ' not created in the database. Check aggregate pipeline.'))
                obj_error.write_error_logs(datetime.datetime.now())

        else:
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/market/market_dashboard_screen.py method: host_comp_market_share',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Market_share cannot be found in the database.')
            obj_error.write_error_logs(datetime.datetime.now())
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/market/market_dashboard_screen.py method: host_comp_market_share',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


def host_deployed_capacity(afilter):
    try:
        if 'JUP_DB_Market_Share' in db.collection_names() and \
                        'JUP_DB_Schedule_Capa' in db.collection_names():
            afilter = deepcopy(defaultdict(list, afilter))
            query = query_builder(afilter)

            prev_yr_from, prev_yr_to = prev_year_range(afilter['fromDate'], afilter['toDate'])
            apipeline_user = \
                [
                    {
                        '$match': query
                    }
                    ,
                    {
                        '$group':
                            {
                                '_id':
                                    {
                                        'od': '$od'
                                    }
                            }
                    }
                    ,
                    {
                        '$project':
                            {
                                '_id': 0,
                                'od': '$_id.od'
                            }
                    }
                    ,
                    {
                        '$lookup':
                            {
                                'from': 'JUP_DB_Schedule_Capa',
                                'localField': 'od',
                                'foreignField': 'od',
                                'as': 'for_capacity'
                            }
                    }
                    ,
                    {
                        '$addFields':
                            {
                                'for_capacity':
                                    {
                                        '$filter':
                                            {
                                                'input': '$for_capacity',
                                                'as': 'for_capacity',
                                                'cond':
                                                    {
                                                        '$or':
                                                            [
                                                                {
                                                                    '$and':
                                                                        [
                                                                            {'$gte': ['$$for_capacity.dep_date', prev_yr_from]},
                                                                            {'$lte': ['$$for_capacity.dep_date', prev_yr_to]}
                                                                        ]
                                                                }
                                                                ,
                                                                {
                                                                    '$and':
                                                                        [
                                                                            {'$gte': ['$$for_capacity.dep_date', afilter['fromDate']]},
                                                                            {'$lte': ['$$for_capacity.dep_date', afilter['toDate']]}
                                                                        ]
                                                                }
                                                            ]
                                                    }
                                            }
                                    }
                            }
                    }
                    ,
                    {
                        '$unwind': '$for_capacity'
                    }
                    ,
                    {
                        '$project':
                            {
                                '_id': 1,
                                'od': '$od',
                                'dep_date': '$for_capacity.dep_date',
                                'J': '$for_capacity.C',
                                'F': '$for_capacity.F',
                                'Y':
                                    {
                                        '$add': ['$for_capacity.Y', '$for_capacity.W']
                                    },
                                'total': '$for_capacity.Total',
                                'airline': '$for_capacity.airline',
                                'flight': '$for_capacity.flight_no',
                                'start_time': '$for_capacity.start_time'

                            }
                    }
                    ,
                    {
                        '$group':
                            {
                                '_id':
                                    {
                                        'airline': '$airline',
                                        'od': '$od',
                                        'dep_date': '$dep_date',
                                        'flight': '$flight'
                                    },
                                'Y':
                                    {
                                        '$push':
                                            {
                                                'capacity': '$Y',
                                                'compartment': 'Y'
                                            }
                                    },
                                'F':
                                    {
                                        '$push':
                                            {
                                                'capacity': '$F',
                                                'compartment': 'F'
                                            }
                                    },
                                'J':
                                    {
                                        '$push':
                                            {
                                                'capacity': '$J',
                                                'compartment': 'J'
                                            }
                                    }

                            }
                    }
                    ,
                    {
                        '$project':
                            {
                                '_id': 0,
                                'airline': '$_id.airline',
                                'od': '$_id.od',
                                'dep_date': '$_id.dep_date',
                                'start_time': '$_id.start_time',
                                'flight': '$_id.flight',
                                'compartment':
                                    {
                                        '$concatArrays':
                                            [
                                                '$Y',
                                                '$F',
                                                '$J'
                                            ]
                                    }

                            }
                    }
                    ,
                    {
                        '$unwind': '$compartment'
                    }
                    ,
                    {
                        '$project':
                            {
                                '_id': 1,
                                'airline': '$airline',
                                'od': '$od',
                                'dep_date': '$dep_date',
                                'start_time': '$start_time',
                                'flight': '$flight',
                                'compartment': '$compartment.compartment',
                                'capacity': '$compartment.capacity',
                                'user_compartment': afilter['compartment']
                            }
                    }
                    ,
                    {
                        '$redact':
                            {
                                '$cond':
                                    {
                                        'if':
                                            {
                                                '$and':
                                                    [
                                                        {
                                                            '$in': ['$compartment','$user_compartment']
                                                        },
                                                    ]
                                            },
                                        'then': '$$DESCEND',
                                        'else':
                                            {
                                                '$cond':
                                                    {
                                                        'if':
                                                            {
                                                                '$eq': [{'$size': '$user_compartment'},0]
                                                            },
                                                        'then': '$$KEEP',
                                                        'else': '$$PRUNE'
                                                    }
                                            }
                                    }
                            }
                    }
                    ,
                    {
                        '$group':
                            {
                                '_id':
                                    {
                                        'airline': '$airline',
                                        'index_sample': '$index_sample'
                                    },
                                'capacity':
                                    {
                                        '$sum':
                                            {
                                                '$cond':
                                                    {
                                                        'if':
                                                            {
                                                                '$and':
                                                                    [
                                                                        {'$gte': ['$dep_date', afilter['fromDate']]},
                                                                        {'$lte': ['$dep_date', afilter['toDate']]}
                                                                    ]
                                                            },
                                                        'then': '$capacity',
                                                        'else': 0
                                                    }
                                            }
                                    },
                                'capacity_lyr':
                                    {
                                        '$sum':
                                            {
                                                '$cond':
                                                    {
                                                        'if':
                                                            {
                                                                '$and':
                                                                    [
                                                                        {'$gte': ['$dep_date', prev_yr_from]},
                                                                        {'$lte': ['$dep_date', prev_yr_to]}
                                                                    ]
                                                            },
                                                        'then': '$capacity',
                                                        'else': 0
                                                    }
                                            }
                                    }
                            }
                    }
                    ,
                    {
                        '$sort': {'capacity': -1}
                    }
                    ,
                    {
                        '$group':
                            {
                                '_id': None,
                                'capacity_airline':
                                    {
                                        '$push':
                                            {
                                                'airline': '$_id.airline',
                                                'capacity': '$capacity',
                                                'capacity_lyr': '$capacity_lyr'
                                            }
                                    }
                            }
                    }
                    ,
                    {
                        '$project':
                            {
                                '_id': 0,
                                'capacity_airline': '$capacity_airline'
                            }
                    }
                    ,
                    {
                        '$unwind':
                            {
                                'path': '$capacity_airline',
                                'includeArrayIndex': 'rank'
                            }
                    }
                    ,
                    {
                        '$addFields':
                            {
                                'rank': {'$add': ['$rank',1]},
                                'capacity_vlyr':
                                    {
                                        '$cond':
                                            {
                                                'if':
                                                    {
                                                        '$gt': ['$capacity_airline.capacity_lyr', 0]
                                                    },
                                                'then':
                                                    {
                                                        '$multiply':
                                                            [
                                                                100,
                                                                {
                                                                    '$subtract':
                                                                        [
                                                                            {
                                                                                '$divide':
                                                                                    [
                                                                                        '$capacity_airline.capacity',
                                                                                        '$capacity_airline.capacity_lyr'
                                                                                    ]
                                                                            },
                                                                            1
                                                                        ]
                                                                }
                                                            ]
                                                    },
                                                'else': 'NA'
                                            }
                                    }
                            }
                    }
                    ,
                    {
                        '$match':
                            {
                                'capacity_airline.airline': host
                            }
                    }
                    ,
                    {
                        '$project':
                            {
                                'airline': '$capacity_airline.airline',
                                'capacity': '$capacity_airline.capacity',
                                'capacity_vlyr': '$capacity_vlyr',
                                'rank': '$rank'
                            }
                    }
                    ,
                    {
                        '$out': result_collection_demo_capacity
                    }
                ]
            db.JUP_DB_Sales.aggregate(apipeline_user, allowDiskUse=True)
            if result_collection_demo_capacity in db.collection_names():
                capacity_collection = db.get_collection(result_collection_demo_capacity)
                if capacity_collection.count() > 0:
                    list_capacity = list(capacity_collection.find())
                    response = \
                        {
                            'capacity': list_capacity[0]['capacity'],
                            'capacity_ly': list_capacity[0]['capacity_vlyr'],
                            'rank': int(list_capacity[0]['rank'])
                        }
                    return response

                else:
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/market/market_dashboard_screen.py method: host_deployed_capacity',
                                                get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list('There is no data.')

                    obj_error.write_error_logs(datetime.datetime.now())
            else:
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/market/market_dashboard_screen.py method: host_deployed_capacity',
                                                get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list(str(result_collection_capacity + ' not created in the database. Check aggregate pipeline.'))
                obj_error.write_error_logs(datetime.datetime.now())

        else:
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/market/market_dashboard_screen.py method: host_deployed_capacity',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Market_Share or JUP_DB_Schedule_Capa'
                                           ' could not be found in the database.')
            obj_error.write_error_logs(datetime.datetime.now())
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/market/market_dashboard_screen.py method: host_deployed_capacity',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


def host_rank(afilter):
    try:
        if 'JUP_DB_Cumulative_Market_share' in db.collection_names():
            afilter = deepcopy(defaultdict(list, afilter))
            query = query_builder(afilter)

            ppln_host_rank = [
                {
                    '$match': query
                }
                ,
                {
                    '$group':
                        {
                            '_id':
                             {
                                 # 'month': '$month',
                                 # 'year': '$year',
                                 'airline':'$MarketingCarrier1'
                             },
                            'revenue': {'$sum': '$revenue'},
                            'pax': {'$sum': '$pax'}
                        }
                }
                ,
                {
                    '$group':
                        {
                            '_id':
                                {
                                    'airline': '$_id.airline'
                                },
                            'from_pax': {'$min': '$pax'},
                            'to_pax': {'$max': '$pax'}
                        },
                }
                ,
                {

                    '$project':
                        {
                            '_id': 0,
                            'airline': '$_id.airline',
                            'pax': {'$subtract':['$to_pax', '$from_pax']}
                        }
                }
                ,
                {
                    '$sort': {'pax': -1}
                }
                ,
                {
                     '$group':
                        {
                             '_id': None,
                             'pax_airline':
                                 {
                                     '$push':
                                         {
                                             'airline': '$airline',
                                             'pax': '$pax'
                                         }
                                 }
                        }
                }
                ,
                {
                    '$project':
                        {
                            '_id': None,
                            'pax_airline': 'pax_airline'
                        }
                }
                ,
                {
                    '$unwind':
                        {
                            'path': '$pax_airline',
                            'includeArrayIndex':'rank'
                        }
                }
                ,
                {
                    '$match':
                        {
                            'pax_airline.airline': host
                        }
                }
                ,
                {
                    '$project':
                        {
                            '_id': 0,
                            'host_rank': '$pax_airline.rank'
                        }
                }
                ,
                {
                    '$out': result_host_rank_coll
                }

            ]
            db.JUP_DB_Cumulative_Market_share.aggregate(ppln_host_rank, allowDiskUse=True)

            if result_host_rank_coll in db.collection_names():
                host_rank = db.get_collection(result_host_rank_coll)
                if host_rank.count > 0:
                    lst_host_rank = list(host_rank.find(projection={'host_rank': 1, '_id': 0}))
                    host_rank.drop()
                    response = \
                        {
                            'host_rank': lst_host_rank[0]
                        }
                    return response
                else:
                    host_rank.drop()
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/market/market_dashboard_screen.py method: host_deployed_capacity',
                                                get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list('There is no data.')
                    obj_error.write_error_logs(datetime.datetime.now())
            else:
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/market/market_dashboard_screen.py method: host_deployed_capacity',
                                                get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list('Resultant host rank collection not created in the database. Check aggregate pipeline.')
                obj_error.write_error_logs(datetime.datetime.now())

        else:
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/market/market_dashboard_screen.py method: host_rank',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Cumulative_Market_share could not be found in the database.')
            obj_error.write_error_logs(datetime.datetime.now())
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/market/market_dashboard_screen.py method: host_rank',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


def get_tiles(afilter):
    # print 'get_tiles'
    response = dict()
    response['get_revenue_pax'] = get_revenue_pax(afilter)
    # response['host_rank'] = host_rank(afilter)
    response['host_deployed_capacity'] = host_deployed_capacity(afilter)
    response['host_comp_market_share'] = host_comp_market_share(afilter)
    return response


if __name__ == '__main__':
    a = {
        'region': [],
        'country': [],
        'pos': [],
        'origin': ['DXB'],
        'destination': ['DOH'],
        'compartment': ['Y'],
        'fromDate': '2017-02-14',
        'toDate': '2017-02-20'
    }
    start_time = time.time()
    print get_tiles(a)
    print (time.time() - start_time)