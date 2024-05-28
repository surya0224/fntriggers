"""
File Name              : market_dashboard_screen
Author                 : Ashwin Kumar
Date Created           : 2016-12-29
Description            : The tiles present in the market dashboard are calulated over here.
                         the tiles are host revenue and pax, host rank, deployed capacity and capacity vlyr,
                         competitor rank and market share.

MODIFICATIONS LOG
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :
"""

import os
import sys

root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir, os.path.pardir))
sys.path.append(root_dir)
from jupiter_AI import client, JUPITER_DB
db = client[JUPITER_DB]
from collections import defaultdict
import pymongo
from copy import deepcopy
import datetime
import time
import inspect
from jupiter_AI.common import ClassErrorObject as error_class

from jupiter_AI.network_level_params import Host_Airline_Code as host
from jupiter_AI.tiles.jup_common_functions_tiles import gen_collection_name as gen
from jupiter_AI.tiles.jup_common_functions_tiles import query_month_year_builder as month_builder
from jupiter_AI.tiles.jup_common_functions_tiles import prev_year_range

result_collection_capacity = gen()
result_collection_demo_capacity = 'JUP_Demo_Host_Capacity'

"""
Try to break up the working into smaller functions like for query_builder
Split file into DAL and BLL, all python processing to be done in BLL
Build and fire the queries in DAL
Leave sufficient lines when a piece of logic is completed
"""

#def get_module_name():
#    return inspect.stack()[1][3]


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

def get_revenue_pax(afilter): # use proper variable name
    try:
        if 'JUP_DB_Cumulative_Market_share' in db.collection_names():
            # use proper variable name
            afilter = deepcopy(defaultdict(list, afilter)) #To be done in BLL and pass to this function
            query = dict() #use proper variable name
            response = dict()
            if afilter['region']:
                query['region'] = {'$in': afilter['region']}
            if afilter['country']:
                query['country'] = {'$in': afilter['country']}
            if afilter['pos']:
                query['pos'] = {'$in': afilter['pos']}
            if afilter['compartment']:
                query['compartment'] = {'$in': afilter['compartment']}
            if afilter['channel']:
                query['channel'] = {'$in': afilter['channel']}

            from_month = datetime.datetime.strptime(afilter['fromDate'], '%Y-%m-%d').month
            from_year = datetime.datetime.strptime(afilter['fromDate'], '%Y-%m-%d').year
            to_month = datetime.datetime.strptime(afilter['toDate'], '%Y-%m-%d').month
            to_year = datetime.datetime.strptime(afilter['toDate'], '%Y-%m-%d').year
            m = deepcopy(to_month) # use proper variable names
            y = deepcopy(to_year) # use proper variable names
            month_year = []

            # what's going on here?
            while True:
                month_year.append({'month': m,
                                   'year': y})
                if m == from_month and y == from_year:
                    break
                m -= 1
                if m == 0:
                    m = 12
                    y -= 1

            query['$or'] = month_year
            query['MarketingCarrier1'] = host

            # use proper variable name
            apipeline_user = [
                {
                    '$match': query
                },
                {
                    '$group': {'_id':None,
                               'revenue': {'$sum': '$revenue'},
                               'pax': {'$sum': '$pax'}
                               }
                } # Use $out : result_collection_name

            ]
            #use proper variable name
            cursor_user = db.JUP_DB_Cumulative_Market_share.aggregate(apipeline_user)
            '''
                if result_collection_name in db.collection_names():
                    if db.get_collection(collection_name).count > 0:
                        # processing
                        # drop collection in finally block
                    else:
                        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupter_AI/tiles/market/market_dashboard_screen.py method: get_revenue_pax',
                                                    get_arg_lists(inspect.currentframe()))
                        obj_error.append_to_error_list('There is no data.')

                        obj_error.write_error_logs(datetime.datetime.now())
                else:
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupter_AI/tiles/market/market_dashboard_screen.py method: get_revenue_pax',
                                                    get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list(str(result_collection_name + ' not created in the database. Check aggregate pipeline.'))

                    obj_error.write_error_logs(datetime.datetime.now())
                '''


            data_user = list(cursor_user) #use proper variable name #try to do all processing within mongodb
            tot_pax_user = 0
            tot_revenue_user = 0
            for i in data_user:
                tot_pax_user += i['pax']
                tot_revenue_user += i['revenue']


            # only create the response dictionary if the resultant collection is created
            response = {
                'tot_pax_user': tot_pax_user,
                'tot_pax_revenue': tot_revenue_user
            }

            return response
        else:
            #print 'Collection does not exist'
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/market/market_dashboard_screen.py method: get_revenue_pax',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Cumulative_Market_share cannot be found in the database.')
            # raise obj_error
            obj_error.write_error_logs(datetime.datetime.now())
    # to handle any pymongo errors
    except (pymongo.errors.ServerSelectionTimeoutError,
                    pymongo.errors.AutoReconnect,
                    pymongo.errors.CollectionInvalid,
                    pymongo.errors.ConfigurationError,
                    pymongo.errors.ConnectionFailure,
                    pymongo.errors.CursorNotFound,
                    pymongo.errors.ExecutionTimeout
                    ) as error_msg:
        #return error_msg
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'pymongo exception in jupter_AI/tiles/market/market_dashboard_screen.py method: get_revenue_pax',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        # raise obj_error
        obj_error.write_error_logs(datetime.datetime.now())
    # to handle any other pythonic exception that may have occurred
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/market/market_dashboard_screen.py method: get_revenue_pax',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        # raise obj_error
        obj_error.write_error_logs(datetime.datetime.now())


# name the function as get_ .....
def host_comp_market_share(afilter):
    try:
        if 'JUP_DB_Cumulative_Market_share' in db.collection_names():
            afilter = deepcopy(defaultdict(list, afilter)) #use proper variable name
            query = dict() #use proper variable name
            response = dict()
            if afilter['region']:
                query['region'] = {'$in': afilter['region']}
            if afilter['country']:
                query['country'] = {'$in': afilter['country']}
            if afilter['pos']:
                query['pos'] = {'$in': afilter['pos']}
            if afilter['compartment']:
                query['compartment'] = {'$in': afilter['compartment']}
            if afilter['channel']:
                query['channel'] = {'$in': afilter['channel']}

            from_month = datetime.datetime.strptime(afilter['fromDate'], '%Y-%m-%d').month
            from_year = datetime.datetime.strptime(afilter['fromDate'], '%Y-%m-%d').year
            to_month = datetime.datetime.strptime(afilter['toDate'], '%Y-%m-%d').month
            to_year = datetime.datetime.strptime(afilter['toDate'], '%Y-%m-%d').year
            m = deepcopy(to_month) # use proper variable name
            y = deepcopy(to_year) # use proper variable name
            month_year = []
            while True:
                month_year.append({'month': m,
                                   'year': y})
                if m == from_month and y == from_year:
                    break
                m -= 1
                if m == 0:
                    m = 12
                    y -= 1

            apipeline_user = [
                {
                    '$match': query
                },
                {
                    # if you can change MarketCarrier to some other name by projecting, please do
                    '$group': {'_id': {'MarketingCarrier':'$MarketingCarrier1'},
                               'pax': {'$sum': '$pax'}
                               }
                } # use $out : result_collection_name
            ]
            # use proper variable names
            cursor_user = db.JUP_DB_Cumulative_Market_share.aggregate(apipeline_user)
            '''
                if result_collection_name in db.collection_names():
                    if db.get_collection(collection_name).count > 0:
                        # processing
                        # drop collection
                    else: # if collection is empty
                        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupter_AI/tiles/market/market_dashboard_screen.py method: host_comp_market_share',
                                                    get_arg_lists(inspect.currentframe()))
                        obj_error.append_to_error_list('There is no data.')

                        obj_error.write_error_logs(datetime.datetime.now())
                else: # if aggregate fails and collection is not created
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupter_AI/tiles/market/market_dashboard_screen.py method: host_comp_market_share',
                                                    get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list(str(result_collection_name + ' not created in the database. Check aggregate pipeline.'))

                    obj_error.write_error_logs(datetime.datetime.now())
            '''
            pre_sort_data = list(cursor_user) # use proper variable name
            data_user = sorted(pre_sort_data, key=lambda k: k['pax'], reverse=True) # use proper variable name # sort in mongodb itself
            #print data_user
            comp_count = 0
            host_count = 0
            top3_competitors = defaultdict(dict)
            host_al = [] # what's this?
            tot_pax = 0
            # Leave line space
            # What's ging on here?
            if len(data_user) != 0:
                for i in data_user: # use proper variable name for 'i'
                    tot_pax += int(i['pax'])
                    #print i
                for i in data_user: # use proper variable name for 'i'
                    #print i
                    if i['_id']['MarketingCarrier'] != host: # why 4?
                        comp_count += 1
                        market_share = (float(i['pax'])/tot_pax) * 100
                        # top3_competitors.append([i['_id']['MarketingCarrier'], i['pax'], market_share])
                        # exec ("%s = %d" % (i['_id']['MarketingCarrier'].encode(),
                        #                    i['_id']['MarketingCarrier'].encode().))
                        top3_competitors[i['_id']['MarketingCarrier'].encode()] = {
                            'market_share': market_share,
                            'pax': float("{0:.2f}".format(i['pax'])),
                            'competitor_rank': comp_count
                        }
                    elif i['_id']['MarketingCarrier'] == host:
                        market_share = (float(i['pax']) / tot_pax) * 100

                        host_al.append({'host': i['_id']['MarketingCarrier'].encode(),
                                        'pax': float("{0:.2f}".format(i['pax'])),
                                        'market_share': market_share})
                        host_count += 1
                    else:
                        pass

            response = {
                'host': host_al,
                'competitor': top3_competitors
            }
            return response
        else:
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/market/market_dashboard_screen.py method: host_comp_market_share',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Cumulative_Market_share cannot be found in the database.')
            # raise obj_error
            obj_error.write_error_logs(datetime.datetime.now())
    # to handle any pymongo errors
    except (pymongo.errors.ServerSelectionTimeoutError,
            pymongo.errors.AutoReconnect,
            pymongo.errors.CollectionInvalid,
            pymongo.errors.ConfigurationError,
            pymongo.errors.ConnectionFailure,
            pymongo.errors.CursorNotFound,
            pymongo.errors.ExecutionTimeout
            ) as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'pymongo exception in jupter_AI/tiles/market/market_dashboard_screen.py method: host_comp_market_share',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        # raise obj_error
        obj_error.write_error_logs(datetime.datetime.now())
    # to handle any other pythonic exception that may have occurred
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/market/market_dashboard_screen.py method: host_comp_market_share',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        # raise obj_error
        obj_error.write_error_logs(datetime.datetime.now())
    finally:
        # drop result_collection_name
        pass

# name the function as get_ .....
def host_deployed_capacity(afilter):
    try:
        if 'JUP_DB_Market_Share' in db.collection_names() and \
                        'JUP_DB_Schedule_Capa' in db.collection_names():
            afilter = deepcopy(defaultdict(list, afilter)) #use proper variable name
            query = defaultdict(list)
            response = dict()
            if afilter['region']:
                query['$and'].append({'region': {'$in': afilter['region']}})
            if afilter['country']:
                # query['country'] = {'$in': afilter['country']}
                query['$and'].append({'country': {'$in': afilter['country']}})
            if afilter['pos']:
                # query['pos'] = {'$in': afilter['pos']}
                query['$and'].append({'pos': {'$in': afilter['pos']}})
            if afilter['compartment']:
                # query['compartment'] = {'$in': afilter['compartment']}
                query['$and'].append({'compartment': {'$in': afilter['compartment']}})
            if afilter['origin']:
                if afilter['destination']:
                    od_build = []
                    for idx, item in enumerate(afilter['origin']):
                        od_build.append({'origin': item, 'destination': afilter['destination'][idx]})
                    query['$and'].append({'$or': od_build})

            from_month = datetime.datetime.strptime(afilter['fromDate'], '%Y-%m-%d').month
            from_year = datetime.datetime.strptime(afilter['fromDate'], '%Y-%m-%d').year
            to_month = datetime.datetime.strptime(afilter['toDate'], '%Y-%m-%d').month
            to_year = datetime.datetime.strptime(afilter['toDate'], '%Y-%m-%d').year
            month_year = month_builder(from_month, from_year, to_month, to_year)
            # query['$or'] = month_year
            query['$and'].append({'$or': month_year})

            prev_yr_from, prev_yr_to = prev_year_range(afilter['fromDate'], afilter['toDate'])
            # print prev_year_range(afilter['fromDate'], afilter['toDate'])
            print prev_yr_from, prev_yr_to

            # query['MarketingCarrier1'] = host
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
                    # ,
                    # {
                    #     '$redact':
                    #         {
                    #             '$cond':
                    #                 {
                    #                     'if':
                    #                         {
                    #                             '$or':
                    #                                 [
                    #                                     {
                    #                                         '$and':
                    #                                             [
                    #                                                 {'$gte': ['$dep_date', prev_yr_from]},
                    #                                                 {'$lte': ['$dep_date', prev_yr_to]}
                    #                                             ]
                    #                                     }
                    #                                     ,
                    #                                     {
                    #                                         '$and':
                    #                                             [
                    #                                                 {'$gte': ['$dep_date', afilter['fromDate']]},
                    #                                                 {'$lte': ['$dep_date', afilter['toDate']]}
                    #                                             ]
                    #                                     }
                    #                                 ]
                    #                         },
                    #                     'then': '$$DESCEND',
                    #                     'else': '$$PRUNE'
                    #                 }
                    #         }
                    # }
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
            # use proper variable name
            cursor_user = db.JUP_DB_Sales.aggregate(apipeline_user)
            if result_collection_demo_capacity in db.collection_names():
                capacity_collection = db.get_collection(result_collection_demo_capacity)
                # print capacity_collection.count()
                if capacity_collection.count() != 0:
                    # print 'hello'
                    # capacity_host = 0
                    # capacity_host_w = 0
                    # capacity_host_y = 0
                    # capacity_host_j = 0
                    # capacity_host_f = 0
                    list_capacity = list(capacity_collection.find())
                    # for i in list_capacity:
                    #     # print i
                    #     if i['_id']['airline'] == host:
                    #         # print 'hello'
                    #         capacity_host = i['capacity']
                    #         capacity_host_w = i['capacity_w']
                    #         capacity_host_y = i['capacity_y']
                    #         capacity_host_j = i['capacity_c']
                    #         capacity_host_f = i['capacity_f']
                    # # print capacity_host, capacity_host_y, capacity_host_f, capacity_host_j, capacity_host_w
                    # if afilter['compartment']:
                    #     if afilter['compartment'] == ["J"] or afilter['compartment'] == ['J']:
                    #         show_capacity = capacity_host_j
                    #         # print 'hello'
                    #     elif afilter['compartment'] == ['Y'] or afilter['compartment'] == ["Y"]:
                    #         show_capacity = capacity_host_y
                    #         # print 'hello'
                    #     else:
                    #         show_capacity = capacity_host
                    #         # print 'hello'
                    # else:
                    #     show_capacity = capacity_host

                    response = \
                        {
                            'capacity': list_capacity[0]['capacity'],
                            'capacity_ly': list_capacity[0]['capacity_vlyr'],
                            'rank': int(list_capacity[0]['rank'])
                        }
                    # capacity_collection.drop()

                    return response

                else:
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/market/market_dashboard_screen.py method: host_deployed_capacity',
                                                get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list('There is no data.')

                    obj_error.write_error_logs(datetime.datetime.now())
                    # capacity_collection.drop()
            else:
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/market/market_dashboard_screen.py method: host_deployed_capacity',
                                                get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list(str(result_collection_capacity + ' not created in the database. Check aggregate pipeline.'))

                obj_error.write_error_logs(datetime.datetime.now())

            # data_user = list(cursor_user)

        #     for i in cursor_user:
        #         print i
        #
        #     # list_of_od = set()
        #     # for i in data_user: # use proper variable name for 'i'
        #     #     if i['od']:
        #     #         list_of_od.add(i['od'])
        #     # list_of_od = list(list_of_od)
        #     # print len(list_of_od)
        #     # for i in list_of_od:
        #     #     print i
        #
        #     # WOOWWWW WHAT'S GOING ON?!?!
        #     list_of_pos= ast.literal_eval(json.dumps(list_of_pos)) # better variable name: lst_pos
        #     list_of_pos = list(list_of_pos)
        #     from_date = str(from_year) + '-' + str(from_month) + '-' + '01'
        #     to_date =  str(to_year) + '-' + str(to_month) + '-' + str(calendar.monthrange(int(to_year), int(to_month))[1])
        #     from_year_lyr = from_year - 1
        #     to_year_lyr = to_year - 1
        #     from_date_lyr = str(from_year_lyr) + '-' + str(from_month) + '-' + '01'
        #     to_date_lyr = str(to_year_lyr) + '-' + str(to_month) + '-' + str(calendar.monthrange(int(to_year),
        #                                                                                           int(to_month))[1])
        #     from_date = datetime.datetime.strptime(from_date, '%Y-%m-%d').strftime('%Y-%m-%d')
        #     to_date = datetime.datetime.strptime(to_date, '%Y-%m-%d').strftime('%Y-%m-%d')
        #     from_date_lyr = datetime.datetime.strptime(from_date_lyr, '%Y-%m-%d').strftime('%Y-%m-%d')
        #     to_date_lyr = datetime.datetime.strptime(to_date_lyr, '%Y-%m-%d').strftime('%Y-%m-%d')
        #     query1 = dict() # use proper variable name
        #     if list_of_pos:
        #         query1['origin'] = {'$in': list_of_pos}
        #     if afilter['compartment']:
        #         query1['compartment'] = {'$in': query['compartment']}
        #     query1['destination'] = hub
        #     query1['airline'] = host
        #     query2 = deepcopy(query1) # use proper variable name
        #     query1['effective_from'] = {'$gte': from_date, '$lte': to_date}
        #
        #     query2['effective_from'] = {'$gte': from_date_lyr, '$lte': to_date_lyr}
        #     apipeline_user1 = [
        #         {
        #             '$match': query1
        #         }
        #     ]
        #     # cursor_user1 = db.JUP_DB_Schedule_Capacity.aggregate(apipeline_user1) # use proper variable name
        #     '''
        #         if result_collection_name in db.collection_names():
        #             if db.get_collection(result_collection_name).count > 0:
        #                 # processing
        #                 # drop collection
        #             else:
        #                 obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
        #                                             'jupter_AI/tiles/market/market_dashboard_screen.py method: host_deployed_capacity',
        #                                             get_arg_lists(inspect.currentframe()))
        #                 obj_error.append_to_error_list('There is no data.')
        #
        #                 obj_error.write_error_logs(datetime.datetime.now())
        #         else:
        #             obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
        #                                             'jupter_AI/tiles/market/market_dashboard_screen.py method: host_deployed_capacity',
        #                                             get_arg_lists(inspect.currentframe()))
        #             obj_error.append_to_error_list(str(result_collection_name + ' not created in the database. Check aggregate pipeline.'))
        #
        #             obj_error.write_error_logs(datetime.datetime.now())
        #     '''
        #     data_user1 = list(cursor_user1) # use proper variable name
        #     total_capacity = 0
        #     for i in data_user1: # use proper variable name in 'i'
        #        date_effective = datetime.datetime.strptime(i['effective_to'], "%Y-%m-%d").strftime('%Y-%m-%d')
        #        if date_effective > to_date:
        #            i['effective_to'] = to_date
        #        frequency = i['frequency']
        #        frequency = ast.literal_eval(json.dumps(frequency))
        #        frequency = list(frequency) # use proper variable name, like lst_frequency or lst_freq
        #        daterange = pd.date_range(datetime.datetime.strptime(i['effective_from'], "%Y-%m-%d"),
        #                                  datetime.datetime.strptime(i['effective_to'], "%Y-%m-%d"))
        #
        #        for single_date in daterange:
        #            if str(single_date.isoweekday()) in frequency:
        #                total_capacity += int(i['capacity'])
        #
        #     # Last year Capacity
        #
        #     apipeline_user2 = [ # use proper variable name
        #         {
        #             '$match': query2
        #         } # use $out : result_collection_name
        #     ]
        #     cursor_user2 = db.JUP_DB_Schedule_Capacity.aggregate(apipeline_user2) # use proper variable name
        #     '''
        #         if result_collection_name in db.collection_names():
        #             if db.get_collection(collection_name).count > 0:
        #                 # processing
        #                 # drop collection
        #             else:
        #                 obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
        #                                             'jupter_AI/tiles/market/market_dashboard_screen.py method: host_deployed_capacity',
        #                                             get_arg_lists(inspect.currentframe()))
        #                 obj_error.append_to_error_list('There is no data.')
        #
        #                 obj_error.write_error_logs(datetime.datetime.now())
        #         else:
        #             obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
        #                                             'jupter_AI/tiles/market/market_dashboard_screen.py method: host_deployed_capacity',
        #                                             get_arg_lists(inspect.currentframe()))
        #             obj_error.append_to_error_list(str(result_collection_name + ' not created in the database. Check aggregate pipeline.'))
        #
        #             obj_error.write_error_logs(datetime.datetime.now())
        #     '''
        #     data_user2 = list(cursor_user2) # use proper variable name
        #     total_capacity_lyr = 0
        #     # what's going on here?
        #     for i in data_user2: # use proper variable name for 'i'
        #         date_effective = datetime.datetime.strptime(i['effective_to'], "%Y-%m-%d").strftime('%Y-%m-%d')
        #         if date_effective > to_date:
        #             i['effective_to'] = to_date
        #         frequency = i['frequency']
        #         frequency = ast.literal_eval(json.dumps(frequency))
        #         frequency = list(frequency)
        #         # use underscores while using multiple words for variable names
        #         daterange = pd.date_range(datetime.datetime.strptime(i['effective_from'], "%Y-%m-%d"),
        #                                   datetime.datetime.strptime(i['effective_to'], "%Y-%m-%d"))
        #
        #         for single_date in daterange:
        #             if str(single_date.isoweekday()) in frequency:
        #                 total_capacity_lyr += int(i['capacity'])
        #     if total_capacity_lyr != 0 and total_capacity != 0:
        #         vlyr = (float(total_capacity_lyr-total_capacity)/total_capacity_lyr)*100
        #     else:
        #         vlyr = 0
        #
        #     response = {
        #         'host_capacity': total_capacity,
        #         'vlyr': vlyr
        #     }
        #     return response
        else:
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/market/market_dashboard_screen.py method: host_deployed_capacity',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Market_Share or JUP_DB_Schedule_Capa'
                                           ' could not be found in the database.')
            # raise obj_error
            obj_error.write_error_logs(datetime.datetime.now())
    # to handle any pymongo errors
    # except (pymongo.errors.ServerSelectionTimeoutError,
    #                 pymongo.errors.AutoReconnect,
    #                 pymongo.errors.CollectionInvalid,
    #                 pymongo.errors.ConfigurationError,
    #                 pymongo.errors.ConnectionFailure,
    #                 pymongo.errors.CursorNotFound,
    #                 pymongo.errors.ExecutionTimeout
    #                 ) as error_msg:
    #     obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
    #                                         'pymongo exception in jupter_AI/tiles/market/market_dashboard_screen.py method: host_deployed_capacity',
    #                                         get_arg_lists(inspect.currentframe()))
    #     obj_error.append_to_error_list(str(error_msg))
    #     obj_error.write_error_logs(datetime.datetime.now())
    # # to handle any other pythonic exception that may have occurred
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/market/market_dashboard_screen.py method: host_deployed_capacity',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        # raise obj_error
        obj_error.write_error_logs(datetime.datetime.now())
    finally:
        # drop result_collection_name
        pass

    # use name get_ ... for function
def host_rank(afilter):
    try:
        if 'JUP_DB_Cumulative_Market_share' in db.collection_names():
            afilter = deepcopy(defaultdict(list, afilter)) # use proper variable name
            query = dict() # use proper variable name
            response = dict()
            if afilter['region']:
                query['region'] = {'$in': afilter['region']}
            if afilter['country']:
                query['country'] = {'$in': afilter['country']}
            if afilter['pos']:
                query['pos'] = {'$in': afilter['pos']}
            if afilter['compartment']:
                query['compartment'] = {'$in': afilter['compartment']}
            if afilter['channel']:
                query['channel'] = {'$in': afilter['channel']}

            from_month = datetime.datetime.strptime(afilter['fromDate'], '%Y-%m-%d').month
            from_year = datetime.datetime.strptime(afilter['fromDate'], '%Y-%m-%d').year
            to_month = datetime.datetime.strptime(afilter['toDate'], '%Y-%m-%d').month
            to_year = datetime.datetime.strptime(afilter['toDate'], '%Y-%m-%d').year
            m = deepcopy(to_month) # use proper variable name
            y = deepcopy(to_year) # use proper variable name
            month_year = [] # is months_year a better variable name?
            # listing out months in the range -- use this comment everywhere
            while True:
                month_year.append({'month': m,
                                   'year': y})
                if m == from_month and y == from_year:
                    break
                m -= 1
                if m == 0:
                    m = 12
                    y -= 1

            query['$or'] = month_year
            print query
            apipeline_user = [ # use proper variable name
                {
                    '$match': query
                },
                {
                    '$group': {'_id': {'month': '$month',
                                       'year': '$year',
                                       'MarketingCarrier':'$MarketingCarrier1'
                                       },
                               'revenue': {'$sum': '$revenue'},
                               'paxo': {'$sum': '$pax'} # use proper variable name
                               }
                },
                {
                    '$group': {'_id':{'MarketingCarrier': '$_id.MarketingCarrier'},
                               'frompax': {'$min': '$paxo'}, #use underscores
                               'topax': {'$max': '$paxo'} # use underscores
                               },
                },
                {

                    '$project': {'MarketingCarrier': '$_id.MarketingCarrier', # MarketingCarrier is a misleading variable name, please change
                                 'pax': {'$subtract':['$topax', '$frompax']}
                               }
                }
                # use $out: result_collection_name
            ]
            cursor_user = db.JUP_DB_Cumulative_Market_share.aggregate(apipeline_user) # use proper variable name
            '''
                if result_collection_name in db.collection_names():
                    if db.get_collection(collection_name).count > 0:
                        # processing
                        # drop collection
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
                    obj_error.append_to_error_list(str(result_collection_name + ' not created in the database. Check aggregate pipeline.'))

                    obj_error.write_error_logs(datetime.datetime.now())
            '''
            pre_sort_data = list(cursor_user) # use proper variable name
            data_user = sorted(pre_sort_data, key=lambda k: k['pax'], reverse=True) # use proper variable name
            rank = 0
            host_rank_value = 0
            if len(data_user) != 0:
                for i in data_user:
                    rank += 1
                    if i['MarketingCarrier'] == host:
                        host_rank_value = rank
                    else:
                        pass

            response = {
                'host_rank': host_rank_value
            }
            return response
        else:
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/market/market_dashboard_screen.py method: host_rank',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Cumulative_Market_share could not be found in the database.')
            # raise obj_error
            obj_error.write_error_logs(datetime.datetime.now())

    #to handle any pymongo errors
    except (pymongo.errors.ServerSelectionTimeoutError,
            pymongo.errors.AutoReconnect,
            pymongo.errors.CollectionInvalid,
            pymongo.errors.ConfigurationError,
            pymongo.errors.ConnectionFailure,
            pymongo.errors.CursorNotFound,
            pymongo.errors.ExecutionTimeout
            ) as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'pymongo exception in jupter_AI/tiles/market/market_dashboard_screen.py method: host_rank',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        # raise obj_error
        obj_error.write_error_logs(datetime.datetime.now())

    # to handle any other pythonic exception that may have occurred
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/market/market_dashboard_screen.py method: host_rank',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        # raise obj_error
        obj_error.write_error_logs(datetime.datetime.now())
    finally:
        # drop result_collection_name
        pass


def get_tiles(afilter): # use proper variable name
    response = dict()
    response['get_revenue_pax'] = get_revenue_pax(afilter)
    response['host_rank'] = host_rank(afilter)
    response['host_deployed_capacity'] = host_deployed_capacity(afilter)
    response['host_comp_market_share'] = host_comp_market_share(afilter)
    return response

def host_deployed_capacity_capa(afilter):
    try:
        if 'JUP_DB_Schedule_Capa' in db.collection_names():
            afilter = deepcopy(defaultdict(list, afilter)) #use proper variable name
            query = dict()
            response = dict()
            if afilter['region']:
                query['region'] = {'$in': afilter['region']}
            if afilter['country']:
                query['country'] = {'$in': afilter['country']}
            if afilter['pos']:
                query['pos'] = {'$in': afilter['pos']}
            if afilter['compartment']:
                query['compartment'] = {'$in': afilter['compartment']}
            query['dep_date'] = {'$gte': afilter['fromDate'], '$lte': afilter['toDate']}
            if afilter['origin']:
                od_build = []
                for idx, item in enumerate(afilter['origin']):  # use proper variable names for 'idx' and 'item'
                    od_build.append({'origin': item, 'destination': afilter['destination'][idx]})
                query['$or'] = od_build
            # use proper variable name
            apipeline_user = [
                {
                    '$match': query
                } # use $out : result_collection_name
                ]
            cursor_user = db.JUP_DB_Schedule_Capa.aggregate(apipeline_user)
            return 0
        else:
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupiter_AI/tiles/market/market_dashboard_screen.py method: host_deployed_capacity_capa',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Schedule_Capa could not be found in the database.')
            # raise obj_error
            obj_error.write_error_logs(datetime.datetime.now())
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/market/market_dashboard_screen.py method: host_rank',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        # raise obj_error
        obj_error.write_error_logs(datetime.datetime.now())



if __name__ == '__main__':
    a = {
        'region': [],
        'country': [],
        'pos': [],
        'origin': ['DXB'],
        'destination': ['DMM'],
        "compartment": [],
        'fromDate': '2016-07-01',
        'toDate': '2016-12-31'
    }
    start_time = time.time()
    print host_rank(afilter=a)
    # market_outlook = db.get_collection(collection_name)
    # market_outlook.drop()
    print (time.time() - start_time)
