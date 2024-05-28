"""
header!

MODIFICATIONS LOG
    S.No                   :    1
    Date Modified          :    2016-12-28
    By                     :    Shamail Mulla
    Modification Details   :    Added error / exception handling codes
"""
import os
import sys
import time
from collections import defaultdict
from copy import deepcopy

start_time = time.time()
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir, os.path.pardir, os.path.pardir))
sys.path.append(root_dir)
from jupiter_AI.network_level_params import Host_Airline_Code as host
from jupiter_AI.tiles.kpi.yield_rasm_seatfactor import yield_pricebiometrics
from jupiter_AI.tiles.jup_common_functions_tiles import query_month_year_builder as month_year1
from jupiter_AI import client, JUPITER_DB
db=client[JUPITER_DB]
import datetime
import inspect
import pymongo
from jupiter_AI.common import ClassErrorObject as error_class

from jupiter_AI.tiles.jup_common_functions_tiles import gen_collection_name

"""
Try to break up the working into smaller functions like for query_builder
Split file into DAL and BLL, all python processing to be done in BLL
Build and fire the queries in DAL
Leave sufficient lines when a piece of logic is completed
"""

# the below function generates a random name for collection
result_collection_name_yield = gen_collection_name()
result_collection_name_market_share = gen_collection_name()
result_collection_name_ticketed = gen_collection_name()


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


# use function written in common
def query_month_year_builder(stdm, stdy, endm, endy):
    m = deepcopy(endm)  # use proper variable names
    y = deepcopy(endy)  # use proper variable names
    month_year = []
    while True:
        month_year.append({'month': m,
                           'year': y})
        if m == stdm and y == stdy:
            break

        m -= 1
        if m == 0:
            m = 12
            y -= 1
    return month_year


# prefix with get_
def host_market_share(afilter):
    try:
        if 'JUP_DB_Market_Share' in db.collection_names():
            afilter = deepcopy(defaultdict(list, afilter))  # use proper variable names
            query = dict()  # use proper variable names
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
            from_obj = datetime.datetime.strptime(afilter['fromDate'], '%Y-%m-%d')
            to_obj = datetime.datetime.strptime(afilter['toDate'], '%Y-%m-%d')
            month_year = month_year1(from_obj.month, from_obj.year, to_obj.month, to_obj.year)
            query['$or'] = month_year

            apipeline_user = [  # use proper variable names
                # explain pipeline stages
                {
                    '$match': query
                }
                ,
                {
                    '$group':
                        {
                            '_id': None,
                            'pax': {'$sum': '$pax'},
                            'pax_host':
                                {
                                    '$sum':
                                        {
                                            '$cond':
                                                {
                                                    'if':
                                                        {
                                                            '$eq': ['$MarketingCarrier1', host]
                                                        },
                                                    'then': '$pax',
                                                    'else': 0
                                                }
                                        }
                                },
                            'pax_lyr': {'$sum': '$pax_1'},
                            'pax_host_lyr':
                                {
                                    '$sum':
                                        {
                                            '$cond':
                                                {
                                                    'if':
                                                        {
                                                            '$eq': ['$MarketingCarrier1', host]
                                                        },
                                                    'then': '$pax_1',
                                                    'else': 0
                                                }
                                        }

                                }
                        }
                }
                ,
                {
                    '$project':
                        {
                            '_id': 0,
                            'Airline': '$_id.MarketingCarrier',
                            'Market_Share':
                                {
                                    '$multiply':
                                        [
                                            {
                                                '$divide':
                                                 ['$pax_host', '$pax']
                                            },
                                            100
                                        ]
                                },
                            'Market_Share_lyr': {
                                '$cond':
                                    {
                                        'if':
                                            {
                                                '$gt': ['$pax_lyr', 0]
                                            },
                                        'then':
                                            {
                                                '$cond':
                                                    {
                                                        'if':
                                                            {
                                                                '$gt': ['$pax_host_lyr', 0]
                                                            },
                                                        'then':
                                                            {
                                                                '$multiply': [
                                                                    {'$divide': ['$pax_host_lyr', '$pax_lyr']}, 100]
                                                            },
                                                        'else': 0
                                                    }
                                            },
                                        'else': 0

                                    }
                            }
                        }  # use $out : result_collection_name
                }
                # ,
                # {
                #     '$out':
                # }
            ]
            cursor_user = db.JUP_DB_Market_Share.aggregate(apipeline_user)  # use proper variable names

            '''
                if result_collection_name in db.collection_names():
                    # pythonic variable given to the newly created collection
                    some_collection = db.get_collection(result_collection_name)
                    if some_collection.count > 0:

                        # retrieve relevant data from collection

                        some_collection.drop()
                        response = dict()

                        # logic

                    else: # in case the collection is empty
                        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                            'jupter_AI/tiles/price_biometrics/price_quote.py method: host_market_share',
                                                            get_arg_lists(inspect.currentframe()))
                        obj_error.append_to_error_list('There is no data.')

                        obj_error.write_error_logs(datetime.datetime.now())
                else: # result collection not created
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupter_AI/tiles/price_biometrics/price_quote.py method: host_market_share',
                                                    get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list(
                        str(result_collection_name + ' not created in the database. Check aggregate pipeline.'))

                    obj_error.write_error_logs(datetime.datetime.now())
            '''
            data_user = list(cursor_user)  # use proper variable names
            if len(data_user) != 0:
                for i in data_user:
                    Market_Share = round(i['Market_Share'], 2)  # use lower case everywhere!
                    Market_Share_lyr = i['Market_Share_lyr']
                    if Market_Share_lyr > 0:
                        Market_Share_vlyr = round((float(Market_Share - Market_Share_lyr) / Market_Share_lyr) * 100, 2)
                    else:
                        Market_Share_vlyr = None
            else:
                Market_Share = None
                Market_Share_vlyr = None

            response = {
                'Market_Share': Market_Share,
                'Market_Share_VLYR': Market_Share_vlyr
            }
            return response
        else:  # if collection to query is not found in the db
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/price_biometrics/price_quote.py method: host_market_share',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Market_Share cannot be found in the database.')
            obj_error.write_error_logs(datetime.datetime.now())
    # handling pymongo exceptions
    except (pymongo.errors.ServerSelectionTimeoutError,
            pymongo.errors.AutoReconnect,
            pymongo.errors.CollectionInvalid,
            pymongo.errors.ConfigurationError,
            pymongo.errors.ConnectionFailure,
            pymongo.errors.CursorNotFound,
            pymongo.errors.ExecutionTimeout
            ) as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'pymongo exception in jupter_AI/tiles/price_biometrics/price_quote.py method: host_market_share',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())
    # to handle any other pythonic exception that may have occurred
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/market/price_biometrics/price_quote.py method: host_market_share',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


# prefic with get_
def tickets_sold(afilter):
    try:
        if 'JUP_DB_Booking_DepDate' in db.collection_names():
            afilter = deepcopy(defaultdict(list, afilter))  # use proper variable names
            query = dict()  # use proper variable names
            response = dict()
            flag = 0  # use proper variable names, what does th flag indicate?
            if afilter['region']:
                query['region'] = {'$in': afilter['region']}
                flag = 1
            if afilter['country']:
                query['country'] = {'$in': afilter['country']}
                flag = 1
            if afilter['pos']:
                query['pos'] = {'$in': afilter['pos']}
                flag = 1
            if afilter['compartment']:
                query['compartment'] = {'$in': afilter['compartment']}
            if afilter['origin'] and afilter['destination']:
                od_build = []
                od = []
                for idx, item in enumerate(afilter['origin']):  # use proper variable names for 'idx' and 'item'
                    od_build.append({'origin': item, 'destination': afilter['destination'][idx]})
                for i in od_build:
                    a = i['origin'] + i['destination']  # a is what?? use proper name
                    od.append({'od': a})
                query['$or'] = od

            query['dep_date'] = {'$gte': afilter['fromDate'], '$lte': afilter['toDate']}  # use proper variable names
            apipeline_user = \
                [  # use proper variable names
                    # explain pipeline stages
                    {
                        '$match': query
                    },
                    {
                        '$group':
                            {
                                '_id': None,
                                'ticket': {'$sum': '$ticket'},
                                'ticket_lyr': {'$sum': '$ticket_1'}

                            }
                    }
                    ,
                    {
                        '$project':
                            {
                                '_id': 0,
                                'Ticketed': '$ticket',
                                'Ticketed_lyr': '$ticket_lyr',
                                'Ticketed_vlyr':
                                    {
                                        '$cond':
                                            {
                                                'if':
                                                    {
                                                        '$gt': ['$ticket_lyr', 0]
                                                    },
                                                'then':
                                                    {
                                                        '$cond':
                                                            {
                                                                'if':
                                                                    {
                                                                        '$gt': ['$ticket', 0]
                                                                    },
                                                                'then':
                                                                    {
                                                                        '$multiply': [100,
                                                                                      {
                                                                                          '$divide':
                                                                                              [
                                                                                                  {
                                                                                                      '$subtract': [
                                                                                                          '$ticket',
                                                                                                          '$ticket_lyr']
                                                                                                  },
                                                                                                  '$ticket_lyr']
                                                                                      }
                                                                                      ]
                                                                    },
                                                                'else': 0
                                                            }
                                                    },
                                                'else': 0
                                            }
                                    }
                            }
                    }
                ]
            cursor_user = db.JUP_DB_Booking_DepDate.aggregate(apipeline_user)  # use proper variable names

            '''
                if result_collection_name in db.collection_names():
                    # pythonic variable given to the newly created collection
                    some_collection = db.get_collection(result_collection_name)
                    if some_collection.count > 0:

                        # retrieve relevant data from collection

                        some_collection.drop()
                        response = dict()

                        # logic

                    else: # in case the collection is empty
                        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                            'jupter_AI/tiles/price_biometrics/price_quote.py method: tickets_sold',
                                                            get_arg_lists(inspect.currentframe()))
                        obj_error.append_to_error_list('There is no data.')

                        obj_error.write_error_logs(datetime.datetime.now())
                else: # result collection not created
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupter_AI/tiles/price_biometrics/price_quote.py method: tickets_sold',
                                                    get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list(
                        str(result_collection_name + ' not created in the database. Check aggregate pipeline.'))

                    obj_error.write_error_logs(datetime.datetime.now())
            '''
            data_user = list(cursor_user)  # use proper variable names
            Ticketed = 0  # lower case!
            Ticketed_vlyr = 0  # again !
            if len(data_user) != 0:
                for i in data_user:
                    Ticketed += i['Ticketed']
                    Ticketed_vlyr += round(i['Ticketed_vlyr'], 2)
            else:
                Ticketed = None
                Ticketed_vlyr = None
            response = {
                'Ticketed': Ticketed,
                'Ticketed_vlyr': Ticketed_vlyr
            }
            return response
        else:  # If collection to query is not found in the database
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/price_biometrics/price_quote.py method: tickets_sold',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Booking_DepDate cannot be found in the database.')
            obj_error.write_error_logs(datetime.datetime.now())
    # handling pymongo exceptions
    except (pymongo.errors.ServerSelectionTimeoutError,
            pymongo.errors.AutoReconnect,
            pymongo.errors.CollectionInvalid,
            pymongo.errors.ConfigurationError,
            pymongo.errors.ConnectionFailure,
            pymongo.errors.CursorNotFound,
            pymongo.errors.ExecutionTimeout
            ) as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'pymongo exception in jupter_AI/tiles/price_biometrics/price_quote.py method: tickets_sold',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())
    # to handle any other pythonic exception that may have occurred
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/market/price_biometrics/price_quote.py method: tickets_sold',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


def build_query_sales_col(dict_scr_filter):
    """
    Build the query for filtering fares from Host Fares Data according to the filter
    :param dict_scr_filter:
    :return:
    """
    qry_sales = dict()
    qry_sales['$and'] = []
    # today = str(date.today())
    if dict_scr_filter['region']:
        qry_sales['$and'].append({'region': {'$in': dict_scr_filter['region']}})
    if dict_scr_filter['country']:
        qry_sales['$and'].append({'country': {'$in': dict_scr_filter['country']}})
    if dict_scr_filter['pos']:
        qry_sales['$and'].append({'pos': {'$in': dict_scr_filter['pos']}})
    if dict_scr_filter['compartment']:
        qry_sales['$and'].append({'compartment': {'$in': dict_scr_filter['compartment']}})
    if dict_scr_filter['origin']:
        od_build = []
        for idx, item in enumerate(dict_scr_filter['origin']):
            od_build.append({'origin': item,
                             'destination': dict_scr_filter['destination'][idx]})
            qry_sales['$and'].append({'$or': od_build})

    qry_sales['$and'].append({'dep_date': {'$gte': dict_scr_filter['fromDate'],
                                           '$lte': dict_scr_filter['toDate']}})
    return qry_sales


def fare_vlyr(afilter):
    qry_sales = build_query_sales_col(afilter)
    ppln_sales = [
        {
            '$match': qry_sales
        }
        ,
        {
            '$group': {
                '_id': None,
                'revenue': {'$sum': '$revenue_base'},
                'revenue_ly': {'$sum': '$revenue_base_1'},
                'pax': {'$sum': '$pax'},
                'pax_ly': {'$sum': '$pax_1'}
            }
        }
        ,
        {
            '$project': {
                'avg_fare': {
                    '$cond': {
                        'if': {'$ne': ['$pax', 0]},
                        'then': {'$divide': ['$revenue', '$pax']},
                        'else': 'NA'
                    }
                }
                ,
                'avg_fare_ly': {
                    '$cond': {
                        'if': {'$ne': ['$pax_ly', 0]},
                        'then': {'$divide': ['$revenue_ly', '$pax_ly']},
                        'else': 'NA'
                    }
                }
            }
        }
        ,
        {
            '$project': {
                'average_fare': '$avg_fare',
                'average_fare_vlyr': {
                    '$cond': {
                        'if': {'$and': [
                            {'$ne': ['$avg_fare', 'NA']},
                            {'$ne': ['$avg_fare_ly', 'NA']},
                            {'$ne': ['avg_fare_ly', 0]}]},
                        'then': {'$divide': [{'$subtract': ['$avg_fare', '$avg_fare_ly']}, '$avg_fare_ly']},
                        'else': 'NA'
                    }
                }
            }
        }
    ]
    data = list(db.JUP_DB_Sales.aggregate(ppln_sales))
    response = dict()
    if len(data) == 1:
        if type(data[0]['average_fare']) in [float, int]:
            response['average_fare'] = round(data[0]['average_fare'], 0)
        else:
            response['average_fare'] = data[0]['average_fare']
        if type(data[0]['average_fare_vlyr']) in [float, int]:
            response['average_fare_vlyr'] = round(data[0]['average_fare_vlyr'], 2)
        else:
            response['average_fare_vlyr'] = data[0]['average_fare_vlyr']
    else:
        response['average_fare'] = 'NA'
        response['average_fare_vlyr'] = 'NA'
    return response


def get_tiles(afilter):
    response = dict()
    response['Yield_Vlyr'] = yield_pricebiometrics(afilter)
    response['tickets_sold'] = tickets_sold(afilter)
    response['host_market_share'] = host_market_share(afilter)
    response['fare_vlyr'] = fare_vlyr(afilter)
    return response


if __name__ == '__main__':
    a = {
        'region': [],
        'country': [],
        'pos': [],
        'origin': [],
        'destination': [],
        'compartment': [],
        'fromDate': '2017-01-01',
        'toDate': '2017-12-31'
    }
    print get_tiles(afilter=a)
