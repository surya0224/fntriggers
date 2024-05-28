"""
File Name              :   current_market_revenue.py
Author                 :   Pavan
Date Created           :   2016-12-21
Description            :   Module to find current revenue for a particular market
MODIFICATIONS LOG
    S.No               :    2
    Date Modified      :    2017-02-08
    By                 :    Shamail Mulla
    Modification Details   : Code optimisation
"""

import datetime
import inspect
import os

root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),os.path.pardir,os.path.pardir,os.path.pardir))
try:
    from jupiter_AI import client,JUPITER_DB,na_value
    db = client[JUPITER_DB]
except:
    pass
from jupiter_AI.network_level_params import Host_Airline_Code
from jupiter_AI.tiles.jup_common_functions_tiles import query_month_year_builder
from jupiter_AI.common import ClassErrorObject as error_class
from copy import deepcopy
from collections import defaultdict

results_collection = 'JUP_DB_EXCHANGE_RATE_TILE_CURRENT_MRKT_SHR_REV'


def get_module_name():
    '''
    function used to get the module name where it is called
    '''
    return inspect.stack()[1][3]


def get_arg_lists(frame):
    '''
    function used to get the list of arguments of the function
    where it is called
    '''
    args, _, _, values = inspect.getargvalues(frame)
    argument_name_list = []
    argument_value_list = []
    for k in args:
        argument_name_list.append(k)
        argument_value_list.append(values[k])
    return argument_name_list, argument_value_list


def query_builder(dict_filter):
    '''
    The function creates query that can be fired on the database
    :param dict_filter: default dictionary filter recieved from the user
    :return: query (dictionary)
    '''
    query = dict()
    # print 'creating query...'
    if dict_filter['region']:
        # print 'region provided'
        query['region'] = {'$in': dict_filter['region']}
    if dict_filter['country']:
        # print 'country provided'
        query['country'] = {'$in': dict_filter['country']}
    if dict_filter['pos']:
        # print 'pos'
        query['pos'] = {'$in': dict_filter['pos']}
    if dict_filter['compartment']:
        # print 'compartment'
        query['compartment'] = {'$in': dict_filter['compartment']}
    if dict_filter['origin']:
        # print 'origin'
        od_build = []
        for idx, item in enumerate(dict_filter['origin']):
            od_build.append({'origin': item, 'destination': dict_filter['destination'][idx]})
            query['$or'] = od_build
    # print 'dates provided'
    from_obj = datetime.datetime.strptime(dict_filter['fromDate'], '%Y-%m-%d')
    # print from_obj
    to_obj = datetime.datetime.strptime(dict_filter['toDate'], '%Y-%m-%d')
    # print to_obj
    # print 'date objects created'

    # the following code converts the dates to month and year values. the below function
    # is taken from the common functions for tiles program

    month_year = query_month_year_builder(from_obj.month, from_obj.year,
                                          to_obj.month, to_obj.year)
    query['$or'] = month_year
    return query


def get_current_market_revenue(afilter):
    '''
    Function aggregates revenue for a particular market
    :param afilter: filter values to find revenue for a particular market
    :return: database response dictionary
    '''
    # print 'get current market rev'
    try:
        # print 'try block'
        flag_collection_query_present = 0
        tot_bookings = 0
        tot_revenue = 0
        current_revenue = 0
        current_market_share = 0
        dict_filter = deepcopy(defaultdict(list, afilter))
        query = query_builder(dict_filter)

        # print 'query built'
        if 'JUP_DB_Market_Share' in db.collection_names():
            # print 'Market share collection present'
            flag_collection_query_present = 1
            mar_rev_pipeline = [
                {
                    '$match': query
                }
                ,
                {
                    '$group':
                        {
                            '_id':
                                {
                                    'airline':'$MarketingCarrier1'
                                }
                                ,
                            'pax': {'$sum': '$pax'},
                            'revenue': {'$sum': '$revenue'}
                        }
                }
                ,
                # Finding pax and revenue for host airline
                {
                    '$group':
                        {
                            '_id': None,
                            'host_pax':
                                {
                                    '$sum':
                                        {
                                            '$cond':
                                                {
                                                    'if': {'$eq': ['$_id.airline', Host_Airline_Code]},
                                                    'then': '$pax',
                                                    'else': 0
                                                }
                                        }
                                }
                            ,
                            'host_revenue':
                                {
                                    '$sum':
                                        {
                                            '$cond':
                                                {
                                                    'if': {'$eq': ['$_id.airline', Host_Airline_Code]},
                                                    'then': '$revenue',
                                                    'else': 0
                                                }
                                        }
                                },
                            'total_pax': {'$sum': '$pax'}
                        }
                }
                ,
                # Calculating % market share of host
                {
                    '$project':
                        {
                            'current_revenue': '$host_revenue',
                            'current_market_share':
                                {
                                    '$cond':
                                        {
                                            'if': {'$ne': ['$total_pax', 0]},
                                            'then': {'$multiply': [{'$divide': ['$host_pax', '$total_pax']}, 100]},
                                            'else': 'NA'
                                        }
                                }
                        }
                }
                ,
                {
                    '$out': results_collection
                }
            ]
            db.JUP_DB_Market_Share.aggregate(mar_rev_pipeline, allowDiskUse=True)

            if results_collection in db.collection_names():
                market_rev = db.get_collection(results_collection)
                # print 'result collection created'
                # lst_mar_rev = list(market_rev.find())
                #market_rev.drop()
                if market_rev.count() > 0:
                    pass
                else:  # in case the collection is empty
                    # market_rev.drop()
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupter_AI/tiles/exchange_rate/current_market_revenue.py method: get_current_market_revenue',
                                                    get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list('There is no data in result collection for this query.')
                    obj_error.write_error_logs(datetime.datetime.now())
                    return na_value
            else:  # in case resultant collection is not created
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                        'jupter_AI/tiles/exchange_rate/current_market_revenue.py method: get_current_market_revenue',
                                                        get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list('results_collection not created in the database. Check aggregate pipeline.')

                obj_error.write_error_logs(datetime.datetime.now())
        else:  # if collection to query is not present in database
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/exchange_rate/current_market_revenue.py method: get_current_market_revenue',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Market_Share cannot be found in the database.')
            obj_error.write_error_logs(datetime.datetime.now())

    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/exchange_rate/current_market_revenue.py method: get_current_market_revenue',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())

if __name__=='__main__':
    import time

    afilter = {
         'region': [],
         'country': [],
         'pos': [],
         'origin': [],
         'destination': [],
         'compartment': [],
         'currency':['USD'],
         'fromDate': '2017-01-01',
         'toDate': '2017-01-31',
          }
    st = time.time()
    get_current_market_revenue(afilter)
    et = time.time()
    print et-st
