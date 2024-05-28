
"""
File Name              :   price_performance_grids
Author                 :   Ashwin Kumar
Date Created           :   2016-12-21
Description            :  This screen allows an analyst to know the score of a price performance
                          with respect to Revenue, pax, yield and market condition. The screen will
                          help him analyse if the pricing strategy or model is effective. The number of
                          bookings achieved and the available seats for a particular fare will also be shown.

MODIFICATIONS LOG
    S.No                   :    1
    Date Modified          :    2016-12-28
    By                     :    Shamail Mulla
    Modification Details   :    Added error / exception handling codes
"""

import datetime
import inspect
import time
from collections import defaultdict
from copy import deepcopy
from jupiter_AI import client, JUPITER_DB
db = client[JUPITER_DB]
from jupiter_AI.common import ClassErrorObject as error_class
from jupiter_AI.tiles.jup_common_functions_tiles import gen_collection_name as gen

#the below function generates a random name for collection
result_collection_name = gen()

"""
the following function query for price performance builds a query that will be used to
identify the grid values in the price performance screen

"""


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


def query_for_price_performance(filter_from_user):
    filter_pp_scn = deepcopy(defaultdict(list, filter_from_user))
    qry_pp_scn = dict()
    if filter_pp_scn['region']:
        qry_pp_scn['region'] = {'$in': filter_pp_scn['region']}
    if filter_pp_scn['country']:
        qry_pp_scn['country'] = {'$in': filter_pp_scn['country']}
    if filter_pp_scn['pos']:
        qry_pp_scn['pos'] = {'$in': filter_pp_scn['pos']}
    if filter_pp_scn['compartment']:
        qry_pp_scn['compartment'] = {'$in': filter_pp_scn['compartment']}
    if filter_pp_scn['origin'] and filter_pp_scn['destination']:
        od = ''.join(filter_pp_scn['origin'] + filter_pp_scn['destination'])
        qry_pp_scn['od'] = od
    qry_pp_scn['dep_date'] = {'$gte': filter_pp_scn['fromDate'], '$lte': filter_pp_scn['toDate']}

    return qry_pp_scn


def price_performance_screen(filter_from_user):
    try:
        if 'JUP_DB_Sales' in db.collection_names():
            filter_pp_scn = deepcopy(defaultdict(list, filter_from_user))
            qry_pp_scn = query_for_price_performance(filter_pp_scn)

            pipeline_pp_scn = [
                {
                    '$match': qry_pp_scn
                }
                ,
                {
                    '$group':
                        {
                            '_id':
                                {
                                    'region': '$region',
                                    'country': '$country',
                                    'pos': '$pos',
                                    'od': '$od',
                                    'compartment': '$compartment',
                                    'channel': '$channel'
                                },
                            'zip':
                                {
                                    '$push':
                                        {
                                            'rev': '$revenue',
                                            'pax': '$pax',
                                            'rev_ly': '$revenue_1',
                                            'pax_ly': '$pax_1',
                                            'avg_fare':
                                                {
                                                    '$cond':
                                                        {
                                                        'if': {'$gt': ['$pax', 0]},
                                                        'then':
                                                            {
                                                            '$cond':
                                                                {
                                                                'if': {'$gt': ['$revenue_base', 0]},
                                                                'then': {'$divide': ['$revenue_base', '$pax']},
                                                                'else': 0
                                                                }
                                                            },
                                                        'else': 0

                                                        }
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
                            'region':'$_id.region',
                            'country': '$_id.country',
                            'pos': '$_id.pos',
                            'od': '$_id.od',
                            'compartment': '$_id.compartment',
                            'zip': '$zip',
                            'channel':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$eq': [
                                                        {'$strcasecmp':['$channel','GDS']}, 0
                                                    ]
                                                },
                                            'then': 'indirect',
                                            'else': 'direct'
                                        }
                                }
                        }
                }
                ,
                {
                    '$facet':
                        {
                            'elasticity': [
                                {
                                    #'$unwind': '$zip',
                                }
                            ],
                            'sample': [
                                {
                                    '$unwind': '$zip',
                                }
                                ,
                                {
                                    '$group':
                                        {
                                            '_id':
                                                {
                                                    'region':'$region'
                                                },
                                            'tot_pax':
                                                {
                                                    '$sum': '$zip.pax'
                                                }
                                        }
                                }
                            ]
                        }
                }
                ,
                {
                    '$unwind': '$elasticity'
                }
                ,
                {
                    '$unwind': '$sample'
                }
                ,
                {
                    '$redact':
                        {
                            '$cond':
                                {
                                    'if':
                                        {
                                            '$eq': ['$sample._id.region', '$elasticity.region']
                                        },
                                    'then': '$$DESCEND',
                                    'else': '$$PRUNE'
                                }
                        }
                }
                ,
                {
                    '$out': result_collection_name
                }
            ]

            db.JUP_DB_Sales.aggregate(pipeline_pp_scn)

            if result_collection_name in db.collection_names():
                # pythonic variable given to the newly created collection
                crsr_result = db.get_collection(result_collection_name)
                if crsr_result.count > 0:
                    lst_cursor = list(crsr_result)
                    crsr_result.drop()
                    for i in lst_cursor:
                        print i
                    print len(lst_cursor)
                    response = dict()

                else: # in case the collection is empty
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                        'jupter_AI/tiles/price_biometrics/price_performance_grids.py method: price_performance_screen',
                                                        get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list('There is no data.')

                    obj_error.write_error_logs(datetime.datetime.now())
            else: # result collection not created
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/price_biometrics/price_performance_grids.py method: price_performance_screen',
                                                get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list('Result collection not created in the database. Check aggregate pipeline.')

                obj_error.write_error_logs(datetime.datetime.now())

        else: # Collection to query is not present
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/price_biometrics/price_performance_grids.py method: price_performance_screen',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Sales cannot be found in the database.')
            obj_error.write_error_logs(datetime.datetime.now())

    # to handle any other pythonic exception that may have occurred
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/market/price_biometrics/price_performance_grids.py method: price_performance_screen',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())

if __name__ == '__main__':
    start_time = time.time()
    a = {
        'region': [],
        'country': [],
        'pos': [],
        'origin': [],
        'destination': [],
        'compartment': [],
        'fromDate': '2016-01-01',
        'toDate': '2017-12-30'
    }

    print price_performance_screen(a)
    print time.time() - start_time



