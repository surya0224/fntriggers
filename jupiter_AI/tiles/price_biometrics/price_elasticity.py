"""
File Name              : Price Elasticity_grid
Author                 : Ashwin Kumar
Date Created           : 2017-01-07
Description            :

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
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),os.path.pardir,os.path.pardir,os.path.pardir))
sys.path.append(root_dir)
import numpy as np
from jupiter_AI import client, JUPITER_DB
db = client[JUPITER_DB]
import datetime
import pymongo
# db = pymongo.MongoClient('localhost:27017')['demoDB']
import inspect
# from jupiter_AI.common.network_level_params import db
from jupiter_AI.tiles.jup_common_functions_tiles import gen_collection_name as gen
from jupiter_AI.common import ClassErrorObject as error_class

fare_threshold = 25  # hard coded values
fare_interval = 250

pe_signal_collection_name = gen()

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


def get_fares_array(min, max, interval):
    fare_buckets = list()
    count = min
    while True:
        fare_buckets.append(count)
        if count > max:
            break
        count += interval
    return fare_buckets


def query_builder(dict_filter):
    query = dict()

    if dict_filter['region']:
        query['region'] = {'$in': dict_filter['region']}
    if dict_filter['country']:
        query['country'] = {'$in': dict_filter['country']}
    if dict_filter['pos']:
        query['pos'] = {'$in': dict_filter['pos']}
    if dict_filter['compartment']:
        query['compartment'] = {'$in': dict_filter['compartment']}
    if dict_filter['origin'] and dict_filter['destination']:
        od_build = []
        od = []
        for idx, item in enumerate(dict_filter['origin']):
            od_build.append({'origin': item, 'destination': dict_filter['destination'][idx]})
        for i in od_build:
            a = i['origin'] + i['destination']
            od.append({'combine_od': a})
        query['$or'] = od
    # if afilter['fare_basis']:
    #     query['fare_basis'] = {'$in': afilter['fare_basis']}
    if dict_filter['pax_type']:
        query['pax_type'] = {'$in': dict_filter['pax_type']}
    query['dep_date'] = {'$gte': dict_filter['fromDate'], '$lte': dict_filter['toDate']}
    query['AIR_CHARGE'] = {'$gte': fare_threshold}
    return query

def pe_signal(afilter):
    try:
        response = 'NA'
        if 'JUP_DB_Sales' in db.collection_names():
            afilter = deepcopy(defaultdict(list, afilter))  # use proper variable names

            db_sales = db.get_collection('JUP_DB_Sales')
            query = query_builder(afilter)
            print query
            # print query
            max_fare_list = list(db_sales.find(query).sort('AIR_CHARGE', -1).limit(1))
            max_fare = max_fare_list[0]['AIR_CHARGE']
            # print max_fare
            fares_bucket = get_fares_array(fare_threshold, max_fare, fare_interval)
            # print fares_bucket
            ppln_pe = [  # use proper variable names
                # explain the working of the pipeline
                {
                    '$match': query
                }
                ,
                {
                    '$group':
                        {
                            '_id':
                                {
                                    'fare': '$AIR_CHARGE'
                                },
                            'pax': {'$sum': '$pax'},
                            'count': {'$sum':1}
                        }
                }
                ,
                {
                    '$match': {
                        'count': {'$gt': 1}
                    }
                }
                ,
                {
                    '$group':
                        {
                            '_id': 0,
                            'fare_details':
                                {
                                    '$push':
                                        {
                                            'fare': '$_id.fare',
                                            'pax': '$pax'
                                        }
                                },
                            'min_fare': {'$min': '$_id.fare'},
                            'max_fare': {'$max': '$_id.fare'}
                        }
                }
                ,
                {
                    '$unwind': '$fare_details'
                }
                ,
                {
                    '$match': {
                        'fare_details.fare': {'$gt': 0},
                        'fare_details.pax': {'$gt': 0}
                    }
                }
                ,
                # {
                #     '$bucket':
                #         {
                #             'groupBy': '$fare_details.fare',
                #             'boundaries': fares_bucket,
                #             # 'granularity': 'E24',
                #             'output':
                #                 {
                #                     'pax': {'$sum': '$fare_details.pax'}
                #                 }
                #         }
                # }
                # ,
                {
                    '$project':
                        {

                            'log_fare': {'$ln': '$fare_details.fare'},
                            'log_pax': {'$ln': '$fare_details.pax'}
                        }
                }
                ,
                {
                    '$project':
                        {
                            'log_fare': '$log_fare',
                            'log_pax': '$log_pax',
                            'log_pax_sq':
                                {
                                    '$multiply': ['$log_pax', '$log_pax']
                                },
                            'log_pax_fare':
                                {
                                    '$multiply': ['$log_fare', '$log_pax']
                                }

                        }
                }
                ,
                {
                    '$group':
                        {
                            '_id': None,
                            'pax_fare': {'$sum': '$log_pax_fare'},
                            'sum_pax': {'$sum': '$log_pax'},
                            'sum_fare': {'$sum': '$log_fare'},
                            'sum_pax_sq': {'$sum': '$log_pax_sq'},
                            'count': {'$sum': 1}
                        }
                }
                ,
                {
                    '$project':
                        {
                            'a':
                                {
                                    '$multiply': ['$count', '$pax_fare']
                                },
                            'b':
                                {
                                    '$multiply': ['$sum_pax', '$sum_fare']
                                },
                            'c':
                                {
                                    '$multiply': ['$count', '$sum_pax_sq']
                                },
                            'd':
                                {
                                    '$multiply': ['$sum_pax', '$sum_pax']
                                },
                            'count': '$count'
                        }
                }
                ,
                {
                    '$project':
                        {
                            'numerator':
                                {
                                    '$subtract': ['$c', '$d']
                                },
                            'denominator':
                                {
                                    '$subtract': ['$a', '$b']
                                },
                            'a': '$a',
                            'b': '$b',
                            'c': '$c',
                            'd': '$d',
                            'count': '$count'
                        }
                }
                ,
                {
                    '$project':
                        {
                            '_id': 0,
                            'elasticity':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$ne': ['$denominator', 0]
                                                },
                                            'then':
                                                {
                                                    '$divide': ['$numerator', '$denominator']
                                                },
                                            'else': 'NA'
                                        }

                                },
                            # 'a': '$a',
                            # 'b': '$b',
                            # 'c': '$c',
                            # 'd': '$d',
                            # 'count': '$count'
                        }
                }
                ,
                {
                    '$out': pe_signal_collection_name
                }

            ]
            print ppln_pe
            # print 'hello'
            # db.JUP_DB_Sales_Flown.aggregate(apipeline_user, allowDiskUse=True)
            db.JUP_DB_Sales.aggregate(ppln_pe, allowDiskUse=True)

            if pe_signal_collection_name in db.collection_names():
                crsr_pe_signal = db.get_collection(pe_signal_collection_name)
                if crsr_pe_signal.count() == 0:
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                        'jupter_AI/tiles/market/price_elasticity.py method: pe_signal',
                                                        get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list('There is no data.')
                    obj_error.write_error_logs(datetime.datetime.now())

                elif crsr_pe_signal.count() ==1:
                    pe_signal_data = list(crsr_pe_signal.find(projection={'elasticity': 1, '_id': 0}))
                    elasticity = pe_signal_data[0][u'elasticity']
                    crsr_pe_signal.drop()
                    response = elasticity
                else:
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                        'jupter_AI/tiles/market/price_elasticity.py method: pe_signal',
                                                        get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list(['Expected 1 document, got ' + crsr_pe_signal.count()])
                    obj_error.write_error_logs(datetime.datetime.now())
            else:
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupter_AI/tiles/market/price_elasticity.py method: pe_signal',
                                                    get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list('Result collection not created. Check pipeline.')
                obj_error.write_error_logs(datetime.datetime.now())
        else: # Collection to query is not present
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/price_biometrics/price_biometric_dashboard.py method: pe_signal',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Sales cannot be found in the database.')
            obj_error.write_error_logs(datetime.datetime.now())
        return response
    # to handle pymongo errors generated in db processing
    except (pymongo.errors.ServerSelectionTimeoutError,
                pymongo.errors.AutoReconnect,
                pymongo.errors.CollectionInvalid,
                pymongo.errors.ConfigurationError,
                pymongo.errors.ConnectionFailure,
                pymongo.errors.CursorNotFound,
                pymongo.errors.ExecutionTimeout
                ) as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'pymongo exception in jupter_AI/tiles/price_biometrics/price_biometric_dashboard.py method: pe_signal',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())
        return 'NA'
    # to handle any other pythonic exception that may have occurred
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/market/price_biometrics/price_biometric_dashboard.py method: pe_signal',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())
        return 'NA'


def reject_outliers(data, m=2):
    return data[abs(data - np.mean(data)) < m * np.std(data)]

if __name__ == '__main__':
    # use proper variable names
    print db
    a = {
        # 'region': a['region'][0],
        # 'country': a['country'][0],
        'pos': ['DXB'],
        'origin': ['DXB'],
        'destination': ['DOH'],
        'compartment': ['Y'],
        'fromDate': '2016-01-01',
        'toDate': '2017-12-31'
    }
    elasticity = pe_signal(afilter=a)
    print 'elasticity', elasticity
    crsr = db.JUP_DB_Sales.aggregate(
        [
            {
                '$group':
                    {
                        '_id': {
                            'pos': '$pos',
                            'origin': '$origin',
                            'destination': '$destination',
                            'compartment': '$compartment'
                            # 'dep_date': '$dep_date'
                        }
                    },
                    'count': {'$sum':1}
            }
            ,
            {
                '$project':
                    {
                        '_id': 0,
                        # 'region': ['$_id.region'],
                        # 'country': ['_id.country'],
                        'pos': ['$_id.pos'],
                        'origin': ['$_id.origin'],
                        'destination': ['$_id.destination'],
                        'compartment': ['$_id.compartment'],
                        # 'fromDate': '$_id.dep_date',
                        # 'toDate': '$_id.dep_date'
                    }
            }
        ]
    )
    for doc in crsr:
        a = doc
        a['fromDate'] = '2016-01-01'
        a['toDate'] = '2017-12-31'
        print a
        elasticity_val = pe_signal(afilter=a)
        print 'pe signal', elasticity_val
        db.JUP_DB_Price_Elasticity.insert({
            # 'region': a['region'][0],
            # 'country': a['country'][0],
            'pos': a['pos'][0],
            'origin': a['origin'][0],
            'destination': a['destination'][0],
            'compartment': a['compartment'][0],
            'fromDate': '2016-01-01',
            'toDate': '2017-12-31',
            'elasticity': elasticity_val
        })
        print time.time() - start_time