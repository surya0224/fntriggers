"""
File Name              :   average_exchange.py
Author                 :   Pavan
Date Created           :   2016-12-21
Description            :   module with methods to calculate tiles for exchange rate dashboard

MODIFICATIONS LOG
    S.No               :    2
    Date Modified      :    2017-01-20
    By                 :    Shamail Mulla
    Modification Details   :Mongofied python logic
"""

import datetime
import inspect
import os

root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),os.path.pardir,os.path.pardir,os.path.pardir))
from jupiter_AI import client,JUPITER_DB,na_value
db = client[JUPITER_DB]
from jupiter_AI.common import ClassErrorObject as error_class
from copy import deepcopy
from collections import defaultdict


results_collection = 'JUP_DB_EXCHANGERATE_AVG_RATE'

# use this to create a temporary collection in the db and name it:
# result_collection_name = gen_collection_name()

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
    if dict_filter['region']:
        query['region'] = {'$in': dict_filter['region']}
    if dict_filter['country']:
        query['country'] = {'$in': dict_filter['country']}
    if dict_filter['pos']:
        query['pos'] = {'$in': dict_filter['pos']}
    if dict_filter['compartment']:
        query['compartment'] = {'$in': dict_filter['compartment']}
    if dict_filter['origin']:
        od_build = []
        for idx, item in enumerate(dict_filter['origin']):  # use proper variable name for 'idx' and 'item'
            od_build.append({'origin': item, 'destination': dict_filter['destination'][idx]})
            query['$or'] = od_build
    query['currency'] = dict_filter['currency'][0]
    query['dep_date'] = {'$gte': dict_filter['fromDate'], '$lte': dict_filter['toDate']}
    # print query
    return query


def get_average_exchange(afilter):
    '''
    Calculates the average exchange rate over a period of time
    :param afilter: dictionary filter received from the user
    :return: None
    '''
    try:
        dict_filter = deepcopy(defaultdict(list, afilter))
        query = query_builder(dict_filter)
        if 'JUP_DB_Sales' in db.collection_names():
            avg_pipeline = [
                {
                    '$match': query
                }
                ,
                # Retrieving revenue generated over a period of time
                {
                    '$group':
                        {
                            '_id':
                                {
                                    'currency': '$currency'
                                },
                            'revenue': {'$sum': '$revenue_base'}
                        }
                }
                ,
                {
                    '$project':
                        {
                            '_id': 0,
                            'currency': '$_id.currency',
                            'revenue': '$revenue'
                        }
                }
                ,
                # Retrieving exchange rate fluctuations for that time period
                {
                    '$lookup':
                        {
                            'from': 'JUP_DB_Exchange_Rate',
                            'localField': 'currency',
                            'foreignField': 'code',
                            'as': 'currency_code'
                        }
                }
                ,
                {
                    '$unwind': '$currency_code'
                }
                ,
                {
                    '$project':
                        {
                            # '_id': 0,
                            'currency': '$_id.currency',
                            'revenue': '$revenue',
                            'ref_rate': '$currency_code.Reference_Rate'
                        }
                }
                ,
                # Converting revenue to required currency
                {
                    '$project':
                        {
                            # '_id': 0,
                            'currency': '$currency',
                            'tot_rev_local_curr': '$revenue',
                            'tot_rev_foreign_curr':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$gt': ['$ref_rate', 0]
                                                },
                                            'then':
                                                {
                                                    '$divide': ['$revenue', '$ref_rate']
                                                },
                                            'else': 'NA'
                                        }
                                }
                        }

                }
                ,
                {
                    '$project':
                        {
                            # '_id': 0,
                            'avg_ex_rate':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$gt': ['$tot_rev_foreign_curr', 0]
                                                },
                                            'then':
                                                {
                                                    '$divide': ['$tot_rev_local_curr', '$tot_rev_foreign_curr']
                                                },
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
            db.JUP_DB_Sales.aggregate(avg_pipeline, allowDiskUse=True)
            if results_collection in db.collection_names():
                # pythonic variable given to the newly created collection
                avg_ex_rate = db[results_collection].find()
                if avg_ex_rate.count() > 0:
                    pass
                else:  # in case the collection is empty
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                        'jupter_AI/tiles/exchange_rate/average_exchange.py method: get_average_exchange',
                                                        get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list('There is no data for this query.')

                    obj_error.write_error_logs(datetime.datetime.now())
                    db[results_collection].insert_one({
                        'avg_ex_rate':'NA'
                    })
                    return na_value
            else:  # in case resultant collection is not created
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupter_AI/tiles/exchange_rate/average_exchange.py method: get_average_exchange',
                                                    get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list('results_collection not created in the database. Check aggregate pipeline.')

                obj_error.write_error_logs(datetime.datetime.now())
        else:  # if collection to query is not present in database
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/exchange_rate/average_exchange.py method: get_average_exchange',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Sales cannot be found in the database.')
            obj_error.write_error_logs(datetime.datetime.now())

    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/exchange_rate/average_exchange.py method: get_average_exchange',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())

if __name__ == '__main__':
    import time
    filter = {
         'region': [],
         'country': [],
         'pos': [],
         'origin': ["DXB"],
         'destination': ["DOH"],
         'compartment': ['Y'],
         'currency':['AED'],
         'fromDate': '2017-02-14',
         'toDate': '2017-02-14'
         }
    st = time.time()
    print get_average_exchange(filter)
    et = time.time()
    #print result
    print et-st
