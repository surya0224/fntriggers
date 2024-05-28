"""
File Name              :   direct_and_indirect_revenue_share.py
Author                 :   Pavan
Date Created           :   2016-12-21
Description            :   module to calculate revenue share in % from direct and indirect sources

MODIFICATIONS LOG
    S.No               :    2
    Date Modified      :    2017-02-08
    By                 :    Shamail Mulla
    Modification Details   : Code optimisation
"""

import datetime
import inspect
import os

try:
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),os.path.pardir,os.path.pardir,os.path.pardir))
    from jupiter_AI import client,JUPITER_DB, na_value
    db = client[JUPITER_DB]
except:
    pass
from jupiter_AI.common import ClassErrorObject as error_class
from copy import deepcopy
from collections import defaultdict

result_collection_name = 'JUP_DB_DISTRIBUTION_DIRECT_AND_INDIRECT'


def get_module_name():
    '''
    Function used to get the module name where it is called
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
        query['dep_date'] = {'$gte': dict_filter['fromDate'], '$lte': dict_filter['toDate']}
        od_build = []
        for idx, item in enumerate(dict_filter['origin']):
            od_build.append({'origin': item, 'destination': dict_filter['destination'][idx]})
            query['$or'] = od_build
    query['dep_date'] = {'$gte': dict_filter['fromDate'], '$lte': dict_filter['toDate']}
    return query


def get_direct_channel_revenue_share(afilter):
    '''
    This function calculates the % revenue contribution from direct and indirect revenue channels and stores the result in the db
    :param afilter: filter values reveived from the user
    :return: None
    '''
    db['JUP_DB_DISTRIBUTION_DIRECT_AND_INDIRECT'].drop()
    try:
        dict_query = deepcopy(defaultdict(list, afilter))
        query = query_builder(dict_query)

        if 'JUP_DB_Sales' in db.collection_names():
            # print 'JUP_DB_SALES COLLECTION IS PRESENT'
            dairs_pipeline = [
                {
                    '$match': query
                }
                ,
                {
                    '$group':
                        {
                            '_id':
                                {
                                    'channel': '$channel'
                                },
                            'revenue': {'$sum': '$revenue_base'}
                        }
                }
                ,
                # Categorising revenue sources into direct and indirect channels
                {
                    '$project':
                        {
                            '_id':1,
                            'channel': '$_id.channel',
                            'revenue': '$revenue',
                            'channel_type':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$or':
                                                        [
                                                            {'$eq': ['$_id.channel', 'GDS']},
                                                            {'$eq': ['$_id.channel', 'TA']}
                                                        ]
                                                }
                                            ,
                                            'then': 'indirect',
                                            'else': 'direct'
                                        }

                                }
                        }
                }
                ,
                # Finding total revenue generated from the 2 sources
                {
                    '$group':
                        {
                            '_id': '$channel_type',
                            'revenue': {'$sum': '$revenue'},
                            # 'channel_type': '$channel_type'
                        }
                }
                ,
                # Finding total revenue generated irrespective of the channel type
                {
                    '$group':
                        {
                            '_id': 0,
                            'rev_details':
                                {
                                    '$push':
                                        {
                                            'channel_type': '$_id',
                                            'revenue': '$revenue'
                                        }
                                },
                            'total_rev': { '$sum': '$revenue'}

                        }
                }
                ,
                {
                    '$unwind': '$rev_details'
                }
                ,
                {
                    '$project':
                        {
                            '_id':0,
                            'channel_type':'$rev_details.channel_type',
                            'channel_revenue':'$rev_details.revenue',
                            'total_revenue':'$total_rev'
                        }
                }
                # ,
                # {
                #     '$group':
                #         {
                #             '_id':'$channel_type',
                #             'channel_revenue': '$channel_revenue',
                #             'total_revenue': '$total_revenue'
                #         }
                # }
                ,
                # Finding % contribution of either revenue sources
                {
                    '$project':
                        {
                            '_id':0,
                            'channel_type':'$channel_type',
                            'channel_revenue_share':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$gt': ['$total_revenue', 0]
                                                },
                                            'then':
                                                {
                                                    '$multiply': [100,
                                                                  {
                                                                      '$divide': ['$channel_revenue','$total_revenue']
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
                        '$out': result_collection_name
                }
            ]
            db.JUP_DB_Sales.aggregate(dairs_pipeline,allowDiskUse=True)

            if result_collection_name in db.collection_names():
                dairs_pipeline = db.get_collection(result_collection_name)
                if dairs_pipeline.count()==0:
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                        'jupter_AI/tiles/distributor_customer_segments/direct_and_indirect_revenue_share.py method: get_direct_channel_revenue_share',
                                                        get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list('There is no data for this query.')
                    obj_error.write_error_logs(datetime.datetime.now())
                    return na_value
                elif dairs_pipeline.count() > 2:
                    # The result collection should have only 2 rows, otherwise something has gone wrong with the pipeline
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                        'jupter_AI/tiles/distributor_customer_segments/direct_and_indirect_revenue_share.py method: get_direct_channel_revenue_share',
                                                        get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list('There are more than 2 rows in the result. Check collection and pipeline.')
                    obj_error.write_error_logs(datetime.datetime.now())
                else:
                    pass
            else: # If the result collection is not created
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupter_AI/tiles/distributor_customer_segments/direct_and_indirect_revenue_share.py method: get_direct_channel_revenue_share',
                                                    get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list('results_collection not created in the database. Check aggregate pipeline.')
                obj_error.write_error_logs(datetime.datetime.now())
        else: # if collection to query is not present in database
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/distributor_customer_segments/direct_and_indirect_revenue_share.py method: get_direct_channel_revenue_share',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Sales cannot be found in the database.')
            obj_error.write_error_logs(datetime.datetime.now())
    # to handle any other pythonic exception that may have occurred
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/distributor_customer_segments/direct_and_indirect_revenue_share.py method: get_direct_channel_revenue_share',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


if __name__=='__main__':
    import time

    filter = {
         'region': [],
         'country': [],
         'pos': [],
         'origin': ["DXB"],
         'destination': ["DOH"],
         'compartment': ["Y"],
         'fromDate': '2017-02-12',
         'toDate': '2017-02-20'
             }
    st = time.time()
    get_direct_channel_revenue_share(filter)
    et = time.time()

    time_taken = et - st

    print time_taken
    #print result