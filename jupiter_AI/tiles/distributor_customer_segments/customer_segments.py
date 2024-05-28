"""
File Name              :   customer_segments.py
Author                 :   Pavan
Date Created           :   2016-12-21
Description            :   module with methods to calculate tiles for distribution and customer segment dashboard
MODIFICATIONS LOG
    S.No               :
    Date Modified      :
    By                 :
    Modification Details   :
"""

import inspect
import os

root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),os.path.pardir,os.path.pardir,os.path.pardir))
try:
    from jupiter_AI import client,JUPITER_DB,na_value
    db = client[JUPITER_DB]
except:
    pass
from copy import deepcopy
from collections import defaultdict


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


def get_customer_segments(afilter):
    data = deepcopy(defaultdict(list, afilter))
    query = dict()
    if data['region']:
        query['region'] = {'$in': data['region']}
    if data['country']:
        query['country'] = {'$in': data['country']}
    if data['pos']:
        query['pos'] = {'$in': data['pos']}
    if data['compartment']:
        query['compartment'] = {'$in': data['compartment']}
    if data['origin']:
        od_build = []
        for idx, item in enumerate(data['origin']):
            od_build.append({'origin': item, 'destination': data['destination'][idx]})
            query['$or'] = od_build
            query['dep_date'] = {'$gte': data['fromDate'], '$lte': data['toDate']}
            customer_segments_pipeline = db['JUP_DB_Distributer'].aggregate([{'$match': query}])
    # total_count_customer_segments = 0
    # distributor_pipeline_list = list(customer_segments_pipeline)
    # if len(distributor_pipeline_list) != 0:
    #     for i in distributor_pipeline_list:
    #         total_count_customer_segments += 1
    #     count_distributors= total_count_customer_segments
    #     response = dict()
    #     response['total_distributors%'] = count_distributors
    return na_value
    # else:
    #     e1 = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1, get_module_name(),
    #                                  get_arg_lists(inspect.currentframe()))
    #     e1.append_to_error_list("Expected 1 document agents but" + str(customer_segments_pipeline.count()))
    #     raise e1
          #return NA

if __name__ == '__main__':
    import time

    st = time.time()
    filter = {
         'region': [],
         'country': [],
         'pos': [],
         'origin': [],
         'destination': [],
         'compartment': [],
         'fromDate': '',
         'toDate': ''
              }

    result = get_customer_segments(filter)
    print result
    #print "time in seconds:",round(time.time() - st, 3)





