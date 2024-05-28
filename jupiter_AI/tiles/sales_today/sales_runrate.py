"""
File Name              :   sales_runrate.py
Author                 :   Pavan
Date Created           :   2016-12-19
Description            :   module with methods to calculate sales runrate for jupiter dashboard (sales today)
MODIFICATIONS LOG      :
    S.No               :
    Date Modified      :
    By                 :
    Modification Details   :
"""
import datetime
import inspect
from collections import defaultdict
from copy import deepcopy
from jupiter_AI import client, JUPITER_DB
db = client[JUPITER_DB]



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


def get_day_difference(fromDate, toDate):
    sd = datetime.datetime.strptime(fromDate, '%Y-%m-%d')
    ed = datetime.datetime.strptime(toDate, '%Y-%m-%d')
    diff = (ed - sd).days
    return diff


def get_sales_runrate(afilter):
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
    if (data['origin'] != [] and data['destination'] != []):
        query['dep_date'] = {'$gte': data['fromDate'], '$lte': data['toDate']}
        sales_runrate_pipeline = db['JUP_DB_Sales'].aggregate([
                                               {
                                                    '$match': query
                                               }
                                               ,
                                               {    '$group': {
                                                                '_id':
                                                                    {
                                                                     'region': '$region',
                                                                     'country': '$country',
                                                                     'pos': '$pos',
                                                                     'origin': '$origin',
                                                                     'destination': '$destination',
                                                                     'compartment': '$compartment'
                                                                    }
                                                                    ,
                                                                'revenue': {'$sum': '$revenue_base'}
                                                              }
                                               }
                                               ])
    else:
        query['dep_date'] = {'$in': [data['fromDate'], data['toDate']]}
        sales_runrate_pipeline = db['JUP_DB_Cumulative_Dep_Date'].aggregate([
                                                             {
                                                                '$match': query
                                                             }
                                                             ,
                                                             {
                                                                 '$group': {
                                                                            '_id':
                                                                                {
                                                                                    'region': '$region',
                                                                                    'country': '$country',
                                                                                    'pos': '$pos',
                                                                                    'compartment': '$compartment'
                                                                                }
                                                                                ,
                                                                            'torevenue': {'$max': '$sale_revenue_base'},
                                                                            'fromrevenue':{'$min': '$sale_revenue_base'}
                                                                            }
                                                             }
                                                             ,
                                                             {
                                                                 "$project": {
                                                                                'region': '$_id.region',
                                                                                'country': '$_id.country',
                                                                                'pos': '$_id.pos',
                                                                                'compartment': '$_id.compartment',
                                                                                'revenue': {'$subtract': ['$torevenue','$fromrevenue']}
                                                                              }
                                                             }
                                                             ])
    tot_revenue = 0
    tot_period = 0
    sales_runrate_pipeline_list = list(sales_runrate_pipeline)
    #print sales_runrate_pipeline_list
    if len(sales_runrate_pipeline_list) != 0:
        for i in sales_runrate_pipeline_list:
            tot_revenue = i['revenue']
            tot_period = get_day_difference(data['fromDate'], data['toDate'])
            runrate = float(tot_revenue) / (tot_period)
        return runrate

    # else:
    #     no_data_bookings_error  = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
    #                                   get_module_name(),
    #                                   get_arg_lists(inspect.currentframe()))
    #     no_data_bookings_error.append_to_error_list("Expected 1 document for sales but got " + str(len(adata)))
    #     raise no_data_bookings_error
    #     return NA

try:
    import time

    st = time.time()
    filter = {
         'region': ['GCC'],
         'country': ['SA'],
         'pos': ['RUH'],
         'origin': ['RUH'],
         'destination': ['CMB'],
         'compartment': ['Y'],
         'fromDate': '2016-09-11',
         'toDate': '2016-09-11'
            }
    result = get_sales_runrate(filter)
    #print result
    # print time.time() - st
except ZeroDivisionError as result :
    print "NA", result
except AttributeError as NA :
    print "NA", NA

