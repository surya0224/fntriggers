"""
File Name              :   no_of_fares.py
Author                 :   Pavan
Date Created           :   2016-12-21
Description            :   Module which outputs number of fares for a given filter
MODIFICATIONS LOG
    S.No               :    2
    Date Modified      :    2017-02-08
    By                 :    Shamail Mulla
    Modification Details   : Code optimisation
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
    '''+
    FUnction used to get the module name where it is called
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
        for idx, item in enumerate(dict_filter['origin']):
            od_build.append({'origin': item, 'destination': dict_filter['destination'][idx]})
            query['$or'] = od_build
    if (dict_filter['origin'] != [] or dict_filter['destination'] != []):
        query['dep_date'] = {'$gte': dict_filter['fromDate'], '$lte': dict_filter['toDate']}
    return query

def get_no_of_fares(afilter):
    '''
    Function aggregates number of fares for a given filter
    :param afilter: dictionary filter recieved from the user
    :return: None
    '''
    dict_filter = deepcopy(defaultdict(list, afilter))
    query = query_builder(dict_filter)

    # print query
    # cursor = db['JUP_DB_Fare'].find([{'$match': query}])
    #fares collection should be there
    # total_count = 0
    # data = list(cursor)
    # print data
    # if len(data) != 0:
    #     for i in data:
    #         total_count += 1
    return na_value
    # else:
    #     e1 = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1, get_module_name(),
    #                                  get_arg_lists(inspect.currentframe()))
    #     e1.append_to_error_list("Expected 1 document from fares but got  " + str(cursor.count()))
    #     raise e1
         #return NA

if __name__ == '__main__':
    import time

    st = time.time()

    filter = {
         'region': ['GCC'],
         'country': ['JO'],
         'pos': ['RUH'],
         'origin': ['RUH'],
         'destination': ['CMB'],
         'compartment': ['Y'],
         'fromDate': '2016-07-01',
         'toDate': '2016-07-01'
            }
    get_no_of_fares(filter)

