"""
File Name              :   distributor.py
Author                 :   Pavan
Date Created           :   2016-12-21
Description            :   module to returns the number of distributors to the tile
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
from jupiter_AI import client, JUPITER_DB,na_value
db = client[JUPITER_DB]
from jupiter_AI.common import ClassErrorObject as error_class
from copy import deepcopy
from collections import defaultdict

def get_module_name():
    """
    Function used to get the module name where it is called
    :return: stack element
    """
    return inspect.stack()[1][3]


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


def query_builder(dict_filter):
    """
    The function creates a query to be fired to the database
    :param dict_filter: dictionary to filter the database collection
    :return: dictionary
    """
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
    query['dep_date'] = {'$gte': dict_filter['fromDate'], '$lte': dict_filter['toDate']}
    query['distributor'] = {'$ne': 'NA'}
    return query


def get_distributor(afilter):
    """
    Counts the number of distributors for the particular filter
    :param afilter: dictionary to query the database
    :return: dictionary of database repsonse
    """
    try:
        dict_filter = deepcopy(defaultdict(list, afilter))
        query = query_builder(dict_filter)
        if 'JUP_DB_Sales' in db.collection_names():
            distributors = db['JUP_DB_Sales'].distinct('agent', query)
            count_distributors = len(distributors)
            if count_distributors > 0:
                response = dict()
                response['distributors'] = count_distributors
                return response
            else: # in case the collection is empty
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupter_AI/tiles/distributor_customer_segments/distributor.py method: get_distributor',
                                                    get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list('There is no distributors for those filter values.')
                return na_value

        else:  # if collection to query is not present in database
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                            'jupter_AI/tiles/distributor_customer_segments/distributor.py method: get_distributor',
                                            get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Sales cannot be found in the database.')
            obj_error.write_error_logs(datetime.datetime.now())
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/distributor_customer_segments/distributor.py method: get_distributor',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())

if __name__=='__main__':
    import time

    st = time.time()
    filter = {
         'region': [],
         'country': [],
         'pos': [],
         'origin': ['DXB'],
         'destination': ['DOH'],
         'compartment': ['Y'],
         'fromDate': '2017-02-14',
         'toDate': '2017-02-20'
            }

    result = get_distributor(filter)
    print result
    print "time in seconds:",round(time.time() - st, 3)
