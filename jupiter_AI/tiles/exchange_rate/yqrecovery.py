"""
File Name              :   yqrecovery.py
Author                 :   Pavan
Date Created           :   2016-12-21
Description            :   module with methods to calculate tiles for exchange rate dashboard
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
from jupiter_AI.common import ClassErrorObject as error_class
from copy import deepcopy
from collections import defaultdict

collection_name = 'JUP_DB_EXCHANGE_YQRECOVERY'


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
        for idx, item in enumerate(dict_filter['origin']):
            od_build.append({'origin': item, 'destination': dict_filter['destination'][idx]})
            query['$or'] = od_build
    query['dep_date'] = {'$gte': dict_filter['fromDate'], '$lte': dict_filter['toDate']}
    return query


def get_yqrecovery(afilter):
    '''
    Function calculates and returns the total fuel charge for the given filter
    :param afilter: dictionary filter recieved from the user
    :return: None
    '''
    try:
        dict_filter = deepcopy(defaultdict(list, afilter))
        query = query_builder(dict_filter)

        if 'JUP_DB_Sales' in db.collection_names():
            yq_pipeline = [
                                {
                                    '$match': query
                                }
                                ,
                                {
                                    '$group':
                                        {
                                            '_id':None,
                                            'fuelcharge': {'$sum': '$surcharge'}
                                        }
                                }
                                ,
                                {
                                    '$project':
                                        {
                                            '_id': 0,
                                            'fuelcharge': '$fuelcharge',
                                            'target_fuel_charge_recovered_percentage' : 'NA',
                                            'target_surcharge' : 'NA'
                                        }
                                }
                                ,
                                {
                                    '$out': collection_name
                                }
                            ]
            db.JUP_DB_Sales.aggregate(yq_pipeline, allowDiskUse=True)

            if collection_name in db.collection_names():
                yq_data = db.get_collection(collection_name)
                # lst_yq = list(yq_data.find())

                if yq_data.count() > 0:
                    # for value in lst_yq:
                    #     total_fuel_charge += value['fuelcharge']
                    #print total_fuel_charge
                    #tot_fuel_charge_recovered_per = (float(total_fuel_charge)/ Target_surcharge)*100
                    # response = dict()
                    # response['total_fuel_charge'] = round(total_fuel_charge, 3)
                    # response['target_fuel_charge_recovered_per '] = na_value
                    # response['Target_surcharge'] = na_value
                    # yq_data.drop()
                    # return response
                    pass
                else:  # in case the collection is empty
                    # yq_data.drop()
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupter_AI/tiles/exchange_rate/current_market_revenue.py method: get_yqrecovery',
                                                    get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list('There is no data for the query.')

                    obj_error.write_error_logs(datetime.datetime.now())
                    return na_value
            else:  # in case resultant collection is not created
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                        'jupter_AI/tiles/exchange_rate/current_market_revenue.py method: get_yqrecovery',
                                                        get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list('results_collection not created in the database. Check aggregate pipeline.')

                obj_error.write_error_logs(datetime.datetime.now())
        else:  # if collection to query is not present in database
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/exchange_rate/current_market_revenue.py method: get_yqrecovery',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Sales cannot be found in the database.')
            obj_error.write_error_logs(datetime.datetime.now())
        # handling pymongo exceptions
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/exchange_rate/current_market_revenue.py method: get_yqrecovery',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())

if __name__ == '__main__':
    import time
    st = time.time()

    afilter = {
         'region': [],
         'country': [],
         'pos': [],
         'origin': ['DXB'],
         'destination': ['DOH'],
         'compartment': [],
         'currency':['USD'],
         'fromDate': '2017-01-01',
         'toDate': '2017-01-31'
         }
    get_yqrecovery(afilter)


