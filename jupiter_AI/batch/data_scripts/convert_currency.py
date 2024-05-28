
# coding: utf-8

# In[5]:

import inspect
from copy import deepcopy
import json
import pymongo
import collections
import pandas as pd
import numpy as np
import global_variable as var
import datetime
import time


# Connect mongodb db business layer
# try:
#     fzDBConn=pymongo.MongoClient(var.mongo_client_url)[var.database]
#     fzDBConn.authenticate('pdssETLUser', 'pdssETL@123', source='admin')
# #     print('connected')
#
#
# except Exception as e:
#     #sys.stderr.write("Could not connect to MongoDB: %s" % e)
#     print("Could not connect to MongoDB: %s" % e)
from jupiter_AI import JUPITER_DB, client, JUPITER_LOGGER
from jupiter_AI.logutils import measure


fzDBConn = client[JUPITER_DB]


@measure(JUPITER_LOGGER)
def get_module_name():
    """
    FUnction used to get the module name where it is called
    """
    return inspect.stack()[1][3]


@measure(JUPITER_LOGGER)
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


@measure(JUPITER_LOGGER)
def convert_currency(value, from_code, to_code='AED'):
    """
    :param from_code:currency code of value
    :param to_code:currency code into which value needs to be converted
    :param value:the value in from_code to be converted to to_code
    :return:value in to_code currency
    """
    if from_code == to_code:
        return value
    else:
        cursor = fzDBConn.JUP_DB_Exchange_Rate.find({'code': {'$in': [from_code, to_code]}})
        # print cursor.count()
        if cursor.count() == 0:
            e1 = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                        get_module_name(),
                                        get_arg_lists(inspect.currentframe()))
            e1.append_to_error_list('Exchange Rates Not Available for '+from_code+' to '+to_code)
            raise e1
        if from_code == 'AED':
            if cursor.count() > 0:
                for currency_doc in cursor:
                    if currency_doc['code'] != 'AED':
                        return float(value)/currency_doc['Reference_Rate']
        elif to_code == 'AED':
            if cursor.count() > 0:
                for currency_doc in cursor:
                    if currency_doc['code'] != 'AED':
                        return float(value)*currency_doc['Reference_Rate']
        else:
            if cursor.count() == 2:
                if cursor[0]['Code'] == from_code:
                    ratio_2to1 = cursor[1]['Reference_Rate'] / cursor[0]['Reference_Rate']
                else:
                    ratio_2to1 = cursor[0]['Reference_Rate'] / cursor[1]['Reference_Rate']
                converted_value = float(value) * ratio_2to1
                return converted_value
            
if __name__ == '__main__':
    print(convert_currency(500, 'AED', 'SAR'))


