"""
File Name              :   dashboard.py
Author                 :   Sai Krishna
Date Created           :   2016-12-19
Description            :   module with methods to calculate tiles for competitor analysis dashboard
MODIFICATIONS LOG
    S.No               : 1
    Date Modified      : 2017-10-02
    By                 : Shamail Mulla
    Modification Details   : Code optimisation
"""

from jupiter_AI.network_level_params import Host_Airline_Code # as host
try:
    from  jupiter_AI import  client, JUPITER_DB, Host_Airline_Code
    db = client[JUPITER_DB]
except:
    pass
import datetime
import inspect
from jupiter_AI.common import ClassErrorObject as error_class


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


def get_tiles(afilter):
    """

    :param afilter:
    :return:
    """
    try:
        if 'JUP_DB_Data_Product_Summary' in db.collection_names():
            response = dict()
            cursor = db.JUP_DB_Data_Product_Summary.find({}, {'Category': 1,
                                                              Host_Airline_Code: 1})
            if cursor.count() != 0:
                for i in cursor:
                    del i['_id']
                    if i['Category'] == 'Pre Booking(157)':
                        response['pre_booking'] = i[Host_Airline_Code]
                    elif i['Category'] == 'At Airport(158)':
                        response['at_airport'] = i[Host_Airline_Code]
                    elif i['Category'] == 'In Flight(271)':
                        response['in_flight'] = i[Host_Airline_Code]
                    elif i['Category'] == 'Post Flight(84)':
                        response['post_flight'] = i[Host_Airline_Code]
            return response
        else: # if collection to query is not present in database
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/competitor_analysis/product_indicator.py method: get_tiles',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Data_Product_Summary cannot be found in the database.')
            obj_error.write_error_logs(datetime.datetime.now())
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/competitor_analysis/product_indicator.py method: get_tiles',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())
