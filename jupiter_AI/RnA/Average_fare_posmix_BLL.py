"""
File Name              :   Average_fare_odmix_RNA_perspective_BLL
Author                 :   Pavansimha Reddy
Date Created           :   2016-12-19
Description            :   Average fare will keep falling due to odmix
MODIFICATIONS LOG      :
    S.No               :
    Date Modified      :
    By                 :
    Modification Details   :
"""

import jupiter_AI.RnA.Average_fare_posmix_DAL as dal
import jupiter_AI.RnA.common_RnA_functions as cf
import random
import inspect
import jupiter_AI.common.ClassErrorObject as errorClass

"""
Based on the filter on screen check the status of averages_fares (if it is low or high)
    average_fares
         POSmix
"""

def get_module_name():
    return inspect.stack()[1][3]

#Returns the arguments list
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
def check_average_fare_posmix(dict_scr_filter):
    """
    Seeing the data/tiles on screen
    This function tells whether to do this
    perspective analysis or not
    :param dict_scr_filter:
    :return:
    """
    return bool(random.getrandbits(1))

def rna_average_fare_posmix(dict_scr_filter):

    """
    the function rna_average_fare_odmix will be called to check the value of the average_fares.

    ******** please enter codes to do the above ***********
    """

    rna_response = dict()
    average_fare_posmix_collection_name = dal.get_average_fare_posmix_rna_data(dict_scr_filter)
    check_for_bookings = cf.get_btc_data(dict_scr_filter)
    # The function get_btc_data needs to be called to compute total ticketed value. if total
    # ticketed value is more than 90 percent then an action is not required. Currently this function
    # is not being called since data is not available to complete this RnA. The function will be called
    # and appropriate code will be populated once data is available

    #   Error handling for no response from dB
    if len(average_fare_posmix_collection_name) == 0:
        # no_data_error = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
        #                                        get_module_name(),
        #                                        get_arg_lists(inspect.currentframe()))
        # desc = 'No response from db for the filter provided'
        # no_data_error.append_to_error_list(desc)
        # raise no_data_error

        #DISPLAYING TEXT AS IT IS
        what = 'Average_fare - posmix'
        why = 'Inappropriate pos mix'
        status_quo = 'Average fare will keep falling'
        action = 'Nothing can be done by juipiter - Just info'

    elif len(average_fare_posmix_collection_name) == 1:
        # appropriate code will be completed once there is relevant data in the collection. Till then, the following
        # values will be displayed
        what = 'Average_fare - posmix'
        why = 'Inappropriate pos mix'
        status_quo = 'Average fare will keep falling'
        action = 'Nothing can be done by juipiter - Just info'


    elif len(average_fare_posmix_collection_name) > 1:
        # appropriate code will be completed once there is relevant data in the collection. Till then, the following
        # values will be displayed
        what = 'Average_fare - posmix'
        why = 'Inappropriate pos mix'
        status_quo = 'Average fare will keep falling'
        action = 'Nothing can be done by juipiter - Just info'

    else:
        # appropriate code will be completed once there is relevant data in the collection. Till then, the following
        # values will be displayed
        what = 'Average_fare - posmix'
        why = 'Inappropriate pos mix'
        status_quo = 'Average fare will keep falling'
        action = 'Nothing can be done by juipiter - Just info'

    rna_response['what'] = what
    rna_response['why'] = why
    rna_response['status quo'] = status_quo
    rna_response['action'] = action

    return rna_response

if __name__ == '__main__':

    a = {
        'region': [],
        'country': [],
        'pos': [],
        'origin': ['DOH', 'DXB'],
        'destination': ['DXB', 'DOH'],
        'compartment': ['Y', 'J'],
        'fromDate': '2016-10-01',
        'toDate': '2016-10-30',
        'from_month': 8,
        'from_year':2016,
        'to_month': 12,
        'to_year': 2016,
        'flag' : 0
    }

    print rna_average_fare_posmix(a)