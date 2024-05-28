"""
File Name              :   Bookings_strength_BLL
Author                 :   Pavansimha Reddy
Date Created           :   2016-12-19
Description            :   RnA analysis for bookings with one subperspective Runrate.

MODIFICATIONS LOG      :
    S.No               :
    Date Modified      :
    By                 :
    Modification Details   :
"""

import jupiter_AI.RnA.Bookings_strength_DAL as dal
import jupiter_AI.RnA.common_RnA_functions as cf
import random
import inspect
import jupiter_AI.common.ClassErrorObject as errorClass

"""
Based on the filter on screen check the status of bookings (if it is low or high)
    bookings
        runrate -
            scale - (positive or negative)
            relevance -

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
def check_strength(dict_scr_filter):
    """
    Seeing the data/tiles on screen
    This function tells whether to do this
    perspective analysis or not
    :param dict_scr_filter:
    :return:
    """
    return bool(random.getrandbits(1))

def rna_strength(dict_scr_filter):

    """
    the function rna_strength will be called to check the value of the bookings.


    ******** please enter codes to do the above ***********
    """

    rna_response = dict()
    strength_collection_name = dal.get_strength_rna_data(dict_scr_filter)
    check_for_bookings = cf.get_btc_data(dict_scr_filter)
    # The function get_btc_data needs to be called to compute total ticketed value. if total
    # ticketed value is more than 90 percent then an action is not required. Currently this function
    # is not being called since data is not available to complete this RnA. The function will be called
    # and appropriate code will be populated once data is available

    #   Error handling for no response from dB
    if len(strength_collection_name) == 0:

        # no_data_error = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
        #                                        get_module_name(),
        #                                        get_arg_lists(inspect.currentframe()))
        # desc = 'No response from db for the filter provided'
        # no_data_error.append_to_error_list(desc)
        # raise no_data_error
        #As for now displaying the text

        what = 'The Bookings have fallen down'
        why = '(strength is low )Pricing model is not good compare to competitors'
        status_quo = 'Further drop in market share'
        action = 'Adjust Price based on pricing model. Use simulation to arrive at a Price. Not an issue when ' \
                 '90 percent of capacity is booked'

    elif len(strength_collection_name) == 1:
        # appropriate code will be completed once there is relevant data in the collection. Till then, the following
        # values will be displayed
        what = 'The Bookings have fallen down'
        why =  '(strength is low )Pricing model is not good compare to competitors'
        status_quo = 'Further drop in market share'
        action = 'Adjust Price based on pricing model. Use simulation to arrive at a Price. Not an issue when ' \
                 '90 percent of capacity is booked'

    elif len(strength_collection_name) > 1:
        # appropriate code will be completed once there is relevant data in the collection. Till then, the following
        # values will be displayed
        what = 'The Bookings have fallen down'
        why = '(strength is low )Pricing model is not good compare to competitors'
        status_quo = 'Further drop in market share'
        action = 'Adjust Price based on pricing model. Use simulation to arrive at a Price. Not an issue when ' \
                 '90 percent of capacity is booked'

    else:
        # appropriate code will be completed once there is relevant data in the collection. Till then, the following
        # values will be displayed
        what = 'The Bookings have fallen down'
        why = '(strength is low )Pricing model is not good compare to competitors'
        status_quo = 'Further drop in market share'
        action = 'Adjust Price based on pricing model. Use simulation to arrive at a Price. Not an issue when ' \
                 '90 percent of capacity is booked'

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

    print rna_strength (a)