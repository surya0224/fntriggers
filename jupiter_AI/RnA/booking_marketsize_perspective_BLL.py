"""
File Name              :   booking_marketsize_perspective_DAL
Author                 :   Ayan Prabhakar Baruah
Date Created           :   2016-12-19
Description            :   RnA analysis for effect of change in marketsize
Data Access Layer

MODIFICATIONS LOG         :
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :
"""

import jupiter_AI.RnA.booking_marketsize_perspective_DAL as dal
import jupiter_AI.RnA.common_RnA_functions as cf
import random

"""
Based on the filter on screen check the status of revenue (if it is low or high)
    market share
        vlyr -
            scale - (positive or negative)
            relevance -

"""

def check_market_share_vlyr(dict_market_size):
    """
    Seeing the data/tiles on screen
    This function tells whether to do this
    perspective analysis or not
    :param dict_scr_filter:
    :return:
    """
    return bool(random.getrandbits(1))

def get_market_size(dict_market_size):

    """
    the function check_market_share_vlyr will be called to check the value of the vlyr. This is being done
    under the assumption that the value of the vlyr drives the RnA

    ******** please enter codes to do the above ***********
    """

    rna_response = dict()
    market_size_collection = dal.get_market_related_data(dict_market_size)
    # The function get_btc_data needs to be called to compute total ticketed value. if total
    # ticketed value is more than 90 percent then an action is not required. Currently this function
    # is not being called since data is not available to complete this RnA. The function will be called
    # and appropriate code will be populated once data is available

    #   Error handling for no response from dB
    if len( market_size_collection) == 0:

        # no_data_error = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
        #                                        get_module_name(),
        #                                        get_arg_lists(inspect.currentframe()))
        # desc = 'No response from db for the filter provided'
        # no_data_error.append_to_error_list(desc)
        # raise no_data_error
        # **************************************************
        # **************************************************

        # The above error code will be uncommented when there is relevant data in the collections
        #to compute R & A.

        what = 'Booking-marketsize'
        why = 'The overall market size has gone down.'
        status_quo = 'Further drop in market share'
        action = 'Do not react if current market share is above fair market share.'\
                 'If not float  an appropirate fare in a RBD with appropirate inventory with a firm end date.'\
                 'Use simulation to arrive at the number.'\
                 'Not an issue if we have more than 90% of the capacity ticketed.'\

    elif len(market_size_collection) == 1:
        # appropriate code will be completed once there is relevant data in the collection. Till then, the following
        # values will be displayed
        what = 'Booking-marketsize'
        why = 'The overall market size has gone down.'
        status_quo = 'Further drop in market share'
        action = 'Do not react if current market share is above fair market share.' \
                 'If not float  an appropirate fare in a RBD with appropirate inventory with a firm end date.' \
                 'Use simulation to arrive at the number.' \
                 'Not an issue if we have more than 90% of the capacity ticketed.' \

    elif len(market_size_collection) > 1:
        # appropriate code will be completed once there is relevant data in the collection. Till then, the following
        # values will be displayed
        what = 'Booking-marketsize'
        why = 'The overall market size has gone down.'
        status_quo = 'Further drop in market share'
        action = 'Do not react if current market share is above fair market share.' \
                 'If not float  an appropirate fare in a RBD with appropirate inventory with a firm end date.' \
                 'Use simulation to arrive at the number.' \
                 'Not an issue if we have more than 90% of the capacity ticketed.' \

    else:
        # should be pass. however, once we get the relevant data appropriate code will be completed
        what = 'Booking-marketsize'
        why = 'The overall market size has gone down.'
        status_quo = 'Further drop in market share'
        action = 'Do not react if current market share is above fair market share.' \
                 'If not float  an appropirate fare in a RBD with appropirate inventory with a firm end date.' \
                 'Use simulation to arrive at the number.' \
                 'Not an issue if we have more than 90% of the capacity ticketed.' \

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

    print get_market_size(a)