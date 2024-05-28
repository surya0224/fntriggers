"""
File Name              :	Pricing_Efficiency_Customer_Segments.py
Author                 :	K Sai Krishna
Date Created           :	2016-12-14
Description            :	RnA analysis for bad price efficiency with Customer Segments as a perspective.
Status                 :

MODIFICATIONS LOG	       :
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :
"""

import inspect
import random
from jupiter_AI import client, JUPITER_DB
db = client[JUPITER_DB]

from jupiter_AI.RnA.common_RnA_functions import gen_collection_name



def get_module_name():
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


def check_price_efficiency(dict_scr_filter):
    """
    Based on the filter on screen check the status of pricing efficiency(is it low or high)
        pricing efficiency -
            pe signal
                scale -
                relevance -
            price intelligence quotient
                scale -
                relevance -
            price stability index
                scale -
                relevance -
            effective fares(%)
                scale - can be 0 to 100
                relevance - low percentage indicates that there are a lot of ineffective fares in the filter selected
                            There Needs to be a threshold for giving the optimum percentage effective fares over the
                            total effective fares.
    :param dict_scr_filter:
    :return:
    """
    """
    Seeing the data/tiles on screen
    This function tells whether to do this
    perspective analysis or not
    """
    return bool(random.getrandbits(1))


def build_query_fares_customer_segments(dict_scr_filter):
    """
    Build the query for filtering fares from Host Fares Data according to the filter
    :param dict_scr_filter:
    :return:
    """
    # qry_fares = dict()
    # qry_fares['$and'] = []
    # today = str(date.today())
    # if dict_scr_filter['region']:
    #     qry_fares['$and'].append({'region': {'$in': dict_scr_filter['region']}})
    # if dict_scr_filter['country']:
    #     qry_fares['$and'].append({'country': {'$in': dict_scr_filter['country']}})
    # if dict_scr_filter['pos']:
    #     qry_fares['$and'].append({'pos': {'$in': dict_scr_filter['pos']}})
    # if dict_scr_filter['compartment']:
    #     qry_fares['$and'].append({'compartment': {'$in': dict_scr_filter['compartment']}})
    # if dict_scr_filter['origin']:
    #     od_build = []
    #     for idx, item in enumerate(dict_scr_filter['origin']):
    #         od_build.append({'origin': item,
    #                          'destination': dict_scr_filter['destination'][idx]})
    #         qry_fares['$and'].append({'$or': od_build})
    #
    # qry_fares['$and'].append({'circular_period_start_date': {'$lte': today}})
    # qry_fares['$and'].append({'circular_period_end_date': {'$gte': today}})
    #
    # # qry_fares['dep_date'].append({'dep_date': {'$gte': dict_scr_filter['fromDate'],
    # #                                            '$lte': dict_scr_filter['toDate']}})
    # return qry_fares
    return dict()


def get_customer_segment_related_data(dict_scr_filter):
    """
    Build the appropriate pipeline and hit the dB to get the relevant data
    for building RnA
    :param dict_scr_filter:
    :return:
    """
    temp_collection = gen_collection_name()
    #   building the query to filter data
    qry_fares = build_query_fares_customer_segments(dict_scr_filter)

    #   Creation of the pipeline for the mongodB aggregate
    ppln_for_customer_segment_fares = [
        {
            '$out': temp_collection
        }

    ]
    #   Getting the response from JUP_DB_Host_Pricing_Data collection for the above aggregate pipeline
    # db.******.aggregate(ppln_for_customer_segment_fares)
    cursor_customer_segments = db[temp_collection].find()
    #   Converting the mongo dB cursor into a list
    customer_segments_data = []
    for doc in cursor_customer_segments:
        del doc['_id']
        customer_segments_data.append(doc)
    return customer_segments_data


def gen_price_efficiency_customer_segment_rna(dict_scr_filter):
    """
    Function takes the filter as input and calls relevant function to get data from dB
    Then based on the response from dB
        Outputs a dictionery with 4 keys(what,why,status_quo,action) to be represented as RnA on screen
    :param dict_scr_filter:
    :return:
    """
    rna_response = dict()
    customer_segments_data = get_customer_segment_related_data(dict_scr_filter)
    #   Error handling of r no response from dB
    if len(customer_segments_data) == 0:
        # no_data_error = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
        #                                        get_module_name(),
        #                                        get_arg_lists(inspect.currentframe()))
        # desc = 'Null response from Database'
        # no_data_error.append_to_error_list(desc)
        # raise no_data_error
        what = ''.join(['Pricing Efficiency is Low'])
        why = ''.join(['Review the availability of Fares for all Customer Segments'])
        status_quo = ''.join([''])
        action = ''.join([''])
    #   only 1 pos od compartment in filter
    elif len(customer_segments_data) == 1:
        what = ''.join(['Pricing Efficiency is Low'])
        why = ''.join(['Review the availability of Fares for all Customer Segments'])
        status_quo = ''.join([''])
        action = ''.join([''])
    #   More than 1 pos,od,compartment combinations in the filter
    elif len(customer_segments_data) > 1:
        what = ''.join(['Pricing Efficiency is Low'])
        why = ''.join(['Review the availability of Fares for all Customer Segments'])
        status_quo = ''.join([''])
        action = ''.join([''])

    rna_response['what'] = what
    rna_response['why'] = why
    rna_response['status quo'] = status_quo
    rna_response['action'] = action
    return rna_response


a = {
        'region': [],
        'country': [],
        'pos': [],
        'origin': [],
        'destination': [],
        'compartment': [],
        'fromDate': '2016-07-01',
        'toDate': '2016-12-31'
    }
k = gen_price_efficiency_customer_segment_rna(a)
print k