"""
File Name              :	Customer_Segment_Product_Features_RnA
Author                 :	K Sai Krishna
Date Created           :	2016-12-19
Description            :	RnA analysis for review of specific_fares for different customer segments
Status                 :    Very Basic only static text response

MODIFICATIONS LOG	       :
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :
"""
import inspect
import jupiter_AI.common.ClassErrorObject as errorClass


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


def build_query(dict_scr_filter):
    """
    Build the query for the relevant collection for filtering the data
    :param dict_scr_filter:
    :return:
    """
    #   Based on the collection where customer segments data will be obtained
    #   this query will be built
    return dict()


def get_data_customer_segment_fares(dict_scr_filter):
    """
    Build the appropriate pipeline for obtaining the data related to customer segments
    Call the ***** collection and get the data
    Send the response as a list
    :param dict_scr_filter:
    :return:
    """
    qry_customer_segments = build_query(dict_scr_filter)
    ppl_customer_segments = [

    ]

    data_customer_segments = list()
    return data_customer_segments


def gen_customer_segments_fares_rna(dict_scr_filter):
    """
    :param dict_scr_filter:
    :return:
    """
    data = get_data_customer_segment_fares(dict_scr_filter)
    if len(data) == 0:
        #   ERROR HANDLING FOR NO RESPONSE FROM dB
        # no_data_error = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
        #                                        get_module_name(),
        #                                        get_arg_lists(inspect.currentframe()))
        # no_data_error.append_to_error_list('Null Response from Database')
        # raise no_data_error
        what = ''.join(['Customer Segments'])
        why = ''.join(['Review the Features of the Products provided for different Customer Segments'])
        status_quo = ''.join([''])
        action = ''.join(['Review customer segment product features and try to match the competitors product features'])
    elif len(data) == 1:
        #   1   Pos,Od,Compartment combination in Consideration
        #   Logic will/should be coded here
        what = ''.join(['Customer Segments'])
        why = ''.join(['Review the Features of the Products provided for different Customer Segments'])
        status_quo = ''.join([''])
        action = ''.join(['Review customer segment product features and try to match the competitors product features'])
    elif len(data) > 1:
        #   Multiple Pos,Od,Compartment combinations in consideration
        what = ''.join(['Customer Segments'])
        why = ''.join(['Review the Features of the Products provided for different Customer Segments'])
        status_quo = ''.join([''])
        action = ''.join(['Review customer segment product features and try to match the competitors product features'])
    response = dict(what=what,
                    why=why,
                    status_quo=status_quo,
                    action=action)
    return response

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

print gen_customer_segments_fares_rna(a)