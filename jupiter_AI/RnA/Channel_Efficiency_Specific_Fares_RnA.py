"""
File Name              :	Channel_Efficiency_Specific_Fares_RnA
Author                 :	K Sai Krishna
Date Created           :	2016-12-19
Description            :	RnA analysis for review of specific_fares for different channels
Status                 :

MODIFICATIONS LOG	       :
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :
"""
import inspect
import json
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

    :param dict_scr_filter:
    :return:
    """
    return dict()


def get_data_channels_fares(dict_scr_filter):
    """
    Build the pipeline required for getting the data from ***** collection
    Obtain the data in required manner by calling the mongodB aggregate
    Convert the data into a list and return it
    :param dict_scr_filter:
    :return:
    """
    qry_channels = build_query(dict_scr_filter)
    ppl_channels_data = [

    ]
    return list()


def gen_channel_efficiency_fares_rna(dict_scr_filter):
    """
    :param dict_scr_filter:
    :return:
    """
    channels_data = get_data_channels_fares(dict_scr_filter)
    if len(channels_data) == 0:
        # no_data_error = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
        #                                        get_module_name(),
        #                                        get_arg_lists(inspect.currentframe()))
        # no_data_error_desc = ''.join(['Null Response obtained from Data Base'])
        # raise no_data_error_desc
        what = ''.join(['Channels'])
        why = ''.join(['Review the Specific Fares Quoted for Channels'])
        status_quo = ''.join([])
        action = ''.join([])
    elif len(channels_data) == 1:
        #   1 POS,OD,Compartment Combination
        what = ''.join(['Channels'])
        why = ''.join(['Review the Specific Fares Quoted for Channels'])
        status_quo = ''.join([])
        action = ''.join([])
    elif len(channels_data) > 1:
        #   Multiple pos,od,compartment combinations
        what = ''.join(['Channels'])
        why = ''.join(['Review the Specific Fares quoted for Channels'])
        status_quo = ''.join([])
        action = ''.join([])
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
        'fromDate': '2016-09-01',
        'toDate': '2016-12-31'
    }
k = gen_channel_efficiency_fares_rna(a)
print json.dumps(k,indent=1)
'''
Add the fares of all channels into a list(unique fares)
T - (B+C+D+E) : A
A - unique fare is numerically only one fare

'''