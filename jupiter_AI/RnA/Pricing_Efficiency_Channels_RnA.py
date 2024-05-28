"""
File Name              :	pricing_efficiency_channels.py
Author                 :	K Sai Krishna
Date Created           :	2016-12-14
Description            :	RnA analysis for bad price efficiency with channels as a perspective.
Status                 :

MODIFICATIONS LOG	       :
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :
"""

import inspect
import json
import random
from datetime import date
from jupiter_AI import client, JUPITER_DB
db = client[JUPITER_DB]
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


def build_query_fares_col(dict_scr_filter):
    """
    Build the query for filtering fares from Host Fares Data according to the filter
    :param dict_scr_filter:
    :return:
    """
    qry_fares = dict()
    qry_fares['$and'] = []
    today = str(date.today())
    if dict_scr_filter['region']:
        qry_fares['$and'].append({'region': {'$in': dict_scr_filter['region']}})
    if dict_scr_filter['country']:
        qry_fares['$and'].append({'country': {'$in': dict_scr_filter['country']}})
    if dict_scr_filter['pos']:
        qry_fares['$and'].append({'pos': {'$in': dict_scr_filter['pos']}})
    if dict_scr_filter['compartment']:
        qry_fares['$and'].append({'compartment': {'$in': dict_scr_filter['compartment']}})
    if dict_scr_filter['origin']:
        od_build = []
        for idx, item in enumerate(dict_scr_filter['origin']):
            od_build.append({'origin': item,
                             'destination': dict_scr_filter['destination'][idx]})
            qry_fares['$and'].append({'$or': od_build})

    qry_fares['$and'].append({'circular_period_start_date': {'$lte': today}})
    qry_fares['$and'].append({'circular_period_end_date': {'$gte': today}})

    # qry_fares['dep_date'].append({'dep_date': {'$gte': dict_scr_filter['fromDate'],
    #                                            '$lte': dict_scr_filter['toDate']}})
    return qry_fares


"""
CC
TA
GDS
Interline
Web

"""


def get_channels_list():
    """
    Returns the exhaustive list of channels for the airline/system
    :return:
    """
    channel_list = ['GDS ', 'WEB ', 'TA']
    return channel_list


def get_channel_related_data(dict_scr_filter):
    """
    Build the appropriate pipeline and hit the dB to get the relevant data
    for building RnA
    :param dict_scr_filter:
    :return:
    """
    #   Exhaustive list of channels
    channels_list = get_channels_list()
    #   building the query to filter data
    qry_fares = build_query_fares_col(dict_scr_filter)

    #   Creation of the pipeline for the mongodB aggregate
    ppln_for_channel_fares = [
        {
            '$match': qry_fares
        }
        ,
        {
            '$group': {
                '_id': {
                    'pos': '$pos',
                    'origin': '$origin',
                    'destination': '$destination',
                    'compartment': '$compartment',
                    'channel': '$channel'
                }
                ,
                'num_fares': {'$sum': 1}
            }
        }
        ,
        {
            '$project': {
                '_id': 0,
                'pos': '$_id.pos',
                'origin': '$_id.origin',
                'destination': '$_id.destination',
                'compartment': '$_id.compartment',
                'channel': '$_id.channel',
                'num_fares': '$num_fares'
            }
        }
        ,
        {
            '$group': {
                '_id': {
                    'pos': '$pos',
                    'origin': '$origin',
                    'destination': '$destination',
                    'compartment': '$compartment',
                },
                'num_channel': {'$sum': 1},

                'channel_list': {'$push': '$channel'}
            }
        }
        ,
        {
            '$project': {
                '_id': 0,
                'pos': '$_id.pos',
                'origin': '$_id.origin',
                'destination': '$_id.destination',
                'compartment': '$_id.compartment',
                'list_channel_no_fares': {
                    '$setDifference': [channels_list, '$channel_list']
                }
            }
        }
        ,
        {
            '$project': {
                '_id': 0,
                'pos': '$_pos',
                'origin': '$origin',
                'destination': '$destination',
                'compartment': '$compartment',
                'num_channel_no_fares': {'$size': '$list_channel_no_fares'},
                'list_channel_no_fares': '$list_channel_no_fares'
            }
        }
    ]

    #   Getting the response from JUP_DB_Host_Pricing_Data collection for the above aggregate pipeline
    channel_cursor = db.JUP_DB_Host_Pricing_Data.aggregate(ppln_for_channel_fares)

    #   Converting the mongo dB cursor into a list
    channel_data = list(channel_cursor)
    return channel_data


def gen_price_efficiency_channels_rna(dict_scr_filter):
    """
    Function takes the filter as input and calls relevant function to get data from dB
    Then based on the response from dB
        Outputs a dictionery with 4 keys(what,why,status_quo,action) to be represented as RnA on screen
    :param dict_scr_filter:
    :return:
    """
    rna_response = dict()
    channels_data = get_channel_related_data(dict_scr_filter)
    #   Error handling of r no response from dB
    if len(channels_data) == 0:
        no_data_error = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                               get_module_name(),
                                               get_arg_lists(inspect.currentframe()))
        desc = 'No response from db for the filter provided'
        no_data_error.append_to_error_list(desc)
        raise no_data_error
    #   only 1 pos od compartment in filter
    elif len(channels_data) == 1:
        if channels_data[0]['list_channel_no_fares']:
            channels_no_fares = [item.encode() for item in channels_data[0]['list_channel_no_fares']]
            what = 'Price Efficiency'
            why = ''.join(['No Fares available for ',
                           str(channels_data[0]['num_channel_no_fares']),
                           str(channels_no_fares),
                           ' for travel between the selected departure dates'])
            status_quo = '***'
            action = 'Quote Fares for these channels in the respective pos,od,compartment combination'
        else:
            channels_no_fares = [item.encode() for item in channels_data[0]['list_channel_no_fares']]
            what = 'Price Efficiency'
            why = ''.join(['Fares are available for all channels in the pos,od,compartment combination selected'])
            status_quo = '***'
            action = '***'
    #   More than 1 pos,od,compartment combinations in the filter
    elif len(channels_data) > 1:
        total = 0
        affected = 0
        for doc in channels_data:
            total += 1
            if doc['num_channel_no_fares'] > 0:
                affected += 1
        print affected
        if affected != 0:
            what = 'Price Efficiency'
            why = ''.join(['No Fares available for atleast one channel in ',
                           str(affected),' out of ', str(total),
                           ' pos,od,compartment combinations selected in the filter'])
            status_quo = '***'
            action = ''.join(['Quote Fares for missing channels in ',
                              str(affected),
                              ' pos,od,compartment combinations'])
        else:
            what = 'Price Efficiency'
            why = ''.join(['Fares are available for all channels in all pos, od, compartments selected'])
            status_quo = '***'
            action = ''.join(['***'])

        rna_response['what'] = what
        rna_response['why'] = why
        rna_response['status quo'] = status_quo
        rna_response['action'] = action
    return rna_response


def price_efficiency_channels_rna_main(dict_scr_filter):
    return json.dumps(gen_price_efficiency_channels_rna(dict_scr_filter), indent=1)


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
k = price_efficiency_channels_rna_main(a)
print k