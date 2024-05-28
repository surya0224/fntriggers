"""
File Name              :	Channel_Efficiency_Inccentives_RnA
Author                 :	K Sai Krishna
Date Created           :	2016-12-19
Description            :	RnA analysis for review of incentives for different channels
Status                 :

MODIFICATIONS LOG	       :
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :
"""
import inspect
import json
import sys
from datetime import date

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


def get_channels_list():
    """
    Returns the exhaustive list of channels for the airline/system
    :return:
    """
    channel_list = ['GDS ', 'WEB ', 'TA']
    return channel_list


def build_query_sales_col(dict_scr_filter):
    """
    Build the query for filtering fares from Host Fares Data according to the filter
    :param dict_scr_filter:
    :return:
    """
    qry_sales = dict()
    qry_sales['$and'] = []
    today = str(date.today())
    if dict_scr_filter['region']:
        qry_sales['$and'].append({'region': {'$in': dict_scr_filter['region']}})
    if dict_scr_filter['country']:
        qry_sales['$and'].append({'country': {'$in': dict_scr_filter['country']}})
    if dict_scr_filter['pos']:
        qry_sales['$and'].append({'pos': {'$in': dict_scr_filter['pos']}})
    if dict_scr_filter['compartment']:
        qry_sales['$and'].append({'compartment': {'$in': dict_scr_filter['compartment']}})
    if dict_scr_filter['origin']:
        od_build = []
        for idx, item in enumerate(dict_scr_filter['origin']):
            od_build.append({'origin': item,
                             'destination': dict_scr_filter['destination'][idx]})
            qry_sales['$and'].append({'$or': od_build})

    qry_sales['$and'].append({'dep_date': {'$gte': dict_scr_filter['fromDate'],
                                               '$lte': dict_scr_filter['toDate']}})
    return qry_sales


def get_channel_fares_data(dict_scr_filter):
    """

    :param dict_scr_filter:
    :return:
    """
    qry_sales_data = build_query_sales_col(dict_scr_filter)
    channel_lst = get_channels_list()
    ppl_channels_data = [
        {
            '$match': qry_sales_data
        }
        ,
        {
            '$group': {
                '_id': {
                    'pos': '$pos',
                    'origin': '$origin',
                    'destination': '$destination',
                    'compartment': '$compartment',
                    'channel': '$channel',
                    'fare': '$fare'
                },
                'no_fares': {'$addToSet': {
                    '$cond': {
                        'if': {'$and':[{'$gt': ['$pax', 0]}, {'$gt': ['$revenue_base', 0]}]},
                        'then': {'$divide': ['$revenue_base', '$pax']},
                        'else': None
                    }
                    }
                },
                'revenue': {'$sum': '$revenue_base'}
            }
        }
        ,
        {
            '$project': {
                'no_fares': {
                    '$filter': {
                        'input': '$no_fares',
                        'as': 'num_fares',
                        'cond': {'$and': [{'$lte': ['$$num_fares', sys.float_info.max]},
                                          {'$gte': ['$$num_fares', sys.float_info.min]}]}
                    }
                },
                'revenue': '$revenue'
            }
        }
        ,
        {
            '$group': {
                '_id': {
                    'pos': '$_id.pos',
                    'origin': '$_id.origin',
                    'destination': '$_id.destination',
                    'compartment': '$_id.compartment'
                },
                #   find the length of '$no fares list obtained earlier
                'total_fares': {'$sum': {'$size': '$no_fares'}},
                'total_revenue': {'$sum': '$revenue'},
                'channel_details': {
                    '$push': {
                        'channel': '$_id.channel',
                        'no_fares': {'$size': '$no_fares'},
                        'revenue': '$revenue'
                    }
                }
            }
        }
        ,
        {
            '$unwind': '$channel_details'
        }
        ,
        {
            '$project': {
                '_id': 0,
                'pos': '$_id.pos',
                'origin': '$_id.origin',
                'destination': '$_id.destination',
                'compartment': '$_id.compartment',
                'channel': '$channel_details.channel',
                'no_fares': '$channel_details.no_fares',
                'total_fares': '$total_fares',
                'percent_of_total_fares': {
                    '$cond': {
                        'if': {'$ne': ['$total_fares', 0]}
                        ,
                        'then': {'$divide': ['$channel_details.no_fares', '$total_fares']}
                        ,
                        'else': None
                    }
                },
                'revenue': '$channel_details.revenue',
                'total_revenue': '$total_revenue',
                'percent_of_total_revenue': {
                    '$cond': {
                        'if': {'$ne': ['$total_revenue', 0]}
                        ,
                        'then': {'$divide': ['$channel_details.revenue', '$total_revenue']}
                        ,
                        'else': None
                    }
                }
            }
        }
        ,
        {
            '$group': {
                '_id': {
                    'pos': '$pos',
                    'origin': '$origin',
                    'destination': '$destination',
                    'compartment': '$compartment'
                }
                ,
                'channel_details': {
                    '$push': {
                        'channel': '$channel',
                        'no_fares': '$no_fares',
                        'percent_of_total_fares': '$percent_of_total_fares',
                        'revenue': '$revenue',
                        'percent_of_total_revenue': '$percent_of_total_revenue'
                    }
                }
            }
        },
        {
            '$project': {
                '_id': 0,
                'pos': '$_id.pos',
                'origin': '$_id.origin',
                'destination': '$_id.destination',
                'compartment': '$_id.compartment',
                'channel_details': '$channel_details'
            }
        }
    ]

    cursor_channel_fares = db.JUP_DB_Sales.aggregate(ppl_channels_data)
    data_channel_fares = list(cursor_channel_fares)
    return data_channel_fares


def gen_channel_efficiency_incentives_rna(dict_scr_filter):
    """
    :param dict_scr_filter:
    :return:
    """
    channels_data = get_channel_fares_data(dict_scr_filter)
    if len(channels_data) == 0:
        no_data_error = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                               get_module_name(),
                                               get_arg_lists(inspect.currentframe()))
        no_data_error_desc = ''.join(['Null Response obtained from Data Base'])
    elif len(channels_data) == 1:
        #   1 POS,OD,Compartment Combination
        what = ''.join(['Channels'])
        why = ''.join(['Review the incentives provided or to be provided for Channels'])
        status_quo = ''.join([])
        action = ''.join([])
    elif len(channels_data) > 1:
        #   Multiple pos,od,compartment combinations
        what = ''.join(['Channels'])
        why = ''.join(['Review the incentives provided or to be provided for Channels'])
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
k = gen_channel_efficiency_incentives_rna(a)
print json.dumps(k,indent=1)