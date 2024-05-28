"""
File Name              :   dashboard.py
Author                 :   Sai Krishna
Date Created           :   2016-12-19
Description            :   module with methods to calculate tiles for competitor analysis dashboard
MODIFICATIONS LOG
    S.No               : 1
    Date Modified      : 2017-02-10
    By                 : Shamail Mulla
    Modification Details   : Code optimisation

    S.No               : 2
    Date Modified      : 2017-02-15
    By                 : Sai Krishna
    Modification Details  :
"""

import datetime
import time
import inspect
from collections import defaultdict
from copy import deepcopy
from jupiter_AI import client, JUPITER_DB
db = client[JUPITER_DB]
from jupiter_AI.common import ClassErrorObject as error_class
from jupiter_AI.network_level_params import Host_Airline_Code

from jupiter_AI.tiles.competitor_analysis.market_leader_host_rank import build_query_mrkt_shr
from jupiter_AI.tiles.jup_common_functions_tiles import query_month_year_builder

revenue_target_weight = 0.6
market_share_target_weight = 0.4
market_share_target = 15


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


def build_query_sales(filter_competitor_dhb_scr):
    """
    Building the query for Sales collection from the filter on screes
    :param filter_competitor_dhb_scr:
    :return:
    """
    qry_sales = defaultdict(list)
    if filter_competitor_dhb_scr['region']:
        qry_sales['$and'].append({'region': {'$in': filter_competitor_dhb_scr['region']}})
    if filter_competitor_dhb_scr['country']:
        qry_sales['$and'].append({'country': {'$in': filter_competitor_dhb_scr['country']}})
    if filter_competitor_dhb_scr['pos']:
        qry_sales['$and'].append({'pos': {'$in': filter_competitor_dhb_scr['pos']}})
    if filter_competitor_dhb_scr['origin']:
        od_build = []
        for idx, item in enumerate(filter_competitor_dhb_scr['origin']):
            od_build.append({'origin': item, 'destination': filter_competitor_dhb_scr['destination'][idx]})
        qry_sales['$and'].append({'$or': od_build})
    if filter_competitor_dhb_scr['compartment']:
        qry_sales['$and'].append({'compartment': {'$in': filter_competitor_dhb_scr['compartment']}})

    qry_sales['$and'].append({'dep_date': {'$gte': filter_competitor_dhb_scr['fromDate'],
                                           '$lte': filter_competitor_dhb_scr['toDate']}})
    return qry_sales

# Please put comments!!
def build_query_target(filter_competitor_dhb_scr):
    """

    :param filter_competitor_dhb_scr:
    :return:
    """
    # complete the docstring! (Refer coding standards)
    qry_target = defaultdict(list)
    if filter_competitor_dhb_scr['region']:
        qry_target['$and'].append({'region': {'$in': filter_competitor_dhb_scr['region']}})
    if filter_competitor_dhb_scr['country']:
        qry_target['$and'].append({'country': {'$in': filter_competitor_dhb_scr['country']}})
    if filter_competitor_dhb_scr['pos']:
        qry_target['$and'].append({'pos': {'$in': filter_competitor_dhb_scr['pos']}})
    if filter_competitor_dhb_scr['origin']:
        od_build = []
        for idx, item in enumerate(filter_competitor_dhb_scr['origin']):
            od_build.append({'origin': item, 'destination': filter_competitor_dhb_scr['destination'][idx]})
        qry_target['$and'].append({'$or': od_build})
    if filter_competitor_dhb_scr['compartment']:
        qry_target['$and'].append({'compartment': {'$in': filter_competitor_dhb_scr['compartment']}})
    from_obj = datetime.datetime.strptime(filter_competitor_dhb_scr['fromDate'], '%Y-%m-%d')
    to_obj = datetime.datetime.strptime(filter_competitor_dhb_scr['toDate'], '%Y-%m-%d')
    qry_target['$and'].append({'$or': query_month_year_builder(from_obj.month, from_obj.year,
                                                               to_obj.month, to_obj.year)})
    return qry_target


def get_price_performance_index(afilter):
    """

    :param afilter:
    :return:
    """
    try:
        if 'JUP_DB_Target_OD' in db.collection_names() and \
                        'JUP_DB_Sales' in db.collection_names() and \
                        'JUP_DB_Market_Share' in db.collection_names():
            ppi = 0

            filter_competitor_dhb_scr = deepcopy(defaultdict(list, afilter))

            qry_sales = build_query_sales(filter_competitor_dhb_scr)
            qry_target = build_query_target(filter_competitor_dhb_scr)
            qry_mrkt_shr = build_query_mrkt_shr(filter_competitor_dhb_scr)

            ppln_revenue = [
                {
                    '$match': qry_sales
                }
                ,
                {
                    '$group':
                        {
                            '_id': None,
                            'revenue': {'$sum': '$revenue_base'}
                        }
                },
                {
                    '$project':
                        {
                            '_id': 0,
                            'revenue': '$revenue'
                        }
                }
            ]

            ppln_target = [
                {
                    '$match': qry_target
                },
                {
                    '$group':
                        {
                            '_id': None,
                            'revenue_target': {'$sum': '$revenue'}
                        }
                },
                {
                    '$project':
                        {
                            '_id': 0,
                            'revenue_target': '$revenue_target'
                        }
                }
            ]

            ppln_mrkt_shr = [
                        {
                            '$match': qry_mrkt_shr
                        },
                        {
                            '$group':
                                {
                                    '_id': None,
                                    'pax_host':
                                        {
                                            "$sum":
                                                {
                                                    "$cond":
                                                        [
                                                            {'$eq': ['$MarketingCarrier1', Host_Airline_Code]},'$pax',0
                                                        ]
                                                }
                                        },
                                    'market_size': {'$sum': '$pax'}
                                }
                        },
                        {
                            '$project':
                                {
                                    '_id': 0,
                                    'market_share': {'$multiply': [{'$divide': ['$pax_host', '$market_size']}, 100]}
                                }
                        }
                    ]

            #   Querying the Target Collection
            crsr_target = db.JUP_DB_Target_OD.aggregate(ppln_target)
            revenue_target_data = list(crsr_target)

            if len(revenue_target_data) == 0:
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    get_module_name(),
                                                    get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list('Null Response from Database')
                obj_error.append_to_error_list(datetime.datetime.now())
                revenue_target = 0

            elif len(revenue_target_data) == 1:
                revenue_target = revenue_target_data[0]['revenue_target']

            else:
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    get_module_name(),
                                                    get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list(['Expected 1 document from database but got ', str(len(revenue_target_data))])
                obj_error.append_to_error_list(datetime.datetime.now())
                revenue_target = 0

            if revenue_target != 0:
                crsr_sales = db.JUP_DB_Sales.aggregate(ppln_revenue)
                lst_revenue_data = list(crsr_sales)
                if len(lst_revenue_data) == 0:
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                        get_module_name(),
                                                        get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list('Null Response from Database')
                    obj_error.append_to_error_list(datetime.datetime.now())
                    pp_score_revenue = None
                elif len(lst_revenue_data) == 1:
                    revenue = lst_revenue_data[0]['revenue']
                    revenue_vtgt = (revenue - revenue_target) * 100 / revenue_target
                    pp_score_revenue = round(revenue_target_weight*revenue_vtgt,0)
                else:
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                        get_module_name(),
                                                        get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list(['Expected 1 document from database but got ', str(len(lst_revenue_data))])
                    obj_error.append_to_error_list(datetime.datetime.now())
                    pp_score_revenue = None
            else:
                pp_score_revenue = None

            crsr_mrkt_shr = db.JUP_DB_Market_Share.aggregate(ppln_mrkt_shr)
            lst_market_share_data = list(crsr_mrkt_shr)
            if len(lst_market_share_data) == 0:
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                                    'jupter_AI/tiles/competitor_analysis/price_performance_index.py method: get_price_performance_index',
                                                    get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list('Null Response from Database')
                obj_error.write_error_logs(datetime.datetime.now())
                pp_score_ms = None
            elif len(lst_market_share_data) == 1:
                market_share = lst_market_share_data[0]['market_share']
                market_share_vtgt = (market_share - market_share_target) * 100 / market_share_target
                if market_share_vtgt < 0:
                    pp_score_ms = round(market_share_target_weight*market_share_vtgt,0)
                else:
                    pp_score_ms = None
            else:
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                                           get_module_name(),
                                                                           get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list(['Expected 1 document from database but got ',
                                                                  str(len(lst_revenue_data))])
                obj_error.append_to_error_list(datetime.datetime.now())
                pp_score_ms = None

            if pp_score_ms:
                ppi += pp_score_ms
            if pp_score_revenue:
                ppi += pp_score_revenue
            if ppi == 0:
                ppi = None
            return ppi
        else: # If one of the collections to query are missing
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                                'jupter_AI/tiles/competitor_analysis/price_performance_index.py method: get_price_performance_index',
                                                get_arg_lists(inspect.currentframe()))
            if 'JUP_DB_Target_OD' not in db.collection_names():
                obj_error.append_to_error_list('JUP_DB_Target_OD collection not present in the database')
            if 'JUP_DB_Sales' not in db.collection_names():
                obj_error.append_to_error_list('JUP_DB_Sales collection not present in the database')
            if 'JUP_DB_Market_Share' not in db.collection_names():
                obj_error.append_to_error_list('JUP_DB_Market_Share collection not present in the database')
            obj_error.write_error_logs(datetime.datetime.now())
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/competitor_analysis/price_performance_index.py method: get_price_performance_index',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


if __name__=="__main__":
    st = time.time()
    a = {
        'region': [],
        'country': [],
        'pos': [],
        'origin': ['DXB'],
        'destination': ['DOH'],
        'compartment': [],
        "toDate": "2017-04-28",
        "fromDate": "2017-07-14"
    }
    print get_price_performance_index(a)