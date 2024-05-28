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
"""

import datetime
import inspect
import time
from collections import defaultdict
from copy import deepcopy
from jupiter_AI import client, JUPITER_DB
db = client[JUPITER_DB]
from jupiter_AI.common import ClassErrorObject as error_class
from jupiter_AI.network_level_params import Host_Airline_Code

from jupiter_AI.tiles.jup_common_functions_tiles import gen_collection_name as gen
from jupiter_AI.tiles.jup_common_functions_tiles import query_month_year_builder

result_coll_name = gen()

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


def build_query_mrkt_shr(filter_competitor_dhb_scr):
    '''

    :param filter_competitor_dhb_scr:
    :return:
    '''
    qry_mrkt_shr = defaultdict(list)
    if filter_competitor_dhb_scr['region']:
        qry_mrkt_shr['$and'].append({'region': {'$in': filter_competitor_dhb_scr['region']}})
    if filter_competitor_dhb_scr['country']:
        qry_mrkt_shr['$and'].append({'country': {'$in': filter_competitor_dhb_scr['country']}})
    if filter_competitor_dhb_scr['pos']:
        qry_mrkt_shr['$and'].append({'pos': {'$in': filter_competitor_dhb_scr['pos']}})
    if filter_competitor_dhb_scr['origin']:
        od_build = []
        for idx, item in enumerate(filter_competitor_dhb_scr['origin']):
            od_build.append({'od': item + filter_competitor_dhb_scr['destination'][idx]})
        qry_mrkt_shr['$and'].append({'$or': od_build})
    if filter_competitor_dhb_scr['compartment']:
        qry_mrkt_shr['$and'].append({'compartment': {'$in': filter_competitor_dhb_scr['compartment']}})
    from_obj = datetime.datetime.strptime(filter_competitor_dhb_scr['fromDate'], '%Y-%m-%d')
    to_obj = datetime.datetime.strptime(filter_competitor_dhb_scr['toDate'], '%Y-%m-%d')
    qry_mrkt_shr['$and'].append({'$or': query_month_year_builder(from_obj.month,from_obj.year,
                                                                 to_obj.month,to_obj.year)})
    return qry_mrkt_shr


def get_market_leader_host_rank(afilter):
    """
        This module calculates the values for 2 tiles
         market leader : airline with the highest pax
         host rank: rank of host in the airlines listed according to pax
    """
    try:
        filter_competitor_dhb_scr = deepcopy(defaultdict(list, afilter))
        if 'JUP_DB_Market_Share' in db.collection_names():
            qry_mrkt_shr_col = build_query_mrkt_shr(filter_competitor_dhb_scr)
            response = dict()
            ppln_ms_data = \
                [
                    {
                        '$match': qry_mrkt_shr_col,
                    }
                    ,
                    {
                        '$group':
                            {
                                '_id': '$MarketingCarrier1',
                                'bookings': {'$sum': '$pax'}
                            }
                    }
                    ,
                    {
                        '$project':
                            {
                                '_id': 0,
                                'airline': '$_id',
                                'bookings': '$bookings'
                            }
                    }
                    ,
                    {
                        '$sort': { 'bookings': -1 }
                    }
                    ,
                    # giving ranks to each airline based on bookings
                    {
                        '$group':
                            {
                                '_id': None,
                                'bookings_airline':
                                    {
                                        '$push':
                                            {
                                                'airline': '$airline',
                                                'bookings': '$bookings'
                                            }
                                    }
                            }
                    }
                    ,
                    {
                        '$unwind':
                            {
                                'path': '$bookings_airline',
                                'includeArrayIndex': 'rank'
                            }
                    }
                    ,
                    {
                        '$project':
                            {
                                '_id': 0,
                                'airline': '$bookings_airline.airline',
                                'bookings': '$bookings_airline.bookings',
                                'rank': '$rank'
                            }
                    }
                    ,
                    {
                        '$out': result_coll_name
                    }
                ]

            db.JUP_DB_Market_Share.aggregate(ppln_ms_data)
            if result_coll_name in db.collection_names():
                mrkt_shr_data = db.get_collection(result_coll_name)
                if mrkt_shr_data.count() > 0:
                    host_data = list(mrkt_shr_data.find({'airline': Host_Airline_Code}))
                    response['host_rank'] = int(host_data[0][u'rank']) + 1
                    top_airline_data = list(mrkt_shr_data.find({}, {'airline': {'$slice': 1}}))
                    response['top_airline'] = top_airline_data[0][u'airline']
                    mrkt_shr_data.drop()
                    return response
                else: # in case the resultant collection is empty
                    mrkt_shr_data.drop()
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                        'jupter_AI/tiles/competitor_analysis/market_leader_host_rank.py method: get_market_leader_host_rank',
                                                        get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list('There is no data for this query.')
                    obj_error.write_error_logs(datetime.datetime.now)
            else: # in case the result collection does not get created
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupter_AI/tiles/market/market_dashboard_screen.py method: host_deployed_capacity',
                                                    get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list(' market share data result not created in the database. Check aggregate pipeline.')
                obj_error.write_error_logs(datetime.datetime.now())
        else: # if collection to query is not present in database
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/competitor_analysis/market_leader_host_rank.py method: get_market_leader_host_rank',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Market_Share cannot be found in the database.')
            obj_error.write_error_logs(datetime.datetime.now())
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/competitor_analysis/market_leader_host_rank.py method: get_market_leader_host_rank',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())

if __name__ == '__main__':
    a = {
        'region': [],
        'country': [],
        'pos': [],
        'origin': [],
        'destination': [],
        "compartment": [],
        'fromDate': '2016-10-01',
        'toDate': '2017-03-01'
    }
    start_time = time.time()
    print get_market_leader_host_rank(a)
    print (time.time() - start_time)