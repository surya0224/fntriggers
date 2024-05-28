"""
File Name              :   dashboard.py
Author                 :   Sai Krishna
Date Created           :   2016-12-19
Description            :   module with methods to calculate tiles for competitor analysis dashboard
MODIFICATIONS LOG
    S.No               : 1
    Date Modified      : 2017-10-02
    By                 : Shamail Mulla
    Modification Details   : Code optimisation
"""
import inspect
import time
from collections import defaultdict
from copy import deepcopy

import numpy

from jupiter_AI.common import ClassErrorObject as errorClass

from jupiter_AI.tiles.competitor_analysis.competitor_rating import get_competitor_rating
from jupiter_AI.tiles.competitor_analysis.market_leader_host_rank import get_market_leader_host_rank
from jupiter_AI.tiles.competitor_analysis.price_intelligence_quotient import get_price_intelligence_quotient
from jupiter_AI.tiles.competitor_analysis.price_performance_index import get_price_performance_index, build_query_sales


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


def get_rank(aarray):
    """
    Module takes an array as input and
    returns an array that represents the ranks of elements in the input array
    when sorted in the descending order
    Rank 0  is the first rank
    While using this function need to add all the elements by +1
    to get the actual ranks(starting from 1)
    :param aarray:
    :return:
    """
    array = numpy.array(aarray)
    order = array.argsort()[::-1]
    return order.argsort()


def analyze_bookings_data(tot_bookings_host, airlines_dict):
    """
    This Function ranks the airlines based on the bookings and returns the
    Airline with First Rank and the host's Rank
    :param tot_bookings_host:No of Bookings for host
    :type tot_bookings_host:int
    :param airlines_dict: dictionary containing airlines names as keys
    :type airlines_dict: defaultdict
    :return top_airline: Top Airlines Name in string
    :return host_rank: Host Rank in String
    """
    bookings_list = [tot_bookings_host]
    airlines = ['host']
    for airline, bookings in airlines_dict.items():
        bookings_list.append(bookings)
        airlines.append(airline)
    top_airline = airlines[bookings_list.index(max(bookings_list))]
    host_rank = get_rank(bookings_list)[0] + 1
    return top_airline, host_rank


def get_revenue_and_vlyr(afilter):
    """
    Function Calculates the revenue and VLYR according to the filter
    :param filter_from_java: filter from java
    :type filter_from_java:dict
    :return tot_revenue_host:Revenue int
    :return vlyr:percentage float between 1-100
    """
    filter_competitor_dhb_scr = deepcopy(defaultdict(list, afilter))
    qry_sales = build_query_sales(filter_competitor_dhb_scr)
    response = dict()
    ppl_revenue_vlyr = [
        {
            '$match': qry_sales,
        },
        {
            '$group':
                {
                    '_id': None,
                    'revenue': {'$sum': '$revenue_base'},
                    'revenue_ly': {'$sum': '$revenue_base_1'}
                }
        },
        {
            '$project':
                {
                    'revenue': '$revenue',
                    'vlyr':
                        {
                            '$cond':
                                {
                                    'if': {'$ne': ['$revenue_ly', 0]},
                                    'then': {'$divide': [{'$subtract': ['$revenue', '$revenue_ly']}, '$revenue_ly']},
                                    'else': 'NA'
                                }

                        },
                    'revenue_ly': '$revenue_ly'
                }
        }
    ]
    crsr_revenue = db.JUP_DB_Sales.aggregate(ppl_revenue_vlyr)
    lst_data_revenue = list(crsr_revenue)
    #   Analysing the data obtained from dB
    if len(lst_data_revenue) != 0:
        if len(lst_data_revenue) == 1:
            response['revenue'] = round(lst_data_revenue[0]['revenue'], 3)
            if lst_data_revenue[0]['vlyr'] != 'NA':
                response['vlyr'] = round(lst_data_revenue[0]['vlyr'], 3)
            else:
                response['vlyr'] = lst_data_revenue[0]['vlyr']
            return response
        else:
            unexpected_data_error = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                                           get_module_name(),
                                                           get_arg_lists(inspect.currentframe()))
            unexpected_data_error.append_to_error_list('Expected 1 document from Sales flown data but obtained ' + str(len(lst_data_revenue)))
            # raise unexpected_data_error
            response['tot_revenue'] = 'NA'
            response['revenue_vlyr'] = 'NA'
            return response
    else:
        no_data_error = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                               get_module_name(),
                                               get_arg_lists(inspect.currentframe()))
        no_data_error.append_to_error_list('Null Response from database')
        response['tot_revenue'] = 'NA'
        response['revenue_vlyr'] = 'NA'
        return response


def get_tiles(filter_competitor_dhb_scr):
    """

    :param filter_competitor_dhb_scr:
    :return:
    """
    response = dict()
    # response['revenue_vlyr'] = get_revenue_and_vlyr(filter_competitor_dhb_scr)
    response['competitor_ratings'] = get_competitor_rating(filter_competitor_dhb_scr)
    response['market_leader_host_rank'] = get_market_leader_host_rank(filter_competitor_dhb_scr)
    response['price_performance_index'] = get_price_performance_index(filter_competitor_dhb_scr)
    response['price_intelligence_quotient'] = get_price_intelligence_quotient(filter_competitor_dhb_scr)
    return response


if __name__ == '__main__':
    st = time.time()
    a = {
        'region': [],
        'country': [],
        'pos': [],
        'origin': ['AGP'],
        'destination': ['AUS'],
        'compartment': [],
        "toDate": "2017-02-28",
        "fromDate": "2017-02-14"
    }
    print get_tiles(filter_competitor_dhb_scr=a)
