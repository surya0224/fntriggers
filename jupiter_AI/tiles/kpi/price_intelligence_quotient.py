"""
#Price Availability Index
File Name              :	price_intelligence_quotient.py
Author                 :	K Sai Krishna
Date Created           :	2016-12-12
Description            :	This file contains the functions for calculating the tiles for
                            a)  price_intelligence_quotient screen of KPI Module
                            b)  price_intelligence_quotient tile on KPI dashboard
MODIFICATIONS LOG	   :
Date Modified          :
Modification Details   :
"""

try:

    from jupiter_AI.tiles.competitor_analysis.dashboard import get_price_intelligence_quotient
    from jupiter_AI.network_level_params import na_value

    from copy import deepcopy
    from collections import defaultdict
except:
    pass

'''
  get_tiles function calculates the 4 tiles of the price intelligence quotient screen
  1   host missing fares
  2   competitors only fares
  3   host only fares
  4   variations in fares
'''

"""
Try to break up the working into smaller functions like for query_builder
Split file into DAL and BLL, all python processing to be done in BLL
Build and fire the queries in DAL
Leave sufficient lines when a piece of logic is completed
"""

import jupiter_AI.common.ClassErrorObject as errorClass

def get_tiles(afilter):
    """
    :param afilter:
    :return:
    """
    response = dict()
    try:
        piq = get_price_intelligence_quotient(afilter=afilter)
        if piq != na_value:
            response['host_missing_fares'] = 100 - piq
        else:
            response['host_missing_fares'] = na_value
        response['competitor_only_fares'] = na_value
        response['host_only_fares'] = na_value
        response['variations_in_fares'] = na_value
    except errorClass.ErrorObject:
        response['host_missing_fares'] = na_value
        response['competitor_only_fares'] = na_value
        response['host_only_fares'] = na_value
        response['variations_in_fares'] = na_value
    return response

