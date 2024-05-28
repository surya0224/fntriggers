import os
import sys
import json
import time
import pymongo.errors

root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),os.path.pardir,os.path.pardir,os.path.pardir))
sys.path.append(root_dir)

import jupiter_AI.tiles.market.market_dashboard_screen as mds
#import jupiter_AI.tiles.market.market_outlook_screen as mos
#import jupiter_AI.tiles.market.market_barometer_scn as mbs

# the following function get tiles will obtain all the tiles present in the market
# dashboard.

def get_tiles(afilter):
    try:
        response = dict()
        response['revenue_pax'] = mds.get_revenue_pax(afilter)
        response['host_comp_market_share'] = mds.host_comp_market_share(afilter)
        response['host_capacity'] = mds.host_deployed_capacity(afilter)
        # response['host_rank'] = mds.host_rank(afilter)
        # response['market_outlook'] = mos.outlook_market(afilter)
        # response['market_barometer'] = mbs.proximity_indicator_screen(afilter)
        return response
    except Exception as error_msg:
        return error_msg
    except pymongo.errors.ServerSelectionTimeoutError as error_msg:
        return error_msg


if __name__ == '__main__':
    try:
        a = {
            'region': [],
            'country': [],
            'pos': [],
            'origin': ['DXB'],
            'destination': ['KTM'],
            'compartment': ['Y'],
            'fromDate': '2017-02-14',
            'toDate': '2017-02-20'
        }
        start_time = time.time()
        print get_tiles(afilter=a)
        print (time.time() - start_time)
    except Exception as error_msg:
        print error_msg
    except pymongo.errors.ServerSelectionTimeoutError as error_msg:
        print error_msg