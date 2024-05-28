import jupiter_AI.tiles.sales_today.sales_flown_profile as sfp
import jupiter_AI.tiles.sales_today.sales_profile as sp
import jupiter_AI.tiles.sales_today.sales_runrate as sr
import jupiter_AI.tiles.sales_today.sales_strength as ss


def get_sales_today_tiles(afilter):
    #   sales_run_rate
    #   sales_strength
    #   sales_profile

    response = dict()
    response['sales_runrate'] = sr.get_sales_runrate(afilter)
    response['sales_strength'] = ss.get_sales_strength(afilter)
    response['sales_profile'] = sp.get_sales_profile(afilter)
    response['sales_flown_profile'] = sfp.get_sales_profile_flown(afilter)
    return response

import time
st = time.time()
P = {'region': ['GCC'],
     'country': ['SA'],
     'pos': ['RUH'],
     'origin': ['RUH'],
     'destination': ['CMB'],
     'compartment': ['Y'],
     'fromDate': '2016-09-10',
     'toDate': '2016-09-12',
     'flag': 'true'}
sales_today = get_sales_today_tiles(P)
print "sales_today_tiles",sales_today
print time.time() - st