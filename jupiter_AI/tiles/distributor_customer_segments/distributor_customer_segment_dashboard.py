"""
File Name              :   distributor.py
Author                 :   Pavan
Date Created           :   2016-12-21
Description            :   This module furnishes relevant information on revenue, pax, yield and average fare for
                            different distribution channels, distributors, frequent flyers and customer segments
MODIFICATIONS LOG
    S.No               :    2
    Date Modified      :    2017-02-08
    By                 :    Shamail Mulla
    Modification Details   : Code optimisation
"""
import jupiter_AI.tiles.distributor_customer_segments.direct_and_indirect_revenue_share as di
import jupiter_AI.tiles.distributor_customer_segments.distributor as ds
import jupiter_AI.tiles.distributor_customer_segments.customer_segments as cs
import jupiter_AI.tiles.distributor_customer_segments.frequent_flyer_revenue as fr
from jupiter_AI import client, JUPITER_DB
db = client[JUPITER_DB]

def get_distributor_customer_segments_tiles(afilter):
    '''
    Calls relevant functions to display distributor and customer segment information
    :param afilter: filter values received form the user
    :return: dictionary of distributor and customer reponses
    '''
    response = dict()
    response['direct_and_indirect_revenue_share'] = di.get_direct_channel_revenue_share(afilter)
    response['distributors'] = ds.get_distributor(afilter)
    response['customer_segments'] = cs.get_customer_segments(afilter)
    response['frequent_flyer_revenue'] = fr.get_frequent_flyer_revenue_share(afilter)
    return response

if __name__ == '__main__':
    import time
    st = time.time()
    filter = {
         'region': [],
         'country': [],
         'pos': [],
         'origin': ['DXB'],
         'destination': ['DOH'],
         'compartment': [],
         'fromDate': '2017-01-01',
         'toDate': '2017-01-31'
}
    distributor_customer = get_distributor_customer_segments_tiles(filter)
    print "distributor_customer_segments_tiles", distributor_customer
    print time.time() - st