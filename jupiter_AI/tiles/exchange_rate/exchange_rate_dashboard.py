"""
File Name              :   no_of_fares.py
Author                 :   Pavan
Date Created           :   2016-12-21
Description            :   Module which outputs exchange rate dashboard tiles
MODIFICATIONS LOG
    S.No               :
    Date Modified      :
    By                 :
    Modification Details   :
"""
import jupiter_AI.tiles.exchange_rate.average_exchange as ae
import jupiter_AI.tiles.exchange_rate.no_of_fares as nf
import jupiter_AI.tiles.exchange_rate.current_market_revenue as cm
import jupiter_AI.tiles.exchange_rate.yqrecovery as yq


def get_exchange_rate_tiles(afilter):
    #   Average_exchange_rate
    #   No_of_fares_impacted_by_currency_fluctuation
    #   current_market_share&current_revenue
    #   Yqrecovery/target%/targetrecoverd

    response = dict()

    response['average_exchange_rate_realization'] = ae.get_average_exchange(afilter)
    response['fares_impacted_by_currency_fluctuation'] = nf.get_no_of_fares(afilter)
    response['current_market_share_current_revenue'] = cm.get_current_market_revenue(afilter)
    response['yqrecovery_target_target_recovered'] = yq.get_yqrecovery(afilter)
    return response

if __name__ == '__main__':
    import time
    st = time.time()
    filter = {
         'region': [],
         'country': [],
         'pos': [],
         'origin': [],
         'destination': [],
         'compartment': [],
         'currency':['USD'],
         'fromDate': '2017-01-01',
         'toDate': '2017-01-31',
            }
    result = get_exchange_rate_tiles(filter)
    print "exchange_rate_dashboard_tiles",result
    print time.time() - st