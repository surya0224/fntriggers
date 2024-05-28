"""
File Name                   :   dashboard.py
Author                      :   Sai Krishna
Date Created                :   2016-12-19
Description                 :   module with methods to calculate tiles for Price Biometric dashboard
MODIFICATIONS LOG           :
    S.No                    :
    Date Modified           :
    By                      :
    Modification Details    :
"""
from jupiter_AI.tiles.competitor_analysis.dashboard import get_price_intelligence_quotient
from jupiter_AI.tiles.kpi.effective_ineffective_fares import get_effective_ineffective_fares
from jupiter_AI.tiles.price_biometrics.analyst_performance import get_price_stability_index_vlyr
from jupiter_AI.tiles.price_biometrics.price_elasticity import pe_signal


def get_tiles(afilter):
    """

    :param afilter:
    :return:
    """
    """
        pe signal
        piq %
        psi %
        effective_fares %
    """
    response = dict()
    response['pe_signal'] = pe_signal(afilter)
    response['price_intelligence_quotient'] = get_price_intelligence_quotient(afilter)
    response['price_stability_index'] = get_price_stability_index_vlyr(afilter)['price_stability_index']
    response['effective_ineffective_fares'] = get_effective_ineffective_fares(afilter)
    return response
if __name__ == '__main__':
    a = {
        'region': [],
        'country': [],
        'pos': [],
        'origin': [],
        'destination': [],
        'compartment': [],
        'fromDate': '2016-01-01',
        'toDate': '2017-12-31',
        'lastfromDate': '2015-07-01',
        'lasttoDate': '2015-07-31'
    }
    print get_tiles(afilter=a)
