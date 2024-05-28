"""
Author              -   Sai Krishna
Completed Dates     -
Calculation of Price War Test -

"""
import MarketElasticity


class PriceWar(object):
    """
    """
    def __init__(self, recommendation):
        """
        """
        self.reco = recommendation

    def get_percent_ms_change_anticipated(self):
        """
        :return:
        """
        return 2

    def get_percent_price_change_recommended(self):
        """
        :return:
        """
        return (self.reco['price_recommendation'] - self.reco['host_pricing_data']['total_fare'])* 100 /float(self.reco['host_pricing_data']['total_fare'])

    def get_market_elasticity(self):
        """
        :return:
        """
        obj = MarketElasticity.MarketElasticity(recommendation=self.reco)
        return obj.get_market_elasticity()

    def get_price_war_bool(self):
        """
        :return:
        """
        prct_chng_ms = self.get_percent_ms_change_anticipated()
        prct_chng_fare = self.get_percent_price_change_recommended()
        mrkt_elasticity = self.get_market_elasticity()
        response = 'NA'
        if prct_chng_ms and prct_chng_fare:
            if prct_chng_ms and prct_chng_fare != 'NA':
                price_war_param = abs(prct_chng_ms / float(prct_chng_fare))
                if mrkt_elasticity and mrkt_elasticity != 'NA':
                    if price_war_param > mrkt_elasticity:
                        response = 'Yes'
                    else:
                        response = 'False'
        return response

if __name__ == '__main__':
    recommendation = {
        'region': 'GCC',
        'country': 'QA',
        'pos': 'DOH',
        'origin': 'DOH',
        'compartment': 'Y',
        'destination': 'BOM',
        'price_recommendation': 520,
        'host_pricing_data':{
            'total_fare': 452
        }
    }
    obj = PriceWar(recommendation=recommendation)
    print obj.get_price_war_bool()