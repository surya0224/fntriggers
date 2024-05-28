"""
Author              - Sai Krishna
Completed Dates     -
    first version - 1.0
Calculation of the Price Stability Index    -
    Is it for the market or should it represent the recommendation's fare.
    If it is for the recommended fare -

    strategic   -   50  ideal 1(max)
    tactical    -   30  ideal 1(max)
    specific    -   20  ideal 1(max)
        If more than relevant percentage consider 1 otherwise consider actual/relevant

        Calculate the average of the three scores and multiply with 10 to get the score out of 10

"""
from jupiter_AI.tiles.price_biometrics.analyst_performance import get_price_stability_index_vlyr


class PriceStabilityIndex(object):
    """
    """
    def __init__(self, recommendation):
        """
        """
        self.reco = recommendation
        self.value = None

    def generate_filter_psi(self):
        """
        :return:
        """
        filter_psi = {
            'region': [self.reco['region']],
            'country': [self.reco['country']],
            'pos': [self.reco['pos']],
            'origin': [self.reco['origin']],
            'destination': [self.reco['destination']],
            'compartment': [self.reco['compartment']],
            'fromDate': self.reco['triggering_data']['dep_date_start'],
            'toDate': self.reco['triggering_data']['dep_date_end']
        }
        return filter_psi

    def get_psi_value(self):
        """
        :return:
        """
        self.filter = self.generate_filter_psi()
        psi_value = get_price_stability_index_vlyr(filter_scr=self.filter)['price_stability_index']
        return psi_value

if __name__ == '__main__':
    recommendation = {
        'region': 'MiddleEast',
        'country': 'AE',
        'pos': 'DXB',
        'origin': 'DXB',
        'destination': 'KTM',
        'compartment': 'Y',
        'triggering_data': {
            'dep_date_start': '2017-02-01',
            'dep_date_end': '2017-02-28'
        }
    }
    obj = PriceStabilityIndex(recommendation=recommendation)
    obj.get_psi_value()
    print 'psi', obj.value
