"""
"""


class ExpectedOutcomes(object):
    def __init__(self, recommendation):
        self.reco = recommendation

    def get_expected_bookings_perc_chng(self):
        return 5

    def get_expected_pax_perc_chng(self):
        return 5

    def get_expected_revenue_perc_chng(self):
        return 5

    def get_expected_ms_perc_chng(self):
        return 5

    def get_expected_outcomes(self):
        response = dict(bookings='NA',
                        pax='NA',
                        revenue='NA',
                        market_share='NA')
        response['bookings'] = self.get_expected_bookings_perc_chng()
        response['pax'] = self.get_expected_pax_perc_chng()
        response['revenue'] = self.get_expected_revenue_perc_chng()
        response['market_share'] = self.get_expected_ms_perc_chng()

        return response