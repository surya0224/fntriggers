"""
Author - Sai Krishna
Date - 2017.3.15
Desc - Tile class for workflow screen for calculation of Success Rate for a recommendation
"""


class HistoricalSuccessRate(object):
    def __init__(self, recommendation):
        self.reco = recommendation

    def build_qry_trigger_review(self):
        review_qry = dict()
        review_qry['region'] = self.reco['region']
        review_qry['country'] = self.reco['country']
        review_qry['pos'] = self.reco['pos']
        review_qry['origin'] = self.reco['origin']
        review_qry['destination'] = self.reco['destination']

    def get_success_rate_val(self):
        return 'NA'