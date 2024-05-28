"""
"""


class ExpectedReactions(object):
    def __init__(self, recommendation):
        self.reco = recommendation

    def get_host_action(self):
        self.host_action = 'INC'
        host_fare_chng = self.reco['price_recommendation'] - self.reco['host_pricing_data']['total_fare']
        if host_fare_chng > 0:
            return 'INC'
        else:
            return 'DEC'

    def get_competitor_reaction(self, airline, host_action):
        if airline in ['EK', 'EY']:
            return 'NIL'
        else:
            return host_action

    def get_top5_comp_reactions(self):
        """
        :return:
        """
        response = dict()
        top5_comp = ['EK', 'EY', 'G9', 'QR', 'ET']
        host_action = self.get_host_action()
        for comp in top5_comp:
            response[comp] = self.get_competitor_reaction(airline=comp,
                                                          host_action=host_action)
        return response


