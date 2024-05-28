"""
Author - Sai Krishna
Date - 15.3.2017
Desc - No of promos and offers for host and competitors effective in the departure dates in consideration of trigger.
       For host last_year_values also
"""
from collections import defaultdict




class PromosAndOffers(object):
    def __init__(self, recommendation):
        """
        """
        self.reco = recommendation

    @staticmethod
    def get_promos_data():
        promos_crsr = db.JUP_DB_Promos.aggregate([
            {
                '$group':
                    {
                        '_id': '$airline',
                        'num': {'$sum': 1}
                    }
            }
            ,
            {
                '$project':
                    {
                        '_id':0,
                        'airline': '$_id',
                        'promos': '$num'
                    }
            }
        ])
        promos_data = list(promos_crsr)
        return promos_data

    @staticmethod
    def get_offers_data():
        offers_crsr = db.JUP_DB_Offers.aggregate([
            {
                '$group':
                    {
                        '_id': '$airline',
                        'num': {'$sum': 1}
                    }
            }
            ,
            {
                '$project':
                    {
                        '_id': 0,
                        'airline': '$_id',
                        'offers': '$num'
                    }
            }
        ])
        offers_data = list(offers_crsr)
        return offers_data

    def get_promos_and_offers_val(self):
        """
        :return:
        """
        response = defaultdict(dict)
        promos_data = self.get_promos_data()
        offers_data = self.get_offers_data()
        for promos_doc in promos_data:
            response[promos_doc['airline']]['promos'] = promos_doc['promos']
        for offers_doc in offers_data:
            response[offers_doc['airline']]['offers'] = offers_doc['offers']
        response = dict(response)
        for key in response.keys():
            response[key]['total'] = response[key]['promos'] + response[key]['offers']
        return response

if __name__=='__main__':
    print db
    reco = {}
    obj = PromosAndOffers(recommendation=reco)
    print obj.get_promos_and_offers_val()