"""
"""
import math
from collections import defaultdict

import numpy

from jupiter_AI import client,JUPITER_DB, today
db = client[JUPITER_DB]


class MarketElasticity(object):
    """
    """
    def __init__(self, recommendation):
        """
        """
        self.reco = recommendation
        self.value = None

    def get_market_elasticity(self):
        """
        :return:
        """
        data = self.get_market_fares_data()
        pax = data[0]
        fares = data[1]
        log_pax = [math.log(x) for x in pax]
        log_fares = [math.log(fare) for fare in fares]
        if 1 < len(log_fares) == len(log_pax) > 1:
            coffs_line = numpy.polyfit(log_pax, log_fares, 1)
            return coffs_line[0]
        else:
            return 'NA'

    def get_market_fares_data(self):
        """
        :return:
        """
        mrkt_elasticity_ppln = self.generate_ppln_mrkt_elasticity()
        # print mrkt_elasticity_ppln
        mrkt_level_data_crsr = db.JUP_DB_Market_Share.aggregate(mrkt_elasticity_ppln)
        data = list(mrkt_level_data_crsr)
        # print data
        if len(data) == 1:
            return data[0]['pax'], data[0]['fares']
        else:
            return [], []

    def build_query_elasticity(self):
        """
        :return:
        """
        qry_ms = defaultdict(list)
        qry_ms['$and'].append({
            'region': self.reco['region'],
            'country': self.reco['country'],
            'pos': self.reco['pos'],
            'od': self.reco['origin'] + self.reco['destination'],
            'compartment': self.reco['compartment']
        })

        # dep_date_start_obj = datetime.datetime.strptime(
        #     self.reco['triggering_data']['dep_date_start'],
        #     '%Y-%m-%d'
        # )
        # dep_date_end_obj = datetime.datetime.strptime(
        #     self.reco['triggering_data']['dep_date_end'],
        #     '%Y-%m-%d'
        # )
        sd = today.day
        sm = today.month
        sy = today.year
        # ed = dep_date_end_obj.day
        # em = dep_date_end_obj.month
        # ey = dep_date_end_obj.year
        month_yr_combinations = [{
            'year': {'$lte': sy}
        }, {
            'year': sy,
            'month': {'$lte': sm}
        }]
        # month_yr_combinations = query_month_year_builder(stdm=sm,
        #                                                  stdy=sy,
        #                                                  endm=em,
        #                                                  endy=ey)
        qry_ms['$and'].append({
            '$or': month_yr_combinations
        })
        # print qry_ms
        return qry_ms

    def generate_ppln_mrkt_elasticity(self):
        """
        :return:
        """
        qry_ms = self.build_query_elasticity()
        ppln_market_elasticity = [
            {
                '$match': dict(qry_ms)
            }
            ,
            {
                '$group': {
                    '_id': {
                        'pos': '$pos',
                        'od': '$od',
                        'compartment': '$compartment',
                        'month': '$month',
                        'year':'$year'
                    }
                    ,
                    'mrkt_pax': {'$sum': '$pax'},
                    'mrkt_revenue': {'$sum': '$revenue'}
                }
            }
            ,
            {
                '$project': {
                    'pos': '$_id.pos',
                    'origin': '$_id.origin',
                    'destination': '$_id.destination',
                    'compartment': '$_id.compartment',
                    'month': '$_id.month',
                    'year': '$_id.year',
                    'pax': '$mrkt_pax',
                    'avg_fare': {
                        '$cond': {
                            'if': {'$gt': ['$mrkt_pax', 0]},
                            'then': {'$divide': ['$mrkt_revenue', '$mrkt_pax']},
                            'else': 'NA'
                        }
                    }
                }
            }
            ,
            {
                '$redact': {
                    '$cond': {
                       'if': {'$ne': ['$avg_fare', 'NA']},
                       'then': "$$DESCEND",
                       'else': "$$PRUNE"
                }
                }
            }
            ,
            {
                '$group': {
                    '_id': None,
                    'fares': {
                        '$push': '$avg_fare'
                    },
                    'pax': {
                        '$push': '$pax'
                    }
                }
            }
            ,
            {
                '$project': {
                    '_id': 0,
                    'fares': '$fares',
                    'pax': '$pax'
                }
            }
        ]
        return ppln_market_elasticity

if __name__=='__main__':

    crsr = db.JUP_DB_Pos_OD_Compartment.find()
    print crsr.count()
    for doc in crsr:
        recommendation = {
            'region': doc['region'],
            'country': doc['country'],
            'pos': doc['pos'],
            'origin': doc['origin'],
            'destination': doc['destination'],
            'compartment': doc['compartment']
        }
        obj = MarketElasticity(recommendation=recommendation)
        val = obj.get_market_elasticity()
        print val
        db.JUP_DB_Pos_OD_Compartment.update(
            {
                '_id': str(doc['_id'])
            },
            {
                '$set': {
                    'market_elasticity': val
                }
            })
