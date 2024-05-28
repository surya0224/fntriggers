"""
File Name              :   PricingActions.py
Author                 :   Sai Krishna
Date Created           :   2017-03-15
Description            :
Modification Log       :
"""
'''
    RBD which we are recommending
    Availability of an RBD
    For the period that we calculate in Ageing
'''
import datetime
from copy import deepcopy

from dateutil.relativedelta import relativedelta

from jupiter_AI import client,JUPITER_DB, SYSTEM_DATE, today, Host_Airline_Code
db = client[JUPITER_DB]


class PricingActions(object):
    """
    """
    def __init__(self, recommendation):
        """
        """
        self.reco = recommendation
        self.market = {
            'region': self.reco['region'],
            'country': self.reco['country'],
            'pos': self.reco['pos'],
            'origin': self.reco['origin'],
            'destination': self.reco['destination'],
            'compartment': self.reco['compartment']
        }

    def generate_workflow_qry(self):
        """
        :return:
        """
        day_6m_earlier_obj = today + relativedelta(months=-6)
        day_6m_earlier_str = datetime.datetime.strftime(day_6m_earlier_obj, '%Y-%m-%d')

        workflow_qry = deepcopy(self.market)
        workflow_qry.update({
            'triggering_event_date': {
                '$lte': SYSTEM_DATE,
                '$gte': day_6m_earlier_str
            }
        })
        return workflow_qry

    def get_host_reactions_num(self):
        """
        :return:
        """
        workflow_qry = self.generate_workflow_qry()
        print workflow_qry
        reco_num_crsr = db.JUP_DB_Workflow_1.find(workflow_qry)
        return reco_num_crsr.count()

    def get_competitor_reactions_num(self):
        """
        :return:
        """
        workflow_qry = self.generate_workflow_qry()
        workflow_qry.update({
            'trigger_type': 'competitor_price_change'
        })

        workflow_ppln = [
            {
                '$match': workflow_qry
            }
            ,
            {
                '$group':
                    {
                        '_id': '$old_doc_data.airline',
                        'num': {'$sum': 1}
                    }
            }
            ,
            {
                '$project': {
                    '_id': 0,
                    'airline': '$_id',
                    'reactions_num': '$num'
                }
            }
        ]
        print workflow_ppln
        comp_reactions_crsr = db.JUP_DB_Workflow_1.aggregate(workflow_ppln)
        comp_reactions_data = list(comp_reactions_crsr)
        return comp_reactions_data

    def get_pricing_actions(self):
        """
        :return:
        """
        response = dict()
        host_num = self.get_host_reactions_num()
        response[Host_Airline_Code] = host_num
        comp_nums = self.get_competitor_reactions_num()
        for comp in comp_nums:
            response[comp['airline']] = comp['reactions_num']

        return response