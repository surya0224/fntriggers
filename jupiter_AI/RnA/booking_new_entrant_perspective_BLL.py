"""
File Name              :   booking_new_entrant_perspective_BLL
Author                 :   Ayan Prabhakar Baruah
Date Created           :   2016-12-19
Description            :   RnA analysis for effect of new entrant in the market
Business logic layer

MODIFICATIONS LOG         :
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :
"""
import jupiter_AI.RnA.booking_new_entrant_perspective_DAL as DAL
import jupiter_AI.RnA.common_RnA_functions as CF
import random

''' based on the filter on screen check the competitiors
and compare with competitors in previous bracket

'''


def check_competitor_from_lyr(dict_competitor_filter):
    '''
    from data on screen look through competitors
    :param dict_competitor_filter:
    :return:list()
    '''
    rna_response = dict()
    market_competitor_collection= DAL.get_competitor_data(dict_competitor_filter)

    if len(market_competitor_collection)==0:
        '''if no new entrants in the market'''

        what = 'New-entrant'
        why = 'there is a new entrant in the market'
        status_quo = 'market share will drop'
        action = 'Do not react if current market share is above fair market share.' \
                 'If not float  an appropirate fare in a RBD with appropirate inventory with a firm end date.' \
                 'Not an issue if we have more than 90% of the capacity ticketed.'

    if len(market_competitor_collection)==1:
        '''if only one new entrant in the market'''

        what = 'New-entrant'
        why = 'there is a new entrant in the market'
        status_quo = 'market share will drop'
        action = 'Do not react if current market share is above fair market share.' \
                 'If not float  an appropirate fare in a RBD with appropirate inventory with a firm end date.' \
                 'Not an issue if we have more than 90% of the capacity ticketed.'
    if len(market_competitor_collection)>1:
        '''if multipe new entrants in the market'''

        what = 'New-entrant'
        why = 'there is a new entrant in the market'
        status_quo = 'market share will drop'
        action = 'Do not react if current market share is above fair market share.' \
                 'If not float  an appropirate fare in a RBD with appropirate inventory with a firm end date.' \
                  'Not an issue if we have more than 90% of the capacity ticketed.'

    rna_response['what'] = what
    rna_response['why'] = why
    rna_response['status quo'] = status_quo
    rna_response['action'] = action

    return rna_response

if __name__ == '__main__':
    a = {
        'region': [],
        'country': [],
        'pos': [],
        'origin': ['DOH', 'DXB'],
        'destination': ['DXB', 'DOH'],
        'compartment': ['Y', 'J'],
        'fromDate': '2016-10-01',
        'toDate': '2016-10-30',
        'from_month': 8,
        'from_year':2016,
        'to_month': 12,
        'to_year': 2016,
        'flag' : 0
    }

    print check_competitor_from_lyr(a)