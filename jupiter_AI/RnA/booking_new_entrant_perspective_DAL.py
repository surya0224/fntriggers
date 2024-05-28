"""
File Name              :   booking_new_entrant_perspective_DAL
Author                 :   Ayan Prabhakar Baruah
Date Created           :   2016-12-19
Description            :   RnA analysis for effect of new entrant in the market
Data Access Layer

MODIFICATIONS LOG         :
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :
"""
import jupiter_AI.RnA.common_RnA_functions as cf

def get_competitor_data(dict_competitor_filter):

    '''
    build appropriate pipeline and hit database to get relevant data
    for building RnA
    :param dict_competitor_filter:
    :return:

    '''
    qry_entrant=cf.build_query_schedule_col(dict_competitor_filter)
    ppln_for_new_entrant =[]

    #ppln_for_new_entrant=[
    #
    #   ]
    # code to check for new entranr

    result = []

    return result


