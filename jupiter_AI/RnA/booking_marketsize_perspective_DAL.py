"""
File Name              :   booking_marketsize_perspective_DAL
Author                 :   Ayan Prabhakar Baruah
Date Created           :   2016-12-19
Description            :   RnA analysis for effect of change in marketsize
Data Access Layer

MODIFICATIONS LOG         :
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :
"""

import jupiter_AI.RnA.common_RnA_functions as cf
def get_marketsize_data(dict_market_size):
    '''
    build appropriate pipeline and get relevant data
    for building the RnA
    :param dict_market_size:
    :return:
    '''

    qry_marketsize=cf.build_query_schedule_col(dict_market_size)
    ppln_for_marketsize=[]

    #build pipeline for marketsize

    result=[]

    return result
