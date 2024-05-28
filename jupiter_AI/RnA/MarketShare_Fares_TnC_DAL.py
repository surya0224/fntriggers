"""
File Name              :   MarketShare_Fares_TnC_DAL
Author                 :   Shamail Mulla
Date Created           :   2016-12-19
Description            :   RnA analysis for increasing/decreasing market share from the perspective of fares terms and conditions.

MODIFICATIONS LOG         :
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :
"""

import jupiter_AI.RnA.common_RnA_functions as CF


def get_market_related_data(dict_scr_filter):
    """
       Build the appropriate pipeline and hit the dB to get the relevant data
       for building RnA
       :param dict_scr_filter:
       :return:
    """
    qry_market_share = CF.build_query_schedule_col(dict_scr_filter)
    ppln_market_share = []

    # ppln_market_share = [
    #
    # ]

    # rest of the codes to compute market share will be completed here
    # code to calculate fair market share has to be included in this.

    #results of the query will be stored in a list
    result = list()
    print type(result)

    return result