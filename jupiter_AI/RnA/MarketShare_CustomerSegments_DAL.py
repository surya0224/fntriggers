
"""
File Name              :   MarketShare_CustomerSegments_DAL
Author                 :   Shamail Mulla
Date Created           :   2016-12-19
Description            :   RnA analysis for increasing/decreasing market share from the perspective of chanels promotions.
Business logic layer

MODIFICATIONS LOG         :
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :
"""

import jupiter_AI.RnA.common_RnA_functions as CF


def get_customer_segments_data(dict_scr_filter):
    """
       Build the appropriate pipeline and hit the dB to get the relevant data
       for building RnA
       :param dict_scr_filter:
       :return:
    """
    qry_market_share = CF.build_query(dict_scr_filter)
    ppln_market_segment_share = []

    # ppln_market_share = [
    #
    # ]

    # rest of the codes to compute market share will be completed here
    # code to calculate fair market share has to be included in this.

    if ppln_market_segment_share:
        # The code in here will compile the data from database. Since data isnt available
        # the code will be updated once relevant data is published in the respective collections
        return 1
    else:
        return 0