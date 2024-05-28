
"""
File Name              :   revenue_market_capacity_perspective_DAL
Author                 :   Ashwin Kumar
Date Created           :   2016-12-16
Description            :   RnA analysis for increasing/decreasing revenue from a market capacity perspective.
Data access layer

MODIFICATIONS LOG         :
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :
"""

import jupiter_AI.RnA.common_RnA_functions as cf

def get_market_related_data(dict_scr_filter):
    """
       Build the appropriate pipeline and hit the dB to get the relevant data
       for building RnA
       :param dict_scr_filter:
       :return:
    """
    qry_schedule = cf.build_query_schedule_col(dict_scr_filter)
    ppln_for_market_size = []

    # ppln_for_market_size = [
    #
    # ]

    # rest of the codes to compute market share will be completed here
    # code to calculate fair market share has to be included in this.

    if ppln_for_market_size:
        # The code in here will compile the data from database. Since data isnt available
        # the code will be updated once relevant data is published in the respective collections
        return 1
    else:
        return 0









