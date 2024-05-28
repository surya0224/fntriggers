"""
File Name              :   market_share_schedule_perspective_DAL
Author                 :   Ashwin Kumar
Date Created           :   2016-12-19
Description            :   RnA analysis for increasing/decreasing market share from a competition capacity perspective.

MODIFICATIONS LOG         :
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :
"""

import jupiter_AI.RnA.common_RnA_functions as cf

def get_host_competiton_capacity_related_data(dict_scr_filter):
    """
       Build the appropriate pipeline and hit the dB to get the relevant data
       for building RnA
       :param dict_scr_filter:
       :return:
    """

    qry_schedule = cf.build_query_schedule_col(dict_scr_filter)
    get_date_range = cf.enumerate_dates(dict_scr_filter['fromDate'],dict_scr_filter['toDate'])
    ppln_for_flight_frequency = []
    #   Creation of the pipeline for the mongodB aggregate
    # ppln_for_flight_frequency = \
    #     [
    #        ]
    # The above code is incomplete. The above code will be completed once we get the competitor data
    # for their respective schedules.

    if ppln_for_flight_frequency:
        # The code in here will compile the data from database. Since data isnt available
        # the code will be updated once relevant data is published in the respective collections
        return 1
    else:
        return 0


