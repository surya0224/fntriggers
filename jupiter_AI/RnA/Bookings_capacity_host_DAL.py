
"""
File Name              :   Bookings_capacity_host_DAL
Author                 :   Pavansimha Reddy
Date Created           :   2016-12-19
Description            :   RnA analysis for bookings with capacity increasing/decreasing with respect to host and competitor
Data access layer

MODIFICATIONS LOG          :
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :
"""

import jupiter_AI.RnA.common_RnA_functions as cf

def get_bookings_capacity_rna_data(dict_scr_filter):
    """
       Build the appropriate pipeline and hit the dB to get the relevant data
       for building RnA
       :param dict_scr_filter:
       :return:
    """
    qry_schedule = cf.build_query_schedule_col(dict_scr_filter)
    ppln_for_bookings_capacity_rna = []

    #ppln_for_bookings_capacity_rna = [
    #
    # ]

    # rest of the codes to compute bookings with respect to capacities will be completed here
    # code to calculate fair market share has to be included in this.

    # results of the query will be stored in a list
    result = list()
    print type(result)

    return result









