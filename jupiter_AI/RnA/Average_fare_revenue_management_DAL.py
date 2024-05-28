
"""
File Name              :   Average_fare_revenue_management_DAL
Author                 :   Pavansimha Reddy
Date Created           :   2016-12-19
Description            :   Average fare will keep falling
Data access layer

MODIFICATIONS LOG          :
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :
"""

import jupiter_AI.RnA.common_RnA_functions as cf

def get_average_fare_revenue_management_rna_data(dict_scr_filter):
    """
       Build the appropriate pipeline and hit the dB to get the relevant data
       for building RnA
       :param dict_scr_filter:
       :return:
    """
    qry_schedule = cf.build_query_schedule_col(dict_scr_filter)
    ppln_for_average_fare_revenue_management = []

    # ppln_for_average_fare_revenue_management = [
    #
    # ]

    result = list()
    print type(result)

    return result








