
"""
File Name              :   Average_fare_price_DAL
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

def get_average_fare_price_rna_data(dict_scr_filter):
    """
       Build the appropriate pipeline and hit the dB to get the relevant data
       for building RnA
       :param dict_scr_filter:
       :return:
    """
    qry_schedule = cf.build_query_schedule_col(dict_scr_filter)
    ppln_for_average_fare_price = []

    # ppln_for_average_fare_price = [
    #
    # ]

    # rest of the codes to compute runrate will be completed here
    # code to calculate fair market share has to be included in this.

    # results of the query will be stored in a list
    result = list()
    print type(result)

    return result








