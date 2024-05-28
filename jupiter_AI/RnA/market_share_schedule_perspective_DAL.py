"""
File Name              :   market_share_schedule_perspective_DAL
Author                 :   Ashwin Kumar
Date Created           :   2016-12-16
Description            :   RnA analysis for increasing/decreasing market share from a price perspective.

MODIFICATIONS LOG         :
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :
"""

import jupiter_AI.RnA.common_RnA_functions as cf

def get_schedule_related_data(dict_scr_filter):
    """
       Build the appropriate pipeline and hit the dB to get the relevant data
       for building RnA
       :param dict_scr_filter:
       :return:
    """

    qry_schedule = cf.build_query_schedule_col(dict_scr_filter)
    # remove compartment from qry_schedule since the number of flights per week or number of flights per day
    # isn't compartment related
    del qry_schedule['compartment']
    get_date_range = cf.enumerate_dates(dict_scr_filter['fromDate'],dict_scr_filter['toDate'])
    ppln_for_flight_frequency = []
    #   Creation of the pipeline for the mongodB aggregate
    # ppln_for_flight_frequency = \
    #     [
    #         {
    #             '$match': qry_schedule
    #         }
    #         ,
    #         {
    #             '$group':
    #                 {
    #                     '_id':
    #                         {
    #                             'airline':'$airline',
    #                             'od':'$od',
    #                             'effective_from': '$effective_from',
    #                             'effective_to': '$effective_to',
    #                             'frequency': '$frequency'
    #
    #                         },
    #                     'capacity': {'$sum':'$capacity'}
    #                 }
    #         }
    #         ,
    #         {
    #             '$project':
    #                 {
    #                     '_id':0,
    #                     'airline': '$airline',
    #                     'od': '$od',
    #                     'effective_from': '$effective_from',
    #                     'effective_to': '$effective_to',
    #                     'frequency': '$frequency',
    #                     'capacity': '$capacity',
    #                     'date_range': get_date_range
    #                 }
    #         }
    #         ,
    #         {
    #             '$unwind':'$date_range'
    #         }
    #         ,
    #         {
    #             '$redact':
    #                 {
    #                     '$cond':
    #                         {
    #                             'if':
    #                                 {
    #                                     '$and': [{'$gte':['$date_range','$effective_from']},
    #                                              {'$lte':['$date_range','$effective_to']}]
    #                                 },
    #                             'then':'$$DESCEND',
    #                             'else':'$$PRUNE'
    #                         }
    #                 }
    #         }
    #         ,
    #         {
    #             '$project':
    #                 {
    #                     '_id': 0,
    #                     'airline': '$airline',
    #                     'od': '$od',
    #                     'effective_from': '$effective_from',
    #                     'effective_to': '$effective_to',
    #                     'frequency': '$frequency',
    #                     'capacity': '$capacity',
    #                     'date_filter': '$date_range'
    #                 }
    #         }
    #         ,
    #         {
    #             '$project':
    #                 {
    #                     '_id': 0,
    #                     'airline': '$airline',
    #                     'od': '$od',
    #                     'effective_from': '$effective_from',
    #                     'effective_to': '$effective_to',
    #                     'frequency': '$frequency',
    #                     'capacity': '$capacity',
    #                     'date_filter': '$date_filter',
    #                     'day_date_filter': {'$isoDayOfWeek':'$date_filter'},
    #
    #                 }
    #         }
    #         ,
    #         {
    #             '$project':
    #                 {
    #                     '_id': 0,
    #                     'airline': '$airline',
    #                     'od': '$od',
    #                     'effective_from': '$effective_from',
    #                     'effective_to': '$effective_to',
    #                     'frequency': '$frequency',
    #                     'capacity': '$capacity',
    #                     'date_filter': '$date_filter',
    #                     'day_date_filter': {'$substr': ['$day_date_filter', 0, 1]},
    #                 }
    #         }
    #         ,
    #         {
    #             '$project':
    #                 {
    #                     '_id': 0,
    #                     'airline': '$airline',
    #                     'od': '$od',
    #                     'effective_from': '$effective_from',
    #                     'effective_to': '$effective_to',
    #                     'frequency': '$frequency',
    #                     'capacity': '$capacity',
    #                     'date_filter': '$date_filter',
    #                     'day_date_filter':'$day_date_filter',
    #                     'frequency_list':
    #                         {
    #                             '$split':
    #                                 [
    #                                     '$frequency',
    #                                     ''
    #                                 ]
    #                         }
    #                 }
    #         }
    #
    # ]
    # The above code is incomplete. The above code will be completed once we get the competitor data
    # for their respective schedules.

    if ppln_for_flight_frequency:
        # The code in here will compile the data from database. Since data isnt available
        # the code will be updated once relevant data is published in the respective collections
        return 1
    else:
        return 0


