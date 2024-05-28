"""
File Name              :    JUP_AI_Batch_Tiles
Author                 :    K Sai Krishna
Date Created           :    2016-12-15
Description            :    This particular code will calculate all the tiles for a particular
                            combinations in Cumulative Collection
Status                 :

MODIFICATIONS LOG          :
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :
"""

import datetime
import inspect
import json
from collections import defaultdict
from copy import deepcopy
from datetime import date
from datetime import timedelta

import jupiter_AI.common.ClassErrorObject as errorClass
from jupiter_AI import JUPITER_LOGGER
from jupiter_AI.logutils import measure


response = dict()


#   ************************************************** DAL ************************************************************


@measure(JUPITER_LOGGER)
def get_module_name():
    """
    Function used to get the module name where it is called
    """
    return inspect.stack()[1][3]



@measure(JUPITER_LOGGER)
def get_arg_lists(frame):
    """
    function used to get the list of arguments of the function
    where it is called
    """
    args, _, _, values = inspect.getargvalues(frame)
    argument_name_list = []
    argument_value_list = []
    for k in args:
        argument_name_list.append(k)
        argument_value_list.append(values[k])
    return argument_name_list, argument_value_list


@measure(JUPITER_LOGGER)
def get_combinations():
    """
    Queries the JUP_DB_Cummulative_Booking_DepDate and gets the exhaustive list of combinations of
    region, country, pos, compartment, channel
    :return:
    """
    ppln_combinations = [
        {
            '$group': {
                '_id': {
                    'region': '$region',
                    'country': '$country',
                    'pos': '$pos',
                    'compartment': '$compartment',
                    'channel': '$channel'
                }
            }
        }
        ,
        {
            '$project': {
                '_id': 0,
                'region': '$_id.region',
                'country': '$_id.country',
                'pos': '$_id.pos',
                'compartment': '$_id.compartment',
                'channel': '$_id.channel'
            }
        }
    ]

    combinations_crsr = db.JUP_DB_Cumulative_Dep_Date.aggregate(ppln_combinations)
    combinations_data = list(combinations_crsr)
    print len(combinations_data)
    return combinations_data


@measure(JUPITER_LOGGER)
def gen_filter_scr(region, country, pos, compartment, channel, screen_name):
    """
    This particular function takes the keys from the cumulative collection and convert them into a filter
     that can be used to call individual tiles values
    :param region:
    :param country:
    :param pos:
    :param compartment:
    :param channel:
    :return:
    """
    lst_filters = []
    today = date.today()
    temp_filter = dict()
    lst = [region, country, pos, compartment, channel]
    lst_keys = ['region', 'country', 'pos', 'compartment', 'channel']
    date_filter = [{'fromDate': str(date.today()),
                    'toDate': str(date.today())},
                   {'fromDate': str(date.today()),
                    'toDate': str(date.today() + timedelta(days=7))},
                   {'fromDate': str(date.today()),
                    'toDate': str(date.today() + timedelta(days=30))}]
    for d_filter in date_filter:
        for idx, item in enumerate(lst):
            if item:
                temp_filter[lst_keys[idx]] = [item]
            else:
                temp_filter[lst_keys[idx]] = []
        # crsr_dates_scr = db.JUP_DB_Date_Filter_Config.find({'screen': 'screen_name'})
        # if crsr_dates_scr.count() == 1:
        #     temp_filter['fromDate'] = crsr_dates_scr[0]['fromDate']
        #     temp_filter['toDate'] = crsr_dates_scr[0]['toDate']
        # elif crsr_dates_scr.count() > 1:
        #     inappropriate_data_error = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
        #                                                       get_module_name(),
        #                                                       get_arg_lists(inspect.currentframe()))
        #     inappropriate_data_error_desc = 'Expected 1 document but obtained more from Database'
        #     raise inappropriate_data_error
        # else:
        #     no_data_error = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
        #                                            get_module_name(),
        #                                            get_arg_lists(inspect.currentframe()))
        temp_filter['fromDate'] = d_filter['fromDate']
        temp_filter['toDate'] = d_filter['toDate']
        lst_filters.append(temp_filter)

    return lst_filters


#   ************************************************** BLL ************************************************************


@measure(JUPITER_LOGGER)
def get_bookings_today_tiles(combination):
    """

    :param combination: dict of the form {'region':,
                                          'country':,
                                          'pos':,
                                          'compartment':,
                                          'channel':,}
    :return:
    """
    """
       Bookings Today
    """
    try:
        temp_filter = deepcopy(gen_filter_scr(combination['region'],
                                              combination['country'],
                                              combination['pos'],
                                              combination['compartment'],
                                              combination['channel'],
                                              'bookings_today'))
        """
            bookings_today
                bookings_run_rate * CODE CHECK NEEDED
                bookings_strength * NO CODE
                bookings_profile - net_bookings / ticketed_bookings* NO-CODE
        """
    except errorClass.ErrorObject as error:
        print error.__str__()
        return None


@measure(JUPITER_LOGGER)
def get_sales_today_tiles(combination):
    try:
        temp_filter = deepcopy(gen_filter_scr(combination['region'],
                                              combination['country'],
                                              combination['pos'],
                                              combination['compartment'],
                                              combination['channel'],
                                              'sales_today'))
        """
            sales_today
                sales_runrate * NO CODE
                sales_strength * NO CODE
                revenue_profile * NO CODE
        """
    except errorClass.ErrorObject as error:
        print error.__str__()
        return None



@measure(JUPITER_LOGGER)
def get_market_dashboard_tiles(filter_comp_analysis_scr):
    """

    :param combination:
    :return:
    """
    try:
        """
            DONE
            Market
                dashboard
                    revenue/pax*
                    host_top_3_competitors_mrkt_shr*
                    deployed_capacity_vlyr*
                    mrkt_shr_rank/deployment_capacity_rnk*
        """
        from jupiter_AI.tiles.market.market_dashboard import get_tiles as mrkt_dhb
        return mrkt_dhb(filter_comp_analysis_scr)
    except errorClass.ErrorObject as error:
        print error.__str__()
        return None


@measure(JUPITER_LOGGER)
def get_market_barometer_tiles(combination):
    """

    :param combination:
    :return:
    """
    try:
        """
            mrkt_barometer
                total_agents*
                friends*
                foes*
                consolidators*
                corporates*
                deals*
                proximity_indicator*
        """
        temp_filter = deepcopy(gen_filter_scr(combination['region'],
                                              combination['country'],
                                              combination['pos'],
                                              combination['compartment'],
                                              combination['channel'],
                                              'market_barometer'))
        from jupiter_AI.tiles.market.market_barometer_scn import get_tiles as mrkt_barometer
        return mrkt_barometer(temp_filter)
    except errorClass.ErrorObject as error:
        print error.__str__()
        return None


@measure(JUPITER_LOGGER)
def get_market_outlook_tiles(combination):
    """

    :param combination:
    :return:
    """
    try:
        """
            mrkt_outlook
                declining*
                growing*
                mature*
                niche*
        """
        temp_filter = deepcopy(gen_filter_scr(combination['region'],
                                              combination['country'],
                                              combination['pos'],
                                              combination['compartment'],
                                              combination['channel'],
                                              'market_outlook'))
        from jupiter_AI.tiles.market.market_outlook_screen import outlook_market
        return outlook_market(temp_filter)
    except errorClass.ErrorObject as error:
        print error.__str__()


@measure(JUPITER_LOGGER)
def get_competitor_analysis_dashboard_tiles(filter_comp_analysis_scr):
    """

    :param combination:
    :return:
    """
    try:
        results = []
        """ DONE
            competitor_analysis
                dashboard
                    price_intelligence_quotient*
                    price_performance*
                    mrkt_leader/my_position*
                    revenue/vlyr*
        """
        from jupiter_AI.tiles.competitor_analysis.dashboard import get_tiles as comp_analysis_dhb
        return comp_analysis_dhb(filter_comp_analysis_scr)
    except errorClass.ErrorObject as error:
        print error.__str__()


@measure(JUPITER_LOGGER)
def get_competitor_analysis_product_indicator_tiles(filter_comp_analysis_scr):
    """

    :param combination:
    :return:
    """
    try:
        """
            product_indicator
                pre_booking*
                at_airport*
                in_flight*
                post_flight*
                competitor_coefficient*
        """
        from jupiter_AI.tiles.competitor_analysis.product_indicator import get_tiles as comp_analysis_product_indicator
        return comp_analysis_product_indicator(filter_comp_analysis_scr)
    except errorClass.ErrorObject as error:
        print error.__str__()


@measure(JUPITER_LOGGER)
def get_kpi_dashboard_tiles(combination):
    """

    :param combination:
    :return:
    """
    try:
        """
            KPI DashBoard
                total_fares*
                total_effective_fares*
                total_user_pax*
                total_host_pax*
        """
        temp_filter = deepcopy(gen_filter_scr(combination['region'],
                                              combination['country'],
                                              combination['pos'],
                                              combination['compartment'],
                                              combination['channel'],
                                              'kpi_dashboard'))
        from jupiter_AI.tiles.kpi.dashboard import get_tiles as kpi_dashboard
        return kpi_dashboard(temp_filter)
    except errorClass.ErrorObject as error:
        print error.__str__()


@measure(JUPITER_LOGGER)
def get_kpi_new_product(combination):
    """

    :param combination:
    :return:
    """
    try:
        response['kpi'] = {}
        """
            new_products
                no_of_products host*
                no of product user*
                competitors_product host*
                total_bookings host*
        """
        temp_filter = deepcopy(gen_filter_scr(combination['region'],
                                              combination['country'],
                                              combination['pos'],
                                              combination['compartment'],
                                              combination['channel'],
                                              'new_products'))
        from jupiter_AI.tiles.kpi.new_products import get_tiles as new_products
        return new_products(temp_filter)
    except errorClass.ErrorObject as error:
        print error.__str__()


@measure(JUPITER_LOGGER)
def get_kpi_piq(combination):
    """

    :param combination:
    :return:
    """
    try:
        """
            price_intelligence_quotient
                host_missing_fares(%)*
                competitors_only_fares(%)*
                host_only_fares(%)*
                variations_in_fare(num)*
        """
        temp_filter = deepcopy(gen_filter_scr(combination['region'],
                                              combination['country'],
                                              combination['pos'],
                                              combination['compartment'],
                                              combination['channel'],
                                              'price_intelligence_quotient'))
        from jupiter_AI.tiles.kpi.price_intelligence_quotient import get_tiles as piq_tiles
        return piq_tiles(temp_filter)
    except errorClass.ErrorObject as error:
        print error.__str__()



@measure(JUPITER_LOGGER)
def get_kpi_pai(combination):
    """

    :param combination:
    :return:
    """
    try:
        """
            price_agility_index(price_stability_index)
                psi_host*
                host_total_fares*
                analyst_psi*
                competitor_psi*
        """
        temp_filter = deepcopy(gen_filter_scr(combination['region'],
                                              combination['country'],
                                              combination['pos'],
                                              combination['compartment'],
                                              combination['channel'],
                                              'price_agility_index'))
        from jupiter_AI.tiles.kpi.price_agility_index import get_price_agility_index
        return get_price_agility_index(temp_filter)
    except errorClass.ErrorObject as error:
        print error.__str__()



@measure(JUPITER_LOGGER)
def get_kpi_revenue_split(combination):
    """

    :param combination:
    :return:
    """
    try:
        """
            revenue_split
                revenue_split(F/J/Y) host*
                revenue_split(F/J/Y) user*
                revenue_split(ADT/CHD/INF) host*
                revenue_split(ADT/CHD/INF) user*
        """
        temp_filter = deepcopy(gen_filter_scr(combination['region'],
                                              combination['country'],
                                              combination['pos'],
                                              combination['compartment'],
                                              combination['channel'],
                                              'revenue_split'))
        from jupiter_AI.tiles.kpi.revenue_split import get_tiles as rev_split
        return rev_split(temp_filter)
    except errorClass.ErrorObject as error:
        print error.__str__()



@measure(JUPITER_LOGGER)
def get_kpi_sig_non_sig_od(combination):
    """

    :param combination:
    :return:
    """
    try:
        """
            significant_non_significant_od
                sig_od/total_od host*
                sig_od/total_od user*
                user_revenue/host_revenue*
                booming_od/fading_od*
        """
        temp_filter = deepcopy(gen_filter_scr(combination['region'],
                                              combination['country'],
                                              combination['pos'],
                                              combination['compartment'],
                                              combination['channel'],
                                              'significant_non_significant_od'))
        from jupiter_AI.tiles.kpi.significant_non_significant_od import get_tiles as sig_non_sig
        return sig_non_sig(temp_filter)
    except errorClass.ErrorObject as error:
        print error.__str__()



@measure(JUPITER_LOGGER)
def get_kpi_yield_rasm_seatfactor(combination):
    """

    :param combination:
    :return:
    """
    try:
        """
            yield_rasm_seatfactor
                yield*
                rasm*
                seat_factor*
        """
        temp_filter = deepcopy(gen_filter_scr(combination['region'],
                                              combination['country'],
                                              combination['pos'],
                                              combination['compartment'],
                                              combination['channel'],
                                              'yield_rasm_seatfactor'))
        from jupiter_AI.tiles.kpi.yield_rasm_seatfactor import get_tiles as yield_rasm
        return yield_rasm(temp_filter)
    except errorClass.ErrorObject as error:
        print error.__str__()


@measure(JUPITER_LOGGER)
def get_kpi_effective_ineffective_fares(combination):
    """

    :param combination:
    :return:
    """
    try:
        """
            effective_ineffective_fares
                total_host_fares*
                num_effective_fares*
                total_host_tickets*
                total_user_tickets_sold*
        """
        temp_filter = deepcopy(gen_filter_scr(combination['region'],
                                              combination['country'],
                                              combination['pos'],
                                              combination['compartment'],
                                              combination['channel'],
                                              'yield_rasm_seatfactor'))
        from jupiter_AI.tiles.kpi.effective_ineffective_fares import get_tiles as effective_ineffective_fares_tiles
        return effective_ineffective_fares_tiles(temp_filter)
    except errorClass.ErrorObject as error:
        print error.__str__()


@measure(JUPITER_LOGGER)
def get_price_biometric_dhb_tiles(combination):
    """

    :param combination:
    :return:
    """
    """
        dashboard
        pe_signal *
        price_intelligence_quotient *
        price_stability *
        effective_fares( %) *
    """
    return None


@measure(JUPITER_LOGGER)
def get_price_biometrics_price_performance_tiles(combination):
    """

    :param combination:
    :return:
    """
    try:
        """
            price_performance
                revenue/vlyr host level
                revenue/vlyr user level
                pax/vlyr user level
                ms/vlyr user level
        """
        temp_filter = deepcopy(gen_filter_scr(combination['region'],
                                              combination['country'],
                                              combination['pos'],
                                              combination['compartment'],
                                              combination['channel'],
                                              'price_performance'))
        from jupiter_AI.tiles.price_biometrics.price_performance import get_tiles as price_performance_tiles
        return price_performance_tiles(temp_filter)
    except errorClass.ErrorObject as error:
        print error.__str__()


@measure(JUPITER_LOGGER)
def get_price_biometrics_price_quote_tiles(combination):
    """

    :param combination:
    :return:
    """
    try:
        """
            price_quote
                yield/vlyr
                tickets_sold/vlyr
                fare/vlyr
                ms/vlyr
        """
        temp_filter = deepcopy(gen_filter_scr(combination['region'],
                                              combination['country'],
                                              combination['pos'],
                                              combination['compartment'],
                                              combination['channel'],
                                              'price_quote'))
        from jupiter_AI.tiles.price_biometrics.price_quote import get_tiles as price_quote_tiles
        return price_quote_tiles(temp_filter)
    except errorClass.ErrorObject as error:
        print error.__str__()


@measure(JUPITER_LOGGER)
def get_price_biometrics_analyst_performance(combination):
    """
    :param combination:
    :return:
    """
    pass


@measure(JUPITER_LOGGER)
def get_exchange_rate_dashboard(combination):
    """

    :param combination:
    :return:
    """
    pass



@measure(JUPITER_LOGGER)
def get_distribution_customer_segment_dashboard(combination):
    """

    :param combination:
    :return:
    """
    pass


@measure(JUPITER_LOGGER)
def get_tiles_values_main():
    """
    Main Function that calculates all tiles for today
    and build the document.
    :return:
    """
    lst_combinations = get_combinations()
    for idx, combination in enumerate(lst_combinations):
        response = defaultdict(dict, combination)
        response['query_type'] = 'batch'
        response['stamp'] = str(datetime.datetime.now())

        #   Comp Analysis Dashboard
        filter_comp_dhb = gen_filter_scr(combination['region'],
                                         combination['country'],
                                         combination['pos'],
                                         combination['compartment'],
                                         combination['channel'],
                                         'competitor_analysis_dashboard')
        response['competitor_analysis'] = defaultdict(list)
        response['competitor_analysis']['dashboard'] = []
        for filt in filter_comp_dhb:
            response['competitor_analysis']['dashboard'].append({'date_filter': {'fromDate': filt['fromDate'],
                                                                                 'toDate': filt['toDate']},
                                                                 'tiles': get_competitor_analysis_dashboard_tiles(filt)}
                                                                )
        #   Comp Analysis Product Indicator
        response['competitor_analysis']['product_indicator'].append({
            'date_filter': None,
            'tiles': get_competitor_analysis_product_indicator_tiles('dummy')
        })

        #   Market Dashboard
        response['market'] = defaultdict(list)
        filter_mrkt_dhb = gen_filter_scr(combination['region'],
                                         combination['country'],
                                         combination['pos'],
                                         combination['compartment'],
                                         combination['channel'],
                                         'market_dashboard')
        response['market']['dashboard'] = []
        for filt in filter_mrkt_dhb:
            response['market']['dashboard'].append({
                'date_filter': {
                    'fromDate': filt['fromDate'],
                    'toDate': filt['toDate']
                },
                'tiles': get_market_dashboard_tiles(filt)
                }
            )

        filter_mrkt_outlook = gen_filter_scr(combination['region'],
                                             combination['country'],
                                             combination['pos'],
                                             combination['compartment'],
                                             combination['channel'],
                                             'market_dashboard')
        response['market']['outlook'] = []
        for filt in filter_mrkt_outlook:
            response['market']['outlook'].append({
                'date_filter': {
                    'fromDate': filt['fromDate'],
                    'toDate': filt['toDate']
                },
                'tiles': get_market_outlook_tiles(filt)
            }
            )

        print json.dumps(response, indent=1)
        db.JUP_DB_KPI_Tiles.insert_one(response)

        """
        #   KPI MODULE
        response['KPI'] = defaultdict(dict)
        response['KPI']['dashboard'] = get_kpi_dashboard_tiles(combination)
        response['KPI']['significant_non_significant_od'] = get_kpi_sig_non_sig_od(combination)
        response['KPI']['revenue_split'] = get_kpi_revenue_split(combination)
        response['KPI']['new_product'] = get_kpi_new_product(combination)
        response['KPI']['yield_rasm_seat_factor'] = get_kpi_yield_rasm_seatfactor(combination)
        response['KPI']['price_intelligence_quotient'] = get_kpi_piq(combination)
        response['KPI']['price_agility_index'] = get_kpi_pai(combination)
        response['KPI']['effective_ineffective_fares'] = get_kpi_effective_ineffective_fares(combination)
        """
        #   COMPETITOR ANALYSIS MODULE
        # response['combination'] = combination
        # response['competitor_analysis'] = defaultdict(dict)
        # response['competitor_analysis']['dashboard'] = get_competitor_analysis_dashboard_tiles(combination)
        # response['competitor_analysis']['product_indicator'] =\
        #     get_competitor_analysis_product_indicator_tiles(combination)
        # print response
        # db.JUP_DB_KPI_Tiles.insert_one(response)
        """
        #   MARKET MODULE
        response['market'] = defaultdict(dict)
        response['market']['dashboard'] = get_market_dashboard_tiles(combination)
        response['market']['outlook'] = get_market_outlook_tiles(combination)
        response['market']['barometer'] = get_market_barometer_tiles(combination)

        #   PRICE BIOMETRICS MODULE
        response['price_biometrics'] = defaultdict(dict)
        response['price_biometrics']['dashboard'] = get_price_biometric_dhb_tiles(combination)
        response['price_biometrics']['price_performance'] = get_price_biometrics_price_performance_tiles(combination)
        response['price_biometrics']['price_quote'] = get_price_biometrics_price_quote_tiles(combination)
        response['price_biometrics']['analyst_performance'] = get_price_biometrics_analyst_performance(combination)

        #   EXCHANGE RATE MODULE
        response['exchange_rate_dashboard'] = defaultdict(dict)

        #   DISTRIBUTION and CUSTOMER SEGMENTS
        response['distribution_and_customer_segments'] = defaultdict(dict)
        """
        """
                analyst_performance
                    price_availability/vlyr*
                    price_stability/vlyr*
                    pe_signal/vtgt*
        """
        """
            distributor_customer_segment
                indirect/direct_revenue*
                frequent_flier_revenue*
                customer_segment*
                distributors*
            exchange_rate
                avg_currency_realization*
                no_of_fares_impacted_by_currency_fluctuation*
                current_mrkt_share/current_revenue*
                yq_recovery/target_%/target*
            performance_dashboard
        """

if __name__ == '__main__':
    get_tiles_values_main()
