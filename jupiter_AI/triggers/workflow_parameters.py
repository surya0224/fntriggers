"""
This code takes the trigger obj as input and obtains all the relevant parameters for workflow collection

ONLY HOST PARAMS -
bookings_data(compartment level)
    bookings
    vlyr
    vtgt
pax_data(compartment level)
    pax
    vlyr
    vtgt
revenue_data(compartment level AED)
    revenue
    vlyr
    vtgt
yield_data(In fills/passenger km )(compartment level)
    yield
    vlyr
    vtgt

yield_data(farebasis level)
    yield
    vlyr
avg_fare_data(compartment level but which currency it should be in filed currency)
    avg_fare
    vlyr
    vtgt
seat_factor_leg1
seat_factor_leg2(if for an OD)

HOST COMP PARAMS -
get top5 competitors

market share
market share vlyr
market share vtgt
fair market share

lowest filed fares
    base fare
    yq
    surcharges
    tax
    frequency
most frequently available lowest fare
    base fare
    yq
    surcharges
    tax
    frequency
"""
import datetime
from collections import defaultdict
from copy import deepcopy
from jupiter_AI import client, today
from jupiter_AI.common.convert_currency import convert_currency
from jupiter_AI.batch.JUP_AI_Batch_Top5_Competitors import obtain_top_5_comp
from jupiter_AI.common.calculate_bookings import get_bookings_vlyr_vtgt
from jupiter_AI.common.calculate_market_share import calculate_market_share
from jupiter_AI.network_level_params import SYSTEM_DATE, Host_Airline_Hub, JUPITER_DB, query_month_year_builder, Host_Airline_Code
from jupiter_AI.triggers.CategorizeRecommendation import Availability
from jupiter_AI.triggers.GetInfareFare import get_valid_infare_fare
from jupiter_AI.triggers.common import get_ratings_details, get_price_movement
from jupiter_AI.triggers.data_level.MainClass import DataLevel
#db = client[JUPITER_DB]

def get_lowest_filed_fare(airline,
                          pos,
                          origin,
                          destination,
                          compartment,
                          oneway_return,
                          currency,
                          sale_date_start,
                          sale_date_end,
                          dep_date_start,
                          dep_date_end):
    """
    :param pos:
    :param origin:
    :param destination:
    :param compartment:
    :param dep_date_start:
    :param dep_date_end:
    :return:
    """
    # response = dict(base_fare='NA',
    #                 yq='NA',
    #                 tax='NA',
    #                 surcharge='NA',
    #                 total_fare='NA')
    #
    # qry_atpco_fares = defaultdict(list)
    #
    # qry_atpco_fares['$and'].append({'airline': airline})
    # qry_atpco_fares['$and'].append({'origin': origin})
    # qry_atpco_fares['$and'].append({'destination': destination})
    # qry_atpco_fares['$and'].append({'compartment': compartment})
    # qry_atpco_fares['$and'].append({'oneway_return': oneway_return})
    # qry_atpco_fares['$and'].append({'currency': currency})
    # # if sale_date_end and sale_date_start:
    # #     qry_atpco_fares['$and'].append(
    # #         {
    # #             '$or':
    # #                 [
    # #                     {
    # #                         '$and':
    # #                             [
    # #                                 {'$eq': ['sale_date_start', None]},
    # #                                 {'$eq': ['sale_date_end', None]}
    # #                             ]
    # #                     }
    # #                     ,
    # #                     {
    # #                         '$and':
    # #                             [
    # #                                 {'$gte': ['sale_date_end',sale_date_start]},
    # #                                 {'$lte': ['sale_date_start',sale_date_end]}
    # #                             ]
    # #                     }
    # #                 ]
    # #         }
    # #     )
    # # elif sale_date_end:
    # #     qry_atpco_fares['$and'].append(
    # #         {
    # #             '$or':
    # #                 [
    # #                     {
    # #                         '$and':
    # #                             [
    # #                                 # {'$eq': ['sale_date_start', None]}
    # #                                 # ,
    # #                                 # {'$eq': ['sale_date_end', None]}
    # #                             ]
    # #                     }
    # #                     ,
    # #                     {
    # #                         '$and':
    # #                             [
    # #                                 {'$gte': ['sale_date_end', sale_date_start]},
    # #                                 {'$lte': ['sale_date_start', sale_date_end]}
    # #                             ]
    # #                     }
    # #                 ]
    # #         }
    # #     )
    # #   sale date
    # #   dep date
    # #   effective date
    #
    # # lowest_filed_fare_crsr = db.JUP_DB_ATPCO_Fares.aggregate(
    # #     [
    # #         {
    # #             '$match': qry_atpco_fares
    # #         }
    # #         ,
    # #         {
    # #             '$sort':
    # #                 {
    # #                     'fare': 1
    # #                 }
    # #         }
    # #         ,
    # #         {
    # #             '$limit': 1
    # #         }
    # #     ]
    # # )
    # # lowest_filed_fare_data = list(lowest_filed_fare_crsr)
    # #
    # # if len(lowest_filed_fare_data) == 1:
    # #     response['base_fare'] = lowest_filed_fare_data[0]['base_fare']
    # #     response['total_fare'] = lowest_filed_fare_data[0]['total_fare']
    # #     response['yq'] = lowest_filed_fare_data[0]['yq']
    # #     response['surcharge'] = lowest_filed_fare_data[0]['surcharge']
    lowest_filed_fare = get_price_movement(airline=airline,
                                           pos=pos,
                                           origin=origin,
                                           destination=destination,
                                           compartment=compartment,
                                           currency=currency,
                                           dep_date_start=dep_date_start,
                                           dep_date_end=dep_date_end,
                                           oneway_return=oneway_return,
                                           source='ATPCO')['lowest_fare']
    return lowest_filed_fare


def update_host_workflow_params_comp(trigger_obj):
    """
    :param class_obj:
    :return:
    """
    import time
    st = time.time()
    market_combination_data = {
        'pos': trigger_obj['pos'],
        'origin': trigger_obj['origin'],
        'destination': trigger_obj['destination'],
        'compartment': trigger_obj['compartment']
    }
    book_start_date = str(today.year)+'-01-01'
    book_end_date = SYSTEM_DATE
    dep_date_start = trigger_obj['triggering_data']['dep_date_start']
    dep_date_end = trigger_obj['triggering_data']['dep_date_end']
    #  Trigger_Id
    # trigger_obj['trigger_id = generate_trigger_id(trigger_name = trigger_obj['trigger_type)

    #  Bookings Data
    #   Bookings Abs
    #   VLYR
    #   VTGT

    trigger_obj['bookings_data'] = get_bookings_vlyr_vtgt(pos=trigger_obj['pos'],
                                                          origin=trigger_obj['origin'],
                                                          destination=trigger_obj['destination'],
                                                          compartment=trigger_obj['compartment'],
                                                          dep_date_from=dep_date_start,
                                                          dep_date_to=dep_date_end)
    print 'BOOKINGS', trigger_obj['bookings_data']
    print '**************************For BOOKINGS - ', time.time() - st

    #  Pax Data
    #   Pax Abs
    #   VLYR
    #   VTGT
    class_obj = DataLevel(data=market_combination_data,
                          system_date=SYSTEM_DATE)
    pax_data = class_obj.get_sales_flown_data(book_date_start=book_start_date,
                                              book_date_end=book_end_date,
                                              dep_date_start=dep_date_start,
                                              dep_date_end=dep_date_end,
                                              parameter='pax')

    print 'PAX DATA', pax_data
    trigger_obj['pax_data'] = dict(pax='NA',
                                   vlyr='NA',
                                   vtgt='NA')

    if type(pax_data['ty']) in [int, float] and pax_data['ty'] >= 0:
        trigger_obj['pax_data']['pax'] = pax_data['ty']
        if type(pax_data['ly']) in [int, float] and pax_data['ly'] > 0:
            trigger_obj['pax_data']['vlyr'] = (pax_data['ty'] - pax_data['ly'])*100/float(pax_data['ly'])

    forecast_pax_val = class_obj.get_forecast_data(book_date_start=book_start_date,
                                                   book_date_end=book_end_date,
                                                   dep_date_start=dep_date_start,
                                                   dep_date_end=dep_date_end,
                                                   parameter='pax')
    print 'FORECAST PAX ', forecast_pax_val

    target_pax_val = class_obj.get_target_data(dep_date_start=dep_date_start,
                                               dep_date_end=dep_date_end,
                                               parameter='pax')
    print 'TARGET PAX ', target_pax_val

    if type(forecast_pax_val) in [int, float] and forecast_pax_val>0:
        if type(target_pax_val) in [int, float] and target_pax_val>0:
            trigger_obj['pax_data']['vtgt'] = (forecast_pax_val - target_pax_val)*100/float(target_pax_val)

    print '**************************For PAX DATA - ', time.time() - st

    trigger_obj['revenue_data'] = dict(revenue='NA',
                                       vlyr='NA',
                                       vtgt='NA')

    #  Revenue Data
    #   Revenue Abs
    #   VLYR
    #   VTGT

    rev_data = class_obj.get_sales_flown_data(book_date_start=book_start_date,
                                              book_date_end=book_end_date,
                                              dep_date_start=dep_date_start,
                                              dep_date_end=dep_date_end,
                                              parameter='revenue')
    print 'REVENUE DATA', rev_data

    if rev_data['ty'] >= 0:
        trigger_obj['revenue_data']['revenue'] = rev_data['ty']

        if rev_data['ly'] > 0:
            trigger_obj['revenue_data']['vlyr'] = (rev_data['ty'] - rev_data['ly'])*100/float(rev_data['ly'])

    forecast_rev_val = class_obj.get_forecast_data(book_date_start=book_start_date,
                                                   book_date_end=book_end_date,
                                                   dep_date_start=dep_date_start,
                                                   dep_date_end=dep_date_end,
                                                   parameter='revenue')
    print 'FORECAST REV', forecast_rev_val

    target_rev_val = class_obj.get_target_data(dep_date_start=dep_date_start,
                                               dep_date_end=dep_date_end,
                                               parameter='revenue')

    print 'TARGET REV', target_rev_val

    if type(forecast_rev_val) in [int, float] and forecast_rev_val>0:
        if type(target_rev_val) in [int, float] and target_rev_val>0:
            trigger_obj['revenue_data']['vtgt'] = (forecast_rev_val - target_rev_val)*100/float(target_rev_val)

    print '**************************For REVENUE - ', time.time() - st

    trigger_obj['yield_data_compartment'] = dict(yield_='NA',
                                                 vlyr='NA',
                                                 vtgt='NA')

    trigger_obj['avg_fare_data'] = dict(avg_fare='NA',
                                        vlyr='NA',
                                        vtgt='NA')

    #   Calculation of OD distance
    od_distance = class_obj.get_od_distance(origin=trigger_obj['origin'],
                                            destination=trigger_obj['destination'])
    print 'OD DISTANCE', od_distance

    #  Yield Data
    #   Yield Abs
    #   VLYR
    #   VTGT

    #  Avg Fare Data
    #   Avg Fare Abs
    #   VLYR
    #   VTGT

    if pax_data['ty'] > 0 and rev_data['ty'] >= 0 and od_distance > 0:
        trigger_obj['avg_fare_data']['avg_fare'] = float(rev_data['ty']) / pax_data['ty']
        trigger_obj['yield_data_compartment']['yield_'] = float(rev_data['ty']) * 100 / (pax_data['ty'] * od_distance)
        if pax_data['ly'] > 0 and rev_data['ly'] >= 0 and od_distance > 0:
            avg_fare_ly = float(rev_data['ly']) / pax_data['ly']
            yield_ly = float(rev_data['ly']) / (pax_data['ly'] * od_distance)
            if avg_fare_ly > 0:
                trigger_obj['avg_fare_data']['vlyr'] = (float(trigger_obj['avg_fare_data']['avg_fare']) - avg_fare_ly)*100 / avg_fare_ly
            yield_ly = float(rev_data['ly']) / (pax_data['ly'] * od_distance)
            if yield_ly > 0:
                trigger_obj['yield_data_compartment']['vlyr'] = (float(trigger_obj['yield_data_compartment']['yield_']) - yield_ly) * 100

    if forecast_pax_val > 0 and forecast_rev_val > 0 and target_pax_val > 0 and target_rev_val > 0 and od_distance > 0:
        forecast_avg_fare = float(forecast_rev_val)/forecast_pax_val
        target_avg_fare = float(target_rev_val)/target_pax_val
        forecast_yield = forecast_rev_val*100/float(forecast_pax_val*od_distance)
        target_yield = target_rev_val*100/float(target_pax_val*od_distance)
        print '***********************Avg Fare/Yield', forecast_avg_fare, target_avg_fare, forecast_yield, target_yield

        trigger_obj['yield_data_compartment']['vtgt'] = (forecast_yield - target_yield)*100/target_yield
        trigger_obj['avg_fare_data']['vtgt'] = (forecast_avg_fare - target_avg_fare)*100/target_avg_fare

    print '**************************For YIELD AND AVGFARE - ', time.time() - st

    trigger_obj['seat_factor'] = dict(leg1='NA',
                                      leg2='NA')
    class_obj_2 = Availability(recommendation=trigger_obj)
    print class_obj_2.reco
    if class_obj_2.if_leg():
        class_obj_2.do_analysis()
        if class_obj_2.availability_perc != 'NA':
            trigger_obj['seat_factor']['leg1'] = 100 - class_obj_2.availability_perc
    else:
        temp_reco_object_1 = deepcopy(trigger_obj)
        temp_reco_object_1['destination'] = Host_Airline_Hub
        leg_1_class_obj = Availability(temp_reco_object_1)
        leg_1_class_obj.do_analysis()
        if leg_1_class_obj.availability_perc != 'NA':
            trigger_obj['seat_factor']['leg1'] = 100 - leg_1_class_obj.availability_perc
        temp_reco_object_2 = deepcopy(trigger_obj)
        temp_reco_object_2['origin'] = Host_Airline_Hub
        leg_2_class_obj = Availability(temp_reco_object_2)
        leg_2_class_obj.do_analysis()
        if leg_2_class_obj.availability_perc != 'NA':
            trigger_obj['seat_factor']['leg2'] = 100 - leg_2_class_obj.availability_perc

    print '**************************For SEATFACTOR - ', time.time() - st
    # if trigger_obj['avg_fare_data['avg_fare'] != 'NA':
    #     trigger_obj['avg_fare_data['avg_fare'] = convert_currency(value=trigger_obj['avg_fare_data['avg_fare'],
    #                                                              from_code='AED',
    #                                                              to_code=trigger_obj['host_pricing_data['currency'])

    # if trigger_obj['revenue_data['revenue'] != 'NA':
    #     trigger_obj['revenue_data['revenue'] = convert_currency(value=trigger_obj['revenue_data['revenue'],
    #                                                            from_code='AED',
    #                                                            to_code=trigger_obj['host_pricing_data['currency'])

    return trigger_obj


def update_host_workflow_params_fb(trigger_obj):
    """
    :param trigger_obj:
    :return:
    """
    market_combination_data = {
        'pos': trigger_obj['pos'],
        'origin': trigger_obj['origin'],
        'destination': trigger_obj['destination'],
        'compartment': trigger_obj['compartment']
    }
    book_start_date = str(today.year)+'-01-01'
    book_end_date = SYSTEM_DATE
    dep_date_start = trigger_obj['triggering_data']['dep_date_start']
    dep_date_end = trigger_obj['triggering_data']['dep_date_end']

    #   Calculation of OD distance
    od_distance = DataLevel.get_od_distance(origin=trigger_obj['origin'],
                                            destination=trigger_obj['destination'])
    print 'OD DISTANCE', od_distance

    trigger_obj['yield_data_fb'] = dict(yield_='NA',
                                        vlyr='NA'
                                        )

    class_obj = DataLevel(data=market_combination_data,
                          system_date=SYSTEM_DATE)

    fb_level_query = dict()
    if market_combination_data['pos']:
        fb_level_query['pos'] = market_combination_data['pos']
    if market_combination_data['origin']:
        fb_level_query['origin'] = market_combination_data['origin']
    if market_combination_data['destination']:
        fb_level_query['destination'] = market_combination_data['destination']
    if market_combination_data['compartment']:
        fb_level_query['compartment'] = market_combination_data['compartment']
    fb_level_query.update({
        'fare_basis': trigger_obj['host_pricing_data']['fare_basis']
    })
    rev_data_fb_level = class_obj.get_sales_flown_data(book_date_start=book_start_date,
                                                       book_date_end=book_end_date,
                                                       dep_date_start=dep_date_start,
                                                       dep_date_end=dep_date_end,
                                                       parameter='revenue',
                                                       query=fb_level_query)
    pax_data_fb_level = class_obj.get_sales_flown_data(book_date_start=book_start_date,
                                                       book_date_end=book_end_date,
                                                       dep_date_start=dep_date_start,
                                                       dep_date_end=dep_date_end,
                                                       parameter='pax',
                                                       query=fb_level_query)

    if pax_data_fb_level['ty'] > 0 and rev_data_fb_level['ty'] >= 0 and od_distance > 0:
        trigger_obj['yield_data_fb']['yield_'] = float(rev_data_fb_level['ty'])*100 / (
        pax_data_fb_level['ty'] * od_distance)

        if pax_data_fb_level['ly'] > 0 and rev_data_fb_level['ly'] >= 0 and od_distance > 0:
            yield_ly_fb = float(rev_data_fb_level['ly'])*100 / (pax_data_fb_level['ly'] * od_distance)
            if yield_ly_fb > 0:
                trigger_obj['yield_data_fb']['vlyr'] = (float(
                    trigger_obj['yield_data_fb']['yield_']) - yield_ly_fb) * 100 / yield_ly_fb

    trigger_obj['reco_yield'] = 'NA'
    if od_distance > 0 and trigger_obj['price_recommendation']:
        reco_price = convert_currency(trigger_obj['price_recommendation'], from_code=trigger_obj['currency'], to_code='AED')
        trigger_obj['reco_yield'] = reco_price*100 / float(od_distance)

    # Farebasis Level Bookings Data
    fb_level_bkng_qry = deepcopy(fb_level_query)
    fb_level_bkng_qry['farebasis'] = fb_level_query['fare_basis']
    bookings_data_fb_level = class_obj.get_bookings_data(book_date_start=book_start_date,
                                                         book_date_end=book_end_date,
                                                         dep_date_start=dep_date_start,
                                                         dep_date_end=dep_date_end,
                                                         query=fb_level_query)

    trigger_obj['bookings_val_fb'] = bookings_data_fb_level['ty']

    #  Seat Factor Data
    #   If leg only SF_leg1
    #   If OD SF_leg1 and SF_leg2 are calculated
    #
    # trigger_obj['seat_factor'] = dict(leg1='NA',
    #                                   leg2='NA')
    trigger_obj['flight_data'] = dict(leg1=dict(outbound='NA',
                                                inbound='NA'),
                                      leg2=dict(outbound='NA',
                                                inbound='NA'))
    class_obj_2 = Availability(recommendation=trigger_obj)
    print class_obj_2.reco
    if class_obj_2.if_leg():
        class_obj_2.do_analysis()
        # trigger_obj['seat_factor']['leg1'] = 100 - class_obj_2.availability_perc
        trigger_obj['flight_data']['leg1']['outbound'] = get_flight_details(leg_origin=trigger_obj['origin'],
                                                                            leg_destination=trigger_obj[
                                                                                'destination'],
                                                                            compartment=trigger_obj['compartment'],
                                                                            dep_date_start=dep_date_start,
                                                                            dep_date_end=dep_date_end)
        if trigger_obj['host_pricing_data']['oneway_return'] == 2:
            trigger_obj['flight_data']['leg1']['inbound'] = get_flight_details(
                leg_origin=trigger_obj['destination'],
                leg_destination=trigger_obj['origin'],
                compartment=trigger_obj['compartment'],
                dep_date_start=dep_date_start,
                dep_date_end=dep_date_end)
    else:
        temp_reco_object_1 = deepcopy(trigger_obj)
        temp_reco_object_1['destination'] = Host_Airline_Hub
        leg_1_class_obj = Availability(temp_reco_object_1)
        leg_1_class_obj.do_analysis()
        # trigger_obj['seat_factor']['leg1'] = 100 - leg_1_class_obj.availability_perc
        trigger_obj['flight_data']['leg1']['outbound'] = get_flight_details(leg_origin=trigger_obj['origin'],
                                                                            leg_destination=temp_reco_object_1[
                                                                                'destination'],
                                                                            compartment=trigger_obj['compartment'],
                                                                            dep_date_start=dep_date_start,
                                                                            dep_date_end=dep_date_end)
        if trigger_obj['host_pricing_data']['oneway_return'] == 2:
            trigger_obj['flight_data']['leg1']['inbound'] = get_flight_details(
                leg_origin=temp_reco_object_1['destination'],
                leg_destination=trigger_obj['origin'],
                compartment=trigger_obj['compartment'],
                dep_date_start=dep_date_start,
                dep_date_end=dep_date_end)

        temp_reco_object_2 = deepcopy(trigger_obj)
        temp_reco_object_2['origin'] = Host_Airline_Hub
        leg_2_class_obj = Availability(temp_reco_object_2)
        leg_2_class_obj.do_analysis()
        # trigger_obj['seat_factor']['leg2'] = 100 - leg_2_class_obj.availability_perc
        trigger_obj['flight_data']['leg2']['outbound'] = get_flight_details(leg_origin=temp_reco_object_2['origin'],
                                                                            leg_destination=trigger_obj[
                                                                                'destination'],
                                                                            compartment=trigger_obj['compartment'],
                                                                            dep_date_start=dep_date_start,
                                                                            dep_date_end=dep_date_end)
        if trigger_obj['host_pricing_data']['oneway_return'] == 2:
            trigger_obj['flight_data']['leg2']['inbound'] = get_flight_details(
                leg_origin=trigger_obj['destination'],
                leg_destination=temp_reco_object_2['origin'],
                compartment=trigger_obj['compartment'],
                dep_date_start=dep_date_start,
                dep_date_end=dep_date_end)

    return trigger_obj


def update_market_workflow_params(trigger_obj):
    """
    :param trigger_obj:
    :return:
    """
    #   Obtaining top5 competitors
    import time
    st = time.time()
    print 'trigger_obj', trigger_obj
    top_5_comp = obtain_top_5_comp(pos=trigger_obj['pos'],
                                   origin=trigger_obj['origin'],
                                   destination=trigger_obj['destination'],
                                   compartment=trigger_obj['compartment'],
                                   dep_date_start=trigger_obj['triggering_data']['dep_date_start'],
                                   dep_date_end=trigger_obj['triggering_data']['dep_date_end'],
                                   )

    print 'COMPETITORS DATA', top_5_comp
    print '******************************************To get top5 comp', time.time() - st
    mrkt_data = dict(host=dict(), comp=list())
    #   Market Share Data
    #   MS
    #   VLYR
    #   FMS
    #   VTGT
    host_ms_data = calculate_market_share(airline=Host_Airline_Code,
                                          pos=trigger_obj['pos'],
                                          origin=trigger_obj['origin'],
                                          destination=trigger_obj['destination'],
                                          compartment=trigger_obj['compartment'],
                                          dep_date_from=trigger_obj['triggering_data']['dep_date_start'],
                                          dep_date_to=trigger_obj['triggering_data']['dep_date_end'])

    mrkt_data['host'].update(host_ms_data)
    print '************************************TO get Host MS', time.time() - st
    #   Most Frequencty Available Infare Fare
    #   Base Fare
    #   Taxes
    #   Currency
    #   Frequency
    host_available_fare = get_valid_infare_fare(airline=Host_Airline_Code,
                                                origin=trigger_obj['origin'],
                                                destination=trigger_obj['destination'],
                                                compartment=trigger_obj['compartment'],
                                                dep_date_start=trigger_obj['triggering_data']['dep_date_start'],
                                                dep_date_end=trigger_obj['triggering_data']['dep_date_end'],
                                                oneway_return=2)

    host_avail_fare_dict = dict(most_available_fare=dict(base_fare=host_available_fare['base_price'],
                                                         tax=host_available_fare['tax'],
                                                         total_fare=host_available_fare['price'],
                                                         frequency=host_available_fare['frequency']))

    mrkt_data['host'].update(host_avail_fare_dict)

    print 'TO get Host Available Fare comp', time.time() - st
    #   Host Rating Details
    #   Comp Rating
    #   Prod Rating
    #   Distributor Rating
    #   Fare rating
    #   Capacity Rating
    #   Market Rating
    host_rating_details = get_ratings_details(origin=trigger_obj['origin'],
                                              destination=trigger_obj['destination'],
                                              airline=Host_Airline_Code,
                                              compartment=trigger_obj['compartment'])

    mrkt_data['host'].update(host_rating_details)
    print '**************************TO get host ratings_details', time.time() - st

    #   Host Price Movement (Lowest Available Fare)
    #   lowest fare frequency
    #   highest fare frequency
    price_movement_host_lowest_available = get_price_movement(airline=Host_Airline_Code,
                                                              pos=trigger_obj['pos'],
                                                              origin=trigger_obj['origin'],
                                                              destination=trigger_obj['destination'],
                                                              compartment=trigger_obj['compartment'],
                                                              oneway_return=2,
                                                              dep_date_start=trigger_obj['triggering_data']['dep_date_start'],
                                                              dep_date_end=trigger_obj['triggering_data']['dep_date_end'],
                                                              currency=trigger_obj['currency'],
                                                              source='infare')

    mrkt_data['host'].update(
        {
            'price_movement_lowest_available':price_movement_host_lowest_available
        }
    )

    #   Host Price Movement (Filed ATPCO Fares)
    #   lowest filed fare
    #   highest filed fare
    price_movement_host_filed = get_price_movement(airline=Host_Airline_Code,
                                                   pos=trigger_obj['pos'],
                                                   origin=trigger_obj['origin'],
                                                   destination=trigger_obj['destination'],
                                                   compartment=trigger_obj['compartment'],
                                                   oneway_return=2,
                                                   dep_date_start=trigger_obj['triggering_data']['dep_date_start'],
                                                   dep_date_end=trigger_obj['triggering_data']['dep_date_end'],
                                                   currency=trigger_obj['currency'],
                                                   source='ATPCO')

    mrkt_data['host'].update({
        'price_movement_filed': price_movement_host_filed
    })

    host_lowest_filed_fare = mrkt_data['host']['price_movement_filed']['lowest_fare']

    mrkt_data['host'].update(dict(lowest_filed_fare=host_lowest_filed_fare))

    print '******************************TO get host filed fare', time.time() - st
    for idx, comp in enumerate(top_5_comp[0]):
        comp_data = dict(airline=comp,
                         source=top_5_comp[1][idx])

        #   Market Share Data
        #   MS
        #   VLYR
        #   FMS
        #   VTGT

        comp_ms_data = calculate_market_share(airline=comp,
                                              pos=trigger_obj['pos'],
                                              origin=trigger_obj['origin'],
                                              destination=trigger_obj['destination'],
                                              compartment=trigger_obj['compartment'],
                                              dep_date_from=trigger_obj['triggering_data']['dep_date_start'],
                                              dep_date_to=trigger_obj['triggering_data']['dep_date_end'])

        comp_data.update(comp_ms_data)

        #   Most Frequency Available Infare Fare
        #   Base Fare
        #   Taxes
        #   Currency
        #   Frequency
        comp_available_fare = get_valid_infare_fare(airline=comp,
                                                    origin=trigger_obj['origin'],
                                                    destination=trigger_obj['destination'],
                                                    compartment=trigger_obj['compartment'],
                                                    dep_date_start=trigger_obj['triggering_data']['dep_date_start'],
                                                    dep_date_end=trigger_obj['triggering_data']['dep_date_end'],
                                                    oneway_return=2)

        comp_avail_fare_dict = dict(most_available_fare=dict(base_fare=comp_available_fare['base_price'],
                                                             tax=comp_available_fare['tax'],
                                                             total_fare=comp_available_fare['price'],
                                                             frequency=comp_available_fare['frequency']))

        comp_data.update(comp_avail_fare_dict)

        #   Host Rating Details
        #   Comp Rating
        #   Prod Rating
        #   Distributor Rating
        #   Fare rating
        #   Capacity Rating
        #   Market Rating
        comp_rating_details = get_ratings_details(origin=trigger_obj['origin'],
                                                  destination=trigger_obj['destination'],
                                                  airline=comp,
                                                  compartment=trigger_obj['compartment'])

        #   Host Price Movement (Lowest Available Fare)
        #   lowest fare frequency
        #   highest fare frequency
        comp_price_movement_lowest_availability = get_price_movement(airline=comp,
                                                                     pos=trigger_obj['pos'],
                                                                     origin=trigger_obj['origin'],
                                                                     destination=trigger_obj['destination'],
                                                                     compartment=trigger_obj['compartment'],
                                                                     oneway_return=2,
                                                                     dep_date_start=trigger_obj['triggering_data']['dep_date_start'],
                                                                     dep_date_end=trigger_obj['triggering_data']['dep_date_end'],
                                                                     currency=trigger_obj['currency'],
                                                                     source='infare')
        comp_data.update({
            'price_movement_lowest_available': comp_price_movement_lowest_availability
        })

        #   Host Price Movement (Filed ATPCO Fares)
        #   lowest filed fare
        #   highest filed fare
        comp_price_movement_filed = get_price_movement(airline=comp,
                                                       pos=trigger_obj['pos'],
                                                       origin=trigger_obj['origin'],
                                                       destination=trigger_obj['destination'],
                                                       compartment=trigger_obj['compartment'],
                                                       oneway_return=2,
                                                       dep_date_start=trigger_obj['triggering_data']['dep_date_start'],
                                                       dep_date_end=trigger_obj['triggering_data']['dep_date_end'],
                                                       currency=trigger_obj['currency'],
                                                       source='ATPCO')
        print 'COMP PRICE MOVEMENT FILED', comp_price_movement_filed
        comp_data.update({
            'price_movement_filed': comp_price_movement_filed
        })
        comp_lowest_filed_fare = comp_data['price_movement_filed']['lowest_fare']

        comp_data.update(dict(lowest_filed_fare=comp_lowest_filed_fare))

        comp_data.update(comp_rating_details)

        mrkt_data['comp'].append(comp_data)
    print '**********************************TO get Comp Data', time.time() - st

    trigger_obj['mrkt_data'] = mrkt_data

    return trigger_obj


def build_qry_leg_level_cap(leg_origin, leg_destination, compartment, dep_date_start, dep_date_end):
    """
    :param leg_origin:
    :param leg_destination:
    :param dep_date_start:
    :param dep_date_end:
    :return:
    """
    query = defaultdict(list)
    query['$and'].append({
        'airline': Host_Airline_Code
    })
    query['$and'].append({
        'origin': leg_origin
    })

    query['$and'].append({
        'destination': leg_destination
    })

    query['$and'].append({
        'compartment': compartment
    })
    print '*******************************', dep_date_start, dep_date_end
    from_obj = datetime.datetime.strptime(dep_date_start, '%Y-%m-%d')
    to_obj = datetime.datetime.strptime(dep_date_end, '%Y-%m-%d')
    month_year = query_month_year_builder(from_obj.month, from_obj.year, to_obj.month, to_obj.year)
    lst_timeseries = list()
    for m_y in month_year:
        # print m_y['month'],m_y['year']
        lst_timeseries.append({'timeseries': (str(m_y['year']) + str(m_y['month']).zfill(2))})
    print 'list', lst_timeseries
    print 'Timeseries built...'
    query['$and'].append({'$or': lst_timeseries})
    print query
    return dict(query)


def generate_ppln_leg_capacity(qry_leg_level_cap):
    """
    :param qry_leg_level_cap:
    :return:
    """
    ppln_flight_no = [
        {
            '$match': qry_leg_level_cap
        }
        ,
        {
            '$sort': {'last_update_date': -1}
        }
        ,
        {
            '$group':
                {
                    '_id':
                        {
                            'flight_num': '$flight',
                            'origin': '$origin',
                            'destination': '$destination',
                            'compartment': '$compartment',
                            'timeseries': '$timeseries'
                        }
                    ,
                    'doc': {'$first': "$$ROOT"}
                }
        }
        ,
        {
            '$project':
                {
                    'flight_num': '$doc.flight',
                    'year': {'$substr': ['$doc.timeseries', 0, 4]},
                    'month': {'$substr': ['$doc.timeseries', 4, -1]},
                    'dow': '$doc.op_days',
                    'frequency': '$doc.frequency'
                }

        }
    ]
    return ppln_flight_no


def get_flight_details(leg_origin, leg_destination, compartment, dep_date_start, dep_date_end):
    """
    :param leg_origin:
    :param leg_destination:
    :param dep_date_start:
    :param dep_date_end:
    :return:
    """

    qry_leg_level_cap = build_qry_leg_level_cap(leg_origin=leg_origin,
                                                leg_destination=leg_destination,
                                                compartment=compartment,
                                                dep_date_start=dep_date_start,
                                                dep_date_end=dep_date_end)
    print qry_leg_level_cap
    ppln_leg_level_cap = generate_ppln_leg_capacity(qry_leg_level_cap=qry_leg_level_cap)
    print ppln_leg_level_cap
    leg_cap_crsr = db['JUP_DB_Capacity'].aggregate(ppln_leg_level_cap)

    lst_leg_cap_data = list(leg_cap_crsr)

    if lst_leg_cap_data:
        return lst_leg_cap_data
    else:
        return 'NA'


class var(object):
    pass

if __name__ == '__main__':
    import json
    obj = var()
    obj.pos = 'DXB'
    obj.origin = 'DXB'
    obj.destination = 'DEL'
    obj.compartment = 'Y'
    obj.trigger_type = 'booking_changes_weekly'
    obj.price_recommendation = 1500
    obj.currency = 'AED'
    obj.triggering_data = {
        'dep_date_start': '2017-05-01',
        'dep_date_end': '2017-05-31'
    }
    obj.host_pricing_data = {
        'fare_basis': 'MO3AE1',
        'RBD': 'L',
        'oneway_return': 2,
        'currency': 'AED'
    }
    obj = obj.__dict__
    update_host_workflow_params_comp(trigger_obj=obj)
    update_host_workflow_params_fb(trigger_obj=obj)
    # print json.dumps(obj.__dict__, indent=1)
    # obtain_top_5_comp(trigger_obj=obj)
    update_market_workflow_params(trigger_obj=obj)
    print json.dumps(obj, indent=1)

    # print get_flight_details('DXB', 'DOH', 'Y', '2017-05-01', '2017-05-31')

