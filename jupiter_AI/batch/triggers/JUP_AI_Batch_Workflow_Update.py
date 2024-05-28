import time
import datetime
from jupiter_AI import client, JUPITER_LOGGER
from jupiter_AI.batch.JUP_AI_Batch_Top5_Competitors import obtain_top_5_comp
from jupiter_AI.common.calculate_market_share import calculate_market_share
from jupiter_AI.network_level_params import Host_Airline_Code, SYSTEM_DATE, JUPITER_DB, today
from jupiter_AI.triggers.common import get_start_end_dates
from jupiter_AI.triggers.GetInfareFare import get_valid_infare_fare
from jupiter_AI.triggers.workflow_parameters import get_lowest_filed_fare
# from jupiter_AI.triggers.workflow_parameters import update_host_workflow_params_comp, update_market_workflow_params
from jupiter_AI.triggers.workflow_parameters import update_host_workflow_params_fb
from jupiter_AI.triggers.host_params_workflow_opt import main_func as update_host_workflow_params_comp
from jupiter_AI.triggers.mrkt_params_workflow_opt import comp_summary_python as update_market_workflow_params
from jupiter_AI.logutils import measure
#db = client[JUPITER_DB]
#print db


@measure(JUPITER_LOGGER)
def update_mrkt_data_comp_level(trigger_obj):
    """
    :return:
    """
    #   Obtaining top5 competitors
    top_5_comp = obtain_top_5_comp(pos=trigger_obj['pos'],
                                   origin=trigger_obj['origin'],
                                   destination=trigger_obj['destination'],
                                   compartment=trigger_obj['compartment'],
                                   dep_date_start=trigger_obj['triggering_data']['dep_date_start'],
                                   dep_date_end=trigger_obj['triggering_data']['dep_date_end'],
                                   region=None,
                                   country=None)

    print 'COMPETITORS DATA', top_5_comp

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
                                                oneway_return=None)

    host_avail_fare_dict = dict(most_available_fare=dict(base_fare=host_available_fare['base_price'],
                                                         tax=host_available_fare['tax'],
                                                         total_fare=host_available_fare['price'],
                                                         frequency=host_available_fare['frequency'],
                                                         oneway_return=host_available_fare['oneway_return']))

    mrkt_data['host'].update(host_avail_fare_dict)

    host_lowest_filed_fare = get_lowest_filed_fare(airline=Host_Airline_Code,
                                                   pos=trigger_obj['pos'],
                                                   origin=trigger_obj['origin'],
                                                   destination=trigger_obj['destination'],
                                                   compartment=trigger_obj['compartment'],
                                                   oneway_return=None,
                                                   currency=trigger_obj['host_pricing_data']['currency'],
                                                   dep_date_start=trigger_obj['triggering_data']['dep_date_start'],
                                                   dep_date_end=trigger_obj['triggering_data']['dep_date_end'],
                                                   sale_date_start=None,
                                                   sale_date_end=None)

    mrkt_data['host'].update(dict(lowest_filed_fare=host_lowest_filed_fare))

    #   Host Rating Details
    #   Comp Rating
    #   Prod Rating
    #   Distributor Rating
    #   Fare rating
    #   Capacity Rating
    #   Market Rating
    # host_rating_details = get_ratings_details(origin=trigger_obj['origin'],
    #                                           destination=trigger_obj['destination'],
    #                                           airline=Host_Airline_Code)
    #
    # mrkt_data['host'].update(host_rating_details)

    #   Host Price Movement (Lowest Available Fare)
    #   lowest fare frequency
    #   highest fare frequency
    # price_movement_host_lowest_available = get_price_movement(airline=Host_Airline_Code,
    #                                                           pos=trigger_obj['pos'],
    #                                                           origin=trigger_obj['origin'],
    #                                                           destination=trigger_obj['destination'],
    #                                                           compartment=trigger_obj['compartment'],
    #                                                           oneway_return=trigger_obj['host_pricing_data'][
    #                                                               'oneway_return'],
    #                                                           dep_date_start=trigger_obj['triggering_data'][
    #                                                               'dep_date_start'],
    #                                                           dep_date_end=trigger_obj['triggering_data'][
    #                                                               'dep_date_end'],
    #                                                           currency=trigger_obj['currency'],
    #                                                           source='infare')
    #
    # mrkt_data['host'].update(
    #     {
    #         'price_movement_lowest_available': price_movement_host_lowest_available
    #     }
    # )
    #
    # #   Host Price Movement (Filed ATPCO Fares)
    # #   lowest filed fare
    # #   highest filed fare
    # price_movement_host_filed = get_price_movement(airline=Host_Airline_Code,
    #                                                pos=trigger_obj['pos'],
    #                                                origin=trigger_obj['origin'],
    #                                                destination=trigger_obj['destination'],
    #                                                compartment=trigger_obj['compartment'],
    #                                                oneway_return=trigger_obj['host_pricing_data']['oneway_return'],
    #                                                dep_date_start=trigger_obj['triggering_data']['dep_date_start'],
    #                                                dep_date_end=trigger_obj['triggering_data']['dep_date_end'],
    #                                                currency=trigger_obj['currency'],
    #                                                source='atpco')
    #
    # mrkt_data['host'].update({
    #     'price_movement_filed': price_movement_host_filed
    # })

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
                                                    oneway_return=None)

        comp_avail_fare_dict = dict(most_available_fare=dict(base_fare=comp_available_fare['base_price'],
                                                             tax=comp_available_fare['tax'],
                                                             total_fare=comp_available_fare['price'],
                                                             frequency=comp_available_fare['frequency']))

        comp_data.update(comp_avail_fare_dict)

        comp_lowest_filed_fare = get_lowest_filed_fare(airline=comp,
                                                       pos=trigger_obj['pos'],
                                                       origin=trigger_obj['origin'],
                                                       destination=trigger_obj['destination'],
                                                       compartment=trigger_obj['compartment'],
                                                       oneway_return=None,
                                                       currency=trigger_obj['currency'],
                                                       dep_date_start=trigger_obj['triggering_data']['dep_date_start'],
                                                       dep_date_end=trigger_obj['triggering_data']['dep_date_end'],
                                                       sale_date_start=None,
                                                       sale_date_end=None)

        comp_data.update(dict(lowest_filed_fare=comp_lowest_filed_fare))
        mrkt_data['comp'].append(comp_data)

    trigger_obj['mrkt_data'] = mrkt_data
    return trigger_obj


@measure(JUPITER_LOGGER)
def update_workflow_docs():
    print db
    crsr = db.JUP_DB_Workflow_1_dummy_copy.find()
    print 'No of Docs', crsr.count()

    for doc in crsr:
        doc = update_host_workflow_params_comp(doc)
        doc = update_host_workflow_params_fb(doc)
        doc = update_market_workflow_params(doc)
        db.JUP_DB_Workflow_1_dummy.update(
            {
                '_id': doc['_id']
            }
            ,
            {
                '$set': doc
            }
        )


@measure(JUPITER_LOGGER)
def get_dep_date_filters():
    crsr = db.JUP_DB_Config_Date.find()
    dates_list = list()

    for doc in crsr:
        for val in doc['configured_days']:
            val_date = today + datetime.timedelta(days=int(val))
            end_date_str = datetime.datetime.strftime(val_date, '%Y-%m-%d')
            tempdict = {'start':SYSTEM_DATE,'end':end_date_str}
            if tempdict not in dates_list:
                dates_list.append(tempdict)

    current_month = today.month
    current_year = today.year
    cm_sd, cm_ed = get_start_end_dates(month=current_month, year=current_year)
    dates_list.append({'start': cm_sd, 'end': cm_ed})

    if current_month == 12:
        next_month = 1
        next_year = current_year+1
    else:
        next_month = current_month+1
        next_year = current_year

    cm_1_sd, cm_1_ed = get_start_end_dates(month=next_month, year=next_year)
    dates_list.append({'start': cm_1_sd, 'end': cm_1_ed})

    if next_month == 12:
        next_month = 1
        next_year = next_year+1
    else:
        next_month = next_month+1
        next_year = next_year

    cm_2_sd, cm_2_ed = get_start_end_dates(month=next_month, year=next_year)
    dates_list.append({'start': cm_2_sd, 'end': cm_2_ed})

    print 'date_ranges?????????', dates_list
    return dates_list


@measure(JUPITER_LOGGER)
def get_unique_ods(dep_range):
    # print db['JUP_DB_Workflow_1_dummy'].find().count()
    unique_od_level_crsr = db['JUP_DB_Workflow_1_dummy'].aggregate(
        [
            {
                '$match':
                    {
                        'triggering_data.dep_date_start': {'$gte': dep_range['start']},
                        'triggering_data.dep_date_end':{'$lte': dep_range['end']},
                        'status':'pending',
                        'price_recommendation':{'$ne':None}
                    }
            }
            ,
            {
                '$sort':{
                    'triggering_data.dep_date_start':1,
                    'triggering_data.dep_date_end':1
                }
            }
            ,
            {
                '$group':
                    {
                        '_id':
                            {
                                'pos':'$pos',
                                'origin': '$origin',
                                'destination': '$destination',
                                'compartment': '$compartment'
                                ,
                                'currency':'$currency'
                            },
                        'dep_date_start': {'$first': '$triggering_data.dep_date_start'},
                        'dep_date_end': {'$last': '$triggering_data.dep_date_end'}
                    }

            }
            ,
            {
                '$project':
                    {
                        '_id':0,
                        'pos':'$_id.pos',
                        'origin': '$_id.origin',
                        'destination': '$_id.destination',
                        'compartment': '$_id.compartment',
                        'dep_date_start':'$dep_date_start',
                        'dep_date_end':'$dep_date_end'
                        # ,
                        # 'currency':'$_id.currency'
                    }
            }
        ]
    )

    combinations = list(unique_od_level_crsr)
    print 'Unique OD Combinations', combinations
    return combinations


@measure(JUPITER_LOGGER)
def update_od_data_user_config():
    """
    :return:
    """
    st = time.time()
    print '*************************************************************', time.time
    dep_date_ranges = get_dep_date_filters()
    # db.JUP_DB_Workflow_OD_user.drop()
    for range_ in dep_date_ranges:
        od_comp_combinations = get_unique_ods(range_)
        print od_comp_combinations
        print '**************************Getting OD Combinations from Workflow', time.time() - st

        for combination in od_comp_combinations:
            crsr = db.JUP_DB_Workflow_OD_user_1_copy.find({
                'pos': combination['pos'],
                'origin': combination['origin'],
                'destination': combination['destination'],
                'compartment': combination['compartment'],
                'dep_date_start': range_['start'],
                'dep_date_end': range_['end']
            })
            if crsr.count() == 0:
                if combination['currency'] != 'USD':
                    # trigger_obj = dict(
                    #     pos=combination['pos'],
                    #     origin=combination['origin'],
                    #     destination=combination['destination'],
                    #     compartment=combination['compartment'],
                    #     # currency=combination['currency'],
                    #     user_dep_date_start=range_['start'],
                    #     user_dep_date_end=range_['end'],
                    #     triggering_data=dict(
                    #         dep_date_start=range_['start'],
                    #         dep_date_end=range_['end']
                    #     ),
                    #     host_pricing_data=dict(
                    #         currency=combination['currency']
                    #     )
                    # )
                    # if trigger_obj['pos'] == 'DXB':
                    #     trigger_obj['currency'] = 'AED'
                    #     trigger_obj['host_pricing_data'] = dict(
                    #         currency='AED'
                    #     )
                    # elif trigger_obj['pos'] == 'DOH':
                    #     trigger_obj['currency'] = 'QAR'
                    #     trigger_obj['host_pricing_data'] = dict(
                    #         currency='QAR'
                    #     )
                    host_data = update_host_workflow_params_comp(pos=combination['pos'],
                                                                 origin=combination['origin'],
                                                                 destination=combination['destination'],
                                                                 compartment=combination['compartment'],
                                                                 dep_date_start=combination['dep_date_start'],
                                                                 dep_date_end=combination['dep_date_end'])
                    print '**************************Getting Host Comp Level Data', time.time() - st
                    mrkt_data = update_market_workflow_params(pos=combination['pos'],
                                                                origin=combination['origin'],
                                                                destination=combination['destination'],
                                                                compartment=combination['compartment'],
                                                                dep_date_start=combination['dep_date_start'],
                                                                dep_date_end=combination['dep_date_end'])
                    print '**************************Getting Mrkt Comp Level Data', time.time() - st
                    # del trigger_obj['triggering_data']

                    response = dict()
                    response['pos'] = combination['pos']
                    response['dep_date_start'] = combination['dep_date_start']
                    response['dep_date_end'] = combination['dep_date_end']
                    response['origin'] = combination['origin']
                    response['destination'] = combination['destination']
                    # response['currency'] = trigger_obj['currency']
                    response['od'] = combination['origin'] + combination['destination']
                    response['compartment'] = combination['compartment']
                    response['pax_data'] = host_data['pax_data']
                    response['revenue_data'] = host_data['revenue_data']
                    response['yield_data'] = host_data['yield_data_compartment']
                    response['avg_fare_data'] = host_data['avg_fare_data']
                    response['seat_factor_data'] = host_data['seat_factor']
                    response['mrkt_data'] = mrkt_data['mrkt_data']
                    response['update_date'] = SYSTEM_DATE

                    print 'Object to be uploaded in system', response
                    id = db.JUP_DB_Workflow_OD_user_1_test.insert(
                        response
                    )
                    print '********************************************** Updated', id


if __name__ == '__main__':
    # update_workflow_docs()
    update_od_data_user_config()
    # dep_range = get_dep_date_filters()
    # print dep_range
    # print get_unique_ods(dep_range)
    # print get_dep_date_filters()