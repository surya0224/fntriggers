import inspect
import jupiter_AI.common.ClassErrorObject as errorClass
from jupiter_AI.common.calculate_market_share import calculate_market_share
from jupiter_AI.common.convert_currency import convert_currency
from jupiter_AI import client, JUPITER_DB, Host_Airline_Code, Host_Airline_Hub, JUPITER_LOGGER
from jupiter_AI.logutils import measure
#db = client[JUPITER_DB]
from jupiter_AI.triggers.GetInfareFare import get_valid_infare_fare
from jupiter_AI.triggers.common import get_ratings_details


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
def get_price_recommendation(h, c):
    '''
    This function contains the Logic used for recommendation
    Oligopoly Model
    Dynamic Pricing Models for Airline by RAMBO 2002 (pg.no/69-71)

    Takes host and competitor data as inputs
    Calculates the fare till 3 decimal points
    '''
    market_share_flag = True
    if not h['market_share']:
        market_share_flag = False
    for doc in c:
        if not doc['market_share']:
            market_share_flag = False
    if market_share_flag:
        # p = sum(i['market_share'] for i in c)
        # compt = 0
        # for i in range(len(c)):
        #     c[i]['market_share_host_excluded'] = c[i]['market_share'] / p
        #     compt_individual = (c[i]['market_share_host_excluded'] * c[i]['price']) / c[i]['rating']
        #     compt = compt + compt_individual
        #     fare = compt * h['rating']

        fare_contribution = 0
        for comp_data in c:
            comp_contribution_indv = comp_data['market_share'] * comp_data['price'] / comp_data['rating']
            fare_contribution += comp_contribution_indv

        host_contribution_indv = h['market_share'] * h['fare'] / h['rating']

        fare_contribution += host_contribution_indv

        fare = h['rating'] * fare_contribution

    else:
        compt = 0
        for comp_fare in c:
            compt += comp_fare['price'] / comp_fare['rating']
        host = h['fare'] / h['rating']
        compt += host
        fare = compt * h['rating']
    price_recommendation = fare
    # change = (fare - h['total_fare']) * 100 / h['total_fare']
    # if change > recommendation_upper_threshold:
    #     price_recommendation = h['total_fare'] * (100 + recommendation_upper_threshold) / 100
    #     calculated_recommendation = fare
    # elif change < recommendation_lower_threshold:
    #     price_recommendation = h['total_fare'] * (100 + recommendation_lower_threshold) / 100
    #     calculated_recommendation = fare
    # else:
    #     price_recommendation = fare
    #     calculated_recommendation = fare

    return price_recommendation


@measure(JUPITER_LOGGER)
def get_recommended_fare(fare_doc):
    """
    :param fare_doc:
    :return:
    """
    mrkt_shr_data = calculate_market_share(airline= Host_Airline_Code,
                                           pos=fare_doc['pos'],
                                           origin=fare_doc['origin'],
                                           destination=fare_doc['destination'],
                                           compartment=fare_doc['compartment'],
                                           dep_date_from=fare_doc['dep_date_from'],
                                           dep_date_to=fare_doc['dep_date_to'])

    host_rating = get_ratings_details(origin=fare_doc['origin'],
                                      destination=fare_doc['destination'],
                                      airline=Host_Airline_Code,
                                      compartment=fare_doc['compartment'])
    fare_doc.update(mrkt_shr_data)
    fare_doc.update(host_rating)
    print 'fare_doc', fare_doc
    comp = []
    for comp_fb in fare_doc['competitor_farebasis']:
        comp_doc = get_valid_infare_fare(airline=comp_fb['airline'],
                                         origin=fare_doc['origin'],
                                         destination=fare_doc['destination'],
                                         compartment=fare_doc['compartment'],
                                         dep_date_start=fare_doc['dep_date_from'],
                                         dep_date_end=fare_doc['dep_date_to'])

        comp_mrkt_shr_data = calculate_market_share(airline=comp_fb['airline'],
                                                    pos=fare_doc['pos'],
                                                    origin=fare_doc['origin'],
                                                    destination=fare_doc['destination'],
                                                    compartment=fare_doc['compartment'],
                                                    dep_date_from=fare_doc['dep_date_from'],
                                                    dep_date_to=fare_doc['dep_date_to'])

        comp_doc.update(comp_mrkt_shr_data)
        print 'host_currency', fare_doc['currency']
        print 'comp_currency', comp_doc['currency']
        print 'comp_price', comp_doc['price']
        comp_doc['price'] = convert_currency(comp_doc['price'],
                                             from_code=comp_doc['currency'],
                                             to_code=fare_doc['currency'])
        print 'comp_price', comp_doc['price']
        comp.append(comp_doc)
        print comp_fb['airline'] + ' doc', comp_doc

    price_recommendation = get_price_recommendation(h=fare_doc,
                                                    c=comp)
    print 'price_recommendation'
    return price_recommendation


@measure(JUPITER_LOGGER)
def __main__():
    """
    Main Function where the batch is run
    Logic/Flow of Code:
    1 For each farebasis in host pricing data collection
    2 Store the host data
    3 Use the host farebasis to query the farebasis collection for
      competitors farebasis mapped to this host farebasis
    4 Get the competitors data by qurying the airline and farebasis
    5 Call the price recommendation function to obtain the recommended fare
    6 Update the JUP_DB_Batch_Price_Recommendation collection with farebasis,
      fare,date,and time
    """
    market_combinations_crsr = db.JUP_DB_ATPCO_Fares.aggregate(
    [
        {
            '$match': {
                'pos': Host_Airline_Hub,
                'origin': Host_Airline_Hub,
                'destination': 'DOH',
                'compartment': 'Y'
            }
        }
        ,
        {
            '$group': {
                '_id': {
                    'pos': '$pos',
                    'origin': '$origin',
                    'destination': '$destination',
                    'compartment': '$compartment'
                }
                ,
                'data': {
                    '$push': '$$ROOT'
                }
            }
        }
    ]
    )
    lst_market_combinations = list(market_combinations_crsr)
    print lst_market_combinations
    if len(lst_market_combinations) > 0:
        for combination in lst_market_combinations:
            fares_data = combination['data']
            print len(fares_data)
            if len(fares_data) > 0:
                change = 0
                host_fb = ''
                host_fbs = [fare_doc['fare_basis'] for fare_doc in fares_data if fare_doc['oneway_return'] == 1 and fare_doc['channel'] == 'WEB']
                host_fl = [fare_doc['total_fare'] for fare_doc in fares_data if fare_doc['oneway_return'] == 1 and fare_doc['channel'] == 'WEB']
                for fare_doc in fares_data:
                    if fare_doc['competitor_farebasis']:
                        print 'comp_fb', fare_doc['competitor_farebasis']
                        recommended_fare = get_recommended_fare(fare_doc)
                        print 'recommended fare', recommended_fare
                        change = recommended_fare - fare_doc['total_fare']
                        print 'change', change
                        differences = map(lambda x: x - recommended_fare, host_fl)
                        zipped_list = zip(host_fbs, host_fl, differences)
                        print zipped_list
                        sorted_tuple = sorted(zipped_list, key=lambda x: x[2])
                        host_fb = sorted_tuple[1][0]
                        print 'host_fb', host_fb

                        fare_doc_id = [fare_doc['_id'] for fare_doc in fares_data if fare_doc['fare_basis'] == host_fb]
                        if fare_doc_id:
                            db.JUP_DB_ATPCO_Fares.update(
                                {
                                    '_id': fare_doc_id[0]
                                }
                                ,
                                {
                                    '$set':
                                        {
                                            'recommended_fare': recommended_fare
                                        }
                                }
                                ,
                                upsert=True
                            )

                for fare_doc in fares_data:
                    try:
                        print fare_doc['recommended_fare']
                    except KeyError:
                        print 'change', change
                        db.JUP_DB_ATPCO_Fares.update(
                            {
                                '_id': fare_doc['_id']
                            }
                            ,
                            {
                                '$set':
                                    {
                                        'recommended_fare': fare_doc['total_fare'] + change
                                    }
                            }
                            ,
                            upsert=True
                        )








    # cursor = db.JUP_DB_ATPCO_Fares.find({
    #     'competitor_farebasis': {'$ne': None}
    # })
    # print cursor.count()
    # if cursor.count() != 0:
    #     for i in cursor:
    #         del i['_id']
    #         print i
    #         farebasis = i['farebasis']
    #         i['fare'] = convert_currency(value=i['fare'], from_code=i['currency'])
    #         if i['competitor_farebasis']:
    #             i['market_share'] = calculate_market_share(airline=i['airline'],
    #                                                        pos=i['pos'],
    #                                                        origin=i['origin'],
    #                                                        destination=i['destination'],
    #                                                        compartment=i['compartment'],
    #                                                        dep_date_from=i['departure_date_from'],
    #                                                        dep_date_to=i['departure_date_to'])['market_share']
    #             print i['market_share']
    #             if i['market_share']:
    #                 host = i
    #                 comp = []
    #                 for j in i['competitor_farebasis']:
    #                     cursor3 = query_competitor_pricing_data({
    #                         'pos': i['pos'],
    #                         'origin': i['origin'],
    #                         'destination': i['destination'],
    #                         'compartment': i['compartment'],
    #                         'airline': j['airline'],
    #                         'farebasis': j['farebasis']})
    #                     print len(cursor3)
    #                     if len(cursor3) == 1:
    #                         for ii in cursor3:
    #                             ii['market_share'] = calculate_market_share(airline=ii['airline'],
    #                                                                         pos=ii['pos'],
    #                                                                         origin=ii['origin'],
    #                                                                         destination=ii['destination'],
    #                                                                         compartment=ii['compartment'],
    #                                                                         dep_date_from=i['departure_date_from'],
    #                                                                         dep_date_to=i['departure_date_to'])['market_share']
    #                             ii['price'] = convert_currency(value=ii['price'], from_code=ii['currency'])
    #                             print ii['airline'],ii['market_share'], ii['price']
    #                             comp.append(ii)
    #                     print host
    #                     print comp
    #                     print host != None and comp != []
    #                     if host != None and comp != []:
    #                         fare = get_price_recommendation(host, comp)
    #                         print fare
    #                         print host
    #                         response = {'recommended_fare': round(fare, 3),
    #                                     'recommended_date': str(datetime.date.today()),
    #                                     'recommended_time': str(datetime.datetime.now().time())}
    #                         response.update(host)
    #                         print json.dumps(response, indent=1)
    #                         # db.JUP_DB_Batch_Price_Recommendation.insert_one(response)
    # else:
    #     e2 = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
    #                                 get_module_name(),
    #                                 get_arg_lists(inspect.currentframe()))
    #     e2.append_to_error_list("Expected at least 1 document for Farebasis but got " + str(cursor.count()))
    #     raise e2


try:
    __main__()
except errorClass.ErrorObject as esub:
    print esub.__str__()
    # db.JUP_DB_Error_Collection.insert_one({'program': 'price_recommendation_batch_updated.py',
    #                                        'description': esub.__str__()})
