import json
#from collections import defaultdict
from jupiter_AI import client, SYSTEM_DATE, JUPITER_DB, JUPITER_LOGGER
from jupiter_AI.logutils import measure
from jupiter_AI.triggers.common import *
#db = client[JUPITER_DB]

@measure(JUPITER_LOGGER)
def build_infare_query(airline, origin, destination, oneway_return,
                       pos, compartment,
                       dep_date_start, dep_date_end,
                       observation_date_start, observation_date_end):
    infare_query = defaultdict(list)

    # Compulsory Parameters to be queried
    infare_query['$and'].append({'carrier': airline.encode()})
    # if pos:
    #     infare_query['$and'].append({'pos': origin.encode()})
    if origin:
        infare_query['$and'].append({'origin': origin.encode()})
    if destination:
        infare_query['$and'].append({'destination': destination.encode()})
    # if compartment:
    #     infare_query['$and'].append({'compartment': compartment.encode()})
    if oneway_return:
        if oneway_return == 2:
            oneway_return = 0
        elif oneway_return == 3:
            oneway_return = 1
        infare_query['$and'].append({'is_one_way': oneway_return})


    # Combinations if observation dates are input as None
    # if observation_date_start and observation_date_end:
    #     infare_query['$and'].append({
    #         'effective_date': {
    #             '$gte': observation_date_start,
    #             '$lte': observation_date_end
    #         }
    #     })
    # elif observation_date_start:
    #     infare_query['$and'].append({
    #         'effective_date': {
    #             '$gte': observation_date_start,
    #             '$lte': SYSTEM_DATE
    #         }
    #     })
    # elif observation_date_end:
    #     infare_query['$and'].append({
    #         'effective_date': {
    #             '$gte': str(today.year) + '-01-01',
    #             '$lte': observation_date_end
    #         }
    #     })
    # else:
    #     latest_update_date = db.JUP_DB_Infare_Fares.find(infare_query).sort("effective_date",-1).limit(1)[0]['effective_date']
    #     infare_query['$and'].append({
    #         'effective_date': {
    #             '$gte':SYSTEM_DATE
    #         }
    #     })

    # Combinations in Query if departure dates are input as Nones
    if dep_date_start and dep_date_end:
        infare_query['$and'].append({
            'outbound_departure_date': {
                '$gte': dep_date_start,
                '$lte': dep_date_end
            }
        })
    elif dep_date_start:
        infare_query['$and'].append({
            'outbound_departure_date': {
                '$gte': dep_date_start
            }
        })
    elif dep_date_end:
        if observation_date_end:
            infare_query['$and'].append({
                'outbound_departure_date': {
                    '$gte': observation_date_end,
                    '$lte': dep_date_end
                }
            })
        else:
            infare_query['$and'].append({
                'outbound_departure_date': {
                    '$gte': SYSTEM_DATE,
                    '$lte': dep_date_end
                }
            })
    else:
        if observation_date_end:
            infare_query['$and'].append({
                'outbound_departure_date': {
                    '$gte': observation_date_start
                }
            })
        else:
            infare_query['$and'].append({
                'outbound_departure_date': {
                    '$gte': SYSTEM_DATE
                }
            })
    return infare_query


@measure(JUPITER_LOGGER)
def generate_ppln_infare(infare_query):
    infare_ppln = [
        {
            '$match': infare_query,
        }
        ,
        {
            '$group': {
                '_id':
                    {
                        'total_fare': '$price_inc',
                        'currency': '$currency',
                        'base_fare': '$price_exc',
                        'tax': '$tax',
                        'farebasis': '$outbound_fare_basis'
                    },
                'frequency': {'$sum':1}
            }
        },
        {
            '$project':
                {
                    '_id':0,
                    'total_fare': '$_id.total_fare',
                    'currency': '$_id.currency',
                    'base_fare': '$_id.base_fare',
                    'tax': '$_id.tax',
                    'farebasis': '$_id.farebasis',
                    'frequency': '$frequency'
                }
        }
    ]
    return infare_ppln


@measure(JUPITER_LOGGER)
def get_infare_unique_fares_data(airline, origin, destination, oneway_return,
                                 pos, compartment,
                                 db,
                                 dep_date_start, dep_date_end,
                                 observation_date_start, observation_date_end):
    """
    :param infare_query:
    :return:
    """
    infare_query = build_infare_query(airline=airline,
                                      origin=origin,
                                      destination=destination,
                                      oneway_return=oneway_return,
                                      pos=pos,
                                      compartment=compartment,
                                      dep_date_start=dep_date_start,
                                      dep_date_end=dep_date_end,
                                      observation_date_start=observation_date_end,
                                      observation_date_end=observation_date_start)
    print dict(infare_query)
    infare_ppln = generate_ppln_infare(infare_query=dict(infare_query))

    infare_crsr = db['JUP_DB_Infare'].aggregate(infare_ppln)

    fare_frequency_list = list(infare_crsr)
    return fare_frequency_list


@measure(JUPITER_LOGGER)
def get_valid_infare_fare(airline, origin, destination, db, oneway_return=None,
                          pos=None, compartment='Y',
                          dep_date_start=None, dep_date_end=None,
                          observation_date_start=None, observation_date_end=None):
    """
    """
    from jupiter_AI.triggers.common import get_ratings_details
    #   Defining the dict to query JUP_DB_Infare_Fares collection

    unique_fares_list = get_infare_unique_fares_data(airline=airline,
                                                     origin=origin,
                                                     destination=destination,
                                                     oneway_return=oneway_return,
                                                     pos=pos,
                                                     compartment=compartment,
                                                     db=db,
                                                     dep_date_start=dep_date_start,
                                                     dep_date_end=dep_date_end,
                                                     observation_date_start=observation_date_end,
                                                     observation_date_end=observation_date_start)

    print unique_fares_list

    sorted_fare_frequency_list = sorted(unique_fares_list,
                                        key=lambda x: x['frequency'],
                                        reverse=True)
    print sorted_fare_frequency_list

    if len(sorted_fare_frequency_list) > 0:
        fare_tbc = sorted_fare_frequency_list[0]['total_fare']
        fare_tbc_frequency = sorted_fare_frequency_list[0]['frequency']
        fare_currency = sorted_fare_frequency_list[0]['currency']
        fare_tax = sorted_fare_frequency_list[0]['tax']
        farebasis = sorted_fare_frequency_list[0]['farebasis']
        base_price = sorted_fare_frequency_list[0]['base_fare']

        print 'tax', fare_tax
        print 'base', fare_tbc
        response = {
            'source': 'Infare',
            'carrier': airline,
            'pos': origin,
            'farebasis': farebasis,
            'origin': origin,
            'destination': destination,
            'compartment': compartment,
            'oneway_return': oneway_return,
            'tax': fare_tax,
            'price': fare_tbc,
            'base_price': base_price,
            'currency': fare_currency,
            'frequency': fare_tbc_frequency
        }
        print response

        ratings_data = get_ratings_details(origin=response['origin'],
                                           destination=response['destination'],
                                           airline=response['carrier'],
                                           db=db,
                                           compartment=response['compartment'])
        response.update(ratings_data)
        # ratings_crsr = db.JUP_DB_Product_Ratings.find({
        #     # 'od': origin + destination,
        #     'airline': response['carrier']
        # })
        # if ratings_crsr.count() == 1:
        #     response['rating'] = ratings_crsr[0]['rating']
        # else:
        #     prod_rating_crsr = db.JUP_DB_Product_Ratings.find({
        #         'airline': response['carrier']
        #     }).limit(1)
        #     response['rating'] = prod_rating_crsr[0]['rating']
        return response
    else:
        return {
            'source': 'Infare',
            'carrier': "NA",
            'pos': "NA",
            'farebasis': "NA",
            'origin': "NA",
            'destination': "NA",
            'compartment': "NA",
            'oneway_return': "NA",
            'tax': "NA",
            'price': "NA",
            'base_price': "NA",
            'currency': "NA",
            'frequency': "NA"
        }

if __name__ == '__main__':
    airline = 'EK'
    origin = 'DXB'
    destination = 'DOH'
    dep_date_start = '2017-05-01'
    dep_date_end = '2017-05-31'
    observation_date_start = '1900-01-01'
    observation_date_end = '2017-05-13'
    oneway_return = 2
    # observation_date_start = '2017-01-01'
    # observation_date_end = '2017-02-28'
    response = get_valid_infare_fare(
        pos='DXB',
        airline=airline,
        origin=origin,
        destination=destination,
        compartment='Y',
        dep_date_start=dep_date_start,
        dep_date_end=dep_date_end,
        observation_date_start=observation_date_start,
        observation_date_end=observation_date_end,
        oneway_return=oneway_return
    )
    print json.dumps(response, indent=1)
