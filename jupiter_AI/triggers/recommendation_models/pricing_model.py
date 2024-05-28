"""
Author: Nikunj Agarwal
Created with <3
Start date of writing code: 30/01/2018
Code Functionality: Computes recommendation for a given market and dep_dates
    1. Reads the Pricing Models defined for this market from JUP_DB_Pricing_Model_Markets.
    2. Computes recommendation for every model that is defined for this market.
        This is read from JUP_DB_Pricing_Model

Collection Dependency List = JUP_DB_Pricing_Model_Markets
                             JUP_DB_Pricing_Model
                             JUP_DB_ATPCO_Fares_Rules
Modifications log:
    1. Author:
       Exact modification made or logic changed/added:
       Date of change:

"""

from jupiter_AI import client, JUPITER_DB, Host_Airline_Code
from jupiter_AI.network_level_params import SYSTEM_DATE
from jupiter_AI.triggers.recommendation_models.oligopoly_fl_ge import get_host_fares_df
from jupiter_AI.triggers.recommendation_models.oligopoly_fl_ge import main as oligopoly
from jupiter_AI.triggers.recommendation_models.oligopoly_fl_ge import get_sellups
from jupiter_AI.triggers.host_params_workflow_opt import get_od_distance
from copy import deepcopy
import pandas as pd
import time
import numpy as np
import datetime
import traceback

#db = client[JUPITER_DB]
HOST_AIRLINE_CODE = HOST_AIRLINE_CODE
EXCHANGE_RATE = {}
currency_crsr = list(db.JUP_DB_Exchange_Rate.find({}))
for curr in currency_crsr:
    EXCHANGE_RATE[curr['code']] = curr['Reference_Rate']


def get_seat_factor(pos, origin, destination, compartment, dep_date_start, dep_date_end):
    if 'DXB' in [origin, destination]:
        ods = [origin + destination]
    else:
        ods = [origin + 'DXB', 'DXB' + destination]
    seat_factor_cursor = db.JUP_DB_Market_Characteristics_Flights.aggregate([
        {
            '$match':
                {
                    'od': {'$in': ods},
                    'compartment': compartment,
                    'dep_date': {'$lte': dep_date_end, '$gte': dep_date_start}
                }
        },
        {
            '$sort': {'snap_date': -1}
        },
        {
            '$group':
                {
                    '_id':
                        {
                            'od': '$od',
                            'compartment': '$compartment',
                            'flight_number': '$flight_number',
                            'dep_date': '$dep_date'
                        },
                    'bookings': {'$first': '$bookings'},
                    'allocation': {'$first': '$allocation'}
                }
        },
        {
            '$group':
                {
                    '_id': None,
                    'bookings': {'$sum': '$bookings'},
                    'allocation': {'$sum': '$allocation'}
                }
        },
        {
            '$project':
                {
                    '_id': 0,
                    'seat_factor': {'$multiply': [{'$divide': ['$bookings', '$allocation']}, 100]}
                }
        }
    ])
    try:
        seat_factor = list(seat_factor_cursor)[0]['seat_factor']
    except:
        seat_factor = 0
    return seat_factor


def get_yield(pos, origin, destination, compartment, dep_date_start, dep_date_end):
    market_combined = pos + origin + destination + compartment
    od_distance = get_od_distance(origin + destination)
    yield_cursor = db.JUP_DB_Manual_Triggers_Module.aggregate([
        {
            '$match':
                {
                    'market_combined': market_combined,
                    'dep_date': {'$lte': dep_date_end, '$gte': dep_date_start},
                    '$sale_pax.value': {'$gt': 0},
                    '$sale_revenue.value': {'$gt': 0}
                }
        },
        {
            '$group':
                {
                    '_id': None,
                    'pax': {'$sum': '$sale_pax.value'},
                    'revenue': {'$sum': '$sale_revenue.value'}
                }
        },
        {
            '$project':
                {
                    '_id': 0,
                    'avg_fare': {'$divide': ['$revenue', '$pax']}
                }
        }
    ])
    try:
        yield_ = list(yield_cursor)[0]['avg_fare'] / od_distance
    except:
        yield_ = 0
    return yield_


def get_avg_fare(pos, origin, destination, compartment, dep_date_start, dep_date_end):
    market_combined = pos + origin + destination + compartment
    avg_fare_cursor = db.JUP_DB_Manual_Triggers_Module.aggregate([
        {
            '$match':
                {
                    'market_combined': market_combined,
                    'dep_date': {'$lte': dep_date_end, '$gte': dep_date_start},
                    '$sale_pax.value': {'$gt': 0},
                    '$sale_revenue.value': {'$gt': 0}
                }
        },
        {
            '$group':
                {
                    '_id': None,
                    'pax': {'$sum': '$sale_pax.value'},
                    'revenue': {'$sum': '$sale_revenue.value'}
                }
        },
        {
            '$project':
                {
                    '_id': 0,
                    'avg_fare': {'$divide': ['$revenue', '$pax']}
                }
        }
    ])
    try:
        avg_fare = list(avg_fare_cursor)[0]['avg_fare']
    except:
        avg_fare = 0
    return avg_fare


def get_revenue(pos, origin, destination, compartment, dep_date_start, dep_date_end):
    market_combined = pos + origin + destination + compartment
    revenue_cursor = db.JUP_DB_Manual_Triggers_Module.aggregate([
        {
            '$match':
                {
                    'market_combined': market_combined,
                    'dep_date': {'$lte': dep_date_end, '$gte': dep_date_start},
                    '$sale_revenue.value': {'$gt': 0}
                }
        },
        {
            '$group':
                {
                    '_id': None,
                    'revenue': {'$sum': '$sale_revenue.value'}
                }
        },
        {
            '$project':
                {
                    '_id': 0,
                    'revenue': '$revenue'
                }
        }
    ])
    try:
        revenue = list(revenue_cursor)[0]['revenue']
    except:
        revenue = 0
    return revenue


def get_pax(pos, origin, destination, compartment, dep_date_start, dep_date_end):
    market_combined = pos + origin + destination + compartment
    pax_cursor = db.JUP_DB_Manual_Triggers_Module.aggregate([
        {
            '$match':
                {
                    'market_combined': market_combined,
                    'dep_date': {'$lte': dep_date_end, '$gte': dep_date_start},
                    '$flown_pax.value': {'$gt': 0}
                }
        },
        {
            '$group':
                {
                    '_id': None,
                    'pax': {'$sum': '$flown_pax.value'}
                }
        },
        {
            '$project':
                {
                    '_id': 0,
                    'pax': '$pax'
                }
        }
    ])
    try:
        pax = list(pax_cursor)[0]['pax']
    except:
        pax = 0
    return pax


def get_booking(pos, origin, destination, compartment, dep_date_start, dep_date_end):
    market_combined = pos + origin + destination + compartment
    booking_cursor = db.JUP_DB_Manual_Triggers_Module.aggregate([
        {
            '$match':
                {
                    'market_combined': market_combined,
                    'dep_date': {'$lte': dep_date_end, '$gte': dep_date_start},
                    'book_pax.value': {'$gt': 0}
                }
        },
        {
            '$group':
                {
                    '_id': None,
                    'booking': {'$sum': '$book_pax.value'}
                }
        },
        {
            '$project':
                {
                    '_id': 0,
                    'booking': '$booking'
                }
        }
    ])
    try:
        booking = list(booking_cursor)[0]['booking']
    except:
        booking = 0
    return booking


def get_market_share(pos, origin, destination, compartment, dep_date_start, dep_date_end):
    market = pos + origin + destination + compartment
    year_start = dep_date_start[:4]
    month_start = dep_date_start[5:7]
    combine_column_1 = year_start + str(int(month_start)) + market
    year_end = dep_date_end[:4]
    month_end = dep_date_end[5:7]
    combine_column_2 = year_end + str(int(month_end)) + market
    market_share_cursor = db.JUP_DB_Pos_OD_Compartment_new.aggregate([
        {
            '$match':
                {
                    'combine_column': {'$in': [combine_column_1, combine_column_2]}
                }
        },
        {
            '$unwind': '$top_5_comp_data'
        },
        {
            '$match':
                {
                    'top_5_comp_data.airline': HOST_AIRLINE_CODE,
                    'top_5_comp_data.pax': {'$gt': 0}
                }
        },
        {
            '$project':
                {
                    'airline': '$top_5_comp_data.airline',
                    'pax': '$top_5_comp_data.pax',
                    'market_size': '$top_5_comp_data.market_size'
                }
        },
        {
            '$group':
                {
                    '_id': None,
                    'pax': {'$sum': '$pax'},
                    'market_size': {'$sum': '$market_size'}
                }
        },
        {
            '$project':
                {
                    '_id': 0,
                    'market_share':
                        {
                            '$cond':
                                [
                                    {'$gt': ['$market_size', 0]},
                                    {'$multiply': [{'$divide': ['$pax', '$market_size']}, 100.0]},
                                    0
                                ]
                        }
                }
        }
    ])
    try:
        market_share = list(market_share_cursor)[0]['market_share']
    except:
        market_share = 0
    return market_share


def get_market_elasticity(pos, origin, destination, compartment, dep_date_start, dep_date_end):
    market_combined = pos + origin + destination + compartment
    elasticity_cursor = db.JUP_DB_Manual_Triggers_Module.aggregate([
        {
            '$match':
                {
                    'market_combined': market_combined,
                    'dep_date': {'$lte': dep_date_end, '$gte': dep_date_start}
                }
        },
        {
            '$unwind': '$sale_farebasis'
        },
        {
            '$match':
                {
                    'sale_farebasis.rev': {'$gt': 0},
                    'sale_farebasis.pax': {'$gt': 0}
                }
        },
        {
            '$group':
                {
                    '_id':
                        {
                            'farebasis': '$sale_farebasis.fare_basis',
                            'dep_date': '$dep_date'
                        },
                    'revenue': {'$sum': '$sale_farebasis.rev'},
                    'pax': {'$sum': '$sale_farebasis.pax'}
                }
        },
        {
            '$project':
                {
                    '_id': 0,
                    'farebasis': '$_id.farebasis',
                    'dep_date': '$_id.dep_date',
                    'revenue': '$revenue',
                    'pax': '$pax',
                    'price': {'$divide': ['$revenue', '$pax']}
                }
        }
    ])
    elasticity_df = pd.DataFrame(list(elasticity_cursor))
    if len(elasticity_df) > 0:
        elasticity_df['x'] = np.log(elasticity_df.price)
        elasticity_df['y'] = np.log(elasticity_df.pax)
        elasticity_df['xy'] = elasticity_df['x'] * elasticity_df['y']
        elasticity_df['sq_x'] = elasticity_df['x'] * elasticity_df['x']
        n = len(elasticity_df)
        a = n * elasticity_df['xy'].sum()
        b = elasticity_df['x'].sum() * elasticity_df['y'].sum()
        c = n * elasticity_df['sq_x'].sum()
        d = elasticity_df['x'].sum() * elasticity_df['x'].sum()
        elasticity = (a - b) / (c - d)
    else:
        elasticity = np.nan
    return elasticity


def get_pricing_models(pos, origin, destination, compartment):
    pos_cursor = db.JUP_DB_Region_Master.aggregate([
        {
            '$match':
                {
                    'POS_CD': pos
                }
        },
        {'$project':
            {
                '_id': 0,
                'city': '$POS_CD',
                'country': '$COUNTRY_CD',
                'region': '$Region',
                'cluster': '$Cluster',
                'netwrok': '$Network'
            }
        }
    ])
    pos_list = list(pos_cursor)[0].values()
    origin_cursor = db.JUP_DB_Region_Master.aggregate([
        {
            '$match':
                {
                    'POS_CD': origin
                }
        },
        {'$project':
            {
                '_id': 0,
                'city': '$POS_CD',
                'country': '$COUNTRY_CD',
                'region': '$Region',
                'cluster': '$Cluster',
                'netwrok': '$Network'
            }
        }
    ])
    origin_list = list(origin_cursor)[0].values()
    destination_cursor = db.JUP_DB_Region_Master.aggregate([
        {
            '$match':
                {
                    'POS_CD': destination
                }
        },
        {'$project':
            {
                '_id': 0,
                'city': '$POS_CD',
                'country': '$COUNTRY_CD',
                'region': '$Region',
                'cluster': '$Cluster',
                'netwrok': '$Network'
            }
        }
    ])
    destination_list = list(destination_cursor)[0].values()
    compartment_list = [compartment, 'all']
    pricing_models_cursor = db.JUP_DB_Pricing_Model_Markets.aggregate([
        {
            '$match':
                {
                    'pos.value': {'$in': pos_list},
                    'origin.value': {'$in': origin_list},
                    'destination.value': {'$in': destination_list},
                    'compartment.value': {'$in': compartment_list}
                }
        },
        {
            '$sort': {'priority': -1}
        },
        {
            '$limit': 1
        },
        {
            "$project":
                {
                    "_id": 0,
                    "model": 1
                }
        }
    ])
    pricing_models = list(pricing_models_cursor)[0]['model']
    return pricing_models


def get_models_definitions(pricing_models):
    models_definitions_cursor = db.JUP_DB_Pricing_Model.find({'model_code': {'$in': pricing_models}})
    models_definitions = list(models_definitions_cursor)
    return models_definitions


def get_host_fares_filtered(host_fares_filtered, primary_filter):
    if primary_filter['rbd']:
        host_fares_filtered = host_fares_filtered[host_fares_filtered['RBD'] == primary_filter['rbd']]
    elif primary_filter['channel']:
        host_fares_filtered = host_fares_filtered[host_fares_filtered['channel'] == primary_filter['channel']]
    elif primary_filter['fare_brand']:
        host_fares_filtered = host_fares_filtered[host_fares_filtered['Fare_brand'] == primary_filter['fare_brand']]
    elif primary_filter['oneway_return']:
        host_fares_filtered = host_fares_filtered[host_fares_filtered['oneway_return'] == str(primary_filter['oneway_return'])]
    elif primary_filter['DOW']:
        host_fares_filtered = host_fares_filtered[host_fares_filtered['DOW'] == primary_filter['DOW']]
    elif primary_filter['time_of_day']:
        host_fares_filtered = host_fares_filtered[host_fares_filtered['time_of_day'] == primary_filter['time_of_day']]
    elif primary_filter['flight_time']:
        host_fares_filtered = host_fares_filtered[host_fares_filtered['flight_time'] == primary_filter['flight_time']]
    host_fares_filtered = host_fares_filtered.sort_values(['fare']).head(1)
    return host_fares_filtered


def get_competitor_match_reco(primary_fare, primary_criteria):
    primary_fare['reco_fare_temp'] = primary_fare['fare']
    if (primary_fare['competitor_farebasis']) and (len(primary_fare['competitor_farebasis'])) > 0:
        competitor_fare_basis = pd.DataFrame(primary_fare['competitor_farebasis'])
        competitor_match = primary_criteria['competitor_match']
        if len(competitor_match) > 0:
            fz_currency = primary_fare['currency'].values[0]
            if primary_fare['oneway_return'] in [1, '1']:
                ow_rt = 'ow'
            else:
                ow_rt = 'rt'
            for competitor in competitor_match:
                carrier = competitor['carrier']
                if carrier in list(competitor_fare_basis['carrier']):
                    if competitor_match[ow_rt]['threshold_type'] == 'P':
                        host_upper_threshold_temp = competitor_fare_basis[competitor_fare_basis['carrier'] == carrier]['fare'] * (1 + competitor_match[ow_rt]['threshold_upper'] / 100.0)
                        host_lower_threshold_temp = competitor_fare_basis[competitor_fare_basis['carrier'] == carrier]['fare'] * (1 + competitor_match[ow_rt]['threshold_lower'] / 100.0)
                    else:
                        host_upper_threshold_temp = competitor_fare_basis[competitor_fare_basis['carrier'] == carrier]['fare'] + competitor_match[ow_rt]['threshold_upper'] * EXCHANGE_RATE[competitor_match[ow_rt]['currency']] / EXCHANGE_RATE[fz_currency]
                        host_lower_threshold_temp = competitor_fare_basis[competitor_fare_basis['carrier'] == carrier]['fare'] + competitor_match[ow_rt]['threshold_lower'] * EXCHANGE_RATE[competitor_match[ow_rt]['currency']] / EXCHANGE_RATE[fz_currency]
                    try:
                        if host_upper_threshold_temp < host_upper_threshold:
                            host_upper_threshold = host_lower_threshold_temp
                    except:
                        host_upper_threshold = host_upper_threshold_temp
                    try:
                        if host_lower_threshold_temp < host_lower_threshold:
                            host_lower_threshold = host_lower_threshold_temp
                    except:
                        host_lower_threshold = host_lower_threshold_temp
                else:  # This carrier is not present in competitor_farebasis for primary fare
                    pass
            reco_fare_temp = min(host_lower_threshold, host_upper_threshold)
            primary_fare['reco_fare_temp'] = reco_fare_temp
        else:  # 'competitor_match' is defined but empty array. Ideally, this should not happen.
            pass
    else:  # primary_fare does not have any competitors mapped
        pass
    return primary_fare


def get_volume_reco(primary_fare, primary_criteria, pos, origin, destination, compartment, dep_date_start, dep_date_end):
    volume = primary_criteria['volume']
    volume_premium = 0
    if len(volume) > 0:
        fz_currency = primary_fare['currency'].values[0]
        for param in volume:
            if param['parameter'] == 'pax':
                pax = get_pax(pos, origin, destination, compartment, dep_date_start, dep_date_end)
                if param['lower_threshold']:
                    lower_threshold = param['lower_threshold']
                else:
                    lower_threshold = 0
                if param['upper_threshold']:
                    upper_threshold = param['upper_threshold']
                else:
                    upper_threshold = 999999999
                if (pax > lower_threshold) and (pax < upper_threshold):
                    if primary_fare['oneway_return'] in [1, '1']:
                        ow_rt = 'ow'
                    else:
                        ow_rt = 'rt'
                    if param[ow_rt]['premium_type'] == 'P':
                        volume_premium += primary_fare['fare'] * (param[ow_rt]['premium_value'] / 100.0)
                    else:  # premium_type = Absolute
                        volume_premium += primary_fare['fare'] + (param[ow_rt]['premium_value'] * EXCHANGE_RATE[param[ow_rt]['currency']] / EXCHANGE_RATE[fz_currency])
                else:  # pax of Market is not within thresholds. So, no premium.
                    continue
            elif param['parameter'] == 'booking':
                booking = get_booking(pos, origin, destination, compartment, dep_date_start, dep_date_end)
                if param['lower_threshold']:
                    lower_threshold = param['lower_threshold']
                else:
                    lower_threshold = 0
                if param['upper_threshold']:
                    upper_threshold = param['upper_threshold']
                else:
                    upper_threshold = 999999999
                if (booking > lower_threshold) and (booking < upper_threshold):
                    if primary_fare['oneway_return'] in [1, '1']:
                        ow_rt = 'ow'
                    else:
                        ow_rt = 'rt'
                    if param[ow_rt]['premium_type'] == 'P':
                        volume_premium += primary_fare['fare'] * (param[ow_rt]['premium_value'] / 100.0)
                    else:  # premium_type = Absolute
                        volume_premium += primary_fare['fare'] + (param[ow_rt]['premium_value'] * EXCHANGE_RATE[param[ow_rt]['currency']] / EXCHANGE_RATE[fz_currency])
                else:  # booking of Market is not within thresholds. So, no premium.
                    continue
            elif param['parameter'] == 'market_share':
                market_share = get_market_share(pos, origin, destination, compartment, dep_date_start, dep_date_end)
                if param['lower_threshold']:
                    lower_threshold = param['lower_threshold']
                else:
                    lower_threshold = 0
                if param['upper_threshold']:
                    upper_threshold = param['upper_threshold']
                else:
                    upper_threshold = 999999999
                if (market_share > lower_threshold) and (market_share < upper_threshold):
                    if primary_fare['oneway_return'] in [1, '1']:
                        ow_rt = 'ow'
                    else:
                        ow_rt = 'rt'
                    if param[ow_rt]['premium_type'] == 'P':
                        volume_premium += primary_fare['fare'] * (param[ow_rt]['premium_value'] / 100.0)
                    else:  # premium_type = Absolute
                        volume_premium += primary_fare['fare'] + (param[ow_rt]['premium_value'] * EXCHANGE_RATE[param[ow_rt]['currency']] / EXCHANGE_RATE[fz_currency])
                else:  # market_share of Market is not within thresholds. So, no premium.
                    continue
            else:  # Volume parameter other than pax/booking/market_share. Ideally, this should not happen
                pass
    else:  # 'volume' is defined but empty array. Ideally, this should not happen.
        pass
    try:
        primary_fare['reco_fare_temp'] += volume_premium
    except:
        primary_fare['reco_fare_temp'] = primary_fare['fare'] + volume_premium
    return primary_fare


def get_value_reco(primary_fare, primary_criteria, pos, origin, destination, compartment, dep_date_start, dep_date_end):
    value = primary_criteria['value']
    value_premium = 0
    if len(value) > 0:
        fz_currency = primary_fare['currency'].values[0]
        for param in value:
            if param['parameter'] == 'revenue':
                revenue = get_revenue(pos, origin, destination, compartment, dep_date_start, dep_date_end)
                if param['lower_threshold']:
                    lower_threshold = param['lower_threshold']
                else:
                    lower_threshold = 0
                if param['upper_threshold']:
                    upper_threshold = param['upper_threshold']
                else:
                    upper_threshold = 999999999
                if (revenue > lower_threshold) and (revenue < upper_threshold):
                    if primary_fare['oneway_return'] in [1, '1']:
                        ow_rt = 'ow'
                    else:
                        ow_rt = 'rt'
                    if param[ow_rt]['premium_type'] == 'P':
                        value_premium += primary_fare['fare'] * (param[ow_rt]['premium_value'] / 100.0)
                    else:  # premium_type = Absolute
                        value_premium += primary_fare['fare'] + (param[ow_rt]['premium_value'] * EXCHANGE_RATE[param[ow_rt]['currency']] / EXCHANGE_RATE[fz_currency])
                else:  # pax of Market is not within thresholds. So, no premium.
                    continue
            elif param['parameter'] == 'yield':
                yield_ = get_yield(pos, origin, destination, compartment, dep_date_start, dep_date_end)
                if param['lower_threshold']:
                    lower_threshold = param['lower_threshold']
                else:
                    lower_threshold = 0
                if param['upper_threshold']:
                    upper_threshold = param['upper_threshold']
                else:
                    upper_threshold = 999999999
                if (yield_ > lower_threshold) and (yield_ < upper_threshold):
                    if primary_fare['oneway_return'] in [1, '1']:
                        ow_rt = 'ow'
                    else:
                        ow_rt = 'rt'
                    if param[ow_rt]['premium_type'] == 'P':
                        value_premium += primary_fare['fare'] * (param[ow_rt]['premium_value'] / 100.0)
                    else:  # premium_type = Absolute
                        value_premium += primary_fare['fare'] + (param[ow_rt]['premium_value'] * EXCHANGE_RATE[param[ow_rt]['currency']] / EXCHANGE_RATE[fz_currency])
                else:  # yield_ of Market is not within thresholds. So, no premium.
                    continue
            elif param['parameter'] == 'average_fare':
                avg_fare = get_avg_fare(pos, origin, destination, compartment, dep_date_start, dep_date_end)
                if param['lower_threshold']:
                    lower_threshold = param['lower_threshold']
                else:
                    lower_threshold = 0
                if param['upper_threshold']:
                    upper_threshold = param['upper_threshold']
                else:
                    upper_threshold = 999999999
                if (avg_fare > lower_threshold) and (avg_fare < upper_threshold):
                    if primary_fare['oneway_return'] in [1, '1']:
                        ow_rt = 'ow'
                    else:
                        ow_rt = 'rt'
                    if param[ow_rt]['premium_type'] == 'P':
                        value_premium += primary_fare['fare'] * (param[ow_rt]['premium_value'] / 100.0)
                    else:  # premium_type = Absolute
                        value_premium += primary_fare['fare'] + (param[ow_rt]['premium_value'] * EXCHANGE_RATE[param[ow_rt]['currency']] / EXCHANGE_RATE[fz_currency])
                else:  # avg_fare of Market is not within thresholds. So, no premium.
                    continue
            else:  # 'value' parameter other than revenue/yield/average_fare. Ideally, this should not happen.
                pass
    else:  # 'value' is defined but empty array. Ideally, this should not happen.
        pass
    try:
        primary_fare['reco_fare_temp'] += value_premium
    except:
        primary_fare['reco_fare_temp'] = primary_fare['fare'] + value_premium
    return primary_fare


def get_seat_factor_reco(primary_fare, primary_criteria, pos, origin, destination, compartment, dep_date_start, dep_date_end):
    sf_criteria = primary_criteria['seat_factor']
    sf_premium = 0
    fz_currency = primary_fare['currency'].values[0]
    seat_factor = get_seat_factor(pos, origin, destination, compartment, dep_date_start, dep_date_end)
    if sf_criteria['lower_threshold']:
        lower_threshold = sf_criteria['lower_threshold']
    else:
        lower_threshold = 0
    if sf_criteria['upper_threshold']:
        upper_threshold = sf_criteria['upper_threshold']
    else:
        upper_threshold = 999999999
    if (seat_factor > lower_threshold) and (seat_factor < upper_threshold):
        if primary_fare['oneway_return'] in [1, '1']:
            ow_rt = 'ow'
        else:
            ow_rt = 'rt'
        if sf_criteria[ow_rt]['premium_type'] == 'P':
            sf_premium += primary_fare['fare'] * (sf_criteria[ow_rt]['premium_value'] / 100.0)
        else:  # premium_type = Absolute
            sf_premium += primary_fare['fare'] + (sf_criteria[ow_rt]['premium_value'] * EXCHANGE_RATE[sf_criteria[ow_rt]['currency']] / EXCHANGE_RATE[fz_currency])
    else:  # seat_factor of Market is not within thresholds. So, no premium.
        pass
    try:
        primary_fare['reco_fare_temp'] += sf_premium
    except:
        primary_fare['reco_fare_temp'] = primary_fare['fare'] + sf_premium
    return primary_fare


def get_elasticity_reco(primary_fare, primary_criteria, pos, origin, destination, compartment, dep_date_start, dep_date_end):
    elasticity_criteria = primary_criteria['elasticity']
    elasticity_premium = 0
    fz_currency = primary_fare['currency'].values[0]
    elasticity = get_market_elasticity(pos, origin, destination, compartment, dep_date_start, dep_date_end)
    if elasticity_criteria['lower_threshold']:
        lower_threshold = elasticity_criteria['lower_threshold']
    else:
        lower_threshold = 0
    if elasticity_criteria['upper_threshold']:
        upper_threshold = elasticity_criteria['upper_threshold']
    else:
        upper_threshold = 999999999
    if (elasticity > lower_threshold) and (elasticity < upper_threshold):
        if primary_fare['oneway_return'] in [1, '1']:
            ow_rt = 'ow'
        else:
            ow_rt = 'rt'
        if elasticity_criteria[ow_rt]['premium_type'] == 'P':
            elasticity_premium += primary_fare['fare'] * (elasticity_criteria[ow_rt]['premium_value'] / 100.0)
        else:  # premium_type = Absolute
            elasticity_premium += primary_fare['fare'] + (elasticity_criteria[ow_rt]['premium_value'] * EXCHANGE_RATE[elasticity_criteria[ow_rt]['currency']] / EXCHANGE_RATE[fz_currency])
    else:  # Elasticity of Market is not within thresholds. So, no premium.
        pass
    try:
        primary_fare['reco_fare_temp'] += elasticity_premium
    except:
        primary_fare['reco_fare_temp'] = primary_fare['fare'] + elasticity_premium
    return primary_fare


def diffuse_oneway_return(host_fares, oneway_return_dict, primary_fare):
    oneway_return_df = pd.DataFrame(oneway_return_dict)
    base_ow_rt = primary_fare['oneway_return']
    base_channel = primary_fare['channel']
    base_rbd = primary_fare['RBD']
    base_fare_brand = primary_fare['fare_brand']

    for iter in oneway_return_df.iterrows():
        host_fares_temp = host_fares[host_fares['']]
    return host_fares


def get_recommendation_for_primary_fare(primary_fare, primary_criteria, pos, origin, destination, compartment, dep_date_start, dep_date_end):
    competitor_match_flag = False
    if primary_criteria['competitor_match']:
        primary_fare = get_competitor_match_reco(primary_fare, primary_criteria)
        competitor_match_flag = True
    if competitor_match_flag:
        if primary_criteria['volume']:
            primary_fare = get_volume_reco(primary_fare, primary_criteria, pos, origin, destination, compartment, dep_date_start, dep_date_end)
        if primary_criteria['value']:
            primary_fare = get_value_reco(primary_fare, primary_criteria, pos, origin, destination, compartment, dep_date_start, dep_date_end)
        if primary_criteria['seat_factor']:
            primary_fare = get_seat_factor_reco(primary_fare, primary_criteria, pos, origin, destination, compartment, dep_date_start, dep_date_end)
        if primary_criteria['elasticity']:
            primary_fare = get_elasticity_reco(primary_fare, primary_criteria, pos, origin, destination, compartment, dep_date_start, dep_date_end)
    return primary_fare


def get_diffused_fares(host_fares, diffuse, primary_fare):
    if diffuse['oneway_return']:
        host_fares = diffuse_oneway_return(host_fares, diffuse['oneway_return'], primary_fare)
    if diffuse['RBD']:
        host_fares = diffuse_rbd(host_fares, diffuse['RBD'])
    if diffuse['channel']:
        host_fares = diffuse_channel(host_fares, diffuse['channel'])
    if diffuse['fare_brand']:
        host_fares = diffuse_fare_brand(host_fares, diffuse['fare_brand'])
    if diffuse['fare_basis']:
        host_fares = diffuse_fare_basis(host_fares, diffuse['fare_basis'])
    if diffuse['flight_time']:
        host_fares = diffuse_flight_time(host_fares, diffuse['flight_time'])
    if diffuse['DOW']:
        host_fares = diffuse_DOW(host_fares, diffuse['DOW'])
    if diffuse['time_of_day']:
        host_fares = diffuse_TOD(host_fares, diffuse['time_of_day'])
    return host_fares


def apply_event_rules(host_fares, event_rules, origin, destination, dep_date_start, dep_date_end):
    od = origin + destination
    start_date_key = 'Start_date_' + SYSTEM_DATE[:4]
    end_date_key = 'End_date_' + SYSTEM_DATE[:4]
    for event in event_rules:
        event_cursor = db.JUP_DB_Pricing_Calendar.find(
            {
                'Market': od,
                start_date_key: {'$lte': dep_date_end},
                end_date_key: {'$gte': dep_date_start}
            },
            {
                '_id': 0,
                'Holiday_Name': 1
            }
        )
        if event['event_name']
        if event['dep_date_from'] >


def apply_general_rules(host_fares, general_rules, pos, origin, destination, compartment, dep_date_start, dep_date_end):
    try:
        event_rules = general_rules['events']
    except:
        event_rules = None
    try:
        dep_date_rules = general_rules['dep_dates']
    except:
        dep_date_rules = None
    try:
        logical_rules = general_rules['logical_rules']
    except:
        logical_rules = None
    if event_rules:
        host_fares = apply_event_rules(host_fares, event_rules, origin, destination, dep_date_start, dep_date_end)
    return host_fares


def main(pos, origin, destination, compartment, dep_date_start, dep_date_end):
    print "Getting Pricing Models defined for this Market . . ."
    pricing_models = get_pricing_models(pos, origin, destination, compartment)
    total_num_models = len(pricing_models)
    model_reco_count = 0
    print "Number of Pricing Models defined for this market = ", total_num_models
    print "Pricing Models defined for this market = ", pricing_models
    models_definitions = get_models_definitions(pricing_models)
    host_fares = get_host_fares_df(pos, origin, destination, compartment, dep_date_start, dep_date_end)
    for model in models_definitions:
        model_effectivity = (model['effective_date_from'] < SYSTEM_DATE) and (model['effective_date_to'] > SYSTEM_DATE)
        if model_effectivity:
            model_reco_count += 1
            host_fares_temp = deepcopy(host_fares)
            try:
                model_code = model['model_code']
            except KeyError:
                model_code = 'No model code available'
            try:
                model_name = model['model_name']
            except KeyError:
                model_name = 'No model name available'
            try:
                model_description = model['model_description']
            except KeyError:
                model_description = 'No model description available'
            try:
                primary_filter = model['primary_criteria']['filter']
            except KeyError:
                primary_filter = None
            if primary_filter:
                primary_fare = get_host_fares_filtered(host_fares_temp, primary_filter)
                primary_fare = get_recommendation_for_primary_fare(primary_fare, model['primary_criteria'], pos, origin, destination, compartment, dep_date_start, dep_date_end)
                host_fares_temp = pd.merge(host_fares_temp, primary_fare, on=['fare_basis'], how='left')
                try:
                    diffuse = model['secondary_criteria']['diffuse']
                except:
                    diffuse = None
                if len(host_fares_temp[host_fares_temp['reco_fare_temp'] > 0] > 0):
                    if diffuse:
                        host_fares_temp = get_diffused_fares(host_fares_temp, diffuse, primary_fare)
                        host_fares_temp['reco_fare_temp'].fillna(-999)
                        host_fares_temp_ow = host_fares_temp[host_fares_temp['oneway_return'].isin(1, '1')]
                        host_fares_temp_rt = host_fares_temp[host_fares_temp['oneway_return'].isin(2, '2')]
                        host_fares_temp_ow = get_sellups(host_fares_temp_ow)
                        host_fares_temp_rt = get_sellups(host_fares_temp_rt)
                        host_fares_temp = pd.concat([host_fares_temp_ow, host_fares_temp_rt]).reset_index(drop=True).sort_values('fare')
                    else:
                        host_fares_temp = get_sellups(host_fares_temp)
                else:  # No primary fare recommendation found. So cannot recommend any fare based on model defined.
                    #  This should not happen ideally.
                    host_fares_temp = oligopoly(pos, origin, destination, compartment, dep_date_start, dep_date_end)
            else:  # Primary Fare cannot be found because filter is not defined. Recommend by Oligopoly_fl_ge.py
                host_fares_temp = oligopoly(pos, origin, destination, compartment, dep_date_start, dep_date_end)
            try:
                general_rules = model['general_rules']
            except:
                general_rules = None
            if general_rules:
                host_fares_temp = apply_general_rules(host_fares_temp, general_rules, pos, origin, destination, compartment, dep_date_start, dep_date_end)
        else:  # model is not effective. So, Skip it.
            continue
    return host_fares


if __name__ == '__main__':
    pos = 'AMM'
    origin = 'AMM'
    destination = 'DXB'
    compartment = 'Y'
    dep_date_start = '2017-07-01'
    dep_date_end = '2017-10-01'
    get_market_elasticity(pos, origin, destination, compartment, dep_date_start, dep_date_end)

