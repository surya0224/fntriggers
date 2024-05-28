"""
Author: Nikunj Agarwal
Created with <3
End date of developement: 2017-11-01
Code functionality:
     Updates collection JUP_DB_Pos_OD_Compartment once every month when data from PaxIS and OAG
     is received. Uses base collections - Market Share, Host_OD_Capacity, OD_Capacity and
     Competitor_Ratings.
     Calculates and stores parameters like:
        Market_Share, FMS, Market_Share_1, Rating, Capacity for every Market/month/year/Airline

Modifications log:
    1. Author: Nikunj Agarwal
       Exact modification made or some logic changed:
        Added pseudo_od to get market share at POS level.
        Added 'market' column = pos + od + compartment. This will help query in market_share_trend triggers
        Added 'month_year' column. This will also help query in market_share_trend trigger
       Date of modification: 29/01/2018
    2. Author:
       Exact modification made or some logic changed:
       Date of modification:
    3. Author:
       Exact modification made or some logic changed:
       Date of modification:

"""

import pandas as pd
import time
import datetime
from datetime import timedelta
from jupiter_AI import mongo_client, JUPITER_DB, JUPITER_LOGGER
from bson.objectid import ObjectId
from jupiter_AI.logutils import measure
from jupiter_AI.network_level_params import SYSTEM_DATE, Host_Airline_Code, Host_Airline_Hub
import traceback
from jupiter_AI.triggers.common import cursor_to_df

# import logging

# logging.basicConfig(filename='POS_OD_C_2.log', level=logging.DEBUG)

random_origin = 'CPH'
random_destination = 'BKK'
random_compartment = 'Y'

HOST_AIRLINE = Host_Airline_Code

COLLECTION_UPDATE_LIST = ['JUP_DB_Pos_OD_Compartment']
COLLECTION_DEPENDENCY_LIST = ['JUP_DB_Market_Share', 'JUP_DB_Competitor_Ratings', 'JUP_DB_Host_OD_Capacity',
                              'JUP_DB_OD_Capacity']
ERROR_FLAG = 0
TOTAL_MARKETS = 0
START_TIME = time.time()
this_year = datetime.datetime.strptime(SYSTEM_DATE, '%Y-%m-%d').year
this_month = datetime.datetime.strptime(SYSTEM_DATE, '%Y-%m-%d').month
last_year = this_year - 1
if this_month < 10:
    this_month_year = str(this_year) + '0' + str(this_month)
else:
    this_month_year = str(this_year) + str(this_month)


@measure(JUPITER_LOGGER)
def str_month(month):
    if month < 10:
        return '0' + str(month)
    else:
        return str(month)


@measure(JUPITER_LOGGER)
def get_market_share_df(snap_date, months, db):
    market_share_cursor = db.JUP_DB_Market_Share.aggregate([
        {
            '$match':
                {
                    # "year": {'$gte': this_year},
                    "snap_date": snap_date,
                    "month": {'$in': months}
                    # "year": {'$gte': last_year}
                    # "pos": {'$in': ['CGP', 'CMB', 'DAC', 'AMM']},
                    # "destination": {'$in': ['KWI', 'UAE', 'AMM']},
                    # "origin": {'$in': ['CMB', 'DAC', 'AMM', 'KTM', 'CGP', 'CMB']},
                    # "compartment": 'Y'
                }
        },
        {
            '$group':
                {
                    '_id':
                        {
                            'pos': '$pos',
                            'origin': '$pseudo_origin',
                            'destination': '$pseudo_destination',
                            'compartment': '$compartment',
                            'month': "$month",
                            'year': "$year",
                            # 'snap_month': '$snap_date'
                        },
                    'market_size': {'$first': '$market_size'},
                    'docs': {"$push": {"airline": "$MarketingCarrier1", "pax": "$pax"}},
                    'original_origin':
                        {
                            '$addToSet': '$origin',
                        },
                    'original_destination':
                        {
                            '$addToSet': '$destination',
                        }
                }
        },
        {
            "$group":
                {
                    "_id":
                        {
                            'pos': '$_id.pos',
                            'origin': '$_id.origin',
                            'destination': '$_id.destination',
                            'compartment': '$_id.compartment',
                            'month': "$_id.month",
                            'year': "$_id.year"
                        },
                    "market_size": {"$sum": "$market_size"},
                    "docs_1": {"$push": "$docs"},
                    'original_origin': {"$addToSet": "$original_origin"},
                    'original_destination': {"$addToSet": "$original_destination"}
                }
        },
        {
            "$unwind": "$docs_1"
        },
        {
            "$unwind": "$docs_1"
        },
        {
            "$group":
                {
                    "_id":
                        {
                            'pos': '$_id.pos',
                            'origin': '$_id.origin',
                            'destination': '$_id.destination',
                            'compartment': '$_id.compartment',
                            'month': "$_id.month",
                            'year': "$_id.year",
                            "airline": "$docs_1.airline"
                        },
                    "pax": {"$sum": "$docs_1.pax"},
                    "market_size": {"$avg": "$market_size"},
                    'original_origin': {"$addToSet": "$original_origin"},
                    'original_destination': {"$addToSet": "$original_destination"}
                }
        },
        {
            "$unwind": "$original_origin"
        },
        {
            "$unwind": "$original_destination"
        },
        {
            "$unwind": "$original_origin"
        },
        {
            "$unwind": "$original_destination"
        },
        {
            "$unwind": "$original_origin"
        },
        {
            "$unwind": "$original_destination"
        },
        {
            "$group": {
                "_id": {"pos": "$_id.pos", "origin": "$_id.origin", "destination": "$_id.destination",
                        "month": "$_id.month", "year": "$_id.year", "compartment": "$_id.compartment",
                        "airline": "$_id.airline", "original_origin": "$original_origin",
                        "original_destination": "$original_destination"},
                "pax": {"$first": "$pax"},
                "market_size": {"$first": "$market_size"}
            }
        },
        {
            '$project':
                {
                    '_id': 0,
                    'pos': '$_id.pos',
                    'origin': '$_id.original_origin',
                    'destination': '$_id.original_destination',
                    'compartment': '$_id.compartment',
                    'airline': '$_id.airline',
                    'month': "$_id.month",
                    'year': "$_id.year",
                    'pax': '$pax',
                    'market_size': '$market_size',
                    'market_share': {
                        "$cond": {"if": {"$gt": ["$market_size", 0]},
                                  "then": {"$multiply": [100, {"$divide": ["$pax", "$market_size"]}]},
                                  "else": "NA"}}
                }
        },
        {
            '$sort':
                {
                    'market_share': -1
                }
        }
    ], allowDiskUse=True)
    market_share_df = cursor_to_df(market_share_cursor)
    market_share_df['od'] = market_share_df['origin'] + market_share_df['destination']
    return market_share_df


@measure(JUPITER_LOGGER)
def get_ratings_df(db):
    rating_cursor = db.JUP_DB_Competitor_Ratings_Old.aggregate([
        # {
        #     '$match':
        #         {
        #             'od': {'$in': ['CPHBKK']}
        #         }
        # },
        {
            '$project':
                {
                    '_id': 0,
                    'origin': '$origin',
                    'destination': '$destination',
                    'compartment': '$compartment',
                    'airline': '$Airline Rating_ratings'
                }
        }
    ], allowDiskUse=True)
    rating_df = cursor_to_df(rating_cursor)
    ratings_dict = []
    for index, market in rating_df.iterrows():
        market_airlines = market['airline'].keys()
        market_ratings = market['airline'].values()
        for i in range(len(market_airlines)):
            ratings_dict.append(
                {'origin': market['origin'], 'destination': market['destination'], 'compartment': market['compartment'],
                 'airline': market_airlines[i],
                 'rating': market_ratings[i]})

    ratings_df = pd.DataFrame(ratings_dict)
    ratings_df['od'] = ratings_df['origin'] + ratings_df['destination']

    return ratings_df


@measure(JUPITER_LOGGER)
def get_ratings_df_temp_product(db):
    product_ratings_cursor = db.JUP_DB_Data_Competitor_Ratings.find(
        {'group': 'Product Rating', 'compartment': {'$in': ['Y']}}
    )
    pr_df = cursor_to_df(product_ratings_cursor)
    pr_df.drop(['_id', 'origin', 'destination', 'component', 'last_update_date_gmt', 'group', 'compartment'], axis=1,
               inplace=True)
    final_df = pd.DataFrame()
    for index, row in pr_df.iterrows():
        airlines = list(row['ratings'].keys())
        ratings = list(row['ratings'].values())
        temp_df = pd.DataFrame({'airline': airlines, 'rating': ratings})
        final_df = pd.concat([final_df, temp_df])
    product_ratings_df = pd.DataFrame(final_df.groupby(by=['airline'], as_index=False)['rating'].mean())
    rt_df = get_ratings_components(db)  # to add individual component ratings
    ratings_df = product_ratings_df.merge(rt_df.rename(columns={'Airline': 'airline'}), on='airline', how='outer')
    ratings_df.fillna(5, inplace=True)
    return ratings_df


@measure(JUPITER_LOGGER)
def get_ratings_components(db):
    ratings_cursor = db.JUP_DB_Rating_ALL.find({}, {'_id': 0})
    rt_df = cursor_to_df(ratings_cursor)
    return rt_df


@measure(JUPITER_LOGGER)
def get_host_capacity_df(months, db):
    cursor_host_capacity = db.JUP_DB_Host_OD_Capacity.aggregate([
        {
            '$match':
                {
                    # 'od': {'$in': ['CMBKWI', 'DACAMM', 'KTMDXB', 'AMMDXB', 'CGPDXB', 'CMBKWI', 'CMBDXB']},
                    'month': {'$in': months}
                    # 'year': {'$gte': last_year}
                    # 'year': {'$gte': this_year}
                }
        },
        {
            '$group':
                {
                    '_id':
                        {
                            'od': '$od',
                            'month': '$month',
                            'year': '$year'
                        },
                    'y_cap': {'$sum': '$y_cap'},
                    'j_cap': {'$sum': '$j_cap'}
                }
        },
        {
            '$project':
                {
                    'od': '$_id.od',
                    'month': '$_id.month',
                    'year': '$_id.year',
                    'y_capacity': '$y_cap',
                    'j_capacity': '$j_cap'
                }

        },
        {
            '$match':
                {
                    '$or':
                        [
                            {'y_capacity': {'$gt': 0}},
                            {'j_capacity': {'$gt': 0}}
                        ]
                }
        },
        {
            '$addFields': {'compartment': ['Y', 'J'], 'airline': Host_Airline_Code}
        },
        {
            '$unwind': '$compartment'
        },
        {
            '$project':
                {
                    '_id': 0,
                    'od': '$od',
                    'month': '$month',
                    'year': '$year',
                    'compartment': '$compartment',
                    'airline': '$airline',
                    'capacity':
                        {
                            '$cond': [
                                {
                                    '$eq': ['$compartment', 'J']
                                },
                                '$j_capacity',
                                '$y_capacity'
                            ]
                        }
                }
        }
    ], allowDiskUse=True)
    host_capacity_df = cursor_to_df(cursor_host_capacity)
    return host_capacity_df


@measure(JUPITER_LOGGER)
def get_comp_capacity_df(months, db):
    data_comp_capacity = db.JUP_DB_OD_Capacity.aggregate([
        {
            '$match':
                {
                    "month": {'$in': months},
                    # "year": {'$gte': last_year},
                    # 'pseudo_od': {'$in': ['CMBKWI', 'DACAMM', 'KTMUAE', 'AMMUAE', 'CGPUAE', 'CMBKWI', 'CMBUAE']},
                    # 'compartment': 'Y',
                    'carrier': {'$ne': Host_Airline_Code}
                }
        },
        {
            '$group':
                {
                    '_id':
                        {
                            'pseudo_od': '$pseudo_od',
                            'month_year': '$month_year',
                            'compartment': '$compartment',
                            'carrier': '$carrier'
                        },
                    'capacity': {'$sum': '$od_capacity'},
                    'original_ods': {'$addToSet': '$od'}
                }
        },
        {
            '$unwind': '$original_ods'
        },
        {
            '$group':
                {
                    '_id':
                        {
                            'pseudo_od': '$_id.pseudo_od',
                            'month_year': '$_id.month_year',
                            'compartment': '$_id.compartment'
                        },
                    'capacity_airline_docs': {'$addToSet': {'airline': '$_id.carrier', 'capacity': '$capacity'}},
                    'original_ods': {'$addToSet': '$original_ods'}
                }
        },
        {
            '$unwind': '$capacity_airline_docs'
        },
        {
            '$unwind': '$original_ods'
        },
        {
            '$project':
                {
                    '_id': 0,
                    'od': '$original_ods',
                    'compartment': '$_id.compartment',
                    'month_year': '$_id.month_year',
                    'airline': '$capacity_airline_docs.airline',
                    'capacity': '$capacity_airline_docs.capacity',
                }
        }
    ], allowDiskUse=True)
    comp_capacity_df = cursor_to_df(data_comp_capacity)
    comp_capacity_df['year'] = comp_capacity_df['month_year'].str.slice(0, 4)
    comp_capacity_df['month'] = comp_capacity_df['month_year'].str.slice(4, )
    comp_capacity_df['year'] = comp_capacity_df['year'].astype(int)
    comp_capacity_df['month'] = comp_capacity_df['month'].astype(int)
    comp_capacity_df.drop(['month_year'], axis=1, inplace=True)
    return comp_capacity_df


@measure(JUPITER_LOGGER)
def get_capacity_df(months, db):
    st = time.time()
    host_capacity_df = get_host_capacity_df(months, db)
    st = time.time()
    comp_capacity_df = get_comp_capacity_df(months, db)
    capacity_frames = [host_capacity_df, comp_capacity_df]
    capacity_df = pd.concat(capacity_frames)
    return capacity_df


@measure(JUPITER_LOGGER)
def get_fms_df(capacity_df, ratings_df):
    # fms_df = pd.merge(capacity_df, ratings_df, on=['od', 'compartment', 'airline'], how='left')
    fms_df = pd.merge(capacity_df, ratings_df, on=['airline'], how='left')  # Only considering Product Ratings
    fms_df[['rating', 'overall_rating', 'distributor_rating',
            'market_rating', 'capacity_rating', 'product_rating', 'fare_rating']].fillna(5, inplace=True)
    fms_df['capacity*rating'] = fms_df['capacity'] * fms_df['rating']
    fms_df['market_capacity*rating'] = fms_df.groupby(by=['od', 'compartment', 'month', 'year'])[
        'capacity*rating'].transform('sum')
    fms_df['FMS'] = fms_df['capacity*rating'] * 100 / fms_df['market_capacity*rating']
    fms_df.drop(['capacity*rating', 'market_capacity*rating'], axis=1, inplace=True)
    return fms_df


@measure(JUPITER_LOGGER)
def add_ly_values(complete_df, db, months):
    min_year = datetime.datetime.today().year - 3
    max_year = datetime.datetime.today().year + 3
    this_year = datetime.datetime.today().year
    final_df = pd.DataFrame()
    # for year in range(min_year, max_year + 1):
    #     complete_ty_df = complete_df[(complete_df['year'] == year)]
    #     if year == min_year:
    #         complete_ty_df['market_share_1'] = 0
    #         complete_ty_df['pax_1'] = 0
    #         complete_ty_df['market_size_1'] = 0
    #         complete_ty_df['capacity_1'] = 0
    #         complete_ty_df['FMS_1'] = 0
    #         temp_df = complete_ty_df
    #     else:
    #         """take the data from the pos_od_compartment_new for the last year data."""
    #
    #         complete_ly_df = complete_df[(complete_df['year'] == year - 1)]
    #         complete_ly_df['market_share_1'] = complete_ly_df['market_share']
    #         complete_ly_df['pax_1'] = complete_ly_df['pax']
    #         complete_ly_df['market_size_1'] = complete_ly_df['market_size']
    #         complete_ly_df['capacity_1'] = complete_ly_df['capacity']
    #         complete_ly_df['FMS_1'] = complete_ly_df['FMS']
    #         complete_ly_df.drop(
    #             ['market_share', 'year', 'FMS', 'capacity', 'pax', 'market_size', 'origin', 'destination', 'rating',
    #              'overall_rating', 'distributor_rating',
    #              'market_rating', 'capacity_rating', 'product_rating', 'fare_rating'], inplace=True, axis=1)
    #         temp_df = pd.merge(complete_ty_df, complete_ly_df, on=['pos', 'od', 'compartment', 'month', 'airline'],
    #                            how='left')
    #
    #     final_df = pd.concat([final_df, temp_df])
    year_list = list(set(complete_df['year'].values))
    last_year_list = [int(x-1) for x in year_list]

    #common_year = list(set(year_list).intersection(last_year_list))

    #month_list = list(set(complete_df['month'].values))
    pos_od_comp_crsr = db.JUP_DB_Pos_OD_Compartment_new.aggregate([
        {
            '$match': {
                "year": {"$in": last_year_list},
                "month": {"$in": months}
            }
        },

        {"$unwind": "$top_5_comp_data"},

        {"$project": {
            "_id": 0,
            "pos": "$pos",
            "od": "$od",
            "compartment": "$compartment",
            "month": "$month",
            "year": "$year",
            "market_size": "$top_5_comp_data.market_size",
            "capacity": "$top_5_comp_data.capacity",
            "market_share": "$top_5_comp_data.market_share",
            "pax": "$top_5_comp_data.pax",
            "FMS": "$top_5_comp_data.FMS",
            "airline": "$top_5_comp_data.airline"
        }
        }

    ])
    p_dF = cursor_to_df(pos_od_comp_crsr)


    if len(p_dF) > 0:
        for year in last_year_list:

            complete_ty_df = complete_df[(complete_df['year'] == year+1)]

            # if len(p_dF[p_dF['year']==year])>0 and len(complete_df[complete_df['year']==year])>0:
            #     left_df = p_dF.set_index(['pos', 'od', 'compartment', 'airline', 'year'])
            #     temp_r_df = complete_df[complete_df['year']==year, ['pos', 'od', 'airline', 'compartment', 'FMS',
            #                                                               'capacity', 'market_share',
            #                                                               'market_size', 'year']]
            #
            #     right_df = temp_r_df.set_index(['pos', 'od', 'compartment', 'airline', 'year'])
            #
            #     res = left_df.reindex(columns=left_df.columns.union(right_df.columns))
            #     res.update(right_df)
            #     res.reset_index(inplace=True)
            #
            #     complete_ly_df = res[(res['year'] == year)]
            #     complete_ly_df['market_share_1'] = complete_ly_df['market_share']
            #     complete_ly_df['pax_1'] = complete_ly_df['pax']
            #     complete_ly_df['market_size_1'] = complete_ly_df['market_size']
            #     complete_ly_df['capacity_1'] = complete_ly_df['capacity']
            #     complete_ly_df['FMS_1'] = complete_ly_df['FMS']
            #     temp_df = pd.merge(complete_ty_df, complete_ly_df,
            #                        on=['pos', 'od', 'compartment', 'month', 'airline', 'year'],
            #                        how='left')
            if len(p_dF[p_dF['year']== year])>0 and len(complete_df[complete_df['year'] == year])==0:

                complete_ly_df = p_dF[(p_dF['year'] == year)]
                complete_ly_df['market_share_1'] = complete_ly_df['market_share']
                complete_ly_df['pax_1'] = complete_ly_df['pax']
                complete_ly_df['market_size_1'] = complete_ly_df['market_size']
                complete_ly_df['capacity_1'] = complete_ly_df['capacity']
                complete_ly_df['FMS_1'] = complete_ly_df['FMS']
                complete_ly_df.drop(
                    ['market_share', 'FMS', 'capacity', 'pax', 'market_size', 'year'], inplace=True, axis=1)
                temp_df = pd.merge(complete_ty_df, complete_ly_df,
                                   on=['pos', 'od', 'compartment', 'month', 'airline'],
                                   how='left')

            elif len(complete_df[complete_df['year'] == year]) > 0:
                complete_ly_df = complete_df[(complete_df['year'] == year - 1)]
                complete_ly_df['market_share_1'] = complete_ly_df['market_share']
                complete_ly_df['pax_1'] = complete_ly_df['pax']
                complete_ly_df['market_size_1'] = complete_ly_df['market_size']
                complete_ly_df['capacity_1'] = complete_ly_df['capacity']
                complete_ly_df['FMS_1'] = complete_ly_df['FMS']
                complete_ly_df.drop(
                    ['market_share', 'year', 'FMS', 'capacity', 'pax', 'market_size', 'origin', 'destination', 'rating',
                     'overall_rating', 'distributor_rating',
                     'market_rating', 'capacity_rating', 'product_rating', 'fare_rating'], inplace=True, axis=1)
                temp_df = pd.merge(complete_ty_df, complete_ly_df,
                                   on=['pos', 'od', 'compartment', 'month', 'airline'],
                                   how='left')

            else:
                complete_ty_df['market_share_1'] = 0
                complete_ty_df['pax_1'] = 0
                complete_ty_df['market_size_1'] = 0
                complete_ty_df['capacity_1'] = 0
                complete_ty_df['FMS_1'] = 0
                temp_df = complete_ty_df

            final_df = pd.concat([final_df, temp_df])

    else:
        for year in last_year_list:

            complete_ty_df = complete_df[(complete_df['year'] == year + 1)]


            if len(complete_df[complete_df['year'] == year]) > 0:
                complete_ly_df = complete_df[(complete_df['year'] == year - 1)]
                complete_ly_df['market_share_1'] = complete_ly_df['market_share']
                complete_ly_df['pax_1'] = complete_ly_df['pax']
                complete_ly_df['market_size_1'] = complete_ly_df['market_size']
                complete_ly_df['capacity_1'] = complete_ly_df['capacity']
                complete_ly_df['FMS_1'] = complete_ly_df['FMS']
                complete_ly_df.drop(
                    ['market_share', 'year', 'FMS', 'capacity', 'pax', 'market_size', 'origin', 'destination', 'rating',
                     'overall_rating', 'distributor_rating',
                     'market_rating', 'capacity_rating', 'product_rating', 'fare_rating'], inplace=True, axis=1)
                temp_df = pd.merge(complete_ty_df, complete_ly_df,
                                   on=['pos', 'od', 'compartment', 'month', 'airline'],
                                   how='left')

            else:
                complete_ty_df['market_share_1'] = 0
                complete_ty_df['pax_1'] = 0
                complete_ty_df['market_size_1'] = 0
                complete_ty_df['capacity_1'] = 0
                complete_ty_df['FMS_1'] = 0
                temp_df = complete_ty_df

            final_df = pd.concat([final_df, temp_df])

    return final_df


@measure(JUPITER_LOGGER)
def get_pos_od_compartment_combinations(db):
    st = time.time()
    ppln_markets_FZ = [
        {
            '$match':
                {
                    'MarketingCarrier1': Host_Airline_Code,
                    'pos': {'$ne': 'NA'}
                }
        }
        ,
        {
            '$group': {
                '_id': {
                    'pos': '$pos',
                    'od': '$od',
                    'compartment': '$compartment'
                }
            }
        }
        ,
        {
            '$project': {
                '_id': 0,
                'pos': '$_id.pos',
                'od': '$_id.od',
                'compartment': '$_id.compartment'
            }
        }
    ]
    crsr_markets_FZ = db.JUP_DB_Market_Share.aggregate(ppln_markets_FZ, allowDiskUse=True)
    list_markets_FZ = list(crsr_markets_FZ)
    markets_FZ_df = pd.DataFrame(list_markets_FZ)
    markets_FZ_df['pos_od_compartment_key'] = markets_FZ_df['pos'] + markets_FZ_df['od'] + markets_FZ_df['compartment']
    pos_od_compartment_series_FZ = markets_FZ_df['pos_od_compartment_key'].values
    return pos_od_compartment_series_FZ


@measure(JUPITER_LOGGER)
def add_pseudo_od(final_df, db):
    city_airport_df = pd.DataFrame(
        list(db.JUP_DB_City_Airport_Mapping.find({}, {'_id': 0, 'Airport_Code': 1, 'City_Code': 1})))
    final_df['origin'] = final_df['od'].str.slice(0, 3)
    final_df['destination'] = final_df['od'].str.slice(3, 6)
    city_airport_df.rename(columns={'Airport_Code': 'origin', 'City_Code': 'pseudo_origin'}, inplace=True)
    final_df = pd.merge(final_df, city_airport_df, on=['origin'], how='left')
    city_airport_df.rename(columns={'origin': 'destination', 'pseudo_origin': 'pseudo_destination'}, inplace=True)
    final_df = pd.merge(final_df, city_airport_df, on=['destination'], how='left')
    final_df['pseudo_od'] = final_df['pseudo_origin'] + final_df['pseudo_destination']
    final_df.drop(['pseudo_origin', 'pseudo_destination'], axis=1, inplace=True)
    final_df['pseudo_od'].fillna(final_df['od'], inplace=True)
    return final_df

def get_user_defined_comp_df(client, months):
    db = client[JUPITER_DB]
    try:
        df_user_defined_competitor = pd.DataFrame(list(
            # db.JUP_DB_Web_Pricing_Ad_Hoc.find({'user_defined_comp': {'$ne': None}},
            #                                   {'_id': 0, 'od': 1, 'user_defined_comp': 1})))
            db.JUP_DB_Web_Pricing_Ad_Hoc.find({'user_defined_comp': {'$ne': None}},
                                                     {'_id': 0, 'od': 1, 'pos': 1, 'compartment': 1,
                                                      'user_defined_comp': 1})))
        # print(df_user_defined_competitor.size)

        mt_docs = db.JUP_DB_Manual_Triggers_Module_Summary.aggregate([
            {"$match": {
                "dep_year": int(this_year),
                "dep_month": {'$in' : [int(this_month),int(this_month)+1]},
                # "pos.City" : {"$ne" : "MIS"}
            }},
            {"$group": {
                "_id": {
                    "pos": "$pos.City",
                    "od": "$od"
                }
            }},
            {"$project": {
                "_id": 0,
                "pos": "$_id.pos",
                "od": "$_id.od"
            }}
        ])

        pd.set_option('display.max_columns', 100)

        mt_df = cursor_to_df(mt_docs)
        # print "market review dataframe"
        # print(mt_df.size)

        # merge both data frames based on od
        # since user_defined_competitor with left join
        result = pd.merge(df_user_defined_competitor, mt_df, how="left", on=["od", "od"])

        print "result size"
        print(result[result["pos_x"] != "***"])
        print(result.size)

        ## get the datafrare where pos_x is not ***
        pd_sel_pos = result[result["pos_x"] != "***"]
        df_user_defined_competitor_Selective_pos = df_user_defined_competitor[
            df_user_defined_competitor["pos"] != "***"]

        # ## drop unnecessary columns
        # result.drop(["pos_x", "pos_y"], axis = 1)

        # ## remove all row other then *** as pos
        result = result[result.pos_x == "***"]

        pd_sel_pos["pos_match"] = pd_sel_pos.apply(lambda x: True if x['pos_x'] == x['pos_y'] else False, axis=1)

        pd_sel_pos_update = pd_sel_pos[pd_sel_pos["pos_match"] == True]

        # pd_sel_pos_update.drop('pos_match', axis = 1)

        # print pd_sel_pos_update
        # print(result[result["od"] == "DXBCCJ"])

        final_df = pd.merge(result, pd_sel_pos_update, how='outer', left_on=['od', 'pos_y', 'compartment'],
                            right_on=['od', 'pos_y', 'compartment'])

        # Global_ = final_df[final_df.pos_x == "***"]
        # Parcial_ = final_df[final_df.pos_x != "***"]

        # Parcial_
        final_df["user_defined_comp_x"] = final_df.apply(
            lambda x: x['user_defined_comp_y'] if x['pos_match'] == True else x['user_defined_comp_x'], axis=1)

        # print "final"
        # print pd_sel_pos_update

        final_df = final_df.drop(['pos_match', 'user_defined_comp_y', 'pos_x_x', 'pos_x_y'], axis=1)
        final_df.rename(columns={"user_defined_comp_x": "user_defined_comp", "pos_y": "pos"}, inplace=True)

        # df_user_defined_competitor_Selective_pos
        # print(final_df[final_df["od"] == "DXBCCJ"])

        concated_res = pd.concat([final_df, df_user_defined_competitor_Selective_pos], ignore_index=True)
        # print(concated_res[concated_res["od"] == "DXBCCJ"])

        # print(concated_res.size)
        # # since user_defined_competitor with left join
        # result_df = pd.merge(result, pd_sel_pos_update, how="left" , on = ["od", "pos_x"])
        #
        # print("dp_sel_pos")
        # print(pd_sel_pos_update.head())
        # # print(pd_sel_pos.drop([False], axis=1))

        # print("final_opt")
        # print(result_df.head())
        # print(result_df[result_df["pos_x"] != "***"])
        concated_res = concated_res.dropna()
        return concated_res
    except KeyError as error:
        # return result
        print error
        return

    except Exception as error:
        print error
        return concated_res


@measure(JUPITER_LOGGER)
def main(months, client):
    db = client[JUPITER_DB]
    start_time = time.time()
    try:
        st = time.time()
        print "Getting Market Share DF"

        snap_dates = list(db.JUP_DB_Market_Share.distinct("snap_date"))
        snap_dates.sort()
        snap_date = snap_dates[-1]
        print snap_date

        market_share_df = get_market_share_df(snap_date=snap_date,
                                              months=months,
                                              db=db)
        print "Time taken to get Market Share DF = ", time.time() - st
        market_share_df['posodc'] = market_share_df['pos'] + market_share_df['origin'] + market_share_df[
            'destination'] + market_share_df['compartment']
        posodc_list = get_pos_od_compartment_combinations(db)
        market_share_df = market_share_df[market_share_df['posodc'].isin(posodc_list)]
        market_share_df.drop(['posodc'], axis=1, inplace=True)
        print "Getting Ratings DF"
        st = time.time()
        # ratings_df = get_ratings_df()
        ratings_df = get_ratings_df_temp_product(db)  # only considering Product Rating
        print "Time taken to get ratings DF = ", time.time() - st
        print "Getting Capacity DF"
        st = time.time()
        capacity_df = get_capacity_df(months, db)
        print "Time taken to get capacity df = ", time.time() - st
        print "Getting FMS DF"
        fms_df = get_fms_df(capacity_df=capacity_df, ratings_df=ratings_df)
        del capacity_df
        # fms_df.drop(['origin', 'destination'], axis=1, inplace=True)
        pos_od_comp_month_year_df = market_share_df[['pos', 'od', 'compartment', 'month', 'year']]
        pos_od_comp_month_year_df.drop_duplicates(inplace=True)
        fms_full_df = pd.merge(pos_od_comp_month_year_df, fms_df, on=['od', 'compartment', 'month', 'year'],
                               how='inner')
        del pos_od_comp_month_year_df
        del fms_df
        complete_df = pd.merge(market_share_df, fms_full_df,
                               on=['pos', 'od', 'compartment', 'month', 'year', 'airline'], how='outer')
        del fms_full_df
        del market_share_df
        st = time.time()
        #  - - - - - This part of the code is written for the case:
        #  Market_Share_DF does not contain Rating for airline. FMS_DF contains ratings.
        # But, for those months/markets where we do not have capacity for an airline, and we have rating,
        # we will miss out on the rating for this particular airline since FMS DF is computed by a left join on capacity
        # So to add rating for any missed airline, again ratings df is merged with complete_df
        complete_df.drop(['rating', 'overall_rating', 'distributor_rating',
                          'market_rating', 'capacity_rating', 'product_rating', 'fare_rating'], axis=1, inplace=True)
        # ratings_df.drop(['origin','destination'], axis=1, inplace=True)
        complete_df = pd.merge(complete_df, ratings_df, on=['airline'], how='left')
        # - - - - - - Done.
        final_df = add_ly_values(complete_df=complete_df, months=months, db=db)
        del complete_df
        final_df.drop_duplicates(inplace=True)
        final_df[['rating', 'overall_rating', 'distributor_rating',
                  'market_rating', 'capacity_rating', 'product_rating', 'fare_rating']].fillna(5, inplace=True)
        final_df = add_pseudo_od(final_df, db)
        # - - - - - - -  - - -     To convert structure of collection to add top_5_competitor_data as an embedded field
        BATCH_SIZE = 100000
        structured_dict = []
        print "Grouping per market/month to create top_5_comp_data_field"
        i = 1
        grouped_by_market_month_df = final_df.groupby(by=['pos', 'od', 'compartment', 'month', 'year'])
        del final_df
        global TOTAL_MARKETS
        TOTAL_MARKETS = len(grouped_by_market_month_df)

        # df_user_defined_competitor = pd.DataFrame(list(
        #     # db.JUP_DB_Web_Pricing_Ad_Hoc.find({'user_defined_comp': {'$ne': None}},
        #     #                                   {'_id': 0, 'od': 1, 'user_defined_comp': 1})))
        #     db.JUP_DB_Web_Pricing_Ad_Hoc.find({'user_defined_comp': {'$ne': None}},
        #                                       {'_id': 0, 'od': 1,'pos':1,'compartment':1, 'user_defined_comp': 1})))

        try:
            df_user_defined_competitor = get_user_defined_comp_df(client, months)
        except Exception as error:
            df_user_defined_competitor = get_user_defined_comp_df(client, months)

        # df_user_defined_competitor['user_defined_comp'] = df_user_defined_competitor['user_defined_comp'].apply(
        #     lambda row: [row])
        # db.JUP_DB_Pos_OD_Compartment_new.remove({'month': {'$in': months}})
        batch_iter = 1
        st = time.time()
        combine_column = []
        for market_month, grouped_airlines in grouped_by_market_month_df:
            # grouped_airlines.sort_values(by=['market_share'], inplace=True, ascending=False)
            if len(structured_dict) < BATCH_SIZE:
                airline_list = list(grouped_airlines['airline'])
                market_share_list = list(grouped_airlines['market_share'])
                fms_list = list(grouped_airlines['FMS'])
                rating_list = list(grouped_airlines['rating'])
                overall_rating_list = list(grouped_airlines['overall_rating'])
                distributor_rating_list = list(grouped_airlines['distributor_rating'])
                market_rating_list = list(grouped_airlines['market_rating'])
                capacity_rating_list = list(grouped_airlines['capacity_rating'])
                product_rating_list = list(grouped_airlines['product_rating'])
                fare_rating_list = list(grouped_airlines['fare_rating'])
                market_share_1_list = list(grouped_airlines['market_share_1'])
                capacity_list = list(grouped_airlines['capacity'])
                pax_list = list(grouped_airlines['pax'])
                market_size_list = list(grouped_airlines['market_size'])
                pax_1_list = list(grouped_airlines['pax_1'])
                market_size_1_list = list(grouped_airlines['market_size_1'])
                capacity_1_list = list(grouped_airlines['capacity_1'])
                fms_1_list = list(grouped_airlines['FMS_1'])
                zipped = zip(airline_list, market_share_list, fms_list, rating_list, overall_rating_list,
                             distributor_rating_list, market_rating_list, capacity_rating_list, product_rating_list,
                             fare_rating_list, market_share_1_list, capacity_list, pax_list, market_size_list,
                             pax_1_list,
                             market_size_1_list, capacity_1_list, fms_1_list)
                airlines_data = []
                for airline_details in zipped:
                    temp = {
                        'airline': airline_details[0],
                        'market_share': airline_details[1],
                        'FMS': airline_details[2],
                        'rating': airline_details[3],
                        'overall_rating': airline_details[4],
                        'distributor_rating': airline_details[5],
                        'market_rating': airline_details[6],
                        'capacity_rating': airline_details[7],
                        'product_rating': airline_details[8],
                        'fare_rating': airline_details[9],
                        'market_share_1': airline_details[10],
                        'capacity': airline_details[11],
                        'pax': airline_details[12],
                        'market_size': airline_details[13],
                        'pax_1': airline_details[14],
                        'market_size_1': airline_details[15],
                        'capacity_1': airline_details[16],
                        'FMS_1': airline_details[17]
                    }

                    for k, v in temp.items():
                        if str(temp[k]) == 'nan':
                            temp[k] = 0.0

                    airlines_data.append(temp)

                structured_dict.append(
                    {
                        'pos': grouped_airlines['pos'].values[0],
                        'od': grouped_airlines['od'].values[0],
                        'pseudo_od': grouped_airlines['pseudo_od'].values[0],
                        'compartment': grouped_airlines['compartment'].values[0],
                        'month': grouped_airlines['month'].values[0],
                        'year': grouped_airlines['year'].values[0],
                        'top_5_comp_data': airlines_data,
                        'last_update_date': SYSTEM_DATE
                    }
                )
                if (i % 10000) == 0:
                    print "Done for market/month number ", str(i), ' out of ', str(TOTAL_MARKETS)
                i += 1
            else:
                clubbed_df = pd.DataFrame(structured_dict)
                # inserting_df = pd.merge(clubbed_df, df_user_defined_competitor, on=['od'], how='left')
                inserting_df = pd.merge(clubbed_df, df_user_defined_competitor, on=['od', 'pos', 'compartment'],
                                        how='left')
                inserting_df['combine_column'] = inserting_df['year'].apply(str) + inserting_df['month'].apply(str) + \
                                                 inserting_df['pos'] + inserting_df['od'] + inserting_df['compartment']
                inserting_df['market'] = inserting_df['pos'] + inserting_df['od'] + inserting_df['compartment']
                inserting_df['month_str'] = inserting_df['month'].apply(lambda row: str_month(row))
                inserting_df['month_year'] = inserting_df['year'].apply(str) + inserting_df['month_str']
                inserting_df['origin'] = inserting_df['od'].str.slice(0, 3)
                inserting_df['destination'] = inserting_df['od'].str.slice(3, 6)
                inserting_df.drop(['month_str'], axis=1, inplace=True)
                combine_column = combine_column + list(inserting_df['combine_column'].values)
                bulk = db.JUP_DB_Pos_OD_Compartment_new.initialize_ordered_bulk_op()
                num = 1
                for index, row in inserting_df.iterrows():
                    bulk.find({
                        'combine_column': row['combine_column']
                    }).upsert().update({
                        '$setOnInsert': {
                            'market': row['market'],
                            'year': row['year'],
                            'month': row['month'],
                            'pos': row['pos'],
                            'origin': row['origin'],
                            'destination': row['destination'],
                            'compartment': row['compartment'],
                            'od': row['od'],
                            'combine_column': row['combine_column'],
                            'month_year': row['month_year'],
                            'pseudo_od': row['pseudo_od']
                        },

                        '$set': {'top_5_comp_data': row['top_5_comp_data'],
                                 'user_defined_comp': row['user_defined_comp'],
                                 'last_update_date': row['last_update_date']

                                 }

                    })
                    if num % 1000 == 0:
                        bulk.execute()
                        bulk = db.JUP_DB_Pos_OD_Compartment_new.initialize_ordered_bulk_op()
                    num = num + 1

                bulk.execute()
                # bulk = db.JUP_DB_Pos_OD_Compartment_new.initialize_ordered_bulk_op()
                # db.JUP_DB_Pos_OD_Compartment_new.insert(inserting_df.to_dict('records'))
                structured_dict = []
                batch_iter += 1
                st = time.time()
        #  To insert remaining Markets whose length of remaining markets < BATCH_SIZE
        if len(structured_dict) != BATCH_SIZE:
            # logging.info("Time taken to create top_5_comp_data field from denormalized DF for BATCH " + str(batch_iter) + " = " + str(time.time() - st))
            clubbed_df = pd.DataFrame(structured_dict)
            # inserting_df = pd.merge(clubbed_df, df_user_defined_competitor, on=['od'], how='left')
            inserting_df = pd.merge(clubbed_df, df_user_defined_competitor, on=['od', 'pos', 'compartment'], how='left')
            inserting_df['combine_column'] = inserting_df['year'].apply(str) + inserting_df['month'].apply(str) + \
                                             inserting_df['pos'] + inserting_df['od'] + inserting_df['compartment']
            inserting_df['market'] = inserting_df['pos'] + inserting_df['od'] + inserting_df['compartment']
            inserting_df['month_str'] = inserting_df['month'].apply(lambda row: str_month(row))
            inserting_df['month_year'] = inserting_df['year'].apply(str) + inserting_df['month_str']
            inserting_df['origin'] = inserting_df['od'].str.slice(0, 3)
            inserting_df['destination'] = inserting_df['od'].str.slice(3, 6)
            inserting_df.drop(['month_str'], axis=1, inplace=True)

            combine_column = combine_column + list(inserting_df['combine_column'].values)

            df_user_defined_competitor_y = df_user_defined_competitor.copy()
            # df_user_defined_competitor_y['pos'] = df_user_defined_competitor_y['od'].str.slice(0, 3)
            # df_user_defined_competitor_y.loc[df_user_defined_competitor_y['pos'].isin(['DXB', 'DWC']), 'pos'] = 'UAE'
            # df_user_defined_competitor_y['compartment'] = 'Y'
            df_user_defined_competitor_y['market'] = df_user_defined_competitor_y['pos'] + df_user_defined_competitor_y[
                'od'] + df_user_defined_competitor_y['compartment']

            web_adhoc_list_y = set(df_user_defined_competitor_y['market'].values)

            print "Got the list of the markets with Y compartment"

            # df_user_defined_competitor_j = df_user_defined_competitor.copy()
            # # df_user_defined_competitor_j['pos'] = df_user_defined_competitor_j['od'].str.slice(0, 3)
            # # df_user_defined_competitor_j.loc[df_user_defined_competitor_j['pos'].isin(['DXB', 'DWC']), 'pos'] = 'UAE'
            # df_user_defined_competitor_j['compartment'] = 'J'
            # df_user_defined_competitor_j['market'] = df_user_defined_competitor_j['pos'] + \
            #                                          df_user_defined_competitor_j['od'] + \
            #                                          df_user_defined_competitor_j['compartment']
            #
            # web_adhoc_list_j = set(df_user_defined_competitor_j['market'].values)

            print "Got the list of the markets with J compartment"
            web_market_list = list(web_adhoc_list_y)
            # web_market_list = list(web_adhoc_list_j) + list(web_adhoc_list_y)
            #year_min = (datetime.datetime.strptime(SYSTEM_DATE, '%Y-%m-%d') - timedelta(days=364)).year
            year_min = (datetime.datetime.strptime(SYSTEM_DATE, '%Y-%m-%d')).year
            year_max = (datetime.datetime.strptime(SYSTEM_DATE, '%Y-%m-%d') + timedelta(days=364)).year

            if len(web_market_list) > 0:
                temp_df_web = pd.DataFrame(web_market_list, columns=['market'])

                temp_df_web['od'] = temp_df_web['market'].str.slice(3, 9)
                temp_df_web = add_pseudo_od(final_df=temp_df_web, db=db)
                temp_df_web['pos'] = temp_df_web['market'].str.slice(0, 3)
                temp_df_web['compartment'] = temp_df_web['market'].str.slice(-1)
                # temp_df_web = temp_df_web.merge(df_user_defined_competitor, how='left', on=['od'])
                temp_df_web = temp_df_web.merge(df_user_defined_competitor, how='left', on=['od', 'pos', 'compartment'])

                temp = {
                    'airline': '',
                    'market_share': 0.0,
                    'FMS': 0.0,
                    'rating': 0.0,
                    'overall_rating': 0.0,
                    'distributor_rating': 0.0,
                    'market_rating': 0.0,
                    'capacity_rating': 0.0,
                    'product_rating': 0.0,
                    'fare_rating': 0.0,
                    'market_share_1': 0.0,
                    'capacity': 0.0,
                    'pax': 0.0,
                    'market_size': 0.0,
                    'pax_1': 0.0,
                    'market_size_1': 0.0,
                    'capacity_1': 0.0,
                    'FMS_1': 0.0
                }
                dict_user = []
                for idx, row in temp_df_web.iterrows():
                    for year in range(year_min, year_max + 1):
                        print(str(year))
                        print(str(months[0]))
                        print(row['market'])

                        y_dict = {
                            'market': row['market'],
                            'month': months[0],
                            'year': year,
                            'top_5_comp_data': [temp],
                            'last_update_date': SYSTEM_DATE,
                            'month_year': str(year) + str_month(months[0]),
                            'combine_column': str(year) + str(months[0]) + row['market']
                        }
                        dict_user.append(y_dict)

                user_def_df = pd.DataFrame(dict_user)
                temp_df_web = temp_df_web.merge(user_def_df, how='left', on=['market'])
                temp_df_web = temp_df_web[~(temp_df_web['combine_column'].isin(combine_column))]
                inserting_df = pd.concat([inserting_df, temp_df_web], ignore_index=True)

            bulk = db.JUP_DB_Pos_OD_Compartment_new.initialize_ordered_bulk_op()
            num = 1
            for index, row in inserting_df.iterrows():
                print row['combine_column']
                bulk.find({
                    'combine_column': row['combine_column']
                }).upsert().update({
                    '$setOnInsert': {
                        'market': row['market'],
                        'year': row['year'],
                        'month': row['month'],
                        'pos': row['pos'],
                        'origin': row['origin'],
                        'destination': row['destination'],
                        'compartment': row['compartment'],
                        'od': row['od'],
                        'combine_column': row['combine_column'],
                        'month_year': row['month_year'],
                        'pseudo_od': row['pseudo_od']
                    },

                    '$set': {'top_5_comp_data': row['top_5_comp_data'],
                             'user_defined_comp': row['user_defined_comp'],
                             'last_update_date': row['last_update_date']

                             }

                })
                if num % 1000 == 0:
                    bulk.execute()
                    bulk = db.JUP_DB_Pos_OD_Compartment_new.initialize_ordered_bulk_op()
                num = num + 1

            bulk.execute()

            # db.JUP_DB_Pos_OD_Compartment_new.insert(inserting_df.to_dict('records'))
    except Exception:
        global ERROR_FLAG
        ERROR_FLAG = 1
        print traceback.print_exc()
    print "Time taken for entire program = ", time.time() - start_time
    return 0


# @measure(JUPITER_LOGGER)
# def main_helper():
#     months_all = [[7, 8, 9], [10, 11, 12]]
#     logging.info("Updating Running status on JUP_DB_Data_Status")
#     id = update_running_status()
#     st = time.time()
#     for months in months_all:
#         logging.info("Removing old records and generating new records for months = " + str(months))
#         # db.JUP_DB_Pos_OD_Compartment_new.remove({'month': {'$in': months}})
#         main(months)
#     logging.info("Time taken to update Collection = " + str(time.time() - st))
#     update_completed_status(object_id=id)
#     logging.info("Updated completed status on JUP_DB_Data_Status. Program ran successfully.")


if __name__ == '__main__':
    from celery import group
    from jupiter_AI.batch.atpco_automation.Automation_tasks import run_pos_od_compartment
    client = mongo_client()
    st = time.time()
    months_list = [[1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11], [12]]
    posodc_group = []
    for months in months_list:
        posodc_group.append(run_pos_od_compartment.s(months=months))
    group2 = group(posodc_group)
    res2 = group2()
    res2.get()
    client.close()
    print 'time taken:', time.time() - st
    print get_ratings_df_temp_product()
    # client = mongo_client()
    # for mon in [[5]]:
    #     main(mon, client)
    # client.close()
    # client = mongo_client()
    # main([5], client)
    # client.close()