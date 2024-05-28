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
from jupiter_AI import client, JUPITER_DB, Host_Airline_Code, JUPITER_LOGGER
from jupiter_AI.logutils import measure
from bson.objectid import ObjectId
from jupiter_AI.network_level_params import SYSTEM_DATE
import traceback
import logging

# logging.basicConfig(filename='POS_OD_C_1.log', level=logging.DEBUG)

db = client[JUPITER_DB]

random_origin = 'CPH'
random_destination = 'BKK'
random_compartment = 'Y'

HOST_AIRLINE = Host_Airline_Code

COLLECTION_UPDATE_LIST = ['JUP_DB_Pos_OD_Compartment']
COLLECTION_DEPENDENCY_LIST = ['JUP_DB_Market_Share','JUP_DB_Competitor_Ratings','JUP_DB_Host_OD_Capacity',
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
def update_running_status():
    id = db.JUP_DB_Data_Status.insert(
        {
            'team': 'analytics',
            'type': 'script',
            'level': 2,
            'name': 'JUP_AI_Batch_Market_Characteristics',
            'title': 'pos_od_compartment',
            'stage': 'processing',
            'last_update_date': datetime.datetime.today().strftime('%Y-%m-%d'),
            'last_update_time': datetime.datetime.today().strftime('%H:%M'),
            'start_time': datetime.datetime.today().strftime('%H:%M'),
            'end_time': datetime.datetime.today().strftime('%H:%M'),
            'run_time': time.time() - START_TIME,
            'collections_updated': COLLECTION_UPDATE_LIST,
            'collections_dependencies': COLLECTION_DEPENDENCY_LIST,
            'params_updated': [
                                {
                                    'collection': 'JUP_DB_Pos_OD_Comartment',
                                    'fields': ['top_5_comp_data','pos','od','compartment','month','year'
                                               'combine_column']
                                }
                               ],
            'number_of_params': 6,
            'logs': {},
            'ip': '172.28.23.8',
            'port': '3354',
            'URL': '',
            'status': 'Running',
            'error': 0
        }
    )
    return id


@measure(JUPITER_LOGGER)
def get_market_share_df(months):
    market_share_df = pd.DataFrame(list(db.JUP_DB_Market_Share.aggregate([
        {
            '$match':
                {
                     # "year": {'$gte': this_year},
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
                            'snap_month': '$snap_date'
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
    ], allowDiskUse=True)))
    market_share_df['od'] = market_share_df['origin'] + market_share_df['destination']
    return market_share_df


@measure(JUPITER_LOGGER)
def get_ratings_df():
    rating_df = pd.DataFrame(list(db.JUP_DB_Competitor_Ratings_Old.aggregate([
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
    )
    )

    ratings_dict = []
    for index, market in rating_df.iterrows():
        market_airlines = market['airline'].keys()
        market_ratings = market['airline'].values()
        for i in range(len(market_airlines)):
            ratings_dict.append(
                {'origin': market['origin'], 'destination': market['destination'],'compartment': market['compartment'], 'airline': market_airlines[i],
                 'rating': market_ratings[i]})

    ratings_df = pd.DataFrame(ratings_dict)
    ratings_df['od'] = ratings_df['origin'] + ratings_df['destination']

    return ratings_df


@measure(JUPITER_LOGGER)
def get_host_capacity_df(months):
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

    capacity_data_cursor = list(cursor_host_capacity)
    host_capacity_df = pd.DataFrame(capacity_data_cursor)
    return host_capacity_df


@measure(JUPITER_LOGGER)
def get_comp_capacity_df(months):
    data_comp_capacity = list(db.JUP_DB_OD_Capacity.aggregate([
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
    ], allowDiskUse=True))
    comp_capacity_df = pd.DataFrame(data_comp_capacity)
    comp_capacity_df['year'] = comp_capacity_df['month_year'].str.slice(0, 4)
    comp_capacity_df['month'] = comp_capacity_df['month_year'].str.slice(4, )
    comp_capacity_df['year'] = comp_capacity_df['year'].astype(int)
    comp_capacity_df['month'] = comp_capacity_df['month'].astype(int)
    comp_capacity_df.drop(['month_year'], axis=1, inplace=True)
    return comp_capacity_df



@measure(JUPITER_LOGGER)
def get_capacity_df(months):
    logging.info("Getting Host Capacity DF from JUP_DB_Host_OD_Capacity . . .")
    st = time.time()
    host_capacity_df = get_host_capacity_df(months)
    logging.info("Time taken to get host capacity df = " + str(time.time() - st))
    logging.info("Getting Competitor Capacity df from JUP_DB_OD_Capacity . . .")
    st = time.time()
    comp_capacity_df = get_comp_capacity_df(months)
    logging.info("Time taken to get Competitor Capacity = " + str(time.time() - st))
    capacity_frames = [host_capacity_df, comp_capacity_df]
    capacity_df = pd.concat(capacity_frames)

    return capacity_df


@measure(JUPITER_LOGGER)
def get_fms_df(capacity_df, ratings_df):
    fms_df = pd.merge(capacity_df, ratings_df, on=['od', 'compartment', 'airline'], how='left')
    fms_df['rating'].fillna(5, inplace=True)
    fms_df['capacity*rating'] = fms_df['capacity'] * fms_df['rating']
    fms_df['market_capacity*rating'] = fms_df.groupby(by=['od', 'compartment', 'month', 'year'])[
        'capacity*rating'].transform('sum')
    fms_df['FMS'] = fms_df['capacity*rating'] * 100 / fms_df['market_capacity*rating']
    fms_df.drop(['capacity*rating', 'market_capacity*rating'], axis=1, inplace=True)
    return fms_df


@measure(JUPITER_LOGGER)
def add_ly_values(complete_df):
    min_year = datetime.datetime.today().year - 3
    max_year = datetime.datetime.today().year + 3
    final_df = pd.DataFrame()
    for year in range(min_year, max_year+1):
        logging.info("year = " + str(year))
        complete_ty_df = complete_df[(complete_df['year'] == year)]
        if year == min_year:
            complete_ty_df['market_share_1'] = 0
            complete_ty_df['pax_1'] = 0
            complete_ty_df['market_size_1'] = 0
            complete_ty_df['capacity_1'] = 0
            complete_ty_df['FMS_1'] = 0
            temp_df = complete_ty_df
            logging.info("Initialized market_share_ly = 0, pax_ly = 0 and market_size_ly = 0 for MIN YEAR")
        else:
            complete_ly_df = complete_df[(complete_df['year'] == year - 1)]
            complete_ly_df['market_share_1'] = complete_ly_df['market_share']
            complete_ly_df['pax_1'] = complete_ly_df['pax']
            complete_ly_df['market_size_1'] = complete_ly_df['market_size']
            complete_ly_df['capacity_1'] = complete_ly_df['capacity']
            complete_ly_df['FMS_1'] = complete_ly_df['FMS']
            complete_ly_df.drop(
                ['market_share', 'year', 'FMS', 'capacity', 'pax', 'market_size', 'origin', 'destination', 'rating'], inplace=True, axis=1)
            temp_df = pd.merge(complete_ty_df, complete_ly_df, on=['pos', 'od', 'compartment', 'month', 'airline'], how='left')
            logging.info("Merged Market Share Last year for the year = " + str(year))
        final_df = pd.concat([final_df, temp_df])
    logging.info("Added Last year market share for all markets.")
    return final_df


@measure(JUPITER_LOGGER)
def get_pos_od_compartment_combinations():
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
    logging.info("Getting List of Markets from Market Share collection . . . . . ")
    crsr_markets_FZ = db.JUP_DB_Market_Share.aggregate(ppln_markets_FZ, allowDiskUse=True)
    list_markets_FZ = list(crsr_markets_FZ)
    markets_FZ_df = pd.DataFrame(list_markets_FZ)
    markets_FZ_df['pos_od_compartment_key'] = markets_FZ_df['pos'] + markets_FZ_df['od'] + markets_FZ_df['compartment']
    pos_od_compartment_series_FZ = markets_FZ_df['pos_od_compartment_key'].values
    logging.info("Time taken to list of markets = " + str(time.time() - st))
    return pos_od_compartment_series_FZ


@measure(JUPITER_LOGGER)
def add_pseudo_od(final_df):
    city_airport_df = pd.DataFrame(list(db.JUP_DB_City_Airport_Mapping.find({}, {'_id': 0, 'Airport_Code': 1, 'City_Code': 1})))
    final_df['origin'] = final_df['od'].str.slice(0, 3)
    final_df['destination'] = final_df['od'].str.slice(3, 6)
    city_airport_df.rename(columns={'Airport_Code': 'origin', 'City_Code': 'pseudo_origin'}, inplace=True)
    final_df = pd.merge(final_df, city_airport_df, on=['origin'], how='left')
    city_airport_df.rename(columns={'origin': 'destination', 'pseudo_origin': 'pseudo_destination'}, inplace=True)
    final_df = pd.merge(final_df, city_airport_df, on=['destination'], how='left')
    final_df['pseudo_od'] = final_df['pseudo_origin'] + final_df['pseudo_destination']
    final_df.drop(['pseudo_origin', 'pseudo_destination'], axis=1, inplace=True)
    return final_df


@measure(JUPITER_LOGGER)
def main(months):
    start_time = time.time()
    try:
        st = time.time()
        logging.info("Getting Market Share df from Market Share collection . . . . . ")
        market_share_df = get_market_share_df(months)
        logging.info("Time taken to get Market Share df = " + str(time.time() - st))
        market_share_df['posodc'] = market_share_df['pos'] + market_share_df['origin'] + market_share_df['destination'] + market_share_df['compartment']
        posodc_list = get_pos_od_compartment_combinations()
        logging.info("Number of unique pos od compartment combinations for FZ = " + str(len(posodc_list)))
        market_share_df = market_share_df[market_share_df['posodc'].isin(posodc_list)]
        logging.info("In market_share_df, len(market_share_df) for unique pos od c = " + str(len(market_share_df)))
        market_share_df.drop(['posodc'], axis=1, inplace=True)
        logging.info("Getting Ratings df from JUP_DB_Competitor_Ratings . . . . . . ")
        st = time.time()
        ratings_df = get_ratings_df()
        logging.info("Time taken to get Ratings DF = " + str(time.time() - st))
        logging.info("Getting Capacity DF . . . . . ")
        st = time.time()
        capacity_df = get_capacity_df(months)
        logging.info("Time taken to get Capacity DF = " + str(time.time() - st))
        logging.info("Getting FMS DF from Ratings DF and Capacity DF . . . . ")
        fms_df = get_fms_df(capacity_df=capacity_df, ratings_df=ratings_df)
        logging.info("Time taken to get FMS DF = " + str(time.time() - st))
        fms_df.drop(['origin', 'destination'], axis=1, inplace=True)
        pos_od_comp_month_year_df = market_share_df[['pos', 'od', 'compartment', 'month', 'year']]
        pos_od_comp_month_year_df.drop_duplicates(inplace=True)
        fms_full_df = pd.merge(pos_od_comp_month_year_df, fms_df, on=['od', 'compartment', 'month', 'year'], how='inner')
        complete_df = pd.merge(market_share_df, fms_full_df, on=['pos', 'od', 'compartment', 'month', 'year', 'airline'], how='outer')
        st = time.time()
        #  - - - - - This part of the code is written for the case:
        #  Market_Share_DF does not contain Rating for airline. FMS_DF contains ratings.
        # But, for those months/markets where we do not have capacity for an airline, and we have rating,
        # we will miss out on the rating for this particular airline since FMS DF is computed by a left join on capacity
        # So to add rating for any missed airline, again ratings df is merged with complete_df
        complete_df.drop(['rating'], axis=1, inplace=True)
        ratings_df.drop(['origin','destination'], axis=1, inplace=True)
        complete_df = pd.merge(complete_df, ratings_df, on=['od', 'compartment', 'airline'], how='left')
        # - - - - - - Done.
        logging.info("Adding Last Year values . . .")
        final_df = add_ly_values(complete_df)
        logging.info("Time taken to get Last year values in Complete DataFrame = " + str(time.time() - st))
        final_df.drop_duplicates(inplace=True)
        final_df['rating'].fillna(5, inplace=True)
        final_df = add_pseudo_od(final_df)
        # - - - - - - -  - - -     To convert structure of collection to add top_5_competitor_data as an embedded field
        BATCH_SIZE = 100000
        logging.info("Initialized Batch Size as 100000 . . . .")
        structured_dict = []
        logging.info("Grouping per market/month to create top_5_comp_data field. . . . . . . .")
        i = 1
        grouped_by_market_month_df = final_df.groupby(by=['pos', 'od', 'compartment', 'month', 'year'])
        global TOTAL_MARKETS
        TOTAL_MARKETS = len(grouped_by_market_month_df)
        logging.info("Reading User defined competitors from CSV . . .")
        df_user_defined_competitor = pd.DataFrame(list(db.JUP_DB_Web_Pricing_Ad_Hoc.find({'user_defined_comp': {'$ne': None}}, {'_id': 0, 'od': 1, 'user_defined_comp': 1})))
        df_user_defined_competitor['user_defined_comp'] = df_user_defined_competitor['user_defined_comp'].apply(lambda row: [row])
        logging.info("Read and Renamed columns of User defined competitors, ready to merge !")
        batch_iter = 1
        st = time.time()
        for market_month, grouped_airlines in grouped_by_market_month_df:
            # grouped_airlines.sort_values(by=['market_share'], inplace=True, ascending=False)
            if len(structured_dict) < BATCH_SIZE:
                airline_list = list(grouped_airlines['airline'])
                market_share_list = list(grouped_airlines['market_share'])
                fms_list = list(grouped_airlines['FMS'])
                rating_list = list(grouped_airlines['rating'])
                market_share_1_list = list(grouped_airlines['market_share_1'])
                capacity_list = list(grouped_airlines['capacity'])
                pax_list = list(grouped_airlines['pax'])
                market_size_list = list(grouped_airlines['market_size'])
                pax_1_list = list(grouped_airlines['pax_1'])
                market_size_1_list = list(grouped_airlines['market_size_1'])
                capacity_1_list = list(grouped_airlines['capacity_1'])
                fms_1_list = list(grouped_airlines['FMS_1'])
                zipped = zip(airline_list, market_share_list, fms_list, rating_list, market_share_1_list, capacity_list, pax_list, market_size_list, pax_1_list, market_size_1_list, capacity_1_list, fms_1_list)
                airlines_data = []
                for airline_details in zipped:
                    airlines_data.append(
                        {
                            'airline': airline_details[0],
                            'market_share': airline_details[1],
                            'FMS': airline_details[2],
                            'rating': airline_details[3],
                            'market_share_1': airline_details[4],
                            'capacity': airline_details[5],
                            'pax': airline_details[6],
                            'market_size': airline_details[7],
                            'pax_1': airline_details[8],
                            'market_size_1': airline_details[9],
                            'capacity_1': airline_details[10],
                            'FMS_1': airline_details[11]
                        }
                    )
                structured_dict.append(
                    {
                        'pos': grouped_airlines['pos'].values[0],
                        'od': grouped_airlines['od'].values[0],
                        'pseudo_od': grouped_airlines['pseudo_od'].values[0],
                        'compartment': grouped_airlines['compartment'].values[0],
                        'month': grouped_airlines['month'].values[0],
                        'year': grouped_airlines['year'].values[0],
                        'top_5_comp_data': airlines_data
                    }
                )
                if (i % 10000) == 0:
                    logging.info("Done for Market/month number " + str(i) + " out of " + str(TOTAL_MARKETS))
                i += 1
            else:
                logging.info("Time taken to create top_5_comp_data field from denormalized DF for BATCH " + str(batch_iter) + " = " + str(time.time() - st))
                clubbed_df = pd.DataFrame(structured_dict)
                inserting_df = pd.merge(clubbed_df, df_user_defined_competitor, on=['od'], how='left')
                inserting_df['combine_column'] = inserting_df['year'].apply(str) + inserting_df['month'].apply(str) + inserting_df['pos'] + inserting_df['od'] + inserting_df['compartment']
                inserting_df['market'] = inserting_df['pos'] + inserting_df['od'] + inserting_df['compartment']
                inserting_df['month_str'] = inserting_df['month'].apply(lambda row: str_month(row))
                inserting_df['month_year'] = inserting_df['year'].apply(str) + inserting_df['month_str']
                inserting_df.drop(['month_str'], axis=1, inplace=True)
                db.JUP_DB_Pos_OD_Compartment_new.insert(inserting_df.to_dict('records'))
                logging.info("Inserted Markets in Batch " + str(batch_iter) + " ! ")
                structured_dict = []
                batch_iter += 1
                st = time.time()
        #  To insert remaining Markets whose length of remaining markets < BATCH_SIZE
        if len(structured_dict) != BATCH_SIZE:
            logging.info("Time taken to create top_5_comp_data field from denormalized DF for BATCH " + str(batch_iter) + " = " + str(time.time() - st))
            clubbed_df = pd.DataFrame(structured_dict)
            inserting_df = pd.merge(clubbed_df, df_user_defined_competitor, on=['od'], how='left')
            inserting_df['combine_column'] = inserting_df['year'].apply(str) + inserting_df['month'].apply(str) + inserting_df['pos'] + inserting_df['od'] + inserting_df['compartment']
            inserting_df['market'] = inserting_df['pos'] + inserting_df['od'] + inserting_df['compartment']
            inserting_df['month_str'] = inserting_df['month'].apply(lambda row: str_month(row))
            inserting_df['month_year'] = inserting_df['year'].apply(str) + inserting_df['month_str']
            inserting_df.drop(['month_str'], axis=1, inplace=True)
            db.JUP_DB_Pos_OD_Compartment_new.insert(inserting_df.to_dict('records'))
            logging.info("Inserted Markets in Batch " + str(batch_iter) + " ! ")
    except Exception:
        global ERROR_FLAG
        ERROR_FLAG = 1
        logging.info("GOT SOME ERROR")
        logging.info(str(traceback.print_exc()))
    logging.info("Time taken for entire program = " + str(time.time() - start_time))
    return 0


@measure(JUPITER_LOGGER)
def update_completed_status(object_id):
    db.JUP_DB_Data_Status.update(
        {'_id': ObjectId(object_id)},
        {
            'team': 'analytics',
            'type': 'script',
            'level': 2,
            'name': 'JUP_AI_Batch_Market_Characteristics',
            'title': 'pos_od_compartment',
            'stage': 'processing',
            'last_update_date': datetime.datetime.today().strftime('%Y-%m-%d'),
            'last_update_time': datetime.datetime.today().strftime('%H:%M'),
            'start_time': datetime.datetime.today().strftime('%H:%M'),
            'end_time': datetime.datetime.today().strftime('%H:%M'),
            'run_time': time.time() - START_TIME,
            'collections_updated': COLLECTION_UPDATE_LIST,
            'collections_dependencies': COLLECTION_DEPENDENCY_LIST,
            'params_updated': [
                                {
                                    'collection': 'JUP_DB_Pos_OD_Comartment',
                                    'fields': ['top_5_comp_data','pos','od','compartment','month','year'
                                               'combine_column']
                                }
                               ],
            'number_of_params': 1,
            'logs': [
                {
                    'number_of_records': TOTAL_MARKETS
                }
            ],
            'ip': '172.28.23.8',
            'port': '3354',
            'URL': '',
            'status': 'Completed',
            'error': ERROR_FLAG
        }
    )
    return 0


@measure(JUPITER_LOGGER)
def main_helper():
    months_all = [[1, 2, 3], [4, 5, 6]]
    logging.info("Updating Running status on JUP_DB_Data_Status")
    id = update_running_status()
    st = time.time()
    for months in months_all:
        logging.info("Removing old records and generating new records for months = " + str(months))
        db.JUP_DB_Pos_OD_Compartment_new.remove({'month': {'$in': months}})
        main(months)
    logging.info("Time taken to update Collection = " + str(time.time() - st))
    update_completed_status(object_id=id)
    logging.info("Updated completed status on JUP_DB_Data_Status. Program ran successfully.")


if __name__ == '__main__':
    logging.info(" - - - - - POS_OD_COMPARTMENT 1 STARTED ON : " + str(SYSTEM_DATE) + " AT TIME = " + str(time.time()) + " - - - - ")
    main_helper()
