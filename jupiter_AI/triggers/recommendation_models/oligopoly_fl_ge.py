"""
Authors: Logic, DF approach for recommendation, Fare inversion detection: Lord Akshay Karangale
         Structure, Reco rating, Sales_fb_level, correct_inverted_fares: Nikunj Agarwal
Created with <3
Functionality of code:
        Calculates fare recommendation, detects inverted fares, calculates rating for recommended fare
        based on Competitor_Fares_thresholds defined in collection - Comp_Star_Rating_Config,
        calculates Yield and Pax for every farebasis in fareladder

NOTE: Here in main function, right now code has been written assuming dep_date range only as
      next 30 days. If we need to get fares for all the days [7, 14, 30, 45, 60, 90] then,
      dep_date_start would by replaced by extreme_start_date and dep_date_end would be replaced by
      extreme_end_date. So, right now dates_list does not have any use
      but in future (if we need to get fares for all mentioned dates at once, then dates_list
      would contain an array of intermediary dep_date_start and dep_date_end accordingly for 7,14,21,etc)
      So, the functions get_pax_yield and get_sales_fb_level will be affected the most.

COLLECTIONS HIT:
        JUP_DB_ATPCO_FARES_RULES - To get host fares
        JUP_DB_Pos_OD_Compartment - To get Market_share and rating for competitors and host used in formula
        JUP_DB_Manual_Triggers_Module - To get pax and yield at fare basis level

Modifications log:
    1. Author: Akshay
       Exact modification made or some logic changed: YQ to be made twice if OD is connection
       Date of modification: 23/11/2017
    2. Author: Nikunj
       Exact modification made or some logic changed: Added functionality to correct inverted
       recommendations for fares which are not already filed inverted
       Date of modification: 15/12/2017
    3. Author: Nikunj
       Exact modification made or some logic changed: If travel dates exactly match any event period in the OD
       show event name instead of travel dates.
       Date of modification: 16/02/2018

"""

import datetime
import time
import traceback
from copy import deepcopy

import numpy as np
import pandas as pd

from jupiter_AI import client, JUPITER_DB, JUPITER_LOGGER, ATPCO_DB, Host_Airline_Hub, Host_Airline_Code, mongo_client
from jupiter_AI.network_level_params import SYSTEM_DATE, today
from jupiter_AI.triggers.common import pos_level_currency_data as get_currency_data
from jupiter_AI.triggers.host_params_workflow_opt import get_od_distance
from jupiter_AI.logutils import measure
import re

pd.options.mode.chained_assignment = None

RECOMMENDATION_UPPER_THRESHOLD = 20
RECOMMENDATION_LOWER_THRESHOLD = -20
FZ_RATING = 4.6
host_rating = FZ_RATING
host_market_share = 0
host_FMS = 0
host_is_ms_fms = 0

client_1 = mongo_client()
db1 = client_1[JUPITER_DB]
exchange_rate = {}
currency_crsr = list(db1.JUP_DB_Exchange_Rate.find({}))
for curr in currency_crsr:
     exchange_rate[curr['code']] = curr['Reference_Rate']
client_1.close()

@measure(JUPITER_LOGGER)
def get_host_fares_df_all(pos,
                          extreme_start_date,
                          extreme_end_date,
                          origin,
                          destination,
                          compartment,
                          db,
                          comp=[Host_Airline_Code],
                          channel=None):
    currency_data = get_currency_data(db=db)
    currency = []
    try:
        if currency_data[pos]['web']:
            currency.append(currency_data[pos]['web'])
        if currency_data[pos]['gds']:
            currency.append(currency_data[pos]['gds'])
    except KeyError:
        currency = []

    tax_query = []
    od_list = [origin + destination]
    do_list = [destination + origin]
    ods = od_list + do_list
    OD_list = deepcopy(od_list)

    for od in ods:
        tax_query.append({
            "$and": [{
                "Origin": od[0:3],
                "Destination": od[3:]
            }]
        })
    tax_crsr = list(db.JUP_DB_Tax_Master.find({
        'Compartment': compartment,
        '$or': tax_query
    }))
    if len(tax_crsr) != 0:
        tax_df = pd.DataFrame(tax_crsr)
        if 'percentage_block' not in tax_df.columns:
               tax_df['percentage_block'] = None
    else:
        tax_df = pd.DataFrame(columns=['Origin',
                                       'Destination',
                                       'Currency',
                                       'OW_RT',
                                       'Compartment',
                                       'Fixed_tax_breakup',
                                       'Percentage_tax_breakup',
                                       'Fixed_tax',
                                       'percentage_block'])
    tax_df = tax_df[['Origin',
                     'Destination',
                     'Currency',
                     'OW_RT',
                     'Compartment',
                     'Fixed_tax_breakup',
                     'Percentage_tax_breakup',
                     'Fixed_tax',
                     'percentage_block']]
    tax_df['Fixed_tax'] = tax_df['Fixed_tax'].fillna(0)
    tax_df['percentage_block'] = tax_df['percentage_block'].fillna(0)
    tax_currencies = list(tax_df['Currency'].unique())
    tax_df['OD'] = tax_df['Origin'] + tax_df['Destination']
    tax_df = tax_df.rename(
        columns={
            'Currency': 'tax_currency',
            'OW_RT': 'oneway_return',
            'Compartment': 'compartment'})
    tax_df.drop(['Origin', 'Destination'], axis=1, inplace=True)
    tax_df['oneway_return'] = tax_df['oneway_return'].apply(str)

    SYSTEM_DATE_LW = str(today - datetime.timedelta(days=7))
    extreme_start_date_n = "0" + extreme_start_date[2:4] + extreme_start_date[5:7] + extreme_start_date[8:10]
    extreme_end_date_n = "0" + extreme_end_date[2:4] + extreme_end_date[5:7] + extreme_end_date[8:10]
    extreme_start_date_obj = datetime.datetime.strptime(extreme_start_date, "%Y-%m-%d")
    extreme_start_date_lw = extreme_start_date_obj - datetime.timedelta(days=7)
    extreme_start_date_lw = extreme_start_date_lw.strftime("%Y-%m-%d")
    extreme_start_date_n_lw = "0" + extreme_start_date_lw[2:4] + extreme_start_date_lw[5:7] + extreme_start_date_lw[
                                                                                              8:10]
    SYSTEM_DATE_MOD = "0" + SYSTEM_DATE[2:4] + SYSTEM_DATE[5:7] + SYSTEM_DATE[8:10]
    SYSTEM_DATE_MOD_LW = "0" + SYSTEM_DATE_LW[2:4] + SYSTEM_DATE_LW[5:7] + SYSTEM_DATE_LW[8:10]

    cols = [
        "effective_from",
        "effective_to",
        "dep_date_from",
        "dep_date_to",
        "sale_date_from",
        "sale_date_to",
        "fare_basis",
        "fare_brand",
        "RBD",
        "rtg",
        "tariff_code",
        "fare_include",
        "private_fare",
        "footnote",
        "batch",
        "origin",
        "destination",
        "OD",
        "compartment",
        "oneway_return",
        "channel",
        "carrier",
        "fare",
        "surcharge_date_start_1",
        "surcharge_date_start_2",
        "surcharge_date_start_3",
        "surcharge_date_start_4",
        "surcharge_date_end_1",
        "surcharge_date_end_2",
        "surcharge_date_end_3",
        "surcharge_date_end_4",
        "surcharge_amount_1",
        "surcharge_amount_2",
        "surcharge_amount_3",
        "surcharge_amount_4",
        "Average_surcharge",
        "total_fare_1",
        "total_fare_2",
        "total_fare_3",
        "total_fare_4",
        "total_fare",
        "YR",
        "YQ",
        "taxes",
        "currency",
        "fare_rule",
        "RBD_type",
        "gfs",
        "last_update_date",
        "last_update_time",
        "competitor_farebasis",
        "travel_date_to",
        "travel_date_from",
        "retention_fare",
        "flight_number",
        "day_of_week",
        'tax_currency',
        'Fixed_tax',
        'percentage_block',
        'Fixed_tax_breakup',
        'Percentage_tax_breakup'
    ]

    print "Aggregating on Temp Fares Collection . . . . "

    # fares_df = fares_df[fares_df['currency'].isin(currency)]
    #
    # fares_df = fares_df[fares_df['OD'].isin(ods)]
    #
    # fares_df = fares_df[fares_df['compartment'] == compartment]
    #
    # fares_df["Footnotes_Cat_14_FN"] = fares_df["Footnotes"].apply(lambda x: x['Cat_14_FN'])
    #
    # fares_df['travel_flag'] = fares_df['Footnotes_Cat_14_FN'].apply(
    #     lambda x: travel_dates_check(x, extreme_start_date, extreme_end_date))
    #
    # fares_df = fares_df[fares_df['travel_flag']]

    query = {
        "$and": [
            {
                "currency": {
                    "$in": currency
                }
            },
            {
                "OD": {
                    "$in": ods
                }
            },
            {
                "compartment": compartment
            },
            {
                "$or": [
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": {}
                            },
                            {
                                "cat_14": {}
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": None
                            },
                            {
                                "cat_14": None
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_COMM": {
                                    "$eq": "0999999"
                                }
                            },
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_EXP": {
                                    "$gte": extreme_start_date_n_lw  # to show expired fares
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_COMM": {
                                    "$lte": extreme_end_date_n
                                }
                            },
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_EXP": {
                                    "$eq": "0000000"
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_COMM": "0999999"
                            },
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_EXP": None
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_COMM": None
                            },
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_EXP": "0000000"
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": {}
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": {
                                    "$lte": extreme_end_date_n
                                }
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": {
                                    "$gte": extreme_start_date_n_lw  # to include expired fares
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": {}
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": {
                                    "$lte": extreme_end_date_n
                                }
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": {
                                    "$eq": "0000000"
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": {}
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": "0999999"
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": {
                                    "$gte": extreme_start_date_n_lw  # to include expired fares
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": {}
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": "0999999"
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": "0000000"
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": {}
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": {
                                    "$lte": extreme_end_date_n
                                }
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": None
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": {}
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": "0999999"
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": None
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": {}
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": None
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": {
                                    "$gte": extreme_start_date_n_lw
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": {}
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": None
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": "0000000"
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": {}
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": None
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": None
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": None
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": {
                                    "$lte": extreme_end_date_n
                                }
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": {
                                    "$gte": extreme_start_date_n_lw
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": None
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": {
                                    "$lte": extreme_end_date_n
                                }
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": {
                                    "$eq": "0000000"
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": None
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": "0999999"
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": {
                                    "$gte": extreme_start_date_n_lw
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": None
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": "0999999"
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": "0000000"
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": None
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": {
                                    "$lte": extreme_end_date_n
                                }
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": None
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": None
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": "0999999"
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": None
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": None
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": None
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": {
                                    "$gte": extreme_start_date_n_lw
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": None
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": None
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": "0000000"
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": None
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": None
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": None
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_COMM": {
                                    "$lte": extreme_end_date_n
                                }
                            },
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_EXP": {
                                    "$gte": extreme_start_date_n_lw
                                }
                            }
                        ]
                    },
                    # {
                    #     "$and": [
                    #         {
                    #             "Footnotes": {}
                    #         }
                    #     ]
                    # },
                    # {
                    #     "$and": [
                    #         {
                    #             "Footnotes": None
                    #         }
                    #     ]
                    # },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_COMM": {
                                    "$lte": extreme_end_date_n
                                }
                            },
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_EXP": None
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_COMM": None
                            },
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_EXP": {
                                    "$gte": extreme_start_date_n_lw
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_COMM": "0000000"
                            },
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_EXP": "0999999"
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_COMM": "0999999"
                            },
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_EXP": "0000000"
                            }
                        ]
                    },
                    # {
                    #     "$and": [
                    #         {
                    #             "Footnotes.Cat_14_FN.TRAVEL_DATES_COMM": None
                    #         },
                    #         {
                    #             "Footnotes.Cat_14_FN.TRAVEL_DATES_EXP": None
                    #         }
                    #     ]
                    # }
                ]
            }

        ]
    }
    # print json.dumps(query)

    fares_data = list(db.temp_fares_triggers.aggregate(
        [
            {
                '$match': query
            }
        ]
    ))
    dF = pd.DataFrame(columns=cols)

    print 'Fares data length:', len(fares_data)

    # exchange_rate = {}
    # pos_country = list(db.JUP_DB_Region_Master.find({"POS_CD": pos}))[0]['COUNTRY_CD']
    # yr_crsr = list(db.JUP_DB_YR_Master.find({"POS-LocCode": pos_country, "Compartment": compartment}))
    # if len(yr_crsr) == 0:
    #     yr_crsr = list(db.JUP_DB_YR_Master.find({"POS-LocCode": "MISC"}))
    # yr_curr = yr_crsr[0]['Curr']
    # yr = {}
    # tax_crsr = list(db.JUP_DB_Tax_Master.find({
    #     'Compartment': compartment,
    #     '$or': tax_query
    # }))
    # if len(tax_crsr) != 0:
    #     tax_df = pd.DataFrame(tax_crsr)
    # else:
    #     tax_df = pd.DataFrame(columns=['Origin',
    #                                    'Destination',
    #                                    'Currency',
    #                                    'OW_RT',
    #                                    'Compartment',
    #                                    'Fixed_tax_breakup',
    #                                    'Percentage_tax_breakup',
    #                                    'Fixed_tax',
    #                                    'Percent_tax'])
    # tax_df['Fixed_tax'] = tax_df['Fixed_tax'].fillna(0)
    # tax_df['Percent_tax'] = tax_df['Percent_tax'].fillna(0)
    # tax_currencies = list(tax_df['Currency'].unique())
    # tax_df['OD'] = tax_df['Origin'] + tax_df['Destination']
    # tax_df = tax_df.rename(
    #     columns={
    #         'Currency': 'tax_currency',
    #         'OW_RT': 'oneway_return'})
    # tax_df.drop(['Origin', 'Destination', 'Compartment'], axis=1, inplace=True)
    # tax_df['oneway_return'] = tax_df['oneway_return'].apply(str)
    # tax_df = tax_df.rename(columns={"Compartment": "compartment"})
    # Implementing YQ from YQ master collection
    # yq_data = list(db.JUP_DB_YQ_Master.find({'compartment': compartment}))
    # yq_df = pd.DataFrame(yq_data)
    # yq_df.drop(['_id', 'compartment'], axis=1, inplace=True)
    # yq_df = yq_df.rename(
    #     columns={
    #         'amount': 'amount_yq',
    #         'currency': 'currency_yq'})
    # yq_currencies = list(yq_df['currency_yq'].unique())
    # yq_df['oneway_return'] = yq_df['oneway_return'].apply(str)

    # currency_crsr = list(db.JUP_DB_Exchange_Rate.find(
    #     {"code": {"$in": currency + tax_currencies}}))
    # for curr in currency_crsr:
    #     exchange_rate[curr['code']] = curr['Reference_Rate']

    # currencies = list(set(currency + tax_currencies))

    # for currency_ in currency:
    #     if yr_curr != currency_:
    #         yr_val = (yr_crsr[0]['Amount'] /
    #                   exchange_rate[yr_curr]) * exchange_rate[currency_]
    #         yr[currency_] = yr_val
    #     else:
    #         yr[currency_] = yr_crsr[0]['Amount']

    if len(fares_data) > 0:
        dF = pd.DataFrame(fares_data)
        dF.fillna(-999, inplace=True)
        dF['surcharge_amount_1'] = 0
        dF['surcharge_amount_2'] = 0
        dF['surcharge_amount_3'] = 0
        dF['surcharge_amount_4'] = 0

        dF['surcharge_date_start_1'] = "NA"
        dF['surcharge_date_start_2'] = "NA"
        dF['surcharge_date_start_3'] = "NA"
        dF['surcharge_date_start_4'] = "NA"

        dF['surcharge_date_end_1'] = "NA"
        dF['surcharge_date_end_2'] = "NA"
        dF['surcharge_date_end_3'] = "NA"
        dF['surcharge_date_end_4'] = "NA"
        dF['flight_number'] = "--"
        dF['day_of_week'] = "--"
        dF = dF.merge(tax_df, on=['OD', 'oneway_return', 'compartment'], how='left')
        dF[['Fixed_tax']] = dF[['Fixed_tax']].fillna(0)
        dF[['Fixed_tax_breakup', 'Percentage_tax_breakup']] = dF[
            ['Fixed_tax_breakup', 'Percentage_tax_breakup']].fillna("")
        dF[['tax_currency']] = dF[['tax_currency']].fillna(-999)

        dF['Fixed_tax'] = dF.apply(lambda row: convert_tax_currency(row['Fixed_tax'],
                                                                    row['currency'],
                                                                    row['tax_currency']), axis=1)
        dF['Fixed_tax'] = dF['Fixed_tax'].fillna(0)
        dF['percentage_block'] = dF['percentage_block'].fillna(0)
        dF['percentage_block'] = dF.apply(lambda row: convert_tax_percentage(row['percentage_block'],
                                                                             row['currency'], row['fare']), axis=1)
        dF['taxes'] = dF['Fixed_tax'] + dF['percentage_block']

        # Merge YQ df and fares df(that has already been merged with tax df) to
        # compute yq
        # dF = dF.merge(yq_df, on=['oneway_return'], how='left')
        # dF['YQ'] = dF.apply(lambda row: row['amount_yq'] /
        #                     exchange_rate[row['currency']] *
        #                     exchange_rate[row['currency_yq']], axis=1)
        zone_crsr = db.JUP_DB_ATPCO_Zone_Master.find({'CITY_CODE': pos}, {'_id': 0, 'FILLER': 0,
                                                                          'CITY_STATE': 0, 'REC_TYPE': 0})
        zone_list = list(zone_crsr)

        for i in range(len(dF)):
            has_category = False
            has_category_15 = False
            tvl_dates = []
            sale_dates = []
            surchrg_amts = []
            try:
                dates_array = dF['Footnotes'][i]['Cat_14_FN']
                has_category = True
            except KeyError:
                try:
                    dates_array = dF['cat_14'][i]
                    if dates_array != -999:
                        has_category = True
                    else:
                        dF.loc[i, 'travel_date_from'] = "1900-01-01"
                        dF.loc[i, 'travel_date_to'] = "2099-12-31"

                except KeyError:
                    dF.loc[i, 'travel_date_from'] = "1900-01-01"
                    dF.loc[i, 'travel_date_to'] = "2099-12-31"
            except TypeError:
                try:
                    dates_array = dF['cat_14'][i]
                    if dates_array != -999:
                        has_category = True
                    else:
                        dF.loc[i, 'travel_date_from'] = "1900-01-01"
                        dF.loc[i, 'travel_date_to'] = "2099-12-31"
                except KeyError:
                    dF.loc[i, 'travel_date_from'] = "1900-01-01"
                    dF.loc[i, 'travel_date_to'] = "2099-12-31"
            if has_category:
                for entry in dates_array:
                    try:
                        tvl_dates.append(
                            str(entry['TRAVEL_DATES_COMM']) + str(entry['TRAVEL_DATES_EXP']))
                    except:
                        pass
                min_indx = np.argmin(tvl_dates)
                from_date = str(dates_array[min_indx]['TRAVEL_DATES_COMM'])
                to_date = str(dates_array[min_indx]['TRAVEL_DATES_EXP'])
                if len(from_date) == 7:
                    from_date = from_date[1:]
                if len(to_date) == 7:
                    to_date = to_date[1:]
                try:
                    from_date = datetime.datetime.strftime(
                        datetime.datetime.strptime(
                            from_date, "%y%m%d"), "%Y-%m-%d")
                except ValueError:
                    from_date = "1900-01-01"
                try:
                    to_date = datetime.datetime.strftime(
                        datetime.datetime.strptime(
                            to_date, "%y%m%d"), "%Y-%m-%d")
                except ValueError:
                    to_date = "2099-12-31"
                dF.loc[i, 'travel_date_from'] = from_date
                dF.loc[i, 'travel_date_to'] = to_date

            try:
                dates_array_15 = dF['Footnotes'][i]['Cat_15_FN']
                has_category_15 = True
            except KeyError:
                try:
                    dates_array_15 = dF['cat_15'][i]
                    if dates_array_15 != -999:
                        has_category_15 = True
                    else:
                        dF.loc[i, 'sale_date_from'] = "1900-01-01"
                        dF.loc[i, 'sale_date_to'] = "2099-12-31"
                except KeyError:
                    dF.loc[i, 'sale_date_from'] = "1900-01-01"
                    dF.loc[i, 'sale_date_to'] = "2099-12-31"
            except TypeError:
                try:
                    dates_array_15 = dF['cat_15'][i]
                    if dates_array_15 != -999:
                        has_category_15 = True
                    else:
                        dF.loc[i, 'sale_date_from'] = "1900-01-01"
                        dF.loc[i, 'sale_date_to'] = "2099-12-31"
                except KeyError:
                    dF.loc[i, 'sale_date_from'] = "1900-01-01"
                    dF.loc[i, 'sale_date_to'] = "2099-12-31"
            if has_category_15:
                for entry in dates_array_15:
                    try:
                        sale_dates.append(
                            str(entry['SALE_DATES_EARLIEST_TKTG']) + str(entry['SALE_DATES_LATEST_TKTG']))
                    except:
                        pass
                min_indx = np.argmin(sale_dates)
                from_date = str(
                    dates_array_15[min_indx]['SALE_DATES_EARLIEST_TKTG'])
                to_date = str(
                    dates_array_15[min_indx]['SALE_DATES_LATEST_TKTG'])
                if len(from_date) == 7:
                    from_date = from_date[1:]
                if len(to_date) == 7:
                    to_date = to_date[1:]
                try:
                    from_date = datetime.datetime.strftime(
                        datetime.datetime.strptime(
                            from_date, "%y%m%d"), "%Y-%m-%d")
                except ValueError:
                    from_date = "1900-01-01"
                try:
                    to_date = datetime.datetime.strftime(
                        datetime.datetime.strptime(
                            to_date, "%y%m%d"), "%Y-%m-%d")
                except ValueError:
                    to_date = "2099-12-31"
                dF.loc[i, 'sale_date_from'] = from_date
                dF.loc[i, 'sale_date_to'] = to_date
            # except KeyError:
            #     dF.loc[i, 'sale_date_from'] = "1900-01-01"
            #     dF.loc[i, 'sale_date_to'] = "2099-12-31"
            # except TypeError:
            #     dF.loc[i, 'sale_date_from'] = "1900-01-01"
            #     dF.loc[i, 'sale_date_to'] = "2099-12-31"
            """Not reading cat_12 from the gen rules."""
            # try:
            #     t = dF['Gen Rules'][i]['cat_12'][0]
            #     cat_12_gen = dF['Gen Rules'][i]['cat_12']
            # except:
            #     cat_12_gen = []
            cat_12_gen = []

            """Changed the concept of reading the cat_12"""
            # try:
            #     t = dF['cat_12'][i][0]
            #     cat_12 = dF['cat_12'][i]
            # except:
            #     cat_12 = []
            cat_12 = []
            cat_15 = []
            try:
                for q in range(len(dF['cat_12'][i])):
                    try:
                        if dF['cat_12'][i][q]['CAT_NO'] == '015':
                            cat_15.append(dF['cat_12'][i][q])
                        else:
                            cat_12.append(dF['cat_12'][i][q])
                    except:
                        pass
            except:
                cat_12 = []

            if len(cat_12) > 0 and len(cat_15) > 0:
                # print cat_15
                binary_value = cat_15_cond(cat_15=cat_15[0], pos=pos, origin=origin,
                                           destination=destination, zone_list=zone_list)

                if binary_value == -1:
                    cat_12 = []
            try:
                cat_12_list = cat_12 + cat_12_gen
                for entry in cat_12_list:
                    key = str(entry['DATE_RANGE_START']) + \
                          str(entry['DATE_RANGE_STOP'])
                    if key == "":
                        key = "000101991231"
                    surchrg_amts.append({
                        "key": key,
                        "surcharge_pct": float(entry['CHARES_PERCENT'][0:3] + "." + entry['CHARES_PERCENT'][3:]),
                        "surcharge_amt1": int(entry['CHARGES_AMT']),
                        "surcharge_amt2": int(entry['CHARGES_AMT2']),
                        "currency": (entry['CHARGES_1ST_CUR'] + entry['CHARGES_2ND_CUR'])[0:3],
                        "start_date": entry['DATE_RANGE_START'],
                        "end_date": entry['DATE_RANGE_STOP'],
                        "dec1": entry['CHARGES_DEC'],
                        "dec2": entry['CHARGES_DEC2']})
                temp_df = pd.DataFrame(surchrg_amts)
                temp_df.sort_values(by='key', inplace=True)
                temp_df['dec2'].replace(to_replace="", value="0", inplace=True)
                temp_df['dec1'].replace(to_replace="", value="0", inplace=True)
                temp_df['dec2'] = temp_df['dec2'].astype('int')
                temp_df['dec1'] = temp_df['dec1'].astype('int')

                temp_df.loc[temp_df['currency'] != "", 'surcharge_amt1'] = temp_df[
                    temp_df['currency'] != ""].apply(
                    lambda row: convert_tax_currency(row['surcharge_amt1'],
                                                     dF.loc[i, 'currency'],
                                                     row['currency']), axis=1)

                temp_df.loc[temp_df['currency'] != "", 'surcharge_amt2'] = temp_df.loc[
                    temp_df['currency'] != "", :].apply(
                    lambda row: convert_tax_currency(row['surcharge_amt2'],
                                                     dF.loc[i, 'currency'],
                                                     row['currency']), axis=1)
                total_surcharge = 0
                surcharge_count = 0
                for j in range(len(temp_df)):
                    try:
                        from_date = datetime.datetime.strftime(datetime.datetime.strptime(
                            str(temp_df.loc[j, 'start_date']), "%y%m%d"), "%Y-%m-%d")
                    except ValueError:
                        from_date = "1900-01-01"
                    try:
                        to_date = datetime.datetime.strftime(datetime.datetime.strptime(
                            str(temp_df.loc[j, 'end_date']), "%y%m%d"), "%Y-%m-%d")
                    except ValueError:
                        to_date = "2099-12-31"
                    dF.loc[i, 'surcharge_amount_' + str(j + 1)] = temp_df.loc[j, 'surcharge_pct'] * 0.01 * dF.loc[
                        i, 'fare'] \
                                                                  + temp_df.loc[j, 'surcharge_amt1'] / 10 ** \
                                                                  temp_df.loc[j, 'dec1'] \
                                                                  + temp_df.loc[j, 'surcharge_amt2'] / 10 ** \
                                                                  temp_df.loc[j, 'dec2']

                    dF.loc[i, 'surcharge_date_start_' + str(j + 1)] = from_date
                    dF.loc[i, 'surcharge_date_end_' + str(j + 1)] = to_date
                    surcharge_count += 1
                    total_surcharge += dF.loc[i, 'surcharge_amount_' + str(j + 1)]

                #dF.loc[i, 'Average_surcharge'] = total_surcharge/surcharge_count
                dF.loc[i, 'Average_surcharge'] = 0
            except KeyError:
                for j in range(4):
                    dF.loc[i, 'surcharge_amount_' + str(j + 1)] = 0
                    dF.loc[i, 'surcharge_date_start_' +
                           str(j + 1)] = "1900-01-01"
                    dF.loc[i, 'surcharge_date_end_' +
                           str(j + 1)] = "2099-12-31"
                dF.loc[i, 'Average_surcharge'] = 0
            except TypeError:
                for j in range(4):
                    dF.loc[i, 'surcharge_amount_' + str(j + 1)] = 0
                    dF.loc[i, 'surcharge_date_start_' +
                           str(j + 1)] = "1900-01-01"
                    dF.loc[i, 'surcharge_date_end_' +
                           str(j + 1)] = "2099-12-31"
                dF.loc[i, 'Average_surcharge'] = 0

            try:
                cat_4 = dF.loc[i, 'cat_4']
                temp = ""
                for element in cat_4:
                    try:
                        temp_st = element['CXR_FLT']['SEGS_CARRIER']
                        if len(temp_st) % 14 != 0:
                            temp_st = temp_st + "    "
                        for m in range(0, len(temp_st), 14):
                            seg = temp_st[m:m + 14]
                            op_cxr = seg[0:2]
                            mrkt_cxr = seg[3:5]
                            range_start = seg[6:10]
                            range_end = seg[10:]
                            if op_cxr == "  ":
                                op_cxr = "--"
                            if mrkt_cxr == "  ":
                                mrkt_cxr = "--"
                            if ((range_start == "****") or (range_end == "****")) and (
                                    (op_cxr == Host_Airline_Code) or (mrkt_cxr == Host_Airline_Code)):
                                temp = temp + op_cxr + " " + mrkt_cxr + " " + "****, "
                            elif (op_cxr == Host_Airline_Code) or (mrkt_cxr == Host_Airline_Code):
                                temp = temp + op_cxr + " " + mrkt_cxr + " " + range_start + "-" + range_end + ", "
                    except KeyError:
                        pass

                    for k in range(3):
                        key_name_cxr = "CXR" + str(k + 1)
                        key_name_flt = "FLT_NO" + str(k + 1)
                        if element[key_name_cxr] == Host_Airline_Code:
                            temp = temp + element[key_name_cxr] + " " + str(element[key_name_flt]) + ", "

                dF.loc[i, 'flight_number'] = temp[:-2]
            except KeyError:
                dF.loc[i, 'flight_number'] = "--"
            except TypeError:
                dF.loc[i, 'flight_number'] = "--"

            try:
                cat_2 = dF.loc[i, 'cat_2']
                unique_list = []
                temp = ""
                for element in cat_2:
                    for wkday in element['DAYOFWEEK']:
                        if wkday not in unique_list:
                            unique_list.append(wkday)
                            temp = temp + wkday + ", "
                dF.loc[i, 'day_of_week'] = temp[:-2]
            except KeyError:
                dF.loc[i, 'day_of_week'] = "--"
            except TypeError:
                dF.loc[i, 'day_of_week'] = "--"

        dF.replace(0, np.NaN, inplace=True)

        dF.replace(np.NaN, 0, inplace=True)

        dF['dep_date_from'] = extreme_start_date
        dF['dep_date_to'] = extreme_end_date
        # dF['YR'] = 0
        # for i in currency:
        #     dF.loc[dF['currency'] == i, 'YR'] = yr[i]
        # dF['YR'] = dF['YR'] * dF['oneway_return'].astype("int")
        dF.replace(-999, np.nan, inplace=True)
        dF['fare'].fillna(0, inplace=True)
        if 'YQ' in list(dF.columns):
            dF['YQ'].fillna(0, inplace=True)
        else:
            dF['YQ'] = 0
        # if "DXB" not in origin+destination:
        #     dF['YQ'] = dF['YQ'] * 2
        if 'YR' in list(dF.columns):
            dF['YR'].fillna(0, inplace=True)
        else:
            dF['YR'] = 0
        # dF.rename(columns={'YQ_TEST': 'YQ', 'YR_TEST': 'YR'}, inplace=True)
        dF['taxes'].fillna(0, inplace=True)
        dF['surcharge_amount_1'].fillna(0, inplace=True)
        dF['surcharge_amount_2'].fillna(0, inplace=True)
        dF['surcharge_amount_3'].fillna(0, inplace=True)
        dF['surcharge_amount_4'].fillna(0, inplace=True)
        dF['Average_surcharge'].fillna(0, inplace=True)
        dF['total_fare_1'] = dF['fare'] + dF['YQ'] + dF['YR'] + dF['surcharge_amount_1'] + dF['taxes']
        dF['total_fare_2'] = dF['fare'] + dF['YQ'] + dF['YR'] + dF['surcharge_amount_2'] + dF['taxes']
        dF['total_fare_3'] = dF['fare'] + dF['YQ'] + dF['YR'] + dF['surcharge_amount_3'] + dF['taxes']
        dF['total_fare_4'] = dF['fare'] + dF['YQ'] + dF['YR'] + dF['surcharge_amount_4'] + dF['taxes']
        dF['total_fare'] = dF['fare'] + dF['YQ'] + dF['YR'] + dF['Average_surcharge'] + dF['taxes']
        dF['retention_fare'] = dF['total_fare'] - dF['taxes']
        dF = dF[cols]
        dF['effective_to'] = dF['effective_to'].fillna("2099-12-31")
        dF['is_expired'] = 0
        dF.loc[(dF['effective_to'] < SYSTEM_DATE) |
               (dF['sale_date_to'] < SYSTEM_DATE) |
               ((dF['travel_date_to'] >= extreme_start_date_lw)
                & (dF['travel_date_to'] < extreme_start_date)), 'is_expired'] = 1
        dF = dF.sort_values("fare")
    dF = dF[dF['effective_from'] < dF['effective_to']]
    print "Total number of effective fares = ", len(dF)
    dF.reset_index(inplace=True)
    dF['display_only'] = 1
    dF.loc[dF['OD'].isin(OD_list), 'display_only'] = 0

    dF['travel_flag_check'] = False

    return dF


@measure(JUPITER_LOGGER)
def get_filtered_df(dep_date_start, dep_date_end, fares_df):
    for i in range(len(fares_df)):
        try:
            dates_array = fares_df['Footnotes'][i]['Cat_14_FN']
            for entry in dates_array:

                if not (entry['TRAVEL_DATES_COMM_1'] <= entry['TRAVEL_DATES_EXP_1'] < dep_date_start) or \
                        (dep_date_end < entry['TRAVEL_DATES_COMM_1'] <= entry['TRAVEL_DATES_EXP_1']):
                    fares_df.loc[i, 'travel_flag_check'] = True
                    break

        except KeyError:
            try:
                dates_array = fares_df['cat_14'][i]
                for entry in dates_array:

                    if not (entry['TRAVEL_DATES_COMM_1'] <= entry['TRAVEL_DATES_EXP_1'] < dep_date_start) or \
                            (dep_date_end < entry['TRAVEL_DATES_COMM_1'] <= entry['TRAVEL_DATES_EXP_1']):
                        fares_df.loc[i, 'travel_flag_check'] = True
                        break

            except KeyError:
                fares_df.loc[i, 'travel_flag_check'] = True

            except TypeError:
                fares_df.loc[i, 'travel_flag_check'] = True

        except TypeError:
            try:
                dates_array = fares_df['cat_14'][i]
                for entry in dates_array:

                    if not (entry['TRAVEL_DATES_COMM_1'] <= entry['TRAVEL_DATES_EXP_1'] < dep_date_start) or \
                            (dep_date_end < entry['TRAVEL_DATES_COMM_1'] <= entry['TRAVEL_DATES_EXP_1']):
                        fares_df.loc[i, 'travel_flag_check'] = True
                        break
            except KeyError:
                fares_df.loc[i, 'travel_flag_check'] = True

            except TypeError:
                fares_df.loc[i, 'travel_flag_check'] = True

    fares_df = fares_df[fares_df['travel_flag_check']]
    fares_df['dep_date_from'] = dep_date_start
    fares_df['dep_date_to'] = dep_date_end

    return fares_df


@measure(JUPITER_LOGGER)
def get_most_popular_fare(
        pos,
        origin,
        destination,
        compartment,
        dep_date_start,
        dep_date_end,
        db,
        oneway_return=2,
        date_list=None):
    date_list = [{'start': dep_date_start, 'end': dep_date_end}]
    st = time.time()
    mapping_crsr = db.JUP_DB_City_Airport_Mapping.find(
        {"Airport_Code": {"$in": [origin, destination]}})
    origin_temp = origin
    destination_temp = destination
    for i in mapping_crsr:
        if i['Airport_Code'] == origin:
            origin_temp = i['City_Code']
        else:
            destination_temp = i['City_Code']
    if pos != origin_temp:
        most_avail_fare_dF = pd.DataFrame(
            columns=[
                'carrier',
                'most_avail_fare_base_ow',
                'most_avail_fare_total_ow',
                'most_avail_fare_tax_ow',
                'most_avail_fare_count_ow',
                'most_avail_fare_base_rt',
                'most_avail_fare_total_rt',
                'most_avail_fare_tax_rt',
                'most_avail_fare_count_rt',
                'currency',
                'fare_basis_ow',
                'fare_basis_rt',
                'observation_date_ow',
                'observation_date_rt'
            ]
        )
        most_avail_fare_dF.set_value(0, 'currency', "NA")
        most_avail_fare_dF.fillna("NA", inplace=True)
    else:
        # print "in else"
        currency_data = get_currency_data(db=db)
        try:
            currency = currency_data[pos]['web']
        except KeyError:
            currency = 'NA'
        # temp_col_name = gen_collection_name()
        most_avail_fare_dF = pd.DataFrame(
            columns=[
                'carrier',
                'most_avail_fare_base_ow',
                'most_avail_fare_total_ow',
                'most_avail_fare_tax_ow',
                'most_avail_fare_count_ow',
                'most_avail_fare_base_rt',
                'most_avail_fare_total_rt',
                'most_avail_fare_tax_rt',
                'most_avail_fare_count_rt',
                'currency',
                'fare_basis_ow',
                'fare_basis_rt',
                'observation_date_ow',
                'observation_date_rt'
            ]
        )
        query = list()
        query.append({'od': {"$in": [origin_temp + destination_temp]}})
        query.append({'compartment': compartment})
        query.append({'price_inc': {'$ne': 0}})
        # query.append({'currency': currency})

        if dep_date_start and dep_date_end:
            query.append({'outbound_departure_date': {
                '$gte': dep_date_start, '$lte': dep_date_end}})
        else:
            query.append(
                {'outbound_departure_date': {'$gte': SYSTEM_DATE}})

        crsr = db.JUP_DB_Infare.aggregate(
            # Pipeline
            [
                # Stage 1
                {
                    '$match': {"$and": query}
                }, {
                "$group": {
                    "_id": {
                        "od": "$od",
                        "observation_date": "$observation_date",
                        "observation_time": "$observation_time",
                        "is_one_way": "$is_one_way",
                        "carrier": "$carrier",
                        "outbound_flight_no": "$outbound_flight_no",
                        "outbound_departure_date": "$outbound_departure_date",
                        "outbound_departure_time": "$outbound_departure_time",
                        "price_exc": "$price_exc",
                        "price_inc": "$price_inc",
                        "tax": "$tax",
                        "currency": "$currency",
                        "compartment": "$compartment",
                        "fb": "$outbound_fare_basis"
                    }
                }
            },
                {
                    '$group':
                        {
                            '_id': {"is_one_way": "$_id.is_one_way",
                                    "carrier": "$_id.carrier"},
                            "max_value": {"$max": "$_id.observation_date"},
                            "docs": {
                                "$push": {
                                    "od": "$_id.od",
                                    "observation_date": "$_id.observation_date",
                                    "is_one_way": "$_id.is_one_way",
                                    "carrier": "$_id.carrier",
                                    "outbound_flight_no": "$_id.outbound_flight_no",
                                    "outbound_departure_date": "$_id.outbound_departure_date",
                                    "price_exc": "$_id.price_exc",
                                    "price_inc": "$_id.price_inc",
                                    "tax": "$_id.tax",
                                    "currency": "$_id.currency",
                                    "compartment": "$_id.compartment",
                                    "fb": "$_id.fb"
                                }
                            }

                        }
                },
                # Stage 2
                {
                    "$unwind": {
                        "path": "$docs"
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "od": "$docs.od",
                        "observation_date": "$docs.observation_date",
                        "is_one_way": "$docs.is_one_way",
                        "carrier": "$docs.carrier",
                        "outbound_flight_no": "$docs.outbound_flight_no",
                        "dep_date": "$docs.outbound_departure_date",
                        "price_exc": "$docs.price_exc",
                        "price_inc": "$docs.price_inc",
                        "tax": "$docs.tax",
                        "fb": "$docs.fb",
                        "currency": "$docs.currency",
                        "compartment": "$docs.compartment",
                        "cmp_value": {
                            "$cmp": ["$max_value", "$docs.observation_date"]
                        }
                    }
                }, {
                "$match": {
                    "cmp_value": 0
                }
            },
                {
                    "$sort": {
                        "observation_time": -1
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "is_one_way": "$is_one_way",
                            "carrier": "$carrier",
                            "outbound_flight_no": "$outbound_flight_no",
                            "od": "$od",
                            "compartment": "$compartment",
                            "dep_date": "$dep_date"
                        },
                        "doc": {
                            "$first": "$$ROOT"
                        }
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "carrier": "$doc.carrier",
                        "od": "$doc.od",
                        "is_one_way": "$doc.is_one_way",
                        "observation_date": "$doc.observation_date",
                        "outbound_flight_no": "$doc.outbound_flight_no",
                        "dep_date": "$doc.dep_date",
                        "fb": "$doc.fb",
                        "tax": "$doc.tax",
                        "price_exc": "$doc.price_exc",
                        "currency": "$doc.currency",
                        "price_inc": "$doc.price_inc",
                        "compartment": "$doc.compartment"
                    }
                }
            ],
            allowDiskUse=True
        )
        df_1 = pd.DataFrame(list(crsr))
        #	df_1['price_inc'] = df_1['price_exc'] + df_1['tax']
        # exchange_rate = {}
        # currency_crsr = list(
        #     db.JUP_DB_Exchange_Rate.find({}))
        # for curr in currency_crsr:
        #     exchange_rate[curr['code']] = curr['Reference_Rate']
        # df_1['frequency'] = 1
        # print df_1
        most_avail_fare_dF = pd.DataFrame()
        # print "len", len(df_1)
        if len(df_1) > 0:
            df_1['price_inc'] = df_1.apply(lambda row: (
                                                               row['price_inc'] / exchange_rate[currency]) *
                                                       exchange_rate[row['currency']], axis=1)
            df_1['price_exc'] = df_1.apply(lambda row: (
                                                               row['price_exc'] / exchange_rate[currency]) *
                                                       exchange_rate[row['currency']], axis=1)
            df_1['tax'] = df_1.apply(lambda row: (
                                                         row['tax'] / exchange_rate[currency]) * exchange_rate[
                                                     row['currency']], axis=1)

            comps = df_1['carrier'].unique()
            if date_list:
                for date_ in date_list:
                    for comp in comps:
                        tmp_df = df_1[(df_1['dep_date'] >= date_['start']) & (
                                df_1['dep_date'] <= date_['end']) & (df_1['carrier'] == comp)]
                        tmp_df = tmp_df.groupby(
                            by=[
                                'carrier',
                                'price_inc',
                                'is_one_way',
                                'tax',
                                'price_exc',
                                'dep_date',
                                'fb',
                                'observation_date'])['outbound_flight_no'].nunique()
                        tmp_df = tmp_df.reset_index()
                        tmp_df = tmp_df.groupby(
                            by=[
                                'carrier',
                                'price_exc',
                                'tax',
                                'is_one_way',
                                'price_inc',
                                'fb',
                                'observation_date'],
                            as_index=False)['outbound_flight_no'].sum()
                        tmp_df = tmp_df.rename(
                            columns={
                                "outbound_flight_no": "most_available_fare_count"})
                        max_ow = tmp_df[(tmp_df['is_one_way'] == 1)
                        ]['most_available_fare_count'].max()
                        max_rt = tmp_df[(tmp_df['is_one_way'] == 0)
                        ]['most_available_fare_count'].max()
                        tmp_df['date_start'] = date_['start']
                        tmp_df['date_end'] = date_['end']
                        ow = tmp_df[tmp_df['is_one_way'] == 1]
                        ow['most_avail_fare_freq_ow'] = ow['most_available_fare_count'] / \
                                                        ow['most_available_fare_count'].sum()
                        rt = tmp_df[tmp_df['is_one_way'] == 0]
                        rt['most_avail_fare_freq_rt'] = rt['most_available_fare_count'] / \
                                                        rt['most_available_fare_count'].sum()
                        ow = ow.rename(
                            columns={
                                "price_inc": "most_avail_fare_total_ow",
                                "price_exc": "most_avail_fare_base_ow",
                                "tax": "most_avail_fare_tax_ow",
                                "most_available_fare_count": "most_avail_fare_count_ow",
                                "fb": "fare_basis_ow",
                                "observation_date": "observation_date_ow"})
                        rt = rt.rename(
                            columns={
                                "price_inc": "most_avail_fare_total_rt",
                                "price_exc": "most_avail_fare_base_rt",
                                "tax": "most_avail_fare_tax_rt",
                                "most_available_fare_count": "most_avail_fare_count_rt",
                                "fb": "fare_basis_rt",
                                "observation_date": "observation_date_rt"})

                        try:
                            most_avail_dict_ow = ow[(ow['most_avail_fare_count_ow'] == max_ow)].to_dict(
                                "records")[0]
                        except BaseException:
                            most_avail_dict_ow = {
                                "carrier": comp,
                                "most_avail_fare_base_ow": "NA",
                                "most_avail_fare_total_ow": "NA",
                                "most_avail_fare_count_ow": "NA",
                                "most_avail_fare_tax_ow": "NA",
                                "most_avail_fare_freq_ow": "NA",
                                "fare_basis_ow": "NA",
                                "observation_date_ow": "NA"}
                        try:
                            most_avail_dict_rt = rt[(rt['most_avail_fare_count_rt'] == max_rt)].to_dict(
                                "records")[0]
                        except BaseException:
                            most_avail_dict_rt = {
                                "carrier": comp,
                                "most_avail_fare_total_rt": "NA",
                                "most_avail_fare_base_rt": "NA",
                                "most_avail_fare_tax_rt": "NA",
                                "most_avail_fare_count_rt": "NA",
                                "most_avail_fare_freq_rt": "NA",
                                "fare_basis_rt": "NA",
                                "observation_date_rt": "NA"}
                        #			print most_avail_dict_ow
                        #			print most_avail_dict_rt
                        ### combining the ow and rt columns in one dict########
                        for k, v in most_avail_dict_rt.items():
                            if k not in most_avail_dict_ow.keys():
                                most_avail_dict_ow[k] = v
                        #			print most_avail_dict_ow
                        #			print most_avail_dict_rt
                        most_avail_fare_dF = pd.concat(
                            [most_avail_fare_dF, pd.DataFrame([most_avail_dict_ow])])
                # print most_avail_fare_dF.head()
                # most_avail_fare_dF['most_avail_fare_total_ow'] = most_avail_fare_dF.apply(lambda row: (row['most_avail_fare_total_ow']/exchange_rate[currency])*exchange_rate[row['currency']], axis=1)
                # most_avail_fare_dF['most_avail_fare_tax_ow'] = most_avail_fare_dF.apply(lambda row: (row['most_avail_fare_tax_ow']/exchange_rate[currency])*exchange_rate[row['currency']], axis=1)
                # most_avail_fare_dF['price_exc'] = most_avail_fare_dF.apply(lambda row: (row['price_exc']/exchange_rate[currency])*exchange_rate[row['currency']], axis=1)
                most_avail_fare_dF['currency'] = currency

            else:
                for comp in comps:
                    tmp_df = df_1[df_1['carrier'] == comp]
                    tmp_df = tmp_df.groupby(
                        by=[
                            'carrier',
                            'price_inc',
                            'is_one_way',
                            'tax',
                            'price_exc',
                            'dep_date',
                            'fb',
                            'observation_date'])['outbound_flight_no'].nunique()
                    tmp_df = tmp_df.reset_index()
                    tmp_df = tmp_df.groupby(by=['carrier',
                                                'price_exc',
                                                'price_inc',
                                                'tax',
                                                'is_one_way',
                                                'fb',
                                                'observation_date'],
                                            as_index=False)['outbound_flight_no'].sum()
                    tmp_df = tmp_df.rename(
                        columns={
                            "outbound_flight_no": "most_available_fare_count"})
                    max_ow = tmp_df[(tmp_df['is_one_way'] == 1)
                    ]['most_available_fare_count'].max()
                    max_rt = tmp_df[(tmp_df['is_one_way'] == 0)
                    ]['most_available_fare_count'].max()
                    tmp_df['date_start'] = dep_date_start
                    tmp_df['date_end'] = dep_date_end
                    ow = tmp_df[tmp_df['is_one_way'] == 1]
                    ow['most_avail_fare_freq_ow'] = ow['most_available_fare_count'] / \
                                                    ow['most_available_fare_count'].sum()
                    rt = tmp_df[tmp_df['is_one_way'] == 0]
                    rt['most_avail_fare_freq_rt'] = rt['most_available_fare_count'] / \
                                                    rt['most_available_fare_count'].sum()
                    ow = ow.rename(
                        columns={
                            "price_inc": "most_avail_fare_total_ow",
                            "price_exc": "most_avail_fare_base_ow",
                            "tax": "most_avail_fare_tax_ow",
                            "most_available_fare_count": "most_avail_fare_count_ow",
                            "fb": "fare_basis_ow",
                            "observation_date": "observation_date_ow"})
                    rt = rt.rename(
                        columns={
                            "price_inc": "most_avail_fare_total_rt",
                            "price_exc": "most_avail_fare_base_rt",
                            "tax": "most_avail_fare_tax_rt",
                            "most_available_fare_count": "most_avail_fare_count_rt",
                            "fb": "fare_basis_rt",
                            "observation_date": "observation_date_rt"})
                    most_avail_dict_ow = ow.loc[(
                            ow['most_avail_fare_count_ow'] == max_ow)].to_dict("records")[0]
                    most_avail_dict_rt = rt.loc[(
                            rt['most_avail_fare_count_rt'] == max_rt)].to_dict("records")[0]
                    most_avail_dict = most_avail_dict_ow.update(
                        most_avail_dict_rt)
                    most_avail_fare_dF = pd.concat(
                        [most_avail_fare_dF, pd.DataFrame(list(most_avail_dict))])
                    # most_avail_fare_dF['price_inc'] = most_avail_fare_dF.apply(
                    #     lambda row: (row['price_inc'] / exchange_rate[currency]) * exchange_rate[row['currency']],
                    #     axis=1)
                    # most_avail_fare_dF['tax'] = most_avail_fare_dF.apply(
                    #     lambda row: (row['tax'] / exchange_rate[currency]) * exchange_rate[row['currency']], axis=1)
                    # most_avail_fare_dF['price_exc'] = most_avail_fare_dF.apply(
                    #     lambda row: (row['price_exc'] / exchange_rate[currency]) * exchange_rate[row['currency']],
                    #     axis=1)
                    most_avail_fare_dF['currency'] = currency

        else:
            most_avail_fare_dF = pd.DataFrame(
                columns=[
                    'carrier',
                    'most_avail_fare_base_ow',
                    'most_avail_fare_total_ow',
                    'most_avail_fare_tax_ow',
                    'most_avail_fare_count_ow',
                    'most_avail_fare_base_rt',
                    'most_avail_fare_total_rt',
                    'most_avail_fare_tax_rt',
                    'most_avail_fare_count_rt',
                    'currency',
                    'fare_basis_ow',
                    'fare_basis_rt',
                    "observation_date_ow",
                    "observation_date_rt"
                ]
            )
            most_avail_fare_dF.set_value(0, 'currency', currency)
            most_avail_fare_dF.fillna("NA", inplace=True)
        # print most_avail_fare_dF
        # print time.time() - st
        # print "currency: ", currency

    # print most_avail_fare_dF
    st = time.time()
    return most_avail_fare_dF


@measure(JUPITER_LOGGER)
def convert_tax_currency(tax_value, host_curr, tax_currency):
    if tax_currency == -999:
        return 0
    else:
        tax_currency = tax_currency.upper()
        host_curr = host_curr.upper()
        tax_value = tax_value / exchange_rate[host_curr] * exchange_rate[tax_currency]
        return tax_value


@measure(JUPITER_LOGGER)
def convert_tax_percentage(percentage_block, currency, fare):
    fareblock = []
    taxblock = []
    farelogic = 0
    linkcode1 = ""
    fare1 = 0
    taxonfare = 0
    farelogic = 0

    if percentage_block != 0:
        for ele in percentage_block:
            if ele['apply_on'] == 'fare':
                fareblock.append(ele)
            else:
                taxblock.append(ele)

    farefromtaxblock = 0
    fareforomfareblock = 0

    for ele in taxblock:
        print ele
        for each in ele:
            print each
            if each == "link_code":
                print ele[each]
                linkcode1 = ele[each]
                print linkcode1
                for elem in fareblock:
                    if linkcode1 == elem['code']:
                        print "tantanatan"
                        Tfare = elem['value'] * fare / 100
                        farefromtaxblock = farefromtaxblock + ele['value'] * Tfare / 100
                        # fareblock.remove(elem)

    for ele in fareblock:
        print fareblock
        if not (ele['threshold_value']):
            fare1 = ele['value'] * fare / 100
            fareforomfareblock += fare1
        else:
            taxonfare = ele['value'] * fare / 100
            farelogic = min(convert_tax_currency(ele['threshold_value'], currency, ele['threshold_currency']),
                            convert_tax_currency(taxonfare, currency,
                                                 ele['threshold_currency']))
            fareforomfareblock += farelogic
    percenttax = farefromtaxblock + fareforomfareblock
    return percenttax


@measure(JUPITER_LOGGER)
def correct_inverted_recommendations(compartment, host_fares, rbd_cur):
    if compartment == 'Y':
        rbd_ladder = rbd_cur['RBDs_Y']
    elif compartment == 'J':
        rbd_ladder = rbd_cur['RBDs_J']
    else:
        rbd_ladder = rbd_cur['RBDs_F']
    map_ranks = pd.DataFrame()
    map_ranks['RBD'] = rbd_ladder
    map_ranks['rank'] = range(len(rbd_ladder), 0, -1)
    # host_fares['unique'] = range(len(host_fares))  # unique is the index of each fare as found in host_fares
    temp_df = host_fares[['RBD', 'recommended_fare', 'oneway_return', 'fare_basis', 'unique', 'fare', 'is_wrong']]
    fares_df_ow = temp_df[temp_df['oneway_return'] == '1'].merge(map_ranks, on='RBD', how='left')
    fares_df_rt = temp_df[temp_df['oneway_return'] == '2'].merge(map_ranks, on='RBD', how='left')
    fares_df_ow = fares_df_ow.sort_values(by=['recommended_fare'])
    fares_df_rt = fares_df_rt.sort_values(by=['recommended_fare'])
    fares_df_ow['prev_rank'] = fares_df_ow['rank'].shift()
    fares_df_rt['prev_rank'] = fares_df_rt['rank'].shift()
    fares_df_ow['diff_rank'] = fares_df_ow['rank'] - fares_df_ow['prev_rank']
    fares_df_rt['diff_rank'] = fares_df_rt['rank'] - fares_df_rt['prev_rank']
    if len(fares_df_ow) > 0:
        fares_df_ow.loc[fares_df_ow['diff_rank'] < 0, "is_wrong_reco"] = 1
    if len(fares_df_rt) > 0:
        fares_df_rt.loc[fares_df_rt['diff_rank'] < 0, "is_wrong_reco"] = 1
    temp_df = pd.concat([fares_df_ow, fares_df_rt])
    host_fares = host_fares.merge(temp_df[['unique', 'is_wrong_reco', 'rank']], on='unique', how='left')
    host_fares['is_wrong_reco'].fillna(0, inplace=True)
    ow_df = host_fares[host_fares['oneway_return'] == '1'].sort_values(by=['rank', 'recommended_fare'])
    ow_df = ow_df.reset_index(drop=True)
    rt_df = host_fares[host_fares['oneway_return'] == '2'].sort_values(by=['rank', 'recommended_fare'])
    rt_df = rt_df.reset_index(drop=True)
    for index, row in ow_df[1:].iterrows():
        if (row['is_wrong_reco'] == 1) and (row['is_wrong'] == 0) and (row['rank'] != ow_df['rank'][index - 1]) and (
                row['recommended_fare'] < ow_df['recommended_fare'][index - 1]):
            try:
                ow_df.loc[index, 'recommended_fare'] = ow_df['recommended_fare'][index - 1]
            except Exception as e:
                print traceback.print_exc()
    for index, row in rt_df[1:].iterrows():
        if (row['is_wrong_reco'] == 1) and (row['is_wrong'] == 0) and (row['rank'] != rt_df['rank'][index - 1]) and (
                row['recommended_fare'] < rt_df['recommended_fare'][index - 1]):
            try:
                rt_df.loc[index, 'recommended_fare'] = rt_df['recommended_fare'][index - 1]
            except Exception as e:
                print traceback.print_exc()
    host_fares = pd.concat([ow_df, rt_df])
    host_fares = host_fares.sort_values(by='fare')
    return host_fares


@measure(JUPITER_LOGGER)
def get_inverted_fares(host_fares, compartment, rbd_cur):
    if compartment == 'Y':
        rbd_ladder = rbd_cur['RBDs_Y']
    elif compartment == 'J':
        rbd_ladder = rbd_cur['RBDs_J']
    else:
        rbd_ladder = rbd_cur['RBDs_F']
    map_ranks = pd.DataFrame()
    map_ranks['RBD'] = rbd_ladder
    map_ranks['rank'] = range(len(rbd_ladder), 0, -1)
    # host_fares['unique'] = range(len(host_fares))  # unique is the index of each fare as found in host_fares
    temp_df = host_fares[['RBD', 'fare', 'oneway_return', 'fare_basis', 'unique']]
    fares_df_ow = temp_df[temp_df['oneway_return'] == '1'].sort_values(by='fare').merge(map_ranks, on='RBD', how='left')
    fares_df_rt = temp_df[temp_df['oneway_return'] == '2'].sort_values(by='fare').merge(map_ranks, on='RBD', how='left')
    fares_df_ow['prev_rank'] = fares_df_ow['rank'].shift()
    fares_df_rt['prev_rank'] = fares_df_rt['rank'].shift()
    fares_df_ow['diff_rank'] = fares_df_ow['rank'] - fares_df_ow['prev_rank']
    fares_df_rt['diff_rank'] = fares_df_rt['rank'] - fares_df_rt['prev_rank']
    fares_df_ow['is_wrong'] = 0
    fares_df_rt['is_wrong'] = 0
    if len(fares_df_ow) > 0:
        fares_df_ow.loc[fares_df_ow['diff_rank'] < 0, "is_wrong"] = 1
    if len(fares_df_rt) > 0:
        fares_df_rt.loc[fares_df_rt['diff_rank'] < 0, "is_wrong"] = 1
    temp_df = pd.concat([fares_df_ow, fares_df_rt])
    host_fares = host_fares.merge(temp_df[['unique', 'is_wrong']], on='unique', how='left')
    host_fares['is_wrong'].fillna(0, inplace=True)
    host_fares.loc[~host_fares['RBD'].isin(rbd_ladder), 'is_wrong'] = 2
    return host_fares


@measure(JUPITER_LOGGER)
def cap_twenty_perc(base_fare, recommended_fare):
    if base_fare * (1 + RECOMMENDATION_LOWER_THRESHOLD / 100.0) >= recommended_fare:
        return base_fare * (1 + RECOMMENDATION_LOWER_THRESHOLD / 100.0)
    elif base_fare * (1 + RECOMMENDATION_UPPER_THRESHOLD / 100.0) <= recommended_fare:
        return base_fare * (1 + RECOMMENDATION_UPPER_THRESHOLD / 100.0)
    else:
        return recommended_fare

def cat_15_cond(cat_15, pos, origin, destination, zone_list):

    if len(cat_15) > 0:

        """apply conditions"""
        print cat_15
        try:
            if cat_15['CTRY'] == 'O' and pos == origin:
                print "check POS = Origin"
                return 0

            elif cat_15['CTRY'] == 'D' and pos == destination:
                print "check POS = Destination"
                return 0
            elif cat_15['CTRY'] == 'B' and pos == origin and pos == destination:
                print "check POS=Origin and destination"
                return 0
            else:
                print "check application condition"
                local_apple = []
                local_type = []
                local_val = []
                region = {
                    'N':'COUNTRY',
                    'A':'AREA',
                    'Z':'ZONE',
                    'C':'CITY'
                }

                for key in cat_15.keys():
                    if re.search('LOCALE_APPL_(\d+)', key):
                        val = re.search('LOCALE_APPL_(\d+)', key).group(1)
                        local_apple.append(cat_15['LOCALE_APPL_'+str(val)])
                        local_type.append(cat_15['LOC_TYPE_'+str(val)])
                        local_val.append(cat_15['LOC_'+str(val)+'_'+region[cat_15['LOC_TYPE_'+str(val)]]+'_'+str(val)])

                local_df = pd.DataFrame({'LOCALE_APPL':local_apple, 'LOC_TYPE':local_type, 'LOC':local_val})

                local_df_n = local_df[local_df['LOCALE_APPL'] =='N']
                local_df_y = local_df[local_df['LOCALE_APPL'] =='Y']
                print zone_list
                region_list = zone_list[0].values()
                if len(local_df_y)>0:
                    #print "len_Y"
                    if len(local_df_y[local_df_y['LOC'].isin(region_list)]) >0:
                        #print "match"
                        return 0
                    else:
                        return -1
                elif len(local_df_n)>0:
                    #print "len_N"
                    if len(local_df_n[local_df_n['LOC'].isin(region_list)]) > 0:
                        #print "match"
                        return -1
                    else:
                        return 0
                else:
                    return 0
        except:
            return 0
    else:
        return 0


@measure(JUPITER_LOGGER)
def get_host_fares_df(pos, origin, destination, compartment, extreme_start_date, extreme_end_date, db, comp=[Host_Airline_Code],
                      channel=None):
    currency_data = get_currency_data(db=db)
    currency = []
    try:
        if currency_data[pos]['web']:
            currency.append(currency_data[pos]['web'])
        if currency_data[pos]['gds']:
            currency.append(currency_data[pos]['gds'])
    except KeyError:
        currency = []

    ### City-Airport Mapping logic to get fares filed for all airports
    ### under a city

    city_ap_crsr = db.JUP_DB_City_Airport_Mapping.find({"Airport_Code": {"$in": [origin, destination]}},
                                                       {"_id": 0, "City_Code": 1, "Airport_Code": 1})
    city_ap_crsr = list(city_ap_crsr)
    pseudo_origins = []
    pseudo_destinations = []
    ods = []
    tax_query = []
    city_ori = origin
    city_dest = destination
    for ap in city_ap_crsr:
        print ap
        if ap['Airport_Code'] == origin:
            city_ori = ap['City_Code']
        else:
            city_dest = ap['City_Code']
    city_ap_crsr2 = db.JUP_DB_City_Airport_Mapping.find({"City_Code": {"$in": [city_ori, city_dest]}},
                                                        {"_id": 0, "City_Code": 1, "Airport_Code": 1})

    city_ap_crsr2 = list(city_ap_crsr2)
    pseudo_origins.append(city_ori)
    pseudo_destinations.append(city_dest)
    for ap in city_ap_crsr2:
        if ap['City_Code'] == city_ori:
            pseudo_origins.append(ap['Airport_Code'])
        else:
            pseudo_destinations.append(ap['Airport_Code'])
    OD_list = []
    for o in pseudo_origins:
        for d in pseudo_destinations:
            temp_od = d + o
            temp_od_1 = o + d
            temp_od = temp_od.replace("UAE", "DXB")
            temp_od_1 = temp_od_1.replace("UAE", "DXB")
            ods.append(temp_od)
            ods.append(temp_od_1)
            OD_list.append(temp_od_1)
            tax_query.append({
                "$and": [{
                    "Origin": temp_od[0:3],
                    "Destination": temp_od[3:]
                }]
            })
            tax_query.append({
                "$and": [{
                    "Origin": temp_od_1[0:3],
                    "Destination": temp_od_1[3:]
                }]
            })

    cols = [
        "effective_from",
        "effective_to",
        "dep_date_from",
        "dep_date_to",
        "sale_date_from",
        "sale_date_to",
        "fare_basis",
        "fare_brand",
        "RBD",
        "rtg",
        "tariff_code",
        "fare_include",
        "private_fare",
        "footnote",
        "batch",
        "origin",
        "destination",
        "OD",
        "compartment",
        "oneway_return",
        "channel",
        "carrier",
        "fare",
        "surcharge_date_start_1",
        "surcharge_date_start_2",
        "surcharge_date_start_3",
        "surcharge_date_start_4",
        "surcharge_date_end_1",
        "surcharge_date_end_2",
        "surcharge_date_end_3",
        "surcharge_date_end_4",
        "surcharge_amount_1",
        "surcharge_amount_2",
        "surcharge_amount_3",
        "surcharge_amount_4",
        "Average_surcharge",
        "total_fare_1",
        "total_fare_2",
        "total_fare_3",
        "total_fare_4",
        "total_fare",
        "YR",
        "YQ",
        "taxes",
        "currency",
        "fare_rule",
        "RBD_type",
        "gfs",
        "last_update_date",
        "last_update_time",
        "competitor_farebasis",
        "travel_date_to",
        "travel_date_from",
        "retention_fare",
        "flight_number",
        "day_of_week",
        'tax_currency',
        'Fixed_tax',
        'percentage_block',
        'Fixed_tax_breakup',
        'Percentage_tax_breakup'
    ]
    print "ods: ", ods
    dF = pd.DataFrame(columns=cols)
    SYSTEM_DATE_LW = datetime.datetime.strftime(today - datetime.timedelta(days=7), '%Y-%m-%d')
    extreme_start_date_n = "0" + extreme_start_date[2:4] + extreme_start_date[5:7] + extreme_start_date[8:10]
    extreme_end_date_n = "0" + extreme_end_date[2:4] + extreme_end_date[5:7] + extreme_end_date[8:10]
    extreme_start_date_obj = datetime.datetime.strptime(extreme_start_date, "%Y-%m-%d")
    extreme_start_date_lw = extreme_start_date_obj - datetime.timedelta(days=7)
    extreme_start_date_lw = extreme_start_date_lw.strftime("%Y-%m-%d")
    extreme_start_date_n_lw = "0" + extreme_start_date_lw[2:4] + extreme_start_date_lw[5:7] + extreme_start_date_lw[
                                                                                              8:10]
    SYSTEM_DATE_MOD = "0" + SYSTEM_DATE[2:4] + SYSTEM_DATE[5:7] + SYSTEM_DATE[8:10]
    SYSTEM_DATE_MOD_LW = "0" + SYSTEM_DATE_LW[2:4] + SYSTEM_DATE_LW[5:7] + SYSTEM_DATE_LW[8:10]
    if channel:
        channels = {"$eq": channel}
    else:
        channels = {"$ne": channel}
    print "Aggregating on ATPCO Fares Rules . . . . "
    query = {
        "$and": [
            {
                "carrier": {"$in": comp}
            },
            {
                "compartment": compartment
            },
            {
                "channel": channels
            },
            {
                "fare_include": True
            },
            {
                "currency": {
                    "$in": currency
                }
            },
            {
                "OD": {
                    "$in": ods
                }
            },
            {
                "$or": [
                    {
                        "effective_from": None
                    },
                    {
                        "effective_from": {
                            "$lte": SYSTEM_DATE
                        }
                    }
                ]
            },
            {
                "$or": [
                    {
                        "effective_to": None
                    },
                    {
                        "effective_to": {
                            "$gte": SYSTEM_DATE_LW  # to include expired fares as well
                        }
                    }
                ]
            },
            # {
            #     "LFF_flag": True
            # },
            {
                "$or": [
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": {}
                            },
                            {
                                "cat_14": {}
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": None
                            },
                            {
                                "cat_14": None
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_COMM": {
                                    "$eq": "0999999"
                                }
                            },
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_EXP": {
                                    "$gte": extreme_start_date_n_lw  # to show expired fares
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_COMM": {
                                    "$lte": extreme_end_date_n
                                }
                            },
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_EXP": {
                                    "$eq": "0000000"
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_COMM": "0999999"
                            },
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_EXP": None
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_COMM": None
                            },
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_EXP": "0000000"
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": {}
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": {
                                    "$lte": extreme_end_date_n
                                }
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": {
                                    "$gte": extreme_start_date_n_lw  # to include expired fares
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": {}
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": {
                                    "$lte": extreme_end_date_n
                                }
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": {
                                    "$eq": "0000000"
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": {}
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": "0999999"
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": {
                                    "$gte": extreme_start_date_n_lw  # to include expired fares
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": {}
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": "0999999"
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": "0000000"
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": {}
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": {
                                    "$lte": extreme_end_date_n
                                }
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": None
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": {}
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": "0999999"
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": None
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": {}
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": None
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": {
                                    "$gte": extreme_start_date_n_lw
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": {}
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": None
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": "0000000"
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": {}
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": None
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": None
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": None
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": {
                                    "$lte": extreme_end_date_n
                                }
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": {
                                    "$gte": extreme_start_date_n_lw
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": None
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": {
                                    "$lte": extreme_end_date_n
                                }
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": {
                                    "$eq": "0000000"
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": None
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": "0999999"
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": {
                                    "$gte": extreme_start_date_n_lw
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": None
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": "0999999"
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": "0000000"
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": None
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": {
                                    "$lte": extreme_end_date_n
                                }
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": None
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": None
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": "0999999"
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": None
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": None
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": None
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": {
                                    "$gte": extreme_start_date_n_lw
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": None
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": None
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": "0000000"
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN": None
                            },
                            {
                                "cat_14.TRAVEL_DATES_COMM": None
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": None
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_COMM": {
                                    "$lte": extreme_end_date_n
                                }
                            },
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_EXP": {
                                    "$gte": extreme_start_date_n_lw
                                }
                            }
                        ]
                    },
                    # {
                    #     "$and": [
                    #         {
                    #             "Footnotes": {}
                    #         }
                    #     ]
                    # },
                    # {
                    #     "$and": [
                    #         {
                    #             "Footnotes": None
                    #         }
                    #     ]
                    # },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_COMM": {
                                    "$lte": extreme_end_date_n
                                }
                            },
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_EXP": None
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_COMM": None
                            },
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_EXP": {
                                    "$gte": extreme_start_date_n_lw
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_COMM": "0000000"
                            },
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_EXP": "0999999"
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_COMM": "0999999"
                            },
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_EXP": "0000000"
                            }
                        ]
                    },
                    # {
                    #     "$and": [
                    #         {
                    #             "Footnotes.Cat_14_FN.TRAVEL_DATES_COMM": None
                    #         },
                    #         {
                    #             "Footnotes.Cat_14_FN.TRAVEL_DATES_EXP": None
                    #         }
                    #     ]
                    # }
                ]
            },
            {
                "$or": [
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_15_FN": {}
                            },
                            {
                                "cat_15": {}
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_15_FN": None
                            },
                            {
                                "cat_15": None
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_15_FN.SALE_DATES_EARLIEST_TKTG": {
                                    "$lte": SYSTEM_DATE_MOD
                                }
                            },
                            {
                                "Footnotes.Cat_15_FN.SALE_DATES_LATEST_TKTG": {
                                    "$gte": SYSTEM_DATE_MOD_LW
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_15_FN.SALE_DATES_EARLIEST_TKTG": {
                                    "$eq": "0999999"
                                }
                            },
                            {
                                "Footnotes.Cat_15_FN.SALE_DATES_LATEST_TKTG": {
                                    "$gte": SYSTEM_DATE_MOD_LW
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_15_FN.SALE_DATES_EARLIEST_TKTG": {
                                    "$lte": SYSTEM_DATE_MOD
                                }
                            },
                            {
                                "Footnotes.Cat_15_FN.SALE_DATES_LATEST_TKTG": {
                                    "$eq": "0000000"
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_15_FN.SALE_DATES_EARLIEST_TKTG": "0999999"
                            },
                            {
                                "Footnotes.Cat_15_FN.SALE_DATES_LATEST_TKTG": "0000000"
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_15_FN.SALE_DATES_EARLIEST_TKTG": {
                                    "$lte": SYSTEM_DATE_MOD
                                }
                            },
                            {
                                "Footnotes.Cat_15_FN.SALE_DATES_LATEST_TKTG": None
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_15_FN.SALE_DATES_EARLIEST_TKTG": "0999999"
                            },
                            {
                                "Footnotes.Cat_15_FN.SALE_DATES_LATEST_TKTG": None
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_15_FN.SALE_DATES_EARLIEST_TKTG": None
                            },
                            {
                                "Footnotes.Cat_15_FN.SALE_DATES_LATEST_TKTG": {
                                    "$gte": SYSTEM_DATE_MOD_LW
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_15_FN.SALE_DATES_EARLIEST_TKTG": None
                            },
                            {
                                "Footnotes.Cat_15_FN.SALE_DATES_LATEST_TKTG": "0000000"
                            }
                        ]
                    },
                    # {
                    #     "$and": [
                    #         {
                    #             "Footnotes.Cat_15_FN.SALE_DATES_EARLIEST_TKTG": None
                    #         },
                    #         {
                    #             "Footnotes.Cat_15_FN.SALE_DATES_LATEST_TKTG": None
                    #         }
                    #     ]
                    # },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_15_FN": {}
                            },
                            {
                                "cat_15.SALE_DATES_EARLIEST_TKTG": {
                                    "$lte": SYSTEM_DATE_MOD
                                }
                            },
                            {
                                "cat_15.SALE_DATES_LATEST_TKTG": {
                                    "$gte": SYSTEM_DATE_MOD_LW
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_15_FN": {}
                            },
                            {
                                "cat_15.SALE_DATES_EARLIEST_TKTG": {
                                    "$lte": SYSTEM_DATE_MOD
                                }
                            },
                            {
                                "cat_15.SALE_DATES_LATEST_TKTG": {
                                    "$eq": "0000000"
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_15_FN": {}
                            },
                            {
                                "cat_15.SALE_DATES_EARLIEST_TKTG": "0999999"
                            },
                            {
                                "cat_15.SALE_DATES_LATEST_TKTG": {
                                    "$gte": SYSTEM_DATE_MOD_LW
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_15_FN": {}
                            },
                            {
                                "cat_15.SALE_DATES_EARLIEST_TKTG": "0999999"
                            },
                            {
                                "cat_15.SALE_DATES_LATEST_TKTG": "0000000"
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_15_FN": {}
                            },
                            {
                                "cat_15.SALE_DATES_EARLIEST_TKTG": {
                                    "$lte": SYSTEM_DATE_MOD
                                }
                            },
                            {
                                "cat_15.SALE_DATES_LATEST_TKTG": None
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_15_FN": {}
                            },
                            {
                                "cat_15.SALE_DATES_EARLIEST_TKTG": "0999999"
                            },
                            {
                                "cat_15.SALE_DATES_LATEST_TKTG": None
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_15_FN": {}
                            },
                            {
                                "cat_15.SALE_DATES_EARLIEST_TKTG": None
                            },
                            {
                                "cat_15.SALE_DATES_LATEST_TKTG": {
                                    "$gte": SYSTEM_DATE_MOD_LW
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_15_FN": {}
                            },
                            {
                                "cat_15.SALE_DATES_EARLIEST_TKTG": None
                            },
                            {
                                "cat_15.SALE_DATES_LATEST_TKTG": "0000000"
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_15_FN": {}
                            },
                            {
                                "cat_15.SALE_DATES_EARLIEST_TKTG": None
                            },
                            {
                                "cat_15.SALE_DATES_LATEST_TKTG": None
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_15_FN": None
                            },
                            {
                                "cat_15.SALE_DATES_EARLIEST_TKTG": {
                                    "$lte": SYSTEM_DATE_MOD
                                }
                            },
                            {
                                "cat_15.SALE_DATES_LATEST_TKTG": {
                                    "$gte": SYSTEM_DATE_MOD_LW
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_15_FN": None
                            },
                            {
                                "cat_15.SALE_DATES_EARLIEST_TKTG": {
                                    "$lte": SYSTEM_DATE_MOD
                                }
                            },
                            {
                                "cat_15.SALE_DATES_LATEST_TKTG": {
                                    "$eq": "0000000"
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_15_FN": None
                            },
                            {
                                "cat_15.SALE_DATES_EARLIEST_TKTG": "0999999"
                            },
                            {
                                "cat_15.SALE_DATES_LATEST_TKTG": {
                                    "$gte": SYSTEM_DATE_MOD_LW
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_15_FN": None
                            },
                            {
                                "cat_15.SALE_DATES_EARLIEST_TKTG": "0999999"
                            },
                            {
                                "cat_15.SALE_DATES_LATEST_TKTG": "0000000"
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_15_FN": None
                            },
                            {
                                "cat_15.SALE_DATES_EARLIEST_TKTG": {
                                    "$lte": SYSTEM_DATE_MOD
                                }
                            },
                            {
                                "cat_15.SALE_DATES_LATEST_TKTG": None
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_15_FN": None
                            },
                            {
                                "cat_15.SALE_DATES_EARLIEST_TKTG": "0999999"
                            },
                            {
                                "cat_15.SALE_DATES_LATEST_TKTG": None
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_15_FN": None
                            },
                            {
                                "cat_15.SALE_DATES_EARLIEST_TKTG": None
                            },
                            {
                                "cat_15.SALE_DATES_LATEST_TKTG": {
                                    "$gte": SYSTEM_DATE_MOD_LW
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_15_FN": None
                            },
                            {
                                "cat_15.SALE_DATES_EARLIEST_TKTG": None
                            },
                            {
                                "cat_15.SALE_DATES_LATEST_TKTG": "0000000"
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_15_FN": None
                            },
                            {
                                "cat_15.SALE_DATES_EARLIEST_TKTG": None
                            },
                            {
                                "cat_15.SALE_DATES_LATEST_TKTG": None
                            }
                        ]
                    }
                ]
            }
        ]
    }
    # print json.dumps(query)
    fares_data = list(db.JUP_DB_ATPCO_Fares_Rules.aggregate(
        [
            {
                '$match': query
            }
        ]
    ))

    #print list(fares_data)

    print "Fares Data length = ", len(fares_data)


    # exchange_rate = {}
    # pos_country = list(db.JUP_DB_Region_Master.find({"POS_CD": pos}))[0]['COUNTRY_CD']
    # yr_crsr = list(db.JUP_DB_YR_Master.find({"POS-LocCode": pos_country, "Compartment": compartment}))
    # if len(yr_crsr) == 0:
    #     yr_crsr = list(db.JUP_DB_YR_Master.find({"POS-LocCode": "MISC"}))
    # yr_curr = yr_crsr[0]['Curr']
    # yr = {}
    tax_crsr = list(db.JUP_DB_Tax_Master.find({
        'Compartment': compartment,
        '$or': tax_query
    }))
    # tax_crsr = list(db.JUP_DB_Tax_Master.find({
    #     'Compartment': compartment,
    #     '$or': [{'Origin': origin,
    #              'Destination': destination},
    #             {'Origin': destination,
    #              'Destination': origin}
    #             ]
    # }))


    if len(tax_crsr) != 0:
        tax_df = pd.DataFrame(tax_crsr)
        if 'percentage_block' not in tax_df.columns:
            tax_df['percentage_block'] = None

    else:
        tax_df = pd.DataFrame(columns=['Origin',
                                       'Destination',
                                       'Currency',
                                       'OW_RT',
                                       'Compartment',
                                       'Fixed_tax_breakup',
                                       'Percentage_tax_breakup',
                                       'Fixed_tax',
                                       'percentage_block'])
    tax_df = tax_df[['Origin',
                     'Destination',
                     'Currency',
                     'OW_RT',
                     'Compartment',
                     'Fixed_tax_breakup',
                     'Percentage_tax_breakup',
                     'Fixed_tax',
                     'percentage_block']]
    tax_df['Fixed_tax'] = tax_df['Fixed_tax'].fillna(0)
    tax_df['percentage_block'] = tax_df['percentage_block'].fillna(0)
    tax_currencies = list(tax_df['Currency'].unique())
    tax_df['OD'] = tax_df['Origin'] + tax_df['Destination']
    tax_df = tax_df.rename(
        columns={
            'Currency': 'tax_currency',
            'OW_RT': 'oneway_return',
            'Compartment':'compartment'})
    tax_df.drop(['Origin', 'Destination'], axis=1, inplace=True)
    tax_df['oneway_return'] = tax_df['oneway_return'].apply(str)
    # Implementing YQ from YQ master collection
    # yq_data = list(db.JUP_DB_YQ_Master.find({'compartment': compartment}))
    # yq_df = pd.DataFrame(yq_data)
    # yq_df.drop(['_id', 'compartment'], axis=1, inplace=True)
    # yq_df = yq_df.rename(
    #     columns={
    #         'amount': 'amount_yq',
    #         'currency': 'currency_yq'})
    # yq_currencies = list(yq_df['currency_yq'].unique())
    # yq_df['oneway_return'] = yq_df['oneway_return'].apply(str)
    # currency_crsr = list(db.JUP_DB_Exchange_Rate.find(
    #     {"code": {"$in": currency + tax_currencies}}))
    # for curr in currency_crsr:
    #     exchange_rate[curr['code']] = curr['Reference_Rate']
    # for currency_ in currency:
    #     if yr_curr != currency_:
    #         yr_val = (yr_crsr[0]['Amount'] /
    #                   exchange_rate[yr_curr]) * exchange_rate[currency_]
    #         yr[currency_] = yr_val
    #     else:
    #         yr[currency_] = yr_crsr[0]['Amount']

    if len(fares_data) > 0:
        dF = pd.DataFrame(fares_data)
        dF.fillna(-999, inplace=True)
        dF['surcharge_amount_1'] = 0
        dF['surcharge_amount_2'] = 0
        dF['surcharge_amount_3'] = 0
        dF['surcharge_amount_4'] = 0

        dF['surcharge_date_start_1'] = "NA"
        dF['surcharge_date_start_2'] = "NA"
        dF['surcharge_date_start_3'] = "NA"
        dF['surcharge_date_start_4'] = "NA"

        dF['surcharge_date_end_1'] = "NA"
        dF['surcharge_date_end_2'] = "NA"
        dF['surcharge_date_end_3'] = "NA"
        dF['surcharge_date_end_4'] = "NA"
        dF['flight_number'] = "--"
        dF['day_of_week'] = "--"
        dF = dF.merge(tax_df, on=['OD', 'oneway_return', 'compartment'], how='left')
        dF[['Fixed_tax']] = dF[['Fixed_tax']].fillna(0)
        dF[['Fixed_tax_breakup', 'Percentage_tax_breakup']] = dF[['Fixed_tax_breakup', 'Percentage_tax_breakup']].fillna("")
        dF[['tax_currency']] = dF[['tax_currency']].fillna(-999)

        dF['Fixed_tax'] = dF.apply(lambda row: convert_tax_currency(row['Fixed_tax'],
                                                                    row['currency'],
                                                                    row['tax_currency']), axis=1)
        dF['Fixed_tax'] = dF['Fixed_tax'].fillna(0)
        dF['percentage_block'] = dF['percentage_block'].fillna(0)
        dF['percentage_block'] = dF.apply(lambda row: convert_tax_percentage(row['percentage_block'],
                                                                             row['currency'], row['fare']), axis=1)
        dF['taxes'] = dF['Fixed_tax'] + dF['percentage_block']

        # Merge YQ df and fares df(that has already been merged with tax df) to
        # compute yq
        # dF = dF.merge(yq_df, on=['oneway_return'], how='left')
        # dF['YQ'] = dF.apply(lambda row: row['amount_yq'] /
        #                     exchange_rate[row['currency']] *
        #                     exchange_rate[row['currency_yq']], axis=1)
        #print dF.to_string()
        #print dF[['fare_basis', 'cat_2', 'fare']].to_string()
        zone_crsr = db.JUP_DB_ATPCO_Zone_Master.find({'CITY_CODE': pos}, {'_id': 0, 'FILLER': 0,
                                                                          'CITY_STATE': 0, 'REC_TYPE': 0})
        zone_list = list(zone_crsr)

        for i in range(len(dF)):
            has_category = False
            has_category_15 = False
            tvl_dates = []
            sale_dates = []
            surchrg_amts = []
            try:
                dates_array = dF['Footnotes'][i]['Cat_14_FN']
                has_category = True
            except KeyError:
                try:
                    dates_array = dF['cat_14'][i]
                    if dates_array != -999:
                        has_category = True
                    else:
                        dF.loc[i, 'travel_date_from'] = "1900-01-01"
                        dF.loc[i, 'travel_date_to'] = "2099-12-31"

                except KeyError:
                    dF.loc[i, 'travel_date_from'] = "1900-01-01"
                    dF.loc[i, 'travel_date_to'] = "2099-12-31"
            except TypeError:
                try:
                    dates_array = dF['cat_14'][i]
                    if dates_array != -999:
                        has_category = True
                    else:
                        dF.loc[i, 'travel_date_from'] = "1900-01-01"
                        dF.loc[i, 'travel_date_to'] = "2099-12-31"
                except KeyError:
                    dF.loc[i, 'travel_date_from'] = "1900-01-01"
                    dF.loc[i, 'travel_date_to'] = "2099-12-31"
            if has_category:
                for entry in dates_array:
                    try:
                        tvl_dates.append(
                            str(entry['TRAVEL_DATES_COMM']) + str(entry['TRAVEL_DATES_EXP']))
                    except:
                        pass
                min_indx = np.argmin(tvl_dates)
                from_date = str(dates_array[min_indx]['TRAVEL_DATES_COMM'])
                to_date = str(dates_array[min_indx]['TRAVEL_DATES_EXP'])
                if len(from_date) == 7:
                    from_date = from_date[1:]
                if len(to_date) == 7:
                    to_date = to_date[1:]
                try:
                    from_date = datetime.datetime.strftime(
                        datetime.datetime.strptime(
                            from_date, "%y%m%d"), "%Y-%m-%d")
                except ValueError:
                    from_date = "1900-01-01"
                try:
                    to_date = datetime.datetime.strftime(
                        datetime.datetime.strptime(
                            to_date, "%y%m%d"), "%Y-%m-%d")
                except ValueError:
                    to_date = "2099-12-31"
                dF.loc[i, 'travel_date_from'] = from_date
                dF.loc[i, 'travel_date_to'] = to_date
            # except KeyError:
            #     dF.loc[i, 'travel_date_from'] = "1900-01-01"
            #     dF.loc[i, 'travel_date_to'] = "2099-12-31"

            # except TypeError:
            #     dF.loc[i, 'travel_date_from'] = "1900-01-01"
            #     dF.loc[i, 'travel_date_to'] = "2099-12-31"
            try:
                dates_array_15 = dF['Footnotes'][i]['Cat_15_FN']
                has_category_15 = True
            except KeyError:
                try:
                    dates_array_15 = dF['cat_15'][i]
                    if dates_array_15 != -999:
                        has_category_15 = True
                    else:
                        dF.loc[i, 'sale_date_from'] = "1900-01-01"
                        dF.loc[i, 'sale_date_to'] = "2099-12-31"
                except KeyError:
                    dF.loc[i, 'sale_date_from'] = "1900-01-01"
                    dF.loc[i, 'sale_date_to'] = "2099-12-31"
            except TypeError:
                try:
                    dates_array_15 = dF['cat_15'][i]
                    if dates_array_15 != -999:
                        has_category_15 = True
                    else:
                        dF.loc[i, 'sale_date_from'] = "1900-01-01"
                        dF.loc[i, 'sale_date_to'] = "2099-12-31"
                except KeyError:
                    dF.loc[i, 'sale_date_from'] = "1900-01-01"
                    dF.loc[i, 'sale_date_to'] = "2099-12-31"
            if has_category_15:
                for entry in dates_array_15:
                    try:
                        sale_dates.append(
                            str(entry['SALE_DATES_EARLIEST_TKTG']) + str(entry['SALE_DATES_LATEST_TKTG']))
                    except:
                        pass
                min_indx = np.argmin(sale_dates)
                from_date = str(
                    dates_array_15[min_indx]['SALE_DATES_EARLIEST_TKTG'])
                to_date = str(
                    dates_array_15[min_indx]['SALE_DATES_LATEST_TKTG'])
                if len(from_date) == 7:
                    from_date = from_date[1:]
                if len(to_date) == 7:
                    to_date = to_date[1:]
                try:
                    from_date = datetime.datetime.strftime(
                        datetime.datetime.strptime(
                            from_date, "%y%m%d"), "%Y-%m-%d")
                except ValueError:
                    from_date = "1900-01-01"
                try:
                    to_date = datetime.datetime.strftime(
                        datetime.datetime.strptime(
                            to_date, "%y%m%d"), "%Y-%m-%d")
                except ValueError:
                    to_date = "2099-12-31"
                dF.loc[i, 'sale_date_from'] = from_date
                dF.loc[i, 'sale_date_to'] = to_date
            # except KeyError:
            #     dF.loc[i, 'sale_date_from'] = "1900-01-01"
            #     dF.loc[i, 'sale_date_to'] = "2099-12-31"
            # except TypeError:
            #     dF.loc[i, 'sale_date_from'] = "1900-01-01"
            #     dF.loc[i, 'sale_date_to'] = "2099-12-31"
            """Not reading cat_12 from the gen rules."""
            # try:
            #     t = dF['Gen Rules'][i]['cat_12'][0]
            #     cat_12_gen = dF['Gen Rules'][i]['cat_12']
            # except:
            #     cat_12_gen = []
            cat_12_gen = []

            """Changed the concept of reading the cat_12"""
            # try:
            #     t = dF['cat_12'][i][0]
            #     cat_12 = dF['cat_12'][i]
            # except:
            #     cat_12 = []
            cat_12 = []
            cat_15 = []
            try:
                for q in range(len(dF['cat_12'][i])):
                    try:
                        if dF['cat_12'][i][q]['CAT_NO'] == '015':
                            cat_15.append(dF['cat_12'][i][q])
                        else:
                            cat_12.append(dF['cat_12'][i][q])
                    except:
                        pass
            except:
                cat_12 = []

            if len(cat_12) > 0 and len(cat_15) > 0:
                #print cat_15
                binary_value = cat_15_cond(cat_15=cat_15[0], pos=pos, origin=origin,
                                               destination=destination, zone_list=zone_list)

                if binary_value == -1:
                    cat_12 = []



            try:
                cat_12_list = cat_12 + cat_12_gen
                for entry in cat_12_list:
                    key = str(entry['DATE_RANGE_START']) + \
                          str(entry['DATE_RANGE_STOP'])
                    if key == "":
                        key = "000101991231"
                    surchrg_amts.append({
                        "key": key,
                        "surcharge_pct": float(entry['CHARES_PERCENT'][0:3] + "." + entry['CHARES_PERCENT'][3:]),
                        "surcharge_amt1": int(entry['CHARGES_AMT']),
                        "surcharge_amt2": int(entry['CHARGES_AMT2']),
                        "currency": (entry['CHARGES_1ST_CUR'] + entry['CHARGES_2ND_CUR'])[0:3],
                        "start_date": entry['DATE_RANGE_START'],
                        "end_date": entry['DATE_RANGE_STOP'],
                        "dec1": entry['CHARGES_DEC'],
                        "dec2": entry['CHARGES_DEC2']})
                temp_df = pd.DataFrame(surchrg_amts)
                temp_df.sort_values(by='key', inplace=True)
                temp_df['dec2'].replace(to_replace="", value="0", inplace=True)
                temp_df['dec1'].replace(to_replace="", value="0", inplace=True)
                temp_df['dec2'] = temp_df['dec2'].astype('int')
                temp_df['dec1'] = temp_df['dec1'].astype('int')

                temp_df.loc[temp_df['currency'] != "", 'surcharge_amt1'] = temp_df[
                    temp_df['currency'] != ""].apply(
                    lambda row: convert_tax_currency(row['surcharge_amt1'],
                                                     dF.loc[i, 'currency'],
                                                     row['currency']), axis=1)

                temp_df.loc[temp_df['currency'] != "", 'surcharge_amt2'] = temp_df.loc[
                    temp_df['currency'] != "", :].apply(
                    lambda row: convert_tax_currency(row['surcharge_amt2'],
                                                     dF.loc[i, 'currency'],
                                                     row['currency']), axis=1)
                total_surcharge = 0
                surcharge_count = 0
                for j in range(len(temp_df)):
                    try:
                        from_date = datetime.datetime.strftime(datetime.datetime.strptime(
                            str(temp_df.loc[j, 'start_date']), "%y%m%d"), "%Y-%m-%d")
                    except ValueError:
                        from_date = "1900-01-01"
                    try:
                        to_date = datetime.datetime.strftime(datetime.datetime.strptime(
                            str(temp_df.loc[j, 'end_date']), "%y%m%d"), "%Y-%m-%d")
                    except ValueError:
                        to_date = "2099-12-31"
                    dF.loc[i, 'surcharge_amount_' + str(j + 1)] = temp_df.loc[j, 'surcharge_pct'] * 0.01 * dF.loc[
                        i, 'fare'] \
                                                                  + temp_df.loc[j, 'surcharge_amt1'] / 10 ** \
                                                                  temp_df.loc[j, 'dec1'] \
                                                                  + temp_df.loc[j, 'surcharge_amt2'] / 10 ** \
                                                                  temp_df.loc[j, 'dec2']

                    dF.loc[i, 'surcharge_date_start_' + str(j + 1)] = from_date
                    dF.loc[i, 'surcharge_date_end_' + str(j + 1)] = to_date
                    surcharge_count += 1
                    total_surcharge += dF.loc[i, 'surcharge_amount_' + str(j + 1)]

                #dF.loc[i, 'Average_surcharge'] = total_surcharge/surcharge_count
                dF.loc[i, 'Average_surcharge'] = 0
            except KeyError:
                for j in range(4):
                    dF.loc[i, 'surcharge_amount_' + str(j + 1)] = 0
                    dF.loc[i, 'surcharge_date_start_' +
                           str(j + 1)] = "1900-01-01"
                    dF.loc[i, 'surcharge_date_end_' +
                           str(j + 1)] = "2099-12-31"
                dF.loc[i, 'Average_surcharge'] = 0
            except TypeError:
                for j in range(4):
                    dF.loc[i, 'surcharge_amount_' + str(j + 1)] = 0
                    dF.loc[i, 'surcharge_date_start_' +
                           str(j + 1)] = "1900-01-01"
                    dF.loc[i, 'surcharge_date_end_' +
                           str(j + 1)] = "2099-12-31"
                dF.loc[i, 'Average_surcharge'] = 0

            try:
                cat_4 = dF.loc[i, 'cat_4']
                temp = ""
                for element in cat_4:
                    try:
                        temp_st = element['CXR_FLT']['SEGS_CARRIER']
                        if len(temp_st) % 14 != 0:
                            temp_st = temp_st + "    "
                        for m in range(0, len(temp_st), 14):
                            seg = temp_st[m:m + 14]
                            op_cxr = seg[0:2]
                            mrkt_cxr = seg[3:5]
                            range_start = seg[6:10]
                            range_end = seg[10:]
                            if op_cxr == "  ":
                                op_cxr = "--"
                            if mrkt_cxr == "  ":
                                mrkt_cxr = "--"
                            if ((range_start == "****") or (range_end == "****")) and (
                                    (op_cxr == Host_Airline_Code) or (mrkt_cxr == Host_Airline_Code)):
                                temp = temp + op_cxr + " " + mrkt_cxr + " " + "****, "
                            elif (op_cxr == Host_Airline_Code) or (mrkt_cxr == Host_Airline_Code):
                                temp = temp + op_cxr + " " + mrkt_cxr + " " + range_start + "-" + range_end + ", "
                    except KeyError:
                        pass

                    for k in range(3):
                        key_name_cxr = "CXR" + str(k + 1)
                        key_name_flt = "FLT_NO" + str(k + 1)
                        if element[key_name_cxr] == Host_Airline_Code:
                            temp = temp + element[key_name_cxr] + " " + str(element[key_name_flt]) + ", "

                dF.loc[i, 'flight_number'] = temp[:-2]
            except KeyError:
                dF.loc[i, 'flight_number'] = "--"
            except TypeError:
                dF.loc[i, 'flight_number'] = "--"

            try:
                cat_2 = dF.loc[i, 'cat_2']
                unique_list = []
                temp = ""
                for element in cat_2:
                    for wkday in element['DAYOFWEEK']:
                        if wkday not in unique_list:
                            unique_list.append(wkday)
                            temp = temp + wkday + ", "
                dF.loc[i, 'day_of_week'] = temp[:-2]
            except KeyError:
                dF.loc[i, 'day_of_week'] = "--"
            except TypeError:
                dF.loc[i, 'day_of_week'] = "--"

        dF.replace(0, np.NaN, inplace=True)

        dF.replace(np.NaN, 0, inplace=True)

        dF['dep_date_from'] = extreme_start_date
        dF['dep_date_to'] = extreme_end_date
        # dF['YR'] = 0
        # for i in currency:
        #     dF.loc[dF['currency'] == i, 'YR'] = yr[i]
        # dF['YR'] = dF['YR'] * dF['oneway_return'].astype("int")
        dF.replace(-999, np.nan, inplace=True)
        dF['fare'].fillna(0, inplace=True)
        if 'YQ' in list(dF.columns):
            dF['YQ'].fillna(0, inplace=True)
        else:
            dF['YQ'] = 0
        # if "DXB" not in origin+destination:
        #     dF['YQ'] = dF['YQ'] * 2
        if 'YR' in list(dF.columns):
            dF['YR'].fillna(0, inplace=True)
        else:
            dF['YR'] = 0
        # dF.rename(columns={'YQ_TEST': 'YQ', 'YR_TEST': 'YR'}, inplace=True)
        dF['taxes'].fillna(0, inplace=True)
        dF['surcharge_amount_1'].fillna(0, inplace=True)
        dF['surcharge_amount_2'].fillna(0, inplace=True)
        dF['surcharge_amount_3'].fillna(0, inplace=True)
        dF['surcharge_amount_4'].fillna(0, inplace=True)
        dF['Average_surcharge'].fillna(0, inplace=True)
        dF['total_fare_1'] = dF['fare'] + dF['YQ'] + dF['YR'] + dF['surcharge_amount_1'] + dF['taxes']
        dF['total_fare_2'] = dF['fare'] + dF['YQ'] + dF['YR'] + dF['surcharge_amount_2'] + dF['taxes']
        dF['total_fare_3'] = dF['fare'] + dF['YQ'] + dF['YR'] + dF['surcharge_amount_3'] + dF['taxes']
        dF['total_fare_4'] = dF['fare'] + dF['YQ'] + dF['YR'] + dF['surcharge_amount_4'] + dF['taxes']
        dF['total_fare'] = dF['fare'] + dF['YQ'] + dF['YR'] + dF['Average_surcharge'] + dF['taxes']
        dF['retention_fare'] = dF['total_fare'] - dF['taxes']
        dF = dF[cols]
        dF['effective_to'] = dF['effective_to'].fillna("2099-12-31")
        dF['is_expired'] = 0
        dF.loc[(dF['effective_to'] < SYSTEM_DATE) |
               (dF['sale_date_to'] < SYSTEM_DATE) |
               ((dF['travel_date_to'] >= extreme_start_date_lw)
                & (dF['travel_date_to'] < extreme_start_date)), 'is_expired'] = 1
        dF = dF.sort_values("fare")
    dF = dF[dF['effective_from'] < dF['effective_to']]
    print "Total number of effective fares = ", len(dF)
    # print dF[['OD', 'is_expired', 'fare_basis', 'fare', 'taxes', 'carrier']]
    # dF['currency_1'] = currency[0]
    # try:
    #     dF['currency_2'] = currency[1]
    # except IndexError:
    #     dF['currency_2'] = "NA"

    dF['display_only'] = 1
    dF.loc[dF['OD'].isin(OD_list), 'display_only'] = 0
    dF['travel_flag_check'] = True
    #print dF.to_string()
    return dF


@measure(JUPITER_LOGGER)
def get_mapped_competitor_fares(host_fares):
    competitor_fares = []
    host_fares = host_fares.dropna(subset=['competitor_farebasis'])
    if len(host_fares) > 0:
        for index, value in host_fares.iterrows():
            for comp_fare_dict in host_fares['competitor_farebasis'][index]:
                comp_fare_dict['host_fb'] = host_fares['fare_basis'][index]
                comp_fare_dict['host_fare_amt'] = host_fares['fare'][index]
                competitor_fares.append(comp_fare_dict)
        competitor_fares_df = pd.DataFrame(competitor_fares)
    else:
        competitor_fares_df = pd.DataFrame(columns=['host_fb', 'host_fare_amt', 'carrier', 'fare_basis', 'fare'])
    return competitor_fares_df


@measure(JUPITER_LOGGER)
def get_weights_df(pos, origin, destination, compartment, dep_date_start, dep_date_end, db):
    global host_rating, host_market_share, host_FMS, host_is_ms_fms
    year = str(datetime.datetime.strptime(dep_date_start, '%Y-%m-%d').year)
    month = str(datetime.datetime.strptime(dep_date_start, '%Y-%m-%d').month)
    combined_query = year + month + pos + origin + destination + compartment
    market_share_cursor = db.JUP_DB_Pos_OD_Compartment_new.find({'combine_column': combined_query})
    market_share_cursor_list = list(market_share_cursor)
    if len(market_share_cursor_list) > 0:
        weights_df = pd.DataFrame(market_share_cursor_list)
        weights_list = []  # Market Share and Rating are referred as weights
        for airline_data in weights_df['top_5_comp_data'][0]:
            temp = {}
            temp['carrier'] = airline_data['airline']
            temp['market_share'] = airline_data['market_share']
            temp['rating'] = airline_data['rating']
            temp['FMS'] = airline_data['FMS']
            weights_list.append(temp)
        weights_df = pd.DataFrame(weights_list)
        weights_df['rating'].fillna(5, inplace=True)
        weights_df['is_ms_fms'] = 0
        weights_df.loc[(weights_df['market_share'].isnull()) & (~weights_df['FMS'].isnull()), 'is_ms_fms'] = 1
        weights_df['market_share'].fillna(weights_df['FMS'], inplace=True)
        weights_df['market_share'].fillna(0, inplace=True)
        weights_df['FMS'].fillna(0, inplace=True)
        if Host_Airline_Code not in list(weights_df['carrier']):
            weights_df = weights_df.append(
                [{'carrier': Host_Airline_Code, 'market_share': 0, 'rating': 5, 'FMS': 0, 'is_ms_fms': 0}])
            weights_df.reset_index(drop=True, inplace=True)
        try:
            host_rating = weights_df[weights_df['carrier'] == Host_Airline_Code]['rating'].values[0]
            host_market_share = weights_df[weights_df['carrier'] == Host_Airline_Code]['market_share'].values[0]
            host_FMS = weights_df[weights_df['carrier'] == Host_Airline_Code]['FMS'].values[0]
            host_is_ms_fms = weights_df[weights_df['carrier'] == Host_Airline_Code]['is_ms_fms'].values[0]
        except KeyError:
            print "This KeyError should not have come ! !"
            host_rating = 5
            host_market_share = 0
            host_FMS = 0
            host_is_ms_fms = 0
    else:
        weights_df = pd.DataFrame(columns=['carrier', 'market_share', 'rating', 'FMS', 'is_ms_fms'])
    return weights_df


@measure(JUPITER_LOGGER)
def get_sellups(host_fares):
    host_fares.reset_index(drop=True, inplace=True)
    size = len(host_fares)
    delta_min_index_model = 0
    min_index_model = 999
    fb_min_index_model = None
    print host_fares.to_string()
    if len(host_fares)>0:
        for index, row in host_fares.iterrows():
            if row['reco_fare_temp'] > 0:
                min_index_model = index
                fb_min_index_model = row['fare_basis']
                if (row['reco_fare_temp'] < (1 + RECOMMENDATION_UPPER_THRESHOLD / 100.0) * row['fare']) and (
                        row['reco_fare_temp'] > (1 + RECOMMENDATION_LOWER_THRESHOLD) * row['fare']):
                    delta_min_index_model = row['reco_fare_temp'] - row['fare']
                elif row['reco_fare_temp'] > row['fare']:
                    delta_min_index_model = (RECOMMENDATION_UPPER_THRESHOLD / 100.0) * row['fare']
                else:
                    delta_min_index_model = (RECOMMENDATION_LOWER_THRESHOLD / 100.0) * row['fare']
                break

        delta_fb = fb_min_index_model
        delta = delta_min_index_model
        for index, row in host_fares.iterrows():
            if index == 0 and row['reco_fare_temp'] == -999:
                host_fares.set_value(index, 'reco_fare_temp', row['fare'])
                host_fares.set_value(index, 'sellup_data', 'No change for Lowest Fare in the fare-ladder')

            elif index < min_index_model and row['reco_fare_temp'] == -999:
                host_fares.set_value(index, 'reco_fare_temp', host_fares['fare'][index] + delta_min_index_model)
                if fb_min_index_model:
                    str_delta = 'Sellup = ' + str(int(delta_min_index_model)) + ' from FB ' + fb_min_index_model
                else:
                    str_delta = 'Sellup = ' + str(int(
                        delta_min_index_model)) + ' because none of the fares in Fare-Ladder was recommended from Oligopoly model'
                host_fares.set_value(index, 'sellup_data', str_delta)

            elif index == min_index_model or row['reco_fare_temp'] != -999:
                if (host_fares['reco_fare_temp'][index] <= (1 + RECOMMENDATION_UPPER_THRESHOLD / 100.0) *
                    host_fares['fare'][index]) and (
                        host_fares['reco_fare_temp'][index] >= (1 + RECOMMENDATION_LOWER_THRESHOLD / 100.0) *
                        host_fares['fare'][index]):
                    delta = host_fares['reco_fare_temp'][index] - host_fares['fare'][index]
                elif host_fares['reco_fare_temp'][index] > host_fares['fare'][index]:
                    delta = (RECOMMENDATION_UPPER_THRESHOLD / 100.0) * host_fares['fare'][index]
                elif host_fares['reco_fare_temp'][index] < host_fares['fare'][index]:
                    delta = (RECOMMENDATION_LOWER_THRESHOLD / 100.0) * host_fares['fare'][index]
                delta_fb = host_fares['fare_basis'][index]

            elif index > min_index_model and row['reco_fare_temp'] == -999:
                host_fares.set_value(index, 'reco_fare_temp', host_fares['fare'][index] + delta)
                str_delta = 'Sellup = ' + str(int(delta)) + ' from FB ' + delta_fb
                host_fares.set_value(index, 'sellup_data', str_delta)
    else:
        cols = list(host_fares.columns.values) + ['sellup_data']
        host_fares = pd.DataFrame(columns=cols)
    return host_fares


@measure(JUPITER_LOGGER)
def get_fare_recommendation(host_fares, fares_weights_df, competitor_fares_df):
    global host_rating, host_market_share
    if len(fares_weights_df) > 0:
        x = fares_weights_df.groupby(by=['host_fb', 'host_fare_amt'], as_index=False)['market_share'].sum()
        x['host_ms'] = host_market_share
        x['total_ms'] = x['market_share'] + x['host_ms']
        merged_df = fares_weights_df.merge(x[['host_fb', 'host_fare_amt', 'total_ms']], on=['host_fb', 'host_fare_amt'],
                                           how='left')
        merged_df['market_share_value'] = merged_df['market_share'] / merged_df['total_ms']
        merged_df['reco_fare_temp'] = (merged_df['fare'] * merged_df['market_share_value']) / (merged_df['rating'])
        final_data_df = merged_df.groupby(by=['host_fb', 'host_fare_amt'], as_index=False)[['reco_fare_temp',
                                                                                            'market_share']].sum()
        final_data_df = final_data_df.rename(columns={"host_fb": "fare_basis", "host_fare_amt": "fare"})
        host_fares = host_fares.merge(final_data_df, on=['fare_basis', 'fare'], how='left')
        host_fares['host_rating'] = host_rating
        host_fares['host_market_share'] = host_market_share
        host_fares['reco_fare_temp'] = host_fares['reco_fare_temp'] + (
                    host_fares['fare'] * host_fares['host_market_share'] / (
                        (host_fares['host_market_share'] + host_fares['market_share']) * host_fares['host_rating']))
        host_fares['reco_fare_temp'] = host_fares['reco_fare_temp'] * host_fares['host_rating']
        host_fares['reco_fare_temp'].fillna(-999, inplace=True)
        host_fares['reco_from_model'] = False
        host_fares.loc[(host_fares['reco_fare_temp'] > 0), 'reco_from_model'] = True
        host_fares_ow = host_fares[host_fares['oneway_return'].isin([1, 3, "1", "3"])]
        host_fares_rt = host_fares[host_fares['oneway_return'].isin([2, "2"])]
        host_fares_ow = get_sellups(host_fares_ow)
        host_fares_rt = get_sellups(host_fares_rt)
        host_fares = pd.concat([host_fares_ow, host_fares_rt], axis=0)
        host_fares['recommended_fare'] = host_fares.apply(
            lambda row: cap_twenty_perc(row['fare'], row['reco_fare_temp']), axis=1)
    else:
        competitor_fares_df['market_share'] = 1
        x = competitor_fares_df.groupby(by=['host_fb', 'host_fare_amt'], as_index=False)['market_share'].sum()
        competitor_fares_df = competitor_fares_df.merge(x, on=['host_fb', 'host_fare_amt'], how='left')
        competitor_fares_df['market_share'] = competitor_fares_df['market_share_x'] / (
                    competitor_fares_df['market_share_y'] + 1)
        competitor_fares_df['rating'] = 5
        competitor_fares_df['reco_fare_temp'] = (competitor_fares_df['fare'] * competitor_fares_df['market_share']) / \
                                                competitor_fares_df['rating']
        mid_df_1 = competitor_fares_df.groupby(by=['host_fb', 'host_fare_amt'], as_index=False)['reco_fare_temp'].sum()
        mid_df_2 = competitor_fares_df.groupby(by=['host_fb', 'host_fare_amt'], as_index=False)['market_share'].mean()
        if len(mid_df_1) == 0:
            mid_df_1 = pd.DataFrame(columns=['host_fb', 'host_fare_amt', 'reco_fare_temp'])
            mid_df_2 = pd.DataFrame(columns=['host_fb', 'host_fare_amt', 'market_share'])
        mid_df_1 = mid_df_1.rename(columns={"host_fb": "fare_basis", "host_fare_amt": "fare"})
        mid_df_2 = mid_df_2.rename(columns={"host_fb": "fare_basis", "host_fare_amt": "fare"})
        host_fares = host_fares.merge(mid_df_1, on=['fare_basis', 'fare'], how='left')
        host_fares = host_fares.merge(mid_df_2, on=['fare_basis', 'fare'], how='left')
        host_fares['host_rating'] = host_rating
        host_fares['host_market_share'] = host_market_share
        host_fares['reco_fare_temp'] = host_fares['reco_fare_temp'] + (
                    host_fares['fare'] * host_fares['host_market_share']) / host_fares['host_rating']
        host_fares['reco_fare_temp'] = host_fares['reco_fare_temp'] * host_rating
        host_fares['reco_fare_temp'].fillna(-999, inplace=True)
        host_fares['reco_from_model'] = False
        host_fares.loc[(host_fares['reco_fare_temp'] > 0), 'reco_from_model'] = True
        host_fares_ow = host_fares[host_fares['oneway_return'].isin([1, 3, "1", "3"])]
        host_fares_rt = host_fares[host_fares['oneway_return'].isin([2, "2"])]
        host_fares_ow = get_sellups(host_fares_ow)
        host_fares_rt = get_sellups(host_fares_rt)
        host_fares = pd.concat([host_fares_ow, host_fares_rt], axis=0)
        host_fares['recommended_fare'] = host_fares.apply(
            lambda row: cap_twenty_perc(row['fare'], row['reco_fare_temp']), axis=1)
    host_fares['recommended_fare_base'] = host_fares['recommended_fare']
    host_fares['recommended_fare'] = host_fares['recommended_fare'] + host_fares['YQ'] + host_fares['YR'] + host_fares[
        'Average_surcharge']  # This is recommended retention fare
    host_fares['recommended_fare_total'] = host_fares['recommended_fare'] + host_fares['taxes']
    host_fares['perc_change'] = (host_fares['recommended_fare_base'] - host_fares['fare']) * 100 / host_fares['fare']
    host_fares['status'] = "I"
    host_fares.loc[host_fares['perc_change'] < 0, 'status'] = "D"
    host_fares.loc[host_fares['perc_change'] == 0, 'status'] = "S"
    return host_fares


@measure(JUPITER_LOGGER)
def get_reco_rating(host_fares, pos, origin, destination, compartment, db):
    fz_currency = host_fares['currency'][0]
    host_fares['competitor_star_rating_config'] = [[]] * len(host_fares)
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
    comp_star_rating_cursor = db.JUP_DB_Competitor_Star_Rating_Configuration.aggregate(
        [
            {
                "$match": {
                    "pos.value": {"$in": pos_list},
                    "origin.value": {"$in": origin_list},
                    "destination.value": {"$in": destination_list},
                    "compartment.value": {"$in": compartment_list},
                    "competitors.eff_dep_date_from": {"$lte": datetime.datetime.strptime(SYSTEM_DATE, "%Y-%m-%d")},
                    "competitors.eff_dep_date_to": {"$gte": datetime.datetime.strptime(SYSTEM_DATE, "%Y-%m-%d")}
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
                        "competitors": 1
                    }
            }
        ]
    )
    # This will return only one document (which has max priority)
    comp_star_rating_cursor_list = list(comp_star_rating_cursor)
    threshold_airline_list = [i['code'] for i in comp_star_rating_cursor_list[0]['competitors']]
    host_fares['competitor_farebasis'].fillna(-999, inplace=True)

    for index, row in host_fares.iterrows():
        comp_config_list = []
        if row['competitor_farebasis'] != -999:
            for competitor in row['competitor_farebasis']:
                if competitor['carrier'] in threshold_airline_list:
                    for i_competitor in comp_star_rating_cursor_list[0]['competitors']:
                        if competitor['carrier'] == i_competitor['code']:
                            comp_name_and_threshold = dict()
                            comp_name_and_threshold['airline'] = i_competitor['code']
                            if row['oneway_return'] in [1, 3, '1', '3']:
                                if i_competitor['ow']['threshold_type'] == 'p':
                                    upper_limit = (1 + i_competitor['ow']['threshold_upper'] / 100.0) * competitor[
                                        'fare']
                                    lower_limit = (1 + i_competitor['ow']['threshold_lower'] / 100.0) * competitor[
                                        'fare']
                                elif i_competitor['ow']['threshold_type'] == 'a':
                                    upper_limit = competitor['fare'] + i_competitor['ow']['threshold_upper'] * \
                                                  exchange_rate[i_competitor['ow']['currency']] / exchange_rate[
                                                      fz_currency]
                                    lower_limit = competitor['fare'] + i_competitor['ow']['threshold_lower'] * \
                                                  exchange_rate[i_competitor['ow']['currency']] / exchange_rate[
                                                      fz_currency]
                                else:
                                    upper_limit = 99999999
                                    lower_limit = 0
                            else:
                                if i_competitor['rt']['threshold_type'] == 'p':
                                    upper_limit = (1 + i_competitor['rt']['threshold_upper'] / 100.0) * competitor[
                                        'fare']
                                    lower_limit = (1 + i_competitor['rt']['threshold_lower'] / 100.0) * competitor[
                                        'fare']
                                elif i_competitor['rt']['threshold_type'] == 'a':
                                    upper_limit = competitor['fare'] + i_competitor['rt']['threshold_upper'] * \
                                                  exchange_rate[i_competitor['rt']['currency']] / exchange_rate[
                                                      fz_currency]
                                    lower_limit = competitor['fare'] + i_competitor['rt']['threshold_lower'] * \
                                                  exchange_rate[i_competitor['rt']['currency']] / exchange_rate[
                                                      fz_currency]
                                else:
                                    upper_limit = 99999999
                                    lower_limit = 0
                            comp_name_and_threshold['upper_limit'] = upper_limit
                            comp_name_and_threshold['lower_limit'] = lower_limit
                            comp_config_list.append(comp_name_and_threshold)
            host_fares.set_value(index, 'competitor_star_rating_config', comp_config_list)
    return host_fares


@measure(JUPITER_LOGGER)
def get_sales_fb_level(pos, origin, destination, compartment, host_fbs, dep_date_start, dep_date_end, db):
    sales_cursor = db.JUP_DB_Manual_Triggers_Module_Summary.aggregate([
        {
            '$match':
                {
                    'pos.City': pos,
                    'origin.City': origin,
                    'destination.City': destination,
                    'compartment.compartment': compartment,
                    'dep_date': {'$lte': dep_date_end, '$gte': dep_date_start}
                }
        },
        {
            '$unwind': {'path': '$farebasis'}
        },
        {
            '$match':
                {
                    'farebasis.farebasis': {'$in': host_fbs}
                }
        },
        {
            '$project':
                {
                    '_id': 0,
                    'dep_date': '$dep_date',
                    'fare_basis': '$farebasis.farebasis',
                    'pax': '$farebasis.pax',
                    'revenue': '$farebasis.rev'
                }
        },
        {
            '$group':
                {
                    '_id':
                        {
                            'fare_basis': '$fare_basis'
                        },
                    'revenue': {'$sum': '$revenue'},
                    'pax': {'$sum': '$pax'}
                }
        },
        {
            '$project':
                {
                    '_id': 0,
                    'fare_basis': '$_id.fare_basis',
                    'fare_pax': '$pax',
                    'revenue': '$revenue'
                }
        }
    ])
    sales_fare_basis = pd.DataFrame(list(sales_cursor))
    return sales_fare_basis


@measure(JUPITER_LOGGER)
def get_pax_yield(pos, origin, destination, compartment, host_fares, dep_date_start, dep_date_end, od_distance, db):
    FZ_CURRENCY = host_fares['currency'][0]
    host_fbs_list = list(set(host_fares['fare_basis']))
    sales_fare_basis = get_sales_fb_level(pos, origin, destination, compartment, host_fbs_list, dep_date_start,
                                          dep_date_end, db=db)
    if len(sales_fare_basis) == 0:
        cols_sales = ['fare_basis', 'fare_pax', 'revenue']
        sales_fare_basis = pd.DataFrame(columns=cols_sales)
    host_fares = host_fares.merge(sales_fare_basis, on='fare_basis', how='left')
    host_fares['current_yield'] = host_fares['revenue'] / host_fares['fare_pax']
    # This part of the code is written to convert yield to AED
    # Some days later. . . This is not required as Revenue is already converted into AED and stored in collection
    # exchange_rate = {}
    # currency_crsr = list(db.JUP_DB_Exchange_Rate.find({"code": FZ_CURRENCY}))
    # for curr in currency_crsr:
    #     exchange_rate[curr['code']] = curr['Reference_Rate']
    if exchange_rate[FZ_CURRENCY]:
        currency_factor = exchange_rate[FZ_CURRENCY]
    else:
        currency_factor = 1.0
    host_fares['current_yield'] = host_fares['current_yield'] / host_fares['od_distance'] * 100
    host_fares['fare_pax'].fillna(0, inplace=True)
    host_fares['current_yield'].fillna(0, inplace=True)
    host_fares['reco_yield'] = host_fares.apply(lambda row: row['recommended_fare'] * exchange_rate[row['currency']] / row['od_distance'] * 100, axis=1)
    return host_fares, currency_factor


@measure(JUPITER_LOGGER)
def get_underlying_data(fares_weights_df, competitor_fares_df):
    underlying_df = pd.DataFrame(columns=['fare_basis', 'fare', 'competitor_data'])
    if len(fares_weights_df) > 0:
        y = fares_weights_df.groupby(by=['host_fb', 'host_fare_amt'])
        underlying_df = []
        for idx, grouped_df in y:
            airline_list = list(grouped_df['carrier'])
            market_share_list = list(grouped_df['market_share'])
            rating_list = list(grouped_df['rating'])
            fare_list = list(grouped_df['fare'])
            fb_list = list(grouped_df['fare_basis'])
            fms_list = list(grouped_df['FMS'])
            is_ms_fms_list = list(grouped_df['is_ms_fms'])
            zipped = zip(airline_list, market_share_list, rating_list, fare_list, fb_list, fms_list, is_ms_fms_list)
            str_dict = dict()
            str_dict['fare_basis'] = idx[0]
            str_dict['fare'] = idx[1]
            str_dict['competitor_data'] = []
            str_dict['competitor_data'].append(add_host_data(idx, host_ms_flag=True))
            for airline_details in zipped:
                str_dict['competitor_data'].append(
                    {
                        "airline": airline_details[0],
                        "market_share": airline_details[1],
                        "rating": airline_details[2],
                        "fare": airline_details[3],
                        "fare_basis": airline_details[4],
                        "FMS": airline_details[5],
                        "is_ms_fms": airline_details[6]
                    }
                )
            underlying_df.append(str_dict)
        underlying_df = pd.DataFrame(underlying_df)
    else:
        if len(competitor_fares_df) > 0:
            competitor_fares_df['market_share'] = 1
            competitor_fares_df['rating'] = 5
            grouped_comp_fares_df = competitor_fares_df.groupby(by=['host_fb', 'host_fare_amt'])
            underlying_df = []
            for idx, grouped_comp in grouped_comp_fares_df:
                airline_list = list(grouped_comp['carrier'])
                market_share_list = list(grouped_comp['market_share'])
                rating_constant = 5
                fare_list = list(grouped_comp['fare'])
                fb_list = list(grouped_comp['fare_basis'])
                zipped = zip(airline_list, market_share_list, fare_list, fb_list)
                str_dict = dict()
                str_dict['fare_basis'] = idx[0]
                str_dict['fare'] = idx[1]
                str_dict['competitor_data'] = []
                str_dict['competitor_data'].append(add_host_data(idx, host_ms_flag=False))
                for airline_details in zipped:
                    str_dict['competitor_data'].append(
                        {
                            "airline": airline_details[0],
                            "market_share": airline_details[1],
                            "rating": rating_constant,
                            "fare": airline_details[2],
                            "fare_basis": airline_details[3],
                            "is_ms_fms": 0
                        }
                    )
                underlying_df.append(str_dict)
            underlying_df = pd.DataFrame(underlying_df)
    return underlying_df


@measure(JUPITER_LOGGER)
def add_host_data(idx, host_ms_flag=True):
    global host_FMS, host_market_share, host_rating, host_is_ms_fms
    host_fare = idx[1]
    host_fb = idx[0]
    if host_ms_flag:
        host_data = {
            "airline": Host_Airline_Code,
            "market_share": host_market_share,
            "FMS": host_FMS,
            "rating": host_rating,
            "fare": host_fare,
            "fare_basis": host_fb,
            "is_ms_fms": host_is_ms_fms
        }
    else:
        host_market_share = 1
        host_data = {
            "airline": Host_Airline_Code,
            "market_share": host_market_share,
            "rating": host_rating,
            "fare": host_fare,
            "fare_basis": host_fb,
            "is_ms_fms": host_is_ms_fms
        }

    return host_data


@measure(JUPITER_LOGGER)
def get_event_travel_dates(fares_df, origin, destination, db):
    """
    If there is any fare in the fare ladder whose travel dates are exactly matching with event dates in
    that market, show event name instead of travel dates.
    """
    od = origin + destination
    this_year = str(datetime.datetime.strptime(SYSTEM_DATE, '%Y-%m-%d').year)
    start_date = 'Start_date_' + this_year
    end_date = 'End_date_' + this_year
    event_cursor = db.JUP_DB_Pricing_Calendar.find({'Market': od},
                                                   {'_id': 0, 'Holiday_Name': 1, start_date: 1, end_date: 1})
    if event_cursor.count() > 0:
        events_df = pd.DataFrame(list(event_cursor))
        events_df.rename(
            columns={'Holiday_Name': 'event_name', start_date: 'travel_date_from', end_date: 'travel_date_to'},
            inplace=True)
    else:
        events_df = pd.DataFrame(columns=['event_name', 'travel_date_from', 'travel_date_to'])
    host_fares = pd.merge(fares_df, events_df, on=['travel_date_from', 'travel_date_to'], how='left')
    return host_fares


@measure(JUPITER_LOGGER)
def get_reco_rbds(dep_date_start, dep_date_end, dates_code, db):
    if dates_code is not None and dates_code[0][0] == 'M':
        dep_date_start_obj = datetime.datetime.strptime(dep_date_start, '%Y-%m-%d')
        delta_time = (12 * dep_date_start_obj.year + dep_date_start_obj.month) - (12 * today.year + today.month)
        reco_rbds = db.Temp_fzDB_tbl_005.find({'period': 'months_' + str(delta_time + 1)}, {'rbd_list': 1, '_id': 0})
        rbd_df = pd.DataFrame(list(reco_rbds)[0]['rbd_list'])

    elif dates_code is not None and dates_code[0][0] == 'W':
        reco_rbds = db.Temp_fzDB_tbl_005.find({'period': 'weeks_' + str(len(dates_code))}, {'rbd_list': 1, '_id': 0})
        rbd_df = pd.DataFrame(list(reco_rbds)[0]['rbd_list'])

    else:
        rbd_df = pd.DataFrame(columns=['rbd', 'flag'])

    rbd_df.rename(columns={'rbd': 'RBD', 'flag': 'reco_flag'}, inplace=True)
    return rbd_df[rbd_df['reco_flag'] == 'Y']


@measure(JUPITER_LOGGER)
def add_pricing_model(host_fares, web_pricing, pos, origin, destination, compartment, dep_date_start, dep_date_end, db,
                      mpf_df=None):
    if web_pricing['competitor_rules']['ow']:
        web_pricing_ow_df = pd.DataFrame(web_pricing['competitor_rules']['ow'])
    else:
        web_pricing_ow_df = pd.DataFrame()
    if web_pricing['competitor_rules']['rt']:
        web_pricing_rt_df = pd.DataFrame(web_pricing['competitor_rules']['rt'])
    else:
        web_pricing_rt_df = pd.DataFrame()

    date_list = [{'start': dep_date_start, 'end': dep_date_end}]
    # if mpf_df is None:
    #     mpf_df = get_most_popular_fare(pos=pos, origin=origin, destination=destination, compartment=compartment,
    #                                    dep_date_start=dep_date_start, dep_date_end=dep_date_end, date_list=date_list)
    try:
        if (len(web_pricing_ow_df) > 0) & (len(mpf_df) > 0):
            delta_currency = list(db.JUP_DB_Pos_Currency_Master.find({'pos': pos}, {'_id': 0, 'web': 1}))[0]['web']
            host_currency = mpf_df[mpf_df['carrier'] == Host_Airline_Code]['currency'].values[0]
            reco_mpf_ow = 999999999
            competitor_benchmark_ow = None
            for competitor in web_pricing_ow_df['carrier'].values:
                try:
                    comp_currency = mpf_df[mpf_df['carrier'] == competitor]['currency'].values[0]
                    comp_benchmark_ow_fare_total_ = \
                        mpf_df[mpf_df['carrier'] == competitor]['most_avail_fare_total_ow'].values[0]
                    delta_ow_ = web_pricing_ow_df[web_pricing_ow_df['carrier'] == competitor]['delta'].values[0]
                    reco_mpf_ow_ = comp_benchmark_ow_fare_total_ + delta_ow_ * exchange_rate[comp_currency] / \
                                   exchange_rate[delta_currency]
                    reco_mpf_ow_ = reco_mpf_ow_ * exchange_rate[host_currency] / exchange_rate[comp_currency]
                    if reco_mpf_ow_ < reco_mpf_ow:
                        reco_mpf_ow = reco_mpf_ow_
                        comp_benchmark_currency = comp_currency
                        competitor_benchmark_ow = competitor
                        comp_benchmark_ow_fare_total = comp_benchmark_ow_fare_total_
                        delta_ow = delta_ow_
                except IndexError:
                    print '{} does not exist in MPF ow df'.format(competitor)

            temp1 = pd.DataFrame()
            if competitor_benchmark_ow:
                nearest_fare_ow = min(host_fares[(host_fares['oneway_return'] == '1') &
                                                 (host_fares['channel'] == 'web')]['total_fare'].values,
                                      key=lambda x: abs(x - reco_mpf_ow))
                temp1 = host_fares[(host_fares['oneway_return'] == '1') &
                                   (host_fares['channel'] == 'web') &
                                   (host_fares['total_fare'] <= nearest_fare_ow)]

            if len(temp1) > 0:
                try:
                    tax_ow = host_fares[(host_fares['oneway_return'] == '1') &
                               (host_fares['channel'] == 'web') &
                               (host_fares['total_fare'] == nearest_fare_ow)]['taxes'].values[0]
                except:
                    tax_ow = 0
                final_currency_ow = host_fares[(host_fares['oneway_return'] == '1') &
                               (host_fares['channel'] == 'web') &
                               (host_fares['total_fare'] == nearest_fare_ow)]['currency'].values[0]
                host_fares.loc[(host_fares['oneway_return'] == '1') &
                               (host_fares['channel'] == 'web') &
                               (host_fares['total_fare'] == nearest_fare_ow), 'web_pricing'] = True
                host_fares.loc[(host_fares['oneway_return'] == '1') &
                               (host_fares['channel'] == 'web') &
                               (host_fares['total_fare'] == nearest_fare_ow), 'recommended_fare'] = \
                    (reco_mpf_ow * exchange_rate[final_currency_ow] / exchange_rate[host_currency]) - tax_ow
                host_fares.loc[(host_fares['oneway_return'] == '1') &
                               (host_fares['channel'] == 'web') &
                               (host_fares['total_fare'] == nearest_fare_ow), 'reco_from_model'] = False
                host_fares.loc[(host_fares['oneway_return'] == '1') &
                               (host_fares['channel'] == 'web') &
                               (host_fares['total_fare'] < nearest_fare_ow), 'fare_less_than_mpf_reco'] = True
                sellup_data_str_ow = 'Recommendation for Most Popular Web Fare based on Competitor Rules.\nCompetitor = ' + str(
                    competitor_benchmark_ow) + ' \nDelta = ' + str(delta_ow) + ' ' + str(
                    delta_currency) + '\nCompetitor MPF inclusive of TAX = ' + str(
                    comp_benchmark_ow_fare_total) + ' ' + str(comp_benchmark_currency)
                host_fares.loc[(host_fares['oneway_return'] == '1') &
                               (host_fares['channel'] == 'web') &
                               (host_fares['total_fare'] == nearest_fare_ow), 'sellup_data'] = sellup_data_str_ow
            else:
                print "Nearest MPF fare for host not found in ATPCO"
        else:
            pass
    except Exception:
        pass
        #file = open('Error.txt', 'a')
        #err_string = pos + origin + destination + compartment + dep_date_start + dep_date_end + '\t' + str(mpf_df.to_dict('records')) + '\n'
        #file.write(err_string)

    try:
        if (len(web_pricing_rt_df) > 0) & (len(mpf_df) > 0):
            delta_currency = list(db.JUP_DB_Pos_Currency_Master.find({'pos': pos}, {'_id': 0, 'web': 1}))[0]['web']
            host_currency = mpf_df[mpf_df['carrier'] == Host_Airline_Code]['currency'].values[0]
            reco_mpf_rt = 999999999
            competitor_benchmark_rt = None
            for competitor in web_pricing_rt_df['carrier'].values:
                try:
                    comp_currency = mpf_df[mpf_df['carrier'] == competitor]['currency'].values[0]
                    comp_benchmark_rt_fare_total_ = \
                        mpf_df[mpf_df['carrier'] == competitor]['most_avail_fare_total_rt'].values[0]
                    delta_rt_ = web_pricing_rt_df[web_pricing_rt_df['carrier'] == competitor]['delta'].values[0]
                    reco_mpf_rt_ = comp_benchmark_rt_fare_total_ + delta_rt_ * exchange_rate[comp_currency] / \
                                   exchange_rate[delta_currency]
                    reco_mpf_rt_ = reco_mpf_rt_ * exchange_rate[host_currency] / exchange_rate[comp_currency]
                    if reco_mpf_rt_ < reco_mpf_rt:
                        reco_mpf_rt = reco_mpf_rt_
                        comp_benchmark_currency = comp_currency
                        competitor_benchmark_rt = competitor
                        comp_benchmark_rt_fare_total = comp_benchmark_rt_fare_total_
                        delta_rt = delta_rt_
                except IndexError:
                    print '{} does not exist in MPF rt df'.format(competitor)

            temp2 = pd.DataFrame()
            if competitor_benchmark_rt:
                nearest_fare_rt = min(host_fares[(host_fares['oneway_return'] == '2') &
                                                 (host_fares['channel'] == 'web')]['total_fare'].values,
                                      key=lambda x: abs(x - reco_mpf_rt))
                temp2 = host_fares[(host_fares['oneway_return'] == '2') &
                                   (host_fares['channel'] == 'web') &
                                   (host_fares['total_fare'] <= nearest_fare_rt)]

            if len(temp2) > 0:
                try:
                    tax_rt = host_fares[(host_fares['oneway_return'] == '2') &
                                        (host_fares['channel'] == 'web') &
                                        (host_fares['total_fare'] == nearest_fare_rt)]['taxes'].values[0]
                except:
                    tax_rt = 0

                final_currency_rt = host_fares[(host_fares['oneway_return'] == '2') &
                                               (host_fares['channel'] == 'web') &
                                               (host_fares['total_fare'] == nearest_fare_rt)]['currency'].values[0]
                host_fares.loc[(host_fares['oneway_return'] == '2') &
                               (host_fares['channel'] == 'web') &
                               (host_fares['total_fare'] == nearest_fare_rt), 'web_pricing'] = True
                host_fares.loc[(host_fares['oneway_return'] == '2') &
                               (host_fares['channel'] == 'web') &
                               (host_fares['total_fare'] == nearest_fare_rt), 'recommended_fare'] = \
                    (reco_mpf_rt * exchange_rate[final_currency_rt] / exchange_rate[host_currency]) - tax_rt
                host_fares.loc[(host_fares['oneway_return'] == '2') &
                               (host_fares['channel'] == 'web') &
                               (host_fares['total_fare'] == nearest_fare_rt), 'reco_from_model'] = False
                host_fares.loc[(host_fares['oneway_return'] == '2') &
                               (host_fares['channel'] == 'web') &
                               (host_fares['total_fare'] < nearest_fare_rt), 'fare_less_than_mpf_reco'] = True
                sellup_data_str_rt = 'Recommendation for Most Popular Web Fare based on Competitor Rules.\nCompetitor = ' + str(
                    competitor_benchmark_rt) + ' \nDelta = ' + str(delta_rt) + ' ' + str(
                    delta_currency) + '\nCompetitor MPF inclusive of TAX = ' + str(
                    comp_benchmark_rt_fare_total) + ' ' + str(comp_benchmark_currency)
                host_fares.loc[(host_fares['oneway_return'] == '2') &
                               (host_fares['channel'] == 'web') &
                               (host_fares['total_fare'] == nearest_fare_rt), 'sellup_data'] = sellup_data_str_rt
            else:
                print "Nearest MPF fare for host not found in ATPCO"

        else:
            pass
    except Exception:
        pass
        #file = open('Error.txt', 'a')
        #err_string = pos + origin + destination + compartment + str(date_list) + '\t' + str(mpf_df.to_dict('records')) + '\n'
        #file.write(err_string)
    return host_fares


@measure(JUPITER_LOGGER)
def web_pricing_caller(host_fares, pos, origin, destination, compartment, dep_date_start, dep_date_end, db, mpf_df=None):
    host_fares['web_pricing'] = False
    host_fares['fare_less_than_mpf_reco'] = False
    web_pricing = list(db.JUP_DB_Web_Pricing_Ad_Hoc.find({'od': origin + destination},
                                                         {'_id': 0, 'competitor_benchmark': 1,
                                                          'competitor_rules': 1}))
    if len(web_pricing) > 0 and str(web_pricing[0]['competitor_rules']) != str(np.nan) and compartment == 'Y' and \
            mpf_df is not None:
        try:
            host_fares = add_pricing_model(host_fares, web_pricing[0], pos, origin, destination, compartment,
                                           dep_date_start, dep_date_end, db, mpf_df)
            print "Web Pricing Done"
        except KeyError:
            pass

    return host_fares


@measure(JUPITER_LOGGER)
def oligopoly_recommendation(pos, origin, destination, compartment, dep_date_start, dep_date_end, db, dates_code=None, mpf_df=None,
                             host_fares=None):
    """
    CSVs were created for testing. If testing is required anytime, just
    uncomment out all the CSVs in all functions.
    """
    print pos, origin, destination, compartment, dep_date_start, dep_date_end
    # if pos:
    #     if pos == destination:
    #         org = deepcopy(destination)
    #         dest = deepcopy(origin)
    #         origin = org
    #         destination = dest
    od_distance = get_od_distance(od=origin + destination, db=db)
    st = time.time()

    travel_flag = 1
    if (host_fares is not None) and (len(host_fares) == 0):
        host_fares = get_host_fares_df(pos=pos,
                                       origin=origin,
                                       destination=destination,
                                       compartment=compartment,
                                       extreme_start_date=dep_date_start,
                                       extreme_end_date=dep_date_end, db=db)
        travel_flag = 0

    if host_fares is None:
        host_fares = get_host_fares_df(pos=pos,
                                       origin=origin,
                                       destination=destination,
                                       compartment=compartment,
                                       extreme_start_date=dep_date_start,
                                       extreme_end_date=dep_date_end, db=db)
        travel_flag = 0

    if travel_flag == 1:
        host_fares = get_filtered_df(dep_date_start=dep_date_start, dep_date_end=dep_date_end, fares_df=host_fares)

    host_fares['od_distance'] = od_distance
    host_fares['od_distance'] = host_fares['od_distance'] * host_fares['oneway_return'].astype('int')
    host_fares['unique'] = range(len(host_fares))
    print "Got host fares from ATPCO_Fares_Rules in %s seconds" % (time.time() - st)
    if len(host_fares) > 0:
        competitor_fares_df = get_mapped_competitor_fares(host_fares=host_fares)
        st = time.time()
        weights_df = get_weights_df(pos=pos,
                                    origin=origin,
                                    destination=destination,
                                    compartment=compartment,
                                    dep_date_start=dep_date_start,
                                    dep_date_end=dep_date_end,
                                    db=db)
        print "Got MS/Rating from Pos_OD_Compartment Collection in %s seconds" % (time.time() - st)
        fares_weights_df = competitor_fares_df.merge(weights_df, on='carrier', how='inner')
        host_fares_not_expired = host_fares[host_fares['is_expired'] == 0]
        host_fares_expired = host_fares[host_fares['is_expired'] == 1]
        # print "3:"
        # print host_fares[['OD', 'display_only']]
        if len(host_fares_not_expired) > 0:
            host_fares = get_fare_recommendation(host_fares_not_expired, fares_weights_df, competitor_fares_df)
        else:
            host_fares = pd.DataFrame()
            host_fares_expired['reco_fare_temp'] = np.nan
            host_fares_expired['recommended_fare'] = np.nan
            host_fares_expired['reco_from_model'] = False
        host_fares = pd.concat([host_fares, host_fares_expired])
        host_fares['reco_fare_temp'].fillna(host_fares['fare'], inplace=True)
        host_fares['recommended_fare'].fillna(
            (host_fares['fare'] + host_fares['YQ'] + host_fares['YR'] + host_fares['Average_surcharge']), inplace=True)
        host_fares.sort_values('fare', inplace=True)
        host_fares.reset_index(drop=True, inplace=True)
        # print "2:"
        # print host_fares[['OD', 'display_only']]
        print "Got recommended fares"
        client_rbd=mongo_client()
        rbd_cur = list(client_rbd[ATPCO_DB].JUP_DB_ATPCO_RBD.find({'CARRIER': Host_Airline_Code}))[0]
        host_fares = get_inverted_fares(host_fares, compartment, rbd_cur)
        # print "1: "
        # print host_fares[['OD', 'display_only']]
        print "Got inverted fares"
        host_fares = get_reco_rating(host_fares=host_fares,
                                     pos=pos,
                                     origin=origin,
                                     destination=destination,
                                     compartment=compartment, db=db)
        print "Got Reco Rating"
        host_fares, currency_factor = get_pax_yield(pos, origin, destination, compartment, host_fares, dep_date_start,
                                                    dep_date_end, od_distance, db=db)
        print "Got sales data from Manual Triggers module"
        # host_fares['reco_yield'] = host_fares['recommended_fare'] * currency_factor / host_fares['od_distance'] * 100
        underlying_df = get_underlying_data(fares_weights_df, competitor_fares_df)
        host_fares = pd.merge(host_fares, underlying_df, on=['fare_basis', 'fare'], how='left')
        host_fares.loc[host_fares['is_expired'] == 1, 'sellup_data'] = 'No fare change for Expired Fares'
        host_fares['recommended_fare'] = host_fares['recommended_fare'].round().astype('int')
        host_fares['retention_fare'] = host_fares['retention_fare'].round().astype('int')
        host_fares = host_fares.sort_values(by='fare')
        host_fares = correct_inverted_recommendations(compartment, host_fares, rbd_cur)
        print "Got inverted recommendations"
        host_fares = get_event_travel_dates(host_fares, origin, destination, db=db)
        #print host_fares.to_string()
        client_rbd.close()
    else:
        print "No fare found for Host in ATPCO Collection."
    return host_fares


@measure(JUPITER_LOGGER)
def main(pos, origin, destination, compartment, dep_date_start, dep_date_end, db, dates_code=None, mpf_df=None,
         host_fares=None):
    """
    CSVs were created for testing. If testing is required anytime, just
    uncomment out all the CSVs in all functions.
    """
    host_fares = oligopoly_recommendation(pos=pos, origin=origin, destination=destination,
                                          compartment=compartment, dep_date_start=dep_date_start,
                                          dep_date_end=dep_date_end, db=db, dates_code=dates_code,
                                          mpf_df=mpf_df, host_fares=host_fares)
    host_fares = web_pricing_caller(host_fares=host_fares, pos=pos, origin=origin,
                                    destination=destination, compartment=compartment,
                                    dep_date_start=dep_date_start, dep_date_end=dep_date_end, db=db,
                                    mpf_df=mpf_df)
    print host_fares.to_string()
    return host_fares.to_dict('records')


# @measure(JUPITER_LOGGER)
# def main_all(pos, origin, destination, compartment, dep_date_start, dep_date_end, dates_code=None, mpf_df=None,
#              host_fares=None):
#     """
#     CSVs were created for testing. If testing is required anytime, just
#     uncomment out all the CSVs in all functions.
#     """
#     print pos, origin, destination, compartment, dep_date_start, dep_date_end
#     # if pos:
#     #     if pos == destination:
#     #         org = deepcopy(destination)
#     #         dest = deepcopy(origin)
#     #         origin = org
#     #         destination = dest
#     od_distance = get_od_distance(origin + destination)
#     st = time.time()
#
#     travel_flag = 1
#     if (host_fares is not None) and (len(host_fares) == 0):
#         host_fares = get_host_fares_df(pos=pos,
#                                        origin=origin,
#                                        destination=destination,
#                                        compartment=compartment,
#                                        extreme_start_date=dep_date_start,
#                                        extreme_end_date=dep_date_end)
#         travel_flag = 0
#
#     if host_fares is None:
#         host_fares = get_host_fares_df(pos=pos,
#                                        origin=origin,
#                                        destination=destination,
#                                        compartment=compartment,
#                                        extreme_start_date=dep_date_start,
#                                        extreme_end_date=dep_date_end)
#         travel_flag = 0
#
#     if travel_flag == 1:
#         host_fares = get_filtered_df(dep_date_start=dep_date_start, dep_date_end=dep_date_end, fares_df=host_fares)
#
#     host_fares['od_distance'] = od_distance
#     host_fares['od_distance'] = host_fares['od_distance'] * host_fares['oneway_return'].astype('int')
#     print "Got host fares from ATPCO_Fares_Rules in %s seconds" % (time.time() - st)
#
#     if len(host_fares) > 0:
#         host_fares = get_event_travel_dates(host_fares, origin, destination)
#         rbd_df = get_reco_rbds(dep_date_start, dep_date_end, dates_code)
#
#         host_fares, currency_factor = get_pax_yield(pos, origin, destination, compartment, host_fares, dep_date_start,
#                                                     dep_date_end, od_distance)
#         print "Got sales data from Manual Triggers module"
#
#         host_fares['unique'] = range(len(host_fares))
#
#         if len(rbd_df) > 0:
#             host_fares = pd.merge(host_fares, rbd_df, on='RBD', how='left')
#             host_fares['reco_flag'].fillna('N', inplace=True)
#             host_fares_not_for_reco = host_fares[host_fares['reco_flag'] == 'N']
#             host_fares = host_fares[host_fares['reco_flag'] == 'Y']
#         else:
#             host_fares['reco_flag'] = 'Y'
#             host_fares_not_for_reco = pd.DataFrame(columns=host_fares.columns)
#
#         competitor_fares_df = get_mapped_competitor_fares(host_fares=host_fares)
#         st = time.time()
#         weights_df = get_weights_df(pos=pos,
#                                     origin=origin,
#                                     destination=destination,
#                                     compartment=compartment,
#                                     dep_date_start=dep_date_start,
#                                     dep_date_end=dep_date_end)
#         print "Got MS/Rating from Pos_OD_Compartment Collection in %s seconds" % (time.time() - st)
#         fares_weights_df = competitor_fares_df.merge(weights_df, on='carrier', how='inner')
#         host_fares_not_expired = host_fares[host_fares['is_expired'] == 0]
#         host_fares_expired = host_fares[host_fares['is_expired'] == 1]
#         # print "3:"
#         # print host_fares[['OD', 'display_only']]
#         if len(host_fares_not_expired) > 0:
#             host_fares = get_fare_recommendation(host_fares_not_expired, fares_weights_df, competitor_fares_df)
#         else:
#             host_fares = pd.DataFrame()
#             host_fares_expired['reco_fare_temp'] = np.nan
#             host_fares_expired['recommended_fare'] = np.nan
#             host_fares_expired['reco_from_model'] = False
#         host_fares = pd.concat([host_fares, host_fares_expired])
#         host_fares['reco_fare_temp'].fillna(host_fares['fare'], inplace=True)
#         host_fares['recommended_fare'].fillna(
#             (host_fares['fare'] + host_fares['YQ'] + host_fares['YR'] + host_fares['Average_surcharge']), inplace=True)
#         host_fares.sort_values('fare', inplace=True)
#         host_fares.reset_index(drop=True, inplace=True)
#         # print "2:"
#         # print host_fares[['OD', 'display_only']]
#         print "Got recommended fares"
#         rbd_cur = list(client[ATPCO_DB].JUP_DB_ATPCO_RBD.find({'CARRIER': Host_Airline_Code}))[0]
#         host_fares = get_inverted_fares(host_fares, compartment, rbd_cur)
#         # print "1: "
#         # print host_fares[['OD', 'display_only']]
#         print "Got inverted fares"
#         host_fares = get_reco_rating(host_fares=host_fares,
#                                      pos=pos,
#                                      origin=origin,
#                                      destination=destination,
#                                      compartment=compartment)
#         print "Got Reco Rating"
#
#         host_fares['reco_yield'] = host_fares['recommended_fare'] * currency_factor / host_fares['od_distance'] * 100
#         underlying_df = get_underlying_data(fares_weights_df, competitor_fares_df)
#         host_fares = pd.merge(host_fares, underlying_df, on=['fare_basis', 'fare'], how='left')
#         host_fares.loc[host_fares['is_expired'] == 1, 'sellup_data'] = 'No fare change for Expired Fares'
#         host_fares['recommended_fare'] = host_fares['recommended_fare'].astype('int')
#         host_fares = host_fares.sort_values(by='fare')
#         host_fares = correct_inverted_recommendations(compartment, host_fares, rbd_cur)
#         print "Got inverted recommendations"
#         host_fares['web_pricing'] = False
#         web_pricing = list(db.JUP_DB_Web_Pricing_Ad_Hoc.find({'od': origin + destination},
#                                                              {'_id': 0, 'competitor_benchmark': 1,
#                                                               'competitor_rules': 1}))
#         if len(web_pricing) > 0 and str(web_pricing[0]['competitor_rules']) != str(np.nan):
#             try:
#                 host_fares = add_pricing_model(host_fares, web_pricing[0], pos, origin, destination, compartment,
#                                                dep_date_start, dep_date_end, mpf_df)
#                 print "Web Pricing Done"
#             except KeyError:
#                 pass
#
#         host_fares_not_for_reco['host_rating'] = host_rating
#         host_fares_not_for_reco['host_market_share'] = host_market_share
#         host_fares_not_for_reco['web_pricing'] = False
#         host_fares_not_for_reco['perc_change'] = 0
#         host_fares_not_for_reco['reco_from_model'] = False
#         host_fares_not_for_reco['reco_yield'] = host_fares_not_for_reco['current_yield']
#         host_fares_not_for_reco['reco_fare_temp'] = host_fares_not_for_reco['fare']
#         host_fares_not_for_reco['recommended_fare_base'] = host_fares_not_for_reco['fare']
#         host_fares_not_for_reco['recommended_fare'] = host_fares_not_for_reco['fare'] + \
#                                                       host_fares_not_for_reco['YQ'] + \
#                                                       host_fares_not_for_reco['YR'] + \
#                                                       host_fares_not_for_reco['Average_surcharge']
#         host_fares_not_for_reco['recommended_fare_total'] = host_fares_not_for_reco['recommended_fare'] + \
#                                                             host_fares_not_for_reco['taxes']
#         host_fares_not_for_reco['sellup_data'] = 'No recommendation for this RBD'
#         host_fares_not_for_reco['status'] = 'S'
#         host_fares_not_for_reco['competitor_farebasis'].fillna(-999, inplace=True)
#         host_fares_not_for_reco['competitor_star_rating_config'] = np.empty((len(host_fares_not_for_reco), 0)).tolist()
#
#         host_fares = pd.concat([host_fares, host_fares_not_for_reco])
#         host_fares = host_fares.sort_values(by='fare')
#
#     else:
#         print "No fares found for Host in ATPCO Collection."
#     return host_fares.to_dict('records')


if __name__ == '__main__':
    # temp_markets_25 = ['CMBDXBCMBY', 'AMMAMMDXBY',
    #                    'KWIBOMKWIJ']
    # for mrkt in temp_markets_25:
    #mrkt = "AMMAMMDWCY"
    # mrkt = "UAEJIBDXBY"
    client = mongo_client()
    db=client[JUPITER_DB]
    #mrkt = "UAEDXBJEDY"
    market_array = ["KWIKWISYDY"]
    for mrkt in market_array:
        print mrkt
        fares = main(pos=mrkt[0:3], origin=mrkt[3:6], destination=mrkt[6:9], compartment=mrkt[9:],
                 dep_date_start='2023-06-18', dep_date_end='2023-08-12', db=db)
        df = pd.DataFrame(fares)
        print df.to_string()

        #print df[['fare', 'fare_basis', 'currency', 'OD']]
    # pd.DataFrame(fares).to_csv('KWIKWIDXB.csv')
    # df=get_host_fares_df(pos='UAE', origin='DXB', destination='GYD', compartment='Y',
    #                   extreme_start_date='2018-04-20', extreme_end_date='2018-05-20')
    # print df[['fare_basis', 'YQ', 'YR', 'taxes']]
    client.close()