from copy import deepcopy
#from jupiter_AI.triggers.host_params_workflow_opt import get_od_distance
from jupiter_AI.triggers.common import pos_level_currency_data as get_currency_data
from collections import defaultdict
from jupiter_AI.network_level_params import SYSTEM_DATE, today
from jupiter_AI import client, JUPITER_DB, Host_Airline_Code
import time
import numpy as np
import pandas as pd
import datetime
import json
import pprint
import traceback

from jupiter_AI.triggers.workflow_mrkt_level_update import get_dep_date_filters

pd.options.mode.chained_assignment = None

#db = client[JUPITER_DB]

RECOMMENDATION_UPPER_THRESHOLD = 20
RECOMMENDATION_LOWER_THRESHOLD = -20
FZ_RATING = 4.6
host_rating = FZ_RATING
host_market_share = 0
host_FMS = 0


def convert_tax_currency(tax_value, host_curr, tax_currency, exchange_rate):
    if tax_currency == -999:
        return 0
    else:
        tax_currency = tax_currency.upper()
        host_curr = host_curr.upper()
        tax_value = tax_value/exchange_rate[host_curr] * exchange_rate[tax_currency]
        return tax_value


def get_inverted_fares(host_fares, compartment):
    if compartment == 'Y':
        rbd_ladder = ["Y", "A", "I", "E", "O", "W", "T", "M", "N", "R", "B", "U", "K", "Q", "L", "V", "H", "G", "S", "X"]
    else:
        rbd_ladder = ["J", "C", "Z", "D"]
    map_ranks = pd.DataFrame()
    map_ranks['RBD'] = rbd_ladder
    map_ranks['rank'] = range(len(rbd_ladder), 0, -1)
    host_fares['unique'] = range(len(host_fares))  # unique is the index of each fare as found in host_fares
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


def travel_dates_check(row , start_date, end_date):
    for i in row:
        if i['TRAVEL_DATES_COMM'] == start_date:
            return True


def get_host_fares_df(pos,
                      extreme_start_date,
                      extreme_end_date,
                      od_list,
                      do_list,
                      compartment,
                      currency_data,
                      date_ranges,
                      exchange_rate,
                      comp=[Host_Airline_Code],
                      channel=None):

    currency = []
    if currency_data[pos]['web']:
        currency.append(currency_data[pos]['web'])
    if currency_data[pos]['gds']:
        currency.append(currency_data[pos]['gds'])

    tax_query = []
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
    else:
        tax_df = pd.DataFrame(columns=['Total',
                                       'Origin',
                                       'Destination',
                                       'Currency',
                                       'OW/RT',
                                       'Compartment'])
    tax_df['Total'].fillna(0)
    tax_currencies = list(tax_df['Currency'].unique())
    tax_df['OD'] = tax_df['Origin'] + tax_df['Destination']
    tax_df = tax_df.rename(
        columns={
            'Currency': 'tax_currency',
            'OW/RT': 'oneway_return',
            'Total': 'tax_value_from_master'})
    tax_df.drop(['Origin', 'Destination', 'Compartment'], axis=1, inplace=True)
    tax_df['oneway_return'] = tax_df['oneway_return'].apply(str)

    SYSTEM_DATE_LW = str(today - datetime.timedelta(days=7))
    extreme_start_date_n = "0" + extreme_start_date[2:4] + extreme_start_date[5:7] + extreme_start_date[8:10]
    extreme_end_date_n = "0" + extreme_end_date[2:4] + extreme_end_date[5:7] + extreme_end_date[8:10]
    extreme_start_date_obj = datetime.datetime.strptime(extreme_start_date, "%Y-%m-%d")
    extreme_start_date_lw = extreme_start_date_obj - datetime.timedelta(days=7)
    extreme_start_date_lw = extreme_start_date_lw.strftime("%Y-%m-%d")
    extreme_start_date_n_lw = "0" + extreme_start_date_lw[2:4] + extreme_start_date_lw[5:7] + extreme_start_date_lw[8:10]
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
        "day_of_week"
    ]

    print "Aggregating on ATPCO Fares Rules . . . . "

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

    fares_data = list(db.uniquefootnote.aggregate(
        [
            {
                '$match': query
            }
        ]
    ))
    dF = pd.DataFrame(columns=cols)

    # exchange_rate = {}
    # pos_country = list(db.JUP_DB_Region_Master.find({"POS_CD": pos}))[0]['COUNTRY_CD']
    # yr_crsr = list(db.JUP_DB_YR_Master.find({"POS-LocCode": pos_country, "Compartment": compartment}))
    # if len(yr_crsr) == 0:
    #     yr_crsr = list(db.JUP_DB_YR_Master.find({"POS-LocCode": "MISC"}))
    # yr_curr = yr_crsr[0]['Curr']
    # yr = {}
    # tax_crsr = list(db.JUP_DB_Tax_Master.find({
    #
    #     '$or': tax_query
    # }))
    # if len(tax_crsr) != 0:
    #     tax_df = pd.DataFrame(tax_crsr)
    # else:
    #     tax_df = pd.DataFrame(columns=['Total',
    #                                    'Origin',
    #                                    'Destination',
    #                                    'Currency',
    #                                    'OW/RT',
    #                                    'Compartment'])
    # tax_df['Total'].fillna(0)
    # tax_currencies = list(tax_df['Currency'].unique())
    # tax_df['OD'] = tax_df['Origin'] + tax_df['Destination']
    # tax_df = tax_df.rename(
    #     columns={
    #         'Currency': 'tax_currency',
    #         'OW/RT': 'oneway_return',
    #         'Total': 'tax_value_from_master'})
    # tax_df.drop(['Origin', 'Destination'], axis=1, inplace=True)
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

    ## .........filtered_dF.....

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
        dF = dF.merge(tax_df, on=['OD', 'oneway_return'], how='left')
        dF[['tax_currency',
            'tax_value_from_master']] = dF[['tax_currency',
                                            'tax_value_from_master']].fillna(-999)

        dF['taxes'] = dF.apply(lambda row: convert_tax_currency(row['tax_value_from_master'],
                                                                row['currency'],
                                                                row['tax_currency'],
                                                                exchange_rate), axis=1)
        # Merge YQ df and fares df(that has already been merged with tax df) to
        # compute yq
        # dF = dF.merge(yq_df, on=['oneway_return'], how='left')
        # dF['YQ'] = dF.apply(lambda row: row['amount_yq'] /
        #                     exchange_rate[row['currency']] *
        #                     exchange_rate[row['currency_yq']], axis=1)
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
                    tvl_dates.append(
                        str(entry['TRAVEL_DATES_COMM']) + str(entry['TRAVEL_DATES_EXP']))
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
                    sale_dates.append(
                        str(entry['SALE_DATES_EARLIEST_TKTG']) + str(entry['SALE_DATES_LATEST_TKTG']))
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
            try:
                for entry in dF['cat_12'][i]:

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
                temp_df['surcharge_amt1'] = temp_df.apply(lambda row: convert_tax_currency(row['surcharge_amt1'],
                                                                                           dF.loc[i, 'currency'],
                                                                                           row['currency'],
                                                                                           exchange_rate), axis=1)
                temp_df['surcharge_amt2'] = temp_df.apply(lambda row: convert_tax_currency(row['surcharge_amt2'],
                                                                                           dF.loc[i, 'currency'],
                                                                                           row['currency'],
                                                                                           exchange_rate), axis=1)
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
                    dF.loc[i,'surcharge_amount_' + str(j + 1)] = temp_df.loc[j, 'surcharge_pct'] * 0.01 * dF.loc[i, 'fare'] \
                                                                 + temp_df.loc[j, 'surcharge_amt1']/10**temp_df.loc[j, 'dec1'] \
                                                                 + temp_df.loc[j, 'surcharge_amt2']/10**temp_df.loc[j, 'dec2']

                    dF.loc[i, 'surcharge_date_start_' + str(j + 1)] = from_date
                    dF.loc[i, 'surcharge_date_end_' + str(j + 1)] = to_date
            except KeyError:
                for j in range(4):
                    dF.loc[i, 'surcharge_amount_' + str(j + 1)] = 0
                    dF.loc[i, 'surcharge_date_start_' +
                           str(j + 1)] = "1900-01-01"
                    dF.loc[i, 'surcharge_date_end_' +
                           str(j + 1)] = "2099-12-31"
            except TypeError:
                for j in range(4):
                    dF.loc[i, 'surcharge_amount_' + str(j + 1)] = 0
                    dF.loc[i, 'surcharge_date_start_' +
                           str(j + 1)] = "1900-01-01"
                    dF.loc[i, 'surcharge_date_end_' +
                           str(j + 1)] = "2099-12-31"

            try:
                cat_4 = dF.loc[i, 'cat_4']
                temp = ""
                for element in cat_4:
                    try:
                        temp_st = element['CXR_FLT']['SEGS_CARRIER']
                        if len(temp_st) % 14 != 0:
                            temp_st = temp_st + "    "
                        for i in range(0, len(temp_st), 14):
                            seg = temp_st[i:i + 14]
                            op_cxr = seg[0:2]
                            mrkt_cxr = seg[3:5]
                            range_start = seg[6:10]
                            range_end = seg[10:]
                            if op_cxr == "  ":
                                op_cxr = "--"
                            if mrkt_cxr == "  ":
                                mrkt_cxr = "--"
                            if ((range_start == "****") or (range_end == "****")) and (
                                (op_cxr == "FZ") or (mrkt_cxr == "FZ")):
                                temp = temp + op_cxr + " " + mrkt_cxr + " " + "****, "
                            elif (op_cxr == "FZ") or (mrkt_cxr == "FZ"):
                                temp = temp + op_cxr + " " + mrkt_cxr + " " + range_start + "-" + range_end + ", "
                    except KeyError:
                        pass

                    for i in range(3):
                        key_name_cxr = "CXR" + str(i + 1)
                        key_name_flt = "FLT_NO" + str(i + 1)
                        if element[key_name_cxr] == "FZ":
                            temp = temp + element[key_name_cxr] + " " + str(element[key_name_flt]) + ", "

                dF.loc[i, 'flight_number'] = temp[:-2]
            except KeyError:
                dF.loc[i, 'flight_number'] = "--"
            except TypeError:
                dF.loc[i, 'flight_number'] = "--"

            try:
                cat_2 = dF.loc[i,'cat_2']
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

        dF['Average_surcharge'] = dF[['surcharge_amount_1',
                                      'surcharge_amount_2',
                                      'surcharge_amount_3',
                                      'surcharge_amount_4']].mean(axis=1)

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
        dF['taxes'].fillna(0)
        dF['surcharge_amount_1'].fillna(0)
        dF['surcharge_amount_2'].fillna(0)
        dF['surcharge_amount_3'].fillna(0)
        dF['surcharge_amount_4'].fillna(0)
        dF['Average_surcharge'].fillna(0)
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
    # city_ap_df = pd.DataFrame(city_ap_crsr)
    # dF = dF.merge(city_ap_df, left_on='origin', right_on='Airport_Code', how='left').drop('Airport_Code',
    #                                                                                       axis=1).rename(
    #     columns={"City_Code": "pseudo_origin"})    print "Total number of effective fares = ", len(dF)
    # dF = dF.merge(city_ap_df, left_on='destination', right_on='Airport_Code', how='left').drop('Airport_Code',
    #                                                                                            axis=1).rename(
    #     columns={"City_Code": "pseudo_destination"})
    # dF['pseudo_od'] = dF['pseudo_origin'] + dF['pseudo_destination']
    # dF_do = dF.copy()
    # dF_do['pseudo_od'] = dF_do['pseudo_destination'] + dF_do['pseudo_origin']
    # dF = pd.concat([dF, dF_do])

    dF['display_only'] = 1
    dF.loc[dF['OD'].isin(OD_list), 'display_only'] = 0

    # db.Temp_fzDB_tbl_003.insert_many(dF.to_dict("records"))
    dF['travel_flag_check'] = False
    # print 'Got fares_df:', time.time() - st_

    for date in date_ranges:
        st = time.time()
        fares_df = deepcopy(dF)
        for i in range(len(fares_df)):
            try:
                dates_array = fares_df['Footnotes'][i]['Cat_14_FN']
                for entry in dates_array:
                    # if (entry['TRAVEL_DATES_COMM_1']  <= date['start'] <= entry['TRAVEL_DATES_EXP_1'] <= date['end']) or \
                    #         (date['start'] <= entry['TRAVEL_DATES_COMM_1'] <= entry['TRAVEL_DATES_EXP_1'] <= date['end']) or \
                    #         (date['start'] <= entry['TRAVEL_DATES_COMM_1'] <= date['end'] <= entry['TRAVEL_DATES_EXP_1']) or \
                    #         (date['start'] <= entry['TRAVEL_DATES_COMM_1'] <= date['end'] <= entry['TRAVEL_DATES_EXP_1'])

                    if not (entry['TRAVEL_DATES_COMM_1'] <= entry['TRAVEL_DATES_EXP_1'] < entry['start']) or \
                            (date['end'] < entry['TRAVEL_DATES_COMM_1'] <= entry['TRAVEL_DATES_EXP_1']):
                        fares_df.loc[i, 'travel_flag_check'] = True
                        break

            except KeyError:
                try:
                    dates_array = fares_df['cat_14'][i]
                    for entry in dates_array:
                        # if (entry['TRAVEL_DATES_COMM_1']  <= date['start'] <= entry['TRAVEL_DATES_EXP_1'] <= date['end']) or \
                        #         (date['start'] <= entry['TRAVEL_DATES_COMM_1'] <= entry['TRAVEL_DATES_EXP_1'] <= date['end']) or \
                        #         (date['start'] <= entry['TRAVEL_DATES_COMM_1'] <= date['end'] <= entry['TRAVEL_DATES_EXP_1']) or \
                        #         (date['start'] <= entry['TRAVEL_DATES_COMM_1'] <= date['end'] <= entry['TRAVEL_DATES_EXP_1'])

                        if not (entry['TRAVEL_DATES_COMM_1'] <= entry['TRAVEL_DATES_EXP_1'] < entry['start']) or \
                                (date['end'] < entry['TRAVEL_DATES_COMM_1'] <= entry['TRAVEL_DATES_EXP_1']):
                            fares_df.loc[i, 'travel_flag_check'] = True
                            break

                except KeyError:
                    fares_df.loc[i, 'travel_flag_check'] = True

            except TypeError:
                try:
                    dates_array = fares_df['cat_14'][i]
                    for entry in dates_array:
                        # if (entry['TRAVEL_DATES_COMM_1']  <= date['start'] <= entry['TRAVEL_DATES_EXP_1'] <= date['end']) or \
                        #         (date['start'] <= entry['TRAVEL_DATES_COMM_1'] <= entry['TRAVEL_DATES_EXP_1'] <= date['end']) or \
                        #         (date['start'] <= entry['TRAVEL_DATES_COMM_1'] <= date['end'] <= entry['TRAVEL_DATES_EXP_1']) or \
                        #         (date['start'] <= entry['TRAVEL_DATES_COMM_1'] <= date['end'] <= entry['TRAVEL_DATES_EXP_1'])

                        if not (entry['TRAVEL_DATES_COMM_1'] <= entry['TRAVEL_DATES_EXP_1'] < entry['start']) or \
                                (date['end'] < entry['TRAVEL_DATES_COMM_1'] <= entry['TRAVEL_DATES_EXP_1']):
                            fares_df.loc[i, 'travel_flag_check'] = True
                            break
                except KeyError:
                    fares_df.loc[i, 'travel_flag_check'] = True

        fares_df = fares_df[fares_df['travel_flag_check']]
        print 'Length of fares:', fares_df.shape[0]

    # return dF


if __name__=='__main__':
    today_triggers = db.Temp_fzDB_tbl_001.aggregate([
        {
            '$addFields':
                {
                    'pos': {'$substr': ['$market', 0, 3]},
                    'origin': {'$substr': ['$market', 3, 3]},
                    'destination': {'$substr': ['$market', 6, 3]},
                    'compartment': {'$substr': ['$market', 9, 1]}
                }
        },
        {
            "$group": {
                "_id": {
                    "pos": "$pos",
                    "origin": "$origin",
                    "destination": "$destination",
                    "compartment": "$compartment"
                }
            }

        }, {
            "$project": {
                "pos": "$_id.pos",
                "origin": "$_id.origin",
                "destination": "$_id.destination",
                "compartment": "$_id.compartment"
            }
        }
    ])

    date_ranges = get_dep_date_filters()
    currency_data = get_currency_data()

    st = time.time()

    # tax_crsr = list(db.JUP_DB_Tax_Master.find({}))
    # tax_df = pd.DataFrame(tax_crsr)
    # tax_df['Total'].fillna(0)
    # tax_currencies = list(tax_df['Currency'].unique())
    # tax_df['OD'] = tax_df['Origin'] + tax_df['Destination']
    # tax_df = tax_df.rename(columns={'Currency': 'tax_currency', 'OW/RT': 'oneway_return',
    #                                 'Total': 'tax_value_from_master'})
    # tax_df.drop(['Origin', 'Destination'], axis=1, inplace=True)
    # tax_df['oneway_return'] = tax_df['oneway_return'].apply(str)
    # tax_df = tax_df.rename(columns={"Compartment": "compartment"})

    exchange_rate = {}
    currency_crsr = list(db.JUP_DB_Exchange_Rate.find({}))
    for curr in currency_crsr:
        exchange_rate[curr['code']] = curr['Reference_Rate']

    for doc in today_triggers:
        od = doc['origin']+doc['destination']
        do = doc['destination']+doc['origin']
        start_dates = []
        end_dates = []
        for date in date_ranges:
            start_dates.append(date['start'])
            end_dates.append(date['end'])

        st_ = time.time()
        extreme_start_date = min(start_dates)
        extreme_end_date = max(end_dates)
        fares_df = get_host_fares_df(pos=doc['pos'],
                                     extreme_start_date=extreme_start_date,
                                     extreme_end_date=extreme_end_date,
                                     od_list=[od],
                                     do_list=[do],
                                     compartment=doc['compartment'],
                                     currency_data=currency_data,
                                     # tax_df=tax_df,
                                     exchange_rate=exchange_rate,
                                     )
        fares_df['travel_flag_check'] = False
        print 'Got fares_df:', time.time() - st_

        for date in date_ranges:
            st = time.time()
            for i in range(len(fares_df)):
                try:
                    dates_array = fares_df['Footnotes'][i]['Cat_14_FN']
                    for entry in dates_array:
                        # if (entry['TRAVEL_DATES_COMM_1']  <= date['start'] <= entry['TRAVEL_DATES_EXP_1'] <= date['end']) or \
                        #         (date['start'] <= entry['TRAVEL_DATES_COMM_1'] <= entry['TRAVEL_DATES_EXP_1'] <= date['end']) or \
                        #         (date['start'] <= entry['TRAVEL_DATES_COMM_1'] <= date['end'] <= entry['TRAVEL_DATES_EXP_1']) or \
                        #         (date['start'] <= entry['TRAVEL_DATES_COMM_1'] <= date['end'] <= entry['TRAVEL_DATES_EXP_1'])

                        if not (entry['TRAVEL_DATES_COMM_1'] <= entry['TRAVEL_DATES_EXP_1'] < entry['start']) or \
                                (date['end'] < entry['TRAVEL_DATES_COMM_1'] <= entry['TRAVEL_DATES_EXP_1']):
                            fares_df.loc[i, 'travel_flag_check'] = True
                            break

                except KeyError:
                    try:
                        dates_array = fares_df['cat_14'][i]
                        for entry in dates_array:
                            # if (entry['TRAVEL_DATES_COMM_1']  <= date['start'] <= entry['TRAVEL_DATES_EXP_1'] <= date['end']) or \
                            #         (date['start'] <= entry['TRAVEL_DATES_COMM_1'] <= entry['TRAVEL_DATES_EXP_1'] <= date['end']) or \
                            #         (date['start'] <= entry['TRAVEL_DATES_COMM_1'] <= date['end'] <= entry['TRAVEL_DATES_EXP_1']) or \
                            #         (date['start'] <= entry['TRAVEL_DATES_COMM_1'] <= date['end'] <= entry['TRAVEL_DATES_EXP_1'])

                            if not (entry['TRAVEL_DATES_COMM_1'] <= entry['TRAVEL_DATES_EXP_1'] < entry['start']) or \
                                    (date['end'] < entry['TRAVEL_DATES_COMM_1'] <= entry['TRAVEL_DATES_EXP_1']):
                                fares_df.loc[i, 'travel_flag_check'] = True
                                break

                    except KeyError:
                        fares_df.loc[i, 'travel_flag_check'] = True

                except TypeError:
                    try:
                        dates_array = fares_df['cat_14'][i]
                        for entry in dates_array:
                            # if (entry['TRAVEL_DATES_COMM_1']  <= date['start'] <= entry['TRAVEL_DATES_EXP_1'] <= date['end']) or \
                            #         (date['start'] <= entry['TRAVEL_DATES_COMM_1'] <= entry['TRAVEL_DATES_EXP_1'] <= date['end']) or \
                            #         (date['start'] <= entry['TRAVEL_DATES_COMM_1'] <= date['end'] <= entry['TRAVEL_DATES_EXP_1']) or \
                            #         (date['start'] <= entry['TRAVEL_DATES_COMM_1'] <= date['end'] <= entry['TRAVEL_DATES_EXP_1'])

                            if not (entry['TRAVEL_DATES_COMM_1'] <= entry['TRAVEL_DATES_EXP_1'] < entry['start']) or \
                                    (date['end'] < entry['TRAVEL_DATES_COMM_1'] <= entry['TRAVEL_DATES_EXP_1']):
                                fares_df.loc[i, 'travel_flag_check'] = True
                                break
                    except KeyError:
                        fares_df.loc[i, 'travel_flag_check'] = True

            fares_df = fares_df[fares_df['travel_flag_check']]
            print 'Length of fares:', fares_df.shape[0]
            print 'Got fares for ', date['start'], ' - ', date['end'], ' in ', time.time() - st
            # print 'Length of fares df = ', data.shape[0]
        break