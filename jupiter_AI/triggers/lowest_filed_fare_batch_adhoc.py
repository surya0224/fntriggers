"""
Author: Abhinav Garg
Created with <3
Date Created: 2018-05-12
File Name: lowest_filed_fare_batch.py

Functionality: Compute the lowest filed fare and highest filed fare for each market for each carrier
               for a transaction date in Manual Trigger Module.

"""

import datetime
from jupiter_AI import JUPITER_DB, mongo_client, SYSTEM_DATE, Host_Airline_Code,JUPITER_LOGGER
from jupiter_AI.logutils import measure
import pandas as pd
import numpy as np
from jupiter_AI.triggers.common import pos_level_currency_data as get_currency_data
import time
import re
client_b = mongo_client()
db_b = client_b[JUPITER_DB]
exchange_rate = {}
currency_crsr = list(db_b.JUP_DB_Exchange_Rate.find({}))
for curr in currency_crsr:
    exchange_rate[curr['code']] = curr['Reference_Rate']
client_b.close()

@measure(JUPITER_LOGGER)
def convert_tax_currency(tax_value, host_curr, tax_currency):
    if tax_currency == -999:
        return 0
    else:
        tax_currency = tax_currency.upper()
        host_curr = host_curr.upper()
        tax_value = tax_value / exchange_rate[host_curr] * exchange_rate[tax_currency]
        return tax_value


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
def get_fares_df(pos, origin, destination, compartment, extreme_start_date, extreme_end_date, db, comp=[Host_Airline_Code]):
    currency_data = get_currency_data()
    currency = []
    try:
        if currency_data[pos]['web']:
            currency.append(currency_data[pos]['web'])
        if currency_data[pos]['gds']:
            currency.append(currency_data[pos]['gds'])
    except KeyError:
        currency = []

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
    for o in pseudo_origins:
        for d in pseudo_destinations:
            temp_od = d + o
            temp_od_1 = o + d
            temp_od = temp_od.replace("UAE", "DXB")
            temp_od_1 = temp_od_1.replace("UAE", "DXB")
            ods.append(temp_od)
            ods.append(temp_od_1)
            # tax_query.append({
            #     "$and": [{
            #         "Origin": temp_od[0:3],
            #         "Destination": temp_od[3:]
            #     }]
            # })
            # tax_query.append({
            #     "$and": [{
            #         "Origin": temp_od_1[0:3],
            #         "Destination": temp_od_1[3:]
            #     }]
            # })

    cols = [
        "effective_from",
        "effective_to",
        "dep_date_from",
        "dep_date_to",
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
        "currency",
        "fare_rule",
        "RBD_type",
        "gfs",
        "last_update_date",
        "last_update_time",
        "competitor_farebasis",
        "travel_date_to",
        "travel_date_from"
    ]
    print "ods: ", ods
    dF = pd.DataFrame(columns=cols)
    extreme_start_date_n = "0" + extreme_start_date[2:4] + extreme_start_date[5:7] + extreme_start_date[8:10]
    extreme_end_date_n = "0" + extreme_end_date[2:4] + extreme_end_date[5:7] + extreme_end_date[8:10]
    SYSTEM_DATE_MOD = "0" + SYSTEM_DATE[2:4] + SYSTEM_DATE[5:7] + SYSTEM_DATE[8:10]

    print "Aggregating on ATPCO Fares Rules . . . . "
    query = {
        "$and": [
            {
                "OD": {
                    "$in": ods
                }
            },
            {
                "currency": {
                    "$in": currency
                }
            },
            {
                "compartment": compartment
            },
            {
                "channel": 'gds'
            },
            {
                "fare_include": True
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
                            "$gte": SYSTEM_DATE
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
                                    "$gte": extreme_start_date_n
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
                                    "$gte": extreme_start_date_n
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
                                    "$gte": extreme_start_date_n
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
                                    "$gte": extreme_start_date_n
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
                                    "$gte": extreme_start_date_n
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
                                    "$gte": extreme_start_date_n
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
                                    "$gte": extreme_start_date_n
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
                                    "$gte": extreme_start_date_n
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
                                    "$gte": extreme_start_date_n
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
                                    "$gte": SYSTEM_DATE_MOD
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
                                    "$gte": SYSTEM_DATE_MOD
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
                                    "$gte": SYSTEM_DATE_MOD
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
                                    "$gte": SYSTEM_DATE_MOD
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
                                    "$gte": SYSTEM_DATE_MOD
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
                                    "$gte": SYSTEM_DATE_MOD
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
                                    "$gte": SYSTEM_DATE_MOD
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
                                    "$gte": SYSTEM_DATE_MOD
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
                                    "$gte": SYSTEM_DATE_MOD
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

    print "Fares Data length = ", len(fares_data)

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
    #     tax_df = pd.DataFrame(columns=['Total',
    #                                    'Origin',
    #                                    'Destination',
    #                                    'Currency',
    #                                    'OW/RT',
    #                                    'Compartment'])
    # tax_df['Total'].fillna(0)
    # tax_df['OD'] = tax_df['Origin'] + tax_df['Destination']
    # tax_df = tax_df.rename(
    #     columns={
    #         'Currency': 'tax_currency',
    #         'OW/RT': 'oneway_return',
    #         'Total': 'tax_value_from_master'})
    # tax_df.drop(['Origin', 'Destination', 'Compartment'], axis=1, inplace=True)
    # tax_df['oneway_return'] = tax_df['oneway_return'].apply(str)
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

        # dF = dF.merge(tax_df, on=['OD', 'oneway_return'], how='left')
        # dF[['tax_currency',
        #     'tax_value_from_master']] = dF[['tax_currency',
        #                                     'tax_value_from_master']].fillna(-999)
        #
        # dF['taxes'] = dF.apply(lambda row: convert_tax_currency(row['tax_value_from_master'],
        #                                                         row['currency'],
        #                                                         row['tax_currency']), axis=1)
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
            surchrg_amts = []
            has_category = False
            tvl_dates = []
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

            # try:
            #     t = dF['Gen Rules'][i]['cat_12'][0]
            #     cat_12_gen = dF['Gen Rules'][i]['cat_12']
            # except:
            #     cat_12_gen = []
            #
            # try:
            #     t = dF['cat_12'][i][0]
            #     cat_12 = dF['cat_12'][i]
            # except:
            #     cat_12 = []

            cat_12_gen = []
            """changed concept of the cat_12"""
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
        dF['surcharge_amount_1'].fillna(0, inplace=True)
        dF['surcharge_amount_2'].fillna(0, inplace=True)
        dF['surcharge_amount_3'].fillna(0, inplace=True)
        dF['surcharge_amount_4'].fillna(0, inplace=True)
        dF['Average_surcharge'].fillna(0, inplace=True)
        dF['total_fare_1'] = dF['fare'] + dF['YQ'] + dF['YR'] + dF['surcharge_amount_1']
        dF['total_fare_2'] = dF['fare'] + dF['YQ'] + dF['YR'] + dF['surcharge_amount_2']
        dF['total_fare_3'] = dF['fare'] + dF['YQ'] + dF['YR'] + dF['surcharge_amount_3']
        dF['total_fare_4'] = dF['fare'] + dF['YQ'] + dF['YR'] + dF['surcharge_amount_4']
        dF['total_fare'] = dF['fare'] + dF['YQ'] + dF['YR'] + dF['Average_surcharge']
        dF = dF[cols]
        dF['effective_to'] = dF['effective_to'].fillna("2099-12-31")
        dF = dF.sort_values("fare")
    print "Total number of effective fares = ", len(dF)
    return dF



@measure(JUPITER_LOGGER)
def get_lowest_filed_fare_dF(
        fares_df,
        pos,
        db,
        date_list=None):
    currency_data = get_currency_data(db=db)
    # print "lowest comp: ", comp

    lowest_fares_dF = pd.DataFrame(
        columns=[
            'carrier',
            'lowest_fare_base',
            'lowest_fare_total',
            'lowest_fare_YQ',
            'lowest_fare_YR',
            'lowest_fare_fb',
            'lowest_fare_surcharge',
            'highest_fare_base',
            'highest_fare_total',
            'highest_fare_fb',
            'highest_fare_YQ',
            'highest_fare_YR',
            'highest_fare_surcharge',
            'currency'
        ]
    )

    lowest_fares_d_ = fares_df
    print "len of lowest fares ", len(lowest_fares_d_)
    # print lowest_fares_d_
    curr_list = lowest_fares_d_['currency'].unique()
    if "YR" not in lowest_fares_d_.columns:
        lowest_fares_d_['YR'] = 0
    final_data = pd.DataFrame(columns=[u'carrier', u'currency', u'lowest_fare_total', u'lowest_fare_YQ',
                                       u'lowest_fare_YR', u'lowest_fare_surcharge', u'lowest_fare_base',
                                       u'lowest_fare_fb', u'lowest_fare_total_rt', u'lowest_fare_YQ_rt',
                                       u'lowest_fare_YR_rt', u'lowest_fare_surcharge_rt',
                                       u'lowest_fare_base_rt', u'lowest_fare_fb_rt', u'highest_fare_total',
                                       u'highest_fare_YQ', u'highest_fare_YR', u'highest_fare_surcharge',
                                       u'highest_fare_base', u'highest_fare_fb', u'highest_fare_total_rt',
                                       u'highest_fare_YQ_rt', u'highest_fare_YR_rt',
                                       u'highest_fare_surcharge_rt', u'highest_fare_base_rt',
                                       u'highest_fare_fb_rt', u'date_start', u'date_end', u'currency_gds',
                                       u'highest_fare_YQ_gds', u'highest_fare_YR_gds',
                                       u'highest_fare_base_gds', u'highest_fare_fb_gds',
                                       u'highest_fare_surcharge_gds', u'highest_fare_total_gds',
                                       u'lowest_fare_YQ_gds', u'lowest_fare_YR_gds', u'lowest_fare_base_gds',
                                       u'lowest_fare_fb_gds', u'lowest_fare_surcharge_gds',
                                       u'lowest_fare_total_gds', u'highest_fare_YQ_rt_gds',
                                       u'highest_fare_YR_rt_gds', u'highest_fare_base_rt_gds',
                                       u'highest_fare_fb_rt_gds', u'highest_fare_surcharge_rt_gds',
                                       u'highest_fare_total_rt_gds', u'lowest_fare_YQ_rt_gds',
                                       u'lowest_fare_YR_rt_gds', u'lowest_fare_base_rt_gds',
                                       u'lowest_fare_fb_rt_gds', u'lowest_fare_surcharge_rt_gds',
                                       u'lowest_fare_total_rt_gds'])

    print 'Length of date list:', len(date_list)

    final_data = pd.DataFrame()
    print lowest_fares_d_['travel_date_from'].unique()
    print lowest_fares_d_['travel_date_to'].unique()
    for date_ in date_list:
        lowest_fares_d = lowest_fares_d_[
            (lowest_fares_d_['travel_date_from'] <= date_['end']) & (
                    lowest_fares_d_['travel_date_to'] >= date_['start'])]
        lowest_fares_d['Average_fare'] = lowest_fares_d['fare'] + lowest_fares_d['YQ'] + \
                                         lowest_fares_d['YR'] + lowest_fares_d['Average_surcharge']

        min_df = lowest_fares_d.loc[
            lowest_fares_d.groupby(by=['oneway_return', 'currency', 'carrier'])['Average_fare'].idxmin()]
        max_df = lowest_fares_d.loc[
            lowest_fares_d.groupby(by=['oneway_return', 'currency', 'carrier'])['Average_fare'].idxmax()]

        min_df['lowest_fare_total'] = min_df['Average_fare']
        min_df['lowest_fare_YQ'] = min_df['YQ']
        min_df['lowest_fare_YR'] = min_df['YR']
        min_df['lowest_fare_surcharge'] = min_df['Average_surcharge']
        min_df['lowest_fare_base'] = min_df['fare']
        min_df['lowest_fare_fb'] = min_df['fare_basis']

        max_df['highest_fare_total'] = max_df['Average_fare']
        max_df['highest_fare_YQ'] = max_df['YQ']
        max_df['highest_fare_YR'] = max_df['YR']
        max_df['highest_fare_surcharge'] = max_df['Average_surcharge']
        max_df['highest_fare_base'] = max_df['fare']
        max_df['highest_fare_fb'] = max_df['fare_basis']

        min_df_rt = min_df[min_df['oneway_return'] == '2']
        max_df_rt = max_df[max_df['oneway_return'] == '2']
        min_df = min_df[min_df['oneway_return'] == '1']
        max_df = max_df[max_df['oneway_return'] == '1']

        min_col_list = ['carrier', 'currency', 'lowest_fare_total', 'lowest_fare_YQ',
                        'lowest_fare_YR', 'lowest_fare_surcharge', 'lowest_fare_base', 'lowest_fare_fb']
        max_col_list = ['carrier', 'currency', 'highest_fare_total', 'highest_fare_YQ',
                        'highest_fare_YR', 'highest_fare_surcharge', 'highest_fare_base', 'highest_fare_fb']
        min_df = min_df.loc[:, min_col_list]
        max_df = max_df.loc[:, max_col_list]
        min_df_rt = min_df_rt.loc[:, min_col_list]
        max_df_rt = max_df_rt.loc[:, max_col_list]

        min_df = pd.merge(min_df, min_df_rt, on=['carrier', 'currency'], how='outer', suffixes=("", "_rt"))
        max_df = pd.merge(max_df, max_df_rt, on=['carrier', 'currency'], how='outer', suffixes=("", "_rt"))

        temp_df = pd.merge(min_df, max_df, on=['carrier', 'currency'], how='outer')
        temp_df['date_start'] = date_['start']
        temp_df['date_end'] = date_['end']

        final_data = pd.concat([final_data, temp_df], ignore_index=True)

        # for competitor in comp:
        #     for curr in curr_list:
        #         result = {"carrier": "NA",
        #                   "currency": "NA",
        #                   "lowest_fare_total": "NA",
        #                   "lowest_fare_YQ": "NA",
        #                   "lowest_fare_YR": "NA",
        #                   "lowest_fare_surcharge": "NA",
        #                   "lowest_fare_base": "NA",
        #                   "lowest_fare_fb": "NA",
        #                   "highest_fare_total": "NA",
        #                   "highest_fare_YQ": "NA",
        #                   "highest_fare_YR": "NA",
        #                   "highest_fare_surcharge": "NA",
        #                   "highest_fare_base": "NA",
        #                   "highest_fare_fb": "NA",
        #                   "lowest_fare_total_rt": "NA",
        #                   "lowest_fare_YQ_rt": "NA",
        #                   "lowest_fare_YR_rt": "NA",
        #                   "lowest_fare_surcharge_rt": "NA",
        #                   "lowest_fare_base_rt": "NA",
        #                   "lowest_fare_fb_rt": "NA",
        #                   "highest_fare_total_rt": "NA",
        #                   "highest_fare_YQ_rt": "NA",
        #                   "highest_fare_YR_rt": "NA",
        #                   "highest_fare_surcharge_rt": "NA",
        #                   "highest_fare_base_rt": "NA",
        #                   "highest_fare_fb_rt": "NA",
        #                   }
        #         temp_df = lowest_fares_d[(lowest_fares_d['carrier'] == competitor) &
        #                                  (lowest_fares_d['currency'] == curr) &
        #                                  (lowest_fares_d['oneway_return'] == "1")]
        #         temp_df_2 = lowest_fares_d[(lowest_fares_d['carrier'] == competitor) &
        #                                    (lowest_fares_d['currency'] == curr) &
        #                                    (lowest_fares_d['oneway_return'] == "2")]
        #
        #         if len(temp_df) > 0:
        #             temp_df_min = temp_df[temp_df['min'] == min(temp_df['min'])]
        #             min_idx = temp_df_min.index[0]
        #
        #             temp_df_max = temp_df[temp_df['max'] == max(temp_df['max'])]
        #             max_idx = temp_df_max.index[0]
        #
        #             result['carrier'] = competitor
        #             result['currency'] = curr
        #             result['lowest_fare_total'] = temp_df_min.loc[min_idx,'min']
        #
        #             result['lowest_fare_YQ'] = temp_df_min.loc[min_idx,
        #                                                               'YQ']
        #             result['lowest_fare_YR'] = temp_df_min.loc[min_idx,
        #                                                               'YR']
        #             result['lowest_fare_surcharge'] = temp_df_min.loc[min_idx,
        #                                                                      'min'] - temp_df_min.loc[min_idx,
        #                                                                                               'YQ'] - temp_df_min.loc[min_idx,
        #                                                                                                                       'YR'] - temp_df_min.loc[min_idx,
        #                                                                                                                                               'fare']
        #             result['lowest_fare_base'] = temp_df_min.loc[min_idx,
        #                                                                 'fare']
        #             result['lowest_fare_fb'] = temp_df_min.loc[min_idx,
        #                                                               'fare_basis']
        #             result['highest_fare_total'] = temp_df_max.loc[max_idx,
        #                                                                   'max']
        #
        #             result['highest_fare_YQ'] = temp_df_max.loc[max_idx,
        #                                                                'YQ']
        #             result['highest_fare_YR'] = temp_df_max.loc[max_idx,
        #                                                                'YR']
        #             result['highest_fare_surcharge'] = temp_df_max.loc[max_idx,
        #                                                                       'max'] - temp_df_max.loc[max_idx,
        #                                                                                                'YQ'] - temp_df_max.loc[max_idx,
        #                                                                                                                        'YR'] - temp_df_max.loc[max_idx,
        #                                                                                                                                                'fare']
        #             result['highest_fare_base'] = temp_df_max.loc[max_idx,
        #                                                                  'fare']
        #             result['highest_fare_fb'] = temp_df_max.loc[max_idx,
        #                                                                'fare_basis']
        #
        #         if len(temp_df_2) > 0:
        #             temp_df_min_2 = temp_df_2[temp_df_2['min'] == min(temp_df_2['min'])]
        #             min_idx_2 = temp_df_min_2.index[0]
        #             temp_df_max_2 = temp_df_2[temp_df_2['max'] == max(temp_df_2['max'])]
        #             max_idx_2 = temp_df_max_2.index[0]
        #             result['carrier'] = competitor
        #             result['currency'] = curr
        #             result['lowest_fare_total_rt'] = temp_df_min_2.loc[min_idx_2, 'min']
        #             result['lowest_fare_YQ_rt'] = temp_df_min_2.loc[min_idx_2,
        #                                                        'YQ']
        #             result['lowest_fare_YR_rt'] = temp_df_min_2.loc[min_idx_2,
        #                                                        'YR']
        #             result['lowest_fare_surcharge_rt'] = temp_df_min_2.loc[min_idx_2,
        #                                                               'min'] - temp_df_min_2.loc[min_idx_2,
        #                                                                                        'YQ'] - temp_df_min_2.loc[
        #                                                   min_idx_2,
        #                                                   'YR'] - temp_df_min_2.loc[min_idx_2,
        #                                                                           'fare']
        #             result['lowest_fare_base_rt'] = temp_df_min_2.loc[min_idx_2,
        #                                                          'fare']
        #             result['lowest_fare_fb_rt'] = temp_df_min_2.loc[min_idx_2,
        #                                                        'fare_basis']
        #             result['highest_fare_total_rt'] = temp_df_max_2.loc[max_idx_2,
        #                                                            'max']
        #             result['highest_fare_YQ_rt'] = temp_df_max_2.loc[max_idx_2,
        #                                                         'YQ']
        #             result['highest_fare_YR_rt'] = temp_df_max_2.loc[max_idx_2,
        #                                                         'YR']
        #             result['highest_fare_surcharge_rt'] = temp_df_max_2.loc[max_idx_2,
        #                                                                'max'] - temp_df_max_2.loc[max_idx_2,
        #                                                                                         'YQ'] - temp_df_max_2.loc[
        #                                                    max_idx_2,
        #                                                    'YR'] - temp_df_max_2.loc[max_idx_2,
        #                                                                            'fare']
        #             result['highest_fare_base_rt'] = temp_df_max_2.loc[max_idx_2,
        #                                                           'fare']
        #             result['highest_fare_fb_rt'] = temp_df_max_2.loc[max_idx_2,
        #                                                         'fare_basis']
        #         my_list.append(result)
        # temp = pd.DataFrame(my_list)
        # temp['date_start'] = date_['start']
        # temp['date_end'] = date_['end']
        # final_data = pd.concat([final_data, temp], ignore_index=True)
        # count += 1
    if len(final_data) > 0:
        # lowest_fares_dF = pd.DataFrame(lowest_fares_d)
        gds_cols = ["currency_gds",
                    "highest_fare_YQ_gds",
                    "highest_fare_YR_gds",
                    "highest_fare_base_gds",
                    "highest_fare_fb_gds",
                    "highest_fare_surcharge_gds",
                    "highest_fare_total_gds",
                    "lowest_fare_YQ_gds",
                    "lowest_fare_YR_gds",
                    "lowest_fare_base_gds",
                    "lowest_fare_fb_gds",
                    "lowest_fare_surcharge_gds",
                    "lowest_fare_total_gds",
                    "highest_fare_YQ_rt_gds",
                    "highest_fare_YR_rt_gds",
                    "highest_fare_base_rt_gds",
                    "highest_fare_fb_rt_gds",
                    "highest_fare_surcharge_rt_gds",
                    "highest_fare_total_rt_gds",
                    "lowest_fare_YQ_rt_gds",
                    "lowest_fare_YR_rt_gds",
                    "lowest_fare_base_rt_gds",
                    "lowest_fare_fb_rt_gds",
                    "lowest_fare_surcharge_rt_gds",
                    "lowest_fare_total_rt_gds",
                    "carrier",
                    "date_start",
                    "date_end"]
        gds_df = pd.DataFrame(columns=gds_cols)
        filed_df = final_data[final_data['currency'] == currency_data[pos]['web']]
        gds_df['carrier'] = filed_df['carrier']
        gds_df['date_start'] = filed_df['date_start']
        gds_df['date_end'] = filed_df['date_end']
        if len(curr_list) > 1:
            gds_df = final_data[final_data['currency'] == currency_data[pos]['gds']]
        if len(filed_df) > 0:
            final_data = filed_df.merge(gds_df, on=['carrier', 'date_start', 'date_end'], how='left',
                                        suffixes=("", "_gds"))
        else:
            gds_df = pd.DataFrame(columns=["currency",
                                           "highest_fare_YQ",
                                           "highest_fare_YR",
                                           "highest_fare_base",
                                           "highest_fare_fb",
                                           "highest_fare_surcharge",
                                           "highest_fare_total",
                                           "lowest_fare_YQ",
                                           "lowest_fare_YR",
                                           "lowest_fare_base",
                                           "lowest_fare_fb",
                                           "lowest_fare_surcharge",
                                           "lowest_fare_total",
                                           "highest_fare_YQ_rt",
                                           "highest_fare_YR_rt",
                                           "highest_fare_base_rt",
                                           "highest_fare_fb_rt",
                                           "highest_fare_surcharge_rt",
                                           "highest_fare_total_rt",
                                           "lowest_fare_YQ_rt",
                                           "lowest_fare_YR_rt",
                                           "lowest_fare_base_rt",
                                           "lowest_fare_fb_rt",
                                           "lowest_fare_surcharge_rt",
                                           "lowest_fare_total_rt",
                                           "carrier",
                                           "date_start",
                                           "date_end"])
            final_data = final_data.merge(gds_df, on=['carrier', 'date_start', 'date_end'], how='left',
                                          suffixes=("_gds", ""))

    return final_data


@measure(JUPITER_LOGGER)
def update_market_adhoc(pos, origin, destination, compartment, db):
    db = db[JUPITER_DB]
    mt_cur = list(db.JUP_DB_Manual_Triggers_Module.aggregate([
        {
            '$match': {'pos.City': pos, 'origin.City': origin, 'destination.City': destination,
                       'compartment.compartment': compartment, 'dep_date': {'$gte': SYSTEM_DATE}}
        },
        {
            '$group': {
                '_id': None,
                'dep_dates': {'$addToSet': '$dep_date'},
                'extreme_start_date': {'$min': '$dep_date'},
                'extreme_end_date': {'$max': '$dep_date'}
            }
        },
        {
            '$project': {
                '_id': 0,
                'extreme_start_date': 1,
                'extreme_end_date': 1,
                'dep_dates': 1
            }
        }
    ]))

    print 'Got dates'

    st = time.time()

    if len(mt_cur) > 0:
        fares_df = get_fares_df(
            pos=pos,
            origin=origin,
            destination=destination,
            compartment=compartment,
            extreme_start_date=mt_cur[0]['extreme_start_date'],
            extreme_end_date=mt_cur[0]['extreme_end_date'],
            db=db
        )
        print 'Got fares in ', time.time() - st

        if len(fares_df) > 0:
            date_list = map(lambda d: {'start': d, 'end': d}, mt_cur[0]['dep_dates'])
            df = get_lowest_filed_fare_dF(fares_df=fares_df, pos=pos, db=db, date_list=date_list)
            print '>>>>>'
            print len(df)

            lff_df = df[['carrier',
                         'currency',
                         'lowest_fare_YQ',
                         'lowest_fare_YQ_rt',
                         'lowest_fare_YR',
                         'lowest_fare_YR_rt',
                         'lowest_fare_base',
                         'lowest_fare_base_rt',
                         'lowest_fare_fb',
                         'lowest_fare_fb_rt',
                         'lowest_fare_surcharge',
                         'lowest_fare_surcharge_rt',
                         'lowest_fare_total',
                         'lowest_fare_total_rt',
                         'date_start',
                         'date_end',
                         'currency_gds',
                         'lowest_fare_YQ_gds',
                         'lowest_fare_YR_gds',
                         'lowest_fare_base_gds',
                         'lowest_fare_fb_gds',
                         'lowest_fare_surcharge_gds',
                         'lowest_fare_total_gds',
                         'lowest_fare_YQ_rt_gds',
                         'lowest_fare_YR_rt_gds',
                         'lowest_fare_base_rt_gds',
                         'lowest_fare_fb_rt_gds',
                         'lowest_fare_surcharge_rt_gds',
                         'lowest_fare_total_rt_gds']]
            hff_df = df[['carrier',
                         'currency',
                         'highest_fare_YQ',
                         'highest_fare_YQ_rt',
                         'highest_fare_YR',
                         'highest_fare_YR_rt',
                         'highest_fare_base',
                         'highest_fare_base_rt',
                         'highest_fare_fb',
                         'highest_fare_fb_rt',
                         'highest_fare_surcharge',
                         'highest_fare_surcharge_rt',
                         'highest_fare_total',
                         'highest_fare_total_rt',
                         'date_start',
                         'date_end',
                         'currency_gds',
                         'highest_fare_YQ_gds',
                         'highest_fare_YR_gds',
                         'highest_fare_base_gds',
                         'highest_fare_fb_gds',
                         'highest_fare_surcharge_gds',
                         'highest_fare_total_gds',
                         'highest_fare_YQ_rt_gds',
                         'highest_fare_YR_rt_gds',
                         'highest_fare_base_rt_gds',
                         'highest_fare_fb_rt_gds',
                         'highest_fare_surcharge_rt_gds',
                         'highest_fare_total_rt_gds',
                         ]]
            print mt_cur[0]['dep_dates']
            for i in mt_cur[0]['dep_dates']:
                # print lff_df['date_start'].unique()
                lff_df_ = lff_df[lff_df['date_start'] == i]
                hff_df_ = hff_df[hff_df['date_start'] == i]
                # print len(lff_df_), len(hff_df_)
                if len(lff_df_) > 0:

                    db.JUP_DB_Manual_Triggers_Module.update({'pos.City': pos,
                                                             'origin.City': origin,
                                                             'destination.City': destination,
                                                             'compartment.compartment': compartment,
                                                             'dep_date': i},
                                                            {
                                                                '$set': {
                                                                    'lowest_filed_fare': lff_df_.to_dict('records'),
                                                                    'highest_filed_fare': hff_df_.to_dict('records'),
                                                                }
                                                            },
                                                            multi=True
                                                            )
                    db.JUP_DB_Manual_Triggers_Module_Summary.update({'pos.City': pos,
                                                                     'origin.City': origin,
                                                                     'destination.City': destination,
                                                                     'compartment': compartment,
                                                                     'dep_date': i},
                                                                    {
                                                                        '$set': {
                                                                            'lowest_filed_fare': lff_df_.to_dict('records'),
                                                                            'highest_filed_fare': hff_df_.to_dict('records'),
                                                                        }
                                                                    },
                                                                    multi=True
                                                                    )
    print 'Updated {}{}{}{}'.format(pos, origin, destination, compartment)


if __name__ == '__main__':
    st = time.time()
    # markets = db.JUP_DB_Manual_Triggers_Module.distinct('market_combined', {'trx_date': SYSTEM_DATE})
    # lff = []
    # db.JUP_DB_Manual_Triggers_Module_Summary.update({},
    #                                                 {'$unset': {'lowest_filed_fare': 1, 'highest_filed_fare': 1}},
    #                                                 multi=True)
    # db.JUP_DB_Manual_Triggers_Module.update({},
    #                                         {'$unset': {'lowest_filed_fare': 1, 'highest_filed_fare': 1}},
    #                                         multi=True)
    # print 'Removed'
    from jupiter_AI.batch.atpco_automation.Automation_tasks import run_lff_adhoc
    from celery import group
    # update_market_adhoc('ATL','ATL','SIN','C')
    from jupiter_AI.batch.Run_triggers import segregate_markets
    # sig_markets, sub_sig_markets, non_sig_markets = segregate_markets()
    # markets = list(set(sig_markets + sub_sig_markets + non_sig_markets))
    # for count, i in enumerate(markets):
    #     update_market_adhoc(pos=i[0:3], origin=i[3:6], destination=i[6:9], compartment=i[9:10])
    #     print 'done:', count, i
    # print len(sig_markets), len(sub_sig_markets), len(non_sig_markets)
    from jupiter_AI import JUPITER_DB, mongo_client, SYSTEM_DATE, Host_Airline_Code,JUPITER_LOGGER
    db = mongo_client()[JUPITER_DB]
    markets = db.JUP_DB_Market_Significance.distinct('market' ,{ "significance.name" : "l.kumarasamy", "rank" : {"$lte" : 100}})
    lff = []
    for i in markets:
        lff.append(run_lff_adhoc.s(pos=i[0:3], origin=i[3:6], destination=i[6:9], compartment=i[9:10]))
    group2 = group(lff)
    res2 = group2()
    res2.get()
