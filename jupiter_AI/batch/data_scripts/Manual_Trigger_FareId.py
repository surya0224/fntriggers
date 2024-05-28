# coding: utf-8

# In[112]:


from copy import deepcopy
import json
import pymongo
import collections
import pandas as pd
import numpy as np
import datetime
import time
from pymongo.errors import BulkWriteError
from convert_currency import convert_currency
from jupiter_AI import JUPITER_LOGGER
from jupiter_AI.logutils import measure


# In[113]:


# Connect mongodb db business layer
# try:
#     fzDBConn=pymongo.MongoClient(var.mongo_client_url)[var.database]
#     fzDBConn.authenticate('pdssETLUser', 'pdssETL@123', source='admin')
#     #prod_conn=pymongo.MongoClient('172.29.4.5:27022')['fzDB_prod']
#     #prod_conn.authenticate('pdssETLUser', 'pdssETL@123', source='admin')
#     print('connected')
#
#
# except Exception as e:
#     #sys.stderr.write("Could not connect to MongoDB: %s" % e)
#     print("Could not connect to MongoDB: %s" % e)
from jupiter_AI import client, JUPITER_DB, Host_Airline_Code, SYSTEM_DATE, Host_Airline_Hub

#fzDBConn = client[JUPITER_DB]

# In[92]:

#dateformat = '%Y-%m-%d'
#SYS_SNAP_DATE = datetime.datetime.strftime(datetime.datetime.now(), dateformat)

SYS_SNAP_DATE = SYSTEM_DATE
defaultCurrency = "AED"


# currency_crsr = list(fzDBConn.JUP_DB_Exchange_Rate.find(
#         {"code": {"$in": currency + [yr_curr] + tax_currencies + yq_currencies}}))
# for curr in currency_crsr:
#     exchange_rate[curr['code']] = curr['Reference_Rate']


@measure(JUPITER_LOGGER)
def fareId(pos, origin, destination, compartment, farebasis, snap_date, dep_date, currency_data, db):
    currency = []

    ### get the currency based on MT pos
    if pos in currency_data:
        if currency_data[pos]['web']:
            currency.append(currency_data[pos]['web'])
    else:
        currency.append(defaultCurrency)
    ods = []
    temp_od = origin + destination
    temp_do = destination + origin
    ods.append(temp_od)
    ods.append(temp_do)
    tax_query = []
    tax_query.append({
        "$and": [{
            "Origin": temp_od[0:3],
            "Destination": temp_od[3:]
        }]
    })
    tax_query.append({
        "$and": [{
            "Origin": temp_do[0:3],
            "Destination": temp_do[3:]
        }]
    })

    cols = [
        "effective_from",
        "effective_to",
        "dep_date_from",
        "dep_date_to",
        "sale_date_from",
        "sale_date_to",
        "last_ticketed_date",
        "fare_basis",
        "fare_brand",
        "RBD",
        "rtg",
        "tariff_code",
        "fare_rule",
        "fare_include",
        "private_fare",
        "footnote",
        "batch",
        "Network",
        "Cluster",
        "region",
        "country",
        "pos",
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
        "Rule_id",
        "RBD_type",
        "Parent_RBD",
        "RBD_hierarchy",
        "derived_filed_fare",
        "combine_pos_od_comp_fb",
        "gfs",
        "combine_faretype",
        "fare_id",
        "dep_date_from_UTC",

        "travel_date_to",
        "travel_date_from",

    ]

    dF = pd.DataFrame(columns=cols)
    extreme_dep_date = "0" + dep_date[2:4] + dep_date[5:7] + dep_date[8:10]
    extreme_snap_date = "0" + snap_date[2:4] + snap_date[5:7] + snap_date[8:10]

    # print("Aggregating on ATPCO Fares Rules . . . . ")
    query = {
        "$and": [
            {
                "carrier": {"$eq": Host_Airline_Code}
            },
            {
                "compartment": compartment
            },

            {
                "fare_basis": farebasis
            },

            {
                "OD": {
                    "$in": ods
                }
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
                                    "$gte": extreme_dep_date  # to show expired fares
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_COMM": {
                                    "$lte": extreme_dep_date
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
                                    "$lte": extreme_dep_date
                                }
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": {
                                    "$gte": extreme_dep_date  # to include expired fares
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
                                    "$lte": extreme_dep_date
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
                                    "$gte": extreme_dep_date  # to include expired fares
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
                                    "$lte": extreme_dep_date
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
                                    "$gte": extreme_dep_date
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
                                    "$lte": extreme_dep_date
                                }
                            },
                            {
                                "cat_14.TRAVEL_DATES_EXP": {
                                    "$gte": extreme_dep_date
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
                                    "$lte": extreme_dep_date
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
                                    "$gte": extreme_dep_date
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
                                    "$lte": extreme_dep_date
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
                                    "$gte": extreme_dep_date
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
                                    "$lte": extreme_dep_date
                                }
                            },
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_EXP": {
                                    "$gte": extreme_dep_date
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes": {}
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes": None
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_COMM": {
                                    "$lte": extreme_dep_date
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
                                    "$gte": extreme_dep_date
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
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_COMM": None
                            },
                            {
                                "Footnotes.Cat_14_FN.TRAVEL_DATES_EXP": None
                            }
                        ]
                    }
                ]
            },
            {
                "$or": [
                    {
                        "effective_from": None
                    },
                    {
                        "effective_from": {
                            "$lte": snap_date
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
                            "$gte": snap_date  # to include expired fares as well
                        }
                    }
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
                                    "$lte": extreme_snap_date
                                }
                            },
                            {
                                "Footnotes.Cat_15_FN.SALE_DATES_LATEST_TKTG": {
                                    "$gte": extreme_snap_date
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
                                    "$gte": extreme_snap_date
                                }
                            }
                        ]
                    },
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_15_FN.SALE_DATES_EARLIEST_TKTG": {
                                    "$lte": extreme_snap_date
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
                                    "$lte": extreme_snap_date
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
                                    "$gte": extreme_snap_date
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
                    {
                        "$and": [
                            {
                                "Footnotes.Cat_15_FN.SALE_DATES_EARLIEST_TKTG": None
                            },
                            {
                                "Footnotes.Cat_15_FN.SALE_DATES_LATEST_TKTG": None
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
                                    "$lte": extreme_snap_date
                                }
                            },
                            {
                                "cat_15.SALE_DATES_LATEST_TKTG": {
                                    "$gte": extreme_snap_date
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
                                    "$lte": extreme_snap_date
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
                                    "$gte": extreme_snap_date
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
                                    "$lte": extreme_snap_date
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
                                    "$gte": extreme_snap_date
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
                                    "$lte": extreme_snap_date
                                }
                            },
                            {
                                "cat_15.SALE_DATES_LATEST_TKTG": {
                                    "$gte": extreme_snap_date
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
                                    "$lte": extreme_snap_date
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
                                    "$gte": extreme_snap_date
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
                                    "$lte": extreme_snap_date
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
                                    "$gte": extreme_snap_date
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

    # ("fetching...",farebasis)
    fares_data = list(db.JUP_DB_ATPCO_Fares_Rules.aggregate(
        [
            {
                '$match': query
            }
        ]
    ))
    # print("fares length  ="+str(len(fares_data)))

    if len(fares_data) > 0:
        dF = pd.DataFrame(fares_data)

    return dF


#     print(currency)


@measure(JUPITER_LOGGER)
def call_fareId(pos, client):
    # fzDBConn.JUP_DB_ATPCO_Fares_Rules.aggregate([{'$match':{'carrier':'FZ'}},{'$out':'ATPCO_Fares_FZ_Temp_for_FareID'}])
    # fzDBConn.ATPCO_Fares_FZ_Temp_for_FareID.createIndex({})
    # DF = fareId(origin, destination, compartment,farebasis ,snap_date, dep_date,currency)
    db = client[JUPITER_DB]

    currency_cursor = db.JUP_DB_Pos_Currency_Master.find()
    currencyPos_dict = dict()
    for currency_cursors in currency_cursor:
        currencyPos_dict[currency_cursors['pos']] = {'web': currency_cursors['web'], 'gds': currency_cursors['gds']}

    currency = "AED"
    number = 1
    updateBulk = db.JUP_DB_Manual_Triggers_Module.initialize_unordered_bulk_op()

    MT_crsr = db.JUP_DB_Manual_Triggers_Module.find(
        {'trx_date': SYS_SNAP_DATE, 'pos.City': pos,'sale_farebasis':{'$ne' : None}},
        {"_id": 1, "trx_date": 1, "dep_date": 1, "pos": 1, "origin": 1, "destination": 1, "compartment": 1,
         "sale_farebasis": 1}, no_cursor_timeout=True)
    clstCount = MT_crsr.count()
    num = 1


    @measure(JUPITER_LOGGER)
    def tripbasedFare(trip, cost):
        if (trip == "2"):
            return cost / 2
        else:
            return cost

    curCount = MT_crsr.count()
    for datas in MT_crsr:
        try:
            # MT_crsr = fzDBConn.JUP_DB_Sales_Flown.find({"pos":datas["pos"],"od":datas["od"],"month":11,"year":2017},{"_id":1,"book_date":1,"dep_date":1,"pos":1,"origin":1,"destination":1,"compartment":1,"fare_basis":1,"currency":1,"AIR_CHARGE":1},no_cursor_timeout=True)
            # '''
            # '''
            # print("Num  -->  "+str(num))
            if "sale_farebasis" in datas and datas['sale_farebasis'] != None:
                for each_crsr in datas["sale_farebasis"]:
                    # print(each_crsr)
                    try:
                        # print(str(curCount)+" / "+str(num)+' / '+datas['pos']+' / '+datas['od'])
                        num += 1
                        if "currency" in each_crsr:
                            currency = each_crsr['currency']
                        else:
                            currency = "AED"
                        # sprint(datas['pos']['City'],datas['origin']['City'], datas['destination']['City'], datas['compartment']['compartment'],each_crsr['fare_basis'], datas['trx_date'], datas['dep_date'])
                        DF = fareId(datas['pos']['City'], datas['origin']['City'], datas['destination']['City'],
                                    datas['compartment']['compartment'], each_crsr['fare_basis'], datas['trx_date'],
                                    datas['dep_date'], currencyPos_dict, db)
                        air_charge = each_crsr['AIR_CHARGE']
                        # print("Data Frame")
                        # print(DF.head())
                        DF["value"] = DF.apply(lambda row: convert_currency(row['fare'], row['currency'], currency),
                                               axis=1)
                        DF["value"] = DF.apply(lambda row: tripbasedFare(row['oneway_return'], row['value']), axis=1)
                        DF = DF.loc[(DF.value - air_charge).abs().argsort()[:1]]
                        fareId_ = DF.iloc[0]['fare_basis'] + "" + DF.iloc[0]['Rule_id'] + "" + DF.iloc[0]['footnote']
                        each_crsr['fareId'] = fareId_
                        print(fareId_, datas['_id'], number)

                    except IndexError as error:
                        # print(error)
                        pass
                        # print(error)
                    except ValueError as error:
                        # print(error)
                        pass
                        # print(error)
                    except KeyError as error:
                        # print(error)
                        pass
                        # print(error)
                    except Exception as error:
                        # print(error)
                        pass
                        # print(error)

            if "sale_farebasis" in datas and datas['sale_farebasis'] != None:
                pass
            else:
                datas["sale_farebasis"] = None

            # fzDBConn.JUP_DB_Manual_Triggers_Module.update_one({"_id":datas['_id'],},{"$set":{'flown_farebasis':datas['flown_farebasis'],'sale_farebasis':datas["sale_farebasis"]}})
            updateBulk.find({"_id": datas['_id'], "dep_date_ISO": {"$ne": None}}).update(
                {'$set': { 'sale_farebasis': datas["sale_farebasis"]}})

            if (number % 1000 == 0):
                try:
                    print(str(clstCount) + " / " + str(number))
                    result1 = updateBulk.execute()
                    updateBulk = db.JUP_DB_Manual_Triggers_Module.initialize_unordered_bulk_op()
                    # print(result1)
                except BulkWriteError as bwe:
                    print(bwe.details)

            number = number + 1

        except IndexError as error:
            # pass
            print(error)
        except ValueError as error:
            # pass
            print(error)
        except KeyError as error:
            # pass
            print(error)
        except Exception as error:
            print(error)
            # pass
    return "Done"


if __name__ == "__main__":
    from celery import group
    from jupiter_AI.batch.atpco_automation.Automation_tasks import run_fareId

    st = time.time()
    group1 = group([run_fareId.s(i) for i in range(1, 13)])
    res1 = group1()
    grp_res = res1.get()
    print 'time taken:', time.time() - st
