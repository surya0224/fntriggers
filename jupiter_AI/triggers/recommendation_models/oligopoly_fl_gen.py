import json
import time
from copy import deepcopy
import numpy as np
from jupiter_AI import client, JUPITER_DB, recommendation_upper_threshold, recommendation_lower_threshold
from jupiter_AI.triggers.mrkt_params_workflow_opt import get_capacity_dF
from jupiter_AI.triggers.host_params_workflow_opt import get_od_distance
#db = client[JUPITER_DB]
from jupiter_AI.triggers.common import pos_level_currency_data as get_currency_data
from jupiter_AI.network_level_params import SYSTEM_DATE, query_month_year_builder, today
from jupiter_AI.batch.fbmapping_batch.JUP_AI_Batch_Fare_Ladder_Mapping import get_exchange_rate_details
from collections import defaultdict
import pandas as pd
from jupiter_AI.network_level_params import SYSTEM_DATE
pd.options.mode.chained_assignment = None
import datetime


def get_host_fares_df(
        pos,
        origin,
        destination,
        compartment,
        extreme_start_date,
        extreme_end_date,
        db,
        comp=None):
    """
    :param origin:
    :param destination:
    :param compartment:
    :param dep_date_start:
    :param dep_date_end:
    :return:
    """
    # print "host_fares_comp: ", comp
    currency_data = get_currency_data(db=db)
    # if pos:
    #     if channel == 'web':
    #         currency = currency_data[pos]['web']
    #     else:
    #         if currency_data[pos]['gds']:
    #             currency = currency_data[pos]['gds']
    #         else:
    #             currency = currency_data[pos]['web']
    # else:
    #     if channel == 'web':
    #         currency = currency_data[origin]['web']
    #     else:
    #         if currency_data[origin]['gds']:
    #             currency = currency_data[origin]['gds']
    #         else:
    currency = []
    if currency_data[pos]['web']:
        currency.append(currency_data[pos]['web'])
    if currency_data[pos]['gds']:
        currency.append(currency_data[pos]['gds'])
    print "currency  =   ", currency

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
        "last_update_date",
        "last_update_time",
        "fare_id",
        "category",
        "total_fare",
        "competitor_farebasis",
        "dep_date_from_UTC",
        "recommended_fare",
        "travel_date_to",
        "travel_date_from"
    ]
    dF = pd.DataFrame(
        columns=cols
    )

    SYSTEM_DATE = "2017-10-10"
    today_lw = today - datetime.timedelta(days=7)
    SYSTEM_DATE_LW = str(today_lw)

    extreme_start_date_obj = datetime.datetime.strptime(extreme_start_date, "%Y-%m-%d")
    extreme_start_date_lw = str(extreme_start_date_obj - datetime.timedelta(days=7))

    extreme_start_date_n = "0" + extreme_start_date[2:4] + extreme_start_date[5:7] + extreme_start_date[8:10]
    extreme_start_date_n_lw = "0" + extreme_start_date_lw[2:4] + extreme_start_date_lw[5:7] + extreme_start_date_lw[8:10]
    extreme_end_date_n = "0" + extreme_end_date[2:4] + extreme_end_date[5:7] + extreme_end_date[8:10]

    SYSTEM_DATE_MOD = "0" + SYSTEM_DATE[2:4] + SYSTEM_DATE[5:7] + SYSTEM_DATE[8:10]
    SYSTEM_DATE_MOD_LW = "0" + SYSTEM_DATE_LW[2:4] + SYSTEM_DATE_LW[5:7] + SYSTEM_DATE_LW[8:10]

    query = defaultdict(list)
    qry_fares = defaultdict(list)
    # print "start: ",extreme_start_date_n
    # print "end: ", extreme_end_date_n
    # print "comp --->", comp
    if comp:
        comp = list(comp)
    else:
        comp = ['FZ']
    query = {
        "$and": [
            {
                "carrier": {"$in": comp}
            },
            {
                "compartment": compartment
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
                    "$in": [
                        origin + destination,
                        destination + origin
                    ]
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
                                    "$gte": extreme_start_date_n_lw # to show expired fares
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
                                    "$gte": extreme_start_date_n_lw # to include expired fares
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
                                    "$gte": extreme_start_date_n_lw # to include expired fares
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
                            "$gte": SYSTEM_DATE_LW # to include expired fares as well
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
    print "Getting host fares dF from ATPCO Fares Rules"
    fares_data = list(db.JUP_DB_ATPCO_Fares_Rules.aggregate(
        [
            {
                '$match': query
            },
            {
                '$addFields':
                    {
                        'effectiveness_comparison': {'$cmp': ['effective_to', 'effective_from']}
                    }
            },
            {
                '$match':
                    {
                        'effectiveness_comparison': {'$gt': 0}
                    }
            },
            {
                '$sort': {'fare': 1}
            }
        ], allowDiskUse=True
    ))
    print "fares_data length = ", len(fares_data)
    # print json.dumps(query)
    ##### Logic to get YR from JUP_DB_YR_Master and Tax from JUP_DB_Tax_Master
    exchange_rate = {}
    pos_country = list(db.JUP_DB_Region_Master.find(
        {"POS_CD": pos}))[0]['COUNTRY_CD']
    yr_crsr = list(db.JUP_DB_YR_Master.find(
        {"POS-LocCode": pos_country, "Compartment": compartment}))
    if len(yr_crsr) == 0:
        yr_crsr = list(db.JUP_DB_YR_Master.find({"POS-LocCode": "MISC"}))
    yr_curr = yr_crsr[0]['Curr']
    yr = {}
    tax_crsr = list(db.JUP_DB_Tax_Master.find({
        'Compartment': compartment,
        '$or': [{'$and': [{'Origin': origin}, {'Destination': destination}]},
                {'$and': [{'Origin': destination}, {'Destination': origin}]}]
    }))
    tax_df = pd.DataFrame(tax_crsr)
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
    print "tax_df = ", tax_df
    # Implementing YQ from YQ master collection
    yq_data = list(db.JUP_DB_YQ_Master.find({'compartment': compartment}))
    yq_df = pd.DataFrame(yq_data)
    yq_df.drop(['_id', 'compartment'], axis=1, inplace=True)
    yq_df = yq_df.rename(
        columns={
            'amount': 'amount_yq',
            'currency': 'currency_yq'})
    yq_currencies = list(yq_df['currency_yq'].unique())
    yq_df['oneway_return'] = yq_df['oneway_return'].apply(str)
    currency_crsr = list(db.JUP_DB_Exchange_Rate.find(
        {"code": {"$in": currency + [yr_curr] + tax_currencies + yq_currencies}}))
    for curr in currency_crsr:
        exchange_rate[curr['code']] = curr['Reference_Rate']
    for currency_ in currency:
        if yr_curr != currency_:
            yr_val = (yr_crsr[0]['Amount'] /
                      exchange_rate[yr_curr]) * exchange_rate[currency_]
            yr[currency_] = yr_val
        else:
            yr[currency_] = yr_crsr[0]['Amount']

    if len(fares_data) > 0:
        dF = pd.DataFrame(fares_data)
        print "fares_data = ", dF.columns
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
        print "dF.head() = ", dF.head()
        dF = dF.merge(tax_df, on=['OD', 'oneway_return'], how='left')
        dF['taxes'] = dF.apply(lambda row: row['tax_value_from_master'] /
                                           exchange_rate[row['currency']] *
                                           exchange_rate[row['tax_currency']], axis=1)
        # Merge YQ df and fares df(that has already been merged with tax df) to
        # compute yq
        dF = dF.merge(yq_df, on=['oneway_return'], how='left')
        dF['YQ'] = dF.apply(lambda row: row['amount_yq'] /
                                        exchange_rate[row['currency']] *
                                        exchange_rate[row['currency_yq']], axis=1)
        for i in range(len(dF)):
            has_category = False
            has_category_15 = False
            tvl_dates = []
            sale_dates = []
            surchrg_amts = []
            # try:
            #     tax = dF.loc[i, 'taxes']['tax_master']
            #     dF.loc[i, 'taxes'] = tax
            # except KeyError:
            #     dF.loc[i, 'taxes'] = 0
            # except TypeError:
            #     dF.loc[i, 'taxes'] = 0
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
                    if not entry['TRAVEL_DATES_COMM']:
                        entry['TRAVEL_DATES_COMM'] = "0000000"
                    if not entry['TRAVEL_DATES_EXP']:
                        entry['TRAVEL_DATES_EXP'] = "0999999"
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
                    if not entry['SALE_DATES_EARLIEST_TKTG']:
                        entry['SALE_DATES_EARLIEST_TKTG'] = "0000000"
                    if not entry['SALE_DATES_LATEST_TKTG']:
                        entry['SALE_DATES_LATEST_TKTG'] = "0999999"
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
                # print "--------------", (dF['cat_12'])
                # print "--------------", (dF['cat_12'][1]['_id'])
                # print "--------------", (dF['cat_12'][0]['_id'])
                # print "--------------", (dF['cat_12'][0]['_id'])
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
                    surchrg_amts.append({"key": key,
                                         "surcharge_pct": int(entry['CHARES_PERCENT'][0:3]),
                                         "surcharge_amt": int(entry['CHARGES_AMT']),
                                         "start_date": entry['DATE_RANGE_START'],
                                         "end_date": entry['DATE_RANGE_STOP']})
                temp_df = pd.DataFrame(surchrg_amts)
                temp_df.sort_values(by='key', inplace=True)

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
                    dF.loc[i,
                           'surcharge_amount_' + str(j + 1)] = temp_df.loc[j,
                                                                           'surcharge_pct'] * 0.01 * dF.loc[i,
                                                                                                            'fare'] + \
                                                               temp_df.loc[j,
                                                                           'surcharge_amt']
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

        dF['Average_surcharge'] = dF[['surcharge_amount_1',
                                      'surcharge_amount_2',
                                      'surcharge_amount_3',
                                      'surcharge_amount_4']].mean(axis=1)
        dF['dep_date_from'] = extreme_start_date
        dF['dep_date_to'] = extreme_end_date
        dF['YR'] = 0
        for i in currency:
            dF.loc[dF['currency'] == i, 'YR'] = yr[i]
        dF['YR'] = dF['YR'] * dF['oneway_return'].astype("int")
        dF.replace(-999, np.nan, inplace=True)
        if "DXB" not in origin+destination:
            dF['YQ'] = dF['YQ'] * 2
        dF = dF[cols]
        dF = dF.sort_values("fare")
        dF['effective_to'] = dF['effective_to'].fillna("2099-12-31")
        dF['is_expired'] = 0
        dF.loc[(dF['effective_to'] < SYSTEM_DATE) &
               (dF['sale_date_to'] < SYSTEM_DATE) &
               (dF['travel_date_to'] < SYSTEM_DATE), 'is_expired'] = 1
    return dF


def get_sales_fb_level_1(pos, origin, destination, compartment, host_fbs, dep_date_start, dep_date_end, db):
    sales_cursor = db.JUP_DB_Manual_Triggers_Module.aggregate([
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
        '$unwind': {'path': '$sale_farebasis'}
    },
    {
        '$match':
        {
            'sale_farebasis.fare_basis': {'$in': host_fbs}
        }
    },
    {
        '$project':
        {
            '_id': 0,
            'dep_date': '$dep_date',
            'fare_basis': '$sale_farebasis.fare_basis',
            'pax': '$sale_farebasis.pax',
            'revenue': '$sale_farebasis.rev'
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


def get_pax_yield(pos, origin, destination, compartment, host_fares, dep_date_start, dep_date_end, od_distance, db):
    FZ_CURRENCY = host_fares['currency'][0]
    host_fbs_list = list(host_fares['fare_basis'])
    print "host_fbs_list  in get_pax_yield = "
    print host_fbs_list
    sales_fare_basis = get_sales_fb_level_1(pos, origin, destination, compartment, host_fbs_list, dep_date_start, dep_date_end, db=db)
    if len(sales_fare_basis) == 0:
        cols_sales = ['fare_basis','fare_pax','revenue']
        sales_fare_basis = pd.DataFrame(columns=cols_sales)
    host_fares = host_fares.merge(sales_fare_basis, on='fare_basis', how='left')
    host_fares['yield'] = host_fares['revenue'] / host_fares['fare_pax']
    # This part of the code is written to convert yield to AED
    exchange_rate = {}
    currency_crsr = list(db.JUP_DB_Exchange_Rate.find({"code": FZ_CURRENCY}))
    for curr in currency_crsr:
        exchange_rate[curr['code']] = curr['Reference_Rate']
    if exchange_rate[FZ_CURRENCY]:
        currency_factor = exchange_rate[FZ_CURRENCY]
    else:
        currency_factor = 1.0
    host_fares['yield'] = host_fares['yield'] * currency_factor / od_distance * 100.0
    return host_fares


def get_mrkt_share_df(mrkt_query, db):
    """
    :param query:
    :return:
    """
    dF = pd.DataFrame(columns=['_id', 'market_share'])
    mrkt_data = list(db.JUP_DB_Market_Share.aggregate(
        [
            {
                '$match': mrkt_query
            },
            {
                '$group': {
                    '_id': {
                        'airline': '$MarketingCarrier1'
                    },
                    'pax': {'$sum': '$pax'},
                    'revenue': {'$sum': '$revenue'},
                    'pax_ly': {'$sum': '$pax_1'},
                    'revenue_ly': {'$sum': '$revenue_1'}
                }
            },
            #  Grouping again to obtain market level aggregates
            {
                '$group': {
                    '_id': None  # {
                    #     'pos':'$pos',
                    #     'od':'$od',
                    #     'compartment':'$compartment'
                    # }
                    ,
                    'airline_level_details': {
                        '$push': {
                            'airline': '$_id.airline',
                            'pax': '$pax',
                            'revenue': '$revenue',
                            'pax_ly': '$pax_ly',
                            'revenue_ly': '$revenue_ly'
                        }
                    },

                    'mrkt_size': {'$sum': '$pax'},
                    'mrkt_revenue': {'$sum': '$revenue'},
                    'mrkt_size_ly': {'$sum': '$pax_ly'},
                    'mrkt_revenue_ly': {'$sum': '$revenue_ly'}
                }
            },

            #  Unwinding the data again to the airline level of a market
            {
                '$unwind': '$airline_level_details'
            },
            # Calculating the required parameters market share, market share last year,
            # average fare,market average fare
            {
                '$project': {
                    '_id': '$airline_level_details.airline',
                    # 'airline': '$airline_level_details.airline',
                    # 'pax': '$airline_level_details.pax',
                    # 'pax_ly': '$airline_level_details.pax_ly',
                    'mrkt_share': {
                        '$cond': {
                            'if': {'$gt': ['$mrkt_size', 0]},
                            'then': {'$multiply': [{'$divide': ['$airline_level_details.pax', '$mrkt_size']}, 100]},
                            'else': None
                        }
                    }
                    # ,
                    # 'mrkt_share_ly': {
                    #     '$cond': {
                    #         'if': {'$gt': ['$mrkt_size_ly', 0]},
                    #         'then': {
                    #             '$multiply': [{'$divide': ['$airline_level_details.pax_ly', '$mrkt_size_ly']}, 100]},
                    #         'else': None
                    #     }
                    # }
                    # ,
                    # 'average_fare': {
                    #     '$cond': {
                    #         'if': {'$gt': ['$airline_level_details.pax', 0]},
                    #         'then': {'$divide': ['$airline_level_details.revenue', '$airline_level_details.pax']},
                    #         'else': None
                    #     }
                    # }
                    # ,
                    # 'mrkt_average_fare': {
                    #     '$cond': {
                    #         'if': {'$gt': ['$mrkt_size', 0]},
                    #         'then': {'$divide': ['$mrkt_revenue', '$mrkt_size']},
                    #         'else': None
                    #     }
                }
            }

        ]
    ))
    if len(mrkt_data) > 0:
        dF = pd.DataFrame(mrkt_data)

    return dF


def get_rating_df(origin, destination, compartment, db):
    """
    :param origin:
    :param destination:
    :return:
    """
    ratings_data = list(db.JUP_DB_Competitor_Ratings.aggregate([
        {
            '$match':
                {
                    'origin': origin,
                    'destination': destination,
                    'compartment': compartment
                }
        },
        {
            '$sort':
                {
                    'last_update_date': -1
                }
        }        # ,
        # {
        #     '$group':
        #         {
        #             '_id': '$airline',
        #             'rating': {
        #                 '$first': '$competitor_rating'
        #             }
        #         }
        # }
        ,
        {
            '$project':
                {
                    # '_id': 0,
                    # 'airline': '$_id',
                    'ratings': '$ratings'
                }
        }
    ]))
    dF = pd.DataFrame(columns=['_id', 'rating'])
    ratings_list = []
    if len(ratings_data) == 1:
        if ratings_data[0]['ratings']:
            ratings_dict = ratings_data[0]['ratings']
            for key, value in ratings_dict.items():
                ratings_list.append({'_id': key, 'rating': value})
    if ratings_list:
        dF = pd.DataFrame(ratings_list)
    dF.fillna(5, inplace=True)
    return dF


def build_mrkt_query(
        pos,
        origin,
        destination,
        compartment,
        dep_date_start,
        dep_date_end):
    """
    :param origin:
    :param destination:
    :param compartment:
    :param dep_date_start:
    :param dep_date_end:
    :return:
    """
    mrkt_share_qry = defaultdict(list)
    if pos:
        mrkt_share_qry['$and'].append({'pos': pos})
    if origin and destination:
        mrkt_share_qry['$and'].append({'od': origin + destination})
    if compartment:
        mrkt_share_qry['$and'].append({'compartment': compartment})

    if not dep_date_start:
        dep_date_start = SYSTEM_DATE
    std_obj = datetime.datetime.strptime(dep_date_start, '%Y-%m-%d')

    if not dep_date_end:
        month_year_combos = [
            {
                'month': {'$gte': std_obj.month},
                'year': std_obj.year
            },
            {
                'year': {'$gt': std_obj.year}
            }
        ]
    else:
        etd_obj = datetime.datetime.strptime(dep_date_end, '%Y-%m-%d')
        month_year_combos = query_month_year_builder(stdm=std_obj.month,
                                                     stdy=std_obj.year,
                                                     endm=etd_obj.month,
                                                     endy=etd_obj.year)

    mrkt_share_qry['$and'].append({'$or': month_year_combos})

    return dict(mrkt_share_qry)


def get_rbd_sellups_df(origin, destination, compartment):
    """
    :param origin:
    :param destination:
    :param compartment:
    :return:
    """
    rbd_sellup_data = list(db.JUP_DB_ATPCO_RBD_Diffuser_copy.aggregate(
        [
            # {
            #     '$match':
            #         {
            #             'origin': origin,
            #             'destination': destination,
            #             'compartment': compartment
            #         }
            # }
            # ,
            {
                '$group':
                    {
                        '_id': '$rbd',
                        'base_rbd': {'$first': '$base_rbd'},
                        'ow_sellup_abs': {'$first': '$amount_ow_absolute'},
                        'ow_sellup_perc': {'$first': '$amount_ow_percentage'},
                        'rt_sellup_abs': {'$first': '$amount_rt_absolute'},
                        'rt_sellup_perc': {'$first': '$amount_rt_percentage'},
                        'rbd_type': {'$first': '$rbd_type'},
                        'order': {'$first': '$base_rbd_order'}
                    }
            },
            {
                '$project':
                    {
                        '_id': 0,
                        'rbd': '$_id',
                        'base_rbd': '$base_rbd',
                        'ow_sellup_abs': '$ow_sellup_abs',
                        'ow_sellup_perc': '$ow_sellup_perc',
                        'rt_sellup_abs': '$rt_sellup_abs',
                        'rt_sellup_perc': '$rt_sellup_perc',
                        'rbd_type': '$rbd_type',
                        'order': "$order"
                    }
            }
        ]
    ))

    dF = pd.DataFrame(rbd_sellup_data)
    return dF


def main(
        pos,
        origin,
        destination,
        compartment,
        dep_date_start,
        dep_date_end,
        db,
        dates_list=[],
        channel='gds'):
    """
    :return:
    """

    extreme_start_date = dep_date_start
    extreme_end_date = dep_date_end

    st = time.time()
    if pos:
        if pos == destination:
            org = deepcopy(destination)
            dest = deepcopy(origin)
            origin = org
            destination = dest
    # Getting OD Distance
    od_distance = get_od_distance(origin + destination, db=db)
    print "got od distance = ", od_distance
    if not od_distance:
        od_distance = -1

    # Getting the Fares effective in those departure dates as a dF
    fares_dF = get_host_fares_df(pos=pos,
                                 origin=origin,
                                 destination=destination,
                                 compartment=compartment,
                                 extreme_start_date=extreme_start_date,
                                 extreme_end_date=extreme_end_date,
                                 db=db
                                 )
    if len(fares_dF) > 0:
        fz_curr = fares_dF['currency'][0]
        fares_dF['recommended_fare'] = 0
        fares_dF['recommended_fare_base'] = 0
        fares_dF['recommended_fare_total'] = 0
        fares_dF['reco_from_model'] = 0
        fares_dF['competitor_star_rating_config'] = [[]] * len(fares_dF)
        fares_dF['reco_rating'] = 0
        fares_dF['total_fare_1'] = 0
        fares_dF['total_fare_2'] = 0
        fares_dF['total_fare_3'] = 0
        fares_dF['total_fare_4'] = 0
        fares_dF['total_fare'] = 0
        fares_dF['retention_fare'] = 0
        fares_dF['accepted_total_fare'] = 0
        fares_dF['accepted_base_fare'] = 0
        fares_dF['no_market_share_flag'] = False
        fares_dF['num_of_comp'] = 0
        fares_dF['reco_from_model'] = False
        fares_dF['reco_rating'] = 0
        fares_dF['od_distance'] = od_distance
        # fares_dF['taxes'] = 0
        if 'YR' not in fares_dF.columns:
            fares_dF['YR'] = 0
        print "got fares df"
        for id, row in fares_dF.iterrows():
            # print id, row
            # total_fare1 = base_fare + yq + yr + surcharge1_amount
            # total_fare2 = base_fare + yq + yr + surcharge2_amount
            # total_fare3 = base_fare + yq + yr + surcharge3_amount
            # total_fare4 = base_fare + yq + yr + surcharge4_amount
            # print "----------->", row['Average_surcharge']
            if row['fare']:
                base_fare = row['fare']
            else:
                base_fare = 0
            if row['YQ']:
                yq = row['YQ']
            else:
                yq = 0
            if row['YR']:
                yr = row['YR']
            else:
                yr = 0
            if row['surcharge_amount_1']:
                surcharge1_amount = row['surcharge_amount_1']
            else:
                surcharge1_amount = 0
            if row['surcharge_amount_2']:
                surcharge2_amount = row['surcharge_amount_2']
            else:
                surcharge2_amount = 0
            if row['surcharge_amount_3']:
                surcharge3_amount = row['surcharge_amount_3']
            else:
                surcharge3_amount = 0
            if row['surcharge_amount_4']:
                surcharge4_amount = row['surcharge_amount_4']
            else:
                surcharge4_amount = 0
            if row['Average_surcharge']:
                avg_surcharge = row['Average_surcharge']
            else:
                avg_surcharge = 0
            # else:
            #     surcharge4_amount = 0
            if row['taxes']:
                taxes = row['taxes']
            else:
                taxes = 0

            total_fare1 = base_fare + yq + yr + surcharge1_amount + taxes
            total_fare2 = base_fare + yq + yr + surcharge2_amount + taxes
            total_fare3 = base_fare + yq + yr + surcharge3_amount + taxes
            total_fare4 = base_fare + yq + yr + surcharge4_amount + taxes
            total_fare = base_fare + yq + yr + avg_surcharge + taxes
            fares_dF.set_value(id, 'total_fare_1', total_fare1)
            fares_dF.set_value(id, 'total_fare_2', total_fare2)
            fares_dF.set_value(id, 'total_fare_3', total_fare3)
            fares_dF.set_value(id, 'total_fare_4', total_fare4)
            fares_dF.set_value(id, 'total_fare', total_fare)
        # print fares_dF[['total_fare_1',
        #                 'total_fare_2',
        #                 'total_fare_3',
        #                 'total_fare_4',
        #                 'total_fare',
        #                 'surcharge_amount_1',
        #                 'surcharge_amount_2',
        #                 'surcharge_amount_3',
        #                 'surcharge_amount_4',
        #                 'YQ',
        #                 'YR',
        #                 'taxes']].loc[0]
        # Airline level Market Share Values
        st = time.time()
        mrkt_shr_qry = build_mrkt_query(pos=pos,
                                        origin=origin,
                                        destination=destination,
                                        compartment=compartment,
                                        dep_date_start=extreme_start_date,
                                        dep_date_end=extreme_end_date)
        print "Built market share query in ", time.time() - st, " seconds."
        st = time.time()
        mrkt_shr_dF = get_mrkt_share_df(mrkt_shr_qry, db=db)
        print "got market share df in ", time.time() - st, " seconds."
        st = time.time()
        capacity_dF = get_capacity_dF(pos=pos,
                                      origin=origin,
                                      destination=destination,
                                      compartment=compartment,
                                      dep_date_start=extreme_start_date,
                                      dep_date_end=extreme_end_date,
                                      db=db)
        print "got capacity df in ", time.time() - st, " seconds."

        # Airline Level Rating Values
        st = time.time()
        ratings_dF = get_rating_df(origin=origin,
                                   destination=destination,
                                   compartment=compartment,
                                   db=db)
        print "got ratings df in ", time.time() - st, " seconds"

        fms_dF = capacity_dF.merge(
            ratings_dF,
            left_on='carrier',
            right_on='_id',
            how='left')
        fms_dF['rating'].fillna(5, inplace=True)
        fms_dF['capacity_rating'] = fms_dF['capacity'] * fms_dF['rating']
        fms_dF['fms'] = fms_dF['capacity_rating'] * \
            100 / fms_dF.sum()['capacity_rating']
        st = time.time()
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
        print "got pos list from region master"
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
        print "got origin list from region master"
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
        print "got destination list from region master"
        compartment_list = [compartment, 'all']
        print "created compartment list"
        print "Got POS, OD, Compartment list in ", time.time() - st, " seconds."
        st = time.time()
        for index, row in fares_dF.iterrows():
            mrkt_shr_dF_temp = mrkt_shr_dF[mrkt_shr_dF['_id'] == row['carrier']]
            fms_dF_temp = fms_dF[fms_dF['carrier'] == row['carrier']]
            mrkt_shr_val = np.NaN
            if len(mrkt_shr_dF_temp) == 1:
                mrkt_shr_val = mrkt_shr_dF_temp.mrkt_share.iloc[0]
            elif len(fms_dF_temp) == 1:
                mrkt_shr_val = fms_dF_temp.fms.iloc[0]
            else:
                mrkt_shr_val = 1
                fares_dF.loc[index, 'no_market_share_flag'] = True
            retention_fare = 0
            if(row['fare']):
                retention_fare += row['fare']
                if(row['YQ']):
                    retention_fare += row['YQ']
                if(row['YR']):
                    retention_fare += row['YR']
                if (row['Average_surcharge']):
                    retention_fare += row['Average_surcharge']
            else:
                retention_fare += row['total_fare']
            ratings_dF_temp = ratings_dF[ratings_dF['_id'] == row['carrier']]
            # print 'Rating Temp DF', ratings_dF_temp

            if len(ratings_dF_temp) == 1:
                ratings_val = ratings_dF_temp.rating.iloc[0]
            else:
                ratings_val = 5

            fares_dF.set_value(index, 'market_share', mrkt_shr_val)
            fares_dF.set_value(index, 'ratings', ratings_val)
            fares_dF.set_value(index, 'retention_fare', retention_fare)
            # print base_rbd_val
            # fares_dF.base_rbd = fares_dF.base_rbd.astype(str)
            # print fares_dF.astype(object)
            # fares_dF.set_value(index, 'base_rbd', base_rbd_val)
            # fares_dF.set_value(index, 'rbd_type', rbd_type_val)
            # fares_dF.set_value(index, 'order', order_val)
            # fares_dF.set_value(index, 'ow_sellup_abs', ow_sellup_abs)
            # fares_dF.set_value(index, 'ow_sellup_perc', ow_sellup_perc)
            # fares_dF.set_value(index, 'rt_sellup_abs', rt_sellup_abs)
            # fares_dF.set_value(index, 'rt_sellup_perc', rt_sellup_perc)
            # fares_dF.set_value(index, 'recommended_fare', None)
        # print fares_dF.head()
        print "Added market_share, ratings and retention fare column in ", time.time() - st, " seconds."

        # Isolating the fares that have been mapped to atleast one of competitor
        # fares
        temp_fares_dF = fares_dF[fares_dF['competitor_farebasis'].notnull()]
        # if len(temp_fares_dF) > 0:
        #     print temp_fares_dF
        # else:
        #     print "No documents in temp_fares_dF"
        st = time.time()
        if len(temp_fares_dF) > 0:

            # print 'Fares with Fb Mapping',
            # print temp_fares_dF
            # Extracting the competitor data for the mapped farebasis
            competitor_data = []

            for index, row in temp_fares_dF.iterrows():
                # print row
                comp_data = []
                # no_market_share_flag is a flag that gets turned on if neither Market share nor Fair market share is
                # available for any of the competitor.
                if not row['no_market_share_flag']:
                    temp_fares_dF.loc[index, 'no_market_share_flag'] = False
                for comp in row['competitor_farebasis']:
                    num_of_comp = len('competitor_farebasis')
                    # if comp['source'] == 'infare':
                    airline = comp['carrier']
                    farebasis = comp['fare_basis']
                    fare = comp['fare']
                    market_share_val_df = pd.DataFrame(
                        mrkt_shr_dF[mrkt_shr_dF['_id'] == airline])
                    fms_val_df = fms_dF[fms_dF['carrier'] == airline]
                    # print market_share_val_df
                    # print len(market_share_val_df.index)
                    market_share_val = np.NaN
                    if len(market_share_val_df) == 1:
                        market_share_val = market_share_val_df.mrkt_share.iloc[0]
                    elif len(fms_val_df) == 1:
                        market_share_val = fms_val_df.fms.iloc[0]
                    else:
                        market_share_val = 1 / (num_of_comp + 1) * 100
                        temp_fares_dF.loc[index, 'no_market_share_flag'] = True
                        temp_fares_dF.loc[index, 'num_of_comp'] = num_of_comp

                    rating_val_df = pd.DataFrame(
                        ratings_dF[ratings_dF['_id'] == airline])
                    # print 'tempratings_val',rating_val_df
                    # print len(rating_val_df.index)
                    if len(rating_val_df.index) == 1:
                        rating_val = rating_val_df.rating.iloc[0]
                    else:
                        rating_val = 5

                    #   airline             comp_tuple[0]
                    #   farebasis           comp_tuple[1]
                    #   fare                comp_tuple[2]
                    #   market_share_val    comp_tuple[3]
                    #   rating_val          comp_tuple[4]
                    # print airline, farebasis, fare, market_share_val, rating_val,
                    # no_market_share_flag, num_of_comp
                    comp_tuple = (
                        airline.encode(),
                        farebasis,
                        fare,
                        market_share_val,
                        rating_val)
                    comp_data.append(comp_tuple)
                competitor_data.append(comp_data)
            temp_fares_dF.loc[:, 'competitor_data'] = competitor_data

            # Recommending the fare for host fares mapped to competitors.
            # Also appending recommended fare rbds' data to lists for further usage(both OW and return)
            # ow_rbds_recommended = []
            # rt_rbds_recommended = []
            diff_index_list = []
            comp_star_rating_cursor = db.JUP_DB_Competitor_Star_Rating_Configuration.aggregate(
                [
                    {
                        "$match": {
                            "pos.value": {
                                "$in": pos_list}, "origin.value": {
                                "$in": origin_list}, "destination.value": {
                                "$in": destination_list}, "compartment.value": {
                                "$in": compartment_list}, "competitors.eff_dep_date_from": {
                                "$lte": datetime.datetime.strptime(
                                    SYSTEM_DATE, "%Y-%m-%d")}, "competitors.eff_dep_date_to": {
                                "$gte": datetime.datetime.strptime(
                                    SYSTEM_DATE, "%Y-%m-%d")}}}, {
                    '$sort': {
                        'priority': -1}}, {
                    '$limit': 1}, {
                    "$project": {
                        "competitors": 1, }}])
            comp_star_rating_cursor_list = list(comp_star_rating_cursor)
            threshold_airline_list = [
                i['code'] for i in comp_star_rating_cursor_list[0]['competitors']]
            print "Threshold airline list:  ", threshold_airline_list
            for index, row in temp_fares_dF.iterrows():
                temp = 0
                currency_converter = get_exchange_rate_details()
                if not row['no_market_share_flag']:
                    total_market_share = 0
                    if row['market_share'] > 0:
                        total_market_share += row['market_share']
                    # print 'total_ms', total_market_share
                    for comp in row['competitor_data']:
                        # print comp[3]
                        total_market_share += comp[3]

                    # print temp
                    if row['market_share'] > 0:
                        if row['retention_fare']:
                            temp += (row['retention_fare']) * (row['market_share'] /
                                                               total_market_share) / (row['ratings'])
                        else:
                            temp += (row['total_fare']) * (row['market_share'] / \
                                     total_market_share) / (row['ratings'])
                        # print temp
                    for comp in row['competitor_data']:
                        # print 'Comp', comp[0]
                        # print 'rating', comp[4]
                        # print 'market_share', comp[3]
                        # print 'abs_mrkt_share', comp[3] / total_market_share
                        # print 'fare', comp[2]
                        temp += (comp[2] * (comp[3] /
                                            total_market_share)) / (comp[4])

                else:
                    if row['retention_fare']:
                        temp += row['retention_fare'] / \
                            (num_of_comp + 1) / (row['ratings'])
                    else:
                        temp += (row['total_fare']) / \
                            (num_of_comp + 1) / (row['ratings'])
                    for comp in row['competitor_data']:
                        temp += comp[2] / (num_of_comp + 1) / (comp[4])

                reco_fare = temp * row['ratings']
                if row['retention_fare']:
                    change_perc = (
                        reco_fare - row['retention_fare']) * 100 / row['retention_fare']
                else:
                    change_perc = (
                        reco_fare - row['total_fare']) * 100 / row['total_fare']
                if recommendation_lower_threshold < change_perc < recommendation_upper_threshold:
                    pass
                elif change_perc > recommendation_upper_threshold:
                    if row['retention_fare']:
                        reco_fare = row['retention_fare'] * \
                            (1 + (recommendation_upper_threshold / float(100)))
                    else:
                        reco_fare = row['total_fare'] * \
                            (1 + (recommendation_upper_threshold / float(100)))
                else:
                    if row['retention_fare']:
                        reco_fare = row['retention_fare'] * \
                            (1 + (recommendation_lower_threshold / float(100)))
                    else:
                        reco_fare = row['total_fare'] * \
                            (1 + (recommendation_lower_threshold / float(100)))

                if row['retention_fare']:
                    diff = reco_fare - row['retention_fare']
                else:
                    diff = reco_fare - row['total_fare']

                if temp > 0:
                    fares_dF.set_value(index, 'reco_from_model', True)

                    # This should ideally return only one document as there is one
                    # document for every market defined by POS, O, D, Compartment
                    comp_config_list = []
                    if len(comp_star_rating_cursor_list) == 1:
                        for individual_comp in row['competitor_data']:
                            if individual_comp[0] in threshold_airline_list:
                                for i_competitor in comp_star_rating_cursor_list[0]['competitors']:
                                    comp_name_and_threshold = defaultdict()
                                    if individual_comp[0] == i_competitor['code']:
                                        comp_name_and_threshold['airline'] = i_competitor['code']
                                        if row['oneway_return'] in [
                                                1, 3]:  # one_way
                                            try:
                                                currency_factor = currency_converter[i_competitor['ow']
                                                                                     ['currency']][row['currency']]
                                            except KeyError:
                                                currency_factor = 1
                                            if i_competitor['ow']['threshold_type'] == 'p':
                                                upper_limit = (
                                                    1 + i_competitor['ow']['threshold_upper'] / 100) * individual_comp[2]
                                                lower_limit = (
                                                    1 + i_competitor['ow']['threshold_lower'] / 100) * individual_comp[2]
                                            elif i_competitor['ow']['threshold_type'] == 'a':
                                                upper_limit = individual_comp[2] + \
                                                    i_competitor['ow']['threshold_upper'] * currency_factor
                                                lower_limit = individual_comp[2] + \
                                                    i_competitor['ow']['threshold_lower'] * currency_factor
                                            else:
                                                upper_limit = 99999999
                                                lower_limit = 0
                                        else:  # return
                                            try:
                                                currency_factor = currency_converter[i_competitor['ow']
                                                                                     ['currency']][row['currency']]
                                            except KeyError:
                                                currency_factor = 1
                                            if i_competitor['rt']['threshold_type'] == 'p':
                                                upper_limit = (
                                                    1 + i_competitor['rt']['threshold_upper'] / 100) * individual_comp[2]
                                                lower_limit = (
                                                    1 + i_competitor['rt']['threshold_lower'] / 100) * individual_comp[2]
                                            elif i_competitor['rt']['threshold_type'] == 'a':
                                                upper_limit = individual_comp[2] + \
                                                    i_competitor['rt']['threshold_upper'] * currency_factor
                                                lower_limit = individual_comp[2] + \
                                                    i_competitor['rt']['threshold_lower'] * currency_factor
                                            else:
                                                upper_limit = 99999999
                                                lower_limit = 0
                                        if upper_limit:
                                            comp_name_and_threshold['upper_limit'] = upper_limit
                                        else:
                                            comp_name_and_threshold['upper_limit'] = 99999999
                                        if lower_limit:
                                            comp_name_and_threshold['lower_limit'] = lower_limit
                                        else:
                                            comp_name_and_threshold['lower_limit'] = 0

                                        comp_config_list.append(comp_name_and_threshold)
                        fares_dF.set_value(index, 'competitor_star_rating_config', comp_config_list)
                        comp_config_list = []

                    #row['competitor_star_rating_config'] = {'EK': {'lower_limit' : 100, 'upper_limit': 500},'QR':{'lower_limit': 200,'upper_limit':500},'XY':{'lower_limit'}}
                diff_index_list.append((index, diff, reco_fare))

            # print diff_index_list
            print "Time taken to calculate recommendations for temp_fare_dF and calculate recommendation rating = ", time.time() - st, " seconds"
            st = time.time()
            if diff_index_list:
                for idx, val in enumerate(diff_index_list):
                    # print idx
                    # print val
                    if idx == 0:
                        for findex, frow in fares_dF.iterrows():
                            if findex != val[0]:
                                if frow['retention_fare']:
                                    fares_dF.set_value(
                                        findex, 'recommended_fare', frow['retention_fare'] + val[1])
                                else:
                                    fares_dF.set_value(
                                        findex, 'recommended_fare', frow['total_fare'] + val[1])
                            else:
                                fares_dF.set_value(
                                    findex, 'recommended_fare', val[2])
                    else:
                        for findex, frow in fares_dF.iterrows():
                            if findex > val[0]:
                                if frow['retention_fare']:
                                    fares_dF.set_value(
                                        findex, 'recommended_fare', frow['retention_fare'] + val[1])
                                else:
                                    fares_dF.set_value(
                                        findex, 'recommended_fare', frow['total_fare'] + val[1])
                            elif findex == val[0]:
                                fares_dF.set_value(
                                    findex, 'recommended_fare', val[2])

                for idx, row in fares_dF.iterrows():
                    if row['retention_fare']:
                        change_perc = (
                            (row['recommended_fare'] -
                             row['retention_fare']) *
                            100 /
                            row['retention_fare'])
                    else:
                        change_perc = (
                            (row['recommended_fare'] -
                             row['total_fare']) *
                            100 /
                            row['total_fare'])
                    if recommendation_lower_threshold < change_perc < recommendation_upper_threshold:
                        pass
                    elif change_perc > recommendation_upper_threshold:
                        if row['retention_fare']:
                            reco_fare = row['retention_fare'] * \
                                (1 + (recommendation_upper_threshold / float(100)))
                        else:
                            reco_fare = row['total_fare'] * \
                                (1 + (recommendation_upper_threshold / float(100)))
                        fares_dF.set_value(idx, 'recommended_fare', reco_fare)
                    else:
                        if row['retention_fare']:
                            reco_fare = row['retention_fare'] * \
                                (1 + (recommendation_lower_threshold / float(100)))
                        else:
                            reco_fare = row['total_fare'] * \
                                (1 + (recommendation_lower_threshold / float(100)))
                        fares_dF.set_value(idx, 'recommended_fare', reco_fare)
                    if row['retention_fare']:
                        fares_dF.set_value(
                            idx,
                            'perc_change',
                            ((row['recommended_fare'] -
                              row['retention_fare']) *
                                100 /
                                row['retention_fare']))
                    else:
                        fares_dF.set_value(
                            idx,
                            'perc_change',
                            ((row['recommended_fare'] - row['total_fare']) * 100 / row['total_fare']))

            print "Time taken to recommend on entire fareladder based on sellups = ", time.time() - st, " seconds."
            fares_dF.recommended_fare_base = fares_dF.recommended_fare - \
                fares_dF.YQ - fares_dF.Average_surcharge - fares_dF.YR
            fares_dF.recommended_fare_total = fares_dF.recommended_fare + fares_dF.taxes
            # fares_dF.reco_yield = fares_dF.recommended_fare / od_distance
            response_dF = fares_dF.filter(items=[
                'dep_date_from',
                'dep_date_to',
                'travel_date_from',
                'travel_date_to',
                'fare_basis',
                'effective_from',
                'effective_to',
                'sale_date_from',
                'sale_date_to',
                'Rule_id',
                'oneway_return',
                'footnote',
                'last_ticketed_date',
                'channel',
                'currency',
                'fare',
                'fare_id',
                'YQ',
                'rtg',
                'tariff_code',
                'reco_from_model',
                'reco_rating',
                'competitor_star_rating_config',
                'surcharge',
                'surcharge_date_start_1',
                'surcharge_date_start_2',
                'surcharge_date_start_3',
                'surcharge_date_start_4',
                'surcharge_date_end_1',
                'surcharge_date_end_2',
                'surcharge_date_end_3',
                'surcharge_date_end_4',
                'surcharge_amount_1',
                'surcharge_amount_2',
                'surcharge_amount_3',
                'surcharge_amount_4',
                'Average_surcharge',
                'total_fare_1',
                'total_fare_2',
                'total_fare_3',
                'total_fare_4',
                'RBD',
                'YR',
                'od_distance',
                'retention_fare',
                'accepted_total_fare',
                'accepted_base_fare',
                'last_update_date',
                'taxes',
                'total_fare',
                'recommended_fare',
                'recommended_fare_base',
                'recommended_fare_total',
                'perc_change',
                'is_expired'
            ])
            for index, row in response_dF.iterrows():
                if row['YR']:
                    yr = row['YR']
                else:
                    yr = 0
                if row['YQ']:
                    yq = row['YQ']
                else:
                    yq = 0
                if row['Average_surcharge']:
                    surcharge_avg = row['Average_surcharge']
                else:
                    surcharge_avg = 0
                if row['recommended_fare']:
                    if row['taxes'] and row['retention_fare']:
                        recommended_fare_total = row['recommended_fare'] + \
                            row['taxes']
                    else:
                        recommended_fare_total = row['recommended_fare']
                    if row['YQ'] or row['Average_surcharge'] or row['YR']:
                        recommended_fare_base = row['recommended_fare'] - \
                            yr - yq - surcharge_avg
                    else:
                        recommended_fare_base = row['recommended_fare']
                else:
                    recommended_fare_total = "NA"
                    recommended_fare_base = "NA"
                response_dF.set_value(
                    index,
                    'recommended_fare_total',
                    recommended_fare_total)
                response_dF.set_value(
                    index,
                    'recommended_fare_base',
                    recommended_fare_base)
                # response_dF.where((pd.notnull(response_dF)), None)
        else:  # NO data in temp_fares_dF ----------> None of the fares in fare ladder have any competitor fare mapped
            # print "Duplicated:  ", fares_dF[fares_dF.index.duplicated()]
            print "None of the fares in fare ladder have any competitor fare mapped"
            if len(fares_dF) > 0:
                for idx, row in fares_dF.iterrows():
                    fares_dF.set_value(
                        idx, 'recommended_fare', row['retention_fare'])
                    fares_dF.set_value(idx, 'recommended_fare_base', row['fare'])
                    fares_dF.set_value(
                        idx, 'recommended_fare_total', row['total_fare'])
            else:
                print "SET ALL RECOMMENDATIONS TO NA"
            fares_dF['perc_change'] = 0
            response_dF = fares_dF[[
                'dep_date_from',
                'dep_date_to',
                'travel_date_from',
                'travel_date_to',
                'fare_basis',
                'effective_from',
                'effective_to',
                'sale_date_from',
                'sale_date_to',
                'Rule_id',
                'oneway_return',
                'footnote',
                'channel',
                # 'last_ticketed_date',
                'currency',
                'fare',
                # 'reco_yield',
                'fare_id',
                'YQ',
                # 'surcharge',
                'RBD',
                'RBD_type',
                'YR',
                'rtg',
                'tariff_code',
                'reco_from_model',
                'competitor_star_rating_config',
                'reco_rating',
                'surcharge_date_start_1',
                'surcharge_date_start_2',
                'surcharge_date_start_3',
                'surcharge_date_start_4',
                'surcharge_date_end_1',
                'surcharge_date_end_2',
                'surcharge_date_end_3',
                'surcharge_date_end_4',
                'surcharge_amount_1',
                'surcharge_amount_2',
                'surcharge_amount_3',
                'surcharge_amount_4',
                'total_fare_1',
                'total_fare_2',
                'total_fare_3',
                'total_fare_4',
                'accepted_total_fare',
                'accepted_base_fare',
                'Average_surcharge',
                'retention_fare',
                'last_update_date',
                'taxes',
                'od_distance',
                'total_fare',
                'recommended_fare',
                'recommended_fare_total',
                'recommended_fare_base',
                'perc_change',
                'is_expired'
            ]]
        for idx, row in response_dF.iterrows():
            if row['RBD'] == 'L':
                if row['recommended_fare'] and row['recommended_fare'] != 'NA' and row['total_fare']:
                    if row['recommended_fare'] > row['retention_fare']:
                        response_dF.set_value(
                            index, 'recommended_fare_total', row['total_fare'])
                        response_dF.set_value(
                            index, 'recommended_fare_base', row['fare'])
                        response_dF.set_value(
                            index, 'recommended_fare', row['retention_fare'])
        exchange_rate = {}
        currency_crsr = list(
            db.JUP_DB_Exchange_Rate.find({"code": fz_curr}))
        for curr in currency_crsr:
            exchange_rate[curr['code']] = curr['Reference_Rate']
        if exchange_rate[fz_curr]:
            currency_factor = exchange_rate[fz_curr]
        else:
            currency_factor = 1.0

        response_dF = get_pax_yield(pos,
                                    origin,
                                    destination,
                                    compartment,
                                    response_dF,
                                    dep_date_start,
                                    dep_date_end,
                                    od_distance,
                                    db=db)

        response_dF['reco_yield'] = response_dF['recommended_fare'] * currency_factor / od_distance * 100

        # for idx, row in response_dF.iterrows():
        #     sales_data = get_sales_fb_level(pos=pos,
        #                                     origin=origin,
        #                                     destination=destination,
        #                                     compartment=compartment,
        #                                     farebasis=row['fare_basis'],
        #                                     sale_start_date=row['sale_date_from'],
        #                                     sale_end_date=row['sale_date_to'],
        #                                     dates_list=dates_list
        #                                     )
        #     response_dF.set_value(idx, 'sales_data', sales_data)
        #     # if sales_data:
        #     #     if sales_data['pax']:
        #     #         pax = sales_data['pax']
        #     #     else:
        #     #         pax = 0
        #     #     if sales_data['revenue'] == 'NA' or not sales_data['revenue']:
        #     #         curr_yield = "NA"
        #     #     elif od_distance > 0 and sales_data['pax'] > 0:
        #     #         curr_yield = sales_data['revenue'] * 100 / ( float(sales_data['pax']) * float(od_distance) )
        #     #     else:
        #     #         curr_yield = "NA"
        #     # else:
        #     #     pax = 'NA'
        #     #     curr_yield = 'NA'
        #     if row['recommended_fare'] and od_distance > 0:
        #         reco_yield = row['recommended_fare'] * 100 / float(od_distance)
        #     else:
        #         reco_yield = 'NA'
        #     response_dF.set_value(idx, 'reco_yield', reco_yield)
        # i = 1
        # response_dF['key_date'] = None
        # for date in dates_list:
        #     temp_dep_date_start = date['start']
        #     temp_dep_date_end = date['end']
        #     response_dF.loc[(response_dF['travel_date_from'] <= temp_dep_date_end) & (
        #         response_dF['travel_date_to'] >= temp_dep_date_start), 'key_date'] = 'date' + str(i)
        # response_dF['fare_pax'] = response_dF.apply(
        #     lambda row: get_pax(row['sales_data'], row['key_date']), axis=1)
        # response_dF['yield'] = response_dF.apply(
        #     lambda row: get_yield(
        #         row['sales_data'],
        #         row['key_date'],
        #         od_distance,
        #         exchange_rate[fz_curr]),
        #     axis=1)
        response_dF['fare_pax'].fillna(0)
        response_dF['yield'].fillna(0)
        response_dF['status'] = "I"
        response_dF.loc[response_dF['perc_change'] < 0, 'status'] = "D"
        response_dF.loc[response_dF['perc_change'] == 0, 'status'] = "S"
        response_dF.fillna(0, inplace=True)
        print response_dF.columns
        return response_dF.to_dict('records')
    else:
        return []


def get_yield(row, key_date_temp, od_distance, exchange_rate):
    if row[0][key_date_temp]:
        if row[0][key_date_temp][0]['revenue'] > 0 and row[0][key_date_temp][0]['pax'] > 0 and od_distance > 0:
            rev = row[0][key_date_temp][0]['revenue'] * exchange_rate
            return rev / (row[0][key_date_temp][0]['pax'] * od_distance)
        else:
            return 'NA'
    else:
        return 'NA'


def get_pax(row, key_date_temp):
    if row[0][key_date_temp]:
        if row[0][key_date_temp][0]['pax']:
            return row[0][key_date_temp][0]['pax']
        else:
            return 'NA'
    else:
        return 'NA'


def get_sales_fb_level(pos,
                       origin,
                       destination,
                       compartment,
                       farebasis,
                       db,
                       sale_start_date=None,
                       sale_end_date=None,
                       dates_list=None):
    """
    :param pos: point of sale
    :param origin: origin
    :param destination: destination
    :param compartment: compartment
    :param fareid: unique combination of farebasis, footnote and ruleid
    :param farebasis: farebasis code of fare
    :param travel_start_date: if travel restrictions are present travel start date of fare
    :param travel_end_date: if travel restrictions are presnet travel end date of fare
    :param sale_start_date: if sale restrictions are present sale start date of fare
    :param sale_end_date: if sale_restrictions are present sale end date of fare
    :return:
    """

    query = defaultdict(list)
    query_facet = defaultdict()
    if pos:
        query['$and'].append({'pos': pos})
    if origin:
        query['$and'].append({'origin': origin})
    if destination:
        query['$and'].append({'destination': destination})
    if compartment:
        query['$and'].append({'compartment': compartment})

    if farebasis:
        query['$and'].append({'fare_basis': farebasis})

    if sale_start_date:
        query['$and'].append({'book_date': {'$gte': sale_start_date}})

    if sale_end_date:
        query['$and'].append({'book_date': {'$lte': sale_end_date}})
    else:
        query['$and'].append({'book_date': {'$lte': SYSTEM_DATE}})
    i = 1
    for date in dates_list:
        key = 'date' + str(i)
        temp_date_query = {key: [
            {
                '$match':
                    {
                        'dep_date': {'$gte': date['start'], '$lte': date['end']}
                    }
            },
            {
                '$group':
                    {
                        '_id': 0,
                        'pax': {'$sum': '$pax'},
                        'revenue': {'$sum': '$revenue'}
                    }
            },
            {
                '$project':
                    {
                        '_id': 0,
                        'pax': '$pax',
                        'revenue': '$revenue'
                    }
            }

        ]}
        query_facet.update(temp_date_query)
        i += 1
    try:
        crsr = db.JUP_DB_Sales.aggregate(
            [
                {
                    '$match': dict(query)
                },
                {
                    '$facet': dict(query_facet)
                }
            ]
        )
        # looks something like this: [{date1: [{pax:10 , revenue: 12}]},{date2:
        # [{pax:11 , revenue: 22}]},{date3: [{pax:12 , revenue: 32}]}]. The
        # values of date range is not mentioned here. Just date + str(index+1)
        # of the date in dates_list
    except BaseException:
        print "Unable to get Sales data from JUP_DB_Sales. Check query."
        return {
            'date0': {
                'pax': None,
                'revenue': None
            }
        }
    data = list(crsr)
    if len(data) > 0:
        return data
    else:
        return {
            'date0': {
                'pax': None,
                'revenue': None
            }
        }


if __name__ == '__main__':
    st = time.time()
    data_ = get_host_fares_df(pos="CMB",
                origin="CMB",
                destination="DXB",
                compartment="Y",
                # dates_list=[{'start': '2017-11-01', 'end': '2017-11-30'}],
                extreme_start_date="2017-11-01",
                extreme_end_date="2017-11-30"
                )
    # print data_[['total_fare_1', 'total_fare_2', 'total_fare_3', 'total_fare_4', 'fare']]

    print "Time taken for complete program = ", time.time() - st, " seconds."
    # mrkt_shr_dF = get_mrkt_share_df(mrkt_shr_qry)
    # print mrkt_shr_dF
    # print "got market share df in ", time.time() - st, " seconds."

    # print "No of Web Fares", len(data_web)
    # print json.dumps(data_web, indent=1)
    # print data_web.head()
    # print "GDS********************************************************************************************"
    # data_gds = main(pos='DXB',
    #                 origin='DXB',
    #                 destination='AMM',
    #                 compartment='Y',
    #                 dep_date_start='2017-05-01',
    #                 dep_date_end='2017-05-31',
    #                 channel='gds')
    # print json.dumps(data_gds, indent=1)
    # print "No of GDS fares", len(data_gds)
    # data = data_gds + data_web
    # print [doc['reco_yield'] for doc in data]
    # print "Total******************************************************************************************"
    # print "No of Total Fares", len(data_web + data_gds)
    # print 'processing time', time.time() - st

    # mrkt_query = build_mrkt_query(
    #     origin='DXB',
    #     destination='AMM',
    #     compartment='Y',
    #     dep_date_start=None,
    #     dep_date_end=None
    # )
    # print get_mrkt_share_df(mrkt_query=mrkt_query)
    # dF = get_rbd_sellups_df(origin='DXB',
    #                         destination='DOH',
    #                         compartment='Y')
    #
    # sellups_dict = defaultdict(dict)
    # for index, row in dF.iterrows():
    #     sellups_dict[row['_id']] = dict(
    #         base=row['base_rbd'],
    #         ow_sellup_abs=row['ow_sellup_abs'],
    #         ow_sellup_perc=row['ow_sellup_perc'],
    #         rt_sellup_abs=row['rt_sellup_abs'],
    #         rt_sellup_perc=row['rt_sellup_perc'],
    #         order=row['order'],
    #         rbd_type=row['rbd_type']
    #     )
    #
    # sellup_val = get_sellup_val(inp_rbd='Y',
    #                             req_rbd='E',
    #                             rbd_type='Strategic',
    #                             oneway_return=2,
    #                             sellups_dict=sellups_dict)
    #
    # print sellup_val
    # mrkt_query = build_mrkt_query('DXB',
    #                               'DOH',
    #                               'Y',
    #                               None,
    #                               None)
    # print get_mrkt_share_df(mrkt_query)
    #
    # print get_rating_df('DXB','DOH')
    # generate_sell_up_matrix(origin='DXB',
    #                         destination='DOH',
    #                         compartment='Y',
    #                         dep_date_start=None,
    #                         dep_date_end=None)

    # sellups_dF = get_rbd_sellups_df(origin='DXB',
    #                                 destination='DOH',
    #                                 compartment='Y')
    # print sellups_dF
    #
    # sellups_dict = defaultdict(dict)
    # for index, row in sellups_dF.iterrows():
    #     sellups_dict[row['_id']] = dict(
    #         base=row['base_rbd'],
    #         ow_sellup_abs=row['ow_sellup_abs'],
    #         ow_sellup_perc=row['ow_sellup_perc'],
    #         rt_sellup_abs=row['rt_sellup_abs'],
    #         rt_sellup_perc=row['rt_sellup_perc'],
    #         rbd_type = row['rbd_type'],
    #         order=row['order']
    #     )
    # import json
    # print json.dumps(dict(sellups_dict), indent=1)
    #
    # print get_sellup_val(inp_rbd='L',
    #                      req_rbd='Q',
    #                      rbd_type='Tactical',
    #                      oneway_return=1,
    #                      sellups_dict=dict(sellups_dict))

"""
fares_dF.loc[:, 'recommended_fare'] = np.nan


    for index, row in temp_fares_dF.iterrows():
        if row['oneway_return'] == 1:
            rbds_accepted_ow.append(row['RBD'])
        elif row['oneway_return'] == 2:
            rbds_accepted_rt.append(row['RBD'])
        for idx, row_ in fares_dF.iterrows():
            if row['fare_basis'] == row_['fare_basis']:
                fares_dF.set_value(idx, 'recommended_fare', row['recommended_fare'])

    ow_fares_dF = fares_dF[fares_dF['oneway_return'] == 1]
    for idx, row in ow_fares_dF.iterrows():

    print 'OW', len(ow_fares_dF)
    rt_fares_dF = fares_dF[fares_dF['oneway_return'] == 2]
    print 'RT', len(rt_fares_dF)

    print ow_fares_dF
    print rt_fares_dF
    print fares_dF.filter(items=['fare_basis', 'recommended_fare', 'total_fare', 'taxes'])
    # print 'HOST FARES'
    # print fares_dF
    # print 'COMP FARES'
    # print comp_fares_dF
    # print "MRKT SHR DATA"
    # print mrkt_shr_dF
    # print 'RATINGS DATA'
    # print ratings_dF
    # print 'RBD sellups'
    # print sellups_dF
    # print 'Calculation of Recommendations dF',
    # print temp_fares_dF
    # print 'Competitor Farebasis dF'
    # print temp_fares_dF.competitor_data
"""
"""
    # temp_fares_dF['farebasis'] = temp_fares_dF.fare_basis
    # if row['oneway_return'] == 1 or row['oneway_return'] == 3:
    #     ow_rbds_recommended.append((row['RBD'], row['order'], row['base_rbd'], row['rbd_type'], reco_fare))
    # elif row['oneway_return'] == 2:
    #     rt_rbds_recommended.append((row['RBD'], row['order'], row['base_rbd'], row['rbd_type'], reco_fare))
print 'Temp Fares'
print temp_fares_dF.filter(items=['farebasis','total_fare','competitor_data','recommended_fare','footnote','Rule_id', 'currency'])
temp_DF = temp_fares_dF.filter(items=[
    'dep_date_from',
    'dep_date_to',
    'compartment'
    'fare_basis',
    'effective_from',
    'effective_to',
    'sale_date_from',
    'sale_date_to',
    'Rule_id',
    'competitor_data'
    'oneway_return',
    'footnote',
    'last_ticketed_date',
    'currency',
    'fare',
    'YQ',
    'surcharge',
    'taxes',
    'total_fare',
    'recommended_fare'
])
print temp_DF
print temp_fares_dF.filter(items=['farebasis','total_fare','footnote','Rule_id','competitor_data']).to_dict('records')
print temp_fares_dF['competitor_data']
writer = pd.ExcelWriter(str(origin) + str(destination)+'.xlsx')
temp_fares_dF.filter(items=['competitor_data','fare_basis','total_fare','recommended_fare','footnote','Rule_id']).to_excel(excel_writer=writer)

writer.save()
print temp_fares_dF['competitor_data']
print 'PRINTING JSON ...............................'
# print temp_fares_dF.to_json()
# print temp_fares_dF.competitor_data[1]

print temp_fares_dF.rt_sellup_perc
temp_fares_dF.ow_sellup_perc.fillna(0, inplace=True)
temp_fares_dF.rt_sellup_perc.fillna(0, inplace=True)
temp_fares_dF.ow_sellup_abs.fillna(0, inplace=True)
temp_fares_dF.ow_sellup_abs.fillna(0, inplace=True)
print temp_fares_dF.rt_sellup_perc
print temp_fares_dF.head()
ow_df = temp_fares_dF[temp_fares_dF['oneway_return'] == 1]
ow_lst_dicts = ow_df.to_dict('records')
print 'OW fares Accepted'
print ow_lst_dicts
rt_df = temp_fares_dF[temp_fares_dF['oneway_return'] == 2]
rt_lst_dicts = rt_df.to_dict('records')
print 'RT fares Accepted'
print rt_lst_dicts


# Creation of a sellup Dict with rbd as key and the rest as values
sellups_dict = defaultdict(dict)
for index, row in sellups_dF.iterrows():
    sellups_dict[row['rbd']] = dict(
        base=row['base_rbd'],
        ow_sellup_abs=row['ow_sellup_abs'],
        ow_sellup_perc=row['ow_sellup_perc'],
        rt_sellup_abs=row['rt_sellup_abs'],
        rt_sellup_perc=row['rt_sellup_perc'],
        order=row['order'],
        rbd_type=row['rbd_type']
    )


ow_rbds_recommended.sort()
print 'OW Recommended Fares', ow_rbds_recommended
rt_rbds_recommended.sort()
print 'RT Recommended Fares', rt_rbds_recommended
for idx, val in enumerate(ow_rbds_recommended):
    for index, row in fares_dF.iterrows():
        if row['oneway_return'] == 1 or row['oneway_return'] == 3:
            if row['RBD'] == val[0]:
                fares_dF.set_value(index, 'recommended_fare', val[4])
            elif idx == 0:
                print 'RBD Type', row['RBD_type'], val[3]
                if row['RBD_type'] == val[3]:
                    print 'SELL UP Request SENT', val[0], row['RBD'], '1', val[3]
                    sellup = get_sellup_val(inp_rbd=val[0],
                                            req_rbd=row['RBD'],
                                            sellups_dict=dict(sellups_dict),
                                            oneway_return=1,
                                            rbd_type=val[3])
                    print 'SellUps', sellup
                    if type(sellup['ow']['abs']) in [int, float, long]:
                        fare = val[4] + sellup['ow']['abs']
                        fares_dF.set_value(index, 'recommended_fare', fare)
                    elif type(sellup['ow']['perc']) in [int, float, long]:
                        fare = val[4] * (1 + sellup['ow']['perc'])
                        fares_dF.set_value(index, 'recommended_fare', fare)
                    else:
                        pass
                else:
                    pass
            else:
                if row['order'] > val[1]:
                    print 'RBD Type', row['RBD_type'], val[3]
                    if row['RBD_type'] == val[3]:
                        print 'SELL UP Request SENT', val[0], row['RBD'], '2', val[3]
                        sellup = get_sellup_val(inp_rbd=val[0],
                                                req_rbd=row['RBD'],
                                                sellups_dict=dict(sellups_dict),
                                                oneway_return=1,
                                                rbd_type=val[3])
                        if type(sellup['ow']['abs']) in [int, float, long]:
                            fare = val[4] + sellup['ow']['abs']
                            fares_dF.set_value(index, 'recommended_fare', fare)
                        elif type(sellup['ow']['perc']) in [int, float, long]:
                            fare = val[4] * (1 + sellup['ow']['perc'])
                            fares_dF.set_value(index, 'recommended_fare', fare)
                        else:
                            pass
                    else:
                        pass
                else:
                    pass

for idx, val in enumerate(rt_rbds_recommended):
    for index, row in fares_dF.iterrows():
        if row['oneway_return'] == 1 or row['oneway_return'] == 3:
            if row['RBD'] == val[0]:
                fares_dF.set_value(index, 'recommended_fare', val[4])
            elif idx == 0:
                if row['RBD_type'] == val[3]:
                    sellup = get_sellup_val(inp_rbd=val[0],
                                            req_rbd=row['RBD'],
                                            sellups_dict=dict(sellups_dict),
                                            oneway_return=1,
                                            rbd_type=val[3])
                    if type(sellup['rt']['abs']) in [int, float]:
                        fare = val[4] + sellup['rt']['abs']
                        fares_dF.set_value(index, 'recommended_fare', fare)
                    elif type(sellup['rt']['perc']) in [int, float]:
                        fare = val[4] * (1 + sellup['rt']['perc'])
                        fares_dF.set_value(index, 'recommended_fare', fare)
                    else:
                        pass
                else:
                    pass
            else:
                if row['order'] > val[1]:
                    if row['RBD_type'] == val[3]:
                        sellup = get_sellup_val(inp_rbd=val[0],
                                                req_rbd=row['RBD'],
                                                sellups_dict=dict(sellups_dict),
                                                oneway_return=1,
                                                rbd_type=val[3])
                        if type(sellup['rt']['abs']) in [int, float]:
                            fare = val[4] + sellup['rt']['abs']
                            fares_dF.set_value(index, 'recommended_fare', fare)
                        elif type(sellup['rt']['perc']) in [int, float]:
                            fare = val[4] * (1 + sellup['rt']['perc'])
                            fares_dF.set_value(index, 'recommended_fare', fare)
                        else:
                            pass
                    else:
                        pass
                else:
                    pass

print fares_dF.filter(items=['recommended_fare', 'fare_basis', 'RBD_type'])
print sellups_dF
fares_dF.recommended_fare_base = fares_dF.recommended_fare - fares_dF.YQ - fares_dF.surcharge
fares_dF.recommended_fare_total = fares_dF.recommended_fare + fares_dF.taxes
response_dF = fares_dF.filter(items=[
    'dep_date_from',
    'dep_date_to',
    'fare_basis',
    'effective_from',
    'effective_to',
    'sale_date_from',
    'sale_date_to',
    'Rule_id',
    'oneway_return',
    'footnote',
    'last_ticketed_date',
    'currency',
    'fare',
    'YQ',
    'surcharge',
    'taxes',
    'total_fare',
    'recommended_fare',
    'recommended_fare_base',
    'recommended_fare_total'
])
# fares_dF.drop([
#     u'combine_faretype',
#     u'combine_pos_od_comp_fb',
#     u'dep_date_from_UTC',
#     u'dep_date_to_UTC',
# ])
# fares_dF.index.str.encode()
# print list(fares_dF)
print response_dF.head()
return response_dF.to_dict('records')
"""
