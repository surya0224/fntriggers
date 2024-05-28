import time
import numpy as np
import datetime

from jupiter_AI import RABBITMQ_HOST, RABBITMQ_PASSWORD, RABBITMQ_PORT, RABBITMQ_USERNAME, client, JUPITER_DB,\
    today, SYSTEM_DATE, Host_Airline_Code, Host_Airline_Hub, JUPITER_LOGGER
from jupiter_AI.logutils import measure
from jupiter_AI.batch.atpco_automation.Automation_tasks import insert_host_fares
from jupiter_AI.triggers.workflow_mrkt_level_update import get_dep_date_filters
import pandas as pd
from celery import group
from jupiter_AI.triggers.common import pos_level_currency_data as get_currency_data, cursor_to_df

url = 'amqp://' + RABBITMQ_USERNAME + \
        ":" + RABBITMQ_PASSWORD + \
        "@" + RABBITMQ_HOST + ":" + str(RABBITMQ_PORT) + "/"

db = client[JUPITER_DB]
fares_temp_collection = 'uniquefootnote'

## To add later

# cache = {}
# redis cache
#
# def get_collection_from_cache(collection_name):
#     if not cache.has_key(collection_name):
#         cache[collection_name] = db.collection.find({})
#
#     return cache.get(collection_name)


@measure(JUPITER_LOGGER)
def get_fares_query(ods, currencies, extreme_start_date, extreme_end_date):
    SYSTEM_DATE_LW = datetime.datetime.strftime(today - datetime.timedelta(days=7), '%Y-%m-%d')
    extreme_start_date_n = "0" + extreme_start_date[2:4] + extreme_start_date[5:7] + extreme_start_date[8:10]
    extreme_end_date_n = "0" + extreme_end_date[2:4] + extreme_end_date[5:7] + extreme_end_date[8:10]
    extreme_start_date_obj = datetime.datetime.strptime(extreme_start_date, "%Y-%m-%d")
    extreme_start_date_lw = extreme_start_date_obj - datetime.timedelta(days=7)
    extreme_start_date_lw = extreme_start_date_lw.strftime("%Y-%m-%d")
    extreme_start_date_n_lw = "0" + extreme_start_date_lw[2:4] + extreme_start_date_lw[5:7] + \
                              extreme_start_date_lw[8:10]
    SYSTEM_DATE_MOD = "0" + SYSTEM_DATE[2:4] + SYSTEM_DATE[5:7] + SYSTEM_DATE[8:10]
    SYSTEM_DATE_MOD_LW = "0" + SYSTEM_DATE_LW[2:4] + SYSTEM_DATE_LW[5:7] + SYSTEM_DATE_LW[8:10]

    query = {
        "$and": [
            {
                "carrier": {"$in": [Host_Airline_Code]}
            },
            {
                "channel": {'$ne': None}
            },
            {
                "fare_include": True
            },
            {
                "currency": {
                    "$in": currencies
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

    return query


@measure(JUPITER_LOGGER)
def parallelize_host_fares():
    st = time.time()

    pos_done = []
    today_triggers = list(db.Temp_fzDB_tbl_001.aggregate([
        {
            '$addFields':
                {
                    'pos': {'$substr': ['$market', 0, 3]},
                    'od': {'$substr': ['$market', 3, 6]},
                    'compartment': {'$substr': ['$market', 9, 1]}
                }
        },
            {
            "$group": {
                "_id": {
                    "pos": "$pos",
                    "dep_date_start": "$dep_date_start",
                    "dep_date_end": "$dep_date_end"
                }, "ods": {
                    "$addToSet": "$od"
                }
            }

        }, {
            "$project":{
                "pos": "$_id.pos",
                "dep_date_start": "$_id.dep_date_start",
                "dep_date_end": "$_id.dep_date_end",
                "ods": 1
            }
        }
    ]))

    triggers_df = pd.DataFrame(today_triggers)
    od_list = triggers_df['ods'].tolist()
    flatten = lambda l: [item for sublist in l for item in sublist]
    od_list = flatten(od_list)
    od_list = list(set(od_list))
    do_list = list(map(lambda x: '{}{}'.format(x[3:], x[:3]), od_list))
    ods = list(set(od_list + do_list))
    print "Got Trigger ODs"

    pos_list = triggers_df['pos'].unique()
    curr_crsr = db.JUP_DB_Pos_Currency_Master.find({}, {'pos': 1, 'gds': 1, 'web': 1})
    curr_df = pd.DataFrame(list(curr_crsr))
    curr_df.fillna('', inplace=True)
    currencies = list(set(curr_df['web'].tolist() + curr_df['gds'].tolist()))
    print 'Got trigger currencies'

    date_ranges = get_dep_date_filters()
    print 'Got Date Ranges'

    currency_data = get_currency_data()
    print 'Got currency_data'

    start_dates = []
    end_dates = []
    for date in date_ranges:
        start_dates.append(date['start'])
        end_dates.append(date['end'])

    # Appending triggers dates
    trigger_start_dates = triggers_df['dep_date_start'].tolist()
    trigger_end_dates = triggers_df['dep_date_end'].tolist()

    start_dates = list(set(start_dates + trigger_start_dates))
    end_dates = list(set(end_dates + trigger_end_dates))

    extreme_start_date = min(start_dates)
    extreme_end_date = max(end_dates)

    query = get_fares_query(ods, currencies, extreme_start_date, extreme_end_date)

    # Inserting today's fares in temp collection

    cur = db.JUP_DB_ATPCO_Fares_Rules.find(
        query,
        {'Average_surcharge': 1,
         'Footnotes': 1,
         'OD': 1,
         'RBD': 1,
         'RBD_type': 1,
         'YQ': 1,
         'YR': 1,
         'batch': 1,
         'carrier': 1,
         'cat_12': 1,
         'cat_14': 1,
         'cat_15': 1,
         'cat_2': 1,
         'cat_4': 1,
         'channel': 1,
         'compartment': 1,
         'competitor_farebasis': 1,
         'currency': 1,
         'day_of_week': 1,
         'dep_date_from': 1,
         'dep_date_to': 1,
         'destination': 1,
         'effective_from': 1,
         'effective_to': 1,
         'fare': 1,
         'fare_basis': 1,
         'fare_brand': 1,
         'fare_include': 1,
         'fare_rule': 1,
         'flight_number': 1,
         'footnote': 1,
         'gfs': 1,
         'last_update_date': 1,
         'last_update_time': 1,
         'oneway_return': 1,
         'origin': 1,
         'private_fare': 1,
         'retention_fare': 1,
         'rtg': 1,
         'sale_date_from': 1,
         'sale_date_to': 1,
         'surcharge_amount_1': 1,
         'surcharge_amount_2': 1,
         'surcharge_amount_3': 1,
         'surcharge_amount_4': 1,
         'surcharge_date_end_1': 1,
         'surcharge_date_end_2': 1,
         'surcharge_date_end_3': 1,
         'surcharge_date_end_4': 1,
         'surcharge_date_start_1': 1,
         'surcharge_date_start_2': 1,
         'surcharge_date_start_3': 1,
         'surcharge_date_start_4': 1,
         'tariff_code': 1,
         'taxes': 1,
         'total_fare': 1,
         'total_fare_1': 1,
         'total_fare_2': 1,
         'total_fare_3': 1,
         'total_fare_4': 1,
         'travel_date_from': 1,
         'travel_date_to': 1
         }
    )
    #
    # fares_temp_collection_count = db[fares_temp_collection].find().count
    # fares_temp_collection_distinct_dates = db[fares_temp_collection].distinct('last_update_date')
    #
    # if fares_temp_collection_count == cur.count() and fares_temp_collection_distinct_dates[0] == SYSTEM_DATE \
    #         and len(fares_temp_collection_distinct_dates) == 1:
    #
    #     db.some_temp_collection.remove({})
    #
    #     print "Removed temp fares collection"
    #
    #     bulk = list()
    #     count = 0
    #     t = 0
    #     for i in cur:
    #         i['last_update_date'] = SYSTEM_DATE
    #         if t == 1000:
    #             db[fares_temp_collection].insert_many(bulk)
    #             print 'Fares inserted:', count
    #             bulk = list()
    #             bulk.append(i)
    #             t = 1
    #         else:
    #             bulk.append(i)
    #             t += 1
    #         count += 1
    #     if t > 0:
    #         db[fares_temp_collection].insert_many(bulk)
    #         print 'Fares inserted:', count
    #
    # else:
    #     print 'Same fares already present in temp fares collection'

    # tax_crsr = list(db.JUP_DB_Tax_Master.find({}, {'_id': 0}))
    # tax_df = pd.DataFrame(tax_crsr)
    # tax_df['Total'].fillna(0)
    # tax_currencies = list(tax_df['Currency'].unique())
    # tax_df['OD'] = tax_df['Origin'] + tax_df['Destination']
    # tax_df = tax_df.rename(columns={'Currency': 'tax_currency', 'OW/RT': 'oneway_return',
    #                                 'Total': 'tax_value_from_master'})
    # tax_df.drop(['Origin', 'Destination'], axis=1, inplace=True)
    # tax_df['oneway_return'] = tax_df['oneway_return'].apply(str)
    # tax_df = tax_df.rename(columns={"Compartment": "compartment"})
    # print 'Got tax data'

    exchange_rate = {}
    currency_crsr = list(db.JUP_DB_Exchange_Rate.find({}))
    for curr in currency_crsr:
        exchange_rate[curr['code']] = curr['Reference_Rate']
    print 'Got exchange_rate data'

    ### Change this later
    
    # fares_cur = db.uniquefootnote.find({})
    # cols = [
    #     "effective_from",
    #     "effective_to",
    #     "dep_date_from",
    #     "dep_date_to",
    #     "sale_date_from",
    #     "sale_date_to",
    #     "fare_basis",
    #     "fare_brand",
    #     "RBD",
    #     "rtg",
    #     "tariff_code",
    #     "fare_include",
    #     "private_fare",
    #     "footnote",
    #     "batch",
    #     "origin",
    #     "destination",
    #     "OD",
    #     "compartment",
    #     "oneway_return",
    #     "channel",
    #     "carrier",
    #     "fare",
    #     "surcharge_date_start_1",
    #     "surcharge_date_start_2",
    #     "surcharge_date_start_3",
    #     "surcharge_date_start_4",
    #     "surcharge_date_end_1",
    #     "surcharge_date_end_2",
    #     "surcharge_date_end_3",
    #     "surcharge_date_end_4",
    #     "surcharge_amount_1",
    #     "surcharge_amount_2",
    #     "surcharge_amount_3",
    #     "surcharge_amount_4",
    #     "Average_surcharge",
    #     "total_fare_1",
    #     "total_fare_2",
    #     "total_fare_3",
    #     "total_fare_4",
    #     "total_fare",
    #     "YR",
    #     "YQ",
    #     "taxes",
    #     "currency",
    #     "fare_rule",
    #     "RBD_type",
    #     "gfs",
    #     "last_update_date",
    #     "last_update_time",
    #     "competitor_farebasis",
    #     "travel_date_to",
    #     "travel_date_from",
    #     "retention_fare",
    #     "flight_number",
    #     "day_of_week"
    # ]
    #
    # df = pd.DataFrame(columns=cols)
    # dF = cursor_to_df(fares_cur, 10000)
    #
    # dF['flight_number'] = "--"
    # dF['day_of_week'] = "--"
    #
    # for i in range(len(dF)):
    #     has_category = False
    #     has_category_15 = False
    #     tvl_dates = []
    #     sale_dates = []
    #     try:
    #         dates_array = dF['Footnotes'][i]['Cat_14_FN']
    #         has_category = True
    #     except KeyError:
    #         try:
    #             dates_array = dF['cat_14'][i]
    #             if dates_array != -999:
    #                 has_category = True
    #             else:
    #                 dF.loc[i, 'travel_date_from'] = "1900-01-01"
    #                 dF.loc[i, 'travel_date_to'] = "2099-12-31"
    #
    #         except KeyError:
    #             dF.loc[i, 'travel_date_from'] = "1900-01-01"
    #             dF.loc[i, 'travel_date_to'] = "2099-12-31"
    #     except TypeError:
    #         try:
    #             dates_array = dF['cat_14'][i]
    #             if dates_array != -999:
    #                 has_category = True
    #             else:
    #                 dF.loc[i, 'travel_date_from'] = "1900-01-01"
    #                 dF.loc[i, 'travel_date_to'] = "2099-12-31"
    #         except KeyError:
    #             dF.loc[i, 'travel_date_from'] = "1900-01-01"
    #             dF.loc[i, 'travel_date_to'] = "2099-12-31"
    #     if has_category:
    #         for entry in dates_array:
    #             tvl_dates.append(
    #                 str(entry['TRAVEL_DATES_COMM']) + str(entry['TRAVEL_DATES_EXP']))
    #         min_indx = np.argmin(tvl_dates)
    #         from_date = str(dates_array[min_indx]['TRAVEL_DATES_COMM'])
    #         to_date = str(dates_array[min_indx]['TRAVEL_DATES_EXP'])
    #         if len(from_date) == 7:
    #             from_date = from_date[1:]
    #         if len(to_date) == 7:
    #             to_date = to_date[1:]
    #         try:
    #             from_date = datetime.datetime.strftime(
    #                 datetime.datetime.strptime(
    #                     from_date, "%y%m%d"), "%Y-%m-%d")
    #         except ValueError:
    #             from_date = "1900-01-01"
    #         try:
    #             to_date = datetime.datetime.strftime(
    #                 datetime.datetime.strptime(
    #                     to_date, "%y%m%d"), "%Y-%m-%d")
    #         except ValueError:
    #             to_date = "2099-12-31"
    #         dF.loc[i, 'travel_date_from'] = from_date
    #         dF.loc[i, 'travel_date_to'] = to_date
    #     # except KeyError:
    #     #     dF.loc[i, 'travel_date_from'] = "1900-01-01"
    #     #     dF.loc[i, 'travel_date_to'] = "2099-12-31"
    #
    #     # except TypeError:
    #     #     dF.loc[i, 'travel_date_from'] = "1900-01-01"
    #     #     dF.loc[i, 'travel_date_to'] = "2099-12-31"
    #     try:
    #         dates_array_15 = dF['Footnotes'][i]['Cat_15_FN']
    #         has_category_15 = True
    #     except KeyError:
    #         try:
    #             dates_array_15 = dF['cat_15'][i]
    #             if dates_array_15 != -999:
    #                 has_category_15 = True
    #             else:
    #                 dF.loc[i, 'sale_date_from'] = "1900-01-01"
    #                 dF.loc[i, 'sale_date_to'] = "2099-12-31"
    #         except KeyError:
    #             dF.loc[i, 'sale_date_from'] = "1900-01-01"
    #             dF.loc[i, 'sale_date_to'] = "2099-12-31"
    #     except TypeError:
    #         try:
    #             dates_array_15 = dF['cat_15'][i]
    #             if dates_array_15 != -999:
    #                 has_category_15 = True
    #             else:
    #                 dF.loc[i, 'sale_date_from'] = "1900-01-01"
    #                 dF.loc[i, 'sale_date_to'] = "2099-12-31"
    #         except KeyError:
    #             dF.loc[i, 'sale_date_from'] = "1900-01-01"
    #             dF.loc[i, 'sale_date_to'] = "2099-12-31"
    #     if has_category_15:
    #         for entry in dates_array_15:
    #             sale_dates.append(
    #                 str(entry['SALE_DATES_EARLIEST_TKTG']) + str(entry['SALE_DATES_LATEST_TKTG']))
    #         min_indx = np.argmin(sale_dates)
    #         from_date = str(
    #             dates_array_15[min_indx]['SALE_DATES_EARLIEST_TKTG'])
    #         to_date = str(
    #             dates_array_15[min_indx]['SALE_DATES_LATEST_TKTG'])
    #         if len(from_date) == 7:
    #             from_date = from_date[1:]
    #         if len(to_date) == 7:
    #             to_date = to_date[1:]
    #         try:
    #             from_date = datetime.datetime.strftime(
    #                 datetime.datetime.strptime(
    #                     from_date, "%y%m%d"), "%Y-%m-%d")
    #         except ValueError:
    #             from_date = "1900-01-01"
    #         try:
    #             to_date = datetime.datetime.strftime(
    #                 datetime.datetime.strptime(
    #                     to_date, "%y%m%d"), "%Y-%m-%d")
    #         except ValueError:
    #             to_date = "2099-12-31"
    #         dF.loc[i, 'sale_date_from'] = from_date
    #         dF.loc[i, 'sale_date_to'] = to_date
    #     # except KeyError:
    #     #     dF.loc[i, 'sale_date_from'] = "1900-01-01"
    #     #     dF.loc[i, 'sale_date_to'] = "2099-12-31"
    #     # except TypeError:
    #     #     dF.loc[i, 'sale_date_from'] = "1900-01-01"
    #     #     dF.loc[i, 'sale_date_to'] = "2099-12-31"
    #     try:
    #         cat_4 = dF.loc[i, 'cat_4']
    #         temp = ""
    #         for element in cat_4:
    #             try:
    #                 temp_st = element['CXR_FLT']['SEGS_CARRIER']
    #                 if len(temp_st) % 14 != 0:
    #                     temp_st = temp_st + "    "
    #                 for i in range(0, len(temp_st), 14):
    #                     seg = temp_st[i:i + 14]
    #                     op_cxr = seg[0:2]
    #                     mrkt_cxr = seg[3:5]
    #                     range_start = seg[6:10]
    #                     range_end = seg[10:]
    #                     if op_cxr == "  ":
    #                         op_cxr = "--"
    #                     if mrkt_cxr == "  ":
    #                         mrkt_cxr = "--"
    #                     if ((range_start == "****") or (range_end == "****")) and (
    #                             (op_cxr == "FZ") or (mrkt_cxr == "FZ")):
    #                         temp = temp + op_cxr + " " + mrkt_cxr + " " + "****, "
    #                     elif (op_cxr == "FZ") or (mrkt_cxr == "FZ"):
    #                         temp = temp + op_cxr + " " + mrkt_cxr + " " + range_start + "-" + range_end + ", "
    #             except KeyError:
    #                 pass
    #
    #             for i in range(3):
    #                 key_name_cxr = "CXR" + str(i + 1)
    #                 key_name_flt = "FLT_NO" + str(i + 1)
    #                 if element[key_name_cxr] == "FZ":
    #                     temp = temp + element[key_name_cxr] + " " + str(element[key_name_flt]) + ", "
    #
    #         dF.loc[i, 'flight_number'] = temp[:-2]
    #     except KeyError:
    #         dF.loc[i, 'flight_number'] = "--"
    #     except TypeError:
    #         dF.loc[i, 'flight_number'] = "--"
    #
    #     try:
    #         cat_2 = dF.loc[i, 'cat_2']
    #         unique_list = []
    #         temp = ""
    #         for element in cat_2:
    #             for wkday in element['DAYOFWEEK']:
    #                 if wkday not in unique_list:
    #                     unique_list.append(wkday)
    #                     temp = temp + wkday + ", "
    #         dF.loc[i, 'day_of_week'] = temp[:-2]
    #     except KeyError:
    #         dF.loc[i, 'day_of_week'] = "--"
    #     except TypeError:
    #         dF.loc[i, 'day_of_week'] = "--"
    #
    # ####
    #
    # # fares_cur = db.some_temp_coll.find({})
    # # cols = [
    # #     "effective_from",
    # #     "effective_to",
    # #     "dep_date_from",
    # #     "dep_date_to",
    # #     "sale_date_from",
    # #     "sale_date_to",
    # #     "fare_basis",
    # #     "fare_brand",
    # #     "RBD",
    # #     "rtg",
    # #     "tariff_code",
    # #     "fare_include",
    # #     "private_fare",
    # #     "footnote",
    # #     "batch",
    # #     "origin",
    # #     "destination",
    # #     "OD",
    # #     "compartment",
    # #     "oneway_return",
    # #     "channel",
    # #     "carrier",
    # #     "fare",
    # #     "surcharge_date_start_1",
    # #     "surcharge_date_start_2",
    # #     "surcharge_date_start_3",
    # #     "surcharge_date_start_4",
    # #     "surcharge_date_end_1",
    # #     "surcharge_date_end_2",
    # #     "surcharge_date_end_3",
    # #     "surcharge_date_end_4",
    # #     "surcharge_amount_1",
    # #     "surcharge_amount_2",
    # #     "surcharge_amount_3",
    # #     "surcharge_amount_4",
    # #     "Average_surcharge",
    # #     "total_fare_1",
    # #     "total_fare_2",
    # #     "total_fare_3",
    # #     "total_fare_4",
    # #     "total_fare",
    # #     "YR",
    # #     "YQ",
    # #     "taxes",
    # #     "currency",
    # #     "fare_rule",
    # #     "RBD_type",
    # #     "gfs",
    # #     "last_update_date",
    # #     "last_update_time",
    # #     "competitor_farebasis",
    # #     "travel_date_to",
    # #     "travel_date_from",
    # #     "retention_fare",
    # #     "flight_number",
    # #     "day_of_week"
    # # ]
    # # fares_data = pd.DataFrame(columns=cols)
    # # fares_data = cursor_to_df(fares_cur, 10000)
    #
    # dF.fillna(-999, inplace=True)
    # dF['surcharge_amount_1'] = 0
    # dF['surcharge_amount_2'] = 0
    # dF['surcharge_amount_3'] = 0
    # dF['surcharge_amount_4'] = 0
    #
    # dF['surcharge_date_start_1'] = "NA"
    # dF['surcharge_date_start_2'] = "NA"
    # dF['surcharge_date_start_3'] = "NA"
    # dF['surcharge_date_start_4'] = "NA"
    #
    # dF['surcharge_date_end_1'] = "NA"
    # dF['surcharge_date_end_2'] = "NA"
    # dF['surcharge_date_end_3'] = "NA"
    # dF['surcharge_date_end_4'] = "NA"

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

    print 'time taken for pre-processing:', time.time() - st
    st1 = time.time()
    market_grp = []
    for doc in today_triggers:
        od = doc['origin']+doc['destination']
        do = doc['destination']+doc['origin']
        start_dates = []
        end_dates = []
        for date in date_ranges:
            start_dates.append(date['start'])
            end_dates.append(date['end'])

        # st_ = time.time()
        extreme_start_date = min(start_dates)
        extreme_end_date = max(end_dates)
        market_grp.append(insert_host_fares.s(pos=doc['pos'],
                           dep_date_start=extreme_start_date,
                           dep_date_end=extreme_end_date,
                           od_list=[od],
                           do_list=[do],
                           compartment=doc['compartment'],
                           currency_data=currency_data,
                            date_ranges=date_ranges,
                           exchange_rate=exchange_rate,
                           ))
        # print 'Got fares for ', od, ' in ', time.time() - st
    market_group = group(market_grp)
    print "Created group of all triggers"
    res = market_group()
    out = res.get()
    print 'Time taken:', time.time() - st1


if __name__ == "__main__":
    get_fares_query(['CMBDXB'], ['AED'])