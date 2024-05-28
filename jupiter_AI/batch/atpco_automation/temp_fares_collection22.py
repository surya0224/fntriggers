import time
from jupiter_AI import today, SYSTEM_DATE, mongo_client, JUPITER_DB, Host_Airline_Code, Host_Airline_Hub, JUPITER_LOGGER
from jupiter_AI.logutils import measure
import datetime


@measure(JUPITER_LOGGER)
def create_temp_fares_collection(client):
    db = client[JUPITER_DB]
    db.temp_fares_triggers.remove({})
    print 'Removed fares from temp fares triggers'
    st = time.time()
    SYSTEM_DATE_LW = datetime.datetime.strftime(today - datetime.timedelta(days=7), '%Y-%m-%d')
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

    print 'Creating temp fares triggers'
    db.JUP_DB_ATPCO_Fares_Rules.aggregate([
        {
            '$match': query
        },
        {
            '$project': {'_id': 0,
                         'Average_surcharge': 1,
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
        },
        {
            '$out': 'temp_fares_triggers'
        }
    ])
    print 'Created in ', time.time() - st


if __name__ == "__main__":
    client = mongo_client()
    create_temp_fares_collection(client)
    client.close()