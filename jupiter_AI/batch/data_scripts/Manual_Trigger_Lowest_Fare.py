from copy import deepcopy
import json
import pymongo
import collections
import pandas as pd
import numpy as np
import global_variable as var
import datetime
import time
from jupiter_AI import Host_Airline_Hub, Host_Airline_Code, JUPITER_LOGGER
from jupiter_AI.logutils import measure
from pymongo.errors import BulkWriteError
from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne
from datetime import datetime
try:
    fzDBConn=pymongo.MongoClient(var.mongo_client_url)[var.derivedDatabase]
    fzDBConn.authenticate(var.userName, var.password, source=var.authDatabase)
    print('connected')

    
except Exception as e:
    #sys.stderr.write("Could not connect to MongoDB: %s" % e)
    print("Could not connect to MongoDB: %s" % e)


# In[45]:
dateformat = '%Y-%m-%d'
SYS_SNAP_DATE = datetime.strftime(datetime.now(), dateformat)    


currency_cursor = fzDBConn.JUP_DB_Pos_Currency_Master.find()
currencyPos_dict = dict()
for currency_cursors in currency_cursor:
    currencyPos_dict[currency_cursors['pos']] = {'web':currency_cursors['web'],'gds':currency_cursors['gds']}
defaultCurrency = "AED"



@measure(JUPITER_LOGGER)
def convert_currency(value, from_code, to_code='AED'):
    """
    :param from_code:currency code of value
    :param to_code:currency code into which value needs to be converted
    :param value:the value in from_code to be converted to to_code
    :return:value in to_code currency
    """
    if from_code == to_code:
        return value
    else:
        cursor = fzDBConn.JUP_DB_Exchange_Rate.find({'code': {'$in': [from_code, to_code]}})
        # print cursor.count()
        if cursor.count() == 0:
            e1 = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                        get_module_name(),
                                        get_arg_lists(inspect.currentframe()))
            e1.append_to_error_list('Exchange Rates Not Available for '+from_code+' to '+to_code)
            raise e1
        if from_code == 'AED':
            if cursor.count() > 0:
                for currency_doc in cursor:
                    if currency_doc['code'] != 'AED':
                        return float(value)/currency_doc['Reference_Rate']
        elif to_code == 'AED':
            if cursor.count() > 0:
                for currency_doc in cursor:
                    if currency_doc['code'] != 'AED':
                        return float(value)*currency_doc['Reference_Rate']
        else:
            if cursor.count() == 2:
                if cursor[0]['Code'] == from_code:
                    ratio_2to1 = cursor[1]['Reference_Rate'] / cursor[0]['Reference_Rate']
                else:
                    ratio_2to1 = cursor[0]['Reference_Rate'] / cursor[1]['Reference_Rate']
                converted_value = float(value) * ratio_2to1
                return converted_value


@measure(JUPITER_LOGGER)
def LFF(pos, origin, destination, compartment, snap_date, dep_date, currency_data, comp=[Host_Airline_Code], channel=None):
    currency = []
    
    ### get the currency based on MT pos
    
    if pos in currency_data:
        if currency_data[pos]['web']:
            currency.append(currency_data[pos]['web'])
    else:
        currency.append(defaultCurrency)

    if pos in currency_data:
        if currency_data[pos]['gds']:
            currency.append(currency_data[pos]['gds'])
    else:
        currency.append(defaultCurrency)
    '''
    if currency_data[pos]['web']:
        currency.append(currency_data[pos]['web'])
    if currency_data[pos]['gds']:
        currency.append(currency_data[pos]['gds'])
    '''
    #print(currency)    
    ### query for both od and do
    city_ap_crsr = fzDBConn.JUP_DB_City_Airport_Mapping.find({"Airport_Code": {"$in": [origin, destination]}},
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
    city_ap_crsr2 = fzDBConn.JUP_DB_City_Airport_Mapping.find({"City_Code": {"$in": [city_ori, city_dest]}},
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
        # "dep_date_from",
        # "dep_date_to",
        # "sale_date_from",
        # "sale_date_to",
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
        # "travel_date_to",
        # "travel_date_from",
        "retention_fare",
        # "flight_number",
        # "day_of_week"
    ]
    
    dF = pd.DataFrame(columns=cols)
    extreme_dep_date = "0" + dep_date[2:4] + dep_date[5:7] + dep_date[8:10]
    extreme_snap_date = "0" + snap_date[2:4] + snap_date[5:7] + snap_date[8:10]
    
    if channel:
        channels = {"$eq": channel}
    else:
        channels = {"$eq": "gds"}
    #print("Aggregating on ATPCO Fares Rules . . . . ")
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
                "LFF_flag": True
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
    fares_data = list(fzDBConn.JUP_DB_ATPCO_Fares_Rules.aggregate(
        [
            {
                '$match': query
            }
        ]
    ))
    
    print("Fares Data length = ", len(fares_data))
    exchange_rate = {}
    # pos_country = list(fzDBConn.JUP_DB_Region_Master.find({"POS_CD": origin}))[0]['COUNTRY_CD']
    # Tax part
    tax_crsr = list(fzDBConn.JUP_DB_Tax_Master.find({
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
    #print(1)
    ### for YR we need country 
    #
    #yr_crsr = list(fzDBConn.JUP_DB_YR_Master.find({"origin_country": pos_country, "Compartment": compartment}))
    #if len(yr_crsr) == 0:
    #    yr_crsr = list(fzDBConn.JUP_DB_YR_Master.find({"origin_country": "MISC"}))
    #yr_curr = yr_crsr[0]['Curr']
    #yr = {}
    #print(2)
    # Implementing YQ from YQ master collection
    #origin_country = list(fzDBConn.JUP_DB_Region_Master.find({"POS_CD": origin}))[0]['COUNTRY_CD']
    #destination_country = list(fzDBConn.JUP_DB_Region_Master.find({"POS_CD": destination}))[0]['COUNTRY_CD']
    #yq_data = list(fzDBConn.JUP_DB_YQ_Master.find({"origin_country":origin_country,"destination_country":destination_country,'compartment': compartment}))
    #yq_df = pd.DataFrame(yq_data)
    #yq_df.drop(['_id', 'compartment'], axis=1, inplace=True)
    #yq_df = yq_df.rename(
    #    columns={
    #        'amount': 'amount_yq',
    #        'currency': 'currency_yq'})
    #yq_currencies = list(yq_df['currency_yq'].unique())
    #yq_df['oneway_return'] = yq_df['oneway_return'].apply(str)
    currency_crsr = list(fzDBConn.JUP_DB_Exchange_Rate.find(
        {"code": {"$in": currency + tax_currencies}}))
    for curr in currency_crsr:
        exchange_rate[curr['code']] = curr['Reference_Rate']
    #for currency_ in currency:
    #    if yr_curr != currency_:
    #        yr_val = (yr_crsr[0]['amount'] /
    #                  exchange_rate[yr_curr]) * exchange_rate[currency_]
    #        yr[currency_] = yr_val
    #    else:
    #        yr[currency_] = yr_crsr[0]['amount']
    #print(3)
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
        dF = dF.merge(tax_df, on=['OD', 'oneway_return'], how='left')
        dF[['tax_currency',
            'tax_value_from_master']] = dF[['tax_currency',
                                            'tax_value_from_master']].fillna(-999)

        dF['taxes'] = dF.apply(lambda row: convert_currency(row['tax_value_from_master'],
                                                                row['currency'],
                                                                row['tax_currency']), axis=1)
        # Merge YQ df and fares df(that has already been merged with tax df) to
        # compute yq
        #dF = dF.merge(yq_df, on=['oneway_return'], how='left')
        #dF['YQ'] = dF.apply(lambda row: row['amount_yq'] /
        #                    exchange_rate[row['currency']] *
        #                    exchange_rate[row['currency_yq']], axis=1)
        #print(len(dF))
        for i in range(len(dF)):
            #print(i)
            has_category = False
            has_category_15 = False
            tvl_dates = []
            sale_dates = []
            surchrg_amts = []
            # try:
            #     dates_array = dF['Footnotes'][i]['Cat_14_FN']
            #     has_category = True
            # except KeyError:
            #     try:
            #         dates_array = dF['cat_14'][i]
            #         if dates_array != -999:
            #             has_category = True
            #         else:
            #             dF.loc[i, 'travel_date_from'] = "1900-01-01"
            #             dF.loc[i, 'travel_date_to'] = "2099-12-31"
            #
            #     except KeyError:
            #         dF.loc[i, 'travel_date_from'] = "1900-01-01"
            #         dF.loc[i, 'travel_date_to'] = "2099-12-31"
            # except TypeError:
            #     try:
            #         dates_array = dF['cat_14'][i]
            #         if dates_array != -999:
            #             has_category = True
            #         else:
            #             dF.loc[i, 'travel_date_from'] = "1900-01-01"
            #             dF.loc[i, 'travel_date_to'] = "2099-12-31"
            #     except KeyError:
            #         dF.loc[i, 'travel_date_from'] = "1900-01-01"
            #         dF.loc[i, 'travel_date_to'] = "2099-12-31"
            # if has_category:
            #     for entry in dates_array:
            #         tvl_dates.append(
            #             str(entry['TRAVEL_DATES_COMM']) + str(entry['TRAVEL_DATES_EXP']))
            #     min_indx = np.argmin(tvl_dates)
            #     from_date = str(dates_array[min_indx]['TRAVEL_DATES_COMM'])
            #     to_date = str(dates_array[min_indx]['TRAVEL_DATES_EXP'])
            #     if len(from_date) == 7:
            #         from_date = from_date[1:]
            #     if len(to_date) == 7:
            #         to_date = to_date[1:]
            #     try:
            #         from_date = datetime.datetime.strftime(
            #             datetime.datetime.strptime(
            #                 from_date, "%y%m%d"), "%Y-%m-%d")
            #     except ValueError:
            #         from_date = "1900-01-01"
            #     try:
            #         to_date = datetime.datetime.strftime(
            #             datetime.datetime.strptime(
            #                 to_date, "%y%m%d"), "%Y-%m-%d")
            #     except ValueError:
            #         to_date = "2099-12-31"
            #     dF.loc[i, 'travel_date_from'] = from_date
            #     dF.loc[i, 'travel_date_to'] = to_date
            # except KeyError:
            #     dF.loc[i, 'travel_date_from'] = "1900-01-01"
            #     dF.loc[i, 'travel_date_to'] = "2099-12-31"

            # except TypeError:
            #     dF.loc[i, 'travel_date_from'] = "1900-01-01"
            #     dF.loc[i, 'travel_date_to'] = "2099-12-31"
            # try:
            #     dates_array_15 = dF['Footnotes'][i]['Cat_15_FN']
            #     has_category_15 = True
            # except KeyError:
            #     try:
            #         dates_array_15 = dF['cat_15'][i]
            #         if dates_array_15 != -999:
            #             has_category_15 = True
            #         else:
            #             dF.loc[i, 'sale_date_from'] = "1900-01-01"
            #             dF.loc[i, 'sale_date_to'] = "2099-12-31"
            #     except KeyError:
            #         dF.loc[i, 'sale_date_from'] = "1900-01-01"
            #         dF.loc[i, 'sale_date_to'] = "2099-12-31"
            # except TypeError:
            #     try:
            #         dates_array_15 = dF['cat_15'][i]
            #         if dates_array_15 != -999:
            #             has_category_15 = True
            #         else:
            #             dF.loc[i, 'sale_date_from'] = "1900-01-01"
            #             dF.loc[i, 'sale_date_to'] = "2099-12-31"
            #     except KeyError:
            #         dF.loc[i, 'sale_date_from'] = "1900-01-01"
            #         dF.loc[i, 'sale_date_to'] = "2099-12-31"
            # if has_category_15:
            #     for entry in dates_array_15:
            #         sale_dates.append(
            #             str(entry['SALE_DATES_EARLIEST_TKTG']) + str(entry['SALE_DATES_LATEST_TKTG']))
            #     min_indx = np.argmin(sale_dates)
            #     from_date = str(
            #         dates_array_15[min_indx]['SALE_DATES_EARLIEST_TKTG'])
            #     to_date = str(
            #         dates_array_15[min_indx]['SALE_DATES_LATEST_TKTG'])
            #     if len(from_date) == 7:
            #         from_date = from_date[1:]
            #     if len(to_date) == 7:
            #         to_date = to_date[1:]
            #     try:
            #         from_date = datetime.datetime.strftime(
            #             datetime.datetime.strptime(
            #                 from_date, "%y%m%d"), "%Y-%m-%d")
            #     except ValueError:
            #         from_date = "1900-01-01"
            #     try:
            #         to_date = datetime.datetime.strftime(
            #             datetime.datetime.strptime(
            #                 to_date, "%y%m%d"), "%Y-%m-%d")
            #     except ValueError:
            #         to_date = "2099-12-31"
            #     dF.loc[i, 'sale_date_from'] = from_date
            #     dF.loc[i, 'sale_date_to'] = to_date
            # except KeyError:
            #     dF.loc[i, 'sale_date_from'] = "1900-01-01"
            #     dF.loc[i, 'sale_date_to'] = "2099-12-31"
            # except TypeError:
            #     dF.loc[i, 'sale_date_from'] = "1900-01-01"
            #     dF.loc[i, 'sale_date_to'] = "2099-12-31"
            
            try:
                for entry in dF['cat_12'][i]:

                    key = str(entry['DATE_RANGE_START']) +                         str(entry['DATE_RANGE_STOP'])
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
                temp_df['surcharge_amt1'] = temp_df.apply(lambda row: convert_currency(row['surcharge_amt1'],
                                                                        dF.loc[i, 'currency'],
                                                                        row['currency']), axis=1)
                temp_df['surcharge_amt2'] = temp_df.apply(lambda row: convert_currency(row['surcharge_amt2'],
                                                                                 dF.loc[i, 'currency'],
                                                                                 row['currency']), axis=1)
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
            # try:
            #     cat_4 = dF.loc[i, 'cat_4']
            #     temp = ""
            #     for element in cat_4:
            #         try:
            #             temp_st = element['CXR_FLT']['SEGS_CARRIER']
            #             if len(temp_st) % 14 != 0:
            #                 temp_st = temp_st + "    "
            #             for i in range(0, len(temp_st), 14):
            #                 seg = temp_st[i:i + 14]
            #                 op_cxr = seg[0:2]
            #                 mrkt_cxr = seg[3:5]
            #                 range_start = seg[6:10]
            #                 range_end = seg[10:]
            #                 if op_cxr == "  ":
            #                     op_cxr = "--"
            #                 if mrkt_cxr == "  ":
            #                     mrkt_cxr = "--"
            #                 if ((range_start == "****") or (range_end == "****")) and (
            #                     (op_cxr == "FZ") or (mrkt_cxr == "FZ")):
            #                     temp = temp + op_cxr + " " + mrkt_cxr + " " + "****, "
            #                 elif (op_cxr == "FZ") or (mrkt_cxr == "FZ"):
            #                     temp = temp + op_cxr + " " + mrkt_cxr + " " + range_start + "-" + range_end + ", "
            #         except KeyError:
            #             pass
            #
            #         for i in range(3):
            #             key_name_cxr = "CXR" + str(i + 1)
            #             key_name_flt = "FLT_NO" + str(i + 1)
            #             if element[key_name_cxr] == "FZ":
            #                 temp = temp + element[key_name_cxr] + " " + str(element[key_name_flt]) + ", "
            #
            #     dF.loc[i, 'flight_number'] = temp[:-2]
            # except KeyError:
            #     pass
            #     #dF.loc[i, 'flight_number'] = "--"
            # except TypeError:
            #     pass
            #
            # try:
            #     cat_2 = dF.loc[i,'cat_2']
            #     unique_list = []
            #     temp = ""
            #     for element in cat_2:
            #         for wkday in element['DAYOFWEEK']:
            #             if wkday not in unique_list:
            #                 unique_list.append(wkday)
            #                 temp = temp + wkday + ", "
            #     dF.loc[i, 'day_of_week'] = temp[:-2]
            # except KeyError:
            #     dF.loc[i, 'day_of_week'] = "--"
            # except TypeError:
            #     dF.loc[i, 'day_of_week'] = "--"

        dF.replace(0, np.NaN, inplace=True)
        
        dF['Average_surcharge'] = dF[['surcharge_amount_1',
                                      'surcharge_amount_2',
                                      'surcharge_amount_3',
                                      'surcharge_amount_4']].mean(axis=1)
        dF.replace(np.NaN, 0, inplace=True)
        dF['dep_date_from'] = dep_date
        dF['dep_date_to'] = dep_date
        #dF['YR'] = 0
        #for i in currency:
        #    dF.loc[dF['currency'] == i, 'YR'] = yr[i]
        #dF['YR'] = dF['YR'] * dF['oneway_return'].astype("int")
        dF.replace(-999, np.nan, inplace=True)
        dF['fare'].fillna(0)
        if 'YQ' in list(dF.columns):
            dF['YQ'].fillna(0, inplace=True)
        else:
            dF['YQ'] = 0
            
        #dF['YQ'].fillna(0)
        #if "DXB" not in origin+destination:
        #    dF['YQ'] = dF['YQ'] * 2
        if 'YR' in list(dF.columns):
            dF['YR'].fillna(0, inplace=True)
        else:
            dF['YR'] = 0
        
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
        # dF['is_expired'] = 0
        # dF.loc[(dF['effective_to'] < snap_date) &
        #        (dF['sale_date_to'] < snap_date) &
        #        (dF['travel_date_to'] < snap_date), 'is_expired'] = 1
        dF = dF.sort_values("fare")
    # dF = dF[dF['effective_from'] < dF['effective_to']]
    #print(dF)
    return dF


@measure(JUPITER_LOGGER)
def lowestFilledFare(pos,origin,destination,compartment,snap_date,dep_date,currencyPos_dict):
    
    currency_data = currencyPos_dict
    od = origin+""+destination
    comp_crscr = fzDBConn.JUP_DB_Pos_OD_Compartment_new.find({"pos":pos,"od":od,"compartment":compartment, "year":int(dep_date[0:4]),"month":int(dep_date[5:7])})
    
    comp = []
    val_curscr = tuple()
    air_array = [Host_Airline_Code]
    try:
        for each_comp in comp_crscr:
            for eachairline in each_comp['top_5_comp_data']:
                air_array.append(eachairline['airline'])
            val_curscr = (air_array,[])
    except IndexError as error:
        print(error)
    except ValueError as error:
        print(error)
    except KeyError as error:
        print(error) 
    
    comp.append(val_curscr)
    if comp == [()]:
        comp = []
        val_curscr = (air_array,[])
        comp.append(val_curscr)
    
#     print(list_comp)

    #currency_data = get_currency_data()
    # print "lowest comp: ", comp
    if pos:
        if pos == destination:
            org = deepcopy(destination)
            dest = deepcopy(origin)
            origin = org
            destination = dest

    lowest_fares_dF = pd.DataFrame(
        columns=[
            'carrier',
            'lowest_fare_base',
            'lowest_fare_total',
            'lowest_fare_YQ',
            'lowest_fare_YR',
            'lowest_fare_fb',
            'lowest_fare_tax',
            'lowest_fare_surcharge',
            'highest_fare_base',
            'highest_fare_total',
            'highest_fare_fb',
            'highest_fare_YQ',
            'highest_fare_YR',
            'highest_fare_tax',
            'highest_fare_surcharge',
            'currency'
        ]
    )
    comp_list = []
    # print "comp", comp

    for tup in comp:
        # print "tup: ", tup[0]
        try:
            for c in tup[0]:
                if c not in comp_list:
                    comp_list.append(c)
        except Exception:
            pass
    
    comp_list.append(Host_Airline_Code)
    comp_list = list(set(comp_list))
    # print ("----> comp_list")
    # print (comp)
    
    lowest_fares_d = LFF(pos, origin, destination, compartment, snap_date, dep_date, currencyPos_dict, comp_list, None)
    
    # print(lowest_fares_d_["carrier"])
    
    curr_list = lowest_fares_d['currency'].unique()
    if "YR" not in lowest_fares_d.columns:
        lowest_fares_d['YR'] = 0
    final_data = pd.DataFrame()
    count = 0
    date_list = [{'end':dep_date,'start':dep_date}]
    
    comp_li_date = []
    
    
    for date_ in date_list:
        # print ("comp: ", comp[count])
        comp_li_date = comp_list
        
        comp_li_date.append(Host_Airline_Code)

        comp_li_date = list(set(comp_li_date))

        # lowest_fares_d = lowest_fares_d_[
        #     (lowest_fares_d_['travel_date_from'] <= date_['end']) & (
        #         lowest_fares_d_['travel_date_to'] >= date_['start'])]
        lowest_fares_d['Average_fare'] = lowest_fares_d['fare'] + lowest_fares_d['YQ'] +             lowest_fares_d['YR'] + lowest_fares_d['Average_surcharge'] + lowest_fares_d['taxes']
        lowest_fares_d['total_fare_1'] = lowest_fares_d['fare'] + lowest_fares_d['YQ'] +             lowest_fares_d['YR'] + lowest_fares_d['surcharge_amount_1'] + lowest_fares_d['taxes']
        lowest_fares_d['total_fare_2'] = lowest_fares_d['fare'] + lowest_fares_d['YQ'] +             lowest_fares_d['YR'] + lowest_fares_d['surcharge_amount_2'] + lowest_fares_d['taxes']
        lowest_fares_d['total_fare_3'] = lowest_fares_d['fare'] + lowest_fares_d['YQ'] +             lowest_fares_d['YR'] + lowest_fares_d['surcharge_amount_3'] + lowest_fares_d['taxes']
        lowest_fares_d['total_fare_4'] = lowest_fares_d['fare'] + lowest_fares_d['YQ'] +             lowest_fares_d['YR'] + lowest_fares_d['surcharge_amount_4'] + lowest_fares_d['taxes']
        lowest_fares_d['min'] = lowest_fares_d.loc[:, ['total_fare_1',
                                                       'total_fare_2',
                                                       'total_fare_3',
                                                       "total_fare_4"]].min(axis=1)
        lowest_fares_d['max'] = lowest_fares_d.loc[:, ['total_fare_1',
                                                       'total_fare_2',
                                                       'total_fare_3',
                                                       "total_fare_4"]].max(axis=1)
        # print lowest_fares_d.head()
        my_list = []
        for competitor in comp_li_date:
#             #print (competitor)
            for curr in curr_list:
                result = {"carrier": "NA",
                          "currency": "NA",
                          "lowest_fare_total": "NA",
                          "lowest_fare_tax": "NA",
                          "lowest_fare_YQ": "NA",
                          "lowest_fare_YR": "NA",
                          "lowest_fare_surcharge": "NA",
                          "lowest_fare_base": "NA",
                          "lowest_fare_fb": "NA",
                          "highest_fare_total": "NA",
                          "highest_fare_tax": "NA",
                          "highest_fare_YQ": "NA",
                          "highest_fare_YR": "NA",
                          "highest_fare_surcharge": "NA",
                          "highest_fare_base": "NA",
                          "highest_fare_fb": "NA",
                          "lowest_fare_total_rt": "NA",
                          "lowest_fare_tax_rt": "NA",
                          "lowest_fare_YQ_rt": "NA",
                          "lowest_fare_YR_rt": "NA",
                          "lowest_fare_surcharge_rt": "NA",
                          "lowest_fare_base_rt": "NA",
                          "lowest_fare_fb_rt": "NA",
                          "highest_fare_total_rt": "NA",
                          "highest_fare_tax_rt": "NA",
                          "highest_fare_YQ_rt": "NA",
                          "highest_fare_YR_rt": "NA",
                          "highest_fare_surcharge_rt": "NA",
                          "highest_fare_base_rt": "NA",
                          "highest_fare_fb_rt": "NA",
                          }
                temp_df = lowest_fares_d[(lowest_fares_d['carrier'] == competitor) &
                                         (lowest_fares_d['currency'] == curr) &
                                         (lowest_fares_d['oneway_return'] == "1")]
                temp_df_2 = lowest_fares_d[(lowest_fares_d['carrier'] == competitor) &
                                           (lowest_fares_d['currency'] == curr) &
                                           (lowest_fares_d['oneway_return'] == "2")]

                if len(temp_df) > 0:
                    temp_df_min = temp_df[temp_df['min'] == min(temp_df['min'])]
                    min_idx = temp_df_min.index[0]

                    temp_df_max = temp_df[temp_df['max'] == max(temp_df['max'])]
                    max_idx = temp_df_max.index[0]

                    result['carrier'] = competitor
                    result['currency'] = curr
                    result['lowest_fare_total'] = temp_df_min.loc[min_idx,'min']
                    result['lowest_fare_tax'] = temp_df_min.loc[min_idx,
                                                                       'taxes']
                    result['lowest_fare_YQ'] = temp_df_min.loc[min_idx,
                                                                      'YQ']
                    result['lowest_fare_YR'] = temp_df_min.loc[min_idx,
                                                                      'YR']
                    result['lowest_fare_surcharge'] = temp_df_min.loc[min_idx,
                                                                             'min'] - temp_df_min.loc[min_idx,
                                                                                                      'YQ'] - temp_df_min.loc[min_idx,
                                                                                                                              'YR'] - temp_df_min.loc[min_idx,
                                                                                                                                                      'fare'] - temp_df_min.loc[min_idx,
                                                                                                                                                                                'taxes']
                    result['lowest_fare_base'] = temp_df_min.loc[min_idx,
                                                                        'fare']
                    result['lowest_fare_fb'] = temp_df_min.loc[min_idx,
                                                                      'fare_basis']
                    result['highest_fare_total'] = temp_df_max.loc[max_idx,
                                                                          'max']
                    result['highest_fare_tax'] = temp_df_max.loc[max_idx,
                                                                        'taxes']
                    result['highest_fare_YQ'] = temp_df_max.loc[max_idx,
                                                                       'YQ']
                    result['highest_fare_YR'] = temp_df_max.loc[max_idx,
                                                                       'YR']
                    result['highest_fare_surcharge'] = temp_df_max.loc[max_idx,
                                                                              'max'] - temp_df_max.loc[max_idx,
                                                                                                       'YQ'] - temp_df_max.loc[max_idx,
                                                                                                                               'YR'] - temp_df_max.loc[max_idx,
                                                                                                                                                       'fare'] - temp_df_max.loc[max_idx,
                                                                                                                                                                                 'taxes']
                    result['highest_fare_base'] = temp_df_max.loc[max_idx,
                                                                         'fare']
                    result['highest_fare_fb'] = temp_df_max.loc[max_idx,
                                                                       'fare_basis']


                if len(temp_df_2) > 0:
                    temp_df_min_2 = temp_df_2[temp_df_2['min'] == min(temp_df_2['min'])]
                    min_idx_2 = temp_df_min_2.index[0]
                    temp_df_max_2 = temp_df_2[temp_df_2['max'] == max(temp_df_2['max'])]
                    max_idx_2 = temp_df_max_2.index[0]
                    result['lowest_fare_total_rt'] = temp_df_min_2.loc[min_idx_2, 'min']
                    result['lowest_fare_tax_rt'] = temp_df_min_2.loc[min_idx_2,
                                                                'taxes']
                    result['lowest_fare_YQ_rt'] = temp_df_min_2.loc[min_idx_2,
                                                               'YQ']
                    result['lowest_fare_YR_rt'] = temp_df_min_2.loc[min_idx_2,
                                                               'YR']
                    result['lowest_fare_surcharge_rt'] = temp_df_min_2.loc[min_idx_2,
                                                                      'min'] - temp_df_min_2.loc[min_idx_2,
                                                                                               'YQ'] - temp_df_min_2.loc[
                                                          min_idx_2,
                                                          'YR'] - temp_df_min_2.loc[min_idx_2,
                                                                                  'fare'] - temp_df_min_2.loc[min_idx_2,
                                                                                                            'taxes']
                    result['lowest_fare_base_rt'] = temp_df_min_2.loc[min_idx_2,
                                                                 'fare']
                    result['lowest_fare_fb_rt'] = temp_df_min_2.loc[min_idx_2,
                                                               'fare_basis']
                    result['highest_fare_total_rt'] = temp_df_max_2.loc[max_idx_2,
                                                                   'max']
                    result['highest_fare_tax_rt'] = temp_df_max_2.loc[max_idx_2,
                                                                 'taxes']
                    result['highest_fare_YQ_rt'] = temp_df_max_2.loc[max_idx_2,
                                                                'YQ']
                    result['highest_fare_YR_rt'] = temp_df_max_2.loc[max_idx_2,
                                                                'YR']
                    result['highest_fare_surcharge_rt'] = temp_df_max_2.loc[max_idx_2,
                                                                       'max'] - temp_df_max_2.loc[max_idx_2,
                                                                                                'YQ'] - temp_df_max_2.loc[
                                                           max_idx_2,
                                                           'YR'] - temp_df_max_2.loc[max_idx_2,
                                                                                   'fare'] - temp_df_max_2.loc[max_idx_2,
                                                                                                             'taxes']
                    result['highest_fare_base_rt'] = temp_df_max_2.loc[max_idx_2,
                                                                  'fare']
                    result['highest_fare_fb_rt'] = temp_df_max_2.loc[max_idx_2,
                                                                'fare_basis']
                my_list.append(result)
        temp = pd.DataFrame(my_list)
        temp['date_start'] = date_['start']
        temp['date_end'] = date_['end']
        final_data = pd.concat([final_data, temp], ignore_index=True)
        count += 1
    if len(final_data) > 0:
        # lowest_fares_dF = pd.DataFrame(lowest_fares_d)
        gds_cols = ["currency_gds",
                    "highest_fare_YQ_gds",
                    "highest_fare_YR_gds",
                    "highest_fare_base_gds",
                    "highest_fare_fb_gds",
                    "highest_fare_surcharge_gds",
                    "highest_fare_tax_gds",
                    "highest_fare_total_gds",
                    "lowest_fare_YQ_gds",
                    "lowest_fare_YR_gds",
                    "lowest_fare_base_gds",
                    "lowest_fare_fb_gds",
                    "lowest_fare_surcharge_gds",
                    "lowest_fare_tax_gds",
                    "lowest_fare_total_gds",
                    "highest_fare_YQ_rt_gds",
                    "highest_fare_YR_rt_gds",
                    "highest_fare_base_rt_gds",
                    "highest_fare_fb_rt_gds",
                    "highest_fare_surcharge_rt_gds",
                    "highest_fare_tax_rt_gds",
                    "highest_fare_total_rt_gds",
                    "lowest_fare_YQ_rt_gds",
                    "lowest_fare_YR_rt_gds",
                    "lowest_fare_base_rt_gds",
                    "lowest_fare_fb_rt_gds",
                    "lowest_fare_surcharge_rt_gds",
                    "lowest_fare_tax_rt_gds",
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
        final_data = filed_df.merge(gds_df, on=['carrier', 'date_start', 'date_end'], how='left', suffixes=("", "_gds"))
    # print lowest_fares_dF.columns
    # print time.time() - st
    st = time.time()
    #print (final_data)
    return final_data


@measure(JUPITER_LOGGER)
def lowFilledFareCalling(snap_date):
    
#     pos = "UAE"
#     origin = "DXB"
#     destination = "DOH"
#     compartment = "Y"
#     snap_date = "2017-10-02"
#     dep_date = "2017-10-12"
    
    
        
    
    
    
#     for index, row in DF.iterrows():
# #         fzDBConn.Test.insert_one({"lowest_filled_fare":row.to_dict})
#         print(row)
    
    #MT_crsr = fzDBConn.JUP_DB_Manual_Triggers_Module.find({'pos.City':'DOH','od':'DOHDWC','compartment.compartment':'Y','dep_date':'2017-10-07','trx_date':'2017-10-10'},{"_id":1,"trx_date":1,"dep_date":1,"pos":1,"origin":1,"destination":1,"compartment":1})   
	# static dep_period
#'''
    data = [
	{"pos":"CMB","od":"DXBCMB"},
	]
    
    dep_month = 11
    dep_year = 2017
    market_count = 0
    #'''
    number = 1
    updateBulk =  fzDBConn.JUP_DB_Manual_Triggers_Module.initialize_unordered_bulk_op()

    for datas in range(1):
        MT_crsr = fzDBConn.JUP_DB_Manual_Triggers_Module.find({"trx_date":{"$gte":SYS_SNAP_DATE,"$lte":SYS_SNAP_DATE}},
                                                              {"_id":1,"trx_date":1,"dep_date":1,"pos":1,"origin":1,"destination":1,"compartment":1},no_cursor_timeout=True)
        num = 1
        market_count = market_count +1
        clstCount = MT_crsr.count()
        for each_crsr in MT_crsr:
            try:
                
                num += 1
                DF = lowestFilledFare(each_crsr['pos']['City'],each_crsr['origin']['City'],each_crsr['destination']['City'],each_crsr['compartment']['compartment'],each_crsr['trx_date'],each_crsr['dep_date'],currencyPos_dict)
                #lowestFilledFare(pos,origin,destination,compartment,snap_date,dep_date,currencyPos_dict):
                #DF = lowestFilledFare(each_crsr['pos']['City'],each_crsr['origin']['City'],each_crsr['destination']['City'],each_crsr['compartment']['compartment'],each_crsr['trx_date'],each_crsr['dep_date'],currencyPos_dict)
                #print ("----")
                #print(DF)
                #DF = DF.where((pd.notnull(DF)), None)
                datas = DF.to_dict("records")
                
                #print(datas)
                arrayData_1 = []
                for value in json.loads(DF.T.to_json()).values():
                    arrayData_1.append(value)
                #fzDBConn.Temp_LFF.insert({'lowestFare':arrayData_1 })
                '''
                arrayData = []
                for data in datas:
                    arrayData.append(dict(data))
                #print(arrayData)
                '''
                updateBulk.find({"_id":each_crsr['_id'],"dep_date_ISO":{"$ne":None}}).update_one({'$set': {'lowestFare':arrayData_1 }})
                #fzDBConn.JUP_DB_Manual_Triggers_Module.update_one({"_id":each_crsr['_id'],"dep_date_ISO":{"$ne":None}},{'$set':{'lowestFare':arrayData}}, upsert=False)
                #fzDBConn.Temp_LFF.insert({"LFF":arrayData})
                #fzDBConn.JUP_DB_Manual_Triggers_Module.update({"_id":each_crsr['_id']},{'$set': {'lowestFare':arrayData }})
                
                if ( number % 10 == 0 ):
                    try:
                        print(str(market_count)+" / "+str(clstCount)+" / "+str(number)+" / "+str(each_crsr['_id']))
                        result1 = updateBulk.execute();
                        updateBulk = fzDBConn.JUP_DB_Manual_Triggers_Module.initialize_unordered_bulk_op();
                        #print(result1)
                    except BulkWriteError as bwe:
                        print(bwe.details)
                
                number = number+1
            except IndexError as error:
                print ("Error")
                print(error)
            except ValueError as error:
                print ("Error2")
                print(error)
            except KeyError as error:
                print ("Error3")
                print(error)
            except TypeError as error:
                print ("Error4")
                print(error)

        #DF = lowestFilledFare(pos,origin,destination,compartment,snap_date,dep_date,currencyPos_dict)
        #jsonFrame = json.loads(DF.T.to_json()).values()

        #fzDBConn.JUP_DB_Manual_Triggers_Module.update({},{})
    try:
        updateBulk.execute();
        #pass
    except:
        pass                
    return "Done"
    
# lowFilledFareCalling(SYS_SNAP_DATE)


if __name__=='__main__':
    st = time.time()
    final_data = lowestFilledFare(pos='MCT', origin='MCT', destination='JED',
                     compartment='Y', snap_date='2018-05-23', dep_date='2018-04-26', currencyPos_dict=currencyPos_dict)
    print final_data
    print 'Total time taken:', time.time() - st
