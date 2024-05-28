import time
import json
import operator
from bson.objectid import ObjectId
import pymongo
from collections import defaultdict
from jupiter_AI import JUPITER_DB, mongo_client, SYSTEM_DATE, Host_Airline_Code, Host_Airline_Hub, JUPITER_LOGGER
from jupiter_AI.logutils import measure
from jupiter_AI.batch.fbmapping_batch.fbmapping_optimizer import optimizefareladder
from jupiter_AI.batch.fbmapping_batch.min_to_min import mintominfareladder as mintomin
import datetime
import traceback
from copy import deepcopy
import pandas as pd

COLLECTION_UPDATE_LIST = ['JUP_DB_ATPCO_Fares_Rules']
COLLECTION_DEPENDENCY_LIST = ['JUP_DB_ATPCO_Fares_Rules']
ERROR_FLAG = 0
TOTAL_MARKETS = 0
START_TIME = time.time()
SYSTEM_DATE_MOD = "0" + SYSTEM_DATE[2:4] + SYSTEM_DATE[5:7] + SYSTEM_DATE[8:10]


@measure(JUPITER_LOGGER)
def get_od_list(db):
    print "Getting unique ODs for FZ . . . . "
    cursor_od_list = db.JUP_DB_Market_Significance.distinct("od")
    od_list = list(cursor_od_list)
    print "TOTAL number of ODs = ", len(od_list)
    return od_list


@measure(JUPITER_LOGGER)
def get_exchange_rate_details(db):
    exchange_rate_crsr = db.JUP_DB_Exchange_Rate.find()
    exchange_rate_data = list(exchange_rate_crsr)
    data = dict()
    for currency_doc in exchange_rate_data:
        try:
            data[currency_doc['code']]
        except KeyError:
            data[currency_doc['code']] = dict()
        for currency_2_doc in exchange_rate_data:
            data[currency_doc['code']][currency_2_doc['code']] = currency_doc['Reference_Rate'] / currency_2_doc['Reference_Rate']

    return data


@measure(JUPITER_LOGGER)
def get_city_airport_mapping(od_list, db):
    mapping_df = pd.DataFrame(list(db.JUP_DB_City_Airport_Mapping.find({}, {'Airport_Code': 1,
                                                                            'City_Code': 1,
                                                                            '_id': 0})))
    od_df = pd.DataFrame(od_list, columns=['od'])
    od_df['origin'] = od_df['od'].str.slice(0, 3)
    od_df['destination'] = od_df['od'].str.slice(3, 6)
    od_df = od_df.merge(mapping_df.rename(columns={'Airport_Code': 'origin', 'City_Code': 'pseudo_origin'}),
                        on='origin', how='left')
    od_df['pseudo_origin'].fillna(od_df['origin'], inplace=True)
    od_df = od_df.merge(mapping_df.rename(columns={'Airport_Code': 'destination', 'City_Code': 'pseudo_destination'}),
                        on='destination', how='left')
    od_df['pseudo_destination'].fillna(od_df['destination'], inplace=True)
    od_df['pseudo_od'] = od_df['pseudo_origin'] + od_df['pseudo_destination']
    return list(set(list(od_df['pseudo_od'].values)))


@measure(JUPITER_LOGGER)
def update_comp_fb(od_list, client):
    db = client[JUPITER_DB]
    st = time.time()
    pseudo_od_list = get_city_airport_mapping(od_list, db)
    fares_ppln = [
        {
            '$match': {
                '$and':
                    [
                        {'pseudo_od': {'$in': pseudo_od_list}},
                        {'channel': 'gds'},
                        {'fare_include': True},
                        {'fare': {'$ne': 0}},
                        {
                            '$or':
                                [
                                    {'effective_from': {'$lte': SYSTEM_DATE}},
                                    {'effective_from': None}
                                ]
                        },
                        {
                            '$or':
                                [
                                    {'effective_to': {'$gte': SYSTEM_DATE}},
                                    {'effective_to': None}
                                ]
                        },
                        {
                            "$or":
                                [
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
        }
        ,
        {
            '$group': {
                '_id':{
                    'carrier':'$carrier',
                    'OD':'$pseudo_od',
                    'compartment':'$compartment',
                    'oneway_return':'$oneway_return'
                },
                'fares':{
                    '$push':{
                        'unique_id':'$_id',
                        'fare':'$fare',
                        'fare_basis':'$fare_basis',
                        'currency':'$currency'
                    }
                }
            }
        },

        {
            '$group': {
                '_id':{
                    'OD':'$_id.OD',
                    'compartment':'$_id.compartment',
                    'oneway_return':'$_id.oneway_return'
                }
                ,
                'docs':{
                    '$push':{
                        'carrier':'$_id.carrier',
                        'fares':'$fares'
                    }
                }
            }
        },

        {
            '$project': {
                '_id':0,
                'OD':'$_id.OD',
                'compartment':'$_id.compartment',
                'oneway_return':'$_id.oneway_return',
                'host_doc':
                    {
                        '$filter': {
                            'input': "$docs",
                            'as': "docs",
                            'cond': { "$eq": [ "$$docs.carrier", Host_Airline_Code ] }
                        }
                    }
                ,
                'comp_docs':
                    {
                        '$filter': {
                            'input': "$docs",
                            'as': "docs",
                            'cond': { "$ne": [ "$$docs.carrier", Host_Airline_Code ] }
                        }
                    }
            }
        },

        {
            '$addFields': {
                'host_docs_count':{'$size':'$host_doc'},
                'comps_count':{'$size':'$comp_docs'}
            }
        },

        {
            '$match': {
                'comps_count':{'$gt':0},
                'host_docs_count':{'$gt':0}
            }
        }
    ]
    # print json.dumps(fares_ppln)
    try:
        st = time.time()
        print "Aggregating on ATPCO FARES to filter out fares"
        crsr = db.JUP_DB_ATPCO_Fares_Rules.aggregate(fares_ppln,allowDiskUse=True)
        print "Time taken to aggregate host and comp fares per OD/compartment/ow_return from ATPCO_Fares_Rules = ", time.time() - st
        global TOTAL_MARKETS
        for combination in crsr:
            print 'OD', combination['OD']
            print 'Compartment', combination['compartment']
            print 'OW/RT', combination['oneway_return']

            mapped = defaultdict(list)

            host_fares = combination['host_doc'][0]['fares']
            host_fl = [doc['fare'] for doc in host_fares]
            print "Host FL = ", host_fl
            host_fbs = [doc['fare_basis'] for doc in host_fares]
            print "Host_fbs = ", host_fbs
            host_ids = [doc['unique_id'] for doc in host_fares]
            host_currencies = [doc['currency'] for doc in host_fares]

            curr_conv_dict2d = get_exchange_rate_details(db)
            currency_freq = defaultdict(int)

            for curr in host_currencies:
                currency_freq[curr] += 1

            host_currency = sorted(currency_freq.items(), key=operator.itemgetter(1),reverse=True)[0][0]
            print "host_currency = ", host_currency
            host_fl_copy = deepcopy(host_fl)
            host_fl = []
            for index, val in enumerate(host_fl_copy):
                try:
                    host_fl.append(val*(curr_conv_dict2d[host_currencies[index]][host_currency]))
                except KeyError:  # Currency not found in crr_conv_dict2d
                    host_fl.append(val)
            print "Convert to market currency: host fareladder = ", host_fl
            for comp_fare_list in combination['comp_docs']:
                comp_fares = comp_fare_list['fares']
                comp_fl = []
                for doc in comp_fares:
                    try:
                        comp_fl.append(doc['fare'] * (curr_conv_dict2d[doc['currency']][host_currency]))
                    except:  # Currency not found in crr_conv_dict2d
                        comp_fl.append(doc['fare'])
                comp_fbs = [doc['fare_basis'] for doc in comp_fares]
                print 'Competitor = ', comp_fare_list['carrier']
                print 'Converted to host currency, Competitor Fareladder = ', comp_fl
                out = optimizefareladder('raw', host_fl, comp_fl)
                # if len(out[0]) == out[0].count(-1):  # If none of the fares were mapped due to threshold being lower maybe. This can happend frequently in case of compartment = J
                #     out = mintomin(host_fl, comp_fl)
                #     print "Done with min to min !"
                for index, item in enumerate(out[0]):
                    print 'INDEX: ', index
                    print 'ITEM: ', item
                    if item != -1:
                        mapped[(str(host_ids[index]),host_fl[index])].append({'carrier': comp_fare_list['carrier'],
                                                                              'fare_basis': comp_fbs[item],
                                                                              'fare': comp_fl[item]
                                                                              })
            for key in mapped.keys():
                db.JUP_DB_ATPCO_Fares_Rules.update(
                    {
                        '_id':ObjectId(key[0])
                    }
                    ,
                    {
                        '$set':{
                            'competitor_farebasis': mapped[key],
                            'host_fare_base_currency': key[1],
                            'base_currency': host_currency
                        }

                    }
                )
            TOTAL_MARKETS += 1
            print "Counter = ", TOTAL_MARKETS
    except Exception:
        global ERROR_FLAG
        ERROR_FLAG = 1
        print "GOT SOME ERROR"
        print traceback.print_exc()
    print "Time taken for complete program = ", time.time() - st
    client.close()


@measure(JUPITER_LOGGER)
def main_helper():
    client = mongo_client()
    db = client[JUPITER_DB]
    st = time.time()
    print "Updating comp farebasis . . . "
    # print "Setting Comp FB = null for FZ fares . . ."
    # st = time.time()
    # db.JUP_DB_ATPCO_Fares_Rules.update({'carrier': 'FZ'},{'$set': {'competitor_farebasis':None}}, multi=True)
    # print "Time taken to set Comp_fb = null for FZ fares = %s seconds" % str(time.time() - st)
    num_of_markets = 500
    # Some random ODs for testing
    # od_list = [['CMBJED','PRGCMB','AMMDXB']]
    od_list = get_od_list(db)
    # od_list = ['VKODXB', 'DXBVKO']
    ods = list()
    counter = 0
    while counter < len(od_list):
        ods.append(od_list[counter: counter + num_of_markets])
        counter = counter + num_of_markets
    for od_list in ods:
        update_comp_fb(od_list, client)
    print "Time taken for these ODs = ", time.time() - st


if __name__ == '__main__':
    main_helper()
