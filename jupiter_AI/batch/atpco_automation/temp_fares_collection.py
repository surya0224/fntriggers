import time
from jupiter_AI import today, SYSTEM_DATE, mongo_client, JUPITER_DB, Host_Airline_Code, Host_Airline_Hub, JUPITER_LOGGER
from jupiter_AI.logutils import measure
import datetime
import pandas as pd
from copy import deepcopy
#from jupiter_AI.batch.Run_triggers import segregate_markets
from celery import group

@measure(JUPITER_LOGGER)
def create_temp_fares_collection(client, od):
    db = client[JUPITER_DB]
    #db.temp_fares_triggers.remove({})
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
                "OD": {"$in": od}
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
    cursor = db.JUP_DB_ATPCO_Fares_Rules.aggregate([
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
        # {
        #     '$out': 'temp_fares_triggers'
        # }
    ])

    num=1
    Bulk = db.temp_fares_triggers.initialize_ordered_bulk_op()
    for x in cursor:
        print x
        try:
            Bulk.insert(x)
            if num%1000 == 0:
                Bulk.execute()
                Bulk = db.temp_fares_triggers.initialize_ordered_bulk_op()

            num = num+1

        except Exception as bwe:
            print(bwe)
    try:
        Bulk.execute()
        pass
    except Exception as bwe:
        print(bwe)

    print 'Created in ', time.time() - st

def segregate_markets():
    client_temp = mongo_client()
    db = client_temp[JUPITER_DB]
    online_mrkts = db.JUP_DB_Market_Significance.aggregate([
        {"$match": {"online": True}},
        {"$sort": {"rank": 1}},
        {"$project": {"_id": 0, "market": "$market", "pax": "$pax", "revenue": "$revenue"}}
    ],
        allowDiskUse=True)

    online_mrkts_df = pd.DataFrame(list(online_mrkts))
    online_mrkts_df['pos'] = online_mrkts_df['market'].str.slice(0, 3)
    online_mrkts_df['origin'] = online_mrkts_df['market'].str.slice(3, 6)
    online_mrkts_df['destination'] = online_mrkts_df['market'].str.slice(6, 9)
    online_mrkts_df['compartment'] = online_mrkts_df['market'].str.slice(9, 10)
    online_mrkts_df['od'] = online_mrkts_df['origin'] + online_mrkts_df['destination']

    all_user = db.JUP_DB_User.find({})
    all_user_df = pd.DataFrame(list(all_user))
    temp_online_mrkts_df = deepcopy(online_mrkts_df)

    sig_list = []
    sub_sig_list = []
    non_sig_list = []
    for idx, row in all_user_df.iterrows():
        online_mrkts_df = deepcopy(temp_online_mrkts_df)
        user_pos_list = row['list_of_pos']
        online_mrkts_df = online_mrkts_df[online_mrkts_df['pos'].isin(user_pos_list)]

        reg_mas = list(db.JUP_DB_Region_Master.find({}))
        reg_mas = pd.DataFrame(reg_mas)
        online_mrkts_df = online_mrkts_df.merge(reg_mas[['POS_CD', 'COUNTRY_NAME_TX']], left_on='pos', right_on='POS_CD',
                                                how='left').drop('POS_CD',
                                                                 axis=1)

        online_mrkts_df = online_mrkts_df.rename(columns={"COUNTRY_NAME_TX": "POS_Country"})

        online_mrkts_df = online_mrkts_df.merge(reg_mas[['POS_CD', 'COUNTRY_NAME_TX']], left_on='origin', right_on='POS_CD',
                                                how='left').drop(
            'POS_CD', axis=1).rename(columns={"COUNTRY_NAME_TX": "origin_Country"})

        online_mrkts_df = online_mrkts_df.merge(reg_mas[['POS_CD', 'COUNTRY_NAME_TX']], left_on='destination',
                                                right_on='POS_CD', how='left').drop(
            'POS_CD', axis=1).rename(columns={"COUNTRY_NAME_TX": "destination_Country"})

        online_mrkts_df = online_mrkts_df[(online_mrkts_df['POS_Country'] == online_mrkts_df['origin_Country']) |
                                          (online_mrkts_df['POS_Country'] == online_mrkts_df['destination_Country'])]

        print len(online_mrkts_df[online_mrkts_df['origin_Country'] == online_mrkts_df['destination_Country']])
        online_mrkts_df = online_mrkts_df[online_mrkts_df['origin_Country'] != online_mrkts_df['destination_Country']]

        online_mrkts_df = online_mrkts_df[['market', 'pos', 'origin', 'destination', 'od', 'compartment', 'pax', 'revenue']]
        online_mrkts_df.drop_duplicates(subset='market', inplace=True)
        total_markets = len(online_mrkts_df)
        print "Number of markets where POS country == O/D country = ", total_markets

        online_mrkts_df.sort_values(by='revenue', ascending=False, inplace=True)
        online_mrkts_df.reset_index(inplace=True)
        online_mrkts_df['cumm_revenue'] = online_mrkts_df['revenue'].cumsum(axis=0)
        online_mrkts_df['cumm_rev_perc'] = online_mrkts_df['cumm_revenue'] / online_mrkts_df['revenue'].sum() * 100

        sig_rev = list(online_mrkts_df[online_mrkts_df['cumm_rev_perc'] <= 80.0]['market'].values)
        sub_sig_rev = list(online_mrkts_df[(online_mrkts_df['cumm_rev_perc'] > 80.0) &
                                           (online_mrkts_df['cumm_rev_perc'] <= 95.0)]['market'].values)

        online_mrkts_df.sort_values(by='pax', ascending=False, inplace=True)
        online_mrkts_df.reset_index(inplace=True)
        online_mrkts_df['cumm_pax'] = online_mrkts_df['pax'].cumsum(axis=0)
        online_mrkts_df['cumm_pax_perc'] = online_mrkts_df['cumm_pax'] / online_mrkts_df['pax'].sum() * 100

        sig_pax = list(online_mrkts_df[online_mrkts_df['cumm_pax_perc'] <= 80.0]['market'].values)
        sub_sig_pax = list(online_mrkts_df[(online_mrkts_df['cumm_pax_perc'] > 80.0) &
                                           (online_mrkts_df['cumm_pax_perc'] <= 95.0)]['market'].values)

        p2p = list(online_mrkts_df[((online_mrkts_df['origin'] == Host_Airline_Hub) | (
                    online_mrkts_df['destination'] == Host_Airline_Hub) |
                                    (online_mrkts_df['origin'] == 'DWC') | (online_mrkts_df['destination'] == 'DWC')) &
                                   ((online_mrkts_df['origin'] == online_mrkts_df['pos']) |
                                    (online_mrkts_df['destination'] == online_mrkts_df['pos']))]['market'].values)

        sig_markets_df = online_mrkts_df[online_mrkts_df['market'].isin(list(set(sig_pax + sig_rev + p2p)))]

        sub_sig_markets_df = online_mrkts_df[(online_mrkts_df['market'].isin(list(set(sub_sig_pax + sub_sig_rev)))) &
                                             (~online_mrkts_df['market'].isin(list(set(sig_markets_df['market']))))]

        non_sig_markets_df = online_mrkts_df[~online_mrkts_df['market'].isin(list(set(sig_markets_df['market'] +
                                                                                      sub_sig_markets_df['market'])))]

        sig_markets_df['sig_flag'] = 'sig'
        sub_sig_markets_df['sig_flag'] = 'sub_sig'
        non_sig_markets_df['sig_flag'] = 'non_sig'
        #print row['name'], "hhfhjfbjbjbjb"
        sig_list = sig_list + list(sig_markets_df['market'].values)
        sub_sig_list = sub_sig_list + list(sub_sig_markets_df['market'].values)
        non_sig_list = non_sig_list + list(non_sig_markets_df['market'].values)
        #print sig_list
        #print sub_sig_list
        #print non_sig_list

    user_defined_comp = db.JUP_DB_Thresholds.find({'item': 'Significant Markets'})[0]['user_defined_markets']
    sig_list = sig_list + user_defined_comp

    client_temp.close()
    sig_list = list(set(sig_list))
    sub_sig_list = list(set(sub_sig_list))
    non_sig_list = list(set(non_sig_list))

    final_subsig_list = list(set(sub_sig_list)-set(sig_list))
    final_nonsig_list = list((set(non_sig_list)-set(sig_list))-set(sub_sig_list))

    """
    Read from the JUP_DB_Threshold collection.
    Add user_defined markets to sig_markets_df.
    """

    """
    If same markets if present in sig, sub_sig and non-sig then remove it from the sub_sig and non-sig.
    """
    print sig_list
    print final_nonsig_list
    print final_subsig_list

    return list(sig_list), list(final_subsig_list), list(final_nonsig_list)

def get_fares_ods_list(db):
    sig_markets, sub_sig_markets, non_sig_markets = segregate_markets()
    markets = list(set(sig_markets + sub_sig_markets))
    df = pd.DataFrame(markets, columns=['market'])
    df['od'] = df['market'].str.slice(3, 9)
    list_ods = list(set(df['od'].values))
    list_dos = []
    for j in list_ods:
        d = j[:3]
        o = j[3:]
        od = o + d
        list_dos.append(od)
    final_ods = list(set(list_ods + list_dos))

    mapping_df = pd.DataFrame(list(db.JUP_DB_City_Airport_Mapping.find({}, {'Airport_Code': 1,
                                                                            'City_Code': 1,
                                                                            '_id': 0})))
    od_df = pd.DataFrame(final_ods, columns=['OD'])
    od_df['origin'] = od_df['OD'].str.slice(0, 3)
    od_df['destination'] = od_df['OD'].str.slice(3, 6)
    od_df = od_df.merge(mapping_df.rename(columns={'Airport_Code': 'origin', 'City_Code': 'pseudo_origin'}),
                        on='origin', how='left')
    od_df['pseudo_origin'].fillna(od_df['origin'], inplace=True)
    od_df = od_df.merge(mapping_df.rename(columns={'City_Code': 'pseudo_origin'}), on='pseudo_origin', how='left')

    od_df['Airport_Code'].fillna(od_df['origin'], inplace=True)
    od_df.drop(['origin'], axis=1, inplace=True)
    od_df.rename(columns={'Airport_Code': 'origin'}, inplace=True)

    od_df = od_df.merge(mapping_df.rename(columns={'Airport_Code': 'destination', 'City_Code': 'pseudo_destination'}),
                        on='destination', how='left')
    od_df['pseudo_destination'].fillna(od_df['destination'], inplace=True)
    od_df = od_df.merge(mapping_df.rename(columns={'City_Code': 'pseudo_destination'}), on='pseudo_destination',
                        how='left')

    od_df['Airport_Code'].fillna(od_df['destination'], inplace=True)
    od_df.drop(['destination'], axis=1, inplace=True)
    od_df.rename(columns={'Airport_Code': 'destination'}, inplace=True)

    od_df['pseudo_od'] = od_df['pseudo_origin'] + od_df['pseudo_destination']
    od_df['pseudo_origin_dest'] = od_df['pseudo_origin'] + od_df['destination']
    od_df['origin_pseudo_dest'] = od_df['origin'] + od_df['pseudo_destination']

    od_df.drop(['OD', 'pseudo_origin', 'pseudo_destination'], axis=1, inplace=True)
    od_df['od'] = od_df['origin'] + od_df['destination']
    ods = list(od_df['od'].values) + list(od_df['pseudo_od'].values) + list(od_df['pseudo_origin_dest'].values) + \
          list(od_df['origin_pseudo_dest'].values)

    ods = list(set(ods))
    return ods

if __name__ == "__main__":
    client = mongo_client()
    db = client[JUPITER_DB]
    # from jupiter_AI.batch.atpco_automation.Automation_tasks import run_temp_fares_collection


    # ods = get_fares_ods_list(db=db)
    temp_fares = list()
    num_markets = 100
    counter = 0
    # while counter < len(ods):
    print("OD's List")
    create_temp_fares_collection(client, od=[
        "DELYVR",
"DELYOW",
"DELYYC",
"DELYTO",
"DELTPE",
"DELSAL",
"DELJED",
"DELBOG",
"DELYHZ",
"DELYMQ",
"DELWAS",
"DELYYZ",
"DELMNL",
"DELEWR",
"DELSFO",
"DELJFK",
"DELJKT",
"DELTYO",
"DELCHI",
"DELKUL"
]
)
        # temp_fares.append(run_temp_fares_collection.s(od=ods[counter:counter+num_markets]))
        # counter = counter + num_markets
    # group_8 = group(temp_fares)
    # res8 = group_8()
    #create_temp_fares_collection(client)
    client.close()