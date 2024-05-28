from jupiter_AI.tiles.jup_common_functions_tiles import gen_collection_name
import pymongo
from pymongo import MongoClient, InsertOne, DeleteMany, ReplaceOne, UpdateOne
#client = MongoClient()
from pymongo.errors import BulkWriteError
from jupiter_AI import client, JUPITER_DB, SYSTEM_DATE, JUPITER_LOGGER, Host_Airline_Code, mongo_client, today
from jupiter_AI.logutils import measure
import datetime
import time
import pandas as pd
from dateutil.relativedelta import relativedelta
from jupiter_AI.batch.JUP_AI_Significant_POSODC import get_online_pos
from jupiter_AI.batch.fbmapping_batch.JUP_AI_Batch_Fare_Ladder_Mapping import get_city_airport_mapping
from jupiter_AI.triggers.common import cursor_to_df


@measure(JUPITER_LOGGER)
def get_revenue_pax_df(ods, db):
    SYSTEM_DATE_LY = (datetime.datetime.strptime(SYSTEM_DATE, '%Y-%m-%d') - relativedelta(years=1)).strftime('%Y-%m-%d')

    crsr = db.JUP_DB_Manual_Triggers_Module_Summary.aggregate([
        {
            '$match': {
                'od':{'$in': list(ods)},
                'dep_date': {
                    '$gte': SYSTEM_DATE_LY,
                    '$lt': SYSTEM_DATE
                },
                # 'flown_revenue': {"$gt": 0},
                # 'flown_pax': {"$gt": 0}
            }
        }
        # Grouping on the basis of OD ans summing up the revenue and pax
        ,
        {
            '$group': {
                '_id': '$od',
                'revenue': {'$sum': '$flown_revenue'},
                'pax': {'$sum': '$flown_pax'}
            }
        }
        ,
        {
            '$project': {
                '_id': 0,
                'OD': '$_id',
                'revenue': '$revenue',
                'pax': '$pax'
            }
        }
    ], allowDiskUse=True)
    df_od = pd.DataFrame(list(crsr))

    return df_od



@measure(JUPITER_LOGGER)
def get_pax_ods(df):
    df_pax = df.sort_values(by='pax', ascending=False)
    df_pax['sum_pax'] = df_pax['pax'].sum()
    df_pax['pax_percent'] = df_pax['pax'] / df_pax['sum_pax'] * 100.0
    df_pax['pax_percent_cum'] = df_pax['pax_percent'].cumsum()
    df_pax.reset_index(inplace=True)
    sig_df_pax = df_pax[df_pax['pax_percent_cum'] <= 81]
    sig_ods_pax = list(sig_df_pax['OD'].values)
    return sig_ods_pax

@measure(JUPITER_LOGGER)
def get_revenue_ods(df):
    df_revenue = df.sort_values(by='revenue', ascending=False)
    df_revenue['sum_revenue'] = df['revenue'].sum()
    df_revenue['revenue_percent'] = df_revenue['revenue'] / df_revenue['sum_revenue'] * 100.0
    df_revenue['revenue_percent_cum'] = df_revenue['revenue_percent'].cumsum()
    df_revenue.reset_index(inplace=True)
    sig_df_revenue = df_revenue[df_revenue['revenue_percent_cum'] <= 81]
    sig_ods_revenue = list(sig_df_revenue['OD'].values)
    return sig_ods_revenue



@measure(JUPITER_LOGGER)
def get_dxb_ods(df):
    df_dxb = df[(df['origin'] == 'DXB') | (df['destination'] == 'DXB')]
    sig_dxb = list(df_dxb['OD'].values)
    return sig_dxb

@measure(JUPITER_LOGGER)
def get_host_od_capacity_ods(db):
    # crsr = db.JUP_DB_temp_OD_List.find({'REMARKS': {'$ne': "Inactive"}}, {'_id':0, 'OD':1,
    #                                                                       "REMARKS":1})
    crsr = db.JUP_DB_OD_Master.find({'REMARKS': {'$ne': "Inactive"}}, {'_id': 0, 'OD': 1,
                                                                          "REMARKS": 1})
    host_ods_df = pd.DataFrame(list(crsr))
    return host_ods_df

@measure(JUPITER_LOGGER)
def get_private_public(cur2_df, od,db):
    private = ""
    public = ""

    flag_public = False
    flag_private = False
    cur2 = cur2_df[cur2_df['pseudo_od'] == od]

    for index, row in cur2.iterrows():
        if flag_private and flag_public:
            return public, private
        elif row['private_fare'] == False and not flag_public:
            flag_public = True
            public = row['tariff_code']
        else:
            private = row['tariff_code']
            flag_private = True

    return public, private


def main_new(client):
    db = client[JUPITER_DB]
    #db.temp_OD.remove()
    host_ods_df = get_host_od_capacity_ods(db=db)
    list_ods = list(host_ods_df['OD'].values)
    SYSTEM_DATE_LW = datetime.datetime.strftime(today - datetime.timedelta(days=7), '%Y-%m-%d')

    mapping_df = pd.DataFrame(list(db.JUP_DB_City_Airport_Mapping.find({}, {'Airport_Code': 1,
                                                                            'City_Code': 1,
                                                                            '_id': 0})))
    od_df = pd.DataFrame(list_ods, columns=['OD'])
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
    od_df.drop(['OD', 'pseudo_origin', 'pseudo_destination'], axis=1, inplace=True)
    od_df['od'] = od_df['origin']+od_df['destination']


    cur2 = db.JUP_DB_ATPCO_Fares_Rules.aggregate([
            {'$match': {'carrier': Host_Airline_Code,
                        'OD': {'$in': list(set(od_df['od'].values))
                                      },
                        "fare_include":True,

                        "$and":[{
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
                        }]
                        }
            },


            {'$project': {'_id': False,
                          'pseudo_od': 1,
                          'private_fare': 1,
                          'tariff_code': 1,
                          'origin_country': 1,
                          'destination_country': 1,
                          'origin_zone': 1,
                          'destination_zone': 1,
                          'origin_area': 1,
                          'destination_area': 1,
                          'origin': 1,
                          'destination': 1
                          }
            },

            {'$group': {'_id': {'pseudo_od': '$pseudo_od',
                                'private_fare': '$private_fare',
                                'tariff_code': '$tariff_code',
                                'origin_country':'$origin_country',
                                'destination_country': '$destination_country',
                                'origin_zone': '$origin_zone',
                                'destination_zone': '$destination_zone',
                                'origin_area': '$origin_area',
                                'destination_area': '$destination_area',
                                'origin':'$origin',
                                'destination':'$destination'
                                }
                        }
            },

            {'$project': {'_id': False,
                            'pseudo_od': '$_id.pseudo_od',
                            'private_fare': '$_id.private_fare',
                            'tariff_code': '$_id.tariff_code',
                            'origin_country': '$_id.origin_country',
                            'destination_country': '$_id.destination_country',
                            'origin_zone': '$_id.origin_zone',
                            'destination_zone': '$_id.destination_zone',
                            'origin_area': '$_id.origin_area',
                            'destination_area': '$_id.destination_area',
                            'origin':'$_id.origin',
                            'destination': '$_id.destination'
                        }
            }
        ], allowDiskUse=True)

    df = cursor_to_df(cur2)
    destination_df = df[['destination_area', 'destination_country', 'destination_zone', 'destination']]
    origin_df = df[['origin_area', 'origin_country', 'origin_zone', 'origin']]
    cur2_df = df[['private_fare', 'pseudo_od', 'tariff_code']]

    print cur2_df.head(n=100).to_string(), ''
    print cur2_df.shape, '@@@@@@@@@@@@@@@@@@@@@@@@@@@@@'
    #temp_public = pd.DataFrame()

    temp_public = cur2_df.loc[cur2_df['private_fare'] == False, ['pseudo_od', 'tariff_code']]
    temp_public.rename(columns={'tariff_code': 'public_tariff'}, inplace=True)
    print temp_public.head(n=100).to_string()


    #temp_private = pd.DataFrame()
    temp_private = cur2_df.loc[cur2_df['private_fare'] == True, ['pseudo_od', 'tariff_code']]
    temp_private.rename(columns={'tariff_code': 'private_tariff'}, inplace=True)

    print temp_private.head(n=100).to_string()

    temp_pr_pb = temp_public.merge(temp_private, on='pseudo_od', how='outer')
    temp_pr_pb.fillna('', inplace=True)
    print temp_pr_pb.head(n=100).to_string()
    print temp_pr_pb.shape, '$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$'

    # cur2_df = cur2_df.merge(temp_pr_pb, on='pseudo_od', how='left')
    #
    # cur2_df.drop(['tariff_code', 'private_fare'], axis=1, inplace=True)
    # cur2_df.drop_duplicates(inplace=True)
    # merged_df = od_df.merge(cur2_df, on='pseudo_od', how='left')
    merged_df = od_df.merge(temp_pr_pb, on='pseudo_od', how='left')
    """Adjust merged_df public and private tariff fields for those pseudo_ods (ods) which were not present in cur2_df"""
    merged_df.fillna('', inplace=True)
    online_pos = get_online_pos(db=db)
    print 'Got Online POS'

    merged_df.rename(columns={"od": "OD"}, inplace=True)
    merged_df['online'] = False
    merged_df.loc[(merged_df['origin'].isin(online_pos) &
                   merged_df['destination'].isin(online_pos)), "online"] = True

    merged_df = merged_df.merge(host_ods_df, on="OD", how='left')
    merged_df.fillna('', inplace=True)
    origin_df.drop_duplicates(inplace=True)
    destination_df.drop_duplicates(inplace=True)

    merged_df = merged_df.merge(origin_df, on="origin", how="left")
    merged_df = merged_df.merge(destination_df, on="destination", how="left")
    merged_df.fillna('', inplace=True)

    mt_query_ods = list(set(merged_df['OD'].values))
    mt_df = get_revenue_pax_df(ods=mt_query_ods, db=db)

    merged_df = merged_df.merge(mt_df, on='OD', how='left')
    merged_df.fillna(0, inplace=True)
    print merged_df.head(n=100).to_string()

    """
    Adjust the pax and revenue to zero for those ods which did not present in the manual trigger module summary collection. 
    """
    pax_ods = get_pax_ods(merged_df)
    revenue_ods = get_revenue_ods(merged_df)
    dxb_ods = get_dxb_ods(merged_df)
    sig_ods = list(set(pax_ods + revenue_ods + dxb_ods))

    print "Got significant ODs"
    merged_df['significant_flag'] = False
    merged_df.loc[merged_df['OD'].isin(sig_ods), 'significant_flag'] = True
    print "Updated significant flag"
    merged_df['last_update_date'] = SYSTEM_DATE
    #
    # bulk = db.Temp_fzDB_tbl_003.initialize_ordered_bulk_op()
    # for i in merged_df.to_dict('records'):
    #     bulk.insert_many(i)
    # bulk.execute()

    db.Temp_MPF_MT.insert(merged_df.to_dict('records'))
    # print "Inserted into collection OD_Master"
    return None

if __name__ == '__main__':
    st = time.time()
    client = mongo_client()

    main_new(client)
    print "Total time taken = ", time.time() - st
    client.close()
