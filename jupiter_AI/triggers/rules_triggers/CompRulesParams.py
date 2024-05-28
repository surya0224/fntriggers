"""
Author: Abhinav Garg
Created with <3
Date Created: 2018-09-05
Code functionality: Contains all common functions used in fetching old and new rules data
Assumptions:
    1. Only those rules where number of recursive segments is equal to 1 are considered
    2. Fare effectivity is not considered while mapping rule numbers to ODs
Modifications log:
    1. Author:
       Exact modification made or some logic changed:
       Date of modification:
    2. Author:
       Exact modification made or some logic changed:
       Date of modification:

"""
import datetime
import pandas as pd
import math
from jupiter_AI import SYSTEM_DATE, JUPITER_DB, mongo_client, today, ATPCO_DB, JUPITER_LOGGER, Host_Airline_Code
from jupiter_AI.logutils import measure

SYSTEM_DATE_ = datetime.datetime.strftime(today - datetime.timedelta(days=1), '%Y-%m-%d')


@measure(JUPITER_LOGGER)
def get_new_rules_df(cat, record_3, fields, client):
    ATPCO = client[ATPCO_DB]
    cur = list(ATPCO.JUP_DB_ATPCO_Record_2_Cat_All.find({'CXR_CODE': {'$ne': Host_Airline_Code}, 'CAT_NO': cat,
                                                         'LAST_UPDATED_DATE': SYSTEM_DATE_, 'NO_SEGS': '001'},
                                                        {'CXR_CODE': 1, 'TARIFF': 1, 'CAT_NO': 1, 'RULE_NO': 1,
                                                         'DATA_TABLE_STRING_TBL_NO_1': 1, 'SEQ_NO': 1,
                                                         'LAST_UPDATED_DATE': 1, 'DATES_EFF_1': 1,
                                                         'DATES_DISC_1': 1, '_id': 0, 'ACTION': 1}))
    new_df = pd.DataFrame(cur)

    if len(new_df) > 0:
        fields.update({'TBL_NO': 1, '_id': 0})
        record_3 = pd.DataFrame(
            list(ATPCO[record_3].find({'TBL_NO': {'$in': list(new_df['DATA_TABLE_STRING_TBL_NO_1'].values)}}, fields)))

        new_df = new_df.rename(columns={'DATA_TABLE_STRING_TBL_NO_1': 'TBL_NO'}).merge(record_3, on='TBL_NO',
                                                                                       how='left')

        return new_df
    else:
        return None


@measure(JUPITER_LOGGER)
def get_old_rules_df(new_df, record_3, fields, client):
    ATPCO = client[ATPCO_DB]
    query = []
    for index, doc in new_df.iterrows():
        query.append(
            {'CXR_CODE': doc['CXR_CODE'], 'RULE_NO': doc['RULE_NO'], 'TARIFF': doc['TARIFF'], 'CAT_NO': doc['CAT_NO'],
             'SEQ_NO': doc['SEQ_NO'],
             'LAST_UPDATED_DATE': {
                 '$lte': datetime.datetime.strftime(datetime.datetime.strptime(SYSTEM_DATE_, '%Y-%m-%d') -
                                                    datetime.timedelta(days=1), '%Y-%m-%d')},
             'DATES_DISC_1': {'$in': [doc['DATES_EFF_1'], datetime.datetime.strftime(datetime.datetime.strptime(
                 doc['DATES_EFF_1'], "%Y%m%d") - datetime.timedelta(days=1), "%Y%m%d")]}})

    old_df = pd.DataFrame()
    for j in range(int(math.ceil(len(query) / 1000.0))):
        temp = []
        print "Old Rules Iteration Count: ", j
        try:
            temp = query[j * 1000: (j + 1) * 1000]
        except IndexError:
            temp = query[j * 1000:]

        cur = ATPCO.JUP_DB_ATPCO_Record_2_Cat_All.aggregate([
            {"$match": {"$or": temp}},
            {"$project": {'CXR_CODE': 1, 'TARIFF': 1, 'CAT_NO': 1, 'RULE_NO': 1,
                          'DATA_TABLE_STRING_TBL_NO_1': 1, 'SEQ_NO': 1,
                          'LAST_UPDATED_DATE': 1, 'DATES_EFF_1': 1,
                          'DATES_DISC_1': 1, '_id': 0, 'ACTION': 1}}
        ])

        temp_df = pd.DataFrame(list(cur))
        old_df = pd.concat([old_df, temp_df])

    fields.update({'TBL_NO': 1, '_id': 0})
    print old_df.to_string()
    record_3 = pd.DataFrame(
        list(ATPCO[record_3].find({'TBL_NO': {'$in': list(old_df['DATA_TABLE_STRING_TBL_NO_1'].values)}}, fields)))

    old_df = old_df.rename(columns={'DATA_TABLE_STRING_TBL_NO_1': 'TBL_NO'}).merge(record_3, on='TBL_NO', how='left')

    return old_df


@measure(JUPITER_LOGGER)
def get_host_rules(df, cat, client):
    db = client[JUPITER_DB]
    mapping_df = pd.DataFrame(list(db.JUP_DB_City_Airport_Mapping.find({}, {'Airport_Code': 1,
                                                                            'City_Code': 1,
                                                                            '_id': 0})))
    df['host_rules'] = [[]] * len(df)
    for idx, row in df[df['ACTION_new'] == '3'].iterrows():
        comp_ods = db.JUP_DB_ATPCO_Fares_Rules.distinct('OD',
                                                        {'carrier': row['CXR_CODE'], 'tariff_code': row['TARIFF'],
                                                         'fare_rule': row['RULE_NO'],
                                                         '{}.SEQ_NO'.format(cat): row['SEQ_NO']})

        od_df = pd.DataFrame(comp_ods, columns=['od'])
        od_df['origin'] = od_df['od'].str.slice(0, 3)
        od_df['destination'] = od_df['od'].str.slice(3, 6)
        od_df = od_df.merge(mapping_df.rename(columns={'Airport_Code': 'origin', 'City_Code': 'pseudo_origin'}),
                            on='origin', how='left')
        od_df['pseudo_origin'].fillna(od_df['origin'], inplace=True)
        od_df = od_df.merge(
            mapping_df.rename(columns={'Airport_Code': 'destination', 'City_Code': 'pseudo_destination'}),
            on='destination', how='left')
        od_df['pseudo_destination'].fillna(od_df['destination'], inplace=True)
        od_df['pseudo_od'] = od_df['pseudo_origin'] + od_df['pseudo_destination']
        pseudo_ods = list(set(list(od_df['pseudo_od'].values)))

        host_rules = db.JUP_DB_ATPCO_Fares_Rules.distinct('fare_rule', {'carrier': Host_Airline_Code,
                                                                        'tariff_code': row['TARIFF'],
                                                                        'pseudo_od': {'$in': pseudo_ods},
                                                                        'fare_include': True})
        if len(host_rules) > 0:
            df.iloc[idx]['host_rules'] = host_rules

    return df
