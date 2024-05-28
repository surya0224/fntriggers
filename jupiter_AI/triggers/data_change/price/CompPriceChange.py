"""
Author: Abhinav Garg
Created with <3
Date Created: 2017-10-20
Code functionality:
    Raises triggers whenever competitor changes its fare in a market beyond certain threshold
    Compare old filed fare and new filed fare of all competitors in host's markets
    and raise trigger if the difference between the two is beyond threshold.
Modifications log:
    1. Author: Abhinav Garg
       Exact modification made or some logic changed: Incorporated city airport mapping for competitor OD
       Date of modification: 2017-12-27
    2. Author:
       Exact modification made or some logic changed:
       Date of modification:
    3. Author:
       Exact modification made or some logic changed:
       Date of modification:

"""
import random

import pandas as pd
import numpy as np
import math
import datetime
from jupiter_AI import client, JUPITER_DB, SYSTEM_DATE, JUPITER_LOGGER, Host_Airline_Code, mongo_client
import jupiter_AI.common.ClassErrorObject as error_class
import traceback
import inspect
from jupiter_AI.triggers.common import get_threshold_values
from jupiter_AI.triggers.data_change.price.competitor import CompPriceChange
from jupiter_AI.triggers.listener_data_level_trigger import analyze
import time
from jupiter_AI.logutils import measure
#db = client[JUPITER_DB]

#fares_rules = db.JUP_DB_ATPCO_Fares_Rules

PCCP_TRIGGERS = []


@measure(JUPITER_LOGGER)
def get_arg_lists(frame):
    """
    function used to get the list of arguments of the function
    where it is called
    """
    args, _, _, values = inspect.getargvalues(frame)
    argument_name_list = []
    argument_value_list = []
    for k in args:
        argument_name_list.append(k)
        argument_value_list.append(values[k])
    return argument_name_list, argument_value_list


@measure(JUPITER_LOGGER)
def get_price_change_data(db):
    print 'start', SYSTEM_DATE
    fares_rules = db.JUP_DB_ATPCO_Fares_Rules
    cur = fares_rules.aggregate([
        {
            "$match":
                {"carrier": {"$ne": Host_Airline_Code},
                 "file_date": datetime.datetime.strftime(datetime.datetime.strptime(SYSTEM_DATE, '%Y-%m-%d') -
                                                         datetime.timedelta(days=1), '%Y%m%d'),
                 "OD": {"$in": get_od_list(db)},
                 "$or": [{"CHANGE_TAGS_2": "X"}, {"CHANGE_TAGS_3": "X"}]
                 }
        },
        {
            "$project":
                {'OD': 1, 'fare_basis': 1, 'fare_rule': 1, 'file_date': 1, 'footnote': 1, 'tariff_code': 1,
                 'carrier': 1, 'currency': 1, 'effective_from': 1, 'effective_to': 1, 'fare': 1,
                 'CHANGE_TAGS_2': 1, 'CHANGE_TAGS_3': 1, 'oneway_return': 1,
                 'cat_14': 1, 'Cat_14_FN': '$Footnotes.Cat_14_FN', 'compartment': 1}
        }
    ])

    new_fares_df = pd.DataFrame()
    for i in cur:
        new_fares_df = pd.concat([new_fares_df, pd.DataFrame([i])])

    try:
        new_fares_df['cat_14']
    except KeyError:
        new_fares_df['cat_14'] = -999
    try:
        new_fares_df['Cat_14_FN']
    except KeyError:
        new_fares_df['Cat_14_FN'] = -999
    new_fares_df['cat_14'].fillna(-999, inplace=True)
    new_fares_df['Cat_14_FN'].fillna(-999, inplace=True)

    new_fares_df['dep_dates'] = new_fares_df.apply(lambda row: dep_dates(row['cat_14'], row['Cat_14_FN']), axis=1)

    new_fares_df['dep_date_range'] = new_fares_df['dep_dates'].apply(lambda row: closest_dep_dates(row))
    new_fares_df['from_date'] = new_fares_df['dep_date_range'].str.slice(start=0, stop=6)
    new_fares_df['to_date'] = new_fares_df['dep_date_range'].str.slice(start=6, stop=12)
    print 'built new fares df'
    query = []
    for index, doc in new_fares_df.iterrows():
        query.append(
            {'OD': doc['OD'], 'carrier': doc['carrier'], 'fare_basis': doc['fare_basis'], 'fare_rule': doc['fare_rule'],
             'footnote': doc['footnote'], 'tariff_code': doc['tariff_code'], 'oneway_return': doc['oneway_return'],
             'currency': doc['currency'], 'file_date': {'$lte': datetime.datetime.strftime(datetime.datetime.strptime(SYSTEM_DATE, '%Y-%m-%d') -
                                                         datetime.timedelta(days=1), '%Y%m%d')}, 'compartment': doc['compartment'],
             'effective_to': {'$in': [doc['effective_from'], datetime.datetime.strftime(datetime.datetime.strptime(
                 doc['effective_from'], "%Y-%m-%d") - datetime.timedelta(days=1), "%Y-%m-%d")]}})

    old_fares_df = pd.DataFrame()
    for j in range(int(math.ceil(len(query) / 1000.0))):
        temp = []
        print "Old Fares Iteration Count: ", j
        try:
            temp = query[j * 1000: (j + 1) * 1000]
        except IndexError:
            temp = query[j * 1000:]

        cur = fares_rules.aggregate([
            {"$match": {"$or": temp}},
            {
                "$project":
                    {'OD': 1, 'fare_basis': 1, 'fare_rule': 1, 'file_date': 1, 'footnote': 1, 'tariff_code': 1, 'carrier': 1,
                     'currency': 1, 'effective_from': 1, 'effective_to': 1, 'fare': 1, 'CHANGE_TAGS_2': 1,
                     'CHANGE_TAGS_3': 1, 'oneway_return': 1, 'cat_14': 1, 'Cat_14_FN': '$Footnotes.Cat_14_FN',
                     'compartment': 1}
            }
        ])

        temp_df = pd.DataFrame(list(cur))
        old_fares_df = pd.concat([old_fares_df, temp_df])

    try:
        old_fares_df['cat_14']
    except KeyError:
        old_fares_df['cat_14'] = -999
    try:
        old_fares_df['Cat_14_FN']
    except KeyError:
        old_fares_df['Cat_14_FN'] = -999

    old_fares_df['cat_14'].fillna(-999, inplace=True)
    old_fares_df['Cat_14_FN'].fillna(-999, inplace=True)

    old_fares_df['dep_dates'] = old_fares_df.apply(lambda row: dep_dates(row['cat_14'], row['Cat_14_FN']), axis=1)

    old_fares_df['dep_date_range'] = old_fares_df['dep_dates'].apply(lambda row: closest_dep_dates(row))
    old_fares_df['from_date'] = old_fares_df['dep_date_range'].str.slice(start=0, stop=6)
    old_fares_df['to_date'] = old_fares_df['dep_date_range'].str.slice(start=6, stop=12)
    print 'built old fares df'
    temp = new_fares_df.groupby(['OD', 'fare_basis', 'carrier', 'fare_rule', 'footnote', 'tariff_code', 'oneway_return',
                                 'currency', 'compartment'])

    structured_dict = []
    df = pd.DataFrame()
    for index, grouped_df in temp:
        fares = list(grouped_df['fare'])
        eff_from = list(grouped_df['effective_from'])
        eff_to = list(grouped_df['effective_to'])
        zipped = zip(fares, eff_from, eff_to)
        constructed_field = []
        for item in zipped:
            if (item[2] > item[1]) or (item[2] == None):
                constructed_field.append({
                    'fare': item[0],
                    'effective_from': item[1],
                    'effective_to': item[2]
                })
        structured_dict.append({
            'OD': list(set(grouped_df['OD']))[0],
            'carrier': list(set(grouped_df['carrier']))[0],
            'fare_basis': list(set(grouped_df['fare_basis']))[0],
            'fare_rule': list(set(grouped_df['fare_rule']))[0],
            'footnote': list(set(grouped_df['footnote']))[0],
            'tariff_code': list(set(grouped_df['tariff_code']))[0],
            'oneway_return': list(set(grouped_df['oneway_return']))[0],
            'currency': list(set(grouped_df['currency']))[0],
            'compartment': list(set(grouped_df['compartment']))[0],
            'fares': constructed_field,
            'dep_date_range': sorted(list(set(grouped_df['dep_date_range'])), reverse=True)[0]
        })

    df = pd.DataFrame(structured_dict)

    final_df = pd.merge(df, old_fares_df,
                        on=['OD', 'carrier', 'fare_basis', 'fare_rule', 'footnote', 'tariff_code', 'oneway_return',
                            'currency', 'compartment'])

    final_df['fares'] = final_df.apply(lambda row: merge_fares(row['fare'], row['effective_from'], row['effective_to'],
                                                               row['fares']), axis=1)

    final_df['fares'] = final_df.apply(
        lambda row: sorted(row['fares'], key=lambda k: (k['effective_from'] + str(k['effective_to']))), axis=1)

    final_df.rename(columns={'dep_date_range_y': 'old_dep_date_range', 'dep_date_range_x': 'new_dep_date_range'},
                    inplace=True)

    final_df.drop(['CHANGE_TAGS_2', 'CHANGE_TAGS_3', 'fare', 'effective_from', 'effective_to', '_id', 'file_date',
                   'from_date', 'to_date', 'dep_dates', 'cat_14', 'Cat_14_FN'], axis=1, inplace=True)

    final_df['old_doc'] = final_df.apply(lambda row: old_new_fare_doc(row['fares'], 'old'), axis=1)
    final_df['new_doc'] = final_df.apply(lambda row: old_new_fare_doc(row['fares'], 'new'), axis=1)

    final_df['percent_change'] = final_df.apply(lambda row: percent_change(row['old_doc'], row['new_doc']), axis=1)

    final_df['flag'] = False

    print "final_df shape before city mapping --- ", final_df.shape[0]

    cur = list(db.JUP_DB_City_Airport_Mapping.find({}, {'Airport_Code': 1, 'City_Code': 1, '_id': 0}))
    final_df['origin'] = final_df.OD.str.slice(0, 3)
    final_df['destination'] = final_df.OD.str.slice(3, 6)
    final_df['Competitor_OD'] = final_df['OD']
    mapping_df = pd.DataFrame(cur)
    mapping_df['destination'] = mapping_df['Airport_Code']
    mapping_df.rename(columns={'Airport_Code': 'origin'}, inplace=True)
    final_df = pd.merge(final_df, mapping_df, on='origin', how='left')
    final_df.rename(columns={'destination_x': 'destination', 'City_Code': 'pseudo_origin'}, inplace=True)
    final_df.pseudo_origin.fillna(final_df['origin'], inplace=True)
    final_df.drop(['destination_y'], axis=1, inplace=True)
    final_df = final_df.merge(mapping_df, on='destination', how='left')
    final_df.fillna('', inplace=True)
    final_df.loc[final_df['City_Code'] == '', 'City_Code'] = final_df.loc[final_df['City_Code'] == '', 'destination']
    final_df.drop(['origin_y'], axis=1, inplace=True)
    final_df.rename(columns={'origin_x': 'origin', 'City_Code': 'pseudo_destination'}, inplace=True)
    mapping_df['pseudo_destination'] = mapping_df['City_Code']
    mapping_df.rename(columns={'City_Code': 'pseudo_origin'}, inplace=True)
    final_df = final_df.merge(mapping_df, on='pseudo_destination', how='left')
    final_df.drop(['destination_x', 'origin_y', 'pseudo_origin_y'], axis=1, inplace=True)
    final_df.rename(columns={'origin_x': 'origin', 'pseudo_origin_x': 'pseudo_origin', 'destination_y': 'destination'},
                    inplace=True)
    final_df.origin.fillna(final_df['pseudo_origin'], inplace=True)
    final_df = final_df.merge(mapping_df, on='pseudo_origin', how='left')
    final_df.drop(['destination_y', 'origin_x', 'pseudo_destination_y'], axis=1, inplace=True)
    final_df.rename(
        columns={'origin_y': 'origin', 'pseudo_destination_x': 'pseudo_destination', 'destination_x': 'destination'},
        inplace=True)
    final_df.destination.fillna(final_df['pseudo_destination'], inplace=True)
    final_df['OD'] = final_df['origin'] + final_df['destination']

    print 'final df shape after city mapping --- ', final_df.shape[0]
    print 'built final df, filtering for true df'
    print final_df['percent_change'], final_df['percent_change'].dtype

    upper, lower = get_threshold_values(trigger_type='competitor_price_change', db=db)

    final_df['pos'] = final_df['pseudo_origin']

    final_df.loc[(((final_df['percent_change'] >= float(upper)) |
                   (final_df['percent_change'] <= float(lower))) &
                  final_df.OD.isin(get_fz_ods(db=db))), 'flag'] = True

    true_df = final_df[final_df['flag'] == True]

    print true_df.head()

    true_df['dep_dates'] = true_df['new_dep_date_range'].apply(lambda row: change_format(row))

    return true_df


@measure(JUPITER_LOGGER)
def get_fz_ods(db):
    cur = db.JUP_DB_Host_OD_Capacity.distinct('od')
    od = list()
    for j in cur:
        od.append(j)
    return od


@measure(JUPITER_LOGGER)
def get_od_list(db):
    cur = list(db.JUP_DB_City_Airport_Mapping.find({}, {'Airport_Code': 1, 'City_Code': 1, '_id': 0}))
    od = list(db.JUP_DB_Host_OD_Capacity.distinct('od'))
    od_df = pd.DataFrame(od, columns=['od'])
    od_df['origin'] = od_df.od.str.slice(0, 3)
    od_df['destination'] = od_df.od.str.slice(3, 6)
    mapping_df = pd.DataFrame(cur)
    mapping_df['destination'] = mapping_df['Airport_Code']
    mapping_df.rename(columns={'Airport_Code': 'origin'}, inplace=True)
    temp_df = pd.merge(od_df, mapping_df, on='origin', how='left')
    temp_df.rename(columns={'destination_x': 'destination', 'City_Code': 'pseudo_origin'}, inplace=True)
    temp_df.pseudo_origin.fillna(temp_df['origin'], inplace=True)
    temp_df.drop(['destination_y'], axis=1, inplace=True)
    temp_df = temp_df.merge(mapping_df, on='destination', how='left')
    temp_df.fillna('', inplace=True)
    temp_df.loc[temp_df['City_Code'] == '', 'City_Code'] = temp_df.loc[temp_df['City_Code'] == '', 'destination']
    temp_df.drop(['origin_y'], axis=1, inplace=True)
    temp_df.rename(columns={'origin_x': 'origin', 'City_Code': 'pseudo_destination'}, inplace=True)
    mapping_df['pseudo_destination'] = mapping_df['City_Code']
    mapping_df.rename(columns={'City_Code': 'pseudo_origin'}, inplace=True)
    ods = temp_df.merge(mapping_df, on='pseudo_destination', how='left')
    ods.drop(['destination_x', 'origin_y', 'pseudo_origin_y'], axis=1, inplace=True)
    ods.rename(columns={'origin_x': 'origin', 'pseudo_origin_x': 'pseudo_origin', 'destination_y': 'destination'},
               inplace=True)
    ods.origin.fillna(ods['pseudo_origin'], inplace=True)
    ods = ods.merge(mapping_df, on='pseudo_origin', how='left')
    ods.drop(['destination_y', 'origin_x', 'pseudo_destination_y'], axis=1, inplace=True)
    ods.rename(
        columns={'origin_y': 'origin', 'pseudo_destination_x': 'pseudo_destination', 'destination_x': 'destination'},
        inplace=True)
    ods.destination.fillna(ods['pseudo_destination'], inplace=True)
    df_1 = ods.loc[:, ['origin', 'pseudo_destination']].rename(columns={'pseudo_destination': 'destination'})
    df_2 = ods.loc[:, ['pseudo_origin', 'destination']].rename(columns={'pseudo_origin': 'origin'})
    ods = pd.concat([ods, df_1, df_2])
    ods.pseudo_destination.fillna(ods['destination'], inplace=True)
    ods.pseudo_origin.fillna(ods['origin'], inplace=True)
    ods['od'] = ods['origin'] + ods['destination']
    ods.drop_duplicates(subset='od', inplace=True)
    od_list = ods.od.values
    print od_list.shape[0]

    return list(od_list)


@measure(JUPITER_LOGGER)
def dep_dates(cat_14, Cat_14_FN):
    if Cat_14_FN != -999:
        return Cat_14_FN
    elif cat_14 != -999:
        return cat_14
    else:
        return []


@measure(JUPITER_LOGGER)
def closest_dep_dates(dep_dates):
    if len(dep_dates) == 0:
        return '000000999999'
    else:
        dates = []
        for i in dep_dates:
            if i['TRAVEL_DATES_COMM'] in ['0999999', '9999999']:
                i['TRAVEL_DATES_COMM'] = '0000000'
            if i['TRAVEL_DATES_EXP'] in ['0000000', '0999999']:
                i['TRAVEL_DATES_EXP'] = '9999999'
            dates.append(i['TRAVEL_DATES_COMM'][1:] + i['TRAVEL_DATES_EXP'][1:])
        temp = sorted(dates)
        return temp[0]


@measure(JUPITER_LOGGER)
def merge_fares(fare, eff_from, eff_to, fares):
    if eff_from < eff_to or eff_to == None:
        fares.append({'fare': fare, 'effective_from': eff_from, 'effective_to': eff_to})
    return fares


@measure(JUPITER_LOGGER)
def old_new_fare_doc(row, flag):
    if flag == 'old' and len(row) > 1:
        return row[-2]
    elif flag == 'new' and len(row) > 1:
        return row[-1]
    else:
        return {}


@measure(JUPITER_LOGGER)
def percent_change(old_doc, new_doc):
    try:
        return (new_doc['fare'] - old_doc['fare']) * 100 / old_doc['fare']
    except KeyError:
        return 0


@measure(JUPITER_LOGGER)
def change_format(dep_date):
    dep_from = dep_date[0:6]
    dep_to = dep_date[6:]

    if dep_from <= datetime.datetime.strptime(SYSTEM_DATE, '%Y-%m-%d').strftime('%y%m%d'):
        dep_from = SYSTEM_DATE
    else:
        dep_from = datetime.datetime.strptime(dep_from, "%y%m%d").strftime("%Y-%m-%d")

    if dep_to >= datetime.datetime.strftime(datetime.datetime.strptime(dep_from, '%Y-%m-%d') + \
                                       datetime.timedelta(days=30), '%y%m%d'):
        dep_to = datetime.datetime.strftime(datetime.datetime.strptime(dep_from, '%Y-%m-%d') + \
                 datetime.timedelta(days=30), '%Y-%m-%d')
    else:
        dep_to = datetime.datetime.strptime(dep_to, "%y%m%d").strftime("%Y-%m-%d")

    return dep_from, dep_to


@measure(JUPITER_LOGGER)
def raise_price_change_triggers(db):

    try:
        true_df = get_price_change_data(db)
    except Exception as error_msg:
        db.JUP_DB_Errors.insert({"err_id": "PCCP",
                                 "error_name": str(error_msg.__class__.__name__),
                                 "error_message": str(error_msg.args[0])})
        true_df = pd.DataFrame()
        print traceback.format_exc()

    for idx, row in true_df.iterrows():
        try:
            print "------------->", row['percent_change'], row['carrier'], row['OD']
            old_doc = row['old_doc']
            new_doc = row['new_doc']
            old_doc['pos'] = row['pos']
            new_doc['pos'] = row['pos']
            old_doc['origin'] = row['OD'][0:3]
            old_doc['destination'] = row['OD'][3:]
            new_doc['origin'] = row['OD'][:3]
            new_doc['destination'] = row['OD'][3:]
            old_doc['fare_basis'] = row['fare_basis']
            old_doc['fare_rule'] = row['fare_rule']
            old_doc['footnote'] = row['footnote']
            old_doc['currency'] = row['currency']
            old_doc['carrier'] = row['carrier']
            old_doc['oneway_return'] = row['oneway_return']
            old_doc['tariff_code'] = row['tariff_code']
            new_doc['fare_basis'] = row['fare_basis']
            new_doc['fare_rule'] = row['fare_rule']
            new_doc['footnote'] = row['footnote']
            new_doc['currency'] = row['currency']
            new_doc['carrier'] = row['carrier']
            new_doc['oneway_return'] = row['oneway_return']
            new_doc['tariff_code'] = row['tariff_code']
            new_doc['dep_from'] = row['dep_dates'][0]
            new_doc['dep_to'] = row['dep_dates'][1]
            new_doc['pct_chng'] = row['percent_change']
            new_doc['compartment'] = row['compartment']
            old_doc['compartment'] = row['compartment']
            old_doc['Competitor_OD'] = row['Competitor_OD']
            new_doc['Competitor_OD'] = row['Competitor_OD']
            cur = list(db.temp_sig_markets.find({'market': row['pos'] + row['OD'] + row['compartment']}, {'sig_flag': 1, '_id': 0}))
            if len(cur) != 0:
                obj = CompPriceChange('competitor_price_change', old_doc, new_doc)
                id = obj.do_analysis(db=db)
                PCCP_TRIGGERS.append(id)
        except Exception as error_msg:
            print "ERROR!!!"
            db.JUP_DB_Errors.insert({"err_id": "PCCP",
                                     "error_name": str(error_msg.__class__.__name__),
                                     "error_message": str(error_msg.args[0])})
            module_name = ''.join(['jupiter_AI/triggers/data_change/price/CompPriceChange.py ',
                                   'method: raise_price_change_trigger'])
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                module_name,
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list(str(error_msg))
            obj_error.write_error_logs(datetime.datetime.now())
            traceback.print_exc()
    return PCCP_TRIGGERS


@measure(JUPITER_LOGGER)
def run(db):
    #db = client[JUPITER_DB]
    print 'Raising price change triggers'
    st = time.time()
    id_list = raise_price_change_triggers(db)
    print 'Raised price change triggers in ', time.time() - st
    TRIGGERS_ALL = id_list
    df = pd.DataFrame()
    df['Triggers'] = TRIGGERS_ALL
    df.dropna(inplace=True)
    TRIGGERS_ALL = df['Triggers'].values
    print "Total NUMBER of TRIGGERS RAISED = ", len(TRIGGERS_ALL)
    random.shuffle(TRIGGERS_ALL)
    count_analyzed = 1
    triggers_wasted = 0
    for trigger in TRIGGERS_ALL:
        # try:
        if trigger is not None:
            analyze(db=db, id=trigger)
        print "Analyzed TRIGGER NUMBER ", count_analyzed, " out of ", len(TRIGGERS_ALL)
        # except Exception as e:
        #     print traceback.print_exc()
        #     db.JUP_DB_Errors.insert({"err_id": str(trigger),
        #                              "error_name": str(e.__class__.__name__),
        #                              "error_message": str(e.args[0])})
        #     triggers_wasted += 1
        count_analyzed += 1
    print "Number of triggers wasted due to some ERROR In some function = ", triggers_wasted, " out of TOTAL TRIGGERS = ", len(TRIGGERS_ALL)


if __name__ == "__main__":
    client = mongo_client()
    db = client[JUPITER_DB]
    run(db=db)
    client.close()
    # true_df = get_price_change_data()