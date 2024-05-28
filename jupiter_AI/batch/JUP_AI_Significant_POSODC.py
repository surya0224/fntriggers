"""
Author: Nikunj Agarwal
Created with <3
Date created: 2018-02-15
Description: Updating Significant flag for specific users at POS, OD, Compartment level
Modifications:

"""
from jupiter_AI import mongo_client, JUPITER_DB, SYSTEM_DATE, Host_Airline_Hub, JUPITER_LOGGER
from jupiter_AI.logutils import measure
from jupiter_AI.triggers.common import cursor_to_df
import datetime
import time
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta

SYSTEM_DATE_LY = (datetime.datetime.strptime(SYSTEM_DATE, '%Y-%m-%d') - relativedelta(years=1)).strftime('%Y-%m-%d')


@measure(JUPITER_LOGGER)
def get_mt_df(db):
    mt_cursor = db.JUP_DB_Manual_Triggers_Module_Summary.aggregate([
        {
            '$match': {
                'dep_date': {
                    '$gte': SYSTEM_DATE_LY,
                    '$lt': SYSTEM_DATE
                },
                'flown_revenue': {"$gt": 0},
                'flown_pax': {"$gt": 0}
            }
        }
        ,
        {
            '$group': {
                '_id':
                    {
                        'pos': '$pos.City',
                        'od': '$od',
                        'compartment': '$compartment'
                    },
                'revenue': {'$sum': '$flown_revenue'},
                'pax': {'$sum': '$flown_pax'}
            }
        }
        ,
        {
            '$project': {
                '_id': 0,
                'pos': '$_id.pos',
                'od': '$_id.od',
                'compartment': '$_id.compartment',
                'revenue': '$revenue',
                'pax': '$pax'
            }
        }
    ])
    print "Queried"
    mt_df = cursor_to_df(mt_cursor)
    mt_df = mt_df[~(mt_df['compartment'] == 'TL')]
    # mt_df.sort_values(by=['revenue', 'pax'], ascending=False, inplace=True)
    # mt_df.reset_index(drop=True, inplace=True)
    # mt_df.reset_index(inplace=True)
    # mt_df.rename(columns={'index': 'rank'}, inplace=True)
    # mt_df['rank'] += 1
    mt_df['market'] = mt_df['pos'] + mt_df['od'] + mt_df['compartment']
    return mt_df


@measure(JUPITER_LOGGER)
def get_host_od_capacity_ods(db):
    crsr = db.JUP_DB_Host_OD_Capacity.distinct('od', {'dep_date': {'$gte': SYSTEM_DATE}})
    host_ods = list(crsr)
    return host_ods


@measure(JUPITER_LOGGER)
def get_pax_markets(df):
    df_pax = df.sort_values(by='pax', ascending=False)
    df_pax['sum_pax'] = df_pax['pax'].sum()
    df_pax['pax_percent'] = df_pax['pax'] / df_pax['sum_pax'] * 100.0
    df_pax['pax_percent_cum'] = df_pax['pax_percent'].cumsum()
    df_pax.reset_index(inplace=True)
    sig_df_pax = df_pax[df_pax['pax_percent_cum'] <= 81]
    sig_markets_pax = list(sig_df_pax['market'].values)
    return sig_markets_pax


@measure(JUPITER_LOGGER)
def get_revenue_markets(df):
    df_revenue = df.sort_values(by='revenue', ascending=False)
    df_revenue['sum_revenue'] = df['revenue'].sum()
    df_revenue['revenue_percent'] = df_revenue['revenue'] / df_revenue['sum_revenue'] * 100.0
    df_revenue['revenue_percent_cum'] = df_revenue['revenue_percent'].cumsum()
    df_revenue.reset_index(inplace=True)
    sig_df_revenue = df_revenue[df_revenue['revenue_percent_cum'] <= 81]
    sig_markets_revenue = list(sig_df_revenue['market'].values)
    return sig_markets_revenue


@measure(JUPITER_LOGGER)
def get_dxb_markets(df):
    df['origin'] = df['od'].str.slice(0, 3)
    df['destination'] = df['od'].str.slice(3, 6)
    df_dxb = df[((df['origin'] == Host_Airline_Hub) | (df['destination'] == Host_Airline_Hub) |
                 (df['origin'] == 'DWC') | (df['destination'] == 'DWC')) &
                ((df['origin'] == df['pos']) | (df['destination'] == df['pos']))]
    sig_dxb = list(df_dxb['market'].values)
    sig_dxb_ods = list(df_dxb['od'].values)
    df.drop(['origin', 'destination'], axis=1, inplace=True)
    return sig_dxb, sig_dxb_ods


@measure(JUPITER_LOGGER)
def get_revenue_ods(df):
    df_revenue = df.sort_values(by='revenue', ascending=False)
    df_revenue['sum_revenue'] = df['revenue'].sum()
    df_revenue['revenue_percent'] = df_revenue['revenue'] / df_revenue['sum_revenue'] * 100.0
    df_revenue['revenue_percent_cum'] = df_revenue['revenue_percent'].cumsum()
    df_revenue.reset_index(inplace=True)
    sig_df_revenue = df_revenue[df_revenue['revenue_percent_cum'] <= 81]
    sig_ods_revenue = list(sig_df_revenue['od'].values)
    return sig_ods_revenue


@measure(JUPITER_LOGGER)
def get_pax_ods(df):
    df_pax = df.sort_values(by='pax', ascending=False)
    df_pax['sum_pax'] = df_pax['pax'].sum()
    df_pax['pax_percent'] = df_pax['pax'] / df_pax['sum_pax'] * 100.0
    df_pax['pax_percent_cum'] = df_pax['pax_percent'].cumsum()
    df_pax.reset_index(inplace=True)
    sig_df_pax = df_pax[df_pax['pax_percent_cum'] <= 81]
    sig_ods_pax = list(sig_df_pax['od'].values)
    return sig_ods_pax


@measure(JUPITER_LOGGER)
def get_online_pos(db):
    reg = pd.DataFrame(list(db.JUP_DB_Region_Master.find({"POS_TYPE": "Online"}, {"_id": 0, "POS_CD": 1})))
    mapping = pd.DataFrame(list(db.JUP_DB_City_Airport_Mapping.find({}, {"Airport_Code": 1, "City_Code": 1, "_id": 0})))
    mapping.rename(columns={'City_Code': 'POS_CD'}, inplace=True)
    df1 = reg.merge(mapping, on=['POS_CD'], how='left')
    mapping.rename(columns={'POS_CD': 'Airport_Code', 'Airport_Code': 'POS_CD'}, inplace=True)
    df2 = reg.merge(mapping, on=['POS_CD'], how='left')
    df1['Airport_Code'].fillna(df1['POS_CD'], inplace=True)
    df2['Airport_Code'].fillna(df2['POS_CD'], inplace=True)
    online_pos = list(set(list(df1['Airport_Code'].values) + list(df2['Airport_Code'].values)))
    return online_pos


@measure(JUPITER_LOGGER)
def get_user_defined_markets(db):
    crsr = db.JUP_DB_Thresholds.find({"item": "Significant Markets", "active": True,
                                      "$or": [{"effective_date_from": {"$lte": SYSTEM_DATE}},
                                              {"effective_date_from": ""}],
                                      "$or": [{"effective_date_to": {"$gte": SYSTEM_DATE}},
                                              {"effective_date_to": ""}]})
    user_defined_markets = []
    for i in crsr:
        user_defined_markets = i['user_defined_markets']
    return user_defined_markets


@measure(JUPITER_LOGGER)
def get_user_defined_ods(db):
    crsr = db.JUP_DB_Thresholds.find({"item": "Significant ODs", "active": True,
                                      "$or": [{"effective_date_from": {"$lte": SYSTEM_DATE}},
                                              {"effective_date_from": ""}],
                                      "$or": [{"effective_date_to": {"$gte": SYSTEM_DATE}},
                                              {"effective_date_to": ""}]})
    user_defined_ods = []
    for i in crsr:
        user_defined_ods = i['user_defined_ods']
    return user_defined_ods


@measure(JUPITER_LOGGER)
def main(client):
    db = client[JUPITER_DB]
    mt_df = get_mt_df(db)
    print "Got Manual Trigger DF"
    online_pos = get_online_pos(db)
    print 'Got Online POS'
    user_defined_markets = get_user_defined_markets(db)
    # user_defined_ods = get_user_defined_ods(db)
    print user_defined_markets
    # print user_defined_ods
    online_df = mt_df
    # print online_df.tail(5)
    user_defined_markets_df = pd.DataFrame(user_defined_markets, columns=['market'])
    user_defined_markets_df['pos'] = user_defined_markets_df['market'].str.slice(0, 3)
    user_defined_markets_df['od'] = user_defined_markets_df['market'].str.slice(3, 9)
    user_defined_markets_df['compartment'] = user_defined_markets_df['market'].str.slice(9, 10)
    user_defined_markets_df['pax'] = 0
    user_defined_markets_df['revenue'] = 0
    online_df = pd.concat([online_df, user_defined_markets_df])
    # print online_df.tail(5)
    online_df.drop_duplicates(subset=['market'], keep='first', inplace=True)
    # online_df.reset_index(drop=True, inplace=True)
    # print online_df.tail(5)
    online_df['online'] = False
    online_df['origin'] = online_df['od'].str.slice(0, 3)
    online_df['destination'] = online_df['od'].str.slice(3, 6)
    online_df.loc[(online_df['origin'].isin(online_pos) &
                   online_df['destination'].isin(online_pos) &
                   online_df['pos'].isin(online_pos)), "online"] = True
    online_df.drop(["origin", "destination"], axis=1, inplace=True)
    true_df = online_df[online_df['online'] == True]
    false_df = online_df[online_df['online'] == False]
    true_df.sort_values(by=['revenue', 'pax'], ascending=False, inplace=True)
    true_df.reset_index(drop=True, inplace=True)
    true_df.reset_index(inplace=True)
    true_df.rename(columns={'index': 'rank'}, inplace=True)
    true_df['rank'] += 1
    false_df['rank'] = 0
    online_df = pd.concat([true_df, false_df])
    # host_od_ods = get_host_od_capacity_ods()
    # host_od_df = pd.DataFrame({'od': host_od_ods, 'online': True})
    # print "Got HOST OD DF"
    # online_df = pd.merge(mt_df, host_od_df, on=['od'], how='left')
    # online_df['online'].fillna(False, inplace=True)
    online_df['significance'] = [[]] * len(online_df)
    users_cursor = db.JUP_DB_User.find({'list_of_pos.0': {'$exists': True}},
                                       {'name': 1, 'list_of_pos': 1, '_id': 0})
    if users_cursor.count() > 0:
        for user in users_cursor:
            user_name = user['name']
            pos_user = user['list_of_pos']
            temp_df = online_df[(online_df['online'] == True) & (online_df['pos'].isin(pos_user))]
            temp_df.drop(['significance'], axis=1, inplace=True)
            temp_df_2 = temp_df.groupby(by=['od'], as_index=False)['pax', 'revenue'].sum()
            pax_markets = get_pax_markets(temp_df)
            revenue_markets = get_revenue_markets(temp_df)
            dxb_markets, dxb_ods = get_dxb_markets(temp_df)
            pax_ods = get_pax_ods(temp_df_2)
            revenue_ods = get_revenue_ods(temp_df_2)
            sig_markets = list(set(pax_markets + revenue_markets + dxb_markets + user_defined_markets))
            sig_ods = list(set(pax_ods + revenue_ods + dxb_ods))
            # user_significant_df = temp_df[temp_df['market'].isin(sig_markets)]
            # user_significant_df['significance_temp'] = [[{'name': user_name, 'significant_flag': True,
            #                                               'od_significance': False}]]*len(user_significant_df)
            # user_significant_df.drop(['pax', 'revenue', 'online', 'pos', 'od', 'compartment', 'rank'], axis=1, inplace=True)
            # temp_df = pd.merge(temp_df, user_significant_df, on=['market'], how='left')
            # for row in temp_df.loc[temp_df['significance_temp'].isnull(), 'significance_temp'].index:
            #     temp_df.at[row, 'significance_temp'] = [{'name': user_name, 'significant_flag': False,
            #                                              'od_significance': False}]
            # print temp_df.columns
            # for idx, row in temp_df.iterrows():
            #     if row['od'] in sig_ods:
            #         temp_df.loc[idx,'significance_temp'][0].update({'od_significance': True})
            #         print row['significance_temp']
            #     else:
            #         row['significance_temp'][0].update({'od_significance': False})
            temp_df['significance_temp'] = [[{'name': user_name, 'od_significance': False,
                                              'significant_flag': False}]] * len(temp_df)

            temp_df.loc[(temp_df['od'].isin(sig_ods)) & (temp_df['market'].isin(sig_markets)),
                        'significance_temp'] = temp_df.loc[
                (temp_df['od'].isin(sig_ods)) & (temp_df['market'].isin(sig_markets)),
                'significance_temp'].apply(
                lambda x: [{'name': user_name, 'significant_flag': True, 'od_significance': True}])

            temp_df.loc[(~temp_df['od'].isin(sig_ods)) & (temp_df['market'].isin(sig_markets)),
                        'significance_temp'] = temp_df.loc[
                (~temp_df['od'].isin(sig_ods)) & (temp_df['market'].isin(sig_markets)),
                'significance_temp'].apply(
                lambda x: [{'name': user_name, 'significant_flag': True, 'od_significance': False}])

            temp_df.loc[(temp_df['od'].isin(sig_ods)) & (~temp_df['market'].isin(sig_markets)),
                        'significance_temp'] = temp_df.loc[
                (temp_df['od'].isin(sig_ods)) & (~temp_df['market'].isin(sig_markets)),
                'significance_temp'].apply(
                lambda x: [{'name': user_name, 'significant_flag': False, 'od_significance': True}])

            temp_df.drop(['pos', 'od', 'compartment', 'revenue', 'pax', 'online', 'rank'], axis=1, inplace=True)
            online_df = pd.merge(online_df, temp_df, on=['market'], how='left')
            for row in online_df.loc[online_df['significance_temp'].isnull(), 'significance_temp'].index:
                online_df.at[row, 'significance_temp'] = []
            online_df['significance'] += online_df['significance_temp']
            online_df.drop(['significance_temp'], axis=1, inplace=True)
            print 'Done for user = ', user_name
        db.JUP_DB_Market_Significance.remove()
        online_df['last_update_date'] = SYSTEM_DATE
        db.JUP_DB_Market_Significance.insert(online_df.to_dict('records'))
    else:  # No user defined in database. Ideally, this should not happen.
        print "No user definition. Something horribly wrong."
        pass

    return None


if __name__ == '__main__':
    st = time.time()
    client = mongo_client()
    main(client)
    client.close()
    print "Time taken for entire program = ", time.time() - st
