"""
File Name              :   JUP_DB_Batch_Significant_OD.py
Author                 :   Sai Krishna had written it. Modified by Nikunj Agarwal.
Date Created           :   2017-02-10
Description            :   Updating the significant_flag, revenue, pax YTD in the JUP_DB_OD_Master Screen
Data access layer

MODIFICATIONS LOG
"""
from jupiter_AI.tiles.jup_common_functions_tiles import gen_collection_name
from jupiter_AI import client, JUPITER_DB, SYSTEM_DATE, JUPITER_LOGGER
from jupiter_AI.logutils import measure
import datetime
import time
import pandas as pd
from dateutil.relativedelta import relativedelta
db = client[JUPITER_DB]



@measure(JUPITER_LOGGER)
def get_host_od_capacity_ods():
    crsr = db.JUP_DB_Host_OD_Capacity.distinct('od')
    host_ods = list(crsr)
    return host_ods


@measure(JUPITER_LOGGER)
def get_manual_trigger_ods():
    crsr = db.JUP_DB_Manual_Triggers_Module.distinct('od')
    return list(crsr)



@measure(JUPITER_LOGGER)
def get_revenue_pax_df():
    SYSTEM_DATE_LY = (datetime.datetime.strptime(SYSTEM_DATE, '%Y-%m-%d') - relativedelta(years=1)).strftime('%Y-%m-%d')
    crsr = db.JUP_DB_Manual_Triggers_Module.aggregate([
        {
            '$match': {
                'dep_date': {
                    '$gte': SYSTEM_DATE_LY,
                    '$lt': SYSTEM_DATE
                },
                'flown_revenue.value': {"$gt": 0},
                'flown_pax.value': {"$gt": 0}
            }
        }
        # Grouping on the basis of OD ans summing up the revenue and pax
        ,
        {
            '$group': {
                '_id': '$od',
                'revenue': {'$sum': '$flown_revenue.value'},
                'pax': {'$sum': '$flown_pax.value'}
            }
        }
        ,
        {
            '$project': {
                '_id': 0,
                'od': '$_id',
                'revenue': '$revenue',
                'pax': '$pax'
            }
        }
    ])
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
    sig_ods_pax = list(sig_df_pax['od'].values)
    return sig_ods_pax


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
def get_dxb_ods(df):
    df['origin'] = df['od'].str.slice(0, 3)
    df['destination'] = df['od'].str.slice(3, 6)
    df_dxb = df[(df['origin'] == 'DXB') | (df['destination'] == 'DXB')]
    sig_dxb = list(df_dxb['od'].values)
    return sig_dxb


@measure(JUPITER_LOGGER)
def main_new():
    host_od_ods = get_host_od_capacity_ods()
    online_df = pd.DataFrame({'od': host_od_ods, 'online': True})
    print "Got online DF"
    manual_trigger_ods = get_manual_trigger_ods()
    mt_df = pd.DataFrame({'od': manual_trigger_ods})
    print "Got MT ODs"
    total_df = pd.merge(mt_df, online_df, on='od', how='left')
    total_df['online'].fillna(False, inplace=True)
    print "Merged online field with MT ods"
    df = get_revenue_pax_df()
    print "Got revenue_pax_df"
    total_df = pd.merge(total_df, df, on=['od'], how='left')
    pax_ods = get_pax_ods(df)
    revenue_ods = get_revenue_ods(df)
    dxb_ods = get_dxb_ods(total_df)
    sig_ods = list(set(pax_ods + revenue_ods + dxb_ods))
    print "Got significant ODs"
    total_df['significant_flag'] = False
    total_df.loc[total_df['od'].isin(sig_ods), 'significant_flag'] = True
    print "Updated significant flag"
    total_df['last_update_date'] = SYSTEM_DATE
    total_df.to_csv('od_master_df.csv')
    db.JUP_DB_OD_Master_new.insert(total_df.to_dict('records'))
    print "Inserted into collection OD_Master"
    return None


if __name__ == '__main__':
    st = time.time()
    db.JUP_DB_OD_Master_new.remove()
    main_new()
    print "Total time taken = ", time.time() - st





