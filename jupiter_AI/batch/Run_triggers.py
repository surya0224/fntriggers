import json
import datetime
import logging
import random
import time
import rabbitpy
import pandas as pd
from jupiter_AI import RABBITMQ_HOST, RABBITMQ_PASSWORD, RABBITMQ_PORT, RABBITMQ_USERNAME, mongo_client, JUPITER_DB, \
    Host_Airline_Hub, JUPITER_LOGGER
from jupiter_AI import SYSTEM_DATE, today, parameters
from jupiter_AI.batch.atpco_automation.Automation_tasks import run_booking_triggers, \
    run_events_triggers, \
    run_pax_triggers, \
    run_revenue_triggers, \
    run_yield_triggers, \
    run_opp_trend_triggers, \
    run_pccp_triggers

from celery import group
import pika

from jupiter_AI.logutils import measure
from copy import deepcopy
url = 'amqp://' + RABBITMQ_USERNAME + \
      ":" + RABBITMQ_PASSWORD + \
      "@" + RABBITMQ_HOST + ":" + str(RABBITMQ_PORT) + "/"

#db = client[JUPITER_DB]
today_1 = today - datetime.timedelta(days=1)


@measure(JUPITER_LOGGER)
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

    # return list(sig_markets_df['market'].values), \
    #        list(sub_sig_markets_df['market'].values), \
    #        list(non_sig_markets_df['market'].values)


@measure(JUPITER_LOGGER)
def data_level_triggers_batch():
    client = mongo_client()
    db = client[JUPITER_DB]
    st = time.time()
    print "Running Data Level Triggers"
    sig_markets_list, sub_sig_markets_list, non_sig_markets_list = segregate_markets()
    sig_markets_df = pd.DataFrame(sig_markets_list, columns=['market'])
    sub_sig_markets_df = pd.DataFrame(sub_sig_markets_list, columns=['market'])
    non_sig_markets_df = pd.DataFrame(non_sig_markets_list, columns=['market'])
    sig_markets_df['pos'] = sig_markets_df['market'].str.slice(0, 3)
    sig_markets_df['od'] = sig_markets_df['market'].str.slice(3, 9)
    sig_markets_df['compartment'] = sig_markets_df['market'].str.slice(9, 10)
    sig_markets_df['sig_flag'] = 'sig'
    sub_sig_markets_df['pos'] = sub_sig_markets_df['market'].str.slice(0, 3)
    sub_sig_markets_df['od'] = sub_sig_markets_df['market'].str.slice(3, 9)
    sub_sig_markets_df['compartment'] = sub_sig_markets_df['market'].str.slice(9, 10)
    sub_sig_markets_df['sig_flag'] = 'sub_sig'
    non_sig_markets_df['pos'] = non_sig_markets_df['market'].str.slice(0, 3)
    non_sig_markets_df['od'] = non_sig_markets_df['market'].str.slice(3, 9)
    non_sig_markets_df['compartment'] = non_sig_markets_df['market'].str.slice(9, 10)
    non_sig_markets_df['sig_flag'] = 'non_sig'
    insert_df = pd.concat([sig_markets_df, sub_sig_markets_df])
    insert_df = insert_df[['market', 'pos', 'od', 'compartment', 'sig_flag']]

    sig_markets = len(sig_markets_df)
    sub_sig_markets = len(sub_sig_markets_df)
    non_sig_markets = len(non_sig_markets_df)

    online_mrkts = list(set(list(sig_markets_df['market'].values) +
                            list(sub_sig_markets_df['market'].values)))

    print "Total sig markets = ", sig_markets
    print "Total sub sig markets = ", sub_sig_markets
    print "Total non sig markets = ", non_sig_markets

    db.temp_sig_markets.remove({})
    db.temp_sig_markets.insert(insert_df.to_dict("records"))
    print 'Inserted markets to temp_sig_markets collection'

    # # online_mrkts = ['VKOVKODXBY', 'VKOVKODXBJ', 'SKGSKGDXBY', 'SKGSKGDXBJ', 'KHIKHIJEDY', 'KHIKHIJEDJ',
    # #                 'CMBCMBJEDY', 'CMBCMBJEDJ', 'CGPCGPDXBY', 'CGPCGPDXBJ', 'AMMAMMDXBY', 'AMMAMMDXBJ']
    trigger_group = []
    counter = 0
    markets = []
    client.close()
    print 'Appending triggers that are independent of markets'
    trigger_group.append(run_pccp_triggers.s())
    print 'Appending triggers that are dependent on markets'
    for mrkt in list(sig_markets_df['market'].values):
        if counter == 100:

            trigger_group.append(run_booking_triggers.s(markets=markets, sig_flag='sig'))
            trigger_group.append(run_pax_triggers.s(markets=markets, sig_flag='sig'))
            trigger_group.append(run_revenue_triggers.s(markets=markets, sig_flag='sig'))
            trigger_group.append(run_yield_triggers.s(markets=markets, sig_flag='sig'))
            trigger_group.append(run_events_triggers.s(markets=markets))
            trigger_group.append(run_opp_trend_triggers.s(markets=markets, sig_flag='sig'))
            markets = []
            markets.append(mrkt)
            counter = 1

        else:
            markets.append(mrkt)
            counter += 1

    if counter > 0:
        trigger_group.append(run_booking_triggers.s(markets=markets, sig_flag='sig'))
        trigger_group.append(run_pax_triggers.s(markets=markets, sig_flag='sig'))
        trigger_group.append(run_revenue_triggers.s(markets=markets, sig_flag='sig'))
        trigger_group.append(run_yield_triggers.s(markets=markets, sig_flag='sig'))
        trigger_group.append(run_events_triggers.s(markets=markets))
        trigger_group.append(run_opp_trend_triggers.s(markets=markets, sig_flag='sig'))
    print "Appended Sig Markets"

    counter = 0
    markets = []
    for mrkt in list(sub_sig_markets_df['market'].values):
        if counter == 100:

            trigger_group.append(run_booking_triggers.s(markets=markets, sig_flag='sub_sig'))
            trigger_group.append(run_pax_triggers.s(markets=markets, sig_flag='sub_sig'))
            trigger_group.append(run_revenue_triggers.s(markets=markets, sig_flag='sub_sig'))
            trigger_group.append(run_yield_triggers.s(markets=markets, sig_flag='sub_sig'))
            trigger_group.append(run_events_triggers.s(markets=markets))
            trigger_group.append(run_opp_trend_triggers.s(markets=markets, sig_flag='sub_sig'))
            markets = []
            markets.append(mrkt)
            counter = 1

        else:
            markets.append(mrkt)
            counter += 1

    if counter > 0:
        trigger_group.append(run_booking_triggers.s(markets=markets, sig_flag='sub_sig'))
        trigger_group.append(run_pax_triggers.s(markets=markets, sig_flag='sub_sig'))
        trigger_group.append(run_revenue_triggers.s(markets=markets, sig_flag='sub_sig'))
        trigger_group.append(run_yield_triggers.s(markets=markets, sig_flag='sub_sig'))
        trigger_group.append(run_events_triggers.s(markets=markets))
        trigger_group.append(run_opp_trend_triggers.s(markets=markets, sig_flag='sub_sig'))
    print "Appended Sub Sig Markets"

    random.shuffle(trigger_group)
    trig_group = group(trigger_group)
    res_group = trig_group()
    trig_grp_res = res_group.get()
    print 'Total time taken to raise and analyze triggers:', time.time() - st


if __name__=="__main__":

    data_level_triggers_batch()