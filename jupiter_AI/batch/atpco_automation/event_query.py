from jupiter_AI import client, JUPITER_DB, mongo_client, Host_Airline_Code, Host_Airline_Hub
import pandas as pd
from copy import deepcopy

# crsr = list(db.Temp_fzDB_tbl_004.aggregate([
#
#     {"$match":{
#         "update_date":{"$ne":None}
#
#
#     }},
#     {
#         "$project":{
#
#             "pos": "$pos",
#             "od": "$od",
#             "compartment":"$compartment",
#             "trigger_id":"$trigger_id",
#             "desc":"$desc"
#
#
#
#         }
#
#
#     }
# ]))
#
#
# for doc in crsr:
#     sp = doc['desc'].split('_')
#     # if len(sp) < 6:
#     #     print "Lenght less than 6:....."
#     #     print doc
#     st = [str(j) for j in sp]
#     flag = False
#
#     for i in st:
#         if len(i)<6:
#             #print "Events present......"
#             flag = True
#     if flag:
#         print "sjsjdbsjbdjsbjdjbjbj"
#         print doc

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

if __name__ == "__main__":
    client = mongo_client()
    db = client[JUPITER_DB]

    sig_markets, sub_sig_markets, non_sig_markets = segregate_markets()
    markets = list(set(sig_markets + sub_sig_markets))
    df = pd.DataFrame(markets, columns=['market'])
    df['od'] = df['market'].str.slice(3,9)
    list_ods = list(set(df['od'].values))
    list_dos = []
    for j in list_ods:
        d =j[:3]
        o =j[3:]
        od = o+d
        list_dos.append(od)
    final_ods = list(set(list_ods+list_dos))

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
    ods = list(od_df['od'].values) + \
          list(od_df['pseudo_od'].values) + \
          list(od_df['pseudo_origin_dest'].values) + \
          list(od_df['origin_pseudo_dest'].values)

    ods = list(set(ods))
    crsr = list(db.temp_fares_triggers.aggregate([
        {"$match":{
            "OD": {"$in": ods}

        }}
    ]))
    print len(crsr)



