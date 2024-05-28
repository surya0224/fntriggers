from jupiter_AI import mongo_client, JUPITER_DB, query_month_year_builder, Host_Airline_Code, SYSTEM_DATE, JUPITER_LOGGER
# from jupiter_AI.BaseparametersCodes.common import get_ly_val
from jupiter_AI.batch.JUP_AI_Batch_Top5_Competitors import obtain_top_5_comp
from jupiter_AI.logutils import measure
from jupiter_AI.triggers.common import pos_level_currency_data as get_currency_data

#db = client[JUPITER_DB]
import time
import json
import datetime
from jupiter_AI.triggers.recommendation_models.oligopoly_fl_ge import get_host_fares_df
import pandas as pd
import numpy as np


@measure(JUPITER_LOGGER)
def get_ly_val(date_str, method='exact', year_diff=-1):
    """
    :param date_str:
    :param method:
    :return:
    """
    assert type(year_diff) in [
        int], 'Invalid Input Type(year_diff) ::Expected int obtained ' + str(type(year_diff))
    assert method == 'exact' or method == '-364', 'Invalid Input(method param) :: Expected exact or -364 obtained ' + str(
        method)
    assert date_str, 'Date should not be None'
    if date_str:
        try:
            date_str_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError as error:
            raise error
        if method == 'exact':
            ly_str_obj = date_str_obj.replace(year=2016)
            ly_date_str = datetime.datetime.strftime(ly_str_obj, '%Y-%m-%d')
            return ly_date_str
        elif method == '-364':
            ly_date_obj = date_str_obj + datetime.timedelta(days=-364)
            ly_date_str = datetime.datetime.strftime(ly_date_obj, '%Y-%m-%d')
            return ly_date_str


@measure(JUPITER_LOGGER)
def get_ms_dF(
        pos,
        origin,
        destination,
        compartment,
        dep_date_start,
        dep_date_end,
        db,
        date_list=None):
    st = time.time()
    # ms_dF = pd.DataFrame(columns = ['carrier', 'market_share', 'pax'])
    dep_date_start_obj = datetime.datetime.strptime(dep_date_start, "%Y-%m-%d")
    dep_date_end_obj = datetime.datetime.strptime(dep_date_end, "%Y-%m-%d")
    # print dep_date_start_obj
    # print dep_date_end_obj

    sm = dep_date_start_obj.month
    sy = dep_date_start_obj.year
    em = dep_date_end_obj.month
    ey = dep_date_end_obj.year
    # print sm,sy,em,ey

    mnth_yr_combinations = query_month_year_builder(
        stdm=sm, stdy=sy, endm=em, endy=ey)
    # print mnth_yr_combinations

    ms_query = {
        '$and':
            [
                {'od': origin + destination},
                {'compartment': compartment},
                {'$or': mnth_yr_combinations}
            ]
    }
    if pos:
        if pos == 'DXB':
            ms_query['$and'].append({'pos': {'$in': ['UAE', 'DXB']}})
        else:
            ms_query['$and'].append({'pos': pos})

    # print ms_query

    ms_crsr = db.JUP_DB_Market_Share.find(
        ms_query, {"MarketingCarrier1": 1, "pax": 1, "month": 1, "_id": 0})

    # ms_data = list(db.JUP_DB_Market_Share.aggregate(
    #
    #     # Pipeline
    #     [
    #         # Stage 1
    #         {
    #             "$match": ms_query
    #         },
    #
    #         # Stage 2
    #         {
    #             '$group': {
    #                 '_id':'$MarketingCarrier1',
    #                 'pax':{'$sum':'$pax'}
    #             }
    #         },
    #
    #         # Stage 3
    #         {
    #             '$group': {
    #                 '_id':None,
    #                 'carrier_det':{
    #                     '$push':'$$ROOT'
    #                 },
    #                 'market_size':{'$sum':'$pax'}
    #             }
    #         },
    #
    #         # Stage 4
    #         {
    #             '$unwind': '$carrier_det'
    #         },
    #
    #         # Stage 5
    #         {
    #             '$project': {
    #                 # specifications
    #                 '_id':0,
    #                 'carrier':'$carrier_det._id',
    #                 'pax':'$carrier_det.pax',
    #                 'market_share':{
    #                     '$cond':
    #                         {
    #                             'if':{'$gt':['$market_size',0]},
    #                             'then':{'$multiply':[{'$divide':['$carrier_det.pax','$market_size']}, 100]},
    #                             'else':'NA'
    #                         }
    #                 }
    #             }
    #         },
    #
    #     ]
    #
    #     # Created with Studio 3T, the IDE for MongoDB - https:#studio3t.com/
    # ))
    print "getting data"
    ms_data = pd.DataFrame(list(ms_crsr))
    # print ms_data
    print "got data"
    final_data = pd.DataFrame()
    if len(ms_data) > 0:
        ms_data = ms_data.groupby(by=['MarketingCarrier1', 'month'], as_index=False)[
            'pax'].sum()
        ms_data.sort_values(by='pax', inplace=True, ascending=False)
        # print "ms_data:----->"
        # print ms_data
        # list_ms = []
        for date_ in date_list:
            sm_1 = int(date_['start'][5:7])
            em_1 = int(date_['end'][5:7])
            # print sm_1, em_1
            # if sm_1 == em_1:
            tmp = ms_data[(ms_data['month'] >= sm_1) &
                          (ms_data['month'] <= em_1)]
            # print tmp
            tmp = tmp.groupby(
                by='MarketingCarrier1',
                as_index=False)['pax'].sum().sort_values(
                by='pax',
                ascending=False)
            mrkt_size = tmp['pax'].sum()
            # print mrkt_size
            tmp['market_share'] = tmp['pax'] / mrkt_size
            tmp['date_start'] = date_['start']
            tmp['date_end'] = date_['end']
            final_data = pd.concat([final_data, tmp], ignore_index=True)
            # list_ms.append(tmp.to_dict("records"))
    # if len(ms_data) > 0:
    #     ms_dF = pd.DataFrame(ms_data)
    # print ms_dF
    # print time.time() - st
    st = time.time()
    # return ms_dF
    return final_data


@measure(JUPITER_LOGGER)
def get_lowest_filed_fare_dF(
        pos,
        origin,
        destination,
        compartment,
        dep_date_start,
        dep_date_end,
        db,
        comp,
        date_list=None):
    currency_data = get_currency_data(db=db)
    # print "lowest comp: ", comp

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
    lowest_fares_d_ = get_host_fares_df(
        pos,
        origin,
        destination,
        compartment,
        dep_date_start,
        dep_date_end,
        db=db,
        comp=comp_list,
        channel='gds')
    if len(lowest_fares_d_) > 0:
        lowest_fares_d_ = lowest_fares_d_[lowest_fares_d_['is_expired'] == 0]
    # print "len of lowest fares ", len(lowest_fares_d_)
    # print lowest_fares_d_
    curr_list = lowest_fares_d_['currency'].unique()
    if "YR" not in lowest_fares_d_.columns:
        lowest_fares_d_['YR'] = 0
    final_data = pd.DataFrame()
    count = 0
    for date_ in date_list:
        # print "comp: ", comp[count]
        comp_li_date = comp[count][0]
        comp_li_date.append(Host_Airline_Code)
        lowest_fares_d = lowest_fares_d_[
            (lowest_fares_d_['travel_date_from'] <= date_['end']) & (
                lowest_fares_d_['travel_date_to'] >= date_['start'])]
        lowest_fares_d['Average_fare'] = lowest_fares_d['fare'] + lowest_fares_d['YQ'] + \
            lowest_fares_d['YR'] + lowest_fares_d['Average_surcharge'] + lowest_fares_d['taxes']
        lowest_fares_d['total_fare_1'] = lowest_fares_d['fare'] + lowest_fares_d['YQ'] + \
            lowest_fares_d['YR'] + lowest_fares_d['surcharge_amount_1'] + lowest_fares_d['taxes']
        lowest_fares_d['total_fare_2'] = lowest_fares_d['fare'] + lowest_fares_d['YQ'] + \
            lowest_fares_d['YR'] + lowest_fares_d['surcharge_amount_2'] + lowest_fares_d['taxes']
        lowest_fares_d['total_fare_3'] = lowest_fares_d['fare'] + lowest_fares_d['YQ'] + \
            lowest_fares_d['YR'] + lowest_fares_d['surcharge_amount_3'] + lowest_fares_d['taxes']
        lowest_fares_d['total_fare_4'] = lowest_fares_d['fare'] + lowest_fares_d['YQ'] + \
            lowest_fares_d['YR'] + lowest_fares_d['surcharge_amount_4'] + lowest_fares_d['taxes']
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
                    result['carrier'] = competitor
                    result['currency'] = curr
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
        if len(filed_df) > 0:
            final_data = filed_df.merge(gds_df, on=['carrier', 'date_start', 'date_end'], how='left', suffixes=("", "_gds"))
        else:
            gds_df = pd.DataFrame(columns=["currency",
                    "highest_fare_YQ",
                    "highest_fare_YR",
                    "highest_fare_base",
                    "highest_fare_fb",
                    "highest_fare_surcharge",
                    "highest_fare_tax",
                    "highest_fare_total",
                    "lowest_fare_YQ",
                    "lowest_fare_YR",
                    "lowest_fare_base",
                    "lowest_fare_fb",
                    "lowest_fare_surcharge",
                    "lowest_fare_tax",
                    "lowest_fare_total",
                    "highest_fare_YQ_rt",
                    "highest_fare_YR_rt",
                    "highest_fare_base_rt",
                    "highest_fare_fb_rt",
                    "highest_fare_surcharge_rt",
                    "highest_fare_tax_rt",
                    "highest_fare_total_rt",
                    "lowest_fare_YQ_rt",
                    "lowest_fare_YR_rt",
                    "lowest_fare_base_rt",
                    "lowest_fare_fb_rt",
                    "lowest_fare_surcharge_rt",
                    "lowest_fare_tax_rt",
                    "lowest_fare_total_rt",
                    "carrier",
                    "date_start",
                    "date_end"])
            final_data = final_data.merge(gds_df, on=['carrier', 'date_start', 'date_end'], how='left', suffixes=("_gds", ""))
    # print lowest_fares_dF.columns
    # print time.time() - st
    # print final_data
    return final_data


@measure(JUPITER_LOGGER)
def get_capacity_dF(
        pos,
        origin,
        destination,
        compartment,
        dep_date_start,
        dep_date_end,
        db,
        date_list=None):
    st = time.time()
    capacity_dF = pd.DataFrame(columns=['carrier', 'capacity'])

    dep_date_start_obj = datetime.datetime.strptime(dep_date_start, "%Y-%m-%d")
    dep_date_end_obj = datetime.datetime.strptime(dep_date_end, "%Y-%m-%d")
    # print dep_date_start_obj
    # print dep_date_end_obj

    sm = dep_date_start_obj.month
    sy = dep_date_start_obj.year
    em = dep_date_end_obj.month
    ey = dep_date_end_obj.year
    # print sm,sy,em,ey

    mnth_yr_combinations = query_month_year_builder(
        stdm=sm, stdy=sy, endm=em, endy=ey)
    # print mnth_yr_combinations
    #
    # comp_capacity_d = list(db.JUP_DB_OD_Capacity.aggregate(
    #
    #     # Pipeline
    #     [
    #         # Stage 1
    #         {
    #             '$match': {
    #                 '$and':
    #                     [
    #                         {'airline':{'$ne':'FZ'}},
    #                         {'od':origin + destination},
    #                         {'compartment':compartment},
    #                         {'$or':mnth_yr_combinations}
    #                     ]
    #             }
    #         },
    #
    #         # Stage 2
    #         {
    #             '$sort': {
    #                 'last_update_date':-1
    #             }
    #         },
    #
    #         # Stage 3
    #         {
    #             '$group': {
    #                 '_id':{
    #                     'od':'$od',
    #                     'compartment':'$compartment',
    #                     'airline':'$airline',
    #                     'month':'$month',
    #                     'year':'$year'
    #                 },
    #                 'capacity':{'$first':'$capacity'}
    #             }
    #         },
    #
    #         # Stage 4
    #         {
    #             '$group': {
    #                 '_id':'$_id.airline',
    #                 'capacity':{'$sum':'$capacity'}
    #             }
    #         },
    #
    #         # Stage 5
    #         {
    #             '$project':{
    #                 '_id':0,
    #                 'carrier':'$_id',
    #                 'capacity':'$capacity'
    #             }
    #         }
    #
    #     ]
    #
    #     # Created with Studio 3T, the IDE for MongoDB - https:#studio3t.com/
    # ))

    """
    {
                '$group': {
                    '_id': {
                        'airline': '$airline',
                        'month': '$month',
                        'year': '$year'
                    },
                    'capacity': {'$sum': '$capacity'}
                }
            },
            {
                '$group': {
                    '_id': '$airline',
                    'capacity': {
                        '$push':
                            {
                                'month':'$month',
                                'year':'$year',
                                'capacity':'$capacity'
                            }
                    }
                }
            },
    """
    comp_capacity_d = list(db.JUP_DB_OD_Capacity.aggregate(

        # Pipeline
        [
            # Stage 1
            {
                '$match': {
                    '$and':
                        [
                            {'airline': {'$ne': Host_Airline_Code}},
                            {'od': origin + destination},
                            {'compartment': compartment},
                            {'$or': mnth_yr_combinations}
                        ]
                }
            },

            # Stage 2
            {
                '$sort': {
                    'last_update_date': -1
                }
            },

            # Stage 3
            {
                '$group': {
                    '_id': {
                        'od': '$od',
                        'compartment': '$compartment',
                        'airline': '$airline',
                        'month': '$month',
                        'year': '$year'
                    },
                    'capacity': {'$first': '$capacity'}
                }
            },

            # Stage 5
            {
                '$project': {
                    '_id': 0,
                    'carrier': '$_id.airline',
                    'capacity': '$capacity',
                    'month': "$_id.month",
                    'year': "$_id.year",

                }
            }

        ]

        # Created with Studio 3T, the IDE for MongoDB - https:#studio3t.com/
    ))
    # host_od_capacity = list(db.JUP_DB_Host_OD_Capacity.aggregate(
    #
    #     # Pipeline
    #     [
    #         # Stage 1
    #         {
    #             '$match': {
    #                 'od':origin + destination,
    #                 'dep_date':{'$gte':dep_date_start,'$lte':dep_date_end}
    #             }
    #         },
    #
    #         # Stage 2
    #         {
    #             '$sort': {
    #                 'last_update_date':-1
    #             }
    #         },
    #
    #         # Stage 3
    #         {
    #             '$group': {
    #                 '_id':{
    #                     'od':'$od',
    #                     'dep_date':'$dep_date'
    #                 },
    #                 'y_capacity':{'$first':'$y_cap'},
    #                 'j_capacity':{'$first':'$j_cap'}
    #             }
    #         },
    #
    #         # Stage 4
    #         {
    #             '$group': {
    #                 '_id':None,
    #                 'y_capacity':{'$sum':'$y_capacity'},
    #                 'j_capacity':{'$sum':'$j_capacity'}
    #             }
    #         },
    #
    #         # Stage 5
    #         {
    #             '$project': {
    #                 'y_capacity':'$y_capacity',
    #                 'j_capacity':'$j_capacity',
    #                 'capacity':{'$sum':['$y_capacity', '$j_capacity']}
    #             }
    #         },
    #
    #     ]
    #
    #     # Created with Studio 3T, the IDE for MongoDB - https:#studio3t.com/
    # ))
    host_od_capacity = list(db.JUP_DB_Host_OD_Capacity.aggregate(

        # Pipeline
        [
            # Stage 1
            {
                '$match': {
                    'od': origin + destination,
                    'dep_date': {'$gte': dep_date_start, '$lte': dep_date_end}
                }
            },

            # Stage 2
            {
                '$sort': {
                    'last_update_date': -1
                }
            },

            # Stage 3
            {
                '$group': {
                    '_id': {
                        'od': '$od',
                        'dep_date': '$dep_date'
                    },
                    'y_capacity': {'$first': '$y_cap'},
                    'j_capacity': {'$first': '$j_cap'}
                }
            },

            {
                '$project': {
                    'y_capacity': '$y_capacity',
                    'j_capacity': '$j_capacity',
                    'dep_date': "$_id.dep_date",
                    "_id": 0
                }
            }

        ]

        # Created with Studio 3T, the IDE for MongoDB - https:#studio3t.com/
    ))

    # print host_od_capacity
    comp_cap_df = pd.DataFrame(comp_capacity_d)
    host_cap_df = pd.DataFrame(host_od_capacity)
    host_cap_df['carrier'] = Host_Airline_Code
    capacity_dF = pd.DataFrame()
    if date_list:
        for date_ in date_list:
            if len(comp_cap_df) != 0:
                sm_1 = int(date_['start'][5:7])
                em_1 = int(date_['end'][5:7])

                tmp_df_comp = comp_cap_df[(comp_cap_df['month'] >= sm_1) & (
                    comp_cap_df['month'] <= em_1)]
                tmp_df_host = host_cap_df[(host_cap_df['dep_date'] >= date_['start']) &
                                          (host_cap_df['dep_date'] <= date_['end'])]
                if len(tmp_df_host) > 0:
                    if compartment == "Y":
                        tmp_df_host['capacity'] = tmp_df_host['y_capacity']
                    elif compartment == "J":
                        tmp_df_host['capacity'] = tmp_df_host['j_capacity']
                    else:
                        tmp_df_host['capacity'] = tmp_df_host['j_capacity'] + \
                            tmp_df_host['y_capacity']
                else:
                    tmp_df_host['capacity'] = np.nan

                tmp_df_comp = tmp_df_comp.groupby(
                    by='carrier', as_index=False)['capacity'].sum()
                tmp_df_host = tmp_df_host.groupby(
                    by='carrier', as_index=False)['capacity'].sum()

                tmp_df_comp = tmp_df_comp[['carrier', 'capacity']]
                tmp_df_host = tmp_df_host[['carrier', 'capacity']]
                tmp_df_combined = pd.concat([tmp_df_comp, tmp_df_host])
                tmp_df_combined['date_start'] = date_['start']
                tmp_df_combined['date_end'] = date_['end']
                capacity_dF = pd.concat([capacity_dF, tmp_df_combined])
            else:
                tmp_df_host = host_cap_df[(host_cap_df['dep_date'] >= date_['start']) &
                                          (host_cap_df['dep_date'] <= date_['end'])]
                if len(tmp_df_host) > 0:
                    if compartment == "Y":
                        tmp_df_host.loc[:,
                                        'capacity'] = tmp_df_host['y_capacity']
                    elif compartment == "J":
                        tmp_df_host.loc[:,
                                        'capacity'] = tmp_df_host['j_capacity']
                    else:
                        tmp_df_host.loc[:, 'capacity'] = tmp_df_host['j_capacity'] + \
                            tmp_df_host['y_capacity']
                else:
                    tmp_df_host.loc[:, 'capacity'] = np.nan
                # tmp_df_comp = tmp_df_comp.groupby(by='carrier', as_index=False)['capacity'].sum()
                tmp_df_host = tmp_df_host.groupby(
                    by='carrier', as_index=False)['capacity'].sum()

                # tmp_df_comp = tmp_df_comp[['carrier', 'capacity']]
                tmp_df_host = tmp_df_host[['carrier', 'capacity']]
                # tmp_df_combined = pd.concat([tmp_df_comp, tmp_df_host])
                tmp_df_host['date_start'] = date_['start']
                tmp_df_host['date_end'] = date_['end']
                capacity_dF = pd.concat([capacity_dF, tmp_df_host])
    else:
        if len(host_cap_df) > 0:
            if compartment == "Y":
                host_cap_df['capacity'] = host_cap_df['y_capacity']
            elif compartment == "J":
                host_cap_df['capacity'] = host_cap_df['j_capacity']
            else:
                host_cap_df['capacity'] = host_cap_df['j_capacity'] + \
                    host_cap_df['y_capacity']
        else:
            host_cap_df['capacity'] = np.nan
        if len(comp_cap_df) != 0:
            comp_cap_df = comp_cap_df.groupby(
                by='carrier', as_index=False)['capacity'].sum()
            comp_cap_df = comp_cap_df[['carrier', 'capacity']]
        host_cap_df = host_cap_df.groupby(
            by='carrier', as_index=False)['capacity'].sum()

        tmp_df_host = host_cap_df[['carrier', 'capacity']]
        tmp_df_combined = pd.concat([comp_cap_df, host_cap_df])
        tmp_df_combined['date_start'] = dep_date_start
        tmp_df_combined['date_end'] = dep_date_end
        capacity_dF = pd.concat([capacity_dF, tmp_df_combined])
        # if len(host_od_capacity) == 1:
        #     if compartment == 'Y':
        #         comp_capacity_d.append({'carrier':'FZ','capacity':host_od_capacity[0]['y_capacity']})
        #     elif compartment == 'J':
        #         comp_capacity_d.append({'carrier':'FZ','capacity':host_od_capacity[0]['j_capacity']})
        #     else:
        #         comp_capacity_d.append({'carrier':'FZ','capacity':host_od_capacity[0]['y_capacity']+host_od_capacity[0]['j_capacity']})
        # else:
        #     comp_capacity_d.append({'carrier':'FZ','capacity':np.nan})
        # if len(comp_capacity_d) > 0:
        #     capacity_dF = pd.DataFrame(comp_capacity_d)
    # print capacity_dF
    # print time.time() - st
    st = time.time()
    return capacity_dF


@measure(JUPITER_LOGGER)
def get_ratings_dict(
        pos,
        origin,
        destination,
        compartment,
        db,
        dep_date_start,
        dep_date_end):
    st = time.time()
    ratings_d = list(db.JUP_DB_Competitor_Ratings.aggregate(

        # Pipeline
        [
            # Stage 1
            {
                '$match': {
                    'origin': origin,
                    'destination': destination,
                    'compartment': compartment
                }
            },

            # Stage 2
            {
                '$sort': {
                    'last_update_date': -1
                }
            },

            # Stage 3
            {
                '$group': {
                    '_id': {
                        'origin': '$origin',
                        'destination': '$destination',
                        'compartment': '$compartment'
                    },
                    'ratings': {'$first': '$ratings'},
                    'product_ratings': {'$first': '$Product Rating_ratings'},
                    'fare_ratings': {'$first': '$Fares Rating_ratings'},
                    'distributor_ratings': {'$first': '$Distributors Rating_ratings'},
                    'market_ratings': {'$first': '$Market Rating_ratings'},
                    'capacity_ratings': {'$first': '$Capacity/Schedule_ratings'}
                }
            },

        ]

        # Created with Studio 3T, the IDE for MongoDB - https:#studio3t.com/
    ))
    ratings_data = dict()
    print len(ratings_d)
    if len(ratings_d) == 1:
        # print ratings_d[0]
        ratings_data['ratings_dict'] = ratings_d[0]['ratings']
        ratings_data['market_ratings_dict'] = ratings_d[0]['market_ratings']
        ratings_data['capacity_ratings_dict'] = ratings_d[0]['capacity_ratings']
        ratings_data['fare_ratings_dict'] = ratings_d[0]['fare_ratings']
        ratings_data['distributor_ratings_dict'] = ratings_d[0]['distributor_ratings']
        ratings_data['product_ratings_dict'] = ratings_d[0]['product_ratings']
    else:
        ratings_data = dict(
            ratings_dict=dict(),
            market_ratings_dict=dict(),
            capacity_ratings_dict=dict(),
            fare_ratings_dict=dict(),
            distributor_ratings_dict=dict(),
            product_ratings_dict=dict()
        )

    # print json.dumps(ratings_dict,indent=1)
    # print time.time() - st
    st = time.time()
    ratings_data = pd.DataFrame(ratings_data)
    return ratings_data


@measure(JUPITER_LOGGER)
def get_most_avail_dict(
        pos,
        origin,
        destination,
        compartment,
        dep_date_start,
        dep_date_end,
        db,
        oneway_return=2,
        date_list=None):
    st = time.time()
    print "starting"
    mapping_crsr = db.JUP_DB_City_Airport_Mapping.find(
        {"Airport_Code": {"$in": [origin, destination]}})
    origin_temp = origin
    destination_temp = destination
    for i in mapping_crsr:
        if i['Airport_Code'] == origin:
            origin_temp = i['City_Code']
        else:
            destination_temp = i['City_Code']
    if pos != origin_temp:
        most_avail_fare_dF = pd.DataFrame(
            columns=[
                'carrier',
                'most_avail_fare_base_ow',
                'most_avail_fare_total_ow',
                'most_avail_fare_tax_ow',
                'most_avail_fare_count_ow',
                'most_avail_fare_base_rt',
                'most_avail_fare_total_rt',
                'most_avail_fare_tax_rt',
                'most_avail_fare_count_rt',
                'currency',
                'fare_basis_ow',
                'fare_basis_rt',
                'observation_date_ow',
                'observation_date_rt'
            ]
        )
        most_avail_fare_dF.set_value(0, 'currency', "NA")
        most_avail_fare_dF.fillna("NA", inplace=True)
    else:
        # print "in else"
        currency_data = get_currency_data(db=db)
        try:
            currency = currency_data[pos]['web']
        except KeyError:
            currency = 'NA'
        # temp_col_name = gen_collection_name()
        most_avail_fare_dF = pd.DataFrame(
            columns=[
                'carrier',
                'most_avail_fare_base_ow',
                'most_avail_fare_total_ow',
                'most_avail_fare_tax_ow',
                'most_avail_fare_count_ow',
                'most_avail_fare_base_rt',
                'most_avail_fare_total_rt',
                'most_avail_fare_tax_rt',
                'most_avail_fare_count_rt',
                'currency',
                'fare_basis_ow',
                'fare_basis_rt',
                'observation_date_ow',
                'observation_date_rt'
            ]
        )
        query = list()
        query.append({'od': {"$in": [origin_temp + destination_temp]}})
        query.append({'compartment': compartment})
        query.append({'price_inc': {'$ne': 0}})
        # query.append({'currency': currency})

        if dep_date_start and dep_date_end:
            query.append({'outbound_departure_date': {
                                 '$gte': dep_date_start, '$lte': dep_date_end}})
        else:
            query.append(
                {'outbound_departure_date': {'$gte': SYSTEM_DATE}})

        print "querying..."
        # print query
        crsr = db.JUP_DB_Infare.aggregate(
            # Pipeline
            [
                # Stage 1
                {
                    '$match': {"$and": query}
                },{
                  "$group":{
                      "_id":{
                      "od": "$od",
                      "observation_date": "$observation_date",
                      "observation_time": "$observation_time",
                      "is_one_way": "$is_one_way",
                      "carrier": "$carrier",
                      "outbound_flight_no": "$outbound_flight_no",
                      "outbound_departure_date": "$outbound_departure_date",
                      "outbound_departure_time": "$outbound_departure_time",
                      "price_exc": "$price_exc",
                      "price_inc": "$price_inc",
                      "tax": "$tax",
                      "currency": "$currency",
                      "compartment": "$compartment",
                      "fb": "$outbound_fare_basis"
                      }
                  }
              },
                {
                    '$group':
                        {
                            '_id': {"is_one_way": "$_id.is_one_way",
                                    "outbound_departure_date": "$_id.outbound_departure_date",
                                    "outbound_flight_no": "$_id.outbound_flight_no",
                                    "compartment": "$_id.compartment",
                                    "carrier": "$_id.carrier"},
                            "max_value": {"$max": "$_id.observation_date"},
                            "docs": {
                                "$push": {
                                    "od": "$_id.od",
                                    "observation_date": "$_id.observation_date",
                                    "is_one_way": "$_id.is_one_way",
                                    "carrier": "$_id.carrier",
                                    "outbound_flight_no": "$_id.outbound_flight_no",
                                    "outbound_departure_date": "$_id.outbound_departure_date",
                                    "price_exc": "$_id.price_exc",
                                    "price_inc": "$_id.price_inc",
                                    "tax": "$_id.tax",
                                    "currency": "$_id.currency",
                                    "compartment": "$_id.compartment",
                                    "fb": "$_id.fb"
                                }
                            }

                        }
                },
                # Stage 2
                {
                    "$unwind": {
                        "path": "$docs"
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "od": "$docs.od",
                        "observation_date": "$docs.observation_date",
                        "is_one_way": "$docs.is_one_way",
                        "carrier": "$docs.carrier",
                        "outbound_flight_no": "$docs.outbound_flight_no",
                        "dep_date": "$docs.outbound_departure_date",
                        "price_exc": "$docs.price_exc",
                        "price_inc": "$docs.price_inc",
                        "tax": "$docs.tax",
                        "fb": "$docs.fb",
                        "currency": "$docs.currency",
                        "compartment": "$docs.compartment",
                        "cmp_value": {
                            "$cmp": ["$max_value", "$docs.observation_date"]
                        }
                    }
                }, {
                    "$match": {
                        "cmp_value": 0
                    }
                },
                {
                    "$sort": {
                        "observation_time": -1
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "is_one_way": "$is_one_way",
                            "carrier": "$carrier",
                            "outbound_flight_no": "$outbound_flight_no",
                            "od": "$od",
                            "compartment": "$compartment",
                            "dep_date": "$dep_date"
                        },
                        "doc": {
                            "$first": "$$ROOT"
                        }
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "carrier": "$doc.carrier",
                        "od": "$doc.od",
                        "is_one_way": "$doc.is_one_way",
                        "observation_date": "$doc.observation_date",
                        "outbound_flight_no": "$doc.outbound_flight_no",
                        "dep_date": "$doc.dep_date",
                        "fb": "$doc.fb",
                        "tax": "$doc.tax",
                        "price_exc":"$doc.price_exc",
                        "currency": "$doc.currency",
                        "price_inc": "$doc.price_inc",
                        "compartment": "$doc.compartment"
                    }
                }
            ],
            allowDiskUse=True
            # Created with Studio 3T, the IDE for MongoDB -
            # https:#studio3t.com/
        )
        print "building df"
        df_1 = pd.DataFrame(list(crsr))
        #	df_1['price_inc'] = df_1['price_exc'] + df_1['tax']
        print "built"
        exchange_rate = {}
        currency_crsr = list(
            db.JUP_DB_Exchange_Rate.find({}))
        # print "Currency_crsr = ", currency_crsr
        for curr in currency_crsr:
            exchange_rate[curr['code']] = curr['Reference_Rate']
        # df_1['frequency'] = 1
        # print df_1
        most_avail_fare_dF = pd.DataFrame()
        # print "len", len(df_1)
        if len(df_1) > 0:
            df_1['price_inc'] = df_1.apply(lambda row: (
                row['price_inc'] / exchange_rate[currency]) * exchange_rate[row['currency']], axis=1)
            df_1['price_exc'] = df_1.apply(lambda row: (
                row['price_exc'] / exchange_rate[currency]) * exchange_rate[row['currency']], axis=1)
            df_1['tax'] = df_1.apply(lambda row: (
                row['tax'] / exchange_rate[currency]) * exchange_rate[row['currency']], axis=1)

            comps = df_1['carrier'].unique()
            if date_list:
                for date_ in date_list:
                    for comp in comps:
                        tmp_df = df_1[(df_1['dep_date'] >= date_['start']) & (
                            df_1['dep_date'] <= date_['end']) & (df_1['carrier'] == comp)]
                        tmp_df = tmp_df.groupby(
                            by=[
                                'carrier',
                                'price_inc',
                                'is_one_way',
                                'tax',
                                'price_exc',
                                'dep_date',
                                'fb',
                                'observation_date'])['outbound_flight_no'].nunique()
                        tmp_df = tmp_df.reset_index()
                        tmp_df = tmp_df.groupby(
                            by=[
                                'carrier',
                                'price_exc',
                                'tax',
                                'is_one_way',
                                'price_inc',
                                'fb',
                                'observation_date'],
                            as_index=False)['outbound_flight_no'].sum()
                        tmp_df = tmp_df.rename(
                            columns={
                                "outbound_flight_no": "most_available_fare_count"})
                        max_ow = tmp_df[(tmp_df['is_one_way'] == 1)
                                        ]['most_available_fare_count'].max()
                        max_rt = tmp_df[(tmp_df['is_one_way'] == 0)
                                        ]['most_available_fare_count'].max()
                        tmp_df['date_start'] = date_['start']
                        tmp_df['date_end'] = date_['end']
                        ow = tmp_df[tmp_df['is_one_way'] == 1]
                        ow['most_avail_fare_freq_ow'] = ow['most_available_fare_count'] / \
                            ow['most_available_fare_count'].sum()
                        rt = tmp_df[tmp_df['is_one_way'] == 0]
                        rt['most_avail_fare_freq_rt'] = rt['most_available_fare_count'] / \
                            rt['most_available_fare_count'].sum()
                        ow = ow.rename(
                            columns={
                                "price_inc": "most_avail_fare_total_ow",
                                "price_exc": "most_avail_fare_base_ow",
                                "tax": "most_avail_fare_tax_ow",
                                "most_available_fare_count": "most_avail_fare_count_ow",
                                "fb": "fare_basis_ow",
                                "observation_date": "observation_date_ow"})
                        rt = rt.rename(
                            columns={
                                "price_inc": "most_avail_fare_total_rt",
                                "price_exc": "most_avail_fare_base_rt",
                                "tax": "most_avail_fare_tax_rt",
                                "most_available_fare_count": "most_avail_fare_count_rt",
                                "fb": "fare_basis_rt",
                                "observation_date": "observation_date_rt"})

                        try:
                            most_avail_dict_ow = ow[(ow['most_avail_fare_count_ow'] == max_ow)].to_dict(
                                "records")[0]
                        except BaseException:
                            most_avail_dict_ow = {
                                "carrier": comp,
                                "most_avail_fare_base_ow": "NA",
                                "most_avail_fare_total_ow": "NA",
                                "most_avail_fare_count_ow": "NA",
                                "most_avail_fare_tax_ow": "NA",
                                "most_avail_fare_freq_ow": "NA",
                                "fare_basis_ow": "NA",
                                "observation_date_ow": "NA"}
                        try:
                            most_avail_dict_rt = rt[(rt['most_avail_fare_count_rt'] == max_rt)].to_dict(
                                "records")[0]
                        except BaseException:
                            most_avail_dict_rt = {
                                "carrier": comp,
                                "most_avail_fare_total_rt": "NA",
                                "most_avail_fare_base_rt": "NA",
                                "most_avail_fare_tax_rt": "NA",
                                "most_avail_fare_count_rt": "NA",
                                "most_avail_fare_freq_rt": "NA",
                                "fare_basis_rt": "NA",
                                "observation_date_rt": "NA"}
                        #			print most_avail_dict_ow
                        #			print most_avail_dict_rt
                        ### combining the ow and rt columns in one dict########
                        for k, v in most_avail_dict_rt.items():
                            if k not in most_avail_dict_ow.keys():
                                most_avail_dict_ow[k] = v
                        #			print most_avail_dict_ow
                        #			print most_avail_dict_rt
                        most_avail_fare_dF = pd.concat(
                            [most_avail_fare_dF, pd.DataFrame([most_avail_dict_ow])])
                # print most_avail_fare_dF.head()
                # most_avail_fare_dF['most_avail_fare_total_ow'] = most_avail_fare_dF.apply(lambda row: (row['most_avail_fare_total_ow']/exchange_rate[currency])*exchange_rate[row['currency']], axis=1)
                # most_avail_fare_dF['most_avail_fare_tax_ow'] = most_avail_fare_dF.apply(lambda row: (row['most_avail_fare_tax_ow']/exchange_rate[currency])*exchange_rate[row['currency']], axis=1)
                # most_avail_fare_dF['price_exc'] = most_avail_fare_dF.apply(lambda row: (row['price_exc']/exchange_rate[currency])*exchange_rate[row['currency']], axis=1)
                most_avail_fare_dF['currency'] = currency

            else:
                for comp in comps:
                    tmp_df = df_1[df_1['carrier'] == comp]
                    tmp_df = tmp_df.groupby(
                        by=[
                            'carrier',
                            'price_inc',
                            'is_one_way',
                            'tax',
                            'price_exc',
                            'dep_date',
                            'fb',
                            'observation_date'])['outbound_flight_no'].nunique()
                    tmp_df = tmp_df.reset_index()
                    tmp_df = tmp_df.groupby(by=['carrier',
                                                'price_exc',
                                                'price_inc',
                                                'tax',
                                                'is_one_way',
                                                'fb',
                                                'observation_date'],
                                            as_index=False)['outbound_flight_no'].sum()
                    tmp_df = tmp_df.rename(
                        columns={
                            "outbound_flight_no": "most_available_fare_count"})
                    max_ow = tmp_df[(tmp_df['is_one_way'] == 1)
                                    ]['most_available_fare_count'].max()
                    max_rt = tmp_df[(tmp_df['is_one_way'] == 0)
                                    ]['most_available_fare_count'].max()
                    tmp_df['date_start'] = dep_date_start
                    tmp_df['date_end'] = dep_date_end
                    ow = tmp_df[tmp_df['is_one_way'] == 1]
                    ow['most_avail_fare_freq_ow'] = ow['most_available_fare_count'] / \
                        ow['most_available_fare_count'].sum()
                    rt = tmp_df[tmp_df['is_one_way'] == 0]
                    rt['most_avail_fare_freq_rt'] = rt['most_available_fare_count'] / \
                        rt['most_available_fare_count'].sum()
                    ow = ow.rename(
                        columns={
                            "price_inc": "most_avail_fare_total_ow",
                            "price_exc": "most_avail_fare_base_ow",
                            "tax": "most_avail_fare_tax_ow",
                            "most_available_fare_count": "most_avail_fare_count_ow",
                            "fb": "fare_basis_ow",
                            "observation_date": "observation_date_ow"})
                    rt = rt.rename(
                        columns={
                            "price_inc": "most_avail_fare_total_rt",
                            "price_exc": "most_avail_fare_base_rt",
                            "tax": "most_avail_fare_tax_rt",
                            "most_available_fare_count": "most_avail_fare_count_rt",
                            "fb": "fare_basis_rt",
                            "observation_date": "observation_date_rt"})
                    most_avail_dict_ow = ow.loc[(
                        ow['most_avail_fare_count_ow'] == max_ow)].to_dict("records")[0]
                    most_avail_dict_rt = rt.loc[(
                        rt['most_avail_fare_count_rt'] == max_rt)].to_dict("records")[0]
                    most_avail_dict = most_avail_dict_ow.update(
                        most_avail_dict_rt)
                    most_avail_fare_dF = pd.concat(
                        [most_avail_fare_dF, pd.DataFrame(list(most_avail_dict))])
                    # most_avail_fare_dF['price_inc'] = most_avail_fare_dF.apply(
                    #     lambda row: (row['price_inc'] / exchange_rate[currency]) * exchange_rate[row['currency']],
                    #     axis=1)
                    # most_avail_fare_dF['tax'] = most_avail_fare_dF.apply(
                    #     lambda row: (row['tax'] / exchange_rate[currency]) * exchange_rate[row['currency']], axis=1)
                    # most_avail_fare_dF['price_exc'] = most_avail_fare_dF.apply(
                    #     lambda row: (row['price_exc'] / exchange_rate[currency]) * exchange_rate[row['currency']],
                    #     axis=1)
                    most_avail_fare_dF['currency'] = currency

        else:
            most_avail_fare_dF = pd.DataFrame(
                columns=[
                    'carrier',
                    'most_avail_fare_base_ow',
                    'most_avail_fare_total_ow',
                    'most_avail_fare_tax_ow',
                    'most_avail_fare_count_ow',
                    'most_avail_fare_base_rt',
                    'most_avail_fare_total_rt',
                    'most_avail_fare_tax_rt',
                    'most_avail_fare_count_rt',
                    'currency',
                    'fare_basis_ow',
                    'fare_basis_rt',
                    "observation_date_ow",
                    "observation_date_rt"
                ]
            )
            most_avail_fare_dF.set_value(0, 'currency', currency)
            most_avail_fare_dF.fillna("NA", inplace=True)
        # print most_avail_fare_dF
        # print time.time() - st
        # print "currency: ", currency

    # print most_avail_fare_dF
    st = time.time()
    return most_avail_fare_dF


@measure(JUPITER_LOGGER)
def generate_response_python(
        pos,
        origin,
        destination,
        compartment,
        dep_date_start,
        dep_date_end,
        dF,
        comp_list,
        date_list=None):
    """
    """
    complete_response = []
    dF.replace("NA", np.nan, inplace=True)
    count = 0
    for date_ in date_list:
        print date_
        mrkt_data = dict(
            host=dict(),
            comp=list()
        )
        host = dF[(dF['carrier'] == Host_Airline_Code) & (dF['date_start'] ==
                                             date_['start']) & (dF['date_end'] == date_['end'])]
        host.reset_index(inplace=True)
        if len(host) == 1:
            mrkt_data['host']['market_share'] = host['market_share'].iloc[0]
            mrkt_data['host']['market_share_vlyr'] = host['market_share_vlyr'].iloc[0]
            # if host['pax'].iloc[0] != 'NA':
            try:
                mrkt_data['host']['pax'] = int(host['pax'].iloc[0])
            except ValueError:
                mrkt_data['host']['pax'] = host['pax'].iloc[0]
            # else:
            #     mrkt_data['host']['pax'] = host['pax'].iloc[0]
            mrkt_data['host']['pax_vlyr'] = host['pax_vlyr'].iloc[0]

            mrkt_data['host']['fms'] = host['fms'].iloc[0]
            mrkt_data['host']['market_share_vtgt'] = host['market_share_vtgt'].iloc[0]
            mrkt_data['host']['lowest_filed_fare'] = dict(
                total_fare=host['lowest_fare_total'].iloc[0],
                base_fare=host['lowest_fare_base'].iloc[0],
                fare_basis=host['lowest_fare_fb'].iloc[0],
                tax=host['lowest_fare_tax'].iloc[0],
                yq=host['lowest_fare_YQ'].iloc[0],
                yr=host['lowest_fare_YR'].iloc[0],
                surcharge=host['lowest_fare_surcharge'].iloc[0],
                currency=host['currency'].iloc[0]
            )

            # if host['lowest_fare_tax'].iloc[0] != 'NA':
            mrkt_data['host']['lowest_filed_fare']['tax'] = float(
                host['lowest_fare_tax'].iloc[0])

            # if host['lowest_fare_YQ'].iloc[0] != 'NA':
            mrkt_data['host']['lowest_filed_fare']['yq'] = float(
                host['lowest_fare_YQ'].iloc[0])

            # if host['lowest_fare_YR'].iloc[0] != 'NA':
            mrkt_data['host']['lowest_filed_fare']['yr'] = float(
                host['lowest_fare_YR'].iloc[0])

            # if host['lowest_fare_surcharge'].iloc[0] != 'NA':
            mrkt_data['host']['lowest_filed_fare']['surcharge'] = float(
                host['lowest_fare_surcharge'].iloc[0])

            mrkt_data['host']['rating'] = host['rating'].iloc[0]
            mrkt_data['host']['market_rating'] = host['market_rating'].iloc[0]
            mrkt_data['host']['fare_rating'] = host['fare_rating'].iloc[0]
            mrkt_data['host']['capacity_rating'] = host['capacity_rating'].iloc[0]
            mrkt_data['host']['product_rating'] = host['product_rating'].iloc[0]
            mrkt_data['host']['distributor_rating'] = host['distributor_rating'].iloc[0]
            mrkt_data['host']['most_available_fare'] = dict(
                base_fare=host['most_avail_fare_base'].iloc[0],
                frequency=host['most_avail_fare_frequency'].iloc[0],
                tax=host['most_avail_fare_tax'].iloc[0],
                total_fare=host['most_avail_fare_total'].iloc[0],
                total_count=host['most_avail_fare_count'].iloc[0],
                currency=host['currency'].iloc[0]
            )

            # if host['most_avail_fare_tax'].iloc[0] != 'NA':
            mrkt_data['host']['most_available_fare']['tax'] = float(
                host['most_avail_fare_tax'].iloc[0])

            # if host['most_avail_fare_frequency'].iloc[0] != 'NA':
            try:
                mrkt_data['host']['most_available_fare']['frequency'] = int(
                    host['most_avail_fare_frequency'].iloc[0])
            # else:
            except ValueError:
                mrkt_data['host']['most_available_fare']['frequency'] = host['most_avail_fare_frequency'].iloc[0]

            # if host['most_avail_fare_count'].iloc[0] != 'NA':
            try:
                mrkt_data['host']['most_available_fare']['total_count'] = int(
                    host['most_avail_fare_count'].iloc[0])
            # else:
            except ValueError:
                mrkt_data['host']['most_available_fare']['total_count'] = host['most_avail_fare_count'].iloc[0]

            mrkt_data['host']['price_movement_filed'] = dict(
                lowest_fare=dict(
                    surcharge=host['lowest_fare_surcharge'].iloc[0],
                    total_fare=host['lowest_fare_total'].iloc[0],
                    base_fare=host['lowest_fare_base'].iloc[0],
                    yq=host['lowest_fare_YQ'].iloc[0],
                    fare_basis=host['lowest_fare_fb'].iloc[0],
                    yr=host['lowest_fare_YR'].iloc[0],
                    tax=host['lowest_fare_tax'].iloc[0],
                ),
                highest_fare=dict(
                    surcharge=host['highest_fare_surcharge'].iloc[0],
                    total_fare=host['highest_fare_total'].iloc[0],
                    base_fare=host['highest_fare_base'].iloc[0],
                    yq=host['highest_fare_YQ'].iloc[0],
                    fare_basis=host['highest_fare_fb'].iloc[0],
                    yr=host['highest_fare_YR'].iloc[0],
                    tax=host['highest_fare_tax'].iloc[0],
                ),
            )
            # if host['lowest_fare_tax'].iloc[0] != 'NA':
            mrkt_data['host']['price_movement_filed']['lowest_fare']['tax'] = float(
                host['lowest_fare_tax'].iloc[0])

            # if host['lowest_fare_YQ'].iloc[0] != 'NA':
            mrkt_data['host']['price_movement_filed']['lowest_fare']['yq'] = float(
                host['lowest_fare_YQ'].iloc[0])

            # if host['lowest_fare_YR'].iloc[0] != 'NA':
            mrkt_data['host']['price_movement_filed']['lowest_fare']['yr'] = float(
                host['lowest_fare_YR'].iloc[0])

            # if host['lowest_fare_surcharge'].iloc[0] != 'NA':
            mrkt_data['host']['price_movement_filed']['lowest_fare']['surcharge'] = float(
                host['lowest_fare_surcharge'].iloc[0])

            # if host['highest_fare_tax'].iloc[0] != 'NA':
            mrkt_data['host']['price_movement_filed']['highest_fare']['tax'] = float(
                host['highest_fare_tax'].iloc[0])

            # if host['highest_fare_YQ'].iloc[0] != 'NA':
            mrkt_data['host']['price_movement_filed']['highest_fare']['yq'] = float(
                host['highest_fare_YQ'].iloc[0])

            # if host['highest_fare_YR'].iloc[0] != 'NA':
            mrkt_data['host']['price_movement_filed']['highest_fare']['yr'] = float(
                host['highest_fare_YR'].iloc[0])

            # if host['highest_fare_surcharge'].iloc[0] != 'NA':
            mrkt_data['host']['price_movement_filed']['highest_fare']['surcharge'] = float(
                host['highest_fare_surcharge'].iloc[0])

        else:
            mrkt_data['host']['market_share'] = "NA"
            mrkt_data['host']['market_share_vlyr'] = "NA"

            mrkt_data['host']['bookings'] = "NA"
            mrkt_data['host']['bookings_vlyr'] = "NA"

            mrkt_data['host']['fms'] = "NA"
            mrkt_data['host']['market_share_vtgt'] = "NA"
            mrkt_data['host']['lowest_filed_fare'] = dict(
                total_fare="NA",
                base_fare="NA",
                tax="NA",
                fare_basis='NA',
                yq="NA",
                yr="NA",
                surcharge="NA",
                currency="NA"
            )

            mrkt_data['host']['rating'] = "NA"
            mrkt_data['host']['market_rating'] = "NA"
            mrkt_data['host']['fare_rating'] = "NA"
            mrkt_data['host']['capacity_rating'] = "NA"
            mrkt_data['host']['product_rating'] = "NA"
            mrkt_data['host']['distributor_rating'] = "NA"

            mrkt_data['host']['most_available_fare'] = dict(
                base_fare="NA",
                frequency="NA",
                tax="NA",
                total_fare="NA",
                total_count="NA",
                currency="NA"
            )
            mrkt_data['host']['price_movement_filed'] = dict(
                lowest_fare=dict(
                    surcharge="NA",
                    total_fare="NA",
                    base_fare="NA",
                    yq="NA",
                    fare_basis="NA",
                    yr="NA",
                    tax="NA",
                ),
                highest_fare=dict(
                    surcharge="NA",
                    total_fare="NA",
                    base_fare="NA",
                    yq="NA",
                    fare_basis="NA",
                    yr="NA",
                    tax="NA",
                )
            )
        print "df in mrkt_workflow_opt --------->"
        print dF.head()
        temp_comp_list = comp_list[count][0]
        try:
            temp_comp_list.remove(Host_Airline_Code)
        except Exception:
            pass
        for competitor in temp_comp_list:
            comp_dF = dF[(dF['carrier'] == competitor) & (
                dF['date_start'] == date_['start']) & (dF['date_end'] == date_['end'])]
            print "DF for competitor: ", competitor
            print comp_dF.head()
            comp_data = dict()
            comp_data['airline'] = competitor
            if len(comp_dF) == 1:
                comp_data['market_share'] = comp_dF['market_share'].iloc[0]
                comp_data['market_share_vlyr'] = comp_dF['market_share_vlyr'].iloc[0]
                # if comp_dF['pax'].iloc[0] != 'NA':
                try:
                    comp_data['pax'] = int(comp_dF['pax'].iloc[0])
                # else:
                except ValueError:
                    comp_data['pax'] = comp_dF['pax'].iloc[0]
                comp_data['pax_vlyr'] = comp_dF['pax_vlyr'].iloc[0]

                comp_data['fms'] = comp_dF['fms'].iloc[0]
                comp_data['market_share_vtgt'] = comp_dF['market_share_vtgt'].iloc[0]
                comp_data['lowest_filed_fare'] = dict(
                    total_fare=comp_dF['lowest_fare_total'].iloc[0],
                    base_fare=comp_dF['lowest_fare_base'].iloc[0],
                    fare_basis=comp_dF['lowest_fare_fb'].iloc[0],
                    tax=comp_dF['lowest_fare_tax'].iloc[0],
                    yq=comp_dF['lowest_fare_YQ'].iloc[0],
                    yr=comp_dF['lowest_fare_YR'].iloc[0],
                    surcharge=comp_dF['lowest_fare_surcharge'].iloc[0],
                    currency=comp_dF['currency'].iloc[0]
                )

                # if comp_dF['lowest_fare_tax'].iloc[0] != 'NA':
                comp_data['lowest_filed_fare']['tax'] = float(
                    comp_dF['lowest_fare_tax'].iloc[0])

                # if comp_dF['lowest_fare_YQ'].iloc[0] != 'NA':
                comp_data['lowest_filed_fare']['yq'] = float(
                    comp_dF['lowest_fare_YQ'].iloc[0])

                # if comp_dF['lowest_fare_YR'].iloc[0] != 'NA':
                comp_data['lowest_filed_fare']['yr'] = float(
                    comp_dF['lowest_fare_YR'].iloc[0])

                # if comp_dF['lowest_fare_surcharge'].iloc[0] != 'NA':
                comp_data['lowest_filed_fare']['surcharge'] = float(
                    comp_dF['lowest_fare_surcharge'].iloc[0])

                comp_data['rating'] = comp_dF['rating'].iloc[0]
                comp_data['market_rating'] = comp_dF['market_rating'].iloc[0]
                comp_data['fare_rating'] = comp_dF['fare_rating'].iloc[0]
                comp_data['capacity_rating'] = comp_dF['capacity_rating'].iloc[0]
                comp_data['product_rating'] = comp_dF['product_rating'].iloc[0]
                comp_data['distributor_rating'] = comp_dF['distributor_rating'].iloc[0]

                comp_data['most_available_fare'] = dict(
                    base_fare=comp_dF['most_avail_fare_base'].iloc[0],
                    frequency=comp_dF['most_avail_fare_frequency'].iloc[0],
                    tax=comp_dF['most_avail_fare_tax'].iloc[0],
                    total_fare=comp_dF['most_avail_fare_total'].iloc[0],
                    total_count=comp_dF['most_avail_fare_count'].iloc[0],
                    currency=comp_dF['currency'].iloc[0]
                )
                # if comp_dF['most_avail_fare_tax'].iloc[0] != 'NA':
                comp_data['most_available_fare']['tax'] = float(
                    comp_dF['most_avail_fare_tax'].iloc[0])

                # if comp_dF['most_avail_fare_frequency'].iloc[0] != 'NA':
                try:
                    comp_data['most_available_fare']['frequency'] = int(
                        comp_dF['most_avail_fare_frequency'].iloc[0])
                # else:
                except ValueError:
                    comp_data['most_available_fare']['frequency'] = comp_dF['most_avail_fare_frequency'].iloc[0]

                # if comp_dF['most_avail_fare_count'].iloc[0] != 'NA':
                try:
                    comp_data['most_available_fare']['total_count'] = int(
                        comp_dF['most_avail_fare_count'].iloc[0])
                # else:
                except ValueError:
                    comp_data['most_available_fare']['total_count'] = comp_dF['most_avail_fare_count'].iloc[0]

                comp_data['price_movement_filed'] = dict(
                    lowest_fare=dict(
                        surcharge=comp_dF['lowest_fare_surcharge'].iloc[0],
                        total_fare=comp_dF['lowest_fare_total'].iloc[0],
                        base_fare=comp_dF['lowest_fare_base'].iloc[0],
                        yq=comp_dF['lowest_fare_YQ'].iloc[0],
                        fare_basis=comp_dF['lowest_fare_fb'].iloc[0],
                        yr=comp_dF['lowest_fare_YR'].iloc[0],
                        tax=comp_dF['lowest_fare_tax'].iloc[0]
                    ),
                    highest_fare=dict(
                        surcharge=comp_dF['highest_fare_surcharge'].iloc[0],
                        total_fare=comp_dF['highest_fare_total'].iloc[0],
                        base_fare=comp_dF['highest_fare_base'].iloc[0],
                        yq=comp_dF['highest_fare_YQ'].iloc[0],
                        fare_basis=comp_dF['highest_fare_fb'].iloc[0],
                        yr=comp_dF['highest_fare_YR'].iloc[0],
                        tax=comp_dF['highest_fare_tax'].iloc[0]
                    )
                )
                # if comp_dF['lowest_fare_tax'].iloc[0] != 'NA':
                comp_data['price_movement_filed']['lowest_fare']['tax'] = float(
                    comp_dF['lowest_fare_tax'].iloc[0])

                # if comp_dF['lowest_fare_YQ'].iloc[0] != 'NA':
                comp_data['price_movement_filed']['lowest_fare']['yq'] = float(
                    comp_dF['lowest_fare_YQ'].iloc[0])

                # if comp_dF['lowest_fare_YR'].iloc[0] != 'NA':
                comp_data['price_movement_filed']['lowest_fare']['yr'] = float(
                    comp_dF['lowest_fare_YR'].iloc[0])

                # if comp_dF['lowest_fare_surcharge'].iloc[0] != 'NA':
                comp_data['price_movement_filed']['lowest_fare']['surcharge'] = float(
                    comp_dF['lowest_fare_surcharge'].iloc[0])

                # if comp_dF['highest_fare_tax'].iloc[0] != 'NA':
                comp_data['price_movement_filed']['highest_fare']['tax'] = float(
                    comp_dF['highest_fare_tax'].iloc[0])

                # if comp_dF['highest_fare_YQ'].iloc[0] != 'NA':
                comp_data['price_movement_filed']['highest_fare']['yq'] = float(
                    comp_dF['highest_fare_YQ'].iloc[0])

                # if comp_dF['highest_fare_YR'].iloc[0] != 'NA':
                comp_data['price_movement_filed']['highest_fare']['yr'] = float(
                    comp_dF['highest_fare_YR'].iloc[0])

                # if comp_dF['highest_fare_surcharge'].iloc[0] != 'NA':
                comp_data['price_movement_filed']['highest_fare']['surcharge'] = float(
                    comp_dF['highest_fare_surcharge'].iloc[0])

            else:
                comp_data = dict()
                comp_data['airline'] = competitor
                comp_data['market_share'] = "NA"
                comp_data['market_share_vlyr'] = "NA"

                comp_data['pax'] = "NA"
                comp_data['pax_vlyr'] = "NA"

                comp_data['fms'] = "NA"
                comp_data['market_share_vtgt'] = "NA"
                comp_data['lowest_filed_fare'] = dict(
                    total_fare="NA",
                    base_fare="NA",
                    fare_basis='NA',
                    tax="NA",
                    yq="NA",
                    yr="NA",
                    surcharge="NA",
                    currency="NA"
                )

                comp_data['rating'] = "NA"
                comp_data['market_rating'] = "NA"
                comp_data['fare_rating'] = "NA"
                comp_data['capacity_rating'] = "NA"
                comp_data['product_rating'] = "NA"
                comp_data['distributor_rating'] = "NA"

                comp_data['most_available_fare'] = dict(
                    base_fare="NA",
                    frequency="NA",
                    tax="NA",
                    total_fare="NA",
                    total_count="NA",
                    currency="NA"
                )
                comp_data['price_movement_filed'] = dict(
                    lowest_fare=dict(
                        surcharges="NA",
                        total_fare="NA",
                        base_fare="NA",
                        yq="NA",
                        fare_basis="NA",
                        yr="NA",
                        tax="NA",
                    ),
                    highest_fare=dict(
                        surcharges="NA",
                        total_fare="NA",
                        base_fare="NA",
                        yq="NA",
                        fare_basis="NA",
                        yr="NA",
                        tax="NA",
                    )
                )

            mrkt_data['comp'].append(comp_data)
        count += 1
        complete_response.append(mrkt_data)
    # print json.dumps(mrkt_data, indent=1)
    return complete_response


@measure(JUPITER_LOGGER)
def generate_response_java(
        pos,
        origin,
        destination,
        compartment,
        dep_date_start,
        dep_date_end,
        dF,
        comp_list):
    """
    """
    response = dict(
        pos=pos,
        origin=origin,
        destination=destination,
        compartment=compartment
    )
    """
    mrkt_data['host']['market_share'] =
        mrkt_data['host']['market_share_vlyr'] =

        mrkt_data['host']['bookings'] =
        mrkt_data['host']['bookings_vlyr'] =

        mrkt_data['host']['fms'] =
        mrkt_data['host']['market_share_vtgt'] =

        mrkt_data['host']['rating'] =
    """
    host = dF[dF['carrier'] == Host_Airline_Code]
    # print host.columns
    if len(host) == 1:
        response.update(
            {
                "host_web_frequently_available_total_fare": host['most_avail_fare_total'].iloc[0],
                "host_web_frequently_available_base_fare": host['most_avail_fare_base'].iloc[0],
                "host_web_frequently_available_fare_frequency": host['most_avail_fare_frequency'].iloc[0],
                "host_web_frequently_available_fare_tax": host['most_avail_fare_tax'].iloc[0],
                "host_web_frequently_available_count": host['most_avail_fare_count'].iloc[0],

                "host_lowest_filed_fare_total_fare": host['lowest_fare_total'].iloc[0],
                "host_lowest_filed_fare_base_fare": host['lowest_fare_base'].iloc[0],
                "host_lowest_filed_fare_tax": host['lowest_fare_tax'].iloc[0],
                "host_lowest_filed_fare_yq": host['lowest_fare_YQ'].iloc[0],
                "host_lowest_filed_fare_yr": host['lowest_fare_YR'].iloc[0],
                "host_lowest_filed_fare_surcharges": host['lowest_fare_surcharge'].iloc[0],
                "host_lowest_filed_fare_farebasis": host['lowest_fare_fb'].iloc[0],

                "marketShare": host['market_share'].iloc[0],
                "marketShare_VLYR": host['market_share_vlyr'].iloc[0],
                "marketShare_VTGT": host['market_share_vtgt'].iloc[0],
                "bookings": host['pax'].iloc[0],
                "bookings_vlyr": host['pax_vlyr'].iloc[0],
                "FMS": host['fms'].iloc[0],

                "hostRating": host['rating'].iloc[0],
                "capacityRating": host['capacity_rating'].iloc[0],
                "productRating": host['product_rating'].iloc[0],
                "fareRating": host['fare_rating'].iloc[0],
                "distributorRating": host['distributor_rating'].iloc[0],
                "marketRating": host['market_rating'].iloc[0],

                # "lowest_available_highestFare_totalFare": "NA",
                # "lowest_available_highestFare_yq": "NA",
                # "lowest_available_highestFare_tax": "NA",
                # "lowest_available_highestFare_surcharge": "NA",
                # "lowest_available_highestFare_baseFare": "NA",
                # "lowest_available_highestFare_frequency": "NA",
                # "lowest_available_lowestFare_totalFare": "NA",
                # "lowest_available_lowestFare_yq": "NA",
                # "lowest_available_lowestFare_tax": "NA",
                # "lowest_available_lowestFare_surcharge": "NA",
                # "lowest_available_lowestFare_baseFare": "NA",
                # "lowest_available_lowestFare_frequency": "NA",

                "filed_fare_highestFare_totalFare": host['highest_fare_total'].iloc[0],
                "filed_fare_highestFare_yq": host['highest_fare_YQ'].iloc[0],
                "filed_fare_highestFare_tax": host['highest_fare_tax'].iloc[0],
                "filed_fare_highestFare_surcharge": host['highest_fare_surcharge'].iloc[0],
                "filed_fare_highestFare_baseFare": host['highest_fare_base'].iloc[0],
                "filed_fare_highestFare_farebasis": host['highest_fare_fb'].iloc[0],

                "filed_fare_lowestFare_totalFare": host['lowest_fare_total'].iloc[0],
                "filed_fare_lowestFare_yq": host['lowest_fare_YQ'].iloc[0],
                "filed_fare_lowestFare_tax": host['lowest_fare_tax'].iloc[0],
                "filed_fare_lowestFare_surcharge": host['lowest_fare_surcharge'].iloc[0],
                "filed_fare_lowestFare_baseFare": host['lowest_fare_base'].iloc[0],
                "filed_fare_lowestFare_farebasis": host['lowest_fare_fb'].iloc[0]
            }
        )
    else:
        response.update(
            {
                "host_web_frequently_available_total_fare": "NA",
                "host_web_frequently_available_base_fare": "NA",
                "host_web_frequently_available_fare_frequency": "NA",
                "host_web_frequently_available_fare_tax": "NA",
                "host_web_frequently_available_count": "NA",

                "host_lowest_filed_fare_total_fare": "NA",
                "host_lowest_filed_fare_base_fare": "NA",
                "host_lowest_filed_fare_yr": "NA",
                "host_lowest_filed_fare_tax": "NA",
                "host_lowest_filed_fare_yq": "NA",
                "host_lowest_filed_fare_surcharges": "NA",
                "host_lowest_filed_fare_farebasis": "NA",

                "marketShare": "NA",
                "marketShare_VLYR": "NA",
                "marketShare_VTGT": "NA",
                "bookings": "NA",
                "hostRating": "NA",
                "capacityRating": "NA",
                "productRating": "NA",
                "fareRating": "NA",
                "distributorRating": "NA",
                "marketRating": "NA",

                # "lowest_available_highestFare_totalFare": "NA",
                # "lowest_available_highestFare_yq": "NA",
                # "lowest_available_highestFare_tax": "NA",
                # "lowest_available_highestFare_surcharge": "NA",
                # "lowest_available_highestFare_baseFare": "NA",
                # "lowest_available_highestFare_frequency": "NA",
                # "lowest_available_lowestFare_totalFare": "NA",
                # "lowest_available_lowestFare_yq": "NA",
                # "lowest_available_lowestFare_tax": "NA",
                # "lowest_available_lowestFare_surcharge": "NA",
                # "lowest_available_lowestFare_baseFare": "NA",
                # "lowest_available_lowestFare_frequency": "NA",

                "filed_fare_highestFare_totalFare": "NA",
                "filed_fare_highestFare_yq": "NA",
                "filed_fare_highestFare_tax": "NA",
                "filed_fare_highestFare_surcharge": "NA",
                "filed_fare_highestFare_baseFare": "NA",
                "filed_fare_highestFare_frequency": "NA",
                "filed_fare_highestFare_farebasis": "NA",

                "filed_fare_lowestFare_totalFare": "NA",
                "filed_fare_lowestFare_yq": "NA",
                "filed_fare_lowestFare_tax": "NA",
                "filed_fare_lowestFare_surcharge": "NA",
                "filed_fare_lowestFare_baseFare": "NA",
                "filed_fare_lowestFare_frequency": "NA",
                "filed_fare_lowestFare_farebasis": "NA",
                "FMS": "NA"
            }
        )

    response.update(
        {
            "comp_web_frequently_available_total_fare": [],
            "comp_web_frequently_available_base_fare": [],
            "comp_web_frequently_available_fare_frequency": [],
            "comp_web_frequently_available_fare_tax": [],
            "comp_web_frequently_available_count": [],

            "comp_lowest_filed_fare_total_fare": [],
            "comp_lowest_filed_fare_base_fare": [],
            "comp_lowest_filed_fare_yr": [],
            "comp_lowest_filed_fare_tax": [],
            "comp_lowest_filed_fare_yq": [],
            "comp_lowest_filed_fare_surcharges": [],
            "comp_lowest_filed_fare_farebasis": [],

            "compCarrier": [],

            "compMarketShare": [],
            "compMarketShare_VLYR": [],
            "compMarketShare_VTGT": [],
            "compBookings": [],
            "compBookings_VLYR": [],
            "compRating": [],
            "compCapacityRating": [],
            "compProductRating": [],
            "compFareRating": [],
            "compDistributorRating": [],
            "compMarketRating": [],

            # "lowest_available_highestFare_totalFare": "NA",
            # "lowest_available_highestFare_yq": "NA",
            # "lowest_available_highestFare_tax": "NA",
            # "lowest_available_highestFare_surcharge": "NA",
            # "lowest_available_highestFare_baseFare": "NA",
            # "lowest_available_highestFare_frequency": "NA",
            # "lowest_available_lowestFare_totalFare": "NA",
            # "lowest_available_lowestFare_yq": "NA",
            # "lowest_available_lowestFare_tax": "NA",
            # "lowest_available_lowestFare_surcharge": "NA",
            # "lowest_available_lowestFare_baseFare": "NA",
            # "lowest_available_lowestFare_frequency": "NA",

            "comp_filed_fare_highestFare_totalFare": [],
            "comp_filed_fare_highestFare_yq": [],
            "comp_filed_fare_highestFare_tax": [],
            "comp_filed_fare_highestFare_surcharge": [],
            "comp_filed_fare_highestFare_baseFare": [],
            # "comp_filed_fare_highestFare_frequency": [],
            "comp_filed_fare_highestFare_farebasis": [],

            "comp_filed_fare_lowestFare_totalFare": [],
            "comp_filed_fare_lowestFare_yq": [],
            "comp_filed_fare_lowestFare_tax": [],
            "comp_filed_fare_lowestFare_surcharge": [],
            "comp_filed_fare_lowestFare_baseFare": [],
            # "comp_filed_fare_lowestFare_frequency": [],
            "comp_filed_fare_lowestFare_farebasis": [],
            "compFMS": []
        }
    )

    comp = dF[dF['carrier'] != Host_Airline_Code]
    if len(comp) > 0:
        if len(comp_list) > 0:
            for comp_val in comp_list:
                temp_comp = comp[comp['carrier'] == comp_val]
                if len(temp_comp) > 0:
                    response["comp_web_frequently_available_total_fare"].append(
                        temp_comp['most_avail_fare_total'].iloc[0])
                    response["comp_web_frequently_available_base_fare"].append(
                        temp_comp['most_avail_fare_base'].iloc[0])
                    response["comp_web_frequently_available_fare_frequency"].append(
                        temp_comp['most_avail_fare_frequency'].iloc[0])
                    response["comp_web_frequently_available_fare_tax"].append(
                        temp_comp['most_avail_fare_tax'].iloc[0])
                    response["comp_web_frequently_available_count"].append(
                        temp_comp['most_avail_fare_count'].iloc[0])

                    response["comp_lowest_filed_fare_total_fare"].append(
                        temp_comp['lowest_fare_total'].iloc[0])
                    response["comp_lowest_filed_fare_base_fare"].append(
                        temp_comp['lowest_fare_base'].iloc[0])
                    response["comp_lowest_filed_fare_yr"].append(
                        temp_comp['lowest_fare_YR'].iloc[0])
                    response["comp_lowest_filed_fare_tax"].append(
                        temp_comp['lowest_fare_tax'].iloc[0])
                    response["comp_lowest_filed_fare_yq"].append(
                        temp_comp['lowest_fare_YQ'].iloc[0])
                    response["comp_lowest_filed_fare_surcharges"].append(
                        temp_comp['lowest_fare_surcharge'].iloc[0])
                    response["comp_lowest_filed_fare_farebasis"].append(
                        temp_comp['lowest_fare_fb'].iloc[0])

                    response["compCarrier"].append(comp_val)

                    response["compMarketShare"].append(
                        temp_comp['market_share'].iloc[0])
                    response["compMarketShare_VLYR"].append(
                        temp_comp['market_share_vlyr'].iloc[0])
                    response["compMarketShare_VTGT"].append(
                        temp_comp['market_share_vtgt'].iloc[0])
                    response["compBookings"].append(temp_comp['pax'].iloc[0])
                    response["compBookings_VLYR"].append(
                        temp_comp['pax_vlyr'].iloc[0])
                    response["compRating"].append(temp_comp['rating'].iloc[0])
                    response["compCapacityRating"].append(
                        temp_comp['capacity_rating'].iloc[0])
                    response["compProductRating"].append(
                        temp_comp['product_rating'].iloc[0])
                    response["compFareRating"].append(
                        temp_comp['fare_rating'].iloc[0])
                    response["compDistributorRating"].append(
                        temp_comp['distributor_rating'].iloc[0])
                    response["compMarketRating"].append(
                        temp_comp['market_rating'].iloc[0])

                    response["comp_filed_fare_highestFare_totalFare"].append(
                        temp_comp['highest_fare_total'].iloc[0])
                    response["comp_filed_fare_highestFare_yq"].append(
                        temp_comp['highest_fare_YQ'].iloc[0])
                    response["comp_filed_fare_highestFare_tax"].append(
                        temp_comp['highest_fare_tax'].iloc[0])
                    response["comp_filed_fare_highestFare_surcharge"].append(
                        temp_comp['highest_fare_surcharge'].iloc[0])
                    response["comp_filed_fare_highestFare_baseFare"].append(
                        temp_comp['highest_fare_base'].iloc[0])
                    # response["comp_filed_fare_highestFare_frequency"].append()
                    response["comp_filed_fare_highestFare_farebasis"].append(
                        temp_comp['highest_fare_fb'].iloc[0])

                    response["comp_filed_fare_lowestFare_totalFare"].append(
                        temp_comp['lowest_fare_total'].iloc[0])
                    response["comp_filed_fare_lowestFare_yq"].append(
                        temp_comp['lowest_fare_YQ'].iloc[0])
                    response["comp_filed_fare_lowestFare_tax"].append(
                        temp_comp['lowest_fare_tax'].iloc[0])
                    response["comp_filed_fare_lowestFare_surcharge"].append(
                        temp_comp['lowest_fare_surcharge'].iloc[0])
                    response["comp_filed_fare_lowestFare_baseFare"].append(
                        temp_comp['lowest_fare_base'].iloc[0])
                    # response["comp_filed_fare_lowestFare_frequency"].append()
                    response["comp_filed_fare_lowestFare_farebasis"].append(
                        temp_comp['lowest_fare_fb'].iloc[0])
                    response["compFMS"].append(temp_comp['fms'].iloc[0])

    return response


@measure(JUPITER_LOGGER)
def get_data(
        pos,
        origin,
        destination,
        compartment,
        dep_date_start,
        dep_date_end,
        db,
        comp,
        date_list=None):
    """
    """
    print "get_Data_comp", comp
    sta = time.time()

    main_dF = pd.DataFrame()
    dep_date_start_ly = get_ly_val(dep_date_start)
    dep_date_end_ly = get_ly_val(dep_date_end)
    print dep_date_start, dep_date_end

    ms_dF = get_ms_dF(
        pos,
        origin,
        destination,
        compartment,
        dep_date_start,
        dep_date_end,
        db=db,
        date_list=date_list)
    print 'MS_DATA', time.time() - sta
    sta = time.time()
    ms_dF = ms_dF.rename(columns={"MarketingCarrier1": "carrier"})
    # print "ms_dF ------->"
    # print ms_dF
    lowest_fares_dF = get_lowest_filed_fare_dF(
        pos,
        origin,
        destination,
        compartment,
        dep_date_start,
        dep_date_end,
        comp,
        db=db,
        date_list=date_list)
    print 'Loest Fares ', time.time() - sta
    sta = time.time()
    # print lowest_fares_dF.columns
    # print "lowest_fares_df ----------->"
    # print lowest_fares_dF
    capacity_dF = get_capacity_dF(
        pos,
        origin,
        destination,
        compartment,
        dep_date_start,
        dep_date_end,
        db=db,
        date_list=date_list)
    print 'Capacity', time.time() - sta
    sta = time.time()
    # print "capacity_df --------->"
    # print capacity_dF
    ratings_data = get_ratings_dict(
        pos,
        origin,
        destination,
        compartment,
        dep_date_start,
        dep_date_end)
    print 'Ratings', time.time() - sta
    sta = time.time()
    # print "ratings_data -------->"
    # print ratings_data
    most_avail_fare_dF = get_most_avail_dict(
        pos,
        origin,
        destination,
        compartment,
        dep_date_start,
        dep_date_end,
        db=db,
        date_list=date_list)
    print 'Avail', time.time() - sta
    sta = time.time()
    # print "most_available --------->"
    # print most_avail_fare_dF
    # print dep_date_start_ly, dep_date_end_ly
    ms_dF_ly = get_ms_dF(
        pos,
        origin,
        destination,
        compartment,
        dep_date_start_ly,
        dep_date_end_ly,
        db=db,
        date_list=date_list)
    # print 'MS_LY_Data', time.time() - sta
    sta = time.time()
    # print ms_dF_ly
    ms_dF_ly = ms_dF_ly.rename(
        columns={
            "market_share": "market_share_ly",
            "pax": "pax_ly",
            "MarketingCarrier1": "carrier"})
    # print 'Market Share LY Modified'
    # print ms_dF_ly
    if len(ms_dF_ly) == 0:
        ms_dF['market_share_ly'] = np.nan
        ms_dF['pax_ly'] = np.nan
    else:
        ms_dF = ms_dF.merge(ms_dF_ly, how='outer')

    print 'Market Share Merged'

    ms_dF['market_share_vlyr'] = (
        ms_dF['market_share'] - ms_dF['market_share_ly']) * 100 / ms_dF['market_share_ly']
    ms_dF['pax_vlyr'] = (ms_dF['pax'] - ms_dF['pax_ly']) * \
        100 / ms_dF['pax_ly']
    print "ms_df_merged --------->"
    # print ms_dF
    if len(ratings_data) == 0:
        print "zeros ratings data"
        carriers = capacity_dF['carrier'].unique()
        ratings_data_1 = pd.DataFrame(
            index=carriers,
            columns=[
                'capacity_ratings_dict',
                'distributor_ratings_dict',
                'fare_ratings_dict',
                'market_ratings_dict',
                'product_ratings_dict',
                'ratings_dict'])
        ratings_data_1.fillna(5, inplace=True)
        # else:
        # for index, row in capacity_dF.iterrows():
        #     carrier = row['carrier']
        #     try:
        #         rating = ratings_data['ratings_dict'][carrier]
        #     except KeyError:
        #         rating = 5
        # ratings_data['ratings_dict'][carrier] = 5
        capacity_dF = pd.merge(capacity_dF,
                               ratings_data_1[['ratings_dict']],
                               how='left',
                               right_index=True,
                               left_on='carrier')
    else:
        capacity_dF = pd.merge(capacity_dF,
                               ratings_data[['ratings_dict']],
                               how='left',
                               right_index=True,
                               left_on='carrier')
    capacity_dF["ratings_dict"].fillna(5, inplace=True)
    # capacity_dF.set_value(index, 'rating', rating)
    # capacity_dF.set_value(index, 'cap_rating', row['capacity'] * rating)
    capacity_dF['cap_rating'] = capacity_dF['capacity'] * \
        capacity_dF['ratings_dict']
    # print capacity_dF.sum()['cap_rating']
    capacity_dF['fms'] = capacity_dF['cap_rating'] * \
        100 / capacity_dF.sum()['cap_rating']
    capacity_dF.drop('ratings_dict', axis=1, inplace=True)
    print "capacity_merged --------->"
    # print capacity_dF
#    del ratings_data_1
    main_dF = ms_dF.merge(lowest_fares_dF, how='outer')
    main_dF = main_dF.merge(capacity_dF, how='outer')
    print "main_df ----------->"
    # print main_dF
    # print main_dF.fms
    # for idx, row in main_dF.iterrows():
    #     if row['fms'] > 0:
    #         ms_vtgt = (row['market_share'] - row['fms']) * 100 / row['fms']
    #         main_dF.set_value(idx, 'market_share_vtgt', ms_vtgt)
    main_dF['market_share_vtgt'] = (
        main_dF['market_share'] - main_dF['fms']) * 100 / main_dF['fms']
    main_dF.fillna("NA", inplace=True)
    main_dF = main_dF.merge(most_avail_fare_dF, how='outer')

    if len(ratings_data) == 0:
        carriers = main_dF['carrier'].unique()
        # print "unique_carrier ----->"
        # print carriers
        ratings_data_1 = pd.DataFrame(
            index=carriers,
            columns=[
                'capacity_ratings_dict',
                'distributor_ratings_dict',
                'fare_ratings_dict',
                'market_ratings_dict',
                'product_ratings_dict',
                'ratings_dict'])
        ratings_data_1.fillna(5, inplace=True)
        main_dF = pd.merge(
            main_dF,
            ratings_data_1,
            how='left',
            right_index=True,
            left_on='carrier')
    else:
        main_dF = pd.merge(
            main_dF,
            ratings_data,
            how='left',
            right_index=True,
            left_on='carrier')

    main_dF[['capacity_ratings_dict',
             'distributor_ratings_dict',
             'fare_ratings_dict',
             'market_ratings_dict',
             'product_ratings_dict',
             'ratings_dict']].fillna(5, inplace=True)
    # for index, row in main_dF.iterrows():
    #     carrier = row['carrier']
    #     try:
    #         rating = ratings_data['ratings_dict'][carrier]
    #     except KeyError:
    #         rating = 5
    #
    #     try:
    #         capacity_rating = ratings_data['capacity_ratings_dict'][carrier]
    #     except KeyError:
    #         capacity_rating = 5
    #
    #     try:
    #         fare_rating = ratings_data['fare_ratings_dict'][carrier]
    #     except KeyError:
    #         fare_rating = 5
    #
    #     try:
    #         distributor_rating = ratings_data['distributor_ratings_dict'][carrier]
    #     except KeyError:
    #         distributor_rating = 5
    #
    #     try:
    #         market_rating = ratings_data['market_ratings_dict'][carrier]
    #     except KeyError:
    #         market_rating = 5
    #
    #     try:
    #         product_rating = ratings_data['product_ratings_dict'][carrier]
    #     except KeyError:
    #         product_rating = 5
    #
    #     main_dF.set_value(index, 'rating', rating)
    #     main_dF.set_value(index, 'capacity_rating', capacity_rating)
    #     main_dF.set_value(index, 'fare_rating', fare_rating)
    #     main_dF.set_value(index, 'market_rating', market_rating)
    #     main_dF.set_value(index, 'product_rating', product_rating)
    #     main_dF.set_value(index, 'distributor_rating', distributor_rating)

    # print main_dF.columns
    main_dF = main_dF.where((pd.notnull(main_dF)), None)

    main_dF.fillna(value='NA', inplace=True)
    if "market_share_vtgt" not in main_dF.columns:
        main_dF['market_share_vtgt'] = "NA"
    # print main_dF
    print 'OTHER ANALYSIS', time.time() - sta
    sta = time.time()
    main_dF = main_dF.rename(
        columns={
            "ratings_dict": "rating",
            "capacity_ratings_dict": "capacity_rating",
            "fare_ratings_dict": "fare_rating",
            "market_ratings_dict": "market_rating",
            "product_ratings_dict": "product_rating",
            "distributor_ratings_dict": "distributor_rating"})
    # print main_dF.columns
    return main_dF


@measure(JUPITER_LOGGER)
def comp_summary_java(
        pos,
        origin,
        destination,
        compartment,
        dep_date_start,
        dep_date_end,
        db):
    main_dF = get_data(
        pos,
        origin,
        destination,
        compartment,
        dep_date_start,
        dep_date_end,
        db=db)
    top5_comp = obtain_top_5_comp(
        [pos],
        [origin],
        [destination],
        [compartment],
        dep_date_start,
        dep_date_end,
        db=db)
    comp = top5_comp[0]
    response = generate_response_java(
        pos,
        origin,
        destination,
        compartment,
        dep_date_start,
        dep_date_end,
        main_dF,
        comp)
    return response


@measure(JUPITER_LOGGER)
def comp_summary_python(
        pos,
        origin,
        destination,
        compartment,
        dep_date_start,
        dep_date_end,
        db,
        date_list=None):
    top5_comp = obtain_top_5_comp(
        [pos],
        [origin],
        [destination],
        [compartment],
        dep_date_start,
        dep_date_end,
        db=db,
        date_list=date_list)
    comp = top5_comp
    main_dF = get_data(
        pos,
        origin,
        destination,
        compartment,
        dep_date_start,
        dep_date_end,
        db=db,
        comp=comp,
        date_list=date_list)
    print "main_df --------->"
    main_dF.head()
    print 'Top 5 Competitors*************************************************'
    print comp
    # print main_dF
    response = generate_response_python(
        pos,
        origin,
        destination,
        compartment,
        dep_date_start,
        dep_date_end,
        main_dF,
        comp,
        date_list=date_list)
    return response


if __name__ == '__main__':
    client =mongo_client()
    db=client[JUPITER_DB]
    import time

    # lowest_df = get_lowest_filed_fare_dF('CMB', 'CMB', 'DXB', 'Y', '2017-09-01', '2017-09-30', comp=['EK','UL', 'EY'],date_list=[{'start':'2017-09-01','end':'2017-09-30'}])
    # print "LOWEST DF . . . "
    # print lowest_df
    # st = time.time()
    # dates_list = [{'start': '2017-04-27', 'end': '2017-05-04'}, {'start': '2017-04-27', 'end': '2017-05-11'},
    #               {'start': '2017-04-27', 'end': '2017-05-18'}, {'start': '2017-04-27', 'end': '2017-05-27'},
    #               {'start': '2017-04-27', 'end': '2017-06-11'}, {'start': '2017-04-27', 'end': '2017-06-26'},
    #               {'start': '2017-04-27', 'end': '2017-07-26'}, {'start': '2017-04-01', 'end': '2017-04-30'},
    #               {'start': '2017-05-01', 'end': '2017-05-31'}, {'start': '2017-06-01', 'end': '2017-06-30'}]
    #
    # # , (['EK', 'AI', '9W', 'WY', 'QR'], ['sys_gen', 'sys_gen', 'sys_gen', 'sys_gen', 'sys_gen']), (['EK', 'AI', '9W', 'WY', 'QR'], ['sys_gen', 'sys_gen', 'sys_gen', 'sys_gen', 'sys_gen'])]
    # comp_list = [(['EK', 'AI', '9W', 'WY', 'QR'], ['sys_gen',
    #                                                'sys_gen', 'sys_gen', 'sys_gen', 'sys_gen'])]
    #
    #     rat= get_data('DXB','DXB','DEL','Y','2017-04-01','2017-07-26', comp_list,date_list = [{'start': '2017-04-27', 'end': '2017-05-27'},
    # {'start': '2017-04-27', 'end': '2017-05-18'}, {'start': '2017-04-27', 'end': '2017-05-27'},
    # {'start': '2017-04-27', 'end': '2017-06-11'}, {'start': '2017-04-27', 'end': '2017-06-26'},
    # {'start': '2017-04-27', 'end': '2017-07-26'}, {'start': '2017-04-01', 'end': '2017-04-30'},
    # {'start': '2017-05-01', 'end': '2017-05-31'}, {'start': '2017-06-01', 'end': '2017-06-30'}])
    #
    #     response = generate_response_python("DXB", "DXB", "DEL", "Y", "2017-04-01", "2017-05-26", dF=rat, comp_list=comp_list, date_list = [{'start': '2017-04-27', 'end': '2017-05-27'},{'start': '2017-04-27', 'end': '2017-05-18'}, {'start': '2017-04-27', 'end': '2017-05-27'},{'start': '2017-04-27', 'end': '2017-06-11'}, {'start': '2017-04-27', 'end': '2017-06-26'},{'start': '2017-04-27', 'end': '2017-07-26'}, {'start': '2017-04-01', 'end': '2017-04-30'},{'start': '2017-05-01', 'end': '2017-05-31'}, {'start': '2017-06-01', 'end': '2017-06-30'}])

    # print rat.head()
    #            {'start': '2017-04-27', 'end': '2017-06-26'},
    #            {'start': '2017-04-27', 'end': '2017-07-26'},
    #            {'start': '2017-04-01', 'end': '2017-04-30'},
    #            {'start': '2017-05-01', 'end': '2017-05-31'},
    #            {'start': '2017-06-01', 'end': '2017-06-30'}])
    # dep_date_start = "2017-10-10"
    # dep_date_end = "2017-11-09"
    # response = get_lowest_filed_fare_dF("CMB",
    #                                'CMB',
    #                                'DXB',
    #                                'Y',
    #                                dep_date_start,
    #                                dep_date_end,
    #                                date_list=[{'start': dep_date_start,
    #                                            "end": dep_date_end}],
    #                                     comp=[([],[])])

    dep_date_start = "2018-04-01"
    dep_date_end = "2018-04-30"
    markets = db.JUP_DB_Pos_OD_Compartment_new.aggregate([
    {
        '$match':
        {
            'month_year': {'$gte': '201803', '$lte': '201804'}
        }
    },
    {
        '$unwind': '$top_5_comp_data'
    },
    {
        '$group':
        {
            '_id':
            {
                'market': '$market'
            },
            'carriers': {'$addToSet': '$top_5_comp_data.airline'}
        }
    },
    {
        '$project':
        {
            '_id': 0,
            'market': '$_id.market',
            'carriers': '$carriers'
        }
    },
        {
            '$limit': 100
        }
    ])
    st = time.time()
    for market in markets:
        response = get_lowest_filed_fare_dF(market['market'][:3],
                                       market['market'][3:6],
                                       market['market'][6:9],
                                       market['market'][9:10],
                                       dep_date_start,
                                       dep_date_end,
                                        db=db,
                                       date_list=[{'start': dep_date_start,
                                                   "end": dep_date_end}],
                                        comp=[(market['carriers'],[])])
    et = time.time()
    print "Total time taken = ", et - st
    client.close()
    # data = comp_summary_python('AAN',"AMM",'DXB','Y','2017-04-27','2017-05-27')
    # print data
    # print json.dumps(rat['ratings_dict'])
    # print time.time() - st
    # print
    # comp_summary_python('ABC','ABC','ABC','G','2017-05-01','2017-07-31')
    # print time.time() - st
    # print get_lowest_filed_fare_dF(None,'AMM','ADD','Y','2017-04-01','2017-04-30')
    # DOH DOH BOM Y 2017-04-01 2017-04-30

    # print get_lowest_filed_fare_dF('DXB', 'DXB', 'DEL', 'Y',
    # dep_date_start='2017-05-01', dep_date_end='2017-05-31')
