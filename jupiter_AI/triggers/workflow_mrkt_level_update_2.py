"""
Author: Nikunj Agarwal
Created with <3
Date: 2017-07-29

The function in this code updates following params for(pos/od/compartment and departure date range)
pos
origin
destination
compartment
dep_date_start
dep_date_end

Performance Params of Host:
    pax_data
    revenue_data
    avg_fare_data
    yield_data
    seat_factor_data

mrkt_data
    {
        'host':{},
        'comp':[
            {comp1 data},
            {comp2 data},
            {comp3 data
        ]

    }
    fare
        [
            {fare doc 1},
            {fare doc 2},
            .
            .
            .

        ]
"""
import calendar
import datetime
import json
import time
from copy import deepcopy

import numpy as np
import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
import socket
from requests.exceptions import ConnectTimeout,\
    HTTPError,\
    ReadTimeout,\
    SSLError,\
    ConnectionError,\
    Timeout
from jupiter_AI import mongo_client, JUPITER_DB, Host_Airline_Hub as hub, JUPITER_LOGGER, Host_Airline_Code
from jupiter_AI.logutils import measure
from jupiter_AI.network_level_params import SYSTEM_DATE, today, JAVA_URL, get_jupiter_logger
from jupiter_AI.triggers.Host_params_workflow import get_performance_data, get_od_capacity, get_bookings_df
# from jupiter_AI.batch.fbmapping_batch.JUP_DB_Batch_Fare_Ladder_Mapping_Web import map_fare_infare
from jupiter_AI.triggers.common import get_start_end_dates
from jupiter_AI.triggers.common import pos_level_currency_data as get_currency_data
from jupiter_AI.triggers.mrkt_params_workflow_opt import comp_summary_python as get_mrkt_params
#from jupiter_AI.triggers.mrkt_params_workflow_opt import get_lowest_filed_fare_dF, get_most_avail_dict
from jupiter_AI.triggers.recommendation_models.pricing_model_new import main as get_fares_data

#db = client[JUPITER_DB]
url = JAVA_URL
SLEEP_TIME = 0.05

@measure(JUPITER_LOGGER)
def get_od_distance(od, db):
    """
    :return: 1592

    """
    crsr = db.JUP_DB_OD_Distance.find({'od': od})
    if crsr.count() == 1:
        distance = crsr[0]['distance']
        return distance


@measure(JUPITER_LOGGER)
def get_config_dates(pos, od, compartment, db):
    """
    :return: dates_list defined in the configuration collection for a day of week
    """
    sig_cur = list(db.temp_sig_markets.find({'market': pos + od + compartment}, {'sig_flag': 1, '_id': 0}))
    if len(sig_cur) != 0:
        sig_flag = sig_cur[0]['sig_flag']
        day_of_week = calendar.day_name[today.weekday()]
        cur = db.JUP_DB_Config_Date.find({'day_of_week': day_of_week,
                                          'market': sig_flag,
                                          'effective_from': {'$lte': SYSTEM_DATE},
                                          '$or': [{'effective_to': {'$gte': SYSTEM_DATE}},
                                                  {'effective_to': ""}]})
        if cur.count() != 0:
            dates_list = []
            for i in cur:
                if i['period'] == 'Rolling':
                    dates_list.append({'start': SYSTEM_DATE,
                                       'end': datetime.datetime.strftime(today +
                                          relativedelta(days=int(i['reco_period'])), "%Y-%m-%d"),
                                       'code_list': ['RL{}{}'.format(int(i['reco_period']), SYSTEM_DATE)]})

                else:
                    if i['period'] == 'Month':
                        date = today + relativedelta(months=int(i['reco_period']))
                    elif i['period'] == 'Quarter':
                        date = today + relativedelta(months=int(4*i['reco_period']))
                    elif i['period'] == 'Year':
                        date = today + relativedelta(years=int(i['reco_period']))
                    elif i['period'] == 'Week':
                        date = today + relativedelta(days=int(7*i['reco_period']))
                    else:
                        date = today
                    date = date.strftime('%Y-%m-%d')
                    date_cur = db.JUP_DB_Calendar_Master.aggregate([
                        {
                            '$match': {'period': i['period'],
                                       'from_date': {'$lte': date},
                                       'to_date': {'$gte': date}
                                       }
                        },
                        {
                            '$group': {
                                '_id': None,
                                'from_date': {'$min': '$from_date'},
                                'to_date': {'$max': '$to_date'},
                                'code_list': {'$addToSet': '$combine_column'}
                            }
                        }
                    ])
                    for j in date_cur:
                        if j['from_date'] < SYSTEM_DATE < j['to_date']:
                            j['from_date'] = SYSTEM_DATE
                        dates_list.append({'start': j['from_date'], 'end': j['to_date'], 'code_list': j['code_list']})

            # if today.month == 12:
            #     next_month = 1
            #     next_year = today.year + 1
            # else:
            #     next_month = today.month + 1
            #     next_year = today.year
            # cm_1_sd, cm_1_ed = get_start_end_dates(month=next_month, year=next_year)
            # code = 'M' + str(next_year) + str(next_month)
            # temp_dict = {'start': cm_1_sd, 'end': cm_1_ed, 'code_list': [code]}
            # if (temp_dict not in dates_list) and (today.day > 15):  # to consider next month reco (configurable)
            #     dates_list.append(temp_dict)

        else:
            dates_list = get_dep_date_filters()
    else:
        dates_list = get_dep_date_filters()
    print 'Number of Recos to do:', len(dates_list)
    return dates_list


@measure(JUPITER_LOGGER)
def get_dep_date_filters(pos=None):
    """
    :return: dates_list = [{'start': '2017-04-27', 'end': '2017-05-04'}, {'start': '2017-04-27', 'end': '2017-05-11'},
                  {'start': '2017-04-27', 'end': '2017-05-18'}, {'start': '2017-04-27', 'end': '2017-05-27'},
                  {'start': '2017-04-27', 'end': '2017-06-11'}, {'start': '2017-04-27', 'end': '2017-06-26'},
                  {'start': '2017-04-27', 'end': '2017-07-26'}, {'start': '2017-04-01', 'end': '2017-04-30'},
                  {'start': '2017-05-01', 'end': '2017-05-31'}, {'start': '2017-06-01', 'end': '2017-06-30'}]

    """
    # crsr = db.JUP_DB_Config_Date.find()
    dates_list = []
    # if pos:
    #     user_crsr = db.JUP_DB_User.find(
    #         {"list_of_pos": pos, "role": "pricing_officer"}).limit(1)
    #     user_crsr = list(user_crsr)
    #     if len(user_crsr) > 0:
    #         curr_year = SYSTEM_DATE[0:4]
    #         cluster_name = user_crsr[0]['cluster']
    #         events_crsr = list(
    #             db.JUP_DB_Pricing_Calendar.find(
    #                 {
    #                     "Cluster": cluster_name,
    #                     "Start_date_" +
    #                     curr_year: {
    #                         "$lte": SYSTEM_DATE},
    #                     "End_date_" +
    #                     curr_year: {
    #                         "$gte": SYSTEM_DATE}}).sort(
    #                 "End_date_" +
    #                     curr_year,
    #                 pymongo.ASCENDING).limit(1))
    #         if events_crsr.count() == 0:
    #             events_crsr = list(
    #                 db.JUP_DB_Pricing_Calendar.find(
    #                     {
    #                         "Cluster": cluster_name,
    #                         "Start_date_" +
    #                         curr_year: {
    #                             "$gte": SYSTEM_DATE}}).sort(
    #                     "Start_date_" +
    #                     curr_year,
    #                     pymongo.ASCENDING).limit(1))
    #             if events_crsr.count() > 0:
    #                 event_date_to = events_crsr[0]['Start_date_' + curr_year]
    #                 event_date_from = events_crsr[0]['End_date_' + curr_year]
    #                 dates_list.append(
    #                     {'start': event_date_from, 'end': event_date_to})

    for val in [7, 14]:

        # Week definition for FZ is Sun-Sat
        val_date = today - datetime.timedelta(days=today.weekday()) + datetime.timedelta(days=int(val) - 1)

        end_date_str = datetime.datetime.strftime(val_date, '%Y-%m-%d')

        # 1 day is added because for FZ Week Definition is Sun-Sat
        current_week = (today + datetime.timedelta(days=1)).isocalendar()[1]
        code1 = 'W' + str((today + datetime.timedelta(days=1)).year) + str(current_week)
        tempdict = {}
        if val == 7:
            tempdict = {'start': SYSTEM_DATE, 'end': end_date_str, 'code_list': [code1]}
        if val == 14:
            next_week = (today + datetime.timedelta(days=8)).isocalendar()[1]
            code2 = 'W' + str((today + datetime.timedelta(days=1)).year) + str(next_week)
            tempdict = {'start': SYSTEM_DATE, 'end': end_date_str, 'code_list': [code1, code2]}
        if tempdict not in dates_list:
            dates_list.append(tempdict)

    # val_date = today + relativedelta(month=1)      ## This is wrong. this will always give year-01-day
    # end_date_str = datetime.datetime.strftime(val_date, '%Y-%m-%d')
    # tempdict = {'start': SYSTEM_DATE, 'end': end_date_str}
    # if tempdict not in dates_list:
    #     dates_list.append(tempdict)

    # def_date_end_obj = today + datetime.timedelta(days=30)
    # def_date_end = datetime.datetime.strftime(def_date_end_obj,'%Y-%m-%d')

    # dates_list.append({'start': SYSTEM_DATE, 'end': def_date_end})

    current_month = today.month
    current_year = today.year
    cm_sd, cm_ed = get_start_end_dates(month=current_month, year=current_year)
    code = 'M' + str(current_year) + str(current_month)
    dates_list.append({'start': SYSTEM_DATE, 'end': cm_ed, 'code_list': [code]})

    if current_month == 12:
        next_month = 1
        next_year = current_year + 1
    else:
        next_month = current_month + 1
        next_year = current_year

    cm_1_sd, cm_1_ed = get_start_end_dates(month=next_month, year=next_year)
    code = 'M' + str(next_year) + str(next_month)
    dates_list.append({'start': cm_1_sd, 'end': cm_1_ed, 'code_list': [code]})

    if next_month == 12:
        next_month = 1
        next_year = next_year + 1
    else:
        next_month = next_month + 1
        next_year = next_year

    cm_2_sd, cm_2_ed = get_start_end_dates(month=next_month, year=next_year)
    code = 'M' + str(next_year) + str(next_month)
    dates_list.append({'start': cm_2_sd, 'end': cm_2_ed, 'code_list': [code]})

    if next_month == 12:
        next_month = 1
        next_year = next_year + 1
    else:
        next_month = next_month + 1
        next_year = next_year

    cm_3_sd, cm_3_ed = get_start_end_dates(month=next_month, year=next_year)
    code = 'M' + str(next_year) + str(next_month)
    dates_list.append({'start': cm_3_sd, 'end': cm_3_ed, 'code_list': [code]})

    return dates_list


#@measure(JUPITER_LOGGER)
# def get_dep_date_filters(pos=None):
#     """
#     :return: dates_list = [{'start': '2017-04-27', 'end': '2017-05-04'}, {'start': '2017-04-27', 'end': '2017-05-11'},
#                   {'start': '2017-04-27', 'end': '2017-05-18'}, {'start': '2017-04-27', 'end': '2017-05-27'},
#                   {'start': '2017-04-27', 'end': '2017-06-11'}, {'start': '2017-04-27', 'end': '2017-06-26'},
#                   {'start': '2017-04-27', 'end': '2017-07-26'}, {'start': '2017-04-01', 'end': '2017-04-30'},
#                   {'start': '2017-05-01', 'end': '2017-05-31'}, {'start': '2017-06-01', 'end': '2017-06-30'}]
#
#     """
#     # crsr = db.JUP_DB_Config_Date.find()
#     dates_list = []
#     # if pos:
#     #     user_crsr = db.JUP_DB_User.find(
#     #         {"list_of_pos": pos, "role": "pricing_officer"}).limit(1)
#     #     user_crsr = list(user_crsr)
#     #     if len(user_crsr) > 0:
#     #         curr_year = SYSTEM_DATE[0:4]
#     #         cluster_name = user_crsr[0]['cluster']
#     #         events_crsr = list(
#     #             db.JUP_DB_Pricing_Calendar.find(
#     #                 {
#     #                     "Cluster": cluster_name,
#     #                     "Start_date_" +
#     #                     curr_year: {
#     #                         "$lte": SYSTEM_DATE},
#     #                     "End_date_" +
#     #                     curr_year: {
#     #                         "$gte": SYSTEM_DATE}}).sort(
#     #                 "End_date_" +
#     #                     curr_year,
#     #                 pymongo.ASCENDING).limit(1))
#     #         if events_crsr.count() == 0:
#     #             events_crsr = list(
#     #                 db.JUP_DB_Pricing_Calendar.find(
#     #                     {
#     #                         "Cluster": cluster_name,
#     #                         "Start_date_" +
#     #                         curr_year: {
#     #                             "$gte": SYSTEM_DATE}}).sort(
#     #                     "Start_date_" +
#     #                     curr_year,
#     #                     pymongo.ASCENDING).limit(1))
#     #             if events_crsr.count() > 0:
#     #                 event_date_to = events_crsr[0]['Start_date_' + curr_year]
#     #                 event_date_from = events_crsr[0]['End_date_' + curr_year]
#     #                 dates_list.append(
#     #                     {'start': event_date_from, 'end': event_date_to})
#
#     for val in [7, 14]:
#         #     for val in doc['configured_days']:
#         val_date = today + datetime.timedelta(days=int(val))
#         # val_date = today + relativedelta(month=1)
#         end_date_str = datetime.datetime.strftime(val_date, '%Y-%m-%d')
#         tempdict = {'start': SYSTEM_DATE, 'end': end_date_str}
#         if tempdict not in dates_list:
#             dates_list.append(tempdict)
#
#     # val_date = today + relativedelta(month=1)      ## This is wrong. this will always give year-01-day
#     # end_date_str = datetime.datetime.strftime(val_date, '%Y-%m-%d')
#     # tempdict = {'start': SYSTEM_DATE, 'end': end_date_str}
#     # if tempdict not in dates_list:
#     #     dates_list.append(tempdict)
#
#     # def_date_end_obj = today + datetime.timedelta(days=30)
#     # def_date_end = datetime.datetime.strftime(def_date_end_obj,'%Y-%m-%d')
#
#     # dates_list.append({'start': SYSTEM_DATE, 'end': def_date_end})
#
#     current_month = today.month
#     current_year = today.year
#     cm_sd, cm_ed = get_start_end_dates(month=current_month, year=current_year)
#     dates_list.append({'start': SYSTEM_DATE, 'end': cm_ed})
#
#     if current_month == 12:
#        next_month = 1
#        next_year = current_year+1
#     else:
#        next_month = current_month+1
#        next_year = current_year
#
#     cm_1_sd, cm_1_ed = get_start_end_dates(month=next_month, year=next_year)
#     dates_list.append({'start': cm_1_sd, 'end': cm_1_ed})
#
#     if next_month == 12:
#        next_month = 1
#        next_year = next_year+1
#     else:
#        next_month = next_month+1
#        next_year = next_year
#
#     cm_2_sd, cm_2_ed = get_start_end_dates(month=next_month, year=next_year)
#     dates_list.append({'start': cm_2_sd, 'end': cm_2_ed})
#
#     if next_month == 12:
#        next_month = 1
#        next_year = next_year+1
#     else:
#        next_month = next_month+1
#        next_year = next_year
#
#     cm_3_sd, cm_3_ed = get_start_end_dates(month=next_month, year=next_year)
#     dates_list.append({'start': cm_3_sd, 'end': cm_3_ed})
#
#     return dates_list


@measure(JUPITER_LOGGER)
def get_revenue_data(df, capacity):
    """
    :return: {revenue: 869, vtgt: 7.5, vlyr: 6.5}

    The df received is already sorted in ascending order of dates
    """
    ## ---------- Calculating revenue and VLYR ------------------- ##

    if 'flown_revenue' in df.columns:
        flown_revenue_ty = df.loc[df['dep_date']
                                  < SYSTEM_DATE, 'flown_revenue'].sum()
        print "flown_revenue_ty  =   ", flown_revenue_ty
    else:
        flown_revenue_ty = 0
    if 'sales_revenue' in df.columns:
        sales_revenue_ty = df.loc[df['dep_date']
                                  >= SYSTEM_DATE, 'sales_revenue'].sum()
        print "sales_revenue_ty  =  ", sales_revenue_ty
    else:
        sales_revenue_ty = 0
    # This will be used to calculate avg_fare and avg_fare_vlyr in
    # get_avg_fare_data()
    global total_revenue_ty
    total_revenue_ty = flown_revenue_ty + sales_revenue_ty
    print "Total revenue ty =   ", total_revenue_ty
    if 'flown_revenue_l' in df.columns:
        flown_revenue_ly = df.loc[df['dep_date'] <
                                  SYSTEM_DATE, 'flown_revenue_l'].sum()
        print "flown_revenue_ly  =  ", flown_revenue_ly
    else:
        flown_revenue_ly = 0
    if 'sales_revenue_l' in df.columns:
        sales_revenue_ly = df.loc[df['dep_date'] >=
                                  SYSTEM_DATE, 'sales_revenue_l'].sum()
        print "sales_revenue_ly =  ", sales_revenue_ly
    else:
        sales_revenue_ly = 0
    # This will be used to calculate avg_fare_vlyr in get_avg_fare_data()
    global total_revenue_ly
    total_revenue_ly = flown_revenue_ly + sales_revenue_ly
    print "total_revenue_ly  =  ", total_revenue_ly
    if type(
            capacity['ly']) in [
        int,
        float,
        np.int64,
        np.float64] and type(total_revenue_ly) in [
        int,
        float,
        np.int64,
        np.float64] and total_revenue_ly > 0 and type(
        capacity['ty']) in [
        int,
        float,
        np.int64,
        np.float64] and capacity['ty'] > 0:
        revenue_vlyr = (
                               (total_revenue_ty * capacity['ly'] / capacity[
                                   'ty']) - total_revenue_ly) * 100 / total_revenue_ly
        print "revenue_vlyr  =   ", revenue_vlyr
    else:
        revenue_vlyr = 'NA'

    ## ----------------- Calculating VTGT --------------------- ##

    df_temp = df[df['dep_date'] >= SYSTEM_DATE]
    df_temp = df_temp.drop_duplicates(subset=['dep_month'])

    start_date = df.iloc[0]['dep_date']
    print "start date in get_revenue_data   =    ", start_date
    start_date_obj = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    start_date_day = start_date_obj.day
    start_date_month = start_date_obj.month
    start_date_year = start_date_obj.year

    end_date = df.iloc[-1]['dep_date']
    print "end_date in get_revenue_data  =  ", end_date
    end_date_obj = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    end_date_day = end_date_obj.day
    end_date_month = end_date_obj.month
    end_date_year = end_date_obj.year

    if 'revenue_target' in df.columns:
        target_middle_months = df_temp[(df_temp['dep_month'] != start_date_month) & (
                df_temp['dep_month'] != end_date_month)]['revenue_target'].sum()
        print "target_middle_months =  ", target_middle_months
    else:
        target_middle_months = 0
    if 'revenue_target' in df.columns:
        # iloc[0] is used to access first element of the pandas Series. Since
        # series contains only one element
        target_revenue_start_month = df_temp.loc[df_temp['dep_month']
                                                 == start_date_month, 'revenue_target'].iloc[0]
        print "target_revenue_start_month  =  ", target_revenue_start_month
    else:
        target_revenue_start_month = 0
    no_days_start_month = calendar.monthrange(
        start_date_year, start_date_month)[1]
    if no_days_start_month > 0:
        target_start_month_extrapolated = target_revenue_start_month / \
                                          no_days_start_month * (no_days_start_month - start_date_day + 1)
        print "target_revenue_start month extrapolated =   ", target_start_month_extrapolated
    else:
        target_start_month_extrapolated = target_revenue_start_month
    if 'revenue_target' in df.columns:
        target_revenue_end_month = df_temp.loc[df_temp['dep_month']
                                               == end_date_month, 'revenue_target'].iloc[0]
        print "target_revenue_end_month  =  ", target_revenue_end_month
    else:
        target_revenue_end_month = 0
    no_days_end_month = calendar.monthrange(end_date_year, end_date_month)[1]
    if no_days_end_month > 0:
        target_end_month_extrapolated = target_revenue_end_month / \
                                        no_days_end_month * end_date_day
        print "target_revenue_end_month_extrapolated  =  ", target_end_month_extrapolated
    else:
        target_end_month_extrapolated = target_revenue_end_month
    global target_revenue

    if start_date_month != end_date_month:
        target_revenue = target_start_month_extrapolated + \
                         target_middle_months + target_end_month_extrapolated
    else:
        target_same_month = target_revenue_start_month
        target_same_month_extrapolated = target_same_month * \
                                         (end_date_day - start_date_day + 1) / no_days_start_month
        target_revenue = target_same_month_extrapolated
    print "total target revenue =   ", target_revenue

    if 'revenue_forecast' in df.columns:
        forecast_middle_months = df_temp[(df_temp['dep_month'] != start_date_month) & (
                df_temp['dep_month'] != end_date_month)]['revenue_forecast'].sum()
        print "forecast middle months =  ", forecast_middle_months
    else:
        forecast_middle_months = 0
    if 'revenue_forecast' in df.columns:
        forecast_revenue_start_month = df_temp.loc[df_temp['dep_month']
                                                   == start_date_month, 'revenue_forecast'].iloc[0]
        print "forecast revenue start month =  ", forecast_revenue_start_month
        print "dType of forecast revenue start month = ", type(forecast_revenue_start_month)
    else:
        forecast_revenue_start_month = 0
    if no_days_start_month > 0:
        forecast_start_month_extrapolated = forecast_revenue_start_month / \
                                            no_days_start_month * (no_days_start_month - start_date_day + 1)
        print "forecast revenue start month extrapolated =   ", forecast_start_month_extrapolated
    else:
        forecast_start_month_extrapolated = forecast_revenue_start_month
    if 'revenue_forecast' in df.columns:
        forecast_revenue_end_month = df_temp.loc[df_temp['dep_month']
                                                 == end_date_month, 'revenue_forecast'].iloc[0]
        print "forecast revenue end month =  ", forecast_revenue_end_month
    else:
        forecast_revenue_end_month = 0
    if no_days_end_month > 0:
        forecast_end_month_extrapolated = forecast_revenue_end_month / \
                                          no_days_end_month * end_date_day
        print "forecast revenue end month extrapolated  =  ", forecast_end_month_extrapolated
    else:
        forecast_end_month_extrapolated = forecast_revenue_end_month
    if start_date_month != end_date_month:
        revenue_future_forecast = forecast_start_month_extrapolated + \
                                  forecast_middle_months + forecast_end_month_extrapolated
    else:
        forecast_same_month = forecast_revenue_start_month
        forecast_same_month_extrapolated = forecast_same_month * \
                                           (end_date_day - start_date_day + 1) / no_days_start_month
        revenue_future_forecast = forecast_same_month_extrapolated
    print "revenue future forecast =  ", revenue_future_forecast

    global actual_revenue
    actual_revenue = flown_revenue_ty + revenue_future_forecast
    print "actual revenue   =    ", actual_revenue
    if target_revenue > 0:
        revenue_vtgt = (actual_revenue - target_revenue) * 100 / target_revenue
        print "Revenue vtgt =  ", revenue_vtgt
    else:
        revenue_vtgt = 'NA'

    revenue_data = {
        'revenue': total_revenue_ty,
        'vlyr': revenue_vlyr,
        'vtgt': revenue_vtgt}

    return revenue_data


@measure(JUPITER_LOGGER)
def get_pax_data(df, capacity):
    """
    :return: {pax: 94, vtgt: 7.5, vlyr: 6.5}
    The df received is already in sorted order of dates
    """
    ## -------------------- Calculating pax and VLYR -------------------##

    if 'flown_pax' in df.columns:
        flown_pax_ty = df.loc[df['dep_date'] < SYSTEM_DATE, 'flown_pax'].sum()
        print "flown_pax_ty is present in columns= ", flown_pax_ty
    else:
        flown_pax_ty = 0
    if 'sales_pax' in df.columns:
        sales_pax_ty = df.loc[df['dep_date'] >= SYSTEM_DATE, 'sales_pax'].sum()
        print "sales_pax_ty is present in columns = ", sales_pax_ty
    else:
        sales_pax_ty = 0
    # This will be used to calculate avg_fare and avg_fare_vlyr in
    # get_avg_fare_data()
    global total_pax_ty
    total_pax_ty = flown_pax_ty + sales_pax_ty
    print "total_pax_ty = ", total_pax_ty
    if 'flown_pax_l' in df.columns:
        flown_pax_ly = df.loc[df['dep_date'] <
                              SYSTEM_DATE, 'flown_pax_l'].sum()
        print "flown_pax_l is present in columns and = ", flown_pax_ly
    else:
        flown_pax_ly = 0
    if 'sales_pax_l' in df.columns:
        sales_pax_ly = df.loc[df['dep_date'] >=
                              SYSTEM_DATE, 'sales_pax_l'].sum()
        print "sales_pax_l is present in columns and = ", sales_pax_ly
    else:
        sales_pax_ly = 0
    global total_pax_ly  # This will be used to calculate avg_fare_vlyr in get_avg_fare_data()
    total_pax_ly = flown_pax_ly + sales_pax_ly
    print "total_pax_ly = ", total_pax_ly
    if type(
            capacity['ty']) in [
        int,
        float,
        np.int64,
        np.float64] and type(
        capacity['ly']) in [
        int,
        float,
        np.int64,
        np.float64] and capacity['ty'] > 0 and type(total_pax_ly) in [
        int,
        float,
        np.int64,
        np.float64] and total_pax_ly > 0:
        pax_vlyr = (
                           (total_pax_ty * capacity['ly'] / capacity['ty']) - total_pax_ly) * 100 / total_pax_ly
        print "pax_vlyr must have some value  = ", pax_vlyr
    else:
        pax_vlyr = 'NA'

    ## -------------------------- Calculating VTGT ---------------------------------- ##

    df_temp = df[df['dep_date'] >= SYSTEM_DATE]
    df_temp = df_temp.drop_duplicates(subset=['dep_month'])

    start_date = df.iloc[0]['dep_date']
    print "start date in get_pax_data   =    ", start_date
    start_date_obj = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    start_date_day = start_date_obj.day
    start_date_month = start_date_obj.month
    start_date_year = start_date_obj.year

    end_date = df.iloc[-1]['dep_date']
    print "end_date in get_pax_data  =  ", end_date
    end_date_obj = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    end_date_day = end_date_obj.day
    end_date_month = end_date_obj.month
    end_date_year = end_date_obj.year

    if 'pax_target' in df.columns:
        target_middle_months = df_temp[(df_temp['dep_month'] != start_date_month) & (
                df_temp['dep_month'] != end_date_month)]['pax_target'].sum()
        print "Target_middle_months has some value = ", target_middle_months
    else:
        target_middle_months = 0
    if 'pax_target' in df.columns:
        target_pax_start_month = df.loc[df['dep_month']
                                        == start_date_month, 'pax_target'].iloc[0]
        print "target_pax_start_month has some value = ", target_pax_start_month
    else:
        target_pax_start_month = 0
    no_days_start_month = calendar.monthrange(
        start_date_year, start_date_month)[1]
    print "Number of days start month = ", no_days_start_month
    if no_days_start_month > 0:
        target_start_month_extrapolated = target_pax_start_month / \
                                          no_days_start_month * (no_days_start_month - start_date_day + 1)
        print "Target_start_month extrapolated = ", target_start_month_extrapolated
    else:
        target_start_month_extrapolated = target_pax_start_month
    if 'pax_target' in df.columns:
        target_pax_end_month = df.loc[df['dep_month']
                                      == end_date_month, 'pax_target'].iloc[0]
        print "target_pax_end_month has some value = ", target_pax_end_month
    else:
        target_pax_end_month = 0
    no_days_end_month = calendar.monthrange(end_date_year, end_date_month)[1]
    print "number of days in end month = ", no_days_end_month
    if no_days_end_month > 0:
        target_end_month_extrapolated = target_pax_end_month / \
                                        no_days_end_month * end_date_day
        print "target pax end month extrapolated has some value = ", target_end_month_extrapolated
    else:
        target_end_month_extrapolated = target_pax_end_month
    global target_pax

    if start_date_month != end_date_month:
        target_pax = target_start_month_extrapolated + \
                     target_middle_months + target_end_month_extrapolated
    else:
        target_same_month = target_pax_start_month
        target_same_month_extrapolated = target_same_month * \
                                         (end_date_day - start_date_day + 1) / no_days_start_month
        target_pax = target_same_month_extrapolated
    print "total target pax = ", target_pax

    if 'pax_forecast' in df.columns:
        forecast_middle_months = df_temp[(df_temp['dep_month'] != start_date_month) & (
                df_temp['dep_month'] != end_date_month)]['pax_forecast'].sum()
        print "forecast middle months has some value = ", forecast_middle_months
    else:
        forecast_middle_months = 0
    if 'pax_forecast' in df.columns:
        forecast_pax_start_month = df.loc[df['dep_month']
                                          == start_date_month, 'pax_forecast'].iloc[0]
        print "forecast pax start month has some value = ", forecast_pax_start_month
    else:
        forecast_pax_start_month = 0
    if no_days_start_month > 0:
        forecast_start_month_extrapolated = forecast_pax_start_month / \
                                            no_days_start_month * (no_days_start_month - start_date_day + 1)
        print "forecast start month extrapolated has some value = ", forecast_start_month_extrapolated
    else:
        forecast_start_month_extrapolated = forecast_pax_start_month
    if 'pax_forecast' in df.columns:
        forecast_pax_end_month = df.loc[df['dep_month']
                                        == end_date_month, 'pax_forecast'].iloc[0]
        print "forecast pax end month has some value = ", forecast_pax_end_month
    else:
        forecast_pax_end_month = 0
    if no_days_end_month > 0:
        forecast_end_month_extrapolated = forecast_pax_end_month / \
                                          no_days_end_month * end_date_day
        print "forecast end month extrapolated has some value = ", forecast_end_month_extrapolated
    else:
        forecast_end_month_extrapolated = forecast_pax_end_month

    if start_date_month != end_date_month:
        pax_future_forecast = forecast_start_month_extrapolated + \
                              forecast_middle_months + forecast_end_month_extrapolated
    else:
        forecast_same_month = forecast_pax_start_month
        forecast_same_month_extrapolated = forecast_same_month * \
                                           (end_date_day - start_date_day + 1) / no_days_start_month
        pax_future_forecast = forecast_same_month_extrapolated
    print "pax_future_forecast   =   ", pax_future_forecast

    global actual_pax
    actual_pax = flown_pax_ty + pax_future_forecast
    print "actual Pax = ", actual_pax
    if target_pax > 0:
        pax_vtgt = (actual_pax - target_pax) * 100 / target_pax
        print "pax vtgt has some value =   ", pax_vtgt
    else:
        pax_vtgt = 'NA'

    pax_data = {'pax': total_pax_ty, 'vlyr': pax_vlyr, 'vtgt': pax_vtgt}

    return pax_data


@measure(JUPITER_LOGGER)
def get_avg_fare_data(pax_data, revenue_data):
    """
    :return: {avg_fare: 400 ,vtgt: 7.8 ,vlyr: 6.5 }

    """
    global avg_fare_ty
    if type(
            pax_data['pax']) in [
        int,
        float,
        np.int64,
        np.float64] and pax_data['pax'] > 0 and type(
        revenue_data['revenue']) in [
        int,
        float,
        np.int64,
        np.float64]:
        avg_fare_ty = revenue_data['revenue'] / pax_data['pax']
        print "avg_fare_ty must be some value = ", avg_fare_ty
    else:
        avg_fare_ty = 'NA'
    if total_pax_ly > 0:
        avg_fare_ly = total_revenue_ly / total_pax_ly
        print "avg_fare_ly must be some value = ", avg_fare_ly
    else:
        avg_fare_ly = 'NA'
    global avg_fare_vlyr
    if type(avg_fare_ly) in [
        int,
        float,
        np.int64,
        np.float64] and avg_fare_ly > 0 and type(avg_fare_ty) in [
        int,
        float,
        np.int64,
        np.float64] and avg_fare_ty > 0:
        avg_fare_vlyr = (avg_fare_ty - avg_fare_ly) * 100 / avg_fare_ly
        print "avg_fare_vlyr must be some value = ", avg_fare_vlyr
    else:
        avg_fare_vlyr = 'NA'
    if type(actual_pax) in [
        int,
        float,
        np.int64,
        np.float64] and actual_pax > 0:
        avg_fare_actual = actual_revenue / actual_pax
        print "avg_fare_actual must be some value = ", avg_fare_actual
    else:
        avg_fare_actual = 'NA'
    if type(target_pax) in [
        int,
        float,
        np.int64,
        np.float64] and target_pax > 0:
        avg_fare_target = target_revenue / target_pax
        print "avg_fare_target must be some value = ", avg_fare_target
    else:
        avg_fare_target = 'NA'
    global avg_fare_vtgt
    if type(avg_fare_actual) in [
        int,
        float,
        np.int64,
        np.float64] and avg_fare_actual > 0 and type(avg_fare_target) in [
        int,
        float,
        np.int64,
        np.float64] and avg_fare_target > 0:
        avg_fare_vtgt = (avg_fare_actual - avg_fare_target) * \
                        100 / avg_fare_target
        print "Avg_fare_vtgt must be some value = ", avg_fare_vtgt
    else:
        avg_fare_vtgt = 'NA'
    avg_fare_data = {
        'avg_fare': avg_fare_ty,
        'vtgt': avg_fare_vtgt,
        'vlyr': avg_fare_vlyr}

    return avg_fare_data


@measure(JUPITER_LOGGER)
def get_yield_data(od_distance):
    """
    :return: {yield_ = 1.45, vtgt: 7.2, vlyr: 6.5}

    """
    if od_distance > 0 and type(avg_fare_ty) in [
        int, float, np.int64, np.float64] and avg_fare_ty > 0:
        yield_ = avg_fare_ty / od_distance
        print "yield_ must be some value = ", yield_
    else:
        yield_ = 'NA'
    yield_vlyr = avg_fare_vlyr
    yield_vtgt = avg_fare_vtgt
    yield_data = {'yield_': yield_, 'vlyr': yield_vlyr, 'vtgt': yield_vtgt}

    return yield_data


@measure(JUPITER_LOGGER)
def get_performance_params(df, capacity, od_distance):
    """
    :return: {'revenue_data' :
                    {'revenue': 782.4, 'vlyr': 7.5, 'vtgt' : 6.8},
              'pax_data' :
                    {'pax': 782.4, 'vlyr': 7.5, 'vtgt' : 6.8},
              'yield_data_compartment' :
                    {'yield_': 782.4, 'vlyr': 7.5, 'vtgt' : 6.8},
              'avg_fare_data' :
                    {'avg_fare': 782.4, 'vlyr': 7.5, 'vtgt' : 6.8}
              }

    """
    response = dict()
    response['revenue_data'] = get_revenue_data(df, capacity)
    response['pax_data'] = get_pax_data(df, capacity)
    response['avg_fare_data'] = get_avg_fare_data(
        pax_data=response['pax_data'],
        revenue_data=response['revenue_data'])
    response['yield_data_compartment'] = get_yield_data(od_distance)
    return response


@measure(JUPITER_LOGGER)
def get_yield(row, key_date_temp, od_distance):
    if row[0][key_date_temp]:
        if row[0][key_date_temp][0]['revenue'] > 0 and row[0][key_date_temp][0]['pax'] > 0 and od_distance > 0:
            return row[0][key_date_temp][0]['revenue'] / \
                   (row[0][key_date_temp][0]['pax'] * od_distance)
        else:
            return 'NA'
    else:
        return 'NA'


@measure(JUPITER_LOGGER)
def get_pax(row, key_date_temp):
    if row[0][key_date_temp]:
        if row[0][key_date_temp][0]['pax']:
            return row[0][key_date_temp][0]['pax']
        else:
            return 'NA'
    else:
        return 'NA'


@measure(JUPITER_LOGGER)
def hit_java_url(response):
    headers = {
        "Connection": "keep-alive",
        "Content-Length": "257",
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.52 Safari/536.5",
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Accept-Encoding": "gzip,deflate",
        "Accept-Language": "q=0.8,en-US",
        "Accept-Charset": "utf-8"}

    if response['dates_code']:
        parameters = {
            "triggerFlag" : "true",
            "combineColumn": response['dates_code'],
            "posMap": {

                "cityArray": [response['pos']]
            },
            "originMap": {
                "cityArray": [response['origin']]
            },
            "destMap": {
                "cityArray": [response['destination']]
            },
            "compMap": {
                "compartmentArray": [response['compartment']]
            }
        }

    else:
        parameters = {
            "triggerFlag":"true",
            "fromDate": response['dep_date_start'],
            "toDate": response['dep_date_end'],
            "posMap": {

                "cityArray": [response['pos']]
            },
            "originMap": {
                "cityArray": [response['origin']]
            },
            "destMap": {
                "cityArray": [response['destination']]
            },
            "compMap": {
                "compartmentArray": [response['compartment']]
            }
        }
    print "sending request..."
    time.sleep(SLEEP_TIME)
    try:
        response_mt = requests.post(url, data=json.dumps(parameters), headers=headers, timeout=100, verify=False)
        print "got response!! ", response_mt.status_code
    except (ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        try:
            response_mt = requests.post(url, data=json.dumps(parameters), headers=headers, timeout=100, verify=False)
            print "got response!! ", response_mt.status_code
        except (ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
            logger = get_jupiter_logger()
            logger.info(json.dumps(parameters))
            raise e
    # if response_mt.text:
    #  response_mt.text['ManualTriggerGrid'] looks like :
    # [{performance params required by python}, {Totals required by UI}]
    #  If response_mt.status_code == 200:
    #       Means -- successfully received data from JAVA URL
    #   If len('ManualTriggerGrid') == 0:
    #       Means There is no data for this particular market in Manual_Triggers_Module Collection
    if response_mt.status_code == 200 and len(json.loads(response_mt.text)['ManualTriggerGrid']) > 0:
        # print "in if statement"
        response_json = json.loads(
            response_mt.text)['ManualTriggerGrid'][0]
        # print "after if"
        response['pax_data'] = dict(
            pax=response_json['pax'],
            vlyr=response_json['paxvlyrperc'],
            vtgt=response_json['paxvtgtperc']
        )
        response['revenue_data'] = dict(
            revenue=response_json['revenue'],
            vlyr=response_json['revenuevlyrperc'],
            vtgt=response_json['revenuevtgtperc']
        )
        response['avg_fare_data'] = dict(
            avg_fare=response_json['avgfare'],
            vlyr=response_json['avgfarevlyr'],
            vtgt=response_json['avgfarevtgt']
        )
        response['yield_data_compartment'] = dict(
            yield_=response_json['yield'],
            vlyr=response_json['yieldvlyrperc'],
            vtgt=response_json['yieldvtgtperc']
        )
        response['seat_factor'] = dict(
            leg1=response_json['bookedsf1'],
            leg2=response_json['bookedsf2'],
            leg1_vlyr=response_json['sf1vlyr'],  # check
            leg2_vlyr=response_json['sf2vlyr']  # check
        )

        return response, response_json

    else:
        print 'Skipped . . .'
        return response, None


@measure(JUPITER_LOGGER)
def main_func(
        pos,
        origin,
        destination,
        compartment,
        dep_date_start,
        dep_date_end,
        dates_list,
        db,
        dates_code=None,
        host_fares=None,
        is_manual=0):
    """
    :Functionality:
            Inserts into Workflow_OD_User - Market_Data, Performance_data, Fares_data for all the dates in dates_list

    """
    # dates_list = [{'start': '2017-04-27', 'end': '2017-05-04'}, {'start': '2017-04-27', 'end': '2017-05-11'}, {'start': '2017-04-27', 'end': '2017-05-18'}, {'start': '2017-04-27', 'end': '2017-05-27'}, {'start': '2017-04-27', 'end': '2017-06-11'}, {'start': '2017-04-27', 'end': '2017-06-26'}, {'start': '2017-04-27', 'end': '2017-07-26'}, {'start': '2017-04-01', 'end': '2017-04-30'}, {'start': '2017-05-01', 'end': '2017-05-31'}, {'start': '2017-06-01', 'end': '2017-06-30'}])
    # Apparently dep_date_start and dep_date_end is not required
    dates_list_final = deepcopy(dates_list)
    if is_manual == 2:
        extreme_start_date = min([date['start']
                                  for date in dates_list_final])
        extreme_end_date = max([date['end'] for date in dates_list_final])
        print "Extreme start date = ", extreme_start_date, " Extreme end date = ", extreme_end_date
        st = time.time()
        print "Market and extreme dates = ", pos, origin, destination, compartment, extreme_start_date, extreme_end_date
        print "Mapping fare_infare . . . . . . . . . . . ."
        # map_fare_infare(origin=origin,
        #                 destination=destination,
        #                 compartment=compartment,
        #                 dep_date_start=extreme_start_date,
        #                 dep_date_end=extreme_end_date,
        #                 )
        print "Time taken to map fare_infare for extreme_range: ", time.time() - st
        st2 = time.time()
        fares_data = get_fares_data(pos=pos,
                                    origin=origin,
                                    destination=destination,
                                    compartment=compartment,
                                    dep_date_start=extreme_start_date,
                                    dep_date_end=extreme_end_date,
                                    db=db
                                    )
        print "time taken to get fares data and sales data from Oligopoly_fl_gen from extreme start date to extreme end date = ", time.time() - st2
        fares_data_df_complete = pd.DataFrame(fares_data)

        extreme_start_date_ly = datetime.datetime.strftime(datetime.datetime.strptime(
            extreme_start_date, "%Y-%m-%d") - datetime.timedelta(days=365), "%Y-%m-%d")
        extreme_end_date_ly = datetime.datetime.strftime(datetime.datetime.strptime(
            extreme_end_date, "%Y-%m-%d") - datetime.timedelta(days=365), "%Y-%m-%d")
        if len(fares_data_df_complete) > 0:
            st = time.time()
            od_distance = get_od_distance(od=origin + destination, db=db)
            print "time taken to get OD Distance = ", time.time() - st, " and distance = ", od_distance
            st = time.time()
            performance_data_complete = get_performance_data(
                pos=pos,
                origin=origin,
                destination=destination,
                compartment=compartment,
                extreme_start_date=extreme_start_date,
                extreme_end_date=extreme_end_date, db=db)
            print "Time taken to get complete performance data from extreme start to extreme end = ", time.time() - st
            st = time.time()
            market_data_complete = get_mrkt_params(
                pos=pos,
                origin=origin,
                destination=destination,
                compartment=compartment,
                dep_date_start=extreme_start_date,
                dep_date_end=extreme_end_date,
                db=db,
                date_list=dates_list_final)
            print "Time taken to get complete market and competitor data from extreme start date to extreme end date = ", time.time() - st
            leg_flag = False
            st = time.time()
            if origin == hub or destination == hub:
                leg_flag = True
                bookings_df_ty = get_bookings_df(
                    origin=origin,
                    destination=destination,
                    compartment=compartment,
                    dep_date_start=extreme_start_date,
                    dep_date_end=extreme_end_date,
                    db=db)
                bookings_df_ly = get_bookings_df(
                    origin=origin,
                    destination=destination,
                    compartment=compartment,
                    dep_date_start=extreme_start_date_ly,
                    dep_date_end=extreme_end_date_ly,
                    db=db)
                print "Got entire bookings df ty and ly for OD in %s seconds" % (time.time() - st)
            else:
                origin1 = origin
                destination1 = hub
                origin2 = hub
                destination2 = destination
                bookings_df_ty_leg1 = get_bookings_df(
                    origin=origin1,
                    destination=destination1,
                    compartment=compartment,
                    dep_date_start=extreme_start_date,
                    dep_date_end=extreme_end_date,
                    db=db)
                bookings_df_ly_leg1 = get_bookings_df(
                    origin=origin1,
                    destination=destination1,
                    compartment=compartment,
                    dep_date_start=extreme_start_date_ly,
                    dep_date_end=extreme_end_date_ly,
                    db=db)
                bookings_df_ty_leg2 = get_bookings_df(
                    origin=origin2,
                    destination=destination2,
                    compartment=compartment,
                    dep_date_start=extreme_start_date,
                    dep_date_end=extreme_end_date,
                    db=db)
                bookings_df_ly_leg2 = get_bookings_df(
                    origin=origin2,
                    destination=destination2,
                    compartment=compartment,
                    dep_date_start=extreme_start_date_ly,
                    dep_date_end=extreme_end_date_ly,
                    db=db)
                time_taken = time.time() - st
                print "Got entire bookings df for ty and ly for both legs in %s seconds" % time_taken

            i = 1
            for date_range in dates_list_final:
                st_i = time.time()
                temp_response = {
                    'pos': pos,
                    'origin': origin,
                    'destination': destination,
                    'od': origin + destination,
                    'compartment': compartment,
                    'dep_date_start': dep_date_start,
                    'dep_date_end': dep_date_end,
                    'update_date': SYSTEM_DATE
                }

                dep_date_start = date_range['start']
                dep_date_end = date_range['end']

                temp_fares_df = fares_data_df_complete[(fares_data_df_complete['travel_date_from'] <= dep_date_end) & (
                        fares_data_df_complete['travel_date_to'] >= dep_date_start)]
                # print "temp_fares_df = ", temp_fares_df
                temp_fares_df['dep_date_from'] = dep_date_start
                temp_fares_df['dep_date_to'] = dep_date_end
                key_date_temp = 'date' + str(i)
                # print "------> temp_fares_df['sales_data'] = ",
                # temp_fares_df['sales_data']
                st2 = time.time()
                temp_fares_df['fare_pax'] = temp_fares_df['sales_data'].apply(
                    lambda row: get_pax(row, key_date_temp))
                temp_fares_df['yield'] = temp_fares_df['sales_data'].apply(
                    lambda row: get_yield(row, key_date_temp, od_distance))
                print "time taken to calculate current yield for dates ", dep_date_start, " to ", dep_date_end, " = ", time.time() - st2
                # print "Now temp_fares_df = ", temp_fares_df
                temp_fares_df.drop('sales_data', axis=1, inplace=True)
                fares_data_temp = temp_fares_df.to_dict('records')

                dep_date_start_ly = datetime.datetime.strftime(datetime.datetime.strptime(
                    dep_date_start, "%Y-%m-%d") - datetime.timedelta(days=365), "%Y-%m-%d")
                dep_date_end_ly = datetime.datetime.strftime(datetime.datetime.strptime(
                    dep_date_end, "%Y-%m-%d") - datetime.timedelta(days=365), "%Y-%m-%d")
                st3 = time.time()
                temp_performance_df = performance_data_complete[
                    (performance_data_complete['dep_date'] >= dep_date_start) & (
                            performance_data_complete['dep_date'] <= dep_date_end)]
                print "time taken to calculate temp Performance df from complete perforance df = ", time.time() - st3
                # print "Temp_performance_df = ", temp_performance_df
                st4 = time.time()
                capacity = get_od_capacity(
                    origin=origin,
                    destination=destination,
                    compartment=compartment,
                    dep_date_start=dep_date_start,
                    dep_date_end=dep_date_end,
                    db=db)
                print "Time taken to get od capacity = ", time.time() - st4, " and od capacity = ", capacity
                st5 = time.time()
                performance_data_temp = get_performance_params(
                    temp_performance_df, capacity, od_distance)
                print "Time taken to get performance params for this dates = ", time.time() - st5, "dates  = ", dep_date_start, " to ", dep_date_end
                st = time.time()
                if leg_flag:
                    temp_bookings_ty_df = bookings_df_ty[(bookings_df_ty['dep_date'] <= dep_date_end) & (
                            bookings_df_ty['dep_date'] >= dep_date_start)]
                    temp_bookings_ly_df = bookings_df_ly[(bookings_df_ly['dep_date'] <= dep_date_end_ly) & (
                            bookings_df_ly['dep_date'] >= dep_date_start_ly)]
                    if compartment == 'Y':
                        bookings_ty = temp_bookings_ty_df['y_booking'].sum(
                        )
                        print "In bookings ty, sum of y_booking = ", bookings_ty
                        capacity_ty = temp_bookings_ty_df['y_capacity'].sum(
                        )
                        print "In bookings ty, sum of y_capacity = ", capacity_ty
                        if capacity_ty > 0:
                            print "going to calculate leg1 sf ty"
                            leg1_sf_ty = bookings_ty * 100.0 / capacity_ty
                            print "leg1_sf_ty  = ", leg1_sf_ty
                        else:
                            leg1_sf_ty = 'NA'

                        bookings_ly = temp_bookings_ly_df['y_booking'].sum(
                        )
                        print "bookings_ly  = ", bookings_ly
                        capacity_ly = temp_bookings_ly_df['y_capacity'].sum(
                        )
                        print "Capacity_ly  =  ", capacity_ly
                        if capacity_ly > 0:
                            leg1_sf_ly = bookings_ly * 100.0 / capacity_ly
                            print "leg1_df_ly = ", leg1_sf_ly
                        else:
                            leg1_sf_ly = 'NA'
                        if type(leg1_sf_ly) in [
                            int, float, np.int64, np.float64] and type(leg1_sf_ty) in [
                            int, float, np.int64, np.float64] and leg1_sf_ly > 0:
                            leg1_sf_vlyr = (
                                                   leg1_sf_ty - leg1_sf_ly) * 100.0 / leg1_sf_ly
                            print "leg1_sf_vlyr  =  ", leg1_sf_vlyr
                        else:
                            leg1_sf_vlyr = 'NA'

                    elif compartment == 'J':
                        bookings_ty = temp_bookings_ty_df['j_booking'].sum(
                        )
                        capacity_ty = temp_bookings_ty_df['j_capacity'].sum(
                        )
                        if capacity_ty > 0:
                            leg1_sf_ty = bookings_ty * 100.0 / capacity_ty
                        else:
                            leg1_sf_ty = 'NA'

                        bookings_ly = temp_bookings_ly_df['j_booking'].sum(
                        )
                        capacity_ly = temp_bookings_ly_df['j_capacity'].sum(
                        )
                        if capacity_ly > 0:
                            leg1_sf_ly = bookings_ly * 100.0 / capacity_ly
                        else:
                            leg1_sf_ly = 'NA'
                        if type(leg1_sf_ly) in [
                            int, float, np.int64, np.float64] and type(leg1_sf_ty) in [
                            int, float, np.int64, np.float64] and leg1_sf_ly > 0:
                            leg1_sf_vlyr = (
                                                   leg1_sf_ty - leg1_sf_ly) * 100.0 / leg1_sf_ly
                        else:
                            leg1_sf_vlyr = 'NA'

                    else:
                        bookings_ty = temp_bookings_ty_df['total_booking'].sum(
                        )
                        capacity_ty = temp_bookings_ty_df['total_capacity'].sum(
                        )
                        if capacity_ty > 0:
                            leg1_sf_ty = bookings_ty * 100.0 / capacity_ty
                        else:
                            leg1_sf_ty = 'NA'

                        bookings_ly = temp_bookings_ly_df['total_booking'].sum(
                        )
                        capacity_ly = temp_bookings_ly_df['total_capacity'].sum(
                        )
                        if capacity_ly > 0:
                            leg1_sf_ly = bookings_ly * 100.0 / capacity_ly
                        else:
                            leg1_sf_ly = 'NA'
                        if type(leg1_sf_ly) in [
                            int, float, np.int64, np.float64] and type(leg1_sf_ty) in [
                            int, float, np.int64, np.float64] and leg1_sf_ly > 0:
                            leg1_sf_vlyr = (
                                                   leg1_sf_ty - leg1_sf_ly) * 100.0 / leg1_sf_ly
                        else:
                            leg1_sf_vlyr = 'NA'

                    leg2_sf = 'NA'
                    leg2_sf_vlyr = 'NA'
                    seat_factor_data = {
                        'leg1': leg1_sf_ty,
                        'leg1_vlyr': leg1_sf_vlyr,
                        'leg2': leg2_sf,
                        'leg2_vlyr': leg2_sf_vlyr}

                else:
                    temp_bookings_ty_df_leg1 = bookings_df_ty_leg1[(bookings_df_ty_leg1['dep_date'] <= dep_date_end) & (
                            bookings_df_ty_leg1['dep_date'] >= dep_date_start)]
                    temp_bookings_ly_df_leg1 = bookings_df_ly_leg1[
                        (bookings_df_ly_leg1['dep_date'] <= dep_date_end_ly) & (
                                bookings_df_ly_leg1['dep_date'] >= dep_date_start_ly)]
                    temp_bookings_ty_df_leg2 = bookings_df_ty_leg2[(bookings_df_ty_leg2['dep_date'] <= dep_date_end) & (
                            bookings_df_ty_leg2['dep_date'] >= dep_date_start)]
                    temp_bookings_ly_df_leg2 = bookings_df_ly_leg2[
                        (bookings_df_ly_leg2['dep_date'] <= dep_date_end_ly) & (
                                bookings_df_ly_leg2['dep_date'] >= dep_date_start_ly)]

                    if compartment == 'Y':
                        bookings_ty_leg1 = temp_bookings_ty_df_leg1['y_booking'].sum(
                        )
                        capacity_ty_leg1 = temp_bookings_ty_df_leg1['y_capacity'].sum(
                        )
                        if capacity_ty_leg1 > 0:
                            leg1_sf_ty = bookings_ty_leg1 * 100.0 / capacity_ty_leg1
                        else:
                            leg1_sf_ty = 'NA'

                        bookings_ly_leg1 = temp_bookings_ly_df_leg1['y_booking'].sum(
                        )
                        capacity_ly_leg1 = temp_bookings_ly_df_leg1['y_capacity'].sum(
                        )
                        if capacity_ly_leg1 > 0:
                            leg1_sf_ly = bookings_ly_leg1 * 100.0 / capacity_ly_leg1
                        else:
                            leg1_sf_ly = 'NA'

                        if type(leg1_sf_ly) in [
                            int, float, np.int64, np.float64] and type(leg1_sf_ty) in [
                            int, float, np.int64, np.float64] and leg1_sf_ly > 0:
                            leg1_sf_vlyr = (
                                                   leg1_sf_ty - leg1_sf_ly) * 100.0 / leg1_sf_ly
                        else:
                            leg1_sf_vlyr = 'NA'

                        bookings_ty_leg2 = temp_bookings_ty_df_leg2['y_booking'].sum(
                        )
                        capacity_ty_leg2 = temp_bookings_ty_df_leg2['y_capacity'].sum(
                        )
                        if capacity_ty_leg2 > 0:
                            leg2_sf_ty = bookings_ty_leg2 * 100.0 / capacity_ty_leg2
                        else:
                            leg2_sf_ty = 'NA'

                        bookings_ly_leg2 = temp_bookings_ly_df_leg2['y_booking'].sum(
                        )
                        capacity_ly_leg2 = temp_bookings_ly_df_leg2['y_capacity'].sum(
                        )
                        if capacity_ly_leg2 > 0:
                            leg2_sf_ly = bookings_ly_leg2 * 100.0 / capacity_ly_leg2
                        else:
                            leg2_sf_ly = 'NA'

                        if type(leg2_sf_ly) in [
                            int, float, np.int64, np.float64] and type(leg2_sf_ty) in [
                            int, float, np.int64, np.float64] and leg2_sf_ly > 0:
                            leg2_sf_vlyr = (
                                                   leg2_sf_ty - leg2_sf_ly) * 100.0 / leg2_sf_ly
                        else:
                            leg2_sf_vlyr = 'NA'

                    elif compartment == 'J':
                        bookings_ty_leg1 = temp_bookings_ty_df_leg1['j_booking'].sum(
                        )
                        capacity_ty_leg1 = temp_bookings_ty_df_leg1['j_capacity'].sum(
                        )
                        if capacity_ty_leg1 > 0:
                            leg1_sf_ty = bookings_ty_leg1 * 100.0 / capacity_ty_leg1
                        else:
                            leg1_sf_ty = 'NA'

                        bookings_ly_leg1 = temp_bookings_ly_df_leg1['j_booking'].sum(
                        )
                        capacity_ly_leg1 = temp_bookings_ly_df_leg1['j_capacity'].sum(
                        )
                        if capacity_ly_leg1 > 0:
                            leg1_sf_ly = bookings_ly_leg1 * 100.0 / capacity_ly_leg1
                        else:
                            leg1_sf_ly = 'NA'
                        if type(leg1_sf_ly) in [
                            int, float, np.int64, np.float64] and type(leg1_sf_ty) in [
                            int, float, np.int64, np.float64] and leg1_sf_ly > 0:
                            leg1_sf_vlyr = (
                                                   leg1_sf_ty - leg1_sf_ly) * 100.0 / leg1_sf_ly
                        else:
                            leg1_sf_vlyr = 'NA'

                        bookings_ty_leg2 = temp_bookings_ty_df_leg2['j_booking'].sum(
                        )
                        capacity_ty_leg2 = temp_bookings_ty_df_leg2['j_capacity'].sum(
                        )
                        if capacity_ty_leg2 > 0:
                            leg2_sf_ty = bookings_ty_leg2 * 100.0 / capacity_ty_leg2
                        else:
                            leg2_sf_ty = 'NA'

                        bookings_ly_leg2 = temp_bookings_ly_df_leg2['j_booking'].sum(
                        )
                        capacity_ly_leg2 = temp_bookings_ly_df_leg2['j_capacity'].sum(
                        )
                        if capacity_ly_leg2 > 0:
                            leg2_sf_ly = bookings_ly_leg2 * 100.0 / capacity_ly_leg2
                        else:
                            leg2_sf_ly = 'NA'
                        if type(leg2_sf_ly) in [
                            int, float, np.int64, np.float64] and type(leg2_sf_ty) in [
                            int, float, np.int64, np.float64] and leg2_sf_ly > 0:
                            leg2_sf_vlyr = (
                                                   leg2_sf_ty - leg2_sf_ly) * 100.0 / leg2_sf_ly
                        else:
                            leg2_sf_vlyr = 'NA'

                    else:
                        bookings_ty_leg1 = temp_bookings_ty_df_leg1['total_booking'].sum(
                        )
                        capacity_ty_leg1 = temp_bookings_ty_df_leg1['total_capacity'].sum(
                        )
                        if capacity_ty_leg1 > 0:
                            leg1_sf_ty = bookings_ty_leg1 * 100.0 / capacity_ty_leg1
                        else:
                            leg1_sf_ty = 'NA'

                        bookings_ly_leg1 = temp_bookings_ly_df_leg1['total_booking'].sum(
                        )
                        capacity_ly_leg1 = temp_bookings_ly_df_leg1['total_capacity'].sum(
                        )
                        if capacity_ly_leg1 > 0:
                            leg1_sf_ly = bookings_ly_leg1 * 100.0 / capacity_ly_leg1
                        else:
                            leg1_sf_ly = 'NA'
                        if type(leg1_sf_ly) in [
                            int, float, np.int64, np.float64] and type(leg1_sf_ty) in [
                            int, float, np.int64, np.float64] and leg1_sf_ly > 0:
                            leg1_sf_vlyr = (
                                                   leg1_sf_ty - leg1_sf_ly) * 100.0 / leg1_sf_ly
                        else:
                            leg1_sf_vlyr = 'NA'

                        bookings_ty_leg2 = temp_bookings_ty_df_leg2['total_booking'].sum(
                        )
                        capacity_ty_leg2 = temp_bookings_ty_df_leg2['total_capacity'].sum(
                        )
                        if capacity_ty_leg2 > 0:
                            leg2_sf_ty = bookings_ty_leg2 * 100.0 / capacity_ty_leg2
                        else:
                            leg2_sf_ty = 'NA'

                        bookings_ly_leg2 = temp_bookings_ly_df_leg2['total_booking'].sum(
                        )
                        capacity_ly_leg2 = temp_bookings_ly_df_leg2['total_capacity'].sum(
                        )
                        if capacity_ly_leg2 > 0:
                            leg2_sf_ly = bookings_ly_leg2 * 100.0 / capacity_ly_leg2
                        else:
                            leg2_sf_ly = 'NA'
                        if type(leg2_sf_ly) in [
                            int, float, np.int64, np.float64] and type(leg2_sf_ty) in [
                            int, float, np.int64, np.float64] and leg2_sf_ly > 0:
                            leg2_sf_vlyr = (
                                                   leg2_sf_ty - leg2_sf_ly) * 100.0 / leg2_sf_ly
                        else:
                            leg2_sf_vlyr = 'NA'

                    seat_factor_data = {
                        'leg1': leg1_sf_ty,
                        'leg1_vlyr': leg1_sf_vlyr,
                        'leg2': leg2_sf_ty,
                        'leg2_vlyr': leg2_sf_vlyr}
                print "Time taken to calculate seat factor data for these dataes ", dep_date_start, dep_date_end, " = ", time.time() - st
                performance_data_temp['seat_factor'] = seat_factor_data
                print "performance_data_temp = ", performance_data_temp
                temp_response.update(performance_data_temp)
                temp_response['fares_docs'] = fares_data_temp
                print "Time taken to calculate entire response for these dates ", dep_date_start, dep_date_end, " = ", time.time() - st
                # Get market_data_temp from complete_market_data for
                # dep_date_start and dep_date_end and convert to dict.
                market_data_temp = market_data_complete[i - 1]
                temp_response['mrkt_data'] = market_data_temp
                print "Seat factor data = ", seat_factor_data
                # db.JUP_DB_Workflow_OD_User.insert(temp_response)
                i += 1
                print "Total time taken to update Workflow_OD_User for these dates - ", dep_date_start, " to ", dep_date_end, " = ", time.time() - st_i

            print 'WORKFLOW UPDATE DONE \n'
    # ////////////////////////////////////////////////////
    else:
        # SYSTEM_DATE = datetime.datetime.now().strftime('%Y-%m-%d')
        st = time.time()
        currency_data = get_currency_data(db=db)
        currency_list = []
        if currency_data[pos]['web']:
            currency_list.append(currency_data[pos]['web'])
        if currency_data[pos]['gds']:
            currency_list.append(currency_data[pos]['gds'])
        show_flag = 3
        is_significant = False
        if pos == origin:
            show_flag = 1
        elif pos == destination:
            show_flag = 2
        significant_crsr = list(db.JUP_DB_Target_OD.find(
            {"pos": pos, "origin": origin, "destination": destination, "month": int(SYSTEM_DATE[5:7]),
             "year": int(SYSTEM_DATE[0:4]), 'pax': {'$gte': 100}}, {"significant_target": 1}).limit(1))
        if len(significant_crsr) == 1:
            is_significant = significant_crsr[0]['significant_target']

        response_ = dict(
            pos=pos,
            origin=origin,
            destination=destination,
            od=origin + destination,
            compartment=compartment,
            dep_date_start=dep_date_start,
            dep_date_end=dep_date_end,
            dates_code=dates_code,
            update_date=SYSTEM_DATE,
            show_flag=show_flag,
            is_significant=is_significant
        )
        # print "currency_list = ", currency_list

        response, response_json = hit_java_url(response_)

        # headers = {
        #     "Connection": "keep-alive",
        #     "Content-Length": "257",
        #     "X-Requested-With": "XMLHttpRequest",
        #     "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.52 Safari/536.5",
        #     "Content-Type": "application/json",
        #     "Accept": "*/*",
        #     "Accept-Encoding": "gzip,deflate",
        #     "Accept-Language": "q=0.8,en-US",
        #     "Accept-Charset": "utf-8"}
        #
        # parameters = {
        #     "fromDate": dep_date_start,
        #     "toDate": dep_date_end,
        #     "posMap": {
        #
        #         "cityArray": [pos]
        #     },
        #     "originMap": {
        #         "cityArray": [origin]
        #     },
        #     "destMap": {
        #         "cityArray": [destination]
        #     },
        #     "compMap": {
        #         "compartmentArray": [compartment]
        #     }
        #     ,
        #     "arcFlag": "web"
        # }
        # print "sending request..."
        # response_mt = requests.post(
        #     url, data=json.dumps(parameters), headers=headers)
        # print "got response!! ", response_mt.status_code
        # # if response_mt.text:
        # #  response_mt.text['ManualTriggerGrid'] looks like :
        # # [{performance params required by python}, {Totals required by UI}]
        # #  If response_mt.status_code == 200:
        # #       Means -- successfully received data from JAVA URL
        # #   If len('ManualTriggerGrid') == 0:
        # #       Means There is no data for this particular market in Manual_Triggers_Module Collection
        # if response_mt.status_code == 200 and len(json.loads(response_mt.text)['ManualTriggerGrid']) > 0:
        #     # print "in if statement"
        #     response_json = json.loads(
        #         response_mt.text)['ManualTriggerGrid'][0]
        #     # print "after if"
        #     response['pax_data'] = dict(
        #         pax=response_json['pax'],
        #         vlyr=response_json['paxvlyrperc'],
        #         vtgt=response_json['paxvtgtperc']
        #     )
        #     response['revenue_data'] = dict(
        #         revenue=response_json['revenue'],
        #         vlyr=response_json['revenuevlyrperc'],
        #         vtgt=response_json['revenuevtgtperc']
        #     )
        #     response['avg_fare_data'] = dict(
        #         avg_fare=response_json['avgfare'],
        #         vlyr=response_json['avgfarevlyr'],
        #         vtgt=response_json['avgfarevtgt']
        #     )
        #     response['yield_data_compartment'] = dict(
        #         yield_=response_json['yield'],
        #         vlyr=response_json['yieldvlyrperc'],
        #         vtgt=response_json['yieldvtgtperc']
        #     )
        #     response['seat_factor'] = dict(
        #         leg1=response_json['bookedsf1'],
        #         leg2=response_json['bookedsf2'],
        #         leg1_vlyr=response_json['sf1vlyr'],  # check
        #         leg2_vtgt=response_json['sf2vlyr']  # check
        #     )

        if response_json:
            try:
                response['currency_web'] = currency_list[0]
            except KeyError:
                response['currency_web'] = "NA"
            # print "response['currency_web'] = ", response['currency_web']
            try:
                if len(currency_list) > 1:
                    response['currency_gds'] = currency_list[1]
                else:
                    response['currency_gds'] = "NA"
            except KeyError:
                response['currency_gds'] = "NA"

            host_data, comp_data, mpf_df_for_web_pricing = build_market_data_from_java_response(
                response_json, dep_date_start, dep_date_end, db=db, dates_code=response['dates_code'])

            response['mrkt_data'] = {"host": host_data, "comp": comp_data}
            #            map_fare_infare(origin=origin,
            #                            destination=destination,
            #                            compartment=compartment,
            #                            dep_date_start=dep_date_start,
            #                            dep_date_end=dep_date_end,
            #                            )
            print "time taken to get mrkt_data for host and competitors = ", time.time() - st
            st = time.time()
            fares_data = get_fares_data(pos=pos,
                                        origin=origin,
                                        destination=destination,
                                        compartment=compartment,
                                        dep_date_start=dep_date_start,
                                        dep_date_end=dep_date_end,
                                        db=db,
                                        mpf_df=mpf_df_for_web_pricing,
                                        host_fares=host_fares,
                                        dates_code=dates_code
                                        )
            response['fares_docs'] = fares_data
            print "time taken for recommendation = ", time.time() - st
            crsr_ct = db.JUP_DB_Workflow_OD_User.find({
                'pos': pos,
                'origin': origin,
                'destination': destination,
                'compartment': compartment,
                'dep_date_start': dep_date_start,
                'dep_date_end': dep_date_end,
                'update_date': SYSTEM_DATE
            }).count()
            if crsr_ct == 0:
                db.JUP_DB_Workflow_OD_User.insert(json.loads(json.dumps(response, indent=1, cls=MyEncoder)))
                print "Inserted into WORKFLOW_OD_USER ! "


class MyEncoder(json.JSONEncoder):
    @measure(JUPITER_LOGGER)
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(MyEncoder, self).default(obj)


@measure(JUPITER_LOGGER)
def get_month_year_query(dep_date_start, dep_date_end):
    start_obj = datetime.datetime.strptime(dep_date_start, "%Y-%m-%d")
    end_obj = datetime.datetime.strptime(dep_date_end, "%Y-%m-%d")
    query = []
    while start_obj <= end_obj:
        print start_obj
        query.append({
            "month": start_obj.month,
            "year": start_obj.year
        })
        # query.append({
        #     "month": start_obj.month,
        #     "year": start_obj.year - 1
        # })
        start_obj = start_obj + relativedelta(months=1)
    return query


@measure(JUPITER_LOGGER)
def build_market_data_from_java_response(
        json_response, dep_date_start, dep_date_end, db, dates_code= None):
    comp_data = []
    host_data = {}
    pos = json_response['pos']
    origin = json_response['origin']
    destination = json_response['destination']
    compartment = json_response['compartment']
    month_year_query = get_month_year_query(dep_date_start, dep_date_end)
    # top5_comp = obtain_top_5_comp([pos], [origin], [destination], [compartment], dep_date_start, dep_date_end,
    #                               date_list=[{"start": dep_date_start,
    #                                           "end": dep_date_end}])
    # comp = top5_comp
    # print "comp:   ", comp
    comp = []
    temp_ = [Host_Airline_Code]
    for i in range(1, 5):
        temp_.append(json_response['comp' + str(i) + 'carrier'])
    ratings_crsr = list(db.JUP_DB_Rating_ALL.find({"Airline": {"$in": temp_}}))
    pax_crsr = list(db.JUP_DB_Pos_OD_Compartment_new.aggregate([
        {
            "$match": {"pos": pos,
                       "od": origin + destination,
                       "compartment": compartment,
                       "$or": month_year_query
                       }
        }, {
            "$unwind": {
                "path": "$top_5_comp_data"
            }
        }, {
            "$match": {
                "top_5_comp_data.airline": {
                    "$in": temp_
                }
            }
        }, {
            "$project": {
                "_id": 0,
                "top_5_comp_data": 1,
                "month": 1,
                "year": 1
            }
        }
    ]))
    # cap_crsr = list(db.JUP_DB_OD_Capacity.aggregate(
    #     [{"$match": {"od": origin + destination, "compartment": compartment, "$or": month_year_query, "airline": {"$in": temp_}}},
    #      {"$project": {"_id": 0, "airline": 1, "od_capacity": 1, "month": 1, "year": 1}}]))
    pax_dict_ty = {}
    pax_dict_ly = {}
    cap_ty = {}
    cap_ly = {}
    market_size_ty = {}
    market_size_ly = {}

    for comp_ in temp_:
        pax_dict_ly[comp_] = 0
        pax_dict_ty[comp_] = 0
        cap_ty[comp_] = 0
        cap_ly[comp_] = 0
        market_size_ty[comp_] = 0
        market_size_ly[comp_] = 0

    for record in pax_crsr:
        for comp_ in temp_:
            if (comp_ == record['top_5_comp_data']['airline']) and (comp_ != Host_Airline_Code):
                if str(record['top_5_comp_data']['pax']) != 'nan':
                    pax_dict_ty[comp_] = pax_dict_ty[comp_] + record['top_5_comp_data']['pax']
                else:
                    pax_dict_ty[comp_] = pax_dict_ty[comp_] + 0
                if str(record['top_5_comp_data']['pax_1']) != 'nan':
                    pax_dict_ly[comp_] = pax_dict_ly[comp_] + record['top_5_comp_data']['pax_1']
                else:
                    pax_dict_ly[comp_] = pax_dict_ly[comp_] + 0
                if str(record['top_5_comp_data']['market_size']) != 'nan':
                    market_size_ty[comp_] = market_size_ty[comp_] + record['top_5_comp_data']['market_size']
                else:
                    market_size_ty[comp_] = market_size_ty[comp_] + 0
                if str(record['top_5_comp_data']['market_size_1']) != 'nan':
                    market_size_ly[comp_] = market_size_ly[comp_] + record['top_5_comp_data']['market_size_1']
                else:
                    market_size_ly[comp_] = market_size_ly[comp_] + 0
                if str(record['top_5_comp_data']['capacity']) != 'nan':
                    cap_ty[comp_] = cap_ty[comp_] + record['top_5_comp_data']['capacity']
                else:
                    cap_ty[comp_] = cap_ty[comp_] + 0
                if str(record['top_5_comp_data']['capacity_1']) != 'nan':
                    cap_ly[comp_] = cap_ly[comp_] + record['top_5_comp_data']['capacity_1']
                else:
                    cap_ly[comp_] = cap_ly[comp_] + 0

    # for record in cap_crsr:
    #     if record['airline'] != "FZ":
    #         if record['year'] == int(SYSTEM_DATE[0:4]):
    #             cap_ty[record['airline']] = cap_ty[record['airline']] + record['od_capacity']
    #         else:
    #             cap_ly[record['airline']] = cap_ly[record['airline']] + record['od_capacity']

    ratings_df = pd.DataFrame(ratings_crsr)
    comp.append((temp_, []))
    host_data['market_share_vlyr'] = json_response['marketsharevlyr']
    host_data['market_share'] = json_response['marketshare']
    host_data['rating'] = ratings_df.loc[(
                                                 ratings_df['Airline'] == Host_Airline_Code), 'overall_rating'].values[0]
    host_data['pax'] = json_response['pax']  # check
    host_data['distributor_rating'] = ratings_df.loc[(
                                                             ratings_df[
                                                                 'Airline'] == Host_Airline_Code), 'distributor_rating'].values[0]
    host_data['fms'] = json_response['fairmarketshare']
    host_data['market_rating'] = ratings_df.loc[(
                                                        ratings_df['Airline'] == Host_Airline_Code), 'market_rating'].values[0]
    host_data['capacity_rating'] = ratings_df.loc[(
                                                          ratings_df['Airline'] == Host_Airline_Code), 'capacity_rating'].values[0]
    host_data['market_share_vtgt'] = json_response['marketsharevtgt']
    host_data['product_rating'] = ratings_df.loc[(
                                                         ratings_df['Airline'] == Host_Airline_Code), 'product_rating'].values[0]
    host_data['pax_vlyr'] = json_response['paxvlyrperc']
    host_data['fare_rating'] = ratings_df.loc[(
                                                      ratings_df['Airline'] == Host_Airline_Code), 'fare_rating'].values[0]
    print "Received Host Performance for Competitor Summary from JAVA"
    print "Getting Lowest filed fare . . ."

    try:
        if len(json_response['lffList']) > 0:
            lowest_filed_fare = pd.DataFrame(json_response['lffList'])
            lowest_filed_fare.rename(columns={
                "dateEnd": "date_end",
                "lowestFareFb_rt": "lowest_fare_fb_rt",
                "currency": "currency",
                "lowestFareYQgds": "lowest_fare_YQ_gds",
                "lowestFareTotalGds_rt": "lowest_fare_total_rt_gds",
                "lowestFareBase": "lowest_fare_base",
                "lowestFareSurcharge": "lowest_fare_surcharge",
                "lowestFareBaseGds": "lowest_fare_base_gds",
                "dateStart": "date_start",
                "lowestFareTotalGds": "lowest_fare_total_gds",
                "lowestFareYQ": "lowest_fare_YQ",
                "lowestFareSurchargeGds": "lowest_fare_surcharge_gds",
                "carrier": "carrier",
                "lowestFareBaseGds_rt": "lowest_fare_base_rt_gds",
                "lowestFareYRGds": "lowest_fare_YR_gds",
                "lowestFareTotal": "lowest_fare_total",
                "lowestFareSurchargegds_rt": "lowest_fare_surcharge_rt_gds",
                "lowestFareBase_rt": "lowest_fare_base_rt",
                "lowestFareYR": "lowest_fare_YR",
                "lowestFareYRGds_rt": "lowest_fare_YR_rt_gds",
                "currencyGds": "currency_gds",
                "lowestFareFb": "lowest_fare_fb",
                "lowestFareTotal_rt": "lowest_fare_total_rt",
                "lowestFareYQgds_rt": "lowest_fare_YQ_rt_gds",
                "lowestFareFbGds_rt": "lowest_fare_fb_rt_gds",
                "lowestFareYQ_rt": "lowest_fare_YQ_rt",
                "lowestFareFbGds": "lowest_fare_fb_gds",
                "lowestFareYR_rt": "lowest_fare_YR_rt",
                "lowestFareSurcharge_rt": "lowest_fare_surcharge_rt"
            }, inplace=True)
        else:
            lowest_filed_fare = pd.DataFrame(
                columns=['carrier', 'currency', 'lowest_fare_YQ', 'lowest_fare_YQ_rt',
                         'lowest_fare_YR', 'lowest_fare_YR_rt', 'lowest_fare_base',
                         'lowest_fare_base_rt', 'lowest_fare_fb', 'lowest_fare_fb_rt',
                         'lowest_fare_surcharge', 'lowest_fare_surcharge_rt',
                         'lowest_fare_tax', 'lowest_fare_tax_rt', 'lowest_fare_total',
                         'lowest_fare_total_rt', 'date_start', 'date_end', 'currency_gds',
                         'lowest_fare_YQ_gds', 'lowest_fare_YR_gds',
                         'lowest_fare_base_gds', 'lowest_fare_fb_gds',
                         'lowest_fare_surcharge_gds', 'lowest_fare_tax_gds',
                         'lowest_fare_total_gds',
                         'lowest_fare_YQ_rt_gds', 'lowest_fare_YR_rt_gds',
                         'lowest_fare_base_rt_gds', 'lowest_fare_fb_rt_gds',
                         'lowest_fare_surcharge_rt_gds', 'lowest_fare_tax_rt_gds',
                         'lowest_fare_total_rt_gds']
            )
    except Exception:
        lowest_filed_fare = pd.DataFrame(
            columns=['carrier', 'currency', 'lowest_fare_YQ', 'lowest_fare_YQ_rt',
                     'lowest_fare_YR', 'lowest_fare_YR_rt', 'lowest_fare_base',
                     'lowest_fare_base_rt', 'lowest_fare_fb', 'lowest_fare_fb_rt',
                     'lowest_fare_surcharge', 'lowest_fare_surcharge_rt',
                     'lowest_fare_tax', 'lowest_fare_tax_rt', 'lowest_fare_total',
                     'lowest_fare_total_rt', 'date_start', 'date_end', 'currency_gds',
                     'lowest_fare_YQ_gds', 'lowest_fare_YR_gds',
                     'lowest_fare_base_gds', 'lowest_fare_fb_gds',
                     'lowest_fare_surcharge_gds', 'lowest_fare_tax_gds',
                     'lowest_fare_total_gds',
                     'lowest_fare_YQ_rt_gds', 'lowest_fare_YR_rt_gds',
                     'lowest_fare_base_rt_gds', 'lowest_fare_fb_rt_gds',
                     'lowest_fare_surcharge_rt_gds', 'lowest_fare_tax_rt_gds',
                     'lowest_fare_total_rt_gds']
        )

    try:
        if len(json_response['hffList']) > 0:
            highest_filed_fare = pd.DataFrame(json_response['hffList'])
            highest_filed_fare.rename(columns={
                "dateEnd": "date_end",
                "highestFareFb_rt": "highest_fare_fb_rt",
                "currency": "currency",
                "highestFareYQgds": "highest_fare_YQ_gds",
                "highestFareTotalGds_rt": "highest_fare_total_rt_gds",
                "highestFareBase": "highest_fare_base",
                "highestFareSurcharge": "highest_fare_surcharge",
                "highestFareBaseGds": "highest_fare_base_gds",
                "dateStart": "date_start",
                "highestFareTotalGds": "highest_fare_total_gds",
                "highestFareYQ": "highest_fare_YQ",
                "highestFareSurchargeGds": "highest_fare_surcharge_gds",
                "carrier": "carrier",
                "highestFareBaseGds_rt": "highest_fare_base_rt_gds",
                "highestFareYRGds": "highest_fare_YR_gds",
                "highestFareTotal": "highest_fare_total",
                "highestFareSurchargegds_rt": "highest_fare_surcharge_rt_gds",
                "highestFareBase_rt": "highest_fare_base_rt",
                "highestFareYR": "highest_fare_YR",
                "highestFareYRGds_rt": "highest_fare_YR_rt_gds",
                "currencyGds": "currency_gds",
                "highestFareFb": "highest_fare_fb",
                "highestFareTotal_rt": "highest_fare_total_rt",
                "highestFareYQgds_rt": "highest_fare_YQ_rt_gds",
                "highestFareFbGds_rt": "highest_fare_fb_rt_gds",
                "highestFareYQ_rt": "highest_fare_YQ_rt",
                "highestFareFbGds": "highest_fare_fb_gds",
                "highestFareYR_rt": "highest_fare_YR_rt",
                "highestFareSurcharge_rt": "highest_fare_surcharge_rt"
            }, inplace=True)
        else:
            highest_filed_fare = pd.DataFrame(
                columns=['carrier', 'currency', 'highest_fare_YQ', 'highest_fare_YQ_rt',
                         'highest_fare_YR', 'highest_fare_YR_rt', 'highest_fare_base',
                         'highest_fare_base_rt', 'highest_fare_fb', 'highest_fare_fb_rt',
                         'highest_fare_surcharge', 'highest_fare_surcharge_rt',
                         'highest_fare_tax', 'highest_fare_tax_rt', 'highest_fare_total',
                         'highest_fare_total_rt', 'date_start', 'date_end', 'currency_gds',
                         'highest_fare_YQ_gds', 'highest_fare_YR_gds',
                         'highest_fare_base_gds', 'highest_fare_fb_gds',
                         'highest_fare_surcharge_gds', 'highest_fare_tax_gds',
                         'highest_fare_total_gds', 'highest_fare_YQ_rt_gds',
                         'highest_fare_YR_rt_gds', 'highest_fare_base_rt_gds',
                         'highest_fare_fb_rt_gds', 'highest_fare_surcharge_rt_gds',
                         'highest_fare_tax_rt_gds', 'highest_fare_total_rt_gds'
                         ]
            )
    except Exception:
        highest_filed_fare = pd.DataFrame(
            columns=['carrier', 'currency', 'highest_fare_YQ', 'highest_fare_YQ_rt',
                     'highest_fare_YR', 'highest_fare_YR_rt', 'highest_fare_base',
                     'highest_fare_base_rt', 'highest_fare_fb', 'highest_fare_fb_rt',
                     'highest_fare_surcharge', 'highest_fare_surcharge_rt',
                     'highest_fare_tax', 'highest_fare_tax_rt', 'highest_fare_total',
                     'highest_fare_total_rt', 'date_start', 'date_end', 'currency_gds',
                     'highest_fare_YQ_gds', 'highest_fare_YR_gds',
                     'highest_fare_base_gds', 'highest_fare_fb_gds',
                     'highest_fare_surcharge_gds', 'highest_fare_tax_gds',
                     'highest_fare_total_gds', 'highest_fare_YQ_rt_gds',
                     'highest_fare_YR_rt_gds', 'highest_fare_base_rt_gds',
                     'highest_fare_fb_rt_gds', 'highest_fare_surcharge_rt_gds',
                     'highest_fare_tax_rt_gds', 'highest_fare_total_rt_gds'
                     ]
        )

    lowest_filed_fare = lowest_filed_fare.merge(highest_filed_fare, on=["date_start", "date_end",
                                                                        "currency", "currency_gds",
                                                                        "carrier"], how='outer')
    lowest_filed_fare['lowest_fare_tax'] = 0
    lowest_filed_fare['lowest_fare_tax_rt'] = 0
    lowest_filed_fare['lowest_fare_tax_rt_gds'] = 0
    lowest_filed_fare['lowest_fare_tax_gds'] = 0
    lowest_filed_fare['highest_fare_tax'] = 0
    lowest_filed_fare['highest_fare_tax_rt'] = 0
    lowest_filed_fare['highest_fare_tax_rt_gds'] = 0
    lowest_filed_fare['highest_fare_tax_gds'] = 0

    # lowest_filed_fare = get_lowest_filed_fare_dF(pos,
    #                                              origin,
    #                                              destination,
    #                                              compartment,
    #                                              dep_date_start,
    #                                              dep_date_end,
    #                                              comp,
    #                                              [{"start": dep_date_start,
    #                                                "end": dep_date_end}])
    # print lowest_filed_fare.head()
    #print json_response['mpfList']
    #print dates_code
    if dates_code:
        dates_code_temp = dates_code[0]
    else:
        dates_code_temp = None

    try:
        if dates_code is not None:

            most_avail_df = pd.DataFrame(
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
            for i in range(1, 6):
                temp_df = pd.DataFrame([{'carrier': json_response['comp{}carrier'.format(i)],
                                         'most_avail_fare_total_ow': json_response['comp{}popularwebfare_ow'.format(i)],
                                         'most_avail_fare_tax_ow': json_response['comp{}Tax'.format(i)],
                                         'most_avail_fare_base_ow': json_response['comp{}popularwebfare_ow'.format(i)] - \
                                                                     json_response['comp{}Tax'.format(i)],
                                         'most_avail_fare_count_ow': 'NA',
                                         'most_avail_fare_freq_ow': json_response['comp{}Frequency'.format(i)],
                                         'observation_date_ow': json_response['comp{}ObservationDate'.format(i)],
                                         'most_avail_fare_total_rt': json_response['comp{}popularwebfare_rt'.format(i)],
                                         'most_avail_fare_tax_rt': json_response['comp{}Tax_rt'.format(i)],
                                         'most_avail_fare_base_rt': json_response['comp{}popularwebfare_rt'.format(i)] - \
                                                                     json_response['comp{}Tax_rt'.format(i)],
                                         'most_avail_fare_count_rt': 'NA',
                                         'most_avail_fare_freq_rt': json_response['comp{}Frequency_rt'.format(i)],
                                         'observation_date_rt': json_response['comp{}ObservationDate_rt'.format(i)],
                                         'currency': json_response['comp{}popularwebfarecurrency_ow'.format(i)],
                                         'fare_basis_ow': json_response['comp{}OutboundFareBasis'.format(i)],
                                         'fare_basis_rt': json_response['comp{}OutboundFareBasis_rt'.format(i)],
                                         }])
                most_avail_df = pd.concat([most_avail_df, temp_df])
            temp_df = pd.DataFrame([{'carrier': Host_Airline_Code,
                                     'most_avail_fare_total_ow': json_response['popularwebfare_ow'],
                                     'most_avail_fare_tax_ow': json_response['hostTax'],
                                     'most_avail_fare_base_ow': json_response['popularwebfare_ow'] - \
                                                                 json_response['hostTax'],
                                     'most_avail_fare_count_ow': 'NA',
                                     'most_avail_fare_freq_ow': json_response['hostFrequency'],
                                     'observation_date_ow': json_response['hostObservationDate'],
                                     'most_avail_fare_total_rt': json_response['popularwebfare_rt'],
                                     'most_avail_fare_tax_rt': json_response['hostTax_rt'],
                                     'most_avail_fare_base_rt': json_response['popularwebfare_rt'] - \
                                                                 json_response['hostTax_rt'],
                                     'most_avail_fare_count_rt': 'NA',
                                     'most_avail_fare_freq_rt': json_response['hostFrequency_rt'],
                                     'observation_date_rt': json_response['hostObservationDate_rt'],
                                     'currency': json_response['popularwebfarecurrency_ow'],
                                     'fare_basis_ow': json_response['hostOutboundFareBasis'],
                                     'fare_basis_rt': json_response['hostOutboundFareBasis_rt'],
                                     }])
            most_avail_df = pd.concat([most_avail_df, temp_df])
            most_avail_df.reset_index(drop=True, inplace=True)
            most_avail_df.fillna('NA', inplace=True)
        elif len(json_response['mpfList']) > 0 and dates_code is None:
            # most_avail_df = pd.DataFrame(
            #     columns=[
            #         'carrier',
            #         'most_avail_fare_base_ow',
            #         'most_avail_fare_total_ow',
            #         'most_avail_fare_tax_ow',
            #         'most_avail_fare_count_ow',
            #         'most_avail_fare_base_rt',
            #         'most_avail_fare_total_rt',
            #         'most_avail_fare_tax_rt',
            #         'most_avail_fare_count_rt',
            #         'currency',
            #         'fare_basis_ow',
            #         'fare_basis_rt',
            #         "observation_date_ow",
            #         "observation_date_rt"
            #     ]
            # )
            # for i in range(1, 6):
            #     temp_df = pd.DataFrame([{'carrier': json_response['{}carrier'.format(i)],
            #                              'most_avail_fare_total_ow': json_response['comp{}popularwebfare_ow'.format(i)],
            #                              'most_avail_fare_tax_ow': json_response['comp{}Tax'.format(i)],
            #                              'most_avail_fare_base_ow': json_response['comp{}popularwebfare_ow'.format(i)] - \
            #                                                          json_response['comp{}Tax'.format(i)],
            #                              'most_avail_fare_count_ow': 'NA',
            #                              'most_avail_fare_freq_ow': json_response['comp{}Frequency'.format(i)],
            #                              'observation_date_ow': json_response['comp{}ObservationDate'.format(i)],
            #                              'most_avail_fare_total_rt': json_response['comp{}popularwebfare_rt'.format(i)],
            #                              'most_avail_fare_tax_rt': json_response['comp{}Tax_rt'.format(i)],
            #                              'most_avail_fare_base_rt': json_response['comp{}popularwebfare_rt'.format(i)] - \
            #                                                          json_response['comp{}Tax_rt'.format(i)],
            #                              'most_avail_fare_count_rt': 'NA',
            #                              'most_avail_fare_freq_rt': json_response['comp{}Frequency_rt'.format(i)],
            #                              'observation_date_rt': json_response['comp{}ObservationDate_rt'.format(i)],
            #                              'currency': json_response['comp{}popularwebfarecurrency_ow'.format(i)],
            #                              'fare_basis_ow': json_response['comp{}OutboundFareBasis'.format(i)],
            #                              'fare_basis_rt': json_response['comp{}OutboundFareBasis_rt'.format(i)],
            #                              }])
            #     most_avail_df = pd.concat([most_avail_df, temp_df])

            most_avail_df = pd.DataFrame(json_response['mpfList'])
            most_avail_df['most_avail_fare_count_ow'] = 'NA'
            most_avail_df['most_avail_fare_count_rt'] = 'NA'

            #print most_avail_df.head().to_string()
            most_avail_df.drop(['currency_rt'], axis =1, inplace=True)

            most_avail_df.rename(columns={'currency_ow':'currency',
                                           'outbound_fare_basis_ow':'fare_basis_ow',
                                           'outbound_fare_basis_rt': 'fare_basis_rt',
                                           'fare_ow' : 'most_avail_fare_total_ow',
                                           'fare_rt' : 'most_avail_fare_total_rt',
                                           'tax_ow' : 'most_avail_fare_tax_ow',
                                           'tax_rt' : 'most_avail_fare_tax_rt',
                                           'frequency_ow' : 'most_avail_fare_freq_ow',
                                           'frequency_rt' : 'most_avail_fare_freq_rt'
                                           }, inplace=True)
            #print most_avail_df.head().to_string()
            most_avail_df['most_avail_fare_base_ow'] = most_avail_df['most_avail_fare_total_ow']-most_avail_df['most_avail_fare_tax_ow']
            most_avail_df['most_avail_fare_base_rt'] = most_avail_df['most_avail_fare_total_rt'] - most_avail_df['most_avail_fare_tax_rt']
            #print most_avail_df.head().to_string()
            # temp_df = pd.DataFrame([{'carrier': Host_Airline_Code,
            #                          'most_avail_fare_total_ow': json_response['popularwebfare_ow'],
            #                          'most_avail_fare_tax_ow': json_response['hostTax'],
            #                          'most_avail_fare_base_ow': json_response['popularwebfare_ow'] - \
            #                                                      json_response['hostTax'],
            #                          'most_avail_fare_count_ow': 'NA',
            #                          'most_avail_fare_freq_ow': json_response['hostFrequency'],
            #                          'observation_date_ow': json_response['hostObservationDate'],
            #                          'most_avail_fare_total_rt': json_response['popularwebfare_rt'],
            #                          'most_avail_fare_tax_rt': json_response['hostTax_rt'],
            #                          'most_avail_fare_base_rt': json_response['popularwebfare_rt'] - \
            #                                                      json_response['hostTax_rt'],
            #                          'most_avail_fare_count_rt': 'NA',
            #                          'most_avail_fare_freq_rt': json_response['hostFrequency_rt'],
            #                          'observation_date_rt': json_response['hostObservationDate_rt'],
            #                          'currency': json_response['popularwebfarecurrency_ow'],
            #                          'fare_basis_ow': json_response['hostOutboundFareBasis'],
            #                          'fare_basis_rt': json_response['hostOutboundFareBasis_rt'],
            #                          }])
            # most_avail_df = pd.concat([most_avail_df, temp_df])
            most_avail_df.reset_index(drop=True, inplace=True)
            most_avail_df.fillna('NA', inplace=True)
            #print "fg fgfjgf"
        else:
            most_avail_df = pd.DataFrame(
                columns=[
                    'carrier',
                    'most_avail_fare_base_ow',
                    'most_avail_fare_total_ow',
                    'most_avail_fare_tax_ow',
                    'most_avail_fare_count_ow',
                    'most_avail_fare_freq_ow',
                    'most_avail_fare_base_rt',
                    'most_avail_fare_total_rt',
                    'most_avail_fare_tax_rt',
                    'most_avail_fare_count_rt',
                    'most_avail_fare_freq_rt',
                    'currency',
                    'fare_basis_ow',
                    'fare_basis_rt',
                    "observation_date_ow",
                    "observation_date_rt"
                ]
            )
            #print "wioooioioi"


    except Exception as err:
        #print err
        most_avail_df = pd.DataFrame(
            columns=[
                'carrier',
                'most_avail_fare_base_ow',
                'most_avail_fare_total_ow',
                'most_avail_fare_tax_ow',
                'most_avail_fare_count_ow',
                'most_avail_fare_freq_ow',
                'most_avail_fare_base_rt',
                'most_avail_fare_total_rt',
                'most_avail_fare_tax_rt',
                'most_avail_fare_count_rt',
                'most_avail_fare_freq_rt',
                'currency',
                'fare_basis_ow',
                'fare_basis_rt',
                "observation_date_ow",
                "observation_date_rt"
            ]
        )

    # most_avail_df = get_most_avail_dict(pos,
    #                                     origin,
    #                                     destination,
    #                                     compartment,
    #                                     dep_date_end=dep_date_end,
    #                                     dep_date_start=dep_date_start,
    #                                     date_list=[{"start": dep_date_start,
    #                                                 "end": dep_date_end}])
    # most_avail_df = most_avail_df[most_avail_df['carrier'] == "FZ"].reindex().to_dict("records")[0]

    try:
        host_data['lowest_filed_fare'] = dict(
            total_fare=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                             == Host_Airline_Code, "lowest_fare_total"].values[0],
            base_fare=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                            == Host_Airline_Code, "lowest_fare_base"].values[0],
            fare_basis=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                             == Host_Airline_Code, "lowest_fare_fb"].values[0],
            tax=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                      == Host_Airline_Code, "lowest_fare_tax"].values[0],
            yq=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                     == Host_Airline_Code, "lowest_fare_YQ"].values[0],
            yr=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                     == Host_Airline_Code, "lowest_fare_YR"].values[0],
            surcharge=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == Host_Airline_Code, "lowest_fare_surcharge"].values[
                0],
            currency=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == Host_Airline_Code, "currency"].values[
                0],
            total_fare_gds=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                                 == Host_Airline_Code, "lowest_fare_total_gds"].values[0],
            base_fare_gds=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                                == Host_Airline_Code, "lowest_fare_base_gds"].values[0],
            fare_basis_gds=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                                 == Host_Airline_Code, "lowest_fare_fb_gds"].values[0],
            tax_gds=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                          == Host_Airline_Code, "lowest_fare_tax_gds"].values[0],
            yq_gds=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                         == Host_Airline_Code, "lowest_fare_YQ_gds"].values[0],
            yr_gds=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                         == Host_Airline_Code, "lowest_fare_YR_gds"].values[0],
            surcharge_gds=
            lowest_filed_fare.loc[lowest_filed_fare['carrier'] == Host_Airline_Code, "lowest_fare_surcharge_gds"].values[
                0],
            currency_gds=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == Host_Airline_Code, "currency_gds"].values[
                0],
            total_fare_rt=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                                == Host_Airline_Code, "lowest_fare_total_rt"].values[0],
            base_fare_rt=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                               == Host_Airline_Code, "lowest_fare_base_rt"].values[0],
            fare_basis_rt=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                                == Host_Airline_Code, "lowest_fare_fb_rt"].values[0],
            tax_rt=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                         == Host_Airline_Code, "lowest_fare_tax_rt"].values[0],
            yq_rt=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                        == Host_Airline_Code, "lowest_fare_YQ_rt"].values[0],
            yr_rt=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                        == Host_Airline_Code, "lowest_fare_YR_rt"].values[0],
            surcharge_rt=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == Host_Airline_Code, "lowest_fare_surcharge_rt"].values[
                0],
            total_fare_gds_rt=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                                    == Host_Airline_Code, "lowest_fare_total_rt_gds"].values[0],
            base_fare_gds_rt=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                                   == Host_Airline_Code, "lowest_fare_base_rt_gds"].values[0],
            fare_basis_gds_rt=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                                    == Host_Airline_Code, "lowest_fare_fb_rt_gds"].values[0],
            tax_gds_rt=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                             == Host_Airline_Code, "lowest_fare_tax_rt_gds"].values[0],
            yq_gds_rt=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                            == Host_Airline_Code, "lowest_fare_YQ_rt_gds"].values[0],
            yr_gds_rt=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                            == Host_Airline_Code, "lowest_fare_YR_rt_gds"].values[0],
            surcharge_gds_rt=
            lowest_filed_fare.loc[lowest_filed_fare['carrier'] == Host_Airline_Code, "lowest_fare_surcharge_rt_gds"].values[
                0]
        )
    except Exception:
        host_data['lowest_filed_fare'] = dict(
            total_fare="NA",
            base_fare="NA",
            tax="NA",
            fare_basis='NA',
            yq="NA",
            yr="NA",
            surcharge="NA",
            currency="NA",
            total_fare_gds="NA",
            base_fare_gds="NA",
            tax_gds="NA",
            fare_basis_gds='NA',
            yq_gds="NA",
            yr_gds="NA",
            surcharge_gds="NA",
            currency_gds="NA",
            total_fare_rt="NA",
            base_fare_rt="NA",
            tax_rt="NA",
            fare_basis_rt='NA',
            yq_rt="NA",
            yr_rt="NA",
            surcharge_rt="NA",
            total_fare_gds_rt="NA",
            base_fare_gds_rt="NA",
            tax_gds_rt="NA",
            fare_basis_gds_rt='NA',
            yq_gds_rt="NA",
            yr_gds_rt="NA",
            surcharge_gds_rt="NA"
        )
    try:
        host_data['price_movement_filed'] = dict(lowest_fare=host_data['lowest_filed_fare'],
                                                 highest_fare=dict(total_fare=lowest_filed_fare.loc[
                                                     lowest_filed_fare['carrier'] == Host_Airline_Code,
                                                     "highest_fare_total"].values[0],
                                                                   base_fare=lowest_filed_fare.loc[
                                                                       lowest_filed_fare['carrier'] == Host_Airline_Code,
                                                                       "highest_fare_base"].values[0],
                                                                   fare_basis=lowest_filed_fare.loc[
                                                                       lowest_filed_fare['carrier'] == Host_Airline_Code,
                                                                       "highest_fare_fb"].values[0],
                                                                   tax=lowest_filed_fare.loc[
                                                                       lowest_filed_fare['carrier'] == Host_Airline_Code,
                                                                       "highest_fare_tax"].values[0],
                                                                   yq=lowest_filed_fare.loc[
                                                                       lowest_filed_fare['carrier'] == Host_Airline_Code,
                                                                       "highest_fare_YQ"].values[0],
                                                                   yr=lowest_filed_fare.loc[
                                                                       lowest_filed_fare['carrier'] == Host_Airline_Code,
                                                                       "highest_fare_YR"].values[0],
                                                                   surcharge=lowest_filed_fare.loc[
                                                                       lowest_filed_fare['carrier'] == Host_Airline_Code,
                                                                       "highest_fare_surcharge"].values[0],
                                                                   currency=lowest_filed_fare.loc[
                                                                       lowest_filed_fare['carrier'] == Host_Airline_Code,
                                                                       "currency"].values[0],
                                                                   ))
    except Exception:
        host_data['price_movement_filed'] = dict(
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
    try:
        host_data['most_available_fare'] = dict(
            base_fare_ow=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'most_avail_fare_base_ow'].values[0],
            frequency_ow=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'most_avail_fare_freq_ow'].values[0],
            tax_ow=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'most_avail_fare_tax_ow'].values[0],
            total_fare_ow=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'most_avail_fare_total_ow'].values[0],
            total_count_ow=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'most_avail_fare_count_ow'].values[0],
            currency=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'currency'].values[0],
            base_fare_rt=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'most_avail_fare_base_rt'].values[0],
            frequency_rt=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'most_avail_fare_freq_rt'].values[0],
            tax_rt=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'most_avail_fare_tax_rt'].values[0],
            total_fare_rt=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'most_avail_fare_total_rt'].values[0],
            total_count_rt=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'most_avail_fare_count_rt'].values[0],
            fare_basis_ow=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'fare_basis_ow'].values[0],
            fare_basis_rt=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'fare_basis_rt'].values[0],
            observation_date_rt=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'observation_date_rt'].values[0],
            observation_date_ow=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'observation_date_ow'].values[0]

        )
    except Exception:
        host_data['most_available_fare'] = dict(
            base_fare_ow="NA",
            frequency_ow="NA",
            tax_ow="NA",
            total_fare_ow="NA",
            total_count_ow="NA",
            currency="NA",
            base_fare_rt="NA",
            frequency_rt="NA",
            tax_rt="NA",
            total_fare_rt="NA",
            total_count_rt="NA",
            fare_basis_ow="NA",
            fare_basis_rt="NA",
            observation_date_rt="NA",
            observation_date_ow="NA"
        )

    for i in range(1, 5):
        temp = {}
        temp['market_share'] = json_response['comp' + str(i) + 'marketshare']
        temp['fms'] = json_response['comp' + str(i) + 'fairmarketshare']
        try:
            # temp['distributor_rating'] = ratings_df.loc[(
            #                                                     ratings_df['Airline'] == json_response[
            #                                                 'comp' + str(i) + 'carrier']), 'distributor_rating'].values[
            #     0]
            # temp['rating'] = ratings_df.loc[(
            #                                         ratings_df['Airline'] == json_response[
            #                                     'comp' + str(i) + 'carrier']), 'overall_rating'].values[0]
            # temp['capacity_rating'] = ratings_df.loc[(
            #                                                  ratings_df['Airline'] == json_response[
            #                                              'comp' + str(i) + 'carrier']), 'capacity_rating'].values[0]
            # temp['market_rating'] = ratings_df.loc[(
            #                                                ratings_df['Airline'] == json_response[
            #                                            'comp' + str(i) + 'carrier']), 'market_rating'].values[0]
            # temp['fare_rating'] = ratings_df.loc[(
            #                                              ratings_df['Airline'] == json_response[
            #                                          'comp' + str(i) + 'carrier']), 'fare_rating'].values[0]
            # temp['product_rating'] = ratings_df.loc[(
            #                                                 ratings_df['Airline'] == json_response[
            #                                             'comp' + str(i) + 'carrier']), 'product_rating'].values[0]

            temp['distributor_rating'] =json_response['distriute' + str(i) + 'Rating']

            temp['rating'] = json_response['comp' + str(i) + 'rating']

            temp['capacity_rating'] = json_response['capacityRating'+str(i)]

            temp['market_rating'] = json_response['market' + str(i) + 'Rating']

            temp['fare_rating'] = json_response['fare' + str(i) + 'Rating']

            temp['product_rating'] = json_response['prduct' + str(i) + 'Rating']


        except Exception:
            temp['rating'] = 5
            temp['distributor_rating'] = 5
            temp['capacity_rating'] = 5
            temp['market_rating'] = 5
            temp['fare_rating'] = 5
            temp['product_rating'] = 5
        try:
            # temp['pax'] = pax_dict_ty[json_response['comp' + str(i) + 'carrier']]  # check
            temp['pax'] = json_response['comp' + str(i) + 'pax']
        except KeyError:
            temp['pax'] = "NA"
        temp['market_share_vtgt'] = json_response['comp' +
                                                  str(i) + 'marketsharevtgt']
        try:
            # temp['market_share_vlyr'] = ((pax_dict_ty[json_response['comp' + str(i) + 'carrier']] /
            #                              market_size_ty[json_response['comp' + str(i) + 'carrier']]) - \
            #                             (pax_dict_ly[json_response['comp' + str(i) + 'carrier']] /
            #                              market_size_ly[json_response['comp' + str(i) + 'carrier']])) * 100
            temp['market_share_vlyr'] = json_response['marketShareVLYR'+str(i)]
        except Exception:
            temp['market_share_vlyr'] = "NA"

        try:
            # cap_adj = cap_ly[json_response['comp' + str(i) + 'carrier']] / cap_ty[
            #     json_response['comp' + str(i) + 'carrier']]
            # temp['pax_vlyr'] = (pax_dict_ty[json_response['comp' + str(i) + 'carrier']] * cap_adj - pax_dict_ly[
            #     json_response['comp' + str(i) + 'carrier']]) * 100 / pax_dict_ly[
            #                        json_response['comp' + str(i) + 'carrier']]
            temp['pax_vlyr'] = json_response['compPaxvlyr'+str(i)]

        except Exception:
            temp['pax_vlyr'] = "NA"
        temp['airline'] = json_response['comp' + str(i) + 'carrier']

        try:
            temp['lowest_filed_fare'] = dict(
                total_fare=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], "lowest_fare_total"].values[0],
                base_fare=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], "lowest_fare_base"].values[0],
                fare_basis=lowest_filed_fare.loc[
                    lowest_filed_fare['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], "lowest_fare_fb"].values[
                    0],
                tax=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], "lowest_fare_tax"].values[0],
                yq=lowest_filed_fare.loc[
                    lowest_filed_fare['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], "lowest_fare_YQ"].values[
                    0],
                yr=lowest_filed_fare.loc[
                    lowest_filed_fare['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], "lowest_fare_YR"].values[
                    0],
                surcharge=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], "lowest_fare_surcharge"].values[0],
                currency=lowest_filed_fare.loc[
                    lowest_filed_fare['carrier'] == json_response['comp' + str(i) + 'carrier'], "currency"].values[0],
                total_fare_gds=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], "lowest_fare_total_gds"].values[0],
                base_fare_gds=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], "lowest_fare_base_gds"].values[0],
                fare_basis_gds=lowest_filed_fare.loc[
                    lowest_filed_fare['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], "lowest_fare_fb_gds"].values[
                    0],
                tax_gds=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], "lowest_fare_tax_gds"].values[0],
                yq_gds=lowest_filed_fare.loc[
                    lowest_filed_fare['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], "lowest_fare_YQ_gds"].values[
                    0],
                yr_gds=lowest_filed_fare.loc[
                    lowest_filed_fare['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], "lowest_fare_YR_gds"].values[
                    0],
                surcharge_gds=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], "lowest_fare_surcharge_gds"].values[0],
                currency_gds=lowest_filed_fare.loc[
                    lowest_filed_fare['carrier'] == json_response['comp' + str(i) + 'carrier'], "currency_gds"].values[
                    0],
                total_fare_rt=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], "lowest_fare_total_rt"].values[0],
                base_fare_rt=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], "lowest_fare_base_rt"].values[0],
                fare_basis_rt=lowest_filed_fare.loc[
                    lowest_filed_fare['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], "lowest_fare_fb_rt"].values[
                    0],
                tax_rt=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], "lowest_fare_tax_rt"].values[0],
                yq_rt=lowest_filed_fare.loc[
                    lowest_filed_fare['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], "lowest_fare_YQ_rt"].values[
                    0],
                yr_rt=lowest_filed_fare.loc[
                    lowest_filed_fare['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], "lowest_fare_YR_rt"].values[
                    0],
                surcharge_rt=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], "lowest_fare_surcharge_rt"].values[0],
                total_fare_gds_rt=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], "lowest_fare_total_rt_gds"].values[0],
                base_fare_gds_rt=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], "lowest_fare_base_rt_gds"].values[0],
                fare_basis_gds_rt=lowest_filed_fare.loc[
                    lowest_filed_fare['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], "lowest_fare_fb_rt_gds"].values[
                    0],
                tax_gds_rt=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], "lowest_fare_tax_rt_gds"].values[0],
                yq_gds_rt=lowest_filed_fare.loc[
                    lowest_filed_fare['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], "lowest_fare_YQ_rt_gds"].values[
                    0],
                yr_gds_rt=lowest_filed_fare.loc[
                    lowest_filed_fare['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], "lowest_fare_YR_rt_gds"].values[
                    0],
                surcharge_gds_rt=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], "lowest_fare_surcharge_rt_gds"].values[0],

            )
        except Exception:
            temp['lowest_filed_fare'] = dict(
                total_fare="NA",
                base_fare="NA",
                tax="NA",
                fare_basis='NA',
                yq="NA",
                yr="NA",
                surcharge="NA",
                currency="NA",
                total_fare_gds="NA",
                base_fare_gds="NA",
                tax_gds="NA",
                fare_basis_gds='NA',
                yq_gds="NA",
                yr_gds="NA",
                surcharge_gds="NA",
                currency_gds="NA",
                total_fare_rt="NA",
                base_fare_rt="NA",
                tax_rt="NA",
                fare_basis_rt='NA',
                yq_rt="NA",
                yr_rt="NA",
                surcharge_rt="NA",
                total_fare_gds_rt="NA",
                base_fare_gds_rt="NA",
                tax_gds_rt="NA",
                fare_basis_gds_rt='NA',
                yq_gds_rt="NA",
                yr_gds_rt="NA",
                surcharge_gds_rt="NA",
            )
        except IndexError:
            temp['lowest_filed_fare'] = dict(
                total_fare="NA",
                base_fare="NA",
                tax="NA",
                fare_basis='NA',
                yq="NA",
                yr="NA",
                surcharge="NA",
                currency="NA",
                total_fare_gds="NA",
                base_fare_gds="NA",
                tax_gds="NA",
                fare_basis_gds='NA',
                yq_gds="NA",
                yr_gds="NA",
                surcharge_gds="NA",
                currency_gds="NA",
                total_fare_rt="NA",
                base_fare_rt="NA",
                tax_rt="NA",
                fare_basis_rt='NA',
                yq_rt="NA",
                yr_rt="NA",
                surcharge_rt="NA",
                total_fare_gds_rt="NA",
                base_fare_gds_rt="NA",
                tax_gds_rt="NA",
                fare_basis_gds_rt='NA',
                yq_gds_rt="NA",
                yr_gds_rt="NA",
                surcharge_gds_rt="NA",
            )
        try:
            temp['price_movement_filed'] = dict(
                lowest_fare=temp['lowest_filed_fare'],
                highest_fare=dict(
                    total_fare=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], "highest_fare_total"].values[0],
                    base_fare=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], "highest_fare_base"].values[0],
                    fare_basis=lowest_filed_fare.loc[
                        lowest_filed_fare['carrier'] == json_response[
                            'comp' + str(i) + 'carrier'], "highest_fare_fb"].values[
                        0],
                    tax=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], "highest_fare_tax"].values[0],
                    yq=lowest_filed_fare.loc[
                        lowest_filed_fare['carrier'] == json_response[
                            'comp' + str(i) + 'carrier'], "highest_fare_YQ"].values[
                        0],
                    yr=lowest_filed_fare.loc[
                        lowest_filed_fare['carrier'] == json_response[
                            'comp' + str(i) + 'carrier'], "highest_fare_YR"].values[
                        0],
                    surcharge=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], "highest_fare_surcharge"].values[0],
                    currency=lowest_filed_fare.loc[
                        lowest_filed_fare['carrier'] == json_response['comp' + str(i) + 'carrier'], "currency"].values[
                        0],
                )
            )
        except Exception:
            temp['price_movement_filed'] = dict(
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
        except IndexError:
            temp['price_movement_filed'] = dict(
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
        try:
            #print json_response['comp'+str(i)+'carrier']
            temp['most_available_fare'] = dict(
                base_fare_ow=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], 'most_avail_fare_base_ow'].values[0],
                frequency_ow=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], 'most_avail_fare_freq_ow'].values[0],
                tax_ow=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], 'most_avail_fare_tax_ow'].values[0],
                total_fare_ow=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], 'most_avail_fare_total_ow'].values[0],
                total_count_ow=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], 'most_avail_fare_count_ow'].values[0],
                currency=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], 'currency'].values[0],
                base_fare_rt=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], 'most_avail_fare_base_rt'].values[0],
                frequency_rt=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], 'most_avail_fare_freq_rt'].values[0],
                tax_rt=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], 'most_avail_fare_tax_rt'].values[0],
                total_fare_rt=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], 'most_avail_fare_total_rt'].values[0],
                total_count_rt=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], 'most_avail_fare_count_rt'].values[0],
                fare_basis_rt=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], 'fare_basis_rt'].values[0],
                fare_basis_ow=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], 'fare_basis_ow'].values[0],
                observation_date_rt=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], 'observation_date_rt'].values[0],
                observation_date_ow=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], 'observation_date_ow'].values[0],
            )
        except Exception:
            temp['most_available_fare'] = dict(
                base_fare_ow="NA",
                frequency_ow="NA",
                tax_ow="NA",
                total_fare_ow="NA",
                total_count_ow="NA",
                currency="NA",
                base_fare_rt="NA",
                frequency_rt="NA",
                tax_rt="NA",
                total_fare_rt="NA",
                total_count_rt="NA",
                fare_basis_ow="NA",
                fare_basis_rt="NA",
                observation_date_ow="NA",
                observation_date_rt="NA"
            )
        except IndexError:
            temp['most_available_fare'] = dict(
                base_fare_ow="NA",
                frequency_ow="NA",
                tax_ow="NA",
                total_fare_ow="NA",
                total_count_ow="NA",
                currency="NA",
                base_fare_rt="NA",
                frequency_rt="NA",
                tax_rt="NA",
                total_fare_rt="NA",
                total_count_rt="NA",
                fare_basis_ow="NA",
                fare_basis_rt="NA"
            )
        comp_data.append(temp)
    return host_data, comp_data, most_avail_df


if __name__ == '__main__':
    client = mongo_client()
    db=client[JUPITER_DB]
    # st = time.time()
    # # print get_dep_date_filters()
    # main_func("AMM", "AMM", "VKO", "Y", "2017-09-01", "2017-09-30",
    #           dates_list=[{"start": "2017-09-01", "end": "2017-09-30"}], is_manual=1)
    # print "TIME TAKEN TO RUN ENTIRE MAIN FUNCTION =", time.time() - st
    print get_config_dates('TRV', 'DXBTRV', 'Y', db=db)
