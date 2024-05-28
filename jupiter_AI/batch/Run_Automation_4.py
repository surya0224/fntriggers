import json
import datetime
import calendar
import logging
from datetime import date
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import datetime as dt
import time
import rabbitpy
import pandas as pd
from jupiter_AI import RABBITMQ_HOST, RABBITMQ_PASSWORD, RABBITMQ_PORT, RABBITMQ_USERNAME, client, JUPITER_DB, ATPCO_DB, \
    JUPITER_LOGGER
from jupiter_AI import SYSTEM_DATE, today, parameters
from jupiter_AI.triggers.common import sending_error_mail
from jupiter_AI.batch.Run_Automation_5 import manual_trigger_wm_scripts
# from jupiter_AI.batch.Run_triggers import data_level_triggers_batch, segregate_markets
from jupiter_AI.batch.atpco_automation.task_group_mappings import tasks_in_automation
from jupiter_AI.batch.atpco_automation.Automation_tasks import run_manual_trigger_module_VLYR_pax_1, \
    run_manual_trigger_forecast, \
    run_manual_trigger_distance, run_manual_trigger_capacity, run_manual_trigger_target, send_exec_stats_mail, \
    run_landing_page_tile_triggers, run_manual_trigger_summary_to_landing_page, run_manual_trigger_to_summary, \
    run_manual_trigger_to_summary_TL, \
    run_manual_trigger_summary_popular_fare, run_manual_trigger_WMQY_market_significant
from celery import group
import pika

from jupiter_AI.logutils import measure

url = 'amqp://' + RABBITMQ_USERNAME + \
      ":" + RABBITMQ_PASSWORD + \
      "@" + RABBITMQ_HOST + ":" + str(RABBITMQ_PORT) + "/"

db = client[JUPITER_DB]
today_1 = today - dt.timedelta(days=1)
SYSTEM_DATE_1 = (today - dt.timedelta(days=1)).strftime('%Y-%m-%d')


@measure(JUPITER_LOGGER)
def manual_trigger_scripts():
    mail_array = []
    with rabbitpy.Connection(url) as conn:
        with conn.channel() as channel:
            queue = rabbitpy.Queue(channel, 'booking')
            print "Waiting for messages from booking queue"
            for message in queue:
                received_msg = message.json()
                if received_msg['message'] == "booking_" + SYSTEM_DATE:
                    print 'Received booking message: ', received_msg
                    message.ack()
                    break
                else:
                    print "Received wrong booking message: ", received_msg
                    message.ack()

    with rabbitpy.Connection(url) as conn:
        with conn.channel() as channel:
            queue = rabbitpy.Queue(channel, 'sales')
            print "Waiting for messages from sales queue"
            for message in queue:
                received_msg = message.json()
                if received_msg['message'] == "sales_" + SYSTEM_DATE:
                    print 'Received sales message: ', received_msg
                    message.ack()
                    break
                else:
                    print "Received wrong sales message: ", received_msg
                    message.ack()

    with rabbitpy.Connection(url) as conn:
        with conn.channel() as channel:
            queue = rabbitpy.Queue(channel, 'flown')
            print "Waiting for messages from flown queue"
            for message in queue:
                received_msg = message.json()
                if received_msg['message'] == "flown_" + SYSTEM_DATE:
                    print 'Received flown message: ', received_msg
                    message.ack()
                    break
                else:
                    print "Received wrong flown message: ", received_msg
                    message.ack()


    count_non_sig = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_group_VLYR',
                                                  'start_date': SYSTEM_DATE,
                                                  'end_time': {'$exists': True}}).count()

    if count_non_sig == 0:
        count = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_group_VLYR',
                                              'start_date': SYSTEM_DATE}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'manual_trigger_group_VLYR',
                                        'start_date': SYSTEM_DATE,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count + 1})

        # vlyr
        pos_list = list(
            db.JUP_DB_Manual_Triggers_Module.distinct('pos.City', {'pos': {'$ne': None}, 'trx_date': SYSTEM_DATE}))
        mtt = []
        for pos in pos_list:
            mtt.append(run_manual_trigger_module_VLYR_pax_1.s(pos=pos))
        group1 = group(mtt)
        res1 = group1()
        grp_res1 = res1.get()
        temp_for_manual_trigger_grp_VLYR = count+1
        check = sending_error_mail(system_date=SYSTEM_DATE, group='manual_trigger_group_VLYR', db=db,
                                   attempt=temp_for_manual_trigger_grp_VLYR)
        if check == 0:
        ## VLYR Script has to run alone to update last year values then other scripts can run parallely
            db.JUP_DB_Celery_Status.update({'group_name': 'manual_trigger_group_VLYR',
                                            'start_date': SYSTEM_DATE,
                                            'attempt_number': count + 1},
                                           {'$set': {
                                               'end_date': SYSTEM_DATE,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
            mail_array.append("run_manual_trigger_module_VLYR_pax_1")
        else:
            return
    mt_forcast = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_group_capacity',
                                               'start_date': SYSTEM_DATE,
                                               'end_time': {'$exists': True}}).count()
    if mt_forcast == 0:
        count = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_group_capacity',
                                              'start_date': SYSTEM_DATE}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'manual_trigger_group_capacity',
                                        'start_date': SYSTEM_DATE,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count + 1})
        db.JUP_DB_Celery_Status.insert({'group_name': 'manual_trigger_group_distance',
                                        'start_date': SYSTEM_DATE,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count + 1})

        # Capacity
        od_list = list(db.JUP_DB_Manual_Triggers_Module.distinct('od', {'pos': {'$ne': None}, 'trx_date': SYSTEM_DATE}))
        numb = 0
        mtt = []
        od_arr = []
        for od in od_list:
            od_arr.append(od)
            if numb == 100:
                mtt.append(run_manual_trigger_capacity.s(od=od_arr))
                mtt.append(run_manual_trigger_distance.s(od=od_arr))
                od_arr = []
                numb = 0
            else:
                numb = numb + 1
        mtt.append(run_manual_trigger_capacity.s(od=od_arr))
        mtt.append(run_manual_trigger_distance.s(od=od_arr))
        group3 = group(mtt)
        res3 = group3()
        grp_res3 = res3.get()
        temp_for_manual_trigger_grp_cap = count+1
        check = sending_error_mail(system_date=SYSTEM_DATE, group='manual_trigger_group_capacity', db=db, attempt=temp_for_manual_trigger_grp_cap)
        if check == 0:
            db.JUP_DB_Celery_Status.update({'group_name': 'manual_trigger_group_capacity',
                                            'start_date': SYSTEM_DATE,
                                            'attempt_number': count + 1},
                                           {'$set': {
                                               'end_date': SYSTEM_DATE,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
            db.JUP_DB_Celery_Status.update({'group_name': 'manual_trigger_group_distance',
                                            'start_date': SYSTEM_DATE,
                                            'attempt_number': count + 1},
                                           {'$set': {
                                               'end_date': SYSTEM_DATE,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
        else:
            return
        # mt_forcast = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_group_distance',
        #                                            'start_date': SYSTEM_DATE,
        #                                            'end_time': {'$exists': True}}).count()
        # if mt_forcast == 0:
        #     count = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_group_distance',
        #                                           'start_date': SYSTEM_DATE}).count()
        #     db.JUP_DB_Celery_Status.insert({'group_name': 'manual_trigger_group_distance',
        #                                     'start_date': SYSTEM_DATE,
        #                                     'start_time': datetime.datetime.now().strftime('%H:%M'),
        #                                     'attempt_number': count + 1})
        #
        #     # Distance
        #     # od_list = list(db.JUP_DB_Manual_Triggers_Module.distinct('od', {'pos': {'$ne': None}, 'trx_date': SYSTEM_DATE}))
        #     # for od in od_list:
        #     mtt.append(run_manual_trigger_distance.s())
        #     group4 = group(mtt)
        #     res4 = group4()
        #     grp_res4 = res4.get()
        #     db.JUP_DB_Celery_Status.update({'group_name': 'manual_trigger_group_distance',
        #                                     'start_date': SYSTEM_DATE,
        #                                     'attempt_number': count + 1},
        #                                    {'$set': {
        #                                        'end_date': SYSTEM_DATE,
        #                                        'end_time': datetime.datetime.now().strftime('%H:%M')
        #                                    }})


    # daily summary
    mt_summary = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_summary_group',
                                               'start_date': SYSTEM_DATE,
                                               'end_time': {'$exists': True}}).count()
    if mt_summary == 0:
        count = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_summary_group',
                                              'start_date': SYSTEM_DATE}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'manual_trigger_summary_group',
                                        'start_date': SYSTEM_DATE,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count + 1})

        pos_list = list(db.JUP_DB_Manual_Triggers_Module.distinct('pos.City'))
        mtt_summary = []
        for pos in pos_list:
            mtt_summary.append(run_manual_trigger_to_summary.s(pos=pos))

        group1 = group(mtt_summary)
        res1 = group1()
        grp_res1 = res1.get()
        temp_for_manual_trigger_summ_grp = count+1
        check = sending_error_mail(system_date=SYSTEM_DATE, group='manual_trigger_summary_group', db=db, attempt=temp_for_manual_trigger_summ_grp)
        if check == 0:
            db.JUP_DB_Celery_Status.update({'group_name': 'manual_trigger_summary_group',
                                            'start_date': SYSTEM_DATE,
                                            'attempt_number': count + 1},
                                           {'$set': {
                                               'end_date': SYSTEM_DATE,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
            mail_array.append("manual_trigger_summary_group")
        else:
            return

    ## We have to add prorated target/forecast in manual triggers summary collection
    doc = dict()
    cal_cursor = db.JUP_DB_Calendar_Master.find({'duration': {'$ne': None}})
    doc_duration = dict()
    for x in cal_cursor:
        doc_duration[x['combine_column']] = x['duration']

    mt_forcast = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_group_forecast',
                                               'start_date': SYSTEM_DATE,
                                               'end_time': {'$exists': True}}).count()
    if mt_forcast == 0:
        # clearing of existing forecast and target values in Market Review summary collection
        cur_date = datetime.datetime.strptime(SYSTEM_DATE, '%Y-%m-%d')
        # cur_date = datetime.strptime("2018-04-14", '%Y-%m-%d')

        if len(str(cur_date.day)) == 2:
            date = str(cur_date.day)
        else:
            date = '0' + str(cur_date.day)

        if len(str(cur_date.month)) == 2:
            month = str(cur_date.month)
        else:
            month = '0' + str(cur_date.month)
        year = cur_date.year
        this_month = cur_date.month
        next_month = cur_date.month + 1
        str_date = str(year) + '-' + month + '-' + date

        cur_date_1 = cur_date - relativedelta(month=cur_date.month - 1)

        if len(str(cur_date_1.day)) == 2:
            date_1wkLTR = str(cur_date_1.day)
        else:
            date_1wkLTR = '0' + str(cur_date_1.day)

        if len(str(cur_date_1.month)) == 2:
            month_1wkLTR = str(cur_date_1.month)
        else:
            month_1wkLTR = '0' + str(cur_date_1.month)
        year_1wkLTR = cur_date_1.year

        dep_date_start = str(year_1wkLTR) + '-' + str(month_1wkLTR) + '-' + '01'

        # print Calendar.monthrange(cur_date.year, cur_date.month+3)[1]
        # cur_date_end = datetime(cur_date.year, cur_date.month+2, Calendar.monthrange(cur_date.year, cur_date.month+2)[1])
        cur_date_end = cur_date + relativedelta(months=+4)

        if len(str(cur_date_end.day)) == 2:
            date_1wkLTR = str(cur_date_end.day)
        else:
            date_1wkLTR = '0' + str(cur_date_end.day)

        if len(str(cur_date_end.month)) == 2:
            month_1wkLTR = str(cur_date_end.month)
        else:
            month_1wkLTR = '0' + str(cur_date_end.month)

        year_1wkLTR = cur_date_end.year
        # dep_date_end = str(year_1wkLTR) +'-'+ month_1wkLTR + '-'+ date_1wkLTR
        # dep_date_end = datetime.strftime(cur_date_end, '%Y-%m-%d')

        dt_range = cur_date + relativedelta(months=+3)
        dep_date_end = datetime.datetime.strftime(dt_range, '%Y-%m') + '-' + str(
            calendar.monthrange(dt_range.year, dt_range.month)[1])

        db.JUP_DB_Manual_Triggers_Module_Summary.update({'dep_date' : {'$gte' : dep_date_start, '$lte' : dep_date_end}},{
            '$set' : {
                "forecast_pax": 0,
                "forecast_avgFare": 0,
                "forecast_revenue": 0,
                "forecast_pax_1": 0,
                "forecast_avgFare_1": 0,
                "forecast_revenue_1": 0,
                "prorate_forecast_pax": 0,
                "prorate_forecast_revenue": 0,
                "target_pax": 0,
                "target_revenue": 0,
                "target_avgFare": 0,
                "target_pax_1": 0,
                "target_revenue_1": 0,
                "target_avgFare_1": 0,
                "significant_target": False,
                "prorate_target_pax": 0,
                "prorate_target_revenue": 0,
                "prorate_target_pax_1": 0,
                "prorate_target_revenue_1": 0,
                "popular_fare_detail" : None
            }
        }, multi = True)
        print dep_date_end, dep_date_start, str_date


        count = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_group_forecast',
                                              'start_date': SYSTEM_DATE}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'manual_trigger_group_forecast',
                                        'start_date': SYSTEM_DATE,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count + 1})

        # Forecast
        mtt = []
        doc = db.JUP_DB_Forecast_OD.distinct('snap_date')
        str_date = doc[len(doc) - 1]
        pos_list = list(db.JUP_DB_Forecast_OD.distinct('pos', {'snap_date': str_date}))
        for pos in pos_list:
            mtt.append(run_manual_trigger_forecast.s(pos=pos, snap_date=str_date, doc=doc_duration))
        group2 = group(mtt)
        res2 = group2()
        grp_res2 = res2.get()
        temp_for_manual_trigger_group_forcast = count+1
        check = sending_error_mail(system_date=SYSTEM_DATE, group='manual_trigger_group_forecast', db=db, attempt=temp_for_manual_trigger_group_forcast)
        if check == 0:
            db.JUP_DB_Celery_Status.update({'group_name': 'manual_trigger_group_forecast',
                                            'start_date': SYSTEM_DATE,
                                            'attempt_number': count + 1},
                                           {'$set': {
                                               'end_date': SYSTEM_DATE,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
            mail_array.append("run_manual_trigger_forecast")
        else:
            return
    mt_target = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_group_target',
                                              'start_date': SYSTEM_DATE,
                                              'end_time': {'$exists': True}}).count()
    if mt_target == 0:
        count = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_group_target',
                                              'start_date': SYSTEM_DATE}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'manual_trigger_group_target',
                                        'start_date': SYSTEM_DATE,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count + 1})

        # Target
        doc = db.JUP_DB_Target_OD.distinct('snap_date')
        str_date = doc[len(doc) - 1]
        pos_list = list(db.JUP_DB_Target_OD.distinct('pos', {'snap_date': str_date}))
        mtt = []
        for i in pos_list:
            mtt.append(run_manual_trigger_target.s(pos=i, snap_date=str_date, doc=doc_duration))
        group3 = group(mtt)
        res3 = group3()
        grp_res3 = res3.get()
        temp_for_manual_trigger_group_target = count+1
        check = sending_error_mail(system_date=SYSTEM_DATE, group='manual_trigger_group_target', db=db, attempt=temp_for_manual_trigger_group_target)
        if check == 0:
            db.JUP_DB_Celery_Status.update({'group_name': 'manual_trigger_group_target',
                                            'start_date': SYSTEM_DATE,
                                            'attempt_number': count + 1},
                                           {'$set': {
                                               'end_date': SYSTEM_DATE,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
            mail_array.append("run_manual_trigger_target")
        else:
            return
    ## send message for updating LFF and HFF in daily summary and weekly monthly summary
    message = {
        "message": "manual_trigger_" + SYSTEM_DATE
    }
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.basic_publish(exchange='',
                          routing_key='automation_pipeline',
                          body=json.dumps(message))

    mt_summary_TL = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_summary_TL_group',
                                                  'start_date': SYSTEM_DATE,
                                                  'end_time': {'$exists': True}}).count()
    if mt_summary_TL == 0:
        count = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_summary_TL_group',
                                              'start_date': SYSTEM_DATE}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'manual_trigger_summary_TL_group',
                                        'start_date': SYSTEM_DATE,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count + 1})

        pos_list = list(db.JUP_DB_Manual_Triggers_Module.distinct('pos.City'))
        mtt_summary = []
        for pos in pos_list:
            mtt_summary.append(run_manual_trigger_to_summary_TL.s(pos=pos))

        group1 = group(mtt_summary)
        res1 = group1()
        grp_res1 = res1.get()
        temp_for_manual_trigger_summ_TL_group = count+1
        check = sending_error_mail(system_date=SYSTEM_DATE, group='manual_trigger_summary_TL_group', db=db, attempt=temp_for_manual_trigger_summ_TL_group)
        if check == 0:
            db.JUP_DB_Celery_Status.update({'group_name': 'manual_trigger_summary_TL_group',
                                            'start_date': SYSTEM_DATE,
                                            'attempt_number': count + 1},
                                           {'$set': {
                                               'end_date': SYSTEM_DATE,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
            mail_array.append("run_manual_trigger_to_summary_TL")
        else:
            return

    popular_fare = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_summary_popular_fare',
                                                 'start_date': SYSTEM_DATE,
                                                 'end_time': {'$exists': True}}).count()
    if popular_fare == 0:
        count = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_summary_popular_fare',
                                              'start_date': SYSTEM_DATE}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'manual_trigger_summary_popular_fare',
                                        'start_date': SYSTEM_DATE,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count + 1})
        od_list = list(db.JUP_DB_OD_Master.distinct('pseudo_od'))
        numb = 0
        mtt = []
        od_arr = []
        for od in od_list:
            od_arr.append(od)
            if numb == 100:
                mtt.append(run_manual_trigger_summary_popular_fare.s(ods=od_arr))
                od_arr = []
                numb = 0
            else:
                numb = numb + 1
        mtt.append(run_manual_trigger_summary_popular_fare.s(ods=od_arr))
        group3 = group(mtt)
        res3 = group3()
        grp_res3 = res3.get()
        temp_for_manual_trigger_summ_landing_page = count+1
        check = sending_error_mail(system_date=SYSTEM_DATE, group='manual_trigger_summary_popular_fare',
                                   db=db, attempt=temp_for_manual_trigger_summ_landing_page)
        if check == 0:
            db.JUP_DB_Celery_Status.update({'group_name': 'manual_trigger_summary_popular_fare',
                                            'start_date': SYSTEM_DATE,
                                            'attempt_number': count + 1},
                                           {'$set': {
                                               'end_date': SYSTEM_DATE,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})

            mail_array.append("run_manual_trigger_summary_popular_fare")
        else:
            return



    mt_summary_LP = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_summary_to_landing_page_group',
                                                  'start_date': SYSTEM_DATE,
                                                  'end_time': {'$exists': True}}).count()
    if mt_summary_LP == 0:
        count = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_summary_to_landing_page_group',
                                              'start_date': SYSTEM_DATE}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'manual_trigger_summary_to_landing_page_group',
                                        'start_date': SYSTEM_DATE,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count + 1})

        pos_list = list(db.JUP_DB_User.distinct('name', {'cluster': {'$ne': 'network'}, 'active': True}))
        network_user = db.JUP_DB_User.find_one({'cluster': {'$eq': 'network'}, 'active': True})
        pos_list.append(network_user['name'])

        ## Update popular fare along with summary to landing page


        mt_lp = []
        for each_user in pos_list:
            mt_lp.append(run_manual_trigger_summary_to_landing_page.s(user=each_user))
        group1 = group(mt_lp)
        res1 = group1()
        grp_res1 = res1.get()
        temp_for_manual_trigger_summ_landing_page = count+1
        check = sending_error_mail(system_date=SYSTEM_DATE, group='manual_trigger_summary_to_landing_page_group',
                                   db=db, attempt=temp_for_manual_trigger_summ_landing_page)
        if check == 0:
            db.JUP_DB_Celery_Status.update({'group_name': 'manual_trigger_summary_to_landing_page_group',
                                            'start_date': SYSTEM_DATE,
                                            'attempt_number': count + 1},
                                           {'$set': {
                                               'end_date': SYSTEM_DATE,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
            mail_array.append("run_manual_trigger_summary_to_landing_page")

        else:
            return

    ## Landing Pages triggers and tiles
    lp_tr_tile = db.JUP_DB_Celery_Status.find({'group_name': 'landing_page_tile_triggers_group',
                                               'start_date': SYSTEM_DATE,
                                               'end_time': {'$exists': True}}).count()
    if lp_tr_tile == 0:
        count = db.JUP_DB_Celery_Status.find({'group_name': 'landing_page_tile_triggers_group',
                                              'start_date': SYSTEM_DATE}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'landing_page_tile_triggers_group',
                                        'start_date': SYSTEM_DATE,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count + 1})

        user_list = list(db.JUP_DB_User.distinct('name', {'cluster': {'$ne': 'network'}, 'active': True}))
        network_user = db.JUP_DB_User.find_one({'cluster': {'$eq': 'network'}, 'active': True})
        user_list.append(network_user['name'])
        comp = db.JUP_DB_Booking_Class.distinct("comp")
        comp.append('TL')
        lp_Tr = []
        for each_user in user_list:
            for each_comp in comp:
                # print each_user+" "+each_comp
                lp_Tr.append(run_landing_page_tile_triggers.s(each_user, each_comp))

        group1 = group(lp_Tr)
        res1 = group1()
        grp_res1 = res1.get()
        temp_landing_page_tile_triggers_group = count+1
        check = sending_error_mail(system_date=SYSTEM_DATE, group='landing_page_tile_triggers_group', db=db, attempt=temp_landing_page_tile_triggers_group)
        if check==0:
            db.JUP_DB_Celery_Status.update({'group_name': 'landing_page_tile_triggers_group',
                                            'start_date': SYSTEM_DATE,
                                            'attempt_number': count + 1},
                                           {'$set': {
                                               'end_date': SYSTEM_DATE,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
            mail_array.append("run_landing_page_tile_triggers")
        else:
            return
    ## market_significant
    lp_tr_tile = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_WMQY_market_significant',
                                               'start_date': SYSTEM_DATE,
                                               'end_time': {'$exists': True}}).count()
    if lp_tr_tile == 0:
        count = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_WMQY_market_significant',
                                              'start_date': SYSTEM_DATE}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'manual_trigger_WMQY_market_significant',
                                        'start_date': SYSTEM_DATE,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count + 1})

        pos_list = db.JUP_DB_Manual_Triggers_Module_Summary.distinct('pos.City')
        lp_Tr = []
        for each_market in pos_list:
            lp_Tr.append(run_manual_trigger_WMQY_market_significant.s(pos=each_market))
        group1 = group(lp_Tr)
        res1 = group1()
        grp_res1 = res1.get()
        temp_manual_trigger_WMQY_market_significant = count+1
        check = sending_error_mail(system_date=SYSTEM_DATE, group='manual_trigger_WMQY_market_significant', db=db, attempt=temp_manual_trigger_WMQY_market_significant)
        if check==0:
            db.JUP_DB_Celery_Status.update({'group_name': 'manual_trigger_WMQY_market_significant',
                                            'start_date': SYSTEM_DATE,
                                            'attempt_number': count + 1},
                                           {'$set': {
                                               'end_date': SYSTEM_DATE,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
            mail_array.append("run_manual_trigger_WMQY_market_significant")
        else:
            return
    if len(mail_array) > 0:

        mail_group = group([send_exec_stats_mail.s(task_list=mail_array,
                                                   system_date=SYSTEM_DATE,
                                                   subject="Manual Trigger Module Scripts")])()
        print mail_group.get()


if __name__ == '__main__':
    manual_trigger_scripts()
    manual_trigger_wm_scripts()
