77777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777"""
Author: Abhinav Garg
Created with <3
Date Created: 2018-03-07
File Name: atpco_master.py

Master file for all atpco related processing run in celery.

"""
import json
import copy
import numpy as np
import pandas as pd
import datetime
import math
import time
import pika
from jupiter_AI.triggers.common import sending_error_mail
import logging
from jupiter_AI.batch.atpco_automation.Automation_tasks import run_fares_and_stage_1, run_record_0_change, \
    run_record_1_change, \
    run_record_2_cat_3_fn_change, run_record_2_cat_10_change, run_record_2_cat_11_fn_change, \
    run_record_2_cat_14_fn_change, \
    run_record_2_cat_15_fn_change, run_record_2_cat_23_fn_change, run_record_2_cat_25_change, \
    run_record_2_cat_all_change, run_update_stage_2_without_rec1, \
    run_record_4_5, run_record_8_change, run_recseg2, run_recseg3, run_fareladder, run_footnote_coll, \
    run_stage_2_rules, run_yqyr, run_yqyr_changetab, run_yqyr_tab178190 , run_yqyr_tab171198, run_yqyr_secpos, run_yqyr_zoneflag, \
    run_yqyr_s1reverse, run_yqyr_s1change, send_exec_stats_mail, run_temp_fares_collection
from jupiter_AI.batch.data_scripts.Fares_Rules_YQ_and_YR_update import call_update
from jupiter_AI.batch.atpco_automation.od_distance import od_distance
from celery import group, chord, chain
from jupiter_AI import client, JUPITER_DB, SYSTEM_DATE, today, parameters, RABBITMQ_HOST, RABBITMQ_PASSWORD, \
    RABBITMQ_PORT, RABBITMQ_USERNAME, ATPCO_DB, JUPITER_LOGGER, ENV, ATPCO_processing_hour
import rabbitpy
from jupiter_AI.logutils import measure
from jupiter_AI.batch.atpco_automation.temp_fares_collection import get_fares_ods_list

from jupiter_AI.batch.fbmapping_batch.JUP_AI_Batch_Fare_Ladder_Mapping import get_od_list

url = 'amqp://' + RABBITMQ_USERNAME + \
      ":" + RABBITMQ_PASSWORD + \
      "@" + RABBITMQ_HOST + ":" + str(RABBITMQ_PORT) + "/"

db = client[JUPITER_DB]
db1 = client[ATPCO_DB]

@measure(JUPITER_LOGGER)
def time_in_range(start, end, x):
    """Return true if x is in the range [start, end]"""
    if start <= end:
        return start <= x <= end
    else:
        return start <= x or x <= end

@measure(JUPITER_LOGGER)
def yqyr(system_date, file_time):
    st = time.time()
    sysdate = datetime.datetime.strptime(system_date, '%Y-%m-%d')

    file_date = datetime.datetime.strftime(sysdate, "%Y%m%d")

1    coll = db.JUP_DB_ATPCO_Fares_Rules
    od_list = {}
    cxr = coll.distinct('carrier', {'file_date': file_date, 'file_time' : {"$in" : file_time}})
    # cxr=["WY"]

    for i in cxr:
        od_list.update({i: []})
        cod = coll.distinct('OD', {'carrier': i, 'file_date': file_date, 'file_time' : {"$in" : file_time}})
        od_list[i] = cod
        # od_list[i]=(intersection(ods, cod))
        # print od_list[i]

    carrier_od = []
    for carrier in list(od_list.keys()):

        curx = db.JUP_DB_Carrier_hubs.find({"carrier": carrier})
        if curx.count() == 0:
            print "no hub found"
            continue
        for x in curx:
            hub = x["hub"]
            hub_country = x["hub_country"]
            hub_zone = x["hub_zone"]
            hub_area = x["hub_area"]

        for OD in od_list[carrier]:

            origin = OD[:3]
            destination = OD[3:]

            curz = db.JUP_DB_ATPCO_Zone_Master.find({"CITY_CODE": {"$in": [origin]}})
            for j in curz:
                if j["CITY_CODE"] == origin:
                    origin_area = j["CITY_AREA"]
                    origin_zone = j["CITY_ZONE"]
                    origin_country = j["CITY_CNTRY"]
            curz1=db.JUP_DB_ATPCO_Zone_Master.find({"CITY_CODE": {"$in": [destination]}})

            for j in curz1:


                if  j["CITY_CODE"] == destination:
                    destination_area = j["CITY_AREA"]
                    destination_zone = j["CITY_ZONE"]
                    destination_country = j["CITY_CNTRY"]


            carrier_od.append(run_yqyr.s(system_date=system_date, file_time=file_time, carrier=[carrier], OD=[OD],hub= hub,hub_area= hub_area,hub_country= hub_country,hub_zone= hub_zone, origin_area=origin_area,
                                         origin_country=origin_country, origin_zone=origin_zone, destination_area=destination_area,destination_country= destination_country,destination_zone= destination_zone))


    group_y = group(carrier_od)
    resy = group_y()
    grpy_res = resy.get()
    print 'yqyr done in', time.time() - st
    # return carrier_od


@measure(JUPITER_LOGGER)
def run_atpco_automation(system_date, file_time):
    st1 = time.time()
    mail_array = []
    count_record_4_5 = db.JUP_DB_Celery_Status.find({'group_name': 'record_4_5_group',
                                                     'start_date': system_date,
                                                     'file_time': file_time,
                                                     'end_time': {'$exists': True}}).count()
    if count_record_4_5 == 0:
        count = db.JUP_DB_Celery_Status.find({'group_name': 'record_4_5_group',
                                              'file_time': file_time,
                                              'start_date': system_date}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'record_4_5_group',
                                        'start_date': system_date,
                                        'file_time': file_time,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count + 1})
        group_1 = group([run_record_4_5.s(system_date=system_date, file_time= file_time), run_yqyr_changetab.s(system_date=system_date, file_time= file_time)])
        res1 = group_1()
        grp1_res = res1.get()
        temp_record_4_5_grp = count + 1
        check = sending_error_mail(system_date=system_date, group='record_4_5_group',
                                   db=db, attempt=temp_record_4_5_grp)
        if check == 0:
            db.JUP_DB_Celery_Status.update({'group_name': 'record_4_5_group',
                                            'start_date': system_date,
                                            'file_time': file_time,
                                            'attempt_number': count + 1},
                                           {'$set': {
                                               'end_date': system_date,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
            mail_array.append("run_record_4_5")
            mail_array.append("run_yqyr_changetab")
            print 'record_4_5 done in', time.time() - st1
        else:
            return

    st2 = time.time()
    count_change_file = db.JUP_DB_Celery_Status.find({'group_name': 'change_file_group',
                                                      'start_date': system_date,
                                                      'file_time': file_time,
                                                      'end_time': {'$exists': True}}).count()
    if count_change_file == 0:
        count = db.JUP_DB_Celery_Status.find({'group_name': 'change_file_group',
                                              'start_date': system_date, "file_time" : file_time}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'change_file_group',
                                        'start_date': system_date,
                                        'file_time': file_time,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count + 1})
        group_2 = group([run_record_8_change.s(system_date=system_date, file_time= file_time),
                         run_record_0_change.s(system_date=system_date, file_time= file_time),
                         run_record_1_change.s(system_date=system_date, file_time= file_time),
                         run_record_2_cat_all_change.s(system_date=system_date, file_time= file_time),
                         run_record_2_cat_25_change.s(system_date=system_date, file_time= file_time),
                         run_record_2_cat_10_change.s(system_date=system_date, file_time= file_time),
                         run_record_2_cat_3_fn_change.s(system_date=system_date, file_time= file_time),
                         run_record_2_cat_11_fn_change.s(system_date=system_date, file_time= file_time),
                         run_record_2_cat_14_fn_change.s(system_date=system_date, file_time= file_time),
                         run_record_2_cat_15_fn_change.s(system_date=system_date, file_time= file_time),
                         run_record_2_cat_23_fn_change.s(system_date=system_date, file_time= file_time),
                         run_yqyr_tab178190.s(system_date=system_date, file_time= file_time),
                         run_yqyr_tab171198.s(system_date=system_date, file_time= file_time),
                         run_yqyr_secpos.s(system_date=system_date, file_time= file_time)
                         ])
        res2 = group_2()
        grp2_res = res2.get()
        temp_change_file_group = count + 1
        check = sending_error_mail(system_date=system_date, group='change_file_group',
                                   db=db, attempt=temp_change_file_group)
        if check == 0:
            db.JUP_DB_Celery_Status.update({'group_name': 'change_file_group',
                                            'start_date': system_date,
                                            'file_time': file_time,
                                            'attempt_number': count + 1},
                                           {'$set': {
                                               'end_date': system_date,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
            mail_array = mail_array + ["run_record_8_change",
                                       "run_record_0_change",
                                       "run_record_1_change",
                                       "run_record_2_cat_all_change.",
                                       "run_record_2_cat_25_change",
                                       "run_record_2_cat_10_change",
                                       "run_record_2_cat_3_fn_change",
                                       "run_record_2_cat_11_fn_change",
                                       "run_record_2_cat_14_fn_change",
                                       "run_record_2_cat_15_fn_change",
                                       "run_record_2_cat_23_fn_change",
                                       "run_yqyr_tab178190",
                                       "run_yqyr_tab171198",
                                       "run_yqyr_secpos"]
            print 'change_file done in', time.time() - st2
        else:
            return


def run_at_file_date_change(system_date, file_time):
    st1 = time.time()
    mail_array = []
    st3 = time.time()
    count_recseg = db.JUP_DB_Celery_Status.find({'group_name': 'recseg_group',
                                                 'start_date': system_date,
                                                 'file_time': file_time,
                                                 'end_time': {'$exists': True}}).count()
    if count_recseg == 0:
        db.Temp_fzDB_tbl_001.remove({})
        count = db.JUP_DB_Celery_Status.find({'group_name': 'recseg_group',
                                              'file_time': file_time,
                                              'start_date': system_date}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'recseg_group',
                                        'start_date': system_date,
                                        'file_time': file_time,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count + 1})
        group_3 = group([run_recseg3.s(system_date=system_date, file_time=file_time), run_recseg2.s(system_date=system_date, file_time=file_time),
                         run_yqyr_zoneflag.s()])
        res3 = group_3()
        grp3_res = res3.get()
        temp_recseg_group = count + 1
        check = sending_error_mail(system_date=system_date, group='recseg_group',
                                   db=db, attempt=temp_recseg_group)
        if check == 0:
            db.JUP_DB_Celery_Status.update({'group_name': 'recseg_group',
                                            'start_date': system_date,
                                            'file_time': file_time,
                                            'attempt_number': count + 1},
                                           {'$set': {
                                               'end_date': system_date,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
            mail_array.append("run_recseg3")
            mail_array.append("run_recseg2")
            mail_array.append("run_yqyr_zoneflag")
            print 'recseg done in', time.time() - st3
        else:
            return

    sty = time.time()
    count_yqyr_s1change = db.JUP_DB_Celery_Status.find({'group_name': 'yqyr_s1change_group',
                                                        'start_date': system_date,
                                                        'file_time': file_time,
                                                        'end_time': {'$exists': True}}).count()
    if count_yqyr_s1change == 0:
        count = db.JUP_DB_Celery_Status.find({'group_name': 'yqyr_s1change_group',
                                              'start_date': system_date, 'file_time': file_time}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'yqyr_s1change_group',
                                        'start_date': system_date,
                                        'file_time': file_time,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count + 1})
        group_y1 = group([run_yqyr_s1change.s(system_date=system_date, file_time= file_time)])
        resy1 = group_y1()
        grpy_res = resy1.get()
        temp_yqyr_slchange_group = count + 1
        check = sending_error_mail(system_date=system_date, group='yqyr_s1change_group',
                                   db=db, attempt=temp_yqyr_slchange_group)
        if check == 0:
            db.JUP_DB_Celery_Status.update({'group_name': 'yqyr_s1change_group',
                                            'start_date': system_date,
                                            'file_time': file_time,
                                            'attempt_number': count + 1},
                                           {'$set': {
                                               'end_date': system_date,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
            mail_array.append("run_yqyr_s1change")
            print 's1change done in', time.time() - sty
        else:
            return

    sty2 = time.time()
    count_yqyr_s1reverse = db.JUP_DB_Celery_Status.find({'group_name': 'yqyr_s1reverse_group',
                                                         'start_date': system_date,
                                                         'file_time': file_time,
                                                         'end_time': {'$exists': True}}).count()
    if count_yqyr_s1reverse == 0:
        count = db.JUP_DB_Celery_Status.find({'group_name': 'yqyr_s1reverse_group',
                                              'file_time': file_time,
                                              'start_date': system_date}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'yqyr_s1reverse_group',
                                        'start_date': system_date,
                                        'file_time': file_time,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count + 1})
        group_y2 = group([run_yqyr_s1reverse.s(system_date, file_time)])
        resy2 = group_y2()
        grpy2_res = resy2.get()
        temp_yqyr_slreverse_group = count + 1
        check = sending_error_mail(system_date=system_date, group='yqyr_s1reverse_group',
                                   db=db, attempt=temp_yqyr_slreverse_group)
        if check == 0:
            db.JUP_DB_Celery_Status.update({'group_name': 'yqyr_s1reverse_group',
                                            'start_date': system_date,
                                            'file_time': file_time,
                                            'attempt_number': count + 1},
                                           {'$set': {
                                               'end_date': system_date,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
            mail_array.append("run_yqyr_s1reverse")
            print 's1reverse done in', time.time() - sty2
        else:
            return

    st4 = time.time()
    count_stage_1_fares = db.JUP_DB_Celery_Status.find({'group_name': 'stage_1_fares_group',
                                                        'file_time': file_time,
                                                        'start_date': system_date,
                                                        'end_time': {'$exists': True}}).count()
    if count_stage_1_fares == 0:
        count = db.JUP_DB_Celery_Status.find({'group_name': 'stage_1_fares_group',
                                              'file_time': file_time,
                                              'start_date': system_date}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'stage_1_fares_group',
                                        'start_date': system_date,
                                        'file_time': file_time,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count + 1})
        if ENV == 'Staging1':
            carriers = ['EK']
            tariffs = ['032']
        else:

            # tariffs_1 = db.JUP_DB_ATPCO_Fares_Rules.distinct('tariff_code')
            # carriers_1 = db.JUP_DB_ATPCO_Fares_Rules.distinct('carrier')
            # tariffs_1 = db.JUP_DB_ATPCO_Fares_change.distinct('tariff_code')
            # tariffs_record_0=db1.JUP_DB_ATPCO_Record_0.distinct( "TARIFF",{ "LAST_UPDATED_DATE":system_date})
            # tariffs_record_1=db1.JUP_DB_ATPCO_Record_1.distinct( "TARIFF",{ "LAST_UPDATED_DATE":system_date})
            # tariffs_record_2_cat_all=db1.JUP_DB_ATPCO_Record_2_Cat_All.distinct( "TARIFF",{ "LAST_UPDATED_DATE":system_date})
            # tariffs_record_2_cat_3_fn=db1.JUP_DB_ATPCO_Record_2_Cat_03_FN.distinct( "TARIFF",{ "LAST_UPDATED_DATE":system_date})
            # tariffs_record_2_cat_11_fn=db1.JUP_DB_ATPCO_Record_2_Cat_11_FN.distinct( "TARIFF",{ "LAST_UPDATED_DATE":system_date})
            # tariffs_record_2_cat_14_fn=db1.JUP_DB_ATPCO_Record_2_Cat_14_FN.distinct( "TARIFF",{ "LAST_UPDATED_DATE":system_date})
            # tariffs_record_2_cat_15_fn=db1.JUP_DB_ATPCO_Record_2_Cat_15_FN.distinct( "TARIFF",{ "LAST_UPDATED_DATE":system_date})l
            # tariffs_record_2_cat_23_fn=db1.JUP_DB_ATPCO_Record_2_Cat_23_FN.distinct( "TARIFF",{ "LAST_UPDATED_DATE":system_date})
            # tariffs_record_2_cat_10_fn=db1.JUP_DB_ATPCO_Record_2_Cat_10_FN.distinct( "TARIFF",{ "LAST_UPDATED_DATE":system_date})
            # tariffs_record_2_cat_25_fn=db1.JUP_DB_ATPCO_Record_2_Cat_25_FN.distinct( "TARIFF",{ "LAST_UPDATED_DATE":system_date})
            # carrier_record_0=db1.JUP_DB_ATPCO_Record_0.distinct( "CXR_CODE",{ "LAST_UPDATED_DATE":system_date})
            # carrier_record_1=db1.JUP_DB_ATPCO_Record_1.distinct( "CXR_CODE",{ "LAST_UPDATED_DATE":system_date})
            # carrier_record_2_cat_all=db1.JUP_DB_ATPCO_Record_2_Cat_All.distinct( "CXR_CODE",{ "LAST_UPDATED_DATE":system_date})
            # carrier_record_2_cat_3_fn=db1.JUP_DB_ATPCO_Record_2_Cat_03_FN.distinct( "CXR_CODE",{ "LAST_UPDATED_DATE":system_date})
            # carrier_record_2_cat_11_fn=db1.JUP_DB_ATPCO_Record_2_Cat_11_FN.distinct( "CXR_CODE",{ "LAST_UPDATED_DATE":system_date})
            # carrier_record_2_cat_14_fn=db1.JUP_DB_ATPCO_Record_2_Cat_14_FN.distinct( "CXR_CODE",{ "LAST_UPDATED_DATE":system_date})
            # carrier_record_2_cat_15_fn=db1.JUP_DB_ATPCO_Record_2_Cat_15_FN.distinct( "CXR_CODE",{ "LAST_UPDATED_DATE":system_date})
            # carrier_record_2_cat_23_fn=db1.JUP_DB_ATPCO_Record_2_Cat_23_FN.distinct( "CXR_CODE",{ "LAST_UPDATED_DATE":system_date})
            # carrier_record_2_cat_10_fn=db1.JUP_DB_ATPCO_Record_2_Cat_10_FN.distinct( "CXR_CODE",{ "LAST_UPDATED_DATE":system_date})
            # carrier_record_2_cat_25_fn=db1.JUP_DB_ATPCO_Record_2_Cat_25_FN.distinct( "CXR_CODE",{ "LAST_UPDATED_DATE":system_date})

            # carriers_2 = db.JUP_DB_ATPCO_Fares_change.distinct('carrier')

            # tariffs = list(set(tariffs_record_0 + tariffs_record_1+tariffs_record_2_cat_all+tariffs_record_2_cat_3_fn+tariffs_record_2_cat_11_fn+tariffs_record_2_cat_14_fn+tariffs_record_2_cat_15_fn+tariffs_record_2_cat_23_fn+tariffs_record_2_cat_10_fn+tariffs_record_2_cat_25))
            # carriers = list(set(carrier_record_0 + carrier_record_1+carrier_record_2_cat_all+carrier_record_2_cat_3_fn+carrier_record_2_cat_11_fn+carrier_record_2_cat_14_fn+carrier_record_2_cat_15_fn+carrier_record_2_cat_23_fn+carrier_record_2_cat_10_fn+carrier_record_2_cat_25))

            # tariffs.remove('004')
            # carriers.remove('EK')

            # tariffs.insert(0, '004')
            # carriers.insert(0, 'EK')
            record_change = db.JUP_DB_ATPCO_Fares_change.aggregate([
                {"$match" : {
                    "file_date": system_date.replace("-", ""),
                    "file_time": {"$in" : file_time}
                }},
                {"$group": {"_id": {"carrier": "$carrier",
                                    "tariff": "$tariff_code"}}},
                {"$project": {
                    "_id": 0,
                    "carrier": "$_id.carrier",
                    "tariff": "$_id.tariff"
                }}

            ])
            for i in record_change:
                print i
                db.Temp_fzDB_tbl_001.update({"carrier": i["carrier"], "tariff": i["tariff"]},
                                            {"carrier": i["carrier"], "tariff": i["tariff"]}, upsert=True)

            record_0 = db1.JUP_DB_ATPCO_Record_0.aggregate([{"$match": {"LAST_UPDATED_DATE": system_date, "LAST_UPDATED_TIME" : {"$in" : file_time}}},

                                                            {"$group": {
                                                                "_id": {"LAST_UPDATED_DATE": "$LAST_UPDATED_DATE",
                                                                        "carrier": "$CXR_CODE", "tariff": "$TARIFF"}}},
                                                            {"$project": {

                                                                "_id": 0,
                                                                "carrier": "$_id.carrier",
                                                                "tariff": "$_id.tariff"
                                                            }}

                                                            ])
            for i in record_0:
                print i
                db.Temp_fzDB_tbl_001.update(
                    {"carrier": i["carrier"], "tariff": i["tariff"]},
                    {"carrier": i["carrier"], "tariff": i["tariff"]},
                    upsert=True)

            record_1 = db1.JUP_DB_ATPCO_Record_1.aggregate([{"$match": {"LAST_UPDATED_DATE": system_date, "LAST_UPDATED_TIME" : {"$in" : file_time}}},

                                                            {"$group": {
                                                                "_id": {"LAST_UPDATED_DATE": "$LAST_UPDATED_DATE",
                                                                        "carrier": "$CXR_CODE", "tariff": "$TARIFF"}}},
                                                            {"$project": {

                                                                "_id": 0,
                                                                "carrier": "$_id.carrier",
                                                                "tariff": "$_id.tariff"
                                                            }}

                                                            ])
            for i in record_1:
                print i
                db.Temp_fzDB_tbl_001.update(
                    {"carrier": i["carrier"], "tariff": i["tariff"]},
                    {"carrier": i["carrier"], "tariff": i["tariff"]},
                    upsert=True)
            Record_2_Cat_All = db1.JUP_DB_ATPCO_Record_2_Cat_All.aggregate(
                [{"$match": {"LAST_UPDATED_DATE": system_date, "LAST_UPDATED_TIME" : {"$in" : file_time}}},

                 {"$group": {"_id": {
                     "LAST_UPDATED_DATE": "$LAST_UPDATED_DATE",
                     "carrier": "$CXR_CODE",
                     "tariff": "$TARIFF"}}},
                 {"$project": {

                     "_id": 0,
                     "carrier": "$_id.carrier",
                     "tariff": "$_id.tariff"
                 }}

                 ])
            for i in Record_2_Cat_All:
                print i
                db.Temp_fzDB_tbl_001.update(
                    {"carrier": i["carrier"], "tariff": i["tariff"]},
                    {"carrier": i["carrier"], "tariff": i["tariff"]},
                    upsert=True)
            Record_2_Cat_03_FN = db1.JUP_DB_ATPCO_Record_2_Cat_03_FN.aggregate(
                [{"$match": {"LAST_UPDATED_DATE": system_date, "LAST_UPDATED_TIME" : {"$in" : file_time}}},

                 {"$group": {"_id": {
                     "LAST_UPDATED_DATE": "$LAST_UPDATED_DATE",
                     "carrier": "$CXR_CODE",
                     "tariff": "$TARIFF"}}},
                 {"$project": {

                     "_id": 0,
                     "carrier": "$_id.carrier",
                     "tariff": "$_id.tariff"
                 }}

                 ])
            for i in Record_2_Cat_03_FN:
                print i
                db.Temp_fzDB_tbl_001.update(
                    {"carrier": i["carrier"], "tariff": i["tariff"]},
                    {"carrier": i["carrier"], "tariff": i["tariff"]}, upsert=True)

            Record_2_Cat_11_FN = db1.JUP_DB_ATPCO_Record_2_Cat_11_FN.aggregate(
                [{"$match": {"LAST_UPDATED_DATE": system_date, "LAST_UPDATED_TIME" : {"$in" : file_time}}},

                 {"$group": {"_id": {
                     "LAST_UPDATED_DATE": "$LAST_UPDATED_DATE",
                     "carrier": "$CXR_CODE",
                     "tariff": "$TARIFF"}}},
                 {"$project": {

                     "_id": 0,
                     "carrier": "$_id.carrier",
                     "tariff": "$_id.tariff"
                 }}

                 ])
            for i in Record_2_Cat_11_FN:
                print i
                db.Temp_fzDB_tbl_001.update(
                    {"carrier": i["carrier"], "tariff": i["tariff"]},
                    {"carrier": i["carrier"], "tariff": i["tariff"]}, upsert=True)

            Record_2_Cat_14_FN = db1.JUP_DB_ATPCO_Record_2_Cat_14_FN.aggregate(
                [{"$match": {"LAST_UPDATED_DATE": system_date, "LAST_UPDATED_TIME" : {"$in" : file_time}}},

                 {"$group": {"_id": {
                     "LAST_UPDATED_DATE": "$LAST_UPDATED_DATE",
                     "carrier": "$CXR_CODE",
                     "tariff": "$TARIFF"}}},
                 {"$project": {

                     "_id": 0,
                     "carrier": "$_id.carrier",
                     "tariff": "$_id.tariff"
                 }}

                 ])
            for i in Record_2_Cat_14_FN:
                print i
                db.Temp_fzDB_tbl_001.update(
                    {"carrier": i["carrier"], "tariff": i["tariff"]},
                    {"carrier": i["carrier"], "tariff": i["tariff"]}, upsert=True)

            Record_2_Cat_15_FN = db1.JUP_DB_ATPCO_Record_2_Cat_15_FN.aggregate(
                [{"$match": {"LAST_UPDATED_DATE": system_date, "LAST_UPDATED_TIME" : {"$in" : file_time}}},

                 {"$group": {"_id": {
                     "LAST_UPDATED_DATE": "$LAST_UPDATED_DATE",
                     "carrier": "$CXR_CODE", "tariff": "$TARIFF"}}},
                 {"$project": {

                     "_id": 0,
                     "carrier": "$_id.carrier",
                     "tariff": "$_id.tariff"
                 }}

                 ])
            for i in Record_2_Cat_15_FN:
                print i
                db.Temp_fzDB_tbl_001.update(
                    {"carrier": i["carrier"], "tariff": i["tariff"]},
                    {"carrier": i["carrier"], "tariff": i["tariff"]}, upsert=True)

            Record_2_Cat_23_FN = db1.JUP_DB_ATPCO_Record_2_Cat_23_FN.aggregate(
                [{"$match": {"LAST_UPDATED_DATE": system_date, "LAST_UPDATED_TIME" : {"$in" : file_time}}},

                 {"$group": {"_id": {
                     "LAST_UPDATED_DATE": "$LAST_UPDATED_DATE",
                     "carrier": "$CXR_CODE", "tariff": "$TARIFF"}}},
                 {"$project": {

                     "_id": 0,
                     "carrier": "$_id.carrier",
                     "tariff": "$_id.tariff"
                 }}

                 ])
            for i in Record_2_Cat_23_FN:
                print i
                db.Temp_fzDB_tbl_001.update(
                    {"carrier": i["carrier"], "tariff": i["tariff"]},
                    {"carrier": i["carrier"], "tariff": i["tariff"]}, upsert=True)
            Record_2_Cat_10 = db1.JUP_DB_ATPCO_Record_2_Cat_10.aggregate(
                [{"$match": {"LAST_UPDATED_DATE": system_date, "LAST_UPDATED_TIME" : {"$in" : file_time}}},

                 {"$group": {"_id": {
                     "LAST_UPDATED_DATE": "$LAST_UPDATED_DATE",
                     "carrier": "$CXR_CODE", "tariff": "$TARIFF"}}},
                 {"$project": {
                     "_id": 0,
                     "carrier": "$_id.carrier",
                     "tariff": "$_id.tariff"
                 }}

                 ])
            for i in Record_2_Cat_10:
                print i
                db.Temp_fzDB_tbl_001.update(
                    {"carrier": i["carrier"], "tariff": i["tariff"]},
                    {"carrier": i["carrier"], "tariff": i["tariff"]}, upsert=True)
            Record_2_Cat_25 = db1.JUP_DB_ATPCO_Record_2_Cat_25.aggregate(
                [{"$match": {"LAST_UPDATED_DATE": system_date, "LAST_UPDATED_TIME" : {"$in" : file_time}}},

                 {"$group": {"_id": {
                     "LAST_UPDATED_DATE": "$LAST_UPDATED_DATE",
                     "carrier": "$CXR_CODE", "tariff": "$TARIFF"}}},
                 {"$project": {

                     "_id": 0,
                     "carrier": "$_id.carrier",
                     "tariff": "$_id.tariff"
                 }}

                 ])
            for i in Record_2_Cat_25:
                print i
                db.Temp_fzDB_tbl_001.update(
                    {"carrier": i["carrier"], "tariff": i["tariff"]},
                    {"carrier": i["carrier"], "tariff": i["tariff"]}, upsert=True)
        carrier_tariff = []
        coll = db.Temp_fzDB_tbl_001
        od_list = {}
        cxr = coll.distinct('carrier')
        for carrier in cxr:
            cod = coll.distinct('tariff', {'carrier': carrier})
            for tariff in cod:
                carrier_tariff.append(run_fares_and_stage_1.s(system_date=system_date,
                                                              file_time = file_time,
                                                              carrier_list=[carrier],
                                                              tariff_list=[tariff]))

        group_4 = group(carrier_tariff)
        res4 = group_4()
        grp4_res = res4.get()
        temp_stage_1_fares_group = count + 1
        check = sending_error_mail(system_date=system_date, group='stage_1_fares_group',
                                   db=db, attempt=temp_stage_1_fares_group)
        if check == 0:
            # call_update(db)
            od_distance(client, system_date)

            print 'fares stage 1 done in', time.time() - st4
        else:
            return
        # st9 = time.time()
        # print 'getting ids of fares shortlisted for rules'
        # ids = []
        # counter_batch = 0
        # count = 0
        # stage_2 = []
        # cur = db.JUP_DB_ATPCO_Fares_Rules.find({'used': 0}, {'_id': 1})
        # for i in cur:
        #     if counter_batch == 100:
        #         print 'appended', count
        #         stage_2.append(run_stage_2_rules.s(system_date=system_date, id_list=ids))
        #         ids = []
        #         ids.append(str(i['_id']))
        #         counter_batch = 1
        #     else:
        #         ids.append(str(i['_id']))
        #         counter_batch += 1
        #     count += 1
        # if counter_batch > 0:
        #     print 'appended', count
        #     stage_2.append(run_stage_2_rules.s(system_date=system_date, id_list=ids))
        #
        # print 'got ids in', time.time() - st9
        #
        # group_5 = group(stage_2)
        # res5 = group_5()
        # grp5_res = res5.get()
        #
        # print 'stage 2 rules done in', time.time() - st9

        # wait = 0
        # while True:
        #     if db.JUP_DB_ATPCO_Fares_Rules.find({'used': 0}).count() == 0 or wait == 7200:
        #         print 'fares and rules done in', time.time() - st4
        #         print 'Count of leftover fares:', db.JUP_DB_ATPCO_Fares_Rules.find({'used': 0}).count()

        db.JUP_DB_Celery_Status.update({'group_name': 'stage_1_fares_group',
                                        'start_date': system_date,
                                        'file_time': file_time,
                                        'attempt_number': count + 1},
                                       {'$set': {
                                           'end_date': system_date,
                                           'end_time': datetime.datetime.now().strftime('%H:%M')
                                       }})
        mail_array.append("run_fares_and_stage_1")

            #     break
            # else:
            #     time.sleep(90)
            #     wait += 90
            #     print 'Waiting Since:', wait, 'secs'

    print 'total time taken', time.time() - st1


def run_stage_2(system_date, file_time):
    sty2 = time.time()
    count_stage_2 = db.JUP_DB_Celery_Status.find({'group_name': 'stage_2_rules_group',
                                                         'start_date': system_date,
                                                         'file_time': file_time,
                                                         'end_time': {'$exists': True}}).count()
    if count_stage_2 == 0:
        count_ = db.JUP_DB_Celery_Status.find({'group_name': 'stage_2_rules_group',
                                              'file_time': file_time,
                                              'start_date': system_date}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'stage_2_rules_group',
                                        'start_date': system_date,
                                        'file_time': file_time,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count_ + 1})
        st9 = time.time()
        print 'getting ids of fares shortlisted for rules'
        ids = []
        counter_batch = 0
        count = 0
        stage_2 = []
        cur = db.TempSummaryColl.find({"used" : 0}, {'_id': 1}, no_cursor_timeout=True)
        for i in cur:
            if counter_batch == 100:
                print 'appended', count
                stage_2.append(run_stage_2_rules.s(system_date=system_date,file_time = file_time, id_list=ids))
                ids = []
                ids.append(str(i['_id']))
                counter_batch = 1
            else:
                ids.append(str(i['_id']))
                counter_batch += 1
            count += 1
        if counter_batch > 0:
            print 'appended', count
            stage_2.append(run_stage_2_rules.s(system_date=system_date,file_time = file_time, id_list=ids))

        print 'got ids in', time.time() - st9
        group_5 = group(stage_2)
        res5 = group_5()
        grp5_res = res5.get()

        ## Update push to Main collection
        ids = []
        counter_batch = 0
        count = 0
        stage_2 = []
        cur = db.TempSummaryColl.find({"used": {"$eq": None}}, {'_id': 1}, no_cursor_timeout=True)
        for i in cur:
            if counter_batch == 100:
                print 'appended', count
                stage_2.append(run_update_stage_2_without_rec1.s(id_list=ids))
                ids = []
                ids.append(str(i['_id']))
                counter_batch = 1
            else:
                ids.append(str(i['_id']))
                counter_batch += 1
            count += 1
        if counter_batch > 0:
            print 'appended', count
            stage_2.append(run_update_stage_2_without_rec1.s(id_list=ids))

        print 'got ids in', time.time() - st9
        group_5 = group(stage_2)
        res5 = group_5()
        grp5_res = res5.get()
        print 'stage 2 rules done in', time.time() - st9


        wait = 0
        while True:
            if db.JUP_DB_ATPCO_Fares_Rules.find({'used': 0}).count() == 0 or wait == 7200:
                print 'fares and rules done in', time.time() - sty2
                print 'Count of leftover fares:', db.JUP_DB_ATPCO_Fares_Rules.find({'used': 0}).count()

                db.JUP_DB_Celery_Status.update({'group_name': 'stage_2_rules_group',
                                                'start_date': system_date,
                                                'file_time': file_time,
                                                'attempt_number': count_ + 1},
                                               {'$set': {
                                                   'end_date': system_date,
                                                   'end_time': datetime.datetime.now().strftime('%H:%M')
                                               }})
                mail_array.append("run_stage_2_rules")

                break
            else:
                time.sleep(90)
                wait += 90
                print 'Waiting Since:', wait, 'secs'
        cur = db.TempSummaryColl.remove({})

    st5 = time.time()
    count_yqyr = db.JUP_DB_Celery_Status.find({'group_name': 'yqyr_process_group',
                                               'start_date': system_date,
                                               'file_time': file_time,
                                               'end_time': {'$exists': True}}).count()
    if count_yqyr == 0:
        count = db.JUP_DB_Celery_Status.find({'group_name': 'yqyr_process_group',
                                              'file_time': file_time,
                                              'start_date': system_date}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'yqyr_process_group',
                                        'start_date': system_date,
                                        'file_time': file_time,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count + 1})
        yqyr(system_date, file_time=file_time)
        # group_yq = group([run_recseg3.s(system_date=system_date), run_recseg2.s(system_date=system_date),
        #                  run_yqyr_zoneflag.s()])
        # resy3 = group_yq()
        # grp3_res = resy3.get()
        temp_yqyr_process_group = count + 1
        check = sending_error_mail(system_date=system_date, group='yqyr_process_group',
                                   db=db, attempt=temp_yqyr_process_group)
        if check == 0:
            db.JUP_DB_Celery_Status.update({'group_name': 'yqyr_process_group',
                                            'start_date': system_date,
                                            'file_time': file_time,
                                            'attempt_number': count + 1},
                                           {'$set': {
                                               'end_date': system_date,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
            mail_array.append("run_yqyr")

            print 'yqyr done in', time.time() - st5
        else:
            return

        sending_error_mail(system_date=system_date, group='stage_2_rules_group',
                           db=db, attempt=count_)


def run_2_atpco_automation(system_date, file_time):
    st1 = time.time()
    mail_array = []
    sty1 = time.time()
    sysdate = datetime.datetime.strptime(system_date, '%Y-%m-%d')

    file_date = datetime.datetime.strftime(sysdate, "%Y%m%d")

    db.JUP_DB_ATPCO_Fares_Rules.update(
        {'carrier': "FZ", 'file_date': file_date, 'channel': "TA", 'origin_country': "IN"}, {'$set': {"YR": 0.0}},
        multi=True)
    db.JUP_DB_ATPCO_Fares_Rules.update({'carrier': "FZ", 'file_date': file_date, 'channel': "web"},
                                       {'$set': {"YR": 0.0}}, multi=True)
    db.JUP_DB_ATPCO_Fares_Rules.update(
        {'carrier': "FZ", 'file_date': file_date, 'Rule_id': "GP08", 'private_fare': True}, {'$set': {"YR": 0.0}},
        multi=True)

    print 'Publishing atpco completion message to fareId queue'
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    message = json.dumps({'message': 'fareId_' + SYSTEM_DATE})
    channel.basic_publish(exchange='',
                          routing_key='fareId',
                          body=message
                          )
    # print 'Publishing atpco completion message to lff_hff_group'
    # connection = pika.BlockingConnection(parameters)
    # channel = connection.channel()
    # message = json.dumps({'message': 'lff_hff_' + SYSTEM_DATE})
    # channel.basic_publish(exchange='',
    #                       routing_key='lff_hff_group',
    #                       body=message
    #                       )

    fn = 0
    fl = 0
    tp = 0
    st5 = time.time()
    count_fl = db.JUP_DB_Celery_Status.find({'group_name': 'fareladder_group',
                                             'start_date': SYSTEM_DATE,
                                             'end_time': {'$exists': True}}).count()
    if count_fl == 0:
        fl = 1
        count_fl = db.JUP_DB_Celery_Status.find({'group_name': 'fareladder_group',
                                                 'start_date': SYSTEM_DATE}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'fareladder_group',
                                        'start_date': SYSTEM_DATE,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count_fl + 1})
        num_of_markets = 500
        list_fl = list()
        od_list = get_od_list(db=db)
        counter = 0
        while counter < len(od_list):
            list_fl.append(run_fareladder.s(od_list[counter: counter + num_of_markets]))
            counter = counter + num_of_markets
        group_7 = group(list_fl)
        res7 = group_7()

    if fl == 1:
        grp7_res = res7.get()
        temp_fareladder_group = count_fl + 1
        check = sending_error_mail(system_date=SYSTEM_DATE, group='fareladder_group',
                                   db=db, attempt=temp_fareladder_group)
        if check == 0:
            db.JUP_DB_Celery_Status.update({'group_name': 'fareladder_group',
                                            'start_date': SYSTEM_DATE,
                                            'attempt_number': count_fl + 1},
                                           {'$set': {
                                               'end_date': SYSTEM_DATE,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
            mail_array.append("run_fareladder")
            print 'fareladder done in', time.time() - st5
        else:
            return

    count_temp_fares = db.JUP_DB_Celery_Status.find({'group_name': 'temp_fares_collection_group',
                                                     'start_date': SYSTEM_DATE,
                                                     'end_time': {'$exists': True}}).count()
    if count_temp_fares == 0:
        tp = 1
        count = db.JUP_DB_Celery_Status.find({'group_name': 'temp_fares_collection_group',
                                              'start_date': SYSTEM_DATE}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'temp_fares_collection_group',
                                        'start_date': SYSTEM_DATE,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count + 1})
        db.temp_fares_triggers.remove({})
        ods = get_fares_ods_list(db=db)
        temp_fares = list()
        num_markets = 100
        counter = 0
        while counter < len(ods):
            temp_fares.append(run_temp_fares_collection.s(od=ods[counter:counter + num_markets]))
            counter = counter + num_markets
        group_8 = group(temp_fares)
        res8 = group_8()
        # group_8 = group([run_temp_fares_collection.s()])
        # res8 = group_8()


    # count_footnote_coll = db.JUP_DB_Celery_Status.find({'group_name': 'available_footnote_group',
    #                                                     'start_date': SYSTEM_DATE,
    #                                                     'end_time': {'$exists': True}}).count()
    # if count_footnote_coll == 0:
    #     fn = 1
    #     count_fn = db.JUP_DB_Celery_Status.find({'group_name': 'available_footnote_group',
    #                                              'start_date': SYSTEM_DATE}).count()
    #     db.JUP_DB_Celery_Status.insert({'group_name': 'available_footnote_group',
    #                                     'start_date': SYSTEM_DATE,
    #                                     'start_time': datetime.datetime.now().strftime('%H:%M'),
    #                                     'attempt_number': count_fn + 1})
    #     group_6 = group([run_footnote_coll.s()])
    #     res6 = group_6()



    # if grp7_res:
    #     print 'Publishing atpco completion message to automation_pipeline queue'
    #     message = json.dumps({'message': 'atpco_'+system_date})
    #     channel.basic_publish(exchange='',
    #                           routing_key='automation_pipeline',
    #                           body=message
    #                           )
    #     print 'Publishing atpco completion message to atpco_fares_rules queue'
    #     message_atpco = json.dumps({"message": "atpco_fares_rules_"+system_date})
    #     channel.basic_publish(exchange='',
    #                           routing_key='atpco_fares_rules',
    #                           body=message_atpco
    #                           )
    #     print "published msg!"

    if tp == 1:
        grp8_res = res8.get()
        temp_temp_fares_collection_group = count + 1
        check = sending_error_mail(system_date=SYSTEM_DATE, group='temp_fares_collection_group',
                                   db=db, attempt=temp_temp_fares_collection_group)
        if check == 0:
            db.JUP_DB_Celery_Status.update({'group_name': 'temp_fares_collection_group',
                                            'start_date': SYSTEM_DATE,
                                            'attempt_number': count + 1},
                                           {'$set': {
                                               'end_date': SYSTEM_DATE,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
            mail_array.append("run_temp_fares_collection")
            print 'temp fares collection done'
        else:
            return

    print 'Publishing atpco completion message to atpco_master queue'
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    message = json.dumps({'message': 'atpco_master_' + SYSTEM_DATE})
    channel.basic_publish(exchange='',
                          routing_key='atpco_master',
                          body=message
                          )

    # if fn == 1:
    #     grp6_res = res6.get()
    #     temp_available_footnote_group = count_fn+1
    #     check = sending_error_mail(system_date=SYSTEM_DATE, group='available_footnote_group',
    #                                db=db, attempt=temp_available_footnote_group)
    #     if check==0:
    #         db.JUP_DB_Celery_Status.update({'group_name': 'available_footnote_group',
    #                                         'start_date': SYSTEM_DATE,
    #                                         'attempt_number': count_fn + 1},
    #                                        {'$set': {
    #                                            'end_date': SYSTEM_DATE,
    #                                            'end_time': datetime.datetime.now().strftime('%H:%M')
    #                                        }})
    #         mail_array.append("run_footnote_coll")
    #         print 'footnotes done in', time.time() - st5
    #     else:
    #         return

    # print 'Publishing final atpco completion message to atpco_master_final queue'
    # connection = pika.BlockingConnection(parameters)
    # channel = connection.channel()
    # message = json.dumps({'message': 'atpco_master_' + SYSTEM_DATE})
    # channel.basic_publish(exchange='',
    #                       routing_key='completed_atpco_master',
    #                       body=message
    #                       )
    # if len(mail_array) > 0:
    #     atpco = client[ATPCO_DB]
    #     system_date_ = datetime.datetime.strptime(system_date, "%Y-%m-%d").strftime("%Y%m%d")
    #     fares_volume = db.JUP_DB_ATPCO_Fares_Rules.find({'file_date': system_date_}).count()
    #
    #     rules_volume = atpco.JUP_DB_ATPCO_Record_1.find({'LAST_UPDATED_DATE': system_date}).count()
    #     rules_volume += atpco.JUP_DB_ATPCO_Record_0.find({'LAST_UPDATED_DATE': system_date}).count()
    #     rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_All.find({'LAST_UPDATED_DATE': system_date}).count()
    #     rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_10.find({'LAST_UPDATED_DATE': system_date}).count()
    #     rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_25.find({'LAST_UPDATED_DATE': system_date}).count()
    #     rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_03_FN.find({'LAST_UPDATED_DATE': system_date}).count()
    #     rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_11_FN.find({'LAST_UPDATED_DATE': system_date}).count()
    #     rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_14_FN.find({'LAST_UPDATED_DATE': system_date}).count()
    #     rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_15_FN.find({'LAST_UPDATED_DATE': system_date}).count()
    #     rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_23_FN.find({'LAST_UPDATED_DATE': system_date}).count()
    #
    #     mail_group = group([send_exec_stats_mail.s(task_list=mail_array,
    #                                                system_date=SYSTEM_DATE,
    #                                                subject="ATPCO Fares Rules",
    #                                                fares_vol=fares_volume,
    #                                                rules_vol=rules_volume)])()
    #     print mail_group.get()
    print 'total time taken', time.time() - st1


if __name__ == '__main__':
    file_time = "00"
    with rabbitpy.Connection(url) as conn:
        mail_array = []
        list_file_time_batch = []
        hours = []
        hours_str = []
        check_date_change = None
        list_hour = []
        with conn.channel() as channel:
            queue = rabbitpy.Queue(channel, 'atpco_connector')
            print "waiting for messages from atpco_connector queue"
            for message in queue:

                received_msg = json.loads(message.body.decode('utf8'))
                print "Received correct message from atpco_connector: ", received_msg
                print 'Starting atpco_automation'
                TODAY_ = SYSTEM_DATE
				File_Time=TIME
                ## Control check for today or yester day file date only we have to run by today
                if received_msg[ 'file_time'] == Time or received_msg['file_date'] == TODAY_:

                    #---------------------------------------------------------------------------------------
                    ## Daily run only
                    try:
                        message.ack()
                        SYSTEM_DATE_1 = received_msg['file_date']
                        file_time = received_msg['file_time']
                        run_atpco_automation(SYSTEM_DATE_1, file_time)
                        run_2_atpco_automation(SYSTEM_DATE_1, file_time)
                        atpco = client[ATPCO_DB]
                        system_date_ = datetime.datetime.strptime(SYSTEM_DATE_1, "%Y-%m-%d").strftime("%Y%m%d")
                        fares_volume = db.JUP_DB_ATPCO_Fares_Rules.find(
                            {'file_date': system_date_, "file_time": {'$in': hours_str}}).count()
                        rules_volume = atpco.JUP_DB_ATPCO_Record_1.find(
                            {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                        rules_volume += atpco.JUP_DB_ATPCO_Record_0.find(
                            {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                        rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_All.find(
                            {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                        rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_10.find(
                            {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                        rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_25.find(
                            {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                        rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_03_FN.find(
                            {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                        rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_11_FN.find(
                            {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                        rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_14_FN.find(
                            {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                        rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_15_FN.find(
                            {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                        rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_23_FN.find(
                            {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                        mail_group = group([send_exec_stats_mail.s(task_list=mail_array,
                                                                   system_date=SYSTEM_DATE,
                                                                   subject="ATPCO Fares Rules",
                                                                   fares_vol=fares_volume,
                                                                   rules_vol=rules_volume)])()
                        print mail_group.get()
    
                        break
                    except KeyError:
                        fr_date = datetime.datetime.strptime(SYSTEM_DATE_1, '%Y-%m-%d')
                        if (fr_date.weekday() == 6 or fr_date.weekday() == 5):
                            message.ack()
                            file_time = "17"
                            run_atpco_automation(SYSTEM_DATE_1, file_time)
                            run_2_atpco_automation(SYSTEM_DATE_1, file_time)
                            atpco = client[ATPCO_DB]
                            system_date_ = datetime.datetime.strptime(SYSTEM_DATE_1, "%Y-%m-%d").strftime("%Y%m%d")
                            fares_volume = db.JUP_DB_ATPCO_Fares_Rules.find(
                                {'file_date': system_date_, "file_time": {'$in': hours_str}}).count()
                            rules_volume = atpco.JUP_DB_ATPCO_Record_1.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_0.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_All.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_10.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_25.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_03_FN.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_11_FN.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_14_FN.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_15_FN.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_23_FN.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            mail_group = group([send_exec_stats_mail.s(task_list=mail_array,
                                                                       system_date=SYSTEM_DATE,
                                                                       subject="ATPCO Fares Rules",
                                                                       fares_vol=fares_volume,
                                                                       rules_vol=rules_volume)])()
                            print mail_group.get()
    
                            break
                        else:
                            message.ack()
                            file_time = "20"
                            run_atpco_automation(SYSTEM_DATE_1, file_time)
                            run_2_atpco_automation(SYSTEM_DATE_1, file_time)
                            atpco = client[ATPCO_DB]
                            system_date_ = datetime.datetime.strptime(SYSTEM_DATE_1, "%Y-%m-%d").strftime("%Y%m%d")
                            fares_volume = db.JUP_DB_ATPCO_Fares_Rules.find(
                                {'file_date': system_date_, "file_time": {'$in': hours_str}}).count()
                            rules_volume = atpco.JUP_DB_ATPCO_Record_1.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_0.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_All.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_10.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_25.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_03_FN.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_11_FN.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_14_FN.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_15_FN.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_23_FN.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            mail_group = group([send_exec_stats_mail.s(task_list=mail_array,
                                                                       system_date=SYSTEM_DATE,
                                                                       subject="ATPCO Fares Rules",
                                                                       fares_vol=fares_volume,
                                                                       rules_vol=rules_volume)])()
                            print mail_group.get()
    
                            break

                    

                    ## Hourly processing
                    '''
                    # ---------------------------------------------------------------------------------------
                        ## decision making of what time entire process and what time part of the script should run
                        ##
                    # if (int(file_time) >= int(ATPCO_processing_hour['run_1']['start']) and int(file_time) <= int(
                    #         ATPCO_processing_hour['run_1']['end'])):

                    message.ack()
                    SYSTEM_DATE_1 = received_msg['file_date']
                    file_time = received_msg['file_time']
                    list_file_time_batch.append(file_time)
                    eachdep = datetime.strptime("2019-09-15", '%Y-%m-%d')
                    eachdep.isoweekday()
                    if (time_in_range(datetime.time(int(ATPCO_processing_hour['run_0']['start']),0,0), datetime.time(int(ATPCO_processing_hour['run_0']['end']),0,0), datetime.time(int(file_time), 0, 0))):
                        hours.append(int(file_time))
                        hours_str.append(file_time)
                        run_atpco_automation(SYSTEM_DATE_1, file_time)

                        if check_date_change == None or check_date_change == SYSTEM_DATE_1:
                            check_date_change = copy.deepcopy(SYSTEM_DATE_1)
                            list_hour.append(file_time)

                        else:
                            if len(list_hour) > 0:
                                run_at_file_date_change(check_date_change, list_hour)
                                check_date_change = copy.deepcopy(SYSTEM_DATE_1)
                                list_hour.clear()
                                list_hour.append(file_time)

                        # message.ack()
                    elif (time_in_range(datetime.time(int(ATPCO_processing_hour['run_1']['start']),0,0), datetime.time(int(ATPCO_processing_hour['run_1']['end']),0,0), datetime.time(int(file_time), 0, 0))):
                        hours.append(int(file_time))
                        hours_str.append(file_time)
                        run_atpco_automation(SYSTEM_DATE_1, file_time)
                        list_hour.append(file_time)
                        # message.ack()
                    elif (time_in_range(datetime.time(int(ATPCO_processing_hour['run_2']['start']), 0, 0), datetime.time(int(ATPCO_processing_hour['run_2']['end']), 0, 0), datetime.time(int(file_time), 0, 0))):
                        hours.append(int(file_time))
                    run_atpco_automation(SYSTEM_DATE_1, file_time)
                        list_hour.append(file_time)
                        # message.ack()
                    if(int(file_time) == int(ATPCO_processing_hour['run_2']['end'])):
                        run_at_file_date_change(SYSTEM_DATE_1, list_hour)
                        run_stage_2(SYSTEM_DATE_1, list_file_time_batch)
                        run_2_atpco_automation(SYSTEM_DATE_1, file_time)
                        if len(mail_array) > 0:
                            atpco = client[ATPCO_DB]
                            system_date_ = datetime.datetime.strptime(SYSTEM_DATE_1, "%Y-%m-%d").strftime("%Y%m%d")
                            fares_volume = db.JUP_DB_ATPCO_Fares_Rules.find(
                                {'file_date': system_date_, "file_time": {'$in': hours_str}}).count()
                            rules_volume = atpco.JUP_DB_ATPCO_Record_1.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_0.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_All.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_10.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_25.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_03_FN.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_11_FN.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_14_FN.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_15_FN.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_23_FN.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            mail_group = group([send_exec_stats_mail.s(task_list=mail_array,
                                                                       system_date=SYSTEM_DATE,
                                                                       subject="ATPCO Fares Rules",
                                                                       fares_vol=fares_volume,
                                                                       rules_vol=rules_volume)])()
                            print mail_group.get()
                        break
                    elif(int(file_time) == int(ATPCO_processing_hour['run_1']['end'])):
                        run_at_file_date_change(SYSTEM_DATE_1, list_hour)
                        run_stage_2(SYSTEM_DATE_1, list_file_time_batch)
                        if len(mail_array) > 0:
                            atpco = client[ATPCO_DB]
                            system_date_ = datetime.datetime.strptime(SYSTEM_DATE_1, "%Y-%m-%d").strftime("%Y%m%d")
                            fares_volume = db.JUP_DB_ATPCO_Fares_Rules.find(
                                {'file_date': system_date_, "file_time": {'$in': hours_str}}).count()
                            rules_volume = atpco.JUP_DB_ATPCO_Record_1.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_0.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_All.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_10.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_25.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_03_FN.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_11_FN.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_14_FN.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_15_FN.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_23_FN.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            mail_group = group([send_exec_stats_mail.s(task_list=mail_array,
                                                                       system_date=SYSTEM_DATE,
                                                                       subject="ATPCO Fares Rules",
                                                                       fares_vol=fares_volume,
                                                                       rules_vol=rules_volume)])()
                            print mail_group.get()
                        break
                    elif(int(file_time) == int(ATPCO_processing_hour['run_0']['end'])):
                        run_at_file_date_change(SYSTEM_DATE_1, list_hour)
                        run_stage_2(SYSTEM_DATE_1, list_file_time_batch)
                        if len(mail_array) > 0:
                            atpco = client[ATPCO_DB]
                            system_date_ = datetime.datetime.strptime(SYSTEM_DATE_1, "%Y-%m-%d").strftime("%Y%m%d")
                            fares_volume = db.JUP_DB_ATPCO_Fares_Rules.find(
                                {'file_date': system_date_, "file_time": {'$in': hours_str}}).count()
                            rules_volume = atpco.JUP_DB_ATPCO_Record_1.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_0.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_All.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_10.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_25.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_03_FN.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_11_FN.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_14_FN.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_15_FN.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_23_FN.find(
                                {'LAST_UPDATED_DATE': SYSTEM_DATE_1, "LAST_UPDATED_TIME": {'$in': hours_str}}).count()
                            mail_group = group([send_exec_stats_mail.s(task_list=mail_array,
                                                                       system_date=SYSTEM_DATE,
                                                                       subject="ATPCO Fares Rules",
                                                                       fares_vol=fares_volume,
                                                                       rules_vol=rules_volume)])()
                            print mail_group.get()
                        break
                        
                  '''

                else:
                    print "Received wrong message from atpco_connector: ", received_msg
                    message.ack()

    print 'Done ATPCO for today:', SYSTEM_DATE, file_time
	
	
	 { 'message': 'atpco_connector_2021-03-23', 'file_date': '2021-03-23', u'file_time': '20','system_date': '2021-03-24'}
