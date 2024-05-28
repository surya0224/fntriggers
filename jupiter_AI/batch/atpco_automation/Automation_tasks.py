"""
Author: Abhinav Garg
Created with <3
Date Created: 2018-03-07
File Name: Automation_tasks.py

Celery config file.
Defines all the celery tasks and configuration parameters.

"""

import json
import datetime
import math
import pika
import time
import os
import traceback
import socket
from requests.exceptions import ConnectTimeout,\
    HTTPError,\
    ReadTimeout,\
    SSLError,\
    ConnectionError,\
    Timeout
from jupiter_AI.batch.atpco_automation.daily_log_mail import send_mail, error_mail
from jupiter_AI.batch.data_scripts.Manual_Trigger_FareId import call_fareId
import pymongo
from celery import Celery, group, states
from jupiter_AI.batch.JUP_AI_OD_Capacity.New_Host_Inventory import host_od_helper as get_leg_capacities
from jupiter_AI.batch.JUP_AI_OD_Capacity.Competitor_OD_Capacity import \
    competitor_capacity_helper as get_od_capacity_simple
from jupiter_AI.batch.atpco_automation.fr import run_stage_2, update_stage_2_without_rec1
from jupiter_AI.batch.copper_flights import generate_copper_flights
from jupiter_AI.batch.golden_flights import generate_golden_flights
from jupiter_AI.batch.lowest_filed_fare_batch import update_market
from jupiter_AI.batch.zinc_flights import generate_zinc_flights
from jupiter_AI.batch.atpco_automation.fc import fare_changes
from jupiter_AI.batch.atpco_automation.record_4_5 import record_4_5
from jupiter_AI.batch.atpco_automation.change_file import record_0_change, record_1_change, record_2_cat_3_fn_change, \
    record_2_cat_10_change, \
    record_2_cat_11_fn_change, record_2_cat_14_fn_change, record_2_cat_15_fn_change, record_2_cat_23_fn_change, \
    record_2_cat_25_change, record_2_cat_all_change, record_8_change
from jupiter_AI.batch.atpco_automation.Recseg2 import recseg2
from jupiter_AI.batch.atpco_automation.Recseg3 import recseg3
from jupiter_AI.batch.Comp_Ratings_New import group_prod_rating
from jupiter_AI.batch.Comp_Ratings_New import group_airline_rating
from jupiter_AI.batch.Comp_Ratings_New import capacity_rating_cap
from jupiter_AI.batch.Comp_Ratings_New import capacity_rating_blocktime
from jupiter_AI.batch.Comp_Ratings_New import capacity_rating_freq
from jupiter_AI.batch.Comp_Ratings_New import market_rating_mktshare
from jupiter_AI.batch.Comp_Ratings_New import group_market_rating_Size_of_Market
from jupiter_AI.batch.Comp_Ratings_New import group_market_rating_Growth_of_market3
from jupiter_AI.batch.Comp_Ratings_New import group_market_rating_No_of_competitors
from jupiter_AI.batch.Comp_Ratings_New import Restriction_Final
from jupiter_AI.batch.Comp_Ratings_New import Agentsf1_Final
from jupiter_AI.batch.Comp_Ratings_New import Agility
# from jupiter_AI.batch.Comp_Ratings_New import compute_comp_rats2
# from jupiter_AI.batch.customer_segmentation import Segment_FZ
# from jupiter_AI.batch.Promotions_Scraping import ITA_Scraping_YQYR
# from jupiter_AI.batch.Promotions_Scraping import Etihad_Airlines_Promotions
from jupiter_AI.batch.Promotions_Scraping import EK_new
from jupiter_AI.batch.customer_segmentation import pyspark_sales
from jupiter_AI.batch.customer_segmentation import pyspark_sales_Flown
from jupiter_AI import RABBITMQ_USERNAME, RABBITMQ_PASSWORD, RABBITMQ_HOST, RABBITMQ_PORT, parameters, JUPITER_DB, \
    MONGO_CLIENT_URL, ANALYTICS_MONGO_USERNAME, ANALYTICS_MONGO_PASSWORD, MONGO_SOURCE_DB, SYSTEM_DATE, today, mongo_client, \
    PYTHON_PATH,  JUPITER_DB, ATPCO_DB
from jupiter_AI.batch.fbmapping_batch.JUP_AI_Batch_Fare_Ladder_Mapping import update_comp_fb
from jupiter_AI.batch.fare_rules.Footnote_collection import generate_footnotes_collection
from jupiter_AI.batch.triggers.data_level_triggers.general.run_opp_trend_triggers import run as opp_trend_triggers
from jupiter_AI.batch.triggers.data_level_triggers.general.run_pax_triggers import run as pax_triggers
from jupiter_AI.batch.triggers.data_level_triggers.general.run_revenue_triggers import run as revenue_triggers
from jupiter_AI.batch.triggers.data_level_triggers.general.run_yield_triggers import run as yield_triggers
from jupiter_AI.batch.triggers.data_level_triggers.general.Events_triggers import run as event_triggers
from jupiter_AI.batch.triggers.data_level_triggers.general.run_booking_triggers import run as booking_triggers
from jupiter_AI.triggers.data_change.price.CompPriceChange import run as price_change_triggers
from jupiter_AI.batch.market_characteristics.market_agents import run_market_agents as agents, market_agents_adhoc
from jupiter_AI.batch.market_characteristics.market_channels import run_market_channels as channels, \
    market_channels_adhoc
from jupiter_AI.batch.market_characteristics.market_distributors import run_market_distributors as distributors, \
    market_distributors_adhoc
from jupiter_AI.batch.market_characteristics.market_farebrand import run_market_farebrand as farebrands, \
    market_farebrand_adhoc
from jupiter_AI.batch.market_characteristics.market_flights import run_market_flights as flights, market_flights_adhoc
from jupiter_AI.batch.market_characteristics.market_segments import run_market_segments as segments, \
    market_segments_adhoc
from jupiter_AI.batch.data_scripts.Manual_Trigger_Capacity import main as manual_trigger_capacity
from jupiter_AI.batch.data_scripts.Manual_Trigger_Distance import main as manual_trigger_distance
from jupiter_AI.batch.data_scripts.Manual_Trigger_Forecast import main as manual_trigger_forecast
from jupiter_AI.batch.data_scripts.Manual_Trigger_Module_VLYR_Pax_1 import main as manual_trigger_module_VLYR_pax_1
from jupiter_AI.batch.data_scripts.Manual_Trigger_Target import main as manual_trigger_target
from jupiter_AI.batch.data_scripts.Landing_Page_Tile_Triggers import main as landing_page_tile_triggers
from jupiter_AI.batch.data_scripts.Manual_Trigger_Summary_To_Landing_Page import main as manual_trigger_summary_to_landing_page
from jupiter_AI.batch.data_scripts.Manual_Trigger_Module_Most_Popular_fare import main as manual_trigger_summary_popular_fare
from jupiter_AI.batch.data_scripts.Manual_Trigger_to_Summary import main as manual_trigger_to_summary
from jupiter_AI.batch.data_scripts.Manual_Trigger_to_Summary_TL import main as manual_trigger_to_summary_TL
from jupiter_AI.batch.data_scripts.Manual_Trigger_WMQY_Weekly import main as manual_trigger_WMQY_weekly
from jupiter_AI.batch.data_scripts.Manual_Trigger_WMQY_Yearly import main as manual_trigger_WMQY_yearly
from jupiter_AI.batch.data_scripts.Manual_Trigger_WMQY_RolledUp import main as manual_trigger_WMQY_rolledUp
from jupiter_AI.batch.data_scripts.Manual_Trigger_WMQY_Event import main as manual_trigger_WMQY_event
from jupiter_AI.batch.data_scripts.Manual_Trigger_WMQY_Quarterly import main as manual_trigger_WMQY_quarterly
from jupiter_AI.batch.data_scripts.Manual_Trigger_WMQY_Monthly import main as manual_trigger_WMQY_monthly
from jupiter_AI.batch.data_scripts.Manual_Trigger_WMQY_Market_Significant import main as manual_trigger_WMQY_market_significant

from jupiter_AI.batch.JUP_AI_Batch_Market_Characteristics import main as pos_od_compartment
from jupiter_AI.batch.JUP_AI_Significant_POSODC import main as market_significance
from task_group_mappings import task_group_map
from celery.utils.log import get_task_logger
import logging
from celery.signals import task_prerun, task_postrun, task_failure
from jupiter_AI.triggers.Performance_Java_triggers import hit_url
from jupiter_AI.triggers.recommendation_models.pos_host_fares_df import get_host_fares_df
from jupiter_AI.batch.atpco_automation.YQYR_celery import yqyr_main
from jupiter_AI.batch.atpco_automation.YQYR_complete import yqyr_complete_main
from jupiter_AI.batch.atpco_automation.Yqyr_changetab import yqyrchangetab_main
from jupiter_AI.batch.atpco_automation.yqyr_tab178190 import yqyr_tab178190_main
from jupiter_AI.batch.atpco_automation.yqyr_tab171198 import yqyr_tab171198_main
from jupiter_AI.batch.atpco_automation.yqyr_secpos import yqyr_secpos_main
from jupiter_AI.batch.atpco_automation.yqyr_zoneflag import yqyr_zoneflag_main
from jupiter_AI.batch.atpco_automation.yqyr_s1change import yqyr_s1change_main
from jupiter_AI.batch.atpco_automation.yqyr_s1reverse import yqyr_s1reverse_main
from jupiter_AI.batch.atpco_automation.exchange_rate_update import exchange_rate_main

from jupiter_AI.triggers.data_level.BookingChangesRolling import main_helper as booking_changes_rolling
from jupiter_AI.triggers.data_level.BookingChangesVLYR import main_helper as booking_changes_vlyr
from jupiter_AI.triggers.data_level.BookingChangesVTGT import main_helper as booking_changes_vtgt
from jupiter_AI.triggers.data_level.BookingChangesWeekly import main_helper as booking_changes_weekly
from jupiter_AI.triggers.data_level.PaxChangesRolling import main_helper as pax_changes_rolling
from jupiter_AI.triggers.data_level.PaxChangesVLYR import main_helper as pax_changes_vlyr
from jupiter_AI.triggers.data_level.PaxChangesVTGT import main_helper as pax_changes_vtgt
from jupiter_AI.triggers.data_level.PaxChangesWeekly import main_helper as pax_changes_weekly
from jupiter_AI.triggers.data_level.RevenueChangesVTGT import main_helper as revenue_changes_vtgt
from jupiter_AI.triggers.data_level.RevenueChangesRolling import main_helper as revenue_changes_rolling
from jupiter_AI.triggers.data_level.RevenueChangesWeekly import main_helper as revenue_changes_weekly
from jupiter_AI.triggers.data_level.RevenueChangesVLYR import main_helper as revenue_changes_vlyr
from jupiter_AI.triggers.data_level.YieldChangesRolling import main_helper as yield_changes_rolling
from jupiter_AI.triggers.data_level.YieldChangesWeekly import main_helper as yield_changes_weekly
from jupiter_AI.triggers.data_level.YieldChangesVLYR import main_helper as yield_changes_vlyr
from jupiter_AI.triggers.data_level.YieldChangesVTGT import main_helper as yield_changes_vtgt
from jupiter_AI.batch.atpco_automation.temp_fares_collection import create_temp_fares_collection
from jupiter_AI.batch.lowest_filed_fare_batch_adhoc import update_market_adhoc
#from jupiter_AI.batch.lowest_filed_fare_batch_snapdate import update_market as update_market_snap
#from jupiter_AI.batch.lowest_filed_fare_batch_addsnap import update_market as update_market_snap
import uuid
logger = get_task_logger(__name__)
TOTAL_TIME_TAKEN = {}
handler = logging.FileHandler(filename=os.path.join(os.path.dirname(PYTHON_PATH + 'logs/'),
                                                    'celery_tasks_{}.log'.format(
                                                        datetime.datetime.strftime(datetime.datetime.now(),
                                                                                   "%d_%m_%Y"))))

logger.addHandler(handler)

start_delimiter = '> start ::'
end_delimiter = '> end ::'
error_delimiter = '> error ::'

url = 'amqp://' + RABBITMQ_USERNAME + \
      ":" + RABBITMQ_PASSWORD + \
      "@" + RABBITMQ_HOST + ":" + str(RABBITMQ_PORT) + "//"

app = Celery('Automation_tasks',
             backend='rpc://',
             broker=url,
             task_ignore_result=True,
             worker_prefetch_multiplier=1,
             worker_max_memory_per_child=2048000,
             task_acks_late=True
             )

d = {}
TASK_NAME = ''
ERROR_MESSAGE = ''
ERROR_CLASS = ''
TASK_ID = ''
ARG = ''
HOST = ''


@task_prerun.connect
# #@measure(JUPITER_LOGGER)
def task_prerun_handler(signal, sender, task_id, task, args, **kwargs):
    global TASK_NAME
    global TASK_ID
    global ARG
    global HOST

    client = mongo_client()
    db = client[JUPITER_DB]
    d[task_id] = datetime.datetime.now()
    TASK_NAME = task.__name__
    TASK_ID = task_id
    ARG = task.request.kwargs
    HOST = task.request.hostname
    count_trig = db.JUP_DB_Celery_Status.find({'group_name': task_group_map.get(task.__name__, "common"),
                                               'start_date': SYSTEM_DATE}).count()
    db.execution_stats.insert_one({"task_id": task_id,
                               "task_name": task.__name__,
                               "group_name": task_group_map.get(task.__name__, "common"),
                               "task_start_date": datetime.datetime.now().strftime("%Y-%m-%d"),
                               "attempt_number": count_trig,
                               "task_start_time": datetime.datetime.now().strftime("%H:%M:%S"),
                               "kwargs": task.request.kwargs,
                               "worker": task.request.hostname,
                               "status": "Started"})
    client.close()


@task_postrun.connect
# #@measure(JUPITER_LOGGER)
def task_postrun_handler(signal, sender, task_id, task, args, retval, state, **kwargs):
    client = mongo_client()
    db = client[JUPITER_DB]
    try:
        st = d.pop(task_id)
        cost = (datetime.datetime.now() - st).seconds
        hrs = cost // 3600
        minutes = (cost % 3600) // 60
        secs = (cost % 3600) % 60
    except KeyError:
        hrs = -1
        minutes = -1
        secs = -1
        st = -1
        cost = -1
    # logger.info(task.__name__ + ' ' + str(cost))
    # Do we need to store history? If so, what is the frequency of history
    # consult with Shameer
    db.execution_stats.update({"task_id": task_id},
                              {"$set": {
                                  "time_taken": str(hrs) + ":" + str(minutes) + ":" + str(secs),
                                  "completed_timestamp": datetime.datetime.now(),
                                  "task_end_date": datetime.datetime.now().strftime("%Y-%m-%d"),
                                  "task_end_time": datetime.datetime.now().strftime("%H:%M:%S"),
                                  "status": "Finished"}})
    client.close()



##@task_failure.connect
def task_failure_handler(exception, einfo):
    # logger_info = error_delimiter + \
    #               ' ' + str(sender.__name__) + ' Failed. Task_id: ' + \
    #               str(task_id) + \
    #               " kwargs: " + \
    #               str(kwargs['kwargs']) + \
    #               " hostname: " + \
    #               str(sender.request.hostname) + \
    #               " timestamp: " + \
    #               datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
    #               " traceback: " + \
    #               str(einfo)
    logger_info = error_delimiter + \
                  ' ' + str(TASK_NAME) + ' Failed. Task_id: ' + \
                  str(TASK_ID) + \
                  " kwargs: " + \
                  str(ARG) + \
                  " hostname: " + \
                  str(HOST) + \
                  " timestamp: " + \
                  datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                  " traceback: " + \
                  str(einfo)


    logger.info(logger_info)
    client = mongo_client()
    db = client[JUPITER_DB]
    db.execution_stats.update({"task_id": TASK_ID},
                              {"$set": {
                                  "error_message": str(exception),
                                  "error_class": str(exception.__class__),
                                  'error_detail':str(einfo),
                                  "status": "Failed"
                                  }})
    # error_mail(task=sender.__name__,
    #            task_id=task_id,
    #            error_class= exception.__class__.__name__,
    #            error_message= str(exception),
    #            hostname=sender.request.hostname,
    #            error=str(einfo))
    client.close()
    print 'Mail Sent'

# @task_prerun.connect
# def task_prerun_handler(signal, sender, task_id, task, args, **kwargs):
#     temp = dict()
#
#     temp['task_id'] = task_id
#     temp['task_name'] = task.__name__
#     temp['group_name'] = task_group_map.get(task.__name__, "common")
#     temp["task_start_date"] = datetime.datetime.now().strftime("%Y-%m-%d")
#     temp["task_start_time"] = datetime.datetime.now().strftime("%H:%M:%S")
#     temp['task_start_time_dt'] = datetime.datetime.now()
#     temp["kwargs"] = task.request.kwargs
#
#     d[task_id] = temp
#
#
# @task_postrun.connect
# def task_postrun_handler(signal, sender, task_id, task, args,  retval, state, **kwargs):
#     try:
#         st = d[task_id]['task_start_time_dt']
#         cost = (datetime.datetime.now() - st).seconds
#         hrs = cost//3600
#         minutes = (cost % 3600)//60
#         secs = (cost % 3600) % 60
#     except KeyError:
#         hrs = -1
#         minutes = -1
#         secs = -1
#         st = -1
#         cost = -1
#
#     d[task_id]["time_taken"] = str(hrs) + ":" + str(minutes) + ":" + str(secs)
#     d[task_id]["task_end_date"] = datetime.datetime.now().strftime("%Y-%m-%d")
#     d[task_id]["task_end_time"] = datetime.datetime.now().strftime("%H:%M:%S")
#     del d[task_id]['task_start_time_dt']
#     logger.info('> stats :: ' + json.dumps(d[task_id]))


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.test_job')
##@measure(JUPITER_LOGGER)
def test_job(tm):
    st = time.time()
    task_id = test_job.request.id
    _kwargs = test_job.request.kwargs
    hostname = test_job.request.hostname
    logger_start = start_delimiter + \
                   " test_job Started. Task_id: " + \
                   str(task_id) + \
                   " kwargs: " + \
                   str(_kwargs) + \
                   " hostname: " + \
                   str(hostname) + \
                   " timestamp: " + \
                   datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(logger_start)
    # try:
    print 'abcd'
    time_taken = time.time() - st
    logger_info = end_delimiter + \
                  ' test_job Done. Task_id: ' + \
                  str(task_id) + \
                  " kwargs: " + \
                  str(_kwargs) + \
                  " hostname: " + \
                  str(hostname) + \
                  " timestamp: " + \
                  datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                  " timetaken: " + \
                  str(time_taken)
    return_arg = {'status': 'Done'}
    logger.info(logger_info)
    return return_arg


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_record_4_5')
##@measure(JUPITER_LOGGER)
def run_record_4_5(system_date, file_time):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_record_4_5.request.id
        _kwargs = run_record_4_5.request.kwargs
        hostname = run_record_4_5.request.hostname
        logger_start = start_delimiter + \
                       " run_record_4_5 Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        record_4_5(system_date=system_date,file_time=file_time, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_record_4_5 Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        einfo = traceback.format_exc()
        run_record_4_5.request.id = str(uuid.uuid4())
        run_record_4_5.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_record_0_change')
##@measure(JUPITER_LOGGER)
def run_record_0_change(system_date,file_time):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_record_0_change.request.id
        _kwargs = run_record_0_change.request.kwargs
        hostname = run_record_0_change.request.hostname
        logger_start = start_delimiter + \
                       " run_record_0_change Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        record_0_change(system_date=system_date,file_time= file_time, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_record_0_change Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        einfo = traceback.format_exc()
        run_record_0_change.request.id = str(uuid.uuid4())
        run_record_0_change.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_record_1_change')
##@measure(JUPITER_LOGGER)
def run_record_1_change(system_date, file_time):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_record_1_change.request.id
        _kwargs = run_record_1_change.request.kwargs
        hostname = run_record_1_change.request.hostname
        logger_start = start_delimiter + \
                       " run_record_1_change Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        record_1_change(system_date=system_date, file_time= file_time, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_record_1_change Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_record_1_change.request.id = str(uuid.uuid4())
        run_record_1_change.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_record_8_change')
##@measure(JUPITER_LOGGER)
def run_record_8_change(system_date, file_time):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_record_8_change.request.id
        _kwargs = run_record_8_change.request.kwargs
        hostname = run_record_8_change.request.hostname
        logger_start = start_delimiter + \
                       " run_record_8_change Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        record_8_change(system_date=system_date, file_time = file_time, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_record_8_change Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_record_8_change.request.id = str(uuid.uuid4())
        run_record_8_change.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_record_2_cat_all_change')
##@measure(JUPITER_LOGGER)
def run_record_2_cat_all_change(system_date, file_time):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_record_2_cat_all_change.request.id
        _kwargs = run_record_2_cat_all_change.request.kwargs
        hostname = run_record_2_cat_all_change.request.hostname
        logger_start = start_delimiter + \
                       " run_record_2_cat_all_change Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        record_2_cat_all_change(system_date=system_date, file_time= file_time, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_record_2_cat_all_change Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_record_2_cat_all_change.request.id = str(uuid.uuid4())
        run_record_2_cat_all_change.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_record_2_cat_10_change')
##@measure(JUPITER_LOGGER)
def run_record_2_cat_10_change(system_date, file_time):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_record_2_cat_10_change.request.id
        _kwargs = run_record_2_cat_10_change.request.kwargs
        hostname = run_record_2_cat_10_change.request.hostname
        logger_start = start_delimiter + \
                       " run_record_2_cat_10_change Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        record_2_cat_10_change(system_date=system_date, file_time= file_time, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_record_2_cat_10_change Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_record_2_cat_10_change.request.id = str(uuid.uuid4())
        run_record_2_cat_10_change.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_record_2_cat_25_change')
##@measure(JUPITER_LOGGER)
def run_record_2_cat_25_change(system_date, file_time):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_record_2_cat_25_change.request.id
        _kwargs = run_record_2_cat_25_change.request.kwargs
        hostname = run_record_2_cat_25_change.request.hostname
        logger_start = start_delimiter + \
                       " run_record_2_cat_25_change Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        record_2_cat_25_change(system_date=system_date, file_time= file_time, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_record_2_cat_25_change Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_record_2_cat_25_change.request.id = str(uuid.uuid4())
        run_record_2_cat_25_change.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_record_2_cat_3_fn_change')
##@measure(JUPITER_LOGGER)
def run_record_2_cat_3_fn_change(system_date, file_time):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_record_2_cat_3_fn_change.request.id
        _kwargs = run_record_2_cat_3_fn_change.request.kwargs
        hostname = run_record_2_cat_3_fn_change.request.hostname
        logger_start = start_delimiter + \
                       " run_record_2_cat_3_fn_change Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        record_2_cat_3_fn_change(system_date=system_date, file_time= file_time, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_record_2_cat_3_fn_change Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_record_2_cat_3_fn_change.request.id = str(uuid.uuid4())
        run_record_2_cat_3_fn_change.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_record_2_cat_11_fn_change')
##@measure(JUPITER_LOGGER)
def run_record_2_cat_11_fn_change(system_date, file_time):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_record_2_cat_11_fn_change.request.id
        _kwargs = run_record_2_cat_11_fn_change.request.kwargs
        hostname = run_record_2_cat_11_fn_change.request.hostname
        logger_start = start_delimiter + \
                       " run_record_2_cat_11_fn_change Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        record_2_cat_11_fn_change(system_date=system_date, file_time= file_time, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_record_2_cat_11_fn_change Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_record_2_cat_11_fn_change.request.id = str(uuid.uuid4())
        run_record_2_cat_11_fn_change.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_record_2_cat_14_fn_change')
##@measure(JUPITER_LOGGER)
def run_record_2_cat_14_fn_change(system_date, file_time):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_record_2_cat_14_fn_change.request.id
        _kwargs = run_record_2_cat_14_fn_change.request.kwargs
        hostname = run_record_2_cat_14_fn_change.request.hostname
        logger_start = start_delimiter + \
                       " run_record_2_cat_14_fn_change Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        record_2_cat_14_fn_change(system_date=system_date, file_time= file_time, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_record_2_cat_14_fn_change Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_record_2_cat_14_fn_change.request.id = str(uuid.uuid4())
        run_record_2_cat_14_fn_change.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_record_2_cat_15_fn_change')
##@measure(JUPITER_LOGGER)
def run_record_2_cat_15_fn_change(system_date, file_time):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_record_2_cat_15_fn_change.request.id
        _kwargs = run_record_2_cat_15_fn_change.request.kwargs
        hostname = run_record_2_cat_15_fn_change.request.hostname
        logger_start = start_delimiter + \
                       " run_record_2_cat_15_fn_change Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        record_2_cat_15_fn_change(system_date=system_date, file_time= file_time, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_record_2_cat_15_fn_change Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_record_2_cat_15_fn_change.request.id = str(uuid.uuid4())
        run_record_2_cat_15_fn_change.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_record_2_cat_23_fn_change')
##@measure(JUPITER_LOGGER)
def run_record_2_cat_23_fn_change(system_date, file_time):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_record_2_cat_23_fn_change.request.id
        _kwargs = run_record_2_cat_23_fn_change.request.kwargs
        hostname = run_record_2_cat_23_fn_change.request.hostname
        logger_start = start_delimiter + \
                       " run_record_2_cat_23_fn_change Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        record_2_cat_23_fn_change(system_date=system_date, file_time= file_time, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_record_2_cat_23_fn_change Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_record_2_cat_23_fn_change.request.id = str(uuid.uuid4())
        run_record_2_cat_23_fn_change.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_recseg2')
##@measure(JUPITER_LOGGER)
def run_recseg2(system_date,file_time):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_recseg2.request.id
        _kwargs = run_recseg2.request.kwargs
        hostname = run_recseg2.request.hostname
        logger_start = start_delimiter + \
                       " run_recseg2 Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        recseg2(system_date=system_date,file_time=file_time, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_recseg2 Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_recseg2.request.id = str(uuid.uuid4())
        run_recseg2.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_recseg3')
##@measure(JUPITER_LOGGER)
def run_recseg3(system_date, file_time):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_recseg3.request.id
        _kwargs = run_recseg3.request.kwargs
        hostname = run_recseg3.request.hostname
        logger_start = start_delimiter + \
                       " run_recseg3 Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        recseg3(system_date=system_date,file_time=file_time, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_recseg3 Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_recseg3.request.id = str(uuid.uuid4())
        run_recseg3.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_fares_and_stage_1')
##@measure(JUPITER_LOGGER)
def run_fares_and_stage_1(system_date, file_time, carrier_list, tariff_list):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_fares_and_stage_1.request.id
        _kwargs = run_fares_and_stage_1.request.kwargs
        hostname = run_fares_and_stage_1.request.hostname
        logger_start = start_delimiter + \
                       " run_fares_and_stage_1 Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        # returned_ids = fare_changes(system_date=system_date, file_time = file_time, carrier_list=carrier_list,
        #                             tariff_list=tariff_list, client=client)
        fare_changes(system_date=system_date, file_time=file_time, carrier_list=carrier_list,
                     tariff_list=tariff_list, client=client)

        # stage_2 = []
        # for k in range(int(math.ceil(len(returned_ids) / 100.0))):
        #     temp = []
        #     print "Group iteration for stage 2: ", k
        #     try:
        #         temp = returned_ids[k * 100: (k + 1) * 100]
        #     except IndexError:
        #         temp = returned_ids[k * 100:]
        #     fr_date = datetime.datetime.strptime(system_date, '%Y-%m-%d')
        #     fr_date = fr_date + datetime.timedelta(days=1)
        #     fr_date = datetime.datetime.strftime(fr_date, '%Y-%m-%d')
        #     stage_2.append(run_stage_2_rules.s(system_date=fr_date,file_time = file_time, id_list=temp))
        #
        # group_1 = group(stage_2)
        # res_1 = group_1()

        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_fares_and_stage_1 Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_fares_and_stage_1.request.id = str(uuid.uuid4())
        run_fares_and_stage_1.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_stage_2_rules')
##@measure(JUPITER_LOGGER)
def run_stage_2_rules(system_date,file_time, id_list):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_stage_2_rules.request.id
        _kwargs = run_stage_2_rules.request.kwargs
        hostname = run_stage_2_rules.request.hostname
        logger_start = start_delimiter + \
                       " run_stage_2_rules Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        run_stage_2(system_date=system_date, file_time = file_time, id_list=id_list, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_stage_2_rules Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_stage_2_rules.request.id = str(uuid.uuid4())
        run_stage_2_rules.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return

# update_stage_2_without_rec1

@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_update_stage_2_without_rec1')
##@measure(JUPITER_LOGGER)
def run_update_stage_2_without_rec1(id_list):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_update_stage_2_without_rec1.request.id
        _kwargs = run_update_stage_2_without_rec1.request.kwargs
        hostname = run_update_stage_2_without_rec1.request.hostname
        logger_start = start_delimiter + \
                       " run_update_stage_2_without_rec1 Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                           str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        update_stage_2_without_rec1(id_list=id_list, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_update_stage_2_without_rec1 Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_update_stage_2_without_rec1.request.id = str(uuid.uuid4())
        run_update_stage_2_without_rec1.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return

@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_fareladder')
##@measure(JUPITER_LOGGER)
def run_fareladder(od_list):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_fareladder.request.id
        _kwargs = run_fareladder.request.kwargs
        hostname = run_fareladder.request.hostname
        logger_start = start_delimiter + \
                       " run_fareladder Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        update_comp_fb(od_list=od_list, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_fareladder Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_fareladder.request.id = str(uuid.uuid4())
        run_fareladder.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_footnote_coll')
##@measure(JUPITER_LOGGER)
def run_footnote_coll():
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_footnote_coll.request.id
        _kwargs = run_footnote_coll.request.kwargs
        hostname = run_footnote_coll.request.hostname
        logger_start = start_delimiter + \
                       " run_footnote_coll Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        generate_footnotes_collection(client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_footnote_coll Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_footnote_coll.request.id = str(uuid.uuid4())
        run_footnote_coll.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.send_msg')
##@measure(JUPITER_LOGGER)
def send_msg(results_grp):
    if results_grp == [16]:
        logger.info("Sending msg to generate triggers!!!")
        message = json.dumps({"message": "updated_atpco_files"})
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.basic_publish(exchange='',
                              routing_key="automation_pipeline",
                              body=message)
    return 1


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_host_od_capacity")
##@measure(JUPITER_LOGGER)
def run_host_od_capacity():
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_host_od_capacity.request.id
        _kwargs = run_host_od_capacity.request.kwargs
        hostname = run_host_od_capacity.request.hostname
        logger_start = start_delimiter + \
                       " run_host_od_capacity Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        get_leg_capacities(client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_host_od_capacity Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_host_od_capacity.request.id = str(uuid.uuid4())
        run_host_od_capacity.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_comp_od_capacity")
##@measure(JUPITER_LOGGER)
def run_comp_od_capacity():
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_comp_od_capacity.request.id
        _kwargs = run_comp_od_capacity.request.kwargs
        hostname = run_comp_od_capacity.request.hostname
        logger_start = start_delimiter + \
                       " run_comp_od_capacity Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        get_od_capacity_simple(client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_comp_od_capacity Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_comp_od_capacity.request.id = str(uuid.uuid4())
        run_comp_od_capacity.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_golden_flights")
##@measure(JUPITER_LOGGER)
def run_golden_flights():
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_golden_flights.request.id
        _kwargs = run_golden_flights.request.kwargs
        hostname = run_golden_flights.request.hostname
        logger_start = start_delimiter + \
                       " run_golden_flights Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        generate_golden_flights(client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_golden_flights Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_golden_flights.request.id = str(uuid.uuid4())
        run_golden_flights.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_copper_flights")
##@measure(JUPITER_LOGGER)
def run_copper_flights():
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_copper_flights.request.id
        _kwargs = run_copper_flights.request.kwargs
        hostname = run_copper_flights.request.hostname
        logger_start = start_delimiter + \
                       " run_copper_flights Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        generate_copper_flights(client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_copper_flights Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_copper_flights.request.id = str(uuid.uuid4())
        run_copper_flights.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_zinc_flights")
##@measure(JUPITER_LOGGER)
def run_zinc_flights():
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_zinc_flights.request.id
        _kwargs = run_zinc_flights.request.kwargs
        hostname = run_zinc_flights.request.hostname
        logger_start = start_delimiter + \
                       " run_zinc_flights Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        generate_zinc_flights(client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_zinc_flights Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_zinc_flights.request.id = str(uuid.uuid4())
        run_zinc_flights.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_market_significance")
##@measure(JUPITER_LOGGER)
def run_market_significance():
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_market_significance.request.id
        _kwargs = run_market_significance.request.kwargs
        hostname = run_market_significance.request.hostname
        logger_start = start_delimiter + \
                       " run_market_significance Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        market_significance(client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_market_significance Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_market_significance.request.id = str(uuid.uuid4())
        run_market_significance.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_pos_od_compartment")
##@measure(JUPITER_LOGGER)
def run_pos_od_compartment(months):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_pos_od_compartment.request.id
        _kwargs = run_pos_od_compartment.request.kwargs
        hostname = run_pos_od_compartment.request.hostname
        logger_start = start_delimiter + \
                       " run_pos_od_compartment Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        pos_od_compartment(months=months, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_pos_od_compartment Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_pos_od_compartment.request.id = str(uuid.uuid4())
        run_pos_od_compartment.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return

# manual trigger target update
@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_manual_trigger_target")
##@measure(JUPITER_LOGGER)
def run_manual_trigger_target(pos, snap_date,doc):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_manual_trigger_target.request.id
        _kwargs = run_manual_trigger_target.request.kwargs
        hostname = run_manual_trigger_target.request.hostname
        logger_start = start_delimiter + \
                       " run_manual_trigger_target Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        manual_trigger_target(pos=pos,snap_date=snap_date,doc=doc , client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_manual_trigger_target Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_manual_trigger_target.request.id = str(uuid.uuid4())
        run_manual_trigger_target.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


#manual_trigger_capacity
@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_manual_trigger_capacity")
##@measure(JUPITER_LOGGER)
def run_manual_trigger_capacity(od):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_manual_trigger_capacity.request.id
        _kwargs = run_manual_trigger_capacity.request.kwargs
        hostname = run_manual_trigger_capacity.request.hostname
        logger_start = start_delimiter + \
                       " run_manual_trigger_capacity Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        manual_trigger_capacity(od=od, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_manual_trigger_capacity Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_manual_trigger_capacity.request.id = str(uuid.uuid4())
        run_manual_trigger_capacity.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


#manual_trigger_distance
@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_manual_trigger_distance")
##@measure(JUPITER_LOGGER)
def run_manual_trigger_distance(od):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_manual_trigger_distance.request.id
        _kwargs = run_manual_trigger_distance.request.kwargs
        hostname = run_manual_trigger_distance.request.hostname
        logger_start = start_delimiter + \
                       " run_manual_trigger_distance Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        manual_trigger_distance(od=od,client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_manual_trigger_distance Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_manual_trigger_distance.request.id = str(uuid.uuid4())
        run_manual_trigger_distance.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


# manual_trigger_forecast
@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_manual_trigger_forecast")
##@measure(JUPITER_LOGGER)
def run_manual_trigger_forecast(pos, snap_date,doc):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_manual_trigger_forecast.request.id
        _kwargs = run_manual_trigger_forecast.request.kwargs
        hostname = run_manual_trigger_forecast.request.hostname
        logger_start = start_delimiter + \
                       " run_manual_trigger_forecast Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        manual_trigger_forecast(pos=pos,snap_date=snap_date,doc=doc , client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_manual_trigger_forecast Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_manual_trigger_forecast.request.id = str(uuid.uuid4())
        run_manual_trigger_forecast.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


# manual_trigger_module_VLYR_pax_1
@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_manual_trigger_module_VLYR_pax_1")
##@measure(JUPITER_LOGGER)
def run_manual_trigger_module_VLYR_pax_1(pos):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_manual_trigger_module_VLYR_pax_1.request.id
        _kwargs = run_manual_trigger_module_VLYR_pax_1.request.kwargs
        hostname = run_manual_trigger_module_VLYR_pax_1.request.hostname
        logger_start = start_delimiter + \
                       " run_manual_trigger_module_VLYR_pax_1 Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        manual_trigger_module_VLYR_pax_1(pos=pos, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_manual_trigger_module_VLYR_pax_1 Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_manual_trigger_module_VLYR_pax_1.request.id = str(uuid.uuid4())
        run_manual_trigger_module_VLYR_pax_1.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


# landing_page_tile_triggers
@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_landing_page_tile_triggers")
##@measure(JUPITER_LOGGER)
def run_landing_page_tile_triggers(each_user, each_comp):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_landing_page_tile_triggers.request.id
        _kwargs = run_landing_page_tile_triggers.request.kwargs
        hostname = run_landing_page_tile_triggers.request.hostname
        logger_start = start_delimiter + \
                       " run_landing_page_tile_triggers Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        landing_page_tile_triggers(each_user, each_comp, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_landing_page_tile_triggers Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_landing_page_tile_triggers.request.id = str(uuid.uuid4())
        run_landing_page_tile_triggers.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


# manual_trigger_summary_to_landing_page
@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_manual_trigger_summary_to_landing_page")
##@measure(JUPITER_LOGGER)
def run_manual_trigger_summary_to_landing_page(user):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_manual_trigger_summary_to_landing_page.request.id
        _kwargs = run_manual_trigaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaager_summary_to_landing_page.request.kwargs
        hostname = run_manual_trigger_summary_to_landing_page.request.hostname
        logger_start = start_delimiter + \
                       " run_manual_trigger_summary_to_landing_page Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        manual_trigger_summary_to_landing_page(user, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_manual_trigger_summary_to_landing_page Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_manual_trigger_summary_to_landing_page.request.id = str(uuid.uuid4())
        run_manual_trigger_summary_to_landing_page.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


# manual_trigger_to_summary
@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_manual_trigger_to_summary")
##@measure(JUPITER_LOGGER)
def run_manual_trigger_to_summary(pos):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_manual_trigger_to_summary.request.id
        _kwargs = run_manual_trigger_to_summary.request.kwargs
        hostname = run_manual_trigger_to_summary.request.hostname
        logger_start = start_delimiter + \
                       " run_manual_trigger_to_summary Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        manual_trigger_to_summary(pos, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_manual_trigger_to_summary Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_manual_trigger_to_summary.request.id = str(uuid.uuid4())
        run_manual_trigger_to_summary.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


# Manual Trigger_to_summary_TL compartment
@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_manual_trigger_to_summary_TL")
##@measure(JUPITER_LOGGER)
def run_manual_trigger_to_summary_TL(pos):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_manual_trigger_to_summary_TL.request.id
        _kwargs = run_manual_trigger_to_summary_TL.request.kwargs
        hostname = run_manual_trigger_to_summary_TL.request.hostname
        logger_start = start_delimiter + \
                       " run_manual_trigger_to_summary_TL Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        manual_trigger_to_summary_TL(pos, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_manual_trigger_to_summary_TL Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_manual_trigger_to_summary_TL.request.id = str(uuid.uuid4())
        run_manual_trigger_to_summary_TL.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


# manual_trigger_summary_popular_fare
@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_manual_trigger_summary_popular_fare")
##@measure(JUPITER_LOGGER)
def run_manual_trigger_summary_popular_fare(ods):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_manual_trigger_summary_popular_fare.request.id
        _kwargs = run_manual_trigger_summary_popular_fare.request.kwargs
        hostname = run_manual_trigger_summary_popular_fare.request.hostname
        logger_start = start_delimiter + \
                       " run_manual_trigger_summary_popular_fare Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        manual_trigger_summary_popular_fare(client=client,ods = ods)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_manual_trigger_summary_popular_fare Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_manual_trigger_summary_popular_fare.request.id = str(uuid.uuid4())
        run_manual_trigger_summary_popular_fare.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


# manual_trigger_WMQY_weekly
@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_manual_trigger_WMQY_weekly")
##@measure(JUPITER_LOGGER)
def run_manual_trigger_WMQY_weekly(pos):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_manual_trigger_WMQY_weekly.request.id
        _kwargs = run_manual_trigger_WMQY_weekly.request.kwargs
        hostname = run_manual_trigger_WMQY_weekly.request.hostname
        logger_start = start_delimiter + \
                       " run_manual_trigger_WMQY_weekly Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        manual_trigger_WMQY_weekly(pos, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_manual_trigger_WMQY_weekly Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_manual_trigger_WMQY_weekly.request.id = str(uuid.uuid4())
        run_manual_trigger_WMQY_weekly.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


# manual_trigger_WMQY_yearly
@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_manual_trigger_WMQY_yearly")
##@measure(JUPITER_LOGGER)
def run_manual_trigger_WMQY_yearly(pos):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_manual_trigger_WMQY_yearly.request.id
        _kwargs = run_manual_trigger_WMQY_yearly.request.kwargs
        hostname = run_manual_trigger_WMQY_yearly.request.hostname
        logger_start = start_delimiter + \
                       " run_manual_trigger_WMQY_yearly Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        manual_trigger_WMQY_yearly(pos, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_manual_trigger_WMQY_yearly Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_manual_trigger_WMQY_yearly.request.id = str(uuid.uuid4())
        run_manual_trigger_WMQY_yearly.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


# manual_trigger_WMQY_rolledUp
@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_manual_trigger_WMQY_rolledUp")
##@measure(JUPITER_LOGGER)
def run_manual_trigger_WMQY_rolledUp(pos):
    try:
            client = mongo_client()
            st = time.time()
            task_id = run_manual_trigger_WMQY_rolledUp.request.id
            _kwargs = run_manual_trigger_WMQY_rolledUp.request.kwargs
            hostname = run_manual_trigger_WMQY_rolledUp.request.hostname
            logger_start = start_delimiter + \
                           " run_manual_trigger_WMQY_rolledUp Started. Task_id: " + \
                           str(task_id) + \
                           " kwargs: " + \
                           str(_kwargs) + \
                           " hostname: " + \
                           str(hostname) + \
                           " timestamp: " + \
                           datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logger.info(logger_start)
            manual_trigger_WMQY_rolledUp(pos, client=client)
            time_taken = time.time() - st
            logger_info = end_delimiter + \
                          ' run_manual_trigger_WMQY_rolledUp Done. Task_id: ' + \
                          str(task_id) + \
                          " kwargs: " + \
                          str(_kwargs) + \
                          " hostname: " + \
                          str(hostname) + \
                          " timestamp: " + \
                          datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                          " timetaken: " + \
                          str(time_taken)
            return_arg = {'status': 'Done'}
            logger.info(logger_info)
            client.close()
            return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_manual_trigger_WMQY_rolledUp.request.id = str(uuid.uuid4())
        run_manual_trigger_WMQY_rolledUp.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return

# manual_trigger_WMQY_event
@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_manual_trigger_WMQY_event")
##@measure(JUPITER_LOGGER)
def run_manual_trigger_WMQY_event(pos):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_manual_trigger_WMQY_event.request.id
        _kwargs = run_manual_trigger_WMQY_event.request.kwargs
        hostname = run_manual_trigger_WMQY_event.request.hostname
        logger_start = start_delimiter + \
                       " run_manual_trigger_WMQY_event Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        manual_trigger_WMQY_event(pos, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_manual_trigger_WMQY_event Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_manual_trigger_WMQY_event.request.id = str(uuid.uuid4())
        run_manual_trigger_WMQY_event.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


# manual_trigger_WMQY_quarterly
@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_manual_trigger_WMQY_quarterly")
##@measure(JUPITER_LOGGER)
def run_manual_trigger_WMQY_quarterly(pos):
    try:
            client = mongo_client()
            st = time.time()
            task_id = run_manual_trigger_WMQY_quarterly.request.id
            _kwargs = run_manual_trigger_WMQY_quarterly.request.kwargs
            hostname = run_manual_trigger_WMQY_quarterly.request.hostname
            logger_start = start_delimiter + \
                           " run_manual_trigger_WMQY_quarterly Started. Task_id: " + \
                           str(task_id) + \
                           " kwargs: " + \
                           str(_kwargs) + \
                           " hostname: " + \
                           str(hostname) + \
                           " timestamp: " + \
                           datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logger.info(logger_start)
            manual_trigger_WMQY_quarterly(pos, client=client)
            time_taken = time.time() - st
            logger_info = end_delimiter + \
                          ' run_manual_trigger_WMQY_quarterly Done. Task_id: ' + \
                          str(task_id) + \
                          " kwargs: " + \
                          str(_kwargs) + \
                          " hostname: " + \
                          str(hostname) + \
                          " timestamp: " + \
                          datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                          " timetaken: " + \
                          str(time_taken)
            return_arg = {'status': 'Done'}
            logger.info(logger_info)
            client.close()
            return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_manual_trigger_WMQY_quarterly.request.id = str(uuid.uuid4())
        run_manual_trigger_WMQY_quarterly.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


# manual_trigger_WMQY_monthly
@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_manual_trigger_WMQY_monthly")
##@measure(JUPITER_LOGGER)
def run_manual_trigger_WMQY_monthly(pos):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_manual_trigger_WMQY_monthly.request.id
        _kwargs = run_manual_trigger_WMQY_monthly.request.kwargs
        hostname = run_manual_trigger_WMQY_monthly.request.hostname
        logger_start = start_delimiter + \
                       " run_manual_trigger_WMQY_monthly Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        manual_trigger_WMQY_monthly(pos, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_manual_trigger_WMQY_monthly Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_manual_trigger_WMQY_monthly.request.id = str(uuid.uuid4())
        run_manual_trigger_WMQY_monthly.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


# manual_trigger_WMQY_market_significant
@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_manual_trigger_WMQY_market_significant")
##@measure(JUPITER_LOGGER)
def run_manual_trigger_WMQY_market_significant(pos):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_manual_trigger_WMQY_market_significant.request.id
        _kwargs = run_manual_trigger_WMQY_market_significant.request.kwargs
        hostname = run_manual_trigger_WMQY_market_significant.request.hostname
        logger_start = start_delimiter + \
                       " run_manual_trigger_WMQY_market_significant Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        manual_trigger_WMQY_market_significant(pos, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_manual_trigger_WMQY_market_significant Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_manual_trigger_WMQY_market_significant.request.id = str(uuid.uuid4())
        run_manual_trigger_WMQY_market_significant.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return

# @app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.send_msg")
# #@measure(JUPITER_LOGGER
# def send_msg(results_grp):
#     if results_grp == [6]:
#         logger.info("Sending msg to generate triggers!!!")
#     return time_taken


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_market_agents")
##@measure(JUPITER_LOGGER)
def run_market_agents():
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_market_agents.request.id
        _kwargs = run_market_agents.request.kwargs
        hostname = run_market_agents.request.hostname
        logger_start = start_delimiter + \
                       " run_market_agents Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        agents(client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_market_agents Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_market_agents.request.id = str(uuid.uuid4())
        run_market_agents.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_market_channels")
##@measure(JUPITER_LOGGER)
def run_market_channels():
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_market_channels.request.id
        _kwargs = run_market_channels.request.kwargs
        hostname = run_market_channels.request.hostname
        logger_start = start_delimiter + \
                       " run_market_channels Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        channels(client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_market_channels Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_market_channels.request.id = str(uuid.uuid4())
        run_market_channels.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_market_distributors")
##@measure(JUPITER_LOGGER)
def run_market_distributors():
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_market_distributors.request.id
        _kwargs = run_market_distributors.request.kwargs
        hostname = run_market_distributors.request.hostname
        logger_start = start_delimiter + \
                       " run_market_distributors Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        distributors(client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_market_distributors Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_market_distributors.request.id = str(uuid.uuid4())
        run_market_distributors.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_market_farebrand")
##@measure(JUPITER_LOGGER)
def run_market_farebrand():
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_market_farebrand.request.id
        _kwargs = run_market_farebrand.request.kwargs
        hostname = run_market_farebrand.request.hostname
        logger_start = start_delimiter + \
                       " run_market_farebrand Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        farebrands(client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_market_farebrand Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_market_farebrand.request.id = str(uuid.uuid4())
        run_market_farebrand.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_market_segments")
##@measure(JUPITER_LOGGER)
def run_market_segments():
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_market_segments.request.id
        _kwargs = run_market_segments.request.kwargs
        hostname = run_market_segments.request.hostname
        logger_start = start_delimiter + \
                       " run_market_segments Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        segments(client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_market_segments Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_market_segments.request.id = str(uuid.uuid4())
        run_market_segments.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_market_flights")
##@measure(JUPITER_LOGGER)
def run_market_flights():
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_market_flights.request.id
        _kwargs = run_market_flights.request.kwargs
        hostname = run_market_flights.request.hostname
        logger_start = start_delimiter + \
                       " run_market_flights Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        flights(client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_market_flights Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_market_flights.request.id = str(uuid.uuid4())
        run_market_flights.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_booking_triggers")
##@measure(JUPITER_LOGGER)
def run_booking_triggers(markets, sig_flag=None):
    try:
        client = mongo_client()
        db = client[JUPITER_DB]
        st = time.time()
        task_id = run_booking_triggers.request.id
        _kwargs = run_booking_triggers.request.kwargs
        hostname = run_booking_triggers.request.hostname
        logger_start = start_delimiter + \
                       " run_booking_triggers Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        booking_triggers(markets=markets, sig_flag=sig_flag, db=db)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_booking_triggers Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg

    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        #exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_booking_triggers.request.id = str(uuid.uuid4())
        run_booking_triggers.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_pax_triggers")
##@measure(JUPITER_LOGGER)
def run_pax_triggers(markets, sig_flag=None):
    try:
        client = mongo_client()
        db= client[JUPITER_DB]
        st = time.time()
        task_id = run_pax_triggers.request.id
        _kwargs = run_pax_triggers.request.kwargs
        hostname = run_pax_triggers.request.hostname
        logger_start = start_delimiter + \
                       " run_pax_triggers Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        pax_triggers(markets=markets, sig_flag=sig_flag, db=db)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_pax_triggers Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        #exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_pax_triggers.request.id = str(uuid.uuid4())
        run_pax_triggers.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        #task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_revenue_triggers")
##@measure(JUPITER_LOGGER)
def run_revenue_triggers(markets, sig_flag=None):
    try:
        client =mongo_client()
        db=client[JUPITER_DB]
        st = time.time()
        task_id = run_revenue_triggers.request.id
        _kwargs = run_revenue_triggers.request.kwargs
        hostname = run_revenue_triggers.request.hostname
        logger_start = start_delimiter + \
                       " run_revenue_triggers Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        revenue_triggers(markets=markets, sig_flag=sig_flag, db=db)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_revenue_triggers Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        #exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_revenue_triggers.request.id= str(uuid.uuid4())
        run_revenue_triggers.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        #task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return



@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_yield_triggers")
##@measure(JUPITER_LOGGER)
def run_yield_triggers(markets, sig_flag=None):
    try:
        client = mongo_client()
        db=client[JUPITER_DB]
        st = time.time()
        task_id = run_yield_triggers.request.id
        _kwargs = run_yield_triggers.request.kwargs
        hostname = run_yield_triggers.request.hostname
        logger_start = start_delimiter + \
                       " run_yield_triggers Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        yield_triggers(markets=markets, sig_flag=sig_flag, db=db)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_yield_triggers Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg

    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        #exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_yield_triggers.request.id = str(uuid.uuid4())
        run_yield_triggers.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_pccp_triggers")
##@measure(JUPITER_LOGGER)
def run_pccp_triggers():
    try:
        client = mongo_client()
        db=client[JUPITER_DB]
        st = time.time()
        task_id = run_pccp_triggers.request.id
        _kwargs = run_pccp_triggers.request.kwargs
        hostname = run_pccp_triggers.request.hostname
        logger_start = start_delimiter + \
                       " run_pccp_triggers Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        price_change_triggers(db=db)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_pccp_triggers Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout,HTTPError, ReadTimeout, SSLError, ConnectionError,Timeout, socket.error) as e:
        #exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_pccp_triggers.request.id = str(uuid.uuid4())
        run_pccp_triggers.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return



@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_opp_trend_triggers")
##@measure(JUPITER_LOGGER)
def run_opp_trend_triggers(markets, sig_flag=None):
    try:
        client =mongo_client()
        db=client[JUPITER_DB]
        st = time.time()
        task_id = run_opp_trend_triggers.request.id
        _kwargs = run_opp_trend_triggers.request.kwargs
        hostname = run_opp_trend_triggers.request.hostname
        logger_start = start_delimiter + \
                       " run_opp_trend_triggers Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        opp_trend_triggers(markets=markets, sig_flag=sig_flag, db=db)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_opp_trend_triggers Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        #exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_opp_trend_triggers.request.id = str(uuid.uuid4())
        run_opp_trend_triggers.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_events_triggers")
##@measure(JUPITER_LOGGER)
def run_events_triggers(markets, sig_flag=None):
    try:
        client= mongo_client()
        db = client[JUPITER_DB]
        st = time.time()
        task_id = run_events_triggers.request.id
        _kwargs = run_events_triggers.request.kwargs
        hostname = run_events_triggers.request.hostname
        logger_start = start_delimiter + \
                       " run_events_triggers Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        event_triggers( markets=markets,db=db)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_events_triggers Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        #exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_events_triggers.request.id = str(uuid.uuid4())
        run_events_triggers.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


# @app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_etihad_promotions_triggers")
# ##@measure(JUPITER_LOGGER)
# def run_etihad_promotions_triggers():
#     st = time.time()
#     logger.info("Running etihad promotions triggers")
#     Etihad_Airlines_Promotions.run()
#     task_id = run_etihad_promotions_triggers.request.id
#     _kwargs = run_etihad_promotions_triggers.request.kwargs
#     hostname = run_etihad_promotions_triggers.request.hostname
#     time_taken = time.time() - st
#     logger_info = 'run_etihad_promotions_triggers Done. Task_id: ' + \
#                   str(task_id) + \
#                   " kwargs: " + \
#                   str(_kwargs) + \
#                   " hostname: " + \
#                   str(hostname) + \
#                   " timetaken: " + \
#                   str(time_taken)
#     logger.info(logger_info)
#     return time_taken


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_emirates_promotions_triggers")
##@measure(JUPITER_LOGGER)
def run_emirates_promotions_triggers():
    try:
        st = time.time()
        task_id = run_emirates_promotions_triggers.request.id
        _kwargs = run_emirates_promotions_triggers.request.kwargs
        hostname = run_emirates_promotions_triggers.request.hostname
        logger_start = start_delimiter + \
                       " run_emirates_promotions_triggers Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        EK_new.run()
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_emirates_promotions_triggers Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_emirates_promotions_triggers.request.id = str(uuid.uuid4())
        run_emirates_promotions_triggers.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_group_airline_rating")
##@measure(JUPITER_LOGGER)
def run_group_airline_rating():
    try:
        st = time.time()
        task_id = run_group_airline_rating.request.id
        _kwargs = run_group_airline_rating.request.kwargs
        hostname = run_group_airline_rating.request.hostname
        logger_start = start_delimiter + \
                       " run_group_airline_rating Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        group_airline_rating.run()
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_group_airline_rating Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_group_airline_rating.request.id = str(uuid.uuid4())
        run_group_airline_rating.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_group_prod_rating")
##@measure(JUPITER_LOGGER)
def run_group_prod_rating():
    try:
        st = time.time()
        task_id = run_group_prod_rating.request.id
        _kwargs = run_group_prod_rating.request.kwargs
        hostname = run_group_prod_rating.request.hostname
        logger_start = start_delimiter + \
                       " run_group_prod_rating Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        group_prod_rating.run()
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_group_prod_rating Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_group_prod_rating.request.id = str(uuid.uuid4())
        run_group_prod_rating.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_capacity_rating_blocktime")
##@measure(JUPITER_LOGGER)
def run_capacity_rating_blocktime():
    try:
        st = time.time()
        task_id = run_capacity_rating_blocktime.request.id
        _kwargs = run_capacity_rating_blocktime.request.kwargs
        hostname = run_capacity_rating_blocktime.request.hostname
        logger_start = start_delimiter + \
                       " run_capacity_rating_blocktime Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        capacity_rating_blocktime.run()
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_capacity_rating_blocktime Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_capacity_rating_blocktime.request.id = str(uuid.uuid4())
        run_capacity_rating_blocktime.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_capacity_rating_freq")
##@measure(JUPITER_LOGGER)
def run_capacity_rating_freq():
    try:
        st = time.time()
        task_id = run_capacity_rating_freq.request.id
        _kwargs = run_capacity_rating_freq.request.kwargs
        hostname = run_capacity_rating_freq.request.hostname
        logger_start = start_delimiter + \
                       " run_capacity_rating_freq Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        capacity_rating_freq.run()
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_capacity_rating_freq Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_capacity_rating_freq.request.id = str(uuid.uuid4())
        run_capacity_rating_freq.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_capacity_rating_cap")
##@measure(JUPITER_LOGGER)
def run_capacity_rating_cap():
    try:
        st = time.time()
        task_id = run_capacity_rating_cap.request.id
        _kwargs = run_capacity_rating_cap.request.kwargs
        hostname = run_capacity_rating_cap.request.hostname
        logger_start = start_delimiter + \
                       " run_capacity_rating_cap Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        capacity_rating_cap.run()
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_capacity_rating_cap Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_capacity_rating_cap.request.id = str(uuid.uuid4())
        run_capacity_rating_cap.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_market_rating_mktshare")
##@measure(JUPITER_LOGGER)
def run_market_rating_mktshare():
    try:
        st = time.time()
        task_id = run_market_rating_mktshare.request.id
        _kwargs = run_market_rating_mktshare.request.kwargs
        hostname = run_market_rating_mktshare.request.hostname
        logger_start = start_delimiter + \
                       " run_market_rating_mktshare Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        market_rating_mktshare.run()
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_market_rating_mktshare Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_market_rating_mktshare.request.id = str(uuid.uuid4())
        run_market_rating_mktshare.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_group_market_rating_Size_of_Market")
##@measure(JUPITER_LOGGER)
def run_group_market_rating_Size_of_Market():
    try:
        st = time.time()
        task_id = run_group_market_rating_Size_of_Market.request.id
        _kwargs = run_group_market_rating_Size_of_Market.request.kwargs
        hostname = run_group_market_rating_Size_of_Market.request.hostname
        logger_start = start_delimiter + \
                       " run_group_market_rating_Size_of_Market Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        group_market_rating_Size_of_Market.run()
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_group_market_rating_Size_of_Market Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_group_market_rating_Size_of_Market.request.id = str(uuid.uuid4())
        run_group_market_rating_Size_of_Market.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_group_market_rating_Growth_of_market3")
##@measure(JUPITER_LOGGER)
def run_group_market_rating_Growth_of_market3():
    try:
        st = time.time()
        task_id = run_group_market_rating_Growth_of_market3.request.id
        _kwargs = run_group_market_rating_Growth_of_market3.request.kwargs
        hostname = run_group_market_rating_Growth_of_market3.request.hostname
        logger_start = start_delimiter + \
                       " run_group_market_rating_Growth_of_market3 Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        group_market_rating_Growth_of_market3.run()
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_group_market_rating_Growth_of_market3 Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_group_market_rating_Growth_of_market3.request.id = str(uuid.uuid4())
        run_group_market_rating_Growth_of_market3.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_group_market_rating_No_of_competitors")
##@measure(JUPITER_LOGGER)
def run_group_market_rating_No_of_competitors():
    try:
        st = time.time()
        task_id = run_group_market_rating_No_of_competitors.request.id
        _kwargs = run_group_market_rating_No_of_competitors.request.kwargs
        hostname = run_group_market_rating_No_of_competitors.request.hostname
        logger_start = start_delimiter + \
                       " run_group_market_rating_No_of_competitors Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        group_market_rating_No_of_competitors.run()
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_group_market_rating_No_of_competitors Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_group_market_rating_No_of_competitors.request.id = str(uuid.uuid4())
        run_group_market_rating_No_of_competitors.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_Restriction_Final")
##@measure(JUPITER_LOGGER)
def run_Restriction_Final():
    try:
        st = time.time()
        task_id = run_Restriction_Final.request.id
        _kwargs = run_Restriction_Final.request.kwargs
        hostname = run_Restriction_Final.request.hostname
        logger_start = start_delimiter + \
                       " run_Restriction_Final Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        Restriction_Final.run()
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_Restriction_Final Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_Restriction_Final.request.id = str(uuid.uuid4())
        run_Restriction_Final.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_Agility")
##@measure(JUPITER_LOGGER)
def run_Agility():
    try:
        st = time.time()
        task_id = run_Agility.request.id
        _kwargs = run_Agility.request.kwargs
        hostname = run_Agility.request.hostname
        logger_start = start_delimiter + \
                       " run_Agility Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        Agility.run()
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_Agility Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_Agility.request.id = str(uuid.uuid4())
        run_Agility.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_Agentsf1_Final")
##@measure(JUPITER_LOGGER)
def run_Agentsf1_Final():
    try:
        st = time.time()
        task_id = run_Agentsf1_Final.request.id
        _kwargs = run_Agentsf1_Final.request.kwargs
        hostname = run_Agentsf1_Final.request.hostname
        logger_start = start_delimiter + \
                       " run_Agentsf1_Final Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        Agentsf1_Final.run()
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_Agentsf1_Final Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_Agentsf1_Final.request.id = str(uuid.uuid4())
        run_Agentsf1_Final.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_pyspark_sales_Flown")
##@measure(JUPITER_LOGGER)
def run_pyspark_sales_Flown():
    try:
        st = time.time()
        task_id = run_pyspark_sales_Flown.request.id
        _kwargs = run_pyspark_sales_Flown.request.kwargs
        hostname = run_pyspark_sales_Flown.request.hostname
        logger_start = start_delimiter + \
                       " run_pyspark_sales_Flown Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        pyspark_sales_Flown.run()
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_pyspark_sales_Flown Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_pyspark_sales_Flown.request.id = str(uuid.uuid4())
        run_pyspark_sales_Flown.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_pyspark_sales")
##@measure(JUPITER_LOGGER)
def run_pyspark_sales():
    try:
        st = time.time()
        task_id = run_pyspark_sales.request.id
        _kwargs = run_pyspark_sales.request.kwargs
        hostname = run_pyspark_sales.request.hostname
        logger_start = start_delimiter + \
                       " run_pyspark_sales Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        pyspark_sales.run()
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_pyspark_sales Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_pyspark_sales.request.id = str(uuid.uuid4())
        run_pyspark_sales.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


# @app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_ita_yqyr")
# ##@measure(JUPITER_LOGGER)
# def run_ita_yqyr(type_actual,comp_actual,docs):
#     st = time.time()
#     logger.info("Running ITA Scraping for YQYR")
#     ITA_Scraping_YQYR.ita_yqyr(type_actual,comp_actual,docs)
#     task_id = run_ita_yqyr.request.id
#     _kwargs = run_ita_yqyr.request.kwargs
#     hostname = run_ita_yqyr.request.hostname
#     time_taken = time.time() - st
#     logger_info = 'run_ita_yqyr Done. Task_id: ' + \
#                   str(task_id) + \
#                   " kwargs: " + \
#                   str(_kwargs) + \
#                   " hostname: " + \
#                   str(hostname) + \
#                   " timetaken: " + \
#                   str(time_taken)
#     logger.info(logger_info)
#     return time_taken


# @app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_compute_competitor_rating")
# ##@measure(JUPITER_LOGGER)
# def run_compute_competitor_rating():
#     st = time.time()
#     logger.info("Running Final Competitor Rating")
#     compute_competitor_rats2.run()
#     task_id = run_compute_competitor_rating.request.id
#     _kwargs = run_compute_competitor_rating.request.kwargs
#     hostname = run_compute_competitor_rating.request.hostname
#     time_taken = time.time() - st
#     logger_info = 'run_compute_competitor_rating Done. Task_id: ' + \
#                   str(task_id) + \
#                   " kwargs: " + \
#                   str(_kwargs) + \
#                   " hostname: " + \
#                   str(hostname) + \
#                   " timetaken: " + \
#                   str(time_taken)
#     logger.info(logger_info)
#     return time_taken


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_market_agents_adhoc")
##@measure(JUPITER_LOGGER)
def run_market_agents_adhoc(snap_date):
    try:
        st = time.time()
        logger.info("Running run_market_agents_adhoc" + str(snap_date))
        market_agents_adhoc(snap_date=snap_date)
        task_id = run_market_agents_adhoc.request.id
        _kwargs = run_market_agents_adhoc.request.kwargs
        hostname = run_market_agents_adhoc.request.hostname
        time_taken = time.time() - st
        logger_info = 'run_market_agents_adhoc' + str(snap_date) + 'Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timetaken: " + \
                      str(time_taken)
        logger.info(logger_info)
        return time_taken
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_market_agents_adhoc.request.id = str(uuid.uuid4())
        run_market_agents_adhoc.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_market_channels_adhoc")
##@measure(JUPITER_LOGGER)
def run_market_channels_adhoc(snap_date):
    st = time.time()
    logger.info("Running run_market_channels_adhoc" + str(snap_date))
    market_channels_adhoc(snap_date=snap_date)
    task_id = run_market_channels_adhoc.request.id
    _kwargs = run_market_channels_adhoc.request.kwargs
    hostname = run_market_channels_adhoc.request.hostname
    time_taken = time.time() - st
    logger_info = 'run_market_channels_adhoc' + str(snap_date) + 'Done. Task_id: ' + \
                  str(task_id) + \
                  " kwargs: " + \
                  str(_kwargs) + \
                  " hostname: " + \
                  str(hostname) + \
                  " timetaken: " + \
                  str(time_taken)
    logger.info(logger_info)
    return time_taken


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_market_distributors_adhoc")
##@measure(JUPITER_LOGGER)
def run_market_distributors_adhoc(snap_date):
    st = time.time()
    logger.info("Running run_market_distributors_adhoc" + str(snap_date))
    market_distributors_adhoc(snap_date=snap_date)
    task_id = run_market_distributors_adhoc.request.id
    _kwargs = run_market_distributors_adhoc.request.kwargs
    hostname = run_market_distributors_adhoc.request.hostname
    time_taken = time.time() - st
    logger_info = 'run_market_distributors_adhoc' + str(snap_date) + 'Done. Task_id: ' + \
                  str(task_id) + \
                  " kwargs: " + \
                  str(_kwargs) + \
                  " hostname: " + \
                  str(hostname) + \
                  " timetaken: " + \
                  str(time_taken)
    logger.info(logger_info)
    return time_taken


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_market_farebrand_adhoc")
##@measure(JUPITER_LOGGER)
def run_market_farebrand_adhoc(snap_date):
    st = time.time()
    logger.info("Running run_market_farebrand_adhoc" + str(snap_date))
    market_farebrand_adhoc(snap_date=snap_date)
    task_id = run_market_farebrand_adhoc.request.id
    _kwargs = run_market_farebrand_adhoc.request.kwargs
    hostname = run_market_farebrand_adhoc.request.hostname
    time_taken = time.time() - st
    logger_info = 'run_market_farebrand_adhoc' + str(snap_date) + 'Done. Task_id: ' + \
                  str(task_id) + \
                  " kwargs: " + \
                  str(_kwargs) + \
                  " hostname: " + \
                  str(hostname) + \
                  " timetaken: " + \
                  str(time_taken)
    logger.info(logger_info)
    return time_taken


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_market_segments_adhoc")
##@measure(JUPITER_LOGGER)
def run_market_segments_adhoc(snap_date):
    st = time.time()
    logger.info("Running run_market_segments_adhoc" + str(snap_date))
    market_segments_adhoc(snap_date=snap_date)
    task_id = run_market_segments_adhoc.request.id
    _kwargs = run_market_segments_adhoc.request.kwargs
    hostname = run_market_segments_adhoc.request.hostname
    time_taken = time.time() - st
    logger_info = 'run_market_segments_adhoc' + str(snap_date) + 'Done. Task_id: ' + \
                  str(task_id) + \
                  " kwargs: " + \
                  str(_kwargs) + \
                  " hostname: " + \
                  str(hostname) + \
                  " timetaken: " + \
                  str(time_taken)
    logger.info(logger_info)
    return time_taken


@app.task(name="jupiter_AI.batch.atpco_automation.Automation_tasks.run_market_flights_adhoc")
##@measure(JUPITER_LOGGER)
def run_market_flights_adhoc(snap_date):
    st = time.time()
    logger.info("Running run_market_flights_adhoc" + str(snap_date))
    market_flights_adhoc(snap_date=snap_date)
    task_id = run_market_flights_adhoc.request.id
    _kwargs = run_market_flights_adhoc.request.kwargs
    hostname = run_market_flights_adhoc.request.hostname
    time_taken = time.time() - st
    logger_info = 'run_market_flights_adhoc' + str(snap_date) + 'Done. Task_id: ' + \
                  str(task_id) + \
                  " kwargs: " + \
                  str(_kwargs) + \
                  " hostname: " + \
                  str(hostname) + \
                  " timetaken: " + \
                  str(time_taken)
    logger.info(logger_info)
    return time_taken


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.performance_response_from_java')
##@measure(JUPITER_LOGGER)
def performance_response_from_java(pos, dep_date_start, dep_date_end):
    try:
        client = mongo_client()
        db= client[JUPITER_DB]
        st = time.time()
        logger.info(
            'Getting Performance Parameter from Java for ' + str(pos) + ' and dates ' + str(dep_date_start) + ' - ' + str(
                dep_date_end))
        hit_url(pos=pos, dep_date_start=dep_date_start, dep_date_end=dep_date_end, db=db)
        task_id = performance_response_from_java.request.id
        _kwargs = performance_response_from_java.request.kwargs
        hostname = performance_response_from_java.request.hostname
        time_taken = time.time() - st
        logger_info = 'Performance Response from Java for Triggers Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timetaken: " + \
                      str(time_taken)
        logger.info(logger_info)
        client.close()
        return time_taken
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        performance_response_from_java.request.id = str(uuid.uuid4())
        performance_response_from_java.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.raise_booking_changes_weekly')
def raise_booking_changes_weekly():
    try:
        client =mongo_client()
        db= client[JUPITER_DB]
        st = time.time()
        logger.info('Raising Booking Changes Weekly Triggers')
        booking_changes_weekly(db=db)
        task_id = raise_booking_changes_weekly.request.id
        _kwargs = raise_booking_changes_weekly.request.kwargs
        hostname = raise_booking_changes_weekly.request.hostname
        time_taken = time.time() - st
        logger_info = 'Booking Changes Weekly Triggers Raised. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timetaken: " + \
                      str(time_taken)
        logger.info(logger_info)
        client.close()
        return time_taken
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        raise_booking_changes_weekly.request.id = str(uuid.uuid4())
        raise_booking_changes_weekly.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.raise_booking_changes_rolling')
def raise_booking_changes_rolling():
    try:
        client = mongo_client()
        db= client[JUPITER_DB]
        st = time.time()
        logger.info('Raising Booking Changes Rolling Triggers')
        booking_changes_rolling(db=db)
        task_id = raise_booking_changes_rolling.request.id
        _kwargs = raise_booking_changes_rolling.request.kwargs
        hostname = raise_booking_changes_rolling.request.hostname
        time_taken = time.time() - st
        logger_info = 'Booking Changes Rolling Triggers Raised. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timetaken: " + \
                      str(time_taken)
        logger.info(logger_info)
        client.close()
        return time_taken
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        raise_booking_changes_rolling.request.id = str(uuid.uuid4())
        raise_booking_changes_rolling.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.raise_booking_changes_vtgt')
def raise_booking_changes_vtgt():
    try:
    #client = mongo_client()
    #db = client[JUPITER_DB]
        st = time.time()
        logger.info('Raising Booking Changes VTGT Triggers')
        booking_changes_vtgt()
        task_id = raise_booking_changes_vtgt.request.id
        _kwargs = raise_booking_changes_vtgt.request.kwargs
        hostname = raise_booking_changes_vtgt.request.hostname
        time_taken = time.time() - st
        logger_info = 'Booking Changes VTGT Triggers Raised. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timetaken: " + \
                      str(time_taken)
        logger.info(logger_info)
        #client.close()
        return time_taken
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        raise_booking_changes_vtgt.request.id = str(uuid.uuid4())
        raise_booking_changes_vtgt.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.raise_booking_changes_vlyr')
def raise_booking_changes_vlyr():
    try:
        client = mongo_client()
        db= client[JUPITER_DB]
        st = time.time()
        logger.info('Raising Booking Changes VLYR Triggers')
        booking_changes_vlyr(db=db)
        task_id = raise_booking_changes_vlyr.request.id
        _kwargs = raise_booking_changes_vlyr.request.kwargs
        hostname = raise_booking_changes_vlyr.request.hostname
        time_taken = time.time() - st
        logger_info = 'Booking Changes VLYR Triggers Raised. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timetaken: " + \
                      str(time_taken)
        logger.info(logger_info)
        client.close()
        return time_taken
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        raise_booking_changes_vlyr.request.id = str(uuid.uuid4())
        raise_booking_changes_vlyr.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.raise_revenue_changes_weekly')
def raise_revenue_changes_weekly():
    try:
        client =mongo_client()
        db= client[JUPITER_DB]
        st = time.time()
        logger.info('Raising Revenue Changes Weekly Triggers')
        revenue_changes_weekly(db=db)
        task_id = raise_revenue_changes_weekly.request.id
        _kwargs = raise_revenue_changes_weekly.request.kwargs
        hostname = raise_revenue_changes_weekly.request.hostname
        time_taken = time.time() - st
        logger_info = 'Revenue Changes Weekly Triggers Raised. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timetaken: " + \
                      str(time_taken)
        logger.info(logger_info)
        client.close()
        return time_taken
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        raise_revenue_changes_weekly.request.id = str(uuid.uuid4())
        raise_revenue_changes_weekly.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.raise_revenue_changes_rolling')
def raise_revenue_changes_rolling():
    try:
        client =mongo_client()
        db= client[JUPITER_DB]
        st = time.time()
        logger.info('Raising revenue Changes Rolling Triggers')
        revenue_changes_rolling(db=db)
        task_id = raise_revenue_changes_rolling.request.id
        _kwargs = raise_revenue_changes_rolling.request.kwargs
        hostname = raise_revenue_changes_rolling.request.hostname
        time_taken = time.time() - st
        logger_info = 'revenue Changes Rolling Triggers Raised. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timetaken: " + \
                      str(time_taken)
        logger.info(logger_info)
        client.close()
        return time_taken
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        raise_revenue_changes_rolling.request.id = str(uuid.uuid4())
        raise_revenue_changes_rolling.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.raise_revenue_changes_vtgt')
def raise_revenue_changes_vtgt():
    try:
        client = mongo_client()
        db=client[JUPITER_DB]
        st = time.time()
        logger.info('Raising revenue Changes VTGT Triggers')
        revenue_changes_vtgt(db=db)
        task_id = raise_revenue_changes_vtgt.request.id
        _kwargs = raise_revenue_changes_vtgt.request.kwargs
        hostname = raise_revenue_changes_vtgt.request.hostname
        time_taken = time.time() - st
        logger_info = 'revenue Changes VTGT Triggers Raised. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timetaken: " + \
                      str(time_taken)
        logger.info(logger_info)
        client.close()
        return time_taken
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        raise_revenue_changes_vtgt.request.id = str(uuid.uuid4())
        raise_revenue_changes_vtgt.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.raise_revenue_changes_vlyr')
def raise_revenue_changes_vlyr():
    try:
        st = time.time()
        logger.info('Raising revenue Changes VLYR Triggers')
        revenue_changes_vlyr()
        task_id = raise_revenue_changes_vlyr.request.id
        _kwargs = raise_revenue_changes_vlyr.request.kwargs
        hostname = raise_revenue_changes_vlyr.request.hostname
        time_taken = time.time() - st
        logger_info = 'revenue Changes VLYR Triggers Raised. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timetaken: " + \
                      str(time_taken)
        logger.info(logger_info)
        return time_taken
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        raise_revenue_changes_vlyr.request.id = str(uuid.uuid4())
        raise_revenue_changes_vlyr.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.raise_pax_changes_weekly')
def raise_pax_changes_weekly():
    try:
        client = mongo_client()
        db =client[JUPITER_DB]
        st = time.time()
        logger.info('Raising pax Changes Weekly Triggers')
        pax_changes_weekly(db=db)
        task_id = raise_pax_changes_weekly.request.id
        _kwargs = raise_pax_changes_weekly.request.kwargs
        hostname = raise_pax_changes_weekly.request.hostname
        time_taken = time.time() - st
        logger_info = 'pax Changes Weekly Triggers Raised. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timetaken: " + \
                      str(time_taken)
        logger.info(logger_info)
        client.close()
        return time_taken
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        raise_pax_changes_weekly.request.id = str(uuid.uuid4())
        raise_pax_changes_weekly.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.raise_pax_changes_rolling')
def raise_pax_changes_rolling():
    try:
        st = time.time()
        logger.info('Raising pax Changes Rolling Triggers')
        pax_changes_rolling()
        task_id = raise_pax_changes_rolling.request.id
        _kwargs = raise_pax_changes_rolling.request.kwargs
        hostname = raise_pax_changes_rolling.request.hostname
        time_taken = time.time() - st
        logger_info = 'pax Changes Rolling Triggers Raised. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timetaken: " + \
                      str(time_taken)
        logger.info(logger_info)
        return time_taken
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        raise_pax_changes_rolling.request.id = str(uuid.uuid4())
        raise_pax_changes_rolling.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.raise_pax_changes_vtgt')
def raise_pax_changes_vtgt():
    try:
        client =mongo_client()
        db=client[JUPITER_DB]
        st = time.time()
        logger.info('Raising pax Changes VTGT Triggers')
        pax_changes_vtgt(db=db)
        task_id = raise_pax_changes_vtgt.request.id
        _kwargs = raise_pax_changes_vtgt.request.kwargs
        hostname = raise_pax_changes_vtgt.request.hostname
        time_taken = time.time() - st
        logger_info = 'pax Changes VTGT Triggers Raised. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timetaken: " + \
                      str(time_taken)
        logger.info(logger_info)
        client.close()
        return time_taken
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        raise_pax_changes_vtgt.request.id = str(uuid.uuid4())
        raise_pax_changes_vtgt.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.raise_pax_changes_vlyr')
def raise_pax_changes_vlyr():
    try:
        client = mongo_client()
        db=client[JUPITER_DB]
        st = time.time()
        logger.info('Raising pax Changes VLYR Triggers')
        pax_changes_vlyr(db=db)
        task_id = raise_pax_changes_vlyr.request.id
        _kwargs = raise_pax_changes_vlyr.request.kwargs
        hostname = raise_pax_changes_vlyr.request.hostname
        time_taken = time.time() - st
        logger_info = 'pax Changes VLYR Triggers Raised. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timetaken: " + \
                      str(time_taken)
        logger.info(logger_info)
        client.close()
        return time_taken
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        raise_pax_changes_vlyr.request.id = str(uuid.uuid4())
        raise_pax_changes_vlyr.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.raise_yield_changes_weekly')
def raise_yield_changes_weekly():
    try:
        client = mongo_client()
        db = client[JUPITER_DB]
        st = time.time()
        logger.info('Raising yield Changes Weekly Triggers')
        yield_changes_weekly(db=db)
        task_id = raise_yield_changes_weekly.request.id
        _kwargs = raise_yield_changes_weekly.request.kwargs
        hostname = raise_yield_changes_weekly.request.hostname
        time_taken = time.time() - st
        logger_info = 'yield Changes Weekly Triggers Raised. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timetaken: " + \
                      str(time_taken)
        logger.info(logger_info)
        client.close()
        return time_taken
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        raise_yield_changes_weekly.request.id = str(uuid.uuid4())
        raise_yield_changes_weekly.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.raise_yield_changes_rolling')
def raise_yield_changes_rolling():
    try:
        client =mongo_client()
        db=client[JUPITER_DB]
        st = time.time()
        logger.info('Raising yield Changes Rolling Triggers')
        yield_changes_rolling(db=db)
        task_id = raise_yield_changes_rolling.request.id
        _kwargs = raise_yield_changes_rolling.request.kwargs
        hostname = raise_yield_changes_rolling.request.hostname
        time_taken = time.time() - st
        logger_info = 'yield Changes Rolling Triggers Raised. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timetaken: " + \
                      str(time_taken)
        logger.info(logger_info)
        client.close()
        return time_taken
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        raise_yield_changes_rolling.request.id = str(uuid.uuid4())
        raise_yield_changes_rolling.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.raise_yield_changes_vtgt')
def raise_yield_changes_vtgt():
    try:
        client = mongo_client()
        db=client[JUPITER_DB]
        st = time.time()
        logger.info('Raising yield Changes VTGT Triggers')
        yield_changes_vtgt(db)
        task_id = raise_yield_changes_vtgt.request.id
        _kwargs = raise_yield_changes_vtgt.request.kwargs
        hostname = raise_yield_changes_vtgt.request.hostname
        time_taken = time.time() - st
        logger_info = 'yield Changes VTGT Triggers Raised. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timetaken: " + \
                      str(time_taken)
        logger.info(logger_info)
        client.close()
        return time_taken
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        raise_yield_changes_vtgt.request.id = str(uuid.uuid4())
        raise_yield_changes_vtgt.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.raise_yield_changes_vlyr')
def raise_yield_changes_vlyr():
    try:
        client = mongo_client()
        db=client[JUPITER_DB]
        st = time.time()
        logger.info('Raising yield Changes VLYR Triggers')
        yield_changes_vlyr(db)
        task_id = raise_yield_changes_vlyr.request.id
        _kwargs = raise_yield_changes_vlyr.request.kwargs
        hostname = raise_yield_changes_vlyr.request.hostname
        time_taken = time.time() - st
        logger_info = 'yield Changes VLYR Triggers Raised. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timetaken: " + \
                      str(time_taken)
        logger.info(logger_info)
        client.close()
        return time_taken
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        raise_yield_changes_vlyr.request.id = str(uuid.uuid4())
        raise_yield_changes_vlyr.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.insert_host_fares')
# #@measure(JUPITER_LOGGER)
def insert_host_fares(pos,
                      dep_date_start,
                      dep_date_end,
                      od_list,
                      do_list,
                      compartment,
                      currency_data,
                      date_ranges,
                      exchange_rate
                      ):
    try:
        st = time.time()
        logger.info('Getting host fares for ' + str(pos) + ' and dates ' + str(dep_date_start) + ' - ' + str(dep_date_end))
        get_host_fares_df(pos=pos,
                          extreme_start_date=dep_date_start,
                          extreme_end_date=dep_date_end,
                          od_list=od_list,
                          do_list=do_list,
                          compartment=compartment,
                          currency_data=currency_data,
                          date_ranges=date_ranges,
                          exchange_rate=exchange_rate
                          )
        task_id = insert_host_fares.request.id
        _kwargs = insert_host_fares.request.kwargs
        hostname = insert_host_fares.request.hostname
        time_taken = time.time() - st
        logger_info = 'Performance Response from Java for Triggers Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timetaken: " + \
                      str(time_taken)
        logger.info(logger_info)
        return time_taken
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        insert_host_fares.request.id = str(uuid.uuid4())
        insert_host_fares.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_fareId')
def run_fareId(pos):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_fareId.request.id
        _kwargs = run_fareId.request.kwargs
        hostname = run_fareId.request.hostname
        logger_start = start_delimiter + \
                       " run_fareId Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        call_fareId(pos=pos, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_fareId Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_fareId.request.id = str(uuid.uuid4())
        run_fareId.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return

@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_yqyr')
def run_yqyr(system_date, file_time, carrier, OD, hub, hub_area, hub_country, hub_zone, origin_area,
                          origin_country, origin_zone, destination_area, destination_country, destination_zone):
    try:
        client = mongo_client()

        st = time.time()
        task_id = run_yqyr.request.id
        _kwargs = run_yqyr.request.kwargs
        hostname = run_yqyr.request.hostname
        logger_start = start_delimiter + \
                       " run_yqyr Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        yqyr_main(system_date=system_date, file_time=file_time, carrier_list=carrier, OD_list=OD, hub=hub, hub_area=hub_area, hub_country=hub_country, hub_zone=hub_zone, origin_area=origin_area,
                          origin_country=origin_country, origin_zone=origin_zone, destination_area=destination_area, destination_country=destination_country, destination_zone=destination_zone, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_yqyr Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_yqyr.request.id = str(uuid.uuid4())
        run_yqyr.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return

@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_yqyr_complete')
def run_yqyr_complete(system_date, carrier, OD, hub, hub_area, hub_country, hub_zone, origin_area,
                          origin_country, origin_zone, destination_area, destination_country, destination_zone):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_yqyr_complete.request.id
        _kwargs = run_yqyr_complete.request.kwargs
        hostname = run_yqyr_complete.request.hostname
        logger_start = start_delimiter + \
                       " run_yqyr_complete Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        yqyr_complete_main(system_date=system_date, carrier_list=carrier, OD_list=OD, hub=hub, hub_area=hub_area, hub_country=hub_country, hub_zone=hub_zone, origin_area=origin_area,
                          origin_country=origin_country, origin_zone=origin_zone, destination_area=destination_area, destination_country=destination_country, destination_zone=destination_zone, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_yqyr_complete Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_yqyr_complete.request.id = str(uuid.uuid4())
        run_yqyr_complete.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_yqyr_changetab')
def run_yqyr_changetab(system_date, file_time):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_yqyr_changetab.request.id
        _kwargs = run_yqyr_changetab.request.kwargs
        hostname = run_yqyr_changetab.request.hostname
        logger_start = start_delimiter + \
                       " run_yqyr_changetab Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        yqyrchangetab_main(system_date=system_date, file_time= file_time,client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_yqyr_changetab Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()

        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_yqyr_changetab.request.id = str(uuid.uuid4())
        run_yqyr_changetab.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_yqyr_tab178190')
def run_yqyr_tab178190(system_date, file_time):
    try:
        client=mongo_client()
        st = time.time()
        task_id = run_yqyr_tab178190.request.id
        _kwargs = run_yqyr_tab178190.request.kwargs
        hostname = run_yqyr_tab178190.request.hostname
        logger_start = start_delimiter + \
                       " run_yqyr_tab178190 Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        yqyr_tab178190_main(system_date=system_date, file_time= file_time,client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_yqyr_tab178190 Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_yqyr_tab178190.request.id = str(uuid.uuid4())
        run_yqyr_tab178190.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_yqyr_tab171198')
def run_yqyr_tab171198(system_date, file_time):
    try:
        client=mongo_client()
        st = time.time()
        task_id = run_yqyr_tab171198.request.id
        _kwargs = run_yqyr_tab171198.request.kwargs
        hostname = run_yqyr_tab171198.request.hostname
        logger_start = start_delimiter + \
                       " run_yqyr_tab178190 Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        yqyr_tab171198_main(system_date=system_date, file_time= file_time, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_yqyr_tab171198 Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_yqyr_tab171198.request.id = str(uuid.uuid4())
        run_yqyr_tab171198.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_yqyr_secpos')
def run_yqyr_secpos(system_date, file_time):
    try:
        client=mongo_client()
        st = time.time()
        task_id = run_yqyr_secpos.request.id
        _kwargs = run_yqyr_secpos.request.kwargs
        hostname = run_yqyr_secpos.request.hostname
        logger_start = start_delimiter + \
                       " run_yqyr_secpos Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        yqyr_secpos_main(system_date=system_date, file_time= file_time, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_yqyr_secpos Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_yqyr_secpos.request.id = str(uuid.uuid4())
        run_yqyr_secpos.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_yqyr_zoneflag')
def run_yqyr_zoneflag():
    try:
        client=mongo_client()
        st = time.time()
        task_id = run_yqyr_zoneflag.request.id
        _kwargs = run_yqyr_zoneflag.request.kwargs
        hostname = run_yqyr_zoneflag.request.hostname
        logger_start = start_delimiter + \
                       " run_yqyr_zoneflag Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        yqyr_zoneflag_main(client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_yqyr_zoneflag Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_yqyr_zoneflag.request.id = str(uuid.uuid4())
        run_yqyr_zoneflag.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_yqyr_s1change')
def run_yqyr_s1change(system_date, file_time):
    try:
        client=mongo_client()
        st = time.time()
        task_id = run_yqyr_s1change.request.id
        _kwargs = run_yqyr_s1change.request.kwargs
        hostname = run_yqyr_s1change.request.hostname
        logger_start = start_delimiter + \
                       " run_yqyr_s1change Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        yqyr_s1change_main(system_date=system_date, file_time= file_time, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_yqyr_s1change Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_yqyr_s1change.request.id = str(uuid.uuid4())
        run_yqyr_s1change.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_yqyr_s1reverse')
def run_yqyr_s1reverse(file_date,file_time):
    try:
        client=mongo_client()
        st = time.time()
        task_id = run_yqyr_s1reverse.request.id
        _kwargs = run_yqyr_s1reverse.request.kwargs
        hostname = run_yqyr_s1reverse.request.hostname
        logger_start = start_delimiter + \
                       " run_yqyr_s1reverse Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        yqyr_s1reverse_main(file_date,file_time, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_yqyr_s1reverse Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_yqyr_s1reverse.request.id = str(uuid.uuid4())
        run_yqyr_s1reverse.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_lff_adhoc')
def run_lff_adhoc(pos, origin, destination, compartment):
    client = mongo_client()
    db = client[JUPITER_DB]
    st = time.time()
    task_id = run_lff.request.id
    _kwargs = run_lff.request.kwargs
    hostname = run_lff.request.hostname
    logger_start = start_delimiter + \
                   " run_lff Started. Task_id: " + \
                   str(task_id) + \
                   " kwargs: " + \
                   str(_kwargs) + \
                   " hostname: " + \
                   str(hostname) + \
                   " timestamp: " + \
                   datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(logger_start)
    update_market_adhoc(pos=pos, origin=origin, destination=destination, compartment=compartment, db=db)
    time_taken = time.time() - st
    logger_info = end_delimiter + \
                  ' run_lff Done. Task_id: ' + \
                  str(task_id) + \
                  " kwargs: " + \
                  str(_kwargs) + \
                  " hostname: " + \
                  str(hostname) + \
                  " timestamp: " + \
                  datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                  " timetaken: " + \
                  str(time_taken)
    return_arg = {'status': 'Done'}
    logger.info(logger_info)
    client.close()
    return return_arg


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_lff')
def run_lff(pos, origin, destination, compartment):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_lff.request.id
        _kwargs = run_lff.request.kwargs
        hostname = run_lff.request.hostname
        logger_start = start_delimiter + \
                       " run_lff Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        update_market(pos=pos, origin=origin, destination=destination, compartment=compartment,client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_lff Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_lff.request.id = str(uuid.uuid4())
        run_lff.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_lff_snap_date')
def run_lff_snap_date(pos, origin, destination, compartment, SYSTEM_DATE):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_lff_snap_date.request.id
        _kwargs = run_lff_snap_date.request.kwargs
        hostname = run_lff_snap_date.request.hostname
        logger_start = start_delimiter + \
                       " run_lff_snap_date Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        update_market_snap(pos=pos, origin=origin, destination=destination, compartment=compartment, client=client,
                      SYSTEM_DATE=SYSTEM_DATE)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_lff_snap_date Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_lff_snap_date.request.id = str(uuid.uuid4())
        run_lff_snap_date.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_lff_non_sig')
def run_lff_non_sig(pos, origin, destination, compartment):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_lff_non_sig.request.id
        _kwargs = run_lff_non_sig.request.kwargs
        hostname = run_lff_non_sig.request.hostname
        logger_start = start_delimiter + \
                       " run_lff_non_sig Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        update_market(pos=pos, origin=origin, destination=destination, compartment=compartment, client=client)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_lff_non_sig Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_lff_non_sig.request.id = str(uuid.uuid4())
        run_lff_non_sig.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_temp_fares_collection')
def run_temp_fares_collection(od):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_temp_fares_collection.request.id
        _kwargs = run_temp_fares_collection.request.kwargs
        hostname = run_temp_fares_collection.request.hostname
        logger_start = start_delimiter + \
                       " run_temp_fares_collection Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        create_temp_fares_collection(client=client, od=od)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_temp_fares_collection Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_temp_fares_collection.request.id = str(uuid.uuid4())
        run_temp_fares_collection.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return

@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.run_exchange_rate')
def run_exchange_rate(system_date,carrier_list):
    try:
        client = mongo_client()
        st = time.time()
        task_id = run_exchange_rate.request.id
        _kwargs = run_exchange_rate.request.kwargs
        hostname = run_exchange_rate.request.hostname
        logger_start = start_delimiter + \
                       " run_exchange_rate Started. Task_id: " + \
                       str(task_id) + \
                       " kwargs: " + \
                       str(_kwargs) + \
                       " hostname: " + \
                       str(hostname) + \
                       " timestamp: " + \
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(logger_start)
        exchange_rate_main(client=client,system_date=system_date,carrier_list=carrier_list)
        time_taken = time.time() - st
        logger_info = end_delimiter + \
                      ' run_exchange_rate Done. Task_id: ' + \
                      str(task_id) + \
                      " kwargs: " + \
                      str(_kwargs) + \
                      " hostname: " + \
                      str(hostname) + \
                      " timestamp: " + \
                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                      " timetaken: " + \
                      str(time_taken)
        return_arg = {'status': 'Done'}
        logger.info(logger_info)
        client.close()
        return return_arg
    except(Exception, ConnectTimeout, HTTPError, ReadTimeout, SSLError, ConnectionError, Timeout, socket.error) as e:
        # exception = sys.exc_info()[0]
        einfo = traceback.format_exc()
        run_exchange_rate.request.id = str(uuid.uuid4())
        run_exchange_rate.send_event("task-failed", traceback=einfo)
        task_failure_handler(exception=e, einfo=einfo)
        # task_failure_handler(signal, sender, task_id, exception, args, traceback, einfo, **kwargs)
        print("type error: " + str(e))
        return


@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.send_exec_stats_mail')
def send_exec_stats_mail(task_list, system_date, subject, fares_vol=None, rules_vol=None):
    import pandas as pd
    from jupiter_AI import client, JUPITER_DB
    db = client[JUPITER_DB]
    crsr = list(db.execution_stats.aggregate([
        {
            "$match": {
                "task_name": {"$in": task_list},
                "task_start_date": system_date
            }
        },
        {
            "$group": {
                "_id": {
                    "group_name": "$group_name"
                },
                "start_time": {
                    "$min": "$task_start_time"
                },
                "end_time": {
                    "$max": "$task_end_time"
                },
                "completed_timestamp": {
                    "$max": "$completed_timestamp"
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "group_name": "$_id.group_name",
                "completed_timestamp": 1,
                "start_time": 1,
                "end_time": 1
            }
        },
        {
            "$sort": {
                "end_time": 1
            }
        }
    ], allowDiskUse=True))
    exc_stats = pd.DataFrame(crsr)
    exc_stats = pd.concat([exc_stats, pd.DataFrame([{'group_name': 'Grand_Total',
                                                     'start_time': min(exc_stats['start_time']),
                                                     'end_time': max(exc_stats['end_time'])}])])

    exc_stats['start_time'] = pd.to_datetime(exc_stats['start_time'], format="%H:%M:%S")
    exc_stats['end_time'] = pd.to_datetime(exc_stats['end_time'], format="%H:%M:%S")
    exc_stats['time_taken'] = exc_stats['end_time'] - exc_stats['start_time']
    exc_stats['time_taken'] = exc_stats['time_taken'] + today
    exc_stats['start_time'] = exc_stats['start_time'].dt.strftime("%H:%M:%S")
    exc_stats['end_time'] = exc_stats['end_time'].dt.strftime("%H:%M:%S")
    exc_stats['time_taken'] = exc_stats['time_taken'].dt.strftime("%H:%M:%S")

    # exc_stats['time_taken'] = exc_stats['time_taken'].apply(lambda x: x.to_seconds())
    # exc_stats['days'] = exc_stats['time_taken'] // (24 * 3600)
    # exc_stats['hrs'] = exc_stats['time_taken'] // 3600
    # exc_stats['mins'] = (exc_stats['time_taken'] % 3600) // 60
    # exc_stats['secs'] = (exc_stats['time_taken'] % 3600) % 60

    return send_mail(exc_stats, subject, fares_vol, rules_vol)
