import json
import datetime
import logging
import time
import rabbitpy
import pandas as pd
from jupiter_AI import RABBITMQ_HOST, RABBITMQ_PASSWORD, RABBITMQ_PORT, RABBITMQ_USERNAME, client, JUPITER_DB, ATPCO_DB, \
    JUPITER_LOGGER
from jupiter_AI import SYSTEM_DATE, today, parameters
from jupiter_AI.triggers.common import sending_error_mail
# from jupiter_AI.batch.Run_triggers import data_level_triggers_batch, segregate_markets
from jupiter_AI.batch.atpco_automation.task_group_mappings import tasks_in_automation
from jupiter_AI.batch.atpco_automation.Automation_tasks import run_manual_trigger_WMQY_rolledUp, \
    run_manual_trigger_WMQY_market_significant, run_manual_trigger_WMQY_event, send_exec_stats_mail, \
    run_manual_trigger_WMQY_quarterly, run_manual_trigger_WMQY_monthly, run_manual_trigger_WMQY_yearly, \
    run_manual_trigger_WMQY_weekly
from celery import group
import pika

from jupiter_AI.logutils import measure

url = 'amqp://' + RABBITMQ_USERNAME + \
      ":" + RABBITMQ_PASSWORD + \
      "@" + RABBITMQ_HOST + ":" + str(RABBITMQ_PORT) + "/"
db = client[JUPITER_DB]
today_1 = today - datetime.timedelta(days=1)
SYSTEM_DATE_1 = (today - datetime.timedelta(days=1)).strftime('%Y-%m-%d')


@measure(JUPITER_LOGGER)
def manual_trigger_wm_scripts():
    mail_array = []
    # with rabbitpy.Connection(url) as conn:
    #     with conn.channel() as channel:
    #         queue = rabbitpy.Queue(channel, 'lff')
    #         print "Waiting for messages from lff queue"
    #         for message in queue:
    #             received_msg = message.json()
    #             if received_msg['message'] == "lff_" + SYSTEM_DATE:
    #                 print 'Received lff message: ', received_msg
    #                 message.ack()
    #                 break
    #             else:
    #                 print "Received wrong lff message: ", received_msg
    #                 message.ack()
    ## Weekly
    ## POS list for globally
    pos_list = list(db.JUP_DB_Manual_Triggers_Module_Summary.distinct('pos.City'))

    lp_tr_tile = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_WMQY_weekly',
                                               'start_date': SYSTEM_DATE,
                                               'end_time': {'$exists': True}}).count()
    if lp_tr_tile == 0:
        count = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_WMQY_weekly',
                                              'start_date': SYSTEM_DATE}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'manual_trigger_WMQY_weekly',
                                        'start_date': SYSTEM_DATE,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count + 1})


        lp_Tr = []
        for each_market in pos_list:
            lp_Tr.append(run_manual_trigger_WMQY_weekly.s(pos=each_market))
        group1 = group(lp_Tr)
        res1 = group1()
        grp_res1 = res1.get()
        temp_manual_trigger_WMQY_weekly = count+1
        check = sending_error_mail(system_date=SYSTEM_DATE, group='manual_trigger_WMQY_weekly',
                                   db=db, attempt= temp_manual_trigger_WMQY_weekly)
        if check == 0:
            db.JUP_DB_Celery_Status.update({'group_name': 'manual_trigger_WMQY_weekly',
                                            'start_date': SYSTEM_DATE,
                                            'attempt_number': count + 1},
                                           {'$set': {
                                               'end_date': SYSTEM_DATE,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
            mail_array.append("run_manual_trigger_WMQY_weekly")
        else:
            return

    ## Monthly
    lp_tr_tile = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_WMQY_monthly',
                                               'start_date': SYSTEM_DATE,
                                               'end_time': {'$exists': True}}).count()
    if lp_tr_tile == 0:
        count = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_WMQY_monthly',
                                              'start_date': SYSTEM_DATE}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'manual_trigger_WMQY_monthly',
                                        'start_date': SYSTEM_DATE,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count + 1})

        lp_Tr = []
        for each_market in pos_list:
            lp_Tr.append(run_manual_trigger_WMQY_monthly.s(pos=each_market))
        group1 = group(lp_Tr)
        res1 = group1()
        grp_res1 = res1.get()
        temp_manual_trigger_WMQY_monthly = count+1
        check = sending_error_mail(system_date=SYSTEM_DATE, group='manual_trigger_WMQY_monthly', db=db, attempt=temp_manual_trigger_WMQY_monthly)
        if check==0:
            db.JUP_DB_Celery_Status.update({'group_name': 'manual_trigger_WMQY_monthly',
                                            'start_date': SYSTEM_DATE,
                                            'attempt_number': count + 1},
                                           {'$set': {
                                               'end_date': SYSTEM_DATE,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
            mail_array.append("run_manual_trigger_WMQY_monthly")
        else:
            return

    '''
    ## RolledUp
    lp_tr_tile = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_WMQY_rolledUp',
                                               'start_date': SYSTEM_DATE,
                                               'end_time': {'$exists': True}}).count()
    if lp_tr_tile == 0:
        count = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_WMQY_rolledUp',
                                              'start_date': SYSTEM_DATE}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'manual_trigger_WMQY_rolledUp',
                                        'start_date': SYSTEM_DATE,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count + 1})

        pos_list = db.JUP_DB_Manual_Triggers_Module_Summary.distinct('pos.City')
        lp_Tr = []
        for each_market in pos_list:
            lp_Tr.append(run_manual_trigger_WMQY_rolledUp.s(pos=each_market))
        group1 = group(lp_Tr)
        res1 = group1()
        grp_res1 = res1.get()
        temp_manual_trigger_WMQY_rolledUp = count+1
        check = sending_error_mail(system_date=SYSTEM_DATE, group='manual_trigger_WMQY_rolledUp', db=db, attempt=temp_manual_trigger_WMQY_rolledUp)
        if check==0:
            db.JUP_DB_Celery_Status.update({'group_name': 'manual_trigger_WMQY_rolledUp',
                                            'start_date': SYSTEM_DATE,
                                            'attempt_number': count + 1},
                                           {'$set': {
                                               'end_date': SYSTEM_DATE,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
            mail_array.append("run_manual_trigger_WMQY_rolledUp")
        else:
            return
    '''
    print 'Publishing completion message to lff_2 queue'
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    message = json.dumps({'message': 'lff_' + SYSTEM_DATE})
    channel.basic_publish(exchange='',
                          routing_key='lff_2',
                          body=message
                          )


    ## Quarterly
    lp_tr_tile = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_WMQY_quarterly',
                                               'start_date': SYSTEM_DATE,
                                               'end_time': {'$exists': True}}).count()
    if lp_tr_tile == 0:
        count = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_WMQY_quarterly',
                                              'start_date': SYSTEM_DATE}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'manual_trigger_WMQY_quarterly',
                                        'start_date': SYSTEM_DATE,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count + 1})

        lp_Tr = []
        for each_market in pos_list:
            lp_Tr.append(run_manual_trigger_WMQY_quarterly.s(pos=each_market))
        group1 = group(lp_Tr)
        res1 = group1()
        grp_res1 = res1.get()
        temp_manual_trigger_WMQY_quarterly = count+1
        check = sending_error_mail(system_date=SYSTEM_DATE, group='manual_trigger_WMQY_quarterly', db=db, attempt=temp_manual_trigger_WMQY_quarterly)
        if check == 0:
            db.JUP_DB_Celery_Status.update({'group_name': 'manual_trigger_WMQY_quarterly',
                                            'start_date': SYSTEM_DATE,
                                            'attempt_number': count + 1},
                                           {'$set': {
                                               'end_date': SYSTEM_DATE,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
            mail_array.append("run_manual_trigger_WMQY_quarterly")
        else:
            return

    ## Yearly
    ## As per flydubai requierment we dont need to process yearly data in summary collection.
    '''
    lp_tr_tile = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_WMQY_yearly',
                                                  'start_date': SYSTEM_DATE,
                                                  'end_time': {'$exists': True}}).count()
    if lp_tr_tile == 0:
        count = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_WMQY_yearly',
                                              'start_date': SYSTEM_DATE}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'manual_trigger_WMQY_yearly',
                                        'start_date': SYSTEM_DATE,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count + 1})

        pos_list = db.JUP_DB_Manual_Triggers_Module_Summary.distinct('pos.City')
        lp_Tr = []
        for each_market in pos_list:
            lp_Tr.append(run_manual_trigger_WMQY_yearly.s(pos=each_market))
        group1 = group(lp_Tr)
        res1 = group1()
        grp_res1 = res1.get()
        db.JUP_DB_Celery_Status.update({'group_name': 'manual_trigger_WMQY_yearly',
                                        'start_date': SYSTEM_DATE,
                                        'attempt_number': count + 1},
                                       {'$set': {
                                           'end_date': SYSTEM_DATE,
                                           'end_time': datetime.datetime.now().strftime('%H:%M')
                                       }})
        mail_array.append("run_manual_trigger_WMQY_yearly")
    
    ## Event
    lp_tr_tile = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_WMQY_event',
                                               'start_date': SYSTEM_DATE,
                                               'end_time': {'$exists': True}}).count()
    if lp_tr_tile == 0:
        count = db.JUP_DB_Celery_Status.find({'group_name': 'manual_trigger_WMQY_event',
                                              'start_date': SYSTEM_DATE}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'manual_trigger_WMQY_event',
                                        'start_date': SYSTEM_DATE,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count + 1})

        pos_list = db.JUP_DB_Manual_Triggers_Module_Summary.distinct('pos.City')
        lp_Tr = []
        for each_market in pos_list:
            lp_Tr.append(run_manual_trigger_WMQY_event.s(pos=each_market))
        group1 = group(lp_Tr)
        res1 = group1()
        grp_res1 = res1.get()
        temp_manual_trigger_WMQY_event = count+1
        check = sending_error_mail(system_date=SYSTEM_DATE, group='manual_trigger_WMQY_event', db=db, attempt=temp_manual_trigger_WMQY_event)
        if check ==0:
            db.JUP_DB_Celery_Status.update({'group_name': 'manual_trigger_WMQY_event',
                                            'start_date': SYSTEM_DATE,
                                            'attempt_number': count + 1},
                                           {'$set': {
                                               'end_date': SYSTEM_DATE,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
            mail_array.append("run_manual_trigger_WMQY_event")
        else:
            return
    '''

    if len(mail_array) > 0:
        mail_group = group([send_exec_stats_mail.s(task_list=mail_array,
                                                   system_date=SYSTEM_DATE,
                                                   subject="Manual Trigger Module WMYQ summary Scripts")])()
        print mail_group.get()


if __name__ == '__main__':
    manual_trigger_wm_scripts()
