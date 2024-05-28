import json
import datetime
import logging
import time
import rabbitpy
import pandas as pd
from jupiter_AI.triggers.common import sending_error_mail
from jupiter_AI import RABBITMQ_HOST, RABBITMQ_PASSWORD, RABBITMQ_PORT, RABBITMQ_USERNAME, client, JUPITER_DB, ATPCO_DB, \
    JUPITER_LOGGER
from jupiter_AI import SYSTEM_DATE, today, parameters
from jupiter_AI.batch.Run_triggers import data_level_triggers_batch, segregate_markets
from jupiter_AI.batch.atpco_automation.task_group_mappings import tasks_in_automation
from jupiter_AI.batch.atpco_automation.Automation_tasks import run_fareId, run_lff_non_sig, send_exec_stats_mail
from celery import group
from jupiter_AI.batch.atpco_automation.controlcheck import main
import pika

from jupiter_AI.logutils import measure

url = 'amqp://' + RABBITMQ_USERNAME + \
      ":" + RABBITMQ_PASSWORD + \
      "@" + RABBITMQ_HOST + ":" + str(RABBITMQ_PORT) + "/"

db = client[JUPITER_DB]
today_1 = today - datetime.timedelta(days=1)
SYSTEM_DATE_1 = (today - datetime.timedelta(days=1)).strftime('%Y-%m-%d')


@measure(JUPITER_LOGGER)
def run_non_sig_automation():
    mail_array = []
    # with rabbitpy.Connection(url) as conn:
    #     with conn.channel() as channel:
    #         queue = rabbitpy.Queue(channel, 'lff_3')
    #         print "Waiting for messages from lff_3 queue"
    #         for message in queue:
    #             received_msg = message.json()
    #             if received_msg['message'] == "lff_" + SYSTEM_DATE:
    #                 print 'Received lff message: ', received_msg
    #                 message.ack()
    #                 break
    #             else:
    #                 print "Received wrong lff message: ", received_msg
    #                 message.ack()
    #
    # count_non_sig = db.JUP_DB_Celery_Status.find({'group_name': 'non_sig_group',
    #                                               'start_date': SYSTEM_DATE,
    #                                               'end_time': {'$exists': True}}).count()
    #
    # if count_non_sig == 0:
    #     count = db.JUP_DB_Celery_Status.find({'group_name': 'non_sig_group',
    #                                           'start_date': SYSTEM_DATE}).count()
    #     db.JUP_DB_Celery_Status.insert({'group_name': 'non_sig_group',
    #                                     'start_date': SYSTEM_DATE,
    #                                     'start_time': datetime.datetime.now().strftime('%H:%M'),
    #                                     'attempt_number': count + 1})
    #     # markets = db.JUP_DB_Manual_Triggers_Module.distinct('market_combined', {'trx_date': SYSTEM_DATE})
    #     # Running LFF just for non sig markets
    #     sig_markets, sub_sig_markets, non_sig_markets = segregate_markets()
    #     markets = list(set(non_sig_markets))
    #     lff = []
    #     for i in markets:
    #         lff.append(run_lff_non_sig.s(pos=i[0:3], origin=i[3:6], destination=i[6:9], compartment=i[9:10]))
    #     group1 = group(lff)
    #     res1 = group1()

    count_fi = db.JUP_DB_Celery_Status.find({'group_name': 'fareId_group',
                                                 'start_date': SYSTEM_DATE,
                                                 'end_time': {'$exists': True}}).count()

    # if (count_fareId == 0) or (count_lff == 0):
    if count_fi == 0:
        with rabbitpy.Connection(url) as conn:
            with conn.channel() as channel:
                queue = rabbitpy.Queue(channel, 'fareId')
                print "Waiting for messages from fareId queue"
                for message in queue:
                    received_msg = message.json()
                    if received_msg['message'] == "fareId_" + SYSTEM_DATE:
                        print 'Received fareId message: ', received_msg
                        fareId_msg = 1
                        message.ack()
                        break
                    else:
                        print "Received wrong fareId message: ", received_msg
                        message.ack()
        count_fi = db.JUP_DB_Celery_Status.find({'group_name': 'fareId_group',
                                                'start_date': SYSTEM_DATE}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'fareId_group',
                                                'start_date': SYSTEM_DATE,
                                                'start_time': datetime.datetime.now().strftime('%H:%M'),
                                                'attempt_number': count_fi + 1})
        pos_list = list(
            db.JUP_DB_Manual_Triggers_Module.distinct('pos.City', {'pos': {'$ne': None}, 'trx_date': SYSTEM_DATE}))
        mtt = []
        for pos in pos_list:
            mtt.append(run_fareId.s(pos=pos))
        group2 = group(mtt)
        res2 = group2()

        # if count_non_sig==0:
        #     grp_res1 = res1.get()
        #     check = sending_error_mail(system_date=SYSTEM_DATE, group='non_sig_group', db=db,
        #                                attempt=count + 1)
        #     if check ==0:
        #         db.JUP_DB_Celery_Status.update({'group_name': 'non_sig_group',
        #                                         'start_date': SYSTEM_DATE,
        #                                         'attempt_number': count + 1},
        #                                        {'$set': {
        #                                            'end_date': SYSTEM_DATE,
        #                                            'end_time': datetime.datetime.now().strftime('%H:%M')
        #                                        }})
        #         mail_array.append("run_lff_non_sig")
        #     else:
        #         return

        if count_fi==0:
            grp_res2 = res2.get()
            check = sending_error_mail(system_date=SYSTEM_DATE, group='fareId_group', db=db,
                                       attempt=count_fi+ 1)
            if check == 0:
                db.JUP_DB_Celery_Status.update({'group_name': 'fareId_group',
                                            'start_date': SYSTEM_DATE,
                                            'attempt_number': count_fi + 1},
                                           {'$set': {
                                               'end_date': SYSTEM_DATE,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
                mail_array.append("run_fareId")
            else:
                return

    if len(mail_array) > 0:
        mail_group = group([send_exec_stats_mail.s(task_list=mail_array,
                                                   system_date=SYSTEM_DATE,
                                                   subject="Non Sig | LFF|FareId")])()
        print mail_group.get()


if __name__ == '__main__':
    run_non_sig_automation()
    main(client=client)