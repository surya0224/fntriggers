import json
import datetime
import logging
import time
import rabbitpy
import requests
import pandas as pd
from jupiter_AI import RABBITMQ_HOST, RABBITMQ_PASSWORD,JAVA_LP_URL,mongo_client, RABBITMQ_PORT, RABBITMQ_USERNAME, client, JUPITER_DB, ATPCO_DB, \
    JUPITER_LOGGER
from jupiter_AI import SYSTEM_DATE, today, parameters
from jupiter_AI.batch.Run_triggers import data_level_triggers_batch, segregate_markets
from jupiter_AI.batch.atpco_automation.task_group_mappings import tasks_in_automation
from jupiter_AI.triggers.common import sending_error_mail
from jupiter_AI.batch.JUP_AI_Significant_POSODC import get_pax_markets, get_revenue_markets, get_dxb_markets
from jupiter_AI.batch.atpco_automation.Automation_tasks import run_host_od_capacity, \
    run_comp_od_capacity, \
    run_pos_od_compartment, \
    run_market_significance, \
    run_golden_flights, \
    send_msg, \
    run_copper_flights, \
    run_zinc_flights, \
    run_market_agents, \
    run_market_flights, \
    run_market_farebrand, \
    run_market_distributors, \
    run_market_channels, \
    run_booking_triggers, \
    run_events_triggers, \
    run_pax_triggers, \
    run_revenue_triggers, \
    run_yield_triggers, \
    run_opp_trend_triggers, \
    run_pccp_triggers, \
    run_group_airline_rating, \
    run_group_prod_rating, \
    run_market_rating_mktshare, \
    run_group_market_rating_Growth_of_market3, \
    run_group_market_rating_Size_of_Market, \
    run_group_market_rating_No_of_competitors, \
    run_capacity_rating_blocktime, \
    run_capacity_rating_freq, \
    run_capacity_rating_cap, \
    run_Agentsf1_Final, \
    run_Agility, \
    run_Restriction_Final, \
    run_emirates_promotions_triggers, \
    run_fareId, \
    run_market_segments, send_exec_stats_mail, run_lff, run_footnote_coll

#from jupiter_AI.batch.atpco_automation.controlcheck import main

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
def run_fzdb_automation():
    pnr_msg = 0
    host_od_msg = 0
    #fareId_msg = 0
    lff_msg =0
    mail_array = []

    with rabbitpy.Connection(url) as conn:
        with conn.channel() as channel:
            queue = rabbitpy.Queue(channel, 'automation_pipeline')
            print "Waiting for messages from automation_pipeline queue"
            for message in queue:
                received_msg = message.json()
                if received_msg['message'] == "manual_trigger_" + SYSTEM_DATE:
                    print 'Received manual_trigger message: ', received_msg
                    pnr_msg = 1
                    message.ack()
                    break
                else:
                    print "Received wrong manual_trigger message: ", received_msg
                    message.ack()

    count_market_group = db.JUP_DB_Celery_Status.find({'group_name': 'market_significance',
                                                       'start_date': SYSTEM_DATE,
                                                       'end_time': {'$exists': True}}).count()
    if count_market_group == 0:
        with rabbitpy.Connection(url) as conn:
            with conn.channel() as channel:
                queue = rabbitpy.Queue(channel, 'host_od_capacity_2')
                print "Waiting for messages from host_od_capacity_2 queue"
                for message in queue:
                    received_msg = message.json()
                    if received_msg['message'] == "host_od_capacity_" + SYSTEM_DATE:
                        print 'Received host_od_capacity_2 message: ', received_msg
                        host_od_msg = 1
                        message.ack()
                        break
                    else:
                        print "received wrong host_od_capacity_2 message: ", received_msg
                        message.ack()

        if (host_od_msg == 1) and (pnr_msg == 1):
            count_mkt = db.JUP_DB_Celery_Status.find({'group_name': 'market_significance',
                                                      'start_date': SYSTEM_DATE}).count()
            db.JUP_DB_Celery_Status.insert({'group_name': 'market_significance',
                                            'start_date': SYSTEM_DATE,
                                            'start_time': datetime.datetime.now().strftime('%H:%M'),
                                            'attempt_number': count_mkt + 1})

            group0 = group([run_market_significance.s()])
            res0 =group0()

            # group0 = group([
            #     run_market_channels.s(),
            #     run_market_farebrand.s(),
            #     run_market_distributors.s(),
            #     run_market_agents.s(),
            #     run_market_segments.s(),
            #     run_market_significance.s()])
            # res0 = group0()

    # count_fareId = db.JUP_DB_Celery_Status.find({'group_name': 'fareId_group',
    #                                              'start_date': SYSTEM_DATE,
    #                                              'end_time': {'$exists': True}}).count()

    # count_lff = db.JUP_DB_Celery_Status.find({'group_name': 'lff_hff_group',
    #                                           'start_date': SYSTEM_DATE,
    #                                           'end_time': {'$exists': True}}).count()

    # if (count_fareId == 0) or (count_lff == 0):
    # if count_lff == 0:
    #     with rabbitpy.Connection(url) as conn:
    #         with conn.channel() as channel:
    #             queue = rabbitpy.Queue(channel, 'fareId')
    #             print "Waiting for messages from fareId queue"
    #             for message in queue:
    #                 received_msg = message.json()
    #                 if received_msg['message'] == "fareId_" + SYSTEM_DATE:
    #                     print 'Received fareId message: ', received_msg
    #                     fareId_msg = 1
    #                     message.ack()
    #                     break
    #                 else:
    #                     print "Received wrong fareId message: ", received_msg
    #                     message.ack()
    # if count_lff == 0:
    #     with rabbitpy.Connection(url) as conn:
    #         with conn.channel() as channel:
    #             queue = rabbitpy.Queue(channel, 'lff_hff_group')
    #             print "Waiting for messages from lff_hff_group queue"
    #             for message in queue:
    #                 received_msg = message.json()
    #                 if received_msg['message'] == "lff_hff_" + SYSTEM_DATE:
    #                     print 'Received host_od_capacity_2 message: ', received_msg
    #                     lff_msg = 1
    #                     message.ack()
    #                     break
    #                 else:
    #                     print "received wrong lff_hff_2 message: ", received_msg
    #                     message.ack()
    #
    #
    #
    # if (pnr_msg == 1) and lff_msg == 1:
    #     # if count_fareId == 0:
    #     #     count_fi = db.JUP_DB_Celery_Status.find({'group_name': 'fareId_group',
    #     #                                              'start_date': SYSTEM_DATE}).count()
    #     #     db.JUP_DB_Celery_Status.insert({'group_name': 'fareId_group',
    #     #                                     'start_date': SYSTEM_DATE,
    #     #                                     'start_time': datetime.datetime.now().strftime('%H:%M'),
    #     #                                     'attempt_number': count_fi + 1})
    #     #     group1 = group([run_fareId.s(i) for i in range(1, 13)])
    #     #     res1 = group1()
    #
    #     if count_lff == 0:
    #         count_lff_hff = db.JUP_DB_Celery_Status.find({'group_name': 'lff_hff_group',
    #                                                       'start_date': SYSTEM_DATE}).count()
    #         db.JUP_DB_Celery_Status.insert({'group_name': 'lff_hff_group',
    #                                         'start_date': SYSTEM_DATE,
    #                                         'start_time': datetime.datetime.now().strftime('%H:%M'),
    #                                         'attempt_number': count_lff_hff + 1})
    #         # markets = db.JUP_DB_Manual_Triggers_Module.distinct('market_combined', {'trx_date': SYSTEM_DATE})
    #         # Running LFF just for sig markets
    #         sig_markets, sub_sig_markets, non_sig_markets = segregate_markets()
    #         markets = list(set(sig_markets + sub_sig_markets))
    #         lff = []
    #         for i in markets:
    #             lff.append(run_lff.s(pos=i[0:3], origin=i[3:6], destination=i[6:9], compartment=i[9:10]))
    #         group2 = group(lff)
    #         res2 = group2()

    if (host_od_msg == 1) and (pnr_msg == 1):
        grp_res0 = res0.get()
        temp = count_mkt+1
        check = sending_error_mail(system_date=SYSTEM_DATE, group='market_significance', db=db, attempt=temp)
        if check == 0:
            # mail_array = mail_array + ["run_market_channels",
            #                            "run_market_farebrand",
            #                            "run_market_distributors",
            #                            "run_market_agents",
            #                            "run_market_segments",
            #                            "run_market_significance"]
            mail_array = mail_array+["run_market_significance"]
            db.JUP_DB_Celery_Status.update({'group_name': 'market_significance',
                                            'start_date': SYSTEM_DATE,
                                            'attempt_number': count_mkt + 1},
                                           {'$set': {
                                               'end_date': SYSTEM_DATE,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
        else:
            return

    #if (pnr_msg == 1) and (fareId_msg == 1):
    if lff_msg == 1 and pnr_msg ==1:
        # grp_res2 = res2.get()
        # temp = count_lff_hff+1
        # check = sending_error_mail(system_date=SYSTEM_DATE, group='lff_hff_group', db=db, attempt=temp)
        # if check == 0:
        #     print 'Publishing lff completion message to lff queue'
        #     connection = pika.BlockingConnection(parameters)
        #     channel = connection.channel()
        #     message = json.dumps({'message': 'lff_' + SYSTEM_DATE})
        #     channel.basic_publish(exchange='',
        #                           routing_key='lff',
        #                           body=message
        #                           )
        #     channel.basic_publish(exchange='',
        #                           routing_key='lff_3',
        #                           body=message
        #                           )
        #     db.JUP_DB_Celery_Status.update({'group_name': 'lff_hff_group',
        #                                     'start_date': SYSTEM_DATE,
        #                                     'attempt_number': count_lff_hff + 1},
        #                                    {'$set': {
        #                                        'end_date': SYSTEM_DATE,
        #                                        'end_time': datetime.datetime.now().strftime('%H:%M')
        #                                    }})
        #     mail_array.append("run_lff")

            # if count_fareId == 0:
            #     grp_res1 = res1.get()
            #     db.JUP_DB_Celery_Status.update({'group_name': 'fareId_group',
            #                                     'start_date': SYSTEM_DATE,
            #                                     'attempt_number': count_fi + 1},
            #                                    {'$set': {
            #                                        'end_date': SYSTEM_DATE,
            #                                        'end_time': datetime.datetime.now().strftime('%H:%M')
            #                                    }})
            #     mail_array.append("run_fareId")
        # else:
        #     return
        if len(mail_array) > 0:
            mail_group = group([send_exec_stats_mail.s(task_list=mail_array,
                                                       system_date=SYSTEM_DATE,
                                                       subject="Market Significance | LFF")])()
            print mail_group.get()




        # group_component_ratings = group([run_group_airline_rating.s(),
    #                                 run_group_market_rating_Size_of_Market.s(),
    #                                 run_group_market_rating_Growth_of_market3.s(),
    #                                 run_group_market_rating_No_of_competitors.s(),
    #                                 run_group_prod_rating.s(),
    #                                 run_capacity_rating_blocktime.s(),
    #                                 run_capacity_rating_freq.s(),
    #                                 run_capacity_rating_cap.s(),
    #                                 run_market_rating_mktshare.s(),
    #                                 #run_Restriction_Final.s(),
    #                                 run_Agility.s(),
    #                                 run_Agentsf1_Final.s()])

    # grp_component_ratings_res = group_component_ratings()
    # grp_component_ratings_res_ = grp_component_ratings_res.get()

    # group_ratings = group([run_compute_competitor_rating.s()])
    # group_ratings_res = group_ratings()
    # group_ratings_res_ = group_ratings_res.get()

    # pos_od_msg = {"message": "pos_od_compartment_"+SYSTEM_DATE}
    # channel.basic_publish(routing_key="pos_od_compartment", exchange="", body=json.dumps(pos_od_msg))

    # with rabbitpy.Connection(url) as conn:
    #     with conn.channel() as channel:
    #         queue = rabbitpy.Queue(channel, 'automation_pipeline')
    #         print "Waiting for messages from automation pipeline queue"
    #         for message in queue:
    #             if (msg_2 != 1) or (msg_3 != 1):
    #                 received_msg = message.json()
    #                 if received_msg['message'] == "atpco_"+SYSTEM_DATE_1:
    #                     print 'Received ATPCO message'
    #                     msg_2 = 1
    #                     message.ack()
    #                 elif received_msg['message'] == "manual_trigger_"+SYSTEM_DATE:
    #                     print 'Received manual Trigger message'
    #                     msg_3 = 1
    #                     message.ack()
    #                 else:
    #                     print "Received wrong message: ", received_msg
    #                     message.ack()
    #             if (msg_2 == 1) and (msg_3 ==1):
    #                 break
    #
    #
    # if (msg_2 == 1) and (msg_3 ==1):
    #     msg_2 = 0
    #     msg_3 = 0
    # group_triggers = group([run_booking_triggers.s(),
    #                         run_yield_triggers.s(),
    #                         run_revenue_triggers.s(),
    #                         run_pax_triggers.s(),
    #                         run_events_triggers.s(),
    #                         run_pccp_trend_triggers.s(),
    #                         run_etihad_promotions_triggers.s(),
    #                         run_emirates_promotions_triggers.s()])
    # group_res = group_triggers()
    # print "Running Triggers"

    # print "Ratings"


@measure(JUPITER_LOGGER)
def run_triggers():
    lff_msg = 0
    atpco_master_msg = 0
    run_automation_2_msg = 0
    with rabbitpy.Connection(url) as conn:
        with conn.channel() as channel:
            queue = rabbitpy.Queue(channel, 'lff_2')
            print "Waiting for messages from lff queue"
            for message in queue:
                received_msg = message.json()
                if received_msg['message'] == "lff_" + SYSTEM_DATE:
                    print 'Received correct lff message: ', received_msg
                    lff_msg = 1
                    message.ack()
                    break
                else:
                    print "Received wrong lff message: ", received_msg
                    message.ack()
    with rabbitpy.Connection(url) as conn:
        with conn.channel() as channel:
            queue = rabbitpy.Queue(channel, 'atpco_master')
            print "Waiting for messages from atpco_master queue"
            for message in queue:
                received_msg = message.json()
                if received_msg['message'] == "atpco_master_" + SYSTEM_DATE:
                    print 'Received correct atpco_master message: ', received_msg
                    atpco_master_msg = 1
                    message.ack()
                    break
                else:
                    print "Received wrong atpco_master message: ", received_msg
                    message.ack()
    with rabbitpy.Connection(url) as conn:
        with conn.channel() as channel:
            queue = rabbitpy.Queue(channel, 'run_automation_2')
            print "Waiting for messages from run_automation_2 queue"
            for message in queue:
                received_msg = message.json()
                if received_msg['message'] == "run_automation_2_" + SYSTEM_DATE:
                    print 'Received correct run_automation_2 message: ', received_msg
                    run_automation_2_msg = 1
                    message.ack()
                    break
                else:
                    print "Received wrong run_automation_2 message: ", received_msg
                    message.ack()

    if (run_automation_2_msg == 1) and (atpco_master_msg == 1) and (lff_msg == 1):
        st = time.time()
        count_triggers = db.JUP_DB_Celery_Status.find({'group_name': 'triggers_group',
                                                       'start_date': SYSTEM_DATE,
                                                       'end_time': {'$exists': True}}).count()

        if count_triggers == 0:
            count_trig = db.JUP_DB_Celery_Status.find({'group_name': 'triggers_group',
                                                       'start_date': SYSTEM_DATE}).count()
            db.JUP_DB_Celery_Status.insert({'group_name': 'triggers_group',
                                            'start_date': SYSTEM_DATE,
                                            'start_time': datetime.datetime.now().strftime('%H:%M'),
                                            'attempt_number': count_trig + 1})

            data_level_triggers_batch()
            temp = count_trig+1
            check = sending_error_mail(system_date=SYSTEM_DATE, group='triggers_group', db=db, attempt=temp)
            if check == 0:
                mail_array = ["run_booking_triggers",
                              "run_pax_triggers",
                              "run_revenue_triggers",
                              "run_yield_triggers",
                              "run_events_triggers",
                              "run_opp_trend_triggers",
                              "run_pccp_triggers"]


                db.JUP_DB_Celery_Status.update({'group_name': 'triggers_group',
                                                'start_date': SYSTEM_DATE,
                                                'attempt_number': count_trig + 1},
                                               {'$set': {
                                                   'end_date': SYSTEM_DATE,
                                                   'end_time': datetime.datetime.now().strftime('%H:%M')
                                               }})
            else:
                return

            mail_group = group([send_exec_stats_mail.s(task_list=mail_array,
                                                       system_date=SYSTEM_DATE,
                                                       subject="Triggers")])()
            print mail_group.get()
        print 'Total time taken to raise and analyze triggers:', time.time() - st

    # type_list = ["OW", "RT"]
    # comp_list = ["Y", "J"]
    # # create docs
    # ita_group = []
    # len_wf = db.JUP_DB_Workflow_OD_User.find({'update_date': SYSTEM_DATE}).count()
    # limits = 100
    # skips = 0
    # docs = int(len_wf / limits)
    # for i in range(0, docs + 1):
    #     for type_ in type_list:
    #         for comp_ in comp_list:
    #             ita_group.append(run_ita_yqyr(type_, comp_, {"skips": skips, "limits": limits}))
    #     skips = skips + limits
    # ita_group = group(ita_group)
    # res_ita_group = ita_group()

def run_after_triggers():
    mkt=0
    fn=0
    mail_array=[]
    count_market_group = db.JUP_DB_Celery_Status.find({'group_name': 'market_group',
                                                       'start_date': SYSTEM_DATE,
                                                       'end_time': {'$exists': True}}).count()
    if count_market_group == 0:
        mkt = 1

        count_mkt = db.JUP_DB_Celery_Status.find({'group_name': 'market_group',
                                                  'start_date': SYSTEM_DATE}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'market_group',
                                        'start_date': SYSTEM_DATE,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count_mkt + 1})

        group0 = group([run_market_channels.s(),
                        run_market_farebrand.s(),
                        run_market_distributors.s(),
                        run_market_agents.s(),
                        run_market_segments.s()])
        res0 = group0()

    count_footnote_coll = db.JUP_DB_Celery_Status.find({'group_name': 'available_footnote_group',
                                                        'start_date': SYSTEM_DATE,
                                                        'end_time': {'$exists': True}}).count()
    if count_footnote_coll == 0:
        fn = 1
        count_fn = db.JUP_DB_Celery_Status.find({'group_name': 'available_footnote_group',
                                                 'start_date': SYSTEM_DATE}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'available_footnote_group',
                                        'start_date': SYSTEM_DATE,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count_fn + 1})
        group_6 = group([run_footnote_coll.s()])
        res6 = group_6()

    if mkt==1:
        grp_res0 = res0.get()
        temp = count_mkt + 1
        check = sending_error_mail(system_date=SYSTEM_DATE, group='market_group', db=db, attempt=temp)
        if check == 0:
            # mail_array = mail_array + ["run_market_channels",
            #                            "run_market_farebrand",
            #                            "run_market_distributors",
            #                            "run_market_agents",
            #                            "run_market_segments",
            #                            "run_market_significance"]
            mail_array = mail_array + ["run_market_group"]
            db.JUP_DB_Celery_Status.update({'group_name': 'market_group',
                                            'start_date': SYSTEM_DATE,
                                            'attempt_number': count_mkt + 1},
                                           {'$set': {
                                               'end_date': SYSTEM_DATE,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
        else:
            return

    if fn == 1:
        grp6_res = res6.get()
        temp_available_footnote_group = count_fn+1
        check = sending_error_mail(system_date=SYSTEM_DATE, group='available_footnote_group',
                                   db=db, attempt=temp_available_footnote_group)
        if check==0:
            db.JUP_DB_Celery_Status.update({'group_name': 'available_footnote_group',
                                            'start_date': SYSTEM_DATE,
                                            'attempt_number': count_fn + 1},
                                           {'$set': {
                                               'end_date': SYSTEM_DATE,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
            mail_array.append("run_footnote_coll")

        else:
            return


    if len(mail_array) > 0:
        mail_group = group([send_exec_stats_mail.s(task_list=mail_array,
                                                   system_date=SYSTEM_DATE,
                                                   subject="Market Group | Avaliable_footnote")])()
        print mail_group.get()



@measure(JUPITER_LOGGER)
def send_summary_mail():
    count_summary = db.JUP_DB_Celery_Status.find({'group_name': 'analytics_summary',
                                                  'start_date': SYSTEM_DATE,
                                                  'end_time': {'$exists': True}}).count()
    if count_summary == 0:
        count = db.JUP_DB_Celery_Status.find({'group_name': 'analytics_summary',
                                              'start_date': SYSTEM_DATE}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'analytics_summary',
                                        'start_date': SYSTEM_DATE,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count + 1})
        atpco = client[ATPCO_DB]
        system_date = SYSTEM_DATE_1
        system_date_ = datetime.datetime.strptime(system_date, "%Y-%m-%d").strftime("%Y%m%d")
        fares_volume = db.JUP_DB_ATPCO_Fares_Rules.find({'file_date': system_date_}).count()

        rules_volume = atpco.JUP_DB_ATPCO_Record_1.find({'LAST_UPDATED_DATE': system_date}).count()
        rules_volume += atpco.JUP_DB_ATPCO_Record_0.find({'LAST_UPDATED_DATE': system_date}).count()
        rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_All.find({'LAST_UPDATED_DATE': system_date}).count()
        rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_10.find({'LAST_UPDATED_DATE': system_date}).count()
        rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_25.find({'LAST_UPDATED_DATE': system_date}).count()
        rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_03_FN.find({'LAST_UPDATED_DATE': system_date}).count()
        rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_11_FN.find({'LAST_UPDATED_DATE': system_date}).count()
        rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_14_FN.find({'LAST_UPDATED_DATE': system_date}).count()
        rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_15_FN.find({'LAST_UPDATED_DATE': system_date}).count()
        rules_volume += atpco.JUP_DB_ATPCO_Record_2_Cat_23_FN.find({'LAST_UPDATED_DATE': system_date}).count()

        mail_group = group([send_exec_stats_mail.s(task_list=tasks_in_automation,
                                                   system_date=SYSTEM_DATE,
                                                   subject="Summary",
                                                   fares_vol=fares_volume,
                                                   rules_vol=rules_volume)])()
        print mail_group.get()
        db.JUP_DB_Celery_Status.update({'group_name': 'analytics_summary',
                                        'start_date': SYSTEM_DATE,
                                        'attempt_number': count + 1},
                                       {'$set': {
                                           'end_date': SYSTEM_DATE,
                                           'end_time': datetime.datetime.now().strftime('%H:%M')
                                       }})


if __name__ == '__main__':
    run_fzdb_automation()
    run_triggers()
    run_after_triggers()

    # atpco_msg = 0
    # with rabbitpy.Connection(url) as conn:
    #     with conn.channel() as channel:
    #         queue = rabbitpy.Queue(channel, 'completed_atpco_master')
    #         print "Waiting for messages from completed_atpco_maaster queue"
    #         for message in queue:
    #             received_msg = message.json()
    #             if received_msg['message'] == "atpco_master_" + SYSTEM_DATE:
    #                 print 'Received correct atpco message: ', received_msg
    #                 atpco_msg = 1
    #                 message.ack()
    #                 break
    #             else:
    #                 print "Received wrong atpco message: ", received_msg
    #                 message.ack()
    # if atpco_msg ==1:
    #     send_summary_mail()
    send_summary_mail()
    main(client=client)
    # db = client[JUPITER_DB]
    # with rabbitpy.Connection(url=url) as conn:
    #     with conn.channel() as channel:
    #         queue = rabbitpy.Queue(channel, 'fzdb_connector')
    #         print "waiting for messages from fzdb_connector queue"
    #         for message in queue:
    #             received_msg = message.json()
    #             if received_msg['message'] == "fzDB_connector_"+SYSTEM_DATE:
    #                 print "Received correct message from fzdb_connector: ", received_msg
    #                 print 'Starting fzdb_automation'
    #                 run_fzdb_automation()
    #                 crsr = list(db.execution_stats.find({"task_end_date": datetime.datetime.now().strftime("%Y-%m-%d")},
    #                                                     {"task_name": 1,
    #                                                      "task_id": 1,
    #                                                      "task_start_date": 1,
    #                                                      "task_end_date": 1,
    #                                                      "task_start_time": 1,
    #                                                      "task_end_time": 1,
    #                                                      "group_name": 1,
    #                                                      "time_taken": 1,
    #                                                      "_id": 0}))
    #                 exc_stats = pd.DataFrame(crsr)
    #                 with open("tasks_log.txt", 'a') as logfile:
    #                     for idx, row in exc_stats.iterrows():
    #                         str_to_write = "task_name: " + \
    #                                        row['task_name'] + \
    #                                        " group_name: " + \
    #                                        str(row['group_name']) + \
    #                                        " task_id: " + \
    #                                        row['task_id'] + \
    #                                        " task_start_date: " + \
    #                                        row['task_start_date'] + \
    #                                        " task_start_time: " + \
    #                                        str(row['task_start_time']) + \
    #                                        " task_end_date: " + \
    #                                        str(row['task_end_date']) + \
    #                                        " task_end_time: " + \
    #                                        str(row['task_end_time']) + \
    #                                        " time_taken: " + \
    #                                        str(row['time_taken']) + \
    #                                        "\n"
    #                         logfile.write(str_to_write)
    #                 crsr = list(db.execution_stats.aggregate([
    #                     {
    #                         "$group": {
    #                             "_id": {
    #                                 "group_name": "$group_name"
    #                             },
    #                             "start_time": {
    #                                 "$min": "$task_start_time"
    #                             },
    #                             "end_time": {
    #                                 "$max": "$task_end_time"
    #                             },
    #                             "completed_timestamp": {
    #                                 "$max": "$completed_timestamp"
    #                             }
    #                         }
    #                     },
    #                     {
    #                         "$project": {
    #                             "_id": 0,
    #                             "group_name": "$_id.group_name",
    #                             "completed_timestamp": 1,
    #                             "start_time": 1,
    #                             "end_time": 1
    #                         }
    #                     }
    #                 ]))
    #                 exc_stats = pd.DataFrame(crsr)
    #                 exc_stats['start_time'] = pd.to_datetime(exc_stats['start_time'], format="%H:%M:%S")
    #                 exc_stats['end_time'] = pd.to_datetime(exc_stats['end_time'], format="%H:%M:%S")
    #                 exc_stats['time_taken'] = exc_stats['end_time'] - exc_stats['start_time']
    #                 exc_stats['start_time'] = exc_stats['start_time'].dt.strftime("%H:%M:%S")
    #                 exc_stats['end_time'] = exc_stats['end_time'].dt.strftime("%H:%M:%S")
    #                 logfile = open('tasks_log.txt', 'a')
    #                 for idx, row in exc_stats.iterrows():
    #                     str_to_write = " group_name: " + \
    #                                    str(row['group_name']) + \
    #                                    " group_start_time: " + \
    #                                    str(row['start_time']) + \
    #                                    " group_end_time: " + \
    #                                    str(row['end_time']) + \
    #                                    " time_taken: " + \
    #                                    str(row['time_taken']) + \
    #                                    " completed_timestamp: " + \
    #                                    str(row['completed_timestamp']) + \
    #                                    "\n"
    #                     logfile.write(str_to_write)
    #                 logfile.close()
    #                 message.ack()
    #                 break
    #             else:
    #                 print "Received wrong message from fzDB_connector: ", received_msg
    #                 message.ack()

    ## Updating existing triggers
    @measure(JUPITER_LOGGER)
    def hit_java_url(user, comp, cur_year, url, SLEEP_TIME):
        try:
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

            dep_year = datetime.datetime
            parameters = {"user": user, "curDay": SYSTEM_DATE, "compartment": comp, "dep_year": cur_year}
            print "sending request..."
            time.sleep(SLEEP_TIME)
            # print parameters
            response_mt = requests.post(url, data=json.dumps(parameters), headers=headers, timeout=100.0, verify=False)
            print "got response!! ", response_mt.status_code

        except Exception as error:
            print(error, parameters)


    try:
        url = JAVA_LP_URL
        SLEEP_TIME = 0.05
        cur_date = datetime.datetime.strptime(SYSTEM_DATE, '%Y-%m-%d')
        cur_year = cur_date.strftime('%Y')
        client = mongo_client()
        db = client[JUPITER_DB]
        # Get the list of users from users collections and get a combination of user with compartment to call Java API
        user = db.JUP_DB_User.find({"active": True})
        comp = db.JUP_DB_Booking_Class.distinct('comp')
        comp.append('TL')
        for each_user in user:
            user = each_user['name']
            for each_comp in comp:
                hit_java_url(user, each_comp, cur_year, url, SLEEP_TIME)

    except Exception as error:
        print(error)

    print 'Done Automation for today:', SYSTEM_DATE