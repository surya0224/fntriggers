import json
import datetime
import rabbitpy
import pandas as pd
from jupiter_AI import RABBITMQ_HOST, RABBITMQ_PASSWORD, RABBITMQ_PORT, RABBITMQ_USERNAME, client, JUPITER_DB
from jupiter_AI import SYSTEM_DATE, today, parameters,JUPITER_LOGGER
from jupiter_AI.logutils import measure
from jupiter_AI.triggers.common import sending_error_mail
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
    send_exec_stats_mail

from celery import group
import pika

url = 'amqp://' + RABBITMQ_USERNAME + \
      ":" + RABBITMQ_PASSWORD + \
      "@" + RABBITMQ_HOST + ":" + str(RABBITMQ_PORT) + "/"

db = client[JUPITER_DB]
today_1 = today - datetime.timedelta(days=1)

@meaure(JUPITER_LOGGER)
def run_fzdb_automation():
    SYSTEM_DATE_1 = (today - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

    pros_msg = 0
    flt_leg_msg = 0
    mail_array = []
    # with rabbitpy.Connection(url) as conn:
    #     with conn.channel() as channel:
    #         queue = rabbitpy.Queue(channel, 'fzdb_connector')
    #         print "Waiting for messages from fzdb_connector queue"
    #         for message in queue:
    #             received_msg = message.json()
    #             if received_msg['message'] == "inventory_leg_" + SYSTEM_DATE:
    #                 print 'Received inventory message: ', received_msg
    #                 message.ack()
    count_cap = db.JUP_DB_Celery_Status.find({'group_name': 'capacity_group',
                                              'start_date': SYSTEM_DATE,
                                              'end_time': {'$exists': True}}).count()
    if count_cap == 0:
        print "Running capacity"
        count = db.JUP_DB_Celery_Status.find({'group_name': 'capacity_group',
                                              'start_date': SYSTEM_DATE}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'capacity_group',
                                        'start_date': SYSTEM_DATE,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count + 1})
        group1 = group([run_host_od_capacity.s(),
                        run_comp_od_capacity.s()
                        ])
        res1 = group1()
        grp1_res = res1.get()
        check = sending_error_mail(system_date=SYSTEM_DATE, group='capacity_group', db=db,
                                   attempt=count + 1)
        if check ==0:
            db.JUP_DB_Celery_Status.update({'group_name': 'capacity_group',
                                            'start_date': SYSTEM_DATE,
                                            'attempt_number': count + 1},
                                           {'$set': {
                                               'end_date': SYSTEM_DATE,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
            mail_array = mail_array + ["run_host_od_capacity", "run_comp_od_capacity"]
        else:
            return
                #     break
                # else:
                #     print "Received wrong inventory message: ", received_msg
                #     message.ack()

    with rabbitpy.Connection(url) as conn:
        with conn.channel() as channel:
            inv_msg = {"message": "host_od_capacity_" + SYSTEM_DATE}
            inv_msg = rabbitpy.Message(channel, body_value=inv_msg)
            inv_msg.publish(exchange="", routing_key="host_od_capacity")

    with rabbitpy.Connection(url) as conn:
        with conn.channel() as channel:
            inv_msg = {"message": "host_od_capacity_" + SYSTEM_DATE}
            inv_msg = rabbitpy.Message(channel, body_value=inv_msg)
            inv_msg.publish(exchange="", routing_key="host_od_capacity_2")
            # channel.basic_publish(routing_key="host_od_capacity", exchange="", body=json.dumps(inv_msg))
            # channel.basic_publish(routing_key="host_od_capacity_2", exchange="", body=json.dumps(inv_msg))

    count_Au_Zn_Cu = db.JUP_DB_Celery_Status.find({'group_name': 'Au_Zn_Cu_group',
                                                   'start_date': SYSTEM_DATE,
                                                   'end_time': {'$exists': True}}).count()
    gcz = 0
    if count_Au_Zn_Cu == 0:
        gcz = 1
        print "Running golden copper zinc flights"
        count_gcz = db.JUP_DB_Celery_Status.find({'group_name': 'Au_Zn_Cu_group',
                                                  'start_date': SYSTEM_DATE}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'Au_Zn_Cu_group',
                                        'start_date': SYSTEM_DATE,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count_gcz + 1})
        group3 = group([run_golden_flights.s(),
                        run_copper_flights.s(),
                        run_zinc_flights.s()])
        res3 = group3()

    count_posodc = db.JUP_DB_Celery_Status.find({'group_name': 'pos_od_compartment_group',
                                                 'start_date': SYSTEM_DATE,
                                                 'end_time': {'$exists': True}}).count()
    poc = 0
    if count_posodc == 0:
        poc = 1
        print "Running pos od compartment"
        count_poc = db.JUP_DB_Celery_Status.find({'group_name': 'pos_od_compartment_group',
                                                  'start_date': SYSTEM_DATE}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'pos_od_compartment_group',
                                        'start_date': SYSTEM_DATE,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count_poc + 1})
        months_list = [[1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11], [12]]
        posodc_group = []
        for months in months_list:
            posodc_group.append(run_pos_od_compartment.s(months=months))
        group2 = group(posodc_group)
        res2 = group2()

    count_flights = db.JUP_DB_Celery_Status.find({'group_name': 'market_flights_group',
                                                  'start_date': SYSTEM_DATE,
                                                  'end_time': {'$exists': True}}).count()
    # if count_flights == 0:
        # with rabbitpy.Connection(url) as conn:
        #     with conn.channel() as channel:
        #         queue = rabbitpy.Queue(channel, 'pros_data')
        #         print "Waiting for messages from pros_data queue"
        #         for message in queue:
        #             received_msg = message.json()
        #             if received_msg['message'] == "pros_data_" + SYSTEM_DATE:
        #                 print 'Received pros_data message: ', received_msg
        #                 pros_msg = 1
        #                 message.ack()
        #                 break
        #             else:
        #                 print "Received wrong pros message: ", received_msg
        #                 message.ack()
        #
        # with rabbitpy.Connection(url) as conn:
        #     with conn.channel() as channel:
        #         queue = rabbitpy.Queue(channel, 'flight_leg_data')
        #         print "Waiting for messages from flight_leg_data queue"
        #         for message in queue:
        #             received_msg = message.json()
        #             if received_msg['message'] == "flight_leg_data_" + SYSTEM_DATE:
        #                 print 'Received flight_leg_data message: ', received_msg
        #                 flt_leg_msg = 1
        #                 message.ack()
        #                 break
        #             else:
        #                 print "Received wrong flt_leg_msg message: ", received_msg
        #                 message.ack()

    if (count_flights == 0):
        print "Running market characteristics flights"
        count = db.JUP_DB_Celery_Status.find({'group_name': 'market_flights_group',
                                              'start_date': SYSTEM_DATE}).count()
        db.JUP_DB_Celery_Status.insert({'group_name': 'market_flights_group',
                                        'start_date': SYSTEM_DATE,
                                        'start_time': datetime.datetime.now().strftime('%H:%M'),
                                        'attempt_number': count + 1})
        mrkt_flt = group([run_mar.ket_flights.s()])
        res_flt = mrkt_flt()
        flt_grp_res = res_flt.get()
        check = sending_error_mail(system_date=SYSTEM_DATE, group='market_flights_group', db=db,
                                   attempt=count + 1)
        if check == 0:
            db.JUP_DB_Celery_Status.update({'group_name': 'market_flights_group',
                                            'start_date': SYSTEM_DATE,
                                            'attempt_number': count + 1},
                                           {'$set': {
                                               'end_date': SYSTEM_DATE,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
            mail_array.append("run_market_flights")
            with rabbitpy.Connection(url) as conn:
                with conn.channel() as channel:
                    sf_msg = {"message": "seat_factor_" + SYSTEM_DATE}
                    sf_msg = rabbitpy.Message(channel, body_value=sf_msg)
                    sf_msg.publish(exchange="", routing_key="seat_factor")
        else:
            return
    # channel.basic_publish(routing_key="seat_factor", exchange="", body=json.dumps(sf_msg))

    if poc == 1:
        grp2_res = res2.get()0
        check = sending_error_mail(system_date=SYSTEM_DATE, group='pos_od_compartment_group', db=db,
                                   attempt=count_poc + 1)
        if check == 0:
            db.JUP_DB_Celery_Status.update({'group_name': 'pos_od_compartment_group',
                                            'start_date': SYSTEM_DATE,
                                            'attempt_number': count_poc + 1},
                                           {'$set': {
                                               'end_date': SYSTEM_DATE,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
            mail_array.append("run_pos_od_compartment")
        else:
            return

    if gcz == 1:
        gr3_res = res3.get()
        check = sending_error_mail(system_date=SYSTEM_DATE, group='Au_Zn_Cu_group', db=db,
                                   attempt=count_gcz + 1)
        if check == 0:
            db.JUP_DB_Celery_Status.update({'group_name': 'Au_Zn_Cu_group',
                                            'start_date': SYSTEM_DATE,
                                            'attempt_number': count_gcz + 1},
                                           {'$set': {
                                               'end_date': SYSTEM_DATE,
                                               'end_time': datetime.datetime.now().strftime('%H:%M')
                                           }})
            mail_array = mail_array + ["run_golden_flights",
                                       "run_copper_flights",
                                       "run_zinc_flights"]
        else:
            return

    print 'Publishing completion message to run_automation_2 queue'
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    message = json.dumps({'message': 'run_automation_2_' + SYSTEM_DATE})
    channel.basic_publish(exchange='',
                          routing_key='run_automation_2',
                          body=message
                          )

    if len(mail_array) > 0:
        mail_group = group([send_exec_stats_mail.s(task_list=mail_array,
                                                   system_date=SYSTEM_DATE,
                                                   subject="Capacity | POS OD Compartment")])()
        print mail_group.get()


if __name__ == '__main__':
    # memory_limit()
    run_fzdb_automation()
    # run_triggers()
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
    print 'Done Automation_2 for today:', SYSTEM_DATE
