import pika
from jupiter_AI import SYSTEM_DATE, parameters, today, mongo_client, JUPITER_DB
import json
import datetime
SYSTEM_DATE_1 = (today - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

connection = pika.BlockingConnection(parameters)
channel = connection.channel()

print 'Declaring Queues'
channel.queue_declare(queue='atpco_connector')
channel.queue_declare(queue='automation_pipeline')
channel.queue_declare(queue='flight_leg_data')
channel.queue_declare(queue='pros_data')
channel.queue_declare(queue='fzdb_connector')
channel.queue_declare(queue='lff_3')
channel.queue_declare(queue='lff_2')
channel.queue_declare(queue='lff')
channel.queue_declare(queue='fareId')
channel.queue_declare(queue='atpco_master')
channel.queue_declare(queue='host_od_capacity')
channel.queue_declare(queue='host_od_capacity_2')
channel.queue_declare(queue='seat_factor')
channel.queue_declare(queue='run_automation_2')
channel.queue_declare(queue='manual_trigger_start')
channel.queue_declare(queue='lff_hff_group')
##
def run():
    client = mongo_client()
    db = client[JUPITER_DB]

    print 'Publishing ETL messages'
    count = db.JUP_DB_Celery_Status.find({'group_name': 'record_4_5_group', 'start_date': SYSTEM_DATE}).count()
    if count > 0:
        message_atpco = json.dumps({'message': 'atpco_connector_' + SYSTEM_DATE, "file_date": SYSTEM_DATE_1})
        channel.basic_publish(exchange='',
                              routing_key='atpco_connector',
                              body=message_atpco
                              )

    count = db.JUP_DB_Celery_Status.find({'group_name': 'market_group', 'start_date': SYSTEM_DATE}).count()
    if count > 0:
        message_mt = json.dumps({'message': 'manual_trigger_' + SYSTEM_DATE})
        channel.basic_publish(exchange='',
                              routing_key='automation_pipeline',
                              body=message_mt
                              )

    count = db.JUP_DB_Celery_Status.find({'group_name': 'market_flights_group', 'start_date': SYSTEM_DATE}).count()
    if count > 0:
        message_flight_leg = json.dumps({'message': 'flight_leg_data_' + SYSTEM_DATE})
        channel.basic_publish(exchange='',
                              routing_key='flight_leg_data',
                              body=message_flight_leg
                              )

    count = db.JUP_DB_Celery_Status.find({'group_name': 'market_flights_group', 'start_date': SYSTEM_DATE}).count()
    if count > 0:
        message_pros = json.dumps({'message': 'pros_data_' + SYSTEM_DATE})
        channel.basic_publish(exchange='',
                              routing_key='pros_data',
                              body=message_pros
                              )
    count = db.JUP_DB_Celery_Status.find({'group_name': 'capacity_group', 'start_date': SYSTEM_DATE}).count()
    if count > 0:
        message_fzdb = json.dumps({'message': 'inventory_leg_' + SYSTEM_DATE})
        channel.basic_publish(exchange='',
                              routing_key='fzdb_connector',
                              body=message_fzdb
                              )

    count = db.JUP_DB_Celery_Status.find({'group_name': 'triggers_group', 'start_date': SYSTEM_DATE}).count()
    if count > 0:
        message_lff = json.dumps({'message': 'lff_' + SYSTEM_DATE})
        channel.basic_publish(exchange='',
                              routing_key='lff_2',
                              body=message_lff
                              )

    count = db.JUP_DB_Celery_Status.find({'group_name': 'lff_hff_group', 'start_date': SYSTEM_DATE}).count()
    if count > 0:
        message_lff_hff = json.dumps({'message': 'lff_hff_' + SYSTEM_DATE, "file_date": SYSTEM_DATE_1})
        channel.basic_publish(exchange='',
                              routing_key='lff_hff_group',
                              body=message_lff_hff
                              )

    client.close()


run()