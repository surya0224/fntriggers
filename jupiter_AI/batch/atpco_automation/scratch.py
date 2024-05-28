import json
import datetime
import math
import pika
import time
import os
from celery import Celery, group, states
from kombu.common import Broadcast
from kombu import Exchange



from jupiter_AI import RABBITMQ_USERNAME, RABBITMQ_PASSWORD, RABBITMQ_HOST, RABBITMQ_PORT,PYTHON_PATH

from celery.utils.log import get_task_logger
import logging
credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
parameters = pika.ConnectionParameters(host=RABBITMQ_HOST,
                                        port=RABBITMQ_PORT,
                                        virtual_host='/',
                                        credentials=credentials,
                                        heartbeat_interval=50,
                                        socket_timeout=1000)

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


connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue='celery2', durable=True)
channel.queue_bind(exchange='celery',
			queue = 'celery2',
                     routing_key='route2')

app = Celery('Automation_tasks',
             backend='rpc://',
             broker=url,
             task_ignore_result=True,
             worker_prefetch_multiplier=1,
             worker_max_memory_per_child=2048000,
             task_acks_late=True
             )


exchange = Exchange('custom_exchange', type='fanout')

CELERY_QUEUES = (
    Broadcast(name='bcast', exchange=exchange),
)





d = {}
TASK_NAME = ''
ERROR_MESSAGE = ''
ERROR_CLASS = ''
TASK_ID = ''
ARG = ''
HOST = ''

@app.task(name='jupiter_AI.batch.atpco_automation.Automation_tasks.test_job')
#@measure(JUPITER_LOGGER)

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






