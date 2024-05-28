import pymongo
import pika
from network_level_params import *

# import resource
#
#
# def memory_limit():
#     soft, hard = resource.getrlimit(resource.RLIMIT_AS)
#     resource.setrlimit(resource.RLIMIT_AS, (get_memory() * 1024 * 0.75, hard))
#
#
# def get_memory():
#     with open('/proc/meminfo', 'r') as mem:
#         free_memory = 0
#         for i in mem:
#             sline = i.split()
#             if str(sline[0]) in ('MemFree:', 'Buffers:', 'Cached:'):
#                 free_memory += int(sline[1])
#     return free_memory


client = pymongo.MongoClient(MONGO_CLIENT_URL)
client.the_database.authenticate(ANALYTICS_MONGO_USERNAME, ANALYTICS_MONGO_PASSWORD, source=MONGO_SOURCE_DB)


def mongo_client():
    client = pymongo.MongoClient(MONGO_CLIENT_URL)
    client.the_database.authenticate(ANALYTICS_MONGO_USERNAME, ANALYTICS_MONGO_PASSWORD, source=MONGO_SOURCE_DB)
    return client

# client = pymongo.MongoClient('localhost:27017')
# print "Connection to dB established"

credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
parameters = pika.ConnectionParameters(host=RABBITMQ_HOST,
                                        port=RABBITMQ_PORT,
                                        virtual_host='/',
                                        credentials=credentials,
                                        heartbeat_interval=50,
                                        socket_timeout=1000)
# parameters = pika.ConnectionParameters(host='localhost')
# connection = pika.BlockingConnection(parameters)
# channel = connection.channel()
# print "Rabbit MQ Connection Created"
