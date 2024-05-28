from jupiter_AI import parameters, MONGO_CLIENT_URL, MONGO_SOURCE_DB, ANALYTICS_MONGO_PASSWORD, ANALYTICS_MONGO_USERNAME, RABBITMQ_USERNAME, RABBITMQ_PASSWORD, RABBITMQ_HOST, RABBITMQ_PORT
import pika
import json
from jupiter_AI import parameters
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

url = 'amqp://' + RABBITMQ_USERNAME + \
        ":" + RABBITMQ_PASSWORD + \
        "@" + RABBITMQ_HOST + ":" + str(RABBITMQ_PORT) + "/"


channel.queue_declare(queue="Queue5")
message = json.dumps({"mesg": "test_msg"})
channel.basic_publish(exchange='',
                      routing_key="Queue5",
                      body=message)
print "sent!"