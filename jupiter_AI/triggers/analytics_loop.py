"""
File Name                   :   analytics_loop.py
Author                      :   Sai Krishna
Date Created                :   2016-07-01
Description                 :   Module that runs 24*7 and listens to the queues in RabbitMQ for triggers
                                Calls the respective class for trigger for analysing the trigger
MODIFICATIONS LOG           :
    S.No                    :
    Date Modified           :
    By                      :
    Modification Details    :
"""
import datetime
import inspect
import json
import sys
import traceback

import bson
import pika
import pymongo

from jupiter_AI import network_level_params as net
from jupiter_AI import parameters, MONGO_CLIENT_URL, MONGO_SOURCE_DB, ANALYTICS_MONGO_PASSWORD, \
    ANALYTICS_MONGO_USERNAME, client, JUPITER_LOGGER
from jupiter_AI.logutils import measure
from jupiter_AI.triggers import trigger_level_analysis
from jupiter_AI.common import ClassErrorObject as error_class
from jupiter_AI.network_level_params import JUPITER_DB
from bson.objectid import ObjectId


sys.stdout.flush()
db = client[JUPITER_DB]


@measure(JUPITER_LOGGER)
def trigger_desc_list():

    '''
    Creates a list of all trigger names
            (desc in JUP_DB_Trigger_Types Collection)
    Used for checking if the class we call is actually
            defined as a trigger in database
    Returns the list of trigger names
    '''

    trigger_desc_list_data = []
    query = []
    cursor = db.JUP_DB_Trigger_Types.find()
    for trigger_doc in cursor:
        del trigger_doc['_id']
        query.append(trigger_doc)

    if len(query) != 0:
        for trigger in query:
            trigger_desc_list_data.append(trigger['desc'])
    else:
        no_trigger_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                                   get_module_name(),
                                                   get_arg_lists(inspect.currentframe()))
        no_trigger_error.append_to_error_list("Number of docs for triggers in trigger types collection cannot be 0")
        raise no_trigger_error
    return trigger_desc_list_data


@measure(JUPITER_LOGGER)
def analyze(id):
    '''
    Takes the id from the message in queue as input
    Does the Entire process of Analytics by calling the relevant class
    After the processing is done prints a message as to processed
    '''

    query = []
    cursor = db.JUP_DB_Triggering_Event.find({'_id': bson.objectid.ObjectId(id)})
    for i in cursor:
        query.append(i)

    if len(query) == 1:
        trigger_list = trigger_desc_list()
        t = query[0]['trigger_type']
        if t in trigger_list:
            print t
            print 'p = trigger_level_analysis.' + t + '(ObjectId(id))'
            try:
                exec 'p = trigger_level_analysis.' + t + '(ObjectId(id))'
            except:
                print "unable to create object of this particular class in trigger level analysis: ", t
            try:
                exec 'p.do_analysis()'
            except:
                print "unable to call do_analysis() function of this particular class: ", t
        else:
            e1 = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                         get_module_name(),
                                         get_arg_lists(inspect.currentframe()))
            e1.append_to_error_list("The trigger_type for the id " + str(
                id) + " is not in the Triggers_List supported by jupiter")
            raise e1
    else:
        e1 = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                     get_module_name(),
                                     get_arg_lists(inspect.currentframe()))
        error_desc = "Expected 1 document in triggering_event collection for id = " + \
                     str(id) + " but got " + str(len(query))
        e1.append_to_error_list(error_desc)
        raise e1


@measure(JUPITER_LOGGER)
def get_module_name():
    '''
    FUnction used to get the module name where it is called
    '''
    return inspect.stack()[1][3]


@measure(JUPITER_LOGGER)
def get_arg_lists(frame):
    '''
    function used to get the list of arguments of the function
    where it is called
    '''
    args, _, _, values = inspect.getargvalues(frame)
    argument_name_list = []
    argument_value_list = []
    for k in args:
        argument_name_list.append(k)
        argument_value_list.append(values[k])
    return argument_name_list, argument_value_list


@measure(JUPITER_LOGGER)
def count_update():
    '''
    Function used to count the messages in each queue defined
    Used while prioritizing messages
    updates the count of messages into a global variable
    '''
    print "Entered Count_update() function"
    global channel
    global message_count
    for i in range(net.MAX_TRIGGER_PRIORITY_LEVEL + 1):
        channel.queue_declare(queue='Queue' + '%s' % i)
        message_count[i] = channel.queue_declare(queue='Queue' + '%s' % i,
                                                 durable=True,
                                                 exclusive=False, auto_delete=False,
                                                 passive=True).method.message_count
    print message_count


@measure(JUPITER_LOGGER)
def processing_function(message_received):
    '''
    Function takes the message from rabbitMQ server as input
    Decodes the message
    Calls the analyze function for analysis
    If any error has occured in the analysis that we haven't yet catched
            it will be stored in Database
    '''
    print "Entered processing_function"
    if message_received[2] is not None:
        print "Received ---***---    " + "%r" % message_received[2]
        r = json.loads(message_received[2])
        print "in Processing function : message_received[2] = ", message_received[2]
        flag = False
        try:
            p = bson.objectid.ObjectId(r['_id'])
        except bson.errors.InvalidId as b:
            flag = True
            # db.JUP_DB_Error_Collection.insert_one({'triggering_event_id': r['_id'],
            #                                        'error_description': str(b)})
        if not flag:
            try:
                try:
                    print "#@measure(JUPITER_LOGGER)Entering " \
                          "def analyze(_id) function "
                    analyze(r['_id'])
                except error_class.ErrorObject as esub:
                    # db.JUP_DB_Error_Collection.insert_one({'triggering_event_id': str(r['_id']),
                    #                                        'error_description': esub.__str__()})
                    if esub.error_level <= error_class.ErrorObject.WARNING:
                        error_flag = False
                    elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL1:
                        error_flag = True
                    elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL2:
                        error_flag = True
                        raise esub
                    print esub
            except KeyError:
                tb = traceback.format_exc()
                print str(tb.encode())
                # db.JUP_DB_Error_Collection.insert_one({'triggering_event_id': r['_id'],
                #                                        'error_description': str(tb.encode())})

'''
The Main Program of the code
Runs an infinite loop that listens continuously at the rabbitMQ server
to receive messages
'''

# if __name__ == '__main__':
#     count = 0
#     while True:
#         try:
#             client = pymongo.MongoClient(MONGO_CLIENT_URL)
#             client.the_database.authenticate(ANALYTICS_MONGO_USERNAME, ANALYTICS_MONGO_PASSWORD, source=MONGO_SOURCE_DB)
#             db = client[JUPITER_DB]
#             # print len(db.collection_names())
#             connection = pika.BlockingConnection(parameters)
#             channel = connection.channel()
#             break
#         except Exception:
#             print 'trying connection'
#             count +=1
#             if count == 100:
#                 break
#             pass
#     print(' [*] Waiting for messages. To exit press CTRL+C')
#     e = error_class.ErrorObject(0, 'None', [])
#     message_count = [None] * 10
#     while True:  # Infinite loop that always runs
#         count_update()
#         i = -1
#         for j in range(net.MAX_TRIGGER_PRIORITY_LEVEL, -1, -1):
#             if message_count[j] != 0:
#                 i = j
#                 break
#         if i >= 0:
#             message_received = channel.basic_get(
#                 queue='Queue' + '%s' % i, no_ack=True)
#             processing_function(message_received)
#             print message_received[2]
#             print datetime.datetime.now()
#             count = 0
#             while True:
#                 try:
#                     count_update()
#                     break
#                 except Exception:
#                     count+=1
#                     if count == 100:
#                         break
#                     print 'trying'


if __name__=="__main__":
    mongo_connect_flag = 0
    rabbit_connect_flag = 0

    while True:
        print "start"
        for establish_count in range(100):
            try:
                client = pymongo.MongoClient(MONGO_CLIENT_URL)
                client.the_database.authenticate(ANALYTICS_MONGO_USERNAME, ANALYTICS_MONGO_PASSWORD, source=MONGO_SOURCE_DB)
                db = client[JUPITER_DB]
                print "Connection with Mongo established"
                mongo_connect_flag = 1
            except Exception:
                print "Cannot Connect to MongoDB"
                continue
            try:
                connection = pika.BlockingConnection(parameters)
                channel = connection.channel()
                print "Connection with RabbitMQ established"
                rabbit_connect_flag = 1
                # channel.queue_delete(queue="Queue5")
                # print "deleted"
            except Exception:
                print "Cannot establish connection with RabbitMQ"
                continue
            if mongo_connect_flag and rabbit_connect_flag:
                print "Both flags are True"
                break
        print "broke"
        message_count = [None] * 10
        while True:
            print "Entered inner While True. Functionality is to constantly read messages from 10 Queues."
            try:
                count_update()
            except:
                print "Count update function raised error."
                break
            i = -1
            for j in range(net.MAX_TRIGGER_PRIORITY_LEVEL, -1, -1):
                if message_count[j] != 0:
                    i = j
                    break
            if i >= 0:
                print i
                try:
                    message_received = channel.basic_get(
			                queue='Queue' + '%s' % i, no_ack=True)
                    print "Message Received from Queue", i," = ", message_received[2]
                except Exception:
                    print "Unable to receive messgae from Queue", i , " where Queue ", i, " has ", message_count[i], " messages."
                    continue
                try:
                    print "Sending message to process"
                    processing_function(message_received)
                except:
                    "Processing function raised an error."
                    continue