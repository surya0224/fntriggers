import sys
import datetime
import inspect
import json
import traceback
import bson
import pika
import rabbitpy
from jupiter_AI import parameters, MONGO_CLIENT_URL, MONGO_SOURCE_DB, ANALYTICS_MONGO_PASSWORD, \
    ANALYTICS_MONGO_USERNAME, RABBITMQ_USERNAME, RABBITMQ_PASSWORD, RABBITMQ_HOST, RABBITMQ_PORT, JUPITER_LOGGER, mongo_client
from jupiter_AI import network_level_params as net
from jupiter_AI.common import ClassErrorObject as error_class
from jupiter_AI.logutils import measure
from jupiter_AI.triggers import trigger_level_analysis
from jupiter_AI.network_level_params import JUPITER_DB
from bson.objectid import ObjectId
import pymongo
from jupiter_AI import client
sys.stdout.flush()

# client = pymongo.MongoClient(MONGO_CLIENT_URL)
# client.the_database.authenticate(ANALYTICS_MONGO_USERNAME, ANALYTICS_MONGO_PASSWORD, source=MONGO_SOURCE_DB)
#db = client[JUPITER_DB]


@measure(JUPITER_LOGGER)
def trigger_desc_list(db):
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
    print "pooooo"
    for trigger_doc in cursor:
        del trigger_doc['_id']
        query.append(trigger_doc)

    if len(query) != 0:
        for trigger in query:
            #	    print trigger
            try:
                trigger_desc_list_data.append(trigger['desc'])
            except KeyError:
                trigger_desc_list_data.append("manual")
    else:
        no_trigger_error = error_class.ErrorObject(
            error_class.ErrorObject.ERRORLEVEL2,
            get_module_name(),
            get_arg_lists(
                inspect.currentframe()))
        no_trigger_error.append_to_error_list(
            "Number of docs for triggers in trigger types collection cannot be 0")
        raise no_trigger_error
    return trigger_desc_list_data


@measure(JUPITER_LOGGER)
def analyze(db, id):
    '''
    Takes the id from the message in queue as input
    Does the Entire process of Analytics by calling the relevant class
    After the processing is done prints a message as to processed
    '''

    cursor = db.JUP_DB_Triggering_Event.find(
        {'_id': bson.objectid.ObjectId(id)})

    query = [i for i in cursor]

    if len(query) == 1:
        trigger_list = trigger_desc_list(db=db)
        t = query[0]['trigger_type']
        if t in trigger_list:
            print 'p = trigger_level_analysis.' + t + str(id)
            exec 'p = trigger_level_analysis.' + t + '(ObjectId(id))'
            exec 'p.do_analysis(db=db)'
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
def processing_function(message_received, db):
    '''
    Function takes the message from rabbitMQ server as input
    Decodes the message
    Calls the analyze function for analysis
    If any error has occured in the analysis that we haven't yet catched
            it will be stored in Database
    '''
    if message_received is not None:
        print "Received" + "%r" % message_received
        r = message_received
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
                    analyze(db=db, id=r['_id'])
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


if __name__ == "__main__":
    client = mongo_client()
    db=client[JUPITER_DB]
    # connection = pika.BlockingConnection(parameters)
    # channel = connection.channel()
    # channel.queue_delete(queue="Queue5")

    # @measure(JUPITER_LOGGER)
    # def callback(ch, method, properties, body):
    #     print "received msg", body
    #     processing_function(json.loads(body))
    #    channel.queue_delete(queue='Queue5')
    #    db.JUP_DB_Workflow.remove()
 #   db.JUP_DB_Workflow_OD_User.remove()

    # channel.basic_consume(callback,
    #                       queue='Queue5',
    #                       no_ack=True)

    # print(' [*] Waiting for messages. To exit press CTRL+C')
    # channel.start_consuming()
    url = 'amqp://' + RABBITMQ_USERNAME + \
        ":" + RABBITMQ_PASSWORD + \
        "@" + RABBITMQ_HOST + ":" + str(RABBITMQ_PORT) + "/"
    with rabbitpy.Connection(url) as conn:
        with conn.channel() as channel:
          #      channel.queue_delete(queue='Queue5')
            queue = rabbitpy.Queue(channel, 'Queue5')
            print "Waiting for messages.."

            # Exit on CTRL-C
            # try:
            # Consume the message
            for message in queue:
                print "In listener_data_level, message = "
                message.pprint(True)

            # try:
                processing_function(message_received=message.json(), db=db)
                message.ack()
            # except:
            #     print traceback.print_exc()
            #     pass

            # except KeyboardInterrupt:
            #     print 'Exited consumer'
    client.close()