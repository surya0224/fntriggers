from jupiter_AI.common import ClassErrorObject as error_class
from jupiter_AI.RnA.common_RnA_functions import gen_collection_name
from jupiter_AI import mongo_client, JUPITER_LOGGER
from jupiter_AI.logutils import measure
from jupiter_AI.network_level_params import JUPITER_DB, query_month_year_builder
from jupiter_AI.network_level_params import SYSTEM_DATE, today, RABBITMQ_HOST, RABBITMQ_PASSWORD, RABBITMQ_PORT,RABBITMQ_USERNAME
from jupiter_AI.network_level_params import WEEKEND_END_DOW, WEEKEND_START_DOW, WEEKEND_END_HOUR, WEEKEND_START_HOUR, WEEKEND_END_MIN, WEEKEND_START_MIN
import inspect
import datetime
import json
import pika
#db = client[JUPITER_DB]
# credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
# parameters = pika.ConnectionParameters(host=RABBITMQ_HOST,
#                                        port=RABBITMQ_PORT,
#                                        virtual_host='/',
#                                        credentials=credentials)
# parameters = pika.ConnectionParameters(host='localhost')
# connection = pika.BlockingConnection(parameters)
# channel = connection.channel()


@measure(JUPITER_LOGGER)
def get_relevant_business_timestamp(datetime_obj, db):
    """
    This Function Considers the possibility of what the weekends are and working hours in a day.
    Take the working hours on a day from configuration.
    Take the weekend days as dow numbers from configuration
    :param datetime_obj: 
    :return: 
    """
    inp_dow = datetime_obj.isoweekday()
    inp_hour = datetime_obj.hour
    inp_min = datetime_obj.minute
    crsr = db.JUP_DB_Business_Hours_Config.find()
    if crsr.count() > 0:
        weekends = crsr[0]['weekend']
        start_time_str = crsr[0]['office_hour_start']
        start_time_hour = int(start_time_str[:2])
        start_time_minute = int(start_time_str[3:])
        end_time_str = crsr[0]['office_hour_end']
        end_time_hour = int(end_time_str[:2])
        end_time_minute = int(end_time_str[3:])
    else:
        weekends = [5, 6]
        start_time_str = '08:00'
        start_time_hour = 8
        start_time_minute = 0
        end_time_str = '15:00'
        end_time_hour = 15
        end_time_minute = 0

    if inp_dow in weekends:
        if max(weekends) == 7:
            next_working_day_dow = 0
        else:
            next_working_day_dow = max(weekends) + 1
        next_working_ts = next_weekday(datetime_obj, next_working_day_dow)
        ts = datetime.datetime(next_working_ts.year,
                               next_working_day_dow.month,
                               next_working_ts.day,
                               start_time_hour,
                               end_time_hour)
        return ts
    else:
        if inp_hour > end_time_hour:
            temp = datetime.datetime(datetime_obj + datetime.timedelta(days=1))
            ts = datetime.datetime(temp.year,
                                   temp.month,
                                   temp.day,
                                   start_time_hour,
                                   start_time_minute)
            if ts.isoweekday() in weekends:
                if max(weekends) == 7:
                    next_working_day_dow = 0
                else:
                    next_working_day_dow = max(weekends) + 1
                next_working_ts = next_weekday(datetime_obj, next_working_day_dow)
                ts = datetime.datetime(next_working_ts.year,
                                       next_working_ts.month,
                                       next_working_ts.day,
                                       start_time_hour,
                                       end_time_hour)
                return ts
            else:
                return ts
        elif inp_hour < start_time_hour:
            ts = datetime.datetime(datetime_obj.year,
                                   datetime_obj.month,
                                   datetime_obj.day,
                                   start_time_hour,
                                   start_time_minute)
            return ts
        else:
            return datetime_obj


@measure(JUPITER_LOGGER)
def lin_search(a, b):
    """
    This function takes 2 inputs an array a and an argument b
    It is a boolean function which returns
    True when b is not present in abs
    False when b is already present in a
    """
    if a != []:
        for i in a:
            if (b == i):
                return False
        return True
    else:
        if b != None:
            return True
        else:
            return False


@measure(JUPITER_LOGGER)
def sending_message(msg):
    '''
    Function to send a message into the rabbitMQ server(queue)
    '''
    # credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    # parameters = pika.ConnectionParameters(host=RABBITMQ_HOST,
    #                                        port=RABBITMQ_PORT,
    #                                        virtual_host='/',
    #                                        credentials=credentials)
    # parameters = pika.ConnectionParameters(host='localhost')
    # connection = pika.BlockingConnection(parameters)
    from jupiter_AI import parameters
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    if (msg['priority'] == 5) or (msg['priority'] == 2):
        channel.queue_declare(queue="Queue5")
        # channel.queue_declare(queue="Queue5", durable=True, exclusive=False,
        #                       auto_delete=False)
        message = json.dumps(msg)
        channel.basic_publish(exchange='',
                              routing_key="Queue5",
                              body=message)
        print '[X] Sent ' + message + ' at ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

    else:
        channel.queue_declare(queue="Queue9")
        # channel.queue_declare(queue="Queue9", durable=True, exclusive=False,
        #                       auto_delete=False)
        message = json.dumps(msg)
        channel.basic_publish(exchange='',
                              routing_key="Queue9",
                              body=message)
        print '[X] Sent ' + message + ' at ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    # for i in range(10):
    #     if msg['priority'] == i:
    #         k = 'Queue' + '%s' % i
    #         channel.queue_declare(queue=str(k))
    #         channel.queue_declare(queue=str(k), durable=True, exclusive=False,
    #                               auto_delete=False, passive=True)
    #         message = json.dumps(msg)
    #         channel.basic_publish(exchange='',
    #                               routing_key=k,
    #                               body=message)
    #         print '[X] Sent ' + message + ' at ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    # channel.close()
    # connection.close()


@measure(JUPITER_LOGGER)
def trigger_hierarchy(self, db):
    """
    Creates the hierarchy to be followed for detecting
    the trigger_type
    Returns a list that contains all the triggers with
    all the charesteristics with level consideration
    Priority of getting triggers
    POS_OD_COMPARTMENT > COUNTRY > REGION > NETWORK/AIRLINE
    returns the list containing all triggers to be considered
    after level of triggers integration
    """
    triggers = []
    trigger_desc = []
    """
    o = db.JUP_DB_Pos_Od_Compartment.find({'pos': self.old_doc_data['pos'],
                                           'origin': self.old_doc_data['origin'],
                                           'destination': self.old_doc_data['destination'],
                                           'compartment': self.old_doc_data['compartment']})
    if o.count() != 0:
        if o.count() == 1:
            cursor = o[0]
            country = cursor['country']
            region = cursor['region']
            if cursor['trigger_types']:
                if len(cursor['trigger_types']) != 0:
                    for i in cursor['trigger_types']:
                        triggers.append(i)
                        trigger_desc.append(i['desc'])
        else:
            e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                        self.get_module_name(),
                                        self.get_arg_lists(inspect.currentframe()))
            e.append_to_error_list(
                "Expected 1 document,received many from query for JUP_DB_Pos_Od_Compartment Collection ")
            raise e
        print trigger_desc
        c = db.JUP_DB_Country.find({'country': country})
        if c.count() != 0:
            if c.count() == 1:
                cursor = c[0]
                if cursor['trigger_types'] != []:
                    if len(cursor['trigger_types']) != 0:
                        for i in cursor['trigger_types']:
                            if (lin_search(trigger_desc, i['desc'])):
                                triggers.append(i)
                                trigger_desc.append(i['desc'])
            else:
                e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                            self.get_module_name(),
                                            self.get_arg_lists(inspect.currentframe()))
                e.append_to_error_list("Expected 1 document,received many from query for Country Collection ")
                raise e
        print trigger_desc
        r = db.JUP_DB_Region.find({'region': region})
        if r.count() != 0:
            if r.count() == 1:
                cursor = r[0]
                if cursor['trigger_types'] != []:
                    if len(cursor['trigger_types']) != 0:
                        for i in cursor['trigger_types']:
                            if (lin_search(trigger_desc, i['desc'])):
                                triggers.append(i)
                                trigger_desc.append(i['desc'])
            else:
                e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                            self.get_module_name(),
                                            self.get_arg_lists(inspect.currentframe()))
                e.append_to_error_list("Expected 1 document,received many for Region Collection")
                raise e
    else:
        pass
    """
    n = db.JUP_DB_Trigger_Types.find()
    if n.count() != 0:
        for i in n:
            if lin_search(trigger_desc, i['desc']):
                del i['_id']
                triggers.append(i)
                trigger_desc.append(i['desc'])
    else:
        e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                    self.get_module_name(),
                                    self.get_arg_lists(inspect.currentframe()))
        e.append_to_error_list("All the triggers should be defined at network level")
        raise e
    return triggers


@measure(JUPITER_LOGGER)
def next_weekday(d, weekday):
    days_ahead = weekday - d.weekday()
    if days_ahead <= 0:
        days_ahead += 7
    return datetime.datetime(d + datetime.timedelta(days_ahead))


@measure(JUPITER_LOGGER)
def create_trigger(self, db, is_event_trigger=False):
    '''
    Called in trigger_birth function
    Function to upload the trigger to the database
    the trigger is stored in JUP_DB_Triggering_Event collection
    '''
    self.date = SYSTEM_DATE
    tdate = datetime.datetime.strftime(today, '%Y-%m-%d')
    ttime = datetime.datetime.strftime(today, '%H:%M')

    #changedd = get_relevant_business_timestamp(today)

    #changed_date = datetime.datetime.strftime(changedd, '%Y-%m-%d')
    #changed_time = datetime.datetime.strftime(changedd, '%H:%M')

    triggering_event_data = {
        'pricing_action_id_at_trigger_time': self.pricing_action_id_at_trigger_time,
        'trigger_type': self.trigger_type,
        'old_doc_data': self.old_doc_data,
        'new_doc_data': self.new_doc_data,
        # 'host_fare_doc': self.host_fare_doc,
        'priority': self.priority,
        'min_interval': self.min_interval,
        'triggering_data': self.triggering_data,
        # 'comp_level_data': self.comp_level_data,
        'category_details': self.cat_details,
        'trigger_id': self.trigger_id,
        'unique_trigger_id': self.unique_trigger_id,
        'lower_threshold': self.lower_threshold,
        'upper_threshold': self.upper_threshold,
        'threshold_type': self.threshold_type,
        'gen_date': tdate,
        'gen_time': ttime,
        'time': datetime.datetime.now().strftime('%H:%M'),
        'date': SYSTEM_DATE,
        'is_event_trigger': is_event_trigger
    }
    # print triggering_event_data
    # print json.dumps(triggering_event_data, indent=1)
    print "trigger-type: ", self.trigger_type
    id = db.JUP_DB_Triggering_Event.insert(triggering_event_data)
    return id


@measure(JUPITER_LOGGER)
def send_trigger_to_queue(self, id):
    """
    Creates the message to be sent into the rabbitMQ server
    Calls the sending_message function to send the message
    If two id's are obtained from the querycode considers
    the latest id is collected and neglects the other
    """
    # cursor = db.JUP_DB_Triggering_Event.find({'date': self.date,
    #                                           'time': self.time,
    #                                           'trigger_type': self.trigger_type,
    #                                           'triggering_data': self.triggering_data,
    #                                           'old_doc_data': self.old_doc_data,
    #                                           'new_doc_data': self.new_doc_data,
    #                                           'priority': self.priority
    #                                           })
    # if cursor.count() == 1:
    #     for i in cursor:
    #         id_value = i['_id']
    if self.trigger_type != "manual":
        print "sending to queue"
        message = {'_id': str(id), 'priority': 5}
    else:
        message = {'_id': str(id), 'priority': 9}
    # print message
    sending_message(message)
    # elif cursor.count() == 0:
    #     e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
    #                                 self.get_module_name(),
    #                                 self.get_arg_lists(inspect.currentframe()))
    #     e.append_to_error_list('No document for query in triggering event collection for ID')
    #     raise e
    # elif cursor.count() > 1:
    #     e = error_class.ErrorObject(error_class.ErrorObject.WARNING,
    #                                 self.get_module_name(),
    #                                 self.get_arg_lists(inspect.currentframe()))
    #     e.append_to_error_list('No document for query in triggering event collection for ID')
    #     ids = []
    #     for i in cursor:
    #         ids.append('_id')
    #     id_value = i[len(ids) - 1]
    #     message = {'_id': str(id_value), 'priority': self.priority}
    #     sending_message(message)
    #     raise e


@measure(JUPITER_LOGGER)
def get_bookings_vlyr_vtgt_fb_level(pos,
                                    origin,
                                    destination,
                                    compartment,
                                    db,
                                    farebasis,
                                    depdate_from=None,
                                    depdate_to=None,
                                    ):
    """
    :param pos:
    :param origin:
    :param destination:
    :param compartment:
    :param farebasis:
    :param depdate_from:
    :param depdate_to:
    :return:
    """
    temp_col_name = gen_collection_name()
    qry_bookings_data = {
        'pos': pos,
        'origin': origin,
        'destination': destination,
        'compartment': compartment,
        # 'farebasis': farebasis,
    }
    target_flag = False
    if depdate_from and depdate_to:
        depdate_from_obj = datetime.datetime.strptime(depdate_from, '%Y-%m-%d')
        depdate_to_obj = datetime.datetime.strptime(depdate_from, '%Y-%m-%d')
        from_month = depdate_from_obj.month
        to_month = depdate_to_obj.month
        from_year = depdate_to_obj.month
        to_year = depdate_to_obj.year
        qry_bookings_data['dep_date'] = {'$gte': depdate_from,
                                         '$lte': depdate_to}
        target_flag = True
    # elif depdate_from:
    #     depdate_from_obj = datetime.datetime.strptime(depdate_from, '%Y-%m-%d')
    #     to_date = datetime.datetime.strftime(depdate_from_obj, '%Y-%m-%d')
    #     qry_bookings_data['dep_date'] = {'$gte': depdate_from,
    #                                      '$lte': to_date}
    # elif depdate_to:
    #     qry_bookings_data['dep_date'] = {'$gte': sale_date_from,
    #                                      '$lte': depdate_to}
    else:
        depdate_from_obj = today
        depdate_to_obj = today + datetime.timedelta(days=7)
        depdate_from = str(depdate_from_obj)
        depdate_to = str(depdate_to_obj)
        from_month = depdate_from_obj.month
        to_month = depdate_to_obj.month
        from_year = depdate_to_obj.month
        to_year = depdate_to_obj.year
        # qry_bookings_data['dep_date'] = {'$gte': depdate_from,
        #                                  '$lte': depdate_to}

    ppln_bookings_data = [
        {
            '$match': qry_bookings_data
        },
        {
            '$group': {
                '_id': None,
                'bookings': {'$sum': '$pax'},
                'bookings_ly': {'$sum': '$pax'}
            }
        },
        {
            '$project': {
                'bookings': '$bookings',
                'bookings_vlyr': {
                    '$cond': {
                        'if': {'$gt': ['$bookings_ly', 0]},
                        'then': {
                            '$divide': [
                                {
                                    '$subtract': ['$bookings', '$bookings_ly']
                                },
                                '$bookings_ly'
                            ]
                        },
                        'else': None
                    }
                }
            }
        }
    ]
    crsr_bookings_data = db.JUP_DB_Booking_DepDate.aggregate(ppln_bookings_data)
    bookings_data = list(crsr_bookings_data)
    db[temp_col_name].drop()
    if len(bookings_data) == 1:
        bookings = bookings_data[0]['bookings']
        vlyr = bookings_data[0]['vlyr']
        if bookings != 0:
            del qry_bookings_data['dep_date']
            qry_bookings_data['$or'] = query_month_year_builder(
                from_month, from_year, to_month, to_year)
            crsr_target = db.JUP_DB_Target_OD.aggregate([
                {
                    '$match': qry_bookings_data
                },
                {
                    '_id': None,
                    'pax': {'$sum': '$pax'}
                }
            ])
            data_target = list(crsr_target)
            if len(data_target) == 1:
                if data_target[0]['pax'] != 0:
                    vtgt = round((bookings - data_target[0]['pax'])
                                 * 100 / data_target[0]['pax'], 2)
                else:
                    vtgt = None
            else:
                vtgt = None
        else:
            vtgt = None
    else:
        bookings = None
        vlyr = None
        vtgt = None
    return {'bookings': bookings, 'vlyr': vlyr, 'vtgt': vtgt}


@measure(JUPITER_LOGGER)
def get_calender_month(number):
    months = ["Unknown",
              "January",
              "Febuary",
              "March",
              "April",
              "May",
              "June",
              "July",
              "August",
              "September",
              "October",
              "November",
              "December"]
    return months[number]

if __name__ == '__main__':
    client =mongo_client()
    db= client[JUPITER_DB]
    import time
    sending_message({'priority':2,'body':'Happy Birthday'})
    # print get_relevant_business_timestamp(today)
    # db.JUP_DB_Workflow.find({})
    client.close()
