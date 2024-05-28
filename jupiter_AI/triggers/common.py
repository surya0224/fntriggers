"""
File Name              :   BookingChangesRolling.py
Author                 :   Sai Krishna
Date Created           :   2016-02-07
Description            :   Common Functions used in all of the trigger codes

MODIFICATIONS LOG
    S.No                   :    1
    Date Modified          :    2017-02-15
    By                     :    Sai Krishna
    Modification Details   :

"""
# from jupiter_AI.batch.fbmapping_batch.JUP_DB_Batch_Fare_Ladder_Mapping import get_host_fareladder

#from jupiter_AI.batch.fbmapping_batch.fbmapping_batch_old import *
from jupiter_AI.triggers.GetInfareFare import get_infare_unique_fares_data
import random
import string
from calendar import monthrange, isleap, day_name
import datetime
from dateutil.relativedelta import relativedelta
from jupiter_AI.batch.atpco_automation.daily_log_mail import send_mail, error_mail,send_mail_error_format
from jupiter_AI import mongo_client
import pandas as pd
import json
from collections import defaultdict
from itertools import chain, islice
from jupiter_AI.network_level_params import JUPITER_DB, JUPITER_LOGGER, SYSTEM_DATE, today, INF_DATE_STR
from jupiter_AI.logutils import measure
#db = client[JUPITER_DB]


@measure(JUPITER_LOGGER)
def get_trigger_config_dates(sig_flag, db):
    day_of_week = day_name[today.weekday()]
    cur = db.JUP_DB_Config_Date.find({'day_of_week': day_of_week,
                                      'market': sig_flag,
                                      'effective_from': {'$lte': SYSTEM_DATE},
                                      '$or': [{'effective_to': {'$gte': SYSTEM_DATE}},
                                              {'effective_to': ""}]})
    dates_list = []
    if cur.count() != 0:
        for i in cur:
            if i['period'] == 'Rolling':
                dates_list.append({'start': SYSTEM_DATE,
                                   'end': datetime.datetime.strftime(today +
                                          relativedelta(days=int(i['reco_period'])), "%Y-%m-%d"),
                                   'code_list': ['RL{}{}'.format(int(i['reco_period']), SYSTEM_DATE)]})

            else:
                if i['period'] == 'Month':
                    date = today + relativedelta(months=int(i['reco_period']))
                elif i['period'] == 'Quarter':
                    date = today + relativedelta(months=int(4 * i['reco_period']))
                elif i['period'] == 'Year':
                    date = today + relativedelta(years=int(i['reco_period']))
                elif i['period'] == 'Week':
                    date = today + relativedelta(days=int(7 * i['reco_period']))
                else:
                    date = today
                date = date.strftime('%Y-%m-%d')
                date_cur = db.JUP_DB_Calendar_Master.aggregate([
                    {
                        '$match': {'period': i['period'],
                                   'from_date': {'$lte': date},
                                   'to_date': {'$gte': date}
                                   }
                    },
                    {
                        '$group': {
                            '_id': None,
                            'from_date': {'$min': '$from_date'},
                            'to_date': {'$max': '$to_date'},
                            'code_list': {'$addToSet': '$combine_column'}
                        }
                    }
                ])
                for j in date_cur:
                    if j['from_date'] < SYSTEM_DATE < j['to_date']:
                        j['from_date'] = SYSTEM_DATE
                    dates_list.append({'start': j['from_date'][:10], 'end': j['to_date'][:10], 'code_list': j['code_list']})
    # dates_list.append({'start': j['from_date'], 'end': j['to_date'], 'code_list': j['code_list']})
    # dates_list=[{'start': '2023-06-01', 'end': '2023-06-30', 'code_list': 'M062023'}]
    return dates_list


@measure(JUPITER_LOGGER)
def chunks(iterable, size=10):
    iterator = iter(iterable)
    for first in iterator:
        yield chain([first], islice(iterator, size - 1))


@measure(JUPITER_LOGGER)
def cursor_to_df(cursor, batch_size=100000):
    df = pd.DataFrame()
    for chunk in chunks(cursor, batch_size):
        df = pd.concat([pd.DataFrame(list(chunk)), df])

    return df


@measure(JUPITER_LOGGER)
def pos_level_currency_data(db):
    """
    :return:
    """
    response = dict()
    crsr = db.JUP_DB_Pos_Currency_Master.find()
    for doc in crsr:
        response[doc['pos']] = {
            'web':doc['web'],
            'gds':doc['gds']
        }
    # print json.dumps(response)
    return response


@measure(JUPITER_LOGGER)
def gen_collection_name():
    time_stamp = datetime.datetime.now().isoformat()
    random_string = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))

    collection_name = 'JUP_Temp_' + time_stamp +'_'+ random_string
    return collection_name


@measure(JUPITER_LOGGER)
def get_start_end_dates(month, year):
    """
    for a month and year returns the tuple with start and end date of the month
    :param month:
    :param year:
    :return:
    """
    last_date = monthrange(year=year,
                           month=month)[1]
    sd = str(year) + "-"+"%02d" %month + '-01'
    ed = str(year) + "-"+"%02d"%month + '-' + "%02d"%last_date
    return sd, ed



#
# @measure(JUPITER_LOGGER)
# def sending_error_mail(system_date, group, db, attempt):
#     curr = db.execution_stats.aggregate([
#         {'$match': {'task_start_date': system_date,
#                     'group_name': group,
#                     'error_class': {'$exists': True},
#                     'attempt_number': attempt
#                     }
#          },
#         {'$group': {
#             '_id': {
#                 "task_name": '$task_name',
#                 "error_class": '$error_class',
#                 "error_message": '$error_message'
#             },
#             'task_id': {'$first': '$task_id'},
#             'hostname': {'$first': '$worker'},
#             'error': {'$first': '$error_detail'}
#
#         }
#         },
#         {'$project': {
#             'task_id': '$task_id',
#             'hostname': '$hostname',
#             'error': '$error',
#             'task_name': '$_id.task_name',
#             'error_class': '$_id.error_class',
#             'error_message': '$_id.error_message'
#         }
#         }])
#     cursorlist = list(curr)
#     print cursorlist
#     print len(cursorlist)
#     if len(cursorlist) > 0:
#         for item in cursorlist:
#             error_mail(task=item['task_name'],
#                        task_id=item['task_id'],
#                        hostname=item['hostname'],
#                        error=item['error'])
#             print 'Error Mail Sent'
#         return -1
#     return 0


@measure(JUPITER_LOGGER)
def sending_error_mail(system_date, group, db, attempt):
    curr = db.execution_stats.aggregate([
        {'$match': {'task_start_date': system_date,
                    'group_name': group,
                    'error_class': {'$exists': True},
                    'attempt_number': attempt
                    }
         },
        {'$group': {
            '_id': {
                "task_name": '$task_name',
                "error_class": '$error_class',
                "error_message": '$error_message'
            },
            'group_name': {'$first': '$group_name'},
            'task_id': {'$first': '$task_id'},
            'hostname': {'$first': '$worker'},
            'error': {'$first': '$error_detail'}
        }
        },
        {'$project': {
            'task_id': '$task_id',
            'group_name':'$group_name',
            'hostname': '$hostname',
            'error': '$error',
            'task_name': '$_id.task_name',
            'error_class': '$_id.error_class',
            'error_message': '$_id.error_message'
        }
        }])
    cursorlist = list(curr)
    df = pd.DataFrame(cursorlist)
    subject = group
    fares_vol = 'NA'
    rules_vol = 'NA'
    #return send_mail_error_format(df, subject, fares_vol, rules_vol)
    print cursorlist
    print len(cursorlist)
    if len(cursorlist) > 0:
        send_mail_error_format(df, subject, fares_vol, rules_vol)
        print 'Error Mail Sent'
        return -1
    return 0


@measure(JUPITER_LOGGER)
def get_no_of_days(month, year):
    """
    for a month and year returns the number of days in it
    :param month:
    :param year:
    :return:
    """
    return monthrange(year=year,
                      month=month)[1]


@measure(JUPITER_LOGGER)
def id_generator(size=8, chars=string.ascii_letters + string.digits):
    """
    random generation of an alpha numeric
    :param size:
    :param chars:
    :return:
    """
    return ''.join(random.choice(chars) for _ in range(size))


@measure(JUPITER_LOGGER)
def generate_rule_trigger_id(trigger_name, competitor, comp_fare_rule, comp_seq):
    """
    generation of Rule trigger ID
    trigger_code + id_generator(6 letter ALPHA NUMERIC number )
    :return:
    """
    trigger_id = ''
    unique_trigger_id = ''
    trigger_crsr = db.JUP_DB_Trigger_Types.find(
        {
            'desc': trigger_name
        }
        ,
        {
            '_id': 1,
            'triggering_event_type': 1,
            'trigger_counter': 1,
            'trigger_code': 1,
            'desc': 1
        }
    )
    if trigger_crsr.count() >= 1:
        # print trigger_crsr[0]
        if trigger_crsr[0]['trigger_code']:
            unique_trigger_id += trigger_crsr[0]['trigger_code']
            last_serial_num = trigger_crsr[0]['trigger_counter']
            if last_serial_num:
                serial_num = last_serial_num + 1
            else:
                serial_num = 1
            serial_num_str = "%06d" % serial_num
            unique_trigger_id += serial_num_str
            db.JUP_DB_Trigger_Types.update(
                {
                    '_id': trigger_crsr[0]['_id']
                }
                ,
                {
                    '$set':
                        {
                            'trigger_counter': serial_num
                        }
                }
            )
            trigger_id += trigger_crsr[0]['trigger_code']
            trigger_id += '-'
            trigger_id += competitor
            trigger_id += '-'
            trigger_id += comp_fare_rule
            trigger_id += '-'
            trigger_id += comp_seq
        else:
            print 'no trigger code present'
    else:
        trigger_id = '****'
        serial_num = 1
        serial_num_str = "%06d" % serial_num
        trigger_id += serial_num_str
    return trigger_id, unique_trigger_id


@measure(JUPITER_LOGGER)
def generate_trigger_id(trigger_name, pct_change, dep_date_start, dep_date_end, origin, destination, db):
    """
    generation of trigger ID
    trigger_code + id_generator(6 letter ALPHA NUMERIC number )
    :return:
    """
    trigger_id = ''
    unique_trigger_id = ''
    trigger_crsr = db.JUP_DB_Trigger_Types.find(
        {
            'desc': trigger_name
        }
        ,
        {
            '_id': 1,
            'triggering_event_type': 1,
            'trigger_counter': 1,
            'trigger_code': 1,
            'desc': 1
        }
    )
    if trigger_crsr.count() >= 1:
        # print trigger_crsr[0]
        if trigger_crsr[0]['trigger_code']:
            unique_trigger_id += trigger_crsr[0]['trigger_code']
            last_serial_num = trigger_crsr[0]['trigger_counter']
            if last_serial_num:
                serial_num = last_serial_num + 1
            else:
                serial_num = 1
            serial_num_str = "%06d" % serial_num
            unique_trigger_id += serial_num_str

            db.JUP_DB_Trigger_Types.update(
                {
                    '_id': trigger_crsr[0]['_id']
                }
                ,
                {
                    '$set':
                        {
                            'trigger_counter': serial_num
                        }
                }
            )
            trigger_id += trigger_crsr[0]['trigger_code']
            if pct_change == "inf":
                trigger_id += "inf"
            elif pct_change > 0:
                if pct_change > 999:
                    trigger_id += "+999"
                else:
                    pct_change_str = "%03d" % pct_change
                    trigger_id += "+" + pct_change_str
            elif pct_change < 0:
                if pct_change < -999:
                    trigger_id += "-999"
                else:
                    pct_change_str = "%04d" % pct_change
                    trigger_id += "" + pct_change_str
            elif trigger_name == "manual":
                trigger_id += "----"
            else:
                trigger_id += "----"
            od = origin + destination
            dep_date_start_mod = datetime.datetime.strptime(dep_date_start, '%d-%m-%Y').strftime('%Y-%m-%d')
            dep_date_end_mod = datetime.datetime.strptime(dep_date_end, '%d-%m-%Y').strftime('%Y-%m-%d')
            this_year = str(datetime.datetime.strptime(SYSTEM_DATE, '%Y-%m-%d').year)
            start_date = 'Start_date_' + this_year
            end_date = 'End_date_' + this_year
            events_cursor = db.JUP_DB_Pricing_Calendar.find({'Market': od}, {'_id': 0, 'Holiday_Name': 1, start_date: 1, end_date: 1})
            if events_cursor.count() > 0:
                events_df = pd.DataFrame(list(events_cursor))
            else:
                events_df = pd.DataFrame(columns=[start_date, end_date, 'Holiday_Name'])
            selected_events = events_df[(events_df[start_date] == dep_date_start_mod) & (events_df[end_date] == dep_date_end_mod)].reset_index()
            if len(selected_events) > 0:
                trigger_id += selected_events['Holiday_Name'][0][:9]
            else:
                trigger_id += dep_date_start[0:2] + \
                              dep_date_start[3:5] + \
                              dep_date_end[0:2] + \
                              dep_date_end[3:5]
        else:
            print 'no trigger code present'
    else:
        trigger_id = '****'
        serial_num = 1
        serial_num_str = "%06d" % serial_num
        trigger_id += serial_num_str
    return trigger_id, unique_trigger_id


@measure(JUPITER_LOGGER)
def get_ratings_details(origin, destination, db,  airline, compartment='Y'):
    """
    """
    response = dict(rating='NA',
                    product_rating='NA',
                    capacity_rating='NA',
                    fare_rating='NA',
                    distributor_rating='NA',
                    market_rating='NA')
    query = dict()
    query['od'] = origin + destination
    if compartment:
        query['compartment'] = compartment

    ratings_crsr = db.JUP_DB_Competitor_Ratings.find(
        {
            'od': origin + destination,
            'compartment': compartment
        }
    )
    # print ratings_crsr.count()
    # print ratings_crsr[0]
    if ratings_crsr.count() == 1:
        if ratings_crsr[0]['competitor_rating']:
            response['rating'] = ratings_crsr[0]['competitor_rating']
        if ratings_crsr[0]['capacity_rating']:
            response['capacity_rating'] = ratings_crsr[0]['capacity_rating']
        if ratings_crsr[0]['product_rating']:
            response['product_rating'] = ratings_crsr[0]['product_rating']
        if ratings_crsr[0]['fare_rating']:
            response['fare_rating'] = ratings_crsr[0]['fare_rating']
        if ratings_crsr[0]['distributor_rating']:
            response['distributor_rating'] = ratings_crsr[0]['distributor_rating']
        if ratings_crsr[0]['market_rating']:
            response['market_rating'] = ratings_crsr[0]['market_rating']
    print response
    return response


@measure(JUPITER_LOGGER)
def get_price_movement(airline,
                       pos,
                       origin,
                       destination,
                       compartment,
                       oneway_return,
                       dep_date_start,
                       dep_date_end,
                       currency,
                       source='infare'):
    """
    :param pos:
    :param origin:
    :param destination:
    :param compartment:
    :return:
    """
    #   Lowest Fare
    #   Average Fare
    #   Highest Fare
    response = dict(lowest_fare=dict(base_fare='NA', tax='NA', yq='NA', yr='NA', surcharges='NA', total_fare='NA', frequency='NA'),
                    highest_fare=dict(base_fare='NA', tax='NA', yq='NA', yr='NA', surcharges='NA', total_fare='NA', frequency='NA'))
    if source == 'infare':
        unique_fares_data = get_infare_unique_fares_data(airline=airline,
                                                         pos=pos,
                                                         origin=origin,
                                                         destination=destination,
                                                         compartment=compartment,
                                                         oneway_return=oneway_return,
                                                         dep_date_start=dep_date_start,
                                                         dep_date_end=dep_date_end,
                                                         observation_date_start=None,
                                                         observation_date_end=None)
        print unique_fares_data
        if len(unique_fares_data) > 0:
            sorted_unique_fares = sorted(unique_fares_data, key=lambda x: x['base_fare'])
            response['lowest_fare'].update(sorted_unique_fares[0])
            response['highest_fare'].update(sorted_unique_fares[-1])
        return response

    elif source == 'ATPCO':
        """
        Filter ATPCO fares by CXR, Market, O/R type (user input to view O/R in WF)
        and departure date range. Fetch the lowest filed fare.
        The above fare can be a tactical fare launched with sale/departure date
        restriction (or even flight/DOW restrictions).
        In this case, we display the restrictions/rules associated with
        the fare in competitor summary (Level 2).
        (If analyst wants to see any other fares, he/she will have to go the fare analysis screen)
        """
        if oneway_return == 3:
            oneway_return = 1
        fare_query = defaultdict(list)
        dep_query = defaultdict(list)
        if not dep_date_start:
            dep_date_start = SYSTEM_DATE
        if not dep_date_end:
            dep_date_end = INF_DATE_STR
        if dep_date_start > dep_date_end:
            print 'ERROR'

        fare_query = defaultdict(list)
        fare_query['$and'].append({'fare': {'$nin': [None, 0.0, 0]}})
        fare_query['$and'].append({'carrier': airline})
        # if pos:
        #     fare_query['$and'].append({'pos': pos})
        if origin:
            fare_query['$and'].append({'origin': origin})
        if destination:
            fare_query['$and'].append({'destination': destination})
        if compartment:
            fare_query['$and'].append({'compartment': compartment})
        if oneway_return:
            if oneway_return != 2:
                fare_query['$and'].append({'oneway_return': {'$in': [1, 3]}})
            fare_query['$and'].append({'oneway_return': oneway_return})
        # if rbd_type:
        #     fare_query['$and'].append({'RBD_type': {'$in':[ rbd_type, None]}})
        if currency:
            fare_query['$and'].append({'currency': currency})

        fare_query['$and'].append(
            {
                '$or':
                    [
                        {'effective_from': None},
                        {'effective_from': {'$lte': SYSTEM_DATE}}
                    ]
            }
        )

        fare_query['$and'].append(
            {
                '$or':
                    [
                        {'effective_to': None},
                        {'effective_to': {'$gte': SYSTEM_DATE}}
                    ]
            }
        )

        dep_query['$or'].append({
            '$and': [
                {'dep_date_from': {'$lte': dep_date_end}},
                {'dep_date_end': {'$gte': dep_date_start}}
            ]
        })
        dep_query['$or'].append({
            '$and': [
                {'dep_date_from': {'$lte': dep_date_end}},
                {'dep_date_end': None}
            ]
        })
        dep_query['$or'].append({
            '$and': [
                {'dep_date_from': None},
                {'dep_date_end': {'$gte': dep_date_start}}
            ]
        })
        dep_query['$or'].append({
            '$and': [
                {'dep_date_from': None},
                {'dep_date_end': None}
            ]
        })

        fare_query['$and'].append(dict(dep_query))

        print dict(fare_query)
        crsr = db.JUP_DB_ATPCO_Fares.aggregate([
            {
                '$match': fare_query
            }
            ,
            {
                '$project':
                    {
                        '_id':0,
                        'fare_basis':'$fare_basis',
                        'base_fare':'$fare',
                        'tax':'$taxes',
                        'surcharge':'$surcharge',
                        'yq':'$YQ',
                        'total_fare':'$total_fare',

                    }
            }
            ,
            {
                '$sort': {
                    'total_fare':1
                }
            }
        ]
        )
        data = list(crsr)
        if len(data) > 0:
            response['lowest_fare']['base_fare'] = data[0]['base_fare']
            response['lowest_fare']['total_fare'] = data[0]['total_fare']
            response['lowest_fare']['tax'] = data[0]['tax']
            response['lowest_fare']['yq'] = data[0]['yq']
            response['lowest_fare']['surcharge'] = data[0]['surcharge']
            response['lowest_fare']['fare_basis'] = data[0]['fare_basis']
            response['highest_fare']['base_fare'] = data[-1]['base_fare']
            response['highest_fare']['total_fare'] = data[-1]['total_fare']
            response['highest_fare']['tax'] = data[-1]['tax']
            response['highest_fare']['yq'] = data[-1]['yq']
            response['highest_fare']['surcharge'] = data[-1]['surcharge']
            response['highest_fare']['fare_basis'] = data[-1]['fare_basis']
        return response
    else:
        return response



    # if airline == Host_Airline_Code:
    #     host_data = get_host_fareladder(pos=pos, origin=origin, destination=destination, compartment=compartment,
    #                                     oneway_return=oneway_return,
    #                                     dep_date_start=dep_date_start, dep_date_end=dep_date_end)
    #     host_fl = numpy.array(host_data[1])
    #     print 'host_fl', host_fl
    #     response['lowest_fare'] = numpy.amin(host_fl)
    #     response['avg_fare'] = numpy.average(host_fl)
    #     response['highest_fare'] = numpy.amax(host_fl)
    #     print 'lowest, average, highest', response['lowest_fare'], response['avg_fare'], response['highest_fare']
    # else:
    #     comp_data = get_competitor_fareladder(airline=airline, pos=pos, origin=origin, destination=destination,
    #                                           compartment=compartment, dep_date_start=dep_date_start,
    #                                           dep_date_end=dep_date_end, host_currency=currency,
    #                                           oneway_return=oneway_return)
    #     comp_fl = numpy.array(comp_data[1])
    #     print 'comp_fl', comp_fl
    #     response['lowest_fare'] = numpy.amin(comp_fl)
    #     response['avg_fare'] = numpy.average(comp_fl)
    #     response['highest_fare'] = numpy.amax(comp_fl)
    #     print 'lowest, average, highest', response['lowest_fare'], response['avg_fare'], response['highest_fare']


@measure(JUPITER_LOGGER)
def get_threshold_values(trigger_type, db):
    cur = db.JUP_DB_Trigger_Types.find({'desc': trigger_type})
    for i in cur:
        upper_threshold = i['upper_threshhold']
        lower_threshold = i['lower_threshhold']
    return upper_threshold, lower_threshold


if __name__ == '__main__':
    client = mongo_client()
    db=client[JUPITER_DB]
    # print db
    # val = get_price_movement(airline='EK',
    #                          pos='DXB',
    #                          origin='DXB',
    #                          destination='DOH',
    #                          compartment='Y',
    #                          oneway_return=2,
    #                          currency='SAR',
    #                          dep_date_start='2017-05-01',
    #                          dep_date_end='2017-05-31')
    # print json.dumps(val,indent=1)

    # print db
    # val = get_price_movement(source='ATPCO',
    #                          airline='EK',
    #                          pos='DXB',
    #                          origin='DXB',
    #                          destination='DOH',
    #                          compartment='Y',
    #                          oneway_return=2,
    #                          currency='AED',
    #                          dep_date_start='2017-02-01',
    #                          dep_date_end='2017-05-31')
    # print json.dumps(val, indent=1)

    # val = get_ratings_details('DXB','DOH','FZ')
    # print val

    pos_level_currency_data(db=db)
    client.close()