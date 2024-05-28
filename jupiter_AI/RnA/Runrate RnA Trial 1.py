import ast
import datetime
import json
import time

import pandas as pd
from jupiter_AI import client,JUPITER_DB
db = client[JUPITER_DB]
start_time = time.time()


from jupiter_AI.network_level_params import Host_Airline_Code as Host

def R_and_A_price_runrate(afilter):
    total_bookings = 0
    total_ticketed = 0
    total_capacity = 0
    RA = []
    upper_threshold = 100        #Hard coded-- should be replaced by values from the configuration screen module
    lower_threshold = 50         #Hard coded-- should be replaced by values from the configuration screen module
    aggregate_upper_threshold = 50 #Hard coded-- should be replaced by values from the configuration screen module
    aggregate_lower_threshold = 10 #Hard coded-- should be replaced by values from the configuration screen module

    query = dict()
    response = dict()
    if afilter['region']:
        query['region'] = {'$in': afilter['region']}
    if afilter['country']:
        query['country'] = {'$in': afilter['country']}
    if afilter['pos']:
        query['pos'] = {'$in': afilter['pos']}
    if afilter['origin']:
        od_build = []
        for idx, item in enumerate(afilter['origin']):
            od_build.append({'origin': item, 'destination': afilter['destination'][idx]})
        query['$or'] = od_build
    if afilter['compartment']:
        query['compartment'] = {'$in': afilter['compartment']}
    a_pipeline_user = [
            {
                '$match': query,
            },
            {
                '$group': {'_id': {'dep_date': '$dep_date',
                                   'od': '$od',
                                   'compartment':'$compartment'},
                           'bookings': {'$sum': '$pax'},
                           'ticketed': {'$sum': '$ticket'}
                           }
            },
            {
                '$group': {'_id': {'od':'$_id.od', 'compartment':'$_id.compartment'},
                           'bookings':{'$sum': '$bookings'},
                           'ticketed':{'$sum': '$ticketed'}}
            }
            ]
    if afilter['flag'] == 0: #for dep date
        query['dep_date'] = {'$gte': afilter['fromDate'], '$lte': afilter['toDate']}
        cursor_user = db.JUP_DB_Booking_DepDate.aggregate(a_pipeline_user)
    elif afilter['flag'] == 1:  # for book date
        query['book_date'] = {'$gte': afilter['fromDate'], '$lte': afilter['toDate']}
        cursor_user = db.JUP_DB_Booking_BookDate.aggregate(a_pipeline_user)

    data_user = list(cursor_user)
    for i in data_user:
        print i
    query1 = dict()
    if afilter['region']:
        query1['region'] = {'$in': afilter['region']}
    if afilter['country']:
        query1['country'] = {'$in': afilter['country']}
    if afilter['pos']:
        query1['pos'] = {'$in': afilter['pos']}
    if afilter['origin']:
        od_build = []
        for idx, item in enumerate(afilter['origin']):
            od_build.append({'origin': item, 'destination': afilter['destination'][idx]})
        query1['$or'] = od_build
    if afilter['compartment']:
        query1['compartment'] = {'$in': afilter['compartment']}
    if afilter['fromDate']:
        query1['effective_from'] = {'$lte': afilter['toDate']}
        query1['effective_to'] = {'$gte':afilter['fromDate']}
    query1['airline'] = Host
    a_pipeline_user1 = [
        {
            '$match': query1,
        }
    ]
    cursor_user1 = db.JUP_DB_Schedule_Capacity.aggregate(a_pipeline_user1)
    data_user1 = list(cursor_user1)
    list_of_od = []
    od_capacity = []
    # for i in data_user1:
    #     print i
    for i in data_user1:
        list_of_od.append(i['od'])
    list_of_od = set(list_of_od)
    list_of_od = list(list_of_od)
    print list_of_od

    for j in list_of_od:
        total_capacity = 0
        for i in data_user1:
            if i['od'] == j:
                date_effective = datetime.datetime.strptime(i['effective_to'], "%Y-%m-%d").strftime('%Y-%m-%d')
                if date_effective > afilter['toDate']:
                    i['effective_to'] = afilter['toDate']
                frequency = i['frequency']
                frequency = ast.literal_eval(json.dumps(frequency))
                frequency = list(frequency)
                daterange = pd.date_range(datetime.datetime.strptime(i['effective_from'], "%Y-%m-%d"),
                                          datetime.datetime.strptime(i['effective_to'], "%Y-%m-%d"))
                for single_date in daterange:
                    # print single_date
                    if str(single_date.isoweekday()) in frequency:
                        total_capacity += int(i['capacity'])
        od_capacity.append({'od': j , 'capacity': total_capacity})
    for j in od_capacity:
        print j

    for j in od_capacity:
        for i in data_user:
            if i['_id']['od'] == j['od']:
                i['capacity'] = j['capacity']
                print i
    print 'hello'
    ods_low_run_rate = 0
    ods_action_required = 0
    for i in data_user:
        print i
        p = do_run_rate_analysis(i['bookings'], i['ticketed'], i['capacity'], upper_threshold, lower_threshold,
                             afilter['fromDate'],afilter['toDate'], )
        if p[0] == False:
            ods_low_run_rate += 1
            if p[1] == False:
                ods_action_required += 1
        individual_RA = {}
        if ods_low_run_rate != 0:
            individual_RA['what'] = 'Negative Bookings'
            if afilter['compartment']:
                individual_RA['why'] = ([str(ods_low_run_rate) + ' out of ' + str(len(list_of_od)) +
                                     ' ods have low run_rate',
                                     ' for the compartment ' + str(i['_id']['compartment']) +
                                     ' w.r.t the respective threshold values'])
            else:
                individual_RA['why'] = ([str(ods_low_run_rate) + ' out of ' + str(len(list_of_od)) +
                                         ' ods have low run_rate',
                                         ' for all compartments ' +
                                         ' w.r.t the respective threshold values'])
            individual_RA['status_quo'] = 'Further Fall in market share'
            if ods_action_required != 0:
                individual_RA['action'] = 'Change Prices according to the model for compartment '+ \
                                          str(i['_id']['compartment']) + ' for '+ str(ods_action_required) +\
                                          ' ODs as less than 90 percent of capacity is ticketed'
            else:
                individual_RA['action'] = 'No action required as 90 percent of capacity is already ticketed'
        if individual_RA != {}:
            RA.append(individual_RA)
    for x in RA:
        print x
    return 5

def get_day_difference(start_date, end_date):
    sd = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    ed = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    diff = (ed - sd).days + 1
    return diff

def do_run_rate_analysis(bookings, ticketed, capacity, upper_threshold, lower_threshold, start_date, end_date,
                         aggregation=False):
    no_of_days = get_day_difference(start_date, end_date)
    print 'no_of_days', no_of_days
    run_rate = float(bookings) / no_of_days
    print 'run_rate', run_rate
    ticketed_percent = float(ticketed) * 100 / capacity
    print 'ticketed_percent', ticketed_percent
    RA = dict()
    info = [True, True]
    if run_rate < lower_threshold:
        if ticketed_percent >= 90:
            info = [False, True]
        else:
            info = [False, False]
    elif run_rate > upper_threshold:
        if ticketed_percent >= 90:
            pass
        else:
            pass
    if aggregation:
        return RA
    else:
        return info


if __name__ == '__main__':
    a = {
        'region': [],
        'country': [],
        'pos': [],
        'origin': ['DOH', 'DXB'],
        'destination': ['DXB', 'DOH'],
        'compartment': ['Y', 'J'],
        'fromDate': '2016-10-01',
        'toDate': '2016-10-30',
        'from_month': 8,
        'from_year':2016,
        'to_month': 12,
        'to_year': 2016,
        'flag' : 0
    }

    print R_and_A_price_runrate(afilter=a)
    print time.time() - start_time
