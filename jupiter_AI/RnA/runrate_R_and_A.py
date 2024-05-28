import datetime
import json
import time
from jupiter_AI import client, JUPITER_DB, Host_Airline_Hub
db = client[JUPITER_DB]



def get_day_difference(start_date, end_date):
    sd = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    ed = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    diff = (ed - sd).days
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
            what = 'The Bookings have fallen down'
            why = ''.join(['The bookings run_rate value ', str(run_rate),
                           'bookings/day has fallen down below the lower threshold value assigned i.e. ',
                           str(lower_threshold)])
            status_quo = 'Further drop in market share'
            action = ''.join(['No action required as calculated ticketed percentage(',
                              str(ticketed_percent), '%) is more than 90'])
            info = [False, True]
        else:
            what = 'The Bookings have fallen down'
            why = ''.join('The bookings run_rate value ',
                          'bookings/day has_fallen down below the lower threshold value assigned i.e. ',
                          str(run_rate),
                          str(lower_threshold))
            status_quo = 'Further drop in market share'
            action = ''.join('Change Relevant Prices according to the Model')
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


def R_and_A_price_runrate(filter):
    total_bookings = 0
    total_ticketed = 0
    total_capacity = 0
    upper_threshold = 5
    lower_threshold = 1
    aggregate_upper_threshold = 50
    aggregate_lower_threshold = 10
    RA = []
    for l in filter['compartment']:
        ods_low_run_rate = 0
        ods_action_required = 0
        total_ods = 0
        for i in filter['region']:
            for j in filter['pos']:
                for k, org in enumerate(filter['origin']):
                    if filter['flag'] == 'true':
                        cursor1 = db.JUP_DB_Booking_DepDate.find({'region': i,
                                                                  'pos': j,
                                                                  'origin': org,
                                                                  'destination': filter['destination'][k],
                                                                  'compartment': l})
                        #   Include Dates of filters here
                        print cursor1.count()
                    else:
                        cursor1 = db.JUP_DB_Booking_BookDate.find({'region': i,
                                                                   'pos': j,
                                                                   'origin': org,
                                                                   'destination': filter['destination'][k],
                                                                   'compartment': l})
                    # cursor1 = db.JUP_DB_OAG_Data.find({'region':i,
                    #										'pos':j,
                    #										'origin':org,
                    #										'destination':filter['destination'][k],
                    #										'compartment':l})
                    # if cursor2.count() != 0:
                    # 	capacity = 0
                    # 	for n in cursor2:
                    # 		capacity += n['capacity']
                    # 	total_capacity += capacity
                    if cursor1.count() != 0:
                        total_ods += 1
                        bookings = 0
                        ticketed = 0
                        capacity = 100
                        for m in cursor1:
                            bookings += int(m['pax'])
                            ticketed += int(m['ticket'])
                        total_bookings += bookings
                        total_ticketed += ticketed
                        total_capacity += capacity
                        p = do_run_rate_analysis(bookings, ticketed, capacity, upper_threshold, lower_threshold,
                                                 filter['start_date'], filter['end_date'])
                        if p[0] == False:
                            ods_low_run_rate += 1
                            if p[0] == False:
                                ods_action_required += 1
        individual_RA = {}
        if ods_low_run_rate != 0:
            individual_RA['what'] = 'Negative Bookings'
            individual_RA['why'] = ''.join([str(ods_low_run_rate), ' out of ', str(total_ods), ' ods have low run_rate',
                                            ' for the compartment ', str(l), ' w.r.t the respective threshold values'])
            individual_RA['status_quo'] = 'Further Fall in market share'
            if ods_action_required != 0:
                individual_RA['action'] = ''.join(['Change Prices according to the model for compartment', str(l),
                                                   'for ', str(ods_action_required),
                                                   'ODs as less than 90 percent of capacity is ticketed'])
            else:
                individual_RA['action'] = 'No action required as 90 percent of capacity is already ticketed'
        if individual_RA != {}:
            RA.append(individual_RA)
    aggregate_RA = do_run_rate_analysis(total_bookings,
                                        total_ticketed,
                                        total_capacity,
                                        aggregate_upper_threshold,
                                        aggregate_lower_threshold,
                                        filter['start_date'],
                                        filter['end_date'],
                                        True)
    if aggregate_RA != {}:
        RA.append(aggregate_RA)
    if RA != []:
        return RA


def main():
    filter = {'region': ['GCC', 'MiddleEast'],
              'pos': ['ELQ', Host_Airline_Hub],
              'origin': ['KRT', 'ELQ'],
              'destination': [Host_Airline_Hub, 'KTM'],
              'compartment': ['J', 'Y', 'A'],
              'start_date': '2016-09-16',
              'end_date': '2016-09-23',
              'flag': 'true'}
    data = R_and_A_price_runrate(filter)
    print json.dumps(data, indent=1)


start_time = time.time()
print start_time
main()
print("--- %s seconds ---" % (time.time() - start_time))
