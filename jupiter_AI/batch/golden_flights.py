"""
Author: Abhinav Garg
Created with <3
Date Created: 2018-01-30
File Name: golden_flights.py

This code analyzes the golden flights that have been running with near full capacity for a significant period of
time and populates the JUP_DB_Golden_Flights collection daily as a batch file.
Base collection used: Inventory Leg

"""
import time
import datetime
from jupiter_AI import SYSTEM_DATE, JUPITER_DB, mongo_client, JUPITER_LOGGER
from jupiter_AI.logutils import measure


@measure(JUPITER_LOGGER)
def get_threshold_values(db):
    period = 0
    seat_factor_upper_threshold = 0
    seat_factor_lower_threshold = 0
    frequency = 0
    user_defined_flights = None
    crsr = db.JUP_DB_Thresholds.find({"item": "Golden Flights", "active": True,
                                      "$or": [{"effective_date_from": {"$lte": SYSTEM_DATE}},
                                              {"effective_date_from": ""}],
                                      "$or": [{"effective_date_to": {"$gte": SYSTEM_DATE}},
                                              {"effective_date_to": ""}]})
    for i in crsr:
        print i
        period = i['dep_period_in_days']
        if i['seat_factor_threshold']['type'] == 'percentage':
            seat_factor_upper_threshold = i['seat_factor_threshold']['upper_value']
            seat_factor_lower_threshold = i['seat_factor_threshold']['lower_value']
        if i['frequency']['type'] == 'percentage':
            frequency = i['frequency']['value']
        try:
            user_defined_flights = i['user_defined_flights']
        except KeyError:
            user_defined_flights = None

    if seat_factor_upper_threshold == '':
        seat_factor_upper_threshold = 200
    return period, seat_factor_upper_threshold, seat_factor_lower_threshold, frequency, user_defined_flights


@measure(JUPITER_LOGGER)
def get_golden_flights_query(valid_start_date, valid_end_date, seat_factor_upper_threshold,
                             seat_factor_lower_threshold):
    query_pipeline_y = [
        {
            '$match': {
                'dep_date': {
                    '$lte': valid_start_date,
                    '$gte': valid_end_date
                },
                'y_seat_factor': {'$ne': None},
                'sys_snap_date': {'$gte': valid_end_date}
            }
        },
        {
            "$sort": {
                "sys_snap_date": 1
            }
        },
        {
            '$project': {
                'flight_num': '$Flight_Number',
                'dep_date': '$dep_date',
                'sys_snap_date': '$snap_date',
                'dow': '$day_of_week',
                'sector_origin': '$origin',
                'sector_destination': '$destination',
                'seat_factor': '$y_seat_factor'
            }
        },
        {
            "$group": {
                "_id": {
                    'dep_date': '$dep_date',
                    'flight_num': '$flight_num',
                    'sector_origin': '$sector_origin',
                    'sector_destination': '$sector_destination',
                    'dow': '$dow',
                },
                "docs": {"$last": "$$ROOT"}
            }
        },
        {
            '$group': {
                '_id': {
                    'flight_num': '$docs.flight_num',
                    'sector_origin': '$docs.sector_origin',
                    'sector_destination': '$docs.sector_destination',
                    'dow': '$docs.dow',
                },
                'total_flown': {
                    '$sum': 1
                },
                'high_sf_times_flown': {
                    '$sum': {
                        '$cond': [{
                            '$gte': ['$docs.seat_factor', seat_factor_lower_threshold]
                        },
                            {'$cond': [{
                                '$lt': ['$docs.seat_factor', seat_factor_upper_threshold]
                            }, 1, 0]},
                            0]
                    }
                },
                'seat_factor_track': {"$push": {'seat_factor': '$docs.seat_factor', 'dep_date': '$docs.dep_date'}}
            }
        },
        {
            '$project': {
                '_id': 0,
                'flight_num': '$_id.flight_num',
                'sector_origin': '$_id.sector_origin',
                'sector_destination': '$_id.sector_destination',
                'compartment': 'Y',
                'total_flown': 1,
                'high_sf_times_flown': 1,
                'seat_factor_track': 1,
                'dow': '$_id.dow',
                'high_sf_percentage': {
                    '$divide': [{
                        '$multiply': ['$high_sf_times_flown', 100]
                    }, '$total_flown']
                },
            }
        },
        {
            '$lookup': {
                'from': 'JUP_DB_Region_Master',
                'localField': 'sector_origin',
                'foreignField': 'POS_CD',
                'as': 'region_docs'
            }
        },
        {"$unwind": {"path": "$region_docs"}},
        {
            '$project': {
                'flight_num': 1,
                'sector_origin': 1,
                'sector_destination': 1,
                'compartment': 1,
                'total_flown': 1,
                'high_sf_times_flown': 1,
                'seat_factor_track': 1,
                'dow': 1,
                'high_sf_percentage': 1,
                'region': '$region_docs.Region',
                'cluster': '$region_docs.Cluster',
                'country': '$region_docs.COUNTRY_CD'
            }
        }
    ]

    query_pipeline_j = [
        {
            '$match': {
                'dep_date': {
                    '$lte': valid_start_date,
                    '$gte': valid_end_date
                },
                'j_seat_factor': {'$ne': None},
                'sys_snap_date': {'$gte': valid_end_date}
            }
        },
        {
            "$sort": {
                "sys_snap_date": 1
            }
        },
        {
            '$project': {
                'flight_num': '$Flight_Number',
                'dep_date': '$dep_date',
                'sys_snap_date': '$snap_date',
                'dow': '$day_of_week',
                'sector_origin': '$origin',
                'sector_destination': '$destination',
                'seat_factor': '$j_seat_factor'
            }
        },
        {
            "$group": {
                "_id": {
                    'dep_date': '$dep_date',
                    'flight_num': '$flight_num',
                    'sector_origin': '$sector_origin',
                    'sector_destination': '$sector_destination',
                    'dow': '$dow',
                },
                "docs": {"$last": "$$ROOT"}
            }
        },
        {
            '$group': {
                '_id': {
                    'flight_num': '$docs.flight_num',
                    'sector_origin': '$docs.sector_origin',
                    'sector_destination': '$docs.sector_destination',
                    'dow': '$docs.dow',
                },
                'total_flown': {
                    '$sum': 1
                },
                'high_sf_times_flown': {
                    '$sum': {
                        '$cond': [{
                            '$gte': ['$docs.seat_factor', seat_factor_lower_threshold]
                        },
                            {'$cond': [{
                                '$lt': ['$docs.seat_factor', seat_factor_upper_threshold]
                            }, 1, 0]},
                            0]
                    }
                },
                'seat_factor_track': {"$push": {'seat_factor': '$docs.seat_factor', 'dep_date': '$docs.dep_date'}}
            }
        },
        {
            '$project': {
                '_id': 0,
                'flight_num': '$_id.flight_num',
                'sector_origin': '$_id.sector_origin',
                'sector_destination': '$_id.sector_destination',
                'compartment': 'J',
                'total_flown': 1,
                'high_sf_times_flown': 1,
                'seat_factor_track': 1,
                'dow': '$_id.dow',
                'high_sf_percentage': {
                    '$divide': [{
                        '$multiply': ['$high_sf_times_flown', 100]
                    }, '$total_flown']
                },
            }
        },
        {
            '$lookup': {
                'from': 'JUP_DB_Region_Master',
                'localField': 'sector_origin',
                'foreignField': 'POS_CD',
                'as': 'region_docs'
            }
        },
        {"$unwind": {"path": "$region_docs"}},
        {
            '$project': {
                'flight_num': 1,
                'sector_origin': 1,
                'sector_destination': 1,
                'compartment': 1,
                'total_flown': 1,
                'high_sf_times_flown': 1,
                'seat_factor_track': 1,
                'dow': 1,
                'high_sf_percentage': 1,
                'region': '$region_docs.Region',
                'cluster': '$region_docs.Cluster',
                'country': '$region_docs.COUNTRY_CD'
            }
        }
    ]
    return query_pipeline_y, query_pipeline_j


@measure(JUPITER_LOGGER)
def add_user_defined_flights(user_defined_flights, db):
    for j in user_defined_flights:
        inv_cur = list(db.JUP_DB_Inventory_Leg.find({'Flight_Number': j['flight_number'],
                                                     'day_of_week': j['day_of_week'],
                                                     'dep_date': {'$gte': SYSTEM_DATE}}).limit(1))
        if len(inv_cur) > 0:
            dict_to_insert = {}
            dict_to_insert['flight_num'] = j['flight_number']
            dict_to_insert['dow'] = j['day_of_week']

            for k in inv_cur:
                dict_to_insert['sector_origin'] = k['origin']
                dict_to_insert['sector_destination'] = k['destination']
                dict_to_insert['compartment'] = j['compartment']

            reg_cur = db.JUP_DB_Region_Master.find({'POS_CD': dict_to_insert['sector_origin']})
            for k in reg_cur:
                dict_to_insert['region'] = k['Region']
                dict_to_insert['cluster'] = k['Cluster']
                dict_to_insert['country'] = k['COUNTRY_CD']

            dict_to_insert["last_update_date"] = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d")
            dict_to_insert['user_defined_flight'] = True
            db.JUP_DB_Golden_Flights.insert(dict_to_insert)
        else:
            print 'No record found in Inventory leg'


@measure(JUPITER_LOGGER)
def generate_golden_flights(client):
    db = client[JUPITER_DB]
    st = time.time()
    period, seat_factor_upper_threshold, seat_factor_lower_threshold, frequency, user_defined_flights = get_threshold_values(
        db)
    valid_start_date = SYSTEM_DATE
    valid_end_date = datetime.datetime.strftime(datetime.datetime.strptime(SYSTEM_DATE, "%Y-%m-%d") -
                                                datetime.timedelta(days=period), "%Y-%m-%d")
    print valid_start_date, valid_end_date
    result = db.JUP_DB_Golden_Flights.delete_many({})
    print "removed", result.deleted_count

    query_pipeline_y, query_pipeline_j = get_golden_flights_query(valid_start_date, valid_end_date,
                                                                  seat_factor_upper_threshold,
                                                                  seat_factor_lower_threshold)
    cursor = db.JUP_DB_Inventory_Leg.aggregate(query_pipeline_y, allowDiskUse=True)
    for i in cursor:
        if i['high_sf_percentage'] >= frequency:
            i["last_update_date"] = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d")
            i['user_defined_flight'] = False
            db.JUP_DB_Golden_Flights.insert(i)

    print "---------- y done -------------"

    cursor = db.JUP_DB_Inventory_Leg.aggregate(query_pipeline_j, allowDiskUse=True)
    for i in cursor:
        if i['high_sf_percentage'] >= frequency:
            i["last_update_date"] = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d")
            i['user_defined_flight'] = False
            db.JUP_DB_Golden_Flights.insert(i)

    print "--------- j done -------------"

    if user_defined_flights:
        print 'Adding user defined flights'
        add_user_defined_flights(user_defined_flights, db)

    print 'time taken', time.time() - st


if __name__ == '__main__':
    client = mongo_client()
    generate_golden_flights(client)
    client.close()
