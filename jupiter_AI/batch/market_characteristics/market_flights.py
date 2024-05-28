"""
Author: Abhinav Garg
Created with <3
Date Created: 2017-08-10
File Name: market_flights.py

Creates flight characteristics from inventory, flight_leg and pros data.

"""
from jupiter_AI import mongo_client, JUPITER_DB, SYSTEM_DATE, Host_Airline_Code, JUPITER_LOGGER
from jupiter_AI.logutils import measure
import pymongo
from datetime import datetime, timedelta
import time
from dateutil.relativedelta import relativedelta
from copy import deepcopy
import pandas as pd
from jupiter_AI.triggers.common import cursor_to_df
from collections import OrderedDict

inv_cols = ['Flight_Number',
            'dep_date',
            'origin',
            'destination',
            'od',
            'j_cap',
            'j_seat_factor',
            'y_seat_factor',
            'j_booking',
            'y_cap',
            'y_booking',
            'j_allocation',
            'y_allocation',
            'day_of_week',
            'sys_snap_date']
flight_leg_cols = ['Flight_num',
                   'od',
                   'dep_date',
                   'snap_date',
                   'origin',
                   'destination',
                   'compartment',
                   'pax',
                   'revenue',
                   'rpkm',
                   'askm',
                   'Yield',
                   'rasm',
                   'Seat_Factor']
pros_cols = ['flight_number',
             'origin',
             'destination',
             'compartment',
             'dep_date',
             'snap_date',
             'leg_cabin_re_calc_out',
             'od']


@measure(JUPITER_LOGGER)
def get_this_year_df(snap_date, db):
    inv_cur = db.JUP_DB_Inventory_Leg.aggregate([
        {
            '$match': {"sys_snap_date": snap_date}
        },
        {
            '$project': {
                '_id': 0,
                'Flight_Number': 1,
                'dep_date': 1,
                'origin': 1,
                'destination': 1,
                'od': 1,
                'j_cap': 1,
                'j_seat_factor': 1,
                'y_seat_factor': 1,
                'j_booking': 1,
                'y_cap': 1,
                'y_booking': 1,
                'j_allocation': 1,
                'y_allocation': 1,
                'day_of_week': 1,
                'sys_snap_date': 1
            }
        }
    ])

    print 'Queried this year inventory'
    y_df = cursor_to_df(inv_cur)
    if len(y_df) == 0:
        y_df = pd.DataFrame(columns=inv_cols)
    y_df.rename(columns={'Flight_Number': 'flight_number', 'sys_snap_date': 'snap_date'}, inplace=True)
    j_df = deepcopy(y_df)
    y_df.drop(['j_cap', 'j_seat_factor', 'j_booking', 'j_allocation'], axis=1, inplace=True)
    y_df.rename(columns={'y_cap': 'capacity', 'y_seat_factor': 'booked_seat_factor', 'y_booking': 'bookings',
                         'y_allocation': 'allocation'}, inplace=True)
    y_df['compartment'] = 'Y'
    j_df.drop(['y_cap', 'y_seat_factor', 'y_booking', 'y_allocation'], axis=1, inplace=True)
    j_df.rename(columns={'j_cap': 'capacity', 'j_seat_factor': 'booked_seat_factor', 'j_booking': 'bookings',
                         'j_allocation': 'allocation'}, inplace=True)
    j_df['compartment'] = 'J'
    inv_df = pd.concat([y_df, j_df])
    inv_df['flight_number'] = inv_df['flight_number'].astype('int')
    print 'Built this year inventory df'

    flight_leg_cur = db.JUP_DB_Flight_Leg.aggregate([
        {'$match': {'snap_date': snap_date}
         },
        {
            '$project': {
                'Flight_num': 1,
                'od': 1,
                'dep_date': 1,
                'snap_date': 1,
                'origin': {'$substr': ['$od', 0, 3]},
                'destination': {'$substr': ['$od', 3, 3]},
                'compartment': 1,
                'pax': 1,
                'revenue': 1,
                'rpkm': 1,
                'askm': 1,
                'Yield': 1,
                'rasm': 1,
                'Seat_Factor': 1,
                '_id': 0,
            }
        }
    ])

    print 'Queried this year flight leg'
    flight_leg_df = cursor_to_df(flight_leg_cur)
    if len(flight_leg_df) == 0:
        flight_leg_df = pd.DataFrame(columns=flight_leg_cols)
    flight_leg_df.rename(columns={'Flight_num': 'flight_number', 'Seat_Factor': 'ticketed_seat_factor',
                                  'Yield': 'yield'}, inplace=True)
    flight_leg_df['flight_number'] = flight_leg_df['flight_number'].astype('int')

    print 'Built this year flight leg df'

    pros_cur = db.JUP_DB_Pros_Flight.aggregate([
        {
            '$match': {'snap_date': snap_date, 'carrier': Host_Airline_Code}
        },
        {
            '$project': {
                'flight_number': 1,
                'origin': 1,
                'destination': 1,
                'compartment': 1,
                'dep_date': 1,
                'snap_date': 1,
                'leg_cabin_re_calc_out': 1,
                'od': {'$concat': ['$origin', '$destination']},
                '_id': 0
            }
        }
    ])

    print 'Queried this year pros'
    pros_df = cursor_to_df(pros_cur)
    if len(pros_df) == 0:
        pros_df = pd.DataFrame(columns=pros_cols)
    pros_df.rename(columns={'leg_cabin_re_calc_out': 'pax_forecast'}, inplace=True)
    pros_df['flight_number'] = pros_df['flight_number'].astype('int')
    print 'Built this year pros df'

    this_year_df = inv_df.merge(flight_leg_df, on=['flight_number', 'od', 'origin', 'destination',
                                                   'compartment', 'dep_date', 'snap_date'], how='left')
    this_year_df = this_year_df.merge(pros_df, on=['flight_number', 'od', 'origin', 'destination',
                                                   'compartment', 'dep_date', 'snap_date'], how='left')
    this_year_df.fillna(0, inplace=True)

    return this_year_df


@measure(JUPITER_LOGGER)
def run_this_year(snap_date, db):
    cur = db.JUP_DB_Inventory_Leg.find(
        {
            "sys_snap_date": snap_date
        },
        {
            '_id': 0,
            'Flight_Number': 1,
            'dep_date': 1,
            'origin': 1,
            'destination': 1,
            'od': 1,
            'j_cap': 1,
            'j_seat_factor': 1,
            'y_seat_factor': 1,
            'j_booking': 1,
            'y_cap': 1,
            'y_booking': 1,
            'j_allocation': 1,
            'y_allocation': 1,
            'day_of_week': 1,
            'sys_snap_date': 1
        }
        , no_cursor_timeout=True)

    bulk_list = []
    count = 1
    t = 0
    for i in cur:
        i['flight_number'] = i['Flight_Number']
        i['snap_date'] = i['sys_snap_date']
        del i['sys_snap_date']
        del i['Flight_Number']
        RBD = deepcopy(i)
        del RBD['day_of_week']
        del RBD['snap_date']
        del RBD['flight_number']
        del RBD['dep_date']
        del RBD['origin']
        del RBD['destination']
        del RBD['od']
        del RBD['j_cap']
        del RBD['j_seat_factor']
        del RBD['y_seat_factor']
        del RBD['j_booking']
        del RBD['y_cap']
        del RBD['y_booking']
        del RBD['j_allocation']
        del RBD['y_allocation']
        doc_1 = dict()
        doc_2 = dict()
        doc_1['flight_number'] = i['flight_number']
        doc_1['origin'] = i['origin']
        doc_1['destination'] = i['destination']
        doc_1['od'] = i['od']
        doc_1['dep_date'] = i['dep_date']
        doc_1['snap_date'] = i['snap_date']
        doc_1['day_of_week'] = i['day_of_week']
        doc_1['compartment'] = 'Y'
        doc_1['booked_seat_factor'] = i['y_seat_factor']
        doc_1['capacity'] = i['y_cap']
        doc_1['bookings'] = i['y_booking']
        doc_1['allocation'] = i['y_allocation']
        doc_2['flight_number'] = i['flight_number']
        doc_2['origin'] = i['origin']
        doc_2['destination'] = i['destination']
        doc_2['od'] = i['od']
        doc_2['dep_date'] = i['dep_date']
        doc_2['snap_date'] = i['snap_date']
        doc_2['day_of_week'] = i['day_of_week']
        doc_2['compartment'] = 'J'
        doc_2['booked_seat_factor'] = i['j_seat_factor']
        doc_2['capacity'] = i['j_cap']
        doc_2['bookings'] = i['j_booking']
        doc_2['allocation'] = i['j_allocation']
        doc_1['RBD_values'] = RBD
        doc_2['RBD_values'] = RBD
        doc_1['combine_column'] = doc_1['od'] + 'Y' + doc_1['dep_date']
        doc_2['combine_column'] = doc_2['od'] + 'J' + doc_2['dep_date']

        flight_leg_cur = db.JUP_DB_Flight_Leg.aggregate([
            {
                "$match": {
                    'Flight_num': i['flight_number'],
                    'od': i['od'],
                    'dep_date': i['dep_date'],
                    'snap_date': {"$lte": i['snap_date']}
                }
            },
            # {
            #     "$sort": {"snap_date": -1}
            # },
            # {
            #     "$group": {
            #         "_id": {
            #             'compartment': '$compartment'
            #         },
            #         'docs': {'$first': '$$ROOT'}
            #     }
            # },
            {
                "$project": {
                    'flight_number': '$Flight_num',
                    'dep_date': '$dep_date',
                    'od': '$od',
                    'origin': '$origin',
                    'destination': '$destination',
                    'compartment': '$compartment',
                    'pax': '$pax',
                    'revenue': '$revenue',
                    'rpkm': '$rpkm',
                    'askm': '$askm',
                    'Yield': '$Yield',
                    'rasm': '$rasm',
                    'Seat_Factor': '$Seat_Factor',
                    '_id': 0
                }
            }
        ], allowDiskUse=True)
        for j in flight_leg_cur:
            if j['compartment'] == 'Y':
                doc_1['pax'] = j['pax']
                doc_1['revenue'] = j['revenue']
                doc_1['rpkm'] = j['rpkm']
                doc_1['askm'] = j['askm']
                doc_1['yield'] = j['Yield']
                doc_1['rasm'] = j['rasm']
                doc_1['ticketed_seat_factor'] = j['Seat_Factor']
            elif j['compartment'] == 'J':
                doc_2['pax'] = j['pax']
                doc_2['revenue'] = j['revenue']
                doc_2['rpkm'] = j['rpkm']
                doc_2['askm'] = j['askm']
                doc_2['yield'] = j['Yield']
                doc_2['rasm'] = j['rasm']
                doc_2['ticketed_seat_factor'] = j['Seat_Factor']

        pros_cur = db.JUP_DB_Pros_Flight.aggregate([
            {
                "$match": {
                    'flight_number': str(i['flight_number']),
                    'carrier': Host_Airline_Code,
                    'origin': i['origin'],
                    'destination': i['destination'],
                    'dep_date': i['dep_date'],
                    'snap_date': {"$lte": i['snap_date']}
                }
            },
            # {
            #     "$sort": {"snap_date": -1}
            # },
            # {
            #     "$group": {
            #         "_id": {
            #             'compartment': '$compartment'
            #         },
            #         'docs': {'$first': '$$ROOT'}
            #     }
            # },
            {
                "$project": {
                    'flight_number': '$flight_number',
                    'dep_date': '$dep_date',
                    'origin': '$origin',
                    'destination': '$destination',
                    'compartment': '$compartment',
                    'pax_forecast': '$leg_cabin_re_calc_out',
                    '_id': 0
                }
            }
        ], allowDiskUse=True)

        for j in pros_cur:
            if j['compartment'] == 'Y':
                doc_1['pax_forecast'] = j['pax_forecast']
            elif j['compartment'] == 'J':
                doc_2['pax_forecast'] = j['pax_forecast']

        if t == 1000:
            print "inserting: ", count
            db.JUP_DB_Market_Characteristics_Flights.insert_many(bulk_list)
            bulk_list = []
            bulk_list.append(doc_1)
            bulk_list.append(doc_2)
            t = 1
        else:
            bulk_list.append(doc_1)
            bulk_list.append(doc_2)
            t += 1
        count += 1
    if t > 0:
        print "inserting: ", count
        db.JUP_DB_Market_Characteristics_Flights.insert_many(bulk_list)
    cur.close()


@measure(JUPITER_LOGGER)
def get_last_year_df(snap_date, db):
    # VLYR Logic for Inventory and Pros, Flown Logic for Flight Leg
    last_year_snap = datetime.strftime(datetime.strptime(snap_date, '%Y-%m-%d') - timedelta(days=364), '%Y-%m-%d')
    inv_cur = db.JUP_DB_Inventory_Leg.aggregate([
        {
            '$match': {"sys_snap_date": last_year_snap}
        },
        {
            '$project': {
                '_id': 0,
                'Flight_Number': 1,
                'dep_date': 1,
                'origin': 1,
                'destination': 1,
                'od': 1,
                'j_cap': 1,
                'j_seat_factor': 1,
                'y_seat_factor': 1,
                'j_booking': 1,
                'y_cap': 1,
                'y_booking': 1,
                'j_allocation': 1,
                'y_allocation': 1,
                'day_of_week': 1,
                'sys_snap_date': 1
            }
        }
    ])

    print 'Queried last year inventory'
    y_df = cursor_to_df(inv_cur)
    if len(y_df) == 0:
        y_df = pd.DataFrame(columns=inv_cols)
    y_df.rename(columns={'Flight_Number': 'flight_number', 'sys_snap_date': 'snap_date'}, inplace=True)
    j_df = deepcopy(y_df)
    y_df.drop(['j_cap', 'j_seat_factor', 'j_booking', 'j_allocation'], axis=1, inplace=True)
    y_df.rename(columns={'y_cap': 'capacity', 'y_seat_factor': 'booked_seat_factor', 'y_booking': 'bookings',
                         'y_allocation': 'allocation'}, inplace=True)
    y_df['compartment'] = 'Y'
    j_df.drop(['y_cap', 'y_seat_factor', 'y_booking', 'y_allocation'], axis=1, inplace=True)
    j_df.rename(columns={'j_cap': 'capacity', 'j_seat_factor': 'booked_seat_factor', 'j_booking': 'bookings',
                         'j_allocation': 'allocation'}, inplace=True)
    j_df['compartment'] = 'J'
    inv_df = pd.concat([y_df, j_df])
    inv_df['flight_number'] = inv_df['flight_number'].astype('int')

    # last_year_ods = inv_df['od'].values
    # delta_ods = list(set(last_year_ods) - set(this_year_ods))
    # inv_df.drop(index=inv_df[inv_df['od'].isin(delta_ods)].index, inplace=True)

    inv_df['dep_date_obj'] = pd.to_datetime(inv_df['dep_date'], format='%Y-%m-%d')
    inv_df['dep_date'] = inv_df['dep_date_obj'] + pd.to_timedelta(364, unit='d')
    inv_df.drop(['dep_date_obj', 'snap_date', 'day_of_week'], axis=1, inplace=True)
    inv_df['dep_date'] = inv_df['dep_date'].dt.strftime('%Y-%m-%d')
    inv_df.rename(columns={'capacity': 'capacity_1', 'bookings': 'bookings_1',
                           'booked_seat_factor': 'booked_seat_factor_1', 'allocation': 'allocation_1'}, inplace=True)
    print 'Built last year inventory df'

    dates = [last_year_snap, snap_date]
    start, end = [datetime.strptime(_, "%Y-%m-%d") for _ in dates]
    end = end + timedelta(days=1)
    dates_range = OrderedDict(
        ((start + timedelta(_)).strftime(r"%Y-%m-%d"), None) for _ in xrange((end - start).days)).keys()

    flight_leg_df = pd.DataFrame(columns=['Flight_num',
                                          'od',
                                          'dep_date',
                                          'snap_date',
                                          'origin',
                                          'destination',
                                          'compartment',
                                          'pax',
                                          'revenue',
                                          'rpkm',
                                          'askm',
                                          'Yield',
                                          'rasm',
                                          'Seat_Factor', 'cmp'])

    for snap_date in dates_range:
        flight_leg_cur = db.JUP_DB_Flight_Leg.aggregate([
            {'$match': {'snap_date': snap_date,
                        'dep_date': snap_date
                        }
             },
            {
                '$project': {
                    'Flight_num': 1,
                    'od': 1,
                    'dep_date': 1,
                    'snap_date': 1,
                    'origin': {'$substr': ['$od', 0, 3]},
                    'destination': {'$substr': ['$od', 3, 3]},
                    'compartment': 1,
                    'pax': 1,
                    'revenue': 1,
                    'rpkm': 1,
                    'askm': 1,
                    'Yield': 1,
                    'rasm': 1,
                    'Seat_Factor': 1,
                    '_id': 0,
                    'cmp': {'$cmp': ['$dep_date', '$snap_date']}
                }
            },
            {
                '$match': {'cmp': 0}
            }
        ])

        print 'Queried last year flight leg'
        # flight_leg_df = cursor_to_df(flight_leg_cur)
        temp_leg_df = cursor_to_df(flight_leg_cur)
        # if len(flight_leg_df) == 0:
        #     flight_leg_df = pd.DataFrame(columns=flight_leg_cols)
        #     flight_leg_df['cmp'] = 0
        if len(temp_leg_df) == 0:
            temp_leg_df = pd.DataFrame(columns=flight_leg_cols)
            temp_leg_df['cmp'] = 0
        flight_leg_df = pd.concat([flight_leg_df, temp_leg_df])

    flight_leg_df.rename(columns={'Flight_num': 'flight_number', 'Seat_Factor': 'ticketed_seat_factor',
                                  'Yield': 'yield'}, inplace=True)
    flight_leg_df['flight_number'] = flight_leg_df['flight_number'].astype('int')
    # last_year_ods = flight_leg_df['od'].values
    # delta_ods = list(set(last_year_ods) - set(this_year_ods))
    # flight_leg_df.drop(index=flight_leg_df[flight_leg_df['od'].isin(delta_ods)].index, inplace=True)

    flight_leg_df['dep_date_obj'] = pd.to_datetime(flight_leg_df['dep_date'], format='%Y-%m-%d')
    flight_leg_df['dep_date'] = flight_leg_df['dep_date_obj'] + pd.to_timedelta(364, unit='d')
    flight_leg_df.drop(['dep_date_obj', 'cmp', 'snap_date'], axis=1, inplace=True)
    flight_leg_df['dep_date'] = flight_leg_df['dep_date'].dt.strftime('%Y-%m-%d')
    flight_leg_df.rename(columns={'pax': 'pax_1', 'revenue': 'revenue_1', 'yield': 'yield_1',
                                  'rpkm': 'rpkm_1', 'askm': 'askm_1', 'rasm': 'rasm_1',
                                  'ticketed_seat_factor': 'ticketed_seat_factor_1'}, inplace=True)
    print 'Built last year flight leg df'

    pros_cur = db.JUP_DB_Pros_Flight.aggregate([
        {
            '$match': {'snap_date': last_year_snap, 'carrier': Host_Airline_Code}
        },
        {
            '$project': {
                'flight_number': 1,
                'origin': 1,
                'destination': 1,
                'compartment': 1,
                'dep_date': 1,
                'snap_date': 1,
                'leg_cabin_re_calc_out': 1,
                'od': {'$concat': ['$origin', '$destination']},
                '_id': 0
            }
        }
    ])

    print 'Queried last year pros'
    pros_df = cursor_to_df(pros_cur)
    if len(pros_df) == 0:
        pros_df = pd.DataFrame(columns=pros_cols)
    pros_df.rename(columns={'leg_cabin_re_calc_out': 'pax_forecast'}, inplace=True)
    pros_df['flight_number'] = pros_df['flight_number'].astype('int')

    # last_year_ods = pros_df['od'].values
    # delta_ods = list(set(last_year_ods) - set(this_year_ods))
    # pros_df.drop(index=pros_df[pros_df['od'].isin(delta_ods)].index, inplace=True)

    pros_df['dep_date_obj'] = pd.to_datetime(pros_df['dep_date'], format='%Y-%m-%d')
    pros_df['dep_date'] = pros_df['dep_date_obj'] + pd.to_timedelta(364, unit='d')
    pros_df.drop(['dep_date_obj', 'snap_date'], axis=1, inplace=True)
    pros_df['dep_date'] = pros_df['dep_date'].dt.strftime('%Y-%m-%d')
    pros_df.rename(columns={'pax_forecast': 'pax_forecast_1'}, inplace=True)
    print 'Built last year pros df'

    last_year_df = inv_df.merge(pros_df, on=['flight_number', 'od', 'origin', 'destination',
                                             'compartment', 'dep_date'], how='outer')

    last_year_df = last_year_df.merge(flight_leg_df, on=['flight_number', 'od', 'origin', 'destination',
                                                         'compartment', 'dep_date'], how='outer')
    last_year_df.fillna(0, inplace=True)

    return last_year_df


@measure(JUPITER_LOGGER)
def run_last_year(snap_date, db):
    bulk_list = []
    count = 1
    t = 0
    cur = db.JUP_DB_Market_Characteristics_Flights.find({
        'snap_date': snap_date
    }, no_cursor_timeout=True)
    for i in cur:
        last_year_dep_date = datetime.strftime(datetime.strptime(i['dep_date'], "%Y-%m-%d") -
                                               timedelta(days=364), "%Y-%m-%d")
        last_year_snap_date = datetime.strftime(datetime.strptime(i['snap_date'], '%Y-%m-%d') -
                                                timedelta(days=364), "%Y-%m-%d")
        last_year = db.JUP_DB_Market_Characteristics_Flights.find({
            'flight_number': i['flight_number'],
            'od': i['od'],
            'dep_date': last_year_dep_date,
            'snap_date': last_year_snap_date,
            'compartment': i['compartment']
        })
        if last_year.count() == 0:
            pass
        else:
            for j in last_year:
                last_year_dict = dict()
                last_year_dict['capacity_1'] = j['capacity']
                last_year_dict['bookings_1'] = j['bookings']
                last_year_dict['booked_seat_factor_1'] = j['booked_seat_factor']
                last_year_dict['allocation_1'] = j['allocation']
                last_year_dict['RBD_values_1'] = j['RBD_values']
                try:
                    last_year_dict['pax_1'] = j['pax']
                    last_year_dict['revenue_1'] = j['revenue']
                    last_year_dict['rpkm_1'] = j['rpkm']
                    last_year_dict['askm_1'] = j['askm']
                    last_year_dict['yield_1'] = j['yield']
                    last_year_dict['rasm_1'] = j['rasm']
                    last_year_dict['ticket_seat_factor_1'] = j['ticketed_seat_factor']
                except KeyError:
                    last_year_dict['pax_1'] = 0
                    last_year_dict['revenue_1'] = 0
                    last_year_dict['rpkm_1'] = 0
                    last_year_dict['askm_1'] = 0
                    last_year_dict['yield_1'] = 0
                    last_year_dict['rasm_1'] = 0
                    last_year_dict['ticket_seat_factor_1'] = 0

                try:
                    last_year_dict['pax_forecast_1'] = j['pax_forecast']
                except KeyError:
                    last_year_dict['pax_forecast_1'] = 0

            if t == 20000:
                print "updating: ", count
                db.JUP_DB_Market_Characteristics_Flights.bulk_write(bulk_list)
                bulk_list = []
                bulk_list.append(pymongo.UpdateOne({"_id": i["_id"]}, {"$set": {
                    'capacity_1': last_year_dict['capacity_1'],
                    'bookings_1': last_year_dict['bookings_1'],
                    'booked_seat_factor_1': last_year_dict['booked_seat_factor_1'],
                    'RBD_values_1': last_year_dict['RBD_values_1'],
                    'allocation_1': last_year_dict['allocation_1'],
                    'pax_1': last_year_dict['pax_1'],
                    'revenue_1': last_year_dict['revenue_1'],
                    'rpkm_1': last_year_dict['rpkm_1'],
                    'askm_1': last_year_dict['askm_1'],
                    'yield_1': last_year_dict['yield_1'],
                    'rasm_1': last_year_dict['rasm_1'],
                    'ticket_seat_factor_1': last_year_dict['ticket_seat_factor_1'],
                    'pax_forecast_1': last_year_dict['pax_forecast_1']
                }}))
                t = 1
            else:
                bulk_list.append(pymongo.UpdateOne({"_id": i["_id"]}, {"$set": {
                    'capacity_1': last_year_dict['capacity_1'],
                    'bookings_1': last_year_dict['bookings_1'],
                    'booked_seat_factor_1': last_year_dict['booked_seat_factor_1'],
                    'RBD_values_1': last_year_dict['RBD_values_1'],
                    'allocation_1': last_year_dict['allocation_1'],
                    'pax_1': last_year_dict['pax_1'],
                    'revenue_1': last_year_dict['revenue_1'],
                    'rpkm_1': last_year_dict['rpkm_1'],
                    'askm_1': last_year_dict['askm_1'],
                    'yield_1': last_year_dict['yield_1'],
                    'rasm_1': last_year_dict['rasm_1'],
                    'ticket_seat_factor_1': last_year_dict['ticket_seat_factor_1'],
                    'pax_forecast_1': last_year_dict['pax_forecast_1']
                }}))
                t += 1
        count += 1

    if t > 0:
        print 'updating:', count
        db.JUP_DB_Market_Characteristics_Flights.bulk_write(bulk_list)
    cur.close()


@measure(JUPITER_LOGGER)
def run_market_flights(client):
    db = client[JUPITER_DB]
    st = time.time()
    snap_date = datetime.strftime(datetime.strptime(SYSTEM_DATE, '%Y-%m-%d') - timedelta(days=1), '%Y-%m-%d')
    print 'Removing present snaps in case of re run'
    db.JUP_DB_Market_Characteristics_Flights.remove({'snap_date': snap_date})
    print 'Removed in ', time.time() - st
    print 'running flights this year for snap:', snap_date
    this_year_df = get_this_year_df(snap_date, db)
    print 'ran flights this year for snap:', snap_date
    print 'running flights last year for snap:', snap_date
    last_year_df = get_last_year_df(snap_date, db)
    print 'ran flights last year for snap:', snap_date
    print 'Merging this year and last and year'
    final_df = this_year_df.merge(last_year_df, on=['flight_number', 'od', 'origin', 'destination',
                                                    'compartment', 'dep_date'], how='outer')
    final_df.fillna(0, inplace=True)
    final_df['combine_column'] = final_df['od'] + final_df['compartment'] + final_df['dep_date']
    final_df['snap_date'] = snap_date
    final_df['dep_date_obj'] = pd.to_datetime(final_df['dep_date'])
    final_df['day_of_week'] = final_df['dep_date_obj'].dt.weekday_name
    final_df.drop(['dep_date_obj'], axis=1, inplace=True)
    counter = 0
    while counter < len(final_df):
        inserting_df = final_df.loc[counter: counter + 50000, :]
        counter = counter + 50000
        docs = inserting_df.to_dict('records')
        print 'Inserting'
        db.JUP_DB_Market_Characteristics_Flights.insert(docs)
    print "time taken: ", time.time() - st


@measure(JUPITER_LOGGER)
def market_flights_adhoc(snap_date, client):
    db = client[JUPITER_DB]
    # # snap_date = datetime.strftime(datetime.strptime(SYSTEM_DATE, '%Y-%m-%d') - timedelta(days=1), '%Y-%m-%d')
    # st = time.time()
    # # print 'running flights this year for snap:', snap_date
    # # run_this_year(snap_date)
    # # print 'ran flights this year for snap:', snap_date
    # print 'running flights last year for snap:', snap_date
    # run_last_year(snap_date)
    # print 'ran flights last year for snap:', snap_date
    # print "time taken: ", time.time() - st

    # snap_date = datetime.strftime(datetime.strptime(SYSTEM_DATE, '%Y-%m-%d') - timedelta(days=1), '%Y-%m-%d')
    st = time.time()
    print 'running flights this year for snap:', snap_date
    this_year_df = get_this_year_df(snap_date, db)
    print 'ran flights this year for snap:', snap_date
    print 'running flights last year for snap:', snap_date
    last_year_df = get_last_year_df(snap_date, db)
    print 'ran flights last year for snap:', snap_date
    print 'Merging this year and last and year'
    final_df = this_year_df.merge(last_year_df, on=['flight_number', 'od', 'origin', 'destination',
                                                    'compartment', 'dep_date'], how='outer')
    final_df.fillna(0, inplace=True)
    final_df['combine_column'] = final_df['od'] + final_df['compartment'] + final_df['dep_date']
    final_df['snap_date'] = snap_date
    final_df['dep_date_obj'] = pd.to_datetime(final_df['dep_date'])
    final_df['day_of_week'] = final_df['dep_date_obj'].dt.weekday_name
    final_df.drop(['dep_date_obj'], axis=1, inplace=True)
    counter = 0
    while counter < len(final_df):
        inserting_df = final_df.loc[counter: counter + 50000, :]
        counter = counter + 50000
        docs = inserting_df.to_dict('records')
        print 'Inserting'
        db.JUP_DB_Market_Characteristics_Flights.insert(docs)
    print "time taken: ", time.time() - st


if __name__ == "__main__":
    client = mongo_client()
    run_market_flights(client)
    client.close()
