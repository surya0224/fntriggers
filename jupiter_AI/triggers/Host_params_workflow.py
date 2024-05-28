'''
Author: Nikunj Agarwal
Created with <3
Date: 26th July, 2017
Functionality of code: Returns a DataFrame containing Sale and Flown Pax and Revenue data from Manual Triggers
                       collection for this year and last year for dep_date_range--> extreme_start_date -
                       extreme_end_date. This data frame will be at dep_date level, so that we can split this
                       master data frame into multiple data frames for 10 different dep_date ranges.
'''

from jupiter_AI import mongo_client, JUPITER_DB, SYSTEM_DATE, today, Host_Airline_Hub as hub, JUPITER_LOGGER
#from jupiter_AI.BaseParametersCodes.common import get_ly_val
# from jupiter_AI.triggers.common import get_no_days
import json
import numpy as np
import pandas as pd
from calendar import monthrange
from collections import defaultdict
import datetime
import time
import math

from jupiter_AI.logutils import measure

#db = client[JUPITER_DB]


@measure(JUPITER_LOGGER)
def get_od_capacity(origin, destination, compartment, dep_date_start, dep_date_end, db):
    """
    :return: {'ty': This year capacity value for this market and dep dates between dep_date_start and dep_date_end,
              'ly': Last year capacity value for this market and dep dates between dep_date_start - 1 yr and dep_date_end - 1yr
              }
    """
    host_od_capacity = list(db.JUP_DB_Host_OD_Capacity.aggregate(
        [
            {
                '$match': {
                    'od': origin + destination
                }
            }
            ,
            {
                '$facet': {
                    'ty':
                    # Pipeline
                        [
                            # Stage 1
                            {
                                '$match': {
                                    'dep_date': {'$gte': dep_date_start, '$lte': dep_date_end}
                                }
                            },

                            # Stage 2
                            {
                                '$sort': {
                                    'last_update_date': -1
                                }
                            },

                            # Stage 3
                            {
                                '$group': {
                                    '_id': {
                                        'od': '$od',
                                        'dep_date': '$dep_date'
                                    },
                                    'y_capacity': {'$first': '$y_cap'},
                                    'j_capacity': {'$first': '$j_cap'}
                                }
                            },

                            # Stage 4
                            {
                                '$group': {
                                    '_id': None,
                                    'y_capacity': {'$sum': '$y_capacity'},
                                    'j_capacity': {'$sum': '$j_capacity'}
                                }
                            },

                            # Stage 5
                            {
                                '$project': {
                                    'y_capacity': '$y_capacity',
                                    'j_capacity': '$j_capacity',
                                    'capacity': {'$sum': ['$y_capacity', '$j_capacity']}
                                }
                            },

                        ],
                    'ly':
                    # Pipeline
                        [
                            # Stage 1
                            {
                                '$match': {
                                    'dep_date': {'$gte': datetime.datetime.strftime(datetime.datetime.strptime(dep_date_start, "%Y-%m-%d") - datetime.timedelta(days = 365), "%Y-%m-%d"), '$lte': datetime.datetime.strftime(datetime.datetime.strptime(dep_date_end, "%Y-%m-%d") - datetime.timedelta(days = 365), "%Y-%m-%d")}
                                }
                            },

                            # Stage 2
                            {
                                '$sort': {
                                    'last_update_date': -1
                                }
                            },

                            # Stage 3
                            {
                                '$group': {
                                    '_id': {
                                        'od': '$od',
                                        'dep_date': '$dep_date'
                                    },
                                    'y_capacity': {'$first': '$y_cap'},
                                    'j_capacity': {'$first': '$j_cap'}
                                }
                            },

                            # Stage 4
                            {
                                '$group': {
                                    '_id': None,
                                    'y_capacity': {'$sum': '$y_capacity'},
                                    'j_capacity': {'$sum': '$j_capacity'}
                                }
                            },

                            # Stage 5
                            {
                                '$project': {
                                    'y_capacity': '$y_capacity',
                                    'j_capacity': '$j_capacity',
                                    'capacity': {'$sum': ['$y_capacity', '$j_capacity']}
                                }
                            },

                        ]
                }
            }

        ]
        # Created with Studio 3T, the IDE for MongoDB - https:#studio3t.com/
    ))
    response = dict(
        ty='NA',
        ly='NA'
    )

    if len(host_od_capacity) == 1:
        ty = host_od_capacity[0]['ty']
        ly = host_od_capacity[0]['ly']
        if len(ty) == 1:
            if compartment:
                if compartment == 'Y':
                    response['ty'] = ty[0]['y_capacity']
                elif compartment == 'J':
                    response['ty'] = ty[0]['j_capacity']
            else:
                response['ty'] = ty[0]['capacity']

        if len(ly) == 1:
            if compartment:
                if compartment == 'Y':
                    response['ly'] = ly[0]['y_capacity']
                elif compartment == 'J':
                    response['ly'] = ly[0]['j_capacity']
            else:
                response['ly'] = ly[0]['capacity']

    return response


@measure(JUPITER_LOGGER)
def get_bookings_df(origin, destination, compartment, dep_date_start, dep_date_end, db):

    bookings_cursor = db.JUP_DB_Inventory_Leg.aggregate([
        {
            '$match': {
                'od': origin + destination,
                'dep_date': {'$gte': dep_date_start, '$lte': dep_date_end}
            }
        },
        {
            '$sort': {
                'last_update_date': -1
            }
        },
        {
            '$group': {
                '_id': {
                    'dep_date': '$dep_date',
                    'flight': '$Flight_Number',
                    'od': '$od'
                },
                'j_booking': {'$first': '$j_booking'},
                'y_booking': {'$first': '$y_booking'},
                'total_booking': {'$first': '$total_booking'},
                'j_cap': {'$first': '$j_cap'},
                'y_cap': {'$first': '$y_cap'},
                'total_cap': {'$first': '$total_cap'}
            }
        },
        {
            '$project':{
                '_id' : 0,
                'flight_no' : '$_id.flight',
                'dep_date' : '$_id.dep_date',
                'y_booking' : '$y_booking',
                'j_booking' : '$j_booking',
                'total_booking' : '$total_booking',
                'y_capacity' : '$y_cap',
                'j_capacity' : '$j_cap',
                'total_capacity' : '$total_cap'
            }
        }

    ])
    bookings_data = list(bookings_cursor)
    bookings_df = pd.DataFrame(bookings_data)
    print bookings_df

    return bookings_df


@measure(JUPITER_LOGGER)
def get_performance_data(pos, origin, destination, compartment, extreme_start_date, extreme_end_date, db):
    """
    :return:

    """
    book_date_end = SYSTEM_DATE
    book_date_start = str(today.year) + '-01-01'
    performance_data_cursor = db.JUP_DB_Manual_Triggers_Module.aggregate([
        {
            '$match':
                {
                    'pos.City' : pos,
                    'origin.City' : origin,
                    'destination.City' : destination,
                    'compartment.compartment' : compartment,
                    'dep_date': {'$gte': extreme_start_date, '$lte': extreme_end_date},
                    'trx_date': {'$lte': book_date_end, '$gte': book_date_start}
                }
        },
        {
            '$group':
                {
                    '_id' :
                        {
                            'dep_date' : '$dep_date'
                        },
                    'sales_pax':{'$sum' : '$sale_pax.value'},
                    'flown_pax':{'$sum': '$flown_pax.value'},
                    'sales_revenue':{'$sum': '$sale_revenue.value'},
                    'flown_revenue':{'$sum': '$flown_revenue.value'},
                    'sales_pax_l':{'$sum': '$sale_pax.value_1'},
                    'flown_pax_l':{'$sum': '$flown_pax.value_1'},
                    'sales_revenue_l':{'$sum': '$sale_revenue.value_1'},
                    'flown_revenue_l': {'$sum': '$flown_revenue.value_1'},
                    'pax_target': {'$addToSet' :'$target.pax'},
                    'revenue_target': {'$addToSet' :'$target.revenue'},
                    'avg_fare_target': {'$addToSet':'$target.avgFare'},
                    'pax_forecast': {'$addToSet':'$forecast.pax'},
                    'revenue_forecast': {'$addToSet': '$forecast.revenue'},
                    'avg_fare_forecast': {'$addToSet' :'$forecast.avgFare'}
                    # 'doc':{'$first': '$$ROOT'}
                }
        },
        {
            '$project' :
                {
                    '_id': 0,
                    'dep_date' : '$_id.dep_date',
                    'sales_pax': '$sales_pax',
                    'flown_pax': '$flown_pax',
                    'sales_revenue': '$sales_revenue',
                    'flown_revenue': '$flown_revenue',
                    'sales_pax_l': '$sales_pax_l',
                    'flown_pax_l': '$flown_pax_l',
                    'sales_revenue_l': '$sales_revenue_l',
                    'flown_revenue_l': '$flown_revenue_l',
                    'pax_target':
                        {'$cond':
                                {
                                    'if': {'$gt': [{'$size': '$pax_target'}, 0]},
                                    'then': {'$arrayElemAt': ['$pax_target', 0]},
                                    'else': None
                                }
                        },
                    'revenue_target':
                        {'$cond':
                                {
                                    'if': {'$gt': [{'$size': '$revenue_target'}, 0]},
                                    'then': {'$arrayElemAt': ['$revenue_target', 0]},
                                    'else': None
                                }
                        },
                    'avg_fare_target':
                        {'$cond':
                                {
                                    'if': {'$gt': [{'$size': '$avg_fare_target'}, 0]},
                                    'then': {'$arrayElemAt': ['$avg_fare_target', 0]},
                                    'else': None
                                }
                        },
                    'pax_forecast':
                        {'$cond':
                                {
                                    'if': {'$gt': [{'$size': '$pax_forecast'}, 0]},
                                    'then': {'$arrayElemAt': ['$pax_forecast', 0]},
                                    'else': None
                                }
                        },
                    'revenue_forecast':
                        {'$cond':
                                {
                                    'if': {'$gt': [{'$size': '$revenue_forecast'}, 0]},
                                    'then': {'$arrayElemAt': ['$revenue_forecast', 0]},
                                    'else': None
                                }
                        },
                    'avg_fare_forecast':
                        {'$cond':
                                {
                                    'if': {'$gt': [{'$size': '$avg_fare_forecast'}, 0]},
                                    'then': {'$arrayElemAt': ['$avg_fare_forecast', 0]},
                                    'else': None
                                }
                        }
                }
        }
    ])
    performance_data = list(performance_data_cursor)
    performance_df = pd.DataFrame(performance_data)
    performance_df.sort_values('dep_date', ascending=True, inplace=True)
    performance_df['dep_date_object'] = pd.to_datetime(performance_df['dep_date'], yearfirst=True)
    performance_df['dep_month'] = performance_df['dep_date_object'].apply(lambda row: row.month)

    start_date = performance_df.iloc[0]['dep_date']
    start_date_obj = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    start_date_month = start_date_obj.month

    end_date = performance_df.iloc[-1]['dep_date']
    end_date_obj = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    end_date_month = end_date_obj.month

    for month in range(start_date_month, end_date_month+1):
        # Filling missing values of forecast pax and target pax of a particular month with the same value of forecast and target pax found in any dep_date of same month
        # print "pax_target series for month = ",month," = " , performance_df.loc[(performance_df['pax_target'] > 0) & (performance_df['dep_month'] == month), 'pax_target'].values
        pax_target_series = performance_df.loc[(performance_df['pax_target'] > 0) & (performance_df['dep_month'] == month), 'pax_target'].values
        if len(pax_target_series) > 0:
            target_pax_for_this_month = pax_target_series[0]
            performance_df.loc[(performance_df['pax_target'].isnull()) & (performance_df['dep_month'] == month), 'pax_target'] = target_pax_for_this_month
        pax_forecast_series = performance_df.loc[(performance_df['pax_forecast'] > 0) & (performance_df['dep_month'] == month), 'pax_forecast']
        if len(pax_forecast_series) > 0:
            forecast_pax_for_this_month = pax_forecast_series.values[0]
            performance_df.loc[(performance_df['pax_forecast'].isnull()) & (performance_df['dep_month'] == month), 'pax_forecast'] = forecast_pax_for_this_month

        # Filling missing values of forecast revenue and target revenue of a particular month with the same value of forecast and target revenue found in any dep_date of same month
        revenue_target_series = performance_df.loc[(performance_df['revenue_target'] > 0) & (performance_df['dep_month'] == month), 'revenue_target']
        if len(revenue_target_series) > 0:
            target_revenue_for_this_month = revenue_target_series.values[0]
            performance_df.loc[(performance_df['revenue_target'].isnull()) & (performance_df['dep_month'] == month), 'revenue_target'] = target_revenue_for_this_month
        revenue_forecast_series = performance_df.loc[(performance_df['revenue_forecast'] > 0) & (performance_df['dep_month'] == month), 'revenue_forecast']
        if len(revenue_forecast_series) > 0:
            forecast_revenue_for_this_month = revenue_forecast_series.values[0]
            performance_df.loc[(performance_df['revenue_forecast'].isnull()) & (performance_df['dep_month'] == month), 'revenue_forecast'] = forecast_revenue_for_this_month

        # Filling missing values of forecast avg_fare and target avg_fare of a particular month with the same value of forecast and target avg_fare found in any dep_date of same month
        avg_fare_target_series = performance_df.loc[(performance_df['avg_fare_target'] > 0) & (performance_df['dep_month'] == month), 'avg_fare_target']
        if len(avg_fare_target_series) > 0:
            target_avg_fare_for_this_month = avg_fare_target_series.values[0]
            performance_df.loc[(performance_df['avg_fare_target'].isnull()) & (performance_df['dep_month'] == month), 'avg_fare_target'] = target_avg_fare_for_this_month
        avg_fare_forecast_series = performance_df.loc[(performance_df['avg_fare_forecast'] > 0) & (performance_df['dep_month'] == month), 'avg_fare_forecast']
        if len(avg_fare_forecast_series) > 0:
            forecast_avg_fare_for_this_month = avg_fare_forecast_series.values[0]
            performance_df.loc[(performance_df['avg_fare_forecast'].isnull()) & (performance_df['dep_month'] == month), 'avg_fare_forecast'] = forecast_avg_fare_for_this_month

    performance_df['pax_target'].fillna(0, inplace = True)
    performance_df['revenue_target'].fillna(0, inplace=True)
    performance_df['revenue_forecast'].fillna(0, inplace=True)
    performance_df['pax_forecast'].fillna(0, inplace=True)
    print performance_df
    return performance_df


if __name__=='__main__':
    st = time.time()
    client = mongo_client()
    db=client[JUPITER_DB]
    # print get_od_capacity('RUH','DXB','Y','2018-03-01','2018-04-01')
    # print "Time taken to get host_od_capacity for this market: ", time.time() - st
    # st = time.time()
    performance_data = get_performance_data(pos='AAN',origin='DXB',destination='UET',compartment='Y',extreme_start_date='2017-04-27',extreme_end_date='2017-05-04')
    # performance_df = pd.DataFrame(performance_data)
    # print performance_df
    print "time taken to get performance data = ", time.time() - st
    client.close()
    # st = time.time()
    # get_bookings_df('BAH', 'DXB', 'Y', '2017-04-30', '2017-05-03')
    # print "Time taken to get inventory data = ", time.time() - st