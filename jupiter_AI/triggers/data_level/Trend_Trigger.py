"""
Author: Nikunj Agarwal
Created with <3
Start Date of writing code: 2017-10-14
Code functionality:
    Raises triggers based on comparison of trend of this year vs last year.
    if diff in pax/revenue/avg_fare/market_share of this year and last year(same date/month) is
    beyond certain threshold for 4/3 continuous days/months, raise a trigger.
Modifications log:
    1. Author: Nikunj Agarwal
       Exact modification made or some logic changed: Added Market Share Trend Trigger
        Logic for Market Share Trend:
            Market Share Trend is calculated only for FZ. Triggers are raised for 3 set of departure dates:
                1. SYSTEM_DATE - End of month (today - 31)
                2. Next month (1 - 30)
                3. Next to next month (1 - 31)
            Actually, dep dates are not of any use in Market Share Trend but are used to follow the framework
            of triggers and to show performance at trigger level. So dep dates are used in this way:
            The month of dep_dates is used to get data for market_share_trend. Market share and market_share_1
            for FZ should be $gt 0 for this month. Latest 4 snaps of market_share is used to evaluate market_share_trend
       Date of modification: 27th Jan 2018
    2. Author:
       Exact modification made or some logic changed:
       Date of modification:
    3. Author:
       Exact modification made or some logic changed:
       Date of modification:


"""
from jupiter_AI import mongo_client
from jupiter_AI.network_level_params import JUPITER_DB, Host_Airline_Code, Host_Airline_Hub
import datetime
import pandas as pd
import numpy as np
from jupiter_AI.network_level_params import today, SYSTEM_DATE, JUPITER_LOGGER
from jupiter_AI.triggers.common import get_start_end_dates, get_trigger_config_dates
from jupiter_AI.triggers.common import generate_trigger_id
from jupiter_AI.triggers.analytics_functions import sending_message
import time
from jupiter_AI.triggers.listener_data_level_trigger import analyze
from jupiter_AI.logutils import measure

#db = client[JUPITER_DB]

TRIGGER_TYPE_PAX = 'pax_trend'
TRIGGER_TYPE_REVENUE = 'revenue_trend'
TRIGGER_TYPE_AVG_FARE = 'avg_fare_trend'
TRIGGER_TYPE_MARKET_SHARE = 'market_share_trend'
THRESHOLD_TYPE = 'Percentage'
LOWER_THRESHOLD = -20
UPPER_THRESHOLD = 20
TRIGGER_PRIORITY = 5
MIN_INTERVAL = 24
counter_revenue_triggers = 0
counter_pax_triggers = 0
counter_avg_fare_triggers = 0
counter_market_share_trend_triggers = 0


# --------------------- PRE ANALYSIS ------------------------- ############


@measure(JUPITER_LOGGER)
def get_pax_data(markets, dep_date_start, db, dep_date_end):
    """
    :return: A DataFrame at Market, Dep_date and transaction date level along with
             revenue and revenue_last_yr values
    """
    pax_cursor = db.JUP_DB_Manual_Triggers_Module.aggregate([
        {
            '$match':
                {
                    'market_combined': {'$in': markets},
                    'dep_date': {'$gte': dep_date_start, '$lte': dep_date_end},
                    # 'sale_pax.value': {'$gt': 0},
                    # 'sale_pax.value_1': {'$gt': 0}
                }
        },
        {
            '$project':
                {
                    '_id': 0,
                    'market_combined': '$market_combined',
                    'dep_date': '$dep_date',
                    'trx_date': '$trx_date',
                    'pax': '$sale_pax.value',
                    'pax_l': '$sale_pax.value_1',
                }
        },
        {
            '$group': {
                '_id': {'market_combined': '$market_combined',
                        'dep_date': '$dep_date',
                        'trx_date': '$trx_date'
                        },
                'pax': {'$sum': '$pax'},
                'pax_l': {'$sum': '$pax_l'}
            }
        },
        {
            '$project': {
                'market_combined': '$_id.market_combined',
                'trx_date': '$_id.trx_date',
                'dep_date': '$_id.dep_date',
                'pax': 1,
                'pax_l': 1,
                '_id': 0
            }
        },
        {
            '$match': {'pax': {'$gt': 0}, 'pax_l': {'$gt': 0}}
        },
        {
            '$sort':
                {
                    'trx_date': -1
                }
        },
        {
            '$group':
                {
                    '_id':
                        {'market_combined': '$market_combined',
                         'dep_date': '$dep_date'
                         },
                    'docs':
                        {
                            '$push':
                                {
                                    'trx_date': '$trx_date',
                                    'pax': '$pax',
                                    'pax_l': '$pax_l'
                                }
                        }
                }
        },
        {
            '$project':
                {
                    '_id': 0,
                    'market_combined': '$_id.market_combined',
                    'dep_date': '$_id.dep_date',
                    'docs': {'$slice': ['$docs', 4]}
                }
        },
        {
            '$match':
                {
                    'docs': {'$size': 4}
                }
        },
        {
            '$unwind': '$docs'
        },
        {
            '$project':
                {
                    'market': '$market_combined',
                    'dep_date': '$dep_date',
                    'trx_date': '$docs.trx_date',
                    'pax': '$docs.pax',
                    'pax_l': '$docs.pax_l'
                }
        }
    ], allowDiskUse=True)
    pax_data = list(pax_cursor)
    pax_df = pd.DataFrame(pax_data)
    return pax_df


@measure(JUPITER_LOGGER)
def get_revenue_data(markets, dep_date_start, db, dep_date_end):
    """
        :return: A DataFrame at Market, Dep_date and transaction date level along with
                 pax and pax_last_yr values
        """
    revenue_cursor = db.JUP_DB_Manual_Triggers_Module.aggregate([
        {
            '$match':
                {
                    'market_combined': {'$in': markets},
                    'dep_date': {'$gte': dep_date_start, '$lte': dep_date_end},
                    # 'sale_revenue.value': {'$gt': 0},
                    # 'sale_revenue.value_1': {'$gt': 0}
                }
        },
        {
            '$project':
                {
                    '_id': 0,
                    'market_combined': '$market_combined',
                    'dep_date': '$dep_date',
                    'trx_date': '$trx_date',
                    'revenue': '$sale_revenue.value',
                    'revenue_l': '$sale_revenue.value_1',
                }
        },
        {
            '$group': {
                '_id': {'market_combined': '$market_combined',
                        'dep_date': '$dep_date',
                        'trx_date': '$trx_date'
                        },
                'revenue': {'$sum': '$revenue'},
                'revenue_l': {'$sum': '$revenue_l'}
            }
        },
        {
            '$project': {
                'market_combined': '$_id.market_combined',
                'trx_date': '$_id.trx_date',
                'dep_date': '$_id.dep_date',
                'revenue': 1,
                'revenue_l': 1,
                '_id': 0
            }
        },
        {
            '$match': {'revenue': {'$gt': 0}, 'revenue_l': {'$gt': 0}}
        },
        {
            '$sort':
                {
                    'trx_date': -1
                }
        },
        {
            '$group':
                {
                    '_id':
                        {'market_combined': '$market_combined',
                         'dep_date': '$dep_date'
                         },
                    'docs':
                        {
                            '$push':
                                {
                                    'trx_date': '$trx_date',
                                    'revenue': '$revenue',
                                    'revenue_l': '$revenue_l'
                                }
                        }
                }
        },
        {
            '$project':
                {
                    '_id': 0,
                    'market_combined': '$_id.market_combined',
                    'dep_date': '$_id.dep_date',
                    'docs': {'$slice': ['$docs', 4]}
                }
        },
        {
            '$match':
                {
                    'docs': {'$size': 4}
                }
        },
        {
            '$unwind': '$docs'
        },
        {
            '$project':
                {
                    'market': '$market_combined',
                    'dep_date': '$dep_date',
                    'trx_date': '$docs.trx_date',
                    'revenue': '$docs.revenue',
                    'revenue_l': '$docs.revenue_l'
                }
        }
    ], allowDiskUse=True)
    revenue_data = list(revenue_cursor)
    revenue_df = pd.DataFrame(revenue_data)
    return revenue_df


@measure(JUPITER_LOGGER)
def get_avg_fare_data(markets, dep_date_start, db, dep_date_end):
    """
    :return: A DataFrame at Market, Dep_date and transaction date level along with
             avg_fare and avg_fare_last_yr values
    """
    pax_cursor = db.JUP_DB_Manual_Triggers_Module.aggregate([
        {
            '$match':
                {
                    'market_combined': {'$in': markets},
                    'dep_date': {'$gte': dep_date_start, '$lte': dep_date_end},
                    # 'sale_pax.value': {'$gt': 0},
                    # 'sale_pax.value_1': {'$gt': 0},
                    # 'sale_revenue.value': {'$gt': 0},
                    # 'sale_revenue.value_1': {'$gt': 0}
                }
        },
        {
            '$group': {
                '_id': {'market_combined': '$market_combined',
                        'dep_date': '$dep_date',
                        'trx_date': '$trx_date'
                        },
                'pax': {'$sum': '$sale_pax.value'},
                'pax_l': {'$sum': '$sale_pax.value_1'},
                'revenue': {'$sum': '$sale_revenue.value'},
                'revenue_l': {'$sum': '$sale_revenue.value_1'}
            }
        },
        {
            '$project': {
                'market_combined': '$_id.market_combined',
                'trx_date': '$_id.trx_date',
                'dep_date': '$_id.dep_date',
                'pax': 1,
                'pax_l': 1,
                'revenue': 1,
                'revenue_l': 1,
                '_id': 0
            }
        },
        {
            '$match': {'pax': {'$gt': 0}, 'pax_l': {'$gt': 0},
                       'revenue': {'$gt': 0}, 'revenue_l': {'$gt': 0}}
        },
        {
            '$project':
                {
                    '_id': 0,
                    'market_combined': '$market_combined',
                    'dep_date': '$dep_date',
                    'trx_date': '$trx_date',
                    'avg_fare': {'$divide': ['$revenue', '$pax']},
                    'avg_fare_l': {'$divide': ['$revenue_l', '$pax_l']},
                }
        },
        {
            '$sort':
                {
                    'trx_date': -1
                }
        },
        {
            '$group':
                {
                    '_id':
                        {'market_combined': '$market_combined',
                         'dep_date': '$dep_date'
                         },
                    'docs':
                        {
                            '$push':
                                {
                                    'trx_date': '$trx_date',
                                    'avg_fare': '$avg_fare',
                                    'avg_fare_l': '$avg_fare_l'
                                }
                        }
                }
        },
        {
            '$project':
                {
                    '_id': 0,
                    'market_combined': '$_id.market_combined',
                    'dep_date': '$_id.dep_date',
                    'docs': {'$slice': ['$docs', 4]}
                }
        },
        {
            '$match':
                {
                    'docs': {'$size': 4}
                }
        },
        {
            '$unwind': '$docs'
        },
        {
            '$project':
                {
                    'market': '$market_combined',
                    'dep_date': '$dep_date',
                    'trx_date': '$docs.trx_date',
                    'avg_fare': '$docs.avg_fare',
                    'avg_fare_l': '$docs.avg_fare_l'
                }
        }
    ], allowDiskUse=True)
    pax_data = list(pax_cursor)
    pax_df = pd.DataFrame(pax_data)
    return pax_df


@measure(JUPITER_LOGGER)
def get_market_share_data(markets, month, db, year):
    month_year = str(year) + '{0:02d}'.format(month)
    market_share_cursor = db.JUP_DB_Pos_OD_Compartment_new.aggregate([
        {
            '$match':
                {
                    'market': {'$in': markets}
                }
        },
        {
            '$unwind': '$top_5_comp_data'
        },
        {
            '$match':
                {
                    'top_5_comp_data.airline': Host_Airline_Code,
                    'month_year': {'$lte': month_year},
                    'top_5_comp_data.market_share': {'$gt': 0},
                    'top_5_comp_data.market_share_1': {'$gt': 0}
                }
        },
        {
            '$sort':
                {
                    'month_year': -1
                }
        },
        {
            '$group':
                {
                    '_id':
                        {
                            'pos': '$pos',
                            'od': '$od',
                            'compartment': '$compartment'
                        },
                    'docs': {
                        '$push': {'year': '$year', 'month': '$month', 'month_year': '$month_year',
                                  'market_share': '$top_5_comp_data.market_share',
                                  'market_share_1': '$top_5_comp_data.market_share_1'}}
                }
        },
        {
            '$project':
                {
                    'pos': '$_id.pos',
                    'od': '$_id.od',
                    'compartment': '$_id.compartment',
                    'docs': {'$slice': ['$docs', 4]}
                }
        },
        {
            '$match':
                {
                    'docs.month_year': month_year,
                    'docs': {'$size': 4}
                }
        },
        {
            '$unwind': '$docs'
        },
        {
            '$project':
                {
                    '_id': 0,
                    'pos': '$pos',
                    'od': '$od',
                    'compartment': '$compartment',
                    'market_share': '$docs.market_share',
                    'year': '$docs.year',
                    'month': '$docs.month',
                    'month_year': '$docs.month_year',
                    'market_share_1': '$docs.market_share_1'
                }
        }
    ])
    market_share_df = pd.DataFrame(list(market_share_cursor))
    if len(market_share_df) > 0:
        market_share_df['market'] = market_share_df['pos'] + market_share_df['od'] + market_share_df['compartment']
    return market_share_df


@measure(JUPITER_LOGGER)
def generate_pax_trend_triggers(markets, dep_date_start, dep_date_end, db):
    global counter_pax_triggers
    pax_trend_triggers = []
    pax_df = get_pax_data(markets=markets, dep_date_start=dep_date_start, dep_date_end=dep_date_end, db=db)
    print pax_df
    if len(pax_df) > 0:
        pax_df['pax_change'] = pax_df['pax'] - pax_df['pax_l']
        pax_df['pax_change_perc'] = pax_df['pax_change'] * 100 / pax_df['pax_l']
        grouped_by_market_and_dep_date_df = pax_df.groupby(by=['market', 'dep_date'])
        for market, market_pax_df in grouped_by_market_and_dep_date_df:
            # eg. market = ('CGPCGPDXBY', '2017-09-01')
            if (market_pax_df['pax_change_perc'] < LOWER_THRESHOLD).all():
                ID = create_pax_trigger(market=market[0],
                                        dep_date=market[1],
                                        trx_dates=market_pax_df['trx_date'],
                                        pax_ty=market_pax_df['pax'],
                                        pax_ly=market_pax_df['pax_l'],
                                        db=db,
                                        pax_change_perc=market_pax_df['pax_change_perc']
                                        )
                pax_trend_triggers.append(ID)
                counter_pax_triggers += 1
            elif (market_pax_df['pax_change_perc'] > UPPER_THRESHOLD).all():
                ID = create_pax_trigger(market=market[0],
                                        dep_date=market[1],
                                        trx_dates=market_pax_df['trx_date'],
                                        pax_ty=market_pax_df['pax'],
                                        db=db,
                                        pax_ly=market_pax_df['pax_l'],
                                        pax_change_perc=market_pax_df['pax_change_perc']
                                        )
                pax_trend_triggers.append(ID)
                counter_pax_triggers += 1
    else:
        print "No market has pax_ty > 0 and pax_ly > 0 from %s to %s and all transaction dates." % (
        dep_date_start, dep_date_end)

    return pax_trend_triggers


@measure(JUPITER_LOGGER)
def generate_revenue_trend_triggers(markets, dep_date_start, db, dep_date_end):
    global counter_revenue_triggers
    revenue_trend_triggers = []
    revenue_df = get_revenue_data(markets=markets, dep_date_start=dep_date_start, db=db, dep_date_end=dep_date_end)
    print revenue_df
    if len(revenue_df) > 0:
        revenue_df['revenue_change'] = revenue_df['revenue'] - revenue_df['revenue_l']
        revenue_df['revenue_change_perc'] = revenue_df['revenue_change'] * 100 / revenue_df['revenue_l']
        grouped_by_market_and_dep_date_df = revenue_df.groupby(by=['market', 'dep_date'])
        for market, market_revenue_df in grouped_by_market_and_dep_date_df:
            # eg. market = ('CGPCGPDXBY', '2017-09-01')
            if (market_revenue_df['revenue_change_perc'] < LOWER_THRESHOLD).all():
                ID = create_revenue_trigger(market=market[0],
                                            dep_date=market[1],
                                            trx_dates=market_revenue_df['trx_date'],
                                            revenue_ty=market_revenue_df['revenue'],
                                            revenue_ly=market_revenue_df['revenue_l'],
                                            db=db,
                                            revenue_change_perc=market_revenue_df['revenue_change_perc']
                                            )
                revenue_trend_triggers.append(ID)
                counter_revenue_triggers += 1
            elif (market_revenue_df['revenue_change_perc'] > UPPER_THRESHOLD).all():
                ID = create_revenue_trigger(market=market[0],
                                            dep_date=market[1],
                                            trx_dates=market_revenue_df['trx_date'],
                                            revenue_ty=market_revenue_df['revenue'],
                                            revenue_ly=market_revenue_df['revenue_l'],
                                            db=db,
                                            revenue_change_perc=market_revenue_df['revenue_change_perc']
                                            )
                revenue_trend_triggers.append(ID)
                counter_revenue_triggers += 1
    else:
        print "No market has revenue_ty > 0 and revenue_ly > 0 from %s to %s and all transaction dates." % (
            dep_date_start, dep_date_end)

    return revenue_trend_triggers


@measure(JUPITER_LOGGER)
def generate_avg_fare_trend_triggers(markets, dep_date_start, db, dep_date_end):
    global counter_avg_fare_triggers
    avg_fare_trend_triggers = []
    avg_fare_df = get_avg_fare_data(markets=markets, dep_date_start=dep_date_start, db=db, dep_date_end=dep_date_end)
    print avg_fare_df
    if len(avg_fare_df) > 0:
        avg_fare_df['avg_fare_change'] = avg_fare_df['avg_fare'] - avg_fare_df['avg_fare_l']
        avg_fare_df['avg_fare_change_perc'] = avg_fare_df['avg_fare_change'] * 100 / avg_fare_df['avg_fare_l']
        grouped_by_market_and_dep_date_df = avg_fare_df.groupby(by=['market', 'dep_date'])
        for market, market_avg_fare_df in grouped_by_market_and_dep_date_df:
            # eg. market = ('CGPCGPDXBY', '2017-09-01')
            if (market_avg_fare_df['avg_fare_change_perc'] < LOWER_THRESHOLD).all():
                ID = create_avg_fare_trigger(market=market[0],
                                             dep_date=market[1],
                                             trx_dates=market_avg_fare_df['trx_date'],
                                             avg_fare_ty=market_avg_fare_df['avg_fare'],
                                             avg_fare_ly=market_avg_fare_df['avg_fare_l'],
                                             db=db,
                                             avg_fare_change_perc=market_avg_fare_df['avg_fare_change_perc']
                                             )
                avg_fare_trend_triggers.append(ID)
                counter_avg_fare_triggers += 1
            elif (market_avg_fare_df['avg_fare_change_perc'] > UPPER_THRESHOLD).all():
                ID = create_avg_fare_trigger(market=market[0],
                                             dep_date=market[1],
                                             trx_dates=market_avg_fare_df['trx_date'],
                                             avg_fare_ty=market_avg_fare_df['avg_fare'],
                                             avg_fare_ly=market_avg_fare_df['avg_fare_l'],
                                             db=db,
                                             avg_fare_change_perc=market_avg_fare_df['avg_fare_change_perc']
                                             )
                avg_fare_trend_triggers.append(ID)
                counter_avg_fare_triggers += 1
    else:
        print "No market has avg_fare_ty > 0 and avg_fare_ly > 0 from %s to %s and all transaction dates." % (
            dep_date_start, dep_date_end)

    return avg_fare_trend_triggers


@measure(JUPITER_LOGGER)
def generate_market_share_trend_triggers(markets, dep_date_start, dep_date_end, month, year, db):
    global counter_market_share_trend_triggers
    market_share_trend_triggers = []
    market_share_df = get_market_share_data(markets=markets, month=month, db=db, year=year)
    print market_share_df
    if len(market_share_df) > 0:
        market_share_df['market_share_change'] = market_share_df['market_share'] - market_share_df['market_share_1']
        market_share_df['market_share_change_percent'] = market_share_df['market_share_change'] * 100.0 / \
                                                         market_share_df['market_share_1']
        grouped_by_markets_df = market_share_df.groupby(by=['market'])
        for market, market_ms_df in grouped_by_markets_df:
            if (market_ms_df['market_share_change_percent'] < LOWER_THRESHOLD).all():
                id = create_market_share_trend_trigger(market=market,
                                                       dep_date_start=dep_date_start,
                                                       dep_date_end=dep_date_end,
                                                       months=list(market_ms_df['month_year']),
                                                       ms_ty=list(market_ms_df['market_share']),
                                                       ms_ly=list(market_ms_df['market_share_1']),
                                                       ms_change_perc=list(market_ms_df['market_share_change_percent']),
                                                       db=db
                                                       )
                market_share_trend_triggers.append(id)
                counter_market_share_trend_triggers += 1
            elif (market_ms_df['market_share_change_percent'] > UPPER_THRESHOLD).all():
                id = create_market_share_trend_trigger(market=market,
                                                       dep_date_start=dep_date_start,
                                                       dep_date_end=dep_date_end,
                                                       months=list(market_ms_df['month_year']),
                                                       ms_ty=list(market_ms_df['market_share']),
                                                       ms_ly=list(market_ms_df['market_share_1']),
                                                       ms_change_perc=list(market_ms_df['market_share_change_percent']),
                                                       db=db
                                                       )
                market_share_trend_triggers.append(id)
                counter_market_share_trend_triggers += 1
    else:
        print "No market has market_share_ty > 0 and market_share_ly > 0 for month %s and remaining two months." % month

    return market_share_trend_triggers


# -------------------- ANALYSIS OF TRIGGER ----------------------- ##########


@measure(JUPITER_LOGGER)
def create_pax_trigger(market, dep_date, trx_dates, pax_ty, pax_ly, db, pax_change_perc):
    """
    Function to insert the trigger and its details to the database.
    the trigger is stored in JUP_DB_Triggering_Event collection
    Return: _id of the document stored in JUP_DB_Triggering_Events
    """
    dates = ''
    for date in trx_dates:
        dates += datetime.datetime.strptime(date.encode('UTF-8'), '%Y-%m-%d').strftime('%d-%m-%Y')
        dates += ', '
    dates = dates[:-2]
    trigger_date = datetime.datetime.strftime(today, '%Y-%m-%d')
    trigger_time = datetime.datetime.strftime(today, '%H:%M')
    triggering_data = {'market': market, 'dep_date_start': dep_date, 'dep_date_end': dep_date, 'trx_dates': dates}
    old_doc_data = {'pos': market[:3],
                    'origin': market[3:6],
                    'destination': market[6:9],
                    'compartment': market[9:],
                    'pax_ly': [int(pax) for pax in list(pax_ly)]
                    }
    new_doc_data = {'pax_ty': [int(pax) for pax in list(pax_ty)],
                    'pax_change_perc': [int(pax_change) for pax_change in list(pax_change_perc)]}
    avg_perc_change = int(np.mean(new_doc_data['pax_change_perc']))
    dep_date_mod = datetime.datetime.strptime(dep_date, '%Y-%m-%d').strftime('%d-%m-%Y')
    triggering_event_data = {
        'trigger_type': TRIGGER_TYPE_PAX,
        'old_doc_data': old_doc_data,
        'new_doc_data': new_doc_data,
        'priority': TRIGGER_PRIORITY,
        'min_interval': MIN_INTERVAL,
        'triggering_data': triggering_data,
        'category_details': {'category': 'A', 'score': 8.5},
        'unique_trigger_id':
            generate_trigger_id(trigger_name=TRIGGER_TYPE_PAX, pct_change=avg_perc_change, dep_date_start=dep_date_mod,
                                dep_date_end=dep_date_mod, db=db,
                                origin=old_doc_data['origin'], destination=old_doc_data['destination'])[1],
        'pricing_action_id_at_trigger_time': None,
        'trigger_id':
            generate_trigger_id(trigger_name=TRIGGER_TYPE_PAX, pct_change=avg_perc_change, dep_date_start=dep_date_mod,
                                dep_date_end=dep_date_mod, db=db,
                                origin=old_doc_data['origin'], destination=old_doc_data['destination'])[0],
        'lower_threshold': LOWER_THRESHOLD,
        'upper_threshold': UPPER_THRESHOLD,
        'threshold_type': THRESHOLD_TYPE,
        'gen_date': trigger_date,
        'gen_time': trigger_time,
        'time': "08:00",
        'date': SYSTEM_DATE,
        'is_event_trigger': False
    }
    id = db.JUP_DB_Triggering_Event.insert(triggering_event_data)
    return id


@measure(JUPITER_LOGGER)
def create_revenue_trigger(market, dep_date, trx_dates, revenue_ty, db, revenue_ly, revenue_change_perc):
    """
    Function to insert the trigger and its details to the database.
    the trigger is stored in JUP_DB_Triggering_Event collection
    Return: _id of the document stored in JUP_DB_Triggering_Events
    """
    dates = ''
    for date in trx_dates:
        dates += datetime.datetime.strptime(date.encode('UTF-8'), '%Y-%m-%d').strftime('%d-%m-%Y')
        dates += ', '
    dates = dates[:-2]
    trigger_date = datetime.datetime.strftime(today, '%Y-%m-%d')
    trigger_time = datetime.datetime.strftime(today, '%H:%M')
    triggering_data = {'market': market, 'dep_date_start': dep_date, 'dep_date_end': dep_date, 'trx_dates': dates}
    old_doc_data = {'pos': market[:3],
                    'origin': market[3:6],
                    'destination': market[6:9],
                    'compartment': market[9:],
                    'revenue_ly': [int(revenue) for revenue in list(revenue_ly)]
                    }
    new_doc_data = {'revenue_ty': [int(revenue) for revenue in list(revenue_ty)],
                    'revenue_change_perc': [int(revenue_change) for revenue_change in list(revenue_change_perc)]}
    avg_perc_change = int(np.mean(new_doc_data['revenue_change_perc']))
    dep_date_mod = datetime.datetime.strptime(dep_date, '%Y-%m-%d').strftime('%d-%m-%Y')
    triggering_event_data = {
        'trigger_type': TRIGGER_TYPE_REVENUE,
        'old_doc_data': old_doc_data,
        'new_doc_data': new_doc_data,
        'priority': TRIGGER_PRIORITY,
        'min_interval': MIN_INTERVAL,
        'triggering_data': triggering_data,
        'category_details': {'category': 'A', 'score': 8.5},
        'unique_trigger_id': generate_trigger_id(trigger_name=TRIGGER_TYPE_REVENUE, pct_change=avg_perc_change,
                                                 dep_date_start=dep_date_mod, dep_date_end=dep_date_mod,
                                                 origin=old_doc_data['origin'],
                                                 db=db,
                                                 destination=old_doc_data['destination'])[1],
        'pricing_action_id_at_trigger_time': None,
        'trigger_id': generate_trigger_id(trigger_name=TRIGGER_TYPE_REVENUE, pct_change=avg_perc_change,
                                          dep_date_start=dep_date_mod, dep_date_end=dep_date_mod,
                                          db=db,
                                          origin=old_doc_data['origin'], destination=old_doc_data['destination'])[0],
        'lower_threshold': LOWER_THRESHOLD,
        'upper_threshold': UPPER_THRESHOLD,
        'threshold_type': THRESHOLD_TYPE,
        'gen_date': trigger_date,
        'gen_time': trigger_time,
        'time': "08:00",
        'date': SYSTEM_DATE,
        'is_event_trigger': False
    }
    id = db.JUP_DB_Triggering_Event.insert(triggering_event_data)
    return id


@measure(JUPITER_LOGGER)
def create_avg_fare_trigger(market, dep_date, trx_dates, avg_fare_ty, avg_fare_ly, db, avg_fare_change_perc):
    """
    Function to insert the trigger and its details to the database.
    the trigger is stored in JUP_DB_Triggering_Event collection
    Return: _id of the document stored in JUP_DB_Triggering_Events
    """
    dates = ''
    for date in trx_dates:
        dates += datetime.datetime.strptime(date.encode('UTF-8'), '%Y-%m-%d').strftime('%d-%m-%Y')
        dates += ', '
    dates = dates[:-2]
    trigger_date = datetime.datetime.strftime(today, '%Y-%m-%d')
    trigger_time = datetime.datetime.strftime(today, '%H:%M')
    triggering_data = {'market': market, 'dep_date_start': dep_date, 'dep_date_end': dep_date, 'trx_dates': dates}
    old_doc_data = {'pos': market[:3],
                    'origin': market[3:6],
                    'destination': market[6:9],
                    'compartment': market[9:],
                    'avg_fare_ly': [int(fare) for fare in list(avg_fare_ly)]
                    }
    new_doc_data = {'avg_fare_ty': [int(fare) for fare in list(avg_fare_ty)],
                    'avg_fare_change_perc': [int(avg_fare_change) for avg_fare_change in list(avg_fare_change_perc)]}
    avg_perc_change = int(np.mean(new_doc_data['avg_fare_change_perc']))
    dep_date_mod = datetime.datetime.strptime(dep_date, '%Y-%m-%d').strftime('%d-%m-%Y')
    triggering_event_data = {
        'trigger_type': TRIGGER_TYPE_AVG_FARE,
        'old_doc_data': old_doc_data,
        'new_doc_data': new_doc_data,
        'priority': TRIGGER_PRIORITY,
        'min_interval': MIN_INTERVAL,
        'triggering_data': triggering_data,
        'category_details': {'category': 'A', 'score': 8.5},
        'unique_trigger_id': generate_trigger_id(trigger_name=TRIGGER_TYPE_AVG_FARE, pct_change=avg_perc_change,
                                                 dep_date_start=dep_date_mod, dep_date_end=dep_date_mod,
                                                 origin=old_doc_data['origin'],
                                                 db=db,
                                                 destination=old_doc_data['destination'])[1],
        'pricing_action_id_at_trigger_time': None,
        'trigger_id': generate_trigger_id(trigger_name=TRIGGER_TYPE_AVG_FARE, pct_change=avg_perc_change,
                                          dep_date_start=dep_date_mod, dep_date_end=dep_date_mod,
                                          db=db,
                                          origin=old_doc_data['origin'], destination=old_doc_data['destination'])[0],
        'lower_threshold': LOWER_THRESHOLD,
        'upper_threshold': UPPER_THRESHOLD,
        'threshold_type': THRESHOLD_TYPE,
        'gen_date': trigger_date,
        'gen_time': trigger_time,
        'time': "08:00",
        'date': SYSTEM_DATE,
        'is_event_trigger': False
    }
    id = db.JUP_DB_Triggering_Event.insert(triggering_event_data)
    return id


@measure(JUPITER_LOGGER)
def create_market_share_trend_trigger(market, dep_date_start, dep_date_end, months, ms_ty, ms_ly, ms_change_perc, db):
    months_mod = ''
    for month in months:
        year_str = month[:4]
        mont = int(month[4:])
        if mont < 10:
            month_str = '0' + str(mont)
        else:
            month_str = str(mont)
        month_year = month_str + '-' + year_str
        months_mod += month_year
        months_mod += ', '
    months_mod = months_mod[:-2]
    trigger_date = datetime.datetime.strftime(today, '%Y-%m-%d')
    trigger_time = datetime.datetime.strftime(today, '%H:%M')
    triggering_data = {'market': market, 'dep_date_start': dep_date_start, 'dep_date_end': dep_date_end,
                       'months': months_mod}
    old_doc_data = {'pos': market[:3],
                    'origin': market[3:6],
                    'destination': market[6:9],
                    'compartment': market[9:10],
                    'market_share_ly': [int(ms) for ms in ms_ly]
                    }
    new_doc_data = {'market_share_ty': [int(ms) for ms in ms_ty],
                    'market_share_change_perc': [int(ms_change) for ms_change in ms_change_perc]}
    avg_perc_change = int(np.mean(new_doc_data['market_share_change_perc']))
    dep_date_start_mod = datetime.datetime.strptime(dep_date_start, '%Y-%m-%d').strftime('%d-%m-%Y')
    dep_date_end_mod = datetime.datetime.strptime(dep_date_end, '%Y-%m-%d').strftime('%d-%m-%Y')
    triggering_event_data = {
        'trigger_type': TRIGGER_TYPE_MARKET_SHARE,
        'old_doc_data': old_doc_data,
        'new_doc_data': new_doc_data,
        'priority': TRIGGER_PRIORITY,
        'min_interval': MIN_INTERVAL,
        'triggering_data': triggering_data,
        'category_details': {'category': 'A', 'score': 8.5},
        'unique_trigger_id': generate_trigger_id(trigger_name=TRIGGER_TYPE_MARKET_SHARE, pct_change=avg_perc_change,
                                                 dep_date_start=dep_date_start_mod, dep_date_end=dep_date_end_mod,
                                                 origin=old_doc_data['origin'],
                                                 db=db,
                                                 destination=old_doc_data['destination'])[1],
        'pricing_action_id_at_trigger_time': None,
        'trigger_id': generate_trigger_id(trigger_name=TRIGGER_TYPE_MARKET_SHARE, pct_change=avg_perc_change,
                                          dep_date_start=dep_date_start_mod, dep_date_end=dep_date_end_mod,
                                          db=db,
                                          origin=old_doc_data['origin'], destination=old_doc_data['destination'])[0],
        'lower_threshold': LOWER_THRESHOLD,
        'upper_threshold': UPPER_THRESHOLD,
        'threshold_type': THRESHOLD_TYPE,
        'gen_date': trigger_date,
        'gen_time': trigger_time,
        'time': "08:00",
        'date': SYSTEM_DATE,
        'is_event_trigger': False
    }
    id = db.JUP_DB_Triggering_Event.insert(triggering_event_data)
    return id


@measure(JUPITER_LOGGER)
def send_trigger_to_queue(id, priority):
    message = {'_id': str(id), 'priority': priority}
    sending_message(message)

    return None


# ------------------------ MAIN FUNCTION ------------------------- ############


@measure(JUPITER_LOGGER)
def main(db, markets, sig_flag=None):
    TREND_TRIGGERS = []
    st = time.time()
    if sig_flag:
        dates_list = get_trigger_config_dates(db=db, sig_flag=sig_flag)
        if len(dates_list) > 0:
            for date in dates_list:
                print "GENERATING PAX TREND TRIGGERS FOR PERIOD:", date['start'], date['end']
                pax_trend_ids = generate_pax_trend_triggers(markets=markets, dep_date_start=date['start'],
                                                            dep_date_end=date['end'], db=db)
                TREND_TRIGGERS += pax_trend_ids
                print "GENERATING REVENUE TREND TRIGGERS FOR PERIOD:", date['start'], date['end']
                revenue_trend_ids = generate_revenue_trend_triggers(markets=markets, dep_date_start=date['start'],
                                                                    dep_date_end=date['end'], db=db)
                TREND_TRIGGERS += revenue_trend_ids
                print "GENERATING AVG_FARE TREND TRIGGERS FOR PERIOD:", date['start'], date['end']
                avg_fare_trend_ids = generate_avg_fare_trend_triggers(markets=markets, dep_date_start=date['start'],
                                                                      dep_date_end=date['end'], db=db)
                TREND_TRIGGERS += avg_fare_trend_ids
                if date['code_list'][0][0] == 'M':
                    print "GENERATING MARKET SHARE TREND TRIGGERS FOR PERIOD:", date['start'], date['end']
                    market_share_trend_ids = generate_market_share_trend_triggers(markets=markets,
                                                                                  dep_date_start=date['start'],
                                                                                  dep_date_end=date['end'],
                                                                                  month=datetime.datetime.strptime(
                                                                                      date['start'], "%Y-%m-%d").month,
                                                                                  year=datetime.datetime.strptime(
                                                                                      date['start'], "%Y-%m-%d").year,
                                                                                  db=db)
                    TREND_TRIGGERS += market_share_trend_ids
    else:
        month1 = today.month
        year1 = today.year
        dep_date_start1, dep_date_end1 = get_start_end_dates(month1, year1)  # 1 - 30/31, i.e end date of the month
        print "GENERATING PAX TREND TRIGGERS FOR 1ST MONTH . . . . . "
        pax_trend_ids1 = generate_pax_trend_triggers(markets=markets, dep_date_start=SYSTEM_DATE,
                                                     dep_date_end=dep_date_end1, db=db)
        TREND_TRIGGERS += pax_trend_ids1
        print "GENERATING REVENUE TREND TRIGGERS FOR 1ST MONTH . . . . . "
        revenue_trend_ids1 = generate_revenue_trend_triggers(markets=markets, dep_date_start=SYSTEM_DATE,
                                                             dep_date_end=dep_date_end1, db=db)
        TREND_TRIGGERS += revenue_trend_ids1
        print "GENERATING AVG_FARE TREND TRIGGERS FOR 1ST MONTH . . . . . "
        avg_fare_trend_ids1 = generate_avg_fare_trend_triggers(markets=markets, dep_date_start=SYSTEM_DATE,
                                                               dep_date_end=dep_date_end1, db=db)
        TREND_TRIGGERS += avg_fare_trend_ids1
        market_share_trend_ids1 = generate_market_share_trend_triggers(markets=markets, dep_date_start=SYSTEM_DATE,
                                                                       dep_date_end=dep_date_end1, month=month1,
                                                                       year=year1, db=db)
        TREND_TRIGGERS += market_share_trend_ids1
        if month1 != 12:
            month2 = month1 + 1
            year2 = year1
        else:
            month2 = 1
            year2 = year1 + 1
        # Generating the trigger for Second Set of departure dates into consideration (Current Month + 1)
        dep_date_start2, dep_date_end2 = get_start_end_dates(month2, year2)
        print "GENERATING PAX TREND TRIGGERS FOR 2ND MONTH . . . . . "
        pax_trend_ids2 = generate_pax_trend_triggers(markets=markets, dep_date_start=dep_date_start2,
                                                     dep_date_end=dep_date_end2, db=db)
        TREND_TRIGGERS += pax_trend_ids2
        print "GENERATING REVENUE TREND TRIGGERS FOR 2ND MONTH . . . . . "
        revenue_trend_ids2 = generate_revenue_trend_triggers(markets=markets, dep_date_start=dep_date_start2,
                                                             dep_date_end=dep_date_end2, db=db)
        TREND_TRIGGERS += revenue_trend_ids2
        print "GENERATING AVG_FARE TREND TRIGGERS FOR 2ND MONTH . . . . . "
        avg_fare_trend_ids2 = generate_avg_fare_trend_triggers(markets=markets, dep_date_start=dep_date_start2,
                                                               dep_date_end=dep_date_end2, db=db)
        TREND_TRIGGERS += avg_fare_trend_ids2
        market_share_trend_ids2 = generate_market_share_trend_triggers(markets=markets, dep_date_start=dep_date_start2,
                                                                       dep_date_end=dep_date_end2, month=month2,
                                                                       year=year2, db=db)
        TREND_TRIGGERS += market_share_trend_ids2
        if month2 != 12:
            month3 = month2 + 1
            year3 = year2
        else:
            month3 = 1
            year3 = year1 + 1
        # Generating the trigger for Third Set of departure dates into consideration (Current Month + 2)
        dep_date_start3, dep_date_end3 = get_start_end_dates(month3, year3)
        print "GENERATING PAX TREND TRIGGERS FOR 3RD MONTH . . . . . "
        pax_trend_ids3 = generate_pax_trend_triggers(markets=markets, dep_date_start=dep_date_start3,
                                                     dep_date_end=dep_date_end3, db=db)
        TREND_TRIGGERS += pax_trend_ids3
        print "GENERATING REVENUE TREND TRIGGERS FOR 3RD MONTH . . . . . "
        revenue_trend_ids3 = generate_revenue_trend_triggers(markets=markets, dep_date_start=dep_date_start3,
                                                             dep_date_end=dep_date_end3, db=db)
        TREND_TRIGGERS += revenue_trend_ids3
        print "GENERATING AVG_FARE TREND TRIGGERS FOR 3RD MONTH . . . . . "
        avg_fare_trend_ids3 = generate_avg_fare_trend_triggers(markets=markets, dep_date_start=dep_date_start3,
                                                               dep_date_end=dep_date_end3, db=db)
        TREND_TRIGGERS += avg_fare_trend_ids3
        market_share_trend_ids3 = generate_market_share_trend_triggers(markets=markets, dep_date_start=dep_date_start3,
                                                                       dep_date_end=dep_date_end3, month=month3,
                                                                       year=year3, db=db)
        TREND_TRIGGERS += market_share_trend_ids3

    print "Number of pax_trend triggers raised for passed markets and three months = ", counter_pax_triggers
    print "Number of revenue_trend triggers raised for passed markets and three months = ", counter_revenue_triggers
    print "Number of avg_fare_trend triggers raised for passed markets and three months = ", counter_avg_fare_triggers
    print "Number of market_share_trend_triggers raised for passed markets and three months = ", counter_market_share_trend_triggers
    print "Total number of TREND TRIGGERS = ", len(TREND_TRIGGERS)
    print "Total time taken to raise Trend Triggers for given markets and 3 months = ", time.time() - st

    return TREND_TRIGGERS


@measure(JUPITER_LOGGER)
def main_helper(db):
    import time

    st = time.time()
    print "Running Data Level Triggers"
    online_mrkts = db.JUP_DB_Market_Significance.aggregate([
        {"$match": {"online": True}},
        {"$sort": {"rank": 1}},
        {"$project": {"_id": 0, "market": "$market", "pax": "$pax", "revenue": "$revenue"}}
    ],
        allowDiskUse=True)

    online_mrkts_df = pd.DataFrame(list(online_mrkts))
    online_mrkts_df['pos'] = online_mrkts_df['market'].str.slice(0, 3)
    online_mrkts_df['origin'] = online_mrkts_df['market'].str.slice(3, 6)
    online_mrkts_df['destination'] = online_mrkts_df['market'].str.slice(6, 9)
    online_mrkts_df['compartment'] = online_mrkts_df['market'].str.slice(9, 10)
    online_mrkts_df['od'] = online_mrkts_df['origin'] + online_mrkts_df['destination']

    reg_mas = list(db.JUP_DB_Region_Master.find({}))
    reg_mas = pd.DataFrame(reg_mas)
    online_mrkts_df = online_mrkts_df.merge(reg_mas[['POS_CD', 'COUNTRY_NAME_TX']], left_on='pos', right_on='POS_CD',
                                            how='left').drop('POS_CD',
                                                             axis=1)

    online_mrkts_df = online_mrkts_df.rename(columns={"COUNTRY_NAME_TX": "POS_Country"})

    online_mrkts_df = online_mrkts_df.merge(reg_mas[['POS_CD', 'COUNTRY_NAME_TX']], left_on='origin', right_on='POS_CD',
                                            how='left').drop(
        'POS_CD', axis=1).rename(columns={"COUNTRY_NAME_TX": "origin_Country"})

    online_mrkts_df = online_mrkts_df.merge(reg_mas[['POS_CD', 'COUNTRY_NAME_TX']], left_on='destination',
                                            right_on='POS_CD', how='left').drop(
        'POS_CD', axis=1).rename(columns={"COUNTRY_NAME_TX": "destination_Country"})

    online_mrkts_df = online_mrkts_df[(online_mrkts_df['POS_Country'] == online_mrkts_df['origin_Country']) |
                                      (online_mrkts_df['POS_Country'] == online_mrkts_df['destination_Country'])]

    print len(online_mrkts_df[online_mrkts_df['origin_Country'] == online_mrkts_df['destination_Country']])
    online_mrkts_df = online_mrkts_df[online_mrkts_df['origin_Country'] != online_mrkts_df['destination_Country']]

    online_mrkts_df = online_mrkts_df[['market', 'pos', 'origin', 'destination', 'od', 'compartment', 'pax', 'revenue']]
    online_mrkts_df.drop_duplicates(subset='market', inplace=True)
    total_markets = len(online_mrkts_df)
    print "Number of markets where POS country == O/D country = ", total_markets

    online_mrkts_df.sort_values(by='revenue', ascending=False, inplace=True)
    online_mrkts_df.reset_index(inplace=True)
    online_mrkts_df['cumm_revenue'] = online_mrkts_df['revenue'].cumsum(axis=0)
    online_mrkts_df['cumm_rev_perc'] = online_mrkts_df['cumm_revenue'] / online_mrkts_df['revenue'].sum() * 100

    sig_rev = list(online_mrkts_df[online_mrkts_df['cumm_rev_perc'] <= 80.0]['market'].values)
    sub_sig_rev = list(online_mrkts_df[(online_mrkts_df['cumm_rev_perc'] > 80.0) &
                                       (online_mrkts_df['cumm_rev_perc'] <= 95.0)]['market'].values)

    online_mrkts_df.sort_values(by='pax', ascending=False, inplace=True)
    online_mrkts_df.reset_index(inplace=True)
    online_mrkts_df['cumm_pax'] = online_mrkts_df['pax'].cumsum(axis=0)
    online_mrkts_df['cumm_pax_perc'] = online_mrkts_df['cumm_pax'] / online_mrkts_df['pax'].sum() * 100

    sig_pax = list(online_mrkts_df[online_mrkts_df['cumm_pax_perc'] <= 80.0]['market'].values)
    sub_sig_pax = list(online_mrkts_df[(online_mrkts_df['cumm_pax_perc'] > 80.0) &
                                       (online_mrkts_df['cumm_pax_perc'] <= 95.0)]['market'].values)

    p2p = list(online_mrkts_df[((online_mrkts_df['origin'] == Host_Airline_Hub) | (
                online_mrkts_df['destination'] == Host_Airline_Hub) |
                                (online_mrkts_df['origin'] == 'DWC') | (online_mrkts_df['destination'] == 'DWC')) &
                               ((online_mrkts_df['origin'] == online_mrkts_df['pos']) |
                                (online_mrkts_df['destination'] == online_mrkts_df['pos']))]['market'].values)

    sig_markets_df = online_mrkts_df[online_mrkts_df['market'].isin(list(set(sig_pax + sig_rev + p2p)))]

    sub_sig_markets_df = online_mrkts_df[online_mrkts_df['market'].isin(list(set(sub_sig_pax + sub_sig_rev)))]

    non_sig_markets_df = online_mrkts_df[~online_mrkts_df['market'].isin(list(set(sig_markets_df['market'] +
                                                                                  sub_sig_markets_df['market'])))]

    sig_markets_df['sig_flag'] = 'sig'
    sub_sig_markets_df['sig_flag'] = 'sub_sig'
    non_sig_markets_df['sig_flag'] = 'non_sig'

    insert_df = pd.concat([sig_markets_df, sub_sig_markets_df])
    insert_df = insert_df[['market', 'pos', 'od', 'compartment', 'sig_flag']]

    sig_markets = len(sig_markets_df)
    sub_sig_markets = len(sub_sig_markets_df)
    non_sig_markets = len(non_sig_markets_df)

    online_mrkts = list(set(list(sig_markets_df['market'].values) +
                            list(sub_sig_markets_df['market'].values)))
    markets = []
    counter = 0
    for mrkt in list(sig_markets_df['market'].values):
        if counter == 2000:
            id_list = main(markets=markets, sig_flag='sig', db=db)
            markets = list()
            # for trigger in id_list:
            #     analyze(trigger)
            markets.append(mrkt)
            counter = 1
        else:
            markets.append(mrkt)
            counter += 1
    if counter > 0:
        id_list = main(markets=markets, sig_flag='sig', db=db)
    markets = []
    counter = 0
    for mrkt in list(sub_sig_markets_df['market'].values):
        if counter == 2000:
            id_list = main(markets=markets, sig_flag='sub_sig', db=db)
            markets = list()
            # for trigger in id_list:
            #     analyze(trigger)
            markets.append(mrkt)
            counter = 1
        else:
            markets.append(mrkt)
            counter += 1
    if counter > 0:
        id_list = main(markets=markets, sig_flag='sub_sig', db=db)
        # for trigger in id_list:
        #     analyze(trigger)
    print 'Total Time Taken', time.time() - st


if __name__ == '__main__':
    client = mongo_client()
    db=client[JUPITER_DB]
    main_helper(db)
    client.close()
    # print get_market_share_data(['CMBCMBDXBY','KWIKWIDXBY'], 5, 2018)
