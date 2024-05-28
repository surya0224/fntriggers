import copy
import json
import pymongo
import calendar
from pymongo.errors import BulkWriteError
from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne
import collections
# import pandas as pd
import numpy as np
# import global_variable as var
from datetime import datetime
import datetime as dt
import time
import timeit
from dateutil.relativedelta import relativedelta
from jupiter_AI import client, JUPITER_DB, JUPITER_LOGGER
from jupiter_AI.logutils import measure
from jupiter_AI.network_level_params import SYSTEM_DATE

# Connect mongodb db business layer
# db = client[JUPITER_DB]
# ------------------------------ Hardcode values start - ----------------------------------
# Network parameter #
# print SYSTEM_DATE
cur_date = datetime.strptime(SYSTEM_DATE, '%Y-%m-%d')
dateformat = '%Y-%m-%d'
dateformat_y_m = '%Y-%m'
yearformat = '%Y'
monthformat = '%m'
month = [cur_date.month - 1, cur_date.month, cur_date.month + 1, cur_date.month + 2]
year = cur_date.year

# print cur_date
dep_date_start = datetime.strftime((cur_date - relativedelta(months=1)), '%Y-%m') +"-01"
dt_range = cur_date + relativedelta(months=+3)
# calendar.monthrange(dt_range.year, dt_range.month)[1])

dep_date_end = datetime.strftime(dt_range, '%Y-%m') + '-'+str(calendar.monthrange(dt_range.year, dt_range.month)[1])
print dep_date_start,dep_date_end
# userNames = ['Europa_CMB']

eachUser = 0


@measure(JUPITER_LOGGER)
def main(userName, client):
    db = client[JUPITER_DB]
    user_data = db.JUP_DB_User.find_one({'name': userName})
    posList = user_data['list_of_pos']
    local1 = dict()
    local1['$or'] = []
    cursor = user_data['list_of_pos_OD']
    try:
        for x in cursor:
            if x['origin'] == "All" and x['destination'] == "All":
                local = dict()
                if x['compartment'] == "TL":
                    local['$and'] = [{'pos.City': x['pos']}]
                else:
                    local['$and'] = [{'pos.City': x['pos']}, {'compartment': x['compartment']}]
                local1['$or'].append(local)

            elif x['origin'] != "All" and x['destination'] != "All":
                local = dict()
                if x['compartment'] == "TL":
                    local['$and'] = [{'pos.City': x['pos']}, {'origin.City': x['origin']},
                                     {'destination.City': x['destination']}]
                else:
                    local['$and'] = [{'pos.City': x['pos']}, {'origin.City': x['origin']},
                                     {'destination.City': x['destination']}, {'compartment': x['compartment']}]
                local1['$or'].append(local)
            elif x['origin'] != "All":
                local = dict()
                if x['compartment'] == "TL":
                    local['$and'] = [{'pos.City': x['pos']}, {'origin.City': x['origin']}]
                else:
                    local['$and'] = [{'pos.City': x['pos']}, {'origin.City': x['origin']},
                                     {'compartment': x['compartment']}]
                local1['$or'].append(local)
            elif x['destination'] != "All":
                local = dict()
                if (x['compartment'] == "TL"):
                    local['$and'] = [{'pos.City': x['pos']}, {'destination.City': x['destination']}]
                else:
                    local['$and'] = [{'pos.City': x['pos']}, {'destination.City': x['destination']},
                                     {'compartment.City': x['compartment']}]
                local1['$or'].append(local)
    except Exception as error:
        print error
    # print local1['$or']
    # print
    Summary_To_Landing_Page_Pipeline = [
        {'$match': {
            'dep_date': {'$gte' : dep_date_start, '$lte' : dep_date_end},
            # 'dep_month': {'$in': month},
            '$or': local1['$or']
        }},

        {'$group': {
            '_id': {
                'dep_month': '$dep_month',
                'dep_year': '$dep_year',
                'dep_date': '$dep_date',
                'compartment': '$compartment'
            },
            'book_paxCapaAdj': {'$sum': '$book_paxCapaAdj'},
            'book_ticketCapaAdj': {'$sum': '$book_ticketCapaAdj'},
            'sale_paxCapaAdj': {'$sum': '$sale_paxCapaAdj'},
            'sale_revenueCapaAdj': {'$sum': '$sale_revenueCapaAdj'},
            'flown_paxCapaAdj': {'$sum': '$flown_paxCapaAdj'},
            'flown_revenueCapaAdj': {'$sum': '$flown_revenueCapaAdj'},
            'book_pax': {'$sum': '$book_pax'},
            'book_ticket': {'$sum': '$book_ticket'},
            'book_pax_1': {'$sum': '$book_pax_1'},
            'book_ticket_1': {'$sum': '$book_ticket_1'},
            'sale_pax': {'$sum': '$sale_pax'},
            'sale_revenue': {'$sum': '$sale_revenue'},
            'sale_pax_1': {'$sum': '$sale_pax_1'},
            'sale_revenue_1': {'$sum': '$sale_revenue_1'},
            'flown_pax': {'$sum': '$flown_pax'},
            'flown_revenue': {'$sum': '$flown_revenue'},
            'flown_pax_1': {'$sum': '$flown_pax_1'},
            'flown_revenue_1': {'$sum': '$flown_revenue_1'},
            'book_snap_pax': {'$sum': "$book_snap_pax"},
            'sale_snap_pax': {'$sum': "$sale_snap_pax"},
            'flown_snap_pax': {'$sum': "$flown_snap_pax"},
            'book_snap_revenue': {'$sum': "$book_snap_revenue"},
            'sale_snap_revenue': {'$sum': "$sale_snap_revenue"},
            'flown_snap_revenue': {'$sum': "$flown_snap_revenue"},
            'target_pax': {'$sum': '$target_pax'},
            'target_avgFare': {'$avg': '$target_avgFare'},
            'target_revenue': {'$sum': '$target_revenue'},
            'distance': {'$sum': '$distance_sale'},
            'distance_1': {'$sum': '$distance_sale_1'},
            'distance_flown': {'$sum': '$distance_flown'},
            'distance_flown_1': {'$sum': '$distance_flown_1'},
            'distance_Target_Pax': {'$sum': '$distance_Target_Pax'},
            'capacity': {'$sum': '$capacity'},
            'capacity_1': {'$sum': '$capacity_1'},
            'market_pax': {'$sum': '$market_pax'},
            'market_size': {'$sum': '$market_size'},
            'target_pax_1': {'$sum': '$target_pax_1'},
            'target_avgFare_1': {'$avg': '$target_avgFare_1'},
            'target_revenue_1': {'$sum': '$target_revenue_1'},
            'market_pax_1': {'$sum': '$market_pax_1'},
            'market_size_1': {'$sum': '$market_size_1'},
            'forecast_pax': {'$sum': '$forecast_pax'},
            'forecast_avgFare': {'$sum': '$forecast_avgFare'},
            'forecast_revenue': {'$sum': '$forecast_revenue'},
            'list_of_pos': {'$addToSet': '$pos.City'},
            'signOD': {'$addToSet': '$signOD'},
            'nonSignOD': {'$addToSet': '$nonSignOD'},
            'totalOD': {'$addToSet': '$totalOD'},
            'host_ratVScap_forFMS': {'$sum': '$host_ratVScap_forFMS'},
            'comp_ratVScap_forFMS': {'$sum': '$comp_ratVScap_forFMS'}
        }},
        {'$group': {
            '_id': {
                'dep_month': '$_id.dep_month',
                'dep_year': '$_id.dep_year',
                'compartment': '$_id.compartment',
                # signOD:'$signOD',
                # nonSignOD: '$nonSignOD',
                # totalOD: '$totalOD'
                # list_of_pos:'$list_of_pos'
            },
            'book_paxCapaAdj': {'$sum': '$book_paxCapaAdj'},
            'book_ticketCapaAdj': {'$sum': '$book_ticketCapaAdj'},
            'sale_paxCapaAdj': {'$sum': '$sale_paxCapaAdj'},
            'sale_revenueCapaAdj': {'$sum': '$sale_revenueCapaAdj'},
            'flown_paxCapaAdj': {'$sum': '$flown_paxCapaAdj'},
            'flown_revenueCapaAdj': {'$sum': '$flown_revenueCapaAdj'},
            'book_pax': {'$sum': '$book_pax'},
            'book_ticket': {'$sum': '$book_ticket'},
            'book_pax_1': {'$sum': '$book_pax_1'},
            'book_ticket_1': {'$sum': '$book_ticket_1'},
            'sale_pax': {'$sum': '$sale_pax'},
            'sale_revenue': {'$sum': '$sale_revenue'},
            'sale_pax_1': {'$sum': '$sale_pax_1'},
            'sale_revenue_1': {'$sum': '$sale_revenue_1'},
            'flown_pax': {'$sum': '$flown_pax'},
            'flown_revenue': {'$sum': '$flown_revenue'},
            'flown_pax_1': {'$sum': '$flown_pax_1'},
            'flown_revenue_1': {'$sum': '$flown_revenue_1'},
            'book_snap_pax': {'$sum': "$book_snap_pax"},
            'sale_snap_pax': {'$sum': "$sale_snap_pax"},
            'flown_snap_pax': {'$sum': "$flown_snap_pax"},
            'book_snap_revenue': {'$sum': "$book_snap_revenue"},
            'sale_snap_revenue': {'$sum': "$sale_snap_revenue"},
            'flown_snap_revenue': {'$sum': "$flown_snap_revenue"},
            'target_pax': {'$max': '$target_pax'},
            'target_avgFare': {'$avg': '$target_avgFare'},
            'target_revenue': {'$max': '$target_revenue'},
            'target_pax_1': {'$sum': '$target_pax_1'},
            'target_avgFare_1': {'$avg': '$target_avgFare_1'},
            'target_revenue_1': {'$sum': '$target_revenue_1'},
            'market_pax': {'$max': '$market_pax'},
            'market_size': {'$max': '$market_size'},
            'market_pax_1': {'$max': '$market_pax_1'},
            'market_size_1': {'$max': '$market_size_1'},
            'forecast_pax': {'$max': '$forecast_pax'},
            'forecast_avgFare': {'$max': '$forecast_avgFare'},
            'forecast_revenue': {'$max': '$forecast_revenue'},
            'distance': {'$sum': '$distance'},
            'distance_1': {'$sum': '$distance_1'},
            'distance_flown': {'$sum': '$distance_flown'},
            'distance_flown_1': {'$sum': '$distance_flown_1'},
            'distance_Target_Pax': {'$max': '$distance_Target_Pax'},
            'capacity': {'$sum': '$capacity'},
            'capacity_1': {'$sum': '$capacity_1'},
            'host_ratVScap_forFMS': {'$max': '$host_ratVScap_forFMS'},
            'comp_ratVScap_forFMS': {'$max': '$comp_ratVScap_forFMS'},
            'dep_date': {'$push': {
                'dep_date': '$_id.dep_date',
                'capacity': '$capacity',
                'capacity_1': '$capacity_1',
                'distance': '$distance',
                'distance_1': '$distance_1',
                'distance_flown': '$distance_flown',
                'distance_flown_1': '$distance_flown_1',
                'distance_Target_Pax': '$distance_Target_Pax',
                'book_pax': '$book_pax',
                'book_ticket': '$book_ticket',
                'book_pax_1': '$book_pax_1',
                'book_ticket_1': '$book_ticket_1',
                'sale_pax': '$sale_pax',
                'sale_revenue': '$sale_revenue',
                'sale_pax_1': '$sale_pax_1',
                'sale_revenue_1': '$sale_revenue_1',
                'flown_pax': '$flown_pax',
                'flown_revenue': '$flown_revenue',
                'flown_pax_1': '$flown_pax_1',
                'flown_revenue_1': '$flown_revenue_1',
                'book_snap_pax': '$book_snap_pax',
                'sale_snap_pax': '$sale_snap_pax',
                'flown_snap_pax': '$flown_snap_pax',
                'book_snap_revenue': '$book_snap_revenue',
                'sale_snap_revenue': '$sale_snap_revenue',
                'flown_snap_revenue': '$flown_snap_revenue',
                'book_paxCapaAdj': '$book_paxCapaAdj',
                'book_ticketCapaAdj': '$book_ticketCapaAdj',
                'sale_paxCapaAdj': '$sale_paxCapaAdj',
                'sale_revenueCapaAdj': '$sale_revenueCapaAdj',
                'flown_paxCapaAdj': '$flown_paxCapaAdj',
                'flown_revenueCapaAdj': '$flown_revenueCapaAdj',
                'target_pax': '$target_pax',
                'target_avgFare': '$target_avgFare',
                'target_revenue': '$target_revenue',
                'signOD': '$signOD',
                'nonSignOD': '$nonSignOD',
                'totalOD': '$totalOD'
            }}
        }},

        {'$project': {
            'dep_month': '$_id.dep_month',
            'dep_year': '$_id.dep_year',
            'compartment': '$_id.compartment',
            # list_of_pos:'$_id.list_of_pos',
            'book_paxCapaAdj': '$book_paxCapaAdj',
            'book_ticketCapaAdj': '$book_ticketCapaAdj',
            'sale_paxCapaAdj': '$sale_paxCapaAdj',
            'sale_revenueCapaAdj': '$sale_revenueCapaAdj',
            'flown_paxCapaAdj': '$flown_paxCapaAdj',
            'flown_revenueCapaAdj': '$flown_revenueCapaAdj',
            'book_pax': '$book_pax',
            'book_ticket': '$book_ticket',
            'book_pax_1': '$book_pax_1',
            'book_ticket_1': '$book_ticket_1',
            'sale_pax': '$sale_pax',
            'sale_revenue': '$sale_revenue',
            'sale_pax_1': '$sale_pax_1',
            'sale_revenue_1': '$sale_revenue_1',
            'flown_pax': '$flown_pax',
            'flown_revenue': '$flown_revenue',
            'flown_pax_1': '$flown_pax_1',
            'flown_revenue_1': '$flown_revenue_1',
            'book_snap_pax': '$book_snap_pax',
            'sale_snap_pax': '$sale_snap_pax',
            'flown_snap_pax': '$flown_snap_pax',
            'book_snap_revenue': '$book_snap_revenue',
            'sale_snap_revenue': '$sale_snap_revenue',
            'flown_snap_revenue': '$flown_snap_revenue',

            'target_pax': '$target_pax',
            'target_avgFare_1': {
                '$cond': [{'$ne': ["$target_pax_1", 0]}, {'$divide': ['$target_revenue_1', '$target_pax_1']}, 0]
            },
            'target_avgFare': {
                '$cond': [{'$ne': ["$target_pax", 0]}, {'$divide': ['$target_revenue', '$target_pax']}, 0]
            },
            'target_revenue': '$target_revenue',
            'target_pax_1': '$target_pax_1',

            'target_revenue_1': '$target_revenue_1',
            'forecast_pax': '$forecast_pax',
            'forecast_avgFare': '$forecast_avgFare',
            'forecast_revenue': '$forecast_revenue',
            'distance': '$distance',
            'distance_1': '$distance_1',
            'distance_flown': '$distance_flown',
            'distance_flown_1': '$distance_flown_1',
            'distance_Target_Pax': '$distance_Target_Pax',

            'capacity': '$capacity',
            'capacity_1': '$capacity_1',

            'market': {
                'pax': '$market_pax',
                'market_size': '$market_size',
                'pax_1': '$market_pax_1',
                'market_size_1': '$market_size_1',
                # host_ratVScap_forFMS:{$max: '$host_ratVScap_forFMS'},
                # comp_ratVScap_forFMS: {$max: '$comp_ratVScap_forFMS'},
                'FMS': {'$cond': [{'$ne': ['$comp_ratVScap_forFMS', 0]}, {'$multiply': [{
                    '$divide': ['$host_ratVScap_forFMS', '$comp_ratVScap_forFMS'
                                ]}, 100]}, 0]}},

            'dep_date': '$dep_date'
        }},

        {'$group': {
            '_id': {
                'dep_month': '$dep_month',
                'compartment': '$compartment',
                'dep_year': '$dep_year',
                # list_of_pos:'$list_of_pos',
                'this_month': {
                    'book_pax': '$book_pax',
                    'book_revenue': '$book_ticket',
                    'book_pax_1': '$book_pax_1',
                    'book_revenue_1': '$book_ticket_1',
                    'sale_pax': '$sale_pax',
                    'sale_revenue': '$sale_revenue',
                    'sale_pax_1': '$sale_pax_1',
                    'sale_revenue_1': '$sale_revenue_1',
                    'flown_pax': '$flown_pax',
                    'flown_revenue': '$flown_revenue',
                    'flown_pax_1': '$flown_pax_1',
                    'flown_revenue_1': '$flown_revenue_1',
                    'book_paxCapaAdj': '$book_paxCapaAdj',
                    'book_ticketCapaAdj': '$book_ticketCapaAdj',
                    'sale_paxCapaAdj': '$sale_paxCapaAdj',
                    'sale_revenueCapaAdj': '$sale_revenueCapaAdj',
                    'flown_paxCapaAdj': '$flown_paxCapaAdj',
                    'flown_revenueCapaAdj': '$flown_revenueCapaAdj',
                    'book_snap_pax': {'$sum': "$book_snap_pax"},
                    'sale_snap_pax': {'$sum': "$sale_snap_pax"},
                    'flown_snap_pax': {'$sum': "$flown_snap_pax"},
                    'book_snap_revenue': {'$sum': "$book_snap_revenue"},
                    'sale_snap_revenue': {'$sum': "$sale_snap_revenue"},
                    'flown_snap_revenue': {'$sum': "$flown_snap_revenue"},
                    'target_pax': '$target_pax',
                    'target_avgFare': '$target_avgFare',
                    'target_revenue': '$target_revenue',
                    'target_pax_1': '$target_pax_1',
                    'target_avgFare_1': '$target_avgFare_1',
                    'target_revenue_1': '$target_revenue_1',
                    'distance': '$distance',
                    'distance_1': '$distance_1',
                    'distance_flown': '$distance_flown',
                    'distance_flown_1': '$distance_flown_1',
                    'distance_Target_Pax': '$distance_Target_Pax',
                    'capacity': '$capacity',
                    'capacity_1': '$capacity_1',
                    'forecast_pax': '$forecast_pax',
                    'forecast_avgFare': '$forecast_avgFare',
                    'forecast_revenue': '$forecast_revenue',
                    'dep_date': '$dep_date',
                    'market': '$market'
                }
            },
            'count': {'$sum': 1}
        }},
        {
            '$addFields': {
                'user': userName
            }
        },
        {
            '$addFields': {
                'list_of_pos': posList
            }
        },
        {'$project': {
            '_id': 0,
            'user': '$user',
            'dep_month': '$_id.dep_month',
            'compartment': '$_id.compartment',
            'dep_year': '$_id.dep_year',
            'list_of_pos': '$list_of_pos',
            'this_month': '$_id.this_month'
        }},
        # {'$out': 'JUP_DB_Landing_Page_V'}
    ]
    cursor = db.JUP_DB_Manual_Triggers_Module_Summary.aggregate(Summary_To_Landing_Page_Pipeline, allowDiskUse=True)

    # cursor = db.Temp_JUP_DB_Landing_Page_V.find(no_cursor_timeout=True)
    for x in cursor:
        # print x
        # del x['_id']
        try:
            x['year_month'] = str(int(x['dep_year'])) + "" + str(int(x['dep_month']))
            db.JUP_DB_Landing_Page_.update(
                {
                    'user': x['user'],
                    'dep_month': x['dep_month'],
                    'dep_year': x['dep_year'],
                    'compartment': x['compartment']

                },
                x,
                upsert=True
            )
        except Exception as error:
            print error
    try:
        # print userName/
        if db.JUP_DB_User.find_one(
                {'cluster': {'$eq': 'network'}, 'name': {'$eq': userName}, 'active': True}) is not None:
            doc = db.JUP_DB_Landing_Page_.find(
                {'user': userName})
            network_user = db.JUP_DB_User.distinct('name', {'cluster': {'$eq': 'network'}, 'name': {'$ne': userName},
                                                            'active': True})
            # print userName

            for each_network_user in network_user:
                print userName + " " + each_network_user
                for each_doc in doc:
                    del each_doc['_id']
                    each_doc['user'] = each_network_user
                    db.JUP_DB_Landing_Page_.update(
                        {
                            'user': each_doc['user'],
                            'dep_month': each_doc['dep_month'],
                            'dep_year': each_doc['dep_year'],
                            'compartment': each_doc['compartment']
                        },
                        each_doc,
                        upsert=True
                    )
    except Exception as error:
        print error

        # db.JUP_DB_Landing_Page_V.remove({})


if __name__ == '__main__':
    db = client[JUPITER_DB]
    pos_list = list(db.JUP_DB_User.distinct('name', {'cluster': {'$ne': 'network'}, 'active': True}))
    network_user = db.JUP_DB_User.find_one({'cluster': {'$eq': 'network'}, 'active': True})
    pos_list.append(network_user['name'])
    for each_user in pos_list:
        #main(each_user, client)
        pass
        # print each_market
    main("Europa_CMB", client)