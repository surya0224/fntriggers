"""
File Name              :   Derived_Factor_Network
Author                 :   Shamail Mulla
Date Created           :   2017-03-30
Description            :  Derived factor for each OD (with one stop) will be calculated at city, country, region,
                            network level or origin to city, country, region, network level of destination for
                            competitors and host.

"""

import datetime
import time
from jupiter_AI.common import ClassErrorObject as error_class
from jupiter_AI import client, JUPITER_DB, Host_Airline_Code as host, Host_Airline_Hub as hub, JUPITER_LOGGER
from jupiter_AI.logutils import measure
db = client[JUPITER_DB]
from jupiter_AI.tiles.jup_common_functions_tiles import query_month_year_builder
from jupiter_AI.tiles.jup_common_functions_tiles import gen_collection_name as gen


@measure(JUPITER_LOGGER)
def capacity_influence_origin_network(d_level):
    desired_decimal = 3  # hard coded value
    dsrd_decimal = 10 ** desired_decimal

    print 'derived factor function: N -', d_level, 'level'

    result_collection_name = gen()
    ppln_df = [
        {
            '$match': {'destination_level': {'$eq': d_level},'assigned_destination':{'$exists': True}}
        }
        ,
        {
            '$group':
                {
                    '_id':
                        {
                            # 'origin': '$origin',
                            'quarter': '$quarter',
                            'year': '$year',
                            'airline': '$airline',
                            'destination': '$assigned_destination'
                        },
                    'od_pax': {'$sum':'$od_pax'}
                }
        }
        ,
        {
            '$group':
                {
                    '_id':
                        {
                            'quarter': '$quarter',
                            'year': '$year',
                            'airline': '$airline'
                        },
                    'pax_origin': {'$sum':'$od_pax'},
                    'od_details':
                        {
                            '$push':
                                {
                                    'destination': '$_id.destination',
                                    'od_pax': '$od_pax'
                                }
                        }
                }
        }
        ,
        {
            '$unwind': '$od_details'
        }
        ,
        # Formula: derived_factor = (O-D pax) / (O-* pax)
        {
            '$addFields':
                {
                    'derived_factor':
                        {
                            '$cond':
                                {
                                    'if':
                                        {
                                            '$gt': ['$pax_origin', 0]
                                        },
                                    'then':
                                        {
                                            '$divide': ['$od_details.od_pax', '$pax_origin']
                                        },
                                    'else': 0
                                }
                        }
                }
        }
        ,
        # Rounding to appropriate significant figures
        {
            '$addFields':
                {
                    'derived_factor':
                        {
                            '$divide':
                                [
                                    {
                                        '$subtract':
                                            [
                                                {
                                                    '$multiply': ['$derived_factor', dsrd_decimal]
                                                },
                                                {
                                                    '$mod':
                                                        [
                                                            {
                                                                '$multiply': ['$derived_factor', dsrd_decimal]
                                                            },
                                                            1
                                                        ]
                                                }
                                            ]
                                    },
                                    dsrd_decimal
                                ]
                        }
                }
        }
        ,
        {
            '$project':
                {
                    '_id': 0,
                    'derived_factor': '$derived_factor',
                    'true_origin': 'NETWORK',
                    'true_destination': '$od_details.destination',
                    'quarter': '$_id.quarter',
                    'year': '$_id.year',
                    'od_pax': '$od_details.od_pax',
                    'pax_origin': '$pax_origin',
                    'operational_days': '1234567',
                    'effective_from': '',
                    'effective_to': '',
                    'user_override': '',
                    'airline': '$_id.airline',
                    'last_update_date': str(datetime.datetime.now().strftime('%Y-%m-%d')),
                    'origin_level': 'N',
                    'destination_level': d_level,
                    'assigned_origin': 'NETWORK',
                    'assigned_destination': '$od_details.destination',
                    'user_override_flag': {'$literal': 0},
                    'weekday_flag': {'$literal': 0}
                }
        }
        ,
        {
            '$addFields':
                {
                    'priority_level':
                        {
                            '$cond':
                                {  # A-N
                                    'if': {'$eq': ['$origin_level', 'A']},
                                    'then': {'$literal': 6},
                                    'else':
                                        {
                                            '$cond':
                                                {  # C-N
                                                    'if': {'$eq': ['$origin_level', 'C']},
                                                    'then': {'$literal': 11},
                                                    'else': {'$literal': 14}  # R-N

                                                }
                                        }
                                }
                        }
                }
        }
        ,
        {
            '$out': result_collection_name
        }
    ]
    db.JUP_DB_Capacity_Derived_Factor.aggregate(ppln_df, allowDiskUse=True)
    crsr_df = db.get_collection(result_collection_name)
    print crsr_df.count(),'records'
    lst_df = list(crsr_df.find(projection={'_id': 0}))
    # db[result_collection_name].drop()
    # db.JUP_DB_Capacity_Derived_Factor.insert(lst_df)


@measure(JUPITER_LOGGER)
def capacity_influence_destination_network(o_level):
    desired_decimal = 3  # hard coded value
    dsrd_decimal = 10 ** desired_decimal

    print 'derived factor function:',o_level,'- N level'

    result_collection_name = gen()
    ppln_df = [
        {
            '$match': {'origin_level': {'$eq': o_level},'assigned_destination':{'$exists': True}}
        }
        ,
        {
            '$group':
                {
                    '_id':
                        {
                            'origin': '$assigned_origin',
                            'quarter': '$quarter',
                            'year': '$year',
                            'airline': '$airline'
                        },
                    'od_pax': {'$sum': '$od_pax'}
                }
        }
        ,
        {
            '$group':
                {
                    '_id':
                        {
                            'quarter': '$quarter',
                            'year': '$year',
                            'airline': '$airline'
                        },
                    'pax_origin': {'$sum': '$od_pax'},
                    'od_details':
                        {
                            '$push':
                                {
                                    'destination': '$_id.destination',
                                    'od_pax': '$od_pax'
                                }
                        }
                }
        }
        ,
        {
            '$unwind': '$od_details'
        }
        ,
        # Formula: derived_factor = (O-D pax) / (O-* pax) = 1
        {
            '$addFields':
                {
                    'derived_factor':1
                }
        }
        ,
        {
            '$project':
                {
                    '_id': 0,
                    'derived_factor': '$derived_factor',
                    'true_destination': 'NETWORK',
                    'true_origin': '$_id.origin',
                    'quarter': '$_id.quarter',
                    'year': '$_id.year',
                    'od_pax': '$od_details.od_pax',
                    'pax_origin': '$pax_origin',
                    'operational_days': '1234567',
                    'effective_from': '',
                    'effective_to': '',
                    'user_override': '',
                    'airline': '$_id.airline',
                    'last_update_date': str(datetime.datetime.now().strftime('%Y-%m-%d')),
                    'destination_level': 'N',
                    'origin_level': o_level,
                    'assigned_origin': '$_id.origin',
                    'assigned_destination': 'NETWORK',
                    'user_override_flag': {'$literal': 0},
                    'weekday_flag': {'$literal': 0}
                }
        }
        ,
        {
            '$addFields':
                {
                    'priority_level':
                        {
                            '$cond':
                                {  # N-A
                                    'if': {'$eq': ['$destination_level', 'A']},
                                    'then': {'$literal': 7},
                                    'else':
                                        {
                                            '$cond':
                                                {  # N-C
                                                    'if': {'$eq': ['$destination_level', 'C']},
                                                    'then': {'$literal': 12},
                                                    'else': {'$literal': 15}  # N-R

                                                }
                                        }
                                }
                        }
                }
        }
        ,
        {
            '$out': result_collection_name
        }
    ]
    db.JUP_DB_Capacity_Derived_Factor.aggregate(ppln_df, allowDiskUse=True)
    crsr_df = db.get_collection(result_collection_name)
    print crsr_df.count(),'records'
    lst_df = list(crsr_df.find(projection={'_id': 0}))
    # db[result_collection_name].drop()
    # db.JUP_DB_Capacity_Derived_Factor.insert(lst_df)


@measure(JUPITER_LOGGER)
def capacity_influence_od_network():
    print 'derived factor function: N - N level'

    result_collection_name = gen()
    ppln_df = [
        {
            '$match':
                {
                    '$and':
                        [
                            {'origin_level': 'R'},
                            {'destination_level': 'R'}
                        ]
                }
        }
        ,
        {
            '$group':
                {
                    '_id':
                        {
                            'airline': '$airline',
                            'quarter': '$quarter',
                            'year': '$year'
                        },
                    'pax_origin': {'$sum': '$pax_origin'},
                    'od_pax': {'$sum': '$od_pax'}
                }
        }
        ,
        {
            '$project':
                {
                    '_id': 0,
                    'airline': '$_id.airline',
                    'quarter': '$_id.quarter',
                    'year': '$_id.year',
                    'pax_origin': '$pax_origin',
                    'od_pax': '$od_pax',
                    'last_update_date': str(datetime.datetime.now().strftime('%Y-%m-%d')),
                    'destination_level': 'N',
                    'origin_level': 'N',
                    'assigned_origin': 'NETWORK',
                    'assigned_destination': 'NETWORK',
                    'operational_days': '1234567',
                    'effective_from': '',
                    'effective_to': '',
                    'user_override': '',
                    'derived_factor':{'$literal': 1},
                    'true_destination': 'NETWORK',
                    'true_origin': 'NETWORK',
                    'user_override_flag': {'$literal': 0},
                    'weekday_flag': {'$literal': 0},
                    'priority_level': {'$literal': 16}
                }
        }
        ,
        {
            '$out': result_collection_name
        }
    ]
    db.JUP_DB_Capacity_Derived_Factor.aggregate(ppln_df, allowDiskUse=True)
    crsr_df = db.get_collection(result_collection_name)
    print crsr_df.count(), 'records'
    lst_df = list(crsr_df.find(projection={'_id': 0}))
    # db[result_collection_name].drop()
    # db.JUP_DB_Capacity_Derived_Factor.insert(lst_df)


if __name__ == '__main__':
    start_time = time.time()

    levels = ['A', 'C', 'R']

    for level in levels:
        capacity_influence_origin_network(level)
        capacity_influence_destination_network(level)

    capacity_influence_od_network()

    print time.time() - start_time