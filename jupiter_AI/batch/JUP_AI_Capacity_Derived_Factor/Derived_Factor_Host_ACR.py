"""
File Name              :   Derived_Factor_Host
Author                 :   Ashwin Kumar
Date Created           :   2017-01-12
Description            :  Capacity factor for each OD (with one stop) will be calculated at network level.

MODIFICATIONS LOG
    S.No                   : 1
    Date Modified          : 2017-03-31
    By                     : Shamail
    Modification Details   : Added function to find host derived factor at network level

"""

from collections import defaultdict
from copy import deepcopy
import json
import pymongo
import inspect
import datetime
import time
from jupiter_AI.common import ClassErrorObject as error_class
from jupiter_AI import client, JUPITER_DB, Host_Airline_Code as host, Host_Airline_Hub as hub, JUPITER_LOGGER
from jupiter_AI.logutils import measure
db = client[JUPITER_DB]
from jupiter_AI.tiles.jup_common_functions_tiles import query_month_year_builder
from jupiter_AI.tiles.jup_common_functions_tiles import gen_collection_name as gen

result_coll_name = gen()


@measure(JUPITER_LOGGER)
def capacity_influence_host(o_level, d_level):
    print 'host derived factor function:', o_level, '-', d_level, 'level'

    if o_level == "A":
        origin_level = 'POS_CD'
    elif o_level == "C":
        origin_level = 'COUNTRY_CD'
    elif o_level == 'R':
        origin_level = "Region"
    else:
        pass

    if d_level == "A":
        destination_level = 'POS_CD'
    elif d_level == "C":
        destination_level = 'COUNTRY_CD'
    elif d_level == "R":
        destination_level = "Region"
    else:
        pass

    desired_decimal = 3 #hard coded value
    dsrd_decimal = 10 ** desired_decimal

    pipeline_capacity = \
    [
        {
            '$addFields':
                {
                    'year_new': {'$substr': ['$dep_date',0,4]},
                    'month_new': {'$substr': ['$dep_date',5,2]}
                }
        }
        ,
        {
            '$addFields':
                {
                    'quarter':
                        {
                            '$cond':
                                {
                                    'if':
                                        {
                                            '$or':
                                                [
                                                    {'$eq': ['$month_new', '03']},
                                                    {'$eq': ['$month_new', '02']},
                                                    {'$eq': ['$month_new', '01']}
                                                ]
                                        },
                                    'then': 'Q1',
                                    'else':
                                        {
                                            '$cond':
                                                {
                                                    'if':
                                                        {
                                                            '$or':
                                                                [
                                                                    {'$eq': ['$month_new', '04']},
                                                                    {'$eq': ['$month_new', '05']},
                                                                    {'$eq': ['$month_new', '06']}
                                                                ]
                                                        },
                                                    'then': 'Q2',
                                                    'else':
                                                        {
                                                            '$cond':
                                                                {
                                                                    'if':
                                                                        {
                                                                            '$or':
                                                                                [
                                                                                    {'$eq': ['$month_new', '07']},
                                                                                    {'$eq': ['$month_new', '08']},
                                                                                    {'$eq': ['$month_new', '09']}
                                                                                ]
                                                                        },
                                                                    'then': 'Q3',
                                                                    'else': 'Q4'
                                                                }
                                                        }
                                                }
                                        }
                                }
                        }
                }
        }
        ,
        {
            '$group':
                {
                    '_id':
                        {
                            'origin': '$origin',
                            'destination': '$destination',
                            'quarter': '$quarter',
                            'year': '$year'
                        },
                    'pax': {'$sum': '$pax'}
                }
        }
        ,
        {
            '$project':
                {
                    'origin': '$_id.origin',
                    'destination': '$_id.destination',
                    'quarter': '$_id.quarter',
                    'year': '$_id.year',
                    'pax': '$pax',
                    'origin_level': origin_level
                }
        }
        ,
        # matching destination origin to its level of hierarchy
        {
            '$lookup':
                {
                    'from': 'JUP_DB_Region_Master',
                    'localField': 'origin',
                    'foreignField': 'POS_CD',
                    'as': 'hierarchy'
                }
        }
        ,
        {
            '$unwind': '$hierarchy'
        }
        ,
        {
            '$project':
                {
                    'destination': '$destination',
                    'origin': {
                        '$cond':
                            {
                                'if': {'$eq': ['$origin_level', 'POS_CD']},
                                'then': '$hierarchy.POS_CD',
                                'else':
                                    {
                                        '$cond':
                                            {
                                                'if': {'$eq': ['$origin_level', 'COUNTRY_CD']},
                                                'then': '$hierarchy.COUNTRY_CD',
                                                'else': '$hierarchy.Region'
                                            }
                                    }
                            }
                    },
                    'quarter': '$quarter',
                    'year': '$year',
                    'pax': '$pax'
                }
        }
        ,
        # summing up pax at hierarchy level of origin
        {
            '$group':
                {
                    '_id':
                        {
                            'origin': '$origin',
                            'destination': '$destination',
                            'quarter': '$quarter',
                            'year': '$year'
                        },
                    'pax': {'$sum': '$pax'}
                }
        }
        ,
        {
            '$project':
                {
                    'origin': '$_id.origin',
                    'destination': '$_id.destination',
                    'quarter': '$_id.quarter',
                    'year': '$_id.year',
                    'pax': '$pax',
                    'destination_level': destination_level
                }
        }
        ,
        # matching destination destination to its level of hierarchy
        {
            '$lookup':
                {
                    'from': 'JUP_DB_Region_Master',
                    'localField': 'destination',
                    'foreignField': 'POS_CD',
                    'as': 'hierarchy'
                }
        }
        ,
        {
            '$unwind': '$hierarchy'
        }
        ,
        {
            '$project':
                {
                    'origin': '$origin',
                    'destination': {
                        '$cond':
                            {
                                'if': {'$eq': ['$destination_level', 'POS_CD']},
                                'then': '$hierarchy.POS_CD',
                                'else':
                                    {
                                        '$cond':
                                            {
                                                'if': {'$eq': ['$destination_level', 'COUNTRY_CD']},
                                                'then': '$hierarchy.COUNTRY_CD',
                                                'else': '$hierarchy.Region'
                                            }
                                    }
                            }
                    },
                    'quarter': '$quarter',
                    'year': '$year',
                    'pax': '$pax'
                }
        }
        ,
        # summing up pax at hierarchy level of destination
        {
            '$group':
                {
                    '_id':
                        {
                            'origin': '$origin',
                            'destination': '$destination',
                            'quarter': '$quarter',
                            'year': '$year'
                        },
                    'pax': {'$sum': '$pax'}
                }
        }
        ,
        {
            '$group':
                {
                    '_id':
                        {
                            'origin': '$origin',
                            'quarter': '$_id.quarter',
                            'year': '$_id.year'
                        },
                    'pax_origin': {'$sum': '$pax'},
                    'od_details':
                        {
                            '$push':
                                {
                                    'destination': '$destination',
                                    'od_pax': '$pax'
                                }
                        }
                }
        }
        ,
        {
            '$unwind': '$od_details'
        }
        ,
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
                                            '$divide':
                                                [
                                                    '$od_details.od_pax',
                                                    '$pax_origin'
                                                ]
                                        },
                                    'else': 0
                                }
                        }
                }
        }
        ,
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
                                                    '$multiply': ['$derived_factor',
                                                                  dsrd_decimal]
                                                },
                                                {
                                                    '$mod':
                                                        [
                                                            {
                                                                '$multiply': ['$derived_factor',
                                                                              dsrd_decimal]
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
                    'airline': host,
                    'derived_factor': '$derived_factor',
                    'true_origin': '$_id.origin',
                    'true_destination': '$od_details.destination',
                    'quarter': '$_id.quarter',
                    'year': '$_id.year',
                    'od_pax': '$od_details.od_pax',
                    'pax_origin': '$pax_origin',
                    'origin_level': o_level,
                    'destination_level': d_level,
                    'operational_days': '1234567',
                    'effective_from': '',
                    'effective_to': '',
                    'user_override': '',
                    'last_update_date': str(datetime.datetime.now().strftime('%Y-%m-%d')),
                    'assigned_origin': '$_id.origin',
                    'assigned_destination': '$od_details.destination',
                    'user_override_flag': {'$literal': 0},
                    'weekday_flag': {'$literal': 0},
                    'priority_level': {'$literal': 0}
                }
        }
        ,
        {
            '$addFields':
                {
                    'priority_level':
                        {
                            '$cond':
                                {
                                    'if': {'$eq': ['$origin_level', 'A']},
                                    'then':  # origin level is city (airport)
                                        {
                                            '$cond':
                                                {  # A-A level
                                                    'if': {'$eq': ['$destination_level', 'A']},
                                                    'then': {'$literal': 1},
                                                    'else':
                                                        {
                                                            '$cond':
                                                                {  # A-C
                                                                    'if': {'$eq': ['$destination_level', 'C']},
                                                                    'then': {'$literal': 2},
                                                                    'else': {'$literal': 4}  # A-R
                                                                }
                                                        }

                                                }
                                        },
                                    'else':  # origin level can be country or region
                                        {
                                            '$cond':
                                                {
                                                    'if': {'$eq': ['$origin_level', 'C']},
                                                    'then':  # origin level is country
                                                        {
                                                            '$cond':
                                                                {  # C-A
                                                                    'if': {'$eq': ['$destination_level', 'A']},
                                                                    'then': {'$literal': 3},
                                                                    'else':
                                                                        {
                                                                            '$cond':
                                                                                {  # C-C
                                                                                    'if': {'$eq': ['$destination_level',
                                                                                                   'C']},
                                                                                    'then': {'$literal': 8},
                                                                                    'else': {'$literal': 9}  # C-R
                                                                                }
                                                                        }
                                                                }
                                                        },
                                                    'else':  # origin level is region
                                                        {
                                                            '$cond':
                                                                {  # R-A
                                                                    'if': {'$eq': ['$destination_level', 'A']},
                                                                    'then': {'$literal': 5},
                                                                    'else':
                                                                        {
                                                                            '$cond':
                                                                                {  # R-C
                                                                                    'if': {'$eq': ['$destination_level',
                                                                                                   'C']},
                                                                                    'then': {'$literal': 10},
                                                                                    'else': {'$literal': 13}  # R-R
                                                                                }
                                                                        },
                                                                }
                                                        }
                                                }
                                        }
                                }
                        }
                }
        }
        ,
        {
            '$out': result_coll_name
        }
    ]
    db.JUP_DB_Sales.aggregate(pipeline_capacity,allowDiskUse=True)
    crsr_df = db.get_collection(result_coll_name)
    print crsr_df.count(), 'records'
    lst_df = list(crsr_df.find(projection={'_id': 0}))
    db[result_coll_name].drop()
    db.JUP_DB_Capacity_Derived_Factor.insert(lst_df)


if __name__ == '__main__':
    start_time = time.time()
    o_level = ['A', 'C', 'R']
    d_level = ['A', 'C', 'R']

    for level1 in o_level:
        for level2 in d_level:
            capacity_influence_host(o_level=level1, d_level=level2)

    # capacity_influence_host(o_level="A", d_level="N")
    #
    # capacity_influence_host(o_level="C", d_level="N")
    #
    # capacity_influence_host(o_level="R", d_level="N")

    print (time.time() - start_time)

