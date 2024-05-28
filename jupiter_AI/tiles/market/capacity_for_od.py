"""
File Name              :   capacity_for_od
Author                 :   Ashwin Kumar
Date Created           :   2017-01-12
Description            :  Capacity factor for each Origin Destination ( with one stop) will be calculated.

"""

import time
from jupiter_AI import client, JUPITER_DB
db = client[JUPITER_DB]



def capacity_influence(afilter):

    qry_capacity = dict()
    # find quarter for current date and find first day of the same very quarter. this should be the input
    # for the aggregation pipeline
    today = time.time()
    qry_capacity['dep_date'] = {'$gte': afilter['fromDate']}
    desired_decimal = 5 #hard coded value
    dsrd_decimal = 10 ** desired_decimal

    pipeline_capacity = \
    [
        # {
        #     '$match': qry_capacity
        # }
        # ,
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
                                    'then': 'q1',
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
                                                    'then': 'q2',
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
                                                                    'then': 'q3',
                                                                    'else': 'q4'
                                                                }
                                                        }
                                                }
                                        }
                                }
                        }
                }
        }
        # ,
        # {
        #     '$sort':
        #         {
        #             'month_new': -1
        #         }
        # }
        ,
        {
            '$group':
                {
                    '_id':
                        {
                            'od': '$od',
                            'quarter': '$quarter',
                            'year': '$year'
                        },
                    'pax': {'$sum': '$pax'}
                }
        }
        ,
        {
            '$addFields':
                {
                    'origin': {'$substr':['$_id.od',0,3]}
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
                                    'od': '$_id.od',
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
                                            '$gt': ['$pax_origin',
                                                    0]
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
            '$sort': {'od_details.od': -1}
        }
    ]

    cursor_capacity = db.JUP_DB_Sales.aggregate(pipeline_capacity)
    capacity_list = list(cursor_capacity)
    sum = 0
    for i in capacity_list:
        print i
        sum += 1
    print sum
    return 0

if __name__ == '__main__':
    a = {
        'region': [],
        'country': [],
        'pos': [],
        'origin': ['DXB'],
        'destination': ['DOH'],
        'compartment': [],
        'fromDate': '2016-12-01',
        'toDate': '2016-12-31'
    }
    start_time = time.time()
    print capacity_influence(afilter=a)
    print (time.time() - start_time)

