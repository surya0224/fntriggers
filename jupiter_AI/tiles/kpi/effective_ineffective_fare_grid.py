"""
File Name              : effective_ineffective_fares
Author                 : Ashwin Kumar
Date Created           : 2016-12-29
Description            : calculate ineffective and effective fares based on revenue and revenue vtgt,
                         pax and pax vtgt, fares and fare vtgt, market share and market share vtgt,
                         yield and yield vtgt. vtgt stands for variation from target. the culmination of
                         these values determine whether a fare is effective or not.

MODIFICATIONS LOG
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :
"""
try:

    from jupiter_AI.network_level_params import query_month_year_builder
    from jupiter_AI.network_level_params import SYSTEM_DATE
    from jupiter_AI.common.convert_currency import convert_currency
    from jupiter_AI.network_level_params import na_value
    from jupiter_AI.network_level_params import Host_Airline_Code as host
    from jupiter_AI.network_level_params import query_month_year_builder
    from jupiter_AI.tiles.jup_common_functions_tiles import gen_collection_name as gen
    result_collection_name = gen()
    from jupiter_AI import client, JUPITER_DB
    db = client[JUPITER_DB]
    from copy import deepcopy
    import inspect
    from collections import defaultdict
    import datetime
    import time
    import numpy
except:
    pass

from jupiter_AI.common import ClassErrorObject as error_class

def get_arg_lists(frame):
    """
    function used to get the list of arguments of the function
    where it is called
    """
    args, _, _, values = inspect.getargvalues(frame)
    argument_name_list = []
    argument_value_list = []
    for k in args:
        argument_name_list.append(k)
        argument_value_list.append(values[k])
    return argument_name_list, argument_value_list


def effective_ineffective_fares(afilter):
    try:
        if 'JUP_DB_Target_OD' in db.collection_names() \
                and 'JUP_DB_Sales' in db.collection_names() \
                and 'JUP_DB_ATPCO_Fares' in db.collection_names() \
                and 'JUP_DB_Market_Share_Last' in db.collection_names() \
                and 'JUP_DB_OD_Distance_Master' in db.collection_names():
            afilter = deepcopy(defaultdict(list, afilter))  # use proper variable names
            query = dict()  # use proper variable names
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
            #   For Sales FLown
            query['dep_date'] = {'$gte': afilter['fromDate'],
                                  '$lte': afilter['toDate']}
            from_month = datetime.datetime.strptime(afilter['fromDate'], '%Y-%m-%d').month
            from_year = datetime.datetime.strptime(afilter['fromDate'], '%Y-%m-%d').year
            to_month = datetime.datetime.strptime(afilter['toDate'], '%Y-%m-%d').month
            to_year = datetime.datetime.strptime(afilter['toDate'], '%Y-%m-%d').year
            month_year_combinations = query_month_year_builder(from_month,from_year,to_month,to_year)
            month_year_list = []
            for i in month_year_combinations:
                month_year_list.append([i['month'],i['year']])

            fare_market = 15.0  # this value has been hard coded for development purposes

            #get the weights for the following parameters - the below values are hard coded for development purposes
            market_share_weight_base = 5
            pax_weight_base = 15
            yield_weight_base = 15
            revenue_weight_base = 15
            fare_weight_base = 50

            total_base = market_share_weight_base + pax_weight_base + yield_weight_base + revenue_weight_base + fare_weight_base

            market_share_weight = float(market_share_weight_base)/total_base
            pax_weight = float(pax_weight_base)/total_base
            yield_weight = float(yield_weight_base)/total_base
            revenue_weight = float(revenue_weight_base)/total_base
            fare_weight = float(fare_weight_base)/total_base


            pipeline_for_effective_non = \
            [
                {
                    '$match': query
                }
                ,
                {
                    '$group':
                        {
                            '_id':
                                {
                                    # 'pos': '$pos',
                                    'od': '$od',
                                    'compartment': '$compartment'
                                },
                            'revenue': {'$sum': '$revenue_base'},
                            'pax': {'$sum': '$pax'}
                        }
                }
                ,
                {
                    '$project':
                        {
                            '_id': 0,
                            # 'pos': '$_id.pos',
                            'od': '$_id.od',
                            'compartment': '$_id.compartment',
                            'revenue': '$revenue',
                            'pax': '$pax'
                        }
                }
                ,
                {
                    '$lookup':
                        {
                            'from': 'JUP_DB_Target_OD',
                            'localField': 'od',
                            'foreignField': 'od',
                            'as': 'target'
                        }
                }
                ,
                {
                    '$unwind': '$target'
                }
                ,
                {
                    '$project':
                        {
                            '_id': 0,
                            'od': '$od',
                            # 'pos': '$pos',
                            'compartment': '$compartment',
                            'revenue': '$revenue',
                            'pax': '$pax',
                            'target_pos': '$target.pos',
                            'target_compartment': '$target.compartment',
                            'target_month': '$target.month',
                            'target_year': '$target.year',
                            'month_year_list': ['$target.month', '$target.year'],
                            'target_revenue': '$target.revenue',
                            'target_pax': '$target.pax'
                        }
                }
                ,
                {
                    '$redact':
                        {
                            '$cond':
                                {
                                    'if':
                                        {
                                            '$and':
                                                [
                                                    # {'$eq': ['$pos', '$target_pos']},
                                                    {'$eq': ['$compartment', '$target_compartment']},
                                                    {'$setIsSubset': [['$month_year_list'],
                                                                      month_year_list]}
                                                    # {'target_month':'$target_month',
                                                    #  'target_year':'$target_year'}
                                                    # {'$gte': ['$target_month', from_month]},
                                                    # {'$lte': ['$target_month', to_month]},
                                                    # {'$gte': ['$target_year', from_year]},
                                                    # {'$lte': ['$target_year', to_year]}
                                                ]
                                        },
                                    'then': '$$DESCEND',
                                    'else': '$$PRUNE'
                                }
                        }
                }
                ,
                {
                    '$group':
                        {
                            '_id':
                                {
                                    # 'pos': '$pos',
                                    'od': '$od',
                                    'compartment': '$compartment',
                                    'revenue': '$revenue',
                                    'pax': '$pax'
                                },
                            'target_revenue': {'$sum': '$target_revenue'},
                            'target_pax': {'$sum': '$target_pax'}
                        }
                }
                ,
                {
                    '$group':
                        {
                            '_id':
                                {
                                    'od': '$_id.od'
                                },
                            'target':
                                {
                                    '$push':
                                        {
                                            # 'pos': '$_id.pos',
                                            'target_revenue': '$target_revenue',
                                            'target_pax': '$target_pax',
                                            'compartment': '$_id.compartment',
                                            'revenue': '$_id.revenue',
                                            'pax': '$_id.pax'
                                        }
                                }
                        }
                }
                ,
                {
                    '$project':
                        {
                            '_id': 0,
                            'od': '$_id.od',
                            'target': '$target'
                        }
                }
                ,
                {
                    '$lookup':
                        {
                            'from': 'JUP_DB_OD_Distance_Master',
                            'localField': 'od',
                            'foreignField': 'od',
                            'as': 'distance'
                        }
                }
                ,
                {
                    '$unwind': '$distance'
                }
                ,
                {
                    '$project':
                        {
                            '_id': 0,
                            'od': '$od',
                            'target': '$target',
                            'distance': '$distance.distance'
                        }
                }
                ,
                {
                    '$lookup':
                        {
                            'from': 'JUP_DB_Market_Share_Last',
                            'localField': 'od',
                            'foreignField': 'od',
                            'as': 'market'
                        }
                }
                ,
                {
                    '$unwind': '$market'
                }
                ,
                {
                    '$project':
                        {
                            '_id': 0,
                            'od': '$od',
                            'distance': '$distance',
                            'target': '$target',
                            'market_pax': '$market.pax',
                            'market_revenue': '$market.revenue',
                            'market_compartment': '$market.compartment',
                            # 'market_pos': '$market.pos',
                            'market_month': '$market.month',
                            'market_year': '$market.year',
                            'market_airline': '$market.MarketingCarrier1'
                        }
                }
                ,
                {
                    '$unwind': '$target'
                }
                ,
                {
                    '$project':
                        {
                            '_id': 0,
                            'od': '$od',
                            'distance': '$distance',
                            'market_pax': '$market_pax',
                            'market_revenue': '$market_revenue',
                            'market_compartment': '$market_compartment',
                            # 'market_pos': '$market_pos',
                            'market_month': '$market_month',
                            'market_year': '$market_year',
                            'market_month_year_list':['$market_month','$market_year'],
                            'market_airline': '$market_airline',
                            # 'target_pos': '$target.pos',
                            'target_compartment': '$target.compartment',
                            'target_pax': '$target.pax',
                            'target_revenue': '$target.revenue',
                            'target_tgt_pax': '$target.target_pax',
                            'target_tgt_rev': '$target.target_revenue',
                            'yield':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$gt': ['$target.pax', 0]
                                                },
                                            'then':
                                                {
                                                    '$divide':
                                                        [
                                                            '$target.revenue',
                                                            {
                                                                '$multiply': ['$target.pax', '$distance']
                                                            }
                                                        ]
                                                },
                                            'else': 'NA'
                                        }
                                },
                            'target_yield':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$gt': ['$target.target_pax', 0]
                                                },
                                            'then':
                                                {
                                                    '$divide':
                                                        [
                                                            '$target.target_revenue',
                                                            {
                                                                '$multiply': ['$target.target_pax', '$distance']
                                                            }
                                                        ]
                                                },
                                            'else': 'NA'
                                        }
                                }

                        }
                }
                ,
                {
                    '$redact':
                        {
                            '$cond':
                                {
                                    'if':
                                        {
                                            '$and':
                                                [
                                                    # {'$gte': ['$market_month', from_month]},
                                                    # {'$lte': ['$market_month', to_month]},
                                                    # {'$eq':['$target_pos', '$market_pos']},
                                                    {'$eq': ['$target_compartment', '$market_compartment']},
                                                    {'$setIsSubset': [['$market_month_year_list'],
                                                                      month_year_list]}

                                                ]
                                        },
                                    'then': '$$DESCEND',
                                    'else': '$$PRUNE'
                                }
                        }
                }
                ,
                {
                    '$group':
                        {
                            '_id':
                                {
                                    # 'pos': '$target_pos',
                                    'od': '$od',
                                    'distance': '$distance',
                                    'compartment': '$target_compartment',
                                    'target_pax': '$target_tgt_pax',
                                    'sales_revenue': '$target_revenue',
                                    'target_revenue': '$target_tgt_rev',
                                    'sales_pax': '$target_pax',
                                    'yield': '$yield',
                                    'target_yield': '$target_yield'
                                },
                            'pax_host':
                                {
                                    '$sum':
                                        {
                                            '$cond':
                                                {
                                                    'if':
                                                        {
                                                            '$eq': ['$market_airline', host]
                                                        },
                                                    'then':  '$market_pax',
                                                    'else': 0
                                                }
                                        }
                                },
                            'pax_market': {'$sum': '$market_pax'},
                        }
                }
                ,
                {
                    '$project':
                        {
                            '_id': 0,
                            # 'pos': '$_id.pos',
                            'od': '$_id.od',
                            'distance': '$_id.distance',
                            'compartment': '$_id.compartment',
                            'target_pax': '$_id.target_pax',
                            'sales_revenue': '$_id.sales_revenue',
                            'target_revenue': '$_id.target_revenue',
                            'sales_pax': '$_id.sales_pax',
                            'yield': '$_id.yield',
                            'target_yield': '$_id.target_yield',
                            'market_share':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$gt': ['$pax_market', 0]
                                                },
                                            'then':
                                                {
                                                    '$multiply':
                                                        [
                                                            100,
                                                            {
                                                                '$divide': ['$pax_host', '$pax_market']
                                                            }
                                                        ]
                                                },
                                            'else': 'NA'
                                        }
                                },
                            'pax_vtgt':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$gt': ['$_id.target_pax', 0]
                                                },
                                            'then':
                                                {
                                                    '$multiply':
                                                        [
                                                            100,
                                                            {
                                                                '$divide':
                                                                    [
                                                                        {
                                                                            '$subtract': ['$_id.sales_pax', '$_id.target_pax']
                                                                        },
                                                                        '$_id.target_pax'
                                                                    ]
                                                            }
                                                        ]
                                                },
                                            'else': 'NA'
                                        }
                                },
                            'revenue_vtgt':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$gt': ['$_id.target_revenue', 0]
                                                },
                                            'then':
                                                {
                                                    '$multiply':
                                                        [
                                                            100,
                                                            {
                                                                '$divide':
                                                                    [
                                                                        {
                                                                            '$subtract': ['$_id.sales_revenue', '$_id.target_revenue']
                                                                        },
                                                                        '$_id.target_revenue'
                                                                    ]
                                                            }
                                                        ]
                                                },
                                            'else': 'NA'
                                        }
                                },
                            'yield_vtgt':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$gt': ['$_id.target_yield', 0]
                                                },
                                            'then':
                                                {
                                                    '$multiply':
                                                        [
                                                            100,
                                                            {
                                                                '$divide':
                                                                    [
                                                                        {
                                                                            '$subtract': ['$_id.yield', '$_id.target_yield']
                                                                        },
                                                                        '$_id.target_yield'
                                                                    ]
                                                            }
                                                        ]
                                                },
                                            'else': 'NA'
                                        }
                                }
                        }
                }
                ,
                {
                    '$project':
                        {
                            'od': '$od',
                            'distance': '$distance',
                            'compartment': '$compartment',
                            # 'pos': '$pos',
                            'sales_pax': '$sales_pax',
                            'target_revenue': '$target_revenue',
                            'sales_revenue': '$sales_revenue',
                            'target_pax': '$target_pax',
                            'pax_vtgt':'$pax_vtgt',
                            'revenue_vtgt': '$revenue_vtgt',
                            'yield_vtgt': '$yield_vtgt',
                            'market_share_vtgt':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$gt': [fare_market, 0]
                                                },
                                            'then':
                                                {
                                                    '$multiply':
                                                        [
                                                            100,
                                                            {
                                                                '$divide':
                                                                    [
                                                                        {
                                                                            '$subtract': ['$market_share', fare_market]
                                                                        },
                                                                        fare_market
                                                                    ]
                                                            }
                                                        ]
                                                },
                                            'else': 'NA'
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
                                    'od': '$od'
                                },
                            'details':
                                {
                                    '$push':
                                        {
                                            'sales_pax': '$sales_pax',
                                            'market_share': '$market_share',
                                            # 'pos': '$pos',
                                            'target_pax': '$target_pax',
                                            'sales_revenue': '$sales_revenue',
                                            'target_revenue': '$target_revenue',
                                            'compartment': '$compartment',
                                            'revenue_vtgt': '$revenue_vtgt',
                                            'pax_vtgt': '$pax_vtgt',
                                            'yield_vtgt': '$yield_vtgt',
                                            'market_share_vtgt': '$market_share_vtgt',
                                            'distance': '$distance',

                                        }
                                }
                        }
                }
                ,
                {
                    '$project':
                        {
                            '_id': 0,
                            'od': '$_id.od',
                            'details': '$details',
                            'origin': {'$substr': ['$_id.od', 0,3]}
                        }
                }
                ,
                {
                    '$lookup':
                        {
                            'from': 'JUP_DB_ATPCO_Fares',
                            'localField': 'origin',
                            'foreignField': 'origin',
                            'as': 'fare'
                        }
                }
                ,
                {
                    '$unwind': '$fare'
                }
                ,
                {
                    '$project':
                        {
                            'od': '$od',
                            # 'fare_pos': '$fare.pos',
                            'fare_destination': '$fare.destination',
                            'fare_compartment': '$fare.compartment',
                            'fare_fare': '$fare.fare',
                            'origin': '$origin',
                            'fare_currency': '$fare.currency',
                            'fare_fare_basis': '$fare.fare_basis',
                            'fare_effective_from': '$fare.effective_from',
                            'fare_effective_to': '$fare.effective_to',
                            'details': '$details'

                        }
                }
                ,
                {
                    '$unwind': '$details'
                }
                ,
                {
                    '$project':
                        {
                            'od': '$od',
                            'fare_pos': '$fare_pos',
                            'fare_destination': '$fare_destination',
                            'fare_compartment': '$fare_compartment',
                            'fare_fare': '$fare_fare',
                            'origin': '$origin',
                            'fare_currency': '$fare_currency',
                            'fare_fare_basis': '$fare_fare_basis',
                            'fare_effective_from': '$fare_effective_from',
                            'fare_effective_to': '$fare_effective_to',
                            'distance': '$details.distance',
                            'sales_pax': '$details.sales_pax',
                            'target_pax': '$details.target_pax',
                            'sales_revenue': '$details.sales_revenue',
                            'target_revenue': '$details.target_revenue',
                            'compartment': '$details.compartment',
                            # 'pos': '$details.pos',
                            'market_share': '$details.market_share',
                            'revenue_vtgt': '$details.revenue_vtgt',
                            'pax_vtgt': '$details.pax_vtgt',
                            'yield_vtgt': '$details.yield_vtgt',
                            'market_share_vtgt': '$details.market_share_vtgt',
                            'ave_fare_target':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$gt': ['$details.target_pax',0]
                                                },
                                            'then':
                                                {
                                                    '$divide': ['$details.target_revenue','$details.target_pax']
                                                },
                                            'else': 'NA'
                                        }
                                }

                        }
                }
                ,
                {
                    '$redact':
                        {
                            '$cond':
                                {
                                    'if':
                                        {
                                            '$and':
                                                [
                                                    # {'$eq': ['$pos', '$fare_pos']},
                                                    {'$eq': ['$compartment', '$fare_compartment']},
                                                    {
                                                        '$eq': ['$fare_destination',
                                                             {
                                                                 '$substr': ['$od', 3, 3]
                                                             }
                                                             ]
                                                    },
                                                    {'$gte': ['$fare_effective_to', SYSTEM_DATE]},
                                                    {'$lte': ['$fare_effective_from', SYSTEM_DATE]}

                                                ]
                                        },
                                    'then': '$$DESCEND',
                                    'else': '$$PRUNE'
                                }
                        }
                }
                ,
                {
                    '$project':
                        {
                            'od': '$od',
                            'compartment': '$compartment',
                            'fare_basis':'$fare_fare_basis',
                            'fare': '$fare_fare',
                            'fare_currency': '$fare_currency',
                            'revenue_vtgt': '$revenue_vtgt',
                            'pax_vtgt': '$pax_vtgt',
                            'market_share_vtgt': '$market_share_vtgt',
                            'yield_vtgt': '$yield_vtgt',
                            'fare_vtgt':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$gt': ['$ave_fare_target',0]
                                                },
                                            'then':
                                                {
                                                    '$multiply':
                                                        [
                                                            100,
                                                            {
                                                                '$divide':
                                                                    [
                                                                        {
                                                                            '$subtract': ['$fare_fare', '$ave_fare_target']
                                                                        },
                                                                        '$ave_fare_target'
                                                                    ]
                                                            }
                                                        ]
                                                },
                                            'else': 'NA'
                                        }
                                }
                        }
                }
                ,
                {
                    '$project':
                        {
                            'od': '$od',
                            'compartment': '$compartment',
                            'fare_basis': '$fare_basis',
                            'fare': '$fare',
                            'fare_currency': '$fare_currency',
                            'revenue_vtgt': '$revenue_vtgt',
                            'pax_vtgt': '$pax_vtgt',
                            'market_share_vtgt': '$market_share_vtgt',
                            'yield_vtgt': '$yield_vtgt',
                            'fare_vtgt': '$fare_vtgt',
                            'effectiveness':
                                {
                                    '$divide':
                                        [
                                            {
                                                '$add':
                                                    [
                                                        {
                                                            '$multiply': ['$pax_vtgt', pax_weight]
                                                        },
                                                        {
                                                            '$multiply': ['$yield_vtgt', yield_weight]
                                                        },
                                                        {
                                                            '$multiply': ['$revenue_vtgt', revenue_weight]
                                                        },
                                                        {
                                                            '$multiply': ['$fare_vtgt', fare_weight]
                                                        },
                                                        {
                                                            '$multiply': ['$market_share_vtgt', market_share_weight]
                                                        }
                                                    ]
                                            },
                                            5
                                        ]
                                }
                        } # use $out : result collection name
                }
            ]

            crsr_effective_ineffective_fares = db.JUP_DB_Sales.aggregate(pipeline_for_effective_non)
            list_effective_ineffective_fares = list(crsr_effective_ineffective_fares)
            effective_fares = 0
            ineffective_fares = 0
            total_fares = 0
            if len(list_effective_ineffective_fares) != 0:
                for doc in list_effective_ineffective_fares:
                    total_fares += 1
                    if doc['effectiveness'] > 0:
                        effective_fares += 1
                    else:
                        ineffective_fares += 1
            else:
                effective_fares = 0
                ineffective_fares = 0
            response = dict()
            response['effective_fares'] = effective_fares
            response['ineffective_fares'] = ineffective_fares
            return response
        else: # collection/s to query not found in db
            if 'JUP_DB_Target_OD' not in db.collection_names():
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupiter_AI/tiles/kpi/effective_ineffective_fare_grid.py method: effective_ineffective_fares',
                                                    get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list('Collection JUP_DB_Target_OD cannot be found in the database.')
                obj_error.write_error_logs(datetime.datetime.now())

            if 'JUP_DB_Sales' not in db.collection_names():
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupiter_AI/tiles/kpi/effective_ineffective_fare_grid.py method: effective_ineffective_fares',
                                                    get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list('Collection JUP_DB_Sales cannot be found in the database.')
                obj_error.write_error_logs(datetime.datetime.now())

            if 'JUP_DB_ATPCO_Fares' not in db.collection_names():
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupiter_AI/tiles/kpi/effective_ineffective_fare_grid.py method: effective_ineffective_fares',
                                                    get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list('Collection JUP_DB_ATPCO_Fares cannot be found in the database.')
                obj_error.write_error_logs(datetime.datetime.now())

            if 'JUP_DB_Market_Share_Last' not in db.collection_names():
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupiter_AI/tiles/kpi/effective_ineffective_fare_grid.py method: effective_ineffective_fares',
                                                    get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list('Collection JUP_DB_Market_Share_Last cannot be found in the database.')
                obj_error.write_error_logs(datetime.datetime.now())

            if 'JUP_DB_OD_Distance_Master' not in db.collection_names():
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupiter_AI/tiles/kpi/effective_ineffective_fare_grid.py method: effective_ineffective_fares',
                                                    get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list('Collection JUP_DB_OD_Distance_Master cannot be found in the database.')
                obj_error.write_error_logs(datetime.datetime.now())

    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupiter_AI/tiles/kpi/effective_ineffective_fare_grid.py method: effective_ineffective_fares',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


def get_tiles(afilter):
    response = dict()
    response['effective_ineffective_fares'] = effective_ineffective_fares(afilter)
    return response

if __name__ == '__main__':
    a = {
        'region': [],
        'country': [],
        'pos': [],
        'origin': [],
        'destination': [],
        'compartment': [],
        'fromDate': '2017-01-01',
        'toDate': '2017-01-12'
    }
    start_time = time.time()
    print get_tiles(afilter=a)
    # market_outlook = db.get_collection(collection_name)
    # market_outlook.drop()
    print (time.time() - start_time)