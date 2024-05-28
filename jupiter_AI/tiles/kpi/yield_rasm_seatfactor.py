"""
Mr. Author, Please include header!

MODIFICATIONS LOG
    S.No                   :    1
    Date Modified          :    2016-12-28
    By                     :    Shamail Mulla
    Modification Details   :    Added error / exception handling codes
"""
from jupiter_AI.common import ClassErrorObject as error_class
from jupiter_AI import client,JUPITER_DB
db = client[JUPITER_DB]
try:
    import json
    from collections import defaultdict
    from copy import deepcopy
    import inspect
    import pymongo
    import time
    import datetime
    import json, ast


    from jupiter_AI.network_level_params import Host_Airline_Hub as host
    from jupiter_AI.tiles.jup_common_functions_tiles import gen_collection_name as gen

    result_collection_name_rasm = gen()
    result_collection_name_yield = gen()
    result_collection_test_yield = 'JUP_DB_Demo_KPI_Yield'
    result_collection_test_rasm_seat_factor = 'JUP_DB_Demo_KPI_rasm_sf'

    yield_collex = db.get_collection(result_collection_test_yield)
    yield_collex.drop()

    rasm_collex = db.get_collection(result_collection_test_rasm_seat_factor)
    rasm_collex.drop()
except:
    pass


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


def yield_pricebiometrics(afilter):
    try:
        # create the query in a different function
        afilter = deepcopy(defaultdict(list, afilter)) # use proper variable names
        query = dict()
        response = dict()
        flag_for_collection = 0
        if afilter['region']:
            query['region'] = {'$in': afilter['region']}
            flag_for_collection = 1
        if afilter['country']:
            query['country'] = {'$in': afilter['country']}
            flag_for_collection = 1
        if afilter['pos']:
            query['pos'] = {'$in': afilter['pos']}
            flag_for_collection = 1
        if afilter['compartment']:
            query['compartment'] = {'$in': afilter['compartment']}
        if afilter['origin']:
            # print 'origin present in filter'
            if flag_for_collection == 0:
                od_build = []
                for idx, item in enumerate(afilter['origin']):
                    if item != host and afilter['destination'][idx] != host:
                        od1 = ''.join(item + host)
                        od_build.append({'od': od1})
                        od2 = ''.join(host + afilter['destination'][idx])
                        od_build.append({'od': od2})
                    else:
                        od = ''.join(item + afilter['destination'][idx])
                        od_build.append({'od': od})
                    od_build = [i for n, i in enumerate(od_build) if i not in od_build[n + 1:]]
                query['$or'] = od_build
            elif flag_for_collection == 1:
                od_build = []
                od = []
                for idx, item in enumerate(afilter['origin']):
                    od_build.append({'origin': item, 'destination': afilter['destination'][idx]})
                for i in od_build:
                    a = i['origin'] + i['destination']
                    od.append({'od': a})
                # print od
                query['$or'] = od
        # print 'generic query built'
        if flag_for_collection == 0:
            if 'JUP_DB_Flight_Leg' in db.collection_names():
                query['dept_dt'] = {'$gte': afilter['fromDate'], '$lte': afilter['toDate']}
                # print 'Flight_leg collection being accessed'
                apipeline_user = [ # use proper variable names
                    {
                        '$match': query
                    },
                    {
                        '$group': {'_id': {'od': '$od'},
                                   'revenue': {'$sum': '$revenue'},
                                   'rpkm': {'$sum': '$rpkm'},
                                   'revenue_lyr': {'$sum': '$revenue_1'},
                                   'rpkm_lyr': {'$sum': '$rpkm_1'},
                                   'pax': {'$sum': '$pax'},
                                   'pax_lyr': {'$sum': '$pax_1'}
                                   }
                    }
                    ,
                    {
                        '$lookup':
                            {
                                'from': 'JUP_DB_OD_Distance_Master',
                                'localField': '_id.od',
                                'foreignField': 'od',
                                'as': 'for_distance'
                            }
                    }
                    ,
                    {
                        '$unwind': '$for_distance'
                    }
                    ,
                    {
                        '$project':
                            {
                                '_id': 0,
                                 'OD': '$_id.od',
                                 'Revenue': '$revenue',
                                 'Pax': '$pax',
                                 'Revenue_lyr': '$revenue_lyr',
                                 'Pax_lyr': '$pax_lyr',
                                 'Distance': '$for_distance.distance'
                            }

                    }
                    ,
                    {
                        '$redact':
                            {'$cond':
                                {
                                    'if': {'$gt': ['$Distance', 0]},
                                    'then': '$$DESCEND',
                                    'else': '$$PRUNE'
                                }
                            }
                    }
                    ,
                    {
                        '$project':
                            {
                                '_id': 0,
                                'OD': '$OD',
                                'Revenue': '$Revenue',
                                'Pax': '$Pax',
                                'Revenue_lyr': '$Revenue_lyr',
                                'Pax_lyr': '$Pax_lyr',
                                'Distance': '$Distance',
                                'rpkm':
                                    {
                                        '$multiply': ['$Distance', '$Pax']
                                    },
                                'rpkm_ly':
                                    {
                                        '$multiply': ['$Distance', '$Pax_lyr']
                                    }
                            }
                    }
                    ,
                    {
                        '$group':
                            {
                                '_id': None,
                                'tot_rpkm': {'$sum': '$rpkm'},
                                'tot_revenue': {'$sum': '$Revenue'},
                                'tot_rpkm_ly': {'$sum': '$rpkm_ly'},
                                'tot_revenue_ly': {'$sum': '$Revenue_lyr'}


                            }
                    }
                    ,
                    {
                        '$project':
                            {
                                '_id': 0,
                                'yield':
                                    {
                                        '$cond':
                                            {
                                                'if':
                                                    {
                                                        '$gt': ['$tot_rpkm', 0]
                                                    },
                                                'then':
                                                    {
                                                        '$multiply':
                                                            [
                                                                100,
                                                                {
                                                                    '$divide':
                                                                        [
                                                                            '$tot_revenue',
                                                                            '$tot_rpkm'
                                                                        ]
                                                                }
                                                            ]
                                                    },
                                                'else': 'NA'
                                            }
                                    },
                                'yield_lyr':
                                    {
                                        '$cond':
                                            {
                                                'if':
                                                    {
                                                        '$gt': ['$tot_rpkm_ly', 0]
                                                    },
                                                'then':
                                                    {
                                                        '$multiply':
                                                            [
                                                                100,
                                                                {
                                                                    '$divide':
                                                                        [
                                                                            '$tot_revenue_ly',
                                                                            '$tot_rpkm_ly'
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
                        '$addFields':
                            {
                                'yield_vlyr':
                                    {
                                        '$cond':
                                            {
                                                'if':
                                                    {
                                                        '$and':
                                                            [
                                                                {'$gt': ['$yield_lyr', 0]},
                                                                {'$ne': [{'$type': '$yield_lyr'}, 'string']}
                                                            ]
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
                                                                                '$subtract': ['$yield', '$yield_lyr']
                                                                            },
                                                                            '$yield_lyr'
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
                        '$out': result_collection_test_yield
                    }

                ]
                db.JUP_DB_Flight_Leg.aggregate(apipeline_user, allowDiskUse=True)
                # print 'Flight Leg aggregate completed'
                '''
                    if result_collection_name in db.collection_names():
                        result_coll_present = 1
                '''
            else:  # JUP_DB_Flight_Leg Collection to query is not present
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupter_AI/tiles/kpi/yield_rasm_seatfactor.py method: yield_pricebiometrics',
                                                    get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list('Collection JUP_DB_Flight_Leg cannot be found in the database.')
                obj_error.write_error_logs(datetime.datetime.now())
                raise obj_error
        else: # if there is region, country or pos
            if 'JUP_DB_Sales' in db.collection_names():
                coll_to_query_present = 1
                # print 'JUP DB sales being accessed'
                query['dep_date'] = {'$gte': afilter['fromDate'], '$lte': afilter['toDate']}
                # print query
                apipeline_user = [ # use proper variable names
                    # Please explain what is going on
                    {
                        '$match': query
                    }
                    ,
                    {
                        '$group':
                            {
                                '_id':
                                    {
                                        'od': '$od'
                                    },
                                   'revenue': {'$sum': '$revenue_base'},
                                   'pax': {'$sum': '$pax'},
                                   'revenue_lyr': {'$sum': '$revenue_base_1'},
                                   'pax_lyr': {'$sum': '$pax_1'}
                            }
                    }
                    ,
                    {
                        '$lookup':
                            {
                                'from': 'JUP_DB_OD_Distance_Master',
                                'localField': '_id.od',
                                'foreignField': 'od',
                                'as': 'for_distance'
                            }
                    }
                    ,
                    {
                        '$unwind': '$for_distance'
                    }
                    ,
                    {
                        '$project':
                            {
                                '_id': 0,
                                'od': '$_id.od',
                                'revenue': '$revenue',
                                'distance': '$for_distance.distance',
                                'pax': '$pax',
                                'revenue_lyr': '$revenue_lyr',
                                'pax_lyr': '$pax_lyr'

                            }
                    }
                    ,
                    {
                        '$project': {'_id': 0,
                                     'od': '$od',
                                     'Revenue': '$revenue',
                                     'Pax': '$pax',
                                     'Revenue_lyr': '$revenue_lyr',
                                     'Pax_lyr': '$pax_lyr',
                                     'Distance': '$distance',
                                     'Yield': {
                                         '$cond':
                                             {
                                                 'if': {'$gt': ['$revenue', 0]},
                                                 'then': {
                                                     '$cond':
                                                         {
                                                             'if': {'$gt': [{'$multiply': ['$pax', '$distance']}, 0]},
                                                             'then': {'$multiply': [100, {'$divide': ['$revenue',
                                                                                                      {'$multiply': ['$pax',
                                                                                                                     '$distance']}]}]},
                                                             'else': 0
                                                         }
                                                 },
                                                 'else': 0
                                             }
                                     },
                                     'Yield_lyr': {
                                         '$cond': {
                                             'if': {'$gt': [{'$multiply': ['$pax_lyr', '$distance']}, 0]},
                                             'then': {
                                                 '$cond': {
                                                     'if': {'$gt': ['$revenue_lyr', 0]},
                                                     'then': {'$multiply': [100, {'$divide': ['$revenue_lyr',
                                                                                              {'$multiply': ['$pax_lyr',
                                                                                                             '$distance']}]}]},
                                                     'else': 0
                                                 }
                                             },
                                             'else': 0
                                         }
                                     }
                                     }
                    }
                    ,
                    {
                        '$project':
                            {
                                '_id': 0,
                                'OD': '$od',
                                'Revenue': '$Revenue',
                                'Pax': '$Pax',
                                'Revenue_lyr': '$Revenue_lyr',
                                'Pax_lyr': '$Pax_lyr',
                                'Distance': '$Distance',
                                'Yield': '$Yield',
                                'Yield_lyr': '$Yield_lyr',
                                'rpkm':
                                    {
                                        '$multiply': ['$Distance', '$Pax']
                                    },
                                'rpkm_ly':
                                    {
                                        '$multiply': ['$Distance', '$Pax_lyr']
                                    },
                                'Yield_Vlyr':
                                    {
                                        '$cond':
                                            {
                                                'if':
                                                    {
                                                        '$gt': ['$Yield_lyr', 0]
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
                                                                                '$subtract':
                                                                                    [
                                                                                        '$Yield',
                                                                                        '$Yield_lyr'
                                                                                    ]
                                                                            },
                                                                            '$Yield_lyr'
                                                                        ]
                                                                }
                                                            ]
                                                    },
                                                'else': 'Last year value NA'
                                            }
                                    }
                            }
                    }
                    ,
                    {
                        '$group':
                            {
                                '_id': None,
                                'tot_rpkm': {'$sum': '$rpkm'},
                                'tot_revenue': {'$sum': '$Revenue'},
                                'tot_rpkm_ly': {'$sum': '$rpkm_ly'},
                                'tot_revenue_ly': {'$sum': '$Revenue_lyr'}

                            }
                    }
                    ,
                    {
                        '$project':
                            {
                                '_id': 0,
                                'yield':
                                    {
                                        '$cond':
                                            {
                                                'if':
                                                    {
                                                        '$gt': ['$tot_rpkm', 0]
                                                    },
                                                'then':
                                                    {
                                                        '$multiply':
                                                        [
                                                            100,
                                                            {
                                                                '$divide':
                                                                    [
                                                                        '$tot_revenue',
                                                                        '$tot_rpkm'
                                                                    ]
                                                            }
                                                        ]
                                                    },
                                                'else': 'NA'
                                            }
                                    },
                                'yield_lyr':
                                    {
                                        '$cond':
                                            {
                                                'if':
                                                    {
                                                        '$gt': ['$tot_rpkm_ly', 0]
                                                    },
                                                'then':
                                                    {
                                                        '$multiply':
                                                            [
                                                                100,
                                                                {
                                                                    '$divide':
                                                                        [
                                                                            '$tot_revenue_ly',
                                                                            '$tot_rpkm_ly'
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
                        '$addFields':
                            {
                                'yield_vlyr':
                                    {
                                        '$cond':
                                            {
                                                'if':
                                                    {
                                                        '$and':
                                                            [
                                                                {'$gt': ['$yield_lyr',0]},
                                                                {'$ne': [{'$type': '$yield_lyr'}, 'string']}
                                                            ]
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
                                                                                '$subtract': ['$yield', '$yield_lyr']
                                                                            },
                                                                            '$yield_lyr'
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
                        '$out': result_collection_test_yield
                    }

                ]
                db.JUP_DB_Sales.aggregate(apipeline_user, allowDiskUse=True)
                # print 'JUP DB sales being aggregatde for yield'
                # use proper variable names
                '''
                    if result_collection_name in db.collection_names():
                        result_coll_present = 1
                '''
            else: # JUP_DB_Sales collection to query not found in db
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupter_AI/tiles/kpi/yield_rasm_seatfactor.py method: yield_pricebiometrics',
                                                    get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list('Collection JUP_DB_Sales cannot be found in the database.')
                obj_error.write_error_logs(datetime.datetime.now())
                raise obj_error

        # if result_collection_name_yield in db.collection_names():
        if result_collection_test_yield in db.collection_names():
            # print 'aggregation successful result collection created'
            # yield_collection = db.get_collection(result_collection_name_yield)
            yield_collection = db.get_collection(result_collection_test_yield)
            if yield_collection.count > 0:
                # print 'result collection contains documents'
                list_yield = list(yield_collection.find())
                # revenue_aggregated = 0
                # revenue_lyr_aggregated = 0
                # rpkm_aggregated = 0
                # rpkm_lyr_aggregated = 0
                # for i in list_yield:
                #     # print i
                #     revenue_aggregated += i['Revenue']
                #     revenue_lyr_aggregated += i['Revenue_lyr']
                #     rpkm_aggregated += (i['Distance']*i['Pax'])
                #     rpkm_lyr_aggregated += (i['Distance'] * i['Pax_lyr'])
                # # print rpkm_aggregated
                # yield_aggregated = (float(revenue_aggregated)/rpkm_aggregated)*100
                #
                # if rpkm_lyr_aggregated != 0:
                #     yield_lyr_aggregated = float(revenue_lyr_aggregated)/rpkm_lyr_aggregated
                # else:
                #     yield_lyr_aggregated = 0
                #
                # if yield_lyr_aggregated != 0:
                #     yield_vlyr = (float(yield_aggregated - yield_lyr_aggregated)/yield_lyr_aggregated)*100
                # else:
                #     yield_vlyr = 'NA'


                # print 'yield is ' + str(yield_aggregated)
                # print 'yield vlyr is ' + str(yield_vlyr)

                # print 'yield is ' + str(list_yield[0]['yield'])
                # print 'yield vlyr is ' + str(list_yield[0]['yield_vlyr'])

                # response = \
                #     {
                #         'Yield': yield_aggregated,
                #         'Yield_VLYR': yield_vlyr
                #     }
                # yield_collection.drop()

                response = \
                    {
                        'Yield': list_yield[0]['yield'],
                        'Yield_VLYR': list_yield[0]['yield_vlyr']
                    }

            else: # result is empty collection
                # yield_collection.drop()
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupter_AI/tiles/kpi/yield_rasm_seatfactor.py method: yield_pricebiometrics',
                                                    get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list('There is no data.')

                obj_error.write_error_logs(datetime.datetime.now())

        else: # resultant collection not created in the db
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/kpi/yield_rasm_seatfactor.py method: yield_pricebiometrics',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('result_collection_yield not created in the database. Check aggregate pipeline.')

            obj_error.write_error_logs(datetime.datetime.now())

        return response
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/kpi/yield_rasm_seatfactor.py method: yield_pricebiometrics',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


def rasm_seatfactor(afilter):
    try:
        if 'JUP_DB_Flight_Leg' in db.collection_names():
            # print 'JUP Flight leg present'
            afilter = deepcopy(defaultdict(list, afilter))
            query = dict()  # use proper variable names
            query1 = dict()  # use proper variable names
            response = dict()
            flag = 0  # use proper variable names
            if afilter['region']:
                query1['region'] = {'$in': afilter['region']}
                flag = 1
            if afilter['country']:
                query1['country'] = {'$in': afilter['country']}
                flag = 1
            if afilter['pos']:
                query1['pos'] = {'$in': afilter['pos']}
                flag = 1
            if afilter['compartment']:
                query1['compartment'] = {'$in': afilter['compartment']}
            if afilter['origin']:
                # print 'orign present in the filter'
                if flag == 0:
                    # print 'region country pos not present in filter'
                    od_build = []
                    for idx, item in enumerate(afilter['origin']): # use proper variable names for 'idx' and 'item'
                        if item != host and afilter['destination'][idx] != host:
                            od1 = ''.join(item + host)
                            od_build.append({'od': od1})
                            od2 = ''.join(host + afilter['destination'][idx])
                            od_build.append({'od': od2})
                        else:
                            od = ''.join(item + afilter['destination'][idx])
                            od_build.append({'od': od})
                        od_build = [i for n, i in enumerate(od_build) if i not in od_build[n + 1:]]
                    query['$or'] = od_build
                elif flag == 1:
                    # print 'region country pos present in filter'
                    od_build1 = []
                    od = []
                    for idx, item in enumerate(afilter['origin']):
                        od_build1.append({'origin': item, 'destination': afilter['destination'][idx]})
                    for i in od_build1:
                        a = i['origin'] + i['destination']
                        od.append({'od': a})
                    # print od
                    query1['$or'] = od
            if flag == 1:
                # print 'pre aggregation pipeline check - region country pos present in filter'
                query1['dep_date'] = {'$gte': afilter['fromDate'], '$lte': afilter['toDate']}
                apipeline_user1 = [ # use proper variable names
                    {
                        '$match': query1
                    },
                    {
                        '$group': {'_id': {'od':'$od'},
                                   'pax': {'$sum': '$pax'},
                                    }
                    }

                ]
                cursor_user1 = db.JUP_DB_Sales.aggregate(apipeline_user1) # use proper variable names
                # print 'aggregate to idenitfy ods and pax succesfully completed'
                data_user1 = list(cursor_user1) # use proper variable names
                od_build2 = []
                if len(data_user1) != 0:
                    for i in data_user1: # use proper variable names for 'i'
                        prefindhost = i['_id']['od']
                        findhost = ast.literal_eval(json.dumps(prefindhost))
                        # print findhost
                        if host not in findhost:
                            # print 'hello 5'
                            od1 = ''.join(findhost[0:3] + host)
                            od_build2.append({'od': od1})
                            od2 = ''.join(host + findhost[3:6])
                            od_build2.append({'od': od2})
                        else:
                            od_build2.append({'od': findhost})
                else:
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                        'jupiter_AI/tiles/kpi/yield_rasm_seatfactor.py method: rasm_seatfactor',
                                                        get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list(
                        str('aggregate to identify list of ods completed however 0 documents were identified'))

                    obj_error.write_error_logs(datetime.datetime.now())

                query['$or'] = od_build2
            query['dept_dt'] = {'$gte': afilter['fromDate'], '$lte': afilter['toDate']}
            # print query
            a_pipeline_user_simple = [
                {
                    '$match': query
                }
                ,
                {
                    '$group':
                        {
                            '_id': None,
                            'revenue': {'$sum': '$revenue'},
                            'rpkm': {'$sum': '$rpkm'},
                            'askm': {'$sum': '$askm'}

                        }
                }
                ,
                {
                    '$project':
                        {
                            '_id': 0,
                            'seat_factor':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$gt': ['$askm',0]
                                                },
                                            'then':
                                                {
                                                    '$multiply':
                                                        [
                                                            100,
                                                            {
                                                                '$divide':
                                                                    [
                                                                        '$rpkm',
                                                                        '$askm'
                                                                    ]
                                                            }
                                                        ]
                                                },
                                            'else': 'NA'
                                        }
                                },
                            'rasm':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$gt': ['$askm', 0]
                                                },
                                            'then':
                                                {
                                                    '$multiply':
                                                        [
                                                            100,
                                                            {
                                                                '$divide':
                                                                    [
                                                                        '$revenue',
                                                                        '$askm'
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
                    '$out': result_collection_test_rasm_seat_factor
                }

            ]

            db.JUP_DB_Flight_Leg.aggregate(a_pipeline_user_simple, allowDiskUse=True)

            # print query

            if result_collection_test_rasm_seat_factor in db.collection_names():
                rasm_sf = db.get_collection(result_collection_test_rasm_seat_factor)
                # print 'JUP_DB_FLight leg aggregation to identify seat factor and rasm successfully completed. Resultant collection created'

                if rasm_sf.count > 0:
                    # print 'resultant collection contains at least one document'
                    list_rasm = list(rasm_sf.find())
                    # rasm_sf.drop()
                    # print list_rasm
                    # print 'rasm is ' + str(list_rasm[0]['rasm'])
                    # print 'seat factor is ' + str(list_rasm[0]['seat_factor'])
                    response = \
                        {
                            'RASM': list_rasm[0]['rasm'],
                            'Seat Factor': list_rasm[0]['seat_factor']
                        }

                else:
                    # rasm_sf.drop()
                    # result is empty collection
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                        'jupiter_AI/tiles/kpi/yield_rasm_seatfactor.py method: rasm_seatfactor',
                                                        get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list('There is no data.')

                    obj_error.write_error_logs(datetime.datetime.now())

            else: # resultant collection not created in the db
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupiter_AI/tiles/kpi/yield_rasm_seatfactor.py method: rasm_seatfactor',
                                                    get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list('result_collection_rasm not created in the database. Check aggregate pipeline.')

                obj_error.write_error_logs(datetime.datetime.now())

            # data_user = list(cursor_user)

            return response
        else: # collection to query not found in db
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupiter_AI/tiles/kpi/yield_rasm_seatfactor.py method: rasm_seatfactor',
                                                    get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list('Collection JUP_DB_Flight_Leg cannot be found in the database.')
                obj_error.write_error_logs(datetime.datetime.now())
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/kpi/yield_rasm_seatfactor.py method: rasm_seatfactor',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


def get_tiles(afilter):
    response = dict()
    response['Yield'] = yield_pricebiometrics(afilter)
    response['RASM_SeatFactor'] = rasm_seatfactor(afilter)
    return response


if __name__ == '__main__':
    st = time.time() # use proper variable names, start_time
    # use proper variable names
    a = {
        'region': ['GCC'],
        'country': [],
        'pos': [],
        'origin': [],
        'destination': [],
        'compartment': [],
        'fromDate': '2016-10-10',
        'toDate': '2016-10-10'
    }
    print get_tiles(afilter=a)
    print time.time() - st



