"""
"""
try:

    import jupiter_AI.common.ClassErrorObject as errorClass
    from jupiter_AI.network_level_params import SYSTEM_DATE
    from jupiter_AI import client, JUPITER_DB
    db = client[JUPITER_DB]
    from jupiter_AI.common.convert_currency import convert_currency
    from jupiter_AI.network_level_params import na_value
    from jupiter_AI.network_level_params import Host_Airline_Code
    from jupiter_AI.network_level_params import query_month_year_builder
    from copy import deepcopy
    from collections import defaultdict
except:
    pass

import inspect
import time

"""
Try to break up the working into smaller functions like for query_builder
Split file into DAL and BLL, all python processing to be done in BLL
Build and fire the queries in DAL
Leave sufficient lines when a piece of logic is completed
"""

'''
4 tiles
    total fares available
    total effective fares
    total host  pax
    total user pax
'''
'''
    rev/rev target
    pax/pax target
    ms/ms target
    avg fare
    yield
'''


def get_module_name():  # gives method name
    return inspect.stack()[1][3]


def get_arg_lists(frame):  # gives argument list for a given method
    args, _, _, values = inspect.getargvalues(frame)
    argument_name_list = []
    argument_value_list = []
    for k in args:
        argument_name_list.append(k)
        argument_value_list.append(values[k])
    return argument_name_list, argument_value_list


def build_qry_fares(afilter):
    """
    """
    afilter = deepcopy(defaultdict(list, afilter))
    query = defaultdict(list)
    response = dict()
    if afilter['region']:
        query['$and'].append({'region':{'$in': afilter['region']}})
    if afilter['country']:
        query['$and'].append({'country':{'$in': afilter['country']}})
    if afilter['pos']:
        query['$and'].append({'pos': {'$in': afilter['pos']}})
    if afilter['origin']:
        od_build = []
        for idx, item in enumerate(afilter['origin']):
            od_build.append({'origin': item, 'destination': afilter['destination'][idx]})
        query['$and'].append({'$or':od_build})
    if afilter['compartment']:
        query['$and'].append({'compartment': {'$in': afilter['compartment']}})
    if afilter['rbd']:
        query['and'].append({'rbd':{'$in': afilter['rbd']}})
    if afilter['farebasis']:
        query['$and'].append({'fare_basis': {'$in': afilter['farebasis']}})
    query['$and'].append({'dep_date': {'$gte':afilter['fromDate'],'$lte':afilter['toDate']}})
    # query['departure_date_from'] = {'$lte': afilter['toDate']}
    # query['departure_date_to'] = {'$gte': afilter['fromDate']}
    # query['$and'].append({'$or':[{'effective_from':{'$lte': SYSTEM_DATE}},{'effective_from':{'$eq': None}}]})
    # query['$and'].append({'$or':[{'effective_to':{'$gte': SYSTEM_DATE}},{'effective_to':{'$eq': None}}]})
    # query['$and'].append({'$or':[{'departure_date_from':{'$lte': afilter['toDate']}},{'departure_date_from':{'$eq': None}}]})
    # query['$and'].append({'$or':[{'departure_date_to':{'$gte': afilter['fromDate']}},{'departure_date_to':{'$eq': None}}]})
    return query

def build_qry_sales_effective(afilter):
    """
    """
    afilter = deepcopy(defaultdict(list, afilter))
    query = defaultdict(list)
    response = dict()
    if afilter['region']:
        query['$and'].append({'region':{'$in': afilter['region']}})
    if afilter['country']:
        query['$and'].append({'country':{'$in': afilter['country']}})
    if afilter['pos']:
        query['$and'].append({'pos': {'$in': afilter['pos']}})
    if afilter['origin']:
        od_build = []
        for idx, item in enumerate(afilter['origin']):
            od_build.append({'origin': item, 'destination': afilter['destination'][idx]})
        query['$and'].append({'$or':od_build})
    if afilter['compartment']:
        query['$and'].append({'compartment': {'$in': afilter['compartment']}})
    if afilter['rbd']:
        query['and'].append({'rbd':{'$in': afilter['rbd']}})
    if afilter['farebasis']:
        query['$and'].append({'fare_basis': {'$in': afilter['farebasis']}})
    query['$and'].append({'dep_date': {'$gte':afilter['fromDate'],'$lte':afilter['toDate']}})
    return query

def build_qry_sales(afilter):
    """
    """
    afilter = deepcopy(defaultdict(list, afilter))
    query = defaultdict(list)
    response = dict()
    if afilter['region']:
        query['$and'].append({'region':{'$in': afilter['region']}})
    if afilter['country']:
        query['$and'].append({'country':{'$in': afilter['country']}})
    if afilter['pos']:
        query['$and'].append({'pos': {'$in': afilter['pos']}})
    if afilter['origin']:
        od_build = []
        for idx, item in enumerate(afilter['origin']):
            od_build.append({'origin': item, 'destination': afilter['destination'][idx]})
        query['$and'].append({'$or':od_build})
    if afilter['compartment']:
        query['$and'].append({'compartment': {'$in': afilter['compartment']}})
    if afilter['rbd']:
        query['and'].append({'rbd':{'$in': afilter['rbd']}})
    if afilter['farebasis']:
        query['$and'].append({'fare_basis': {'$in': afilter['farebasis']}})
    query['$and'].append({'dep_date': {'$gte':afilter['fromDate'],'$lte':afilter['toDate']}})
    return query

def get_effective_ineffective_fares(afilter):
    """
    """
    qry_sales_effective = build_qry_sales_effective(afilter)
    print qry_sales_effective
    """
    # ppln_effective_fares = [
    #     # // Stage 1
    #     {
    #         '$match': qry_fares_collection
    #     }
    #     ,
    #     {
    #         '$group': {
    #             '_id': {
    #                 'origin': '$origin',
    #                 'destination': '$destination'
    #             },
    #             'farebasis_details': {
    #                 '$push': "$$ROOT"
    #             }
    #         }
    #     },
    #     # {
    #     #     '$project': {
    #     #         '_id':0,
    #     #         'od': {'$concat':['$_id.origin','$_id.destination']},
    #     #         'farebasis_details': '$farebasis_details'
    #     #     }
    #     # },
    #     # # // Stage 2
    #     # {
    #     #     '$lookup': {
    #     #         "from" : "JUP_DB_Sales",
    #     #         "localField" : "od",
    #     #         "foreignField" : "od",
    #     #         "as" : "sales"
    #     #     }
    #     # },
    #     # {
    #     #     '$unwind': '$farebasis_details'
    #     # },
    #     # // Stage 3
    #     # {
    #     #     # '$addFields': {
    #     #     '$project': {
    #     #         "_id":0,
    #     #         "source": "$farebasis_details.source",
    #     #         "effective_from": "$farebasis_details.effective_from",
    #     #         "effective_to": "$farebasis_details.effective_to",
    #     #         "departure_date_from": "$farebasis_details.departure_date_from",
    #     #         "departure_date_to": "$farebasis_details.departure_date_to",
    #     #         "fare_basis": "$farebasis_details.fare_basis",
    #     #         "RBD": "$farebasis_details.RBD",
    #     #         "fare_rule": "$farebasis_details.fare_rule",
    #     #         "tariff_code": "$farebasis_details.tariff_code",
    #     #         "footnote": "$farebasis_details.footnote",
    #     #         "rtg": "$farebasis_details.rtg",
    #     #         "batch": "$farebasis_details.batch",
    #     #         "region": "$farebasis_details.region",
    #     #         "country": "$farebasis_details.country",
    #     #         "pos": "$_id.pos",
    #     #         "origin": "$farebasis_details.origin",
    #     #         "destination": "$farebasis_details.destination",
    #     #         "compartment": "$farebasis_details.compartment",
    #     #         "oneway_return": "$farebasis_details.oneway_return",
    #     #         "channel": "$farebasis_details.channel",
    #     #         "carrier": "$farebasis_details.carrier",
    #     #         "fare": "$farebasis_details.fare",
    #     #         "surcharge": "$farebasis_details.surcharge",
    #     #         "currency": "$farebasis_details.currency",
    #     #         "Rule_id": "$farebasis_details.Rule_id",
    #     #         "RBD_type": "$farebasis_details.RBD_type",
    #     #         "Parent_RBD": "$farebasis_details.Parent_RBD",
    #     #         "RBD_hierarchy": "$farebasis_details.RBD_hierarchy",
    #     #         "derived_filed_fare": "$farebasis_details.derived_filed_fare",
    #     #         "last_update_date": "$farebasis_details.last_update_date",
    #     #         "last_update_time": "$farebasis_details.last_update_time",
    #     #         'sales': {
    #     #             '$filter': {
    #     #                    'input': "$sales",
    #     #                    'as': "sales",
    #     #                    'cond': {'$and': [{'$gte': ["$$sales.dep_date", afilter['fromDate']]},
    #     #                                      {'$lte': ["$$sales.dep_date", afilter['toDate']]},
    #     #                                      # {'$eq': ["$$sales.origin", "$origin"]},
    #     #                                      # {'$eq': ["$$sales.destination", "$destination"]},
    #     #                                      # {'$eq': ["$$sales.pos", "$farebasis_details.pos"]},
    #     #                                      {'$eq': ["$$sales.fare_basis", "$farebasis_details.fare_basis"]}]}
    #     #                 }
    #     #         }
    #     #     }
    #     # },

    #     # // Stage 4
    #     # {
    #     #     # '$addFields': {
    #     #     '$project': {
    #     #         "source": "$source",
    #     #         "effective_from": "$effective_from",
    #     #         "effective_to": "$effective_to",
    #     #         "departure_date_from": "$departure_date_from",
    #     #         "departure_date_to": "$departure_date_to",
    #     #         "fare_basis": "$fare_basis",
    #     #         "RBD": "$RBD",
    #     #         "fare_rule": "$fare_rule",
    #     #         "tariff_code": "$tariff_code",
    #     #         "footnote": "$footnote",
    #     #         "rtg": "$rtg",
    #     #         "batch": "$batch",
    #     #         "region": "$region",
    #     #         "country": "$country",
    #     #         "pos": "$pos",
    #     #         "origin": "$origin",
    #     #         "destination": "$destination",
    #     #         "compartment": "$compartment",
    #     #         "oneway_return": "$oneway_return",
    #     #         "channel": "$channel",
    #     #         "carrier": "$carrier",
    #     #         "fare": "$fare",
    #     #         "surcharge": "$surcharge",
    #     #         "currency": "$currency",
    #     #         "Rule_id": "$Rule_id",
    #     #         "RBD_type": "$RBD_type",
    #     #         "Parent_RBD": "$Parent_RBD",
    #     #         "RBD_hierarchy": "$RBD_hierarchy",
    #     #         "derived_filed_fare": "$derived_filed_fare",
    #     #         "last_update_date": "$last_update_date",
    #     #         "last_update_time": "$last_update_time",
    #     #         "pax":{'$sum':'$sales.pax'},
    #     #         "pax_ly":{'$sum':'$sales.pax_1'},
    #     #         "revenue":{'$sum':'$sales.revenue_base'},
    #     #         "revenue_ly":{'$sum':'$sales.revenue_base_ly'}
    #     #     }
    #     # },
    #     #
    #     # # // Stage 5
    #     # {
    #     #     # '$addFields': {
    #     #     '$project': {
    #     #         "source": "$source",
    #     #         "effective_from": "$effective_from",
    #     #         "effective_to": "$effective_to",
    #     #         "departure_date_from": "$departure_date_from",
    #     #         "departure_date_to": "$departure_date_to",
    #     #         "fare_basis": "$fare_basis",
    #     #         "RBD": "$RBD",
    #     #         "fare_rule": "$fare_rule",
    #     #         "tariff_code": "$tariff_code",
    #     #         "footnote": "$footnote",
    #     #         "rtg": "$rtg",
    #     #         "batch": "$batch",
    #     #         "region": "$region",
    #     #         "country": "$country",
    #     #         "pos": "$pos",
    #     #         "origin": "$origin",
    #     #         "destination": "$destination",
    #     #         "compartment": "$compartment",
    #     #         "oneway_return": "$oneway_return",
    #     #         "channel": "$channel",
    #     #         "carrier": "$carrier",
    #     #         "fare": "$fare",
    #     #         "surcharge": "$surcharge",
    #     #         "currency": "$currency",
    #     #         "Rule_id": "$Rule_id",
    #     #         "RBD_type": "$RBD_type",
    #     #         "Parent_RBD": "$Parent_RBD",
    #     #         "RBD_hierarchy": "$RBD_hierarchy",
    #     #         "derived_filed_fare": "$derived_filed_fare",
    #     #         "last_update_date": "$last_update_date",
    #     #         "last_update_time": "$last_update_time",
    #     #         "pax": {'$sum': '$pax'},
    #             "pax_vlyr": {
    #                 '$cond': {
    #                     'if': {'$gt':['$pax_ly',0]},
    #                     'then': {'$divide': [{'$subtract':['$pax','$pax_ly']},'$pax_ly']},
    #                     'else': None
    #                 }
    #             },
    #             "revenue_vlyr": {
    #                 '$cond': {
    #                     'if': {'$gt':['$revenue_ly',0]},
    #                     'then': {'$divide': [{'$subtract':['$revenue','$revenue_ly']},'$revenue_ly']},
    #                     'else': None
    #                 }
    #             }
    #         }
    #     },
    #     #
    #     # # // Stage 6
    #     # {
    #     #     # '$addFields': {
    #     #     '$project': {
    #     #         "source": "$source",
    #     #         "effective_from": "$effective_from",
    #     #         "effective_to": "$effective_to",
    #     #         "departure_date_from": "$departure_date_from",
    #     #         "departure_date_to": "$departure_date_to",
    #     #         "fare_basis": "$fare_basis",
    #     #         "RBD": "$RBD",
    #     #         "fare_rule": "$fare_rule",
    #     #         "tariff_code": "$tariff_code",
    #     #         "footnote": "$footnote",
    #     #         "rtg": "$rtg",
    #     #         "batch": "$batch",
    #     #         "region": "$region",
    #     #         "country": "$country",
    #     #         "pos": "$pos",
    #     #         "origin": "$origin",
    #     #         "destination": "$destination",
    #     #         "compartment": "$compartment",
    #     #         "oneway_return": "$oneway_return",
    #     #         "channel": "$channel",
    #     #         "carrier": "$carrier",
    #     #         "fare": "$fare",
    #     #         "surcharge": "$surcharge",
    #     #         "currency": "$currency",
    #     #         "Rule_id": "$Rule_id",
    #     #         "RBD_type": "$RBD_type",
    #     #         "Parent_RBD": "$Parent_RBD",
    #     #         "RBD_hierarchy": "$RBD_hierarchy",
    #     #         "derived_filed_fare": "$derived_filed_fare",
    #     #         "last_update_date": "$last_update_date",
    #     #         "last_update_time": "$last_update_time",
    #     #         "pax": {'$sum': '$pax'},
    #             "effectivity": {
    #                 "$cond": {
    #                     'if': {'$and':[{'$ne':['$pax_vlyr', None]},
    #                                    {'$ne':['$revenue_vlyr', None]}]},
    #                     'then':{'$add':['$pax_vlyr','$revenue_vlyr']},
    #                     'else':{
    #                         '$cond': {
    #                             'if': {'$ne':['$pax_vlyr', None]},
    #                             'then':'$pax_vlyr',
    #                             'else': {
    #                                 '$cond': {
    #                                     'if': {'$ne':['$revenue_vlyr', None]},
    #                                     'then': '$revenue_vlyr',
    #                                     'else': None
    #                                 }
    #                             }
    #                         }
    #                     }
    #                 }
    #             }
    #         }
    #     }
    #     # ,
    #     {
    #         '$group': {
    #             '_id':None,
    #             'total': {'$sum':1},
    #             'effective': {
    #                 '$sum': {
    #                     '$cond': {
    #                         'if':{'$and':[{'$gte':['$effectivity',0]},
    #                                       {'$ne':['$effectivity',None]}]},
    #                         'then':1,
    #                         'else':0
    #                     }
    #                 }
    #             },
    #             'ineffective': {
    #                 '$sum': {
    #                     '$cond': {
    #                         'if':{'$and':[{'$lt':['$effectivity',0]},
    #                                       {'$ne':['$effectivity',None]}]},
    #                         'then':1,
    #                         'else':0
    #                     }
    #                 }
    #             },
    #             'ticketed':{'$sum':'$pax'}
    #         }
    #     }
    #     # ,
    #     {
    #         '$out': 'JUP_DB_Effective_Fares_Temp'
    #     }
    # ]
    """
    ppln_sales_collection = [
        # // Stage 1
        {
            '$match': dict(qry_sales_effective)
        },

        # // Stage 2
        {
            '$group': {
                '_id': {
                    'region': '$region',
                    'country': '$country',
                    'pos': '$pos',
                    'origin': '$origin',
                    'destination': '$destination',
                    'compartment': '$compartment',
                    'farebasis': '$fare_basis'
                },
                'pax': {'$sum':'$pax'},
                'pax_ly':{'$sum':'$pax_1'},
                'revenue': {'$sum':'$revenue_base'},
                'revenue_ly': {'$sum':'revenue_base_1'}
            }
        },

        # // Stage 3
        {
            '$project': {
                'pax':'$pax',
                'pax_ly':'$pax_ly',
                'revenue':'$revenue',
                'revenue_ly': '$revenue_ly',
                "pax_vlyr": {
                                '$cond': {
                                    'if': {'$gt':['$pax_ly',0]},
                                    'then': {'$divide': [{'$subtract':['$pax','$pax_ly']},'$pax_ly']},
                                    'else': None
                                }
                            },
                "revenue_vlyr": {
                                '$cond': {
                                    'if': {'$gt':['$revenue_ly',0]},
                                    'then': {'$divide': [{'$subtract':['$revenue','$revenue_ly']},'$revenue_ly']},
                                    'else': None
                                }
                            }
            }
        },

        # // Stage 4
        {
            '$project': {
                'pax':'$pax',
                'pax_ly':'$pax_ly',
                'pax_vlyr':'$pax_vlyr',
                'revenue':'$revenue',
                'revenue_ly': '$revenue_ly',
                'revenue_vlyr':'$revenue_vlyr',
                'effectivity': {
                                '$cond': {
                                    'if': {'$and':[{'$ne':['$pax_vlyr', None]},
                                                   {'$ne':['$revenue_vlyr', None]}]},
                                    'then':{'$add':['$pax_vlyr','$revenue_vlyr']},
                                    'else':{
                                        '$cond': {
                                            'if': {'$ne':['$pax_vlyr', None]},
                                            'then':'$pax_vlyr',
                                            'else': {
                                                '$cond': {
                                                    'if': {'$ne':['$revenue_vlyr', None]},
                                                    'then': '$revenue_vlyr',
                                                    'else': 1
                                                }
                                            }
                                        }
                                    }
                                }
                }           
            }
        },

        # // Stage 5
        {
            '$group': {
                            '_id':None,
                            'total': {'$sum':1},
                            'effective': {
                                '$sum': {
                                    '$cond': {
                                        'if':{'$and':[{'$gte':['$effectivity',0]},
                                                      {'$eq':['$effectivity',None]}
                                                      ]},
                                        'then':1,
                                        'else':0
                                    }
                                }
                            },
                            'ineffective': {
                                '$sum': {
                                    '$cond': {
                                        'if':{'$lt':['$effectivity',0]},
                                        'then':1,
                                        'else':0
                                    }
                                }
                            }
                        }
        }
        ,
        {
            '$out': 'JUP_DB_Effective_Fares_Temp'
        }
    ]
    print ppln_sales_collection
    response = dict()
    db.JUP_DB_Sales.aggregate(ppln_sales_collection, allowDiskUse=True)
    crsr_effective_fares = db.JUP_DB_Effective_Fares_Temp.find()
    if crsr_effective_fares.count() == 1:
        effective = crsr_effective_fares[0]['effective']
        ineffective = crsr_effective_fares[0]['ineffective']
        total = crsr_effective_fares[0]['total']
    # print lst_effective_fares
    # if len(lst_effective_fares) != 0:
    #     ticketed = 0
    #     total = 0
    #     effective = 0
    #     ineffective = 0
    #     for fare_doc in lst_effective_fares:
    #         ticketed += 1
    #         total += 1
    #         if fare_doc['effectivity'] >= 0:
    #             effective += 1
    #         elif fare_doc['effectivity'] < 0:
    #             ineffective +=1
        response['total'] = total
        response['effective'] = effective
        response['ineffective'] = ineffective
        db.JUP_DB_Effective_Fares_Temp.drop()
        return response

    # else:
    #     no_data_error = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
    #                                            get_module_name(),
    #                                            get_arg_lists(inspect.currentframe()))
    #     no_data_error_desc = ''.join(['No Documents obtained from JUP_DB_Sales colection for given filter'])
    #     no_data_error.append_to_error_list(no_data_error_desc)
    #     return {'total': None,'effective': None,'ineffective': None,'ticketed':None}
        # raise no_data_error


def get_ticketed(afilter):
    """
    """
    qry_sales = build_qry_sales(afilter)
    ppln_sales = [
    {
        '$match': qry_sales
    }
    ,
    {
        '$group': {
            '_id':None,
            'ticketed': {'$sum':'$pax'}
        }
    }
    ]
    crsr = db.JUP_DB_Sales.aggregate(ppln_sales)
    data = list(crsr)
    if len(data)==1:
        return data[0]['ticketed']
    else:
        return None


def get_tiles(afilter):
    """
    """
    response = dict()
    response['user_fares'] = get_effective_ineffective_fares(afilter)
    response['user_ticketed'] = get_ticketed(afilter)
    host_level_filter = dict()
    host_level_filter['fromDate'] = afilter['fromDate']
    host_level_filter['toDate'] = afilter['toDate']
    response['host_ticketed'] = get_ticketed(host_level_filter)
    response['host_fares'] = get_effective_ineffective_fares(host_level_filter)
    return response







# def get_tiles(afilter):
#     afilter = deepcopy(defaultdict(list, afilter)) # use proper variable names
#     query = dict() # use proper variable names
#     response = dict()
#     if afilter['region']:
#         query['region'] = {'$in': afilter['region']}
#     if afilter['country']:
#         query['country'] = {'$in': afilter['country']}
#     if afilter['pos']:
#         query['pos'] = {'$in': afilter['pos']}
#     query3 = deepcopy(query) # use proper variable names
#     if afilter['origin']:
#         od_build = []
#         for idx, item in enumerate(afilter['origin']):
#             od_build.append({'origin': item, 'destination': afilter['destination'][idx]})
#         query['$or'] = od_build
#     if afilter['compartment']:
#         query['compartment'] = {'$in': afilter['compartment']}
#     query4 = deepcopy(query) # use proper variable names

#     #   For Sales FLown
#     query1 = deepcopy(query) # use proper variable names
#     query1['dep_date'] = {'$gte': afilter['fromDate'],
#                           '$lte': afilter['toDate']}
#     # use proper variable names
#     cursor1 = db.JUP_DB_Sales_Flown.aggregate([
#         {
#             '$match': query1
#         },
#         {
#             '$group': {
#                 '_id': {
#                     'pos': '$pos',
#                     'origin': '$origin',
#                     'destination': '$destination',
#                     'compartment': '$compartment'
#                 },
#                 'revenue': {'$sum':'$revenue_base'}
#             }
#         },
#         {
#             '$project': {
#                 '_id': 0,
#                 'pos': '$_id.pos',
#                 'origin': '$_id.origin',
#                 'destination': '$_id.destination',
#                 'compartment': '$_id.compartment',
#                 'revenue': '$revenue'
#             }
#         }
#     ])
#     data_revenue = [] # use proper variable names
#     for i in cursor1:
#         data_revenue.append(i)

#     #   For Target
#     # use proper variable names
#     query2 = deepcopy(query)
#     from_obj = datetime.datetime.strptime(afilter['fromDate'], '%Y-%m-%d')
#     to_obj = datetime.datetime.strptime(afilter['toDate'], '%Y-%m-%d')
#     mon_year_combs = query_month_year_builder(from_obj.month,
#                                               from_obj.year,
#                                               to_obj.month,
#                                               to_obj.year)
#     if afilter['origin']:
#         query2['$and'] = [{'$or': query2['$or']},{'$or': mon_year_combs}]
#     else:
#         query2['$or'] = mon_year_combs
#     # use proper variable names
#     cursor2 = db.JUP_DB_Target_OD.aggregate([
#         {
#             '$match': query2
#         },
#         {
#             '$group': {
#                 '_id': {
#                     'pos': '$pos',
#                     'origin': '$origin',
#                     'destination': '$destination',
#                     'compartment': '$compartment'
#                 },
#                 'pax': {'$sum': '$pax'},
#                 'revenue': {'$sum': '$revenue'}
#             }
#         },
#         {
#             '$project': {
#                 '_id': 0,
#                 'pos': '$_id.pos',
#                 'origin': '$_id.origin',
#                 'destination': '$_id.destination',
#                 'compartment': '$_id.compartment',
#                 'target_rev': '$revenue',
#                 'target_pax': '$pax'
#             }
#         }

#     ])
#     data_target = [] # use proper variable names
#     for i in cursor2: # use proper variable names for 'i'
#         data_target.append(i)

#     #   For Market Share
#     if afilter['origin']:
#         ods = []
#         for idx, item in enumerate(afilter['origin']):
#             ods.append(item+afilter['destination'][idx])
#         query3['od'] = {'$in': ods}
#     query3['$or'] = mon_year_combs # use proper variable names
#     # use proper variable names
#     cursor3 = db.JUP_DB_Market_Share.aggregate([
#         {
#             '$match': query3,
#         },
#         {
#             '$group': {
#                 '_id': {
#                     'pos': '$pos',
#                     'od': '$od',
#                     'compartment': '$compartment'
#                 },
#                 'pax_host': {
#                     "$sum": {"$cond": [
#                         {'$eq': ['$MarketingCarrier1', Host_Airline_Code]},
#                         '$pax',
#                         0
#                     ]}},
#                 'pax':{'$sum': '$pax'}
#             }
#         },
#         {
#             '$project':
#                 {
#                     '_id': 0,
#                     'pos': '$_id.pos',
#                     'od': '$_id.od',
#                     'compartment':'$_id.compartment',
#                     'pax': '$pax_host',
#                     'market_share': {'$multiply': [{'$divide':['$pax_host','$pax']},100]}
#                 }
#         }
#     ])
#     data_ms = [] # use proper variable names
#     for i in cursor3: # use proper variable names for 'i'
#         i['origin'] = i['od'][:3]
#         i['destination'] = i['od'][3:]
#         del i['od']
#         data_ms.append(i)

#     query4['effective_from'] = {'$lte': SYSTEM_DATE}
#     query4['effective_to'] = {'$gte': SYSTEM_DATE}

#     cursor4 = db.JUP_DB_ATPCO_Fares.find(query4) # use proper variable names
#     # print 'no of fares',cursor4.count()
#     effective_count = 0
#     total_count = 0
#     # WHAT IS GOING ON HERE BUDDY?
#     for i in cursor4:
#         total_count += 1
#         ms_d = [(p['market_share'],p['pax']) for p in data_ms if (i['pos'] == p['pos'] and i['origin'] == p['origin'] and
#                                                                 i['destination'] == p['destination'] and
#                                                      i['compartment'] == p['compartment'])]
#         if len(ms_d) == 1:
#             ms = ms_d[0][0]
#             pax = ms_d[0][0]

#         trgt = [(q['target_rev'],q['target_pax']) for q in data_target if (i['pos'] == q['pos'] and
#                                                                            i['origin'] == q['origin'] and
#                                                                            i['destination'] == q['destination'] and
#                                                                            i['compartment'] == q['compartment'])]
#         if len(trgt) == 1:
#             rev_trgt = trgt[0][0]
#             pax_trgt = trgt[0][1]

#         # use proper variable names
#         rev_d = [r['revenue'] for r in data_revenue if (i['pos'] == r['pos'] and
#                                                        i['origin'] == r['origin'] and
#                                                        i['destination'] == r['destination'] and
#                                                        i['compartment'] == r['compartment'])]
#         if len(rev_d) == 1:
#             rev = rev_d[0]
#         # dist = db.JUP_DB_Flight_Leg.distinct('distance',{'$or':[{'leg_origin': i['origin'],
#         #                                                  'leg_destination': i['destination']},
#         #                                                 {'leg_origin':i['destination'],
#         #                                                  'leg_destination': i['origin']}]})
#         try:

#             # distance = dist[0]
#             # yield_val = rev/(pax*distance)
#             # yield_target = rev_trgt/(rev_trgt*distance)

#             ms_vtgt = (ms - 20)/20
#             rev_vtgt = (rev - rev_trgt)/rev_trgt
#             pax_vtgt = (pax - pax_trgt)/pax_trgt
#             fare_vtgt = ((convert_currency(i['price'],i['currency']) - (float(rev_trgt)/pax_trgt))/
#                          (float(rev_trgt)/pax_trgt))
#             # yield_vtgt = (yield_val - yield_target)*100/yield_target
#             # use proper variable names.. WHAT IS effective_ness ???
#             effective_ness = numpy.mean([ms_vtgt,rev_vtgt,pax_vtgt,fare_vtgt])
#                              # + yield_vtgt
#             # print 'effective_ness'
#             # print effective_ness

#             if effective_ness > 0:
#                 effective_count += 1

#             response = defaultdict(int)
#             response['total_fares_filter'] = total_count
#             response['effective_fares_filter'] = effective_count
#         except Exception as e:
#             print str(e)

#     query5 = deepcopy(query)     # use proper variable names
#     # print query1
#     # use proper variable names
#     cursor5 = db.JUP_DB_Booking_DepDate.aggregate([
#         {
#             '$match': query1
#         },
#         {
#             '$group': {
#                 '_id': None,
#                 'ticketed': {'$sum': '$ticket'}
#             }
#         },
#         {
#             '$project': {
#                 '_id': 0,
#                 'ticketed': '$ticketed'
#             }
#         }
#     ])

#     data_ticket_user = list(cursor5)     # use proper variable names.. lst_
#     # print data_ticket_user
#     if len(data_ticket_user) == 1:
#         response['total_user_ticket'] = data_ticket_user[0]['ticketed']

#     # use proper variable names
#     cursor6 = db.JUP_DB_Booking_DepDate.aggregate([
#         # format it nicely
#         {
#             '$match': {'dep_date': {'$gte': afilter['fromDate'],
#                                     '$lte': afilter['toDate']}}
#         },
#         {
#             '$group': {
#                 '_id': None,
#                 'ticketed': {'$sum': '$ticket'}
#             }
#         },
#         {
#             '$project': {
#                 '_id': 0,
#                 'ticketed': '$ticketed'
#             }
#         }
#     ])
#     data_ticket_host = list(cursor6)     # use proper variable names lst_
#     #     print data_ticket_user
#     if len(data_ticket_host) == 1:
#         response['total_host_ticket'] = data_ticket_host[0]['ticketed']

#     # print response
#     return response


# # isn't get_host_effective_fares_perc a better name?
# # def get_effective_fares_percentage_host(afilter):
# #     del afilter['region']
# #     del afilter['country']
# #     del afilter['pos']
# #     del afilter['origin']
# #     del afilter['destination']
# #     afilter = deepcopy(defaultdict(list, afilter))     # use proper variable names
# #     query = dict()     # use proper variable names
# #     response = dict()
# #     if afilter['region']:
# #         query['region'] = {'$in': afilter['region']}
# #     if afilter['country']:
# #         query['country'] = {'$in': afilter['country']}
# #     if afilter['pos']:
# #         query['pos'] = {'$in': afilter['pos']}
# #     query3 = deepcopy(query)     # use proper variable names

# #     if afilter['origin']:
# #         od_build = []
# #         for idx, item in enumerate(afilter['origin']):
# #             od_build.append({'origin': item, 'destination': afilter['destination'][idx]})
# #         query['$or'] = od_build
# #     if afilter['compartment']:
# #         query['compartment'] = {'$in': afilter['compartment']}
# #     query4 = deepcopy(query)

# #     #   For Sales FLown
# #     query1 = deepcopy(query) # use proper variable names
# #     query1['dep_date'] = {'$gte': afilter['fromDate'],
# #                           '$lte': afilter['toDate']}
# #     # print query1
# #     # use proper variable names
# #     cursor1 = db.JUP_DB_Sales_Flown.aggregate([
# #         {
# #             '$match': query1
# #         },
# #         {
# #             '$group': {
# #                 '_id': {
# #                     'pos': '$pos',
# #                     'origin': '$origin',
# #                     'destination': '$destination',
# #                     'compartment': '$compartment'
# #                 },
# #                 'revenue': {'$sum': '$revenue_base'}
# #             }
# #         },
# #         {
# #             '$project': {
# #                 '_id': 0,
# #                 'pos': '$_id.pos',
# #                 'origin': '$_id.origin',
# #                 'destination': '$_id.destination',
# #                 'compartment': '$_id.compartment',
# #                 'revenue': '$revenue'
# #             }
# #         }
# #     ])
# #     # use proper variable names
# #     data_revenue = []
# #     for i in cursor1:
# #         data_revenue.append(i)
# #     print len(data_revenue)
# #     # For Target
# #     # use proper variable names
# #     query2 = deepcopy(query)
# #     # use jup_common_functions instead
# #     from_obj = datetime.datetime.strptime(afilter['fromDate'], '%Y-%m-%d')
# #     to_obj = datetime.datetime.strptime(afilter['toDate'], '%Y-%m-%d')
# #     mon_year_combs = query_month_year_builder(from_obj.month,
# #                                               from_obj.year,
# #                                               to_obj.month,
# #                                               to_obj.year)
# #     if afilter['origin']:
# #         query2['$and'] = [{'$or': query2['$or']}, {'$or': mon_year_combs}] # use proper variable names
# #     else:
# #         query2['$or'] = mon_year_combs
# #     print query2
# #     # use proper variable names
# #     cursor2 = db.JUP_DB_Target_OD.aggregate([
# #         {
# #             '$match': query2
# #         },
# #         {
# #             '$group': {
# #                 '_id': {
# #                     'pos': '$pos',
# #                     'origin': '$origin',
# #                     'destination': '$destination',
# #                     'compartment': '$compartment'
# #                 },
# #                 'pax': {'$sum': '$pax'},
# #                 'revenue': {'$sum': '$revenue'}
# #             }
# #         },
# #         {
# #             '$project': {
# #                 '_id': 0,
# #                 'pos': '$_id.pos',
# #                 'origin': '$_id.origin',
# #                 'destination': '$_id.destination',
# #                 'compartment': '$_id.compartment',
# #                 'target_rev': '$revenue',
# #                 'target_pax': '$pax'
# #             }
# #         }

# #     ])
# #     data_target = [] # use proper variable names.. shouldn't it be lst_...
# #     for i in cursor2: # use proper variable names for 'i'
# #         data_target.append(i)
# #     print len(data_target)
# #     # For Market Share
# #     if afilter['origin']:
# #         ods = []
# #         for idx, item in enumerate(afilter['origin']): # use proper variable names for 'idx' and 'item'
# #             ods.append(item + afilter['destination'][idx])
# #         query3['od'] = {'$in': ods}
# #     query3['$or'] = mon_year_combs
# #     # use proper variable names
# #     cursor3 = db.JUP_DB_Market_Share.aggregate([
# #         {
# #             '$match': query3,
# #         },
# #         {
# #             '$group': {
# #                 '_id': {
# #                     'pos': '$pos',
# #                     'od': '$od',
# #                     'compartment': '$compartment'
# #                 },
# #                 'pax_host': {
# #                     "$sum": {"$cond": [
# #                         {'$eq': ['$MarketingCarrier1', Host_Airline_Code]},
# #                         '$pax',
# #                         0
# #                     ]}},
# #                 'pax': {'$sum': '$pax'}
# #             }
# #         },
# #         {
# #             '$project':
# #                 {
# #                     '_id': 0,
# #                     'pos': '$_id.pos',
# #                     'od': '$_id.od',
# #                     'compartment': '$_id.compartment',
# #                     'pax': '$pax_host',
# #                     'market_share': {'$multiply': [{'$divide': ['$pax_host', '$pax']}, 100]}
# #                 }
# #         }
# #     ])
# #     data_ms = []
# #     for i in cursor3: # use proper variable names for 'i'
# #         i['origin'] = i['od'][:3]
# #         i['destination'] = i['od'][3:]
# #         del i['od']
# #         data_ms.append(i)
# #     print len(data_ms)
# #     query4['effective_from'] = {'$lte': SYSTEM_DATE}
# #     query4['effective_to'] = {'$gte': SYSTEM_DATE}

# #     # use proper variable names
# #     cursor4 = db.JUP_DB_ATPCO_Fares.find(query4)
# #     print 'no of fares', cursor4.count()
# #     effective_count = 0
# #     total_count = 0
# #     response['effective_fares_percentage'] = na_value
# #     # WHAT IS GOING ONNN DUDE???
# #     # WHAT IS p[], q[], r[]
# #     for i in cursor4:
# #         total_count += 1
# #         ms_d = [(p['market_share'], p['pax']) for p in data_ms if
# #                 (i['pos'] == p['pos'] and i['origin'] == p['origin'] and
# #                  i['destination'] == p['destination'] and
# #                  i['compartment'] == p['compartment'])]
# #         if len(ms_d) == 1:
# #             ms = ms_d[0][0]
# #             pax = ms_d[0][0]

# #         trgt = [(q['target_rev'], q['target_pax']) for q in data_target if (i['pos'] == q['pos'] and
# #                                                                             i['origin'] == q['origin'] and
# #                                                                             i['destination'] == q['destination'] and
# #                                                                             i['compartment'] == q['compartment'])]
# #         if len(trgt) == 1:
# #             rev_trgt = trgt[0][0]
# #             pax_trgt = trgt[0][1]

# #         rev_d = [r['revenue'] for r in data_revenue if (i['pos'] == r['pos'] and
# #                                                         i['origin'] == r['origin'] and
# #                                                         i['destination'] == r['destination'] and
# #                                                         i['compartment'] == r['compartment'])]
# #         if len(rev_d) == 1:
# #             rev = rev_d[0]
# #         # dist = db.JUP_DB_Flight_Leg.distinct('distance',{'$or':[{'leg_origin': i['origin'],
# #         #                                                  'leg_destination': i['destination']},
# #         #                                                 {'leg_origin':i['destination'],
# #         #                                                  'leg_destination': i['origin']}]})
# #         try: #try must be right on top

# #             # distance = dist[0]
# #             # yield_val = rev/(pax*distance)
# #             # yield_target = rev_trgt/(rev_trgt*distance)

# #             ms_vtgt = (ms - 20) / 20
# #             rev_vtgt = (rev - rev_trgt) / rev_trgt
# #             pax_vtgt = (pax - pax_trgt) / pax_trgt
# #             fare_vtgt = ((convert_currency(i['price'], i['currency']) - (float(rev_trgt) / pax_trgt)) /
# #                          (float(rev_trgt) / pax_trgt))
# #             # yield_vtgt = (yield_val - yield_target)*100/yield_target
# #             effective_ness = numpy.mean([ms_vtgt, rev_vtgt, pax_vtgt, fare_vtgt])
# #             # + yield_vtgt
# #             print 'effective_ness'
# #             print effective_ness

# #             if effective_ness > 0:
# #                 effective_count += 1

# #             response = defaultdict(int)
# #             response['effective_fares_percentage'] = round(float(total_count)/effective_count,3)
# #         except Exception as e:
# #             print str(e)
# #     print response
# #     return response


# # # def get_effective_ineffective_fares(afilter):
# #     afilter = deepcopy(defaultdict(list, afilter)) # use proper variable names
# #     query = dict() # use proper variable names
# #     response = dict()
# #     if afilter['region']:
# #         query['region'] = {'$in': afilter['region']}
# #     if afilter['country']:
# #         query['country'] = {'$in': afilter['country']}
# #     if afilter['pos']:
# #         query['pos'] = {'$in': afilter['pos']}
# #     query3 = deepcopy(query) # use proper variable names
# #     if afilter['origin']:
# #         od_build = []
# #         for idx, item in enumerate(afilter['origin']): # use proper variable names for 'idx' and 'item'
# #             od_build.append({'origin': item, 'destination': afilter['destination'][idx]})
# #         query['$or'] = od_build
# #     if afilter['compartment']:
# #         query['compartment'] = {'$in': afilter['compartment']}
# #     query4 = deepcopy(query) # use proper variable names

# #     #   For Sales FLown
# #     query1 = deepcopy(query) # use proper variable names
# #     query1['dep_date'] = {'$gte': afilter['fromDate'],
# #                           '$lte': afilter['toDate']}
# #     # use proper variable names
# #     cursor1 = db.JUP_DB_Sales_Flown.aggregate([
# #         {
# #             '$match': query1
# #         },
# #         {
# #             '$group': {
# #                 '_id': {
# #                     'pos': '$pos',
# #                     'origin': '$origin',
# #                     'destination': '$destination',
# #                     'compartment': '$compartment'
# #                 },
# #                 'revenue': {'$sum': '$revenue_base'}
# #             }
# #         },
# #         {
# #             '$project': {
# #                 '_id': 0,
# #                 'pos': '$_id.pos',
# #                 'origin': '$_id.origin',
# #                 'destination': '$_id.destination',
# #                 'compartment': '$_id.compartment',
# #                 'revenue': '$revenue'
# #             }
# #         }
# #     ])
# #     data_revenue = [] # use proper variable names
# #     for i in cursor1: # use proper variable names for 'i'
# #         data_revenue.append(i)

# #     # For Target
# #     query2 = deepcopy(query) # use proper variable names
# #     # use jup_common_functions
# #     from_obj = datetime.datetime.strptime(afilter['fromDate'], '%Y-%m-%d')
# #     to_obj = datetime.datetime.strptime(afilter['toDate'], '%Y-%m-%d')
# #     mon_year_combs = query_month_year_builder(from_obj.month,
# #                                               from_obj.year,
# #                                               to_obj.month,
# #                                               to_obj.year)
# #     if afilter['origin']:
# #         query2['$and'] = [{'$or': query2['$or']}, {'$or': mon_year_combs}]
# #     else:
# #         query2['$or'] = mon_year_combs# use proper variable names
# #     cursor2 = db.JUP_DB_Target_OD.aggregate([
# #         {
# #             '$match': query2
# #         }
# #         ,
# #         {
# #             '$group': {
# #                 '_id': {
# #                     'pos': '$pos',
# #                     'origin': '$origin',
# #                     'destination': '$destination',
# #                     'compartment': '$compartment'
# #                 }
# #                 ,
# #                 'pax': {'$sum': '$pax'},
# #                 'revenue': {'$sum': '$revenue'}
# #             }
# #         },
# #         {

# #             '$project': {
# #                 '_id': 0,
# #                 'pos': '$_id.pos',
# #                 'origin': '$_id.origin',
# #                 'destination': '$_id.destination',
# #                 'compartment': '$_id.compartment',
# #                 'target_rev': '$revenue',
# #                 'target_pax': '$pax'
# #             }
# #         }

# #     ])
# #     data_target = [] # use proper variable names, like lst_...
# #     for i in cursor2: # use proper variable names for 'i'
# #         data_target.append(i)

# #     # For Market Share
# #     if afilter['origin']:
# #         ods = []
# #         for idx, item in enumerate(afilter['origin']): # use proper variable names for 'idx' and 'item'
# #             ods.append(item + afilter['destination'][idx])
# #         query3['od'] = {'$in': ods} # use proper variable names
# #     query3['$or'] = mon_year_combs
# #     # use proper variable names
# #     cursor3 = db.JUP_DB_Market_Share.aggregate([
# #         {
# #             '$match': query3,
# #         },
# #         {
# #             '$group': {
# #                 '_id': {
# #                     'pos': '$pos',
# #                     'od': '$od',
# #                     'compartment': '$compartment'
# #                 },
# #                 'pax_host': {
# #                     "$sum": {"$cond": [
# #                         {'$eq': ['$MarketingCarrier1', Host_Airline_Code]},
# #                         '$pax',
# #                         0
# #                     ]}},
# #                 'pax': {'$sum': '$pax'}
# #             }
# #         },
# #         {
# #             '$project':
# #                 {
# #                     '_id': 0,
# #                     'pos': '$_id.pos',
# #                     'od': '$_id.od',
# #                     'compartment': '$_id.compartment',
# #                     'pax': '$pax_host',
# #                     'market_share': {'$multiply': [{'$divide': ['$pax_host', '$pax']}, 100]}
# #                 }
# #         }
# #     ])
# #     data_ms = []
# #     for i in cursor3: # use proper variable names for 'i'
# #         i['origin'] = i['od'][:3]
# #         i['destination'] = i['od'][3:]
# #         del i['od']
# #         data_ms.append(i)

# #     query4['effective_from'] = {'$lte': SYSTEM_DATE}
# #     query4['effective_to'] = {'$gte': SYSTEM_DATE}

# #     # use proper variable names
# #     cursor4 = db.JUP_DB_ATPCO_Fares.find(query4)
# #     print 'no of fares', cursor4.count()
# #     effective_count = 0
# #     total_count = 0
# #     # Again, what is going on here?? Please explain with short comments
# #     for i in cursor4: # use proper variable names for 'i'
# #         total_count += 1
# #         ms_d = [(p['market_share'], p['pax']) for p in data_ms if
# #                 (i['pos'] == p['pos'] and i['origin'] == p['origin'] and
# #                  i['destination'] == p['destination'] and
# #                  i['compartment'] == p['compartment'])]
# #         if len(ms_d) == 1:
# #             ms = ms_d[0][0]
# #             pax = ms_d[0][0]

# #         trgt = [(q['target_rev'], q['target_pax']) for q in data_target if (i['pos'] == q['pos'] and
# #                                                                             i['origin'] == q['origin'] and
# #                                                                             i['destination'] == q['destination'] and
# #                                                                             i['compartment'] == q['compartment'])]
# #         if len(trgt) == 1:
# #             rev_trgt = trgt[0][0]
# #             pax_trgt = trgt[0][1]

# #         rev_d = [r['revenue'] for r in data_revenue if (i['pos'] == r['pos'] and
# #                                                         i['origin'] == r['origin'] and
# #                                                         i['destination'] == r['destination'] and
# #                                                         i['compartment'] == r['compartment'])]
# #         if len(rev_d) == 1:
# #             rev = rev_d[0]
# #         # dist = db.JUP_DB_Flight_Leg.distinct('distance',{'$or':[{'leg_origin': i['origin'],
# #         #                                                  'leg_destination': i['destination']},
# #         #                                                 {'leg_origin':i['destination'],
# #         #                                                  'leg_destination': i['origin']}]})
# #         try: # should be at the beginning of the function

# #             # distance = dist[0]
# #             # yield_val = rev/(pax*distance)
# #             # yield_target = rev_trgt/(rev_trgt*distance)

# #             ms_vtgt = (ms - 20) / 20
# #             rev_vtgt = (rev - rev_trgt) / rev_trgt
# #             pax_vtgt = (pax - pax_trgt) / pax_trgt
# #             fare_vtgt = ((convert_currency(i['price'], i['currency']) - (float(rev_trgt) / pax_trgt)) /
# #                          (float(rev_trgt) / pax_trgt))
# #             # yield_vtgt = (yield_val - yield_target)*100/yield_target
# #             effective_ness = numpy.mean([ms_vtgt, rev_vtgt, pax_vtgt, fare_vtgt])
# #             # + yield_vtgt
# #             print 'effective_ness'
# #             print effective_ness

# #             if effective_ness > 0:
# #                 effective_count += 1

# #             response = defaultdict(int)
# #             response['total_fares_filter'] = total_count
# #             response['effective_fares_filter'] = effective_count
# #             response['ineffective_fares_filter'] = total_count - effective_count
# #         except Exception as e:
# #             print str(e)


# def get_effective_ineffective_fares(afilter):
#     """
#     """
    # afilter = deepcopy(defaultdict(list, afilter))
    # query = dict()
    # response = dict()
    # if afilter['region']:
    #     query['region'] = {'$in': afilter['region']}
    # if afilter['country']:
    #     query['country'] = {'$in': afilter['country']}
    # if afilter['pos']:
    #     query['pos'] = {'$in': afilter['pos']}
    # query3 = deepcopy(query)
    # if afilter['origin']:
    #     od_build = []
    #     for idx, item in enumerate(afilter['origin']):
    #         od_build.append({'origin': item, 'destination': afilter['destination'][idx]})
    #     query['$or'] = od_build
    # if afilter['compartment']:
    #     query['compartment'] = {'$in': afilter['compartment']}
    # query['dep_date'] = {'$gte': afilter['fromDate'],
    #                      '$lte': afilter['toDate']}
#     ppln_sales_collection = [
#         {
#             '$match': query
#         },
#         {
#             '$group': {
#                 '_id': {
#                     'pos': '$pos',
#                     'origin': '$origin',
#                     'destination': '$destination',
#                     'compartment': '$compartment',
#                     'farebasis': '$fare_basis'
#                 },
#                 'revenue': {'$sum': '$revenue_base'},
#                 'revenue_ly': {'$sum': '$revenue_base_1'},
#                 'pax': {'$sum': '$pax'},
#                 'pax_ly': {'$sum':'$pax_1'}
#             }
#         },
#         {
#             '$project': {
#                 '_id': 0,
#                 'pos': '$_id.pos',
#                 'origin': '$_id.origin',
#                 'destination': '$_id.destination',
#                 'compartment': '$_id.compartment',
#                 'farebasis': '$_id.fare_basis',
#                 'pax': '$pax',
#                 'pax_vlyr': {
#                     '$cond': {
#                         'if': {'$gt': ['$pax_ly',0]},
#                         'then': {'$divide': [{'$subtract':['$pax', '$pax_ly']}, '$pax_ly']},
#                         'else': None
#                     }},
#                 'revenue_vlyr': {
#                     '$cond': {
#                         'if': {'$gt': ['$revenue_ly',0]},
#                         'then': {'$divide': [{'$subtract':['$revenue', '$revenue_ly']}, '$revenue_ly']},
#                         'else': None
#                     }

#                 }
#         }
#         }
#         ,
#         '$project': {
#                 '_id': 0,
#                 'pos': '$pos',
#                 'origin': '$origin',
#                 'destination': '$destination',
#                 'compartment': '$compartment',
#                 'farebasis': '$fare_basis',
#                 'pax': '$pax',
#                 'effectivity': {
#                     '$cond': {
#                         'if': {'$and'[{'$neq':['$pax_vlyr',None]},
#                                       {'$neq':['$revenue_vlyr',None]}]},
#                         'then': {'$add':['$pax_vlyr', '$revenue_vlyr']},
#                         'else': None
#                     }
#                 }
#         }
#         ]
#     crsr_sales_collection = db.JUP_DB_Sales.aggregate(ppln_sales_collection)
#     lst_sales_collection = list(crsr_sales_collection)
#     if len(lst_sales_collection)!=0:
#         ticketed = 0
#         total = 0
#         effective = 0
#         ineffective = 0
#         for fare_doc in lst_sales_collection:
#             ticketed += fare_doc['pax']
#             total += 1
#             if fare_doc['effectivity'] >= 0:
#                 effective += 1
#             elif fare_doc['ineffective'] < 0:
#                 ineffective += 1
#         response['user_total'] = total
#         response['user_effective'] = effective
#         response['user_ineffective'] = ineffective
#         response['user_ticketed'] = ticketed
#         return response
#     else:
        # no_data_error = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
        #                                        get_module_name(),
        #                                        get_arg_lists(inspect.currentframe()))
        # no_data_error_desc = ''.join(['No Documents obtained from JUP_DB_Sales colection for given filter'])
        # no_data_error.append_to_error_list(no_data_error_desc)
        # return {'user_total': None,'user_effective': None,'user_ineffective': None,'user_ticketed':None}
        # # raise no_data_error


# def get_effective_ineffective_fares_host(afilter):
#     """
#     """
#     afilter = deepcopy(defaultdict(list, afilter))
#     query = dict()
#     response = dict()
#     # if afilter['region']:
#     #     query['region'] = {'$in': afilter['region']}
#     # if afilter['country']:
#     #     query['country'] = {'$in': afilter['country']}
#     # if afilter['pos']:
#     #     query['pos'] = {'$in': afilter['pos']}
#     # query3 = deepcopy(query)
#     # if afilter['origin']:
#     #     od_build = []
#     #     for idx, item in enumerate(afilter['origin']):
#     #         od_build.append({'origin': item, 'destination': afilter['destination'][idx]})
#     #     query['$or'] = od_build
#     # if afilter['compartment']:
#     #     query['compartment'] = {'$in': afilter['compartment']}
#     query['dep_date'] = {'$gte': afilter['fromDate'],
#                          '$lte': afilter['toDate']}
#     ppln_fares_collection = [
#         {
#             '$match': query
#         },
#         {
#             '$group': {
#                 '_id': {
#                     'pos': '$pos',
#                     'origin': '$origin',
#                     'destination': '$destination',
#                     'compartment': '$compartment',
#                     'farebasis': '$fare_basis'
#                 },
#                 'revenue': {'$sum': '$revenue_base'},
#                 'revenue_ly': {'$sum': '$revenue_base_1'},
#                 'pax': {'$sum': '$pax'},
#                 'pax_ly': {'$sum':'$pax_1'}
#             }
#         },
#         {
#             '$project': {
#                 '_id': 0,
#                 'pos': '$_id.pos',
#                 'origin': '$_id.origin',
#                 'destination': '$_id.destination',
#                 'compartment': '$_id.compartment',
#                 'farebasis': '$_id.fare_basis',
#                 'pax': '$pax',
#                 'pax_vlyr': {
#                     '$cond': {
#                         'if': {'$gt': ['$pax_ly',0]},
#                         'then': {'$divide': [{'$subtract':['$pax', '$pax_ly']}, '$pax_ly']},
#                         'else': None
#                     }},
#                 'revenue_vlyr': {
#                     '$cond': {
#                         'if': {'$gt': ['$revenue_ly',0]},
#                         'then': {'$divide': [{'$subtract':['$revenue', '$revenue_ly']}, '$revenue_ly']},
#                         'else': None
#                     }

#                 }
#         }
#         }
#         ,
#         '$project': {
#                 '_id': 0,
#                 'pos': '$pos',
#                 'origin': '$origin',
#                 'destination': '$destination',
#                 'compartment': '$compartment',
#                 'farebasis': '$fare_basis',
#                 'pax': '$pax',
#                 'effectivity': {
#                     '$cond': {
#                         'if': {'$and'[{'$neq':['$pax_vlyr',None]},
#                                       {'$neq':['$revenue_vlyr',None]}]},
#                         'then': {'$add':['$pax_vlyr', '$revenue_vlyr']},
#                         'else': None
#                     }
#                 }
#         }
#         ]
#     crsr_sales_collection = db.JUP_DB_Sales.aggregate(ppln_sales_collection)
#     lst_sales_collection = list(crsr_sales_collection)
#     if len(lst_sales_collection)!=0:
#         ticketed = 0
#         total = 0
#         effective = 0
#         ineffective = 0
#         for fare_doc in lst_sales_collection:
#             ticketed += fare_doc['pax']
#             total += 1
#             if fare_doc['effectivity'] >= 0:
#                 effective += 1
#             elif fare_doc['ineffective'] < 0:
#                 ineffective += 1
#         response['host_total'] = total
#         response['host_effective'] = effective
#         response['host_ineffective'] = ineffective
#         response['host_ticketed'] = ticketed
#         return response
#     else:
#         no_data_error = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
#                                                get_module_name(),
#                                                get_arg_lists(inspect.currentframe()))
#         no_data_error_desc = ''.join(['No Documents obtained from JUP_DB_Sales colection for given filter'])
#         no_data_error.append_to_error_list(no_data_error_desc)
#         return {'host_total': None,'host_effective': None,'user_ineffective': None,'user_ticketed':None}
#         # raise no_data_error


if __name__ == '__main__':
    st = time.time()
    a = {
        'region': [],
        'country': [],
        'pos': [],
        'origin': [],
        'destination': [],
        'compartment': [],
        'fromDate': '2016-01-01',
        'toDate': '2017-04-01',
        # 'lastfromDate': '2015-07-01',
        # 'lasttoDate': '2015-07-31'
    }
    print db
    print get_effective_ineffective_fares(afilter=a)
    print time.time() - st