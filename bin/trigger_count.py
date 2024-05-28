#!/usr/bin/env python
# print "Content-Type: application/json"
import json
from copy import deepcopy

"""
Requirement - To provide the pending triggers taking pos as input and the response requested is in the following form
    {
        total:{
            manual:123,
            system:1234,
            sales_review:123123,
            sales_request:3456
        }
        ,
        {
            'od_wise':
                [
                    {
                        od:DXBDOH,
                        manual:1,
                        system:5,
                        sales_review:12,
                        sales_request:124
                    }
                    ,
                    .
                    .
                    .
                    .
                    .
                    .
                ]
        }
    } 
"""
from jupiter_AI import client, JUPITER_DB
import cgi

db = client[JUPITER_DB]
try:
    form = cgi.FieldStorage()
    input_filter_json = form.getvalue('filter')
    print "input_filter_json = ",input_filter_json
    input_filter_dict = deepcopy(json.loads(input_filter_json))
    # input_filter_dict = {
    #     'pos':[]
    # }

    query = dict()
    if input_filter_dict['pos']:
        query['pos'] = {'$in': input_filter_dict['pos']}
    query['status'] = 'pending'

    # Pipeline
    ppln = [
            # Stage 1
            {
                '$match': query
            },

            # Stage 2
            {
                '$facet': {
                    'total':
                        [
                            {
                                '$group':{
                                    '_id':None,
                                    'manual':{
                                      '$sum':{
                                                '$cond':{
                                                    'if':{'$eq':['$type_of_trigger','manual']},
                                                    'then':1,
                                                    'else': 0
                                                }
                                            }
                                        },
                                    'system': {
                                        '$sum': {
                                            '$cond': {
                                                'if': {'$eq': ['$type_of_trigger', 'system']},
                                                'then': 1,
                                                'else': 0
                                            }
                                        }
                                    },
                                    'sales_request': {
                                        '$sum': {
                                            '$cond': {
                                                'if': {'$eq': ['$type_of_trigger', 'sales_request']},
                                                'then': 1,
                                                'else': 0
                                            }
                                        }
                                    },
                                    'sales_review': {
                                        '$sum': {
                                            '$cond': {
                                                'if': {'$eq': ['$type_of_trigger', 'sales_review']},
                                                'then': 1,
                                                'else': 0
                                            }
                                        }
                                    }
                                }
                            }
                            ,
                            {
                                '$project':{
                                    '_id':0,
                                    'manual':'$manual',
                                    'system':'$system',
                                    'sales_request':'$sales_request',
                                    'sales_review':'$sales_review'
                                }
                            }
                        ]
                        ,
                   'od_wise':
                       [
                            {
                                '$group':{
                                    '_id':'$od',
                                    'manual':{
                                      '$sum':{
                                            '$cond':{
                                                'if':{'$eq':['$type_of_trigger','manual']},
                                                'then':1,
                                                'else': 0
                                            }
                                        }
                                    },
                                'system': {
                                    '$sum': {
                                        '$cond': {
                                            'if': {'$eq': ['$type_of_trigger', 'system']},
                                            'then': 1,
                                            'else': 0
                                        }
                                    }
                                },
                                'sales_request': {
                                    '$sum': {
                                        '$cond': {
                                            'if': {'$eq': ['$type_of_trigger', 'sales_request']},
                                            'then': 1,
                                            'else': 0
                                        }
                                    }
                                },
                                'sales_review': {
                                    '$sum': {
                                        '$cond': {
                                            'if': {'$eq': ['$type_of_trigger', 'sales_review']},
                                            'then': 1,
                                            'else': 0
                                        }
                                    }
                                }
                                }
                            }
                            ,
                            {
                                '$project':
                                    {
                                        '_id':0,
                                        'od':'$_id',
                                        'manual':'$manual',
                                        'system':'$system',
                                        'sales_review':'$sales_review',
                                        'sales_request':'$sales_request'

                            }
                            }
                        ]
                    # add more facets
                }
            },

            # Stage 3
            {
                '$project': {
                   '_id':0,
                   'total':{'$arrayElemAt': ['$total', 0]},
                   'od_wise':'$od_wise'
                }
            },

        ]
    data = list(db.JUP_DB_Workflow.aggregate(ppln))
    if len(data) == 1:
        print json.dumps(data[0])
except Exception:
    import traceback
    print traceback.print_exc()
    from time import strftime, localtime
    from jupiter_AI.common.mail_error_msg import send_simple_message
    from jupiter_AI import NOTIFICATION_EMAIL
    p = ''.join(['ERROR : ',
                 traceback.format_exc(),
                 ' \nTIME : ',
                 strftime("%a, %d %b %Y %H:%M:%S ", localtime()),
                 'IST'])
    send_simple_message(to=NOTIFICATION_EMAIL, subject='Trigger Count URL', body=p)