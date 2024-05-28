"""
File Name              : market_outlook_screen
Author                 : Ashwin Kumar
Date Created           : 2016-12-20
Description            : Displays the summary of region wise break-up of different market types like
                         ' Declining','Growing','Mature' and 'Niche' with respect to parameters such as
                         Revenue, Pax, market size, market share for both host airline and top 3 competitors
                         and fair market share

MODIFICATIONS LOG
    S.No                   : 1
    Date Modified          : 2016-12-27
    By                     : Shamail Mulla
    Modification Details   : Error and Exception handling added
"""

import datetime
import inspect
import time
from collections import defaultdict
from copy import deepcopy

import pymongo

from jupiter_AI.common import ClassErrorObject as error_class
from jupiter_AI import client
from jupiter_AI.network_level_params import JUPITER_DB
db = client[JUPITER_DB]
from jupiter_AI.tiles.jup_common_functions_tiles import gen_collection_name as gen
from jupiter_AI.tiles.jup_common_functions_tiles import query_month_year_builder

#the below function generates a random name for collection
collection_name = gen()

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

def query_for_market_outlook(afilter):
    filter_for_outlook = deepcopy(defaultdict(list, afilter))
    query_for_outlook = dict()
    if filter_for_outlook['region']:
        query_for_outlook['region'] = {'$in': filter_for_outlook['region']}
    if filter_for_outlook['country']:
        query_for_outlook['country'] = {'$in': filter_for_outlook['country']}
    if filter_for_outlook['pos']:
        query_for_outlook['pos'] = {'$in': filter_for_outlook['pos']}
    if afilter['compartment']:
        query_for_outlook['compartment'] = {'$in': filter_for_outlook['compartment']}
    if filter_for_outlook['origin'] and filter_for_outlook['destination']:
        od = ''.join(filter_for_outlook['origin'] + filter_for_outlook['destination'])
        query_for_outlook['od'] = od
    from_obj = datetime.datetime.strptime(filter_for_outlook['fromDate'], '%Y-%m-%d')
    to_obj = datetime.datetime.strptime(filter_for_outlook['toDate'], '%Y-%m-%d')

    # the following code converts the dates to month and year values. the below function
    # is taken from the common functions for tiles program

    month_year = query_month_year_builder(from_obj.month,
                                          from_obj.year,
                                          to_obj.month,
                                          to_obj.year)
    query_for_outlook['$or'] = month_year

    return query_for_outlook

"""
Displays the summary of region wise break-up of different market types like
' Declining','Growing','Mature' and 'Niche' with respect to parameters such
as Revenue, Pax, market size, market share for both host airline and top 3
competitors and fair market share.
"""

def outlook_market(afilter):
    try:
        if 'JUP_DB_Market_Share' in db.collection_names():
            query_mo = query_for_market_outlook(afilter)

            # A market is defined as the combination of Region Country POS OD compartment and RBD. In the following
            # aggregate function, the market does not consider the RBD as the data is not available for competitors
            # at an RBD level

            # the below variables define the type of market
            threshold_growing = 5  # hardcoded... need to change this
            threshold_declining = -5  # hardcoded... need to change this
            # use lower case for all variable names
            threshold_niche = 1000  # Niche threshold to be defined by the user. hard coded fo the time being

            # the following list of dictionaries will be used as an input to the mongodb aggregate function.
            pipeline_market_outlook = [
                # the first stage of the pipeline is to match the records from the collection based on the query.
                # the query is used to filter the collection based on the user's input
                {
                    '$match': query_mo
                }
                ,
                {
                    '$group':
                        {
                            '_id':
                                {
                                    'region': '$region',
                                    'country': '$country',
                                    'pos': '$pos',
                                    'od': '$od',
                                    'compartment': '$compartment',
                                    'airline': '$MarketingCarrier1'
                                },
                            'pax': {'$sum': '$pax'},
                            'pax_1': {'$sum': '$pax_1'}
                        }
                }
                ,
                {
                    '$group':
                        {
                            '_id':
                                {
                                    'region': '$_id.region',
                                    'country': '$_id.country',
                                    'pos': '$_id.pos',
                                    'od': '$_id.od',
                                    'compartment': '$_id.compartment',
                                },
                            'market_size': {'$sum': '$pax'},
                            'market_size_1': {'$sum': '$pax_1'},
                            'airline_details':
                                {
                                    '$push':
                                        {
                                            'airline': '$_id.airline',
                                            'airline_pax': '$pax',
                                            'airline_pax_1': '$pax_1'
                                        }
                                }
                        }
                }
                ,
                {
                    '$unwind': '$airline_details'
                }
                ,
                {
                    '$project':
                        {
                            '_id': 0,
                            'region': '$_id.region',
                            'country': '$_id.country',
                            'pos': '$_id.pos',
                            'od': '$_id.od',
                            'compartment': '$_id.compartment',
                            'market_size': '$market_size',
                            'market_size_1': '$market_size_1',
                            'airline': '$airline_details.airline',
                            'airline_pax': '$airline_details.airline_pax',
                            'airline_pax_1': '$airline_details.airline_pax_1'

                        }
                }
                ,
                {
                    '$project':
                        {
                            'region': '$region',
                            'country': '$country',
                            'pos': '$pos',
                            'od': '$od',
                            'compartment': '$compartment',
                            'market_size': '$market_size',
                            'market_size_1': '$market_size_1',
                            'airline': '$airline',
                            'airline_pax': '$airline_pax',
                            'airline_pax_1': '$airline_pax_1',
                            'market_vlyr':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$gt': ['$market_size_1', 0]
                                                },
                                            'then' :
                                                {
                                                    '$multiply': [100,
                                                                  {
                                                                      '$divide':
                                                                          [
                                                                              {
                                                                                  '$subtract': ['$market_size', '$market_size_1']
                                                                              },
                                                                              '$market_size_1'
                                                                          ]
                                                                  }
                                                                  ]
                                                },
                                            'else': 'market size last year values not available or it is a new market'
                                        }
                                },
                            'market_share':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$gt': ['$market_size', 0]
                                                },
                                            'then':
                                                {
                                                    '$multiply': [100,
                                                                  {
                                                                    '$divide': ['$airline_pax', '$market_size']
                                                                  }
                                                                  ]
                                                },
                                            'else': 'market size not available or is zero'
                                        }
                                },
                            'market_share_1':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$gt': ['$market_size_1', 0]
                                                },
                                            'then':
                                                {
                                                    '$multiply': [100,
                                                                  {
                                                                      '$divide': ['$airline_pax_1', '$market_size_1']
                                                                  }
                                                                  ]
                                                },
                                            'else': 'market size for last year not available or is zero'
                                        }
                                }
                        }
                }
                ,
                {
                    '$project':
                        {
                            'region': '$region',
                            'country': '$country',
                            'pos': '$pos',
                            'od': '$od',
                            'compartment': '$compartment',
                            'market_size': '$market_size',
                            'market_size_1': '$market_size_1',
                            'airline': '$airline',
                            'airline_pax': '$airline_pax',
                            'airline_pax_1': '$airline_pax_1',
                            'market_size_vlyr': '$market_vlyr',
                            'market_share': '$market_share',
                            'market_share_1': '$market_share_1',
                            'market_condition':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$and':
                                                        [
                                                            {'$ne':[{'$type': '$market_vlyr'}, 'string']},
                                                            {'$gte': ['$market_vlyr', threshold_growing]}
                                                        ]
                                                },
                                            'then': 'growing',
                                            'else':
                                                {
                                                    '$cond':
                                                        {
                                                            'if':
                                                                {
                                                                    '$and':
                                                                        [
                                                                            {'$ne': [{'$type': '$market_vlyr'}, 'string']},
                                                                            {'$lte': ['$market_vlyr', threshold_declining]}
                                                                        ]
                                                                },
                                                            'then': 'declining',
                                                            'else':
                                                                {
                                                                    '$cond':
                                                                        {
                                                                            'if':
                                                                                {
                                                                                    '$and':
                                                                                        [
                                                                                            {'$ne': [{'$type': '$market_vlyr'}, 'string']},
                                                                                            {'$lt': ['$market_vlyr', threshold_growing]},
                                                                                            {'$gt': ['$market_vlyr', threshold_declining]}
                                                                                        ]
                                                                                },
                                                                            'then': 'mature',
                                                                            'else': 'last year value is zero or not available'
                                                                        }
                                                                }
                                                        }
                                                }
                                        }
                                },
                            'niche_market':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$lte': ['$market_size', threshold_niche]
                                                },
                                            'then': 'yes',
                                            'else': 'no'
                                        }
                                },
                            'market_share_vlyr':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$and':
                                                        [
                                                            {'$gt': ['$market_share_1', 0]},
                                                            {'$ne': [{'$type': '$market_share_1'}, 'string']}

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
                                                                            '$subtract': ['$market_share',
                                                                                          '$market_share_1']
                                                                        },
                                                                        '$market_share_1'
                                                                    ]
                                                            }
                                                        ]
                                                },
                                            'else': 'market share last year is either zero or unavailable'
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
                                    'region': '$region',
                                    'country': '$country',
                                    'pos': '$pos',
                                    'od': '$od',
                                    'compartment': '$compartment',
                                    'market_size': '$market_size',
                                    'market_size_1': '$market_size_1',
                                    'market_size_vlyr': '$market_size_vlyr',
                                    'market_condition': '$market_condition',
                                    'niche_market': '$niche_market'
                                },
                            'airline_details':
                                {
                                    '$push':
                                        {
                                            'airline': '$airline',
                                            'airline_pax': '$airline_pax',
                                            'airline_pax_1': '$airline_pax_1',
                                            'market_share': '$market_share',
                                            'market_share_1': '$market_share_1',
                                            'market_share_vlyr': '$market_share_vlyr'
                                        }
                                }
                        }
                }
                ,
                {
                    '$out': collection_name
                }

                ]
            db.JUP_DB_Market_Share.aggregate(pipeline_market_outlook, allowDiskUse=True)

            if collection_name in db.collection_names():
                # pythonic variable given to the newly created collection
                market_outlook = db.get_collection(collection_name)

                if market_outlook.count()>0:
                    growing_market = market_outlook.find({'_id.market_condition': 'growing'}).count()
                    declining_market = market_outlook.find({'_id.market_condition': 'declining'}).count()
                    niche_market = market_outlook.find({'_id.niche_market': 'yes'}).count()
                    mature_market = market_outlook.find({'_id.market_condition': 'mature'}).count()
                    no_last_year = market_outlook.find({'_id.market_condition': 'last year value is zero or not available'}).count()
                    total_markets = growing_market + declining_market + mature_market + no_last_year
                    market_outlook.drop()

                    response = {
                        'growing': growing_market,
                        'declining': declining_market,
                        'mature': mature_market,
                        'niche': niche_market,
                        'no_lyr_markets': no_last_year,
                        'total_markets': total_markets
                    }
                    return response
                else: # in case the collection is empty
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                        'jupter_AI/tiles/market/market_outlook_screen.py method: outlook_market',
                                                        get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list('There is no data.')

                    obj_error.write_error_logs(datetime.datetime.now())
            else: # resultant collection is not created
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupter_AI/tiles/market/market_outlook_screen.py method: outlook_market',
                                                    get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list(
                    str(collection_name + ' not created in the database. Check aggregate pipeline.'))

                obj_error.write_error_logs(datetime.datetime.now())
        else: # in case collection to query is not found
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/market/market_outlook_screen.py method: outlook_market',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Market_Share_Last cannot be found in the database.')
            obj_error.write_error_logs(datetime.datetime.now())
    # handling pymongo errors
    except (pymongo.errors.ServerSelectionTimeoutError,
                    pymongo.errors.AutoReconnect,
                    pymongo.errors.CollectionInvalid,
                    pymongo.errors.ConfigurationError,
                    pymongo.errors.ConnectionFailure,
                    pymongo.errors.CursorNotFound,
                    pymongo.errors.ExecutionTimeout
                    ) as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'pymongo exception in jupter_AI/tiles/market/market_outlook_screen.py method: outlook_market',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())
    # to handle any other pythonic exception that may have occurred
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/market/market_outlook_screen.py method: outlook_market',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        # raise obj_error
        obj_error.write_error_logs(datetime.datetime.now())


# the below function calls the market barometer function. this function was created for UI inegration
def get_tiles(afilter):
    response = dict()
    response['Market_outlook'] = outlook_market(afilter)
    return response

if __name__ == '__main__':
    a = {
        'region': [],
        'country': [],
        'pos': [],
        'origin': ['DXB'],
        'destination': ['DOH'],
        'compartment': ['Y'],
        'fromDate': '2017-02-01',
        'toDate': '2017-02-28'
    }
    start_time = time.time()
    print get_tiles(afilter=a)
    # market_outlook = db.get_collection(collection_name)
    # market_outlook.drop()
    print (time.time() - start_time)
