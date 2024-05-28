
"""
File Name              :   market_barometer_scn
Author                 :   Ashwin Kumar
Date Created           :   2016-12-20
Description            :  Market barometer screen which calculates the proximity indicator values for
                          all agents and categorizes them into friends and foes.

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

from jupiter_AI.common import ClassErrorObject as error_class
from jupiter_AI.network_level_params import Host_Airline_Code as host
from jupiter_AI import client, JUPITER_DB
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

def query_for_market_barometer(afilter):
    filter_pi = deepcopy(defaultdict(list, afilter))
    qry_pi = dict()
    if filter_pi['region']:
        qry_pi['region'] = {'$in': filter_pi['region']}
    if filter_pi['country']:
        qry_pi['country'] = {'$in': filter_pi['country']}
    if filter_pi['pos']:
        qry_pi['pos'] = {'$in': filter_pi['pos']}
    if afilter['compartment']:
        qry_pi['compartment'] = {'$in': filter_pi['compartment']}
    if filter_pi['origin'] and filter_pi['destination']:
        od = ''.join(filter_pi['origin'] + filter_pi['destination'])
        qry_pi['od'] = od
    from_obj = datetime.datetime.strptime(filter_pi['fromDate'], '%Y-%m-%d')
    to_obj = datetime.datetime.strptime(filter_pi['toDate'], '%Y-%m-%d')

    # the following code converts the dates to month and year values. the below function
    # is taken from the common functions for tiles program

    month_year = query_month_year_builder(from_obj.month,
                                          from_obj.year,
                                          to_obj.month,
                                          to_obj.year)
    qry_pi['$or'] = month_year

    return qry_pi


def proximity_indicator_screen(afilter):
    try:
        if 'JUP_DB_Market_Share_Last' in db.collection_names():
            # print 'Collection Present'
            # the following code is done to check performance of the code in seconds
            start_time = time.time()

            # A market is defined as the combination of Region Country POS OD compartment and RBD. In the following
            # aggregate function, the market does not consider the RBD as the data is not available for competitors
            # at an RBD level

            qry_pi = query_for_market_barometer(afilter)

            apipeline_user = [
                # the following pipeline matches the documents from the collection with the filter values
                {
                    '$match': qry_pi
                }
                ,
                # the following group function calculates the total pax, rev, pax_lyr and rev_lyr
                # for each agent and each airline operating in a given region country pos od compartment
                # from the previous pipeline stage
                {
                    '$group':
                        {
                            '_id':
                                {
                                    'agent': '$agent',
                                    'region': '$region',
                                    'country': '$country',
                                    'pos': '$pos',
                                    'od': '$od',
                                    'carrier': '$MarketingCarrier1',
                                    'compartment': '$compartment',
                                },
                            'pax': {'$sum': '$pax'},
                            'rev': {'$sum': '$revenue'},
                            'pax_lyr': {'$sum': '$pax_1'},
                            'rev_lyr': {'$sum': '$revenue_1'}
                               }
                }
                ,
                # The following group function calculates the total pax, rev, pax_lyr and rev_lyr
                # for each agent alone. Airline details preserves the airline name, pax, rev, rev_lyr, pax_lyr
                # values for which the agent has sold tickets for
                {
                    '$group':
                        {
                            '_id':
                            {
                                'agent': '$_id.agent',
                                'region': '$_id.region',
                                'country': '$_id.country',
                                'pos': '$_id.pos',
                                'od': '$_id.od',
                                'compartment': '$_id.compartment'
                             },
                            'tot_pax': {'$sum': '$pax'},
                            'tot_rev': {'$sum': '$rev'},
                            'tot_pax_lyr': {'$sum': '$pax_lyr'},
                            'tot_rev_lyr': {'$sum': '$rev_lyr'},
                            'airline_details':
                                {
                                 '$push':
                                     {
                                      'airline_name': '$_id.carrier',
                                      'pax': '$pax',
                                      'rev': '$rev',
                                      'pax_lyr': '$pax_lyr',
                                      'rev_lyr': '$rev_lyr'
                                     }
                                }
                        }
                }
                ,
                # the following unwind is needed to unwind the Airline details list. This helps in further grouping based on
                # airlines, for each element of the list Airline details an individual document is created
                {
                    '$unwind': '$airline_details'
                }
                ,
                # The following group function is required to group the results of the above pipeline in terms of
                # airlines region country pos od and compartment. This group function will calculate the total revenue
                # and pax generated by an airline for a given market. The agent details calculated in the previous pipeline
                # are pushed into an array called Agent Airline details. This helps in preserving the fields that were computed
                # in the previous stage
                {
                    '$group':
                        {
                            '_id':
                                {
                                    'airline': '$airline_details.airline_name',
                                    'region': '$_id.region',
                                    'country': '$_id.country',
                                    'pos': '$_id.pos','od': '$_id.od',
                                    'compartment': '$_id.compartment'
                                },
                            'airline_pax': {'$sum': '$airline_details.pax'},
                            'airline_pax_lyr': {'$sum': '$airline_details.pax_lyr'},
                            'airline_rev': {'$sum': '$airline_details.rev'},
                            'airline_rev_lyr': {'$sum': '$airline_details.rev_lyr'},
                            'agent_airline_details':
                                {
                                    '$push':
                                        {
                                           'pax_airline_agent': '$airline_details.pax',
                                           'pax_airline_agent_lyr': '$airline_details.pax_lyr',
                                           'rev_airline_agent': '$airline_details.rev',
                                           'rev_airline_agent_lyr': '$airline_details.rev_lyr',
                                           'agent': '$_id.agent',
                                           'agent_pax': '$tot_pax',
                                           'agent_pax_lyr': '$tot_pax_lyr',
                                           'agent_rev': '$tot_rev',
                                           'agent_rev_lyr': '$tot_rev_lyr',
                                        }
                                }
                        }
                }
                ,
                # the following code calculates the total revenue, pax, revenue lyr and pax lyr for a given market.
                # the definition of a market is defined in the 54th line. details regarding individual airlines and their
                # respective agents are being preserved for further aggregation. The following pipeline is used to compute
                # the market size and revenue for each market which will further be used for calculation of market share

                {
                    '$group':
                        {
                            '_id':
                                {
                                    'region': '$_id.region',
                                    'country': '$_id.country',
                                    'pos': '$_id.pos',
                                    'od': '$_id.od',
                                    'compartment': '$_id.compartment'
                                },
                            'tot_pax': {'$sum': '$airline_pax'},
                            'tot_pax_lyr': {'$sum': '$airline_pax_lyr'},
                            'tot_rev': {'$sum': '$airline_rev'},
                            'tot_rev_lyr': {'$sum': '$airline_rev'},
                            'all_details':
                                {
                                    '$push':
                                        {
                                            'airline': '$_id.airline',
                                            'airline_pax': '$airline_pax',
                                            'agent_airline_details': '$agent_airline_details',
                                            'airline_pax_lyr': '$airline_pax_lyr',
                                            'airline_rev': '$airline_rev',
                                            'airline_rev_lyr': '$airline_rev_lyr'
                                        }
                                }
                        }
                }
                ,
                # the following code unwinds the elements in the array all details to create a separate document for each element
                {
                    '$unwind': '$all_details'
                }
                ,
                # the following code unwinds the elements in the array agent airline details to create a separate line for each element
                # of the list
                {
                    '$unwind': '$all_details.agent_airline_details'
                }
                ,
                # the following project pipeline stage renames all the necessary fields and calculates market share of each airline,
                # market share in terms of revenue, market share vlyr, agent to airline share pax, agent to airline share rev,
                # agent to airline share pax lyr, agent to airline share rev lyr.
                {
                    '$project':
                        {
                            '_id': 1,
                            'country': '$_id.country',
                            'region': '$_id.region',
                            'pos': '$_id.pos',
                            'od': '$_id.od',
                            'compartment': '$_id.compartment',
                            'airline': '$all_details.airline',
                            'tot_pax_market': '$tot_pax',
                            'tot_pax_lyr_market': '$tot_pax_lyr',
                            'tot_rev_market': '$tot_rev',
                            'tot_rev_lyr_market': '$tot_rev_lyr',
                            'agent': '$all_details.agent_airline_details.agent',
                            'pax_airline_agent': '$all_details.agent_airline_details.pax_airline_agent',
                            'pax_airline_agent_lyr': '$all_details.agent_airline_details.pax_airline_agent_lyr',
                            'rev_airline_agent': '$all_details.agent_airline_details.rev_airline_agent',
                            'rev_airline_agent_lyr': '$all_details.agent_airline_details.rev_airline_agent_lyr',
                            'agent_tot_pax': '$all_details.agent_airline_details.agent_pax',
                            'agent_tot_rev': '$all_details.agent_airline_details.agent_rev',
                            'agent_tot_pax_lyr': '$all_details.agent_airline_details.agent_pax_lyr',
                            'agent_tot_rev_lyr': '$all_details.agent_airline_details.agent_rev_lyr',
                            'airline_pax': '$all_details.airline_pax',
                            'airline_pax_lyr': '$all_details.airline_pax_lyr',
                            'airline_rev': '$all_details.airline_rev',
                            'airline_rev_lyr':'$all_details.airline_rev_lyr',
                            'market_share_airline':
                                {
                                    '$cond':
                                        {
                                            'if': {'$gt': ['$all_details.airline_pax', 0]},
                                            'then':
                                                {
                                                    '$cond':
                                                        {
                                                            'if': {'$gt': ['$tot_pax', 0]},
                                                            'then': {'$divide': ['$all_details.airline_pax','$tot_pax']},
                                                            'else':0
                                                        }
                                                },
                                            'else':0
                                        }
                                },
                            'market_share_airline_rev':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$gt': ['$all_details.airline_rev', 0]},
                                            'then':
                                                {
                                                    '$cond':
                                                        {
                                                            'if':
                                                                {
                                                                    '$gt': ['$tot_rev', 0]
                                                                },
                                                            'then':
                                                                {
                                                                    '$divide': ['$all_details.airline_rev',
                                                                                '$tot_rev']
                                                                },
                                                            'else':0
                                                        }
                                                },
                                            'else':0
                                        }
                                },

                            'market_share_airline_lyr':
                                {
                                    '$cond':
                                         {
                                             'if':
                                                 {
                                                     '$gt':['$all_details.airline_pax_lyr',0]
                                                 },
                                             'then':
                                                 {
                                                    '$cond':
                                                        {
                                                            'if':
                                                                {
                                                                    '$gt': ['$tot_pax_lyr',0]
                                                                },
                                                            'then':
                                                                {
                                                                    '$divide': ['$all_details.airline_pax_lyr',
                                                                                '$tot_pax_lyr']
                                                                },
                                                            'else':0
                                                         }
                                                 },
                                             'else':0
                                         }
                                },
                            'agent_to_airline_share':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$gt': ['$all_details.agent_airline_details.pax_airline_agent', 0]
                                                },
                                            'then':
                                                {
                                                    '$cond':
                                                        {
                                                            'if':
                                                                {
                                                                    '$gt': ['$all_details.agent_airline_details.agent_pax',0]
                                                                },
                                                            'then':
                                                                {
                                                                    '$divide': ['$all_details.agent_airline_details.pax_airline_agent',
                                                                                '$all_details.agent_airline_details.agent_pax']
                                                                },
                                                            'else':0
                                                        }
                                                },
                                            'else':0
                                        }
                                },
                            'agent_to_airline_share_rev':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$gt': ['$all_details.agent_airline_details.rev_airline_agent', 0]
                                                },
                                            'then':
                                                {
                                                    '$cond':
                                                        {
                                                            'if':
                                                                {
                                                                    '$gt': ['$all_details.agent_airline_details.agent_rev',0]
                                                                },
                                                            'then':
                                                                {
                                                                    '$divide': ['$all_details.agent_airline_details.rev_airline_agent',
                                                                                '$all_details.agent_airline_details.agent_rev']
                                                                },
                                                            'else':0
                                                        }
                                                },
                                            'else':0
                                        }
                                },
                            'agent_to_airline_share_lyr':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$gt': ['$all_details.agent_airline_details.pax_airline_agent_lyr',0]
                                                },
                                            'then':
                                                {
                                                    '$cond':
                                                        {
                                                            'if':
                                                                {
                                                                    '$gt': ['$all_details.agent_airline_details.agent_pax_lyr',0]
                                                                },
                                                            'then':
                                                                {
                                                                    '$divide': ['$all_details.agent_airline_details.pax_airline_agent_lyr',
                                                                                '$all_details.agent_airline_details.agent_pax_lyr']
                                                                },
                                                            'else':0
                                                        }
                                                },
                                            'else':0
                                        }
                                },
                            'agent_to_airline_share_rev_lyr':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$gt': ['$all_details.agent_airline_details.rev_airline_agent_lyr', 0]
                                                },
                                            'then':
                                                {
                                                    '$cond':
                                                        {
                                                            'if':
                                                                {
                                                                    '$gt': ['$all_details.agent_airline_details.agent_rev_lyr', 0]
                                                                },
                                                            'then':
                                                                {
                                                                    '$divide': ['$all_details.agent_airline_details.rev_airline_agent_lyr',
                                                                                '$all_details.agent_airline_details.agent_rev_lyr']
                                                                },
                                                            'else': 0
                                                        }
                                                },
                                            'else': 0
                                        }
                                }
                        }
                }
                ,
                # the following pipeline stage is used to calculate the proximity indicator pi in terms of revenue and pax
                {
                    '$project':
                        {
                            '_id': 1,
                            'country': '$country',
                            'region': '$region',
                            'pos': '$pos',
                            'od': '$od',
                            'compartment': '$compartment',
                            'airline': '$airline',
                            'tot_pax_market': '$tot_pax_market',
                            'tot_pax_lyr_market': '$tot_pax_lyr_market',
                            'tot_rev_market': '$tot_rev_market',
                            'tot_rev_lyr_market': '$tot_rev_lyr_market',
                            'agent': '$agent',
                            'pax_airline_agent': '$pax_airline_agent',
                            'pax_airline_agent_lyr':'$pax_airline_agent_lyr',
                            'rev_airline_agent':'$rev_airline_agent',
                            'rev_airline_agent_lyr':'$rev_airline_agent_lyr',
                            'agent_tot_pax': '$agent_tot_pax',
                            'agent_tot_rev': '$agent_tot_rev',
                            'agent_tot_pax_lyr': '$agent_tot_pax_lyr',
                            'agent_tot_rev_lyr': '$agent_tot_rev_lyr',
                            'airline_pax': '$airline_pax',
                            'airline_pax_lyr': '$airline_pax_lyr',
                            'airline_rev': '$airline_rev',
                            'airline_rev_lyr': '$airline_rev_lyr',
                            'agent_to_airline_share': '$agent_to_airline_share',
                            'market_share_airline':'$market_share_airline',
                            'pi_pax':
                                {
                                    '$multiply': [100,
                                                  {
                                                      '$multiply': ['$agent_tot_pax',
                                                                    {
                                                                        '$subtract': ['$agent_to_airline_share',
                                                                                      '$market_share_airline']
                                                                    }
                                                                    ]
                                                  }
                                                  ]
                            },
                            'pi_rev':
                                {
                                    '$multiply': [100,
                                                  {
                                                     '$multiply': ['$agent_tot_rev',
                                                                   {
                                                                         '$subtract': ['$agent_to_airline_share_rev',
                                                                                       '$market_share_airline_rev']
                                                                   }
                                                                   ]
                                                  }
                                                  ]
                                },
                            'revenue_vlyr_agent':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$gt': ['$agent_to_airline_share_rev_lyr',0]
                                                },
                                            'then':
                                                {
                                                    '$cond':
                                                        {
                                                            'if':
                                                                {
                                                                    '$gt': ['$agent_to_airline_share_rev',0]
                                                                },
                                                            'then':
                                                                {
                                                                    '$multiply':
                                                                        [
                                                                            {
                                                                                '$divide':
                                                                                    [
                                                                                        {
                                                                                            '$subtract': ['$agent_to_airline_share_rev',
                                                                                                          '$agent_to_airline_share_rev_lyr'
                                                                                                         ]
                                                                                        },
                                                                                        '$agent_to_airline_share_rev_lyr'
                                                                                    ]
                                                                            },
                                                                            100
                                                                        ]
                                                                },
                                                            'else':0
                                                        }
                                                },
                                            'else':0
                                        }
                                }
                        }
                }
                ,
                # the following code renames the field names for further aggregation stages and calculates market share in terms
                # of percentage
                {
                    '$project':
                        {
                            '_id': 1,
                            'country': '$country',
                            'region': '$region',
                            'pos': '$pos',
                            'od': '$od',
                            'compartment': '$compartment',
                            'airline': '$airline',
                            'agent': '$agent',
                            'pax_airline_agent': '$pax_airline_agent',
                            'pax_airline_agent_lyr': '$pax_airline_agent_lyr',
                            'rev_airline_agent': '$rev_airline_agent',
                            'rev_airline_agent_lyr': '$rev_airline_agent_lyr',
                            'tot_pax_market': '$tot_pax_market',
                            'tot_pax_lyr_market': '$tot_pax_lyr_market',
                            'tot_rev_market': '$tot_rev_market',
                            'tot_rev_lyr_market': '$tot_rev_lyr_market',
                            'agent_tot_pax': '$agent_tot_pax',
                            'agent_tot_rev': '$agent_tot_rev',
                            'agent_tot_pax_lyr': '$agent_tot_pax_lyr',
                            'agent_tot_rev_lyr': '$agent_tot_rev_lyr',
                            'pi_pax': '$pi_pax',
                            'pi_rev': '$pi_rev',
                            'revenue_vlyr_agent': '$revenue_vlyr_agent',
                            'airline_pax': '$airline_pax',
                            'airline_pax_lyr': '$airline_pax_lyr',
                            'airline_rev': '$airline_rev',
                            'airline_rev_lyr': '$airline_rev_lyr',
                            'agent_to_airline_share': '$agent_to_airline_share',
                            'market_share_airline':
                                {
                                    '$multiply': ['$market_share_airline',100]
                                }
                        }
                }
                ,
                # the following pipeline identifies friends and foes wrt revenue and pax based on the proximity indicator values
                {
                    '$project':
                        {
                            '_id': 0,
                            'country': '$country',
                            'region': '$region',
                            'pos': '$pos',
                            'od': '$od',
                            'compartment': '$compartment',
                            'airline': '$airline',
                            'agent': '$agent',
                            'pax_airline_agent': '$pax_airline_agent',
                            'pax_airline_agent_lyr': '$pax_airline_agent_lyr',
                            'rev_airline_agent': '$rev_airline_agent',
                            'rev_airline_agent_lyr': '$rev_airline_agent_lyr',
                            'tot_pax_market': '$tot_pax_market',
                            'tot_pax_lyr_market': '$tot_pax_lyr_market',
                            'tot_rev_market': '$tot_rev_market',
                            'tot_rev_lyr_market': '$tot_rev_lyr_market',
                            'agent_tot_pax': '$agent_tot_pax',
                            'agent_tot_rev': '$agent_tot_rev',
                            'agent_tot_pax_lyr': '$agent_tot_pax_lyr',
                            'agent_tot_rev_lyr': '$agent_tot_rev_lyr',
                            'pi_pax': '$pi_pax',
                            'pi_rev': '$pi_rev',
                            'friend_or_foe_rev':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$gte': ['$pi_rev', 0]
                                                },
                                            'then': 'friend',
                                            'else': 'foe'
                                        }
                                },
                            'friend_or_foe_pax':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$gte': ['$pi_pax', 0]
                                                },
                                            'then': 'friend',
                                            'else': 'foe'
                                        }
                                },
                            'airline_pax': '$airline_pax',
                            'airline_pax_lyr': '$airline_pax_lyr',
                            'airline_rev': '$airline_rev',
                            'airline_rev_lyr': '$airline_rev_lyr',
                            'agent_to_airline_share': '$agent_to_airline_share',
                            'market_share_airline': '$market_share_airline',
                            'top3_competitors_flag':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$eq': ['$airline',host]
                                                },
                                            'then': 1,
                                            'else': 0
                                        }
                                }
                        }
                }
                ,
                # The following stage outputs the results of this aggregation pipeline to a coll
                # after the string in the results_collection variable. this is done because pipe
                # have a RAM limit of 100 MB and if a stage exceeds this the aggregation fails.
                {
                    '$out': collection_name
                }
            ]

            db.JUP_DB_Market_Share_Last.aggregate(apipeline_user, allowDiskUse=True)

            if collection_name in db.collection_names():
                # pythonic variable given to the newly created collection
                prox_ind = db.get_collection(collection_name)
                if prox_ind.count > 0:

                    # market barometer tile values being calculated for available data
                    friends_rev = prox_ind.find({'airline': host, 'friend_or_foe_rev': 'friend'}).count()
                    foes_rev = prox_ind.find({'airline': host, 'friend_or_foe_rev': 'foe'}).count()
                    friends_pax = prox_ind.find({'airline': host, 'friend_or_foe_pax': 'friend'}).count()
                    foes_pax = prox_ind.find({'airline': host, 'friend_or_foe_pax': 'foe'}).count()

                    prox_ind.drop()
                    response = dict()

                    # tile values being stored in dictionary for UI to read
                    response['Total_Friends_Rev'] = friends_rev
                    response['Total_Foes_Rev'] = foes_rev
                    response['Total_Agents_Rev'] = friends_rev + foes_rev
                    response['Total_Friends_Pax'] = friends_pax
                    response['Total_Foes_Pax'] = foes_pax
                    response['Total_Agents_Pax'] = friends_pax + foes_pax
                    response['Proximity_Indicator_Rev'] = 0  # hardcoded will have to change
                    response['Proximity_Indicator_Pax'] = 0  # hardcoded will have to change

                    # the collection created in the above aggregate function gets dropped from the database


                    return response

                else: # in case the collection is empty
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                        'jupter_AI/tiles/market/market_barometer_scn.py method: get_revenue_pax',
                                                        get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list('There is no data.')

                    obj_error.write_error_logs(datetime.datetime.now())
            else: # in case resultant collection is not created
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupiter_AI/tiles/market/market_barometer_scn.py method: get_revenue_pax',
                                                    get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list(
                    str(collection_name + ' not created in the database. Check aggregate pipeline.'))

                obj_error.write_error_logs(datetime.datetime.now())

            # total time taken for aggregation being displayed
            print time.time() - start_time

        else: # in case collection to query is not found
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/market/market_barometer_scn.py method: proximity_indicator_screen',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Market_Share_Last cannot be found in the database.')
            obj_error.write_error_logs(datetime.datetime.now())

    # to handle any other pythonic exception that may have occurred
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupiter_AI/tiles/market/market_barometer_scn.py method: proximity_indicator_screen',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())

# the below function calls the market barometer function. this function was created for UI inegration
def get_tiles(afilter):
    response = dict()
    response['Market_barometer'] = proximity_indicator_screen(afilter)
    return response

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
    print get_tiles(afilter=a)
    print (time.time() - start_time)

