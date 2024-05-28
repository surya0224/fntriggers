"""
File Name              :   PriorityAssignment
Author                 :   Shamail Mulla
Date Created           :   2017-04-04
Description            :  Priority for each level of hierarchy is assigned to all derived factors.

"""

import datetime
import time
from jupiter_AI.common import ClassErrorObject as error_class
from jupiter_AI.common.network_level_params import db
from jupiter_AI.common.network_level_params import Host_Airline_Code as host
from jupiter_AI.common.network_level_params import Host_Airline_Hub as hub
from jupiter_AI.tiles.jup_common_functions_tiles import query_month_year_builder
from jupiter_AI.tiles.jup_common_functions_tiles import gen_collection_name as gen
from jupiter_AI import JUPITER_LOGGER
from jupiter_AI.logutils import measure


result_collection_name = gen()


@measure(JUPITER_LOGGER)
def assign_priority_level():
    print 'assinging priorities'
    ppln_priority = [
        {
            '$project':
                {
                    'priority_level':
                        {
                            '$cond':
                                {
                                    'if':{'$eq': ['$origin_level', 'A']},
                                    'then': # origin level is city (airport)
                                        {
                                            '$cond':
                                                { # A-A level
                                                    'if': {'$eq': ['$destination_level', 'A']},
                                                    'then': {'$literal': 1},
                                                    'else':
                                                        {
                                                            '$cond':
                                                                { # A-C
                                                                    'if': {'$eq': ['$destination_level', 'C']},
                                                                    'then': {'$literal': 2},
                                                                    'else':
                                                                        {
                                                                            '$cond':
                                                                                { # A-R
                                                                                    'if': {'$eq': ['$destination_level','R']},
                                                                                    'then': {'$literal': 4},
                                                                                    'else': {'$literal': 5} # A-N
                                                                                }
                                                                        }
                                                                }
                                                        }

                                                }
                                        },
                                    'else': # origin level can be country or region or network
                                        {
                                            '$cond':
                                                {
                                                    'if': {'$eq': ['$origin_level', 'C']},
                                                    'then': # origin level is country
                                                        {
                                                            '$cond':
                                                                { # C-A
                                                                    'if': {'$eq': ['$destination_level', 'A']},
                                                                    'then': {'$literal': 3},
                                                                    'else':
                                                                        {
                                                                            '$cond':
                                                                                { # C-C
                                                                                    'if': {'$eq': ['$destination_level', 'C']},
                                                                                    'then': {'$literal': 8},
                                                                                    'else':
                                                                                        {
                                                                                            '$cond':
                                                                                                { # C-R
                                                                                                    'if': {'$eq': ['$destination_level','R']},
                                                                                                    'then': {'$literal': 9},
                                                                                                    'else': {'$literal': 11} # C-N
                                                                                                    }
                                                                                        }
                                                                                }
                                                                        }
                                                                }
                                                        },
                                                    'else': # origin level is region or network
                                                        {
                                                            '$cond':
                                                                {
                                                                    'if': {'$eq': ['$origin_level', 'R']},
                                                                    'then': # origin level is region
                                                                        {
                                                                            '$cond':
                                                                                {  # R-A
                                                                                    'if': {'$eq': ['$destination_level','A']},
                                                                                    'then': {'$literal': 5},
                                                                                    'else':
                                                                                        {
                                                                                            '$cond':
                                                                                                {  # R-C
                                                                                                    'if': {'$eq': ['$destination_level','C']},
                                                                                                    'then': {'$literal': 10},
                                                                                                    'else':
                                                                                                        {
                                                                                                            '$cond':
                                                                                                                {  # R-R
                                                                                                                    'if': {
                                                                                                                        '$eq': ['$destination_level','R']},
                                                                                                                    'then': {'$literal': 13},
                                                                                                                    'else': {'$literal': 14} # C-N
                                                                                                                }
                                                                                                        }
                                                                                                }
                                                                                        }
                                                                                }
                                                                        },
                                                                    'else':
                                                                        { # origin level is network
                                                                            '$cond':
                                                                                { # N-A
                                                                                    'if': {'$eq': ['$destination_level','A']},
                                                                                    'then': {'$literal': 7},
                                                                                    'else':
                                                                                        {
                                                                                            '$cond':
                                                                                                {  # N-C
                                                                                                    'if': {'$eq': ['$destination_level','C']},
                                                                                                    'then': {'$literal': 12},
                                                                                                    'else':
                                                                                                        {
                                                                                                            '$cond':
                                                                                                                {  # N-R
                                                                                                                    'if': {
                                                                                                                        '$eq': ['$destination_level','R']},
                                                                                                                    'then': {'$literal': 15},
                                                                                                                    'else': {'$literal': 16} # N-N
                                                                                                                }
                                                                                                        }
                                                                                                }
                                                                                        }
                                                                                }
                                                                        }
                                                                }
                                                        }
                                                }
                                        }
                                }
                        }
                }
        }
    ]
    db.JUP_DB_Capacity_Derived_Factor.aggregate(ppln_priority, allowDiskUse=True)
    print 'assignment complete'

if __name__ == '__main__':
    start_time = time.time()
    assign_priority_level()
    print time.time() - start_time