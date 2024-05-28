"""
File Name              :   runrate_DAL
Author                 :   Shamail Mulla
Date Created           :   2017-01-06
Description            :  This file accesses the db to calculate the bookings runrate for the given filter

MODIFICATIONS LOG
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :
"""

import datetime
import inspect
import time
from jupiter_AI import client, JUPITER_DB
db = client[JUPITER_DB]
from jupiter_AI.common import ClassErrorObject as error_class
from jupiter_AI.network_level_params import Host_Airline_Code as host

from jupiter_AI.tiles.jup_common_functions_tiles import gen_collection_name as gen

upper_threshold = 1  # Hard coded-- should be replaced by values from the configuration screen module
lower_threshold = 0.5  # Hard coded-- should be replaced by values from the configuration screen module
aggregate_upper_threshold = 50  # Hard coded-- should be replaced by values from the configuration screen module
aggregate_lower_threshold = 10  # Hard coded-- should be replaced by values from the configuration screen module

R_n_A_response = []
result_coll_name = gen()


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


def query_builder(dict_filter):
    query = dict()
    if dict_filter['region']:
        query['region'] = {'$in': dict_filter['region']}
    if dict_filter['country']:
        query['country'] = {'$in': dict_filter['country']}
    if dict_filter['pos']:
        query['pos'] = {'$in': dict_filter['pos']}
    if dict_filter['origin']:
        od_build = []
        for idx, item in enumerate(dict_filter['origin']):
            od_build.append({'origin': item, 'destination': dict_filter['destination'][idx]})
        query['$or'] = od_build
    if dict_filter['compartment']:
        query['compartment'] = {'$in': dict_filter['compartment']}
    if dict_filter['flag'] == 0:  # for dep date
        query['dep_date'] = {'$gte': dict_filter['fromDate'], '$lte': dict_filter['toDate']}
    #    date_type = '$dep_date'
    # elif dict_filter['flag'] == 1:  # for book date
    #     query['book_date'] = {'$gte': dict_filter['fromDate'], '$lte': dict_filter['toDate']}
    #     date_type = '$book_date'

    return query


def get_days(start_date, end_date):
    obj_start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    obj_end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    days = (obj_end_date - obj_start_date).days + 1
    return days


def analyse_price_runrate(dict_filter):
    try:
        coll_to_query = 1
        response = dict()
        # check if collectiones to query are present
        if 'JUP_DB_Booking_BookDate' not in db.collection_names():
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupiter_AI/RnA/runrate_DAL.py method: analyse_price_runrate',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Booking_BookDate cannot be found in the database.')
            obj_error.write_error_logs(datetime.datetime.now())
            coll_to_query = 0

        if 'JUP_DB_Schedule_Capa' not in db.collection_names():
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupiter_AI/RnA/runrate_DAL.py method: analyse_price_runrate',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Schedule_Capa cannot be found in the database.')
            obj_error.write_error_logs(datetime.datetime.now())
            coll_to_query = 0

        if coll_to_query == 1:
            query = query_builder(dict_filter)

            days = get_days(dict_filter['fromDate'],dict_filter['toDate'])
            print days, type(days)
            ppln_runrate = [
                {
                    '$match': query
                }
                ,
                # retrieving bookings and ticketed for every od, compartment and date
                {
                    '$group':
                        {
                            '_id':
                            {
                                # 'date': '$dep_date',
                                'od': '$od',
                                'comp': '$compartment'
                            },
                            'bookings': {'$sum': '$pax'},
                            'ticketed': {'$sum': '$ticket'}
                       }
                }
                ,
                # converting compartments according to the following conversions
                # depdate (comp) - compartment (new field)
                # A                 F
                # J                 J
                # Y                 Y
                {
                    '$project':
                        {
                            '_id': 1,
                            # 'date': '$_id.date',
                            'od': '$_id.od',
                            'bookings': '$bookings',
                            'ticketed': '$ticketed',
                            'compartment':
                                {
                                    '$cond':
                                        {
                                            'if': { '$eq': ['$_id.comp', 'A'] },
                                            'then': 'F',
                                            'else': '$_id.comp'
                                        }
                                }
                        }
                }
                ,
                # Grouping by only OD to match documents from capacity collection
                {
                    '$group':
                        {
                            '_id':
                                {
                                    'od': '$_id.od'
                                },
                            'b_t_details':
                                {
                                    '$push':
                                        {
                                            'compartment': '$compartment',
                                            # 'date': '$_id.date',
                                            'ticketed': '$ticketed',
                                            'bookings': '$bookings'
                                        }
                                }
                        }
                }
                ,
                # calculating number of ODs matched
                {
                    '$group':
                        {
                            '_id': 0,
                            'details':
                                {
                                    '$push':
                                        {
                                            'od': '$_id.od',
                                            'b_t_details': '$b_t_details'
                                        }
                                },
                            'total_ods': {'$sum': 1}
                        }
                }
                ,
                {
                    '$unwind': '$details'
                }
                ,
                # getting documents by OD again
                {
                    '$project':
                        {
                            '_id': 0,
                            'od': '$details.od',
                            'b_t_details': '$details.b_t_details',
                            'total_ods': '$total_ods'
                        }
                }
                ,
                # retrieving capacity for each unique od
                {
                    '$lookup':
                        {
                            'from': 'JUP_DB_Schedule_Capa',
                            'localField': 'od',
                            'foreignField': 'od',
                            'as': 'capacity_collection'
                        }
                }
                ,
                {
                    '$unwind': '$b_t_details'
                }
                ,
                # selecting only host capacity details for matching dates
                {
                    '$addFields':
                        {
                            'capacity_collection':
                                {
                                    '$filter':
                                        {
                                            'input': '$capacity_collection',
                                            'as': 'capacity_collection',
                                            'cond':
                                                {
                                                    '$and':
                                                        [
                                                            {'$eq': ['$$capacity_collection.airline', host]},
                                                            {'$gte': ['$$capacity_collection.dep_date', dict_filter['fromDate']]},
                                                            {'$lte': ['$$capacity_collection.dep_date', dict_filter['toDate']]}
                                                        ]
                                                }
                                        }
                                }
                        }
                }

                ,
                {
                    '$project':
                        {
                            '_id': 1,
                            # 'date': '$b_t_details.date',
                            'od': '$od',
                            'compartment': '$b_t_details.compartment',
                            'total_ods': '$total_ods',
                            'ticketed': '$b_t_details.ticketed',
                            'bookings': '$b_t_details.bookings',
                            'capacity_collection': '$capacity_collection'
                        }
                }
                ,
                {
                    '$unwind': '$capacity_collection'
                }
                ,
                # creating intermediate collection having capacities by od, compartment
                # matching the compartments as given below
                # depdate - scheduled capa
                # F         F
                # J         C
                # Y         Y (economy), W(premium economy)
                {
                    '$group':
                        {
                            '_id':
                                {
                                    # 'date': '$date',
                                    'od': '$od',
                                    'compartment': '$compartment',
                                    'total_ods': '$total_ods',
                                    'ticketed': '$ticketed',
                                    'bookings': '$bookings'
                                },

                            'F_capacity': {'$sum': '$capacity_collection.F'},
                            'J_capacity': {'$sum': '$capacity_collection.C'},
                            'W_capacity': {'$sum': '$capacity_collection.W'},
                            'Y_capacity': {'$sum': '$capacity_collection.Y'}
                        }
                }
                ,
                {
                    '$project':
                        {
                            '_id': 1,
                            # 'date': '$_id.date',
                            'od': '$_id.od',
                            'compartment': '$_id.compartment',
                            'bookings': '$_id.bookings',
                            'ticketed': '$_id.ticketed',
                            'total_ods': '$_id.total_ods',
                            'F_capacity': '$F_capacity',
                            'J_capacity': '$J_capacity',
                            'Y_capacity': {'$add': ['$W_capacity','$Y_capacity']}
                        }
                }
                # start from here
                ,
                {
                    '$project':
                        {
                            '_id': 1,
                            'od': '$_id.od',
                            'compartment': '$_id.compartment',
                            'bookings': '$bookings',
                            'ticketed': '$ticketed',
                            'total_ods': '$_id.total_ods',
                            'capacity':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$eq': ['$compartment', 'Y']
                                                },
                                            'then': '$Y_capacity',
                                            'else':
                                                { # either compartment F or J or NA
                                                    '$cond':
                                                        {
                                                            'if':
                                                                {
                                                                    '$eq': ['$compartment', 'J'],
                                                                },
                                                            'then': '$J_capacity',
                                                            'else': # either compartment F or NA
                                                                {
                                                                    '$cond':
                                                                        {
                                                                            'if':
                                                                                {
                                                                                    '$eq': ['$compartment', 'F'],
                                                                                },
                                                                            'then': '$F_capacity',
                                                                            'else':'NA'
                                                                        }
                                                                }
                                                        }
                                                }
                                        }
                                }
                        }
                }
                ,
                # # finding runrate and ticketed percentage
                {
                    '$project':
                        {
                            '_id': 1,
                            'od': '$od',
                            'compartment': '$compartment',
                            'total_ods': '$total_ods',
                            'runrate':
                                {
                                    '$divide': ['$bookings', days]
                                },
                            'ticketed_perc':
                                {
                                    '$cond':
                                        {
                                            'if':
                                                {
                                                    '$gt': ['$capacity', 0]
                                                },
                                            'then':
                                                {
                                                    '$divide': ['$ticketed', '$capacity']
                                                },
                                            'else': 'NA'
                                        }
                                }
                        }
                }
                ,
                # checking for low runrate and ticketed
                {
                    '$project':
                        {
                            '_id': 1,
                             'od': '$od',
                            'compartment': '$compartment',
                            'runrate': '$runrate',
                            'total_ods': '$total_ods',
                            'flag_low_rr': # give number of ods with low runrate
                                {
                                    '$cond':
                                        {
                                            'if':{'$lt': ['$runrate', lower_threshold]},
                                            'then': 1,
                                            'else': 0
                                        }
                                },
                            'flag_low_ticketed':
                                {
                                    '$cond':
                                        {
                                            'if':{'$lt': ['$ticketed_perc', 0.9]},
                                            'then': 1,
                                            'else': 0
                                        }
                                }
                        }
                }
                # ,
                # # DO NOT DO THIS
                # # removing ODs which have more than higher runrate
                # {
                #     '$redact':
                #         {
                #             '$cond':
                #                 {
                #                     'if':
                #                         {
                #                             {'$eq': ['$flag_low_rr', 0]}
                #                         },
                #                     'then': '$$DESCEND',
                #                     'else': '$$PRUNE'
                #                 }
                #         }
                # }
                ,
                # grouping ODs based on ticketed
                {
                    '$group':
                        {
                            '_id':
                                {
                                    'flag_low_runrate': '$flag_low_rr',
                                },
                            'runrate_details':
                            {
                                '$push':
                                    {
                                        'od':'$od',
                                        'compartment': '$compartment',
                                        'runrate': '$runrate',
                                        'flag_low_ticketed': '$flag_low_ticketed',
                                        'total_ods': '$total_ods'
                                    }

                            },
                            'high_low_runrate': {'$sum': 1}
                        }
                }
                # ,
                # # creating RnA responses
                # {
                #     '$project':
                #         {
                #             '_id': None,
                #             'od': '$od',
                #             'what': 'Negative bookings',
                #             'why': 'Low runrate',
                #             'status_quo': 'Further drop in market share',
                #             'action':
                #                 {
                #                     '$cond':
                #                         {
                #                             'if':
                #                                 {
                #                                     {'$eq': ['$_id.flag_low_ticketed', 1]}
                #                                 },
                #                             'then': 'Change price according to model',
                #                             'else': 'No action required as 90 percent of capacity is already ticketed'
                #                         }
                #                 }
                #         }
                # }
                ,
                {
                    '$out': result_coll_name
                }
            ]

            db.JUP_DB_Booking_DepDate.aggregate(ppln_runrate, allowDiskUse=True)
            # lst_cursor = list(cursor)

            # for document in lst_cursor:
            #     print document

            # print len(lst_cursor)

            if result_coll_name in db.collection_names():
                # pythonic variable given to the newly created collection
                rr_details = db.get_collection(result_coll_name)
                print 'rr_details',rr_details.count()
                if rr_details.count() == 2: # in case the collection is empty
                    low_rr_ods = rr_details.find({'_id.flag_low_runrate':1})
                    high_rr_ods = rr_details.find({'_id.flag_low_runrate':0})

                    lst_low_rr_ods = list(low_rr_ods)
                    lst_high_rr_ods = list(high_rr_ods)

                    rr_details.drop()
                    print lst_high_rr_ods
                    print lst_low_rr_ods

                else: # resultant collection is empty
                    rr_details.drop()
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                        'jupter_AI/RnA/runrate_DAL.py method: analyse_price_runrate',
                                                        get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list('There is no proper data in result collection.')

                    obj_error.write_error_logs(datetime.datetime.now())
            else:  # in case resultant collection is not created
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupter_AI/RnA/runrate_DAL.py method: analyse_price_runrate',
                                                    get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list('Runrate data collection not created in the database. Check aggregate pipeline.')

                obj_error.write_error_logs(datetime.datetime.now())

    # to handle all exception that may have occurred
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/RnA/runrate_DAL.py method: analyse_price_runrate',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())

if __name__ == '__main__':
    filter = {
        'region': [],
        'country': [],
        'pos': [],
        'origin': ['DXB','DXB'],
        'destination': ['ROV', 'DOH'],
        'compartment': [],
        'fromDate': '2016-12-01',
        'toDate': '2017-01-30',
        # 'from_month': 8,
        # 'from_year': 2016,
        # 'to_month': 12,
        # 'to_year': 2016,
        'flag': 0 # not required
    }
    start_time = time.time()
    # print db.JUP_DB_Booking_DepDate.distinct('compartment')
    analyse_price_runrate(filter)
    print time.time() - start_time