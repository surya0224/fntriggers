"""
File Name              :   JUP_AI_Golden_Flights.py
Author                 :   Shamail Mulla
Date Created           :   2017-04-20
Description            :  Monthly competitor capacities are checked to see if there are any new competitors or if any
                        competitor has existed.

"""

from copy import deepcopy
from collections import defaultdict
import datetime
import time
import inspect

try:
    from jupiter_AI import client, JUPITER_DB, JUPITER_LOGGER
    from jupiter_AI.logutils import measure
    db = client[JUPITER_DB]
except:
    pass
from jupiter_AI.common import ClassErrorObject as error_class
from jupiter_AI.tiles.jup_common_functions_tiles import gen_collection_name as gen

end_date = datetime.datetime.today()


@measure(JUPITER_LOGGER)
def get_module_name():
    return inspect.stack()[1][3]


@measure(JUPITER_LOGGER)
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



@measure(JUPITER_LOGGER)
def get_configurations():
    """
    Picks up all the configurations for the golden flights for a market
    :return: configurations (list)
    """
    try:
        if 'JUP_DB_Golden_Flights_Configuration' in db.collection_names():
            collection_name = gen()

            ppln_config = [
                {
                    '$project':
                        {
                            'compartment': '$compartment.value',
                            'destination': '$destination.value',
                            'origin': '$origin.value',
                            'compartment_level': '$compartment.level',
                            'destination_level': '$destination.level',
                            'origin_level': '$origin.level',
                            'last_update_date': '$last_updated_date',
                            'dow': '$dow.value',
                            'priority': '$priority',
                            'past_week_count': '$past_week_count',
                            'min_flights': {'$divide':['$threshold_min_flights', 100]},
                            'target_sf': '$target_seat_factor'
                        }
                }
                ,
                {
                    '$group':
                        {
                            '_id':
                                {
                                    'compartment': '$compartment',
                                    'origin': '$origin',
                                    'destination': '$destination',
                                    'last_update_date': '$last_update_date',
                                    'dow': '$dow',
                                    'origin_level': '$origin_level',
                                    'compartment_level': '$compartment_level',
                                    'destination_level': '$destination_level',
                                },
                            'config_details':
                                {
                                    '$push':
                                        {
                                            'priority': '$priority',
                                            'past_week_count': '$past_week_count',
                                            'min_flights': '$min_flights',
                                            'target_sf': '$target_sf'
                                        }
                                }
                        }
                }
                ,
                {
                    '$sort': {'_id.last_update_date': -1}
                }
                ,
                {
                    '$group':
                        {
                            '_id':
                                {
                                    'compartment': '$_id.compartment',
                                    'origin': '$_id.origin',
                                    'destination': '$_id.destination',
                                    'dow': '$_id.dow',
                                    'origin_level': '$_id.origin_level',
                                    'compartment_level': '$_id.compartment_level',
                                    'destination_level': '$_id.destination_level',
                                },
                            'priority': {'$first': '$config_details.priority'},
                            'past_week_count': {'$first': '$config_details.past_week_count'},
                            'min_flights': {'$first': '$config_details.min_flights'},
                            'target_sf': {'$first': '$config_details.target_sf'},
                        }
                }
                ,
                {
                    '$project':
                        {
                            '_id': 0,
                            'compartment': '$_id.compartment',
                            'origin': '$_id.origin',
                            'destination': '$_id.destination',
                            'dow': '$_id.dow',
                            'priority': {'$arrayElemAt': ['$priority', 0]},
                            'past_week_count': {'$arrayElemAt': ['$past_week_count', 0]},
                            'min_flights': {'$arrayElemAt': ['$min_flights', 0]},
                            'target_sf': {'$arrayElemAt': ['$target_sf', 0]},
                            'origin_level': '$_id.origin_level',
                            'compartment_level': '$_id.compartment_level',
                            'destination_level': '$_id.destination_level',
                        }
                }
                ,
                {
                    '$out': collection_name
                }
            ]
            db.JUP_DB_Golden_Flights_Configuration.aggregate(ppln_config, allowDiskUse=True)
            crsr_config = db.get_collection(collection_name).find(projection={'_id': 0})
            lst_config = list(crsr_config)
            # print '\nOD legs:',len(lst_od_legs),'records.'
            db[collection_name].drop()
            return lst_config
        else:
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                                'jupter_AI/batch/JUP_AI_Golden_Flights.py method: get_configurations',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Golden_Flights_Configuration not found in the database')
            obj_error.write_error_logs(datetime.datetime.now())
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/batch/JUP_AI_Golden_Flights.py method: get_configurations',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())



@measure(JUPITER_LOGGER)
def query_builder_past_sf(afilter):
    """
    Creates query to pick up all records for a particular configuration
    :param afilter: market filter to build for identifying golden flights
    :return: query for db (dict)
    """
    try:
        dict_filter = deepcopy(defaultdict(list, afilter))
        query = defaultdict(list)

        if dict_filter['origin']:
            lst_origin = []
            if dict_filter['origin_level'] == 'City':
                query['$and'].append({'leg_origin': dict_filter['origin']})
            elif dict_filter['origin_level'] == 'Country':
                # Get list of cities from Region master for that country
                cities = db.JUP_DB_Region_Master.distinct('POS_CD', {'COUNTRY_CD': dict_filter['origin']})
                for city in cities:
                    lst_origin.append({'leg_origin':city})
                query['$and'].append({'$or': lst_origin})
            elif dict_filter['origin_level'] == 'Region':
                # Get list of cities from Region master for that region
                cities = db.JUP_DB_Region_Master.distinct('POS_CD', {'Region': dict_filter['origin']})
                for city in cities:
                    lst_origin.append({'leg_origin': city})
                query['$and'].append({'$or': lst_origin})

        if dict_filter['destination']:
            lst_destination = []
            if dict_filter['destination_level'] == 'City':
                query['$and'].append({'leg_destination': dict_filter['destination']})
            elif dict_filter['destination_level'] == 'Country':
                cities = db.JUP_DB_Region_Master.distinct('POS_CD', {'COUNTRY_CD': dict_filter['destination']})
                for city in cities:
                    lst_destination.append({'leg_destination': city})
                query['$and'].append({'$or': lst_destination})
                # Get list of cities from Region master for that country
                pass
            elif dict_filter['destination_level'] == 'Region':
                # Get list of cities from Region master for that region
                cities = db.JUP_DB_Region_Master.distinct('POS_CD', {'Region': dict_filter['destination']})
                for city in cities:
                    lst_destination.append({'leg_destination': city})
                query['$and'].append({'$or': lst_destination})


        if dict_filter['compartment']:
            lst_comp = []
            for compartment in dict_filter['compartment']:
                lst_comp.append({'compartment':compartment})
            query['$and'].append({'$or': lst_comp})

        if dict_filter['past_week_count']:
            days_difference = datetime.timedelta(days=int(dict_filter['past_week_count'])*7)
            start_date = end_date - days_difference
            # print query
            query['$and'].append(
                {
                    'dep_date':
                        {
                            '$gte': start_date.strftime('%Y-%m-%d'),
                            '$lte': end_date.strftime('%Y-%m-%d')
                        }
                }
            )

        if dict_filter['flight_no']:
            lst_filghts = []
            for flight in dict_filter['flight_no']:
                lst_filghts.append({'Flight_num': flight})
            query['$and'].append({'$or': lst_filghts})

        return dict(query)
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/batch/JUP_AI_Golden_Flights.py method: query_builder_past_sf',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


@measure(JUPITER_LOGGER)
def get_flight_sf(dict_config):
    """
    Identifies which flights were performed well in the last 'n' weeks, as specified by dict_config
    :param dict_config: Configurations for a market to identify its golden flights
    :return: list of golden flights for that market
    """
    try:
        if 'JUP_DB_Flight_Leg' in db.collection_names():
            collection_name = gen()

            query = query_builder_past_sf(dict_config)
            print query
            ppln_sf = [
                {
                    '$match': query
                }
                ,
                # Finding day of week for a particular departure date
                {
                    '$project':
                        {
                            'dep_date': '$dep_date',
                            'origin': '$leg_origin',
                            'destination': '$leg_destination',
                            'flight_no': '$Flight_num',
                            'compartment': '$compartment',
                            'last_update_date': '$last_update_date',
                            'dow': {'$dayOfWeek': '$dep_date_ISO'},
                            'sf': '$Seat_Factor'
                        }
                }
                ,
                # Retrieving the most recently updated seat factor for the market
                {
                    '$group':
                        {
                            '_id':
                                {
                                    'dep_date': '$dep_date',
                                    'origin': '$origin',
                                    'destination': '$destination',
                                    'flight_no': '$flight_no',
                                    'compartment': '$compartment',
                                    'last_update_date': '$last_update_date',
                                    'dow': '$dow'
                                },
                            'seat_factor_records':
                                {
                                    '$push':
                                        {
                                            'sf': '$sf'
                                        }
                                }

                        }
                }
                ,
                {
                    '$sort': {'_id.last_update_date': -1}
                }
                ,
                {
                    '$group':
                        {
                            '_id':
                                {
                                    'dep_date': '$_id.dep_date',
                                    'origin': '$_id.origin',
                                    'destination': '$_id.destination',
                                    'flight_no': '$_id.flight_no',
                                    'compartment': '$_id.compartment',
                                    'dow': '$_id.dow'
                                },
                            'sf': {'$first': '$seat_factor_records.sf'}
                        }
                }
                ,
                {
                    '$project':
                        {
                            'dep_date': '$_id.dep_date',
                            'origin': '$_id.origin',
                            'destination': '$_id.destination',
                            'flight_no': '$_id.flight_no',
                            'compartment': '$_id.compartment',
                            'dow': '$_id.dow',
                            'sf':{'$arrayElemAt': ['$sf', 0]}
                        }
                }
                ,
                # Finding total times the flight has flown in the particular market
                {
                    '$group':
                        {
                            '_id':
                                {
                                    # 'dep_date': '$_id.dep_date',
                                    'origin': '$_id.origin',
                                    'destination': '$_id.destination',
                                    'flight_no': '$_id.flight_no',
                                    'compartment': '$_id.compartment',
                                    'dow': '$_id.dow'
                                },
                            'sf_details':
                                {
                                    '$push':
                                        {
                                            'dep_date': '$_id.dep_date',
                                            'sf': '$sf'
                                        }
                                },
                            'total_flights': {'$sum':1}
                        }
                }
                ,
                {
                    '$unwind': '$sf_details'
                }
                ,
                {
                    '$project':
                        {
                            '_id':0,
                            'origin': '$_id.origin',
                            'destination': '$_id.destination',
                            'flight_no': '$_id.flight_no',
                            'compartment': '$_id.compartment',
                            'dep_date': '$sf_details.dep_date',
                            'sf': '$sf_details.sf',
                            'total_flights': '$total_flights',
                            'dow': '$_id.dow'
                        }
                }
                ,
                # Selecting flights having required (or greater) seat factor for that market
                {
                    '$redact':
                        {
                            '$cond':
                                {
                                    'if': {'$gte': ['$sf', dict_config['target_sf']]},
                                    'then': '$$DESCEND',
                                    'else': '$$PRUNE'
                                }
                        }
                }
                ,
                # Counting flights having (or greater) target seat factor for that market
                {
                    '$group':
                        {
                            '_id':
                                {
                                    'origin': '$origin',
                                    'destination': '$destination',
                                    'flight_no': '$flight_no',
                                    'compartment': '$compartment',
                                    'total_flights': '$total_flights',
                                    'dow': '$dow'
                                },
                            'seat_factor_records':
                                {
                                    '$push':
                                        {
                                            'dep_date': '$dep_date',
                                            'sf':'$sf'
                                        }
                                },
                            'high_sf_flights': {'$sum':1}
                        }
                }
                ,
                {
                    '$unwind': '$seat_factor_records'
                }
                ,
                # Finding ratio of flights having high enough seat factor
                {
                    '$project':
                        {
                            '_id': 0,
                            'dep_date': '$seat_factor_records.dep_date',
                            'dow': '$_id.dow',
                            'origin': '$_id.origin',
                            'destination': '$_id.destination',
                            'flight_no': '$_id.flight_no',
                            'compartment': '$_id.compartment',
                            # 'total_flights': '$_id.total_flights',
                            'sf': '$seat_factor_records.sf',
                            # 'high_sf_flights':'$high_sf_flights',
                            'high_sf_flights': {'$divide':['$high_sf_flights', '$_id.total_flights']}
                        }
                }
                ,
                # Removing documents having a low seat factor
                {
                    '$redact':
                        {
                            '$cond':
                                {
                                    'if': {'$gte': ['$high_sf_flights_perc', dict_config['min_flights']]},
                                    'then': '$$DESCEND',
                                    'else': '$$PRUNE'
                                }
                        }
                }
                ,
                # Finding region and country of origin city
                {
                    '$lookup':
                        {
                            'from': 'JUP_DB_Region_Master',
                            'localField': 'origin',
                            'foreignField': 'POS_CD',
                            'as': 'region_master'
                        }
                }
                ,
                {
                    '$addFields':
                        {
                            'region':
                                {
                                    '$filter':
                                        {
                                            'input': '$region_master',
                                            'as': 'region_master',
                                            'cond': {'$eq': ['$$region_master.POS_CD', '$origin']}
                                        }
                                }
                        }
                }
                ,
                # Finding percentage of flights having high seat factor
                {
                    '$project':
                        {
                            '_id': 0,
                            'dep_date': '$dep_date',
                            'dow': '$dow',
                            'origin': '$origin',
                            'destination': '$destination',
                            'flight_no': '$flight_no',
                            'compartment': '$compartment',
                            'sf': '$sf',
                            'high_sf_flights_perc': {'$multiply': ['$high_sf_flights', 100]},
                            'region':{'$arrayElemAt': ['$region_master.Region', 0]},
                            'country': {'$arrayElemAt': ['$region_master.COUNTRY_CD', 0]}
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
                                    'flight_no': '$flight_no',
                                    'compartment': '$compartment',
                                    'high_sf_flights_perc': '$high_sf_flights_perc',
                                    'region': '$region.Region',
                                    'dow': '$dow',
                                    'country': '$country'
                                },
                            'seat_factor_records':
                                {
                                    '$push':
                                        {
                                            'dep_date':'$dep_date'
                                        }
                                }
                        }
                }
                ,
                {
                    '$project':
                        {
                            'region': '$_id.region',
                            'country': '$_id.country',
                            'sector_origin': '$_id.origin',
                            'sector_destination': '$_id.destination',
                            'flight_no': '$_id.flight_no',
                            'compartment': '$_id.compartment',
                            'high_sf_flights_perc': '$_id.high_sf_flights_perc',
                            'dep_date_from': {'$min': '$seat_factor_records.dep_date'},
                            'dep_date_to': {'$max': '$seat_factor_records.dep_date'},
                            'dow': '$_id.dow',
                            'user_system_flag': {'$literal': 0},
                            'last_update_date': datetime.datetime.now().strftime('%Y-%m-%d')
                            # 'sf': '$seat_factor_records.sf',
                            # 'dep_date': '$seat_factor_records.dep_date',
                        }
                }
                ,
                {
                    '$sort': {'dow': 1}
                }
                ,
                {
                    '$out': collection_name
                }
            ]
            db.JUP_DB_Flight_Leg.aggregate(ppln_sf, allowDiskUse=True)
            # crsr_sf = db.get_collection(collection_name).find(projection={'_id': 0})
            if dict_config['dow']:
                crsr_sf = db.get_collection(collection_name).find({'dow': dict_config['dow']}, projection={'_id': 0})
            else:
                crsr_sf = db.get_collection(collection_name).find(projection={'_id': 0})
            lst_sf = list(crsr_sf)
            db[collection_name].drop()

            return lst_sf
        else:
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                                'jupter_AI/batch/JUP_AI_Golden_Flights.py method: get_flight_sf',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Flight_Leg not found in the database')
            obj_error.write_error_logs(datetime.datetime.now())
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/batch/JUP_AI_Golden_Flights.py method: get_flight_sf',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


@measure(JUPITER_LOGGER)
def check_golden_flights():
    try:
        # Step 1: Get market-wise configurations
        lst_config = get_configurations()

        # Step 2: For each market, check seat factor against configurations
        for config in lst_config:
            print '\n',config
            if config['compartment'] == 'all':
                del config['compartment']
            if config['origin'] == 'Network':
                del config['origin']
            if config['destination'] == 'Network':
                del config['destination']

            lst_golden_flights = get_flight_sf(config)
            # for flight in lst_golden_flights:
            #     print flight

            # Step 3: If golden flights are found for that configuration then insert in DB
            if lst_golden_flights:
                db.JUP_DB_Golden_Flights.insert(lst_golden_flights)
                print len(lst_golden_flights),'records inserted.'
            else:
                print 'No golden flights for this configuration'
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/batch/JUP_AI_Golden_Flights.py method: check_golden_flights',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


if __name__ == '__main__':
    # # lst = get_configurations()
    # # for doc in lst:
    # #     print doc
    # config = \
    #     {
    #         'origin': 'DXB',
    #         'destination': 'DOH',
    #         'past_week_count': 35,
    #         'compartment': 'Y',
    #         'target_sf': 80,
    #         'dow': 6,
    #         'min_flights': 0.5
    #     }
    #
    # lst = get_flight_sf(config)
    #
    # for doc in lst:
    #     print doc
    check_golden_flights()