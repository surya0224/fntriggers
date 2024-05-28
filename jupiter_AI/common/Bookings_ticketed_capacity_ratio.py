import inspect
from collections import defaultdict
from copy import deepcopy
from datetime import datetime

import jupiter_AI.common.ClassErrorObject  as errorClass
from jupiter_AI.RnA.common_RnA_functions import enumerate_dates as ed
from jupiter_AI.RnA.common_RnA_functions import gen_collection_name as gen
from jupiter_AI.network_level_params import Host_Airline_Code as host
from jupiter_AI import client, JUPITER_DB, JUPITER_LOGGER
from jupiter_AI.logutils import measure
db = client[JUPITER_DB]
from jupiter_AI.tiles.jup_common_functions_tiles import get_od_list_leg_level as get_od

results_collection = gen()

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
def capacity(afilter):

    afilter = deepcopy(defaultdict(list, afilter))
    query = dict()
    from_date = str(afilter['fromDate'])
    to_date = str(afilter['toDate'])
    print from_date, to_date
    print query
    if afilter['region']:
        query['region'] = {'$in': afilter['region']}
    if afilter['country']:
        query['country'] = {'$in': afilter['country']}
    if afilter['pos']:
        query['pos'] = {'$in': afilter['pos']}
    if afilter['compartment']:
        query['compartment'] = {'$in': afilter['compartment']}
    if afilter['origin'] and afilter['destination']:
        query['or'] = get_od(afilter)
    query['dep_date'] = {'$gte': afilter['fromDate'], '$lte': afilter['toDate']}

    date_list = ed(from_date,to_date)
    # date_list = pd.date_range(datetime.datetime.strptime(from_date, "%Y-%m-%d"),
    #                                       datetime.datetime.strptime(to_date, "%Y-%m-%d")).tolist()
    date_list_new =[]
    print date_list
    print type(date_list)
    if len(date_list):
        for i in date_list:
            date_list_new.append({'date': i.strftime('%Y-%m-%d'), 'day_of_week': str(i.isoweekday())})
    else:
        pass
    print query
    pipeline_capacity = [
        # the following pipeline matches the documents from the collection with the filter values
        {
            '$match': query
        }
        ,
        # the following pipeline groups documents based on od and compartment after which the total number of bookings
        # and ticketed are calculated for each od and compartment combo
        {
            '$group':
                {
                    '_id': {'od': '$od', 'compartment_bookings': '$compartment'},
                    'bookings': {"$sum": "$pax"},
                    'ticketed': {"$sum": "$ticket"}
                }
        }
        ,
        # projecting the above pipeline and renaming the fields
        {
            "$project":
                {
                    '_id': 0,
                    'od_bookings': '$_id.od',
                    'compartment_bookings': '$_id.compartment_bookings',
                    "bookings": '$bookings',
                    "ticketed": '$ticketed'
                }
        }
        # a lookup is done in the following pipeline. lookup will match keys between two collections (local and foreign)
        # and to each input document, the $lookup stage adds a new array field whose elements are the matching documents
        # from the joined collection
        ,
        {
            '$lookup':
                {
                    'from': 'JUP_DB_Schedule_Capacity',
                    'localField': 'od_bookings',
                    'foreignField': 'od',
                    'as': 'capacity_collection'
                }
        }
        ,
        # The following code deconstructs capacity collection array field from the input
        # documents to output a document for each element of the array.
        {
            '$unwind': '$capacity_collection'
        }
        ,
        # The following code projects the above pipeline with the required fields for
        # further aggregation and also renames the fields for convenience
        {
            '$project':
                {
                    '_id': 0,
                    'od': '$od_bookings',
                    'bookings': '$bookings',
                    'ticketed': '$ticketed',
                    'effective_from': '$capacity_collection.effective_from',
                    'effective_to': '$capacity_collection.effective_to',
                    'frequency': '$capacity_collection.frequency',
                    'capacity': '$capacity_collection.capacity',
                    'compartment': '$compartment_bookings',
                    'compartment_sc': '$capacity_collection.compartment' ,
                    'airline': '$capacity_collection.airline'
                }
        }
        ,
        # the redact functionality is used to redact the output of the previous pipeline stage based on a given condition.
        # it prunes the documents that dont meet the condition and passes the documents that meet the condition to the next stage.
        # the following codes prune the documents in which the airline code is not equal to the host airline code
        {
            '$redact':
                {
                    '$cond':
                        {
                            'if':
                                {
                                    '$eq': [host, '$airline']
                                },
                            'then': '$$DESCEND',
                            'else': '$$PRUNE'
                        }
                }
        }
        ,
        #  The following codes projects the above pipeline stage fir further aggregation. additionally, the documents from the above
        # pipeline contains two fields with compartment values. one is from the local collection and the other one is from
        # the foreign collection
        {
            '$project':
                {
                    '_id': 0,
                    'od': '$od',
                    'bookings': '$bookings',
                    'ticketed': '$ticketed',
                    'effective_from': '$effective_from',
                    'effective_to': '$effective_to',
                    'frequency': '$frequency',
                    'capacity': '$capacity',
                    'compartment': '$compartment',
                    'compartment_sc': '$compartment_sc'
                }
        }
        ,
        # The following code redacts the documents where the two compartment values dont match. this is because we only need
        # documents where the compartment values are equal for further aggregation and to identify the right capacity
        # for a given compartment
        {
            '$redact':
                {
                    '$cond':
                        {
                            'if': {'$eq': ['$compartment', '$compartment_sc']},
                            'then': '$$DESCEND',
                            'else': '$$PRUNE'
                        }
                }
        }
        ,
        # the following code redacts documents that don't fall within the date range specified in the filter. this is done
        # by checking if the effective from date for a schedule to be lesser than equal to than the to date specified in
        # the filter and if the effective to date for a schedule is greater than equal to from date specified in the filter
        {
            '$redact':
                {
                    '$cond':
                        {
                            'if':
                                {'$and':
                                     [{'$lte': ['$effective_from', to_date]},
                                      {'$gte': ['$effective_to', from_date]}]},
                            'then': '$$DESCEND',
                            'else': '$$PRUNE'
                        }
                }
        }
        ,
        # the following code projects the above pipeline with the required fields for further aggregation
        # and renames the fields for convenience
        {
            '$project':
                {
                    '_id': 0,
                    'od': '$od',
                    'bookings': '$bookings',
                    'ticketed': '$ticketed',
                    'effective_from': '$effective_from',
                    'effective_to': '$effective_to',
                    'capacity': '$capacity',
                    'compartment': '$compartment',
                    'frequency': '$frequency'
                }
        }
        ,
        # the following code changes the effective from and effective to date values that fall between the date values in the filter
        # for example if the effective from is June 1 2016 and effective to is August 31 2016 and if the filter from is July 1 2016
        # and the filter to is July 31 2016. The following pipeline changes the effective from and effective to the corresponding filter
        # from and to values
        {
            '$project':
                {
                    '_id': 0,
                    'od': '$od',
                    'bookings': '$bookings',
                    'ticketed': '$ticketed',
                    'effective_from': '$effective_from',
                    'effective_to': '$effective_to',
                    'new_effective_from':
                        {
                            '$cond':
                                {
                                    'if': {'$lte': ['$effective_from', from_date]},
                                    'then': from_date,
                                    'else': '$effective_from'
                                }
                        },
                    'new_effective_to':
                        {
                            '$cond':
                                {
                                    'if': {'$gt': ['$effective_to', to_date]},
                                    'then': to_date,
                                    'else': '$effective_to'
                                }
                        },
                    'capacity': '$capacity',
                    'compartment': '$compartment',
                    'frequency': '$frequency'
                }
        }
        ,
        # the following code adds a list of dictionary that contains 2 key value pairs. the values of the first key are the
        # dates between the filter from and to dates. The values of the second key are the corresponding iso week of the day
        # values. the values are both converted to string. This list of dictionary is created outside the pipeline.
        {
            '$project':
                {
                    '_id': 0,
                    'od': '$od',
                    'bookings': '$bookings',
                    'ticketed': '$ticketed',
                    'effective_from': '$new_effective_from',
                    'effective_to': '$new_effective_to',
                    'capacity': '$capacity',
                    'compartment': '$compartment',
                    'frequency': '$frequency',
                    'date_list': date_list_new
                }
        }
        ,
        # The list of dictionary 'date_list' is being unwound to create a document for each element of the list
        {
            '$unwind': '$date_list'
        }
        ,
        # The following code keeps all date_list.date values that are lesser than equal to the effective to and
        # greater than equal to effective from. The other documents that fall out of range are of no importance
        # to calculate the total capacity
        {
            '$redact':
                {
                    '$cond':
                        {
                            'if':
                                {'$and':
                                     [{'$lte': ['$date_list.date', '$effective_to']},
                                      {'$gte': ['$date_list.date', '$effective_from']}]},
                            'then': '$$DESCEND',
                            'else': '$$PRUNE'
                        }
                }
        }
        ,
        # the following pipeline stage projects the required fields for the next pipeline stage and also the identifies the
        # the length of the field 'frequency' that is required for the next pipeline.
        {
            '$project':
                {
                    '_id': 0,
                    'od': '$od',
                    'bookings': '$bookings',
                    'ticketed': '$ticketed',
                    'effective_from': '$new_effective_from',
                    'effective_to': '$new_effective_to',
                    'capacity': '$capacity',
                    'compartment': '$compartment',
                    'frequency': '$frequency',
                    'day': '$date_list.date',
                    'day_of_week':'$date_list.day_of_week',
                    # 'day_of_week_test':{'$isoDayOfWeek': '$date_list'},
                    # 'day_of_week': {'$substr': [{'$isoDayOfWeek':'$date_list'}, 0, 1]},
                    'frequency_length': {'$strLenBytes': '$frequency'}
                }
        }
        ,
        # the following pipeline checks if the values in day of the week field created in the above pipeline is present
        # in the string in the frequency field. as mongodb does not have a search string function. the length of the string and
        # the substring functionality of mongodb was used to identify the presence of day of week letter in the frequency string
        # the assumption that is made over here is that the length of the frequency is string is never going to be more than 7
        # since there are only seven days in a week. if the day of week is present in frequency then value of day in freq is 1 else 0
        {
            '$project':
                {
                    'od': '$od',
                    'bookings': '$bookings',
                    'ticketed': '$ticketed',
                    'effective_from': '$new_effective_from',
                    'effective_to': '$new_effective_to',
                    'capacity': '$capacity',
                    'compartment': '$compartment',
                    'frequency': '$frequency',
                    'day_in_freq':
                        {
                            '$cond':
                                {
                                    'if': {'$eq': ['$frequency_length', 1]},
                                    'then':{
                                        '$cond':
                                            {
                                                'if':
                                                    {
                                                        '$eq':
                                                            [
                                                                {'$substr':['$frequency',
                                                                            0,
                                                                            1]},
                                                                '$day_of_week'
                                                            ]
                                                    },
                                                'then': 1,
                                                'else': 0

                                            }
                                    },
                                    'else':
                                        {
                                            '$cond':
                                                {
                                                    'if': {'$eq': ['$frequency_length',
                                                                   2]},
                                                    'then':
                                                        {
                                                            '$cond':
                                                                {
                                                                    'if':
                                                                        {
                                                                            '$eq':
                                                                                [
                                                                                    {'$substr': ['$frequency',
                                                                                                 0,
                                                                                                 1]},
                                                                                    '$day_of_week'
                                                                                ]
                                                                        },
                                                                    'then': 1,
                                                                    'else':
                                                                        {
                                                                            '$cond':
                                                                                {
                                                                                    'if':
                                                                                        {
                                                                                            '$eq':
                                                                                                [
                                                                                                    {'$substr': [
                                                                                                        '$frequency',
                                                                                                        1,
                                                                                                        1]},
                                                                                                    '$day_of_week'
                                                                                                ]
                                                                                        },
                                                                                    'then': 1,
                                                                                    'else': 0
                                                                                }
                                                                        }
                                                                }
                                                        },
                                                    'else':
                                                        {
                                                            '$cond':
                                                                {
                                                                    'if':
                                                                        {
                                                                            '$eq': ['$frequency_length',
                                                                                    3]
                                                                        },
                                                                    'then':
                                                                        {
                                                                            '$cond':
                                                                                {
                                                                                    'if':
                                                                                        {
                                                                                            '$eq':
                                                                                                [
                                                                                                    {'$substr': ['$frequency',
                                                                                                                 0,
                                                                                                                 1]},
                                                                                                    '$day_of_week'
                                                                                                ]
                                                                                        },
                                                                                    'then': 1,
                                                                                    'else':
                                                                                        {
                                                                                            '$cond':
                                                                                                {
                                                                                                    'if':
                                                                                                        {
                                                                                                            '$eq':
                                                                                                                [
                                                                                                                    {'$substr': [
                                                                                                                        '$frequency',
                                                                                                                        1,
                                                                                                                        1]},
                                                                                                                    '$day_of_week'
                                                                                                                ]
                                                                                                        },
                                                                                                    'then': 1,
                                                                                                    'else':
                                                                                                        {
                                                                                                            '$cond':
                                                                                                                {
                                                                                                                    'if':{'$substr': [
                                                                                                                        '$frequency',
                                                                                                                        2,
                                                                                                                        1]},
                                                                                                                    'then':1,
                                                                                                                    'else': 0
                                                                                                                }
                                                                                                        }
                                                                                                }
                                                                                        }
                                                                                }

                                                                        },
                                                                    'else':
                                                                        {

                                                                            '$cond':
                                                                                {
                                                                                    'if':
                                                                                        {
                                                                                            '$eq': ['$frequency_length',
                                                                                                    4]
                                                                                        },
                                                                                    'then':
                                                                                        {
                                                                                            '$cond':
                                                                                                {
                                                                                                    'if':
                                                                                                        {
                                                                                                            '$eq':
                                                                                                                [
                                                                                                                    {'$substr': ['$frequency',
                                                                                                                                 0,
                                                                                                                                 1]},
                                                                                                                    '$day_of_week'
                                                                                                                ]
                                                                                                        },
                                                                                                    'then': 1,
                                                                                                    'else':
                                                                                                        {
                                                                                                            '$cond':
                                                                                                                {
                                                                                                                    'if':
                                                                                                                        {
                                                                                                                            '$eq':
                                                                                                                                [
                                                                                                                                    {'$substr': [
                                                                                                                                        '$frequency',
                                                                                                                                        1,
                                                                                                                                        1]},
                                                                                                                                    '$day_of_week'
                                                                                                                                ]
                                                                                                                        },
                                                                                                                    'then': 1,
                                                                                                                    'else':
                                                                                                                        {
                                                                                                                            '$cond':
                                                                                                                                {
                                                                                                                                    'if': {'$substr': [
                                                                                                                                        '$frequency',
                                                                                                                                        2,
                                                                                                                                        1]},
                                                                                                                                    'then': 1,
                                                                                                                                    'else':
                                                                                                                                        {

                                                                                                                                            '$cond':
                                                                                                                                                {
                                                                                                                                                    'if': {'$substr': [
                                                                                                                                                        '$frequency',
                                                                                                                                                        3,
                                                                                                                                                        1]},
                                                                                                                                                    'then': 1,
                                                                                                                                                    'else': 0
                                                                                                                                                }

                                                                                                                                        }
                                                                                                                                }
                                                                                                                        }
                                                                                                                }
                                                                                                        }
                                                                                                }

                                                                                        },
                                                                                    'else':
                                                                                        {

                                                                                            '$cond':
                                                                                                {
                                                                                                    'if':
                                                                                                        {
                                                                                                            '$eq': ['$frequency_length',
                                                                                                                    5]
                                                                                                        },
                                                                                                    'then':
                                                                                                        {
                                                                                                            '$cond':
                                                                                                                {
                                                                                                                    'if':
                                                                                                                        {
                                                                                                                            '$eq':
                                                                                                                                [
                                                                                                                                    {'$substr': ['$frequency',
                                                                                                                                                 0,
                                                                                                                                                 1]},
                                                                                                                                    '$day_of_week'
                                                                                                                                ]
                                                                                                                        },
                                                                                                                    'then': 1,
                                                                                                                    'else':
                                                                                                                        {
                                                                                                                            '$cond':
                                                                                                                                {
                                                                                                                                    'if':
                                                                                                                                        {
                                                                                                                                            '$eq':
                                                                                                                                                [
                                                                                                                                                    {'$substr': [
                                                                                                                                                        '$frequency',
                                                                                                                                                        1,
                                                                                                                                                        1]},
                                                                                                                                                    '$day_of_week'
                                                                                                                                                ]
                                                                                                                                        },
                                                                                                                                    'then': 1,
                                                                                                                                    'else':
                                                                                                                                        {
                                                                                                                                            '$cond':
                                                                                                                                                {
                                                                                                                                                    'if': {'$substr': [
                                                                                                                                                        '$frequency',
                                                                                                                                                        2,
                                                                                                                                                        1]},
                                                                                                                                                    'then': 1,
                                                                                                                                                    'else':
                                                                                                                                                        {

                                                                                                                                                            '$cond':
                                                                                                                                                                {
                                                                                                                                                                    'if': {'$substr': [
                                                                                                                                                                        '$frequency',
                                                                                                                                                                        3,
                                                                                                                                                                        1]},
                                                                                                                                                                    'then': 1,
                                                                                                                                                                    'else':
                                                                                                                                                                        {

                                                                                                                                                                            '$cond':
                                                                                                                                                                                {
                                                                                                                                                                                    'if': {'$substr': [
                                                                                                                                                                                        '$frequency',
                                                                                                                                                                                        4,
                                                                                                                                                                                        1]},
                                                                                                                                                                                    'then': 1,
                                                                                                                                                                                    'else': 0
                                                                                                                                                                                }

                                                                                                                                                                        }

                                                                                                                                                                }
                                                                                                                                                        }
                                                                                                                                                }
                                                                                                                                        }
                                                                                                                                }
                                                                                                                        }
                                                                                                                }
                                                                                                        },
                                                                                                    'else':
                                                                                                        {
                                                                                                            '$cond':
                                                                                                                {
                                                                                                                    'if':
                                                                                                                        {
                                                                                                                            '$eq': ['$frequency_length',
                                                                                                                                    6]
                                                                                                                        },
                                                                                                                    'then':
                                                                                                                        {
                                                                                                                            '$cond':
                                                                                                                                {
                                                                                                                                    'if':
                                                                                                                                        {
                                                                                                                                            '$eq':
                                                                                                                                                [
                                                                                                                                                    {'$substr': ['$frequency',
                                                                                                                                                                 0,
                                                                                                                                                                 1]},
                                                                                                                                                    '$day_of_week'
                                                                                                                                                ]
                                                                                                                                        },
                                                                                                                                    'then': 1,
                                                                                                                                    'else':
                                                                                                                                        {
                                                                                                                                            '$cond':
                                                                                                                                                {
                                                                                                                                                    'if':
                                                                                                                                                        {
                                                                                                                                                            '$eq':
                                                                                                                                                                [
                                                                                                                                                                    {'$substr': [
                                                                                                                                                                        '$frequency',
                                                                                                                                                                        1,
                                                                                                                                                                        1]},
                                                                                                                                                                    '$day_of_week'
                                                                                                                                                                ]
                                                                                                                                                        },
                                                                                                                                                    'then': 1,
                                                                                                                                                    'else':
                                                                                                                                                        {
                                                                                                                                                            '$cond':
                                                                                                                                                                {
                                                                                                                                                                    'if': {'$substr': [
                                                                                                                                                                        '$frequency',
                                                                                                                                                                        2,
                                                                                                                                                                        1]},
                                                                                                                                                                    'then': 1,
                                                                                                                                                                    'else':
                                                                                                                                                                        {

                                                                                                                                                                            '$cond':
                                                                                                                                                                                {
                                                                                                                                                                                    'if': {'$substr': [
                                                                                                                                                                                        '$frequency',
                                                                                                                                                                                        3,
                                                                                                                                                                                        1]},
                                                                                                                                                                                    'then': 1,
                                                                                                                                                                                    'else':
                                                                                                                                                                                        {

                                                                                                                                                                                            '$cond':
                                                                                                                                                                                                {
                                                                                                                                                                                                    'if': {'$substr': [
                                                                                                                                                                                                        '$frequency',
                                                                                                                                                                                                        4,
                                                                                                                                                                                                        1]},
                                                                                                                                                                                                    'then': 1,
                                                                                                                                                                                                    'else':
                                                                                                                                                                                                        {

                                                                                                                                                                                                            '$cond':
                                                                                                                                                                                                                {
                                                                                                                                                                                                                    'if': {'$substr': [
                                                                                                                                                                                                                        '$frequency',
                                                                                                                                                                                                                        5,
                                                                                                                                                                                                                        1]},
                                                                                                                                                                                                                    'then': 1,
                                                                                                                                                                                                                    'else': 0
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
                                                                                                                        },
                                                                                                                    'else':
                                                                                                                        {

                                                                                                                            '$cond':
                                                                                                                                {
                                                                                                                                    'if':
                                                                                                                                        {
                                                                                                                                            '$eq': ['$frequency_length',
                                                                                                                                                    7]
                                                                                                                                        },
                                                                                                                                    'then':
                                                                                                                                        {
                                                                                                                                            '$cond':
                                                                                                                                                {
                                                                                                                                                    'if':
                                                                                                                                                        {
                                                                                                                                                            '$eq':
                                                                                                                                                                [
                                                                                                                                                                    {'$substr': ['$frequency',
                                                                                                                                                                                 0,
                                                                                                                                                                                 1]},
                                                                                                                                                                    '$day_of_week'
                                                                                                                                                                ]
                                                                                                                                                        },
                                                                                                                                                    'then': 1,
                                                                                                                                                    'else':
                                                                                                                                                        {
                                                                                                                                                            '$cond':
                                                                                                                                                                {
                                                                                                                                                                    'if':
                                                                                                                                                                        {
                                                                                                                                                                            '$eq':
                                                                                                                                                                                [
                                                                                                                                                                                    {'$substr': [
                                                                                                                                                                                        '$frequency',
                                                                                                                                                                                        1,
                                                                                                                                                                                        1]},
                                                                                                                                                                                    '$day_of_week'
                                                                                                                                                                                ]
                                                                                                                                                                        },
                                                                                                                                                                    'then': 1,
                                                                                                                                                                    'else':
                                                                                                                                                                        {
                                                                                                                                                                            '$cond':
                                                                                                                                                                                {
                                                                                                                                                                                    'if': {'$substr': [
                                                                                                                                                                                        '$frequency',
                                                                                                                                                                                        2,
                                                                                                                                                                                        1]},
                                                                                                                                                                                    'then': 1,
                                                                                                                                                                                    'else':
                                                                                                                                                                                        {

                                                                                                                                                                                            '$cond':
                                                                                                                                                                                                {
                                                                                                                                                                                                    'if': {'$substr': [
                                                                                                                                                                                                        '$frequency',
                                                                                                                                                                                                        3,
                                                                                                                                                                                                        1]},
                                                                                                                                                                                                    'then': 1,
                                                                                                                                                                                                    'else':
                                                                                                                                                                                                        {

                                                                                                                                                                                                            '$cond':
                                                                                                                                                                                                                {
                                                                                                                                                                                                                    'if': {'$substr': [
                                                                                                                                                                                                                        '$frequency',
                                                                                                                                                                                                                        4,
                                                                                                                                                                                                                        1]},
                                                                                                                                                                                                                    'then': 1,
                                                                                                                                                                                                                    'else':
                                                                                                                                                                                                                        {

                                                                                                                                                                                                                            '$cond':
                                                                                                                                                                                                                                {
                                                                                                                                                                                                                                    'if': {'$substr': [
                                                                                                                                                                                                                                        '$frequency',
                                                                                                                                                                                                                                        5,
                                                                                                                                                                                                                                        1]},
                                                                                                                                                                                                                                    'then': 1,
                                                                                                                                                                                                                                    'else':
                                                                                                                                                                                                                                        {

                                                                                                                                                                                                                                            '$cond':
                                                                                                                                                                                                                                                {
                                                                                                                                                                                                                                                    'if': {'$substr': [
                                                                                                                                                                                                                                                        '$frequency',
                                                                                                                                                                                                                                                        6,
                                                                                                                                                                                                                                                        1]},
                                                                                                                                                                                                                                                    'then': 1,
                                                                                                                                                                                                                                                    'else': 0
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
                                                                                                                                        },
                                                                                                                                    'else': 0
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
                }
        }
        ,
        # the following code redacts documents based on the value of day_in_freq. the ones with values of 1 are kept and
        # the others are pruned because a value of 1 denotes that a day of week was present in the frequency string in
        # the previous stage
        {
            '$redact':
                {
                    '$cond':
                        {
                            'if':
                                {
                                    '$eq': ['$day_in_freq', 1]
                                },
                            'then': '$$DESCEND',
                            'else': '$$PRUNE'
                        }
                }
        }
        ,
        # The documents are grouped on the basis of od compartment bookings and ticketed. bookings and ticketed are
        # are also group ids because they were grouped in a previous stage and that a unique combination of od and
        # compartment will have a unique values of bookings and ticketed. based on this unique combination, capacity
        # is summed up.
        {
            '$group':
                {
                    '_id':
                        {
                            'od': '$od',
                            'compartment': '$compartment',
                            'bookings': '$bookings',
                            'ticketed': '$ticketed'
                        },
                    'capacity': {'$sum': '$capacity'}

                }
        }
        ,
        # the following stage identifies the ratio between ticketed and capacity as this ratio
        # is serves as flag for RnA. the ratio is named as ticketed capacity
        {
           '$project':
               {
                   'od': '$_id.od',
                   'bookings':'$_id.bookings',
                   'compartment': '$_id.compartment',
                   'ticketed': '$_id.ticketed',
                   'capacity':'$capacity',
                   'ticketed_capacity':
                       {
                           '$cond':
                               {
                                   'if':
                                       {
                                           '$gt': ['$capacity', 0]
                                       },
                                   'then': {'$divide':['$_id.ticketed','$capacity']},
                                   'else': 'no capacity data'
                               }
                       }
               }
        }
        ,
        # the following stage identifies whether an action is required or not based on the ticketed capacity ratio
        {
            '$project':
                {
                    '_id': 0,
                    'od':'$od',
                    'bookings':'$bookings',
                    'compartment': '$compartment',
                    'ticketed':'$ticketed',
                    'capacity': '$capacity',
                    'ticketed_capacity': '$ticketed_capacity',
                    'ticketed_capacity_action':
                        {
                            '$cond':
                                {
                                    'if':
                                        {
                                            '$gte': ['$ticketed_capacity', 0.9]
                                        },
                                    'then': 1,
                                    'else': 0
                                }
                        }
                }
        }
        # The following stage outputs the results of this aggregation pipeline to a collection named
        # after the string in the results_collection variable. this is done because pipeline stages
        # have a RAM limit of 100 MB and if a stage exceeds this the aggregation fails.
        ,
        {
            '$out': results_collection
        }

    ]
    # the following variable cursor user is iused to run the aggregation pipeline on mongodb
    cursor_user = db.JUP_DB_Booking_DepDate.aggregate(pipeline_capacity, allowDiskUse=True)
    # the following code stores the collection details to a variable called dummy
    dummy = db.get_collection(results_collection)
    # the following code gets all the documents from the collection
    cursor_user = dummy.find()
    # tfc (the following code) checks if there any documents are present in the collection
    if dummy.find().count() !=0:
        # the following for loop prints all the documents
        for i in cursor_user:
            print i
    else:
        # enter error handling
        no_data_error = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                                       get_module_name(),
                                                       get_arg_lists(inspect.currentframe()))
        no_data_error.append_to_error_list('Null Response from Database')
        raise no_data_error

    dummy.drop()

    return 0



if __name__ == '__main__':
    start_time = datetime.now()
    a = {
        'region': [],
        'country': [],
        'pos': [],
        'origin': ['DXB'],
        'destination': ['DOH'],
        'compartment': [],
        'fromDate': '2016-08-01',
        'toDate': '2016-09-30'
    }

    print capacity(a)
    print datetime.now() - start_time




