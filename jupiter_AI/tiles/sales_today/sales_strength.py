"""
File Name              :   sales_strength.py
Author                 :   Pavan
Date Created           :   2016-12-19
Description            :   module with methods to calculate sales strength for jupiter dashboard (sales today)
MODIFICATIONS LOG      :
    S.No               :
    Date Modified      :
    By                 :
    Modification Details   :
"""
import inspect
import random
import string
from collections import defaultdict
from copy import deepcopy
from jupiter_AI import client, JUPITER_DB
db = client[JUPITER_DB]
from jupiter_AI.network_level_params import Host_Airline_Code as host, Host_Airline_Hub



def get_module_name():
    '''
    FUnction used to get the module name where it is called
    '''
    return inspect.stack()[1][3]


def get_arg_lists(frame):
    '''
    function used to get the list of arguments of the function
    where it is called
    '''
    args, _, _, values = inspect.getargvalues(frame)
    argument_name_list = []
    argument_value_list = []
    for k in args:
        argument_name_list.append(k)
        argument_value_list.append(values[k])
    return argument_name_list, argument_value_list

import datetime
def gen_collection_name():
    time_stamp = datetime.datetime.now().isoformat()
    random_string = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))

    collection_name = 'JUP_Temp_' + time_stamp +'_'+ random_string
    return collection_name

def previous_year(adate):
    date_format = "%Y-%m-%d"
    dt = datetime.datetime.strptime(adate, date_format)
    year = dt.year
    if year%4 == 0:
    	date = dt - datetime.timedelta(days=366)
    else:
    	date = dt - datetime.timedelta(days=365)
    return datetime.datetime.strftime(date, date_format)


def get_sales_strength(afilter):
    data = deepcopy(defaultdict(list, afilter))
    query = dict()
    results_collection = gen_collection_name()
    if data['region']:
        query['region'] = {'$in': data['region']}
    if data['country']:
        query['country'] = {'$in': data['country']}
    if data['pos']:
        query['pos'] = {'$in': data['pos']}
    if data['compartment']:
        query['compartment'] = {'$in': data['compartment']}
    if data['origin']:
        od_build = []
        for idx, item in enumerate(data['origin']):
            od_build.append({'origin': item, 'destination': data['destination'][idx]})
            query['$or'] = od_build
    if (data['origin'] != [] and data['destination'] != []):
        query['dep_date'] = {'$gte': data['fromDate'], '$lte': data['toDate']}
    #print query
    sales_strength_pipeline = [
            {
                '$match': query

            }
            ,
            {'$group':
                {
                    '_id': {'od': '$od', 'compartment': '$compartment'},
                    'current_revenue': {"$sum": "$revenue"},
                    'last_revenue': {"$sum": "$revenue_1"}
                }
            }
            ,
            {"$project":
                {
                    "_id": 0,
                    'od': '$_id.od',
                    'compartment': '$_id.compartment',
                    "current_revenue": '$current_revenue',
                    "last_revenue": '$last_revenue'
                }
            }
            ,
            {
                '$lookup':
                    {
                        'from': 'JUP_DB_Schedule_Capacity',
                        'localField': 'od',
                        'foreignField': 'od',
                        'as': 'capacity_collection'
                    }
            }
            ,
            {
                '$unwind': '$capacity_collection'
            }
            ,
            {
                '$project':
                    {
                        '_id': 0,
                        'current_revenue':'$current_revenue',
                        'last_revenue':'$last_revenue',
                        'od':'$od',
                        'compartment':'$compartment',
                        'current_capacity':'$capacity_collection.capacity',
                        'frequency':'$capacity_collection.frequency',
                        'compartment_cc': '$capacity_collection.compartment',
                        'airline': '$capacity_collection.airline',
                        'effective_from': '$capacity_collection.effective_from',
                        'effective_to': '$capacity_collection.effective_to'
                    }
            }
            ,
            {
                '$redact':
                    {
                        '$cond':
                            {
                                'if': {'$eq': [host, '$airline']},
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
                        'current_revenue':'$current_revenue',
                        'last_revenue':'$last_revenue',
                        'od':'$od',
                        'compartment':'$compartment',
                        'capacity':'$current_capacity',
                        'frequency':'$frequency',
                        'compartment_cc': '$compartment_cc',
                        'airline': '$airline',
                        'effective_from': '$effective_from',
                        'effective_to': '$effective_to'
                    }
            }
            ,
            {
                '$redact':
                    {
                        '$cond':
                            {
                                'if': {'$eq': ['$compartment', '$compartment_cc']},
                                'then': '$$DESCEND',
                                'else': '$$PRUNE'
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
                                    {'$and':
                                         [{'$lte': ['$effective_from', data['toDate']]},
                                          {'$gte': ['effective_to', data['fromDate']]}]},
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
                        'od': '$od',
                        'current_revenue': '$current_revenue',
                        'last_revenue': '$last_revenue',
                        'effective_from': '$effective_from',
                        'effective_to': '$effective_to',
                        'capacity': '$capacity',
                        'compartment': '$compartment',
                        'frequency': '$frequency'
                    }
            }
            ,
            {
                '$project':
                    {
                        '_id': 0,
                        'od': '$od',
                        'current_revenue': '$current_revenue',
                        'last_revenue': '$last_revenue',
                        'effective_from': '$effective_from',
                        'effective_to': '$effective_to',
                        'new_effective_from':
                            {
                                '$cond':
                                    {
                                        'if': {'$lt': ['$effective_from', data['fromDate']]},
                                        'then': data['fromDate'],
                                        'else': '$effective_from'
                                    }
                            },
                        'new_effective_to':
                            {
                                '$cond':
                                    {
                                        'if': {'$gt': ['$effective_to', data['toDate']]},
                                        'then': data['toDate'],
                                        'else': '$effective_to'
                                    }
                            },
                        'capacity': '$capacity',
                        'compartment': '$compartment',
                        'frequency': '$frequency'
                    }
            }
            ,
            {
                '$project':
                    {
                        '_id': 0,
                        'od': '$od',
                        'current_revenue': '$current_revenue',
                        'last_revenue': '$last_revenue',
                        'effective_from': '$new_effective_from',
                        'effective_to': '$new_effective_to',
                        'capacity': '$capacity',
                        'compartment': '$compartment',
                        'frequency': '$frequency'
                    }
            }
            ,
            {
                '$group':
                    {
                        '_id':
                            {
                                'od': '$od',
                                'compartment': '$compartment',
                                'current_revenue': '$current_revenue',
                                'last_revenue': '$last_revenue',
                            },
                        'capacity_details':
                            {
                                '$push':
                                    {
                                        'effective_from': '$effective_from',
                                        'effective_to': '$effective_to',
                                        'capacity': '$capacity',
                                        'frequency': '$frequency'
                                    }
                            }

                    }
            }
            ,
            {

                '$project':
                    {
                        'od': '$_id.od',
                        'compartment': '$_id.compartment',
                        'current_revenue': '$_id.current_revenue',
                        'last_revenue': '$_id.last_revenue',
                        'capacity_details': '$capacity_details'
                    }
            }
            ,
            {
                '$out': results_collection
            }

        ]
    perform_aggregate = db.JUP_DB_Sales.aggregate(sales_strength_pipeline, allowDiskUse=True)
    get_collection = db.get_collection(results_collection)
    strength_data = get_collection.find()
    strength_data_count = get_collection.find().count()
    strength_data_list = list(strength_data)
    current_revenue = 0
    last_revenue = 0
    capacity_this = 0
    capacity_last = 16200
    #print strength_data_list
    get_collection.drop()
    if len(strength_data_list) != 0:
        for i in strength_data_list:
            current_revenue += i['current_revenue']
            last_revenue += i['last_revenue']
            #capacity_this += i['capacity_details']['capacity']
        #print type(capacity_this)
            #capacity_last += i['capacity_1']
        #strength = float((((current_revenue) - (last_revenue)) / (last_revenue)) * ((capacity_last) / (capacity_this)))
        #return strength
        return 0

    # else:
         # e1 = error_class.ErrorObject(error_class.ErrorObject.ERROR,
         #                              self.get_module_name(),
         #                              self.get_arg_lists(inspect.currentframe()))
         # e1.append_to_error_list("Expected 1 document for sales but got " + str(len(data)))
         # raise e1
         #return NA

import time

st = time.time()
filter = {
         'region': [],
         'country': [],
         'pos': [],
         'origin': ['DOH'],
         'destination': [Host_Airline_Hub],
         'compartment': ['Y'],
         'fromDate': '2016-09-11',
         'toDate': '2016-10-10'
            }
result = get_sales_strength(filter)
#print result

# try:
#     import time
#
#     st = time.time()
#     filter = {
#          'region': ['GCC'],
#          'country': ['SA'],
#          'pos': ['RUH'],
#          'origin': ['RUH'],
#          'destination': ['CMB'],
#          'compartment': ['Y'],
#          'fromDate': '2016-09-11',
#          'toDate': '2016-09-11'
#             }
#     result = get_sales_strength(filter)
#     print result
#     # print time.time() - st
# except ZeroDivisionError as NA :
#     print "NA", NA
# except AttributeError as NA :
#     print "NA", NA






