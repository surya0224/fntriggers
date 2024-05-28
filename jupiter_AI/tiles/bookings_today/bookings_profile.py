"""
File Name              :   bookings_profile.py
Author                 :   Pavan
Date Created           :   2016-12-19
Description            :   module with methods to calculate bookings profile for jupiter dashboard (bookings today)
MODIFICATIONS LOG      :
    S.No               :
    Date Modified      :
    By                 :
    Modification Details   :
"""
import inspect
from collections import defaultdict
from copy import deepcopy
from jupiter_AI import client,JUPITER_DB
db = client[JUPITER_DB]



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


def get_bookings_profile(afilter):
    data = deepcopy(defaultdict(list, afilter))
    query = dict()
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
        if data['flag'] == 'true':
            bookings_profile_pipeline = db['JUP_DB_Booking_DepDate'].aggregate([
                                                             {
                                                                '$match': query
                                                             }
                                                             ,
                                                             {
                                                                 '$group':
                                                                     {
                                                                     '_id':
                                                                         {
                                                                            'segment': '$segment_status'},
                                                                            'Bookings': {'$sum': '$pax'},
                                                                            'Ticketed': {'$sum': '$ticket'}
                                                                     }
                                                             }
                                                             ])

        else:
            query['dep_date'] = {'$in': [data['fromDate'], data['toDate']]}
            bookings_profile_pipeline = db['JUP_DB_Cumulative_Trx_Date'].aggregate([
                                                                 {
                                                                    '$match': query
                                                                 }
                                                                 ,
                                                                 {
                                                                     '$group':
                                                                         {
                                                                             '_id':
                                                                                 {
                                                                                'segment_status': '$segment_status'},
                                                                                'tobookings': {'$max': '$pax'},
                                                                                'frombookings': {'$min': '$pax'},
                                                                                'toticket': {'$max': '$ticket'},
                                                                                'fromticket': {'$min': '$ticket'}
                                                                         }
                                                                 }
                                                                 ,
                                                                 {
                                                                     '$project':
                                                                         {
                                                                             'segment':'$_id.segment_status',
                                                                             'Bookings': {'$subtract': ['$tobookings','$frombookings']},
                                                                             'Ticketed': {'$subtract': ['$toticket','$fromticket']}
                                                                         }
                                                                 }
                                                                 ])
    else:
        query['dep_date'] = {'$in': [data['fromDate'], data['toDate']]}
        bookings_profile_pipeline = db['JUP_DB_Cumulative_Dep_Date'].aggregate([
                                                             {
                                                                '$match': query
                                                             }
                                                             ,
                                                             {
                                                                 '$group':
                                                                     {
                                                                         '_id':
                                                                             {
                                                                                 'segment': '$segment_status'},
                                                                                 'tobookings': {'$max': '$pax'},
                                                                                 'frombookings': {'$min': '$pax'},
                                                                                 'toticket': {'$max': '$ticket'},
                                                                                 'fromticket': {'$min': '$ticket'}
                                                                     }
                                                             }
                                                             ,
                                                             {
                                                                 '$project':
                                                                     {
                                                                         'segment': '$_id.segment',
                                                                         'Bookings': {'$subtract': ['$tobookings','$frombookings']},
                                                                         'Ticketed': {'$subtract': ['$toticket','$fromticket']}}
                                                             }
                                                             ])

    bookings = 0
    ticketed_bookings = 0
    net_bookings = 0
    Ticketed_bookings = 0
    bookings_profile_pipeline_list = list(bookings_profile_pipeline)
    if len(bookings_profile_pipeline_list) != 0:
        #print bookings_profile_pipeline_list
        for i in bookings_profile_pipeline_list:
            bookings += i['Bookings']
            if i['_id']['segment'] == 'ACTIVE':
                net_bookings = i['Bookings']
                ticketed_bookings += i['Ticketed']
                #print ticketed_bookings
                Ticketed_bookings = (float(ticketed_bookings) / bookings) * 100
            # print Ticketed_bookings
            response = dict()
            response['Net_bookings'] = net_bookings
            response['Ticketed_bookings'] = Ticketed_bookings
        return response
    # else:
    #     pass
    #     # e1=error_class.ErrorObject(error_class.ErrorObject.ERROR,
    #     # self.get_module_name(),
    #     # self.get_arg_lists(inspect.currentframe()))
    #     # e1.append_to_error_list("Expected 1 document for bookings but got " +str(len(data)))
    #     # raise e1
          #return NA


try:
    import time

    st = time.time()
    filter = {
         'region': ['GCC'],
         'country': ['SA'],
         'pos': ['RUH'],
         'origin': ['RUH'],
         'destination': ['CMB'],
         'compartment': ['Y'],
         'fromDate': '2016-09-11',
         'toDate': '2016-09-11',
         'flag':'true'
            }
    result = get_bookings_profile(filter)
    print result
    # print time.time() - st
except ZeroDivisionError as result :
    print "NA", result
except AttributeError as NA :
    print "NA", NA
