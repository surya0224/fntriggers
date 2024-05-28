"""
Mr. Author, Please include header!

MODIFICATIONS LOG
    S.No                   : 1
    Date Modified          : 2017-01-20
    By                     : Shamail Mulla
    Modification Details   : Error and Exception handling added

    S.No                   : 2
    Date Modified          : 2017-01-31
    By                     : Sai Krishna
    Modification Details   : Changed the code to get the new products from
                             static to dynamic
"""
try:
    import json
    import time
    from collections import defaultdict
    from copy import deepcopy
    from jupiter_AI import client,JUPITER_DB
    db = client[JUPITER_DB]
    from jupiter_AI.network_level_params import Host_Airline_Code # as host


except:
    pass

import datetime
import inspect

from jupiter_AI.common import ClassErrorObject as error_class


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

def get_tiles(afilter):
    """

    :param afilter:
    :return:
    """
    #   no of new products host level
    #   no of products user level
    #   no of products competitor host level
    #   total bookings obtained for these new competitors
    try:
        if 'JUP_DB_New_Products' in db.collection_names():
            afilter = deepcopy(defaultdict(list, afilter)) # use proper variable names
            query = dict() # use proper variable names
            response = dict()
            if afilter['region']:
                query['region'] = {'$in': afilter['region']}
            if afilter['country']:
                query['country'] = {'$in': afilter['country']}
            if afilter['pos']:
                query['pos'] = {'$in': afilter['pos']}
            if afilter['origin']:
                ods = []
                for idx, item in enumerate(afilter['origin']):
                    ods.append(item+afilter['detination'][idx])
                query['od'] = {'$in': ods}
            if afilter['compartment']:
                query['compartment'] = {'$in': afilter['compartment']}

            query['carrier'] = Host_Airline_Code
            query['date'] = {'$gte': afilter['fromDate'], '$lte': afilter['toDate']}
            response['new_products_host'] = db.JUP_DB_New_Products.find({'carrier': Host_Airline_Code}).count()
            response['new_products_competitor'] = db.JUP_DB_New_Products.find({'carrier': {'$ne': Host_Airline_Code}}).count()
            response['new_products_user_level'] = db.JUP_DB_New_Products.find(query).count()
            response['new_product_bookings'] = 'NA'

            return response
        else:
            # in case either one of the collection to query is not present
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/kpi/new_prducts.py method: get_tiles',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection/s JUP_DB_New_Products cannot be found in the database.')
            obj_error.write_error_logs(datetime.datetime.now())
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/kpi/new_products.py method: get_tiles',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())

def get_new_products(afilter):
    """

    :param afilter:
    :return:
    """
    try:
        if 'JUP_DB_New_Products' in db.collection_names():
            afilter = deepcopy(defaultdict(list, afilter))
            query = dict() # use proper variable names
            response = dict()
            if afilter['region']:
                query['region'] = {'$in': afilter['region']}
            if afilter['country']:
                query['country'] = {'$in': afilter['country']}
            if afilter['pos']:
                query['pos'] = {'$in': afilter['pos']}
            if afilter['origin']:
                od_build = []
                for idx, item in enumerate(afilter['origin']): # use proper variable names for 'idx' and 'item'
                    od_build.append({'origin': item, 'destination': afilter['destination'][idx]})
                query['$or'] = od_build
            if afilter['compartment']:
                query['compartment'] = {'$in': afilter['compartment']}

            query['date'] = {'$gte': afilter['fromDate'], '$lte': afilter['toDate']}
            query['carrier'] = Host_Airline_Code

            return db.JUP_DB_New_Products.find(query).count()
        else:
            # in case either one of the collection to query is not present
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/kpi/new_prducts.py method: get_new_prodcuts',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection/s JUP_DB_New_Products cannot be found in the database.')
            obj_error.write_error_logs(datetime.datetime.now())

    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/kpi/new_products.py method: get_new_products',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


if __name__ == '__main__':
    st = time.time()

    # use proper variable names
    a = {
        'region': [],
        'country': [],
        'pos': [],
        'origin': [],
        'destination': [],
        'compartment': [],
        'fromDate': '2016-07-01',
        'toDate': '2016-12-31',
        'lastfromDate': '2015-07-01',
        'lasttoDate': '2015-12-31'
    }
    print get_tiles(afilter=a)
    print get_new_products(afilter=a)
    print time.time() - st
