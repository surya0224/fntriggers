"""
Mr. Author, please include header!

MODIFICATIONS LOG
    S.No                   : 1
    Date Modified          : 2016-12-29
    By                     : Shamail Mulla
    Modification Details   : Error and Exception handling added
"""

try:
    import json
    import time
    import inspect
    import pymongo
    import time
    import datetime
    from collections import defaultdict
    from copy import deepcopy
    from jupiter_AI import client, JUPITER_DB
    db = client[JUPITER_DB]

    from jupiter_AI.tiles.jup_common_functions_tiles import gen_collection_name as gen
    result_collection_name = gen()

except:
    pass

"""
Try to break up the working into smaller functions like for query_builder
Split file into DAL and BLL, all python processing to be done in BLL
Build and fire the queries in DAL
Leave sufficient lines when a piece of logic is completed
"""


"""
The code from line 111 feels repitative from line 50 onwards. If the only difference is the query being passed
then make a function out of it and pass queries with different parameters to it.
"""

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
    try:
        if 'JUP_DB_Sales' in db.collection_names():
            afilter = deepcopy(defaultdict(list, afilter)) # use proper variable names
            # create the query in a different function
            query = dict() # use proper variable names
            response = dict()
            # convert all keys in inputs filter to plural
            if afilter['region']:
                query['region'] = {'$in': afilter['region']}
            if afilter['country']:
                query['country'] = {'$in': afilter['country']}
            if afilter['pos']:
                query['pos'] = {'$in': afilter['pos']}
            if afilter['origin']:
                od_build = []
                for idx, item in enumerate(afilter['origin']):
                    od_build.append({'origin': item, 'destination': afilter['destination'][idx]})
                query['$or'] = od_build
            if afilter['compartment']:
                query['compartment'] = {'$in': afilter['compartment']}

            query['dep_date'] = {'$gte': afilter['fromDate'], '$lte': afilter['toDate']}

            apipeline = [
                {
                    '$match': query
                }
                # ,
                # {
                #     '$facet': {
                #         'compartment_wise': [
                #             {
                #                 '$group': {
                #                     '_id': '$compartment',
                #                     'revenue': {'$sum': 'revenue_base'}
                #                 }
                #             }
                #         ],
                #         'pax_type_wise': [
                #             {
                #                 '$group': {
                #                     '_id': '$pax_type',
                #                     'revenue': {'$sum': 'revenue_base'}
                #                 }
                #             }
                #         ]
                #     }
                # }
                ,
                {
                    '$group': {'_id': {'compartment': '$compartment',
                                       'pax_type': '$pax_type'},
                               'revenue': {'$sum': "$revenue_base"}}
                }
                ,
                {
                    '$project': {
                        '_id': 0,
                        'compartment': '$_id.compartment',
                        'pax_type': '$_id.pax_type',
                        'revenue': '$revenue'
                    }
                } # use $out : result_collection_name
            ]
            # use proper variable names
            cursor = db.JUP_DB_Sales.aggregate(apipeline) # do not assign to cursor
            '''
            if result_collection_name in db.collection_names():
                # pythonic variable given to the newly created collection
                collection = db.get_collection(collection_name)
                if collection.count > 0:

                    # retrieve useful data
                    collection.drop()
                    # logic

                else: # in case the collection is empty
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                        'jupter_AI/tiles/kpi/revenue_split.py method: get_tiles',
                                                        get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list('There is no data.')
                    obj_error.write_error_logs(datetime.datetime.now())

            else: # in case resultant collection is not created
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupter_AI/tiles/kpi/revenue_split.py method: get_tiles',
                                                    get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list(
                    str(result_collection_name + ' not created in the database. Check aggregate pipeline.'))

                obj_error.write_error_logs(datetime.datetime.now())
            '''
            data = list(cursor) # use proper variable names, like lst_...
            tot_revenue = sum(i['revenue'] for i in data)
            adt_rev = 0
            chd_rev = 0
            inf_rev = 0
            A_rev = 0
            J_rev = 0
            Y_rev = 0
            #Explain what are you trying to achieve
            if len(data) != 0:
                for i in data: # use proper variable names for 'i'
                    if i['pax_type'] == 'ADT ':
                        adt_rev += i['revenue']
                    elif i['pax_type'] == 'CHD ':
                        chd_rev += i['revenue']
                    elif i['pax_type'] == 'INF ':
                        inf_rev += i['revenue']

                    if i['compartment'] == 'A' or i['compartment'] == 'F':
                        A_rev += i['revenue']
                    elif i['compartment'] == 'J':
                        J_rev += i['revenue']
                    elif i['compartment'] == 'Y':
                        Y_rev += i['revenue']
            # try to do the following within the pipeline
            if tot_revenue != 0:
                response['passenger_type_user'] = {'ADT': round(float(adt_rev)*100 / tot_revenue,2),
                                                   'CHD': round(float(chd_rev)*100 / tot_revenue,2),
                                                   'INF': round(float(inf_rev)*100 / tot_revenue,2)}
                response['compartment_user'] = {'F': round(float(A_rev)*100 / tot_revenue,2),
                                                'J': round(float(J_rev)*100 / tot_revenue,2),
                                                'Y': round(float(Y_rev)*100 / tot_revenue,2)}
            else:
                response['passenger_type_user'] = {'ADT': "NA",
                                                   'CHD': "NA",
                                                   'INF': "NA"}
                response['compartment_user'] = {'F': "NA",
                                                'J': "NA",
                                                'Y': "NA"}
            # try to combine the 2 db hits in a single pipeline
            # use a better variable names
            apipeline_host = [
                {
                    '$match': {
                        'dep_date': {'$gte': afilter['fromDate'], '$lte': afilter['toDate']}}
                },
                {
                    '$group': {
                        '_id': {
                            'compartment': '$compartment',
                            'pax_type': '$pax_type'
                        },
                        'revenue': {'$sum': '$revenue_base'}
                    }
                },
                {
                    '$project': {
                        'compartment': '$_id.compartment',
                        'pax_type': '$_id.pax_type',
                        'revenue': '$revenue'
                    }
                }
            ]
            # use a better variable names
            cursor_host = db.JUP_DB_Sales.aggregate(apipeline_host)
            data_host = list(cursor_host) # use a better variable names
            tot_revenue_host = sum(i['revenue'] for i in data_host)
            adt_rev_host = 0
            chd_rev_host = 0
            inf_rev_host = 0
            A_rev_host = 0
            J_rev_host = 0
            Y_rev_host = 0
            if len(data_host) != 0:
                for i in data_host:
                    if i['pax_type'] == 'ADT ':
                        adt_rev_host += i['revenue']
                    elif i['pax_type'] == 'CHD ':
                        chd_rev_host += i['revenue']
                    elif i['pax_type'] == 'INF ':
                        inf_rev_host += i['revenue']

                    if i['compartment'] == 'A' or i['compartment'] == 'F':
                        A_rev_host += i['revenue']
                    elif i['compartment'] == 'J':
                        J_rev_host += i['revenue']
                    elif i['compartment'] == 'Y':
                        Y_rev_host += i['revenue']
            if tot_revenue_host != 0:
                response['passenger_type_host'] = {'ADT': round(float(adt_rev_host)*100 / tot_revenue_host,2),
                                                   'CHD': round(float(chd_rev_host)*100 / tot_revenue_host,2),
                                                   'INF': round(float(inf_rev_host)*100 / tot_revenue_host,2)}
                response['compartment_host'] = {'F': round(float(A_rev_host)*100 / tot_revenue_host,2),
                                                'J': round(float(J_rev_host)*100 / tot_revenue_host,2),
                                                'Y': round(float(Y_rev_host)*100 / tot_revenue_host,2)}
            else:
                response['passenger_type_host'] = {'ADT': "NA",
                                                   'CHD': "NA",
                                                   'INF': "NA"}
                response['compartment_host'] = {'F': "NA",
                                                'J': "NA",
                                                'Y': "NA"}
            return response
        else:  # in case the collection to query is not present
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/kpi/revenue_split.py method: get_tiles',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Sales cannot be found in the database.')
            obj_error.write_error_logs(datetime.datetime.now())

    # handling pymongo exceptions
    except (pymongo.errors.ServerSelectionTimeoutError,
                pymongo.errors.AutoReconnect,
                pymongo.errors.CollectionInvalid,
                pymongo.errors.ConfigurationError,
                pymongo.errors.ConnectionFailure,
                pymongo.errors.CursorNotFound,
                pymongo.errors.ExecutionTimeout
                ) as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'pymongo exception in jupter_AI/tiles/kpi/revenue_split.py method: get_tiles',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())
    # to handle any other pythonic exception that may have occurred
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/kpi/revenue_split.py method: get_tiles',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


def get_revenue_split_compartment(afilter):
    """

    :param afilter:
    :return:
    """
    try:
        if 'JUP_DB_Sales' in db.collection_names():
            afilter = deepcopy(defaultdict(list, afilter)) # use proper variable names
            query = dict() # use proper variable names
            response = dict()
            # create query in a separate function
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

            query['dep_date'] = {'$gte': afilter['fromDate'], '$lte': afilter['toDate']}

            apipeline = [ # use proper variable names
                {
                    '$match': query
                }
                ,
                {
                    '$group': {'_id': '$compartment',
                               'revenue': {'$sum': "$revenue_base"}}
                }
                ,
                {
                    '$project': {
                        '_id': 0,
                        'compartment': '$_id',
                        'revenue': '$revenue'
                    }
                } # use $out : result_collection_name

            ]
            cursor = db.JUP_DB_Sales.aggregate(apipeline) # use proper variable names
            '''
            if result_collection_name in db.collection_names():
                # pythonic variable given to the newly created collection
                collection = db.get_collection(collection_name)
                if collection.count > 0:

                    # retrieve useful data
                    collection.drop()
                    # logic

                else: # in case the resultant collection is empty
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                        'jupter_AI/tiles/kpi/revenue_split.py method: get_revenue_split_compartment',
                                                        get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list('There is no data.')
                    obj_error.write_error_logs(datetime.datetime.now())

            else: # in case resultant collection is not created
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupter_AI/tiles/kpi/revenue_split.py method: get_revenue_split_compartment',
                                                    get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list(
                    str(result_collection_name + ' not created in the database. Check aggregate pipeline.'))

                obj_error.write_error_logs(datetime.datetime.now())
            '''
            compartments_revenue_host = defaultdict(int)
            data = list(cursor) # use proper variable names
            tot_revenue_host = sum(i['revenue'] for i in data) # use proper variable names for 'i'
            response['revenue_percent_F'] = 0
            response['revenue_percent_J'] = 0
            response['revenue_percent_Y'] = 0
            if len(data) != 0:
                for i in data:
                    if i['compartment'] == 'F' or i['compartment'] == 'A':
                        response['revenue_percent_F'] = round(i['revenue']*100 / tot_revenue_host,2)
                    elif i['compartment'] == 'J':
                        response['revenue_percent_J'] = round(i['revenue']*100 / tot_revenue_host,2)
                    elif i['compartment'] == 'Y':
                        response['revenue_percent_Y'] = round(i['revenue']*100 / tot_revenue_host,2)
                    else:
                        response['NA'] = round(i['revenue']*100 / tot_revenue_host,2)
            else:
                response['revenue_percent_F'] = "NA"
                response['revenue_percent_J'] = "NA"
                response['revenue_percent_Y'] = "NA"
                response['NA'] = 'NA'
            return response
        else:  # in case the collection to query is not present
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/kpi/revenue_split.py method: get_revenue_split_compartment',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Sales cannot be found in the database.')
            obj_error.write_error_logs(datetime.datetime.now())
    # handling pymongo exceptions
    except (pymongo.errors.ServerSelectionTimeoutError,
                pymongo.errors.AutoReconnect,
                pymongo.errors.CollectionInvalid,
                pymongo.errors.ConfigurationError,
                pymongo.errors.ConnectionFailure,
                pymongo.errors.CursorNotFound,
                pymongo.errors.ExecutionTimeout
                ) as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'pymongo exception in jupter_AI/tiles/kpi/revenue_split.py method: get_revenue_split_compartment',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())
    # to handle any other pythonic exception that may have occurred
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/kpi/revenue_split.py method: get_revenue_split_compartment',
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
        'origin': ["DXB"],
        'destination': ["DOH"],
        'compartment': ["Y"],
        'fromDate': '2017-02-14',
        'toDate': '2017-02-20'
    }
    print get_tiles(afilter=a)
    print time.time() - st
