"""
File Name              :   price_agility_index.py
Author                 :   Sai Krishna
Date Created           :   2016-12-19
Description            :   Functions for tiles calculation in price agility index screen of KPI dashboard
Data access layer

MODIFICATIONS LOG
    S.No                   : 1
    Date Modified          : 2016-12-29
    By                     : Shamail Mulla
    Modification Details   : Error and Exception handling added
"""

# try:

from jupiter_AI.network_level_params import na_value, SYSTEM_DATE
from jupiter_AI import client, JUPITER_DB
db = client[JUPITER_DB]

# except:
#     pass


"""
Try to break up the working into smaller functions like for query_builder
Split file into DAL and BLL, all python processing to be done in BLL
Build and fire the queries in DAL
Leave sufficient lines when a piece of logic is completed
"""

import datetime
import inspect
from collections import defaultdict
from copy import deepcopy

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
    4   tiles
    price agility index
    no of change/ no of fares (Host Level)
    no of changes/ no of fares Analyst level(User level)
    competitor PSI(Cannot find as of right now so hard coded to 'NA')
    :param filter_pai_scr:
    :return:
    """
    try:
        if 'JUP_DB_Workflow' in db.collection_names() and 'JUP_DB_ATPCO_Fares' in db.collection_names():
            filter_pai_scr = deepcopy(defaultdict(list, afilter))
            query = defaultdict(list)

            response = dict()
            # try to create the query in a different method
            if filter_pai_scr['region']:
                query['$and'].append({'region': {'$in': filter_pai_scr['region']}})
            if filter_pai_scr['country']:
                query['$and'].append({'country': {'$in': filter_pai_scr['country']}})
            if filter_pai_scr['pos']:
                query['$and'].append({'pos': {'$in': filter_pai_scr['pos']}})
            if filter_pai_scr['origin']:
                od_build = []
                for idx, item in enumerate(filter_pai_scr['origin']):
                    od_build.append({'origin': item, 'destination': filter_pai_scr['destination'][idx]})
                query['$and'].append({'$or': od_build})
            if filter_pai_scr['compartment']:
                query['$and'].append({'compartment': {'$in': filter_pai_scr['compartment']}})
            #   building the query for JUP_DB_Cum_Pricing_Actions collection for recommendation
            #  where action has already been taken between the two dates given in filter
            qry_cum_pricing_actions = deepcopy(query)
            qry_cum_pricing_actions['$and'].append({'action_date':{'$lte':SYSTEM_DATE}})
            qry_cum_pricing_actions['$and'].append({'status': 'APPROVE'})
            #   qry_cum_pricing_actions['$and'].append({'action_date': {'$gte': filter_pai_scr['fromDate'],
            #  '$lte': filter_pai_scr['toDate']}})
            cursor1 = db.JUP_DB_Workflow.find(qry_cum_pricing_actions)
            num_of_changes_filter = cursor1.count()

            qry_fares = deepcopy(query)
            qry_fares['$and'].append({'effective_from': {'$lte': SYSTEM_DATE}})
            qry_fares['$and'].append({'effective_to': {'$gte': SYSTEM_DATE}})
            # qry_fares['$and'].append({'dep_date': {'$gte': filter_pai_scr['fromDate'],
            #                                     '$lte': filter_pai_scr['toDate']}})
            cursor2 = db.JUP_DB_ATPCO_Fares.find(qry_fares)
            no_of_fares_filter = cursor2.count()

            if no_of_fares_filter != 0:
                pai = num_of_changes_filter/no_of_fares_filter
            else:
                pai = na_value

            response['user_price_agility_index'] = pai
            response['no_of_changes_user'] = num_of_changes_filter
            response['total_no_of_fares_user'] = no_of_fares_filter
            response['competitor_pai'] = na_value

            crsr_changes_host = db.JUP_DB_Cum_Pricing_Actions.find({'status': 'approved'
                                                                       # ,
                                                                    # 'action_date': {
                                                                    #     '$gte': filter_pai_scr['fromDate'],
                                                                    #     '$lte': filter_pai_scr['toDate']}
                                                                    })
            no_changes_host = crsr_changes_host.count()

            crsr_total_fares_host = db.JUP_DB_ATPCO_Fares.find({'effective_from': {'$lte': SYSTEM_DATE},
                                                                'effective_to': {'$gte': SYSTEM_DATE}
                                                                #    ,
                                                                # 'dep_date': {
                                                                #     '$gte': filter_pai_scr['fromDate'],
                                                                #     '$lte': filter_pai_scr['toDate']}
                                                                })
            total_fares_host = crsr_total_fares_host.count()
            response['no_of_changes_host'] = no_changes_host
            response['total_fares_host'] = total_fares_host
            if total_fares_host != 0:
                response['host_price_agility_index'] = no_changes_host/total_fares_host
            else:
                response['host_price_agility_index'] = None
            return response
        else:  # in case either one of the collections to query are not present
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/kpi/price_agility_index.py method: get_tiles',
                                                get_arg_lists(inspect.currentframe()))
            if 'JUP_DB_Workflow' not in db.collection_names():
                obj_error.append_to_error_list(
                    'Collection/s JUP_DB_Workflow cannot be found in the database.')
            else:
                obj_error.append_to_error_list('Collection/s JUP_DB_ATPCO_Fares cannot be found in the database.')
            obj_error.write_error_logs(datetime.datetime.now())
    # handling pymongo exceptions
    # except (pymongo.errors.ServerSelectionTimeoutError,
    #             pymongo.errors.AutoReconnect,
    #             pymongo.errors.CollectionInvalid,
    #             pymongo.errors.ConfigurationError,
    #             pymongo.errors.ConnectionFailure,
    #             pymongo.errors.CursorNotFound,
    #             pymongo.errors.ExecutionTimeout
    #             ) as error_msg:
    #     obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
    #                                         'pymongo exception in jupter_AI/tiles/kpi/price_agility_index.py method: get_tiles',
    #                                         get_arg_lists(inspect.currentframe()))
    #     obj_error.append_to_error_list(str(error_msg))
    #     obj_error.write_error_logs(datetime.datetime.now())
    # to handle any other pythonic exception that may have occurred
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/kpi/price_agility_index.py method: get_tiles',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


def get_price_agility_index(afilter):
    """

    :return:
    """
    try:
        if 'JUP_DB_Workflow' in db.collection_names() and 'JUP_DB_ATPCO_Fares' in db.collection_names():
            filter_pai_scr = deepcopy(defaultdict(list, afilter))
            query = dict()
            query['$and'] = []
            response = dict()
            if filter_pai_scr['region']:
                query['$and'].append({'region': {'$in': filter_pai_scr['region']}})
            if filter_pai_scr['country']:
                query['$and'].append({'country': {'$in': filter_pai_scr['country']}})
            if filter_pai_scr['pos']:
                query['$and'].append({'pos': {'$in': filter_pai_scr['pos']}})
            if filter_pai_scr['origin']:
                od_build = []
                for idx, item in enumerate(filter_pai_scr['origin']):
                    od_build.append({'origin': item, 'destination': filter_pai_scr['destination'][idx]})
                query['$and'].append({'$or': od_build})
            if filter_pai_scr['compartment']:
                query['$and'].append({'compartment': {'$in': filter_pai_scr['compartment']}})

            # building the query for JUP_DB_Cum_Pricing_Actions collection for recommendation
            #  where action has already been taken between the two dates given in filter
            qry_cum_pricing_actions = deepcopy(query)
            qry_cum_pricing_actions['$and'].append({'status': 'APPROVE'})
            qry_cum_pricing_actions['$and'].append(
                {'action_date': {'$lte': SYSTEM_DATE}})
            cursor1 = db.JUP_DB_Cum_Pricing_Actions_New_Test_1.find(qry_cum_pricing_actions)
            num_of_changes_filter = cursor1.count()

            qry_fares = deepcopy(query)
            qry_fares['$and'].append({'effective_from': {'$lte': SYSTEM_DATE}})
            qry_fares['$and'].append({'effective_to': {'$gte': SYSTEM_DATE}})
            # qry_fares['$and'].append({'dep_date': {'$gte': filter_pai_scr['fromDate'],
            #                                     '$lte': filter_pai_scr['toDate']}})
            cursor2 = db.JUP_DB_ATPCO_Fares.find(qry_fares)
            no_of_fares_filter = cursor2.count()
            if no_of_fares_filter != 0:
                pai = num_of_changes_filter / no_of_fares_filter
            else:
                pai = na_value
            return pai
        else:  # in case either one of the collections to query are not present
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupiter_AI/tiles/kpi/price_agility_index.py method: get_price_agility_index',
                                                get_arg_lists(inspect.currentframe()))
            if 'JUP_DB_Cum_Pricing_Actions' not in db.collection_names():
                obj_error.append_to_error_list('Collection/s JUP_DB_Workflow cannot be found in the database.')
            else:
                obj_error.append_to_error_list('Collection/s JUP_DB_ATPCO_Fares cannot be found in the database.')
            obj_error.write_error_logs(datetime.datetime.now())
    # handling pymongo exceptions
    # except (pymongo.errors.ServerSelectionTimeoutError,
    #             pymongo.errors.AutoReconnect,
    #             pymongo.errors.CollectionInvalid,
    #             pymongo.errors.ConfigurationError,
    #             pymongo.errors.ConnectionFailure,
    #             pymongo.errors.CursorNotFound,
    #             pymongo.errors.ExecutionTimeout
    #             ) as error_msg:
    #     obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
    #                                         'pymongo exception in jupter_AI/tiles/kpi/price_agility_index.py method: get_price_agility_index',
    #                                         get_arg_lists(inspect.currentframe()))
    #     obj_error.append_to_error_list(str(error_msg))
    #     obj_error.write_error_logs(datetime.datetime.now())
    # to handle any other pythonic exception that may have occurred
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/kpi/price_agility_index.py method: get_price_agility_index',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


def get_price_stability_index(filter_pai_scr):
    try:
        if 'JUP_DB_Sales_Flown' in db.collection_names() and 'JUP_DB_ATPCO_RBD_To_Fare_Type' in db.collection_names():
            filter_pai_scr = deepcopy(defaultdict(list, filter_pai_scr))
            query = dict()
            response = dict()
            if filter_pai_scr['region']:
                query['region'] = {'$in': filter_pai_scr['region']}
            if filter_pai_scr['country']:
                query['country'] = {'$in': filter_pai_scr['country']}
            if filter_pai_scr['pos']:
                query['pos'] = {'$in': filter_pai_scr['pos']}
            if filter_pai_scr['origin']:
                od_build = []
                for idx, item in enumerate(filter_pai_scr['origin']):
                    od_build.append({'origin': item, 'destination': filter_pai_scr['destination'][idx]})
                query['$or'] = od_build
            if filter_pai_scr['compartment']:
                query['compartment'] = {'$in': filter_pai_scr['compartment']}
            query['dep_date'] = {'$gte': filter_pai_scr['fromDate'],
                                 '$lte': filter_pai_scr['toDate']}

            # try to do all the operations below in 1 aggregate pipeline
            cursor = db.JUP_DB_ATPCO_RBD_To_Fare_Type.find()
            rbds_tactical = []
            rbds_specific = []
            rbds_strategic = []
            if cursor.count() != 0:
                for i in cursor:
                    if i['fare_type'] == 'Strategic':
                        rbds_strategic.append(i['rbd'])
                    elif i['fare_type'] == 'Tactical':
                        rbds_tactical.append(i['rbd'])
                    elif i['fare_type'] == 'Specific':
                        rbds_specific.append(i['rbd'])
            else:
                pass

            apipeline = [
                {
                    '$match': query
                },
                {
                    '$group': {
                        '_id': '$fare_basis',
                        'revenue': {'$sum': '$revenue_base'}
                    }
                },
                {
                    '$project': {
                        '_id': 0,
                        'farebasis': '$_id',
                        'rbd': {'$substr': ['$_id', 0, 1]},
                        'revenue': '$revenue'
                        }
                }
                # {
                #     '$group': {
                #         '_id': None,
                #         'rev_tactical': {
                #             "$sum": {"$cond": [
                #                 {'$in': ['$rbd', rbds_tactical]},
                #                 '$revenue',
                #                 0
                #             ]}},
                #         'rev_strategic': {
                #             "$sum": {"$cond": [
                #                 {'$in': ['$rbd', rbds_strategic]},
                #                 '$revenue',
                #                 0
                #             ]}},
                #         'rev_specific': {
                #             "$sum": {"$cond": [
                #                 {'$in': ['$rbd', rbds_specific]},
                #                 '$revenue',
                #                 0
                #             ]}}
                #         }
                # },
                # {
                #     '$project': {
                #         '_id': 0,
                #         'revenue':'$revenue'
                #     }
                # }

                # use $out : result_collection_name
            ]

            cursor2 = db.JUP_DB_Sales_Flown.aggregate(apipeline) # do no assign cursor yet, simply run aggregate pipeline
            '''
            if result_collection_name in db.collection_names():
                # pythonic variable given to the newly created collection
                collection = db.get_collection(result_collection_name)
                if collection.count > 0:

                    # retrieve important data
                    collection.drop()
                    # logic

                else: # in case the collection is empty
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                        'jupter_AI/tiles/kpi/price_agility_index.py method: get_price_stability_index',
                                                        get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list('There is no data.')

                    obj_error.write_error_logs(datetime.datetime.now())
            else: # in case resultant collection is not created
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupter_AI/tiles/kpi/price_agility_index.py method: get_price_stability_index',
                                                    get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list(
                    str(result_collection_name + ' not created in the database. Check aggregate pipeline.'))

                obj_error.write_error_logs(datetime.datetime.now())
            '''
            data2 = []
            tactical_revenue = 0
            strategic_revenue = 0
            specific_revenue = 0
            tot_revenue = 0
            # try to do all of the operations below within the aggregate pipeline
            for i in cursor2:
                data2.append(i)
            if len(data2) != 0:
                for j in data2:
                    tot_revenue += j['revenue']
                    if j['rbd'] in rbds_tactical:
                        tactical_revenue += j['revenue']
                    elif j['rbd'] in rbds_strategic:
                        strategic_revenue += j['revenue']
                    elif j['rbd'] in rbds_specific:
                        specific_revenue += j['revenue']
                if tot_revenue != 0:
                    tactical = (tactical_revenue / tot_revenue) * 100 / 30
                    specific = (specific_revenue / tot_revenue) * 100 / 20
                    strategy = (strategic_revenue / tot_revenue) * 100 / 50
                    if tactical > 1:
                        tactical = 1
                    if specific > 1:
                        specific = 1
                    if strategy > 1:
                        strategy = 1
                    psi = round((tactical + specific + strategy)*10/3, 4)
                else:
                    psi = na_value
            else:
                psi = na_value
            return psi
        else:  # in case either one of the collections to query are not present
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/kpi/price_agility_index.py method: get_price_stability_index',
                                                get_arg_lists(inspect.currentframe()))
            if 'JUP_DB_ATPCO_RBD_To_Fare_Type' not in db.collection_names():
                obj_error.append_to_error_list('Collection/s JUP_DB_ATPCO_RBD_To_Fare_Type cannot be found in the database.')
            else:
                obj_error.append_to_error_list('Collection/s JUP_DB_Sales_Flown cannot be found in the database.')
            obj_error.write_error_logs(datetime.datetime.now())
    # handling pymongo exceptions
    # except (pymongo.errors.ServerSelectionTimeoutError,
    #             pymongo.errors.AutoReconnect,
    #             pymongo.errors.CollectionInvalid,
    #             pymongo.errors.ConfigurationError,
    #             pymongo.errors.ConnectionFailure,
    #             pymongo.errors.CursorNotFound,
    #             pymongo.errors.ExecutionTimeout
    #             ) as error_msg:
    #     obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
    #                                         'pymongo exception in jupter_AI/tiles/kpi/price_agility_index.py method: get_price_stability_index',
    #                                         get_arg_lists(inspect.currentframe()))
    #     obj_error.append_to_error_list(str(error_msg))
    #     obj_error.write_error_logs(datetime.datetime.now())
    # to handle any other pythonic exception that may have occurred
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/kpi/price_agility_index.py method: get_price_stability_index',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


if __name__ == '__main__':
    import time
    st = time.time()
    a = {
        'region': [],
        'country': [],
        'pos': [],
        'origin': [],
        'destination': [],
        'compartment': [],
        'fromDate': '2016-01-01',
        'toDate': '2016-12-28'
    }
    print get_tiles(a)
