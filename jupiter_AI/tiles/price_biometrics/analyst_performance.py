'''
header!

MODIFICATIONS LOG
    S.No                   :    1
    Date Modified          :    2016-12-28
    By                     :    Shamail Mulla
    Modification Details   :    Added error / exception handling codes
'''

import datetime
import inspect
from collections import defaultdict
from copy import deepcopy

import pymongo

from jupiter_AI.common import ClassErrorObject as error_class
from jupiter_AI import client,JUPITER_DB, na_value
db = client[JUPITER_DB]

'''
Leave lines when a section of logic is completed
Break up the program into DAL and BLL and do pythonic calculations in BLL
'''
def get_module_name():
    '''
    function used to get the module name where it is called
    '''
    return inspect.stack()[1][3]


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


def build_qry_sales_flown(filter_scr):
    filter_scr = deepcopy(defaultdict(list, filter_scr))
    qry_sales = dict()
    response = dict()
    if filter_scr['region']:
        qry_sales['region'] = {'$in': filter_scr['region']}
    if filter_scr['country']:
        qry_sales['country'] = {'$in': filter_scr['country']}
    if filter_scr['pos']:
        qry_sales['pos'] = {'$in': filter_scr['pos']}
    if filter_scr['origin']:
        od_build = []
        for idx, item in enumerate(filter_scr['origin']):
            od_build.append({'origin': item, 'destination': filter_scr['destination'][idx]})
        qry_sales['$or'] = od_build
    if filter_scr['compartment']:
        qry_sales['compartment'] = {'$in': filter_scr['compartment']}
    qry_sales['dep_date'] = {'$gte': filter_scr['fromDate'],
                             '$lte': filter_scr['toDate']}
    return qry_sales


def get_price_stability_index_vlyr(filter_scr):
    try:
        #   Get the list of RBDs alloted to a fare type
        if 'JUP_DB_ATPCO_RBD_To_Fare_Type' in db.collection_names():
            cursor = db.JUP_DB_ATPCO_RBD_To_Fare_Type.find() # give proper variable names
            rbds_tactical = []
            rbds_specific = []
            rbds_strategic = []
            # try to do this in pipeline itself
            if cursor.count() != 0:
                for i in cursor:
                    if i['fare_type'] == 'Strategic':
                        rbds_strategic.append(i['rbd'])
                    elif i['fare_type'] == 'Tactical':
                        rbds_tactical.append(i['rbd'])
                    elif i['fare_type'] == 'Specific':
                        rbds_specific.append(i['rbd'])
            else:
                no_rbd_data_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                            get_module_name(),
                                                            get_arg_lists(inspect.currentframe()))
                no_rbd_data_error.append_to_error_list('No Data Related to Mapping of an RBD to fare type')

            #   Get the Price Stability Index by using the above three lists of RBDS
            if 'JUP_DB_Sales_Flown' in db.collection_names():
                response = dict()
                qry_sales = build_qry_sales_flown(filter_scr)
                apipeline = [
                    {
                        '$match': qry_sales
                    }
                    ,
                    {
                        '$group': {
                            '_id': '$fare_basis',
                            'revenue': {'$sum': '$revenue_base'},
                            'revenue_ly': {'$sum': '$revenue_base_1'}
                        }
                    }
                    ,
                    {
                        '$project': {
                            '_id': 0,
                            'farebasis': '$_id',
                            'rbd': {'$substr': ['$_id', 0, 1]},
                            'revenue': '$revenue',
                            'revenue_ly': '$revenue_ly'
                            }
                    }
                    # ,
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
                    #             ]}},
                    #         'rev_tactical_ly': {
                    #             "$sum": {"$cond": [
                    #                 {'$in': ['$rbd', rbds_tactical]},
                    #                 '$revenue_ly',
                    #                 0
                    #             ]}},
                    #         'rev_strategic_ly': {
                    #             "$sum": {"$cond": [
                    #                 {'$in': ['$rbd', rbds_strategic]},
                    #                 '$revenue_ly',
                    #                 0
                    #             ]}},
                    #         'rev_specific_ly': {
                    #             "$sum": {"$cond": [
                    #                 {'$in': ['$rbd', rbds_specific]},
                    #                 '$revenue_ly',
                    #                 0
                    #             ]}}
                    #         },
                    #         'revenue_ly': {"$sum": '$revenue_ly'},
                    #         'revenue': {'$sum':'$revenue'}
                    # }
                    # ,
                    # {
                    #     '$project': {
                    #         'tactical': {
                    #             '$cond': {
                    #                 'if':{'$gt':['$revenue', 0]},
                    #                 'then': {'$divide':[{'$multiply':['$rev_tactical',100/30]},'$revenue']},
                    #                 'else': None
                    #             }
                    #         }
                    #         ,
                    #         'strategic': {
                    #             '$cond': {
                    #                 'if':{'$gt':['$revenue':0]},
                    #                 'then':{'$divide':[{'$multiply':['$rev_strategic',100/50]},'$revenue']},
                    #                 'else': None
                    #             }
                    #         }
                    #         ,
                    #         'specific': {
                    #             '$cond': {
                    #                 'if':{'$gt':['$revenue':0]},
                    #                 'then':{'$divide':[{'$multiply':['$rev_specific',100/50]},'$revenue']},
                    #                 'else': None
                    #             }
                    #         }
                    #     }
                    # }
                    # # use $out : result_collection_name
                ]
                # please do everything in 1 pipeline / db hit
                cursor2 = db.JUP_DB_Sales_Flown.aggregate(apipeline) # use proper variable names
                '''
                    if result_collection_name in db.collection_names():
                        # pythonic variable given to the newly created collection
                        some_collection = db.get_collection(result_collection_name)
                        if some_collection.count > 0:

                            # retrieve relevant data from collection

                            some_collection.drop()
                            response = dict()

                            # logic

                        else: # in case the collection is empty
                            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                                'jupter_AI/tiles/price_biometrics/analyst_performance.py method: get_price_stability_index_vlyr',
                                                                get_arg_lists(inspect.currentframe()))
                            obj_error.append_to_error_list('There is no data.')

                            obj_error.write_error_logs(datetime.datetime.now())

                    else: # result collection not created
                        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                        'jupter_AI/tiles/price_biometrics/analyst_performance.py method: get_price_stability_index_vlyr',
                                                        get_arg_lists(inspect.currentframe()))
                        obj_error.append_to_error_list(
                            str(result_collection_name + ' not created in the database. Check aggregate pipeline.'))

                        obj_error.write_error_logs(datetime.datetime.now())
                '''
                data2 = [] # use proper variable names
                tactical_revenue = 0
                strategic_revenue = 0
                specific_revenue = 0
                tot_revenue = 0
                tot_revenue_ly = 0
                tactical_revenue_ly = 0
                strategic_revenue_ly = 0
                specific_revenue_ly = 0
                # try to do this in the pipeline itself
                # use comments to explain what is going on
                for i in cursor2:
                    data2.append(i)
                # print data2
                if len(data2) != 0:
                    for j in data2:
                        tot_revenue += j['revenue']
                        tot_revenue_ly += 0
                        if j['rbd'] in rbds_tactical:
                            tactical_revenue += j['revenue']
                            tactical_revenue_ly += j['revenue_ly']
                        elif j['rbd'] in rbds_strategic:
                            strategic_revenue += j['revenue']
                            strategic_revenue_ly += j['revenue_ly']
                        elif j['rbd'] in rbds_specific:
                            specific_revenue += j['revenue']
                            specific_revenue_ly += j['revenue_ly']
                    if tot_revenue != 0:
                        tactical = (tactical_revenue / tot_revenue) * 100 / float(30)
                        specific = (specific_revenue / tot_revenue) * 100 / float(20)
                        strategy = (strategic_revenue / tot_revenue) * 100 / float(50)
                        if tactical > 1:
                            tactical = 1
                        if specific > 1:
                            specific = 1
                        if strategy > 1:
                            strategy = 1
                        psi = round((tactical + specific + strategy)*10/3, 4)
                    else:
                        psi = na_value
                    if tot_revenue_ly != 0:
                        tactical_ly = (tactical_revenue_ly / tot_revenue_ly) * 100 / float(30)
                        specific_ly = (specific_revenue_ly / tot_revenue_ly) * 100 / float(20)
                        strategy_ly = (strategic_revenue_ly / tot_revenue_ly) * 100 / float(50)
                        if tactical_ly > 1:
                            tactical_ly = 1
                        if specific_ly > 1:
                            specific_ly = 1
                        if strategy_ly > 1:
                            strategy_ly = 1
                        psi_ly = round((tactical_ly + specific_ly + strategy_ly)*10/float(3), 4)
                    else:
                        psi_ly = na_value
                    if psi_ly != na_value and psi != na_value and psi_ly != 0:
                        psi_vlyr = (psi - psi_ly)*100/psi_ly
                    else:
                        psi_vlyr = na_value
                else:
                    psi = na_value
                    psi_vlyr = na_value
                response['price_stability_index'] = psi
                response['price_stability_index_vlyr'] = psi_vlyr
                return response
            else: # If JUP_DB_Sales_Flown collection to qry_sales is not present
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupter_AI/tiles/price_biometrics/analyst performance.py method: get_price_stability_index_vlyr',
                                                    get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list(
                    'Collection JUP_DB_Sales_Flown cannot be found in the database.')
                obj_error.write_error_logs(datetime.datetime.now())
        else: # If JUP_DB_ATPCO_RBD_To_Fare_Type collection to qry_sales is not present
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/price_biometrics/analyst performance.py method: get_price_stability_index_vlyr',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_ATPCO_RBD_To_Fare_Type cannot be found in the database.')
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
                                            'pymongo exception in jupter_AI/tiles/price_biometrics/analyst_performance.py method: get_price_stability_index_vlyr',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())
    # to handle any other pythonic exception that may have occurred
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/market/analyst_performance.py method: get_price_stability_index_vlyr',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


def get_tiles(filter_analyst_p_scr):
    """

    :param filter_anal_scr:
    :return:
    """
    from jupiter_AI.tiles.competitor_analysis.dashboard import get_price_intelligence_quotient
    from jupiter_AI.tiles.price_biometrics.price_elasticity import pe_signal
    response = dict()
    response['price_stability_index_vlyr'] = get_price_stability_index_vlyr(filter_analyst_p_scr)
    response['price_intelligence_quotient'] = get_price_intelligence_quotient(filter_analyst_p_scr)
    response['pe_signal'] = pe_signal(filter_analyst_p_scr)
    return response

if __name__ == '__main__':
    a = {
	"origin": ["DXB"],
	"toDate": "2017-02-28",
	"compartment": ["Y"],
	"fromDate": "2017-02-01",
	"country": ["AE"],
	"region": ["MiddleEast"],
	"destination": ["DOH"],
	"pos": ["DXB"]}
    print get_tiles(a)
