"""
File Name              :   dashboard.py
Author                 :   Sai Krishna
Date Created           :   2016-12-19
Description            :   module with methods to calculate tiles for competitor analysis dashboard
MODIFICATIONS LOG
    S.No               : 1
    Date Modified      : 2017-10-02
    By                 : Shamail Mulla
    Modification Details   : Code optimisation
"""

import datetime
import inspect
import time
from collections import defaultdict
from copy import deepcopy

from jupiter_AI.RnA.Distributor_Agent_Rev_Mgmt_Fare_Availability_RnA import get_theoretical_maximum_num_fares
from jupiter_AI.common import ClassErrorObject as error_class
from jupiter_AI import client,JUPITER_DB, na_value
db = client[JUPITER_DB]
from jupiter_AI.network_level_params import SYSTEM_DATE
from jupiter_AI.tiles.jup_common_functions_tiles import gen_collection_name as gen

result_coll_name = gen()


def get_module_name():
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


def build_query_fares_col(filter_competitor_dhb_scr):
    '''

    :param filter_competitor_dhb_scr:
    :return:
    '''
    qry_fares = defaultdict(list)
    if filter_competitor_dhb_scr['region']:
        qry_fares['$and'].append({'region': {'$in': filter_competitor_dhb_scr['region']}})
    if filter_competitor_dhb_scr['country']:
        qry_fares['$and'].append({'country': {'$in': filter_competitor_dhb_scr['country']}})
    if filter_competitor_dhb_scr['pos']:
        qry_fares['$and'].append({'pos': {'$in': filter_competitor_dhb_scr['pos']}})
    if filter_competitor_dhb_scr['origin']:
        od_build = []
        for index, origin in enumerate(filter_competitor_dhb_scr['origin']):
            od_build.append({'origin': origin, 'destination': filter_competitor_dhb_scr['destination'][index]})
        qry_fares['$and'].append({'$or': od_build})
    if filter_competitor_dhb_scr['compartment']:
        qry_fares['$and'].append({'compartment': {'$in': filter_competitor_dhb_scr['compartment']}})
    qry_fares['$and'].append({'effective_from': {'$lte': SYSTEM_DATE}})
    qry_fares['$and'].append({'effective_to': {'$gte': SYSTEM_DATE}})
    # qry_fares['$and'].append({'dep_date': {'$lte': filter_competitor_dhb_scr['toDate'],
    #                                        '$gte': filter_competitor_dhb_scr['fromDate']}})
    return qry_fares


def get_price_intelligence_quotient(afilter):
    '''

    :param afilter:
    :return:
    '''
    try:
        response = 'NA'
        if 'JUP_DB_ATPCO_Fares' in db.collection_names():
            filter_competitor_dhb_scr = deepcopy(defaultdict(list, afilter))
            qry_fares = build_query_fares_col(filter_competitor_dhb_scr)
            response = dict()
            ppln_fares = [
                {
                    '$match': qry_fares
                }
                ,
                {
                    '$group':
                        {
                            '_id':
                                {
                                    'pos': '$pos',
                                    'origin': '$origin',
                                    'destination': '$destination',
                                    'compartment': '$compartment'
                                }
                            ,
                            'num_fares': {'$sum': 1}
                        }
                }
                ,
                {
                    '$project':
                        {
                            '_id': 0,
                            'pos': '$_id.pos',
                            'origin': '$_id.origin',
                            'destination': '$_id.destination',
                            'compartment': '$_id.compartment',
                            'num_fares': '$num_fares',
                            'theoretical_maximum':
                                {
                                    '$cond':
                                        {
                                            'if': {'$eq': ['$_id.compartment', 'Y']},
                                            'then': get_theoretical_maximum_num_fares('Y'),
                                            'else':
                                                {
                                                    '$cond':
                                                        {
                                                            'if': {'$eq': ['$_id.compartment', 'J']},
                                                            'then': get_theoretical_maximum_num_fares('J'),
                                                            'else':
                                                                {
                                                                    '$cond':
                                                                        {
                                                                            'if': {'$or': [{'$eq': ['$_id.compartment', 'A']},
                                                                                           {'$eq': ['$_id.compartment', 'F']}]},
                                                                            'then': get_theoretical_maximum_num_fares('F'),
                                                                            'else': 0
                                                                        }
                                                                }
                                                        }
                                                }
                                        }
                                }
                        }
                }
                ,
                {
                    '$group':
                        {
                            '_id': None,
                            'num_fares': {'$sum': '$num_fares'},
                            'theoretical_maximum': {'$sum': '$theoretical_maximum'}
                        }
                }
                ,
                {
                    '$project':
                        {
                            '_id':0,
                            'availability':
                                {
                                    '$cond':
                                        {
                                            'if': {'$ne': ['$theoretical_maximum', 0]},
                                            'then': {'$divide': ['$num_fares', '$theoretical_maximum']},
                                            'else': na_value
                                        }
                                }
                        }
                }
                ,
                {
                    '$out': result_coll_name
                }
            ]
            db.JUP_DB_ATPCO_Fares.aggregate(ppln_fares)
            if result_coll_name in db.collection_names():
                piq = db.get_collection(result_coll_name)
                if piq.count() == 0: # pipeline has returned an empty function
                    piq.drop()
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                        'jupter_AI/tiles/competitor_analysis/price_intelligence_quotient.py method: get_price_intelligence_quotient',
                                                        get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list('There is no data.')
                    obj_error.write_error_logs(datetime.datetime.now())
                elif piq.count() == 1:
                    lst_fares_data = list(piq.find(projection={'availability': 1, '_id': 0}))
                    price_intelligence_quotient = round(lst_fares_data[0][u'availability'], 3)
                    piq.drop()
                    response = price_intelligence_quotient
                else: # something went wrong in the pipeline and we got multiple results
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                        'jupter_AI/tiles/competitor_analysis/price_intelligence_quotient.py method: get_price_intelligence_quotient',
                                                        get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list('Expected 1 document from database but got ',piq.count())
                    piq.drop()
                    obj_error.write_error_logs(datetime.datetime.now())
            else: # in case the result collection does not get created
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupter_AI/tiles/competitor_analysis/price_intelligence_quotient.py method: get_price_intelligence_quotient',
                                                    get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list('Market share data result not created in the database. Check aggregate pipeline.')

        else: # if collection to query is not present in database
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupter_AI/tiles/competitor_analysis/price_intelligence_quotient.py method: get_price_intelligence_quotient',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_ATPCO_Fares cannot be found in the database.')
            obj_error.write_error_logs(datetime.datetime.now())
        return response
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/competitor_analysis/price_intelligence_quotient.py method: get_price_intelligence_quotient',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())

if __name__ == '__main__':
    a = {
        'region': ["MiddleEast"],
        'country': ["AE"],
        'pos': ["DXB"],
        'origin': ["DXB"],
        'destination': ["DOH"],
        "compartment": ["Y"],
        'fromDate': '2018-02-14',
        'toDate': '2018-02-20'
    }
    start_time = time.time()
    print get_price_intelligence_quotient(a)
    print (time.time() - start_time)