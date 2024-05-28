"""
File Name              :	Distributor_Agent_Rev_Mgmt_Fare_Availability_RnA
Author                 :	K Sai Krishna
Date Created           :	2016-12-14
Description            :	RnA analysis for reviewing availability for pos,od,compartment
                            Revenue management perspective
                            combinations
Status                 :

MODIFICATIONS LOG	       :
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :
"""
import inspect
import json
from datetime import date

import jupiter_AI.common.ClassErrorObject as errorClass
from jupiter_AI import client, JUPITER_DB, na_value
db = client[JUPITER_DB]

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


def build_query_fares_col(dict_scr_filter):
    """
    Build the query for filtering fares from ATPCO Fares Collection according to the filter
    :param dict_scr_filter:
    :return:
    """
    qry_fares = dict()
    qry_fares['$and'] = []
    today = str(date.today())
    if dict_scr_filter['region']:
        qry_fares['$and'].append({'region': {'$in': dict_scr_filter['region']}})
    if dict_scr_filter['country']:
        qry_fares['$and'].append({'country': {'$in': dict_scr_filter['country']}})
    if dict_scr_filter['pos']:
        qry_fares['$and'].append({'pos': {'$in': dict_scr_filter['pos']}})
    if dict_scr_filter['compartment']:
        qry_fares['$and'].append({'compartment': {'$in': dict_scr_filter['compartment']}})
    if dict_scr_filter['origin']:
        od_build = []
        for idx, item in enumerate(dict_scr_filter['origin']):
            od_build.append({'origin': item,
                             'destination': dict_scr_filter['destination'][idx]})
            qry_fares['$and'].append({'$or': od_build})

    qry_fares['$and'].append({'effective_from': {'$lte': today}})
    qry_fares['$and'].append({'effective_to': {'$gte': today}})

    # qry_fares['dep_date'].append({'dep_date': {'$gte': dict_scr_filter['fromDate'],
    #                                            '$lte': dict_scr_filter['toDate']}})
    return qry_fares


def get_theoretical_maximum_num_fares(compartment):
    """
    Returns the exhaustive list of channels for the airline/system
    :return:
    """
    channels = 6
    trip_type = 2
    promotion_fares = 2
    if compartment == 'Y':
        rbds = 8
        fare_brands = 6
    elif compartment == 'J':
        rbds = 3
        fare_brands = 2
    elif compartment == 'F' or compartment == 'A':
        rbds = 3
        fare_brands = 1
    num_fares = rbds * trip_type * promotion_fares * fare_brands * channels
    return num_fares


def get_fares_related_data(dict_scr_filter):
    """
    Build the appropriate pipeline and hit the dB to get the relevant data
    for building RnA
    :param dict_scr_filter:
    :return:
    """
    #   building the query to filter data
    qry_fares = build_query_fares_col(dict_scr_filter)

    #   Creation of the pipeline for the mongodB aggregate
    ppln_for_fares = [
        {
            '$match': qry_fares
        }
        ,
        {
            '$group': {
                '_id': {
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
            '$project': {
                '_id': 0,
                'pos': '$_id.pos',
                'origin': '$_id.origin',
                'destination': '$_id.destination',
                'compartment': '$_id.compartment',
                'availability': {
                    '$cond': {
                        'if': {'$eq': ['$_id.compartment', 'Y']},
                        'then': {'$divide': ['$num_fares', get_theoretical_maximum_num_fares('Y')]},
                        'else': {
                            '$cond': {
                                'if': {'$eq': ['$_id.compartment', 'J']},
                                'then': {'$divide': ['$num_fares', get_theoretical_maximum_num_fares('J')]},
                                'else': {
                                    '$cond': {
                                        'if': {'$or': [{'$eq': ['$_id.compartment', 'A']},
                                                       {'$eq': ['$_id.compartment', 'F']}]},
                                        'then': {'$divide': ['$num_fares', get_theoretical_maximum_num_fares('F')]},
                                        'else': na_value
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        {
            '$project': {
                'pos': '$pos',
                'origin': '$origin',
                'destination': '$destination',
                'compartment': '$compartment',
                'availability': {
                    '$divide': [
                        {
                            '$subtract': [
                                {
                                    '$multiply': ['$availability', 10000],
                                }
                                ,
                                {
                                    '$mod': [{'$multiply': ['$availability', 10000]}, 1]}]
                                }
                        ,
                        10000
                            ]
                        }
                }
        }
    ]

    #   Getting the response from JUP_DB_Host_Pricing_Data collection for the above aggregate pipeline
    fares_cursor = db.JUP_DB_ATPCO_Fares.aggregate(ppln_for_fares)

    #   Converting the mongo dB cursor into a list
    fares_data = list(fares_cursor)
    return fares_data


def analyze_availability(availability_val):
    threshold = 0.01
    if availability_val < threshold:
        return True
    else:
        return False


def gen_rev_mgmt_availability_rna(dict_scr_filter):
    """
    Function takes the filter as input and calls relevant function to get data from dB
    Then based on the response from dB
        Outputs a dictionery with 4 keys(what,why,status_quo,action) to be represented as RnA on screen
    :param dict_scr_filter:
    :return:
    """
    rna_response = dict()
    fares_data = get_fares_related_data(dict_scr_filter)
    #   Error handling of r no response from dB
    if len(fares_data) == 0:
        no_data_error = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                               get_module_name(),
                                               get_arg_lists(inspect.currentframe()))
        desc = 'No response from db for the filter provided'
        no_data_error.append_to_error_list(desc)
        raise no_data_error
    #   only 1 pos od compartment in filter
    elif len(fares_data) == 1:
        if analyze_availability(availability_val=fares_data[0]['availability']):
            what = 'Revenue Management'
            why = ''.join(['Fare availability of this compartment(', str(fares_data[0]['availability']),
                           ') is below the defined threshold'])
            status_quo = '***'
            action = '***'
            rna_response['what'] = what
            rna_response['why'] = why
            rna_response['status quo'] = status_quo
            rna_response['action'] = action
            return rna_response

    #   More than 1 pos,od,compartment combinations in the filter
    elif len(fares_data) > 1:
        total = 0
        affected = 0
        for doc in fares_data:
            total += 1
            if analyze_availability(availability_val=doc['availability']):
                affected += 1
        what = 'Revenue Management'
        why = ''.join(['Price Availability is lower than threshold for ',
                       str(affected),' out of ', str(total),
                       ' pos,od,compartment combinations selected in the filter'])
        status_quo = '***'
        action = ''.join(['***'])
        rna_response['what'] = what
        rna_response['why'] = why
        rna_response['status quo'] = status_quo
        rna_response['action'] = action
        return rna_response


def rev_mgmt_availability_rna_main(dict_scr_filter):
    return json.dumps(gen_rev_mgmt_availability_rna(dict_scr_filter), indent=1)

if __name__ == '__main__':
    a = {
            'region': [],
            'country': [],
            'pos': [],
            'origin': [],
            'destination': [],
            'compartment': [],
            'fromDate': '2016-07-01',
            'toDate': '2016-12-31'
        }
    k = rev_mgmt_availability_rna_main(a)
    print k