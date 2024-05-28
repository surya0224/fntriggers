"""
File Name              :   significant_non_significant_od.py
Author                 :   Sai Krishna
Date Created           :   2016-12-19
Description            :   module with methods to calculate tiles for significant and non significant screen
                           in KPI dashboard

MODIFICATIONS LOG
    S.No                   : 1
    Date Modified          : 2017-02-15
    By                     : Sai Krishna
    Modification Details   :
        1   Optimizing the code
                Scrapping of two different functions for getting the data for significant non significant screen and
                KPI dashboard
                instead created a universal individual function for getting significant data
        2   Consideration of Sales Flown Collection if toDate in filter is less than today(system date)
                Otherwise using Sales Flown
        3   Creation of Common functions for generation of pipelines,building query,getting data from any of the
            collections.
        4   Option to Consider the significant and non significance of OD on teh basis of 'pax' also.
            By default 'revenue' is considered.
        5   Change in the way Booming and Fading ODs are obtained.
            Earlier we were taking pax_1 and revenue_base_1 values.
            Now we are considering the entire flown data for the same departure range last year.
            The departure range is obtained by doing the following
                last_year_date = current_date - 364
        6   Comments Done
        7   Error Handling Done
    S.No                   : 2
    Date Modified          : 2017-02-15
    By                     : Sai Krishna
    Modification Details   :
        changed a loop over a cursor to obtain the data in list format to list(cursor) in line 220
"""

import datetime
import inspect
import json
from collections import defaultdict
from copy import deepcopy

from jupiter_AI.common import ClassErrorObject as error_class
from jupiter_AI import client, JUPITER_DB,SYSTEM_DATE
db = client[JUPITER_DB]
from jupiter_AI.tiles.jup_common_functions_tiles import gen_collection_name


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


def build_query_sales_or_flown(filter_sig_od_scr):
    """
    Function to query either the Sales or Flown Collection on the basis of filter.
    Since the structure of both JUP_DB_Sales and JUP_DB_Sales_Flown are same common function is being used
    This function mainly transforms the filter obtained into a dict that can be queried according to the
    structure of respective collections
    :param filter_sig_od_scr: dict representing the filter on screen
    :returns a dict which is utilized as  query in '$match' stage of pipeline
    """
    filter_sig_od_scr = deepcopy(defaultdict(list, filter_sig_od_scr))
    dict_qry_response = defaultdict(list)
    if filter_sig_od_scr['region']:
        dict_qry_response['$and'].append({'region': {'$in': filter_sig_od_scr['region']}})

    if filter_sig_od_scr['country']:
        dict_qry_response['$and'].append({'country': {'$in': filter_sig_od_scr['country']}})

    if filter_sig_od_scr['pos']:
        dict_qry_response['$and'].append({'pos': {'$in': filter_sig_od_scr['pos']}})

    if filter_sig_od_scr['origin']:
        od_build = []
        for index, origin in enumerate(filter_sig_od_scr['origin']):
            od_build.append({'origin': origin, 'destination': filter_sig_od_scr['destination'][index]})
        dict_qry_response['$and'].append({'$or': od_build})

    if filter_sig_od_scr['compartment']:
        dict_qry_response['$and'].append({'compartment': {'$in': filter_sig_od_scr['compartment']}})

    dict_qry_response['$and'].append({'dep_date': {'$gte': filter_sig_od_scr['fromDate'],
                                                   '$lte': filter_sig_od_scr['toDate']}})

    return dict(dict_qry_response)


def gen_ppln_sales_or_flown(dict_qry_sales_or_flown, temp_collection, factor='revenue'):
    """
    Function generating a common pipeline  for either of JUP_DB_Sales and JUP_DB_Sales_Flown collections
    considering the parameters provided
    Arguments
    :dict_qry_sales_or_flown: filtering the collection with this query
    :factor: which factor to be considered.'revenue' by default but 'pax' can also be provided
    :temp_collection: temp collection name where data would be stored
    :returns: a pipeline(a list of dicts(aggregation stages) to be called in aggregate function
    """
    #   factor 'pax' represents the data from column 'pax' in collections
    #   factor 'revenue' represents the data from column 'revenue_base' in collections
    #   'revenue_base' is considered as it represents revenue in AED(host currency)
    if factor == 'pax':
        parameter = 'pax'
    else:
        parameter = 'revenue_base'
    ppln_sales_or_flown = [
        {
            '$match': dict_qry_sales_or_flown
        }
        #   for factor='revenue' the below group turs out to be
        #     {
        #         '$group':
        #             {
        #                 '_id': '$od',
        #                 'revenue': {'$sum': '$revenue_base'}
        #             }
        #     }
        #   So to have a common pipeline for both factors this style has been adopted
        ,
        #   Grouping at OD level
        {
            '$group':
                {
                    '_id': '$od',
                    factor: {'$sum': '$' + parameter}
                }
        }
        ,
        #   Storing the OD level values in od_level_data key and Calculating Totals
        {
            '$group':
                {
                    '_id': None,
                    'od_level_data':
                        {
                            '$push':
                                {
                                    'od': '$_id',
                                    factor: '$' + factor
                                }
                        },
                    'total': {'$sum': '$' + factor}
                }
        }
        ,
        {
            '$unwind': '$od_level_data'
        }
        ,
        #   Here the meaning of 'total' changes with the factor under consideration,
        #   for factor='revenue' it is total revenue in AED
        #   for factor='pax' it is total pax
        {
            '$project':
                {
                    '_id': 0,
                    'od': '$od_level_data.od',
                    factor: '$od_level_data.' + factor,
                    'total': '$total'
                }
        }
        ,
        #   Descending order Sorting
        {
            '$sort': {factor: -1}
        }
        ,
        {
            '$out': temp_collection
        }
    ]

    return ppln_sales_or_flown


def get_od_level_data(dict_qry_sales_or_flown, collection_name='JUP_DB_Sales', factor='revenue'):
    """
    Function collects the data from relevant collection('collection_name')
    considering the factor('factor') by using the query('dict_qry_sales_or_flown') provided
    Arguments:
    :collection_name: by default will consider JUP_DB_Sales to fetch the data but considers 'JUP_DB_Sales_Flown'
                            when provided
    :dict_qry_sales:  filtering the collection with this query
    :factor: which factor to be considered.'revenue' by default but 'pax' can also be provided
    :returns the data obtained from dB as a list of dictioneries
             the data is at OD level.
    """
    temp_collection = gen_collection_name()
    if collection_name == 'JUP_DB_Sales' or collection_name == 'JUP_DB_Sales_Flown':
        if collection_name in db.collection_names():
            #   Pipeline Generation
            ppln_od_level_data = gen_ppln_sales_or_flown(dict_qry_sales_or_flown=dict_qry_sales_or_flown,
                                                         temp_collection=temp_collection,
                                                         factor=factor)
            dict_od_level_data = []
            #   Obtaining data from the relevant collection
            if collection_name == 'JUP_DB_Sales':
                db.JUP_DB_Sales.aggregate(ppln_od_level_data, allowDiskUse=True)
            elif collection_name == 'JUP_DB_Sales_Flown':
                db.JUP_DB_Sales_Flown.aggregate(ppln_od_level_data, allowDiskUse=True)

            #   Check if the temp_collection with aggregated has been created or not
            if temp_collection in db.collection_names():
                od_level_crsr = db[temp_collection].find({}, {'od': 1,
                                                              factor: 1,
                                                              'total': 1})
                dict_od_level_data = list(od_level_crsr)
                db[temp_collection].drop()
            else:
                no_temp_collection_err = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                            'jupter_AI/tiles/kpi/significant_non_significant_od.py method: get_od_level_data',
                                                            get_arg_lists(inspect.currentframe()))
                no_temp_collection_err_desc = ''.join(['collection_name - ', temp_collection,
                                                       ' Not Created in Database'])
                no_temp_collection_err.append_to_error_list(no_temp_collection_err_desc)
                no_temp_collection_err.write_error_logs(datetime.datetime.now())
            return dict_od_level_data
        else:
            no_collection_err = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                            'jupter_AI/tiles/kpi/significant_non_significant_od.py method: get_od_level_data',
                                                            get_arg_lists(inspect.currentframe()))
            no_collection_err_desc = ''.join(['collection_name - ', str(collection_name),
                                              ' Not Present in Database'])
            no_collection_err.append_to_error_list(no_collection_err_desc)
            no_collection_err.write_error_logs(datetime.datetime.now())
    else:
        incorrect_input_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                            'jupter_AI/tiles/kpi/significant_non_significant_od.py method: get_od_level_data',
                                            get_arg_lists(inspect.currentframe()))
        incorrect_input_error_desc = ''.join(['Incorrect Input for collection_name - '
                                              'Expected JUP_DB_Sales or JUP_DB_Sales_Flown ',
                                              'but obtained ',str(collection_name)])
        incorrect_input_error.append_to_error_list(incorrect_input_error_desc)
        incorrect_input_error.write_error_logs(datetime.datetime.now())


def get_significant_ods_data(filter_sig_od_scr, factor='revenue'):
    """
    Main Function to obtain the Significant Non Significant ODs data
    Arguments:
    filter_sig_od_scr(dict): the filter from UI in dict format
    factor(str): which parameter to consider for calculating significance of ODs. By default it is 'revenue'
                 but can also have a value of 'pax'
    returns the list of significant, non significant and total ODs in the filter considered as keys of a dictionery
    """
    #   Null Assumptions of return parameters
    sig_ods = []
    non_sig_ods = []
    total_ods = []

    #   for the total_revenue or total_pax whichever in consideration
    total_factor = 0

    #   Obtaining the Query
    dict_qry_flown_or_sales = build_query_sales_or_flown(filter_sig_od_scr)

    #   Identification of which collection to call with and calling the collection with factor to be considered as
    #   input if required
    if filter_sig_od_scr['toDate'] < SYSTEM_DATE:
        if factor == 'pax':
            od_level_data = get_od_level_data(dict_qry_sales_or_flown=dict_qry_flown_or_sales,
                                              collection_name='JUP_DB_Sales_Flown',
                                              factor='pax')
        else:
            od_level_data = get_od_level_data(dict_qry_sales_or_flown=dict_qry_flown_or_sales,
                                              collection_name='JUP_DB_Sales_Flown',
                                              factor='revenue')
    else:
        if factor == 'pax':
            od_level_data = get_od_level_data(dict_qry_sales_or_flown=dict_qry_flown_or_sales,
                                              factor='pax')
        else:
            od_level_data = get_od_level_data(dict_qry_sales_or_flown=dict_qry_flown_or_sales)

    #  Obtaining the significant, Non significant and Total Ods if data is available
    if len(od_level_data) > 0:
        interim_factor_value = 0
        for od_data in od_level_data:
            total_ods.append(od_data['od'])
            total_factor = od_data['total']
            if interim_factor_value < 0.8 * od_data['total']:
                sig_ods.append(od_data['od'])
                interim_factor_value += od_data[factor]
            else:
                non_sig_ods.append(od_data['od'])
    else:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                            'jupter_AI/tiles/kpi/significant_non_significant_od.py method: get_significant_ods_data',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list('No Data Obtained for the Filter')
        obj_error.write_error_logs(datetime.datetime.now())

    #   The usage is 'total_'+factor is to generate relevant keys for different factors
    #   i.e. 'total_revenue' for 'revenue' and 'total_pax' for 'pax'
    return {
        'significant' : sig_ods,
        'non_significant': non_sig_ods,
        'total': total_ods,
        'total_'+factor: total_factor
    }


def get_tiles(filter_sig_od_scr):
    """
    Obtaining the Tiles values for Significant and Non Significant OD Screen
    Arguments:
    :filter_sig_od_scr: filter on teh screen in dict format
    :returns all the tiles for significant_non_significant_od screens as a dictionery
    """
    try:
        response_to_UI = dict()

        #   First Defining the User Level Filter to obtain the Data
        #   This is a duplicate of 'filter_sig_od_scr'
        user_level_filter = deepcopy(filter_sig_od_scr)
        user_level_response = get_significant_ods_data(filter_sig_od_scr=user_level_filter)
        user_sig_ods = user_level_response['significant']
        response_to_UI['user_significant_od'] = len(user_sig_ods)
        user_non_sig_ods = user_level_response['non_significant']
        response_to_UI['user_non_significant_od'] = len(user_non_sig_ods)
        response_to_UI['tot_ods_user'] = len(user_level_response['total'])
        response_to_UI['total_revenue_user'] = user_level_response['total_revenue']

        #   Since host represents neglecting all the filters except date filters.
        #   Just assigning only the date filters adn obtaining the data
        host_level_filter = {
            'fromDate': filter_sig_od_scr['fromDate'],
            'toDate': filter_sig_od_scr['toDate']
        }
        host_level_response = get_significant_ods_data(filter_sig_od_scr=host_level_filter)
        response_to_UI['host_significant_od'] = len(host_level_response['significant'])
        response_to_UI['host_non_significant_od'] = len(host_level_response['non_significant'])
        response_to_UI['tot_ods_host'] = len(host_level_response['total'])
        response_to_UI['total_revenue_host'] = host_level_response['total_revenue']

        #   For Booming and Fading OD Calculations, duplicating the user level filter for last year by change only
        #   in the 'fromDate' and 'toDate'
        #   The dates are obtained by subtracting 364 days from currenct date
        last_year_filter = deepcopy(filter_sig_od_scr)
        obj_fromDate_ly = datetime.datetime.strptime(filter_sig_od_scr['fromDate'],
                                                     '%Y-%m-%d') + datetime.timedelta(days=-364)
        fromDate_ly = datetime.datetime.strftime(obj_fromDate_ly,'%Y-%m-%d')
        obj_toDate_ly = datetime.datetime.strptime(filter_sig_od_scr['toDate'],
                                                   '%Y-%m-%d') + datetime.timedelta(days=-364)
        toDate_ly = datetime.datetime.strftime(obj_toDate_ly, '%Y-%m-%d')

        last_year_filter['fromDate'] = fromDate_ly
        last_year_filter['toDate'] = toDate_ly

        last_year_response = get_significant_ods_data(last_year_filter)

        if last_year_response['significant']:
            #   Booming OD - significant this year but not last year
            booming_ods = set(user_sig_ods).difference(last_year_response['significant'])
            response_to_UI['booming_od_user'] = len(booming_ods)
        else:
            response_to_UI['booming_od_user'] = 'NA'

        if last_year_response['non_significant']:
            #   Fading OD - non significant this year but not non significant last year
            fading_ods = set(user_non_sig_ods).difference(last_year_response['non_significant'])
            response_to_UI['fading_od_user'] = len(fading_ods)
        else:
            response_to_UI['fading_od_user'] = 'NA'

        return response_to_UI
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                            'jupter_AI/tiles/kpi/significant_non_significant_od.py method: get_tiles',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


def get_significant_non_significant_ods_user(filter_kpi_dhb_scr):
    """
    Function to get the tile valus for Significant Non Significant OD in KPI dashboard
    :param filter_kpi_dhb_scr:
    :return:
    """
    try:
        response = dict()
        significant_od_data = get_significant_ods_data(filter_sig_od_scr=filter_kpi_dhb_scr)

        response['significant_ods'] = len(significant_od_data['significant'])
        response['non_significant_ods'] = len(significant_od_data['non_significant'])

        return response
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                            'jupter_AI/tiles/kpi/significant_non_significant_od.py method: get_significant_non_significant_ods_user',
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
        'fromDate': '2017-02-15',
        'toDate': '2017-02-15'
    }
    print json.dumps(get_tiles(a), indent=1)
    print get_significant_non_significant_ods_user(a)
    print time.time() - st