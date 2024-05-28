"""

"""
import inspect
try:
    from jupiter_AI import client,JUPITER_DB, Host_Airline_Code as host
    #db = client[JUPITER_DB]
except:
    pass
from jupiter_AI.common import ClassErrorObject as error_class
import datetime

def get_quarter(month):
	"""
	"""
	return ((month-1) // 3) + 1


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


def get_str(month):
    if month < 10:
        return '0' + str(month)
    else:
        return str(month)


def validate_data(dict_user_filter):
    """
    If the filter values is valid
    :param dict_user_filter: Dictionary having parameters from the user
    :return: For a valid filter, returns true else false
    """
    try:
        data_validated = True

        if dict_user_filter is None:
            data_validated = False
        # print data_validated
        # More validations can be added later on
        return data_validated
    except:
        pass


def find_df_ods(origin, destination, db):
    crsr_ods = db['JUP_DB_Market_Share'].find({'origin': origin, 'destination': destination})
    if crsr_ods.count()>0:
        return origin, destination
    else: # if origin or destination does not exist in the collection
        # check if origin is present at city or country or region level. if neither, take network value.
        if db['JUP_DB_Capacity_Derived_Factor'].find({'assigned_origin': origin}).count() < 1:
            lst_origin = list(db['JUP_DB_Region_Master'].find({'POS_CD': origin}, {"COUNTRY_CD": 1, "Region": 1}))
            for doc in lst_origin:
                crsr_country = db['JUP_DB_Capacity_Derived_Factor'].find({'assigned_origin': doc[u'COUNTRY_CD']},
                                                                         {"assigned_origin": 1})
                if crsr_country.count() > 0:
                    lst_country = list(crsr_country)
                    origin = lst_country[0][u'assigned_origin']
                else:
                    crsr_region = db['JUP_DB_Capacity_Derived_Factor'].find({'assigned_origin': doc[u'Region']},
                                                                            {"assigned_origin": 1})
                    if crsr_region.count() > 0:
                        lst_region = list(crsr_region)
                        origin = lst_region[0][u'assigned_origin']
                    else:
                        origin = 'NETWORK'

        # check if destination is present at city or country or region level. if neither, take network value.
        if db['JUP_DB_Capacity_Derived_Factor'].find({'assigned_destination': destination}).count() < 1:
            lst_destination = list(
                db['JUP_DB_Region_Master'].find({'POS_CD': destination}, {"COUNTRY_CD": 1, "Region": 1}))
            for doc in lst_destination:
                crsr_country = db['JUP_DB_Capacity_Derived_Factor'].find({'assigned_destination': doc[u'COUNTRY_CD']},
                                                                         {"assigned_destination": 1})
                if crsr_country.count() > 0:
                    lst_country = list(crsr_country)
                    destination = lst_country[0][u'assigned_destination']
                else:
                    crsr_region = db['JUP_DB_Capacity_Derived_Factor'].find({'assigned_destination': doc[u'Region']},
                                                                            {"assigned_destination": 1})
                    if crsr_region.count() > 0:
                        lst_region = list(crsr_region)
                        destination = lst_region[0][u'assigned_destination']
                    else:
                        destination = 'NETWORK'

        return origin, destination


def query_builder_df(dict_user_filter, db):
    """
    The function creates a query the database for derived factor for od combination/s
    :param dict_user_filter: filter to retrieve marginal revenue for a particular od and date range
    :return: query dictionary
    """
    from copy import deepcopy
    from collections import defaultdict

    try:
        query = defaultdict(list)
        # if validate_data(dict_user_filter):
            # print 'query_builder_df'
        dict_filter = deepcopy(defaultdict(list, dict_user_filter))
        # print 'Creating query...'

        # Creating O&D combinations from input filter
        if dict_filter['od']:
            origin, destination = find_df_ods(origin=dict_filter['od'][:3],
                                              destination=dict_filter['od'][3:],
                                              db=db)
            query['$and'].append({'assigned_origin': origin})
            query['$and'].append({'assigned_destination': destination})

        # Retrieving unique quarters for each month year combination
        if dict_filter['quarter']:
            query['$and'].append({'quarter': dict_filter['quarter']})

        if dict_filter['year']:
            query['$and'].append({'year': dict_filter['year']})

        if dict_filter['airline']:
            query['$and'].append({'airline': dict_filter['airline']})

        # else:
        #     print 'Invalid Query'
        # print query
        return dict(query)
    except Exception as error_msg:
        import traceback
        print traceback.print_exc()
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/batch/Host_OD_Capacity.py method: query_builder_df',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


def get_od_derived_factors(dict_filter, db):
    """
    This function retrieves legs of all ODs
    :param dict_filter: filter to retrieve marginal revenue for a particular od and date range
    :return: pair of legs of each OD
    """
    try:
        if 'JUP_DB_Capacity_Derived_Factor' in db.collection_names():
            # print 'get_od_derived_factors'
            query_der_fact = query_builder_df(dict_filter, db=db)
            # print query_der_fact
            ppln_od_df = [
                {
                    '$match': query_der_fact
                }
                ,
                {
                    '$project':
                        {
                            'derived_factor':
                                {
                                    '$cond':
                                        {
                                            'if': {'$eq': ['$user_override_flag', {'$literal': 0}]},
                                            'then': '$derived_factor',
                                            'else': '$user_override'
                                        }
                                },
                            'last_update_date': '$last_update_date'
                        }
                }
                ,
                {
                    '$sort': {'last_update_date': -1}
                }
                ,
                {
                    '$group':
                        {
                            '_id': None,
                            'derived_factor': {'$first': '$derived_factor'}
                        }
                }
            ]
            crsr_der_fac = db.JUP_DB_Capacity_Derived_Factor.aggregate(ppln_od_df, allowDiskUse=True)
            lst_od_der_fac = list(crsr_der_fac)
            if len(lst_od_der_fac) > 0:
                df = lst_od_der_fac[0][u'derived_factor']
            else:
                df = 1
            # print 'OD DERIVED FACTOR: ', str(len(lst_od_der_fac)), 'records'
            return df
        else:
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                                'jupter_AI/batch/Host_OD_Capacity.py method: get_od_derived_factors',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Capacity_Derived_Factor not found in database')
            obj_error.write_error_logs(datetime.datetime.now())
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/batch/Host_OD_Capacity.py method: get_od_derived_factors',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())