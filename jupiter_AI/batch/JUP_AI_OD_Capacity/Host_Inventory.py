"""
File Name              :   Host_Inventory.py
Author                 :   Shamail Mulla
Date Created           :   2017-04-14
Description            :  Host capacity is calculated at daily level using legs.

"""
import traceback
import math
from jupiter_AI.tiles.jup_common_functions_tiles import query_month_year_builder
from copy import deepcopy
from collections import defaultdict
import datetime
import time
import inspect

lst_od_capacity = []
count = 0

try:
    from jupiter_AI import client, JUPITER_DB, JUPITER_LOGGER
    from jupiter_AI.logutils import measure
    db = client[JUPITER_DB]
    # import pymongo
    # db = pymongo.MongoClient('localhost:27017')['fzDB']
    from jupiter_AI import Host_Airline_Code as host, SYSTEM_DATE
except BaseException:
    pass
# from jupiter_AI.common.network_level_params import Host_Airline_Code as host
from jupiter_AI.common import ClassErrorObject as error_class
from jupiter_AI.tiles.jup_common_functions_tiles import gen_collection_name as gen
from jupiter_AI.batch.JUP_AI_OD_Capacity.region_master_derived_factor import *
import pandas as pd
from dateutil.relativedelta import relativedelta


@measure(JUPITER_LOGGER)
def get_quarter(month):
    """
    """
    return ((month - 1) // 3) + 1


@measure(JUPITER_LOGGER)
def get_module_name():
    return inspect.stack()[1][3]


@measure(JUPITER_LOGGER)
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


@measure(JUPITER_LOGGER)
def get_str(month):
    if month < 10:
        return '0' + str(month)
    else:
        return str(month)


@measure(JUPITER_LOGGER)
def query_builder_leg_capacity(dict_user_filter):
    """
    The function creates a query database to retrieve leg level capacities
    :param dict_user_filter: filter to retrieve marginal revenue for a particular od and date range
    :return: query dictionary
    """
    try:
        # print 'query_builder_leg_capacity'
        dict_filter = deepcopy(defaultdict(list, dict_user_filter))
        # print 'Creating query...'
        query = defaultdict(list)

        lst_legs = list()
        if dict_filter['od']:
            for leg in dict_filter['od']:
                if leg != '':
                    lst_legs.append({'od': leg})
            # print 'legs'
            # print lst_legs
            query['$and'].append({'$or': lst_legs})

        if dict_filter['date']:
            query['$and'].append({'dep_date': dict_filter['date']})
        # print query
        query['$and'].append({'sys_snap_date': {"$lt": SYSTEM_DATE}})
        return dict(query)
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(
            error_class.ErrorObject.ERRORLEVEL2,
            'jupter_AI/batch/JUP_AI_OD_Capacity/Host_Inventory.py method: query_builder_leg_capacity',
            get_arg_lists(
                inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


@measure(JUPITER_LOGGER)
def get_leg_capacities(dict_filter):
    """
        This function finds the capacities for legs
        :param dict_filter: filter to retrieve marginal revenue for a particular od and date range
        :return: capacity list
        """
    try:
        coll_name = gen()
        if 'JUP_DB_Inventory_Leg' in db.collection_names():
            # # Query the leg level capacity collection
            print 'JUP_DB_Inventory_Leg collection present.'
            query_legs_capacity = query_builder_leg_capacity(dict_filter)
            ppln_legs_capacity = [
                {
                    '$match': query_legs_capacity
                },
                {
                    '$project':
                        {
                            'dep_date': '$dep_date',
                            'od': '$od',
                            # 'od': {'$concat': ['$leg_origin', '$leg_destination']},
                            # 'flight_num':{'$Flight_Number'},
                            # 'snap_date': '$snap_date',
                            'sys_snap_date': '$sys_snap_date',
                            'capacity': '$total_cap',
                            'j': '$j_cap',
                            'y': '$y_cap',
                            'y_booking': "$y_booking",
                            'j_booking': "$j_booking",
                            'flight_number': "$Flight_Number"
                        }
                },
                {
                    '$group':
                        {
                            '_id':
                                {
                                    'dep_date': '$dep_date',
                                    'od': '$od',
                                    # 'snap_date': '$snap_date',
                                    'flight_number': '$flight_number',
                                    'sys_snap_date': "$sys_snap_date"
                                },
                            'j': {'$sum': '$j'},
                            'y': {'$sum': '$y'},
                            'capacity': {'$sum': '$capacity'},
                            'y_bookings': {"$sum": "$y_booking"},
                            'j_bookings': {"$sum": "$j_booking"}
                        }
                },
                {
                    '$sort': {'_id.sys_snap_date': -1}
                },
                {
                    '$group':
                        {
                            '_id':
                                {
                                    'dep_date': '$_id.dep_date',
                                    'od': '$_id.od',
                                    'flight_number': '$_id.flight_number'
                                },
                            'j': {'$first': '$j'},
                            'y': {'$first': '$y'},
                            'capacity': {'$first': '$capacity'},
                            'y_bookings': {"$first": "$y_bookings"},
                            'j_bookings': {"$first": "$j_bookings"},
                            'snap_date': {"$first": "$_id.sys_snap_date"}
                        }
                },
                {
                    '$group':
                        {
                            '_id':
                                {
                                    'dep_date': '$_id.dep_date',
                                    'od': '$_id.od',
                                },
                            'j': {'$sum': '$j'},
                            'y': {'$sum': '$y'},
                            'capacity': {'$sum': '$capacity'},
                            'y_bookings': {"$sum": "$y_bookings"},
                            'j_bookings': {"$sum": "$j_bookings"},
                            'snap_date': {"$first": "$snap_date"}
                        }
                },
                {
                    '$project':
                        {
                            '_id': 0,
                            'leg': '$_id.od',
                            'dep_date': '$_id.dep_date',
                            'j': '$j',
                            'y': '$y',
                            'capacity': '$capacity',
                            'y_bookings': "$y_bookings",
                            "j_bookings": "$j_bookings",
                            "snap_date": "$snap_date",
                            'od': "$_id.od"
                        }
                },
                {
                    '$out': coll_name
                }
            ]
            ppln_inventory = [
                {
                    '$project':
                        {
                            'dep_date': '$dep_date',
                            'od': '$od',
                            # 'od': {'$concat': ['$leg_origin', '$leg_destination']},
                            # 'flight_num':{'$Flight_Number'},
                            # 'snap_date': '$snap_date',
                            'sys_snap_date': '$sys_snap_date',
                            'capacity': '$total_cap',
                            'j': '$j_cap',
                            'y': '$y_cap',
                            'y_booking': "$y_booking",
                            'j_booking': "$j_booking",
                            'flight_number': "$Flight_Number"
                        }
                },
                {
                    "$group":
                        {
                            "_id":
                                {
                                    "dep_date": "$dep_date",
                                    "snap_date": "$sys_snap_date",
                                    "od": "$od"
                                },
                            "capacity": {"$sum": "$total_cap"},
                            "j": {"$sum": "$j"},
                            "y": {"$sum": "$y"},
                            "y_bookings": {"$sum": "$y_booking"},
                            "j_bookings": {"$sum": "$j_booking"}
                        }

                },
                {
                    "$project":
                        {
                            "od": "$_id.od",
                            "dep_date_1": "$_id.dep_date",
                            "snap_date_1": "$_id.snap_date",
                            "capacity": 1,
                            "j": 1,
                            "y": 1,
                            "y_bookings": 1,
                            "j_bookings": 1
                        }
                }
            ]
            # print 'ppln', ppln_legs_capacity
            db.JUP_DB_Inventory_Leg.aggregate(
                ppln_legs_capacity, allowDiskUse=True)
            lst_legs_capacity = db.get_collection(
                coll_name).find(projection={'_id': 0})
            # lst_legs_capacity = list(crsr_leg_capacities)
            # legs_capacity_df = pd.DataFrame(lst_legs_capacity)
            # inventory_crsr = db.JUP_DB_Inventory_Leg.aggregate(
            #     ppln_inventory, allowDiskUse=True)
            # inventory_df = pd.DataFrame(list(inventory_crsr))
            # legs_capacity_df['dep_date_obj'] = pd.to_datetime(legs_capacity_df['dep_date'], yearfirst=True)
            # legs_capacity_df['snap_date_obj'] = pd.to_datetime(legs_capacity_df['snap_date'], yearfirst=True)
            # legs_capacity_df['dep_date_1'] = legs_capacity_df.dep_date_obj.\
            #     apply(lambda row: (row - relativedelta(years=1)).strftime("%Y-%m-%d"))
            # legs_capacity_df['snap_date_1'] = legs_capacity_df.snap_date_obj.\
            #     apply(lambda row: (row - datetime.timedelta(days=364)).strftime("%Y-%m-%d"))
            #
            # legs_capacity_df = legs_capacity_df.merge(inventory_df,
            #                                           on=['od', 'dep_date_1', 'snap_date_1'],
            #                                           how='left',
            #                                           suffixes=("", "_1"))
            #
            # lst_legs_capacity = legs_capacity_df.to_dict("records")

            # db[coll_name].drop()
            #
            # print len(lst_legs_capacity),'leg capacities'
            leg_dict = defaultdict(dict)
            for data in lst_legs_capacity:
                try:
                    leg_dict[data['leg']]
                except BaseException:
                    leg_dict[data['leg']] = defaultdict(dict)

                leg_dict[data['leg']][data['dep_date']] = data
            db[coll_name].drop()
            return leg_dict
        else:
            obj_error = error_class.ErrorObject(
                error_class.ErrorObject.ERRORLEVEL2,
                'jupiter_AI/batch/JUP_AI_OD_Capacity/Host_Inventory.py method: get_leg_capacities',
                get_arg_lists(
                    inspect.currentframe()))
            obj_error.append_to_error_list(
                'Collection JUP_DB_Inventory_Flight not found in the database')
            obj_error.write_error_logs(datetime.datetime.now())
            print traceback.format_exc()
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(
            error_class.ErrorObject.ERRORLEVEL2,
            'jupter_AI/batch/JUP_AI_OD_Capacity/Host_Inventory.py method: get_leg_capacities',
            get_arg_lists(
                inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())
        print traceback.format_exc()


@measure(JUPITER_LOGGER)
def get_od_legs(dict_filter):
    """
    This function breaks down an OD to leg pairs
    :param dict_filter: filter to retrieve marginal revenue for a particular od and date range
    :return: list of ODs with their legs
    """
    print dict_filter
    try:
        coll_name = gen()
        print 'JUP_DB_Capacity_OD_Master' in db.collection_names()
        if 'JUP_DB_Capacity_OD_Master' in db.collection_names():
            print 'get_od_legs'
            if dict_filter['od']:
                query = {'od': {'$in': dict_filter['od']}}
            else:
                query = dict()
            ppln_od_legs = [
                {
                    '$match': query
                },
                {
                    '$project':
                        {
                            '_id': 0,
                            'od': '$od',
                            'leg1': '$Leg1',
                            'leg2': '$Leg 2'
                        }
                },
                {
                    '$out': coll_name
                }
            ]
            print ppln_od_legs
            db.JUP_DB_Capacity_OD_Master.aggregate(
                ppln_od_legs, allowDiskUse=True)
            crsr_od_legs = db.get_collection(
                coll_name).find(projection={'_id': 0})
            lst_od_legs = list(crsr_od_legs)
            print '\nOD legs:', len(lst_od_legs), 'records.'
            db[coll_name].drop()
            return lst_od_legs
        else:
            obj_error = error_class.ErrorObject(
                error_class.ErrorObject.ERRORLEVEL2,
                'jupter_AI/batch/JUP_AI_OD_Capacity/Host_OD_Capacity.py method: get_od_legs',
                get_arg_lists(
                    inspect.currentframe()))
            obj_error.append_to_error_list(
                'Collection JUP_DB_Capacity_OD_Master not found in database')
            obj_error.write_error_logs(datetime.datetime.now())
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(
            error_class.ErrorObject.ERRORLEVEL2,
            'jupter_AI/batch/JUP_AI_OD_Capacity/Host_OD_Capacity.py method: get_od_legs',
            get_arg_lists(
                inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


@measure(JUPITER_LOGGER)
def find_df_ods(origin, destination):
    try:
        crsr_ods = db['JUP_DB_Market_Share'].find(
            {'origin': origin, 'destination': destination})
        if crsr_ods.count() > 0:
            return origin, destination
        else:  # if origin or destination does not exist in the collection
            # check if origin is present at city or country or region level. if
            # neither, take network value.
            if db['JUP_DB_Capacity_Derived_Factor'].find(
                    {'assigned_origin': origin}).count() < 1:
                lst_origin = list(db['JUP_DB_Region_Master'].find(
                    {'POS_CD': origin}, {"COUNTRY_CD": 1, "Region": 1}))
                for doc in lst_origin:
                    crsr_country = db['JUP_DB_Capacity_Derived_Factor'].find(
                        {'assigned_origin': doc[u'COUNTRY_CD']}, {"assigned_origin": 1})
                    if crsr_country.count() > 0:
                        lst_country = list(crsr_country)
                        origin = lst_country[0][u'assigned_origin']
                    else:
                        crsr_region = db['JUP_DB_Capacity_Derived_Factor'].find(
                            {'assigned_origin': doc[u'Region']}, {"assigned_origin": 1})
                        if crsr_region.count() > 0:
                            lst_region = list(crsr_region)
                            origin = lst_region[0][u'assigned_origin']
                        else:
                            origin = 'NETWORK'

            # check if destination is present at city or country or region
            # level. if neither, take network value.
            if db['JUP_DB_Capacity_Derived_Factor'].find(
                    {'assigned_destination': destination}).count() < 1:
                lst_destination = list(db['JUP_DB_Region_Master'].find(
                    {'POS_CD': destination}, {"COUNTRY_CD": 1, "Region": 1}))
                for doc in lst_destination:
                    crsr_country = db['JUP_DB_Capacity_Derived_Factor'].find(
                        {'assigned_destination': doc[u'COUNTRY_CD']}, {"assigned_destination": 1})
                    if crsr_country.count() > 0:
                        lst_country = list(crsr_country)
                        destination = lst_country[0][u'assigned_destination']
                    else:
                        crsr_region = db['JUP_DB_Capacity_Derived_Factor'].find(
                            {'assigned_destination': doc[u'Region']}, {"assigned_destination": 1})
                        if crsr_region.count() > 0:
                            lst_region = list(crsr_region)
                            destination = lst_region[0][u'assigned_destination']
                        else:
                            destination = 'NETWORK'

            return origin, destination
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(
            error_class.ErrorObject.ERRORLEVEL2,
            'jupter_AI/batch/Host_OD_Capacity.py method: find_df_ods',
            get_arg_lists(
                inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


@measure(JUPITER_LOGGER)
def query_builder_df(dict_user_filter):
    """
    The function creates a query the database for derived factor for od combination/s
    :param dict_user_filter: filter to retrieve marginal revenue for a particular od and date range
    :return: query dictionary
    """
    try:
        query = defaultdict(list)
        # if validate_data(dict_user_filter):
        # print 'query_builder_df'
        dict_filter = deepcopy(defaultdict(list, dict_user_filter))
        # print 'Creating query...'

        # Creating O&D combinations from input filter
        if dict_filter['od']:
            origin, destination = find_df_ods(
                origin=dict_filter['od'][:3], destination=dict_filter['od'][3:])
            query['$and'].append({'assigned_origin': origin})
            query['$and'].append({'assigned_destination': destination})

        # Retrieving unique quarters for each month year combination
        if dict_filter['quarter']:
            query['$and'].append({'quarter': dict_filter['quarter']})

        if dict_filter['year']:
            query['$and'].append({'year': dict_filter['year']})

        query['$and'].append({'airline': host})

        # else:
        #     print 'Invalid Query'
        # print query
        return dict(query)
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(
            error_class.ErrorObject.ERRORLEVEL2,
            'jupter_AI/batch/JUP_AI_OD_Capacity/Host_OD_Capacity.py method: query_builder_df',
            get_arg_lists(
                inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


def get_od_derived_factors(dict_filter):
    """
    This function retrieves legs of all ODs
    :param dict_filter: filter to retrieve marginal revenue for a particular od and date range
    :return: pair of legs of each OD
    """
    try:
        coll_name = gen()
        if 'JUP_DB_Capacity_Derived_Factor' in db.collection_names():
            # print 'get_od_derived_factors'
            query_der_fact = query_builder_df(dict_filter)
            # print query_der_fact
            ppln_od_df = [{'$match': query_der_fact},
                          {'$project': {'derived_factor': {'$cond': {'if': {'$eq': ['$user_override_flag',
                                                                                    {'$literal': 0}]},
                                                                     'then': '$derived_factor',
                                                                     'else': '$user_override'}},
                                        'last_update_date': '$last_update_date'}},
                          {'$sort': {'last_update_date': -1}},
                          {'$group': {'_id': None,
                                      'derived_factor': {'$first': '$derived_factor'}}}]
            crsr_der_fac = db.JUP_DB_Capacity_Derived_Factor.aggregate(
                ppln_od_df, allowDiskUse=True)
            lst_od_der_fac = list(crsr_der_fac)
            if len(lst_od_der_fac) > 0:
                df = lst_od_der_fac[0][u'derived_factor']
            else:
                df = 1
            # print 'OD DERIVED FACTOR: ', str(len(lst_od_der_fac)), 'records'
            return df
        else:
            obj_error = error_class.ErrorObject(
                error_class.ErrorObject.ERRORLEVEL2,
                'jupter_AI/batch/JUP_AI_OD_Capacity/Host_Inventory.py method: get_od_derived_factors',
                get_arg_lists(
                    inspect.currentframe()))
            obj_error.append_to_error_list(
                'Collection JUP_DB_Capacity_Derived_Factor not found in database')
            obj_error.write_error_logs(datetime.datetime.now())
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(
            error_class.ErrorObject.ERRORLEVEL2,
            'jupter_AI/batch/JUP_AI_OD_Capacity/Host_Inventory.py method: get_od_derived_factors',
            get_arg_lists(
                inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


@measure(JUPITER_LOGGER)
def get_od_capacity(dict_filter):
    print "dropping..."
    db.JUP_DB_Host_OD_Capacity.remove()
    print "dropped..."
    count = 0
    try:
        region_country_data = get_region_country_details()
        # derived_Factor_data = get_derived_factor_details()
        import time
        st = time.time()
        print st, 'start time'
        # Step 1: Break down OD to leg level
        lst_od_legs = get_od_legs(dict_filter)
        print time.time() - st, 'getting ods'
        if lst_od_legs:
            # # Getting unique list of legs
            lst_legs = []
            print '\nOD LEGS:', str(len(lst_od_legs)), 'records.'

            for od_leg in lst_od_legs:
                # print od_leg
                if od_leg[u'leg1'] not in lst_legs:
                    lst_legs.append(od_leg[u'leg1'])
                if od_leg[u'leg2'] not in lst_legs and od_leg[u'leg2'] is not None:
                    lst_legs.append(od_leg[u'leg2'])
            print '\nUnique legs:', str(len(lst_legs))
            print time.time() - st, 'getting unique legs'

            # Step 2: Retrieve monthly leg level capacities
            dict_filter['od'] = lst_legs
            # print 'updates dict_filter',dict_filter
            # Capacities for unique legs have been retrieved
            dict_legs_capacity = get_leg_capacities(dict_filter)
            print 'Leg Capacity Dict'
            # print json.dumps(dict_legs_capacity, indent=1)
            # print '\nLeg capacities:', len(lst_legs_capacity), 'records.'
            # print time.time() - st, 'getting leg capacities'
            # Step 3: Find OD capacity using derived factor and leg capacities
            # print 'calculate_OD_capacity'
            client.close()
            lst_od_capacity = []
            for index, od_leg in enumerate(lst_od_legs):
                leg1 = od_leg[u'leg1']
                leg2 = od_leg[u'leg2']

                leg1_dict = dict_legs_capacity[leg1]
                if leg2:
                    leg2_dict = dict_legs_capacity[leg2]

                    for key1, value1 in leg1_dict.iteritems():
                        for key2, value2 in leg2_dict.iteritems():
                            if key1 == key2:
                                month = int(key1[5:7])
                                year = int(key1[:4])
                                dict_od_capacity = dict(
                                    origin=od_leg['od'][:3],
                                    destination=od_leg['od'][3:],
                                    od=od_leg['od'],
                                    dep_date=key1,
                                    month=month,
                                    year=year,
                                    quarter='Q' + str(get_quarter(int(month))),
                                    combine_column=key1 + od_leg['od'],
                                    combine_column_od_month_year=od_leg['od'] + str(month) + str(year)
                                )
                                # dF = get_derived_factor_val(
                                #     carrier=host,
                                #     origin=dict_od_capacity['origin'],
                                #     destination=dict_od_capacity['destination'],
                                #     quarter=dict_od_capacity['quarter'],
                                #     year=dict_od_capacity['year'],
                                #     derived_factor_data=derived_Factor_data,
                                #     region_country_data=region_country_data)
                                dF = 1
                                od_y_cap = 0
                                if value1['y'] > 0:
                                    if value2['y'] > 0:
                                        if dF > 0:
                                            od_y_cap = math.ceil(
                                                min(value1['y'], value2['y']) * dF)
                                        else:
                                            od_y_cap = math.ceil(
                                                min(value1['y'], value2['y']))
                                    elif dF > 0:
                                        od_y_cap = math.ceil(value1['y'] * dF)
                                    else:
                                        od_y_cap = math.ceil(value1['y'])
                                else:
                                    if value2['y'] > 0:
                                        if dF > 0:
                                            od_y_cap = math.ceil(
                                                value2['y'] * dF)
                                        else:
                                            od_y_cap = math.ceil(value2['y'])

                                # od_y_cap_1 = 0
                                # if value1['y_1'] > 0:
                                #     if value2['y_1'] > 0:
                                #         if dF > 0:
                                #             od_y_cap_1 = math.ceil(
                                #                 min(value1['y_1'], value2['y_1']) * dF)
                                #         else:
                                #             od_y_cap_1 = math.ceil(
                                #                 min(value1['y_1'], value2['y_1']))
                                #     elif dF > 0:
                                #         od_y_cap_1 = math.ceil(value1['y_1'] * dF)
                                #     else:
                                #         od_y_cap_1 = math.ceil(value1['y_1'])
                                # else:
                                #     if value2['y_1'] > 0:
                                #         if dF > 0:
                                #             od_y_cap_1 = math.ceil(
                                #                 value2['y_1'] * dF)
                                #         else:
                                #             od_y_cap_1 = math.ceil(value2['y_1'])

                                od_j_cap = 0
                                if value1['j'] > 0:
                                    if value2['j'] > 0:
                                        if dF > 0:
                                            od_j_cap = math.ceil(
                                                min(value1['j'], value2['j']) * dF)
                                        else:
                                            od_j_cap = math.ceil(
                                                min(value1['j'], value2['j']))
                                    elif dF > 0:
                                        od_j_cap = math.ceil(value1['j'] * dF)
                                    else:
                                        od_j_cap = math.ceil(value1['j'])
                                else:
                                    if value2['j'] > 0:
                                        if dF > 0:
                                            od_j_cap = math.ceil(
                                                value2['j'] * dF)
                                        else:
                                            od_j_cap = math.ceil(value2['j'])

                                # od_j_cap_1 = 0
                                # if value1['j_1'] > 0:
                                #     if value2['j_1'] > 0:
                                #         if dF > 0:
                                #             od_j_cap_1 = math.ceil(
                                #                 min(value1['j_1'], value2['j_1']) * dF)
                                #         else:
                                #             od_j_cap_1 = math.ceil(
                                #                 min(value1['j_1'], value2['j_1']))
                                #     elif dF > 0:
                                #         od_j_cap_1 = math.ceil(value1['j_1'] * dF)
                                #     else:
                                #         od_j_cap_1 = math.ceil(value1['j_1'])
                                # else:
                                #     if value2['j_1'] > 0:
                                #         if dF > 0:
                                #             od_j_cap_1 = math.ceil(
                                #                 value2['j_1'] * dF)
                                #         else:
                                #             od_j_cap_1 = math.ceil(value2['j_1'])

                                od_cap = od_y_cap + od_j_cap
                                # od_cap_1 = od_y_cap_1 + od_j_cap_1

                                if od_cap > 0:
                                    dict_od_capacity['leg1_capacity'] = value1['y'] + \
                                        value1['j']
                                    dict_od_capacity['leg1'] = value1['leg']
                                    dict_od_capacity['leg1_y'] = value1['y']
                                    dict_od_capacity['leg1_j'] = value1['j']
                                    dict_od_capacity['leg1_y_bookings'] = value1['y_bookings']
                                    dict_od_capacity['leg1_j_bookings'] = value1['j_bookings']
                                    dict_od_capacity['leg2_capacity'] = value2['y'] + \
                                        value2['j']

                                    dict_od_capacity['leg2'] = value2['leg']
                                    dict_od_capacity['leg2_y'] = value2['y']
                                    dict_od_capacity['leg2_j'] = value2['j']
                                    dict_od_capacity['leg2_y_bookings'] = value2['y_bookings']
                                    dict_od_capacity['leg2_j_bookings'] = value2['j_bookings']

                                    dict_od_capacity['derived_factor'] = dF
                                    dict_od_capacity['y_cap'] = od_y_cap
                                    dict_od_capacity['j_cap'] = od_j_cap
                                    dict_od_capacity['capacity'] = od_cap
                                    dict_od_capacity['od_capacity'] = od_cap
                                    dict_od_capacity['snap_date'] = value1['snap_date']
                                    dict_od_capacity['last_update_date'] = SYSTEM_DATE

                                    # if od_cap_1 > 0:
                                    #     dict_od_capacity['leg1_capacity_1'] = value1['y_1'] + \
                                    #                                         value1['j_1']
                                    #     dict_od_capacity['leg1_y_1'] = value1['y_1']
                                    #     dict_od_capacity['leg1_j_1'] = value1['j_1']
                                    #     dict_od_capacity['leg1_y_bookings_1'] = value1['y_bookings_1']
                                    #     dict_od_capacity['leg1_j_bookings_1'] = value1['j_bookings_1']
                                    #     dict_od_capacity['leg2_capacity_1'] = value2['y_1'] + \
                                    #                                         value2['j_1']
                                    #
                                    #     dict_od_capacity['leg2_y_1'] = value2['y_1']
                                    #     dict_od_capacity['leg2_j_1'] = value2['j_1']
                                    #     dict_od_capacity['leg2_y_bookings_1'] = value2['y_bookings_1']
                                    #     dict_od_capacity['leg2_j_bookings_1'] = value2['j_bookings_1']
                                    #
                                    #     dict_od_capacity['y_cap_1'] = od_y_cap_1
                                    #     dict_od_capacity['j_cap_1'] = od_j_cap_1
                                    #     dict_od_capacity['capacity_1'] = od_cap_1
                                    #     dict_od_capacity['od_capacity_1'] = od_cap_1

                                    try:
                                        org_data = region_country_data[dict_od_capacity['origin']]
                                    except KeyError:
                                        org_data = {
                                            'region': None,
                                            'country': None,
                                            'cluster': None
                                        }

                                    try:
                                        dest_data = region_country_data[dict_od_capacity['destination']]
                                    except KeyError:
                                        dest_data = {
                                            'region': None,
                                            'country': None,
                                            'cluster': None
                                        }

                                    dict_od_capacity['origin_country'] = org_data['country']
                                    dict_od_capacity['origin_region'] = org_data['region']
                                    dict_od_capacity['origin_cluster'] = org_data['cluster']
                                    dict_od_capacity['origin_network'] = 'Network'

                                    dict_od_capacity['destination_country'] = dest_data['country']
                                    dict_od_capacity['destination_region'] = dest_data['region']
                                    dict_od_capacity['destination_cluster'] = dest_data['cluster']
                                    dict_od_capacity['destination_network'] = 'Network'
                                    lst_od_capacity.append(dict_od_capacity)
                                    count += 1
                                    print count
                                    if len(lst_od_capacity) == 1000:
                                        db.JUP_DB_Host_OD_Capacity.insert_many(
                                            lst_od_capacity)
                                        lst_od_capacity = list()
                                    # print len(lst_od_capacity)

                else:
                    for key1, value1 in leg1_dict.iteritems():
                        month = int(key1[5:7])
                        year = int(key1[:4])
                        dict_od_capacity = dict(
                            origin=od_leg['od'][:3],
                            destination=od_leg['od'][3:],
                            od=od_leg['od'],
                            dep_date=key1,
                            month=month,
                            year=year,
                            quarter='Q' + str(get_quarter(int(month))),
                            combine_column=key1 + od_leg['od'],
                            combine_column_od_month_year=od_leg['od'] + str(month) + str(year)
                        )
                        #### changed the logic to only min(leg1_cap, leg2_cap) so it is
                        #### equivalent to having derived factor of 1

                        # dF = get_derived_factor_val(
                        #     carrier=host,
                        #     origin=dict_od_capacity['origin'],
                        #     destination=dict_od_capacity['destination'],
                        #     quarter=dict_od_capacity['quarter'],
                        #     year=dict_od_capacity['year'],
                        #     derived_factor_data=derived_Factor_data,
                        #     region_country_data=region_country_data)
                        dF = 1
                        od_y_cap = 0
                        if value1['y'] > 0:
                            if dF > 0:
                                od_y_cap = math.ceil(value1['y'] * dF)
                            else:
                                od_y_cap = math.ceil(value1['y'])

                        od_j_cap = 0
                        if value1['j'] > 0:
                            if dF > 0:
                                od_j_cap = math.ceil(value1['j'] * dF)
                            else:
                                od_j_cap = math.ceil(value1['j'])

                        od_cap = od_y_cap + od_j_cap
                        # od_y_cap_1 = 0
                        # if value1['y_1'] > 0:
                        #     if dF > 0:
                        #         od_y_cap_1 = math.ceil(value1['y_1'] * dF)
                        #     else:
                        #         od_y_cap_1 = math.ceil(value1['y_1'])
                        #
                        # od_j_cap_1 = 0
                        # if value1['j_1'] > 0:
                        #     if dF > 0:
                        #         od_j_cap_1 = math.ceil(value1['j_1'] * dF)
                        #     else:
                        #         od_j_cap_1 = math.ceil(value1['j_1'])
                        #
                        # od_cap_1 = od_y_cap_1 + od_j_cap_1

                        if od_cap > 0:
                            dict_od_capacity['leg1_capacity'] = value1['y'] + \
                                value1['j']
                            dict_od_capacity['leg1'] = value1['leg']
                            dict_od_capacity['leg1_y'] = value1['y']
                            dict_od_capacity['leg1_j'] = value1['j']
                            dict_od_capacity['leg1_y_bookings'] = value1['y_bookings']
                            dict_od_capacity['leg1_j_bookings'] = value1['j_bookings']
                            dict_od_capacity['leg2_capacity'] = None
                            dict_od_capacity['leg2'] = None
                            dict_od_capacity['leg2_y'] = None
                            dict_od_capacity['leg2_j'] = None
                            dict_od_capacity['leg2_y_bookings'] = None
                            dict_od_capacity['leg2_j_bookings'] = None
                            dict_od_capacity['derived_factor'] = dF
                            dict_od_capacity['y_cap'] = od_y_cap
                            dict_od_capacity['j_cap'] = od_j_cap
                            dict_od_capacity['capacity'] = od_cap
                            dict_od_capacity['od_capacity'] = od_cap
                            dict_od_capacity['snap_date'] = value1['snap_date']
                            dict_od_capacity['last_update_date'] = SYSTEM_DATE
                            # if od_cap_1 > 0:
                            #     dict_od_capacity['leg1_capacity_1'] = value1['y_1'] + \
                            #                                         value1['j_1']
                            #     dict_od_capacity['leg1_y_1'] = value1['y_1']
                            #     dict_od_capacity['leg1_j_1'] = value1['j_1']
                            #     dict_od_capacity['leg1_y_bookings_1'] = value1['y_bookings_1']
                            #     dict_od_capacity['leg1_j_bookings_1'] = value1['j_bookings_1']
                            #     dict_od_capacity['leg2_capacity'] = None
                            #     dict_od_capacity['leg2'] = None
                            #     dict_od_capacity['leg2_y'] = None
                            #     dict_od_capacity['leg2_j'] = None
                            #     dict_od_capacity['leg2_y_bookings'] = None
                            #     dict_od_capacity['leg2_j_bookings'] = None
                            #     dict_od_capacity['y_cap_1'] = od_y_cap_1
                            #     dict_od_capacity['j_cap_1'] = od_j_cap_1
                            #     dict_od_capacity['capacity_1'] = od_cap_1
                            #     dict_od_capacity['od_capacity_1'] = od_cap_1

                            try:
                                org_data = region_country_data[dict_od_capacity['origin']]
                            except KeyError:
                                org_data = {
                                    'region': None,
                                    'country': None,
                                    'cluster': None
                                }

                            try:
                                dest_data = region_country_data[dict_od_capacity['destination']]
                            except KeyError:
                                dest_data = {
                                    'region': None,
                                    'country': None,
                                    'cluster': None
                                }

                            dict_od_capacity['origin_country'] = org_data['country']
                            dict_od_capacity['origin_region'] = org_data['region']
                            dict_od_capacity['origin_cluster'] = org_data['cluster']
                            dict_od_capacity['origin_network'] = 'Network'

                            dict_od_capacity['destination_country'] = dest_data['country']
                            dict_od_capacity['destination_region'] = dest_data['region']
                            dict_od_capacity['destination_cluster'] = dest_data['cluster']
                            dict_od_capacity['destination_network'] = 'Network'
                            count += 1
                            print count
                            lst_od_capacity.append(dict_od_capacity)
                            if len(lst_od_capacity) == 1000:

                                db.JUP_DB_Host_OD_Capacity.insert_many(
                                    lst_od_capacity)
                                lst_od_capacity = list()
            """
            for index, od_leg in enumerate(lst_od_legs):
                # print 'od details:',od_leg
                # print 'OD Index', index
                # Retrieving leg pairs for an OD
                # KHIJED
                # print 'od',od_leg
                leg1 = od_leg[u'leg1']  # KHIDXB
                leg2 = od_leg[u'leg2']
                # print 'LEG1', leg1
                # print 'LEG2', leg2
                for leg1_capa in lst_legs_capacity:
                    leg1_capacity = -1
                    leg2_capacity = -1
                    # print 'leg1',leg1_capa
                    # Finding leg capacities for an OD pair
                    dict_od_capacity = dict()
                    if leg1 == leg1_capa[u'leg']:
                        # print '1st leg'
                        # print 'leg1', leg1_capa
                        leg1_capacity = leg1_capa[u'capacity']
                        leg1_j = leg1_capa[u'j']
                        leg1_y = leg1_capa[u'y']
                        dep_date = leg1_capa[u'dep_date']
                        month = int(dep_date[5:7])
                        year = int(dep_date[:4])
                        # print 'capa',leg1_capacity
                        # Find leg2 if only leg1 is found
                        if od_leg[u'leg2']:
                            # print '2nd leg',leg2
                            for leg2_capa in lst_legs_capacity:
                                # print 'leg2', leg2_capa
                                leg2 = od_leg[u'leg2']
                                if leg2 == leg2_capa[u'leg'] and dep_date == leg2_capa[u'dep_date']:
                                    leg2_capacity = leg2_capa[u'capacity']
                                    leg2_j = leg2_capa[u'j']
                                    leg2_y = leg2_capa[u'y']
                                    # print 'capa',leg2_capacity


                    if leg1_capacity > 0:
                        od_capacity = leg1_capacity
                        # dict_od_capacity['airline'] = host
                        dict_od_capacity['od'] = od_leg[u'od']
                        dict_od_capacity['origin'] = od_leg['od'][:3]
                        dict_od_capacity['destination'] = od_leg['od'][3:]
                        dict_od_capacity['leg1'] = leg1
                        dict_od_capacity['leg1_capacity'] = leg1_capacity
                        dict_od_capacity['leg1_j'] = leg1_j
                        dict_od_capacity['leg1_y'] = leg1_y
                        dict_od_capacity['dep_date'] = dep_date
                        dict_od_capacity['month'] = int(dep_date[5:7])
                        dict_od_capacity['quarter'] = 'Q' + str(get_quarter(int(month)))
                        dict_od_capacity['year'] = year
                        od_capacity = leg1_capacity
                        od_j_capacity = leg1_j
                        od_y_capacity = leg1_y

                        # actual_capacity = leg1_capacity
                        if leg2_capacity > 0:
                            # print 'leg2_capacity exists'
                            dict_od_capacity['leg2'] = leg2
                            dict_od_capacity['leg2_capacity'] = leg2_capacity
                            dict_od_capacity['leg2_j'] = leg2_j
                            dict_od_capacity['leg2_y'] = leg2_y

                            od_capacity = min(leg2_capacity, leg1_capacity)
                            od_j_capacity = min(leg1_j, leg2_j)
                            od_y_capacity = min(leg1_y, leg2_y)

                        # print dict_od_capacity
                        # Step 4: Retrieve derived factor for that particular od
                        # print 'Retrieving derived factor for', dict_od_capacity['od']
                        # print time.time() - st, 'Till Obtaining the Derived Factor'
                        df = get_derived_factor_val(carrier='FZ',
                                                    origin=dict_od_capacity['od'][:3],
                                                    destination=dict_od_capacity['od'][3:],
                                                    quarter=dict_od_capacity['quarter'],
                                                    year=dict_od_capacity['year'],
                                                    derived_factor_data=derived_Factor_data,
                                                    region_country_data=region_country_data)
                        # df = get_od_derived_factors(dict_od_capacity)
                        # print time.time() - st, 'Obtaining the Derived Factor'
                        # print 'Derived Factor',df
                        od_capacity *= df
                        od_j_capacity *= df
                        od_y_capacity *= df
                        try:
                            org_data = region_country_data[dict_od_capacity['origin']]
                        except KeyError:
                            org_data = {
                                'region':None,
                                'country':None,
                                'cluster':None
                            }

                        try:
                            dest_data = region_country_data[dict_od_capacity['destination']]
                        except KeyError:
                            dest_data = {
                                'region':None,
                                'country':None,
                                'cluster':None
                            }

                        dict_od_capacity['origin_country'] = org_data['country']
                        dict_od_capacity['origin_region'] = org_data['region']
                        dict_od_capacity['origin_cluster'] = org_data['cluster']
                        # dict_od_capacity['origin_network'] = 'Network'

                        dict_od_capacity['destination_country'] = dest_data['country']
                        dict_od_capacity['destination_region'] = dest_data['region']
                        dict_od_capacity['destination_cluster'] = dest_data['cluster']
                        # dict_od_capacity['destination_network'] = 'Network'

                        dict_od_capacity['j_cap'] = round(od_j_capacity)
                        dict_od_capacity['y_cap'] = round(od_y_capacity)
                        dict_od_capacity['od_capacity'] = od_j_capacity + od_y_capacity
                        dict_od_capacity['derived_factor'] = df
                        # dict_od_capacity['last_update_date'] = '2017-03-03'
                        dict_od_capacity['last_update_date'] = str(datetime.datetime.now().strftime('%Y-%m-%d'))
                        # print time.time() - st, 'Updating One Doc'
                    if dict_od_capacity:
                        lst_od_capacity.append(dict_od_capacity)
                        # db.JUP_DB_Host_OD_Capacity.insert(dict_od_capacity)
                        print 'Temperory DOCs length', len(lst_od_capacity)
            """
            derived_Factor_data = None
            region_country_data = None
            dict_legs_capacity = None
            # The above codes is just to kill dump data storing in python
            # print '\nFINAL:', len(lst_od_capacity), 'records'
            # # for od in lst_od_capacity:
            # #     print od
            # import pymongo
            # from jupiter_AI.network_level_params import MONGO_SOURCE_DB, MONGO_CLIENT_URL, ANALYTICS_MONGO_USERNAME, \
            #     ANALYTICS_MONGO_PASSWORD
            # client2 = pymongo.MongoClient('localhost:27017')
            # # client2.the_database.authenticate(ANALYTICS_MONGO_USERNAME, ANALYTICS_MONGO_PASSWORD,
            #                                   # source=MONGO_SOURCE_DB)
            # db2 = client2[JUPITER_DB]
            # db2.JUP_DB_Host_OD_Capacity.insert(lst_od_capacity)
            # print 'Records inserted'
            print time.time() - st
        else:
            print 'No ODs for host airline'
    except Exception as error_msg:
        print traceback.print_exc()
        obj_error = error_class.ErrorObject(
            error_class.ErrorObject.ERRORLEVEL2,
            'jupiter_AI/batch/JUP_AI_OD_Capacity/Host_Inventory.py method: get_od_capacity',
            get_arg_lists(
                inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


if __name__ == '__main__':
    # db.JUP_DB_Capacity_Derived_Factor.update({$or:[{true_origin:"DXB"},
    # {true_destination:"DXB"}]},{$set:{derived_factor:1}}, {multi:true})
    a = {
        'od': [],
        # 'date':'2016-10-15'
        # 'fromDate': '2016-01-01'
    }
    st = time.time()
    print get_od_capacity(a)
    print st - time.time()
