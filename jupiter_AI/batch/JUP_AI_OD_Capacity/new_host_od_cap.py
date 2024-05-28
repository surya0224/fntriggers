"""
File Name              : JUP_AI_Host_OD_Capacity
Author                 : Shamail Mulla
Date Created           : 2017-01-30
Description            : This file calculates the OD capacity from leg level capacities only for host airline

MODIFICATIONS LOG
    S.No                   : 2
    Date Modified          : 2017-04-18
    By                     : Shamail
    Modification Details   : Put commonly used functions in the common_functions.py file

"""
import json
from jupiter_AI.tiles.jup_common_functions_tiles import query_month_year_builder
from copy import deepcopy
from collections import defaultdict
import datetime
import inspect

try:
    from jupiter_AI import client, JUPITER_DB, JUPITER_LOGGER
    from jupiter_AI.logutils import measure
    db = client[JUPITER_DB]
except:
    pass
from common_functions import *

lst_od_capacity = list()


@measure(JUPITER_LOGGER)
def query_builder_od_to_leg(dict_user_filter):
    """
    The function creates a query the database for derived factor for od combination/s
    :param dict_user_filter: filter to retrieve marginal revenue for a particular od and date range
    :return: query dictionary
    """
    try:
        if validate_data(dict_user_filter):
            # print 'query_builder_od_to_leg'
            dict_filter = deepcopy(defaultdict(list, dict_user_filter))
            # print 'Creating query...'
            query = defaultdict(list)

            # Creating O&D combinations from input filter
            if dict_filter['origin'] and dict_filter['destination']:
                od_build = []
                od = []
                for idx, item in enumerate(dict_user_filter['origin']):
                    od_build.append({'origin': item, 'destination': dict_user_filter['destination'][idx]})
                for i in od_build:
                    od_pair = i['origin'] + i['destination']
                    od.append({'od': od_pair})
                query['$and'].append({'$or': od})
        # print query
        return query
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/batch/Host_OD_Capacity.py method: query_builder_od_to_leg',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


@measure(JUPITER_LOGGER)
def get_od_legs(dict_filter):
    """
    This function breaks down an OD to leg pairs
    :param dict_filter: filter to retrieve marginal revenue for a particular od and date range
    :return: list of ODs with their legs
    """
    try:
        if 'JUP_DB_Capacity_OD_Master' in db.collection_names():
            # print 'get_od_legs'
            query_od_to_leg = query_builder_od_to_leg(dict_filter)
            ppln_od_legs = [
                # { # remove
                #     '$match': {'od':'DOHKTM'}
                # }
                # ,
                {
                    '$project':
                        {
                            # '_id': None,
                            'od': '$od',
                            'leg1': '$Leg1',
                            'leg2': '$Leg 2'
                        }
                }
                ,
                {
                    '$out': 'od_legs'
                }
            ]
            db.JUP_DB_Capacity_OD_Master.aggregate(ppln_od_legs, allowDiskUse=True)
            crsr_od_legs = db.get_collection('od_legs').find()
            lst_od_legs = list(crsr_od_legs)
            # print '\nOD legs:',len(lst_od_legs),'records.'
            db['od_legs'].drop()
            return lst_od_legs
        else:
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                                'jupter_AI/batch/Host_OD_Capacity.py method: get_od_legs',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Capacity_OD_Master not found in database')
            obj_error.write_error_logs(datetime.datetime.now())
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/batch/Host_OD_Capacity.py method: get_od_legs',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


@measure(JUPITER_LOGGER)
def query_builder_leg_capacity(dict_user_filter):
    """
    The function creates a query database to retrieve leg level capacities
    :param dict_user_filter: filter to retrieve marginal revenue for a particular od and date range
    :return: query dictionary
    """
    try:
        # if validate_data(dict_user_filter):
            # print 'query_builder_leg_capacity'
        dict_filter = deepcopy(defaultdict(list, dict_user_filter))
        # print 'Creating query...'
        query = defaultdict(list)

        lst_legs = list()
        if dict_filter['od']:
            for leg in dict_filter['od']:
                if leg != '':
                    lst_legs.append({'origin': leg[0:3].encode(),
                                     'destination': leg[3:6].encode()})
            # print 'legs'
            # print lst_legs
            query['$and'].append({'$or': lst_legs})

        query['$and'].append({'airline': host})

        # Creating query for compartments
        lst_compartments = []
        if dict_filter['compartment']:
            for compartment in dict_filter['compartment']:
                lst_compartments.append({'compartment': compartment})
            query['$and'].append({'$or': lst_compartments})
        # print query
        return query
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/batch/Host_OD_Capacity.py method: query_builder_leg_capacity',
                                            get_arg_lists(inspect.currentframe()))
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
        if 'JUP_DB_Capacity' in db.collection_names():
            # # Query the leg level capacity collection
            # print 'get_leg_capacities'
            query_legs_capacity = query_builder_leg_capacity(dict_filter)
            # print query_legs_capacity
            ppln_legs_capacity = [
                {
                    '$match': dict(query_legs_capacity)
                }
                ,
                {
                    '$project':
                        {
                            # 'airline': '$airline',
                            'month_year': '$timeseries',
                            'compartment': '$compartment',
                            'od': {'$concat': ['$origin', '$destination']},
                            'last_update_date': '$last_updated_date',
                            'capacity': '$compartment_seats',
                            'distance': '$distance'
                        }
                }
                ,
                {
                    '$redact':
                        {
                            '$cond':
                                {
                                    'if': {'$ne': ['$compartment', 'N/A']},
                                    'then': '$$DESCEND',
                                    'else': '$$PRUNE'
                                }
                        }
                }
                ,
                {
                    '$group':
                        {
                            '_id':
                                {
                                    # 'airline': '$airline',
                                    'month_year': '$month_year',
                                    'compartment': '$compartment',
                                    'od': '$od',
                                    'last_update_date': '$last_update_date'
                                },
                            'capacity': {'$sum': '$capacity'},
                            'distance': {'$first': '$distance'}
                        }
                }
                ,
                {
                    '$sort': {'_id.last_update_date': -1}
                }
                ,
                {
                    '$group':
                        {
                            '_id':
                                {
                                    # 'airline': '$_id.airline',
                                    'month_year': '$_id.month_year',
                                    'compartment': '$_id.compartment',
                                    'od': '$_id.od'
                                },
                            'capacity':{'$first': '$capacity'},
                            'distance': {'$first': '$distance'}
                        }
                }
                ,
                {
                    '$project':
                        {
                            '_id': 0,
                            'leg': '$_id.od',
                            # 'airline': '$_id.airline',
                            'year': {'$substr': ['$_id.month_year', 0, 4]},
                            'month': {'$substr': ['$_id.month_year', 4, 2]},
                            # 'month_year': '$_id.month_year',
                            'compartment': '$_id.compartment',
                            'capacity': '$capacity',
                            'distance': '$distance'
                        }
                }
                ,
                {
                    '$out': 'legs_capacities'
                }
            ]
            # print 'ppln', ppln_legs_capacity
            db.JUP_DB_Capacity.aggregate(ppln_legs_capacity, allowDiskUse=True)
            crsr_leg_capacities = db.get_collection('legs_capacities').find(projection={'_id': 0})
            lst_legs_capacity = list(crsr_leg_capacities)
            db['legs_capacities'].drop()

            # print '\nLEG CAPACITIES by compartment by month_year:', str(len(lst_legs_capacity)), ' records'
            # for leg_capa in lst_legs_capacity:
            #     print leg_capa
            return lst_legs_capacity
        else:
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                                'jupter_AI/batch/Host_OD_Capacity.py method: get_leg_capacities',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Capacity not found in the database')
            obj_error.write_error_logs(datetime.datetime.now())
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/batch/Host_OD_Capacity.py method: get_leg_capacities',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


@measure(JUPITER_LOGGER)
def calculate_OD_capacity(dict_filter):
    """
    This function creates a collection in the database having OD level capacities
    :param dict_filter:
    :return: None

    """
    # db.JUP_DB_Host_OD_Capacity.drop()
    lst_od_capacity = []
    df_factor = get_od_derived_factors([])
    print "got df"
    count = 0
    # try:
    # Step 1: Break down OD to leg level
    lst_od_legs = get_od_legs(dict_filter)
    print "lst_od_legs: ", lst_od_legs[0]
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
        # print '\nUnique legs:', str(len(lst_legs))

        # Step 2: Retrieve monthly leg level capacities
        # print 'dict_filter',dict_filter
        # # Update the filter
        # if dict_filter['origin']:
        #     print 'origin specified'
        #     del dict_filter['origin']
        # if dict_filter['destination']:
        #     print 'destination specified'
        #     del dict_filter['destination']
        dict_filter['od'] = lst_legs
        # print 'updates dict_filter',dict_filter
        # Capacities for unique legs have been retrieved
        lst_legs_capacity = get_leg_capacities(dict_filter)
        # print '\nLeg capacities:',len(lst_legs_capacity),'records.'

        # Step 3: Find OD capacity using derived factor and leg capacities
        # print 'calculate_OD_capacity'
        print "lst_legs: ",lst_legs_capacity[0]
        df_legs = pd.DataFrame(lst_od_legs)
        df_capacity = pd.DataFrame(lst_legs_capacity)
        df_legs = pd.merge(df_legs, df_capacity, how='left', left_on='leg1', right_on='leg')
        df_legs = df_legs[['leg1', 'leg2', 'od', 'capacity', 'compartment', 'distance', 'month', 'year']]
        df_legs = df_legs.rename(columns={'capacity': 'leg1_capacity'})
        # print df_legs.isnull().sum()
        df_legs = df_legs.dropna()
        df_legs = pd.merge(df_legs, df_capacity, left_on=['leg2', 'compartment', 'month', 'year'],
                           right_on=['leg', 'compartment', 'month', 'year'], how='left')

        df_legs = df_legs.rename(columns={"capacity": "leg2_capacity",
                                          "distance_x": "leg1_distance",
                                          "distance_y": "leg2_distance"
                                          })
        df_legs['leg2_distance'].fillna(0, inplace=True)
        df_legs = df_legs.drop('leg', axis=1)
        df_legs['total_distance'] = df_legs['leg1_distance'] + df_legs['leg2_distance']
        df_legs['airline'] = host
        df_legs['origin'] = df_legs['od'].str.slice(0,3)
        df_legs['destination'] = df_legs['od'].str.slice(3,6)
        # print df_legs.isnull().sum()
        df_legs['quarter'] = df_legs['month'].apply(lambda row: 'Q' + str(get_quarter(int(row))))
        df_legs = pd.merge(df_legs, df_factor, how='left', on=['airline', 'origin', 'destination', 'quarter', 'year'])
        df_legs['derived_factor'].fillna(1, inplace=True)
        df_legs['combine_column'] = df_legs.apply(lambda row: str(row['year']) + str(get_str(int(row['month'])) + str(row['od']) + str(row['compartment'])), axis=1)
        df_legs['od_capacity'] = df_legs.apply(lambda row: min(row['leg1_capacity'], row['leg2_capacity']), axis=1)
        df_legs['capacity'] = df_legs['od_capacity'] * df_legs['derived_factor']
        df_legs['last_update_date'] = str(datetime.datetime.now().strftime('%Y-%m-%d'))
        print df_legs[df_legs['leg2'] != ""].head()
        for od_leg in lst_od_legs:
            # Retrieving leg pairs for an OD
            # KHIJED
            # print 'od',od_leg
            leg1 = od_leg[u'leg1'] # KHIDXB

            for leg1_capa in lst_legs_capacity:
                leg1_capacity = -1
                leg2_capacity = -1
                month = 0
                year = 0
                compartment = ''
                dict_od_capacity = dict()
                # Finding leg capacities for an OD pair
                if leg1 == leg1_capa[u'leg']:
                    # print '1st leg'
                    # print 'leg1', leg1,
                    leg1_capacity = leg1_capa[u'capacity']
                    total_distance = leg1_capa[u'distance']

                    od_capacity = leg1_capacity
                    dict_od_capacity['airline'] = host
                    dict_od_capacity['od'] = od_leg[u'od']
                    dict_od_capacity['leg1'] = leg1
                    dict_od_capacity['compartment'] = leg1_capa[u'compartment']
                    dict_od_capacity['month'] = int(leg1_capa[u'month'])
                    dict_od_capacity['year'] = int(leg1_capa[u'year'])
                    dict_od_capacity['quarter'] = 'Q' + str(get_quarter(int(leg1_capa[u'month'])))

                    # print 'capa',leg1_capacity
                    # Find leg2 if only leg1 is found
                    if od_leg[u'leg2']:
                        # print '2nd leg',
                        for leg2_capa in lst_legs_capacity:
                            leg2 = od_leg[u'leg2']

                            if leg2 == leg2_capa[u'leg'] and int(leg1_capa[u'month']) == int(leg2_capa[u'month']) and \
                                            int(leg1_capa[u'year']) == int(leg2_capa[u'year']) and \
                                            leg1_capa[u'compartment'] == leg2_capa[u'compartment']:
                                dict_od_capacity['leg2'] = leg2
                                leg2_capacity = leg2_capa[u'capacity']
                                total_distance += leg2_capa[u'distance']
                                od_capacity = min(leg2_capacity, leg1_capacity)
                                combine_column = str(leg2_capa[u'year']) + get_str(int(leg1_capa[u'month'])) + str(
                                    od_leg[u'od']) + str(compartment)
                                dict_od_capacity['capa_combine'] = combine_column
                                    # print 'capa',leg2_capacity

                    # print 'Finding notional capacity'
                    # Step 4: Retrieve derived factor for that particular od
                    # print 'Retrieving derived factor for', dict_od_capacity['od']
                    # df = get_od_derived_factors(dict_od_capacity)
                    df = df_factor.loc[(df_factor['airline'] == dict_od_capacity['airline']) &
                                   (df_factor['destination'] == dict_od_capacity['od'][3:]) &
                                   (df_factor['origin'] == dict_od_capacity['od'][:3]) &
                                   (df_factor['quarter'] == dict_od_capacity['quarter']) &
                                   (df_factor['year'] == dict_od_capacity['year']), 'derived_factor'].values
                    if len(df) > 0:
                        df = df[0]
                    else:
                        df = 1
                    # print 'Derived Factor',df
                    od_capacity *= df
                    dict_od_capacity['capacity'] = od_capacity
                    dict_od_capacity['distance'] = total_distance
                    dict_od_capacity['last_update_date'] = str(datetime.datetime.now().strftime('%Y-%m-%d'))

                    # print dict_od_capacity

                if dict_od_capacity:
                    print count
                    lst_od_capacity.append(dict_od_capacity)
                    if len(lst_od_capacity) == 10000:
                        print "inserted"
                        # db.JUP_DB_Host_OD_Capacity.insert_many(lst_od_capacity)
                        lst_od_capacity = list()
                    count += 1
        # print '\nFINAL:', len(lst_od_capacity), 'records'
        # for od in lst_od_capacity:
        #     print od
        # db.JUP_DB_Host_OD_Capacity.insert(lst_od_capacity)
        print 'Records inserted'
    else:
        print 'No ODs for host airline'
    # except Exception as error_msg:
    #     print "error"
    #     obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
    #                                         'jupter_AI/batch/Host_OD_Capacity.py method: get_leg_capacities',
    #                                         get_arg_lists(inspect.currentframe()))
    #     obj_error.append_to_error_list(str(error_msg))
    #     obj_error.write_error_logs(datetime.datetime.now())
        # raise error_msg


if __name__ == '__main__':
    a = {
        'origin': [],
        'destination': [],
        'compartment': [],
        'fromDate': '',
        'toDate': ''
    }
    start_time = datetime.datetime.now()
    calculate_OD_capacity(a)

    print str(datetime.datetime.now() - start_time),'seconds'