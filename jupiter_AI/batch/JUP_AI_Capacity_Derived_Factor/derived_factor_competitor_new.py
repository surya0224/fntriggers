"""
File Name              :   JUP_AI_Competitor_OD_Capacity.py
Author                 :   Shamail Mulla
Date Created           :   2017-04-04
Description            :  Competitor true OD is derived from leg capacities using minimum distance stops at monthly level.

MODIFICATIONS LOG
    S.No                   : 1
    Date Modified          : 2017-04-18
    By                     : Shamail
    Modification Details   : Put commonly used functions in the common_functions.py file

    S.No 				   : 2
    Date Modified 		   : 2017-05-20 to 2017-05-27
    By                     : Sai Krishna
    Modification Details   : Revamped the Logic of the code as the earlier code was taking a lot of time.

    					    LOGIC EARLIER -
    					    1.	Earlier the process of the code was get the ODs for competitors from market share collection.
    					    2.  Get the Leg level departure month level capacity data from JUP_DB_Capacity(OAG data)
    					    3.  For every competitor For every OD find possible leg combinations from the leg level data.
    					        And the leg combinations with least distance is considered as the valid combination.
    					    4.  For that combination obtain the derived factor and use it to get the nominal od capacity.
    					    5.  Individually update documents in JUP_DB_OD_Capacity collection as and when generated.

							MODIFIED LOGIC -
							1.	Obtain leg level data at airline,leg,compartment,dep_month,dep_year level.
							2.	Assuming the case where in leg itself can be an OD create the OD_LEVEL CAPACITY document for each document
								he leg level data.
								Storing the values in a list to bulk insert later.
							3.	loop over the leg level data twice to obtain possible OD combinations using the leg leven data
								combinations where leg1_destination == leg2_origin.
								Obtain all the possible combinations
							4.	Get the best combinations on the basis of lowest total distance and get the final list of documents to be uploaded.
							5.	Insert both the lists(only 1 leg OD docs obtained in 2) and the (2 leg OD docs obtained in 4) in JUP_DB_OD_Capacity

							Old Code have not been modified but the new logics are written as functions with underscore simple


"""
# from jupiter_AI.tiles.jup_common_functions_tiles import query_month_year_builder
import jupiter_AI.common.ClassErrorObject  as error_class
from collections import defaultdict
from jupiter_AI.batch.JUP_AI_OD_Capacity.region_master_derived_factor import get_derived_factor_val, get_region_country_details, get_derived_factor_details
from jupiter_AI import client, JUPITER_DB
db = client[JUPITER_DB]
# from copy import deepcopy
# from collections import defaultdict
import datetime
import time
import inspect
import pymongo
import math
import numpy as np
import pandas as pd
from bson.objectid import ObjectId
try:
    from jupiter_AI import client, JUPITER_DB, Host_Airline_Code as host,JUPITER_LOGGER
    from jupiter_AI.logutils import measure
    db = client[JUPITER_DB]
except:
    pass


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
    argument_name_list=[]
    argument_value_list=[]
    for k in args:
        argument_name_list.append(k)
        argument_value_list.append(values[k])
    return argument_name_list, argument_value_list


@measure(JUPITER_LOGGER)
def get_quarter(month):
    """
    take the month number as input and return the quarter number in which the month falls
    Eq - inp - 1
         out - 1

         inp - 7
         out - 3

         inp - 11
         out - 4
    """
    return ((month-1) // 3) + 1


@measure(JUPITER_LOGGER)
def get_leg_details_simple():
    """
    For every airline, for each leg and compartment,for each dep mpnth and year, its distance and compartment seats are retrieved
    :return: list of airlines' details
    """
    try:
        if 'JUP_DB_Capacity' in db.collection_names():
            ppln_distance = [
                {
                    '$project':
                        {
                            'airline': '$airline',
                            'month_year': '$timeseries',
                            'origin': '$origin',
                            'destination': '$destination',
                            'compartment': '$compartment',
                            'last_update_date': '$last_updated_date',
                            'compartment_capacity': '$compartment_seats',
                            'distance': '$distance',
                            'flight': '$flight'
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
                            '_id':
                                {
                                    'airline': '$airline',
                                    'origin': '$origin',
                                    'destination': '$destination',
                                    'month_year': '$month_year',
                                    'compartment': '$compartment',
                                    'flight': '$flight'
                                },
                            'capacity': {'$first': '$compartment_capacity'},
                            'distance': {'$first': '$distance'}
                        }
                }
                ,
                {
                    '$group':
                        {
                            '_id':
                                {
                                    'airline': '$_id.airline',
                                    'origin': '$_id.origin',
                                    'destination': '$_id.destination',
                                    'month_year': '$_id.month_year',
                                    'compartment': '$_id.compartment'
                                },
                            'capacity': {'$sum':'$capacity'},
                            'distance': {'$first': '$distance'}
                        }
                }
                ,
                {
                    '$project':
                        {
                            '_id':0,
                            'carrier':'$_id.airline',
                            'leg_origin':'$_id.origin',
                            'leg_destination':'$_id.destination',
                            'month_year':'$_id.month_year',
                            'compartment':'$_id.compartment',
                            'capacity':'$capacity',
                            'distance':'$distance'
                        }
                }
                ,
                {
                    '$out':'temp_comp_capa_collection'
                }
            ]
            data_dict = defaultdict()
            db.JUP_DB_Capacity.aggregate(ppln_distance,allowDiskUse=True)
            crsr_airline_legs = db['temp_comp_capa_collection'].find({},{'_id':0})
            lst_airline_legs = list(crsr_airline_legs)
            for doc in lst_airline_legs:
                data_dict[(doc['carrier'], doc['leg_origin'], doc['leg_destination'], doc['month_year'], doc['compartment'])] = doc
            return data_dict
            # return lst_airline_legs
        else:
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                                'jupiter_AI/batch/JUP_AI_OD_Capacity/Competitor_OD_Capacity.py method: get_leg_details',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('Collection JUP_DB_Capacity not found in the db')
            obj_error.write_error_logs(datetime.datetime.now())
    except Exception as error_msg:
        import traceback
        print traceback.print_exc()
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupiter_AI/batch/JUP_AI_OD_Capacity/Competitor_OD_Capacity.py method: get_leg_details',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


@measure(JUPITER_LOGGER)
def get_od_capacity_simple():
    """
    Creates a new collection JUP_DB_Leg_Level_Capacities containing an OD,
    their legs and respective capacities.
    """
    db.temp_col_capa.drop()
    st = time.time()
    # print st
    print "getting region country details..."
    # get the region to country level data
    region_country_data = get_region_country_details()
    print time.time() - st

    # dF_data = get_derived_factor_details()
    # print time.time() - st

    leg_level_dict = get_leg_details_simple()
    print time.time() - st
    print 'Leg Level Docs', len(leg_level_dict.keys())
    client.close()

    for key, leg1 in leg_level_dict.iteritems():
        quarter = 'Q' + str(get_quarter(int(leg1['month_year'][4:])))
        month = int(leg1['month_year'][4:])
        year = int(leg1['month_year'][:4])
        # df_leg1 = get_derived_factor_val(leg1['carrier'], leg1['leg_origin'], leg1['leg_destination'], quarter, year,
        #                                  dF_data, region_country_data)
        try:
            org_data = region_country_data[leg1['leg_origin']]
        except KeyError:
            org_data = {
                'region': None,
                'country': None,
                'cluster': None
            }

        try:
            dest_data = region_country_data[leg1['leg_destination']]
        except KeyError:
            dest_data = {
                'region': None,
                'country': None,
                'cluster': None
            }

        docl = dict(
            leg1=leg1['leg_origin'] + leg1['leg_destination'],
            leg1_origin=leg1['leg_origin'],
            leg1_destination=leg1['leg_destination'],
            leg1_distance=leg1['distance'],
            leg1_capacity=leg1['capacity'],

            leg2=np.nan,
            leg2_origin=np.nan,
            leg2_destination=np.nan,
            leg2_distance=np.nan,
            leg2_capacity=np.nan,

            airline=leg1['carrier'],
            od=leg1['leg_origin'] + leg1['leg_destination'],
            origin=leg1['leg_origin'],
            origin_country=org_data['country'],
            origin_cluster=org_data['cluster'],
            origin_region=org_data['region'],
            origin_network='Network',
            destination=leg1['leg_destination'],
            total_distance=leg1['distance'],
            compartment=leg1['compartment'],
            destination_country=dest_data['country'],
            destination_cluster=dest_data['cluster'],
            destination_region=dest_data['region'],
            destination_network='Network',

            month=month,
            year=year,
            # capacity=math.ceil(leg1['capacity'] * df_leg1),
            # od_capacity=math.ceil(leg1['capacity'] * df_leg1),
            # derived_factor=df_leg1,
            quarter=quarter,
            last_update_date=str(datetime.datetime.now().strftime('%Y-%m-%d'))
        )
        print "updating collection..."
        db.temp_col_capa.insert(docl)

    # uncomment this - this snippet aggregates leg_1 leg_2 capacities for an od
    # carrier, origin, destination, month_year, compartment
    for key1, leg1 in leg_level_dict.iteritems():
        quarter = 'Q' + str(get_quarter(int(leg1['month_year'][4:])))
        month = int(leg1['month_year'][4:])
        year = int(leg1['month_year'][:4])
        for key2, leg2 in leg_level_dict.iteritems():
            # print "i am in n2 loop"
            if (key1[0] == key2[0] and
                key1[3] == key2[3] and
                key1[4] == key2[4] and
                key1[2] == key2[1] and
                key1[1] != key2[2]):

                # print time.time() - st
                try:
                    org_data = region_country_data[leg1['leg_origin']]
                except KeyError:
                    org_data = {
                        'region': None,
                        'country': None,
                        'cluster': None
                    }

                try:
                    dest_data = region_country_data[leg2['leg_destination']]
                except KeyError:
                    dest_data = {
                        'region': None,
                        'country': None,
                        'cluster': None
                    }

                doc = dict(
                    leg1=leg1['leg_origin'] + leg1['leg_destination'],
                    leg1_origin=leg1['leg_origin'],
                    leg1_destination=leg1['leg_destination'],
                    leg1_distance=leg1['distance'],
                    leg1_capacity=leg1['capacity'],

                    leg2=leg2['leg_origin'] + leg2['leg_destination'],
                    leg2_origin=leg2['leg_origin'],
                    leg2_destination=leg2['leg_destination'],
                    leg2_distance=leg2['distance'],
                    leg2_capacity=leg2['capacity'],

                    airline=leg1['carrier'],
                    od=leg1['leg_origin'] + leg2['leg_destination'],
                    compartment=leg1['compartment'],
                    origin=leg1['leg_origin'],
                    origin_country=org_data['country'],
                    origin_cluster=org_data['cluster'],
                    origin_region=org_data['region'],
                    origin_network='Network',
                    destination=leg2['leg_destination'],
                    destination_country=dest_data['country'],
                    destination_cluster=dest_data['cluster'],
                    destination_region=dest_data['region'],
                    destination_network='Network',
                    total_distance=leg1['distance'] + leg2['distance'],

                    month=month,
                    year=year,
                    quarter=quarter,
                    last_update_date=str(datetime.datetime.now().strftime('%Y-%m-%d'))
                )
                print "updating collection n2..."
                db.temp_col_capa.insert(doc)


    crsr = db.temp_col_capa.aggregate([{"$sort": {"total_distance": 1}}, {"$group": {"_id": {"airline": "$airline", "od":"$od", "compartment": "$compartment", "month": "$month", "year": "$year"},'doc':{'$first':'$$ROOT'}}},{"$project": {"_id":0,"origin":"$doc.origin","origin_cluster":"$doc.origin_cluster","leg2_capacity":"$doc.leg2_capacity","destination_region":"$doc.destination_region","month":"$doc.month","leg2_origin":"$doc.leg2_origin","destination_cluster":"$doc.destination_cluster","year":"$doc.year","leg1_destination":"$doc.leg1_destination","leg1_distance":"$doc.leg1_distance","origin_region":"$doc.origin_region","destination_network":"$doc.destination_network","destination":"$doc.destination","leg2_destination":"$doc.leg2_destination","compartment":"$doc.compartment","airline":"$doc.airline","origin_country":"$doc.origin_country","leg1_origin":"$doc.leg1_origin","leg1":"$doc.leg1","destination_country":"$doc.destination_country","total_distance":"$doc.total_distance","last_update_date":"$doc.last_update_date","od":"$doc.od","leg2_distance":"$doc.leg2_distance","origin_network":"$doc.origin_network","leg1_capacity":"$doc.leg1_capacity","leg2":"$doc.leg2","quarter":"$doc.quarter"}}], allowDiskUse = True)
    docs = []
    db.create_collection("JUP_DB_OD_Leg_level_Capacity")
    count = 0
    for i in crsr:
        # if count %1000 == 0:
        print count
        docs.append(i)
        if len(docs) == 1000:
            start_time = time.time()
            db.JUP_DB_OD_Leg_level_Capacity.insert_many(docs)
            docs = list()
            print str(time.time() - start_time)
        count +=1
    print 'Docs Inserted'


@measure(JUPITER_LOGGER)
def get_derived_factors_city():
    mt_pax_ppln = [

    ]
    print "getting derived factor..."
    crsr_derived_factor = db.JUP_DB_Capacity_Derived_Factor.find({"origin_level": "A",
                                                     "destination_level": "A", "true_origin":{"$ne": None}},{"_id": 0})

    pax_origin_df = pd.DataFrame(list(crsr_derived_factor))
    pax_origin_df = pax_origin_df.rename(columns={"true_origin":"origin", "true_destination":"destination"})

    print "got df..."
    print "getting leg level capacities..."
    leg_level_capacities_crsr = db.JUP_DB_OD_Leg_level_Capacity.find({},{"_id":0,
                                                                         "origin": 1,
                                                                         "destination": 1,
                                                                         "year": 1,
                                                                         "quarter": 1,
                                                                         "leg1": 1,
                                                                         "leg2": 1,
                                                                         "leg1_capacity": 1,
                                                                         "leg2_capacity": 1,
                                                                         "airline": 1})

    leg_level_capacities_df = pd.DataFrame(list(leg_level_capacities_crsr))
    print "got leg level capacities..."
    pax_origin_df = pax_origin_df.merge(leg_level_capacities_df, on=['origin',
                                                                     'destination',
                                                                     'year',
                                                                     'quarter',
                                                                     'airline'], how='left')

    pax_origin_df['min_capacity'] = pax_origin_df[['leg1_capacity', 'leg2_capacity']].min(axis=1)
    pax_origin_df = pax_origin_df[pax_origin_df['year'] == 2016]
    pax_origin_df['derived_factor_new'] = pax_origin_df['od_pax']/pax_origin_df['min_capacity']
    pax_origin_df['derived_factor_new'].fillna(1, inplace=True)
    print pax_origin_df.loc[pax_origin_df['airline']=="FZ",['leg1', 'leg2', 'derived_factor', 'origin', 'destination', 'min_capacity', 'derived_factor_new']].head()
    db.create_collection("JUP_DB_Capacity_Derived_Factor_New")
    print "inserting..."
    db["JUP_DB_Capacity_Derived_Factor_New"].insert_many(pax_origin_df.to_dict("records"))
    print "inserted..."
if __name__ == '__main__':
    start_time = time.time()
    get_od_capacity_simple()
    print str(time.time() - start_time), 'seconds'