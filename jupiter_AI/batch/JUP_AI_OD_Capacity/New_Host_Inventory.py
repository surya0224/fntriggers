"""
Author                 : Akshay Karangale
Description            : This file calculates the OD capacity from leg level capacities only for host airline

MODIFICATIONS LOG
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :

"""

import traceback
import datetime
import inspect
from pymongo import UpdateOne
from jupiter_AI import SYSTEM_DATE, today, Host_Airline_Hub, mongo_client, JUPITER_DB, Host_Airline_Code, JUPITER_LOGGER
from jupiter_AI.logutils import measure
from jupiter_AI.common import ClassErrorObject as error_class
from jupiter_AI.tiles.jup_common_functions_tiles import gen_collection_name as gen
import pandas as pd
from dateutil.relativedelta import relativedelta


today_ = today - datetime.timedelta(days=1)

lst_od_capacity = []
count = 0


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
def get_leg_capacities(today_, db):
    """
        This function finds the capacities for legs
        :param dict_filter: filter to retrieve marginal revenue for a particular od and date range
        :return: capacity list
        """
    SYSTEM_DATE_ = today_
    today_ = datetime.datetime.strptime(today_, "%Y-%m-%d")
    SYSTEM_DATE_1 = (today_ - relativedelta(days=364)).strftime("%Y-%m-%d")
    try:
        coll_name = gen()
        if 'JUP_DB_Inventory_Leg' in db.collection_names():
            # # Query the leg level capacity collection
            print 'JUP_DB_Inventory_Leg collection present.'
            # query_legs_capacity = query_builder_leg_capacity(dict_filter)

            # Pipeline to get capacity for this year
            ppln_legs_capacity = [
                {
                    '$match': {"sys_snap_date": SYSTEM_DATE_}
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
                }
                # {
                #     '$out': coll_name
                # }
            ]

            # Pipeline to get capacity for last year
            ppln_inventory = [{"$match": {"sys_snap_date": SYSTEM_DATE_1}},
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
                            "capacity": {"$sum": "$capacity"},
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
                            "j_bookings": 1,
                            "_id":0
                        }
                }
            ]
            # print 'ppln', ppln_legs_capacity
            lst_legs_capacity = db.JUP_DB_Inventory_Leg.aggregate(
                ppln_legs_capacity, allowDiskUse=True)
            # lst_legs_capacity = db.get_collection(
            #     coll_name).find(projection={'_id': 0})
            # lst_legs_capacity = list(crsr_leg_capacities)
            legs_capacity_df = pd.DataFrame()
            count = 0
            temp = []
            for market in lst_legs_capacity:
                if len(temp) < 100000:
                    temp.append(dict(market))
                    count += 1
                else:
                    temp_df = pd.DataFrame(temp)
                    legs_capacity_df = pd.concat([legs_capacity_df, temp_df])
                    temp = []
                    print "Count : ", count
            if len(temp) < 100000:
                legs_capacity_df = pd.concat([legs_capacity_df, pd.DataFrame(temp)])
                print "Count : ", count
            print "Done lst_legs_capacity_df"

            inventory_crsr = db.JUP_DB_Inventory_Leg.aggregate(
                ppln_inventory, allowDiskUse=True)
            inventory_df = pd.DataFrame()
            count = 0
            temp = []
            for market in inventory_crsr:
                if len(temp) < 100000:
                    temp.append(dict(market))
                    count += 1
                else:
                    temp_df = pd.DataFrame(temp)
                    inventory_df = pd.concat([inventory_df, temp_df])
                    temp = []
                    print "Count : ", count
            if len(temp) < 100000:
                inventory_df = pd.concat([inventory_df, pd.DataFrame(temp)])
                print "Count : ", count
            print "Done inventory_df"
            if len(inventory_df) == 0:
                inventory_df = pd.DataFrame(columns=['od',
                                                     'dep_date_1',
                                                     'snap_date_1',
                                                     'capacity',
                                                     'j',
                                                     'y',
                                                     'y_bookings',
                                                     'j_bookings'])
            # inventory_df = pd.DataFrame(list(inventory_crsr))

            # Logic to get last year's capacity alongside this year's capacity
            legs_capacity_df['dep_date_obj'] = pd.to_datetime(legs_capacity_df['dep_date'], yearfirst=True)
            legs_capacity_df['snap_date_obj'] = pd.to_datetime(legs_capacity_df['snap_date'], yearfirst=True)
            legs_capacity_df['dep_date_1'] = legs_capacity_df.dep_date_obj.\
                apply(lambda row: (row - datetime.timedelta(days=364)).strftime("%Y-%m-%d"))
            legs_capacity_df['snap_date_1'] = legs_capacity_df.snap_date_obj.\
                apply(lambda row: (row - datetime.timedelta(days=364)).strftime("%Y-%m-%d"))
            #
            # legs_capacity_df['snap_date_1'] = legs_capacity_df['snap_date']
            legs_capacity_df = legs_capacity_df.merge(inventory_df,
                                                      on=['od', 'dep_date_1', 'snap_date_1'],
                                                      how='left',
                                                      suffixes=("", "_1"))
            #
            del inventory_df
            # lst_legs_capacity = legs_capacity_df.to_dict("records")

            # db[coll_name].drop()
            #
            # print len(lst_legs_capacity),'leg capacities'

            # Logic to insert leg capacities into Host_OD_Collection
            legs_capacity_df = legs_capacity_df.drop(['dep_date_obj', 'snap_date_obj', 'dep_date_1', 'snap_date_1'], axis=1)
            orig_columns = legs_capacity_df.columns

            legs_capacity_df['origin'] = legs_capacity_df.od.str.slice(0, 3)
            legs_capacity_df['destination'] = legs_capacity_df.od.str.slice(3, )

            region_details = db.JUP_DB_Region_Master.find({}, {"_id": 0,
                                                               "POS_CD": 1,
                                                               "COUNTRY_CD": 1,
                                                               "Cluster": 1,
                                                               "Region": 1})

            region_details = pd.DataFrame(list(region_details))

            region_details = region_details.rename(columns={"POS_CD": "origin",
                                                            "COUNTRY_CD": "origin_country",
                                                            "Cluster": "origin_cluster",
                                                            "Region": "origin_region"})

            legs_capacity_df = legs_capacity_df.merge(region_details, on='origin', how='left')

            region_details = region_details.rename(columns={"origin": "destination",
                                                            "origin_country": "destination_country",
                                                            "origin_cluster": "destination_cluster",
                                                            "origin_region": "destination_region"})

            legs_capacity_df = legs_capacity_df.merge(region_details, on='destination', how='left')

            legs_capacity_df['leg2_y'] = pd.np.NaN
            legs_capacity_df['leg2_j'] = pd.np.NaN
            legs_capacity_df['leg2_y_bookings'] = pd.np.NaN
            legs_capacity_df['leg2_j_bookings'] = pd.np.NaN
            legs_capacity_df['leg2_capacity'] = pd.np.NaN
            legs_capacity_df['leg2_y_1'] = pd.np.NaN
            legs_capacity_df['leg2_j_1'] = pd.np.NaN
            legs_capacity_df['leg2_y_bookings_1'] = pd.np.NaN
            legs_capacity_df['leg2_j_bookings_1'] = pd.np.NaN
            legs_capacity_df['leg2_capacity_1'] = pd.np.NaN
            legs_capacity_df['leg2'] = pd.np.NaN

            legs_capacity_df = legs_capacity_df.rename(columns={
                "capacity": "leg1_capacity",
                "j": "leg1_j",
                "j_bookings": "leg1_j_bookings",
                "leg": "leg1",
                "y": "leg1_y",
                "y_bookings": "leg1_y_bookings",
                "capacity_1": "leg1_capacity_1",
                "j_1": "leg1_j_1",
                "y_1": "leg1_y_1",
                "y_bookings_1": "leg1_y_bookings_1",
                "j_bookings_1": "leg1_j_bookings_1"
            })

            legs_capacity_df['od_capacity'] = legs_capacity_df['leg1_capacity']
            legs_capacity_df['od_capacity_1'] = legs_capacity_df['leg1_capacity_1']
            legs_capacity_df['y_cap'] = legs_capacity_df['leg1_y']
            legs_capacity_df['j_cap'] = legs_capacity_df['leg1_j']
            legs_capacity_df['y_cap_1'] = legs_capacity_df['leg1_y_1']
            legs_capacity_df['j_cap_1'] = legs_capacity_df['leg1_j_1']

            legs_capacity_df['month'] = legs_capacity_df['dep_date'].str.slice(5, 7).astype('int')
            legs_capacity_df['year'] = legs_capacity_df['dep_date'].str.slice(0, 4).astype('int')
            legs_capacity_df['derived_factor'] = 1
            legs_capacity_df['combine_column'] = legs_capacity_df['dep_date'] + legs_capacity_df['od']
            legs_capacity_df['combine_column_od_month_year'] = legs_capacity_df['od'] + \
                                                               legs_capacity_df['month'].astype('str') + \
                                                               legs_capacity_df['year'].astype('str')
            legs_capacity_df['last_update_date'] = SYSTEM_DATE

            operations = []
            # for chunk in chunker(legs_capacity_df, 1000):
            #     operations = []
            #     if len(chunk) > 0:
            #         records = chunk.to_dict("records")
            #         for record in records:
            #             operations.append(
            #                 UpdateOne({"dep_date": record['dep_date'], "od": record['od']},
            #                           {"$set": record}, upsert=True))
            #         db.JUP_DB_Host_OD_Capacity.insert_many(chunk.to_dict("records"))
            #         count = count + len(chunk)
            #         print "Done: ", count
            count = 0
            for idx, row in legs_capacity_df.iterrows():
                if count == 1000:
                    print "inserting..."
                    db.JUP_DB_Host_OD_Capacity.bulk_write(operations)
                    operations = []
                    row_dict = row.to_dict()
                    operations.append(
                        UpdateOne({"dep_date": row['dep_date'], "od": row['od']},
                                  {"$set": row_dict}, upsert=True)
                    )
                    count=1
                else:
                    row_dict = row.to_dict()
                    operations.append(
                        UpdateOne({"dep_date": row['dep_date'], "od": row['od']},
                                  {"$set": row_dict}, upsert=True)
                    )
                    count += 1
            if count > 0:
                print "inserting..."
                db.JUP_DB_Host_OD_Capacity.bulk_write(operations)

            count = 0
            legs_capacity_df = legs_capacity_df.rename(columns={
                "leg1_capacity": "capacity",
                "leg1_y": "y",
                "leg1_j": "j",
                "leg1_y_1": "y_1",
                "leg1_j_1": "j_1",
                "leg1_y_bookings": "y_bookings",
                "leg1_j_bookings": "j_bookings",
                "leg1_y_bookings_1": "y_bookings_1",
                "leg1_j_bookings_1": "j_bookings_1",
                "leg1": "leg",
                "leg1_capacity_1": "capacity_1"
            })

            # Logic to insert capacities for connection ODs into the collection
            legs_capacity_df = legs_capacity_df[orig_columns]

            org_df = legs_capacity_df[legs_capacity_df.od.str.slice(0,3) == Host_Airline_Hub]
            dest_df = legs_capacity_df[legs_capacity_df.od.str.slice(3,) == Host_Airline_Hub]

            dest_df['origin'] = dest_df.od.str.slice(0, 3)
            dest_df['destination'] = dest_df.od.str.slice(3, )

            org_df['origin'] = org_df.od.str.slice(0, 3)
            org_df['destination'] = org_df.od.str.slice(3, )

            dest_df = dest_df.rename(columns={"origin": "destination", "destination": "origin"})

            for chunk in chunker(org_df, 10000):
                final_df = calculate_capacity(org_df=chunk, dest_df=dest_df)
                region_details = db.JUP_DB_Region_Master.find({}, {"_id": 0,
                                                                   "POS_CD": 1,
                                                                   "COUNTRY_CD": 1,
                                                                   "Cluster": 1,
                                                                   "Region": 1})

                region_details = pd.DataFrame(list(region_details))

                region_details = region_details.rename(columns={"POS_CD": "origin",
                                                                "COUNTRY_CD": "origin_country",
                                                                "Cluster": "origin_cluster",
                                                                "Region": "origin_region"})

                final_df = final_df.merge(region_details, on='origin', how='left')

                region_details = region_details.rename(columns={"origin": "destination",
                                                                "origin_country": "destination_country",
                                                                "origin_cluster": "destination_cluster",
                                                                "origin_region": "destination_region"})

                final_df = final_df.merge(region_details, on='destination', how='left')

                if len(final_df) > 0:
                    # db.JUP_DB_Host_OD_Capacity.insert_many(final_df.to_dict("records"))
                    # count = count + len(final_df)
                    # print "Done: ", count
                    operations = []
                    count = 0
                    for idx, row in final_df.iterrows():
                        if count == 1000:
                            print "inserting..."
                            db.JUP_DB_Host_OD_Capacity.bulk_write(operations)
                            operations = []
                            row_dict = row.to_dict()
                            operations.append(
                                UpdateOne({"dep_date": row['dep_date'], "od": row['od']},
                                          {"$set": row_dict}, upsert=True)
                            )
                            count = 1
                        else:
                            row_dict = row.to_dict()
                            operations.append(
                                UpdateOne({"dep_date": row['dep_date'], "od": row['od']},
                                          {"$set": row_dict}, upsert=True)
                            )
                            count += 1
                    if count > 0:
                        print "inserting..."
                        db.JUP_DB_Host_OD_Capacity.bulk_write(operations)


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


# Function to actually calculate the connection capacities from leg capacities


@measure(JUPITER_LOGGER)
def calculate_capacity(org_df, dest_df):
    final_df = org_df.merge(
        dest_df[['origin',
                 'destination',
                 'dep_date',
                 'j',
                 'y',
                 'j_1',
                 'y_1',
                 'y_bookings',
                 'y_bookings_1',
                 'j_bookings',
                 'j_bookings_1',
                 'capacity',
                 'capacity_1']],
        on=['origin', 'dep_date'],
        how='inner')

    final_df = final_df.rename(columns={"j_x": "leg2_j",
                                   "j_y": "leg1_j",
                                   "y_x": "leg2_y",
                                   "y_y": "leg1_y",
                                   "y_bookings_x": "leg2_y_bookings",
                                   "y_bookings_y": "leg1_y_bookings",
                                   "j_bookings_x": "leg2_j_bookings",
                                   "j_bookings_y": "leg1_j_bookings",
                                   "y_bookings_1_x": "leg2_y_bookings_1",
                                   "y_bookings_1_y": "leg1_y_bookings_1",
                                   "j_bookings_1_x": "leg2_j_bookings_1",
                                   "j_bookings_1_y": "leg1_j_bookings_1",
                                   "j_1_y": "leg1_j_1",
                                   "j_1_x": "leg2_j_1",
                                   "y_1_y": "leg1_y_1",
                                   "y_1_x": "leg2_y_1",
                                   "destination_x": "true_destination",
                                   "destination_y": "true_origin",
                                   "capacity_x": "leg2_capacity",
                                   "capacity_y": "leg1_capacity",
                                   "capacity_1_x": "leg2_capacity_1",
                                   "capacity_1_y": "leg1_capacity_1"})

    final_df = final_df.drop(final_df[final_df['true_origin'] == final_df['true_destination']].index)

    final_df = final_df.drop(['leg', 'od', 'origin'], axis=1)

    final_df['od'] = final_df['true_origin'] + final_df['true_destination']

    final_df['od_capacity'] = final_df[['leg2_capacity', 'leg1_capacity']].min(axis=1)
    final_df['y_cap'] = final_df[['leg2_y', 'leg1_y']].min(axis=1)
    final_df['j_cap'] = final_df[['leg2_j', 'leg1_j']].min(axis=1)

    final_df['od_capacity_1'] = final_df[['leg2_capacity_1', 'leg1_capacity_1']].min(axis=1)
    final_df['y_cap_1'] = final_df[['leg2_y_1', 'leg1_y_1']].min(axis=1)
    final_df['j_cap_1'] = final_df[['leg2_j_1', 'leg1_j_1']].min(axis=1)

    final_df = final_df.rename(columns={
        "true_origin": "origin",
        "true_destination": "destination"
    })

    final_df['last_update_date'] = SYSTEM_DATE

    final_df['month'] = final_df['dep_date'].str.slice(5, 7).astype('int')
    final_df['year'] = final_df['dep_date'].str.slice(0, 4).astype('int')
    final_df['derived_factor'] = 1
    final_df['leg1'] = final_df['origin'] + Host_Airline_Hub
    final_df['leg2'] = Host_Airline_Hub + final_df['destination']
    final_df['combine_column_od_month_year'] = final_df['od'] + final_df['month'].astype('str') + final_df['year'].astype('str')
    final_df['combine_column'] = final_df['dep_date'] + final_df['od']

    return final_df


@measure(JUPITER_LOGGER)
def chunker(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))


@measure(JUPITER_LOGGER)
def host_od_helper(client):
    db = client[JUPITER_DB]
    # today_1 is one day minus today's date
    today_1 = today - datetime.timedelta(days=1)
    SYSTEM_DATE_ = today_1.strftime("%Y-%m-%d")
    inv_crsr = db.JUP_DB_Inventory_Leg.find({"sys_snap_date": SYSTEM_DATE_})
    if inv_crsr.count() > 0:
        # db.JUP_DB_Host_OD_Capacity.remove({})
        get_leg_capacities(SYSTEM_DATE_, db)
    else:
        print "Inventory not updated for: ", SYSTEM_DATE


if __name__ == "__main__":
    client = mongo_client()
    host_od_helper(client=client)
    client.close()
