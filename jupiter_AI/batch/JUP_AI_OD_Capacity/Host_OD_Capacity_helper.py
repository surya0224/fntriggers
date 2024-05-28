import pandas as pd
import numpy as np
from jupiter_AI import client, Host_Airline_Hub, Host_Airline_Code, JUPITER_LOGGER
from jupiter_AI.logutils import measure
import datetime
from dateutil import relativedelta
from jupiter_AI.network_level_params import SYSTEM_DATE
db = client['fzDB_prod']
print "querying..."
crsr = db.JUP_DB_Host_OD_Capacity.find({}, {"_id": 0})
# db.JUP_DB_Host_OD_Capacity.remove({})
print "building df..."
# cap_df_ = pd.DataFrame(list(crsr))
print "running logic...!"
this_year = int(SYSTEM_DATE[0:4])
# cap_df_['dep_date_obj'] = pd.to_datetime(cap_df_['dep_date'])
# print cap_df.head()


@measure(JUPITER_LOGGER)
def get_ly_values(crsr):
    for i in crsr:
        cap_df = pd.DataFrame(list(i))
        for idx, row in cap_df.iterrows():
            print idx
            if Host_Airline_Hub in row['od']:
                is_constructed = 0
            else:
                is_constructed = 1
            or_list = []
            dep_date = row['dep_date']
            snap_date = row['snap_date']
            dep_date = datetime.datetime.strptime(dep_date, "%Y-%m-%d")
            snap_date = datetime.datetime.strptime(snap_date, "%Y-%m-%d")
            dep_date_1 = dep_date - relativedelta.relativedelta(years=1)
            snap_date_1 = snap_date - datetime.timedelta(days=364)
            dep_date_1 = datetime.datetime.strftime(dep_date_1, "%Y-%m-%d")
            snap_date_1 = datetime.datetime.strftime(snap_date_1, "%Y-%m-%d")
            if is_constructed == 0:
                od = row['od']
                or_list.append({"od": od, "dep_date": dep_date_1, "sys_snap_date": snap_date_1})
                print or_list
                ppln_legs_capacity = [
                 {
                     '$match': {"$or": or_list}
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
                                 },
                             'j': {'$sum': '$j'},
                             'y': {'$sum': '$y'},
                             'capacity': {'$sum': '$capacity'},
                             'y_bookings': {"$sum": "$y_booking"},
                             'j_bookings': {"$sum": "$j_booking"},
                             'snap_date': {"$first": "$sys_snap_date"}
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
                ]
                crsr = db.JUP_DB_Inventory_Leg.aggregate(ppln_legs_capacity, allowDiskUse=True)
                temp_df = pd.DataFrame(list(crsr))
                if len(temp_df) > 0:
                    temp_df['dep_date'] = pd.to_datetime(temp_df['dep_date'], yearfirst=True)
                    temp_df['snap_date'] = pd.to_datetime(temp_df['snap_date'], yearfirst=True)
                    temp_df['dep_date'] = temp_df['dep_date'].apply(lambda row: datetime.datetime.strftime(row + relativedelta.relativedelta(years=1), "%Y-%m-%d"))
                    temp_df['snap_date'] = temp_df['snap_date'].apply(lambda row: datetime.datetime.strftime(row + datetime.timedelta(days=364), "%Y-%m-%d"))
                    cap_df = cap_df.merge(temp_df, on=['od', 'dep_date', 'snap_date'], how='left', suffixes=("", "_1"))
                    db.JUP_DB_Host_OD_Capacity_1.insert_many(cap_df.to_dict("records"))
                else:
                    result_dict = [{
                        "od": row['od'],
                        "dep_date": temp_df.loc[0, 'dep_date'],
                        "snap_date": temp_df.loc[0, 'snap_date'],
                        "capacity": np.NaN,
                        "od_capacity": np.NaN,
                        "leg1_y": np.NaN,
                        "leg1_j": np.NaN,
                        "leg2_y": np.NaN,
                        "leg2_j": np.NaN,
                        "leg1_capacity": np.NaN,
                        "leg2_capacity": np.NaN,
                        "leg1_y_bookings": np.NaN,
                        "leg1_j_bookings": np.NaN,
                        "leg2_y_bookings": np.NaN,
                        "leg2_j_bookings": np.NaN,
                        "y_cap": np.NaN,
                        "j_cap": np.NaN
                    }]
                    cap_df = cap_df.merge(pd.DataFrame(temp_df), on=['od', 'dep_date', 'snap_date'], how='left')
                    db.JUP_DB_Host_OD_Capacity_1.insert_many(cap_df.to_dict("records"))
            else:
                od1 = row['leg1']
                od2 = row['leg2']
                od_list = [od1, od2]
                or_list.append({"od": {"$in": od_list}, "dep_date":dep_date_1, "sys_snap_date": snap_date_1})
                ppln_legs_capacity = [
                    {
                        '$match': {"$or": or_list}
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
                ]
                crsr = db.JUP_DB_Inventory_Leg.aggregate(ppln_legs_capacity, allowDiskUse=True)
                temp_df = pd.DataFrame(list(crsr))
                if len(temp_df) > 0:
                    temp_df['dep_date'] = pd.to_datetime(temp_df['dep_date'], yearfirst=True)
                    temp_df['snap_date'] = pd.to_datetime(temp_df['snap_date'], yearfirst=True)
                    temp_df['dep_date'] = temp_df['dep_date'].apply(
                        lambda row: datetime.datetime.strftime(row + relativedelta.relativedelta(years=1), "%Y-%m-%d"))
                    temp_df['snap_date'] = temp_df['snap_date'].apply(
                        lambda row: datetime.datetime.strftime(row + datetime.timedelta(days=364), "%Y-%m-%d"))
                    if len(temp_df) == 1:
                        if temp_df.loc[0, 'od'][0:3] == Host_Airline_Hub:
                            leg1_y_capacity = temp_df.loc[0, 'y']
                            leg1_j_capacity = temp_df.loc[0, 'j']
                            leg2_y_capacity = np.NaN
                            leg2_j_capacity = np.NaN
                            leg1_capacity = leg1_y_capacity + leg1_j_capacity
                            leg2_capacity = leg2_y_capacity + leg2_j_capacity
                            leg1_y_bookings = temp_df.loc[0, 'y_bookings']
                            leg1_j_bookings = temp_df.loc[0, 'j_bookings']
                            leg2_y_bookings = np.NaN
                            leg2_j_bookings = np.NaN
                            od_y_cap = leg1_y_capacity
                            od_j_cap = leg1_j_capacity
                            od_capacity = od_y_cap + od_j_cap
                        else:
                            leg1_y_capacity = np.NaN
                            leg1_j_capacity = np.NaN
                            leg2_y_capacity = temp_df.loc[0, 'y']
                            leg2_j_capacity = temp_df.loc[0, 'j']
                            leg1_capacity = leg1_y_capacity + leg1_j_capacity
                            leg2_capacity = leg2_y_capacity + leg2_j_capacity
                            leg1_y_bookings = np.NaN
                            leg1_j_bookings = np.NaN
                            leg2_y_bookings = temp_df.loc[0, 'y_bookings']
                            leg2_j_bookings = temp_df.loc[0, 'j_bookings']
                            od_y_cap = leg2_y_capacity
                            od_j_cap = leg2_j_capacity
                            od_capacity = od_y_cap + od_j_cap
                    elif len(temp_df) > 1:
                        if temp_df.loc[0, 'od'][0:3] == Host_Airline_Hub:
                            leg1_y_capacity = temp_df.loc[0, 'y']
                            leg1_j_capacity = temp_df.loc[0, 'j']
                            leg2_y_capacity = temp_df.loc[1, 'y']
                            leg2_j_capacity = temp_df.loc[1, 'j']
                            leg1_capacity = leg1_y_capacity + leg1_j_capacity
                            leg2_capacity = leg2_y_capacity + leg2_j_capacity
                            leg1_y_bookings = temp_df.loc[0, 'y_bookings']
                            leg1_j_bookings = temp_df.loc[0, 'j_bookings']
                            leg2_y_bookings = temp_df.loc[1, 'y_bookings']
                            leg2_j_bookings = temp_df.loc[1, 'j_bookings']
                            od_y_cap = leg1_y_capacity + leg2_y_capacity
                            od_j_cap = leg1_j_capacity + leg2_j_capacity
                            od_capacity = od_y_cap + od_j_cap
                        else:
                            leg1_y_capacity = temp_df.loc[1, 'y']
                            leg1_j_capacity = temp_df.loc[1, 'j']
                            leg2_y_capacity = temp_df.loc[0, 'y']
                            leg2_j_capacity = temp_df.loc[0, 'j']
                            leg1_capacity = leg1_y_capacity + leg1_j_capacity
                            leg2_capacity = leg2_y_capacity + leg2_j_capacity
                            leg1_y_bookings = temp_df.loc[1, 'y_bookings']
                            leg1_j_bookings = temp_df.loc[1, 'j_bookings']
                            leg2_y_bookings = temp_df.loc[0, 'y_bookings']
                            leg2_j_bookings = temp_df.loc[0, 'j_bookings']
                            od_y_cap = leg1_y_capacity + leg2_y_capacity
                            od_j_cap = leg1_j_capacity + leg2_j_capacity
                            od_capacity = od_y_cap + od_j_cap
                    else:
                        leg1_y_capacity = np.NaN
                        leg1_j_capacity = np.NaN
                        leg2_y_capacity = np.NaN
                        leg2_j_capacity = np.NaN
                        leg1_capacity = np.NaN
                        leg2_capacity = np.NaN
                        leg1_y_bookings = np.NaN
                        leg1_j_bookings = np.NaN
                        leg2_y_bookings = np.NaN
                        leg2_j_bookings = np.NaN
                        od_y_cap = np.NaN
                        od_j_cap = np.NaN
                        od_capacity = np.NaN

                    result_dict = [{
                        "od": row['od'],
                        "dep_date": temp_df.loc[0, 'dep_date'],
                        "snap_date": temp_df.loc[0, 'snap_date'],
                        "capacity": od_capacity,
                        "od_capacity": od_capacity,
                        "leg1_y": leg1_y_capacity,
                        "leg1_j": leg1_j_capacity,
                        "leg2_y": leg2_y_capacity,
                        "leg2_j": leg2_j_capacity,
                        "leg1_capacity": leg1_capacity,
                        "leg2_capacity": leg2_capacity,
                        "leg1_y_bookings": leg1_y_bookings,
                        "leg1_j_bookings": leg1_j_bookings,
                        "leg2_y_bookings": leg2_y_bookings,
                        "leg2_j_bookings": leg2_j_bookings,
                        "y_cap": od_y_cap,
                        "j_cap": od_j_cap
                    }]
                    temp_df = pd.DataFrame(result_dict)
                    cap_df = cap_df.merge(temp_df, on=['od', 'dep_date', 'snap_date'], how='left', suffixes=("", "_1"))
                    db.JUP_DB_Host_OD_Capacity_1.insert_many(cap_df.to_dict("records"))
                else:
                    # else:
                    result_dict = [{
                        "od": row['od'],
                        "dep_date": temp_df.loc[0, 'dep_date'],
                        "snap_date": temp_df.loc[0, 'snap_date'],
                        "capacity": np.NaN,
                        "od_capacity": np.NaN,
                        "leg1_y": np.NaN,
                        "leg1_j": np.NaN,
                        "leg2_y": np.NaN,
                        "leg2_j": np.NaN,
                        "leg1_capacity": np.NaN,
                        "leg2_capacity": np.NaN,
                        "leg1_y_bookings": np.NaN,
                        "leg1_j_bookings": np.NaN,
                        "leg2_y_bookings": np.NaN,
                        "leg2_j_bookings": np.NaN,
                        "y_cap": np.NaN,
                        "j_cap": np.NaN
                    }]
                    cap_df = cap_df.merge(pd.DataFrame(temp_df), on=['od', 'dep_date', 'snap_date'], how='left')
                    db.JUP_DB_Host_OD_Capacity_1.insert_many(cap_df.to_dict("records"))


@measure(JUPITER_LOGGER)
def chunker(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))

# for chunk in chunker(, 10000):
#     db.JUP_DB_Host_OD_Capacity_1.insert_many(chunk.to_dict("records"))