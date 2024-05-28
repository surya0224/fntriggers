"""
File Name              :  Competitor_OD_Capacity.py
Author                 :  Akshay Karangale
Date Created           :  2017-11-04
Description            :  Competitor true OD is derived from leg capacities using minimum distance stops at monthly level.

MODIFICATIONS LOG
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :

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
import jupiter_AI.common.ClassErrorObject as error_class
from jupiter_AI import mongo_client, JUPITER_DB, SYSTEM_DATE, JUPITER_LOGGER
from jupiter_AI.logutils import measure
import pandas as pd
import datetime
import time
import inspect

SYSTEM_DATE_1 = datetime.datetime.strftime(datetime.datetime.strptime(SYSTEM_DATE, '%Y-%m-%d') -
                    datetime.timedelta(days=1), '%Y-%m-%d')


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
    return ((month - 1) // 3) + 1


@measure(JUPITER_LOGGER)
def get_leg_details_simple(db):
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
                            'last_update_date': '$last_update_date',
                            'compartment_capacity': '$seats',
                            'distance': '$distance',
                            'flight': '$flight',
                            'dep_time': '$dep_time',
                            'block_hr_min': {"$multiply":["$block_time_hr", 60]},
                            'block_mn': "$block_time_mn",
                            'freq': '$freq'
                        }
                },
                {
                    '$project':
                        {
                            'airline': '$airline',
                            'month_year': '$month_year',
                            'origin': '$origin',
                            'destination': '$destination',
                            'compartment': '$compartment',
                            'last_update_date': '$last_update_date',
                            'compartment_capacity': '$compartment_capacity',
                            'distance': '$distance',
                            'flight': '$flight',
                            'dep_time': '$dep_time',
                            'block_time_min': {"$add": ["$block_hr_min", "$block_mn"]},
                            'freq': '$freq'
                        }
                },
                {
                    '$sort': {'last_update_date': -1}
                },
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
                                    'flight': '$flight',
                                    'dep_time': '$dep_time'
                                },
                            'capacity': {'$first': '$compartment_capacity'},
                            'distance': {'$first': '$distance'},
                            'block_time': {'$first': '$block_time_min'},
                            'freq': {"$first": "$freq"}
                        }
                },
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
                            'capacity': {'$sum': '$capacity'},
                            'distance': {'$first': '$distance'},
                            'block_time': {'$avg': '$block_time'},
                            'freq': {"$avg": "$freq"}
                        }
                },
                {
                    '$project':
                        {
                            '_id': 0,
                            'carrier': '$_id.airline',
                            'leg_origin': '$_id.origin',
                            'leg_destination': '$_id.destination',
                            'month_year': '$_id.month_year',
                            'compartment': '$_id.compartment',
                            'capacity': '$capacity',
                            'distance': '$distance',
                            'block_time': '$block_time',
                            'freq': '$freq'
                        }
                },
                # {
                #     '$out': 'temp_comp_capa_collection'
                # }
            ]
            oag_crsr = db.JUP_DB_Capacity.aggregate(ppln_distance, allowDiskUse=True)
            oag_df = pd.DataFrame(list(oag_crsr))
            return oag_df

        else:
            obj_error = error_class.ErrorObject(
                error_class.ErrorObject.ERRORLEVEL2,
                'jupiter_AI/batch/JUP_AI_OD_Capacity/Competitor_OD_Capacity.py method: get_leg_details',
                get_arg_lists(
                    inspect.currentframe()))
            obj_error.append_to_error_list(
                'Collection JUP_DB_Capacity not found in the db')
            obj_error.write_error_logs(datetime.datetime.now())
    except Exception as error_msg:
        import traceback
        print traceback.print_exc()
        obj_error = error_class.ErrorObject(
            error_class.ErrorObject.ERRORLEVEL2,
            'jupiter_AI/batch/JUP_AI_OD_Capacity/Competitor_OD_Capacity.py method: get_leg_details',
            get_arg_lists(
                inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


@measure(JUPITER_LOGGER)
def get_od_capacity_simple(db):
    """
    Converts the Leg Capacity Data into OD level data for all airlines.
    """

    cap_crsr = list(db.JUP_DB_Capacity.find({"last_update_date": SYSTEM_DATE_1}).limit(1))
    if len(cap_crsr) > 0:
        print "removing previous documents..."
        db.JUP_DB_OD_Capacity.remove({})
        city_ap_crsr = list(db.JUP_DB_City_Airport_Mapping.find({}, {"_id": 0}))
        print "Hi"

        city_ap_df = pd.DataFrame(city_ap_crsr)

        cap = get_leg_details_simple(db)

        orig_columns = cap.columns

        region_details = db.JUP_DB_Region_Master.find({}, {"_id": 0,
                                                           "POS_CD": 1,
                                                           "COUNTRY_CD": 1,
                                                           "Cluster": 1,
                                                           "Region": 1})

        region_details = pd.DataFrame(list(region_details))

        region_details = region_details.rename(columns={"POS_CD": "leg_origin",
                                                        "COUNTRY_CD": "origin_country",
                                                        "Cluster": "origin_cluster",
                                                        "Region": "origin_region"})

        cap = cap.merge(region_details, on='leg_origin', how='left')

        region_details = region_details.rename(columns={"leg_origin": "leg_destination",
                                                        "origin_country": "destination_country",
                                                        "origin_cluster": "destination_cluster",
                                                        "origin_region": "destination_region"})

        cap = cap.merge(region_details, on='leg_destination', how='left')

        cap = cap.rename(columns={"leg_origin": "origin",
                                   "leg_destination": "destination"})

        cap = cap.merge(city_ap_df[['Airport_Code', 'City_Code']], left_on='origin',
                                              right_on='Airport_Code', how='left').rename(
            columns={"City_Code": "pseudo_origin"}).drop('Airport_Code', axis=1)

        cap = cap.merge(city_ap_df[['Airport_Code', 'City_Code']], left_on='destination',
                                              right_on='Airport_Code', how='left').rename(
            columns={"City_Code": "pseudo_destination"}).drop('Airport_Code', axis=1)
        print "hi@@@@"

        cap.loc[cap['pseudo_origin'].isnull(), 'pseudo_origin'] = cap.loc[
            cap['pseudo_origin'].isnull(), 'pseudo_origin'].fillna(value='')

        cap.loc[cap['pseudo_origin'] == "", 'pseudo_origin'] = cap.loc[cap['pseudo_origin'] == "", 'pseudo_origin'] + \
                                                                       cap.loc[cap['pseudo_origin'] == "", 'origin']

        cap.loc[cap['pseudo_destination'].isnull(), 'pseudo_destination'] = cap.loc[
            cap['pseudo_destination'].isnull(), 'pseudo_destination'].fillna(value='')

        cap.loc[cap['pseudo_destination'] == "", 'pseudo_destination'] = cap.loc[cap['pseudo_destination'] == "", 'pseudo_destination'] + \
                                                                                 cap.loc[cap['pseudo_destination'] == "", 'destination']

        cap['pseudo_od'] = cap['pseudo_origin'] + cap['pseudo_destination']

        cap['od'] = cap['origin'] + cap['destination']
        print "kiiiiiii"


        # cap.drop(['distance'], axis=1, inplace=True)

        cap = cap.rename(columns={"capacity": "od_capacity",
                                  "block_time": "od_block_time",
                                  "freq": "od_freq"})

        cap["is_constructed"] = 0
        print "loppp"

        counter = 0

        cap['month'] = cap['month_year'].str.slice(4,).astype('int')
        cap['year'] = cap['month_year'].str.slice(0,4).astype('int')
        print cap["month"]
        print cap["year"]
        for chunk in chunker(cap, 1000):

            counter += len(chunk)

            if len(chunk) > 0:

                db.JUP_DB_OD_Capacity.insert_many(chunk.to_dict("records"))

                print "inserted " + str(counter) + " out of " + str(len(cap))

        cap = cap.rename(columns={"origin": "leg_origin",
                                  "destination": "leg_destination",
                                  "od_capacity": "capacity",
                                  "od_block_time": "block_time",
                                  "od_freq": "freq"})

        cap = cap[orig_columns]

        update_od_capacity(cap, city_ap_df=city_ap_df, db=db)


@measure(JUPITER_LOGGER)
def chunker(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))


@measure(JUPITER_LOGGER)
def calculate_capacity(hubs_origin_df, hubs_destination_df, city_ap_df):
    hubs_origin_df = hubs_origin_df.merge(
        hubs_destination_df[['leg_origin', 'leg_destination', 'compartment', 'month_year', 'capacity', 'freq', 'block_time']],
        on=['leg_origin',
            'compartment',
            'month_year'],
        how='inner')

    hubs_origin_df = hubs_origin_df.rename(columns={"leg_destination_y": "origin",
                                                    "leg_destination_x": "destination",
                                                    "capacity_x": "leg2_capacity",
                                                    "capacity_y": "leg1_capacity",
                                                    "block_time_x": "leg2_block_time",
                                                    "block_time_y": "leg1_block_time",
                                                    "freq_x": "leg2_freq",
                                                    "freq_y": "leg1_freq"})

    hubs_origin_df.drop(hubs_origin_df[(hubs_origin_df['origin'] == hubs_origin_df['destination'])].index, inplace=True)

    hubs_origin_df = hubs_origin_df.merge(city_ap_df[['Airport_Code', 'City_Code']], left_on='origin',
                                          right_on='Airport_Code', how='left').rename(
        columns={"City_Code": "pseudo_origin"}).drop('Airport_Code', axis=1)

    hubs_origin_df = hubs_origin_df.merge(city_ap_df[['Airport_Code', 'City_Code']], left_on='destination',
                                          right_on='Airport_Code', how='left').rename(
        columns={"City_Code": "pseudo_destination"}).drop('Airport_Code', axis=1)

    hubs_origin_df.loc[hubs_origin_df['pseudo_origin'].isnull(), 'pseudo_origin'] = hubs_origin_df.loc[
        hubs_origin_df['pseudo_origin'].isnull(), 'pseudo_origin'].fillna(value='')

    hubs_origin_df.loc[hubs_origin_df['pseudo_origin'] == "", 'pseudo_origin'] = hubs_origin_df.loc[hubs_origin_df[
                                                                                                        'pseudo_origin'] == "", 'pseudo_origin'] + \
                                                                                 hubs_origin_df.loc[hubs_origin_df[
                                                                                                        'pseudo_origin'] == "", 'origin']

    hubs_origin_df.loc[hubs_origin_df['pseudo_destination'].isnull(), 'pseudo_destination'] = hubs_origin_df.loc[
        hubs_origin_df['pseudo_destination'].isnull(), 'pseudo_destination'].fillna(value='')

    hubs_origin_df.loc[hubs_origin_df['pseudo_destination'] == "", 'pseudo_destination'] = hubs_origin_df.loc[
                                                                                               hubs_origin_df[
                                                                                                   'pseudo_destination'] == "", 'pseudo_destination'] + \
                                                                                           hubs_origin_df.loc[
                                                                                               hubs_origin_df[
                                                                                                   'pseudo_destination'] == "", 'destination']

    #     hubs_origin_df = hubs_origin_df.merge(city_ap_df[['Airport_Code', 'City_Code']], left_on='pseudo_origin', right_on='City_Code', how='left').rename(columns={"Airport_Code": "origin_ap"}).drop('City_Code', axis=1)

    #     hubs_origin_df = hubs_origin_df.merge(city_ap_df[['Airport_Code', 'City_Code']], left_on='pseudo_destination', right_on='City_Code', how='left').rename(columns={"Airport_Code": "destination_ap"}).drop('City_Code', axis=1)

    #     hubs_origin_df.loc[hubs_origin_df['origin_ap'].isnull(), 'origin_ap'] = hubs_origin_df.loc[hubs_origin_df['origin_ap'].isnull(), 'origin_ap'].fillna(value='')

    #     hubs_origin_df.loc[hubs_origin_df['origin_ap'] == "", 'origin_ap'] = hubs_origin_df.loc[hubs_origin_df['origin_ap'] == "", 'origin_ap'] + hubs_origin_df.loc[hubs_origin_df['origin_ap'] == "", 'origin']

    #     hubs_origin_df.loc[hubs_origin_df['destination_ap'].isnull(), 'destination_ap'] = hubs_origin_df.loc[hubs_origin_df['destination_ap'].isnull(), 'destination_ap'].fillna(value='')

    #     hubs_origin_df.loc[hubs_origin_df['destination_ap'] == "", 'destination_ap'] = hubs_origin_df.loc[hubs_origin_df['destination_ap'] == "", 'destination_ap'] + hubs_origin_df.loc[hubs_origin_df['destination_ap'] == "", 'destination']

    hubs_origin_df['pseudo_od'] = hubs_origin_df['pseudo_origin'] + hubs_origin_df['pseudo_destination']

    hubs_origin_df['od'] = hubs_origin_df['origin'] + hubs_origin_df['destination']

    #     hubs_origin_df = hubs_origin_df.merge(host_ods_df, on='od', how='inner')

    hubs_origin_df.drop(['distance', 'leg_origin'], axis=1, inplace=True)

    hubs_origin_df['od_capacity'] = hubs_origin_df[['leg2_capacity', 'leg1_capacity']].min(axis=1)

    hubs_origin_df['od_block_time'] = hubs_origin_df[['leg2_block_time', 'leg1_block_time']].sum(axis=1)

    hubs_origin_df['od_freq'] = hubs_origin_df[['leg2_freq', 'leg1_freq']].min(axis=1)

    hubs_origin_df['month'] = hubs_origin_df['month_year'].str.slice(4,).astype('int')
    hubs_origin_df['year'] = hubs_origin_df['month_year'].str.slice(0,4).astype('int')

    return hubs_origin_df


@measure(JUPITER_LOGGER)
def update_od_capacity(cap, city_ap_df, db):
    print "getting hubs list..."
    hubs_list = list(db.JUP_DB_Carrier_hubs.find({}))

    hubs = {}

    for data in hubs_list:
        hubs[data['carrier']] = data['hub']

    host_ods = list(db.JUP_DB_Host_OD_Capacity.distinct("od"))

    host_ods_df = pd.DataFrame()

    host_ods_df['od'] = host_ods

    airlines = cap['carrier'].unique()

    print "number of carriers: ", len(airlines)
    airline_counter = 0
    for carrier in airlines:
        airline_counter += 1
        print "updating capacity for carrier: ", carrier
        print "airline_counter: ", airline_counter
        try:
            hub = hubs[carrier]
        except KeyError:
            hub = None
        if hub:
            hubs_origin_df = cap[(cap['carrier'] == carrier) & (cap['leg_origin'] == hub)]
            hubs_destination_df = cap[(cap['carrier'] == carrier) & (cap['leg_destination'] == hub)]
            hubs_destination_df = hubs_destination_df.rename(columns={"leg_origin": "leg_destination",
                                                                      "leg_destination": "leg_origin"})

            if len(hubs_origin_df) * len(hubs_destination_df) < 2 * 10 ** 6:
                capacity_df = calculate_capacity(hubs_origin_df=hubs_origin_df,
                                                 hubs_destination_df=hubs_destination_df,
                                                 city_ap_df=city_ap_df)
                capacity_df['is_constructed'] = 1
                capacity_df['hub'] = hub
                if len(capacity_df) > 0:
                    db.JUP_DB_OD_Capacity.insert_many(capacity_df.to_dict("records"))
                    print "inserted!"
            else:
                print "chunking and merging ..."
                CHUNK_SIZE = 200000 / len(hubs_destination_df)
                # counter = 0
                for chunk in chunker(hubs_origin_df, CHUNK_SIZE):
                    if len(chunk) > 0:
                        capacity_chunk = calculate_capacity(hubs_origin_df=chunk,
                                                            hubs_destination_df=hubs_destination_df,
                                                            city_ap_df=city_ap_df)
                        capacity_chunk['is_constructed'] = 1
                        capacity_chunk['hub'] = hub
                        if len(capacity_chunk) > 0:
                            db.JUP_DB_OD_Capacity.insert_many(capacity_chunk.to_dict("records"))
                            # counter += len(capacity_chunk)
                            print "inserted!"


@measure(JUPITER_LOGGER)
def competitor_capacity_helper(client):
    db = client[JUPITER_DB]
    start_time = time.time()

    cap_crsr = db.JUP_DB_Capacity.find({"last_update_date": SYSTEM_DATE_1})
    if cap_crsr.count() > 0:
        get_od_capacity_simple(db)
        print str(time.time() - start_time), 'seconds'
    else:
        print "JUP_DB_Capacity not updated for: ", SYSTEM_DATE


if __name__ == '__main__':
    client = mongo_client()
    competitor_capacity_helper(client=client)
    client.close()
