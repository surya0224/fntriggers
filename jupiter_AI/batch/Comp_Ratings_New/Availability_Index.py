from jupiter_AI import JUPITER_DB, client, JUPITER_LOGGER
from jupiter_AI.logutils import measure


@measure(JUPITER_LOGGER)
def run():
    from pymongo import MongoClient
    from pymongo import UpdateOne
    import pandas as pd
    import numpy as np
    import datetime
    import scipy.stats as ss
    import time
    from jupiter_AI.network_level_params import SYSTEM_DATE, SYSTEM_TIME
    db = client[JUPITER_DB]

    # client = MongoClient("mongodb://localhost:27017")
    # db = client.local

    pd.set_option("display.max_columns", 100)


    TOLERANCE = 0.001


    @measure(JUPITER_LOGGER)
    def rescale_values(list_series, min_score, max_score):
        """
        given a list of input ratings, and minimum and maximum scores, rescales the list to a list of scores so that lowest score=min_score and highest score=max_score
        """
        series_min = min(list_series)
        series_max = max(list_series)
        series_range = series_max - series_min
        score_range = max_score - min_score
        if abs(series_range) < TOLERANCE:  ## all values in list_series are the same
            return 5, None, None
        slope = score_range / series_range
        intercept = min_score - slope * series_min
        out = [0 for i in list_series]
        for i in range(len(out)):
            out[i] = slope * list_series[i] + intercept
        return out, slope, intercept

    skips = 0
        # print ln1

    t1 = 0
    total = []
    updated = 0
    bulk_list = []
    lim = []
    count1 = 0
    count2 = 1
    t2 = 0
    li = 0
    count_batch = 0
    # batch_size = 100
    batch_size = 500
    # od_s1 = ['DXBCMB', 'CMBDXB', 'MLEAMM', 'AMMDXB']
    od_s1 = list(db.JUP_DB_OD_Master.distinct('OD'))
    # od_s1 = list(set(od_s1))
    total_ods = len(od_s1)

    while count_batch < total_ods:
            od_s2 = od_s1[count_batch:count_batch+batch_size]
            # print str(count_batch) + " out of " + str(total_ods)
            ods = []
            # print od_s2
            city_airport = list(
                db.JUP_DB_City_Airport_Mapping.aggregate([{'$project': {'_id': 0, 'City_Code': 1, 'Airport_Code': 1}}]))
            for oridest in od_s2:
                origin = oridest[:3]
                destination = oridest[3:]
                # print origin,destination
                for l in city_airport:
                    if origin == l['Airport_Code']:
                        origin = l['City_Code']
                    else:
                        pass
                        # city_code_ori_list.append(l['City_Code'])
                    if destination == l['Airport_Code']:
                        destination = l['City_Code']
                    else:
                        pass
                ods.append(origin + destination)
            print len(ods)
            # print ods
            final_odlist = []
            airport_code_ori_list = []
            airport_code_dest_list = []
            for l in ods:
                # print l
                origin = l[:3]
                destination = l[3:]
                airport_code_ori_list = []
                airport_code_dest_list = []
                for k in city_airport:
                    if origin == k['City_Code']:
                        airport_code_ori_list.append(origin)
                        airport_code_ori_list.append(k['Airport_Code'])
                    if destination == k['City_Code']:
                        airport_code_dest_list.append(destination)
                        airport_code_dest_list.append(k['Airport_Code'])
                # print airport_code_ori_list, airport_code_dest_list
                print len(airport_code_ori_list), len(airport_code_dest_list)

                for m in airport_code_ori_list:
                    for n in airport_code_dest_list:
                        final_odlist.append(m + n)
            final_odlist = list(set(final_odlist))
            # print final_odlist
            print len(final_odlist)

            print "started"
            total_markets = len(final_odlist)
            # print "total_markets: ",total_markets
            fare5 = list(db.JUP_DB_ATPCO_Fares_Rules.aggregate(
                [{"$match": {"OD": {'$in': final_odlist},
                             '$or': [{"effective_to": {'$gte': SYSTEM_DATE}},{"effective_to": None}]}},
                 {"$lookup": {"from": "JUP_DB_City_Airport_Mapping", 'localField': "origin", "foreignField": "Airport_Code",
                              "as": "city_mapping"}}, {"$unwind": {"path": "$city_mapping"}}, {
                     "$project": {"OD": 1, "origin": 1, "destination": 1, "pseudo_origin": "$city_mapping.City_Code",
                                  "pseudo_destination": "$city_mapping.City_Code", "compartment": 1, "carrier": 1,"fare":1}}, {
                     "$lookup": {"from": "JUP_DB_City_Airport_Mapping", 'localField': "destination",
                                 "foreignField": "Airport_Code", "as": "city_mapping"}}
                    ,
                 {"$unwind": {"path": "$city_mapping"}}, {
                     '$project': {'_id':0,"pseudo_destination": "$city_mapping.City_Code", "pseudo_origin": 1, "OD": 1,
                                  "compartment": 1, "carrier": 1,"fare":1}}
                 ]))
            # print temp_markets
            # print "fare5:", fare5
            fare55 = pd.DataFrame(fare5)
            if len(fare55) != 0:
                for i in range(len(fare55)):
                    fare55['pseudo_od'] = fare55['pseudo_origin'] + fare55['pseudo_destination']
                fare55 = fare55.drop(['pseudo_origin', 'pseudo_destination'], axis=1)
                cur2 = pd.DataFrame(fare55['OD'])
                ood = []
                for k in cur2['OD']:
                    ood.append(k)
                ood = list(set(ood))
                # print "ood: ", ood
                fare55 = fare55.drop(['OD'], axis=1)
                fare55 = fare55.groupby(['pseudo_od', 'compartment', 'carrier']).count().reset_index()
                grouped_df = fare55.groupby(by=['pseudo_od', 'compartment'])
                li = []
                val1 = []
                for odc, values in grouped_df:
                    # print odc
                    val = values['fare'].values
                    list_ranks = ss.rankdata(val)
                    min_rank = min(list_ranks)
                    max_rank = max(list_ranks)
                    max_score = 10.0
                    min_score = 0.0
                    if len(list_ranks) < 4:
                        max_score = 7.5
                        min_score = 2.5
                    list_ratings, _, _ = rescale_values(list_ranks, max_score,
                                                        min_score)

                    #     print "list_ratings:", list_ratings
                    li = list_ratings
                    try:
                        len_li = len(li)
                    except Exception:
                        li = [list_ratings]
                    # print "li -->", li
                    li = np.array(li)
                    li = 10 - li
                    # print "li upgraded------------->", li
                    od_dict = {}
                    # print "total docs inserted should be: ", len(grouped_df) * len(ood)
                    # print "odc:", odc
                    od_dict['origin'] = odc[0][:3]
                    od_dict['destination'] = odc[0][3:]
                    od_dict['compartment'] = odc[1]
                    od_dict['pos'] = odc[0][:3]
                    od_dict['group'] = "Fares Rating"
                    od_dict['component'] = "Price Availability Index"
                    od_dict['last_update_date_gmt'] = SYSTEM_DATE+" "+ SYSTEM_TIME
                    carrier_list = values['carrier']

                    ratings_list = li
                    # print ratings_list
                    # print "carrier list", carrier_list.values
                    zipped_carrier = zip(carrier_list.values, ratings_list)
                    agent_ratings = []
                    temp_dict = {}
                    for carr in zipped_carrier:
                        # print "-- carrier", carrier
                        temp_dict['' + str(carr[0]) + ''] = carr[1]
                    od_dict['ratings'] = temp_dict

                    od_in = od_dict['origin'] + od_dict['destination']
                    # print od_in
                    # print "od_dict: ", od_dict

                    airport_code_ori_list = []
                    airport_code_dest_list = []

                    for x in city_airport:
                        origin = od_dict['origin']
                        destination = od_dict['destination']
                        #     print origin, destination
                        if origin == x['City_Code']:
                            airport_code_ori_list.append(x['Airport_Code'])
                        if destination == x['City_Code']:
                            airport_code_dest_list.append(x['Airport_Code'])
                    # print airport_code_ori_list, airport_code_dest_list
                    # print len(airport_code_ori_list), len(airport_code_dest_list)

                    for m in airport_code_ori_list:
                        for n in airport_code_dest_list:
                            od_in = m + n
                            # print od_in
                            if od_in in ood:
                                # print "od_in list: ", od_in
                                od_dict['origin'] = od_in[:3]
                                od_dict['destination'] = od_in[3:]
                                total.append(od_dict)
            count_batch += batch_size

    total = list(np.unique(total))
    initial = 0
    sets1 = 1000
    print len(total)
    while initial <= len(total):
        sets2 = total[initial:sets1]
        print "hjgku-----", len(sets2)
        final_list = []
        for li in sets2:
            try:
                print li['_id']
            except:
                # print "////////", li
                final_list.append(li)
        print "############# ", len(final_list)
        try:
            db.JUP_DB_Data_Competitor_Ratings.insert_many(final_list)
        except:
            pass
        initial = initial + sets1
        sets1 += sets1


if __name__ == "__main__":
    run()


