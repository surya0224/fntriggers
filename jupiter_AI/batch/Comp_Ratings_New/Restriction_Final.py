from jupiter_AI import JUPITER_LOGGER
from jupiter_AI.logutils import measure


@measure(JUPITER_LOGGER)
def run():

    from pymongo import MongoClient
    from pymongo import UpdateOne
    import pandas as pd
    from jupiter_AI.network_level_params import SYSTEM_DATE, SYSTEM_TIME

    # client = MongoClient("mongodb://analytics:KNjSZmiaNUGLmS0Bv2@13.92.251.7:42525/")
    # # client = MongoClient("mongodb://localhost:27017")
    # # db = client.local
    # db = client.fzDB_stg
    from jupiter_AI import JUPITER_DB, client
    db = client[JUPITER_DB]
    import datetime
    import scipy.stats as ss
    import time

    TOLERANCE = 0.001
    pd.set_option("display.max_columns", 100)


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


    import numpy as np

    df1 = pd.DataFrame(
        columns=['cat_101', 'cat_102', 'cat_103', 'cat_104', 'cat_105', 'cat_106', 'cat_107', 'cat_108', 'cat_109',
                 'cat_11', 'cat_12', 'cat_13', 'cat_14', 'cat_15', 'cat_16', 'cat_17', 'cat_18', 'cat_19', 'cat_20',
                 'cat_21', 'cat_22', 'cat_23', 'cat_25', 'cat_26', 'cat_27', 'cat_28', 'cat_29', 'cat_35', 'cat_1',
                 'cat_2', 'cat_3', 'cat_31', 'cat_33', 'cat_4', 'cat_5', 'cat_50', 'cat_6', 'cat_7', 'cat_8',
                 'cat_9', 'cat_10'])
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
    batch_size = 500
    # od_s1 = ['DXBCMB','DACAMM', 'DXBCGP', 'EBBCGP', 'MLEAMM', 'AMMDXB', 'CMBDXB', 'DXBAMM', 'DACAMM', 'DXBCGP', 'EBBCGP', 'SLLDAC',
    #         'KTMDXB', 'DACDXB', 'CGPDXB', 'CMBKWI', 'PZUDXB', 'BAHDXB', 'DXBVKO', 'DXBGYD', 'MHDBAH', 'NJFBAH', 'BAHNJF',
    #        'KTMRUH', 'KTMAHB', 'KTMELQ','BOMKWI']
    od_s1 = list(db.JUP_DB_OD_Master.distinct('OD'))
    # od_s1 = list(set(od_s1))
    total_ods = len(od_s1)

    while count_batch < total_ods:
        od_s2 = od_s1[count_batch:count_batch+batch_size]
        print str(count_batch) + " out of " + str(total_ods)
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
        count_batch += batch_size

            # 'MLEAMM', 'AMMDXB', 'DXBAMM', 'DACAMM', 'DXBCGP', 'EBBCGP', 'SLLDAC',
            # 'KTMDXB', 'DACDXB', 'CGPDXB', 'CMBKWI', 'PZUDXB', 'BAHDXB', 'DXBVKO', 'DXBGYD', 'MHDBAH', 'NJFBAH', 'BAHNJF',
           # 'KTMRUH', 'KTMAHB', 'KTMELQ','BOMKWI']

        print "started"
        total_markets = len(final_odlist)
        # print "total_markets: ",total_markets
        fare5 = list(db.JUP_DB_ATPCO_Fares_Rules.aggregate(
            [{"$match": {"OD": {'$in': final_odlist},
                         '$or': [{"effective_to": {'$gte': SYSTEM_DATE}},{"effective_to": None}]}},
             {"$lookup": {"from": "JUP_DB_City_Airport_Mapping", 'localField': "origin", "foreignField": "Airport_Code",
                          "as": "city_mapping"}}, {"$unwind": {"path": "$city_mapping"}}, {
                 "$project": {"OD": 1, "origin": 1, "destination": 1, "pseudo_origin": "$city_mapping.City_Code",
                              "pseudo_destination": "$city_mapping.City_Code", "compartment": 1, "carrier": 1, "cat_101": 1,
                              "cat_102": 1, "cat_103": 1, "cat_104": 1, "cat_105": 1, "cat_106": 1, "cat_107": 1,
                              "cat_108": 1, "cat_109": 1, "cat_11": 1, "cat_12": 1, "cat_13": 1, "cat_14": 1, "cat_15": 1,
                              "cat_16": 1, "cat_17": 1, "cat_18": 1, "cat_19": 1,
                              "cat_20": 1, "cat_21": 1, "cat_22": 1, "cat_23": 1, "cat_25": 1, "cat_26": 1, "cat_27": 1,
                              "cat_28": 1, "cat_29": 1,
                              "cat_35": 1, "cat_1": 1, "cat_2": 1, "cat_3": 1, "cat_31": 1, "cat_33": 1, "cat_4": 1,
                              "cat_5": 1, "cat_50": 1, "cat_6": 1, "cat_7": 1, "cat_8": 1, "cat_9": 1, "cat_10": 1}}, {
                 "$lookup": {"from": "JUP_DB_City_Airport_Mapping", 'localField': "destination",
                             "foreignField": "Airport_Code", "as": "city_mapping"}}
                ,
             {"$unwind": {"path": "$city_mapping"}}, {
                 '$project': {'_id':0,"pseudo_destination": "$city_mapping.City_Code", "pseudo_origin": 1, "OD": 1,
                              "compartment": 1, "carrier": 1, "cat_101": 1,
                              "cat_102": 1, "cat_103": 1, "cat_104": 1, "cat_105": 1, "cat_106": 1, "cat_107": 1,
                              "cat_108": 1, "cat_109": 1, "cat_11": 1, "cat_12": 1, "cat_13": 1, "cat_14": 1, "cat_15": 1,
                              "cat_16": 1, "cat_17": 1, "cat_18": 1, "cat_19": 1,
                              "cat_20": 1, "cat_21": 1, "cat_22": 1, "cat_23": 1, "cat_25": 1, "cat_26": 1, "cat_27": 1,
                              "cat_28": 1, "cat_29": 1,
                              "cat_35": 1, "cat_1": 1, "cat_2": 1, "cat_3": 1, "cat_31": 1, "cat_33": 1, "cat_4": 1,
                              "cat_5": 1, "cat_50": 1, "cat_6": 1, "cat_7": 1, "cat_8": 1, "cat_9": 1, "cat_10": 1}}
             ]))
        lim = []

        fare55 = pd.DataFrame(fare5)
        if len(fare55) != 0:
            # print fare55.head()
            for i in fare55.columns:
                if "cat_" in i:
                    lim.append(i)
            df = fare55[lim]
            for i in fare55.columns:
                if "cat_" in i:
                    fare55 = fare55.drop([i], axis=1)
            # print fare55.head()


            @measure(JUPITER_LOGGER)
            def replace_empty_lists(row):
                try:
                    if len(row) == 0:
                        return np.nan
                    else:
                        return 1
                except:
                    pass
            for col in df.columns:
                # print col
                df[col] = df[col].apply(lambda row: replace_empty_lists(row))

            df2 = pd.concat([df1, df])
            df3 = pd.concat([df2, fare55], axis=1)
            df4 = df3.isnull().sum(axis=1)
            df4 = pd.DataFrame(df4)
            df4.columns = ['a']
            df4['scores'] = 41 - df4['a']
            df4 = df4.drop(['a'], axis=1)
            df4 = pd.concat([fare55, df4], axis=1)
            # print df4.head()
            for i in range(len(df4)):
                df4['pseudo_od'] = df4['pseudo_origin'] + df4['pseudo_destination']

            df4 = df4.drop(['pseudo_origin', 'pseudo_destination'], axis=1)
            cur2 = pd.DataFrame(df4['OD'])
            ood = []
            for k in cur2['OD']:
                ood.append(k)
            ood = list(set(ood))
            # print "ood: ", ood
            df4 = df4.drop(['OD'], axis=1)
            df4 = pd.DataFrame(df4.groupby(['pseudo_od', 'compartment', 'carrier']).mean()).reset_index()
            grouped_df = df4.groupby(by=['pseudo_od', 'compartment'])

            li = []
            val1 = []
            for odc, values in grouped_df:
                # print odc
                val = values['scores'].values
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
                    #             li.append(list_ratings)
                # print "li -->", li
                # li = np.array(li)
                # li = 10 - li
                od_dict = {}
                # print "total docs inserted should be: ", len(grouped_df)*len(ood)
                # print "odc:", odc
                od_dict['origin'] = odc[0][:3]
                od_dict['destination'] = odc[0][3:]
                od_dict['compartment'] = odc[1]
                od_dict['pos'] = odc[0][:3]
                od_dict['group'] = "Fares Rating"
                od_dict['component'] = "Restriction Index"
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