# Agents
from jupiter_AI import JUPITER_DB, client, JUPITER_LOGGER
from jupiter_AI.logutils import measure


@measure(JUPITER_LOGGER)
def run():
    from pymongo import MongoClient
    from pymongo import UpdateOne
    import pandas as pd
    import numpy as np
    from jupiter_AI.network_level_params import SYSTEM_DATE, SYSTEM_TIME

    from jupiter_AI import JUPITER_DB, client
    db = client[JUPITER_DB]

    import datetime
    import scipy.stats as ss
    import time
    from pymongo.errors import BulkWriteError

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

    count_batch = 0
    # batch_size = 100
    batch_size = 500
    total = []
    updated = 0
    inserted = 0
    bulk_list = []
    t1 = 0
    count1 = 0
    count2 = 1

    # od_s1 = ['DXBCMB', 'CMBDXB', 'MLEAMM', 'AMMDXB', 'DXBAMM', 'DACAMM', 'DXBCGP', 'EBBCGP', 'SLLDAC', 'KTMDXB', 'DACDXB', 'CGPDXB', 'CMBKWI',
    #        'PZUDXB', 'BAHDXB', 'DXBVKO', 'DXBGYD', 'MHDBAH', 'NJFBAH', 'BAHNJF',
    #                                                   'KTMRUH', 'KTMAHB', 'KTMELQ','BOMKWI']
    od_s1 = list(db.JUP_DB_OD_Master.distinct('OD'))
    total_ods = len(od_s1)

    while count_batch < total_ods:
        od_s2 = od_s1[count_batch:count_batch+batch_size]
        # print str(count_batch) + " out of " + str(total_ods)
        ods = []
        # print od_s2
        city_airport = list(db.JUP_DB_City_Airport_Mapping.aggregate([{'$project': {'_id': 0, 'City_Code': 1, 'Airport_Code': 1}}]))
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
            # print len(airport_code_ori_list), len(airport_code_dest_list)

            for m in airport_code_ori_list:
                for n in airport_code_dest_list:
                    final_odlist.append(m + n)
                    #     if l not in final_odlist:
        # final_odlist.append(l)
        final_odlist = list(set(final_odlist))
        # print final_odlist
        # print len(final_odlist)
            # 'MLEAMM', 'AMMDXB', 'DXBAMM', 'DACAMM', 'DXBCGP', 'EBBCGP', 'SLLDAC',
            # 'KTMDXB', 'DACDXB', 'CGPDXB', 'CMBKWI', 'PZUDXB', 'BAHDXB', 'DXBVKO', 'DXBGYD', 'MHDBAH', 'NJFBAH', 'BAHNJF',
            # 'KTMRUH', 'KTMAHB', 'KTMELQ','BOMKWI']
        print "started"
        total_markets = len(final_odlist)
        # try:'year':SYSTEM_DATE[0:4]

        mart_1 = list(db.JUP_DB_Market_Share_Last.aggregate([{"$match":{"od":{'$in':final_odlist},'month': SYSTEM_DATE[5:7],'year':SYSTEM_DATE[0:4],'compartment':{'$in':['Y','J']}}},
                 {"$lookup":{"from":"JUP_DB_City_Airport_Mapping", 'localField':"origin", "foreignField":"Airport_Code", "as":"city_mapping"}},
                 {"$unwind":{"path":"$city_mapping"}},
                 {"$project":{"od":1,"origin":1, "destination":1, "pseudo_origin":"$city_mapping.City_Code","_id": 0, "MarketingCarrier1": 1, "agent": 1,"compartment":1,"pos":1}},
                 {"$lookup":{"from":"JUP_DB_City_Airport_Mapping", 'localField':"destination", "foreignField":"Airport_Code", "as":"city_mapping"}},
                 {"$unwind":{"path":"$city_mapping"}},
                 {"$project":{"od":1, "pseudo_destination":"$city_mapping.City_Code", "pseudo_origin":1,"_id": 0, "MarketingCarrier1": 1,"pos":1, "compartment":1, "agent": 1}}]))

        mart = pd.DataFrame((mart_1))
        del mart_1
        count = len(mart)
        # print count
        # print mart.head()
        #         mar2 = pd.concat([mart, mar])
        #         print mar2.head()
        #         mart1 = mart.groupby(['od', 'compartment', 'MarketingCarrier1'], as_index=False).count()
        # print mart.shape
        if len(mart) != 0:
            cur2 = pd.DataFrame(mart['od'])
            ood = []
            for k in cur2['od']:
                ood.append(k)
            ood = list(set(ood))
            # print "ood: ", ood

            mart['pseudo_od']=mart['pseudo_origin']+mart['pseudo_destination']
            mart = mart.drop(['od','pseudo_origin', 'pseudo_destination'], axis=1)
            # print mart.head()
            grouped_df = mart.groupby(by=['pseudo_od', 'compartment','pos'])
            for od_c, values in grouped_df:
                # print od_c
                # print values
                #             print values['agent']
                val = values['agent']
                # print "val: ", val
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
                li = list_ratings
                # print "li", li
                try:
                    len_li = len(li)
                except Exception:
                    li = [list_ratings]
                # li.append(list_ratings)
                li = np.array(li)
                li = 10 - li
                # print "li -->", li
                # print "Inserted docs should be: ", len(grouped_df)*len(ood)

                #             mart1 = mart1.drop(['agent'], axis=1)
                #         grouped_df = mart1.groupby(by=['od', 'compartment'])
                od_dict = {}
                od_dict['origin'] = od_c[0][:3]
                od_dict['destination'] = od_c[0][3:]
                od_dict['compartment'] = od_c[1]
                od_dict['pos'] = od_c[2]
                od_dict['group'] = "Distributors Rating"
                od_dict['component'] = "Number of channels/distributors"
                od_dict['last_update_date_gmt'] = SYSTEM_DATE+" "+ SYSTEM_TIME
                carrier_list = values['MarketingCarrier1']
                ratings_list = li
                for i in range(len(ratings_list)):
                    ratings_list[i]=10.0-ratings_list[i]

                # print carrier_list
                zipped_carrier = zip(carrier_list, ratings_list)
                # print carrier_list
                # print ratings_list
                # agent_ratings = []
                temp_dict = {}
                for carrier in zipped_carrier:
                    temp_dict['' + str(carrier[0]) + ''] = carrier[1]
                    #         temp_dict['agent'] = carrier[1]
                    #         temp_dict['main'] = str(+str(carrier[0])+ ":" +str(carrier[1])+)
                    # agent_ratings.append(temp_dict)
                od_dict['ratings'] = temp_dict
                # print od_dict
                od_in = od_dict['origin'] + od_dict['destination']
                # print od_in
                # print "od_dict: ", od_dict
                #
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
                print len(airport_code_ori_list), len(airport_code_dest_list)

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
        # except:
        #     count_batch += batch_size
        #     pass

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
    # if
    run()