from jupiter_AI import JUPITER_DB, client, JUPITER_LOGGER
from jupiter_AI.logutils import measure


@measure(JUPITER_LOGGER)
def run():

    from pymongo.errors import BulkWriteError
    import json
    import numpy as np
    import datetime
    import time
    # from jupiter_AI import network_level_params as net
    from jupiter_AI.network_level_params import SYSTEM_DATE, SYSTEM_TIME
    import inspect
    import scipy.stats as ss
    import pandas as pd
    import collections
    import pymongo
    from bson.objectid import ObjectId
    # from datetime import datetime
    from time import gmtime, strftime
    from pymongo import MongoClient
    db = client[JUPITER_DB]
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

    # od_s1 = ['CMBDXB','DXBCMB','MLEAMM', 'AMMDXB']

    od_s1 = list(db.JUP_DB_OD_Master.distinct('OD'))
    print od_s1
    print len(od_s1)
    ods = []
    city_airport = list(db.JUP_DB_City_Airport_Mapping.aggregate([{'$project': {'_id': 0, 'City_Code': 1, 'Airport_Code': 1}}]))
    for oridest in od_s1:
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
    print ods

    t1 = 0
    total = []
    updated = 0
    bulk_list = []
    count1 = 0
    count2 = 1
    t2 = 0
    li = 0
    count_batch = 0
    # batch_size = 100
    batch_size = 500

    total_markets = len(ods)
    while count_batch < total_markets:
        temp_markets = ods[count_batch: count_batch + batch_size]
        cur1 = pd.DataFrame(list(db.JUP_DB_Competitor_Capacity.find({"pseudo_od":{'$in':ods},"month_year":SYSTEM_DATE[0:4]+SYSTEM_DATE[5:7]}
                                                                 ,{'od':1,'od_freq':1,'pseudo_od':1,'compartment':1,'carrier':1,'_id':0 })))

        # print cur1.head()
        if len(cur1) != 0:
            cur2 = pd.DataFrame(cur1['od'])
            ood = []
            for k in cur2['od']:
                ood.append(k)
            ood = list(set(ood))
            # print "ood: ", ood
            # cur2['pseudo_od'] = cur1['pseudo_od']
            cur1 = cur1.drop(['od'], axis=1)
            print cur1.head()
            cu3 = pd.DataFrame(cur1.groupby(['pseudo_od', 'compartment', 'carrier']).mean()).reset_index()
            grouped_df = cu3.groupby(by=['pseudo_od', 'compartment'])

            li = []
            val1 = []
            for odc, values in grouped_df:
                val = values['od_freq'].values
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

                # print "list_ratings:", list_ratings
                li = list_ratings
                try:
                    len_li = len(li)
                except Exception:
                    li = [list_ratings]
                for i in range(len(li)):
                    li[i] = 10.0 - li[i]
                # print "li -->", li
                od_dict = {}
                # print "odc:", odc
                od_dict['origin'] = odc[0][:3]
                od_dict['destination'] = odc[0][3:]
                od_dict['compartment'] = odc[1]
                od_dict['group'] = "Capacity/Schedule"
                od_dict['component'] = "Frequency"
                od_dict['last_update_date_gmt'] = SYSTEM_DATE + " " + SYSTEM_TIME
                carrier_list = values['carrier']
                ratings_list = li
                zipped_carrier = zip(carrier_list.values, ratings_list)
                agent_ratings = []
                temp_dict = {}
                for carrier in zipped_carrier:
                    temp_dict['' + str(carrier[0]) + ''] = carrier[1]
                od_dict['ratings'] = temp_dict
                od_in = od_dict['origin'] + od_dict['destination']
                # print od_in
                # print "od_dict: ", od_dict
                airport_code_ori_list = []
                airport_code_dest_list = []
                print "total no. od docs: ", len(grouped_df) * len(ood)
                for x in city_airport:
                    origin = od_dict['origin']
                    destination = od_dict['destination']
                    if origin == x['City_Code']:
                        airport_code_ori_list.append(x['Airport_Code'])
                    if destination == x['City_Code']:
                        airport_code_dest_list.append(x['Airport_Code'])
                # print airport_code_ori_list, airport_code_dest_list
                # print len(airport_code_ori_list), len(airport_code_dest_list)
                for m in airport_code_ori_list:
                    for n in airport_code_dest_list:
                        od_in = m + n
                        print od_in
                        if od_in in ood:
                            print "od_in list: ", od_in
                            od_dict['origin'] = od_in[:3]
                            od_dict['destination'] = od_in[3:]

                            print "od_dict----:", od_dict

                            print "done"
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
                print "////////", li
                final_list.append(li)
        print "############# ", final_list
        try:
            db.JUP_DB_Data_Competitor_Ratings.insert_many(final_list)
        except:
            pass
        initial = initial + sets1
        sets1 += sets1

if __name__ == "__main__":
    run()