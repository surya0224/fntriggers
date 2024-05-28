from jupiter_AI import JUPITER_LOGGER
from jupiter_AI.logutils import measure


@measure(JUPITER_LOGGER)
def run():

    # import jupiter_AI.common.ClassErrorObject as error_class
    from pymongo.errors import BulkWriteError
    import json
    import numpy as np
    import datetime
    import time
    # from jupiter_AI import network_level_params as net
    import inspect
    import scipy.stats as ss
    import pandas as pd
    import collections
    from jupiter_AI.network_level_params import SYSTEM_TIME,SYSTEM_DATE
    import pymongo
    from bson.objectid import ObjectId
    from time import gmtime, strftime
    from pymongo import MongoClient
    from jupiter_AI import JUPITER_DB, client
    db = client[JUPITER_DB]
    # od_s1 = ['CMBDXB']#,'DXBCMB','MLEAMM', 'AMMDXB', 'DXBAMM', 'DACAMM', 'DXBCGP', 'EBBCGP', 'SLLDAC',
    # 'KTMDXB', 'DACDXB', 'CGPDXB', 'CMBKWI', 'PZUDXB', 'BAHDXB', 'DXBVKO', 'DXBGYD', 'MHDBAH', 'NJFBAH', 'BAHNJF',
    # 'KTMRUH', 'KTMAHB', 'KTMELQ','BOMKWI']
    od_s1 = list(db.JUP_DB_OD_Master.distinct('OD'))
    # od_s1 = od_s[:25]
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
    # temp_markets1 = ['UAEAMM', 'DACAMM']

    total_markets = len(ods)
    while count_batch < total_markets:
        print str(count_batch) + " out of " + str(total_markets)
        temp_markets = ods[count_batch: count_batch + batch_size]
        # fare5 = list(db.JUP_DB_ATPCO_Fares_Rules.find({'OD': {'$in': temp_markets}, 'compartment': {'$in': ['Y', 'J']}}))
        print temp_markets

        cur1 = pd.DataFrame(list(db.JUP_DB_Pos_OD_Compartment_new.aggregate([
                    {
                        '$match':
                            {'pseudo_od': {'$in': temp_markets},'month':3,  'year':2017

                             }
                    },
                    {'$unwind': "$top_5_comp_data"},
                    {'$match': {'top_5_comp_data.pax': {'$gt': 0}}},
                    {
                        '$group':
                            {
                                '_id':
                                    {'pseudo_od': "$pseudo_od",
                                     'compartment': "$compartment",
                                     'pos':"$pos",
                                     'airline': "$top_5_comp_data.airline"

                                     },
                                'original_ods': {'$addToSet': '$od'},
                                'pax': {'$sum': "$top_5_comp_data.pax"}
                            }

                    },
                    {'$unwind': "$original_ods"

                     }
                ])))

        print cur1.head()
        print len(cur1)

        if len(cur1) !=0:
            cur1['dict'] = cur1['_id']
            #df[col] = df[col].apply(lambda row: replace_empty_lists(row))
            cur1['pseudo_od'] = cur1.dict.apply(lambda row: row['pseudo_od'])
            cur1['compartment'] = cur1.dict.apply(lambda row: row['compartment'])
            cur1['airline'] = cur1.dict.apply(lambda row: row['airline'])
            cur1['pos'] = cur1.dict.apply(lambda row: row['pos'])
            cur1 = cur1.drop(['dict', '_id'], axis=1)
            cur2 = pd.DataFrame(cur1['original_ods'])
            ood = []
            for k in cur2['original_ods']:
                ood.append(k)
            ood = list(set(ood))
            print "ood: ", ood
            cur2['pseudo_od'] = cur1['pseudo_od']
            cur1 = cur1.drop(['original_ods'], axis=1)
            print cur1.head()
            grouped_df = cur1.groupby(by=['pseudo_od', 'compartment','pos'])
            li = []
            val1 = []
            for odc, values in grouped_df:
                val = values['pax'].values
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

                print "list_ratings:", list_ratings
                li = list_ratings
                try:
                    len_li = len(li)
                except Exception:
                    li = [list_ratings]
                for i in range(len(li)):
                    li[i] = 10.0 - li[i]

                    #             li.append(list_ratings)
                print "li -->", li
                # li = np.array(li)
                # li = 10 - li
                od_dict = {}
                print "odc:", odc
                od_dict['origin'] = odc[0][:3]
                od_dict['destination'] = odc[0][3:]
                od_dict['compartment'] = odc[1]
                od_dict['pos'] = odc[2]
                od_dict['group'] = "Market Rating"
                od_dict['component'] = "Market Share"
                od_dict['last_update_date_gmt'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                carrier_list = values['airline']
                print values['airline']
                #             print carrier_list
                # print od_dict
                ratings_list = li
                print ratings_list
                print "carrier list", carrier_list.values
                zipped_carrier = zip(carrier_list.values, ratings_list)
                agent_ratings = []
                temp_dict = {}
                for carrier in zipped_carrier:
                    # print "-- carrier", carrier
                    temp_dict['' + str(carrier[0]) + ''] = carrier[1]
                    # print "temp:", temp_dict
                    #         temp_dict['agent'] = carrier[1]
                    #         temp_dict['main'] = str(+str(carrier[0])+ ":" +str(carrier[1])+)
                    # agent_ratings.append(temp_dict)
                od_dict['ratings'] = temp_dict
                od_in = od_dict['origin'] + od_dict['destination']
                print od_in
                print "od_dict: ", od_dict
                airport_code_ori_list=[]
                airport_code_dest_list=[]
                print "total no. od docs: ", len(grouped_df)*len(ood)
                for x in city_airport:
                    origin = od_dict['origin']
                    destination = od_dict['destination']
                    if origin == x['City_Code']:
                        airport_code_ori_list.append(x['Airport_Code'])
                    if destination == x['City_Code']:
                        airport_code_dest_list.append(x['Airport_Code'])
                print airport_code_ori_list, airport_code_dest_list
                print len(airport_code_ori_list), len(airport_code_dest_list)
                for m in airport_code_ori_list:
                    for n in airport_code_dest_list:
                        od_in = m+n
                        print od_in
                        if od_in in ood:
                            print "od_in list: ", od_in
                            od_dict['origin'] = od_in[:3]
                            od_dict['destination']=od_in[3:]

                            print "od_dict----:", od_dict

                            print "done"
                            total.append(od_dict)

        # count_batch += batch_size
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