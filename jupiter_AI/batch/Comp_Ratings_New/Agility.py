    # Main Code 2
    # how many documents are replaced(edited/ added/ deleted) last week

from jupiter_AI import JUPITER_DB, client, JUPITER_LOGGER
from jupiter_AI.logutils import measure


@measure(JUPITER_LOGGER)
def run():
    from pymongo import MongoClient
    from pymongo import UpdateOne
    import pandas as pd
    import datetime
    from jupiter_AI.network_level_params import SYSTEM_DATE, SYSTEM_TIME
    # client = MongoClient("mongodb://analytics:KNjSZmiaNUGLmS0Bv2@172.28.23.9:43535/")

    db = client[JUPITER_DB]
    pd.set_option("display.max_columns",100)
    import datetime
    import scipy.stats as ss
    import time

    skips = 0
    # print ln1
    TOLERANCE = 0.001


    @measure(JUPITER_LOGGER)
    def rescale_values(list_series, min_score, max_score):
        """
        given a list of input ratings, and minimum and maximum scores, rescales the list to a list of scores so that lowest score=min_score and highest score=max_score
        """
        series_min=min(list_series)
        series_max=max(list_series)
        series_range=series_max-series_min
        score_range=max_score-min_score
        if abs(series_range) < TOLERANCE:   ## all values in list_series are the same
            return 5, None, None
        slope=score_range/series_range
        intercept=min_score - slope*series_min
        out=[0 for i in list_series]
        for i in range(len(out)):
            out[i]=slope*list_series[i]+intercept
        return out, slope, intercept

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

    date_td = datetime.datetime.now()
    date_lw = date_td - datetime.timedelta(days=7)
    date_td = date_td.strftime("%y%m%d")
    date_lw = date_lw.strftime("%y%m%d")

    od_s1 = list(db.JUP_DB_OD_Master.distinct('OD'))
    # od_s1 = list(set(od_s1))
    total_ods = len(od_s1)

    while count_batch < total_ods:
        od_s2 = od_s1[count_batch:count_batch + batch_size]
        print str(count_batch) + " out of " + str(total_ods)
        ods = []
        print od_s2
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
        print ods
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
        print final_odlist
        print len(final_odlist)
        count_batch += batch_size

        # 'MLEAMM', 'AMMDXB', 'DXBAMM', 'DACAMM', 'DXBCGP', 'EBBCGP', 'SLLDAC',
        # 'KTMDXB', 'DACDXB', 'CGPDXB', 'CMBKWI', 'PZUDXB', 'BAHDXB', 'DXBVKO', 'DXBGYD', 'MHDBAH', 'NJFBAH', 'BAHNJF',
        # 'KTMRUH', 'KTMAHB', 'KTMELQ','BOMKWI']
        try:
            print "started"
            # ods_cursor = db.JUP_DB_ATPCO_Fares_Rules.distinct('OD')
            # ods=list(ods_cursor)
            total_markets = len(final_odlist)
            print "total_markets: ", total_markets
            # temp_markets = final_odlist
            # print temp_markets

            fare6_list = list(db.JUP_DB_ATPCO_Fares_Rules.aggregate(
                    [{"$match": {"OD": {'$in': final_odlist},'$or': [{"effective_to": {'$gte': SYSTEM_DATE}},{"effective_to": None}],
                                                                                 'compartment': {'$in':['Y','J']},'ACTION': 'R', 'FARE_TAGS_14': {'$ne': 'X'}}},
                                               {
                                                   '$project':
                                                       {
                                                           'gfs': 1, '_id': 0, 'OD': 1, 'compartment': 1, 'carrier': 1,
                                                           'yearSubstring': {'$substr': ["$gfs", 0, 2]},
                                                           'monthdaySubtring': {'$substr': ["$gfs", 2, 4]}

                                                       }
                                               },
                                               {"$project": {
                                                   'gfs': 1, 'OD': 1, 'compartment': 1, 'carrier': 1,
                                                   'date': {"$concat": ['$yearSubstring', '$monthdaySubtring']}}
                                               },
                                               {
                                                   "$match": {
                                                       "date": {"$lte": date_td, "$gte": date_lw}
                                                   }
                                               }

                                               ], allowDiskUse=True))

            fare6 = pd.DataFrame(fare6_list)
            del fare6_list
            # fare6 = fare6.drop(['gfs'],axis=1)
            if len(fare6) != 0:
                fare7 = fare6.groupby(['OD', 'carrier', 'compartment'],as_index=False).count()
                # print fare7
                list_ranks = ss.rankdata(fare7['date'])
                min_rank = min(list_ranks)
                max_rank = max(list_ranks)
                max_score = 10.0
                min_score = 0.0
                if len(list_ranks) < 4:
                    max_score = 7.5
                    min_score = 2.5
                list_ratings, _, _ = rescale_values(list_ranks, max_score,
                                                    min_score)  ## reverse order - note that min and max are specified in reverse
                fare7['ratings'] = list_ratings
                fare7 = fare7.drop(['date','gfs'], axis=1)
                print fare7.shape
                print fare7.head()
                # fare7 = pd.DataFrame(fare7.groupby(['OD', 'compartment', 'carrier']).mean()).reset_index()
                grouped_df = fare7.groupby(by=['OD', 'compartment'])

                inserted = 0
                for od_c, values in grouped_df:
                    od_dict = {}
                    od_dict['origin'] = od_c[0][:3]
                    od_dict['destination'] = od_c[0][3:]
                    od_dict['compartment'] = od_c[1]
                    od_dict['group'] = "Fares Rating"
                    od_dict['component'] = "Price Agility Index"
                    od_dict['last_update_date_gmt'] = SYSTEM_DATE+" "+ SYSTEM_TIME
                    carrier_list = values['carrier']
                    ratings_list = values['ratings']
                    zipped_carrier = zip(carrier_list, ratings_list)
                    agent_ratings = []
                    temp_dict = {}
                    for carrier in zipped_carrier:
                        temp_dict['' + str(carrier[0]) + ''] = carrier[1]
                        #         temp_dict['agent'] = carrier[1]
                        #         temp_dict['main'] = str(+str(carrier[0])+ ":" +str(carrier[1])+)
                        # agent_ratings.append(temp_dict)
                    od_dict['ratings'] = temp_dict
                    total.append(od_dict)

                    if t1 == 200:
                        print total
                        db.JUP_DB_Data_Competitor_Ratings.insert_many(total)
                        total = []
                        total.append(od_dict)
                        st = time.time()
                        print "updating: ", count1
                        print "updated!", time.time() - st
                        t1 = 0
                    else:
                        total.append(od_dict)
                        t1 += 1
                        count1 += 1

                if t1 > 0:
                    print total
                    db.JUP_DB_Data_Competitor_Ratings.insert_many(total)
                    print "inserted 2nd"
                    st = time.time()
                    print "updating: ", count1
                    print "updated!", time.time() - st
                    total = []
                    t1 = 0
            count_batch += batch_size
                # except:
            #     print "temp_markets not present: ", temp_markets
            #     count_batch += batch_size
            #     pass
        except:
            print "od's not present: ", od_s1
            count_batch += batch_size
            pass

if __name__ == "__main__":
    run()