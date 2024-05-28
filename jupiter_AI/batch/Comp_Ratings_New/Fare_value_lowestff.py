import pandas as pd
import pymongo
from jupiter_AI import JUPITER_DB, client, JUPITER_LOGGER
from jupiter_AI.logutils import measure
db = client[JUPITER_DB]
pd.set_option("display.max_columns",100)
import datetime
import scipy.stats as ss
import time

TOLERANCE=0.001


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
batch_size = 1


od_s1 = ['TBSDWC', 'GYDDWC','DXBCMB', 'CMBDXB', 'MLEAMM', 'AMMDXB', 'DXBAMM', 'DACAMM', 'DXBCGP', 'EBBCGP', 'SLLDAC',
                                                       'KTMDXB', 'DACDXB', 'CGPDXB', 'CMBKWI', 'PZUDXB', 'BAHDXB', 'DXBVKO', 'DXBGYD', 'MHDBAH', 'NJFBAH', 'BAHNJF',
                                                       'KTMRUH', 'KTMAHB', 'KTMELQ','BOMKWI']

total_ods = len(od_s1)

while count_batch < total_ods:
    od_s2 = od_s1[count_batch:count_batch+batch_size]
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
        print "total_markets: ",total_markets
        # temp_markets = final_odlist
        # print temp_markets
        fare5 = list(db.JUP_DB_ATPCO_Fares_Rules.aggregate())

    except:
        pass

print "started"
total_markets = len(ods)
while count_batch < total_markets:
    try:
        print str(count_batch) + " out of " + str(total_markets)
        temp_markets = ods[count_batch: count_batch + batch_size]
                     # .skip(skips).limit(500000))
        print temp_markets
        cur_1 = db.JUP_DB_Manual_Triggers_Module.find({'od': {'$in': temp_markets}, 'compartment.compartment': {'$in':['Y','J']},'dep_month':3, 'dep_year':SYSTEM_DATE[0:4]},{'_id':0, 'od':1, 'compartment.compartment':1,
                                                       'lowestFare':1})

        # print count #'popular_fare':1
        cur1 = pd.DataFrame(list(cur_1))
        print cur1.head
        cur = cur1.groupby(['od', 'compartment', ''], as_index=False).count()
        print cur1.shape
        print cur1

        list_ranks = ss.rankdata(mart1['agent'])
        min_rank = min(list_ranks)
        max_rank = max(list_ranks)
        max_score = 10.0
        min_score = 0.0
        if len(list_ranks) < 4:
            max_score = 7.5
            min_score = 2.5
        list_ratings, _, _ = rescale_values(list_ranks, max_score,
                                            min_score)  ## reverse order - note that min and max are specified in reverse
        mart1['ratings'] = list_ratings
        mart1 = mart1.drop(['agent'], axis=1)
        grouped_df = mart1.groupby(by=['od', 'compartment'])

        for od_c, values in grouped_df:
            od_dict = {}
            od_dict['origin'] = od_c[0][:3]
            od_dict['destination'] = od_c[0][3:]
            od_dict['compartment'] = od_c[1]
            od_dict['group'] = "Distributors Rating"
            od_dict['component'] = "Number of channels/distributors"
            od_dict['last_update_date_gmt'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            carrier_list = values['MarketingCarrier1']
            ratings_list = values['ratings']
            zipped_carrier = zip(carrier_list, ratings_list)
            # agent_ratings = []
            temp_dict = {}
            for carrier in zipped_carrier:
                temp_dict['' + str(carrier[0]) + ''] = carrier[1]
                #         temp_dict['agent'] = carrier[1]
                #         temp_dict['main'] = str(+str(carrier[0])+ ":" +str(carrier[1])+)
                # agent_ratings.append(temp_dict)
            od_dict['ratings'] = temp_dict

            # import insert_many



            if t1 == 200:
                # print total
                # db.JUP_DB_Data_Competitor_Ratings.insert_many(total)
                # total = []
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
            # db.JUP_DB_Data_Competitor_Ratings.insert_many(total)
            print "inserted 2nd"
            st = time.time()
            print "updating: ", count1
            print "updated!", time.time() - st
            total = []
            t1 = 0

        count_batch += batch_size

    except:
        print "temp_markets not present: ", temp_markets
        count_batch += batch_size
        pass
