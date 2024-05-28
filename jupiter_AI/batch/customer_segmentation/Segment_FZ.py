"""
Author: Prathyusha Gontla
End date of developement: 2018-02-01
Code functionality:
     Updates collection JUP_DB_Sales with segments_fz, segments asked by client everyday.
     Uses base collections - Sales, Pricing Calendar

MODIFICATIONS LOG
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :

"""
from jupiter_AI import JUPITER_LOGGER
from jupiter_AI.logutils import measure


@measure(JUPITER_LOGGER)
def run():
    # updating segment_fz
    from pymongo import MongoClient
    from pymongo import UpdateOne
    from pymongo import UpdateOne
    from bson import ObjectId
    import time
    import datetime
    from datetime import datetime, timedelta
    import pandas as pd
    from jupiter_AI import JUPITER_DB, client, Host_Airline_Code, Host_Airline_Hub
    db = client[JUPITER_DB]
    from jupiter_AI.network_level_params import SYSTEM_DATE

    # client = MongoClient("mongodb://analytics:KNjSZmiaNUGLmS0Bv2@172.28.23.9:43535/")
    # db = client.fzDB_stg
    conn = client[JUPITER_DB]
    print SYSTEM_DATE
    yest = datetime.strptime(SYSTEM_DATE,"%Y-%m-%d") - timedelta(days=1)
    # print yest
    yesterday= yest.strftime("%Y-%m-%d")
    # print yesterday
    bulk_list = []
    count = 1
    t = 0


    @measure(JUPITER_LOGGER)
    def segments(row):
        cursor = row
        try:
            if ((cursor['origin'] == 'DXB' and cursor['destination'] == 'JED') or
                    (cursor['origin'] == 'JED' and cursor['destination'] == 'DXB') or
                    (cursor['origin'] == 'TIF' and cursor['destination'] == 'DXB') or
                    (cursor['origin'] == 'MED' and cursor['destination'] == 'DXB')) and (
                                '2017-09-04' <= cursor['dep_date'] <= '2017-10-04' or
                                '2018-08-25' <= cursor['dep_date'] <= '2018-09-25'):
                return "Hajj"

            elif cursor['origin'] == 'KTM' and cursor['pos'] == 'KTM' and cursor['fare_basis'][:1] == 'G' and cursor[
                                                                                                                  'fare_basis'][
                                                                                                              1:2] == 'O':
                return "Labor"
            elif cursor['fare_basis'][-1] == '3' or cursor['iata_number'] == '7001285':
                return "Corporate"
            # elif cursor['origin'] in ['BHV', 'CJL', 'CHB', 'DBA', 'DEA', 'DSK', 'LYP', 'GIL', 'GJT', 'GWD', 'HDD', 'ISB'
            #     , 'JAG', 'JIW', 'KCF', 'KHI', 'KDD', 'LHE', 'XJM', 'MJD', 'MUX', 'MFG', 'WNS', 'ORW', 'PJG'
            #     , 'PAJ', 'PSI', 'PEW', 'UET', 'RYK', 'RAZ', 'SKT', 'RZS', 'SDT', 'SYW', 'SBQ', 'MPD', 'KDU', 'SUL',
            #                           'SKZ', 'TLB', 'TUK', 'PZH'] and cursor['pos'] in ['BHV', 'CJL', 'CHB', 'DBA', 'DEA',
            #                                                                             'DSK', 'LYP', 'GIL', 'GJT', 'GWD',
            #                                                                             'HDD', 'ISB'
            #     , 'JAG', 'JIW', 'KCF', 'KHI', 'KDD', 'LHE', 'XJM', 'MJD', 'MUX', 'MFG', 'WNS', 'ORW', 'PJG'
            #     , 'PAJ', 'PSI', 'PEW', 'UET', 'RYK', 'RAZ', 'SKT', 'RZS', 'SDT', 'SYW', 'SBQ', 'MPD', 'KDU', 'SUL',
            #                                                                             'SKZ', 'TLB', 'TUK', 'PZH'] and \
            #                 cursor['destination'] in ['JED', 'MED'] and ('2017-08-30' <= cursor['dep_date'] <= '2017-09-04'
            #     or '2018-01-01' <= cursor['dep_date'] <= '2018-06-30') and cursor['fare_basis'][1:2] == 'R':
            #     return "Umrah"
        except:
            pass

    skips = 0
    limits = 100000
    #'$gt':yesterday
    loop_coun = db.JUP_DB_Sales.find({'snap_date':{'$gt':yesterday},'origin':{'$in':['DXB','KTM','TIF','JED','MED']},'pos':{'$in':['KTM','UAE']},'destination':{'$in':['JED','DXB','MED']}}).count()
    loop_count = int(loop_coun/limits)
    loop_count = loop_count+2
    #for 28.5 mn, I write 1,30 in for loop.
    for turns in range(1,loop_count):
        sales_data = db.JUP_DB_Sales.find({'snap_date':{'$gt':yesterday},'origin':{'$in':['DXB','KTM','TIF','JED','MED']},'pos':{'$in':['KTM','UAE']},'destination':{'$in':['JED','DXB','MED']}},
                                          {'dep_date': 1,'od': 1, 'month': 1, 'origin': 1,
                                                             'destination': 1, 'pos': 1, 'iata_number': 1, 'fare_basis': 1},no_cursor_timeout=True).skip(skips).limit(limits)
        cursor = pd.DataFrame(list(sales_data))
        print cursor.head()
        cursor['segment_fz'] = cursor.apply(lambda row: segments(row), axis=1)
        print "-------->>>>>>>>>>>>"
        print cursor.head()
        print "segments are done.. updating into collection will start"

        for j in range(len(cursor)):
            if t == 1000:
                st = time.time()
                print "updating: ", count
                conn['JUP_DB_Sales'].bulk_write(bulk_list)
                print "updated!", time.time() - st
                bulk_list = []
                t = 0
                bulk_list.append(
                    UpdateOne({"_id": ObjectId(cursor['_id'][j])}, {'$set': {'segment_fz': cursor['segment_fz'][j]}}))
                t += 1
                count += 1
            else:
                bulk_list.append(
                    UpdateOne({"_id": ObjectId(cursor['_id'][j])}, {'$set': {'segment_fz': cursor['segment_fz'][j]}}))
                t += 1
                # print "t: ", t
        if len(bulk_list) != 0:
            st = time.time()
            conn['JUP_DB_Sales'].bulk_write(bulk_list)
            print "updated!---", time.time() - st
        skips = skips + 100000
        print "updated sales records: ", skips

    sales_data.close()

    #segments in sales_flown will start

    bulk_list = []
    count = 1
    t = 0

    skips = 0
    limits = 100000

    loop_coun = db.JUP_DB_Sales_Flown.find({'snap_date':{'$gt':yesterday},'origin':{'$in':['DXB','KTM','TIF','JED','MED']},'pos':{'$in':['KTM','UAE']},'destination':{'$in':['JED','DXB','MED']}}).count()
    loop_count = int(loop_coun/limits)
    loop_count = loop_count+2
    print loop_count
    #for 28.5 mn, I write 1,30 in for loop.
    for turns in range(1,loop_count):
        salesf_data = db.JUP_DB_Sales_Flown.find({'snap_date':{'$gt':yesterday},'origin':{'$in':['DXB','KTM','TIF','JED','MED']},'pos':{'$in':['KTM','UAE']},'destination':{'$in':['JED','DXB','MED']}}, {'dep_date': 1,
                                                             'od': 1, 'month': 1, 'origin': 1,
                                                             'destination': 1, 'pos': 1, 'iata_number': 1, 'fare_basis': 1},no_cursor_timeout=True).skip(skips).limit(limits)
        cursor = pd.DataFrame(list(salesf_data))
        print cursor.head()
        cursor['segment_fz'] = cursor.apply(lambda row: segments(row), axis=1)
        print "-------->>>>>>>>>>>>"
        print cursor.head()
        print "segments_flown are done.. updating into collection will start"

        for j in range(len(cursor)):
            if t == 1000:
                st = time.time()
                print "updating: ", count
                conn['JUP_DB_Sales_Flown'].bulk_write(bulk_list)
                print "updated!", time.time() - st
                bulk_list = []
                t = 0
                bulk_list.append(
                    UpdateOne({"_id": ObjectId(cursor['_id'][j])}, {'$set': {'segment_fz': cursor['segment_fz'][j]}}))
                t += 1
                count += 1
            else:
                bulk_list.append(
                    UpdateOne({"_id": ObjectId(cursor['_id'][j])}, {'$set': {'segment_fz': cursor['segment_fz'][j]}}))
                t += 1
                # print "t: ", t
        if len(bulk_list) != 0:
            st = time.time()
            conn['JUP_DB_Sales_Flown'].bulk_write(bulk_list)
            print "updated!---", time.time() - st
        skips = skips + 100000
        print "updated sales_flown records: ", skips

    salesf_data.close()

if __name__=="__main__":
    run()
