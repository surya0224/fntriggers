from pymongo import MongoClient
from pymongo import UpdateOne
from bson import ObjectId
import pandas as pd
import numpy as np
# client = MongoClient("mongodb://analytics:KNjSZmiaNUGLmS0Bv2@13.92.251.7:42525/")
# db = client.fzDB_stg
from jupiter_AI import JUPITER_DB, client
db = client[JUPITER_DB]
import datetime
import scipy.stats as ss
import time
t1=0
skips = 0
limits = 50
bulk_list = []
# loop_cou = list(db.JUP_DB_ATPCO_Fares_Rules.find({'$or': [{"effective_to": {'$gte': '2018-03-15'}},{"effective_to": None}]}))
# loop_coun = 5157780
loop_coun = 100
loop_count = int(loop_coun/limits)
loop_count = loop_count+2
city_airport = list(
            db.JUP_DB_City_Airport_Mapping.aggregate([{'$project': {'_id': 0, 'City_Code': 1, 'Airport_Code': 1}}]))
for turns in range(1,loop_count):
    print "loop started"
    fare6 = list(db.Fare_rules_test.find({'$or': [{"effective_to": {'$gte': '2018-01-15'}},{"effective_to": None}],
                             },{'OD':1,'origin':1,'destination':1}))
    for i in range(len(fare6)):
        for l in city_airport:
            if fare6[i]['origin'] == l['Airport_Code']:
                fare6[i]['pseudo_origin'] = l['City_Code']
            if fare6[i]['destination'] == l['Airport_Code']:
                fare6[i]['pseudo_destination'] = l['City_Code']
    f2=pd.DataFrame(fare6)
    try:
        f2['pseudo_origin']
    except:
        f2['pseudo_origin'] = np.nan
    try:
        f2['pseudo_destination']
    except:
        f2['pseudo_destination'] = np.nan
    try:
        print f2['pseudo_od']
    except:
        f2['pseudo_od']=np.nan
    # f2.columns = ['OD', '_id', 'origin', 'destination', 'pseudo_origin', 'pseudo_destination', 'pseudo_od']
    f2['pseudo_origin'].fillna(0,inplace=True)
    f2['pseudo_destination'].fillna(0,inplace=True)
    for i in range(len(f2)):
        if f2['pseudo_origin'][i]==0:
            f2['pseudo_origin'][i] = f2['origin'][i]

    for i in range(len(f2)):
        if f2['pseudo_destination'][i]==0:
            f2['pseudo_destination'][i] = f2['destination'][i]

    print f2.head()
    for i in range(len(f2)):
        f2['pseudo_od'][i] = f2['pseudo_origin'][i] + f2['pseudo_destination'][i]

    for j in range(len(f2)):
        if t1 == 10:
            st = time.time()
            db['Fare_rules_test'].bulk_write(bulk_list)
            print "updated!", time.time() - st
            bulk_list = []
            bulk_list.append(
                UpdateOne({"_id": ObjectId(f2['_id'][j])}, {'$set': {'pseudo_od': f2['pseudo_od'][j],'pseudo_origin': f2['pseudo_origin'][j],'pseudo_destination': f2['pseudo_destination'][j]}}))
            t1 = 0
        else:
            bulk_list.append(
                UpdateOne({"_id": ObjectId(f2['_id'][j])}, {'$set': {'pseudo_od': f2['pseudo_od'][j],'pseudo_origin': f2['pseudo_origin'][j],'pseudo_destination': f2['pseudo_destination'][j]}}))
            t1 += 1
    skips = skips + 50
    # print "-------"
    # print bulk_list
if len(bulk_list) != 0:
    db['Fare_rules_test'].bulk_write(bulk_list)

