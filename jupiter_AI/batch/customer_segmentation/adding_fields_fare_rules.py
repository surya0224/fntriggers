from pymongo import MongoClient
from pymongo import UpdateOne
from bson import ObjectId
import pandas as pd
import numpy as np
client = MongoClient("mongodb://analytics:KNjSZmiaNUGLmS0Bv2@13.92.251.7:42525/")
db = client.fzDB_stg
# from jupiter_AI import JUPITER_DB, client
# db = client[JUPITER_DB]
import datetime
import scipy.stats as ss
import time
df1 = pd.DataFrame(columns=['cat_101', 'cat_102', 'cat_103', 'cat_104', 'cat_105', 'cat_106', 'cat_107', 'cat_108','cat_109','cat_11', 'cat_12', 'cat_13', 'cat_14', 'cat_15', 'cat_16', 'cat_17', 'cat_18', 'cat_19', 'cat_20','cat_21', 'cat_22', 'cat_23', 'cat_25', 'cat_26', 'cat_27', 'cat_28', 'cat_29', 'cat_35', 'cat_1','cat_2', 'cat_3', 'cat_31', 'cat_33', 'cat_4', 'cat_5', 'cat_50', 'cat_6', 'cat_7', 'cat_8','cat_9', 'cat_10'])
t1=0
skips = 0
limits = 500000
bulk_list = []
# loop_cou = list(db.JUP_DB_ATPCO_Fares_Rules.find({'$or': [{"effective_to": {'$gte': '2018-03-15'}},{"effective_to": None}]}))
loop_coun = 5724
loop_count = int(loop_coun/limits)
loop_count = loop_count+2
#for 28.5 mn, I write 1,30 in for loop.
for turns in range(1,loop_count):
    print "loop started"
    #{'$or': [{"effective_to": {'$gte': '2018-01-15'}},{"effective_to": None}]}
    cursor = db.JUP_DB_ATPCO_Fares_Rules.find({"OD": {'$exists':True}, "channel": {'$exists': True}, "effective_from": {'$lte': '2018-03-17'},"effective_to": {'$gte': '2018-03-17'},},{"OD": 1, "cat_101": 1,
                                  "cat_102": 1, "cat_103": 1, "cat_104": 1, "cat_105": 1, "cat_106": 1, "cat_107": 1,
                                  "cat_108": 1, "cat_109": 1, "cat_11": 1, "cat_12": 1, "cat_13": 1, "cat_14": 1, "cat_15": 1,
                                  "cat_16": 1, "cat_17": 1, "cat_18": 1, "cat_19": 1,
                                  "cat_20": 1, "cat_21": 1, "cat_22": 1, "cat_23": 1, "cat_25": 1, "cat_26": 1, "cat_27": 1,
                                  "cat_28": 1, "cat_29": 1,
                                  "cat_35": 1, "cat_1": 1, "cat_2": 1, "cat_3": 1, "cat_31": 1, "cat_33": 1, "cat_4": 1,
                                  "cat_5": 1, "cat_50": 1, "cat_6": 1, "cat_7": 1, "cat_8": 1, "cat_9": 1, "cat_10": 1},no_cursor_timeout = True).skip(skips).limit(limits)
    f1 = pd.DataFrame(cursor)
#     fare5.del()
    lim = []
    # print f1.head()
    for i in f1.columns:
#         print i
        if "cat_" in i:
            lim.append(i)
    df = f1[lim]
    for i in f1.columns:
        if "cat_" in i:
            f1 = f1.drop([i], axis=1)
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
    df3 = pd.concat([df2, f1], axis=1)
    df4 = df3.isnull().sum(axis=1)
    df4 = pd.DataFrame(df4)
    df4.columns = ['a']
    df4['restrictions'] = 41 - df4['a']
    df4 = df4.drop(['a'], axis=1)
    df4 = pd.concat([f1, df4], axis=1)
    print df4.head()
    for j in range(len(df4)):
        if t1 == 1000:
            st = time.time()
            db['JUP_DB_ATPCO_Fares_Rules'].bulk_write(bulk_list)
            print "updated!", time.time() - st
            bulk_list = []
            bulk_list.append(UpdateOne({"_id": ObjectId(df4['_id'][j])}, {'$set': {'restrictions': df4['restrictions'][j]}}))
            t1 = 0
        else:
            bulk_list.append(UpdateOne({"_id": ObjectId(df4['_id'][j])}, {'$set': {'restrictions': df4['restrictions'][j]}}))
            t1 += 1
    skips = skips + 500000
    # print "-------"
    # print bulk_list
if len(bulk_list) !=0:
    db['JUP_DB_ATPCO_Fares_Rules'].bulk_write(bulk_list)
