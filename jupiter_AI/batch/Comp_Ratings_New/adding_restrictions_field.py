from pymongo import MongoClient
from pymongo import UpdateOne
import pandas as pd
# client = MongoClient("mongodb://analytics:KNjSZmiaNUGLmS0Bv2@13.92.251.7:42525/")
# db = client.fzDB_stg
from jupiter_AI import JUPITER_DB, client
db = client[JUPITER_DB]
import datetime
import scipy.stats as ss
import time
import re

cat_list = []
big_l = []
main_list=[]
restriction_list=[]
count = 0
t1=0
skips = 0
limits = 2
loop_coun = db.JUP_DB_ATPCO_Fares_Rules.find({}).count()
# loop_coun = len(loop_cou)
loop_count = int(loop_coun/limits)
loop_count = loop_count+2

for k in range(0,2):
    cur = db.JUP_DB_ATPCO_Fares_Rules.find({},{"cat_101": 1,
                                  "cat_102": 1, "cat_103": 1, "cat_104": 1, "cat_105": 1, "cat_106": 1, "cat_107": 1,
                                  "cat_108": 1, "cat_109": 1, "cat_11": 1, "cat_12": 1, "cat_13": 1, "cat_14": 1, "cat_15": 1,
                                  "cat_16": 1, "cat_17": 1, "cat_18": 1, "cat_19": 1,
                                  "cat_20": 1, "cat_21": 1, "cat_22": 1, "cat_23": 1, "cat_25": 1, "cat_26": 1, "cat_27": 1,
                                  "cat_28": 1, "cat_29": 1, "cat_35": 1, "cat_1": 1, "cat_2": 1, "cat_3": 1, "cat_31": 1, "cat_33": 1, "cat_4": 1,
                                  "cat_5": 1, "cat_50": 1, "cat_6": 1, "cat_7": 1, "cat_8": 1, "cat_9": 1, "cat_10": 1, "Footnotes.Cat_3_FN":1., "Footnotes.Cat_11_FN":1, "Footnotes.Cat_14_FN":1, "Footnotes.Cat_15_FN":1, "Footnotes.Cat_23_FN":1}).skip(skips).limit(limits)


    for i in cur:
        columns_list = list(i.keys())
        big_l.append(columns_list)
        main_list.append(i['_id'])

    df = pd.DataFrame(main_list)
    for m in range(len(big_l)):
        for i in big_l[m]:
            if "cat_" or "Cat_" in i:
                count +=1
                print "count----: ",count
        restriction_list.append(count)
    print restriction_list
    df['restrictions'] = restriction_list

    #     if t1 == 1000:
    #         st = time.time()
    #         db['JUP_DB_ATPCO_Fares_Rules'].bulk_write(bulk_list)
    #         print "updated!", time.time() - st
    #         bulk_list.append(UpdateOne({"_id": ObjectId(df_3['_id'][j])}, {'$set': {'segment': df_3['segment'][j]}}))
    #         bulk_list = []
    #         t1 = 0
    #     else:
    #         bulk_list.append(UpdateOne({"_id": ObjectId(df_3['_id'][j])}, {'$set': {'segment': df_3['segment'][j]}}))
    #         t1 += 1
    # bulk_list = []
    # count2 = 1
    #
    skips = skips+limits