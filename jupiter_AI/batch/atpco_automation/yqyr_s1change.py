"""
Author: Anamika Jaiswal
Date Created: 2018-05-15
File Name: yqyr_s1change.py

This code is used to update S1 record from the S1 change record.

"""

import pymongo
from pymongo import MongoClient
import time
from datetime import datetime, timedelta

from jupiter_AI import JUPITER_DB, ATPCO_DB, JUPITER_LOGGER
from jupiter_AI.logutils import measure



@measure(JUPITER_LOGGER)
def yqyr_s1change_main(system_date,file_time,client):
    db = client[ATPCO_DB]

    yqyr_main=db.JUP_DB_ATPCO_YQYR_RecS1
    yqyr_change=db.JUP_DB_ATPCO_YQYR_RecS1_change

    cur = yqyr_change.find({"file_date" : system_date, "file_time" : {"$in" : file_time}}).sort([("file_time" , pymongo.ASCENDING)])
    count = 1
    for i in cur:

        #print "querying changed coll 3"
        cur_1 = yqyr_main.find({"CXR_CODE":i["CXR_CODE"], "SERVICE_TYPE_TAX_CODE":i["SERVICE_TYPE_TAX_CODE"], "SERVICE_TYPE_SUB_CODE":i["SERVICE_TYPE_SUB_CODE"],
                                                "SEQ_NO":i["SEQ_NO"]})
        #print "queried"
        for j in cur_1:

            if(j["DISC_DATE"] > i["EFF_DATE"]):
                j["DISC_DATE"] = datetime.strftime(datetime.strptime(i["EFF_DATE"], "%y%m%d") - timedelta(days = 1), "%y%m%d")

                #print j["DATES_DISC_1"], "---- CHANGED ORIGINAL DISC DATE"
            yqyr_main.update({"_id":j["_id"]}, j)
        yqyr_main.save(i)
        print (count)
        count += 1

    db.JUP_DB_ATPCO_YQYR_RecS1_change.remove({"file_date" : system_date, "file_time" : {"$in" : file_time}})
    db.JUP_DB_ATPCO_YQYR_Table_190_change.remove({"file_date" : system_date, "file_time" : {"$in" : file_time}})
    db.JUP_DB_ATPCO_YQYR_Table_178_change.remove({"file_date" : system_date, "file_time" : {"$in" : file_time}})
    db.JUP_DB_ATPCO_YQYR_Table_171_change.remove({"file_date" : system_date, "file_time" : {"$in" : file_time}})
    db.JUP_DB_ATPCO_YQYR_Table_173_change.remove({"file_date" : system_date, "file_time" : {"$in" : file_time}})
    db.JUP_DB_ATPCO_YQYR_Table_198_change.remove({"file_date" : system_date, "file_time" : {"$in" : file_time}})



