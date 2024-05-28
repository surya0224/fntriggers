from pymongo import MongoClient
import numpy as np
from datetime import datetime, timedelta
import math
from dateutil.relativedelta import relativedelta

from jupiter_AI import client, ATPCO_DB, JUPITER_DB, SYSTEM_DATE, today, Host_Airline_Code

db_ATPCO = client[ATPCO_DB]
db_fzDB = client[JUPITER_DB]

# today = datetime(2018, 2, 15)
# SYSTEM_DATE = datetime.strftime(today, '%Y-%m-%d')
query_date = datetime.strftime(today, "%Y%m%d")
record_3_cat_3 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_03
record_3_cat_11 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_11
record_3_cat_14 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_14
record_3_cat_15 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_15
expired_rules_without_FN = db_fzDB.expired_rules_without_FN
expired_rules_without_FN.remove({})
record_2_cat_all = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_All

cur_record_3_cat_14 = record_3_cat_14.find({})
cur_record_3_cat_15 = record_3_cat_15.find({})
cur_record_3_cat_3 = record_3_cat_3.find({})
cur_record_3_cat_11 = record_3_cat_11.find({})

record_3_cat_3_date = []
record_3_cat_3_tbl_no = []
for i in cur_record_3_cat_3:
    if len(i["DATE_RANGE_STOP"]) != 4:
        try:
            DATE_RANGE_STOP_DATE_FORMAT = datetime.strptime(i["DATE_RANGE_STOP"],'%y%m%d')
            record_3_cat_3_date.append(DATE_RANGE_STOP_DATE_FORMAT)
            record_3_cat_3_tbl_no.append(i["TBL_NO"])
        except ValueError:
            pass

record_3_cat_11_date = []
record_3_cat_11_tbl_no = []
for i in cur_record_3_cat_11:
    if len(i["RESTRICTED_DATES_STOP"]) != 4:
        try:
            RESTRICTED_DATES_STOP_DATE_FORMAT = datetime.strptime(i["RESTRICTED_DATES_STOP"],'%y%m%d')
            record_3_cat_11_date.append(RESTRICTED_DATES_STOP_DATE_FORMAT)
            record_3_cat_11_tbl_no.append(i["TBL_NO"])
        except ValueError:
            pass

record_3_cat_14_date = []
record_3_cat_14_tbl_no = []
for i in cur_record_3_cat_14:
        try:
            TRAVEL_DATES_EXP_DATE_FORMAT = datetime.strptime(i["TRAVEL_DATES_EXP"][1:],'%y%m%d')
            record_3_cat_14_date.append(TRAVEL_DATES_EXP_DATE_FORMAT)
            record_3_cat_14_tbl_no.append(i["TBL_NO"])
        except ValueError:
            pass

record_3_cat_15_date = []
record_3_cat_15_tbl_no = []
for i in cur_record_3_cat_15:
        try:
            SALE_DATES_LATEST_TKTG_DATE_FORMAT = datetime.strptime(i["SALE_DATES_LATEST_TKTG"][1:],'%y%m%d')
            record_3_cat_15_date.append(SALE_DATES_LATEST_TKTG_DATE_FORMAT)
            record_3_cat_15_tbl_no.append(i["TBL_NO"])
        except ValueError:
            pass

record_3_cat_14_date = np.array(record_3_cat_14_date)
record_3_cat_15_date = np.array(record_3_cat_15_date)
record_3_cat_14_tbl_no = np.array(record_3_cat_14_tbl_no)
record_3_cat_15_tbl_no = np.array(record_3_cat_15_tbl_no)
record_3_cat_3_date = np.array(record_3_cat_3_date)
record_3_cat_11_date = np.array(record_3_cat_11_date)
record_3_cat_3_tbl_no = np.array(record_3_cat_3_tbl_no)
record_3_cat_11_tbl_no = np.array(record_3_cat_11_tbl_no)

cat_14_expired_rules_tbl_no = record_3_cat_14_tbl_no[record_3_cat_14_date < (today - relativedelta(months=1))]
cat_15_expired_rules_tbl_no = record_3_cat_15_tbl_no[record_3_cat_15_date < (today - relativedelta(months=1))]
cat_3_expired_rules_tbl_no = record_3_cat_3_tbl_no[record_3_cat_3_date < (today - relativedelta(months=1))]
cat_11_expired_rules_tbl_no = record_3_cat_11_tbl_no[record_3_cat_11_date < (today - relativedelta(months=1))]

print len(cat_3_expired_rules_tbl_no)
print len(cat_11_expired_rules_tbl_no)
print len(cat_14_expired_rules_tbl_no)
print len(cat_15_expired_rules_tbl_no)
cat_14_expired_rules_tbl_no = list(cat_14_expired_rules_tbl_no)
cat_15_expired_rules_tbl_no = list(cat_15_expired_rules_tbl_no)
cat_3_expired_rules_tbl_no = list(cat_3_expired_rules_tbl_no)
cat_11_expired_rules_tbl_no = list(cat_11_expired_rules_tbl_no)

expired_rules_tbl_no = cat_14_expired_rules_tbl_no + cat_15_expired_rules_tbl_no + cat_3_expired_rules_tbl_no + cat_11_expired_rules_tbl_no
print len(expired_rules_tbl_no)

count = 1
for j in range(int(math.ceil(len(cat_14_expired_rules_tbl_no)/1000.0))):
    temp = []
    for i in range(200):
        try:
            temp.append({"DATA_TABLE_STRING_TBL_NO_"+str(i+1):{"$in":cat_14_expired_rules_tbl_no[j*1000:j*1000+1000]}})
        except IndexError:
            temp.append({"DATA_TABLE_STRING_TBL_NO_"+str(i+1):{"$in":cat_14_expired_rules_tbl_no[j*1000:]}})
    print "appended"
    cur_record_2_cat_all = record_2_cat_all.find({"$or":temp, "CXR_CODE" :{"$eq":Host_Airline_Code},
                                                  "DATES_EFF_1": {"$lte": query_date},
                                                  "DATES_DISC_1": {"$gte": query_date},
                                                  "CAT_NO":{"$in":["014"]}})
    print "queried", cur_record_2_cat_all.count()
    for i in cur_record_2_cat_all:
        print "inside"
        expired_rules_without_FN.save(i)
        print count, "14"
        count += 1

count = 1
for j in range(int(math.ceil(len(cat_15_expired_rules_tbl_no)/1000.0))):
    temp = []
    for i in range(200):
        try:
            temp.append({"DATA_TABLE_STRING_TBL_NO_"+str(i+1):{"$in":cat_15_expired_rules_tbl_no[j*1000:j*1000+1000]}})
        except IndexError:
            temp.append({"DATA_TABLE_STRING_TBL_NO_"+str(i+1):{"$in":cat_15_expired_rules_tbl_no[j*1000:]}})
    print "appended"
    cur_record_2_cat_all = record_2_cat_all.find({"$or":temp, "CXR_CODE" :{"$eq":Host_Airline_Code},
                                                  "DATES_EFF_1": {"$lte": query_date},
                                                  "DATES_DISC_1": {"$gte": query_date},
                                                  "CAT_NO":{"$in":["015"]}})
    print "queried", cur_record_2_cat_all.count()
    for i in cur_record_2_cat_all:
        print "inside"
        expired_rules_without_FN.save(i)
        print count, "15"
        count += 1

count = 1
for j in range(int(math.ceil(len(cat_3_expired_rules_tbl_no)/1000.0))):
    temp = []
    for i in range(200):
        try:
            temp.append({"DATA_TABLE_STRING_TBL_NO_"+str(i+1):{"$in":cat_3_expired_rules_tbl_no[j*1000:j*1000+1000]}})
        except IndexError:
            temp.append({"DATA_TABLE_STRING_TBL_NO_"+str(i+1):{"$in":cat_3_expired_rules_tbl_no[j*1000:]}})
    print "appended"
    cur_record_2_cat_all = record_2_cat_all.find({"$or":temp,
                                                  "DATES_EFF_1": {"$lte": query_date},
                                                  "DATES_DISC_1": {"$gte": query_date},
                                                  "CXR_CODE" :{"$eq":Host_Airline_Code}, "CAT_NO":{"$in":["003"]}})
    print "queried", cur_record_2_cat_all.count()
    for i in cur_record_2_cat_all:
        print "inside"
        expired_rules_without_FN.save(i)
        print count, "3"
        count += 1

count = 1
for j in range(int(math.ceil(len(cat_11_expired_rules_tbl_no)/1000.0))):
    temp = []
    for i in range(200):
        try:
            temp.append({"DATA_TABLE_STRING_TBL_NO_"+str(i+1):{"$in":cat_11_expired_rules_tbl_no[j*1000:j*1000+1000]}})
        except IndexError:
            temp.append({"DATA_TABLE_STRING_TBL_NO_"+str(i+1):{"$in":cat_11_expired_rules_tbl_no[j*1000:]}})
    print "appended"
    cur_record_2_cat_all = record_2_cat_all.find({"$or":temp, "CXR_CODE" :{"$eq":Host_Airline_Code},
                                                  "DATES_EFF_1": {"$lte": query_date},
                                                  "DATES_DISC_1": {"$gte": query_date},
                                                  "CAT_NO":{"$in":["011"]}})
    print "queried", cur_record_2_cat_all.count()
    for i in cur_record_2_cat_all:
        print "inside"
        expired_rules_without_FN.save(i)
        print count, "11"
        count += 1

cur = expired_rules_without_FN.find({})
count = 1
for i in cur:
    temp = []
    j = 1
    while (j<=200):
        try:
            temp.append(i["DATA_TABLE_STRING_TBL_NO_"+str(j)])
        except KeyError:
            break
        j += 1
    i["record_3_data"] = []
    if (i["CAT_NO"] == "014"):
        if set(temp) <= set(cat_14_expired_rules_tbl_no):
            cur14 = record_3_cat_14.find({"TBL_NO": {"$in": temp}})
            for j in cur14:
                i["record_3_data"].append(j)
            expired_rules_without_FN.update({"_id": i["_id"]}, i)
        else:
            expired_rules_without_FN.remove({"_id": i["_id"]})
    elif i["CAT_NO"] == "015":
        if set(temp) <= set(cat_15_expired_rules_tbl_no):
            cur15 = record_3_cat_15.find({"TBL_NO": {"$in": temp}})
            for j in cur15:
                i["record_3_data"].append(j)
            expired_rules_without_FN.update({"_id": i["_id"]}, i)
        else:
            expired_rules_without_FN.remove({"_id": i["_id"]})
    elif i["CAT_NO"] == "011":
        if set(temp) <= set(cat_11_expired_rules_tbl_no):
            cur11 = record_3_cat_11.find({"TBL_NO": {"$in": temp}})
            for j in cur11:
                i["record_3_data"].append(j)
            expired_rules_without_FN.update({"_id": i["_id"]}, i)
        else:
            expired_rules_without_FN.remove({"_id": i["_id"]})
    elif i["CAT_NO"] == "003":
        if set(temp) <= set(cat_3_expired_rules_tbl_no):
            cur3 = record_3_cat_3.find({"TBL_NO": {"$in": temp}})
            for j in cur3:
                i["record_3_data"].append(j)
            expired_rules_without_FN.update({"_id": i["_id"]}, i)
        else:
            expired_rules_without_FN.remove({"_id": i["_id"]})
    print count
    count += 1