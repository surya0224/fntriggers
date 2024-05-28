from pymongo import MongoClient
import numpy as np
from datetime import datetime, timedelta
import math
from dateutil.relativedelta import relativedelta

from jupiter_AI import client, ATPCO_DB, JUPITER_DB, SYSTEM_DATE, today, Host_Airline_Code

db_ATPCO = client[ATPCO_DB]
db_fzDB = client[JUPITER_DB]
record_3_cat_15_FN = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_15_FN
record_3_cat_14_FN = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_14_FN
record_2_cat_15_FN = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_15_FN
record_2_cat_14_FN = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_14_FN
record_2_cat_all = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_All
record_2_cat_10 = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_10
expired_FN_rules = db_fzDB.expired_FN_rules
expired_FN = db_fzDB.expired_FN
expired_FN.remove({})
expired_FN_rules.remove({})

# today = datetime(2018, 2, 15)
# SYSTEM_DATE = datetime.strftime(today, '%Y-%m-%d')
query_date = datetime.strftime(today, "%Y%m%d")
cur_record_3_cat_15_FN = record_3_cat_15_FN.find({})
cur_record_3_cat_14_FN = record_3_cat_14_FN.find({})

cat_14_date = []
cat_14_tbl_no = []
for i in cur_record_3_cat_14_FN:
    try:
        TRAVEL_DATES_EXP_DATE_FORMAT = datetime.strptime(i["TRAVEL_DATES_EXP"][1:],'%y%m%d')
        cat_14_date.append(TRAVEL_DATES_EXP_DATE_FORMAT)
        cat_14_tbl_no.append(i["TBL_NO"])
    except ValueError:
        pass

cat_15_date = []
cat_15_tbl_no = []
for i in cur_record_3_cat_15_FN:
    try:
        SALE_DATES_LATEST_TKTG_DATE_FORMAT = datetime.strptime(i["SALE_DATES_LATEST_TKTG"][1:],'%y%m%d')
        cat_15_date.append(SALE_DATES_LATEST_TKTG_DATE_FORMAT)
        cat_15_tbl_no.append(i["TBL_NO"])
    except ValueError:
        pass

cat_15_date = np.array(cat_15_date)
cat_15_tbl_no = np.array(cat_15_tbl_no)
cat_14_date = np.array(cat_14_date)
cat_14_tbl_no = np.array(cat_14_tbl_no)
print cat_15_tbl_no
print cat_14_tbl_no

expired_FN_cat_15_dates = cat_15_date[cat_15_date < (today - timedelta(days = 14))]
expired_FN_cat_15_tbl_no = cat_15_tbl_no[cat_15_date < (today - timedelta(days = 14))]
expired_FN_cat_14_dates = cat_14_date[cat_14_date < (today - timedelta(days = 14))]
expired_FN_cat_14_tbl_no = cat_14_tbl_no[cat_14_date < (today - timedelta(days = 14))]
expired_FN_cat_15_tbl_no = list(expired_FN_cat_15_tbl_no)
print len(expired_FN_cat_15_tbl_no)
expired_FN_cat_14_tbl_no = list(expired_FN_cat_14_tbl_no)
print len(expired_FN_cat_14_tbl_no)

FN_filter = [unichr(i) for i in range(65,91,1)]
for i in range(1,100,1):
    FN_filter.append(str(i))
for i in range(65,91,1):
    for j in range(65,91,1):
        FN_filter.append(unichr(i)+unichr(j))

add_on_fn = ["1F","2F","3F","4F","5F","6F","7F","8F","9F","1T","2T","3T","4T","5T","6T","7T","8T","9T"]
FN_filter = FN_filter + add_on_fn
print FN_filter

expired_FN_Tar = []

count = 1
for j in range(int(math.ceil(len(expired_FN_cat_15_tbl_no) / 1000.0))):
    temp = []
    for i in range(200):
        try:
            temp.append(
                {"DATA_TABLE_STRING_TBL_NO_" + str(i + 1): {"$in": expired_FN_cat_15_tbl_no[j * 1000:j * 1000 + 1000]}})
        except IndexError:
            temp.append({"DATA_TABLE_STRING_TBL_NO_" + str(i + 1): {"$in": expired_FN_cat_15_tbl_no[j * 1000:]}})
    cur_record_2_cat_15_FN = record_2_cat_15_FN.find({"$or": temp, "DATES_EFF_1": {"$lte": query_date},
                                                      "DATES_DISC_1": {"$gte": query_date},
                                                      "FT_NT": {"$nin": FN_filter}, "CXR_CODE": Host_Airline_Code})
    for i in cur_record_2_cat_15_FN:
        expired_FN_Tar.append({"FT_NT": i["FT_NT"], "TARIFF": i["TARIFF"]})
        expired_FN.save(i)
        print count, "15"
        count += 1

count = 1
for j in range(int(math.ceil(len(expired_FN_cat_14_tbl_no) / 1000.0))):
    temp = []
    for i in range(200):
        try:
            temp.append(
                {"DATA_TABLE_STRING_TBL_NO_" + str(i + 1): {"$in": expired_FN_cat_14_tbl_no[j * 1000:j * 1000 + 1000]}})
        except IndexError:
            temp.append({"DATA_TABLE_STRING_TBL_NO_" + str(i + 1): {"$in": expired_FN_cat_14_tbl_no[j * 1000:]}})
    cur_record_2_cat_14_FN = record_2_cat_14_FN.find({"$or": temp, "DATES_EFF_1": {"$lte": query_date},
                                                      "DATES_DISC_1": {"$gte": query_date},
                                                      "FT_NT": {"$nin": FN_filter}, "CXR_CODE": Host_Airline_Code})
    for i in cur_record_2_cat_14_FN:
        expired_FN_Tar.append({"FT_NT": i["FT_NT"], "TARIFF": i["TARIFF"]})
        expired_FN.save(i)
        print count, "14"
        count += 1

print len(expired_FN_Tar)

cur = expired_FN.find({})
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
        if set(temp) <= set(expired_FN_cat_14_tbl_no):
            cur14 = record_3_cat_14_FN.find({"TBL_NO": {"$in": temp}})
            for j in cur14:
                i["record_3_data"].append(j)
            expired_FN.update({"_id": i["_id"]}, i)
        else:
            expired_FN.remove({"_id": i["_id"]})
    elif i["CAT_NO"] == "015":
        if set(temp) <= set(expired_FN_cat_15_tbl_no):
            cur15 = record_3_cat_15_FN.find({"TBL_NO": {"$in": temp}})
            for j in cur15:
                i["record_3_data"].append(j)
            expired_FN.update({"_id": i["_id"]}, i)
        else:
            expired_FN.remove({"_id": i["_id"]})
    print count
    count += 1

count = 1
cur = record_2_cat_all.find({"$or":expired_FN_Tar, "CXR_CODE":Host_Airline_Code, "DATES_EFF_1": {"$lte": query_date},
                                                  "DATES_DISC_1": {"$gte": query_date}})
for i in cur:
    expired_FN_rules.save(i)
    print count, "all"
    count += 1

count = 1
cur = record_2_cat_10.find({"$or":expired_FN_Tar, "CXR_CODE":Host_Airline_Code, "DATES_EFF_1": {"$lte": query_date},
                                                  "DATES_DISC_1": {"$gte": query_date}})
for i in cur:
    expired_FN_rules.save(i)
    print count, "10"
    count += 1

# temp = []
# for i in expired_FN_Tar:
#     temp.append(tuple(i.items()))
# temp = set(temp)
#
# query = []
# for i in temp:
#     query.append(dict(i))
# print query
#
# cat = ["101","102","103","104","105","106","107","108","109"]
# for i in range(1,51):
#     if i < 10:
#         cat.append("00"+str(i))
#     elif i > 10:
#         cat.append("0"+str(i))
#
# for j in query:
#     for k in cat:
#         cur_1 = expired_FN.find({"FT_NT":j["FT_NT"],"TARIFF":j["TARIFF"], "CAT_NO":k})
#
#         cur_1 = list(cur_1)
#         print len(cur_1)
#         if len(cur_1) > 1:
#             print k, "fn"
#             for i in cur_1:
#                 i["Remarks"] = "Multiple Sequences"
#                 print "update cur_1"
#                 expired_FN.update({"_id":i["_id"]}, i)

cur_1 = expired_FN.find({})
count = 1
for i in cur_1:
    if i["CAT_NO"] == "014":
        cur14 = record_2_cat_14_FN.find({"TARIFF":i["TARIFF"], "FT_NT":i["FT_NT"], "CXR_CODE":Host_Airline_Code,
                                         "DATES_EFF_1": {"$lte": query_date},
                                         "DATES_DISC_1": {"$gte": query_date}
                                         })
        cur14 = list(cur14)
        print len(cur14)
        if len(cur14) > 1:
            i["Remarks"] = "Multiple Sequences"
            print "update cur14"
            expired_FN.update({"_id": i["_id"]}, i)

    else:
        cur15 = record_2_cat_15_FN.find({"TARIFF": i["TARIFF"], "FT_NT": i["FT_NT"], "CXR_CODE":Host_Airline_Code,
                                         "DATES_EFF_1": {"$lte": query_date},
                                         "DATES_DISC_1": {"$gte": query_date}
                                         })
        cur15 = list(cur15)
        print len(cur15)
        if len(cur15) > 1:
            i["Remarks"] = "Multiple Sequences"
            print "update cur15"
            expired_FN.update({"_id": i["_id"]}, i)
    print count, "------------"
    count += 1