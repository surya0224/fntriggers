from pymongo import MongoClient
import time
from datetime import datetime, timedelta

client = MongoClient('localhost', 43535)
db = client.ATPCO_stg_Aug
# client = MongoClient("mongodb://dbteam:KNjSZmiaNUGLmS0Bv2@13.92.251.7:42525/")
# db = client.ATPCO

record_4_fn = db.JUP_DB_ATPCO_Record_4_FN
record_4 = db.JUP_DB_ATPCO_Record_4
record_5 = db.JUP_DB_ATPCO_Record_5
record_1 = db.JUP_DB_ATPCO_Record_1
record_2_cat_003_fn = db.JUP_DB_ATPCO_Record_2_Cat_03_FN
record_2_cat_011_fn = db.JUP_DB_ATPCO_Record_2_Cat_11_FN
record_2_cat_014_fn = db.JUP_DB_ATPCO_Record_2_Cat_14_FN
record_2_cat_015_fn = db.JUP_DB_ATPCO_Record_2_Cat_15_FN
record_2_cat_023_fn = db.JUP_DB_ATPCO_Record_2_Cat_23_FN
record_2_cat_all = db.JUP_DB_ATPCO_Record_2_Cat_All
record_2_cat_10 = db.JUP_DB_ATPCO_Record_2_Cat_10
record_2_cat_25 = db.JUP_DB_ATPCO_Record_2_Cat_25

system_date = datetime(2017,9,22)
system_date = datetime.strftime(system_date, "%Y%m%d")

cur = record_4_fn.find({})
print "start"
count = 1
for i in cur:
    print i["OLD_SEQ_NO"], " --- ORIGINAL"
    exec "record_2_cat_" + str(i['CAT_NO']) + "_fn.update({'CXR_CODE':i['CXR_CODE'], 'TARIFF':i['TARIFF'], " \
                                        "'FT_NT':i['FT_NT'], 'SEQ_NO':i['OLD_SEQ_NO'], " \
                                        "'DATES_EFF_1':{'$lte':system_date}, 'DATES_DISC_1':{'$gte':system_date}}, " \
                                        "{'$set':{'SEQ_NO':i['NEW_SEQ_NO']}}, multi = True)"
    print i["NEW_SEQ_NO"], " --- NEW"
    print count, "footnotes done"
    count += 1
print "done"

cur = record_4.find({})
count = 1
for i in cur:
    print i["CAT_NO"]
    if i["CAT_NO"] == "010":
        print "updating CAT 10 coll"
        record_2_cat_10.update({'CXR_CODE':i['CXR_CODE'],'TARIFF':i['TARIFF'], 'RULE_NO':i['RULE_NO'],
                                     'SEQ_NO':i['OLD_SEQ_NO'], 'DATES_EFF_1':{'$lte':system_date},
                                     'DATES_DISC_1':{'$gte':system_date}}, {'$set':{'SEQ_NO':i['NEW_SEQ_NO']}}, multi = True)
    elif i["CAT_NO"] == "025":
        print "updating COLL 25 coll"
        record_2_cat_25.update({'CXR_CODE': i['CXR_CODE'], 'TARIFF': i['TARIFF'], 'RULE_NO': i['RULE_NO'],
                                     'SEQ_NO': i['OLD_SEQ_NO'], 'DATES_EFF_1': {'$lte': system_date},
                                     'DATES_DISC_1': {'$gte': system_date}}, {'$set': {'SEQ_NO': i['NEW_SEQ_NO']}}, multi = True)
    else:
        print "updating CAT ALL coll"
        record_2_cat_all.update({'CAT_NO': i['CAT_NO'],'CXR_CODE':i['CXR_CODE'],
                                 'TARIFF':i['TARIFF'], 'RULE_NO':i['RULE_NO'], 'SEQ_NO':i['OLD_SEQ_NO'],
                                 'DATES_EFF_1':{'$lte':system_date}, 'DATES_DISC_1':{'$gte':system_date}},
                                {'$set':{'SEQ_NO':i['NEW_SEQ_NO']}}, multi = True)
    print count, "rules done"
    count += 1

cur = record_5.find({})
count = 1
for i in cur:
    record_1.update({'CXR_CODE': i['CXR_CODE'], 'TARIFF': i['TARIFF'], 'RULE_NO': i['RULE_NO'],
                          'FARE_CLASS': i['FARE_CLASS'], 'SEQ_NO': i['OLD_SEQ_NO'], 'DATES_EFF_1': {'$lte': system_date},
                          'DATES_DISC_1': {'$gte': system_date}}, {'$set': {'SEQ_NO': i['NEW_SEQ_NO']}}, multi = True)
    print count, "record 1 done"
    count += 1
