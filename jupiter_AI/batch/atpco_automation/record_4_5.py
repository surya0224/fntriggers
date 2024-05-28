import pymongo
from pymongo import MongoClient
import time
import copy
from datetime import datetime, timedelta

from jupiter_AI import ATPCO_DB, JUPITER_DB, JUPITER_LOGGER, mongo_client
from jupiter_AI.logutils import measure


@measure(JUPITER_LOGGER)
def record_4_5(system_date,file_time, client):
    db = client[ATPCO_DB]
    db_fzDB = client[JUPITER_DB]

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

    # cr = db_fzDB.JUP_DB_ATPCO_Temp_Dates_DND.find({'file':'record_4_5.py'})
    # for i in cr:
    #     system_date = i['date']
    # db_fzDB.JUP_DB_ATPCO_Temp_Dates_DBD.update({'file':'record_4_5.py'},{'$set':{'status':'In Progress'}})

    system_date_1 = copy.deepcopy(system_date)
    system_date = datetime.strptime(system_date,'%Y-%m-%d')
    print (system_date)
    yesterday_ISO = system_date - timedelta(days=1)
    yesterday = datetime.strftime(yesterday_ISO, '%Y-%m-%d')
    system_date = datetime.strftime(system_date, "%Y%m%d")

    record_4_fn.remove({
        "LAST_UPDATED_DATE": yesterday,
        "LAST_UPDATED_TIME": file_time
    })
    record_4.remove({
        "LAST_UPDATED_DATE": yesterday,
        "LAST_UPDATED_TIME": file_time
    })
    record_5.remove({
        "LAST_UPDATED_DATE": yesterday,
        "LAST_UPDATED_TIME": file_time
    })

    cur = record_4_fn.find({
        "LAST_UPDATED_DATE": system_date_1,
        "LAST_UPDATED_TIME": file_time
        })
    print ("Processing Record 4_FN")
    count = 0
    for i in cur:
        #print i["OLD_SEQ_NO"], " --- ORIGINAL"
        exec ("record_2_cat_" + str(i['CAT_NO']) + "_fn.update({'CXR_CODE':i['CXR_CODE'], 'TARIFF':i['TARIFF'], " \
                                            "'FT_NT':i['FT_NT'], 'SEQ_NO':i['OLD_SEQ_NO'], " \
                                            "'DATES_EFF_1':{'$lte':system_date}, 'DATES_DISC_1':{'$gte':system_date}}, " \
                                            "{'$set':{'SEQ_NO':i['NEW_SEQ_NO'], 'LAST_RUN_TIME' : i['LAST_RUN_TIME'], 'LAST_RUN_DATE' : i['LAST_RUN_DATE']}}, multi = True)")
        #print i["NEW_SEQ_NO"], " --- NEW"
        #print count, "footnotes done"
        # record_4_fn.remove({"_id" : i["_id"]})
        count += 1
    print (count, "footnotes updated")

    cur = record_4.find({ "LAST_UPDATED_DATE": system_date_1,"LAST_UPDATED_TIME": file_time})
    print ("Processing Record 4")
    count = 0
    for i in cur:
        #print i["CAT_NO"]
        if i["CAT_NO"] == "010":
            #print ("updating CAT 10 coll")
            record_2_cat_10.update({'CXR_CODE':i['CXR_CODE'],'TARIFF':i['TARIFF'], 'RULE_NO':i['RULE_NO'],
                                         'SEQ_NO':i['OLD_SEQ_NO'], 'DATES_EFF_1':{'$lte':system_date},
                                         'DATES_DISC_1':{'$gte':system_date}}, {'$set':{'SEQ_NO':i['NEW_SEQ_NO'], 'LAST_RUN_TIME' : i['LAST_RUN_TIME'], 'LAST_RUN_DATE' : i['LAST_RUN_DATE']}}, multi = True)
        elif i["CAT_NO"] == "025":
            #print ("updating CAT 25 coll")
            record_2_cat_25.update({'CXR_CODE': i['CXR_CODE'], 'TARIFF': i['TARIFF'], 'RULE_NO': i['RULE_NO'],
                                         'SEQ_NO': i['OLD_SEQ_NO'], 'DATES_EFF_1': {'$lte': system_date},
                                         'DATES_DISC_1': {'$gte': system_date}}, {'$set': {'SEQ_NO': i['NEW_SEQ_NO'], 'LAST_RUN_TIME' : i['LAST_RUN_TIME'], 'LAST_RUN_DATE' : i['LAST_RUN_DATE']}}, multi = True)
        else:
            #print ("updating CAT ALL coll")
            record_2_cat_all.update({'CAT_NO': i['CAT_NO'],'CXR_CODE':i['CXR_CODE'],
                                     'TARIFF':i['TARIFF'], 'RULE_NO':i['RULE_NO'], 'SEQ_NO':i['OLD_SEQ_NO'],
                                     'DATES_EFF_1':{'$lte':system_date}, 'DATES_DISC_1':{'$gte':system_date}},
                                    {'$set':{'SEQ_NO':i['NEW_SEQ_NO'], 'LAST_RUN_TIME' : i['LAST_RUN_TIME'], 'LAST_RUN_DATE' : i['LAST_RUN_DATE']}}, multi = True)
        #print count, "rules done"
        # record_4.remove({"_id" : i["_id"]})
        count += 1
    print (count, "Record 2 updated")

    cur = record_5.find({"LAST_UPDATED_DATE": system_date_1,"LAST_UPDATED_TIME": file_time})
    print ("Processing Record 5")
    count = 0
    for i in cur:
        record_1.update({'CXR_CODE': i['CXR_CODE'], 'TARIFF': i['TARIFF'], 'RULE_NO': i['RULE_NO'],
                              'FARE_CLASS': i['FARE_CLASS'], 'SEQ_NO': i['OLD_SEQ_NO'], 'DATES_EFF_1': {'$lte': system_date},
                              'DATES_DISC_1': {'$gte': system_date}}, {'$set': {'SEQ_NO': i['NEW_SEQ_NO'], 'LAST_RUN_TIME' : i['LAST_RUN_TIME'], 'LAST_RUN_DATE' : i['LAST_RUN_DATE']}}, multi = True)
        #print count, "record 1 done"
        # record_5.remove({"_id" : i["_id"]})
        count += 1
    print (count, "Record 1 updated")

    # cr = db_fzDB.JUP_DB_ATPCO_Temp_Dates_DND.find({'file':'record_4_5.py'})
    # for i in cr:
    #     i['date'] = datetime.strftime(datetime.strptime(i['date'],'%Y-%m-%d')+timedelta(days=1),'%Y-%m-%d')
    #     i['status'] = 'To Do'
    #     db_fzDB.JUP_DB_ATPCO_Temp_Dates_DND.update({'_id':i['_id']}, i)
    #     print 'record_4_5 date updated'


if __name__=='__main__':
    client = mongo_client()
    record_4_5("2019-03-12", client)