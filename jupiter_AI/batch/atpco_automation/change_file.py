import pymongo
from pymongo import MongoClient
import time
from bson import ObjectId
from datetime import datetime, timedelta
from jupiter_AI import ATPCO_DB, JUPITER_DB,mongo_client


def record_2_cat_3_fn_change(system_date, file_time, client):
    db = client[ATPCO_DB]
    db_fzDB = client[JUPITER_DB]

    record_2_cat_3_fn = db.JUP_DB_ATPCO_Record_2_Cat_03_FN_change
    record_2_cat_3_fn_main = db.JUP_DB_ATPCO_Record_2_Cat_03_FN

    # cr = db_fzDB.JUP_DB_ATPCO_Temp_Dates_DND.find({'file':'change_file.py'})
    # for i in cr:
    #     system_date = i['date']
    system_date_1 = datetime.strptime(system_date,'%Y-%m-%d')
    print (system_date)

    # db_fzDB.JUP_DB_ATPCO_Temp_Dates_DBD.update({'file':'change_file.py'},{'$set':{'status':'In Progress'}})

    cur = record_2_cat_3_fn.find({"LAST_UPDATED_DATE" : datetime.strftime(system_date_1, '%Y-%m-%d'),"LAST_UPDATED_TIME" : file_time},no_cursor_timeout=True)
    system_date = datetime.strftime(system_date_1, "%Y%m%d")
    print ("starting record_2_cat_3_fn", cur.count())
    count = 1
    for i in cur:
        if i["ACTION"] == "2":
            #print "passed ACTION - 2"
            pass
        else:
            #print "querying changed coll 3"
            cur_1 = record_2_cat_3_fn_main.find({"CXR_CODE":i["CXR_CODE"], "TARIFF":i["TARIFF"], "FT_NT":i["FT_NT"],
                                                 "CAT_NO":i["CAT_NO"], "SEQ_NO":i["SEQ_NO"]})
            #print "queried"
            for j in cur_1:
                #print "-------------------------------------------"
                #print j["DATES_DISC_1"], "---- ORIGINAL DISC DATE"
                #print i["DATES_EFF_1"], "---- NEW EFF DATE"
                if (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] == system_date):
                    j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d"), "%y%m%d")
                    j["DATES_DISC_1"] = i["DATES_EFF_1"]
                elif (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] != system_date):
                    j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days = 1), "%y%m%d")
                    j["DATES_DISC_1"] = datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days = 1), "%Y%m%d")

                #print j["DATES_DISC_1"], "---- CHANGED ORIGINAL DISC DATE"
                record_2_cat_3_fn_main.update({"_id":j["_id"]}, j)

        _id = i['_id']
        del i['_id']
        record_2_cat_3_fn_main.insert(i)
        record_2_cat_3_fn.remove({'_id': ObjectId(_id)})
        print (count)
        count += 1


def record_2_cat_11_fn_change(system_date, file_time, client):
    db = client[ATPCO_DB]
    db_fzDB = client[JUPITER_DB]

    record_2_cat_11_fn = db.JUP_DB_ATPCO_Record_2_Cat_11_FN_change
    record_2_cat_11_fn_main = db.JUP_DB_ATPCO_Record_2_Cat_11_FN

    # cr = db_fzDB.JUP_DB_ATPCO_Temp_Dates_DND.find({'file': 'change_file.py'})
    # for i in cr:
    #     system_date = i['date']
    system_date_1 = datetime.strptime(system_date, '%Y-%m-%d')
    print (system_date)

    # db_fzDB.JUP_DB_ATPCO_Temp_Dates_DBD.update({'file': 'change_file.py'}, {'$set': {'status': 'In Progress'}})

    cur = record_2_cat_11_fn.find({"LAST_UPDATED_DATE" : datetime.strftime(system_date_1, '%Y-%m-%d'),"LAST_UPDATED_TIME" : file_time},no_cursor_timeout=True)
    system_date = datetime.strftime(system_date_1, "%Y%m%d")
    print ("starting record_2_cat_11_fn", cur.count())
    count = 1
    for i in cur:
        if i["ACTION"] == "2":
            #print "passed ACTION - 2"
            pass
        else:
            #print "querying changed coll 11"
            cur_1 = record_2_cat_11_fn_main.find({"CXR_CODE": i["CXR_CODE"], "TARIFF": i["TARIFF"], "FT_NT": i["FT_NT"],
                                                  "CAT_NO": i["CAT_NO"], "SEQ_NO": i["SEQ_NO"]})
            #print "queried"
            for j in cur_1:
                #print "-------------------------------------------"
                #print j["DATES_DISC_1"], "---- ORIGINAL DISC DATE"
                #print i["DATES_EFF_1"], "---- NEW EFF DATE"
                if (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] == system_date):
                    j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d"), "%y%m%d")
                    j["DATES_DISC_1"] = i["DATES_EFF_1"]
                elif (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] != system_date):
                    j["DATES_DISC"] = str(0) + datetime.strftime(
                        datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days=1), "%y%m%d")
                    j["DATES_DISC_1"] = datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days=1),
                                                          "%Y%m%d")

                #print (j["DATES_DISC_1"], "---- CHANGED ORIGINAL DISC DATE")
                record_2_cat_11_fn_main.update({"_id": j["_id"]}, j)

        _id = i['_id']
        del i['_id']
        record_2_cat_11_fn_main.insert(i)
        record_2_cat_11_fn.remove({'_id': ObjectId(_id)})
        print (count)
        count += 1


def record_2_cat_14_fn_change(system_date, file_time, client):
    db = client[ATPCO_DB]
    db_fzDB = client[JUPITER_DB]

    record_2_cat_14_fn = db.JUP_DB_ATPCO_Record_2_Cat_14_FN_change
    record_2_cat_14_fn_main = db.JUP_DB_ATPCO_Record_2_Cat_14_FN

    # cr = db_fzDB.JUP_DB_ATPCO_Temp_Dates_DND.find({'file': 'change_file.py'})
    # for i in cr:
    #     system_date = i['date']
    system_date_1 = datetime.strptime(system_date, '%Y-%m-%d')
    print (system_date)

    # db_fzDB.JUP_DB_ATPCO_Temp_Dates_DBD.update({'file': 'change_file.py'}, {'$set': {'status': 'In Progress'}})
    cur = record_2_cat_14_fn.find({"LAST_UPDATED_DATE" : datetime.strftime(system_date_1, '%Y-%m-%d'),"LAST_UPDATED_TIME" : file_time},no_cursor_timeout=True)
    system_date = datetime.strftime(system_date_1, "%Y%m%d")
    print ("starting record_2_cat_14_fn", cur.count())
    count = 1
    for i in cur:
        if i["ACTION"] == "2":
            #print "passed ACTION - 2"
            pass
        else:
            #print "querying changed coll 14"
            cur_1 = record_2_cat_14_fn_main.find({"CXR_CODE": i["CXR_CODE"], "TARIFF": i["TARIFF"], "FT_NT": i["FT_NT"],
                                                  "CAT_NO": i["CAT_NO"], "SEQ_NO": i["SEQ_NO"]})
            #print "queried"
            for j in cur_1:
                #print "-------------------------------------------"
                #print j["DATES_DISC_1"], "---- ORIGINAL DISC DATE"
                #print i["DATES_EFF_1"], "---- NEW EFF DATE"
                if (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] == system_date):
                    j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d"), "%y%m%d")
                    j["DATES_DISC_1"] = i["DATES_EFF_1"]
                elif (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] != system_date):
                    j["DATES_DISC"] = str(0) + datetime.strftime(
                        datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days=1), "%y%m%d")
                    j["DATES_DISC_1"] = datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days=1),
                                                          "%Y%m%d")

                #print j["DATES_DISC_1"], "---- CHANGED ORIGINAL DISC DATE"
                record_2_cat_14_fn_main.update({"_id": j["_id"]}, j)

        _id = i['_id']
        del i['_id']
        record_2_cat_14_fn_main.insert(i)
        record_2_cat_14_fn.remove({'_id': ObjectId(_id)})
        print (count)
        count += 1


def record_2_cat_15_fn_change(system_date, file_time, client):
    db = client[ATPCO_DB]
    db_fzDB = client[JUPITER_DB]

    record_2_cat_15_fn = db.JUP_DB_ATPCO_Record_2_Cat_15_FN_change
    record_2_cat_15_fn_main = db.JUP_DB_ATPCO_Record_2_Cat_15_FN

    # cr = db_fzDB.JUP_DB_ATPCO_Temp_Dates_DND.find({'file': 'change_file.py'})
    # for i in cr:
        # system_date = i['date']
    system_date_1 = datetime.strptime(system_date, '%Y-%m-%d')
    print (system_date)

    # db_fzDB.JUP_DB_ATPCO_Temp_Dates_DBD.update({'file': 'change_file.py'}, {'$set': {'status': 'In Progress'}})
    cur = record_2_cat_15_fn.find({"LAST_UPDATED_DATE" : datetime.strftime(system_date_1, '%Y-%m-%d'),"LAST_UPDATED_TIME" : file_time},no_cursor_timeout=True)
    system_date = datetime.strftime(system_date_1, "%Y%m%d")
    print ("starting rec 2 cat 15", cur.count())
    count = 1
    for i in cur:
        if i["ACTION"] == "2":
            pass
            #print ("passed ACTION - 2")
        else:
            #print ("querying changed coll 15")
            cur_1 = record_2_cat_15_fn_main.find({"CXR_CODE": i["CXR_CODE"], "TARIFF": i["TARIFF"], "FT_NT": i["FT_NT"],
                                                  "CAT_NO": i["CAT_NO"], "SEQ_NO": i["SEQ_NO"]})
            #print "queried"
            for j in cur_1:
                #print "-------------------------------------------"
                #print j["DATES_DISC_1"], "---- ORIGINAL DISC DATE"
                #print i["DATES_EFF_1"], "---- NEW EFF DATE"
                if (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] == system_date):
                    j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d"), "%y%m%d")
                    j["DATES_DISC_1"] = i["DATES_EFF_1"]
                elif (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] != system_date):
                    j["DATES_DISC"] = str(0) + datetime.strftime(
                        datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days=1), "%y%m%d")
                    j["DATES_DISC_1"] = datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days=1),
                                                          "%Y%m%d")

                #print j["DATES_DISC_1"], "---- CHANGED ORIGINAL DISC DATE"
                record_2_cat_15_fn_main.update({"_id": j["_id"]}, j)

        _id = i['_id']
        del i['_id']
        record_2_cat_15_fn_main.insert(i)
        record_2_cat_15_fn.remove({'_id': ObjectId(_id)})
        print (count)
        count += 1


def record_2_cat_23_fn_change(system_date, file_time, client):
    db = client[ATPCO_DB]
    db_fzDB = client[JUPITER_DB]

    record_2_cat_23_fn = db.JUP_DB_ATPCO_Record_2_Cat_23_FN_change
    record_2_cat_23_fn_main = db.JUP_DB_ATPCO_Record_2_Cat_23_FN

    # cr = db_fzDB.JUP_DB_ATPCO_Temp_Dates_DND.find({'file': 'change_file.py'})
    # for i in cr:
    #     system_date = i['date']
    system_date_1 = datetime.strptime(system_date, '%Y-%m-%d')
    print (system_date)

    # db_fzDB.JUP_DB_ATPCO_Temp_Dates_DBD.update({'file': 'change_file.py'}, {'$set': {'status': 'In Progress'}})

    cur = record_2_cat_23_fn.find({"LAST_UPDATED_DATE" : datetime.strftime(system_date_1, '%Y-%m-%d'),"LAST_UPDATED_TIME" : file_time},no_cursor_timeout=True)
    system_date = datetime.strftime(system_date_1, "%Y%m%d")
    print ("starting rec 2 cat 23 fn", cur.count())
    count = 1
    for i in cur:
        if i["ACTION"] == "2":
            pass
            #print "passed ACTION - 2"
        else:
            #print "querying changed coll 23"
            cur_1 = record_2_cat_23_fn_main.find({"CXR_CODE": i["CXR_CODE"], "TARIFF": i["TARIFF"], "FT_NT": i["FT_NT"],
                                                  "CAT_NO": i["CAT_NO"], "SEQ_NO": i["SEQ_NO"]})
            #print "queried"
            for j in cur_1:
                #print "-------------------------------------------"
                #print j["DATES_DISC_1"], "---- ORIGINAL DISC DATE"
                #print i["DATES_EFF_1"], "---- NEW EFF DATE"
                if (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] == system_date):
                    j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d"), "%y%m%d")
                    j["DATES_DISC_1"] = i["DATES_EFF_1"]
                elif (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] != system_date):
                    j["DATES_DISC"] = str(0) + datetime.strftime(
                        datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days=1), "%y%m%d")
                    j["DATES_DISC_1"] = datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days=1),
                                                          "%Y%m%d")

                #print j["DATES_DISC_1"], "---- CHANGED ORIGINAL DISC DATE"
                record_2_cat_23_fn_main.update({"_id": j["_id"]}, j)

        _id = i['_id']
        del i['_id']
        record_2_cat_23_fn_main.insert(i)
        record_2_cat_23_fn.remove({'_id': ObjectId(_id)})
        print (count)
        count += 1


def record_2_cat_all_change(system_date, file_time, client):
    db = client[ATPCO_DB]
    db_fzDB = client[JUPITER_DB]

    record_2_cat_all = db.JUP_DB_ATPCO_Record_2_Cat_All_change
    record_2_cat_all_main = db.JUP_DB_ATPCO_Record_2_Cat_All

    # cr = db_fzDB.JUP_DB_ATPCO_Temp_Dates_DND.find({'file': 'change_file.py'})
    # for i in cr:
    #     system_date = i['date']
    system_date_1 = datetime.strptime(system_date, '%Y-%m-%d')
    print (system_date)

    # db_fzDB.JUP_DB_ATPCO_Temp_Dates_DBD.update({'file': 'change_file.py'}, {'$set': {'status': 'In Progress'}})

    cur = record_2_cat_all.find({"LAST_UPDATED_DATE" : datetime.strftime(system_date_1, '%Y-%m-%d'),"LAST_UPDATED_TIME" : file_time},no_cursor_timeout=True)
    system_date = datetime.strftime(system_date_1, "%Y%m%d")
    count = 1
    st = time.time()
    print ("starting", cur.count())
    for i in cur:
        if i["ACTION"] == "2":
            pass
            #print ("passed ACTION - 2")
        else:
            #print "querying changed coll all"
            cur_1 = record_2_cat_all_main.find({"CXR_CODE":i["CXR_CODE"], "TARIFF":i["TARIFF"], "RULE_NO":i["RULE_NO"],
                                                "CAT_NO":i["CAT_NO"], "SEQ_NO":i["SEQ_NO"]})
            #print "queried"
            for j in cur_1:
                #print "-------------------------------------------"
                #print j["DATES_DISC_1"], "---- ORIGINAL DISC DATE"
                #print i["DATES_EFF_1"], "---- NEW EFF DATE"
                if (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] == system_date):
                    j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d"), "%y%m%d")
                    j["DATES_DISC_1"] = i["DATES_EFF_1"]
                elif (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] != system_date):
                    j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days = 1), "%y%m%d")
                    j["DATES_DISC_1"] = datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days = 1), "%Y%m%d")

                #print j["DATES_DISC_1"], "---- CHANGED ORIGINAL DISC DATE"
                record_2_cat_all_main.update({"_id":j["_id"]}, j)

        _id = i['_id']
        del i['_id']
        record_2_cat_all_main.insert(i)
        record_2_cat_all.remove({'_id': ObjectId(_id)})
        print (count)
        count += 1
    print ("done", time.time() - st)


def record_2_cat_10_change(system_date, file_time, client):
    db = client[ATPCO_DB]
    db_fzDB = client[JUPITER_DB]

    record_2_cat_10 = db.JUP_DB_ATPCO_Record_2_Cat_10_change
    record_2_cat_10_main = db.JUP_DB_ATPCO_Record_2_Cat_10

    # cr = db_fzDB.JUP_DB_ATPCO_Temp_Dates_DND.find({'file': 'change_file.py'})
    # for i in cr:
    #     system_date = i['date']
    system_date_1 = datetime.strptime(system_date, '%Y-%m-%d')
    print (system_date)

    # db_fzDB.JUP_DB_ATPCO_Temp_Dates_DBD.update({'file': 'change_file.py'}, {'$set': {'status': 'In Progress'}})

    cur = record_2_cat_10.find({"LAST_UPDATED_DATE" : datetime.strftime(system_date_1, '%Y-%m-%d'),"LAST_UPDATED_TIME" : file_time},no_cursor_timeout=True)
    system_date = datetime.strftime(system_date_1, "%Y%m%d")
    print ("starting rec 2 cat 10", cur.count())
    for i in cur:
        if i["ACTION"] == "2":
            #print "passed ACTION - 2"
            pass
        else:
            #print "querying changed coll 10"
            cur_1 = record_2_cat_10_main.find({"CXR_CODE":i["CXR_CODE"], "TARIFF":i["TARIFF"], "RULE_NO":i["RULE_NO"],
                                               "CAT_NO":i["CAT_NO"], "SEQ_NO":i["SEQ_NO"]})
            #print "queried"
            for j in cur_1:
                #print "-------------------------------------------"
                #print j["DATES_DISC_1"], "---- ORIGINAL DISC DATE"
                #print i["DATES_EFF_1"], "---- NEW EFF DATE"
                if (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] == system_date):
                    j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d"), "%y%m%d")
                    j["DATES_DISC_1"] = i["DATES_EFF_1"]
                elif (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] != system_date):
                    j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days = 1), "%y%m%d")
                    j["DATES_DISC_1"] = datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days = 1), "%Y%m%d")

                #print j["DATES_DISC_1"], "---- CHANGED ORIGINAL DISC DATE"
                record_2_cat_10_main.update({"_id":j["_id"]}, j)

        _id = i['_id']
        del i['_id']
        record_2_cat_10_main.insert(i)
        record_2_cat_10.remove({'_id': ObjectId(_id)})


def record_2_cat_25_change(system_date, file_time, client):
    db = client[ATPCO_DB]
    db_fzDB = client[JUPITER_DB]

    record_2_cat_25 = db.JUP_DB_ATPCO_Record_2_Cat_25_change
    record_2_cat_25_main = db.JUP_DB_ATPCO_Record_2_Cat_25

    # cr = db_fzDB.JUP_DB_ATPCO_Temp_Dates_DND.find({'file': 'change_file.py'})
    # for i in cr:
    #     system_date = i['date']
    system_date_1 = datetime.strptime(system_date, '%Y-%m-%d')
    print (system_date)

    # db_fzDB.JUP_DB_ATPCO_Temp_Dates_DBD.update({'file': 'change_file.py'}, {'$set': {'status': 'In Progress'}})

    cur = record_2_cat_25.find({"LAST_UPDATED_DATE" : datetime.strftime(system_date_1, '%Y-%m-%d'),"LAST_UPDATED_TIME" : file_time},no_cursor_timeout=True)
    system_date = datetime.strftime(system_date_1, "%Y%m%d")
    print ("starting rec 2 cat 25", cur.count())
    for i in cur:
        if i["ACTION"] == "2":
            #print "passed ACTION - 2"
            pass
        else:
            #print "querying changed coll 25"
            cur_1 = record_2_cat_25_main.find({"CXR_CODE":i["CXR_CODE"], "TARIFF":i["TARIFF"], "RULE_NO":i["RULE_NO"],
                                               "CAT_NO":i["CAT_NO"], "SEQ_NO":i["SEQ_NO"]})
            #print "queried"
            for j in cur_1:
               # print "-------------------------------------------"
                #print j["DATES_DISC_1"], "---- ORIGINAL DISC DATE"
                #print i["DATES_EFF_1"], "---- NEW EFF DATE"
                if (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] == system_date):
                    j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d"), "%y%m%d")
                    j["DATES_DISC_1"] = i["DATES_EFF_1"]
                elif (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] != system_date):
                    j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days = 1), "%y%m%d")
                    j["DATES_DISC_1"] = datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days = 1), "%Y%m%d")

                #print j["DATES_DISC_1"], "---- CHANGED ORIGINAL DISC DATE"
                record_2_cat_25_main.update({"_id":j["_id"]}, j)

        _id = i['_id']
        del i['_id']
        record_2_cat_25_main.insert(i)
        record_2_cat_25.remove({'_id': ObjectId(_id)})


def record_1_change(system_date, file_time, client):
    db = client[ATPCO_DB]
    db_fzDB = client[JUPITER_DB]

    record_1 = db.JUP_DB_ATPCO_Record_1_change
    record_1_main = db.JUP_DB_ATPCO_Record_1

    # cr = db_fzDB.JUP_DB_ATPCO_Temp_Dates_DND.find({'file': 'change_file.py'})
    # for i in cr:
    #     system_date = i['date']
    system_date_1 = datetime.strptime(system_date, '%Y-%m-%d')
    print (system_date)

    # db_fzDB.JUP_DB_ATPCO_Temp_Dates_DBD.update({'file': 'change_file.py'}, {'$set': {'status': 'In Progress'}})

    cur = record_1.find({"LAST_UPDATED_DATE" : datetime.strftime(system_date_1, '%Y-%m-%d'),"LAST_UPDATED_TIME" : file_time},no_cursor_timeout=True)
    system_date = datetime.strftime(system_date_1, "%Y%m%d")
    print ("Record_1 start", cur.count())
    count = 1
    for i in cur:
        if i["ACTION"] == "2":
            #print "passed ACTION - 2"
            pass
        else:
            #print "querying changed coll 1"
            cur_1 = record_1_main.find({"CXR_CODE":i["CXR_CODE"], "TARIFF":i["TARIFF"], "RULE_NO":i["RULE_NO"],
                                        "FARE_CLASS":i["FARE_CLASS"], "SEQ_NO":i["SEQ_NO"]})
            #print "queried"
            for j in cur_1:
                #print "-------------------------------------------"
                #print j["DATES_DISC_1"], "---- ORIGINAL DISC DATE"
                #print i["DATES_EFF_1"], "---- NEW EFF DATE"
                if (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] == system_date):
                    j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d"), "%y%m%d")
                    j["DATES_DISC_1"] = i["DATES_EFF_1"]
                elif (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] != system_date):
                    j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days = 1), "%y%m%d")
                    j["DATES_DISC_1"] = datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days = 1), "%Y%m%d")

                #print j["DATES_DISC_1"], "---- CHANGED ORIGINAL DISC DATE"
                record_1_main.update({"_id":j["_id"]}, j)

        _id = i['_id']
        del i['_id']
        record_1_main.insert(i)
        record_1.remove({'_id': ObjectId(_id)})
        print (count)
        count += 1
    print "Record_1 end"


def record_0_change(system_date, file_time,client):
    db = client[ATPCO_DB]
    db_fzDB = client[JUPITER_DB]

    record_0 = db.JUP_DB_ATPCO_Record_0_change
    record_0_main = db.JUP_DB_ATPCO_Record_0

    # cr = db_fzDB.JUP_DB_ATPCO_Temp_Dates_DND.find({'file': 'change_file.py'})
    # for i in cr:
    #     system_date = i['date']
    system_date_1 = datetime.strptime(system_date, '%Y-%m-%d')
    print (system_date)

    # db_fzDB.JUP_DB_ATPCO_Temp_Dates_DBD.update({'file': 'change_file.py'}, {'$set': {'status': 'In Progress'}})

    cur = record_0.find({"LAST_UPDATED_DATE" : datetime.strftime(system_date_1, '%Y-%m-%d'),"LAST_UPDATED_TIME" : file_time },no_cursor_timeout=True)
    system_date = datetime.strftime(system_date_1, "%Y%m%d")
    print ("starting rec 0", cur.count())
    for i in cur:
        if i["ACTION"] == "2":
            #print "passed ACTION - 2"
            pass
        else:
            #print "querying changed coll 0"
            cur_1 = record_0_main.find({"CXR_CODE":i["CXR_CODE"], "TARIFF":i["TARIFF"]})
            #print "queried"
            for j in cur_1:
                #print "-------------------------------------------"
                #print j["DATES_DISC_1"], "---- ORIGINAL DISC DATE"
                #print i["DATES_EFF_1"], "---- NEW EFF DATE"
                if (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] == system_date):
                    j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d"), "%y%m%d")
                    j["DATES_DISC_1"] = i["DATES_EFF_1"]
                elif (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] != system_date):
                    j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days = 1), "%y%m%d")
                    j["DATES_DISC_1"] = datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days = 1), "%Y%m%d")

                #print j["DATES_DISC_1"], "---- CHANGED ORIGINAL DISC DATE"
                record_0_main.update({"_id":j["_id"]}, j)

        _id = i['_id']
        del i['_id']
        record_0_main.insert(i)
        record_0.remove({'_id': ObjectId(_id)})


def record_8_change(system_date, file_time, client):
    db = client[ATPCO_DB]
    db_fzDB = client[JUPITER_DB]

    record_8 = db.JUP_DB_ATPCO_Record_8_change
    record_8_main = db.JUP_DB_ATPCO_Record_8

    # cr = db_fzDB.JUP_DB_ATPCO_Temp_Dates_DND.find({'file': 'change_file.py'})
    # for i in cr:
    #     system_date = i['date']
    system_date_1 = datetime.strptime(system_date, '%Y-%m-%d')
    print (system_date)

    # db_fzDB.JUP_DB_ATPCO_Temp_Dates_DBD.update({'file': 'change_file.py'}, {'$set': {'status': 'In Progress'}})
    cur = record_8.find({"LAST_UPDATED_DATE" : datetime.strftime(system_date_1, '%Y-%m-%d'),"LAST_UPDATED_TIME" : file_time  },no_cursor_timeout=True)
    system_date = datetime.strftime(system_date_1, "%Y%m%d")
    print ("starting rec 8", cur.count())
    for i in cur:
        if i["ACTION"] == "2":
            #print "passed ACTION - 2"
            pass
        else:
            #print "querying changed coll 8"
            cur_1 = record_8_main.find({"CXR_CODE":i["CXR_CODE"], "TARIFF":i["TARIFF"], "RULE_NO":i["RULE_NO"],
                                        "REC_ID":i["REC_ID"]})
            #print "queried"
            for j in cur_1:
                #print "-------------------------------------------"
                #print j["DATES_DISC_1"], "---- ORIGINAL DISC DATE"
                #print i["DATES_EFF_1"], "---- NEW EFF DATE"
                if (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] == system_date):
                    j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d"), "%y%m%d")
                    j["DATES_DISC_1"] = i["DATES_EFF_1"]
                elif (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] != system_date):
                    j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days = 1), "%y%m%d")
                    j["DATES_DISC_1"] = datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days = 1), "%Y%m%d")

                #print j["DATES_DISC_1"], "---- CHANGED ORIGINAL DISC DATE"
                record_8_main.update({"_id":j["_id"]}, j)

        _id = i['_id']
        del i['_id']
        record_8_main.insert(i)
        record_8.remove({'_id': ObjectId(_id)})


# def update_change_file_date():
#     cr = db_fzDB.JUP_DB_ATPCO_Temp_Dates_DND.find({'file':'change_file.py'})
#     for i in cr:
#         i['date'] = datetime.strftime(datetime.strptime(i['date'],'%Y-%m-%d')+timedelta(days=1),'%Y-%m-%d')
#         i['status'] = 'To Do'
#         db_fzDB.JUP_DB_ATPCO_Temp_Dates_DND.update({'_id':i['_id']}, i)


if __name__=='__main__':

    client = mongo_client()
    record_1_change('2017-10-09', "10", client)




