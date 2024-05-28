"""
Author: Abhinav Garg
Created with <3
Date Created: 2017-09-10
File Name: fr.py

This code maps the ATPCO fares to its associated rules and updates the relevant categories in
JUP_DB_ATPCO_Fares_Rules as a batch file.

"""
import json
import numpy as np
import pymongo
from bson import ObjectId
from pymongo import MongoClient, UpdateOne
import datetime
import time
import re
import math

from jupiter_AI import JUPITER_DB, ATPCO_DB, JUPITER_LOGGER
from jupiter_AI.logutils import measure


@measure(JUPITER_LOGGER)
def wild_card_check(r1, r2):
    rulesTT = r2.replace('-', '(.*)')
    rulesTTs = r2.replace('-', '(.*)')[(len(r2.replace('-', '(.*)')) - 4):] == '(.*)'

    if (r2.replace('-', '(.*)')[(len(r2.replace('-', '(.*)')) - 4):] == '(.*)'):
        pass
    else:
        rulesTT = rulesTT + '(.*)'
    _sysRegex = r"\b(?=\w)" + rulesTT + r"\b(?!\w)"
    if re.search(_sysRegex, r1, re.IGNORECASE):
        return True
    else:
        return False


@measure(JUPITER_LOGGER)
def run_stage_1(system_date,file_time, carrier_list, tariff_list, client):
    db_ATPCO = client[ATPCO_DB]
    db_fzDB = client[JUPITER_DB]
    fare_record = db_fzDB.JUP_DB_ATPCO_Fares_Rules
    temp_col = db_fzDB.TempSummaryColl
    record_0 = db_ATPCO.JUP_DB_ATPCO_Record_0
    record_1 = db_ATPCO.JUP_DB_ATPCO_Record_1
    record_2_cat_all = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_All
    record_2_cat_3_fn = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_03_FN
    record_2_cat_11_fn = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_11_FN
    record_2_cat_14_fn = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_14_FN
    record_2_cat_15_fn = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_15_FN
    record_2_cat_23_fn = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_23_FN
    record_2_cat_10 = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_10
    record_2_cat_25 = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_25
    record_3_cat_001 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_01
    record_3_cat_002 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_02
    record_3_cat_003 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_03
    record_3_cat_004 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_04
    record_3_cat_005 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_05
    record_3_cat_006 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_06
    record_3_cat_007 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_07
    record_3_cat_008 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_08
    record_3_cat_009 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_09
    record_3_cat_011 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_11
    record_3_cat_012 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_12
    record_3_cat_013 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_13
    record_3_cat_014 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_14
    record_3_cat_015 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_15
    record_3_cat_016 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_16
    record_3_cat_017 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_17
    record_3_cat_018 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_18
    record_3_cat_019 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_19
    record_3_cat_020 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_20
    record_3_cat_021 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_21
    record_3_cat_022 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_22
    record_3_cat_023 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_23
    record_3_cat_025 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_25
    record_3_cat_026 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_26
    record_3_cat_027 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_27
    record_3_cat_028 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_28
    record_3_cat_029 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_29
    record_3_cat_031 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_31
    record_3_cat_033 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_33
    record_3_cat_050 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_50
    record_3_cat_035 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_35
    record_3_cat_101 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_101
    record_3_cat_102 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_102
    record_3_cat_103 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_103
    record_3_cat_104 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_104
    record_3_cat_106 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_106
    record_3_cat_107 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_107
    record_3_cat_108 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_108
    record_3_cat_109 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_109
    record_3_cat_003_fn = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_03_FN
    record_3_cat_011_fn = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_11_FN
    record_3_cat_014_fn = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_14_FN
    record_3_cat_015_fn = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_15_FN
    record_3_cat_023_fn = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_23_FN
    record_3_table_986 = db_ATPCO.JUP_DB_ATPCO_Record_3_Table_986
    record_3_table_983 = db_ATPCO.JUP_DB_ATPCO_Record_3_Table_983
    system_date = datetime.datetime.strptime(system_date, '%Y-%m-%d')
    print system_date
    # today_date=datetime.datetime.strftime(system_date, "%Y-%m-%d")
    query_date = system_date - datetime.timedelta(days=1)
    query_date_with_dash = datetime.datetime.strftime(query_date, "%Y-%m-%d")
    query_date = datetime.datetime.strftime(query_date, "%Y%m%d")
    day_before_yesterday = datetime.datetime.strftime(system_date - datetime.timedelta(days=2), '%Y%m%d')
    day_before_yesterday_with_dash = datetime.datetime.strftime(system_date - datetime.timedelta(days=2), '%Y-%m-%d')
    fares_date = datetime.datetime.strftime(system_date, "%Y%m%d")
    fares_date_with_dash = datetime.datetime.strftime(system_date, "%Y-%m-%d")

    print query_date
    query = []
    query.append({'carrier': {'$in': carrier_list}, 'tariff_code': {'$in': tariff_list}, 'used': 0})  # for sanity check
    st_1 = time.time()
    print "getting change data"
    cur_record_0 = record_0.find(
        {"DATES_DISC_1": {"$gte": query_date}, "CXR_CODE": {"$in": carrier_list}, 'TARIFF': {'$in': tariff_list},
         "$or": [{"DATES_EFF_1": {"$gte": query_date}, "LAST_UPDATED_DATE": query_date_with_dash, "LAST_UPDATED_TIME" : {"$in" : file_time}}, {"DATES_EFF_1": {"$eq": day_before_yesterday},
                                                        "LAST_UPDATED_DATE": query_date_with_dash, "LAST_UPDATED_TIME" : {"$in" : file_time}}],
         })#.sort([("file_time" , pymongo.ASCENDING)])
    for i in cur_record_0:
        temp = {'carrier': i['CXR_CODE'], 'tariff_code': i['TARIFF'], "used": 1}
        query.append(temp)

    cur_record_1 = record_1.find(
        {"DATES_DISC_1": {"$gte": query_date}, "CXR_CODE": {"$in": carrier_list}, 'TARIFF': {'$in': tariff_list},
         "$or": [{"DATES_EFF_1": {"$gte": query_date},"LAST_UPDATED_DATE": query_date_with_dash, "LAST_UPDATED_TIME" : {"$in" : file_time}}, {"DATES_EFF_1": {"$eq": day_before_yesterday},
                                                        "LAST_UPDATED_DATE": query_date_with_dash, "LAST_UPDATED_TIME" : {"$in" : file_time}}],
         })#.sort([("file_time" , pymongo.ASCENDING)])
    for i in cur_record_1:
        temp = {'carrier': i['CXR_CODE'], 'tariff_code': i['TARIFF'], 'fare_rule': i['RULE_NO'],
                'fare_basis': i['FARE_CLASS'], "used": 1}
        query.append(temp)

    cur_record_2_cat_all = record_2_cat_all.find(
        {"DATES_DISC_1": {"$gte": query_date}, "CXR_CODE": {"$in": carrier_list}, 'TARIFF': {'$in': tariff_list},
         "$or": [{"DATES_EFF_1": {"$gte": query_date},"LAST_UPDATED_DATE": query_date_with_dash, "LAST_UPDATED_TIME" : {"$in" : file_time}}, {"DATES_EFF_1": {"$eq": day_before_yesterday},
                                                        "LAST_UPDATED_DATE": query_date_with_dash, "LAST_UPDATED_TIME" : {"$in" : file_time}}],
         })#.sort([("file_time" , pymongo.ASCENDING)])
    for i in cur_record_2_cat_all:
        temp = {'carrier': i['CXR_CODE'], 'tariff_code': i['TARIFF'], 'fare_rule': i['RULE_NO'], "used": 1}
        query.append(temp)

    cur_record_2_cat_10 = record_2_cat_10.find(
        {"DATES_DISC_1": {"$gte": query_date}, "CXR_CODE": {"$in": carrier_list}, 'TARIFF': {'$in': tariff_list},
         "$or": [{"DATES_EFF_1": {"$gte": query_date},"LAST_UPDATED_DATE": query_date_with_dash, "LAST_UPDATED_TIME" : {"$in" : file_time}}, {"DATES_EFF_1": {"$eq": day_before_yesterday},
                                                        "LAST_UPDATED_DATE": query_date_with_dash, "LAST_UPDATED_TIME" : {"$in" : file_time}}],
         })#.sort([("file_time" , pymongo.ASCENDING)])
    for i in cur_record_2_cat_10:
        temp = {'carrier': i['CXR_CODE'], 'tariff_code': i['TARIFF'], 'fare_rule': i['RULE_NO'], "used": 1}
        query.append(temp)

    cur_record_2_cat_25 = record_2_cat_25.find(
        {"DATES_DISC_1": {"$gte": query_date}, "CXR_CODE": {"$in": carrier_list}, 'TARIFF': {'$in': tariff_list},
         "$or": [{"DATES_EFF_1": {"$gte": query_date},"LAST_UPDATED_DATE": query_date_with_dash, "LAST_UPDATED_TIME" : {"$in" : file_time}}, {"DATES_EFF_1": {"$eq": day_before_yesterday},
                                                        "LAST_UPDATED_DATE": query_date_with_dash, "LAST_UPDATED_TIME" : {"$in" : file_time}}],
         })#.sort([("file_time" , pymongo.ASCENDING)])
    for i in cur_record_2_cat_25:
        temp = {'carrier': i['CXR_CODE'], 'tariff_code': i['TARIFF'], 'fare_rule': i['RULE_NO'], "used": 1}
        query.append(temp)

    cur_record_2_cat_3_fn = record_2_cat_3_fn.find(
        {"DATES_DISC_1": {"$gte": query_date}, "CXR_CODE": {"$in": carrier_list}, 'TARIFF': {'$in': tariff_list},
         "$or": [{"DATES_EFF_1": {"$gte": query_date},"LAST_UPDATED_DATE": query_date_with_dash, "LAST_UPDATED_TIME" : {"$in" : file_time}}, {"DATES_EFF_1": {"$eq": day_before_yesterday},
                                                        "LAST_UPDATED_DATE": query_date_with_dash, "LAST_UPDATED_TIME" : {"$in" : file_time}}],
         })#.sort([("file_time" , pymongo.ASCENDING)])
    for i in cur_record_2_cat_3_fn:
        temp = {'carrier': i['CXR_CODE'], 'tariff_code': i['TARIFF'], 'footnote': i['FT_NT'], "used": 1}
        query.append(temp)

    cur_record_2_cat_11_fn = record_2_cat_11_fn.find(
        {"DATES_DISC_1": {"$gte": query_date}, "CXR_CODE": {"$in": carrier_list}, 'TARIFF': {'$in': tariff_list},
         "$or": [{"DATES_EFF_1": {"$gte": query_date},"LAST_UPDATED_DATE": query_date_with_dash, "LAST_UPDATED_TIME" : {"$in" : file_time}}, {"DATES_EFF_1": {"$eq": day_before_yesterday},
                                                        "LAST_UPDATED_DATE": query_date_with_dash, "LAST_UPDATED_TIME" : {"$in" : file_time}}],
         })#.sort([("file_time" , pymongo.ASCENDING)])
    for i in cur_record_2_cat_11_fn:
        temp = {'carrier': i['CXR_CODE'], 'tariff_code': i['TARIFF'], 'footnote': i['FT_NT'], "used": 1}
        query.append(temp)

    cur_record_2_cat_14_fn = record_2_cat_14_fn.find(
        {"DATES_DISC_1": {"$gte": query_date}, "CXR_CODE": {"$in": carrier_list}, 'TARIFF': {'$in': tariff_list},
         "$or": [{"DATES_EFF_1": {"$gte": query_date},"LAST_UPDATED_DATE": query_date_with_dash, "LAST_UPDATED_TIME" : {"$in" : file_time}}, {"DATES_EFF_1": {"$eq": day_before_yesterday},
                                                        "LAST_UPDATED_DATE": query_date_with_dash, "LAST_UPDATED_TIME" : {"$in" : file_time}}],
         })#.sort([("file_time" , pymongo.ASCENDING)])
    for i in cur_record_2_cat_14_fn:
        temp = {'carrier': i['CXR_CODE'], 'tariff_code': i['TARIFF'], 'footnote': i['FT_NT'], "used": 1}
        query.append(temp)

    cur_record_2_cat_15_fn = record_2_cat_15_fn.find(
        {"DATES_DISC_1": {"$gte": query_date}, "CXR_CODE": {"$in": carrier_list}, 'TARIFF': {'$in': tariff_list},
         "$or": [{"DATES_EFF_1": {"$gte": query_date},"LAST_UPDATED_DATE": query_date_with_dash, "LAST_UPDATED_TIME" : {"$in" : file_time}}, {"DATES_EFF_1": {"$eq": day_before_yesterday},
                                                        "LAST_UPDATED_DATE": query_date_with_dash, "LAST_UPDATED_TIME" : {"$in" : file_time}}],
         })#.sort([("file_time" , pymongo.ASCENDING)])
    for i in cur_record_2_cat_15_fn:
        temp = {'carrier': i['CXR_CODE'], 'tariff_code': i['TARIFF'], 'footnote': i['FT_NT'], "used": 1}
        query.append(temp)

    cur_record_2_cat_23_fn = record_2_cat_23_fn.find(
        {"DATES_DISC_1": {"$gte": query_date}, "CXR_CODE": {"$in": carrier_list}, 'TARIFF': {'$in': tariff_list},
         "$or": [{"DATES_EFF_1": {"$gte": query_date},"LAST_UPDATED_DATE": query_date_with_dash, "LAST_UPDATED_TIME" : {"$in" : file_time}}, {"DATES_EFF_1": {"$eq": day_before_yesterday},
                                                        "LAST_UPDATED_DATE": query_date_with_dash, "LAST_UPDATED_TIME" : {"$in" : file_time}}],
         })#.sort([("file_time" , pymongo.ASCENDING)])
    for i in cur_record_2_cat_23_fn:
        temp = {'carrier': i['CXR_CODE'], 'tariff_code': i['TARIFF'], 'footnote': i['FT_NT'], "used": 1}
        query.append(temp)

    query.append({"carrier": {"$in": carrier_list},
                  "used": None,
                  'tariff_code': {'$in': tariff_list},
                  "$or": [{"effective_from": {"$lte": query_date_with_dash}, "effective_to": None},
                          {"effective_from": {"$lte": query_date_with_dash},
                           "effective_to": {"$gte": query_date_with_dash}},
                          {"effective_from": {"$lte": day_before_yesterday_with_dash},
                           "effective_to": day_before_yesterday_with_dash,
                           "file_date": query_date,'file_time' : {"$in" : file_time}},
                          {"effective_from": {"$gt": query_date_with_dash}, "effective_to": None},
                          {"effective_from": {"$gt": query_date_with_dash}, "effective_to": {"$gt": query_date_with_dash}}]})
    print "got change data in ", time.time() - st_1
    st_2 = time.time()
    num = 1
    Bulk = temp_col.initialize_unordered_bulk_op()
    Bulk1 = fare_record.initialize_unordered_bulk_op()
    query = list(np.unique(np.array(query)))

    print query
    ids = []
    counti = 1
    countj = 1
    for k in range(int(math.ceil(len(query) / 1000.0))):
        temp = []
        print "Iteration: ", k
        try:
            temp = query[k * 1000: (k + 1) * 1000]
        except IndexError:
            temp = query[k * 1000:]

        cur_fare_record = fare_record.find({"$or": temp}, no_cursor_timeout=True)
        print "stage 1 start"

        for i in cur_fare_record:

            if ((i["effective_from"] <= query_date_with_dash and i["effective_to"] == None) or \
                    (i["effective_from"] <= query_date_with_dash and i["effective_to"] >= query_date_with_dash) or \
                    (i["effective_from"] <= day_before_yesterday_with_dash and i[
                        "effective_to"] == day_before_yesterday_with_dash and i["file_date"] == query_date) or \
                    (i["effective_from"] > query_date_with_dash and i["effective_to"] == None) or (i["effective_from"] > query_date_with_dash and i["effective_from"] > i["effective_to"])):
                for m in range(1, 51):
                    try:
                        del i['cat_' + str(m)]
                    except KeyError:
                        pass
                for m in range(1, 10):
                    try:
                        del i['cat_10' + str(m)]
                    except KeyError:
                        pass
                try:
                    del i['Footnotes']
                except KeyError:
                    pass
                try:
                    del i['record 2 cat 10']
                except KeyError:
                    pass
                try:
                    del i['Gen Rules']
                except KeyError:
                    pass
                try:
                    del i['used']
                except KeyError:
                    pass
                try:
                    del i['Cat_14_FN_range']
                except KeyError:
                    pass
                try:
                    del i['Cat_15_FN_range']
                except KeyError:
                    pass


                print i["fare_basis"], i["fare_rule"], i["carrier"], i["tariff_code"]
                cur_record_1 = record_1.find(
                    {"FARE_CLASS": {"$eq": i["fare_basis"]}, "RULE_NO": {"$eq": i["fare_rule"]},
                     "CXR_CODE": {"$eq": i["carrier"]}, "TARIFF": {"$eq": i["tariff_code"]},
                     "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
                     "OW_RT": {"$in": ["", i["oneway_return"]]},
                     "FT_NT": {"$in": ["", i['footnote']]},
                     "DATES_EFF_1": {"$lte": fares_date},
                     "DATES_DISC_1": {"$gte": fares_date},
                     "LOC_1_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
                     "LOC_1_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
                     "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
                     "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
                     "LOC_2_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
                     "LOC_2_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
                     "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
                     "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
                     })

                print cur_record_1.count()
                for j in cur_record_1:
                    i['TYPE_CODES_FARE_TYPE'] = j['TYPE_CODES_FARE_TYPE']
                    i['TYPE_CODES_DAY_OF_WEEK_TYPE'] = j['TYPE_CODES_DAY_OF_WEEK_TYPE']
                    i['TYPE_CODES_SEASON_TYPE'] = j['TYPE_CODES_SEASON_TYPE']
                    i['fareID'] = i['fare_basis'] + i['fare_rule'] + i['footnote']
                    i["used"] = 0
                    ids.append(str(i['_id']))
                    try:
                        Bulk.find({
                            '_id': i['_id']
                        }).upsert().update({"$set": i})
                        if num % 100 == 0:
                            try:
                                Bulk.execute()
                                Bulk = temp_col.initialize_unordered_bulk_op()
                            except Exception as bwe:
                                print(bwe)
                    except Exception as error:
                        print(error)
                    print "inserted.....", countj
                    countj += 1
                    break

                try:
                    Bulk1.find({
                        '_id': i['_id']
                    }).update({"$set": i})
                    if num % 100 == 0:
                        try:
                            Bulk1.execute()
                            Bulk1 = fare_record.initialize_unordered_bulk_op()
                        except Exception as bwe:
                            print(bwe)
                    num = num + 1
                except Exception as error:
                    print(error)
                # temp_col.insert(i)
            print i['_id']
            print "done ----- ", counti
            counti += 1
        cur_fare_record.close()
    try:
        # for final part of update
        Bulk.execute()
        Bulk1.execute()
    except Exception as bwe:
        print(bwe)
    print "stage 1 complete in ", time.time() - st_2

    return ids


@measure(JUPITER_LOGGER)
def convert_to_objectid(x):
    return ObjectId(x)

@measure(JUPITER_LOGGER)
def update_stage_2_without_rec1(id_list, client):
    """
    Pushing from temp collection to main collection(ATPCO_Fares_Rules) collections
    :param id_list:
    :param client:
    :return:
    """
    num = 1
    db_fzDB = client[JUPITER_DB]
    Bulk = db_fzDB.JUP_DB_ATPCO_Fares_Rules.initialize_unordered_bulk_op()
    cur_2 = db_fzDB.TempSummaryColl.find({"_id": {"$in": id_list}}, no_cursor_timeout=True)
    for doc in cur_2:
        try:
            Bulk.find({
                '_id': doc["_id"]
            }).update(doc)
            if num % 100 == 0:
                try:
                    Bulk.execute()
                    Bulk = db_fzDB.JUP_DB_ATPCO_Fares_Rules.initialize_unordered_bulk_op()
                except Exception as bwe:
                    print(bwe)
            num = num + 1
        except Exception as error:
            print(error)
    try:
        Bulk.execute()
    except Exception as error:
        print(error)

@measure(JUPITER_LOGGER)
def run_stage_2(system_date, file_time, id_list, client):
    db_ATPCO = client[ATPCO_DB]
    db_fzDB = client[JUPITER_DB]
    fare_record = db_fzDB.JUP_DB_ATPCO_Fares_Rules
    temp_col = db_fzDB.TempSummaryColl
    record_0 = db_ATPCO.JUP_DB_ATPCO_Record_0
    record_1 = db_ATPCO.JUP_DB_ATPCO_Record_1
    record_2_cat_all = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_All
    record_2_cat_3_fn = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_03_FN
    record_2_cat_11_fn = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_11_FN
    record_2_cat_14_fn = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_14_FN
    record_2_cat_15_fn = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_15_FN
    record_2_cat_23_fn = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_23_FN
    record_2_cat_10 = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_10
    record_2_cat_25 = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_25
    record_3_cat_001 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_01
    record_3_cat_002 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_02
    record_3_cat_003 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_03
    record_3_cat_004 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_04
    record_3_cat_005 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_05
    record_3_cat_006 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_06
    record_3_cat_007 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_07
    record_3_cat_008 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_08
    record_3_cat_009 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_09
    record_3_cat_011 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_11
    record_3_cat_012 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_12
    record_3_cat_013 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_13
    record_3_cat_014 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_14
    record_3_cat_015 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_15
    record_3_cat_016 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_16
    record_3_cat_017 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_17
    record_3_cat_018 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_18
    record_3_cat_019 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_19
    record_3_cat_020 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_20
    record_3_cat_021 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_21
    record_3_cat_022 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_22
    record_3_cat_023 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_23
    record_3_cat_025 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_25
    record_3_cat_026 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_26
    record_3_cat_027 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_27
    record_3_cat_028 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_28
    record_3_cat_029 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_29
    record_3_cat_031 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_31
    record_3_cat_033 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_33
    record_3_cat_050 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_50
    record_3_cat_035 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_35
    record_3_cat_101 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_101
    record_3_cat_102 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_102
    record_3_cat_103 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_103
    record_3_cat_104 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_104
    record_3_cat_106 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_106
    record_3_cat_107 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_107
    record_3_cat_108 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_108
    record_3_cat_109 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_109
    record_3_cat_003_fn = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_03_FN
    record_3_cat_011_fn = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_11_FN
    record_3_cat_014_fn = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_14_FN
    record_3_cat_015_fn = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_15_FN
    record_3_cat_023_fn = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_23_FN
    record_3_table_986 = db_ATPCO.JUP_DB_ATPCO_Record_3_Table_986
    record_3_table_983 = db_ATPCO.JUP_DB_ATPCO_Record_3_Table_983
    vec = np.vectorize(convert_to_objectid)
    ids = list(vec(id_list))
    system_date = datetime.datetime.strptime(system_date, '%Y-%m-%d')
    print system_date
    today_date = datetime.datetime.strftime(system_date, "%Y-%m-%d")
    query_date = system_date - datetime.timedelta(days=1)
    query_date_with_dash = datetime.datetime.strftime(query_date, "%Y-%m-%d")
    query_date = datetime.datetime.strftime(query_date, "%Y%m%d")
    day_before_yesterday = datetime.datetime.strftime(system_date - datetime.timedelta(days=2), '%Y%m%d')
    day_before_yesterday_with_dash = datetime.datetime.strftime(system_date - datetime.timedelta(days=2), '%Y-%m-%d')
    fares_date = datetime.datetime.strftime(system_date, "%Y%m%d")

    st_3 = time.time()
    t = 0
    bulk = []
    # temp_cur = fare_record.find({'_id': {'$in': ids}}, no_cursor_timeout=True)
    temp_cur = temp_col.find({'_id': {'$in': ids}}, no_cursor_timeout=True)
    count = 1
    print "stage 2 start"
    for i in temp_cur:
        restrictions = 0
        st = time.time()
        final_insert = {}

        countk = 1

        cur_record_0 = record_0.find({"TARIFF": {"$eq": i["tariff_code"]}, "CXR_CODE": {"$eq": i["carrier"]},
                                      "DATES_EFF_1": {"$lte": query_date},
                                      "DATES_DISC_1": {"$gte": query_date}})
        final_insert["Gen Rules"] = {}
        i["Gen Rules"] = {}
        for h in cur_record_0:

            for g in range(int(h["NO_SEGS"])):
                cur_record_2_cat_all = record_2_cat_all.find(
                    {"CXR_CODE": {"$eq": i["carrier"]}, "GEN_APPL": {"$eq": ""},
                     "CAT_NO": {"$eq": h["CAT_NO_" + str(g + 1)]},
                     "TARIFF": {"$eq": h["SCR_TARIFF_" + str(g + 1)]},
                     "RULE_NO": {"$eq": h["GENERAL_RULE_NO_" + str(g + 1)]}, "NO_APPL": {"$eq": ""},
                     "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
                     "OW_RT": {"$in": ["", i["oneway_return"]]},
                     "FT_NT": {"$in": ["", i['footnote']]},
                     "DATES_EFF_1": {"$lte": fares_date},
                     "DATES_DISC_1": {"$gte": fares_date},
                     "TYPE_CODES_DAY_OF_WEEK_TYPE": {"$in": ["", i["TYPE_CODES_DAY_OF_WEEK_TYPE"]]},
                     "TYPE_CODES_FARE_TYPE": {"$in": ["", i["TYPE_CODES_FARE_TYPE"]]},
                     "TYPE_CODES_SEASON_TYPE": {"$in": ["", i["TYPE_CODES_SEASON_TYPE"]]},
                     "LOC_1_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
                     "LOC_1_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
                     "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
                     "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
                     "LOC_2_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
                     "LOC_2_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
                     "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
                     "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
                     }).sort("SEQ_NO", 1)
                for j in cur_record_2_cat_all:
                    if wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "":

                        ri_list = {}
                        inside_cat = {}
                        tbl = 1
                        while (tbl <= 200):
                            try:

                                if inside_cat.get(j['DATA_TABLE_STRING_CAT_NO_' + str(tbl)]):
                                    inside_cat[j['DATA_TABLE_STRING_CAT_NO_' + str(tbl)]].append(
                                        j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                                else:
                                    inside_cat.update(
                                        {j['DATA_TABLE_STRING_CAT_NO_' + str(tbl)]: [
                                            j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]]})

                                ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j[
                                    "DATA_TABLE_STRING_RI_" + str(tbl)]
                                tbl += 1
                            except KeyError as e:
                                break

                        temp_list = []

                        for o in inside_cat.keys():
                            #print inside_cat[o]
                            exec "cur_1 = record_3_cat_" + str(o) + ".find({'TBL_NO':{'$in':inside_cat[o]}})"

                            for k in cur_1:
                                k["RI"] = ri_list[k["TBL_NO"]]
                                k["SEQ_NO"] = j["SEQ_NO"]
                                temp_list.append(k)

                        if (j["CAT_NO"] < "010"):
                            i["Gen Rules"]["cat_" + j["CAT_NO"][2]] = temp_list
                        else:
                            i["Gen Rules"]["cat_" + j["CAT_NO"][1:]] = temp_list
                        break

        cur_record_2_cat_all = record_2_cat_all.find(
            {"RULE_NO": {"$eq": i["fare_rule"]}, "CXR_CODE": {"$eq": i["carrier"]},
             "NO_APPL": {"$eq": ""}, "TARIFF": {"$eq": i["tariff_code"]},
             "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
             "OW_RT": {"$in": ["", i["oneway_return"]]},
             "FT_NT": {"$in": ["", i['footnote']]},
             "DATES_EFF_1": {"$lte": fares_date},
             "DATES_DISC_1": {"$gte": fares_date},
             "TYPE_CODES_DAY_OF_WEEK_TYPE": {"$in": ["", i["TYPE_CODES_DAY_OF_WEEK_TYPE"]]},
             "TYPE_CODES_FARE_TYPE": {"$in": ["", i["TYPE_CODES_FARE_TYPE"]]},
             "TYPE_CODES_SEASON_TYPE": {"$in": ["", i["TYPE_CODES_SEASON_TYPE"]]},
             "LOC_1_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
             "LOC_1_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
             "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
             "LOC_2_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
             "LOC_2_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
             "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
             }).sort([("CAT_NO", 1), ("SEQ_NO", 1)])
        cat_check = []
        for j in cur_record_2_cat_all:
            if ((j["CAT_NO"] not in cat_check) and (
                    wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "")):
                cat_check.append(j["CAT_NO"])
                restrictions += 1
                # tbl = 1
                # tbl_no_list = []
                ri_list = {}
                inside_cat = {}
                tbl = 1
                while (tbl <= 200):
                    try:

                        if inside_cat.get(j['DATA_TABLE_STRING_CAT_NO_' + str(tbl)]):
                            inside_cat[j['DATA_TABLE_STRING_CAT_NO_' + str(tbl)]].append(
                                j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                        else:
                            inside_cat.update(
                                {j['DATA_TABLE_STRING_CAT_NO_' + str(tbl)]: [
                                    j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]]})

                        ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                        tbl += 1
                    except KeyError as e:
                        break

                temp_list = []

                for o in inside_cat.keys():
                    print inside_cat[o]
                    exec "cur_1 = record_3_cat_" + str(o) + ".find({'TBL_NO':{'$in':inside_cat[o]}})"

                    for k in cur_1:
                        k["RI"] = ri_list[k["TBL_NO"]]
                        k["SEQ_NO"] = j["SEQ_NO"]
                        temp_list.append(k)

                if (j["CAT_NO"] < "010"):
                    i["cat_" + j["CAT_NO"][2]] = temp_list
                else:
                    i["cat_" + j["CAT_NO"][1:]] = temp_list

        cur_record_2_cat_10 = record_2_cat_10.find(
            {"RULE_NO": {"$eq": i["fare_rule"]}, "CXR_CODE": {"$eq": i["carrier"]},
             "NO_APPL": {"$eq": ""}, "TARIFF": {"$eq": i["tariff_code"]},
             "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
             "OW_RT": {"$in": ["", i["oneway_return"]]},
             "FT_NT": {"$in": ["", i['footnote']]},
             "DATES_EFF_1": {"$lte": fares_date},
             "DATES_DISC_1": {"$gte": fares_date},
             "TYPE_CODES_DAY_OF_WEEK_TYPE": {"$in": ["", i["TYPE_CODES_DAY_OF_WEEK_TYPE"]]},
             "TYPE_CODES_FARE_TYPE": {"$in": ["", i["TYPE_CODES_FARE_TYPE"]]},
             "TYPE_CODES_SEASON_TYPE": {"$in": ["", i["TYPE_CODES_SEASON_TYPE"]]},
             "LOC_1_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
             "LOC_1_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
             "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
             "LOC_2_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
             "LOC_2_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
             "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
             }).sort("SEQ_NO", 1)
        temp_list_1 = []
        temp_list_2 = []
        temp_list_3 = []
        temp_list_4 = []
        temp_list_6 = []
        temp_list_7 = []
        temp_list_8 = []
        temp_list_9 = []
        i["record 2 cat 10"] = {}
        for j in cur_record_2_cat_10:

            if (wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == ""):
                i['record 2 cat 10']['DATA_TABLE'] = j['DATA_TABLE']
                i['record 2 cat 10']['COMB_APPLICATION_CT2_CXR'] = j['COMB_APPLICATION_CT2_CXR']
                i['record 2 cat 10']['JT_CXR_TBL_NO'] = j['JT_CXR_TBL_NO']
                i['record 2 cat 10']['COMB_APPLICATION_FILLER'] = j['COMB_APPLICATION_FILLER']
                i['record 2 cat 10']['TARIFF'] = j['TARIFF']
                i['record 2 cat 10']['COMB_APPLICATION_ARB'] = j['COMB_APPLICATION_ARB']
                i['record 2 cat 10']['TYPE_CODES_SEASON_TYPE'] = j['TYPE_CODES_SEASON_TYPE']
                i['record 2 cat 10']['ACTION'] = j['ACTION']
                i['record 2 cat 10']['COMB_APPLICATION_T_R'] = j['COMB_APPLICATION_T_R']
                i['record 2 cat 10']['COMB_APPLICATION_DOJ'] = j['COMB_APPLICATION_DOJ']
                i['record 2 cat 10']['LOC_2_COUNTRY'] = j['LOC_2_COUNTRY']
                i['record 2 cat 10']['COMB_APPLICATION_CT2P_T_R'] = j['COMB_APPLICATION_CT2P_T_R']
                i['record 2 cat 10']['NO_APPL'] = j['NO_APPL']
                i['record 2 cat 10']['COMB_APPLICATION_F_C'] = j['COMB_APPLICATION_F_C']
                i['record 2 cat 10']['COMB_APPLICATION_SOJ'] = j['COMB_APPLICATION_SOJ']
                i['record 2 cat 10']['LOC_1_CITY'] = j['LOC_1_CITY']
                i['record 2 cat 10']['COMB_APPLICATION_END_T_R'] = j['COMB_APPLICATION_END_T_R']
                i['record 2 cat 10']['TYPE_CODES_FARE_TYPE'] = j['TYPE_CODES_FARE_TYPE']
                i['record 2 cat 10']['COMB_APPLICATION_END_CXR'] = j['COMB_APPLICATION_END_CXR']
                i['record 2 cat 10']['COMB_APPLICATION_CT2_T_R'] = j['COMB_APPLICATION_CT2_T_R']
                i['record 2 cat 10']['DATES_DISC'] = j['DATES_DISC_1']
                i['record 2 cat 10']['LOC_1_COUNTRY'] = j['LOC_1_COUNTRY']
                i['record 2 cat 10']['CXR_CODE'] = j['CXR_CODE']
                i['record 2 cat 10']['FILLER'] = j['FILLER']
                i['record 2 cat 10']['COMB_APPLICATION_CT2_F_C1'] = j['COMB_APPLICATION_CT2_F_C1']
                i['record 2 cat 10']['LOC_2_AREA'] = j['LOC_2_AREA']
                i['record 2 cat 10']['COMB_APPLICATION_CXR'] = j['COMB_APPLICATION_CXR']
                i['record 2 cat 10']['NO_SEGS'] = j['NO_SEGS']
                i['record 2 cat 10']['BATCH_NO'] = j['BATCH_NO']
                i['record 2 cat 10']['COMB_APPLICATION_CT2P_CXR'] = j['COMB_APPLICATION_CT2P_CXR']
                i['record 2 cat 10']['RULE_NO'] = j['RULE_NO']
                i['record 2 cat 10']['REC_TYPE'] = j['REC_TYPE']
                i['record 2 cat 10']['CAT_NO'] = j['CAT_NO']
                i['record 2 cat 10']['COMB_APPLICATION_FILLER3'] = j['COMB_APPLICATION_FILLER3']
                i['record 2 cat 10']['COMB_APPLICATION_FILLER2'] = j['COMB_APPLICATION_FILLER2']
                i['record 2 cat 10']['COMB_APPLICATION_FILLER4'] = j['COMB_APPLICATION_FILLER4']
                i['record 2 cat 10']['BATCH_CI'] = j['BATCH_CI']
                i['record 2 cat 10']['COMB_APPLICATION_CT2P_F_C'] = j['COMB_APPLICATION_CT2P_F_C']
                i['record 2 cat 10']['COMB_APPLICATION_END'] = j['COMB_APPLICATION_END']
                i['record 2 cat 10']['OW_RT'] = j['OW_RT']
                i['record 2 cat 10']['LOC_1_TYPE'] = j['LOC_1_TYPE']
                i['record 2 cat 10']['COMB_APPLICATION_O_D'] = j['COMB_APPLICATION_O_D']
                i['record 2 cat 10']['LOC_1_ZONE'] = j['LOC_1_ZONE']
                i['record 2 cat 10']['LOC_2'] = j['LOC_2']
                i['record 2 cat 10']['DATES_EFF'] = j['DATES_EFF_1']
                i['record 2 cat 10']['LOC_1_AREA'] = j['LOC_1_AREA']
                i['record 2 cat 10']['COMB_APPLICATION_CT2PLUS'] = j['COMB_APPLICATION_CT2PLUS']
                i['record 2 cat 10']['COMB_APPLICATION_END_F_C'] = j['COMB_APPLICATION_END_F_C']
                i['record 2 cat 10']['RTG_NO'] = j['RTG_NO']
                i['record 2 cat 10']['TYPE_CODES_DAY_OF_WEEK_TYPE'] = j['TYPE_CODES_DAY_OF_WEEK_TYPE']
                i['record 2 cat 10']['LOC_2_CITY'] = j['LOC_2_CITY']
                i['record 2 cat 10']['SEQ_NO'] = j['SEQ_NO']
                i['record 2 cat 10']['LOC_1'] = j['LOC_1']
                i['record 2 cat 10']['FT_NT'] = j['FT_NT']
                i['record 2 cat 10']['LOC_2_ZONE'] = j['LOC_2_ZONE']
                i['record 2 cat 10']['COMB_APPLICATION_CT2'] = j['COMB_APPLICATION_CT2']
                i['record 2 cat 10']['MCN'] = j['MCN']
                i['record 2 cat 10']['SAME_PT_TBL'] = j['SAME_PT_TBL']
                i['record 2 cat 10']['LOC_1_STATE'] = j['LOC_1_STATE']
                i['record 2 cat 10']['FARE_CLASS'] = j['FARE_CLASS']
                tbl = 1
                restrictions += 8
                tbl_no_list = []
                ri_list = {}
                while (tbl <= 200):
                    try:
                        tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                        ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                        tbl += 1
                    except KeyError as e:
                        break

                cur_1 = record_3_cat_101.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_1:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_1.append(k)
                i["cat_101"] = temp_list_1

                cur_2 = record_3_cat_102.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_2:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_2.append(k)
                i["cat_102"] = temp_list_2

                cur_3 = record_3_cat_103.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_3:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_3.append(k)
                i["cat_103"] = temp_list_3

                cur_4 = record_3_cat_104.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_4:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_4.append(k)
                i["cat_104"] = temp_list_4

                cur_6 = record_3_cat_106.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_6:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_6.append(k)
                i["cat_106"] = temp_list_6

                cur_7 = record_3_cat_107.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_7:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_7.append(k)
                i["cat_107"] = temp_list_7

                cur_8 = record_3_cat_108.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_8:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_8.append(k)
                i["cat_108"] = temp_list_8

                cur_9 = record_3_cat_109.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_9:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_9.append(k)
                i["cat_109"] = temp_list_9
                break

        cur_record_2_cat_25 = record_2_cat_25.find(
            {"RULE_NO": {"$eq": i["fare_rule"]}, "CXR_CODE": {"$eq": i["carrier"]},
             "NO_APPL": {"$eq": ""}, "TARIFF": {"$eq": i["tariff_code"]},
             "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
             "OW_RT": {"$in": ["", i["oneway_return"]]},
             "FT_NT": {"$in": ["", i['footnote']]},
             "DATES_EFF_1": {"$lte": fares_date},
             "DATES_DISC_1": {"$gte": fares_date},
             "TYPE_CODES_DAY_OF_WEEK_TYPE": {"$in": ["", i["TYPE_CODES_DAY_OF_WEEK_TYPE"]]},
             "TYPE_CODES_FARE_TYPE": {"$in": ["", i["TYPE_CODES_FARE_TYPE"]]},
             "TYPE_CODES_SEASON_TYPE": {"$in": ["", i["TYPE_CODES_SEASON_TYPE"]]},
             "LOC_1_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
             "LOC_1_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
             "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
             "LOC_2_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
             "LOC_2_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
             "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
             }).sort("SEQ_NO", 1)
        for j in cur_record_2_cat_25:
            print "inside record 2 cat 25"

            if wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "":
                tbl = 1
                restrictions += 1
                tbl_no_list = []
                ri_list = {}
                while (tbl <= 200):
                    try:
                        tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                        ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                        tbl += 1
                    except KeyError as e:
                        break

                cur_1 = record_3_cat_025.find({"TBL_NO": {"$in": tbl_no_list}})
                temp_list = []
                for k in cur_1:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list.append(k)
                i["cat_25"] = temp_list
                break

        final_insert["Footnotes"] = {}
        i["Footnotes"] = {}
        temp_list_3_fn = []
        temp_list_11_fn = []
        temp_list_14_fn = []
        temp_list_15_fn = []
        temp_list_23_fn = []

        cur_record_2_cat_3_fn = record_2_cat_3_fn.find(
            {"CXR_CODE": {"$eq": i["carrier"]}, "FT_NT": {"$eq": i["footnote"]},
             "TARIFF": {"$eq": i["tariff_code"]}, "NO_APPL": {"$eq": ""},
             "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
             "OW_RT": {"$in": ["", i["oneway_return"]]},
             "DATES_EFF_1": {"$lte": fares_date},
             "DATES_DISC_1": {"$gte": fares_date},
             "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
             "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
             }).sort("SEQ_NO", 1)
        for j in cur_record_2_cat_3_fn:

            if wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "":
                restrictions += 1
                tbl = 1
                tbl_no_list = []
                ri_list = {}
                while (tbl <= 200):
                    try:
                        tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                        ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                        tbl += 1
                    except KeyError as e:
                        break

                i["Footnotes"]["FN_ID"] = i["footnote"]
                i["Footnotes"]["Tariff"] = i["tariff_code"]

                cur_3_fn = record_3_cat_003_fn.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_3_fn:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_3_fn.append(k)
                i["Footnotes"]["Cat_03_FN"] = temp_list_3_fn
                break

        cur_record_2_cat_11_fn = record_2_cat_11_fn.find(
            {"CXR_CODE": {"$eq": i["carrier"]}, "FT_NT": {"$eq": i["footnote"]},
             "TARIFF": {"$eq": i["tariff_code"]}, "NO_APPL": {"$eq": ""},
             "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
             "OW_RT": {"$in": ["", i["oneway_return"]]},
             "DATES_EFF_1": {"$lte": fares_date},
             "DATES_DISC_1": {"$gte": fares_date},
             "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
             "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
             }).sort("SEQ_NO", 1)
        for j in cur_record_2_cat_11_fn:

            if wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "":
                restrictions += 1
                tbl = 1
                tbl_no_list = []
                ri_list = {}
                while (tbl <= 200):
                    try:
                        tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                        ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                        tbl += 1
                    except KeyError as e:
                        break

                i["Footnotes"]["FN_ID"] = i["footnote"]
                i["Footnotes"]["Tariff"] = i["tariff_code"]

                cur_11_fn = record_3_cat_011_fn.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_11_fn:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_11_fn.append(k)
                i["Footnotes"]["Cat_11_FN"] = temp_list_11_fn
                break

        cur_record_2_cat_14_fn = record_2_cat_14_fn.find(
            {"CXR_CODE": {"$eq": i["carrier"]}, "FT_NT": {"$eq": i["footnote"]},
             "TARIFF": {"$eq": i["tariff_code"]}, "NO_APPL": {"$eq": ""},
             "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
             "OW_RT": {"$in": ["", i["oneway_return"]]},
             "DATES_EFF_1": {"$lte": fares_date},
             "DATES_DISC_1": {"$gte": fares_date},
             "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
             "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
             }).sort("SEQ_NO", 1)
        for j in cur_record_2_cat_14_fn:

            if wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "":
                restrictions += 1
                tbl = 1
                tbl_no_list = []
                ri_list = {}
                while (tbl <= 200):
                    try:
                        tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                        ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                        tbl += 1
                    except KeyError as e:
                        break

                i["Footnotes"]["FN_ID"] = i["footnote"]
                i["Footnotes"]["Tariff"] = i["tariff_code"]

                cur_14_fn = record_3_cat_014_fn.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_14_fn:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_14_fn.append(k)
                i["Footnotes"]["Cat_14_FN"] = temp_list_14_fn
                break

        cur_record_2_cat_15_fn = record_2_cat_15_fn.find(
            {"CXR_CODE": {"$eq": i["carrier"]}, "FT_NT": {"$eq": i["footnote"]},
             "TARIFF": {"$eq": i["tariff_code"]}, "NO_APPL": {"$eq": ""},
             "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
             "OW_RT": {"$in": ["", i["oneway_return"]]},
             "DATES_EFF_1": {"$lte": fares_date},
             "DATES_DISC_1": {"$gte": fares_date},
             "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
             "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
             }).sort("SEQ_NO", 1)
        for j in cur_record_2_cat_15_fn:

            if wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "":
                restrictions += 1
                tbl = 1
                tbl_no_list = []
                ri_list = {}
                while (tbl <= 200):
                    try:
                        tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                        ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                        tbl += 1
                    except KeyError as e:
                        break

                i["Footnotes"]["FN_ID"] = i["footnote"]
                i["Footnotes"]["Tariff"] = i["tariff_code"]

                cur_15_fn = record_3_cat_015_fn.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_15_fn:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_15_fn.append(k)
                i["Footnotes"]["Cat_15_FN"] = temp_list_15_fn
                break

        cur_record_2_cat_23_fn = record_2_cat_23_fn.find(
            {"CXR_CODE": {"$eq": i["carrier"]}, "FT_NT": {"$eq": i["footnote"]},
             "TARIFF": {"$eq": i["tariff_code"]}, "NO_APPL": {"$eq": ""},
             "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
             "OW_RT": {"$in": ["", i["oneway_return"]]},
             "DATES_EFF_1": {"$lte": fares_date},
             "DATES_DISC_1": {"$gte": fares_date},
             "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
             "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
             }).sort("SEQ_NO", 1)
        for j in cur_record_2_cat_23_fn:
            if wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "":
                restrictions += 1
                tbl = 1
                tbl_no_list = []
                ri_list = {}
                while (tbl <= 200):
                    try:
                        tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                        ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                        tbl += 1
                    except KeyError as e:
                        break

                i["Footnotes"]["FN_ID"] = i["footnote"]
                i["Footnotes"]["Tariff"] = i["tariff_code"]

                cur_23_fn = record_3_cat_023_fn.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_23_fn:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_23_fn.append(k)
                i["Footnotes"]["Cat_23_FN"] = temp_list_23_fn
                break
        countj = 1
        count986 = 1
        try:
            for j in range(len(i["cat_4"])):
                cur_record_3_table_986 = record_3_table_986.find({"TBL_NO": {"$eq": i["cat_4"][j]["CXR_FLT"]}})
                for k in cur_record_3_table_986:
                    i["cat_4"][j]["CXR_FLT"] = k
                cur_record_3_table_986 = record_3_table_986.find({"TBL_NO": {"$eq": i["cat_4"][j]["CXR_TBL"]}})
                for k in cur_record_3_table_986:
                    i["cat_4"][j]["CXR_TBL"] = k
                countj += 1

            countj = 1
            for j in range(len(i["cat_12"])):
                cur_record_3_table_986 = record_3_table_986.find({"TBL_NO": {"$eq": i["cat_12"][j]["CXR_FLT_TBL_NO"]}})
                for k in cur_record_3_table_986:
                    i["cat_12"][j]["CXR_FLT_TBL_NO"] = k
                countj += 1

            count986 += 1
        except KeyError:
            pass


        # to create Cat_14_FN_range, Cat_15_FN_range
        rCat_14 = []
        rCat_15 = []
        try:
            for l in i["Footnotes"]["Cat_14_FN"]:
                if not (l["TRAVEL_DATES_COMM_1"] == "1900-01-01" and l["TRAVEL_DATES_EXP_1"] == "2099-12-31"):
                    range1 = l["TRAVEL_DATES_COMM_1"]
                    range2 = l["TRAVEL_DATES_EXP_1"]
                    rangex = {"start": range1, "stop": range2}
                    rCat_14.append(rangex)
        except KeyError:
            pass

        try:
            for l in i["Footnotes"]["Cat_15_FN"]:
                if not (l["SALE_DATES_EARLIEST_TKTG_1"] == "1900-01-01" and l[
                    "SALE_DATES_LATEST_TKTG_1"] == "2099-12-31"):
                    range1 = l["SALE_DATES_EARLIEST_TKTG_1"]
                    range2 = l["SALE_DATES_LATEST_TKTG_1"]
                    rangex = {"start": range1, "stop": range2}
                    rCat_15.append(rangex)
        except KeyError:
            pass

        try:
            for l in i["cat_14"]:
                if not (l["TRAVEL_DATES_COMM_1"] == "1900-01-01" and l["TRAVEL_DATES_EXP_1"] == "2099-12-31"):
                    range1 = l["TRAVEL_DATES_COMM_1"]
                    range2 = l["TRAVEL_DATES_EXP_1"]
                    rangex = {"start": range1, "stop": range2}
                    rCat_14.append(rangex)
        except KeyError:
            pass

        try:
            for l in i["cat_15"]:
                if not (l["SALE_DATES_EARLIEST_TKTG_1"] == "1900-01-01" and l[
                    "SALE_DATES_LATEST_TKTG_1"] == "2099-12-31"):
                    range1 = l["SALE_DATES_EARLIEST_TKTG_1"]
                    range2 = l["SALE_DATES_LATEST_TKTG_1"]
                    rangex = {"start": range1, "stop": range2}
                    rCat_15.append(rangex)
        except KeyError:
            pass

        if not rCat_14:
            rCat_14.append({"start": "1900-01-01", "stop": "2099-12-31"})
        if not rCat_15:
            rCat_15.append({"start": "1900-01-01", "stop": "2099-12-31"})

        i["Cat_14_FN_range"]=rCat_14
        i["Cat_15_FN_range"]=rCat_15
        
        i["used"] = 1
        i["fare_rules_date"] = today_date
        i['restrictions'] = restrictions
        print i["_id"]
        if t == 100:
            print 'updating:', count
            fare_record.bulk_write(bulk)
            bulk = []
            t = 0
            bulk.append(UpdateOne({'_id': i['_id']}, {'$set': i}))
            t += 1
        else:
            bulk.append(UpdateOne({'_id': i['_id']}, {'$set': i}))
            t += 1
        print count, "  ", time.time() - st
        count += 1
    if len(bulk) > 0:
        print 'updating:', count
        fare_record.bulk_write(bulk)
    temp_cur.close()
    print "stage 2 complete in ", time.time() - st_3


@measure(JUPITER_LOGGER)
def stage_2_helper(system_date, client):
    db_ATPCO = client[ATPCO_DB]
    db_fzDB = client[JUPITER_DB]
    fare_record = db_fzDB.JUP_DB_ATPCO_Fares_Rules
    record_0 = db_ATPCO.JUP_DB_ATPCO_Record_0
    record_1 = db_ATPCO.JUP_DB_ATPCO_Record_1
    record_2_cat_all = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_All
    record_2_cat_3_fn = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_03_FN
    record_2_cat_11_fn = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_11_FN
    record_2_cat_14_fn = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_14_FN
    record_2_cat_15_fn = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_15_FN
    record_2_cat_23_fn = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_23_FN
    record_2_cat_10 = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_10
    record_2_cat_25 = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_25
    record_3_cat_001 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_01
    record_3_cat_002 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_02
    record_3_cat_003 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_03
    record_3_cat_004 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_04
    record_3_cat_005 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_05
    record_3_cat_006 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_06
    record_3_cat_007 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_07
    record_3_cat_008 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_08
    record_3_cat_009 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_09
    record_3_cat_011 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_11
    record_3_cat_012 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_12
    record_3_cat_013 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_13
    record_3_cat_014 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_14
    record_3_cat_015 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_15
    record_3_cat_016 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_16
    record_3_cat_017 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_17
    record_3_cat_018 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_18
    record_3_cat_019 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_19
    record_3_cat_020 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_20
    record_3_cat_021 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_21
    record_3_cat_022 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_22
    record_3_cat_023 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_23
    record_3_cat_025 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_25
    record_3_cat_026 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_26
    record_3_cat_027 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_27
    record_3_cat_028 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_28
    record_3_cat_029 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_29
    record_3_cat_031 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_31
    record_3_cat_033 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_33
    record_3_cat_050 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_50
    record_3_cat_035 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_35
    record_3_cat_101 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_101
    record_3_cat_102 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_102
    record_3_cat_103 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_103
    record_3_cat_104 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_104
    record_3_cat_106 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_106
    record_3_cat_107 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_107
    record_3_cat_108 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_108
    record_3_cat_109 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_109
    record_3_cat_003_fn = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_03_FN
    record_3_cat_011_fn = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_11_FN
    record_3_cat_014_fn = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_14_FN
    record_3_cat_015_fn = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_15_FN
    record_3_cat_023_fn = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_23_FN
    record_3_table_986 = db_ATPCO.JUP_DB_ATPCO_Record_3_Table_986
    record_3_table_983 = db_ATPCO.JUP_DB_ATPCO_Record_3_Table_983
    system_date = datetime.datetime.strptime(system_date, '%Y-%m-%d')
    print system_date

    query_date = system_date - datetime.timedelta(days=1)
    query_date_with_dash = datetime.datetime.strftime(query_date, "%Y-%m-%d")
    query_date = datetime.datetime.strftime(query_date, "%Y%m%d")
    day_before_yesterday = datetime.datetime.strftime(system_date - datetime.timedelta(days=2), '%Y%m%d')
    day_before_yesterday_with_dash = datetime.datetime.strftime(system_date - datetime.timedelta(days=2), '%Y-%m-%d')
    fares_date = datetime.datetime.strftime(system_date, "%Y-%m-%d")

    st_3 = time.time()
    t = 0
    bulk = []
    temp_cur = fare_record.find({'used': 0}, no_cursor_timeout=True)
    count = 1
    print "stage 2 start"
    for i in temp_cur:
        restrictions = 0
        st = time.time()
        final_insert = {}

        countk = 1

        cur_record_0 = record_0.find({"TARIFF": {"$eq": i["tariff_code"]}, "CXR_CODE": {"$eq": i["carrier"]},
                                      "DATES_EFF_1": {"$lte": query_date},
                                      "DATES_DISC_1": {"$gte": query_date}})
        final_insert["Gen Rules"] = {}
        i["Gen Rules"] = {}
        for h in cur_record_0:

            for g in range(int(h["NO_SEGS"])):
                cur_record_2_cat_all = record_2_cat_all.find(
                    {"CXR_CODE": {"$eq": i["carrier"]}, "GEN_APPL": {"$eq": ""},
                     "CAT_NO": {"$eq": h["CAT_NO_" + str(g + 1)]},
                     "TARIFF": {"$eq": h["SCR_TARIFF_" + str(g + 1)]},
                     "RULE_NO": {"$eq": h["GENERAL_RULE_NO_" + str(g + 1)]}, "NO_APPL": {"$eq": ""},
                     "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
                     "OW_RT": {"$in": ["", i["oneway_return"]]},
                     "FT_NT": {"$in": ["", i['footnote']]},
                     "DATES_EFF_1": {"$lte": query_date},
                     "DATES_DISC_1": {"$gte": query_date},
                     "TYPE_CODES_DAY_OF_WEEK_TYPE": {"$in": ["", i["TYPE_CODES_DAY_OF_WEEK_TYPE"]]},
                     "TYPE_CODES_FARE_TYPE": {"$in": ["", i["TYPE_CODES_FARE_TYPE"]]},
                     "TYPE_CODES_SEASON_TYPE": {"$in": ["", i["TYPE_CODES_SEASON_TYPE"]]},
                     "LOC_1_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
                     "LOC_1_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
                     "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
                     "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
                     "LOC_2_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
                     "LOC_2_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
                     "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
                     "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
                     }).sort("SEQ_NO", 1)
                for j in cur_record_2_cat_all:
                    if wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "":
                        tbl = 1
                        tbl_no_list = []
                        ri_list = {}
                        while (tbl <= 200):
                            try:
                                tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                                ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j[
                                    "DATA_TABLE_STRING_RI_" + str(tbl)]
                                tbl += 1
                            except KeyError as e:
                                break

                        exec "cur_1 = record_3_cat_" + str(j['CAT_NO']) + ".find({'TBL_NO': {'$in': tbl_no_list}})"
                        temp_list = []
                        for k in cur_1:
                            k["RI"] = ri_list[k["TBL_NO"]]
                            k["SEQ_NO"] = j["SEQ_NO"]
                            temp_list.append(k)
                        if (j["CAT_NO"] < "010"):
                            i["Gen Rules"]["cat_" + j["CAT_NO"][2]] = temp_list
                        else:
                            i["Gen Rules"]["cat_" + j["CAT_NO"][1:]] = temp_list
                        break

        cur_record_2_cat_all = record_2_cat_all.find(
            {"RULE_NO": {"$eq": i["fare_rule"]}, "CXR_CODE": {"$eq": i["carrier"]},
             "NO_APPL": {"$eq": ""}, "TARIFF": {"$eq": i["tariff_code"]},
             "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
             "OW_RT": {"$in": ["", i["oneway_return"]]},
             "FT_NT": {"$in": ["", i['footnote']]},
             "DATES_EFF_1": {"$lte": query_date},
             "DATES_DISC_1": {"$gte": query_date},
             "TYPE_CODES_DAY_OF_WEEK_TYPE": {"$in": ["", i["TYPE_CODES_DAY_OF_WEEK_TYPE"]]},
             "TYPE_CODES_FARE_TYPE": {"$in": ["", i["TYPE_CODES_FARE_TYPE"]]},
             "TYPE_CODES_SEASON_TYPE": {"$in": ["", i["TYPE_CODES_SEASON_TYPE"]]},
             "LOC_1_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
             "LOC_1_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
             "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
             "LOC_2_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
             "LOC_2_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
             "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
             }).sort([("CAT_NO", 1), ("SEQ_NO", 1)])
        cat_check = []
        for j in cur_record_2_cat_all:
            if ((j["CAT_NO"] not in cat_check) and (
                    wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "")):
                cat_check.append(j["CAT_NO"])
                restrictions += 1
                tbl = 1
                tbl_no_list = []
                ri_list = {}
                while (tbl <= 200):
                    try:
                        tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                        ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                        tbl += 1
                    except KeyError as e:
                        break

                exec "cur_1 = record_3_cat_" + str(j['CAT_NO']) + ".find({'TBL_NO':{'$in':tbl_no_list}})"
                temp_list = []
                for k in cur_1:
                    k["RI"] = ri_list[k["TBL_NO"]]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list.append(k)
                if (j["CAT_NO"] < "010"):
                    i["cat_" + j["CAT_NO"][2]] = temp_list
                else:
                    i["cat_" + j["CAT_NO"][1:]] = temp_list

        cur_record_2_cat_10 = record_2_cat_10.find(
            {"RULE_NO": {"$eq": i["fare_rule"]}, "CXR_CODE": {"$eq": i["carrier"]},
             "NO_APPL": {"$eq": ""}, "TARIFF": {"$eq": i["tariff_code"]},
             "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
             "OW_RT": {"$in": ["", i["oneway_return"]]},
             "FT_NT": {"$in": ["", i['footnote']]},
             "DATES_EFF_1": {"$lte": query_date},
             "DATES_DISC_1": {"$gte": query_date},
             "TYPE_CODES_DAY_OF_WEEK_TYPE": {"$in": ["", i["TYPE_CODES_DAY_OF_WEEK_TYPE"]]},
             "TYPE_CODES_FARE_TYPE": {"$in": ["", i["TYPE_CODES_FARE_TYPE"]]},
             "TYPE_CODES_SEASON_TYPE": {"$in": ["", i["TYPE_CODES_SEASON_TYPE"]]},
             "LOC_1_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
             "LOC_1_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
             "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
             "LOC_2_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
             "LOC_2_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
             "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
             }).sort("SEQ_NO", 1)
        temp_list_1 = []
        temp_list_2 = []
        temp_list_3 = []
        temp_list_4 = []
        temp_list_6 = []
        temp_list_7 = []
        temp_list_8 = []
        temp_list_9 = []
        i["record 2 cat 10"] = {}
        for j in cur_record_2_cat_10:

            if (wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == ""):
                i['record 2 cat 10']['DATA_TABLE'] = j['DATA_TABLE']
                i['record 2 cat 10']['COMB_APPLICATION_CT2_CXR'] = j['COMB_APPLICATION_CT2_CXR']
                i['record 2 cat 10']['JT_CXR_TBL_NO'] = j['JT_CXR_TBL_NO']
                i['record 2 cat 10']['COMB_APPLICATION_FILLER'] = j['COMB_APPLICATION_FILLER']
                i['record 2 cat 10']['TARIFF'] = j['TARIFF']
                i['record 2 cat 10']['COMB_APPLICATION_ARB'] = j['COMB_APPLICATION_ARB']
                i['record 2 cat 10']['TYPE_CODES_SEASON_TYPE'] = j['TYPE_CODES_SEASON_TYPE']
                i['record 2 cat 10']['ACTION'] = j['ACTION']
                i['record 2 cat 10']['COMB_APPLICATION_T_R'] = j['COMB_APPLICATION_T_R']
                i['record 2 cat 10']['COMB_APPLICATION_DOJ'] = j['COMB_APPLICATION_DOJ']
                i['record 2 cat 10']['LOC_2_COUNTRY'] = j['LOC_2_COUNTRY']
                i['record 2 cat 10']['COMB_APPLICATION_CT2P_T_R'] = j['COMB_APPLICATION_CT2P_T_R']
                i['record 2 cat 10']['NO_APPL'] = j['NO_APPL']
                i['record 2 cat 10']['COMB_APPLICATION_F_C'] = j['COMB_APPLICATION_F_C']
                i['record 2 cat 10']['COMB_APPLICATION_SOJ'] = j['COMB_APPLICATION_SOJ']
                i['record 2 cat 10']['LOC_1_CITY'] = j['LOC_1_CITY']
                i['record 2 cat 10']['COMB_APPLICATION_END_T_R'] = j['COMB_APPLICATION_END_T_R']
                i['record 2 cat 10']['TYPE_CODES_FARE_TYPE'] = j['TYPE_CODES_FARE_TYPE']
                i['record 2 cat 10']['COMB_APPLICATION_END_CXR'] = j['COMB_APPLICATION_END_CXR']
                i['record 2 cat 10']['COMB_APPLICATION_CT2_T_R'] = j['COMB_APPLICATION_CT2_T_R']
                i['record 2 cat 10']['DATES_DISC'] = j['DATES_DISC_1']
                i['record 2 cat 10']['LOC_1_COUNTRY'] = j['LOC_1_COUNTRY']
                i['record 2 cat 10']['CXR_CODE'] = j['CXR_CODE']
                i['record 2 cat 10']['FILLER'] = j['FILLER']
                i['record 2 cat 10']['COMB_APPLICATION_CT2_F_C1'] = j['COMB_APPLICATION_CT2_F_C1']
                i['record 2 cat 10']['LOC_2_AREA'] = j['LOC_2_AREA']
                i['record 2 cat 10']['COMB_APPLICATION_CXR'] = j['COMB_APPLICATION_CXR']
                i['record 2 cat 10']['NO_SEGS'] = j['NO_SEGS']
                i['record 2 cat 10']['BATCH_NO'] = j['BATCH_NO']
                i['record 2 cat 10']['COMB_APPLICATION_CT2P_CXR'] = j['COMB_APPLICATION_CT2P_CXR']
                i['record 2 cat 10']['RULE_NO'] = j['RULE_NO']
                i['record 2 cat 10']['REC_TYPE'] = j['REC_TYPE']
                i['record 2 cat 10']['CAT_NO'] = j['CAT_NO']
                i['record 2 cat 10']['COMB_APPLICATION_FILLER3'] = j['COMB_APPLICATION_FILLER3']
                i['record 2 cat 10']['COMB_APPLICATION_FILLER2'] = j['COMB_APPLICATION_FILLER2']
                i['record 2 cat 10']['COMB_APPLICATION_FILLER4'] = j['COMB_APPLICATION_FILLER4']
                i['record 2 cat 10']['BATCH_CI'] = j['BATCH_CI']
                i['record 2 cat 10']['COMB_APPLICATION_CT2P_F_C'] = j['COMB_APPLICATION_CT2P_F_C']
                i['record 2 cat 10']['COMB_APPLICATION_END'] = j['COMB_APPLICATION_END']
                i['record 2 cat 10']['OW_RT'] = j['OW_RT']
                i['record 2 cat 10']['LOC_1_TYPE'] = j['LOC_1_TYPE']
                i['record 2 cat 10']['COMB_APPLICATION_O_D'] = j['COMB_APPLICATION_O_D']
                i['record 2 cat 10']['LOC_1_ZONE'] = j['LOC_1_ZONE']
                i['record 2 cat 10']['LOC_2'] = j['LOC_2']
                i['record 2 cat 10']['DATES_EFF'] = j['DATES_EFF_1']
                i['record 2 cat 10']['LOC_1_AREA'] = j['LOC_1_AREA']
                i['record 2 cat 10']['COMB_APPLICATION_CT2PLUS'] = j['COMB_APPLICATION_CT2PLUS']
                i['record 2 cat 10']['COMB_APPLICATION_END_F_C'] = j['COMB_APPLICATION_END_F_C']
                i['record 2 cat 10']['RTG_NO'] = j['RTG_NO']
                i['record 2 cat 10']['TYPE_CODES_DAY_OF_WEEK_TYPE'] = j['TYPE_CODES_DAY_OF_WEEK_TYPE']
                i['record 2 cat 10']['LOC_2_CITY'] = j['LOC_2_CITY']
                i['record 2 cat 10']['SEQ_NO'] = j['SEQ_NO']
                i['record 2 cat 10']['LOC_1'] = j['LOC_1']
                i['record 2 cat 10']['FT_NT'] = j['FT_NT']
                i['record 2 cat 10']['LOC_2_ZONE'] = j['LOC_2_ZONE']
                i['record 2 cat 10']['COMB_APPLICATION_CT2'] = j['COMB_APPLICATION_CT2']
                i['record 2 cat 10']['MCN'] = j['MCN']
                i['record 2 cat 10']['SAME_PT_TBL'] = j['SAME_PT_TBL']
                i['record 2 cat 10']['LOC_1_STATE'] = j['LOC_1_STATE']
                i['record 2 cat 10']['FARE_CLASS'] = j['FARE_CLASS']
                tbl = 1
                restrictions += 8
                tbl_no_list = []
                ri_list = {}
                while (tbl <= 200):
                    try:
                        tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                        ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                        tbl += 1
                    except KeyError as e:
                        break

                cur_1 = record_3_cat_101.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_1:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_1.append(k)
                i["cat_101"] = temp_list_1

                cur_2 = record_3_cat_102.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_2:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_2.append(k)
                i["cat_102"] = temp_list_2

                cur_3 = record_3_cat_103.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_3:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_3.append(k)
                i["cat_103"] = temp_list_3

                cur_4 = record_3_cat_104.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_4:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_4.append(k)
                i["cat_104"] = temp_list_4

                cur_6 = record_3_cat_106.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_6:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_6.append(k)
                i["cat_106"] = temp_list_6

                cur_7 = record_3_cat_107.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_7:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_7.append(k)
                i["cat_107"] = temp_list_7

                cur_8 = record_3_cat_108.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_8:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_8.append(k)
                i["cat_108"] = temp_list_8

                cur_9 = record_3_cat_109.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_9:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_9.append(k)
                i["cat_109"] = temp_list_9
                break

        cur_record_2_cat_25 = record_2_cat_25.find(
            {"RULE_NO": {"$eq": i["fare_rule"]}, "CXR_CODE": {"$eq": i["carrier"]},
             "NO_APPL": {"$eq": ""}, "TARIFF": {"$eq": i["tariff_code"]},
             "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
             "OW_RT": {"$in": ["", i["oneway_return"]]},
             "FT_NT": {"$in": ["", i['footnote']]},
             "DATES_EFF_1": {"$lte": query_date},
             "DATES_DISC_1": {"$gte": query_date},
             "TYPE_CODES_DAY_OF_WEEK_TYPE": {"$in": ["", i["TYPE_CODES_DAY_OF_WEEK_TYPE"]]},
             "TYPE_CODES_FARE_TYPE": {"$in": ["", i["TYPE_CODES_FARE_TYPE"]]},
             "TYPE_CODES_SEASON_TYPE": {"$in": ["", i["TYPE_CODES_SEASON_TYPE"]]},
             "LOC_1_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
             "LOC_1_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
             "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
             "LOC_2_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
             "LOC_2_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
             "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
             }).sort("SEQ_NO", 1)
        for j in cur_record_2_cat_25:
            print "inside record 2 cat 25"

            if wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "":
                tbl = 1
                restrictions += 1
                tbl_no_list = []
                ri_list = {}
                while (tbl <= 200):
                    try:
                        tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                        ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                        tbl += 1
                    except KeyError as e:
                        break

                cur_1 = record_3_cat_025.find({"TBL_NO": {"$in": tbl_no_list}})
                temp_list = []
                for k in cur_1:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list.append(k)
                i["cat_25"] = temp_list
                break

        final_insert["Footnotes"] = {}
        i["Footnotes"] = {}
        temp_list_3_fn = []
        temp_list_11_fn = []
        temp_list_14_fn = []
        temp_list_15_fn = []
        temp_list_23_fn = []

        cur_record_2_cat_3_fn = record_2_cat_3_fn.find(
            {"CXR_CODE": {"$eq": i["carrier"]}, "FT_NT": {"$eq": i["footnote"]},
             "TARIFF": {"$eq": i["tariff_code"]}, "NO_APPL": {"$eq": ""},
             "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
             "OW_RT": {"$in": ["", i["oneway_return"]]},
             "DATES_EFF_1": {"$lte": query_date},
             "DATES_DISC_1": {"$gte": query_date},
             "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
             "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
             }).sort("SEQ_NO", 1)
        for j in cur_record_2_cat_3_fn:

            if wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "":
                restrictions += 1
                tbl = 1
                tbl_no_list = []
                ri_list = {}
                while (tbl <= 200):
                    try:
                        tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                        ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                        tbl += 1
                    except KeyError as e:
                        break

                i["Footnotes"]["FN_ID"] = i["footnote"]
                i["Footnotes"]["Tariff"] = i["tariff_code"]

                cur_3_fn = record_3_cat_003_fn.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_3_fn:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_3_fn.append(k)
                i["Footnotes"]["Cat_03_FN"] = temp_list_3_fn
                break

        cur_record_2_cat_11_fn = record_2_cat_11_fn.find(
            {"CXR_CODE": {"$eq": i["carrier"]}, "FT_NT": {"$eq": i["footnote"]},
             "TARIFF": {"$eq": i["tariff_code"]}, "NO_APPL": {"$eq": ""},
             "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
             "OW_RT": {"$in": ["", i["oneway_return"]]},
             "DATES_EFF_1": {"$lte": query_date},
             "DATES_DISC_1": {"$gte": query_date},
             "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
             "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
             }).sort("SEQ_NO", 1)
        for j in cur_record_2_cat_11_fn:

            if wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "":
                restrictions += 1
                tbl = 1
                tbl_no_list = []
                ri_list = {}
                while (tbl <= 200):
                    try:
                        tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                        ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                        tbl += 1
                    except KeyError as e:
                        break

                i["Footnotes"]["FN_ID"] = i["footnote"]
                i["Footnotes"]["Tariff"] = i["tariff_code"]

                cur_11_fn = record_3_cat_011_fn.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_11_fn:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_11_fn.append(k)
                i["Footnotes"]["Cat_11_FN"] = temp_list_11_fn
                break

        cur_record_2_cat_14_fn = record_2_cat_14_fn.find(
            {"CXR_CODE": {"$eq": i["carrier"]}, "FT_NT": {"$eq": i["footnote"]},
             "TARIFF": {"$eq": i["tariff_code"]}, "NO_APPL": {"$eq": ""},
             "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
             "OW_RT": {"$in": ["", i["oneway_return"]]},
             "DATES_EFF_1": {"$lte": query_date},
             "DATES_DISC_1": {"$gte": query_date},
             "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
             "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
             }).sort("SEQ_NO", 1)
        for j in cur_record_2_cat_14_fn:

            if wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "":
                restrictions += 1
                tbl = 1
                tbl_no_list = []
                ri_list = {}
                while (tbl <= 200):
                    try:
                        tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                        ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                        tbl += 1
                    except KeyError as e:
                        break

                i["Footnotes"]["FN_ID"] = i["footnote"]
                i["Footnotes"]["Tariff"] = i["tariff_code"]

                cur_14_fn = record_3_cat_014_fn.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_14_fn:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_14_fn.append(k)
                i["Footnotes"]["Cat_14_FN"] = temp_list_14_fn
                break

        cur_record_2_cat_15_fn = record_2_cat_15_fn.find(
            {"CXR_CODE": {"$eq": i["carrier"]}, "FT_NT": {"$eq": i["footnote"]},
             "TARIFF": {"$eq": i["tariff_code"]}, "NO_APPL": {"$eq": ""},
             "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
             "OW_RT": {"$in": ["", i["oneway_return"]]},
             "DATES_EFF_1": {"$lte": query_date},
             "DATES_DISC_1": {"$gte": query_date},
             "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
             "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
             }).sort("SEQ_NO", 1)
        for j in cur_record_2_cat_15_fn:

            if wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "":
                restrictions += 1
                tbl = 1
                tbl_no_list = []
                ri_list = {}
                while (tbl <= 200):
                    try:
                        tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                        ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                        tbl += 1
                    except KeyError as e:
                        break

                i["Footnotes"]["FN_ID"] = i["footnote"]
                i["Footnotes"]["Tariff"] = i["tariff_code"]

                cur_15_fn = record_3_cat_015_fn.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_15_fn:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_15_fn.append(k)
                i["Footnotes"]["Cat_15_FN"] = temp_list_15_fn
                break

        cur_record_2_cat_23_fn = record_2_cat_23_fn.find(
            {"CXR_CODE": {"$eq": i["carrier"]}, "FT_NT": {"$eq": i["footnote"]},
             "TARIFF": {"$eq": i["tariff_code"]}, "NO_APPL": {"$eq": ""},
             "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
             "OW_RT": {"$in": ["", i["oneway_return"]]},
             "DATES_EFF_1": {"$lte": query_date},
             "DATES_DISC_1": {"$gte": query_date},
             "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
             "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
             }).sort("SEQ_NO", 1)
        for j in cur_record_2_cat_23_fn:
            if wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "":
                restrictions += 1
                tbl = 1
                tbl_no_list = []
                ri_list = {}
                while (tbl <= 200):
                    try:
                        tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                        ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                        tbl += 1
                    except KeyError as e:
                        break

                i["Footnotes"]["FN_ID"] = i["footnote"]
                i["Footnotes"]["Tariff"] = i["tariff_code"]

                cur_23_fn = record_3_cat_023_fn.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_23_fn:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_23_fn.append(k)
                i["Footnotes"]["Cat_23_FN"] = temp_list_23_fn
                break
        countj = 1
        count986 = 1
        try:
            for j in range(len(i["cat_4"])):
                cur_record_3_table_986 = record_3_table_986.find({"TBL_NO": {"$eq": i["cat_4"][j]["CXR_FLT"]}})
                for k in cur_record_3_table_986:
                    i["cat_4"][j]["CXR_FLT"] = k
                cur_record_3_table_986 = record_3_table_986.find({"TBL_NO": {"$eq": i["cat_4"][j]["CXR_TBL"]}})
                for k in cur_record_3_table_986:
                    i["cat_4"][j]["CXR_TBL"] = k
                countj += 1

            countj = 1
            for j in range(len(i["cat_12"])):
                cur_record_3_table_986 = record_3_table_986.find({"TBL_NO": {"$eq": i["cat_12"][j]["CXR_FLT_TBL_NO"]}})
                for k in cur_record_3_table_986:
                    i["cat_12"][j]["CXR_FLT_TBL_NO"] = k
                countj += 1

            count986 += 1
        except KeyError:
            pass

        i["used"] = 1
        i['restrictions'] = restrictions
        # print i["_id"]
        if t == 100:
            print 'updating:', count
            fare_record.bulk_write(bulk)
            bulk = []
            t = 0
            bulk.append(UpdateOne({'_id': i['_id']}, {'$set': i}))
            t += 1
        else:
            bulk.append(UpdateOne({'_id': i['_id']}, {'$set': i}))
            t += 1
        # print count, "  ", time.time() - st
        count += 1
    if len(bulk) > 0:
        print 'updating:', count
        fare_record.bulk_write(bulk)
    temp_cur.close()
    print "stage 2 complete in ", time.time() - st_3


@measure(JUPITER_LOGGER)
def fares_rules(system_date, carrier_list, tariff_list, client):
    db_ATPCO = client[ATPCO_DB]
    db_fzDB = client[JUPITER_DB]
    fare_record = db_fzDB.JUP_DB_ATPCO_Fares_Rules
    record_0 = db_ATPCO.JUP_DB_ATPCO_Record_0
    record_1 = db_ATPCO.JUP_DB_ATPCO_Record_1
    record_2_cat_all = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_All
    record_2_cat_3_fn = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_03_FN
    record_2_cat_11_fn = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_11_FN
    record_2_cat_14_fn = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_14_FN
    record_2_cat_15_fn = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_15_FN
    record_2_cat_23_fn = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_23_FN
    record_2_cat_10 = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_10
    record_2_cat_25 = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_25
    record_3_cat_001 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_01
    record_3_cat_002 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_02
    record_3_cat_003 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_03
    record_3_cat_004 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_04
    record_3_cat_005 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_05
    record_3_cat_006 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_06
    record_3_cat_007 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_07
    record_3_cat_008 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_08
    record_3_cat_009 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_09
    record_3_cat_011 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_11
    record_3_cat_012 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_12
    record_3_cat_013 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_13
    record_3_cat_014 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_14
    record_3_cat_015 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_15
    record_3_cat_016 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_16
    record_3_cat_017 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_17
    record_3_cat_018 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_18
    record_3_cat_019 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_19
    record_3_cat_020 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_20
    record_3_cat_021 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_21
    record_3_cat_022 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_22
    record_3_cat_023 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_23
    record_3_cat_025 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_25
    record_3_cat_026 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_26
    record_3_cat_027 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_27
    record_3_cat_028 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_28
    record_3_cat_029 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_29
    record_3_cat_031 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_31
    record_3_cat_033 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_33
    record_3_cat_050 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_50
    record_3_cat_035 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_35
    record_3_cat_101 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_101
    record_3_cat_102 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_102
    record_3_cat_103 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_103
    record_3_cat_104 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_104
    record_3_cat_106 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_106
    record_3_cat_107 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_107
    record_3_cat_108 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_108
    record_3_cat_109 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_109
    record_3_cat_003_fn = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_03_FN
    record_3_cat_011_fn = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_11_FN
    record_3_cat_014_fn = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_14_FN
    record_3_cat_015_fn = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_15_FN
    record_3_cat_023_fn = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_23_FN
    record_3_table_986 = db_ATPCO.JUP_DB_ATPCO_Record_3_Table_986
    record_3_table_983 = db_ATPCO.JUP_DB_ATPCO_Record_3_Table_983
    system_date = datetime.datetime.strptime(system_date,'%Y-%m-%d')
    print system_date

    query_date = system_date - datetime.timedelta(days=1)
    query_date_with_dash = datetime.datetime.strftime(query_date, "%Y-%m-%d")
    query_date = datetime.datetime.strftime(query_date, "%Y%m%d")
    day_before_yesterday = datetime.datetime.strftime(system_date - datetime.timedelta(days=2), '%Y%m%d')
    day_before_yesterday_with_dash = datetime.datetime.strftime(system_date - datetime.timedelta(days=2),'%Y-%m-%d')
    fares_date = datetime.datetime.strftime(system_date, "%Y-%m-%d")

    print query_date
    query = []

    st_1 = time.time()
    print "getting change data"
    cur_record_0 = record_0.find({"DATES_DISC_1":{"$gte":query_date}, "CXR_CODE":{"$in":carrier_list}, 'TARIFF': {'$in': tariff_list},
                                  "$or": [{"DATES_EFF_1":{"$eq":query_date}}, {"DATES_EFF_1": {"$eq": day_before_yesterday},
                                                                               "LAST_UPDATED_DATE": query_date_with_dash}],
                                  })
    for i in cur_record_0:
        temp = {'carrier': i['CXR_CODE'],'tariff_code': i['TARIFF'], "used":1}
        query.append(temp)

    cur_record_1 = record_1.find({"DATES_DISC_1":{"$gte":query_date}, "CXR_CODE":{"$in":carrier_list}, 'TARIFF': {'$in': tariff_list},
                                  "$or": [{"DATES_EFF_1":{"$eq":query_date}}, {"DATES_EFF_1": {"$eq": day_before_yesterday},
                                                                               "LAST_UPDATED_DATE": query_date_with_dash}],
                                  })
    for i in cur_record_1:
        temp = {'carrier': i['CXR_CODE'],'tariff_code': i['TARIFF'],'fare_rule': i['RULE_NO'],'fare_basis': i['FARE_CLASS'], "used":1}
        query.append(temp)

    cur_record_2_cat_all = record_2_cat_all.find({"DATES_DISC_1":{"$gte":query_date}, "CXR_CODE":{"$in":carrier_list}, 'TARIFF': {'$in': tariff_list},
                                  "$or": [{"DATES_EFF_1":{"$eq":query_date}}, {"DATES_EFF_1": {"$eq": day_before_yesterday},
                                                                               "LAST_UPDATED_DATE": query_date_with_dash}],
                                  })
    for i in cur_record_2_cat_all:
        temp = {'carrier': i['CXR_CODE'],'tariff_code':i['TARIFF'],'fare_rule':i['RULE_NO'], "used":1}
        query.append(temp)

    cur_record_2_cat_10 = record_2_cat_10.find({"DATES_DISC_1":{"$gte":query_date}, "CXR_CODE":{"$in":carrier_list}, 'TARIFF': {'$in': tariff_list},
                                  "$or": [{"DATES_EFF_1":{"$eq":query_date}}, {"DATES_EFF_1": {"$eq": day_before_yesterday},
                                                                               "LAST_UPDATED_DATE": query_date_with_dash}],
                                  })
    for i in cur_record_2_cat_10:
        temp = {'carrier': i['CXR_CODE'], 'tariff_code': i['TARIFF'], 'fare_rule': i['RULE_NO'], "used":1}
        query.append(temp)

    cur_record_2_cat_25 = record_2_cat_25.find({"DATES_DISC_1":{"$gte":query_date}, "CXR_CODE":{"$in":carrier_list}, 'TARIFF': {'$in': tariff_list},
                                  "$or": [{"DATES_EFF_1":{"$eq":query_date}}, {"DATES_EFF_1": {"$eq": day_before_yesterday},
                                                                               "LAST_UPDATED_DATE": query_date_with_dash}],
                                  })
    for i in cur_record_2_cat_25:
        temp = {'carrier': i['CXR_CODE'], 'tariff_code': i['TARIFF'], 'fare_rule': i['RULE_NO'], "used":1}
        query.append(temp)

    cur_record_2_cat_3_fn = record_2_cat_3_fn.find({"DATES_DISC_1":{"$gte":query_date}, "CXR_CODE":{"$in":carrier_list}, 'TARIFF': {'$in': tariff_list},
                                  "$or": [{"DATES_EFF_1":{"$eq":query_date}}, {"DATES_EFF_1": {"$eq": day_before_yesterday},
                                                                               "LAST_UPDATED_DATE": query_date_with_dash}],
                                  })
    for i in cur_record_2_cat_3_fn:
        temp = {'carrier': i['CXR_CODE'], 'tariff_code': i['TARIFF'], 'footnote': i['FT_NT'], "used":1}
        query.append(temp)

    cur_record_2_cat_11_fn = record_2_cat_11_fn.find({"DATES_DISC_1":{"$gte":query_date}, "CXR_CODE":{"$in":carrier_list}, 'TARIFF': {'$in': tariff_list},
                                  "$or": [{"DATES_EFF_1":{"$eq":query_date}}, {"DATES_EFF_1": {"$eq": day_before_yesterday},
                                                                               "LAST_UPDATED_DATE": query_date_with_dash}],
                                  })
    for i in cur_record_2_cat_11_fn:
        temp = {'carrier': i['CXR_CODE'], 'tariff_code': i['TARIFF'], 'footnote': i['FT_NT'], "used":1}
        query.append(temp)

    cur_record_2_cat_14_fn = record_2_cat_14_fn.find({"DATES_DISC_1":{"$gte":query_date}, "CXR_CODE":{"$in":carrier_list}, 'TARIFF': {'$in': tariff_list},
                                  "$or": [{"DATES_EFF_1":{"$eq":query_date}}, {"DATES_EFF_1": {"$eq": day_before_yesterday},
                                                                               "LAST_UPDATED_DATE": query_date_with_dash}],
                                  })
    for i in cur_record_2_cat_14_fn:
        temp = {'carrier': i['CXR_CODE'], 'tariff_code': i['TARIFF'], 'footnote': i['FT_NT'], "used":1}
        query.append(temp)

    cur_record_2_cat_15_fn = record_2_cat_15_fn.find({"DATES_DISC_1":{"$gte":query_date}, "CXR_CODE":{"$in":carrier_list}, 'TARIFF': {'$in': tariff_list},
                                  "$or": [{"DATES_EFF_1":{"$eq":query_date}}, {"DATES_EFF_1": {"$eq": day_before_yesterday},
                                                                               "LAST_UPDATED_DATE": query_date_with_dash}],
                                  })
    for i in cur_record_2_cat_15_fn:
        temp = {'carrier': i['CXR_CODE'], 'tariff_code': i['TARIFF'], 'footnote': i['FT_NT'], "used":1}
        query.append(temp)

    cur_record_2_cat_23_fn = record_2_cat_23_fn.find({"DATES_DISC_1":{"$gte":query_date}, "CXR_CODE":{"$in":carrier_list}, 'TARIFF': {'$in': tariff_list},
                                  "$or": [{"DATES_EFF_1":{"$eq":query_date}}, {"DATES_EFF_1": {"$eq": day_before_yesterday},
                                                                               "LAST_UPDATED_DATE": query_date_with_dash}],
                                  })
    for i in cur_record_2_cat_23_fn:
        temp = {'carrier': i['CXR_CODE'], 'tariff_code': i['TARIFF'], 'footnote': i['FT_NT'], "used":1}
        query.append(temp)

    query.append({"carrier":{"$in":carrier_list}, "used": None, 'tariff_code': {'$in': tariff_list},
                  "$or": [{"effective_from": {"$lte": query_date_with_dash}, "effective_to": None},
                          {"effective_from": {"$lte": query_date_with_dash}, "effective_to": {"$gte": query_date_with_dash}},
                          {"effective_from": {"$lte": day_before_yesterday_with_dash}, "effective_to": day_before_yesterday_with_dash,
                           "file_date": query_date}]})
    print "got change data in ",  time.time() - st_1
    st_2 = time.time()

    query = list(np.unique(np.array(query)))

    print query

    counti = 1
    countj = 1
    for k in range(int(math.ceil(len(query) / 1000.0))):
        temp = []
        print "Iteration: ", k
        try:
            temp = query[k * 1000: (k + 1) * 1000]
        except IndexError:
            temp = query[k * 1000:]

        cur_fare_record = fare_record.find({"$or": temp}, no_cursor_timeout=True)
        print "stage 1 start"

        for i in cur_fare_record:

            if (i["effective_from"] <= query_date_with_dash and i["effective_to"] == None) or \
               (i["effective_from"] <= query_date_with_dash and i["effective_to"] >= query_date_with_dash) or \
               (i["effective_from"] <= day_before_yesterday_with_dash and i["effective_to"] == day_before_yesterday_with_dash and i["file_date"] == query_date):
                for m in range(1, 51):
                    try:
                        del i['cat_' + str(m)]
                    except KeyError:
                        pass
                for m in range(1, 10):
                    try:
                        del i['cat_10' + str(m)]
                    except KeyError:
                        pass
                try:
                    del i['Footnotes']
                except KeyError:
                    pass
                try:
                    del i['record 2 cat 10']
                except KeyError:
                    pass
                try:
                    del i['Gen Rules']
                except KeyError:
                    pass
                try:
                    del i['used']
                except KeyError:
                    pass

                print i["fare_basis"], i["fare_rule"], i["carrier"], i["tariff_code"]
                cur_record_1 = record_1.find({"FARE_CLASS": {"$eq": i["fare_basis"]}, "RULE_NO": {"$eq": i["fare_rule"]},
                                              "CXR_CODE": {"$eq": i["carrier"]}, "TARIFF": {"$eq": i["tariff_code"]},
                                              "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
                                              "OW_RT": {"$in": ["", i["oneway_return"]]},
                                              "FT_NT": {"$in": ["", i['footnote']]},
                                              "DATES_EFF_1": {"$lte": query_date},
                                              "DATES_DISC_1": {"$gte": query_date},
                                              "LOC_1_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
                                              "LOC_1_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
                                              "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
                                              "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
                                              "LOC_2_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
                                              "LOC_2_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
                                              "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
                                              "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
                                              })

                print cur_record_1.count()
                for j in cur_record_1:
                    i['TYPE_CODES_FARE_TYPE'] = j['TYPE_CODES_FARE_TYPE']
                    i['TYPE_CODES_DAY_OF_WEEK_TYPE'] = j['TYPE_CODES_DAY_OF_WEEK_TYPE']
                    i['TYPE_CODES_SEASON_TYPE'] = j['TYPE_CODES_SEASON_TYPE']
                    i['fareID'] = i['fare_basis'] + i['fare_rule'] + i['footnote']
                    i["used"] = 0
                    print "inserted.....", countj
                    countj += 1
                    break
            fare_record.update({'_id': i['_id']}, i)
            print i['_id']
            print "done ----- ", counti
            counti += 1

        cur_fare_record.close()

    print "stage 1 complete in ", time.time() - st_2

    st_3 = time.time()
    t = 0
    bulk = []
    temp_cur = fare_record.find({"carrier":{"$in":carrier_list}, "used":0, 'tariff_code': {'$in': tariff_list}}, no_cursor_timeout = True)
    count = 1
    print "stage 2 start"
    for i in temp_cur:
        restrictions = 0
        st = time.time()
        final_insert = {}

        countk = 1

        cur_record_0 = record_0.find({"TARIFF": {"$eq": i["tariff_code"]}, "CXR_CODE": {"$eq": i["carrier"]},
                                      "DATES_EFF_1": {"$lte": query_date},
                                      "DATES_DISC_1": {"$gte": query_date}})
        final_insert["Gen Rules"] = {}
        i["Gen Rules"] = {}
        for h in cur_record_0:

            for g in range(int(h["NO_SEGS"])):
                cur_record_2_cat_all = record_2_cat_all.find(
                    {"CXR_CODE": {"$eq": i["carrier"]}, "GEN_APPL": {"$eq": ""},
                     "CAT_NO": {"$eq": h["CAT_NO_" + str(g + 1)]},
                     "TARIFF": {"$eq": h["SCR_TARIFF_" + str(g + 1)]},
                     "RULE_NO": {"$eq": h["GENERAL_RULE_NO_" + str(g + 1)]}, "NO_APPL": {"$eq": ""},
                     "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
                     "OW_RT": {"$in": ["", i["oneway_return"]]},
                     "FT_NT": {"$in": ["", i['footnote']]},
                     "DATES_EFF_1": {"$lte": query_date},
                     "DATES_DISC_1": {"$gte": query_date},
                     "TYPE_CODES_DAY_OF_WEEK_TYPE": {"$in": ["", i["TYPE_CODES_DAY_OF_WEEK_TYPE"]]},
                     "TYPE_CODES_FARE_TYPE": {"$in": ["", i["TYPE_CODES_FARE_TYPE"]]},
                     "TYPE_CODES_SEASON_TYPE": {"$in": ["", i["TYPE_CODES_SEASON_TYPE"]]},
                     "LOC_1_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
                     "LOC_1_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
                     "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
                     "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
                     "LOC_2_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
                     "LOC_2_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
                     "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
                     "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
                     }).sort("SEQ_NO", 1)
                for j in cur_record_2_cat_all:
                    if wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "":
                        tbl = 1
                        tbl_no_list = []
                        ri_list = {}
                        while (tbl <= 200):
                            try:
                                tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                                ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j[
                                    "DATA_TABLE_STRING_RI_" + str(tbl)]
                                tbl += 1
                            except KeyError as e:
                                break

                        exec "cur_1 = record_3_cat_" + str(j['CAT_NO']) + ".find({'TBL_NO': {'$in': tbl_no_list}})"
                        temp_list = []
                        for k in cur_1:
                            k["RI"] = ri_list[k["TBL_NO"]]
                            k["SEQ_NO"] = j["SEQ_NO"]
                            temp_list.append(k)
                        if (j["CAT_NO"] < "010"):
                            i["Gen Rules"]["cat_" + j["CAT_NO"][2]] = temp_list
                        else:
                            i["Gen Rules"]["cat_" + j["CAT_NO"][1:]] = temp_list
                        break

        cur_record_2_cat_all = record_2_cat_all.find(
            {"RULE_NO": {"$eq": i["fare_rule"]}, "CXR_CODE": {"$eq": i["carrier"]},
             "NO_APPL": {"$eq": ""}, "TARIFF": {"$eq": i["tariff_code"]},
             "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
             "OW_RT": {"$in": ["", i["oneway_return"]]},
             "FT_NT": {"$in": ["", i['footnote']]},
             "DATES_EFF_1": {"$lte": query_date},
             "DATES_DISC_1": {"$gte": query_date},
             "TYPE_CODES_DAY_OF_WEEK_TYPE": {"$in": ["", i["TYPE_CODES_DAY_OF_WEEK_TYPE"]]},
             "TYPE_CODES_FARE_TYPE": {"$in": ["", i["TYPE_CODES_FARE_TYPE"]]},
             "TYPE_CODES_SEASON_TYPE": {"$in": ["", i["TYPE_CODES_SEASON_TYPE"]]},
             "LOC_1_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
             "LOC_1_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
             "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
             "LOC_2_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
             "LOC_2_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
             "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
             }).sort([("CAT_NO", 1), ("SEQ_NO", 1)])
        cat_check = []
        for j in cur_record_2_cat_all:
            if ((j["CAT_NO"] not in cat_check) and (
                    wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "")):
                cat_check.append(j["CAT_NO"])
                restrictions += 1
                tbl = 1
                tbl_no_list = []
                ri_list = {}
                while (tbl <= 200):
                    try:
                        tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                        ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                        tbl += 1
                    except KeyError as e:
                        break

                exec "cur_1 = record_3_cat_" + str(j['CAT_NO']) + ".find({'TBL_NO':{'$in':tbl_no_list}})"
                temp_list = []
                for k in cur_1:
                    k["RI"] = ri_list[k["TBL_NO"]]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list.append(k)
                if (j["CAT_NO"] < "010"):
                    i["cat_" + j["CAT_NO"][2]] = temp_list
                else:
                    i["cat_" + j["CAT_NO"][1:]] = temp_list

        cur_record_2_cat_10 = record_2_cat_10.find(
            {"RULE_NO": {"$eq": i["fare_rule"]}, "CXR_CODE": {"$eq": i["carrier"]},
             "NO_APPL": {"$eq": ""}, "TARIFF": {"$eq": i["tariff_code"]},
             "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
             "OW_RT": {"$in": ["", i["oneway_return"]]},
             "FT_NT": {"$in": ["", i['footnote']]},
             "DATES_EFF_1": {"$lte": query_date},
             "DATES_DISC_1": {"$gte": query_date},
             "TYPE_CODES_DAY_OF_WEEK_TYPE": {"$in": ["", i["TYPE_CODES_DAY_OF_WEEK_TYPE"]]},
             "TYPE_CODES_FARE_TYPE": {"$in": ["", i["TYPE_CODES_FARE_TYPE"]]},
             "TYPE_CODES_SEASON_TYPE": {"$in": ["", i["TYPE_CODES_SEASON_TYPE"]]},
             "LOC_1_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
             "LOC_1_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
             "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
             "LOC_2_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
             "LOC_2_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
             "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
             }).sort("SEQ_NO", 1)
        temp_list_1 = []
        temp_list_2 = []
        temp_list_3 = []
        temp_list_4 = []
        temp_list_6 = []
        temp_list_7 = []
        temp_list_8 = []
        temp_list_9 = []
        i["record 2 cat 10"] = {}
        for j in cur_record_2_cat_10:

            if (wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == ""):
                i['record 2 cat 10']['DATA_TABLE'] = j['DATA_TABLE']
                i['record 2 cat 10']['COMB_APPLICATION_CT2_CXR'] = j['COMB_APPLICATION_CT2_CXR']
                i['record 2 cat 10']['JT_CXR_TBL_NO'] = j['JT_CXR_TBL_NO']
                i['record 2 cat 10']['COMB_APPLICATION_FILLER'] = j['COMB_APPLICATION_FILLER']
                i['record 2 cat 10']['TARIFF'] = j['TARIFF']
                i['record 2 cat 10']['COMB_APPLICATION_ARB'] = j['COMB_APPLICATION_ARB']
                i['record 2 cat 10']['TYPE_CODES_SEASON_TYPE'] = j['TYPE_CODES_SEASON_TYPE']
                i['record 2 cat 10']['ACTION'] = j['ACTION']
                i['record 2 cat 10']['COMB_APPLICATION_T_R'] = j['COMB_APPLICATION_T_R']
                i['record 2 cat 10']['COMB_APPLICATION_DOJ'] = j['COMB_APPLICATION_DOJ']
                i['record 2 cat 10']['LOC_2_COUNTRY'] = j['LOC_2_COUNTRY']
                i['record 2 cat 10']['COMB_APPLICATION_CT2P_T_R'] = j['COMB_APPLICATION_CT2P_T_R']
                i['record 2 cat 10']['NO_APPL'] = j['NO_APPL']
                i['record 2 cat 10']['COMB_APPLICATION_F_C'] = j['COMB_APPLICATION_F_C']
                i['record 2 cat 10']['COMB_APPLICATION_SOJ'] = j['COMB_APPLICATION_SOJ']
                i['record 2 cat 10']['LOC_1_CITY'] = j['LOC_1_CITY']
                i['record 2 cat 10']['COMB_APPLICATION_END_T_R'] = j['COMB_APPLICATION_END_T_R']
                i['record 2 cat 10']['TYPE_CODES_FARE_TYPE'] = j['TYPE_CODES_FARE_TYPE']
                i['record 2 cat 10']['COMB_APPLICATION_END_CXR'] = j['COMB_APPLICATION_END_CXR']
                i['record 2 cat 10']['COMB_APPLICATION_CT2_T_R'] = j['COMB_APPLICATION_CT2_T_R']
                i['record 2 cat 10']['DATES_DISC'] = j['DATES_DISC_1']
                i['record 2 cat 10']['LOC_1_COUNTRY'] = j['LOC_1_COUNTRY']
                i['record 2 cat 10']['CXR_CODE'] = j['CXR_CODE']
                i['record 2 cat 10']['FILLER'] = j['FILLER']
                i['record 2 cat 10']['COMB_APPLICATION_CT2_F_C1'] = j['COMB_APPLICATION_CT2_F_C1']
                i['record 2 cat 10']['LOC_2_AREA'] = j['LOC_2_AREA']
                i['record 2 cat 10']['COMB_APPLICATION_CXR'] = j['COMB_APPLICATION_CXR']
                i['record 2 cat 10']['NO_SEGS'] = j['NO_SEGS']
                i['record 2 cat 10']['BATCH_NO'] = j['BATCH_NO']
                i['record 2 cat 10']['COMB_APPLICATION_CT2P_CXR'] = j['COMB_APPLICATION_CT2P_CXR']
                i['record 2 cat 10']['RULE_NO'] = j['RULE_NO']
                i['record 2 cat 10']['REC_TYPE'] = j['REC_TYPE']
                i['record 2 cat 10']['CAT_NO'] = j['CAT_NO']
                i['record 2 cat 10']['COMB_APPLICATION_FILLER3'] = j['COMB_APPLICATION_FILLER3']
                i['record 2 cat 10']['COMB_APPLICATION_FILLER2'] = j['COMB_APPLICATION_FILLER2']
                i['record 2 cat 10']['COMB_APPLICATION_FILLER4'] = j['COMB_APPLICATION_FILLER4']
                i['record 2 cat 10']['BATCH_CI'] = j['BATCH_CI']
                i['record 2 cat 10']['COMB_APPLICATION_CT2P_F_C'] = j['COMB_APPLICATION_CT2P_F_C']
                i['record 2 cat 10']['COMB_APPLICATION_END'] = j['COMB_APPLICATION_END']
                i['record 2 cat 10']['OW_RT'] = j['OW_RT']
                i['record 2 cat 10']['LOC_1_TYPE'] = j['LOC_1_TYPE']
                i['record 2 cat 10']['COMB_APPLICATION_O_D'] = j['COMB_APPLICATION_O_D']
                i['record 2 cat 10']['LOC_1_ZONE'] = j['LOC_1_ZONE']
                i['record 2 cat 10']['LOC_2'] = j['LOC_2']
                i['record 2 cat 10']['DATES_EFF'] = j['DATES_EFF_1']
                i['record 2 cat 10']['LOC_1_AREA'] = j['LOC_1_AREA']
                i['record 2 cat 10']['COMB_APPLICATION_CT2PLUS'] = j['COMB_APPLICATION_CT2PLUS']
                i['record 2 cat 10']['COMB_APPLICATION_END_F_C'] = j['COMB_APPLICATION_END_F_C']
                i['record 2 cat 10']['RTG_NO'] = j['RTG_NO']
                i['record 2 cat 10']['TYPE_CODES_DAY_OF_WEEK_TYPE'] = j['TYPE_CODES_DAY_OF_WEEK_TYPE']
                i['record 2 cat 10']['LOC_2_CITY'] = j['LOC_2_CITY']
                i['record 2 cat 10']['SEQ_NO'] = j['SEQ_NO']
                i['record 2 cat 10']['LOC_1'] = j['LOC_1']
                i['record 2 cat 10']['FT_NT'] = j['FT_NT']
                i['record 2 cat 10']['LOC_2_ZONE'] = j['LOC_2_ZONE']
                i['record 2 cat 10']['COMB_APPLICATION_CT2'] = j['COMB_APPLICATION_CT2']
                i['record 2 cat 10']['MCN'] = j['MCN']
                i['record 2 cat 10']['SAME_PT_TBL'] = j['SAME_PT_TBL']
                i['record 2 cat 10']['LOC_1_STATE'] = j['LOC_1_STATE']
                i['record 2 cat 10']['FARE_CLASS'] = j['FARE_CLASS']
                tbl = 1
                restrictions += 8
                tbl_no_list = []
                ri_list = {}
                while (tbl <= 200):
                    try:
                        tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                        ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                        tbl += 1
                    except KeyError as e:
                        break

                cur_1 = record_3_cat_101.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_1:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_1.append(k)
                i["cat_101"] = temp_list_1

                cur_2 = record_3_cat_102.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_2:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_2.append(k)
                i["cat_102"] = temp_list_2

                cur_3 = record_3_cat_103.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_3:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_3.append(k)
                i["cat_103"] = temp_list_3

                cur_4 = record_3_cat_104.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_4:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_4.append(k)
                i["cat_104"] = temp_list_4

                cur_6 = record_3_cat_106.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_6:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_6.append(k)
                i["cat_106"] = temp_list_6

                cur_7 = record_3_cat_107.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_7:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_7.append(k)
                i["cat_107"] = temp_list_7

                cur_8 = record_3_cat_108.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_8:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_8.append(k)
                i["cat_108"] = temp_list_8

                cur_9 = record_3_cat_109.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_9:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_9.append(k)
                i["cat_109"] = temp_list_9
                break

        cur_record_2_cat_25 = record_2_cat_25.find(
            {"RULE_NO": {"$eq": i["fare_rule"]}, "CXR_CODE": {"$eq": i["carrier"]},
             "NO_APPL": {"$eq": ""}, "TARIFF": {"$eq": i["tariff_code"]},
             "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
             "OW_RT": {"$in": ["", i["oneway_return"]]},
             "FT_NT": {"$in": ["", i['footnote']]},
             "DATES_EFF_1": {"$lte": query_date},
             "DATES_DISC_1": {"$gte": query_date},
             "TYPE_CODES_DAY_OF_WEEK_TYPE": {"$in": ["", i["TYPE_CODES_DAY_OF_WEEK_TYPE"]]},
             "TYPE_CODES_FARE_TYPE": {"$in": ["", i["TYPE_CODES_FARE_TYPE"]]},
             "TYPE_CODES_SEASON_TYPE": {"$in": ["", i["TYPE_CODES_SEASON_TYPE"]]},
             "LOC_1_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
             "LOC_1_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
             "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
             "LOC_2_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
             "LOC_2_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
             "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
             }).sort("SEQ_NO", 1)
        for j in cur_record_2_cat_25:
            print "inside record 2 cat 25"

            if wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "":
                tbl = 1
                restrictions += 1
                tbl_no_list = []
                ri_list = {}
                while (tbl <= 200):
                    try:
                        tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                        ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                        tbl += 1
                    except KeyError as e:
                        break

                cur_1 = record_3_cat_025.find({"TBL_NO": {"$in": tbl_no_list}})
                temp_list = []
                for k in cur_1:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list.append(k)
                i["cat_25"] = temp_list
                break

        final_insert["Footnotes"] = {}
        i["Footnotes"] = {}
        temp_list_3_fn = []
        temp_list_11_fn = []
        temp_list_14_fn = []
        temp_list_15_fn = []
        temp_list_23_fn = []

        cur_record_2_cat_3_fn = record_2_cat_3_fn.find(
            {"CXR_CODE": {"$eq": i["carrier"]}, "FT_NT": {"$eq": i["footnote"]},
             "TARIFF": {"$eq": i["tariff_code"]}, "NO_APPL": {"$eq": ""},
             "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
             "OW_RT": {"$in": ["", i["oneway_return"]]},
             "DATES_EFF_1": {"$lte": query_date},
             "DATES_DISC_1": {"$gte": query_date},
             "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
             "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
             }).sort("SEQ_NO", 1)
        for j in cur_record_2_cat_3_fn:

            if wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "":
                restrictions += 1
                tbl = 1
                tbl_no_list = []
                ri_list = {}
                while (tbl <= 200):
                    try:
                        tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                        ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                        tbl += 1
                    except KeyError as e:
                        break

                i["Footnotes"]["FN_ID"] = i["footnote"]
                i["Footnotes"]["Tariff"] = i["tariff_code"]

                cur_3_fn = record_3_cat_003_fn.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_3_fn:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_3_fn.append(k)
                i["Footnotes"]["Cat_03_FN"] = temp_list_3_fn
                break

        cur_record_2_cat_11_fn = record_2_cat_11_fn.find(
            {"CXR_CODE": {"$eq": i["carrier"]}, "FT_NT": {"$eq": i["footnote"]},
             "TARIFF": {"$eq": i["tariff_code"]}, "NO_APPL": {"$eq": ""},
             "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
             "OW_RT": {"$in": ["", i["oneway_return"]]},
             "DATES_EFF_1": {"$lte": query_date},
             "DATES_DISC_1": {"$gte": query_date},
             "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
             "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
             }).sort("SEQ_NO", 1)
        for j in cur_record_2_cat_11_fn:

            if wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "":
                restrictions += 1
                tbl = 1
                tbl_no_list = []
                ri_list = {}
                while (tbl <= 200):
                    try:
                        tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                        ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                        tbl += 1
                    except KeyError as e:
                        break

                i["Footnotes"]["FN_ID"] = i["footnote"]
                i["Footnotes"]["Tariff"] = i["tariff_code"]

                cur_11_fn = record_3_cat_011_fn.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_11_fn:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_11_fn.append(k)
                i["Footnotes"]["Cat_11_FN"] = temp_list_11_fn
                break

        cur_record_2_cat_14_fn = record_2_cat_14_fn.find(
            {"CXR_CODE": {"$eq": i["carrier"]}, "FT_NT": {"$eq": i["footnote"]},
             "TARIFF": {"$eq": i["tariff_code"]}, "NO_APPL": {"$eq": ""},
             "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
             "OW_RT": {"$in": ["", i["oneway_return"]]},
             "DATES_EFF_1": {"$lte": query_date},
             "DATES_DISC_1": {"$gte": query_date},
             "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
             "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
             }).sort("SEQ_NO", 1)
        for j in cur_record_2_cat_14_fn:

            if wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "":
                restrictions += 1
                tbl = 1
                tbl_no_list = []
                ri_list = {}
                while (tbl <= 200):
                    try:
                        tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                        ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                        tbl += 1
                    except KeyError as e:
                        break

                i["Footnotes"]["FN_ID"] = i["footnote"]
                i["Footnotes"]["Tariff"] = i["tariff_code"]

                cur_14_fn = record_3_cat_014_fn.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_14_fn:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_14_fn.append(k)
                i["Footnotes"]["Cat_14_FN"] = temp_list_14_fn
                break

        cur_record_2_cat_15_fn = record_2_cat_15_fn.find(
            {"CXR_CODE": {"$eq": i["carrier"]}, "FT_NT": {"$eq": i["footnote"]},
             "TARIFF": {"$eq": i["tariff_code"]}, "NO_APPL": {"$eq": ""},
             "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
             "OW_RT": {"$in": ["", i["oneway_return"]]},
             "DATES_EFF_1": {"$lte": query_date},
             "DATES_DISC_1": {"$gte": query_date},
             "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
             "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
             }).sort("SEQ_NO", 1)
        for j in cur_record_2_cat_15_fn:

            if wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "":
                restrictions += 1
                tbl = 1
                tbl_no_list = []
                ri_list = {}
                while (tbl <= 200):
                    try:
                        tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                        ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                        tbl += 1
                    except KeyError as e:
                        break

                i["Footnotes"]["FN_ID"] = i["footnote"]
                i["Footnotes"]["Tariff"] = i["tariff_code"]

                cur_15_fn = record_3_cat_015_fn.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_15_fn:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_15_fn.append(k)
                i["Footnotes"]["Cat_15_FN"] = temp_list_15_fn
                break

        cur_record_2_cat_23_fn = record_2_cat_23_fn.find(
            {"CXR_CODE": {"$eq": i["carrier"]}, "FT_NT": {"$eq": i["footnote"]},
             "TARIFF": {"$eq": i["tariff_code"]}, "NO_APPL": {"$eq": ""},
             "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
             "OW_RT": {"$in": ["", i["oneway_return"]]},
             "DATES_EFF_1": {"$lte": query_date},
             "DATES_DISC_1": {"$gte": query_date},
             "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
             "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
             }).sort("SEQ_NO", 1)
        for j in cur_record_2_cat_23_fn:
            if wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "":
                restrictions += 1
                tbl = 1
                tbl_no_list = []
                ri_list = {}
                while (tbl <= 200):
                    try:
                        tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                        ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                        tbl += 1
                    except KeyError as e:
                        break

                i["Footnotes"]["FN_ID"] = i["footnote"]
                i["Footnotes"]["Tariff"] = i["tariff_code"]

                cur_23_fn = record_3_cat_023_fn.find({"TBL_NO": {"$in": tbl_no_list}})
                for k in cur_23_fn:
                    k["RI"] = ri_list[k['TBL_NO']]
                    k["SEQ_NO"] = j["SEQ_NO"]
                    temp_list_23_fn.append(k)
                i["Footnotes"]["Cat_23_FN"] = temp_list_23_fn
                break
        countj = 1
        count986 = 1
        try:
            for j in range(len(i["cat_4"])):
                cur_record_3_table_986 = record_3_table_986.find({"TBL_NO": {"$eq": i["cat_4"][j]["CXR_FLT"]}})
                for k in cur_record_3_table_986:
                    i["cat_4"][j]["CXR_FLT"] = k
                cur_record_3_table_986 = record_3_table_986.find({"TBL_NO": {"$eq": i["cat_4"][j]["CXR_TBL"]}})
                for k in cur_record_3_table_986:
                    i["cat_4"][j]["CXR_TBL"] = k
                countj += 1

            countj = 1
            for j in range(len(i["cat_12"])):
                cur_record_3_table_986 = record_3_table_986.find({"TBL_NO": {"$eq": i["cat_12"][j]["CXR_FLT_TBL_NO"]}})
                for k in cur_record_3_table_986:
                    i["cat_12"][j]["CXR_FLT_TBL_NO"] = k
                countj += 1

            count986 += 1
        except KeyError:
            pass

        i["used"] = 1
        i['restrictions'] = restrictions
        print i["_id"]
        if t == 100:
            print 'updating:', count
            fare_record.bulk_write(bulk)
            bulk = []
            t = 0
            bulk.append(UpdateOne({'_id': i['_id']}, {'$set': i}))
            t += 1
        else:
            bulk.append(UpdateOne({'_id': i['_id']}, {'$set': i}))
            t += 1
        print count, "  ", time.time() - st
        count += 1
    if len(bulk) > 0:
        print 'updating:', count
        fare_record.bulk_write(bulk)
    temp_cur.close()
    print "stage 2 complete in ", time.time() - st_3
    # cr = db_fzDB.JUP_DB_ATPCO_Temp_Dates_DND.find({'file':'fr11.py'})
    # for i in cr:
    #     i['date'] = datetime.datetime.strftime(datetime.datetime.strptime(i['date'],'%Y-%m-%d')+datetime.timedelta(days=1),'%Y-%m-%d')
    #     i['status'] = 'To Do'
    #     db_fzDB.JUP_DB_ATPCO_Temp_Dates_DND.update({'_id':i['_id']}, i)

    print "total time taken --- ", time.time() - st_1

# if __name__=='__main__':
#     client = mongo_client()
#     run_stage_1("2018-12-10", ["FZ"], ["185"], client)