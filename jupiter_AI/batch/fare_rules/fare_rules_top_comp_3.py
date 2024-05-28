"""
Author: Abhinav Garg
Created with <3
Date Created: 2017-07-10
File Name: fare_rules_top_comp_3.py

This code maps the ATPCO fares to its associated rules and populates the relevant categories in
JUP_DB_ATPCO_Fares_Rules for 2/3 competitors whenever full refresh of ATPCO comes.

"""
from pymongo import MongoClient
import datetime
import time
import re
from jupiter_AI import JUPITER_LOGGER
from jupiter_AI.logutils import measure


client = MongoClient('localhost', 43535)

db_ATPCO = client.ATPCO_stg_Aug

db_fzDB = client.fzDB_stg

# client_local = MongoClient()
# db_local = client_local.local

# fare_record_local = db_local.JUP_DB_ATPCO_Fares
fare_record = db_fzDB.JUP_DB_ATPCO_Fares_Rules
# fare_rules = db_local.JUP_DB_ATPCO_Fares_Rules
# fare_rules_mongo = db_fzDB.JUP_DB_ATPCO_Fares_Rules
# fare_rules_mongo_2 = db_fzDB.JUP_DB_ATPCO_Fares_2
record_0 = db_ATPCO.JUP_DB_ATPCO_Record_0
record_1 = db_ATPCO.JUP_DB_ATPCO_Record_1
# record_1_local = db_local.JUP_DB_ATPCO_Record_1
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

system_date = datetime.datetime(2017, 10, 9)
query_date = datetime.datetime.strftime(system_date, "%Y%m%d")
fares_date = datetime.datetime.strftime(system_date, "%Y-%m-%d")
carrier_list = ["SV"]

cur_fare_record = fare_record.find({"carrier":{"$in":carrier_list}}, timeout = False, allowDiskUse = True)
# # cur_fare_record = fare_record.find({"carrier":"FZ", "fare_basis":"AOW2IN1","fare_rule":"01IN","tariff_code":'4'})
#
print "stage 1 start"
counti, countj = 1, 1

for i in cur_fare_record:
    if counti%1000 == 0:
        print counti

    #if i['effective_from'] <= fares_date and (i['effective_to'] >= fares_date or i['effective_to'] == None):
    countk = 1
    print i["fare_basis"], i["fare_rule"], i["carrier"], i["tariff_code"]
    cur_record_1 = record_1.find({"FARE_CLASS": {"$eq": i["fare_basis"]}, "RULE_NO": {"$eq": i["fare_rule"]}, \
                                  "CXR_CODE": {"$eq": i["carrier"]}, "TARIFF": {"$eq": i["tariff_code"]},
                                  "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
                                  "OW_RT": {"$in": ["", i["oneway_return"]]},
                                  "FT_NT": {"$in": ["", i['footnote']]},
                                  "DATES_EFF_1":{"$lte":query_date},
                                  "DATES_DISC_1":{"$gte":query_date},
                                  "LOC_1_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
                                  "LOC_1_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
                                  "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
                                  "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
                                  "LOC_2_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
                                  "LOC_2_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
                                  "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
                                  "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
                                  })
    cur_record_1 = list(cur_record_1)
    print len(cur_record_1)
    for j in cur_record_1:
        i['TYPE_CODES_FARE_TYPE'] = j['TYPE_CODES_FARE_TYPE']
        i['TYPE_CODES_DAY_OF_WEEK_TYPE'] = j['TYPE_CODES_DAY_OF_WEEK_TYPE']
        i['TYPE_CODES_SEASON_TYPE'] = j['TYPE_CODES_SEASON_TYPE']
        i['fareID'] = i['fare_basis']+i['fare_rule']+i['footnote']
        i["used"] = 0
        print "inserted.....", countj
        countj += 1
        break
    print i['_id']
    fare_record.update({'_id': i['_id']}, i)
    print "done FZ ",counti
    counti += 1

cur_fare_record.close()


@measure(JUPITER_LOGGER)
def wild_card_check(r1, r2):
    rulesTT = r2.replace('-', '(.*)')
    rulesTTs = r2.replace('-', '(.*)')[(len(r2.replace('-', '(.*)'))-4):] == '(.*)'

    if(r2.replace('-', '(.*)')[(len(r2.replace('-', '(.*)'))-4):] == '(.*)'):
        pass
    else:
        rulesTT = rulesTT+'(.*)'
    _sysRegex = r"\b(?=\w)" + rulesTT + r"\b(?!\w)"
    if re.search(_sysRegex, r1, re.IGNORECASE):
        return True
    else:
        return False

print "stage 1 complete"
temp_cur = fare_record.find({"used":0, "carrier":{"$in":carrier_list}
                                # , "tariff_code":{"$nin":['022',
                                #                         '026',
                                #                         '032',
                                #                         '033',
                                #                         '034',
                                #                         '084',
                                #                         '185',
                                #                         '188',
                                #                         '305',
                                #                         '458',
                                #                         '880',
                                #                         ]}
                             }, timeout = False, allowDiskUse = True)\
    # .sort([("carrier",1),("tariff_code",1),("fare_rule",1),\
    #                                                            ("fare_basis",1),("footnote",1)])
count = 1
fare_basis, tariff_code, carrier, fare_rule, footnote = 0,0,0,0,0
print "stage 2 start"
for i in temp_cur:
    st = time.time()
    final_insert = {}

    countk = 1

    cur_record_0 = record_0.find({"TARIFF": {"$eq": i["tariff_code"]}, "CXR_CODE": {"$eq": i["carrier"]},
                                  "DATES_EFF_1":{"$lte":query_date},
                                  "DATES_DISC_1":{"$gte":query_date}})
    final_insert["Gen Rules"] = {}
    i["Gen Rules"] = {}
    for h in cur_record_0:

        for g in range(int(h["NO_SEGS"])):
            cur_record_2_cat_all = record_2_cat_all.find(
                {"CXR_CODE": {"$eq": i["carrier"]}, "GEN_APPL": {"$eq": ""}, \
                 "CAT_NO": {"$eq": h["CAT_NO_" + str(g + 1)]},
                 "TARIFF": {"$eq": h["SCR_TARIFF_" + str(g + 1)]}, \
                 "RULE_NO": {"$eq": h["GENERAL_RULE_NO_" + str(g + 1)]}, "NO_APPL": {"$eq": ""},
                 "RTG_NO":{"$in":["00000","99999","88888",i["rtg"]]},
                 "OW_RT":{"$in":["",i["oneway_return"]]},
                 "FT_NT":{"$in":["",i['footnote']]},
                 "DATES_EFF_1":{"$lte":query_date},
                 "DATES_DISC_1":{"$gte":query_date},
                 "TYPE_CODES_DAY_OF_WEEK_TYPE":{"$in":["",i["TYPE_CODES_DAY_OF_WEEK_TYPE"]]},
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
                # print "inside record 2 for record 0"

                if wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "":
                    # print "record 2 conditions satisfied for record 0"
                    # print j, "0000000000000000000000"
                    tbl = 1
                    tbl_no_list = []
                    ri_list = {}
                    while (tbl <= 200):
                        try:
                            tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                            ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j[
                                "DATA_TABLE_STRING_RI_" + str(tbl)]
                            # print "tbl_no:", j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]
                            #                         ri_list.append(j["DATA_TABLE_TABLE_STRING_RI_"+str(tbl)])
                            tbl += 1
                        except KeyError as e:
                            # print "error occured", e.args
                            break

                    # print "table no appended for record 0"

                    exec "cur_1 = record_3_cat_" + str(j['CAT_NO'])+ ".find({'TBL_NO': {'$in': tbl_no_list}})"
                    temp_list = []
                    for k in cur_1:
                        k["RI"] = ri_list[k["TBL_NO"]]
                        k["SEQ_NO"] = j["SEQ_NO"]
                        temp_list.append(k)
                    if (j["CAT_NO"] < "010"):
                        i["Gen Rules"]["cat_" + j["CAT_NO"][2]] = temp_list
                    else:
                        i["Gen Rules"]["cat_" + j["CAT_NO"][1:]] = temp_list
                    # print temp_list, "++++++"
                    break
    # print "gen rules categories inserted"


    # if (i["tariff_code"] != tariff_code or i["carrier"] != carrier or i["fare_rule"] != fare_rule):
    cur_record_2_cat_all = record_2_cat_all.find(
        {"RULE_NO": {"$eq": i["fare_rule"]}, "CXR_CODE": {"$eq": i["carrier"]}, \
         "NO_APPL": {"$eq": ""}, "TARIFF": {"$eq": i["tariff_code"]},
         "RTG_NO":{"$in":["00000","99999","88888",i["rtg"]]},
         "OW_RT":{"$in":["",i["oneway_return"]]},
         "FT_NT":{"$in":["",i['footnote']]},
         "DATES_EFF_1":{"$lte":query_date},
         "DATES_DISC_1":{"$gte":query_date},
         "TYPE_CODES_DAY_OF_WEEK_TYPE":{"$in":["",i["TYPE_CODES_DAY_OF_WEEK_TYPE"]]},
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
        # cur_record_2_cat_all = list(cur_record_2_cat_all)
    cat_check = []
    for j in cur_record_2_cat_all:
        # print "inside record 2"
        if ((j["CAT_NO"] not in cat_check) and (wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "")):
            # print "record 2 conditions satisfied"
            # print j["CAT_NO"], "*******************************"
            cat_check.append(j["CAT_NO"])
            tbl = 1
            tbl_no_list = []
            ri_list = {}
            while (tbl <= 200):
                try:
                    tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                    ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                    # print "tbl_no:", j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]
                    #                         ri_list.append(j["DATA_TABLE_TABLE_STRING_RI_"+str(tbl)])
                    tbl += 1
                except KeyError as e:
                    # print "error occured", e.args
                    break

            # print "table no appended"

            exec "cur_1 = record_3_cat_" + str(j['CAT_NO']) + ".find({'TBL_NO':{'$in':tbl_no_list}})"
            temp_list = []
            for k in cur_1:
                k["RI"] = ri_list[k["TBL_NO"]]
                k["SEQ_NO"] = j["SEQ_NO"]
                temp_list.append(k)
            if (j["CAT_NO"] < "010"):
                i["cat_"+j["CAT_NO"][2]] = temp_list
            else:
                i["cat_"+j["CAT_NO"][1:]] = temp_list

    # print "categories inserted"

    # if (i["tariff_code"] != tariff_code or i["carrier"] != carrier or i["fare_rule"] != fare_rule):
    cur_record_2_cat_10 = record_2_cat_10.find(
        {"RULE_NO": {"$eq": i["fare_rule"]}, "CXR_CODE": {"$eq": i["carrier"]}, \
         "NO_APPL": {"$eq": ""}, "TARIFF": {"$eq": i["tariff_code"]},
         "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
         "OW_RT": {"$in": ["", i["oneway_return"]]},
         "FT_NT": {"$in": ["", i['footnote']]},
         "DATES_EFF_1":{"$lte":query_date},
         "DATES_DISC_1":{"$gte":query_date},
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
        # cur_record_2_cat_10 = list(cur_record_2_cat_10)
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
        # print "inside record 2 cat 10"

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
            # print "record 2 cat 10 conditions satisfied"
            tbl = 1
            tbl_no_list = []
            ri_list = {}
            while (tbl <= 200):
                try:
                    tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                    ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                    # print "tbl_no:", j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]
                    #                         ri_list.append(j["DATA_TABLE_TABLE_STRING_RI_"+str(tbl)])
                    tbl += 1
                except KeyError as e:
                    # print "error occured", e.args
                    break

            # print "table no appended"


            cur_1 = record_3_cat_101.find({"TBL_NO": {"$in": tbl_no_list}})
            #                     temp_list_1 = []
            for k in cur_1:
                k["RI"] = ri_list[k['TBL_NO']]
                k["SEQ_NO"] = j["SEQ_NO"]
                temp_list_1.append(k)
            # temp_list = temp_list[0]
            i["cat_101"] = temp_list_1

            cur_2 = record_3_cat_102.find({"TBL_NO": {"$in": tbl_no_list}})
            #                     temp_list_2 = []
            for k in cur_2:
                k["RI"] = ri_list[k['TBL_NO']]
                k["SEQ_NO"] = j["SEQ_NO"]
                temp_list_2.append(k)
            # temp_list = temp_list[0]
            i["cat_102"] = temp_list_2

            cur_3 = record_3_cat_103.find({"TBL_NO": {"$in": tbl_no_list}})
            #                     temp_list_3 = []
            for k in cur_3:
                k["RI"] = ri_list[k['TBL_NO']]
                k["SEQ_NO"] = j["SEQ_NO"]
                temp_list_3.append(k)
            # temp_list = temp_list[0]
            i["cat_103"] = temp_list_3

            cur_4 = record_3_cat_104.find({"TBL_NO": {"$in": tbl_no_list}})
            #                     temp_list_4 = []
            for k in cur_4:
                k["RI"] = ri_list[k['TBL_NO']]
                k["SEQ_NO"] = j["SEQ_NO"]
                temp_list_4.append(k)
            # temp_list = temp_list[0]
            i["cat_104"] = temp_list_4

            cur_6 = record_3_cat_106.find({"TBL_NO": {"$in": tbl_no_list}})
            #                     temp_list_6 = []
            for k in cur_6:
                k["RI"] = ri_list[k['TBL_NO']]
                k["SEQ_NO"] = j["SEQ_NO"]
                temp_list_6.append(k)
            # temp_list = temp_list[0]
            i["cat_106"] = temp_list_6

            cur_7 = record_3_cat_107.find({"TBL_NO": {"$in": tbl_no_list}})
            #                     temp_list_7 = []
            for k in cur_7:
                k["RI"] = ri_list[k['TBL_NO']]
                k["SEQ_NO"] = j["SEQ_NO"]
                temp_list_7.append(k)
            # temp_list = temp_list[0]
            i["cat_107"] = temp_list_7
            #                     print final_insert["cat_107"], "1"

            cur_8 = record_3_cat_108.find({"TBL_NO": {"$in": tbl_no_list}})
            #                     temp_list_8 = []
            for k in cur_8:
                k["RI"] = ri_list[k['TBL_NO']]
                k["SEQ_NO"] = j["SEQ_NO"]
                temp_list_8.append(k)
            # temp_list = temp_list[0]
            i["cat_108"] = temp_list_8
            # print final_insert["cat_107"], "1"

            cur_9 = record_3_cat_109.find({"TBL_NO": {"$in": tbl_no_list}})
            #                     temp_list_9 = []
            for k in cur_9:
                k["RI"] = ri_list[k['TBL_NO']]
                k["SEQ_NO"] = j["SEQ_NO"]
                temp_list_9.append(k)
                #                         temp_list = temp_list[0]
                # print k["TBL_NO"], "-----------------109"
            i["cat_109"] = temp_list_9
            # print final_insert["cat_107"], "1"
            break

    # if (i["tariff_code"] != tariff_code or i["carrier"] != carrier or i["fare_rule"] != fare_rule):
    cur_record_2_cat_25 = record_2_cat_25.find(
        {"RULE_NO": {"$eq": i["fare_rule"]}, "CXR_CODE": {"$eq": i["carrier"]}, \
         "NO_APPL": {"$eq": ""}, "TARIFF": {"$eq": i["tariff_code"]},
         "RTG_NO":{"$in":["00000","99999","88888",i["rtg"]]},
         "OW_RT":{"$in":["",i["oneway_return"]]},
         "FT_NT":{"$in":["",i['footnote']]},
         "DATES_EFF_1":{"$lte":query_date},
         "DATES_DISC_1":{"$gte":query_date},
         "TYPE_CODES_DAY_OF_WEEK_TYPE":{"$in":["",i["TYPE_CODES_DAY_OF_WEEK_TYPE"]]},
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
        # cur_record_2_cat_25 = list(cur_record_2_cat_25)
    #         print final_insert["cat_107"], "2"
    for j in cur_record_2_cat_25:
        print "inside record 2 cat 25"

        if wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "":
            # print "record 2 cat 25 conditions satisfied"
            tbl = 1
            tbl_no_list = []
            ri_list = {}
            while (tbl <= 200):
                try:
                    tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                    ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                    # print "tbl_no:", j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]
                    #                         ri_list.append(j["DATA_TABLE_TABLE_STRING_RI_"+str(tbl)])
                    tbl += 1
                except KeyError as e:
                    # print "error occured", e.args
                    break

            # print "table no appended"

            cur_1 = record_3_cat_025.find({"TBL_NO": {"$in": tbl_no_list}})
            temp_list = []
            for k in cur_1:
                k["RI"] = ri_list[k['TBL_NO']]
                k["SEQ_NO"] = j["SEQ_NO"]
                temp_list.append(k)
            # temp_list = temp_list[0]
            i["cat_25"] = temp_list
            break

    final_insert["Footnotes"] = {}
    i["Footnotes"] = {}
    temp_list_3_fn = []
    temp_list_11_fn = []
    temp_list_14_fn = []
    temp_list_15_fn = []
    temp_list_23_fn = []

    # if (i["tariff_code"] != tariff_code or i["carrier"] != carrier or i["footnote"] != footnote):
    cur_record_2_cat_3_fn = record_2_cat_3_fn.find(
            {"CXR_CODE": {"$eq": i["carrier"]}, "FT_NT": {"$eq": i["footnote"]}, \
             "TARIFF": {"$eq": i["tariff_code"]}, "NO_APPL": {"$eq": ""},
             "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
             "OW_RT": {"$in": ["", i["oneway_return"]]},
             "DATES_EFF_1":{"$lte":query_date},
             "DATES_DISC_1":{"$gte":query_date},
             "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
             "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
             }).sort("SEQ_NO", 1)
        # cur_record_2_cat_3_fn = list(cur_record_2_cat_3_fn)
    #         print final_insert["cat_107"], "3"
    for j in cur_record_2_cat_3_fn:
        # print "inside record 2 cat 3 fn"

        if wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "":
            # print "record 2 cat 3 fn conditions satisfied"
            tbl = 1
            tbl_no_list = []
            ri_list = {}
            while (tbl <= 200):
                try:
                    tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                    ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                    # print "tbl_no:", j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]
                    #                         ri_list.append(j["DATA_TABLE_TABLE_STRING_RI_"+str(tbl)])
                    tbl += 1
                except KeyError as e:
                    # print "error occured", e.args
                    break

            # print "table no appended"
            i["Footnotes"]["FN_ID"] = i["footnote"]
            i["Footnotes"]["Tariff"] = i["tariff_code"]

            cur_3_fn = record_3_cat_003_fn.find({"TBL_NO": {"$in": tbl_no_list}})
            #                 temp_list = []
            for k in cur_3_fn:
                k["RI"] = ri_list[k['TBL_NO']]
                k["SEQ_NO"] = j["SEQ_NO"]
                temp_list_3_fn.append(k)
            # temp_list = temp_list[0]
            i["Footnotes"]["Cat_03_FN"] = temp_list_3_fn
            break

    # if (i["tariff_code"] != tariff_code or i["carrier"] != carrier or i["footnote"] != footnote):
    cur_record_2_cat_11_fn = record_2_cat_11_fn.find(
            {"CXR_CODE": {"$eq": i["carrier"]}, "FT_NT": {"$eq": i["footnote"]}, \
             "TARIFF": {"$eq": i["tariff_code"]}, "NO_APPL": {"$eq": ""},
             "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
             "OW_RT": {"$in": ["", i["oneway_return"]]},
             "DATES_EFF_1":{"$lte":query_date},
             "DATES_DISC_1":{"$gte":query_date},
             "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
             "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
             }).sort("SEQ_NO", 1)
        # cur_record_2_cat_11_fn = list(cur_record_2_cat_11_fn)

    #         print final_insert["cat_107"], "4"
    for j in cur_record_2_cat_11_fn:
        # print "inside record 2 cat 11 fn"

        if wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "":
            # print "record 2 cat 11 fn conditions satisfied"
            tbl = 1
            tbl_no_list = []
            ri_list = {}
            while (tbl <= 200):
                try:
                    tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                    ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                    # print "tbl_no:", j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]
                    #                         ri_list.append(j["DATA_TABLE_TABLE_STRING_RI_"+str(tbl)])
                    tbl += 1
                except KeyError as e:
                    # print "error occured", e.args
                    break

            # print "table no appended"
            i["Footnotes"]["FN_ID"] = i["footnote"]
            i["Footnotes"]["Tariff"] = i["tariff_code"]

            cur_11_fn = record_3_cat_011_fn.find({"TBL_NO": {"$in": tbl_no_list}})
            #                 temp_list = []
            for k in cur_11_fn:
                k["RI"] = ri_list[k['TBL_NO']]
                k["SEQ_NO"] = j["SEQ_NO"]
                temp_list_11_fn.append(k)
            # temp_list = temp_list[0]
            i["Footnotes"]["Cat_11_FN"] = temp_list_11_fn
            break

    # if (i["tariff_code"] != tariff_code or i["carrier"] != carrier or i["footnote"] != footnote):
    cur_record_2_cat_14_fn = record_2_cat_14_fn.find(
            {"CXR_CODE": {"$eq": i["carrier"]}, "FT_NT": {"$eq": i["footnote"]}, \
             "TARIFF": {"$eq": i["tariff_code"]}, "NO_APPL": {"$eq": ""},
             "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
             "OW_RT": {"$in": ["", i["oneway_return"]]},
             "DATES_EFF_1":{"$lte":query_date},
             "DATES_DISC_1":{"$gte":query_date},
             "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
             "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
             }).sort("SEQ_NO", 1)
        # cur_record_2_cat_14_fn = list(cur_record_2_cat_14_fn)
    #         print final_insert["cat_107"], "5"
    for j in cur_record_2_cat_14_fn:
        # print "inside record 2 cat 14 fn"

        if wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "":
            # print "record 2 cat 14 fn conditions satisfied"
            tbl = 1
            tbl_no_list = []
            ri_list = {}
            while (tbl <= 200):
                try:
                    tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                    ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                    # print "tbl_no:", j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]
                    #                         ri_list.append(j["DATA_TABLE_TABLE_STRING_RI_"+str(tbl)])
                    tbl += 1
                except KeyError as e:
                    # print "error occured", e.args
                    break

            # print "table no appended"
            i["Footnotes"]["FN_ID"] = i["footnote"]
            i["Footnotes"]["Tariff"] = i["tariff_code"]

            cur_14_fn = record_3_cat_014_fn.find({"TBL_NO": {"$in": tbl_no_list}})
            #                 temp_list = []
            for k in cur_14_fn:
                k["RI"] = ri_list[k['TBL_NO']]
                k["SEQ_NO"] = j["SEQ_NO"]
                temp_list_14_fn.append(k)
            # temp_list = temp_list[0]
            i["Footnotes"]["Cat_14_FN"] = temp_list_14_fn
            break

    # if (i["tariff_code"] != tariff_code or i["carrier"] != carrier or i["footnote"] != footnote):
    cur_record_2_cat_15_fn = record_2_cat_15_fn.find(
            {"CXR_CODE": {"$eq": i["carrier"]}, "FT_NT": {"$eq": i["footnote"]}, \
             "TARIFF": {"$eq": i["tariff_code"]}, "NO_APPL": {"$eq": ""},
             "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
             "OW_RT": {"$in": ["", i["oneway_return"]]},
             "DATES_EFF_1":{"$lte":query_date},
             "DATES_DISC_1":{"$gte":query_date},
             "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
             "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
             }).sort("SEQ_NO", 1)
        # cur_record_2_cat_15_fn = list(cur_record_2_cat_15_fn)
    #         print final_insert["cat_107"], "6"
    for j in cur_record_2_cat_15_fn:
        # print "inside record 2 cat 15 fn"

        if wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "":
            # print "record 2 cat 15 fn conditions satisfied"
            tbl = 1
            tbl_no_list = []
            ri_list = {}
            while (tbl <= 200):
                try:
                    tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                    ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                    # print "tbl_no:", j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]
                    #                         ri_list.append(j["DATA_TABLE_TABLE_STRING_RI_"+str(tbl)])
                    tbl += 1
                except KeyError as e:
                    # print "error occured", e.args
                    break

            # print "table no appended"
            i["Footnotes"]["FN_ID"] = i["footnote"]
            i["Footnotes"]["Tariff"] = i["tariff_code"]

            cur_15_fn = record_3_cat_015_fn.find({"TBL_NO": {"$in": tbl_no_list}})
            #                 temp_list = []
            for k in cur_15_fn:
                k["RI"] = ri_list[k['TBL_NO']]
                k["SEQ_NO"] = j["SEQ_NO"]
                temp_list_15_fn.append(k)
            # temp_list = temp_list[0]
            i["Footnotes"]["Cat_15_FN"] = temp_list_15_fn
            break

    # if (i["tariff_code"] != tariff_code or i["carrier"] != carrier or i["footnote"] != footnote):
    cur_record_2_cat_23_fn = record_2_cat_23_fn.find(
            {"CXR_CODE": {"$eq": i["carrier"]}, "FT_NT": {"$eq": i["footnote"]}, \
             "TARIFF": {"$eq": i["tariff_code"]}, "NO_APPL": {"$eq": ""},
             "RTG_NO": {"$in": ["00000", "99999", "88888", i["rtg"]]},
             "OW_RT": {"$in": ["", i["oneway_return"]]},
             "DATES_EFF_1":{"$lte":query_date},
             "DATES_DISC_1":{"$gte":query_date},
             "LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
             "LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
             "LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]}
             }).sort("SEQ_NO", 1)
        # cur_record_2_cat_23_fn = list(cur_record_2_cat_23_fn)
    #         print final_insert["cat_107"], "7"
    for j in cur_record_2_cat_23_fn:
        # print "inside record 2 cat 23 fn"

        if wild_card_check(i["fare_basis"], j["FARE_CLASS"]) or j["FARE_CLASS"] == "":
            # print "record 2 cat 23 fn conditions satisfied"
            tbl = 1
            tbl_no_list = []
            ri_list = {}
            while (tbl <= 200):
                try:
                    tbl_no_list.append(j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                    ri_list[j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = j["DATA_TABLE_STRING_RI_" + str(tbl)]
                    # print "tbl_no:", j["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]
                    #                         ri_list.append(j["DATA_TABLE_TABLE_STRING_RI_"+str(tbl)])
                    tbl += 1
                except KeyError as e:
                    # print "error occured", e.args
                    break

            # print "table no appended"
            i["Footnotes"]["FN_ID"] = i["footnote"]
            i["Footnotes"]["Tariff"] = i["tariff_code"]

            cur_23_fn = record_3_cat_023_fn.find({"TBL_NO": {"$in": tbl_no_list}})
            #                 temp_list = []
            for k in cur_23_fn:
                k["RI"] = ri_list[k['TBL_NO']]
                k["SEQ_NO"] = j["SEQ_NO"]
                temp_list_23_fn.append(k)
            # temp_list = temp_list[0]
            i["Footnotes"]["Cat_23_FN"] = temp_list_23_fn
            break
    countj = 1
    count986 = 1
    try:
        for j in range(len(i["cat_4"])):
            # try:
            #     i["cat_4"][j]["CXR_FLT"]["SEGS_CARRIER"]
            # except KeyError:
            cur_record_3_table_986 = record_3_table_986.find({"TBL_NO":{"$eq":i["cat_4"][j]["CXR_FLT"]}})
            for k in cur_record_3_table_986:
              i["cat_4"][j]["CXR_FLT"] = k
            cur_record_3_table_986 = record_3_table_986.find({"TBL_NO": {"$eq": i["cat_4"][j]["CXR_TBL"]}})
            for k in cur_record_3_table_986:
                i["cat_4"][j]["CXR_TBL"] = k
            # print countj, "cat_4"
            countj += 1

        countj = 1
        for j in range(len(i["cat_12"])):
            cur_record_3_table_986 = record_3_table_986.find({"TBL_NO": {"$eq": i["cat_12"][j]["CXR_FLT_TBL_NO"]}})
            for k in cur_record_3_table_986:
                i["cat_12"][j]["CXR_FLT_TBL_NO"] = k
            # print countj, "cat_12"
            countj += 1

        # fare_record.save(i)
        # print "inserted986", count986
        count986 += 1
    except KeyError:
        # print "error"
        pass

# print i, "-----------"
#     del i['BATCH_CI']
#     del i['LOC_1_COUNTRY']
    # del i['FCI_CARRIER_TBL_990_1']
    # del i['FCI_DI_1']
    del i['TYPE_CODES_FARE_TYPE']
    # del i['FCI_PSGR_AGE_MIN_1']
    # del i['FCI_TDM_1']
    # del i['FARE_CLASS_INFORMATION_SEGS']
    # del i['FCI_PSGR_AGE_MAX_1']
    # del i['OW_RT']
    # del i['LOC_1_TYPE']
    # del i['LOC_2_AREA']
    # del i['TEXT_TBL_NO']
    # del i['LOC_1_ZONE']
    # del i['DATES_DISC']
    # del i['TARIFF']
    # del i['LOC_2_STATE']
    # del i['CXR_CODE']
    # del i['FCI_TICKETING_CODE_1']
    # del i['FT_NT']
    # del i['LOC_2_CITY']
    # del i['FCI_PSGR_TYPE_1']
    # del i['LOC_1_AREA']
    # del i['FCI_FILLER_1']
    # del i['ACTION']
    # del i['DATES_DISC_1']
    # del i['FCI_TCM_1']
    # del i['NO_SEGS']
    # del i['RTG_NO']
    # del i['LOC_2_ZONE']
    # del i['LOC_2_COUNTRY']
    # del i['DATES_EFF']
    # del i['RULE_NO']
    # del i['DATES_EFF_1']
    # del i['FCI_RBD_TBL_994_1']
    # del i['FCI_DATE_TBL_994_1']
    # del i['BATCH_NO']
    # del i['MCN']
    # del i['PRC_CAT']
    # del i['FCI_TICKET_DESIGNATOR_1']
    # del i['DIS_CAT']
    # del i['LOC_1_STATE']
    # del i['FARE_CLASS']
    # del i['FCI_RBD_1']
    # del i['LOC_1_CITY']
    del i['TYPE_CODES_DAY_OF_WEEK_TYPE']
    # del i['REC_TYPE']
    # del i['SEQ_NO']
    # del i['UNAVAIL']
    # del i['LOC_2_TYPE']
    # del i['_id']
    # del i['LOC_2']
    del i['TYPE_CODES_SEASON_TYPE']
    # del i['FCI_COMMERCIAL_NAME_1']
    # del i['LOC_1']
    i["used"] = 1
    # print i["cat_4"]
    print i["_id"]
    fare_record.update({'_id':i['_id']}, i)
    # i["used"] = 1
    # temp_fare_rules_mongo_2.save(i)
    # print "document inserted in collection"
    print count, "  ", time.time() - st
    # print final_insert["Gen Rules"]
    # print final_insert["cat_9"]
    count += 1

temp_cur.close()
print "stage 2 complete"