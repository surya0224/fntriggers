from pymongo import MongoClient
import time
from datetime import datetime, timedelta

client = MongoClient('localhost', 43535)
db = client.ATPCO_stg_Aug
db_fzDB = client.fzDB_stg
# # client = MongoClient("mongodb://dbteam:KNjSZmiaNUGLmS0Bv2@13.92.251.7:42525/")
# # db = client.ATPCO
#
record_2_cat_3_fn = db.JUP_DB_ATPCO_Record_2_Cat_03_FN_change
record_2_cat_11_fn = db.JUP_DB_ATPCO_Record_2_Cat_11_FN_change
record_2_cat_14_fn = db.JUP_DB_ATPCO_Record_2_Cat_14_FN_change
record_2_cat_15_fn = db.JUP_DB_ATPCO_Record_2_Cat_15_FN_change
record_2_cat_23_fn = db.JUP_DB_ATPCO_Record_2_Cat_23_FN_change
record_2_cat_all = db.JUP_DB_ATPCO_Record_2_Cat_All_change
record_2_cat_10 = db.JUP_DB_ATPCO_Record_2_Cat_10_change
record_2_cat_25 = db.JUP_DB_ATPCO_Record_2_Cat_25_change
record_1 = db.JUP_DB_ATPCO_Record_1_change
record_0 = db.JUP_DB_ATPCO_Record_0_change
record_8 = db.JUP_DB_ATPCO_Record_8_change
record_2_cat_3_fn_main = db.JUP_DB_ATPCO_Record_2_Cat_03_FN
record_2_cat_11_fn_main = db.JUP_DB_ATPCO_Record_2_Cat_11_FN
record_2_cat_14_fn_main = db.JUP_DB_ATPCO_Record_2_Cat_14_FN
record_2_cat_15_fn_main = db.JUP_DB_ATPCO_Record_2_Cat_15_FN
record_2_cat_23_fn_main = db.JUP_DB_ATPCO_Record_2_Cat_23_FN
record_2_cat_all_main = db.JUP_DB_ATPCO_Record_2_Cat_All
record_2_cat_10_main = db.JUP_DB_ATPCO_Record_2_Cat_10
record_2_cat_25_main = db.JUP_DB_ATPCO_Record_2_Cat_25
record_0_main = db.JUP_DB_ATPCO_Record_0
record_1_main = db.JUP_DB_ATPCO_Record_1
record_8_main = db.JUP_DB_ATPCO_Record_8
fare_record = db_fzDB.JUP_DB_ATPCO_Fares_change_2
fare_record_main = db_fzDB.JUP_DB_ATPCO_Fares_Dump

system_date = datetime(2017,9,22)
#query_date = str(0)+datetime.strftime(system_date, "%y%m%d")
system_date = datetime.strftime(system_date, "%Y%m%d")
#
# file_dates = ["20170820","20170821","20170822","20170823","20170824","20170825","20170826","20170827","20170828",
#               "20170829","20170830","20170831","20170901","20170902","20170903","20170904","20170905","20170906",
#               "20170907","20170908","20170909","20170910","20170911","20170912","20170913","20170914","20170915","20170916","20170917", "20170918","20170919","20170920","20170921"]
st = time.time()
count = 1
#
cur = fare_record.find({}).skip(30609264)

print "start", cur.count()
for i in cur:
	if i["ACTION"] == "I":
		print "passed ACTION - I"
	else:
		tariff = i["tariff_code"]
		carrier = i["carrier"]
		od = i["OD"]
		link_no = i["LINK_NO"]
		fare_basis = i["fare_basis"]
		eff_date = i["effective_from"]
		disc_date = i["effective_to"]
		query_date = datetime.strftime(datetime.strptime(i["file_date"], "%Y%m%d"), "%Y-%m-%d")

		 # if int(tariff) < 10:
		 #     tariff = str(0) + str(0) + str(tariff)
		 # elif 9 < int(tariff) < 100 :
		 #     tariff = str(0) + str(tariff)
		 # else:
		 #     tariff = str(tariff)

		print "querying fare record"
		cur_1 = fare_record_main.find({"tariff_code":tariff, "carrier":carrier, "OD":od, "fare_basis":fare_basis,
									   "LINK_NO":link_no})
		print "queried", cur_1.count()
		for j in cur_1:
			print "-------------------------------------------"
			print j["effective_to"], "---- ORIGINAL DISC DATE"
			print eff_date, "---- NEW EFF DATE"

			if (eff_date == query_date) and (j["effective_to"] == None or j["effective_to"] > eff_date):
				j["effective_to"] = datetime.strftime(datetime.strptime(eff_date, "%Y-%m-%d"), "%Y-%m-%d")
			elif (eff_date != query_date) and (j["effective_to"] == None or j["effective_to"] > eff_date):
				j["effective_to"] = datetime.strftime(datetime.strptime(eff_date, "%Y-%m-%d") - timedelta(days=1), "%Y-%m-%d")

			# if (eff_date == query_date) and (j["effective_to"] == "0999999" or
			# 	datetime.strftime(datetime.strptime(j["effective_to"][1:], "%y%m%d"), "%Y-%m-%d") > eff_date):
			# 	j["effective_to"] = str(0) + datetime.strftime(datetime.strptime(eff_date, "%Y-%m-%d"), "%y%m%d")
			# elif (eff_date != query_date) and (j["effective_to"] == "0999999" or
			# 	datetime.strftime(datetime.strptime(j["effective_to"][1:], "%y%m%d"), "%Y-%m-%d") > eff_date):
			# 	j["effective_to"] = str(0) + datetime.strftime(datetime.strptime(eff_date, "%Y-%m-%d") - timedelta(days=1), "%y%m%d")

			print j["effective_to"], "---- CHANGED ORIGINAL DISC DATE"
			fare_record_main.update({"_id": j["_id"]}, j)

	# if (i["effective_to"] != "" and i["effective_to"] != None):
	# 	i["effective_to"] = str(0) + datetime.strftime(datetime.strptime(i["effective_to"], "%Y-%m-%d"), "%y%m%d")
	# else:
	# 	i["effective_to"] = "0999999"
    #
	# if (i["effective_from"] != "" and i["effective_from"] != None):
	# 	i["effective_from"] = str(0) + datetime.strftime(datetime.strptime(i["effective_from"], "%Y-%m-%d"), "%y%m%d")
	# else:
	# 	i["effective_from"] = "0999999"
	fare_record_main.insert(i)
	print count
	count += 1
cur.close()
print "time taken -- ", time.time() - st





cur = record_2_cat_3_fn.find({})
print "start", cur.count()
count = 1
for i in cur:
    if i["ACTION"] == "2":
        print "passed ACTION - 2"
    else:
        print "querying changed coll 3"
        cur_1 = record_2_cat_3_fn_main.find({"CXR_CODE":i["CXR_CODE"], "TARIFF":i["TARIFF"], "FT_NT":i["FT_NT"],
                                             "CAT_NO":i["CAT_NO"], "SEQ_NO":i["SEQ_NO"]})
        print "queried"
        for j in cur_1:
            print "-------------------------------------------"
            print j["DATES_DISC_1"], "---- ORIGINAL DISC DATE"
            print i["DATES_EFF_1"], "---- NEW EFF DATE"
            if (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] == system_date):
                j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d"), "%y%m%d")
                j["DATES_DISC_1"] = i["DATES_EFF_1"]
            elif (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] != system_date):
                j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days = 1), "%y%m%d")
                j["DATES_DISC_1"] = datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days = 1), "%Y%m%d")

            print j["DATES_DISC_1"], "---- CHANGED ORIGINAL DISC DATE"
            record_2_cat_3_fn_main.update({"_id":j["_id"]}, j)

    record_2_cat_3_fn_main.save(i)
    print count
    count += 1

cur = record_2_cat_11_fn.find({})
print "start", cur.count()
count = 1
for i in cur:
    if i["ACTION"] == "2":
        print "passed ACTION - 2"
    else:
        print "querying changed coll 11"
        cur_1 = record_2_cat_11_fn_main.find({"CXR_CODE": i["CXR_CODE"], "TARIFF": i["TARIFF"], "FT_NT": i["FT_NT"],
                                              "CAT_NO": i["CAT_NO"], "SEQ_NO": i["SEQ_NO"]})
        print "queried"
        for j in cur_1:
            print "-------------------------------------------"
            print j["DATES_DISC_1"], "---- ORIGINAL DISC DATE"
            print i["DATES_EFF_1"], "---- NEW EFF DATE"
            if (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] == system_date):
                j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d"), "%y%m%d")
                j["DATES_DISC_1"] = i["DATES_EFF_1"]
            elif (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] != system_date):
                j["DATES_DISC"] = str(0) + datetime.strftime(
                    datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days=1), "%y%m%d")
                j["DATES_DISC_1"] = datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days=1),
                                                      "%Y%m%d")

            print j["DATES_DISC_1"], "---- CHANGED ORIGINAL DISC DATE"
            record_2_cat_11_fn_main.update({"_id": j["_id"]}, j)

    record_2_cat_11_fn_main.save(i)
    print count
    count += 1

cur = record_2_cat_14_fn.find({})
print "start", cur.count()
count = 1
for i in cur:
    if i["ACTION"] == "2":
        print "passed ACTION - 2"
    else:
        print "querying changed coll 14"
        cur_1 = record_2_cat_14_fn_main.find({"CXR_CODE": i["CXR_CODE"], "TARIFF": i["TARIFF"], "FT_NT": i["FT_NT"],
                                              "CAT_NO": i["CAT_NO"], "SEQ_NO": i["SEQ_NO"]})
        print "queried"
        for j in cur_1:
            print "-------------------------------------------"
            print j["DATES_DISC_1"], "---- ORIGINAL DISC DATE"
            print i["DATES_EFF_1"], "---- NEW EFF DATE"
            if (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] == system_date):
                j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d"), "%y%m%d")
                j["DATES_DISC_1"] = i["DATES_EFF_1"]
            elif (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] != system_date):
                j["DATES_DISC"] = str(0) + datetime.strftime(
                    datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days=1), "%y%m%d")
                j["DATES_DISC_1"] = datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days=1),
                                                      "%Y%m%d")

            print j["DATES_DISC_1"], "---- CHANGED ORIGINAL DISC DATE"
            record_2_cat_14_fn_main.update({"_id": j["_id"]}, j)

    record_2_cat_14_fn_main.save(i)
    print count
    count += 1

cur = record_2_cat_15_fn.find({})
print "start", cur.count()
count = 1
for i in cur:
    if i["ACTION"] == "2":
        print "passed ACTION - 2"
    else:
        print "querying changed coll 15"
        cur_1 = record_2_cat_15_fn_main.find({"CXR_CODE": i["CXR_CODE"], "TARIFF": i["TARIFF"], "FT_NT": i["FT_NT"],
                                              "CAT_NO": i["CAT_NO"], "SEQ_NO": i["SEQ_NO"]})
        print "queried"
        for j in cur_1:
            print "-------------------------------------------"
            print j["DATES_DISC_1"], "---- ORIGINAL DISC DATE"
            print i["DATES_EFF_1"], "---- NEW EFF DATE"
            if (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] == system_date):
                j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d"), "%y%m%d")
                j["DATES_DISC_1"] = i["DATES_EFF_1"]
            elif (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] != system_date):
                j["DATES_DISC"] = str(0) + datetime.strftime(
                    datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days=1), "%y%m%d")
                j["DATES_DISC_1"] = datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days=1),
                                                      "%Y%m%d")

            print j["DATES_DISC_1"], "---- CHANGED ORIGINAL DISC DATE"
            record_2_cat_15_fn_main.update({"_id": j["_id"]}, j)

    record_2_cat_15_fn_main.save(i)
    print count
    count += 1

cur = record_2_cat_23_fn.find({})
print "start", cur.count()
count = 1
for i in cur:
    if i["ACTION"] == "2":
        print "passed ACTION - 2"
    else:
        print "querying changed coll 23"
        cur_1 = record_2_cat_23_fn_main.find({"CXR_CODE": i["CXR_CODE"], "TARIFF": i["TARIFF"], "FT_NT": i["FT_NT"],
                                              "CAT_NO": i["CAT_NO"], "SEQ_NO": i["SEQ_NO"]})
        print "queried"
        for j in cur_1:
            print "-------------------------------------------"
            print j["DATES_DISC_1"], "---- ORIGINAL DISC DATE"
            print i["DATES_EFF_1"], "---- NEW EFF DATE"
            if (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] == system_date):
                j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d"), "%y%m%d")
                j["DATES_DISC_1"] = i["DATES_EFF_1"]
            elif (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] != system_date):
                j["DATES_DISC"] = str(0) + datetime.strftime(
                    datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days=1), "%y%m%d")
                j["DATES_DISC_1"] = datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days=1),
                                                      "%Y%m%d")

            print j["DATES_DISC_1"], "---- CHANGED ORIGINAL DISC DATE"
            record_2_cat_23_fn_main.update({"_id": j["_id"]}, j)

    record_2_cat_23_fn_main.save(i)
    print count
    count += 1

cur = record_2_cat_all.find({})
count = 1
st = time.time()
print "start", cur.count()
for i in cur:
    if i["ACTION"] == "2":
        print "passed ACTION - 2"
    else:
        print "querying changed coll all"
        cur_1 = record_2_cat_all_main.find({"CXR_CODE":i["CXR_CODE"], "TARIFF":i["TARIFF"], "RULE_NO":i["RULE_NO"],
                                            "CAT_NO":i["CAT_NO"], "SEQ_NO":i["SEQ_NO"]})
        print "queried"
        for j in cur_1:
            print "-------------------------------------------"
            print j["DATES_DISC_1"], "---- ORIGINAL DISC DATE"
            print i["DATES_EFF_1"], "---- NEW EFF DATE"
            if (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] == system_date):
                j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d"), "%y%m%d")
                j["DATES_DISC_1"] = i["DATES_EFF_1"]
            elif (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] != system_date):
                j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days = 1), "%y%m%d")
                j["DATES_DISC_1"] = datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days = 1), "%Y%m%d")

            print j["DATES_DISC_1"], "---- CHANGED ORIGINAL DISC DATE"
            record_2_cat_all_main.update({"_id":j["_id"]}, j)

    record_2_cat_all_main.save(i)
    print count
    count += 1
print "done", time.time() - st

cur = record_2_cat_10.find({})
print "start", cur.count()
for i in cur:
    if i["ACTION"] == "2":
        print "passed ACTION - 2"
    else:
        print "querying changed coll 10"
        cur_1 = record_2_cat_10_main.find({"CXR_CODE":i["CXR_CODE"], "TARIFF":i["TARIFF"], "RULE_NO":i["RULE_NO"],
                                           "CAT_NO":i["CAT_NO"], "SEQ_NO":i["SEQ_NO"]})
        print "queried"
        for j in cur_1:
            print "-------------------------------------------"
            print j["DATES_DISC_1"], "---- ORIGINAL DISC DATE"
            print i["DATES_EFF_1"], "---- NEW EFF DATE"
            if (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] == system_date):
                j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d"), "%y%m%d")
                j["DATES_DISC_1"] = i["DATES_EFF_1"]
            elif (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] != system_date):
                j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days = 1), "%y%m%d")
                j["DATES_DISC_1"] = datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days = 1), "%Y%m%d")

            print j["DATES_DISC_1"], "---- CHANGED ORIGINAL DISC DATE"
            record_2_cat_10_main.update({"_id":j["_id"]}, j)

    record_2_cat_10_main.save(i)

cur = record_2_cat_25.find({})
print "start", cur.count()
for i in cur:
    if i["ACTION"] == "2":
        print "passed ACTION - 2"
    else:
        print "querying changed coll 25"
        cur_1 = record_2_cat_25_main.find({"CXR_CODE":i["CXR_CODE"], "TARIFF":i["TARIFF"], "RULE_NO":i["RULE_NO"],
                                           "CAT_NO":i["CAT_NO"], "SEQ_NO":i["SEQ_NO"]})
        print "queried"
        for j in cur_1:
            print "-------------------------------------------"
            print j["DATES_DISC_1"], "---- ORIGINAL DISC DATE"
            print i["DATES_EFF_1"], "---- NEW EFF DATE"
            if (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] == system_date):
                j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d"), "%y%m%d")
                j["DATES_DISC_1"] = i["DATES_EFF_1"]
            elif (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] != system_date):
                j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days = 1), "%y%m%d")
                j["DATES_DISC_1"] = datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days = 1), "%Y%m%d")

            print j["DATES_DISC_1"], "---- CHANGED ORIGINAL DISC DATE"
            record_2_cat_25_main.update({"_id":j["_id"]}, j)

    record_2_cat_25_main.save(i)

cur = record_1.find({})
print "Record_1 start", cur.count()
count = 1
for i in cur:
    if i["ACTION"] == "2":
        print "passed ACTION - 2"
    else:
        print "querying changed coll 1"
        cur_1 = record_1_main.find({"CXR_CODE":i["CXR_CODE"], "TARIFF":i["TARIFF"], "RULE_NO":i["RULE_NO"],
                                    "FARE_CLASS":i["FARE_CLASS"], "SEQ_NO":i["SEQ_NO"]})
        print "queried"
        for j in cur_1:
            print "-------------------------------------------"
            print j["DATES_DISC_1"], "---- ORIGINAL DISC DATE"
            print i["DATES_EFF_1"], "---- NEW EFF DATE"
            if (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] == system_date):
                j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d"), "%y%m%d")
                j["DATES_DISC_1"] = i["DATES_EFF_1"]
            elif (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] != system_date):
                j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days = 1), "%y%m%d")
                j["DATES_DISC_1"] = datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days = 1), "%Y%m%d")

            print j["DATES_DISC_1"], "---- CHANGED ORIGINAL DISC DATE"
            record_1_main.update({"_id":j["_id"]}, j)

    record_1_main.save(i)
    print count
    count += 1
print "Record_1 end"
print count

cur = record_0.find({})
print "start", cur.count()
for i in cur:
    if i["ACTION"] == "2":
        print "passed ACTION - 2"
    else:
        print "querying changed coll 0"
        cur_1 = record_0_main.find({"CXR_CODE":i["CXR_CODE"], "TARIFF":i["TARIFF"]})
        print "queried"
        for j in cur_1:
            print "-------------------------------------------"
            print j["DATES_DISC_1"], "---- ORIGINAL DISC DATE"
            print i["DATES_EFF_1"], "---- NEW EFF DATE"
            if (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] == system_date):
                j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d"), "%y%m%d")
                j["DATES_DISC_1"] = i["DATES_EFF_1"]
            elif (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] != system_date):
                j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days = 1), "%y%m%d")
                j["DATES_DISC_1"] = datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days = 1), "%Y%m%d")

            print j["DATES_DISC_1"], "---- CHANGED ORIGINAL DISC DATE"
            record_0_main.update({"_id":j["_id"]}, j)

    record_0_main.save(i)

cur = record_8.find({})
print "start", cur.count()
for i in cur:
    if i["ACTION"] == "2":
        print "passed ACTION - 2"
    else:
        print "querying changed coll 8"
        cur_1 = record_8_main.find({"CXR_CODE":i["CXR_CODE"], "TARIFF":i["TARIFF"], "RULE_NO":i["RULE_NO"],
                                    "REC_ID":i["REC_ID"]})
        print "queried"
        for j in cur_1:
            print "-------------------------------------------"
            print j["DATES_DISC_1"], "---- ORIGINAL DISC DATE"
            print i["DATES_EFF_1"], "---- NEW EFF DATE"
            if (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] == system_date):
                j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d"), "%y%m%d")
                j["DATES_DISC_1"] = i["DATES_EFF_1"]
            elif (j["DATES_DISC_1"] > i["DATES_EFF_1"]) and (i["DATES_EFF_1"] != system_date):
                j["DATES_DISC"] = str(0) + datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days = 1), "%y%m%d")
                j["DATES_DISC_1"] = datetime.strftime(datetime.strptime(i["DATES_EFF_1"], "%Y%m%d") - timedelta(days = 1), "%Y%m%d")

            print j["DATES_DISC_1"], "---- CHANGED ORIGINAL DISC DATE"
            record_8_main.update({"_id":j["_id"]}, j)

    record_8_main.save(i)
