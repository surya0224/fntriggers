"""
Author: Anamika Jaiswal
Date Created: 2018-05-15
File Name: yqyr_tab178190.py

This code puts the user defined zone values in embedded fields for JRNY_LOC_1_TYPE, and generates CARRIER_APPL_TABLE_NO_190.

"""

import pymongo
from pymongo import MongoClient
import time
from datetime import datetime, timedelta

from jupiter_AI import  JUPITER_DB, ATPCO_DB, JUPITER_LOGGER
from jupiter_AI.logutils import measure



@measure(JUPITER_LOGGER)
def yqyr_tab178190_main(system_date, file_time,client):
    db = client[ATPCO_DB]
    s1 = db.JUP_DB_ATPCO_YQYR_RecS1_change

    tab = db.JUP_DB_ATPCO_YQYR_Table_178
    cur = s1.find({"$or": [{"JRNY_LOC_1_TYPE": "U"}, {"JRNY_LOC_2_TYPE": "U"},{"CARRIER_APPL_TABLE_NO_190":{'$ne':"00000000"}}]}, no_cursor_timeout=True)
    # print cur.count()

    count = 0
    for i in cur:
        arrcity = []
        arrcountry = []
        arrzone = []
        arrarea = []
        narrcity = []
        narrcountry = []
        narrzone = []
        narrarea = []
        arrcity2 = []
        arrcountry2 = []
        arrzone2 = []
        arrarea2 = []
        narrcity2 = []
        narrcountry2 = []
        narrzone2 = []
        narrarea2 = []
        car=[]
        ncar=[]

        if i["JRNY_LOC_1_TYPE"] == "U":
            cur2 = tab.find({"TBL_NO": i["JRNY_LOC_1_ZONE_TABLE_NO_178"],'cancel':None}, no_cursor_timeout=True)

            # print (cur2.count())
            for j in cur2:
                if j["APPL"] == "":
                    if j["GEO_LOC_TYPE"] == "C":
                        arrcity.append(j["GEO_LOC"])
                    if j["GEO_LOC_TYPE"] == "A":
                        arrarea.append(j["GEO_LOC"])
                    if j["GEO_LOC_TYPE"] == "Z":
                        arrzone.append(j["GEO_LOC"])
                    if j["GEO_LOC_TYPE"] == "N":
                        arrcountry.append(j["GEO_LOC"])
                if j["APPL"] == "N":
                    if j["GEO_LOC_TYPE"] == "C":
                        narrcity.append(j["GEO_LOC"])
                    if j["GEO_LOC_TYPE"] == "A":
                        narrarea.append(j["GEO_LOC"])
                    if j["GEO_LOC_TYPE"] == "Z":
                        narrzone.append(j["GEO_LOC"])
                    if j["GEO_LOC_TYPE"] == "N":
                        narrcountry.append(j["GEO_LOC"])
            temp = {'APPL': '', "GEO_LOC_CITY": arrcity, "GEO_LOC_ZONE": arrzone, "GEO_LOC_AREA": arrarea,
                    "GEO_LOC_COUNTRY": arrcountry}
            temp2 = {'APPL': 'N', "GEO_LOC_CITY": narrcity, "GEO_LOC_ZONE": narrzone, "GEO_LOC_AREA": narrarea,
                     "GEO_LOC_COUNTRY": narrcountry}

            i["JRNY_LOC_1_ZONE_TABLE_178"] = [temp, temp2]

            s1.update({"_id": i["_id"]}, {'$set': {"JRNY_LOC_1_ZONE_TABLE_178":[temp,temp2]}})

        if i["JRNY_LOC_2_TYPE"] == "U":
            cur2 = tab.find({"TBL_NO": i["JRNY_LOC_2_ZONE_TABLE_NO_178"],'cancel':None}, no_cursor_timeout=True)

            # print (cur2.count())
            for j in cur2:
                if j["APPL"] == "":
                    if j["GEO_LOC_TYPE"] == "C":
                        arrcity2.append(j["GEO_LOC"])
                    if j["GEO_LOC_TYPE"] == "A":
                        arrarea2.append(j["GEO_LOC"])
                    if j["GEO_LOC_TYPE"] == "Z":
                        arrzone2.append(j["GEO_LOC"])
                    if j["GEO_LOC_TYPE"] == "N":
                        arrcountry2.append(j["GEO_LOC"])
                if j["APPL"] == "N":
                    if j["GEO_LOC_TYPE"] == "C":
                        narrcity2.append(j["GEO_LOC"])
                    if j["GEO_LOC_TYPE"] == "A":
                        narrarea2.append(j["GEO_LOC"])
                    if j["GEO_LOC_TYPE"] == "Z":
                        narrzone2.append(j["GEO_LOC"])
                    if j["GEO_LOC_TYPE"] == "N":
                        narrcountry2.append(j["GEO_LOC"])
            temp3 = {'APPL': '', "GEO_LOC_CITY": arrcity2, "GEO_LOC_ZONE": arrzone2, "GEO_LOC_AREA": arrarea2,
                     "GEO_LOC_COUNTRY": arrcountry2}
            temp4 = {'APPL': 'N', "GEO_LOC_CITY": narrcity2, "GEO_LOC_ZONE": narrzone2, "GEO_LOC_AREA": narrarea2,
                     "GEO_LOC_COUNTRY": narrcountry2}

            #i["JRNY_LOC_2_ZONE_TABLE_178"] = [temp3, temp4]
            s1.update({"_id": i["_id"]}, {'$set': {"JRNY_LOC_2_ZONE_TABLE_178":[temp3,temp4]}})

        count += 1
        print(count)
    print "Done tab 178"

    tab190 = db.JUP_DB_ATPCO_YQYR_Table_190
    counti=1
    cur = s1.find({"CARRIER_APPL_TABLE_NO_190": {'$ne': "00000000"}}, no_cursor_timeout=True)
    for i in cur:
        print counti
        car = []
        ncar = []
        #print i["CARRIER_APPL_TABLE_NO_190"]
        cur2 = tab190.find({"TBL_NO": i["CARRIER_APPL_TABLE_NO_190"],'cancel':None}, no_cursor_timeout=True)

        for j in cur2:

            no_segs = j['NO_OF_SEGS']
            for a in range(int(no_segs)):

                if j['APPL_SUBSTRING_' + str(a + 1)] in ["", " "]:
                    car.append(j['CXR_SUBSTRING_' + str(a + 1)])

                if j['APPL_SUBSTRING_' + str(a + 1)] == "X":
                    ncar.append(j['CXR_SUBSTRING_' + str(a + 1)])

        tempc = {'APPL': " ", 'CARRIER': car}
        tempc2 = {'APPL': 'X', "CARRIER": ncar}

        counti+=1
        # i["CARRIER_TABLE_190"] =[temp,temp2]

        s1.update({"_id": i["_id"]}, {"$set": {"CARRIER_TABLE_190": [tempc, tempc2]}})
    print "Done tab 190"
