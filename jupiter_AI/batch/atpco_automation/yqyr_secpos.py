"""
Author: Anamika Jaiswal
Date Created: 2018-05-15
File Name: yqyr_secpos.py

This code puts the user defined zone values in embedded fields for SECT_PRT_LOC_TYPE, POS, POT.

"""

import pymongo
from pymongo import MongoClient


from jupiter_AI import JUPITER_DB, ATPCO_DB, JUPITER_LOGGER
from jupiter_AI.logutils import measure




@measure(JUPITER_LOGGER)
def yqyr_secpos_main(system_date, file_time,client):
    db = client[ATPCO_DB]
    s1 = db.JUP_DB_ATPCO_YQYR_RecS1_change
    tab = db.JUP_DB_ATPCO_YQYR_Table_178
    cur = s1.find({"$or": [{"SECT_PRT_LOC1_TYPE": "U"}, {"SECT_PRT_LOC_2_TYPE": "U"}]}, no_cursor_timeout=True)

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
        if i["SECT_PRT_LOC1_TYPE"] == "U":
            cur2 = tab.find({"TBL_NO": i["SECT_PRT_LOC_1_ZONE_TABLE_NO_178"],'cancel':None}, no_cursor_timeout=True)

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

            # i["SECT_PRT_LOC_1_ZONE_TABLE_178"] =[temp,temp2]

            s1.update({"_id": i["_id"]}, {'$set': {"SECT_PRT_LOC_1_ZONE_TABLE_178":[temp,temp2]}})

        if i["SECT_PRT_LOC_2_TYPE"] == "U":
            cur2 = tab.find({"TBL_NO": i["SECT_PRT_LOC_2_ZONE_TABLE_NO_178"],'cancel':None}, no_cursor_timeout=True)

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

            #i["SECT_PRT_LOC_2_ZONE_TABLE_178"] =[temp3,temp4]
            s1.update({"_id": i["_id"]}, {'$set': {"SECT_PRT_LOC_2_ZONE_TABLE_178":[temp3,temp4]}})


        count += 1
        print(count)


    tab = db.JUP_DB_ATPCO_YQYR_Table_178
    cur = s1.find({"POS_TYPE": "U"}, no_cursor_timeout=True)
    for i in cur:
        arrcity = []
        arrcountry = []
        arrzone = []
        arrarea = []
        narrcity = []
        narrcountry = []
        narrzone = []
        narrarea = []

        cur2 = tab.find({"TBL_NO": i["POS_ZONE_TABLE_178"],'cancel':None}, no_cursor_timeout=True)

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

        # i["POS_ZONE_178"] =[temp,temp2]

        s1.update({"_id": i["_id"]}, {"$set": {"POS_ZONE_178": [temp, temp2]}})

    cur = s1.find({"POT_TYPE": "U"}, no_cursor_timeout=True)
    for i in cur:
        arrcity = []
        arrcountry = []
        arrzone = []
        arrarea = []
        narrcity = []
        narrcountry = []
        narrzone = []
        narrarea = []


        cur2 = tab.find({"TBL_NO": i["POT_ZONE_TABLE_NO_178"],'cancel':None}, no_cursor_timeout=True)

        #print cur2.count()
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
        temp5 = {'APPL': '', "GEO_LOC_CITY": arrcity, "GEO_LOC_ZONE": arrzone, "GEO_LOC_AREA": arrarea,
                "GEO_LOC_COUNTRY": arrcountry}
        temp6 = {'APPL': 'N', "GEO_LOC_CITY": narrcity, "GEO_LOC_ZONE": narrzone, "GEO_LOC_AREA": narrarea,
                 "GEO_LOC_COUNTRY": narrcountry}

        #i["POT_ZONE_178"] =[temp5,temp6]

        s1.update({"_id": i["_id"]}, {"$set": {"POT_ZONE_178": [temp5, temp6]}})
    print "Sec 178 and Pos 178 done"




