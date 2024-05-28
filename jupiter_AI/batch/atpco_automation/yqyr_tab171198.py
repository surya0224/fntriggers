"""
Author: Anamika Jaiswal
Date Created: 2018-05-15
File Name: yqyr_tab171198.py

This code puts the user defined zone values in embedded fields for JRNY_LOC_1_TYPE, and generates CARRIER_APPL_TABLE_NO_190.

"""

import pymongo
from pymongo import MongoClient
import time
from datetime import datetime, timedelta

from jupiter_AI import  JUPITER_DB, ATPCO_DB, JUPITER_LOGGER
from jupiter_AI.logutils import measure



@measure(JUPITER_LOGGER)
def yqyr_tab171198_main(system_date, file_time,client):
    db = client[ATPCO_DB]
    s1 = db.JUP_DB_ATPCO_YQYR_RecS1_change

    tab = db.JUP_DB_ATPCO_YQYR_Table_171

    cur = s1.find({"FARE_CLASS_TBL_NO_171": {'$ne': "00000000"}}, no_cursor_timeout=True)

    for i in cur:
        fcc = []
        ft = []
        cur2 = tab.find({"TBL_NO": i["FARE_CLASS_TBL_NO_171"], 'cancel': None}, no_cursor_timeout=True)

        for j in cur2:
            fcc.append(j["FARE_CLASS_CODE"])
            ft.append(j["FARE_TYPE"])
        ft = [x for x in ft if x != '']
        if len(ft) == 0:
            ft = [""]
        temp1 = {"FARE_CLASS_CODE": fcc, "FARE_TYPE": ft}

        s1.update({"_id": i["_id"]}, {'$set': {"FARE_CLASS_TBL_171": temp1}})


#-----------------------------------------------------------------------------------------------------------------
    tab = db.JUP_DB_ATPCO_YQYR_Table_173
    cur = s1.find({"TKT_DSG_TBL_NO_173": {'$ne': "00000000"}}, no_cursor_timeout=True)

    for i in cur:
        cur2 = tab.find({"TBL_NO": i["TKT_DSG_TBL_NO_173"], 'cancel': None}, no_cursor_timeout=True).limit(1)

        for j in cur2:

            s1.update({"_id": i["_id"]}, {'$set': {"TICKET_DESIGNATOR": j["TICKET_DESIGNATOR"]}})


#----------------------------------------------------------------------------------------------------------------

    tab = db.JUP_DB_ATPCO_YQYR_Table_198
    cur = s1.find({"RBD_TBL_NO_198": {'$ne': "00000000"}}, no_cursor_timeout=True)

    for i in cur:
        cur2 = tab.find({"TBL_NO": i["RBD_TBL_NO_198"], 'cancel': None}, no_cursor_timeout=True)
        temp2 = []
        for j in cur2:
            temp2.append(j["RBD_1"])
            temp2.append(j["RBD_2"])
            temp2.append(j["RBD_3"])
            temp2.append(j["RBD_4"])
            temp2.append(j["RBD_5"])
            temp2 = [x for x in temp2 if x != '']


        s1.update({"_id": i["_id"]}, {'$set': {"RBD_198": temp2}})


