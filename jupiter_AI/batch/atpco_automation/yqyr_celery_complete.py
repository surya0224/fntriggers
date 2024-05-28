#yw\qyr without filedate

"""
Author: Anamika Jaiswal
Date Created: 2018-05-15
File Name: YQYR_celery.py

This code computes the YQYR values for the each fare record on Celery.
No threading
"""


import pymongo
from pymongo import MongoClient
import math
client = MongoClient()
import time
import re
import datetime
import pandas as pd


# from jupiter_AI import client, JUPITER_DB, ATPCO_DB, JUPITER_LOGGER
# from jupiter_AI.logutils import measure
#
#
# db_ATPCO = client[ATPCO_DB]
# db_fzDB = client[JUPITER_DB]


db_ATPCO=pymongo.MongoClient("35.154.147.153:27022")["ATPCO_stg"]
db_ATPCO.authenticate('mgadmin', 'mgadmin@2018', source='admin')

db_fzDB=pymongo.MongoClient("35.154.147.153:27022")["fzDB_stg"]
db_fzDB.authenticate('mgadmin', 'mgadmin@2018', source='admin')

coll = db_fzDB.JUP_DB_ATPCO_Fares_Rules
# coll = db_fzDB.yqyrnewconn
# coll = db_fzDB.yqyrptp
#
hubc=db_fzDB.JUP_DB_Carrier_hubs
# coll=db_fzDB.TBD
# coll=db_fzDB.ZZYQYR
yqyr = db_ATPCO.JUP_DB_ATPCO_YQYR_RecS1
# yqyr=db_fzDB.test_s1
exr = db_fzDB.JUP_DB_Exchange_Rate


count = 1


# @measure(JUPITER_LOGGER)
def wild_card_check(r1, r2):
    rulesTT = r2.replace('-', '(.*)')
    #rulesTTs = r2.replace('-', '(.*)')[(len(r2.replace('-', '(.*)')) - 4):] == '(.*)'

    if (r2.replace('-', '(.*)')[(len(r2.replace('-', '(.*)')) - 4):] == '(.*)'):
        pass
    else:
        rulesTT = rulesTT + '(.*)'
    _sysRegex = r"\b(?=\w)" + rulesTT + r"\b(?!\w)"
    if re.search(_sysRegex, r1, re.IGNORECASE):
        return True
    else:
        return False


# @measure(JUPITER_LOGGER)
def YQYQvalue(y1,y2,ysfa1,ysfa2,owrt):

    val=0
    if ysfa1!=ysfa2:
        val=y1+y2
        if owrt== "Y":
            val=val*2
    elif ysfa1=="":
        val=y1+y2
        if owrt == "Y":
            val=val*2
    elif ysfa1=="1":
        val=max(y1,y2)
        if owrt == "Y":
            val=val*2
    elif ysfa1=="2":
        val = max(y1, y2)

    return val




def stage1_hub1(cxr,origin, origin_area, origin_zone, origin_country, destination, destination_area,
             destination_zone, destination_country,system_date, hub, hub_area, hub_zone, hub_country):
    print "============HUB1 stage1============="

    cur2 = yqyr.find({"$and": [{"flag": 1}, {"CXR_CODE": cxr}, {"DISC_DATE": {"$gte": system_date}},
                               {"EFF_DATE": {"$lte": system_date}},
                               {"$or": [{"TICKET_DATES_FIRST": "999999"},
                                        {"TICKET_DATES_FIRST": {"$lt": system_date}}]},
                               {"TICKET_DATES_LAST": {"$gte": system_date}},

                               {"POS_AREA": {"$in": [origin_area, ""]}},
                               {"POS_ZONE": {"$in": [origin_zone, ""]}},
                               {"POS_CITY": {"$in": [origin, ""]}},
                               {"POS_CNTRY": {"$in": [origin_country, ""]}},
                               {"POT_AREA": {"$in": [origin_area, ""]}},
                               {"POT_CITY": {"$in": [origin, ""]}},
                               {"POT_CNTRY": {"$in": [origin_country, ""]}},

                               {"JRNY_LOC_1_AREA": {"$in": [origin_area, ""]}},
                               {"JRNY_LOC_1_CITY": {"$in": [origin, ""]}},
                               {"JRNY_LOC_1_CNTRY": {"$in": [origin_country, ""]}},
                               {"JRNY_LOC_2_AREA": {"$in": [destination_area, ""]}},
                               {"JRNY_LOC_2_CITY": {"$in": [destination, ""]}},
                               {"JRNY_LOC_2_CNTRY": {"$in": [destination_country, ""]}},
                               {"SECT_PRT_LOC_1_AREA": {"$in": [origin_area, ""]}},
                               {"SECT_PRT_LOC_1_CITY": {"$in": [origin, ""]}},
                               {"SECT_PRT_LOC_1_CNTRY": {"$in": [origin_country, ""]}},
                               {"SECT_PRT_LOC_2_AREA": {"$in": [hub_area, ""]}},
                               {"SECT_PRT_LOC_2_CITY": {"$in": [hub, ""]}},
                               {"SECT_PRT_LOC_2_CNTRY": {"$in": [hub_country, ""]}},
                               {"POT_ZONE": {"$in": [origin_zone, ""]}},
                               {"JRNY_LOC_1_ZONE": {"$in": [origin_zone, ""]}},
                               {"JRNY_LOC_2_ZONE": {"$in": [destination_zone, ""]}},
                               {"SECT_PRT_LOC_1_ZONE": {"$in": [origin_zone, ""]}},
                               {"SECT_PRT_LOC_2_ZONE": {"$in": [hub_zone, ""]}},

                               {"$or": [{'POS_ZONE_178': None},
                                        {'$and': [{"POS_ZONE_178.1.GEO_LOC_AREA": {"$ne": origin_area}},
                                                  {"POS_ZONE_178.1.GEO_LOC_ZONE": {"$ne": origin_zone}},
                                                  {"POS_ZONE_178.1.GEO_LOC_CITY": {"$ne": origin}},
                                                  {"POS_ZONE_178.1.GEO_LOC_COUNTRY": {"$ne": origin_country}},
                                                  {"$or": [{"POS_ZONE_178.0.GEO_LOC_CITY": {"$eq": origin}},
                                                           {"POS_ZONE_178.0.GEO_LOC_AREA": {
                                                               "$eq": origin_area}},
                                                           {"POS_ZONE_178.0.GEO_LOC_ZONE": {
                                                               "$eq": origin_zone}}, {
                                                               "POS_ZONE_178.0.GEO_LOC_COUNTRY": {
                                                                   "$eq": origin_country}},
                                                           {'$and': [{"POS_ZONE_178.0.GEO_LOC_AREA": {'$size': 0}},
                                                                     {"POS_ZONE_178.0.GEO_LOC_ZONE": {'$size': 0}},
                                                                     {"POS_ZONE_178.0.GEO_LOC_COUNTRY": {
                                                                         '$size': 0}},
                                                                     {"POS_ZONE_178.0.GEO_LOC_CITY": {'$size': 0}}]}
                                                           ]}]}]},

                               {"$or": [{'POT_ZONE_178': None},
                                        {'$and': [{"POT_ZONE_178.1.GEO_LOC_AREA": {"$ne": origin_area}},
                                                  {"POT_ZONE_178.1.GEO_LOC_ZONE": {"$ne": origin_zone}},
                                                  {"POT_ZONE_178.1.GEO_LOC_CITY": {"$ne": origin}},
                                                  {"POT_ZONE_178.1.GEO_LOC_COUNTRY": {"$ne": origin_country}},
                                                  {"$or": [{"POT_ZONE_178.0.GEO_LOC_CITY": {"$eq": origin}},
                                                           {"POT_ZONE_178.0.GEO_LOC_AREA": {
                                                               "$eq": origin_area}},
                                                           {"POT_ZONE_178.0.GEO_LOC_ZONE": {
                                                               "$eq": origin_zone}}, {
                                                               "POT_ZONE_178.0.GEO_LOC_COUNTRY": {
                                                                   "$eq": origin_country}},
                                                           {'$and': [{"POT_ZONE_178.0.GEO_LOC_AREA": {'$size': 0}},
                                                                     {"POT_ZONE_178.0.GEO_LOC_ZONE": {'$size': 0}},
                                                                     {"POT_ZONE_178.0.GEO_LOC_COUNTRY": {
                                                                         '$size': 0}},
                                                                     {"POT_ZONE_178.0.GEO_LOC_CITY": {'$size': 0}}]}
                                                           ]}]}]},
                               {"$or": [{'CARRIER_TABLE_190': None},
                                        {'$and': [{"$or": [{"CARRIER_TABLE_190.0.CARRIER": {'$size': 0}}, {
                                            "CARRIER_TABLE_190.0.CARRIER": {"$in": [cxr, "$$"]}}]},
                                                  {"$or": [{"CARRIER_TABLE_190.1.CARRIER": {'$size': 0}},
                                                           {"CARRIER_TABLE_190.1.CARRIER": {"$ne": cxr}}]}]
                                         }]},

                               {"$or": [{'JRNY_LOC_1_ZONE_TABLE_178': None},
                                        {'$and': [
                                            {"JRNY_LOC_1_ZONE_TABLE_178.1.GEO_LOC_AREA": {"$ne": origin_area}},
                                            {"JRNY_LOC_1_ZONE_TABLE_178.1.GEO_LOC_ZONE": {"$ne": origin_zone}},
                                            {"JRNY_LOC_1_ZONE_TABLE_178.1.GEO_LOC_CITY": {"$ne": origin}},
                                            {"JRNY_LOC_1_ZONE_TABLE_178.1.GEO_LOC_COUNTRY": {
                                                "$ne": origin_country}},
                                            {"$or": [
                                                {"JRNY_LOC_1_ZONE_TABLE_178.0.GEO_LOC_CITY": {"$eq": origin}},
                                                {"JRNY_LOC_1_ZONE_TABLE_178.0.GEO_LOC_AREA": {
                                                    "$eq": origin_area}},
                                                {"JRNY_LOC_1_ZONE_TABLE_178.0.GEO_LOC_ZONE": {
                                                    "$eq": origin_zone}},
                                                {"JRNY_LOC_1_ZONE_TABLE_178.0.GEO_LOC_COUNTRY": {
                                                    "$eq": origin_country}},
                                                {'$and': [
                                                    {"JRNY_LOC_1_ZONE_TABLE_178.0.GEO_LOC_AREA": {'$size': 0}},
                                                    {"JRNY_LOC_1_ZONE_TABLE_178.0.GEO_LOC_CITY": {'$size': 0}},
                                                    {"JRNY_LOC_1_ZONE_TABLE_178.0.GEO_LOC_ZONE": {'$size': 0}},
                                                    {"JRNY_LOC_1_ZONE_TABLE_178.0.GEO_LOC_COUNTRY": {'$size': 0}}

                                                ]}

                                            ]}]}]},
                               {"$or": [{'JRNY_LOC_2_ZONE_TABLE_178': None},
                                        {'$and': [{"JRNY_LOC_2_ZONE_TABLE_178.1.GEO_LOC_AREA": {
                                            "$ne": destination_area}},
                                            {"JRNY_LOC_2_ZONE_TABLE_178.1.GEO_LOC_ZONE": {
                                                "$ne": destination_zone}},
                                            {"JRNY_LOC_2_ZONE_TABLE_178.1.GEO_LOC_CITY": {
                                                "$ne": destination}},
                                            {"JRNY_LOC_2_ZONE_TABLE_178.1.GEO_LOC_COUNTRY": {
                                                "$ne": destination_country}},
                                            {"$or": [{"JRNY_LOC_2_ZONE_TABLE_178.0.GEO_LOC_CITY": {
                                                "$eq": destination}},
                                                {"JRNY_LOC_2_ZONE_TABLE_178.0.GEO_LOC_AREA": {
                                                    "$eq": destination_area}},
                                                {"JRNY_LOC_2_ZONE_TABLE_178.0.GEO_LOC_ZONE": {
                                                    "$eq": destination_zone}},
                                                {"JRNY_LOC_2_ZONE_TABLE_178.0.GEO_LOC_COUNTRY": {
                                                    "$eq": destination_country}},
                                                {'$and': [{"JRNY_LOC_2_ZONE_TABLE_178.0.GEO_LOC_AREA": {
                                                    '$size': 0}}, {
                                                    "JRNY_LOC_2_ZONE_TABLE_178.0.GEO_LOC_CITY": {
                                                        '$size': 0}},
                                                    {"JRNY_LOC_2_ZONE_TABLE_178.0.GEO_LOC_ZONE": {
                                                        '$size': 0}}, {
                                                        "JRNY_LOC_2_ZONE_TABLE_178.0.GEO_LOC_COUNTRY": {
                                                            '$size': 0}}

                                                ]}

                                            ]}]}]},

                               {"$or": [{'SECT_PRT_LOC_1_ZONE_TABLE_178': None},
                                        {'$and': [{"SECT_PRT_LOC_1_ZONE_TABLE_178.1.GEO_LOC_CITY": {
                                            "$ne": origin}},
                                            {
                                                "SECT_PRT_LOC_1_ZONE_TABLE_178.1.GEO_LOC_COUNTRY": {
                                                    "$ne": origin_country}},
                                            {"SECT_PRT_LOC_1_ZONE_TABLE_178.1.GEO_LOC_AREA": {
                                                "$ne": origin_area}},
                                            {"SECT_PRT_LOC_1_ZONE_TABLE_178.1.GEO_LOC_ZONE": {
                                                "$ne": origin_zone}},
                                            {"$or": [{
                                                "SECT_PRT_LOC_1_ZONE_TABLE_178.0.GEO_LOC_CITY": {
                                                    "$eq": origin}},
                                                {
                                                    "SECT_PRT_LOC_1_ZONE_TABLE_178.0.GEO_LOC_AREA": {
                                                        "$eq": origin_area}},
                                                {
                                                    "SECT_PRT_LOC_1_ZONE_TABLE_178.0.GEO_LOC_ZONE": {
                                                        "$eq": origin_zone}},
                                                {
                                                    "SECT_PRT_LOC_1_ZONE_TABLE_178.0.GEO_LOC_COUNTRY": {
                                                        "$eq": origin_country}},
                                                {'$and': [{
                                                    "SECT_PRT_LOC_1_ZONE_TABLE_178.0.GEO_LOC_AREA": {
                                                        '$size': 0}}, {
                                                    "SECT_PRT_LOC_1_ZONE_TABLE_178.0.GEO_LOC_CITY": {
                                                        '$size': 0}},
                                                    {
                                                        "SECT_PRT_LOC_1_ZONE_TABLE_178.0.GEO_LOC_ZONE": {
                                                            '$size': 0}}, {
                                                        "SECT_PRT_LOC_1_ZONE_TABLE_178.0.GEO_LOC_COUNTRY": {
                                                            '$size': 0}}

                                                ]}

                                            ]}]}]},
                               {"$or": [{'SECT_PRT_LOC_2_ZONE_TABLE_178': None},
                                        {'$and': [{"SECT_PRT_LOC_2_ZONE_TABLE_178.1.GEO_LOC_CITY": {
                                            "$ne": hub}},
                                            {
                                                "SECT_PRT_LOC_2_ZONE_TABLE_178.1.GEO_LOC_COUNTRY": {
                                                    "$ne": hub_country}},
                                            {"SECT_PRT_LOC_2_ZONE_TABLE_178.1.GEO_LOC_AREA": {
                                                "$ne": hub_area}},
                                            {"SECT_PRT_LOC_2_ZONE_TABLE_178.1.GEO_LOC_ZONE": {
                                                "$ne": hub_zone}},
                                            {"$or": [{
                                                "SECT_PRT_LOC_2_ZONE_TABLE_178.0.GEO_LOC_CITY": {
                                                    "$eq": hub}},
                                                {
                                                    "SECT_PRT_LOC_2_ZONE_TABLE_178.0.GEO_LOC_AREA": {
                                                        "$eq": hub_area}},
                                                {
                                                    "SECT_PRT_LOC_2_ZONE_TABLE_178.0.GEO_LOC_ZONE": {
                                                        "$eq": hub_zone}},
                                                {
                                                    "SECT_PRT_LOC_2_ZONE_TABLE_178.0.GEO_LOC_COUNTRY": {
                                                        "$eq": hub_country}},
                                                {'$and': [{
                                                    "SECT_PRT_LOC_2_ZONE_TABLE_178.0.GEO_LOC_AREA": {
                                                        '$size': 0}}, {
                                                    "SECT_PRT_LOC_2_ZONE_TABLE_178.0.GEO_LOC_CITY": {
                                                        '$size': 0}},
                                                    {
                                                        "SECT_PRT_LOC_2_ZONE_TABLE_178.0.GEO_LOC_ZONE": {
                                                            '$size': 0}}, {
                                                        "SECT_PRT_LOC_2_ZONE_TABLE_178.0.GEO_LOC_COUNTRY": {
                                                            '$size': 0}}

                                                ]}

                                            ]}]}]}

                               ]}, no_cursor_timeout=True).sort("SEQ_NO", pymongo.ASCENDING)
    # print cur2.count()
    ######PUT EXCEPTION FOR EMPTY daTaFRaME
    df_stage1 = pd.DataFrame(list(cur2))

    # print list(df_stage1)
    return df_stage1

def stage1_hub2(cxr,origin, origin_area, origin_zone, origin_country, destination, destination_area,
             destination_zone, destination_country,system_date, hub, hub_area, hub_zone, hub_country):
    print "============HUB2 stage1============="

    cur3 = yqyr.find({"$and": [{"flag": 1}, {"CXR_CODE": cxr}, {"DISC_DATE": {"$gte": system_date}},
                               {"EFF_DATE": {"$lt": system_date}},
                               {"$or": [{"TICKET_DATES_FIRST": "999999"},
                                        {"TICKET_DATES_FIRST": {"$lt": system_date}}]},
                               {"TICKET_DATES_LAST": {"$gte": system_date}},

                               {"POS_AREA": {"$in": [origin_area, ""]}},
                               {"POS_ZONE": {"$in": [origin_zone, ""]}},
                               {"POS_CITY": {"$in": [origin, ""]}},
                               {"POS_CNTRY": {"$in": [origin_country, ""]}},
                               {"POT_AREA": {"$in": [origin_area, ""]}},
                               {"POT_CITY": {"$in": [origin, ""]}},
                               {"POT_CNTRY": {"$in": [origin_country, ""]}},

                               {"JRNY_LOC_1_AREA": {"$in": [origin_area, ""]}},
                               {"JRNY_LOC_1_CITY": {"$in": [origin, ""]}},
                               {"JRNY_LOC_1_CNTRY": {"$in": [origin_country, ""]}},
                               {"JRNY_LOC_2_AREA": {"$in": [destination_area, ""]}},
                               {"JRNY_LOC_2_CITY": {"$in": [destination, ""]}},
                               {"JRNY_LOC_2_CNTRY": {"$in": [destination_country, ""]}},
                               {"SECT_PRT_LOC_1_AREA": {"$in": [hub_area, ""]}},
                               {"SECT_PRT_LOC_1_CITY": {"$in": [hub, ""]}},
                               {"SECT_PRT_LOC_1_CNTRY": {"$in": [hub_country, ""]}},
                               {"SECT_PRT_LOC_2_AREA": {"$in": [destination_area, ""]}},
                               {"SECT_PRT_LOC_2_CITY": {"$in": [destination, ""]}},
                               {"SECT_PRT_LOC_2_CNTRY": {"$in": [destination_country, ""]}},
                               {"POT_ZONE": {"$in": [origin_zone, ""]}},
                               {"JRNY_LOC_1_ZONE": {"$in": [origin_zone, ""]}},
                               {"JRNY_LOC_2_ZONE": {"$in": [destination_zone, ""]}},
                               {"SECT_PRT_LOC_1_ZONE": {"$in": [hub_zone, ""]}},
                               {"SECT_PRT_LOC_2_ZONE": {"$in": [destination_zone, ""]}},

                               {"$or": [{'POS_ZONE_178': None},
                                        {'$and': [{"POS_ZONE_178.1.GEO_LOC_AREA": {"$ne": origin_area}},
                                                  {"POS_ZONE_178.1.GEO_LOC_ZONE": {"$ne": origin_zone}},
                                                  {"POS_ZONE_178.1.GEO_LOC_CITY": {"$ne": origin}},
                                                  {"POS_ZONE_178.1.GEO_LOC_COUNTRY": {"$ne": origin_country}},
                                                  {"$or": [{"POS_ZONE_178.0.GEO_LOC_CITY": {"$eq": origin}},
                                                           {"POS_ZONE_178.0.GEO_LOC_AREA": {
                                                               "$eq": origin_area}},
                                                           {"POS_ZONE_178.0.GEO_LOC_ZONE": {
                                                               "$eq": origin_zone}}, {
                                                               "POS_ZONE_178.0.GEO_LOC_COUNTRY": {
                                                                   "$eq": origin_country}},
                                                           {'$and': [
                                                               {"POS_ZONE_178.0.GEO_LOC_AREA": {'$size': 0}},
                                                               {"POS_ZONE_178.0.GEO_LOC_ZONE": {'$size': 0}},
                                                               {"POS_ZONE_178.0.GEO_LOC_COUNTRY": {'$size': 0}},
                                                               {"POS_ZONE_178.0.GEO_LOC_CITY": {'$size': 0}}]}
                                                           ]}]}]},

                               {"$or": [{'POT_ZONE_178': None},
                                        {'$and': [{"POT_ZONE_178.1.GEO_LOC_AREA": {"$ne": origin_area}},
                                                  {"POT_ZONE_178.1.GEO_LOC_ZONE": {"$ne": origin_zone}},
                                                  {"POT_ZONE_178.1.GEO_LOC_CITY": {"$ne": origin}},
                                                  {"POT_ZONE_178.1.GEO_LOC_COUNTRY": {"$ne": origin_country}},
                                                  {"$or": [{"POT_ZONE_178.0.GEO_LOC_CITY": {"$eq": origin}},
                                                           {"POT_ZONE_178.0.GEO_LOC_AREA": {
                                                               "$eq": origin_area}},
                                                           {"POT_ZONE_178.0.GEO_LOC_ZONE": {
                                                               "$eq": origin_zone}}, {
                                                               "POT_ZONE_178.0.GEO_LOC_COUNTRY": {
                                                                   "$eq": origin_country}},
                                                           {'$and': [
                                                               {"POT_ZONE_178.0.GEO_LOC_AREA": {'$size': 0}},
                                                               {"POT_ZONE_178.0.GEO_LOC_ZONE": {'$size': 0}},
                                                               {"POT_ZONE_178.0.GEO_LOC_COUNTRY": {'$size': 0}},
                                                               {"POT_ZONE_178.0.GEO_LOC_CITY": {'$size': 0}}]}
                                                           ]}]}]},
                               {"$or": [{'CARRIER_TABLE_190': None},
                                        {'$and': [{"$or": [{"CARRIER_TABLE_190.0.CARRIER": {'$size': 0}}, {
                                            "CARRIER_TABLE_190.0.CARRIER": {"$in": [cxr, "$$"]}}]},
                                                  {"$or": [{"CARRIER_TABLE_190.1.CARRIER": {'$size': 0}},
                                                           {"CARRIER_TABLE_190.1.CARRIER": {"$ne": cxr}}]}]
                                         }]},

                               {"$or": [{'JRNY_LOC_1_ZONE_TABLE_178': None},
                                        {'$and': [
                                            {"JRNY_LOC_1_ZONE_TABLE_178.1.GEO_LOC_AREA": {"$ne": origin_area}},
                                            {"JRNY_LOC_1_ZONE_TABLE_178.1.GEO_LOC_ZONE": {"$ne": origin_zone}},
                                            {"JRNY_LOC_1_ZONE_TABLE_178.1.GEO_LOC_CITY": {"$ne": origin}},
                                            {"JRNY_LOC_1_ZONE_TABLE_178.1.GEO_LOC_COUNTRY": {
                                                "$ne": origin_country}},
                                            {"$or": [
                                                {"JRNY_LOC_1_ZONE_TABLE_178.0.GEO_LOC_CITY": {"$eq": origin}},
                                                {"JRNY_LOC_1_ZONE_TABLE_178.0.GEO_LOC_AREA": {
                                                    "$eq": origin_area}},
                                                {"JRNY_LOC_1_ZONE_TABLE_178.0.GEO_LOC_ZONE": {
                                                    "$eq": origin_zone}},
                                                {"JRNY_LOC_1_ZONE_TABLE_178.0.GEO_LOC_COUNTRY": {
                                                    "$eq": origin_country}},
                                                {'$and': [
                                                    {"JRNY_LOC_1_ZONE_TABLE_178.0.GEO_LOC_AREA": {'$size': 0}},
                                                    {"JRNY_LOC_1_ZONE_TABLE_178.0.GEO_LOC_CITY": {'$size': 0}},
                                                    {"JRNY_LOC_1_ZONE_TABLE_178.0.GEO_LOC_ZONE": {'$size': 0}},
                                                    {"JRNY_LOC_1_ZONE_TABLE_178.0.GEO_LOC_COUNTRY": {
                                                        '$size': 0}}

                                                ]}

                                            ]}]}]},
                               {"$or": [{'JRNY_LOC_2_ZONE_TABLE_178': None},
                                        {'$and': [{"JRNY_LOC_2_ZONE_TABLE_178.1.GEO_LOC_AREA": {
                                            "$ne": destination_area}},
                                            {"JRNY_LOC_2_ZONE_TABLE_178.1.GEO_LOC_ZONE": {
                                                "$ne": destination_zone}},
                                            {"JRNY_LOC_2_ZONE_TABLE_178.1.GEO_LOC_CITY": {
                                                "$ne": destination}},
                                            {"JRNY_LOC_2_ZONE_TABLE_178.1.GEO_LOC_COUNTRY": {
                                                "$ne": destination_country}},
                                            {"$or": [{"JRNY_LOC_2_ZONE_TABLE_178.0.GEO_LOC_CITY": {
                                                "$eq": destination}},
                                                {"JRNY_LOC_2_ZONE_TABLE_178.0.GEO_LOC_AREA": {
                                                    "$eq": destination_area}},
                                                {"JRNY_LOC_2_ZONE_TABLE_178.0.GEO_LOC_ZONE": {
                                                    "$eq": destination_zone}},
                                                {"JRNY_LOC_2_ZONE_TABLE_178.0.GEO_LOC_COUNTRY": {
                                                    "$eq": destination_country}},
                                                {'$and': [{"JRNY_LOC_2_ZONE_TABLE_178.0.GEO_LOC_AREA": {
                                                    '$size': 0}}, {
                                                    "JRNY_LOC_2_ZONE_TABLE_178.0.GEO_LOC_CITY": {
                                                        '$size': 0}},
                                                    {"JRNY_LOC_2_ZONE_TABLE_178.0.GEO_LOC_ZONE": {
                                                        '$size': 0}}, {
                                                        "JRNY_LOC_2_ZONE_TABLE_178.0.GEO_LOC_COUNTRY": {
                                                            '$size': 0}}

                                                ]}

                                            ]}]}]},

                               {"$or": [{'SECT_PRT_LOC_1_ZONE_TABLE_178': None},
                                        {'$and': [{"SECT_PRT_LOC_1_ZONE_TABLE_178.1.GEO_LOC_CITY": {
                                            "$ne": hub}},
                                            {
                                                "SECT_PRT_LOC_1_ZONE_TABLE_178.1.GEO_LOC_COUNTRY": {
                                                    "$ne": hub_country}},
                                            {"SECT_PRT_LOC_1_ZONE_TABLE_178.1.GEO_LOC_AREA": {
                                                "$ne": hub_area}},
                                            {"SECT_PRT_LOC_1_ZONE_TABLE_178.1.GEO_LOC_ZONE": {
                                                "$ne": hub_zone}},
                                            {"$or": [{
                                                "SECT_PRT_LOC_1_ZONE_TABLE_178.0.GEO_LOC_CITY": {
                                                    "$eq": hub}},
                                                {
                                                    "SECT_PRT_LOC_1_ZONE_TABLE_178.0.GEO_LOC_AREA": {
                                                        "$eq": hub_area}},
                                                {
                                                    "SECT_PRT_LOC_1_ZONE_TABLE_178.0.GEO_LOC_ZONE": {
                                                        "$eq": hub_zone}},
                                                {
                                                    "SECT_PRT_LOC_1_ZONE_TABLE_178.0.GEO_LOC_COUNTRY": {
                                                        "$eq": hub_country}},
                                                {'$and': [{
                                                    "SECT_PRT_LOC_1_ZONE_TABLE_178.0.GEO_LOC_AREA": {
                                                        '$size': 0}}, {
                                                    "SECT_PRT_LOC_1_ZONE_TABLE_178.0.GEO_LOC_CITY": {
                                                        '$size': 0}},
                                                    {
                                                        "SECT_PRT_LOC_1_ZONE_TABLE_178.0.GEO_LOC_ZONE": {
                                                            '$size': 0}}, {
                                                        "SECT_PRT_LOC_1_ZONE_TABLE_178.0.GEO_LOC_COUNTRY": {
                                                            '$size': 0}}

                                                ]}

                                            ]}]}]},
                               {"$or": [{'SECT_PRT_LOC_2_ZONE_TABLE_178': None},
                                        {'$and': [{"SECT_PRT_LOC_2_ZONE_TABLE_178.1.GEO_LOC_CITY": {
                                            "$ne": destination}},
                                            {
                                                "SECT_PRT_LOC_2_ZONE_TABLE_178.1.GEO_LOC_COUNTRY": {
                                                    "$ne": destination_country}},
                                            {"SECT_PRT_LOC_2_ZONE_TABLE_178.1.GEO_LOC_AREA": {
                                                "$ne": destination_area}},
                                            {"SECT_PRT_LOC_2_ZONE_TABLE_178.1.GEO_LOC_ZONE": {
                                                "$ne": destination_zone}},
                                            {"$or": [{
                                                "SECT_PRT_LOC_2_ZONE_TABLE_178.0.GEO_LOC_CITY": {
                                                    "$eq": destination}},
                                                {
                                                    "SECT_PRT_LOC_2_ZONE_TABLE_178.0.GEO_LOC_AREA": {
                                                        "$eq": destination_area}},
                                                {
                                                    "SECT_PRT_LOC_2_ZONE_TABLE_178.0.GEO_LOC_ZONE": {
                                                        "$eq": destination_zone}},
                                                {
                                                    "SECT_PRT_LOC_2_ZONE_TABLE_178.0.GEO_LOC_COUNTRY": {
                                                        "$eq": destination_country}},
                                                {'$and': [{
                                                    "SECT_PRT_LOC_2_ZONE_TABLE_178.0.GEO_LOC_AREA": {
                                                        '$size': 0}}, {
                                                    "SECT_PRT_LOC_2_ZONE_TABLE_178.0.GEO_LOC_CITY": {
                                                        '$size': 0}},
                                                    {
                                                        "SECT_PRT_LOC_2_ZONE_TABLE_178.0.GEO_LOC_ZONE": {
                                                            '$size': 0}}, {
                                                        "SECT_PRT_LOC_2_ZONE_TABLE_178.0.GEO_LOC_COUNTRY": {
                                                            '$size': 0}}

                                                ]}

                                            ]}]}]}

                               ]}, no_cursor_timeout=True).sort("SEQ_NO", pymongo.ASCENDING)

    # print cur2.count()
    ######PUT EXCEPTION FOR EMPTY daTaFRaME
    df_stage1 = pd.DataFrame(list(cur3))

    # print list(df_stage1)
    return df_stage1


def stage1_p2p(cxr,origin,origin_area,origin_zone,origin_country,destination,destination_area,destination_zone,destination_country,system_date):

    print "============P2P stage 1============="


    cur2 = yqyr.find({"$and": [ {"flag":1},{"CXR_CODE": cxr}, {"DISC_DATE": {"$gte": system_date}},
                               {"EFF_DATE": {"$lt": system_date}},
                               {"$or": [{"TICKET_DATES_FIRST": "999999"},
                                        {"TICKET_DATES_FIRST": {"$lt": system_date}}]},
                               {"TICKET_DATES_LAST": {"$gte": system_date}},

                               {"POS_AREA": {"$in": [origin_area, ""]}},
                               {"POS_ZONE": {"$in": [origin_zone, ""]}},
                               {"POS_CITY": {"$in": [origin, ""]}},
                               {"POS_CNTRY": {"$in": [origin_country, ""]}},
                               {"POT_AREA": {"$in": [origin_area, ""]}},
                               {"POT_CITY": {"$in": [origin, ""]}},
                               {"POT_CNTRY": {"$in": [origin_country, ""]}},

                               {"JRNY_LOC_1_AREA": {"$in": [origin_area, ""]}},
                               {"JRNY_LOC_1_CITY": {"$in": [origin, ""]}},
                               {"JRNY_LOC_1_CNTRY": {"$in": [origin_country, ""]}},
                               {"JRNY_LOC_2_AREA": {"$in": [destination_area, ""]}},
                               {"JRNY_LOC_2_CITY": {"$in": [destination, ""]}},
                               {"JRNY_LOC_2_CNTRY": {"$in": [destination_country, ""]}},
                               {"SECT_PRT_LOC_1_AREA": {"$in": [origin_area, ""]}},
                               {"SECT_PRT_LOC_1_CITY": {"$in": [origin, ""]}},
                               {"SECT_PRT_LOC_1_CNTRY": {"$in": [origin_country, ""]}},
                               {"SECT_PRT_LOC_2_AREA": {"$in": [destination_area, ""]}},
                               {"SECT_PRT_LOC_2_CITY": {"$in": [destination, ""]}},
                               {"SECT_PRT_LOC_2_CNTRY": {"$in": [destination_country, ""]}},
                               {"POT_ZONE": {"$in": [origin_zone, ""]}},
                               {"JRNY_LOC_1_ZONE": {"$in": [origin_zone, ""]}},
                               {"JRNY_LOC_2_ZONE": {"$in": [destination_zone, ""]}},
                               {"SECT_PRT_LOC_1_ZONE": {"$in": [origin_zone, ""]}},
                               {"SECT_PRT_LOC_2_ZONE": {"$in": [destination_zone, ""]}},

                               {"$or": [{'POS_ZONE_178': None},
                                        {'$and': [{"POS_ZONE_178.1.GEO_LOC_AREA": {"$ne": origin_area}},
                                                  {"POS_ZONE_178.1.GEO_LOC_ZONE": {"$ne": origin_zone}},
                                                  {"POS_ZONE_178.1.GEO_LOC_CITY": {"$ne": origin}},
                                                  {"POS_ZONE_178.1.GEO_LOC_COUNTRY": {"$ne": origin_country}},
                                                  {"$or": [{"POS_ZONE_178.0.GEO_LOC_CITY": {"$eq": origin}},
                                                           {"POS_ZONE_178.0.GEO_LOC_AREA": {"$eq": origin_area}},
                                                           {"POS_ZONE_178.0.GEO_LOC_ZONE": {"$eq": origin_zone}}, {
                                                               "POS_ZONE_178.0.GEO_LOC_COUNTRY": {
                                                                   "$eq": origin_country}},
                                                           {'$and': [{"POS_ZONE_178.0.GEO_LOC_AREA": {'$size': 0}},
                                                                     {"POS_ZONE_178.0.GEO_LOC_ZONE": {'$size': 0}},
                                                                     {"POS_ZONE_178.0.GEO_LOC_COUNTRY": {'$size': 0}},
                                                                     {"POS_ZONE_178.0.GEO_LOC_CITY": {'$size': 0}}]}
                                                           ]}]}]},


                               {"$or": [{'POT_ZONE_178': None},
                                        {'$and': [{"POT_ZONE_178.1.GEO_LOC_AREA": {"$ne": origin_area}},
                                                  {"POT_ZONE_178.1.GEO_LOC_ZONE": {"$ne": origin_zone}},
                                                  {"POT_ZONE_178.1.GEO_LOC_CITY": {"$ne": origin}},
                                                  {"POT_ZONE_178.1.GEO_LOC_COUNTRY": {"$ne": origin_country}},
                                                  {"$or": [{"POT_ZONE_178.0.GEO_LOC_CITY": {"$eq": origin}},
                                                           {"POT_ZONE_178.0.GEO_LOC_AREA": {"$eq": origin_area}},
                                                           {"POT_ZONE_178.0.GEO_LOC_ZONE": {"$eq": origin_zone}}, {
                                                               "POT_ZONE_178.0.GEO_LOC_COUNTRY": {
                                                                   "$eq": origin_country}},
                                                           {'$and': [{"POT_ZONE_178.0.GEO_LOC_AREA": {'$size': 0}},
                                                                     {"POT_ZONE_178.0.GEO_LOC_ZONE": {'$size': 0}},
                                                                     {"POT_ZONE_178.0.GEO_LOC_COUNTRY": {'$size': 0}},
                                                                     {"POT_ZONE_178.0.GEO_LOC_CITY": {'$size': 0}}]}
                                                           ]}]}]},
                               {"$or": [{'CARRIER_TABLE_190': None},
                                        {'$and': [{"$or": [{"CARRIER_TABLE_190.0.CARRIER": {'$size': 0}}, {
                                            "CARRIER_TABLE_190.0.CARRIER": {"$in": [cxr, "$$"]}}]},
                                                  {"$or": [{"CARRIER_TABLE_190.1.CARRIER": {'$size': 0}},
                                                           {"CARRIER_TABLE_190.1.CARRIER": {"$ne": cxr}}]}]
                                         }]},




                            {"$or":[{'JRNY_LOC_1_ZONE_TABLE_178':None},
                                   {'$and':[{"JRNY_LOC_1_ZONE_TABLE_178.1.GEO_LOC_AREA": {"$ne": origin_area}},
                                            {"JRNY_LOC_1_ZONE_TABLE_178.1.GEO_LOC_ZONE": {"$ne": origin_zone}},
                                            {"JRNY_LOC_1_ZONE_TABLE_178.1.GEO_LOC_CITY": {"$ne": origin}},
                                            {"JRNY_LOC_1_ZONE_TABLE_178.1.GEO_LOC_COUNTRY": {"$ne": origin_country}},
                                    {"$or":[{"JRNY_LOC_1_ZONE_TABLE_178.0.GEO_LOC_CITY": {"$eq":origin}},
                                        {"JRNY_LOC_1_ZONE_TABLE_178.0.GEO_LOC_AREA": {"$eq": origin_area}},
                                        {"JRNY_LOC_1_ZONE_TABLE_178.0.GEO_LOC_ZONE": {"$eq": origin_zone}},
                                        {"JRNY_LOC_1_ZONE_TABLE_178.0.GEO_LOC_COUNTRY": {"$eq": origin_country}},
                                        {'$and':[{"JRNY_LOC_1_ZONE_TABLE_178.0.GEO_LOC_AREA":{'$size':0}},{"JRNY_LOC_1_ZONE_TABLE_178.0.GEO_LOC_CITY":{'$size':0}},
                                                 {"JRNY_LOC_1_ZONE_TABLE_178.0.GEO_LOC_ZONE":{'$size':0}},{"JRNY_LOC_1_ZONE_TABLE_178.0.GEO_LOC_COUNTRY":{'$size':0}}

                                                ]}

                                           ]}]}]},
                            {"$or":[{'JRNY_LOC_2_ZONE_TABLE_178':None},
                                   {'$and':[{"JRNY_LOC_2_ZONE_TABLE_178.1.GEO_LOC_AREA": {"$ne": destination_area}},
                                            {"JRNY_LOC_2_ZONE_TABLE_178.1.GEO_LOC_ZONE": {"$ne": destination_zone}},
                                            {"JRNY_LOC_2_ZONE_TABLE_178.1.GEO_LOC_CITY": {"$ne": destination}},
                                            {"JRNY_LOC_2_ZONE_TABLE_178.1.GEO_LOC_COUNTRY": {"$ne":destination_country}},
                                    {"$or":[{"JRNY_LOC_2_ZONE_TABLE_178.0.GEO_LOC_CITY": {"$eq": destination}},
                                        {"JRNY_LOC_2_ZONE_TABLE_178.0.GEO_LOC_AREA": {"$eq": destination_area}},
                                        {"JRNY_LOC_2_ZONE_TABLE_178.0.GEO_LOC_ZONE": {"$eq": destination_zone}},
                                        {"JRNY_LOC_2_ZONE_TABLE_178.0.GEO_LOC_COUNTRY": {"$eq": destination_country}},
                                        {'$and':[{"JRNY_LOC_2_ZONE_TABLE_178.0.GEO_LOC_AREA":{'$size':0}},{"JRNY_LOC_2_ZONE_TABLE_178.0.GEO_LOC_CITY":{'$size':0}},
                                                 {"JRNY_LOC_2_ZONE_TABLE_178.0.GEO_LOC_ZONE":{'$size':0}},{"JRNY_LOC_2_ZONE_TABLE_178.0.GEO_LOC_COUNTRY":{'$size':0}}

                                                ]}

                                           ]}]}]},


                                    {"$or": [{'SECT_PRT_LOC_1_ZONE_TABLE_178': None},
                                                           {'$and': [{"SECT_PRT_LOC_1_ZONE_TABLE_178.1.GEO_LOC_CITY": {
                                                               "$ne": origin}},
                                                                     {
                                                                         "SECT_PRT_LOC_1_ZONE_TABLE_178.1.GEO_LOC_COUNTRY": {
                                                                             "$ne": origin_country}},
                                                                     {"SECT_PRT_LOC_1_ZONE_TABLE_178.1.GEO_LOC_AREA": {
                                                                         "$ne": origin_area}},
                                                                     {"SECT_PRT_LOC_1_ZONE_TABLE_178.1.GEO_LOC_ZONE": {
                                                                         "$ne": origin_zone}},
                                                                     {"$or": [{
                                                                                  "SECT_PRT_LOC_1_ZONE_TABLE_178.0.GEO_LOC_CITY": {
                                                                                      "$eq": origin}},
                                                                              {
                                                                                  "SECT_PRT_LOC_1_ZONE_TABLE_178.0.GEO_LOC_AREA": {
                                                                                      "$eq": origin_area}},
                                                                              {
                                                                                  "SECT_PRT_LOC_1_ZONE_TABLE_178.0.GEO_LOC_ZONE": {
                                                                                      "$eq": origin_zone}},
                                                                              {
                                                                                  "SECT_PRT_LOC_1_ZONE_TABLE_178.0.GEO_LOC_COUNTRY": {
                                                                                      "$eq": origin_country}},
                                                                              {'$and': [{
                                                                                            "SECT_PRT_LOC_1_ZONE_TABLE_178.0.GEO_LOC_AREA": {
                                                                                                '$size': 0}}, {
                                                                                            "SECT_PRT_LOC_1_ZONE_TABLE_178.0.GEO_LOC_CITY": {
                                                                                                '$size': 0}},
                                                                                        {
                                                                                            "SECT_PRT_LOC_1_ZONE_TABLE_178.0.GEO_LOC_ZONE": {
                                                                                                '$size': 0}}, {
                                                                                            "SECT_PRT_LOC_1_ZONE_TABLE_178.0.GEO_LOC_COUNTRY": {
                                                                                                '$size': 0}}

                                                                                        ]}

                                                                              ]}]}]},
                                    {"$or": [{'SECT_PRT_LOC_2_ZONE_TABLE_178': None},
                                                           {'$and': [{"SECT_PRT_LOC_2_ZONE_TABLE_178.1.GEO_LOC_CITY": {
                                                               "$ne": destination}},
                                                                     {
                                                                         "SECT_PRT_LOC_2_ZONE_TABLE_178.1.GEO_LOC_COUNTRY": {
                                                                             "$ne": destination_country}},
                                                                     {"SECT_PRT_LOC_2_ZONE_TABLE_178.1.GEO_LOC_AREA": {
                                                                         "$ne": destination_area}},
                                                                     {"SECT_PRT_LOC_2_ZONE_TABLE_178.1.GEO_LOC_ZONE": {
                                                                         "$ne": destination_zone}},
                                                                     {"$or": [{
                                                                                  "SECT_PRT_LOC_2_ZONE_TABLE_178.0.GEO_LOC_CITY": {
                                                                                      "$eq": destination}},
                                                                              {
                                                                                  "SECT_PRT_LOC_2_ZONE_TABLE_178.0.GEO_LOC_AREA": {
                                                                                      "$eq": destination_area}},
                                                                              {
                                                                                  "SECT_PRT_LOC_2_ZONE_TABLE_178.0.GEO_LOC_ZONE": {
                                                                                      "$eq": destination_zone}},
                                                                              {
                                                                                  "SECT_PRT_LOC_2_ZONE_TABLE_178.0.GEO_LOC_COUNTRY": {
                                                                                      "$eq": destination_country}},
                                                                              {'$and': [{
                                                                                            "SECT_PRT_LOC_2_ZONE_TABLE_178.0.GEO_LOC_AREA": {
                                                                                                '$size': 0}}, {
                                                                                            "SECT_PRT_LOC_2_ZONE_TABLE_178.0.GEO_LOC_CITY": {
                                                                                                '$size': 0}},
                                                                                        {
                                                                                            "SECT_PRT_LOC_2_ZONE_TABLE_178.0.GEO_LOC_ZONE": {
                                                                                                '$size': 0}}, {
                                                                                            "SECT_PRT_LOC_2_ZONE_TABLE_178.0.GEO_LOC_COUNTRY": {
                                                                                                '$size': 0}}

                                                                                        ]}

                                                                              ]}]}]}


                               ]}, no_cursor_timeout=True).sort("SEQ_NO", pymongo.ASCENDING)
    # print cur2.count()


    ######PUT EXCEPTION FOR EMPTY daTaFRaME
    df_stage1 = pd.DataFrame(list(cur2))

    # print list(df_stage1)
    # print df_stage1[['RBD_1','RBD_2','RBD_3']]
    return df_stage1

def yqyr_hub(system_date, file_date,carrier_list, OD_list,hub,hub_area,hub_country,hub_zone,origin,origin_area,origin_country,origin_zone,destination,destination_area,destination_country,destination_zone):
    print "============HUB============="

    df_stage1_hub1 = stage1_hub1(carrier_list[0], origin, origin_area, origin_zone, origin_country, destination,
                           destination_area,destination_zone, destination_country, system_date, hub, hub_area, hub_zone, hub_country)

    df_stage1_hub2 = stage1_hub2(carrier_list[0], origin, origin_area, origin_zone, origin_country, destination,
                           destination_area,destination_zone, destination_country, system_date, hub, hub_area, hub_zone, hub_country)

    if df_stage1_hub1.empty or df_stage1_hub2.empty:
        print('Hub DataFrame is empty!')
        return

    cur = coll.find(
        {'carrier': {'$in': carrier_list}, 'OD': {'$in': OD_list},
         '$or': [{'effective_to': None}, {'effective_to': {'$gt': file_date}}]},
        no_cursor_timeout=True)
    # print cur.count()

    for i in cur:
        sta = time.time()

        seqno = []
        checklist = []
        yqi1 = 0
        yqf1 = 0
        yri1 = 0
        yrf1 = 0
        yqi2 = 0
        yqf2 = 0
        yri2 = 0
        yrf2 = 0
        yriseq = ""
        yrfseq = ""
        yqisfa1 = ""
        yqisfa2 = ""
        yqfsfa1 = ""
        yqfsfa2 = ""
        yrisfa1 = ""
        yrisfa2 = ""
        yrfsfa1 = ""
        yrfsfa2 = ""
        yrfseqno = []
        yqfseqno = []
        yriseqno = []
        yqiseqno = []

        if i["oneway_return"] == "1":
            owrt = "N"
        else:
            owrt = "Y"
        rbd = i['RBD']
        fare=i['fare']
        fare_basis=i['fare_basis']
        currency=i['currency']
        compartment=i['compartment']
        Reference_Rate=i['Reference_Rate']

        print (i["carrier"], i["RBD"], i["compartment"], i["origin"], i["origin_area"],
               i["origin_zone"], i["origin_country"],
               i["destination"], i["destination_area"], i["destination_zone"], i["destination_country"], system_date,
               i["fare_basis"], i["fare"], i["currency"], i["Reference_Rate"], owrt)

        df_stage2_hub1 = df_stage1_hub1.loc[(

            # (df_stage1_hub1.RBD_198.isna()| df_stage1_hub1.RBD_198 ==  rbd) &
            (
                (df_stage1_hub1.RBD_1 == rbd) | (df_stage1_hub1.RBD_2 == rbd) | (df_stage1_hub1.RBD_3 == rbd) | (
                    (df_stage1_hub1.RBD_1 == '') & (df_stage1_hub1.RBD_2 == '') & (df_stage1_hub1.RBD_3 == ''))) &
            (df_stage1_hub1.CABIN.isin([compartment, ''])) &
            (df_stage1_hub1.RTN_TO_ORIG.isin([owrt, ''])))]

        ###### create index on



        for index, j in df_stage2_hub1.iterrows():

            if j.get('RBD_198') :
                if isinstance(j['RBD_198'], list) and rbd not in j['RBD_198']:
                    continue

            if j['SERVICE_TYPE_TAX_CODE'] + j['SERVICE_TYPE_SUB_CODE'] in checklist:
                continue

            match = 0
            if j.get('FARE_CLASS_TBL_171') and isinstance(j['FARE_CLASS_TBL_171'], dict):
                # print type(j.get('FARE_CLASS_TBL_171'))

                for l in range(0, len(j['FARE_CLASS_TBL_171']['FARE_CLASS_CODE'])):
                    if (j['FARE_CLASS_TBL_171']['FARE_CLASS_CODE'][l] == "" or wild_card_check(i['fare_basis'],
                                                                                               j['FARE_CLASS_TBL_171'][
                                                                                                   'FARE_CLASS_CODE'][
                                                                                                   l])):
                        match += 1
                        # print j['FARE_CLASS_TBL_171']['FARE_CLASS_CODE'][l] + " matched!!"
                        break
                    else:
                        continue



            elif (j["FARE_BASIS_CODE"] == "" or wild_card_check(i['fare_basis'], j["FARE_BASIS_CODE"])):

                match += 1
            else:
                continue

            if match < 1:
                continue

            if j["SERVICE_FEE_PERCENT"] > 0.0:
                j["SERVICE_FEE_AMOUNT"] = (j["SERVICE_FEE_PERCENT"] * fare) / 100


            elif j['SERVICE_FEE_CUR'] != currency:

                y_cur = j['SERVICE_FEE_AMOUNT']/j["Reference_Rate"]
                f_cur = Reference_Rate

                j["SERVICE_FEE_AMOUNT"] = y_cur / f_cur
            else:
                pass

            if (j['SERVICE_TYPE_TAX_CODE'] == "YQ" and j['SERVICE_TYPE_SUB_CODE'] == "I"):
                yqi1 = j["SERVICE_FEE_AMOUNT"]
                checklist.append("YQI")
                yqisfa1 = j['SERVICE_FEE_APPL']
                yqiseqno.append(j['SEQ_NO'])


            elif (j['SERVICE_TYPE_TAX_CODE'] == "YQ" and j['SERVICE_TYPE_SUB_CODE'] == "F"):
                yqfsfa1 = j['SERVICE_FEE_APPL']
                yqf1 = j["SERVICE_FEE_AMOUNT"]
                checklist.append("YQF")
                yqfseqno.append(j['SEQ_NO'])
            elif (j['SERVICE_TYPE_TAX_CODE'] == "YR" and j['SERVICE_TYPE_SUB_CODE'] == "I"):
                yrisfa1 = j['SERVICE_FEE_APPL']
                yri1 = j["SERVICE_FEE_AMOUNT"]
                checklist.append("YRI")
                yriseqno.append(j['SEQ_NO'])

            elif (j['SERVICE_TYPE_TAX_CODE'] == "YR" and j['SERVICE_TYPE_SUB_CODE'] == "F"):
                yrfsfa1 = j['SERVICE_FEE_APPL']
                yrf1 = j["SERVICE_FEE_AMOUNT"]
                checklist.append("YRF")
                yrfseqno.append(j['SEQ_NO'])

        checklist=[]
        df_stage2_hub2 = df_stage1_hub2.loc[(

        # (df_stage1_hub2.RBD_198.isna()| df_stage1_hub2.RBD_198 ==  rbd) &
        (
            (df_stage1_hub2.RBD_1 == rbd) | (df_stage1_hub2.RBD_2 == rbd) | (df_stage1_hub2.RBD_3 == rbd) | (
                (df_stage1_hub2.RBD_1 == '') & (df_stage1_hub2.RBD_2 == '') & (df_stage1_hub2.RBD_3 == ''))) &
        (df_stage1_hub2.CABIN.isin([compartment, ''])) &
        (df_stage1_hub2.RTN_TO_ORIG.isin([owrt, ''])))]

        ###### create index on df_stage2


        for index, j in df_stage2_hub2.iterrows():
            # print j['SEQ_NO']
            if j.get('RBD_198') :
                if isinstance(j['RBD_198'], list) and rbd not in j['RBD_198']:
                    continue

            if j['SERVICE_TYPE_TAX_CODE'] + j['SERVICE_TYPE_SUB_CODE'] in checklist:
                continue

            match = 0
            if j.get('FARE_CLASS_TBL_171') and isinstance(j['FARE_CLASS_TBL_171'], dict):
                # print type(j.get('FARE_CLASS_TBL_171'))

                for l in range(0, len(j['FARE_CLASS_TBL_171']['FARE_CLASS_CODE'])):
                    if (j['FARE_CLASS_TBL_171']['FARE_CLASS_CODE'][l] == "" or wild_card_check(i['fare_basis'],
                                                                             j['FARE_CLASS_TBL_171']['FARE_CLASS_CODE'][l])):
                        match += 1
                        # print j['FARE_CLASS_TBL_171']['FARE_CLASS_CODE'][l] + " matched!!"
                        break
                    else:
                        continue



            elif (j["FARE_BASIS_CODE"] == "" or wild_card_check(i['fare_basis'], j["FARE_BASIS_CODE"])):

                match += 1
            else:
                continue

            if match < 1:
                # print i['SEQ_NO']
                continue

            if j["SERVICE_FEE_PERCENT"] > 0.0:
                j["SERVICE_FEE_AMOUNT"] = (j["SERVICE_FEE_PERCENT"] * fare) / 100


            elif j['SERVICE_FEE_CUR'] != currency:

                y_cur = j['SERVICE_FEE_AMOUNT']/j["Reference_Rate"]
                f_cur = Reference_Rate
                j["SERVICE_FEE_AMOUNT"] = y_cur / f_cur

            if (j['SERVICE_TYPE_TAX_CODE'] == "YQ" and j['SERVICE_TYPE_SUB_CODE'] == "I"):
                yqi2 = j["SERVICE_FEE_AMOUNT"]
                checklist.append("YQI")
                yqisfa2 = j['SERVICE_FEE_APPL']
                yqiseqno.append(j['SEQ_NO'])


            elif (j['SERVICE_TYPE_TAX_CODE'] == "YQ" and j['SERVICE_TYPE_SUB_CODE'] == "F"):
                yqf2 = j["SERVICE_FEE_AMOUNT"]
                checklist.append("YQF")
                yqfsfa2 = j['SERVICE_FEE_APPL']
                yqfseqno.append(j['SEQ_NO'])

            elif (j['SERVICE_TYPE_TAX_CODE'] == "YR" and j['SERVICE_TYPE_SUB_CODE'] == "I"):
                yrisfa2 = j['SERVICE_FEE_APPL']
                yri2 = j["SERVICE_FEE_AMOUNT"]
                checklist.append("YRI")
                yriseqno.append(j['SEQ_NO'])

            elif (j['SERVICE_TYPE_TAX_CODE'] == "YR" and j['SERVICE_TYPE_SUB_CODE'] == "F"):
                yrfsfa2 = j['SERVICE_FEE_APPL']
                yrf2 = j["SERVICE_FEE_AMOUNT"]
                checklist.append("YRF")
                yrfseqno.append(j['SEQ_NO'])

            # print "CONNECTION 2:", yqi2, yqf2, yri2, yrf2, seqno

        YQf = YQYQvalue(yqf1, yqf2, yqfsfa1, yqfsfa2, owrt)
        YQi = YQYQvalue(yqi1, yqi2, yqisfa1, yqisfa2, owrt)
        YRf = YQYQvalue(yrf1, yrf2, yrfsfa1, yrfsfa2, owrt)
        YRi = YQYQvalue(yri1, yri2, yrisfa1, yrisfa2, owrt)
        YQ = YQf + YQi
        YR = YRi + YRf
        YQYR_SEQ_NO =  [yqiseqno,yqfseqno,yriseqno,yrfseqno]
        YQ_flag = 2
        checklist = []

        # print YQ, YR
        print yqf1, yqf2,yqi1, yqi2,yrf1, yrf2,yri1, yri2
        print [YQ, YR, YQYR_SEQ_NO, YQ_flag]
        coll.update({"_id": i["_id"]}, {'$set': {'YQ': YQ, 'YR': YR, 'YQI_seqno': YQYR_SEQ_NO[0],'YQF_seqno': YQYR_SEQ_NO[1],'YRI_seqno': YQYR_SEQ_NO[2],'YRF_seqno': YQYR_SEQ_NO[3], 'YQ_flag':YQ_flag}})

        print ("hub-->", time.time() - sta)




# @measure(JUPITER_LOGGER)
def yqyr_p2p(system_date,file_date, carrier_list, OD_list,origin,origin_area,origin_country,origin_zone,destination,destination_area,destination_country,destination_zone):

    print "============P2P============="
    df_stage1 = stage1_p2p(carrier_list[0], origin, origin_area, origin_zone, origin_country, destination, destination_area,
                       destination_zone, destination_country, system_date)
    if df_stage1.empty:
        print('DataFrame is empty!')
        return

    cur = coll.find(
        {'carrier': {'$in': carrier_list}, 'OD': {'$in': OD_list},
         '$or': [{'effective_to': None}, {'effective_to': {'$gt': file_date}}]},
        no_cursor_timeout=True)
    # print cur.count()

    for i in cur:

        sta = time.time()
        yqi = 0
        yqf = 0
        yri = 0
        yrf = 0
        yrfseqno = []
        yqfseqno = []
        yriseqno = []
        yqiseqno = []

        if i["oneway_return"] == "1":
            owrt = "N"
        else:
            owrt = "Y"
        rbd = i['RBD']
        compartment=i['compartment']


        print (i["carrier"], i["RBD"], i["compartment"], i["origin"], i["origin_area"],
               i["origin_zone"], i["origin_country"],
               i["destination"], i["destination_area"], i["destination_zone"], i["destination_country"], system_date,
               i["fare_basis"], i["fare"], i["currency"], i["Reference_Rate"], owrt)

        df_stage2 = df_stage1.loc[(

            # (df_stage1.RBD_198.isna()| df_stage1.RBD_198 ==  rbd) &
            (
                (df_stage1.RBD_1 == rbd) | (df_stage1.RBD_2 == rbd) | (df_stage1.RBD_3 == rbd) | (
                    (df_stage1.RBD_1 == '') & (df_stage1.RBD_2 == '') & (df_stage1.RBD_3 == ''))) &
            (df_stage1.CABIN.isin([compartment, ''])) &
            (df_stage1.RTN_TO_ORIG.isin([owrt, ''])))]

        ###### create index on df_stage2

        checklist = []
        for index, j in df_stage2.iterrows():

            if j.get('RBD_198') :
                if isinstance(j['RBD_198'], list) and rbd not in j['RBD_198']:
                    continue


            if j['SERVICE_TYPE_TAX_CODE'] + j['SERVICE_TYPE_SUB_CODE'] in checklist:
                continue

            match = 0

            if j.get('FARE_CLASS_TBL_171') and isinstance(j['FARE_CLASS_TBL_171'], dict):
                # print type(j.get('FARE_CLASS_TBL_171'))

                for l in range(0, len(j['FARE_CLASS_TBL_171']['FARE_CLASS_CODE'])):
                    if (j['FARE_CLASS_TBL_171']['FARE_CLASS_CODE'][l] == "" or wild_card_check(i['fare_basis'],
                                                                             j['FARE_CLASS_TBL_171']['FARE_CLASS_CODE'][l])):
                        match += 1
                        # print j['FARE_CLASS_TBL_171']['FARE_CLASS_CODE'][l] + " matched!!"
                        break
                    else:
                        continue



            elif (j["FARE_BASIS_CODE"] == "" or wild_card_check(i['fare_basis'], j["FARE_BASIS_CODE"])):

                match += 1
            else:
                continue

            if match < 1:
                continue

            if j["SERVICE_FEE_PERCENT"] > 0.0:
                j["SERVICE_FEE_AMOUNT"] = (j["SERVICE_FEE_PERCENT"] * i['fare']) / 100

            elif j['SERVICE_FEE_CUR'] != i['currency']:

                y_cur = j['SERVICE_FEE_AMOUNT']/j["Reference_Rate"]
                f_cur = i['Reference_Rate']

                j["SERVICE_FEE_AMOUNT"] = y_cur / f_cur
            else:
                pass
            # add service fee appl


            if ((j['SERVICE_FEE_APPL'] in ["", "1"]) and owrt == "Y"):
                j["SERVICE_FEE_AMOUNT"] = j["SERVICE_FEE_AMOUNT"] * 2
            else:
                pass

            if (j['SERVICE_TYPE_TAX_CODE'] == "YQ" and j['SERVICE_TYPE_SUB_CODE'] == "I"):
                yqi = j["SERVICE_FEE_AMOUNT"]
                checklist.append("YQI")
                yqiseqno.append(j['SEQ_NO'])

            elif (j['SERVICE_TYPE_TAX_CODE'] == "YQ" and j['SERVICE_TYPE_SUB_CODE'] == "F"):
                yqf = j["SERVICE_FEE_AMOUNT"]
                checklist.append("YQF")
                yqfseqno.append(j['SEQ_NO'])

            elif (j['SERVICE_TYPE_TAX_CODE'] == "YR" and j['SERVICE_TYPE_SUB_CODE'] == "I"):
                yri = j["SERVICE_FEE_AMOUNT"]
                checklist.append("YRI")
                yriseqno.append(j['SEQ_NO'])

            elif (j['SERVICE_TYPE_TAX_CODE'] == "YR" and j['SERVICE_TYPE_SUB_CODE'] == "F"):
                yrf = j["SERVICE_FEE_AMOUNT"]
                checklist.append("YRF")
                yrfseqno.append(j['SEQ_NO'])

        # print (yqi, yqf, yri, yrf)

        YQ = yqi + yqf
        YR = yri + yrf
        YQYR_SEQ_NO = [yqiseqno, yqfseqno, yriseqno, yrfseqno]
        YQ_flag = 1
        # print YQYR_SEQ_NO

        print [YQ, YR, YQYR_SEQ_NO[0],YQYR_SEQ_NO[1],YQYR_SEQ_NO[2],YQYR_SEQ_NO[3], YQ_flag]
        coll.update({"_id": i["_id"]}, {'$set': {'YQ': YQ, 'YR': YR, 'YQI_seqno': YQYR_SEQ_NO[0],'YQF_seqno': YQYR_SEQ_NO[1],'YRI_seqno': YQYR_SEQ_NO[2],'YRF_seqno': YQYR_SEQ_NO[3], 'YQ_flag':YQ_flag}})
        print ("p2pe-->", time.time() - sta)



# @measure(JUPITER_LOGGER)
def yqyr_main(system_date, carrier_list, OD_list,hub,hub_area,hub_country,hub_zone,origin_area,origin_country,origin_zone,destination_area,destination_country,destination_zone):

    origin=OD_list[0][:3]
    destination=OD_list[0][3:]
    file_date=system_date
    # print file_date
    sysdate = datetime.datetime.strptime(system_date, '%Y-%m-%d')
    system_date=datetime.datetime.strftime(sysdate, "%y%m%d")
    # print system_date,file_date

    print carrier_list, origin, origin_area, origin_zone, origin_country, destination, destination_area,destination_zone, destination_country, system_date

    if hub in [origin, destination]:
        yqyr_p2p(system_date,file_date, carrier_list, OD_list,origin,origin_area,origin_country,origin_zone,destination,destination_area,destination_country,destination_zone)

    else:
        yqyr_hub(system_date,file_date, carrier_list, OD_list,hub,hub_area,hub_country,hub_zone,origin,origin_area,origin_country,origin_zone,destination,destination_area,destination_country,destination_zone)

"""=========================================================================================================================================================="""
if __name__=='__main__':
    st = time.time()
    carriers = ["EY"]
    ods=["XNBMOW","AUHDMM"]
    system_date="2018-12-24"
    for carrier in carriers:
        for OD in ods:
            origin = OD[:3]
            destination = OD[3:]
            curx = db_fzDB.JUP_DB_Carrier_hubs.find({"carrier": carrier})
            for x in curx:
                hub = x["hub"]
                hub_country = x["hub_country"]
                hub_zone = x["hub_zone"]
                hub_area = x["hub_area"]
            curz = db_fzDB.JUP_DB_ATPCO_Zone_Master.find({"CITY_CODE": {"$in": [origin, destination]}})
            for j in curz:
                if j["CITY_CODE"] == origin:
                    origin_area = j["CITY_AREA"]
                    origin_zone = j["CITY_ZONE"]
                    origin_country = j["CITY_CNTRY"]

                elif j["CITY_CODE"] == destination:
                    destination_area = j["CITY_AREA"]
                    destination_zone = j["CITY_ZONE"]
                    destination_country = j["CITY_CNTRY"]


            yqyr_main(system_date, [carrier], [OD], hub, hub_area, hub_country, hub_zone, origin_area,
                          origin_country, origin_zone, destination_area, destination_country, destination_zone)

    print ("END-->", time.time() - st)
