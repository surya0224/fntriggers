"""
Author: Anamika Jaiswal
Date Created: 2018-05-15
File Name: yqyr_zoneflag.py

This code is used to reslove zone issue and generates flag as per current logic.

"""

import pymongo
from pymongo import MongoClient
import time
from datetime import datetime, timedelta


from jupiter_AI import JUPITER_DB, ATPCO_DB, JUPITER_LOGGER
from jupiter_AI.logutils import measure




@measure(JUPITER_LOGGER)
def yqyr_zoneflag_main(client):
    db = client[ATPCO_DB]


    s1 = db.JUP_DB_ATPCO_YQYR_RecS1_change

    tab = db.JUP_DB_ATPCO_YQYR_Table_178



    b = ["210", "211", "212"]

    s1.update({"POS_ZONE": "210"}, {"$set": {"POS_ZONE": b}}, multi= True)



    db.JUP_DB_ATPCO_YQYR_RecS1_change.update({"POT_ZONE": "210"}, {"$set": {"POT_ZONE": b}}, multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update({'SECT_PRT_LOC_1_ZONE': "210"}, {'$set': {'SECT_PRT_LOC_1_ZONE': b}}, multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update({'SECT_PRT_LOC_2_ZONE': "210"}, {'$set': {'SECT_PRT_LOC_2_ZONE': b}},multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update({"JRNY_LOC_1_ZONE": "210"}, {"$set": {"JRNY_LOC_1_ZONE": b}}, multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update({"JRNY_LOC_2_ZONE": "210"}, {"$set": {"JRNY_LOC_2_ZONE": b}}, multi=True)

    b = ["211", "212"]

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update(
        {'SECT_PRT_LOC_1_ZONE_TABLE_178': {'$exists':True}, 'SECT_PRT_LOC_1_ZONE_TABLE_178.0.GEO_LOC_ZONE': "210"}, {'$push': {
        'SECT_PRT_LOC_1_ZONE_TABLE_178.0.GEO_LOC_ZONE': {'$each': b}}}, multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update(
        {'SECT_PRT_LOC_1_ZONE_TABLE_178': {'$exists':True}, 'SECT_PRT_LOC_1_ZONE_TABLE_178.1.GEO_LOC_ZONE': "210"}, {'$push': {
        'SECT_PRT_LOC_1_ZONE_TABLE_178.1.GEO_LOC_ZONE': {'$each': b}}}, multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update(
        {'SECT_PRT_LOC_2_ZONE_TABLE_178': {'$exists':True}, 'SECT_PRT_LOC_2_ZONE_TABLE_178.0.GEO_LOC_ZONE': "210"}, {'$push': {
        'SECT_PRT_LOC_2_ZONE_TABLE_178.0.GEO_LOC_ZONE': {'$each': b}}}, multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update(
        {'SECT_PRT_LOC_2_ZONE_TABLE_178': {'$exists':True}, 'SECT_PRT_LOC_2_ZONE_TABLE_178.1.GEO_LOC_ZONE': "210"}, {'$push': {
        'SECT_PRT_LOC_2_ZONE_TABLE_178.1.GEO_LOC_ZONE': {'$each': b}}}, multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update(
        {'POS_ZONE_178': {'$exists':True}, 'POS_ZONE_178.0.GEO_LOC_ZONE': "210"}, {'$push': {
        'POS_ZONE_178.0.GEO_LOC_ZONE': {'$each': b}}}, multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update(
        {'POS_ZONE_178': {'$exists':True}, 'POS_ZONE_178.1.GEO_LOC_ZONE': "210"}, {'$push': {
        'POS_ZONE_178.1.GEO_LOC_ZONE': {'$each': b}}}, multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update(
        {'POT_ZONE_178': {'$exists':True}, 'POT_ZONE_178.0.GEO_LOC_ZONE': "210"}, {'$push': {
        'POT_ZONE_178.0.GEO_LOC_ZONE': {'$each': b}}}, multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update(
        {'POT_ZONE_178': {'$exists':True}, 'POT_ZONE_178.1.GEO_LOC_ZONE': "210"}, {'$push': {
        'POT_ZONE_178.1.GEO_LOC_ZONE': {'$each': b}}}, multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update(
        {'JRNY_LOC_1_ZONE_TABLE_178': {'$exists':True}, 'JRNY_LOC_1_ZONE_TABLE_178.0.GEO_LOC_ZONE': "210"}, {'$push': {
        'JRNY_LOC_1_ZONE_TABLE_178.0.GEO_LOC_ZONE': {'$each': b}}}, multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update(
        {'JRNY_LOC_1_ZONE_TABLE_178': {'$exists':True}, 'JRNY_LOC_1_ZONE_TABLE_178.1.GEO_LOC_ZONE': "210"}, {'$push': {
        'JRNY_LOC_1_ZONE_TABLE_178.1.GEO_LOC_ZONE': {'$each': b}}}, multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update(
        {'JRNY_LOC_2_ZONE_TABLE_178': {'$exists':True}, 'JRNY_LOC_2_ZONE_TABLE_178.0.GEO_LOC_ZONE': "210"}, {'$push': {
        'JRNY_LOC_2_ZONE_TABLE_178.0.GEO_LOC_ZONE': {'$each': b}}}, multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update(
        {'JRNY_LOC_2_ZONE_TABLE_178': {'$exists':True}, 'JRNY_LOC_2_ZONE_TABLE_178.1.GEO_LOC_ZONE': "210"}, {'$push': {
        'JRNY_LOC_2_ZONE_TABLE_178.1.GEO_LOC_ZONE': {'$each': b}}}, multi=True)



    b = ["230", "231", "232", "233"]

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update({"POS_ZONE": "230"}, {"$set": {"POS_ZONE": b}}, multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update({"POT_ZONE": "230"}, {"$set": {"POT_ZONE": b}}, multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update({'SECT_PRT_LOC_1_ZONE': "230"}, {'$set': {'SECT_PRT_LOC_1_ZONE': b}},multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update({'SECT_PRT_LOC_2_ZONE': "230"}, {'$set': {'SECT_PRT_LOC_2_ZONE': b}}, multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update({"JRNY_LOC_1_ZONE": "230"}, {"$set": {"JRNY_LOC_1_ZONE": b}}, multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update({"JRNY_LOC_2_ZONE": "230"}, {"$set": {"JRNY_LOC_2_ZONE": b}}, multi=True)

    b = ["231", "232", "233"]

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update(
        {'SECT_PRT_LOC_1_ZONE_TABLE_178': {'$exists':True}, 'SECT_PRT_LOC_1_ZONE_TABLE_178.0.GEO_LOC_ZONE': "230"}, {'$push': {
        'SECT_PRT_LOC_1_ZONE_TABLE_178.0.GEO_LOC_ZONE': {'$each': b}}}, multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update(
        {'SECT_PRT_LOC_1_ZONE_TABLE_178': {'$exists':True}, 'SECT_PRT_LOC_1_ZONE_TABLE_178.1.GEO_LOC_ZONE': "230"}, {'$push': {
        'SECT_PRT_LOC_1_ZONE_TABLE_178.1.GEO_LOC_ZONE': {'$each': b}}}, multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update(
        {'SECT_PRT_LOC_2_ZONE_TABLE_178': {'$exists':True}, 'SECT_PRT_LOC_2_ZONE_TABLE_178.0.GEO_LOC_ZONE': "230"}, {'$push': {
        'SECT_PRT_LOC_2_ZONE_TABLE_178.0.GEO_LOC_ZONE': {'$each': b}}}, multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update(
        {'SECT_PRT_LOC_2_ZONE_TABLE_178': {'$exists':True}, 'SECT_PRT_LOC_2_ZONE_TABLE_178.1.GEO_LOC_ZONE': "230"}, {'$push': {
        'SECT_PRT_LOC_2_ZONE_TABLE_178.1.GEO_LOC_ZONE': {'$each': b}}}, multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update(
        {'POS_ZONE_178': {'$exists':True}, 'POS_ZONE_178.0.GEO_LOC_ZONE': "230"}, {'$push': {
        'POS_ZONE_178.0.GEO_LOC_ZONE': {'$each': b}}}, multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update(
        {'POS_ZONE_178': {'$exists':True}, 'POS_ZONE_178.1.GEO_LOC_ZONE': "230"}, {'$push': {
        'POS_ZONE_178.1.GEO_LOC_ZONE': {'$each': b}}}, multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update(
        {'POT_ZONE_178': {'$exists':True}, 'POT_ZONE_178.0.GEO_LOC_ZONE': "230"}, {'$push': {
        'POT_ZONE_178.0.GEO_LOC_ZONE': {'$each': b}}}, multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update(
        {'POT_ZONE_178': {'$exists':True}, 'POT_ZONE_178.1.GEO_LOC_ZONE': "230"}, {'$push': {
        'POT_ZONE_178.1.GEO_LOC_ZONE': {'$each': b}}}, multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update(
        {'JRNY_LOC_1_ZONE_TABLE_178': {'$exists':True}, 'JRNY_LOC_1_ZONE_TABLE_178.0.GEO_LOC_ZONE': "230"}, {'$push': {
        'JRNY_LOC_1_ZONE_TABLE_178.0.GEO_LOC_ZONE': {'$each': b}}}, multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update(
        {'JRNY_LOC_1_ZONE_TABLE_178': {'$exists':True}, 'JRNY_LOC_1_ZONE_TABLE_178.1.GEO_LOC_ZONE': "230"}, {'$push': {
        'JRNY_LOC_1_ZONE_TABLE_178.1.GEO_LOC_ZONE': {'$each': b}}}, multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update(
        {'JRNY_LOC_2_ZONE_TABLE_178': {'$exists':True}, 'JRNY_LOC_2_ZONE_TABLE_178.0.GEO_LOC_ZONE': "230"}, {'$push': {
        'JRNY_LOC_2_ZONE_TABLE_178.0.GEO_LOC_ZONE': {'$each': b}}}, multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update(
        {'JRNY_LOC_2_ZONE_TABLE_178': {'$exists':True}, 'JRNY_LOC_2_ZONE_TABLE_178.1.GEO_LOC_ZONE': "230"}, {'$push': {
        'JRNY_LOC_2_ZONE_TABLE_178.1.GEO_LOC_ZONE': {'$each': b}}}, multi=True)




    db.JUP_DB_ATPCO_YQYR_RecS1_change.update( {'$or':[{"PSGR_TYPE":{"$nin": ["ADT", ""]}},{"EQP":{'$ne':""}},{"SECT_PRT_INTL_DOM":{"$nin":["","I"]}},{"POS_CODE_TYPE":{'$ne':""}}]}, {"$set" : {"flag" :0}},multi=True)

    db.JUP_DB_ATPCO_YQYR_RecS1_change.update( {'flag':{'$ne':0}}, {"$set" : {"flag" :1}},multi=True)




    db.JUP_DB_ATPCO_YQYR_RecS1_change.update( {'CXR_CODE':"TK","JRNY_WITHIN_LOC_GEO_LOC": { '$ne': "" }}, {"$set" : {"flag" :0}},multi=True)

    count=0
    curs1=db.JUP_DB_ATPCO_YQYR_RecS1_change.find({})
    for j in curs1:

        if ("/" in j["FARE_BASIS_CODE"]) or ("*" in j["FARE_BASIS_CODE"]):
            db.JUP_DB_ATPCO_YQYR_RecS1_change.update({"_id": j["_id"]}, {"$set" : {"flag" :0}},multi=True)
        count+=1
        print count