from jupiter_AI import JUPITER_DB, ATPCO_DB, Host_Airline_Code, JUPITER_LOGGER, SYSTEM_DATE, today
from pandas import DataFrame;'l/nb'
from jupiter_AI.logutils import measure
import json
import requests
import traceback
import datetime
from jupiter_AI import SECURITY_CLIENT_ID, SECURITY_CLIENT_SECRET_ID, MAIL_URL, AUTH_URL, MAIL_RECEIVERS, ENV
from dateutil.relativedelta import relativedelta

@measure(JUPITER_LOGGER)
def Check_distiance(system_date, client, file_date):
    try:
        db = client[JUPITER_DB]
        cursor = db.JUP_DB_ATPCO_Fares_Rules.find({'carrier': Host_Airline_Code,
                                                   'file_date': file_date,"OD_distance":{"$lte":0}}).count()
        if cursor == 0:
            print ("no records with null valuse")
        else:
            cursor1 = db.JUP_DB_ATPCO_Fares_Rules.find({'carrier': Host_Airline_Code,
                                                       'file_date': file_date, "OD_distance": {"$lte": 0}},{"OD":1,"file_date":1})
            for i in cursor1:
                db.grades.update(
                    {"_id": i["_id"]},
                    {"$set": {"file_date": i["file_date"] ,"OD":i["OD"]}},
                    upsert=True)

    except:
        print("An exception occurred")
@measure(JUPITER_LOGGER)
def check_YQ(system_date, client, file_date):
    try:
        db = client[JUPITER_DB]


        cursor3= db.JUP_DB_ATPCO_Fares_Rules.find({
                        "file_date": file_date,
                        "carrier": Host_Airline_Code,
                        "YQ": {
                            "$eq": {"$in":[None, 0]}},
                                "$or":[
                                    {
                                        "effective_to": None},
                                    {"effective_to": {"$gte": system_date}}]
                                }).count()
        if cursor3==0:
            print ("passed")
        else:
            cursor3 = db.JUP_DB_ATPCO_Fares_Rules.find({
                "file_date": file_date,
                "carrier": Host_Airline_Code,
                "YQ": {
                    "$eq": {"$in": [None, 0]}},
                "$or": [
                    {
                        "effective_to": None},
                    {"effective_to": {"$gte": system_date}}]
            },{"file_date":1,"carrier":1,"OD":1,"YQ":1})
            for j in cursor3:
                db.grades.update(
                    {"_id": j["_id"]},
                    {"$set": {"file_date": j["file_date"], "OD": j["OD"],"YQ":j["YQ"]}},
                    upsert=True)
    except:
        print("some thing goes wrong")
def Check_tax(system_date, client, file_date):
    try:
        db = client[JUPITER_DB]
        cursor5=db.JUP_DB_ATPCO_Fares_Rules.aggregate([{"$match":{"carrier":"FZ","fare_include":True,"file_date":file_date}},{"$group":{"_id":

                          {"od":"$OD",
                             "oneway_return" :"$oneway_return",
                               "compartment":  "$compartment"
                          }}},{"$project":{
                         "_id":0,

                        "combine_column_new": {"$concat": ["$_id.od", "$_id.compartment","$_id.oneway_return",]}}},


                              ])
        for i in cursor5:
            cursor6=db.JUP_DB_Tax_Master.distinct("combine_column")
            for j in cursor6:
                if i not in j:
                    print (i)
                else:
                    print ("all matched")












