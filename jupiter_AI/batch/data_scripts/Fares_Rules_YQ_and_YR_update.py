from __future__ import division
import copy
import json
import pymongo
import calendar
from pymongo.errors import BulkWriteError
from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne
import collections
# import pandas as pd
import numpy as np
from datetime import datetime
import datetime as dt
import time
from dateutil.relativedelta import relativedelta
import global_variable as var
import sys
from jupiter_AI import client,Host_Airline_Code, JUPITER_DB, JUPITER_LOGGER
from jupiter_AI.logutils import measure
from jupiter_AI.network_level_params import SYSTEM_DATE


def call_update(db):
    try:
        script_start_time = time.clock()
        cur_date = datetime.strptime(SYSTEM_DATE, '%Y-%m-%d')


        dt_range = cur_date + relativedelta(days=-1)
        day_1 = datetime.strftime(dt_range, '%Y%m%d')


        print SYSTEM_DATE,day_1

        print ".... Queriying....."
        # Aggregation of Farerules collections based on yesterday fares
        db.JUP_DB_ATPCO_Fares_Rules.aggregate([
            {"$match":{
                "carrier": Host_Airline_Code,
                "file_date" : day_1
            }},
            {"$project":{
                "origin": 1, "destination": 1, "compartment": 1, "oneway_return": 1, "currency": 1
            }},
            {"$group":{
                "_id": {
                "origin": '$origin', "destination": '$destination', "compartment": '$compartment',
                "oneway_return": '$oneway_return', "currency": '$currency'
                }
            }},
            {"$project":{
                "origin": '$_id.origin', "destination": '$_id.destination',
                "compartment": '$_id.compartment', "oneway_return": '$_id.oneway_return', "currency": '$_id.currency'
            }},
            {"$out":'Temp_fzDB_tbl_005'}
        ])
        print ".... Temp_collection_Output....."
        exch_dic = dict()
        for eachDOc in db.JUP_DB_Exchange_Rate.find():
            exch_dic[eachDOc["Currency"]] = eachDOc["Reference_Rate"];


        num = 1
        Bulk = db.JUP_DB_ATPCO_Fares_Rules.initialize_unordered_bulk_op()
        cursor = db.Temp_fzDB_tbl_005.find({},no_cursor_timeout= True)
        print ".... Update start....."
        for doc in cursor:
            YQYRCursor = db.JUP_DB_Tax_Master.find_one({"Origin": doc['origin'],
                                                       "Destination": doc['destination'],
                                                       "Compartment": doc['compartment'],
                                                       "OW_RT": doc['oneway_return']
                           });
            #print YQYRCursor
            if YQYRCursor != None:
                YQ = YQYRCursor['YQ'] * exch_dic['USD'];
                YR = YQYRCursor['YR'] * exch_dic['USD'];
            else:
                YQ = 0
                YR = 0
            try:
                if doc['currency'] != "AED":
                    YQ = YQ / exch_dic[doc['currency']]
                    YR = YR / exch_dic[doc['currency']]
            except Exception as bwe:
                YQ = YQ
                YR = YR

            print YQ,YR
            Bulk.find({
                "file_date" : day_1,
                "carrier": Host_Airline_Code,
                "origin": doc['origin'],
                "destination": doc['destination'],
                "compartment": doc['compartment'],
                "oneway_return": doc['oneway_return'],
                "currency": doc['currency']
            }).update(
            {"$set" : {
                "YQ": YQ,
                "YR": YR
            }})

            if num % 100 == 0:
                try:
                    Bulk.execute()
                    Bulk = db.JUP_DB_ATPCO_Fares_Rules.initialize_unordered_bulk_op()
                    print "updating = ",num
                except Exception as bwe:
                    Bulk = db.JUP_DB_ATPCO_Fares_Rules.initialize_unordered_bulk_op()
                    print(bwe)
            num = num+1
            dates = None
        try:
            Bulk.execute()
        except Exception as bwe:
            print(bwe)

        # Update
        db.JUP_DB_ATPCO_Fares_Rules.update({"carrier":"FZ", "file_date" : day_1,"channel":"TA","origin_country":"IN"},{"$set":{"YR":0.0}},multi=True)
        db.JUP_DB_ATPCO_Fares_Rules.update({"carrier":"FZ", "file_date" : day_1,"channel":"web"},{"$set":{"YR":0.0}},multi=True)
        db.JUP_DB_ATPCO_Fares_Rules.update({"carrier":"FZ", "file_date" : day_1,"Rule_id":"GP08","private_fare":True},{"$set":{"YR":0.0}},multi=True)
    except TypeError as typeError:
        print typeError
    except KeyError as typeError:
        print typeError
    except Exception as error:
        print error


if __name__ == '__main__':
    try :
        db = client[JUPITER_DB]
        call_update(db)
    except Exception as error:
        print error