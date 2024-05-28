"""
Author: Abhinav Garg
Created with <3
Date Created: 2017-09-10
File Name: fc.py

This code updates the ATPCO fares in JUP_DB_ATPCO_Fares_Rules whenever fare changes happen.

"""
import pymongo
from bson import ObjectId
from pymongo import MongoClient
import time
from datetime import datetime, timedelta

from jupiter_AI import JUPITER_DB, ATPCO_DB, Host_Airline_Code, Host_Airline_Hub, JUPITER_LOGGER
from jupiter_AI.batch.atpco_automation.fr import fares_rules, run_stage_1
from jupiter_AI.logutils import measure


@measure(JUPITER_LOGGER)
def fare_changes(system_date, file_time, carrier_list, tariff_list, client):
    db = client[ATPCO_DB]
    db_fzDB = client[JUPITER_DB]
    # cr = db_fzDB.JUP_DB_ATPCO_Temp_Dates_DND.find({'file':'fc1.py'})
    # for i in cr:
    #     system_date = i['date']
    print system_date
    # db_fzDB.JUP_DB_ATPCO_Temp_Dates_DBD.update({'file':'fc1.py'},{'$set':{'status':'In Progress'}})
    rbd_date = datetime.strptime(system_date, '%Y-%m-%d').strftime('%y%m%d')
    file_date_ = datetime.strptime(system_date, '%Y-%m-%d').strftime('%Y%m%d')
    fare_record = db_fzDB.JUP_DB_ATPCO_Fares_change
    fare_record_main = db_fzDB.JUP_DB_ATPCO_Fares_Rules
    st = time.time()
    count = 1

    cur = fare_record.find({"carrier": {"$in": carrier_list}, 'tariff_code': {'$in': tariff_list}, "file_date" : file_date_, "file_time" : {"$in" : file_time}} , no_cursor_timeout=True).sort([("file_time" , pymongo.ASCENDING)])

    print ("starting Fares change", cur.count())
    for i in cur:
        if i["ACTION"] == "I":
            print ("passed ACTION - I")
        else:
            tariff = i["tariff_code"]
            carrier = i["carrier"]
            od = i["OD"]
            link_no = i["LINK_NO"]
            fare_basis = i["fare_basis"]
            eff_date = i["effective_from"]
            disc_date = i["effective_to"]
            query_date = datetime.strftime(datetime.strptime(i["file_date"], "%Y%m%d"), "%Y-%m-%d")

            print ("querying fare record")
            cur_1 = fare_record_main.find({"tariff_code":tariff, "carrier":carrier, "OD":od, "fare_basis":fare_basis,
                                           "LINK_NO":link_no}, no_cursor_timeout = True)
            print ("queried", cur_1.count())
            for j in cur_1:
                print (j["effective_to"], "---- ORIGINAL DISC DATE")
                print (eff_date, "---- NEW EFF DATE")

                if (eff_date == query_date) and (j["effective_to"] == None or j["effective_to"] > eff_date):
                    j["effective_to"] = datetime.strftime(datetime.strptime(eff_date, "%Y-%m-%d"), "%Y-%m-%d")
                elif (eff_date != query_date) and (j["effective_to"] == None or j["effective_to"] > eff_date):
                    j["effective_to"] = datetime.strftime(datetime.strptime(eff_date, "%Y-%m-%d") - timedelta(days=1), "%Y-%m-%d")

                print (j["effective_to"], "---- CHANGED ORIGINAL DISC DATE")
                fare_record_main.update({"_id": j["_id"]}, j)

        _id = i['_id']
        del i['_id']

        if i['carrier'] != Host_Airline_Code:
            cur_1 = db.JUP_DB_ATPCO_RBD.find({"CARRIER": i["carrier"], "$or": [{"EFF_DATE": "999999"}, {"EFF_DATE": {"$lte": rbd_date}}],
                              "DISC_DATE": {"$gte": rbd_date},
                              "$or": [{"FIRST_TKT_DATE": "999999"}, {"FIRST_TKT_DATE": {"$lte": rbd_date}}],
                              "LAST_TKT_DATE": {"$gte": rbd_date},
                              "GEO_LOC_1_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
                              "GEO_LOC_1_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
                              "GEO_LOC_1_CITY": {"$in": [i["origin"], i["destination"], ""]},
                              "GEO_LOC_1_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
                              "GEO_LOC_2_AREA": {"$in": [i["origin_area"], i["destination_area"], ""]},
                              "GEO_LOC_2_ZONE": {"$in": [i["origin_zone"], i["destination_zone"], ""]},
                              "GEO_LOC_2_CITY": {"$in": [i["origin"], i["destination"], ""]},
                              "GEO_LOC_2_COUNTRY": {"$in": [i["origin_country"], i["destination_country"], ""]},
                              "FLIGHT_NO_1": "", "FLIGHT_NO_2": "", "EQUIPMENT": ""})

            i['compartment'] = 'Undefined'
            for j in cur_1:
                try:
                    if i["RBD"] in j["RBDs_Y"]:
                        i['compartment'] = "Y"
                    elif i["RBD"] in j["RBDs_J"]:
                        i['compartment'] = "J"
                    elif i["RBD"] in j["RBDs_F"]:
                        i['compartment'] = "F"
                    else:
                        i['compartment'] = "Undefined"
                except KeyError:
                    i['compartment'] = 'Undefined'

        else:
            cur2 = db_fzDB.JUP_DB_Booking_Class.find({"Code": i["RBD"]})
            for j in cur2:
                i["compartment"] = j["comp"]

        try:
            i['combine_column'] = i["OD"] + i['compartment'] + i["currency"]

        except KeyError:
            i['combine_column'] = i["OD"] + "Undefined" + i["currency"]

        fare_record_main.insert(i)
        fare_record.remove({'_id': ObjectId(_id)})
        print (count)
        count += 1
    cur.close()
    print ("time taken -- ", time.time() - st)

    print 'Running Fares Rules for ', carrier_list[0], tariff_list[0]

    fr_date = datetime.strptime(system_date, '%Y-%m-%d')
    # fr_date = fr_date + timedelta(days=1)
    fr_date = datetime.strftime(fr_date, '%Y-%m-%d')

    ids = run_stage_1(system_date=fr_date,file_time=file_time, carrier_list=carrier_list, tariff_list=tariff_list, client=client)

    # fares_rules(system_date=fr_date, carrier_list=carrier_list, tariff_list=tariff_list)
    # return ids


if __name__=='__main__':
    fare_changes(Host_Airline_Code)