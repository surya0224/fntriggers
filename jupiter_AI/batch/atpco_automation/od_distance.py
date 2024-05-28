import pymongo
from pymongo import MongoClient

#client = MongoClient()

from datetime import datetime, timedelta
from jupiter_AI import mongo_client
import datetime
import time
from jupiter_AI import JUPITER_DB, ATPCO_DB, Host_Airline_Code, Host_Airline_Hub, JUPITER_LOGGER


def od_distance(client,system_date):
    db= client[JUPITER_DB]
    system_date = datetime.datetime.strptime(system_date, '%Y-%m-%d')
    system_date=datetime.datetime.strftime(system_date, "%Y%m%d")

    ods=db.JUP_DB_ATPCO_Fares_Rules.distinct('OD',{'carrier': Host_Airline_Code, 'file_date': {'$eq':system_date}, "OD_distance":0})
    #print ods
    missing_ods=[]
    for a in ods:


        sub1=a[:3]
        sub2=a[3:]
        od1=sub1+"DXB"
        od2="DXB"+sub2

        if "DXB" in a:
            cur = db.JUP_DB_OD_Distance_Master.find_one({"od": a})
            try:
                dis = cur.get("distance", 1)
            except:
                dis = 1
                missing_ods.append(a)


            #print "dis for :"+a+str(dis)

        else:

            cur1 = db.JUP_DB_OD_Distance_Master.find_one({"od": od1})
            cur2 = db.JUP_DB_OD_Distance_Master.find_one({"od": od2})
            try:
                dis1 = cur1.get("distance", 0)
            except:
                dis1=0
                missing_ods.append(od1)
            try:

                dis2 = cur2.get("distance", 0)

            except:
                dis2=0
                missing_ods.append(od2)
            # print dis1,dis2
            if dis1==0 or dis2==0:
                dis=1
                print "Distance not found for: "+a
            else:
                dis=dis1+dis2
                #print "Distance found for "+a+str(dis)

        db.JUP_DB_ATPCO_Fares_Rules.update({'carrier':Host_Airline_Code,'file_date':system_date,'OD':a},{'$set':{'OD_distance':dis}},multi=True)


if __name__ == '__main__':
    client = mongo_client()
    od_distance("2018-04-02")
