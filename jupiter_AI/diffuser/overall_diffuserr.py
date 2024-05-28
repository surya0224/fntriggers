'''
Author: Premkumar Narasimhan
Date: 2nd April, 2017
Purpose:
    Calculates the Channel diffuser
Explanation:
    Derives from "diffuser" base class
## the channel diffuser is of type "absolute" - sellup/selldown is always with respect to the base_farebasis amount, and not the with respect to the next lower farebasis in the farebasis hierarchy
 The diffuser output is in the structure diffuser.list_of_dicts_diffuser_output, where "diffuser" is the base class for "rbd_diffuser"
'''
import jupiter_AI.common.ClassErrorObject as errorClass
import datetime
import json
import time
import jupiter_AI.network_level_params as net
import inspect
import collections
import copy
import jupiter_AI.common.ClassErrorObject as error_class
import ClassDiffuser as diffuserClass
import ClassChannelDiffuserReference as channelDiffuserReferenceClass
import pymongo
import ClassRBDDiffuser as rbd_diffuser
import ClassFarebrandDiffuser as farebrand_diffuser
import ClassChannelDiffuser as channel_diffuser


start_time = time.time()
# print start_time
MONGO_CLIENT_URL = '13.92.251.7:42525'
#net.MONGO_CLIENT_URL
ANALYTICS_MONGO_USERNAME = 'analytics'
ANALYTICS_MONGO_PASSWORD = 'KNjSZmiaNUGLmS0Bv2'
JUPITER_DB = 'testDB'
client = pymongo.MongoClient(MONGO_CLIENT_URL)
client.the_database.authenticate(ANALYTICS_MONGO_USERNAME, ANALYTICS_MONGO_PASSWORD, source='admin')
db = client[JUPITER_DB]
def main_func(pos, origin , destination , compartment , country , region , price , farebasis):
    lst = [{"pos": pos , "origin": origin, "destination": destination , "compartment": compartment, "country": country,
           "region": region, "price": price, "farebasis": farebasis }]
    # lst=[{"pos" : "Network", "origin" : "Network", "destination" : "Network", "compartment" : "Y", "country" : "Network", "region" : "Network", "price" : 1000, "farebasis" : "MR1AE1"}]
    finallist=[]
    rbd_output=rbd_diffuser.main_func(lst)
    for i in rbd_output:
         finallist.append(json.dumps(i))
    farebrand_output=farebrand_diffuser.main_func(rbd_output)
    for i in farebrand_output:
        finallist.append(json.dumps(i))
    channel_output=channel_diffuser.main_func(farebrand_output)
    for i in channel_output:
        finallist.append(json.dumps(i))
    print finallist
# main_func(pos="", origin="", destination = "", compartment="Y",country ="india", region ="", price=700, farebasis="TRS6AE2")
