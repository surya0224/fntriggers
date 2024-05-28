import jupiter_AI.common.ClassErrorObject as error_class
import datetime
import json
import time
from jupiter_AI import network_level_params as net, JUPITER_LOGGER
from jupiter_AI.logutils import measure


import pymongo



@measure(JUPITER_LOGGER)
def main():
    cursor = db.JUP_DB_Data_Competitor_Weights.find({'compartment':'Y'})
    print 'cursor_count', cursor.count()
    dict_all={}
    i=0
    for c in cursor:
        i+=1
        dict_ratings    = {}
        dict_duplicates = {}
        compartment     = c['compartment']
        group           = c['group']
        component       = c['component']
        weight          = c['weight']
        print i, ',', compartment, ',', group, ',', component, ',', weight
start_time = time.time()
print start_time
start_time = time.time()
print start_time
MONGO_CLIENT_URL = '13.92.251.7:42525'
#net.MONGO_CLIENT_URL
ANALYTICS_MONGO_USERNAME = 'analytics'
ANALYTICS_MONGO_PASSWORD = 'KNjSZmiaNUGLmS0Bv2'
JUPITER_DB = 'fzDB_stg'
client = pymongo.MongoClient(MONGO_CLIENT_URL)
client.the_database.authenticate(ANALYTICS_MONGO_USERNAME, ANALYTICS_MONGO_PASSWORD, source='admin')
db = client[JUPITER_DB]
#from pymongo import MongoClient
#client = MongoClient("mongodb://analytics:KNjSZmiaNUGLmS0Bv2@13.92.251.7:42525/")
#db = client.fzDB_stg

TOLERANCE=0.001         ## error tolerance
COLLECTION = 'JUP_DB_Capacity_1'

main()

