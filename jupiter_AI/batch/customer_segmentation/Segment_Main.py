#updating segment_main
from pymongo import MongoClient
from pymongo import UpdateOne
import pandas as pd
# client = MongoClient("mongodb://analytics:KNjSZmiaNUGLmS0Bv2@172.28.23.9:43535/")
# db = client.fzDB_stg
from jupiter_AI import JUPITER_DB, client, JUPITER_LOGGER
from jupiter_AI.logutils import measure
from jupiter_AI.triggers.common import cursor_to_df
db = client[JUPITER_DB]
conn = client['fzDB_stg']
from pymongo import UpdateOne
from bson import ObjectId
import time

bulk_list = []
count = 1
t = 0


@measure(JUPITER_LOGGER)
def segment_main(row):
    try:
        if row['segment_amadeus'] != None and row['segment_fz'] != None:
            return row['segment_fz']
        elif row['segment_amadeus'] != None and row['segment_fz'] == None:
            return row['segment_amadeus']
        elif row['segment_amadeus'] == None and row['segment_fz'] != None:
            return row['segment_fz']
        elif row['segment_amadeus'] == None and row['segment_fz'] == None:
            return None
        else:
            pass
    except:
        pass



skips = 0
limits = 1000000

loop_coun = db.JUP_DB_Sales.find({}).count()
loop_count = int(loop_coun/limits)
loop_count = loop_count+2
#for 28.5 mn, I write 1,30 in for loop.
for turns in range(1,loop_count):

    cursor_1 = db.JUP_DB_Sales.find({},{'od':1, 'segment_amadeus':1,'segment_fz':1})
    cursor = cursor_to_df(cursor_1)

    cursor['segment'] = cursor.apply(lambda row: segment_main(row),axis=1)

    for j in range(len(cursor)):
        if t == 1000:
            st = time.time()
            print "updating: ", count
            conn['JUP_DB_Sales'].bulk_write(bulk_list)
            print "updated!", time.time() - st
            bulk_list = []
            t = 0
            bulk_list.append(UpdateOne({"_id": ObjectId(cursor['_id'][j])}, {'$set': {'segment': cursor['segment'][j]}}))
            t += 1
            count += 1
        else:
            bulk_list.append(UpdateOne({"_id": ObjectId(cursor['_id'][j])}, {'$set': {'segment': cursor['segment'][j]}}))
            t += 1
            # print "t: ", t
    if len(bulk_list) != 0:
        st = time.time()
        conn['JUP_DB_Sales'].bulk_write(bulk_list)
        print "updated!---", time.time() - st
    else:
        pass
    skips = skips + limits
    print "updated records: ", skips


#updating sales_flown will start

print "Sales done. Sales_Flown starts now"

bulk_list = []
count = 1
t = 0

skips = 0
limits = 1000000

loop_coun = db.JUP_DB_Sales_Flown.find({}).count()
loop_count = int(loop_coun/limits)
loop_count = loop_count+2
print loop_count
#for 28.5 mn, I write 1,30 in for loop.
for turns in range(1,loop_count):

    cursor_1 = db.JUP_DB_Sales_Flown.find({},{'od':1,'segment_amadeus':1,'segment_fz':1})
    cursor = cursor_to_df(cursor_1)
    cursor['segment'] = cursor.apply(lambda row: segment_main(row),axis=1)

    for j in range(len(cursor)):
        if t == 1000:
            st = time.time()
            print "updating: ", count
            conn['JUP_DB_Sales_Flown'].bulk_write(bulk_list)
            print "updated!", time.time() - st
            bulk_list = []
            t = 0
            bulk_list.append(UpdateOne({"_id": ObjectId(cursor['_id'][j])}, {'$set': {'segment': cursor['segment'][j]}}))
            t += 1
            count += 1
        else:
            bulk_list.append(UpdateOne({"_id": ObjectId(cursor['_id'][j])}, {'$set': {'segment': cursor['segment'][j]}}))
            t += 1
            # print "t: ", t
    if len(bulk_list) != 0:
        st = time.time()
        conn['JUP_DB_Sales_Flown'].bulk_write(bulk_list)
        print "updated!---", time.time() - st
    else:
        pass
    skips = skips + limits
    print "updated records: ", skips

