import pymongo
from pymongo import MongoClient
client = MongoClient()
import time
from pymongo.errors import BulkWriteError

#
db=pymongo.MongoClient("172.28.23.5:27022")["ATPCO_prod"]
db.authenticate('pdssETLUser', 'pdssETL@123', source='admin')
db_fzDB=pymongo.MongoClient("172.28.23.5:27022")["fzDB_prod"]
db_fzDB.authenticate('pdssETLUser', 'pdssETL@123', source='admin')

st = time.time()


num=1
count=0
coll = db_fzDB.JUP_DB_ATPCO_Fares_Rules
updateBulk =  db_fzDB.JUP_DB_ATPCO_Fares_Rules.initialize_unordered_bulk_op()
# coll = db_fzDB.temptemp
# updateBulk =  db_fzDB.temptemp.initialize_unordered_bulk_op()

cur=coll.find({'carrier':"FZ",'rtg' :{'$in':["00699","00399","00199","00799","00299","00099","00019","00009"]}},no_cursor_timeout = True)
for i in cur:
    fare_basis=i['fare_basis'][:-1]
    if '6' in fare_basis:
        fare_brand="LITE"
    elif '7' in fare_basis:
        fare_brand="VALUE"
    elif '8' in fare_basis:
        fare_brand="FLEX"
    elif '9' in fare_basis:
        fare_brand="BUSINESS"
    elif '5' in fare_basis:
        fare_brand="FLY+VISA"
    else: fare_brand=""
    # print i['fare_basis'],fare_brand
    updateBulk.find({"_id": i["_id"]}).update({'$set': {'fare_brand': fare_brand}})

    if (num % 100== 0):
        try:
            result1 = updateBulk.execute()
            updateBulk = db_fzDB.JUP_DB_ATPCO_Fares_Rules.initialize_unordered_bulk_op()
            now = time.time()
            print("{0} rows processing time : {1}".format(num, time.time() - st))
            # print(result1)
        except BulkWriteError as bwe:
            print(bwe.details)
    num = num + 1
print ("time taken -- ", time.time() - st)

try:
    updateBulk.execute()
except:
    pass
