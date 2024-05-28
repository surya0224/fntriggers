from pymongo import MongoClient
db = MongoClient(['35.154.147.153:27022'])['fzDB_stg']
db.authenticate('mgadmin','mgadmin@2018', "admin")
system_date=db.JUP_DB_Data_Status.find().sort('last_update_date', -1).limit(1)
for i in system_date:
    t=str(i['last_update_date'])
    print t