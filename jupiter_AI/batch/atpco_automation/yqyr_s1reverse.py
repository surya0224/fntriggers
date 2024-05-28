"""
Author: Anamika Jaiswal
Date Created: 2018-05-15
File Name: yqyr_s1reverse.py

This code applies the reverse logic in the S1 collection
"""


from jupiter_AI import  JUPITER_DB, ATPCO_DB, JUPITER_LOGGER
from jupiter_AI.logutils import measure
import pymongo


@measure(JUPITER_LOGGER)
def yqyr_s1reverse_main(file_date,file_time, client):
    db = client[ATPCO_DB]
    count =0
    coll = db.JUP_DB_ATPCO_YQYR_RecS1
    cur = coll.find({'$and':[{'file_date':file_date, "file_time" : {"$in" : file_time}},{'JRNY_INDICATOR':""},{"$or":[{"JRNY_LOC_1_TYPE":{'$ne':""}},{"JRNY_LOC_2_TYPE":{'$ne':""}}]},{'reverse':None},{"reverse_check":None}]}).sort([("file_time" , pymongo.ASCENDING)])


    for i in cur:

        j=dict(i)


        del j['_id']
        j['JRNY_LOC_1_AREA']=i['JRNY_LOC_2_AREA']
        j['JRNY_LOC_1_CITY']=i['JRNY_LOC_2_CITY']
        j['JRNY_LOC_1_ZONE']=i['JRNY_LOC_2_ZONE']
        j['JRNY_LOC_1_CNTRY']=i['JRNY_LOC_2_CNTRY']
        j['JRNY_LOC_1_GEO_LOC']=i['JRNY_LOC_2_GEO_LOC']
        j['JRNY_LOC_1_TYPE']=i['JRNY_LOC_2_TYPE']
        j['JRNY_LOC_1_ZONE_TABLE_NO_178']=i['JRNY_LOC_2_ZONE_TABLE_NO_178']


        j['JRNY_LOC_2_AREA']=i['JRNY_LOC_1_AREA']
        j['JRNY_LOC_2_CITY']=i['JRNY_LOC_1_CITY']
        j['JRNY_LOC_2_ZONE']=i['JRNY_LOC_1_ZONE']
        j['JRNY_LOC_2_CNTRY']=i['JRNY_LOC_1_CNTRY']
        j['JRNY_LOC_2_GEO_LOC']=i['JRNY_LOC_1_GEO_LOC']
        j['JRNY_LOC_2_TYPE']=i['JRNY_LOC_1_TYPE']
        j['JRNY_LOC_2_ZONE_TABLE_NO_178']=i['JRNY_LOC_1_ZONE_TABLE_NO_178']



        if i['JRNY_LOC_1_TYPE']=="U" and i['JRNY_LOC_2_TYPE']=="U":
            j['JRNY_LOC_1_ZONE_TABLE_178'] = i['JRNY_LOC_2_ZONE_TABLE_178']
            j['JRNY_LOC_2_ZONE_TABLE_178'] = i['JRNY_LOC_1_ZONE_TABLE_178']
        elif i['JRNY_LOC_2_TYPE']=="U":
            j['JRNY_LOC_1_ZONE_TABLE_178'] = i['JRNY_LOC_2_ZONE_TABLE_178']
            j['JRNY_LOC_2_ZONE_TABLE_178'] = None
        elif i['JRNY_LOC_1_TYPE']=="U":
            j['JRNY_LOC_2_ZONE_TABLE_178'] = i['JRNY_LOC_1_ZONE_TABLE_178']
            j['JRNY_LOC_1_ZONE_TABLE_178'] = None

        j['reverse']=1
        coll .insert(j)
        count+=1
        print count


    print "Done S1 reverse part 1"


    coll = db.JUP_DB_ATPCO_YQYR_RecS1
    cur2 = coll.find({'$and':[{'file_date':file_date,"file_time" : {"$in" : file_time}},{'SECT_PRT_FROM_TO':{'$in':["","2"]}},{"$or":[{"SECT_PRT_LOC1_TYPE":{'$ne':""}},{"SECT_PRT_LOC_2_TYPE":{'$ne':""}}]},{'reverse2':None},{"reverse_check":None}]}).sort([("file_time" , pymongo.ASCENDING)])
    #cur=coll.find({})

    for i in cur2:

        j=dict(i)


        del j['_id']
        j['SECT_PRT_LOC_1_AREA']=i['SECT_PRT_LOC_2_AREA']
        j['SECT_PRT_LOC_1_CITY']=i['SECT_PRT_LOC_2_CITY']
        j['SECT_PRT_LOC_1_ZONE']=i['SECT_PRT_LOC_2_ZONE']
        j['SECT_PRT_LOC_1_CNTRY']=i['SECT_PRT_LOC_2_CNTRY']
        j['SECT_PRT_LOC1_GEO_LOC']=i['SECT_PRT_LOC_2_GEO_LOC']
        j['SECT_PRT_LOC1_TYPE']=i['SECT_PRT_LOC_2_TYPE']
        j['SECT_PRT_LOC_1_ZONE_TABLE_NO_178']=i['SECT_PRT_LOC_2_ZONE_TABLE_NO_178']


        j['SECT_PRT_LOC_2_AREA']=i['SECT_PRT_LOC_1_AREA']
        j['SECT_PRT_LOC_2_CITY']=i['SECT_PRT_LOC_1_CITY']
        j['SECT_PRT_LOC_2_ZONE']=i['SECT_PRT_LOC_1_ZONE']
        j['SECT_PRT_LOC_2_CNTRY']=i['SECT_PRT_LOC_1_CNTRY']
        j['SECT_PRT_LOC_2_GEO_LOC']=i['SECT_PRT_LOC1_GEO_LOC']
        j['SECT_PRT_LOC_2_TYPE']=i['SECT_PRT_LOC1_TYPE']
        j['SECT_PRT_LOC_2_ZONE_TABLE_NO_178']=i['SECT_PRT_LOC_1_ZONE_TABLE_NO_178']



        if i['SECT_PRT_LOC1_TYPE']=="U" and i['SECT_PRT_LOC_2_TYPE']=="U":
            j['SECT_PRT_LOC_1_ZONE_TABLE_178'] = i['SECT_PRT_LOC_2_ZONE_TABLE_178']
            j['SECT_PRT_LOC_2_ZONE_TABLE_178'] = i['SECT_PRT_LOC_1_ZONE_TABLE_178']
        elif i['SECT_PRT_LOC_2_TYPE']=="U":
            j['SECT_PRT_LOC_1_ZONE_TABLE_178'] = i['SECT_PRT_LOC_2_ZONE_TABLE_178']
            j['SECT_PRT_LOC_2_ZONE_TABLE_178'] = None
        elif i['SECT_PRT_LOC1_TYPE']=="U":
            j['SECT_PRT_LOC_2_ZONE_TABLE_178'] = i['SECT_PRT_LOC_1_ZONE_TABLE_178']
            j['SECT_PRT_LOC_1_ZONE_TABLE_178'] = None

        j['reverse2']=1
        coll.insert(j)
        if i['SECT_PRT_FROM_TO']=="2":
            coll.delete_one(i)

        count += 1
        print count

    coll.update({'file_date':file_date,"file_time" : {"$in" : file_time}}, {"$set": {"reverse_check": 1}}, multi= True)


    print "Done S1 reverse part 2"
