"""
Author: Anamika Jaiswal
Date Created: 2018-05-15
File Name: yqyr_changetab.py

This code is used to update S1 tab 178 and tab 190 record from the corresponding change record.

"""

import pymongo
from pymongo import MongoClient
import time

from jupiter_AI import  JUPITER_DB, ATPCO_DB, JUPITER_LOGGER
from jupiter_AI.logutils import measure




@measure(JUPITER_LOGGER)
def yqyrchangetab_main(system_date, file_time,client):
    db = client[ATPCO_DB]
    coll_YQYR_Table_190 = db.JUP_DB_ATPCO_YQYR_Table_190_change
    cur_YQYR_Table_190 = coll_YQYR_Table_190.find({"file_date" : system_date, "file_time" : file_time})
    tab_main=db.JUP_DB_ATPCO_YQYR_Table_190
    ## YQYR Table 190
    counter = 0
    for i in cur_YQYR_Table_190:
        if counter % 100 == 0:
            print("done 190 change: ", counter)
        try:
            if i["CXR_SUBSTRING_1"]:
                pass
        except KeyError:
            no_segs = i['NO_OF_SEGS']
            i['CARRIER'] = "@" + i['CARRIER']
            for j in range(int(no_segs)):
                if j == 0:
                    i['CXR_SUBSTRING_' + str(j + 1)] = i['CARRIER'][(j * 4) + 1:(j * 4) + 4]
                else:
                    i['APPL_SUBSTRING_' + str(j + 1)] = i['CARRIER'][j * 4]
                    i['CXR_SUBSTRING_' + str(j + 1)] = i['CARRIER'][(j * 4) + 1:(j * 4) + 4]
            coll_YQYR_Table_190.save(i)
            if i["ACTION"] == "2":
                # print "passed ACTION - 2"
                pass
            else:
                i['cancel'] = 1
                cur_1 = tab_main.find({"TBL_NO": i["TBL_NO"]})
                for m in cur_1:
                    m['cancel'] = 1

                tab_main.update({"_id": m["_id"]}, m)

            tab_main.save(i)

        counter += 1

    print "Tab 190 change done"

#---------------------------------------------------------------------------------------------------

    tab_178=db.JUP_DB_ATPCO_YQYR_Table_178
    tab_change=db.JUP_DB_ATPCO_YQYR_Table_178_change

    cur = tab_change.find({"file_date" : system_date, "file_time" : file_time})
    #print ("starting 178", cur.count())
    count = 1
    for i in cur:
        if i["ACTION"] == "2":
            #print "passed ACTION - 2"
            pass
        else:
            #print "querying changed coll 3"
            cur_1 = tab_178.find({"TBL_NO":i["TBL_NO"]})
            i['cancel'] = 1
            #print "queried"
            for j in cur_1:

                j['cancel']=1

                #print j["DATES_DISC_1"], "---- CHANGED ORIGINAL DISC DATE"
                tab_178.update({"_id":j["_id"]}, j)

        tab_178.save(i)
        print (count)
        count += 1

    print "Tab 178 change done"
#---------------------------------------------------------------------------------------------------
    tab_171=db.JUP_DB_ATPCO_YQYR_Table_171
    tab_change=db.JUP_DB_ATPCO_YQYR_Table_171_change
    cur = tab_change.find({"file_date" : system_date, "file_time" : file_time})
    # print ("starting 171", cur.count())
    count = 1
    for i in cur:
        if i["ACTION"] == "2":
            # print "passed ACTION - 2"
            pass
        else:
            # print "querying changed coll 3"
            i['cancel'] = 1
            cur_1 = tab_171.find({"TBL_NO": i["TBL_NO"]})
            # print "queried"
            for j in cur_1:
                j['cancel'] = 1

                # print j["DATES_DISC_1"], "---- CHANGED ORIGINAL DISC DATE"
                tab_171.update({"_id": j["_id"]}, j)

        tab_171.save(i)
        print (count)
        count += 1

    print "Tab 171 change done"

#---------------------------------------------------------------------------------------------------
    # tab_173=db.JUP_DB_ATPCO_YQYR_Table_173
    # tab_change=db.JUP_DB_ATPCO_YQYR_Table_173_change
    # cur = tab_change.find({})
    # # print ("starting 173", cur.count())
    # count = 1
    # for i in cur:
    #     if i["ACTION"] == "2":
    #         # print "passed ACTION - 2"
    #         pass
    #     else:
    #         # print "querying changed coll 3"
    #         i['cancel'] = 1
    #         cur_1 = tab_173.find({"TBL_NO": i["TBL_NO"]})
    #         # print "queried"
    #         for j in cur_1:
    #             j['cancel'] = 1

    #             # print j["DATES_DISC_1"], "---- CHANGED ORIGINAL DISC DATE"
    #             tab_173.update({"_id": j["_id"]}, j)

    #     tab_173.save(i)
    #     print (count)
    #     count += 1

    # print "Tab 173 change done"

#---------------------------------------------------------------------------------------------------
    tab_198=db.JUP_DB_ATPCO_YQYR_Table_198
    tab_change=db.JUP_DB_ATPCO_YQYR_Table_198_change
    cur = tab_change.find({"file_date" : system_date, "file_time" : file_time})
    # print ("starting 198", cur.count())
    count = 1
    for i in cur:
        if i["ACTION"] == "2":
            # print "passed ACTION - 2"
            pass
        else:
            # print "querying changed coll 3"
            cur_1 = tab_198.find({"TBL_NO": i["TBL_NO"]})
            i['cancel'] = 1
            # print "queried"
            for j in cur_1:
                j['cancel'] = 1

                # print j["DATES_DISC_1"], "---- CHANGED ORIGINAL DISC DATE"
                tab_198.update({"_id": j["_id"]}, j)

        tab_198.save(i)
        print (count)
        count += 1

    print "Tab 198 change done"

#---------------------------------------------------------------------------------------------------