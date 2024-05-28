import pymongo
from pymongo import MongoClient
import time
from datetime import datetime, timedelta

from jupiter_AI import ATPCO_DB, JUPITER_DB, JUPITER_LOGGER
from jupiter_AI.logutils import measure


@measure(JUPITER_LOGGER)
def recseg2(system_date,file_time, client):
    db = client[ATPCO_DB]
    db_fzDB = client[JUPITER_DB]

    # cr = db_fzDB.JUP_DB_ATPCO_Temp_Dates_DND.find({'file':'Recseg2.py'})
    # for i in cr:
    #     system_date = i['date']
    system_date = datetime.strptime(system_date,'%Y-%m-%d')
    print system_date
    query_date = datetime.strftime(system_date, "%Y%m%d")
    check_date = datetime.strftime(system_date, "%Y-%m-%d")
    # db_fzDB.JUP_DB_ATPCO_Temp_Dates_DBD.update({'file':'Recseg2.py'},{'$set':{'status':'In Progress'}})

    coll_Record_0 = db.JUP_DB_ATPCO_Record_0

    cur_Record_0 = coll_Record_0.find({"LAST_UPDATED_DATE": {'$gte': check_date}, "LAST_UPDATED_TIME" : {"$in" : file_time}},no_cursor_timeout=True).sort([("file_time" , pymongo.ASCENDING)])

    print ("Record 0:")
    counter = 0
    for i in cur_Record_0:
        if counter%1000 == 0:
            print("done: ",counter)
        try:
            if i["GENERAL_RULE_NO_1"]:
                pass
        except KeyError:
            no_segs = i['NO_SEGS']
            for j in range(int(no_segs)):

                i['GENERAL_RULE_NO_'+str(j+1)] = i['SEGS_COLUMN'][(j*10):(j*10)+4]
                i['SCR_TARIFF_'+str(j+1)] = i['SEGS_COLUMN'][(j*10)+4:(j*10)+7]
                i['CAT_NO_'+str(j+1)] = i['SEGS_COLUMN'][(j*10)+7:(j*10)+10]
            coll_Record_0.save(i)
        counter += 1


    # In[ ]:

    coll_Record1 = db.JUP_DB_ATPCO_Record_1
    cur_Record1 = coll_Record1.find({"LAST_UPDATED_DATE": {'$gte': check_date}, "LAST_UPDATED_TIME" : {"$in" : file_time}},no_cursor_timeout=True).sort([("file_time" , pymongo.ASCENDING)])
    print ("Record 1:")
    ## Record 1
    counter = 0
    for i in cur_Record1:
        if counter%1000 == 0:
            print("done: ",counter)
        try:
            if i["FCI_DI_1"]:
                pass
        except KeyError:
            no_segs = i['NO_SEGS']
            for j in range(int(no_segs)):
                i['FCI_DI_'+str(j+1)] = i['FARE_CLASS_INFORMATION_SEGS'][(j*83)]
                i['FCI_TICKETING_CODE_'+str(j+1)] = i['FARE_CLASS_INFORMATION_SEGS'][(j*83)+1:(j*83)+11]
                i['FCI_TCM_'+str(j+1)] = i['FARE_CLASS_INFORMATION_SEGS'][(j*83)+11:(j*83)+12]
                i['FCI_TICKET_DESIGNATOR_'+str(j+1)] = i['FARE_CLASS_INFORMATION_SEGS'][(j*83)+12:(j*83)+22]
                i['FCI_TDM_'+str(j+1)] = i['FARE_CLASS_INFORMATION_SEGS'][(j*83)+22:(j*83)+23]
                i['FCI_PSGR_TYPE_'+str(j+1)] = i['FARE_CLASS_INFORMATION_SEGS'][(j*83)+23:(j*83)+26]
                i['FCI_PSGR_AGE_MIN_'+str(j+1)] = i['FARE_CLASS_INFORMATION_SEGS'][(j*83)+26:(j*83)+28]
                i['FCI_PSGR_AGE_MAX_'+str(j+1)] = i['FARE_CLASS_INFORMATION_SEGS'][(j*83)+28:(j*83)+30]
                i['FCI_FILLER_'+str(j+1)] = i['FARE_CLASS_INFORMATION_SEGS'][(j*83)+30:(j*83)+33]
                i['FCI_DATE_TBL_994_'+str(j+1)] = i['FARE_CLASS_INFORMATION_SEGS'][(j*83)+33:(j*83)+41]
                i['FCI_RBD_'+str(j+1)] = i['FARE_CLASS_INFORMATION_SEGS'][(j*83)+41:(j*83)+57]
                i['FCI_CARRIER_TBL_990_'+str(j+1)] = i['FARE_CLASS_INFORMATION_SEGS'][(j*83)+57:(j*83)+65]
                i['FCI_RBD_TBL_994_'+str(j+1)] = i['FARE_CLASS_INFORMATION_SEGS'][(j*83)+65:(j*83)+73]
                i['FCI_COMMERCIAL_NAME_'+str(j+1)] = i['FARE_CLASS_INFORMATION_SEGS'][(j*83)+73:(j*83)+83]
            coll_Record1.save(i)
        counter += 1


    # In[ ]:

    ## Record 2 Cat 03 FN
    coll_Record_2_Cat_03_FN = db.JUP_DB_ATPCO_Record_2_Cat_03_FN
    cur_Record_2_Cat_03_FN = coll_Record_2_Cat_03_FN.find({"LAST_UPDATED_DATE": {'$gte': check_date}, "LAST_UPDATED_TIME" : {"$in" : file_time}},no_cursor_timeout=True).sort([("file_time" , pymongo.ASCENDING)])
    print ("Record_2_Cat_03_FN:")
    counter = 0
    for i in cur_Record_2_Cat_03_FN:
        if counter%1000 == 0:
            print("done: ",counter)
        try:
            if i["DATA_TABLE_STRING_RI_1"]:
                pass
        except KeyError:
            no_segs = i['NO_SEGS']
            for j in range(int(no_segs)):
                try:
                    i['DATA_TABLE_STRING_RI_'+str(j+1)] = i['DATA_TABLE'][(j*14)]
                except IndexError:
                    i['DATA_TABLE_STRING_RI_'+str(j+1)] = ""
                try:
                    i['DATA_TABLE_STRING_CAT_NO_'+str(j+1)] = i['DATA_TABLE'][(j*14)+1:(j*14)+4]
                except IndexError:
                    i['DATA_TABLE_STRING_CAT_NO_'+str(j+1)] = ""
                try:
                    i['DATA_TABLE_STRING_TBL_NO_'+str(j+1)] = i['DATA_TABLE'][(j*14)+4:(j*14)+12]
                except IndexError:
                    i['DATA_TABLE_STRING_TBL_NO_'+str(j+1)] = ""
                try:
                    i['DATA_TABLE_STRING_IO_'+str(j+1)] = i['DATA_TABLE'][(j*14)+12]
                except IndexError:
                    i['DATA_TABLE_STRING_IO_'+str(j+1)] = ""
                try:
                    i['DATA_TABLE_STRING_DI_'+str(j+1)] = i['DATA_TABLE'][(j*14)+13]
                except IndexError:
                    i['DATA_TABLE_STRING_DI_'+str(j+1)] = ""
            coll_Record_2_Cat_03_FN.save(i)
        counter += 1


    # In[ ]:

    coll_Record_2_Cat_11_FN = db.JUP_DB_ATPCO_Record_2_Cat_11_FN
    cur_Record_2_Cat_11_FN = coll_Record_2_Cat_11_FN.find({"LAST_UPDATED_DATE": {'$gte': check_date}, "LAST_UPDATED_TIME" : {"$in" : file_time}},no_cursor_timeout=True).sort([("file_time" , pymongo.ASCENDING)])

    print ("Record_2_Cat_11_FN:")
    ## Record 2 Cat 11 FN
    counter = 0
    for i in cur_Record_2_Cat_11_FN:
        if counter%1000 == 0:
            print("done: ",counter)
        try:
            if i["DATA_TABLE_STRING_RI_1"]:
                pass
        except KeyError:
            no_segs = i['NO_SEGS']
            for j in range(int(no_segs)):
                try:
                    i['DATA_TABLE_STRING_RI_'+str(j+1)] = i['DATA_TABLE'][(j*14)]
                except IndexError:
                    i['DATA_TABLE_STRING_RI_'+str(j+1)] = ""
                try:
                    i['DATA_TABLE_STRING_CAT_NO_'+str(j+1)] = i['DATA_TABLE'][(j*14)+1:(j*14)+4]
                except IndexError:
                    i['DATA_TABLE_STRING_CAT_NO_'+str(j+1)] = ""
                try:
                    i['DATA_TABLE_STRING_TBL_NO_'+str(j+1)] = i['DATA_TABLE'][(j*14)+4:(j*14)+12]
                except IndexError:
                    i['DATA_TABLE_STRING_TBL_NO_'+str(j+1)] = ""
                try:
                    i['DATA_TABLE_STRING_IO_'+str(j+1)] = i['DATA_TABLE'][(j*14)+12]
                except IndexError:
                    i['DATA_TABLE_STRING_IO_'+str(j+1)] = ""
                try:
                    i['DATA_TABLE_STRING_DI_'+str(j+1)] = i['DATA_TABLE'][(j*14)+13]
                except IndexError:
                    i['DATA_TABLE_STRING_DI_'+str(j+1)] = ""
            coll_Record_2_Cat_11_FN.save(i)
        counter += 1



    # In[ ]:

    coll_Record_2_Cat_25 = db.JUP_DB_ATPCO_Record_2_Cat_25

    cur_Record_2_Cat_25 = coll_Record_2_Cat_25.find({"LAST_UPDATED_DATE": {'$gte': check_date}, "LAST_UPDATED_TIME" : {"$in" : file_time}},no_cursor_timeout=True).sort([("file_time" , pymongo.ASCENDING)])
    print ("Record_2_Cat_25:")
    ## Record 2 Cat 25
    counter = 0
    for i in cur_Record_2_Cat_25:
        if counter%1000 == 0:
            print("done: ",counter)
        try:
            if i["DATA_TABLE_STRING_RI_1"]:
                pass
        except KeyError:
            no_segs = i['NO_SEGS']
            for j in range(int(no_segs)):
                try:
                    i['DATA_TABLE_STRING_RI_'+str(j+1)] = i['DATA_TABLE'][(j*14)]
                except IndexError:
                    i['DATA_TABLE_STRING_RI_'+str(j+1)] = ""
                try:
                    i['DATA_TABLE_STRING_CAT_NO_'+str(j+1)] = i['DATA_TABLE'][(j*14)+1:(j*14)+4]
                except IndexError:
                    i['DATA_TABLE_STRING_CAT_NO_'+str(j+1)] = ""
                try:
                    i['DATA_TABLE_STRING_TBL_NO_'+str(j+1)] = i['DATA_TABLE'][(j*14)+4:(j*14)+12]
                except IndexError:
                    i['DATA_TABLE_STRING_TBL_NO_'+str(j+1)] = ""
                try:
                    i['DATA_TABLE_STRING_IO_'+str(j+1)] = i['DATA_TABLE'][(j*14)+12]
                except IndexError:
                    i['DATA_TABLE_STRING_IO_'+str(j+1)] = ""
                try:
                    i['DATA_TABLE_STRING_DI_'+str(j+1)] = i['DATA_TABLE'][(j*14)+13]
                except IndexError:
                    i['DATA_TABLE_STRING_DI_'+str(j+1)] = ""
            coll_Record_2_Cat_25.save(i)
        counter += 1


    # In[ ]:

    coll_Record_3_Cat_107 = db.JUP_DB_ATPCO_Record_3_Cat_107
    cur_Record_3_Cat_107= coll_Record_3_Cat_107.find({"LAST_UPDATED_DATE": {'$gte': check_date}, "LAST_UPDATED_TIME" : {"$in" : file_time}},no_cursor_timeout=True).sort([("file_time" , pymongo.ASCENDING)])

    print ("Record_3_Cat_107:")
    ## Record_3_Cat_107
    counter = 0
    for i in cur_Record_3_Cat_107:
        if counter%1000 == 0:
            print("done: ",counter)
        try:
            if i["PRIME_1"]:
                pass
        except KeyError:
            no_segs = i['NO_SEGS']
            for j in range(int(no_segs)):
                i['PRIME_'+str(j+1)] = i['AFTER_SEGS'][(j*13)]
                i['DEF_'+str(j+1)] = i['AFTER_SEGS'][(j*13)+1:(j*13)+2]
                i['APPL_'+str(j+1)] = i['AFTER_SEGS'][(j*13)+2:(j*13)+3]
                i['SAME_'+str(j+1)] = i['AFTER_SEGS'][(j*13)+3:(j*13)+4]
                i['TAR_NO_'+str(j+1)] = i['AFTER_SEGS'][(j*13)+4:(j*13)+7]
                i['RULE_NO_'+str(j+1)] = i['AFTER_SEGS'][(j*13)+7:(j*13)+11]
                i['GLOBAL_'+str(j+1)] = i['AFTER_SEGS'][(j*13)+11:(j*13)+13]
            coll_Record_3_Cat_107.save(i)
        counter += 1


    # In[ ]:




    # In[ ]:

    coll_Record_3_Cat_108 = db.JUP_DB_ATPCO_Record_3_Cat_108
    cur_Record_3_Cat_108= coll_Record_3_Cat_108.find({"LAST_UPDATED_DATE": {'$gte': check_date}, "LAST_UPDATED_TIME" : {"$in" : file_time}},no_cursor_timeout=True).sort([("file_time" , pymongo.ASCENDING)])
    ## Record_3_Cat_108

    print ("Record_3_Cat_108:")
    counter = 0
    for i in cur_Record_3_Cat_108:
        if counter%1000 == 0:
            print("done: ",counter)
        try:
            if i["APPL_1"]:
                pass
        except KeyError:
            no_segs = i['NO_SEGS']
            for j in range(int(no_segs)):
                try :
                    i['APPL_'+str(j+1)] = i['AFTER_SEGS'][(j*31)]
                except IndexError:
                    i['APPL_'+str(j+1)] = ""

                try:
                    i['NORM1_'+str(j+1)] = i['AFTER_SEGS'][(j*31)+1]
                except IndexError:
                    i['NORM1_'+str(j+1)] = ""
                try:
                    i['OWRT_'+str(j+1)] = i['AFTER_SEGS'][(j*31)+2]
                except IndexError:
                    i['OWRT_'+str(j+1)] = ""
                try:
                    i['SAME_'+str(j+1)] = i['AFTER_SEGS'][(j*31)+3]
                except IndexError:
                    i['SAME_'+str(j+1)] = ""
                try:
                    i['VAL_'+str(j+1)] = i['AFTER_SEGS'][(j*31)+4]
                except IndexError:
                    i['VAL_'+str(j+1)] = ""
                try:
                    i['TYPE_'+str(j+1)] = i['AFTER_SEGS'][(j*31)+5]
                except IndexError:
                    i['TYPE_'+str(j+1)] = ""
                try:
                    i['FARE_CLASS_TYPE_'+str(j+1)] = i['AFTER_SEGS'][(j*31)+6:(j*31)+14]
                except IndexError:
                    i['FARE_CLASS_TYPE_'+str(j+1)] = ""
                try:
                    i['PEN_SERVICE_CHG_APPL_'+str(j+1)] = i['AFTER_SEGS'][(j*31)+14]
                except IndexError:
                    i['PEN_SERVICE_CHG_APPL_'+str(j+1)] = ""
                try:
                    i['PEN_SERVICE_CHG_APPL_MRPH_'+str(j+1)] = i['AFTER_SEGS'][(j*31)+15]
                except IndexError:
                    i['PEN_SERVICE_CHG_APPL_MRPH_'+str(j+1)] = ""
                try:
                    i['PEN_SERVICE_CHG_APPL_APPEND_'+str(j+1)] = i['AFTER_SEGS'][(j*31)+16:(j*31)+21]
                except IndexError:
                    i['PEN_SERVICE_CHG_APPL_APPEND_'+str(j+1)] = ""
                try:
                    i['RESERVED_'+str(j+1)] = i['AFTER_SEGS'][(j*31)+21:(j*31)+31]
                except IndexError:
                    i['RESERVED_'+str(j+1)] = ""

            coll_Record_3_Cat_108.save(i)
        counter += 1


    # In[ ]:

    coll_Record_2_Cat_14_FN = db.JUP_DB_ATPCO_Record_2_Cat_14_FN
    cur_Record_2_Cat_14_FN = coll_Record_2_Cat_14_FN.find({"LAST_UPDATED_DATE": {'$gte': check_date}, "LAST_UPDATED_TIME" : {"$in" : file_time}},no_cursor_timeout=True).sort([("file_time" , pymongo.ASCENDING)])

    ## Record 2 Cat 14 FN
    print ("Record_2_Cat_14_FN:")
    counter = 0
    for i in cur_Record_2_Cat_14_FN:
        if counter%1000 == 0:
            print("done: ",counter)
        try:
            if i["DATA_TABLE_STRING_RI_1"]:
                pass
        except KeyError:
            no_segs = i['NO_SEGS']
            for j in range(int(no_segs)):
                try:
                    i['DATA_TABLE_STRING_RI_'+str(j+1)] = i['DATA_TABLE'][(j*14)]
                except IndexError:
                    i['DATA_TABLE_STRING_RI_'+str(j+1)] = ""
                try:
                    i['DATA_TABLE_STRING_CAT_NO_'+str(j+1)] = i['DATA_TABLE'][(j*14)+1:(j*14)+4]
                except IndexError:
                    i['DATA_TABLE_STRING_CAT_NO_'+str(j+1)] = ""
                try:
                    i['DATA_TABLE_STRING_TBL_NO_'+str(j+1)] = i['DATA_TABLE'][(j*14)+4:(j*14)+12]
                except IndexError:
                    i['DATA_TABLE_STRING_TBL_NO_'+str(j+1)] = ""
                try:
                    i['DATA_TABLE_STRING_IO_'+str(j+1)] = i['DATA_TABLE'][(j*14)+12]
                except IndexError:
                    i['DATA_TABLE_STRING_IO_'+str(j+1)] = ""
                try:
                    i['DATA_TABLE_STRING_DI_'+str(j+1)] = i['DATA_TABLE'][(j*14)+13]
                except IndexError:
                    i['DATA_TABLE_STRING_DI_'+str(j+1)] = ""
            coll_Record_2_Cat_14_FN.save(i)
        counter += 1


    # In[ ]:

    coll_Record_2_Cat_10 = db.JUP_DB_ATPCO_Record_2_Cat_10

    cur_Record_2_Cat_10 = coll_Record_2_Cat_10.find({"LAST_UPDATED_DATE": {'$gte': check_date}, "LAST_UPDATED_TIME" : {"$in" : file_time}},no_cursor_timeout=True).sort([("file_time" , pymongo.ASCENDING)])

    ## Record 2 Cat 10

    print ("Record_2_Cat_10:")
    counter = 0
    for i in cur_Record_2_Cat_10:
        if counter%1000 == 0:
            print("done: ",counter)
        try:
            if i["DATA_TABLE_STRING_RI_1"]:
                pass
        except KeyError:
            no_segs = i['NO_SEGS']
            for j in range(int(no_segs)):
                try:
                    i['DATA_TABLE_STRING_RI_'+str(j+1)] = i['DATA_TABLE'][(j*14)]
                except IndexError:
                    i['DATA_TABLE_STRING_RI_'+str(j+1)] = ""
                try:
                    i['DATA_TABLE_STRING_CAT_NO_'+str(j+1)] = i['DATA_TABLE'][(j*14)+1:(j*14)+4]
                except IndexError:
                    i['DATA_TABLE_STRING_CAT_NO_'+str(j+1)] = ""
                try:
                    i['DATA_TABLE_STRING_TBL_NO_'+str(j+1)] = i['DATA_TABLE'][(j*14)+4:(j*14)+12]
                except IndexError:
                    i['DATA_TABLE_STRING_TBL_NO_'+str(j+1)] = ""
                try:
                    i['DATA_TABLE_STRING_IO_'+str(j+1)] = i['DATA_TABLE'][(j*14)+12]
                except IndexError:
                    i['DATA_TABLE_STRING_IO_'+str(j+1)] = ""
                try:
                    i['DATA_TABLE_STRING_DI_'+str(j+1)] = i['DATA_TABLE'][(j*14)+13]
                except IndexError:
                    i['DATA_TABLE_STRING_DI_'+str(j+1)] = ""
            coll_Record_2_Cat_10.save(i)
        counter += 1


    # In[ ]:

    coll_Record_2_Cat_23_FN = db.JUP_DB_ATPCO_Record_2_Cat_23_FN

    cur_Record_2_Cat_23_FN = coll_Record_2_Cat_23_FN.find({"LAST_UPDATED_DATE": {'$gte': check_date}, "LAST_UPDATED_TIME" : {"$in" : file_time}},no_cursor_timeout=True).sort([("file_time" , pymongo.ASCENDING)])

    ## Record 2 Cat 23 FN
    print ("Record_2_Cat_23_FN:")
    counter = 0
    for i in cur_Record_2_Cat_23_FN:
        if counter%1000 == 0:
            print("done: ",counter)
        try:
            if i["DATA_TABLE_STRING_RI_1"]:
                pass
        except KeyError:
            no_segs = i['NO_SEGS']
            for j in range(int(no_segs)):
                try:
                    i['DATA_TABLE_STRING_RI_'+str(j+1)] = i['DATA_TABLE'][(j*14)]
                except IndexError:
                    i['DATA_TABLE_STRING_RI_'+str(j+1)] = ""
                try:
                    i['DATA_TABLE_STRING_CAT_NO_'+str(j+1)] = i['DATA_TABLE'][(j*14)+1:(j*14)+4]
                except IndexError:
                    i['DATA_TABLE_STRING_CAT_NO_'+str(j+1)] = ""
                try:
                    i['DATA_TABLE_STRING_TBL_NO_'+str(j+1)] = i['DATA_TABLE'][(j*14)+4:(j*14)+12]
                except IndexError:
                    i['DATA_TABLE_STRING_TBL_NO_'+str(j+1)] = ""
                try:
                    i['DATA_TABLE_STRING_IO_'+str(j+1)] = i['DATA_TABLE'][(j*14)+12]
                except IndexError:
                    i['DATA_TABLE_STRING_IO_'+str(j+1)] = ""
                try:
                    i['DATA_TABLE_STRING_DI_'+str(j+1)] = i['DATA_TABLE'][(j*14)+13]
                except IndexError:
                    i['DATA_TABLE_STRING_DI_'+str(j+1)] = ""
            coll_Record_2_Cat_23_FN.save(i)
        counter += 1

    coll_RBD = db.JUP_DB_ATPCO_RBD
    cur_RBD = coll_RBD.find({},no_cursor_timeout=True)
    ## RBD
    print ("RBD:")
    counter = 0
    for i in cur_RBD:
        if counter % 1000 == 0:
            print("done: ", counter)
        try:
            if i["CABIN_1"]:
                pass
        except KeyError:
            no_segs = i['NO_OF_SEGMENTS']
            i['CABIN_' + 'F'] = ''
            i['RBDs_' + 'F'] = ''
            i['CABIN_' + 'J'] = ''
            i['RBDs_' + 'J'] = ''
            i['CABIN_' + 'Y'] = ''
            i['RBDs_' + 'Y'] = ''
            for j in range(int(no_segs)):
                if i['CABIN_RBD'][(j * 21)] == "R" or i['CABIN_RBD'][(j * 21)] == "F":
                    i['CABIN_' + 'F'] = i['CABIN_' + 'F'] + str(j + 1)
                    i['RBDs_' + 'F'] = (i['RBDs_' + 'F'] + " " + i['CABIN_RBD'][(j * 21) + 1:(j * 21) + 21]).strip()
                elif i['CABIN_RBD'][(j * 21)] == "C" or i['CABIN_RBD'][(j * 21)] == "J":
                    i['CABIN_' + 'J'] = i['CABIN_' + 'J'] + str(j + 1)
                    i['RBDs_' + 'J'] = (i['RBDs_' + 'J'] + " " + i['CABIN_RBD'][(j * 21) + 1:(j * 21) + 21]).strip()
                elif i['CABIN_RBD'][(j * 21)] == "W" or i['CABIN_RBD'][(j * 21)] == "Y":
                    i['CABIN_' + 'Y'] = i['CABIN_' + 'Y'] + str(j + 1)
                    i['RBDs_' + 'Y'] = (i['RBDs_' + 'Y'] + " " + i['CABIN_RBD'][(j * 21) + 1:(j * 21) + 21]).strip()
            a1 = sum(c != ' ' for c in i['RBDs_' + 'F']) - 1
            i['RBDs_' + 'F'] = i['RBDs_' + 'F'].split(' ', a1)
            a2 = sum(c != ' ' for c in i['RBDs_' + 'J']) - 1
            i['RBDs_' + 'J'] = i['RBDs_' + 'J'].split(' ', a2)
            a3 = sum(c != ' ' for c in i['RBDs_' + 'Y']) - 1
            i['RBDs_' + 'Y'] = i['RBDs_' + 'Y'].split(' ', a3)
            i['CABIN_' + 'F'] = list(i['CABIN_' + 'F'])
            i['CABIN_' + 'J'] = list(i['CABIN_' + 'J'])
            i['CABIN_' + 'Y'] = list(i['CABIN_' + 'Y'])

            coll_RBD.save(i)
        counter += 1

    coll_Record_3_Cat_50 = db.JUP_DB_ATPCO_Record_3_Cat_50

    cur_Record_3_Cat_50 = coll_Record_3_Cat_50.find({"LAST_UPDATED_DATE": {'$gte': check_date}, "LAST_UPDATED_TIME" : {"$in" : file_time}},no_cursor_timeout=True).sort([("file_time" , pymongo.ASCENDING)])
    ## Record 3 Cat 50
    print ("Record_3_Cat_50:")
    counter = 0
    for i in cur_Record_3_Cat_50:
        if counter % 1000 == 0:
            print("done: ", counter)
        try:
            if i["GEO_APPL_1"]:
                pass
        except KeyError:
            no_segs = i['NO_SEGS']

            for j in range(int(no_segs)):
                i['GEO_APPL_' + str(j + 1)] = i['SEGS_LOC'][(j * 7)]
                i['LOC_TYPE_' + str(j + 1)] = i['SEGS_LOC'][((j * 7) + 1)]
                if (i['LOC_TYPE_' + str(j + 1)] == "A"):
                    i["GEO_LOC_AREA_" + str(j + 1)] = i["SEGS_LOC"][((j * 7) + 2)]
                elif (i['LOC_TYPE_' + str(j + 1)] == "S"):
                    i["GEO_LOC_STATE_" + str(j + 1)] = i["SEGS_LOC"][((j * 7) + 2):((j * 7) + 6)]
                elif (i['LOC_TYPE_' + str(j + 1)] == "Z"):
                    i["GEO_LOC_ZONE_" + str(j + 1)] = i["SEGS_LOC"][((j * 7) + 2):((j * 7) + 5)]
                elif (i['LOC_TYPE_' + str(j + 1)] == "C"):
                    i["GEO_LOC_CITY_" + str(j + 1)] = i["SEGS_LOC"][((j * 7) + 2):((j * 7) + 7)]
                elif (i['LOC_TYPE_' + str(j + 1)] == "N"):
                    i["GEO_LOC_COUNTRY_" + str(j + 1)] = i["SEGS_LOC"][((j * 7) + 2):((j * 7) + 4)]
                elif (i['LOC_TYPE_' + str(j + 1)] == "P"):
                    i["GEO_LOC_AIRPORT_" + str(j + 1)] = i["SEGS_LOC"][((j * 7) + 2):((j * 7) + 7)]
                elif (i['LOC_TYPE_' + str(j + 1)] == "G"):
                    i["GEO_LOC_GLOBAL_" + str(j + 1)] = i["SEGS_LOC"][((j * 7) + 2):((j * 7) + 4)]

            coll_Record_3_Cat_50.save(i)
        counter += 1

    # cr = db_fzDB.JUP_DB_ATPCO_Temp_Dates_DND.find({'file':'Recseg2.py'})
    # for i in cr:
    #     i['date'] = datetime.strftime(datetime.strptime(i['date'],'%Y-%m-%d')+timedelta(days=1),'%Y-%m-%d')
    #     i['status'] = 'To Do'
    #     db_fzDB.JUP_DB_ATPCO_Temp_Dates_DND.update({'_id':i['_id']}, i)


if __name__=='__main__':
    recseg2()
