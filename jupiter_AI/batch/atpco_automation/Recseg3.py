from jupiter_AI import ATPCO_DB, JUPITER_DB, JUPITER_LOGGER
from jupiter_AI.logutils import measure
import pymongo
from pymongo import MongoClient
import time
from datetime import datetime, timedelta


@measure(JUPITER_LOGGER)
def recseg3(system_date,file_time, client):
    db = client[ATPCO_DB]
    db_fzDB = client[JUPITER_DB]

    # cr = db_fzDB.JUP_DB_ATPCO_Temp_Dates_DND.find({'file':'Recseg3.py'})
    # for i in cr:
    #     system_date = i['date']
    system_date = datetime.strptime(system_date,'%Y-%m-%d')
    print system_date
    query_date = datetime.strftime(system_date, "%Y%m%d")
    check_date = datetime.strftime(system_date, "%Y-%m-%d")
    # db_fzDB.JUP_DB_ATPCO_Temp_Dates_DBD.update({'file':'Recseg3.py'},{'$set':{'status':'In Progress'}})

    # In[ ]:

    coll_Record_2_Cat_All = db.JUP_DB_ATPCO_Record_2_Cat_All

    cur_Record_2_Cat_All = coll_Record_2_Cat_All.find({"LAST_UPDATED_DATE": {'$gte': check_date}, "LAST_UPDATED_TIME" : {"$in" : file_time}}, no_cursor_timeout=True).sort([("file_time" , pymongo.ASCENDING)])

    ## Record 2 Cat All
    print ("Record_2_Cat_All:")
    counter = 0
    for i in cur_Record_2_Cat_All:
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
            coll_Record_2_Cat_All.save(i)
        counter += 1
    cur_Record_2_Cat_All.close()

    # In[ ]:

    coll_Record_3_Cat27 = db.JUP_DB_ATPCO_Record_3_Cat_27
    cur_Record_3_Cat27 = coll_Record_3_Cat27.find({"LAST_UPDATED_DATE": {'$gte': check_date},"LAST_UPDATED_TIME" : {"$in" : file_time}}, no_cursor_timeout=True).sort([("file_time" , pymongo.ASCENDING)])
    ## Record_3_Cat27
    print ("Record_3_Cat_27:")

    counter = 0
    for i in cur_Record_3_Cat27:
        if counter%1000 == 0:
            print("done: ",counter)
        try:
            if i["MIN_GRND_AMT_1"]:
                pass
        except KeyError:
            no_segs = i['NO_SEGS']
            for j in range(int(no_segs)):
                i['MIN_GRND_AMT_'+str(j+1)] = i['SEGS_COLUMN'][(j*83)]
                i['GROUND_CRUISE_CUR1_'+str(j+1)] = i['SEGS_COLUMN'][(j*83)+1:(j*83)+11]
                i['GROUND_CRUISE_DEC_'+str(j+1)] = i['SEGS_COLUMN'][(j*83)+11:(j*83)+12]
                i['GROUND_CUISE_FILLER1_'+str(j+1)] = i['SEGS_COLUMN'][(j*83)+12:(j*83)+22]
                i['GROUND_CUISE_FILLER2_'+str(j+1)] = i['SEGS_COLUMN'][(j*83)+22:(j*83)+23]
                i['GROUND_CUISE_FILLER3_'+str(j+1)] = i['SEGS_COLUMN'][(j*83)+23:(j*83)+26]
                i['GROUND_CUISE_APPL_'+str(j+1)] = i['SEGS_COLUMN'][(j*83)+26:(j*83)+28]
                i['GROUND_CUISE_TRANS_'+str(j+1)] = i['SEGS_COLUMN'][(j*83)+28:(j*83)+30]
                i['GROUND_CUISE_FILLER4_'+str(j+1)] = i['SEGS_COLUMN'][(j*83)+30:(j*83)+33]
                i['CUISE_CAR_'+str(j+1)] = i['SEGS_COLUMN'][(j*83)+33:(j*83)+41]
                i['CUISE_FILLER5_'+str(j+1)] = i['SEGS_COLUMN'][(j*83)+41:(j*83)+57]
                i['CUISE_RESORT_'+str(j+1)] = i['SEGS_COLUMN'][(j*83)+57:(j*83)+65]
                i['CUISE_SHIP_'+str(j+1)] = i['SEGS_COLUMN'][(j*83)+65:(j*83)+73]
                i['CUISE_OTHER_'+str(j+1)] = i['SEGS_COLUMN'][(j*83)+73:(j*83)+83]
                i['CUISE_FREE_'+str(j+1)] = i['SEGS_COLUMN'][(j*83)+73:(j*83)+83]
                i['CUISE_FILLER6_'+str(j+1)] = i['SEGS_COLUMN'][(j*83)+73:(j*83)+83]
                i['CUISE_FILLER7_'+str(j+1)] = i['SEGS_COLUMN'][(j*83)+73:(j*83)+83]
            coll_Record_3_Cat27.save(i)
        counter += 1

    cur_Record_3_Cat27.close()

    # In[ ]:

    coll_Record_3_Table_986 = db.JUP_DB_ATPCO_Record_3_Table_986
    cur_Record_3_Table_986 = coll_Record_3_Table_986.find({"LAST_UPDATED_DATE": {'$gte': check_date},"LAST_UPDATED_TIME" : {"$in" : file_time}}, no_cursor_timeout=True).sort([("file_time" , pymongo.ASCENDING)])
    ## Record_3_Table_986

    print ("Record_3_Table_986:")
    counter = 0
    for i in cur_Record_3_Table_986:
        if counter%1000 == 0:
            print("done: ",counter)
        try:
            if i["MARKETING_CARRIER_1"]:
                pass
        except KeyError:
            no_segs = i['NO_SEGS']
            for j in range(int(no_segs)):
                i['MARKETING_CARRIER_'+str(j+1)] = i['SEGS_CARRIER'][(j*14):(j*14)+3]
                i['OPERATING_CARRIER_'+str(j+1)] = i['SEGS_CARRIER'][(j*14)+3:(j*14)+6]
                i['FLT_NO_1_'+str(j+1)] = i['SEGS_CARRIER'][(j*14)+6:(j*14)+10]
                i['FLT_NO_2_'+str(j+1)] = i['SEGS_CARRIER'][(j*14)+10:(j*14)+14]

            coll_Record_3_Table_986.save(i)
        counter += 1

    # In[2]:
    cur_Record_3_Table_986.close()

    # In[ ]:


    coll_Record_2_Cat_15_FN = db.JUP_DB_ATPCO_Record_2_Cat_15_FN

    cur_Record_2_Cat_15_FN = coll_Record_2_Cat_15_FN.find({"LAST_UPDATED_DATE": {'$gte': check_date},"LAST_UPDATED_TIME" : {"$in" : file_time}}, no_cursor_timeout=True).sort([("file_time" , pymongo.ASCENDING)])

    ## Record 2 Cat 15 FN

    print ("Record_2_Cat_15_FN:")
    counter = 0
    for i in cur_Record_2_Cat_15_FN:
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
            coll_Record_2_Cat_15_FN.save(i)
        counter += 1

    cur_Record_2_Cat_15_FN.close()
    # In[ ]:


    coll_Record_3_Cat_15 = db.JUP_DB_ATPCO_Record_3_Cat_15

    cur_Record_3_Cat_15 = coll_Record_3_Cat_15.find({"LAST_UPDATED_DATE": {'$gte': check_date},"LAST_UPDATED_TIME" : {"$in" : file_time}}, no_cursor_timeout=True).sort([("file_time" , pymongo.ASCENDING)])

    ## Record 3 Cat 15
    print ("Record_3_Cat_15:")
    counter = 0
    for i in cur_Record_3_Cat_15:
        if counter%1000 == 0:
            print("done: ",counter)
        try:
            if i["LOCALE_APPL_1"]:
                pass
        except KeyError:
            no_segs = i['NO_SEGS']
            if no_segs!="000" :
                for j in range(int(no_segs)):
                    try:
                        i['LOCALE_APPL_'+str(j+1)] = i['AFTER_SEGS'][(j*16)]
                    except IndexError:
                        i['LOCALE_APPL_'+str(j+1)] =""
                    try:
                        i['LOC_TYPE_'+str(j+1)] = i['AFTER_SEGS'][((j*16)+1)]
                    except IndexError:
                         i['LOC_TYPE_'+str(j+1)] =""
                    try:
                        if(i['LOC_TYPE_'+str(j+1)] == "A"):
                            i["LOC_1_AREA_"+str(j+1)] = i["AFTER_SEGS"][((j*16)+2)]
                        elif (i['LOC_TYPE_'+str(j+1)] == "S"):
                            i["LOC_1_STATE_"+str(j+1)] = i["AFTER_SEGS"][((j*16)+2):((j*16)+4)]
                        elif (i['LOC_TYPE_'+str(j+1)] == "Z"):
                            i["LOC_1_ZONE_"+str(j+1)] = i["AFTER_SEGS"][((j*16)+2):((j*16)+5)]
                        elif (i['LOC_TYPE_'+str(j+1)] == "C"):
                            i["LOC_1_CITY_"+str(j+1)] = i["AFTER_SEGS"][((j*16)+2):((j*16)+7)]
                        elif (i['LOC_TYPE_'+str(j+1)] == "N"):
                            i["LOC_1_COUNTRY_"+str(j+1)] = i["AFTER_SEGS"][((j*16)+2):((j*14)+4)]
                        elif (i['LOC_TYPE_'+str(j+1)] == "P"):
                            i["LOC_1_AIRPORT_"+str(j+1)] = i["AFTER_SEGS"][((j*16)+2):((j*16)+7)]
                        elif (i['LOC_TYPE_'+str(j+1)] == "T"):
                            i["TRAVEL_AGENCY1_"+str(j+1)] = i["AFTER_SEGS"][((j*16)+2):((j*16)+7)]
                        elif (i['LOC_TYPE_'+str(j+1)] == "I"):
                            i["IATA_TRAVEL_AGENCY NO1_"+str(j+1)] = i["AFTER_SEGS"][((j*16)+2):((j*16)+9)]
                        elif (i['LOC_TYPE_'+str(j+1)] == "H"):
                            i["HOME_IATA_AGENCY_NO1_"+str(j+1)] = i["AFTER_SEGS"][((j*16)+2):((j*16)+9)]
                        elif (i['LOC_TYPE_'+str(j+1)] == "U"):
                            i["HOME_TRAVEL_AGENCY_CODE1_"+str(j+1)] = i["AFTER_SEGS"][((j*16)+2):((j*16)+7)]
                        elif (i['LOC_TYPE_'+str(j+1)] == "X"):
                            i["DEPARTMENT_IDENTIFIER1_"+str(j+1)] = i["AFTER_SEGS"][((j*16)+2):((j*16)+9)]
                        elif (i['LOC_TYPE_'+str(j+1)] == "V"):
                            i["CRS_CXR_DEPARTMENT_CODE1_"+str(j+1)] = i["AFTER_SEGS"][((j*16)+2):((j*16)+7)]
                    except IndexError:
                        print("")
                    try:
                        if i["AFTER_SEGS"][((j*16)+9)]!="":
                            if(i['LOC_TYPE_'+str(j+1)] == "A"):

                                i["LOC_2_AREA_"+str(j+1)] = i["AFTER_SEGS"][((j*16)+9)]

                            elif (i['LOC_TYPE_'+str(j+1)] == "S"):
                                i["LOC_2_STATE_"+str(j+1)] = i["AFTER_SEGS"][((j*16)+9):((j*16)+11)]
                            elif (i['LOC_TYPE_'+str(j+1)] == "Z"):
                                i["LOC_2_ZONE_"+str(j+1)] = i["AFTER_SEGS"][((j*16)+9):((j*16)+12)]
                            elif (i['LOC_TYPE_'+str(j+1)] == "C"):
                                i["LOC_2_CITY_"+str(j+1)] = i["AFTER_SEGS"][((j*16)+9):((j*16)+16)]
                            elif (i['LOC_TYPE_'+str(j+1)] == "N"):
                                i["LOC_2_COUNTRY_"+str(j+1)] = i["AFTER_SEGS"][((j*16)+9):((j*16)+11)]
                            elif (i['LOC_TYPE_'+str(j+1)] == "P"):
                                i["LOC_2_AIRPORT_"+str(j+1)] = i["AFTER_SEGS"][((j*16)+9):((j*16)+14)]
                            elif (i['LOC_TYPE_'+str(j+1)] == "T"):
                                i["TRAVEL_AGENCY2_"+str(j+1)] = i["AFTER_SEGS"][((j*16)+9):((j*16)+14)]
                            elif (i['LOC_TYPE_'+str(j+1)] == "I"):
                                i["IATA_TRAVEL_AGENCY NO2_"+str(j+1)] = i["AFTER_SEGS"][((j*16)+9):((j*16)+16)]
                            elif (i['LOC_TYPE_'+str(j+1)] == "H"):
                                i["HOME_IATA_AGENCY_NO2_"+str(j+1)] = i["AFTER_SEGS"][((j*16)+9):((j*16)+16)]
                            elif (i['LOC_TYPE_'+str(j+1)] == "U"):
                                i["HOME_TRAVEL_AGENCY_CODE2_"+str(j+1)] = i["AFTER_SEGS"][((j*16)+9):((j*16)+14)]
                            elif (i['LOC_TYPE_'+str(j+1)] == "X"):
                                i["DEPARTMENT_IDENTIFIER2_"+str(j+1)] = i["AFTER_SEGS"][((j*16)+9):((j*16)+16)]
                            elif (i['LOC_TYPE_'+str(j+1)] == "V"):
                                i["CRS_CXR_DEPARTMENT_COD2E_"+str(j+1)] = i["AFTER_SEGS"][((j*16)+9):((j*16)+14)]
                    except IndexError:
                        print ("")
            coll_Record_3_Cat_15.save(i)
        counter += 1

    cur_Record_3_Cat_15.close()

    coll_Record8 = db.JUP_DB_ATPCO_Record_8
    cur_Record8 = coll_Record8.find({"LAST_UPDATED_DATE": {'$gte': check_date},"LAST_UPDATED_TIME" : {"$in" : file_time}}, no_cursor_timeout=True).sort([("file_time" , pymongo.ASCENDING)])
    ## Record 8
    print ("Record_8:")
    counter = 0
    for i in cur_Record8:
        if counter%1000 == 0:
            print("done: ",counter)
        try:
            if i["SEC_PASS_TYPE_1"]:
                pass
        except KeyError:
            no_segs = i['NO_SEGS']
            for j in range(int(no_segs)):
                i['SEC_PASS_TYPE_'+str(j+1)] = i['SEGS_COLUMN'][(j*7):(j*7)+3]
                i['SEGS_RESERVED_'+str(j+1)] = i['SEGS_COLUMN'][(j*7)+3:(j*7)+7]
            coll_Record8.save(i)
        counter += 1


    cur_Record8.close()
    # cr = db_fzDB.JUP_DB_ATPCO_Temp_Dates_DND.find({'file':'Recseg3.py'})
    # for i in cr:
    #     i['date'] = datetime.strftime(datetime.strptime(i['date'],'%Y-%m-%d')+timedelta(days=1),'%Y-%m-%d')
    #     i['status'] = 'To Do'
    #     db_fzDB.JUP_DB_ATPCO_Temp_Dates_DND.update({'_id':i['_id']}, i)


if __name__=='__main__':
    recseg3()