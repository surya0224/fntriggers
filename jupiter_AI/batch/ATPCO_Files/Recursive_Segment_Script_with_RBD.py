
# coding: utf-8

# In[1]:

import pip
pip.main(['install','pymongo'])
import pymongo
from pymongo import MongoClient

client = MongoClient("mongodb://dbteam:KNjSZmiaNUGLmS0Bv2@172.28.23.9:43535/")

db = client.ATPCO_new


# In[ ]:

coll_Record_0 = db.JUP_DB_ATPCO_Record_0 

cur_Record_0 = coll_Record_0.find({})

## Record 0
counter = 0
for i in cur_Record_0:
    if counter%100 == 0:
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
cur_Record1 = coll_Record1.find({})
## Record 1
counter = 0
for i in cur_Record1:
    if counter%100 == 0:
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
cur_Record_2_Cat_03_FN = coll_Record_2_Cat_03_FN.find({})

counter = 0
for i in cur_Record_2_Cat_03_FN:
    if counter%100 == 0:
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
cur_Record_2_Cat_11_FN = coll_Record_2_Cat_11_FN.find({})

## Record 2 Cat 11 FN
counter = 0
for i in cur_Record_2_Cat_11_FN:
    if counter%100 == 0:
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

cur_Record_2_Cat_25 = coll_Record_2_Cat_25.find({})

## Record 2 Cat 25
counter = 0
for i in cur_Record_2_Cat_25:
    if counter%100 == 0:
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
cur_Record_3_Cat_107= coll_Record_3_Cat_107.find({})
## Record_3_Cat_107
counter = 0
for i in cur_Record_3_Cat_107:
    if counter%100 == 0:
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
cur_Record_3_Cat_108= coll_Record_3_Cat_108.find({})
## Record_3_Cat_108
counter = 0
for i in cur_Record_3_Cat_108:
    if counter%100 == 0:
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
cur_Record_2_Cat_14_FN = coll_Record_2_Cat_14_FN.find({})

## Record 2 Cat 14 FN
counter = 0
for i in cur_Record_2_Cat_14_FN:
    if counter%100 == 0:
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

cur_Record_2_Cat_10 = coll_Record_2_Cat_10.find({})

## Record 2 Cat 10
counter = 0
for i in cur_Record_2_Cat_10:
    if counter%100 == 0:
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

cur_Record_2_Cat_23_FN = coll_Record_2_Cat_23_FN.find({})

## Record 2 Cat 23 FN
counter = 0
for i in cur_Record_2_Cat_23_FN:
    if counter%100 == 0:
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


# In[ ]:




# In[ ]:

coll_Record_2_Cat_All = db.JUP_DB_ATPCO_Record_2_Cat_All

cur_Record_2_Cat_All = coll_Record_2_Cat_All.find({})

## Record 2 Cat All
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


# In[ ]:

coll_Record_3_Cat27 = db.JUP_DB_ATPCO_Record_3_Cat_27
cur_Record_3_Cat27 = coll_Record_3_Cat27.find({})
## Record_3_Cat27
counter = 0
for i in cur_Record_3_Cat27:
    if counter%100 == 0:
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


# In[ ]:

coll_Record_3_Table_986 = db.JUP_DB_ATPCO_Record_3_Table_986
cur_Record_3_Table_986 = coll_Record_3_Table_986.find({})
## Record_3_Table_986
counter = 0
for i in cur_Record_3_Table_986:
    if counter%100 == 0:
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

coll_Record_3_Cat_50 = db.JUP_DB_ATPCO_Record_3_Cat_50 

cur_Record_3_Cat_50 = coll_Record_3_Cat_50.find({})

## Record 3 Cat 50
counter = 0
for i in cur_Record_3_Cat_50:
    if counter%100 == 0:
        print("done: ",counter)
    try:
        if i["GEO_APPL_1"]:
            pass
    except KeyError:   
        no_segs = i['SEGS']
       
        for j in range(int(no_segs)):
            i['GEO_APPL_'+str(j+1)] = i['SEGS_LOC'][(j*7)]
            i['LOC_TYPE_'+str(j+1)] = i['SEGS_LOC'][((j*7)+1)]
            if(i['LOC_TYPE_'+str(j+1)] == "A"):
                i["GEO_LOC_AREA_"+str(j+1)] = i["SEGS_LOC"][((j*7)+2)]
            elif (i['LOC_TYPE_'+str(j+1)] == "S"):
                i["GEO_LOC_STATE_"+str(j+1)] = i["SEGS_LOC"][((j*7)+2):((j*7)+6)]
            elif (i['LOC_TYPE_'+str(j+1)] == "Z"):
                i["GEO_LOC_ZONE_"+str(j+1)] = i["SEGS_LOC"][((j*7)+2):((j*7)+5)]
            elif (i['LOC_TYPE_'+str(j+1)] == "C"):
                i["GEO_LOC_CITY_"+str(j+1)] = i["SEGS_LOC"][((j*7)+2):((j*7)+7)]
            elif (i['LOC_TYPE_'+str(j+1)] == "N"):
                i["GEO_LOC_COUNTRY_"+str(j+1)] = i["SEGS_LOC"][((j*7)+2):((j*7)+4)]
            elif (i['LOC_TYPE_'+str(j+1)] == "P"):
                i["GEO_LOC_AIRPORT_"+str(j+1)] = i["SEGS_LOC"][((j*7)+2):((j*7)+7)]
            elif (i['LOC_TYPE_'+str(j+1)] == "G"):
                i["GEO_LOC_GLOBAL_"+str(j+1)] = i["SEGS_LOC"][((j*7)+2):((j*7)+4)]

        coll_Record_3_Cat_50.save(i)
    counter += 1


# In[ ]:


coll_Record_2_Cat_15_FN = db.JUP_DB_ATPCO_Record_2_Cat_15_FN

cur_Record_2_Cat_15_FN = coll_Record_2_Cat_15_FN.find({})

## Record 2 Cat 15 FN
counter = 0
for i in cur_Record_2_Cat_15_FN:
    if counter%100 == 0:
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


# In[ ]:


coll_Record_3_Cat_15 = db.JUP_DB_ATPCO_Record_3_Cat_15 

cur_Record_3_Cat_15 = coll_Record_3_Cat_15.find({})

## Record 3 Cat 15
counter = 0
for i in cur_Record_3_Cat_15:
    if counter%100 == 0:
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


# In[ ]:
coll_Record_3_Table_986 = db.JUP_DB_ATPCO_Record_3_Table_986
cur_Record_3_Table_986 = coll_Record_3_Table_986.find({})
## Record_3_Table_986
counter = 0
for i in cur_Record_3_Table_986:
    if counter%100 == 0:
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


coll_Record_40 = db.JUP_DB_ATPCO_Record_40
cur_Record_40 = coll_Record_40.find({})
## Record 40
counter = 0
for i in cur_Record_40:
    if counter%100 == 0:
        print("done: ",counter)
    try:
        if i["APPL_SUBSTRING_1"]:
            pass
    except KeyError:
        no_segs = i['NO_SEGS']
        for j in range(int(no_segs)):
            i['TEXT_DATA_SUBSTRING_'+str(j+1)] = i['TEXT_DATA'][j + j * 63:(j+1) + 63 * (j+1)]
        coll_Record_40.save(i)
    counter += 1

    
# In[ ]:

coll_YQYR_Table_190 = db.JUP_DB_ATPCO_YQYR_Table_190
cur_YQYR_Table_190 = coll_YQYR_Table_190.find({})
## YQYR Table 190
counter = 0 
for i in cur_YQYR_Table_190:
    if counter%100 == 0:
        print("done: ",counter)
    try:
        if i["APPL_SUBSTRING_1"]:
            pass
    except KeyError:    
        no_segs = i['NO_OF_SEGS']
        for j in range(int(no_segs)):
            i['APPL_SUBSTRING_'+str(j+1)] = i['APPL_CXR'][j*4]
            i['CXR_SUBSTRING_'+str(j+1)] = i['APPL_CXR'][(j*4)+1:(j*4)+4]
        coll_YQYR_Table_190.save(i)
    counter += 1


# In[ ]:

coll_YQYR_Table_186 = db.JUP_DB_ATPCO_YQYR_Table_186
cur_YQYR_Table_186 = coll_YQYR_Table_186.find({})
## YQYR Table 186
counter = 0
for i in cur_YQYR_Table_186:
    if counter%100 == 0:
        print("done: ",counter)
    try:
        if i["MKT_CXR_SUBSTRING_1"]:
            pass
    except KeyError:    
        no_segs = i['NO_SEGS']
        for j in range(int(no_segs)):
            i['MKT_CXR_SUBSTRING_'+str(j+1)] = i['CARRIER'][(j*14):(j*14)+3]
            i['OPT_CXR_SUBSTRING_'+str(j+1)] = i['CARRIER'][(j*14)+3:(j*14)+6]
            i['FLT_NO_1_SUBSTRING_'+str(j+1)] = i['CARRIER'][(j*14)+6:(j*14)+10]
            i['FLT_NO_2_SUBSTRING_'+str(j+1)] = i['CARRIER'][(j*14)+10:(j*14)+14]
        coll_YQYR_Table_186.save(i)
    counter += 1


    # In[ ]:

coll_Record_20_Cat_25 = db.JUP_DB_ATPCO_Record_20_Cat_25
cur_Record_20_Cat_25 = coll_Record_20_Cat_25.find({})
## Record 20 Cat 25
counter = 0
for i in cur_Record_20_Cat_25:
    if counter%100 == 0:
        print("done: ",counter)
    try:
        if i["CAT_ID_TEXT_SUBSTRING_1"]:
            pass
    except KeyError:    
        no_segs = i['NO_SEGS']
        for j in range(int(no_segs)):
            i['CAT_ID_TEXT_SUBSTRING_'+str(j+1)] = i['CATEGORY_ID_TEXT'][(j*64):(j*64)+64]
        coll_Record_20_Cat_25.save(i)
    counter += 1

# In[ ]:

coll_Record_20_Cat_10 = db.JUP_DB_ATPCO_Record_20_Cat_10
cur_Record_20_Cat_10 = coll_Record_20_Cat_10.find({})
## Record 20 Cat 10
counter = 0
for i in cur_Record_20_Cat_10:
    if counter%100 == 0:
        print("done: ",counter)
    try:
        if i["CAT_ID_TEXT_SUBSTRING_1"]:
            pass
    except KeyError:    
        no_segs = i['NO_SEGS']
        for j in range(int(no_segs)):
            i['CAT_ID_TEXT_SUBSTRING_'+str(j+1)] = i['CAT_ID_TEXT'][(j*64):(j*64)+64]
        coll_Record_20_Cat_10.save(i)
    counter += 1


# In[6]:

coll_Record_20_All = db.JUP_DB_ATPCO_Record_20_All
cur_Record_20_All = coll_Record_20_All.find({})
## Record 20 Cat All
counter = 0
for i in cur_Record_20_All:
    if counter%100 == 0:
        print("done: ",counter)
    try:
        if i["CAT_ID_TEXT_SUBSTRING_1"]:
            pass
    except KeyError:    
        no_segs = i['NO_SEGS']
        for j in range(int(no_segs)):
            i['CAT_ID_TEXT_SUBSTRING_'+str(j+1)] = i['CATEGORY_ID_TEXT'][(j*64):(j*64)+64]
        coll_Record_20_All.save(i)
    counter += 1


coll_RBD = db.JUP_DB_ATPCO_RBD
cur_RBD = coll_RBD.find({})
## RBD
counter = 0
for i in cur_RBD:
    if counter%100 == 0:
        print("done: ",counter)
    try:
        if i["CABIN_1"]:
            pass
    except KeyError:    
        no_segs = i['NO_SEGS']
        i['CABIN_'+'F']=''
        i['RBDs_'+'F']=''
        i['CABIN_'+'J']=''
        i['RBDs_'+'J']=''
        i['CABIN_'+'Y']=''
        i['RBDs_'+'Y']=''
        for j in range(int(no_segs)):
            if i['CABIN_RBD'][(j*21)] =="R" or i['CABIN_RBD'][(j*21)] =="F":
                i['CABIN_'+'F'] =i['CABIN_'+'F']+str(j+1)
                i['RBDs_'+'F'] =(i['RBDs_'+'F']+" "+i['CABIN_RBD'][(j*21)+1:(j*21)+21]).strip()
            elif i['CABIN_RBD'][(j*21)] =="C" or i['CABIN_RBD'][(j*21)] =="J":
                i['CABIN_'+'J'] =i['CABIN_'+'J']+str(j+1)
                i['RBDs_'+'J'] =(i['RBDs_'+'J']+" "+i['CABIN_RBD'][(j*21)+1:(j*21)+21]).strip()
            elif i['CABIN_RBD'][(j*21)] =="W" or i['CABIN_RBD'][(j*21)] =="Y":
                i['CABIN_'+'Y'] =i['CABIN_'+'Y']+str(j+1)
                i['RBDs_'+'Y'] =(i['RBDs_'+'Y']+" "+i['CABIN_RBD'][(j*21)+1:(j*21)+21]).strip()
        a1= sum(c != ' ' for c in i['RBDs_'+'F']) -1        
        i['RBDs_'+'F'] = i['RBDs_'+'F'].split(' ',a1)
        a2= sum(c != ' ' for c in i['RBDs_'+'J']) -1        
        i['RBDs_'+'J'] = i['RBDs_'+'J'].split(' ',a2)
        a3= sum(c != ' ' for c in i['RBDs_'+'Y']) -1        
        i['RBDs_'+'Y'] = i['RBDs_'+'Y'].split(' ',a3)
        i['CABIN_'+'F'] = list(i['CABIN_'+'F'])
        i['CABIN_'+'J'] = list(i['CABIN_'+'J'])
        i['CABIN_'+'Y'] = list(i['CABIN_'+'Y'])

        
        coll_RBD.save(i)
    counter += 1
    
    
coll_Record8 = db.JUP_DB_ATPCO_Record_8
cur_Record8 = coll_Record8.find({})
## Record 8
counter = 0
for i in cur_Record8:
    if counter%100 == 0:
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
    
    
    
coll_Record65 = db.JUP_DB_ATPCO_Record_65
cur_Record65 = coll_Record65.find({})
## Record 65
counter = 0
for i in cur_Record65:
    if counter%100 == 0:
        print("done: ",counter)
    try:
        if i["LINE_DATA_1"]:
            pass
    except KeyError:    
        no_segs = i['NO_SEGS']
        for j in range(int(no_segs)):
            i['LINE_DATA_'+str(j+1)] = i['LINE_DATA'][(j*64):(j*64)+64]
        coll_Record65.save(i)
    counter += 1
    
    
    
    