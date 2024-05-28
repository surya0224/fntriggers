#fare control check


import pymongo
from jupiter_AI import Host_Airline_Code, Host_Airline_Hub
from pymongo import MongoClient
client = MongoClient()
import time
import re
from datetime import datetime, timedelta
from pandas import DataFrame

from jupiter_AI import client, JUPITER_DB, ATPCO_DB
db_ATPCO= client[ATPCO_DB]
db_fzDB = client[JUPITER_DB]


coll = db_fzDB.JUP_DB_ATPCO_Fares_Rules
temp=[]
#cur= rec25.find({"CXR_CODE":"FZ","LAST_UPDATED_DATE":system_date})
cur= coll.find({"carrier": Host_Airline_Code,"fare_include":True,'effective_to':None})
count=0
flist=[]
for i in cur:
    if i['fare']*i['Reference_Rate']<100.0:
        count+=1
        print ("count:", count)
        flist.append({"type":"fare","carrier":i['carrier'],"tariff_code":i['tariff_code'],"Rule_id":i['Rule_id'],"origin":i['origin'],"destination":i['destination'],"oneway_return":i['oneway_return'],"currency":i['currency'],"fare":i['fare'],"fare_basis":i['fare_basis'],"footnote":i['footnote'],"rtg":i['rtg'],"effective_from":i['effective_from'],"gfs":i['gfs']})
        #db_ATPCO.testTBD.insert({"type":"fare","carrier":i['carrier'],"tariff_code":i['tariff_code'],"Rule_id":i['Rule_id'],"origin":i['origin'],"destination":i['destination'],"oneway_return":i['oneway_return'],"currency":i['currency'],"fare":i['fare'],"fare_basis":i['fare_basis'],"footnote":i['footnote'],"rtg":i['rtg'],"effective_from":i['effective_from'],"gfs":i['gfs']})


for t in range(len(flist)):
    db_ATPCO.JUP_DB_ATPCO_control_check.insert(flist[t])

df = DataFrame(flist,columns=["carrier", "tariff_code", "Rule_id",
     "origin", "destination", "oneway_return",
     "currency", "fare", "fare_basis", "footnote",
     "rtg", "effective_from", "gfs"])

#print df.columns
df.to_excel('test_fare.xlsx', sheet_name='fare', index=False)

#--------------------------------------------------------------------------------------------------------------------------
rec25 = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_25
rec35 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_25
exr=db_fzDB.JUP_DB_Exchange_Rate
temp=[]
#cur= rec25.find({"CXR_CODE":"FZ","LAST_UPDATED_DATE":system_date})
cur= rec25.find({"CXR_CODE":Host_Airline_Code,'DATES_DISC':"0999999"})

for i in cur:

    for j in range(int(i["NO_SEGS"])):
        temp.append(i["DATA_TABLE_STRING_TBL_NO_"+str(j+1)])


temp=list(set(temp))
count=0
count1=0
dlist={}
tab_list=[]
cur2= rec35.find({'TBL_NO':{'$in':temp}})

for j in cur2:
    check=0

    if j['FARE_CAL_PERCENT']!="0000000" and int(j['FARE_CAL_PERCENT'][:3])<50:
        print "gotcha",j['FARE_CAL_PERCENT'],j['TBL_NO']
        count1+=1
        check=1

    if j['FARE_CAL_AMT']!="000000000":
        if int(j["FARE_CAL_DEC"])!=0:
            fare=float(j['FARE_CAL_AMT'][:-int(j["FARE_CAL_DEC"])]+"."+j['FARE_CAL_AMT'][-int(j["FARE_CAL_DEC"]):])
        else:
            fare = float(j['FARE_CAL_AMT'])
        fcur = exr.find({"code": j['FARE_CAL_CUR']}).limit(1)
        for x in fcur:
            fare2 = x["Reference_Rate"] * fare

        if fare2<100.0:
            print "fare less", fare2
            count+=1
            check=1

        #print j['FARE_CAL_AMT'],j["FARE_CAL_DEC"],"---->", fare,j['FARE_CAL_CUR'],fare2, j['TBL_NO']

    if j['FARE_CAL_AMOUNT']!="000000000":
        if int(j["FARE_CAL_DEC1"])!=0:
            farex=float(j['FARE_CAL_AMOUNT'][:-int(j["FARE_CAL_DEC1"])]+"."+j['FARE_CAL_AMOUNT'][-int(j["FARE_CAL_DEC1"]):])
        else:
            farex = float(j['FARE_CAL_AMOUNT'])
        fcur = exr.find({"code": j['FARE_CAL_CURR']}).limit(1)
        for x in fcur:
            fare3 = x["Reference_Rate"] * farex

        if fare3<100.0:
            print "fare less", fare3
            count+=1
            check=1

    if check==1:
        tab_list.append(j['TBL_NO'])
        dlist.update({j['TBL_NO']: {'FARE_CAL_PERCENT':j['FARE_CAL_PERCENT'],'FARE_CAL_AMT':j['FARE_CAL_AMT'],'FARE_CAL_CUR':j['FARE_CAL_CUR'],'FARE_CAL_DEC':j["FARE_CAL_DEC"],'FARE_CAL_AMOUNT':j['FARE_CAL_AMOUNT'],'FARE_CAL_CURR':j['FARE_CAL_CURR'],'FARE_CAL_DEC1':j["FARE_CAL_DEC1"]}})

#getting rec2 for dlisted tbl nos
flist=[]
curx= rec25.find({"CXR_CODE": Host_Airline_Code,'DATES_DISC':"0999999"})

for i in curx:
    for j in range(int(i["NO_SEGS"])):
        if i["DATA_TABLE_STRING_TBL_NO_"+str(j+1)] in tab_list:
            flist.append({'type':"cat25",'CAT_NO':"025",'CXR_CODE':Host_Airline_Code,'TARIFF':i['TARIFF'],'RULE_NO':i['RULE_NO'],'LOC_1':i['LOC_1'],'LOC_2':i['LOC_2'],'NO_APPL':i['NO_APPL'],'DATES_EFF_2':i['DATES_EFF_2'],'DATA_TABLE_STRING_TBL_NO_1':i["DATA_TABLE_STRING_TBL_NO_"+str(j+1)],'SEQ_NO':i['SEQ_NO'],'FARE_CAL_PERCENT':dlist[i["DATA_TABLE_STRING_TBL_NO_"+str(j+1)]]['FARE_CAL_PERCENT'],'FARE_CAL_AMT':dlist[i["DATA_TABLE_STRING_TBL_NO_"+str(j+1)]]['FARE_CAL_AMT'],'FARE_CAL_CUR':dlist[i["DATA_TABLE_STRING_TBL_NO_"+str(j+1)]]['FARE_CAL_CUR'],'FARE_CAL_DEC':dlist[i["DATA_TABLE_STRING_TBL_NO_"+str(j+1)]]['FARE_CAL_DEC'],'FARE_CAL_AMOUNT':dlist[i["DATA_TABLE_STRING_TBL_NO_"+str(j+1)]]['FARE_CAL_AMOUNT'],'FARE_CAL_CURR':dlist[i["DATA_TABLE_STRING_TBL_NO_"+str(j+1)]]['FARE_CAL_CURR'],'FARE_CAL_DEC1':dlist[i["DATA_TABLE_STRING_TBL_NO_"+str(j+1)]]['FARE_CAL_DEC1']})


for t in range(len(flist)):
    db_ATPCO.JUP_DB_ATPCO_control_check.insert(flist[t])

df = DataFrame(flist,columns=["CAT_NO", "CXR_CODE", "TARIFF",
     "RULE_NO", "LOC_1", "LOC_2",
     "NO_APPL",
     'DATES_EFF_2',"DATA_TABLE_STRING_TBL_NO_1",'SEQ_NO','FARE_CAL_PERCENT','FARE_CAL_AMT','FARE_CAL_CUR','FARE_CAL_DEC','FARE_CAL_AMOUNT','FARE_CAL_CURR','FARE_CAL_DEC1'])


df.to_excel('test_cat25.xlsx', sheet_name='cat25', index=False)

#--------------------------------------------------------------------------------------------------------
rec25 = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_All
rec35 = db_ATPCO.JUP_DB_ATPCO_Record_3_Cat_16
exr=db_fzDB.JUP_DB_Exchange_Rate
temp=[]
#cur= rec25.find({"CXR_CODE":"FZ","LAST_UPDATED_DATE":system_date})
cur= rec25.find({"CXR_CODE":Host_Airline_Code,"CAT_NO":"016",'DATES_DISC':"0999999"})

for i in cur:
    if i['RULE_NO']!="GP09" and  i['RULE_NO']!="VA99" and i['RULE_NO'][:2] in ["01","GP","62","VA"]:
        for j in range(int(i["NO_SEGS"])):
            temp.append(i["DATA_TABLE_STRING_TBL_NO_"+str(j+1)])

# print (temp)
# print len(temp)

#temp=np.unique(temp)
temp=list(set(temp))
# print len(temp)
count=0
dlist={}
tab_list=[]
cur2= rec35.find({'TBL_NO':{'$in':temp}})
# print "yo",cur2.count()
for j in cur2:
    #print j['FARE_CAL_PERCENT']
    check=0

    if j['AMT_1']!="0000000":
        if int(j["DEC_1"])!=0:
            fare=float(j['AMT_1'][:-int(j["DEC_1"])]+"."+j['AMT_1'][-int(j["DEC_1"]):])
        else:
            fare = float(j['AMT_1'])
        fcur = exr.find({"code": j['CUR_1']}).limit(1)
        for x in fcur:
            fare2 = x["Reference_Rate"] * fare

        if fare2>2000.0:
            #print "fare", fare2
            count+=1
            check=1

        #print j['FARE_CAL_AMT'],j["FARE_CAL_DEC"],"---->", fare,j['FARE_CAL_CUR'],fare2, j['TBL_NO']

    if j['AMT_2']!="0000000":
        if int(j["DEC_2"])!=0:
            farex=float(j['AMT_2'][:-int(j["DEC_2"])]+"."+j['AMT_2'][-int(j["DEC_2"]):])
        else:
            farex = float(j['AMT_2'])
        fcur = exr.find({"code": j['CUR_2']}).limit(1)
        for x in fcur:
            fare3 = x["Reference_Rate"] * farex

        if fare3>2000.0:
            #print "fare", fare3
            count+=1
            check=1
    if check==1:
        dlist.update({j['TBL_NO']: {'AMT_1':j['AMT_1'],'CUR_1':j['CUR_1'],'DEC_1':j["DEC_1"],'AMT_2':j['AMT_2'],'CUR_2':j['CUR_2'],'DEC_2':j["DEC_2"]}})
        tab_list.append(j['TBL_NO'])

# print dlist
# print tab_list


#getting rec2 for dlisted tbl nos
flist=[]
curx= rec25.find({"CXR_CODE":Host_Airline_Code,"CAT_NO":"016",'DATES_DISC':"0999999"})

for i in curx:
    for j in range(int(i["NO_SEGS"])):
        if i["DATA_TABLE_STRING_TBL_NO_"+str(j+1)] in tab_list:
            flist.append({'type':"cat16",'CAT_NO':"016",'CXR_CODE':Host_Airline_Code,'TARIFF':i['TARIFF'],'RULE_NO':i['RULE_NO'],'LOC_1':i['LOC_1'],'LOC_2':i['LOC_2'],'FARE_CLASS':i['FARE_CLASS'],'NO_APPL':i['NO_APPL'],'TYPE_CODES_SEASON_TYPE':i['TYPE_CODES_SEASON_TYPE'],'TYPE_CODES_DAY_OF_WEEK_TYPE':i['TYPE_CODES_DAY_OF_WEEK_TYPE'],'OW_RT':i['OW_RT'],'DATES_EFF_2':i['DATES_EFF_2'],'RTG_NO':i['RTG_NO'],'DATA_TABLE_STRING_TBL_NO_1':i["DATA_TABLE_STRING_TBL_NO_"+str(j+1)],'SEQ_NO':i['SEQ_NO'],'AMT_1':dlist[i["DATA_TABLE_STRING_TBL_NO_"+str(j+1)]]['AMT_1'],'CUR_1':dlist[i["DATA_TABLE_STRING_TBL_NO_"+str(j+1)]]['CUR_1'],'DEC_1':dlist[i["DATA_TABLE_STRING_TBL_NO_"+str(j+1)]]['DEC_1'],'AMT_2':dlist[i["DATA_TABLE_STRING_TBL_NO_"+str(j+1)]]['AMT_2'],'CUR_2':dlist[i["DATA_TABLE_STRING_TBL_NO_"+str(j+1)]]['CUR_2'],'DEC_2':dlist[i["DATA_TABLE_STRING_TBL_NO_"+str(j+1)]]['DEC_2']})

# print flist
# print len(flist)


for t in range(len(flist)):
    db_ATPCO.JUP_DB_ATPCO_control_check.insert(flist[t])

df = DataFrame(flist,columns=["CAT_NO", "CXR_CODE", "TARIFF",
     "RULE_NO", "LOC_1", "LOC_2",
     "FARE_CLASS", "NO_APPL", "TYPE_CODES_SEASON_TYPE", "TYPE_CODES_DAY_OF_WEEK_TYPE",
     "OW_RT", 'DATES_EFF_2',"RTG_NO", "DATA_TABLE_STRING_TBL_NO_1",'SEQ_NO','AMT_1','CUR_1','DEC_1','AMT_2','CUR_2','DEC_2'])

# print df.columns
df.to_excel('test.xlsx', sheet_name='cat16', index=False)

print "total fares:",count


