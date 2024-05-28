
from datetime import datetime
# from sklearn.cluster import KMeans
import pandas as pd
import numpy as np
import os
from sklearn import datasets, linear_model
from sklearn.model_selection import train_test_split
# from matplotlib import pyplot as plt

from pymongo import MongoClient
from pymongo import UpdateOne
from bson import ObjectId
import time
from sklearn.cluster import MiniBatchKMeans
from sklearn.externals import joblib
from jupiter_AI import JUPITER_DB, client
db = client[JUPITER_DB]

# model_f = joblib.load("mainmodel_Sales.sav")

skips = 0
limits = 1000000
loop_coun = db.JUP_DB_Sales.find({}).count()
# loop_coun = len(loop_cou)
loop_count = int(loop_coun/limits)
loop_count = loop_count+2

flight_duration_minutes_list = []
Dept_Sta_list = []
Arvl_Sta_list = []
print "making duration df"
#Find the required columns from the Capacity collection
cursor = db.JUP_DB_OD_Duration.find({},no_cursor_timeout=True)
#print "length: ", cursor.count()
for i in cursor:
    Dept_Sta = i["Dept Sta"]
    Dept_Sta_list.append(Dept_Sta)
    Arvl_Sta = i["Arvl Sta"]
    Arvl_Sta_list.append(Arvl_Sta)
    flight_duration_minutes = i["flight_duration_minutes"]
    flight_duration_minutes_list.append(flight_duration_minutes)
#Defining a new dataframe with the required columns
Duration = pd.DataFrame({'flight_duration_minutes' : flight_duration_minutes_list, 'Dept_Sta' : Dept_Sta_list, 'Arvl_Sta' : Arvl_Sta_list})
Duration['OD'] = Duration['Dept_Sta'].map(str) + Duration['Arvl_Sta']
Duration.drop(Duration.columns[0:2], axis=1, inplace=True)
Duration.columns = ['Time', 'OD']
pd.set_option("display.max_columns", 100)
#Group all the similar ODs and find the OD which has the minimum time
Duration11 = Duration.groupby(by = 'OD', as_index = False)['Time'].mean()
print Duration11.head()
print Duration11.shape
# Duration = Duration11
# model = joblib.load("mainmodel-MBKM1.sav")
cursor.close()

train11_mean = []
train22_median = []
train33_mean = []
train44_mean = []
train55_mean = []
train66_mean = []
train77_mean = []
train88_mean = []
train99_mean = []

final_df = pd.DataFrame()
final_df['SEGMENT'] = [0,1,2,3,4,5]
final_df['Duration'] = [0,0,0,0,0,0]
#final_df['DOW'] = [0,0,0,0,0,0]
final_df['Pax'] = [0,0,0,0,0,0]
final_df['Duration_Index'] = [0,0,0,0,0,0]
final_df['Channel_GDS'] = [0,0,0,0,0,0]
final_df['Channel_WEB'] = [0,0,0,0,0,0]
final_df['Compartment_J'] = [0,0,0,0,0,0]
final_df['Compartment_Y'] = [0,0,0,0,0,0]
final_df['Advance_Booking'] = [0,0,0,0,0,0]

# from sklearn.externals import joblib
# loop2
skips = 0
limits = 1000000

seg_0 = [0, 0, 0, 0, 0, 0, 0]
seg_1 = [0, 0, 0, 0, 0, 0, 0]
seg_2 = [0, 0, 0, 0, 0, 0, 0]
seg_3 = [0, 0, 0, 0, 0, 0, 0]
seg_4 = [0, 0, 0, 0, 0, 0, 0]
seg_5 = [0, 0, 0, 0, 0, 0, 0]

for turns in range(1, loop_count):
    cursor = db.JUP_DB_Sales.find({}, no_cursor_timeout=True).skip(skips).limit(limits)
    dep_date_list = []
    book_date_list = []
    month_list = []
    pax_list = []
    compartment_list = []
    channel_list = []
    revenue_list = []
    od_list = []
    duration_list = []
    _id_list = []
    for i in cursor:
        dep_date = i['dep_date']
        dep_date_list.append(dep_date)
        book_date = i['book_date']
        book_date_list.append(book_date)
        pax = i['pax']
        pax_list.append(pax)
        compartment = i['compartment']
        compartment_list.append(compartment)
        channel = i['channel']
        channel_list.append(channel)
        revenue = i['revenue']
        revenue_list.append(revenue)
        id = i['_id']
        _id_list.append(id)
        od = i['od']
        od_list.append(od)
        month = i['month']
        month_list.append(month)
    day_of_week_list = []
    bookdate_list = []
    depdate_list = []
    for each_dep in dep_date_list:
        eachdep = datetime.strptime(each_dep, '%Y-%m-%d')
        depdate_list.append(eachdep)
        day_of_week = eachdep.isoweekday()
        day_of_week_list.append(day_of_week)
    for each_book in book_date_list:
        eachbook = datetime.strptime(each_book, '%Y-%m-%d')
        bookdate_list.append(eachbook)
    advance_booking_list = []
    for i in range(len(dep_date_list)):
        advance_booking = depdate_list[i] - bookdate_list[i]
        advance_booking_list.append(advance_booking.days)
    df1 = pd.DataFrame({'OD': od_list, 'Book_Date': book_date_list, 'id': _id_list, 'Dep_Date': dep_date_list,
                        'Advance_Booking': advance_booking_list, 'Pax': pax_list, 'Compartment': compartment_list,
                        'Channel': channel_list, 'Fare': revenue_list, 'DOW': day_of_week_list,
                        'Month': month_list})
    # df1.to_csv("salesf1.csv", index=False)
    salesf = pd.merge(df1, Duration11, how='left')
    # print (salesf.head())
    df = salesf
    # Dropping NA values from the 'Compartment' column
    df212 = df.dropna(subset=['Compartment'])
    try:
        df212 = df212[df212.Compartment != 'Others']
    except:
        pass
    # Changing all the values other than WEB to GDS in 'Channel' column
    try:
        df212.loc[df212['Channel'] != 'WEB', 'Channel'] = 'GDS'
    except:
        pass
    # Getting dummies for the categorical varibale columns - 'Compartment' and 'Channel'
    df2 = pd.get_dummies(df212, columns=['Channel', "Compartment"])
    # Deleting the original columns after creating dummies
    del (df212['Compartment'], df212['Channel'])
    print(len(df2))
    df2['Duration'] = df2['Time']
    df2.drop(df2.columns[9], axis=1, inplace=True)
    print df2.head()
    df2['Duration'] = df2['Duration'].fillna(df2['Duration'].mean())
    df2['Duration_Index'] = df2['Fare'] / df2['Duration']
    print df2.isnull().sum()
    df3 = pd.DataFrame()
    # Taking only the columns required for applying KMeans into a new dataframe
    try:
        df3 = df2[['Advance_Booking', 'Channel_GDS', 'Channel_WEB', 'Compartment_J', 'Compartment_Y', 'DOW', 'Duration',
                   'Pax', 'Duration_Index']]
        print(len(df3))
    except:
        df3 = df2[['Advance_Booking', 'Channel_GDS', 'Compartment_J', 'Compartment_Y', 'DOW', 'Duration', 'Pax',
                   'Duration_Index']]
        print(len(df3))
    model_f = joblib.load('mainmodel_Sales.sav')
    predict = model_f.predict(df3)
    print(predict)
    # Assigning the column SEGMENT in the testing data which contains the cluster number
    df5 = df3.assign(SEGMENT=predict)
    df6 = df5.copy()
    train_11 = df6.groupby(by='SEGMENT', as_index=False)['Duration'].sum()
    print train_11
    temp_df = pd.DataFrame()
    temp_df = train_11.copy()
    final_df['Duration'] = temp_df['Duration'] + final_df['Duration']

    seg_0[0] += len(df6[(df6['SEGMENT'] == 0) & (df6['DOW'] == 1)])
    seg_0[1] += len(df6[(df6['SEGMENT'] == 0) & (df6['DOW'] == 2)])
    seg_0[2] += len(df6[(df6['SEGMENT'] == 0) & (df6['DOW'] == 3)])
    seg_0[3] += len(df6[(df6['SEGMENT'] == 0) & (df6['DOW'] == 4)])
    seg_0[4] += len(df6[(df6['SEGMENT'] == 0) & (df6['DOW'] == 5)])
    seg_0[5] += len(df6[(df6['SEGMENT'] == 0) & (df6['DOW'] == 6)])
    seg_0[6] += len(df6[(df6['SEGMENT'] == 0) & (df6['DOW'] == 7)])

    seg_1[0] += len(df6[(df6['SEGMENT'] == 1) & (df6['DOW'] == 1)])
    seg_1[1] += len(df6[(df6['SEGMENT'] == 1) & (df6['DOW'] == 2)])
    seg_1[2] += len(df6[(df6['SEGMENT'] == 1) & (df6['DOW'] == 3)])
    seg_1[3] += len(df6[(df6['SEGMENT'] == 1) & (df6['DOW'] == 4)])
    seg_1[4] += len(df6[(df6['SEGMENT'] == 1) & (df6['DOW'] == 5)])
    seg_1[5] += len(df6[(df6['SEGMENT'] == 1) & (df6['DOW'] == 6)])
    seg_1[6] += len(df6[(df6['SEGMENT'] == 1) & (df6['DOW'] == 7)])

    seg_2[0] += len(df6[(df6['SEGMENT'] == 2) & (df6['DOW'] == 1)])
    seg_2[1] += len(df6[(df6['SEGMENT'] == 2) & (df6['DOW'] == 2)])
    seg_2[2] += len(df6[(df6['SEGMENT'] == 2) & (df6['DOW'] == 3)])
    seg_2[3] += len(df6[(df6['SEGMENT'] == 2) & (df6['DOW'] == 4)])
    seg_2[4] += len(df6[(df6['SEGMENT'] == 2) & (df6['DOW'] == 5)])
    seg_2[5] += len(df6[(df6['SEGMENT'] == 2) & (df6['DOW'] == 6)])
    seg_2[6] += len(df6[(df6['SEGMENT'] == 2) & (df6['DOW'] == 7)])

    seg_3[0] += len(df6[(df6['SEGMENT'] == 3) & (df6['DOW'] == 1)])
    seg_3[1] += len(df6[(df6['SEGMENT'] == 3) & (df6['DOW'] == 2)])
    seg_3[2] += len(df6[(df6['SEGMENT'] == 3) & (df6['DOW'] == 3)])
    seg_3[3] += len(df6[(df6['SEGMENT'] == 3) & (df6['DOW'] == 4)])
    seg_3[4] += len(df6[(df6['SEGMENT'] == 3) & (df6['DOW'] == 5)])
    seg_3[5] += len(df6[(df6['SEGMENT'] == 3) & (df6['DOW'] == 6)])
    seg_3[6] += len(df6[(df6['SEGMENT'] == 3) & (df6['DOW'] == 7)])

    seg_4[0] += len(df6[(df6['SEGMENT'] == 4) & (df6['DOW'] == 1)])
    seg_4[1] += len(df6[(df6['SEGMENT'] == 4) & (df6['DOW'] == 2)])
    seg_4[2] += len(df6[(df6['SEGMENT'] == 4) & (df6['DOW'] == 3)])
    seg_4[3] += len(df6[(df6['SEGMENT'] == 4) & (df6['DOW'] == 4)])
    seg_4[4] += len(df6[(df6['SEGMENT'] == 4) & (df6['DOW'] == 5)])
    seg_4[5] += len(df6[(df6['SEGMENT'] == 4) & (df6['DOW'] == 6)])
    seg_4[6] += len(df6[(df6['SEGMENT'] == 4) & (df6['DOW'] == 7)])

    seg_5[0] += len(df6[(df6['SEGMENT'] == 5) & (df6['DOW'] == 1)])
    seg_5[1] += len(df6[(df6['SEGMENT'] == 5) & (df6['DOW'] == 2)])
    seg_5[2] += len(df6[(df6['SEGMENT'] == 5) & (df6['DOW'] == 3)])
    seg_5[3] += len(df6[(df6['SEGMENT'] == 5) & (df6['DOW'] == 4)])
    seg_5[4] += len(df6[(df6['SEGMENT'] == 5) & (df6['DOW'] == 5)])
    seg_5[5] += len(df6[(df6['SEGMENT'] == 5) & (df6['DOW'] == 6)])
    seg_5[6] += len(df6[(df6['SEGMENT'] == 5) & (df6['DOW'] == 7)])

    train_33 = df6.groupby(by='SEGMENT', as_index=False)['Pax'].sum()
    temp_df = pd.DataFrame()
    temp_df = train_33.copy()
    final_df['Pax'] = temp_df['Pax'] + final_df['Pax']
    train_44 = df6.groupby(by='SEGMENT', as_index=False)['Duration_Index'].sum()
    temp_df = pd.DataFrame()
    temp_df = train_44.copy()
    final_df['Duration_Index'] = temp_df['Duration_Index'] + final_df['Duration_Index']
    train_55 = df6.groupby(by='SEGMENT', as_index=False)['Channel_GDS'].sum()
    temp_df = pd.DataFrame()
    temp_df = train_55.copy()
    final_df['Channel_GDS'] = temp_df['Channel_GDS'] + final_df['Channel_GDS']
    try:
        train_66 = df6.groupby(by='SEGMENT', as_index=False)['Channel_WEB'].sum()
        temp_df = pd.DataFrame()
        temp_df = train_66.copy()
        final_df['Channel_WEB'] = temp_df['Channel_WEB'] + final_df['Channel_WEB']
    except:
        pass
    train_77 = df6.groupby(by='SEGMENT', as_index=False)['Compartment_J'].sum()
    temp_df = pd.DataFrame()
    temp_df = train_77.copy()
    final_df['Compartment_J'] = temp_df['Compartment_J'] + final_df['Compartment_J']
    train_88 = df6.groupby(by='SEGMENT', as_index=False)['Compartment_Y'].sum()
    temp_df = pd.DataFrame()
    temp_df = train_88.copy()
    final_df['Compartment_Y'] = temp_df['Compartment_Y'] + final_df['Compartment_Y']
    train_99 = df6.groupby(by='SEGMENT', as_index=False)['Advance_Booking'].sum()
    temp_df = pd.DataFrame()
    temp_df = train_99.copy()
    final_df['Advance_Booking'] = temp_df['Advance_Booking'] + final_df['Advance_Booking']
    skips += 1000000

cursor.close()
final_df = final_df / len(final_df)
DOW = []
seg_0_Dow = seg_0.index(max(seg_0)) + 1
DOW.append(seg_0_Dow)
seg_1_Dow = seg_1.index(max(seg_1)) + 1
DOW.append(seg_1_Dow)
seg_2_Dow = seg_2.index(max(seg_2)) + 1
DOW.append(seg_2_Dow)
seg_3_Dow = seg_3.index(max(seg_3)) + 1
DOW.append(seg_3_Dow)
seg_4_Dow = seg_4.index(max(seg_4)) + 1
DOW.append(seg_4_Dow)
seg_5_Dow = seg_5.index(max(seg_5)) + 1
DOW.append(seg_5_Dow)

dow_series = pd.Series(DOW)
final_df['DOW'] = dow_series.values

Segment_Ranks = pd.DataFrame()
Segment_Ranks['Duration'] = final_df['Duration'].rank()-1
Segment_Ranks['DOW'] = final_df['DOW'].rank(method='min')-1
Segment_Ranks['Pax'] = final_df['Pax'].rank()-1
Segment_Ranks['Duration_Index'] = final_df['Duration_Index'].rank()-1
Segment_Ranks['Channel_GDS'] = final_df['Channel_GDS'].rank()-1
Segment_Ranks['Channel_WEB'] = final_df['Channel_WEB'].rank()-1
Segment_Ranks['Compartment_J'] = final_df['Compartment_J'].rank()-1
Segment_Ranks['Compartment_Y'] = final_df['Compartment_Y'].rank()-1
Segment_Ranks['Advance_Booking'] = final_df['Advance_Booking'].rank()-1
Segment_Ranks['Segment'] = [0,1,2,3,4,5]

customer_ranks = pd.DataFrame()
customer_ranks['Customer_Groups'] = [0,1,2,3,4,5]
customer_ranks['Advance_Booking'] = [2,0,3,5,1,4]
customer_ranks['DOW'] = [3,5,5,3,0,3]
customer_ranks['Duration'] = [1,0,2,5,3,4]
customer_ranks['Pax'] = [4,0,2,3,1,5]
customer_ranks['Duration_Index'] = [0,2,4,5,3,1]
customer_ranks['Channel_GDS'] = [5,4,2,0,3,1]
customer_ranks['Channel_WEB'] = [0,1,3,5,2,4]
customer_ranks['Compartment_J'] = [1,0,5,4,3,2]
customer_ranks['Compartment_Y'] = [4,5,0,1,2,3]
print "customer_ranks", customer_ranks

df_1=Segment_Ranks.set_index("Segment")
df_2 = customer_ranks.set_index("Customer_Groups")
error_sum = 0

error_sum_list = []
index_list = []
#Defining permutations for the range 012345
from itertools import permutations
perms = [''.join(p) for p in permutations('012345')]
for permut_number in range(len(perms)):
    error_sum=0
    for cluster_number in range(6):
        error1 = abs(df_1.loc[cluster_number, "Advance_Booking"] - df_2.loc[int(perms[permut_number][cluster_number]), "Advance_Booking"])
        error2 = abs(df_1.loc[cluster_number, "DOW"] - df_2.loc[int(perms[permut_number][cluster_number]), "DOW"])
        error3 = abs(df_1.loc[cluster_number, "Duration"] - df_2.loc[int(perms[permut_number][cluster_number]), "Duration"])
        error4 = abs(df_1.loc[cluster_number, "Pax"] - df_2.loc[int(perms[permut_number][cluster_number]), "Pax"])
        error5 = abs(df_1.loc[cluster_number, "Duration_Index"] - df_2.loc[int(perms[permut_number][cluster_number]), "Duration_Index"])
        error6 = abs(df_1.loc[cluster_number, "Channel_GDS"] - df_2.loc[int(perms[permut_number][cluster_number]), "Channel_GDS"])
        error7 = abs(df_1.loc[cluster_number, "Channel_WEB"] - df_2.loc[int(perms[permut_number][cluster_number]), "Channel_WEB"])
        error8 = abs(df_1.loc[cluster_number, "Compartment_J"] - df_2.loc[int(perms[permut_number][cluster_number]), "Compartment_J"])
        error9 = abs(df_1.loc[cluster_number, "Compartment_Y"] - df_2.loc[int(perms[permut_number][cluster_number]), "Compartment_Y"])
        #Summing all the errors
        error_sum += error1 + error2 + error3 + error4 + error5 + error6 + error7 + error8 + error9
        #print error_sum
    error_sum_list.append(error_sum)
    index_list.append(perms[permut_number])

a = index_list[error_sum_list.index(min(error_sum_list))]
print a


model_f = joblib.load("mainmodel_Sales.sav")

skips = 0
limits = 1000000
for turns in range(1,loop_count):
    cursor = db.JUP_DB_Sales.find({},no_cursor_timeout=True).skip(skips).limit(limits)
    dep_date_list = []
    book_date_list = []
    month_list = []
    pax_list = []
    compartment_list = []
    channel_list = []
    revenue_list =  []
    od_list = []
    duration_list = []
    _id_list = []
    for i in cursor:
        dep_date = i['dep_date']
        dep_date_list.append(dep_date)
        book_date = i['book_date']
        book_date_list.append(book_date)
        pax = i['pax']
        pax_list.append(pax)
        compartment = i['compartment']
        compartment_list.append(compartment)
        channel = i['channel']
        channel_list.append(channel)
        revenue = i['revenue']
        revenue_list.append(revenue)
        id = i['_id']
        _id_list.append(id)
        od = i['od']
        od_list.append(od)
        month = i['month']
        month_list.append(month)
    day_of_week_list = []
    bookdate_list = []
    depdate_list = []
    for each_dep in dep_date_list:
        eachdep = datetime.strptime(each_dep,'%Y-%m-%d')
        depdate_list.append(eachdep)
        day_of_week = eachdep.isoweekday()
        day_of_week_list.append(day_of_week)
    for each_book in book_date_list:
        eachbook = datetime.strptime(each_book, '%Y-%m-%d')
        bookdate_list.append(eachbook)
    advance_booking_list = []
    for i in range(len(dep_date_list)):
        advance_booking = depdate_list[i]-bookdate_list[i]
        advance_booking_list.append(advance_booking.days)
    df1 = pd.DataFrame({'OD' : od_list, 'Book_Date': book_date_list, 'id' : _id_list, 'Dep_Date' : dep_date_list, 'Advance_Booking' : advance_booking_list, 'Pax' : pax_list, 'Compartment' : compartment_list, 'Channel' : channel_list, 'Fare' : revenue_list, 'DOW' : day_of_week_list, 'Month' : month_list})
    print(df1.head())
    # df1.to_csv("salesf1.csv", index=False)
    salesf = pd.merge(df1, Duration11, how = 'left')
    print (salesf.head())
    df = salesf
    #Dropping NA values from the 'Compartment' column
    df212 = df.dropna(subset=['Compartment'])
    try:
        df212 = df212[df212.Compartment != 'Others']
    except:
        pass
    #Changing all the values other than WEB to GDS in 'Channel' column
    df212.loc[df212['Channel']!='WEB', 'Channel'] = 'GDS'
    #Getting dummies for the categorical varibale columns - 'Compartment' and 'Channel'
    df2 = pd.get_dummies(df212, columns=['Channel', "Compartment"])
    #Deleting the original columns after creating dummies
    del(df212['Compartment'], df212['Channel'])
    print(len(df2))
    df2['Duration'] = df2['Time']
    df2.drop(df2.columns[9],axis=1,inplace = True)
    print df2.head()
    df2['Duration']=df2['Duration'].fillna(df2['Duration'].mean())
    df2['Duration_Index'] = df2['Fare'] / df2['Duration']
    print df2.isnull().sum()
    df3 = pd.DataFrame()
    #Taking only the columns required for applying KMeans into a new dataframe
    try:
        df3 = df2[['Advance_Booking', 'Channel_GDS', 'Channel_WEB','Compartment_J', 'Compartment_Y', 'DOW', 'Duration','Pax','Duration_Index']]
        print(len(df3))
    except:
        df3 = df2[['Advance_Booking', 'Channel_GDS','Compartment_J', 'Compartment_Y', 'DOW', 'Duration','Pax','Duration_Index']]
        print(len(df3))
    #train, test = train_test_split(df3, test_size=0.2, random_state=123)
    # train = df3
    predict = model_f.predict(df3)
    print(predict)
    # Assigning the column SEGMENT in the testing data which contains the cluster number
    df4 = df3.assign(SEGMENT=predict)
    df6 = df4
    df7 = df6
    #Assigning Customer Groups columns to excel file based on the cluster number
    df7['Customer_Groups'] = int(a[0])
    df7.loc[df7['SEGMENT'] == 1, 'Customer_Groups'] = int(a[1])
    df7.loc[df7['SEGMENT'] == 2, 'Customer_Groups'] = int(a[2])
    df7.loc[df7['SEGMENT'] == 3, 'Customer_Groups'] = int(a[3])
    df7.loc[df7['SEGMENT'] == 4, 'Customer_Groups'] = int(a[4])
    df7.loc[df7['SEGMENT'] == 5, 'Customer_Groups'] = int(a[5])
    df7['Customer_Groups'] = df7['Customer_Groups'].map({1: "Puritans", 2: "Visibility Seekers", 3: "Accolade Stalkers", 4: "Duty Travellers", 5: "Virtuous Travelers",0: "Modesty Pioneers"})
    #df7.head()
    df_2 = pd.merge(df2, df7,how="left")
    # df_2 = df_2.drop_duplicates().reset_index(drop=True)
    df_2 = df_2.drop(['Advance_Booking','DOW','Channel_GDS','Compartment_J','Duration','Duration_Index','SEGMENT'], axis =1)
    df_3 = df_2.rename(columns={'Book_Date': 'book_date', 'Dep_Date': 'dep_date', 'Fare' : 'AIR_CHARGE','Month' : 'month','OD' : 'od','Pax': 'pax','id': '_id','Compartment_Y' : 'compartment','Customer_Groups' : 'segment'})
    bulk_list = []
    count1 = 1
    t1 = 0
    li = 0
    for j in range(len(df_3)):
        if t1 == 5000:
            st = time.time()
            print "updating: ", count1
            db['JUP_DB_Sales'].bulk_write(bulk_list)
            print "updated!", time.time() - st
            bulk_list.append(UpdateOne({"_id": ObjectId(df_3['_id'][j])}, {'$set': {'segment_amadeus': df_3['segment'][j]}}))
            bulk_list = []
            count1 += 1
            li += 5000
            t1 = 0
        else:
            bulk_list.append(UpdateOne({"_id": ObjectId(df_3['_id'][j])}, {'$set': {'segment_amadeus': df_3['segment'][j]}}))
            t1 += 1
    bulk_list = []
    count2 = 1
    # t2 = 0
    for j in range(len(df_3)):
        if j > li:
            bulk_list.append(UpdateOne({"_id": ObjectId(df_3['_id'][j])}, {'$set': {'segment_amadeus': df_3['segment'][j]}}))
            st = time.time()
            print "updating: ", count2
            db['JUP_DB_Sales'].bulk_write(bulk_list)
            print "updated!", time.time() - st
            bulk_list = []
            count2 += 1
            t2 = 0
        else:
            pass
    skips = skips + 1000000

cursor.close()