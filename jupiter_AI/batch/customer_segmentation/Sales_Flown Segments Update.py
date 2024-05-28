import pandas as pd
from datetime import datetime
import pymongo
from pymongo import MongoClient
from sklearn.cluster import KMeans
import os
from jupiter_AI import JUPITER_LOGGER
from jupiter_AI.logutils import measure

client = MongoClient("mongodb://analytics:KNjSZmiaNUGLmS0Bv2@13.92.251.7:42525/")

# Connecting to Local Mongo DB
conn = client['fzDB_stg']

flight_duration_minutes_list = []
Dept_Sta_list = []
Arvl_Sta_list = []

# Find the required columns from the Capacity collection
cursor = conn.JUP_DB_OD_Duration.find({})
# print "length: ", cursor.count()
for i in cursor:
    Dept_Sta = i["Dept Sta"]
    Dept_Sta_list.append(Dept_Sta)
    Arvl_Sta = i["Arvl Sta"]
    Arvl_Sta_list.append(Arvl_Sta)
    flight_duration_minutes = i["flight_duration_minutes"]
    flight_duration_minutes_list.append(flight_duration_minutes)
# Defining a new dataframe with the required columns
Duration = pd.DataFrame(
    {'flight_duration_minutes': flight_duration_minutes_list, 'Dept_Sta': Dept_Sta_list, 'Arvl_Sta': Arvl_Sta_list})

Duration['OD'] = Duration['Dept_Sta'].map(str) + Duration['Arvl_Sta']
Duration.drop(Duration.columns[[0,1]], axis=1, inplace=True)
Duration.columns = ['Time', 'OD']
pd.set_option("display.max_columns", 100)

#Group all the similar ODs and find the OD which has the minimum time
Duration11 = Duration.groupby(by = 'OD', as_index = False)['Time'].mean()

#Finding the required columns from the Sales_Flown collection
cursor = conn.Sales_Test.find()
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

    # print(dep_date_list[0:5])
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
#df1.to_csv("salesf1.csv", index=False)

#main=pd.read_csv("/home/prathyusha/Downloads/Simulation/salesf1.csv")

salesf = pd.merge(df1, Duration11, how = 'left')

print salesf.head()


os.chdir("/home/prathyusha/Downloads/Simulation")


#Reading the csv file
df = salesf


#Dropping NA values from the 'Compartment' column
df1 = df.dropna(subset=['Compartment'])
#Changing all the values other than WEB to GDS in 'Channel' column
df1.loc[df1['Channel']!='WEB', 'Channel'] = 'GDS'
#Getting dummies for the categorical varibale columns - 'Compartment' and 'Channel'
df2 = pd.get_dummies(df1, columns=['Channel', "Compartment"])
#Deleting the original columns after creating dummies
del(df1['Compartment'], df1['Channel'])
print(len(df2))

df2['Duration'] = df2['Time']
df2.drop(df2.columns[9],axis=1,inplace = True)
df2['Duration']=df2['Duration'].fillna(df2['Duration'].mean())
df2['Duration_Index'] = df2['Fare'] / df2['Duration']
print df2.isnull().sum()

df3 = pd.DataFrame()
#Taking only the columns required for applying KMeans into a new dataframe
df3 = df2[['Advance_Booking', 'Channel_GDS', 'Channel_WEB','Compartment_J', 'Compartment_Y', 'DOW', 'Duration','Pax','Duration_Index']]
print(len(df3))

from sklearn import datasets, linear_model
from sklearn.model_selection import train_test_split
from matplotlib import pyplot as plt
#df = pd.DataFrame(np.random.randn(100, 2))

# msk = np.random.rand(len(df3)) < 0.7

# train = df[msk]

# test = df[~msk]

train, test = train_test_split(df3, test_size=0.3, random_state=123)
print train.head()
print train.info()

#Applying KMeans model on training data and diving it into 6 clusters
#Here random state is assigned inorder to get the same clusters every time we run the code
model = KMeans(n_clusters=6, random_state = 9999).fit(train)
# model.fit(train)

#Getting the labels for each row in the training data as to which cluster they belong to
labels=model.labels_
print(labels)

#Assigning a new column named SEGMENT in the training data which contains the cluster number
df4 = train.assign(SEGMENT = labels)
print(df4.head(n=10))

X = pd.DataFrame()
X = train
y = pd.DataFrame()
y = test
#Predicting the model for test data
predict = model.predict(y)
print(predict)
#Assigning the column SEGMENT in the testing data which contains the cluster number
df5 = test.assign(SEGMENT = predict)
print(df5.head())
#Printing the centroids of the clusters
centroids = model.cluster_centers_
print(centroids)
#Concatenating two dataframes one below the other to get the final dataframe
final_df  = pd.concat([df4, df5], axis = 0)
print(len(final_df))
df6 = final_df
train_1, test_1 = train_test_split(df6, test_size=0.3, random_state=123)
print train_1.head()
train_11 = train_1.groupby(by = 'SEGMENT', as_index = False)['Duration'].mean()

train_11
train_111 = train_11.rank()
#train_111
# Segment_Ranks = {'Duration':}
Segment_Ranks = pd.DataFrame()

Segment_Ranks['Duration'] = train_111['Duration']
Segment_Ranks['Duration'] = Segment_Ranks['Duration'] - 1
print Segment_Ranks
train_22 = train_1.groupby(by = 'SEGMENT', as_index = False)['DOW'].median()
train_222 = train_22.rank(method='min')
Segment_Ranks['DOW'] = train_222['DOW']
Segment_Ranks['DOW'] = Segment_Ranks['DOW'] - 1
train_33 = train_1.groupby(by = 'SEGMENT', as_index = False)['Pax'].mean()
train_44 = train_1.groupby(by = 'SEGMENT', as_index = False)['Duration_Index'].mean()
train_55 = train_1.groupby(by = 'SEGMENT', as_index = False)['Channel_GDS'].mean()
train_66 = train_1.groupby(by = 'SEGMENT', as_index = False)['Channel_WEB'].mean()
train_77 = train_1.groupby(by = 'SEGMENT', as_index = False)['Compartment_J'].mean()
train_88 = train_1.groupby(by = 'SEGMENT', as_index = False)['Compartment_Y'].mean()
train_99 = train_1.groupby(by = 'SEGMENT', as_index = False)['Advance_Booking'].mean()
train_333 = train_33.rank()
Segment_Ranks['Pax'] = train_333['Pax']
Segment_Ranks['Pax'] = Segment_Ranks['Pax'] -1
train_444 = train_44.rank()
Segment_Ranks['Duration_Index'] = train_444['Duration_Index'] - 1
train_555 = train_55.rank()
Segment_Ranks['Channel_GDS'] = train_555['Channel_GDS'] - 1
train_666 = train_66.rank()
Segment_Ranks['Channel_WEB'] = train_666['Channel_WEB'] - 1
train_777 = train_77.rank()
Segment_Ranks['Compartment_J'] = train_777['Compartment_J'] - 1
train_888 = train_88.rank()
Segment_Ranks['Compartment_Y'] = train_888['Compartment_Y'] - 1
train_999 = train_99.rank()
Segment_Ranks['Advance_Booking'] = train_999['Advance_Booking'] - 1
print Segment_Ranks
Segment_Ranks['Segment'] = [0,1,2,3,4,5]
print Segment_Ranks
#Defining a function to split the list into n number of parts
i = 0


@measure(JUPITER_LOGGER)
def split_list(alist, wanted_parts=i):
    length = len(alist)
    return [ alist[i*length // wanted_parts: (i+1)*length // wanted_parts]
             for i in range(wanted_parts) ]

#Customer_Group_Ranks csv file contains the ranks of the various customers for the above 9 parameters which is given manually from Amadeus documentation
customer_ranks = pd.read_csv("/home/prathyusha/Downloads/Customer Propensity and Segmentation/Data/Customer_Group_Ranks.csv")

df_1=Segment_Ranks.set_index("Segment")
df_2 = customer_ranks.set_index("Customer_Groups")
error_sum = 0

error_sum_list = []
index_list = []
#Defining permutations for the range 012345
from itertools import permutations
perms = [''.join(p) for p in permutations('012345')]
#print(perms)
for permut_number in range(len(perms)):
    for cluster_number in range(6):
        assignment=[cluster_number, perms[permut_number][cluster_number]]
        #print(perms[permut_number][cluster_number])
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
        error_sum = error1+error2+error3+error4+error5+error6+error7+error8+error9
        error_sum_list.append(error_sum)
        #Inserting all the permutations into a list to find out the index of the minimum summation error
        index_list.append(int(perms[permut_number]))

a = index_list[error_sum_list.index(min(error_sum_list))]
print a
print(len(error_sum_list))
#print(len(error_sum_list))
summation = split_list(error_sum_list, wanted_parts = 720)
#print(summation)
result_list = []
for each_list in summation:
    result = sum(each_list)
    result_list.append(result)
print(min(result_list))
print(result_list.index(min(result_list)))
req_permutation = split_list(index_list, wanted_parts = 720)
#Getting the permutation for the cluster sequence 012345 which has minimum error
print(req_permutation[result_list.index(min(result_list))])

df7 = df6
#Assigning Customer Groups columns to excel file based on the cluster number
df7['Customer_Groups'] = int(a[0])
df7.loc[df7['SEGMENT'] == 1, 'Customer_Groups'] = int(a[1])
df7.loc[df7['SEGMENT'] == 2, 'Customer_Groups'] = int(a[2])
df7.loc[df7['SEGMENT'] == 3, 'Customer_Groups'] = int(a[3])
df7.loc[df7['SEGMENT'] == 4, 'Customer_Groups'] = int(a[4])
df7.loc[df7['SEGMENT'] == 5, 'Customer_Groups'] = int(a[5])
# df7['Customer_Groups'] = 1
# df7.loc[df7['SEGMENT'] == 1, 'Customer_Groups'] = 0
# df7.loc[df7['SEGMENT'] == 2, 'Customer_Groups'] = 3
# df7.loc[df7['SEGMENT'] == 3, 'Customer_Groups'] = 4
# df7.loc[df7['SEGMENT'] == 4, 'Customer_Groups'] = 2
# df7.loc[df7['SEGMENT'] == 5, 'Customer_Groups'] = 5
print df7.head()


#import pandas as pd
import numpy as np
from sklearn import datasets, linear_model
from sklearn.model_selection import train_test_split
from matplotlib import pyplot as plt
df7['Customer_Groups'] = df7['Customer_Groups'].map({ 1 : "Puritans", 2 : "Visibility Seekers", 3 : "Accolade Stalkers", 4 : "Duty Travellers", 5 : "Virtuous Travelers", 0 : "Modesty Pioneers"})
df_1 = df2
df_2 = pd.merge(df_1, df, how='left')
#df_3['compartment'] = df_3['compartment'].map({ 1 : "Y", 0 : "J" })
df_2 = df_2.drop(['Advance_Booking',
 'DOW',
 'Channel_GDS',
 'Channel_WEB',
 'Compartment_J',
 'Duration',
 'Duration_Index',
 'SEGMENT'], axis =1)
df_3 = df_2.rename(columns={'Book_Date': 'book_date', 'Dep_Date': 'dep_date', 'Fare' : 'AIR_CHARGE',
 'Month' : 'month',
 'OD' : 'od',
 'Pax': 'pax',
 'id': '_id',
 'Compartment_Y' : 'compartment',
 'Customer_Groups' : 'segment'})
from pymongo import UpdateOne
from bson import ObjectId
import time
print df_3.shape
print df_3.isnull().sum()
bulk_list = []
count = 1
t = 0
for j in range(len(df_3)):
    bulk_list.append(UpdateOne({"_id": ObjectId(df_3['_id'][j])}, {'$set': {'segment': df_3['segment'][j]}}))
print "list_final: ", bulk_list


# for j in range(len(df_3)):
#     if t == 1000:
#         st = time.time()
#         print "updating: ", count
#         conn['JUP_DB_Sales_Flown'].bulk_write(bulk_list)
#         print "updated!", time.time() - st
#         bulk_list = []
#         count += 1
#         t = 0
#     else:
#         bulk_list.append(UpdateOne({"_id": ObjectId(df_3['_id'][j])}, {'$set': {'segment': df_3['segment'][j]}}))
#         t += 1
#     if j > 844000:
#         break
#
# bulk_list = []
# count = 1
# t = 0
# for j in range(len(df_3)):
#     if j > 843000:
#         bulk_list.append(UpdateOne({"_id": ObjectId(df_3['_id'][j])}, {'$set': {'segment': df_3['segment'][j]}}))
#         st = time.time()
#         print "updating: ", count
#         conn['JUP_DB_Sales_Flown'].bulk_write(bulk_list)
#         print "updated!", time.time() - st
#         bulk_list = []
#         count += 1
#         t = 0
#     else:
#         pass

