'''
Action :
    1. get data from database
    2. return to the appropriate data grid
    
'''

from collections import defaultdict
from copy import deepcopy
from jupiter_AI.network_level_params import MONGO_CLIENT_URL, MONGO_SOURCE_DB, ANALYTICS_MONGO_PASSWORD, ANALYTICS_MONGO_USERNAME, RABBITMQ_USERNAME, RABBITMQ_PASSWORD, RABBITMQ_HOST, RABBITMQ_PORT
import pymongo
import json
import time
import pandas as pd
from datetime import datetime, timedelta
from dateutil.parser import parse
import global_variable as var

# Connect mongodb db business layer
try:
    conn=pymongo.MongoClient(var.mongo_client_url)[var.database]
    conn.authenticate(ANALYTICS_MONGO_USERNAME, ANALYTICS_MONGO_PASSWORD, source=MONGO_SOURCE_DB)
    
    raw_data_conn=pymongo.MongoClient(var.mongo_client_url)[var.rawdatabase]
    raw_data_conn.authenticate(ANALYTICS_MONGO_USERNAME, ANALYTICS_MONGO_PASSWORD, source=MONGO_SOURCE_DB)
    
except Exception as e:
    #sys.stderr.write("Could not connect to MongoDB: %s" % e)
    print("Could not connect to MongoDB: %s" % e)

def connectDBServer():
    # Connect mongodb db business layer
    try:
        conn=pymongo.MongoClient(var.mongo_client_url)[var.database]
        conn.authenticate(ANALYTICS_MONGO_USERNAME, ANALYTICS_MONGO_PASSWORD, source=MONGO_SOURCE_DB)
        
        raw_data_conn=pymongo.MongoClient(var.mongo_client_url)[var.rawdatabase]
        raw_data_conn.authenticate(ANALYTICS_MONGO_USERNAME, ANALYTICS_MONGO_PASSWORD, source=MONGO_SOURCE_DB)
        
    except Exception as e:
        #sys.stderr.write("Could not connect to MongoDB: %s" % e)
        print("Could not connect to MongoDB: %s" % e)


# for last updated date and time    
now = datetime.now()


def _getData(data):
    #connect DB server if not connect
    connectDBServer()
    # Mapping Paxis into Market
    
    connector = data.replace("Paxis", "Market")
    print(connector)
    cursor = conn.JUP_DB_Data_Validation_.find({"connector":connector})
    capa_dic=dict()
    list_capa=list()
    for i in cursor:
        json.dumps({'connector':i['connector' ],'Pre_Approval_Date':i['Pre_Approval_Date' ],'Duplicate':i['Duplicate' ],
                   'No_of_Market':i['No_of_Market' ],'Raw_count':i['Raw_count' ],'Affected_time':i['Affected_time' ]})
        
        capa_dic['connector'] = i['connector']
        capa_dic['Pre_Approval_Date'] = i["Pre_Approval_Date"]
        capa_dic['Duplicate'] = i["Duplicate"]
        capa_dic['No_of_Market'] = i["No_of_Market"]
        capa_dic['Raw_count'] = i["Raw_count"]
        capa_dic['Affected_time'] = i["Affected_time"]
        #print i['Pre_Approval_Date' ]
        #print(datetime.strptime(i['Pre_Approval_Date' ], "%Y-%m-%d"))
        list_capa.append( {"Pre_Approval_Date":time.mktime(datetime.strptime(i['Pre_Approval_Date' ], "%Y-%m-%d").timetuple()),
                           'Raw_count':i['Raw_count' ],'Duplicate':i['Duplicate' ],'No_of_Market':i['No_of_Market' ],'Affected_time':i['Affected_time'],'connector':i['connector' ]} )
    return list_capa
    #print(list_capa)    


def query_for_data_validation(types):
    
    #connect DB server if not connect
    connectDBServer()
    print(types['typeof'])
    if(types['typeof']=='Booking'):
        try:
            # sale create match fields for now added in hadhoc doc aftersome time later we need to change 
            qry_pi = dict()
            qry_pi['book_date'] = {'$eq': types['trxDate']}
            qry_pi['dep_date'] = {'$eq': types['depDate']}
            #qry_pi['origin'] = {'$eq': 'ALA'}
            #qry_pi['destination'] = {'$eq': 'DXB'}
            qry_pi['pos'] = {'$eq': types['pos']}

            # add into pipeline in the query
            pipeline_derived = [
                # the following pipeline matches the documents from the collection with the filter values
                {
                    '$match': qry_pi
                }
                ,
                {'$group':{
                  '_id':{
                      'book_date':'$book_date',
                      'dep_date':'$dep_date',
                      'pos':'$pos',
                      'origin':'$origin',
                      'destination':'$destination',
                      'farebasis':'$farebasis'
                      },
                      'pax':{'$sum':'$pax'},
                      'revenue':{'$sum':'$revenue'}
                  }
                  }
                ,
                {'$project':{
                      '_id':1,
                      'book_date':'$_id.book_date',
                      'dep_date':'$_id.dep_date',
                      'pos':'$_id.pos',
                      'origin':'$_id.origin',
                      'destination':'$_id.destination',
                      'farebasis':'$_id.farebasis',
                      'pax':'$pax',
                      'revenue':'$revenue'
                    }
                 }
                ,
                {
                    '$out': 'Temp_collection'
                }
            ]

            # running the aggregate pipeline from sales collection
            conn.JUP_DB_Booking_DepDate.aggregate(pipeline_derived, allowDiskUse=True)

            #For raw data set
            raw_qry_pi = dict()
            raw_qry_pi['SEG_BOOK_DT_LCL_KEY'] = {'$eq': types['trxDate']+' 00:00:00.000000000'}
            raw_qry_pi['DEPARTURE_DT'] = {'$eq': types['depDate']+' 00:00:00.000000000'}
            #raw_qry_pi['FROM_AIRPORT'] = {'$eq': 'ALA'}
            #raw_qry_pi['TO_AIRPORT'] = {'$eq': 'DXB'}
            raw_qry_pi['POS_OFFICE_OR_CITY_CDE'] = {'$eq': types['pos']}

            # add into pipeline in the query
            pipeline_raw = [
                # the following pipeline matches the documents from the collection with the filter values
                {
                    '$match': raw_qry_pi
                }
                ,
                {'$project':{
                 '_id':0,
                 'book_date':'$SEG_BOOK_DT_LCL_KEY',
                 'dep_date':'$DEPARTURE_DT',    
                 'pos':'$POS_OFFICE_OR_CITY_CDE',
                 'origin':'$FROM_AIRPORT',
                 'destination':'$TO_AIRPORT',
                 'farebasis':'$fare_basis_cde',    
                 'pax': {
                         '$cond': { 'if': { '$ne': [ "$PAX_TYPE_CDE", 'INF' ] }, 'then': '$SEGMENT_COUNT', 'else': 0 }
                       },
                 'revenue':{ '$subtract':[ '$TOTAL_CHRGS_RPT_CURR_NET', "$TAX_CHARGE" ]  }
                 }
                 }
                ,
                {'$group':{
                  '_id':{
                      'book_date':'$book_date',
                      'dep_date':'$dep_date',
                      'pos':'$pos',
                      'origin':'$origin',
                      'destination':'$destination',
                      'farebasis':'$farebasis'
                      },
                      'pax':{'$sum':'$pax'},
                      'revenue':{'$sum':'$revenue'}
                  }
                  }
                ,
                {'$project':{
                      '_id':0,
                      'book_date':'$_id.book_date',
                      'dep_date':'$_id.dep_date',
                      'pos':'$_id.pos',
                      'origin':'$_id.origin',
                      'destination':'$_id.destination',
                      'farebasis':'$_id.farebasis',
                      'raw_pax':'$pax',
                      'raw_revenue':'$revenue'
                    }
                 }
                ,
                {
                    '$out': 'Temp_collection2'
                }
            ]

            # running the aggregate pipeline from sales collection in rawData db
            raw_data_conn.JUP_DB_Booking.aggregate(pipeline_raw, allowDiskUse=True)

            # read data from temp collection which is created from the last two aggregate pipelines
            saleDerived = conn.Temp_collection.find({},{'_id':0})
            saleRaw = raw_data_conn.Temp_collection2.find({},{'_id':0})
            saleDList = list()
            saleRList = list()
            for i in saleDerived:
                #i['book_date'] = time.mktime(datetime.strptime(i['book_date'], "%Y-%m-%d").timetuple())
                #i['dep_date'] = time.mktime(datetime.strptime(i['dep_date'], "%Y-%m-%d").timetuple())
                saleDList.append(i)
                
            for i in saleRaw:
                i['book_date'] = i['book_date'][0:10]
                i['dep_date'] = i['dep_date'][0:10]
                saleRList.append(i)
            
            # add to dataFrame for merging two different table into one common
            saleDerivedTable = pd.DataFrame(saleDList)
            saleRawTable = pd.DataFrame(saleRList)
            mergedTable = pd.merge(saleDerivedTable, saleRawTable, how='outer', on=['book_date', 'dep_date','pos','origin','destination','farebasis'])
            dataDict = mergedTable.to_dict(orient='records')
            #print(dataDict)
            #print(saleRawTable)
        except Exception as e:
            print e
            dataDict = 'no data' 
    elif(types['typeof']=='Sales'):
        try:
            # sale create match fields for now added in hadhoc doc aftersome time later we need to change 
            qry_pi = dict()
            qry_pi['book_date'] = {'$eq': types['trxDate']}
            qry_pi['dep_date'] = {'$eq': types['depDate']}
            #qry_pi['origin'] = {'$eq': 'ALA'}
            #qry_pi['destination'] = {'$eq': 'DXB'}
            qry_pi['pos'] = {'$eq': types['pos']}

            # add into pipeline in the query
            pipeline_derived = [
                # the following pipeline matches the documents from the collection with the filter values
                {
                    '$match': qry_pi
                }
                ,
                {'$group':{
                  '_id':{
                      'book_date':'$book_date',
                      'dep_date':'$dep_date',
                      'pos':'$pos',
                      'origin':'$origin',
                      'destination':'$destination',
                      'farebasis':'$fare_basis'
                      },
                      'pax':{'$sum':'$pax'},
                      'revenue':{'$sum':'$revenue'}
                  }
                  }
                ,
                {'$project':{
                      '_id':1,
                      'book_date':'$_id.book_date',
                      'dep_date':'$_id.dep_date',
                      'pos':'$_id.pos',
                      'origin':'$_id.origin',
                      'destination':'$_id.destination',
                      'farebasis':'$_id.farebasis',
                      'pax':'$pax',
                      'revenue':'$revenue'
                    }
                 }
                ,
                {
                    '$out': 'Temp_collection'
                }
            ]

            # running the aggregate pipeline from sales collection
            conn.JUP_DB_Sales.aggregate(pipeline_derived, allowDiskUse=True)

            #For raw data set
            raw_qry_pi = dict()
            raw_qry_pi['SEG_BOOK_DT_LCL_KEY'] = {'$eq': types['trxDate']+' 00:00:00.000000000'}
            raw_qry_pi['DEPARTURE_DT'] = {'$eq': types['depDate']+' 00:00:00.000000000'}
            #raw_qry_pi['FROM_AIRPORT'] = {'$eq': 'ALA'}
            #raw_qry_pi['TO_AIRPORT'] = {'$eq': 'DXB'}
            raw_qry_pi['POS_OFFICE_OR_CITY_CDE'] = {'$eq': types['pos']}

            # add into pipeline in the query
            pipeline_raw = [
                # the following pipeline matches the documents from the collection with the filter values
                {
                    '$match': raw_qry_pi
                }
                ,
                {'$project':{
                 '_id':0,
                 'book_date':'$SEG_BOOK_DT_LCL_KEY',
                 'dep_date':'$DEPARTURE_DT',    
                 'pos':'$POS_OFFICE_OR_CITY_CDE',
                 'origin':'$FROM_AIRPORT',
                 'destination':'$TO_AIRPORT',
                 'farebasis':'$fare_basis_cde',    
                 'pax': {
                         '$cond': { 'if': { '$ne': [ "$PAX_TYPE_CDE", 'INF' ] }, 'then': '$SEGMENT_COUNT', 'else': 0 }
                       },
                 'revenue':{ '$subtract':[ '$TOTAL_CHRGS_RPT_CURR_NET', "$TAX_CHARGE" ]  }
                 }
                 }
                ,
                {'$group':{
                  '_id':{
                      'book_date':'$book_date',
                      'dep_date':'$dep_date',
                      'pos':'$pos',
                      'origin':'$origin',
                      'destination':'$destination',
                      'farebasis':'$farebasis'
                      },
                      'pax':{'$sum':'$pax'},
                      'revenue':{'$sum':'$revenue'}
                  }
                  }
                ,
                {'$project':{
                      '_id':0,
                      'book_date':'$_id.book_date',
                      'dep_date':'$_id.dep_date',
                      'pos':'$_id.pos',
                      'origin':'$_id.origin',
                      'destination':'$_id.destination',
                      'farebasis':'$_id.farebasis',
                      'raw_pax':'$pax',
                      'raw_revenue':'$revenue'
                    }
                 }
                ,
                {
                    '$out': 'Temp_collection2'
                }
            ]

            # running the aggregate pipeline from sales collection in rawData db
            raw_data_conn.JUP_DB_Sales.aggregate(pipeline_raw, allowDiskUse=True)

            # read data from temp collection which is created from the last two aggregate pipelines
            saleDerived = conn.Temp_collection.find({},{'_id':0})
            saleRaw = raw_data_conn.Temp_collection2.find({},{'_id':0})
            saleDList = list()
            saleRList = list()
            for i in saleDerived:
                #i['book_date'] = time.mktime(datetime.strptime(i['book_date'], "%Y-%m-%d").timetuple())
                #i['dep_date'] = time.mktime(datetime.strptime(i['dep_date'], "%Y-%m-%d").timetuple())
                saleDList.append(i)
                
            for i in saleRaw:
                i['book_date'] = i['book_date'][0:10]
                i['dep_date'] = i['dep_date'][0:10]
                saleRList.append(i)
            
            # add to dataFrame for merging two different table into one common
            saleDerivedTable = pd.DataFrame(saleDList)
            saleRawTable = pd.DataFrame(saleRList)
            mergedTable = pd.merge(saleDerivedTable, saleRawTable, how='outer', on=['book_date', 'dep_date','pos','origin','destination','farebasis'])
            dataDict = mergedTable.to_dict(orient='records')
            #print(dataDict)
            #print(saleRawTable)
        except Exception as e:
            print e
            dataDict = 'no data'
            
    elif(types['typeof']=='Flown'):
        try:
            # sale create match fields for now added in hadhoc doc aftersome time later we need to change 
            qry_pi = dict()
            qry_pi['book_date'] = {'$eq': types['trxDate']}
            qry_pi['dep_date'] = {'$eq': types['depDate']}
            #qry_pi['origin'] = {'$eq': 'ALA'}
            #qry_pi['destination'] = {'$eq': 'DXB'}
            qry_pi['pos'] = {'$eq': types['pos']}

            # add into pipeline in the query
            pipeline_derived = [
                # the following pipeline matches the documents from the collection with the filter values
                {
                    '$match': qry_pi
                }
                ,
                {'$group':{
                  '_id':{
                      'book_date':'$book_date',
                      'dep_date':'$dep_date',
                      'pos':'$pos',
                      'origin':'$origin',
                      'destination':'$destination',
                      'farebasis':'$fare_basis'
                      },
                      'pax':{'$sum':'$pax'},
                      'revenue':{'$sum':'$revenue'}
                  }
                  }
                ,
                {'$project':{
                      '_id':1,
                      'book_date':'$_id.book_date',
                      'dep_date':'$_id.dep_date',
                      'pos':'$_id.pos',
                      'origin':'$_id.origin',
                      'destination':'$_id.destination',
                      'farebasis':'$_id.farebasis',
                      'pax':'$pax',
                      'revenue':'$revenue'
                    }
                 }
                ,
                {
                    '$out': 'Temp_collection'
                }
            ]

            # running the aggregate pipeline from sales collection
            conn.JUP_DB_Sales_Flown.aggregate(pipeline_derived, allowDiskUse=True)

            #For raw data set
            raw_qry_pi = dict()
            raw_qry_pi['SEG_BOOK_DT_LCL_KEY'] = {'$eq': types['trxDate']+' 00:00:00.000000000'}
            raw_qry_pi['DEPARTURE_DT'] = {'$eq': types['depDate']+' 00:00:00.000000000'}
            #raw_qry_pi['FROM_AIRPORT'] = {'$eq': 'ALA'}
            #raw_qry_pi['TO_AIRPORT'] = {'$eq': 'DXB'}
            raw_qry_pi['POS_OFFICE_OR_CITY_CDE'] = {'$eq': types['pos']}

            # add into pipeline in the query
            pipeline_raw = [
                # the following pipeline matches the documents from the collection with the filter values
                {
                    '$match': raw_qry_pi
                }
                ,
                {'$project':{
                 '_id':0,
                 'book_date':'$SEG_BOOK_DT_LCL_KEY',
                 'dep_date':'$DEPARTURE_DT',    
                 'pos':'$POS_OFFICE_OR_CITY_CDE',
                 'origin':'$FROM_AIRPORT',
                 'destination':'$TO_AIRPORT',
                 'farebasis':'$fare_basis_cde',    
                
                 'pax': {'$cond':{'if':{'$ne':["$RES_SEG_STATUS_DESC" , "CANCELED            "]},
                                  'then':{'$cond': {
                                      'if': { '$ne': [ "$PAX_TYPE_CDE", 'INF' ] },
                                                     'then': '$SEGMENT_COUNT',
                                                     'else': 0 }},
                                  'else':0}
                         
                       },
               
                 'revenue': {'$cond':{
                     'if':{'$ne':["$RES_SEG_STATUS_DESC" , "CANCELED            "]},
                     'then':{'$subtract':[ '$TOTAL_CHRGS_RPT_CURR_NET', "$TAX_CHARGE" ]} ,
                     'else':0} }
                 }
                 }
                ,
                {'$group':{
                  '_id':{
                      'book_date':'$book_date',
                      'dep_date':'$dep_date',
                      'pos':'$pos',
                      'origin':'$origin',
                      'destination':'$destination',
                      'farebasis':'$farebasis'
                      },
                      'pax':{'$sum':'$pax'},
                      'revenue':{'$sum':'$revenue'}
                  }
                  },
                  
                {'$project':{
                      '_id':0,
                      'book_date':'$_id.book_date',
                      'dep_date':'$_id.dep_date',
                      'pos':'$_id.pos',
                      'origin':'$_id.origin',
                      'destination':'$_id.destination',
                      'farebasis':'$_id.farebasis',
                      'raw_pax':'$pax',
                      'raw_revenue':'$revenue'
                    }
                 }
                ,
                {
                    '$out': 'Temp_collection2'
                }
            ]

            # running the aggregate pipeline from sales collection in rawData db
            raw_data_conn.JUP_DB_Sales_Flown.aggregate(pipeline_raw, allowDiskUse=True)

            # read data from temp collection which is created from the last two aggregate pipelines
            saleDerived = conn.Temp_collection.find({},{'_id':0})
            saleRaw = raw_data_conn.Temp_collection2.find({},{'_id':0})
            saleDList = list()
            saleRList = list()
            for i in saleDerived:
                #i['book_date'] = time.mktime(datetime.strptime(i['book_date'], "%Y-%m-%d").timetuple())
                #i['dep_date'] = time.mktime(datetime.strptime(i['dep_date'], "%Y-%m-%d").timetuple())
                saleDList.append(i)
                
            for i in saleRaw:
                i['book_date'] = i['book_date'][0:10]
                i['dep_date'] = i['dep_date'][0:10]
                saleRList.append(i)
            
            # add to dataFrame for merging two different table into one common
            saleDerivedTable = pd.DataFrame(saleDList)
            saleRawTable = pd.DataFrame(saleRList)
            mergedTable = pd.merge(saleDerivedTable, saleRawTable, how='outer', on=['book_date', 'dep_date','pos','origin','destination','farebasis'])
            dataDict = mergedTable.to_dict(orient='records')
            #print(mergedTable)
            #print(saleRawTable)
        except Exception as e:
            print e
            dataDict = e
            
    elif(types['typeof']=='Flight_Leg'):
        try:
            # sale create match fields for now added in hadhoc doc aftersome time later we need to change 
            qry_pi = dict()
            #qry_pi['book_date'] = {'$eq': types['trxDate']}
            qry_pi['dep_date'] = {'$eq': types['depDate']}
            qry_pi['od'] = {'$eq': types['od']}
            #qry_pi['destination'] = {'$eq': 'DXB'}
            #qry_pi['pos'] = {'$eq': types['pos']}


            # Flight_Leg is direct value so no need to aggregate and combine table
            Flight_Leg = conn.JUP_DB_Flight_Leg.find(qry_pi,{'_id':0})
            Flight_LegDList = list()
            for i in Flight_Leg:
                #i['book_date'] = time.mktime(datetime.strptime(i['book_date'], "%Y-%m-%d").timetuple())
                #i['dep_date'] = time.mktime(datetime.strptime(i['dep_date'], "%Y-%m-%d").timetuple())
                Flight_LegDList.append(i)
                
            flightLegDFrame = pd.DataFrame(Flight_LegDList)    
            dataDict = flightLegDFrame.to_dict(orient='records')
            #print(mergedTable)
            #print(saleRawTable)
        except Exception as e:
            print e
            dataDict = e
            
    elif(types['typeof']=='Forecast_od'):
        try:
            # In Forecase departure date not in actual date format so just give year and month only so format it accoudingly
            parsedDate = parse(types['depDate'])
            
            # sale create match fields for now added in hadhoc doc aftersome time later we need to change 
            qry_pi = dict()
            #qry_pi['book_date'] = {'$eq': types['trxDate']}
            
            qry_pi['departureMonth'] = {'$eq': parsedDate.strftime('%Y%m')}
            qry_pi['od'] = {'$eq': types['od']}
            qry_pi['pos'] = {'$eq': types['pos']}
            
            #qry_pi['destination'] = {'$eq': 'DXB'}
            

            # Forecast_od is direct value so no need to aggregate and combine table
            Forecast_od = conn.JUP_DB_Forecast_OD.find(qry_pi,{'_id':0})
            forecastDList = list()
            for i in Forecast_od:
                #i['book_date'] = time.mktime(datetime.strptime(i['book_date'], "%Y-%m-%d").timetuple())
                #i['dep_date'] = time.mktime(datetime.strptime(i['dep_date'], "%Y-%m-%d").timetuple())
                forecastDList.append(i)
                
            forecastDFrame = pd.DataFrame(forecastDList)
            
            dataDict = forecastDFrame.to_dict(orient='records')
            #print(mergedTable)
            #print(saleRawTable)
        except Exception as e:
            print e
            dataDict = e
            
    elif(types['typeof']=='Market'):
        try:
            # In Forecase departure date not in actual date format so just give year and month only so format it accoudingly
            parsedDate = parse(types['depDate'])
            
            # sale create match fields for now added in hadhoc doc aftersome time later we need to change 
            qry_pi = dict()
            #qry_pi['book_date'] = {'$eq': types['trxDate']}
            
            qry_pi['year'] = {'$eq': int(parsedDate.strftime('%Y'))}
            qry_pi['month'] = {'$eq': int(parsedDate.strftime('%m'))}
            qry_pi['od'] = {'$eq': types['od']}
            qry_pi['pos'] = {'$eq': types['pos']}
            qry_pi['MarketingCarrier1'] = {'$eq': types['airline']}
            #qry_pi['destination'] = {'$eq': 'DXB'}
            

            # Forecast_od is direct value so no need to aggregate and combine table
            Market_od = conn.JUP_DB_Market_Share.find(qry_pi,{'_id':0})
            Market_odDList = list()
            for i in Market_od:
                #i['book_date'] = time.mktime(datetime.strptime(i['book_date'], "%Y-%m-%d").timetuple())
                #i['dep_date'] = time.mktime(datetime.strptime(i['dep_date'], "%Y-%m-%d").timetuple())
                Market_odDList.append(i)
                
            Market_odDFrame = pd.DataFrame(Market_odDList)    
            dataDict = Market_odDFrame.to_dict(orient='records')
            #print(mergedTable)
            #print(saleRawTable)
        except Exception as e:
            print e
            dataDict = 'no data'
            
    elif(types['typeof'] == 'Target_Leg'):
        try:
            # In Forecase departure date not in actual date format so just give year and month only so format it accoudingly
            parsedDate = parse(types['depDate'])
            
            # sale create match fields for now added in hadhoc doc aftersome time later we need to change 
            qry_pi = dict()
            #qry_pi['book_date'] = {'$eq': types['trxDate']}
            
            qry_pi['year'] = {'$eq': str(int(parsedDate.strftime('%Y')))}
            qry_pi['month'] = {'$eq': str(int(parsedDate.strftime('%m')))}
            qry_pi['od'] = {'$eq': 'DXBMAA'}
            #qry_pi['pos'] = {'$eq': types['pos']}
            
            #qry_pi['destination'] = {'$eq': 'DXB'}
            

            # Forecast_od is direct value so no need to aggregate and combine table
            Target_Leg_od = conn.JUP_DB_Target_Leg.find(qry_pi,{'_id':0})
            Target_Leg_odDList = list()
            for i in Target_Leg_od:
                #i['book_date'] = time.mktime(datetime.strptime(i['book_date'], "%Y-%m-%d").timetuple())
                #i['dep_date'] = time.mktime(datetime.strptime(i['dep_date'], "%Y-%m-%d").timetuple())
                Target_Leg_odDList.append(i)
                
            Target_Leg_odDFrame = pd.DataFrame(Target_Leg_odDList)    
            dataDict = Target_Leg_odDFrame.to_dict(orient='records')
            #print(mergedTable)
            #print(saleRawTable)
        except Exception as e:
            print e
            dataDict = 'no data'
            
    elif(types['typeof']=='Target_OD'):
        try:
            # In Forecase departure date not in actual date format so just give year and month only so format it accoudingly
            parsedDate = parse(types['depDate'])
            
            # sale create match fields for now added in hadhoc doc aftersome time later we need to change 
            qry_pi = dict()
            #qry_pi['book_date'] = {'$eq': types['trxDate']}
            
            qry_pi['year'] = {'$eq': int(parsedDate.strftime('%Y'))}
            qry_pi['month'] = {'$eq': int(parsedDate.strftime('%m'))}
            qry_pi['od'] = {'$eq': 'DXBMAA'}
            qry_pi['pos'] = {'$eq': types['pos']}
            
            #qry_pi['destination'] = {'$eq': 'DXB'}
            

            # Forecast_od is direct value so no need to aggregate and combine table
            Target_OD = conn.JUP_DB_Target_OD.find(qry_pi,{'_id':0})
            Target_ODDList = list()
            for i in Target_OD:
                #i['book_date'] = time.mktime(datetime.strptime(i['book_date'], "%Y-%m-%d").timetuple())
                #i['dep_date'] = time.mktime(datetime.strptime(i['dep_date'], "%Y-%m-%d").timetuple())
                Target_ODDList.append(i)
                
            Target_ODDFrame = pd.DataFrame(Target_ODDList)    
            dataDict = Target_ODDFrame.to_dict(orient='records')
            #print(mergedTable)
            #print(saleRawTable)
        except Exception as e:
            print e
            dataDict = 'no data'
            
    elif(types['typeof']=='ATPCO'):
        try:
            
            dataDict['info'] = 'Yet to decide' ;
            #print(mergedTable)
            #print(saleRawTable)
        except Exception as e:
            print e
            dataDict = 'no data' ;

    elif(types['typeof']=='Infare'):
        try:
            
            dataDict['info'] = 'Yet to decide' ;
            #print(mergedTable)
            #print(saleRawTable)
        except Exception as e:
            print e
            dataDict = 'no data' ;
                   
    return dataDict
            
#'''
#query_for_data_validation()
toDay = datetime.now() - timedelta(79)
yesterDay = toDay -  timedelta(1)

_dataParameter = dict()
_dataParameter['typeof'] = "Forecast_od";
_dataParameter['pos'] = 'MAA'
_dataParameter['od'] = 'DXBMAA'
_dataParameter['airline'] = 'EK'

_dataParameter['trxDate'] = yesterDay.strftime('%Y-%m-%d')
_dataParameter['depDate'] = toDay.strftime('%Y-%m-%d')
#print(query_for_data_validation(_dataParameter))
#gridData = engClass.query_for_data_validation(_dataParameter)
#print(_getData('Forecast_od'))    

#'''
