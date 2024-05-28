import copy
import json
import pymongo
import calendar
from pymongo.errors import BulkWriteError
from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne
import collections
import pandas as pd
import numpy as np
import math
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import time
from jupiter_AI import client, JUPITER_DB, JUPITER_LOGGER
from jupiter_AI.logutils import measure
from jupiter_AI.network_level_params import SYSTEM_DATE

# Connect mongodb db business layer
# db = client[JUPITER_DB]

dateformat = '%Y-%m-%d'
dateformat_y_m = '%Y-%m'
yearformat = '%Y'
monthformat = '%m'

# userNames = ['Europa_CMB']

@measure(JUPITER_LOGGER)
def uniquGroupArray(array):
    od = ''
    type = ''
    result = dict()
    opt = []
    i=0
    arr_Length = len(array)

    for x in array:

        if od != x['od']:
            if i == 0:
                result[x['type']] = x['value']
                opt = []

            else:
                opt.append(result)
                result = dict()
                result[x['type']] = x['value']

        else:
            if type == x['type']:
                result[x['type']] = result[x['type']] + x['value']

            else:
                result[x['type']] = x['value']

        i = i+1

        result['od'] = x['od']

        od = x['od']
        type = x['type']
        if i == arr_Length:
            opt.append(result)

    return opt

@measure(JUPITER_LOGGER)
def uniqueODTrigger(array):
    od = ''
    type = ''
    result = dict()
    opt = []
    i = 0
    arr_Length = len(array)

    for x in array:
        if (od != x['od']) or (type != x['type']):

            if i == 0:
                result['value'] = 0
                opt = []

            else:
                opt.append({'od': result['od'],
                            'type': result['type'],
                            'value': result['value']})
                result['value'] =0

        i = i +1
        result['value'] = result['value'] +1
        result['od'] = x['od']
        result['type'] = x['type']

        od = x['od']
        type = x['type']
        if i == arr_Length:
            opt.append({'od':result['od'],
                        'type':result['type'],
                        'value':result['value']})

    return opt

@measure(JUPITER_LOGGER)
def compare(a,b):
    if a['od'] < b['od']:
        return -1
    if a['od'] > b['od']:
        return 1
    return 0

@measure(JUPITER_LOGGER)
def compare_depdate(a,b):
    if a['dep_date'] < b['dep_date']:
        return -1
    if a['dep_date'] > b['dep_date']:
        return 1
    return 0

@measure(JUPITER_LOGGER)
def SQLGetElement(data,index):
    num =0
    hour = 0
    while num < index:
        if data[num]['status'].lower() == 'pending':
            hour =0

        else:
            if data[num]['action_date'] is not None and data[num]['action_time'] is not None:
                if data[num]['action_time'].count(':') == 2:
                    # print(str(data[num]['action_date'])+''+str(data[num]['action_time'])+'')
                    testDate = datetime.strptime(str(data[num]['action_date'])+' '+str(data[num]['action_time'])+'', "%Y-%m-%d %H:%M:%S")
                    testDate1 = datetime.strptime(str(data[num]['triggering_event_date'])+' '+str(data[num]['triggering_event_time'])+'', "%Y-%m-%d %H:%M:%S")
                    hour = hour + abs(testDate-testDate1)/36*math.exp(5)

                else:
                    # print(str(data[num]['action_date']) + ' ' + str(data[num]['action_time']) + ':00')
                    testDate = datetime.strptime(str(data[num]['action_date'])+' '+str(data[num]['action_time'])+':00', "%Y-%m-%d %H:%M:%S")
                    testDate1 = datetime.strptime(str(data[num]['triggering_event_date']) + ' ' + str(data[num]['triggering_event_time']) + ':00', "%Y-%m-%d %H:%M:%S")
                    diff =  testDate1 - testDate
                    min, secs = divmod(diff.days * 86400 + diff.seconds, 60)
                    hour, minutes = divmod(min, 60)
                    # print hour
                    #hour = hour + abs(testDate - testDate1) / 36 * math.exp(5)
                    # print testDate1
                    # print testDate
        num = num +1
    return hour

@measure(JUPITER_LOGGER)
def SQLAlg(data):
    SQLArray = dict()
    lenOfArray = len(data)
    tenVal = math.ceil(lenOfArray*.1)
    tfVal = math.ceil(lenOfArray*.25)
    fiVal = math.ceil(lenOfArray*.5)
    sfVal = math.ceil(lenOfArray*.75)
    hnVal = math.ceil(lenOfArray*1)
    SQLArray['10p'] = SQLGetElement(data, tenVal)
    SQLArray['25p'] = SQLGetElement(data, tfVal)
    SQLArray['50p'] = SQLGetElement(data, fiVal)
    SQLArray['75p'] = SQLGetElement(data, sfVal)
    SQLArray['100p'] = SQLGetElement(data, hnVal)
    num = 0
    penCount = 0
    while num<len(data):
        penCount = penCount + 1
        num = num +1
    SQLArray['pending'] = penCount
    return SQLArray

@measure(JUPITER_LOGGER)
def orderPending(data):
    tempArr1 = []
    tempArr2 = []

    for x in data:
        if x['status'] == 'pending':
            tempArr2.append(x)
        else:
            tempArr1.append(x)

    tempOrderedArray = []
    if tempArr1 is None:
        tempOrderedArray = tempArr1

    else:
        tempOrderedArray = tempArr1+tempArr2

    return SQLAlg(tempOrderedArray)

@measure(JUPITER_LOGGER)
def yield_(revenue, distance):
    if distance ==0 or revenue ==0:
        return 0
    else:
        return (revenue/distance)*100

@measure(JUPITER_LOGGER)
def vlyr(pax,pax_,capacity,capacity_):
    if capacity != 0 and capacity_ != 0 and pax != 0 and pax_ != 0:
        return ((pax*(capacity_/capacity))-pax_)/pax_
    elif pax != 0 and pax_ != 0:
        return ((pax-pax_)/pax_)
    else:
        return 0

@measure(JUPITER_LOGGER)
def vlyr_capacity_Adj(pax, pax_):
    if pax != 0 and pax_ != 0:
        return (pax-pax_)/pax_
    else:
        return 0

@measure(JUPITER_LOGGER)
def daysInMonth(month,year):
    return calendar.monthrange(int(year),int(month))[1]

@measure(JUPITER_LOGGER)
def VTGT_pureFuture(monthTargetPax, monthForecastPax):
    if monthTargetPax ==0:
        return 1
    else:
        return (monthForecastPax - monthTargetPax)/monthTargetPax

@measure(JUPITER_LOGGER)
def VTGT_purePast(monthTargetPax, monthFlownPax):
    if monthTargetPax ==0:
        return 1
    else:
        return (monthFlownPax - monthTargetPax)/monthTargetPax

@measure(JUPITER_LOGGER)
def VTGT_mix(monthTargetPax_, onlyFlownPax_, monthForecastPax_, count_, curr_month, curr_year):
    if monthTargetPax_ != 0:
        return ((onlyFlownPax_ + (monthForecastPax_/daysInMonth(curr_month, curr_year))*(daysInMonth(curr_month, curr_year) - count_))-monthTargetPax_)/monthTargetPax_
    else:
        return 0

@measure(JUPITER_LOGGER)
def Sale_VTGT_mix(monthTargetPax_, onlyFlownPax_, onlySalesPax):
    if monthTargetPax_ != 0 :
        return ((onlyFlownPax_+onlySalesPax)-monthTargetPax_)/monthTargetPax_
    else:
        return 0

@measure(JUPITER_LOGGER)
def main(user, comp_each, client):
    db = client[JUPITER_DB]
# for index, eachUser in enumerate(userNames):
    #print index, eachUser
    # user = eachUser
    curDay = datetime.strptime(SYSTEM_DATE, '%Y-%m-%d')

    if len(str(curDay.day)) == 2:
        Date = str(curDay.day)
    else:
        Date = '0'+str(curDay.day)

    if len(str(curDay.month)) == 2:
        Month = str(curDay.month)
    else:
        Month = '0'+str(curDay.month)

    Year = str(curDay.year)
    snap_date_para = Year+'-'+Month+'-'+Date

    sig_count = 0;
    total_count = 0;
    non_sig_count = 0;
    sig_cur = db.JUP_DB_Market_Significance.aggregate([
        {'$unwind': '$significance'},
    {'$match': {
        'online': True, 'significance.name': user,
    }},
    {'$group': {
        '_id': '$significance.name',
        'sig_count': {'$sum': {'$cond': [{'$eq': ['$significance.significant_flag', True]}, 1, 0]}},
        'non_sig_count': {'$sum': {'$cond': [{'$eq': ['$significance.significant_flag', False]}, 1, 0]}},
        'tot_count': {'$sum': 1}
    }}
    ], allowDiskUse =  True)

    for x in sig_cur:
        sig_count = x['sig_count']
        total_count = x['tot_count']
        non_sig_count = x['non_sig_count']

    yesterDay = curDay-timedelta(days=+1)
    weekDay = curDay-timedelta(days=+8)
    monthDay = curDay-timedelta(days=+30)
    yearDay = str(curDay.year)+'-01-01'

    curr_date = curDay.strftime("%d")
    curr_month = curDay.strftime("%m")
    original_month = curDay.strftime("%d")

    # comp = db.JUP_DB_Booking_Class.distinct("comp")
    # comp.append('TL')

    # for comp_each in comp:
    user_data = db.JUP_DB_User.find_one({'name': user})
    posList = user_data['list_of_pos']
    queryBuilder = dict()
    queryBuilder['$or'] = []

    for x in user_data['list_of_pos_OD']:
        if x['origin'] == 'All' and x['destination'] == 'All':
            local = dict()
            if x['compartment'] == 'TL' and comp_each == 'TL':
                local['$and'] = [{'pos':x['pos']}]
                queryBuilder['$or'].append(local)

            elif x['compartment'] == 'TL' or x['compartment'] == comp_each:
                local['$and'] = [{'pos':x['pos']},{'compartment':comp_each}]
                queryBuilder['$or'].append(local)


        elif x['origin'] != 'All' and x['destination'] != 'All':
            local = dict()
            if x['compartment'] == 'TL' and comp_each == 'TL':
                local['$and'] = [{'pos':x['pos']}, {'origin':x['origin']}, {'destination':x['destination']}]
                queryBuilder['$or'].append(local)

            elif x['compartment'] == 'TL' or x['compartment'] == comp_each:
                local['$and'] = [{'pos':x['pos']}, {'origin':x['origin']}, {'destination':x['destination']}, {'compartment': comp_each}]
                queryBuilder['$or'].append(local)


        elif x['origin'] != 'All':
            local = dict()
            if x['compartment'] == 'TL' and comp_each == 'TL':
                local['$and'] = [{'pos':x['pos']}, {'origin':x['origin']}]
                queryBuilder['$or'].append(local)

            elif x['compartment'] == 'TL' or x['compartment'] == comp_each:
                local['$and'] = [{'pos':x['pos']}, {'origin': x['origin']}, {'compartment': comp_each}]
                queryBuilder['$or'].append(local)


        elif x['destination'] != 'All':
            local = dict()
            if x['compartment'] == 'TL' and comp_each == 'TL':
                local['$and'] = [{'pos': x['pos']}, {'destination': x['destination']}]
                queryBuilder['$or'].append(local)

            elif x['compartment'] == 'TL' or x['compartment'] == comp_each:
                local['$and'] = [{'pos': x['pos']}, {'destination': x['destination']}, {'compartment': comp_each}]
                queryBuilder['$or'].append(local)

    num = -1
    while (num <= 3):
        dt_range = curDay + relativedelta(months=+(num-1))
        #print dt_range
        curr_month = datetime.strftime(dt_range, '%m')

        # if len(str(curDay.month -1 + (num))) == 2:
        #     curr_month = str(curDay.month -1 + (num))[-2:]
        # else:
        #     curr_month = '0' + str(curDay.month -1 + (num))[-2:]

        # curr_month = '0' + str(curDay.month -1 + (num))[-2:]
        try:
            # print(queryBuilder['$or'])

            curr_year = datetime.strftime(dt_range, '%Y')
            str_date = curr_year+'-'+curr_month+'-'+curr_date
            cursor = db.get_collection('JUP_DB_Workflow').find({'$or': queryBuilder['$or'], "update_date" : str_date},{"triggering_event_date" : 1,"recommendation_category" : 1,"status" :1,"od" :1,"pos" :1,"compartment" :1,"type_of_trigger":1, "trigger_type":1,"action_date" : 1, "action_time" : 1,"triggering_event_time" : 1, "type" : 1})
            print(curr_month, curr_year, str_date)
            curAry = []
            cuObj = dict()
            odUserAry = []
            triggerOverAllFare = 0
            triggerOverAllCritical = 0
            odArr = []
            TriggerOd = dict()
            TriggerType = dict()
            TriggerOverall = dict()
            Trigger = dict()
            triggerStatus = []
            triggerStatusObjDay = dict()
            triggerStatusObjMonth = dict()
            triggerStatusObjWeek = dict()
            triggerStatusObjYTD = dict()
            triggerStatusObjRes = dict()
            sysTriggerType = dict()
            TriggerOdSub = []
            SQLArray = []
            SQLObj = dict()
            SQLDay = []
            SQLWeek = []
            SQLMonth = []
            SQLYTD = []
            SQLRes = dict()
            SQLAllTrigger = dict()
            ManualSQLObj = dict()
            ManualSQLDay = []
            ManualSQLWeek = []
            ManualSQLMonth = []
            ManualSQLYTD = []
            SystemSQLObj = dict()
            SystemSQLDay = []
            SystemSQLWeek = []
            SystemSQLMonth = []
            SystemSQLYTD = []
            queryBuild = dict()
            query_builds = dict()
            flag_initial = False
            target = dict()
            for x in cursor:
                if str_date == x['triggering_event_date']:
                    curObj = x

                    if x['recommendation_category'] == 'A' and x['status'].lower() == 'pending':
                        triggerOverAllCritical = triggerOverAllCritical +1

                    if x['status'].lower() == 'pending':
                        queryBuild = dict()

                        if not flag_initial:
                            query_builds['$or'] = []
                            flag_initial = True

                        queryBuild['$and'] = [{'od': {'$eq':x['od']}}, {'pos': {'$eq':x['pos']}}, {'compartment': {'$eq': x['compartment']}}]
                        query_builds['$or'].append(queryBuild)

                        curAry.append(curObj)
                        odArr.append(x['od'])

                        if x['od'] not in TriggerOd:
                            TriggerOd[x['od']] = 1
                        else:
                            TriggerOd[x['od']] = TriggerOd[x['od']] + 1

                        if x['type_of_trigger'] not in TriggerType:
                            TriggerType[x['type_of_trigger']] = 1
                        else:
                            TriggerType[x['type_of_trigger']] = TriggerType[x['type_of_trigger']] +1

                        if x['type_of_trigger'].lower() == 'system':
                            if x['trigger_type'] not in sysTriggerType:
                                sysTriggerType[x['trigger_type']] = 1
                            else:
                                sysTriggerType[x['trigger_type']] =  sysTriggerType[x['trigger_type']] +1

                        TriggerOdSub.append({'od': x['od'], 'type': x['type_of_trigger']})

                        #i = 0
                        #while i < len(TriggerOd)-1:
                        #   target = target[TriggerOd[i]]
                        #   i = i+1

                    if x['status'] not in triggerStatusObjDay:
                        triggerStatusObjDay[x['status']] = 1
                    else:
                        triggerStatusObjDay[x['status']] = triggerStatusObjDay[x['status']] +1


                if datetime.strptime(str(x['triggering_event_date']), "%Y-%m-%d").date() >= weekDay.date():
                    if x['status'] not in triggerStatusObjWeek:
                        triggerStatusObjWeek[x['status']] = 1
                    else:
                        triggerStatusObjWeek[x['status']] = triggerStatusObjWeek[x['status']] + 1

                if datetime.strptime(str(x['triggering_event_date']), "%Y-%m-%d").date() >= monthDay.date():
                    if x['status'] not in triggerStatusObjMonth:
                        triggerStatusObjMonth[x['status']] = 1
                    else:
                        triggerStatusObjMonth[x['status']] = triggerStatusObjMonth[x['status']] +1

                if datetime.strptime(str(x['triggering_event_date']), "%Y-%m-%d").date() >= datetime.strptime(yearDay, "%Y-%m-%d").date():
                    if x['status'] not in triggerStatusObjYTD:
                        triggerStatusObjYTD[x['status']] = 1
                    else:
                        triggerStatusObjYTD[x['status']] = triggerStatusObjYTD[x['status']] +1

                if str_date == str(x['triggering_event_date']):

                    SQLDay.append(x)

                if datetime.strptime(str(x['triggering_event_date']), "%Y-%m-%d").date() >= weekDay.date():
                    SQLWeek.append(x)

                if datetime.strptime(str(x['triggering_event_date']), "%Y-%m-%d").date() >= monthDay.date():
                    SQLMonth.append(x)

                if datetime.strptime(str(x['triggering_event_date']), "%Y-%m-%d").date() >= datetime.strptime(yearDay, "%Y-%m-%d").date():
                    SQLYTD.append(x)

                if x['type_of_trigger'].lower() == 'system':

                    if str_date == x['triggering_event_date']:
                        SystemSQLDay.append(x)

                    if datetime.strptime(str(x['triggering_event_date']), "%Y-%m-%d").date() >= weekDay.date():
                        SystemSQLWeek.append(x)

                    if datetime.strptime(str(x['triggering_event_date']), "%Y-%m-%d").date() >= monthDay.date():
                        SystemSQLMonth.append(x)

                    if datetime.strptime(str(x['triggering_event_date']), "%Y-%m-%d").date() >= datetime.strptime(yearDay, "%Y-%m-%d").date():
                        SystemSQLYTD.append(x)

                elif x['type_of_trigger'].lower() == 'manual':

                    if str_date == x['triggering_event_date']:
                        ManualSQLDay.append(x)

                    if datetime.strptime(str(x['triggering_event_date']), "%Y-%m-%d").date() >= weekDay.date():
                        ManualSQLWeek.append(x)

                    if datetime.strptime(str(x['triggering_event_date']), "%Y-%m-%d").date() >= monthDay.date():
                        ManualSQLMonth.append(x)

                    if datetime.strptime(str(x['triggering_event_date']), "%Y-%m-%d").date() >= datetime.strptime(yearDay, "%Y-%m-%d").date():
                        ManualSQLYTD.append(x)

            triggerStatusObjRes['Day'] = triggerStatusObjDay
            triggerStatusObjRes['Week'] = triggerStatusObjWeek
            triggerStatusObjRes['Month'] = triggerStatusObjMonth
            triggerStatusObjRes['YTD'] = triggerStatusObjYTD

            triggerOverAllOd = list(set(odArr))
            od = ''

            uniqueFareCount = db.JUP_DB_Workflow_OD_User.aggregate([
                {'$match':query_builds},
                {'$unwind':'$fares_docs'},
                {'$group':{
                    '_id':'$fares_docs.fare_basis'
                }},
                {
                    '$group':{
                        '_id':None,
                        'count':{'$sum':1}
                    }
                },
                # {'$out':'Temp_LP_Fare_Count'}
            ])
            for doc in uniqueFareCount:
                if 'count' in doc:
                    TriggerOverall['Fares'] = doc['count']
                else:
                    TriggerOverall['Fares'] = 0
            # uniqueFareCount = db.Temp_LP_Fare_Count.find_one()

            triggerOverAllTotal = len(curAry)

            TriggerType['work_package'] = db.JUP_DB_WorkPackage.find({'user':user, 'filingstatus':'pending'}).count()
            TriggerType['sales_request'] = db.JUP_DB_PSO.find({'username': user, 'status': 'Pending'}).count()
            TriggerOverall['Critical'] = triggerOverAllCritical
            TriggerOverall['Total'] = triggerOverAllTotal

            if triggerOverAllOd is not None:
                TriggerOverall['od'] = len(triggerOverAllOd)
            else:
                TriggerOverall['od'] = 0

            Trigger['overAll'] = TriggerOverall
            TriggerOd['detail'] = uniquGroupArray(sorted(uniqueODTrigger(TriggerOdSub)))
            Trigger['triggerOd'] = TriggerOd
            TriggerType['detail'] = sysTriggerType
            Trigger['triggerTypes'] = TriggerType
            Trigger['triggerStatus'] = triggerStatusObjRes

            SQLObj['Day'] = orderPending(SQLDay)
            SQLObj['Week'] = orderPending(SQLWeek)
            SQLObj['Month'] = orderPending(SQLMonth)
            SQLObj['Year'] = orderPending(SQLYTD)

            KPIObj = dict()
            KPIObjSubDoc = dict()
            saleRev = 0
            salePax = 0
            saleRev_1 = 0
            salePax_1 = 0
            capacity_1 = 0
            capacity = 0
            distance = 0
            distance_1 = 0
            distance_flown = 0
            distance_flown_1 = 0
            onlyFlownPax = 0
            onlyFlownRev = 0
            onlySalesPax = 0
            onlySalesRev = 0
            monthTargetPax = 0
            monthTargetRev = 0
            monthForecastPax = 0
            monthForecastRev = 0
            count = 0
            agility =0
            MarketShare = 0
            MarketShare_1 = 0
            ReviewOD =0
            landing_page_cursor = dict()
            data = db.JUP_DB_Landing_Page_.find_one({'user':user,'dep_month':int(curr_month),'dep_year':int(curr_year),'compartment':comp_each})
            landing_page_cursor['datas'] = data

            updateDepDateData = []
            monthPax_Dist = 0
            monthPax_Dist_1 =0

            for x in landing_page_cursor['datas']['this_month']['dep_date']:
                if datetime.strptime(str(x['dep_date']), "%Y-%m-%d") > yesterDay:
                    if x['sale_pax'] == 0:
                        x['avgFare'] =0
                    else:
                        x['avgFare'] = x['sale_revenue']/x['sale_pax']

                    if x['distance'] == 0:
                        x['yield'] = 0
                    else:
                        x['yield'] = (x['sale_revenue']/x['distance'])*100


                    monthPax_Dist = monthPax_Dist + x['distance']
                    monthPax_Dist_1 = monthPax_Dist_1 + x['distance_1']

                elif datetime.strptime(str(x['dep_date']), "%Y-%m-%d") <= yesterDay:
                    if x['flown_pax'] == 0:
                        x['avgFare'] = 0
                    else:
                        x['avgFare'] = x['flown_revenue']/x['flown_pax']

                    if (x['distance_flown'] and x['flown_pax']) == 0:
                        x['yield'] = 0
                    else:
                        x['yield'] = (x['flown_revenue'] / x['distance_flown']) * 100

                    monthPax_Dist = monthPax_Dist + x['distance_flown']
                    monthPax_Dist_1 = monthPax_Dist_1 + x['distance_flown_1']

                updateDepDateData.append(x)


            target_pax_from_original = landing_page_cursor['datas']['this_month']['target_pax']
            target_rev_from_original = landing_page_cursor['datas']['this_month']['target_revenue']
            target_avg_from_original = landing_page_cursor['datas']['this_month']['target_avgFare']
            forecast_pax_from_original = landing_page_cursor['datas']['this_month']['forecast_pax']
            forecast_rev_from_original = landing_page_cursor['datas']['this_month']['forecast_revenue']
            forecast_avg_from_original = landing_page_cursor['datas']['this_month']['forecast_avgFare']
            target_pax_from_original_1 = landing_page_cursor['datas']['this_month']['target_pax_1']
            target_rev_from_original_1 = landing_page_cursor['datas']['this_month']['target_revenue_1']
            target_avg_from_original_1 = landing_page_cursor['datas']['this_month']['target_avgFare_1']
            forecast_pax_from_original_1 = 0
            forecast_rev_from_original_1 = 0
            forecast_avg_from_original_1 = 0
            market_pax_from_original = 0
            market_rev_from_original = 0
            market_size_from_original = 0
            market_pax_from_original_1 = 0
            market_rev_from_original_1 = 0
            market_size_from_original_1 = 0
            paxVTGT = 0
            revVTGT = 0
            yieldVTGT = 0
            avgVTGT = 0
            paxForecastVTGT = 0
            revForecastVTGT = 0
            yieldForecastVTGT = 0
            avgForecastVTGT = 0
            MarketShareVTGT = 0
            FMS = 0

            FMSObj = db.JUP_DB_Pos_OD_Compartment_new.aggregate([
                {
                    '$match':
                        {
                            '$or': queryBuilder['$or'],
                            'month': {'$eq': int(curr_month)},
                            'year': {'$eq': int(curr_year)},
                        }
                },
                {
                    '$unwind': '$top_5_comp_data'
                },
                {
                    '$match':
                        {
                        'top_5_comp_data.capacity': {'$ne': None}
                        }
                },
                {
                '$project':
                    {
                        'pos': '$pos',
                        'airline': '$top_5_comp_data.airline',
                        'capacity_rating': {'$multiply': ['$top_5_comp_data.capacity', '$top_5_comp_data.overall_rating']},
                        'capacity': '$top_5_comp_data.capacity',
                        'pax': '$top_5_comp_data.pax',
                        'market_size': '$top_5_comp_data.market_size',
                        'pax_1': '$top_5_comp_data.pax_1',
                        'market_size_1': '$top_5_comp_data.market_size_1',
                        'rating': '$top_5_comp_data.rating'

                    }
            },

                {
                    '$group':
                        {
                            '_id':
                                {
                                'pos': None
                                },
                            'pax': {'$sum': {'$cond': [{'$and': [{'$eq': ['$airline', 'AI']},{'$ne':['$pax', None]}]}, '$pax', 0]}},
                            'market_size': {'$sum': {'$cond': [{'$and': [{'$eq': ['$airline','AI']},{'$ne': ['$market_size', None]}]}, '$market_size',0]}},
                            'pax_1': {'$sum': {'$cond': [{'$and': [{'$eq': ['$airline','AI']}, {'$ne': ['$pax_1', None]}]}, '$pax_1',0]}},
                            'market_size_1': {'$sum': {'$cond': [{'$and': [{'$eq': ['$airline','AI']}, {'$ne': ['$market_size_1', None]}]}, '$market_size_1',0]}},
                            'capacity_rating_FZ': {'$sum': {'$cond': [{'$eq': ['$airline','AI']}, '$capacity_rating',0]}},
                            'capacity_rating_total': {'$sum': '$capacity_rating'},
                            'host_capacity': {'$sum': {'$cond': [{'$eq': ['$airline', 'AI']}, '$capacity', 0]}},
                            'host_rating': {'$sum': {'$cond': [{'$eq': ['$airline', 'AI']}, '$rating', 0]}},
                            # 'comp_capacity': {'$sum': {'$cond': [{'$eq': ['$airline', 'FZ']}, 0, '$capacity']}},
                            'comp_capacity': {'$sum': '$capacity'},
                            # 'comp_rating': {'$sum': {'$cond': [{'$eq': ['$airline', 'FZ']},0, '$rating']}},
                            'comp_rating': {'$sum': '$rating'},
                        }
                },
                {"$match": {
                    "capacity_rating_total": {"$ne": 0}
                }},
                {
                    '$project':
                        {
                            '_id': 0,
                            'pos': '$_id.pos',
                            'capacity_rating_FZ': '$capacity_rating_FZ',
                            'capacity_rating_total': '$capacity_rating_total',
                            'host_capacity': '$host_capacity',
                            'host_rating': '$host_rating',
                            'comp_capacity': '$comp_capacity',
                            'comp_rating': '$comp_rating',
                            'FMS': {'$multiply': [{'$divide': ['$capacity_rating_FZ', '$capacity_rating_total']}, 100]},
                            'pax': '$pax',
                            'market_size': '$market_size',
                            'pax_1': '$pax_1',
                            'market_size_1': '$market_size_1',
                        }
                }
            ])

            capacity_rating_FZ = 0
            capacity_rating_total = 0
            host_capacity = 0
            host_rating = 0
            comp_capacity = 0
            comp_rating = 0

            for x in FMSObj:
                FMS = x['FMS']
                capacity_rating_FZ = x['capacity_rating_FZ']
                capacity_rating_total = x['capacity_rating_total']
                host_capacity = x['host_capacity']
                host_rating = x['host_rating']
                comp_capacity = x['comp_capacity']
                comp_rating = x['comp_rating']
                market_pax_from_original = x['pax']
                market_rev_from_original = 0
                market_size_from_original = x['market_size']
                market_pax_from_original_1 = x['pax_1']
                market_rev_from_original_1 = 0
                market_size_from_original_1 = x['market_size_1']

            bookingPax = 0
            bookingPax_1 = 0
            MonthPaxDist = 0
            MonthPaxDist_1 = 0
            snap_pax = 0
            snap_revenue = 0
            paxCapa = 0
            revCapa = 0

            for x in landing_page_cursor['datas']['this_month']['dep_date']:
                if datetime.strptime(str(x['dep_date']), "%Y-%m-%d") > yesterDay:
                    onlySalesPax = onlySalesPax + x['sale_pax']
                    onlySalesRev = onlySalesRev + x['sale_revenue']
                    salePax = salePax + x['sale_pax']
                    saleRev = saleRev + x['sale_revenue']
                    salePax_1 = salePax_1 + x['sale_pax_1']
                    saleRev_1 = saleRev_1 + x['sale_revenue_1']
                    MonthPaxDist = MonthPaxDist + x['distance']
                    MonthPaxDist_1 = MonthPaxDist_1 + x['distance_1']
                    snap_pax = snap_pax + x['sale_snap_pax']
                    paxCapa = paxCapa + x['sale_paxCapaAdj']
                    revCapa = revCapa + x['sale_revenueCapaAdj']
                    snap_revenue = snap_revenue + x['sale_snap_revenue']
                    bookingPax = bookingPax + x['book_paxCapaAdj']
                    bookingPax_1 = bookingPax_1 + x['book_pax_1']

                elif datetime.strptime(str(x['dep_date']), "%Y-%m-%d") <= yesterDay:
                    salePax = salePax+x['flown_pax']
                    saleRev = saleRev + x['flown_revenue']
                    salePax_1 = salePax_1+x['flown_pax_1']
                    saleRev_1 = saleRev_1+x['flown_revenue_1']
                    onlyFlownPax = onlyFlownPax+x['flown_pax']
                    onlyFlownRev = onlyFlownRev + x['flown_revenue']
                    MonthPaxDist = MonthPaxDist + x['distance_flown']
                    MonthPaxDist_1 = MonthPaxDist_1 + x['distance_flown_1']
                    snap_pax = snap_pax + x['flown_snap_pax']
                    snap_revenue = snap_revenue + x['flown_snap_revenue']
                    paxCapa = paxCapa + x['flown_paxCapaAdj']
                    revCapa = revCapa + x['flown_revenueCapaAdj']
                    bookingPax = bookingPax + x['book_paxCapaAdj']
                    bookingPax_1 = bookingPax_1+x['book_pax_1']

                    count = count + 1

            if landing_page_cursor['datas'] is None:
                capacity = 0
            else:
                capacity = landing_page_cursor['datas']['this_month']['capacity']

            if landing_page_cursor['datas'] is None:
                capacity_1 = 0
            else:
                capacity_1 = landing_page_cursor['datas']['this_month']['capacity_1']

            if landing_page_cursor['datas'] is None:
                distance = 0
            else:
                distance = landing_page_cursor['datas']['this_month']['distance']

            flownRevenueLastYRMonth = landing_page_cursor['datas']['this_month']['flown_revenue_1']
            TargetPaxDist = landing_page_cursor['datas']['this_month']['distance_Target_Pax']

            if market_size_from_original == 0:
                MarketShare = 0
            else:
                MarketShare = (market_pax_from_original/(market_size_from_original)*100)

            if market_size_from_original_1 == 0:
                MarketShare_1 = 0
            else:
                MarketShare_1 = (market_pax_from_original_1/(market_size_from_original_1)*100)

            thisMonthFromLanding = landing_page_cursor['datas']['this_month']
            monthTargetPax = target_pax_from_original
            monthTargetRev = target_rev_from_original
            monthForecastPax = forecast_pax_from_original
            monthForecastRev = forecast_rev_from_original

            if market_size_from_original == 0:
                MarketShare = 0
            else:
                MarketShare = (market_pax_from_original/market_size_from_original)*100

            comprtmentBuilder = dict()

            if comp_each == 'TL':
                comprtmentBuilder['compartment'] = {'$ne': None}
            else:
                comprtmentBuilder['compartment'] = {'$eq': comp_each}

            Target = db.JUP_DB_Analyst_Target.find_one({'user': user, 'compartment': comprtmentBuilder['compartment']})
            TargetRevenue = monthTargetRev
            TargetPax = Target['kpi']['ticket']
            TargetAgility = Target['kpi']['agility']
            TargetFMS = Target['kpi']['ms_growth']
            TargetRevOfTopOD = Target['kpi']['review_od']
            TargetCurrency = Target['kpi']['revenue_unit']
            agility_unit = Target['kpi']['agility_unit']
            ticket_unit = Target['kpi']['ticket_unit']
            ms_growth_unit = Target['kpi']['ms_growth_unit']
            review_od_unit = Target['kpi']['review_od_unit']
            tiles = dict()
            Fares = dict()
            tiles['Fares'] = None
            signOD = dict()
            signOD['totalOD'] = non_sig_count
            signOD['sigOD'] = sig_count
            signOD['noneSigOD'] = non_sig_count
            signOD['totalOD_count'] = total_count
            signOD['sigOD_count'] = sig_count
            signOD['noneSigOD_count'] = non_sig_count
            tiles['Sig'] = signOD
            if capacity_1 is None or capacity_1 == 0:
                capacity_1 = capacity

            bookingVLYR = vlyr_capacity_Adj(bookingPax, bookingPax_1)*100
            paxVLYR = vlyr_capacity_Adj(paxCapa, salePax_1) * 100
            revVLYR = vlyr_capacity_Adj(revCapa, saleRev_1) * 100

            if yield_(saleRev_1, MonthPaxDist_1) == 0:
                yieldVLYR = 1
            else:
                yieldVLYR = (yield_(saleRev, MonthPaxDist) - yield_(saleRev_1, MonthPaxDist_1))/yield_(saleRev_1, MonthPaxDist_1)*100

            if salePax_1 == 0 or saleRev_1 == 0 or snap_pax == 0:
                avgVLYR = 0
            else:
                avgVLYR = ((snap_revenue/snap_pax)-(saleRev_1/salePax_1))/(saleRev_1/salePax_1)*100

            MarketShareVLYR = MarketShare - MarketShare_1

            if FMS == 0:
                MarketShareVTGT = 100
            else:
                MarketShareVTGT = MarketShare-FMS

            if int(original_month) == int(curr_month):
                paxForecastVTGT = VTGT_mix(monthTargetPax, onlyFlownPax, monthForecastPax, count, int(curr_month), int(curr_year))*100
                revForecastVTGT = VTGT_mix(monthTargetRev, onlyFlownRev, monthForecastRev, count, int(curr_month), int(curr_year))*100

                if paxForecastVTGT == 0:
                    avgForecastVTGT = revForecastVTGT
                else:
                    avgForecastVTGT = revForecastVTGT/paxForecastVTGT

            elif int(original_month) > int(curr_month):
                paxForecastVTGT = VTGT_purePast(monthTargetPax, onlyFlownPax)*100
                revForecastVTGT = VTGT_purePast(monthTargetRev, onlyFlownRev)*100

                if paxForecastVTGT == 0:
                    avgForecastVTGT = revForecastVTGT
                else:
                    avgForecastVTGT = revForecastVTGT/paxForecastVTGT

            elif int(original_month) < int(curr_month):
                paxForecastVTGT = VTGT_pureFuture(monthTargetPax, monthForecastPax)*100
                revForecastVTGT = VTGT_pureFuture(monthTargetRev, monthForecastRev)*100

                if paxForecastVTGT == 0:
                    avgForecastVTGT = revForecastVTGT
                else:
                    avgForecastVTGT = revForecastVTGT/paxForecastVTGT

            if MonthPaxDist == 0:
                yieldForecastVTGT = revForecastVTGT*100
            else:
                yieldForecastVTGT = (revForecastVTGT/MonthPaxDist)*100


            if int(original_month) == int(curr_month):
                paxVTGT = Sale_VTGT_mix(monthTargetPax, onlyFlownPax, onlySalesPax)*100
                revVTGT = Sale_VTGT_mix(monthTargetRev, onlyFlownRev, onlySalesRev)*100
                if monthTargetPax == 0:
                    avgVTGT = 0
                else :
                    avgVTGT = ((((onlyFlownRev+onlySalesRev)/(onlyFlownPax+onlySalesPax))-(monthTargetRev/monthTargetPax))/(monthTargetRev/monthTargetPax))*100

            elif int(original_month) > int(curr_month):
                paxVTGT = VTGT_purePast(monthTargetPax, onlyFlownPax)*100
                revVTGT = VTGT_purePast(monthTargetRev, onlyFlownRev)*100
                if monthTargetPax == 0:
                    avgVTGT = 0
                else :
                    avgVTGT = (((onlyFlownRev/onlyFlownPax)-(monthTargetRev/monthTargetPax))/(monthTargetRev/monthTargetPax))*100

            elif int(original_month) < int(curr_month):
                paxVTGT = VTGT_pureFuture(monthTargetPax, onlySalesPax)*100
                revVTGT = VTGT_pureFuture(monthTargetRev, onlySalesRev)*100
                if monthTargetPax == 0 or onlySalesPax == 0 or monthTargetPax ==0:
                    avgVTGT = 0
                else :
                    avgVTGT = (((onlySalesRev/onlySalesPax)-(monthTargetRev/monthTargetPax))/(monthTargetRev/monthTargetPax))*100

            TargetYield = yield_(monthTargetRev, TargetPaxDist)
            ActualYield = yield_(saleRev, MonthPaxDist)
            LastYRYield = yield_(saleRev_1, MonthPaxDist_1)

            if TargetYield ==0:
                yieldVTGT = 100
            else:
                yieldVTGT = ((ActualYield-TargetYield)/TargetYield)*100

            paxt = dict()
            revt = dict()
            yieldt = dict()
            avgt = dict()
            markett = dict()

            paxt['vlyr'] = paxVLYR
            paxt['vtgt'] = paxVTGT
            paxt['sale'] = onlySalesPax
            paxt['flown'] = onlyFlownPax
            paxt['target'] = monthTargetPax
            paxt['forecast'] = monthForecastPax
            paxt['pax_Adj_Capa_SaleFlown'] = paxCapa
            revt['rev_Adj_Capa_SaleFlown'] = revCapa
            paxt['Forecast_vtgt'] = paxForecastVTGT
            paxt['value'] = salePax
            paxt['value_1'] = salePax_1
            revt['vlyr'] = revVLYR
            revt['value'] = saleRev
            revt['value_1'] = saleRev_1
            yieldt['vlyr'] = yieldVLYR
            yieldt['value'] = yield_(saleRev, MonthPaxDist)
            yieldt['value_LY'] = yield_(saleRev_1, MonthPaxDist_1)
            yieldt['revenueLY'] = saleRev_1
            yieldt['paxDist'] = MonthPaxDist_1

            yieldt['Target_yield'] = TargetYield
            avgt['vlyr'] = avgVLYR

            if saleRev ==0 or salePax ==0:
                avgt['value'] = 0
            else:
                avgt['value'] = saleRev/salePax

            markett['vlyr'] = MarketShareVLYR
            markett['value'] = MarketShare
            revt['vtgt'] = revVTGT
            revt['Forecast_vtgt'] = revForecastVTGT
            yieldt['Forecast_vtgt'] = yieldForecastVTGT
            yieldt['vtgt'] = yieldVTGT
            avgt['Forecast_vtgt'] = avgForecastVTGT
            avgt['vtgt'] = avgVTGT
            markett['vtgt'] = MarketShareVTGT

            totalTrigger = len(SQLMonth)
            percentageOfComp = 0
            processedTriggerWithinOneDay = 0

            for x in SQLMonth:
                if x['triggering_event_date'] == x['action_date']:
                    processedTriggerWithinOneDay = processedTriggerWithinOneDay +1

            if totalTrigger != 0:
                percentageOfComp = (processedTriggerWithinOneDay/totalTrigger)*100

            revForKPI = onlyFlownRev + ((monthForecastRev/daysInMonth(curr_month, curr_year)*(daysInMonth(curr_month, curr_year) - count)))

            KPIObj['Revenue'] = {'current': revForKPI, 'target': TargetRevenue, 'vtgt': revForecastVTGT}
            KPIObj['Forward_Booking'] = {'current': bookingVLYR, 'target': TargetPax, 'unit': ticket_unit}
            KPIObj['Market_Share_Growth'] = {'current': MarketShareVLYR, 'target': TargetFMS,'unit': ms_growth_unit}
            KPIObj['Agility'] = {'current': agility, 'target': TargetAgility, 'unit': agility_unit}
            KPIObj['Review_of_Top_OD'] = {'current': percentageOfComp, 'target': TargetRevOfTopOD, 'unit': review_od_unit}

            tiles['pax'] = paxt
            tiles['revenue'] = revt
            tiles['market'] = markett
            tiles['yield'] = yieldt
            tiles['avgFare'] = avgt

            SystemSQLObj['Day'] = orderPending(SystemSQLDay)
            SystemSQLObj['Week'] = orderPending(SystemSQLWeek)
            SystemSQLObj['Month'] = orderPending(SystemSQLMonth)
            SystemSQLObj['Year'] = orderPending(SystemSQLYTD)

            ManualSQLObj['Day'] = orderPending(ManualSQLDay)
            ManualSQLObj['Week'] = orderPending(ManualSQLWeek)
            ManualSQLObj['Month'] = orderPending(ManualSQLMonth)
            ManualSQLObj['Year'] = orderPending(ManualSQLYTD)

            SQLAllTrigger['manual'] = ManualSQLObj
            SQLAllTrigger['system'] = SystemSQLObj
            # print sorted(updateDepDateData, key = lambda i: i['dep_date'])
            # print updateDepDateData.sort(compare_depdate)

            db.JUP_DB_Landing_Page_.update({
                'user': user, 'dep_month': int(curr_month), 'dep_year': int(curr_year), 'compartment': comp_each},
                {
                    '$set': {
                        'currency': TargetCurrency,
                        'this_month.dep_date': sorted(updateDepDateData, key = lambda i: i['dep_date']),
                        'this_month.market.pax': market_pax_from_original,
                        'this_month.market.pax_1': market_pax_from_original_1,
                        'this_month.market.market_size': market_size_from_original,
                        'this_month.market.market_size_1': market_size_from_original_1,
                        'this_month.market.capacity_rating_total': capacity_rating_total,
                        'this_month.market.capacity_rating_FZ': capacity_rating_FZ,
                        'this_month.market.host_capacity': host_capacity,
                        'this_month.market.host_rating': host_rating,
                        'this_month.market.comp_capacity': comp_capacity,
                        'this_month.market.comp_rating': comp_rating,
                        'this_month.market.FMS': FMS,
                        'this_month.Actual_PaxVsDistance': monthPax_Dist,
                        'this_month.Actual_PaxVsDistance_1': monthPax_Dist_1,
                        'this_month.capacity': capacity,
                        'this_month.capacity_1': capacity_1,
                        'last_update_date': curDay,
                        'Trigger': Trigger,
                        'SQL_All_Triggers': SQLAllTrigger,
                        'SQL': SQLObj,
                        'KPI': KPIObj,
                        'Tiles': tiles
                        }
                },
                upsert=True
                )
            try:
                #print user
                if db.JUP_DB_User.find_one({'cluster': {'$eq': 'network'}, 'name': {'$eq': user}, 'active': True}) is not None:
                    doc = db.JUP_DB_Landing_Page_.find_one(
                        {'user': user, 'dep_month': int(curr_month), 'dep_year': int(curr_year), 'compartment': comp_each},{"_id" : 0})
                    network_user = db.JUP_DB_User.distinct('name',
                                                           {'cluster': {'$eq': 'network'}, 'name': {'$ne': user},
                                                            'active': True})
                    # print userName
                    #print network_user
                    doc1 = list()
                    doc1.append(doc)
                    for each_network_user in network_user:
                        for each_doc in doc1:

                            each_doc['user'] = each_network_user
                            db.JUP_DB_Landing_Page_.update(
                                {
                                    'user': each_doc['user'],
                                    'dep_month': each_doc['dep_month'],
                                    'dep_year': each_doc['dep_year'],
                                    'compartment': each_doc['compartment']
                                },
                                each_doc,
                                upsert=True
                            )
            except Exception as error:
                print(error)

            # doc = db.JUP_DB_Landing_Page_.find_one({'user': user, 'dep_month': int(curr_month), 'dep_year': int(curr_year), 'compartment': comp_each})
            # network_user = db.JUP_DB_User.distinct('name', {'cluster': {'$eq': 'network'}, 'name' : {'$ne' : user}, 'active': True})
            # for each_network_user in network_user:
            #     del doc['_id']
            #     doc['user'] = each_network_user
            #     db.JUP_DB_Landing_Page_.insert(doc)
        except Exception as error:
            print(error)
        num = num + 1

if __name__ == '__main__':
    # db = client[JUPITER_DB]
    # user_list = list(db.JUP_DB_User.distinct('name', {'cluster' : {'$ne' : 'network'},'active':True}))
    # network_user = db.JUP_DB_User.find_one({'cluster' : {'$eq' : 'network'},'active':True})
    # user_list.append(network_user['name'])
    # comp = db.JUP_DB_Booking_Class.distinct("comp")
    # comp.append('TL')
    # # for each_user in user_list:
    #    for each_comp in comp:
    #        print(each_user+" "+each_comp)
    #        main(each_user, each_comp,client)
    main('Vishnu', 'TL', client)
    main('Vishnu', 'Y', client)
    main('Vishnu', 'J', client)

