"""
header!

"""
import datetime
import time
from collections import defaultdict
from copy import deepcopy
from jupiter_AI import client, JUPITER_DB
db = client[JUPITER_DB]
from jupiter_AI.network_level_params import Host_Airline_Code

from jupiter_AI.network_level_params import na_value
from jupiter_AI.network_level_params import query_month_year_builder

'''
Tiles in this screen
    revenue/vlyr host DONE
    pax/vlyr    host DONE
    revenue/vlyr user DONE
    ms/ms vlyr user DONE
'''


"""
Try to break up the working into smaller functions like for query_builder
Split file into DAL and BLL, all python processing to be done in BLL
Build and fire the queries in DAL
Leave sufficient lines when a piece of logic is completed
"""


def get_tiles(afilter):
    afilter = deepcopy(defaultdict(list, afilter)) # use proper variable names
    query = dict() # use proper variable names
    response = dict()
    revenue_host = na_value
    revenue_vlyr_user = na_value
    revenue_user = na_value
    revenue_vlyr_host = na_value
    pax_host = na_value
    pax_vlyr_host = na_value
    market_share = na_value
    market_share_vlyr = na_value
    if afilter['region']:
        query['region'] = {'$in': afilter['region']}
    if afilter['country']:
        query['country'] = {'$in': afilter['country']}
    if afilter['pos']:
        query['pos'] = {'$in': afilter['pos']}
    if afilter['compartment']:
        query['compartment'] = {'$in': afilter['compartment']}

    query3 = deepcopy(query) # use proper variable names

    if afilter['origin']:
        od_build = []
        for idx, item in enumerate(afilter['origin']): # use proper variable names for 'idx' and 'item'
            od_build.append({'origin': item, 'destination': afilter['destination'][idx]})
        query['$or'] = od_build

    query1 = deepcopy(query) # use proper variable names
    query1['dep_date'] = {'$gte':afilter['fromDate'],'$lte':afilter['toDate']}
    # print query1
    apipeline1 = [ # use proper variable names
        {
            '$match': query1
        },
        {
            '$group': {
                '_id': None,
                'revenue': {'$sum': '$revenue_base'},
                'revenue_ly': {'$sum': '$revenue_base_1'},
            }
        },
        {
            '$project': {
                '_id': 0,
                'revenue': '$revenue',
                'revenue_ly': '$revenue_ly'
            }
        }
    ]
    cursor1 = db.JUP_DB_Sales.aggregate(apipeline1) # use proper variable names
    data1 = [] # use proper variable names
    for i in cursor1: # use proper variable names
        data1.append(i)
    if len(data1) != 0:
        revenue_user = sum(i['revenue'] for i in data1) # i???
        # print revenue_user
        revenue_ly_user = sum(i['revenue_ly'] for i in data1)
        if revenue_ly_user != 0:
            revenue_vlyr_user = (revenue_user - revenue_ly_user)*100/revenue_ly_user
        else:
            revenue_vlyr_user = na_value
    #   revenue user
    #   revenue vlyr user

    apipeline2 = [ # use proper variable names
        # explain the pipeline in commments
        {
            '$match': {
                'dep_date': {'$gte': afilter['fromDate'],
                             '$lte': afilter['toDate']}
            }
        },
        {
            '$group': {
                '_id': None,
                'revenue': {'$sum': '$revenue_base'},
                'revenue_ly': {'$sum': '$revenue_base_1'},
                'pax': {'$sum': '$pax'},
                'pax_ly': {'$sum': '$pax_1'}
            }
        },
        {
            '$project': {
                '_id': 0,
                'revenue': '$revenue',
                'revenue_ly': '$revenue_ly',
                'pax': '$pax',
                'pax_ly': '$pax_ly'
            }
        }
    ]
    cursor2 = db.JUP_DB_Sales.aggregate(apipeline2) # use proper variable names
    data2 = [] # use proper variable names
    for i in cursor2: # use proper variable names
        data2.append(i)
    if len(data2) != 0:
        revenue_host = sum(i['revenue'] for i in data2)
        # print revenue_host
        revenue_ly_host = sum(i['revenue_ly'] for i in data2)
        pax_host = sum(i['pax'] for i in data2)
        # print pax_host
        pax_ly_host = sum(i['pax_ly'] for i in data2)
        if revenue_ly_host != 0:
            revenue_vlyr_host = (revenue_host - revenue_ly_host)*100/revenue_ly_host
        else:
            revenue_vlyr_host = na_value
        if pax_ly_host != 0:
            pax_vlyr_host = (pax_host - pax_ly_host)*100/pax_ly_host
        else:
            pax_vlyr_host = na_value

    # what's all this??
    stm = datetime.datetime.strptime(afilter['fromDate'], '%Y-%m-%d').month
    sty = datetime.datetime.strptime(afilter['fromDate'], '%Y-%m-%d').year
    tm = datetime.datetime.strptime(afilter['toDate'], '%Y-%m-%d').month
    ty = datetime.datetime.strptime(afilter['toDate'], '%Y-%m-%d').year
    query3['$or'] = query_month_year_builder(stdm=stm,
                                             stdy=sty,
                                             endm=tm,
                                             endy=ty)
    if afilter['origin']:
        od_build = []
        del query['$or']
        query3 = defaultdict(list)
        query3['$and'].append({'$or': query_month_year_builder(stdm=stm,
                                                               stdy=sty,
                                                               endm=tm,
                                                               endy=ty)})
        for idx, item in enumerate(afilter['origin']): # use proper variable names
            od_build.append({'od': item + afilter['destination'][idx]})
        query3['$and'].append({'$or': od_build})
    # print query3
    apipeline3 = [ # use proper variable names
        # explain the pipeline stages
        {
            '$match': query3
        },
        {
            '$group': {
                '_id': None,
                'pax_host': {
                    "$sum": {"$cond": [
                        {'$eq': ['$MarketingCarrier1', Host_Airline_Code]},
                        '$pax',
                        0
                    ]}},
                'total_pax': {
                    '$sum': '$pax'
                },
                'pax_host_ly': {
                    "$sum": {"$cond": [
                        {'$eq': ['$MarketingCarrier1', Host_Airline_Code]},
                        '$pax_1',
                        0
                ]}},
                'total_pax_ly': {
                    '$sum': '$pax_1'
                }
            }
        },
        {
            '$project': {
                '_id': 0,
                'pax_host': '$pax_host',
                'total_pax': '$total_pax',
                'pax_host_ly': '$pax_host_ly',
                'total_pax_ly': '$total_pax_ly'
        }
        }
    ]
    # print apipeline3
    cursor3 = db.JUP_DB_Market_Share.aggregate(apipeline3) # use proper variable names
    data3 = [] # use proper variable names
    for i in cursor3: # use proper variable names
        data3.append(i)

    if len(data3) == 1:
        if data3[0]['total_pax'] != 0:
            market_share = round(((float(data3[0]['pax_host']) / data3[0]['total_pax']) * 100),2)
        else:
            market_share = na_value
        if data3[0]['total_pax_ly'] != 0:
            market_share_ly = round(((float(data3[0]['pax_host_ly']) / data3[0]['total_pax_ly']) * 100),2)
        else:
            market_share_ly = na_value
        if market_share != na_value and market_share_ly != na_value and market_share_ly != 0:
            market_share_vlyr = round(((float(market_share - market_share_ly)/market_share_ly)*100),2)
        else:
            market_share_vlyr = na_value
    response['revenue_host'] = revenue_host
    response['revenue_user'] = revenue_user
    response['revenue_vlyr_host'] = revenue_vlyr_host
    response['revenue_vlyr_user'] = revenue_vlyr_user
    response['pax_vlyr_host'] = pax_vlyr_host
    response['pax_host'] = pax_host
    response['market_share_user'] = market_share
    response['market_share_vlyr_user'] = market_share_vlyr
    return response


if __name__ == '__main__':
    st = time.time()
    a = {
        'region': [],
        'country': [],
        'pos': ['DXB'],
        'origin': ['DXB'],
        'destination': ['DOH'],
        'compartment': ['Y'],
        'fromDate': '2017-02-14',
        'toDate': '2017-02-20',
        'lastfromDate': '2015-12-01',
        'lasttoDate': '2015-07-31'
    }
    print get_tiles(afilter=a)
