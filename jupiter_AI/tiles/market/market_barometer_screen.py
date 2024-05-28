import datetime
import json
from collections import defaultdict
from copy import deepcopy

from jupiter_AI.network_level_params import Host_Airline_Code as Host
from jupiter_AI import client,JUPITER_DB, query_month_year_builder
db = client[JUPITER_DB]


def proximity_indicator_screen(afilter):
    afilter = deepcopy(defaultdict(list, afilter))
    query = dict()
    response = dict()
    if afilter['region']:
        query['region'] = {'$in': afilter['region']}
    if afilter['country']:
        query['country'] = {'$in': afilter['country']}
    if afilter['pos']:
        query['pos'] = {'$in': afilter['pos']}
    if afilter['compartment']:
        query['compartment'] = {'$in': afilter['compartment']}
    if afilter['origin'] and afilter['destination']:
        od = ''.join(afilter['origin'] + afilter['destination'])
        query['od'] = od
    from_obj = datetime.datetime.strptime(afilter['fromDate'], '%Y-%m-%d')
    to_obj = datetime.datetime.strptime(afilter['toDate'], '%Y-%m-%d')
    month_year = query_month_year_builder(stdm=from_obj.month,
                                          stdy=from_obj.year,
                                          endm=to_obj.month,
                                          endy=to_obj.year)
    query['$or'] = month_year

    apipeline_user = [
        {
            '$match': query
        },
        {
            '$group': {'_id': {'agent': '$agent','region': '$region','country':'$country',
                               'pos':'$pos','od':'$od','carrier':'$MarketingCarrier1'},
                       'pax': {'$sum': '$pax'},
                       'pax_host': {
                           "$sum": {"$cond": [
                               {'$eq': ['$MarketingCarrier1', Host]},
                               '$pax',
                               0
                           ]}
                       },
                       'rev': {'$sum': '$revenue'},
                       'rev_host': {
                           "$sum": {"$cond": [
                               {'$eq': ['$MarketingCarrier1', Host]},
                               '$revenue',
                               0
                           ]}
                       }
                       }
        },
        {
            '$project': {'_id': 0,
                         'Agent': '$_id.agent',
                         'Pax': '$pax',
                         'Pax_Host': '$pax_host',
                         'Revenue':'$rev',
                         'Rev_host':'$rev_host',
                         'pos_host':'$pos_host',
                         'Market_Share_host': {'$multiply':[{'$divide':['$pax_host','$pax']},100]}
                         }
        },
        {
            '$group':{'_id': 0,
                      'tot_pax': {'$sum':'$Pax'},
                      'tot_host_pax':{'$sum':'$Pax_Host'},
                      'Agent_details': {'$push': {'Agent_Name': "$Agent", 'Pax': "$Pax", 'Pax_host':'$Pax_Host',
                                                  'Agent_Share_Host':'$Market_Share_Host'}}
            }
        },
        {
            '$unwind':'$Agent_details'
        },
        {
            '$project':{ '_id': 0,
                         'Agent_Name':'$Agent_details.Agent_Name',
                         'Tot_Pax': '$Agent_details.Pax',
                         'Pax_host': '$Agent_details.Pax_host',
                         'tot_pax':'$tot_pax',
                         'tot_host_pax':'$tot_host_pax',
                         'PI': {'$multiply':['$Agent_details.Pax',
                                             {'$subtract':[{'$divide':['$Agent_details.Pax_host','$Agent_details.Pax']},
                                                           {'$divide': ['$tot_host_pax', '$tot_pax']}]}]},

                          }

        },
        {
            '$project':
                {
                    '_id': 0,
                    'Agent_name': '$Agent_Name',
                    'PI':'$PI',
                    # 'Tot_pax':'$Tot_Pax',
                    # 'Pax_host':'$Pax_host',
                    # 'entire_pax':'$tot_pax',
                    # 'entire_host_pax':'$tot_host_pax',
                    'Friend_or_Foe': {'$cond': {
                        'if': {'$gte': ['$PI',0]}, 'then': 'Friend', 'else': 'Foe'}}
                }
        }
    ]

    cursor_user = db.JUP_DB_Market_Share_Last.aggregate(apipeline_user, allowDiskUse=True)

    data_user = list(cursor_user)

    total = 0
    friends = 0
    foes = 0
    for doc in data_user:
        total += 1
        if doc['Friend_or_Foe'] == 'Friend':
            friends += 1
        elif doc['Friend_or_Foe'] == 'Foe':
            foes += 1
    # Friend = []
    # Foe = []
    # tot_pax = 0
    # tot_host_pax = 0
    # for i in data_user:
    #     tot_pax += i['Pax']
    #     tot_host_pax += i['Pax_Host']
    # Market_Share_Host = (float(tot_host_pax)/tot_pax) * 100
    # for i in data_user:
    #     Prox_Indic = (i['Market_Share_host'] - Market_Share_Host) * i['Pax']
        # An agent with a prox indic of 0 is my
        #  friend since he has market share equal
        #  to mine and i[pax] cannot be 0
    #     if Prox_Indic >= 0:
    #         Friend.append({'Agent':i['Agent'], 'Proximity_Indicator':Prox_Indic})
    #     else:
    #         Foe.append({'Agent':i['Agent'], 'Proximity_Indicator':Prox_Indic})
    #
    # print tot_host_pax
    # PI_tile = (((float(tot_host_pax)/tot_pax)*100) - Market_Share_Host) * tot_pax
    # print PI_tile, tot_pax, Market_Share_Host
    # print Friend
    # print Foe
    # print len(Friend), len(Foe), float(len(Friend))/(len(Foe) + len(Friend))
    response = {
        'total_agents': total,
        'friends': friends,
        'foes': foes
        }
    return json.dumps(response)

if __name__ == '__main__':
    import time
    st = time.time()
    a = {
        'region': [],
        'country': [],
        'pos': ["DXB"],
        'origin': [],
        'destination': [],
        'compartment': [],
        'fromDate': '2017-02-14',
        'toDate': '2017-02-20'
    }
    print proximity_indicator_screen(a)