"""
Author - Sai
Date - 20.6.2017

For O/D/Comp/DepStart/DepEnd 
    get most frequently available fares in the market.
        get the host most frequently available fare from Infare.
        get the competitor most frequently available fare from Infare.
    get the web fares for host from ATPCO.
        map host web fares from atpco to the host most frequently available fare from infare.
        lets call this atpco fare as _id_
    assign all the competitors most frequently available fare to competitor farebasis column in _id_
"""
import json
from collections import defaultdict

import time

from jupiter_AI import client, JUPITER_DB, SYSTEM_DATE, Host_Airline_Code, JUPITER_LOGGER
from jupiter_AI.logutils import measure
from jupiter_AI.triggers.mrkt_params_workflow_opt import get_most_avail_dict
from bson.objectid import ObjectId
from jupiter_AI.batch.fbmapping_batch.fbmapping_optimizer import optimizefareladder
db = client[JUPITER_DB]


@measure(JUPITER_LOGGER)
def map_fare_infare(origin, destination, compartment, dep_date_start, dep_date_end, date_list=None):
    st = time.time()
    """
    :param origin: 
    :param destination: 
    :param compartment: 
    :param dep_date_start: 
    :param dep_date_end: 
    :return: 
    """
    pos = None
    avail_fare_dF_RT = get_most_avail_dict(pos, origin, destination, compartment, dep_date_start, dep_date_end)
    st2 = time.time()
    print "time taken to get avail_fare_df_return: ", st2 - st
    avail_fare_dF_OW = get_most_avail_dict(pos, origin, destination, compartment, dep_date_start, dep_date_end, oneway_return=1)
    print "time taken to get avail_fare_df_one_way: ", time.time() - st2
    # print 'Avail Fare RT', avail_fare_dF_RT
    # print 'Avail Fare OW', avail_fare_dF_OW

    host_fare_RT = avail_fare_dF_RT[avail_fare_dF_RT['carrier'] == Host_Airline_Code]
    # print host_fare_RT
    comp_fares_RT = avail_fare_dF_RT[avail_fare_dF_RT['carrier'] != Host_Airline_Code]
    host_fare_OW = avail_fare_dF_OW[avail_fare_dF_OW['carrier'] == Host_Airline_Code]
    #print host_fare_OW
    comp_fares_OW = avail_fare_dF_OW[avail_fare_dF_OW['carrier'] != Host_Airline_Code]

    response_RT = list()
    response_OW = list()

    for idx, row in comp_fares_RT.iterrows():
        response_RT.append({
            'carrier': row['carrier'],
            'fare_basis': "",
            'fare': row['most_avail_fare_base_rt'],
            'frequency': row['most_avail_fare_freq_rt']
        })

    for idx, row in comp_fares_OW.iterrows():
        response_OW.append({
            'carrier': row['carrier'],
            'fare_basis': "",
            'fare': row['most_avail_fare_base_ow'],
            'frequency': row['most_avail_fare_freq_ow']
        })

    data = host_fare_records(origin, destination, compartment, dep_date_start, dep_date_end)
    # print json.dumps(data, indent=1)
    if len(host_fare_RT) > 0:
        host_fare_infare = float(host_fare_RT['most_avail_fare_base'].iloc[0])
        # print 'Infare Fare', host_fare_infare

        host_fl_infare = []
        host_fl_infare.append(host_fare_infare)

        if len(data['rt']) > 0:
            host_rt_data = data['rt']
            host_fl_atpco = [doc['fare'] for doc in host_rt_data]
            host_fbs = [doc['fare_basis'] for doc in host_rt_data]
            host_ids = [doc['unique_id'] for doc in host_rt_data]
            # print host_fl_infare
            # print host_fl_atpco
            st3 = time.time()
            out = optimizefareladder('raw', host_fl_infare, host_fl_atpco)
            print "time taken to optimizefareladder return = ", time.time() - st3
            # print 'OUT***********', out
            host_id_to_be_updated = str(host_ids[out[0][0]])
            # print 'ID Updated', host_id_to_be_updated
            if response_RT:
                # print host_id_to_be_updated
                # print response_RT
                db.JUP_DB_ATPCO_Fares_Rules.update(
                    {
                        '_id':ObjectId(host_id_to_be_updated)
                    }
                    ,
                    {
                        '$set':{'competitor_farebasis': response_RT}
                    }
                )
                print 'Updated', host_id_to_be_updated

    if len(host_fare_OW) > 0:
        host_fare_infare = host_fare_OW['most_avail_fare_base'].iloc[0]
        host_fl_infare = []
        host_fl_infare.append(host_fare_infare)
        if len(data['ow']) > 0:
            host_rt_data = data['ow']
            host_fl_atpco = [doc['fare'] for doc in host_rt_data]
            host_fbs = [doc['fare_basis'] for doc in host_rt_data]
            host_ids = [doc['unique_id'] for doc in host_rt_data]
            # print host_fl_atpco
            # print host_fl_infare
            st4 = time.time()
            out = optimizefareladder('raw', host_fl_atpco, host_fl_infare)
            print "time taken to optimizefareladder one_way = ", time.time() - st4

            host_id_to_be_updated = str(host_ids[out[0][0]])

            if response_OW:
                # print host_id_to_be_updated,
                # print response_OW
                db.JUP_DB_ATPCO_Fares_Rules.update(
                    {
                        '_id': ObjectId(host_id_to_be_updated)
                    }
                    ,
                    {
                        '$set':{'competitor_farebasis': response_OW}
                    }
                )
                print 'Updated', host_id_to_be_updated


@measure(JUPITER_LOGGER)
def host_fare_records(origin, destination, compartment, dep_date_start, dep_date_end):
    st = time.time()
    """
    :param self: 
    :return: 
    """
    SYSTEM_DATE_MOD =  "0" + SYSTEM_DATE[2:4] + SYSTEM_DATE[5:7] + SYSTEM_DATE[8:10]
    # SYSTEM_DATE format is modified - '0yymmdd'. Because, in ATPCO_Fares_Rules this is the format of date for effective_date_from and effective_date_to
    query = defaultdict(list)
    qry_fares = defaultdict(list)
    query['$and'].append({'origin': origin})
    query['$and'].append({'carrier': Host_Airline_Code})
    query['$and'].append({'destination': destination})
    query['$and'].append({'compartment': compartment})
    query['$and'].append({'fare_include': True})
    query['$and'].append({'channel': 'web'})
    qry_fares['$or'].append({
        '$and': [
            {'dep_date_from': {'$lte': dep_date_end}},
            {'dep_date_end': {'$gte': dep_date_start}}
        ]
    })
    qry_fares['$or'].append({
        '$and': [
            {'dep_date_from': {'$lte': dep_date_end}},
            {'dep_date_end': None}
        ]
    })
    qry_fares['$or'].append({
        '$and': [
            {'dep_date_from': None},
            {'dep_date_end': {'$gte': dep_date_start}}
        ]
    })
    qry_fares['$or'].append({
        '$and': [
            {'dep_date_from': None},
            {'dep_date_end': None}
        ]
    })
    query['$and'].append(dict(qry_fares))
    query['$and'].append(
        {
            '$or':
                [
                    {'effective_from': None},
                    {'effective_from': {'$lte': SYSTEM_DATE_MOD}}
                ]
        }

    )
    query['$and'].append(
        {
            '$or':
                [
                    {'effective_to': None},
                    {'effective_to': {'$gte': SYSTEM_DATE_MOD}}
                ]
        }
    )
    # print dict(query)

    fares_ppln = [
        # Matching Code/ For entire ladder batch will be null query
        {
            '$match': dict(query)
        }
        ,
        # Assuming both 1,3 values for oneway_return and else as  return
        {
            '$addFields': {
                "ow_rt": {
                    '$cond': {
                        'if': {'$in': ['$oneway_return', [1, 3]]},
                        'then': 1,
                        'else': {
                            '$cond': {
                                'if': {'eq': ['$oneway_return', 2]},
                                'then': 2,
                                'else': None
                            }
                        }
                    }
                }
            }
        },

        # Grouping the docs on the basis of carrier/OD/compartment/oneway_return/RBD_type and storing al the fares
        {
            '$group': {
                '_id': {
                    'OD': '$OD',
                    'compartment': '$compartment',
                    'oneway_return': '$ow_rt'
                },
                'fares': {
                    '$push': {
                        'unique_id': '$_id',
                        'fare': '$fare',
                        'fare_basis': '$fare_basis',
                        'currency': '$currency',
                        'YQ':'$YQ',
                        'YR':'$YR',
                        'surcharge':'$surcharge_average'
                    }
                }
            }
        },

        # # Projecting Fields
        # {
        #     '$project':{
        #         '_id': 1,
        #         'fares': '$fares'
        #     }
        # }
        # ,
        # Outing the result in a temperory collection
        {
            '$out': 'fareladder_matching_temp_col_web'
        }
    ]
    response = dict(
        ow=[],
        rt=[]
    )
    db.JUP_DB_ATPCO_Fares_Rules.aggregate(fares_ppln,allowDiskUse=True)
    crsr = db.fareladder_matching_temp_col_web.find()
    # print 'ATPCO Docs', crsr.count()
    if crsr.count() > 0:
        for doc in crsr:
            if doc['_id']['oneway_return'] == 1:
                for val in doc['fares']:
                    val['unique_id'] = str(val['unique_id'])
                    total_fare = val['fare']
                    if type(val['YQ']) in [int,float]:
                        total_fare += val['YQ']
                    elif type(val['YR'] in [int,float]):
                        total_fare += val['YR']
                    elif type(val['surcharge_average']):
                        total_fare += val['surcharge_average']
                    val.update({'total_fare': total_fare})
                    response['ow'].append(val)
            elif doc['_id']['oneway_return'] == 2:
                for val in doc['fares']:
                    val['unique_id'] = str(val['unique_id'])
                    total_fare = val['fare']
                    if type(val['YQ']) in [int,float]:
                        total_fare += val['YQ']
                    elif type(val['YR'] in [int,float]):
                        total_fare += val['YR']
                    elif type(val['surcharge_average']):
                        total_fare += val['surcharge_average']
                    val.update({'total_fare': total_fare})
                    response['rt'].append(val)

    db.fareladder_matching_temp_col_web.drop()
    print "time taken to get host_fare_records = ", time.time() - st
    return response


if __name__ == '__main__':
    pos_od_combinations = ['AANDXBUETY', 'AANDXBUETJ', 'AANDXBAHBY', 'AANDXBAHBJ', 'AANCMBDXBY', 'AANCMBDXBJ',
                           'AANADDDXBY', 'AANADDDXBJ', 'AANDXBDOHY', 'AANDXBDOHJ', 'CGPCGPMCTY']

    for market in pos_od_combinations:
        # print market[3:6], market[6:9], market[-1]
        map_fare_infare(market[3:6],market[6:9],market[-1],'2017-04-01','2017-07-26')
    # data = list(db.JUP_DB_ATPCO_Fares_Rules.aggregate(
    #     [
    #         {
    #             '$match':
    #                 {
    #                     'carrier': 'FZ',
    #                     'fare_include': True,
    #                     'channel': 'web',
    #                     'compartment': {'$in': ['Y', 'J']}
    #                 }
    #         }
    #         ,
    #         {
    #             '$group':
    #                 {
    #                     '_id':
    #                         {
    #                             'origin': '$origin',
    #                             'destination': '$destination',
    #                             'compartment': '$compartment'
    #                         }
    #                 }
    #         }
    #     ]
    # ))
    #
    # print 'Combinations', len(data)
    # st = time.time()
    # for index, doc in enumerate(data):
    #     map_fare_infare(origin=doc['_id']['origin'],
    #                     destination=doc['_id']['destination'],
    #                     compartment=doc['_id']['compartment'],
    #                     dep_date_start=None,
    #                     dep_date_end=None
    #                     )
    # print time.time() - st
