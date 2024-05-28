import pymongo
import json
import time
from collections import defaultdict
st = time.time()
# print st
from jupiter_AI import client, JUPITER_DB, Host_Airline_Hub, Host_Airline_Code, JUPITER_LOGGER
from jupiter_AI.logutils import measure
db = client[JUPITER_DB]


@measure(JUPITER_LOGGER)
def get_region_country_details():
    crsr = db.JUP_DB_Region_Master.aggregate(

        # Pipeline
        [
            # Grouping on the basis of City and getting Country and Region
            {
                '$group': {
                    '_id':'$POS_CD',
                    'country':{'$first':'$COUNTRY_CD'},
                    'region':{'$first':'$Region'},
                    'cluster':{'$first':'$Cluster'}
                }
            },

        ]
    )
    data = list(crsr)
    dict_data = dict()
    for doc in data:
        if doc['_id']:
            dict_data[doc['_id']] = dict(country=doc['country'],region=doc['region'],cluster=doc['cluster'])
    try:
        dict_data['DWC']
    except KeyError:
        dict_data['DWC'] = {'country':'AE','region':'MiddleEast','cluster':'MEASC'}
    return dict_data


@measure(JUPITER_LOGGER)
def get_derived_factor_details():
    db.JUP_DB_Capacity_Derived_Factor.aggregate(
        # Pipeline
        [
            # Removing Docs where origin and destination are not defined
            {
                '$match': {
                    'assigned_origin':{'$exists':True},
                    'assigned_destination':{'$exists':True}
                }
            },

            # Sorting on the basis of last update date
            {
                '$sort': {
                    'last_update_date':-1
                }
            },

            # Grouping on the basis of airline/origin/destination/quarter/year
            {
                '$group': {
                    '_id':{
                        'quarter':'$quarter',
                        'year':'$year',
                        'origin':'$assigned_origin',
                        'destination':'$assigned_destination',
                        'carrier':'$airline'
                    },
                    'priority_level':{'$first':'$priority_level'},
                    'user_override_flag':{'$first':'$user_override_flag'},
                    'user_override':{'$first':'$user_override'},
                    'derived_factor':{'$first':'$derived_factor'}
                    # 'details':{
                    # 	'$push':{
                    # 		//'priority_level':'$priority_level',
                    # 		//'user_override_flag':'$user_override_flag',
                    # 		//'user_override':'$user_override',
                    # 		//'derived_factor':'$derived_factor'
                    # 		'doc':'$$ROOT'
                    # 	}
                    # }
                }
            },
            {
                '$project': {
                    'origin':'$_id.origin',
                    'destination':'$_id.destination',
                    'carrier':'$_id.carrier',
                    'quarter':'$_id.quarter',
                    'year':'$_id.year',
                    'priority_level':'$priority_level',
                    'user_override_flag':'$user_override_flag',
                    'user_override':'$user_override',
                    'derived_factor':'$derived_factor'
                }
            },

            # Outing this into a temperory collection
            {
                '$out': "tempcol"
            },
        ],
        allowDiskUse=True
    )
    crsr = list(db['tempcol'].find())
    print 'Derived Factor Docs Count', len(crsr)
    data = defaultdict(dict)
    db['tempcol'].drop()
    for doc in crsr:
        data[(doc['carrier'], doc['origin'], doc['destination'], doc['quarter'], doc['year'])] = dict(
            priority=doc['priority_level'],
            user_override_flag = doc['user_override_flag'],
            user_override = doc['user_override'],
            derived_factor = doc['derived_factor']
        )
    # print len(data.keys())
    # print data['FZ','DXB','DOH','Q3',2017]
    # print data['FZ','DOH','DXB','Q3',2017]
    return data


@measure(JUPITER_LOGGER)
def get_derived_factor_val(carrier, origin, destination, quarter, year, derived_factor_data, region_country_data):
    """
    """
    list_derived_factor_details = []
    df = 1
    o = origin
    try:
        o_c = region_country_data[origin]['country']
        o_r = region_country_data[origin]['region']
        origin_list = [o,o_c,o_r,'Network']

    except KeyError:
        origin_list = [origin,'Network']
    # print origin_list
    d = destination
    try:
        d_c = region_country_data[destination]['country']
        d_r = region_country_data[destination]['region']
        destination_list = [d,d_c,d_r,'Network']
    except KeyError:
        destination_list = [destination, 'Network']
    # print destination_list
    for o_val in origin_list:
        for d_val in destination_list:
            # print 'O VAL', o_val
            # print 'D VAL', d_val
            val = derived_factor_data[(carrier, o_val, d_val, quarter, year)]
            # print 'VAL', val
            if val:
                list_derived_factor_details.append(val)
    # print 'LIST', list_derived_factor_details
    if list_derived_factor_details:
        list_derived_factor_details_sorted = sorted(list_derived_factor_details, key=lambda x: x['priority'])
        # print list_derived_factor_details
        for index, doc in enumerate(list_derived_factor_details_sorted):
            derived_factor_doc_selected = list_derived_factor_details_sorted[index]
            # print 'Derived Factor Doc', derived_factor_doc_selected
            if derived_factor_doc_selected['user_override_flag'] == 1:
                if derived_factor_doc_selected['user_override'] > 0:
                    return derived_factor_doc_selected['user_override']
                else:
                    if derived_factor_doc_selected['derived_factor'] > 0:
                        return derived_factor_doc_selected['derived_factor']
            else:
                if derived_factor_doc_selected['derived_factor'] > 0:
                    return derived_factor_doc_selected['derived_factor']
        return 1
    else:
        return 1

if __name__=='__main__':
    df_data = get_derived_factor_details()
    region_country_data = get_region_country_details()
    df1 = get_derived_factor_val( Host_Airline_Code, Host_Airline_Hub, 'DOH', 'Q3', 2017, df_data, region_country_data)
    df2 = get_derived_factor_val('EK', 'DXB', 'DOH', 'Q3', 2017, df_data, region_country_data)
    df3 = get_derived_factor_val('QR', 'DXB', 'DOH', 'Q3', 2017, df_data, region_country_data)
    df4 = get_derived_factor_val('G9', 'DMM', 'HYD', 'Q3', 2017, df_data, region_country_data)
    print df1, df2, df3