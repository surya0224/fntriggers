"""
Author : Murugan
Functionality of code:
        Calculates fare recommendation, detects inverted fares, calculates rating for recommended fare
        based on Competitor_Fares_thresholds defined in collection - Comp_Star_Rating_Config,
        calculates Yield and Pax for every farebasis in fareladder

"""
from __future__ import division
import datetime
import math
import time
import requests
from copy import deepcopy
import pymongo
import json
import collections
import pandas as pd
# from jupiter_AI.triggers.workflow_mrkt_level_update import hit_java_url, build_market_data_from_java_response, main_func
from jupiter_AI import client, JUPITER_DB, JUPITER_LOGGER, ATPCO_DB, Host_Airline_Hub, Host_Airline_Code, mongo_client
from jupiter_AI.network_level_params import SYSTEM_DATE, today, Host_Airline_Code
from jupiter_AI.triggers.common import cursor_to_df,pos_level_currency_data as get_currency_data
from jupiter_AI.triggers.host_params_workflow_opt import get_od_distance
from jupiter_AI.triggers.recommendation_models.oligopoly_fl_ge import get_pax_yield
from jupiter_AI.logutils import measure
import numpy as np
from pandas.io.json import json_normalize
from dateutil.relativedelta import relativedelta
from jupiter_AI import JAVA_URL, client, SYSTEM_DATE, JUPITER_DB, JUPITER_LOGGER, ATPCO_DB, Host_Airline_Hub, Host_Airline_Code, mongo_client
from jupiter_AI.triggers.recommendation_models.oligopoly_fl_ge import get_host_fares_df
url = JAVA_URL
SLEEP_TIME = 0.05
import pandas as pd
import numpy as np
list_fare_brand_overall_y = ["Lite", "Value", "FLY+Visa", "Flex", "GDS"]
list_fare_brand_overall_j = ["Business"]
# list_fare_brand_overall = []

list_channel_overall = ["Web", "TA", "GDS"]
channel_fb_y = ["TA Lite", "Web Value", "TA Value", "GDS Value", "TA FLY+Visa", "Web Flex", "TA Flex", "GDS GDS Flex"]
channel_fb_j = ["GDS Business"]
# channel_fb = []
HOST_AIRLINE_CODE = Host_Airline_Code
EXCHANGE_RATE = {}
currency_crsr = list(client[JUPITER_DB].JUP_DB_Exchange_Rate.find({}))
for curr in currency_crsr:
    EXCHANGE_RATE[curr['code']] = curr['Reference_Rate']

@measure(JUPITER_LOGGER)


@measure(JUPITER_LOGGER)
def convert_tax_currency(tax_value, host_curr, tax_currency):
    if tax_currency == -999:
        return 0
    else:
        tax_currency = tax_currency.upper()
        host_curr = host_curr.upper()
        tax_value = tax_value / EXCHANGE_RATE[host_curr] * EXCHANGE_RATE[tax_currency]
        return tax_value

@measure(JUPITER_LOGGER)
def get_pricing_models(pos, origin, destination, compartment, db):
    pos_cursor = db.JUP_DB_Region_Master.aggregate([
        {
            '$match':
                {
                    'POS_CD': pos
                }
        },
        {'$project':
            {
                '_id': 0,
                'city': '$POS_CD',
                'country': '$COUNTRY_CD',
                'region': '$Region',
                'cluster': '$Cluster',
                'network': '$Network'
            }
        }
    ])
    # print list(pos_cursor)

    pos_list = (list(pos_cursor)[0]).values()

    origin_cursor = db.JUP_DB_Region_Master.aggregate([
        {
            '$match':
                {
                    'Airport_Code': origin
                }
        },
        {'$project':
            {
                '_id': 0,
                'city': '$Airport_Code',
                'country': '$COUNTRY_CD',
                'region': '$Region',
                'cluster': '$Cluster',
                'network': '$Network'
            }
        }
    ])
    origin_list = (list(origin_cursor)[0]).values()
    destination_cursor = db.JUP_DB_Region_Master.aggregate([
        {
            '$match':
                {
                    'Airport_Code': destination
                }
        },
        {'$project':
            {
                '_id': 0,
                'city': '$Airport_Code',
                'country': '$COUNTRY_CD',
                'region': '$Region',
                'cluster': '$Cluster',
                'network': '$Network'
            }
        }
    ])
    destination_list = (list(destination_cursor)[0]).values()
    compartment_list = [compartment, 'all']
    # print compartment_list
    # print destination_list, compartment_list
    from collections import OrderedDict
    # pipe =
    dic = OrderedDict()
    dic['level_priority'] = 1
    dic['m_priority'] = -1
    pipe = [
        {
            '$match':
                {
                    'pos.value': {'$in': pos_list},
                    'origin.value': {'$in': origin_list},
                    'destination.value': {'$in': destination_list},
                    'compartment.value': {'$in': compartment_list}
                }
        },
        {"$addFields": {
            "level_priority": {
                "$switch": {
                    "branches": [
                        {"case": {"$eq": ["$pos.level", "Network"]}, "then": 5},
                        {"case": {"$eq": ["$pos.level", "city"]}, "then": 1},
                        {"case": {"$eq": ["$pos.level", "country"]}, "then": 2},
                        {"case": {"$eq": ["$pos.level", "cluster"]}, "then": 3},
                        {"case": {"$eq": ["$pos.level", "region"]}, "then": 4},
                    ],
                    "default": 5
                }
            }
        }},
        {"$unwind" : "$model"},
        {
            '$match':
                {
                    'model.eff_date_from': {'$lte': SYSTEM_DATE},
                    'model.eff_date_to': {'$gte': SYSTEM_DATE}
                }
        },
        {
            "$addFields":
                {
                    "m_priority": '$model.priority'
                }
        },
        # {'$sort': {'level_priority': 1, 'model.priority': -1}},
        # {'$sort': {'level_priority': pymongo.ASCENDING, 'm_priority': pymongo.DESCENDING }},
        {'$sort': dic},
        {
            "$addFields" :
                {
                    "model" : ["$model"]
                }
        },
        {
            '$limit': 1
        },
        {
            "$project":
                {
                    "_id": 0,
                    "model": 1,
                    "FCR" :1,
                    "Add-On" : 1
                }
        }
    ]
    print "pricing model query"
    print pipe
    pricing_models_cursor = db.JUP_DB_Pricing_Model_Markets.aggregate(pipe)
    # print list(pricing_models_cursor)
    pricing_models = list(pricing_models_cursor)#[0]['model']
    print pricing_models
    return pricing_models

def fare_brand_creation(add_on, mpf_recomended_fare_rt, channel, fare_brand, delta_currency, compartment, channel_fb, sellup):
    ## Check the sign of each farebrand formula
    # print("Fare brand starting computation")
    # print "channel ", channel
    # print "fare_brand ", fare_brand

    if channel.lower() == "all" and fare_brand.lower() == "all":
        ## All & All mean lowest of farebrand
        flag = "straight"
        if compartment == "Y":
            channel = "Web"
            fare_brand = "Lite"
        else:
            channel = "Web"
            fare_brand = "Business"
        fare_brand_value, sellup_builder = fare_brand_formula_sign(add_on, mpf_recomended_fare_rt, channel, fare_brand, delta_currency, flag, channel_fb, sellup)

        # print("both all")
    elif channel.lower() == "all" and fare_brand.lower() != "all" and len(fare_brand)>0:
        print(("channel all", fare_brand))
        flag = "straight"
        if compartment == "Y":
            channel = "Web"
            fare_brand = "Lite"
        else:
            channel = "Web"
            fare_brand = "Business"
        fare_brand_value, sellup_builder = fare_brand_formula_sign(add_on, mpf_recomended_fare_rt, channel, fare_brand, delta_currency, flag, channel_fb, sellup)
    elif fare_brand.lower() == "all" and channel.lower() != "all" and len(channel)>0:
        flag = "straight"
        print(("fare_brand all", channel))
        if compartment == "Y":
            channel = "Web"
            fare_brand = "Lite"
        else:
            channel = "Web"
            fare_brand = "Business"
        fare_brand_value, sellup_builder = fare_brand_formula_sign(add_on, mpf_recomended_fare_rt, channel, fare_brand, delta_currency, flag, channel_fb, sellup)
    else:
        flag = "straight"
        # if compartment == "Y":
        #     channel = "Web"
        #     fare_brand = "Value"
        # else:
        #     channel = "Web"
        #     fare_brand = "Business"
        fare_brand_value, sellup_builder = fare_brand_formula_sign(add_on, mpf_recomended_fare_rt, channel, fare_brand, delta_currency, flag, channel_fb, sellup)
        # print((fare_brand, channel))
    # print asd
    return fare_brand_value, sellup_builder

def hit_java_url(response):
    headers = {
        "Connection": "keep-alive",
        "Content-Length": "257",
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.52 Safari/536.5",
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Accept-Encoding": "gzip,deflate",
        "Accept-Language": "q=0.8,en-US",
        "Accept-Charset": "utf-8"}

    flag_rolling = False
    if response['dates_code']:
        if "RL" in response['dates_code'][0]:
            flag_rolling = True
            parameters = {
                "triggerFlag": "true",
                "fromDate": response['dep_date_start'],
                "toDate": response['dep_date_end'],
                "posMap": {

                    "cityArray": [response['pos']]
                },
                "originMap": {
                    "cityArray": [response['origin']]
                },
                "destMap": {
                    "cityArray": [response['destination']]
                },
                "compMap": {
                    "compartmentArray": [response['compartment']]
                }
            }

    if (response['dates_code']) and (not flag_rolling):
        parameters = {
            "triggerFlag": "true",
            "combineColumn": response['dates_code'],
            "posMap": {

                "cityArray": [response['pos']]
            },
            "originMap": {
                "cityArray": [response['origin']]
            },
            "destMap": {
                "cityArray": [response['destination']]
            },
            "compMap": {
                "compartmentArray": [response['compartment']]
            }
        }

    else:
        parameters = {
            "triggerFlag": "true",
            "fromDate": response['dep_date_start'],
            "toDate": response['dep_date_end'],
            "posMap": {

                "cityArray": [response['pos']]
            },
            "originMap": {
                "cityArray": [response['origin']]
            },
            "destMap": {
                "cityArray": [response['destination']]
            },
            "compMap": {
                "compartmentArray": [response['compartment']]
            }
        }
    print "sending request..."
    time.sleep(SLEEP_TIME)
    try:
        response_mt = requests.post(url, data=json.dumps(parameters), headers=headers, timeout=100.0, verify=False)
    except Exception as error:
        try:
            response_mt = requests.post(url, data=json.dumps(parameters), headers=headers, timeout=150.0, verify=False)
        except Exception as error:
            pass

    print "got response!! ", response_mt.status_code

    # if response_mt.text:
    #  response_mt.text['ManualTriggerGrid'] looks like :
    # [{performance params required by python}, {Totals required by UI}]
    #  If response_mt.status_code == 200:
    #       Means -- successfully received data from JAVA URL
    #   If len('ManualTriggerGrid') == 0:
    #       Means There is no data for this particular market in Manual_Triggers_Module Collection
    if response_mt.status_code == 200 and len(json.loads(response_mt.text)['ManualTriggerGrid']) > 0:
        # print "in if statement"
        response_json = json.loads(
            response_mt.text)['ManualTriggerGrid'][0]
        # print "after if"
        response['pax_data'] = dict(
            pax=response_json['pax'],
            vlyr=response_json['paxvlyrperc'],
            vtgt=response_json['paxvtgtperc']
        )
        response['revenue_data'] = dict(
            revenue=response_json['revenue'],
            vlyr=response_json['revenuevlyrperc'],
            vtgt=response_json['revenuevtgtperc']
        )
        response['avg_fare_data'] = dict(
            avg_fare=response_json['avgfare'],
            vlyr=response_json['avgfarevlyr'],
            vtgt=response_json['avgfarevtgt']
        )
        response['yield_data_compartment'] = dict(
            yield_=response_json['yield'],
            vlyr=response_json['yieldvlyrperc'],
            vtgt=response_json['yieldvtgtperc']
        )
        response['seat_factor'] = dict(
            leg1=response_json['bookedsf1'],
            leg2=response_json['bookedsf2'],
            leg1_vlyr=response_json['sf1vlyr'],  # check
            leg2_vlyr=response_json['sf2vlyr']  # check
        )

        return response, response_json

    else:
        print 'Skipped . . .'
        return response, None

@measure(JUPITER_LOGGER)
def get_month_year_query(dep_date_start, dep_date_end):
    start_obj = datetime.datetime.strptime(dep_date_start, "%Y-%m-%d")
    end_obj = datetime.datetime.strptime(dep_date_end, "%Y-%m-%d")
    query = []
    while start_obj <= end_obj:
        print start_obj
        query.append({
            "month": start_obj.month,
            "year": start_obj.year
        })
        # query.append({
        #     "month": start_obj.month,
        #     "year": start_obj.year - 1
        # })
        start_obj = start_obj + relativedelta(months=1)
    return query

def build_market_data_from_java_response(
        json_response, dep_date_start, dep_date_end, db, dates_code= None):
    comp_data = []
    host_data = {}
    pos = json_response['pos']
    origin = json_response['origin']
    destination = json_response['destination']
    compartment = json_response['compartment']
    month_year_query = get_month_year_query(dep_date_start, dep_date_end)
    # top5_comp = obtain_top_5_comp([pos], [origin], [destination], [compartment], dep_date_start, dep_date_end,
    #                               date_list=[{"start": dep_date_start,
    #                                           "end": dep_date_end}])
    # comp = top5_comp
    # print "comp:   ", comp
    comp = []
    temp_ = [Host_Airline_Code]
    for i in range(1, 5):
        temp_.append(json_response['comp' + str(i) + 'carrier'])
    ratings_crsr = list(db.JUP_DB_Rating_ALL.find({"Airline": {"$in": temp_}}))
    # pax_crsr = list(db.JUP_DB_Pos_OD_Compartment_new.aggregate([
    #     {
    #         "$match": {"pos": pos,
    #                    "od": origin + destination,
    #                    "compartment": compartment,
    #                    "$or": month_year_query
    #                    }
    #     }, {
    #         "$unwind": {
    #             "path": "$top_5_comp_data"
    #         }
    #     }, {
    #         "$match": {
    #             "top_5_comp_data.airline": {
    #                 "$in": temp_
    #             }
    #         }
    #     }, {
    #         "$project": {
    #             "_id": 0,
    #             "top_5_comp_data": 1,
    #             "month": 1,
    #             "year": 1
    #         }
    #     }
    # ]))
    # # cap_crsr = list(db.JUP_DB_OD_Capacity.aggregate(
    # #     [{"$match": {"od": origin + destination, "compartment": compartment, "$or": month_year_query, "airline": {"$in": temp_}}},
    # #      {"$project": {"_id": 0, "airline": 1, "od_capacity": 1, "month": 1, "year": 1}}]))
    # pax_dict_ty = {}
    # pax_dict_ly = {}
    # cap_ty = {}
    # cap_ly = {}
    # market_size_ty = {}
    # market_size_ly = {}
    #
    # for comp_ in temp_:
    #     pax_dict_ly[comp_] = 0
    #     pax_dict_ty[comp_] = 0
    #     cap_ty[comp_] = 0
    #     cap_ly[comp_] = 0
    #     market_size_ty[comp_] = 0
    #     market_size_ly[comp_] = 0
    #
    # for record in pax_crsr:
    #     for comp_ in temp_:
    #         if (comp_ == record['top_5_comp_data']['airline']) and (comp_ != Host_Airline_Code):
    #             if str(record['top_5_comp_data']['pax']) != 'nan':
    #                 pax_dict_ty[comp_] = pax_dict_ty[comp_] + record['top_5_comp_data']['pax']
    #             else:
    #                 pax_dict_ty[comp_] = pax_dict_ty[comp_] + 0
    #             if str(record['top_5_comp_data']['pax_1']) != 'nan':
    #                 pax_dict_ly[comp_] = pax_dict_ly[comp_] + record['top_5_comp_data']['pax_1']
    #             else:
    #                 pax_dict_ly[comp_] = pax_dict_ly[comp_] + 0
    #             if str(record['top_5_comp_data']['market_size']) != 'nan':
    #                 market_size_ty[comp_] = market_size_ty[comp_] + record['top_5_comp_data']['market_size']
    #             else:
    #                 market_size_ty[comp_] = market_size_ty[comp_] + 0
    #             if str(record['top_5_comp_data']['market_size_1']) != 'nan':
    #                 market_size_ly[comp_] = market_size_ly[comp_] + record['top_5_comp_data']['market_size_1']
    #             else:
    #                 market_size_ly[comp_] = market_size_ly[comp_] + 0
    #             if str(record['top_5_comp_data']['capacity']) != 'nan':
    #                 cap_ty[comp_] = cap_ty[comp_] + record['top_5_comp_data']['capacity']
    #             else:
    #                 cap_ty[comp_] = cap_ty[comp_] + 0
    #             if str(record['top_5_comp_data']['capacity_1']) != 'nan':
    #                 cap_ly[comp_] = cap_ly[comp_] + record['top_5_comp_data']['capacity_1']
    #             else:
    #                 cap_ly[comp_] = cap_ly[comp_] + 0

    # for record in cap_crsr:
    #     if record['airline'] != "FZ":
    #         if record['year'] == int(SYSTEM_DATE[0:4]):
    #             cap_ty[record['airline']] = cap_ty[record['airline']] + record['od_capacity']
    #         else:
    #             cap_ly[record['airline']] = cap_ly[record['airline']] + record['od_capacity']

    ratings_df = pd.DataFrame(ratings_crsr)
    comp.append((temp_, []))
    host_data['market_share_vlyr'] = json_response['marketsharevlyr']
    host_data['market_share'] = json_response['marketshare']
    host_data['rating'] = ratings_df.loc[(
                                                 ratings_df['Airline'] == Host_Airline_Code), 'overall_rating'].values[0]
    host_data['pax'] = json_response['pax']  # check
    host_data['distributor_rating'] = ratings_df.loc[(
                                                             ratings_df[
                                                                 'Airline'] == Host_Airline_Code), 'distributor_rating'].values[0]
    host_data['fms'] = json_response['fairmarketshare']
    host_data['market_rating'] = ratings_df.loc[(
                                                        ratings_df['Airline'] == Host_Airline_Code), 'market_rating'].values[0]
    host_data['capacity_rating'] = ratings_df.loc[(
                                                          ratings_df['Airline'] == Host_Airline_Code), 'capacity_rating'].values[0]
    host_data['market_share_vtgt'] = json_response['marketsharevtgt']
    host_data['product_rating'] = ratings_df.loc[(
                                                         ratings_df['Airline'] == Host_Airline_Code), 'product_rating'].values[0]
    host_data['pax_vlyr'] = json_response['paxvlyrperc']
    host_data['fare_rating'] = ratings_df.loc[(
                                                      ratings_df['Airline'] == Host_Airline_Code), 'fare_rating'].values[0]

    # lowest_filed_fare = get_lowest_filed_fare_dF(pos,
    #                                              origin,
    #                                              destination,
    #                                              compartment,
    #                                              dep_date_start,
    #                                              dep_date_end,
    #                                              comp,
    #                                              [{"start": dep_date_start,
    #                                                "end": dep_date_end}])
    # print lowest_filed_fare.head()
    #print json_response['mpfList']
    #print dates_code
    if dates_code:
        dates_code_temp = dates_code[0]
    else:
        dates_code_temp = ""

    try:
        if (dates_code is not None) and ("RL" not in dates_code_temp):

            most_avail_df = pd.DataFrame(
                columns=[
                    'carrier',
                    'most_avail_fare_base_ow',
                    'most_avail_fare_total_ow',
                    'most_avail_fare_tax_ow',
                    'most_avail_fare_count_ow',
                    'most_avail_fare_base_rt',
                    'most_avail_fare_total_rt',
                    'most_avail_fare_tax_rt',
                    'most_avail_fare_count_rt',
                    'currency',
                    'fare_basis_ow',
                    'fare_basis_rt',
                    "observation_date_ow",
                    "observation_date_rt"
                ]
            )
            for i in range(1, 6):
                temp_df = pd.DataFrame([{'carrier': json_response['comp{}carrier'.format(i)],
                                         'most_avail_fare_total_ow': json_response['comp{}popularwebfare_ow'.format(i)],
                                         'most_avail_fare_tax_ow': json_response['comp{}Tax'.format(i)],
                                         'most_avail_fare_base_ow': json_response['comp{}popularwebfare_ow'.format(i)] - \
                                                                     json_response['comp{}Tax'.format(i)],
                                         'most_avail_fare_count_ow': 'NA',
                                         'most_avail_fare_freq_ow': json_response['comp{}Frequency'.format(i)],
                                         'observation_date_ow': json_response['comp{}ObservationDate'.format(i)],
                                         'most_avail_fare_total_rt': json_response['comp{}popularwebfare_rt'.format(i)],
                                         'most_avail_fare_tax_rt': json_response['comp{}Tax_rt'.format(i)],
                                         'most_avail_fare_base_rt': json_response['comp{}popularwebfare_rt'.format(i)] - \
                                                                     json_response['comp{}Tax_rt'.format(i)],
                                         'most_avail_fare_count_rt': 'NA',
                                         'most_avail_fare_freq_rt': json_response['comp{}Frequency_rt'.format(i)],
                                         'observation_date_rt': json_response['comp{}ObservationDate_rt'.format(i)],
                                         'currency': json_response['comp{}popularwebfarecurrency_ow'.format(i)],
                                         'fare_basis_ow': json_response['comp{}OutboundFareBasis'.format(i)],
                                         'fare_basis_rt': json_response['comp{}OutboundFareBasis_rt'.format(i)],
                                         }])
                most_avail_df = pd.concat([most_avail_df, temp_df])
            temp_df = pd.DataFrame([{'carrier': Host_Airline_Code,
                                     'most_avail_fare_total_ow': json_response['popularwebfare_ow'],
                                     'most_avail_fare_tax_ow': json_response['hostTax'],
                                     'most_avail_fare_base_ow': json_response['popularwebfare_ow'] - \
                                                                 json_response['hostTax'],
                                     'most_avail_fare_count_ow': 'NA',
                                     'most_avail_fare_freq_ow': json_response['hostFrequency'],
                                     'observation_date_ow': json_response['hostObservationDate'],
                                     'most_avail_fare_total_rt': json_response['popularwebfare_rt'],
                                     'most_avail_fare_tax_rt': json_response['hostTax_rt'],
                                     'most_avail_fare_base_rt': json_response['popularwebfare_rt'] - \
                                                                 json_response['hostTax_rt'],
                                     'most_avail_fare_count_rt': 'NA',
                                     'most_avail_fare_freq_rt': json_response['hostFrequency_rt'],
                                     'observation_date_rt': json_response['hostObservationDate_rt'],
                                     'currency': json_response['popularwebfarecurrency_ow'],
                                     'fare_basis_ow': json_response['hostOutboundFareBasis'],
                                     'fare_basis_rt': json_response['hostOutboundFareBasis_rt'],
                                     }])
            most_avail_df = pd.concat([most_avail_df, temp_df])
            most_avail_df.reset_index(drop=True, inplace=True)
            most_avail_df.fillna('NA', inplace=True)
        elif len(json_response['mpfList']) > 0 and ((dates_code is None) or ("RL" in dates_code_temp)):

            most_avail_df = pd.DataFrame(json_response['mpfList'])
            most_avail_df['most_avail_fare_count_ow'] = 'NA'
            most_avail_df['most_avail_fare_count_rt'] = 'NA'

            #print most_avail_df.head().to_string()
            most_avail_df.drop(['currency_rt'], axis =1, inplace=True)

            most_avail_df.rename(columns={'currency_ow':'currency',
                                           'outbound_fare_basis_ow':'fare_basis_ow',
                                           'outbound_fare_basis_rt': 'fare_basis_rt',
                                           'fare_ow' : 'most_avail_fare_total_ow',
                                           'fare_rt' : 'most_avail_fare_total_rt',
                                           'tax_ow' : 'most_avail_fare_tax_ow',
                                           'tax_rt' : 'most_avail_fare_tax_rt',
                                           'frequency_ow' : 'most_avail_fare_freq_ow',
                                           'frequency_rt' : 'most_avail_fare_freq_rt'
                                           }, inplace=True)
            #print most_avail_df.head().to_string()
            most_avail_df['most_avail_fare_base_ow'] = most_avail_df['most_avail_fare_total_ow']-most_avail_df['most_avail_fare_tax_ow']
            most_avail_df['most_avail_fare_base_rt'] = most_avail_df['most_avail_fare_total_rt'] - most_avail_df['most_avail_fare_tax_rt']
            most_avail_df.reset_index(drop=True, inplace=True)
            most_avail_df.fillna('NA', inplace=True)
            #print "fg fgfjgf"
        else:
            most_avail_df = pd.DataFrame(
                columns=[
                    'carrier',
                    'most_avail_fare_base_ow',
                    'most_avail_fare_total_ow',
                    'most_avail_fare_tax_ow',
                    'most_avail_fare_count_ow',
                    'most_avail_fare_freq_ow',
                    'most_avail_fare_base_rt',
                    'most_avail_fare_total_rt',
                    'most_avail_fare_tax_rt',
                    'most_avail_fare_count_rt',
                    'most_avail_fare_freq_rt',
                    'currency',
                    'fare_basis_ow',
                    'fare_basis_rt',
                    "observation_date_ow",
                    "observation_date_rt"
                ]
            )
            #print "wioooioioi"


    except Exception as err:
        #print err
        most_avail_df = pd.DataFrame(
            columns=[
                'carrier',
                'most_avail_fare_base_ow',
                'most_avail_fare_total_ow',
                'most_avail_fare_tax_ow',
                'most_avail_fare_count_ow',
                'most_avail_fare_freq_ow',
                'most_avail_fare_base_rt',
                'most_avail_fare_total_rt',
                'most_avail_fare_tax_rt',
                'most_avail_fare_count_rt',
                'most_avail_fare_freq_rt',
                'currency',
                'fare_basis_ow',
                'fare_basis_rt',
                "observation_date_ow",
                "observation_date_rt"
            ]
        )

    try:
        host_data['most_available_fare'] = dict(
            base_fare_ow=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'most_avail_fare_base_ow'].values[0],
            frequency_ow=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'most_avail_fare_freq_ow'].values[0],
            tax_ow=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'most_avail_fare_tax_ow'].values[0],
            total_fare_ow=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'most_avail_fare_total_ow'].values[0],
            total_count_ow=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'most_avail_fare_count_ow'].values[0],
            currency=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'currency'].values[0],
            base_fare_rt=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'most_avail_fare_base_rt'].values[0],
            frequency_rt=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'most_avail_fare_freq_rt'].values[0],
            tax_rt=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'most_avail_fare_tax_rt'].values[0],
            total_fare_rt=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'most_avail_fare_total_rt'].values[0],
            total_count_rt=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'most_avail_fare_count_rt'].values[0],
            fare_basis_ow=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'fare_basis_ow'].values[0],
            fare_basis_rt=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'fare_basis_rt'].values[0],
            observation_date_rt=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'observation_date_rt'].values[0],
            observation_date_ow=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'observation_date_ow'].values[0]
        )
    except Exception:
        host_data['most_available_fare'] = dict(
            base_fare_ow="NA",
            frequency_ow="NA",
            tax_ow="NA",
            total_fare_ow="NA",
            total_count_ow="NA",
            currency="NA",
            base_fare_rt="NA",
            frequency_rt="NA",
            tax_rt="NA",
            total_fare_rt="NA",
            total_count_rt="NA",
            fare_basis_ow="NA",
            fare_basis_rt="NA",
            observation_date_rt="NA",
            observation_date_ow="NA"
        )

    for i in range(1, 5):
        temp = {}
        temp['market_share'] = json_response['comp' + str(i) + 'marketshare']
        temp['fms'] = json_response['comp' + str(i) + 'fairmarketshare']
        try:
            # temp['distributor_rating'] = ratings_df.loc[(
            #                                                     ratings_df['Airline'] == json_response[
            #                                                 'comp' + str(i) + 'carrier']), 'distributor_rating'].values[
            #     0]
            # temp['rating'] = ratings_df.loc[(
            #                                         ratings_df['Airline'] == json_response[
            #                                     'comp' + str(i) + 'carrier']), 'overall_rating'].values[0]
            # temp['capacity_rating'] = ratings_df.loc[(
            #                                                  ratings_df['Airline'] == json_response[
            #                                              'comp' + str(i) + 'carrier']), 'capacity_rating'].values[0]
            # temp['market_rating'] = ratings_df.loc[(
            #                                                ratings_df['Airline'] == json_response[
            #                                            'comp' + str(i) + 'carrier']), 'market_rating'].values[0]
            # temp['fare_rating'] = ratings_df.loc[(
            #                                              ratings_df['Airline'] == json_response[
            #                                          'comp' + str(i) + 'carrier']), 'fare_rating'].values[0]
            # temp['product_rating'] = ratings_df.loc[(
            #                                                 ratings_df['Airline'] == json_response[
            #                                             'comp' + str(i) + 'carrier']), 'product_rating'].values[0]

            temp['distributor_rating'] =json_response['distriute' + str(i) + 'Rating']

            temp['rating'] = json_response['comp' + str(i) + 'rating']

            temp['capacity_rating'] = json_response['capacityRating'+str(i)]

            temp['market_rating'] = json_response['market' + str(i) + 'Rating']

            temp['fare_rating'] = json_response['fare' + str(i) + 'Rating']

            temp['product_rating'] = json_response['prduct' + str(i) + 'Rating']


        except Exception:
            temp['rating'] = 5
            temp['distributor_rating'] = 5
            temp['capacity_rating'] = 5
            temp['market_rating'] = 5
            temp['fare_rating'] = 5
            temp['product_rating'] = 5
        try:
            # temp['pax'] = pax_dict_ty[json_response['comp' + str(i) + 'carrier']]  # check
            temp['pax'] = json_response['comp' + str(i) + 'pax']
        except KeyError:
            temp['pax'] = "NA"
        temp['market_share_vtgt'] = json_response['comp' +
                                                  str(i) + 'marketsharevtgt']
        try:
            # temp['market_share_vlyr'] = ((pax_dict_ty[json_response['comp' + str(i) + 'carrier']] /
            #                              market_size_ty[json_response['comp' + str(i) + 'carrier']]) - \
            #                             (pax_dict_ly[json_response['comp' + str(i) + 'carrier']] /
            #                              market_size_ly[json_response['comp' + str(i) + 'carrier']])) * 100
            temp['market_share_vlyr'] = json_response['marketShareVLYR'+str(i)]
        except Exception:
            temp['market_share_vlyr'] = "NA"

        try:
            # cap_adj = cap_ly[json_response['comp' + str(i) + 'carrier']] / cap_ty[
            #     json_response['comp' + str(i) + 'carrier']]
            # temp['pax_vlyr'] = (pax_dict_ty[json_response['comp' + str(i) + 'carrier']] * cap_adj - pax_dict_ly[
            #     json_response['comp' + str(i) + 'carrier']]) * 100 / pax_dict_ly[
            #                        json_response['comp' + str(i) + 'carrier']]
            temp['pax_vlyr'] = json_response['compPaxvlyr'+str(i)]

        except Exception:
            temp['pax_vlyr'] = "NA"
        temp['airline'] = json_response['comp' + str(i) + 'carrier']

        try:
            #print json_response['comp'+str(i)+'carrier']
            temp['most_available_fare'] = dict(
                base_fare_ow=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], 'most_avail_fare_base_ow'].values[0],
                frequency_ow=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], 'most_avail_fare_freq_ow'].values[0],
                tax_ow=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], 'most_avail_fare_tax_ow'].values[0],
                total_fare_ow=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], 'most_avail_fare_total_ow'].values[0],
                total_count_ow=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], 'most_avail_fare_count_ow'].values[0],
                currency=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], 'currency'].values[0],
                base_fare_rt=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], 'most_avail_fare_base_rt'].values[0],
                frequency_rt=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], 'most_avail_fare_freq_rt'].values[0],
                tax_rt=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], 'most_avail_fare_tax_rt'].values[0],
                total_fare_rt=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], 'most_avail_fare_total_rt'].values[0],
                total_count_rt=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], 'most_avail_fare_count_rt'].values[0],
                fare_basis_rt=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], 'fare_basis_rt'].values[0],
                fare_basis_ow=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], 'fare_basis_ow'].values[0],
                observation_date_rt=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], 'observation_date_rt'].values[0],
                observation_date_ow=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                    'comp' + str(i) + 'carrier'], 'observation_date_ow'].values[0],
            )
        except Exception:
            temp['most_available_fare'] = dict(
                base_fare_ow="NA",
                frequency_ow="NA",
                tax_ow="NA",
                total_fare_ow="NA",
                total_count_ow="NA",
                currency="NA",
                base_fare_rt="NA",
                frequency_rt="NA",
                tax_rt="NA",
                total_fare_rt="NA",
                total_count_rt="NA",
                fare_basis_ow="NA",
                fare_basis_rt="NA",
                observation_date_ow="NA",
                observation_date_rt="NA"
            )
        except IndexError:
            temp['most_available_fare'] = dict(
                base_fare_ow="NA",
                frequency_ow="NA",
                tax_ow="NA",
                total_fare_ow="NA",
                total_count_ow="NA",
                currency="NA",
                base_fare_rt="NA",
                frequency_rt="NA",
                tax_rt="NA",
                total_fare_rt="NA",
                total_count_rt="NA",
                fare_basis_ow="NA",
                fare_basis_rt="NA"
            )
        comp_data.append(temp)
    return host_data, comp_data, most_avail_df



def get_popular_fare(pos, origin, destination, compartment,dep_date_start, dep_date_end, db):
    currency_data = get_currency_data(db=db)
    currency_list = []
    if currency_data[pos]['web']:
        currency_list.append(currency_data[pos]['web'])
    if currency_data[pos]['gds']:
        currency_list.append(currency_data[pos]['gds'])
    show_flag = 3
    is_significant = False
    if pos == origin:
        show_flag = 1
    elif pos == destination:
        show_flag = 2
    significant_crsr = list(db.JUP_DB_Target_OD.find(
        {
            "pos": pos, "origin": origin,
            "destination": destination,
            "compartment": compartment,
            "month": int(SYSTEM_DATE[5:7]),
            "year": int(SYSTEM_DATE[0:4]),
            'pax': {'$gte': 100}
        },
        {
            "significant_target": 1
        }
        ).sort(
        [("snap_date", pymongo.DESCENDING)]).limit(1))
    if len(significant_crsr) == 1:
        is_significant = significant_crsr[0]['significant_target']

    response_ = dict(
        pos=pos,
        origin=origin,
        destination=destination,
        od=origin + destination,
        compartment=compartment,
        dep_date_start=dep_date_start,
        dep_date_end=dep_date_end,
        dates_code=[],
        update_date=SYSTEM_DATE,
        show_flag=show_flag,
        is_significant=is_significant
    )
    # print "currency_list = ", currency_list

    response, response_json = hit_java_url(response_)
    if response_json:
        try:
            response['currency_web'] = currency_list[0]
        except KeyError:
            response['currency_web'] = "NA"
        # print "response['currency_web'] = ", response['currency_web']
        try:
            if len(currency_list) > 1:
                response['currency_gds'] = currency_list[1]
            else:
                response['currency_gds'] = "NA"
        except KeyError:
            response['currency_gds'] = "NA"

        host_data, comp_data, mpf_df_for_web_pricing = build_market_data_from_java_response(
            response_json, dep_date_start, dep_date_end, db=db, dates_code=response['dates_code'])

        response['mrkt_data'] = {"host": host_data, "comp": comp_data}
        mpf_df_for_web_pricing = mpf_df_for_web_pricing.drop([
            # 'carrier',
            # 'currency',
            'fare_basis_ow',
            'fare_basis_rt',
            'most_avail_fare_base_ow',
            'most_avail_fare_base_rt',
            'most_avail_fare_count_ow',
            'most_avail_fare_count_rt',
            'most_avail_fare_freq_ow',
            'most_avail_fare_freq_rt',
            # 'most_avail_fare_tax_ow',
            # 'most_avail_fare_tax_rt',
            # 'most_avail_fare_total_ow',
            # 'most_avail_fare_total_rt',
            'observation_date_ow',
            'observation_date_rt'

        ], axis=1)

        pd.set_option('display.max_columns', 500)
        pd.set_option('display.width', 1000)
    else:
        cols = [
            'carrier',
            'currency',
            'most_avail_fare_tax_ow',
            'most_avail_fare_tax_rt',
            'most_avail_fare_total_ow',
            'most_avail_fare_total_rt',
        ]
        mpf_df_for_web_pricing = pd.DataFrame(columns=cols)
        # print(list(mpf_df_for_web_pricing.columns.values))
        # print(mpf_df_for_web_pricing)
    return mpf_df_for_web_pricing

def get_lowest_mpf_reco(mpf_df, flag):
    reco_fare = 0
    if flag == "ow":
        ## Removing of Nan fields
        mpf_df = mpf_df.dropna(subset=['ow_reco_fare'])
        ## Removing of 0 reco fares
        mpf_df = mpf_df[mpf_df['ow_reco_fare'] > 0]
        ## Remove host fare's reco fare incase
        mpf_df = mpf_df[mpf_df['carrier'] != Host_Airline_Code]
        reco_fare = mpf_df['ow_reco_fare'].min()
        min_df_ = mpf_df.loc[
            mpf_df.groupby(by=['ow_channel', 'ow_fare_brand'])['ow_reco_fare'].idxmin()]
        # print min_df_
        try:
            channel = min_df_['ow_channel'].iloc[0]
            fare_brand = min_df_['ow_fare_brand'].iloc[0]
            carrier = min_df_['carrier'].iloc[0]
            differential = min_df_['ow_differential'].iloc[0]
        except IndexError:
            channel = "All"
            fare_brand = "All"
            carrier = ""
            differential = 0
        # print min_df_

    else:
        ## Removing of Nan fields
        mpf_df = mpf_df.dropna(subset=['rt_reco_fare'])
        ## Removing of 0 reco fares
        mpf_df = mpf_df[mpf_df['rt_reco_fare'] > 0]
        ## Remove host fare's reco fare incase
        mpf_df = mpf_df[mpf_df['carrier'] != Host_Airline_Code]
        reco_fare = mpf_df['rt_reco_fare'].min()
        min_df__ = mpf_df.loc[
        mpf_df.groupby(by=['rt_channel', 'rt_fare_brand'])['rt_reco_fare'].idxmin()]
        try:
            channel = min_df__['rt_channel'].iloc[0]
            fare_brand = min_df__['rt_fare_brand'].iloc[0]
            carrier = min_df__['carrier'].iloc[0]
            differential = min_df__['rt_differential'].iloc[0]
        except IndexError:
            channel = "All"
            fare_brand = "All"
            carrier = ""
            differential = 0

    return reco_fare,channel,fare_brand, carrier, differential

def calculate(symbol, tot, new):
    # We are making 0 into 1 where multiplication or division we are perform
    # to avoid 0 divison error or result 0
    if symbol.lower() == "+":
        tot = tot + new
    elif symbol == "-":
        tot = tot - new
    elif symbol == "*":
        if tot == 0:
            tot = 1
        elif new == 0:
            new = 1
        else:
            pass
        tot = tot * new
    elif symbol == "/":
        if tot == 0:
            tot = 1
        elif new == 0:
            new = 1
        else:
            pass
        tot = tot / new
    return tot

def do_analyse(formula_, each, arithmatic_sumbols, add_on, fare_brand_value, sellup_builder):
    sellup_builder.update({each : ""})
    sellup_builder_ = ""
    # print "-------------------",each,'--------------------------'
    formula = deepcopy(formula_)
    # print "formula ", formula_
    # print " after mul/div check "
    builder = ""
    temp = 0
    len_ = 0
    no_value_for_any_brand = ""
    calc_limit = [ 3, 5, 7, 9 ] ## Atleast 2 values are required for computation
    sign = ""
    # first hit after for mul/div
    count = 0
    while count < (len(formula) - 1):
        if formula[count] in ["mul", "div"]:
            a = formula[count-1]
            b = formula[count]
            c = formula[count+1]
            for formula_parameter in [a,b,c]:
                if "%" in formula_parameter:
                    num_percent, brand = formula_parameter.split('%')
                    num_percent = int(num_percent)
                    brand = brand.strip()
                    if brand in fare_brand_value:
                        value = (fare_brand_value[brand] / 100) * num_percent
                        len_ = len_ + 1
                        if len_ in calc_limit:
                            sellup_builder_ = sellup_builder_ + " " + brand + " = " + str(value) + "" + sign + "" + str(
                                temp)
                            temp = calculate(sign, temp, value)
                        # print(value)
                    else:
                        pass
                        # print("We don't have value for ", brand)
                    # print num_percent, brand.strip()
                elif formula_parameter in fare_brand_value:
                    if len_ == 0:
                        temp = temp + fare_brand_value[formula_parameter]
                        sellup_builder_ = sellup_builder_ + " " + formula_parameter + " = " + str(
                            fare_brand_value[formula_parameter])
                        builder = str(temp)
                    len_ = len_ + 1
                    if len_ in calc_limit:
                        sellup_builder_ = sellup_builder_ + " " + formula_parameter + " = " + str(
                            fare_brand_value[formula_parameter]) + " " + sign + " " + str(temp)
                        temp = calculate(sign, temp, fare_brand_value[formula_parameter])

                elif formula_parameter in arithmatic_sumbols:
                    len_ = len_ + 1
                    sign = arithmatic_sumbols[formula_parameter]
                    if sign.lower() == "add":
                        sign = "+"
                    elif sign.lower() == "sub":
                        sign = "-"
                    elif sign.lower() == "mul":
                        sign = "*"
                    elif sign.lower() == "div":
                        sign = "/"
                    else:
                        pass
                        # print("check your symbols")
                    builder = builder + sign

                elif "FEE" in formula_parameter:
                    # print add_on[formula_parameter]
                    len_ = len_ + 1
                    # print len_
                    if len_ in calc_limit:
                        temp = calculate(sign, temp, add_on[formula_parameter])
                        if "TT FEE" not in formula_parameter:
                            sellup_builder_ = sellup_builder_ + " " + formula_parameter + " = " + str(
                                add_on[formula_parameter])
                    else:
                        temp = add_on[formula_parameter]
                        sellup_builder_ = sellup_builder_ + " " + formula_parameter + " = " + str(
                            add_on[formula_parameter])

                elif formula_parameter not in fare_brand_value:
                    no_value_for_any_brand = formula_parameter
                    temp = temp + 0  ## add with 0 by default
                    # print "no data for ", formula_parameter

            # data[count - 1] = perform_operation(data[count - 1], data[count], expression[count + 1])

            formula[count - 1] = 'TT FEE'
            del formula[count + 1]
            del formula[count]
            # print temp
        count += 1
        add_on['TT FEE'] = deepcopy(temp)
        temp = 0
        # print "mul/div value", temp
        # print "mul/div value", add_on['TT FEE']
    # print "After updation of MUL/DIV condition"
    # print formula
    builder = ""
    temp = 0
    len_ = 0
    no_value_for_any_brand = ""
    for formula_parameter in formula:
        # print formula_parameter
        if "%" in str(formula_parameter):
            num_percent, brand = formula_parameter.split('%')
            num_percent = int(num_percent)
            brand = brand.strip()
            if brand in fare_brand_value:
                value = (fare_brand_value[brand]/100)*num_percent
                sellup_builder_ = sellup_builder_ +" "+ brand + " = " + str(value)
                len_ = len_ + 1
                if len_ in calc_limit:
                    temp = calculate(sign, temp, value)
                # print(value)
            else:
                pass
                # print("We don't have value for ", brand)
            # print num_percent, brand.strip()
        elif formula_parameter in fare_brand_value:
            if len_ == 0:
                temp = temp + fare_brand_value[formula_parameter]
                sellup_builder_ = sellup_builder_ + " " + formula_parameter + " = " + str(fare_brand_value[formula_parameter])
                builder = str(temp)
            len_ = len_+1
            if len_ in calc_limit :
                temp = calculate(sign, temp, fare_brand_value[formula_parameter])
                sellup_builder_ = sellup_builder_ + " " + formula_parameter + " = " + str(
                    fare_brand_value[formula_parameter])

        elif formula_parameter in arithmatic_sumbols:
            len_ = len_ + 1
            sign = arithmatic_sumbols[formula_parameter]
            if sign.lower() == "add":
                sign = "+"
            elif sign.lower() == "sub":
                sign = "-"
            elif sign.lower() == "mul":
                sign = "*"
            elif sign.lower() == "div":
                sign = "/"
            else:
                pass
                # print("check your symbols")
            builder = builder+sign

        elif "FEE" in str(formula_parameter):
            # print add_on[formula_parameter]
            len_ = len_ + 1
            # print len_
            if len_ in calc_limit :
                temp = calculate(sign, temp, add_on[formula_parameter])
                if "TT FEE" not in formula_parameter:
                    sellup_builder_ = sellup_builder_ + " " + formula_parameter + " = " + str(
                        add_on[formula_parameter])
            else:
                temp = add_on[formula_parameter]

        elif formula_parameter not in fare_brand_value:
            no_value_for_any_brand = formula_parameter
            temp = temp + 0 ## add with 0 by default

            # print "no data for ", formula_parameter

    if no_value_for_any_brand != "":
        if each in fare_brand_value:
            # Need to reverse the formula for lower level farebrands
            # Start formula with existing farebrand
            inverse_formula = []
            # check same brand is occuring twice
            twice_occur = False
            count = 0
            for formula_parameter in formula_:
                if formula_parameter == no_value_for_any_brand:
                    count = count + 1
            if count == 2:
                num_value = 0
                if "add" in formula_:
                    for formula_parameter in formula_:
                        if "FEE" in str(formula_parameter):
                            num_value = 1 + add_on[formula_parameter]
                if "sub" in formula_:
                    for formula_parameter in formula_:
                        if "FEE" in str(formula_parameter):
                            num_value = 1 - add_on[formula_parameter]
                            # inverse_formula.append(formula_parameter)
                fare_brand_value[no_value_for_any_brand] = fare_brand_value[each] / num_value

            else:
                inverse_formula.append(each)
                for formula_parameter in formula_:
                    # Check with farebrand
                    if formula_parameter == no_value_for_any_brand:
                        pass
                    elif "FEE" in str(formula_parameter):
                        inverse_formula.append(formula_parameter)

                    elif formula_parameter in arithmatic_sumbols:
                        sign = formula_parameter
                        # sign = arithmatic_sumbols[formula_parameter]
                        if formula_parameter.lower() == "add":
                            sign = "sub"
                        elif formula_parameter.lower() == "sub":
                            sign = "add"
                        elif formula_parameter.lower() == "mul":
                            sign = "div"
                        elif formula_parameter.lower() == "div":
                            sign = "mul"
                        else:
                            pass
                        inverse_formula.append(sign)

                # print "inverted formula", inverse_formula
                sellup_builder.update({each : ""})
                fare_brand_value , sellup_builder= do_analyse(inverse_formula, no_value_for_any_brand, arithmatic_sumbols, add_on, fare_brand_value, sellup_builder)
        # fare_brand_value[no_value_for_any_brand] = fare_brand_value[each] - temp
    else:
        fare_brand_value[each] = temp
        sellup_builder.update({each: sellup_builder_})
    # print "sellup_builder"
    # print sellup_builder
    return fare_brand_value, sellup_builder


def fare_brand_formula_sign(add_on, mpf_recomended_fare_rt, channel, fare_brand, delta_currency, flag, channel_fb, sellup):
    arithmatic_sumbols = {"add": "+", "sub": "-", "mul": "*", "div": "/"}
    fare_brand_value = {}
    fare_brand_value[channel + " " + fare_brand] = round(mpf_recomended_fare_rt, 0)
    sellup_builder = dict()
    sellup_builder.update({channel + " " + fare_brand : sellup})
    while len(fare_brand_value) < len(channel_fb)+1:
        for each in channel_fb:

            inverse = False
            # print each
            # formula = fare_brand_formula[each][add_on["TFEE IND"]]

            formula = fare_brand_formula[each]
            try:
                fare_brand_value, sellup_builder = do_analyse(formula, each, arithmatic_sumbols, add_on, fare_brand_value, sellup_builder)

                # fare_brand_value, sellup_builder[each] = do_analyse(formula, each, arithmatic_sumbols, add_on, fare_brand_value)
                # print "fare_brand_value length", len(fare_brand_value)
                # print "channel_fb length", len(channel_fb)
            except KeyError as error:
                print "KeyError", error

            formula[:]
            # print fare_brand_value
    # sellup_builder.update(
    #             {channel + " " + fare_brand: (sellup_builder[channel + " " + fare_brand]) + " " + sellup})
    return fare_brand_value, sellup_builder


def get_fee_from_configuration(fee_label, Add_ons, host_currency,FCR_currency, reco_fare):
    fee = 0
    if Add_ons["FEE_type"] == "A":
        if Add_ons[fee_label] == None:
            fee = 0
        else:
            fee = Add_ons[fee_label] / EXCHANGE_RATE[host_currency] * EXCHANGE_RATE[FCR_currency]

    elif Add_ons["FEE_type"] == "P":
        if Add_ons[fee_label] == None:
            fee = 0
        else:
            fee = (reco_fare / 100) * Add_ons[fee_label]

    if fee_label == "Web-FLEX FEE" or fee_label == "GDS-FLEX FEE":
        if Add_ons[fee_label] == None:
            fee = 0
        else:
            fee = (Add_ons[fee_label]/ 100)

    return fee
# print(fare_brand_creation(mpf_recomended_fare_rt, channel, fare_brand))


def depth_fb(df_with_match,fbc_standardation, pos, add_on, channel, fare_brand_, compartment):

    farebasis = ""
    fare_brand = [x for x in list(fbc_standardation['position_array'][6]['value_array']) if x['value'] == df_with_match['fare_brand']]
    # print df_with_match['fare_brand']
    # print "depth"
    # print fare_brand[0]

    ## This is for Exceptional cases GDS channel without any farebrand
    if channel.lower() == "gds" and fare_brand_.lower() =="value" or channel.lower() == "gds" and fare_brand_.lower() =="business":
        if df_with_match['oneway_return'] == '1':
            farebasis = farebasis + df_with_match['RBD'] + "OW"
        else:
            farebasis = farebasis + df_with_match['RBD'] + "R1Y"

        farebasis = farebasis + \
                    compartment.replace("Y", "2").replace("J", "1") + pos + "1"

    else:
        if df_with_match['oneway_return'] == '1':
            farebasis = farebasis + df_with_match['RBD'] + "O"
        else:
            farebasis = farebasis + df_with_match['RBD'] + "R"
        if fare_brand is None :
            # fare_brand
            farebasis = farebasis + add_on[df_with_match['RBD']][df_with_match['oneway_return'].replace("1", "ow").replace("2", "rt")]["BAG FEE_type"][0] + pos
        elif "lite" in fare_brand[0]['value'].lower():
            farebasis = farebasis + str(fare_brand[0]['key']) + pos
        else:
            farebasis = farebasis + add_on[df_with_match['RBD']][df_with_match['oneway_return'].replace("1", "ow").replace("2", "rt")]["BAG FEE_type"][0] +str(fare_brand[0]['key'])+ pos

        fare_type = [x for x in list(fbc_standardation['position_array'][8]['value_array']) if df_with_match['channel'].lower() in x['value'].lower()]
        if len(fare_type)>0:
            farebasis = farebasis + str(fare_type[0]['key'])
    return farebasis

def generate_new_farebaseis(df_with_match,fbc_standardation, pos, add_on, channel, fare_brand, compartment):
    # print df_with_match

    df_with_match.loc[df_with_match['new_fare'] == True, 'compartment'] = compartment
    df_with_match.loc[df_with_match['new_fare'] == True, 'fare_basis'] = df_with_match[df_with_match['new_fare'] == True].apply(lambda row: depth_fb(
                                                                row,
                                                                fbc_standardation,
                                                                pos,
                                                                add_on,
                                                                channel,
                                                                fare_brand,
                                                                compartment
                                                      ), axis=1)
    # df_with_match.iloc[""] df_new_fare_basis
    # print df_with_match

    return df_with_match

def base_fareconditions(s):
    if (s['new_fare'] == True):
        return s['recommended_fare'] - s['YQ'] - s['YR']
    else:
        return s['base_fare']

def FCR_Sellup_creation(host_fares, RBD_min_fare_ow,rbd_df_ow, pos, add_on_ow,compartment, ow_rt, FCR_fare_value, base_RBD, db, delta_currency_gds, delta_currency_web):
    ## creation of FCR creation dict to pandas dataframe
    row = []
    for key in RBD_min_fare_ow.keys():
        # for k, v in RBD_min_fare_ow[key].items():
        row.append({"RBD": key, "oneway_return": ow_rt, "base_fare": RBD_min_fare_ow[key]})

    # for key in RBD_min_fare_rt.keys():
    #     # for k, v in RBD_min_fare_rt[key].items():
    #     row.append({"RBD": key, "oneway_return": "2", "base_fare": RBD_min_fare_rt[key]})

    full_df = pd.DataFrame(row)
    ow_df = full_df[full_df['oneway_return'] == ow_rt]
    ow_df = ow_df.sort_values(by = "base_fare")

    ow_df["rank"] = ow_df["base_fare"].rank(ascending=1)
    # rt_df["rank"] = rt_df["base_fare"].rank(ascending=1)
    currency = host_fares['currency'].iloc[0]
    # print "---------------Host Fares-----------------"
    # print host_fares
    print "---------------Host Fares Count-----------------"
    print host_fares[(host_fares.channel == "gds") & (host_fares.fare_brand == "GDS FLEX")]
    print ow_df
    print len(host_fares[host_fares['oneway_return'] == ow_rt])
    # print len(host_fares[(host_fares['channel'] == "TA") & (host_fares['fare_brand'] == "LITE")])
    temp_df = host_fares[['RBD', 'fare','currency', 'oneway_return', 'fare_basis','compartment', 'channel', 'fare_brand', 'fare_rule', 'footnote', 'YQ']]
    # temp_df["base_fare"] = 0
    fares_df_ow = temp_df[temp_df['oneway_return'] == ow_rt].sort_values(by='fare').merge(ow_df, on=['RBD', 'oneway_return'], how='outer')
    fares_df_ow['base_fare'].fillna(0, inplace=True)
    print len(fares_df_ow)
    print len(fares_df_ow[fares_df_ow['base_fare'] <= 0])
    # fares_df_rt = temp_df[temp_df['oneway_return'] == '2'].sort_values(by='fare').merge(rt_df, on=['RBD', 'oneway_return'], how='outer')
    fbc_standardation = list(db.JUP_DB_Farebasis_Standardization.find({"journey_type" : {"$in": ['OW', 'RT']}}, {"journey_type" : 1, "position_array" : 1}).sort([("journey_type", pymongo.ASCENDING)]))
    # print host_fares.iloc[0]
    ## List of columns should be required for new fares
    effective_from = host_fares['effective_from'].iloc[0]
    effective_to = host_fares['effective_to'].iloc[0]
    dep_date_from = host_fares['dep_date_from'].iloc[0]
    dep_date_to = host_fares['dep_date_to'].iloc[0]
    sale_date_from = host_fares['sale_date_from'].iloc[0]
    sale_date_to = host_fares['sale_date_to'].iloc[0]
    fare_basis = host_fares['fare_basis'].iloc[0]
    fare_brand = host_fares['fare_brand'].iloc[0]
    RBD = host_fares['RBD'].iloc[0]
    rtg = host_fares['rtg'].iloc[0]
    fare_include = host_fares['fare_include'].iloc[0]
    private_fare = host_fares['private_fare'].iloc[0]
    footnote = host_fares['footnote'].iloc[0]
    batch = host_fares['batch'].iloc[0]
    origin = host_fares['origin'].iloc[0]
    destination = host_fares['destination'].iloc[0]
    OD = host_fares['OD'].iloc[0]
    compartment = host_fares['compartment'].iloc[0]
    oneway_return = host_fares['oneway_return'].iloc[0]
    channel = host_fares['channel'].iloc[0]
    carrier = host_fares['carrier'].iloc[0]
    fare = host_fares['fare'].iloc[0]
    surcharge_date_start_1 = host_fares['surcharge_date_start_1'].iloc[0]
    surcharge_date_start_2 = host_fares['surcharge_date_start_2'].iloc[0]
    surcharge_date_start_3 = host_fares['surcharge_date_start_3'].iloc[0]
    surcharge_date_start_4 = host_fares['surcharge_date_start_4'].iloc[0]
    surcharge_date_end_1 = host_fares['surcharge_date_end_1'].iloc[0]
    surcharge_date_end_2 = host_fares['surcharge_date_end_2'].iloc[0]
    surcharge_date_end_3 = host_fares['surcharge_date_end_3'].iloc[0]
    surcharge_date_end_4 = host_fares['surcharge_date_end_4'].iloc[0]
    surcharge_amount_1 = host_fares['surcharge_amount_1'].iloc[0]
    surcharge_amount_2 = host_fares['surcharge_amount_2'].iloc[0]
    surcharge_amount_3 = host_fares['surcharge_amount_3'].iloc[0]
    surcharge_amount_4 = host_fares['surcharge_amount_4'].iloc[0]
    Average_surcharge = host_fares['Average_surcharge'].iloc[0]
    total_fare_1 = host_fares['total_fare_1'].iloc[0]
    total_fare_2 = host_fares['total_fare_2'].iloc[0]
    total_fare_3 = host_fares['total_fare_3'].iloc[0]
    total_fare_4 = host_fares['total_fare_4'].iloc[0]
    total_fare = host_fares['total_fare'].iloc[0]
    # YQ = host_fares['YQ'].iloc[0]
    try:
        gds_df = host_fares[host_fares['channel'] == "gds"]
        gds_YR = gds_df['YR'].iloc[0]
        gds_YQ = gds_df['YQ'].iloc[0]
        gds_currency = gds_df['currency'].iloc[0]
        gds_taxes = gds_df['taxes'].iloc[0]
        gds_Average_surcharge = gds_df['Average_surcharge'].iloc[0]
        gds_tariff_code = gds_df['tariff_code'].iloc[0]
    except Exception as error:
        print "No existing fares for GDS channel"
        gds_YR = 0
        gds_YQ = 0
        gds_Average_surcharge = 0
        gds_currency = delta_currency_gds
        gds_tariff_code = ""
        taxes = 0

    try:
        web_df = host_fares[host_fares['channel'] == "web"]
        web_YR = web_df['YR'].iloc[0]
        web_YQ = web_df['YQ'].iloc[0]
        web_currency = web_df['currency'].iloc[0]
        web_taxes = web_df['taxes'].iloc[0]
        web_Average_surcharge = web_df['Average_surcharge'].iloc[0]
        web_tariff_code = web_df['tariff_code'].iloc[0]
    except Exception as error:
        print "No existing fares for GDS channel"
        web_YR = 0
        web_YQ = 0
        web_taxes = 0
        web_Average_surcharge = 0
        web_currency = delta_currency_web
        web_tariff_code = ""

    try:
        ta_df = host_fares[host_fares['channel'] == "TA"]
        ta_YR = ta_df['YR'].iloc[0]
        ta_YQ = ta_df['YQ'].iloc[0]
        ta_taxes = ta_df['taxes'].iloc[0]
        ta_currency = ta_df['currency'].iloc[0]
        ta_Average_surcharge = ta_df['Average_surcharge'].iloc[0]
        ta_tariff_code = ta_df['tariff_code'].iloc[0]
    except Exception as error:
        print "No existing fares for GDS channel"
        ta_YR = 0
        ta_YQ = 0
        ta_taxes = 0
        ta_Average_surcharge = 0
        ta_currency = delta_currency_web
        ta_tariff_code = web_tariff_code

    YQ = host_fares['YQ'].iloc[0]
    taxes = host_fares['taxes'].iloc[0]
    # currency = host_fares['currency'].iloc[0]
    fare_rule = host_fares['fare_rule'].iloc[0]
    RBD_type = host_fares['RBD_type'].iloc[0]
    gfs = host_fares['gfs'].iloc[0]
    last_update_date = host_fares['last_update_date'].iloc[0]
    last_update_time = host_fares['last_update_time'].iloc[0]
    competitor_farebasis = host_fares['competitor_farebasis'].iloc[0]
    travel_date_to = host_fares['travel_date_to'].iloc[0]
    travel_date_from = host_fares['travel_date_from'].iloc[0]
    retention_fare = host_fares['retention_fare'].iloc[0]
    flight_number = host_fares['flight_number'].iloc[0]
    day_of_week = host_fares['day_of_week'].iloc[0]
    tax_currency = host_fares['tax_currency'].iloc[0]
    Fixed_tax = host_fares['Fixed_tax'].iloc[0]
    percentage_block = host_fares['percentage_block'].iloc[0]
    Fixed_tax_breakup = host_fares['Fixed_tax_breakup'].iloc[0]
    Percentage_tax_breakup = host_fares['Percentage_tax_breakup'].iloc[0]
    is_expired = host_fares['is_expired'].iloc[0]
    display_only = host_fares['display_only'].iloc[0]
    travel_flag_check = host_fares['travel_flag_check'].iloc[0]

    original_farebrand = ""
    # List_of_reco_df = pd.DataFrame(columns=["RBD", "fare", "oneway_return", "fare_basis", "compartment", "channel", "fare_brand",  "base_fare",  "rank",  "gds Value",  "gds Value max", "to_delete_fare",  "reco_fare", "new_fare"])
    List_of_reco_df = pd.DataFrame()
    if compartment == "Y":
        list_ = ["Web Lite","TA Lite", "Web Value", "TA Value", "TA FLY+Visa", "Web Flex", "TA Flex", "GDS GDS Flex", "GDS  "]
        # list_ = ["TA Lite"]
    else:
        list_ = ["Web Business", "GDS  "]
    # list = ["TA Lite", "web Value", "TA Value", "TA FLY+Visa", "web Flex", "TA Flex", "gds GDS Flex"]:
    ## This is for the combination of Channel and Farebrand combination
    for channel_fb in list_:
        try:
            ## This is for Exceptional cases where we get only gds fare without any fare brand
            if channel_fb ==  "GDS  ":
                if compartment == "Y":
                    channel_fb = "GDS Value"
                else:
                    channel_fb = "GDS Business"

            if channel_fb.split(" ",2) [0] == "TA":
                channel = channel_fb.split(" ", 2)[0]
            else:
                ## GDS and Web channels we are considering as lower case only in ATPCO Fares Rules collection
                channel = channel_fb.split(" ", 2)[0]#.lower()

            fare_brand = channel_fb.split(" ", 1)[1].upper()
            original_farebrand = channel_fb.split(" ", 1)[1].upper()
            fare_brand_ = channel_fb.split(" ", 1)[1]

            print rbd_df_ow
            print channel+" "+fare_brand_+"------"+channel+" "+fare_brand_+ " max"
            each_combination = rbd_df_ow[["RBD", channel+" "+fare_brand_, channel+" "+fare_brand_ + " max"]]

            # specific_channels_fares_df_ow = fares_df_ow[(fares_df_ow["channel"] == channel) & (fares_df_ow["fare_brand"] == fare_brand)]


            # ## This is for Exceptional cases where we get only gds fare without any fare brand
            if (channel.lower() == "gds" and fare_brand.lower() == "value") or (channel.lower() == "gds" and fare_brand.lower() == "business"):
                specific_channels_fares_df_ow = fares_df_ow[
                    (fares_df_ow["channel"] == channel.lower().replace("gds", "gds").replace("web", "web").replace("ta", "TA")) & (fares_df_ow["fare_brand"] == "")]
                each_combination['channel'] = data = channel.lower().replace("gds", "gds").replace("web", "web").replace("ta", "TA")
                # each_combination['fare_brand'] = data = fare_brand
                each_combination['fare_brand'] = data = ""
            else:
                specific_channels_fares_df_ow = fares_df_ow[
                    (fares_df_ow["channel"] == channel.lower().replace("gds", "gds").replace("web", "web").replace("ta", "TA")) & (fares_df_ow["fare_brand"] == fare_brand)]
                each_combination['channel'] = data = channel.lower().replace("gds", "gds").replace("web", "web").replace("ta", "TA")
                each_combination['fare_brand'] = data = fare_brand

            each_combination = each_combination.dropna(subset=[channel_fb])

            ## Check with fares where RBD is matching
            df_with_match = pd.merge(specific_channels_fares_df_ow, each_combination, on=["RBD", "channel", "fare_brand"], how='outer')
            print "FCR sellup data"
            print(df_with_match)

            df_with_match["to_delete_fare"] = False
            df_with_match["new_fare"] = False

            # Fares to be delete
            df_with_match.loc[df_with_match[channel_fb].isnull(), "to_delete_fare"] = True

            # recommend new base fare
            # Get the minimum fare
            both_fares_df = df_with_match[df_with_match['base_fare'].notnull() & df_with_match[channel_fb].notnull()]

            # Only for base rbd we should update the minimum value all the other RBD we should the range of fares if it's lying then
            # Keep the same fare other wise update minimum fares

            # For updating base RBD fares
            df_with_match.loc[(
                          (df_with_match['fare'].notnull()) &
                          (df_with_match[channel_fb].notnull()) &
                          (df_with_match['RBD'] == base_RBD)), "reco_fare"] = \
            df_with_match.loc[(
                    (df_with_match['fare'].notnull()) &
                    (df_with_match[channel_fb].notnull()) &
                    (df_with_match['RBD'] == base_RBD))][channel_fb]

            # Check and update RBD min range of fares whether existing fares are there or update the recommend fare
            df_with_match.loc[(
                      (df_with_match['fare'].notnull()) &
                      (df_with_match[channel_fb].notnull()) &
                      (df_with_match['RBD'] != base_RBD)), "reco_fare"] = \
                    df_with_match.loc[(
                            (df_with_match['fare'].notnull()) &
                            (df_with_match[channel_fb].notnull()) &
                            (df_with_match['RBD'] != base_RBD))][channel_fb]

            ## Add fare with YQ or YR as per the calculation
            df_with_match['temp_fare'] = df_with_match['fare'] + df_with_match['YQ']
            # # Replace any existing range of fare for any given RBD
            df_with_match.loc[(
                              (df_with_match['fare'].notnull()) &
                              (df_with_match['temp_fare'] >= df_with_match[channel_fb]) &
                              (df_with_match['temp_fare'] < df_with_match[channel + " " + fare_brand_ + " max"]) &
                              (df_with_match['RBD'] != base_RBD)
                              ), "reco_fare"] = \
                    df_with_match.loc[(
                            (df_with_match['fare'].notnull()) &
                            (df_with_match['temp_fare'] >= df_with_match[channel_fb]) &
                            (df_with_match['temp_fare'] < df_with_match[channel + " " + fare_brand_ + " max"]) &
                            (df_with_match['RBD'] != base_RBD)
                    )]['temp_fare']



            ## New fare creation
            ## Assign reco fares of any RBD channel and Farebrands lowest fare amount
            df_with_match.loc[df_with_match['fare'].isnull(), "new_fare"] = True
            df_with_match.loc[df_with_match['fare'].isnull(), "to_delete_fare"] = False
            df_with_match.loc[df_with_match['fare'].isnull(), "reco_fare"] = \
            df_with_match.loc[df_with_match['fare'].isnull()][channel_fb]
            df_with_match.loc[df_with_match['fare'].isnull(), "oneway_return"] = ow_rt
            print " With Reco "
            print(df_with_match)
            ## Create fare basis according to the combineation
            data_df = generate_new_farebaseis(df_with_match, fbc_standardation[0], pos, add_on_ow, channel, fare_brand, compartment)

            ## Drop the base values of core channel and fare brand values otherthan actual recommendation values
            data_df = data_df.drop([
                channel + " " + fare_brand_,
                channel + " " + fare_brand_ + " max",
                "rank"
            ],  axis = 1)

            # print data_df
            print "-----------------before merge - {0} {1} ---------------".format(channel, fare_brand)
            print data_df
            List_of_reco_df = List_of_reco_df.append(data_df)
            # List_of_reco_df['currency'] = currency
            each_combination = each_combination.drop(each_combination.index, inplace=True)

            # print  List_of_reco_df
            # print generate_new_farebaseis(df_with_match,fbc_standardation[0], pos, add_on_ow, channel, fare_brand, compartment)

        except IOError as error:
            print "{0} is not there in RBD wise Reco fares list".format(error)
    # host_fares = host_fares.merge(List_of_reco_df[['RBD', 'fare','currency', 'oneway_return', 'fare_basis','compartment', 'channel', 'fare_brand', 'fare_rule', 'footnote', 'reco_fare', 'new_fare', 'to_delete_fare']], on='fare_basis', how='outer')
    # print asd
    List_of_reco_df = List_of_reco_df.reset_index()
    print List_of_reco_df
    print "Overall count"
    print len(List_of_reco_df)
    print "New fares count"
    print len(List_of_reco_df[List_of_reco_df['new_fare'] == True])
    print "Old fares count"
    print len(List_of_reco_df[List_of_reco_df['new_fare'] == False])
    print len(host_fares)
    host_fares = pd.merge(host_fares, List_of_reco_df, how='outer', on=['RBD', 'fare','currency', 'oneway_return', 'fare_basis','compartment', 'channel', 'fare_brand', 'fare_rule', 'footnote', "YQ"])
    print("---------------------After recommendation--------------")
    # print len(host_fares)
    print len(host_fares)

    ## Updating default values for new fares
    host_fares.loc[host_fares['fare'].isnull(), "fare_rule"] = ""
    host_fares.loc[host_fares['fare'].isnull(), "footnote"] = ""
    # host_fares.loc[host_fares['fare'].isnull(), "base_fare"] = 0
    ## Update the commonly available records
    host_fares.loc[host_fares['fare'].isnull(), 'effective_from'] = effective_from
    host_fares.loc[host_fares['fare'].isnull(), 'effective_to'] = effective_to
    host_fares.loc[host_fares['fare'].isnull(), 'dep_date_from'] = dep_date_from
    host_fares.loc[host_fares['fare'].isnull(), 'dep_date_to'] = dep_date_to
    host_fares.loc[host_fares['fare'].isnull(), 'sale_date_from'] = ""
    host_fares.loc[host_fares['fare'].isnull(), 'sale_date_to'] = ""
    host_fares.loc[host_fares['fare'].isnull(), 'rtg'] = rtg

    host_fares.loc[host_fares['fare'].isnull(), 'fare_include'] = fare_include
    host_fares.loc[host_fares['fare'].isnull(), 'private_fare'] = private_fare
    host_fares.loc[host_fares['fare'].isnull(), 'batch'] = batch
    host_fares.loc[host_fares['fare'].isnull(), 'origin'] = origin
    host_fares.loc[host_fares['fare'].isnull(), 'destination'] = destination
    host_fares.loc[host_fares['fare'].isnull(), 'OD'] = OD
    host_fares.loc[host_fares['fare'].isnull(), 'carrier'] = carrier
    host_fares.loc[host_fares['fare'].isnull(), 'surcharge_date_start_1'] = surcharge_date_start_1
    host_fares.loc[host_fares['fare'].isnull(), 'surcharge_date_start_2'] = surcharge_date_start_2
    host_fares.loc[host_fares['fare'].isnull(), 'surcharge_date_start_3'] = surcharge_date_start_3
    host_fares.loc[host_fares['fare'].isnull(), 'surcharge_date_start_4'] = surcharge_date_start_4
    host_fares.loc[host_fares['fare'].isnull(), 'surcharge_date_end_1'] = surcharge_date_end_1
    host_fares.loc[host_fares['fare'].isnull(), 'surcharge_date_end_2'] = surcharge_date_end_2
    host_fares.loc[host_fares['fare'].isnull(), 'surcharge_date_end_3'] = surcharge_date_end_3
    host_fares.loc[host_fares['fare'].isnull(), 'surcharge_date_end_4'] = surcharge_date_end_4
    host_fares.loc[host_fares['fare'].isnull(), 'surcharge_amount_1'] = surcharge_amount_1
    host_fares.loc[host_fares['fare'].isnull(), 'surcharge_amount_2'] = surcharge_amount_2
    host_fares.loc[host_fares['fare'].isnull(), 'surcharge_amount_3'] = surcharge_amount_3
    host_fares.loc[host_fares['fare'].isnull(), 'surcharge_amount_4'] = surcharge_amount_4
    # host_fares.loc[host_fares['fare'].isnull(), 'Average_surcharge'] = Average_surcharge
    host_fares.loc[host_fares['fare'].isnull(), 'total_fare_1'] = total_fare_1
    host_fares.loc[host_fares['fare'].isnull(), 'total_fare_2'] = total_fare_2
    host_fares.loc[host_fares['fare'].isnull(), 'total_fare_3'] = total_fare_3
    host_fares.loc[host_fares['fare'].isnull(), 'total_fare_4'] = total_fare_4
    # host_fares.loc[host_fares['fare'].isnull(), 'total_fare'] = total_fare
    ## Web channel YQ should have 0
    # host_fares.loc[host_fares['fare'].isnull(), 'YR'] = YR
    # delta_currency_web
    # convert_tax_currency(Add_ons[key][ow_rt]['minimum'], delta_currency, FCR_currency)
    host_fares.loc[host_fares['channel'] == "web", 'Average_surcharge'] = convert_tax_currency(web_Average_surcharge, delta_currency_web, web_currency)
    host_fares.loc[host_fares['channel'] == "TA", 'Average_surcharge'] = convert_tax_currency(ta_Average_surcharge, delta_currency_web, ta_currency)
    host_fares.loc[host_fares['channel'] == "gds", 'Average_surcharge'] = convert_tax_currency(gds_Average_surcharge, delta_currency_gds, gds_currency)

    host_fares.loc[host_fares['channel'] == "web", 'YR'] = 0
    host_fares.loc[host_fares['channel'] == "TA", 'YR'] = convert_tax_currency(ta_YR, delta_currency_web, ta_currency)
    host_fares.loc[host_fares['channel'] == "gds", 'YR'] = convert_tax_currency(gds_YR, delta_currency_gds, gds_currency)

    host_fares.loc[host_fares['channel'] == "web", 'taxes'] = web_taxes
    host_fares.loc[host_fares['channel'] == "TA", 'taxes'] = web_taxes
    host_fares.loc[host_fares['channel'] == "gds", 'taxes'] = convert_tax_currency(web_taxes, delta_currency_gds,
                                                                                delta_currency_web)


    host_fares.loc[host_fares['channel'] == "web", 'YQ'] = convert_tax_currency(web_YQ, delta_currency_web, web_currency)
    host_fares.loc[host_fares['channel'] == "TA", 'YQ'] = convert_tax_currency(web_YQ, delta_currency_web, web_currency)
    host_fares.loc[host_fares['channel'] == "gds", 'YQ'] = convert_tax_currency(gds_YQ, delta_currency_gds, gds_currency)

    host_fares.loc[host_fares['channel'] == "web", 'currency'] = delta_currency_web
    host_fares.loc[host_fares['channel'] == "TA", 'currency'] = delta_currency_web
    host_fares.loc[host_fares['channel'] == "gds", 'currency'] = delta_currency_gds

    host_fares.loc[host_fares['channel'] == "web", 'tariff_code'] = web_tariff_code
    host_fares.loc[host_fares['channel'] == "TA", 'tariff_code'] = ta_tariff_code
    host_fares.loc[host_fares['channel'] == "gds", 'tariff_code'] = gds_tariff_code
    # ta_tariff_code = web_tariff_code
    # host_fares.loc[host_fares['fare'].isnull(), 'tariff_code'] = tariff_code

    # host_fares.loc[host_fares['fare'].isnull(), 'YQ'] = YQ
    # host_fares.loc[host_fares['fare'].isnull(), 'taxes'] = taxes
    # host_fares.loc[host_fares['fare'].isnull(), 'currency'] = currency

    host_fares.loc[host_fares['fare'].isnull(), 'RBD_type'] = ""
    host_fares.loc[host_fares['fare'].isnull(), 'gfs'] = ""
    host_fares.loc[host_fares['fare'].isnull(), 'last_update_date'] = last_update_date
    host_fares.loc[host_fares['fare'].isnull(), 'last_update_time'] = last_update_time
    host_fares.loc[host_fares['fare'].isnull(), 'competitor_farebasis'] = None
    host_fares.loc[host_fares['fare'].isnull(), 'travel_date_to'] = travel_date_to
    host_fares.loc[host_fares['fare'].isnull(), 'travel_date_from'] = travel_date_from
    # host_fares.loc[host_fares['fare'].isnull(), 'YQ'] = YQ
    host_fares.loc[host_fares['fare'].isnull(), 'retention_fare'] = 0
    host_fares.loc[host_fares['fare'].isnull(), 'flight_number'] = ""
    host_fares.loc[host_fares['fare'].isnull(), 'day_of_week'] = ""
    host_fares.loc[host_fares['fare'].isnull(), 'tax_currency'] = tax_currency
    host_fares.loc[host_fares['fare'].isnull(), 'Fixed_tax'] = Fixed_tax
    host_fares.loc[host_fares['fare'].isnull(), 'percentage_block'] = percentage_block
    host_fares.loc[host_fares['fare'].isnull(), 'Fixed_tax_breakup'] = Fixed_tax_breakup
    host_fares.loc[host_fares['fare'].isnull(), 'Percentage_tax_breakup'] = Percentage_tax_breakup
    host_fares.loc[host_fares['fare'].isnull(), 'is_expired'] = is_expired
    host_fares.loc[host_fares['fare'].isnull(), 'display_only'] = display_only
    host_fares.loc[host_fares['fare'].isnull(), 'travel_flag_check'] = travel_flag_check
    # Adding tax
    host_fares.loc[(host_fares['fare'].isnull()) & (host_fares['channel'] == "gds"), 'fare_rule'] = "01"+pos
    host_fares.loc[(host_fares['fare'].isnull()) & (host_fares['channel'] == "web"), 'fare_rule'] = "62"+pos
    host_fares.loc[(host_fares['fare'].isnull()) & (host_fares['channel'] == "TA"), 'fare_rule'] = "VA"+pos
    host_fares.loc[host_fares['fare'].isnull(), "fare"] = 0
    # host_fares.loc[host_fares['fare'].isnull(), "base_fare"] = host_fares.loc[host_fares['fare'].isnull()]['reco_fare']
    # host_fares.loc[host_fares['fare'].isnull(), "base_fare"] = 0

    ## Updation of flags require fields for workflow screen
    host_fares = host_fares.rename(columns={"reco_fare": "recommended_fare"})
    # host_fares.loc[host_fares['fare'].isnull(), "fare"] = host_fares[host_fares['fare'] == 0]['recommended_fare']
    # host_fares['fare'] = np.where(host_fares['fare'] == 0, host_fares['recommended_fare'], host_fares['fare'])
    # host_fares['fare'] = host_fares['recommended_fare']

    if "Base Fare" in FCR_fare_value and "YQ" in FCR_fare_value and "YR" in FCR_fare_value:

        host_fares.loc[host_fares['new_fare'] == True, 'fare'] = host_fares[
            host_fares['new_fare'] == True].apply(
            lambda row: row['recommended_fare'] - row['YQ'] - row['YR'], axis=1)

        host_fares['recommended_fare_base'] = host_fares['recommended_fare'] - host_fares['YQ'] - host_fares['YR']
        # # new change
        # host_fares['recommended_fare_base'] = np.where(host_fares['recommended_fare'] == 0, host_fares['fare'], host_fares['recommended_fare_base'])
        host_fares['recommended_fare'] = host_fares['recommended_fare_base'] + host_fares['YQ'] + host_fares['YR'] + \
                                         host_fares[
                                             'Average_surcharge']  # This is recommended retention fare


    elif "Base Fare" in FCR_fare_value and "YQ" in FCR_fare_value:
        host_fares.loc[host_fares['new_fare'] == True, 'fare'] = host_fares[
            host_fares['new_fare'] == True].apply(
            lambda row: row['recommended_fare'] - row['YQ'], axis=1)

        host_fares['recommended_fare_base'] = host_fares['recommended_fare'] - host_fares['YQ']
        # # new change
        # host_fares['recommended_fare_base'] = np.where(host_fares['recommended_fare'] == 0, host_fares['fare'],
        #                                                host_fares['recommended_fare_base'])
        host_fares['recommended_fare'] = host_fares['recommended_fare_base'] + host_fares['YQ'] + host_fares['YR'] + \
                                         host_fares[
                                             'Average_surcharge']  # This is recommended retention fare
    elif "Base Fare" in FCR_fare_value:

        host_fares.loc[host_fares['new_fare'] == True, 'fare'] = host_fares[
            host_fares['new_fare'] == True].apply(
            lambda row: row['recommended_fare'], axis=1)

        host_fares['recommended_fare_base'] = host_fares['recommended_fare']
        # # new change
        # host_fares['recommended_fare_base'] = np.where(host_fares['recommended_fare'] == 0, host_fares['fare'],
        #                                                host_fares['recommended_fare_base'])
        host_fares['recommended_fare'] = host_fares['recommended_fare_base'] + host_fares['YQ'] + host_fares['YR'] + \
                                         host_fares[
                                             'Average_surcharge']  # This is recommended retention fare

    host_fares['base_fare'].fillna(0, inplace=True)
    host_fares['recommended_fare_total'] = host_fares['recommended_fare'] + host_fares['taxes']
    host_fares['perc_change'] = (host_fares['recommended_fare_base'] - host_fares['fare']) * 100 / host_fares['fare']
    host_fares['status'] = "I"
    host_fares['total_fare'] = host_fares['fare'] + host_fares['YQ'] + host_fares['YR'] + host_fares['Average_surcharge'] + host_fares['taxes']

    # host_fares.loc[host_fares['perc_change'] > 0, 'status'] = "I"
    host_fares.loc[host_fares['perc_change'] < 0, 'status'] = "S"
    host_fares.loc[host_fares['new_fare'] == True, 'status'] = "N"
    host_fares.loc[host_fares['to_delete_fare'] == True, 'status'] = "D"
    host_fares['reco_from_model'] = False
    host_fares.loc[(host_fares['recommended_fare'] > 0), 'reco_from_model'] = True
    host_fares['base_fare'] = np.where(host_fares['new_fare'] == True, host_fares['recommended_fare_base'],
                                              host_fares['base_fare'])
    host_fares['fare'].fillna(0, inplace=True)
    host_fares['total_fare'].fillna(0, inplace=True)
    host_fares['recommended_fare_total'].fillna(0, inplace=True)
    host_fares['recommended_fare_base'].fillna(0, inplace=True)
    host_fares['recommended_fare'].fillna(0, inplace=True)
    host_fares['base_fare'] = host_fares['base_fare'].round(0).astype(float)
    host_fares['fare'] = host_fares['fare'].round(0).astype(float)
    host_fares['total_fare'] = host_fares['total_fare'].round(0).astype(float)
    host_fares['recommended_fare_total'] = host_fares['recommended_fare_total'].round(0).astype(float)
    host_fares['recommended_fare_base'] = host_fares['recommended_fare_base'].round(0).astype(float)
    host_fares['recommended_fare'] = host_fares['recommended_fare'].round(0).astype(float)
    # host_fares['recommended_fare'] = np.where(host_fares['recommended_fare'] == 0, host_fares['fare'], host_fares['recommended_fare'])
    # print "FCR Sellup"
    # print host_fares
    # host_fares['recommended_fare_base']

    return host_fares

def get_add_on_doc(host_fares, base_baggage, add_od_doc, Add_ons, mpf_recomended_fare, delta_currency, FCR_currency, ow_rt, compartment, FCR_fare_value, fare_brand_currency):

    # print "get_add_on_doc"
    # print host_fares
    Add_ons = Add_ons
    RBD_Add_on = {}
    RBD_min_fares = {}
    for key in Add_ons.keys():
        add_on = {}
        add_on_ow_rt = {}
        add_on["BAG FEE_type"] = base_baggage
        # add_on["BAG FEE_type"] = Add_ons[key][ow_rt]['Add-on']['BAG FEE']['Type_BAG']
        # print key
        for type in add_od_doc:
            # add_on["TFEE IND"] = Add_ons[key][ow_rt]['TFEE IND']
            # print(Add_ons[key][ow_rt]['Add-on'])
            if Add_ons[key]['compartment'] == compartment:
                add_on[type] = get_fee_from_configuration(type, Add_ons[key][ow_rt]['Add-on'][type], delta_currency, fare_brand_currency,
                                                            mpf_recomended_fare)
                add_on_ow_rt[ow_rt] = add_on
        RBD_Add_on[key] = add_on_ow_rt

        if Add_ons[key]['compartment'] == compartment:
            if Add_ons[key][ow_rt]['minimum'] != None:
                RBD_min_fares[key] = convert_tax_currency(Add_ons[key][ow_rt]['minimum'], delta_currency, FCR_currency)
                # print RBD_min_fares[key]
                # print Add_ons[key][ow_rt]['minimum']
            else:
                RBD_min_fares[key] = 0

    sorted_RBD_min_fares = sorted(RBD_min_fares.items(), key=lambda kv: kv[1])
    # print sorted_RBD_min_fares
    RBD_min_fares_ = dict()
    first_hit = True
    previous_key = ""

    for key in collections.OrderedDict(sorted_RBD_min_fares):
        if first_hit == True:
            previous_key = key
            first_hit = False
        else:
            RBD_min_fares_[previous_key] = RBD_min_fares[key]
            previous_key = key
    RBD_min_fares_[previous_key] = 999999999
    RBD_actual_range_fares = deepcopy(RBD_min_fares)
    # print "RBD_min_fares_['R']"
    # print RBD_min_fares['R']
    # print RBD_min_fares_['R']
    for key in RBD_min_fares.keys():
        existing_fare = 0
        try:
            fares = host_fares[host_fares['RBD'] == key]
            if len(fares) > 0:
                # print fares
                fare = fares['fare'].iloc[0]
                if "Base Fare" in FCR_fare_value and "YQ" in FCR_fare_value and "YR" in FCR_fare_value:
                    existing_fare = round(fares['fare'].iloc[0], 0) + round(fares['YQ'].iloc[0], 0) + \
                                                     round(fares['YR'].iloc[0], 0)  # This is recommended retention fare
                elif "Base Fare" in FCR_fare_value and "YQ" in FCR_fare_value:
                    existing_fare = round(fares['fare'].iloc[0], 0) +  round(fares['YQ'].iloc[0], 0)  # This is recommended retention fare
                elif "Base Fare" in FCR_fare_value:
                    existing_fare = round(fares['fare'].iloc[0], 0)  # This is recommended retention fare


                # host_fares['base_fare'].fillna(0, inplace=True)
                # existing_fare = host_fares['fare'].iloc[0]
                if existing_fare >= RBD_min_fares[key] and existing_fare < RBD_min_fares_[key]:
                    # RBD_min_fares[key] = fare
                    RBD_min_fares[key] = existing_fare
                else:
                    pass

        except Exception as error:
            print error
            pass

    # print RBD_min_fares_
    # print RBD_min_fares
    # print asf
    return RBD_Add_on, RBD_min_fares, RBD_actual_range_fares, RBD_min_fares_

def chunk(l_fb, fb):
    # print fb
    l_fb.reverse()
    fbs = []
    for each in l_fb:
        fbs.append(each)
        # if each in fb:
        #     break
    l_fb.reverse()
    return fbs

def RBD_wise_farebrand(fare_brand_value, FCR_creation,base_channel, base_fare_brand, list_fare_brand_overall, fb_flag, RBD_wise_max_fare):

    ## filter list of fare brand for the actual list
    print list_fare_brand_overall
    print base_fare_brand
    filtered_base_fb = list()
    filtered_base_fb = chunk(list_fare_brand_overall, base_fare_brand)
    print "after filter"
    print filtered_base_fb
    print fare_brand_value
    # print filtered_base_fb
    filtered_fb = {k : v for k, v in fare_brand_value[fare_brand_value.keys()[0]].iteritems() if k.split(" ", 1)[1].rsplit(" ", 1)[0] in filtered_base_fb }
    # print filtered_fb
    ## consider base fare brand
    print "filter fb"
    print filtered_fb

    # row = []
    # for key in FCR_creation.keys():
    #     for k, v in FCR_creation[key].items():
    #         row.append({"RBD": key, "oneway_return": k.replace("ow", "1").replace("rt", "2"), "base_fare": v})
    # full_df = pd.DataFrame(row)

    ## RBD level new recommendation
    row = []
    for key in fare_brand_value.keys():
        rbd_dic = {}
        rbd_dic["RBD"] = key
        rbd_dic["oneway_return"] = fb_flag
        for k, v in fare_brand_value[key].items():
            rbd_dic[k] = v
            rbd_dic[k+" max"] = RBD_wise_max_fare[key]
        row.append(rbd_dic)
    full_df = pd.DataFrame(row)

    # ow_df = full_df[full_df['oneway_return'] == "1"]
    rt_df = full_df[full_df['oneway_return'] == fb_flag]

    # Updating of max fares only for base fare brand, all other fare brand will have the same number
    # of fare brand value as max value
    for k, v in filtered_fb.items():
        rt_df = rt_df.sort_values(by=k)
        if base_fare_brand in k and base_channel in k:
            # rt_df[k + " max"] = rt_df[k + " max"]
            pass
        else:
            # rt_df[k+" max"] = rt_df[k].shift(-1, axis = 0)
            # rt_df[k+" max"].fillna(999999, inplace=True)
            rt_df[k + " max"] = rt_df[k]

    pd.set_option('display.max_columns', 500)
    return rt_df

@measure(JUPITER_LOGGER)
def get_inverted_fares(host_fares, compartment, rbd_cur):
    if compartment == 'Y':
        rbd_ladder = rbd_cur['RBDs_Y']
    elif compartment == 'J':
        rbd_ladder = rbd_cur['RBDs_J']
    else:
        rbd_ladder = rbd_cur['RBDs_F']
    map_ranks = pd.DataFrame()
    map_ranks['RBD'] = rbd_ladder
    map_ranks['rank'] = range(len(rbd_ladder), 0, -1)
    host_fares['unique'] = range(len(host_fares))  # unique is the index of each fare as found in host_fares
    temp_df = host_fares[["RBD",  "base_fare", "channel", "compartment", "fare", "fare_basis", "fare_brand", "new_fare", "oneway_return", "recommended_fare", "to_delete_fare", "unique"]]
    # temp_df = host_fares[['RBD', 'fare', 'oneway_return', 'fare_basis', 'unique']]
    fares_df_ow = temp_df[temp_df['oneway_return'] == '1'].sort_values(by='recommended_fare').merge(map_ranks, on='RBD', how='left')
    fares_df_rt = temp_df[temp_df['oneway_return'] == '2'].sort_values(by='recommended_fare').merge(map_ranks, on='RBD', how='left')
    fares_df_ow['prev_rank'] = fares_df_ow['rank'].shift()
    fares_df_rt['prev_rank'] = fares_df_rt['rank'].shift()
    fares_df_ow['diff_rank'] = fares_df_ow['rank'] - fares_df_ow['prev_rank']
    fares_df_rt['diff_rank'] = fares_df_rt['rank'] - fares_df_rt['prev_rank']
    fares_df_ow['is_wrong'] = 0
    fares_df_rt['is_wrong'] = 0
    if len(fares_df_ow) > 0:
        fares_df_ow.loc[fares_df_ow['diff_rank'] < 0, "is_wrong"] = 1
    if len(fares_df_rt) > 0:
        fares_df_rt.loc[fares_df_rt['diff_rank'] < 0, "is_wrong"] = 1
    temp_df = pd.concat([fares_df_ow, fares_df_rt])
    host_fares = host_fares.merge(temp_df[['unique', 'is_wrong']], on='unique', how='left')
    host_fares['is_wrong'].fillna(0, inplace=True)
    host_fares.loc[~host_fares['RBD'].isin(rbd_ladder), 'is_wrong'] = 2
    return host_fares


def get_sellup_doc(pricing_models,compartment, db):
    # print pricing_models['model']['sellup_no']
    try:
        sellup_code = pricing_models['sellup_no']
    except KeyError as error:
        sellup_code = "SP001"
    sellup_doc = db.JUP_DB_Sellup_Master.find_one({"sellup_no": sellup_code })
    if len(sellup_doc) > 0:
        if compartment == "Y":
            fare_brand_formulas = sellup_doc["sellup_formulas_Y"]
        else:
            fare_brand_formulas = sellup_doc["sellup_formulas_J"]

        RBD_sellup = sellup_doc["RBD"]
        FCR_currency = sellup_doc['FCR']["currency"]
        fare_brand_currency = sellup_doc['fare_brand']["currency"]
        FCR = sellup_doc['FCR']["fare_value"]
    else:
        print " No farebrand formula and RBD "
        fare_brand_formulas = []
        RBD_sellup = []
        ## Default keep as AED
        FCR_currency = "AED"
    return fare_brand_formulas, RBD_sellup, FCR_currency, FCR, fare_brand_currency

def check_threshould(row, fare_percentage,p_a_flag, Flag):
    return_flag = False
    try:
        if Flag == True:
            if p_a_flag.lower()== 'p':
                variance = row['base_fare'] + row['base_fare'] / 100 * fare_percentage
                if row['recommended_fare'] >= variance:
                    return_flag = True
            elif p_a_flag.lower()== 'a':
                variance = row['base_fare'] + fare_percentage
                if row['recommended_fare'] >= variance:
                    return_flag = True
        else:
            if p_a_flag.lower()== 'p':
                variance = row['base_fare'] - row['base_fare']/100 * fare_percentage
                # print str(variance), str(row['reco_fare'])
                if row['recommended_fare'] <= variance:
                    return_flag = True
            elif p_a_flag.lower()== 'a':
                variance = row['base_fare'] - fare_percentage
                if row['recommended_fare'] <= variance:
                    return_flag = True
    except Exception as error:
        print error

    return return_flag


def get_sales_fb_level(pos, origin, destination, compartment, host_fbs, dep_date_start, dep_date_end, db):
    sales_cursor = db.JUP_DB_Manual_Triggers_Module_Summary.aggregate([
        {
            '$match':
                {
                    'pos.City': pos,
                    'origin.City': origin,
                    'destination.City': destination,
                    'compartment': compartment,
                    'dep_date': {'$lte': dep_date_end, '$gte': dep_date_start}
                }
        },
        {
            '$unwind': {'path': '$farebasis'}
        },
        {
            '$match':
                {
                    'farebasis.farebasis': {'$in': host_fbs}
                }
        },
        {
            '$project':
                {
                    '_id': 0,
                    'dep_date': '$dep_date',
                    'fare_basis': '$farebasis.fare_basis',
                    'pax': '$farebasis.pax',
                    'revenue': '$farebasis.rev'
                }
        },
        {
            '$group':
                {
                    '_id':
                        {
                            'fare_basis': '$fare_basis'
                        },
                    'revenue': {'$sum': '$revenue'},
                    'pax': {'$sum': '$pax'}
                }
        },
        {
            '$project':
                {
                    '_id': 0,
                    'fare_basis': '$_id.fare_basis',
                    'fare_pax': '$pax',
                    'revenue': '$revenue'
                }
        }
    ])
    sales_fare_basis = pd.DataFrame(list(sales_cursor))
    return sales_fare_basis


@measure(JUPITER_LOGGER)
def get_pax_yield(pos, origin, destination, compartment, host_fares, dep_date_start, dep_date_end, od_distance, db):
    FZ_CURRENCY = host_fares['currency'].iloc[0]
    host_fbs_list = list(set(host_fares['fare_basis']))
    sales_fare_basis = get_sales_fb_level(pos, origin, destination, compartment, host_fbs_list, dep_date_start,
                                          dep_date_end, db=db)
    if len(sales_fare_basis) == 0:
        cols_sales = ['fare_basis', 'fare_pax', 'revenue']
        sales_fare_basis = pd.DataFrame(columns=cols_sales)
    host_fares = host_fares.merge(sales_fare_basis, on='fare_basis', how='left')
    host_fares['current_yield'] = host_fares['revenue'] / host_fares['fare_pax']
    # This part of the code is written to convert yield to AED
    # Some days later. . . This is not required as Revenue is already converted into AED and stored in collection
    # exchange_rate = {}
    # currency_crsr = list(db.JUP_DB_Exchange_Rate.find({"code": FZ_CURRENCY}))
    # for curr in currency_crsr:
    #     exchange_rate[curr['code']] = curr['Reference_Rate']
    if EXCHANGE_RATE[FZ_CURRENCY] != None:
        currency_factor = EXCHANGE_RATE[FZ_CURRENCY]
    else:
        currency_factor = 1.0
    host_fares['current_yield'] = host_fares['current_yield'] / host_fares['od_distance'] * 100
    host_fares['fare_pax'].fillna(0, inplace=True)
    host_fares['current_yield'].fillna(0, inplace=True)
    host_fares['reco_yield'] = host_fares.apply(lambda row: row['recommended_fare'] * EXCHANGE_RATE[row['currency']] / row['od_distance'] * 100, axis=1)
    return host_fares, currency_factor

# def reco_fare_check_y():


def ow_rt_fare_check(New_DF_ow, New_DF_rt, ow_rt_percentage):
    temp_df = New_DF_rt[
        ['RBD', 'currency', 'oneway_return', 'compartment', 'channel', 'fare_brand', 'recommended_fare', 'footnote']]
    fares_df_ow = New_DF_ow.merge(temp_df, on=['RBD', 'currency', 'compartment', 'channel', 'fare_brand', 'footnote'], how='left')
    fares_df_ow['recommended_fare_y'].fillna(0, inplace=True)
    fares_df_ow['ow_rt_breach'] = fares_df_ow['recommended_fare_x'] > (fares_df_ow['recommended_fare_y']/100*ow_rt_percentage)
    fares_df_ow['sellup_data'] = np.where(fares_df_ow['ow_rt_breach'] == True, fares_df_ow['sellup_data']+", OW fare is breaching "+str(ow_rt_percentage)+"% of RT fare", fares_df_ow['sellup_data'])
    fares_df_ow['ow_rt_breach_fare'] = fares_df_ow[fares_df_ow['ow_rt_breach'] == True]["recommended_fare_x"]
    # fares_df_ow['ow_rt_breach_fare_actual'] = fares_df_ow[fares_df_ow['ow_rt_breach'] == True]["recommended_fare_x"]

    fares_df_ow.loc[fares_df_ow['ow_rt_breach'] == True, 'recommended_fare_x'] = fares_df_ow[
        fares_df_ow['ow_rt_breach'] == True].apply(lambda row: (row['recommended_fare_y']/100 * ow_rt_percentage),axis=1)
    # print fares_df_ow
    #
    # # fares_df_ow['recommended_fare_x'] = fares_df_ow[fares_df_ow['ow_rt_breach'] == True]
    fares_df_ow = fares_df_ow.rename(columns={"recommended_fare_x": "recommended_fare", "oneway_return_x" : "oneway_return"})
    fares_df_ow = fares_df_ow.drop(['recommended_fare_y', 'oneway_return_y'],axis=1)
    # fares_df_ow
    # New_DF_ow, New_DF_rt, ow_rt_percentage
    # print "------------after update ---------------------"
    return fares_df_ow

def convert_tax_percentage(percentage_block, currency, fare):
    fareblock = []
    taxblock = []
    farelogic = 0
    linkcode1 = ""
    fare1 = 0
    taxonfare = 0
    farelogic = 0

    if percentage_block != 0:
        for ele in percentage_block:
            if ele['apply_on'] == 'fare':
                fareblock.append(ele)
            else:
                taxblock.append(ele)

    farefromtaxblock = 0
    fareforomfareblock = 0

    for ele in taxblock:
        print ele
        for each in ele:
            print each
            if each == "link_code":
                print ele[each]
                linkcode1 = ele[each]
                print linkcode1
                for elem in fareblock:
                    if linkcode1 == elem['code']:
                        print "tantanatan"
                        Tfare = elem['value'] * fare / 100
                        farefromtaxblock = farefromtaxblock + ele['value'] * Tfare / 100
                        # fareblock.remove(elem)

    for ele in fareblock:
        print fareblock
        if not (ele['threshold_value']):
            fare1 = ele['value'] * fare / 100
            fareforomfareblock += fare1
        else:
            taxonfare = ele['value'] * fare / 100
            farelogic = min(convert_tax_currency(ele['threshold_value'], currency, ele['threshold_currency']),
                            convert_tax_currency(taxonfare, currency,
                                                 ele['threshold_currency']))
            fareforomfareblock += farelogic
    percenttax = farefromtaxblock + fareforomfareblock
    return percenttax


fare_brand_formula = {}

def comp_match(pos, origin, destination, compartment,dep_date_start, dep_date_end, db, dates_code=None, mpf_df=None,
         host_fares=None):
    Fares = list()
    ## For choosing list of Farebrand and channel for based on compartment
    if compartment == "Y":
        list_fare_brand_overall = list_fare_brand_overall_y
        channel_fb = channel_fb_y
    else:
        list_fare_brand_overall = list_fare_brand_overall_j
        channel_fb = channel_fb_j
    New_DF_ow = pd.DataFrame()
    pricing_models = get_pricing_models(pos, origin, destination, compartment, db=db)
    # print pricing_models
    if len(list(pricing_models)) > 0:

        # FCR_creation = pricing_models[0]['FCR']
        # Add_ons = pricing_models[0]['Add-On']

        # currency_crsr = list(client[JUPITER_DB].JUP_DB_Fare_brand.find({"code": "farebrand_creation"}))[0]
        # fare_brand_formula["TA Lite"] = currency_crsr['desc']['TA Lite']
        # fare_brand_formula["web Value"] = currency_crsr['desc']['Web Value']
        # fare_brand_formula["TA Value"] = currency_crsr['desc']['TA Value']
        # fare_brand_formula["gds Value"] = currency_crsr['desc']['GDS Value']
        # fare_brand_formula["TA FLY+Visa"] = currency_crsr['desc']['TA FLY+Visa']
        # fare_brand_formula["web Flex"] = currency_crsr['desc']['Web Flex']
        # fare_brand_formula["TA Flex"] = currency_crsr['desc']['TA Flex']
        # fare_brand_formula["gds GDS Flex"] = currency_crsr['desc']['GDS Flex']

        for model_definition in pricing_models[0]['model']:
            if model_definition['eff_date_from'] <= SYSTEM_DATE and model_definition['eff_date_to'] >= SYSTEM_DATE:
                fare_brand_formulas, RBD_sellup, FCR_currency, FCR_fare_value, fare_brand_currency = get_sellup_doc(model_definition,compartment, db)
                ## For now unwanted
                unwanted_num = {"srt", "end"}
                ## Farebrand formula will vary based on compartment of what we give
                if compartment == "Y":
                    try:
                        fare_brand_formula["TA Lite"] = [ele for ele in fare_brand_formulas['TA Lite'] if ele not in unwanted_num]
                    except Exception:
                        fare_brand_formula["TA Lite"] = fare_brand_formulas['TA Lite']
                    try:
                        fare_brand_formula["Web Value"] = [ele for ele in fare_brand_formulas['Web Value'] if ele not in unwanted_num]
                    except Exception:
                        fare_brand_formula["Web Value"] = fare_brand_formulas['Web Value']
                    try:
                        fare_brand_formula["TA Value"] = [ele for ele in fare_brand_formulas['TA Value'] if ele not in unwanted_num]
                    except Exception:
                        fare_brand_formula["TA Value"] = fare_brand_formulas['TA Value']
                    try:
                        fare_brand_formula["GDS Value"] = [ele for ele in fare_brand_formulas['GDS Value'] if ele not in unwanted_num]
                    except Exception:
                        fare_brand_formula["GDS Value"] = fare_brand_formulas['GDS Value']
                    try:
                        fare_brand_formula["TA FLY+Visa"] = [ele for ele in fare_brand_formulas['TA FLY+Visa'] if ele not in unwanted_num]
                    except Exception:
                        fare_brand_formula["TA FLY+Visa"] = fare_brand_formulas['TA FLY+Visa']
                    try:
                        fare_brand_formula["Web Flex"] = [ele for ele in fare_brand_formulas['Web Flex'] if ele not in unwanted_num]
                    except Exception:
                        fare_brand_formula["Web Flex"] = fare_brand_formulas['Web Flex']
                    try:
                        fare_brand_formula["TA Flex"] = [ele for ele in fare_brand_formulas['TA Flex'] if ele not in unwanted_num]
                    except Exception:
                        fare_brand_formula["TA Flex"] = fare_brand_formulas['TA Flex']
                    try:
                        fare_brand_formula["GDS GDS Flex"] = [ele for ele in fare_brand_formulas['GDS Flex'] if ele not in unwanted_num]
                    except Exception:
                        fare_brand_formula["GDS GDS Flex"] = fare_brand_formulas['GDS Flex']
                else:
                    try:
                        fare_brand_formula["GDS Business"] = fare_brand_formulas['GDS Business'].remove("srt").remove("end")
                    except Exception:
                        fare_brand_formula["GDS Business"] = fare_brand_formulas['GDS Business']
                # print RBD_sellup
                # '''
                ## For competitor match model
                # if 'competitor_match' in model_definition['model_name']:
                if model_definition['model_name']:

                    ## Reading configuration
                    primary_criteria = model_definition['primary_criteria']
                    general_rules = model_definition['general_rules']
                    secondary_criteria = model_definition['secondary_criteria']['filter']
                    if 'competitor_match' in model_definition:
                        competitor_match = model_definition['competitor_match']
                    else:
                        competitor_match = []
                    # base_fare_brand = primary_criteria['filter']['base_fare_brand']
                    # base_fare_channel = primary_criteria['filter']['base_channel']
                    try:
                        base_baggage = model_definition['baggage']
                        base_fare_brand = model_definition['base_brand']

                    except KeyError:
                        print "No baggage definition"
                        base_fare_brand = "Lite"
                        base_baggage = "B"
                    # print secondary_criteria
                    # print primary_criteria
                    # print competitor_match
                    if primary_criteria['filter']['base_fare_based'] == "MPF" and primary_criteria['filter']['fare_feed'] == "Infare":

                        if mpf_df is not None:
                            mpf_df = get_popular_fare(pos, origin, destination, compartment, dep_date_start, dep_date_end,
                                                      db)
                            # print mpf_df
                        else:
                            mpf_df = get_popular_fare(pos, origin, destination, compartment, dep_date_start, dep_date_end, db)
                        print "mpf_df"
                        print mpf_df
                        currency_doc = list(db.JUP_DB_Pos_Currency_Master.find({'pos': pos}, {'_id': 0, 'web': 1, 'gds' : 1}))
                        delta_currency = currency_doc[0]['web']
                        delta_currency_web = currency_doc[0]['web']
                        delta_currency_gds = currency_doc[0]['gds']
                        if delta_currency_gds == "":
                            delta_currency_gds = delta_currency_web
                        else:
                            pass

                        mpf_df['currency'] = delta_currency
                        mpf_df['rt_channel'] = "NA"
                        mpf_df['ow_channel'] = "NA"
                        mpf_df['rt_fare_brand'] = "NA"
                        mpf_df['ow_fare_brand'] = "NA"
                        mpf_df['rt_differential'] = 0
                        mpf_df['ow_differential'] = 0
                        print mpf_df
                        print competitor_match

                        ow_No_reco_needed = False
                        rt_No_reco_needed = False
                        competitor_match_ = list()
                        # control check with base_fare_brand
                        for check_each in competitor_match:
                            try:
                                if base_fare_brand in check_each['ow']['fare_brand']:
                                    competitor_match_.append(check_each)
                            except KeyError:
                                pass

                            try:
                                if base_fare_brand in check_each['rt']['fare_brand']:
                                    competitor_match_.append(check_each)
                            except KeyError:
                                pass

                        print competitor_match_

                        for each_carrier in competitor_match_:
                            carrier = each_carrier['carrier']
                            try:
                                ow_rec = each_carrier['ow']
                            except KeyError:
                                ow_rec = None

                            try:
                                rt_rec = each_carrier['rt']
                            except KeyError:
                                rt_rec = None
                            ## For OW differentials
                            if ow_rec != None:
                                if ow_rec['differential_type'] == "P":
                                    ow_rec_fare = mpf_df[mpf_df['carrier']== carrier]['most_avail_fare_total_ow'] * (1 + ow_rec['differential'] / 100.0)
                                    mpf_df.loc[mpf_df['carrier'] == carrier, "ow_reco_fare"] = ow_rec_fare
                                    mpf_df.loc[mpf_df['carrier'] == carrier, "ow_channel"] = ow_rec['channel']
                                    mpf_df.loc[mpf_df['carrier'] == carrier, "ow_fare_brand"] = ow_rec['fare_brand']
                                    mpf_df.loc[mpf_df['carrier'] == carrier, "ow_differential"] = ow_rec['differential'] * EXCHANGE_RATE[ow_rec['currency']] / EXCHANGE_RATE[delta_currency]


                                elif ow_rec['differential_type'] == "A":
                                    if "ow_reco_fare" in mpf_df:
                                        # try:
                                        existing_fare = mpf_df[mpf_df['carrier'] == carrier]['ow_reco_fare']
                                        # except Exception:
                                        #     existing_fare = 0
                                        # print existing_fare
                                        new_fare = mpf_df[mpf_df['carrier'] == carrier]['most_avail_fare_total_ow'] + ow_rec['differential'] * EXCHANGE_RATE[ow_rec['currency']] / EXCHANGE_RATE[delta_currency]
                                        if new_fare.empty:
                                            new_fare = 0
                                        if existing_fare.empty:
                                            existing_fare = 0
                                        # print new_fare
                                        if math.isnan(existing_fare):
                                            ow_reco_fare = mpf_df[mpf_df['carrier'] == carrier]['most_avail_fare_total_ow'] + ow_rec['differential'] * EXCHANGE_RATE[ow_rec['currency']] / EXCHANGE_RATE[delta_currency]
                                            mpf_df.loc[mpf_df['carrier'] == carrier, "ow_reco_fare"] = ow_reco_fare
                                            mpf_df.loc[mpf_df['carrier'] == carrier, "ow_channel"] = ow_rec['channel']
                                            mpf_df.loc[mpf_df['carrier'] == carrier, "ow_fare_brand"] = ow_rec['fare_brand']
                                            mpf_df.loc[mpf_df['carrier'] == carrier, "ow_differential"] = ow_rec['differential'] * EXCHANGE_RATE[ow_rec['currency']] / EXCHANGE_RATE[delta_currency]

                                        elif int(existing_fare) > int(new_fare):
                                            ow_reco_fare = mpf_df[mpf_df['carrier'] == carrier][
                                                               'most_avail_fare_total_ow'] + ow_rec['differential'] * \
                                                           EXCHANGE_RATE[ow_rec['currency']] / EXCHANGE_RATE[
                                                               delta_currency]
                                            mpf_df.loc[mpf_df['carrier'] == carrier, "ow_reco_fare"] = ow_reco_fare
                                            mpf_df.loc[mpf_df['carrier'] == carrier, "ow_channel"] = ow_rec['channel']
                                            mpf_df.loc[mpf_df['carrier'] == carrier, "ow_fare_brand"] = ow_rec[
                                                'fare_brand']
                                            mpf_df.loc[mpf_df['carrier'] == carrier, "ow_differential"] = ow_rec['differential'] * EXCHANGE_RATE[ow_rec['currency']] / EXCHANGE_RATE[delta_currency]

                                    else:
                                        ow_reco_fare = mpf_df[mpf_df['carrier'] == carrier]['most_avail_fare_total_ow'] + ow_rec['differential'] * EXCHANGE_RATE[ow_rec['currency']] / EXCHANGE_RATE[delta_currency]
                                        mpf_df.loc[mpf_df['carrier'] == carrier, "ow_reco_fare"] = ow_reco_fare
                                        mpf_df.loc[mpf_df['carrier'] == carrier, "ow_channel"] = ow_rec['channel']
                                        mpf_df.loc[mpf_df['carrier'] == carrier, "ow_fare_brand"] = ow_rec['fare_brand']
                                        mpf_df.loc[mpf_df['carrier'] == carrier, "ow_differential"] = ow_rec['differential'] * EXCHANGE_RATE[ow_rec['currency']] / EXCHANGE_RATE[delta_currency]
                            else:
                                ow_No_reco_needed = True

                            ## For RT differentials
                            if rt_rec != None:
                                if rt_rec['differential_type'] == "P":
                                    rt_rec_fare = mpf_df[mpf_df['carrier'] == carrier][
                                                      'most_avail_fare_total_rt'] * (
                                                              1 + rt_rec['differential'] / 100.0)
                                    mpf_df.loc[mpf_df['carrier'] == carrier, "rt_reco_fare"] = rt_rec_fare
                                    mpf_df.loc[mpf_df['carrier'] == carrier, "rt_channel"] = rt_rec['channel']
                                    mpf_df.loc[mpf_df['carrier'] == carrier, "rt_fare_brand"] = rt_rec['fare_brand']
                                    mpf_df.loc[mpf_df['carrier'] == carrier, "rt_differential"] = rt_rec['differential'] * EXCHANGE_RATE[rt_rec['currency']] / \
                                                          EXCHANGE_RATE[delta_currency]

                                elif rt_rec['differential_type'] == "A":
                                    if "rt_reco_fare" in mpf_df:
                                        existing_fare = mpf_df[mpf_df['carrier'] == carrier]['rt_reco_fare']
                                        new_fare = mpf_df[mpf_df['carrier'] == carrier]['most_avail_fare_total_rt'] + \
                                                          rt_rec['differential'] * EXCHANGE_RATE[rt_rec['currency']] / \
                                                          EXCHANGE_RATE[delta_currency]
                                        # print int(existing_fare)
                                        if new_fare.empty:
                                            new_fare = 0
                                        if existing_fare.empty:
                                            existing_fare = 0
                                        if math.isnan(existing_fare):
                                            rt_rec_fare = mpf_df[mpf_df['carrier'] == carrier]['most_avail_fare_total_rt'] + \
                                                          rt_rec['differential'] * EXCHANGE_RATE[rt_rec['currency']] / \
                                                          EXCHANGE_RATE[delta_currency]
                                            mpf_df.loc[mpf_df['carrier'] == carrier, "rt_reco_fare"] = rt_rec_fare
                                            mpf_df.loc[mpf_df['carrier'] == carrier, "rt_channel"] = rt_rec['channel']
                                            mpf_df.loc[mpf_df['carrier'] == carrier, "rt_fare_brand"] = rt_rec['fare_brand']
                                            mpf_df.loc[mpf_df['carrier'] == carrier, "rt_differential"] = rt_rec['differential'] * EXCHANGE_RATE[rt_rec['currency']] / \
                                                      EXCHANGE_RATE[delta_currency]
                                        elif int(existing_fare) > int(new_fare):
                                            rt_rec_fare = mpf_df[mpf_df['carrier'] == carrier][
                                                              'most_avail_fare_total_rt'] + \
                                                          rt_rec['differential'] * EXCHANGE_RATE[rt_rec['currency']] / \
                                                          EXCHANGE_RATE[delta_currency]
                                            mpf_df.loc[mpf_df['carrier'] == carrier, "rt_reco_fare"] = rt_rec_fare
                                            mpf_df.loc[mpf_df['carrier'] == carrier, "rt_channel"] = rt_rec['channel']
                                            mpf_df.loc[mpf_df['carrier'] == carrier, "rt_fare_brand"] = rt_rec[
                                                'fare_brand']
                                            mpf_df.loc[mpf_df['carrier'] == carrier, "rt_differential"] = rt_rec['differential'] * EXCHANGE_RATE[rt_rec['currency']] / \
                                                      EXCHANGE_RATE[delta_currency]
                                    else:
                                        rt_rec_fare = mpf_df[mpf_df['carrier'] == carrier]['most_avail_fare_total_rt'] + \
                                                      rt_rec['differential'] * EXCHANGE_RATE[rt_rec['currency']] / \
                                                      EXCHANGE_RATE[delta_currency]
                                        mpf_df.loc[mpf_df['carrier'] == carrier, "rt_reco_fare"] = rt_rec_fare
                                        mpf_df.loc[mpf_df['carrier'] == carrier, "rt_channel"] = rt_rec['channel']
                                        mpf_df.loc[mpf_df['carrier'] == carrier, "rt_fare_brand"] = rt_rec['fare_brand']
                                        mpf_df.loc[mpf_df['carrier'] == carrier, "rt_differential"] = rt_rec['differential'] * EXCHANGE_RATE[rt_rec['currency']] / \
                                                      EXCHANGE_RATE[delta_currency]
                            else:
                                rt_No_reco_needed = True

                                # print(ow_rec_fare)
                                # print(mpf_df)
                            # print rt_rec, ow_rec, carrier
                            # mpf_df.loc[df['carrier'] == competitor_match[key]['carrier'] and df['carrier'] == competitor_match[key]['carrier']]
                        # competitor_match_df = cursor_to_df(competitor_match)
                        print mpf_df
                        print("-------------------Popular fare data frame-------------------")

                        # mpf_df['rt_channel'] = mpf_df['rt_channel'].fillna()
                        # mpf_df['rt_channel'] = mpf_df['rt_channel'].fillna('NA', inplace=True)
                        # mpf_df['ow_channel'] = mpf_df['ow_channel'].fillna('NA', inplace=True)
                        # mpf_df['rt_fare_brand'] = mpf_df['rt_fare_brand'].fillna('NA', inplace=True)
                        # mpf_df['ow_fare_brand'] = mpf_df['ow_fare_brand'].fillna('NA', inplace=True)
                        # print mpf_df
                        # print asc
                        # print("-------------------Recommendable popular fare with Tax----------------")

                        ## If there no ow/rt competitor match records found in Pricing model configuration\
                        # then take the lowest RBS's min fare as recommended fare
                        if len(competitor_match_) == 0:
                            rt_No_reco_needed = True
                            ow_No_reco_needed = True
                        elif len(mpf_df[mpf_df['rt_channel'] != "NA"]) > 0:
                            if len(mpf_df[mpf_df['ow_channel'] != "NA"]) > 0:
                                pass
                            else:
                                ow_No_reco_needed = True

                        elif len(mpf_df[mpf_df['ow_channel'] != "NA"]) > 0:
                            if len(mpf_df[mpf_df['rt_channel'] != "NA"]) > 0:
                                pass
                            else:
                                rt_No_reco_needed = True

                        ow_fare_for_no_reco = list()
                        rt_fare_for_no_reco = list()
                        for key in RBD_sellup.keys():
                            try:
                                if RBD_sellup[key]['compartment'] == compartment:
                                    print RBD_sellup[key]['ow']['minimum']
                                    if RBD_sellup[key]['ow']['minimum'] != None:
                                    # if RBD_sellup[key]['ow']['minimum'] != None and int(RBD_sellup[key]['ow']['minimum']) != 0:
                                        ow_fare_for_no_reco.append(int(
                                        convert_tax_currency(RBD_sellup[key]['ow']['minimum'], delta_currency,
                                                             FCR_currency)
                                        ))
                                    if RBD_sellup[key]['rt']['minimum'] != None:
                                    # if RBD_sellup[key]['rt']['minimum'] != None and int(RBD_sellup[key]['rt']['minimum']) != 0:
                                        rt_fare_for_no_reco.append(int(
                                            convert_tax_currency(RBD_sellup[key]['rt']['minimum'], delta_currency,
                                                                 FCR_currency)
                                        ))
                            except TypeError:
                                pass

                        rt_fare_for_no_reco = sorted(rt_fare_for_no_reco)[0]
                        ow_fare_for_no_reco = sorted(ow_fare_for_no_reco)[0]
                        ow_sellup_builder = ""
                        rt_sellup_builder = ""
                        # print rt_fare_for_no_reco
                        if ow_No_reco_needed == False:
                            mpf_recomended_fare_ow, ow_channel, ow_fare_brand, ow_carrier, ow_differential = get_lowest_mpf_reco(mpf_df, "ow")
                            if mpf_recomended_fare_ow != 0 and math.isnan(mpf_recomended_fare_ow) != True:
                                ow_sellup_builder = "Recommendation for Most Popular Web Fare based on the Competitor Match. " \
                                                    "Competitor = {0} Delta = {1} Competitor MPF inclusive of TAX = {2}".format(ow_carrier, ow_differential, mpf_recomended_fare_ow)
                            else:
                                ow_sellup_builder = "Recommendation is based on FCR logic since there is no MPF"
                                ow_No_reco_needed == True

                            if math.isnan(mpf_recomended_fare_ow) == True:
                                if compartment == "Y":
                                    mpf_recomended_fare_ow, ow_channel, ow_fare_brand = ow_fare_for_no_reco, "Web", base_fare_brand
                                else:
                                    mpf_recomended_fare_ow, ow_channel, ow_fare_brand = ow_fare_for_no_reco, "Web", base_fare_brand
                            else:
                                pass
                        else:
                            if compartment == "Y":
                                mpf_recomended_fare_ow, ow_channel, ow_fare_brand = ow_fare_for_no_reco, "Web", base_fare_brand
                            else:
                                mpf_recomended_fare_ow, ow_channel, ow_fare_brand = ow_fare_for_no_reco, "Web", base_fare_brand

                        if rt_No_reco_needed == False:
                            mpf_recomended_fare_rt, rt_channel, rt_fare_brand, rt_carrier, rt_differential = get_lowest_mpf_reco(mpf_df, "rt")
                            if mpf_recomended_fare_rt != 0 and math.isnan(mpf_recomended_fare_rt) != True:
                                rt_sellup_builder = "Recommendation for Most Popular Web Fare based on the Competitor Match. " \
                                                    "Competitor = {0} Delta = {1} Competitor MPF inclusive of TAX = {2}".format(
                                    rt_carrier, rt_differential, mpf_recomended_fare_rt)
                            else:
                                rt_sellup_builder = ow_sellup_builder = "Recommendation is based on FCR logic since there is no MPF"
                                rt_No_reco_needed = True
                            if math.isnan(mpf_recomended_fare_rt) == True:
                                if compartment == "Y":
                                    mpf_recomended_fare_rt, rt_channel, rt_fare_brand = rt_fare_for_no_reco, "Web", base_fare_brand
                                else:
                                    mpf_recomended_fare_rt, rt_channel, rt_fare_brand = rt_fare_for_no_reco, "Web", base_fare_brand
                            else:
                                pass
                        else:
                            if compartment == "Y":
                                mpf_recomended_fare_rt, rt_channel, rt_fare_brand = rt_fare_for_no_reco, "Web", base_fare_brand
                            else:
                                mpf_recomended_fare_rt, rt_channel, rt_fare_brand = rt_fare_for_no_reco, "Web", base_fare_brand

                        print("OW_RT = OW  Fare = {0} Channel = {1} Farebrand = {2}".format(mpf_recomended_fare_ow,
                                                                                            ow_channel, ow_fare_brand))
                        print("OW_RT = RT  Fare = {0} Channel = {1} Farebrand = {2}".format(mpf_recomended_fare_rt, rt_channel, rt_fare_brand))
                        #
                        # print("-------------------Recommendable popular fare without Tax----------------")
                        # print("Reco fare, Tax, Updated Fare")
                        ## Remove tax from total fare
                        tax_ow = 0
                        tax_rt = 0
                        tax = list(db.JUP_DB_Tax_Master.find({
                            "Origin": origin,
                            "Destination": destination,
                            "Compartment": compartment,
                        }, {"OW_RT": 1, "Currency": 1, "Percent_tax": 1, "Fixed_tax": 1}))
                        # print tax
                        tax_ow_doc = filter(lambda x: x["OW_RT"] == "1", tax)
                        tax_rt_doc = filter(lambda x: x["OW_RT"] == "2", tax)
                        if tax_ow_doc != [] and mpf_recomended_fare_ow != 0:
                            try:
                                ow_tax_curr = tax_ow_doc[0]['Currency']
                                if 'percentage_block' in tax_rt_doc[0]:
                                    ow_tax_per = tax_ow_doc[0]['percentage_block']
                                else:
                                    ow_tax_per = 0

                                ow_tax_fix = tax_ow_doc[0]['Fixed_tax']
                            except Exception:
                                ow_tax_curr = "AED"
                                ow_tax_per = 0
                                ow_tax_fix = 0

                            tax_ow = convert_tax_currency(ow_tax_fix, delta_currency , ow_tax_curr) + convert_tax_percentage(ow_tax_per,
                                                                             ow_tax_curr, mpf_recomended_fare_ow)

                            mpf_recomended_fare_ow_update = mpf_recomended_fare_ow - tax_ow
                            # print 'tax of ow', tax_ow
                            print(mpf_recomended_fare_ow, tax_ow, mpf_recomended_fare_ow_update)
                        else:
                            mpf_recomended_fare_ow_update = mpf_recomended_fare_ow
                            try:
                                tax_ow = tax_ow_doc[0]['Fixed_tax']
                            except:
                                tax_ow = 0

                        if tax_rt_doc != [] and mpf_recomended_fare_rt != 0:
                            try:
                                rt_tax_curr = tax_rt_doc[0]['Currency']
                                if 'percentage_block' in tax_rt_doc[0]:
                                    rt_tax_per = tax_rt_doc[0]['percentage_block']
                                else:
                                    rt_tax_per = 0

                                rt_tax_fix = tax_rt_doc[0]['Fixed_tax']
                            except Exception:
                                rt_tax_curr = "AED"
                                rt_tax_per = 0
                                rt_tax_fix = 0
                            tax_rt = convert_tax_currency(rt_tax_fix, delta_currency , rt_tax_curr) + convert_tax_percentage(rt_tax_per,
                                                                             rt_tax_curr, mpf_recomended_fare_rt)
                            # print 'tax of rt', tax_rt
                            mpf_recomended_fare_rt_update = mpf_recomended_fare_rt - tax_rt
                            print(mpf_recomended_fare_rt, tax_rt, mpf_recomended_fare_rt_update)
                        else:
                            mpf_recomended_fare_rt_update = mpf_recomended_fare_rt
                            try:
                                tax_rt = tax_rt_doc[0]['Fixed_tax']
                            except:
                                tax_rt = 0
                        # ## Keep the control check of negative reco fare
                        # if mpf_recomended_fare_ow_update < 0:
                        #     mpf_recomended_fare_ow_update = 1
                        # if mpf_recomended_fare_rt_update < 0:
                        #     mpf_recomended_fare_rt_update = 1


                        ## get fee details
                        ## get_the_list of fee parameter
                        print "tax_rt"
                        print tax_rt
                        print tax_ow
                        # print asc
                        add_od_doc = db.JUP_DB_Fare_brand.find_one({"compartment" : compartment})
                        try:
                            add_od_doc = add_od_doc['Add-on']
                        except KeyError as error:
                            add_od_doc = []



                        host_fares = get_host_fares_df(pos=pos,
                                                       origin=origin,
                                                       destination=destination,
                                                       compartment=compartment,
                                                       extreme_start_date=dep_date_start,
                                                       extreme_end_date=dep_date_end, db=db)

                        # Give recommendation to active fares only
                        host_fares_not_expired = host_fares[host_fares['is_expired'] == 0]
                        host_fares_expired = host_fares[host_fares['is_expired'] == 1]
                        # print tax_ow
                        # print tax_rt
                        # host_fares_not_expired['taxes'] = np.where(host_fares_not_expired['oneway_return'] == "1",
                        #                                      tax_ow, tax_rt)
                        host_fares_not_expired.loc[
                            host_fares_not_expired['oneway_return'] == "1", 'taxes'] = tax_ow
                        host_fares_not_expired.loc[
                            host_fares_not_expired['oneway_return'] == "2", 'taxes'] = tax_rt

                        # Update FCR Sellup to existing fares for base fare brand
                        print ow_channel, ow_fare_brand
                        print rt_channel, rt_fare_brand

                        if ow_channel.lower() == "gds" and (ow_fare_brand.lower() == "value" or ow_fare_brand == "BUSINESS" ):

                            add_on_ow, RBD_min_fare_ow, RBD_actual_range_fares_ow, RBD_wise_max_fare_ow = get_add_on_doc(host_fares_not_expired[(host_fares_not_expired.oneway_return == "1") &
                                                            (host_fares_not_expired.channel == ow_channel.lower().replace("gds", "gds").replace("web", "web").replace("ta", "TA")) &
                                                            (host_fares_not_expired.fare_brand == "")],
                                                                        base_baggage, add_od_doc, RBD_sellup,
                                                                        mpf_recomended_fare_ow_update, delta_currency,
                                                                        FCR_currency, "ow", compartment, FCR_fare_value, fare_brand_currency)
                        else:

                            add_on_ow, RBD_min_fare_ow, RBD_actual_range_fares_ow, RBD_wise_max_fare_ow = get_add_on_doc(host_fares_not_expired[(host_fares_not_expired.oneway_return == "1") &
                                                                                               (host_fares_not_expired.channel == ow_channel.lower().replace("gds", "gds").replace("web", "web").replace("ta", "TA")) &
                                                                                               (host_fares_not_expired.fare_brand == ow_fare_brand.upper())],
                                                                        base_baggage, add_od_doc, RBD_sellup,
                                                                        mpf_recomended_fare_ow_update, delta_currency,
                                                                        FCR_currency, "ow", compartment, FCR_fare_value, fare_brand_currency)

                        print "check from here"
                        if rt_channel.lower() == "gds" and (rt_fare_brand.lower() == "value" or rt_fare_brand == "BUSINESS"):

                            add_on_rt, RBD_min_fare_rt, RBD_actual_range_fares_rt, RBD_wise_max_fare_rt = get_add_on_doc(host_fares_not_expired[(host_fares_not_expired.oneway_return == "2") &
                                                                                               (host_fares_not_expired.channel == rt_channel.lower().replace("gds", "gds").replace("web", "web").replace("ta", "TA")) &
                                                                                               (host_fares_not_expired.fare_brand == "")],
                                                                        base_baggage, add_od_doc, RBD_sellup,
                                                                        mpf_recomended_fare_rt_update, delta_currency,
                                                                        FCR_currency, "rt", compartment, FCR_fare_value, fare_brand_currency)
                        else:
                            add_on_rt, RBD_min_fare_rt, RBD_actual_range_fares_rt, RBD_wise_max_fare_rt = get_add_on_doc(host_fares_not_expired[(host_fares_not_expired.oneway_return == "2") &
                                                                                               (host_fares_not_expired.channel == rt_channel.lower().replace("gds", "gds").replace("web", "web").replace("ta", "TA")) &
                                                                                               (host_fares_not_expired.fare_brand == rt_fare_brand.upper())],
                                                                        base_baggage, add_od_doc, RBD_sellup,
                                                                        mpf_recomended_fare_rt_update, delta_currency,
                                                                        FCR_currency, "rt", compartment, FCR_fare_value, fare_brand_currency)

                        # print asd
                        base_RBD_ow = ""
                        base_RBD_rt = ""
                        last_updated_fare = 0
                        last_updated_fare_RBD = ""
                        # print RBD_wise_max_fare_ow
                        # print asc

                        for i in sorted(RBD_actual_range_fares_ow.items(), key = lambda kv:(kv[1], kv[0])):
                            if mpf_recomended_fare_ow_update < i[1] and last_updated_fare < mpf_recomended_fare_ow_update:
                                base_RBD_ow = last_updated_fare_RBD
                                break
                            elif mpf_recomended_fare_ow_update <= 0 and i[1] != 0:
                                base_RBD_ow = i[0]
                                mpf_recomended_fare_ow_update = i[1]
                                break
                            else:
                                last_updated_fare_RBD = i[0]
                                last_updated_fare = i[1]

                        last_updated_fare = 0
                        last_updated_fare_RBD = ""
                        # print "Printing Low RBD"
                        # print "RT MPF"
                        # print mpf_recomended_fare_rt_update

                        for i in sorted(RBD_actual_range_fares_rt.items(), key=lambda kv: (kv[1], kv[0])):
                            # print i
                            if mpf_recomended_fare_rt_update < i[1] and last_updated_fare < mpf_recomended_fare_rt_update:
                                base_RBD_rt = last_updated_fare_RBD
                                break
                            elif mpf_recomended_fare_rt_update <= 0 and i[1] != 0:
                                base_RBD_rt = i[0]
                                mpf_recomended_fare_rt_update = i[1]
                                break
                            else:
                                last_updated_fare_RBD = i[0]
                                last_updated_fare = i[1]


                        # print base_RBD_rt, base_RBD_ow
                        first_hit = False
                        fare_brand_value_ow = {}
                        sellup_for_fare_brand_ow = {}
                        sellup_for_fare_brand_rt = {}
                        fare_brand_value_rt = {}
                        # print rt_channel, rt_fare_brand,
                        print RBD_min_fare_ow
                        # print asd
                        hit_flag = False
                        print "base_RBD_ow"
                        print base_RBD_ow
                        print base_RBD_rt
                        ## Only for base fare we need to consider exact other wise need to consider FCR sellup fares
                        for i in sorted(RBD_min_fare_ow.items(), key=lambda kv: (kv[1], kv[0])):
                            # if

                            if i[0] == base_RBD_ow:
                                print "inside main loop"
                                print i[0]
                                first_hit = True
                                # base OW fare recommendation should be 65% lesser then RT
                                if 'ow_rt' in general_rules:
                                    if general_rules['ow_rt']["active"] == True:
                                        ## For percentage
                                        # New_DF_ow['yield_breach'] = False
                                        # New_DF_rt['yield_breach'] = False
                                        if general_rules['ow_rt']["type"] == "P":
                                            ow_rt_percentage = general_rules['ow_rt']["value"]
                                            if i[0] == base_RBD_rt:
                                                hit_flag = True
                                            if (mpf_recomended_fare_ow_update > (RBD_min_fare_rt[i[0]] / 100 * ow_rt_percentage)) and (hit_flag == True):
                                                print i[0]
                                                print "inside basefare"
                                                hit_flag = True
                                                mpf_recomended_fare_ow_update = (RBD_min_fare_rt[i[0]] / 100 * ow_rt_percentage)

                                                fare_brand_value_ow[i[0]], sellup_for_fare_brand_ow[
                                                    i[0]] = fare_brand_creation(add_on_ow[i[0]]['ow'],
                                                                                mpf_recomended_fare_ow_update,
                                                                                ow_channel,
                                                                                ow_fare_brand,
                                                                                delta_currency, compartment, channel_fb, "")
                                                RBD_wise_max_fare_ow[i[0]] = mpf_recomended_fare_ow_update
                                                sellup_for_fare_brand_ow[i[0]][ow_channel + " " + ow_fare_brand] = \
                                                    sellup_for_fare_brand_ow[i[0]][
                                                        ow_channel + " " + ow_fare_brand] + "" + "OW fare is breaching " + str(
                                                        ow_rt_percentage) + "% of RT fare"

                                            else:
                                                fare_brand_value_ow[i[0]], sellup_for_fare_brand_ow[
                                                    i[0]] = fare_brand_creation(add_on_ow[i[0]]['ow'],
                                                                                mpf_recomended_fare_ow_update,
                                                                                ow_channel,
                                                                                ow_fare_brand,
                                                                                delta_currency, compartment, channel_fb, "")

                                else:
                                    fare_brand_value_ow[i[0]], sellup_for_fare_brand_ow[i[0]]  = fare_brand_creation(add_on_ow[i[0]]['ow'],
                                                                              mpf_recomended_fare_ow_update, ow_channel,
                                                                              ow_fare_brand,
                                                                              delta_currency, compartment, channel_fb, "")
                            elif first_hit == True:
                                print "else loop"
                                # base OW fare recommendation should be 65% lesser then RT
                                if 'ow_rt' in general_rules:
                                    if general_rules['ow_rt']["active"] == True:
                                        ## For percentage
                                        # New_DF_ow['yield_breach'] = False
                                        # New_DF_rt['yield_breach'] = False
                                        if general_rules['ow_rt']["type"] == "P":
                                            ow_rt_percentage = general_rules['ow_rt']["value"]

                                            if i[0] == base_RBD_rt:
                                                hit_flag = True
                                            if (i[1] > (RBD_min_fare_rt[i[0]] / 100 * ow_rt_percentage)) and (hit_flag == True):

                                                ff = (RBD_min_fare_rt[i[0]]/ 100 * ow_rt_percentage)

                                                fare_brand_value_ow[i[0]], sellup_for_fare_brand_ow[
                                                    i[0]] = fare_brand_creation(add_on_ow[i[0]]['ow'],
                                                                                ff, ow_channel,
                                                                                ow_fare_brand,
                                                                                delta_currency, compartment, channel_fb, "")
                                                RBD_wise_max_fare_ow[i[0]] = ff
                                                sellup_for_fare_brand_ow[i[0]][ow_channel + " " + ow_fare_brand] = \
                                                sellup_for_fare_brand_ow[i[0]][
                                                    ow_channel + " " + ow_fare_brand] + "" + "OW fare is breaching " + str(
                                                    ow_rt_percentage) + "% of RT fare"
                                                # print i[0]
                                                # print fare_brand_value_ow[i[0]]
                                                # print sellup_for_fare_brand_ow[
                                                #     i[0]]
                                                # print asc
                                                # print i[0]
                                                # print i[1]
                                                # print RBD_min_fare_rt[i[0]]
                                                # print hit_flag
                                            else:
                                                # print i[0]
                                                # print i[1]
                                                # print RBD_min_fare_rt[i[0]]
                                                # print hit_flag
                                                fare_brand_value_ow[i[0]], sellup_for_fare_brand_ow[
                                                    i[0]] = fare_brand_creation(add_on_ow[i[0]]['ow'],
                                                                                i[1], ow_channel,
                                                                                ow_fare_brand,
                                                                                delta_currency, compartment, channel_fb, "")

                                else:
                                    fare_brand_value_ow[i[0]], sellup_for_fare_brand_ow[i[0]] = fare_brand_creation(
                                        add_on_ow[i[0]]['ow'],
                                        i[1], ow_channel,
                                        ow_fare_brand,
                                        delta_currency, compartment, channel_fb, "")

                            else:
                                fare_brand_value_ow[i[0]], sellup_for_fare_brand_ow[i[0]] = fare_brand_creation(
                                    add_on_ow[i[0]]['ow'],
                                    i[1], ow_channel,
                                    ow_fare_brand,
                                    delta_currency, compartment, channel_fb, "")
                                if i[0] == base_RBD_rt:
                                    hit_flag = True

                        # """
                        first_hit = False
                        # print asc
                        # Only for base fare we need to consider exact other wise need to consider FCR sell-up fares
                        for i in sorted(RBD_min_fare_rt.items(), key=lambda kv: (kv[1], kv[0])):
                            # print i
                            if i[0] == base_RBD_rt:
                                first_hit = True
                                fare_brand_value_rt[i[0]], sellup_for_fare_brand_rt[i[0]] = fare_brand_creation(add_on_rt[i[0]]['rt'],
                                                                          mpf_recomended_fare_rt_update, rt_channel,
                                                                          rt_fare_brand,
                                                                          delta_currency, compartment, channel_fb, "")
                            elif first_hit == True:
                                fare_brand_value_rt[i[0]], sellup_for_fare_brand_rt[i[0]] = fare_brand_creation(add_on_rt[i[0]]['rt'],
                                                                          i[1], rt_channel,
                                                                          rt_fare_brand,
                                                                          delta_currency, compartment, channel_fb, "")
                            else:
                                pass

                        # print fare_brand_value_rt["L"]
                        # print asd
                        # For GDS channel update the recommend fare to GDS currency from Web currency.
                        for key, value in fare_brand_value_ow.items():
                            # print key
                            for k, v in fare_brand_value_ow[key].items():
                                if "gds" in k.lower():
                                    # print key, k, fare_brand_value_ow[key][k], convert_tax_currency(fare_brand_value_ow[key][k], delta_currency_gds, delta_currency_web), delta_currency_gds, delta_currency_web
                                    fare_brand_value_ow[key][k] = convert_tax_currency(fare_brand_value_ow[key][k],
                                                                                       delta_currency_gds,
                                                                                       delta_currency_web)
                                    # print fare_brand_value_ow[key][k]
                            # print fare_brand_value_ow[key]

                        for key, value in fare_brand_value_rt.items():
                            # print key
                            for k, v in fare_brand_value_rt[key].items():
                                if "gds" in k.lower():
                                    # print key, k, fare_brand_value_ow[key][k], convert_tax_currency(fare_brand_value_ow[key][k], delta_currency_gds, delta_currency_web), delta_currency_gds, delta_currency_web
                                    fare_brand_value_rt[key][k] = convert_tax_currency(fare_brand_value_rt[key][k],
                                                                                       delta_currency_gds,
                                                                                       delta_currency_web)

                        # print fare_brand_value_rt['R']
                        # print asd
                        rbd_df_ow = RBD_wise_farebrand(fare_brand_value_ow, "" , ow_channel, ow_fare_brand ,
                                                       list_fare_brand_overall, "1", RBD_wise_max_fare_ow)
                        rbd_df_rt = RBD_wise_farebrand(fare_brand_value_rt, "", rt_channel, rt_fare_brand ,
                                                       list_fare_brand_overall, "2", RBD_wise_max_fare_rt)

                        New_DF_ow = pd.DataFrame()
                        New_DF_rt = pd.DataFrame()
                        # print "RBD_min_fare_rt"
                        # print rbd_df_ow[['RBD', 'Web Lite', 'Web Lite max']]
                        # print rbd_df_ow
                        # print asd

                        if len(host_fares_not_expired) > 0:
                            country_cd = list(db.JUP_DB_Region_Master.find({"POS_CD": pos}))[0]['COUNTRY_CD']

                            FCR_Sellup_creation_rt_without_filter = FCR_Sellup_creation(
                                host_fares_not_expired[host_fares_not_expired['oneway_return'] == "2"], RBD_min_fare_rt,
                                rbd_df_rt, country_cd, add_on_rt, compartment, "2", FCR_fare_value, base_RBD_rt, db, delta_currency_gds, delta_currency_web)
                            FCR_Sellup_creation_ow_without_filter = FCR_Sellup_creation(
                                host_fares_not_expired[host_fares_not_expired['oneway_return'] == "1"], RBD_min_fare_ow,
                                rbd_df_ow, country_cd, add_on_ow, compartment, "1", FCR_fare_value, base_RBD_ow, db, delta_currency_gds, delta_currency_web)
                            FCR_Sellup_creation_ow_without_filter = FCR_Sellup_creation_ow_without_filter.reset_index()
                            FCR_Sellup_creation_rt_without_filter = FCR_Sellup_creation_rt_without_filter.reset_index()

                        # space "" farebrand is there so we started considering those space as GDS Value fare brand
                        if compartment == "Y":
                            FCR_Sellup_creation_rt_without_filter.loc[FCR_Sellup_creation_rt_without_filter['fare_brand'] == "", 'fare_brand'] = "VALUE"
                            FCR_Sellup_creation_ow_without_filter.loc[FCR_Sellup_creation_ow_without_filter['fare_brand'] == "", 'fare_brand'] = "VALUE"
                        elif compartment == "J":
                            FCR_Sellup_creation_rt_without_filter.loc[
                                FCR_Sellup_creation_rt_without_filter['fare_brand'] == "", 'fare_brand'] = "BUSINESS"
                            FCR_Sellup_creation_ow_without_filter.loc[
                                FCR_Sellup_creation_ow_without_filter['fare_brand'] == "", 'fare_brand'] = "BUSINESS"
                        else:
                            pass
                        for rbd in sellup_for_fare_brand_rt.keys():
                            # print
                            for channel_fb in sellup_for_fare_brand_rt[rbd].keys():
                                if channel_fb.split(" ", 2)[0] == "TA":
                                    channel = channel_fb.split(" ", 2)[0]
                                else:
                                    ## GDS and Web channels we are considering as lower case only in ATPCO Fares Rules collection
                                    channel = channel_fb.split(" ", 2)[0].lower()

                                fare_brand = channel_fb.split(" ", 1)[1].upper()
                                # original_farebrand = channel_fb.split(" ", 1)[1].upper()
                                # fare_brand_ = channel_fb.split(" ", 1)[1]
                                FCR_Sellup_creation_rt_without_filter.loc[
                                    (FCR_Sellup_creation_rt_without_filter.RBD == rbd) &
                                    (FCR_Sellup_creation_rt_without_filter.fare_brand == fare_brand.upper()) &
                                    (FCR_Sellup_creation_rt_without_filter.channel == channel.upper().replace("WEB","web").replace(
                                        "GDS", "gds")), 'sellup_data'] = "Sellup = "+sellup_for_fare_brand_rt[rbd][channel_fb]

                        for rbd in sellup_for_fare_brand_ow.keys():
                            for channel_fb in sellup_for_fare_brand_ow[rbd].keys():
                                if channel_fb.split(" ", 2)[0] == "TA":
                                    channel = channel_fb.split(" ", 2)[0]
                                else:
                                    ## GDS and Web channels we are considering as lower case only in ATPCO Fares Rules collection
                                    channel = channel_fb.split(" ", 2)[0].lower()

                                fare_brand = channel_fb.split(" ", 1)[1].upper()
                                # original_farebrand = channel_fb.split(" ", 1)[1].upper()
                                # fare_brand_ = channel_fb.split(" ", 1)[1]
                                FCR_Sellup_creation_ow_without_filter.loc[
                                    (FCR_Sellup_creation_ow_without_filter.RBD == rbd) &
                                    (FCR_Sellup_creation_ow_without_filter.fare_brand == fare_brand.upper()) &
                                    (FCR_Sellup_creation_ow_without_filter.channel == channel.upper().replace("WEB","web").replace(
                                        "GDS", "gds")), 'sellup_data'] = "Sellup = "+sellup_for_fare_brand_ow[rbd][channel_fb]

                        for key in RBD_min_fare_rt.keys():
                            try:
                                FCR_Sellup_creation_rt_without_filter.loc[
                                    (FCR_Sellup_creation_rt_without_filter.RBD == key) &
                                    (FCR_Sellup_creation_rt_without_filter.fare_brand == rt_fare_brand.upper()) &
                                    (FCR_Sellup_creation_rt_without_filter.channel == rt_channel.upper().replace("WEB",
                                                                                                                 "web").replace(
                                        "GDS", "gds")), 'sellup_data'] = "As" \
                                                                         " per FCR logic, Lowest fare value is {0} in RBD {1}".format(
                                    fare_brand_value_rt[key][rt_channel + " " + rt_fare_brand], key)
                            except KeyError:
                                pass
                        for key in RBD_min_fare_ow.keys():
                            try:
                                FCR_Sellup_creation_ow_without_filter.loc[
                                    (FCR_Sellup_creation_ow_without_filter.RBD == key) &
                                    (FCR_Sellup_creation_ow_without_filter.fare_brand == ow_fare_brand.upper()) &
                                    (FCR_Sellup_creation_ow_without_filter.channel == ow_channel.upper().replace("WEB",
                                                                                                                 "web").replace(
                                        "GDS", "gds")), 'sellup_data'] = "As" \
                                                                         " per FCR logic, Lowest fare value is {0} in RBD {1} {2} ".format(
                                    fare_brand_value_ow[key][ow_channel + " " + ow_fare_brand], key, sellup_for_fare_brand_ow[key][ow_channel + " " + ow_fare_brand])
                            except KeyError:
                                pass

                        if rt_No_reco_needed == True:
                            try:
                                FCR_Sellup_creation_rt_without_filter.loc[
                                    (FCR_Sellup_creation_rt_without_filter.RBD == base_RBD_rt) &
                                    (FCR_Sellup_creation_rt_without_filter.fare_brand == rt_fare_brand.upper()) &
                                    (FCR_Sellup_creation_rt_without_filter.channel == rt_channel.upper().replace("WEB", "web").replace("GDS", "gds")), 'sellup_data'] = "As" \
                                                " per FCR logic, Lowest fare value is {0} in RBD {1}".format(fare_brand_value_rt[base_RBD_rt][rt_channel+" "+rt_fare_brand], base_RBD_rt)
                            except KeyError:
                                pass
                        else:
                            FCR_Sellup_creation_rt_without_filter.loc[
                                (FCR_Sellup_creation_rt_without_filter.RBD == base_RBD_rt) &
                                (FCR_Sellup_creation_rt_without_filter.fare_brand == rt_fare_brand.upper()) &
                                (FCR_Sellup_creation_rt_without_filter.channel == rt_channel.upper().replace("WEB", "web").replace("GDS", "gds")), 'sellup_data'] = rt_sellup_builder

                        if ow_No_reco_needed == True:
                            FCR_Sellup_creation_ow_without_filter.loc[
                                (FCR_Sellup_creation_ow_without_filter.RBD == base_RBD_ow) &
                                (FCR_Sellup_creation_ow_without_filter.fare_brand == ow_fare_brand.upper()) &
                                (FCR_Sellup_creation_ow_without_filter.channel == ow_channel.upper().replace("WEB", "web").replace("GDS", "gds")), 'sellup_data'] = "As" \
                                            " per FCR logic, Lowest fare value is {0} in RBD {1}".format(fare_brand_value_ow[base_RBD_ow][ow_channel+" "+ow_fare_brand], base_RBD_ow)
                        else:
                            FCR_Sellup_creation_ow_without_filter.loc[
                                (FCR_Sellup_creation_ow_without_filter.RBD == base_RBD_ow) &
                                 (FCR_Sellup_creation_ow_without_filter.fare_brand == ow_fare_brand.upper()) &
                                (FCR_Sellup_creation_ow_without_filter.channel == ow_channel.upper().replace("WEB", "web").replace("GDS", "gds")), 'sellup_data'] = ow_sellup_builder


                        # Earlier there are no farebrand name is there for Value in APTCO fare rules collection instead there is just
                        # base_RBD_ow = ""
                        # base_RBD_rt = ""
                        # Remove the existing sellup and update to reco fare as existing fare only
                        if (base_RBD_ow == "L" or base_RBD_ow == "Z") and RBD_min_fare_ow[base_RBD_ow] == 0 :
                            FCR_Sellup_creation_ow_without_filter.loc[
                                (FCR_Sellup_creation_ow_without_filter.RBD == base_RBD_ow)
                                , 'sellup_data'] = ""

                            FCR_Sellup_creation_ow_without_filter.loc[
                                (FCR_Sellup_creation_ow_without_filter.RBD == base_RBD_ow)
                                , 'recommended_fare'] = FCR_Sellup_creation_ow_without_filter.loc[
                                (FCR_Sellup_creation_ow_without_filter.RBD == base_RBD_ow)]['retention_fare']

                        if (base_RBD_rt == "L" or base_RBD_rt == "Z") and RBD_min_fare_rt[base_RBD_rt] == 0 :
                            FCR_Sellup_creation_rt_without_filter.loc[
                                (FCR_Sellup_creation_rt_without_filter.RBD == base_RBD_rt)
                                , 'sellup_data'] = ""

                            FCR_Sellup_creation_rt_without_filter.loc[
                                (FCR_Sellup_creation_rt_without_filter.RBD == base_RBD_rt)
                                , 'recommended_fare'] = FCR_Sellup_creation_rt_without_filter.loc[
                                (FCR_Sellup_creation_rt_without_filter.RBD == base_RBD_rt)]['retention_fare']

                        ## Applying on secondary filters with these recommendations
                        ## Inclusion table also we have added within this logic
                        for each_combo in secondary_criteria:
                            ## Channel Filters
                            ## Step -1 Replace filter channels into ATPCO applicable cases
                            channel_filters = each_combo['channel']
                            for num in range(len(channel_filters)):
                                channel_filters[num] = channel_filters[num].lower().replace('ta', 'TA').replace('gds',
                                                                                                                'gds').replace(
                                    'web', 'web')

                            ## Farebrand Filters
                            ## Step -1 Replace filter channels into ATPCO applicable cases
                            fare_brand_filters = each_combo['fare_brand']
                            for num in range(len(fare_brand_filters)):
                                fare_brand_filters[num] = fare_brand_filters[num].lower()\
                                                                                        .replace('lite', 'LITE')\
                                                                                        .replace('value', 'VALUE')\
                                                                                        .replace('gds flex', 'GDS FLEX')\
                                                                                        .replace('fly+visa', 'FLY+VISA')\
                                                                                        .replace('gds', '')\
                                                                                        .replace('flex', 'FLEX')\
                                                                                        .replace('business', 'BUSINESS')
                            ## Applying all filters for both OW and RT
                            FCR_Sellup_creation_ow_without_filter['filter'] = FCR_Sellup_creation_ow_without_filter.apply(
                                        lambda row: True if (row['RBD'] in each_combo['RBD'] and row['channel'] in channel_filters and
                                                            row['fare_brand'] in fare_brand_filters)
                                        else False, axis=1)

                            FCR_Sellup_creation_rt_without_filter[
                                'filter'] = FCR_Sellup_creation_rt_without_filter.apply(
                                lambda row: True if (
                                            row['RBD'] in each_combo['RBD'] and row['channel'] in channel_filters and
                                            row['fare_brand'] in fare_brand_filters)
                                else False, axis=1)

                            FCR_Sellup_creation_ow_without_filter_ = FCR_Sellup_creation_ow_without_filter.loc[~(
                                        (FCR_Sellup_creation_ow_without_filter['filter'] == False) & (
                                            FCR_Sellup_creation_ow_without_filter['new_fare'] == True))]

                            FCR_Sellup_creation_rt_without_filter_ = FCR_Sellup_creation_rt_without_filter.loc[~(
                                        (FCR_Sellup_creation_rt_without_filter['filter'] == False) & (
                                            FCR_Sellup_creation_rt_without_filter['new_fare'] == True))]

                            FCR_Sellup_creation_ow_without_filter_.loc[
                                FCR_Sellup_creation_ow_without_filter_['filter'] == False, 'to_delete_fare'] = False

                            FCR_Sellup_creation_rt_without_filter_.loc[
                                FCR_Sellup_creation_rt_without_filter_['filter'] == False, 'to_delete_fare'] = False


                            print("--------------  After secondary filter  -------------")
                            print("--------------  OW  -------------")
                            print FCR_Sellup_creation_rt_without_filter_
                            # print("--------------  RT  -------------")
                            # print FCR_Sellup_creation_rt_without_filter_
                        # '''
                        # Add more filter where base RBD is coming as L or Z RBD Remove the new recommendation fares to avoid zero fare recommendation
                        if compartment == "Y":
                            try:
                                if ow_No_reco_needed == True:
                                    # del RBD_min_fare_ow['L']
                                    FCR_Sellup_creation_ow_without_filter__ = FCR_Sellup_creation_ow_without_filter_.loc[
                                        ~(
                                                (FCR_Sellup_creation_ow_without_filter_['RBD'] == "L") & (
                                                FCR_Sellup_creation_ow_without_filter_['new_fare'] == True))]

                                else:
                                    FCR_Sellup_creation_ow_without_filter__ = FCR_Sellup_creation_ow_without_filter_

                                if rt_No_reco_needed == True:
                                    FCR_Sellup_creation_rt_without_filter__ = FCR_Sellup_creation_rt_without_filter_.loc[
                                        ~(
                                                (FCR_Sellup_creation_rt_without_filter_['RBD'] == "L") & (
                                                FCR_Sellup_creation_rt_without_filter_['new_fare'] == True))]
                                else:
                                    FCR_Sellup_creation_rt_without_filter__ = FCR_Sellup_creation_rt_without_filter_

                            except KeyError:
                                pass
                        else:
                            try:
                                if ow_No_reco_needed == True:
                                    FCR_Sellup_creation_ow_without_filter__ = FCR_Sellup_creation_ow_without_filter_.loc[
                                        ~(
                                                (FCR_Sellup_creation_ow_without_filter_['RBD'] == "Z") & (
                                                FCR_Sellup_creation_ow_without_filter_['new_fare'] == True))]
                                else:
                                    FCR_Sellup_creation_ow_without_filter__ = FCR_Sellup_creation_ow_without_filter_

                                if rt_No_reco_needed == True:
                                    FCR_Sellup_creation_rt_without_filter__ = FCR_Sellup_creation_rt_without_filter_.loc[
                                        ~(
                                                (FCR_Sellup_creation_rt_without_filter_['RBD'] == "Z") & (
                                                FCR_Sellup_creation_rt_without_filter_['new_fare'] == True))]
                                else:
                                    FCR_Sellup_creation_rt_without_filter__ = FCR_Sellup_creation_rt_without_filter_

                            except KeyError:
                                pass


                        New_DF_ow = FCR_Sellup_creation_ow_without_filter__
                        New_DF_rt = FCR_Sellup_creation_rt_without_filter__
                        # print "FCR_Sellup_creation_ow_without_filter"
                        # print New_DF_rt[['recommended_fare']]
                        od_distance = get_od_distance(od=(origin + destination), db=db)

                        New_DF_ow.loc[New_DF_ow['channel'] == "gds", 'currency'] = delta_currency_gds
                        New_DF_rt.loc[New_DF_rt['channel'] == "gds", 'currency'] = delta_currency_gds

                        # adding od_distance for Yield check
                        New_DF_ow['od_distance'] = od_distance
                        New_DF_rt['od_distance'] = od_distance * 2
                        # New_DF_ow = New_DF_ow.rename(columns={"reco_fare": "recommended_fare"})
                        # New_DF_rt = New_DF_rt.rename(columns={"reco_fare": "recommended_fare"})

                        ## Update default values to dataframes
                        New_DF_rt['is_expired'].fillna(0, inplace=True)
                        New_DF_ow['is_expired'].fillna(0, inplace=True)

                        print "######### Before general rules ################"
                        print len(New_DF_rt)
                        ## Do a general check to the recommeded fares
                        if general_rules != {} and len(New_DF_rt) or len(New_DF_ow):
                            # print 1
                            # print New_DF_rt[['recommended_fare']]
                            ## check for inversion check
                            if 'Inversion' in general_rules:
                                if general_rules['Inversion']["active"] ==  True:
                                    # As per competitor match Inversion will never happen So Keep the flag  as false
                                    New_DF_ow['inversion_flag'] = False
                                    New_DF_ow['inversion_desc'] = ""
                                    # rbd_cur = list(client[ATPCO_DB].JUP_DB_ATPCO_RBD.find({'CARRIER': Host_Airline_Code}))[0]
                                    # # print get_inverted_fares(New_DF_ow, compartment, rbd_cur)
                                    # New_DF_ow = get_inverted_fares(New_DF_ow, compartment, rbd_cur)
                                    # New_DF_rt = get_inverted_fares(New_DF_rt, compartment, rbd_cur)
                                    # print 'Inversion'
                                    print len(New_DF_ow)
                                    # pass
                            else:
                                print "Inversion check is not available for this market "+ pos + origin + destination
                                # lower
                            # print 2
                            # print New_DF_rt[['recommended_fare']]
                            if 'lower_threshold' in general_rules:

                                if general_rules['lower_threshold']["active"] ==  True:
                                    ## For percentage
                                    New_DF_ow['lower_threshould_breach'] = False
                                    New_DF_rt['lower_threshould_breach'] = False
                                    if general_rules['lower_threshold']["type"] ==  "P":
                                        lower_fare_percentage = general_rules['lower_threshold']["value"]
                                        New_DF_ow.loc[New_DF_ow['new_fare'] != True, 'lower_threshould_breach'] = New_DF_ow[New_DF_ow['new_fare'] != True].apply(lambda row: check_threshould(row, lower_fare_percentage,'p', False), axis=1)
                                        New_DF_rt.loc[New_DF_rt['new_fare'] != True, 'lower_threshould_breach'] = New_DF_rt[New_DF_rt['new_fare'] != True].apply(lambda row: check_threshould(row, lower_fare_percentage,'p', False), axis=1)
                                        print 'lower'
                                        print len(New_DF_ow)
                                    else:
                                        ## For absolute
                                        lower_fare_value = general_rules['lower_threshold']["value"]
                                        New_DF_ow.loc[New_DF_ow['new_fare'] != True, 'lower_threshould_breach'] = New_DF_ow[
                                            New_DF_ow['new_fare'] != True].apply(
                                            lambda row: check_threshould(row, lower_fare_percentage, 'a', False), axis=1)
                                        New_DF_rt.loc[New_DF_rt['new_fare'] != True, 'lower_threshould_breach'] = New_DF_rt[
                                            New_DF_rt['new_fare'] != True].apply(
                                            lambda row: check_threshould(row, lower_fare_percentage, 'a', False), axis=1)
                                        print 'lower'
                                        print len(New_DF_ow)
                            else:
                                print "Lower threshould check is not available for this market " + pos + origin + destination

                            # print 3
                            # print New_DF_rt[['recommended_fare']]
                            if 'upper_threshold' in general_rules:
                                if general_rules['upper_threshold']["active"] ==  True:
                                    ## For percentage
                                    New_DF_ow['upper_threshold'] = False
                                    New_DF_rt['upper_threshold'] = False
                                    if general_rules['upper_threshold']["type"] ==  "P":
                                        # print "--------------------------check upper threshould ----------------------"
                                        # print New_DF_ow
                                        # print "--------------------------check upper threshould ----------------------"
                                        lower_fare_percentage = general_rules['upper_threshold']["value"]
                                        New_DF_ow.loc[New_DF_ow['new_fare'] != True, 'upper_threshold_breach'] = New_DF_ow[New_DF_ow['new_fare'] != True].apply(lambda row: check_threshould(row, lower_fare_percentage,'p', True), axis=1)
                                        New_DF_rt.loc[New_DF_rt['new_fare'] != True, 'upper_threshold_breach'] = New_DF_rt[New_DF_rt['new_fare'] != True].apply(lambda row: check_threshould(row, lower_fare_percentage,'p', True), axis=1)
                                        print 'upper'
                                        print len(New_DF_ow)
                                    else:
                                        ## For absolute
                                        lower_fare_value = general_rules['upper_threshold']["value"]
                                        New_DF_ow.loc[New_DF_ow['new_fare'] != True, 'upper_threshold_breach'] = New_DF_ow[
                                            New_DF_ow['new_fare'] != True].apply(
                                            lambda row: check_threshould(row, lower_fare_percentage, 'a', True), axis=1)
                                        New_DF_rt.loc[New_DF_rt['new_fare'] != True, 'upper_threshold_breach'] = New_DF_rt[
                                            New_DF_rt['new_fare'] != True].apply(
                                            lambda row: check_threshould(row, lower_fare_percentage, 'a', True), axis=1)
                                        print 'upper'
                                        print len(New_DF_ow)
                            else:
                                print "Lower threshould check is not available for this market " + pos + origin + destination
                            # print 4
                            # print New_DF_rt[['recommended_fare']]
                            if 'floor_price' in general_rules:
                                if general_rules['floor_price']["active"] ==  True:
                                    New_DF_ow['floor_price_breach'] = False
                                    New_DF_rt['floor_price_breach'] = False
                                    New_DF_ow.loc[New_DF_ow['new_fare'] != True, 'floor_price_breach'] = New_DF_ow[
                                        New_DF_ow['new_fare'] != True].apply(
                                        lambda row: True if row['recommended_fare'] <= general_rules['floor_price']["value"] else False, axis=1)
                                    New_DF_rt.loc[New_DF_rt['new_fare'] != True, 'floor_price_breach'] = New_DF_rt[
                                        New_DF_rt['new_fare'] != True].apply(
                                        lambda row: True if row['recommended_fare'] <= general_rules['floor_price']["value"] else False, axis=1)
                                    print 'floor'
                                    print len(New_DF_ow)
                            else:
                                print "Floor Price check is not available for this market " + pos + origin + destination

                            # if 'yield' in general_rules:
                            #     if general_rules['yield']["active"] ==  True:
                            #         ## For percentage
                            #         # New_DF_ow['yield_breach'] = False
                            #         # New_DF_rt['yield_breach'] = False
                            #         if general_rules['yield']["type"] ==  "P":
                            #             yield_percentage = general_rules['yield']["value"]
                            #             # print yield_percentage
                            #             New_DF_ow['yield_breach'] = New_DF_ow['reco_yield'] <= yield_percentage
                            #             New_DF_rt['yield_breach'] = New_DF_rt['reco_yield'] <= yield_percentage
                            #             print 'Yield'
                            #             print len(New_DF_ow)
                            # else:
                            #     print "yield check is not available for this market " + pos + origin + destination
                            # print 5
                            # print New_DF_rt[['recommended_fare']]
                            # print New_DF_rt[["oneway_return", "RBD", "fare_basis", "fare_brand", "channel", "recommended_fare", 'sellup_data','fare', 'base_fare', "recommended_fare_base"]]
                            # if 'ow_rt' in general_rules:
                            #     if general_rules['ow_rt']["active"] ==  True:
                            #         ## For percentage
                            #         # New_DF_ow['yield_breach'] = False
                            #         # New_DF_rt['yield_breach'] = False
                            #         if general_rules['ow_rt']["type"] ==  "P":
                            #             ow_rt_percentage = general_rules['ow_rt']["value"]
                            #             New_DF_ow = ow_rt_fare_check(New_DF_ow, New_DF_rt, ow_rt_percentage)
                            #             print 'ow_rt'
                            #             print len(New_DF_ow)
                            #             # New_DF_ow['ow_rt_breach'] = New_DF_ow['reco_yield'] <= yield_percentage
                            #             # New_DF_rt['ow_rt_breach'] = New_DF_rt['reco_yield'] <= yield_percentage
                            # else:
                            #     print "ow_rt check is not available for this market " + pos + origin + destination
                            # print 6
                            # print New_DF_ow[["oneway_return", "RBD", "fare_basis", "fare_brand", "channel", "recommended_fare", 'sellup_data','fare', 'base_fare', "recommended_fare_base"]]
                            if 'low_fare' in general_rules:
                                if general_rules['low_fare']["active"] == True:
                                    ## For percentage
                                    # New_DF_ow['yield_breach'] = False
                                    # New_DF_rt['yield_breach'] = False
                                    if general_rules['low_fare']["type"] == "P":
                                        low_fare_percentage = general_rules['low_fare']["value"]
                                        low_fare_currency= general_rules['low_fare']["currency"]
                                        New_DF_ow['low_fare_breach'] = New_DF_ow.apply(lambda row : row['recommended_fare'] <= convert_tax_currency(low_fare_percentage, row['currency'], low_fare_currency), axis=1)
                                        New_DF_rt['low_fare_breach'] = New_DF_rt.apply(lambda row : row['recommended_fare'] <= convert_tax_currency(low_fare_percentage, row['currency'], low_fare_currency), axis=1)
                                        print 'low fare'
                                        # print len(New_DF_ow)
                            else:
                                print "low_fare check is not available for this market " + pos + origin + destination

                        else:
                            print "No general rule is available for this Market and model"+ pos + origin + destination

                        # This is to avoid hitting DB twise for ow/rt s
                        # print 7
                        # print New_DF_ow[["oneway_return", "RBD", "fare_basis", "fare_brand", "channel", "recommended_fare", 'sellup_data','fare', 'base_fare', "recommended_fare_base"]]
                        # print New_DF_rt[["oneway_return", "RBD", "fare_basis", "fare_brand", "channel", "recommended_fare", 'sellup_data','fare', 'base_fare', "recommended_fare_base"]]
                        # print host_fares_expired
                        Fares = pd.concat([New_DF_ow, New_DF_rt, host_fares_expired])
                        # print "reco fare "
                        # df = Fares[(Fares.oneway_return == "2")& (Fares.RBD == "U")]
                        # print df[["oneway_return", "RBD", "fare_basis", "fare_brand", "channel", "recommended_fare", 'sellup_data','fare', 'base_fare', "recommended_fare_base"]]

                        Fares['recommended_fare'].fillna(0, inplace=True)

                        Fares['recommended_fare'] = np.where(Fares['recommended_fare'] == 0,
                                                                  Fares['fare']+Fares['YQ'] + Fares['YR'] + \
                                                                Fares['Average_surcharge'], Fares['recommended_fare'])
                        Fares['base_fare_negative'] = Fares.apply(
                            lambda row: row['recommended_fare_base'] < convert_tax_currency(10, row['currency'], "USD"),
                            axis=1)
                        # Fares = Fares.loc[~(Fares['base_fare_negative'] == False)]
                        # print Fares[
                        #     ['base_fare_negative', "recommended_fare_base", "oneway_return", "RBD", "fare_basis",
                        #      "fare_brand", "channel", "recommended_fare", 'fare', 'base_fare', "new_fare"]]
                        # print asc
                        Fares, currency_factor = get_pax_yield(pos, origin, destination, compartment, Fares,
                                                               dep_date_start,
                                                               dep_date_end, od_distance, db=db)
                        # print "reco fare "
                        # df = Fares[(Fares.oneway_return == "2") & (Fares.RBD == "B")]
                        # print df[["oneway_return", "RBD", "fare_basis", "fare_brand", "channel", "recommended_fare",
                        #           'sellup_data', 'fare', 'base_fare', "recommended_fare_base"]]
                        # if

                        if general_rules != {} and len(Fares):
                            if 'yield' in general_rules:
                                if general_rules['yield']["active"] ==  True:
                                    ## For percentage
                                    # New_DF_ow['yield_breach'] = False
                                    # New_DF_rt['yield_breach'] = False
                                    if general_rules['yield']["type"] ==  "P":
                                        yield_percentage = general_rules['yield']["value"]
                                        # print yield_percentage
                                        Fares['yield_breach'] = Fares['reco_yield'] <= yield_percentage
                                        # New_DF_rt['yield_breach'] = New_DF_rt['reco_yield'] <= yield_percentage
                                        print 'Yield'
                                        print len(New_DF_ow)
                            else:
                                print "yield check is not available for this market " + pos + origin + destination

                    elif primary_criteria['filter']['base_fare_based'] == "LFF" and primary_criteria['filter']['fare_feed'] == "ATPCO":
                        ## Need to develop LFF based comp match
                        print("Need to develop LFF based comp match")
                    elif primary_criteria['filter']['base_fare_based'] == "HFF" and primary_criteria['filter']['fare_feed'] == "ATPCO":
                        ## Need to develop HFF based comp match
                        print("Need to develop HFF based comp match")
                    else:
                        print("check primary criteria")

                # '''
            else:
                print model_definition['model_no']



        # New_DF_rt, currency_factor = get_pax_yield(pos, origin, destination, compartment, New_DF_rt,
        #                                            dep_date_start,
        #                                            dep_date_end, od_distance, db=db)


    else:
        print "no effective model defined for this market : "+pos+origin+destination+compartment
    # '''
    ## For testing prupose
    # Fares = New_DF_ow
    # print len(New_DF_rt)
    # print len(New_DF_ow)
    # print len(host_fares_expired)
    # host_fares_expired['new_fare'] = False

    if "pandas.core.frame.DataFrame" in str(type(Fares)):
        return Fares.to_dict('records')
    else:
        return {}

if __name__ == "__main__":
    # dep_date_start = "2019-09-01"
    # dep_date_end = "2019-10-31",
    # pos = "UAE",
    # origin = "DXB"
    # destination = "AMM"
    # compartment = "Y"
    # main_func("AMM", "AMM", "VKO", "Y", "2017-09-01", "2017-09-30",
    #           dates_list=[{"start": "2017-09-01", "end": "2017-09-30"}], is_manual=1)

    client = mongo_client()
    db = client[JUPITER_DB]
    market_array = [
        # "VKOVKODXBY",
        # "AMMAMMDXBY",
            "UAEDXBSLLY",
            # "ADDADDDXBY",
            # "ADDADDDXBJ",
            # "UAEDXBSLLY",
            # "UAEDXBSLLJ",
            # "SLLSLLDXBY",
            # "SLLSLLDXBJ"
        # "ADDADDDXBY",
        # "GYDGYDDXBY",
    ]
    for mrkt in market_array:
        print mrkt
        fares = comp_match(pos=mrkt[0:3], origin=mrkt[3:6], destination=mrkt[6:9], compartment=mrkt[9:],
                           dep_date_start='2019-06-14', dep_date_end='2019-08-13', db=db)
        df = pd.DataFrame(fares)
        # df = df[(df.oneway_return == "2")]
        # df = df[(df.fare_basis == "	UOB3AZ1")]
        df = df[(df.RBD == "Y")
                # & (df.oneway_return == "2")
        ]
        # print df.to_string()
        print df[["oneway_return", "RBD", "fare_basis", "fare_brand", "channel", "currency", "recommended_fare", 'sellup_data','fare', 'base_fare', "recommended_fare_base",  "new_fare", "footnote"]]

        # print df[['fare', 'fare_basis', 'currency', 'OD']]
    # pd.DataFrame(fares).to_csv('KWIKWIDXB.csv')
    # df=get_host_fares_df(pos='UAE', origin='DXB', destination='GYD', compartment='Y',
    #                   extreme_start_date='2018-04-20', extreme_end_date='2018-05-20')
    # print df[['fare_basis', 'YQ', 'YR', 'taxes']]
    client.close()
    # print("calling")`