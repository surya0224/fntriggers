from __future__ import division
import json
import re
import pymongo
from collections import defaultdict
from celery import group
from flask import Flask, jsonify, request
from gevent.pywsgi import WSGIServer
from copy import deepcopy
import time
from jupiter_AI.triggers.manual_triggers.MainClass import ManualTrigger
from jupiter_AI.batch.customer_segmentation.Dates_Logic import quarter_logic
import datetime
from jupiter_AI import client, JUPITER_DB, JUPITER_LOGGER, mongo_client, SYSTEM_DATE
from jupiter_AI.logutils import measure
from jupiter_AI.batch.atpco_automation.Automation_tasks import run_booking_triggers, \
    run_events_triggers, \
    run_pax_triggers, \
    run_revenue_triggers, \
    run_yield_triggers, \
    run_opp_trend_triggers

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import sys


app = Flask(__name__)
app.config["DEBUG"] = True


@measure(JUPITER_LOGGER)
@app.route('/')
def index():
    return "Hello World!"


@measure(JUPITER_LOGGER)
@app.route('/QuarterLogic', methods=['POST'])
def quarter_log():
    rand = request.json[0]
    # print rand
    # print datetime.datetime.strptime(rand['start_date'], "%Y-%m-%d"), datetime.datetime.strptime(rand['end_date'],
    #                                                                                              "%Y-%m-%d")
    result = quarter_logic(datetime.datetime.strptime(rand['start_date'], "%Y-%m-%d"),
                           datetime.datetime.strptime(rand['end_date'], "%Y-%m-%d"))
    return jsonify(result)


@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE')
    return response

def convert_currency(currency_doc, value, from_code, to_code='AED'):
    """
    :param from_code:currency code of value
    :param to_code:currency code into which value needs to be converted
    :param value:the value in from_code to be converted to to_code
    :return:value in to_code currency
    """
    if from_code == to_code:
        return value
    else:
        # cursor = db.JUP_DB_Exchange_Rate.find({'code': {'$in': [from_code, to_code]}})
        # print cursor.count()
        if from_code == 'AED':
            return float(value)/currency_doc[to_code]
        elif to_code == 'AED':
            return float(value)*currency_doc[from_code]
        else:
            ratio_2to1 = float(value) / currency_doc[from_code]

            ratio_2to1 = cursor[0]['Reference_Rate'] / cursor[1]['Reference_Rate']
            converted_value = float(value) * ratio_2to1
            return converted_value


def get_fee_from_configuration(fee_label, Add_ons, host_currency, FCR_currency, reco_fare, EXCHANGE_RATE):
    # print fee_label, 'h f', host_currency, 'FCR fee', FCR_currency


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
            fee = (Add_ons[fee_label] / 100)
    return fee


def get_add_on_doc(base_baggage, add_od_doc, Add_ons, mpf_recomended_fare, delta_currency, FCR_currency, ow_rt, compartment, EXCHANGE_RATE, fare_brand_currency):
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
            # if Add_ons[key]['compartment'] == compartment:
            #     add_on[type] = get_fee_from_configuration(type, Add_ons[key][ow_rt]['Add-on'][type], delta_currency, FCR_currency,
            #                                                 mpf_recomended_fare, EXCHANGE_RATE)
            # else:
            if Add_ons[key]['compartment'] == compartment:
                add_on[type] = get_fee_from_configuration(type, Add_ons[key][ow_rt]['Add-on'][type], delta_currency,
                                                          fare_brand_currency, mpf_recomended_fare, EXCHANGE_RATE)

                add_on_ow_rt[ow_rt] = add_on
        RBD_Add_on[key] = add_on_ow_rt
        if Add_ons[key]['compartment'] == compartment:
            if Add_ons[key][ow_rt]['minimum'] != None:
                RBD_min_fares[key] = Add_ons[key][ow_rt]['minimum'] / EXCHANGE_RATE[delta_currency] * EXCHANGE_RATE[FCR_currency]
                # convert_tax_currency(Add_ons[key][ow_rt]['minimum'], delta_currency, FCR_currency)
            else:
                RBD_min_fares[key] = 0
    return RBD_Add_on, RBD_min_fares

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

def do_analyse(formula_, each, arithmatic_sumbols, add_on, fare_brand_value):

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
                            temp = calculate(sign, temp, value)
                        # print(value)
                    else:
                        pass
                        # print("We don't have value for ", brand)
                    # print num_percent, brand.strip()
                elif formula_parameter in fare_brand_value:
                    if len_ == 0:
                        temp = temp + fare_brand_value[formula_parameter]
                        builder = str(temp)
                    len_ = len_ + 1
                    if len_ in calc_limit:
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
                    else:
                        temp = add_on[formula_parameter]

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
                builder = str(temp)
            len_ = len_+1
            if len_ in calc_limit :
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
            builder = builder+sign

        elif "FEE" in str(formula_parameter):
            # print add_on[formula_parameter]
            len_ = len_ + 1
            # print len_
            if len_ in calc_limit :
                temp = calculate(sign, temp, add_on[formula_parameter])
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
                fare_brand_value = do_analyse(inverse_formula, no_value_for_any_brand, arithmatic_sumbols, add_on, fare_brand_value)
        # fare_brand_value[no_value_for_any_brand] = fare_brand_value[each] - temp
    else:
        fare_brand_value[each] = temp
    # print fare_brand_value
    # print "final value ", temp
    return fare_brand_value



def fare_brand_formula_sign(fare_brand_formula, add_on, mpf_recomended_fare_rt, channel, fare_brand, delta_currency, flag, channel_fb):
    arithmatic_sumbols = {"add": "+", "sub": "-", "mul": "*", "div": "/"}
    fare_brand_value = {}
    fare_brand_value[channel + " " + fare_brand] = mpf_recomended_fare_rt

    while len(fare_brand_value) < len(channel_fb)+1:
        for each in channel_fb:
            inverse = False
            # print each
            # formula = fare_brand_formula[each][add_on["TFEE IND"]]
            formula = fare_brand_formula[each]
            try:
                fare_brand_value = do_analyse(formula, each, arithmatic_sumbols, add_on, fare_brand_value)
                # print "fare_brand_value length", len(fare_brand_value)
                # print "channel_fb length", len(channel_fb)
            except KeyError as error:
                pass
                # print "KeyError", error

            formula[:]
            # print fare_brand_value

    return fare_brand_value

# depth_fb(row, fbc_standardation, pos, add_on, channel, fare_brand, compartment)
def depth_fb(df_with_match,fbc_standardation, pos, add_on, channel, fare_brand_, compartment):

    farebasis = ""
    print df_with_match['fare_brand'].upper()
    fare_brand = [x for x in list(fbc_standardation['position_array'][6]['value_array']) if x['value'] == df_with_match['fare_brand'].upper()]

    # print add_on
    print channel.lower()+"   "+fare_brand_.lower()

    ## This is for Exceptional cases GDS channel without any farebrand
    if (channel.lower() == "gds" and fare_brand_.lower() =="value") or (channel.lower() == "gds" and fare_brand_.lower() == "business"):
        if df_with_match['owrt'] == '1':
            farebasis = farebasis + df_with_match['rbd'] + "OW"
        else:
            farebasis = farebasis + df_with_match['rbd'] + "R1Y"

        farebasis = farebasis + \
                    compartment.replace("Y", "2").replace("J", "1") + pos + "1"

    else:
        if df_with_match['owrt'] == '1':
            farebasis = farebasis + df_with_match['rbd'] + "O"
        else:
            farebasis = farebasis + df_with_match['rbd'] + "R"
        if fare_brand is None:
            # fare_brand
            farebasis = farebasis + add_on[df_with_match['rbd']][df_with_match['owrt'].replace("1", "ow").replace("2", "rt")]["BAG FEE_type"][0] + pos
        elif "lite" in fare_brand_.lower():
            farebasis = farebasis + str(fare_brand[0]['key']) + pos
        else:
            farebasis = farebasis + add_on[df_with_match['rbd']][df_with_match['owrt'].replace("1", "ow").replace("2", "rt")]["BAG FEE_type"][0] +str(fare_brand[0]['key'])+ pos

        fare_type = [x for x in list(fbc_standardation['position_array'][8]['value_array']) if df_with_match['channel'].lower() in x['value'].lower()]
        if len(fare_type) > 0:
            farebasis = farebasis + str(fare_type[0]['key'])
    return farebasis

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
                'city': '$POS_CD',
                'country': '$Airport_Code',
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
            '$sort': {"level_priority" : 1, 'model.priority': -1}
        },
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
                    "Add-On" : 1,
                    "pos" : '$pos.value',
                    "origin" : '$origin.value',
                    "destination" : '$destination.value',
                    "compartment" : '$compartment.value'
                }
        }
    ]
    # print "pricing model query"
    # print pipe
    pricing_models_cursor = db.JUP_DB_Pricing_Model_Markets.aggregate(pipe)
    # print list(pricing_models_cursor)
    pricing_models = list(pricing_models_cursor)#[0]['model']
    # print pricing_models
    return pricing_models

def get_fare_brand_and_channel_from_fbc(fbc1, position_array):
    channel = ""
    fare_brand = ""
    for doc in position_array:
        if doc['position'] == 8:
            for each_value in doc['value_array']:
                if len(fbc1) == 6:
                    if each_value['key'] == fbc1[5]:
                        channel = each_value['value'].replace(" Strategic", "")
                if len(fbc1) == 7:
                    if each_value['key'] == fbc1[6]:
                        channel = each_value['value'].replace(" Strategic", "")
                if len(fbc1) == 8:
                    if each_value['key'] == fbc1[7]:
                        channel = each_value['value'].replace(" Strategic", "")
                # print channel
        if doc['position'] == 5:
            for each_value in doc['value_array']:
                if len(fbc1) == 6:
                    if each_value['key'] == fbc1[2]:
                        fare_brand = each_value['value'].replace("", "")
                if len(fbc1) == 7:
                    if each_value['key'] == fbc1[3]:
                        fare_brand = each_value['value'].replace("", "")
                    elif "2" == fbc1[3]:
                        fare_brand = "Value"
                    elif "1" == fbc1[3]:
                        fare_brand = "Business"
                if len(fbc1) == 8:
                    if each_value['key'] == fbc1[4]:
                        fare_brand = each_value['value'].replace("", "")

                    if "2" == fbc1[4]:
                        fare_brand = "Value"
                    elif "1" == fbc1[4]:
                        fare_brand = "Business"

    # print fbc1, channel, fare_brand
    return channel.replace("web", "Web").replace("ta", "TA").replace("gds", "GDS"), fare_brand.lower().replace("business", "Business").replace("lite", "Lite").replace("value", "Value").replace("fly+visa", "FLY+Visa").replace("gds flex","GDS Flex").replace("flex", "Flex")


@measure(JUPITER_LOGGER)
@app.route('/fareBrandCreation', methods=['POST', 'OPTIONS'])
def fare_brand_creation():
    print("start of fb creation")
    client = mongo_client()
    db = client[JUPITER_DB]
    markets = request.json
    # markets = {"type":"new","screen":"promotion_farereview","delta":10,"currency":"AED","fares":[{"fbc":"WO6AE2","od":"DXBAMM","pos":"UAE","compartment":"Y","rbd":"W","channel":"web","carrier":"FZ","tariffcode":"458","origin":"DXB","destination":"AMM","ruleid":"62AE","fareid":None,"ow_rt":"1","rtg":"00699","footnote":"","yq":80,"yr":0,"tax":120,"rbd_type":None,"surcharge":0,"totalFare":1150,"fare":950,"fareBreachedValue":0,"breachedFlag":None,"effectiveFromDate":"2019-04-04","effectiveToDate":None,"currency":"AED","od_distance":0,"p2porconn":"p2p","yield":0,"flight_duration":0,"fareBrand":None,"indexAttr":65,"showfare":True,"updateBaseFare":960,"retfare":1030,"recfare":1040,"recyield":104000,"updatedTotalFare":1160,"ry":"104000.0","applied_fare_basis":"LOS6GS5","applied_footnote":"FN"},{"fbc":"MR6AE2","od":"DXBAMM","pos":"UAE","compartment":"Y","rbd":"M","channel":"web","carrier":"FZ","tariffcode":"458","origin":"DXB","destination":"AMM","ruleid":"62AE","fareid":None,"ow_rt":"2","rtg":"00699","footnote":"71","yq":160,"yr":0,"tax":415,"rbd_type":None,"surcharge":0,"totalFare":1585,"fare":1010,"fareBreachedValue":0,"breachedFlag":None,"effectiveFromDate":"2019-04-30","effectiveToDate":None,"currency":"AED","od_distance":0,"p2porconn":"p2p","yield":0,"flight_duration":0,"fareBrand":None,"indexAttr":46,"showfare":True,"updateBaseFare":1020,"retfare":1170,"recfare":1180,"recyield":118000,"updatedTotalFare":1595,"ry":"118000.0","applied_fare_basis":"LRS6GS5","applied_footnote":"FN"}]}
    type_ = markets["type"]
    delta = markets["delta"]

    if "delta_type" in markets:
        delta_type = markets["delta_type"]
    else:
        delta_type = "A"

    input_ = []
    fare_doc = []
    # Meant to segregate the fare records into different markets, in case if we are getting different market inputs
    # to gather from UI
    comp_dict = dict()
    comp_cur = list(db.JUP_DB_Booking_Class.find())
    for cur in comp_cur:
        comp_dict[cur['Code']] = cur['comp']

    city_airport = dict()
    airport_cur = list(db.JUP_DB_City_Airport_Mapping.find())
    for cur in airport_cur:
        city_airport[cur['Airport_Code']] = cur['FZ_online_pos']

    for each_fares in markets['fares']:
        oorigin = city_airport[each_fares['od'][:3]].replace("UAE", "DXB")
        ddestination = city_airport[each_fares['od'][-3:]].replace("UAE", "DXB")
        each_fares.update({'origin': oorigin})
        each_fares.update({'destination': ddestination})
        try:
            if each_fares['pos'] != None:
                ppos = each_fares['pos']
            else:
                ppos = oorigin.replace("DXB", "UAE")
                each_fares.update({'pos': oorigin.replace("DXB", "UAE")})
        except KeyError as error:
            # print error
            each_fares.update({'pos': oorigin.replace("DXB", "UAE")})
            ppos = oorigin.replace("DXB", "UAE")

        if "compartment" in each_fares:
            ccompartment = each_fares['compartment']
        else:
            each_fares.update({'ccompartment': comp_dict[each_fares['temprbd']]})
            ccompartment = comp_dict[each_fares['temprbd']]

        if type_ == "update":
            # Updating of common column for different screen fares columns
            if "originalrecmndFare" in each_fares:
                each_fares.update({'fare': each_fares["originalrecmndFare"]})
            else:
                each_fares.update({"originalrecmndFare": each_fares['fare']})
            if "temprbd" in each_fares:
                each_fares.update({'rbd': each_fares['temprbd']})
            else:
                each_fares.update({'temprbd': each_fares['rbd']})

            if "fbc" in each_fares:
                each_fares.update({'farebasis': each_fares['fbc']})
            else:
                each_fares.update({'fbc': each_fares['farebasis']})

        # Foot note also required for different recommendation on same farebasis
        footnote = ""
        if 'footnote' in each_fares:
            footnote = each_fares['footnote']

        input_.append((each_fares, ppos + "" + oorigin + "" + ddestination + "" + ccompartment + "" + footnote))
    res = defaultdict(list)
    for v, k in input_: res[k].append(v)

    for each_fare in res:
        # each_fare = "AMMAMMDXBY"
        # print res[each_fare]
        currency = markets["currency"]
        if currency == "":
            currency = "AED"

        f_doc = res[each_fare][0]
        try:
            origin = f_doc['origin']
            destination = f_doc['destination']
        except KeyError:
            origin = f_doc['od'][:3]
            destination = f_doc['od'][-3:]

        if 'pos' in f_doc:
            if f_doc['pos'] != None:
                pos = f_doc['pos']
            else:
                pos = origin
        else:
            pos = origin

        if 'compartment' in f_doc:
            compartment = f_doc['compartment']
        else:
            if "temprbd" in f_doc:
                rbd_ = f_doc['temprbd']
                comp_doc = db.JUP_DB_Booking_Class.find_one({"Code": rbd_})
                compartment = comp_doc['comp']
            elif "rbd" in f_doc:
                rbd_ = f_doc['rbd']
                comp_doc = db.JUP_DB_Booking_Class.find_one({"Code": rbd_})
                compartment = comp_doc['comp']
        # print compartment
        pricing_models = get_pricing_models(pos, origin, destination, compartment, db=db)
        print pricing_models
        if len(list(pricing_models)) > 0:
            # print pricing_models[0]
            try:
                sellup_code = pricing_models[0]['model'][0]['sellup_no']
            except KeyError as error:
                print "Sellup didn't define in the pricing model"
                print pricing_models
                sellup_code = "SP001"
        else:
            sellup_code = "SP001"

        print "Sellup no"
        print sellup_code
        sellup_doc = db.JUP_DB_Sellup_Master.find_one({"sellup_no": sellup_code})
        # print "sellup_doc"
        # print sellup_doc
        if len(sellup_doc) > 0:
            if compartment == "Y":
                fare_brand_formulas = sellup_doc["sellup_formulas_Y"]
                channel_fb = ["TA Lite", "Web Value", "TA Value", "GDS Value", "TA FLY+Visa", "Web Flex", "TA Flex",
                              "GDS GDS Flex"]
            else:
                fare_brand_formulas = sellup_doc["sellup_formulas_J"]
                channel_fb = ["GDS Business"]

            RBD_sellup = sellup_doc["RBD"]
            FCR_currency = sellup_doc['FCR']["currency"]
            fare_brand_currency = sellup_doc['fare_brand']["currency"]
            FCR_formula = sellup_doc['FCR']["fare_value"]
            farebrand_value_calc = sellup_doc['fare_brand']["fare_value"]
        else:
            print " No farebrand formula and RBD "
            fare_brand_formulas = []
            RBD_sellup = []
            ## Default keep as AED
            FCR_currency = "AED"
            fare_brand_currency = "AED"

        try:
            base_fare_brand = pricing_models[0]['model']['primary_criteria']['filter']['base_fare_brand']
            base_fare_channel = pricing_models[0]['model']['primary_criteria']['filter']['base_channel']
        except Exception:
            if compartment == "Y":
                base_fare_brand = "Web"
                base_fare_channel = "Lite"
            else:
                base_fare_brand = "Web"
                base_fare_channel = "Business"
        print base_fare_brand, base_fare_channel

        fare_brand_formula = dict()
        fare_brand_formula_J = dict()
        unwanted_num = {"srt", "end"}
        try:
            base_fare_brand = pricing_models[0]['model'][0]['base_brand']
            # base_fare_channel = pricing_models[0]['model']['primary_criteria']['filter']['base_channel']
        except Exception:
            if compartment == "Y":
                base_fare_brand = "Lite"
                base_fare_channel = "Web"
            else:
                base_fare_brand = "Business"
                base_fare_channel = "Web"

        if compartment == "Y":
            try:
                fare_brand_formula["TA Lite"] = [ele for ele in fare_brand_formulas['TA Lite'] if
                                                 ele not in unwanted_num]
            except Exception:
                fare_brand_formula["TA Lite"] = fare_brand_formulas['TA Lite']
            try:
                fare_brand_formula["Web Value"] = [ele for ele in fare_brand_formulas['Web Value'] if
                                                   ele not in unwanted_num]
            except Exception:
                fare_brand_formula["Web Value"] = fare_brand_formulas['Web Value']
            try:
                fare_brand_formula["TA Value"] = [ele for ele in fare_brand_formulas['TA Value'] if
                                                  ele not in unwanted_num]
            except Exception:
                fare_brand_formula["TA Value"] = fare_brand_formulas['TA Value']
            try:
                fare_brand_formula["GDS Value"] = [ele for ele in fare_brand_formulas['GDS Value'] if
                                                   ele not in unwanted_num]
            except Exception:
                fare_brand_formula["GDS Value"] = fare_brand_formulas['GDS Value']
            try:
                fare_brand_formula["TA FLY+Visa"] = [ele for ele in fare_brand_formulas['TA FLY+Visa'] if
                                                     ele not in unwanted_num]
            except Exception:
                fare_brand_formula["TA FLY+Visa"] = fare_brand_formulas['TA FLY+Visa']
            try:
                fare_brand_formula["Web Flex"] = [ele for ele in fare_brand_formulas['Web Flex'] if
                                                  ele not in unwanted_num]
            except Exception:
                fare_brand_formula["Web Flex"] = fare_brand_formulas['Web Flex']
            try:
                fare_brand_formula["TA Flex"] = [ele for ele in fare_brand_formulas['TA Flex'] if
                                                 ele not in unwanted_num]
            except Exception:
                fare_brand_formula["TA Flex"] = fare_brand_formulas['TA Flex']
            try:
                fare_brand_formula["GDS GDS Flex"] = [ele for ele in fare_brand_formulas['GDS Flex'] if
                                                      ele not in unwanted_num]
            except Exception:
                fare_brand_formula["GDS GDS Flex"] = fare_brand_formulas['GDS Flex']
        else:
            try:
                fare_brand_formula["GDS Business"] = fare_brand_formulas['GDS Business'].remove("srt").remove("end")
            except Exception:
                fare_brand_formula["GDS Business"] = fare_brand_formulas['GDS Business']

        # fare_brand_formulas
        # RBD_sellup
        # FCR_currency
        EXCHANGE_RATE = {}
        currency_crsr = list(client[JUPITER_DB].JUP_DB_Exchange_Rate.find({}))
        for curr in currency_crsr:
            EXCHANGE_RATE[curr['code']] = curr['Reference_Rate']

        add_od_doc = db.JUP_DB_Fare_brand.find_one({"compartment": {"$exists": True}})
        try:
            add_od_doc = add_od_doc['Add-on']
        except KeyError as error:
            add_od_doc = []
        # Required for generating new fare basis
        fbc_standardation = list(db.JUP_DB_Farebasis_Standardization.find({"journey_type": {"$in": ['OW', 'RT']}},
                                                                          {"journey_type": 1,
                                                                           "position_array": 1}).sort(
            [("journey_type", pymongo.ASCENDING)]))
        flag = "Straight"
        base_baggage = "B"
        # list_ = ["web Business", "gds  "]
        # print markets
        pos_curr = dict()
        if type_ == "new":
            ll = list(db.JUP_DB_Pos_Currency_Master.find({}))
            for each in ll:
                web_cur = each["web"]
                gds_cur = each["gds"]
                if "" == gds_cur:
                    gds_cur = web_cur
                pos_curr[each['pos']] = {"gds" : gds_cur, "web" : web_cur}
        count = 0
        new_fare_doc = []
        updateBaseFare = 0
        updatedTotalFare = 0
        if type_ == "update":
            rank = 9999
            rank_rt = 9999
            base_fare_rank = 9999
            base_fare_rank_rt = 9999
            previous_rbd = ""
            previous_rbd_rt = ""
            base_fb_rbd_dict_ow = dict()
            base_fb_rbd_dict_rt = dict()
            print "base_fare_brand"
            print base_fare_brand
            print fare_brand_currency
            for doc_ in sorted(res[each_fare], key=lambda i: i['rbd']):
                # for doc_ in sorted(res["AMMAMMDXBY"], key = lambda i: i['rbd']) :
                try:
                    channel, farebrand = get_fare_brand_and_channel_from_fbc(doc_['fbc'],
                                                                             fbc_standardation[0]["position_array"])
                    doc_.update({'channel': channel})
                    doc_.update({'farebrand': farebrand})

                    if markets['screen'] == "faredashboard" or markets['screen'] == "workflow":
                        if doc_["selected_fare"] == True:
                            doc_.update({"channel_fb": (doc_['channel'] + " " + doc_['farebrand'])
                                        .replace("Web Lite", "1")
                                        .replace("TA Lite", "2")
                                        .replace("Web Value", "3")
                                        .replace("TA Value", "4")
                                        .replace("GDS Value", "5")
                                        .replace("TA FLY+Visa", "6")
                                        .replace("Web Flex", "7")
                                        .replace("TA Flex", "8")
                                        .replace("GDS GDS Flex", "9")
                                        .replace("Web Business", "1")
                                        .replace("GDS Business", "2")})

                            if "owrt" in doc_:
                                doc_["ow_rt"] = doc_["owrt"]
                            else:
                                doc_["owrt"] = doc_["ow_rt"]
                            # print doc_['rbd'], doc_['fbc'], doc_['channel'], doc_['farebrand'], doc_['channel_fb']
                            # To avoid any other fare brands then usual fare brands
                            tt = int(doc_['channel_fb'])
                            if doc_["ow_rt"] == "1":
                                base_fb_rbd_dict_ow.update({doc_['rbd']: doc_})
                            else:
                                base_fb_rbd_dict_rt.update({doc_['rbd']: doc_})

                    else:
                        doc_.update({"channel_fb": (doc_['channel'] + " " + doc_['farebrand'])
                                    .replace("Web Lite", "1")
                                    .replace("TA Lite", "2")
                                    .replace("Web Value", "3")
                                    .replace("TA Value", "4")
                                    .replace("GDS Value", "5")
                                    .replace("TA FLY+Visa", "6")
                                    .replace("Web Flex", "7")
                                    .replace("TA Flex", "8")
                                    .replace("GDS GDS Flex", "9")
                                    .replace("Web Business", "1")
                                    .replace("GDS Business", "2")})

                        if "owrt" in doc_:
                            doc_["ow_rt"] = doc_["owrt"]
                        else:
                            doc_["owrt"] = doc_["ow_rt"]
                        # print doc_['rbd'], doc_['fbc'], doc_['channel'], doc_['farebrand'], doc_['channel_fb']
                        # To avoid any other fare brands then usual fare brands
                        tt = int(doc_['channel_fb'])
                        if doc_["ow_rt"] == "1":
                            if (previous_rbd == doc_['rbd'] and doc_[
                                'farebrand'] == base_fare_brand and base_fare_rank > int(doc_['channel_fb'])):
                                base_fb_rbd_dict_ow.update({doc_['rbd']: doc_})
                                previous_rbd = doc_['rbd']
                                base_fare_rank = int(doc_['channel_fb'])
                            elif (previous_rbd != doc_['rbd'] and doc_['farebrand'] == base_fare_brand):
                                base_fb_rbd_dict_ow.update({doc_['rbd']: doc_})
                                previous_rbd = doc_['rbd']
                                base_fare_rank = int(doc_['channel_fb'])

                            elif previous_rbd != doc_['rbd'] or (previous_rbd == doc_['rbd'] and rank > int(
                                    doc_['channel_fb']) and base_fare_rank == 9999):
                                base_fb_rbd_dict_ow.update({doc_['rbd']: doc_})
                                previous_rbd = doc_['rbd']
                                rank = int(doc_['channel_fb'])
                                base_fare_rank = 9999
                        else:
                            if (previous_rbd_rt == doc_['rbd'] and doc_[
                                'farebrand'] == base_fare_brand and base_fare_rank_rt > int(doc_['channel_fb'])):
                                base_fb_rbd_dict_rt.update({doc_['rbd']: doc_})
                                previous_rbd_rt = doc_['rbd']
                                base_fare_rank_rt = int(doc_['channel_fb'])
                            elif (previous_rbd_rt != doc_['rbd'] and doc_['farebrand'] == base_fare_brand):
                                base_fb_rbd_dict_rt.update({doc_['rbd']: doc_})
                                previous_rbd_rt = doc_['rbd']
                                base_fare_rank_rt = int(doc_['channel_fb'])

                            elif previous_rbd_rt != doc_['rbd'] or (previous_rbd_rt == doc_['rbd'] and rank_rt > int(
                                    doc_['channel_fb']) and base_fare_rank_rt == 9999):
                                base_fb_rbd_dict_rt.update({doc_['rbd']: doc_})
                                previous_rbd_rt = doc_['rbd']
                                rank_rt = int(doc_['channel_fb'])
                                base_fare_rank_rt = 9999
                        # elif rank > int(doc_['channel_fb']):
                        #     base_fb_rbd_dict.update({doc_['rbd']: doc_})

                except ValueError as error:
                    # print error
                    pass

            ow_update_fares = dict()
            ow_update_add_on = dict()
            rt_update_fares = dict()
            rt_update_add_on = dict()
            _update_delta_val_ow = dict()
            _update_delta_val_rt = dict()
            base_local_currency_ow = {}
            base_local_currency_rt = {}
            print "farebrand_value_calc"
            print farebrand_value_calc
            for key in base_fb_rbd_dict_ow.keys():
                doc = base_fb_rbd_dict_ow[key]
                base_local_currency_ow.update({doc['temprbd'] : doc["currency"]})
                if markets['screen'] == "workflow":
                    delta_ = delta
                else:
                    delta_ = delta / EXCHANGE_RATE[doc["currency"]] * EXCHANGE_RATE[currency]
                if delta_type == "P":
                    currency = doc["currency"]
                    delta_ = (delta/100)*doc["originalrecmndFare"]

                if markets['screen'] == "workflow":
                    # if "Base Fare" in farebrand_value_calc and "YQ" in farebrand_value_calc and "YR" in farebrand_value_calc:
                    #     fare_amount = doc["originalrecmndFare"]
                    # elif "Base Fare" in farebrand_value_calc and "YQ" in farebrand_value_calc:
                    #     fare_amount = doc["originalrecmndFare"] - doc["currentYR"]
                    # elif "Base Fare" in farebrand_value_calc:
                    #     fare_amount = doc["originalrecmndFare"] - doc["currentYQ"] - doc["currentYR"]
                    if "Base Fare" in farebrand_value_calc and "YQ" in farebrand_value_calc and "YR" in farebrand_value_calc:
                        fare_amount = doc["recoBaseFare"] + doc["currentYQ"] + doc["currentYR"]
                    elif "Base Fare" in farebrand_value_calc and "YQ" in farebrand_value_calc:
                        fare_amount = doc["recoBaseFare"] + doc["currentYQ"]
                    elif "Base Fare" in farebrand_value_calc:
                        fare_amount = doc["recoBaseFare"]
                    print "fare_amount"
                    print fare_amount
                    if delta_type == "P":
                        currency = doc["currency"]
                        delta_ = (delta / 100) * fare_amount
                    add_on, RBD_min_fare = get_add_on_doc(base_baggage, add_od_doc, RBD_sellup,
                                                          fare_amount + delta_, doc["currency"],
                                                          FCR_currency, "ow",
                                                          compartment, EXCHANGE_RATE, fare_brand_currency)
                    ow_update_fares[key] = fare_brand_formula_sign(fare_brand_formula, add_on[doc['temprbd']]['ow'],
                                                                   fare_amount + delta_, doc['channel'],
                                                                   doc['farebrand'],
                                                                   doc["currency"], flag, channel_fb)
                    ow_update_add_on[key] = add_on[doc['temprbd']]['ow']

                else:
                    if "Base Fare" in farebrand_value_calc and "YQ" in farebrand_value_calc and "YR" in farebrand_value_calc:
                        fare_amount = doc["originalrecmndFare"] + doc["yq"] + doc["yr"]
                    elif "Base Fare" in farebrand_value_calc and "YQ" in farebrand_value_calc:
                        fare_amount = doc["originalrecmndFare"] + doc["yq"]
                    elif "Base Fare" in farebrand_value_calc:
                        fare_amount = doc["originalrecmndFare"]

                    # print "fare_amount"
                    # print fare_amount
                    if delta_type == "P":
                        currency = doc["currency"]
                        delta_ = (delta / 100) * doc["originalrecmndFare"]
                        _update_delta_val_ow[key] = {"delta" : (delta / 100) * doc["originalrecmndFare"], "base_fare" : doc["originalrecmndFare"]}

                    add_on, RBD_min_fare = get_add_on_doc(base_baggage, add_od_doc, RBD_sellup,
                                                          fare_amount + delta_, doc["currency"],
                                                          FCR_currency, "ow",
                                                          compartment, EXCHANGE_RATE, fare_brand_currency)
                    ow_update_fares[key] = fare_brand_formula_sign(fare_brand_formula, add_on[doc['temprbd']]['ow'],
                                                                   fare_amount + delta_, doc['channel'],
                                                                   doc['farebrand'],
                                                                   doc["currency"], flag, channel_fb)
                    ow_update_add_on[key] = add_on[doc['temprbd']]['ow']
                    # add_on, RBD_min_fare = get_add_on_doc(base_baggage, add_od_doc, RBD_sellup,
                    #                                       doc["originalrecmndFare"] + delta_, doc["currency"], FCR_currency, "ow",
                    #                                       compartment, EXCHANGE_RATE, fare_brand_currency)
                    # ow_update_fares[key] = fare_brand_formula_sign(fare_brand_formula, add_on[doc['temprbd']]['ow'],
                    #                                                doc["originalrecmndFare"] + delta_, doc['channel'],
                    #                                                doc['farebrand'],
                    #                                                doc["currency"], flag, channel_fb)
                    # ow_update_add_on[key] = add_on[doc['temprbd']]['ow']


            for key in base_fb_rbd_dict_rt.keys():
                doc = base_fb_rbd_dict_rt[key]
                base_local_currency_rt.update({doc['temprbd'] : doc["currency"]})

                if markets['screen'] == "workflow":
                    delta_ = delta
                else:
                    delta_ = delta / EXCHANGE_RATE[doc["currency"]] * EXCHANGE_RATE[currency]

                if delta_type == "P":
                    currency = doc["currency"]
                    delta_ = (delta/100)*doc["originalrecmndFare"]

                if markets['screen'] == "workflow":
                    if "Base Fare" in farebrand_value_calc and "YQ" in farebrand_value_calc and "YR" in farebrand_value_calc:
                        fare_amount = doc["recoBaseFare"] + doc["currentYQ"] + doc["currentYR"]
                    elif "Base Fare" in farebrand_value_calc and "YQ" in farebrand_value_calc:
                        fare_amount = doc["recoBaseFare"] + doc["currentYQ"]
                    elif "Base Fare" in farebrand_value_calc:
                        fare_amount = doc["recoBaseFare"]
                    # if "Base Fare" in farebrand_value_calc and "YQ" in farebrand_value_calc and "YR" in farebrand_value_calc:
                    #     fare_amount = doc["originalrecmndFare"]
                    # elif "Base Fare" in farebrand_value_calc and "YQ" in farebrand_value_calc:
                    #     fare_amount = doc["originalrecmndFare"] - doc["currentYR"]
                    # elif "Base Fare" in farebrand_value_calc:
                    #     fare_amount = doc["originalrecmndFare"]  - doc["currentYQ"] - doc["currentYR"]

                    if delta_type == "P":
                        currency = doc["currency"]
                        delta_ = (delta / 100) * fare_amount
                    add_on, RBD_min_fare = get_add_on_doc(base_baggage, add_od_doc, RBD_sellup,
                                                          fare_amount + delta_, doc["currency"],
                                                          FCR_currency, "rt",
                                                          compartment, EXCHANGE_RATE, fare_brand_currency)
                    rt_update_fares[key] = fare_brand_formula_sign(fare_brand_formula, add_on[doc['temprbd']]['rt'],
                                                                   fare_amount + delta_, doc['channel'],
                                                                   doc['farebrand'],
                                                                   doc["currency"], flag, channel_fb)
                    rt_update_add_on[key] = add_on[doc['temprbd']]['rt']


                else:
                    if "Base Fare" in farebrand_value_calc and "YQ" in farebrand_value_calc and "YR" in farebrand_value_calc:
                        fare_amount = doc["originalrecmndFare"] + doc["yq"] + doc["yr"]
                    elif "Base Fare" in farebrand_value_calc and "YQ" in farebrand_value_calc:
                        fare_amount = doc["originalrecmndFare"] + doc["yq"]
                    elif "Base Fare" in farebrand_value_calc:
                        fare_amount = doc["originalrecmndFare"]

                    if delta_type == "P":
                        currency = doc["currency"]
                        delta_ = (delta / 100) * doc["originalrecmndFare"]
                        _update_delta_val_rt[key] = {"delta" : (delta / 100) * doc["originalrecmndFare"], "base_fare" : doc["originalrecmndFare"]}
                    add_on, RBD_min_fare = get_add_on_doc(base_baggage, add_od_doc, RBD_sellup,
                                                          fare_amount + delta_, doc["currency"],
                                                          FCR_currency, "rt",
                                                          compartment, EXCHANGE_RATE, fare_brand_currency)
                    rt_update_fares[key] = fare_brand_formula_sign(fare_brand_formula, add_on[doc['temprbd']]['rt'],
                                                                   fare_amount + delta_, doc['channel'],
                                                                   doc['farebrand'],
                                                                   doc["currency"], flag, channel_fb)
                    rt_update_add_on[key] = add_on[doc['temprbd']]['rt']


                    # add_on, RBD_min_fare = get_add_on_doc(base_baggage, add_od_doc, RBD_sellup,
                    #                                       doc["originalrecmndFare"] + delta_, doc["currency"], FCR_currency, "rt",
                    #                                       compartment, EXCHANGE_RATE, fare_brand_currency)
                    # rt_update_fares[key] = fare_brand_formula_sign(fare_brand_formula, add_on[doc['temprbd']]['rt'],
                    #                                                doc["originalrecmndFare"] + delta_, doc['channel'],
                    #                                                doc['farebrand'],
                    #                                                doc["currency"], flag, channel_fb)
                    # rt_update_add_on[key] = add_on[doc['temprbd']]['rt']


                # add_on, RBD_min_fare = get_add_on_doc(base_baggage, add_od_doc, RBD_sellup,
                #                                       doc["originalrecmndFare"] + delta_, doc["currency"], FCR_currency, "rt",
                #                                       compartment, EXCHANGE_RATE, fare_brand_currency)
                # rt_update_fares[key] = fare_brand_formula_sign(fare_brand_formula, add_on[doc['temprbd']]['rt'],
                #                                                doc["originalrecmndFare"] + delta_, doc['channel'],
                #                                                doc['farebrand'],
                #                                                doc["currency"], flag, channel_fb)
                # rt_update_add_on[key] = add_on[doc['temprbd']]['rt']
                # print key
                # print fare_brand_value

            print "rt_update_fares"
            print rt_update_fares
            # print "delta_"
            # print delta_
            # print ow_update_add_on
            # print ow_update_fares
            # print rt_update_add_on
            # print asc

            for doc in res[each_fare]:
                # compartment
                base_local_currency = doc["currency"]
                if doc['od_distance'] == 0:
                    doc['od_distance'] = 1

                # print "delta", str(delta)
                if markets['screen'] == "workflow":
                    # print("loop start")
                    channel, farebrand = get_fare_brand_and_channel_from_fbc(doc['fbc'],
                                                                             fbc_standardation[0]["position_array"])
                    if doc["owrt"] == "1":
                        try:
                            fare = ow_update_fares[doc['rbd']][channel + " " + farebrand] / EXCHANGE_RATE[doc["currency"]] * EXCHANGE_RATE[base_local_currency_ow[doc['rbd']]]
                            fee_detail = ow_update_add_on[doc['rbd']]
                        except KeyError as error:
                            print error
                            fare = doc['fare']
                            fee_detail = []
                    else:
                        try:
                            fare = rt_update_fares[doc['rbd']][channel + " " + farebrand] / EXCHANGE_RATE[doc["currency"]] * EXCHANGE_RATE[base_local_currency_rt[doc['rbd']]]
                            fee_detail = rt_update_add_on[doc['rbd']]
                        except KeyError as error:
                            print error
                            fare = doc['fare']
                            fee_detail = []

                    if "Base Fare" in farebrand_value_calc and "YQ" in farebrand_value_calc and "YR" in farebrand_value_calc:
                        base_fare = fare - doc["currentYQ"] - doc["currentYR"]
                        # fare
                    elif "Base Fare" in farebrand_value_calc and "YQ" in farebrand_value_calc:
                        base_fare = fare - doc["currentYQ"]
                    elif "Base Fare" in farebrand_value_calc:
                        base_fare = fare
                    else:
                        base_fare = 0

                    doc.update({'editbasefare' : round(base_fare,1)})
                                # fare - doc['currentYQ'] - doc['currentSurcharges']})
                    doc.update({'recoTotalFare' : round(base_fare + doc["currentYR"] + doc['currentYQ'] + doc['currentSurcharges'] + doc['currentTax'],1)})
                    doc.update({'recmndFare' : round(base_fare + doc['currentYQ'] + doc['currentYR'] + doc['currentSurcharges'], 1)})
                    doc.update({'recmndYield' : round((((base_fare + doc['currentYQ'] + doc['currentYR'] + doc['currentSurcharges'])/ EXCHANGE_RATE["AED"] *
                                                        EXCHANGE_RATE[doc["currency"]]) / doc['od_distance']) * 100, 1)})
                    doc.update({'fee_detail' : fee_detail})
                    fare_doc.append(doc)

                else:
                    # print doc['fbc']
                    channel, farebrand = get_fare_brand_and_channel_from_fbc(doc['fbc'],
                                                                             fbc_standardation[0]["position_array"])

                    if doc["ow_rt"] == "1":
                        try:
                            fare = ow_update_fares[doc['rbd']][channel + " " + farebrand]  / EXCHANGE_RATE[doc["currency"]] * EXCHANGE_RATE[base_local_currency_ow[doc['rbd']]]
                            fee_detail = ow_update_add_on[doc['rbd']]
                        except KeyError as error:
                            print error
                            fare = doc['fare']
                            fee_detail = []
                    else:
                        try:
                            fare = rt_update_fares[doc['rbd']][channel + " " + farebrand] / EXCHANGE_RATE[doc["currency"]] * EXCHANGE_RATE[base_local_currency_rt[doc['rbd']]]
                            fee_detail = rt_update_add_on[doc['rbd']]
                        except KeyError as error:
                            print error
                            fare = doc['fare']
                            fee_detail = []

                    if "Base Fare" in farebrand_value_calc and "YQ" in farebrand_value_calc and "YR" in farebrand_value_calc:
                        base_fare = fare - doc["yq"] - doc["yr"]

                    elif "Base Fare" in farebrand_value_calc and "YQ" in farebrand_value_calc:
                        base_fare = fare - doc["yq"]
                    elif "Base Fare" in farebrand_value_calc:
                        base_fare = fare
                    else:
                        base_fare = 0

                    if "selected_fare" in doc:
                        # print str(_update_delta_val_ow[doc["rbd"]]['delta']) + " " + str(
                        #     _update_delta_val_ow[doc["rbd"]]['base_fare'])
                        # print doc_["selected_fare"]
                        if delta_type == "P" and doc["selected_fare"] == True:
                            if doc["ow_rt"] == "1":
                                base_fare = _update_delta_val_ow[doc["rbd"]]['delta'] + _update_delta_val_ow[doc["rbd"]]['base_fare']
                            else:
                                base_fare =_update_delta_val_rt[doc["rbd"]]['delta'] + _update_delta_val_rt[doc["rbd"]]['base_fare']
                    else:
                        # print str(_update_delta_val_ow[doc["rbd"]]['delta']) + " " + str(_update_delta_val_ow[doc["rbd"]]['base_fare'])
                        if delta_type == "P" and  base_fare_channel+" "+ base_fare_brand == channel + " " + farebrand:
                            if doc["ow_rt"] == "1":

                                base_fare = _update_delta_val_ow[doc["rbd"]]['delta'] + \
                                            _update_delta_val_ow[doc["rbd"]]['base_fare']
                            else:
                                base_fare = _update_delta_val_rt[doc["rbd"]]['delta'] + \
                                            _update_delta_val_rt[doc["rbd"]]['base_fare']
                    # _update_delta_val['key']['ow']['delta']
                    # _update_delta_val['key']['ow']['basefare']
                    yield_ = round(((((
                        base_fare + doc['yq'] + doc['yr'] + doc['surcharge']) / EXCHANGE_RATE["AED"] *
                                                        EXCHANGE_RATE[doc["currency"]]) / doc['od_distance']) * 100), 1)
                    # doc['recyield'] = yield_
                    # doc['updateBaseFare'] = fare
                    # print str(fare) + ", " + str(doc['yq']) + ", " + str(doc['yr']) + ", " + str(
                    #     doc['tax']) + ", " + str(doc['surcharge'])
                    # doc['updatedTotalFare'] = round(
                    #     round(fare) + round(doc['yq']) + round(doc['yr']) + round(doc['tax']) + round(doc['surcharge']))

                    doc.update({"new_fare" : round(base_fare, 0)})
                    doc.update({"fee_detail" : fee_detail})
                    doc.update({"recyield" : yield_})
                    doc.update({"updateBaseFare" : round(base_fare,0)})
                    doc.update({"updatedTotalFare" : round(
                        base_fare + doc['yq'] + doc['yr'] + doc['tax'] + doc['surcharge'], 0)})


                    if doc['fare'] == doc['updateBaseFare']:
                        doc.update({'farestatus': "S"})
                    elif doc['fare'] > doc['updateBaseFare']:
                        doc.update({'farestatus': "D"})
                    else:
                        doc.update({'farestatus': "I"})

                    fare_doc.append(doc)
        else:
            for doc in res[each_fare]:

                if doc['od_distance'] == 0:
                    doc['od_distance'] = 1
                delta_ = delta / EXCHANGE_RATE[doc["currency"]] * EXCHANGE_RATE[currency]
                if "applied_fare_basis" in doc:
                    doc.update({"fbc__": doc['applied_fare_basis'].strip()})
                else:
                    doc.update({"fbc__": doc['farebasis'].strip()})
                channel, fare_brand = get_fare_brand_and_channel_from_fbc(doc['fbc__'],
                                                                          fbc_standardation[0][
                                                                              "position_array"])
                print "channel, fare_brand"
                print channel, fare_brand
                if channel != "" and fare_brand != "":
                    if markets['screen'] == "workflow":
                        fbc = deepcopy(doc['farebasis'])
                        # print("loop start")
                        # Default parameter in case if we don't have any farebrand coming from API request
                        # if "applied_fare_basis" in doc:
                        #     doc.update({"fbc__": doc['applied_fare_basis'].strip()})
                        # else:
                        #     doc.update({"fbc__": doc['farebasis'].strip()})
                        # channel, fare_brand = get_fare_brand_and_channel_from_fbc(doc['fbc__'],
                        #                                                           fbc_standardation[0][
                        #                                                               "position_array"])
                        if "Base Fare" in farebrand_value_calc and "YQ" in farebrand_value_calc and "YR" in farebrand_value_calc:
                            fare_amount = doc["recoBaseFare"] + doc["currentYQ"] + doc["currentYR"]
                        elif "Base Fare" in farebrand_value_calc and "YQ" in farebrand_value_calc:
                            fare_amount = doc["recoBaseFare"] + doc["currentYQ"]
                        elif "Base Fare" in farebrand_value_calc:
                            fare_amount = doc["recoBaseFare"]

                        if delta_type == "P":
                            currency = doc["currency"]
                            delta_ = (delta / 100) * fare_amount

                        if doc["owrt"] == "1":
                            # if delta_type == "P":
                            #     currency = doc["currency"]
                            #     delta_ = (delta / 100) * doc["originalrecmndFare"]
                            doc['rbd'] = doc['temprbd']
                            add_on, RBD_min_fare = get_add_on_doc(base_baggage, add_od_doc, RBD_sellup,
                                                                  fare_amount + delta_, doc["currency"], FCR_currency, "ow",
                                                                  compartment, EXCHANGE_RATE, fare_brand_currency)
                            fare_brand_value = fare_brand_formula_sign(fare_brand_formula, add_on[doc['temprbd']]['ow'],
                                                                       fare_amount + delta_, channel, fare_brand,
                                                                       doc["currency"], flag, channel_fb)
                            print fare_brand_value
                            country = fbc[-3:-1]
                            fee_detail = add_on[doc['rbd']]['ow']
                            for key, value in fare_brand_value.items():
                                docs = deepcopy(doc)
                                fare = value
                                docs.update({'channel': key.split(" ", 2)[0]})
                                if key.split(" ", 2)[0] == "Web":
                                    docs.update({'ruleId': "62" + country})
                                    docs.update({'currentYR': 0})
                                elif key.split(" ", 2)[0] == "GDS":
                                    docs.update({'ruleId': "01" + country})
                                else:
                                    docs.update({'ruleId': "VA" + country})

                                docs.update({'fare_brand': key.split(" ", 1)[1]})
                                fare_brand = key.split(" ", 1)[1]
                                channel = key.split(" ", 2)[0]

                                if key.split(" ", 2)[0] == "GDS":
                                    fare = (fare / EXCHANGE_RATE[pos_curr[doc['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]])
                                    docs.update({'currency': pos_curr[doc['pos']]['gds']})
                                    # doc.update({'currentYQ': doc['currentYQ'] / EXCHANGE_RATE[pos_curr[doc['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})
                                    # doc.update({'currentYR': doc['currentYR'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})
                                    # doc.update({'currentSurcharges': doc['currentSurcharges'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})
                                    # doc.update({'currentTax': doc['currentTax'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})

                                    docs.update({'currentYQ': docs['currentYQ'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})
                                    docs.update({'currentYR': docs['currentYR'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})
                                    docs.update({'currentSurcharges': docs['currentSurcharges'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})
                                    docs.update({'currentTax': docs['currentTax'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})
                                    # doc["currency"] currentTax currentSurcharges
                                    # doc["currentYQ"]
                                else:
                                    fare = (fare / EXCHANGE_RATE[pos_curr[doc['pos']]['web']] * EXCHANGE_RATE[
                                        doc["currency"]])
                                    docs.update({'currency': pos_curr[doc['pos']]['web']})
                                    # doc.update({'currentYQ': doc['currentYQ'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})
                                    # doc.update({'currentYR': doc['currentYR'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})
                                    # doc.update({'currentSurcharges': doc['currentSurcharges'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})
                                    # doc.update({'currentTax': doc['currentTax'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})

                                    docs.update({'currentYQ': docs['currentYQ'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})
                                    docs.update({'currentYR': docs['currentYR'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})
                                    docs.update({'currentSurcharges': docs['currentSurcharges'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})
                                    docs.update({'currentTax': docs['currentTax'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})


                                if "Base Fare" in farebrand_value_calc and "YQ" in farebrand_value_calc and "YR" in farebrand_value_calc:
                                    base_fare = fare - docs["currentYQ"] - docs["currentYR"]
                                    # fare
                                elif "Base Fare" in farebrand_value_calc and "YQ" in farebrand_value_calc:
                                    base_fare = fare - docs["currentYQ"]
                                elif "Base Fare" in farebrand_value_calc:
                                    base_fare = fare
                                else:
                                    base_fare = 0

                                # fare - doc['currentYR'] - doc['currentYQ'] - doc['currentSurcharges']
                                docs.update({'editbasefare': round(base_fare, 0) })
                                docs.update({'currentBaseFare': round(base_fare, 0) })
                                docs.update(
                                    {'recoTotalFare': round(base_fare + docs["currentYR"] + docs['currentYQ'] + docs['currentSurcharges'] + docs['currentTax'] ,0)})
                                docs.update(
                                    {'currentTotalFare': round(base_fare + docs["currentYR"] + docs['currentYQ'] + docs['currentSurcharges'] + docs['currentTax'] ,0)})

                                docs.update({'recmndFare': round(base_fare + docs['currentYQ'] + docs['currentYR'] + docs['currentSurcharges'], 2) })
                                docs.update({'currentFare': round(base_fare + docs['currentYQ'] + docs['currentYR'] + docs['currentSurcharges'], 0)})
                                docs.update({'originalrecmndFare': round(base_fare + docs['currentYQ'] + docs['currentYR'] + docs['currentSurcharges'], 0)})
                                docs.update({'recmndYield': round(((((base_fare + docs['currentYQ'] + docs['currentYR'] + docs['currentSurcharges'])/ EXCHANGE_RATE["AED"] * EXCHANGE_RATE[docs["currency"]]) / docs['od_distance']) * 100), 0)})
                                docs.update({'fee_detail': fee_detail})
                                docs.update({'farebasis': depth_fb(docs, fbc_standardation[0], country, add_on, channel,
                                                                   fare_brand, compartment)})
                                fare_doc.append(docs)

                        else:
                            # if "fare_brand" in markets:
                            #     fare_brand = markets['fare_brand']
                            #     channel = markets['channel']
                            # else:
                            #     fare_brand = "Lite"
                            #     channel = "Web"
                            # if delta_type == "P":
                            #     currency = doc["currency"]
                            #     delta_ = (delta / 100) * doc["originalrecmndFare"]

                            doc['rbd'] = doc['temprbd']

                            add_on, RBD_min_fare = get_add_on_doc(base_baggage, add_od_doc, RBD_sellup,
                                                                  fare_amount + delta_, doc["currency"], FCR_currency, "rt",
                                                                  compartment, EXCHANGE_RATE, fare_brand_currency)
                            fare_brand_value = fare_brand_formula_sign(fare_brand_formula, add_on[doc['temprbd']]['rt'],
                                                                       fare_amount + delta_, channel, fare_brand,
                                                                       doc["currency"], flag, channel_fb)
                            # print fare_brand_value
                            fee_detail = add_on[doc['rbd']]['rt']
                            country = fbc[-3:-1]
                            print fare_brand_value
                            for key, value in fare_brand_value.items():
                                docs = deepcopy(doc)
                                fare = value
                                if key.split(" ", 2)[0] == "Web":
                                    docs.update({'ruleId': "62" + country})
                                    docs.update({'currentYR': 0})
                                elif key.split(" ", 2)[0] == "GDS":
                                    docs.update({'ruleId': "01" + country})
                                else:
                                    docs.update({'ruleId': "VA" + country})
                                docs.update({'channel': key.split(" ", 2)[0]})
                                docs.update({'fare_brand': key.split(" ", 1)[1]})
                                fare_brand = key.split(" ", 1)[1]
                                channel = key.split(" ", 2)[0]
                                if key.split(" ", 2)[0] == "GDS":
                                    fare = (fare / EXCHANGE_RATE[pos_curr[doc['pos']]['gds']] * EXCHANGE_RATE[
                                        doc["currency"]])
                                    docs.update({'currency': pos_curr[doc['pos']]['gds']})
                                    # doc.update({'currentYQ': doc['currentYQ'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})
                                    # doc.update({'currentYR': doc['currentYR'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})
                                    # doc.update({'currentSurcharges': doc['currentSurcharges'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})
                                    # doc.update({'currentTax': doc['currentTax'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})

                                    docs.update({'currentYQ': docs['currentYQ'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})
                                    docs.update({'currentYR': docs['currentYR'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})
                                    docs.update({'currentSurcharges': docs['currentSurcharges'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})
                                    docs.update({'currentTax': docs['currentTax'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})
                                    # doc["currency"] currentTax currentSurcharges
                                    # doc["currentYQ"]
                                else:
                                    fare = (fare / EXCHANGE_RATE[pos_curr[doc['pos']]['web']] * EXCHANGE_RATE[
                                        doc["currency"]])
                                    docs.update({'currency': pos_curr[doc['pos']]['web']})
                                    # doc.update({'currentYQ': doc['currentYQ'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})
                                    # doc.update({'currentYR': doc['currentYR'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})
                                    # doc.update({'currentSurcharges': doc['currentSurcharges'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})
                                    # doc.update({'currentTax': doc['currentTax'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})

                                    docs.update({'currentYQ': docs['currentYQ'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})
                                    docs.update({'currentYR': docs['currentYR'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})
                                    docs.update({'currentSurcharges': docs['currentSurcharges'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})
                                    docs.update({'currentTax': docs['currentTax'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})

                                if "Base Fare" in farebrand_value_calc and "YQ" in farebrand_value_calc and "YR" in farebrand_value_calc:
                                    base_fare = fare - docs["currentYQ"] - docs["currentYR"]
                                    # fare
                                elif "Base Fare" in farebrand_value_calc and "YQ" in farebrand_value_calc:
                                    base_fare = fare - docs["currentYQ"]
                                elif "Base Fare" in farebrand_value_calc:
                                    base_fare = fare
                                else:
                                    base_fare = 0

                                # fare - doc['currentYR'] - doc['currentYQ'] - doc['currentSurcharges']
                                docs.update({'editbasefare': round(base_fare, 0) })
                                docs.update({'currentBaseFare': round(base_fare, 0)})
                                docs.update(
                                    {'recoTotalFare': round(base_fare + docs["currentYR"] + docs['currentYQ'] + docs['currentSurcharges'] + docs['currentTax'], 0) })
                                docs.update(
                                    {'currentTotalFare': round(base_fare + docs["currentYR"] + docs['currentYQ'] + docs['currentSurcharges'] + docs['currentTax'], 0) })

                                docs.update({'recmndFare': round(base_fare + docs['currentYQ'] + docs['currentYR'] + docs['currentSurcharges'], 2) })
                                docs.update({'currentFare': round(base_fare + docs['currentYQ'] + docs['currentYR'] + docs['currentSurcharges'], 0)})
                                docs.update({'originalrecmndFare': round(base_fare + docs['currentYQ'] + docs['currentYR'] + docs['currentSurcharges'], 0)})
                                docs.update({'recmndYield': round(((((base_fare + docs['currentYQ'] + docs['currentYR'] + docs['currentSurcharges'])/ EXCHANGE_RATE["AED"] * EXCHANGE_RATE[docs["currency"]]) / docs['od_distance']) * 100), 0)})
                                docs.update({'fee_detail': fee_detail})
                                docs.update({'farebasis': depth_fb(docs, fbc_standardation[0], country, add_on, channel,
                                                                   fare_brand, compartment)})
                                fare_doc.append(docs)

                    else:
                        # Replace promo channel into normal
                        original_channel = deepcopy(channel)
                        channel = channel.replace(" Promo", "")
                        temp_dummy_rbd_for_screen = doc['rbd']
                        doc.update({'rbd': doc['applied_fare_basis'].strip()[:1]})
                        print doc['applied_fare_basis'].strip()[:1]
                        print (doc['fare'] + delta)
                        # print doc['rbd']
                        # temp_dummy_rbd_for_screen = doc['applied_fare_basis'].strip()
                        if doc["ow_rt"] == "1":
                            if delta_type == "P":
                                currency = doc["currency"]
                                delta_ = (delta / 100) * doc["fare"]
                            doc["owrt"] = "1"
                            add_on, RBD_min_fare = get_add_on_doc(base_baggage, add_od_doc, RBD_sellup,
                                                                  doc['fare'] + delta_, doc["currency"], FCR_currency, "ow",
                                                                  compartment, EXCHANGE_RATE, fare_brand_currency)
                            fee_detail = add_on[doc['rbd']]['ow']
                            fare_brand_value = fare_brand_formula_sign(fare_brand_formula, add_on[doc['rbd']]['ow'],
                                                                       doc['fare'] + delta_, channel,
                                                                       fare_brand,
                                                                       doc["currency"], flag, channel_fb)
                            # print fare_brand_value
                            fbc = deepcopy(doc['applied_fare_basis'])
                            check_special_fare = fbc[2]
                            try:
                                fare_letter = int(check_special_fare)
                                check_special_fare = ""
                            except ValueError as error:
                                print error
                                # check_special_fare

                            print "check_special_fare"
                            print check_special_fare
                            country = doc['fbc'][-3:-1]
                            base_fare = doc['fare']
                            # print fare_brand_value
                            if compartment == "Y":
                                del fare_brand_value['TA FLY+Visa']
                                del fare_brand_value['TA Lite']
                                del fare_brand_value['TA Flex']
                                del fare_brand_value['TA Value']
                            for key, value in fare_brand_value.items():
                                docs = deepcopy(doc)
                                fare = value
                                if key.split(" ", 2)[0] == "Web":
                                    docs.update({'ruleid': "62" + country})
                                    docs.update({'yr': 0})
                                elif key.split(" ", 2)[0] == "GDS":
                                    docs.update({'ruleid': "01" + country})
                                else:
                                    docs.update({'ruleid': "VA" + country})

                                channel = key.split(" ", 2)[0]
                                channel_ = channel.replace("Web", "Web Promo").replace("TA", "TA Promo")
                                docs.update({'channel': channel_})
                                docs.update({'fare_brand': key.split(" ", 1)[1]})
                                fare_brand = key.split(" ", 1)[1]
                                if key.split(" ", 2)[0] == "GDS":
                                    fare = (fare / EXCHANGE_RATE[pos_curr[doc['pos']]['gds']] * EXCHANGE_RATE[
                                        doc["currency"]])
                                    docs.update({'currency': pos_curr[doc['pos']]['gds']})
                                    # doc.update({'yq': doc['yq'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})
                                    # doc.update({'yr': doc['yr'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})
                                    # doc.update({'surcharge': doc['surcharge'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})
                                    # doc.update({'tax': doc['tax'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})

                                    docs.update({'yq': docs['yq'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})
                                    docs.update({'yr': docs['yr'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})
                                    docs.update({'surcharge': docs['surcharge'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})
                                    docs.update({'tax': docs['tax'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})
                                    # doc["currency"] currentTax currentSurcharges
                                    # doc["currentYQ"]
                                else:
                                    fare = (fare / EXCHANGE_RATE[pos_curr[doc['pos']]['web']] * EXCHANGE_RATE[
                                        doc["currency"]])
                                    docs.update({'currency': pos_curr[doc['pos']]['web']})
                                    # doc.update({'yq': doc['yq'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})
                                    # doc.update({'yr': doc['yr'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})
                                    # doc.update({'surcharge': doc['surcharge'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})
                                    # doc.update({'tax': doc['tax'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})

                                    docs.update({'yq': docs['yq'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})
                                    docs.update({'yr': docs['yr'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})
                                    docs.update({'surcharge': docs['surcharge'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})
                                    docs.update({'tax': docs['tax'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})

                                docs.update({'editbasefare': round(fare, 1)})
                                docs.update({"updateBaseFare": round((fare - docs['yr']), 1)})
                                docs.update(
                                    {'updatedTotalFare': round(fare + docs['yr'] + docs['yq'] + docs['tax'] + docs[
                                        'surcharge'], 1)})
                                docs.update(
                                    {'recoTotalFare': round(fare + docs['yr'] + docs['yq'] + docs['tax'] + docs[
                                        'surcharge'], 1)})
                                docs.update({'recmndFare': round(fare + docs['yr'] + docs['yq'], 1)})
                                docs.update({'recmndYield': round(((((fare + docs['yr'] + docs['yq'])/ EXCHANGE_RATE["AED"] * EXCHANGE_RATE[docs["currency"]]) / docs['od_distance']) * 100), 1)})
                                docs.update({'fee_detail': fee_detail})

                                fbc_new = depth_fb(docs, fbc_standardation[0], country, add_on, channel_,
                                                   fare_brand, compartment)
                                # fbc_new = doc['applied_fare_basis']
                                # print fbc_new
                                fare_brand_array = [x for x in list(fbc_standardation[0]['position_array'][6]['value_array']) if
                                              x['value'] == fare_brand.upper()]

                                channel_array = [x for x in list(fbc_standardation[0]['position_array'][8]['value_array']) if
                                             channel_.lower() in x['value'].lower()]

                                if channel_.lower() == "gds" and fare_brand.lower() == "value" or channel_.lower() == "gds" and fare_brand.lower() == "business":
                                    docs.update({'applied_fare_basis': fbc_new.replace('OW1', "OWPR").replace('OW2', "OWPR")})
                                else:
                                    list_of_nums = map(int, re.findall('\d+', docs['applied_fare_basis']))
                                    print docs['applied_fare_basis'], list_of_nums, channel_, fare_brand, str(
                                        channel_array[0]['key']), str(fare_brand_array[0]['key'])
                                    docs.update({'applied_fare_basis': docs['applied_fare_basis'].replace(
                                        str(list_of_nums[0]), str(fare_brand_array[0]['key']))
                                                .replace(str(list_of_nums[1]), str(channel_array[0]['key']))
                                                 })
                                    print docs['applied_fare_basis']

                                if docs['applied_fare_basis'] == fbc:
                                    docs.update({'fare': base_fare})
                                else:
                                    docs.update({'fare': 0})
                                    docs.update({'fbc': ""})
                                    docs.update({'totalFare': round(docs['yr'] + docs['yq'] + docs['tax'] + docs[
                                        'surcharge'])})

                                docs.update({'rbd': temp_dummy_rbd_for_screen})

                                fare_doc.append(docs)
                        else:
                            if delta_type == "P":
                                currency = doc["currency"]
                                delta_ = (delta / 100) * doc["fare"]
                            doc["owrt"] = "2"
                            add_on, RBD_min_fare = get_add_on_doc(base_baggage, add_od_doc, RBD_sellup,
                                                                  doc['fare'] + delta_, doc["currency"], FCR_currency, "rt",
                                                                  compartment, EXCHANGE_RATE,fare_brand_currency )
                            fee_detail = add_on[doc['rbd']]['rt']
                            fare_brand_value = fare_brand_formula_sign(fare_brand_formula, add_on[doc['rbd']]['rt'],
                                                                       doc['fare'] + delta_, channel,
                                                                       fare_brand,
                                                                       doc["currency"], flag, channel_fb)
                            # print fare_brand_value
                            fbc = deepcopy(doc['applied_fare_basis'])
                            country = doc['fbc'][-3:-1]
                            base_fare = doc['fare']
                            check_special_fare = fbc[2]
                            try:
                                fare_letter = int(check_special_fare)
                                check_special_fare = ""
                            except ValueError as error:
                                print error
                                # check_special_fare
                            if compartment == "Y":
                                del fare_brand_value['TA FLY+Visa']
                                del fare_brand_value['TA Lite']
                                del fare_brand_value['TA Flex']
                                del fare_brand_value['TA Value']
                            for key, value in fare_brand_value.items():
                                docs = deepcopy(doc)
                                fare = value
                                channel = key.split(" ", 2)[0]
                                channel_ = channel.replace("Web", "Web Promo").replace("TA", "TA Promo")
                                docs.update({'channel': channel_})
                                docs.update({'fare_brand': key.split(" ", 1)[1]})
                                fare_brand = key.split(" ", 1)[1]
                                if key.split(" ", 2)[0] == "Web":
                                    docs.update({'ruleid': "62" + country})
                                    docs.update({'yr': 0})
                                elif key.split(" ", 2)[0] == "GDS":
                                    docs.update({'ruleid': "01" + country})
                                else:
                                    docs.update({'ruleid': "VA" + country})
                                if key.split(" ", 2)[0] == "GDS":
                                    fare = (fare / EXCHANGE_RATE[pos_curr[doc['pos']]['gds']] * EXCHANGE_RATE[
                                        doc["currency"]])
                                    docs.update({'currency': pos_curr[doc['pos']]['gds']})
                                    # doc.update({'yq': doc['yq'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})
                                    # doc.update({'yr': doc['yr'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})
                                    # doc.update({'surcharge': doc['surcharge'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})
                                    # doc.update({'tax': doc['tax'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})

                                    docs.update({'yq': docs['yq'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})
                                    docs.update({'yr': docs['yr'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})
                                    docs.update({'surcharge': docs['surcharge'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})
                                    docs.update({'tax': docs['tax'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['gds']] * EXCHANGE_RATE[doc["currency"]]})
                                    # doc["currency"] currentTax currentSurcharges
                                    # doc["currentYQ"]
                                else:
                                    fare = (fare / EXCHANGE_RATE[pos_curr[doc['pos']]['web']] * EXCHANGE_RATE[
                                        doc["currency"]])
                                    docs.update({'currency': pos_curr[doc['pos']]['web']})
                                    # doc.update({'yq': doc['yq'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})
                                    # doc.update({'yr': doc['yr'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})
                                    # doc.update({'surcharge': doc['surcharge'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})
                                    # doc.update({'tax': doc['tax'] / EXCHANGE_RATE[
                                    #     pos_curr[doc['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})

                                    docs.update({'yq': docs['yq'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})
                                    docs.update({'yr': docs['yr'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})
                                    docs.update({'surcharge': docs['surcharge'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})
                                    docs.update({'tax': docs['tax'] / EXCHANGE_RATE[
                                        pos_curr[docs['pos']]['web']] * EXCHANGE_RATE[doc["currency"]]})

                                docs.update({'editbasefare': round(fare, 0)})
                                docs.update({"updateBaseFare": round((fare - docs['yr']),0)})
                                docs.update(
                                    {'recoTotalFare': round(fare + docs['yr'] + docs['yq'] + docs['tax'] + doc[
                                        'surcharge'], 0)})
                                docs.update(
                                    {'updatedTotalFare': round(fare + docs['yr'] + docs['yq'] + docs['tax'] + docs[
                                        'surcharge'], 0)})
                                docs.update({'recmndFare': round(fare + docs['yr'] + docs['yq'], 0)})
                                docs.update({'recmndYield': round(((((fare + docs['yr'] + docs['yq']) / EXCHANGE_RATE[
                                    "AED"] * EXCHANGE_RATE[docs["currency"]]) / docs['od_distance']) * 100), 1)})
                                docs.update({'fee_detail': fee_detail})

                                fare_brand_array = [x for x in
                                                    list(fbc_standardation[0]['position_array'][6]['value_array']) if
                                                    x['value'] == fare_brand.upper()]

                                channel_array = [x for x in list(fbc_standardation[0]['position_array'][8]['value_array'])
                                                 if
                                                 channel_.lower() in x['value'].lower()]
                                fbc_new = depth_fb(docs, fbc_standardation[0], country, add_on, channel_,
                                                   fare_brand, compartment)
                                if channel_.lower() == "gds" and fare_brand.lower() == "value" or channel_.lower() == "gds" and fare_brand.lower() == "business":
                                    docs.update({'applied_fare_basis': fbc_new.replace('R1Y2', "RTPR").replace('R1Y1', "RTPR")})
                                else:
                                    # print docs['applied_fare_basis'], list_of_nums, channel_, fare_brand, str(
                                    #     channel_array[0]['key']), str(fare_brand_array[0]['key'])

                                    list_of_nums = map(int, re.findall('\d+', docs['applied_fare_basis']))
                                    docs.update({'applied_fare_basis': docs['applied_fare_basis'].replace(
                                        str(list_of_nums[0]), str(fare_brand_array[0]['key']))
                                                .replace(str(list_of_nums[1]), str(channel_array[0]['key']))
                                                 })
                                    print docs['applied_fare_basis']

                                if docs['applied_fare_basis'] == fbc:
                                    docs.update({'fare': base_fare})
                                else:
                                    docs.update({'fare': 0})
                                    docs.update({'fbc': ""})
                                    docs.update({'totalFare': round(docs['yr'] + doc['yq'] + doc['tax'] + doc[
                                        'surcharge'])})
                                docs.update({'rbd': temp_dummy_rbd_for_screen})
                                fare_doc.append(docs)
                else:
                    fare_doc.append("This "+doc['fbc__']+" fare is not defined in Pricing model, Please check your requested fare" )

    trigger_list = {"fares": fare_doc}
    resp = {"message": trigger_list, "status": 200}
    # print resp
    client.close()
    return jsonify(resp)


@measure(JUPITER_LOGGER)
@app.route('/raiseManualTrigger', methods=['POST'])
def raise_manual_trigger():
    client = mongo_client()
    db = client[JUPITER_DB]
    trigger_list = ""
    print request.json
    markets = request.json
    for doc in markets:
        pos = doc['pos']
        origin = doc['origin']
        destination = doc['destination']
        compartment = doc['compartment']
        dep_date_start = doc['fromDate']
        dep_date_end = doc['toDate']
        reason = doc['reason']
        work_package_name = doc['work_package_name']
        flag = doc['flag']
        obj = ManualTrigger(pos=pos,
                            origin=origin,
                            destination=destination,
                            compartment=compartment,
                            dep_date_start=dep_date_start,
                            dep_date_end=dep_date_end,
                            reason=reason,
                            work_package_name=work_package_name,
                            flag=flag)
        obj.do_analysis(db=db)
        trigger_list = trigger_list + obj.trigger_id + ","
        print 'Trigger ID', obj.trigger_id, 'Generated, Queued for Analysis'
    resp = {"message": trigger_list, "status": 200}
    client.close()
    return jsonify(resp)


@measure(JUPITER_LOGGER)
@app.route('/runStrategicQuery', methods=['POST'])
def cluster_query():
    doc = request.json[0]
    print doc
    output = [{'message': 'No query defined'}]
    db = client[JUPITER_DB]
    cur = db.JUP_DB_Market_Query.find({'user': doc['user'], 'name': doc['name']}, {'collection_name': 1,
                                                                                   'queryList': 1,
                                                                                   '_id': 0})

    for i in cur:
        collection_name = i['collection_name']
        query = i['queryList']
        print json.loads(query)
        output = db[collection_name].aggregate(json.loads(query), allowDiskUse=True)
    client.close()
    return jsonify(list(output))
    # if cur.count() == 0:
    #     client.close()
    #     return jsonify({"message": "No query defined"})
    # else:
    #     for i in cur:
    #         collection_name = i['collection_name']
    #         query = i['queryList']
    #         print json.loads(query)
    #         output = db[collection_name].aggregate(json.loads(query), allowDiskUse=True)
    #     client.close()
    #     return jsonify(list(output))


@measure(JUPITER_LOGGER)
@app.route('/raiseSystemTriggers', methods=['POST'])
def raise_system_triggers():
    print request.json
    trigger_request = request.json
    markets = trigger_request['markets']
    trigger_group = []
    counter = 0
    st = time.time()
    for mrkt in markets:
        if counter == 100:

            trigger_group.append(run_booking_triggers.s(markets=markets))
            trigger_group.append(run_pax_triggers.s(markets=markets))
            trigger_group.append(run_revenue_triggers.s(markets=markets))
            trigger_group.append(run_yield_triggers.s(markets=markets))
            trigger_group.append(run_events_triggers.s(markets=markets))
            trigger_group.append(run_opp_trend_triggers.s(markets=markets))
            markets = []
            markets.append(mrkt)
            counter = 1

        else:
            markets.append(mrkt)
            counter += 1

    if counter > 0:
        trigger_group.append(run_booking_triggers.s(markets=markets))
        trigger_group.append(run_pax_triggers.s(markets=markets))
        trigger_group.append(run_revenue_triggers.s(markets=markets))
        trigger_group.append(run_yield_triggers.s(markets=markets))
        trigger_group.append(run_events_triggers.s(markets=markets))
        trigger_group.append(run_opp_trend_triggers.s(markets=markets))

    trig_group = group(trigger_group)
    res_group = trig_group()
    trig_grp_res = res_group.get()
    print 'Total time taken to raise and analyze triggers:', time.time() - st
    resp = {"message": "Triggers Generated for {}".format(markets), "status": 200}
    return jsonify(resp)

# Getting ITA views
@measure(JUPITER_LOGGER)
@app.route('/get_ita_view', methods=['POST'])
def get_ita_view():
    st = time.time()
    doc = request.json
    origin = doc['origin']
    destination = doc['destination']
    input_fb = doc['fb']
    input_fare = float(doc['fare'])
    dep_date = doc['dep_date']

    print('getting the ita view for below : ')
    print(origin, destination, input_fb, input_fare, dep_date)

    print('starting scraping')

    options = webdriver.ChromeOptions()
    prefs = {"profile.managed_default_content_settings.images": 2, 'disk-cache-size': 4096}
    options.add_experimental_option("prefs", prefs)
    options.add_argument('--headless')

    caps = DesiredCapabilities().CHROME

    driver = webdriver.Chrome(executable_path='/app/Program_files/chromedriver_linux64/chromedriver',
                              chrome_options=options,
                              desired_capabilities=caps)
    driver.set_page_load_timeout(10)

    driver.maximize_window()

    try:
        driver.get("https://matrix.itasoftware.com/")
    except TimeoutException as e:
        print('stopping page load')
        driver.execute_script("window.stop();")

    wait = WebDriverWait(driver, 60)

    # clicking on one-way tab
    try:
        wait.until(EC.element_to_be_clickable((By.XPATH, "//div[contains(text(),'One-way')]"))).click()
    except TimeoutException as e:
        print('one-way tab could not be found')
        print(e)
        return jsonify('')
    except Exception as e:
        print('exception occured when trying to click on one-way tab')
        return jsonify('')

    # entering the origin
    try:
        origin_input = wait.until(EC.visibility_of_element_located((By.XPATH,
                                                                    '//*[@id="searchPanel-0"]/div/table/tbody/tr[2]/td/div/div[2]/div/div/div[2]/div/div/div/input')))
        for ch in origin:
            origin_input.send_keys(ch)
        time.sleep(1)

        origin_element = driver.find_element_by_xpath('//span[contains(text(), "' + origin + '")]').click()
    except TimeoutException as e:
        print('could not find the departing from input box')
        print(e)
        return jsonify('')
    except NoSuchElementException as e:
        print('the origin is not listed in the dropdown')
        print(e)
        return jsonify('')
    except Exception as e:
        print('exception occured when inputing origin')
        print(e)
        return jsonify('')

    # entering the destination
    try:
        destination_input = driver.find_element_by_xpath(
            '//*[@id="searchPanel-0"]/div/table/tbody/tr[2]/td/div/div[2]/div/div/div[4]/div/div/div/input')
        for ch in destination:
            destination_input.send_keys(ch)
        time.sleep(1)

        origin_element = driver.find_element_by_xpath('//span[contains(text(), "' + destination + '")]').click()
    except NoSuchElementException as e:
        print('the destination is not listed in the dropdown')
        print(e)
        return jsonify('')
    except TimeoutException as e:
        print('could not find the destination input box')
        print(e)
        return jsonify('')
    except Exception as e:
        print('exception occured when inputing destination')
        print(e)
        return jsonify('')

    # entering the departure date
    try:
        date_input = driver.find_element_by_xpath(
            '//*[@id="searchPanel-0"]/div/table/tbody/tr[2]/td/div/div[2]/div/div/div[9]/div[1]/div[1]/div[2]/input')
        for ch in dep_date:
            date_input.send_keys(ch)
        time.sleep(1)
    except NoSuchElementException as e:
        print('could not find departure date input box')
        print(e)
        return jsonify('')
    except Exception as e:
        print('exception occured when entering departure date')
        print(e)
        return jsonify('')

    print('class_name')
    class_name = driver.find_element_by_id('matrixWelcomeText').get_attribute('class')
    class_name = class_name[:class_name.index('-')]
    print(class_name)

    # clicking the search button
    try:
        driver.find_element_by_id("searchButton-0").click()
    except NoSuchElementException as e:
        print('could not find the search button')
        print(e)
        return jsonify('')
    except Exception as e:
        print('exception occured when trying to click search button')
        print(e)
        return jsonify('')

    # clicking emirates tab
    try:
        wait.until(EC.visibility_of_element_located((By.XPATH, '//a[contains(text(),"Emirates")]'))).click()
    except TimeoutException as e:
        print('could not find the emirate tab')
        print(e)
        return jsonify('')
    except Exception as e:
        print('exception occured when trying to click emirates tab')
        print(e)
        return jsonify('')

    def check_for_combined_service(flight):

        try:
            flight.find_element_by_xpath('.//span[contains(text(), "others")]')
        except NoSuchElementException:
            return False
        return True

    def extract_amount(fee):
        try:
            print(fee)
            fee_f = re.findall(r'[,\d]+.\d+', fee)[0]
            print(fee_f)
            fee_f = float(fee_f.replace(',', ''))
            return fee_f
        except Exception as e:
            print('exception occured when extracting amount')
            print(e)
            return jsonify('')

    # checking for the fare matching the input fare
    try:
        time.sleep(2)
        flights_list = driver.find_elements_by_xpath('//div[@class="' + class_name + '-u-j"]')
    except NoSuchElementException as e:
        print('could not find the xpath for list of flights')
        print(e)
        return jsonify('')
    except Exception as e:
        print('exception occured when trying to find all list of flights')
        print(e)
        return jsonify('')

    print(len(flights_list))

    for idx in range(len(flights_list)):

        try:
            flight = driver.find_elements_by_xpath('//div[@class="' + class_name + '-u-j"]')[idx]
        except Exception as e:
            print('exception occured when trying to get detail of a particular flight : ')
            print(e)
            return jsonify('')

        flight_detail = dict()
        flight_detail['travel_date'] = datetime.datetime.strptime(dep_date, '%m/%d/%y').strftime('%Y%m%d')

        if (not check_for_combined_service(flight)):

            # navigating to the details page
            try:
                flight.find_element_by_xpath('.//button[@class="' + class_name + '-I-c"]').click()
                time.sleep(3)
            except NoSuchElementException as e:
                print('could not find the button to navigate to the details page')
                print(e)
                return jsonify('')
            except Exception as e:
                print('exception occured when trying to navigate to the details page')
                print(e)
                return jsonify('')

            # getting from and to details
            try:
                od_day = driver.find_element_by_xpath('//div[@class="' + class_name + '-j-l"]').text
            except NoSuchElementException as e:
                print('could not find the xpath for to and from detail')
                print(e)
                return jsonify('')
            except Exception as e:
                print('exception occured when getting origin and destination detail')
                print(e)
                return jsonify('')

            # print(od_day)

            try:
                od_day = od_day.split(' ')
                flight_detail['origin'] = od_day[1][1:-1]
                flight_detail['destination'] = od_day[4][1:-1]
                flight_detail['day_of_week'] = od_day[6][:-1]
                print(flight_detail['origin'])
                print(flight_detail['destination'])
                print(flight_detail['day_of_week'])
            except Exception as e:
                print('exception occured when extracting od and day detail')
                print(e)
                return jsonify('')

            # getting other details about travel.
            try:
                details = driver.find_elements_by_xpath('//tr[@class="' + class_name + '-j-i"]/td')
            except NoSuchElementException as e:
                print('could not find the xpath to get flight_no, dep time, arrvl time details')
                print(e)
                return jsonify('')
            except Exception as e:
                print('exception occured when getting flight_no, dep time, arrvl time details')
                print(e)
                return jsonify('')

            try:
                details = [detail.text for detail in details]
                if len(details) > 0:
                    flight_detail['flight_no'] = details[0]
                    flight_detail['dep_time'] = details[1][details[1].find(':') + 1:]
                    flight_detail['arvl_time'] = details[2][details[2].find(':') + 1:]
                    flight_detail['duration'] = details[3]
                    flight_detail['flight_type'] = details[4]
                    comp = details[5].split()[0]
                    print('compartment is : ', comp)
                    if 'Economy' in comp:
                        flight_detail['compartment'] = 'Y'
                    elif 'Business' in comp:
                        flight_detail['compartment'] = 'J'
                    flight_detail['RBD'] = details[5].split()[1][1:-1]
            except Exception as e:
                print('exception occured when extracting flight_no, dep tim, arrvl time details')
                print(e)
                return jsonify('')

            # getting fare breakup detail
            try:
                fare_break_up = driver.find_elements_by_xpath(
                    '//div[@class="' + class_name + '-k-k"]/div[@class="' + class_name + '-k-m"]/table/tbody/tr/td/table/tbody/tr/td/table[1]/tbody/tr')
            except NoSuchElementException as e:
                print('could not find the xpath of fare breakup detail')
                print(e)
                return jsonify('')
            except Exception as e:
                print('exception occured when getting fare breakup details')
                print(e)
                return jsonify('')

            is_fb_detail = True
            tax_breakup = dict()
            fb_detail = None
            valid_fare = False

            for ele in fare_break_up:
                if is_fb_detail:
                    try:
                        fb_detail = ele.find_elements_by_xpath('.//tr')[0].find_element_by_xpath(
                            './/span[@class="gwt-InlineLabel"]').text
                        fb = fb_detail.split()[2]
                    except NoSuchElementException as e:
                        print('exception occured when getting the fb detail')
                        print(e)
                        return jsonify('')
                    except Exception as e:
                        print('exception occured when extracting fb details')
                        print(e)
                        return jsonify('')

                    try:
                        fee = ele.find_element_by_xpath(
                            './/td[@class="' + class_name + '-k-f"]/div[@class="gwt-Label"]').text
                        fee_f = extract_amount(fee)
                    except NoSuchElementException as e:
                        print('excepting occured when getting the fee for fb')
                        print(e)
                        return jsonify('')
                    except Exception as e:
                        print('excepting occured when extracting the fee for fb')
                        print(e)
                        return jsonify('')

                    print(fb, fee_f, type(fb), type(fee_f))
                    print(input_fb, input_fare, type(input_fb), type(input_fare))

                    if input_fb == fb and input_fare == fee_f:
                        valid_fare = True

                    if valid_fare:
                        try:
                            base_detail = ele.find_elements_by_xpath('.//tr')[1].find_element_by_xpath(
                                './/div[@class="' + class_name + '-k-c"]').text
                            base_detail += ' ,' + ele.find_elements_by_xpath('.//tr')[2].find_element_by_xpath(
                                './/div[@class="' + class_name + '-k-c"]').text
                            base_detail = base_detail.split(',')

                            print(fb)
                            print(base_detail[0].split()[-1])
                            print(base_detail[1].split()[0])
                            print(fee_f)

                            flight_detail['fb'] = fb
                            flight_detail['pax_type'] = base_detail[0].split()[-1][-1]
                            if 'ONE-WAY' in base_detail[1].split()[0]:
                                flight_detail['oneway_return'] = '1'
                            flight_detail['fare'] = fee_f
                        except NoSuchElementException as e:
                            print('exception occured when getting base details')
                            print(e)
                            return jsonify('')
                        except Exception as e:
                            print('exception occured when extracting base details')
                            print(e)
                            return jsonify('')

                        # get currency
                        try:
                            flight_detail['currency'] = re.findall(r'[^0-9,.]+', fee)[0]
                        except Exception as e:
                            print('exception occured when getting the currency')
                            print(e)
                            return jsonify('')

                    else:
                        break
                    is_fb_detail = False
                    flight_detail['total_tax'] = 0

                else:
                    try:
                        service = ele.find_element_by_xpath(
                            './/td[@class="' + class_name + '-k-g"]/div[@class="gwt-Label"]').text
                        print(service)
                    except NoSuchElementException as e:
                        print('could not find the service detail')
                        print(e)
                        return jsonify('')
                    except Exception as e:
                        print('exception occured when getting the service detail')
                        print(e)
                        return jsonify('')

                    if service == 'Subtotal per passenger':
                        xpth = './/td[@class="' + class_name + '-k-l"]/div[@class="gwt-Label"]'
                    elif service == 'Subtotal For 1 adult':
                        xpth = './/td[@class="' + class_name + '-k-o"]/div[@class="gwt-Label"]'
                    else:
                        xpth = './/td[@class="' + class_name + '-k-f"]/div[@class="gwt-Label"]'
                    try:
                        fee = ele.find_element_by_xpath(xpth).text
                    except NoSuchElementException as e:
                        print('could not find the fee detail of a service')
                        print(e)
                        return jsonify('')
                    except Exception as e:
                        print('exception occured when getting fee detail of a service')
                        print(e)
                        return jsonify('')

                    try:
                        if service == 'Number of passengers':
                            flight_detail['pax_no'] = fee
                        else:
                            fee = extract_amount(fee)
                        print(fee)

                        if '(YQ)' in service:
                            flight_detail['YQ'] = fee
                        elif '(YR)' in service:
                            flight_detail['YR'] = fee
                        elif service == 'Subtotal per passenger':
                            flight_detail['total_fare'] = fee

                        if service != 'Number of passengers' and 'Subtotal' not in service:
                            tax_breakup[service] = fee
                            if '(YQ)' not in service and '(YR)' not in service:
                                flight_detail['total_tax'] += fee

                    except Exception as e:
                        print('exception occured when extracting the fee detail of a service')
                        print(e)
                        return jsonify('')

            flight_detail['tax_breakup'] = tax_breakup
            flight_detail['source'] = 'ita'
            print('----' * 5)
            print('complete flight detail to be inserted to db: ')
            print(flight_detail)
            if valid_fare:
                # response = ''
                # for key in flight_detail:
                #     response += key + ' : ' + str(flight_detail[key]) + '</br>'
                driver.close()
                print('execution time : ' + str(time.time() - st))
                return jsonify(flight_detail)
                # db.scraping_test.insert(flight_detail)

            # going back to list of fare pages
            driver.execute_script("window.history.go(-1)")
            time.sleep(2)

            # filtering only the emirates flights
            wait.until(EC.visibility_of_element_located((By.XPATH, '//a[contains(text(),"Emirates")]'))).click()
            time.sleep(2)
        else:
            print('combined')

        print('---' * 5)

    driver.close()
    print('execution time : ' + str(time.time() - st))
    return jsonify('')

if __name__ == '__main__':
    print "Server Started..."
    print "To stop the Server press CTRL+C"
    # fare_brand_creation()
    http_server = WSGIServer(('', 5000), app)
    http_server.serve_forever()
