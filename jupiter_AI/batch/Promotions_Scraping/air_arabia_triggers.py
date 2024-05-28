import pandas as pd
from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoRuleChangeTrigger
from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoDateChangeTrigger
from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoFareChangeTrigger
from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoNewPromotionTrigger
import datetime
import traceback
import pymongo
client = pymongo.MongoClient("mongodb://analytics:KNjSZmiaNUGLmS0Bv2@13.92.251.7:42525/")
db = client['fzDB_stg']
from jupiter_AI import JUPITER_LOGGER
from jupiter_AI.logutils import measure



cursor_OD = list(db.JUP_DB_OD_Master.aggregate([{'$project': {'_id': 0, 'OD': 1}}]))
print len(cursor_OD)
city_airport = list(db.JUP_DB_City_Airport_Mapping.aggregate([{'$project': {'_id':0, 'City_Code':1, 'Airport_Code':1}}]))
print len(city_airport)



@measure(JUPITER_LOGGER)
def airport_city_map(origin,destination):
    airport_code_ori_list = []
    airport_code_dest_list = []
    for l in city_airport:
        if origin == l['Airport_Code']:
            origin = l['City_Code']
        else:
            pass
        if destination == l['Airport_Code']:
            destination = l['City_Code']
        else:
            pass
    # print origin, destination
    for k in city_airport:
        if origin == k['City_Code']:
            airport_code_ori_list.append(k['Airport_Code'])
        if destination == k['City_Code']:
            airport_code_dest_list.append(k['Airport_Code'])
    # print airport_code_ori_list, airport_code_dest_list
    # print len(airport_code_ori_list), len(airport_code_dest_list)

    if origin == "VIE":
        airport_code_ori_list = ['XWC']
    if destination == "VIE":
        airport_code_dest_list = ['XWC']
    if origin == "UAE":
        airport_code_ori_list = ['DWC', 'DXB']
    if destination == "UAE":
        airport_code_dest_list = ['DXB', 'DWC']
    if airport_code_ori_list == []:
        airport_code_ori_list = []
        airport_code_ori_list.append(data_doc['OD'][:3])
    if airport_code_dest_list == []:
        airport_code_dest_list = []
        airport_code_dest_list.append(data_doc['OD'][3:])
    # print airport_code_ori_list, airport_code_dest_list
    return airport_code_ori_list, airport_code_dest_list

print "read def1"



@measure(JUPITER_LOGGER)
def check_od_and_raise_trigger(airport_code_ori_list, airport_code_dest_list):
    print "orig_list: ", airport_code_ori_list
    print "dest_lst: ", airport_code_dest_list
    for m in range(len(airport_code_ori_list)):
        origin = airport_code_ori_list[m]
        for k in range(len(airport_code_dest_list)):
            print "k: ", k
            destination = airport_code_dest_list[k]
            data_doc['OD'] = origin + destination
            doc['OD'] = origin + destination
            print "OD going for trigger: ", data_doc['OD']
            # print "data_doc: ", data_doc
            # print "cursor:", cursor_OD
            for i in cursor_OD:
                if (data_doc['OD'] == i['OD']):
                    print "OD in list:", i['OD']
                    print "present OD:", data_doc['OD']
                    rule_trig = 0


                    @measure(JUPITER_LOGGER)
                    def rule_change():
                        rule_trig = 0
                        a = 0
                        print "rule_change_trigger_starts"
                        cursor = list(db.JUP_DB_Promotions1.aggregate([
                            {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                                        'Fare': data_doc['Fare'],
                                        'dep_date_from': data_doc['dep_date_from'],
                                        'dep_date_to': data_doc['dep_date_to']}}, {'$project': {'_id': 0}},
                            {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
                        print cursor

                        if len(cursor) > 0:
                            for i in range(len(cursor)):
                                if (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                                            cursor[i]['Currency'] == data_doc['Currency']) \
                                        and (cursor[i]['Valid till'] == data_doc['Valid till']) \
                                        and (cursor[i]['compartment'] == data_doc['compartment']):
                                    pass
                                else:
                                    pass
                                if (cursor[i]['Valid from'] != data_doc['Valid from']) or (
                                            cursor[i]['Valid till'] != data_doc['Valid till']) \
                                        or (
                                                    cursor[i]['Currency'] != data_doc['Currency']) \
                                        or (
                                                    cursor[i]['compartment'] != data_doc['compartment']):
                                    print "Raise Rule Change Trigger"  ### Call the trigger
                                    promo_rule_change_trigger = PromoRuleChangeTrigger("promotions_ruleschange",
                                                                                       old_doc_data=cursor[0],
                                                                                       new_doc_data=doc,
                                                                                       changed_field="Rules")
                                    promo_rule_change_trigger.do_analysis()
                                    rule_trig += 1
                                else:
                                    pass
                                if rule_trig == 1:
                                    a += 1
                                    break
                            print "rule change checked"
                        else:
                            print "No matches in Rule Change Trigger Check"
                            pass

                    print "done with rule change"

                    date_trig = 0


                    @measure(JUPITER_LOGGER)
                    def date_change():
                        date_trig = 0
                        print "date range trigger starts"
                        print data_doc
                        cursor = list(db.JUP_DB_Promotions1.aggregate([
                            {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                                        'compartment': data_doc['compartment'], 'Currency': data_doc['Currency'],
                                        'Fare': data_doc['Fare'], 'Valid from': data_doc['Valid from'],
                                        'Valid till': data_doc['Valid till'],
                                        }}, {'$project': {'_id': 0}},
                            {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
                        # cursor = json.dump(cursor)
                        print cursor
                        if len(cursor) > 0:
                            for i in range(len(cursor)):
                                if (cursor[i]['dep_date_from'] == data_doc['dep_date_from']) and (
                                            cursor[i]['dep_date_to'] == data_doc['dep_date_to']):
                                    pass
                                else:
                                    pass
                                if (cursor[i]['dep_date_from'] != data_doc['dep_date_from']) or (
                                            cursor[i]['dep_date_to'] != data_doc['dep_date_to']):
                                    print "Raise Date Change Trigger (dep period is updated)"
                                    promo_date_change_trigger = PromoDateChangeTrigger("promotions_dateschange",
                                                                                       old_doc_data=cursor[0],
                                                                                       new_doc_data=doc,
                                                                                       changed_field="dep_period")
                                    promo_date_change_trigger.do_analysis()
                                    date_trig += 1
                                else:
                                    pass
                                if date_trig == 1:
                                    a = 2
                                    break
                                else:
                                    pass
                        else:
                            print "No docs in Date Change trigger check"
                            pass

                    print "coming in ----yes1"
                    fare_trig = 0


                    @measure(JUPITER_LOGGER)
                    def fare_change():
                        fare_trig = 0
                        print "fare_range"
                        cursor = list(db.JUP_DB_Promotions1.aggregate([
                            {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                                        'compartment': data_doc['compartment'], 'Currency': data_doc['Currency'],
                                        'Valid from': data_doc['Valid from'],
                                        'dep_date_from': data_doc['dep_date_from'],
                                        'dep_date_to': data_doc['dep_date_to'],
                                        'Valid till': data_doc['Valid till'],
                                        }}, {'$project': {'_id': 0}},
                            {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
                        # cursor = json.dump(cursor)
                        print cursor
                        if len(cursor) > 0:
                            for i in range(len(cursor)):
                                if (cursor[i]['Fare'] == data_doc['Fare']):
                                    pass
                                else:
                                    pass
                                if (cursor[i]['Fare'] != data_doc['Fare']):
                                    print "Raise Fare Change Trigger (Fare is updated)"  ### Call the trigger
                                    promo_fare_change_trigger = PromoFareChangeTrigger("promotions_fareschange",
                                                                                       old_doc_data=cursor[i],
                                                                                       new_doc_data=doc,
                                                                                       changed_field="Fare")
                                    promo_fare_change_trigger.do_analysis()
                                    fare_trig += 1
                                else:
                                    pass
                                if fare_trig == 1:
                                    a = 3
                                    break
                                else:
                                    pass
                        else:
                            print "No docs in fares change check"
                            pass

                    new_trig = 0


                    @measure(JUPITER_LOGGER)
                    def new_promotion():
                        new_trig = 0
                        print "new_promotion"
                        # cursor = list(db.JUP_DB_Promotions.find({}))
                        cursor = list(db.JUP_DB_Promotions1.aggregate([
                            {'$match': {'Airline': data_doc['Airline']}}, {'$project': {'_id': 0}}, ]))
                        print "cursor:", cursor
                        print "len of cursor:", len(cursor)
                        if len(cursor) > 0:
                            for i in range(len(cursor)):
                                change_flag_list = []
                                if (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                                            cursor[i]['Valid till'] == data_doc['Valid till']) \
                                        and (
                                                                        cursor[i]['compartment'] == data_doc[
                                                                        'compartment']
                                                                and (cursor[i]['OD'] == data_doc['OD']) and (
                                                                        cursor[i]['Fare'] == data_doc['Fare']) and (
                                                                    cursor[i]['Currency'] == data_doc['Currency']) and (
                                                                cursor[i]['dep_date_from'] == data_doc[
                                                                'dep_date_from']) and (
                                                            cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                                    pass
                                else:
                                    pass

                                if (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                                            cursor[i]['Valid till'] == data_doc['Valid till']) \
                                        and (
                                                                        cursor[i]['compartment'] == data_doc[
                                                                        'compartment']
                                                                and (cursor[i]['OD'] != data_doc['OD']) and (
                                                                        cursor[i]['Fare'] == data_doc['Fare']) and (
                                                                    cursor[i]['Currency'] == data_doc['Currency']) and (
                                                                cursor[i]['dep_date_from'] == data_doc[
                                                                'dep_date_from']) and (
                                                            cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                                    print("Raise New Promotion Released Trigger")
                                    promo_new_promotion_trigger = PromoNewPromotionTrigger(
                                        "new_promotions",
                                        old_doc_data=cursor[i],
                                        new_doc_data=doc,
                                        changed_field="OD")
                                    promo_new_promotion_trigger.do_analysis()
                                    new_trig += 1
                                    if new_trig == 1:
                                        break
                                    else:
                                        pass
                                else:
                                    pass

                                if cursor[i]['Valid from'] != data_doc['Valid from']:
                                    change_flag_list.append(1)
                                else:
                                    pass
                                if cursor[i]['Valid till'] != data_doc['Valid till']:
                                    change_flag_list.append(2)
                                else:
                                    pass
                                if cursor[i]['compartment'] != data_doc['compartment']:
                                    change_flag_list.append(5)
                                else:
                                    pass
                                if cursor[i]['OD'] != data_doc['OD']:
                                    change_flag_list.append(6)
                                else:
                                    pass
                                if cursor[i]['Fare'] != data_doc['Fare']:
                                    change_flag_list.append(7)
                                else:
                                    pass
                                if cursor[i]['Currency'] != data_doc['Currency']:
                                    change_flag_list.append(8)
                                else:
                                    pass
                                if cursor[i]['dep_date_from'] != data_doc['dep_date_from']:
                                    change_flag_list.append(9)
                                else:
                                    pass
                                if cursor[i]['dep_date_to'] != data_doc['dep_date_to']:
                                    change_flag_list.append(10)
                                else:
                                    pass
                                print "change_flag_list:", change_flag_list
                                if len(change_flag_list) > 2:
                                    print("Raise New Promotion Released Trigger, new promotions are released")
                                    promo_new_promotion_trigger = PromoNewPromotionTrigger("new_promotions",
                                                                                           old_doc_data=cursor[i],
                                                                                           new_doc_data=doc,
                                                                                           changed_field="new")
                                    promo_new_promotion_trigger.do_analysis()
                                    new_trig += 1
                                else:
                                    pass
                                if new_trig == 1:
                                    a = 4
                                    break
                                else:
                                    pass

                        else:
                            print "No docs in New Promo check"
                            pass

                    for i in range(1, 2):
                        rule_change()
                        if rule_trig == 1:
                            break
                        else:
                            pass
                        date_change()
                        if date_trig == 1:
                            break
                        else:
                            pass
                        fare_change()
                        if fare_trig == 1:
                            break
                        else:
                            pass
                        new_promotion()
                        if new_trig == 1:
                            break
                        else:
                            pass
                    else:
                        pass


proms = db.JUP_DB_Promotions1.find({'origin': 'EVN'}, {'_id': 0})
# print proms , 'Last updated Date': '2018-01-22'
prom = list(proms)
print len(prom)
print "Changed"
for i in prom:
    # print i
    data_doc = i
    doc = i
    print "data_doc: ", data_doc
    origin = data_doc['origin']
    destination = data_doc['destination']

    airport_code_ori_list, airport_code_dest_list = airport_city_map(origin, destination)
    print airport_code_ori_list, airport_code_dest_list

    airport_city_map(origin,destination)
    check_od_and_raise_trigger(airport_code_ori_list, airport_code_dest_list)



