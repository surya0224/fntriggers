from jupiter_AI import JUPITER_LOGGER
from jupiter_AI.logutils import measure


@measure(JUPITER_LOGGER)
def run():

    import time
    from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoRuleChangeTrigger
    from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoDateChangeTrigger
    from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoFareChangeTrigger
    from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoNewPromotionTrigger
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.keys import Keys
    from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
    from selenium.webdriver.support.ui import Select
    import re
    from dateutil.parser import parse
    from pymongo import UpdateOne
    from pyvirtualdisplay import Display

    # Connection to MongoDB
    import pymongo
    client = pymongo.MongoClient("mongodb://analytics:KNjSZmiaNUGLmS0Bv2@13.92.251.7:42525/")
    db = client['fzDB_stg']

    #Openingdriver
    #chrome_path = r"/home/prathyusha/Downloads/Projects Codes/Promotions/chromedriver"
    #chrome_path = r"/var/www/html/jupiter/python/jupiter_AI/batch/Promotions_Scraping/chromedriver"
    #driver = webdriver.Chrome(chrome_path)
    driver = webdriver.PhantomJS()

    #from flynava import basic.py
    driver.wait = WebDriverWait(driver, 5)


    #All offers available on this link only
    driver.get("https://www.ethiopianairlines.com/AE/EN/book/offers")
    driver.wait = WebDriverWait(driver, 5)

    #Scraping starts
    container = driver.find_element_by_xpath('//*[@id="main-content"]/div/div[1]/div')
    container_detail = container.find_elements_by_tag_name('div')


    for index,eachcontainer in enumerate(container_detail):
        change_flag_list =[]
        if not (index % 3):   #Same data printing thrice
            #Code for Origin and Compartment
            try :
                content1 = eachcontainer.text.splitlines()
                print content1
                Origin = (content1[0].split('-')[0])
                Destination = (content1[0].split('-')[1])
                #Match codes for Origin and Destination from IATA codes
                try:
                    cursor = db.JUP_DB_IATA_Codes.find()
                    for i in cursor:
                        if all(word in i['City'].lower() for word in re.findall(r'\w+',Origin.lower())):
                            Origin = i['Code']
                        if all(word in i['City'].lower() for word in re.findall(r'\w+',Destination.lower())):
                            Destination = i['Code']
                except:
                    pass
                print "origin: ", Origin
                print "destination: ", Destination

                #Code for Fare and Currency
                fare_curr = (content1[1].split('From')[1])
                Fare = re.findall(r'\w+',fare_curr)[0]
                Currency = re.findall(r'\w+',fare_curr)[1]
                print Fare
                print Currency

                #Description
                description = content1[3]
                print description

                #Collection in MongoDB
                doc = {
                    "Airline" : "ET",
                    "OD" : Origin + Destination,
                    "Valid from" : "",
                    "Valid till" : "",
                    "Class" : "",
                    "Fare" : Fare,
                    "Minimum Stay" : "",
                    "Maximum Stay" : "",
                    "Travel Date" : "",
                    "Description" : description,
                    "Url" : "https://www.ethiopianairlines.com/AE/EN/book/offers",
                    "Last updated Date": time.strftime('%Y-%m-%d'),
                    "Last updated Time":time.strftime('%H')
                     }
                data_doc = {
                    "Airline" : "ET",
                    "OD" : Origin + Destination,
                    "Valid from" : "",
                    "Valid till" : "",
                    "Class" : "",
                    "Fare" : Fare,
                    "Minimum Stay" : "",
                    "Maximum Stay" : "",
                    "Travel Date" : "",
                    "Description" : description,
                    "Url" : "https://www.ethiopianairlines.com/AE/EN/book/offers",
                     }
                cursor = list(
                    db.JUP_DB_Airline_OD.aggregate([{'$match': {'Airline': "FZ"}}, {'$project': {'_id': 0, 'OD': 1}}]))
                print "1---yes"
                for i in cursor:
                    if (data_doc['OD'] == i):


                        @measure(JUPITER_LOGGER)
                        def rule_change():
                            print "rule_change_trigger_starts"
                            cursor = list(db.JUP_DB_Promotions.aggregate([
                                {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                                            'Currency': data_doc['Currency'], 'Fare': data_doc['Fare'],
                                            'dep_date_from': data_doc['dep_date_from'],
                                            'dep_date_to': data_doc['dep_date_to']}},
                                {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
                            # cursor = json.dump(cursor)
                            print cursor
                            # print "data_doc: ", data_doc
                            if len(cursor) > 0:
                                if (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                                            cursor[0]['Valid till'] == data_doc['Valid till']) \
                                        and (
                                                    cursor[0]['Compartment'] == data_doc['Compartment']):
                                    pass
                                else:
                                    pass
                                if (cursor[0]['Valid from'] != data_doc['Valid from']) and (
                                            cursor[0]['Valid till'] == data_doc['Valid till']) \
                                        and (
                                                    cursor[0]['Compartment'] == data_doc['Compartment']):
                                    print "Raise Rule Change Trigger (Valid from is updated)"  ### Call the trigger
                                    promo_rule_change_trigger = PromoRuleChangeTrigger("promotions_ruleschange",
                                                                                       old_doc_data=cursor[0],
                                                                                       new_doc_data=doc,
                                                                                       changed_field="Valid from")
                                    promo_rule_change_trigger.do_analysis()
                                else:
                                    pass

                                if (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                                            cursor[0]['Valid till'] != data_doc['Valid till']) \
                                        and (
                                                    cursor[0]['Compartment'] == data_doc['Compartment']):
                                    print "Raise Rule Change Trigger (Valid till is updated)"  ### Call the trigger
                                    promo_rule_change_trigger = PromoRuleChangeTrigger("promotions_ruleschange",
                                                                                       old_doc_data=cursor[0],
                                                                                       new_doc_data=doc,
                                                                                       changed_field="Valid till")
                                    promo_rule_change_trigger.do_analysis()
                                else:
                                    pass

                                if (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                                            cursor[0]['Valid till'] == data_doc['Valid till']) \
                                        and (
                                                    cursor[0]['Compartment'] != data_doc['Compartment']):
                                    print "Raise Rule Change Trigger (Compartment is updated)"  ### Call the trigger
                                    promo_rule_change_trigger = PromoRuleChangeTrigger("promotions_ruleschange",
                                                                                       old_doc_data=cursor[0],
                                                                                       new_doc_data=doc,
                                                                                       changed_field="Compartment")
                                    promo_rule_change_trigger.do_analysis()
                                else:
                                    pass
                                print "rule change checked"
                            else:
                                print "i m in else1"
                                pass

                        print "done with rule change"


                        @measure(JUPITER_LOGGER)
                        def date_change():
                            print "date range trigger starts"
                            print data_doc
                            cursor = list(db.JUP_DB_Promotions.aggregate([
                                {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                                            'Compartment': data_doc['Compartment'], 'Currency': data_doc['Currency'],
                                            'Fare': data_doc['Fare'], 'Valid from': data_doc['Valid from'],
                                            'Valid till': data_doc['Valid till'],
                                            }},
                                {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
                            # cursor = json.dump(cursor)
                            print cursor
                            if len(cursor) > 0:

                                if (cursor[0]['dep_date_from'] == data_doc['dep_date_from']) and (
                                            cursor[0]['dep_date_to'] == data_doc['dep_date_to']):
                                    pass
                                else:
                                    pass
                                if (cursor[0]['dep_date_from'] != data_doc['dep_date_from']) and (
                                            cursor[0]['dep_date_to'] == data_doc['dep_date_to']):
                                    print "Raise Date Change Trigger (dep date from is updated)"  ### Call the trigger
                                    promo_date_change_trigger = PromoDateChangeTrigger("promotions_dateschange",
                                                                                       old_doc_data=cursor[0],
                                                                                       new_doc_data=doc,
                                                                                       changed_field="dep_date_from")
                                    promo_date_change_trigger.do_analysis()
                                else:
                                    pass

                                if (cursor[0]['dep_date_from'] == data_doc['dep_date_from']) and (
                                            cursor[0]['dep_date_to'] != data_doc['dep_date_to']):
                                    print "Raise Date Change Trigger (dep date from is updated)"  ### Call the trigger
                                    promo_date_change_trigger = PromoDateChangeTrigger("promotions_dateschange",
                                                                                       old_doc_data=cursor[0],
                                                                                       new_doc_data=doc,
                                                                                       changed_field="dep_date_to")
                                    promo_date_change_trigger.do_analysis()
                                else:
                                    pass

                            else:
                                print "i m in else2"
                                pass

                        print "coming in ----yes1"


                        @measure(JUPITER_LOGGER)
                        def fare_change():
                            print "fare_range"
                            cursor = list(db.JUP_DB_Promotions.aggregate([
                                {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                                            'Compartment': data_doc['Compartment'], 'Currency': data_doc['Currency'],
                                            'Valid from': data_doc['Valid from'],
                                            'dep_date_from': data_doc['dep_date_from'],
                                            'dep_date_to': data_doc['dep_date_to'],
                                            'Valid till': data_doc['Valid till'],
                                            }},
                                {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
                            # cursor = json.dump(cursor)
                            print cursor
                            if len(cursor) > 0:

                                if (cursor[0]['Fare'] == data_doc['Fare']):
                                    pass
                                else:
                                    pass
                                if (cursor[0]['Fare'] != data_doc['Fare']):
                                    print "Raise Fare Change Trigger (Fare is updated)"  ### Call the trigger
                                    promo_fare_change_trigger = PromoFareChangeTrigger("promotions_fareschange",
                                                                                       old_doc_data=cursor[0],
                                                                                       new_doc_data=doc,
                                                                                       changed_field="Fare")
                                    promo_fare_change_trigger.do_analysis()
                                else:
                                    pass
                            else:
                                print "i am in else3"
                                pass


                        @measure(JUPITER_LOGGER)
                        def new_promotion():
                            print "new_promotion"
                            cursor = list(db.JUP_DB_Promotions.aggregate([
                                {'$match': {'Airline': data_doc['Airline']}}]))
                            # cursor = json.dump(cursor)
                            print "cursor:", cursor
                            if len(cursor) > 0:
                                for i in range(len(cursor)):
                                    if (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                                                cursor[i]['Valid till'] == data_doc['Valid till']) \
                                            and (
                                                                            cursor[i]['Compartment'] == data_doc[
                                                                            'Compartment']
                                                                    and (cursor[i]['OD'] == data_doc['OD']) and (
                                                                            cursor[i]['Fare'] == data_doc['Fare']) and (
                                                                        cursor[i]['Currency'] == data_doc[
                                                                        'Currency']) and (
                                                                    cursor[i]['dep_date_from'] == data_doc[
                                                                    'dep_date_from']) and (
                                                                cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                                        pass
                                    else:
                                        pass

                                    if (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                                                cursor[i]['Valid till'] == data_doc['Valid till']) \
                                            and (
                                                                            cursor[i]['Compartment'] == data_doc[
                                                                            'Compartment']
                                                                    and (cursor[i]['OD'] == data_doc['OD']) and (
                                                                            cursor[i]['Fare'] == data_doc['Fare']) and (
                                                                        cursor[i]['Currency'] != data_doc[
                                                                        'Currency']) and (
                                                                    cursor[i]['dep_date_from'] == data_doc[
                                                                    'dep_date_from']) and (
                                                                cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                                        print("Raise New Promotion Released Trigger")
                                        promo_new_promotion_trigger = PromoNewPromotionTrigger(
                                            "new_promotions",
                                            old_doc_data=cursor[i],
                                            new_doc_data=doc,
                                            changed_field="Currency")
                                        promo_new_promotion_trigger.do_analysis()
                                    else:
                                        pass

                                    if (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                                                cursor[i]['Valid till'] == data_doc['Valid till']) \
                                            and (
                                                                            cursor[i]['Compartment'] == data_doc[
                                                                            'Compartment']
                                                                    and (cursor[i]['OD'] != data_doc['OD']) and (
                                                                            cursor[i]['Fare'] == data_doc['Fare']) and (
                                                                        cursor[i]['Currency'] == data_doc[
                                                                        'Currency']) and (
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
                                    if cursor[i]['Compartment'] != data_doc['Compartment']:
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
                                    if cursor[i]['Start Date'] != data_doc['dep_date_from']:
                                        change_flag_list.append(9)
                                    else:
                                        pass
                                    if cursor[i]['End Date'] != data_doc['dep_date_to']:
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
                                    else:
                                        pass
                            else:
                                print "i m in else4"
                                pass

                        rule_change()
                        print "coming in---yes2"
                        date_change()
                        fare_change()
                        new_promotion()

                    else:
                        # print "OD is not in FZ list, So no trigger will be raised"
                        pass

                print "2---yes"

                # except:
                #     pass


                #print "3----yes"

                # bulk_doc = []

                #print "4----yes"
                # for j in range(len()):
                if t == 200:
                    st = time.time()
                    print "updating: ", count

                    print bulk_update_doc
                    db['JUP_DB_Promotions'].bulk_write(bulk_update_doc)
                    print "updated!", time.time() - st
                    bulk_list = []
                    count1 += 1
                    t = 0
                    print "yes---------------------->"
                else:
                    # bulk_data_doc.append(data_doc )
                    # bulk_doc.append(doc)
                    print "5---else--yes"

                    bulk_update_doc.append(UpdateOne(data_doc, {"$set": doc}, upsert=True))
                    print bulk_update_doc
                    t += 1
                    print "t= :", t
                    # if j > 4:  #??
                    #     break

                    #db.JUP_DB_Promotions.update(data_doc,doc,upsert = True)
                    #print "yes"


                # def rule_change():
                #     print "rule_change_trigger_starts"
                #     cursor = list(db.JUP_DB_Promotions.aggregate([
                #         {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                #                     'Currency': data_doc['Currency'], 'Fare': data_doc['Fare'],
                #                     'dep_date_from': data_doc['dep_date_from'],
                #                     'dep_date_to': data_doc['dep_date_to']}},
                #         {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
                #     # cursor = json.dump(cursor)
                #     print cursor
                #     #print "data_doc: ", data_doc
                #     if len(cursor) > 0:
                #         if (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                #             cursor[0]['Valid till'] == data_doc['Valid till']) \
                #                  and (
                #             cursor[0]['Compartment'] == data_doc['Compartment']):
                #             pass
                #         elif (cursor[0]['Valid from'] != data_doc['Valid from']) and (
                #             cursor[0]['Valid till'] == data_doc['Valid till']) \
                #                 and (
                #             cursor[0]['Compartment'] == data_doc['Compartment']):
                #             print "Raise Rule Change Trigger (Valid from is updated)"  ### Call the trigger
                #             promo_rule_change_trigger = PromoRuleChangeTrigger("promotions_ruleschange",
                #                                                                old_doc_data=cursor[0],
                #                                                                new_doc_data=doc,
                #                                                                changed_field="Valid from")
                #             promo_rule_change_trigger.do_analysis()
                #
                #         elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                #             cursor[0]['Valid till'] != data_doc['Valid till']) \
                #                  and (
                #             cursor[0]['Compartment'] == data_doc['Compartment']):
                #             print "Raise Rule Change Trigger (Valid till is updated)"  ### Call the trigger
                #             promo_rule_change_trigger = PromoRuleChangeTrigger("promotions_ruleschange",
                #                                                                old_doc_data=cursor[0],
                #                                                                new_doc_data=doc,
                #                                                                changed_field="Valid till")
                #             promo_rule_change_trigger.do_analysis()
                #
                #
                #         elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                #             cursor[0]['Valid till'] == data_doc['Valid till']) \
                #                  and (
                #             cursor[0]['Compartment'] != data_doc['Compartment']):
                #             print "Raise Rule Change Trigger (Compartment is updated)"  ### Call the trigger
                #             promo_rule_change_trigger = PromoRuleChangeTrigger("promotions_ruleschange",
                #                                                                old_doc_data=cursor[0],
                #                                                                new_doc_data=doc,
                #                                                                changed_field="Compartment")
                #             promo_rule_change_trigger.do_analysis()
                #
                #     else:
                #         pass
                #
                # def date_change():
                #     print "date_range"
                #     print data_doc
                #     cursor = list(db.JUP_DB_Promotions.aggregate([
                #         {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                #                     'Compartment': data_doc['Compartment'], 'Currency': data_doc['Currency'],
                #                     'Fare': data_doc['Fare'], 'Valid from': data_doc['Valid from'],
                #                     'Valid till': data_doc['Valid till'],
                #                     }},
                #         {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
                #     #cursor = json.dump(cursor)
                #     print cursor
                #     if len(cursor) > 0:
                #
                #         if (cursor[0]['dep_date_from'] == data_doc['dep_date_from']) and (
                #             cursor[0]['dep_date_to'] == data_doc['dep_date_to']):
                #             pass
                #         elif (cursor[0]['dep_date_from'] != data_doc['dep_date_from']) and (
                #             cursor[0]['dep_date_to'] == data_doc['dep_date_to']):
                #             print "Raise Date Change Trigger (dep date from is updated)"  ### Call the trigger
                #             promo_date_change_trigger = PromoDateChangeTrigger("promotions_dateschange",
                #                                                                old_doc_data=cursor[0],
                #                                                                new_doc_data=doc,
                #                                                                changed_field="dep_date_from")
                #             promo_date_change_trigger.do_analysis()
                #         elif (cursor[0]['dep_date_from'] == data_doc['dep_date_from']) and (
                #             cursor[0]['dep_date_to'] != data_doc['dep_date_to']):
                #             print "Raise Date Change Trigger (dep date from is updated)"  ### Call the trigger
                #             promo_date_change_trigger = PromoDateChangeTrigger("promotions_dateschange",
                #                                                                old_doc_data=cursor[0],
                #                                                                new_doc_data=doc,
                #                                                                changed_field="dep_date_to")
                #             promo_date_change_trigger.do_analysis()
                #
                #     else:
                #         pass
                # def fare_change():
                #     print "fare_range"
                #     cursor = list(db.JUP_DB_Promotions.aggregate([
                #         {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                #                     'Compartment': data_doc['Compartment'], 'Currency': data_doc['Currency'],
                #                     'Valid from': data_doc['Valid from'],
                #                     'dep_date_from': data_doc['dep_date_from'],
                #                     'dep_date_to': data_doc['dep_date_to'],
                #                     'Valid till': data_doc['Valid till'],
                #                     }},
                #         {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
                #     #cursor = json.dump(cursor)
                #     print cursor
                #     if len(cursor) > 0:
                #
                #         if (cursor[0]['Fare'] == data_doc['Fare']):
                #             pass
                #         elif (cursor[0]['Fare'] != data_doc['Fare']):
                #             print "Raise Fare Change Trigger (Fare is updated)"  ### Call the trigger
                #             promo_fare_change_trigger = PromoFareChangeTrigger("promotions_fareschange",
                #                                                                old_doc_data=cursor[0],
                #                                                                new_doc_data=doc,
                #                                                                changed_field="Fare")
                #             promo_fare_change_trigger.do_analysis()
                #     else:
                #         pass
                #
                # def new_promotion():
                #     print "new_promotion"
                #     cursor = list(db.JUP_DB_Promotions.aggregate([
                #         {'$match': {'Airline': data_doc['Airline']}}]))
                #     #cursor = json.dump(cursor)
                #     print cursor
                #     if len(cursor) > 0:
                #         for i in range(len(cursor)):
                #             if (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                #                 cursor[i]['Valid till'] == data_doc['Valid till']) \
                #                      and (
                #                                     cursor[i]['Compartment'] == data_doc['Compartment']
                #                             and (cursor[i]['OD'] == data_doc['OD']) and (
                #                             cursor[i]['Fare'] == data_doc['Fare']) and (
                #                         cursor[i]['Currency'] == data_doc['Currency']) and (
                #                     cursor[i]['dep_date_from'] == data_doc['dep_date_from']) and (
                #                 cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                #                 pass
                #
                #             elif (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                #                 cursor[i]['Valid till'] == data_doc['Valid till']) \
                #                      and (
                #                                     cursor[i]['Compartment'] == data_doc['Compartment']
                #                             and (cursor[i]['OD'] == data_doc['OD']) and (
                #                             cursor[i]['Fare'] == data_doc['Fare']) and (
                #                         cursor[i]['Currency'] != data_doc['Currency']) and (
                #                     cursor[i]['dep_date_from'] == data_doc['dep_date_from']) and (
                #                 cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                #                 print("Raise New Promotion Released Trigger")
                #                 promo_new_promotion_trigger = PromoNewPromotionTrigger(
                #                     "new_promotions",
                #                     old_doc_data=cursor[i],
                #                     new_doc_data=doc,
                #                     changed_field="Currency")
                #                 promo_new_promotion_trigger.do_analysis()
                #
                #             elif (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                #                 cursor[i]['Valid till'] == data_doc['Valid till']) \
                #                      and (
                #                                             cursor[i]['Compartment'] == data_doc['Compartment']
                #                                     and (cursor[i]['OD'] != data_doc['OD']) and (
                #                                     cursor[i]['Fare'] == data_doc['Fare']) and (
                #                                         cursor[i]['Currency'] == data_doc['Currency']) and (
                #                             cursor[i]['dep_date_from'] == data_doc['dep_date_from']) and (
                #                                 cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                #                 print("Raise New Promotion Released Trigger")
                #                 promo_new_promotion_trigger = PromoNewPromotionTrigger(
                #                     "new_promotions",
                #                     old_doc_data=cursor[i],
                #                     new_doc_data=doc,
                #                     changed_field="OD")
                #                 promo_new_promotion_trigger.do_analysis()
                #
                #             elif (cursor[i]['Valid from'] != data_doc['Valid from']) and (
                #                 cursor[i]['Valid till'] != data_doc['Valid till']) \
                #                      and (
                #                                     cursor[i]['Compartment'] != data_doc['Compartment'] and (
                #                                 cursor[i]['OD'] != data_doc['OD']) and (
                #                             cursor[i]['Fare'] != data_doc['Fare']) and (
                #                         cursor[i]['Currency'] != data_doc['Currency']) and (
                #                             cursor[i]['dep_date_from'] != data_doc['dep_date_from']) and (
                #                 cursor[i]['dep_date_to'] != data_doc['dep_date_to'])):
                #                 print("Raise New Promotion Released Trigger")
                #             promo_new_promotion_trigger = PromoNewPromotionTrigger(
                #                 "new_promotions",
                #                 old_doc_data=cursor[i],
                #                 new_doc_data=doc,
                #                 changed_field="new")
                #             promo_new_promotion_trigger.do_analysis()
                #
                # rule_change()
                # date_change()
                # fare_change()
                # new_promotion()
                #
                # db.JUP_DB_Promotions.update(data_doc,doc,upsert = True)
                # print "yes"


            except:
                pass
    driver.close()

if __name__ == '__main__':
  run()
