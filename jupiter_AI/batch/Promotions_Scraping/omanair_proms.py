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
    from dateutil.parser import parse
    import re

    # Connection to MongoDB
    import pymongo
    client = pymongo.MongoClient("mongodb://analytics:KNjSZmiaNUGLmS0Bv2@13.92.251.7:42525/")
    db = client['fzDB']

    #Opening driver
    chrome_path = r"/home/prathyusha/Downloads/chromedriver"
    driver = webdriver.Chrome(chrome_path)
    driver.wait = WebDriverWait(driver, 5)

    #This page shows all promotions
    driver.get("http://www.omanair.com/ph/en/best-fares-deals/best-fares")
    driver.wait = WebDriverWait(driver, 5)

    #Look for promotions table
    container = driver.find_element_by_xpath('//*[@id="mainContent"]/div[2]/div[2]/div/div[2]')
    tables = container.find_elements_by_tag_name('table')


    #Code to get all promotions links in url_box
    url_box = []
    for eachtable in tables:
        rows = eachtable.find_elements_by_tag_name('tr')
        print 'foundrows'

        for eachrow in rows:
            try:
                data = eachrow.find_elements_by_tag_name('td')
                url = data[0].find_element_by_tag_name('a').get_attribute('href')
                url_box.append(url)
            except:
              pass

    #Extracting promotion from eachlink
    for eachurl in url_box:
        driver.get(eachurl)
        inner_box = driver.find_element_by_xpath('//*[@id="mainContent"]/div[2]/div[2]/div[2]/div[1]/div/div')
        elements = inner_box.find_elements_by_tag_name('ul')
        lists1 = elements[0].find_elements_by_tag_name('li')

        #List1 gives Compartment,OD and Trip Type
        for index,eachlist in enumerate(lists1):
            #Code for Compartment of flight/promotion
            if 'service' in eachlist.text.lower():
                Compartment = eachlist.find_element_by_class_name('lableDescription').text
                if 'business' in Compartment.lower():
                    Compartment = 'J'
                elif 'econ' in Compartment.lower():
                    Compartment = 'Y'
                elif ('first') in Compartment.lower():
                    Compartment = 'Z'
                print 'Compartmnet:'+ Compartment

            #Code for Origin and Compartment
            if 'routing' in eachlist.text.lower():
                Routing = eachlist.find_element_by_class_name('lableDescription').text
                print 'Routing:'+ Routing
                ODs = re.findall(r'\w+',Routing)
                Origin = ODs[0]
                Destination = ODs[1]
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
                print 'Origin:',Origin
                print 'Destination:', Destination
                OD = Origin + Destination

                #Code for Type - Return/One way
                length_Ods =  len(ODs)
                if length_Ods == 3:
                    Trip_Type = 'Return'
                elif length_Ods == 2:
                    Trip_Type = 'One Way'
                print 'Trip_Type:',Trip_Type

        #Lists2 gives Fare,Currency and description
        lists2 = elements[1].find_elements_by_tag_name('li')
        for eachlist in lists2:
            if 'fare' in eachlist.text.lower():
                fare_curr = eachlist.find_element_by_class_name('lableDescription').text
                fc = re.findall(r'\w+',fare_curr)
                print 'currency:',fc[1]
                fare = re.findall(r'\d+',fare_curr)
                fare_elem = ""
                for index in range(len(fare)):
                    fe = str(fare[index])
                    fare_elem+= fe
                Fare = int(filter(str.isdigit,str(fare_elem)))
                print 'fare:',Fare
        Description = elements[1].find_element_by_class_name('note').text
        print 'Description:' + Description

        #Lists3 gives stay duration, sale validity, travel validity and refund details
        lists3 = elements[2].find_elements_by_tag_name('li')
        for eachlist in lists3:
            #Stay duration
            if 'stay' in eachlist.text.lower():
                Maximum_stay = eachlist.find_element_by_class_name('lableDescription').text
                print 'Maximum_stay:'+ Maximum_stay
            #Sale Validity dates
            if 'sale validity' in eachlist.text.lower():
                SV = eachlist.find_element_by_class_name('lableDescription').text
                SV1 = SV.split('From',1)[1]
                Valid_from= parse(SV1.split('to',1)[0]).strftime('%Y-%m-%d')
                Valid_till = parse(SV1.split('to',1)[1]).strftime('%Y-%m-%d')
                print 'Valid_from:'+Valid_from
                print 'Valid_till:'+Valid_till
            #Travel Validity dates
            if 'travel validity' in eachlist.text.lower():
                TV = eachlist.find_element_by_class_name('lableDescription').text
                TV1 = TV.split('From',1)[1]
                Travel_from= parse(TV1.split('to',1)[0]).strftime('%Y-%m-%d')
                Travel_till = parse(TV1.split('to',1)[1]).strftime('%Y-%m-%d')
                print 'Travel_from:'+Travel_from
                print 'Travel_till:'+Travel_till
            #refund details
            if 'refund' in eachlist.text.lower():
                Refund = eachlist.find_element_by_class_name('lableDescription').text
                print 'Refund:'+Refund

        #MongoDB collection
        doc = {
              "Airline" : "WY",
              "OD" : Origin + Destination,
              "Valid from" : Valid_from,
              "Valid till" : Valid_till,
              "Type" : Trip_Type,
              "Compartment" : Compartment,
              "Fare" : Fare,
              "Currency":fc[1],
              "dep_date_from" : Travel_from,
              "dep_date_to" : Travel_till,
              'Maximum stay' : Maximum_stay,
              'Refund' : Refund,
              "Url" : eachurl,
              "Last updated Date": time.strftime('%Y-%m-%d'),
              "Last updated Time":time.strftime('%H')
               }
        data_doc = {
             "Airline" : "WY",
             "OD" : Origin + Destination,
             "Valid from" : Valid_from,
             "Valid till" : Valid_till,
             "Type" : Trip_Type,
             "Compartment" : Compartment,
             "Fare" : Fare,
             "Currency":fc[1],
             "dep_date_from" : Travel_from,
             "dep_date_to" : Travel_till,
             'Maximum stay' : Maximum_stay,
             'Refund' : Refund,
             "Url" : eachurl
              }
        print doc
        print data_doc


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
                elif (cursor[0]['Valid from'] != data_doc['Valid from']) and (
                            cursor[0]['Valid till'] == data_doc['Valid till']) \
                        and (
                                    cursor[0]['Compartment'] == data_doc['Compartment']):
                    print "Raise Rule Change Trigger (Valid from is updated)"  ### Call the trigger
                    promo_rule_change_trigger = PromoRuleChangeTrigger("promotions_ruleschange",
                                                                       old_doc_data=cursor[0],
                                                                       new_doc_data=doc,
                                                                       changed_field="Valid from")
                    promo_rule_change_trigger.do_analysis()

                elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                            cursor[0]['Valid till'] != data_doc['Valid till']) \
                        and (
                                    cursor[0]['Compartment'] == data_doc['Compartment']):
                    print "Raise Rule Change Trigger (Valid till is updated)"  ### Call the trigger
                    promo_rule_change_trigger = PromoRuleChangeTrigger("promotions_ruleschange",
                                                                       old_doc_data=cursor[0],
                                                                       new_doc_data=doc,
                                                                       changed_field="Valid till")
                    promo_rule_change_trigger.do_analysis()


                elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (
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


        @measure(JUPITER_LOGGER)
        def date_change():
            print "date_range"
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
                elif (cursor[0]['dep_date_from'] != data_doc['dep_date_from']) and (
                            cursor[0]['dep_date_to'] == data_doc['dep_date_to']):
                    print "Raise Date Change Trigger (dep date from is updated)"  ### Call the trigger
                    promo_date_change_trigger = PromoDateChangeTrigger("promotions_dateschange",
                                                                       old_doc_data=cursor[0],
                                                                       new_doc_data=doc,
                                                                       changed_field="dep_date_from")
                    promo_date_change_trigger.do_analysis()
                elif (cursor[0]['dep_date_from'] == data_doc['dep_date_from']) and (
                            cursor[0]['dep_date_to'] != data_doc['dep_date_to']):
                    print "Raise Date Change Trigger (dep date from is updated)"  ### Call the trigger
                    promo_date_change_trigger = PromoDateChangeTrigger("promotions_dateschange",
                                                                       old_doc_data=cursor[0],
                                                                       new_doc_data=doc,
                                                                       changed_field="dep_date_to")
                    promo_date_change_trigger.do_analysis()

            else:
                pass


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
                elif (cursor[0]['Fare'] != data_doc['Fare']):
                    print "Raise Fare Change Trigger (Fare is updated)"  ### Call the trigger
                    promo_fare_change_trigger = PromoFareChangeTrigger("promotions_fareschange",
                                                                       old_doc_data=cursor[0],
                                                                       new_doc_data=doc,
                                                                       changed_field="Fare")
                    promo_fare_change_trigger.do_analysis()
            else:
                pass


        @measure(JUPITER_LOGGER)
        def new_promotion():
            print "new_promotion"
            cursor = list(db.JUP_DB_Promotions.aggregate([
                {'$match': {'Airline': data_doc['Airline']}}]))
            # cursor = json.dump(cursor)
            print cursor
            if len(cursor) > 0:
                for i in range(len(cursor)):
                    if (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                                cursor[i]['Valid till'] == data_doc['Valid till']) \
                            and (
                                                            cursor[i]['Compartment'] == data_doc['Compartment']
                                                    and (cursor[i]['OD'] == data_doc['OD']) and (
                                                            cursor[i]['Fare'] == data_doc['Fare']) and (
                                                        cursor[i]['Currency'] == data_doc['Currency']) and (
                                                    cursor[i]['dep_date_from'] == data_doc['dep_date_from']) and (
                                                cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                        pass

                    elif (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                                cursor[i]['Valid till'] == data_doc['Valid till']) \
                            and (
                                                            cursor[i]['Compartment'] == data_doc['Compartment']
                                                    and (cursor[i]['OD'] == data_doc['OD']) and (
                                                            cursor[i]['Fare'] == data_doc['Fare']) and (
                                                        cursor[i]['Currency'] != data_doc['Currency']) and (
                                                    cursor[i]['dep_date_from'] == data_doc['dep_date_from']) and (
                                                cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                        print("Raise New Promotion Released Trigger")
                        promo_new_promotion_trigger = PromoNewPromotionTrigger(
                            "new_promotions",
                            old_doc_data=cursor[i],
                            new_doc_data=doc,
                            changed_field="Currency")
                        promo_new_promotion_trigger.do_analysis()

                    elif (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                                cursor[i]['Valid till'] == data_doc['Valid till']) \
                            and (
                                                            cursor[i]['Compartment'] == data_doc['Compartment']
                                                    and (cursor[i]['OD'] != data_doc['OD']) and (
                                                            cursor[i]['Fare'] == data_doc['Fare']) and (
                                                        cursor[i]['Currency'] == data_doc['Currency']) and (
                                                    cursor[i]['dep_date_from'] == data_doc['dep_date_from']) and (
                                                cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                        print("Raise New Promotion Released Trigger")
                        promo_new_promotion_trigger = PromoNewPromotionTrigger(
                            "new_promotions",
                            old_doc_data=cursor[i],
                            new_doc_data=doc,
                            changed_field="OD")
                        promo_new_promotion_trigger.do_analysis()

                    elif (cursor[i]['Valid from'] != data_doc['Valid from']) and (
                                cursor[i]['Valid till'] != data_doc['Valid till']) \
                            and (
                                                            cursor[i]['Compartment'] != data_doc['Compartment'] and (
                                                                cursor[i]['OD'] != data_doc['OD']) and (
                                                            cursor[i]['Fare'] != data_doc['Fare']) and (
                                                        cursor[i]['Currency'] != data_doc['Currency']) and (
                                                    cursor[i]['dep_date_from'] != data_doc['dep_date_from']) and (
                                                cursor[i]['dep_date_to'] != data_doc['dep_date_to'])):
                        print("Raise New Promotion Released Trigger")
                    promo_new_promotion_trigger = PromoNewPromotionTrigger(
                        "new_promotions",
                        old_doc_data=cursor[i],
                        new_doc_data=doc,
                        changed_field="new")
                    promo_new_promotion_trigger.do_analysis()

        rule_change()
        date_change()
        fare_change()
        new_promotion()


        #db.JUP_DB_Promotions.update(data_doc1,data_doc,upsert = True)
        print 'Yes'

    driver.close()
if __name__ == "__main__":
    run()