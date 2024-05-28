#this is Ethihad Airlines
from jupiter_AI import JUPITER_LOGGER
from jupiter_AI.logutils import measure


@measure(JUPITER_LOGGER)
def run():
    from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoRuleChangeTrigger
    from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoDateChangeTrigger
    from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoFareChangeTrigger
    from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoNewPromotionTrigger
    from selenium import webdriver
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By
    from requests.exceptions import ConnectionError
    import time
    from dateutil.parser import parse
    import datetime
    import re
    import pandas as pd
    import numpy as np
    #dbecting to Mongodb
    import pymongo
    client = pymongo.MongoClient("mongodb://analytics:KNjSZmiaNUGLmS0Bv2@13.92.251.7:42525/")
    db = client['fzDB']

    #Defining the Chrome path
    chrome_path = r"/home/prathyusha/Downloads/Projects Codes/Promotions/chromedriver"
    driver = webdriver.Chrome(chrome_path)
    driver.get("http://www.etihad.com/en-in/")
    driver1 = webdriver.Chrome(chrome_path)
    wait = WebDriverWait(driver, 30)
    wait = WebDriverWait(driver1, 30)

    #Function for scraping the promotions


    @measure(JUPITER_LOGGER)
    def promotions(url):
        driver1.get(url)
        wait.until(EC.visibility_of_element_located((By.XPATH, "//*[@id='contentJump']/div[2]/div[1]/div[2]")))
        offer = driver1.find_element_by_xpath("//*[@id='contentJump']/div[2]/div[1]/div[2]")
        each_content = offer.text.splitlines()
        origin = each_content[3]
        destination = each_content[5]
        cursor = db.JUP_DB_IATA_Codes.find()
        for i in cursor:
          if all(word in i['City'].lower() for word in re.findall(r'\w+',each_content[3].lower())):
            origin = i["Code"]
            #print("Origin :",origin)
          if all(word in i['City'].lower() for word in re.findall(r'\w+',each_content[5].lower())):
            destination = i["Code"]
            #print("Destination :",destination)
        print(origin+destination)

        dt = parse(each_content[7])
        valid_till = dt.strftime('%Y-%m-%d')
        print("Valid till :", valid_till)

        travel = each_content[9].split("-")
        dt = parse(travel[0])
        startdate = dt.strftime('%Y-%m-%d')
        print("dep_date_from :", startdate)

        dt = parse(travel[1])
        enddate = dt.strftime('%Y-%m-%d')
        print("dep_date_to :", enddate)

        stay = each_content[11].split('/')
        print("Minimum Stay :", stay[0])
        print("Maximim Stay :", stay[1])
        if each_content[13] == "Economy":
          classes = "Y"
          print(classes)
        if each_content[13] == "Business":
          classes = "J"
          print(classes)
        fares = each_content[15].split(" ")
        print("Fare : ", fares[1])
        print("Currency :", fares[0])

        #Storing the parameters in a dictionary
        data_doc = dict()
        data_doc = {
          "Airline" : "EY",
          "OD" : origin+destination,
          "Valid from" : "",
          "Valid till" : valid_till,
          "Compartment" : classes,
          "Fare" : fares[1],
          "Currency" : fares[0],
          "Minimum Stay" : stay[0],
          "Maximum Stay" : stay[1],
          "dep_date_from" : startdate,
          "dep_date_to" : enddate,
          "Url" : urls
          }

        doc = {
          "Airline" : "EY",
          "OD" : origin+destination,
          "Valid from" : "",
          "Valid till" : valid_till,
          "Compartment" : classes,
          "Fare" : fares[1],
          "Currency" : fares[0],
          "Minimum Stay" : stay[0],
          "Maximum Stay" : stay[1],
          "dep_date_from" : startdate,
          "dep_date_to" : enddate,
          "Url" : url,
          "Last Updated Date" : datetime.datetime.now().strftime ("%Y-%m-%d"),
          "Last Updated Time" : datetime.datetime.now().strftime ("%H")
          }


        @measure(JUPITER_LOGGER)
        def rule_change():
            cursor = list(db.JUP_DB_Promotions.aggregate([
            {'$match': { 'OD': data_doc['OD'], 'Airline' : data_doc['Airline'],
                        'Currency' : data_doc['Currency'], 'Fare': data_doc['Fare'],'dep_date_from': data_doc['dep_date_from'] , 'dep_date_to': data_doc['dep_date_to'] } },
              {"$sort":{"Last Updated Date":-1, "Last Updated Time": -1}}]))
            print cursor
            if len(cursor) > 0:
                if (cursor[0]['Valid from'] == data_doc['Valid from']) and (cursor[0]['Valid till'] == data_doc['Valid till']) \
                        and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (cursor[0]['Maximum Stay'] == data_doc['Maximum Stay']) and (cursor[0]['Compartment']== data_doc['Compartment']):
                    pass
                elif (cursor[0]['Valid from'] != data_doc['Valid from']) and (cursor[0]['Valid till'] == data_doc['Valid till']) \
                        and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (cursor[0]['Maximum Stay'] == data_doc['Maximum Stay'] and (cursor[0]['Compartment']== data_doc['Compartment'])):
                    print "Raise Rule Change Trigger (Valid from is updated)" ### Call the trigger
                    promo_rule_change_trigger = PromoRuleChangeTrigger("promotions_ruleschange",
                                                                       old_doc_data=cursor[0],
                                                                       new_doc_data=doc,
                                                                       changed_field="Valid from")
                    promo_rule_change_trigger.do_analysis()

                elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (cursor[0]['Valid till'] != data_doc['Valid till']) \
                        and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (cursor[0]['Maximum Stay'] == data_doc['Maximum Stay'] and (cursor[0]['Compartment']== data_doc['Compartment'])):
                    print "Raise Rule Change Trigger (Valid till is updated)" ### Call the trigger
                    promo_rule_change_trigger = PromoRuleChangeTrigger("promotions_ruleschange",
                                                                       old_doc_data=cursor[0],
                                                                       new_doc_data=doc,
                                                                       changed_field="Valid till")
                    promo_rule_change_trigger.do_analysis()

                elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (cursor[0]['Valid till'] == data_doc['Valid till']) \
                        and (cursor[0]['Minimum Stay'] != data_doc['Minimum Stay']) and (cursor[0]['Maximum Stay'] == data_doc['Maximum Stay'] and (cursor[0]['Compartment']== data_doc['Compartment'])):
                    print "Raise Rule Change Trigger (Minimum Stay is updated)" ### Call the trigger
                    promo_rule_change_trigger = PromoRuleChangeTrigger("promotions_ruleschange",
                                                                       old_doc_data=cursor[0],
                                                                       new_doc_data=doc,
                                                                       changed_field="Minimum Stay")
                    promo_rule_change_trigger.do_analysis()
                elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (cursor[0]['Valid till'] == data_doc['Valid till']) \
                        and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (cursor[0]['Maximum Stay'] != data_doc['Maximum Stay'] and (cursor[0]['Compartment']== data_doc['Compartment'])):
                    print "Raise Rule Change Trigger (Maximum Stay is updated)" ### Call the trigger
                    promo_rule_change_trigger = PromoRuleChangeTrigger("promotions_ruleschange",
                                                                       old_doc_data=cursor[0],
                                                                       new_doc_data=doc,
                                                                       changed_field="Maximum Stay")
                    promo_rule_change_trigger.do_analysis()
                elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (cursor[0]['Valid till'] == data_doc['Valid till']) \
                        and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (cursor[0]['Maximum Stay'] == data_doc['Maximum Stay'] and (cursor[0]['Compartment']!= data_doc['Compartment'])):
                    print "Raise Rule Change Trigger (Compartment is updated)" ### Call the trigger
                    promo_rule_change_trigger = PromoRuleChangeTrigger("promotions_ruleschange",
                                                                       old_doc_data=cursor[0],
                                                                       new_doc_data=doc,
                                                                       changed_field="Compartment")
                    promo_rule_change_trigger.do_analysis()

            else:
                pass


        @measure(JUPITER_LOGGER)
        def date_change():
            cursor = list(db.JUP_DB_Promotions.aggregate([
                {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                            'Compartment': data_doc['Compartment'], 'Currency': data_doc['Currency'],
                            'Fare': data_doc['Fare'], 'Valid from': data_doc['Valid from'], 'Valid till' :data_doc['Valid till'],
                  'Minimum Stay': data_doc['Minimum Stay'], 'Maximum Stay': data_doc['Maximum Stay']}},
                {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
            print cursor
            if len(cursor) > 0:

                if (cursor[0]['dep_date_from'] == data_doc['dep_date_from']) and (cursor[0]['dep_date_to'] == data_doc['dep_date_to']):
                    pass
                elif (cursor[0]['dep_date_from'] != data_doc['dep_date_from']) and (cursor[0]['dep_date_to'] == data_doc['dep_date_to']):
                    print "Raise Date Change Trigger (dep date from is updated)" ### Call the trigger
                    promo_date_change_trigger = PromoDateChangeTrigger("promotions_dateschange",
                                                                       old_doc_data=cursor[0],
                                                                       new_doc_data=doc,
                                                                       changed_field="dep_date_from")
                    promo_date_change_trigger.do_analysis()
                elif (cursor[0]['dep_date_from'] == data_doc['dep_date_from']) and (cursor[0]['dep_date_to'] != data_doc['dep_date_to']):
                    print "Raise Date Change Trigger (dep date from is updated)" ### Call the trigger
                    promo_date_change_trigger = PromoDateChangeTrigger("promotions_dateschange",
                                                                       old_doc_data=cursor[0],
                                                                       new_doc_data=doc,
                                                                       changed_field="dep_date_to")
                    promo_date_change_trigger.do_analysis()

            else:
                pass


        @measure(JUPITER_LOGGER)
        def fare_change():
            cursor = list(db.JUP_DB_Promotions.aggregate([
                {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                            'Compartment': data_doc['Compartment'], 'Currency': data_doc['Currency'],
                            'Valid from': data_doc['Valid from'], 'dep_date_from': data_doc['dep_date_from'] , 'dep_date_to': data_doc['dep_date_to'],
                            'Valid till': data_doc['Valid till'], 'Minimum Stay': data_doc['Minimum Stay'], 'Maximum Stay': data_doc['Maximum Stay']}},
                {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
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
        change_flag_list = []
        print "working"


        @measure(JUPITER_LOGGER)
        def new_promotion():
            cursor = list(db.JUP_DB_Promotions.aggregate([
                {'$match': { 'Airline' : data_doc['Airline']}} ]))
            print cursor
            if len(cursor) > 0:
                for i in range(len(cursor)):
                    if (cursor[i]['Valid from'] == data_doc['Valid from']) and (cursor[i]['Valid till'] == data_doc['Valid till']) \
                        and (cursor[i]['Minimum Stay'] == data_doc['Minimum Stay']) and (cursor[i]['Maximum Stay'] == data_doc['Maximum Stay']) and (cursor[i]['Compartment']== data_doc['Compartment']
                        and (cursor[i]['OD'] == data_doc['OD']) and (cursor[i]['Fare'] == data_doc['Fare']) and (cursor[i]['Currency']== data_doc['Currency']) and (cursor[i]['dep_date_from']== data_doc['dep_date_from']) and (cursor[i]['dep_date_to']== data_doc['dep_date_to'])):
                        pass

                    elif (cursor[i]['Valid from'] == data_doc['Valid from']) and (cursor[i]['Valid till'] == data_doc['Valid till']) \
                        and (cursor[i]['Minimum Stay'] == data_doc['Minimum Stay']) and (cursor[i]['Maximum Stay'] == data_doc['Maximum Stay']) and (cursor[i]['Compartment']== data_doc['Compartment']
                        and (cursor[i]['OD'] == data_doc['OD']) and (cursor[i]['Fare'] == data_doc['Fare']) and (cursor[i]['Currency']!= data_doc['Currency']) and (cursor[i]['dep_date_from']== data_doc['dep_date_from']) and (cursor[i]['dep_date_to']== data_doc['dep_date_to'])):
                        print("Raise New Promotion Released Trigger, Currency has been updated")
                        promo_new_promotion_trigger = PromoNewPromotionTrigger("new_promotions",
                                                                           old_doc_data=cursor[i],
                                                                           new_doc_data=doc,
                                                                           changed_field="Currency")
                        promo_new_promotion_trigger.do_analysis()

                    elif (cursor[i]['Valid from'] == data_doc['Valid from']) and (cursor[i]['Valid till'] == data_doc['Valid till']) \
                            and (cursor[i]['Minimum Stay'] == data_doc['Minimum Stay']) and (cursor[i]['Maximum Stay'] == data_doc['Maximum Stay']) and (
                            cursor[i]['Compartment'] == data_doc['Compartment']
                            and (cursor[i]['OD'] != data_doc['OD']) and (cursor[i]['Fare'] == data_doc['Fare']) and (
                            cursor[i]['Currency'] == data_doc['Currency']) and (cursor[i]['dep_date_from'] == data_doc['dep_date_from']) and (
                        cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                        print("Raise New Promotion Released Trigger, OD has been updated")
                        promo_new_promotion_trigger = PromoNewPromotionTrigger("new_promotions",
                                                                               old_doc_data=cursor[i],
                                                                               new_doc_data=doc,
                                                                               changed_field="OD")
                        promo_new_promotion_trigger.do_analysis()

                    elif cursor[i]['Valid  from'] != data_doc['Valid from']:
                        change_flag_list.append(1)
                    elif cursor[i]['Valid till'] != data_doc['Valid till']:
                        change_flag_list.append(2)
                    elif cursor[i]['Minimum Stay'] != data_doc['Minimum Stay']:
                        change_flag_list.append(3)
                    elif cursor[i]['Maximum Stay'] != data_doc['Maximum Stay']:
                        change_flag_list.append(4)
                    elif cursor[i]['Compartment'] != data_doc['Compartment']:
                        change_flag_list.append(5)
                    elif cursor[i]['OD'] != data_doc['OD']:
                        change_flag_list.append(6)
                    elif cursor[i]['Fare'] != data_doc['Fare']:
                        change_flag_list.append(7)
                    elif cursor[i]['Currency'] != data_doc['Currency']:
                        change_flag_list.append(8)
                    elif cursor[i]['dep_date_from'] != data_doc['dep_date_from']:
                        change_flag_list.append(9)
                    elif cursor[i]['dep_date_to'] != data_doc['dep_date_to']:
                        change_flag_list.append(10)
                    print ("change_flag_list:" )
                    print change_flag_list
                    if len(change_flag_list) > 2:
                        print("Raise New Promotion Released Trigger, new promotions are released")
                        promo_new_promotion_trigger = PromoNewPromotionTrigger("new_promotions",
                                                                               old_doc_data=cursor[i],
                                                                               new_doc_data=doc,
                                                                               changed_field="new")
                        promo_new_promotion_trigger.do_analysis()

        rule_change()
        date_change()
        fare_change()
        new_promotion()


        ##Upserting the parameters into mongodb
        db.JUP_DB_Promotions.update(data_doc,doc, upsert = True)
        print "yes"



    country = ["eg", "sd", "tz", "ug", "bd", "cn", "in", "np", "pk", "th", "vn", "at", "ro", "ru", "tr", "rs", "bh", "ir", "iq", "jo", "kw", "ae", "lb", "om", "qa", "sa"]
    for each_country in country:
        print "country_name: ", each_country
        driver.get("http://www.etihad.com/en-"+each_country+"/")
        deals = driver.find_element_by_xpath("//*[@id='navMain']/li[4]/a").click()
        time.sleep(10)
        promotion_table = driver.find_elements_by_xpath("//*[@id='promos']/article/div/div[4]/ul")
        for lists in promotion_table:
          try:
              urls = lists.find_elements_by_tag_name("li")
              for eachURL in urls:
                url = eachURL.find_elements_by_tag_name("a")
                for Url_list in url:
                  urlss = Url_list.get_attribute("'")
                  print "urlss = ",urlss
                  print "type(urlss) = ", type(urlss)
                  #Calling the function
                  if urlss is not None:
                    promotions(urlss)

          except:
              pass

        #Promotion table2 is not present for all the countries
        try:
          promotion_table2 = driver.find_elements_by_xpath("//*[@id='promos']/div/article[1]/div/ul")
          for lists in promotion_table2:
            urls = lists.find_elements_by_tag_name("li")
            for eachURL in urls:
              url = eachURL.find_elements_by_tag_name("a")
              for Url_list in url:
                urlss = Url_list.get_attribute("href")
                print "urls = ",urlss
                print "type(urlss) = ", type(urlss)
                #Calling the function
                if urlss is not None:
                    promotions(urlss)
        except:
          pass

        #Promotion table3 is not present for all the countries
        try:
          promotion_table3 = driver.find_elements_by_xpath("//*[@id='promos']/div/article[2]/div/ul")
          for lists in promotion_table3:
            urls = lists.find_elements_by_tag_name("li")
            for eachURL in urls:
              url = eachURL.find_elements_by_tag_name("a")
              for Url_list in url:
                urlss = Url_list.get_attribute("href")
                print "urlss = " ,urlss
                print "type(urlss) = ", type(urlss)
                #Calling the function
                if urlss is not None:
                    promotions(urlss)
        except:
          pass
    driver.close()
if __name__ == "__main__":
    run()