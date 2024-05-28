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

  # Connecting to Mongodb
  import pymongo
  client = pymongo.MongoClient("mongodb://analytics:KNjSZmiaNUGLmS0Bv2@13.92.251.7:42525/")
  db = client['fzDB']

  #Defining the Chrome Path
  chrome_path = r"/home/prathyusha/Downloads/chromedriver"
  driver = webdriver.Chrome(chrome_path)
  driver.get("https://www.qantas.com/in/en/flight-deals.html/amd/all/economy/all/lowest")
  driver1 = webdriver.Chrome(chrome_path)
  driver2 = webdriver.Chrome(chrome_path)
  driver3 = webdriver.Chrome(chrome_path)
  wait = WebDriverWait(driver, 20)
  wait = WebDriverWait(driver1, 20)
  wait = WebDriverWait(driver2, 20)
  wait = WebDriverWait(driver3, 20)


  #Function to scrape the promotions


  @measure(JUPITER_LOGGER)
  def promotions(url):
    driver1.get(url)
    #wait.until(EC.visibility_of_element_located((By.XPATH, "//*[@id='flight-offers']/div/div[3]/div[4]/div/div/ul")))
    OD =driver1.find_element_by_xpath("//*[@id='main']/div[2]")
    ODs = OD.find_element_by_tag_name("h1")
    each_origin = ODs.text.split("to")
    print(each_origin)
    orig = each_origin[0].split(" ")
    origin = orig[1]
    print(orig[1])
    dest = each_origin[1]
    destination = dest
    print(destination)
    cursor = db.JUP_DB_IATA_Codes.find()
    try:
      for i in cursor:
        if all(word in i['City'].lower() for word in re.findall(r'\w+',orig[1].lower())):
          origin = i["Code"]
          #print("Origin :",origin)
        if all(word in i['City'].lower() for word in re.findall(r'\w+',dest.lower())):
          destination = i["Code"]
          #print("Destination :",destination)
      print(origin+destination)
    except:
      pass

    try:

      price_table = driver1.find_element_by_xpath("//*[@id='flight-offers']/div/div[3]/div[4]/div/div/ul")
      price = price_table.find_elements_by_tag_name("li")
      for i in range(len(price)):
        req_content = price[i].text.splitlines()
        print req_content
        try:
          if len(req_content)==8:
            print(req_content)
            print(len(req_content))
            content = req_content
            dt = parse(content[1])
            startdate = dt.strftime('%Y-%m-%d')
            print("dep_date_from : ", startdate)
            dt = parse(content[3])
            enddate = dt.strftime('%Y-%m-%d')
            print("dep_date_to : ", enddate)
            cls = content[5].split(" ")
            print("Type :", cls[0])
            if "Economy" in cls[1]:
              classes = "Y"
              print(classes)
            if "Business" in cls[1]:
              classes = "J"
              print(classes)
            if "First" in cls[1]:
              classes = "Z"
              print(classes)
            if "Premium" in cls[1]:
              classes = "Z"
              print(classes)
            print("Fare : ", content[6][3:-1])
            print("Currency :", content[6][:3])

            #Storing the parameters in a dictionary
            data_doc = {
            "Airline" : "QF",
            "OD" : origin+destination,
            "Valid from" : '',
            "Valid till" : '',
            "Type" : cls[0],
            "Compartment" : classes,
            "Fare" : content[6][3:-1],
            "Currency" : content[6][:3],
            "Minimum Stay" : "Nill",
            "Maximum Stay" : "12 Months",
            "dep_date_from" : startdate,
            "dep_date_to" : enddate,
            "Url" : url
             }
            doc = {
            "Airline" : "QF",
            "OD" : origin+destination,
            "Valid from" : '',
            "Valid till" : '',
            "Type" : cls[0],
            "Compartment" : classes,
            "Fare" : content[6][3:-1],
            "Currency" : content[6][:3],
            "Minimum Stay" : "Nill",
            "Maximum Stay" : "12 Months",
            "dep_date_from" : startdate,
            "dep_date_to" : enddate,
            "Url" : url,
            "Last Updated Date" : datetime.datetime.now().strftime ("%Y-%m-%d"),
            "Last Updated Time" : datetime.datetime.now().strftime ("%H")
             }


            @measure(JUPITER_LOGGER)
            def rule_change():
              cursor = list(db.JUP_DB_Promotions.aggregate([
                {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                            'Currency': data_doc['Currency'], 'Fare': data_doc['Fare'],
                            'dep_date_from': data_doc['dep_date_from'], 'dep_date_to': data_doc['dep_date_to']}},
                {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
              print cursor
              if len(cursor) > 0:
                if (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                  cursor[0]['Valid till'] == data_doc['Valid till']) \
                        and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                  cursor[0]['Maximum Stay'] == data_doc['Maximum Stay']) and (
                  cursor[0]['Compartment'] == data_doc['Compartment']):
                  pass
                elif (cursor[0]['Valid from'] != data_doc['Valid from']) and (
                  cursor[0]['Valid till'] == data_doc['Valid till']) \
                        and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                    cursor[0]['Maximum Stay'] == data_doc['Maximum Stay'] and (
                  cursor[0]['Compartment'] == data_doc['Compartment'])):
                  print "Raise Rule Change Trigger (Valid from is updated)"  ### Call the trigger
                  promo_rule_change_trigger = PromoRuleChangeTrigger("Promotions Rule Change Trigger",
                                                                     old_doc_data=cursor[0],
                                                                     new_doc_data=doc,
                                                                     changed_field="Valid from")
                  promo_rule_change_trigger.do_analysis()

                elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                  cursor[0]['Valid till'] != data_doc['Valid till']) \
                        and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                    cursor[0]['Maximum Stay'] == data_doc['Maximum Stay'] and (
                  cursor[0]['Compartment'] == data_doc['Compartment'])):
                  print "Raise Rule Change Trigger (Valid till is updated)"  ### Call the trigger
                  promo_rule_change_trigger = PromoRuleChangeTrigger("Promotions Rule Change Trigger",
                                                                     old_doc_data=cursor[0],
                                                                     new_doc_data=doc,
                                                                     changed_field="Valid till")
                  promo_rule_change_trigger.do_analysis()

                elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                  cursor[0]['Valid till'] == data_doc['Valid till']) \
                        and (cursor[0]['Minimum Stay'] != data_doc['Minimum Stay']) and (
                    cursor[0]['Maximum Stay'] == data_doc['Maximum Stay'] and (
                  cursor[0]['Compartment'] == data_doc['Compartment'])):
                  print "Raise Rule Change Trigger (Minimum Stay is updated)"  ### Call the trigger
                  promo_rule_change_trigger = PromoRuleChangeTrigger("Promotions Rule Change Trigger",
                                                                     old_doc_data=cursor[0],
                                                                     new_doc_data=doc,
                                                                     changed_field="Minimum Stay")
                  promo_rule_change_trigger.do_analysis()
                elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                  cursor[0]['Valid till'] == data_doc['Valid till']) \
                        and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                    cursor[0]['Maximum Stay'] != data_doc['Maximum Stay'] and (
                  cursor[0]['Compartment'] == data_doc['Compartment'])):
                  print "Raise Rule Change Trigger (Maximum Stay is updated)"  ### Call the trigger
                  promo_rule_change_trigger = PromoRuleChangeTrigger("Promotions Rule Change Trigger",
                                                                     old_doc_data=cursor[0],
                                                                     new_doc_data=doc,
                                                                     changed_field="Maximum Stay")
                  promo_rule_change_trigger.do_analysis()
                elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                  cursor[0]['Valid till'] == data_doc['Valid till']) \
                        and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                    cursor[0]['Maximum Stay'] == data_doc['Maximum Stay'] and (
                  cursor[0]['Compartment'] != data_doc['Compartment'])):
                  print "Raise Rule Change Trigger (Compartment is updated)"  ### Call the trigger
                  promo_rule_change_trigger = PromoRuleChangeTrigger("Promotions Rule Change Trigger",
                                                                     old_doc_data=cursor[0],
                                                                     new_doc_data=doc,
                                                                     changed_field="Compartment")
                  promo_rule_change_trigger.do_analysis()

              else:
                print("Raise New Promotion Released Trigger")
                promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                                                                       old_doc_data=cursor[0],
                                                                       new_doc_data=doc,
                                                                       changed_field="Rules")
                promo_new_promotion_trigger.do_analysis()


            @measure(JUPITER_LOGGER)
            def date_change():
              cursor = list(db.JUP_DB_Promotions.aggregate([
                {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                            'Compartment': data_doc['Compartment'], 'Currency': data_doc['Currency'],
                            'Fare': data_doc['Fare'], 'Valid from': data_doc['Valid from'],
                            'Valid till': data_doc['Valid till'],
                            'Minimum Stay': data_doc['Minimum Stay'], 'Maximum Stay': data_doc['Maximum Stay']}},
                {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
              print cursor
              if len(cursor) > 0:

                if (cursor[0]['dep_date_from'] == data_doc['dep_date_from']) and (
                  cursor[0]['dep_date_to'] == data_doc['dep_date_to']):
                  pass
                elif (cursor[0]['dep_date_from'] != data_doc['dep_date_from']) and (
                  cursor[0]['dep_date_to'] == data_doc['dep_date_to']):
                  print "Raise Date Change Trigger (dep date from is updated)"  ### Call the trigger
                  promo_date_change_trigger = PromoDateChangeTrigger("Promotions Date Change Trigger",
                                                                     old_doc_data=cursor[0],
                                                                     new_doc_data=doc,
                                                                     changed_field="dep_date_from")
                  promo_date_change_trigger.do_analysis()
                elif (cursor[0]['dep_date_from'] == data_doc['dep_date_from']) and (
                  cursor[0]['dep_date_to'] != data_doc['dep_date_to']):
                  print "Raise Date Change Trigger (dep date from is updated)"  ### Call the trigger
                  promo_date_change_trigger = PromoDateChangeTrigger("Promotions Date Change Trigger",
                                                                     old_doc_data=cursor[0],
                                                                     new_doc_data=doc,
                                                                     changed_field="dep_date_to")
                  promo_date_change_trigger.do_analysis()

              else:
                print("Raise New Promotion Released Trigger")
                promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                                                                       old_doc_data=cursor[0],
                                                                       new_doc_data=doc,
                                                                       changed_field="Dates")
                promo_new_promotion_trigger.do_analysis()


            @measure(JUPITER_LOGGER)
            def fare_change():
              cursor = list(db.JUP_DB_Promotions.aggregate([
                {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                            'Compartment': data_doc['Compartment'], 'Currency': data_doc['Currency'],
                            'Valid from': data_doc['Valid from'], 'dep_date_from': data_doc['dep_date_from'],
                            'dep_date_to': data_doc['dep_date_to'],
                            'Valid till': data_doc['Valid till'], 'Minimum Stay': data_doc['Minimum Stay'],
                            'Maximum Stay': data_doc['Maximum Stay']}},
                {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
              print cursor
              if len(cursor) > 0:

                if (cursor[0]['Fare'] == data_doc['Fare']):
                  pass
                elif (cursor[0]['Fare'] != data_doc['Fare']):
                  print "Raise Fare Change Trigger (Fare is updated)"  ### Call the trigger
                  promo_fare_change_trigger = PromoFareChangeTrigger("Promotions Fare Change Trigger",
                                                                     old_doc_data=cursor[0],
                                                                     new_doc_data=doc,
                                                                     changed_field="Fare")
                  promo_fare_change_trigger.do_analysis()
              else:
                pass
                # print("Raise New Promotion Released Trigger")
                # promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                #                                                        old_doc_data=cursor[0],
                #                                                        new_doc_data=doc,
                #                                                        changed_field="")
                # promo_new_promotion_trigger.do_analysis()


            @measure(JUPITER_LOGGER)
            def new_promotion():
              cursor = list(db.JUP_DB_Promotions.aggregate([
                {'$match': {'Airline': data_doc['Airline']}}]))
              print cursor
              if len(cursor) > 0:
                for i in range(len(cursor)):
                  if (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                    cursor[i]['Valid till'] == data_doc['Valid till']) \
                          and (cursor[i]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                    cursor[i]['Maximum Stay'] == data_doc['Maximum Stay']) and (
                              cursor[i]['Compartment'] == data_doc['Compartment']
                          and (cursor[i]['OD'] == data_doc['OD']) and (cursor[i]['Fare'] == data_doc['Fare']) and (
                        cursor[i]['Currency'] == data_doc['Currency']) and (
                      cursor[i]['dep_date_from'] == data_doc['dep_date_from']) and (
                    cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                    pass

                  elif (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                    cursor[i]['Valid till'] == data_doc['Valid till']) \
                          and (cursor[i]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                    cursor[i]['Maximum Stay'] == data_doc['Maximum Stay']) and (
                              cursor[i]['Compartment'] == data_doc['Compartment']
                          and (cursor[i]['OD'] == data_doc['OD']) and (cursor[i]['Fare'] == data_doc['Fare']) and (
                        cursor[i]['Currency'] != data_doc['Currency']) and (
                      cursor[i]['dep_date_from'] == data_doc['dep_date_from']) and (
                    cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                    print("Raise New Promotion Released Trigger")
                    promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                                                                           old_doc_data=cursor[i],
                                                                           new_doc_data=doc,
                                                                           changed_field="Currency")
                    promo_new_promotion_trigger.do_analysis()

                  elif (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                    cursor[i]['Valid till'] == data_doc['Valid till']) \
                          and (cursor[i]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                    cursor[i]['Maximum Stay'] == data_doc['Maximum Stay']) and (
                                      cursor[i]['Compartment'] == data_doc['Compartment']
                                  and (cursor[i]['OD'] != data_doc['OD']) and (
                                  cursor[i]['Fare'] == data_doc['Fare']) and (
                                        cursor[i]['Currency'] == data_doc['Currency']) and (
                              cursor[i]['dep_date_from'] == data_doc['dep_date_from']) and (
                                    cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                    print("Raise New Promotion Released Trigger")
                    promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                                                                           old_doc_data=cursor[i],
                                                                           new_doc_data=doc,
                                                                           changed_field="OD")
                    promo_new_promotion_trigger.do_analysis()

                  elif (cursor[i]['Valid from'] != data_doc['Valid from']) and (
                    cursor[i]['Valid till'] != data_doc['Valid till']) \
                          and (cursor[i]['Minimum Stay'] != data_doc['Minimum Stay']) and (
                    cursor[i]['Maximum Stay'] != data_doc['Maximum Stay']) and (
                              cursor[i]['Compartment'] != data_doc['Compartment'] and (
                            cursor[i]['OD'] != data_doc['OD']) and (cursor[i]['Fare'] != data_doc['Fare']) and (
                        cursor[i]['Currency'] != data_doc['Currency']) and (
                              cursor[i]['dep_date_from'] != data_doc['dep_date_from']) and (
                    cursor[i]['dep_date_to'] != data_doc['dep_date_to'])):
                    print("Raise New Promotion Released Trigger")
                  promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                                                                         old_doc_data=cursor[i],
                                                                         new_doc_data=doc,
                                                                         changed_field="new")
                  promo_new_promotion_trigger.do_analysis()

            #Upserting the values into Mongodb
            db.JUP_DB_Promotions.update(data_doc,doc, upsert = True)
            print "yes"

        except:
          if len(req_content)>8:
            print(req_content)
            print(len(req_content))
            content2 = req_content

            dt = parse(content2[1])
            startdate = dt.strftime('%Y-%m-%d')
            print("dep_date_from : ", startdate)
            dt = parse(content[4])
            enddate = dt.strftime('%Y-%m-%d')
            print("dep_date_to : ", enddate)
            dt = parse(content[6])
            valid_till = dt.strftime('%Y-%m-%d')
            print("Valid_till : ", valid_till)
            cls = content[7].split(" ")
            print("Type :", cls[0])
            if "Economy" in cls[1]:
              classes = "Y"
              print(classes)
            if "Business" in cls[1]:
              classes = "J"
              print(classes)
            if "First" in cls[1]:
              classes = "Z"
              print(classes)
            if "Premium" in cls[1]:
              classes = "Z"
              print(classes)
            print("Fare : ", content[8][3:-1])
            print("Currency :", content[8][:3])




            data_doc = {
              "Airline" : "QF",
              "OD" : origin+destination,
              "Valid from" : '',
              "Valid till" : valid_till,
              "Type" : cls[0],
              "Compartment" : classes,
              "Fare" : content[8][3:-1],
              "Currency" : content[8][:3],
              "Minimum Stay" : "Nill",
              "Maximum Stay" : "12 Months",
              "dep_date_from" : startdate,
              "dep_date_to" : enddate,
              "Url" : url
               }
            doc = {
              "Airline" : "QF",
              "OD" : origin+destination,
              "Valid from" : '',
              "Valid till" : valid_till,
              "Type" : cls[0],
              "Compartment" : classes,
              "Fare" : content[8][3:-1],
              "Currency" : content[8][:3],
              "Minimum Stay" : "Nill",
              "Maximum Stay" : "12 Months",
              "dep_date_from" : startdate,
              "dep_date_to" : enddate,
              "Url" : url,
              "Last Updated Date" : datetime.datetime.now().strftime ("%Y-%m-%d"),
              "Last Updated Time" : datetime.datetime.now().strftime ("%H")
               }
            print data_doc
            print doc


            @measure(JUPITER_LOGGER)
            def rule_change():
              cursor = list(db.JUP_DB_Promotions.aggregate([
                {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                            'Currency': data_doc['Currency'], 'Fare': data_doc['Fare'],
                            'dep_date_from': data_doc['dep_date_from'], 'dep_date_to': data_doc['dep_date_to']}},
                {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
              print cursor
              if len(cursor) > 0:
                if (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                  cursor[0]['Valid till'] == data_doc['Valid till']) \
                        and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                  cursor[0]['Maximum Stay'] == data_doc['Maximum Stay']) and (
                  cursor[0]['Compartment'] == data_doc['Compartment']):
                  pass
                elif (cursor[0]['Valid from'] != data_doc['Valid from']) and (
                  cursor[0]['Valid till'] == data_doc['Valid till']) \
                        and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                    cursor[0]['Maximum Stay'] == data_doc['Maximum Stay'] and (
                  cursor[0]['Compartment'] == data_doc['Compartment'])):
                  print "Raise Rule Change Trigger (Valid from is updated)"  ### Call the trigger
                  promo_rule_change_trigger = PromoRuleChangeTrigger("Promotions Rule Change Trigger",
                                                                     old_doc_data=cursor[0],
                                                                     new_doc_data=doc,
                                                                     changed_field="Valid from")
                  promo_rule_change_trigger.do_analysis()

                elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                  cursor[0]['Valid till'] != data_doc['Valid till']) \
                        and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                    cursor[0]['Maximum Stay'] == data_doc['Maximum Stay'] and (
                  cursor[0]['Compartment'] == data_doc['Compartment'])):
                  print "Raise Rule Change Trigger (Valid till is updated)"  ### Call the trigger
                  promo_rule_change_trigger = PromoRuleChangeTrigger("Promotions Rule Change Trigger",
                                                                     old_doc_data=cursor[0],
                                                                     new_doc_data=doc,
                                                                     changed_field="Valid till")
                  promo_rule_change_trigger.do_analysis()

                elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                  cursor[0]['Valid till'] == data_doc['Valid till']) \
                        and (cursor[0]['Minimum Stay'] != data_doc['Minimum Stay']) and (
                    cursor[0]['Maximum Stay'] == data_doc['Maximum Stay'] and (
                  cursor[0]['Compartment'] == data_doc['Compartment'])):
                  print "Raise Rule Change Trigger (Minimum Stay is updated)"  ### Call the trigger
                  promo_rule_change_trigger = PromoRuleChangeTrigger("Promotions Rule Change Trigger",
                                                                     old_doc_data=cursor[0],
                                                                     new_doc_data=doc,
                                                                     changed_field="Minimum Stay")
                  promo_rule_change_trigger.do_analysis()
                elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                  cursor[0]['Valid till'] == data_doc['Valid till']) \
                        and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                    cursor[0]['Maximum Stay'] != data_doc['Maximum Stay'] and (
                  cursor[0]['Compartment'] == data_doc['Compartment'])):
                  print "Raise Rule Change Trigger (Maximum Stay is updated)"  ### Call the trigger
                  promo_rule_change_trigger = PromoRuleChangeTrigger("Promotions Rule Change Trigger",
                                                                     old_doc_data=cursor[0],
                                                                     new_doc_data=doc,
                                                                     changed_field="Maximum Stay")
                  promo_rule_change_trigger.do_analysis()
                elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                  cursor[0]['Valid till'] == data_doc['Valid till']) \
                        and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                    cursor[0]['Maximum Stay'] == data_doc['Maximum Stay'] and (
                  cursor[0]['Compartment'] != data_doc['Compartment'])):
                  print "Raise Rule Change Trigger (Compartment is updated)"  ### Call the trigger
                  promo_rule_change_trigger = PromoRuleChangeTrigger("Promotions Rule Change Trigger",
                                                                     old_doc_data=cursor[0],
                                                                     new_doc_data=doc,
                                                                     changed_field="Compartment")
                  promo_rule_change_trigger.do_analysis()

              else:
                print("Raise New Promotion Released Trigger")
                promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                                                                       old_doc_data=cursor[0],
                                                                       new_doc_data=doc,
                                                                       changed_field="Rules")
                promo_new_promotion_trigger.do_analysis()


            @measure(JUPITER_LOGGER)
            def date_change():
              cursor = list(db.JUP_DB_Promotions.aggregate([
                {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                            'Compartment': data_doc['Compartment'], 'Currency': data_doc['Currency'],
                            'Fare': data_doc['Fare'], 'Valid from': data_doc['Valid from'],
                            'Valid till': data_doc['Valid till'],
                            'Minimum Stay': data_doc['Minimum Stay'], 'Maximum Stay': data_doc['Maximum Stay']}},
                {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
              print cursor
              if len(cursor) > 0:

                if (cursor[0]['dep_date_from'] == data_doc['dep_date_from']) and (
                  cursor[0]['dep_date_to'] == data_doc['dep_date_to']):
                  pass
                elif (cursor[0]['dep_date_from'] != data_doc['dep_date_from']) and (
                  cursor[0]['dep_date_to'] == data_doc['dep_date_to']):
                  print "Raise Date Change Trigger (dep date from is updated)"  ### Call the trigger
                  promo_date_change_trigger = PromoDateChangeTrigger("Promotions Date Change Trigger",
                                                                     old_doc_data=cursor[0],
                                                                     new_doc_data=doc,
                                                                     changed_field="dep_date_from")
                  promo_date_change_trigger.do_analysis()
                elif (cursor[0]['dep_date_from'] == data_doc['dep_date_from']) and (
                  cursor[0]['dep_date_to'] != data_doc['dep_date_to']):
                  print "Raise Date Change Trigger (dep date from is updated)"  ### Call the trigger
                  promo_date_change_trigger = PromoDateChangeTrigger("Promotions Date Change Trigger",
                                                                     old_doc_data=cursor[0],
                                                                     new_doc_data=doc,
                                                                     changed_field="dep_date_to")
                  promo_date_change_trigger.do_analysis()

              else:
                print("Raise New Promotion Released Trigger")
                promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                                                                       old_doc_data=cursor[0],
                                                                       new_doc_data=doc,
                                                                       changed_field="Dates")
                promo_new_promotion_trigger.do_analysis()


            @measure(JUPITER_LOGGER)
            def fare_change():
              cursor = list(db.JUP_DB_Promotions.aggregate([
                {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                            'Compartment': data_doc['Compartment'], 'Currency': data_doc['Currency'],
                            'Valid from': data_doc['Valid from'], 'dep_date_from': data_doc['dep_date_from'],
                            'dep_date_to': data_doc['dep_date_to'],
                            'Valid till': data_doc['Valid till'], 'Minimum Stay': data_doc['Minimum Stay'],
                            'Maximum Stay': data_doc['Maximum Stay']}},
                {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
              print cursor
              if len(cursor) > 0:

                if (cursor[0]['Fare'] == data_doc['Fare']):
                  pass
                elif (cursor[0]['Fare'] != data_doc['Fare']):
                  print "Raise Fare Change Trigger (Fare is updated)"  ### Call the trigger
                  promo_fare_change_trigger = PromoFareChangeTrigger("Promotions Fare Change Trigger",
                                                                     old_doc_data=cursor[0],
                                                                     new_doc_data=doc,
                                                                     changed_field="Fare")
                  promo_fare_change_trigger.do_analysis()
              else:
                pass
                # print("Raise New Promotion Released Trigger")
                # promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                #                                                        old_doc_data=cursor[0],
                #                                                        new_doc_data=doc,
                #                                                        changed_field="")
                # promo_new_promotion_trigger.do_analysis()


            @measure(JUPITER_LOGGER)
            def new_promotion():
              cursor = list(db.JUP_DB_Promotions.aggregate([
                {'$match': {'Airline': data_doc['Airline']}}]))
              print cursor
              if len(cursor) > 0:
                for i in range(len(cursor)):
                  if (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                    cursor[i]['Valid till'] == data_doc['Valid till']) \
                          and (cursor[i]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                    cursor[i]['Maximum Stay'] == data_doc['Maximum Stay']) and (
                              cursor[i]['Compartment'] == data_doc['Compartment']
                          and (cursor[i]['OD'] == data_doc['OD']) and (cursor[i]['Fare'] == data_doc['Fare']) and (
                        cursor[i]['Currency'] == data_doc['Currency']) and (
                      cursor[i]['dep_date_from'] == data_doc['dep_date_from']) and (
                    cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                    pass

                  elif (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                    cursor[i]['Valid till'] == data_doc['Valid till']) \
                          and (cursor[i]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                    cursor[i]['Maximum Stay'] == data_doc['Maximum Stay']) and (
                              cursor[i]['Compartment'] == data_doc['Compartment']
                          and (cursor[i]['OD'] == data_doc['OD']) and (cursor[i]['Fare'] == data_doc['Fare']) and (
                        cursor[i]['Currency'] != data_doc['Currency']) and (
                      cursor[i]['dep_date_from'] == data_doc['dep_date_from']) and (
                    cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                    print("Raise New Promotion Released Trigger")
                    promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                                                                           old_doc_data=cursor[i],
                                                                           new_doc_data=doc,
                                                                           changed_field="Currency")
                    promo_new_promotion_trigger.do_analysis()

                  elif (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                    cursor[i]['Valid till'] == data_doc['Valid till']) \
                          and (cursor[i]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                    cursor[i]['Maximum Stay'] == data_doc['Maximum Stay']) and (
                                      cursor[i]['Compartment'] == data_doc['Compartment']
                                  and (cursor[i]['OD'] != data_doc['OD']) and (
                                  cursor[i]['Fare'] == data_doc['Fare']) and (
                                        cursor[i]['Currency'] == data_doc['Currency']) and (
                              cursor[i]['dep_date_from'] == data_doc['dep_date_from']) and (
                                    cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                    print("Raise New Promotion Released Trigger")
                    promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                                                                           old_doc_data=cursor[i],
                                                                           new_doc_data=doc,
                                                                           changed_field="OD")
                    promo_new_promotion_trigger.do_analysis()

                  elif (cursor[i]['Valid from'] != data_doc['Valid from']) and (
                    cursor[i]['Valid till'] != data_doc['Valid till']) \
                          and (cursor[i]['Minimum Stay'] != data_doc['Minimum Stay']) and (
                    cursor[i]['Maximum Stay'] != data_doc['Maximum Stay']) and (
                              cursor[i]['Compartment'] != data_doc['Compartment'] and (
                            cursor[i]['OD'] != data_doc['OD']) and (cursor[i]['Fare'] != data_doc['Fare']) and (
                        cursor[i]['Currency'] != data_doc['Currency']) and (
                              cursor[i]['dep_date_from'] != data_doc['dep_date_from']) and (
                    cursor[i]['dep_date_to'] != data_doc['dep_date_to'])):
                    print("Raise New Promotion Released Trigger")
                  promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                                                                         old_doc_data=cursor[i],
                                                                         new_doc_data=doc,
                                                                         changed_field="new")
                  promo_new_promotion_trigger.do_analysis()

            db.JUP_DB_Promotions.update(data_doc,doc, upsert = True)
          print "yes"
    except:
      pass


  @measure(JUPITER_LOGGER)
  def promotions2(url):
    driver2.get(url)

    #wait.until(EC.visibility_of_element_located((By.XPATH, "//*[@id='flight-offers']/div/div/div[4]/div/div/ul")))
    OD =driver2.find_element_by_xpath("//*[@id='main']/div[2]")
    ODs = OD.find_element_by_tag_name("h1")
    each_origin = ODs.text.split("to")
    print(each_origin)
    orig = each_origin[0].split(" ")
    origin = orig[1]
    print(orig[1])
    dest = each_origin[1]
    destination = dest
    print(dest.strip())
    cursor = db.JUP_DB_IATA_Codes.find()
    try:
      for i in cursor:
        if all(word in i['City'].lower() for word in re.findall(r'\w+',orig[1].lower())):
          origin = i["Code"]
          #print("Origin :",origin)
        if all(word in i['City'].lower() for word in re.findall(r'\w+',dest.lower())):
          destination = i["Code"]
          #print("Destination :",destination)
      print(origin+destination)
    except:
      pass

    price_table = driver2.find_element_by_xpath("//*[@id='flight-offers']/div/div[3]/div[4]/div/div/ul")
    price = price_table.find_elements_by_tag_name("li")
    for i in range(len(price)):
      req_content = price[i].text.splitlines()
      print len(req_content)
      print req_content
      try:
        if len(req_content)==8:
          print(req_content)
          print(len(req_content))
          content = req_content
          dt = parse(content[1])
          startdate = dt.strftime('%Y-%m-%d')
          print("dep_date_from : ", startdate)
          dt = parse(content[3])
          enddate = dt.strftime('%Y-%m-%d')
          print("dep_date_to : ", enddate)
          cls = content[5].split(" ")
          print("Type :", cls[0])
          if "Business" in cls[1]:
            classes = "J"
            print(classes)

          print("Fare : ", content[6][3:-1])
          print("Currency :", content[6][:3])
          data_doc = {
          "Airline" : "QF",
          "OD" : origin+destination,
          "Valid from" : '',
          "Valid till" : '',
          "Type" : cls[0],
          "Compartment" : classes,
          "Fare" : content[6][3:-1],
          "Currency" : content[6][:3],
          "Minimum Stay" : "Nill",
          "Maximum Stay" : "12 Months",
          "dep_date_from" : startdate,
          "dep_date_to" : enddate,
          "Url" : url
           }
          doc = {
          "Airline" : "QF",
          "OD" : origin+destination,
          "Valid from" : '',
          "Valid till" : '',
          "Type" : cls[0],
          "Compartment" : classes,
          "Fare" : content[6][3:-1],
          "Currency" : content[6][:3],
          "Minimum Stay" : "Nill",
          "Maximum Stay" : "12 Months",
          "dep_date_from" : startdate,
          "dep_date_to" : enddate,
          "Url" : url,
          "Last Updated Date" : datetime.datetime.now().strftime ("%Y-%m-%d"),
          "Last Updated Time" : datetime.datetime.now().strftime ("%H")
           }
          print(data_doc)
          print doc


          @measure(JUPITER_LOGGER)
          def rule_change():
            cursor = list(db.JUP_DB_Promotions.aggregate([
              {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                          'Currency': data_doc['Currency'], 'Fare': data_doc['Fare'],
                          'dep_date_from': data_doc['dep_date_from'], 'dep_date_to': data_doc['dep_date_to']}},
              {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
            print cursor
            if len(cursor) > 0:
              if (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                cursor[0]['Valid till'] == data_doc['Valid till']) \
                      and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                cursor[0]['Maximum Stay'] == data_doc['Maximum Stay']) and (
                cursor[0]['Compartment'] == data_doc['Compartment']):
                pass
              elif (cursor[0]['Valid from'] != data_doc['Valid from']) and (
                cursor[0]['Valid till'] == data_doc['Valid till']) \
                      and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                  cursor[0]['Maximum Stay'] == data_doc['Maximum Stay'] and (
                cursor[0]['Compartment'] == data_doc['Compartment'])):
                print "Raise Rule Change Trigger (Valid from is updated)"  ### Call the trigger
                promo_rule_change_trigger = PromoRuleChangeTrigger("Promotions Rule Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="Valid from")
                promo_rule_change_trigger.do_analysis()

              elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                cursor[0]['Valid till'] != data_doc['Valid till']) \
                      and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                  cursor[0]['Maximum Stay'] == data_doc['Maximum Stay'] and (
                cursor[0]['Compartment'] == data_doc['Compartment'])):
                print "Raise Rule Change Trigger (Valid till is updated)"  ### Call the trigger
                promo_rule_change_trigger = PromoRuleChangeTrigger("Promotions Rule Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="Valid till")
                promo_rule_change_trigger.do_analysis()

              elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                cursor[0]['Valid till'] == data_doc['Valid till']) \
                      and (cursor[0]['Minimum Stay'] != data_doc['Minimum Stay']) and (
                  cursor[0]['Maximum Stay'] == data_doc['Maximum Stay'] and (
                cursor[0]['Compartment'] == data_doc['Compartment'])):
                print "Raise Rule Change Trigger (Minimum Stay is updated)"  ### Call the trigger
                promo_rule_change_trigger = PromoRuleChangeTrigger("Promotions Rule Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="Minimum Stay")
                promo_rule_change_trigger.do_analysis()
              elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                cursor[0]['Valid till'] == data_doc['Valid till']) \
                      and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                  cursor[0]['Maximum Stay'] != data_doc['Maximum Stay'] and (
                cursor[0]['Compartment'] == data_doc['Compartment'])):
                print "Raise Rule Change Trigger (Maximum Stay is updated)"  ### Call the trigger
                promo_rule_change_trigger = PromoRuleChangeTrigger("Promotions Rule Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="Maximum Stay")
                promo_rule_change_trigger.do_analysis()
              elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                cursor[0]['Valid till'] == data_doc['Valid till']) \
                      and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                  cursor[0]['Maximum Stay'] == data_doc['Maximum Stay'] and (
                cursor[0]['Compartment'] != data_doc['Compartment'])):
                print "Raise Rule Change Trigger (Compartment is updated)"  ### Call the trigger
                promo_rule_change_trigger = PromoRuleChangeTrigger("Promotions Rule Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="Compartment")
                promo_rule_change_trigger.do_analysis()

            else:
              print("Raise New Promotion Released Trigger")
              promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                                                                     old_doc_data=cursor[0],
                                                                     new_doc_data=doc,
                                                                     changed_field="Rules")
              promo_new_promotion_trigger.do_analysis()


          @measure(JUPITER_LOGGER)
          def date_change():
            cursor = list(db.JUP_DB_Promotions.aggregate([
              {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                          'Compartment': data_doc['Compartment'], 'Currency': data_doc['Currency'],
                          'Fare': data_doc['Fare'], 'Valid from': data_doc['Valid from'],
                          'Valid till': data_doc['Valid till'],
                          'Minimum Stay': data_doc['Minimum Stay'], 'Maximum Stay': data_doc['Maximum Stay']}},
              {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
            print cursor
            if len(cursor) > 0:

              if (cursor[0]['dep_date_from'] == data_doc['dep_date_from']) and (
                cursor[0]['dep_date_to'] == data_doc['dep_date_to']):
                pass
              elif (cursor[0]['dep_date_from'] != data_doc['dep_date_from']) and (
                cursor[0]['dep_date_to'] == data_doc['dep_date_to']):
                print "Raise Date Change Trigger (dep date from is updated)"  ### Call the trigger
                promo_date_change_trigger = PromoDateChangeTrigger("Promotions Date Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="dep_date_from")
                promo_date_change_trigger.do_analysis()
              elif (cursor[0]['dep_date_from'] == data_doc['dep_date_from']) and (
                cursor[0]['dep_date_to'] != data_doc['dep_date_to']):
                print "Raise Date Change Trigger (dep date from is updated)"  ### Call the trigger
                promo_date_change_trigger = PromoDateChangeTrigger("Promotions Date Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="dep_date_to")
                promo_date_change_trigger.do_analysis()

            else:
              print("Raise New Promotion Released Trigger")
              promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                                                                     old_doc_data=cursor[0],
                                                                     new_doc_data=doc,
                                                                     changed_field="Dates")
              promo_new_promotion_trigger.do_analysis()


          @measure(JUPITER_LOGGER)
          def fare_change():
            cursor = list(db.JUP_DB_Promotions.aggregate([
              {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                          'Compartment': data_doc['Compartment'], 'Currency': data_doc['Currency'],
                          'Valid from': data_doc['Valid from'], 'dep_date_from': data_doc['dep_date_from'],
                          'dep_date_to': data_doc['dep_date_to'],
                          'Valid till': data_doc['Valid till'], 'Minimum Stay': data_doc['Minimum Stay'],
                          'Maximum Stay': data_doc['Maximum Stay']}},
              {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
            print cursor
            if len(cursor) > 0:

              if (cursor[0]['Fare'] == data_doc['Fare']):
                pass
              elif (cursor[0]['Fare'] != data_doc['Fare']):
                print "Raise Fare Change Trigger (Fare is updated)"  ### Call the trigger
                promo_fare_change_trigger = PromoFareChangeTrigger("Promotions Fare Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="Fare")
                promo_fare_change_trigger.do_analysis()
            else:
              pass
              # print("Raise New Promotion Released Trigger")
              # promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
              #                                                        old_doc_data=cursor[0],
              #                                                        new_doc_data=doc,
              #                                                        changed_field="")
              # promo_new_promotion_trigger.do_analysis()


          @measure(JUPITER_LOGGER)
          def new_promotion():
            cursor = list(db.JUP_DB_Promotions.aggregate([
              {'$match': {'Airline': data_doc['Airline']}}]))
            print cursor
            if len(cursor) > 0:
              for i in range(len(cursor)):
                if (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                  cursor[i]['Valid till'] == data_doc['Valid till']) \
                        and (cursor[i]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                  cursor[i]['Maximum Stay'] == data_doc['Maximum Stay']) and (
                            cursor[i]['Compartment'] == data_doc['Compartment']
                        and (cursor[i]['OD'] == data_doc['OD']) and (cursor[i]['Fare'] == data_doc['Fare']) and (
                      cursor[i]['Currency'] == data_doc['Currency']) and (
                    cursor[i]['dep_date_from'] == data_doc['dep_date_from']) and (
                  cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                  pass

                elif (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                  cursor[i]['Valid till'] == data_doc['Valid till']) \
                        and (cursor[i]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                  cursor[i]['Maximum Stay'] == data_doc['Maximum Stay']) and (
                            cursor[i]['Compartment'] == data_doc['Compartment']
                        and (cursor[i]['OD'] == data_doc['OD']) and (cursor[i]['Fare'] == data_doc['Fare']) and (
                      cursor[i]['Currency'] != data_doc['Currency']) and (
                    cursor[i]['dep_date_from'] == data_doc['dep_date_from']) and (
                  cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                  print("Raise New Promotion Released Trigger")
                  promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                                                                         old_doc_data=cursor[i],
                                                                         new_doc_data=doc,
                                                                         changed_field="Currency")
                  promo_new_promotion_trigger.do_analysis()

                elif (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                  cursor[i]['Valid till'] == data_doc['Valid till']) \
                        and (cursor[i]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                  cursor[i]['Maximum Stay'] == data_doc['Maximum Stay']) and (
                                    cursor[i]['Compartment'] == data_doc['Compartment']
                                and (cursor[i]['OD'] != data_doc['OD']) and (
                                cursor[i]['Fare'] == data_doc['Fare']) and (
                                      cursor[i]['Currency'] == data_doc['Currency']) and (
                            cursor[i]['dep_date_from'] == data_doc['dep_date_from']) and (
                                  cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                  print("Raise New Promotion Released Trigger")
                  promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                                                                         old_doc_data=cursor[i],
                                                                         new_doc_data=doc,
                                                                         changed_field="OD")
                  promo_new_promotion_trigger.do_analysis()

                elif (cursor[i]['Valid from'] != data_doc['Valid from']) and (
                  cursor[i]['Valid till'] != data_doc['Valid till']) \
                        and (cursor[i]['Minimum Stay'] != data_doc['Minimum Stay']) and (
                  cursor[i]['Maximum Stay'] != data_doc['Maximum Stay']) and (
                            cursor[i]['Compartment'] != data_doc['Compartment'] and (
                          cursor[i]['OD'] != data_doc['OD']) and (cursor[i]['Fare'] != data_doc['Fare']) and (
                      cursor[i]['Currency'] != data_doc['Currency']) and (
                            cursor[i]['dep_date_from'] != data_doc['dep_date_from']) and (
                  cursor[i]['dep_date_to'] != data_doc['dep_date_to'])):
                  print("Raise New Promotion Released Trigger")
                promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                                                                       old_doc_data=cursor[i],
                                                                       new_doc_data=doc,
                                                                       changed_field="new")
                promo_new_promotion_trigger.do_analysis()

          db.JUP_DB_Promotions.update(data_doc,doc, upsert = True)
        print("yes")
      except:
        if len(req_content)>8:
          print(req_content)
          print(len(req_content))
          content2 = req_content

          dt = parse(content2[1])
          startdate = dt.strftime('%Y-%m-%d')
          print("dep_date_from : ", startdate)
          dt = parse(content[4])
          enddate = dt.strftime('%Y-%m-%d')
          print("dep_date_to : ", enddate)
          dt = parse(content[6])
          valid_till = dt.strftime('%Y-%m-%d')
          print("Valid_till : ", valid_till)
          cls = content[7].split(" ")
          print("Type :", cls[0])
          if "Business" in cls[1]:
            classes = "J"
            print(classes)
          print("Fare : ", content[8][3:-1])
          print("Currency :", content[8][:3])

          data_doc = {
            "Airline" : "QF",
            "OD" : origin+destination,
            "Valid from" : '',
            "Valid till" : valid_till,
            "Type" : cls[0],
            "Compartment" : classes,
            "Fare" : content[8][3:-1],
            "Currency" : content[8][:3],
            "Minimum Stay" : "Nill",
            "Maximum Stay" : "12 Months",
            "dep_date_from" : startdate,
            "dep_date_to" : enddate,
            "Url" : url
             }
          doc = {
            "Airline" : "QF",
            "OD" : origin+destination,
            "Valid from" : '',
            "Valid till" : valid_till,
            "Type" : cls[0],
            "Compartment" : classes,
            "Fare" : content[8][3:-1],
            "Currency" : content[8][:3],
            "Minimum Stay" : "Nill",
            "Maximum Stay" : "12 Months",
            "dep_date_from" : startdate,
            "dep_date_to" : enddate,
            "Url" : url,
            "Last Updated Date" : datetime.datetime.now().strftime ("%Y-%m-%d"),
            "Last Updated Time" : datetime.datetime.now().strftime ("%H")
             }
          print data_doc
          print doc


          @measure(JUPITER_LOGGER)
          def rule_change():
            cursor = list(db.JUP_DB_Promotions.aggregate([
              {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                          'Currency': data_doc['Currency'], 'Fare': data_doc['Fare'],
                          'dep_date_from': data_doc['dep_date_from'], 'dep_date_to': data_doc['dep_date_to']}},
              {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
            print cursor
            if len(cursor) > 0:
              if (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                cursor[0]['Valid till'] == data_doc['Valid till']) \
                      and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                cursor[0]['Maximum Stay'] == data_doc['Maximum Stay']) and (
                cursor[0]['Compartment'] == data_doc['Compartment']):
                pass
              elif (cursor[0]['Valid from'] != data_doc['Valid from']) and (
                cursor[0]['Valid till'] == data_doc['Valid till']) \
                      and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                  cursor[0]['Maximum Stay'] == data_doc['Maximum Stay'] and (
                cursor[0]['Compartment'] == data_doc['Compartment'])):
                print "Raise Rule Change Trigger (Valid from is updated)"  ### Call the trigger
                promo_rule_change_trigger = PromoRuleChangeTrigger("Promotions Rule Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="Valid from")
                promo_rule_change_trigger.do_analysis()

              elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                cursor[0]['Valid till'] != data_doc['Valid till']) \
                      and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                  cursor[0]['Maximum Stay'] == data_doc['Maximum Stay'] and (
                cursor[0]['Compartment'] == data_doc['Compartment'])):
                print "Raise Rule Change Trigger (Valid till is updated)"  ### Call the trigger
                promo_rule_change_trigger = PromoRuleChangeTrigger("Promotions Rule Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="Valid till")
                promo_rule_change_trigger.do_analysis()

              elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                cursor[0]['Valid till'] == data_doc['Valid till']) \
                      and (cursor[0]['Minimum Stay'] != data_doc['Minimum Stay']) and (
                  cursor[0]['Maximum Stay'] == data_doc['Maximum Stay'] and (
                cursor[0]['Compartment'] == data_doc['Compartment'])):
                print "Raise Rule Change Trigger (Minimum Stay is updated)"  ### Call the trigger
                promo_rule_change_trigger = PromoRuleChangeTrigger("Promotions Rule Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="Minimum Stay")
                promo_rule_change_trigger.do_analysis()
              elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                cursor[0]['Valid till'] == data_doc['Valid till']) \
                      and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                  cursor[0]['Maximum Stay'] != data_doc['Maximum Stay'] and (
                cursor[0]['Compartment'] == data_doc['Compartment'])):
                print "Raise Rule Change Trigger (Maximum Stay is updated)"  ### Call the trigger
                promo_rule_change_trigger = PromoRuleChangeTrigger("Promotions Rule Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="Maximum Stay")
                promo_rule_change_trigger.do_analysis()
              elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                cursor[0]['Valid till'] == data_doc['Valid till']) \
                      and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                  cursor[0]['Maximum Stay'] == data_doc['Maximum Stay'] and (
                cursor[0]['Compartment'] != data_doc['Compartment'])):
                print "Raise Rule Change Trigger (Compartment is updated)"  ### Call the trigger
                promo_rule_change_trigger = PromoRuleChangeTrigger("Promotions Rule Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="Compartment")
                promo_rule_change_trigger.do_analysis()

            else:
              print("Raise New Promotion Released Trigger")
              promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                                                                     old_doc_data=cursor[0],
                                                                     new_doc_data=doc,
                                                                     changed_field="Rules")
              promo_new_promotion_trigger.do_analysis()


          @measure(JUPITER_LOGGER)
          def date_change():
            cursor = list(db.JUP_DB_Promotions.aggregate([
              {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                          'Compartment': data_doc['Compartment'], 'Currency': data_doc['Currency'],
                          'Fare': data_doc['Fare'], 'Valid from': data_doc['Valid from'],
                          'Valid till': data_doc['Valid till'],
                          'Minimum Stay': data_doc['Minimum Stay'], 'Maximum Stay': data_doc['Maximum Stay']}},
              {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
            print cursor
            if len(cursor) > 0:

              if (cursor[0]['dep_date_from'] == data_doc['dep_date_from']) and (
                cursor[0]['dep_date_to'] == data_doc['dep_date_to']):
                pass
              elif (cursor[0]['dep_date_from'] != data_doc['dep_date_from']) and (
                cursor[0]['dep_date_to'] == data_doc['dep_date_to']):
                print "Raise Date Change Trigger (dep date from is updated)"  ### Call the trigger
                promo_date_change_trigger = PromoDateChangeTrigger("Promotions Date Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="dep_date_from")
                promo_date_change_trigger.do_analysis()
              elif (cursor[0]['dep_date_from'] == data_doc['dep_date_from']) and (
                cursor[0]['dep_date_to'] != data_doc['dep_date_to']):
                print "Raise Date Change Trigger (dep date from is updated)"  ### Call the trigger
                promo_date_change_trigger = PromoDateChangeTrigger("Promotions Date Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="dep_date_to")
                promo_date_change_trigger.do_analysis()

            else:
              print("Raise New Promotion Released Trigger")
              promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                                                                     old_doc_data=cursor[0],
                                                                     new_doc_data=doc,
                                                                     changed_field="Dates")
              promo_new_promotion_trigger.do_analysis()


          @measure(JUPITER_LOGGER)
          def fare_change():
            cursor = list(db.JUP_DB_Promotions.aggregate([
              {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                          'Compartment': data_doc['Compartment'], 'Currency': data_doc['Currency'],
                          'Valid from': data_doc['Valid from'], 'dep_date_from': data_doc['dep_date_from'],
                          'dep_date_to': data_doc['dep_date_to'],
                          'Valid till': data_doc['Valid till'], 'Minimum Stay': data_doc['Minimum Stay'],
                          'Maximum Stay': data_doc['Maximum Stay']}},
              {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
            print cursor
            if len(cursor) > 0:

              if (cursor[0]['Fare'] == data_doc['Fare']):
                pass
              elif (cursor[0]['Fare'] != data_doc['Fare']):
                print "Raise Fare Change Trigger (Fare is updated)"  ### Call the trigger
                promo_fare_change_trigger = PromoFareChangeTrigger("Promotions Fare Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="Fare")
                promo_fare_change_trigger.do_analysis()
            else:
              pass
              # print("Raise New Promotion Released Trigger")
              # promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
              #                                                        old_doc_data=cursor[0],
              #                                                        new_doc_data=doc,
              #                                                        changed_field="")
              # promo_new_promotion_trigger.do_analysis()


          @measure(JUPITER_LOGGER)
          def new_promotion():
            cursor = list(db.JUP_DB_Promotions.aggregate([
              {'$match': {'Airline': data_doc['Airline']}}]))
            print cursor
            if len(cursor) > 0:
              for i in range(len(cursor)):
                if (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                  cursor[i]['Valid till'] == data_doc['Valid till']) \
                        and (cursor[i]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                  cursor[i]['Maximum Stay'] == data_doc['Maximum Stay']) and (
                            cursor[i]['Compartment'] == data_doc['Compartment']
                        and (cursor[i]['OD'] == data_doc['OD']) and (cursor[i]['Fare'] == data_doc['Fare']) and (
                      cursor[i]['Currency'] == data_doc['Currency']) and (
                    cursor[i]['dep_date_from'] == data_doc['dep_date_from']) and (
                  cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                  pass

                elif (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                  cursor[i]['Valid till'] == data_doc['Valid till']) \
                        and (cursor[i]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                  cursor[i]['Maximum Stay'] == data_doc['Maximum Stay']) and (
                            cursor[i]['Compartment'] == data_doc['Compartment']
                        and (cursor[i]['OD'] == data_doc['OD']) and (cursor[i]['Fare'] == data_doc['Fare']) and (
                      cursor[i]['Currency'] != data_doc['Currency']) and (
                    cursor[i]['dep_date_from'] == data_doc['dep_date_from']) and (
                  cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                  print("Raise New Promotion Released Trigger")
                  promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                                                                         old_doc_data=cursor[i],
                                                                         new_doc_data=doc,
                                                                         changed_field="Currency")
                  promo_new_promotion_trigger.do_analysis()

                elif (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                  cursor[i]['Valid till'] == data_doc['Valid till']) \
                        and (cursor[i]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                  cursor[i]['Maximum Stay'] == data_doc['Maximum Stay']) and (
                                    cursor[i]['Compartment'] == data_doc['Compartment']
                                and (cursor[i]['OD'] != data_doc['OD']) and (
                                cursor[i]['Fare'] == data_doc['Fare']) and (
                                      cursor[i]['Currency'] == data_doc['Currency']) and (
                            cursor[i]['dep_date_from'] == data_doc['dep_date_from']) and (
                                  cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                  print("Raise New Promotion Released Trigger")
                  promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                                                                         old_doc_data=cursor[i],
                                                                         new_doc_data=doc,
                                                                         changed_field="OD")
                  promo_new_promotion_trigger.do_analysis()

                elif (cursor[i]['Valid from'] != data_doc['Valid from']) and (
                  cursor[i]['Valid till'] != data_doc['Valid till']) \
                        and (cursor[i]['Minimum Stay'] != data_doc['Minimum Stay']) and (
                  cursor[i]['Maximum Stay'] != data_doc['Maximum Stay']) and (
                            cursor[i]['Compartment'] != data_doc['Compartment'] and (
                          cursor[i]['OD'] != data_doc['OD']) and (cursor[i]['Fare'] != data_doc['Fare']) and (
                      cursor[i]['Currency'] != data_doc['Currency']) and (
                            cursor[i]['dep_date_from'] != data_doc['dep_date_from']) and (
                  cursor[i]['dep_date_to'] != data_doc['dep_date_to'])):
                  print("Raise New Promotion Released Trigger")
                promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                                                                       old_doc_data=cursor[i],
                                                                       new_doc_data=doc,
                                                                       changed_field="new")
                promo_new_promotion_trigger.do_analysis()

          db.JUP_DB_Promotions.update(data_doc,doc, upsert = True)
          print("yes")


  @measure(JUPITER_LOGGER)
  def promotions3(url):
    driver3.get(url)
    wait.until(EC.visibility_of_element_located((By.XPATH, "//*[@id='flight-offers']/div/div/div[4]/div/div/ul")))
    OD =driver3.find_element_by_xpath("//*[@id='main']/div[2]")
    ODs = OD.find_element_by_tag_name("h1")
    each_origin = ODs.text.split("to")
    print(each_origin)
    orig = each_origin[0].split(" ")
    origin = orig[1]
    print(orig[1])
    dest = each_origin[1]
    destination = dest
    print(dest.strip())
    cursor = db.JUP_DB_IATA_Codes.find()
    try:
      for i in cursor:
        if all(word in i['City'].lower() for word in re.findall(r'\w+',orig[1].lower())):        origin = i["Code"]
          #print("Origin :",origin)
        if all(word in i['City'].lower() for word in re.findall(r'\w+',dest.lower())):
          destination = i["Code"]
          #print("Destination :",destination)
      print(origin+destination)
    except:
      print "didn't pass JUP_DB_IATA_Codes"
      pass
    price_table = driver3.find_element_by_xpath("//*[@id='flight-offers']/div/div[3]/div[4]/div/div/ul")
    price = price_table.find_elements_by_tag_name("li")
    for i in range(len(price)):
      req_content = price[i].text.splitlines()
      try:
        if len(req_content)==8:
          print(req_content)
          print(len(req_content))
          content = req_content
          dt = parse(content[1])
          startdate = dt.strftime('%Y-%m-%d')
          print("dep_date_from : ", startdate)
          dt = parse(content[3])
          enddate = dt.strftime('%Y-%m-%d')
          print("dep_date_to : ", enddate)
          cls = content[5].split(" ")
          print("Type :", cls[0])
          if "Premium Economy" in cls[1]:
            classes = "J"
            print(classes)

          print("Fare : ", content[6][3:-1])
          print("Currency :", content[6][:3])
          data_doc = {
          "Airline" : "QF",
          "OD" : origin+destination,
          "Valid from" : '',
          "Valid till" : '',
          "Type" : cls[0],
          "Compartment" : classes,
          "Fare" : content[6][3:-1],
          "Currency" : content[6][:3],
          "Minimum Stay" : "Nill",
          "Maximum Stay" : "12 Months",
          "dep_date_from" : startdate,
          "dep_date_to" : enddate,
          "Url" : url
           }
          doc = {
          "Airline" : "QF",
          "OD" : origin+destination,
          "Valid from" : '',
          "Valid till" : '',
          "Type" : cls[0],
          "Compartment" : classes,
          "Fare" : content[6][3:-1],
          "Currency" : content[6][:3],
          "Minimum Stay" : "Nill",
          "Maximum Stay" : "12 Months",
          "dep_date_from" : startdate,
          "dep_date_to" : enddate,
          "Url" : url,
          "Last Updated Date" : datetime.datetime.now().strftime ("%Y-%m-%d"),
          "Last Updated Time" : datetime.datetime.now().strftime ("%H")
           }
          print(data_doc)
          print(doc)


          @measure(JUPITER_LOGGER)
          def rule_change():
            cursor = list(db.JUP_DB_Promotions.aggregate([
              {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                          'Currency': data_doc['Currency'], 'Fare': data_doc['Fare'],
                          'dep_date_from': data_doc['dep_date_from'], 'dep_date_to': data_doc['dep_date_to']}},
              {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
            print cursor
            if len(cursor) > 0:
              if (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                cursor[0]['Valid till'] == data_doc['Valid till']) \
                      and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                cursor[0]['Maximum Stay'] == data_doc['Maximum Stay']) and (
                cursor[0]['Compartment'] == data_doc['Compartment']):
                pass
              elif (cursor[0]['Valid from'] != data_doc['Valid from']) and (
                cursor[0]['Valid till'] == data_doc['Valid till']) \
                      and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                  cursor[0]['Maximum Stay'] == data_doc['Maximum Stay'] and (
                cursor[0]['Compartment'] == data_doc['Compartment'])):
                print "Raise Rule Change Trigger (Valid from is updated)"  ### Call the trigger
                promo_rule_change_trigger = PromoRuleChangeTrigger("Promotions Rule Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="Valid from")
                promo_rule_change_trigger.do_analysis()

              elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                cursor[0]['Valid till'] != data_doc['Valid till']) \
                      and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                  cursor[0]['Maximum Stay'] == data_doc['Maximum Stay'] and (
                cursor[0]['Compartment'] == data_doc['Compartment'])):
                print "Raise Rule Change Trigger (Valid till is updated)"  ### Call the trigger
                promo_rule_change_trigger = PromoRuleChangeTrigger("Promotions Rule Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="Valid till")
                promo_rule_change_trigger.do_analysis()

              elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                cursor[0]['Valid till'] == data_doc['Valid till']) \
                      and (cursor[0]['Minimum Stay'] != data_doc['Minimum Stay']) and (
                  cursor[0]['Maximum Stay'] == data_doc['Maximum Stay'] and (
                cursor[0]['Compartment'] == data_doc['Compartment'])):
                print "Raise Rule Change Trigger (Minimum Stay is updated)"  ### Call the trigger
                promo_rule_change_trigger = PromoRuleChangeTrigger("Promotions Rule Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="Minimum Stay")
                promo_rule_change_trigger.do_analysis()
              elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                cursor[0]['Valid till'] == data_doc['Valid till']) \
                      and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                  cursor[0]['Maximum Stay'] != data_doc['Maximum Stay'] and (
                cursor[0]['Compartment'] == data_doc['Compartment'])):
                print "Raise Rule Change Trigger (Maximum Stay is updated)"  ### Call the trigger
                promo_rule_change_trigger = PromoRuleChangeTrigger("Promotions Rule Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="Maximum Stay")
                promo_rule_change_trigger.do_analysis()
              elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                cursor[0]['Valid till'] == data_doc['Valid till']) \
                      and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                  cursor[0]['Maximum Stay'] == data_doc['Maximum Stay'] and (
                cursor[0]['Compartment'] != data_doc['Compartment'])):
                print "Raise Rule Change Trigger (Compartment is updated)"  ### Call the trigger
                promo_rule_change_trigger = PromoRuleChangeTrigger("Promotions Rule Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="Compartment")
                promo_rule_change_trigger.do_analysis()

            else:
              print("Raise New Promotion Released Trigger")
              promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                                                                     old_doc_data=cursor[0],
                                                                     new_doc_data=doc,
                                                                     changed_field="Rules")
              promo_new_promotion_trigger.do_analysis()


          @measure(JUPITER_LOGGER)
          def date_change():
            cursor = list(db.JUP_DB_Promotions.aggregate([
              {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                          'Compartment': data_doc['Compartment'], 'Currency': data_doc['Currency'],
                          'Fare': data_doc['Fare'], 'Valid from': data_doc['Valid from'],
                          'Valid till': data_doc['Valid till'],
                          'Minimum Stay': data_doc['Minimum Stay'], 'Maximum Stay': data_doc['Maximum Stay']}},
              {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
            print cursor
            if len(cursor) > 0:

              if (cursor[0]['dep_date_from'] == data_doc['dep_date_from']) and (
                cursor[0]['dep_date_to'] == data_doc['dep_date_to']):
                pass
              elif (cursor[0]['dep_date_from'] != data_doc['dep_date_from']) and (
                cursor[0]['dep_date_to'] == data_doc['dep_date_to']):
                print "Raise Date Change Trigger (dep date from is updated)"  ### Call the trigger
                promo_date_change_trigger = PromoDateChangeTrigger("Promotions Date Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="dep_date_from")
                promo_date_change_trigger.do_analysis()
              elif (cursor[0]['dep_date_from'] == data_doc['dep_date_from']) and (
                cursor[0]['dep_date_to'] != data_doc['dep_date_to']):
                print "Raise Date Change Trigger (dep date from is updated)"  ### Call the trigger
                promo_date_change_trigger = PromoDateChangeTrigger("Promotions Date Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="dep_date_to")
                promo_date_change_trigger.do_analysis()

            else:
              print("Raise New Promotion Released Trigger")
              promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                                                                     old_doc_data=cursor[0],
                                                                     new_doc_data=doc,
                                                                     changed_field="Dates")
              promo_new_promotion_trigger.do_analysis()


          @measure(JUPITER_LOGGER)
          def fare_change():
            cursor = list(db.JUP_DB_Promotions.aggregate([
              {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                          'Compartment': data_doc['Compartment'], 'Currency': data_doc['Currency'],
                          'Valid from': data_doc['Valid from'], 'dep_date_from': data_doc['dep_date_from'],
                          'dep_date_to': data_doc['dep_date_to'],
                          'Valid till': data_doc['Valid till'], 'Minimum Stay': data_doc['Minimum Stay'],
                          'Maximum Stay': data_doc['Maximum Stay']}},
              {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
            print cursor
            if len(cursor) > 0:

              if (cursor[0]['Fare'] == data_doc['Fare']):
                pass
              elif (cursor[0]['Fare'] != data_doc['Fare']):
                print "Raise Fare Change Trigger (Fare is updated)"  ### Call the trigger
                promo_fare_change_trigger = PromoFareChangeTrigger("Promotions Fare Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="Fare")
                promo_fare_change_trigger.do_analysis()
            else:
              pass
              # print("Raise New Promotion Released Trigger")
              # promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
              #                                                        old_doc_data=cursor[0],
              #                                                        new_doc_data=doc,
              #                                                        changed_field="")
              # promo_new_promotion_trigger.do_analysis()


          @measure(JUPITER_LOGGER)
          def new_promotion():
            cursor = list(db.JUP_DB_Promotions.aggregate([
              {'$match': {'Airline': data_doc['Airline']}}]))
            print cursor
            if len(cursor) > 0:
              for i in range(len(cursor)):
                if (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                  cursor[i]['Valid till'] == data_doc['Valid till']) \
                        and (cursor[i]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                  cursor[i]['Maximum Stay'] == data_doc['Maximum Stay']) and (
                            cursor[i]['Compartment'] == data_doc['Compartment']
                        and (cursor[i]['OD'] == data_doc['OD']) and (cursor[i]['Fare'] == data_doc['Fare']) and (
                      cursor[i]['Currency'] == data_doc['Currency']) and (
                    cursor[i]['dep_date_from'] == data_doc['dep_date_from']) and (
                  cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                  pass

                elif (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                  cursor[i]['Valid till'] == data_doc['Valid till']) \
                        and (cursor[i]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                  cursor[i]['Maximum Stay'] == data_doc['Maximum Stay']) and (
                            cursor[i]['Compartment'] == data_doc['Compartment']
                        and (cursor[i]['OD'] == data_doc['OD']) and (cursor[i]['Fare'] == data_doc['Fare']) and (
                      cursor[i]['Currency'] != data_doc['Currency']) and (
                    cursor[i]['dep_date_from'] == data_doc['dep_date_from']) and (
                  cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                  print("Raise New Promotion Released Trigger")
                  promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                                                                         old_doc_data=cursor[i],
                                                                         new_doc_data=doc,
                                                                         changed_field="Currency")
                  promo_new_promotion_trigger.do_analysis()

                elif (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                  cursor[i]['Valid till'] == data_doc['Valid till']) \
                        and (cursor[i]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                  cursor[i]['Maximum Stay'] == data_doc['Maximum Stay']) and (
                                    cursor[i]['Compartment'] == data_doc['Compartment']
                                and (cursor[i]['OD'] != data_doc['OD']) and (
                                cursor[i]['Fare'] == data_doc['Fare']) and (
                                      cursor[i]['Currency'] == data_doc['Currency']) and (
                            cursor[i]['dep_date_from'] == data_doc['dep_date_from']) and (
                                  cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                  print("Raise New Promotion Released Trigger")
                  promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                                                                         old_doc_data=cursor[i],
                                                                         new_doc_data=doc,
                                                                         changed_field="OD")
                  promo_new_promotion_trigger.do_analysis()

                elif (cursor[i]['Valid from'] != data_doc['Valid from']) and (
                  cursor[i]['Valid till'] != data_doc['Valid till']) \
                        and (cursor[i]['Minimum Stay'] != data_doc['Minimum Stay']) and (
                  cursor[i]['Maximum Stay'] != data_doc['Maximum Stay']) and (
                            cursor[i]['Compartment'] != data_doc['Compartment'] and (
                          cursor[i]['OD'] != data_doc['OD']) and (cursor[i]['Fare'] != data_doc['Fare']) and (
                      cursor[i]['Currency'] != data_doc['Currency']) and (
                            cursor[i]['dep_date_from'] != data_doc['dep_date_from']) and (
                  cursor[i]['dep_date_to'] != data_doc['dep_date_to'])):
                  print("Raise New Promotion Released Trigger")
                promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                                                                       old_doc_data=cursor[i],
                                                                       new_doc_data=doc,
                                                                       changed_field="new")
                promo_new_promotion_trigger.do_analysis()


          db.JUP_DB_Promotions.update(data_doc,doc, upsert= True)
          print "yes"
      except:
        if len(req_content)>8:
          print(req_content)
          print(len(req_content))
          content2 = req_content

          dt = parse(content2[1])
          startdate = dt.strftime('%Y-%m-%d')
          print("dep_date_from : ", startdate)
          dt = parse(content[4])
          enddate = dt.strftime('%Y-%m-%d')
          print("dep_date_to : ", enddate)
          dt = parse(content[6])
          valid_till = dt.strftime('%Y-%m-%d')
          print("Valid_till : ", valid_till)
          cls = content[7].split(" ")
          print("Type :", cls[0])
          if "Premium Economy" in cls[1]:
            classes = "J"
            print(classes)
          print("Fare : ", content[8][3:-1])
          print("Currency :", content[8][:3])




          data_doc = {
            "Airline" : "QF",
            "OD" : origin+destination,
            "Valid from" : '',
            "Valid till" : valid_till,
            "Type" : cls[0],
            "Compartment" : classes,
            "Fare" : content[8][3:-1],
            "Currency" : content[8][:3],
            "Minimum Stay" : "Nill",
            "Maximum Stay" : "12 Months",
            "dep_date_from" : startdate,
            "dep_date_to" : enddate,
            "Url" : url
             }
          doc = {
            "Airline" : "QF",
            "OD" : origin+destination,
            "Valid from" : '',
            "Valid till" : valid_till,
            "Type" : cls[0],
            "Compartment" : classes,
            "Fare" : content[8][3:-1],
            "Currency" : content[8][:3],
            "Minimum Stay" : "Nill",
            "Maximum Stay" : "12 Months",
            "dep_date_from" : startdate,
            "dep_date_to" : enddate,
            "Url" : url,
            "Last Updated Date" : datetime.datetime.now().strftime ("%Y-%m-%d"),
            "Last Updated Time" : datetime.datetime.now().strftime ("%H")
             }
          print data_doc
          print doc


          @measure(JUPITER_LOGGER)
          def rule_change():
            cursor = list(db.JUP_DB_Promotions.aggregate([
              {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                          'Currency': data_doc['Currency'], 'Fare': data_doc['Fare'],
                          'dep_date_from': data_doc['dep_date_from'], 'dep_date_to': data_doc['dep_date_to']}},
              {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
            print cursor
            if len(cursor) > 0:
              if (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                cursor[0]['Valid till'] == data_doc['Valid till']) \
                      and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                cursor[0]['Maximum Stay'] == data_doc['Maximum Stay']) and (
                cursor[0]['Compartment'] == data_doc['Compartment']):
                pass
              elif (cursor[0]['Valid from'] != data_doc['Valid from']) and (
                cursor[0]['Valid till'] == data_doc['Valid till']) \
                      and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                  cursor[0]['Maximum Stay'] == data_doc['Maximum Stay'] and (
                cursor[0]['Compartment'] == data_doc['Compartment'])):
                print "Raise Rule Change Trigger (Valid from is updated)"  ### Call the trigger
                promo_rule_change_trigger = PromoRuleChangeTrigger("Promotions Rule Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="Valid from")
                promo_rule_change_trigger.do_analysis()

              elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                cursor[0]['Valid till'] != data_doc['Valid till']) \
                      and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                  cursor[0]['Maximum Stay'] == data_doc['Maximum Stay'] and (
                cursor[0]['Compartment'] == data_doc['Compartment'])):
                print "Raise Rule Change Trigger (Valid till is updated)"  ### Call the trigger
                promo_rule_change_trigger = PromoRuleChangeTrigger("Promotions Rule Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="Valid till")
                promo_rule_change_trigger.do_analysis()

              elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                cursor[0]['Valid till'] == data_doc['Valid till']) \
                      and (cursor[0]['Minimum Stay'] != data_doc['Minimum Stay']) and (
                  cursor[0]['Maximum Stay'] == data_doc['Maximum Stay'] and (
                cursor[0]['Compartment'] == data_doc['Compartment'])):
                print "Raise Rule Change Trigger (Minimum Stay is updated)"  ### Call the trigger
                promo_rule_change_trigger = PromoRuleChangeTrigger("Promotions Rule Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="Minimum Stay")
                promo_rule_change_trigger.do_analysis()
              elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                cursor[0]['Valid till'] == data_doc['Valid till']) \
                      and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                  cursor[0]['Maximum Stay'] != data_doc['Maximum Stay'] and (
                cursor[0]['Compartment'] == data_doc['Compartment'])):
                print "Raise Rule Change Trigger (Maximum Stay is updated)"  ### Call the trigger
                promo_rule_change_trigger = PromoRuleChangeTrigger("Promotions Rule Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="Maximum Stay")
                promo_rule_change_trigger.do_analysis()
              elif (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                cursor[0]['Valid till'] == data_doc['Valid till']) \
                      and (cursor[0]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                  cursor[0]['Maximum Stay'] == data_doc['Maximum Stay'] and (
                cursor[0]['Compartment'] != data_doc['Compartment'])):
                print "Raise Rule Change Trigger (Compartment is updated)"  ### Call the trigger
                promo_rule_change_trigger = PromoRuleChangeTrigger("Promotions Rule Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="Compartment")
                promo_rule_change_trigger.do_analysis()

            else:
              print("Raise New Promotion Released Trigger")
              promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                                                                     old_doc_data=cursor[0],
                                                                     new_doc_data=doc,
                                                                     changed_field="Rules")
              promo_new_promotion_trigger.do_analysis()


          @measure(JUPITER_LOGGER)
          def date_change():
            cursor = list(db.JUP_DB_Promotions.aggregate([
              {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                          'Compartment': data_doc['Compartment'], 'Currency': data_doc['Currency'],
                          'Fare': data_doc['Fare'], 'Valid from': data_doc['Valid from'],
                          'Valid till': data_doc['Valid till'],
                          'Minimum Stay': data_doc['Minimum Stay'], 'Maximum Stay': data_doc['Maximum Stay']}},
              {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
            print cursor
            if len(cursor) > 0:

              if (cursor[0]['dep_date_from'] == data_doc['dep_date_from']) and (
                cursor[0]['dep_date_to'] == data_doc['dep_date_to']):
                pass
              elif (cursor[0]['dep_date_from'] != data_doc['dep_date_from']) and (
                cursor[0]['dep_date_to'] == data_doc['dep_date_to']):
                print "Raise Date Change Trigger (dep date from is updated)"  ### Call the trigger
                promo_date_change_trigger = PromoDateChangeTrigger("Promotions Date Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="dep_date_from")
                promo_date_change_trigger.do_analysis()
              elif (cursor[0]['dep_date_from'] == data_doc['dep_date_from']) and (
                cursor[0]['dep_date_to'] != data_doc['dep_date_to']):
                print "Raise Date Change Trigger (dep date from is updated)"  ### Call the trigger
                promo_date_change_trigger = PromoDateChangeTrigger("Promotions Date Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="dep_date_to")
                promo_date_change_trigger.do_analysis()

            else:
              print("Raise New Promotion Released Trigger")
              promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                                                                     old_doc_data=cursor[0],
                                                                     new_doc_data=doc,
                                                                     changed_field="Dates")
              promo_new_promotion_trigger.do_analysis()


          @measure(JUPITER_LOGGER)
          def fare_change():
            cursor = list(db.JUP_DB_Promotions.aggregate([
              {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                          'Compartment': data_doc['Compartment'], 'Currency': data_doc['Currency'],
                          'Valid from': data_doc['Valid from'], 'dep_date_from': data_doc['dep_date_from'],
                          'dep_date_to': data_doc['dep_date_to'],
                          'Valid till': data_doc['Valid till'], 'Minimum Stay': data_doc['Minimum Stay'],
                          'Maximum Stay': data_doc['Maximum Stay']}},
              {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
            print cursor
            if len(cursor) > 0:

              if (cursor[0]['Fare'] == data_doc['Fare']):
                pass
              elif (cursor[0]['Fare'] != data_doc['Fare']):
                print "Raise Fare Change Trigger (Fare is updated)"  ### Call the trigger
                promo_fare_change_trigger = PromoFareChangeTrigger("Promotions Fare Change Trigger",
                                                                   old_doc_data=cursor[0],
                                                                   new_doc_data=doc,
                                                                   changed_field="Fare")
                promo_fare_change_trigger.do_analysis()
            else:
              pass
              # print("Raise New Promotion Released Trigger")
              # promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
              #                                                        old_doc_data=cursor[0],
              #                                                        new_doc_data=doc,
              #                                                        changed_field="")
              # promo_new_promotion_trigger.do_analysis()


          @measure(JUPITER_LOGGER)
          def new_promotion():
            cursor = list(db.JUP_DB_Promotions.aggregate([
              {'$match': {'Airline': data_doc['Airline']}}]))
            print cursor
            if len(cursor) > 0:
              for i in range(len(cursor)):
                if (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                  cursor[i]['Valid till'] == data_doc['Valid till']) \
                        and (cursor[i]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                  cursor[i]['Maximum Stay'] == data_doc['Maximum Stay']) and (
                            cursor[i]['Compartment'] == data_doc['Compartment']
                        and (cursor[i]['OD'] == data_doc['OD']) and (cursor[i]['Fare'] == data_doc['Fare']) and (
                      cursor[i]['Currency'] == data_doc['Currency']) and (
                    cursor[i]['dep_date_from'] == data_doc['dep_date_from']) and (
                  cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                  pass

                elif (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                  cursor[i]['Valid till'] == data_doc['Valid till']) \
                        and (cursor[i]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                  cursor[i]['Maximum Stay'] == data_doc['Maximum Stay']) and (
                            cursor[i]['Compartment'] == data_doc['Compartment']
                        and (cursor[i]['OD'] == data_doc['OD']) and (cursor[i]['Fare'] == data_doc['Fare']) and (
                      cursor[i]['Currency'] != data_doc['Currency']) and (
                    cursor[i]['dep_date_from'] == data_doc['dep_date_from']) and (
                  cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                  print("Raise New Promotion Released Trigger")
                  promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                                                                         old_doc_data=cursor[i],
                                                                         new_doc_data=doc,
                                                                         changed_field="Currency")
                  promo_new_promotion_trigger.do_analysis()

                elif (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                  cursor[i]['Valid till'] == data_doc['Valid till']) \
                        and (cursor[i]['Minimum Stay'] == data_doc['Minimum Stay']) and (
                  cursor[i]['Maximum Stay'] == data_doc['Maximum Stay']) and (
                                    cursor[i]['Compartment'] == data_doc['Compartment']
                                and (cursor[i]['OD'] != data_doc['OD']) and (
                                cursor[i]['Fare'] == data_doc['Fare']) and (
                                      cursor[i]['Currency'] == data_doc['Currency']) and (
                            cursor[i]['dep_date_from'] == data_doc['dep_date_from']) and (
                                  cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                  print("Raise New Promotion Released Trigger")
                  promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                                                                         old_doc_data=cursor[i],
                                                                         new_doc_data=doc,
                                                                         changed_field="OD")
                  promo_new_promotion_trigger.do_analysis()

                elif (cursor[i]['Valid from'] != data_doc['Valid from']) and (
                  cursor[i]['Valid till'] != data_doc['Valid till']) \
                        and (cursor[i]['Minimum Stay'] != data_doc['Minimum Stay']) and (
                  cursor[i]['Maximum Stay'] != data_doc['Maximum Stay']) and (
                            cursor[i]['Compartment'] != data_doc['Compartment'] and (
                          cursor[i]['OD'] != data_doc['OD']) and (cursor[i]['Fare'] != data_doc['Fare']) and (
                      cursor[i]['Currency'] != data_doc['Currency']) and (
                            cursor[i]['dep_date_from'] != data_doc['dep_date_from']) and (
                  cursor[i]['dep_date_to'] != data_doc['dep_date_to'])):
                  print("Raise New Promotion Released Trigger")
                promo_new_promotion_trigger = PromoNewPromotionTrigger("Promotions New Promotions Trigger",
                                                                       old_doc_data=cursor[i],
                                                                       new_doc_data=doc,
                                                                       changed_field="new")
                promo_new_promotion_trigger.do_analysis()

          db.JUP_DB_Promotions.update(data_doc,doc, upsert = True)
          print "yes"

  #Scraping the promotions of economy class
  city = ["amd", "axt", "aoj", "akj", "bkk", "pek", "blr", "ctu", "maa", "ckg", "cjb", "dad", "del", "dps", "fuk", "foc", "can", "kwl", "hkd", "hgh", "han", "hij", "sgn", "hkg", "hyd", "izo", "cgk", "kog", "kkj", "kcz", "cok", "ccu", "kmq", "kmj", "kmg", "kuh", "mnl", "myj", "mes", "mmb", "msj", "kmi", "bom", "ngs", "ngo", "nkg", "ngb", "obo", "oit", "okj", "oka", "itm", "kix", "tao", "cts", "sel", "pvg", "she", "shm", "sin", "sub", "tpe", "tak", "trv", "tks", "hnd", "nrt", "ubj", "wnz", "wuh", "sia", "xmn", "gaj", "cgo"]
  for each_city in city:
    driver.get("https://www.qantas.com/in/en/flight-deals.html/"+each_city+"/all/economy/all/lowest")
    time.sleep(10)
    try:
      promotion_table = driver.find_element_by_xpath("//*[@id='flight-offers']/div/div/div[4]/div/div/ul")
      promo = promotion_table.find_elements_by_tag_name("li")
      for each_promo in promo:
        link2 = each_promo.find_element_by_class_name("price-list__column-3")
        urls = link2.find_element_by_tag_name("a")
        url = urls.get_attribute("href")
        print(url)
        promotions(url)
    except:
      pass

  #Scraping the promotions of Business Class
  city = ["amd", "axt", "aoj", "akj", "bkk", "pek", "blr", "ctu", "maa", "ckg", "cjb", "dad", "del", "dps", "fuk", "foc", "can", "kwl", "hkd", "hgh", "han", "hij", "sgn", "hkg", "hyd", "izo", "cgk", "kog", "kkj", "kcz", "cok", "ccu", "kmq", "kmj", "kmg", "kuh", "mnl", "myj", "mes", "mmb", "msj", "kmi", "bom", "ngs", "ngo", "nkg", "ngb", "obo", "oit", "okj", "oka", "itm", "kix", "tao", "cts", "sel", "pvg", "she", "shm", "sin", "sub", "tpe", "tak", "trv", "tks", "hnd", "nrt", "ubj", "wnz", "wuh", "sia", "xmn", "gaj", "cgo"]
  for each_city in city:
    driver.get("https://www.qantas.com/in/en/flight-deals.html/"+each_city+"/all/business/all/lowest")
    time.sleep(10)
    promotion_table = driver.find_element_by_xpath("//*[@id='flight-offers']/div/div/div[4]/div/div/ul")
    promo = promotion_table.find_elements_by_tag_name("li")
    for each_promo in promo:
      link2 = each_promo.find_element_by_class_name("price-list__column-3")
      urls = link2.find_element_by_tag_name("a")
      url = urls.get_attribute("href")
      print(url)
      check = driver.find_element_by_xpath("//*[@id='flight-offers']/div/div/div[1]/div")
      print(check.text)
      if "Business" in check.text:
        promotions2(url)
      else:
        print("No offers")
  driver.close()

  #Scraping the promotions of Premium Economy class
  city = ["amd", "axt", "aoj", "akj", "bkk", "pek", "blr", "ctu", "maa", "ckg", "cjb", "dad", "del", "dps", "fuk", "foc", "can", "kwl", "hkd", "hgh", "han", "hij", "sgn", "hkg", "hyd", "izo", "cgk", "kog", "kkj", "kcz", "cok", "ccu", "kmq", "kmj", "kmg", "kuh", "mnl", "myj", "mes", "mmb", "msj", "kmi", "bom", "ngs", "ngo", "nkg", "ngb", "obo", "oit", "okj", "oka", "itm", "kix", "tao", "cts", "sel", "pvg", "she", "shm", "sin", "sub", "tpe", "tak", "trv", "tks", "hnd", "nrt", "ubj", "wnz", "wuh", "sia", "xmn", "gaj", "cgo"]
  for each_city in city:
    driver.get("https://www.qantas.com/in/en/flight-deals.html/"+each_city+"/all/premium-economy/all/lowest")
    time.sleep(10)
    promotion_table = driver.find_element_by_xpath("//*[@id='flight-offers']/div/div/div[4]/div/div/ul")
    promo = promotion_table.find_elements_by_tag_name("li")
    for each_promo in promo:
      link2 = each_promo.find_element_by_class_name("price-list__column-3")
      urls = link2.find_element_by_tag_name("a")
      url = urls.get_attribute("href")
      print(url)
      check = driver.find_element_by_xpath("//*[@id='flight-offers']/div/div/div[1]/div")
      print(check.text)
      if "Premium" in check.text:
        promotions3(url)
      else:
        print("No offers")

  driver.close()
if __name__ == "__main__":
    run()