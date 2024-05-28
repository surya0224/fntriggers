#Author : Vinusha Anem
#Written : May 2017
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
  driver.get("https://www.flyroyalbrunei.com/en/united-arab-emirates/deals/")
  driver1 = webdriver.Chrome(chrome_path)
  wait = WebDriverWait(driver, 20)
  wait = WebDriverWait(driver1, 20)

  #Function for scraping promotions


  @measure(JUPITER_LOGGER)
  def promotions(url):
    driver1.get(url)
    time.sleep(5)
    wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR,("body > div.page.page_ > div > div.page__content.page-block.g-relative > div > div.special-deals__menu-content.g-cleared > section > div > div > div.deals-expanded__content.g-cleared > div.deals-expanded__content_right > div.booking-block_container"))))
    offer = driver1.find_element_by_css_selector("body > div.page.page_ > div > div.page__content.page-block.g-relative > div > div.special-deals__menu-content.g-cleared > section > div > div > div.deals-expanded__content.g-cleared > div.deals-expanded__content_right > div.booking-block_container")
    OD = offer.find_elements_by_class_name("description_left")
    for ODs in OD:
      req_ODs = ODs.text.splitlines()
      orig = req_ODs[0].split(" ")
      orig1 = orig[1].strip()
      origin = orig1
      dest1 = req_ODs[1].strip()
      if "(via brunei)" in dest1.lower():
        dest=dest1.split("(")
        destination1 = dest[0]
        destination = destination1
      else:
        destination1 = dest1
        destination = destination1
      cursor = db.JUP_DB_IATA_Codes.find()
      try:
        for i in cursor:
          if all(word in i['City'].lower() for word in re.findall(r'\w+',orig1.lower())):
            origin = i["Code"]
            #print("Origin :",origin)
          if all(word in i['City'].lower() for word in re.findall(r'\w+',destination1.lower())):
            destination = i["Code"]
            #print("Destination :",destination)
        print(origin+destination)
      except:
        pass

      req_OD = ODs.text.lower().splitlines()
      start_date = None
      end_date = None
      try:
        enddate = req_OD[3].split("until",1)[1]
        dt = parse(enddate)
        end_date = dt.strftime('%Y-%m-%d')
        print("dep_date_to:", end_date )
      except:
        pass
      try:
        startdate = req_OD[3].split("-")
        dt = parse(startdate[0])
        start_date = dt.strftime('%Y-%m-%d')
        print("dep_date_from:", start_date )

        enddate = req_OD[3].split("-")
        dt = parse(enddate[1])
        end_date = dt.strftime('%Y-%m-%d')
        print("dep_date_to:", end_date )
      except:
        pass
    content = offer.find_elements_by_class_name("description_right")
    for each_content in content:
      req_content = each_content.text.splitlines()
      if "From" in req_content[0]:
        print("Currency:",req_content[1][:3])
        print("Fare:",req_content[1][3:-1])
      else:
        print("Currency :", req_content[0][:3])
        print("Fare:", req_content[0][3:-1])
    classes = "Y"
    valid_till = None
    terms = None
    try:
      if "Economy" in req_content[2]:
        classes = "Y"
        print(classes)
      if "Business" in req_content[2]:
        classes = "J"
        print(classes)
      content2 = driver1.find_element_by_css_selector("body > div.page.page_ > div > div.page__content.page-block.g-relative > div > div.special-deals__menu-content.g-cleared > section > div > div > div.deals-expanded__content.g-cleared > div.deals-expanded__content_left > div")
      req_content2 = content2.find_elements_by_tag_name("p")
      valid_till = "2017-05-31"
      for each_content in req_content2:
        if "Valid for sale until" in each_content.text:
          validity = each_content.text.split("until",1)[1]
          print(validity)
          dt = parse(validity.split("2017")[0])
          valid_till = dt.strftime('%Y-%m-%d')
          print("Valid till:", valid_till )
        terms = each_content.text
        print(terms)
    except:
      pass

  #Storing the parameters in a dictionary
    data_doc = {
      "Airline" : "BI",
      "OD" : origin+destination,
      "Valid from" : '',
      "Valid till" : valid_till,
      "Compartment" : classes,
      "Type" : "Return",
      "Fare" : req_content[1][3:-1],
      "Currency" : req_content[1][:3],
      "Minimum Stay" : '',
      "Maximum Stay" : '',
      "dep_date_from" : start_date,
      "dep_date_to" : end_date,
      "Url" : urls,
      "Terms and Conditions" : terms
      }
    doc = {
      "Airline" : "BI",
      "OD" : origin+destination,
      "Valid from" : '',
      "Valid till" : valid_till,
      "Compartment" : classes,
      "Type" : "Return",
      "Fare" : req_content[1][3:-1],
      "Currency" : req_content[1][:3],
      "Minimum Stay" : '',
      "Maximum Stay" : '',
      "dep_date_from" : start_date,
      "dep_date_to" : end_date,
      "Url" : urls,
      "Terms and Conditions" : terms,
      "Last Updated Date" : datetime.datetime.now().strftime ("%Y-%m-%d"),
      "Last Updated Time" : datetime.datetime.now().strftime ("%H")
       }
    print(data_doc)
    print doc


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

  #Upserting the values into Mongodb
    db.JUP_DB_Promotions.update(data_doc, doc, upsert = True)


  button1 = driver.find_element_by_css_selector("body > div.page.page_special-deals > div > header > div.header__links-wrapper.header-nav > div.nav__info-links-wrapper > ul > li.g-section.header-nav__item.header-nav__item-toplinks.header-nav__item_country.g-margin.g-relative > a").click()
  button2 = driver.find_element_by_xpath("//*[@id='site_preference']/div[2]").click()
  country = driver.find_element_by_xpath("//*[@id='site_preference']/div[2]/div")
  country_list = country.find_elements_by_tag_name("div")

  for i in range(len(country_list)):
    print("Country : ", country_list[i].text)
    each_origin = country_list[i].click()
    submit = driver.find_element_by_xpath("//*[@id='site_preference']/div[4]/button").click()
    time.sleep(10)
    try:
      more = driver.find_element_by_css_selector("body > div.page.page_special-deals > div > div.page__content.page-block.g-relative > div > div.special-deals__menu-content.g-cleared > div").click()
    except:
      pass
    try:
      more1 = driver.find_element_by_css_selector("body > div.page.page_special-deals > div > div.page__content.page-block.g-relative > div > div.special-deals__menu-content.g-cleared > div").click()
    except:
      pass
    try:
      more2 = driver.find_element_by_css_selector("body > div.page.page_special-deals > div > div.page__content.page-block.g-relative > div > div.special-deals__menu-content.g-cleared > div").click()
    except:
      pass
    promotion_table = driver.find_element_by_css_selector("body > div.page.page_special-deals > div > div.page__content.page-block.g-relative > div > div.special-deals__menu-content.g-cleared > section > div.special-deals__menu-content.g-cleared > section > div")
    if "Sorry" in promotion_table.text:
      print("No offers available")
      button1 = driver.find_element_by_css_selector("body > div.page.page_special-deals > div > header > div.header__links-wrapper.header-nav > div.nav__info-links-wrapper > ul > li.g-section.header-nav__item.header-nav__item-toplinks.header-nav__item_country.g-margin.g-relative > a").click()
      button2 = driver.find_element_by_xpath("//*[@id='site_preference']/div[2]").click()
      country = driver.find_element_by_xpath("//*[@id='site_preference']/div[2]/div")
      country_list = country.find_elements_by_tag_name("div")
      time.sleep(5)
    else:
      req_cls = promotion_table.find_elements_by_class_name("description_right")
      for each_req_cls in req_cls:
        url = each_req_cls.find_elements_by_tag_name("a")
        for each_url in url:
          urls = each_url.get_attribute("href")
          print(urls)
          promotions(urls)
          button1 = driver.find_element_by_css_selector("body > div.page.page_special-deals > div > header > div.header__links-wrapper.header-nav > div.nav__info-links-wrapper > ul > li.g-section.header-nav__item.header-nav__item-toplinks.header-nav__item_country.g-margin.g-relative > a").click()
          button2 = driver.find_element_by_xpath("//*[@id='site_preference']/div[2]").click()
          country = driver.find_element_by_xpath("//*[@id='site_preference']/div[2]/div")
          country_list = country.find_elements_by_tag_name("div")
          time.sleep(5)
  driver.close()


if __name__ == "__main__":
    run()