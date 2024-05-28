#check output once agn!
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
    from dateutil.parser import parse
    import re
    import datetime


    #dbecting to Local Mongodb
    import pymongo
    client = pymongo.MongoClient("mongodb://analytics:KNjSZmiaNUGLmS0Bv2@13.92.251.7:42525/")
    db = client['fzDB']

    #Defining the chrome driver path
    chrome_path = r"/home/prathyusha/Downloads/chromedriver"
    #chrome_path = r"/var/www/html/jupiter/python/jupiter_AI/batch/Promotions_Scraping/chromedriver"
    driver = webdriver.Chrome(chrome_path)
    #Getting the URL
    driver.get("https://www.mea.com.lb/english/Plan-and-Book/Special-Offers")
    driver1 = webdriver.Chrome(chrome_path)
    wait = WebDriverWait(driver, 20)
    wait = WebDriverWait(driver1, 20)


    #Function to scrape each promotion


    @measure(JUPITER_LOGGER)
    def promotions(url):
      driver1.get(url)
      wait.until(EC.visibility_of_element_located((By.XPATH, "//*[@id='content_divLeftContentContainer']")))
      offer = driver1.find_element_by_xpath("//*[@id='content_divLeftContentContainer']")
      OD = offer.find_elements_by_tag_name("h2")
      for ODs in OD:
        each_OD = ODs.text.split('-')
        print each_OD
        origin = each_OD[0].strip()
        print "origin:", origin
        destination = each_OD[-1].strip()
        print "destination: ", destination
        try:
          cursor = db.JUP_DB_IATA_Codes.find()
          for i in cursor:
            if all(word in i['City'].lower() for word in re.findall(r'\w+',each_OD[0].lower())):
              origin = i["Code"]
              print("Origin :",origin)
            if all(word in i['City'].lower() for word in re.findall(r'\w+',each_OD[-1].lower())):
              destination = i["Code"]
              print("Destination :",destination)
              print(origin+destination)
        except:
          pass
      date = driver1.find_element_by_xpath("//*[@id='content_content_lblDate']")
      each_date = date.text.split(' ')
      print date.text
      dt = parse(each_date[0])
      valid_from = dt.strftime('%Y-%m-%d')
      print("Valid from :", valid_from )
      dt = parse(each_date[2])
      valid_till = dt.strftime('%Y-%m-%d')
      print("Valid till :", valid_till)
      content = driver1.find_element_by_xpath("//*[@id='content_divLeftContentContainer']")
      each_content = content.text.splitlines()
      print each_content
      for text in each_content:
        if "class:" in text.lower():
          cls = text.split("Class:",1)[1]
          cls1 = cls.split(",")
          if cls1[0].strip() == "Economy":
            classes = "Y"
            print(classes)
          if cls1[0].strip() == "Business":
            classes = "J"
            print(classes)
        else:
          classes= None
        if "minimum stay:" in text.lower():
          min_stay = text.split("Minimum Stay:",1)[1]
          print(min_stay)
        else:
          min_stay = None
        if "maximum stay:" in text.lower():
          max_stay = text.split("Maximum Stay:",1)[1]
          print(max_stay)
        else:
          max_stay = None
        if "outbound travel" in text.lower():
          start_date = None
          try:
            startdate = re.findall(r'after(.*?)and',text)
            for date in startdate:
              print(date)
              dt = parse(date)
              start_date = dt.strftime('%Y-%m-%d')
              print("dep_date_from:", start_date )
          except:
            pass
          try:
            enddate = text.split("before",1)[1]
            dt = parse(enddate)
            end_date = dt.strftime('%Y-%m-%d')
            print("dep_date_to:", end_date )
          except:
            pass
        else:
          start_date = None
          end_date = None
    #Storing the parameters in a dictionary

        data_doc = {
          "Airline" : "ME",
          "OD" : origin+destination,
          "Valid from" : valid_from,
          "Valid till" : valid_till,
          "Compartment" : classes,
          "Fare" : fare[1],
          "Currency" : fare[2],
          "Minimum Stay" : min_stay,
          "Maximum Stay" : max_stay,
          "dep_date_from" : start_date,
          "dep_date_to" : end_date,
          "Url" : urls,
           }
        doc = {
          "Airline" : "ME",
          "OD" : origin+destination,
          "Valid from" : valid_from,
          "Valid till" : valid_till,
          "Compartment" : classes,
          "Fare" : fare[1],
          "Currency" : fare[2],
          "Minimum Stay" : min_stay,
          "Maximum Stay" : max_stay,
          "Url" : urls,
          "dep_date_from" : start_date,
          "dep_date_to" : end_date,
          "Url" : urls,
          "Last Updated Date" : datetime.datetime.now().strftime ("%Y-%m-%d"),
          "Last Updated Time" : datetime.datetime.now().strftime ("%H")
           }
        print data_doc
        print doc
#Upserting the parameters into Mongodb
    #db.JUP_DB_Promotions.update(data_doc, doc, upsert = True)
        print "yes"
      # except:
      #     print "error in data_doc n doc"
      #     pass


    promotion_table = driver.find_elements_by_xpath("//*[@id='ctl00_ctl00_content_content_BICMSZone1_ctl00_ctl00_RadAjaxPanel1']")
    for lists in promotion_table:
      urls = lists.find_elements_by_class_name("listingDescp")
      for eachURL in urls:
        fares = eachURL.text.splitlines()
        for text in fares:
          if "starting" in text:
            fares = text.split("starting",1)[1]
            fare = fares.split(" ")
            print("Fare: ",fare[1])
            print("Currency: ",fare[2])
        url = eachURL.find_elements_by_tag_name("a")
        urls = url[0].get_attribute("href")
        print(urls)
        promotions(urls)

    driver.close()


if __name__ == "__main__":
    run()
