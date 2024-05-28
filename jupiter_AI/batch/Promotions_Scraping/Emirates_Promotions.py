#Author : Vinusha Anem
#Written : May 2017
from jupiter_AI import JUPITER_LOGGER
from jupiter_AI.logutils import measure


@measure(JUPITER_LOGGER)
def run():

  from selenium import webdriver
  from selenium.webdriver.support.ui import WebDriverWait
  from requests.exceptions import ConnectionError
  from selenium.webdriver.support import expected_conditions as EC
  from selenium.webdriver.common.by import By
  import time
  from dateutil.parser import parse
  import datetime
  from datetime import timedelta


  #dbecting to Local Mongodb
  # import pymongo
  # client = pymongo.MongoClient("mongodb://analytics:KNjSZmiaNUGLmS0Bv2@13.92.251.7:42525/")
  # db = client['fzDB']

  #Defining the Chrome Path
  #chrome_path = r"/home/prathyusha/Downloads/chromedriver"
  chrome_path = r"/var/www/html/jupiter/python/jupiter_AI/batch/Promotions_Scraping/chromedriver"
  driver = webdriver.Chrome(chrome_path)
  #url = driver.get("https://www.emirates.com/ae/english/destinations/featured-fares/city/DXB/countries/IN")
  #driver1 = webdriver.Chrome(chrome_path)
  wait = WebDriverWait(driver, 20)
  #wait = WebDriverWait(driver1, 60)

  #Scraping promotions for countries which do not have multiple cities as origin
  country = ["af"]
  for each_country in country:
    driver.get("https://www.emirates.com/"+each_country+"/english/book/featured-fares/")
    url = "https://www.emirates.com/"+each_country+"/english/book/featured-fares/"
    print(url)
    time.sleep(5)

    check_offers = driver.find_element_by_xpath("//*[@id='content']/div/div/div[3]/div[2]/div/div[6]")
    if "Sorry" in check_offers.text:
      print("No offers")
    else:
      #try:
      wait.until(EC.visibility_of_element_located((By.XPATH,("//*[@id='content']/div/div/div[3]/div[2]/div/div[6]/div[2]/div/div[2]/div/card/div"))))
      #button = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[1]/div/div[1]/div[1]/div/select-fly-out/div").click()

      origin_path = driver.find_element_by_xpath("//*[@id='content']/div/div/div[3]/div[2]/div/div[6]/div[2]/div/div[2]/div/card/div/div[2]/div[2]/h2/span")
      origin = origin_path.text.split(" ")
      orig = origin[-2].strip("()")
      print("Origin :", origin[-2].strip("()"))

  #Scrolling the page down to load more offers
      driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
      time.sleep(10)
      driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
      time.sleep(5)
      driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
      time.sleep(5)
      driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
      time.sleep(5)

      wait.until(EC.visibility_of_element_located((By.XPATH,("//*[@id='content']/div/div/div[3]/div[2]/div/div[6]/div[2]/div/div[2]"))))
      promotion_table = driver.find_element_by_xpath("//*[@id='content']/div/div/div[3]/div[2]/div/div[6]/div[2]/div/div[2]")
      lists = promotion_table.find_elements_by_class_name("card--ff")
      for each_list in lists[:10]:
        content = each_list.text.splitlines()
        print len(content)
        print content
        if len(content)==16:
          destination = content[2].split(" ")
          print("Destination : ", destination[-1].strip("()"))
          classes = "Y"
          validity = None
          startdate = None
          enddate = None
          if "Economy" in content[4]:
            classes = "Y"
            print("Class : ", classes)
          if "Business" in content[4]:
            classes = "J"
            print("Class :", classes)
          if "First" in content[4]:
            classes = "Z"
            print("Class : ", classes)
          fare = content[6]
          print("Fare : ", fare[:-1])
          currency = content[7]
          print("Currency :", currency)
          validity = content[9].split(" ")
          if "days" in validity:
            valid = validity[-2]
            print("valid:",valid)
            valid_till = datetime.datetime.now() + timedelta(days= int(valid))
            #validtill = datetime.strftime(valid_till)
            print("valid_till :", valid_till)
          if "hours" in validity:
            valid = validity[-2]
            print("valid:",valid)
            valid_till = datetime.datetime.now() + timedelta(hours = int(valid))
            #validtill = datetime.strftime(valid_till)
            print("valid_till :", valid_till)
          if "hour(s)" in validity:
            valid = validity[-4]
            print("valid:",valid)
            valid_till = datetime.datetime.now() + timedelta(hours = int(valid))
            #validtill = datetime.strftime(valid_till)
            print("valid_till :", valid_till)
          travel = content[8].split(" ")
          print(travel)
          print(travel[3]+" "+travel[4]+" "+travel[5])
          dt = parse(travel[3]+" "+travel[4]+" "+travel[5])
          startdate = dt.strftime('%Y-%m-%d')
          print("dep_date_from : ", startdate )
          dt = parse(travel[7]+" "+travel[8]+" "+travel[9])
          enddate = dt.strftime('%Y-%m-%d')
          print("dep_date_to : ", enddate)

          data_doc = {
            "Airline" : "EK",
            "OD" : origin[-1].strip("()")+destination[-1].strip("()"),
            "Valid from" : '',
            "Valid till" : valid_till,
            "Compartment" : classes,
            "Fare" : fare[:-1],
            "Currency" : currency,
            "Minimum Stay" : '',
            "Maximum Stay" : '',
            "dep_date_from" : startdate,
            "dep_date_to" : enddate,
            "Url" : url
             }
          doc = {
            "Airline" : "EK",
            "OD" : origin[-1].strip("()")+destination[-1].strip("()"),
            "Valid from" : '',
            "Valid till" : valid_till,
            "Compartment" : classes,
            "Fare" : fare[:-1],
            "Currency" : currency,
            "Minimum Stay" : '',
            "Maximum Stay" : '',
            "dep_date_from" : startdate,
            "dep_date_to" : enddate,
            "Url" : url,
            "Last Updated Date" : datetime.datetime.now().strftime ("%Y-%m-%d"),
            "Last Updated Time" : datetime.datetime.now().strftime ("%H")
             }

          # db.JUP_DB_Promotions.update(data_doc,doc, upsert = True)

        if len(content)==21:
          print(content)
          destination = content[2].split(" ")
          print("Destination : ", destination[-1].strip("()"))
          classes = "Y"
          validity = None
          startdate = None
          enddate = None
          if "Economy" in content[4]:
            classes = "Y"
            print("Class : ", classes)
          if "Business" in content[4]:
            classes = "J"
            print("Class :", classes)
          if "First" in content[4]:
            classes = "Z"
            print("Class : ", classes)
          fare = content[6]
          print("Fare : ", fare[:-1])
          currency = content[7]
          print("Currency :", currency)
          validity = content[13].split(" ")
          if "days" in validity:
            valid = validity[-2]
            print("valid:",valid)
            valid_till = datetime.datetime.now() + timedelta(days= int(valid))
            #validtill = datetime.strftime(valid_till)
            print("valid_till :", valid_till)
          if "hours" in validity:
            valid = validity[-2]
            print("valid:",valid)
            valid_till = datetime.datetime.now() + timedelta(hours = int(valid))
            #validtill = datetime.strftime(valid_till)
            print("valid_till :", valid_till)
          if "hour(s)" in validity:
            valid = validity[-4]
            print("valid:",valid)
            valid_till = datetime.datetime.now() + timedelta(hours = int(valid))
            #validtill = datetime.strftime(valid_till)
            print("valid_till :", valid_till)
          travel = content[17].split(" ")
          print(travel)
          print(travel[0]+" "+travel[1]+" "+travel[2])
          dt = parse(travel[0]+" "+travel[1]+" "+travel[2])
          startdate = dt.strftime('%Y-%m-%d')
          print("dep_date_from : ", startdate )
          dt = parse(travel[4]+" "+travel[5]+" "+travel[6])
          enddate = dt.strftime('%Y-%m-%d')
          print("dep_date_to : ", enddate)

          data_doc = {
            "Airline" : "EK",
            "OD" : origin[-1].strip("()")+destination[-1].strip("()"),
            "Valid from" : '',
            "Valid till" : valid_till,
            "Compartment" : classes,
            "Fare" : fare[:-1],
            "Currency" : currency,
            "Minimum Stay" : '',
            "Maximum Stay" : '',
            "dep_date_from" : startdate,
            "dep_date_to" : enddate,
            "Url" : url
             }
          doc = {
            "Airline" : "EK",
            "OD" : origin[-1].strip("()")+destination[-1].strip("()"),
            "Valid from" : '',
            "Valid till" : valid_till,
            "Compartment" : classes,
            "Fare" : fare[:-1],
            "Currency" : currency,
            "Minimum Stay" : '',
            "Maximum Stay" : '',
            "dep_date_from" : startdate,
            "dep_date_to" : enddate,
            "Url" : url,
            "Last Updated Date" : datetime.datetime.now().strftime ("%Y-%m-%d"),
            "Last Updated Time" : datetime.datetime.now().strftime ("%H")
             }

          # db.JUP_DB_Promotions.update(data_doc,doc, upsert = True)


        if len(content)==21:
          print(content)
          destination = content[2].split(" ")
          print("Destination : ", destination[-1].strip("()"))
          classes = "Y"
          validity = None
          startdate = None
          enddate = None
          if "Economy" in content[9]:
            classes = "Y"
            print("Class : ", classes)
          if "Business" in content[9]:
            classes = "J"
            print("Class :", classes)
          if "First" in content[9]:
            classes = "Z"
            print("Class : ", classes)
          fare = content[11]
          print("Fare : ", fare[:-1])
          currency = content[12]
          print("Currency :", currency)
          validity = content[13].split(" ")
          if "days" in validity:
            valid = validity[-2]
            print("valid:",valid)
            valid_till = datetime.datetime.now() + timedelta(days= int(valid))
            #validtill = datetime.strftime(valid_till)
            print("valid_till :", valid_till)
          if "hours" in validity:
            valid = validity[-2]
            print("valid:",valid)
            valid_till = datetime.datetime.now() + timedelta(hours = int(valid))
            #validtill = datetime.strftime(valid_till)
            print("valid_till :", valid_till)
          if "hour(s)" in validity:
            valid = validity[-4]
            print("valid:",valid)
            valid_till = datetime.datetime.now() + timedelta(hours = int(valid))
            #validtill = datetime.strftime(valid_till)
            print("valid_till :", valid_till)
          travel = content[17].split(" ")
          print(travel)
          print(travel[3]+" "+travel[4]+" "+travel[5])
          dt = parse(travel[3]+" "+travel[4]+" "+travel[5])
          startdate = dt.strftime('%Y-%m-%d')
          print("dep_date_from : ", startdate )
          dt = parse(travel[4]+" "+travel[5]+" "+travel[6])
          enddate = dt.strftime('%Y-%m-%d')
          print("dep_date_to : ", enddate)

          data_doc = {
            "Airline" : "EK",
            "OD" : origin[-1].strip("()")+destination[-1].strip("()"),
            "Valid from" : '',
            "Valid till" : valid_till,
            "Compartment" : classes,
            "Fare" : fare[:-1],
            "Currency" : currency,
            "Minimum Stay" : '',
            "Maximum Stay" : '',
            "dep_date_from" : startdate,
            "dep_date_to" : enddate,
            "Url" : url
             }
          doc = {
            "Airline" : "EK",
            "OD" : origin[-1].strip("()")+destination[-1].strip("()"),
            "Valid from" : '',
            "Valid till" : valid_till,
            "Compartment" : classes,
            "Fare" : fare[:-1],
            "Currency" : currency,
            "Minimum Stay" : '',
            "Maximum Stay" : '',
            "dep_date_from" : startdate,
            "dep_date_to" : enddate,
            "Url" : url,
            "Last Updated Date" : datetime.datetime.now().strftime ("%Y-%m-%d"),
            "Last Updated Time" : datetime.datetime.now().strftime ("%H")
             }

          # db.JUP_DB_Promotions.update(data_doc,doc, upsert = True)
          print "yes"


  #Scraping promotions for countries which do not have multiple cities as origin
  country = ["af"]
  for each_country in country:
    driver.get("https://www.emirates.com/" + each_country + "/english/book/featured-fares/")
    url = "https://www.emirates.com/" + each_country + "/english/book/featured-fares/"
    print(url)
    time.sleep(5)

    check_offers = driver.find_element_by_xpath("//*[@id='content']/div/div/div[3]/div[2]/div/div[6]/div[2]")
    if "Sorry" in check_offers.text:
      print("No offers")
    else:
      wait.until(EC.visibility_of_element_located(
        (By.XPATH, ("//*[@id='content']/div/div/div[3]/div[2]/div/div[6]/div[2]/div/div[2]/div/h2"))))

      origin_path = driver.find_element_by_xpath(
        "//*[@id='content']/div/div/div[3]/div[2]/div/div[6]/div[2]/div/div[2]/div/card/div/div[2]/div[2]/h2/span")
      # origin_path = driver.find_element_by_xpath(
      # "//*[@id='content']/div/div/div[3]/div[2]/div/div[6]/div[2]/div/div[2]/div/card/div/div2/div2/h2/span")
      origin = origin_path.text.split(" ")
      orig = origin[-2].strip("()")
      print("Origin :", origin[-2].strip("()"))

      # Scrolling the page down to load more offers
      driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
      time.sleep(5)
      driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
      time.sleep(5)
      driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
      time.sleep(5)
      driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
      time.sleep(5)

      wait.until(EC.visibility_of_element_located(
        (By.XPATH, ("//*[@id='content']/div/div/div[3]/div[2]/div/div[6]/div[2]/div/div[2]"))))
      #print "1--done"
      promotion_table2 = driver.find_element_by_xpath(
        "//*[@id='content']/div/div/div[3]/div[2]/div/div[6]/div[2]/div/div[2]")
      #print "2--done"
      lists2 = promotion_table2.find_elements_by_class_name("card--ff")
      print lists2
      for each_list in lists2[:10]:
        #print "3--done"
        content = each_list.text.splitlines()
        print len(content)
        if len(content) == 16:
          print(content)
          destination = content[2].split(" ")
          print("Destination : ", destination[-1].strip("()"))
          classes = "Y"
          if "Economy" in content[4]:
            classes = "Y"
            print("Class : ", classes)
          if "Business" in content[4]:
            classes = "J"
            print("Class :", classes)
          if "First" in content[4]:
            classes = "Z"
            print("Class : ", classes)
          fare = content[6]
          print("Fare : ", fare[:-1])
          currency = content[7]
          print("Currency :", currency)
          validity = content[9].split(" ")
          if "days" in validity:
            valid = validity[-2]
            print("valid:",valid)
            valid_till = datetime.datetime.now() + timedelta(days=int(valid))
            # validtill = datetime.strftime(valid_till)
            print("valid_till :", valid_till)
          if "hours" in validity:
            valid = validity[-2]
            print("valid:",valid)
            valid_till = datetime.datetime.now() + timedelta(hours=int(valid))
          if "hour(s)" in validity:
            valid = validity[-4]
            valid_till = datetime.datetime.now() + timedelta(hours=int(valid))
            # validtill = datetime.strftime(valid_till)
            print("valid_till :", valid_till)
          travel = content[8].split(" ")
          print(travel)
          print(travel[3] + " " + travel[4] + " " + travel[5])
          dt = parse(travel[3] + " " + travel[4] + " " + travel[5])
          startdate = dt.strftime('%Y-%m-%d')
          print("dep_date_from : ", startdate)
          dt = parse(travel[7] + " " + travel[8] + " " + travel[9])
          enddate = dt.strftime('%Y-%m-%d')
          print("dep_date_to : ", enddate)

          data_doc = {
            "Airline": "EK",
            "OD": origin[-1].strip("()") + destination[-1].strip("()"),
            "Valid from": '',
            "Valid till": valid_till,
            "Compartment": classes,
            "Fare": fare[:-1],
            "Currency": currency,
            "Minimum Stay": '',
            "Maximum Stay": '',
            "dep_date_from": startdate,
            "dep_date_to": enddate,
            "Url": url
          }
          doc = {
            "Airline": "EK",
            "OD": origin[-1].strip("()") + destination[-1].strip("()"),
            "Valid from": '',
            "Valid till": valid_till,
            "Compartment": classes,
            "Fare": fare[:-1],
            "Currency": currency,
            "Minimum Stay": '',
            "Maximum Stay": '',
            "dep_date_from": startdate,
            "dep_date_to": enddate,
            "Url": url,
            "Last Updated Date": datetime.datetime.now().strftime("%Y-%m-%d"),
            "Last Updated Time": datetime.datetime.now().strftime("%H")
          }

          # db.JUP_DB_Promotions.update(data_doc, doc, upsert=True)
          print "yes1"

        if len(content) == 21:
          print "11----done"
          print(content)
          destination = content[1].split(" ")
          print("Destination : ", destination[-1].strip("()"))
          classes = "Y"
          if "Economy" in content[3]:
            classes = "Y"
            print("Class : ", classes)
          if "Business" in content[3]:
            classes = "J"
            print("Class :", classes)
          if "First" in content[3]:
            classes = "Z"
            print("Class : ", classes)
          fare = content[5]
          print("Fare : ", fare[:-1])
          currency = content[6]
          print("Currency :", currency)
          validity = content[12].split(" ")
          if "days" in validity:
            valid = validity[-2]
            print("valid:",valid)
            valid_till = datetime.datetime.now() + timedelta(days=int(valid))
            # validtill = datetime.strftime(valid_till)
            print("valid_till :", valid_till)
          if "hours" in validity:
            valid = validity[-2]
            print("valid:",valid)
            valid_till = datetime.datetime.now() + timedelta(hours=int(valid))
          if "hour(s)" in validity:
            valid = validity[-4]
            valid_till = datetime.datetime.now() + timedelta(hours=int(valid))
            # validtill = datetime.strftime(valid_till)
            print("valid_till :", valid_till)
          travel = content[16].split(" ")
          print(travel)
          print(travel[3] + " " + travel[4] + " " + travel[5])
          dt = parse(travel[0] + " " + travel[1] + " " + travel[2])
          startdate = dt.strftime('%Y-%m-%d')
          print("dep_date_from : ", startdate)
          dt = parse(travel[4] + " " + travel[5] + " " + travel[6])
          enddate = dt.strftime('%Y-%m-%d')
          print("dep_date_to : ", enddate)

          data_doc = {
            "Airline": "EK",
            "OD": origin[-1].strip("()") + destination[-1].strip("()"),
            "Valid from": '',
            "Valid till": valid_till,
            "Compartment": classes,
            "Fare": fare[:-1],
            "Currency": currency,
            "Minimum Stay": '',
            "Maximum Stay": '',
            "dep_date_from": startdate,
            "dep_date_to": enddate,
            "Url": url
          }
          doc = {
            "Airline": "EK",
            "OD": origin[-1].strip("()") + destination[-1].strip("()"),
            "Valid from": '',
            "Valid till": valid_till,
            "Compartment": classes,
            "Fare": fare[:-1],
            "Currency": currency,
            "Minimum Stay": '',
            "Maximum Stay": '',
            "dep_date_from": startdate,
            "dep_date_to": enddate,
            "Url": url,
            "Last Updated Date": datetime.datetime.now().strftime("%Y-%m-%d"),
            "Last Updated Time": datetime.datetime.now().strftime("%H")
          }

          db.JUP_DB_Promotions.update(data_doc, doc, upsert=True)
          print "yes2"

        if len(content) == 21:
          print(content)
          destination = content[1].split(" ")
          print("Destination : ", destination[-1].strip("()"))
          classes = "Y"
          if "Economy" in content[8]:
            classes = "Y"
            print("Class : ", classes)
          if "Business" in content[8]:
            classes = "J"
            print("Class :", classes)
          if "First" in content[8]:
            classes = "Z"
            print("Class : ", classes)
          fare = content[10]
          print("Fare : ", fare[:-1])
          currency = content[11]
          print("Currency :", currency)
          validity = content[12].split(" ")
          if "days" in validity:
            valid = validity[-2]
            print("valid:",valid)
            valid_till = datetime.datetime.now() + timedelta(days=int(valid))
            # validtill = datetime.strftime(valid_till)
            print("valid_till :", valid_till)
          if "hours" in validity:
            valid = validity[-2]
            print("valid:",valid)
            valid_till = datetime.datetime.now() + timedelta(hours=int(valid))
          if "hour(s)" in validity:
            valid = validity[-4]
            valid_till = datetime.datetime.now() + timedelta(hours=int(valid))
            # validtill = datetime.strftime(valid_till)
            print("valid_till :", valid_till)
          travel = content[16].split(" ")
          print(travel)
          print(travel[3] + " " + travel[4] + " " + travel[5])
          dt = parse(travel[0] + " " + travel[1] + " " + travel[2])
          startdate = dt.strftime('%Y-%m-%d')
          print("dep_date_from : ", startdate)
          dt = parse(travel[4] + " " + travel[5] + " " + travel[6])
          enddate = dt.strftime('%Y-%m-%d')
          print("dep_date_to : ", enddate)

          data_doc = {
            "Airline": "EK",
            "OD": origin[-1].strip("()") + destination[-1].strip("()"),
            "Valid from": '',
            "Valid till": valid_till,
            "Compartment": classes,
            "Fare": fare[:-1],
            "Currency": currency,
            "Minimum Stay": '',
            "Maximum Stay": '',
            "dep_date_from": startdate,
            "dep_date_to": enddate,
            "Url": url
          }
          doc = {
            "Airline": "EK",
            "OD": origin[-1].strip("()") + destination[-1].strip("()"),
            "Valid from": '',
            "Valid till": valid_till,
            "Compartment": classes,
            "Fare": fare[:-1],
            "Currency": currency,
            "Minimum Stay": '',
            "Maximum Stay": '',
            "dep_date_from": startdate,
            "dep_date_to": enddate,
            "Url": url,
            "Last Updated Date": datetime.datetime.now().strftime("%Y-%m-%d"),
            "Last Updated Time": datetime.datetime.now().strftime("%H")
          }

          db.JUP_DB_Promotions.update(data_doc, doc, upsert=True)
          print "yes3"



  #Scraping promotions for countries which have multiple cities as origin
  country = ["in", "pk", "ru", "sa", "vn"]
  for each_country in country:
    driver.get("https://www.emirates.com/"+each_country+"/english/book/featured-fares/")
    url = "https://www.emirates.com/"+each_country+"/english/book/featured-fares/"
    print(url)
    time.sleep(5)
    check_offers = driver.find_element_by_xpath("//*[@id='content']/div/div/div[3]/div[2]/div/div[6]/div[2]")
    print "----1 done"
    if "Sorry" in check_offers.text:
      print("No offers")
    else:
      city = ["Ahmedabad (AMD)", "Bengaluru (BLR)", "Chennai (MAA)", "Delhi (DEL)", "Hyderabad (HYD)", "Kochi (COK)", "Kolkata (CCU)", "Mumbai (BOM)", "Thiruvananthapuram (TRV)", "Karachi (KHI)", "Islamabad (ISB)", "Lahore (LHE)", "Multan (MUX)", "Peshawar (PEW)", "Sialkot (SKT)", "Moscow (DME)", "St Petersburg (LED)", "Dammam (DMM)", "Jeddah (JED)", "Medina (Mainah)(MED)", "Riyadh (RUH)", "Hanoi (HAN)", "Ho Chi Minh City (SGN)"]
      button_dummy = driver.find_element_by_xpath(".//*[@id='content']/div/div/div[3]/div[2]/div/div[6]/div[2]/div/div[1]/div/div[1]/div[1]/div/select-fly-out/eol-select/div[1]/i[1]")
      button = button_dummy.click()
      print "----2 done"
      select_city = driver.find_element_by_xpath("//*[@id='content']/div/div/div[3]/div[2]/div/div[6]/div[2]/div/div/div/div[1]/div[1]/div/select-fly-out/eol-select/div[2]/div/section/ul")
      #select_city = driver.find_element_by_xpath("//*[@id='content']/div/div/div[3]/div[2]/div/div[6]/div[2]/div/div[1]/div/div[1]/div[1]/div/select-fly-out//eol-select/div[2]/div[1]/section/ul")

      city_names = select_city.find_elements_by_tag_name("li")
      print city_names
      print "----3 done"
      each_city_list = []
      for i in range(len(city_names)):
        each_city_list.append(city_names[i].text)
      print each_city_list
      # for i in range(len(city_names)):
      #   print i
      #   print(len(city_names))
      #   print city_names[0].text
      #   print("gjsd", city_names[i].text)

      # for each_city in each_city_list:
      #   print("City :", each_city)
      print "Length of city_names = ", len(city_names)
      for i in range(len(city_names)):
        print "City name:", city_names[i]
        print "City text: ", city_names[i].text
        city_names[i].click()
        print "Clicked: ", i
        button = button_dummy.click()
        print "Button clicked."
        select_city = driver.find_element_by_xpath("//*[@id='content']/div/div/div[3]/div[2]/div/div[6]/div[2]/div/div/div/div[1]/div[1]/div/select-fly-out/eol-select/div[2]/div/section/ul")
        city_names = select_city.find_elements_by_tag_name("li")



      for i in range(len(city_names)):
        print "City: ", city_names[i].text
        city_names[i].click()
        submit = driver.find_element_by_xpath("//*[@id='content']/div/div/div[3]/div[2]/div/div[6]/div[2]/div/div[1]/div/div[1]/div[3]/a").click()
        time.sleep(10)

        #Scrolling the page down to load more offers
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(10)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(5)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(5)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(5)
        #wait.until(EC.visibility_of_element_located((By.XPATH,("//*[@id='content']/div/div/div[3]/div[2]/div/div[6]/div[2]/div/div[1]/div/div[1]/div[1]/div/select-fly-out/eol-select/div"))))
        #button = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[1]/div/div[1]/div[1]/div/select-fly-out/div").click()
        origin_path = driver.find_element_by_xpath(".//*[@id='special_trip_from_0']")
        origin = origin_path.text.split(" ")
        print origin
        orig = origin[-2].strip("()")
        print("Origin :", origin[-2].strip("()"))

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(10)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        wait.until(EC.visibility_of_element_located((By.XPATH,("//*[@id='content']/div/div/div[3]/div[2]/div/div[6]/div[2]/div/div[2]"))))
        promotion_table = driver.find_element_by_xpath("//*[@id='content']/div/div/div[3]/div[2]/div/div[6]/div[2]/div/div[2]")
        lists = promotion_table.find_elements_by_class_name("card--ff")
        for each_list in lists[:10]:
          content = each_list.text.splitlines()
          print(len(content))
          if len(content)==16:
            print(content)
            destination = content[2].split(" ")
            print("Destination : ", destination[-1].strip("()"))
            classes = "Y"
            validity = None
            startdate = None
            enddate = None
            if "Economy" in content[4]:
              classes = "Y"
              print("Class : ", classes)
            if "Business" in content[4]:
              classes = "J"
              print("Class :", classes)
            if "First" in content[4]:
              classes = "Z"
              print("Class : ", classes)
            fare = content[6]
            print("Fare : ", fare[:-1])
            currency = content[7]
            print("Currency :", currency)
            validity = content[9].split(" ")
            if "days" in validity:
              valid = validity[-2]
              print("valid:" ,valid)
              valid_till = datetime.datetime.now() + timedelta(days= int(valid))
              #validtill = datetime.strftime(valid_till)
              print("valid_till :", valid_till)
            if "hours" in validity:
              valid = validity[-2]
              print("valid:",valid)
              valid_till = datetime.datetime.now() + timedelta(hours = int(valid))
              #validtill = datetime.strftime(valid_till)
              print("valid_till :", valid_till)
            if "hour(s)" in validity:
              valid = validity[-4]
              print("valid:",valid)
              valid_till = datetime.datetime.now() + timedelta(hours = int(valid))
              #validtill = datetime.strftime(valid_till)
              print("valid_till :", valid_till)
            travel = content[8].split(" ")
            print(travel)
            print(travel[3]+" "+travel[4]+" "+travel[5])
            dt = parse(travel[3]+" "+travel[4]+" "+travel[5])
            startdate = dt.strftime('%Y-%m-%d')
            print("dep_date_from : ", startdate )
            dt = parse(travel[7]+" "+travel[8]+" "+travel[9])
            enddate = dt.strftime('%Y-%m-%d')
            print("dep_date_to : ", enddate)

            data_doc = {
              "Airline" : "EK",
              "OD" : origin[-1].strip("()")+destination[-1].strip("()"),
              "Valid from" : '',
              "Valid till" : valid_till,
              "Compartment" : classes,
              "Fare" : fare[:-1],
              "Currency" : currency,
              "Minimum Stay" : '',
              "Maximum Stay" : '',
              "dep_date_from" : startdate,
              "dep_date_to" : enddate,
              "Url" : url
               }
            doc = {
              "Airline" : "EK",
              "OD" : origin[-1].strip("()")+destination[-1].strip("()"),
              "Valid from" : '',
              "Valid till" : valid_till,
              "Compartment" : classes,
              "Fare" : fare[:-1],
              "Currency" : currency,
              "Minimum Stay" : '',
              "Maximum Stay" : '',
              "dep_date_from" : startdate,
              "dep_date_to" : enddate,
              "Url" : url,
              "Last Updated Date" : datetime.datetime.now().strftime ("%Y-%m-%d"),
              "Last Updated Time" : datetime.datetime.now().strftime ("%H")
               }
            #unhash it later!
            #db.JUP_DB_Promotions.update(data_doc,doc, upsert = True)
            print "yes"
        button = driver.find_element_by_xpath(".//*[@id='content']/div/div/div[3]/div[2]/div/div[6]/div[2]/div/div[1]/div/div[1]/div[1]/div/select-fly-out/eol-select/div[1]/i[1]").click()
        select_city = driver.find_element_by_xpath(
          "//*[@id='content']/div/div/div[3]/div[2]/div/div[6]/div[2]/div/div/div/div[1]/div[1]/div/select-fly-out/eol-select/div[2]/div/section/ul")
        city_names = select_city.find_elements_by_tag_name("li")
  driver.close()

"""
  #Scraping promotions for countries which have multiple cities as origin
  country = ["in", "pk", "ru", "sa", "vn"]
  for each_country in country:
    driver.get("https://www.emirates.com/"+each_country+"/english/book/featured-fares/")
    url = "https://www.emirates.com/"+each_country+"/english/book/featured-fares/"
    print(url)
    time.sleep(5)
    check_offers = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]")
    if "Sorry" in check_offers.text:
      print("No offers")
    else:
      city = ["Ahmedabad (AMD)", "Bengaluru (BLR)", "Chennai (MAA)", "Delhi (DEL)", "Hyderabad (HYD)", "Kochi (COK)", "Kolkata (CCU)", "Mumbai (BOM)", "Thiruvananthapuram (TRV)", "Karachi (KHI)", "Islamabad (ISB)", "Lahore (LHE)", "Multan (MUX)", "Peshawar (PEW)", "Sialkot (SKT)", "Moscow (DME)", "St Petersburg (LED)", "Dammam (DMM)", "Jeddah (JED)", "Medina (Mainah)(MED)", "Riyadh (RUH)", "Hanoi (HAN)", "Ho Chi Minh City (SGN)"]
      button = driver.find_element_by_xpath("//*[@id='from']").click()
      select_city = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[1]/div/div[1]/div[1]/div/select-fly-out/div/ul")
      city_names = select_city.find_elements_by_tag_name("li")
      for i in range(len(city_names)):
        print(len(city_names))
        print(city_names[i].text)
        for each_city in city:
          if each_city in city_names[i].text:
            print("City :", each_city)
            city_names[i].click()
            submit = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[1]/div/div[1]/div[3]/a").click()
            time.sleep(10)

            #Scrolling the page down to load more offers
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(10)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(5)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(5)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(5)
            wait.until(EC.visibility_of_element_located((By.XPATH,("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[1]/div/div[1]/div[1]/div/select-fly-out/div"))))
            #button = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[1]/div/div[1]/div[1]/div/select-fly-out/div").click()
            origin_path = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[2]/div/h2")
            origin = origin_path.text.split(" ")
            orig = origin[-1].strip("()")
            print("Origin :", origin[-1].strip("()"))

            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(10)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            wait.until(EC.visibility_of_element_located((By.XPATH,("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[2]"))))
            promotion_table = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[2]")
            lists = promotion_table.find_elements_by_class_name("card--ff")
            for each_list in lists[:10]:
              content = each_list.text.splitlines()
              print(content)
              print(len(content))
              if len(content)==21:
                print(content)
                destination = content[2].split(" ")
                print("Destination : ", destination[-1].strip("()"))
                classes = "Y"
                validity = None
                startdate = None
                enddate = None
                if "Economy" in content[4]:
                  classes = "Y"
                  print("Class : ", classes)
                if "Business" in content[4]:
                  classes = "J"
                  print("Class :", classes)
                if "First" in content[4]:
                  classes = "Z"
                  print("Class : ", classes)
                fare = content[6]
                print("Fare : ", fare[:-1])
                currency = content[7]
                print("Currency :", currency)
                validity = content[13].split(" ")
                if "days" in validity:
                  valid = validity[-2]
                  print("valid:",valid)
                  valid_till = datetime.datetime.now() + timedelta(days= int(valid))
                  #validtill = datetime.strftime(valid_till)
                  print("valid_till :", valid_till)
                if "hours" in validity:
                  valid = validity[-2]
                  print("valid:",valid)
                  valid_till = datetime.datetime.now() + timedelta(hours = int(valid))
                  #validtill = datetime.strftime(valid_till)
                  print("valid_till :", valid_till)
                if "hour(s)" in validity:
                  valid = validity[-4]
                  print("valid:",valid)
                  valid_till = datetime.datetime.now() + timedelta(hours = int(valid))
                  #validtill = datetime.strftime(valid_till)
                  print("valid_till :", valid_till)
                travel = content[17].split(" ")
                print(travel)
                print(travel[0]+" "+travel[1]+" "+travel[2])
                dt = parse(travel[0]+" "+travel[1]+" "+travel[2])
                startdate = dt.strftime('%Y-%m-%d')
                print("dep_date_from : ", startdate )
                dt = parse(travel[4]+" "+travel[5]+" "+travel[6])
                enddate = dt.strftime('%Y-%m-%d')
                print("dep_date_to : ", enddate)

                data_doc = {
                  "Airline" : "EK",
                  "OD" : origin[-1].strip("()")+destination[-1].strip("()"),
                  "Valid from" : '',
                  "Valid till" : valid_till,
                  "Compartment" : classes,
                  "Fare" : fare[:-1],
                  "Currency" : currency,
                  "Minimum Stay" : '',
                  "Maximum Stay" : '',
                  "dep_date_from" : startdate,
                  "dep_date_to" : enddate,
                  "Url" : url
                   }
                doc = {
                  "Airline" : "EK",
                  "OD" : origin[-1].strip("()")+destination[-1].strip("()"),
                  "Valid from" : '',
                  "Valid till" : valid_till,
                  "Compartment" : classes,
                  "Fare" : fare[:-1],
                  "Currency" : currency,
                  "Minimum Stay" : '',
                  "Maximum Stay" : '',
                  "dep_date_from" : startdate,
                  "dep_date_to" : enddate,
                  "Url" : url,
                  "Last Updated Date" : datetime.datetime.now().strftime ("%Y-%m-%d"),
                  "Last Updated Time" : datetime.datetime.now().strftime ("%H")
                   }

                db.JUP_DB_Promotions.update(data_doc,doc, upsert = True)
                print "yes"
                button = driver.find_element_by_xpath("//*[@id='from']").click()
                select_city = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[1]/div/div[1]/div[1]/div/select-fly-out/div/ul")
                city_names = select_city.find_elements_by_tag_name("li")

  #Scraping promotions for countries which have multiple cities as origin
  country = ["in", "pk", "ru", "sa", "vn"]
  for each_country in country:
    driver.get("https://www.emirates.com/"+each_country+"/english/book/featured-fares/")
    url = "https://www.emirates.com/"+each_country+"/english/book/featured-fares/"
    print(url)
    time.sleep(5)
    check_offers = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]")
    if "Sorry" in check_offers.text:
      print("No offers")
    else:
      city = ["Ahmedabad (AMD)", "Bengaluru (BLR)", "Chennai (MAA)", "Delhi (DEL)", "Hyderabad (HYD)", "Kochi (COK)", "Kolkata (CCU)", "Mumbai (BOM)", "Thiruvananthapuram (TRV)", "Karachi (KHI)", "Islamabad (ISB)", "Lahore (LHE)", "Multan (MUX)", "Peshawar (PEW)", "Sialkot (SKT)", "Moscow (DME)", "St Petersburg (LED)", "Dammam (DMM)", "Jeddah (JED)", "Medina (Mainah)(MED)", "Riyadh (RUH)", "Hanoi (HAN)", "Ho Chi Minh City (SGN)"]
      button = driver.find_element_by_xpath("//*[@id='from']").click()
      select_city = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[1]/div/div[1]/div[1]/div/select-fly-out/div/ul")
      city_names = select_city.find_elements_by_tag_name("li")
      for i in range(len(city_names)):
        print(len(city_names))
        print(city_names[i].text)
        for each_city in city:
          if each_city in city_names[i].text:
            print("City :", each_city)
            city_names[i].click()
            submit = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[1]/div/div[1]/div[3]/a").click()
            time.sleep(10)

            #Scrolling the page down to load more offers
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(10)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(5)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(5)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(5)
            wait.until(EC.visibility_of_element_located((By.XPATH,("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[1]/div/div[1]/div[1]/div/select-fly-out/div"))))
            #button = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[1]/div/div[1]/div[1]/div/select-fly-out/div").click()
            origin_path = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[2]/div/h2")
            origin = origin_path.text.split(" ")
            orig = origin[-1].strip("()")
            print("Origin :", origin[-1].strip("()"))

            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(10)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            wait.until(EC.visibility_of_element_located((By.XPATH,("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[2]"))))
            promotion_table = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[2]")
            lists = promotion_table.find_elements_by_class_name("card--ff")
            for each_list in lists[:10]:
              content = each_list.text.splitlines()
              print(content)
              print(len(content))

              if len(content)==21:
                print(content)
                destination = content[2].split(" ")
                print("Destination : ", destination[-1].strip("()"))
                classes = "Y"
                validity = None
                startdate = None
                enddate = None
                if "Economy" in content[9]:
                  classes = "Y"
                  print("Class : ", classes)
                if "Business" in content[9]:
                  classes = "J"
                  print("Class :", classes)
                if "First" in content[9]:
                  classes = "Z"
                  print("Class : ", classes)
                fare = content[11]
                print("Fare : ", fare[:-1])
                currency = content[12]
                print("Currency :", currency)
                validity = content[13].split(" ")
                if "days" in validity:
                  valid = validity[-2]
                  print("valid:",valid)
                  valid_till = datetime.datetime.now() + timedelta(days= int(valid))
                  #validtill = datetime.strftime(valid_till)
                  print("valid_till :", valid_till)
                if "hours" in validity:
                  valid = validity[-2]
                  print("valid:",valid)
                  valid_till = datetime.datetime.now() + timedelta(hours = int(valid))
                  #validtill = datetime.strftime(valid_till)
                  print("valid_till :", valid_till)
                if "hour(s)" in validity:
                  valid = validity[-4]
                  print("valid:",valid)
                  valid_till = datetime.datetime.now() + timedelta(hours = int(valid))
                  #validtill = datetime.strftime(valid_till)
                  print("valid_till :", valid_till)
                travel = content[17].split(" ")
                print(travel)
                print(travel[0]+" "+travel[1]+" "+travel[2])
                dt = parse(travel[0]+" "+travel[1]+" "+travel[2])
                startdate = dt.strftime('%Y-%m-%d')
                print("dep_date_from : ", startdate )
                dt = parse(travel[4]+" "+travel[5]+" "+travel[6])
                enddate = dt.strftime('%Y-%m-%d')
                print("dep_date_to : ", enddate)

                data_doc = {
                  "Airline" : "EK",
                  "OD" : origin[-1].strip("()")+destination[-1].strip("()"),
                  "Valid from" : '',
                  "Valid till" : valid_till,
                  "Compartment" : classes,
                  "Fare" : fare[:-1],
                  "Currency" : currency,
                  "Minimum Stay" : '',
                  "Maximum Stay" : '',
                  "dep_date_from" : startdate,
                  "dep_date_to" : enddate,
                  "Url" : url
                   }
                doc = {
                  "Airline" : "EK",
                  "OD" : origin[-1].strip("()")+destination[-1].strip("()"),
                  "Valid from" : '',
                  "Valid till" : valid_till,
                  "Compartment" : classes,
                  "Fare" : fare[:-1],
                  "Currency" : currency,
                  "Minimum Stay" : '',
                  "Maximum Stay" : '',
                  "dep_date_from" : startdate,
                  "dep_date_to" : enddate,
                  "Url" : url,
                  "Last Updated Date" : datetime.datetime.now().strftime ("%Y-%m-%d"),
                  "Last Updated Time" : datetime.datetime.now().strftime ("%H")
                   }

                db.JUP_DB_Promotions.update(data_doc,doc, upsert = True)
                print "yes"
                button = driver.find_element_by_xpath("//*[@id='from']").click()
                select_city = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[1]/div/div[1]/div[1]/div/select-fly-out/div/ul")
                city_names = select_city.find_elements_by_tag_name("li")

  #Scraping promotions for countries which have multiple cities as origin
  country = ["in", "pk", "ru", "sa", "vn"]
  for each_country in country:
    driver.get("https://www.emirates.com/"+each_country+"/english/book/featured-fares/")
    url = "https://www.emirates.com/"+each_country+"/english/book/featured-fares/"
    print(url)
    time.sleep(5)
    check_offers = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]")
    if "Sorry" in check_offers.text:
      print("No offers")
    else:
      city = ["Ahmedabad (AMD)", "Bengaluru (BLR)", "Chennai (MAA)", "Delhi (DEL)", "Hyderabad (HYD)", "Kochi (COK)", "Kolkata (CCU)", "Mumbai (BOM)", "Thiruvananthapuram (TRV)", "Karachi (KHI)", "Islamabad (ISB)", "Lahore (LHE)", "Multan (MUX)", "Peshawar (PEW)", "Sialkot (SKT)", "Moscow (DME)", "St Petersburg (LED)", "Dammam (DMM)", "Jeddah (JED)", "Medina (Mainah)(MED)", "Riyadh (RUH)", "Hanoi (HAN)", "Ho Chi Minh City (SGN)"]
      button = driver.find_element_by_xpath("//*[@id='from']").click()
      select_city = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[1]/div/div[1]/div[1]/div/select-fly-out/div/ul")
      city_names = select_city.find_elements_by_tag_name("li")
      for i in range(len(city_names)):
        print(len(city_names))
        print(city_names[i].text)
        for each_city in city:
          if each_city in city_names[i].text:
            print("City :", each_city)
            city_names[i].click()
            submit = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[1]/div/div[1]/div[3]/a").click()
            time.sleep(10)

            #Scrolling the page down to load more offers
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(10)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(5)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(5)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(15)

            wait.until(EC.visibility_of_element_located((By.XPATH,("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[2]/div/h2"))))
            origin_path = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[2]/div/h2")
            origin = origin_path.text.split(" ")
            orig = origin[-1].strip("()")
            print("Origin :", origin[-1].strip("()"))
            promotion_table2 = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[3]")
            lists2 = promotion_table2.find_elements_by_class_name("card--ff")
            for each_list in lists2[:10]:
              content = each_list.text.splitlines()
              print(content)
              print(len(content))
              if len(content)==16:
                print(content)
                destination = content[1].split(" ")
                print("Destination : ", destination[-1].strip("()"))
                classes = "Y"
                if "Economy" in content[3]:
                  classes = "Y"
                  print("Class : ", classes)
                if "Business" in content[3]:
                  classes = "J"
                  print("Class :", classes)
                if "First" in content[3]:
                  classes = "Z"
                  print("Class : ", classes)
                fare = content[5]
                print("Fare : ", fare[:-1])
                currency = content[6]
                print("Currency :", currency)
                validity = content[7].split(" ")
                if "days" in validity:
                  valid = validity[-2]
                  print("valid:",valid)
                  valid_till = datetime.datetime.now() + timedelta(days= int(valid))
                  #validtill = datetime.strftime(valid_till)
                  print("valid_till :", valid_till)
                if "hours" in validity:
                  valid = validity[-2]
                  print("valid:",valid)
                  valid_till = datetime.datetime.now() + timedelta(hours = int(valid))
                if "hour(s)" in validity:
                  valid = validity[-4]
                  valid_till = datetime.datetime.now() + timedelta(hours = int(valid))
                  #validtill = datetime.strftime(valid_till)
                  print("valid_till :", valid_till)
                travel = content[11].split(" ")
                print(travel)
                print(travel[0]+" "+travel[1]+" "+travel[2])
                dt = parse(travel[0]+" "+travel[1]+" "+travel[2])
                startdate = dt.strftime('%Y-%m-%d')
                print("dep_date_from : ", startdate )
                dt = parse(travel[4]+" "+travel[5]+" "+travel[6])
                enddate = dt.strftime('%Y-%m-%d')
                print("dep_date_to : ", enddate)

                data_doc = {
                  "Airline" : "EK",
                  "OD" : origin[-1].strip("()")+destination[-1].strip("()"),
                  "Valid from" : '',
                  "Valid till" : valid_till,
                  "Compartment" : classes,
                  "Fare" : fare[:-1],
                  "Currency" : currency,
                  "Minimum Stay" : '',
                  "Maximum Stay" : '',
                  "dep_date_from" : startdate,
                  "dep_date_to" : enddate,
                  "Url" : url
                   }
                doc = {
                  "Airline" : "EK",
                  "OD" : origin[-1].strip("()")+destination[-1].strip("()"),
                  "Valid from" : '',
                  "Valid till" : valid_till,
                  "Compartment" : classes,
                  "Fare" : fare[:-1],
                  "Currency" : currency,
                  "Minimum Stay" : '',
                  "Maximum Stay" : '',
                  "dep_date_from" : startdate,
                  "dep_date_to" : enddate,
                  "Url" : url,
                  "Last Updated Date" : datetime.datetime.now().strftime ("%Y-%m-%d"),
                  "Last Updated Time" : datetime.datetime.now().strftime ("%H")
                   }

                db.JUP_DB_Promotions.update(data_doc,doc, upsert = True)
                print "yes"
                button = driver.find_element_by_xpath("//*[@id='from']").click()
                select_city = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[1]/div/div[1]/div[1]/div/select-fly-out/div/ul")
                city_names = select_city.find_elements_by_tag_name("li")

  #Scraping promotions for countries which have multiple cities as origin
  country = ["in", "pk", "ru", "sa", "vn"]
  for each_country in country:
    driver.get("https://www.emirates.com/"+each_country+"/english/book/featured-fares/")
    url = "https://www.emirates.com/"+each_country+"/english/book/featured-fares/"
    print(url)
    time.sleep(5)
    check_offers = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]")
    if "Sorry" in check_offers.text:
      print("No offers")
    else:
      city = ["Ahmedabad (AMD)", "Bengaluru (BLR)", "Chennai (MAA)", "Delhi (DEL)", "Hyderabad (HYD)", "Kochi (COK)", "Kolkata (CCU)", "Mumbai (BOM)", "Thiruvananthapuram (TRV)", "Karachi (KHI)", "Islamabad (ISB)", "Lahore (LHE)", "Multan (MUX)", "Peshawar (PEW)", "Sialkot (SKT)", "Moscow (DME)", "St Petersburg (LED)", "Dammam (DMM)", "Jeddah (JED)", "Medina (Mainah)(MED)", "Riyadh (RUH)", "Hanoi (HAN)", "Ho Chi Minh City (SGN)"]
      button = driver.find_element_by_xpath("//*[@id='from']").click()
      select_city = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[1]/div/div[1]/div[1]/div/select-fly-out/div/ul")
      city_names = select_city.find_elements_by_tag_name("li")
      for i in range(len(city_names)):
        print(len(city_names))
        print(city_names[i].text)
        for each_city in city:
          if each_city in city_names[i].text:
            print("City :", each_city)
            city_names[i].click()
            submit = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[1]/div/div[1]/div[3]/a").click()
            time.sleep(10)

            #Scrolling the page down to load more offers
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(10)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(5)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(5)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(15)

            wait.until(EC.visibility_of_element_located((By.XPATH,("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[2]/div/h2"))))
            origin_path = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[2]/div/h2")
            origin = origin_path.text.split(" ")
            orig = origin[-1].strip("()")
            print("Origin :", origin[-1].strip("()"))
            promotion_table2 = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[3]")
            lists2 = promotion_table2.find_elements_by_class_name("card--ff")
            for each_list in lists2[:10]:
              content = each_list.text.splitlines()
              print(content)
              print(len(content))
              if len(content)==21:
                print(content)
                destination = content[1].split(" ")
                print("Destination : ", destination[-1].strip("()"))
                classes = "Y"
                if "Economy" in content[3]:
                  classes = "Y"
                  print("Class : ", classes)
                if "Business" in content[3]:
                  classes = "J"
                  print("Class :", classes)
                if "First" in content[3]:
                  classes = "Z"
                  print("Class : ", classes)
                fare = content[5]
                print("Fare : ", fare[:-1])
                currency = content[6]
                print("Currency :", currency)
                validity = content[12].split(" ")
                if "days" in validity:
                  valid = validity[-2]
                  print("valid:",valid)
                  valid_till = datetime.datetime.now() + timedelta(days= int(valid))
                  #validtill = datetime.strftime(valid_till)
                  print("valid_till :", valid_till)
                if "hours" in validity:
                  valid = validity[-2]
                  print("valid:",valid)
                  valid_till = datetime.datetime.now() + timedelta(hours = int(valid))
                if "hour(s)" in validity:
                  valid = validity[-4]
                  valid_till = datetime.datetime.now() + timedelta(hours = int(valid))
                  #validtill = datetime.strftime(valid_till)
                  print("valid_till :", valid_till)
                travel = content[16].split(" ")
                print(travel)
                print(travel[0]+" "+travel[1]+" "+travel[2])
                dt = parse(travel[0]+" "+travel[1]+" "+travel[2])
                startdate = dt.strftime('%Y-%m-%d')
                print("dep_date_from : ", startdate )
                dt = parse(travel[4]+" "+travel[5]+" "+travel[6])
                enddate = dt.strftime('%Y-%m-%d')
                print("dep_date_to : ", enddate)

                data_doc = {
                  "Airline" : "EK",
                  "OD" : origin[-1].strip("()")+destination[-1].strip("()"),
                  "Valid from" : '',
                  "Valid till" : valid_till,
                  "Compartment" : classes,
                  "Fare" : fare[:-1],
                  "Currency" : currency,
                  "Minimum Stay" : '',
                  "Maximum Stay" : '',
                  "dep_date_from" : startdate,
                  "dep_date_to" : enddate,
                  "Url" : url
                   }
                doc = {
                  "Airline" : "EK",
                  "OD" : origin[-1].strip("()")+destination[-1].strip("()"),
                  "Valid from" : '',
                  "Valid till" : valid_till,
                  "Compartment" : classes,
                  "Fare" : fare[:-1],
                  "Currency" : currency,
                  "Minimum Stay" : '',
                  "Maximum Stay" : '',
                  "dep_date_from" : startdate,
                  "dep_date_to" : enddate,
                  "Url" : url,
                  "Last Updated Date" : datetime.datetime.now().strftime ("%Y-%m-%d"),
                  "Last Updated Time" : datetime.datetime.now().strftime ("%H")
                   }

                db.JUP_DB_Promotions.update(data_doc,doc, upsert = True)
                print "yes"
                button = driver.find_element_by_xpath("//*[@id='from']").click()
                select_city = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[1]/div/div[1]/div[1]/div/select-fly-out/div/ul")
                city_names = select_city.find_elements_by_tag_name("li")


  #Scraping promotions for countries which have multiple cities as origin
  country = ["in", "pk", "ru", "sa", "vn"]
  for each_country in country:
    driver.get("https://www.emirates.com/"+each_country+"/english/book/featured-fares/")
    url = "https://www.emirates.com/"+each_country+"/english/book/featured-fares/"
    print(url)
    time.sleep(5)
    check_offers = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]")
    if "Sorry" in check_offers.text:
      print("No offers")
    else:
      city = ["Ahmedabad (AMD)", "Bengaluru (BLR)", "Chennai (MAA)", "Delhi (DEL)", "Hyderabad (HYD)", "Kochi (COK)", "Kolkata (CCU)", "Mumbai (BOM)", "Thiruvananthapuram (TRV)", "Karachi (KHI)", "Islamabad (ISB)", "Lahore (LHE)", "Multan (MUX)", "Peshawar (PEW)", "Sialkot (SKT)", "Moscow (DME)", "St Petersburg (LED)", "Dammam (DMM)", "Jeddah (JED)", "Medina (Mainah)(MED)", "Riyadh (RUH)", "Hanoi (HAN)", "Ho Chi Minh City (SGN)"]
      button = driver.find_element_by_xpath("//*[@id='from']").click()
      select_city = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[1]/div/div[1]/div[1]/div/select-fly-out/div/ul")
      city_names = select_city.find_elements_by_tag_name("li")
      for i in range(len(city_names)):
        print(len(city_names))
        print(city_names[i].text)
        for each_city in city:
          if each_city in city_names[i].text:
            print("City :", each_city)
            city_names[i].click()
            submit = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[1]/div/div[1]/div[3]/a").click()
            time.sleep(10)

            #Scrolling the page down to load more offers
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(10)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(5)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(5)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(15)

            wait.until(EC.visibility_of_element_located((By.XPATH,("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[2]/div/h2"))))
            origin_path = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[2]/div/h2")
            origin = origin_path.text.split(" ")
            orig = origin[-1].strip("()")
            print("Origin :", origin[-1].strip("()"))
            promotion_table2 = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[3]")
            lists2 = promotion_table2.find_elements_by_class_name("card--ff")
            for each_list in lists2[:10]:
              content = each_list.text.splitlines()
              print(content)
              print(len(content))
              if len(content)==21:
                print(content)
                destination = content[1].split(" ")
                print("Destination : ", destination[-1].strip("()"))
                classes = "Y"
                if "Economy" in content[8]:
                  classes = "Y"
                  print("Class : ", classes)
                if "Business" in content[8]:
                  classes = "J"
                  print("Class :", classes)
                if "First" in content[8]:
                  classes = "Z"
                  print("Class : ", classes)
                fare = content[10]
                print("Fare : ", fare[:-1])
                currency = content[11]
                print("Currency :", currency)
                validity = content[12].split(" ")
                if "days" in validity:
                  valid = validity[-2]
                  print("valid:",valid)
                  valid_till = datetime.datetime.now() + timedelta(days= int(valid))
                  #validtill = datetime.strftime(valid_till)
                  print("valid_till :", valid_till)
                if "hours" in validity:
                  valid = validity[-2]
                  print("valid:",valid)
                  valid_till = datetime.datetime.now() + timedelta(hours = int(valid))
                if "hour(s)" in validity:
                  valid = validity[-4]
                  valid_till = datetime.datetime.now() + timedelta(hours = int(valid))
                  #validtill = datetime.strftime(valid_till)
                  print("valid_till :", valid_till)
                travel = content[16].split(" ")
                print(travel)
                print(travel[0]+" "+travel[1]+" "+travel[2])
                dt = parse(travel[0]+" "+travel[1]+" "+travel[2])
                startdate = dt.strftime('%Y-%m-%d')
                print("dep_date_from : ", startdate )
                dt = parse(travel[4]+" "+travel[5]+" "+travel[6])
                enddate = dt.strftime('%Y-%m-%d')
                print("dep_date_to : ", enddate)

                data_doc = {
                  "Airline" : "EK",
                  "OD" : origin[-1].strip("()")+destination[-1].strip("()"),
                  "Valid from" : '',
                  "Valid till" : valid_till,
                  "Compartment" : classes,
                  "Fare" : fare[:-1],
                  "Currency" : currency,
                  "Minimum Stay" : '',
                  "Maximum Stay" : '',
                  "dep_date_from" : startdate,
                  "dep_date_to" : enddate,
                  "Url" : url
                   }
                doc = {
                  "Airline" : "EK",
                  "OD" : origin[-1].strip("()")+destination[-1].strip("()"),
                  "Valid from" : '',
                  "Valid till" : valid_till,
                  "Compartment" : classes,
                  "Fare" : fare[:-1],
                  "Currency" : currency,
                  "Minimum Stay" : '',
                  "Maximum Stay" : '',
                  "dep_date_from" : startdate,
                  "dep_date_to" : enddate,
                  "Url" : url,
                  "Last Updated Date" : datetime.datetime.now().strftime ("%Y-%m-%d"),
                  "Last Updated Time" : datetime.datetime.now().strftime ("%H")
                   }

                db.JUP_DB_Promotions.update(data_doc,doc, upsert = True)
                print "yes"
                button = driver.find_element_by_xpath("//*[@id='from']").click()
                select_city = driver.find_element_by_xpath("//*[@id='content']/div/div/div[2]/div[2]/div/div[6]/div[2]/div/div[1]/div/div[1]/div[1]/div/select-fly-out/div/ul")
                city_names = select_city.find_elements_by_tag_name("li")
"""

if __name__ == '__main__':
  run()

