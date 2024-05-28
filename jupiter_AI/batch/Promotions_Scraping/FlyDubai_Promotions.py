#Author : Vinusha Anem
#Written : May 2017
from jupiter_AI import JUPITER_LOGGER
from jupiter_AI.logutils import measure



@measure(JUPITER_LOGGER)
def run():

  from selenium import webdriver
  from selenium.webdriver.support.ui import WebDriverWait
  from selenium.webdriver.support import expected_conditions as EC
  from selenium.webdriver.common.by import By
  from requests.exceptions import ConnectionError
  from dateutil.parser import parse
  import datetime

  #Connecting to Local Mongodb
  import pymongo
  import global_variable as var
  try:
      conn=pymongo.MongoClient(var.mongo_client_url)[var.database]
      #conn.authenticate('analytics', 'KNjSZmiaNUGLmS0Bv2', source='admin')
  except ConnectionError:
          print ("Could not connect")

  #Defining the Chrome Path
  chrome_path = r"/home/prathyusha/Downloads/Projects Codes/Promotions/chromedriver"
  driver = webdriver.Chrome(chrome_path)
  driver.get("https://www.flydubai.com/en/offers/")
  driver1 = webdriver.Chrome(chrome_path)
  wait = WebDriverWait(driver, 20)
  wait = WebDriverWait(driver1, 50)

  #Function to scrape the promotions


  @measure(JUPITER_LOGGER)
  def promotions(url):
    driver1.get(url)
    title = driver.find_element_by_css_selector("body > div.page-container.ti-over > section:nth-child(6) > div > div.offset > div > div > h1")
    if "Bank" in title.text:
      print("No offer")
    else:

      wait.until(EC.visibility_of_element_located((By.CLASS_NAME,("highlight-box"))))
      title = driver1.find_element_by_css_selector("body > div.page-container.ti-over > section:nth-child(6) > div > div:nth-child(3)")
      content = driver1.find_elements_by_class_name("highlight-box")
      #print(title.text)

      for i in range(len(content)):
        OD1 = content[i].text.splitlines()
        origin = title.text.splitlines()
        destination = OD1[1].strip()
        print(origin[1]+destination)
        classes = "Y"
        if "Economy" in OD1[2]:
          classes = "Y"
          print(classes)
        if "Business" in OD1[2]:
          classes = "J"
          print(classes)

        fare = None
        currency = None
        if OD1[3].startswith("One-Way"):
          splitting = OD1[3].split(" ")
          print("Type:" , splitting[0])
          fare = splitting[2]
          print("Fare : ", fare )
          currency = splitting[1]
          print("Currency : ",currency )

        if OD1[3].startswith("Return"):
          splitting = OD1[3].split(" ")
          print("Type:" , splitting[0])

          fare = splitting[2]
          print("Fare : ", fare )
          currency = splitting[1]
          print("Currency : ",currency )

        try:
          travel = driver1.find_element_by_css_selector("body > div.page-container.ti-over > section:nth-child(6) > div > div:nth-child(11) > div > div > div")
          tag = travel.find_elements_by_tag_name("li")
          validity = tag[0].text.split("on",1)[1]
          dt = parse(validity)
          valid_till = dt.strftime('%Y-%m-%d')
          print("Valid_till :", valid_till)
          travel_date = tag[1].text.split("between",1)[1]
          date = travel_date.split("and")
          startdate = date[0]
          dt = parse(startdate)
          start_date = dt.strftime('%Y-%m-%d')
          print("dep_date_from:", start_date)
          enddate = date[1]
          dt = parse(enddate)
          end_date = dt.strftime('%Y-%m-%d')
          print("dep_date_to:", end_date)
        except:
          travel = driver1.find_element_by_css_selector("body > div.page-container.ti-over > section:nth-child(6) > div > div:nth-child(10) > div > div > div")
          tag = travel.find_elements_by_tag_name("li")
          validity = tag[0].text.split("on",1)[1]
          dt = parse(validity)
          valid_till = dt.strftime('%Y-%m-%d')
          print("Valid_till :", valid_till)
          travel_date = tag[1].text.split("between",1)[1]
          date = travel_date.split("and")
          startdate = date[0]
          dt = parse(startdate)
          start_date = dt.strftime('%Y-%m-%d')
          print("dep_date_from:", start_date)
          enddate = date[1]
          dt = parse(enddate)
          end_date = dt.strftime('%Y-%m-%d')
          print("dep_date_to:", end_date)

  #Storing the parameters in a dictionary
        data_doc = dict()
        data_doc = {
          "Airline" : "FZ",
          "OD" : origin[1]+destination,
          "Valid from" : '',
          "Valid till" : valid_till,
          "Compartment" : classes ,
          "Type" : splitting[0],
          "Fare" : fare,
          "Currency" : currency,
          "Minimum Stay" : '',
          "Maximum Stay" : '',
          "dep_date_from" : start_date,
          "dep_date_to" : end_date,
          "Url" : urls
            }
        doc = {
          "Airline" : "FZ",
          "OD" : origin[1]+destination,
          "Valid from" : '',
          "Valid till" : valid_till,
          "Compartment" : classes ,
          "Type" : splitting[0],
          "Fare" : fare,
          "Currency" : currency,
          "Minimum Stay" : '',
          "Maximum Stay" : '',
          "dep_date_from" : start_date,
          "dep_date_to" : end_date,
          "Url" : urls,
          "Last Updated Date" : datetime.datetime.now().strftime ("%Y-%m-%d"),
          "Last Updated Time" : datetime.datetime.now().strftime ("%H")
            }
  #Upserting the values into Mongodb
        conn.JUP_DB_Promotions.update(data_doc,doc,upsert = True)


  @measure(JUPITER_LOGGER)
  def promotions1(url):
    driver1.get(url)
    title = driver.find_element_by_css_selector("body > div.page-container.ti-over > section:nth-child(6) > div > div.offset > div > div > h1")
    if "Bank" in title.text:
      print("No offer")
    else:

      wait.until(EC.visibility_of_element_located((By.CLASS_NAME,("highlight-box"))))
      title = driver1.find_element_by_css_selector("body > div.page-container.ti-over > section:nth-child(6) > div > div:nth-child(3)")
      content = driver1.find_elements_by_class_name("highlight-box")
      #print(title.text)

      for i in range(len(content)):
        OD1 = content[i].text.splitlines()
        origin = title.text.splitlines()
        destination = OD1[1].strip()
        print(origin[1]+destination)
        classes = "Y"
        if "Economy" in OD1[2]:
          classes = "Y"
          print(classes)
        if "Business" in OD1[2]:
          classes = "J"
          print(classes)

        fare1 = None
        currency1 = None
        if OD1[4].startswith("Return"):
          splitting1 = OD1[4].split(" ")
          print("Type:", splitting1[0])
          fare1 = splitting1[2]
          print("Fare1 : ",fare1 )
          currency1 = splitting1[1]
          print("Currency1 : ",currency1 )
          try:
            travel = driver1.find_element_by_css_selector("body > div.page-container.ti-over > section:nth-child(6) > div > div:nth-child(11) > div > div > div")
            tag = travel.find_elements_by_tag_name("li")
            validity = tag[0].text.split("on",1)[1]
            dt = parse(validity)
            valid_till = dt.strftime('%Y-%m-%d')
            print("Valid_till :", valid_till)
            travel_date = tag[1].text.split("between",1)[1]
            date = travel_date.split("and")
            startdate = date[0]
            dt = parse(startdate)
            start_date = dt.strftime('%Y-%m-%d')
            print("dep_date_from:", start_date)
            enddate = date[1]
            dt = parse(enddate)
            end_date = dt.strftime('%Y-%m-%d')
            print("dep_date_to:", end_date)
          except:
            travel = driver1.find_element_by_css_selector("body > div.page-container.ti-over > section:nth-child(6) > div > div:nth-child(10) > div > div > div")
            tag = travel.find_elements_by_tag_name("li")
            validity = tag[0].text.split("on",1)[1]
            dt = parse(validity)
            valid_till = dt.strftime('%Y-%m-%d')
            print("Valid_till :", valid_till)
            travel_date = tag[1].text.split("between",1)[1]
            date = travel_date.split("and")
            startdate = date[0]
            dt = parse(startdate)
            start_date = dt.strftime('%Y-%m-%d')
            print("dep_date_from:", start_date)
            enddate = date[1]
            dt = parse(enddate)
            end_date = dt.strftime('%Y-%m-%d')
            print("dep_date_to:", end_date)

          data_doc = dict()
          data_doc = {
            "Airline" : "FZ",
            "OD" : origin[1]+destination,
            "Valid from" : '',
            "Valid till" : valid_till,
            "Compartment" : classes ,
            "Type" : splitting1[0],
            "Fare" : fare1,
            "Currency" : currency1,
            "Minimum Stay" : '',
            "Maximum Stay" : '',
            "dep_date_from" : start_date,
            "dep_date_to" : end_date,
            "Url" : urls
              }
          doc = {
            "Airline" : "FZ",
            "OD" : origin[1]+destination,
            "Valid from" : '',
            "Valid till" : valid_till,
            "Compartment" : classes ,
            "Type" : splitting1[0],
            "Fare" : fare1,
            "Currency" : currency1,
            "Minimum Stay" : '',
            "Maximum Stay" : '',
            "dep_date_from" : start_date,
            "dep_date_to" : end_date,
            "Url" : urls,
            "Last Updated Date" : datetime.datetime.now().strftime ("%Y-%m-%d"),
            "Last Updated Time" : datetime.datetime.now().strftime ("%H")
              }

          conn.JUP_DB_Promotions.update(data_doc, doc, upsert = True)


  @measure(JUPITER_LOGGER)
  def promotions2(url):
    driver1.get(url)
    title = driver.find_element_by_css_selector("body > div.page-container.ti-over > section:nth-child(6) > div > div.offset > div > div > h1")
    if "Bank" in title.text:
      print("No offer")
    else:

      wait.until(EC.visibility_of_element_located((By.CLASS_NAME,("highlight-box"))))
      title = driver1.find_element_by_css_selector("body > div.page-container.ti-over > section:nth-child(6) > div > div:nth-child(3)")
      content = driver1.find_elements_by_class_name("highlight-box")
      #print(title.text)

      for i in range(len(content)):
        OD1 = content[i].text.splitlines()
        print(len(OD1))
        print(OD1)
        if len(OD1)>8:
          origin = title.text.splitlines()
          destination = OD1[1].strip()
          print(origin[1]+destination)

          fare2 = None
          currency2 = None
          classes2 = "J"

          if "Business" in OD1[5]:
            classes2 = "J"
            print(classes2)
            if OD1[6].startswith("One-Way"):
              splitting2 = OD1[6].split(" ")
              print("Type:", splitting2[0])
              fare2 = splitting2[2]
              print("Fare : ",fare2 )
              currency2 = splitting2[1]
              print("Currency : ",currency2)

            if OD1[6].startswith("Return"):
              splitting2 = OD1[6].split(" ")
              print("Type:", splitting2[0])

              fare2 = splitting2[2]
              print("Fare : ",fare2 )
              currency2 = splitting2[1]
              print("Currency : ",currency2)

            try:
              travel = driver1.find_element_by_css_selector("body > div.page-container.ti-over > section:nth-child(6) > div > div:nth-child(11) > div > div > div")
              tag = travel.find_elements_by_tag_name("li")
              validity = tag[0].text.split("on",1)[1]
              dt = parse(validity)
              valid_till = dt.strftime('%Y-%m-%d')
              print("Valid_till :", valid_till)
              travel_date = tag[1].text.split("between",1)[1]
              date = travel_date.split("and")
              startdate = date[0]
              dt = parse(startdate)
              start_date = dt.strftime('%Y-%m-%d')
              print("dep_date_from:", start_date)
              enddate = date[1]
              dt = parse(enddate)
              end_date = dt.strftime('%Y-%m-%d')
              print("dep_date_to:", end_date)
            except:
              travel = driver1.find_element_by_css_selector("body > div.page-container.ti-over > section:nth-child(6) > div > div:nth-child(10) > div > div > div")
              tag = travel.find_elements_by_tag_name("li")

              validity = tag[0].text.split("on",1)[1]
              dt = parse(validity)
              valid_till = dt.strftime('%Y-%m-%d')
              print("Valid_till :", valid_till)
              travel_date = tag[1].text.split("between",1)[1]
              date = travel_date.split("and")
              startdate = date[0]
              dt = parse(startdate)
              start_date = dt.strftime('%Y-%m-%d')
              print("dep_date_from:", start_date)
              enddate = date[1]
              dt = parse(enddate)
              end_date = dt.strftime('%Y-%m-%d')
              print("dep_date_to:", end_date)

            data_doc = dict()
            data_doc = {
              "Airline" : "FZ",
              "OD" : origin[1]+destination,
              "Valid from" : '',
              "Valid till" : valid_till,
              "Compartment" : classes2,
              "Type" : splitting2[0],
              "Fare" : fare2,
              "Currency" : currency2,
              "Minimum Stay" : '',
              "Maximum Stay" : '',
              "dep_date_from" : start_date,
              "dep_date_to" : end_date,
              "Url" : urls
                }
            doc = {
              "Airline" : "FZ",
              "OD" : origin[1]+destination,
              "Valid from" : '',
              "Valid till" : valid_till,
              "Compartment" : classes2,
              "Type" : splitting2[0],
              "Fare" : fare2,
              "Currency" : currency2,
              "Minimum Stay" : '',
              "Maximum Stay" : '',
              "dep_date_from" : start_date,
              "dep_date_to" : end_date,
              "Url" : urls,
              "Last Updated Date" : datetime.datetime.now().strftime ("%Y-%m-%d"),
              "Last Updated Time" : datetime.datetime.now().strftime ("%H")
                }

            conn.JUP_DB_Promotions.update(data_doc,doc, upsert = True)


  @measure(JUPITER_LOGGER)
  def promotions3(url):
    driver1.get(url)
    title = driver.find_element_by_css_selector("body > div.page-container.ti-over > section:nth-child(6) > div > div.offset > div > div > h1")
    if "Bank" in title.text:
      print("No offer")
    else:

      wait.until(EC.visibility_of_element_located((By.CLASS_NAME,("highlight-box"))))
      title = driver1.find_element_by_css_selector("body > div.page-container.ti-over > section:nth-child(6) > div > div:nth-child(3)")
      content = driver1.find_elements_by_class_name("highlight-box")
      #print(title.text)

      for i in range(len(content)):
        OD1 = content[i].text.splitlines()
        print(len(OD1))
        print(OD1)
        if len(OD1)>8:
          origin = title.text.splitlines()
          destination = OD1[1].strip()
          print(origin[1]+destination)

          fare2 = None
          currency2 = None
          classes2 = "J"

          if "Business" in OD1[5]:
            classes2 = "J"
            print(classes2)

            if OD1[7].startswith("Return"):
              splitting2 = OD1[7].split(" ")
              print("Type:", splitting2[0])

              fare2 = splitting2[2]
              print("Fare : ",fare2 )
              currency2 = splitting2[1]
              print("Currency : ",currency2)

              try:
                travel = driver1.find_element_by_css_selector("body > div.page-container.ti-over > section:nth-child(6) > div > div:nth-child(11) > div > div > div")
                tag = travel.find_elements_by_tag_name("li")
                validity = tag[0].text.split("on",1)[1]
                dt = parse(validity)
                valid_till = dt.strftime('%Y-%m-%d')
                print("Valid_till :", valid_till)
                travel_date = tag[1].text.split("between",1)[1]
                date = travel_date.split("and")
                startdate = date[0]
                dt = parse(startdate)
                start_date = dt.strftime('%Y-%m-%d')
                print("dep_date_from:", start_date)
                enddate = date[1]
                dt = parse(enddate)
                end_date = dt.strftime('%Y-%m-%d')
                print("dep_date_to:", end_date)
              except:
                travel = driver1.find_element_by_css_selector("body > div.page-container.ti-over > section:nth-child(6) > div > div:nth-child(10) > div > div > div")
                tag = travel.find_elements_by_tag_name("li")

                validity = tag[0].text.split("on",1)[1]
                dt = parse(validity)
                valid_till = dt.strftime('%Y-%m-%d')
                print("Valid_till :", valid_till)
                travel_date = tag[1].text.split("between",1)[1]
                date = travel_date.split("and")
                startdate = date[0]
                dt = parse(startdate)
                start_date = dt.strftime('%Y-%m-%d')
                print("dep_date_from:", start_date)
                enddate = date[1]
                dt = parse(enddate)
                end_date = dt.strftime('%Y-%m-%d')
                print("dep_date_to:", end_date)

              data_doc = dict()
              data_doc = {
                "Airline" : "FZ",
                "OD" : origin[1]+destination,
                "Valid from" : '',
                "Valid till" : valid_till,
                "Compartment" : classes2,
                "Type" : splitting2[0],
                "Fare" : fare2,
                "Currency" : currency2,
                "Minimum Stay" : '',
                "Maximum Stay" : '',
                "dep_date_from" : start_date,
                "dep_date_to" : end_date,
                "Url" : urls
                  }
              doc = {
                "Airline" : "FZ",
                "OD" : origin[1]+destination,
                "Valid from" : '',
                "Valid till" : valid_till,
                "Compartment" : classes2,
                "Type" : splitting2[0],
                "Fare" : fare2,
                "Currency" : currency2,
                "Minimum Stay" : '',
                "Maximum Stay" : '',
                "dep_date_from" : start_date,
                "dep_date_to" : end_date,
                "Url" : urls,
                "Last Updated Date" : datetime.datetime.now().strftime ("%Y-%m-%d"),
                "Last Updated Time" : datetime.datetime.now().strftime ("%H")
                  }

              conn.JUP_DB_Promotions.update(data_doc,doc, upsert = True)


  @measure(JUPITER_LOGGER)
  def promotions4(url):
    driver1.get(url)
    title = driver.find_element_by_css_selector("body > div.page-container.ti-over > section:nth-child(6) > div > div.offset > div > div > h1")
    if "Bank" in title.text:
      print("No offer")
    else:

      wait.until(EC.visibility_of_element_located((By.CLASS_NAME,("highlight-box"))))
      title = driver1.find_element_by_css_selector("body > div.page-container.ti-over > section:nth-child(6) > div > div:nth-child(3)")
      content = driver1.find_elements_by_class_name("highlight-box")
      #print(title.text)

      for i in range(len(content)):
        OD1 = content[i].text.splitlines()
        print(len(OD1))
        print(OD1)
        if len(OD1)>8:
          origin = title.text.splitlines()
          destination = OD1[1].strip()
          print(origin[1]+destination)

          fare3 = None
          currency3 = None
          classes3 = "J"

          if "Business" in OD1[6]:
            classes3 = "J"
            print(classes3)

            if OD1[7].startswith("One-Way"):
              splitting3 = OD1[7].split(" ")
              print("Type:", splitting3[0])

              fare3 = splitting3[2]
              print("Fare : ",fare3 )
              currency3 = splitting3[1]
              print("Currency : ",currency3)


            if OD1[7].startswith("Return"):
              splitting3 = OD1[7].split(" ")
              print("Type:", splitting3[0])

              fare3 = splitting3[2]
              print("Fare : ",fare3 )
              currency3 = splitting3[1]
              print("Currency : ",currency3)

            try:
              travel = driver1.find_element_by_css_selector("body > div.page-container.ti-over > section:nth-child(6) > div > div:nth-child(11) > div > div > div")
              tag = travel.find_elements_by_tag_name("li")
              validity = tag[0].text.split("on",1)[1]
              dt = parse(validity)
              valid_till = dt.strftime('%Y-%m-%d')
              print("Valid_till :", valid_till)
              travel_date = tag[1].text.split("between",1)[1]
              date = travel_date.split("and")
              startdate = date[0]
              dt = parse(startdate)
              start_date = dt.strftime('%Y-%m-%d')
              print("dep_date_from:", start_date)
              enddate = date[1]
              dt = parse(enddate)
              end_date = dt.strftime('%Y-%m-%d')
              print("dep_date_to:", end_date)
            except:
              travel = driver1.find_element_by_css_selector("body > div.page-container.ti-over > section:nth-child(6) > div > div:nth-child(10) > div > div > div")
              tag = travel.find_elements_by_tag_name("li")

              validity = tag[0].text.split("on",1)[1]
              dt = parse(validity)
              valid_till = dt.strftime('%Y-%m-%d')
              print("Valid_till :", valid_till)
              travel_date = tag[1].text.split("between",1)[1]
              date = travel_date.split("and")
              startdate = date[0]
              dt = parse(startdate)
              start_date = dt.strftime('%Y-%m-%d')
              print("dep_date_from:", start_date)
              enddate = date[1]
              dt = parse(enddate)
              end_date = dt.strftime('%Y-%m-%d')
              print("dep_date_to:", end_date)

            data_doc = dict()
            data_doc = {
              "Airline" : "FZ",
              "OD" : origin[1]+destination,
              "Valid from" : '',
              "Valid till" : valid_till,
              "Compartment" : classes3,
              "Type" : splitting3[0],
              "Fare" : fare3,
              "Currency" : currency3,
              "Minimum Stay" : '',
              "Maximum Stay" : '',
              "dep_date_from" : start_date,
              "dep_date_to" : end_date,
              "Url" : urls
                }
            doc = {
              "Airline" : "FZ",
              "OD" : origin[1]+destination,
              "Valid from" : '',
              "Valid till" : valid_till,
              "Compartment" : classes3,
              "Type" : splitting3[0],
              "Fare" : fare3,
              "Currency" : currency3,
              "Minimum Stay" : '',
              "Maximum Stay" : '',
              "dep_date_from" : start_date,
              "dep_date_to" : end_date,
              "Url" : urls,
              "Last Updated Date" : datetime.datetime.now().strftime ("%Y-%m-%d"),
              "Last Updated Time" : datetime.datetime.now().strftime ("%H")
                }

            conn.JUP_DB_Promotions.update(data_doc, doc, upsert = True)



  @measure(JUPITER_LOGGER)
  def promotions5(url):
    driver1.get(url)
    title = driver.find_element_by_css_selector("body > div.page-container.ti-over > section:nth-child(6) > div > div.offset > div > div > h1")
    if "Bank" in title.text:
      print("No offer")
    else:

      wait.until(EC.visibility_of_element_located((By.CLASS_NAME,("highlight-box"))))
      title = driver1.find_element_by_css_selector("body > div.page-container.ti-over > section:nth-child(6) > div > div:nth-child(3)")
      content = driver1.find_elements_by_class_name("highlight-box")
      #print(title.text)

      for i in range(len(content)):
        OD1 = content[i].text.splitlines()
        print(len(OD1))
        print(OD1)
        if len(OD1)>8:
          origin = title.text.splitlines()
          destination = OD1[1].strip()
          print(origin[1]+destination)

          fare3 = None
          currency3 = None
          classes3 = "J"

          if "Business" in OD1[6]:
            classes3 = "J"
            print(classes3)

            if OD1[8].startswith("Return"):
              splitting3 = OD1[8].split(" ")
              print("Type:", splitting3[0])

              fare3 = splitting3[2]
              print("Fare : ",fare3 )
              currency3 = splitting3[1]
              print("Currency : ",currency3)

              try:
                travel = driver1.find_element_by_css_selector("body > div.page-container.ti-over > section:nth-child(6) > div > div:nth-child(11) > div > div > div")
                tag = travel.find_elements_by_tag_name("li")
                validity = tag[0].text.split("on",1)[1]
                dt = parse(validity)
                valid_till = dt.strftime('%Y-%m-%d')
                print("Valid_till :", valid_till)
                travel_date = tag[1].text.split("between",1)[1]
                date = travel_date.split("and")
                startdate = date[0]
                dt = parse(startdate)
                start_date = dt.strftime('%Y-%m-%d')
                print("dep_date_from:", start_date)
                enddate = date[1]
                dt = parse(enddate)
                end_date = dt.strftime('%Y-%m-%d')
                print("dep_date_to:", end_date)
              except:
                travel = driver1.find_element_by_css_selector("body > div.page-container.ti-over > section:nth-child(6) > div > div:nth-child(10) > div > div > div")
                tag = travel.find_elements_by_tag_name("li")

                validity = tag[0].text.split("on",1)[1]
                dt = parse(validity)
                valid_till = dt.strftime('%Y-%m-%d')
                print("Valid_till :", valid_till)
                travel_date = tag[1].text.split("between",1)[1]
                date = travel_date.split("and")
                startdate = date[0]
                dt = parse(startdate)
                start_date = dt.strftime('%Y-%m-%d')
                print("dep_date_from:", start_date)
                enddate = date[1]
                dt = parse(enddate)
                end_date = dt.strftime('%Y-%m-%d')
                print("dep_date_to:", end_date)

              data_doc = dict()
              data_doc = {
                "Airline" : "FZ",
                "OD" : origin[1]+destination,
                "Valid from" : '',
                "Valid till" : valid_till,
                "Compartment" : classes3,
                "Type" : splitting3[0],
                "Fare" : fare3,
                "Currency" : currency3,
                "Minimum Stay" : '',
                "Maximum Stay" : '',
                "dep_date_from" : start_date,
                "dep_date_to" : end_date,
                "Url" : urls
                  }
              doc = {
                "Airline" : "FZ",
                "OD" : origin[1]+destination,
                "Valid from" : '',
                "Valid till" : valid_till,
                "Compartment" : classes3,
                "Type" : splitting3[0],
                "Fare" : fare3,
                "Currency" : currency3,
                "Minimum Stay" : '',
                "Maximum Stay" : '',
                "dep_date_from" : start_date,
                "dep_date_to" : end_date,
                "Url" : urls,
                "Last Updated Date" : datetime.datetime.now().strftime ("%Y-%m-%d"),
                "Last Updated Time" : datetime.datetime.now().strftime ("%H")
                  }

              conn.JUP_DB_Promotions.update(data_doc, doc, upsert = True)



  promotion_table = driver.find_elements_by_xpath("//*[@id='global-offers']/div")
  for lists in promotion_table:
    classes = lists.find_elements_by_class_name("caption")
    for each_class in classes:
      url = each_class.find_elements_by_tag_name("a")
      for each_url in url:
        urls = each_url.get_attribute("href")
        print(urls)
        promotions(urls)
        promotions1(urls)
        promotions2(urls)
        promotions3(urls)
        promotions4(urls)
        promotions5(urls)







if __name__ == "__main__":
    run()