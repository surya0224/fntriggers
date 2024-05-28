#pending.. debug n sort it out!
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
  import datetime


  #Connecting to Local Mongodb
  # import pymongo
  # client = pymongo.MongoClient("mongodb://analytics:KNjSZmiaNUGLmS0Bv2@13.92.251.7:42525/")
  # db = client['fzDB']

  #Defining the Chrome Path
  chrome_path = r"/home/prathyusha/Downloads/chromedriver"
  driver = webdriver.Chrome(chrome_path)
  driver1 = webdriver.Chrome(chrome_path)
  wait = WebDriverWait(driver, 50)
  print "--1done"
  wait = WebDriverWait(driver1, 70)

  #Function to scrape promotions


  @measure(JUPITER_LOGGER)
  def promotions(url):
    driver1.get(url)
    print url
    #no_offer = driver1.find_element_by_xpath("//*[@id='noPromotionAvailableError']")
    try:
      print "--2done"
      wait.until(EC.visibility_of_element_located((By.XPATH, "//*[@id='main-inner']/div[3]/div[1]/figure/div")))
      offer = driver1.find_element_by_xpath("//*[@id='main-inner']/div[2]")
      ODs = offer.find_element_by_tag_name("h2")
      OD = ODs.text.split(" ")
      origin = OD[4].strip("()")
      destination = OD[-1].strip("()")
      print(origin+destination)

      offer2 = driver1.find_element_by_xpath("//*[@id='main-inner']/div[3]/div[1]/figure/div/div[2]")
      cls = offer2.find_elements_by_tag_name("h3")
      for each_OD in cls:
        res = each_OD.text.splitlines()
        classes = "Y"
        if "Economy" in res[1]:
          classes = "Y"
          print(classes)
        if "Business" in res[1]:
          classes = "J"
          print(classes)

      fares = driver1.find_element_by_xpath("//*[@id='main-inner']/div[3]/div[1]/figure/div/div[3]")
      fare = fares.find_element_by_tag_name("h3")
      fares = fare.text.split(" ")
      print("Currency : ", fares[1][:3])
      print("Fare : ", fares[1][3:-1])
      contents = driver1.find_element_by_xpath("//*[@id='main-inner']/div[3]/div[2]/div/div[1]")
      content = contents.find_elements_by_tag_name("dd")
      validity = content[0].text.split('to')
      dt = parse(validity[0])
      valid_from = dt.strftime('%Y-%m-%d')
      print("Valid from : ", valid_from)
      dt = parse(validity[1])
      valid_till = dt.strftime('%Y-%m-%d')
      print("Valid till : ", valid_till)
      print("Minimum Stay:", content[3].text)
      print("Maximum Stay:", content[4].text)
      travel = content[5].text.split('to')
      dt = parse(travel[0])
      startdate = dt.strftime('%Y-%m-%d')
      print("dep_date_from:", startdate)
      dt = parse(travel[1])
      enddate = dt.strftime('%Y-%m-%d')
      print("dep_date_to:", enddate)


  #Storing the parameters in a dictionary
      data_doc = dict()

      data_doc = {
        "Airline" : "SQ",
        "OD" : origin+destination,
        "Valid from" : valid_from,
        "Valid till" : valid_till,
        "Compartment" : classes,
        "Fare" : fares[1][3:-1],
        "Currency" : fares[1][:3],
        "Minimum Stay" : content[3].text,
        "Maximum Stay" : content[4].text,
        "dep_date_from" : startdate,
        "dep_date_to" : enddate,
        "Url" : urls

        }
      doc = {
        "Airline" : "SQ",
        "OD" : origin+destination,
        "Valid from" : valid_from,
        "Valid till" : valid_till,
        "Compartment" : classes,
        "Fare" : fares[1][3:-1],
        "Currency" : fares[1][:3],
        "Minimum Stay" : content[3].text,
        "Maximum Stay" : content[4].text,
        "dep_date_from" : startdate,
        "dep_date_to" : enddate,
        "Url" : urls,
        "Last Updated Date" : datetime.datetime.now().strftime ("%Y-%m-%d"),
        "Last Updated Time" : datetime.datetime.now().strftime ("%H")
        }
      # db.JUP_DB_Promotions.update(data_doc,doc, upsert = True)
      print "yes"
    except Exception as e:
      print(e)





  country = ["bd", "in", "sa", "th","tr", "ae", "vn" ]
  city = [["Dhaka"], ["Mumbai","Delhi", "Chennai", "Kolkata", "Bengaluru", "Ahmedabad"],["Jeddah"], ["Bangkok","Phuket", "Ko-Samui", "Chiang-Mai"], ["Istanbul"], ["Dubai"], ["Ho-Chin-Minh-City", "Hanoi"]]
  print city
  for i in range(len(country)):
    for j in city[i]:
      print "Country:", j
      driver.get("http://www.singaporeair.com/en_UK/"+country[i]+"/special-offers/flight-from-"+j+"/")
      try:
        see_more = driver.find_element_by_class_name("promotion-btn").click()
      except:
        pass
      try:
        see_more2 = driver.find_element_by_xpath("//*[@id='main-inner']/div[7]").click()
      except:
        pass
      try:
        see_more3 = driver.find_element_by_xpath("//*[@id='main-inner']/div[7]").click()
      except:
        pass
      try:
        see_more4 = driver.find_element_by_xpath("//*[@id='main-inner']/div[7]").click()
      except:
        pass
      try:
        see_more5 = driver.find_element_by_xpath("//*[@id='main-inner']/div[7]").click()
      except:
        pass
      try:
        see_more6 = driver.find_element_by_xpath("//*[@id='main-inner']/div[7]").click()
      except:
        pass
      try:
        see_more7 = driver.find_element_by_xpath("//*[@id='main-inner']/div[7]").click()
      except:
        pass
      try:
        see_more8 = driver.find_element_by_xpath("//*[@id='main-inner']/div[7]").click()
      except:
        pass
      try:
        see_more9 = driver.find_element_by_xpath("//*[@id='main-inner']/div[7]").click()
      except:
        pass
      try:
        see_more10 = driver.find_element_by_xpath("//*[@id='main-inner']/div[7]").click()
      except:
        pass


      promotion_table = driver.find_elements_by_xpath("//*[@id='main-inner']/div[5]")
      for lists in promotion_table:
        urls = lists.find_elements_by_class_name("promotion-item__content")
        for eachURL in urls:
          url = eachURL.find_elements_by_tag_name("a")
          for Url_list in url:
            urls = Url_list.get_attribute("href")
            print(urls)
            #Calling the function
            promotions(urls)

  driver.close()
if __name__ == "__main__":
    run()