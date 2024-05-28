import sys

reload(sys)
sys.setdefaultencoding('utf8')
# from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoRuleChangeTrigger
# from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoDateChangeTrigger
# from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoFareChangeTrigger
# from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoNewPromotionTrigger
from selenium import webdriver
# from jupiter_AI.network_level_params import SYSTEM_DATE,SYSTEM_TIME
import pymongo
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from requests.exceptions import ConnectionError
from pymongo import UpdateOne
import time
import traceback
from dateutil.parser import parse
import datetime
import re
from bs4 import BeautifulSoup
import requests

client = pymongo.MongoClient("mongodb://analytics:KNjSZmiaNUGLmS0Bv2@13.92.251.7:42525/")
db = client['fzDB_stg']

# Connecting to Mongodb
# from jupiter_AI import JUPITER_DB, client
# db = client[JUPITER_DB]
chrome_path = r"/home/prathyusha/Downloads/chromedriver"
driver = webdriver.Chrome(chrome_path)
driver.wait = WebDriverWait(driver, 5)

country = [
    "eg" ,"sd", "tz"]#, "ug", "bd", "cn", "np", "pk", "th", "at",  "ro", "ru", "tr", "rs", "bh", "ir", "jo", "kw", "ae", "lb", "om", "qa", "sa"]
t = 0
bulk_update_doc = []
# check sd and ir which doesn't have proms
#
# count = 0
# url = "http://www.etihad.com/en-eg/deals/promotions-main/"
# print url
# driver.get("http://www.etihad.com/en-eg/deals/promotions-main/")
# time.sleep(5)
for each_country in country:
    try:
        count = 0
        url = "http://www.etihad.com/en-" + each_country + "/deals/promotions-main/"
        print url
        driver.get("http://www.etihad.com/en-" + each_country + "/deals/promotions-main/")
        time.sleep(5)
    # driver.wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="promos"]/article/div/div[4]/a')))
        driver.refresh()
        driver.execute_script("window.scrollTo(0,500);")
        time.sleep(5)
        # print "It has view more button"
        more = driver.find_element_by_xpath('//*[@id="promos"]/article/div/div[4]/a')
        more.click()
        # time.sleep(5)
        # table = driver.find_element_by_xpath('//*[@id="fares"]/div/article/div/div[5]')
        # offer = table.find_elements_by_tag_name("h3")

        import bs4
        from bs4 import BeautifulSoup
        import requests

        url = "http://www.etihad.com/en-" + each_country + "/deals/promotions-main/"
        requests.get(url)
        response = requests.get(url)
        source = driver.page_source
        soup = BeautifulSoup(source, "lxml")
        valid_date_data = soup.find_all("p", {"class": "clearBoth fontSize1"})
        tables_list = []
        for i in range(len(valid_date_data)):
            if "valid only" in str(valid_date_data[i].text) or "Book before" in str(valid_date_data[i].text) or "Book online before" in str(valid_date_data[i].text) or "Book by" in str(valid_date_data[i].text) or "Book until" in str(
                    valid_date_data[i].text):
                print "-----", valid_date_data[i]
                table_no = i
                tables_list.append(i)
            else:
                pass
        print "table no.: ", tables_list
        more = driver.find_element_by_xpath('//*[@id="promos"]/article/div/div[4]/a')
        more.click()

                # // *[ @ id = "promos"] / article / div / div[4] / a
        # // *[ @ id = "promos"] / div / article / div / a
                # //*[@id="promos"]/div/article/div/a
                # print valid_date_data[i]
        # table_no \
        # = -999

        tables = soup.find_all("ul", {"class": "priceList"})

        print len(tables)
        # print len(li)
        for i in tables_list:
            print "-----------",i
            count_proms = tables[i].find_all('li')
        print "//////", count_proms
        print len(tables[0].find_all('li'))
        for i in range(len(count_proms)):
            context = tables[i].find_all('li')[i].text
        # print context

            print context.splitlines()

            origin = context.splitlines()[3].split(" - ")[0]
            destination = context.splitlines()[3].split(" - ")[1]

            # destination
            context.splitlines()[5].split(" ")

            class1 = context.splitlines()[4].split(" - ")[1]
            if class1 == "Economy":
                classes = "Y"
            else:
                classes = "J"
            print(classes)
            fares = context.splitlines()[5].split(" ")[2]
            Currency = context.splitlines()[5].split(" ")[1]
            print("Fare : ", fares)
            print("Currency :", Currency)
        # more= driver.find_element_by_xpath('//*[@id="2419199"]/span[2]')
        # more.click()
        # time.sleep(5)
        # valdity = driver.find_element_by_xpath('//*[@id="contentJump"]/div[2]/div[1]/div[2]/article/div/div[4]/div')
        # break
    except:
        pass
