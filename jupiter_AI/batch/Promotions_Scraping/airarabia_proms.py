import time
import traceback

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, NoSuchElementException, ElementNotVisibleException
from selenium.webdriver.support.ui import Select
from dateutil.parser import parse
import re
from pymongo import UpdateOne
from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoRuleChangeTrigger
from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoDateChangeTrigger
from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoFareChangeTrigger
from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoNewPromotionTrigger
import pandas as pd
import datetime
from jupiter_AI import JUPITER_DB, client, JUPITER_LOGGER
from jupiter_AI.logutils import measure


Start_Date = datetime.datetime.now()
parse(str(Start_Date)).strftime('%Y-%m-%d')

import pymongo
db = client['fzDB_stg']

chrome_path = r"/home/prathyusha/Downloads/chromedriver"
driver = webdriver.Chrome(chrome_path)
driver.wait = WebDriverWait(driver, 5)
#Opening Website
url_1 = "http://www.airarabia.com/en/best-offers"
driver.get(url_1)
# driver.refresh()
print url_1
time.sleep(5)
#Fixing Currency to "AED"
try:
    driver.find_element_by_xpath('/html/body/div[6]/div/div[3]/div[1]/div[1]').click()
except:
    pass
user_curr = driver.find_element_by_xpath('/html/body/header/div/div/div/div[2]/span/div/div[2]/ul')
driver.find_element_by_xpath('/html/body/header/div[1]/div/div[1]/div[2]/span/span').click()
user_curr_opt = user_curr.find_elements_by_tag_name('li')
for eachuser_curr in user_curr_opt:
    if eachuser_curr.text == 'AED':
        eachuser_curr.click()
        print "done"
        break
    else:
        print "not found AED"
        pass
cursor_OD = list(db.JUP_DB_OD_Master.aggregate([{'$project': {'_id': 0, 'OD': 1}}]))
city_airport = list(db.JUP_DB_City_Airport_Mapping.aggregate([{'$project': {'_id': 0, 'City_Code': 1, 'Airport_Code': 1}}]))

# driver.refresh()
driver.execute_script("window.scrollTo(0,300);")
time.sleep(5)
#Collecting List of Origins
form = driver.find_element_by_xpath('//*[@id="flight_offers_search"]/div[1]/div/div[2]/input[2]')
form.click()
driver.wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="flight_offers_search"]/div[1]/div/div[2]/div/div[2]')))
cols = []
for i in range(1,4):
    count = 0
    lists = driver.find_element_by_xpath('//*[@id="flight_offers_search"]/div[1]/div/div[2]/div/div[2]/ul[' + str(i) + ']')
    names = lists.find_elements_by_tag_name("li")
    for each in names:
        count += 1
    print "no. of origins:" , count
    temp = []
    for r in range(count):
        names1 = driver.find_element_by_xpath('//*[@id="flight_offers_search"]/div[1]/div/div[2]/div/div[2]/ul[' + str(i) + ']/li[' + str(r+1) + ']/a').text
        temp.append(names1)
    cols.append(temp)
#     print "column_list_ori: ", temp
print "cols: ", cols
df = pd.DataFrame(cols)

page_html = driver.page_source
from bs4 import BeautifulSoup
soup = BeautifulSoup(page_html,"html.parser")

import requests
response = requests.get('http://www.airarabia.com/en/best-offers')

# driver.refresh()
count3 = 0
t=0
bulk_update_doc = []
ori_countries_done =[]
time.sleep(5)
airport_counter = 1

ap_index = 0
dest_ap_index = 0
for column_ind in range(len(cols)):
    for country_ind in range(len(cols[column_ind])):
        dest_ap_index = 0
        print "Country_ori: ", cols[column_ind][country_ind]
        no_of_ap = len(soup.find_all('li', {"data-country": cols[column_ind][country_ind]}))
        print "Airports_ori: ", no_of_ap
        for ap in range(no_of_ap):
            print "ap no. : ", ap
            # click on the from- ori country- airport
            driver.find_element_by_xpath('//*[@id="flight_offers_search"]/div[1]/div/div[2]/input[2]').click()
            if cols[column_ind][country_ind] not in ori_countries_done:
                # click on the country name
                time.sleep(5)
                print "column_ind: ", column_ind
                print "country_ind: ", country_ind  # //*[@id="flight_offers_search"]/div[1]/div/div[2]/div/div[2]/ul[2]/li[1]/a
                # clicking country
                ap_index += 1
                print ap_index
                driver.find_element_by_xpath('//*[@id="flight_offers_search"]/div[1]/div/div[2]/input[2]').click()
                time.sleep(5)  # //*[@id="flight_offers_search"]/div[1]/div/div[2]/div/div[2]/ul[1]/li[2]/a
                driver.find_element_by_xpath('//*[@id="flight_offers_search"]/div[1]/div/div[2]/div/div[2]/ul[' + str(
                    column_ind + 1) + ']/li[' + str(country_ind + 1) + ']/a').click()
                time.sleep(10)
                ori_ap = driver.find_element_by_xpath(
                    '//*[@id="flight_offers_search"]/div[1]/div/div[2]/div/div[3]/ul/li[' + str(
                        ap_index) + ']/a/span[1]').text
                print "Origin Airport:", ori_ap
                driver.find_element_by_xpath(
                    '//*[@id="flight_offers_search"]/div[1]/div/div[2]/div/div[3]/ul/li[' + str(
                        ap_index) + ']/a').click()
                time.sleep(5)
                print "clicked on origin country- airport"
                time.sleep(10)  # //*[@id="flight_offers_search"]/div[1]/div/div[2]/div/div[3]/ul/li[46]/a/span[1]
                #                            //*[@id="flight_offers_search"]/div[2]/div/div[2]/input[2]
                #                 driver.find_element_by_xpath('//*[@id="flight_offers_search"]/div[1]/div/div[2]/div/div[3]/ul[1]/li['+str(airport_counter)+']/a/span[1]').click()
                airport_counter += 1
                airport_counter_dest = 1
                # making e_cols
                e_form = driver.find_element_by_xpath('//*[@id="flight_offers_search"]/div[2]/div/div[2]/input[2]')
                e_form.click()
                driver.wait.until(EC.visibility_of_element_located(
                    (By.XPATH, '//*[@id="flight_offers_search"]/div[2]/div/div[2]/div/div[2]')))
                e_cols = []
                # t_count = 0
                try:
                    for e_i in range(1, 3):
                        e_count = 0
                        e_lists = driver.find_element_by_xpath(
                            '//*[@id="flight_offers_search"]/div[2]/div/div[2]/div/div[2]/ul[' + str(e_i) + ']')
                        e_names = e_lists.find_elements_by_tag_name("li")
                        for e_each in e_names:
                            e_count += 1
                        print "no. of destinations:", e_count
                        e_temp = []
                        time.sleep(10)
                        for e_r in range(e_count):
                            # print e_r
                            # print e_count
                            e_names1 = driver.find_element_by_xpath(
                                '//*[@id="flight_offers_search"]/div[2]/div/div[2]/div/div[2]/ul[' + str(
                                    e_i) + ']/li[' + str(e_r + 1) + ']/a').text
                            e_temp.append(
                                e_names1)  # //*[@id="flight_offers_search"]     /div[2]/div/div[2]/div/div[2]/ul[1]/li[1]/a
                        e_cols.append(e_temp)
                        #                         print e_temp
                        #     t_count += 1
                        #     print t_count
                except:
                    pass
                print e_cols
                time.sleep(5)
                e_df = pd.DataFrame(e_cols)
                for dest_column_ind in range(len(e_cols)):
                    for dest_country_ind in range(len(e_cols[dest_column_ind])):
                        print "dest_column_ind: ", dest_column_ind
                        print "dest_country_ind: ", dest_country_ind
                        no_of_ap_dest = len(
                            soup.find_all('li', {"data-country": e_cols[dest_column_ind][dest_country_ind]}))
                        if cols[column_ind][country_ind] != e_cols[dest_column_ind][dest_country_ind]:
                            print "Country_dest: ", e_cols[dest_column_ind][dest_country_ind]
                            print "Airports_dest: ", no_of_ap_dest
                            if no_of_ap_dest != 0:
                                if e_cols[dest_column_ind][dest_country_ind] == "Pakistan":
                                    no_of_ap_dest = 6
                                    print "pakistan---"
                                if e_cols[dest_column_ind][dest_country_ind] == "United Arab Emirates":
                                    no_of_ap_dest = 5
                                    print "Airports_dest: ", no_of_ap_dest

                                    #                                     dest_country_ind = dest_country_ind+1
                                for dest_ap in range(no_of_ap_dest):
                                    print "dest_ap no.: ", dest_ap
                                    driver.execute_script("window.scrollTo(0, 300)")
                                    # clicking on to
                                    driver.find_element_by_xpath(
                                        '//*[@id="flight_offers_search"]/div[2]/div/div[2]/input[2]').click()
                                    time.sleep(5)
                                    # clicking on dest country
                                    driver.find_element_by_xpath('//*[@id="flight_offers_search"]/div[2]/div/div[2]/div/div[2]/ul[' + str(dest_column_ind + 1) + ']' + '/li[' + str(dest_country_ind + 1) + ']/a').click()
                                    #  //*[@id="flight_offers_search"]/div[2]/div/div[2]/div/div[3]/ul/li[1]/a/span[1]
                                    #  //*[@id="flight_offers_search"]/div[2]/div/div[2]/div/div[3]/ul/li[2]/a/span[1]
                                    # //*[@id="flight_offers_search"]/div[2]/div/div[2]/div/div[3]/ul/li[3]/a/span[1]
                                    dest_ap_index += 1
                                    print "dest_ap_index", dest_ap_index
                                    time.sleep(10)
                                    print "Destination Airport: ", driver.find_element_by_xpath(
                                        '//*[@id="flight_offers_search"]/div[2]/div/div[2]/div/div[3]/ul/li[' + str(dest_ap_index) + ']/a/span[1]').text
                                    time.sleep(5)
                                    x_path_ap = '//*[@id="flight_offers_search"]/div[2]/div/div[2]/div/div[3]/ul/li[' + str(dest_ap_index) + ']/a/span[1]'
                                    print "ap counter:", airport_counter_dest

                                    driver.find_element_by_xpath(x_path_ap).click()
                                    # click search
                                    time.sleep(10)
                                    driver.find_element_by_xpath('//*[@id="flight_offers_search"]/div[4]/input').click()
                                    # click list view //*[@id="block-airarabia-general-search-offers"]/div/div[1]/div[2]/div/div/div/div[2]/a
                                    time.sleep(10)
                                    try:
                                        driver.find_element_by_xpath(
                                            '//*[@id="block-airarabia-general-search-offers"]/div/div[1]/div[2]/div/div/div/div[2]/a').click()
                                        time.sleep(5)
                                        table = driver.find_element_by_xpath(
                                            '//*[@id="block-airarabia-general-search-offers"]/div/div[2]/div/div[3]/div[1]/div')
                                        # time.sleep(5)
                                        time.sleep(10)
                                        count = 0
                                        # driver.refresh()
                                        source = driver.page_source
                                        soup1 = BeautifulSoup(source, 'lxml')

                                        temp = soup1.find_all("div", {"itemprop": "offers"})
                                        count = len(temp)
                                        count = count - 1
                                        print "count:", count
                                        try:  # clicking on show more
                                            driver.execute_script(
                                                "window.scrollTo(0, 700)")  # //*[@id="block-airarabia-general-search-offers"]/div/div[2]/div/div[3]/div[1]/div/div[10]/a
                                            driver.find_element_by_xpath(
                                                '//*[@id="block-airarabia-general-search-offers"]/div/div[2]/div/div[3]/div[1]/div/div[' + str(count + 1) + ']/a').click()
                                        except:
                                            driver.execute_script("window.scrollTo(0, 400)")
                                        pass
                                    except:
                                        print "in except----------------"
                                        driver.refresh()
                                        driver.find_element_by_xpath(
                                            '//*[@id="flight_offers_search"]/div[1]/div/div[2]/input[2]').click()
                                        time.sleep(5)
                                        driver.find_element_by_xpath(
                                            '//*[@id="flight_offers_search"]/div[1]/div/div[2]/div/div[2]/ul[' + str(column_ind + 1) + ']/li[' + str(country_ind + 1) + ']/a').click()
                                        time.sleep(10)
                                        driver.find_element_by_xpath(
                                            '//*[@id="flight_offers_search"]/div[1]/div/div[2]/div/div[3]/ul/li[' + str(ap_index) + ']/a').click()
                                        time.sleep(5)
                                        driver.find_element_by_xpath(
                                            '//*[@id="flight_offers_search"]/div[2]/div/div[2]/input[2]').click()
                                        time.sleep(5)
                                        # clicking on dest country
                                        driver.find_element_by_xpath(
                                            '//*[@id="flight_offers_search"]/div[2]/div/div[2]/div/div[2]/ul[' + str(dest_column_ind + 1) + ']' + '/li[' + str(dest_country_ind + 1) + ']/a').click()
                                        time.sleep(5)
                                        driver.find_element_by_xpath(
                                            '//*[@id="flight_offers_search"]/div[2]/div/div[2]/div/div[3]/ul/li[' + str(dest_ap_index) + ']/a/span[1]').click()
                                        time.sleep(5)
                                        driver.find_element_by_xpath(
                                            '//*[@id="flight_offers_search"]/div[4]/input').click()
                                        # click list view //*[@id="block-airarabia-general-search-offers"]/div/div[1]/div[2]/div/div/div/div[2]/a
                                        time.sleep(10)
                                        driver.find_element_by_xpath(
                                            '//*[@id="block-airarabia-general-search-offers"]/div/div[1]/div[2]/div/div/div/div[2]/a').click()
                                        time.sleep(10)
                                        count = 0
                                        # driver.refresh()
                                        source = driver.page_source
                                        soup1 = BeautifulSoup(source, 'lxml')
                                        temp = soup1.find_all("div", {"itemprop": "offers"})
                                        count = len(temp)
                                        count = count - 1
                                        print "count:", count
                                        try:  # clicking on show more
                                            driver.execute_script(
                                                "window.scrollTo(0, 700)")  # //*[@id="block-airarabia-general-search-offers"]/div/div[2]/div/div[3]/div[1]/div/div[10]/a
                                            driver.find_element_by_xpath(
                                                '//*[@id="block-airarabia-general-search-offers"]/div/div[2]/div/div[3]/div[1]/div/div[' + str(count + 1) + ']/a').click()
                                        except:
                                            driver.execute_script("window.scrollTo(0, 400)")
                                        pass
                                        # driver.wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="block-airarabia-general-search-offers"]/div/div[2]/div/div[3]/div[1]/div')))
                                        time.sleep(5)
                                        table = driver.find_element_by_xpath(
                                            '//*[@id="block-airarabia-general-search-offers"]/div/div[2]/div/div[3]/div[1]/div')
                                        time.sleep(5)

                                    table = driver.find_element_by_xpath(
                                        '//*[@id="block-airarabia-general-search-offers"]/div/div[2]/div/div[3]/div[1]/div')
                                    time.sleep(5)
                                    try:  # clicking on show more
                                        driver.execute_script(
                                            "window.scrollTo(0, 700)")  # //*[@id="block-airarabia-general-search-offers"]/div/div[2]/div/div[3]/div[1]/div/div[10]/a
                                        driver.find_element_by_xpath(
                                            '//*[@id="block-airarabia-general-search-offers"]/div/div[2]/div/div[3]/div[1]/div/div[7]/a').click()
                                    except:
                                        driver.execute_script("window.scrollTo(0, 400)")
                                        pass
                                    time.sleep(10)
                                    count = 0
                                    # driver.refresh()
                                    source = driver.page_source
                                    soup1 = BeautifulSoup(source, 'lxml')

                                    temp = soup1.find_all("div", {"itemprop": "offers"})
                                    count = len(temp)
                                    count = count - 1
                                    print "count:", count
                                    try: #clicking on show more
                                        driver.execute_script("window.scrollTo(0, 700)")  # //*[@id="block-airarabia-general-search-offers"]/div/div[2]/div/div[3]/div[1]/div/div[10]/a
                                        driver.find_element_by_xpath('//*[@id="block-airarabia-general-search-offers"]/div/div[2]/div/div[3]/div[1]/div/div[' + str(count+1) + ']/a').click()
                                    except:
                                      driver.execute_script("window.scrollTo(0, 400)")
                                    pass
                                if count > 0:
                                    rows = table.find_elements_by_tag_name("div")
                                    for each in rows:
                                        content = each.text.splitlines()
                                        if len(content) == 5:
                                            print "content:", content
                                            origin = content[0].split()[0]
                                            destination = content[0].split()[3]
                                            print "origin:", origin
                                            print "dest:", destination
                                            try:
                                                cursor = db.JUP_DB_IATA_Codes.find()
                                                for i in cursor:
                                                    if all(word in i['City'].lower() for word in
                                                           re.findall(r'\w+', content[0].split()[0].lower())):
                                                        origin = i["Code"]
                                                        # print("Origin :",origin)
                                                    if all(word in i['City'].lower() for word in
                                                           re.findall(r'\w+', content[0].split()[3].lower())):
                                                        destination = i["Code"]
                                                        # print("Destination :",destination)
                                                print "OD:", origin + destination
                                            except:
                                                pass
                                            dt = parse(content[1])
                                            Start_Date = datetime.datetime.now()
                                            #                                     Valid_from = parse(str(Start_Date)).strftime('%Y-%m-%d')
                                            Valid_till = dt.strftime('%Y-%m-%d')
                                            print("Valid till :", Valid_till)
                                            travel = content[3].split("-")
                                            Start_date = dt.strftime('%Y-%m-%d')
                                            print("dep_date_from :", Start_date)
                                            End_date = dt.strftime('%Y-%m-%d')
                                            print("dep_date_to :", End_date)
                                            Currency = content[2].split()[0]
                                            Type = content[2].split()[2]
                                            if 'way' in Type.lower():
                                                Type = 'One Way'
                                            else:
                                                Type = 'Return'
                                            print 'type:', Type
                                            Airline = 'G9'
                                            compartment = 'J'
                                            print 'Currency:', Currency
                                            Fare = content[2].split(".")[0].split()[1]
                                            r = re.compile("\d*")
                                            m = r.findall(Fare)
                                            print m[0]
                                            Fare = m[0]
                                            print 'Fare:', Fare
                                            url = "http://www.airarabia.com/en/best-offers"
                                            doc = {
                                                "Airline": Airline,
                                                "OD_1": origin+destination,
                                                "OD": origin + destination,
                                                "Valid from": None,
                                                "Valid till": Valid_till,
                                                "Oneway/ Return": Type,
                                                "compartment": compartment,
                                                "Fare": Fare,
                                                "Currency": Currency,
                                                "dep_date_from": Start_date,
                                                "dep_date_to": End_date,
                                                "Url": url,
                                                "Last updated Date": time.strftime('%Y-%m-%d'),
                                                "Last updated Time": time.strftime('%H')
                                            }
                                            data_doc = {
                                                "Airline": Airline,
                                                "OD_1": origin + destination,
                                                "OD": origin + destination,
                                                "Valid from": None,
                                                "Valid till": Valid_till,
                                                "Oneway/ Return": Type,
                                                "compartment": compartment,
                                                "Fare": Fare,
                                                "Currency": Currency,
                                                "dep_date_from": Start_date,
                                                "dep_date_to": End_date,
                                                "Url": url,
                                            }
                                            print "data-doc: ", data_doc
                                            print "doc: ", doc
                                            print "yes"
                                            time.sleep(10)

                                            cursor_OD = list(
                                                db.JUP_DB_OD_Master.aggregate([{'$project': {'_id': 0, 'OD': 1}}]))
                                            print "1---yes"
                                            a = 0
                                            # print "cursor:", cursor_OD
                                            for i in cursor_OD:
                                                if (data_doc['OD'] == i['OD']):
                                                    print "OD in list:", i['OD']
                                                    print "present OD:", data_doc['OD']
                                                    rule_trig = 0



                                                    @measure(JUPITER_LOGGER)
                                                    def rule_change():
                                                        rule_trig = 0
                                                        a = 0
                                                        print "rule_change_trigger_starts"
                                                        cursor = list(db.JUP_DB_Promotions.aggregate([
                                                            {'$match': {'OD': data_doc['OD'],
                                                                        'Airline': data_doc['Airline'],
                                                                        'Fare': data_doc['Fare'],
                                                                        'dep_date_from': data_doc['dep_date_from'],
                                                                        'dep_date_to': data_doc['dep_date_to']}},
                                                            {"$sort": {"Last Updated Date": -1,
                                                                       "Last Updated Time": -1}}]))
                                                        # cursor = json.dump(cursor)
                                                        print cursor
                                                        # print "data_doc: ", data_doc
                                                        if len(cursor) > 0:
                                                            for i in range(len(cursor)):
                                                                if (cursor[i]['Valid from'] == data_doc[
                                                                    'Valid from']) and (
                                                                            cursor[i]['Currency'] == data_doc[
                                                                            'Currency']) \
                                                                        and (
                                                                                    cursor[i]['Valid till'] == data_doc[
                                                                                    'Valid till']) \
                                                                        and (
                                                                                    cursor[i]['compartment'] ==
                                                                                    data_doc[
                                                                                        'compartment']):
                                                                    pass
                                                                else:
                                                                    pass
                                                                if (cursor[i]['Valid from'] != data_doc[
                                                                    'Valid from']) or (
                                                                            cursor[i]['Valid till'] != data_doc[
                                                                            'Valid till']) \
                                                                        or (
                                                                                    cursor[i]['Currency'] != data_doc[
                                                                                    'Currency']) \
                                                                        or (
                                                                                    cursor[i]['compartment'] !=
                                                                                    data_doc[
                                                                                        'compartment']):
                                                                    print "Raise Rule Change Trigger"  ### Call the trigger
                                                                    promo_rule_change_trigger = PromoRuleChangeTrigger(
                                                                        "promotions_ruleschange",
                                                                        old_doc_data=cursor[0],
                                                                        new_doc_data=doc,
                                                                        changed_field="Rules")
                                                                    promo_rule_change_trigger.do_analysis()
                                                                    rule_trig += 1
                                                                else:
                                                                    pass
                                                                if rule_trig == 1:
                                                                    a += 1
                                                                    break

                                                            print "rule change checked"
                                                        else:
                                                            print "No matches in Rule Change Trigger Check"
                                                            pass


                                                    print "done with rule change"

                                                    date_trig = 0


                                                    @measure(JUPITER_LOGGER)
                                                    def date_change():
                                                        date_trig = 0
                                                        print "date range trigger starts"
                                                        print data_doc
                                                        cursor = list(db.JUP_DB_Promotions.aggregate([
                                                            {'$match': {'OD': data_doc['OD'],
                                                                        'Airline': data_doc['Airline'],
                                                                        'compartment': data_doc['compartment'],
                                                                        'Currency': data_doc['Currency'],
                                                                        'Fare': data_doc['Fare'],
                                                                        'Valid from': data_doc['Valid from'],
                                                                        'Valid till': data_doc['Valid till'],
                                                                        }},
                                                            {"$sort": {"Last Updated Date": -1,
                                                                       "Last Updated Time": -1}}]))
                                                        # cursor = json.dump(cursor)
                                                        print cursor
                                                        if len(cursor) > 0:
                                                            for i in range(len(cursor)):

                                                                if (cursor[i]['dep_date_from'] == data_doc[
                                                                    'dep_date_from']) and (
                                                                            cursor[i]['dep_date_to'] == data_doc[
                                                                            'dep_date_to']):
                                                                    pass
                                                                else:
                                                                    pass
                                                                if (cursor[i]['dep_date_from'] != data_doc[
                                                                    'dep_date_from']) or (
                                                                            cursor[i]['dep_date_to'] != data_doc[
                                                                            'dep_date_to']):
                                                                    print "Raise Date Change Trigger (dep period is updated)"
                                                                    promo_date_change_trigger = PromoDateChangeTrigger(
                                                                        "promotions_dateschange",
                                                                        old_doc_data=cursor[0],
                                                                        new_doc_data=doc,
                                                                        changed_field="dep_period")
                                                                    promo_date_change_trigger.do_analysis()
                                                                    date_trig += 1
                                                                else:
                                                                    pass
                                                                if date_trig == 1:
                                                                    a = 2
                                                                    break
                                                                else:
                                                                    pass

                                                            else:
                                                                print "No docs in Date Change trigger check"
                                                                pass


                                                    print "coming in ----yes1"
                                                    fare_trig = 0


                                                    @measure(JUPITER_LOGGER)
                                                    def fare_change():
                                                        fare_trig = 0
                                                        print "fare_range"
                                                        cursor = list(db.JUP_DB_Promotions.aggregate([
                                                            {'$match': {'OD': data_doc['OD'],
                                                                        'Airline': data_doc['Airline'],
                                                                        'compartment': data_doc['compartment'],
                                                                        'Currency': data_doc['Currency'],
                                                                        'Valid from': data_doc['Valid from'],
                                                                        'dep_date_from': data_doc['dep_date_from'],
                                                                        'dep_date_to': data_doc['dep_date_to'],
                                                                        'Valid till': data_doc['Valid till'],
                                                                        }},
                                                            {"$sort": {"Last Updated Date": -1,
                                                                       "Last Updated Time": -1}}]))
                                                        # cursor = json.dump(cursor)
                                                        print cursor
                                                        if len(cursor) > 0:
                                                            for i in range(len(cursor)):
                                                                if (cursor[i]['Fare'] == data_doc['Fare']):
                                                                    pass
                                                                else:
                                                                    pass
                                                                if (cursor[i]['Fare'] != data_doc['Fare']):
                                                                    print "Raise Fare Change Trigger (Fare is updated)"  ### Call the trigger
                                                                    promo_fare_change_trigger = PromoFareChangeTrigger(
                                                                        "promotions_fareschange",
                                                                        old_doc_data=cursor[0],
                                                                        new_doc_data=doc,
                                                                        changed_field="Fare")
                                                                    promo_fare_change_trigger.do_analysis()
                                                                    fare_trig += 1
                                                                else:
                                                                    pass
                                                                if fare_trig == 1:
                                                                    a = 3
                                                                    break
                                                                else:
                                                                    pass
                                                            else:
                                                                print "No docs in fares change check"
                                                                pass


                                                    new_trig = 0


                                                    @measure(JUPITER_LOGGER)
                                                    def new_promotion():
                                                        new_trig = 0
                                                        print "new_promotion"
                                                        # cursor = list(db.JUP_DB_Promotions.find({}))
                                                        cursor = list(db.JUP_DB_Promotions.aggregate([
                                                            {'$match': {'Airline': data_doc['Airline']}}]))
                                                        print "cursor:", cursor
                                                        if len(cursor) > 0:

                                                            for i in range(len(cursor)):
                                                                change_flag_list = []
                                                                if (cursor[i]['Valid from'] == data_doc[
                                                                    'Valid from']) and (
                                                                            cursor[i]['Valid till'] == data_doc[
                                                                            'Valid till']) \
                                                                        and (
                                                                                                        cursor[i][
                                                                                                            'compartment'] ==
                                                                                                        data_doc[
                                                                                                            'compartment']
                                                                                                and (
                                                                                                            cursor[i][
                                                                                                                'OD'] ==
                                                                                                            data_doc[
                                                                                                                'OD']) and (
                                                                                                        cursor[i][
                                                                                                            'Fare'] ==
                                                                                                        data_doc[
                                                                                                            'Fare']) and (
                                                                                                    cursor[i][
                                                                                                        'Currency'] ==
                                                                                                    data_doc[
                                                                                                        'Currency']) and (
                                                                                                cursor[i][
                                                                                                    'dep_date_from'] ==
                                                                                                data_doc[
                                                                                                    'dep_date_from']) and (
                                                                                            cursor[i]['dep_date_to'] ==
                                                                                            data_doc['dep_date_to'])):
                                                                    pass
                                                                else:
                                                                    pass

                                                                if (cursor[i]['Valid from'] == data_doc[
                                                                    'Valid from']) and (
                                                                            cursor[i]['Valid till'] == data_doc[
                                                                            'Valid till']) \
                                                                        and (
                                                                                                        cursor[i][
                                                                                                            'compartment'] ==
                                                                                                        data_doc[
                                                                                                            'compartment']
                                                                                                and (
                                                                                                            cursor[i][
                                                                                                                'OD'] !=
                                                                                                            data_doc[
                                                                                                                'OD']) and (
                                                                                                        cursor[i][
                                                                                                            'Fare'] ==
                                                                                                        data_doc[
                                                                                                            'Fare']) and (
                                                                                                    cursor[i][
                                                                                                        'Currency'] ==
                                                                                                    data_doc[
                                                                                                        'Currency']) and (
                                                                                                cursor[i][
                                                                                                    'dep_date_from'] ==
                                                                                                data_doc[
                                                                                                    'dep_date_from']) and (
                                                                                            cursor[i]['dep_date_to'] ==
                                                                                            data_doc['dep_date_to'])):
                                                                    print("Raise New Promotion Released Trigger")
                                                                    promo_new_promotion_trigger = PromoNewPromotionTrigger(
                                                                        "new_promotions",
                                                                        old_doc_data=cursor[i],
                                                                        new_doc_data=doc,
                                                                        changed_field="OD")
                                                                    promo_new_promotion_trigger.do_analysis()
                                                                    new_trig += 1
                                                                    if new_trig == 1:
                                                                        break
                                                                    else:
                                                                        pass
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
                                                                try:
                                                                    if cursor[i]['compartment'] != data_doc[
                                                                        'compartment']:
                                                                        change_flag_list.append(5)
                                                                    else:
                                                                        pass
                                                                except:
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
                                                                if cursor[i]['dep_date_from'] != data_doc[
                                                                    'dep_date_from']:
                                                                    change_flag_list.append(9)
                                                                else:
                                                                    pass
                                                                if cursor[i]['dep_date_to'] != data_doc['dep_date_to']:
                                                                    change_flag_list.append(10)
                                                                else:
                                                                    pass
                                                                print "change_flag_list:", change_flag_list
                                                                if len(change_flag_list) > 2:
                                                                    print(
                                                                        "Raise New Promotion Released Trigger, new promotions are released")
                                                                    promo_new_promotion_trigger = PromoNewPromotionTrigger(
                                                                        "new_promotions",
                                                                        old_doc_data=cursor[i],
                                                                        new_doc_data=doc,
                                                                        changed_field="new")
                                                                    promo_new_promotion_trigger.do_analysis()
                                                                    new_trig += 1
                                                                else:
                                                                    pass
                                                                if new_trig == 1:
                                                                    a = 4
                                                                    break
                                                                else:
                                                                    pass

                                                        else:
                                                            print "No docs in New Promo check"
                                                            pass


                                                    for i in range(1, 2):
                                                        rule_change()
                                                        if rule_trig == 1:
                                                            break
                                                        else:
                                                            pass
                                                        date_change()
                                                        if date_trig == 1:
                                                            break
                                                        else:
                                                            pass
                                                        fare_change()
                                                        if fare_trig == 1:
                                                            break
                                                        else:
                                                            pass
                                                        new_promotion()
                                                        if new_trig == 1:
                                                            break
                                                        else:
                                                            pass
                                                    else:
                                                        pass

                                            if t == 3:
                                                st = time.time()
                                                print "updating: ", count3

                                                # print bulk_update_doc
                                                db['JUP_DB_Promotions'].bulk_write(bulk_update_doc)
                                                print "updated!", time.time() - st
                                                bulk_list = []
                                                count3 += 1
                                                t = 0
                                                # driver.execute_script("window.scrollTo(0,600);")
                                                print "yes---------------------->"
                                            else:
                                                bulk_update_doc.append(UpdateOne(data_doc, {"$set": doc}, upsert=True))
                                                # print bulk_update_doc
                                                t += 1

                                                print "t= :", t


                                                # dest_countries_done.append(e_cols[dest_column_ind][dest_country_ind])
                                        ori_countries_done.append(cols[column_ind][country_ind])
                                        # print "ori_countries_done", ori_countries_done
                                        # print "dest_countries_done", dest_countries_done
                                    # except:
                                    # print traceback.print_exc()
                                    # print "No Proms"



                                else:
                                    driver.find_element_by_xpath(
                                        '//*[@id="block-airarabia-general-search-offers"]/div/div[2]/div/div[1]/p')
                                    print "No Offers"
                                    pass



