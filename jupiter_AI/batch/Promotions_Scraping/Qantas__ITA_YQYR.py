import pandas as pd
import numpy as np
import sys
reload(sys)
sys.setdefaultencoding('utf8')
from selenium import webdriver
import pymongo
from selenium.webdriver.support.ui import WebDriverWait
from requests.exceptions import ConnectionError
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from dateutil.parser import parse
import datetime
import re
from bs4 import BeautifulSoup
import requests
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
# Connecting to Mongodb
from jupiter_AI import JUPITER_DB, client, JUPITER_LOGGER
from jupiter_AI.logutils import measure
db = client[JUPITER_DB]
# chrome_path = r"/home/prathyusha/Downloads/chromedriver1"

driver = webdriver.PhantomJS()#service_args=PHANTOMJS_ARGS)
driver.wait = WebDriverWait(driver, 7)

# options = webdriver.ChromeOptions()
# driver = webdriver.Chrome(executable_path=chrome_path, chrome_options=options)
# driver.wait = WebDriverWait(driver, 5)

city_airport = list(db.JUP_DB_City_Airport_Mapping.aggregate([{'$project': {'_id': 0, 'City_Code': 1, 'Airport_Code': 1}}]))

data = pd.read_csv("/home/prathyusha/Flynava/Qantas_Competitors.csv")
dep_dates=[]
dep_date1= datetime.date(2018,9,01)
dep_date_1= datetime.date(2018,9,01)
dep_dates.append(dep_date1)
for i in range(1,16):
    dep_date1 = dep_date1 + datetime.timedelta(days=6)
    dep_dates.append(dep_date1)
for i in range(1,6):
    dep_date_1 = dep_date_1 - datetime.timedelta(days=6)
    dep_dates.append(dep_date_1)
dep_dates.sort()
print dep_dates
airlines_list = {}


@measure(JUPITER_LOGGER)
def airlines(A):
    if A== "JQ":
        airlines_list['JQ']="Jetstar"
    if A=="VA":
        airlines_list['VA']="Virgin Australia"
    if A=="ZL":
        airlines_list['ZL']="Rex"
    if A=="EK":
        airlines_list['EK']="Emirates"
    if A == "NZ":
        airlines_list['NZ']="Air New Zealand"
    if A == "TR":
        airlines_list['TR']="Scoot"
    if A == "CI":
        airlines_list['CI']="China Airlines"
    if A == "SQ":
        airlines_list['SQ']="Singapore Airlines"
    if A == "MH":
        airlines_list['MH']="Malaysia Airlines"
    if A == "CX":
        airlines_list['CX']="Cathay Pacific"
    if A == "MU":
        airlines_list['MU']="China Eastern"
    if A == "VN":
        airlines_list['VN']="Vietnam"
    if A == "MF":
        airlines_list['MF']="Xiamen"
    if A == "PR":
        airlines_list['PR']="Philippine Airlines"
    if A == "CA":
        airlines_list['CA']="Air China"
    if A == "OZ":
        airlines_list['OZ'] = "Asiana"
    if A == "KE":
        airlines_list['KE'] = "Korean Air"
    if A == "CZ":
        airlines_list['CZ'] = "China Southern"
    if A == "TG":
        airlines_list['TG']="THAI"
    if A == "FJ":
        airlines_list['FJ'] = "Fiji Airways"
    if A == "PG":
        airlines_list['PG'] = "Bangkok Airways"
    if A == "JU":
        airlines_list['JU'] = "JAL"
    if A == "AY":
        airlines_list['AY'] = "Finnair"
    if A == "HX":
        airlines_list['HX'] = "Hong Kong Airlines"
    if A == "BA":
        airlines_list['BA']="British Airways"
    if A == "NH":
        airlines_list['NH'] = "ANA"
    if A == "EY":
        airlines_list['EY'] = "Etihad"
    if A == "QR":
        airlines_list['QR'] = "Qatar Airways"
    if A == "VS":
        airlines_list['VS'] = "Virgin Atlantic"
    if A == "UA":
        airlines_list['UA'] = "United"
    if A == "DL":
        airlines_list['DL'] = "Delta"
    if A == "AA":
        airlines_list['AA'] = "American"
    if A == "AC":
        airlines_list['AC'] = "Air Canada"
    if A == "LH":
        airlines_list['LH'] = "Lufthansa"
    if A == "JU":
        airlines_list['JU'] = "JAL"
    if A == "QF":
        airlines_list['JU'] = "Qantas"
    if A == "AF":
        airlines_list['AF']="Air France"
    if A == "RA":
        airlines_list['RA']="Nepal Airlines"
    if A == "RJ":
        airlines_list['RJ']="Royal Jordanian"
    if A == "SA":
        airlines_list['SA']="South African"
    if A == "SN":
        airlines_list['SN']="Brussels Airlines"
    if A == "SQ":
        airlines_list['SQ']="Singapore Airlines"
    if A == "SU":
        airlines_list['SU']="Aeroflot"
    if A == "QF":
        airlines_list['QF'] = "Qantas"
    if A == "SU":
        airlines_list['JS'] = "Jetstar"
    if A == "SV":
        airlines_list['SV']="Saudia"
    if A == "TG":
        airlines_list['TG']="THAI"
    if A == "TK":
        airlines_list['TK']="Turkish Airlines"
    if A == "EY":
        airlines_list['EY'] = "Etihad"
    if A == "UA":
        airlines_list['UA']="United"
    if A == "UL":
        airlines_list['UL']="SriLankan"
    if A == "VS":
        airlines_list['VS']="Virgin Atlantic"
    if A == "WY":
        airlines_list['WY']="Oman Air"
    if A == "XY":
        airlines_list['XY']="Flynas"


@measure(JUPITER_LOGGER)
def ita_scraping(origin1, destination1, dep_date_start1, dep_date_end1, travel_type1, cabin1, A1, A2, A3, A4,
                 A5,A6,A7,A8,A9,A10,A11,A12,A13,A14,A15,A16,A17,A18,A19,A20,A21,A22,A23,A24):
    driver.get("https://matrix.itasoftware.com/")
    od = origin1 + destination1
    do = destination1 + origin1
    time.sleep(8)
    if travel_type1 == "OW":
        driver.find_element_by_xpath(
            '//*[@id="searchPanel-0"]/div/table/tbody/tr[1]/td/table/tbody/tr/td[3]/div/div').click()
        origin = driver.find_element_by_xpath('//*[@id="searchPanel-0"]/div/table/tbody/tr[2]/td/div/div[2]/div/div/div[2]/div/div/div/input')
        time.sleep(4)
        origin.send_keys(origin1[0])
        time.sleep(3)
        origin.send_keys(origin1[1])
        time.sleep(6)
        origin.send_keys(origin1[2])
        time.sleep(8)
        try:
            try:
                driver.wait.until(
                    EC.visibility_of_element_located(
                        (By.XPATH, '/html/body/div[5]/div/table/tbody/tr[2]/td[2]/div/div/table/tbody/tr[2]')))

                oo = driver.find_element_by_xpath(
                '/html/body/div[5]/div/table/tbody/tr[2]/td[2]/div/div/table/tbody/tr[2]').text
            except:
                driver.wait.until(
                    EC.visibility_of_element_located(
                        (By.XPATH, '/html/body/div[4]/div/table/tbody/tr[2]/td[2]/div/div/table/tbody/tr[2]')))

                oo = driver.find_element_by_xpath(
                    '/html/body/div[4]/div/table/tbody/tr[2]/td[2]/div/div/table/tbody/tr[2]').text
            oo1 = oo.split(" ")[-1].split("(")[1].split(")")[0]
            if oo1 == origin1:
                actions = ActionChains(driver)
                actions.send_keys(Keys.ARROW_DOWN).perform()
                time.sleep(3)
        except:
            pass
        origin.send_keys(u'\ue007')
        destination = driver.find_element_by_xpath('//*[@id="searchPanel-0"]/div/table/tbody/tr[2]/td/div/div[2]/div/div/div[4]/div/div/div/input')
        destination.send_keys(destination1[0])
        time.sleep(3)
        destination.send_keys(destination1[1])
        time.sleep(3)
        destination.send_keys(destination1[2])
        time.sleep(5)
        try:
            try:
                dd = driver.find_element_by_xpath(
                    '/html/body/div[4]/div/table/tbody/tr[2]/td[2]/div/div/table/tbody/tr[2]').text
            except:
                dd = driver.find_element_by_xpath(
                    '/html/body/div[4]/div/table/tbody/tr[2]/td[2]/div/div/table/tbody/tr[2]').text
            dd1 = dd.split(" ")[-1].split("(")[1].split(")")[0]
            if dd1 == destination1:
                actions = ActionChains(driver)
                actions.send_keys(Keys.ARROW_DOWN).perform()
                time.sleep(3)
        except:
            pass
        destination.send_keys(u'\ue007')

    else:
        pass
    time.sleep(6)
    #//*[@id="cityPair-orig-0"]
    if travel_type1 == "RT":
        time.sleep(8)
        # try:
            # driver.find_element_by_xpath(
            #     '//*[@id="searchPanel-0"]/div/table/tbody/tr[2]/td/div/div[2]/div/div/div[2]/div/div/div/input').click()
        origin = driver.find_element_by_xpath(".//input[contains(@id,'cityPair-orig-')]")
        # except:
            # driver.find_element_by_xpath(
            #     '//*[@id="searchPanel-0"]/div/table/tbody/tr[2]/td/div/div[2]/div/div/div[2]/div/div/div/input').click()
            # origin = driver.find_element_by_xpath(
            #     '//*[@id="searchPanel-0"]/div/table/tbody/tr[1]/td/div/div[2]/div/div/div[2]/div/div/div/input')
        time.sleep(3)
        origin.send_keys(origin1[0])
        time.sleep(2)
        origin.send_keys(origin1[1])
        time.sleep(5)
        origin.send_keys(origin1[2])
        time.sleep(4)
        try:
            try:
                driver.wait.until(
                    EC.visibility_of_element_located(
                        (By.XPATH, '/html/body/div[5]/div/table/tbody/tr[2]/td[2]/div/div/table/tbody/tr[2]')))

                oo = driver.find_element_by_xpath(
                '/html/body/div[5]/div/table/tbody/tr[2]/td[2]/div/div/table/tbody/tr[2]').text
            except:
                driver.wait.until(
                    EC.visibility_of_element_located(
                        (By.XPATH, '/html/body/div[4]/div/table/tbody/tr[2]/td[2]/div/div/table/tbody/tr[2]')))

                oo = driver.find_element_by_xpath(
                    '/html/body/div[4]/div/table/tbody/tr[2]/td[2]/div/div/table/tbody/tr[2]').text
            oo1 = oo.split(" ")[-1].split("(")[1].split(")")[0]
            if oo1 == origin1:
                actions = ActionChains(driver)
                actions.send_keys(Keys.ARROW_DOWN).perform()
                time.sleep(3)
        except:
            pass
        origin.send_keys(u'\ue007')
        destination = driver.find_element_by_xpath(".//input[contains(@id,'cityPair-dest-')]")

        destination.send_keys(destination1[0])
        time.sleep(3)
        destination.send_keys(destination1[1])
        time.sleep(3)
        destination.send_keys(destination1[2])
        time.sleep(5)
        try:
            try:
                dd = driver.find_element_by_xpath(
                    '/html/body/div[4]/div/table/tbody/tr[2]/td[2]/div/div/table/tbody/tr[2]').text
            except:
                dd = driver.find_element_by_xpath(
                    '/html/body/div[4]/div/table/tbody/tr[2]/td[2]/div/div/table/tbody/tr[2]').text
            dd1 = dd.split(" ")[-1].split("(")[1].split(")")[0]
            if dd1 == destination1:
                actions = ActionChains(driver)
                actions.send_keys(Keys.ARROW_DOWN).perform()
                time.sleep(3)
        except:
            pass
        destination.send_keys(u'\ue007')
    if cabin1 == "Y":
        driver.find_element_by_xpath('//*[@id="searchPanel-0"]/div/div/div[4]/div/select').click()
        driver.find_element_by_xpath('//*[@id="searchPanel-0"]/div/div/div[4]/div/select/option[1]').click()
    elif cabin1 == "J":
        driver.find_element_by_xpath('//*[@id="searchPanel-0"]/div/div/div[4]/div/select').click()
        driver.find_element_by_xpath('//*[@id="searchPanel-0"]/div/div/div[4]/div/select/option[3]').click()
    elif cabin1 == "F":
        driver.find_element_by_xpath('//*[@id="searchPanel-0"]/div/div/div[4]/div/select').click()
        driver.find_element_by_xpath('//*[@id="searchPanel-0"]/div/div/div[4]/div/select/option[4]').click()
    else:
        pass
    time.sleep(4)
    if travel_type1 == "OW":
        dep_date_start = driver.find_element_by_xpath('//*[@id="searchPanel-0"]/div/table/tbody/tr[2]/td/div/div[2]/div/div/div[9]/div[1]/div[1]/div[2]/input')
        dep_date_start.send_keys(dep_date_start1)
        time.sleep(3)
        dep_date_start.send_keys(u'\ue007')

    if travel_type1 == "RT":
        dep_date_end = driver.find_element_by_xpath(".//input[contains(@id,'cityPair-retDate-')]")
        dep_date_end.send_keys(dep_date_end1)
        time.sleep(3)
        dep_date_end.send_keys(u'\ue007')
        dep_date_start = driver.find_element_by_xpath(".//input[contains(@id,'cityPair-outDate-')]")
        dep_date_start.send_keys(dep_date_start1)
        time.sleep(3)
        dep_date_start.send_keys(u'\ue007')

    time.sleep(7)
    print A1
    airlines(A1)
    airlines(A2)
    airlines(A3)
    airlines(A4)
    airlines(A5)
    airlines(A6)
    airlines(A7)
    airlines(A8)
    airlines(A9)
    airlines(A10)
    airlines(A11)
    airlines(A12)
    airlines(A13)
    airlines(A14)
    airlines(A15)
    airlines(A16)
    airlines(A17)
    airlines(A18)
    airlines(A19)
    airlines(A20)
    airlines(A21)
    airlines(A22)
    airlines(A23)
    airlines(A24)
    return origin1, destination1, dep_date_start1, dep_date_end1, travel_type1, cabin1
# data = data[3:]
# print data.head()
# print data['Markets'][0]
for saa in range(len(dep_dates)-1):
    for sa in range(len(data)):
        # print sa
        # print "----------",data['Markets'][sa][:3]
        origin3 = data['Markets'][sa][:3]
        destination3 = data['Markets'][sa][3:]
        dep_date_start2 = dep_dates[saa].strftime("%m/%d/%Y")
        dep_date_end2 = dep_dates[saa+1].strftime("%m/%d/%Y")
        travel_type2 = data['Journey'][sa]
        cabin2 = data['Compartment'][sa]
        A1 = data['Competitors1'][sa]
        A2 = data['Competitors2'][sa]
        A3 = data['Competitors3'][sa]
        A4 = data['Competitors4'][sa]
        A5 = data['Competitors5'][sa]
        A6 = data['Competitors6'][sa]
        A7 = data['Competitors7'][sa]
        A8 = data['Competitors8'][sa]
        A9 = data['Competitors9'][sa]
        A10 = data['Competitors10'][sa]
        A11 = data['Competitors11'][sa]
        A12 = data['Competitors12'][sa]
        A13 = data['Competitors13'][sa]
        A14 = data['Competitors14'][sa]
        A15 = data['Competitors15'][sa]
        A16 = data['Competitors16'][sa]
        A17 = data['Competitors17'][sa]
        A18 = data['Competitors18'][sa]
        A19 = data['Competitors19'][sa]
        A20 = data['Competitors20'][sa]
        A21 = data['Competitors21'][sa]
        A22 = data['Competitors22'][sa]
        A23 = data['Competitors23'][sa]
        A24 = data['Competitors24'][sa]

        ods = []
        origin_ = origin3
        destination_ = destination3
        for l in city_airport:
            if origin_ == l['Airport_Code']:
                origin_ = l['City_Code']
            else:
                pass
                # city_code_ori_list.append(l['City_Code'])
            if destination_ == l['Airport_Code']:
                destination_ = l['City_Code']
            else:
                pass
        ods.append(origin_ + destination_)
        print len(ods)
        # print ods
        final_odlist = []
        airport_code_ori_list = []
        airport_code_dest_list = []
        for l in ods:
            origin_ = l[:3]
            destination_ = l[3:]
        for k in city_airport:
            if origin_ == k['City_Code']:
                airport_code_ori_list.append(k['Airport_Code'])
            if destination_ == k['City_Code']:
                airport_code_dest_list.append(k['Airport_Code'])
        airport_code_ori_list.append(origin_)
        airport_code_dest_list.append(destination_)
        airport_code_ori_list = list(set(airport_code_ori_list))
        airport_code_dest_list = list(set(airport_code_dest_list))
        print airport_code_ori_list, airport_code_dest_list
        print len(airport_code_ori_list), len(airport_code_dest_list)
        for m in airport_code_ori_list:
            for n in airport_code_dest_list:
                final_odlist.append(m + n)
        for pp in final_odlist:
            try:

                origin2 = pp[:3]
                destination2 = pp[3:]
                origin1, destination1, dep_date_start1, dep_date_end1, travel_type1, cabin1 = ita_scraping(origin2, destination2,dep_date_start2, dep_date_end2, travel_type2, cabin2, A1, A2, A3, A4,
                     A5,A6,A7,A8,A9,A10,A11,A12,A13,A14,A15,A16,A17,A18,A19,A20,A21,A22,A23,A24)
                print "airlines_list: ", airlines_list
                driver.wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="contentwrapper"]/div[1]/div/div[6]/div[1]')))

                print time.sleep(30)
                driver.wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="contentwrapper"]/div[1]/div/div[6]/div[1]')))
                print "page loaded"
                # driver.wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="contentwrapper"]/div[1]/div/div[5]/div/div/table/tbody/tr/td[2]/table/tbody/tr/td[2]/div/div/table/tbody/tr[1]/td[1]')))
                url2 = driver.current_url
                print "url2 : ", url2
                time.sleep(4)
                response = requests.get(url2)
                time.sleep(4)
                source = driver.page_source
                time.sleep(4)
                soup = BeautifulSoup(source, "lxml")
                time.sleep(4)
                # print soup
                time.sleep(4)
                # driver.wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="contentwrapper"]/div[1]/div/div[6]/div[1]')))
                time.sleep(4)
                airlines_bs = soup.find_all("td", {"class": "IR6M2QD-h-n"})
                time.sleep(4)
                print airlines_bs
                # pri )
                print airlines_list.values()
                if len(airlines_bs) != 0:
                    print "what!"
                    for i in range(len(airlines_bs)):
                        print i
                        print airlines_bs[i].text
                        if airlines_bs[i].text in airlines_list.values():
                            print "clicking: ", airlines_bs[i].text
                            Airline = airlines_bs[i].text
                            for r in range(len(airlines_list.values())):
                                if Airline == airlines_list.values()[r]:
                                    print r
                                    airline_code = airlines_list.keys()[r]
                            print "]]]]]]] "
                            time.sleep(6)
                            xx = driver.find_element_by_xpath(
                                '//*[@id="contentwrapper"]/div[1]/div/div[6]/div[4]/div[4]/div[1]/div/div[1]')
                            if "No" not in xx.text:
                                # clicking on selected airline
                                driver.find_element_by_xpath(
                                    '//*[@id="contentwrapper"]/div[1]/div/div[5]/div/div/table/tbody/tr/td[2]/table/tbody/tr/td[2]/div/div/table/tbody/tr[1]/td[' + str(
                                        i + 1) + ']/a').click()
                                # clicking on 1st in the list
                                print "////"
                                time.sleep(6)
                                xx = driver.find_element_by_xpath(
                                    '//*[@id="contentwrapper"]/div[1]/div/div[6]/div[4]/div[4]/div[1]/div/div[1]')

                                try:
                                    airline_name = driver.find_element_by_xpath(
                                        '//*[@id="contentwrapper"]/div[1]/div/div[6]/div[4]/div[2]/div/div[1]/div[1]/table/tbody/tr/td[2]/table/tbody/tr[1]/td[1]/div/div[2]')
                                except:
                                    airline_name = driver.find_element_by_xpath(
                                        '//*[@id="contentwrapper"]/div[1]/div/div[6]/div[4]/div[2]/div/div[1]/div[1]/table/tbody/tr/td[2]/table/tbody/tr[1]/td[1]/div/div')
                                if "others" in airline_name.text:
                                    pass
                                else:

                                    try:
                                        driver.find_element_by_xpath(
                                            '//*[@id="contentwrapper"]/div[1]/div/div[6]/div[4]/div[2]/div/div[1]/div[1]/table/tbody/tr/td[1]/div/button/span').click()
                                    except:
                                        driver.find_element_by_xpath(
                                            '//*[@id="contentwrapper"]/div[1]/div/div[6]/div[4]/div[2]/div/div[1]/div[1]/table/tbody/tr/td[1]/div/button/span').click()
                                    time.sleep(30)
                                    driver.wait.until(EC.visibility_of_element_located((By.XPATH,
                                                                                        '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[1]/td[1]/div[2]/div[1]')))

                                    conn_flights = driver.find_element_by_xpath(
                                        '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[1]/td[1]/div[2]/div[2]/table[1]/tbody')
                                    conn_on = conn_flights.find_elements_by_tag_name('tr')
                                    # if connection is there:
                                    if len(conn_on) > 3:
                                        layover_on = driver.find_element_by_xpath(
                                            '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[1]/td[1]/div[2]/div[2]/table[1]/tbody/tr[4]/td[2]/div')
                                        lay1 = layover_on.text.split(" ")[-1]
                                        od1 = origin1 + lay1
                                        flightno_1 = driver.find_element_by_xpath(
                                            '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[1]/td[1]/div[2]/div[2]/table[1]/tbody/tr[3]/td[1]/div').text
                                        airline_flight1 = flightno_1.split(" ")[0]
                                        for r in range(len(airlines_list.values())):
                                            if airline_flight1 == airlines_list.values()[r]:
                                                print r
                                                flightno_code1 = airlines_list.keys()[r]
                                        flightno1_1 = flightno_code1 + flightno_1.split(" ")[1]
                                        rbd1 = driver.find_element_by_xpath(
                                            '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[1]/td[1]/div[2]/div[2]/table[1]/tbody/tr[3]/td[6]/div').text.split(
                                            "(")[1].split(")")[0]
                                        od2 = lay1 + destination1
                                        flightno_2 = driver.find_element_by_xpath(
                                            '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[1]/td[1]/div[2]/div[2]/table[1]/tbody/tr[6]/td[1]/div').text
                                        airline_flight23 = flightno_2.split(" ")[0]
                                        for r in range(len(airlines_list.values())):
                                            if airline_flight23 == airlines_list.values()[r]:
                                                print r
                                                flightno_code23 = airlines_list.keys()[r]
                                        flightno23_2 = flightno_code23 + flightno_2.split(" ")[1]
                                        rbd2 = driver.find_element_by_xpath(
                                            '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[1]/td[1]/div[2]/div[2]/table[1]/tbody/tr[6]/td[6]/div').text.split(
                                            "(")[1].split(")")[0]
                                        itin = {}
                                        itin['itin_1'] = {}
                                        # itin['itin_2'] = {}
                                        itin['itin_1']['conn1'] = {"od": od1, "flightno": flightno1_1, "rbd": rbd1}
                                        itin['itin_1']['conn2'] = {"od": od2, "flightno": flightno23_2, "rbd": rbd2}

                                    else:
                                        # if no connections present
                                        itinerary1 = driver.find_element_by_xpath(
                                            '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[1]/td[1]/div[2]/div[2]/table[1]/tbody/tr[1]/td[2]/div').text
                                        if "(" in itinerary1.split(" ")[1]:
                                            kl = 1
                                        elif "(" in itinerary1.split(" ")[2]:
                                            kl = 2
                                        else:
                                            pass
                                        if "(" in itinerary1.split(" ")[4]:
                                            kt = 4
                                        elif "(" in itinerary1.split(" ")[5]:
                                            kt = 5
                                        od1 = \
                                        ((itinerary1.split(" ")[kl] + itinerary1.split(" ")[kt]).split("(")[1]).split(")")[
                                            0] + \
                                        ((itinerary1.split(" ")[kl] + itinerary1.split(" ")[kt]).split("(")[2]).split(")")[
                                            0]
                                        flightno_1 = driver.find_element_by_xpath(
                                            '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[1]/td[1]/div[2]/div[2]/table[1]/tbody/tr[2]/td[1]/div').text
                                        airline_flight1 = flightno_1.split(" ")[0]
                                        for r in range(len(airlines_list.values())):
                                            if airline_flight1 == airlines_list.values()[r]:
                                                print r
                                                flightno_code1 = airlines_list.keys()[r]
                                        flightno1_1 = flightno_code1 + flightno_1.split(" ")[1]

                                        rbd1 = driver.find_element_by_xpath(
                                            '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[1]/td[1]/div[2]/div[2]/table[1]/tbody/tr[2]/td[6]/div').text.split(
                                            "(")[1].split(")")[0]
                                        conn_flights = driver.find_element_by_xpath(
                                            '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[1]/td[1]/div[2]/div[2]/table[1]/tbody')
                                        conn_flights1 = conn_flights.find_elements_by_tag_name('tr')
                                        print "--------", len(conn_flights1)

                                        itin = {}
                                        itin['itin_1'] = {}
                                        # itin['itin_2'] = {}
                                        itin['itin_1']['conn1'] = {"od": od1, "flightno": flightno1_1, "rbd": rbd1}

                                    # now return journey, if connecting flights are there
                                    if travel_type1 != "OW":
                                        try:
                                            conn_flights1 = driver.find_element_by_xpath(
                                                '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[1]/td[1]/div[2]/div[2]/table[2]/tbody')
                                            conn_rt = conn_flights1.find_elements_by_tag_name('tr')
                                            if len(conn_rt) > 3:
                                                layover_rt = driver.find_element_by_xpath(
                                                    '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[1]/td[1]/div[2]/div[2]/table[2]/tbody/tr[4]/td[2]/div')
                                                lay2 = layover_rt.text.split(" ")[-1]
                                                od11 = destination1 + lay2
                                                flightno_11 = driver.find_element_by_xpath(
                                                    '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[1]/td[1]/div[2]/div[2]/table[2]/tbody/tr[3]/td[1]/div').text
                                                airline_flight11 = flightno_11.split(" ")[0]
                                                for r in range(len(airlines_list.values())):
                                                    if airline_flight11 == airlines_list.values()[r]:
                                                        print r
                                                        flightno_code11 = airlines_list.keys()[r]
                                                flightno2_11 = flightno_code11 + flightno_11.split(" ")[1]

                                                rbd11 = driver.find_element_by_xpath(
                                                    '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[1]/td[1]/div[2]/div[2]/table[2]/tbody/tr[3]/td[6]/div').text.split(
                                                    "(")[1].split(")")[0]
                                                od22 = lay2 + origin1
                                                flightno_22 = driver.find_element_by_xpath(
                                                    '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[1]/td[1]/div[2]/div[2]/table[2]/tbody/tr[6]/td[1]/div').text
                                                airline_flight22 = flightno_22.split(" ")[0]
                                                for r in range(len(airlines_list.values())):
                                                    if airline_flight22 == airlines_list.values()[r]:
                                                        print r
                                                        flightno_code22 = airlines_list.keys()[r]
                                                flightno2_22 = flightno_code22 + flightno_22.split(" ")[1]

                                                rbd22 = driver.find_element_by_xpath(
                                                    '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[1]/td[1]/div[2]/div[2]/table[2]/tbody/tr[6]/td[6]/div').text.split(
                                                    "(")[1].split(")")[0]
                                                itin['itin_2'] = {}
                                                itin['itin_2']['conn1'] = {"od": od11, "flightno": flightno2_11,
                                                                           "rbd": rbd11}
                                                itin['itin_2']['conn2'] = {"od": od22, "flightno": flightno2_22,
                                                                           "rbd": rbd22}
                                                fare2_details = driver.find_element_by_xpath(
                                                    '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[3]/td/div/div/div[3]/table[1]/tbody/tr/td[1]/table/tbody/tr[2]/td/table/tbody/tr[2]').text
                                                value2 = fare2_details.split(" ")[-1].split(")")[1]
                                                if value2[3] == ".":
                                                    fare2_value = float(value2[4:].replace(",", ""))
                                                else:
                                                    fare2_value = float(value2[3:].replace(",", ""))
                                                Fare_2 = {}
                                                Fare_2['farebasiscode'] = fare2_details.split(" ")[4]
                                                Fare_2['od'] = fare2_details.split(" ")[5] + fare2_details.split(" ")[7]
                                                Fare_2['value'] = fare2_value
                                            else:
                                                # if no connections in return jurney
                                                itinerary2 = driver.find_element_by_xpath(
                                                    '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[1]/td[1]/div[2]/div[2]/table[2]/tbody/tr[1]/td[2]/div').text
                                                if "(" in itinerary2.split(" ")[1]:
                                                    kl = 1
                                                elif "(" in itinerary2.split(" ")[2]:
                                                    kl = 2
                                                else:
                                                    pass
                                                if "(" in itinerary2.split(" ")[4]:
                                                    kt = 4
                                                elif "(" in itinerary2.split(" ")[5]:
                                                    kt = 5

                                                od2 = ((itinerary2.split(" ")[kl] + itinerary2.split(" ")[kt]).split("(")[
                                                           1]).split(")")[0] + \
                                                      ((itinerary2.split(" ")[kl] + itinerary2.split(" ")[kt]).split("(")[
                                                           2]).split(")")[0]
                                                flightno_2 = driver.find_element_by_xpath(
                                                    '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[1]/td[1]/div[2]/div[2]/table[2]/tbody/tr[2]/td[1]/div').text
                                                airline_flight2 = flightno_2.split(" ")[0]
                                                for r in range(len(airlines_list.values())):
                                                    if airline_flight2 == airlines_list.values()[r]:
                                                        print r
                                                        flightno_code2 = airlines_list.keys()[r]
                                                flightno2_2 = flightno_code2 + flightno_2.split(" ")[1]

                                                rbd2 = driver.find_element_by_xpath(
                                                    '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[1]/td[1]/div[2]/div[2]/table[2]/tbody/tr[2]/td[6]/div').text.split(
                                                    "(")[1].split(")")[0]
                                                depdate2 = itinerary2.split(" ")[-3] + itinerary2.split(" ")[-2] + \
                                                           itinerary2.split(" ")[-1] + " " + \
                                                           itinerary2.split(" ")[-4]
                                                itin['itin_2'] = {}
                                                itin['itin_2']['conn1'] = {"od": od2, "flightno": flightno2_2, "rbd": rbd2}
                                                fare2_details = driver.find_element_by_xpath(
                                                    '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[3]/td/div/div/div[3]/table[1]/tbody/tr/td[1]/table/tbody/tr[2]/td/table/tbody/tr[2]').text
                                                value2 = fare2_details.split(" ")[-1].split(")")[1]
                                                if value2[3] == ".":
                                                    fare2_value = float(value2[4:].replace(",", ""))
                                                else:
                                                    fare2_value = float(value2[3:].replace(",", ""))
                                                Fare_2 = {}
                                                Fare_2['farebasiscode'] = fare2_details.split(" ")[4]
                                                Fare_2['od'] = fare2_details.split(" ")[5] + fare2_details.split(" ")[7]
                                                Fare_2['value'] = fare2_value
                                                # print itinerary_2
                                                print Fare_2
                                        except:
                                            # if travel_type1 != "OW":
                                            fare2_details = driver.find_element_by_xpath(
                                                '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[3]/td/div/div/div[3]/table[1]/tbody/tr/td[1]/table/tbody/tr[2]/td/table/tbody/tr[2]').text
                                            value2 = fare2_details.split(" ")[-1].split(")")[1]
                                            if value2[3] == ".":
                                                fare2_value = float(value2[4:].replace(",", ""))
                                            else:
                                                fare2_value = float(value2[3:].replace(",", ""))

                                            Fare_2 = {}
                                            Fare_2['farebasiscode'] = fare2_details.split(" ")[4]
                                            Fare_2['od'] = fare2_details.split(" ")[5] + fare2_details.split(" ")[7]
                                            Fare_2['value'] = fare2_value
                                            print Fare_2
                                            # itinerary_2 = ""
                                            # Fare_2 = ""
                                            # pass

                                            itinerary2 = driver.find_element_by_xpath(
                                                '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[1]/td[1]/div[2]/div[2]/table[2]/tbody/tr[1]/td[2]/div').text
                                            if "(" in itinerary2.split(" ")[1]:
                                                kl = 1
                                            elif "(" in itinerary2.split(" ")[2]:
                                                kl = 2
                                            else:
                                                pass
                                            if "(" in itinerary2.split(" ")[4]:
                                                kt = 4
                                            elif "(" in itinerary2.split(" ")[5]:
                                                kt = 5

                                            od2 = \
                                            ((itinerary2.split(" ")[kl] + itinerary2.split(" ")[kt]).split("(")[1]).split(
                                                ")")[0] + \
                                            ((itinerary2.split(" ")[kl] + itinerary2.split(" ")[kt]).split("(")[2]).split(
                                                ")")[0]
                                            flightno_2 = driver.find_element_by_xpath(
                                                '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[1]/td[1]/div[2]/div[2]/table[2]/tbody/tr[2]/td[1]/div').text
                                            airline_flight2 = flightno_2.split(" ")[0]
                                            for r in range(len(airlines_list.values())):
                                                if airline_flight2 == airlines_list.values()[r]:
                                                    print r
                                                    flightno_code2 = airlines_list.keys()[r]
                                            flightno2_2 = flightno_code2 + flightno_2.split(" ")[1]

                                            rbd2 = driver.find_element_by_xpath(
                                                '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[1]/td[1]/div[2]/div[2]/table[2]/tbody/tr[2]/td[6]/div').text.split(
                                                "(")[1].split(")")[0]
                                            itin['itin_2'] = {}
                                            itin['itin_2']['conn1'] = {"od": od2, "flightno": flightno2_2, "rbd": rbd2}

                                    fares_bs = soup.find_all("div", {"class": "IR6M2QD-l-m"})
                                    fare1_details = driver.find_element_by_xpath(
                                        '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[3]/td/div/div/div[3]/table[1]/tbody/tr/td[1]/table/tbody/tr[2]/td/table/tbody/tr[1]').text
                                    value = fare1_details.split(" ")[-1].split(")")[1]
                                    if value[3] == ".":
                                        fare1_value = float(value[4:].replace(",", ""))
                                    else:
                                        fare1_value = float(value[3:].replace(",", ""))
                                    # Fares = {}
                                    # Fares['Fare_1'] = {"carrier": fare1_details.split(" ")[3],
                                    #                    "farebasiscode": fare1_details.split(" ")[4],
                                    #                    "od": fare1_details.split(" ")[5] + fare1_details.split(" ")[7],
                                    #                    "value": fare1_value}

                                    Fare_1 = {}
                                    Fare_1['farebasiscode'] = fare1_details.split(" ")[4]
                                    Fare_1['od'] = fare1_details.split(" ")[5] + fare1_details.split(" ")[7]
                                    Fare_1['value'] = fare1_value

                                    url3 = driver.current_url
                                    response = requests.get(url3)
                                    source = driver.page_source
                                    soup3 = BeautifulSoup(source, "lxml")
                                    time.sleep(5)
                                    taxes_f = soup3.find_all("td", {"class": "IR6M2QD-l-g"})
                                    taxes_v = soup3.find_all("td", {"class": "IR6M2QD-l-f"})
                                    taxes_subtotal = soup3.find_all("td", {"class": "IR6M2QD-l-l"})
                                    taxes_fields_list = []
                                    taxes_values_list = []
                                    if travel_type1 == "RT":
                                        for j in range(2, len(taxes_f) - 3):
                                            try:
                                                taxes_fields_list.append(taxes_f[j].text.split("(")[1].split(")")[0])
                                                taxes_values_list.append(taxes_v[j].text)
                                            except:
                                                taxes_fields_list.append(taxes_f[j].text)
                                                taxes_values_list.append(taxes_subtotal[0].text)
                                    else:
                                        for j in range(1, len(taxes_f) - 3):
                                            try:
                                                taxes_fields_list.append(taxes_f[j].text.split("(")[1].split(")")[0])
                                                taxes_values_list.append(taxes_v[j].text)
                                            except:
                                                taxes_fields_list.append(taxes_f[j].text)
                                                taxes_values_list.append(taxes_subtotal[0].text)
                                    # print itinerary_1
                                    # print Fare_1
                                    # removing currency name in value
                                    taxes_values = []
                                    if taxes_values_list[1][2] == ".":
                                        for i in range(len(taxes_values_list)):
                                            taxes_values.append(taxes_values_list[i][3:])
                                    else:
                                        for i in range(len(taxes_values_list)):
                                            taxes_values.append(taxes_values_list[i][2:])
                                    for k in range(len(taxes_values)):
                                        taxes_values[k] = float(taxes_values[k].replace(",", ""))
                                    taxes = dict(
                                        [(taxes_fields_list[i], taxes_values[i]) for i in range(len(taxes_fields_list))])
                                    print "taxes: ", taxes
                                    # print "Fares: ", Fares
                                    try:
                                        yq_value = float(taxes['YQ'])
                                    except:
                                        yq_value = 0.0
                                    try:
                                        yr_value = float(taxes['YR'])
                                    except:
                                        yr_value = 0.0
                                    # print taxes
                                    print yr_value, yq_value
                                    print taxes['Subtotal per passenger']
                                    sub_total = float(taxes['Subtotal per passenger'])
                                    print "sub_total: ", sub_total

                                    taxes_new = {}
                                    for i in range(len(taxes)):
                                        if taxes_fields_list[i] != "YQ" and taxes_fields_list[i] != "YR" and \
                                                        taxes_fields_list[i] != "Subtotal per passenger":
                                            taxes_new['taxes_' + str(i) + ''] = {"code": taxes_fields_list[i],
                                                                                 "value": taxes_values[i]}
                                    currency1 = "AUD"
                                    pos1 = origin1
                                    try:
                                        fare3_details = driver.find_element_by_xpath(
                                            '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[3]/td/div/div/div[3]/table[1]/tbody/tr/td[1]/table/tbody/tr[2]/td/table/tbody/tr[3]').text
                                        value3 = fare3_details.split(" ")[-1].split(")")[1]
                                        if value3[3] == ".":
                                            fare3_value = float(value3[4:].replace(",", ""))
                                        else:
                                            fare3_value = float(value3[3:].replace(",", ""))
                                        Fare_3 = {}
                                        Fare_3['farebasiscode'] = fare3_details.split(" ")[4]
                                        Fare_3['od'] = fare3_details.split(" ")[5] + fare3_details.split(" ")[7]
                                        Fare_3['value'] = fare3_value
                                        print Fare_3
                                    except:
                                        pass
                                    try:
                                        fare4_details = driver.find_element_by_xpath(
                                            '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[3]/td/div/div/div[3]/table[1]/tbody/tr/td[1]/table/tbody/tr[2]/td/table/tbody/tr[4]').text
                                        value4 = fare4_details.split(" ")[-1].split(")")[1]
                                        if value4[3] == ".":
                                            fare4_value = float(value4[4:].replace(",", ""))
                                        else:
                                            fare4_value = float(value4[3:].replace(",", ""))

                                        Fare_4 = {}
                                        Fare_4['farebasiscode'] = fare4_details.split(" ")[4]
                                        Fare_4['od'] = fare4_details.split(" ")[5] + fare4_details.split(" ")[7]
                                        Fare_4['value'] = fare4_value
                                        print Fare_4
                                    except:
                                        pass

                                    if travel_type1 == "RT":
                                        data_doc = dict()
                                        Fares = {}
                                        Fares['Fare_1'] = Fare_1
                                        Fares['Fare_2'] = Fare_2
                                        if len(Fares) ==2:
                                            total_fare = Fares['Fare_1']['value'] + Fares['Fare_2']['value']
                                        elif len(Fares)==3:
                                            total_fare = Fares['Fare_1']['value'] + Fares['Fare_2']['value'] + Fares['Fare_3']['value']
                                        elif len(Fares)==4:
                                            total_fare = Fares['Fare_1']['value'] + Fares['Fare_2']['value'] + Fares['Fare_3']['value'] + Fares['Fare_4']['value']
                                        data_doc = {
                                            "origin": origin1,
                                            "destination": destination1,
                                            "od": origin1 + destination1,
                                            "pos": pos1,
                                            "compartment": cabin1,
                                            "dep_date_start": datetime.datetime.strptime(dep_date_start1,
                                                                                         "%m/%d/%Y").strftime("%Y-%m-%d"),
                                            "dep_date_end": datetime.datetime.strptime(dep_date_end1, "%m/%d/%Y").strftime(
                                                "%Y-%m-%d"),
                                            "currency": currency1,
                                            "oneway/return": travel_type1,
                                            "airline": airline_code,
                                            # "itinerary_1":itinerary_1,
                                            # "itinerary_2":itinerary_2,
                                            "itinerary": itin,
                                            "Fares": Fares,
                                            "total_basefare":total_fare,
                                            "total_taxfare":sub_total-total_fare,
                                            # "fare_1":Fare_1,
                                            # "fare_2":Fare_2,
                                            "YQ": yq_value,
                                            "YR": yr_value,
                                            "subtotal": sub_total,
                                            "taxes": taxes_new
                                        }
                                        print "data_doc: ", data_doc
                                    else:
                                        data_doc = dict()
                                        Fares = {}
                                        Fares['Fare_1'] = Fare_1
                                        total_fare = Fares['Fare_1']['value']
                                        data_doc = {
                                            "origin": origin1,
                                            "destination": destination1,
                                            "od": origin1 + destination1,
                                            "pos": pos1,
                                            "compartment": cabin1,
                                            "dep_date_start": datetime.datetime.strptime(dep_date_start1,
                                                                                         "%m/%d/%Y").strftime(
                                                "%Y-%m-%d"),
                                            "dep_date_end": datetime.datetime.strptime(dep_date_end1, "%m/%d/%Y").strftime(
                                                "%Y-%m-%d"),
                                            "currency": currency1,
                                            "oneway/return": travel_type1,
                                            "airline": airline_code,
                                            # "itinerary_1": itinerary_1,
                                            # "fare_1": Fare_1,
                                            "itinerary": itin,
                                            "Fares": Fares,
                                            "total_basefare": total_fare,
                                            "total_taxfare": sub_total - total_fare,
                                            "YQ": yq_value,
                                            "YR": yr_value,
                                            "subtotal": sub_total,
                                            "taxes": taxes_new
                                        }
                                        print "data_doc: ", data_doc
                                    db['JUP_DB_ITA_YQYR'].update(data_doc, data_doc, upsert=True)
                                    print "Inserted in to Collection"
                                    driver.find_element_by_xpath(
                                        '//*[@id="contentwrapper"]/div[1]/div/div[1]/div[1]/table/tbody/tr/td[4]/div/a').click()
                            else:
                                pass

            except:
                pass

driver.close()
driver.quit()