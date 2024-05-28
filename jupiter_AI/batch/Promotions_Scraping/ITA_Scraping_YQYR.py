"""
Author: Prathyusha Gontla
End date of developement: 2018-5-10
Code functionality:
             Scrapes YQYR values from ITA website and updates in JUP_DB_ITA_YQYR Collection.

MODIFICATIONS LOG
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :

"""
from jupiter_AI import JUPITER_LOGGER
from jupiter_AI.logutils import measure


@measure(JUPITER_LOGGER)
def ita_yqyr(type_actual,comp_actual,docss):
    import sys
    reload(sys)
    # sys.setdefaultencoding('utf8')
    from selenium import webdriver
    import pymongo
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By
    from requests.exceptions import ConnectionError
    from pymongo import UpdateOne
    from jupiter_AI.network_level_params import SYSTEM_TIME, SYSTEM_DATE
    import time
    import pandas as pd
    import traceback
    from dateutil.parser import parse
    import datetime
    import traceback
    import re
    from bs4 import BeautifulSoup
    import requests
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.keys import Keys
    # Connecting to Mongodb
    from jupiter_AI import JUPITER_DB, client
    db = client[JUPITER_DB]

    requests.get("https://free-proxy-list.net/")
    response = requests.get("https://free-proxy-list.net/")
    soup = BeautifulSoup(response.text, "lxml")
    ips = soup.find_all("tr")
    ips_list = []
    for i in range(len(ips)):
        #     print i
        try:
            ips_list.append(ips[i].find_all("td")[0].text + ":" + ips[i].find_all("td")[1].text)
        except:
            pass
    print ips_list
    try:
        PHANTOMJS_ARGS = [
            "--proxy="+str(ips_list[0])+"",
            "--proxy-type=http,https",
        ]

        driver = webdriver.PhantomJS(service_args=PHANTOMJS_ARGS)
        driver.wait = WebDriverWait(driver, 5)
        driver.wait = WebDriverWait(driver, 20)

        driver.get("https://matrix.itasoftware.com/")
        driver.find_element_by_xpath(
        '//*[@id="searchPanel-0"]/div/table/tbody/tr[1]/td/table/tbody/tr/td[3]/div/div').click()
    except:
        try:
            PHANTOMJS_ARGS = [
                "--proxy="+str(ips_list[1])+"",
                "--proxy-type=http,https",
            ]
            print PHANTOMJS_ARGS
            driver = webdriver.PhantomJS(service_args=PHANTOMJS_ARGS)
            driver.wait = WebDriverWait(driver, 5)
            driver.wait = WebDriverWait(driver, 20)

            driver.get("https://matrix.itasoftware.com/")
            driver.find_element_by_xpath(
                '//*[@id="searchPanel-0"]/div/table/tbody/tr[1]/td/table/tbody/tr/td[3]/div/div').click()
        except:
            try:
                PHANTOMJS_ARGS = [
                    "--proxy="+str(ips_list[2])+"",
                    "--proxy-type=http,https",
                ]
                print PHANTOMJS_ARGS
                driver = webdriver.PhantomJS(service_args=PHANTOMJS_ARGS)
                driver.wait = WebDriverWait(driver, 5)
                driver.wait = WebDriverWait(driver, 20)

                driver.get("https://matrix.itasoftware.com/")
                driver.find_element_by_xpath(
                    '//*[@id="searchPanel-0"]/div/table/tbody/tr[1]/td/table/tbody/tr/td[3]/div/div').click()
            except:
                print "no proxy is working"
    print PHANTOMJS_ARGS
    ## chrome_path = r"/home/prathyusha/Downloads/chromedriver1"
    # driver = webdriver.Chrome(chrome_path)
    # driver.wait = WebDriverWait(driver, 5)
    ## options = webdriver.ChromeOptions()
    # no_proxy = "168.70.1.202:8118"
    # options.add_argument('--proxy-server=%s' % no_proxy)
    ##driver = webdriver.Chrome(executable_path=chrome_path, chrome_options=options)
    ##driver.wait = WebDriverWait(driver, 5)

    city_airport = list(db.JUP_DB_City_Airport_Mapping.aggregate([{'$project': {'_id': 0, 'City_Code': 1, 'Airport_Code': 1}}]))


    airlines_list = {}


    @measure(JUPITER_LOGGER)
    def airlines(A):
        if A== "EK":
            airlines_list['EK']="Emirates"
        if A=="G9":
            airlines_list['G9']="Air Arabia"
        if A=="UL":
            airlines_list['UL']="SriLankan Airlines"
        if A=="FZ":
            airlines_list['FZ']="flydubai"
        if A == "MU":
            airlines_list['MU']="China Eastern"
        if A == "KE":
            airlines_list['KE']="Korean Air"
        if A == "3U":
            airlines_list['3U']="Sichuan"
        if A == "9W":
            airlines_list['9W']="Jet Airways"
        if A == "AC":
            airlines_list['AC']="Air Canada"
        if A == "AF":
            airlines_list['AF']="Air France"
        if A == "AY":
            airlines_list['AY']="Finnair"
        if A == "BA":
            airlines_list['BA']="British Airways"
        if A == "BG":
            airlines_list['BG']="Biman"
        if A == "CX":
            airlines_list['CX']="Cathay Pacific"
        if A == "CZ":
            airlines_list['CZ']="China Southern"
        if A == "DL":
            airlines_list['DL']="Delta"
        if A == "ET":
            airlines_list['ET']="Ethiopian"
        if A == "GF":
            airlines_list['GF']="Gulf Air"
        if A == "IB":
            airlines_list['IB']="Iberia"
        if A == "J9":
            airlines_list['J9']="Jazeera"
        if A == "JL":
            airlines_list['JL']="JAL"
        if A == "KC":
            airlines_list['KC']="Air Astana"
        if A == "KE":
            airlines_list['KE']="Korean Air"
        if A == "KL":
            airlines_list['KL']="KLM"
        if A == "KU":
            airlines_list['KU']="Kuwait Airways"
        if A == "LH":
            airlines_list['LH']="Lufthansa"
        if A == "LX":
            airlines_list['LX']="SWISS"
        if A == "MS":
            airlines_list['MS']="EgyptAir"
        if A == "OS":
            airlines_list['OS']="Austrian"
        if A == "PK":
            airlines_list['PK']="Pakistan"
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
    def ita_scraping(origin1, destination1, dep_date_start1, dep_date_end1, travel_type1, cabin1,currency1, A1, A2, A3, A4,
                     A5):
        print origin1, destination1,dep_date_start1, dep_date_end1, travel_type1, cabin1,currency1
        driver.get("https://matrix.itasoftware.com/")
        od = origin1 + destination1
        do = destination1 + origin1
        time.sleep(10)
        if travel_type1 == "OW":
            driver.find_element_by_xpath(
                '//*[@id="searchPanel-0"]/div/table/tbody/tr[1]/td/table/tbody/tr/td[3]/div/div').click()
            origin = driver.find_element_by_xpath('//*[@id="searchPanel-0"]/div/table/tbody/tr[2]/td/div/div[2]/div/div/div[2]/div/div/div/input')
            time.sleep(4)
            origin.send_keys(origin1[0])
            time.sleep(3)
            origin.send_keys(origin1[1])
            time.sleep(7)
            origin.send_keys(origin1[2])
            time.sleep(7)
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
            time.sleep(8)
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
        time.sleep(7)
        #//*[@id="cityPair-orig-0"]
        if travel_type1 == "RT":
            origin = driver.find_element_by_xpath(".//input[contains(@id,'cityPair-orig-')]")
            time.sleep(4)
            origin.send_keys(origin1[0])
            time.sleep(4)
            origin.send_keys(origin1[1])
            time.sleep(6)
            origin.send_keys(origin1[2])
            time.sleep(6)
            # try:
            #     actions = ActionChains(driver)
            #     actions.send_keys(Keys.ARROW_DOWN).perform()
            #     time.sleep(2)
            # except:
            #     pass

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
            time.sleep(6)
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
        else:
            pass
        time.sleep(5)
        currency = driver.find_element_by_xpath('//*[@id="searchPanel-0"]/div/div/div[8]/div/input')
        currency.send_keys(currency1[0])
        time.sleep(2)
        currency.send_keys(currency1[1])
        time.sleep(2)
        currency.send_keys(currency1[2])
        time.sleep(2)
        currency.send_keys(u'\ue007') #//*[@id="searchPanel-0"]/div/div/div[9]/div/input
        try:
            pos=driver.find_element_by_xpath('//*[@id="searchPanel-2"]/div/div/div[9]/div/input')
        except:
            pos = driver.find_element_by_xpath('//*[@id="searchPanel-0"]/div/div/div[9]/div/input')
        time.sleep(3)
        # pos.send_keys(pos1[0])
        # time.sleep(3)
        # pos.send_keys(pos1[1])
        # time.sleep(3)
        # pos.send_keys(pos1[2])
        # time.sleep(4)
        # pos.send_keys(u'\ue007')
        time.sleep(4)
        if travel_type1 == "OW":
            dep_date_start = driver.find_element_by_xpath('//*[@id="searchPanel-0"]/div/table/tbody/tr[2]/td/div/div[2]/div/div/div[9]/div[1]/div[1]/div[2]/input')
            dep_date_start.send_keys(dep_date_start1)
            time.sleep(4)
            dep_date_start.send_keys(u'\ue007')

        if travel_type1 == "RT":
            dep_date_end = driver.find_element_by_xpath(".//input[contains(@id,'cityPair-retDate-')]")
            dep_date_end.send_keys(dep_date_end1)
            time.sleep(4)
            dep_date_end.send_keys(u'\ue007')
            dep_date_start = driver.find_element_by_xpath(".//input[contains(@id,'cityPair-outDate-')]")
            dep_date_start.send_keys(dep_date_start1)
            time.sleep(4)
            dep_date_start.send_keys(u'\ue007')

        time.sleep(8)
        # url2 = "https://matrix.itasoftware.com/#view-flights:research=" + str(od) + ":" + str(do) + ""
        print A1
        airlines(A1)
        airlines(A2)
        airlines(A3)
        airlines(A4)
        airlines(A5)
        return origin1, destination1, dep_date_start1, dep_date_end1, travel_type1, cabin1
        # url2 = "https://matrix.itasoftware.com/#view-flights:research=" + str(od) + ":" + str(do) + ""

    # ita_scraping("BOM","DXB",'05/27/2018', '06/7/2018','RT','J','AED','EK','G9','FZ','A4','A5')
    # origin2="CMB"
    # destination2="JED"
    #airport_city_mapping
    #'update_date':datetime.datetime.today().strftime("%Y-%m-%d") #'update_date':SYSTEM_DATE
    skips = docss['skips']
    limits = docss['limits']
    # wfn = db.JUP_DB_Workflow_OD_User.find({'update_date':SYSTEM_DATE}).count()
    # docs = int(wfn/limits)
    wf=db.JUP_DB_Workflow_OD_User.find({'update_date':SYSTEM_DATE},{'_id':0,'origin':1,'destination':1,'dep_date_start':1,'dep_date_end':1,'mrkt_data.comp.airline':1}).skip(skips).limit(limits)
    wf2 = pd.DataFrame(list(wf))
    wf2['A1']=0
    wf2['A2']=0
    wf2['A3']=0
    wf2['A4']=0
    wf2['A5']=0
    wf2['comp'] = wf2.mrkt_data.apply(lambda row: row['comp'])

    for i in range(len(wf2)):
        if len(wf2['comp'][i]) == 1:
            wf2['A1'][i]=wf2['comp'][i][0]['airline']
        elif len(wf2['comp'][i]) == 2:
            wf2['A1'][i]=wf2['comp'][i][0]['airline']
            wf2['A2'][i]=wf2['comp'][i][1]['airline']
        elif len(wf2['comp'][i]) == 3:
            wf2['A1'][i]=wf2['comp'][i][0]['airline']
            wf2['A2'][i]=wf2['comp'][i][1]['airline']
            wf2['A3'][i]=wf2['comp'][i][2]['airline']
        elif len(wf2['comp'][i]) == 4:
            wf2['A1'][i]=wf2['comp'][i][0]['airline']
            wf2['A2'][i]=wf2['comp'][i][1]['airline']
            wf2['A3'][i]=wf2['comp'][i][2]['airline']
            wf2['A4'][i]=wf2['comp'][i][3]['airline']
        elif len(wf2['comp'][i]) == 5:
            wf2['A1'][i]=wf2['comp'][i][0]['airline']
            wf2['A2'][i]=wf2['comp'][i][1]['airline']
            wf2['A3'][i]=wf2['comp'][i][2]['airline']
            wf2['A4'][i]=wf2['comp'][i][3]['airline']
            wf2['A5'][i]=wf2['comp'][i][4]['airline']
    wf2 = wf2.drop(['mrkt_data','comp'],axis=1)
    wf2 = wf2.drop_duplicates()
    print wf2.head()
    print "no. of today's records in workflow od user",len(wf2)

    for ra in range(len(wf2)):
        origin3 = wf2['origin'][ra]
        destination3 = wf2['destination'][ra] # wf2['destination'][ra]
        dep_date_start2 = datetime.datetime.strptime(wf2['dep_date_start'][ra],"%Y-%m-%d").strftime("%m/%d/%Y")
        dep_date_end2 = datetime.datetime.strptime(wf2['dep_date_end'][ra],"%Y-%m-%d").strftime("%m/%d/%Y")
        travel_type2 = type_actual
        cabin2 = comp_actual
        A1 = wf2['A1'][ra]
        A2 = wf2['A2'][ra]
        A3 = wf2['A3'][ra]
        A4 = wf2['A4'][ra]
        A5 = wf2['A5'][ra]
        ods=[]
        origin_=origin3
        destination_=destination3
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
        airport_code_dest_list=list(set(airport_code_dest_list))
        print airport_code_ori_list, airport_code_dest_list
        print len(airport_code_ori_list), len(airport_code_dest_list)
        for m in airport_code_ori_list:
            for n in airport_code_dest_list:
                final_odlist.append(m + n)
        for pp in final_odlist:
            try:
                origin2 = pp[:3]
                destination2 = pp[3:]
                origin1, destination1, dep_date_start1, dep_date_end1, travel_type1, cabin1= ita_scraping(origin2,destination2,dep_date_start2, dep_date_end2,travel_type2,cabin2,"AED",A1,A2,A3,A4,A5)
                print "airlines_list: ",airlines_list
                driver.wait.until(
                        EC.visibility_of_element_located((By.XPATH, '//*[@id="contentwrapper"]/div[1]/div/div[6]/div[1]')))

                print time.sleep(50)
                driver.wait.until(EC.visibility_of_element_located((By.XPATH,'//*[@id="contentwrapper"]/div[1]/div/div[6]/div[1]')))
                print "page loaded"
                # driver.wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="contentwrapper"]/div[1]/div/div[5]/div/div/table/tbody/tr/td[2]/table/tbody/tr/td[2]/div/div/table/tbody/tr[1]/td[1]')))
                url2 = driver.current_url
                print "url2 : ",url2
                time.sleep(5)
                response = requests.get(url2)
                time.sleep(5)
                source = driver.page_source
                time.sleep(5)
                soup = BeautifulSoup(source, "lxml")
                time.sleep(5)
                # print soup
                time.sleep(5)
                # driver.wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="contentwrapper"]/div[1]/div/div[6]/div[1]')))
                time.sleep(5)
                airlines_bs = soup.find_all("td", {"class": "IR6M2QD-h-n"})
                time.sleep(5)
                print airlines_bs
                # pri )
                print airlines_list.values()
                if len(airlines_bs) !=0:
                    print "what!"
                    if len(airlines_list)!=0:
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
                                time.sleep(8)
                                xx = driver.find_element_by_xpath(
                                    '//*[@id="contentwrapper"]/div[1]/div/div[6]/div[4]/div[4]/div[1]/div/div[1]')
                                if "No" not in xx.text:
                                #clicking on selected airline
                                    driver.find_element_by_xpath('//*[@id="contentwrapper"]/div[1]/div/div[5]/div/div/table/tbody/tr/td[2]/table/tbody/tr/td[2]/div/div/table/tbody/tr[1]/td[' + str(
                                            i + 1) + ']/a').click()
                                    #clicking on 1st in the list
                                    print "////"
                                    time.sleep(8)
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
                                        time.sleep(25)
                                        driver.wait.until(EC.visibility_of_element_located((By.XPATH,'//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[1]/td[1]/div[2]/div[1]' )))

                                        conn_flights = driver.find_element_by_xpath(
                                            '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[1]/td[1]/div[2]/div[2]/table[1]/tbody')
                                        conn_on = conn_flights.find_elements_by_tag_name('tr')
                                        #if connection is there:
                                        if len(conn_on) >= 3:
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
                                            #if no connections present
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
                                            od1 = ((itinerary1.split(" ")[kl] + itinerary1.split(" ")[kt]).split("(")[1]).split(")")[0] + \
                                              ((itinerary1.split(" ")[kl] + itinerary1.split(" ")[kt]).split("(")[2]).split(")")[0]
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
                                            conn_flights = driver.find_element_by_xpath('//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[1]/td[1]/div[2]/div[2]/table[1]/tbody')
                                            conn_flights1=conn_flights.find_elements_by_tag_name('tr')
                                            print "--------",len(conn_flights1)

                                            itin = {}
                                            itin['itin_1'] = {}
                                            # itin['itin_2'] = {}
                                            itin['itin_1']['conn1'] = {"od": od1, "flightno": flightno1_1, "rbd": rbd1}

                                        #now return journey, if connecting flights are there
                                        if travel_type1 != "OW":
                                            try:
                                                conn_flights1 = driver.find_element_by_xpath(
                                                    '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[1]/td[1]/div[2]/div[2]/table[2]/tbody')
                                                conn_rt = conn_flights1.find_elements_by_tag_name('tr')
                                                if len(conn_rt) >= 3:
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
                                                    itin['itin_2']['conn1'] = {"od": od11, "flightno": flightno2_11, "rbd": rbd11}
                                                    itin['itin_2']['conn2'] = {"od": od22, "flightno": flightno2_22, "rbd": rbd22}
                                                    fare2_details = driver.find_element_by_xpath(
                                                        '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[3]/td/div/div/div[3]/table[1]/tbody/tr/td[1]/table/tbody/tr[2]/td/table/tbody/tr[2]').text
                                                    value2 = fare2_details.split(" ")[-1].split(")")[1]
                                                    if value2[3] == ".":
                                                        fare2_value = float(value2[4:].replace(",",""))
                                                    else:
                                                        fare2_value = float(value2[3:].replace(",",""))
                                                    Fare_2 = {}
                                                    Fare_2['farebasiscode'] = fare2_details.split(" ")[4]
                                                    Fare_2['od'] = fare2_details.split(" ")[5] + fare2_details.split(" ")[7]
                                                    Fare_2['value'] = fare2_value
                                                else:
                                                    #if no connections in return jurney
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

                                                    od2 = ((itinerary2.split(" ")[kl] + itinerary2.split(" ")[kt]).split("(")[1]).split(")")[0] + \
                                                          ((itinerary2.split(" ")[kl] + itinerary2.split(" ")[kt]).split("(")[2]).split(")")[0]
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
                                                    depdate2 = itinerary2.split(" ")[-3] + itinerary2.split(" ")[-2] + itinerary2.split(" ")[-1] + " " + \
                                                               itinerary2.split(" ")[-4]
                                                    itin['itin_2'] = {}
                                                    itin['itin_2']['conn1'] = {"od": od2, "flightno": flightno2_2, "rbd": rbd2}
                                                    fare2_details = driver.find_element_by_xpath(
                                                        '//*[@id="contentwrapper"]/div[1]/div/table/tbody/tr[3]/td/div/div/div[3]/table[1]/tbody/tr/td[1]/table/tbody/tr[2]/td/table/tbody/tr[2]').text
                                                    value2 = fare2_details.split(" ")[-1].split(")")[1]
                                                    if value2[3] == ".":
                                                        fare2_value = float(value2[4:].replace(",",""))
                                                    else:
                                                        fare2_value = float(value2[3:].replace(",",""))
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

                                                od2 = ((itinerary2.split(" ")[kl] + itinerary2.split(" ")[kt]).split("(")[1]).split(")")[0] + \
                                                      ((itinerary2.split(" ")[kl] + itinerary2.split(" ")[kt]).split("(")[2]).split(")")[0]
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
                                            fare1_value = float(value[4:].replace(",",""))
                                        else:
                                            fare1_value = float(value[3:].replace(",",""))
                                        # Fares = {}
                                        # Fares['Fare_1'] = {"carrier": fare1_details.split(" ")[3],
                                        #                    "farebasiscode": fare1_details.split(" ")[4],
                                        #                    "od": fare1_details.split(" ")[5] + fare1_details.split(" ")[7],
                                        #                    "value": fare1_value}

                                        Fare_1={}
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
                                        #removing currency name in value
                                        taxes_values = []
                                        if taxes_values_list[1][2] == ".":
                                            for i in range(len(taxes_values_list)):
                                                taxes_values.append(taxes_values_list[i][3:])
                                        else:
                                            for i in range(len(taxes_values_list)):
                                                taxes_values.append(taxes_values_list[i][2:])
                                        for k in range(len(taxes_values)):
                                            taxes_values[k] = float(taxes_values[k].replace(",", ""))

                                        # print taxes_fields_list
                                        # print taxes_values_list
                                        # taxes_fields_list.append('ZR')
                                        # for i in range(len(taxes_fields_list)):
                                        #     print i
                                        #     while i < len(taxes_fields_list):
                                        #         if taxes_fields_list[i + 1] == taxes_fields_list[i]:
                                        #             print "hcjjk-------",i
                                        # taxes = zip(taxes_fields_list,taxes_values_list)
                                        taxes = dict([(taxes_fields_list[i], taxes_values[i]) for i in range(len(taxes_fields_list))])
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
                                        print yr_value,yq_value
                                        print taxes['Subtotal per passenger']
                                        sub_total = float(taxes['Subtotal per passenger'])
                                        print "sub_total: ",sub_total
                                        taxes_new = {}
                                        for i in range(len(taxes)):
                                            if taxes_fields_list[i] != "YQ" and taxes_fields_list[i] != "YR" and taxes_fields_list[i] != "Subtotal per passenger":
                                                taxes_new['taxes_' + str(i) + ''] = {"code": taxes_fields_list[i], "value": taxes_values[i]}
                                        pos1 = origin1
                                        currency1 = "AED"
                                        if travel_type1 == "RT":
                                            data_doc = dict()
                                            Fares = {}
                                            Fares['Fare_1'] = Fare_1
                                            Fares['Fare_2']=Fare_2

                                            data_doc = {
                                                "origin":origin1,
                                                "destination":destination1,
                                                "od":origin1+destination1,
                                                "pos":pos1,
                                                "compartment":cabin1,
                                                "dep_date_start":datetime.datetime.strptime(dep_date_start1, "%m/%d/%Y").strftime("%Y-%m-%d"),
                                                "dep_date_end": datetime.datetime.strptime(dep_date_end1, "%m/%d/%Y").strftime("%Y-%m-%d"),
                                                "currency": currency1,
                                                "oneway/return": travel_type1,
                                                "airline": airline_code,
                                                # "itinerary_1":itinerary_1,
                                                # "itinerary_2":itinerary_2,
                                                "itinerary":itin,
                                                "Fares":Fares,
                                                # "fare_1":Fare_1,
                                                # "fare_2":Fare_2,
                                                "YQ": yq_value,
                                                "YR":yr_value,
                                                "subtotal": sub_total,
                                                "taxes":taxes_new,
                                                "updated_date":datetime.datetime.today().strftime("%Y-%m-%d")
                                            }
                                            print "data_doc: ",data_doc
                                        else:
                                            data_doc = dict()
                                            Fares = {}
                                            Fares['Fare_1'] = Fare_1
                                            data_doc = {
                                                "origin": origin1,
                                                "destination": destination1,
                                                "od": origin1 + destination1,
                                                "pos": pos1,
                                                "compartment": cabin1,
                                                "dep_date_start": datetime.datetime.strptime(dep_date_start1, "%m/%d/%Y").strftime(
                                                    "%Y-%m-%d"),
                                                "dep_date_end": datetime.datetime.strptime(dep_date_end1, "%m/%d/%Y").strftime("%Y-%m-%d"),
                                                "currency": currency1,
                                                "oneway/return": travel_type1,
                                                "airline": airline_code,
                                                # "itinerary_1": itinerary_1,
                                                # "fare_1": Fare_1,
                                                "itinerary":itin,
                                                "Fares": Fares,
                                                "YQ": yq_value,
                                                "YR": yr_value,
                                                "subtotal": sub_total,
                                                "taxes": taxes_new,
                                                "updated_date":datetime.datetime.today().strftime("%Y-%m-%d")
                                            }
                                            print "data_doc: ", data_doc
                                        db['JUP_DB_ITA_YQYR'].update(data_doc,data_doc,upsert=True)
                                        print "Inserted in to Collection"
                                        driver.find_element_by_xpath(
                                            '//*[@id="contentwrapper"]/div[1]/div/div[1]/div[1]/table/tbody/tr/td[4]/div/a').click()
                                else:
                                    pass
                                        #//*[@id="contentwrapper"]/div[1]/div/div[1]/div[1]/table/tbody/tr/td[4]/div/a
            # driver.close()
            except Exception:
                print "Error in search parameters"
                print traceback.print_exc()
                pass
            # continue
    driver.close()
    driver.quit()
    # phantom.quit()

# ita_yqyr("RT","Y",{"skips":0,"limits":15})

# phantom.quit();
# import signal
#
# driver.service.process.send_signal(signal.SIGTERM) # kill the specific phantomjs child proc
# driver.quit()