"""
Author: Prathyusha Gontla
End date of developement: 2018-1-30
Code functionality:
             Scrapes Promotions from EK website and updates in JUP_DB_Promotions Collection.
             Checks with previous data and if new promotions is seen, raises trigger.

MODIFICATIONS LOG
    S.No                   :1
    Date Modified          :2018-01-12
    By                     :Prathyusha Gontla
    Modification Details   :Added Airport City Mapping, Integrated IP Masking

"""
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
    from requests.exceptions import ConnectionError
    from pymongo import UpdateOne
    from dateutil.parser import parse
    import datetime
    from datetime import timedelta
    #Connecting to Local Mongodb
    import pymongo
    import datetime
    now = datetime.datetime.now()
    from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoRuleChangeTrigger
    from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoDateChangeTrigger
    from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoFareChangeTrigger
    from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoNewPromotionTrigger
    from jupiter_AI.network_level_params import SYSTEM_TIME,SYSTEM_DATE
    from jupiter_AI import JUPITER_DB, client
    db = client[JUPITER_DB]
    # from pymongo import MongoClient
    # client = MongoClient("mongodb://analytics:KNjSZmiaNUGLmS0Bv2@13.92.251.7:42525/")
    # db = client.fzDB_stg

    cursor_OD = list(db.JUP_DB_OD_Master.aggregate([{'$project': {'_id': 0, 'OD': 1}}]))
    print "Taken OD_Master"
    city_airport = list(db.JUP_DB_City_Airport_Mapping.aggregate([{'$project': {'_id':0, 'City_Code':1, 'Airport_Code':1}}]))
    print "Taken City_Airport"
    # chrome_path = r"/home/prathyusha/Downloads/chromedriver"
    # driver = webdriver.Chrome(chrome_path)
    # driver = webdriver.PhantomJS()
    pg = 0
    from bs4 import BeautifulSoup
    import requests

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


    @measure(JUPITER_LOGGER)
    def more_promos1():
        PHANTOMJS_ARGS = [
            "--proxy="+str(ips_list[pg])+"",
            "--proxy-type=http,https",
        ]
        driver = webdriver.PhantomJS(service_args=PHANTOMJS_ARGS)
        driver.wait = WebDriverWait(driver, 5)

        wait = WebDriverWait(driver, 20)
        country = ["af","at", "bh", "bd","bg", "cz", "eg", "et","ir","iq","jo", "kw","lb", "om", "qa", "ro", "sd", "tz", "th", "tr", "ug", "ua", "ae"]
        # country = ["bh"]
        t=0
        bulk_update_doc = []

        for each_country in country:
            count = 0
            url = "https://www.emirates.com/"+each_country+"/english/book/featured-fares/"
            driver.get(url)
            print(url)
            time.sleep(5)
            try:
                check_offers = driver.find_element_by_xpath(
                    '//*[@id="content"]/div/div/div[4]/div[2]/div/div[6]/div/div[1]/div/p')
                if "Sorry" in check_offers.text:
                    print("No offers")
                else:
                    print "Something wrong"
            except:  # //*[@id="content"]/div/div/div[4]/div[2]/div/div[6]/div/div/div[2]/div/card[1]/div/div[3]
                wait.until(EC.visibility_of_element_located(
                    (By.XPATH, ('//*[@id="content"]/div/div/div[4]/div[2]/div/div[6]/div/div/div[2]/div/card[1]/div/div[3]'))))
                print "step1"
                prom_table_list = ['//*[@id="content"]/div/div/div[4]/div[2]/div/div[6]/div/div/div[2]/div',
                                   '//*[@id="content"]/div/div/div[4]/div[2]/div/div[6]/div/div/div[3]']
                prom_types_list = ["card--ff", "ng-scope"]
                for m, n in zip(prom_table_list, prom_types_list):
                    print m, n
                    promotion_table = driver.find_element_by_xpath(m)
                    lists = promotion_table.find_elements_by_class_name(n)
                    for each_list in lists[:30]:
                        if len(each_list.text.splitlines()) > 10:
                            content = each_list.text.splitlines()
                            loop = []

                            for j in content:
                                if "Business Class" in j:
                                    loop.append("Business Class")
                                if "Economy Class" in j:
                                    loop.append("Economy Class")
                            loop = set(loop)
                            print "loop: ", loop
                            for l in loop:
                                print "class running: --- ", l
                                print "content: ", content
                                for i in content:
                                    if "flights from" in i:
                                        origin = i.split(" ")[5].strip("()")
                                        print "origin: ", origin
                                        destination = i.split(" ")[-1].strip("()")
                                        print "destination: ", destination
                                    if l in i:
                                        print i
                                        classes = i.split(" ")[0]
                                        print "class------------", classes
                                        if classes == "Economy":
                                            classes = "Y"
                                        elif classes == "Business":
                                            classes = "J"
                                        elif classes == "First":
                                            classes = "F"
                                        else:
                                            classes = "Y"
                                        fare = i.split(" ")[5].strip("*")
                                        currency = i.split(" ")[6]
                                        print "class: ", classes
                                        print "fare: ", fare
                                        print "currency: ", currency
                                    if "travel between" in i:
                                        travel = i.split(" ")
                                        dt = parse(travel[3] + " " + travel[4] + " " + travel[5])
                                        startdate = dt.strftime('%Y-%m-%d')
                                        print("dep_date_from : ", startdate)
                                        dt = parse(travel[7] + " " + travel[8] + " " + travel[9])
                                        enddate = dt.strftime('%Y-%m-%d')
                                        print("dep_date_to : ", enddate)
                                    if "trip" in i:
                                        types = i.split(" ")[0]
                                        print "type: ", types
                                    if "expires" in i:
                                        if "hours" in i or "hour(s)" in i:
                                            hours = i.split(" ")[3]
                                            valid_till = now + datetime.timedelta(hours=int(hours))
                                            valid_till = valid_till.strftime('%Y-%m-%d')
                                        elif "days" in i:
                                            days = i.split(" ")[3]
                                            valid_till = now + datetime.timedelta(days=int(days))
                                            valid_till = valid_till.strftime('%Y-%m-%d')
                                        print("valid_till :", valid_till)
                                data_doc = {
                                    "Airline": "EK",
                                    "OD_1": origin + destination,
                                    "OD": origin + destination,
                                    "Valid from": '',
                                    "Valid till": valid_till,
                                    "compartment": classes,
                                    "Fare": fare,
                                    "Oneway/ Return": types,
                                    "Currency": currency,
                                    "Minimum Stay": '',
                                    "Maximum Stay": '',
                                    "dep_date_from": startdate,
                                    "dep_date_to": enddate,
                                    "Url": url
                                }
                                doc = {
                                    "Airline": "EK",
                                    "OD_1": origin + destination,
                                    "OD": origin + destination,
                                    "Valid from": '',
                                    "Valid till": valid_till,
                                    "Oneway/ Return": types,
                                    "compartment": classes,
                                    "Fare": fare,
                                    "Currency": currency,
                                    "Minimum Stay": '',
                                    "Maximum Stay": '',
                                    "dep_date_from": startdate,
                                    "dep_date_to": enddate,
                                    "Url": url,
                                    "Last Updated Date": SYSTEM_DATE,
                                    "Last Updated Time": SYSTEM_TIME
                                }

                                # db.JUP_DB_Promotions.update(data_doc, doc, upsert=True)

                                print "data_doc:", data_doc
                                print "doc:", doc
                                print "1---yes"
                                a = 0
                                airport_code_ori_list = []
                                airport_code_dest_list = []
                                for l in city_airport:
                                    if origin == l['Airport_Code']:
                                        origin = l['City_Code']
                                    else:
                                        pass
                                        # city_code_ori_list.append(l['City_Code'])
                                    if destination == l['Airport_Code']:
                                        destination = l['City_Code']
                                    else:
                                        pass
                                print origin, destination

                                # city_code_dest_list.append(l['City_Code'])
                                for k in city_airport:
                                    if origin == k['City_Code']:
                                        airport_code_ori_list.append(k['Airport_Code'])
                                    if destination == k['City_Code']:
                                        airport_code_dest_list.append(k['Airport_Code'])
                                print airport_code_ori_list, airport_code_dest_list
                                print len(airport_code_ori_list), len(airport_code_dest_list)

                                if origin == "VIE":
                                    airport_code_ori_list = ['XWC']
                                if destination == "VIE":
                                    airport_code_dest_list = ['XWC']
                                if origin == "UAE":
                                    airport_code_ori_list = ['DWC', 'DXB']
                                if destination == "UAE":
                                    airport_code_dest_list = ['DXB', 'DWC']
                                if airport_code_ori_list == []:
                                    airport_code_ori_list = []
                                    airport_code_ori_list.append(data_doc['OD'][:3])
                                if airport_code_dest_list == []:
                                    airport_code_dest_list = []
                                    airport_code_dest_list.append(data_doc['OD'][3:])
                                print airport_code_ori_list, airport_code_dest_list
                                for m in range(len(airport_code_ori_list)):
                                    origin = airport_code_ori_list[m]
                                    for k in range(len(airport_code_dest_list)):
                                        destination = airport_code_dest_list[k]
                                        data_doc['OD'] = origin + destination
                                        doc['OD'] = origin + destination
                                        print data_doc['OD']
                                        print "data_doc: ", data_doc
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
                                                        {'$match': {'OD': data_doc['OD_1'], 'Airline': data_doc['Airline'],
                                                                    'Fare': data_doc['Fare'],
                                                                    'dep_date_from': data_doc['dep_date_from'],
                                                                    'dep_date_to': data_doc['dep_date_to']}}, {'$project': {'_id': 0}},
                                                        {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
                                                    # cursor = json.dump(cursor)
                                                    print cursor
                                                    # print "data_doc: ", data_doc
                                                    if len(cursor) > 0:
                                                        for i in range(len(cursor)):
                                                            if (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                                                                        cursor[i]['Currency'] == data_doc['Currency']) and (
                                                                cursor[i]['Maximum Stay'] == data_doc['Maximum Stay']) \
                                                                    and (cursor[i]['Minimum Stay'] == data_doc['Minimum Stay']) \
                                                                    and (cursor[i]['Oneway/ Return'] == data_doc['Oneway/ Return']) \
                                                                    and (
                                                                                cursor[i]['Valid till'] == data_doc['Valid till']) \
                                                                    and (
                                                                                cursor[i]['compartment'] == data_doc['compartment']):
                                                                pass
                                                            else:
                                                                pass
                                                            if (cursor[i]['Valid from'] != data_doc['Valid from']) or (
                                                                cursor[i]['Maximum Stay'] != data_doc['Maximum Stay']) \
                                                                    or (cursor[i]['Minimum Stay'] != data_doc['Minimum Stay']) \
                                                                    or (cursor[i]['Oneway/ Return'] != data_doc['Oneway/ Return']) or (
                                                                        cursor[i]['Valid till'] != data_doc['Valid till']) \
                                                                    or (
                                                                                cursor[i]['Currency'] != data_doc['Currency']) \
                                                                    or (
                                                                                cursor[i]['compartment'] != data_doc['compartment']):
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
                                                        {'$match': {'OD': data_doc['OD_1'], 'Airline': data_doc['Airline'],
                                                                    'compartment': data_doc['compartment'],
                                                                    'Currency': data_doc['Currency'],
                                                                    'Fare': data_doc['Fare'], 'Maximum Stay': data_doc['Maximum Stay'],
                                                                    'Minimum Stay': data_doc['Minimum Stay'],
                                                                    'Oneway/ Return': data_doc['Oneway/ Return'],
                                                                    }}, {'$project': {'_id': 0}},
                                                        {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
                                                    # cursor = json.dump(cursor)
                                                    print cursor
                                                    if len(cursor) > 0:
                                                        for i in range(len(cursor)):

                                                            if (cursor[i]['dep_date_from'] == data_doc['dep_date_from']) and (
                                                                        cursor[i]['dep_date_to'] == data_doc['dep_date_to']):
                                                                pass
                                                            else:
                                                                pass
                                                            if (cursor[i]['dep_date_from'] != data_doc['dep_date_from']) or (
                                                                cursor[i]['Valid from'] != data_doc['Valid from']) or (
                                                                        cursor[i]['dep_date_to'] != data_doc['dep_date_to']) or (
                                                                cursor[i]['Valid till'] != data_doc['Valid till']):
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
                                                        {'$match': {'OD': data_doc['OD_1'], 'Airline': data_doc['Airline'],
                                                                    'compartment': data_doc['compartment'],
                                                                    'Currency': data_doc['Currency'],
                                                                    'Valid from': data_doc['Valid from'],
                                                                    'dep_date_from': data_doc['dep_date_from'],
                                                                    'dep_date_to': data_doc['dep_date_to'],
                                                                    'Oneway/ Return': data_doc['Oneway/ Return'],
                                                                    'Valid till': data_doc['Valid till'],
                                                                    'Maximum Stay': data_doc['Maximum Stay'],
                                                                    'Minimum Stay': data_doc['Minimum Stay']
                                                                    }}, {'$project': {'_id': 0}},
                                                        {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
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
                                                                    old_doc_data=cursor[i],
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
                                                        {'$match': {'Airline': data_doc['Airline']}}, {'$project': {'_id': 0}}, ]))
                                                    print "cursor:", cursor
                                                    print "len of cursor:", len(cursor)
                                                    if len(cursor) > 0:

                                                        for i in range(len(cursor)):
                                                            change_flag_list = []
                                                            if (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                                                                        cursor[i]['Valid till'] == data_doc['Valid till']) \
                                                                    and (
                                                                                                                cursor[i][
                                                                                                                    'compartment'] ==
                                                                                                                data_doc[
                                                                                                                    'compartment'] and (
                                                                                                            cursor[i]['Minimum Stay'] ==
                                                                                                            data_doc['Minimum Stay'])
                                                                                                    and (cursor[i]['OD'] == data_doc[
                                                                                                        'OD_1']) and (
                                                                                                    cursor[i]['Maximum Stay'] ==
                                                                                                    data_doc['Maximum Stay']) and (
                                                                                                        cursor[i]['Fare'] == data_doc[
                                                                                                        'Fare']) and (
                                                                                            cursor[i]['Oneway/ Return'] == data_doc[
                                                                                            'Oneway/ Return']) and (
                                                                                                cursor[i]['Currency'] == data_doc[
                                                                                                'Currency']) and (
                                                                                            cursor[i]['dep_date_from'] == data_doc[
                                                                                            'dep_date_from']) and (
                                                                                        cursor[i]['dep_date_to'] == data_doc[
                                                                                        'dep_date_to'])):
                                                                pass
                                                            else:
                                                                pass

                                                            if (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                                                                        cursor[i]['Valid till'] == data_doc['Valid till']) \
                                                                    and (
                                                                                                    cursor[i]['compartment'] ==
                                                                                                    data_doc['compartment']
                                                                                            and (cursor[i]['OD'] != data_doc[
                                                                                                'OD_1']) and (
                                                                                                    cursor[i]['Fare'] == data_doc[
                                                                                                    'Fare']) and (
                                                                                                cursor[i]['Currency'] == data_doc[
                                                                                                'Currency']) and (
                                                                                            cursor[i]['dep_date_from'] == data_doc[
                                                                                            'dep_date_from']) and (
                                                                                        cursor[i]['dep_date_to'] == data_doc[
                                                                                        'dep_date_to'])):
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
                                                            if cursor[i]['compartment'] != data_doc['compartment']:
                                                                change_flag_list.append(5)
                                                            else:
                                                                pass
                                                            if cursor[i]['OD'] != data_doc['OD_1']:
                                                                change_flag_list.append(6)
                                                            else:
                                                                pass
                                                            if cursor[i]['Fare'] != data_doc['Fare']:
                                                                change_flag_list.append(7)
                                                            else:
                                                                pass
                                                            if cursor[i]['Maximum Stay'] != data_doc['Maximum Stay']:
                                                                change_flag_list.append(7)
                                                            else:
                                                                pass
                                                            if cursor[i]['Minimum Stay'] != data_doc['Minimum Stay']:
                                                                change_flag_list.append(7)
                                                            else:
                                                                pass
                                                            if cursor[i]['Oneway/ Return'] != data_doc['Oneway/ Return']:
                                                                change_flag_list.append(7)
                                                            else:
                                                                pass
                                                            if cursor[i]['Currency'] != data_doc['Currency']:
                                                                change_flag_list.append(8)
                                                            else:
                                                                pass
                                                            if cursor[i]['dep_date_from'] != data_doc['dep_date_from']:
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
                                                                promo_new_promotion_trigger = PromoNewPromotionTrigger("new_promotions",
                                                                                                                       old_doc_data=
                                                                                                                       cursor[i],
                                                                                                                       new_doc_data=doc,
                                                                                                                       changed_field="new")
                                                                # print "new_doc_data", new_doc_data
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

                                        if t == 2:
                                            st = time.time()
                                            print "updating: ", count

                                            # print bulk_update_doc
                                            db['JUP_DB_Promotions'].bulk_write(bulk_update_doc)
                                            print "updated!", time.time() - st
                                            bulk_list = []
                                            bulk_update_doc.append(UpdateOne(data_doc, {"$set": doc}, upsert=True))
                                            count += 1
                                            t = 0
                                            # driver.execute_script("window.scrollTo(0,600);")
                                            print "yes---------------------->"
                                        else:
                                            bulk_update_doc.append(UpdateOne(data_doc, {"$set": doc}, upsert=True))
                                            # print bulk_update_doc
                                            t += 1
                                            print "t= :", t

                                    continue

        driver.close()
        driver.quit()

    try:
        more_promos1()

    except:
    # print traceback.print_exc()
        pg += 1
        print "pg: ", pg
        try:
          more_promos1()
        except:
          pg+=1
          print "pg: ", pg
          try:
            more_promos1()
          except:
            pg += 1
            print "pg: ", pg
            try:
              more_promos1()
            except:
              pg += 1
              print "pg: ", pg
              try:
                more_promos1()
              except:
                pg += 1
                print "pg: ", pg
                try:
                  more_promos1()
                except:
                  pg += 1
                  print "pg: ", pg
                  try:
                    more_promos1()
                  except:
                    pg += 1
                    print "pg: ", pg
                    try:
                      more_promos1()
                    except:
                      pg += 1
                      print "pg: ", pg
                      try:
                        more_promos1()
                      except:
                        pg += 1
                        print "pg: ", pg
                        try:
                          more_promos1()
                        except:
                          pg += 1
                          print "pg: ", pg
                          try:
                            more_promos1()
                          except:
                            pg += 1
                            print "pg: ", pg
                            try:
                              more_promos1()
                            except:
                              pg += 1
                              print "pg: ", pg
                              try:
                                more_promos1()
                              except:
                                pg += 1
                                print "pg: ", pg
                                more_promos1()
                                pass



if __name__ == "__main__":
    run()

