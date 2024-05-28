#full website has been changed! edit from dynamic dropdown - origin!
from jupiter_AI import JUPITER_LOGGER
from jupiter_AI.logutils import measure


@measure(JUPITER_LOGGER)
def run():
    from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoRuleChangeTrigger
    from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoDateChangeTrigger
    from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoFareChangeTrigger
    from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoNewPromotionTrigger
    import pandas as pd
    import time
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.keys import Keys
    from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
    from selenium.webdriver.support.ui import Select
    from dateutil.parser import parse
    from jupiter_AI.network_level_params import SYSTEM_DATE, today, INF_DATE_STR
    import datetime
    import os

    #Connection to MongoDB
    import pymongo
    client = pymongo.MongoClient("mongodb://analytics:KNjSZmiaNUGLmS0Bv2@13.92.251.7:42525/")
    db = client['fzDB_stg']

    #Opening driver
    #chrome_path = r"/home/prathyusha/Downloads/Projects Codes/Promotions/chromedriver"
    #chrome_path = r"/var/www/html/jupiter/python/jupiter_AI/batch/Promotions_Scraping/chromedriver"
    #driver = webdriver.Firefox()
    #driver = webdriver.Chrome("/usr/lib/chromium-browser/chromedriver")
    #os.environ["webdriver.chrome.driver"] = chrome_path
    driver = webdriver.PhantomJS()
   # driver = webdriver.PhantomJS()
    driver.wait = WebDriverWait(driver, 5)


    list_region = ['HK','AU','BH','BD','KH','CA','CN','DK','DE','IN','ID','IL','JP','KR','MY','MV','MM','NL','NP','PK','PH','ZA','QA','SG','SA','LK','TW','TH']
    website = "https://www.cathaypacific.com/cx/en_"

    #Scraping starts here
    for eachlist in list_region:
        print eachlist
        change_flag_list = []
        driver.get(website+eachlist+"/offers.html")
        print website+eachlist+"/offers.html"
        driver.wait
        dropdown = False

        #Get all promotions in url_box
        try:
            print "i am in"
            origin_box = driver.find_element_by_class_name("dynamic-dropdown")
            origin_list = origin_box.find_element_by_tag_name('div')
            print origin_list
            origin_list.click()
            origin_list = origin_list.find_element_by_tag_name('select')
            options = origin_list.find_elements_by_tag_name('option')
            url_box =[]
            for eachopt in options:
                data_url = eachopt.get_attribute('data-url')
                url = "https://www.cathaypacific.com"+data_url
                url_box.append(url)
            dropdown = True

        except Exception,e :
            print e
            driver.save_screenshot('screenshot.png')
            pass
        print "step2"

        # print url_box
        #Some links have options to choose from cities, if they have
        if dropdown:
            print "step3"
            for eachurl in url_box:
                driver.get(eachurl)
                print eachurl
                #Code to get all offers on page
                var1 = True
                while var1:
                    try:
                        driver.wait.until(EC.visibility_of_element_located(
                            (By.XPATH,"/html/body/main/div/div[4]/div/div/div[2]/a")))
                        explore = driver.find_element_by_xpath("/html/body/main/div/div[4]/div/div/div[2]/a")
                        explore.click()
                        print 'tried'
                    except Exception, e:
                        print e
                        var1 = False


                #Scraping data starts from here
                container = driver.find_elements_by_tag_name('ul')
                for eachcon in container:

                    if 'equal-height offers-display' in eachcon.get_attribute('class'):
                        print 'find qual-height offers-display'
                        link_list = eachcon.find_elements_by_tag_name('li')
                        for eachelem in link_list:
                            if 'flights' in eachelem.get_attribute('data-offer-type'):
                                #Initializing all columns
                                Origin = "Origin"
                                Destination = ""
                                OD = ""
                                Valid_from = ""
                                Valid_till = ""
                                Type = ""
                                Compartment = ""
                                Fare = ""
                                Currency = ""
                                Start_date = ""
                                End_date = ""
                                url = ""
                                dat = eachelem.find_element_by_tag_name('a')
                                url =  dat.get_attribute('href')
                                print 'url1:', url
                                #Code to get Origin
                                try:
                                    Origin  = dat.get_attribute('data-cxlinktag-offer-origin')
                                    try:
                                        cursor = db.JUP_DB_IATA_Codes.find()
                                        for i in cursor:
                                            if all(word in i['City'].lower() for word in re.findall(r'\w+',Origin.lower())):
                                                Origin = i['Code']
                                    except:
                                        pass
                                    print 'Origin1:', Origin
                                except:
                                    pass

                                #Compartment code
                                try:
                                    Compartment = dat.get_attribute('data-cxlinktag-offer-cabin')
                                    if 'C' in Compartment:
                                        Compartment = 'J'
                                    elif 'Y' in Compartment:
                                        Compartment = 'Y'
                                    print 'Compartment1:',Compartment
                                except:
                                    pass

                                #Code to get validity dates
                                try:
                                    Valid_from = dat.get_attribute('data-cxlinktag-offer-date-start')
                                    print 'Valid_from1:',Valid_from
                                except:
                                    pass
                                #Code to get travelling dates
                                content = eachelem.find_element_by_class_name('content')
                                try:
                                    period = content.find_element_by_class_name('period')
                                    span = period.find_elements_by_tag_name('span')
                                    Start_date = span[0].text
                                    dt = parse(Start_date)
                                    Start_date = dt.strftime('%Y-%m-%d')
                                    print 'Start_date1:', Start_date
                                    End_date = span[1].text
                                    dt = parse(End_date)
                                    End_date = dt.strftime('%Y-%m-%d')
                                    print 'End_date1:', End_date
                                except:
                                    pass

                                #Code to get Destination
                                try:
                                    Destination = content.find_element_by_class_name('destination').text
                                    try:
                                        cursor = db.JUP_DB_IATA_Codes.find()
                                        for i in cursor:
                                            if all(word in i['City'].lower() for word in re.findall(r'\w+',Destination.lower())):
                                                Destination = i['Code']
                                    except:
                                        pass
                                    print 'Destination1:',Destination
                                except:
                                    pass

                                #Code to get fares and Currency
                                try:
                                    fc = content.find_element_by_class_name('fare')
                                    fare_curr = fc.find_element_by_tag_name('b')
                                    fare_curr = fare_curr.find_element_by_class_name('price').text
                                    Currency  = filter(str.isalpha, str(fare_curr))
                                    Fare = filter(str.isdigit,str(fare_curr))
                                    print 'Fare11:',Fare
                                    print 'Currency11:',Currency
                                except:
                                    pass

                                #Collection
                                doc = {
                                  "Airline" : "CX",
                                  "Origin" : Origin,
                                  "Destination" : Destination,
                                  "OD" : Origin + Destination,
                                  "Valid from" : Valid_from,
                                  "Valid till" : "",
                                  "Type" : "",
                                  "Compartment" : Compartment,
                                  "Fare" : Fare,
                                  "Currency":Currency,
                                  "dep_date_from" : Start_date,
                                  "dep_date_to" : End_date,
                                  "Url" : url,
                                  "Last Update": time.strftime('%Y-%m-%d'),
                                  "Last updated Time":time.strftime('%H')
                                   }
                                data_doc = {
                                   "Airline" : "CX",
                                   "Origin" : Origin,
                                   "Destination" : Destination,
                                   "OD" : Origin + Destination,
                                   "Valid from" : Valid_from,
                                   "Valid till" : "",
                                   "Type" : "",
                                   "Compartment" : Compartment,
                                   "Fare" : Fare,
                                   "Currency":Currency,
                                   "dep_date_from" : Start_date,
                                   "dep_date_to" : End_date,
                                   "Url" : url,
                                    }
                                print "data-doc: "
                                print data_doc
                                print "doc-"
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
                                        print("Raise New Promotion Released Trigger")
                                        promo_new_promotion_trigger = PromoNewPromotionTrigger(
                                            "new_promotions",
                                            old_doc_data=cursor[0],
                                            new_doc_data=doc,
                                            changed_field="Rules")
                                        promo_new_promotion_trigger.do_analysis()


                                @measure(JUPITER_LOGGER)
                                def date_change():
                                    print "date_range"
                                    print data_doc
                                    cursor = list(db.JUP_DB_Promotions.aggregate([
                                        {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                                                    'Compartment': data_doc['Compartment'],
                                                    'Currency': data_doc['Currency'],
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
                                        print("Raise New Promotion Released Trigger")
                                        promo_new_promotion_trigger = PromoNewPromotionTrigger(
                                            "new_promotions",
                                            old_doc_data=cursor[0],
                                            new_doc_data=doc,
                                            changed_field="Dates")
                                        promo_new_promotion_trigger.do_analysis()


                                @measure(JUPITER_LOGGER)
                                def fare_change():
                                    print "fare_range"
                                    cursor = list(db.JUP_DB_Promotions.aggregate([
                                        {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                                                    'Compartment': data_doc['Compartment'],
                                                    'Currency': data_doc['Currency'],
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
                                        print("Raise New Promotion Released Trigger")
                                        promo_new_promotion_trigger = PromoNewPromotionTrigger("new_promotions",
                                                                                               old_doc_data=cursor[0],
                                                                                               new_doc_data=doc,
                                                                                               changed_field="")
                                        promo_new_promotion_trigger.do_analysis()


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
                                                                                    cursor[i]['Compartment'] ==
                                                                                    data_doc['Compartment']
                                                                            and (
                                                                                cursor[i]['OD'] == data_doc['OD']) and (
                                                                                    cursor[i]['Fare'] == data_doc[
                                                                                    'Fare']) and (
                                                                                cursor[i]['Currency'] == data_doc[
                                                                                'Currency']) and (
                                                                            cursor[i]['dep_date_from'] == data_doc[
                                                                            'dep_date_from']) and (
                                                                        cursor[i]['dep_date_to'] == data_doc[
                                                                        'dep_date_to'])):
                                                pass

                                            elif (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                                                        cursor[i]['Valid till'] == data_doc['Valid till']) \
                                                    and (
                                                                                    cursor[i]['Compartment'] ==
                                                                                    data_doc['Compartment']
                                                                            and (
                                                                                cursor[i]['OD'] == data_doc['OD']) and (
                                                                                    cursor[i]['Fare'] == data_doc[
                                                                                    'Fare']) and (
                                                                                cursor[i]['Currency'] != data_doc[
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
                                                    changed_field="Currency")
                                                promo_new_promotion_trigger.do_analysis()

                                            elif (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                                                        cursor[i]['Valid till'] == data_doc['Valid till']) \
                                                    and (
                                                                                    cursor[i]['Compartment'] ==
                                                                                    data_doc['Compartment']
                                                                            and (
                                                                                cursor[i]['OD'] != data_doc['OD']) and (
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

                                            elif (cursor[i]['Valid from'] != data_doc['Valid from']) and (
                                                        cursor[i]['Valid till'] != data_doc['Valid till']) \
                                                    and (
                                                                                    cursor[i]['Compartment'] !=
                                                                                    data_doc['Compartment'] and (
                                                                                        cursor[i]['OD'] != data_doc[
                                                                                        'OD']) and (
                                                                                    cursor[i]['Fare'] != data_doc[
                                                                                    'Fare']) and (
                                                                                cursor[i]['Currency'] != data_doc[
                                                                                'Currency']) and (
                                                                            cursor[i]['dep_date_from'] != data_doc[
                                                                            'dep_date_from']) and (
                                                                        cursor[i]['dep_date_to'] != data_doc[
                                                                        'dep_date_to'])):
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

                                db.JUP_DB_Promotions.update(data_doc,doc,upsert = True)
                                print 'yes'

        #if link does not have option to choose, code starts from here
        else:
            print "steps---2"
            # var1 = True
            # while var1:
            try:
                driver.wait.until(EC.visibility_of_element_located(
                    (By.XPATH,"/html/body/main/div/div[4]/div/div/div[2]/a")))
                explore = driver.find_element_by_xpath("/html/body/main/div/div[4]/div/div/div[2]/a")
                explore.click()
                print 'tried'
            except Exception, e:
                print e
                print "jcbkd--"
                    # var1 = False

            print "next step"
            container = driver.find_elements_by_tag_name('ul')
            print container
            for eachcon in container:
                if 'equal-height offers-display' in eachcon.get_attribute('class'):
                    print 'find qual-height offers-display'
                    link_list = eachcon.find_elements_by_tag_name('li')
                    for eachelem in link_list:
                        if 'flights' in eachelem.get_attribute('data-offer-type'):
                            print "in if loop"
                            Origin = "Origin"
                            Destination = ""
                            OD = ""
                            Valid_from = ""
                            Valid_till = ""
                            Type = ""
                            Compartment = ""
                            Fare = ""
                            Currency = ""
                            Start_date = ""
                            End_date = ""
                            url = ""


                            dat = eachelem.find_element_by_tag_name('a')
                            url = dat.get_attribute('href')
                            print 'url2:', url
                            try:
                                Origin  = dat.get_attribute('data-cxlinktag-offer-origin')
                                try:
                                    cursor = db.JUP_DB_IATA_Codes.find()
                                    for i in cursor:
                                        if all(word in i['City'].lower() for word in re.findall(r'\w+',Origin.lower())):
                                            Origin = i['Code']
                                except:
                                    pass
                                print 'Origin2:', Origin
                            except:
                                pass

                            try:
                                Compartment = dat.get_attribute('data-cxlinktag-offer-cabin')
                                if 'C' in Compartment:
                                    Compartment = 'J'
                                elif 'Y' in Compartment:
                                    Compartment = 'Y'
                                print 'Compartment2:',Compartment
                            except:
                                pass

                            try:
                                Valid_from = dat.get_attribute('data-cxlinktag-offer-date-start')
                                print 'Valid_from2:',Valid_from
                            except:
                                pass

                            content = eachelem.find_element_by_class_name('content')
                            try:
                                period = content.find_element_by_class_name('period')
                                span = period.find_elements_by_tag_name('span')
                                Start_date = span[0].text
                                dt = parse(Start_date)
                                Start_date = dt.strftime('%Y-%m-%d')
                                print 'Start_date2:', Start_date
                                End_date = span[1].text
                                dt = parse(End_date)
                                End_date = dt.strftime('%Y-%m-%d')
                                print 'End_date2:', End_date
                            except:
                                pass

                            try:
                                Destination = content.find_element_by_class_name('destination').text
                                try:
                                    cursor = db.JUP_DB_IATA_Codes.find()
                                    for i in cursor:
                                        if all(word in i['City'].lower() for word in re.findall(r'\w+',Destination.lower())):
                                            Destination = i['Code']
                                except:
                                    pass
                                print 'Destination2:',Destination
                            except:
                                pass


                            try:
                                fc = content.find_element_by_class_name('fare')
                                fare_curr = fc.find_element_by_tag_name('b')
                                fare_curr = fare_curr.find_element_by_class_name('price').text
                                Currency  = filter(str.isalpha, str(fare_curr))
                                Fare = filter(str.isdigit,str(fare_curr))
                                print 'Fare21:',Fare
                                print 'Currency21:',Currency
                            except:
                                pass

                            doc = {
                              "Airline" : "CX",

                              "OD" : Origin + Destination,
                              "Valid from" : Valid_from,
                              "Valid till" : "",
                              "Type" : "",
                              "Compartment" : Compartment,
                              "Fare" : Fare,
                              "Currency":Currency,
                              "dep_date_from" : Start_date,
                              "dep_date_to" : End_date,
                              "Url" : url,
                              "Last updated Date": time.strftime('%Y-%m-%d'),
                              "Last updated Time":time.strftime('%H')
                               }
                            data_doc = {
                               "Airline" : "CX",

                               "OD" : Origin + Destination,
                               "Valid from" : Valid_from,
                               "Valid till" : "",
                               "Type" : "",
                               "Compartment" : Compartment,
                               "Fare" : Fare,
                               "Currency":Currency,
                               "dep_date_from" : Start_date,
                               "dep_date_to" : End_date,
                               "Url" : url,
                                }
                            print "data-doc: "
                            print data_doc
                            print "doc-"
                            print doc
                            x = pd.DataFrame()


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
                                                'Compartment': data_doc['Compartment'],
                                                'Currency': data_doc['Currency'],
                                                'Fare': data_doc['Fare'], 'Valid from': data_doc['Valid from'],
                                                'Valid till': data_doc['Valid till'],
                                                }},
                                    {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
                                # cursor = json.dump(cursor)
                                print cursor
                                if len(cursor) > 0:

                                    if True:
                                        try:
                                            (cursor[0]['dep_date_from'] == data_doc['dep_date_from']) and (
                                                cursor[0]['dep_date_to'] == data_doc['dep_date_to'])
                                        except KeyError:
                                            if dict(dep_date_start="", dep_date_end=""):
                                                dep_date_end_object = today + datetime.timedelta(days=90)
                                                dep_date_end = datetime.datetime.strftime(dep_date_end_object,
                                                                                          '%Y-%m-%d')

                                                dep_date_start = SYSTEM_DATE
                                            else:
                                                pass
                                    else:
                                        pass
                                    if True:
                                        try:
                                            if(cursor[0]['dep_date_from'] != data_doc['dep_date_from']) and (
                                                cursor[0]['dep_date_to'] == data_doc['dep_date_to']):

                                                print "Raise Date Change Trigger (dep date from is updated)"  ### Call the trigger
                                                promo_date_change_trigger = PromoDateChangeTrigger("promotions_dateschange",
                                                                                                       old_doc_data=cursor[0],
                                                                                                       new_doc_data=doc,
                                                                                                       changed_field="dep_date_from")
                                                promo_date_change_trigger.do_analysis()
                                        except KeyError:
                                            if dict(dep_date_start="", dep_date_end=""):
                                                dep_date_end_object = today + datetime.timedelta(days=90)
                                                dep_date_end = datetime.datetime.strftime(dep_date_end_object,
                                                                                          '%Y-%m-%d')

                                                dep_date_start = SYSTEM_DATE

                                    if True:
                                        try:
                                            if (cursor[0]['dep_date_from'] == data_doc['dep_date_from']) and (
                                                        cursor[0]['dep_date_to'] != data_doc['dep_date_to']):
                                                print "Raise Date Change Trigger (dep date from is updated)"  ### Call the trigger
                                                promo_date_change_trigger = PromoDateChangeTrigger(
                                                    "promotions_dateschange",
                                                    old_doc_data=cursor[0],
                                                    new_doc_data=doc,
                                                    changed_field="dep_date_from")
                                                promo_date_change_trigger.do_analysis()
                                        except KeyError:
                                            if dict(dep_date_start="", dep_date_end=""):
                                                dep_date_end_object = today + datetime.timedelta(days=90)
                                                dep_date_end = datetime.datetime.strftime(dep_date_end_object,
                                                                                          '%Y-%m-%d')

                                                dep_date_start = SYSTEM_DATE
                                else:
                                    pass


                            @measure(JUPITER_LOGGER)
                            def fare_change():
                                print "fare_range"
                                cursor = list(db.JUP_DB_Promotions.aggregate([
                                    {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                                                'Compartment': data_doc['Compartment'],
                                                'Currency': data_doc['Currency'],
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
                            try:


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
                                                                                    cursor[i]['Compartment'] == data_doc[
                                                                                    'Compartment']
                                                                            and (cursor[i]['OD'] == data_doc['OD']) and (
                                                                                    cursor[i]['Fare'] == data_doc[
                                                                                    'Fare']) and (
                                                                                cursor[i]['Currency'] == data_doc[
                                                                                'Currency']) and (
                                                                            cursor[i]['dep_date_from'] == data_doc[
                                                                            'dep_date_from']) and (
                                                                        cursor[i]['dep_date_to'] == data_doc[
                                                                        'dep_date_to'])):
                                                pass

                                            elif (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                                                        cursor[i]['Valid till'] == data_doc['Valid till']) \
                                                    and (
                                                                                    cursor[i]['Compartment'] == data_doc[
                                                                                    'Compartment']
                                                                            and (cursor[i]['OD'] == data_doc['OD']) and (
                                                                                    cursor[i]['Fare'] == data_doc[
                                                                                    'Fare']) and (
                                                                                cursor[i]['Currency'] != data_doc[
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
                                                    changed_field="Currency")
                                                promo_new_promotion_trigger.do_analysis()

                                            if True:
                                                try:
                                                    (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                                                        cursor[i]['Valid till'] == data_doc['Valid till']) \
                                                    and (
                                                                                    cursor[i]['Compartment'] == data_doc[
                                                                                    'Compartment']
                                                                            and (cursor[i]['OD'] != data_doc['OD']) and (
                                                                                    cursor[i]['Fare'] == data_doc[
                                                                                    'Fare']) and (
                                                                                cursor[i]['Currency'] == data_doc[
                                                                                'Currency']) and (
                                                                            cursor[i]['dep_date_from'] == data_doc[
                                                                            'dep_date_from']) and (
                                                                        cursor[i]['dep_date_to'] == data_doc[
                                                                        'dep_date_to']))
                                                except KeyError:
                                                    dep_date_end_object = today + datetime.timedelta(days=90)
                                                    dep_date_end = datetime.datetime.strftime(dep_date_end_object,
                                                                                              '%Y-%m-%d')

                                                    dep_date_start=SYSTEM_DATE
                                                    cursor[i]['dep_date_from'] = dep_date_start
                                                    cursor[i]['dep_date_to'] = dep_date_end

                                                    print("Raise New Promotion Released Trigger")
                                                    promo_new_promotion_trigger = PromoNewPromotionTrigger(
                                                        "new_promotions",
                                                        old_doc_data=cursor[i],
                                                        new_doc_data=doc,
                                                        changed_field="OD")
                                                    promo_new_promotion_trigger.do_analysis()
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

                                            if cursor[i]['Compartment'] != data_doc['Compartment']:

                                                change_flag_list.append(5)

                                            else:

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

                                            if True:
                                                try:
                                                    cursor[i]['Start Date'] != data_doc['dep_date_from']
                                                    change_flag_list.append(9)
                                                except KeyError:
                                                    dep_date_end_object = today + datetime.timedelta(days=90)
                                                    dep_date_end = datetime.datetime.strftime(dep_date_end_object,
                                                                                              '%Y-%m-%d')

                                                    dep_date_start=SYSTEM_DATE
                                                    cursor[i]['dep_date_from'] = dep_date_start
                                                    cursor[i]['dep_date_to'] = dep_date_end
                                            else:
                                                pass

                                            if True:
                                                try:
                                                    cursor[i]['End Date'] != data_doc['dep_date_to']
                                                    change_flag_list.append(10)
                                                except KeyError:
                                                    dep_date_end_object = today + datetime.timedelta(days=90)
                                                    dep_date_end = datetime.datetime.strftime(dep_date_end_object,
                                                                                              '%Y-%m-%d')

                                                    dep_date_start = SYSTEM_DATE
                                                    cursor[i]['dep_date_from'] = dep_date_start
                                                    cursor[i]['dep_date_to'] = dep_date_end
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

                                                promo_new_promotion_trigger.do_analysis()


                            except KeyError:
                                    if dict(dep_date_start= "", dep_date_end=""):
                                        dep_date_end_object = today + datetime.timedelta(days=90)
                                        dep_date_end = datetime.datetime.strftime(dep_date_end_object, '%Y-%m-%d')

                                        dep_date_start=SYSTEM_DATE
                                    else:
                                        pass

                            rule_change()
                            date_change()
                            fare_change()
                            new_promotion()


                            db.JUP_DB_Promotions.update(data_doc,doc,upsert = True)
                            print 'yes'
    driver.close()
if __name__ == '__main__':
  run()


