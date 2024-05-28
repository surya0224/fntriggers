#strange output of change flag list
from jupiter_AI import JUPITER_LOGGER
from jupiter_AI.logutils import measure


@measure(JUPITER_LOGGER)
def run():
    from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoRuleChangeTrigger
    from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoDateChangeTrigger
    from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoFareChangeTrigger
    from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoNewPromotionTrigger
    from jupiter_AI import Host_Airline_Code, Host_Airline_Hub
    import time
    import json
    from selenium import webdriver
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By
    import re
    from dateutil.parser import parse
    from pymongo import UpdateOne
    from pyvirtualdisplay import Display

    # display = Display(visible=0, size=(800, 600))
    # display.start()

    from bson import ObjectId

    #The regions malaysiaairlines work in
    region_list = ['au','id','ph','in','cn','hk','sg','vn','tw','kr','my','th','uk','nz','hq','usa']

    #Connection to MongoDB
    import pymongo
    client = pymongo.MongoClient()
    db = client['mybasic2']

    print "opened mongodb"
    #Opening driver
    chrome_path = r"/home/prathyusha/Downloads/chromedriver"
    # chrome_path = r"/var/www/html/jupiter/python/jupiter_AI/batch/Promotions_Scraping/chromedriver"
    driver = webdriver.Chrome(chrome_path)
    # driver = webdriver.Firefox()
    driver.wait = WebDriverWait(driver, 5)


    #Scraping starts here
    for each_region in region_list:
        url = "https://www.malaysiaairlines.com/" + each_region + "/en/deals/deals-of-the-day.html"
        #url = "https://www.malaysiaairlines.com/th/en/deals/deals-of-the-day.html"
        try:
            driver.get(url)
            driver.wait = WebDriverWait(driver, 5)
        except:
            pass

        #Code to close popup if it comes
        try:
            driver.switch_to_window(driver.window_handles[-1])
            driver.find_element_by_xpath("//*[@id='cookie_policy_modal']/div/p[4]/button").click()
            #Sleep beacuse newsletter comes nearly after 15-16 seconds
            time.sleep(20)
        except:
            pass

        #Newsletter closing
        try:
            driver.find_element_by_xpath("//*[@id='mab_newsletter_newcustomer']/div[1]/div/i").click()
            time.sleep(5)
        except:
            pass

        time.sleep(10)
        #Look for promotions table
        try:
            driver.wait.until(EC.visibility_of_element_located((By.ID, "deals-country--list")))
            time.sleep(10)

            Table = driver.find_element_by_id('deals-country--list')
            Rows = Table.find_elements_by_tag_name('tr')
            print url
            print "len of rows", len(Rows)
            count=0
            count1 = 1
            t = 0
            bulk_update_doc = []
            # try:
            cursor_codes = list(db.JUP_DB_IATA_Codes.find())
            #df = DataFrame(cursor)
            print "cursor: ", cursor_codes
            # except:
            #     print "am not taking IATA codes"
            #     pass

            for row in Rows:
                if count == 0:
                    count += 1
                    continue

                change_flag_list = []

                try:
                    content = row.find_elements_by_tag_name('td')
                    print "content: ", content.text
                    #Code for Origin and Compartment
                    Origin = content[0].text
                    Destination = content[1].text
                    print "origin: ", Origin
                    print "destination: ", Destination


                    #Match codes for Origin and Destination from IATA codes
                    try:
                        # cursor = db.JUP_DB_IATA_Codes.find()
                        for i in cursor_codes:
                            if all(word in i['City'].lower() for word in re.findall(r'\w+', Origin[1].lower())):
                                Origin = i["Code"]
                                # print("Origin :",origin)
                            if all(word in i['City'].lower() for word in re.findall(r'\w+', Destination.lower())):
                                Destination = i["Code"]
                                # print("Destination :",destination)
                        print("OD: ", Origin + Destination)
                        # for i in cursor:
                        #     if any(word in i['City'].lower() for word in re.findall(r'\w+',Origin.lower())):
                        #         Origin = i['Code']
                        #     if any(word in i['City'].lower() for word in re.findall(r'\w+',Destination.lower())):
                        #         Destination = i['Code']
                    except Exception, e:
                        # print "Error_1"
                        # print e
                        pass


                    #Code for Compartment of flight/promotion
                    Compartment = content[2].text
                    if "economy" in Compartment.lower():
                        Compartment = "Y"
                    elif "business" in  Compartment.lower():
                        Compartment = "J"
                    elif "first" in Compartment.lower():
                        Compartment = "J"
                    print "Compartment: ", Compartment

                    #Code for Type - Return/One way
                    Type = content[3].text
                    print "type:", Type

                    #Code for promotion valid dates
                    VT = content[4].text
                    dt = parse(VT) #??
                    Valid_Till = dt.strftime('%Y-%m-%d')
                    print "Valid_Till: ", Valid_Till

                    #Code for Fare and Currency
                    curr = content[6].text
                    Fare = re.findall(r'\w+',curr)[1]
                    Currency = re.findall(r'\w+',curr)[0]
                    print "fare: ", Fare
                    print "currency:", Currency

                    #Code for dep_date_from and dep_date_to for travelling
                    Start_date_End_date = content[5].text
                    var1 = re.findall(r'\w+',Start_date_End_date)
                    Start_date = var1[2]+'-'+var1[1]+'-'+var1[0]
                    End_date = var1[5]+'-'+var1[4]+'-'+var1[3]
                    print "Start_date:" ,Start_date
                    print "End_date:" , End_date

                except Exception, e:
                    print "Error@content"
                    print e
                    pass

                #MongoDB collection
                doc = {
                  "Airline" : "MH",
                  "OD" : Origin + Destination,
                  "Valid from" : "",
                  "Valid till" : Valid_Till,
                  "Type" : Type,
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
                     "Airline" : "MH",
                     "OD" : Origin + Destination,
                     "Valid from" : "",
                     "Valid till" : Valid_Till,
                     "Type" : Type,
                     "Compartment" : Compartment,
                     "Fare" : Fare,
                     "Currency":Currency,
                     "dep_date_from" : Start_date,
                     "dep_date_to" : End_date,
                     "Url" : url,
                      }
                print "data-doc: ", data_doc
                print "doc: ", doc

                #try:


                @measure(JUPITER_LOGGER)
                def rule_change():
                    print "rule change trigger starts"
                    print data_doc
                cursor = list(db.JUP_DB_Airline_OD.aggregate([{ '$match': { 'Airline' : Host_Airline_Code}},{'$project': {'_id':0, 'OD':1}}]))
                # print "1---yes"
                for i in cursor:
                    if (data_doc['OD'] == i):


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
                            #print "data_doc: ", data_doc
                            if len(cursor) > 0:
                                if (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                                    cursor[0]['Valid till'] == data_doc['Valid till']) \
                                         and (
                                    cursor[0]['Compartment'] == data_doc['Compartment']):
                                    pass
                                else:
                                    pass
                                if (cursor[0]['Valid from'] != data_doc['Valid from']) and (
                                    cursor[0]['Valid till'] == data_doc['Valid till']) \
                                        and (
                                    cursor[0]['Compartment'] == data_doc['Compartment']):
                                    print "Raise Rule Change Trigger (Valid from is updated)"  ### Call the trigger
                                    promo_rule_change_trigger = PromoRuleChangeTrigger("promotions_ruleschange",
                                                                                       old_doc_data=cursor[0],
                                                                                       new_doc_data=doc,
                                                                                       changed_field="Valid from")
                                    promo_rule_change_trigger.do_analysis()
                                else:
                                    pass

                                if (cursor[0]['Valid from'] == data_doc['Valid from']) and (
                                    cursor[0]['Valid till'] != data_doc['Valid till']) \
                                         and (
                                    cursor[0]['Compartment'] == data_doc['Compartment']):
                                    print "Raise Rule Change Trigger (Valid till is updated)"  ### Call the trigger
                                    promo_rule_change_trigger = PromoRuleChangeTrigger("promotions_ruleschange",
                                                                                       old_doc_data=cursor[0],
                                                                                       new_doc_data=doc,
                                                                                       changed_field="Valid till")
                                    promo_rule_change_trigger.do_analysis()
                                else:
                                    pass

                                if (cursor[0]['Valid from'] == data_doc['Valid from']) and (
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
                                print "rule change checked"
                            else:
                                print "i m in else1"
                                pass
                        print "done with rule change"


                        @measure(JUPITER_LOGGER)
                        def date_change():
                            print "date range trigger starts"
                            print data_doc
                            cursor = list(db.JUP_DB_Promotions.aggregate([
                                {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                                            'Compartment': data_doc['Compartment'], 'Currency': data_doc['Currency'],
                                            'Fare': data_doc['Fare'], 'Valid from': data_doc['Valid from'],
                                            'Valid till': data_doc['Valid till'],
                                            }},
                                {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
                            #cursor = json.dump(cursor)
                            print cursor
                            if len(cursor) > 0:

                                if (cursor[0]['dep_date_from'] == data_doc['dep_date_from']) and (
                                    cursor[0]['dep_date_to'] == data_doc['dep_date_to']):
                                    pass
                                else:
                                    pass
                                if (cursor[0]['dep_date_from'] != data_doc['dep_date_from']) and (
                                    cursor[0]['dep_date_to'] == data_doc['dep_date_to']):
                                    print "Raise Date Change Trigger (dep date from is updated)"  ### Call the trigger
                                    promo_date_change_trigger = PromoDateChangeTrigger("promotions_dateschange",
                                                                                       old_doc_data=cursor[0],
                                                                                       new_doc_data=doc,
                                                                                       changed_field="dep_date_from")
                                    promo_date_change_trigger.do_analysis()
                                else:
                                    pass

                                if (cursor[0]['dep_date_from'] == data_doc['dep_date_from']) and (
                                    cursor[0]['dep_date_to'] != data_doc['dep_date_to']):
                                    print "Raise Date Change Trigger (dep date from is updated)"  ### Call the trigger
                                    promo_date_change_trigger = PromoDateChangeTrigger("promotions_dateschange",
                                                                                       old_doc_data=cursor[0],
                                                                                       new_doc_data=doc,
                                                                                       changed_field="dep_date_to")
                                    promo_date_change_trigger.do_analysis()
                                else:
                                    pass

                            else:
                                print "i m in else2"
                                pass
                        print "coming in ----yes1"


                        @measure(JUPITER_LOGGER)
                        def fare_change():
                            print "fare_range"
                            cursor = list(db.JUP_DB_Promotions.aggregate([
                                {'$match': {'OD': data_doc['OD'], 'Airline': data_doc['Airline'],
                                            'Compartment': data_doc['Compartment'], 'Currency': data_doc['Currency'],
                                            'Valid from': data_doc['Valid from'],
                                            'dep_date_from': data_doc['dep_date_from'],
                                            'dep_date_to': data_doc['dep_date_to'],
                                            'Valid till': data_doc['Valid till'],
                                            }},
                                {"$sort": {"Last Updated Date": -1, "Last Updated Time": -1}}]))
                            #cursor = json.dump(cursor)
                            print cursor
                            if len(cursor) > 0:

                                if (cursor[0]['Fare'] == data_doc['Fare']):
                                    pass
                                else:
                                    pass
                                if (cursor[0]['Fare'] != data_doc['Fare']):
                                    print "Raise Fare Change Trigger (Fare is updated)"  ### Call the trigger
                                    promo_fare_change_trigger = PromoFareChangeTrigger("promotions_fareschange",
                                                                                       old_doc_data=cursor[0],
                                                                                       new_doc_data=doc,
                                                                                       changed_field="Fare")
                                    promo_fare_change_trigger.do_analysis()
                                else:
                                    pass
                            else:
                                print "i am in else3"
                                pass


                        @measure(JUPITER_LOGGER)
                        def new_promotion():
                            print "new_promotion"
                            cursor = list(db.JUP_DB_Promotions.aggregate([
                                {'$match': {'Airline': data_doc['Airline']}}]))
                            #cursor = json.dump(cursor)
                            print "cursor:" , cursor
                            if len(cursor) > 0:
                                for i in range(len(cursor)):
                                    if (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                                        cursor[i]['Valid till'] == data_doc['Valid till']) \
                                             and (
                                                            cursor[i]['Compartment'] == data_doc['Compartment']
                                                    and (cursor[i]['OD'] == data_doc['OD']) and (
                                                    cursor[i]['Fare'] == data_doc['Fare']) and (
                                                cursor[i]['Currency'] == data_doc['Currency']) and (
                                            cursor[i]['dep_date_from'] == data_doc['dep_date_from']) and (
                                        cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                                        pass
                                    else:
                                        pass

                                    if (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                                        cursor[i]['Valid till'] == data_doc['Valid till']) \
                                             and (
                                                            cursor[i]['Compartment'] == data_doc['Compartment']
                                                    and (cursor[i]['OD'] == data_doc['OD']) and (
                                                    cursor[i]['Fare'] == data_doc['Fare']) and (
                                                cursor[i]['Currency'] != data_doc['Currency']) and (
                                            cursor[i]['dep_date_from'] == data_doc['dep_date_from']) and (
                                        cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
                                        print("Raise New Promotion Released Trigger")
                                        promo_new_promotion_trigger = PromoNewPromotionTrigger(
                                            "new_promotions",
                                            old_doc_data=cursor[i],
                                            new_doc_data=doc,
                                            changed_field="Currency")
                                        promo_new_promotion_trigger.do_analysis()
                                    else:
                                        pass

                                    if (cursor[i]['Valid from'] == data_doc['Valid from']) and (
                                        cursor[i]['Valid till'] == data_doc['Valid till']) \
                                             and (
                                                                    cursor[i]['Compartment'] == data_doc['Compartment']
                                                            and (cursor[i]['OD'] != data_doc['OD']) and (
                                                            cursor[i]['Fare'] == data_doc['Fare']) and (
                                                                cursor[i]['Currency'] == data_doc['Currency']) and (
                                                    cursor[i]['dep_date_from'] == data_doc['dep_date_from']) and (
                                                        cursor[i]['dep_date_to'] == data_doc['dep_date_to'])):
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
                                    if cursor[i]['Start Date'] != data_doc['dep_date_from']:
                                        change_flag_list.append(9)
                                    else:
                                        pass
                                    if cursor[i]['End Date'] != data_doc['dep_date_to']:
                                        change_flag_list.append(10)
                                    else:
                                        pass
                                    print "change_flag_list:" , change_flag_list
                                    if len(change_flag_list) > 2:
                                        print("Raise New Promotion Released Trigger, new promotions are released")
                                        promo_new_promotion_trigger = PromoNewPromotionTrigger("new_promotions",
                                                                                               old_doc_data=cursor[i],
                                                                                               new_doc_data=doc,
                                                                                               changed_field="new")
                                        promo_new_promotion_trigger.do_analysis()
                                    else:
                                        pass
                            else:
                                print "i m in else4"
                                pass

                        rule_change()
                        #print "coming in---yes2"
                        date_change()
                        fare_change()
                        new_promotion()

                    else:
                        #print "OD is not in FZ list, So no trigger will be raised"
                        pass

                # print "2---yes"
                # print "3----yes"

                # print "4----yes"
                # for j in range(len()):
                if t == 50:
                    st = time.time()
                    print "updating: ", count

                    print bulk_update_doc
                    db['JUP_DB_Promotions'].bulk_write(bulk_update_doc)
                    print "updated!", time.time() - st
                    bulk_list = []
                    count1 += 1
                    t = 0
                    #print "yes---------------------->"
                else:
                    # bulk_data_doc.append(data_doc )
                    # bulk_doc.append(doc)
                    # print "5---else--yes"

                    bulk_update_doc.append(UpdateOne(data_doc, {"$set" :doc}, upsert=True))
                    print bulk_update_doc
                    t += 1
                    print "t= :", t
                    # if j > 4:  #??
                    #     break

                # db.JUP_DB_Promotions.update(data_doc,doc,upsert = True)
                # print "yes"



                # except Exception, e:
                #     print "Error!!!"
                #     print e
                #     pass
        except:
            pass
    # except:
    #     print "country website not working"
    #     pass
    print "almost done---yes"
    driver.close()
if __name__ == "__main__":
    run()
