import time

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import re
import numpy as np
from jupiter_AI.network_level_params import SYSTEM_DATE, today, INF_DATE_STR
import datetime


#Chrome Testing
options = webdriver.ChromeOptions()
chrome_path = r"/home/prathyusha/Downloads/chromedriver"
no_proxy = "24.72.232.124:3128"
#"110.77.187.49:42619"
options.add_argument('--proxy-server=%s' % no_proxy)
# # options.add_argument('window-size=1920x1080')
driver = webdriver.Chrome(executable_path=chrome_path,chrome_options=options)
driver.wait = WebDriverWait(driver, 5)
# url = "http://demo.flynava.com/auth/login/views/login.html#/login"
# url = "https://www.flydubai.com/en/"
url = "http://roninmittal.me/"
driver.get(url)
time.sleep(115)
print "hit"


"""
# #PhantomJS Testing
service_args = [
    '--proxy=185.82.212.95:8080',
    # '--proxy-type=http',
    ]
driver = webdriver.PhantomJS(service_args=service_args)
driver.wait = WebDriverWait(driver, 5)
#url = "http://demo.flynava.com/auth/login/views/login.html#/login"
url = "https://www.flydubai.com/en/"
driver.get(url)
time.sleep(10)
print "hit"
"""

# main_window_handle = None
# while not main_window_handle:
#     main_window_handle = driver.current_window_handle
# #driver.find_element_by_xpath(u'//a[text()="click here"]').click()
# signin_window_handle = None
# while not signin_window_handle:
#     for handle in driver.window_handles:
#         if handle != main_window_handle:
#             signin_window_handle = handle
#             break
# driver.switch_to.window(signin_window_handle)
# driver.find_element_by_xpath(u'//input[@id="Username"]').send_keys("demouser")
# driver.find_element_by_xpath(u'//input[@value="OK"]').click()
# driver.find_element_by_xpath(u'//input[@id="Password"]').send_keys("dlc61357")
# driver.find_element_by_xpath(u'//input[@value="OK"]').click()
# driver.switch_to.window(main_window_handle) #or driver.switch_to_default_content()


# from twill import get_browser
#
# b = get_browser()
# from twill.commands import go, showforms, formclear, fv, submit
#
# # fv("1", "Logon", "Logon")
#
# # submit()
# # show()
# # showforms()
#
# go('http://demo.flynava.com/auth/login/views/login.html#/login')
# showforms()
#
# fv("0", "Username", "demouser")
# fv("0", "Password", "dl6cd1357")
# formaction('Login','https://trakcarelabwebview.nhls.ac.za/trakcarelab/csp/logon.csp#TRAK_main')
# submit()
# print "donneee.. yo!"
# import pymongo
# client = pymongo.MongoClient("mongodb://analytics:KNjSZmiaNUGLmS0Bv2@13.92.251.7:42525/")
# db = client['fzDB_stg']
#

# username =driver.find_element_by_name('Username')
# print "79"
# username.send_keys('demouser')
#
# password = driver.find_element_by_name('Password')
# print "ghrsl"
# password.send_keys('dl6cd1357')
#
# form = driver.find_element_by_id('login')
# form.submit()

#driver.find_element_by_xpath("html/body/div[1]/div/div[2]/form/div[3]/button[1]").click()

# user = driver.find_element_by_xpath(".//*[@id='userid']" )
# user.send_keys('Europa')
# pwd = driver.find_element_by_xpath(".//*[@id='pass']")
# pwd.send_keys('dl6cd1357')
#
# form = driver.find_element_by_xpath("html/body/div[1]/div/div[2]/form/div[3]/button[1]")
# form.submit()
# #driver.find_element_by_xpath(".//*[@id='jupiter-search']")
# # driver.find_element_by_xpath(".//*[@id='searchbox']/div[2]/ul/li/a/div/div").click()
# # origin1 =driver.find_element_by_xpath(".//*[@id='jupiter-search']").click()
#
# # print "step2 done--"
#
#
# module_list = ['manualtrigger', 'fareupdate']
# for each_module in module_list:
#     # url = "https://www.malaysiaairlines.com/" + each_region + "/en/deals/deals-of-the-day.html"
#     url = "http://demo.flynava.com/modules/" + each_module + "/views/" + each_module + ".html"
#     driver.get(url)
#     driver.wait = WebDriverWait(driver, 5)
#     print "opened manual triggers page"
#     time.sleep(20)
#     # Look for query table
#     driver.wait.until(EC.visibility_of_element_located((By.ID, "querysystem")))
#     Table = driver.find_element_by_id('querysystem')
#     # Columns = Table.find_elements_by_tag_name('col-sm-2')
#
#     origin_list = []
#     dest_list = []
#     cursor_od = db.JUP_DB_OD_Master.find({})
#     for i in cursor_od:
#         origin = i["origin"]
#         origin_list.append(origin)
#         destination = i["destination"]
#         dest_list.append(destination)
#
#
#     # if user == "Europa" :
#     #
#     # if user == "":
#     #
#     # if user ==
#     # if user ==
#
#     date = SYSTEM_DATE
#     compartment_list = ['J', 'Y', 'J,Y']
#
#     n_iterations = 100
#     for i in range(n_iterations):
#         rand_ori = np.random.randint(0,150,1)[0]
#         rand_dest = np.random.randint(0,150,1)[0]
#         rand_pos = np.random.randint(0,4,1)[0]
#         rand_date_from = np.random.randint(0, 90, size=1)[0]
#         rand_date_to = np.random.randint(0,90,1)[0]
#         rand_comp = np.random.randint(0,2,1)[0]
#         # print rand_ori
#         print "origin: " ,origin_list[rand_ori]
#         ori = origin_list[rand_ori]
#         dest = dest_list[rand_dest]
#         print "dest:" ,dest
#         # user1 = driver.find_element_by_xpath(".//*[@id='username']")
#         # if user1 == "Europa":
#         POS_list = ['AMM', 'CMB', 'CGP', 'UAE']
#         pos = POS_list[rand_pos]
#         # else:
#         #     pass
#         # elif user1 == "Metis":
#         #     POS_list = ['DAC', 'CGP', 'ZYL', 'CMB', 'MLE', 'EBB', 'ZNZ', 'DAR', 'JRO', 'KTM', 'HRJ', 'ASM', 'BKK',
#         #                 'HBE']
#         #     pos = POS_list[rand_pos]
#         # elif user1 == "Vanissa":
#         #     POS_list = origin_list
#         #     pos = POS_list[rand_pos]
#         # elif user1 == "Himalia":
#         #     POS_list = ['KWI', 'BAH', 'RUH']
#         #     pos = POS_list[rand_pos]
#
#         #dept_date_from = int((datetime.datetime.strptime(str(x),"%Y%m%d") + datetime.timedelta(days=10)).strftime("%Y%m%d"))
#         print date
#         print rand_date_from
#         # date_1 = datetime.datetime.strptime(date, "%/%d/%y")
#         date_1 = datetime.datetime.strptime(date, '%Y-%m-%d')
#         print date_1
#         date_1 = date_1.date()
#         dep_date_from = date_1 - datetime.timedelta(days=rand_date_from)
#         print dep_date_from
#         dep_date_from = dep_date_from.strftime('%d-%m-%Y')
#         print dep_date_from
#         dep_date_to = date_1 + datetime.timedelta(days=rand_date_to)
#         dep_date_to = dep_date_to.strftime('%d-%m-%Y')
#         comp = compartment_list[rand_comp]
#         print dep_date_to
#         print "compartment: ", comp
#         #running query
#         origin_value = driver.find_element_by_xpath(".//select[@id='mqsexorigin']")
#         origin_value.send_keys(ori)
#         print "origin inserted"
#         dest_value = driver.find_element_by_xpath(".//select[@id='mqsexdest']")
#         dest_value.send_keys(dest)
#         # dep_date_from_value = driver.find_element_by_xpath(".//input[@class='fromdate']")
#         dep_date_from_value = driver.find_element_by_xpath("//*[@id='querysystem']/div[2]/div[2]/div/input")
#         dep_date_from_value.send_keys(dep_date_from)
#         dep_date_to_value = driver.find_element_by_xpath("//*[@id='querysystem']/div[2]/div[2]/div/input[2]")
#         dep_date_to_value.send_keys(dep_date_to)
#         print "dep_date_inserted"
#         pos_value = driver.find_element_by_xpath(".//*[@id='mqspos_chosen']/ul/li/input").click()
#         pos_value.send_keys(pos)
#
#         # print "pos inserted"
#         compartment_list_value = driver.find_element_by_xpath("//*[@id='select2-drop']/div/input")
#         compartment_list_value.send_keys(compartment_list)
#         print "compartment inserted"
#         driver.find_element_by_xpath(".//*[@id='salesquery']").click()
#         print "query is running"
#         time.sleep(10)
#         #get all rows
#         rows = driver.find_element_by_xpath('//*[@id="currency_table"]/table/tbody/tr[2]')
#         rows = []
#         cursor = list(db.manual.aggregate([{ '$match': { 'origin.city' : ori, 'destination.city': "dest", 'pos.city': "pos", 'compartment': "comp", 'dep_date': {'$gt': "dep_date_from", '$lte': "dep_date_to"}}}]))
#         for row in rows:
#             for doc in cursor:
#                 if (row['pos'] == doc['pos.city']) and (row['origin'] == doc['origin.city']) and  (row['destination'] == doc['destination.city']):
#                     pass

# driver.close()