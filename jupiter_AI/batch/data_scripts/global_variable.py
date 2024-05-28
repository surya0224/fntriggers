'''
Global variable value we can give from here
'''
#'''
from datetime import datetime
import datetime as dt
import calendar
import time

dateformat = '%Y-%m-%d'
systemDate_ISO = datetime.now()
systemDate = datetime.strftime(systemDate_ISO, dateformat)
#systemDate = "2017-03-07"
'''
# Fn local
derivedDatabase ='fzDB_stg'
mongo_client_url ='104.211.229.36:27022'
userName = 'mgadmin'
password = 'mgadmin$2018'
rawdatabase='rawDB_stg'
atpcoDatabase = "ATPCO_stg"
authDatabase = "admin"
server = "Dev"
'''

#'''
# Staging
derivedDatabase='fzDB_stg'
mongo_client_url='172.28.23.5:27017'
userName = 'pdssETLUser'
password = 'pdssETL@2017'
rawdatabase='rawDB_stg'
atpcoDatabase = "ATPCO_stg"
authDatabase = "admin"
server = "Stg"
#'''
'''
# Production
derivedDatabase='fzDB_prod'
mongo_client_url='172.29.4.5:27022'
userName = 'pdssETLUser'
password = 'pdssETL@123'
rawdatabase='rawDB_prod'
atpcoDatabase = "ATPCO_prod"
authDatabase = "admin"
server = "Prod"
#'''
#'''
## RabbitMQ
# Production
rabbitMQIP = '172.29.4.5'
rabbitMQPort = 5672
rabbitMQuserName = 'mqadmin'
rabbitMQpassword = 'mqadminpdss'

# User_Details #
Excel_path = "F:/Data validation/FZ- data/Final_Users_PDSS_final version_Flynava_20180709_v7.xlsx"
Sheet_name = "Users"
