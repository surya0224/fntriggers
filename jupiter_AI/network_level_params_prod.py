# NETWORK_LEVEL_PARAMETERS LIST
import logging
import os
import datetime

# logging.basicConfig(filename=os.path.join(os.path.dirname(os.path.abspath(__file__)),
#                                           'logger_{}.log'.format(datetime.datetime.strftime(datetime.datetime.now(),
#                                                                                             "%d_%m_%Y"))),
#                     level=logging.INFO)
import pymongo
from copy import deepcopy
from datetime import date

ENV = 'Prod'
NOTIFICATION_EMAIL = 'analytics@flynava.com'
MAX_TRIGGER_PRIORITY_LEVEL = 9
MONGO_CLIENT_URL = '172.29.4.5:27022,172.29.104.5:27022'
ANALYTICS_MONGO_USERNAME = 'pdssAppUser'
ANALYTICS_MONGO_PASSWORD = 'pdssJup@123'
MONGO_SOURCE_DB = 'admin'
JUPITER_DB = 'fzDB_prod'
ATPCO_DB = 'ATPCO_prod'
RABBITMQ_HOST = '172.29.4.5'
RABBITMQ_PORT = 5672
RABBITMQ_USERNAME = 'mqadmin'
RABBITMQ_PASSWORD = 'mqadminpdss'
ATPCO_processing_hour = {"run_0" : {"start" : "21", "end" : "12"}, "run_1" : {"start" : "13", "end" : "16"}, "run_2" : {"start" : "17", "end" : "20"}}
JAVA_URL = "https://pdssapi.flydubai.com/jupiter/getManualTriggers"
JAVA_LP_URL = "https://pdssapi.flydubai.com/jupiter/runLandingPageScript"
SECURITY_CLIENT_ID = 'pdss'
SECURITY_CLIENT_SECRET_ID = '386667c1-0528-40b3-9054-2a3215742dbd'
AUTH_URL = 'https://smartrez.flydubai.com/smartrez-gateway/security/createServiceToken'
MAIL_URL = 'https://smartrez.flydubai.com/smartrez-gateway/smartrezmail/mail/send'
MAIL_RECEIVERS = ["Shameer.Koya@flydubai.com", "L.Kumarasamy@flydubai.com",
                  "fz_support@flynava.com", "rm.itappsupport@flydubai.com",
                  "IT.ProdServices@flydubai.com"]
PYTHON_PATH = '/app/python/analytics/'
Host_Airline_Code = 'FZ'
Currency_List = ['GBP', 'AED', 'INR', 'USD']
Host_Airline_Hub = 'DXB'
recommendation_lower_threshold = -20
recommendation_upper_threshold = +20
SYSTEM_DATE = datetime.datetime.now().strftime('%Y-%m-%d')
# SYSTEM_DATE = '2018-03-22'
INF_DATE_STR = '2099-12-31'
def mongo_client():
    client = pymongo.MongoClient(MONGO_CLIENT_URL)
    client.the_database.authenticate(ANALYTICS_MONGO_USERNAME, ANALYTICS_MONGO_PASSWORD, source=MONGO_SOURCE_DB)
    return client

client = mongo_client()
db = client[JUPITER_DB]
## System date change
doc = db.JUP_DB_Data_Status.distinct('snap_date')
str_date = doc[len(doc) - 1]
if SYSTEM_DATE == str_date:
    pass
else:
    SYSTEM_DATE = str_date

SYSTEM_TIME = datetime.datetime.now().strftime("%H:%M:%S")
today = datetime.datetime.strptime(SYSTEM_DATE, '%Y-%m-%d')
na_value = 'NA'
BOOKING_COL_NAME = 'JUP_DB_Booking_DepDate'
SALES_COL_NAME = 'JUP_DB_Sales'
FLOWN_COL_NAME = 'JUP_DB_Sales_Flown'
FORECAST_OD_COL_NAME = 'JUP_DB_Forecast_OD'
FORECAST_LEG_COL_NAME = 'JUP_DB_Forecast_Leg'
TARGET_COL_NAME = 'JUP_DB_Target_OD'
MARKET_SHARE_COL_NAME = 'JUP_DB_Market_Share'
HOST_CAPACITY_OD = 'JUP_DB_Inventory_OD'
HOST_INVENTORY_LEG = 'JUP_DB_Inventory_Flight'
COMP_CAPACITY_OD = 'JUP_DB_OD_Capacity'
COMP_RATINGS = 'JUP_DB_Competitor_Ratings'
WEEKEND_START_DOW = 3
WEEKEND_START_HOUR = 15
WEEKEND_START_MIN = 0
WEEKEND_END_DOW = 0
WEEKEND_END_HOUR = 8
WEEKEND_END_MIN = 0

logging.basicConfig(filename=os.path.join(os.path.dirname(PYTHON_PATH + 'logs/'),
                                          'python_{}.log'.format(datetime.datetime.strftime(datetime.datetime.now(),
                                                                                            "%d_%m_%Y"))),
                    level=logging.INFO)
JUPITER_LOGGER = logging.getLogger(__name__)


def get_jupiter_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    # self.args = " ".join(args)
    # self.kwargs = ";".join(['{}-{}'.format(k, v) for k, v in kwargs.items()])
    handler = logging.FileHandler(os.path.join(os.path.dirname(PYTHON_PATH + 'logs/'),
                                               'logger_{}.log'.format(
                                                   datetime.datetime.strftime(datetime.datetime.now(),
                                                                              "%d_%m_%Y"))))
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def close_client():
    global client
    client.close()


def query_month_year_builder(stdm, stdy, endm, endy):
    m = deepcopy(endm)
    y = deepcopy(endy)
    month_year = []
    if endy == 9999:
        list_query = [
            {
                'month': {'$gte': stdm},
                'year': stdy
            }
            ,
            {
                'year': {'$gt': stdy}
            }
        ]
        return list_query
    else:
        while True:
            month_year.append({'month': m,
                               'year': y})
            if m == stdm and y == stdy:
                break

            m -= 1
            if m == 0:
                m = 12
                y -= 1
        return month_year
