import json
import numpy as np
import pandas as pd
import datetime
import math
import time
from jupiter_AI.batch.atpco_automation.Automation_tasks import run_exchange_rate
from celery import group, chord, chain
from jupiter_AI import client, JUPITER_DB, SYSTEM_DATE, today, parameters, RABBITMQ_HOST, RABBITMQ_PASSWORD, \
    RABBITMQ_PORT, RABBITMQ_USERNAME, ATPCO_DB, JUPITER_LOGGER
from jupiter_AI.logutils import measure
import rabbitpy


url = 'amqp://' + RABBITMQ_USERNAME + \
      ":" + RABBITMQ_PASSWORD + \
      "@" + RABBITMQ_HOST + ":" + str(RABBITMQ_PORT) + "/"

db = client[JUPITER_DB]
db_ATPCO = client[ATPCO_DB]




@measure(JUPITER_LOGGER)
def exchange_master(system_date):

    st = time.time()
    coll = db.JUP_DB_ATPCO_Fares_Rules
    cxr = coll.distinct('carrier')
    carrier_od = []

    for carrier in cxr:
        carrier_od.append(run_exchange_rate.s(system_date=system_date,carrier_list=[carrier]))
    group_y = group(carrier_od)
    resy = group_y()
    grpy_res = resy.get()
    print 'ex rate done in', time.time() - st



if __name__ == '__main__':
    system_date = datetime.datetime.strftime(today - datetime.timedelta(days=1), "%Y-%m-%d")
    # system_date = "2018-12-27"
    exchange_master(system_date)