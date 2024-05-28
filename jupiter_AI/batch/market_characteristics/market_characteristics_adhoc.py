"""
Author: Abhinav Garg
Created with <3
Date Created: 2018-03-07
File Name: atpco_master.py

This code is the master file which calls the tasks for daily ATPCO related processing and
sends it to the rabbitmq which is then picked up by the Celery.

"""
import json
import numpy as np
import pandas as pd
import datetime
import math
import time
import pika
from jupiter_AI.triggers.common import cursor_to_df
from jupiter_AI.batch.atpco_automation.Automation_tasks import run_market_channels_adhoc, \
    run_market_agents_adhoc, run_market_distributors_adhoc, run_market_farebrand_adhoc, run_market_segments_adhoc, \
    run_market_flights_adhoc
from celery import group, chord, chain
from jupiter_AI import client, JUPITER_DB, SYSTEM_DATE, today, parameters, RABBITMQ_HOST, RABBITMQ_PASSWORD, \
    RABBITMQ_PORT, RABBITMQ_USERNAME, memory_limit, JUPITER_LOGGER
from jupiter_AI.logutils import measure
import rabbitpy

url = 'amqp://' + RABBITMQ_USERNAME + \
        ":" + RABBITMQ_PASSWORD + \
        "@" + RABBITMQ_HOST + ":" + str(RABBITMQ_PORT) + "/"

db = client[JUPITER_DB]


@measure(JUPITER_LOGGER)
def run_market_char_automation():
    channel_group = []
    agent_group = []
    farebrand_group = []
    distributor_group = []
    segment_group = []
    flight_group = []

    snap_date_start = '2016-12-01'
    snap_date_end = '2017-01-01'
    snap_date = snap_date_start
    while snap_date != snap_date_end:
        channel_group.append(run_market_channels_adhoc.s(snap_date=snap_date))
        agent_group.append(run_market_agents_adhoc.s(snap_date=snap_date))
        distributor_group.append(run_market_distributors_adhoc.s(snap_date=snap_date))
        farebrand_group.append(run_market_farebrand_adhoc.s(snap_date=snap_date))
        flight_group.append(run_market_flights_adhoc.s(snap_date=snap_date))
        snap_date = datetime.datetime.strftime(datetime.datetime.strptime(snap_date, '%Y-%m-%d') +
                                               datetime.timedelta(days=1), '%Y-%m-%d')
    print 'appended'
    group1 = group(channel_group)()
    group2 = group(agent_group)()
    group3 = group(distributor_group)()
    group4 = group(farebrand_group)()
    group6 = group(flight_group)()

    res1 = group1.get()
    print 'channel done'

    res2 = group2.get()
    print 'agent done'

    res3 = group3.get()
    print 'distributor done'

    res4 = group4.get()
    print 'farebrand done'
    # group5 = group(segment_group)()
    # res5 = group5.get()
    # print 'segment done'

    res6 = group6.get()
    print 'flight done'


if __name__ == '__main__':
    run_market_char_automation()
    print 'Done'
