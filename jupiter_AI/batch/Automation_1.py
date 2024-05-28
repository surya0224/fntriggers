import json

from jupiter_AI.batch.JUP_AI_OD_Capacity.New_Host_Inventory import get_leg_capacities
from jupiter_AI.batch.JUP_AI_OD_Capacity.Competitor_OD_Capacity import get_od_capacity_simple
from jupiter_AI.batch.copper_flights import generate_copper_flights
from jupiter_AI.batch.golden_flights import generate_golden_flights
from jupiter_AI.batch.zinc_flights import generate_zinc_flights
from celery import Celery
import time
import pika
from jupiter_AI import RABBITMQ_USERNAME, RABBITMQ_PASSWORD, RABBITMQ_HOST, RABBITMQ_PORT, JUPITER_LOGGER
from jupiter_AI.logutils import measure




url = 'pyamqp://' + RABBITMQ_USERNAME + \
        ":" + RABBITMQ_PASSWORD + \
        "@" + RABBITMQ_HOST + ":" + str(RABBITMQ_PORT) + "//"

app = Celery('Automation_1', backend='amqp://',broker="pyamqp://guest@localhost//")


@app.task(name="jupiter_AI.batch.Automation_1.run_host_od_capacity")


@measure(JUPITER_LOGGER)
def run_host_od_capacity():
    # get_leg_capacities()
    print "Running Host OD Capacity"
    time.sleep(10)
    print "Ran Host OD Capacity"
    return 1


@app.task(name="jupiter_AI.batch.Automation_1.run_comp_od_capacity")


@measure(JUPITER_LOGGER)
def run_comp_od_capacity():
    # get_od_capacity_simple()
    print "Running Competitor OD Capacity"
    time.sleep(20)
    print "Ran Competitor OD Capacity"
    return 2


@app.task(name="jupiter_AI.batch.Automation_1.run_golden_flights")


@measure(JUPITER_LOGGER)
def run_golden_flights():
    # get_od_capacity_simple()
    print "Running golden flights"
    time.sleep(10)
    print "Ran golden flights"
    return 3


@app.task(name="jupiter_AI.batch.Automation_1.run_copper_flights")


@measure(JUPITER_LOGGER)
def run_copper_flights():
    # get_od_capacity_simple()
    print "Running copper flights"
    time.sleep(10)
    print "Ran copper flights"
    return 4


@app.task(name="jupiter_AI.batch.Automation_1.run_zinc_flights")


@measure(JUPITER_LOGGER)
def run_zinc_flights():
    # get_od_capacity_simple()
    print "Running zinc flights"
    time.sleep(10)
    print "Ran zinc flights"
    return 5


@app.task(name="jupiter_AI.batch.Automation_1.run_pod_od_compartment")


@measure(JUPITER_LOGGER)
def run_pos_od_compartment():
    # get_od_capacity_simple()
    print "Running POS_OD"
    time.sleep(10)
    print "Ran POS_OD"
    return 6


@app.task(name="jupiter_AI.batch.Automation_1.send_msg")


@measure(JUPITER_LOGGER)
def send_msg(results_grp):
    if results_grp == [6]:
        print "Sending msg to generate triggers!!!"
    return 7


@app.task(name="jupiter_AI.batch.Automation_1.run_market_agents")


@measure(JUPITER_LOGGER)
def run_market_agents():
    print "Running Market Agents"
    time.sleep(50)
    print "Ran Market Agents"
    return 8


@app.task(name="jupiter_AI.batch.Automation_1.run_market_channels")


@measure(JUPITER_LOGGER)
def run_market_channels():
    print "Running Market Channels"
    time.sleep(50)
    print "Ran Market Channels"
    return 9


@app.task(name="jupiter_AI.batch.Automation_1.run_market_distributors")


@measure(JUPITER_LOGGER)
def run_market_distributors():
    print "Running Market Distributors"
    time.sleep(50)
    print "Ran Market Distributors"
    return 10


@app.task(name="jupiter_AI.batch.Automation_1.run_market_farebrand")


@measure(JUPITER_LOGGER)
def run_market_farebrand():
    print "Running Market Farebrand"
    time.sleep(50)
    print "Ran Market Farebrand"
    return 11


@app.task(name="jupiter_AI.batch.Automation_1.run_market_flights")


@measure(JUPITER_LOGGER)
def run_market_flights():
    print "Running Market Flights"
    time.sleep(50)
    print "Ran Market Flights"
    return 12
