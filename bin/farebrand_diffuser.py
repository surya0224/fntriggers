#!/usr/bin/env python
import cgi
import cgitb
cgitb.enable()

print "Content-Type: text/html"
print

from jupiter_AI.diffuser.ClassFarebrandDiffuser import  main_func
from copy import deepcopy
import json
try:
    form = cgi.FieldStorage()
    # input_filter_json = form.getvalue('filter')
    # print input_filter_json
    input_filter_dict = deepcopy(json.loads(form['filter'].value))
    # print input_filter_dict
    # input_filter_dict = [{
    #     # "region": "Network",
    #     # "country": "Network",
    #     # "pos": "Network",
    #     # "origin": "Network",
    #     # "destination": "Network",
    #     # "compartment": "All",
    #     # "ow_rt_flag": "RT",
    #     # "base_fare": 1142.204,
    #     # "base_farebasis": UR1AE1"
    #  pos , origin , destination , compartment, farebasis , price , region
    # }]
    for doc in input_filter_dict:
        pos1 = doc['pos']
        origin1 = doc['origin']
        destination1 = doc['destination']
        compartment1 = doc['compartment']
        region1 = doc['region']
        price1 = doc['base_fare']
        farebasis1 = doc['base_farebasis']
        main_func(pos1 , origin1, destination1, compartment1 , farebasis1 , price1, region1)
except Exception:
    # print traceback.print_exc()
    import traceback
    from time import strftime, localtime
    from jupiter_AI.common.mail_error_msg import send_simple_message
    from jupiter_AI import NOTIFICATION_EMAIL

    p = ''.join(['ERROR : ',
                 traceback.format_exc(),
                 ' \nTIME : ',
                 strftime("%a, %d %b %Y %H:%M:%S ", localtime()),
                 'IST'])
    send_simple_message(to=NOTIFICATION_EMAIL, subject='farebrand_diffuser_pythoncall', body=p)


# main_func(pos1="doh", origin1="dxb" , destination1="doh", compartment1= "Y", farebasis1= "ER1IN1", price1= 430 , region1= "Middle_east")