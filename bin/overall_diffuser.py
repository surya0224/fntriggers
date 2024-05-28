#!/usr/bin/env python
import cgi
import cgitb
cgitb.enable()

print "Content-Type: text/html"
print

from jupiter_AI.diffuser.overall_diffuserr import main_func
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
    # }]
    for doc in input_filter_dict:
        pos1 = doc['pos']
        origin1 = doc['origin']
        destination1 = doc['destination']
        compartment1 = doc['compartment']
        region1 = doc['region']
        country1 = "undefined"
        price1 =  doc['base_fare']
        base_farebasis1 = doc['base_farebasis']
        main_func(pos1, origin1 , destination1 , compartment1 , country1 , region1 , price1, base_farebasis1)
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
    send_simple_message(to=NOTIFICATION_EMAIL, subject='OVERALL__DIFFUSER_pythoncall', body=p)