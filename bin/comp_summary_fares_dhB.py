#!/usr/bin/env python
# print "Content-Type: application/json"
print
import cgi
import datetime
import json
import smtplib
from copy import deepcopy
from email.mime.text import MIMEText
from time import localtime, strftime
from jupiter_AI.triggers.mrkt_params_workflow_opt import comp_summary_java
from jupiter_AI import NOTIFICATION_EMAIL
from jupiter_AI.common.mail_error_msg import send_simple_message
from jupiter_AI.common.tweak_filter import tweak_filter
try:
    form = cgi.FieldStorage()
    input_filter_json = form.getvalue('filter')
    input_filter_dict = deepcopy(json.loads(input_filter_json))
    # input_filter_dict =  {'posArray': ['DXB'],
    #                       'odArray': ['DOHDXB'],
    #                       'compartmentArray': ['Y'],
    #                       'fromDate': '2017-05-01',
    #                       'toDate': '2017-07-31'}

    if input_filter_dict['odArray']:
        od_list = input_filter_dict['odArray']
        del input_filter_dict['odArray']
        input_filter_dict['origin'] = [od[:3] for od in od_list]
        input_filter_dict['destination'] = [od[3:] for od in od_list]
    else:
        del input_filter_dict['odArray']
        input_filter_dict['origin'] = None
        input_filter_dict['destination'] = None

    if input_filter_dict['posArray'] and len(input_filter_dict['posArray']) == 1:
        pos = input_filter_dict['posArray'][0]
    else:
        pos = None

    if input_filter_dict['origin'] and len(input_filter_dict['origin']) == 1:
        origin = input_filter_dict['origin'][0]
    else:
        origin = None

    if input_filter_dict['destination'] and len(input_filter_dict['destination']) == 1:
        destination = input_filter_dict['destination'][0]
    else:
        destination = None

    if input_filter_dict['compartmentArray'] and len(input_filter_dict['compartmentArray']) == 1:
        compartment = input_filter_dict['compartmentArray'][0]
    else:
        compartment = None

    response = comp_summary_java(pos=pos,
                                 origin=origin,
                                 destination=destination,
                                 compartment=compartment,
                                 dep_date_start=input_filter_dict['fromDate'],
                                 dep_date_end=input_filter_dict['toDate'])

    result_json = json.dumps(response)
    print result_json
except Exception as e:
    import traceback
    p = ''.join(['ERROR : ',
                 traceback.format_exc(),
                 ' \nTIME : ',
                 strftime("%a, %d %b %Y %H:%M:%S ", localtime()),
                 'IST'])
    send_simple_message(to=NOTIFICATION_EMAIL, subject='Comp Summary Fares dHb URL Call', body=p)
