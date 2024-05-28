#!/usr/bin/env python
# print "Content-Type: application/json"
print
import json
import cgi
import traceback
from copy import deepcopy
from jupiter_AI.common.tweak_filter import tweak_filter
from jupiter_AI.batch.JUP_AI_Batch_Top5_Competitors import obtain_top_5_comp
from jupiter_AI import client,JUPITER_DB
db = client[JUPITER_DB]


try:
    form = cgi.FieldStorage()
    input_filter_json = form.getvalue('filter')
    input_filter_dict = deepcopy(json.loads(input_filter_json))
    # input_filter_dict =  {'region':['GCC'],
    #                       'country':['QA'],
    #                       'pos': ['DOH'],
    #                       'origin': ["DOH"],
    #                       'destination':['BOM'],
    #                       'compartment': ["Y"],
    #                       'fromDate': '2017-06-01',
    #                       'toDate': '2017-06-30'}
    region = input_filter_dict['region']
    country = input_filter_dict['country']
    pos = input_filter_dict['pos']
    origin = input_filter_dict['origin']
    destination = input_filter_dict['destination']
    compartment = input_filter_dict['compartment']
    dep_date_start = input_filter_dict['fromDate']
    dep_date_end = input_filter_dict['toDate']

    # callable_filter = tweak_filter(input_filter_dict)
    top5_comp = obtain_top_5_comp(region=region,
                                  country=country,
                                  pos=pos,
                                  origin=origin,
                                  destination=destination,
                                  compartment=compartment,
                                  dep_date_start=dep_date_start,
                                  dep_date_end=dep_date_end)
    result_json = json.dumps(top5_comp)
    print result_json
except Exception:
    print traceback.print_exc()
    import traceback
    from time import strftime, localtime
    from jupiter_AI.common.mail_error_msg import send_simple_message
    from jupiter_AI import NOTIFICATION_EMAIL

    p = ''.join(['ERROR : ',
                 traceback.format_exc(),
                 ' \nTIME : ',
                 strftime("%a, %d %b %Y %H:%M:%S ", localtime()),
                 'IST'])
    send_simple_message(to=NOTIFICATION_EMAIL, subject='Top5 Competitors Calculations', body=p)
