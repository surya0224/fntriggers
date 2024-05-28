#!/usr/bin/env python
import cgi
import cgitb
cgitb.enable()

print "Content-Type: text/html"
print

import json
import traceback
from copy import deepcopy
from jupiter_AI.batch.JUP_AI_Batch_Top5_Competitors import obtain_top_5_comp
from jupiter_AI.triggers.manual_triggers.MainClass import ManualTrigger


try:
    form = cgi.FieldStorage()
    # input_filter_json = form.getvalue('filter')
    # print input_filter_json
    input_filter_dict = deepcopy(json.loads(form['filter'].value))
    # print input_filter_dict
    # input_filter_dict = [
    #     {'pos': 'DOH',
    #      'origin': "DOH",
    #      'destination': 'BOM',
    #      'compartment': "Y",
    #      'reason': "Manual Trigger Test",
    #      'fromDate': '2017-01-01',
    #      'toDate': '2017-03-30',
    #      'work_package_name':'WP1'},
    #     {'pos': 'DOH',
    #      'origin': "DOH",
    #      'destination': 'BOM',
    #      'compartment': "Y",
    #      'reason': "Manual Trigger Test",
    #      'fromDate': '2017-01-01',
    #      'toDate': '2017-03-30',
    #      'work_package_name':'WP1'}
    # ]
    for doc in input_filter_dict:
        pos = doc['pos']
        origin = doc['origin']
        destination = doc['destination']
        compartment = doc['compartment']
        dep_date_start = doc['fromDate']
        dep_date_end = doc['toDate']
        reason = doc['reason']
        work_package_name = doc['work_package_name']
        flag = doc['flag']
        obj = ManualTrigger(pos=pos,
                            origin=origin,
                            destination=destination,
                            compartment=compartment,
                            dep_date_start=dep_date_start,
                            dep_date_end=dep_date_end,
                            reason=reason,
                            work_package_name=work_package_name,
                            flag=flag)
        obj.do_analysis()
        print 'Trigger ID', obj.trigger_id, 'Generated, Queued for Analysis'
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
    send_simple_message(to=NOTIFICATION_EMAIL, subject='Manual Trigger UI - Python Call', body=p)
