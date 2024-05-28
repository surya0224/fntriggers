#!/usr/bin/env python
# print "Content-Type: application/json"
from jupiter_AI.common.mail_error_msg import send_simple_message

print
import cgi
import datetime
import json
import smtplib
from copy import deepcopy
from email.mime.text import MIMEText
from time import localtime, strftime
from jupiter_AI import client, JUPITER_DB, NOTIFICATION_EMAIL

db = client[JUPITER_DB]
from jupiter_AI.common.tweak_filter import tweak_filter

from jupiter_AI.tiles.competitor_analysis.dashboard import get_tiles as comp_analysis_dhb_tiles

try:
    form = cgi.FieldStorage()
    input_filter_json = form.getvalue('filter')
    input_filter_dict = deepcopy(json.loads(input_filter_json))
    # input_filter_dict =  {'region': [],
    #                       'country': [],
    #                       'pos': [],
    #                       'od': ["DXBDOH"],
    #                       'compartment': ["Y"],
    #                       'fromDate': '2017-02-14',
    #                       'toDate': '2017-02-20'}
    callable_filter = tweak_filter(input_filter_dict)
    tiles_dict = comp_analysis_dhb_tiles(callable_filter)
    result_json = json.dumps(tiles_dict)
    stamp = str(datetime.datetime.now())
    db.JUP_DB_AdHoc_Tiles.insert_one({'time_stamp': stamp,
                                      'module': 'competitor_analysis',
                                      'screen': 'dashboard',
                                      'filter': input_filter_dict,
                                      'tiles': tiles_dict})
    print result_json
except Exception as e:
    import traceback

    p = ''.join(['ERROR : ',
                 traceback.format_exc(),
                 ' \nTIME : ',
                 strftime("%a, %d %b %Y %H:%M:%S ", localtime()),
                 'IST'])
    send_simple_message(to=NOTIFICATION_EMAIL, subject='Comp Analysis Dashboard Tiles', body=p)



