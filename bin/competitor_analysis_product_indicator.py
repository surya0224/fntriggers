#!/usr/bin/env python
# print "Content-Type: application/json"
import cgi
import datetime
import json
import traceback
from copy import deepcopy
from jupiter_AI import client, JUPITER_DB
db = client[JUPITER_DB]
from jupiter_AI.common.tweak_filter import tweak_filter
from jupiter_AI.tiles.competitor_analysis.product_indicator import get_tiles as product_indicator

print
try:
    form = cgi.FieldStorage()
    input_filter_json = form.getvalue('filter')
    input_filter_dict = deepcopy(json.loads(input_filter_json))
    # input_filter_dict =  {'region': [],
    #                       'country': [],
    #                       'pos': [],
    #                       'od': [],
    #                       'compartment': [],
    #                       'fromDate': '2016-01-01',
    #                       'toDate': '2017-12-31'}
    callable_filter = tweak_filter(input_filter_dict)
    tiles_dict = product_indicator(callable_filter)
    result_json = json.dumps(tiles_dict)
    stamp = str(datetime.datetime.now())
    db.JUP_DB_AdHoc_Tiles.insert_one({'time_stamp': stamp,
                                      'module': 'competitor_analysis',
                                      'screen': 'product_indicator',
                                      'filter': input_filter_dict,
                                      'tiles': tiles_dict})
    print result_json
except Exception:
    import traceback
    from time import strftime, localtime
    from jupiter_AI.common.mail_error_msg import send_simple_message
    from jupiter_AI import NOTIFICATION_EMAIL
    p = ''.join(['ERROR : ',
                 traceback.format_exc(),
                 ' \nTIME : ',
                 strftime("%a, %d %b %Y %H:%M:%S ", localtime()),
                 'IST'])
    send_simple_message(to=NOTIFICATION_EMAIL, subject='Comp Analysis Product Indicator', body=p)