#!/usr/bin/env python
# print "Content-Type: application/json"

import datetime
import json
import traceback
from jupiter_AI import client, JUPITER_DB
db = client[JUPITER_DB]
from jupiter_AI.common.tweak_filter import tweak_filter
from jupiter_AI.tiles.kpi.new_products import get_new_products

print
try:
    # form = cgi.FieldStorage()
    # input_filter_json = form.getvalue('filter')
    # input_filter_dict = deepcopy(json.loads(input_filter_json))
    input_filter_dict =  {'region': [],
                          'country': [],
                          'pos': [],
                          'od': [],
                          'compartment': [],
                          'fromDate': '2017-01-01',
                          'toDate': '2017-03-31'}
    callable_filter = tweak_filter(input_filter_dict)
    tiles_dict = get_new_products(callable_filter)
    result_json = json.dumps(tiles_dict)
    stamp = str(datetime.datetime.now())
    db.JUP_DB_AdHoc_Tiles.insert_one({'time_stamp': stamp,
                                      'module': 'kpi',
                                      'screen': 'yield_rasm_seatfactor',
                                      'filter': input_filter_dict,
                                      'tiles': tiles_dict})
    print
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
    send_simple_message(to=NOTIFICATION_EMAIL, subject='KPI New Product Tiles', body=p)
