#!/usr/bin/env python
# print "Content-Type: application/json"
print
import datetime
import json
import traceback
import cgi
from jupiter_AI.common.tweak_filter import tweak_filter
from copy import deepcopy
from jupiter_AI.tiles.price_biometrics.dashboard import get_tiles as price_biometrics_dhb_tiles
from jupiter_AI import client, JUPITER_DB
db = client[JUPITER_DB]
try:
    form = cgi.FieldStorage()
    input_filter_json = form.getvalue('filter')
    input_filter_dict = deepcopy(json.loads(input_filter_json))
    # input_filter_dict = {
    #     "region": [],
    #     "country": [],
    #     "pos": [],
    #     "od": [],
    #     "compartment": [],
    #     "fromDate": "2017-01-18",
    #     "toDate": "2017-01-18"
    # }
    callable_filter = tweak_filter(input_filter_dict)
    tiles_dict = price_biometrics_dhb_tiles(callable_filter)
    result_json = json.dumps(tiles_dict)
    stamp = str(datetime.datetime.now())
    db.JUP_DB_AdHoc_Tiles.insert_one({'time_stamp': stamp,
                                      'module': 'price_biometrics',
                                      'screen': 'price_biometrics_dhb_scn',
                                      'filter': input_filter_dict,
                                      'tiles': tiles_dict})
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
    send_simple_message(to=NOTIFICATION_EMAIL, subject='Price Bio Dashboard Tiles', body=p)

