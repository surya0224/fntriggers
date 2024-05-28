#!/usr/bin/env python
# print "Content-Type: application/json"
print
import datetime
import json
import traceback
from copy import deepcopy
from jupiter_AI.common.tweak_filter import tweak_filter
import cgi
from jupiter_AI.tiles.price_biometrics.price_quote import get_tiles as price_quote_tiles
from jupiter_AI import client,JUPITER_DB
db = client[JUPITER_DB]

try:
    form = cgi.FieldStorage()
    input_filter_json = form.getvalue('filter')
    input_filter_dict = deepcopy(json.loads(input_filter_json))
    # input_filter_dict = \
    #     {
    #         'region': [],
    #         'country': [],
    #         'pos': ["DXB"],
    #         'od': ["DXBDOH"],
    #         'compartment': [],
    #         'fromDate': '2017-02-14',
    #         'toDate': '2017-02-20'
    #     }
    callable_filter = tweak_filter(input_filter_dict)
    tiles_dict = price_quote_tiles(callable_filter)
    tiles_dict['Yield_Vlyr'] = dict()
    yield_data = db.JUP_DB_Demo_KPI_Yield.find()[0]
    del yield_data['_id']
    tiles_dict['Yield_Vlyr'] = yield_data
    result_json = json.dumps(tiles_dict)
    stamp = str(datetime.datetime.now())
    db.JUP_DB_AdHoc_Tiles.insert_one({'time_stamp': stamp,
                                      'module': 'price_biometrics',
                                      'screen': 'price_quote_scn',
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
    send_simple_message(to=NOTIFICATION_EMAIL, subject='Price Bio Price Quote Tiles', body=p)
