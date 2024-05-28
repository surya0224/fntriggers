#!/usr/bin/env python
# print "Content-Type: application/json \n\n"
print
import cgi
import datetime
import json
import traceback

from jupiter_AI.common.tweak_filter import tweak_filter
from jupiter_AI import client
from jupiter_AI.network_level_params import JUPITER_DB
db = client[JUPITER_DB]
from jupiter_AI.tiles.kpi.dashboard import get_tiles as kpi_dhb_tiles

try:
    form = cgi.FieldStorage()
    input_filter_json = form.getvalue('filter')
    input_filter_dict = json.loads(input_filter_json)
    # input_filter_dict =  {'region': [],
    #                       'country': [],
    #                       'pos': [],
    #                       'od': [],
    #                       'compartment': [],
    #                       'fromDate': '2017-01-01',
    #                       'toDate': '2017-01-17'}
    callable_filter = tweak_filter(input_filter_dict)
    tiles_dict = kpi_dhb_tiles(callable_filter)
    tiles_dict['yield_rasm_seatfactor'] = dict()
    yield_data = db.JUP_DB_Demo_KPI_Yield.find()[0]
    del yield_data['_id']
    tiles_dict['yield_rasm_seatfactor']['Yield'] = yield_data
    rasm_data = db.JUP_DB_Demo_KPI_rasm_sf.find()[0]
    del rasm_data['_id']
    tiles_dict['yield_rasm_seatfactor']['RASM_SeatFactor'] = rasm_data
    result_json = json.dumps(tiles_dict)
    stamp = str(datetime.datetime.now())
    db.JUP_DB_AdHoc_Tiles.insert_one({'time_stamp': stamp,
                                      'module': 'kpi',
                                      'screen': 'dashboard',
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
    send_simple_message(to=NOTIFICATION_EMAIL, subject='KPI Dashboard Tiles', body=p)

