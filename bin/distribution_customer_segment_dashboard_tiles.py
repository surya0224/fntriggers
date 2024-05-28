#!/usr/bin/env python
# print "Content-Type: application/json"
print
import cgi
import datetime
import json
import traceback
from copy import deepcopy
from jupiter_AI import client, JUPITER_DB
db = client[JUPITER_DB]
from jupiter_AI.common.tweak_filter import tweak_filter

from jupiter_AI.tiles.distributor_customer_segments.distributor_customer_segment_dashboard import get_distributor_customer_segments_tiles as dc_tiles

try:
    form = cgi.FieldStorage()
    input_filter_json = form.getvalue('filter')
    input_filter_dict = deepcopy(json.loads(input_filter_json))
    callable_filter = tweak_filter(input_filter_dict)
    print json.dumps(callable_filter)
    tiles_dict = dc_tiles(callable_filter)
    print json.dumps(tiles_dict)
    tiles_dict['direct_and_indirect_revenue_share'] = dict()
    crsr = db.JUP_DB_DISTRIBUTION_DIRECT_AND_INDIRECT.find()
    if crsr.count() > 0:
        tiles_dict['direct_and_indirect_revenue_share']['indirect'] = crsr[0]['channel_revenue_share']
        tiles_dict['direct_and_indirect_revenue_share']['direct'] = crsr[1]['channel_revenue_share']
    else:
        tiles_dict['direct_and_indirect_revenue_share']['indirect'] = "NA"
        tiles_dict['direct_and_indirect_revenue_share']['direct'] = "NA"
    result_json = json.dumps(tiles_dict)
    stamp = str(datetime.datetime.now())
    id = db.JUP_DB_AdHoc_Tiles.insert({'time_stamp': stamp,
                                      'module': 'distribution_customer_segment',
                                      'screen': 'dashboard',
                                      'filter': input_filter_dict,
                                      'tiles': tiles_dict})
    print
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
    send_simple_message(to=NOTIFICATION_EMAIL, subject='Distributor and Customer Segment Tiles', body=p)