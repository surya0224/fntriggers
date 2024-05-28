#!/usr/bin/env python
# print "Content-Type: application/json"
print
import datetime
import json
import traceback
import cgi
from jupiter_AI.common.tweak_filter import tweak_filter
from copy import deepcopy
from jupiter_AI.tiles.market.market_dashboard import get_tiles as market_dhb_tiles
from jupiter_AI import client,JUPITER_DB
db = client[JUPITER_DB]

try:
    form = cgi.FieldStorage()
    input_filter_json = form.getvalue('filter')
    input_filter_dict = deepcopy(json.loads(input_filter_json))
    # input_filter_dict = {
    #             'region': [],
    #             'country': [],
    #             'pos': ["DXB"],
    #             'od': [],
    #             'compartment': ["Y"],
    #             'fromDate': "2017-02-14",
    #             'toDate': "2017-02-20"
    #         }
    callable_filter = tweak_filter(input_filter_dict)
    tiles_dict = market_dhb_tiles(callable_filter)
    capacity_crsr = db.JUP_Demo_Host_Capacity.find()
    if capacity_crsr.count() == 1:
        capacity_data = capacity_crsr[0]
        del capacity_data['_id']
        tiles_dict['host_capacity'] = {'capacity': capacity_data['capacity']}
        tiles_dict['deployed_capacity_rank'] = {'host_rank': capacity_data['rank']}
    tiles_dict['host_capacity'] = {'capacity': 189,
                                   'vlyr': -50}
    tiles_dict['deployed_capacity_rank'] = {'host_rank': 5}
    result_json = json.dumps(tiles_dict)
    stamp = str(datetime.datetime.now())
    k = db.JUP_DB_AdHoc_Tiles.insert({'time_stamp': stamp,
                                      'module': 'market',
                                      'screen': 'dashboard',
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
    send_simple_message(to=NOTIFICATION_EMAIL, subject='Market Dashboard Tiles', body=p)