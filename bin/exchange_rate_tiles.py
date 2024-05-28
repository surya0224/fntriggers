#!/usr/bin/env python
# print "Content-Type: application/json"
print
import cgi
import datetime
import json
import smtplib
import traceback
from copy import deepcopy
from email.mime.text import MIMEText
from time import localtime, strftime

from jupiter_AI.common.tweak_filter import tweak_filter

from jupiter_AI.tiles.exchange_rate.exchange_rate_dashboard import get_exchange_rate_tiles as er_tiles
from jupiter_AI import client, JUPITER_DB
db = client[JUPITER_DB]
try:
    form = cgi.FieldStorage()
    input_filter_json = form.getvalue('filter')
    input_filter_dict = deepcopy(json.loads(input_filter_json))
    # input_filter_dict = \
    #     {
    #          'region': [],
    #          'country': [],
    #          'pos': [],
    #          'od': ["DXBDOH"],
    #          'compartment': ["Y"],
    #          'currency': ['AED'],
    #          'fromDate': '2017-02-14',
    #          'toDate': '2017-02-14',
    #             }
    callable_filter = tweak_filter(input_filter_dict)
    db.JUP_DB_EXCHANGERATE_AVG_RATE.drop()
    db.JUP_DB_EXCHANGE_RATE_TILE_CURRENT_MRKT_SHR_REV.drop()
    db.JUP_DB_EXCHANGE_YQRECOVERY.drop()
    tiles_dict = er_tiles(callable_filter)
    # db.JUP_DB_EXCHANGERATE_AVG_RATE.drop()
    # db.JUP_DB_EXCHANGE_RATE_TILE_CURRENT_MRKT_SHR_REV.drop()
    # db.JUP_DB_EXCHANGE_YQRECOVERY.drop()
    exchange_rate_crsr = db.JUP_DB_EXCHANGERATE_AVG_RATE.find()
    if exchange_rate_crsr.count() > 0:
        exchange_details = exchange_rate_crsr[0]
        del exchange_details['_id']
        tiles_dict['average_exchange_rate_realization'] = exchange_details['avg_ex_rate']
    else:
        tiles_dict['average_exchange_rate_realization'] = None
    yq_crsr = db.JUP_DB_EXCHANGE_YQRECOVERY.find()
    if yq_crsr.count() > 0:
        yq_data = yq_crsr[0]
        del yq_data['_id']
        tiles_dict["yqrecovery_target_target_recovered"] = yq_data
    else:
        tiles_dict["yqrecovery_target_target_recovered"] = None
    current_mrkt_shr_crsr = db.JUP_DB_EXCHANGE_RATE_TILE_CURRENT_MRKT_SHR_REV.find()
    if current_mrkt_shr_crsr.count() > 0:
        current_data = current_mrkt_shr_crsr[0]
        # print current_data
        del current_data['_id']
        tiles_dict["current_market_share_current_revenue"] = current_data
    else:
        tiles_dict["current_market_share_current_revenue"] = None
    result_json = json.dumps(tiles_dict)
    stamp = str(datetime.datetime.now())
    db.JUP_DB_AdHoc_Tiles.insert_one({'time_stamp': stamp,
                                      'module': 'exchange_rate',
                                      'screen': 'dashboard',
                                      'filter': input_filter_dict,
                                      'tiles': tiles_dict})
    print result_json
except Exception as e:
    import traceback
    from time import strftime, localtime
    from jupiter_AI.common.mail_error_msg import send_simple_message
    from jupiter_AI import NOTIFICATION_EMAIL

    p = ''.join(['ERROR : ',
                 traceback.format_exc(),
                 ' \nTIME : ',
                 strftime("%a, %d %b %Y %H:%M:%S ", localtime()),
                 'IST'])
    send_simple_message(to=NOTIFICATION_EMAIL, subject='Exchange Rate Tiles', body=p)