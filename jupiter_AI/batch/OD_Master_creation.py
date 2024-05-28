"""
Author: Nikunj Agarwal
Created with <3
Date created: 2018-04-11
Description: Updating OD level params
Modifications:

"""
from jupiter_AI import client, JUPITER_DB, SYSTEM_DATE, JUPITER_LOGGER
from jupiter_AI.logutils import measure
from jupiter_AI.triggers.common import cursor_to_df
import datetime
import time
import pandas as pd
from dateutil.relativedelta import relativedelta
db = client[JUPITER_DB]
SYSTEM_DATE_LY = (datetime.datetime.strptime(SYSTEM_DATE, '%Y-%m-%d') - relativedelta(years=1)).strftime('%Y-%m-%d')


@measure(JUPITER_LOGGER)
def get_mt_df():
    mt_cursor = db.JUP_DB_Manual_Triggers_Module.aggregate([
        {
            '$match': {
                'dep_date': {
                    '$gte': SYSTEM_DATE_LY,
                    '$lt': SYSTEM_DATE
                },
                'flown_revenue.value': {"$gt": 0},
                'flown_pax.value': {"$gt": 0}
            }
        }
        ,
        {
            '$group': {
                '_id':
                    {
                        'od': '$od',
                    },
                'revenue': {'$sum': '$flown_revenue.value'},
                'pax': {'$sum': '$flown_pax.value'}
            }
        }
        ,
        {
            '$project': {
                '_id': 0,
                'od': '$_id.od',
                'revenue': '$revenue',
                'pax': '$pax'
            }
        }
    ])
    mt_df = cursor_to_df(mt_cursor)
