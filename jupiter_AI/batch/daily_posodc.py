"""
Author: Nikunj Agarwal
Created with <3
Date: 2018-03-26
Code functionality: Updates Capacity, raging and FMS for host and rating, FMS for competitors for the months greater than
 today's month. This code will run daily because host capacity gets updated daily.
"""

import pandas as pd
import time
import datetime
from jupiter_AI import client, JUPITER_DB,JUPITER_LOGGER
from jupiter_AI.logutils import measure
from bson.objectid import ObjectId
from jupiter_AI.network_level_params import SYSTEM_DATE
import traceback

db = client[JUPITER_DB]


@measure(JUPITER_LOGGER)
def str_month(month):
    if month < 10:
        return '0' + str(month)
    else:
        return str(month)


@measure(JUPITER_LOGGER)
def get_pos_od_c_df():
    this_month_year = SYSTEM_DATE[:4] + SYSTEM_DATE[5:7]
    df = pd.DataFrame(list(db.JUP_DB_Pos_OD_Compartment_new.aggregate([
        {
            '$match':
                {
                    # 'market': 'CMBCMBDXBY',
                    'month_year': {'$gt': this_month_year}
                }
        },
        {
            '$unwind': '$top_5_comp_data'
        },
        {
            '$project':
                {
                    '_id': '$_id',
                    'pos': '$pos',
                    'od': '$od',
                    'compartment': '$compartment',
                    'airline': '$top_5_comp_data.airline',
                    'month': '$month',
                    'year': '$year',
                    'month_year': '$month_year',
                    'capacity': '$top_5_comp_data.capacity',
                    'capacity_1': '$top_5_comp_data.capacity_1',
                    'FMS': '$top_5_comp_data.FMS',
                    'FMS_1': '$top_5_comp_data.FMS_1',
                    'rating': '$top_5_comp_data.rating'
                }
        }
    ])
    ))
    return df


@measure(JUPITER_LOGGER)
def main():
    current_pos_od_c_df = get_pos_od_c_df()

    return 0


if __name__ == '__main__':
    main()




