"""
RnA requirements
    What
        host average fare is greater than competitor's fares
    Why
        because competitor has lowered its prices
        or
        host has increased his prices
    Status Quo
        market share will reduce
    Action
        change prices according to the model
"""

import datetime
import os
import sys
from collections import defaultdict
from copy import deepcopy
from jupiter_AI import client, JUPITER_DB
db = client[JUPITER_DB]
from jupiter_AI.network_level_params import Host_Airline_Code

from jupiter_AI.network_level_params import na_value
from jupiter_AI.network_level_params import query_month_year_builder

#   The below two lines of code is just to make sure that we have the dir containing the package jupiter_AI in path
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir, os.path.pardir))
sys.path.append(root_dir)
print root_dir


def get_rna_price_average_fare(afilter):
    """
    Explanation:
        Motivation:
            1
                a)  an overall according to the filter
                    how is the average fare of host with
                    respect to the competitors.
                    for the range of departure months in filter.
                b)  decrease in the average fares of competitors
                    decrease in fares of competitors for these
                    departure dates(triggers)
                    increase in the average fares of host
                    host price increased(recommendations)
                c)  if ticketed(%) for those departure dates
                    is greater than that of the overall capacity
                    of filter
                        NO Action
                    else
                        Pricing action based on model with
                        new data into consideration
            2
                The entire analysis listed for one
                pos-origin-destination-compartment level
                    a,b,c,d

        Requirements:
            1   Market data for the months of departure in filter
                    host
                    competitors
                        pax
                        revenue
                        avg.fare
                        dep month
            2   From Bookings Data
                    ticketed pax for the months of departure from filter
    :param afilter:
    :return:
    """
    afilter = deepcopy(defaultdict(list, afilter))
    query = dict()
    response = dict()
    # REQUIRED VALUES FROM dB
    lower_threshold = -5
    upper_threshold = 5
    from_obj = datetime.datetime.strptime(afilter['fromDate'], '%Y-%m-%d')
    to_obj = datetime.datetime.strptime(afilter['toDate'], '%Y-%m-%d')
    month_year_combinations = query_month_year_builder(from_obj.month,
                                                       from_obj.year,
                                                       to_obj.month,
                                                       to_obj.year)
    if afilter['region']:
        query['region'] = {'$in': afilter['region']}
    if afilter['country']:
        query['country'] = {'$in': afilter['country']}
    if afilter['pos']:
        query['pos'] = {'$in': afilter['pos']}
    if afilter['origin']:
        od_build = []
        for idx, item in enumerate(afilter['origin']):
            od_build.append({'origin': item, 'destination': afilter['destination'][idx]})
        query['$and'] = [{'$or': od_build}, {'$or':month_year_combinations}]
    else:
        query['$or'] = month_year_combinations
    if afilter['compartment']:
        query['compartment'] = {'$in': afilter['compartment']}
    apipeline = [
        {
            '$match': query
        },
        {
            '$group': {
                '_id': '$MarketingCarrier1',
                'pax': {'$sum': '$pax'},
                'revenue': {'$sum': '$revenue'}
            }
        },
        {
            '$project': {
                '_id': 0,
                'airline': '_id',
                'avg_fare':
                    {
                        '$cond': {'if': {'$pax': {'$gt': 0}},
                                  'then': {'$divide': ['$revenue', '$pax']},
                                  'else': na_value}
                    }
            }
        }
    ]
    cursor = db.JUP_DB_Market_Share.aggregate(apipeline)
    data = []
    dump = []
    for i in cursor:
        data.append(i)
    print data
    host_fare = [j['avg_fare'] for j in data if j['airline'] == Host_Airline_Code]
    if len(host_fare) == 1 and host_fare[0] != 0:
        for k in data:
            if k['airline'] != Host_Airline_Code:
                change = (k["avg_fare"] - host_fare[0]) / host_fare[0]
                if lower_threshold < change < upper_threshold:
                    print 'OK'
                else:
                    dump.append((change, k['airline'], k['avg_fare'], host_fare))
    print dump

if __name__ == '__main__':
    import time
    st = time.time()
    a = {
        'region': [],
        'country': [],
        'pos': [],
        'origin': [],
        'destination': [],
        'compartment': [],
        'fromDate': '2016-12-01',
        'toDate': '2016-12-31',
        'lastfromDate': '2015-07-01',
        'lasttoDate': '2015-07-31'
    }
    get_rna_price_average_fare(afilter=a)