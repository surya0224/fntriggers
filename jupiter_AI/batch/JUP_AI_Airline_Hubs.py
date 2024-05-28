"""
File Name              :   JUP_AI_Airline_Hubs.py
Author                 :   Shamail Mulla
Date Created           :   2017-04-04
Description            :  Hubs are calculated for each airline.

"""

from collections import Counter

from statistics import mode

try:
    from jupiter_AI import client, JUPITER_DB
    db = client[JUPITER_DB]
except:
    pass

airline_hub = []


def find_modes(lst):
    counter = Counter(lst)
    _, val = counter.most_common(1)[0]
    return [x for x, y in counter.items() if y == val]


def get_airline_hub():

    print 'Finding hub...'
    ppln_find_ods = [
        {
            '$project':
                {
                    # '_id': 0,
                    'airline': '$airline',
                    'origin': '$origin',
                    'destination': '$destination'
                }
        }
        ,
        {
            '$group':
                {
                    '_id': '$airline',
                    'od':
                        {
                            '$push':
                                {
                                    'origin': '$origin',
                                    'destination': '$destination'
                                }
                        }
                }
        }
    ]
    crsr_ods = db.JUP_DB_Capacity.aggregate(ppln_find_ods)
    lst_ods = list(crsr_ods)
    lst_hubs = []
    multimodal_airlines = 0
    # print len(lst_ods),'airlines:'

    for airline in lst_ods:
        # print airline[u'_id'],
        lst_city = []
        for od in airline[u'od']:
            lst_city.append(od[u'origin'])
            lst_city.append(od[u'destination'])
        # print len(lst_city)
        try:
            hub = mode(lst_city)
            lst_hubs.append({'airline': airline[u'_id'], 'hub': hub})
        except:
            multimodal_airlines += 1
            lst_modes = find_modes(lst_city)
            lst_hubs.append({'airline': airline[u'_id'], 'hub': lst_modes})

    print multimodal_airlines,'airlines have multiple modes of',len(lst_hubs)

    for hubs in lst_hubs:
        print hubs


    # db.JUP_DB_Airline_Hubs.insert(lst_hubs)


if __name__== '__main__':
    get_airline_hub()