import jupiter_AI.tiles.bookings_today.bookings_profile as bp
import jupiter_AI.tiles.bookings_today.bookings_runrate as br
import jupiter_AI.tiles.bookings_today.bookings_strength as bs


def get_bookings_today_tiles(afilter):
    #   bookings_runrate
    #   bookings_profile
    #   bookings_strength

    response = dict()
    response['bookings_runrate'] = br.get_bookings_runrate(afilter)
    response['bookings_strength'] = bs.get_bookings_strength(afilter)
    response['bookings_profile'] = bp.get_bookings_profile(afilter)

    return response

import time
st = time.time()
P = {'region': ['GCC'],
     'country': ['SA'],
     'pos': ['RUH'],
     'origin': ['RUH'],
     'destination': ['CMB'],
     'compartment': ['Y'],
     'fromDate': '2016-09-18',
     'toDate': '2016-09-20',
     'flag': 'true'}
bookings_today = get_bookings_today_tiles(P)
print"bookings_today_tiles",bookings_today
print time.time() - st