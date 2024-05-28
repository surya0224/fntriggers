"""
"""
import datetime
from jupiter_AI import client, JUPITER_LOGGER
from jupiter_AI.logutils import measure
from jupiter_AI.network_level_params import JUPITER_DB
from jupiter_AI.network_level_params import SYSTEM_DATE, today
from jupiter_AI.triggers.data_level.MainClass import DataLevel
#db = client[JUPITER_DB]

@measure(JUPITER_LOGGER)
def get_bookings_target(pos, origin, destination, compartment, dep_date_from, dep_date_to):
    """
    """
    obj = DataLevel(data={
        'pos': pos,
        'origin': origin,
        'destination': destination,
        'compartment': compartment
    }, system_date=SYSTEM_DATE)
    return obj.get_target_data(dep_date_start=dep_date_from,
                               dep_date_end=dep_date_to,
                               parameter='pax')


@measure(JUPITER_LOGGER)
def get_bookings_vlyr_vtgt(pos, origin, destination, compartment, dep_date_from, dep_date_to):
    """
    """
    today_obj = today,
    if not dep_date_from:
        dep_date_from = datetime.datetime.strftime(today_obj, '%Y-%m-%d')
    if not dep_date_to:
        dep_date_to = '9999-12-31'
    print 'DEP DATE FROM', dep_date_from
    print 'DEP DATE TO', dep_date_to
    query = dict()
    if pos:
        query['pos'] = pos
    if origin and destination:
        query['od'] = origin + destination
        # if destination:
        # query['destination'] = destination
    if compartment:
        query['compartment'] = compartment
    query['dep_date'] = {'$gte':dep_date_from,
                         '$lte':dep_date_to}
    query['book_date']= {'$lte':SYSTEM_DATE}

    print query
    crsr_bookings = db.JUP_DB_Booking_BookDate.aggregate(
    # // Pipeline
    [
        # // Stage 1
        {
            '$match': query
        },

        # // Stage 2
        {
            '$group': {
                '_id': None,
                'bookings': {'$sum':'$pax'},
                'bookings_ly': {'$sum': '$pax_1'}
            }
        },

        # // Stage 3
        {
            '$project': {
                '_id': 0,
                'bookings': '$bookings',
                'bookings_ly': '$bookings_ly',
                'bookings_vlyr': {
                    '$cond': {
                        'if': {'$gt': ['$bookings_ly', 0]},
                        'then': {
                            '$multiply':[{'$divide': [{'$subtract': ['$bookings', '$bookings_ly']}, '$bookings_ly']},
                                         100]
                        }
                        ,
                        'else': 'NA'
                    }
                }
            }
        },
    ]
    )

    list_bookings = list(crsr_bookings)
    print list_bookings
    response = dict()
    if len(list_bookings) == 1:
        bookings = list_bookings[0]['bookings']
        if list_bookings[0]['bookings_vlyr'] and list_bookings[0]['bookings_vlyr'] != 'NA':
            bookings_vlyr = round(list_bookings[0]['bookings_vlyr'], 2)
        else:
            bookings_vlyr = list_bookings[0]['bookings_vlyr'].encode()
        if bookings and bookings != 0:
            bookings_target = get_bookings_target(pos,
                                                  origin,
                                                  destination,
                                                  compartment,
                                                  dep_date_from,
                                                  dep_date_to)
            print bookings_target
            if bookings_target and bookings_target != 0:
                bookings_vtgt = round(float(bookings - bookings_target)*100/bookings_target,2)
            else:
                bookings_vtgt = "NA"
        else:
            bookings_vtgt = "NA"
    else:
        bookings = 'NA'
        bookings_vlyr = 'NA'
        bookings_vtgt = 'NA'

    response['bookings'] = bookings
    response['bookings_vlyr'] = bookings_vlyr
    response['bookings_vtgt'] = bookings_vtgt
    return response


if __name__ == '__main__':
    print get_bookings_vlyr_vtgt('DXB', 'DXB', 'DOH', 'Y', '2017-05-01', '2017-05-30')