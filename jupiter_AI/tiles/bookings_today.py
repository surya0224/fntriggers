from collections import defaultdict
from copy import deepcopy




def get_bookings_today_tiles(afilter):
    #   bookings_runrate
    #   bookings_profile
    #   bookings_strength
    pass


def bookings_runrate(afilter):
    """
        region
        country
        pos
        origin
        destination
        compartment
    """
    afilter = deepcopy(defaultdict(list, afilter))
    query = dict()
    response = dict()
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
        query['$or'] = od_build
    if afilter['compartment']:
        query['compartment'] = {'$in': afilter['compartment']}

    query['dep_date'] = {'$gte': afilter['fromDate'], '$lte': afilter['toDate']}

    if not afilter['origin'] == [] or not afilter['destination'] == []:
        if afilter['flag'] == 'true':
            cursor = db['JUP_DB_Booking_DepDate'].aggregate([{'$match': {'region': {'$in': afilter['region']},
                                                                         'country': {'$in': afilter['country']},
                                                                         'pos': {'$in': afilter['pos']},
                                                                         'origin': {'$in': afilter['origin']},
                                                                         'destination': {'$in': afilter['destination']},
                                                                         'compartment': {'$in': afilter['compartment']},
                                                                         'dep_date': {'$gte': afilter['fromDate'],
                                                                                      '$lte': afilter['toDate']}}},
                                                             {'$group': {
                                                                 '_id': {'region': "$region", 'country': "$country",
                                                                         'pos': "$pos", 'origin': '$origin',
                                                                         'destination': '$destination',
                                                                         'compartment': '$compartment'},
                                                                 'bookings': {'$sum': '$pax'}}}])
        else:
            if afilter['flag'] == 'true':
                cursor = db['JUP_DB_Cumulative_Trx_Date'].aggregate([{'$match': {'region': {'$in': afilter['region']},
                                                                                 'country': {'$in': afilter['country']},
                                                                                 'pos': {'$in': afilter['pos']},
                                                                                 'compartment': {
                                                                                     '$in': afilter['compartment']},
                                                                                 'book_date': {'$in': ['2016-09-18',
                                                                                                       '2016-09-20']}}},
                                                                     {'$group': {'_id': {'region': "$region",
                                                                                         'country': "$country",
                                                                                         'pos': "$pos",
                                                                                         'compartment': '$compartment'},
                                                                                 'tobookings': {'$max': '$pax'},
                                                                                 'frombookings': {'$min': '$pax'}}},
                                                                     {'$project': {'region': '$_id.region',
                                                                                   'country': '$_id.country',
                                                                                   'pos': '$_id.pos',
                                                                                   'compartment': '$_id.compartment',
                                                                                   'bookings': {
                                                                                       '$subtract': ['$tobookings',
                                                                                                     '$frombookings']}}}])
            else:
                cursor = db['JUP_DB_Cumulative_Dep_Date'].aggregate([{'$match': {'region': {'$in': afilter['region']},
                                                                                 'country': {'$in': afilter['country']},
                                                                                 'pos': {'$in': afilter['pos']},
                                                                                 'compartment': {
                                                                                     '$in': afilter['compartment']},
                                                                                 'book_date': {
                                                                                     '$gte': afilter['fromDate'],
                                                                                     '$lte': afilter[
                                                                                         'toDate']}}},
                                                                     {'$group': {'_id': {'region': "$region",
                                                                                         'country': "$country",
                                                                                         'pos': "$pos",
                                                                                         'compartment': '$compartment'},
                                                                                 'bookings': {'$sum': '$pax'}}}])

    else:
        pass

    bookings = 0
    adata = list(cursor)
    print adata
    if len(adata) != 0:
        for i in adata:
            bookings += i['bookings']
            print afilter['fromDate']
            print afilter['toDate']
            # tot_period = get_day_difference(afilter['fromDate'], afilter['toDate'])
            Runrate = float((bookings) / (tot_period))
        return Runrate

    else:
        pass
        # e1=errorClass.ErrorObject(errorClass.ErrorObject.ERROR,
        # self.get_module_name(),
        # self.get_arg_lists(inspect.currentframe()))
        # e1.append_to_error_list("Expected 1 document for bookings but got " +str(len(afilter)))
        # raise e1
