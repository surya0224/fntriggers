from jupiter_AI import client, JUPITER_DB, JUPITER_LOGGER
from jupiter_AI.logutils import measure


@measure(JUPITER_LOGGER)
def func():
    db = client[JUPITER_DB]
    collection_1 = db.JUP_DB_ATPCO_Fares_Rules
    cur1 = collection_1.find({}, no_cursor_timeout=True)

    count = 0
    for k in cur1:
        pseudo_origin = k['origin']
        pseudo_destination = k['destination']

        cur2 = db.JUP_DB_City_Airport_Mapping.find({'Airport_Code': {'$in': [k['origin'], k['destination']]}},
                                                   no_cursor_timeout=True)

        for j in cur2:
            if k['origin'] == j['Airport_Code']:
                pseudo_origin = j['City_Code']
            elif k['destination'] == j['Airport_Code']:
                pseudo_destination = j['City_Code']

        collection_1.update({'_id': k['_id']}, {
            '$set': {'pseudo_origin': pseudo_origin, 'pseudo_destination': pseudo_destination,
                     'pseudo_od': pseudo_origin + pseudo_destination}})
        count += 1
        if count % 1000 == 0:
            print count, 'done'
    print count, 'done'

    cur1.close()


if __name__ == '__main__':
    func()
