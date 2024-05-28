from jupiter_AI import JUPITER_DB, client, SYSTEM_DATE, today, Host_Airline_Code, Host_Airline_Hub
from datetime import datetime, timedelta

db = client[JUPITER_DB]

one_week_date = today + timedelta(days=7)
system_date_plus_seven = one_week_date.strftime('%Y-%m-%d')
system_date_n = str(0) + today.strftime('%y%m%d')
system_date_plus_seven_n = str(0) + one_week_date.strftime('%y%m%d')
print system_date_n, system_date_plus_seven_n

print 'Removing old docs...'
db.JUP_DB_ATPCO_Fares_Expiring.remove()

print 'Querying new docs....'

cur = db.JUP_DB_ATPCO_Fares_new.find({
    'carrier': Host_Airline_Code,
    'fare_include': True,
    '$or': [
        {
            'Footnotes.Cat_14_FN.TRAVEL_DATES_EXP': {'$lte': system_date_plus_seven_n, '$gte': system_date_n}
        },
        {
            'cat_14.TRAVEL_DATES_EXP': {'$lte': system_date_plus_seven_n, '$gte': system_date_n}
        },
        {
            'Footnotes.Cat_15_FN.SALE_DATES_LATEST_TKTG': {'$lte': system_date_plus_seven_n, '$gte': system_date_n}
        },
        {
            'cat_15.SALE_DATES_LATEST_TKTG': {'$lte': system_date_plus_seven_n, '$gte': system_date_n}
        },
        {
            'effective_to': {'$lte': system_date_plus_seven, '$gte': SYSTEM_DATE}
        }
    ]
}, no_cursor_timeout=True)

bulk = []
t = 0
count = 0
for i in cur:
    if t == 100:
        print 'inserting:', count
        db.JUP_DB_ATPCO_Fares_Expiring.insert(bulk)
        bulk = []
        bulk.append(i)
        t = 1
    else:
        bulk.append(i)
        t += 1
    count += 1
if t > 0:
    print 'inserting:', count
    db.JUP_DB_ATPCO_Fares_Expiring.insert(bulk)