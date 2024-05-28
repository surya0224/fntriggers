from jupiter_AI.triggers.data_change_send import identify_trigger
c = {
    'collection':'JUP_DB_Competitor_Pricing_Data',
    'field': 'price',
    'action': 'change'
}
o = {
    "airline": "QR",
    "pos": "UAE",
    "origin": "DXB",
    "destination": "DOH",
    "compartment": "Y",
    "farebasis": "OJAEP1NE",
    "price": 700.0,
    "currency": "AED",
    "rating": 1.2,
    "circular_period_start_date": "2016-08-15",
    "circular_period_end_date": "2016-08-15",
}
n = {
    "airline": "QR",
    "pos": "UAE",
    "origin": "DXB",
    "destination": "DOH",
    "compartment": "Y",
    "farebasis": "OJAEP1NE",
    "price": 900.0,
    "currency": "AED",
    "rating": 1.2,
    "circular_period_start_date": "2016-08-15",
    "circular_period_end_date": "2016-08-15",
}
print c
print o
print n
l = identify_trigger(c,o,n)
