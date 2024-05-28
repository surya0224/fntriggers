import pymongo
import time
import numpy as np
import requests
from datetime import *
import pandas as pd
import urllib
from dateutil.relativedelta import *
from celery import group
from celery import Celery
from jupiter_AI import SYSTEM_DATE, JUPITER_DB, mongo_client, JUPITER_LOGGER
from jupiter_AI.logutils import measure


# username = urllib.quote_plus('mgadmin')
# password = urllib.quote_plus('mgadmin@2018')
# username1 = urllib.quote_plus('dbteam')
# password1 = urllib.quote_plus('KNjSZmiaNUGLmS0Bv2')

# try:
# #    dbod1 = pymongo.MongoClient("mongodb://%s:%s@13.92.251.7:42525" % (username1, password1))["internsDB"]
#     dbod1 =
#
#     print "connected"
# except Exception as e:
#     print "Not connected"


@measure(JUPITER_LOGGER)
def direct(db,
           origin,
           destination,
           depdate1,
           depdate2,
           airline):
    URL = "http://flydubai.production.vayant.com:9080"

    PARAMS = {
        "TaxQuoteRequest": {
            "ResponseType": "jsonv2",
            "SessionId": "00000000-0000-0000-0000-000000000000",
            "User": "FZIETFares1",
            "Password": "7f9bf4b49855eaeaa044bd5bdbef69b97d9296c4",
            "FlightRoutes": [
                {
                    "Id": 1,
                    "Cabin": "Any",
                    "Departure": {
                        "Cities": [
                            origin
                        ],
                        "Dates": {
                            "DepartureDates": [
                                {
                                    "DateTime": {
                                        "Date": depdate1,
                                        "Time": {
                                            "Start": "00:00:00",
                                            "End": "23:59:59"
                                        }
                                    }
                                }
                            ]
                        }
                    },
                    "Arrival": {
                        "Cities": [
                            destination
                        ],
                        "ArrivalTime": {
                            "Time": {
                                "Start": "00:00:00",
                                "End": "23:59:59"
                            }
                        }
                    },
                    "Routes": [
                        {
                            "Id": 1,
                            "Sectors": [
                                {
                                    "Id": 1,
                                    "Origin": origin,
                                    "Destination": destination,
                                    "Airline": airline,
                                    "OperatingCarriers": [
                                        airline
                                    ]
                                }
                            ]
                        }
                    ]
                },
                {
                    "Id": 2,
                    "Cabin": "Any",
                    "Departure": {
                        "Cities": [
                            destination
                        ],
                        "Dates": {
                            "DepartureDates": [
                                {
                                    "DateTime": {
                                        "Date": depdate2,
                                        "Time": {
                                            "Start": "00:00:00",
                                            "End": "23:59:59"
                                        }
                                    }
                                }
                            ]
                        }
                    },
                    "Arrival": {
                        "Cities": [
                            origin
                        ],
                        "ArrivalTime": {
                            "Time": {
                                "Start": "00:00:00",
                                "End": "23:59:59"
                            }
                        }
                    },
                    "Routes": [
                        {
                            "Id": 2,
                            "Sectors": [
                                {
                                    "Id": 1,
                                    "Origin": destination,
                                    "Destination": origin,
                                    "Airline": airline,
                                    "OperatingCarriers": [
                                        airline
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ],
            "Sellers": [
                {
                    "Code": "FZWEB",
                    "SearchRules": [
                        {
                            "FareType": "All",
                            "Airline": [
                                "All"
                            ]
                        }
                    ]
                }
            ],
            "Preferences": {
                "ShowRefundableFares": "Any",
                "ShowChangeableFares": "Any",
                "ReturnPriceDetails":
                    {
                        "YqyrBreakdown": True,
                        "AirportTaxPerFlight": True
                    }
                ,
                "CheckAvailability": False,
                "OverrideSignificantSegment": False,
                "SplitTickets": {
                    "AllowSplitTickets": False
                },
                "OptionalServices": False
            },
            "PassengerTypes": [
                {
                    "TypeId": 1,
                    "PaxType": "Adult"
                }
            ],
            "PassengerGroups": [
                {
                    "Members": [
                        {
                            "Id": 1,
                            "TypeId": 1,
                            "Cabin": "Economy"
                        }
                    ]
                }
            ]
        }
    }

    # sending get request and saving the response as response object
    # r = requests.put(url = URL, data = json.dumps(PARAMS))
    r = requests.put(url=URL, json=PARAMS)
    opt_docs = r.json()
    bdoc1 = dict()
    bdoc2 = dict()
    edoc1 = dict()
    edoc2 = dict()
    fbdoc = dict()
    fedoc = dict()

    #    z= opt_docs['SearchResponse']['FlightGroups']

    if "Response" in opt_docs:
        return 1
    elif opt_docs['SearchResponse']['FlightGroups'] == []:
        return 1
    else:
        y = 5
        for each_price_soln in opt_docs['SearchResponse']['Solutions'][0]['PricingSolutions']:
            for each_booking_code in each_price_soln['BookingSolutions'][0]['BookingCodes']:
                if each_booking_code['Rbd'][0]['Cabin'] == 'Business' and each_booking_code['Rbd'][1][
                    'Cabin'] == 'Business' and y < 10:
                    print 'Result for Business Exist'
                    j1 = each_price_soln['BookingSolutions'][0]['Price']['SegmentPrices'][0]['Tax']
                    j2 = each_price_soln['BookingSolutions'][0]['Price']['SolutionPrice']['Tax']
                    j1yqyr = each_price_soln['BookingSolutions'][0]['Price']['SegmentPrices'][0]['Yqyr']
                    j2yqyr = each_price_soln['BookingSolutions'][0]['Price']['SolutionPrice']['Yqyr']
                    j1yq = each_price_soln['BookingSolutions'][0]['Price']['YqyrBreakdown'][0]['Amount']
                    j2yq = each_price_soln['BookingSolutions'][0]['Price']['YqyrBreakdown'][0]['Amount'] + \
                           each_price_soln['BookingSolutions'][0]['Price']['YqyrBreakdown'][1]['Amount']
                    j1yr = j1yqyr - j1yq
                    j2yr = j2yqyr - j2yq
                    for each_response in each_price_soln['BookingSolutions'][0]['Price'][
                        'AirportTaxTripSegmentBreakdown']:
                        if each_response['TripSegment'] == 1:
                            bdoc1[each_response['TaxCode']] = each_response['Amount']

                    for each_response in each_price_soln['BookingSolutions'][0]['Price'][
                        'AirportTaxTripSegmentBreakdown']:
                        if each_response['TripSegment'] == 2:
                            bdoc2[each_response['TaxCode']] = each_response['Amount']

                    y = 15
                else:
                    pass
                    # print 'Result for Business Doesnt exist'

        z = dict()
        for key in bdoc1.keys():
            if key in bdoc2.keys():
                z[key] = bdoc1[key] + bdoc2[key]

        fbdoc = bdoc1.copy()
        fbdoc.update(bdoc2)
        fbdoc.update(z)

        # print bdoc2
        q = 5

        for each_price_soln in opt_docs['SearchResponse']['Solutions'][0]['PricingSolutions']:
            for each_booking_code in each_price_soln['BookingSolutions'][0]['BookingCodes']:
                if each_booking_code['Rbd'][0]['Cabin'] == 'Economy' and each_booking_code['Rbd'][1][
                    'Cabin'] == 'Economy' and q < 10:
                    print 'Result for Economy Exist'
                    y1 = each_price_soln['BookingSolutions'][0]['Price']['SegmentPrices'][0]['Tax']
                    y2 = each_price_soln['BookingSolutions'][0]['Price']['SolutionPrice']['Tax']
                    y1yqyr = each_price_soln['BookingSolutions'][0]['Price']['SegmentPrices'][0]['Yqyr']
                    y2yqyr = each_price_soln['BookingSolutions'][0]['Price']['SolutionPrice']['Yqyr']
                    y1yq = each_price_soln['BookingSolutions'][0]['Price']['YqyrBreakdown'][0]['Amount']
                    y2yq = each_price_soln['BookingSolutions'][0]['Price']['YqyrBreakdown'][0]['Amount'] + \
                           each_price_soln['BookingSolutions'][0]['Price']['YqyrBreakdown'][1]['Amount']
                    y1yr = y1yqyr - y1yq
                    y2yr = y2yqyr - y2yq
                    for each_response in each_price_soln['BookingSolutions'][0]['Price'][
                        'AirportTaxTripSegmentBreakdown']:
                        if each_response['TripSegment'] == 1:
                            edoc1[each_response['TaxCode']] = each_response['Amount']

                    for each_response in each_price_soln['BookingSolutions'][0]['Price'][
                        'AirportTaxTripSegmentBreakdown']:
                        if each_response['TripSegment'] == 2:
                            edoc2[each_response['TaxCode']] = each_response['Amount']

                    q = 15
                else:
                    pass
                    # print 'Result for Economy Doesnt exist'
        l = dict()
        for key in edoc1.keys():
            if key in edoc2.keys():
                l[key] = edoc1[key] + edoc2[key]

        fedoc = edoc1.copy()
        fedoc.update(edoc2)
        fedoc.update(l)

        for each_res in opt_docs['SearchResponse']['FlightGroups']:
            if each_res['TripSegment'] == 1:
                org = each_res['Origin']
                des = each_res['Destination']
                org_date = each_res['Departure'][0:10]

        pk = str(date.today())
        curr_date = pk[8:10] + "-" + pk[5:7] + "-" + pk[2:4]

        gk = str(datetime.now().time())
        curr_tim = gk[0:5]

        #    for each_res in opt_docs['SearchResponse']['FlightGroups']:
        #        if each_res['TripSegment'] == 2:
        #            des_date = each_res['Arrival'][0:10]

        d1 = {
            "chnl": "WEB",
            "OW_RT": "1",
            "pos": "DXB",
            "Departure_Date": org_date,
            "Currency": opt_docs['SearchResponse']['FlightGroups'][0]['MinPrice']['TotalPrice']['Currency'],
            "Origin": org,
            "Destination": des,
            "Date": curr_date,
            "Time": curr_tim,
            "Percentage_tax_breakup": "",
            "Fixed_tax_breakup": "",
            "Compartment": "J",
            "YQYR": j1yqyr,
            "YQ": j1yq,
            "YR": j1yr,
            "Fixed_tax": j1,
            "Fixed_tax_codes": bdoc1,
            "Percent_tax": 0.0,
            "Percentage_tax_codes": "",
        }

        d2 = {
            "chnl": "WEB",
            "OW_RT": "1",
            "pos": "DXB",
            "Departure_Date": org_date,
            "Currency": opt_docs['SearchResponse']['FlightGroups'][0]['MinPrice']['TotalPrice']['Currency'],
            "Origin": org,
            "Destination": des,
            "Percentage_tax_breakup": "",
            "Fixed_tax_breakup": "",
            "Date": curr_date,
            "Time": curr_tim,
            "Compartment": "Y",
            "YQYR": y1yqyr,
            "YQ": y1yq,
            "YR": y1yr,
            "Fixed_tax": y1,
            "Fixed_tax_codes": edoc1,
            "Percent_tax": 0.0,
            "Percentage_tax_codes": "",
        }

        d3 = {
            "chnl": "WEB",
            "OW_RT": "2",
            "pos": "DXB",
            "Departure_Date": org_date,
            "Currency": opt_docs['SearchResponse']['FlightGroups'][0]['MinPrice']['TotalPrice']['Currency'],
            "Percentage_tax_breakup": "",
            "Fixed_tax_breakup": "",
            "Origin": org,
            "Destination": des,
            "Date": curr_date,
            "Time": curr_tim,
            "Compartment": "J",
            "YQYR": j2yqyr,
            "YQ": j2yq,
            "YR": j2yr,
            "Fixed_tax": j2,
            "Fixed_tax_codes": fbdoc,
            "Percent_tax": 0.0,
            "Percentage_tax_codes": "",
        }

        d4 = {
            "chnl": "WEB",
            "OW_RT": "2",
            "pos": "DXB",
            "Departure_Date": org_date,
            "Currency": opt_docs['SearchResponse']['FlightGroups'][0]['MinPrice']['TotalPrice']['Currency'],
            "Origin": org,
            "Percentage_tax_breakup": "",
            "Fixed_tax_breakup": "",
            "Destination": des,
            "Date": curr_date,
            "Time": curr_tim,
            "Compartment": "Y",
            "YQYR": y2yqyr,
            "YQ": y2yq,
            "YR": y2yr,
            "Fixed_tax": y2,
            "Fixed_tax_codes": fedoc,
            "Percent_tax": 0.0,
            "Percentage_tax_codes": "",
        }

        db.JUP_DB_Tax_Master.insert(d1)
        db.JUP_DB_Tax_Master.insert(d2)
        db.JUP_DB_Tax_Master.insert(d3)
        db.JUP_DB_Tax_Master.insert(d4)
        return 1


@measure(JUPITER_LOGGER)
def connect(db, origin, destination, depdate1, depdate2, hub, airline):
    # api-endpoint
    URL = "http://flydubai.production.vayant.com:9080"

    # location given here
    # location = "delhi technological university"

    # defining a params dict for the parameters to be sent to the API
    PARAMS = {
        "TaxQuoteRequest": {
            "ResponseType": "jsonv2",
            "SessionId": "00000000-0000-0000-0000-000000000000",
            "User": "FZIETFares1",
            "Password": "7f9bf4b49855eaeaa044bd5bdbef69b97d9296c4",
            "FlightRoutes": [
                {
                    "Id": 1,
                    "Cabin": "Any",
                    "Departure": {
                        "Cities": [
                            origin
                        ],
                        "Dates": {
                            "DepartureDates": [
                                {
                                    "DateTime": {
                                        "Date": depdate1,
                                        "Time": {
                                            "Start": "00:00:00",
                                            "End": "23:59:59"
                                        }
                                    }
                                }
                            ]
                        }
                    },
                    "Arrival": {
                        "Cities": [
                            destination
                        ],
                        "ArrivalTime": {
                            "Time": {
                                "Start": "00:00:00",
                                "End": "23:59:59"
                            }
                        }
                    },
                    "Routes": [
                        {
                            "Id": 1,
                            "Sectors": [
                                {
                                    "Id": 1,
                                    "Origin": origin,
                                    "Destination": hub,
                                    "Airline": airline,
                                    "OperatingCarriers": [
                                        airline
                                    ]
                                },
                                {
                                    "Id": 2,
                                    "Origin": hub,
                                    "Destination": destination,
                                    "Airline": airline,
                                    "OperatingCarriers": [
                                        airline
                                    ]
                                }
                            ]
                        }
                    ]
                },
                {
                    "Id": 2,
                    "Cabin": "Any",
                    "Departure": {
                        "Cities": [
                            destination
                        ],
                        "Dates": {
                            "DepartureDates": [
                                {
                                    "DateTime": {
                                        "Date": depdate2,
                                        "Time": {
                                            "Start": "00:00:00",
                                            "End": "23:59:59"
                                        }
                                    }
                                }
                            ]
                        }
                    },
                    "Arrival": {
                        "Cities": [
                            origin
                        ],
                        "ArrivalTime": {
                            "Time": {
                                "Start": "00:00:00",
                                "End": "23:59:59"
                            }
                        }
                    },
                    "Routes": [
                        {
                            "Id": 2,
                            "Sectors": [
                                {
                                    "Id": 1,
                                    "Origin": destination,
                                    "Destination": hub,
                                    "Airline": airline,
                                    "OperatingCarriers": [
                                        airline
                                    ]
                                },
                                {
                                    "Id": 2,
                                    "Origin": hub,
                                    "Destination": origin,
                                    "Airline": airline,
                                    "OperatingCarriers": [
                                        airline
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ],
            "Sellers": [
                {
                    "Code": "FZWEB",
                    "SearchRules": [
                        {
                            "FareType": "All",
                            "Airline": [
                                "All"
                            ]
                        }
                    ]
                }
            ],
            "Preferences": {
                "ShowRefundableFares": "Any",
                "ShowChangeableFares": "Any",
                "ReturnPriceDetails":
                    {
                        "YqyrBreakdown": True,
                        "AirportTaxPerFlight": True
                    }
                ,
                "CheckAvailability": False,
                "OverrideSignificantSegment": False,
                "SplitTickets": {
                    "AllowSplitTickets": False
                },
                "OptionalServices": False
            },
            "PassengerTypes": [
                {
                    "TypeId": 1,
                    "PaxType": "Adult"
                }
            ],
            "PassengerGroups": [
                {
                    "Members": [
                        {
                            "Id": 1,
                            "TypeId": 1,
                            "Cabin": "Economy"
                        }
                    ]
                }
            ]
        }
    }

    each_li = dict()
    dd = dict()
    bdoc1 = dict()
    bdoc2 = dict()
    edoc1 = dict()
    edoc2 = dict()

    r = requests.put(url=URL, json=PARAMS)
    opt_docs = r.json()
    j2yq = 0

    #    l = opt_docs['SearchResponse']['FlightGroups']

    if "Response" in opt_docs:
        return 1
    elif opt_docs['SearchResponse']['FlightGroups'] == []:
        return 1
    else:
        y = 5
        for each_price_soln in opt_docs['SearchResponse']['Solutions'][0]['PricingSolutions']:
            for each_booking_code in each_price_soln['BookingSolutions'][0]['BookingCodes']:
                if each_booking_code['Rbd'][0]['Cabin'] == 'Business' and each_booking_code['Rbd'][1][
                    'Cabin'] == 'Business':
                    if each_booking_code['Rbd'][2]['Cabin'] == 'Business' and each_booking_code['Rbd'][3][
                        'Cabin'] == 'Business' and y < 10:
                        print "Business result exist"
                        j1 = each_price_soln['BookingSolutions'][0]['Price']['SegmentPrices'][0]['Tax']
                        j2 = each_price_soln['BookingSolutions'][0]['Price']['SolutionPrice']['Tax']
                        j1yqyr = each_price_soln['BookingSolutions'][0]['Price']['SegmentPrices'][0]['Yqyr']
                        j2yqyr = each_price_soln['BookingSolutions'][0]['Price']['SolutionPrice']['Yqyr']
                        j1yq = each_price_soln['BookingSolutions'][0]['Price']['YqyrBreakdown'][0]['Amount'] + \
                               each_price_soln['BookingSolutions'][0]['Price']['YqyrBreakdown'][1]['Amount']
                        for yq in each_price_soln['BookingSolutions'][0]['Price']['YqyrBreakdown']:
                            if yq['TaxCode'] == "YQ":
                                j2yq = j2yq + yq['Amount']
                        j1yr = each_price_soln['BookingSolutions'][0]['Price']['SegmentPrices'][0]['Yqyr'] - j1yq
                        j2yr = each_price_soln['BookingSolutions'][0]['Price']['SolutionPrice']['Yqyr'] - j2yq
                        jdb = pd.DataFrame(
                            each_price_soln['BookingSolutions'][0]['Price']['AirportTaxTripSegmentBreakdown'])
                        df = jdb[jdb.TripSegment != 2]
                        df2 = df.groupby(by='TaxCode')['Amount'].sum()
                        df3 = df2.reset_index()
                        print df2.head()
                        final_dict = dict()
                        for dt in df3.to_dict('records'):
                            final_dict.update({dt['TaxCode']: int(dt['Amount'])})
                        print final_dict
                        # bdoc1 = dict(df3.set_index('TaxCode').T.to_dict('records'))
                        db4 = pd.DataFrame(
                            each_price_soln['BookingSolutions'][0]['Price']['AirportTaxTripSegmentBreakdown'])
                        df5 = db4.groupby(by='TaxCode')['Amount'].sum()
                        df6 = df5.reset_index()
                        print df5.head()
                        print df5.dtypes
                        final_dict1 = dict()
                        for dt1 in df6.to_dict('records'):
                            final_dict1.update({dt1['TaxCode']: int(dt1['Amount'])})
                        print final_dict1

                        y = y + 20

        y2yq = 0
        l = 5
        for each_eprice_soln in opt_docs['SearchResponse']['Solutions'][0]['PricingSolutions']:
            for each_ebooking_code in each_eprice_soln['BookingSolutions'][0]['BookingCodes']:
                if each_ebooking_code['Rbd'][0]['Cabin'] == 'Economy' and each_ebooking_code['Rbd'][1][
                    'Cabin'] == 'Economy':
                    if each_ebooking_code['Rbd'][2]['Cabin'] == 'Economy' and each_ebooking_code['Rbd'][3][
                        'Cabin'] == 'Economy' and l < 10:
                        print "Economy result exist"
                        y1 = each_eprice_soln['BookingSolutions'][0]['Price']['SegmentPrices'][0]['Tax']
                        y2 = each_eprice_soln['BookingSolutions'][0]['Price']['SolutionPrice']['Tax']
                        y1yqyr = each_eprice_soln['BookingSolutions'][0]['Price']['SegmentPrices'][0]['Yqyr']
                        y2yqyr = each_eprice_soln['BookingSolutions'][0]['Price']['SolutionPrice']['Yqyr']
                        y1yq = each_eprice_soln['BookingSolutions'][0]['Price']['YqyrBreakdown'][0]['Amount'] + \
                               each_eprice_soln['BookingSolutions'][0]['Price']['YqyrBreakdown'][1]['Amount']
                        for eyq in each_eprice_soln['BookingSolutions'][0]['Price']['YqyrBreakdown']:
                            if eyq['TaxCode'] == "YQ":
                                y2yq = y2yq + eyq['Amount']
                        y1yr = each_eprice_soln['BookingSolutions'][0]['Price']['SegmentPrices'][0]['Yqyr'] - y1yq
                        y2yr = each_eprice_soln['BookingSolutions'][0]['Price']['SolutionPrice']['Yqyr'] - y2yq
                        dbe = pd.DataFrame(
                            each_eprice_soln['BookingSolutions'][0]['Price']['AirportTaxTripSegmentBreakdown'])
                        df7 = dbe[dbe.TripSegment != 2]
                        df8 = df7.groupby(by='TaxCode')['Amount'].sum()
                        df9 = df8.reset_index()
                        print df9.head()
                        final_dict2 = dict()
                        for dt3 in df9.to_dict('records'):
                            final_dict2.update({dt3['TaxCode']: int(dt3['Amount'])})
                        print final_dict2
                        #                edoc1 = df9.set_index('TaxCode').T.to_dict('records')
                        dbe1 = pd.DataFrame(
                            each_eprice_soln['BookingSolutions'][0]['Price']['AirportTaxTripSegmentBreakdown'])
                        dfe1 = dbe1.groupby(by='TaxCode')['Amount'].sum()
                        dfe2 = dfe1.reset_index()
                        print dfe1.head()
                        final_dict4 = dict()
                        for dt4 in dfe2.to_dict('records'):
                            final_dict4.update({dt4['TaxCode']: int(dt4['Amount'])})
                        print final_dict4

                        #                edoc2 = dfe2.set_index('TaxCode').T.to_dict('records')
                        l = l + 20

        for each_res in opt_docs['SearchResponse']['FlightGroups']:
            if each_res['TripSegment'] == 1:
                org = each_res['Origin']
                des = each_res['Destination']
                org_date = each_res['Departure'][0:10]

        pk = str(date.today())
        curr_date = pk[8:10] + "-" + pk[5:7] + "-" + pk[2:4]

        gk = str(datetime.now().time())
        curr_tim = gk[0:5]

        d1 = {
            "chnl": "WEB",
            "OW_RT": "1",
            "pos": "DXB",
            "Departure_Date": org_date,
            "Currency": str(opt_docs['SearchResponse']['FlightGroups'][0]['MinPrice']['TotalPrice']['Currency']),
            "Origin": org,
            "Percentage_tax_breakup": "",
            "Fixed_tax_breakup": "",
            "Destination": des,
            "Date": curr_date,
            "Time": curr_tim,
            "Compartment": "J",
            "YQYR": j1yqyr,
            "YQ": j1yq,
            "YR": j1yr,
            "Fixed_tax": j1,
            "Fixed_tax_codes": final_dict,
            "Percent_tax": 0.0,
            "Percentage_tax_codes": "",
        }

        d2 = {
            "chnl": "WEB",
            "OW_RT": "1",
            "pos": "DXB",
            "Departure_Date": org_date,
            "Currency": opt_docs['SearchResponse']['FlightGroups'][0]['MinPrice']['TotalPrice']['Currency'],
            "Origin": org,
            "Percentage_tax_breakup": "",
            "Fixed_tax_breakup": "",
            "Destination": des,
            "Date": curr_date,
            "Time": curr_tim,
            "Compartment": "Y",
            "YQYR": y1yqyr,
            "YQ": y1yq,
            "YR": y1yr,
            "Fixed_tax": y1,
            "Fixed_tax_codes": final_dict2,
            "Percent_tax": 0.0,
            "Percentage_tax_codes": "",
        }

        d3 = {
            "chnl": "WEB",
            "OW_RT": "2",
            "pos": "DXB",
            "Departure_Date": org_date,
            "Currency": opt_docs['SearchResponse']['FlightGroups'][0]['MinPrice']['TotalPrice']['Currency'],
            "Origin": org,
            "Percentage_tax_breakup": "",
            "Fixed_tax_breakup": "",
            "Destination": des,
            "Date": curr_date,
            "Time": curr_tim,
            "Compartment": "J",
            "YQYR": j2yqyr,
            "YQ": j2yq,
            "YR": j2yr,
            "Fixed_tax": j2,
            "Fixed_tax_codes": final_dict1,
            "Percent_tax": 0.0,
            "Percentage_tax_codes": "",
        }

        d4 = {
            "chnl": "WEB",
            "OW_RT": "2",
            "pos": "DXB",
            "Departure_Date": org_date,
            "Currency": opt_docs['SearchResponse']['FlightGroups'][0]['MinPrice']['TotalPrice']['Currency'],
            "Origin": org,
            "Destination": des,
            "Percentage_tax_breakup": "",
            "Fixed_tax_breakup": "",
            "Date": curr_date,
            "Time": curr_tim,
            "Compartment": "Y",
            "YQYR": y2yqyr,
            "YQ": y2yq,
            "YR": y2yr,
            "Fixed_tax": y2,
            "Fixed_tax_codes": final_dict4,
            "Percent_tax": 0.0,
            "Percentage_tax_codes": "",
        }

        db.JUP_DB_Tax_Master.insert(d1)
        db.JUP_DB_Tax_Master.insert(d2)
        db.JUP_DB_Tax_Master.insert(d3)
        db.JUP_DB_Tax_Master.insert(d4)
        return 1


@measure(JUPITER_LOGGER)
def recent_days(db, dt1, op_days, l):
    day_num = {
        "1": MO,
        "2": TU,
        "3": WE,
        "4": TH,
        "5": FR,
        "6": SA,
        "7": SU
    }

    dates = []
    TODAY = date.today()

    for i in range(1, len(day_num) + 1, 1):
        z = TODAY + relativedelta(days=60 + l, weekday=day_num[str(i)])
        dates.append(str(z))

    dy = []
    for z in dt1[op_days]:
        for i in range(0, 10):
            if z[i] == " ":
                pass
            else:
                l = z[i]
                dy.append(dates[int(l) - 1])
                break

    return dy
    # db.getCollection('JUP_DB_Capacity').aggregate([{"$group":{"_id":{"airline":"$airline","origin":"$origin","destination":"$destination"},"op_days":{ "$first":"$op_days"}}},{"$project":{"_id":0,"airline":"$_id.airline","origin":"$_id.origin","destination":"$_id.destination","op_days":1,"last_update_date":"2018-04-01"}}],{allowDiskUse:true})


@measure(JUPITER_LOGGER)
def main(db):
    # try:
    #    dbod = pymongo.MongoClient("mongodb://%s:%s@104.211.201.88:27022" % (username, password))["fzDB_stg"]
    #    print "connected"
    # except Exception as e:
    #     print "Not connected"

    ab = db.JUP_DB_Capacity.aggregate([
        {
            "$sort": {
                "last_update_date": -1
            }
        },
        {
            "$limit": 1
        },
        {
            "$project": {
                "_id": 0,
                "last_update_date": 1
            }
        }
    ], allowDiskUse=True)

    # db.getCollection('JUP_DB_Capacity').aggregate([{$sort:{"last_update_date":-1}},{$limit:1},{$project:{"_id":0,"last_update_date":1}}],{allowDiskUse:true})

    latest = list(ab)
    # date= latest['last_update_date']
    dt = pd.DataFrame(latest)
    latest = str(dt['last_update_date'][0])
    # print latest

    ab1 = db.JUP_DB_Capacity.aggregate([
        {
            "$group": {
                "_id": {
                    "airline": "$airline",
                    "origin": "$origin",
                    "destination": "$destination",
                },
                "First_op_day": {
                    "$first": "$op_days"
                }
            }
        },

        {
            "$project": {
                "_id": 0,
                "airline": "$_id.airline",
                "origin": "$_id.origin",
                "destination": "$_id.destination",
                "First_op_day": 1,
                "last_update_date": latest
            }
        }
    ], allowDiskUse=True)

    latest1 = list(ab1)
    # date= latest['last_update_date']
    dt1 = pd.DataFrame(latest1)

    # print dt1
    dat = recent_days(db, dt1, 'First_op_day', 0)
    dates_of_dept = pd.DataFrame(dat)
    dates_of_dept.columns = ['dates_of_dept']
    out_gng = dt1.join(dates_of_dept)
    # print "out_gng task done"

    dat2 = recent_days(db, dt1, 'First_op_day', 40)
    dates_of_ret = pd.DataFrame(dat2)
    dates_of_ret.columns = ['dates_of_ret']
    ret = dt1.join(dates_of_ret)
    ret_gng = ret.rename(index=str, columns={"origin": "destination", "destination": "origin"})
    # print "ret_gng task done"

    fin = pd.merge(out_gng, ret_gng, how='inner', on=["origin", "destination", "airline", "last_update_date"])
    print fin

    ab2 = db.JUP_DB_Carrier_hubs.aggregate([
        {
            "$group": {
                "_id": {
                    "hub": "$hub",
                    "carrier": "$carrier"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "hub": "$_id.hub",
                "carrier": "$_id.carrier"}
        }], allowDiskUse=True)

    latest1 = list(ab2)
    # date= latest['last_update_date']
    dt1 = pd.DataFrame(latest1)

    db.JUP_DB_Tax_Master.remove()

    hub_data = dt1.rename(index=str, columns={"carrier": "airline"})
    hub_data_origin = hub_data.rename(index=str, columns={"hub": "origin"})
    hub_data_dest = hub_data.rename(index=str, columns={"hub": "destination"})

    fin_hub = pd.merge(fin, hub_data, how='inner', on=["airline"])

    fin_hub_1 = pd.merge(fin_hub, hub_data_origin, how='inner', on=["origin", "airline"])
    fin_hub_2 = pd.merge(fin_hub, hub_data_dest, how='inner', on=["destination", "airline"])
    direct_flights = pd.merge(fin_hub_1, fin_hub_2, how='outer')

    """
    Can form multiple groups of the task "direct_tax" which will have the 6 parameters and each set of parameter is independent of the other.

    Once the group of tasks is formed we can call the group.
    """

    for i in range(0, len(direct_flights)):
        direct(db, direct_flights['origin'][i], direct_flights['destination'][i], direct_flights['dates_of_dept'][i],
               direct_flights['dates_of_ret'][i], direct_flights['airline'][i])

    df_imp = pd.concat([fin_hub, direct_flights])
    connecting_flights = df_imp.drop_duplicates(keep=False)

    for i in range(0, len(connecting_flights)):
        connect(db, connecting_flights['origin'], connecting_flights['destination'],
                connecting_flights['dates_of_dept'], connecting_flights['dates_of_ret'], connecting_flights['hub'],
                connecting_flights['airline'])
    return 1


if __name__ == '__main__':
    client = mongo_client()
    db = client[JUPITER_DB].noCursorTimeout()
    main(db)
    db.close()
    client.close()






