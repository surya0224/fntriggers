import pymongo
import json
from collections import defaultdict
from jupiter_AI import client, JUPITER_DB, Host_Airline_Code, Host_Airline_Hub, JUPITER_LOGGER
from jupiter_AI.logutils import measure
db = client[JUPITER_DB]

@measure(JUPITER_LOGGER)
def get_ms_vals(month=None,year=None,pos=None,od=None,compartment=None):
    query = dict()

    if pos:
        query['pos'] = pos

    if od:
        query['od'] = od

    if compartment:
        query['compartment'] = compartment

    if month:
        query['month'] = month
        if year:
            query['year'] = year
    else:
        query = {
            '$or':[
                {'month':{'$gte':5},'year':2017},
                {'year':{'$gt':2017}}
            ]
        }

    db.JUP_DB_Market_Share.aggregate(

        # Pipeline
        [
            # Stage 1
            {
                '$match': query
            },

            # Stage 2
            {
                '$group': {
                    '_id':{
                        'pos':'$pos',
                        'od':'$od',
                        'compartment':'$compartment',
                        'carrier':'$MarketingCarrier1'
                    },
                    'pax':{'$sum':'$pax'}
                }
            },

            # Stage 3
            {
                '$group': {
                    '_id':{
                        'pos':'$_id.pos',
                        'od':'$_id.od',
                        'compartment':'$_id.compartment'
                    },
                    'market_size':{'$sum':'$pax'},
                    'carrier_details':{
                        '$push':{
                            'carrier':'$_id.carrier',
                            'pax':'$pax'
                        }
                    }
                }
            },

            # Stage 4
            {
                '$project':{
                    '_id':0,
                    'pos':'$_id.pos',
                    'od':'$_id.od',
                    'compartment':'$_id.compartment',
                    'market_size':'$market_size',
                    'carrier_details':'$carrier_details'

                }
            },

            # Stage 4.1
            {
                '$unwind':'$carrier_details'
            },

            # Stage 5
            {
                '$project':{
                    'pos':'$pos',
                    'od':'$od',
                    'compartment':'$compartment',
                    'carrier':'$carrier_details.carrier',
                    'market_share':{
                        '$multiply':
                            [
                                {
                                    '$divide':
                                        [
                                            '$carrier_details.pax',
                                            '$market_size'
                                        ]
                                },
                                100
                            ]
                    }
                }
            },

            # Stage 6
            {
                '$out':'temp_col_ms'
            }

        ],allowDiskUse=True

        # Created with Studio 3T, the IDE for MongoDB - https://studio3t.com/

    )
    data = dict()
    crsr =  db.temp_col_ms.find()
    # print list(crsr)
    for doc in crsr:
        try:
            data[(doc['pos'].encode(),doc['od'].encode(),doc['compartment'].encode())]
        except KeyError:
            data[(doc['pos'].encode(),doc['od'].encode(),doc['compartment'].encode())] = dict()
        data[(doc['pos'].encode(),doc['od'].encode(),doc['compartment'].encode())][doc['carrier'].encode()] = round(doc['market_share'],2)

    return data


@measure(JUPITER_LOGGER)
def get_rating_vals():
    db.JUP_DB_Competitor_Ratings.aggregate(

        # Pipeline
        [
            # Stage 1
            {
                '$sort': {
                    'last_update_date':-1
                }
            },

            # Stage 2
            {
                '$group': {
                    '_id':{
                        'od':'$od',
                        'carrier':'$airline'
                    },
                    'rating':{'$first':'$competitor_rating'}
                }
            },

            # Stage 3
            {
                '$group': {
                    '_id':'$_id.od',
                    'rating_details':{
                        '$push':{
                            'carrier':'$_id.carrier',
                            'rating':'$rating'
                        }
                    }
                }
            },

            # Stage 4
            {
                '$out': "temp_ratings_collection"
            }

        ],allowDiskUse=True
    )

    crsr = db.temp_ratings_collection.find()
    data = defaultdict(dict)

    for doc in crsr:
        for carrier_doc in doc['rating_details']:
            data[doc['_id'].encode()][carrier_doc['carrier'].encode()] = carrier_doc['rating']

    print data['DXBDOH']
    print data['DOHDAC']
    return data


@measure(JUPITER_LOGGER)
def get_fares_recommended_fares(pos, origin, destination, compartment, dep_month, dep_year):
    """
    """
    od = origin + destination
    ms_data = get_ms_vals(month=dep_month,year=dep_year,pos=pos,od=od,compartment=compartment)
    ratings_data = get_rating_vals()
    crsr = db.JUP_DB_ATPCO_Fares.find({
        'origin':origin,
        'destination':destination,
        'compartment':compartment
    }).sort({'total_fare':1})
    data = list(crsr)
    response_data = []

    for doc,index in enumerate(data):
        if doc['competitor_farebasis']:
            market_share_doc = ms_data[(pos,od,compartment)]
            print 'MS', market_share_doc
            ratings_doc = ratings_data[od]
            print 'RATINGS', ratings_doc

            norm_ms_data = dict()
            norm_ms_data[Host_Airline_Code] = market_share_doc[Host_Airline_Code]
            total_market_share = 0
            total_market_share += market_share_doc[Host_Airline_Code]
            for comp_rec in fare_doc['competitor_farebasis']:
                total_market_share += market_share_doc[comp_rec['carrier']]
                norm_ms_data[comp_rec['carrier']] = market_share_doc[comp_rec['carrier']]

            for key in norm_ms_data:
                norm_ms_data[key] = norm_ms_data[key]/ total_market_share

            contri = 0
            contri += fare_doc['total_fare'] * norm_ms_data[Host_Airline_Code] / ratings_doc[Host_Airline_Code]

            for comp_rec in fare_doc['competitor_farebasis']:
                contri += comp_rec['fare'] * norm_ms_data[comp_rec['carrier']] / ratings_doc[comp_rec['carrier']]

            recofare = contri * ratings_doc[Host_Airline_Code]
            print 'Fare', fare_doc['total_fare'], "Recommended Fare", recofare
            doc['recommended_fare'] = recofare



@measure(JUPITER_LOGGER)
def update_reco_sellups():
    pass


if __name__=='__main__':
    # get_fares_recommended_fares('DXB','DXB','DOH','Y',5,2017)
    ms_data = get_ms_vals()
    print 'MARKET SHARE VALUES OBTAINED'
    ratings_data = get_rating_vals()
    print 'RATING VALUES OBTAINED'
    crsr = db.JUP_DB_ATPCO_Fares.find({'competitor_farebasis':{'$ne':None}})
    print 'Fare Records TB Updated',crsr.count()

    for fare_doc in crsr:
        pos = fare_doc['origin']
        od = fare_doc['OD']
        compartment = fare_doc['compartment']
        print pos,od,compartment
        try:
            market_share_doc = ms_data[(pos,od,compartment)]
            print 'MS', market_share_doc
            ratings_doc = ratings_data[od]
            print 'RATINGS', ratings_doc

            norm_ms_data = dict()
            norm_ms_data[Host_Airline_Code] = market_share_doc[Host_Airline_Code]
            total_market_share = 0
            total_market_share += market_share_doc[Host_Airline_Code]
            for comp_rec in fare_doc['competitor_farebasis']:
                total_market_share += market_share_doc[comp_rec['carrier']]
                norm_ms_data[comp_rec['carrier']] = market_share_doc[comp_rec['carrier']]

            for key in norm_ms_data:
                norm_ms_data[key] = norm_ms_data[key]/ total_market_share

            contri = 0
            contri += fare_doc['total_fare'] * norm_ms_data[Host_Airline_Code] / ratings_doc[Host_Airline_Code]

            for comp_rec in fare_doc['competitor_farebasis']:
                contri += comp_rec['fare'] * norm_ms_data[comp_rec['carrier']] / ratings_doc[comp_rec['carrier']]

            recofare = contri * ratings_doc[Host_Airline_Code]
            print 'Fare', fare_doc['total_fare'], "Recommended Fare", recofare

            db.JUP_DB_ATPCO_Fares.update(
                {
                    '_id':fare_doc['_id']
                }
                ,
                {
                    '$set':
                        {
                            'recommended_fare': recofare
                        }
                }
            )



        except KeyError as e:
            print e

