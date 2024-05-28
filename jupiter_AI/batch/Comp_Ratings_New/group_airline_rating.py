def run():
    """
     Author: Prem Narasimhan
     Completed date:
         first version: Mar 3, 2017
     This program calculates ratings for the "Product Rating" group.

     Input collections:
         JUP_DB_Data_Competitor_Weights
         JUP_DB_Market_Share
     Output collection:
         JUP_DB_Data_Competitor_Ratings
     For testing:
         uncomment one layer at end of code and run the code
     For live:
        call main() with list of pos/origin/destination/compartment/airline
     Overall logic:
         1. Market share records are read from "JUP_DB_Market_Share" (which is at pos/od/compartment/airline/year/month level) into "dict_mkt_share_group_records"
         2. the data in "dict_mkt_share_group_records" is compiled into the dictionary "dict_mkt_share_results"
         3. average mkt_share is computed for each ailrine within pos/od/compartment
         4. mkt_share is directly scaled from 0.0 to 10 - this is the final score
         5. records are updated into "JUP_DB_Competitor_Ratings" collection
     Error handling:
         1. If the error level=WARNING, errors are collected in obj_err_main (defined at global level) but processing continues
         2. At the end of the program, the errors in obj_err_main are flushed to JUP_DB_Errors_All

     This version does overall ratings correctly but groupwise ratings have not been implemented
    """
    import jupiter_AI.common.ClassErrorObject as error_class
    import datetime
    import json
    import time
    from jupiter_AI import network_level_params as net
    import inspect
    import collections
    import pymongo
    from bson.objectid import ObjectId
    from datetime import datetime
    from time import gmtime, strftime
    from copy import deepcopy
    # import make_unique_list
    import scipy.stats as ss


    def main():
        query=dict()

        dict1={'Category':'At Airport','Sub_Category':'economy class','Features':'baggage weights'}
        dict2={'Category':'At Airport','Sub_Category':'business class','Features':'baggage weights'}
        dict3={'Category':'At Airport','Sub_Category':'AIRPORT INFORMATION','Features':'baggage status'}
        dict4={'Category':'At Airport','Sub_Category':'GROUND / AIRPORT','Features':'bulky-baggage counter'}
        dict5={'Category':'At Airport','Sub_Category':'AIRPORT INFORMATION','Features':'home printed baggage tags'}
        dict6={'Category':'At Airport','Sub_Category':'FIRST CLASS AT THE AIRPORT','Features':'priority baggage delivery'}
        dict7={'Category':'Pre Booking','Sub_Category':'Travel insurance','Features':'Baggage'}
        dict8={'Category':'Pre Booking','Sub_Category':'Essential Information','Features':'Baggage'}
        dict9={'Category':'Pre Booking','Sub_Category':'frequent flier program','Features':'free baggage allowance'}
        dict10={'Category':'Pre Booking','Sub_Category':'frequent flier program','Features':'priority baggage handling'}
        dict11={'Category':'In Flight','Sub_Category':'Baggage allowance'}
        dict12={'Category':'Post Flight','Sub_Category':'Baggage'}
        query_or1=[dict1, dict2, dict3, dict4, dict5, dict6, dict7, dict8, dict9, dict10, dict11, dict12]


        dict21={'Category':'At Airport','Sub_Category':'Services','Features':'corporate meeting and working'}
        dict22={'Category':'Pre Booking','Sub_Category':'Arranging for Corporate Travel','Features':'Meetings, Incentives,Conventions & Exhibitions'}
        dict23={'Category':'Pre Booking','Sub_Category':'Arranging for Corporate Travel','Features':'Business Rewards'}
        dict24={'Category':'Pre Booking','Sub_Category':'Arranging for Corporate Travel','Features':'Event Planning'}
        dict25={'Category':'Post Flight','Sub_Category':'Arrival Lounge','Features':'corporate meeting and working'}
        query_or2=[dict21, dict22, dict23, dict24, dict25]


        dict31={'Category':'In-Flight','Sub_Category':'Crew','Features':'safety equipment procedures'}
        dict32={'Category':'In-Flight','Sub_Category':'special assistance','Features':'child car safety seat'}
        query_or3=[dict31, dict32]

        dict41={'Category':'At Airport','Sub_Category':'BOARDING','Features':'ground professionals'}
        dict42={'Category':'At Airport','Sub_Category':'GROUND / AIRPORT'}
        query_or4=[dict41, dict42]

        dict51={'Category':'At Airport','Sub_Category':'FIRST CLASS AT THE AIRPORT','Features':'check in services'}
        dict52={'Category':'At Airport','Sub_Category':'FIRST CLASS AT THE AIRPORT','Features':'Limousine service'}
        dict53={'Category':'At Airport','Sub_Category':'FIRST CLASS AT THE AIRPORT','Features':'Valet parking service'}
        dict54={'Category':'At Airport','Sub_Category':'GROUND / AIRPORT','Features':'Transfer services'}
        dict55={'Category':'At Airport','Sub_Category':'GROUND / AIRPORT','Features':'Quality of Check-in service'}
        dict56={'Category':'At Airport','Sub_Category':'GROUND / AIRPORT','Features':'Airline Lounge Staff Service standards'}
        dict57={'Category':'In Flight','Sub_Category':'Crew','Features':'Consistency of Service'}
        dict58={'Category':'In Flight','Sub_Category':'Crew','Features':'Friendliness and servicehospitality'}
        dict59={'Category':'In Flight','Sub_Category':'Crew','Features':'Meal service efficiency'}
        dict60={'Category':'In Flight','Sub_Category':'Crew','Features':'service procedures '}
        dict61={'Category':'In Flight','Sub_Category':'Crew','Features':'Service Attentiveness /Efficiency'}
        dict62={'Category':'In Flight','Sub_Category':'economy','Features':'class inflight services'}
        dict63={'Category':'In Flight','Sub_Category':'Food','Features':'bar service'}
        dict64={'Category':'In Flight','Sub_Category':'Food','Features':'buttler service'}
        dict65={'Category':'At Airport','Sub_Category':'Services'}
        dict66={'Category':'In Flight','Sub_Category':'IN-FLIGHT SERVICES'}
        dict67={'Category':'In Flight','Sub_Category':'passenger satisfaction and service quality'}
        query_or5=[dict51, dict52, dict53, dict54, dict55, dict56, dict57, dict58, dict59, dict60, dict61, dict62, dict63, dict64, dict65, dict66, dict67]


        ratings={}
        weights={}
        get_ratings_and_weights(ratings, weights, query_or1, 'baggage')
        get_ratings_and_weights(ratings, weights, query_or2, 'corporate')
        get_ratings_and_weights_safety(ratings, weights, query_or3, 'safety')
        get_ratings_and_weights(ratings, weights, query_or4, 'ground')
        get_ratings_and_weights(ratings, weights, query_or5, 'service')
    ##    get_ratings_and_weights_image(ratings, weights, 'image')
        get_ratings_and_weights_skytrax_airline(ratings, weights)
    ##    dict_airlines=get_airlines(ratings)
        scores= get_scores(ratings, weights)
    ##    for component in scores:
    ##        print component
    ##        i=0
    ##        for airline in scores[component]:
    ##            i=i+1
    ##            if i > 3:
    ##                continue
    ##            print scores[component][airline]
        update_collection(scores)
    ##

    def get_ratings_tmp():
        ratings_tmp={
            "EK" : "5",
            "EY" : "5",
            "LH" : "10",
            "VS" : "5",
            "BA" : "5",
            "SQ" : "5",
            "AA" : "5",
            "QR" : "5",
            "FZ" : "5",
            "CX" : "5",
            "NH" : "5",
            "TK" : "5",
            "QF" : "5",
            "GA" : "5",
            "BR" : "5",
            "OZ" : "5",
            "LX" : "5",
            "AF" : "5",
            "OS" : "5",
            "TG" : "5",
            "JL" : "5",
            "NZ" : "5",
            "KA" : "5",
            "HU" : "5",
            "PG" : "0",
            "MH" : "5",
            "AK" : "5",
            "AC" : "0",
            "KL" : "5",
            "A3" : "5",
            "HX" : "5",
            "AY" : "5",
            "LA" : "5",
            "D8" : "5",
            "SA" : "5",
            "WY" : "5",
            "CZ" : "5",
            "KE" : "5",
            "U2" : "5",
            "KC" : "5",
            "JQ" : "5",
            "MI" : "5",
            "D7" : "5",
            "I9" : "5",
            "DL" : "5",
            "SU" : "5",
            "WS" : "5",
            "PD" : "5",
            "AV" : "5",
            "B6" : "5",
            "JJ" : "5",
            "CI" : "5",
            "SN" : "5",
            "VN" : "5",
            "IB" : "5",
            "SK" : "5",
            "TZ" : "5",
            "BY" : "5",
            "UA" : "5",
            "EI" : "5",
            "AD" : "5",
            "HM" : "5",
            "CM" : "5",
            "AS" : "5",
            "G9": "5",
            "KQ": "5",
            "CA": "5",
            "MS": "5",
            "SV": "5",
            "UL": "5",
            "RJ": "5",
            "9W": "5"
        }
        return ratings_tmp

    def get_ratings_and_weights(ratings, weights, query_or, component):
        print "called 1"
        query={}
        query['$or']=query_or
        cursor=db.JUP_DB_Data_Product.find(query)
        # print "cursor:", cursor
        print query
        ratings[component]=dict()
        print ratings
        weights[component]=dict()
        print weights
        j = 0
        ratings_tmp=get_ratings_tmp()
        print ratings_tmp
        i=0
        for c in cursor:
            i+=1
            category=c['Category']
            subcategory=c['Sub_Category']
            feature=c['Features']
            if subcategory==feature:
                continue
            j+=1
            for airline in ratings_tmp:
                try:
                    ratings_tmp[airline]=c[airline]
                except:
                    pass
            ratings[component][(category, subcategory, feature)]=dict()
            print ratings[component][(category, subcategory, feature)]
            ratings[component][(category, subcategory, feature)].update(ratings_tmp)
            weights[component][(category, subcategory, feature)]=1.0
            print weights[component][(category, subcategory, feature)]

    def get_ratings_and_weights_safety(ratings, weights, query_or, component):
        print "In 2"
        query={}
        query['$or']=query_or
        print query
        cursor=db.JUP_DB_Data_Product.find(query)
        print cursor
        ratings[component]=dict()
        print ratings
        weights[component]=dict()
        print weights
        ratings_tmp=get_ratings_tmp()
        print ratings_tmp
        j=0
        for c in cursor:
            category=c['Category']
            subcategory=c['Sub_Category']
            feature=c['Features']
            if subcategory==feature:
                continue
            j+=1
            for airline in ratings_tmp:
                ratings_tmp[airline]=c[airline]
            ratings[component][(category, subcategory, feature)]=dict()
            ratings[component][(category, subcategory, feature)].update(ratings_tmp)
            if feature=='safety equipment procedures':
                weights[component][(category, subcategory, feature)]=80.0
            else:
                weights[component][(category, subcategory, feature)]=20.0

    def get_ratings_and_weights_image(ratings, weights, component):
        query={}
        cursor=db.JUP_DB_Data_Product.find(query)
        ratings[component]=dict()
        weights[component]=dict()
        j=0
        for c in cursor:
            category=c['Category']
            subcategory=c['Sub_Category']
            feature=c['Feature']
            if subcategory==feature:
                continue
            try:
                core_elements=c['Core elements']
            except:
                pass
            if core_elements.find('PERCEPTION') >= 0:
                j+=1
                ratings[component][(category, subcategory, feature)]=dict()
                ratings[component][(category, subcategory, feature)].update(c['ratings'])
                weights[component][(category, subcategory, feature)]=1


    ##def get_ratings_and_weights(ratings, weights, query_or, component):
    ##    query={}
    ##    query['$or']=query_or
    ##    cursor=db.JUP_DB_Data_Product.find(query)
    ##    ratings[component]=dict()
    ##    weights[component]=dict()
    ##    j=0
    ##    for c in cursor:
    ##        category=c['Category']
    ##        subcategory=c['Sub_Category']
    ##        feature=c['Feature']
    ##        if subcategory==feature:
    ##            continue
    ##        j+=1
    ##        ratings[component][(category, subcategory, feature)]=dict()
    ##        ratings[component][(category, subcategory, feature)].update(c['ratings'])
    ##        weights[component][(category, subcategory, feature)]=1.0

    def get_ratings_and_weights_skytrax_airline(ratings, weights):
        airline_numbers=[str(i+1) for i in range(100)]
        cursor=db.JUP_DB_Data_Skytrax.find({'param':'airline_code'})
        airline_codes={}
        for c in cursor:
            for i in airline_numbers:
                airline_codes[i]=c[i]

        cursor=db.JUP_DB_Data_Skytrax.find({'param':'airline_rating'})
        airline_ratings={}
        for c in cursor:
            for i in airline_numbers:
                airline_ratings[i]=c[i]

        c1={}
        for i in airline_codes:
            c1[airline_codes[i]]=airline_ratings[i]

        component='skytrax airline'
        ratings[component]={}
        weights[component]={}
        category='skytrax airline'
        subcategory=category
        feature=category
        weights[component][(category, subcategory, feature)]=1.0
        ratings[component][(category, subcategory, feature)]=dict()
        ratings[component][(category, subcategory, feature)].update(c1)

        cursor=db.JUP_DB_Data_Skytrax.find({'param':'lounge_rating'})
        lounge_ratings={}
        for c in cursor:
            for i in airline_numbers:
                lounge_ratings[i]=c[i]

        c2={}
        for i in airline_codes:
            c2[airline_codes[i]]=lounge_ratings[i]
        component='skytrax airport'
        ratings[component]={}
        weights[component]={}
        category='skytrax airport'
        subcategory=category
        feature=category
        weights[component][(category, subcategory, feature)]=1.0
        ratings[component][(category, subcategory, feature)]=dict()
        ratings[component][(category, subcategory, feature)].update(c2)

    def get_airlines(ratings):
        dict_airlines={}
        for component in ratings:
            for index in ratings[component]:
                dict_airlines.update(ratings[component][index])
        return dict_airlines

    def get_scores(ratings, weights):
        for component in weights:
            total_weight=0.0
            for weight_index in weights[component]:
                total_weight += weights[component][weight_index]
            if abs(total_weight) < TOLERANCE:
                for weight_index in weights[component]:
                    weights[component][weight_index] =0.0
            else:
                for weight_index in weights[component]:
                    weights[component][weight_index] /= total_weight

        scores=dict()
        dict_airlines=get_ratings_tmp()
        for component in weights:
            scores[component]=dict()
            scores[component].update(dict_airlines)
            for airline in dict_airlines:
                scores[component][airline]=0.0

        for component in ratings:
            #csf is category/subcategory/feature
            for csf in ratings[component]:
                weight=weights[component][csf]
                for airline in dict_airlines:
                    try:
                        tmp_rating=float(ratings[component][csf][airline])
                    except:
                        tmp_rating=IMPUTED_RATING
                    scores[component][airline] += tmp_rating*weight
    ##    for component in scores:
    ##        for airline in scores[component]:
    ##            print component, airline, scores[component][airline]
    ##    print scores
        # update_collection(scores)
        return scores


    def update_collection(scores):
        for component in scores:
            query={'group':'Airline Rating', 'component': component}
            cursor=db.JUP_DB_Data_Competitor_Ratings.remove(query)

        dict_rec = {}
        for compartment in ['F', 'J', 'Y']:
            dict_rec['compartment']=compartment
            dict_rec['origin']=None
            dict_rec['destination']=None
            dict_rec['group']='Airline Rating'
            dict_rec['last_update_date_gmt'] = "{:%Y-%m-%d}".format(datetime.utcnow())
            for component in scores:
                dict_rec['component']=component
                dict_rec['ratings']=scores[component]
                dict_rec['_id']=ObjectId()
                print "dict_rec: ", dict_rec
                db.JUP_DB_Data_Competitor_Ratings.insert(dict_rec)

    start_time = time.time()
    print "start_time: ", start_time
    # MONGO_CLIENT_URL = '13.92.251.7:42525'
    # #net.MONGO_CLIENT_URL
    # ANALYTICS_MONGO_USERNAME = 'analytics'
    # ANALYTICS_MONGO_PASSWORD = 'KNjSZmiaNUGLmS0Bv2'
    # JUPITER_DB = 'fzDB_stg'
    # client = pymongo.MongoClient(MONGO_CLIENT_URL)
    # client.the_database.authenticate(ANALYTICS_MONGO_USERNAME, ANALYTICS_MONGO_PASSWORD, source='admin')
    # db = client[JUPITER_DB]
    from pymongo import MongoClient
    from jupiter_AI import JUPITER_DB, client
    db = client[JUPITER_DB]

    obj_err_main = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, "main", "")
    obj_err_main.append_to_error_list("main error level")
    DICT_ERROR_ODS={}
    IMPUTED_RATING=5
    MIN_RECORD_LENGTH=60
    TOLERANCE=0.001         ## error tolerance
    COLLECTION = 'JUP_DB_Capacity_1'

    main()

    ##print obj_err_main
    dict_errors={}
    dict_errors['program']='competitor ratings'
    dict_errors['date']="{:%Y-%m-%d}".format(datetime.now())
    dict_errors['time']=time.time()
    dict_errors['error description']=obj_err_main.as_string()
    db.JUP_DB_Errors_All.insert(dict_errors)
    print("--- %s seconds ---" % (time.time() - start_time))
    print "dict_errors: ", dict_errors


if __name__ == "__main__":
    run()