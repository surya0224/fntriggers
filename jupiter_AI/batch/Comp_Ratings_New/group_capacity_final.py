from jupiter_AI import JUPITER_LOGGER
from jupiter_AI.logutils import measure


@measure(JUPITER_LOGGER)
def run():

    """
     Author: Prem Narasimhan
     Completed date:
         first version: Mar 5, 2017
     This program calculates ratings for the "Capapcity/Schedule" group.

     Input collections:
         JUP_DB_Capacity
         JUP_DB_Data_OAG_Airline_OTP
     Output collection:
         JUP_DB_Data_Competitor_Ratings
     For testing:
         uncomment one layer from code at end of prgram
     For live:
         call main with list of o/d/compartment/airline combinations
     Overall logic:
         1. capacity records are read from "JUP_DB_Capacity" (which is at flight/airline/od/compartment level) into "dict_capacity_group_records"
         2. the data in "dict_capacity_group_records" is compiled into the dictionary "dict_freq_results"
         3. the individual freq's are compiled into a list for each od/compartment/airline (note that pos is missing in this hierarchy)
         4. average freq is computed for each ailrine/od/compartment
         5. freq is ranked, then scaled from 0.0 to 10 - this is the final score
         6. records are updated into "JUP_DB_Competitor_Ratings" collection

         7. the data in "dict_capacity_group_records" is compiled into the dictionary "dict_capacity_results"
         8. the individual capacity's are compiled into a list for each od/compartment/airline
         9. total capacity is computed for each ailrine with od/compartment
         10. capacity is scaled (note: capacity is not ranked, unlike frequency) from 0.0 to 10 - this is the final score
         11. records are updated into "JUP_DB_Competitor_Ratings" collection

         12. data is read from JUP_DB_Data_OAG_Airline_OTP (which is at airline/year/month/ level) into dict_OTP_records
         13. the individual OTP's are compiled into a list for each airline
         14. average OTP is computed for each ailrine
         15. average_OTP is ranked (assumed that low OTP is worse), then scaled from 0.0 to 10 - this is the final score
         16. records are updated into "JUP_DB_Competitor_Ratings" collection

     Error handling:
         1. If the error level=WARNING, errors are collected in obj_err_main (defined at global level) but processing continues
         2. At the end of the program, the errors in obj_err_main are flushed to JUP_DB_Errors_All
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
    import math
    import copy

    import scipy.stats as ss

    @measure(JUPITER_LOGGER)
    def main():
        """
        list_nonunique_dict_airline_o_d_compartment is a list of dicts:
            dict_out['airline']=c['airline']
            dict_out['origin']=c['origin']
            dict_out['destination']=c['destination']
            dict_out['compartment']=c['compartment']
        """
        cd = list(db.JUP_DB_OD_Capacity.aggregate([{'$group': {'_id': {'origin':"$pseudo_origin" , 'destination': "$pseudo_destination", 'compartment': "$compartment"}}}]))
        # c = list(cd) aggregate([{$group: {_id: { 'origin':"$origin" , 'destination': "$destination", compartment: "$compartment"}}} ])
        list_ods=[]
        for i in range(len(cd)):
            list_ods.append(cd[i]['_id'])


        list_origins=get_list_of_origins()

        n_origins=1

        i=0
        starting_number=0
        ending_number=n_origins
        stop=False
        l2=0
        for i in range(10000):
            if i >25:
                break
            chosen_origins=[]
            if ending_number > len(list_origins):
                ending_number=len(list_origins)
                stop=True
            for j in range(starting_number, ending_number):
                chosen_origins.append(list_origins[j])
            # print 'processing', starting_number, 'to', ending_number, 'starting origin', chosen_origins[0], 'ending origin', chosen_origins[len(chosen_origins)-1]

            print "chosen_origins: ", chosen_origins
            if len(chosen_origins) != 0:
                dict_capacity_group_records = read_in_capacity_group_data(chosen_origins)
                dict_compartments = get_compartments(dict_capacity_group_records)

    ##        dict_freq_results = {}
    ##        update_freq(dict_freq_results, dict_capacity_group_records)
    ##        compute_average_freq(dict_freq_results)
    ##        compute_freq_scores(dict_freq_results)
    ##
            dict_capacity_results = {}
            update_capacity(dict_capacity_results, dict_capacity_group_records, 2017)
            compute_total_capacity(dict_capacity_results)
            compute_capacity_scores(dict_capacity_results)

    ##        dict_blocktime_results  = {}
    ##        update_blocktime(dict_blocktime_results, dict_capacity_group_records)
    ##        compute_average_blocktime(dict_blocktime_results)
    ##        compute_blocktime_scores(dict_blocktime_results)

    ##        update_Data_Competitor_Ratings_collection(dict_freq_results, dict_capacity_results, dict_blocktime_results, dict_compartments)
            update_Data_Competitor_Ratings_collection2(dict_capacity_results, dict_compartments)

            starting_number+=n_origins
            ending_number+=n_origins
            if stop:
                break



    @measure(JUPITER_LOGGER)
    def get_list_of_origins():
        od_s1 = list(db.JUP_DB_OD_Master.distinct('OD'))
        print od_s1
        print len(od_s1)
        ods = []
        city_airport = list(db.JUP_DB_City_Airport_Mapping.aggregate([{'$project': {'_id': 0, 'City_Code': 1, 'Airport_Code': 1}}]))
        for oridest in od_s1:
            origin = oridest[:3]
            destination = oridest[3:]
            # print origin,destination
            for l in city_airport:
                if origin == l['Airport_Code']:
                    origin = l['City_Code']
                else:
                    pass
                    # city_code_ori_list.append(l['City_Code'])
                if destination == l['Airport_Code']:
                    destination = l['City_Code']
                else:
                    pass
            ods.append(origin + destination)
        print len(ods)
        print ods
        # cursor = db.JUP_DB_OD_Capacity.find({'pseudo_od':{'$in':ods}})
        # list_origin=[]
        # for c in cursor:
        #     list_origin.append(c)
        list_origin = ods
            # ['DXBCMB', 'CMBDXB', 'MLEAMM', 'AMMDXB']
        return list_origin



    @measure(JUPITER_LOGGER)
    def get_compartments(dict_capacity_group_records):
        dict_compartments={'Y':1, 'J':1}
        return dict_compartments


    @measure(JUPITER_LOGGER)
    def read_in_capacity_group_data(list_ods):
        """
        This method reads in data from JUP_DB_Capacity
        """
        query={}
        orq = []
        for i in list_ods:
            orqi={'pseudo_od':i}
            #print 'orqi', orqi
            orq.append(orqi),
        #print 'orq', orq
        query['$or']=orq
        cursor = db.JUP_DB_OD_Capacity.find(query)
        dict_capacity_records={}
        counter_cursor=0
        print 'cursor_count', cursor.count()
        for c in cursor:
            dict_capacity_records[counter_cursor]=c
            counter_cursor+=1
            # print counter_cursor
        return dict_capacity_records


    @measure(JUPITER_LOGGER)
    def update_capacity(dict_capacity_results, dict_capacity_records, ayear):
        """
        This method is used to set up hierarchy of dictionaries - origin/destination/compartment/airline
        """
        for cap_rec_index in dict_capacity_records:
            pseudo_od=dict_capacity_records[cap_rec_index]['pseudo_od']
            airline         = dict_capacity_records[cap_rec_index]['carrier']
            origin          = dict_capacity_records[cap_rec_index]['pseudo_origin']
            destination     = dict_capacity_records[cap_rec_index]['pseudo_destination']
            true_origin     = dict_capacity_records[cap_rec_index]['origin']
            true_destination     = dict_capacity_records[cap_rec_index]['destination']
            compartment     = dict_capacity_records[cap_rec_index]['compartment']
            month_year      = int(dict_capacity_records[cap_rec_index]['month_year'])
            if compartment=='N/A' or compartment=='n/a' or compartment=='NA' or compartment=='na':
                continue
            capacity        = dict_capacity_records[cap_rec_index]['od_capacity']
            try:
                dict_capacity_results[origin]
            except:
                dict_capacity_results[origin] = dict()
            try:
                dict_capacity_results[origin][destination]
            except:
                dict_capacity_results[origin][destination] = dict()
            try:
                dict_capacity_results[origin][destination][compartment]
            except:
                dict_capacity_results[origin][destination][compartment] = dict()
            try:
                dict_capacity_results[origin][destination][compartment][airline]
            except:
                dict_capacity_results[origin][destination][compartment][airline] = dict()
            year=int(month_year/100)
            if year != ayear:
                continue
            try:
                dict_capacity_results[origin][destination][compartment][airline][true_origin]
            except:
                dict_capacity_results[origin][destination][compartment][airline][true_origin] = dict()
            try:
                dict_capacity_results[origin][destination][compartment][airline][true_origin][true_destination]
            except:
                dict_capacity_results[origin][destination][compartment][airline][true_origin][true_destination] = dict()
            try:
                dict_capacity_results[origin][destination][compartment][airline][true_origin][true_destination]['list_capacities']
            except:
                dict_capacity_results[origin][destination][compartment][airline][true_origin][true_destination]['list_capacities']=[]
            try:
                capacity
                if math.isnan(capacity):
                    capacity2=0.0
                else:
                    capacity2= float(capacity)
            except:
                capacity2=0.0
            dict_capacity_results[origin][destination][compartment][airline][true_origin][true_destination]['list_capacities'].append(capacity2)
            print 'or1', origin, destination, compartment, airline, true_origin, true_destination,dict_capacity_results[origin][destination][compartment][airline][true_origin][true_destination]['list_capacities']
    ##        except:
    ##            dict_capacity_results[origin][destination][compartment][airline]['list_capacities']=[]
    ##        try:
    ##            dict_capacity_results[origin][destination][compartment][airline]['list_capacities']
    ##        except:
    ##            dict_capacity_results[origin][destination][compartment][airline]['list_capacities']=[]


    @measure(JUPITER_LOGGER)
    def compute_total_capacity(dict_capacity_results2):
        """
        This function computes total capacity for o/d/compartment/airline from flight/airline/o/d/compartment level data
        """
        dict_capacity_results=copy.deepcopy(dict_capacity_results2)
        for origin in dict_capacity_results:
            for destination in dict_capacity_results[origin]:
                for compartment in dict_capacity_results[origin][destination]:
                    for airline in dict_capacity_results[origin][destination][compartment]:
                        for true_origin in dict_capacity_results[origin][destination][compartment][airline]:
                            for true_destination in dict_capacity_results[origin][destination][compartment][airline][true_origin]:
                                try:
                                    list_capacities = dict_capacity_results[origin][destination][compartment][airline][true_origin][true_destination]['list_capacities']
                                    print 'list_caps', origin, destination, compartment, airline, list_capacities
                                except:
                                    print 'erer'
                                    continue
                                #print 'list_cap', list_capacities
                                if len(list_capacities) == 0:         ## no freq record found for this od_compartment_airline
                                    total_capacity = None
                                else:
                                    total_capacity = float(sum(list_capacities))
                                dict_capacity_results2[origin][destination][compartment][airline][true_origin][true_destination]['total_capacity']=total_capacity
                                print 'total_caps', origin, destination, compartment, airline, dict_capacity_results2[origin][destination][compartment][airline][true_origin][true_destination]['total_capacity']


    @measure(JUPITER_LOGGER)
    def compute_capacity_scores(dict_capacity_results2):
        """
        rescales the capacities for different airlines so that they range from 0 to 10
        """
        dict_capacity_results=copy.deepcopy(dict_capacity_results2)
        for origin in dict_capacity_results:
            for destination in dict_capacity_results[origin]:
                for compartment in dict_capacity_results[origin][destination]:
                    list_ratings=[]
                    list_airlines=[]
                    for airline in dict_capacity_results[origin][destination][compartment]:
                        print 'dicct', origin, destination,compartment, airline, dict_capacity_results[origin][destination][compartment][airline]
                        for true_origin in dict_capacity_results[origin][destination][compartment][airline]:
                            for true_destination in dict_capacity_results[origin][destination][compartment][airline][true_origin]:
                                print 'dicct2', 'origin', origin, destination, compartment, airline, true_origin, true_destination
                                print 'dicct3', dict_capacity_results[origin][destination][compartment][airline][true_origin][true_destination]
                                list_capacities=[]
                                #for airline in dict_capacity_results[origin][destination][compartment]:
                                try:
                                    total_capacity= dict_capacity_results[origin][destination][compartment][airline][true_origin][true_destination]['total_capacity']
                                except:
                                    print 'errorerror'
                                    continue
                                print 'camehere9'
                                if total_capacity != None:
                                    list_airlines.append(airline)
                                    list_ratings.append(total_capacity)
                                    print 'airo', list_airlines, list_ratings
                                if len(list_ratings) ==0:
                                    obj_err = error_class.ErrorObject(error_class.ErrorObject.WARNING, get_module_name(), get_arg_lists(inspect.currentframe()))
                                    obj_err.append_to_error_list("no airlines capacities were found for origin=" + origin + ", destination=" + destination + ", compartment=" + compartment)
                                    obj_err_main.append_to_error_object_list(obj_err)
                                    print 'errerr'
                                    continue
                                if len(list_ratings)==1:
                                    print 'camehere6'
                                    list_ratings[0]=IMPUTED_RATING
                                    dict_capacity_results2[origin][destination][compartment][airline][true_origin][true_destination]['list_airlines']=list_airlines
                                    dict_capacity_results2[origin][destination][compartment][airline][true_origin][true_destination]['list_ratings']=list_ratings
                                    dict_capacity_results[origin][destination][compartment][airline][true_origin][true_destination]['ratings']=dict(zip(list_airlines, list_ratings))
                                    continue
                                max_score=10.0
                                min_score=0.0
                                print 'camehere8'
                                if len(list_ratings) < 4:
                                    max_score=7.5
                                    min_score=2.5
                                list_ratings, _, _ = rescale_values(list_ratings, min_score, max_score)
                                if list_ratings==None:
                                    list_ratings=[IMPUTED_RATING for i in list_capacities]
                                for i in range(len(list_ratings)):
                                    list_ratings[i]=10.0-list_ratings[i]
                                dict_capacity_results2[origin][destination][compartment][airline][true_origin][true_destination]['list_airlines']=list_airlines
                                dict_capacity_results2[origin][destination][compartment][airline][true_origin][true_destination]['list_ratings']=list_ratings
                                dict_capacity_results2[origin][destination][compartment][airline][true_origin][true_destination]['ratings']=dict(zip(list_airlines, list_ratings))
                                print 'camehere7'
                                print 'zip', dict_capacity_results2[origin][destination][compartment][airline][true_origin][true_destination]['ratings']




    @measure(JUPITER_LOGGER)
    def rescale_values(list_series, min_score, max_score):
        """
        given a list of input ratings, and minimum and maximum scores, rescales the list to a list of scores so that lowest score=min_score and highest score=max_score
        """
        series_min=min(list_series)
        series_max=max(list_series)
        series_range=series_max-series_min
        score_range=max_score-min_score
        if abs(series_range) < TOLERANCE:   ## all values in list_series are the same
            return None, None, None
        slope=score_range/series_range
        intercept=min_score - slope*series_min
        out=[0 for i in list_series]
        for i in range(len(out)):
            out[i]=slope*list_series[i]+intercept
        return out, slope, intercept


    @measure(JUPITER_LOGGER)
    def update_Data_Competitor_Ratings_collection2(dict_capacity_results, dict_compartments):
        """
        delete existing records and then insert the new records into JUP_DB_Data_Competitor_Ratings
        """
        dict_capacity_rating_rec = {}
        for origin in dict_capacity_results:
            for destination in dict_capacity_results[origin]:
                for compartment in dict_capacity_results[origin][destination]:
                    kk=0
                    for airline in dict_capacity_results[origin][destination][compartment]:
                        kk+=1
                        if kk >1:
                            break;

                    for true_origin in dict_capacity_results[origin][destination][compartment][airline]:
                        for true_destination in dict_capacity_results[origin][destination][compartment][airline][true_origin]:
                            print 'camehere10'
                            print 'origin', origin, 'dest', destination, 'compart', compartment, 'true_origin', true_origin, 'tru_dest', true_destination
                            dict_capacity_rating_rec['peudo_origin'] = origin
                            dict_capacity_rating_rec['pseudo_destination'] = destination
                            dict_capacity_rating_rec['origin'] = true_origin
                            dict_capacity_rating_rec['destination'] = true_destination
                            dict_capacity_rating_rec['compartment'] = compartment
                            dict_capacity_rating_rec['group'] = 'Capacity/Schedule'
                            dict_capacity_rating_rec['component'] = 'Capacity'
                            dict_capacity_rating_rec['last_update_date_gmt']=strftime("%Y-%m-%d %H:%M:%S", gmtime())
                            try:
                                dict_capacity_rating_rec['ratings'] = dict_capacity_results[origin][destination][compartment][airline][true_origin][true_destination]['ratings']
                            except:
                                print origin, destination, compartment
                                continue
                            dict_capacity_rating_rec['_id']=ObjectId()
                            db.JUP_DB_Data_Competitor_Ratings.insert(dict_capacity_rating_rec)





    @measure(JUPITER_LOGGER)
    def get_module_name():
        return inspect.stack()[1][3]


    @measure(JUPITER_LOGGER)
    def get_arg_lists(frame):
        """
        function used to get the list of arguments of the function
        where it is called
        """
        args, _, _, values = inspect.getargvalues(frame)
        argument_name_list=[]
        argument_value_list=[]
        for k in args:
            argument_name_list.append(k)
            argument_value_list.append(values[k])
        return argument_name_list, argument_value_list


    start_time = time.time()
    print start_time
    # MONGO_CLIENT_URL = '13.92.251.7:42525'
    # #net.MONGO_CLIENT_URL
    # ANALYTICS_MONGO_USERNAME = 'analytics'
    # ANALYTICS_MONGO_PASSWORD = 'KNjSZmiaNUGLmS0Bv2'
    # JUPITER_DB = 'fzDB_stg'
    # client = pymongo.MongoClient(MONGO_CLIENT_URL)
    # client.the_database.authenticate(ANALYTICS_MONGO_USERNAME, ANALYTICS_MONGO_PASSWORD, source='admin')
    # db = client[JUPITER_DB]
    from pymongo import MongoClient
    client = MongoClient("mongodb://analytics:KNjSZmiaNUGLmS0Bv2@13.92.251.7:42525/")
    db = client.fzDB_stg
    obj_err_main = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, "main", "")
    obj_err_main.append_to_error_list("main error level")
    DICT_ERROR_ODS={}
    IMPUTED_RATING=5
    MIN_RECORD_LENGTH=60
    TOLERANCE=0.001         ## error tolerance
    COLLECTION = 'JUP_DB_Capacity_1'



    #list_dict_airline_o_d_compartment=tmp_get_list_for_capacity_query()
    main()

    ##print obj_err_main
    dict_errors={}
    dict_errors['program']='competitor ratings'
    dict_errors['date']="{:%Y-%m-%d}".format(datetime.now())
    dict_errors['time']=time.time()
    ##dict_errors['error description']=obj_err_main.as_string()
    db.JUP_DB_Errors_All.insert(dict_errors)
    print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == "__main__":
    run()