from jupiter_AI import JUPITER_LOGGER
from jupiter_AI.logutils import measure


@measure(JUPITER_LOGGER)
def run():
    """
     Author: Prem Narasimhan
     Completed date:
         Second version: May 17, 2017
     This program calculates ratings for the "Market Rating" group.

     Input collections:
         JUP_DB_Market_Share
     Output collection:
         JUP_DB_Data_Competitor_Ratings
     For testing:
         uncomment one layer at end of code and run the code
     For live:
        call main() with list of origin/destination/compartment/airline
     Overall logic:
         1. Market share records are read from "JUP_DB_Market_Share" (which is at od/compartment/airline/year/month level) into "dict_mkt_share_group_records"
         2. the data in "dict_mkt_share_group_records" is compiled into the nested dictionary "dict_mkt_share_recs"
         3. total pax is computed for each ailrine within od/compartment
         4. total pax is directly scaled from 0.0 to 10 - this is the final score
         5. records are updated into "JUP_DB_Competitor_Ratings" collection
     Error handling:
         1. If the error level=WARNING, errors are collected in obj_err_main (defined at global level) but processing continues
         2. At the end of the program, the errors in obj_err_main are flushed to JUP_DB_Errors_All
         Revision2:
         Introduced list of lists rather than dict of dicts
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
    import scipy.stats as ss


    @measure(JUPITER_LOGGER)
    def main():
        list_ods=get_list_of_ods()

        n_origins=1

        starting_number=0
        ending_number=n_origins
        stop=False
        l2=0
        pseudo_ods_map={}
        list_competitors=[]
        dict_compartments={}
        for i in range(10000):
            if i >24:
                break
            chosen_origins=[]
            if ending_number > len(list_ods):
                ending_number=len(list_ods)
                stop=True
            for j in range(starting_number, ending_number):
                chosen_origins.append(list_ods[j])
            print 'processing', starting_number, 'to', ending_number


            print 'chosen_origis', chosen_origins
            dict_mkt_share_recs = read_mkt_share_data(chosen_origins, pseudo_ods_map)
            for compartment in dict_mkt_share_recs:
                dict_compartments[compartment]=1
                for pseudo_od in dict_mkt_share_recs[compartment]:
                        for pos in dict_mkt_share_recs[compartment][pseudo_od]:
                            market_share_data=generate_market_share(compartment, pseudo_od, pos, dict_mkt_share_recs)
                            #print 'p6', dict_mkt_share_recs
                            competitors=number_of_competitors(market_share_data, 2017) #[rec[1], rec[2], rec[3], len(dict_airlines), dict_airlines, rec[6]], rec1=comp,rec2=pseudo_od,rec3=pos, rec[6]=pseudo_od2
                            if competitors is not None:
                                list_competitors.append(competitors)

            starting_number+=n_origins
            ending_number+=n_origins
            if stop:
                break

        print 'x18', dict_compartments
        #print 'p81', list_competitors
        list_ratings=rank_competitors_across_markets(list_competitors, dict_compartments)

                #print 'x10', list_competitors
    ##            for icnt in range(len(list_competitors)):
    ##                irec=list_competitors[icnt]
    ##                irec.append(list_ratings[icnt])
    ##                #print 'p75 ratings', irec
    ##                #print 'p75a', irec[5]
    ##                icnt+=1

        update_Data_Competitor_Ratings_collection(list_competitors, pseudo_ods_map)


    @measure(JUPITER_LOGGER)
    def get_list_of_ods():
        cursor = db.JUP_DB_Pos_OD_Compartment_new.distinct('pseudo_od')
        list_pseudo_od=[]
        for c in cursor:
            list_pseudo_od.append(c)
        return list_pseudo_od


    @measure(JUPITER_LOGGER)
    def get_compartments(dict_mkt_share_recs):
        dict_compartments={'Y':1, 'J':1}
        return dict_compartments


    # def read_existing_recs_in_comp_ratings():
    ##    cursor=db.JUP_DB_Data_Competitor_Ratings.find({'group':'Market Rating'})
    ##    dict_od_compartments_already_done={}
    ##    for c in cursor:
    ##        origin=c['origin']
    ##        destination=c['destination']
    ##        compartment=c['compartment']
    ##        try:
    ##            dict_od_compartments_already_done[origin]
    ##        except:
    ##            dict_od_compartments_already_done[origin] = dict()
    ##        try:
    ##            dict_od_compartments_already_done[origin][destination]
    ##        except:
    ##            dict_od_compartments_already_done[origin][destination] = dict()
    ##        try:
    ##            dict_od_compartments_already_done[origin][destination][compartment]
    ##        except:
    ##            dict_od_compartments_already_done[origin][destination][compartment] = dict()
    ##    count=0
    ##    for origin in  dict_od_compartments_already_done:
    ##        for destination in dict_od_compartments_already_done[origin]:
    ##            for compartment in dict_od_compartments_already_done[origin][destination]:
    ##                count += 1
    ##    print 'number of od already done', count
    ##    return dict_od_compartments_already_done


    @measure(JUPITER_LOGGER)
    def read_mkt_share_data(list_ods, pseudo_ods_map):
        """
        This method accumulates total pax for each airline at od_compartment level
        dict_mkt_share_recs is unique for each od/compartment/airline
        """
        odlist=pseudo_ods_map
        query={}
        lst=[]
        for i in list_ods:
            lst.append({'pseudo_od':i})
        query['$or']=lst
        print 'querry', query
        cursor = db.JUP_DB_Pos_OD_Compartment_new.find(query)
        #cursor = db.JUP_DB_Pos_OD_Compartment_new.find({'od':'SLLHKG'})
        print 'cursor_count', cursor.count()
        dict_mkt_share_recs=dict()
        k=0
        recs=[]
        for c in cursor:

            rec=0
            k+=1
            for i in c['top_5_comp_data']:
                airline         = i['airline']

                od              = c['od']
                pseudo_od       = c['pseudo_od']
                pseudo_od2      = pseudo_od
                try:
                    pod=float(pseudo_od)
                    if math.isnan(pseudo_od):
                        pseudo_od=od
                except:
                    pass

                try:
                    odlist[pseudo_od]
                except:
                    odlist[pseudo_od]={}
                try:
                    odlist[pseudo_od][od]
                except:
                    odlist[pseudo_od][od]={}

                origin          = od[:3]
                if origin is None:
                    continue
                destination     = od[3:]
                compartment     = c['compartment']
                if compartment=='Others':
                    continue
                year            = c['year']
                month           = c['month']
                pax             = i['pax']
                pos             = c['pos']
                #print 'x2', pos, airline
                try:
                    dict_mkt_share_recs[compartment]
                except:
                    dict_mkt_share_recs[compartment] = dict()
                try:
                    dict_mkt_share_recs[compartment][pseudo_od]
                except:
                    dict_mkt_share_recs[compartment][pseudo_od] = dict()
                try:
                    dict_mkt_share_recs[compartment][pseudo_od][pos]
                except:
                    dict_mkt_share_recs[compartment][pseudo_od][pos] = dict()
                #print 'x6', pos, airline
                #print 'x3',dict_mkt_share_recs
                try:
                    dict_mkt_share_recs[compartment][pseudo_od][pos][airline]
                except:
                    dict_mkt_share_recs[compartment][pseudo_od][pos][airline] = dict()
                #print 'x5',airline, dict_mkt_share_recs
                try:
                    dict_mkt_share_recs[compartment][pseudo_od][pos][airline][year]
                except:
                    dict_mkt_share_recs[compartment][pseudo_od][pos][airline][year] = dict()
                try:
                    dict_mkt_share_recs[compartment][pseudo_od][pos][airline][year][month]
                except:
                    dict_mkt_share_recs[compartment][pseudo_od][pos][airline][year][month] = {}
                #print 'x4', dict_mkt_share_recs
                dict_mkt_share_recs[compartment][pseudo_od][pos][airline][year][month]['pseudo_od2'] = pseudo_od2
        #print 'x1', dict_mkt_share_recs
        return dict_mkt_share_recs


    @measure(JUPITER_LOGGER)
    def generate_market_share(compartment, pseudo_od,pos, dict_mkt_share_recs):
        recs=[]
        for airline in dict_mkt_share_recs[compartment][pseudo_od][pos]:
            for year in dict_mkt_share_recs[compartment][pseudo_od][pos][airline]:
                for month in dict_mkt_share_recs[compartment][pseudo_od][pos][airline][year]:
                    rec=[]
                    rec.append(airline)
                    rec.append(compartment)
                    rec.append(pseudo_od)
                    rec.append(pos)
                    rec.append(year)
                    rec.append(month)
                    rec.append(dict_mkt_share_recs[compartment][pseudo_od][pos][airline][year][month]['pseudo_od2'])
                    recs.append(rec)
                    #print 'p7', rec
        #print 'x7', recs
        return recs


    @measure(JUPITER_LOGGER)
    def number_of_competitors(mkt_recs, ayear):
        dict_airlines={}
        for rec in mkt_recs:
            #print 'x8', rec[4]
            if rec[4] != ayear: #rec[4] is year
                continue
            dict_airlines[rec[0]]=1 #rec[0] is airline
            #print 'p79', rec[0]
            #print 'p82', rec
            #print 'x9', len(dict_airlines)
        if len(dict_airlines)==0:
            return None
        competitors=[rec[1], rec[2], rec[3], len(dict_airlines), dict_airlines, rec[6]]
        #print 'p78', competitors
        return competitors


    @measure(JUPITER_LOGGER)
    def rank_competitors_across_markets(list_recs, dict_compartments):
        results=[]
        for compartment in dict_compartments:
            compart_recs=[]
            for rec in list_recs:
                if rec[0]==compartment:
                    compart_recs.append(rec)
            list_ratings=[]
            print 'x13', len(compart_recs)
            maxi=0
            for rec in compart_recs:
                #print 'p80', rec
                list_ratings.append(rec[3]) #ncompetitors
                maxi=max(maxi, rec[3])
            print 'x19', maxi
            #print 'p76', compart_recs
            if len(list_ratings) ==0:
                obj_err = error_class.ErrorObject(error_class.ErrorObject.WARNING, get_module_name(), get_arg_lists(inspect.currentframe()))
                obj_err.append_to_error_list("something wrong")
                obj_err_main.append_to_error_object_list(obj_err)
                return list_ratings
            list_ranks=ss.rankdata(list_ratings)
    ##        tmp1=[i for i in range(13)]
    ##        tmp2=ss.rankdata(tmp1)
    ##        tmp3, _, _ = rescale_values(tmp2, 0.0, 10.0)
    ##        print 'x15', tmp1
    ##        print 'x16', tmp2
    ##        print 'x17', tmp3

            outs=[]
            out=[]
            for k in range(len(list_ranks)):
                out=[]
                out.append(list_ratings[k])
                out.append(list_ranks[k])
                outs.append(out)
        ##    print 'x12', outs
            min_rank=min(list_ranks)
            max_rank=max(list_ranks)
            rank_range=max_rank-min_rank
            if len(list_ratings)==1:
                list_ratings[0]=IMPUTED_RATING
                return list_ratings
            max_score=10.0
            min_score=0.0

            max_score=10.0
            min_score=0.0
            if len(list_ranks) < 4:
                max_score=7.5
                min_score=2.5
            list_ratings, _, _ = rescale_values(list_ranks, min_score, max_score)
            #print 'x12', list_ratings
            if list_ratings==None:
                list_ratings=[IMPUTED_RATING for i in list_ranks]
            for i in range(len(list_ratings)):
                list_ratings[i]=10.0-list_ratings[i]
            #print 'x14', list_ratings
            outs2=[]
            out=[]
            if list_ratings is not None:
                for k in range(len(list_ratings)):
                    out=[]
                    out.append(outs[k][0])
                    out.append(outs[k][1])
                    out.append(list_ratings[k])
                    outs2.append(out)
                    compart_recs[k].append(list_ratings[k])
                print 'x11', outs2
            for rec in compart_recs:
                results.append(rec)
        return list_ratings



    @measure(JUPITER_LOGGER)
    def rescale_values(list_series, min_score, max_score):
        """
        given a list of input numbers, and minimum and maximum scores, rescales the list to a list of ratings so that lowest rating=min_score and highest rating=max_score
        in other words, converts the list_series linearly to lie between min_score and max_score
        """
        series_min=min(list_series)
        series_max=max(list_series)
        series_range=series_max-series_min
        score_range=max_score-min_score
        if abs(series_range) < TOLERANCE:
            return [None, None, None]
        slope=score_range/series_range
        intercept=min_score - slope*series_min
        out=[0 for i in list_series]
        for i in range(len(out)):
            out[i]=slope*list_series[i]+intercept
        return [out, slope, intercept]


    @measure(JUPITER_LOGGER)
    def update_Data_Competitor_Ratings_collection(list_competitors, pseudo_ods_map):
        for icnt in range(len(list_competitors)):
            rec=list_competitors[icnt]
            pseudo_od = rec[1]
            for od in pseudo_ods_map[pseudo_od]:
                dict_mkt_share_rating_rec=dict()
                #rec0=comp,rec1=origin,rec2=destination, rec3=pos, rec4=growth, rec5=dict_airlines, rec6=float_ratings
                ## common data
                dict_mkt_share_rating_rec['compartment'] = rec[0]
                dict_mkt_share_rating_rec['pseudo_od'] = rec[5]
                dict_mkt_share_rating_rec['od'] = od
                dict_mkt_share_rating_rec['origin'] = od[:3]
                dict_mkt_share_rating_rec['destination'] = od[3:]
                dict_mkt_share_rating_rec['pos'] = rec[2]
                dict_mkt_share_rating_rec['group'] = 'Market Rating'
                dict_mkt_share_rating_rec['last_update_date_gmt']=strftime("%Y-%m-%d %H:%M:%S", gmtime())

                dict_mkt_share_rating_rec['component'] = 'No: of Competitors'
                dict_mkt_share_rating_rec['ncompetitors']=rec[3]
                dict_airlines=rec[4]
                float_ratings=rec[6]
                for airline in dict_airlines:
                    dict_airlines[airline]=float_ratings  #for every airline in market, size_of_market rating is same for eacch line
                dict_mkt_share_rating_rec['ratings']=dict_airlines
                dict_mkt_share_rating_rec['_id']=ObjectId()
                #print 'p77', dict_mkt_share_rating_rec
                db.JUP_DB_Data_Competitor_Ratings.insert(dict_mkt_share_rating_rec)


    @measure(JUPITER_LOGGER)
    def update_Data_Competitor_Ratings_collection2(dict_mkt_share_recs):
        for origin in dict_mkt_share_recs:
            for destination in dict_mkt_share_recs[origin]:
                for compartment in dict_mkt_share_recs[origin][destination]:
                    list_airlines = dict_mkt_share_recs[origin][destination][compartment]['list_airlines1']
                    list_ratings = dict_mkt_share_recs[origin][destination][compartment]['list_ratings1']
                    dict_mkt_share_rating_rec=dict()

                    ## common data
                    dict_mkt_share_rating_rec['origin'] = origin
                    dict_mkt_share_rating_rec['destination'] = destination
                    dict_mkt_share_rating_rec['compartment'] = compartment
                    dict_mkt_share_rating_rec['group'] = 'Market Rating'
                    dict_mkt_share_rating_rec['last_update_date_gmt']=strftime("%Y-%m-%d %H:%M:%S", gmtime())

                    dict_mkt_share_rating_rec['component'] = 'Market Share'
                    # if len(list_airlines) < 3:
                    #     continue
                    dict_mkt_share_rating_rec['ratings']=dict(zip(list_airlines, list_ratings))
                    dict_mkt_share_rating_rec['_id']=ObjectId()
                    db.JUP_DB_Data_Competitor_Ratings.insert(dict_mkt_share_rating_rec)


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
    from jupiter_AI import JUPITER_DB, client
    db = client[JUPITER_DB]
    obj_err_main = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, "main", "")
    obj_err_main.append_to_error_list("main error level")
    DICT_ERROR_ODS={}
    IMPUTED_RATING=5
    MIN_RECORD_LENGTH=60
    TOLERANCE=0.001         ## error tolerance
    COLLECTION = 'JUP_DB_Capacity_1'


    @measure(JUPITER_LOGGER)
    def tmp_get_list_for_mkt_share_query():
        """
        temporary function to supply arguments to main
        """

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