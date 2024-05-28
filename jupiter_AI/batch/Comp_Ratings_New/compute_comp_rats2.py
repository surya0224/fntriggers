"""
 Author: Prem Narasimhan
 Completed date:
     first version: Apri 16, 2018
 This program calculates competitor ratings.

 Input collections:
     JUP_DB_Data_Competitor_Weights
     JUP_DB_Data_Competitor_Ratings
 Output collection:
     JUP_DB_Competitor_Ratings
 Overall logic:
     1. Get all weights records from JUP_DB_Competitor_Weights records (at compartment/region/country/pos/od level). region=None means wildcarding or record exists at more graular level (like pos).
 Change from previous version (compute_competitor_rating_new(13a):
    1. Problem with Distributor weights in
 Change from previous version (compute_competitor_rating_new(12a):
    1. introduced group ratings
    2. introduced logic to normalize component level weights to 1.0. Earlier logic had a hole: if a component was missing so that component level weights did not add to 1, it was not getting renormalized to 1.
    3. Removed lots of comments
    4. Earlier code was repeating calculate_ratings functions many times, causing mutliple records in JUP_DB_Competitor_Ratings
 Hardcodes/critical notes:
    1. Correct_component_codes() - make component names in rating and weights collections are same (makes weights name same as ratings names)
    2. currently only network level weights are supported - this restrictio is in seed_template()
"""
from jupiter_AI import Host_Airline_Code, Host_Airline_Hub, JUPITER_LOGGER
from jupiter_AI.logutils import measure


@measure(JUPITER_LOGGER)
def run():
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
    import scipy.stats as ss
    import numpy as np
    import pandas as pd
    from operator import itemgetter
    import math

    pd.options.display.max_rows = 1000


    @measure(JUPITER_LOGGER)
    def main(dict_mkts_to_be_printed):
        start_time=time.time()

        df_grp_wts, df_comp_wts=prepare_global_wts()
        print 'L1 #global group weights overall, across all compartments=', df_grp_wts.compartment.count()
        print 'L2 #global component weights overall, across all compartments=', df_comp_wts.compartment.count()

        #get ratings where origin, destination and pos are none.
        #Currently these ratings belong to the 'Product Rating' and 'Airline Rating' groups
        #these ratings will be added to template for every market
        df_nonod_ratings_global=get_ratings_from_collection(None)

        list_ods=get_list_of_ods()
        print 'L3 #ods=', len(list_ods)

        nods_in_batch=100
        print 'L4 batch size(#ods)=', nods_in_batch
        print 'L4 #od batches=', len(list_ods) / nods_in_batch
        print ' '

        starting_number=0
        ending_number=nods_in_batch
        stop=False

        tot_mkts=0
        for i in range(10000): #10000 is number of iterations, so max number of ods computed = 10000*nods_in_batch
            #if i == 1:
            #    break
            chosen_ods=[]
            if ending_number > len(list_ods):   #handle gracefully if final batch has less than 1000(nods_in_batches) ods
                ending_number=len(list_ods)
                stop=True
            for j in range(starting_number, ending_number):
                chosen_ods.append(list_ods[j])

            print 'L5a processing batch # ', i, 'starting from', starting_number, 'to', ending_number, 'starting od', chosen_ods[0], 'ending origin', chosen_ods[len(chosen_ods)-1]
            #main loop that calculates the rating for each market within a given batch of od's
            tot_mkts = batch_loop(chosen_ods, df_nonod_ratings_global, df_grp_wts, df_comp_wts, tot_mkts, dict_mkts_to_be_printed)
            elapsed_time = time.time() - start_time

            print 'L5b processed batch # ', i, 'elapsed_time=', elapsed_time/60, '(minutes)'
            print ' '

            starting_number+=nods_in_batch
            ending_number+=nods_in_batch
            if stop:
                break


    @measure(JUPITER_LOGGER)
    def prepare_global_wts():
        '''
        get all weights docs from JUP_DB_Data_Competitor_Weights
        break them into group weights df and component weights df
        '''

        cursor = db.JUP_DB_Data_Competitor_Weights_prod.find()
        print 'L6 number of docs in JUP_DB_Data_Competitor_Weights', cursor.count()
        if cursor.count() == 0:
            obj_err = error_class.ErrorObject(error_class.ErrorObject.WARNING, get_module_name(), get_arg_lists(inspect.currentframe()))
            error_msg="no records found in JUP_DB_Data_Competitor_Weights collection"
            obj_err.append_to_error_list(error_msg)
            obj_err_main.append_to_error_object_list(obj_err)
            raise error_msg
        df =  pd.DataFrame(list(cursor))
        del df['_id']

        #correct the names of some components taken from JUP_DB_Data_Competitor_Weights
        #these components have different names in the JUP_DB_Data_Competitor_Weights and JUP_DB_Data_Competitor_Ratings1 collections
        #make these names the same by changing the names from JUP_DB_Data_Competitor_Weights collection
        # it is better to align in data, but doing it thru program for quickness, will change later
        df=correct_component_names(df)

        df['ix_compartment']=df['compartment']
        df['ix_region']=df['region']
        df['ix_country']=df['country']
        df['ix_od']=df['od']
        df['ix_group']=df['group']
        df['ix_component']=df['component']

        df_grp_wts=df[df['group']==df['component']]
        df_grp_wts=normalize_grp_wts(df_grp_wts) #currently check for zero group weight is not doneshould be added
        df_grp_wts.set_index(['ix_compartment', 'ix_region', 'ix_country', 'ix_od', 'ix_group'], inplace=True)
        #print 'M9a count df_grp_wts', df_grp_wts.compartment.count()

        df_comp_wts=df[df['group']!=df['component']]
        df_comp_wts=normalize_comp_wts(df_comp_wts)
        df_comp_wts.set_index(['ix_compartment', 'ix_region', 'ix_country', 'ix_od', 'ix_group', 'ix_component'], inplace=True)
        #print 'M10a count df_comp_wts', df_comp_wts.compartment.count()

        return df_grp_wts, df_comp_wts


    @measure(JUPITER_LOGGER)
    def correct_component_names(df):
        '''
        #correct the names of some components taken from JUP_DB_Data_Competitor_Weights
        #these components have different names in the JUP_DB_Data_Competitor_Weights and JUP_DB_Data_Competitor_Ratings1 collections
        #make these names the same by changing the names from JUP_DB_Data_Competitor_Weights collection
        # it is better to align in data, but doing it thru program for quickness, will change later
        '''
        df=df[df['component']!='Brand Image']
        df=df[df['component']!='Type of Aircrafts']
        df=df[df['component']!='On Time Performance']
        df=df[df['component']!='Currency Export Restrictions']
        df=df[df['component']!='Countertrade']
        for ix in df.index:
            if df.loc[ix, 'group']=='Airline Rating' and df.loc[ix, 'component']=='Corporate':
                df.loc[ix, 'component']='corporate'
            if df.loc[ix, 'group']=='Airline Rating' and df.loc[ix, 'component']=='Ground handling':
                df.loc[ix, 'component']='ground'
            if df.loc[ix, 'group']=='Airline Rating' and df.loc[ix, 'component']=='Reliability/ baggage, on-time':
                df.loc[ix, 'component']='baggage'
            if df.loc[ix, 'group']=='Airline Rating' and df.loc[ix, 'component']=='Safety':
                df.loc[ix, 'component']='safety'
            if df.loc[ix, 'group']=='Airline Rating' and df.loc[ix, 'component']=='Service Recovery':
                df.loc[ix, 'component']='service'
            if df.loc[ix, 'group']=='Airline Rating' and df.loc[ix, 'component']=='Skytrax rating for Airline':
                df.loc[ix, 'component']='skytrax airline'
            if df.loc[ix, 'group']=='Airline Rating' and df.loc[ix, 'component']=='Skytrax rating for Hub airport':
                df.loc[ix, 'component']='skytrax airport'

            if df.loc[ix, 'group']=='Capacity/Schedule' and df.loc[ix, 'component']=='Block Time':
                df.loc[ix, 'component']='blocktime'
            if df.loc[ix, 'group']=='Capacity/Schedule' and df.loc[ix, 'component']=='Connecting time (convenience)':
                df.loc[ix, 'component']='blocktime'

            if df.loc[ix, 'group']=='Market Rating' and df.loc[ix, 'component']=='Growth of Market':
                df.loc[ix, 'component']='Growth of market'
            if df.loc[ix, 'group']=='Market Rating' and df.loc[ix, 'component']=='No: of Competitors':
                df.loc[ix, 'component']='No: of competitors'

            if df.loc[ix, 'group']=='Distributors Rating' and df.loc[ix, 'region']==None and df.loc[ix, 'country']==None and df.loc[ix, 'pos']==None:
                df.loc[ix, 'rating_definition_level']='pos_od_compartment'

            if df.loc[ix, 'group']=='Fares Rating' and df.loc[ix, 'region']==None and df.loc[ix, 'country']==None and df.loc[ix, 'pos']==None:
                df.loc[ix, 'rating_definition_level']='pos_od_compartment'

        return df


    @measure(JUPITER_LOGGER)
    def normalize_grp_wts(df):
        '''
        calculate normalized group weights that add to 1.0
        '''
        df['weight']=pd.to_numeric(df['weight'], errors='coerce')
        ser=(df.groupby(['compartment', 'group'])['weight'].sum()
           .groupby(level = 0).transform(lambda x: x/x.sum()))
        ser.rename('norm_grp_wt', inplace=True)
        df2=ser.to_frame()
        df2.reset_index(inplace=True) #converts index into columns; inplace=True does the replacement on the dataframe df itself (without it a copy of dataframe would be returned)
        df=pd.merge(df, df2, how='left', on=['compartment', 'group'])
        return df


    @measure(JUPITER_LOGGER)
    def normalize_comp_wts(df):
        '''
        calculate normalized component weights that add to 1.0
        '''
        df['weight']=pd.to_numeric(df['weight'], errors='coerce')
        ser=(df.groupby(['compartment', 'group', 'component'])['weight'].sum()
           .groupby(level = [0,1]).transform(lambda x: x/x.sum()))
        ser.rename('norm_comp_wt', inplace=True)
        df2=ser.to_frame()
        df2.reset_index(inplace=True) #converts index into columns; inplace=True does the replacement on the dataframe df itself (without it a copy of dataframe would be returned)
        df=pd.merge(df, df2, how='left', on=['compartment', 'group', 'component'])
        return df


    @measure(JUPITER_LOGGER)
    def get_list_of_ods():
        cursor = db.JUP_DB_OD_Master.distinct('OD')
        list_ods=[]
        for c in cursor:
            list_ods.append(c)
        return sorted(list_ods)


    @measure(JUPITER_LOGGER)
    def batch_loop(chosen_ods, df_nonod_ratings_global, df_grp_wts, df_comp_wts, tot_mkts, dict_mkts_to_be_printed):
        '''
        main loop for this batch of ods
        '''

        #get raw ratings (but after filteting out invalid compartments) from collection, which will be used in multiple paces
        df_od_ratings_batch=get_od_ratings_batch(chosen_ods)
        if df_od_ratings_batch is None: #no ratings record for this batch
            return tot_mkts

        #get unique markets
        dict_mkts={}
        df=df_od_ratings_batch.reset_index()
        for ix in df.index:
            if df.loc[ix, 'pos'] is not None:
                dict_mkts[(df.loc[ix, 'compartment'], df.loc[ix, 'origin'], df.loc[ix, 'destination'], df.loc[ix, 'pos'])]=1
        tot_mkts += len(dict_mkts)
        print 'L8 #unique_markets in batch=', len(dict_mkts), ', total markets so far=', tot_mkts

        #print 'M31a unique_markets', dict_mkts

        n_empty_templates=0
        n_good_templates=0
        for mkt in dict_mkts:
            #print 'M35 market', mkt
            df_template, od_airlines = build_template(mkt, df_od_ratings_batch, df_nonod_ratings_global, df_grp_wts, df_comp_wts)
            if df_template is None: #skip rating for this market
                n_empty_templates+=1
                continue

            n_good_templates+=1

            airline_ratings, group_ratings=calculate_ratings(df_template, od_airlines, mkt)

            update_collection(mkt, df_grp_wts, airline_ratings, group_ratings)

            if dict_mkts_to_be_printed is not None:
                if mkt in dict_mkts_to_be_printed:
                    print 'L100a', mkt
                    print 'L100b, airline ratings', airline_ratings
                    print 'L100c final template'
                    print df_template

        print 'empty_templates/good templates=', n_empty_templates, n_good_templates
        return tot_mkts


    @measure(JUPITER_LOGGER)
    def get_od_ratings_batch(chosen_ods):
        """
        Get ratings from JUP_DB_Data_Competitor_Ratings collection
        if ratings are at od level, take only those airlines that are in df_available_mkts
        if od is wildcarded, retain all the ratings from JUP_DB_Data_Competitor_Ratings
        """
        query={}
        orq = []
        for i in chosen_ods:
            orqi={'origin':i[:3], 'destination':i[-3:]}
            #print orqi
            #print 'orqi', orqi
            orq.append(orqi)
        query['$or']=orq
        df=get_ratings_from_collection(query) #df_ratings_batch_raw
        #print 'M26 od_ratings for this batch', df
        return df


    @measure(JUPITER_LOGGER)
    def get_ratings_from_collection(query):
        '''
        this is called by 2 different functions, one for od=none and other for od=not none
        it weeds out incorrect compartments
        it checks if there is a column called 'ratings' in the collection
        '''

        if query ==None:
            cursor = db.JUP_DB_Data_Competitor_Ratings2.find({'origin':None, 'destination':None})
            print 'L7a docs in JUP_DB_Data_Competitor_Ratings with origin=None and destination=None', cursor.count()
            if cursor.count() == 0:
                print 'FATAL ERROR: no docs in JUP_DB_Data_Competitor_Ratings for non-od groups'
                raise ''
        else:
            cursor = db.JUP_DB_Data_Competitor_Ratings2.find(query)
            #print 'L7b recs in JUP_DB_Data_Competitor_Ratings which have non-null origin and destination in this batch', cursor.count()
            if cursor.count() == 0:
                print 'ERROR: no docs in JUP_DB_Data_Competitor_Ratings for this batch, skipping this batch'
                return None


        df=pd.DataFrame(list(cursor))
        df=df[(df['compartment'] != 'NA') & (df['compartment'] != 'N/A') & (df['compartment'] != 'na') & (df['compartment'] != 'n/a') & (df['compartment'] != 'Others')]
        if query==None:
            df.loc[:, 'pos']=None

        #check if there is a "ratings" in header
        try:
            df['ratings']
        except:
            msg="record in JUP_DB_Data_Competitor_Ratings collection doesnt have ratings"
            print msg
            obj_err = error_class.ErrorObject(error_class.ErrorObject.WARNING, get_module_name(), get_arg_lists(inspect.currentframe()))
            obj_err.append_to_error_list(msg)
            obj_err_main.append_to_error_object_list(obj_err)
            raise obj_err_main

    ##    print 'L30', df[['compartment', 'origin', 'destination', 'pos']]
    ##    df.loc[:['c', 'o', 'd', 'p']] = df.loc[:, ['compartment', 'origin', 'destination', 'pos']]
    ##    df_markets = df.index.unique()
    ##    df_markets[index1] = df_markets.index()
    ##    print 'L29', df_markets

        df['ix_compartment']=df['compartment']
        df['ix_origin']=df['origin']
        df['ix_destination']=df['destination']
        try:
            df['ix_pos']=df['pos']
        except:
            df['pos']=None
            df['ix_pos']=None
        df['ix_group']=df['group']
        df['ix_component']=df['component']
        df=df.set_index(['ix_compartment', 'ix_origin', 'ix_destination', 'ix_pos', 'ix_group', 'ix_component'])

        if query ==None:
            print 'L9a nonod recs in JUP_DB_Data_Competitor_Ratings after filtering for invalid compartments', df.compartment.count()
            #print 'L27a nonod ratings', df
        else:
            pass
            #print 'L9b od batch recs in JUP_DB_Data_Competitor_Ratings after filtering for invalid compartments', df.compartment.count()
            #print 'L27b od ratings', df

        return df #df_ratings_raw


    @measure(JUPITER_LOGGER)
    def build_template(mkt, df_od_ratings_batch, df_nonod_ratings_global, df_grp_wts, df_comp_wts):
        '''
        Build template(as in the spreadsheet made by Arvind/Mahesh) for this market
        Primarily it contaians all components for market
        columns include group, component, normalized group weight, normalized component weight, rating_definition_level, one column for each arline containing component ratings
        '''
        compartment, origin, destination, pos = mkt
        od=origin+destination

        #set up the template from df_comp_wts
        df_template = seed_template(compartment, od, df_comp_wts)  #retrieves component weights for this market from df_comp_wts (which is for entire batch of markets)
        if df_template is None:
            return df_template, None
        df_template['compartment']=compartment
        df_template['origin']=origin
        df_template['destination']=destination
        df_template['pos']=pos

        #get group weights from df_grp_wts
        df_template = pd.merge(df_template, df_grp_wts[['compartment', 'group', 'norm_grp_wt']], on=['compartment', 'group'], how='left')
        #print 'M36d template with od+nonod ratings (without imputing) and weights', "(" + str(df_template.compartment.count())+" recs)", df_template
        #print 'M36f', df_template

        #incorporate rating_definition_level into template from df_comp_wts
        df_tmp=get_rating_definition_level(compartment, df_comp_wts)
        if df_tmp is None:
            return None, None
        #merge template with df_tmp to get rating_definition_level
        df_template = pd.merge(df_template, df_tmp[['group', 'component', 'rating_definition_level']], on=['group', 'component'], how='left')
        #print 'M36a template with od ratings (without imputing) and weights', "(" + str(df_template.compartment.count())+" recs)", df_template

        #pull out ratings for this market from df_od_ratings_batch
        df_ratings_tmp=get_ratings_for_template_from_od_ratings_batch(mkt, df_od_ratings_batch)
        #merge ratings (without imputing) from groups which have ratings at od level with the template
        df_template = pd.merge(df_template, df_ratings_tmp[['group', 'component', 'ratings']], on=['group', 'component'], how='left')
        #print 'M36a template with od ratings (without imputing) and weights', "(" + str(df_template.compartment.count())+" recs)", df_template

        #merge ratings (without imputing) from groups which have ratings at global level (right now, 'Product Rating' and 'Airline Rating') with the template
        #these non-od group ratings are not present in the previous merge
        df_template = pd.merge(df_template, df_nonod_ratings_global[['compartment', 'group', 'component', 'ratings']], on=['compartment', 'group', 'component'], how='left')
        #print 'M36c template with od+nonod ratings (without imputing) and weights', "(" + str(df_template.compartment.count())+" recs)", df_template

        #build one column in the template for each airline in the od-group list
        #if an airline is in any od-type group, it must be a column
        #if an airline is in no  od-type group, it should not be a column
        od_airlines = create_airline_cols_in_template(df_template, mkt)
        if od_airlines=={}:
            return None, None

        #if airline is present in any od-group ratings, and is also in ratings_x(od-group) or ratings_y(non-od group), copy the ratings into the template
        #if not, let the ratings for the airline be IMPUTED_RATING
        fill_template_for_ratings(df_template, od_airlines)
        #print 'M36d template with od+nonod ratings (without imputing) and weights', "(" + str(df_template.compartment.count())+" recs)", df_template

        return df_template, od_airlines


    @measure(JUPITER_LOGGER)
    def seed_template(compartment, od, df_comp_wts):
        '''
        set up a list of components as template from df_comp_wts
        this template will be gradually built up by other funcitons
        currently only network level weights are supported
        '''
    ##    print 'L37 od', od
        #try od level weights first
    ##    df=df_comp_wts.loc[[compartment, None, None, od], ['group', 'component', 'weight', 'norm_comp_wt']]
    ##    dfc=df.group.count()
    ##    if dfc > 0:
    ##        return df

        #try network level weights
        #df=df_comp_wts.loc[(df_comp_wts.compartment==compartment) & (df_comp_wts.region==None) and (df_comp_wts.country==None) and (df_comp_wts.od==None), ['weight', 'norm_comp_wt']]
        try:
            df=df_comp_wts.loc[[compartment, None, None, None], ['group', 'component', 'norm_comp_wt']]
            #print 'LL1', df.head()
        except:
            return None
        dfc=df.group.count()
        #print 'M33 #template comp wts for od ', od, '=', dfc

        return df


    @measure(JUPITER_LOGGER)
    def get_rating_definition_level(compartment, df_comp_wts):
        '''
        #incorporate rating_definition_level into template
        '''

        #get rating definition level from compartment/network level weights recs in df_comp_wts
        #df_tmp=df_comp_wts[df_comp_wts['compartment']==compartment & df_comp_wts['region']==None & df_comp_wts['country']==None & df_comp_wts['od']==None, ['weight', 'rating_definition_level']] #network level weights
        #df_tmp=df_comp_wts.loc[(df_comp_wts['compartment']==compartment) & (df_comp_wts['region']==None) & (df_comp_wts['country']==None) & (df_comp_wts['od']==None), ['weight', 'rating_definition_level']] #network level weights
        #df_tmp=df_comp_wts.loc[df_comp_wts['compartment']==compartment, ['weight', 'rating_definition_level']] #network level weights
        #df_tmp=df_comp_wts.loc[df_comp_wts['compartment']==compartment, df_comp_wts.columns] #network level weights
        #print 'L38 df_tmp(recs=' + str(df_tmp.weight.count()) + ")", df_tmp
        #df_tmp=df_tmp.loc[df_tmp['pos'] ==None, df_tmp.columns] #network level weights
        #doing the pruning record by record because the doing the wholesale drop above doesnt work

        try:
            df_tmp=df_comp_wts.loc[df_comp_wts['compartment']==compartment, df_comp_wts.columns]
        except:
            return None
        if df_tmp['compartment'].count()==0:
            return None
        for ix in df_tmp.index:
            if df_tmp.loc[ix, 'region'] is not None:
                df_tmp=df_tmp.drop(ix)
        for ix in df_tmp.index:
            if df_tmp.loc[ix, 'country'] is not None:
                df_tmp=df_tmp.drop(ix)
        for ix in df_tmp.index:
            if df_tmp.loc[ix, 'od'] is not None:
                df_tmp=df_tmp.drop(ix)
        for ix in df_tmp.index:
            if df_tmp.loc[ix, 'pos'] is not None:
                df_tmp=df_tmp.drop(ix)
        for ix in df_tmp.index:
            if df_tmp.loc[ix, 'group']=='Market Rating' and df_tmp.loc[ix, 'component']=='Market Elasticity':
                df_tmp.loc[ix, 'rating_definition_level']='pos_od_compartment'
        #print 'M36x', df_tmp
        return df_tmp


    @measure(JUPITER_LOGGER)
    def get_ratings_for_template_from_od_ratings_batch(mkt, df_od_ratings_batch):
        '''
        pull out ratings for this market from df_od_ratings_batch, which contains ratings for all ods in this batch
        logic is:
           1. collect ratings records for 'pos' level records (Fares Rating, Distributors Rating, Market Rating groups only)
           2. collect ratings records for 'pos' level records ('Capacity/Schedule' Rating)
           3. concatenate the two dataframes row-wise
        '''

        compartment, origin, destination, pos = mkt
        mkt_without_pos=[compartment, origin, destination, None]

        #pull out ratings for this market from df_od_ratings_batch - this is only for groups which are at pos level
        df_ratings_tmp1=pd.DataFrame()
        try:
            df_ratings_tmp1=df_od_ratings_batch.loc[mkt,df_od_ratings_batch.columns]
        except:
            pass
        #print 'M36b3 ratings for given market df_od_ratings_batch', df_ratings_tmp1

        df_ratings_tmp2=pd.DataFrame()
        df_ratings_tmp2 = df_od_ratings_batch[df_od_ratings_batch['compartment']==compartment]
        #if compartment=='Y' and origin=='ADD' and destination=='AHB' and pos=='ADD':
        #    print 'M39', len(df_ratings_tmp2)
        df_ratings_tmp2 = df_ratings_tmp2[df_ratings_tmp2['origin']==origin]
        #if compartment=='Y' and origin=='ADD' and destination=='AHB' and pos=='ADD':
        #    print 'M40', len(df_ratings_tmp2)
        df_ratings_tmp2 = df_ratings_tmp2[df_ratings_tmp2['destination']==destination]
        #if compartment=='Y' and origin=='ADD' and destination=='AHB' and pos=='ADD':
        #    print 'M41', len(df_ratings_tmp2), df_ratings_tmp2
        df_ratings_tmp2.reset_index(inplace=True)
        for ix in df_ratings_tmp2.index:
            if df_ratings_tmp2.loc[ix, 'pos'] is not None:
                df_ratings_tmp2=df_ratings_tmp2.drop(ix)
        #if compartment=='Y' and origin=='ADD' and destination=='AHB' and pos=='ADD':
        #    print 'M42', len(df_ratings_tmp2)
        #if compartment=='Y' and origin=='ADD' and destination=='AHB' and pos=='ADD':
        #    print 'M38', df_ratings_tmp2

    ##    try:
    ##        df_ratings_tmp2=df_od_ratings_batch.loc[mkt_without_pos, df_od_ratings_batch.columns]  #this gets "Capacity" group records, which are not at pos level
    ##    except:
    ##        pass
    ##    if len(df_ratings_tmp2) > 0:
    ##        for ix in df_ratings_tmp2.index:
    ##            try:
    ##                a=df_ratings_tmp2.loc[ix, 'pos']
    ##            except:
    ##                print 'FAT ERR: mkt=', mkt
    ##                print 'M100 df_ratings_tmp2', len(df_ratings_tmp2), mkt_without_pos, ix, df_ratings_tmp2
    ##            if df_ratings_tmp2.loc[ix, 'pos'] is not None:
    ##                df_ratings_tmp2 = df_ratings_tmp2.drop(ix)
        #print 'M36b4 ratings for given market df_od_ratings_batch', df_ratings_tmp2

        df_ratings_tmp=pd.concat([df_ratings_tmp1, df_ratings_tmp2])
        #print 'M36b ratings for given market', df_ratings_tmp, "(" + str(df_ratings_tmp.compartment.count())+" recs)"
        #print 'M36b2 ratings for given market df_od_ratings_batch', df_ratings_tmp
        #print 'M36b2 done'

        return df_ratings_tmp


    @measure(JUPITER_LOGGER)
    def create_airline_cols_in_template(df_template, mkt):
        '''
        #build one column in the template for each airline in the od-group list
        #if an airline is in any od-type group, it must be a column
        #if an airline is in no  od-type group, it should not be a column
        '''
        compartment, origin, destination, pos = mkt

        #build one column in the template for each airline in the od-group list
        #if an airline is in any od-type group, it must be a column
        #if an airline is in no  od-type group, it should not be a column
        od_airlines={}
        #if compartment=='Y' and origin=='ADD' and destination=='AHB':
        #    print 'M37', df_template
        for ix in df_template.index:
            rdl=df_template.loc[ix, 'rating_definition_level']
            if rdl=='network':
                continue
            rx=df_template.loc[ix, 'ratings_x']
            if is_dict(rx):
                od_airlines.update(rx)
        #print 'M37a', od_airlines
        return od_airlines


    @measure(JUPITER_LOGGER)
    def fill_template_for_ratings(df_template, od_airlines):
        '''
        #if airline is present in any od-group ratings, and is also in ratings_x(od-group) or ratings_y(non-od group), copy the ratings into the template
        #if not, let the ratings for the airline be IMPUTED_RATING
        '''
        list_airlines = []
        for airline in od_airlines:
            df_template[airline]=IMPUTED_RATING
        #print 'M36.4', df_template
        for ix in df_template.index:
            ratings={}
            ratings_x=df_template.loc[ix, 'ratings_x']
            ratings_y=df_template.loc[ix, 'ratings_y']
            if is_dict(ratings_y):
                ratings=ratings_y;
            if is_dict(ratings_x):
                ratings=ratings_x;
            if ratings=={}:
                continue #no ratings for this group, keep default of IMPUTED_RATING from previous step for every airline
            for airline in ratings:
                if airline in od_airlines:
                    df_template.loc[ix, airline]=ratings[airline]
        #print 'M36.5', df_template


    @measure(JUPITER_LOGGER)
    def calculate_ratings(df_template, od_airlines, mkt):
        airline_ratings={}
        for airline in od_airlines:
            airline_ratings[airline] = (df_template[airline]*df_template['norm_grp_wt']*df_template['norm_comp_wt']).sum()
        groups=df_template['group'].unique()
        group_ratings={}
        #print 'L200', df_template
        for group in groups:
            dfg=df_template[df_template['group']==group]
            group_ratings[group]={}
            for airline in od_airlines:
    ##            print 'L200e', mkt
    ##            print 'L200f', group
    ##            print 'L200a airline', airline
    ##            print 'L200b ratings', dfg[airline]
    ##            print 'L200c ratings out', (dfg[airline]*dfg['norm_comp_wt']).sum()
                group_ratings[group][airline] = (dfg[airline]*dfg['norm_comp_wt']).sum()
    ##            print 'L200d', group_ratings
        return airline_ratings, group_ratings


    @measure(JUPITER_LOGGER)
    def update_collection(mkt, df_grp_wts, airline_ratings, group_ratings):
        compartment, origin, destination, pos=mkt
        dict_rec={}
        dict_rec['compartment']=compartment
        dict_rec['origin']=origin
        dict_rec['destination']=destination
        dict_rec['pos']=pos
        dict_rec['ratings']=airline_ratings
        for group in group_ratings:
            #print 'group===', group
            for ix in df_grp_wts.index:
                if df_grp_wts.loc[ix, 'compartment']==compartment and df_grp_wts.loc[ix, 'group']==group:
                    dict_rec[group+'weight']=df_grp_wts.loc[ix, 'norm_grp_wt']
                    break
            #print 'M99a grp wt', dict_rec[group+'weight']
            dict_rec[group+'_ratings']=group_ratings[group]
            #print 'M99b grp rtg', dict_rec[group+'_ratings']
        dict_rec['_id']=ObjectId()
        dict_rec['last_update_date_gmt'] = "{:%Y-%m-%d}".format(datetime.utcnow())
        #print 'M100d, dict_rec', dict_rec
        db.JUP_DB_Competitor_Ratings4.upsert(dict_rec,dict_rec)



    ##def call_trigger(old_doc, new_doc):
    ##    if old_doc[airline]==net.Host_Airline_Code:
    ##        name='host_rating_change'
    ##        obj = HostRatingChange(name = name, old_database_doc = old_doc, new_database_doc = new_doc)
    ##        obj.do_analysis()
    ##    else:
    ##        name='competitor_rating_change'
    ##        obj = CompRatingChange(name = name, old_database_doc = old_doc, new_database_doc = new_doc)
    ##        obj.do_analysis()


    @measure(JUPITER_LOGGER)
    def is_dict(x):
        #print 'x43', type(x) is dict
        return type(x) is type(dict())


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




    ##def check_groups_and_components(dict_weights, dict_network_weights, compartment, region, country, od):
    ##    for group in dict_weights:
    ##        for component in dict_weights[group]:
    ##            try:
    ##                dict_network_weights[group][component]
    ##            except:
    ##                obj_err = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1, get_module_name(), get_arg_lists(inspect.currentframe()))
    ##                obj_err.append_to_error_list("".join(["compartment=", compartment, "region=", region, "country=", country, "od=", od, ": extra groups/components found in weights for this hierarchy"]))
    ##                obj_err_main.append_to_error_object_list(obj_err)
    ##                return "ERROR"
    ##    for group in dict_network_weights:
    ##        for component in dict_network_weights[group]:
    ##            try:
    ##                dict_weights[group][component]
    ##            except:
    ##                obj_err = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1, get_module_name(), get_arg_lists(inspect.currentframe()))
    ##                obj_err.append_to_error_list("".join(["compartment=", compartment, "region=", region, "country=", country, "od=", od, ": some groups/components are not found in weights for this hierarchy"]))
    ##                obj_err_main.append_to_error_object_list(obj_err)
    ##                return "ERROR"
    ##    return "OK"
    ##
    ##


    start_time = time.time()
    print start_time
    start_time = time.time()
    print start_time
    # MONGO_CLIENT_URL = '13.92.251.7:42525'
    #net.MONGO_CLIENT_URL
    # ANALYTICS_MONGO_USERNAME = 'analytics'
    # ANALYTICS_MONGO_PASSWORD = 'KNjSZmiaNUGLmS0Bv2'
    # JUPITER_DB = 'fzDB_stg'
    # client = pymongo.MongoClient(MONGO_CLIENT_URL)
    # client.the_database.authenticate(ANALYTICS_MONGO_USERNAME, ANALYTICS_MONGO_PASSWORD, source='admin')
    # db = client[JUPITER_DB]
    #from pymongo import MongoClient
    #client = MongoClient("mongodb://analytics:KNjSZmiaNUGLmS0Bv2@13.92.251.7:42525/")
    #db = client.fzDB_stg
    from jupiter_AI import JUPITER_DB, client
    db = client[JUPITER_DB]

    obj_err_main = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, "main", "")
    obj_err_main.append_to_error_list("main error level")
    DICT_ERROR_ODS={}
    IMPUTED_RATING=5
    MIN_RECORD_LENGTH=60
    TOLERANCE=0.001         ## error tolerance
    COLLECTION = 'JUP_DB_Capacity_1'

    TOT_MKTS=0

    dict_mkts_to_be_printed={('Y', 'ADD', 'AHB', 'ADD'):1, ('Y', Host_Airline_Hub, 'MHD', 'MHD'):1}
    main(dict_mkts_to_be_printed)

    ##print obj_err_main
    dict_errors={}
    dict_errors['program']='competitor ratings'
    dict_errors['date']="{:%Y-%m-%d}".format(datetime.now())
    dict_errors['time']=time.time()
    dict_errors['error description']=obj_err_main.as_string()
    db.JUP_DB_Errors_All.insert(dict_errors)
    print("--- %s seconds ---" % (time.time() - start_time))

if __name__ == "__main__":
    run()
