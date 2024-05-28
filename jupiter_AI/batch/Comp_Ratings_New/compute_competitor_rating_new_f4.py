from jupiter_AI import network_level_params as net, JUPITER_LOGGER
from jupiter_AI.logutils import measure


@measure(JUPITER_LOGGER)
def run():
    """
     Author: Prem Narasimhan
     Completed date:
         first version: Mar 3, 2017
     This program calculates competitor ratings.

     Input collections:
         JUP_DB_Data_Competitor_Weights
         JUP_DB_Data_Competitor_Ratings
     Output collection:
         JUP_DB_Competitor_Ratings
     Overall logic:
         1. Get all weights records from JUP_DB_Competitor_Weights records (at compartment/region/country/pos/od level). region=None means wildcarding or record exists at more graular level (like pos).
         2. Get all available od-compartments/airlines in group='Market Rating' from JUP_DB_Competitor_Ratings collection
         3. Get all available od-compartments/airlines in group='Capacity/Schedule' from JUP_DB_Competitor_Ratings collection
         4. Get od-compartments/airlines in both 2 and 3.
         5. Get all ratings records from JUP_DB_Competitor_Ratings collection
            This step ensures that only those records are retained which are in (4) above.
         6. Partition the ratings records
         7. For each partition,
            calculate ratings and group ratings
            update collection
               old rating is to be copied into old_doc_data, new ratings into new_doc_data and both sent to

    """
    from jupiter_AI.common import ClassErrorObject as error_class
    # import jupiter_AI.common.ClassErrorObject as error_class
    import datetime
    import json
    import time
    import inspect
    import collections
    import pymongo
    from bson.objectid import ObjectId
    from datetime import datetime
    from time import gmtime, strftime
    from copy import deepcopy
    import scipy.stats as ss
    from operator import itemgetter


    @measure(JUPITER_LOGGER)
    def main():
        """
            ## Get weights from JUP_DB_Data_Competitor_Weights collection
            ## the function returns 'error' when:
            ##    a component record is found, but its group has not yet been found
            ##    implemented_flag or rating_definition_flag is not found in network level record (a record is network level  if region=country=pos=od=None)
            ## returns:

        """
        dict_all_weights={}
        check=get_all_weights_records(dict_all_weights)
        if check=='error':
            obj_err = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1, get_module_name(), get_arg_lists(inspect.currentframe()))
            error_msg="error found in get_all_weights_records(), aborting run"
            obj_err.append_to_error_list(error_msg)
            print error_msg
            raise error_msg

        ## checks if granular records have the same set of records as the most gorss_level records (which is at compartment level)
        ##check_hierarchical_weights(dict_all_weights)

        ##   dict_all_weights[compartment][region][country][od]['error_status'] is set to "ERROR" if group weight adds to 0.0
        determine_group_weights(dict_all_weights)

        ##   dict_all_weights[compartment][region][country][od]['error_status'] is set to "ERROR" if component weight within a group adds to 0.0
        determine_component_weights(dict_all_weights)

        ##   gets all available od-compartments in group=Market Rating from JUP_DB_Competitor_Ratings collection
        dict_market_ratings_markets_airlines={}
        get_list_of_ratings_markets_airlines(dict_market_ratings_markets_airlines, 'Market Rating')
        #print 'market rating', dict_market_ratings_markets_airlines


        ##   gets all available od-compartments in group="Capcity/Schedule" from JUP_DB_Competitor_Ratings collection
        dict_capacity_ratings_markets_airlines={}
        get_list_of_ratings_markets_airlines(dict_capacity_ratings_markets_airlines, 'Capacity/Schedule')
        #print 'capacity', dict_capacity_ratings_markets_airlines

        dict_fares_ratings_markets_airlines={}
        get_list_of_ratings_markets_airlines(dict_fares_ratings_markets_airlines, 'Fares Rating')
        #print 'fares0', dict_fares_ratings_markets_airlines

        dict_distributors_ratings_markets_airlines={}
        get_list_of_ratings_markets_airlines(dict_distributors_ratings_markets_airlines, 'Distributors Rating')
        #print 'distributors', dict_distributors_ratings_markets_airlines

        ##   gets od-compartments available in both groups, "Market Rating" and "Capacity/Schedule"
        dict_available_ratings_markets_airlines=get_list_of_available_ratings_markets_airlines(dict_capacity_ratings_markets_airlines, dict_market_ratings_markets_airlines)
        #print 'available' , dict_available_ratings_markets_airlines


        ##   gets all available od-compartments in group=Market Rating from JUP_DB_Competitor_Ratings collection
        dict_available_ratings_markets_airlines = get_list_of_available_ratings_markets_airlines2(dict_available_ratings_markets_airlines, dict_fares_ratings_markets_airlines)
        for compartment in dict_available_ratings_markets_airlines:
            for origin in dict_available_ratings_markets_airlines[compartment]:
                if origin=='PZU':
                    print 'origin=PZUa'
        dict_available_ratings_markets_airlines = get_list_of_available_ratings_markets_airlines2(dict_available_ratings_markets_airlines, dict_distributors_ratings_markets_airlines)
        for compartment in dict_available_ratings_markets_airlines:
            for origin in dict_available_ratings_markets_airlines[compartment]:
                if origin=='PZU':
                    print 'origin=PZUb'
        dict_all_ratings={}
        dict_all_ratings2=get_all_ratings_records(dict_all_ratings, dict_available_ratings_markets_airlines)
        for compartment in dict_all_ratings:
            for origin in dict_all_ratings[compartment]:
                for destination in dict_all_ratings[compartment][origin]:
                    for pos in dict_all_ratings[compartment][origin][destination]:
                        for group in dict_all_ratings[compartment][origin][destination][pos]:
                            for component in dict_all_ratings[compartment][origin][destination][pos][group]:
                                pass
        list_dict_partitioned_od_compartments=get_partitioned_od_compartments(dict_available_ratings_markets_airlines, 1000)
        print 'partition list', len(list_dict_partitioned_od_compartments)
        ##print list_dict_partitioned_od_compartments[0]

        ## calculate ratings in each partition
        for dict_od_compartments_partition in list_dict_partitioned_od_compartments:
            dict_ratings=calculate_ratings(dict_all_ratings, dict_all_weights, dict_od_compartments_partition)
    ##
    ##        dict_group_ratings=calculate_group_ratings(dict_all_ratings, dict_all_weights, dict_od_compartments_partition)
    ##
    ##        update_collection(dict_ratings, dict_all_ratings2, dict_group_ratings, dict_od_compartments_partition)


    @measure(JUPITER_LOGGER)
    def get_list_of_available_ratings_markets_airlines2(dict_available_ratings_markets_airlines, dict_fares_ratings_markets_airlines):
        #dict_available_ratings_markets_airlines={}
        temp_dict = dict_available_ratings_markets_airlines
        #print 'fares', dict_fares_ratings_markets_airlines
        #print 'dict', dict_available_ratings_markets_airlines
        for compartment in dict_fares_ratings_markets_airlines:
            for origin in dict_fares_ratings_markets_airlines[compartment]:
                for destination in dict_fares_ratings_markets_airlines[compartment][origin]:
                    for airline in dict_fares_ratings_markets_airlines[compartment][origin][destination]:
                        try:
                            temp_dict[compartment]
                        except:
                            temp_dict[compartment]={}
                        try:
                            temp_dict[compartment][origin]
                        except:
                            temp_dict[compartment][origin]={}
                        try:
                            temp_dict[compartment][origin][destination]
                        except:
                            temp_dict[compartment][origin][destination]={}
                        for airline in dict_fares_ratings_markets_airlines[compartment][origin][destination]:
                            temp_dict[compartment][origin][destination][airline]=dict_fares_ratings_markets_airlines[compartment][origin][destination][airline]
        for compartment in temp_dict:
            for origin in temp_dict[compartment]:
                if origin=='PZU':
                    print 'origin=PZUc'
        #print 'temp_dict', temp_dict
        return temp_dict

    @measure(JUPITER_LOGGER)
    def get_all_weights_records(dict_weights):
        '''
        get all weights records from the search list above
        the function returns 'error' when a component record is found, but its group has not yet been found
        it also returns error when implemented_flag or rating_definition_flag is not found in network level record
        '''
        print "dict_weights: ", dict_weights
        cursor = db.JUP_DB_Data_Competitor_Weights.find()
        print 'bbb', cursor.count()
        if cursor.count() == 0:
            obj_err = error_class.ErrorObject(error_class.ErrorObject.WARNING, get_module_name(), get_arg_lists(inspect.currentframe()))
            error_msg="no records found in JUP_DB_Data_Competitor_Weights collection"
            obj_err.append_to_error_list(error_msg)
            obj_err_main.append_to_error_object_list(obj_err)
            raise error_msg
        for c in cursor:
            compartment     = c['compartment']
            if compartment=='Others':
                continue
            region          = c['region']
            country         = c['country']
            pos             = c['pos']
            od              = c['od']
            try:
                dict_weights[compartment]
            except:
                dict_weights[compartment]={}
            try:
                dict_weights[compartment][region]
            except:
                dict_weights[compartment][region]={}
            try:
                dict_weights[compartment][region][country]
            except:
                dict_weights[compartment][region][country]={}
            try:
                dict_weights[compartment][region][country][od]
            except:
                dict_weights[compartment][region][country][od]={}

            group=c['group']
            component=c['component']
            if group==component:
                print "group: ", group##group level record
                print "comp: ", component
                try:
                    dict_weights[compartment][region][country][od][group]
                except:
                    dict_weights[compartment][region][country][od][group]={}
                    dict_weights[compartment][region][country][od][group]['group_weight']=c['weight']
                continue
            ##print compartment, region,country, od, group
            print "group_out: ", group
            print "comp_out: ", component
            try:
                dict_weights[compartment][region][country][od][group]  ## we are in a component level record, and its group has not yet been found, so cannot proceed
            except:
                print "in except1"
                dict_weights[compartment][region][country][od][group]={}
            try:
                dict_weights[compartment][region][country][od][group][component]
            except:
                dict_weights[compartment][region][country][od][group][component]={}
                dict_weights[compartment][region][country][od][group][component]['component_weight']=c['weight']
            if region==None and country==None and od==None:             ## network level record
                try:
                    dict_weights[compartment][region][country][od][group][component]['implemented_flag']=c['implemented_flag']
                    dict_weights[compartment][region][country][od][group][component]['rating_definition_level']=c['rating_definition_level']
                except:
                    obj_err = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, get_module_name(), get_arg_lists(inspect.currentframe()))
                    print "Severe error: implementation_flag or rating_definition_level not found in network level record, run aborting !!!!!!!!"
                    obj_err.append_to_error_list("Severe error: implementation_flag or rating_definition_level not found in network level record, run aborting !!!!!!!!")
                    obj_err_main.append_to_error_object_list(obj_err)
                    return 'error'
            # break
    ##    for compartment in dict_weights:
    ##        for region in dict_weights[compartment]:
    ##            for country in dict_weights[compartment][region]:
    ##                for od in dict_weights[compartment][region][country]:
    ##                    for group in dict_weights[compartment][region][country][od]:
    ##                        if group=='error_status':
    ##                            continue
    ##                        print compartment, region,country, od, group
        # print "dict_weights: ", dict_weights[compartment][region][country][od][group]
        # p
        for compartment in dict_weights:
            for region in dict_weights[compartment]:
                for country in dict_weights[compartment][region]:
                    for od in dict_weights[compartment][region][country]:
                        for group in dict_weights[compartment][region][country][od]:
                            if group=='error_status':
                                continue
                            try:
                                print "------------->"
                                print dict_weights[compartment][region][country][od][group]
                                dict_weights[compartment][region][country][od][group]['group_weight']
                            except:
                                ## we are in a component level record, and its group has not yet been found, so cannot proceed
                                obj_err = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, get_module_name(), get_arg_lists(inspect.currentframe()))
                                error_msg="compartment=" + str(compartment) + ", region=" + str(region) + ", country="+str(country) + ", od=" + str(od) + ", group = " + str(group) + ", no group level record found, stopping calculation"
                                print error_msg
                                obj_err.append_to_error_list(error_msg)
                                obj_err_main.append_to_error_object_list(obj_err)
                                return 'error'

        ncomponents=0
        nods=0
        for compartment in dict_weights:
            for region in dict_weights[compartment]:
                for country in dict_weights[compartment][region]:
                    for od in dict_weights[compartment][region][country]:
                        nods +=1
                        for group in dict_weights[compartment][region][country][od]:
                            if group=='error_status':
                                continue
                            for component in dict_weights[compartment][region][country][od][group]:
                                ncomponents+=1
        print 'nods=', nods, ', ncomponents=', ncomponents


    @measure(JUPITER_LOGGER)
    def determine_group_weights(dict_all_weights):
        '''
        calculate normalized weights that add to 1.0
        '''
        ##   dict_all_weights[compartment][region][country][od]['error_status']='OK' or "ERROR"
        ##   dict_all_weights[compartment][region][country][od][group]['group_weight']
        ##   dict_all_weights[compartment][region][country][od][group]['normalized_group_weight']
        ##   dict_all_weights[compartment][region][country][od][group][component]['component_weight']
        ##   dict_all_weights[compartment][region][country][od][group][component]['normalized_component_weight']
        ##   dict_all_weights[compartment][region][country][od][group][component]['implemented_flag']
        ##   dict_all_weights[compartment][region][country][od][group][component]['rating_definition_level']
        ##   dict_all_weights[compartment][region][country][od]['error_status']='OK'
        ##   dict_all_weights[compartment][region][country][pos][od][group][component]['rating_definition_level']
        for compartment in dict_all_weights:
            for region in dict_all_weights[compartment]:
                for country in dict_all_weights[compartment][region]:
                    for od in dict_all_weights[compartment][region][country]:
                        dict_weights=dict_all_weights[compartment][region][country][od]
                        total_weight=0.0
                        for group in dict_weights:
                            if group=='error_status':
                                continue
                            total_weight += float(dict_weights[group]['group_weight'])
                        if abs(total_weight) < TOLERANCE:
                            for group in dict_weights:
                                dict_weights[group]['normalized_group_weight'] = 0.0
                            obj_err = error_class.ErrorObject(error_class.ErrorObject.WARNING, get_module_name(), get_arg_lists(inspect.currentframe()))
                            obj_err.append_to_error_list("".join(["compartment = ", compartment, "region=", region, "country=", country, "od=", od, ": total group_weight=0"]))
                            obj_err_main.append_to_error_object_list(obj_err)
                            dict_all_weights[compartment][region][country][od]['error_status']='ERROR'
                            continue
                        for group in dict_weights:
                            #print 'n4', dict_weights
                            dict_weights[group]['normalized_group_weight']=float(dict_weights[group]['group_weight'])/total_weight
                            print 'n5', group, dict_weights[group]['normalized_group_weight']
                        dict_all_weights[compartment][region][country][od]['error_status']='OK'
    ##    for compartment in dict_all_weights:
    ##        for region in dict_all_weights[compartment]:
    ##            for country in dict_all_weights[compartment][region]:
    ##                for od in dict_all_weights[compartment][region][country]:
    ##                    for group in dict_all_weights[compartment][region][country][od]:
    ##                        if group=='error_status':
    ##                            continue
    ##                        print compartment, region, country, od, group, dict_all_weights[compartment][region][country][od][group]['normalized_group_weight']


    @measure(JUPITER_LOGGER)
    def determine_component_weights(dict_all_weights):
        '''
        calculate normalized weights that add to 1.0 within each group
        '''
        for compartment in dict_all_weights:
            for region in dict_all_weights[compartment]:
                for country in dict_all_weights[compartment][region]:
                    for od in dict_all_weights[compartment][region][country]:
                        dict_weights=dict_all_weights[compartment][region][country][od]
                        if dict_all_weights[compartment][region][country][od]['error_status']=="ERROR":
                            continue
                        for group in dict_weights:
                            if group=='error_status':
                                continue
                            total_weight=0.0
                            for component in dict_weights[group]:
                                if component=='group_weight' or component=='normalized_group_weight':
                                    continue
                                total_weight += float(dict_weights[group][component]['component_weight'])
                            if abs(total_weight) < TOLERANCE:
                                for component in dict_weights[group]:
                                    dict_weights[group]['normalized_component_weight'] = 0.0
                                obj_err = error_class.ErrorObject(error_class.ErrorObject.WARNING, get_module_name(), get_arg_lists(inspect.currentframe()))
                                obj_err.append_to_error_list("".join(["compartment = ", compartment, "region=", region, "country=", country, "od=", od, 'group', group, ": total component_weight=0"]))
                                obj_err_main.append_to_error_object_list(obj_err)
                                dict_all_weights[compartment][region][country][od]['error_status']='ERROR'
                                continue
                            for component in dict_weights[group]:
                                if component=='group_weight' or component=='normalized_group_weight':
                                    continue
                                dict_weights[group][component]['normalized_component_weight']=float(dict_weights[group][component]['component_weight'])/total_weight
                        dict_all_weights[compartment][region][country][od]['error_status']='OK'
    ##    for compartment in dict_all_weights:
    ##        for region in dict_all_weights[compartment]:
    ##            for country in dict_all_weights[compartment][region]:
    ##                for od in dict_all_weights[compartment][region][country]:
    ##                    for group in dict_all_weights[compartment][region][country][od]:
    ##                        if group=='error_status':
    ##                            continue
    ##                        for component in dict_all_weights[compartment][region][country][od][group]:
    ##                            if component=='group_weight' or component=='normalized_group_weight':
    ##                                continue
    ##                            print compartment, region, country, od, group, component, ',', dict_all_weights[compartment][region][country][od][group][component]['normalized_component_weight']


    @measure(JUPITER_LOGGER)
    def get_list_of_ratings_markets_airlines(dict_group_ratings, group):
        #cursor = db.JUP_DB_Data_Competitor_Ratings_Test1.find({'group':group, 'origin':'SKT', 'destination':'RUH'})

        #cursor = db.JUP_DB_Data_Competitor_Ratings_Test1.find({'group':group, 'origin':'CMB', 'destination':'DXB'})
        cursor = db.JUP_DB_Data_Competitor_Ratings.find({'group':group})
        print 'cursor_count', cursor.count(), group
        print 'recs in JUP_DB_Data_Competitor_Ratings',group, cursor.count()
    ##    if cursor.count() == 0:
    ##        obj_err = error_class.ErrorObject(error_class.ErrorObject.WARNING, get_module_name(), get_arg_lists(inspect.currentframe()))
    ##        error_msg="no records found in JUP_DB_Data_Competitor_Ratings collection for group=" +str(group)
    ##        obj_err.append_to_error_list(error_msg)
    ##        print error_msg
    ##        obj_err_main.append_to_error_object_list(obj_err)
    ##        raise error_msg
        for c in cursor:
            compartment     = c['compartment']
            if compartment=='NA' or compartment=='N/A' or compartment=='na' or compartment=='n/a':
                continue
            origin          = c['origin']
            destination     = c['destination']
            try:
                pos=c['pos']
            except:
                pos=""
            if pos==None:
                pos=""
            try:
                dict_group_ratings[compartment]
            except:
                dict_group_ratings[compartment]={}
            try:
                dict_group_ratings[compartment][origin]
            except:
                dict_group_ratings[compartment][origin]={}
            try:
                dict_group_ratings[compartment][origin][destination]
            except:
                dict_group_ratings[compartment][origin][destination]={}
            try:
                dict_group_ratings[compartment][origin][destination][pos]
            except:
                dict_group_ratings[compartment][origin][destination][pos]={}
            for airline in c['ratings']:
                dict_group_ratings[compartment][origin][destination][pos][airline]=1.0
        j=0
        for compartment in dict_group_ratings:
            for origin in dict_group_ratings[compartment]:
                for destination in dict_group_ratings[compartment][origin]:
                    for pos in dict_group_ratings[compartment][origin][destination]:
                        j+=1
        print "no. of ods in", group, ' records:', j
        print 'od1', dict_group_ratings
        return dict_group_ratings


    @measure(JUPITER_LOGGER)
    def get_list_of_available_ratings_markets_airlines(dict_capacity_ratings_markets_airlines, dict_market_ratings_markets_airlines):
        dict_available_ratings_markets_airlines={}
        print 'capp', dict_capacity_ratings_markets_airlines
        for compartment in dict_capacity_ratings_markets_airlines:
            for origin in dict_capacity_ratings_markets_airlines[compartment]:
                for destination in dict_capacity_ratings_markets_airlines[compartment][origin]:
                    for airline in dict_capacity_ratings_markets_airlines[compartment][origin][destination]:
                        try:
                            dict_available_ratings_markets_airlines[compartment]
                        except:
                            dict_available_ratings_markets_airlines[compartment]={}
                        try:
                            dict_available_ratings_markets_airlines[compartment][origin]
                        except:
                            dict_available_ratings_markets_airlines[compartment][origin]={}
                        try:
                            dict_available_ratings_markets_airlines[compartment][origin][destination]
                        except:
                            dict_available_ratings_markets_airlines[compartment][origin][destination]={}
                        for airline in dict_capacity_ratings_markets_airlines[compartment][origin][destination]:
                            dict_available_ratings_markets_airlines[compartment][origin][destination][airline]=dict_capacity_ratings_markets_airlines[compartment][origin][destination][airline]

        for compartment in dict_market_ratings_markets_airlines:
            for origin in dict_market_ratings_markets_airlines[compartment]:
                for destination in dict_market_ratings_markets_airlines[compartment][origin]:
                    for airline in dict_market_ratings_markets_airlines[compartment][origin][destination]:
                        try:
                            dict_available_ratings_markets_airlines[compartment]
                        except:
                            dict_available_ratings_markets_airlines[compartment]={}
                        try:
                            dict_available_ratings_markets_airlines[compartment][origin]
                        except:
                            dict_available_ratings_markets_airlines[compartment][origin]={}
                        try:
                            dict_available_ratings_markets_airlines[compartment][origin][destination]
                        except:
                            dict_available_ratings_markets_airlines[compartment][origin][destination]={}
                        for airline in dict_market_ratings_markets_airlines[compartment][origin][destination]:
                            dict_available_ratings_markets_airlines[compartment][origin][destination][airline]=dict_market_ratings_markets_airlines[compartment][origin][destination][airline]


        k=0
        for compartment in dict_available_ratings_markets_airlines:
            for origin in dict_available_ratings_markets_airlines[compartment]:
                k+=1
        print 'available ods', k
        return dict_available_ratings_markets_airlines


    @measure(JUPITER_LOGGER)
    def get_all_ratings_records(dict_all_ratings, available_ratings_markets_airlines):
        """
        Get ratings from JUP_DB_Data_Competitor_Ratings collection
        if ratings are at od level, take only those airlines that are in availabl_ratings_markets_airlines
        if od is wildcarded, retain all the ratings from JUP_DB_Data_Competitor_Ratings
        """
        dict_all_ratings2={}

        #cursor = db.JUP_DB_Data_Competitor_Ratings_Test1.find({'origin':'SKT', 'destination':'RUH'})
        #qor=[{'origin':'CMB', 'destination':'DXB'}, {'group':'Product Rating'}]
        #qor=[{'origin':'CMB', 'destination':'DXB'}]
        #query={}
        #query['$or']=qor
        #cursor = db.JUP_DB_Data_Competitor_Ratings_Test1.find(query)
        cursor = db.JUP_DB_Data_Competitor_Ratings.find()
        #cursor = db.JUP_DB_Data_Competitor_Ratings_Test1.find({'origin':'CMB', 'destination':'DXB'})
        print 'ccc', cursor.count()
        if cursor.count() == 0:
            print 'came here'
            obj_err = error_class.ErrorObject(error_class.ErrorObject.WARNING, get_module_name(), get_arg_lists(inspect.currentframe()))
            obj_err.append_to_error_list("no records found in JUP_DB_Data_Competitor_Ratings collection")
            obj_err_main.append_to_error_object_list(obj_err)
            raise obj_err_main
        nrecs=0
        for c in cursor:
            compartment     = c['compartment']
            if compartment=='NA' or compartment=='N/A' or compartment=='na' or compartment=='n/a':
                continue
            try:
                pos         = c['pos']

            except:
                print 'a1', 'pos blank'
                pos=""
            if pos==None:
                pos=""
            origin          = c['origin']
            destination     = c['destination']
            group           = c['group']
            component       = c['component']

            nrecs=0
            try:
                nrecs+=1
                ratings=c['ratings']
            except:
                print c
                print 'ddd', nrecs
                obj_err = error_class.ErrorObject(error_class.ErrorObject.WARNING, get_module_name(), get_arg_lists(inspect.currentframe()))
                obj_err.append_to_error_list("no records found in JUP_DB_Data_Competitor_Weights collection")
                obj_err_main.append_to_error_object_list(obj_err)
                raise obj_err_main
            if origin!=None and destination!=None:  ##origin and destination are not wildcarded - filter out ratings that are not in the available od-compartment list
                try:
                    available_ratings_markets_airlines[compartment][origin][destination]
                except:
                    #print available_ratings_markets_airlines
                    #print 'reached not available'
                    continue
            try:
                dict_all_ratings[compartment]
            except:
                dict_all_ratings[compartment]={}
            try:
                dict_all_ratings[compartment][origin]
            except:
                dict_all_ratings[compartment][origin]={}
            try:
                dict_all_ratings[compartment][origin][destination]
            except:
                dict_all_ratings[compartment][origin][destination]={}
            try:
                dict_all_ratings[compartment][origin][destination][pos]
            except:
                dict_all_ratings[compartment][origin][destination][pos]={}
            try:
                dict_all_ratings[compartment][origin][destination][pos][group]
            except:
                dict_all_ratings[compartment][origin][destination][pos][group]={}
            try:
                dict_all_ratings[compartment][origin][destination][pos][group][component]
                obj_err = error_class.ErrorObject(error_class.ErrorObject.WARNING, get_module_name(), get_arg_lists(inspect.currentframe()))
                obj_err.append_to_error_list("compartment=" + compartment + ", pos=" + pos + ", od=" + od + ", group = " + group + ", component=" + component + ": duplicate component rating detected, current rating ignored")
                obj_err_main.append_to_error_object_list(obj_err)
                continue
            except:
                dict_all_ratings[compartment][origin][destination][pos][group][component]={}
            if origin!=None and destination!=None:
                new_ratings=available_ratings_markets_airlines[compartment][origin][destination][pos]
                out_ratings={}
                for airline in new_ratings: ## if airline in competitor ratings as well as in available_ratings_markets_airlines, then include in final ratings
                    try:
                        ratings[airline]
                        out_ratings[airline]=c['ratings'][airline]
                    except:
                        out_ratings[airline]=IMPUTED_RATING
            else: ## if origin or destination is wild carded, include all airlines
                out_ratings=c['ratings']
            dict_all_ratings[compartment][origin][destination][pos][group][component]['ratings']=out_ratings

            try:
                dict_all_ratings2[compartment]
            except:
                dict_all_ratings2[compartment]={}
            try:
                dict_all_ratings2[compartment][origin]
            except:
                dict_all_ratings2[compartment][origin]={}
            try:
                dict_all_ratings2[compartment][origin][destination]
            except:
                dict_all_ratings2[compartment][origin][destination]={}
            try:
                dict_all_ratings2[compartment][origin][destination][pos]
            except:
                dict_all_ratings2[compartment][origin][destination][pos]={}
            nrecs+=1
        return dict_all_ratings2


    @measure(JUPITER_LOGGER)
    def get_partitioned_od_compartments(dict_od_compartments, n_od_compartments):
        j=0
        list_dicts=[]
        dict_partition={}
        k=0
        for compartment in dict_od_compartments:
            for origin in dict_od_compartments[compartment]:
                for destination in dict_od_compartments[compartment][origin]:
                    #print 'came here'
                    try:
                        dict_partition[compartment]
                    except:
                        dict_partition[compartment]={}
                    try:
                        dict_partition[compartment][origin]
                    except:
                        dict_partition[compartment][origin]={}
                    try:
                        dict_partition[compartment][origin][destination]
                    except:
                        dict_partition[compartment][origin][destination]={}
                    j+=1
                    k+=1
                    for airline in dict_od_compartments[compartment][origin][destination]:
                        dict_partition[compartment][origin][destination][airline]=1.0
                    if j>n_od_compartments:
                        list_dicts.append(dict_partition)
                        dict_partition={}
                        j=0
        print "partition has " + str(k) + "odcompartments"
        list_dicts.append(dict_partition)
        return list_dicts


    @measure(JUPITER_LOGGER)
    def build_weight_rec(compartment, origin, destination, pos, dict_all_weights):
    #this has every compartment/origin/destination group/component normalized weights
    #the dict weight records cannot have a None argument for origin or destination
    # but the dict_weights_records picks up hte weights for None origin even when non-None origin is passed as argument!
        #print 'p20', compartment, origin, destination
        weightrecs=[]
        #print 'p1a', compartment, origin, destination
        if origin is not None:
            if destination is not None:
                dict_weight_records = get_weight_level_for_od_compartment(dict_all_weights, origin, destination, compartment)
                if dict_weight_records != None:
                    for group in dict_weight_records:
                        if group=='error_status':
                            continue
                        for component in dict_weight_records[group]:
                            if component=='error_status' or component=='group_weight' or component=='normalized_group_weight':
                                continue
                            comp_weight=dict_weight_records[group][component]['normalized_component_weight']
                            if group=='Airline Rating' and component=='Brand Image':  #Brand Image not yet implemented
                                continue
                            if group=='Product Rating' and component=='Value Adds':  #temporatrily notimplemented
                                continue
                            if group=='Airline Rating' and component=='Corporate':
                                component='corporate'
                            if group=='Airline Rating' and component=='Ground handling':
                                component='ground'
                            if group=='Airline Rating' and component=='Reliability/ baggage, on-time':
                                component='baggage'
                            if group=='Airline Rating' and component=='Safety':
                                component='safety'
                            if group=='Airline Rating' and component=='Service Recovery':
                                component='service'
                            if group=='Airline Rating' and component=='Skytrax rating for Airline':
                                component='skytrax airline'
                            if group=='Airline Rating' and component=='Skytrax rating for Hub airport':
                                component='skytrax airport'
                            if group=='Capacity/Schedule' and component=='Type of Aircrafts':
                                continue
                            if group=='Capacity/Schedule' and component=='On Time Performance':
                                continue
                            if group=='Capacity/Schedule' and component=='Block Time':
                                component='blocktime'
                            if group=='Capacity/Schedule' and component=='Connecting time (convenience)':
                                continue
                            if group=='Market Rating' and component=='Growth of Market':
                                component='Growth of market'
                            if group=='Market Rating' and component=='No: of Competitors':
                                component='No: of competitors'
                            if group=='Market Rating' and component=='Currency Export Restrictions':
                                component='Currency export restrictions'
                            if group=='Market Rating' and component=='Countertrade':
                                component='Countertrade'
                            weightrec=[]
                            weightrec.append(group)
                            weightrec.append(component)
                            weightrec.append(compartment)
                            weightrec.append(origin)
                            weightrec.append(destination)
                            weightrec.append(pos)
                            weightrec.append(dict_weight_records[group]['normalized_group_weight'])
                            weightrec.append(comp_weight)
                            weightrecs.append(weightrec)
        s=  sorted(weightrecs, key = lambda x: (x[0], x[1]))
        #for weightrec in weightrecs:
        #    print 'p1', weightrec[0], weightrec[1], weightrec[2], 'origin', weightrec[3], 'dest', weightrec[4], 'pos',weightrec[5], weightrec[6], weightrec[7]
        return s


    @measure(JUPITER_LOGGER)
    def build_rating_rec(compartment, origin, destination, pos, dict_all_ratings):
        #this has only those ratings from JUP_DB_Data_Competitors
        ratingrecs=[]
        for group in dict_all_ratings[compartment][origin][destination][pos]:
            if group=='error_status':
                continue
            if group=='Product Rating' or group=='Airline Rating':
                continue
            for component in dict_all_ratings[compartment][origin][destination][pos][group]:
                if component=='error_status' or component=='group_weight' or component=='normalized_group_weight':
                    continue
                ratingrec=[]
                ratingrec.append(group)
                ratingrec.append(component)
                ratingrec.append(compartment)
                ratingrec.append(origin)
                ratingrec.append(destination)
                ratingrec.append(pos)
                ratingrec.append({})
                last_field_num=len(ratingrec)
                for airline in dict_all_ratings[compartment][origin][destination][pos][group][component]['ratings']:
                    ratingrec[last_field_num-1][airline]=dict_all_ratings[compartment][origin][destination][pos][group][component]['ratings'][airline]
                ratingrecs.append(ratingrec)
        s=  sorted(ratingrecs, key = lambda x: (x[0], x[1])) #sorted by group/component
        #for rec in ratingrecs:
        #    print 'p16', rec[2], rec[3], rec[4], rec[5], rec[0], rec[1], rec[6]
        print ' '
        #print 'p2', s
        return s


    @measure(JUPITER_LOGGER)
    def read_nonos_rating_recs(compartment, origin, destination, pos, dict_all_ratings):
        #read in ratings for on-os records from dict_all_ratings
        #for component in dict_all_ratings[compartment][None][None]:
        #    print 'p52', compartment, dict_all_ratings[compartment][None][None]

        ratingrecs=[]
        pos=''
        #print 'p61', dict_all_ratings[compartment][None][None][pos]
        if dict_all_ratings!=None:
          if compartment != "Others":

              for group in dict_all_ratings[compartment][None][None][pos]:
                #print 'p53', group
                if group=='error_status':
                    continue
                if group!='Product Rating' and group!='Airline Rating':
                    continue
                for component in dict_all_ratings[compartment][None][None][pos][group]:
                    if component=='error_status' or component=='group_weight' or component=='normalized_group_weight':
                        continue
                    #print 'p54', group, component
                    ratingrec=[]
                    ratingrec.append(group)
                    ratingrec.append(component)
                    ratingrec.append(compartment)
                    ratingrec.append(origin)
                    ratingrec.append(destination)
                    ratingrec.append(pos)
                    ratingrec.append({})
                    last_field_num=len(ratingrec)
                    ratings=ratingrec[last_field_num-1]
            ##            for airline in dict_all_ratings[compartment][None][None][pos][group][component]['ratings']:
        ##                ratingrec.append({airline: dict_all_ratings[compartment][None][None][pos][group][component]['ratings'][airline]})
                    for airline in dict_all_ratings[compartment][None][None][pos][group][component]['ratings']:
                        ratingrec[last_field_num-1][airline]=dict_all_ratings[compartment][None][None][pos][group][component]['ratings'][airline]
                    #print 'p58', ratingrec[0], ratingrec[1], ratingrec[2], ratingrec[3], ratingrec[4], ratingrec[5]
                    ratingrecs.append(ratingrec)
                    #print 'p59', ratingrecs
        s=  sorted(ratingrecs, key = lambda x: (x[0], x[1])) #sorted by group/component
        #print 'p57', s
        #for rec in ratingrecs:
        #    print 'p51a', rec
        #for rec in ratingrecs:
        #    print 'p51', rec[2], rec[3], rec[4], rec[5], rec[0], rec[1], rec[6]
        #print ' '
        #print 'p2', s
        return s


    @measure(JUPITER_LOGGER)
    def introduce_rating_recs_into_weight_recs(compartment, origin, destination, pos, weight_recs, rating_recs, dict_all_ratings):
        #introduce rating recs info into weight recs when they are not already there
        weight_recs_copy=deepcopy(weight_recs)
     #   print 'p34', weight_recs_copy
    ##    for i in weight_recs_copy:
    ##        print 'p33', i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7]
        for rec in rating_recs:
            group=rec[0]
            component=rec[1]
            weight_rec_num=find_rating_rec_in_weight_rec(group, component, weight_recs)
            if weight_rec_num==None:
                continue
    #        print 'p18', weight_rec_num
    ##        weight_recs_copy[weight_rec_num][0]=group
    ##        weight_recs_copy[weight_rec_num][1]=component
    ##        weight_recs_copy[weight_rec_num][2]=compartment
    ##        weight_recs_copy[weight_rec_num][3]=origin
    ##        weight_recs_copy[weight_rec_num][4]=destination
            weight_recs_copy[weight_rec_num].append({})
            last_field_num=len(weight_recs_copy[weight_rec_num])
            for airline in dict_all_ratings[compartment][origin][destination][pos][group][component]['ratings']:
                #print 'p31', airline
                weight_recs_copy[weight_rec_num][last_field_num-1][airline]=dict_all_ratings[compartment][origin][destination][pos][group][component]['ratings'][airline]
                #print 'p31a', weight_recs_copy[weight_rec_num]
        #print 'p35', weight_recs_copy
        #print 'p36'
        #for i in weight_recs_copy:
        #    print 'p37', i
        #for i in weight_recs_copy:
        #    print 'p32', i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7]
        return weight_recs_copy


    @measure(JUPITER_LOGGER)
    def is_dict(x):
        #print 'x43', type(x) is dict
        return type(x) is type(dict())


    @measure(JUPITER_LOGGER)
    def replace_non_os_weight_recs_with_non_os_airline_ratings(non_os_rating_recs, weight_recs):
        #non-os-rating rec
    ##            ratingrec.append(group)
    ##            ratingrec.append(component)
    ##            ratingrec.append(compartment)
    ##            ratingrec.append(origin)
    ##            ratingrec.append(destination)
    ##            ratingrec.append(pos)
    ##            ratingrec.append({})
    ##            last_field_num=len(ratingrec)
        #weight-rec
    ##                        weightrec.append(group)
    ##                        weightrec.append(component)
    ##                        weightrec.append(compartment)
    ##                        weightrec.append(origin)
    ##                        weightrec.append(destination)
    ##                        weightrec.append(pos)
    ##                        weightrec.append(dict_weight_records[group]['normalized_group_weight'])
    ##                        weightrec.append(dict_weight_records[group][component]['normalized_component_weight'])
    ##                        ratings
        for rec in non_os_rating_recs:
            last_field_num=len(rec) #contains ratings fields
            ratings=rec[last_field_num-1]
            if not is_dict(ratings):
                continue
            weight_rec_num=find_rating_rec_in_weight_rec(rec[0], rec[1], weight_recs)
            if weight_rec_num==None:
                continue
            weight_recs[weight_rec_num].append(ratings)
        #for i in weight_recs:
        #    print 'p68', i
        return weight_recs


    @measure(JUPITER_LOGGER)
    def introduce_airline_ratings_into_rating_recs_with_weight_recs(weight_recs):
        #for os type records, try if airline rating is present in any other group/component record. If present, leave it as it is, otherwise set ratings to 5.
        weight_recs2=deepcopy(weight_recs)
        for rec in weight_recs:
            if rec[0]=='Product Rating' or rec[0]=='Airline Rating':
                continue
            #print 'p40d', rec[1]
            last_field_num=len(rec) #contains ratings fields
            ratings=rec[last_field_num-1]
            if is_dict(ratings):
                for airline in ratings:
                    for rec2 in weight_recs2:
                        if rec2[0]=='Product Rating' or rec2[0]=='Airline Rating':
                            continue
                        last_field_num2=len(rec2)
                        ratings2=rec2[last_field_num2-1]
                        if is_dict(ratings2):
                            try:
                                ratings2[airline]
                            except:
                                rec2[last_field_num2-1][airline]=5
                        else:
                            rec2.append({airline: 5})
        #for i in weight_recs2:
            #print 'p38', i
        return weight_recs2


    ##set_product_airline_indicator(weight_recs):
    ##    #This function sets an indicator in product/airline rating records to 0 if a record is not found in od-based groups, and to 0 otherwise
    ##    for rec in weight_recs:
    ##        last_field_num=len(rec) #contains ratings fields
    ##        ratings=rec[last_field_num-1]
    ##        if rec[0] == 'Product Rating' or rec[0] == 'Airline Rating':
    ##            continue
    ##        for airline in ratings:
    ##            for rec2 in weight_recs:
    ##                if rec[0] != 'Product Rating' and rec[0] != 'Airline Rating':
    ##                    continue
    ##                ratings2=rec2[last_field_num-1]
    ##                try:a
    ##                    ratings2[airline]
    ##                    rec2.append(1)
    ##                except:
    ##                    rec2.append(0)


    @measure(JUPITER_LOGGER)
    def exclude_nonos_airline_list(weight_recs):
        #for each non-os type recod, for each airline, blacklist those airlines that are not found in any type (os or non-os) record
        exclude_airlines={}
        for rec in weight_recs:
            if rec[0] != 'Product Rating' and rec[0]!='Airline Rating': #proceed only if non-os type record
                continue
            last_field_num=len(rec) #contains ratings fields
            ratings=rec[last_field_num-1]
            if not is_dict(ratings):
                continue
            for airline in ratings:
                for rec2 in weight_recs:
                    #if rec2[0] == 'Product Rating' or rec2[0]=='Airline Rating':
                    #    continue
                    last_field_num2=len(rec2)
                    ratings2=rec2[last_field_num2-1]
                    if not is_dict(ratings2):
                        exclude_airlines[(rec[0], rec[1], airline)]=1
                        #if airline=='EK':
                        #print 'p68ab', ratings2
                        #print 'p68ac', rec2
                        #print 'p68aa', airline, rec2[0], rec2[1], airline, exclude_airlines[(rec[0],rec[1], airline)]
                        continue
                    try:
                        ratings2[airline]
                    except:
                        exclude_airlines[(rec[0], rec[1], airline)]=1
                        #if airline=='EK':
                        #print 'p68d', airline, rec[0], rec[1], exclude_airlines[(rec[0], rec[1], airline)]
        return exclude_airlines


    @measure(JUPITER_LOGGER)
    def exclude_os_airline_list(weight_recs):
    ##                        weightrec.append(group)
    ##                        weightrec.append(component)
    ##                        weightrec.append(compartment)
    ##                        weightrec.append(origin)
    ##                        weightrec.append(destination)
    ##                        weightrec.append(pos)
    ##                        weightrec.append(dict_weight_records[group]['normalized_group_weight'])
    ##                        weightrec.append(dict_weight_records[group][component]['normalized_component_weight'])
    ##                        ratings
        #for each os type recod, for each airline, blacklist those airlines that are not found in any non-os type
        exclude_airlines={}
        for rec in weight_recs:
            if rec[0] == 'Product Rating' or rec[0]=='Airline Rating': #proceed only if os type record
                continue
            last_field_num=len(rec) #contains ratings fields
            ratings=rec[last_field_num-1]
            if not is_dict(ratings):
                continue
            for airline in ratings:
                #if airline=='KU' and rec[2]=='Y' and rec[3]=='CMB' and rec[4]=='DXB' and rec[5]=='UAE':
                #    print 'p69c', airline, rec[0], rec[1], rec[2], rec[3], rec[4], rec[5]
                for rec2 in weight_recs:
                    if rec2[0] != 'Product Rating' and rec2[0]!='Airline Rating': #proceed only if os type records
                        continue
                    last_field_num2=len(rec2)
                    ratings2=rec2[last_field_num2-1]
                    #print 'p69f', ratings2
                    #print 'p69g', rec2
                    if not is_dict(ratings2):
                        exclude_airlines[(rec[0], rec[1], airline)]=1
                        #if airline=='EK': #and rec[2]=='Y' and rec[3]=='CMB' and rec[4]=='DXB' and rec[5]=='UAE':
                        #print 'p69c', airline, rec[0], rec[1], rec[2], rec[3], rec[4], rec[5], exclude_airlines[(rec[0], rec[1], airline)]
                        continue
                    try:
                        ratings2[airline]
                    except:
                        exclude_airlines[(rec[0], rec[1], airline)]=1
                        #if airline=='EK': #and rec[2]=='Y' and rec[3]=='CMB' and rec[4]=='DXB' and rec[5]=='UAE':
                        #print 'p69d', airline, rec[0], rec[1], rec[2], rec[3], rec[4], rec[5], exclude_airlines[(rec[0], rec[1], airline)]
        return exclude_airlines


    ##def exclude_os_airline_list(weight_recs):
    ##    #for each non-os type recod, for each airline, black those airlines that are not found in any os-type record
    ##    exclude_airlines={}
    ##    for rec in weight_recs:
    ##        if rec[0] == 'Product Rating' or rec[0]=='Airline Rating':
    ##            continue
    ##        last_field_num=len(rec) #contains ratings fields
    ##        ratings=rec[last_field_num-1]
    ##        for airline in ratings:
    ##            for rec2 in weight_recs:
    ##                if rec2[0] != 'Product Rating' and rec2[0]!='Airline Rating':
    ##                    continue
    ##                ratings2=rec[last_field_num-1]
    ##                try:
    ##                    ratings2[airline]
    ##                except:
    ##                    exclude_airlines[(rec[0], rec[1])]=airline
    ##    for rec in weight_recs:
    ##        if rec[0] == 'Product Rating' or rec[0]=='Airline Rating':
    ##            continue
    ##        print 'p39', exclude_airlines[(rec[0], rec[1])]
    ##    return exclude_airlines


    @measure(JUPITER_LOGGER)
    def find_rating_rec_in_weight_rec(group, component, weight_recs):
        #print 'p20', weight_recs
        for i in range(len(weight_recs)):
            rec=weight_recs[i]
            #print 'p19', rec
            if group==rec[0] and component==rec[1]:
                return i
        return None


    ##                                         group in dict_weight_records:
    ##                    if group=='error_status':
    ##                        continue
    ##                    for component in dict_weight_records[group]:
    ##                        if component=='error_status' or component=='group_weight' or component=='normalized_group_weight':
    ##                            continue
    ##                        weightrec=[]
    ##                        weightrec.append(group)
    ##                        weightrec.append(component)
    ##                        f,
    ##                        weightrec.append(dict_weight_records[group]['normalized_group_weight'])
    ##                        weightrec.append(dict_weight_records[group][component]['normalized_component_weight']
    ##                        weightrecs.append(weightrec)
    ##    return  sorted(weightedrecs, key = lambda x: (x[0], x[1]))
    ##    #For each compartment/origin/destination/pos, if group/component record in found in dict_weight_records but not in dict_ratings, copy these group/components over
    ##    #if there no record in dict_ratings but in dict_weight_records, insert group/component into dict_ratings
    ##    for compartment in dict_ratings:
    ##        for origin in dict_ratings[compartment]:
    ##            for destination in dict_ratings[compartment][origin]:
    ##                if origin is None or destination is None:
    ##                    continue
    ##                dict_weight_records = get_weight_level_for_od_compartment(dict_all_weights, origin, destination, compartment)
    ##                for pos in dict_ratings[compartment][origin][destination]:
    ##                    for group in dict_weight_records:
    ##                        if group=='error_status':
    ##                            continue
    ##                        try:
    ##                            dict_ratings[compartment][origin][destination][pos][group]
    ##                        except:
    ##                            pass
    ##                            #dict_ratings[compartment][origin][destination][pos][group]={}
    ##                        for component in dict_weight_records[group]:
    ##                            if component=='error_status' or component=='group_weight' or component=='normalized_group_weight':
    ##                                continue
    ##                            #print 'x64', group, component
    ##                            try:
    ##                                    dict_ratings[compartment][origin][destination][pos][group]={}
    ##                            except:
    ##                                pass
    ##                            try:
    ##                                dict_ratings[compartment][origin][destination][pos][group][component]
    ##                                print 'x67', dict_ratings[compartment][origin][destination][pos][group][component]
    ##                                dict_ratings[compartment][origin][destination][pos][group][component]
    ##                            except:
    ##                                print 'x65', 'found in weight but not dict_rating', compartment, origin, pos, group, component
    ##                                dict_ratings[compartment][origin][destination][pos][group][component]={}
    ##                            dict_ratings[compartment][origin][destination][pos][group][component]['normalized_group_weight']=dict_weight_records[group]['normalized_group_weight']
    ##                            dict_ratings[compartment][origin][destination][pos][group][component]['normalized_component_weight']=dict_weight_records[group][component]['normalized_component_weight']
    ##
    ##    for compartment in dict_ratings:
    ##        for origin in dict_ratings[compartment]:
    ##            for destination in dict_ratings[compartment][origin]:
    ##                for pos in dict_ratings[compartment][origin][destination]:
    ##                    for group in dict_ratings[compartment][origin][destination][pos]:
    ##                        for component in dict_ratings[compartment][origin][destination][pos][group]:
    ##                                print 'x18a', compartment, origin, destination, pos, group, component, dict_ratings[compartment][origin][destination][pos][group][component]

    def calculate_ratings_for_pos(compartment, origin, destination, pos, dict_all_ratings, dict_all_weights):
        #print 'p21', compartment, origin, destination, pos
        weight_recs=build_weight_rec(compartment, origin, destination, pos, dict_all_weights)
        rating_recs=build_rating_rec(compartment, origin, destination, pos, dict_all_ratings)
        non_os_rating_recs=read_nonos_rating_recs(compartment, origin, destination, pos, dict_all_ratings)
        weight_recs=introduce_rating_recs_into_weight_recs(compartment, origin, destination, pos, weight_recs, rating_recs, dict_all_ratings)
        weight_recs=introduce_airline_ratings_into_rating_recs_with_weight_recs(weight_recs)
        replace_non_os_weight_recs_with_non_os_airline_ratings(non_os_rating_recs, weight_recs)
        exclude_nonos_airlines = exclude_nonos_airline_list(weight_recs)
        exclude_os_airlines = exclude_os_airline_list(weight_recs)
        ratings=calculate_ratings2(weight_recs, exclude_nonos_airlines, exclude_os_airlines, compartment, origin, destination, pos)
        #print 'p72', ratings
        update_collection(compartment, origin, destination, pos, ratings)


    @measure(JUPITER_LOGGER)
    def calculate_ratings2(weight_recs, exclude_nonos_airlines, exclude_os_airlines, compartment, origin, destination, pos):
    ##                        weightrec.append(group)
    ##                        weightrec.append(component)
    ##                        weightrec.append(compartment)
    ##                        weightrec.append(origin)
    ##                        weightrec.append(destination)
    ##                        weightrec.append(pos)
    ##                        weightrec.append(dict_weight_records[group]['normalized_group_weight'])
    ##                        weightrec.append(dict_weight_records[group][component]['normalized_component_weight'])
    ##                        ratings
        contributions={}
        for rec in weight_recs:
            last_field_num=len(rec) #contains ratings fields
            ratings=rec[last_field_num-1]
            if not is_dict(ratings):
                continue
            for airline in ratings:
                try:
                    exclude_nonos_airlines[(rec[0], rec[1], airline)]
                    exclude1=1
                except:
                    exclude1=0
                try:
                    exclude_os_airlines[(rec[0], rec[1], airline)]
                    exclude2=2
                except:
                    exclude2=0
                #if pos=='UAE':
                #    if airline == 'EY' or airline=='EK' or airline=='WY':
                #        print 'p71', ',', compartment, ',', origin, ',', destination, ',', pos, ',', rec[0], ',', rec[1], ',', rec[6], ',', rec[7], ',', airline, ',', ratings[airline], ',', exclude1, ',', exclude2
                try:
                    exclude_nonos_airlines[(rec[0], rec[1], airline)]
                    continue
                except:
                    exclude=0
                try:
                    exclude_os_airlines[(rec[0], rec[1], airline)]
                    continue
                except:
                    exclude=0
                try:
                    contributions[airline]
                except:
                    contributions[airline]=0.0
                contributions[airline] += rec[6]*rec[7]*ratings[airline]
        return contributions


    @measure(JUPITER_LOGGER)
    def calculate_ratings(dict_all_ratings2, dict_all_weights, dict_od_compartments):
        dict_all_ratings=deepcopy(dict_all_ratings2)
    ##    for compartment in dict_all_ratings:
    ##        print 'p70', 'compartment', compartment
    ##        for origin in dict_all_ratings[compartment]:
    ##            for destination in dict_all_ratings[compartment][origin]:
    ##                for pos in dict_all_ratings[compartment][origin][destination]:
    ##                    for group in dict_all_ratings[compartment][origin][destination][pos]:
    ##                        for component in dict_all_ratings[compartment][origin][destination][pos][group]:
    ##                            pass
    ##                        #print 'p23', compartment, origin, destination, pos, group, component
    ##
    ##    dict_ratings={}
    ##    jj=0
    ##    jk=0
    ##    # copy non-null origin/destination from dict_all_ratings into dict_ratings
    ##
    ##    for compartment in dict_all_ratings:
    ##        for origin in dict_all_ratings[compartment]:
    ##            pass
    ##        #print 'p12', compartment, origin


        #ratings=calculate_ratings_for_pos('Y', 'CMB', 'DXB', 'BTS', dict_all_ratings, dict_all_weights)
        for compartment in dict_all_ratings:
            for origin in dict_all_ratings[compartment]:
                for destination in dict_all_ratings[compartment][origin]:
                    for pos in dict_all_ratings[compartment][origin][destination]:
                        #for group in dict_all_ratings[compartment][origin][destination][pos]:
                         #   for component in dict_all_ratings[compartment][origin][destination][pos][group]:
                        pass
                        print 'p27', compartment, origin, destination, pos
                        ratings=calculate_ratings_for_pos(compartment, origin, destination, pos, dict_all_ratings, dict_all_weights)
        print 'p72', 'came here'
        #ratings=calculate_ratings_for_pos('Y', 'CMB','DXB', 'RUH', dict_all_ratings, dict_all_weights)
        #ratings=calculate_ratings_for_pos('Y', 'CMB','DXB', 'UAE', dict_all_ratings, dict_all_weights)
        #ratings=calculate_ratings_for_pos('Y', 'CMB','DXB', 'CMB', dict_all_ratings, dict_all_weights)
        #ratings=calculate_ratings_for_pos('Y', 'CMB','DXB', 'BTS', dict_all_ratings, dict_all_weights)
        #ratings=calculate_ratings_for_pos('J', 'CMB','DXB', '', dict_all_ratings, dict_all_weights)

    ##    for compartment in dict_all_ratings:
    ##        for origin in dict_all_ratings[compartment]:
    ##            for destination in dict_all_ratings[compartment][origin]:
    ##                for pos in dict_all_ratings[compartment][origin][destination]:
    ##                    print 'p26', compartment, origin, destination, pos
    ##                    #ratings=calculate_ratings_for_pos(compartment, origin, destination, pos, dict_all_ratings, dict_all_weights)
    ##
    ##    for compartment in dict_all_ratings:
    ##        for origin in dict_all_ratings[compartment]:
    ##            if origin==None or origin=='NA' or origin=='N/A' or origin=='na' or origin=='n/a':
    ##                continue
    ##            for destination in dict_all_ratings[compartment][origin]:
    ##                if destination==None or destination=='NA' or destination=='N/A' or destination=='na' or destination=='n/a':
    ##                    continue
    ##                for pos in dict_all_ratings[compartment][origin][destination]:
    ##                    try:
    ##                        dict_ratings[compartment]
    ##                    except:
    ##                        dict_ratings[compartment]=dict()
    ##                    try:
    ##                        dict_ratings[compartment][origin]
    ##                    except:
    ##                        dict_ratings[compartment][origin]={}
    ##                    try:
    ##                        dict_ratings[compartment][origin][destination]
    ##                    except:
    ##                        dict_ratings[compartment][origin][destination]=dict()
    ##                    try:
    ##                        dict_ratings[compartment][origin][destination][pos1]
    ##                    except:
    ##                        dict_ratings[compartment][origin][destination][pos]=dict_all_ratings[compartment][origin][destination][pos]
    ##
    ##    for compartment in dict_ratings:
    ##        for origin in dict_ratings[compartment]:
    ##            for destination in dict_ratings[compartment][origin]:
    ##                for pos in dict_ratings[compartment][origin][destination]:
    ##                    for group in dict_ratings[compartment][origin][destination][pos]:
    ##                        for component in dict_ratings[compartment][origin][destination][pos][group]:
    ##                                pass
    ##                            #print 'x17', compartment, origin, destination, pos, group, component , dict_ratings[compartment][origin][destination][pos][group][component]
    ##    # copy null origin/destination from dict_all_ratings into dict_ratings
    ##    for compartment in dict_all_ratings:
    ##        for origin in dict_all_ratings[compartment]:
    ##            if origin is not None:
    ##                continue
    ##            for destination in dict_all_ratings[compartment][origin]:
    ##                if destination is not None:
    ##                    continue
    ##                for pos in dict_all_ratings[compartment][origin][destination]:
    ##                    try:
    ##                        dict_ratings[compartment]
    ##                    except:
    ##                        continue
    ##                    try:
    ##                        dict_ratings[compartment][origin]
    ##                    except:
    ##                        dict_ratings[compartment][origin]={}
    ##                    try:
    ##                        dict_ratings[compartment][origin][destination]
    ##                    except:
    ##                        dict_ratings[compartment][origin][destination]=dict()
    ##                    try:
    ##                        dict_ratings[compartment][origin][destination][pos]
    ##                    except:
    ##
    ##                        dict_ratings[compartment][origin][destination][pos]=dict_all_ratings[compartment][origin][destination][pos]
    ##
    ##    for compartment in dict_ratings:
    ##        for origin in dict_ratings[compartment]:
    ##            for destination in dict_ratings[compartment][origin]:
    ##                for pos in dict_ratings[compartment][origin][destination]:
    ##                    for group in dict_ratings[compartment][origin][destination][pos]:
    ##                        for component in dict_ratings[compartment][origin][destination][pos][group]:
    ##                                pass
    ##                            #print 'x18', compartment, origin, destination, pos, group, component , dict_ratings[compartment][origin][destination][pos][group][component]
    ##
    ### at this pointdict_ratings has all all records from dict_all_ratings - where dict_all_ratings is from JUP_DB_Data_Competitor_Ratings
    ##
    ####    #print 'x11a', dict_ratings[compartment][origin][destination][pos]
    ####    print 'x11', dict_ratings
    ####    for compartment in dict_ratings:
    ####        print 'cmp', compartment
    ####        for origin in dict_ratings[compartment]:
    ####            for destination in dict_ratings[compartment][origin]:
    ####                for pos in dict_ratings[compartment][origin][destination]:
    ####                    #print 'x18', dict_ratings[compartment][origin][destination]
    ####                    for group in dict_ratings[compartment][origin][destination][pos]:
    ####                        for component in dict_ratings[compartment][origin][destination][pos][group]:
    ####                            for ratings in dict_ratings[compartment][origin][destination][pos][group][component]:
    ####                                print 'x17', compartment, origin, destination, pos, group, component, ratings, dict_ratings[compartment][origin][destination][pos][group][component]
    ##
    ##
    ##    for compartment in dict_ratings:
    ##        for origin in dict_ratings[compartment]:
    ##            if origin is None:
    ##                continue
    ##            for destination in dict_ratings[compartment][origin]:
    ##                if destination is None:
    ##                    continue
    ##                dict_weight_records = get_weight_level_for_od_compartment(dict_all_weights, origin, destination, compartment)
    ##                for group in dict_weight_records:
    ##                    for component in dict_weight_records[group]:
    ##                        pass
    ##                    #print 'p10', compartment, origin, destination, group, component, dict_weight_records[group][component]
    ##
    ##    for compartment in dict_all_ratings:
    ##        for origin in dict_all_ratings[compartment]:
    ##            for destination in dict_all_ratings[compartment][origin]:
    ##                for pos in dict_all_ratings[compartment][origin][destination]:
    ##                    ratings=calculate_ratings_for_pos(compartment, origin, destination, pos, dict_all_ratings, dict_all_weights)
    ##
    ##                dict_weight_records = get_weight_level_for_od_compartment(dict_all_weights, origin, destination, compartment)
    ##                for group in dict_weight_records:
    ##                    if group=='error_status':
    ##                        continue
    ##                    for component in dict_weight_records[group]:
    ##                        if component=='error_status' or component=='group_weight' or component=='normalized_group_weight':
    ##                            continue
    ##                        #print group, component
    ##                        #print 'x63', dict_weight_records[group]['normalized_group_weight'], dict_weight_records[group][component]['normalized_component_weight']
    ##
    ##    #For each compartment/origin/destination/pos, if group/component record in found in dict_weight_records but not in dict_ratings, copy these group/components over
    ##    #if there no record in dict_ratings but in dict_weight_records, insert group/component into dict_ratings
    ##    for compartment in dict_ratings:
    ##        for origin in dict_ratings[compartment]:
    ##            for destination in dict_ratings[compartment][origin]:
    ##                if origin is None or destination is None:
    ##                    continue
    ##                dict_weight_records = get_weight_level_for_od_compartment(dict_all_weights, origin, destination, compartment)
    ##                print 'p5', origin, destination, compartment
    ##                print 'p5', dict_weight_records
    ##                for pos in dict_ratings[compartment][origin][destination]:
    ##                    for group in dict_weight_records:
    ##                        if group=='error_status':
    ##                            continue
    ##                        try:
    ##                            dict_ratings[compartment][origin][destination][pos][group]
    ##                        except:
    ##                            pass
    ##                            #dict_ratings[compartment][origin][destination][pos][group]={}
    ##                        for component in dict_weight_records[group]:
    ##                            if component=='error_status' or component=='group_weight' or component=='normalized_group_weight':
    ##                                continue
    ##                            #print 'x64', group, component
    ##                            try:
    ##                                    dict_ratings[compartment][origin][destination][pos][group]={}
    ##                            except:
    ##                                pass
    ##                            try:
    ##                                dict_ratings[compartment][origin][destination][pos][group][component]
    ##                                print 'x67', dict_ratings[compartment][origin][destination][pos][group][component]
    ##                                dict_ratings[compartment][origin][destination][pos][group][component]
    ##                            except:
    ##                                print 'x65', 'found in weight but not dict_rating', compartment, origin, pos, group, component
    ##                                dict_ratings[compartment][origin][destination][pos][group][component]={}
    ##                            dict_ratings[compartment][origin][destination][pos][group][component]['normalized_group_weight']=dict_weight_records[group]['normalized_group_weight']
    ##                            dict_ratings[compartment][origin][destination][pos][group][component]['normalized_component_weight']=dict_weight_records[group][component]['normalized_component_weight']
    ##
    ##    for compartment in dict_ratings:
    ##        for origin in dict_ratings[compartment]:
    ##            for destination in dict_ratings[compartment][origin]:
    ##                for pos in dict_ratings[compartment][origin][destination]:
    ##                    for group in dict_ratings[compartment][origin][destination][pos]:
    ##                        for component in dict_ratings[compartment][origin][destination][pos][group]:
    ##                                print 'x18a', compartment, origin, destination, pos, group, component, dict_ratings[compartment][origin][destination][pos][group][component]
    ##
    ##
    ##    print 'p8a'
    ##
    ####for each compartment/origin/destination/pos, copy each airline into every group
    ##    for compartment in dict_ratings:
    ##        for origin in dict_ratings[compartment]:
    ##            for destination in dict_ratings[compartment][origin]:
    ##                for pos in dict_ratings[compartment][origin][destination]:
    ##                    for group in dict_ratings[compartment][origin][destination][pos]:
    ##                        for component in dict_ratings[compartment][origin][destination][pos][group]:
    ##                            print 'x71',  dict_ratings[compartment][origin][destination][pos][group][component]
    ##                            try:
    ##                                dict_ratings[compartment][origin][destination][pos][group][component]['ratings']
    ##                            except:
    ##                                dict_ratings[compartment][origin][destination][pos][group][component]['ratings']={}
    ##
    ##
    ##    dict_ratings2=deepcopy(dict_ratings)
    ##    print 'p9'
    ##
    ##    for compartment in dict_ratings:
    ##        for origin in dict_ratings[compartment]:
    ##            for destination in dict_ratings[compartment][origin]:
    ##                for pos in dict_ratings[compartment][origin][destination]:
    ##                    for group in dict_ratings[compartment][origin][destination][pos]:
    ##                        if group=='error_status':
    ##                            continue
    ##                        ##print 'came hereh2'
    ##                        for component in dict_ratings[compartment][origin][destination][pos][group]:
    ##                            if component=='error_status' or component=='group_weight' or component=='normalized_group_weight':
    ##                                continue
    ##                            #print 'x72',  dict_ratings[compartment][origin][destination][pos][group][component]
    ##                            for group2 in dict_ratings2[compartment][origin][destination][pos]:
    ##                                #print 'x84', dict_ratings2[compartment][origin][destination][pos]
    ##                                if group2=='error_status':
    ##                                    continue
    ##                                for component2 in dict_ratings2[compartment][origin][destination][pos][group2]:
    ##                                    if component2=='error_status' or component2=='group_weight' or component2=='normalized_group_weight':
    ##                                        continue
    ##                                    print 'x70', dict_ratings2[compartment][origin][destination][pos][group2][component2]
    ##                                    try:
    ##                                        dict_ratings2[compartment][origin][destination][pos][group2][component2]['ratings']
    ##                                    except:
    ##                                        continue
    ##                                    print 'x93', compartment, origin, destination, pos, group2, component2, dict_ratings2[compartment][origin][destination][pos][group2][component2]
    ##                                    for airline in dict_ratings2[compartment][origin][destination][pos][group2][component2]['ratings']:
    ##                                        print 'x66', airline
    ##                                        try:
    ##                                            dict_ratings[compartment][origin][destination][pos][group][component]['ratings']
    ##                                        except:
    ##                                            dict_ratings[compartment][origin][destination][pos][group][component]['ratings']={}
    ##                                        try:
    ##                                            dict_ratings[compartment][origin][destination][pos][group][component]['ratings'][airline]
    ##                                        except:
    ##                                            print 'x86', 'came here'
    ##                                            try:
    ##                                                dict_ratings[compartment][origin][destination][pos][group][component]['ratings'][airline]=5.0
    ##                                            except:
    ##                                                print 'x85', 'fail'
    ##                                    print 'x80', compartment, origin, destination, pos, group, component, dict_ratings[compartment][origin][destination][pos][group][component]
    ##
    ##    for compartment in dict_ratings:
    ##        for origin in dict_ratings[compartment]:
    ##            for destination in dict_ratings[compartment][origin]:
    ##                for pos in dict_ratings[compartment][origin][destination]:
    ##                    for group in dict_ratings[compartment][origin][destination][pos]:
    ##                        for component in dict_ratings[compartment][origin][destination][pos][group]:
    ##                                print 'x19', compartment, origin, destination, pos, group, component, dict_ratings[compartment][origin][destination][pos][group][component]
    ##
    ##
    ##    for compartment in dict_ratings:
    ##        print 'x22', compartment
    ##        for origin in dict_ratings[compartment]:
    ##            for destination in dict_ratings[compartment][origin]:
    ##                if destination == None or origin==None:
    ##                    continue
    ##                dict_weight_records = get_weight_level_for_od_compartment(dict_all_weights, origin, destination, compartment)
    ##                for pos in dict_ratings[compartment][origin][destination]:
    ##                    for group in dict_weight_records:
    ##                        try:
    ##                            dict_ratings[compartment][origin][destination][pos][group]
    ##                        except:
    ##                            dict_ratings[compartment][origin][destination][pos][group]={}
    ##                        for component in dict_weight_records[group]:
    ##                            try:
    ##                                dict_ratings[compartment][origin][destination][pos][group][component]
    ##                            except:
    ##                                dict_ratings[compartment][origin][destination][pos][group][component]={}
    ##                                #dict_ratings[compartment][origin][destination][pos][group][component]['normailzed_group_weight']=dict_weight_records[group]['normalized_group_weight']
    ##                                #dict_ratings[compartment][origin][destination][pos][group][component]['normalized_component_weight']=dict_weight_records[group][component]['normalized_component_weight']
    ##
    ##
    ### At this point dict_ratings has both groups with od's as well as those without od's
    ### each group has its ratings associated with it
    ##
    ##
    ####                    dict_ratings[compartment][origin][destination][pos]['ratings']=dict()
    ####                    print 'n3', dict_od_compartments
    ####                    dict_ratings[compartment][origin][destination][pos]['ratings'].update(dict_od_compartments[compartment][origin][destination])
    ####                    print 'n4', dict_ratings[compartment][origin][destination][pos]['ratings']
    ####                    for airline in dict_ratings[compartment][origin][destination][pos]['ratings']:
    ####                        dict_ratings[compartment][origin][destination][pos]['ratings'][airline]=0.0
    ####                    ##try if weights exists at od level - if they do, return od_compartment level weights, otherwise return network level weights records
    ####                    ## returns:
    ####                    ##   dict_all_weights[compartment][region][country][od] - return all records at group/component level
    ####                    ##   dict_all_weights has the folllowing structure
    ####                    ##   dict_all_weights[compartment][region][country][od]['error_status']='OK' or "ERROR"
    ####                    ##   dict_all_weights[compartment][region][country][od][group]['group_weight']
    ####                    ##   dict_all_weights[compartment][region][country][od][group]['normalized_group_weight']
    ####                    ##   dict_all_weights[compartment][region][country][od][group][component]['component_weight']
    ####                    ##   dict_all_weights[compartment][region][country][od][group][component]['normalized_component_weight']
    ####                    ##   dict_all_weights[compartment][region][country][od][group][component]['implemented_flag']
    ####                    ##   dict_all_weights[compartment][region][country][od][group][component]['rating_definition_level']
    ####                    dict_weight_records = get_weight_level_for_od_compartment(dict_all_weights, origin, destination, compartment)
    ####                    print 'n6', dict_weight_records
    ####                    for group in dict_weight_records:
    ####                        print 'n7', group
    ####                    for group in dict_weight_records:
    ####                        if group=='error_status':
    ####                            continue
    ####                        for component in dict_weight_records[group]:
    ####                            if component=='group_weight' or component=='normalized_group_weight':
    ####                                continue
    ####                            dict_ratings_records=get_ratings_records(dict_all_weights, origin, destination, pos, compartment, group, component, dict_all_ratings)
    ####                            if dict_ratings_records==None:
    ####                                ## ratings records not found, assumning imputed rating for all airlines
    ####                                for airline in dict_od_compartments[compartment][origin][destination]:
    ####                                    airline_component_rating=IMPUTED_RATING
    ####                                    x = dict_weight_records[group]['normalized_group_weight'] * dict_weight_records[group][component]['normalized_component_weight'] * airline_component_rating
    ####                                    #print group, '\t', component, '\t', dict_weight_records[group]['normalized_group_weight'], '\t', dict_weight_records[group][component]['normalized_component_weight'], '\t', airline_component_rating, '\t', x
    ####                                    #print '0', dict_ratings[compartment][origin][destination]['ratings'][airline], dict_weight_records[group]['normalized_group_weight'], dict_weight_records[group][component]['normalized_component_weight'], airline_component_rating, compartment, origin, destination, group, component
    ####                                    dict_ratings[compartment][origin][destination][pos]['ratings'][airline] += x
    ####                                    #print '1', dict_ratings[compartment][origin][destination]['ratings'][airline], dict_weight_records[group]['normalized_group_weight'], dict_weight_records[group][component]['normalized_component_weight'], airline_component_rating, compartment, origin, destination, group, component
    ####                            else:
    ####                                ratings=dict_ratings_records['ratings']
    ####                                for airline in dict_od_compartments[compartment][origin][destination]:
    ####                                    try:
    ####                                        airline_component_rating=ratings[airline]
    ####                                    except:
    ####                                        airline_component_rating=IMPUTED_RATING
    ####                                    x = dict_weight_records[group]['normalized_group_weight'] * dict_weight_records[group][component]['normalized_component_weight'] * airline_component_rating
    ####                                    #print '0', dict_ratings[compartment][origin][destination]['ratings'][airline], dict_weight_records[group]['normalized_group_weight'], dict_weight_records[group][component]['normalized_component_weight'], airline_component_rating, compartment, origin, destination, group, component
    ####                                    #print group, '\t', component, '\t', dict_weight_records[group]['normalized_group_weight'], '\t', dict_weight_records[group][component]['normalized_component_weight'], '\t', airline_component_rating, '\t', x
    ####                                    dict_ratings[compartment][origin][destination][pos]['ratings'][airline] += x
    ####                                    #print '1', dict_ratings[compartment][origin][destination]['ratings'][airline], dict_weight_records[group]['normalized_group_weight'], dict_weight_records[group][component]['normalized_component_weight'], airline_component_rating, compartment, origin, destination, group, component
    ####                            jj+=1
    ####                            jk+=1
    ####                            if jj>1000:
    ####                                jj=0
    ######    j=0
    ######    for compartment in dict_ratings:
    ######        if j>1:
    ######            break
    ######        for origin in dict_ratings[compartment]:
    ######            if j>1:
    ######                break
    ######            for destination in dict_ratings[compartment][origin]:
    ######                if j>1:
    ######                    break
    ######                for airline in dict_ratings[compartment][origin][destination]['ratings']:
    ######                    j+=1
    ######                    print dict_ratings
    ######                    if j>1:
    ######                        break
    ##    return dict_ratings
    ##
    ##
    ####    dict_ratings={}
    ####    jj=0
    ####    jk=0
    ####    print 'd2', dict_all_ratings
    ####    #dict_all_ratings=copy.deepcopy(dict_all_ratings2)
    ####
    ####    dict_all_ratings2={}
    ####    for compartment in dict_all_ratings:
    ####        try:
    ####            dict_all_ratings2[compartment]
    ####        except:
    ####            dict_all_ratings2[compartment]={}
    ####        for origin in dict_all_ratings[compartment]:
    ####            if origin==None or origin=='NA' or origin=='N/A' or origin=='na' or origin=='n/a':
    ####                continue
    ####            try:
    ####                dict_all_ratings2[compartment][origin]
    ####            except:
    ####                dict_all_ratings2[compartment][origin]={}
    ####            for destination in dict_all_ratings[compartment][origin]:
    ####                try:
    ####                    dict_all_ratings2[compartment][origin][destination]
    ####                except:
    ####                    dict_all_ratings2[compartment][origin][destination]={}
    ######                for pos in dict_all_ratings2[compartment][origin][destination]:
    ######                    try:
    ######                        for pos in dict_all_ratings2[compartment][origin][destination][pos]:
    ######                    except:
    ######                        for pos in dict_all_ratings2[compartment][origin][destination][pos]={}
    ####
    ####    for compartment in dict_all_ratings:
    ####        for origin in dict_all_ratings[compartment]:
    ####            if origin==None or origin=='NA' or origin=='N/A' or origin=='na' or origin=='n/a':
    ####                continue
    ####            for destination in dict_all_ratings[compartment][origin]:
    ####                for pos in dict_all_ratings[compartment][origin][destination]:
    ####                    for group in dict_all_ratings[compartment][origin][destination][pos]:
    ####                        print 'x5', dict_all_ratings[compartment][origin][destination][pos][group]
    ####                        for component in dict_all_ratings[compartment][origin][destination][pos][group]:
    ####                            for airline in dict_all_ratings[compartment][origin][destination][pos][group][component]:
    ####                                dict_all_ratings2[compartment][origin][destination][pos][airline]=0.0
    ####    print 'x4', dict_all_ratings
    ####
    ####    for compartment in dict_all_ratings:
    ####        for origin in dict_all_ratings[compartment]:
    ####            if origin==None or origin=='NA' or origin=='N/A' or origin=='na' or origin=='n/a':
    ####                continue
    ####            for destination in dict_all_ratings[compartment][origin]:
    ####                dict_weight_records = get_weight_level_for_od_compartment(dict_all_weights, origin, destination, compartment)
    ####                for pos in dict_all_ratings[compartment][origin][destination]:
    ####                    print 'w1', dict_all_ratings
    ####                    for group in dict_all_ratings[compartment][origin][destination][pos]:
    ####                        for component in dict_all_ratings[compartment][origin][destination][pos][group]:
    ####                            print 'w2', dict_all_ratings[compartment][origin][destination][pos][group]
    ####                            #for component in dict_weight_records[group]:
    ####                            #if component=='group_weight' or component=='normalized_group_weight':
    ####                            #    continue
    ####                            dict_ratings_records=get_ratings_records(dict_all_weights, origin, destination, pos, compartment, group, component, dict_all_ratings)
    ####                            print 'x1', dict_ratings_records
    ####                            if dict_ratings_records==None:
    ####                                print 'n75', group
    ####                                print 'n9', dict_all_ratings[compartment][origin][destination][pos]
    ####                                for airline in dict_all_ratings[compartment][origin][destination][pos][group][component]:
    ####                                    airline_component_rating=IMPUTED_RATING
    ####                                    x = dict_weight_records[group]['normalized_group_weight'] * dict_weight_records[group][component]['normalized_component_weight'] * airline_component_rating
    ####                                    #print 'd7', dict_ratings
    ####                                    try:
    ####                                        dict_ratings[compartment][origin][destination][pos]['ratings'][airline]
    ####                                    except:
    ####                                        dict_ratings[compartment][origin][destination][pos]['ratings'][airline]=0.0
    ####                                    dict_ratings[compartment][origin][destination][pos]['ratings'][airline] += x
    ####                                    #print '1', dict_ratings[compartment][origin][destination]['ratings'][airline], dict_weight_records[group]['normalized_group_weight'], dict_weight_records[group][component]['normalized_component_weight'], airline_component_rating, compartment, origin, destination, group, component
    ####
    ####                            else:
    ####                                ratings=dict_ratings_records['ratings']
    ####                                print 'd5', ratings
    ####                                for airline in dict_all_ratings[compartment][origin][destination][pos][group][component]:
    ####                                    try:
    ####                                        airline_component_rating=ratings[airline]
    ####                                    except:
    ####                                        airline_component_rating=IMPUTED_RATING
    ####                                    print 'x2', dict_weight_records
    ####                                    x = dict_weight_records[group]['normalized_group_weight'] * dict_weight_records[group][component]['normalized_component_weight'] * airline_component_rating
    ####                                    #print '0', dict_ratings[compartment][origin][destination]['ratings'][airline], dict_weight_records[group]['normalized_group_weight'], dict_weight_records[group][component]['normalized_component_weight'], airline_component_rating, compartment, origin, destination, group, component
    ####                                    #print group, '\t', component, '\t', dict_weight_records[group]['normalized_group_weight'], '\t', dict_weight_records[group][component]['normalized_component_weight'], '\t', airline_component_rating, '\t', x
    ####                                    try:
    ####                                        dict_ratings[compartment][origin][destination][pos]['ratings'][airline]
    ####                                    except:
    ####                                        print 'v.5', dict_ratings
    ####                                        dict_ratings[compartment][origin][destination][pos]['ratings'][airline]=0.0
    ####                                    dict_ratings[compartment][origin][destination][pos]['ratings'][airline] += x
    ####                                    print 'v0', dict_ratings
    ####
    ####    print 'n8', dict_ratings
    ####
    ####    for compartment in dict_od_compartments:
    ####        for origin in dict_od_compartments[compartment]:
    ####            if origin==None or origin=='NA' or origin=='N/A' or origin=='na' or origin=='n/a':
    ####                continue
    ####            for destination in dict_od_compartments[compartment][origin]:
    ####                if destination==None or destination=='NA' or destination=='N/A' or destination=='na' or destination=='n/a':
    ####                    continue
    ####                try:
    ####                    dict_ratings[compartment]
    ####                except:
    ####                    dict_ratings[compartment]=dict()
    ####                try:
    ####                    dict_ratings[compartment][origin]
    ####                except:
    ####                    dict_ratings[compartment][origin]={}
    ####                try:
    ####                    dict_ratings[compartment][origin][destination]
    ####                except:
    ####                    dict_ratings[compartment][origin][destination]=dict()
    ####                for pos in dict_all_ratings[compartment][origin][destination]:
    ####                    try:
    ####                        dict_ratings[compartment][origin][destination][pos]
    ####                    except:
    ####                        dict_ratings[compartment][origin][destination][pos]={}
    ####                    dict_ratings[compartment][origin][destination][pos]['ratings']=dict()
    ####                    print 'd25', dict_ratings
    ####                    print 'd3', dict_all_ratings
    ####
    ####
    ######                    dict_ratings[compartment][origin][destination][pos]['ratings'].update(dict_all_ratings[compartment][origin][destination][pos]['ratings'])
    ######                    print 'v1', dict_ratings[compartment][origin][destination][pos]['ratings']
    ######                for airline in dict_ratings[compartment][origin][destination][pos]['ratings']:
    ######                    dict_ratings[compartment][origin][destination]['ratings'][airline]=0.0
    ####                ##try if weights exists at od level - if they do, return od_compartment level weights, otherwise return network level weights records
    ####                ## returns:
    ####                ##   dict_all_weights[compartment][region][country][od] - return all records at group/component level
    ####                ##   dict_all_weights has the folllowing structure
    ####                ##   dict_all_weights[compartment][region][country][od]['error_status']='OK' or "ERROR"
    ####                ##   dict_all_weights[compartment][region][country][od][group]['group_weight']
    ####                ##   dict_all_weights[compartment][region][country][od][group]['normalized_group_weight']
    ####                ##   dict_all_weights[compartment][region][country][od][group][component]['component_weight']
    ####                ##   dict_all_weights[compartment][region][country][od][group][component]['normalized_component_weight']
    ####                ##   dict_all_weights[compartment][region][country][od][group][component]['implemented_flag']
    ####                ##   dict_all_weights[compartment][region][country][od][group][component]['rating_definition_level']
    ####                dict_weight_records = get_weight_level_for_od_compartment(dict_all_weights, origin, destination, compartment)
    ####                #print dict_weight_records
    ####                if dict_weight_records != None:
    ####                    for group in dict_weight_records:
    ####                        #print 'group', group
    ####                        if group=='error_status':
    ####                            continue
    ####                        for component in dict_weight_records[group]:
    ####                            if component=='group_weight' or component=='normalized_group_weight':
    ####                                continue
    ####                            dict_ratings_records=get_ratings_records(dict_all_weights, origin, destination, pos, compartment, group, component, dict_all_ratings)
    ####                            if dict_ratings_records==None:
    ####                                ## ratings records not found, assumning imputed rating for all airlines
    ####                                for airline in dict_od_compartments[compartment][origin][destination]:
    ####                                    airline_component_rating=IMPUTED_RATING
    ####                                    x = dict_weight_records[group]['normalized_group_weight'] * dict_weight_records[group][component]['normalized_component_weight'] * airline_component_rating
    ####                                    #print group, '\t', component, '\t', dict_weight_records[group]['normalized_group_weight'], '\t', dict_weight_records[group][component]['normalized_component_weight'], '\t', airline_component_rating, '\t', x
    ####                                    #print '0', dict_ratings[compartment][origin][destination]['ratings'][airline], dict_weight_records[group]['normalized_group_weight'], dict_weight_records[group][component]['normalized_component_weight'], airline_component_rating, compartment, origin, destination, group, component
    ####                                    print 'd7', dict_ratings
    ####                                    try:
    ####                                        dict_ratings[compartment][origin][destination][pos]['ratings'][airline]
    ####                                    except:
    ####                                        dict_ratings[compartment][origin][destination][pos]['ratings'][airline]=0.0
    ####                                    dict_ratings[compartment][origin][destination][pos]['ratings'][airline] += x
    ####                                    #print '1', dict_ratings[compartment][origin][destination]['ratings'][airline], dict_weight_records[group]['normalized_group_weight'], dict_weight_records[group][component]['normalized_component_weight'], airline_component_rating, compartment, origin, destination, group, component
    ####                            else:
    ####                                ratings=dict_ratings_records['ratings']
    ####                                print 'd5', dict_ratings_records
    ####                                for airline in dict_od_compartments[compartment][origin][destination]:
    ####                                    try:
    ####                                        airline_component_rating=ratings[airline]
    ####                                    except:
    ####                                        airline_component_rating=IMPUTED_RATING
    ####                                    x = dict_weight_records[group]['normalized_group_weight'] * dict_weight_records[group][component]['normalized_component_weight'] * airline_component_rating
    ####                                    #print '0', dict_ratings[compartment][origin][destination]['ratings'][airline], dict_weight_records[group]['normalized_group_weight'], dict_weight_records[group][component]['normalized_component_weight'], airline_component_rating, compartment, origin, destination, group, component
    ####                                    #print group, '\t', component, '\t', dict_weight_records[group]['normalized_group_weight'], '\t', dict_weight_records[group][component]['normalized_component_weight'], '\t', airline_component_rating, '\t', x
    ####                                    try:
    ####                                        dict_ratings[compartment][origin][destination][pos]['ratings'][airline]
    ####                                    except:
    ####                                        dict_ratings[compartment][origin][destination][pos]['ratings'][airline]=0.0
    ####                                    dict_ratings[compartment][origin][destination][pos]['ratings'][airline] += x
    ####                                    print 'v0', dict_ratings
    ####                            jj+=1
    ####                            jk+=1
    ####                            if jj>1000:
    ####                                jj=0
    ######    j=0
    ######    for compartment in dict_ratings:
    ######        if j>1:
    ######            break
    ######        for origin in dict_ratings[compartment]:
    ######            if j>1:
    ######                break
    ######            for destination in dict_ratings[compartment][origin]:
    ######                if j>1:
    ######                    break
    ######                for airline in dict_ratings[compartment][origin][destination]['ratings']:
    ######                    j+=1
    ######                    print dict_ratings
    ######                    if j>1:
    ######                        break
    ####    return dict_ratings


    @measure(JUPITER_LOGGER)
    def calculate_group_ratings(dict_all_ratings, dict_all_weights, dict_od_compartments):
        pass
    ##    dict_ratings={}
    ##    for compartment in dict_od_compartments:
    ##        for origin in dict_od_compartments[compartment]:
    ##            if origin==None or origin=='NA' or origin=='N/A' or origin=='na' or origin=='n/a':
    ##                continue
    ##            for destination in dict_od_compartments[compartment][origin]:
    ##                if destination==None or destination=='NA' or destination=='N/A' or destination=='na' or destination=='n/a':
    ##                    continue
    ##                for pos in dict_all_ratings[compartment][origin][destination]:
    ##                    try:
    ##                        dict_ratings[compartment]
    ##                    except:
    ##                        dict_ratings[compartment]=dict()
    ##                    try:
    ##                        dict_ratings[compartment][origin]
    ##                    except:
    ##                        dict_ratings[compartment][origin]={}
    ##                    try:
    ##                        dict_ratings[compartment][origin][destination]
    ##                    except:
    ##                        dict_ratings[compartment][origin][destination]=dict()
    ##                    try:
    ##                        dict_ratings[compartment][origin][destination][pos]
    ##                    except:
    ##                        dict_ratings[compartment][origin][destination][pos]=dict()
    ##
    ##                    ##try if weights exists at od level - if they do, return od_compartment level weights, otherwise return network level weights records
    ##                    ## returns:
    ##                    ##   dict_all_weights[compartment][region][country][od] - return all records at group/component level
    ##                    ##   dict_all_weights has the folllowing structure
    ##                    ##   dict_all_weights[compartment][region][country][od]['error_status']='OK' or "ERROR"
    ##                    ##   dict_all_weights[compartment][region][country][od][group]['group_weight']
    ##                    ##   dict_all_weights[compartment][region][country][od][group]['normalized_group_weight']
    ##                    ##   dict_all_weights[compartment][region][country][od][group][component]['component_weight']
    ##                    ##   dict_all_weights[compartment][region][country][od][group][component]['normalized_component_weight']
    ##                    ##   dict_all_weights[compartment][region][country][od][group][component]['implemented_flag']
    ##                    ##   dict_all_weights[compartment][region][country][od][group][component]['rating_definition_level']
    ##                    dict_weight_records = get_weight_level_for_od_compartment(dict_all_weights, origin, destination, compartment)
    ##                    for group in dict_weight_records:
    ##                        if group=='error_status':
    ##                            continue
    ##                        try:
    ##                            dict_ratings[compartment][origin][destination][pos][group]
    ##                        except:
    ##                            dict_ratings[compartment][origin][destination][pos][group]=dict()
    ##                        dict_ratings[compartment][origin][destination][pos][group]['ratings']=dict()
    ##                        dict_ratings[compartment][origin][destination][pos][group]['ratings'].update(dict_od_compartments[compartment][origin][destination])
    ##                        print 'x3aa', dict_ratings
    ##                        for airline in dict_ratings[compartment][origin][destination][pos][group]['ratings']:
    ##                            dict_ratings[compartment][origin][destination][pos][group]['ratings'][airline]=0.0
    ##                        for component in dict_weight_records[group]:
    ##                            if component=='group_weight' or component=='normalized_group_weight':
    ##                                continue
    ##                            dict_ratings_records=get_ratings_records(dict_all_weights, origin, destination, pos, compartment, group, component, dict_all_ratings)
    ##                            if dict_ratings_records==None:
    ##                                ## ratings records not found, assumning imputed rating for all airlines
    ##                                for airline in dict_od_compartments[compartment][origin][destination]:
    ##                                    airline_component_rating=IMPUTED_RATING
    ##                                    x = dict_weight_records[group][component]['normalized_component_weight'] *airline_component_rating
    ##                                    dict_ratings[compartment][origin][destination][pos][group]['ratings'][airline] += x
    ##                                    dict_ratings[compartment][origin][destination][pos][group]['group_weight'] = dict_weight_records[group]['normalized_group_weight']
    ##    ##                                print dict_ratings[compartment][origin][destination][group]['group_weight'], dict_ratings[compartment][origin][destination][group]['ratings'][airline], dict_weight_records[group][component]['normalized_component_weight'], airline_component_rating, compartment, origin, destination, group, component
    ##                            else:
    ##                                ratings=dict_ratings_records['ratings']
    ##                                for airline in dict_od_compartments[compartment][origin][destination]:
    ##                                    try:
    ##                                        airline_component_rating=ratings[airline]
    ##                                    except:
    ##                                        airline_component_rating=IMPUTED_RATING
    ##                                    x = dict_weight_records[group][component]['normalized_component_weight'] *airline_component_rating
    ##                                    dict_ratings[compartment][origin][destination][pos][group]['ratings'][airline] += x
    ##                                    dict_ratings[compartment][origin][destination][pos][group]['group_weight'] = dict_weight_records[group]['normalized_group_weight']
    ##    ##                                print dict_ratings[compartment][origin][destination][group]['group_weight'], dict_ratings[compartment][origin][destination][group]['ratings'][airline], dict_weight_records[group][component]['normalized_component_weight'], airline_component_rating, compartment, origin, destination, group, component
    ##    ##                    print dict_ratings[compartment][origin][destination]
    ##    ##    print 'dd2', dict_ratings, dict_ratings[compartment][origin][destination][group]['ratings']['FZ']
    ##    return dict_ratings
    ##
    ####    dict_ratings={}
    ####    for compartment in dict_od_compartments:
    ####        for origin in dict_od_compartments[compartment]:
    ####            if origin==None or origin=='NA' or origin=='N/A' or origin=='na' or origin=='n/a':
    ####                continue
    ####            for destination in dict_od_compartments[compartment][origin]:
    ####                if destination==None or destination=='NA' or destination=='N/A' or destination=='na' or destination=='n/a':
    ####                    continue
    ######    dict_airlines={'FZ':0}
    ######    for compartment in ['Y']:
    ######        for origin in ['CPH']:
    ######            if origin==None or origin=='NA' or origin=='N/A' or origin=='na' or origin=='n/a':
    ######                continue
    ######            for destination in ['BKK']:
    ######                if destination==None or destination=='NA' or destination=='N/A' or destination=='na' or destination=='n/a':
    ######                    continue
    ####                try:
    ####                    dict_ratings[compartment]
    ####                except:
    ####                    dict_ratings[compartment]=dict()
    ####                try:
    ####                    dict_ratings[compartment][origin]
    ####                except:
    ####                    dict_ratings[compartment][origin]={}
    ####                try:
    ####                    dict_ratings[compartment][origin][destination]
    ####                except:
    ####                    dict_ratings[compartment][origin][destination]=dict()
    ####                for pos in dict_all_ratings[compartment][origin][destination]:
    ####                    try:
    ####                        dict_ratings[compartment][origin][destination][pos]
    ####                    except:
    ####                        dict_ratings[compartment][origin][destination][pos]={}
    ####                    dict_ratings[compartment][origin][destination][pos]['ratings']=dict()
    ####                   # dict_ratings[compartment][origin][destination][pos]['ratings'].update(dict_all_ratings[compartment][origin][destination][pos]['ratings'])
    ####                    print 'v1', dict_ratings[compartment][origin][destination][pos]['ratings']
    ####
    ####                ##try if weights exists at od level - if they do, return od_compartment level weights, otherwise return network level weights records
    ####                ## returns:
    ####                ##   dict_all_weights[compartment][region][country][od] - return all records at group/component level
    ####                ##   dict_all_weights has the folllowing structure
    ####                ##   dict_all_weights[compartment][region][country][od]['error_status']='OK' or "ERROR"
    ####                ##   dict_all_weights[compartment][region][country][od][group]['group_weight']
    ####                ##   dict_all_weights[compartment][region][country][od][group]['normalized_group_weight']
    ####                ##   dict_all_weights[compartment][region][country][od][group][component]['component_weight']
    ####                ##   dict_all_weights[compartment][region][country][od][group][component]['normalized_component_weight']
    ####                ##   dict_all_weights[compartment][region][country][od][group][component]['implemented_flag']
    ####                ##   dict_all_weights[compartment][region][country][od][group][component]['rating_definition_level']
    ####                dict_weight_records = get_weight_level_for_od_compartment(dict_all_weights, origin, destination, compartment)
    ####                if dict_weight_records != None:
    ####                    for group in dict_weight_records:
    ####                        if group=='error_status':
    ####                            continue
    ####                        try:
    ####                            dict_ratings[compartment][origin][destination][pos][group]
    ####                        except:
    ####                            dict_ratings[compartment][origin][destination][pos][group]=dict()
    ####                        dict_ratings[compartment][origin][destination][pos][group]['ratings']=dict()
    ####                        #dict_ratings[compartment][origin][destination][pos][group]['ratings'].update(dict_od_compartments[compartment][origin][destination])
    ####                        #for airline in dict_ratings[compartment][origin][destination][pos][group]['ratings']:
    ####                        #    dict_ratings[compartment][origin][destination][group]['ratings'][airline]=0.0
    ####                        for component in dict_weight_records[group]:
    ####                            if component=='group_weight' or component=='normalized_group_weight':
    ####                                continue
    ####                            dict_ratings_records=get_ratings_records(dict_all_weights, origin, destination, pos, compartment, group, component, dict_all_ratings)
    ####                            if dict_ratings_records==None:
    ####                                ## ratings records not found, assumning imputed rating for all airlines
    ####                                for airline in dict_od_compartments[compartment][origin][destination]:
    ####                                    for pos in dict_all_ratings[compartment][origin][destination]:
    ####                                        airline_component_rating=IMPUTED_RATING
    ####                                    x = dict_weight_records[group][component]['normalized_component_weight'] *airline_component_rating
    ####                                    try:
    ####                                        dict_ratings[compartment][origin][destination][pos]['ratings'][airline]
    ####                                    except:
    ####                                        dict_ratings[compartment][origin][destination][pos]['ratings'][airline]=0.0
    ####                                    print 'v2', dict_ratings #[compartment][origin][destination][pos]
    ####                                    dict_ratings[compartment][origin][destination][pos][group]['ratings'][airline] += x
    ####                                    dict_ratings[compartment][origin][destination][pos][group]['group_weight'] = dict_weight_records[group]['normalized_group_weight']
    ####    ##                                print dict_ratings[compartment][origin][destination][group]['group_weight'], dict_ratings[compartment][origin][destination][group]['ratings'][airline], dict_weight_records[group][component]['normalized_component_weight'], airline_component_rating, compartment, origin, destination, group, component
    ####                            else:
    ####                                ratings=dict_ratings_records['ratings']
    ####                                for airline in dict_od_compartments[compartment][origin][destination]:
    ####                                    try:
    ####                                        airline_component_rating=ratings[airline]
    ####                                    except:
    ####                                        airline_component_rating=IMPUTED_RATING
    ####                                    x = dict_weight_records[group][component]['normalized_component_weight'] *airline_component_rating
    ####                                    try:
    ####                                        dict_ratings[compartment][origin][destination][pos]['ratings'][airline]
    ####                                    except:
    ####                                        dict_ratings[compartment][origin][destination][pos]['ratings'][airline]=0.0
    ####                                    try:
    ####                                        dict_ratings[compartment][origin][destination][pos]['ratings'][airline]
    ####                                    except:
    ####                                        dict_ratings[compartment][origin][destination][pos]['ratings'][airline]=0.0
    ####                                    dict_ratings[compartment][origin][destination][pos][group]['ratings'][airline] += x
    ####                                    dict_ratings[compartment][origin][destination][pos][group]['group_weight'] = dict_weight_records[group]['normalized_group_weight']
    ######                                print dict_ratings[compartment][origin][destination][group]['group_weight'], dict_ratings[compartment][origin][destination][group]['ratings'][airline], dict_weight_records[group][component]['normalized_component_weight'], airline_component_rating, compartment, origin, destination, group, component
    ######                    print dict_ratings[compartment][origin][destination]
    ######    print 'dd2', dict_ratings, dict_ratings[compartment][origin][destination][group]['ratings']['FZ']
    ####    return dict_ratings
    ##
    ##
    ####   dict_all_ratings[compartment][origin][destination][group][component]['ratings'] (a dictionary of airlines and their ratings)
    ####   dict_all_weights[compartment][region][country][od]
    ##    ##   dict_all_weights[compartment][region][country][od]['error_status']='OK' or "ERROR"
    ##    ##   dict_all_weights[compartment][region][country][od][group]['group_weight']
    ##    ##   dict_all_weights[compartment][region][country][od][group]['normalized_group_weight']
    ##    ##   dict_all_weights[compartment][region][country][od][group][component]['component_weight']
    ##    ##   dict_all_weights[compartment][region][country][od][group][component]['normalized_component_weight']
    ##    ##   dict_all_weights[compartment][region][country][od][group][component]['implemented_flag']
    ##    ##   dict_all_weights[compartment][region][country][od][group][component]['rating_definition_level']
    ##    ##   dict_all_weights[compartment][region][country][od]['error_status']='OK'
    ##
    ##    ##   dict_all_ratings[compartment][origin][destination][group][component]['ratings'] (a dictionary of airlines and their ratings)


    @measure(JUPITER_LOGGER)
    def get_ratings_records(dict_all_weights, origin, destination, pos, compartment, group, component, dict_all_ratings):
        rating_definition_level=dict_all_weights[compartment][None][None][None][group][component]['rating_definition_level']   ## the 3 "None"s are for region, country and od
        if rating_definition_level=="network": ## if rating definition level is od_compartment, keep o and d as it is
            origin=None
            destination=None
        try:
            ratings=dict_all_ratings[compartment][origin][destination][pos][group][component]   ##return entire group/component structure
            return ratings
        except:
            return None


    @measure(JUPITER_LOGGER)
    def get_weight_level_for_od_compartment(dict_all_weights, origin, destination, compartment):
        ## dict_weights[compartment][region][country][od][group][component]['component_weight']=c['weight']
        ##try if weights exists at od level
        region=None
        country=None
        od=origin+destination
        try:
            dict_all_weights[compartment][region][country][od]
            ##check=check_groups_and_components(dict_all_weights[compartment][region][country][od], dict_all_groups_and_components)
            check=dict_all_weights[compartment][region][country][od]['error_status']
            print 'or1', origin, destination, compartment,dict_all_weights[compartment][region][country][od]
            if check=='OK':
                return dict_all_weights[compartment][region][country][od]
        except:
            pass
    ##    ## try if weights exist at country level
    ##    region=None
    ##    country=get_country(origin)
    ##    od=None
    ##    try:
    ##        dict_all_weights[compartment][region][country][od]
    ##        ##check=check_groups_and_components(dict_all_weights[compartment][region][country][od], dict_all_groups_and_components)
    ##        check=dict_all_weights[compartment][region][country][od]['error_status']
    ##        if check=='OK':
    ##            return dict_all_weights[compartment][region][country][od]
    ##    except:
    ##        pass
    ##    ## try if weights exist at region level
    ##    region=get_region(origin)
    ##    country=None
    ##    od=None
    ##    try:
    ##        dict_all_weights[compartment][region][country][od]
    ##        ##check=check_groups_and_components(dict_all_weights[compartment][region][country][od], dict_all_groups_and_components)
    ##        check=dict_all_weights[compartment][region][country][od]['error_status']
    ##        if check=='OK':
    ##            return dict_all_weights[compartment][region][country][od]
    ##    except:
    ##        pass
        ## network level
        region=None
        country=None
        od=None
        try:
            dict_all_weights[compartment][region][country][od]
            check=dict_all_weights[compartment][region][country][od]['error_status']
            if check=='OK':
                #print 'checn', dict_all_weights[compartment][region][country][od]
                return dict_all_weights[compartment][region][country][od]
        except:
            obj_err = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1, get_module_name(), get_arg_lists(inspect.currentframe()))
            obj_err.append_to_error_list("".join(["compartment=", compartment, "origin=", origin, "destination=", destination, ": weights records not found in JUP_DB_Competitor_Weights collection"]))
            obj_err_main.append_to_error_object_list(obj_err)
            print 'came here none'
            return None


    @measure(JUPITER_LOGGER)
    def update_collection2(dict_scores, dict_all_ratings2, dict_group_scores, dict_od_compartments_partition):
        """
        delete existing records and then insert the new records into JUP_DB_Data_Competitor_Ratings
        """
        print 'update collection reached'
        j=0
        for compartment in dict_od_compartments_partition:
            for origin in dict_od_compartments_partition[compartment]:
                for destination in dict_od_compartments_partition[compartment][origin]:
                    j+=1
        print 'recs to be written', j
        list_replace=[]
        for compartment in dict_od_compartments_partition:
            for origin in dict_od_compartments_partition[compartment]:
                if origin==None:
                    continue
                for destination in dict_od_compartments_partition[compartment][origin]:
                    if destination==None:
                        continue
                    dict_replace={}
                    dict_replace['compartment']=compartment
                    dict_replace['origin']=origin
                    dict_replace['destination']=destination
                    try:
                        for origin1 in dict_all_ratings2[compartment][origin][destination]:
                            org=origin1
                        #print 'rr3', org
                    except:
                        origin1=""
                        print 'a2', 'bad origin1'
                    dict_replace['pos']=origin1
                    #print 'rr5', origin1
                    list_replace.append(dict_replace)
        process_query(dict_scores, dict_group_scores, origin1, list_replace)


    @measure(JUPITER_LOGGER)
    def update_collection(compartment, origin, destination, pos, ratings):
        dict_rec={}
        dict_rec['compartment']=compartment
        dict_rec['origin']=origin
        dict_rec['destination']=destination
        if origin != None and destination != None:
            dict_rec['od'] = origin+destination
        else:
            dict_rec['od'] = origin
            print "------------",dict_rec['od']
        dict_rec['pos']=pos
        dict_rec['ratings']=ratings
        dict_rec['_id']=ObjectId()
        dict_rec['last_update_date_gmt'] = "{:%Y-%m-%d}".format(datetime.utcnow())
        db.JUP_DB_Promos.insert(dict_rec)


    @measure(JUPITER_LOGGER)
    def process_query(ratings, compartment, origin, destination, pos):
        #cursor=db.JUP_DB_Competitor_Ratings2.find({'$or':list_replace})
        #print 'recs_to be deleted:', cursor.count()

        for dict_replace in list_replace:
            dict_rec = {}
            compartment=dict_replace['compartment']
            origin=dict_replace['origin']
            destination=dict_replace['destination']
            dict_rec['compartment']=compartment
            dict_rec['origin']=origin
            dict_rec['pos']=dict_replace['pos']
            #print 'rr6', dict_replace['pos']
            if origin != None and destination != None:
                dict_rec['od'] = origin + destination
            dict_rec['destination']=destination
            dict_rec['ratings']=dict_scores[compartment][origin][destination]['ratings']

            dict_rec['last_update_date_gmt'] = "{:%Y-%m-%d}".format(datetime.utcnow())
            for group in dict_group_scores[compartment][origin][destination]:
                if group=='ratings' or group=='group_weight' or group=='error_status':
                    continue
                dict_rec[group+'_ratings']=dict_group_scores[compartment][origin][destination][group]['ratings']
                dict_rec[group+'_weight']=dict_group_scores[compartment][origin][destination][group]['group_weight']
            dict_rec['_id']=ObjectId()
            #print 'dict_rec', dict_rec
            db.JUP_DB_Promos.insert(dict_rec)


    @measure(JUPITER_LOGGER)
    def process_query2(dict_scores, dict_group_scores, origin1, list_replace):
        #cursor=db.JUP_DB_Competitor_Ratings2.find({'$or':list_replace})
        #print 'recs_to be deleted:', cursor.count()

        for dict_replace in list_replace:
            dict_rec = {}
            compartment=dict_replace['compartment']
            origin=dict_replace['origin']
            destination=dict_replace['destination']
            dict_rec['compartment']=compartment
            dict_rec['origin']=origin
            dict_rec['pos']=dict_replace['pos']
            #print 'rr6', dict_replace['pos']
            dict_rec['destination']=destination
            if origin != None and destination != None:
                dict_rec['od'] = origin + destination
            dict_rec['ratings']=dict_scores[compartment][origin][destination]['ratings']

            dict_rec['last_update_date_gmt'] = "{:%Y-%m-%d}".format(datetime.utcnow())
            for group in dict_group_scores[compartment][origin][destination]:
                if group=='ratings' or group=='group_weight' or group=='error_status':
                    continue
                dict_rec[group+'_ratings']=dict_group_scores[compartment][origin][destination][group]['ratings']
                dict_rec[group+'_weight']=dict_group_scores[compartment][origin][destination][group]['group_weight']
            dict_rec['_id']=ObjectId()
            #print 'dict_rec', dict_rec
            db.JUP_DB_Promos.insert(dict_rec)

    ##def process_query(dict_scores, dict_group_scores, compartment, origin, destination):
    ##    query={'compartment': compartment, 'origin':origin, 'destination': destination}
    ##    cursor=db.JUP_DB_Competitor_Ratings.find(query)
    ##    if cursor.count() > 1:
    ##        print 'error', compartment, origin, destination
    ##    old_ratings=None
    ##    for c in cursor: ## there is expected to be only one record as result of query
    ##        old_ratings=c['ratings']
    ##    #print compartment, origin,destination
    ##    new_ratings=dict_scores[compartment][origin][destination]['ratings']
    ##
    ##    db.JUP_DB_Competitor_Ratings2.remove(query)
    ##
    ##    for airline in new_ratings:
    ##        new_doc={'airline':airline, 'origin': origin, 'destination':destination, 'compartment':compartment, 'rating':new_ratings[airline]}
    ##        try:
    ##            old_rating=old_ratings[airline]
    ##        except:
    ##            old_rating=None
    ##        old_doc={'airline':airline, 'origin': origin, 'destination':destination, 'compartment':compartment, 'rating':old_ratings}
    ##        #call_trigger(old_doc, new_doc)
    ##
    ##
    ##    dict_rec = {}
    ##    dict_rec['compartment']=compartment
    ##    dict_rec['origin']=origin
    ##    dict_rec['destination']=destination
    ##    dict_rec['ratings']=new_ratings
    ##    dict_rec['last_update_date_gmt'] = "{:%Y-%m-%d}".format(datetime.utcnow())
    ##    for group in dict_group_scores[compartment][origin][destination]:
    ##        if group=='ratings' or group=='group_weight' or group=='error_status':
    ##            continue
    ##        dict_rec[group+'_ratings']=dict_group_scores[compartment][origin][destination][group]['ratings']
    ##        dict_rec[group+'_weight']=dict_group_scores[compartment][origin][destination][group]['group_weight']
    ##    dict_rec['_id']=ObjectId()
    ##    db.JUP_DB_Competitor_Ratings2.insert(dict_rec)

    ##from jupiter_AI.triggers.data_change.rating.competitor import HostRatingChange
    ##from jupiter_AI.triggers.data_change.rating.competitor import CompRatingChange


    @measure(JUPITER_LOGGER)
    def call_trigger(old_doc, new_doc):
        if old_doc[airline]==net.Host_Airline_Code:
            name='host_rating_change'
            obj = HostRatingChange(name = name, old_database_doc = old_doc, new_database_doc = new_doc)
            obj.do_analysis()
        else:
            name='competitor_rating_change'
            obj = CompRatingChange(name = name, old_database_doc = old_doc, new_database_doc = new_doc)
            obj.do_analysis()


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


    ##def get_compartments_from_ratings(dict_all_ratings):
    ##    dict_compartments={}
    ##    for compartment in dict_all_ratings:
    ##        if compartment=='NA' or compartment=='na' or compartment=='N/A' or compartment=='n/a':
    ##            continue
    ##        dict_compartments[compartment]=0.0
    ##    return dict_compartments
    ##
    ##
    ##def get_airlines_from_ratings(dict_all_ratings):
    ##    dict_airlines2={}
    ##    dict_airlines={}
    ##    for compartment in dict_all_ratings:
    ##        for origin in dict_all_ratings[compartment]:
    ##            for destination in dict_all_ratings[compartment][origin]:
    ##                for group in dict_all_ratings[compartment][origin][destination]:
    ##                    for component in dict_all_ratings[compartment][origin][destination][group]:
    ##                        dict_airlines2.update(dict_all_ratings[compartment][origin][destination][group][component]['ratings'])
    ##    for airline in dict_airlines2:
    ##        if airline=='NA' or airline=='na' or airline=='N/A' or airline=='n/a':
    ##            continue
    ##        dict_airlines[airline]=0.0
    ##    return dict_airlines
    ##
    ##
    ##def check_hierarchical_weights(dict_all_weights):
    ##    for compartment in dict_all_weights:
    ##        for region in dict_all_weights[compartment]:
    ##            for country in dict_all_weights[compartment][region]:
    ##                for od in dict_all_weights[compartment][region][country]:
    ##                    if region==None and country==None and od==None:     ## network level record
    ##                        continue
    ##                    for group in dict_all_weights[compartment][region][country][od]:
    ##                        if group=='error_status':
    ##                            continue
    ##                        for component in dict_all_weights[compartment][region][country][od][group]:
    ##                            if component=='group_weight' or component=='normalized_group_weight':
    ##                                continue
    ##                            #####@@@@@@@@@@@@@@@@@@@@@@
    ##    dict_weight_records = get_weight_level_for_od_compartment(dict_all_weights, 'JFK', 'AHB', 'Y')

    ##def get_groups_and_components(dict_all_weights):
    ##    dict_groups_and_components={}
    ##    for compartment in dict_all_weights:
    ##        try:
    ##            dict_groups_and_components[compartment]
    ##        except:
    ##            dict_groups_and_components[compartment]=dict()
    ##        for region in dict_all_weights[compartment]:
    ##            for country in dict_all_weights[compartment][region]:
    ##                for od in dict_all_weights[compartment][region][country]:
    ##                    for group in dict_all_weights[compartment][region][country][od]:
    ##                        if group=='error_status':
    ##                            continue
    ##                        try:
    ##                            dict_groups_and_components[compartment][group]
    ##                        except:
    ##                            dict_groups_and_components[compartment][group]=dict()
    ##                        for component in dict_all_weights[compartment][region][country][od][group]:
    ##                            if component=='group_weight' or component=='normalized_group_weight':
    ##                                continue
    ##                            try:
    ##                                dict_groups_and_components[compartment][group][component]
    ##                            except:
    ##                                dict_groups_and_components[compartment][group][component]=1
    ##    return dict_groups_and_components


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


    db.JUP_DB_Promos.remove({'compartment':"Others"})

if __name__ == '__main__':
    run()