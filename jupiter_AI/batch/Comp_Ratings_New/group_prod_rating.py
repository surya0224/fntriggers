"""
 Author: Prem Narasimhan
 Completed date:
     first version: Mar 3, 2017
 This program calculates ratings for the "Product Rating" group.

 Input collections:
     JUP_DB_Data_Product
 Output collection:
     JUP_DB_Data_Competitor_Ratings
 For testing:
     Remove one layer of comments at the end of the program to get a sampe run
 For live run:
     call main()
 Overall logic:
     1. Fix the list of airlines and compartments
     2. The components in group "Product Rating" (except for "Value Adds" component) are obtained from the collection JUP_DB_Data_Competitor_Weights collection, network level records
     3. Data for the components in step 2 are obtained from JUP_DB_Data_Product collection and updated into "dict_prod_rating_records"
     4. List of subcategories, and feature weights/ratings are compiled into a dictionary from "dict_prod_rating_records"
     5. subcategory weights are normalized so they add to 1.0
     6. feature weights are normalized so they add to 1.0 within each subcategory
     7. scores are computed for each compartment/category (a category is called "group" in JUP_DB_Competitor_Weights)
     8. from "dict_prod_rating_records" in step 3, list of "Value_added" are obtained for each feature
        A "Value_added" is an airline or airlines that are top rated in that feature, usually means a 10 while all others are 5 or 0.
     9. Get the "Value_added" frequency count for each airline/compartment
     10. The frequency counts are directly scaled(without ranking) so that they fall in the range 0 to 10 (min rank maps to 0, max rank maps to 10)
     11. Update JUP_DB_Data_Competitor_Ratings collection
         od anc compartment are set to None (because this group is at "network" level)
         one record for each compartment/airline level
         Records are introduced for all components in "Product Rating" group, including "Value Adds"
 Error handling:
     1. If the error level=WARNING, errors are collected in obj_err_main (defined at global level) but processing continues
     2. At the end of the program, the errors in obj_err_main are flushed to JUP_DB_Errors_All
"""
from jupiter_AI import JUPITER_LOGGER
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

    import scipy.stats as ss


    @measure(JUPITER_LOGGER)
    def main():
        """
        Obtain list of components (except "Value Adds") from JUP_DB_Data_Competitor
        Search for Product Rating components in network level records ('region':None, 'country':None, 'pos': None, 'od':None) in JUP_DB_Data_Competitor_Weights
        Returns:
            list of components (second level of he=ierarchy in JUP_DB_Data_Weights: group/component; the "component" is the same as "category" in JUP_DB_Data_Product
        """
        dict_product_rating_components=get_product_rating_components_from_Data_Competitor_Weights()
        print 'ddd', dict_product_rating_components
        """
        read in the data into dict_prod_rating_records from JUP_DB_Data_Product, corresponding to those components in dict_product_rating_components
        returns:
            csf (category/subcategory/feature in JUP_DB_Data_Product)
                csf[compartment][category][subcategory]['subcategory_weight']=1
                csf[compartment][category][subcategory][feature]['feature_weight']=1
                csf[compartment][category][subcategory][feature]['ratings']=c['ratings']
            dict_value_adds
                dict_value_adds[compartment][airline]
        """
        csf, dict_value_adds=read_in_data_from_Data_Product(dict_product_rating_components)

        compute_subcategory_weights(csf)

        compute_feature_weights(csf)

        """
        returns:
            dict_results in format dict_results[(compartment, category, airline)]
        """
        dict_results=compute_category_scores(csf)

        """
        dict_transformed_results in format dict_transformed_results[compartment][category][airline]
        """
        dict_transformed_results=transform_results(dict_results)

        """
        dict_value_adds[compartment]['dict_value_adds_rating']: the key ['dict_value_adds_rating'] is added to dict_value_adds by this function
        """
        get_value_adds_score(dict_value_adds)

        dict_airlines=get_airlines(csf)

        """
        this functions adds airlines (assigning a rating of 0.0) that have never been a value add in any feature
        this is done because we want such airlines to have a value add score of 0.0
        """
        introduce_zero_rating_airlines_into_value_adds(dict_value_adds, dict_airlines)

        update_Data_Competitor_Ratings_collection(dict_transformed_results, dict_value_adds)


    @measure(JUPITER_LOGGER)
    def introduce_zero_rating_airlines_into_value_adds(dict_value_adds, dict_airlines):
        for compartment in dict_value_adds:
            for airline in dict_airlines:
                if airline not in dict_value_adds[compartment]['dict_value_adds_rating']:
                    dict_value_adds[compartment]['dict_value_adds_rating'][airline]=0.0


    @measure(JUPITER_LOGGER)
    def transform_results(dict_results):
        dict_transformed_results=dict()
        for key in dict_results:
            compartment=key[0]
            category=key[1]
            airline=key[2]
            try:
                dict_transformed_results[compartment]
            except:
                dict_transformed_results[compartment]=dict()
            try:
                dict_transformed_results[compartment][category]
            except:
                dict_transformed_results[compartment][category]=dict()
            dict_transformed_results[compartment][category][airline]=dict_results[key]
        return dict_transformed_results


    @measure(JUPITER_LOGGER)
    def get_airlines(csf):
        dict_airlines={}
        for compartment in csf:
            for category in csf[compartment]:
                for subcategory in csf[compartment][category]:
                    for feature in csf[compartment][category][subcategory]:
                        if feature=='subcategory_weight':
                            continue
                        dict_airlines.update(csf[compartment][category][subcategory][feature]['ratings'])
        return dict_airlines


    @measure(JUPITER_LOGGER)
    def get_product_rating_components_from_Data_Competitor_Weights():
        """
        Obtain list of components (except "Value Adds") from JUP_DB_Data_Competitor
        Search for Product Rating components in network level records ('region':None, 'country':None, 'pos': None, 'od':None) in JUP_DB_Data_Competitor_Weights
        """
        dict_product_rating_components={}
        cursor = db.JUP_DB_Data_Competitor_Weights.find({'group':'Product Rating', 'region':None, 'country':None, 'pos': None, 'od':None })
        for c in cursor:
            group=c['group']
            component=c['component']
            if component == 'Value Adds':             ## treat 'Value Adds' separately
                continue
            if group==component:     ##group level record
                continue
            dict_product_rating_components[component]=1
        return dict_product_rating_components


    @measure(JUPITER_LOGGER)
    def get_ratings_tmp():
        return {
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
            "G9" : "5",
            "KQ" : "5",
            "CA" : "5",
            "MS" : "5",
            "SV" : "5",
            "UL" : "5",
            "RJ" : "5",
            "9W" : "5"
        }


    @measure(JUPITER_LOGGER)
    def read_in_data_from_Data_Product(dict_product_rating_components):
        """
        This method reads in data from JUP_DB_Data_Product, based on "dict_product_rating_components"
        Some records are short (as defined by MIN_RECORD_LENGTH), ignore them
        """
        category_build = []
        for component in dict_product_rating_components:
            print 'bbb', component
            category_build.append({'Category': component})  # a "component" in product rating group in JUP_DB_Data_Competitor_Weights is a "category" in JUP_DB_Data_Product

        query={}
        query['$or'] = category_build
        print query
        cursor = db.JUP_DB_Data_Product.find(query)
        print 'aaa', cursor.count(), 'ccc'
        csf=dict()
        dict_value_adds=dict()
        for c in cursor:
            category            = c['Category']
            subcategory         = c['Sub_Category']
            feature             = c['Features']
            if subcategory==feature:
                continue
            if category=="" or subcategory=="" or feature=="":
                continue
            dict_compartments={'F':1,'J':1,'Y':1}
            for compartment in dict_compartments:
                try:
                    csf[compartment]
                except:
                    csf[compartment]=dict()
                try:
                    csf[compartment][category]
                except:
                    csf[compartment][category]=dict()
                try:
                    csf[compartment][category][subcategory]
                except:
                    csf[compartment][category][subcategory]=dict()
                try:
                    csf[compartment][category][subcategory][feature]
                except:
                    csf[compartment][category][subcategory][feature]=dict()
                csf[compartment][category][subcategory]['subcategory_weight']=1
                csf[compartment][category][subcategory][feature]['feature_weight']=1
                csf[compartment][category][subcategory][feature]['ratings']={}
                for airline in get_ratings_tmp():
                    try:
                        csf[compartment][category][subcategory][feature]['ratings'][airline]=c[airline]
                    except:
                        pass
                try:
                    dict_value_adds[compartment]
                except:
                    dict_value_adds[compartment]={}
                try:
                    str_value_adds  = c['Value_added']
                except:
                    continue
                value_adds          = str_value_adds.split(',')

                for i in range(len(value_adds)):
                    airline=value_adds[i]
                    if airline=="":
                        continue
                    try:
                        dict_value_adds[compartment][airline] += 1
                    except:
                        dict_value_adds[compartment][airline] = 1
        return [csf, dict_value_adds]


    @measure(JUPITER_LOGGER)
    def compute_subcategory_weights(csf):
        """
        Converts the raw subcategory weights such that sum of normalized weights equals 1
        """
        for compartment in csf:
            for category in csf[compartment]:
                total_weight=0.0
                for subcategory in csf[compartment][category]:
                    total_weight += csf[compartment][category][subcategory]['subcategory_weight']
                for subcategory in csf[compartment][category]:
                    if abs(total_weight) < TOLERANCE:
                        obj_err = error_class.ErrorObject(error_class.ErrorObject.WARNING, get_module_name(), get_arg_lists(inspect.currentframe()))
                        obj_err.append_to_error_list("category="+category+ ": total raw category weight close to zero, setting normalized weight to zero for compartment" + compartment)
                        obj_err_main.append_to_error_object_list(obj_err)
                        csf[compartment][category][subcategory]['subcategory_weight'] = 0
                    else:
                        csf[compartment][category][subcategory]['subcategory_weight'] /= total_weight


    @measure(JUPITER_LOGGER)
    def compute_feature_weights(csf):
        """
        Converts the raw feature weights such that sum of normalized weights equals 1 within each subcategory
        """

        for compartment in csf:
            for category in csf[compartment]:
                for subcategory in csf[compartment][category]:
                    total_weight=0.0
                    for feature in csf[compartment][category][subcategory]:
                        if feature == 'subcategory_weight':      ## we are interested only in sucategoriess which are keys, not the key 'category_weight'
                            continue
                        total_weight += csf[compartment][category][subcategory][feature]['feature_weight']
                    for feature in csf[compartment][category][subcategory]:
                        if feature == 'subcategory_weight':      ## we are interested only in subcategories which are keys, not the key 'subcategory_weight'
                            continue
                        if abs(total_weight) < TOLERANCE:
                            obj_err = error_class.ErrorObject(error_class.ErrorObject.WARNING, get_module_name(), get_arg_lists(inspect.currentframe()))
                            obj_err.append_to_error_list("category="+category+ "subcategory=" + subcategory + ": total raw subcategory weight close to zero, setting normalized weight to zero for compartment=" + compartment)
                            obj_err_main.append_to_error_object_list(obj_err)
                            csf[compartment][category][subcategory][feature]['feature_weight'] = 0
                        else:
                            csf[compartment][category][subcategory][feature]['feature_weight'] /= total_weight


    @measure(JUPITER_LOGGER)
    def compute_category_scores(csf):
        """
        Computes the output score for each compartment/airline
        Each compnent contributes subcategory_weight * feature_weight * ratings[airline] for a given airline
        """
        dict_results={}
        for compartment in csf:
            for category in csf[compartment]:
                for subcategory in csf[compartment][category]:
                    subcategory_weight=csf[compartment][category][subcategory]['subcategory_weight']
                    for feature in csf[compartment][category][subcategory]:
                        if feature == 'subcategory_weight':
                            continue
                        feature_weight=csf[compartment][category][subcategory][feature]['feature_weight']
                        dict_ratings=csf[compartment][category][subcategory][feature]['ratings']
                        for airline in dict_ratings:
                            rating=float(dict_ratings[airline])
                            try:
                                dict_results[(compartment, category, airline)] += subcategory_weight * feature_weight * rating
                            except:
                                dict_results[(compartment, category, airline)]  = subcategory_weight * feature_weight * rating
        return dict_results


    @measure(JUPITER_LOGGER)
    def get_value_adds_score(dict_value_adds):
        """
            Translate the frequency counts to ranks and then to ratings on a scale of 0 to 10
        """
        for compartment in dict_value_adds:
            list_value_adds_counts=[]
            list_airlines=[]
            for airline in dict_value_adds[compartment]:
                list_airlines.append(airline)
                list_value_adds_counts.append(dict_value_adds[compartment][airline])
            list_ratings, _,_ =rescale_values(list_value_adds_counts, 0.0, 10.0)
            if list_ratings==None:
                list_ratings=[IMPUTED_RATING for i in list_value_adds_counts]
            for i in range(len(list_ratings)):
                list_ratings[i]=10.0-list_ratings[i]
            dict_value_adds[compartment]['dict_value_adds_rating']=dict(zip(list_airlines, list_ratings))


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
    def update_Data_Competitor_Ratings_collection(dict_transformed_results, dict_value_adds):
        """
        delete existing records and then insert the new records into JUP_DB_Data_Competitor_Ratings
        """

        db.JUP_DB_Data_Competitor_Ratings.remove({'group':'Product Rating'})
        dict_prod_rating_rec=dict()
        for compartment in dict_transformed_results:
            dict_prod_rating_rec['compartment'] = compartment
            for component in dict_transformed_results[compartment]:
                dict_prod_rating_rec = {}
                dict_prod_rating_rec['origin'] = None
                dict_prod_rating_rec['destination'] = None
                dict_prod_rating_rec['compartment'] = compartment
                dict_prod_rating_rec['group'] = 'Product Rating'
                dict_prod_rating_rec['component'] = component
                dict_prod_rating_rec['last_update_date_gmt']=strftime("%Y-%m-%d %H:%M:%S", gmtime())
                dict_prod_rating_rec['ratings']=dict_transformed_results[compartment][component]
                dict_prod_rating_rec['_id']=ObjectId()
                db.JUP_DB_Data_Competitor_Ratings.insert(dict_prod_rating_rec)
        for compartment in dict_value_adds:        ## if ratings not found (this really should not happen unless it is a programming error; precheck error and add to DICT_ERROR_ODS so the od won't get updated
            dict_prod_rating_rec = {}
            dict_prod_rating_rec['origin'] = None
            dict_prod_rating_rec['destination'] = None
            dict_prod_rating_rec['group'] = 'Product Rating'
            dict_prod_rating_rec['compartment'] = compartment
            component='Value Adds'
            dict_prod_rating_rec['component'] = component
            dict_prod_rating_rec['last_update_date_gmt'] = "{:%Y-%m-%d}".format(datetime.now())
            dict_prod_rating_rec['ratings']=dict_value_adds[compartment]['dict_value_adds_rating']
            dict_prod_rating_rec['_id']=ObjectId()
            db.JUP_DB_Data_Competitor_Ratings.insert(dict_prod_rating_rec)


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