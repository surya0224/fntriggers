"""

 Author: Prem Narasimhan
 Completed date:
     first version: Feb 9, 2017
 This program calculates competitor rating at od level
 Overall logic:
     1. Fix the list of airlines (currently hard coded)
     2. There are 2 levels of attributes - category and component
     3. category level weights are available in those records where category==Components.
     4. component level weights as well as airline ratings are in the records where category != Components
     5. The ratings score for each airline (the output) is computed as:
        sum {over all features} [(weight of category) * (weight of subcategory) * (weight of feature) * (rating for the feature for given airline)] 
     6. Ratings can range from 0 to 10, with mean rating being 5.
     7. If an airline has no feature ranking for a given feature, the rating is imputed the mean rating of 5.
     8. The ratings are updated into JUP_DB_Competitor_Ratings using following logic
        a. Read all documents with airlines, od combination as in JUP_DB_Data_Competitor and store them in a dictionary
        b. delete all records in JUP_DB_Competitor_Ratings having same query criteria as in 8.a. 
        c. for existing records, just change the "rating", "competitor_ratings" and "last_update_date" fields
        d. for new records insert the full record, setting "product_rating" to null
 Error handling:
     1. If the error level=WARNING, errors are collected in obj_err_main (defined at global level) but processing continues
     2. If error is serious, the score for these od's cannot be computed. I collected these ods in DICT_ERROR_ODS and at the time of updating these records I skip such ods
     3. At the end of the program, the errors in obj_err_main are flushed to JUP_DB_Errors_All 
"""
import datetime
import inspect
import time
from datetime import datetime

import pymongo
from bson.objectid import ObjectId

import jupiter_AI.common.ClassErrorObject as error_class



def main():
    list_airlines                               = determine_airlines()
    """
        dict_weights_and_ratings is a dictionary of dictionary of dictionaries
            dict_weights_and_ratings[od] contains category dictionaries for that od
            dict_weights_and_ratings[od][category] contains component dictionaries that belong to the category. dict_weights_and_ratings[od][category] also contains the key 'category_weight'
            dict_weights_and_ratings[od][category][component] is a dictionary that contains 'component_weight' and the list 'airline_ratings'
    """
    dict_weights_and_ratings                   = dict()
    ## get_list_of_categories_for_all_ods() has to run before get_list_of_components_for_all_ods(), because a category has to defined before one its component is added to dict_weights_and_ratings
    get_list_of_categories_for_all_ods          (dict_weights_and_ratings)
    get_list_of_components_for_all_ods          (dict_weights_and_ratings, list_airlines)
    compute_category_weights                    (dict_weights_and_ratings)
    compute_component_weights                   (dict_weights_and_ratings)
    compute_scores                              (dict_weights_and_ratings, list_airlines)            
    update_Competitor_Ratings_collection        (dict_weights_and_ratings, list_airlines)


def determine_airlines():
    """
    airlines hard-coded as of now because the collection JUP_DB_Data_Competitor has these airlines as column names
    it is preferable that each airline has a separate record - ask Ashok to do this if possible
    """
    airlines=[
        "EK",
        "G9",
        "EY",
        "QR",
        "ET",
        "9W",
        "FZ"
        ]
    return airlines


def get_list_of_categories_for_all_ods(dict_weights_and_ratings):
    """
    This method updates the dictionary of lists "dict_weights_and_ratings" with category level dictionaries
    After this function runs, we get a dictionary of dictionaries, like dict_weights_and_ratings[od][category]
    Each category contains some components. So components cannot be read in before its parent (a category) is read in.
    So this method has to precede get_list_of_components_for_all_ods()
    """
    cursor = db.JUP_DB_Data_Competitor.find({})
    for c in cursor:
        od              = c['od']
        category        = c['category']
        component       = c['Components']
        if category     != component:   ## not a category level records
            continue
        category_weight = c['weights']     
        if category_weight == None:
            category_weight = 0
        update_category (od, category, category_weight, dict_weights_and_ratings)


def get_list_of_components_for_all_ods(dict_weights_and_ratings, list_airlines):
    """
    This method updates the dictionary of lists "dict_weights_and_ratings" with component level dictionaries
    """
    cursor = db.JUP_DB_Data_Competitor.find({})
    for c in cursor:
        od                  = c['od']
        category            = c['category']
        component           = c['Components']
        if category         == component:       ## category level record, skip it (we are only interested in component level records)
            continue
        component_weight    = c['weights']
        if component_weight == None:
            component_weight = 0
        list_ratings= []
        
        for airline in list_airlines:
            try:
                x=float(c[airline])    ## (raw ratings range from 0 to 10, so normalize rating by dividing by 10)
                list_ratings.append(x)
                continue
            except:
                obj_err = error_class.ErrorObject(error_class.ErrorObject.WARNING, get_module_name(), get_arg_lists(inspect.currentframe()))
                obj_err.append_to_error_list("rating missing for airline=" + airline + ", od=" + od + ", category=" + category + ", component=" + component + "; rating assumed to be mean=" + str(IMPUTED_RATING))
                obj_err_main.append_to_error_object_list(obj_err)
                x=IMPUTED_RATING      ## missing value gets mean normalized rating
                list_ratings.append(x)
        update_component(od, category, component, component_weight, list_ratings, dict_weights_and_ratings)
    return dict_weights_and_ratings        


def update_category(od, category, category_weight, dict_weights_and_ratings):
    """
    This method is helper function to get_list_of_categories_for_all_ods(): it updates the dictionary of lists "dict_weights_and_ratings"
    """
    try:
        dict_weights_and_ratings[od]
    except:
        dict_weights_and_ratings[od] = dict()
    try:
        dict_weights_and_ratings[od][category]
    except:
        dict_weights_and_ratings[od][category] = dict()
    dict_weights_and_ratings[od][category]['category_weight']=category_weight


def update_component(od, category, component, component_weight, list_ratings, dict_weights_and_ratings):
    """
    This method is helper function to get_list_of_compoents_for_all_ods(): it updates the dictionary of lists "dict_weights_and_ratings"
    """
    try:
        dict_weights_and_ratings[od][category]     
        try:
            dict_weights_and_ratings[od][category][component]      ##component should not already exist; if it does the component is duplicated, raise warning but proceed with current component (overwrite previous value)
            obj_err = error_class.ErrorObject(error_class.ErrorObject.WARNING, get_module_name(), get_arg_lists(inspect.currentframe()))
            obj_err.append_to_error_list("duplicate component: category=" + category + ", component="+ component)
            obj_err_main.append_to_error_object_list(obj_err)
            dict_weights_and_ratings[od][category][component]['component_weight']=component_weight   
            dict_weights_and_ratings[od][category][component]['list_ratings']=list_ratings
            return
        except:
            dict_weights_and_ratings[od][category][component] = dict()
            dict_weights_and_ratings[od][category][component]['component_weight']=component_weight   
            dict_weights_and_ratings[od][category][component]['list_ratings']=list_ratings
            return
    except:     ## parent category for component doesnt exist, dont update for this od
        DICT_ERROR_ODS[od]=1
        obj_err = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, get_module_name(), get_arg_lists(inspect.currentframe()))  
        obj_err.append_to_error_list("category="+category+" doesnt exist for component for od=" + od)
        obj_err_main.append_to_error_object_list(obj_err)


def compute_category_weights(dict_weights_and_ratings):
    """
    Converts the raw category weights such that sum of normalized weights equals 1
    """
    for od in dict_weights_and_ratings:
        total_weight=0.0
        for category in dict_weights_and_ratings[od]:
            total_weight += dict_weights_and_ratings[od][category]['category_weight']
        for category in dict_weights_and_ratings[od]:
            if abs(total_weight) < TOLERANCE:
                obj_err = error_class.ErrorObject(error_class.ErrorObject.WARNING, get_module_name(), get_arg_lists(inspect.currentframe()))  
                obj_err.append_to_error_list("category="+category+": total raw weight close to zero, setting normalized weight to zero for od=" + od)
                obj_err_main.append_to_error_object_list(obj_err)
                dict_weights_and_ratings[od][category]['category_weight'] = 0
            else:
                dict_weights_and_ratings[od][category]['category_weight'] /= total_weight
        

def compute_component_weights(dict_weights_and_ratings):
    """
    Converts the raw component weights such that sum of normalized weights equals 1
    """
    for od in dict_weights_and_ratings:
        for category in dict_weights_and_ratings[od]:
            total_weight=0.0
            for component in dict_weights_and_ratings[od][category]:
                if component == 'category_weight':      ## we are interested only in components which are keys, not the key 'category_weight'
                    continue
                total_weight += dict_weights_and_ratings[od][category][component]['component_weight']
            for component in dict_weights_and_ratings[od][category]:
                if component == 'category_weight':      ## we are interested only in components which are keys, not the key 'category_weight'
                    continue
                if abs(total_weight) < TOLERANCE:
                    obj_err = error_class.ErrorObject(error_class.ErrorObject.WARNING, get_module_name(), get_arg_lists(inspect.currentframe()))  
                    obj_err.append_to_error_list("category="+category+ "component=" + component + ": total raw weight close to zero, setting normalized weight to zero for od=" + od)
                    obj_err_main.append_to_error_object_list(obj_err)
                    dict_weights_and_ratings[od][category][component]['component_weight'] = 0
                else:
                    dict_weights_and_ratings[od][category][component]['component_weight'] /= total_weight


def compute_scores(dict_weights_and_ratings, list_airlines):
    """
    Computes the output score for each airline
    Each compnent contributes category_weight * component_weight * ratings[airline] for a given airline
    """
    for od in dict_weights_and_ratings:
        scores = [0.0 for j in range(len(list_airlines))]
        for category in dict_weights_and_ratings[od]:
            category_weight=dict_weights_and_ratings[od][category]['category_weight']
            for component in dict_weights_and_ratings[od][category]:
                if component == 'category_weight':  ##  we are interested only in components which are keys, not the key 'category_weight'
                    continue
                component_weight=dict_weights_and_ratings[od][category][component]['component_weight']
                list_ratings=dict_weights_and_ratings[od][category][component]['list_ratings']
                for j in range(len(list_ratings)):
                    rating=list_ratings[j]
                    scores[j]+= category_weight * component_weight * rating
        dict_weights_and_ratings[od]['scores']=scores
        

def update_Competitor_Ratings_collection(dict_weights_and_ratings, list_airlines):
    """
    read all documents for corresponding airlines and od combinations
    delete these records from JUP_DB_Competitor_Ratings
    insert records back into JUP_DB_Competitor_Ratings
       if this document (ie od-airline combo) existed before, keep "product_rating" as before, otherwise make this None (null in MongoDB lingo)
    """
    oda_build = []
    for od in dict_weights_and_ratings:
        origin=od[0:3]
        destination=od[3:6]
        for airline in list_airlines:
            oda_build.append({'origin': origin, 'destination': destination, 'airline':airline})

    query={}
    query['$or'] = oda_build

    dict_existing_recs={}
    cursor = db.JUP_DB_Competitor_Ratings.find(query)
    for c in cursor:
        od = c['od']
        airline=c['airline']
        try:
            dict_existing_recs[od]
        except:
            dict_existing_recs[od]={}
        try:
            dict_existing_recs[od][airline]
        except:
            dict_existing_recs[od][airline]={}
        dict_existing_recs[od][airline]['origin'] = od[0:3]
        dict_existing_recs[od][airline]['destination'] = od[3:6]
        dict_existing_recs[od][airline]['last_update_date'] = c['last_update_date']
        dict_existing_recs[od][airline]['rating']=c['rating']
        dict_existing_recs[od][airline]['competitor_rating'] = c['competitor_rating']
        dict_existing_recs[od][airline]['product_rating'] = c['product_rating']

    db.JUP_DB_Competitor_Ratings.remove(query)

    for od in dict_weights_and_ratings:        ## if ratings not found (this really should not happen unless it is a programming error; precheck error and add to DICT_ERROR_ODS so the od won't get updated
        for airline in list_airlines:
            try:
                rating = find_score(airline, list_airlines, dict_weights_and_ratings[od]['scores'])
            except error_class.ErrorObject as obj_err:
                obj_err_main.append_to_error_object_list(obj_err)
                DICT_ERROR_ODS[od]=1

    ## insert records
    dict_comptt_rating_rec={}
    for od in dict_weights_and_ratings:
        try:
            DICT_ERROR_ODS[od]      ## this dictionary contains lis of od with severe errors, hence ratings are not overwritten
            continue
        except:
            pass
        dict_comptt_rating_rec['od']    = od
        dict_comptt_rating_rec['origin'] = od[0:3]
        dict_comptt_rating_rec['destination'] = od[3:6]
        dict_comptt_rating_rec['last_update_date'] = "{:%Y-%m-%d}".format(datetime.now())
        for airline in list_airlines:
            dict_comptt_rating_rec['airline']= airline
            rating = find_score(airline, list_airlines, dict_weights_and_ratings[od]['scores'])
            dict_comptt_rating_rec['rating'] = rating
            dict_comptt_rating_rec['competitor_rating'] = rating
            dict_comptt_rating_rec['product_rating'] = None
            dict_comptt_rating_rec['_id'] = ObjectId() 
            try:
                dict_existing_recs[od][airline]
                dict_comptt_rating_rec['product_rating'] = dict_existing_recs[od][airline]['product_rating']
            except:
                dict_comptt_rating_rec['product_rating'] = None
            db.JUP_DB_Competitor_Ratings.insert(dict_comptt_rating_rec)


def find_score(airline, list_airlines, list_scores):
    """
    given an airline, find its place in list_airlines, and retrieve corresponding element from list_scores
    """
    counter = -1
    found=False
    for a in list_airlines:
        counter += 1
        if a==airline:
            found=True
            break
    if not found:
        obj_err = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, get_module_name(), get_arg_lists(inspect.currentframe()))
        obj_err.append_to_error_list("internal error: airline rating not found for airline = " + airline)
        raise obj_err
    return list_scores[counter]


##def check_if_host_in_list_airlines(list_airlines):
##    host_airline=net.Host_Airline_Code
##    for airline in list_airlines:
##        if airline == host_airline:
##            return True
##    return False


def get_module_name():
    return inspect.stack()[1][3]


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
MONGO_CLIENT_URL = '13.92.251.7:42525'
#net.MONGO_CLIENT_URL
ANALYTICS_MONGO_USERNAME = 'analytics'
ANALYTICS_MONGO_PASSWORD = 'KNjSZmiaNUGLmS0Bv2'
JUPITER_DB = 'testDB'
client = pymongo.MongoClient(MONGO_CLIENT_URL)
client.the_database.authenticate(ANALYTICS_MONGO_USERNAME, ANALYTICS_MONGO_PASSWORD, source='admin')
db = client[JUPITER_DB]
obj_err_main = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, "main", "")
obj_err_main.append_to_error_list("main error level")
DICT_ERROR_ODS={}
IMPUTED_RATING=5
TOLERANCE=0.001         ## error tolerance
main()
dict_errors={}
dict_errors['program']='competitor ratings'
dict_errors['date']="{:%Y-%m-%d}".format(datetime.now())
dict_errors['time']=time.time()
dict_errors['error description']=obj_err_main.as_string()
db.JUP_DB_Errors_All.insert(dict_errors)
print("--- %s seconds ---" % (time.time() - start_time))
