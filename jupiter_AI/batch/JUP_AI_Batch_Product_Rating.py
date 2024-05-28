"""
 Author: Prem Narasimhan
 Completed date:
     first version: Feb 14, 2017
 This program calculates product rating at network level
 Input collection:
     JUP_DB_Data_Product
 Output collection:
     JUP_DB_Product_Ratings
 Overall logic:
     1. Fix the list of airlines (currently hard coded)
     2. There are 3 levels of attributes - category, subcategory and features
     3. Currently no weight has been assigned to any of the attributes, so each category/subcategory/feature gets a weight of 1
     4. There are some short records (<10 fields) in JUP_DB_Data_Product - I have simply omitted them
     5. Each record can have F,J, Y fields (any number of these 3). If "F"==1, then include it in calculation, otherwise there is no "F" for this feature
     6. Each record can have 1 or more "core_elements". There is also a special core element called "all" that includes all features
     7. Given a compartment and core_element, the feature strucure is built only for those features that have this compartment and core_element;
        Thus, "all" core_elements will have more features than a specific core_element.
     8. The ratings score for each airline (the output) is computed as:
        sum {over all features} [(weight of category) * (weight of subcategory) * (weight of feature) * (rating for the feature for given airline)] 
     9. Ratings can be 0, 5, or 10, with mean rating being 5.
     10. If an airline has no feature ranking for a given feature, the rating is imputed the mean rating of 5.
     11. The ratings are updated into JUP_DB_Product_Ratings using following logic
        a. delete all records in JUP_DB_Competitor_Ratings having key as compartment/core_element/airline.
        b. insert the new records
 Error handling:
     1. If the error level=WARNING, errors are collected in obj_err_main (defined at global level) but processing continues
     2. If error is serious, the score for these od's cannot be computed. I collected these ods in DICT_ERROR_ODS and at the time of updating these records I skip such ods
     3. At the end of the program, the errors in obj_err_main are flushed to JUP_DB_Errors_All 
"""
import datetime
import inspect
import time
from datetime import datetime

from bson.objectid import ObjectId
from jupiter_AI import client, JUPITER_DB, JUPITER_LOGGER
from jupiter_AI.logutils import measure
import jupiter_AI.common.ClassErrorObject as error_class
db = client[JUPITER_DB]


@measure(JUPITER_LOGGER)
def main():
    list_airlines = determine_airlines()
    ## read in the data into dict_prod_rating_records from JUP_DB_Data_Product
    dict_prod_rating_records = {}
    read_in_data(dict_prod_rating_records)
    ## get list of all core_elements in the collection
    dict_core_elements = dict()
    get_list_of_core_elements(dict_prod_rating_records, dict_core_elements)
    dict_core_elements['all'] = 1  ## add 'all' core_element, which denotes all core_elements
    """
        dict_weights_and_ratings is a dictionary of dictionary of dictionaries which is built from "dict_prod_rating_records"
            dict_weights_and_ratings[category] contains category dictionaries
            dict_weights_and_ratings[category][subcategory] contains subcategory dictionaries that belong to the category. dict_weights_and_ratings[od][category] also contains the key 'category_weight'
            dict_weights_and_ratings[category][subcategory][feature] is a dictionary that contains 'feature_weight' and the list 'airline_ratings'. It also contains the key "subcategory_weight"
    """
    dict_results = {}
    for compartment in ['F', 'J', 'Y']:
        for core_element in dict_core_elements:
            dict_weights_and_ratings = {}
            get_list_of_features(dict_prod_rating_records, list_airlines, dict_weights_and_ratings, compartment,
                                 core_element)
            compute_category_weights(dict_weights_and_ratings)
            compute_subcategory_weights(dict_weights_and_ratings)
            compute_feature_weights(dict_weights_and_ratings)
            compute_scores(dict_weights_and_ratings, list_airlines, compartment, core_element, dict_results)
    update_Product_Ratings_collection(list_airlines, dict_results)


@measure(JUPITER_LOGGER)
def determine_airlines():
    """
    airlines hard-coded as of now because the collection JUP_DB_Data_Product has these airlines as column names
    it is preferable that each airline has a separate record - ask Ashok to do this if possible
    """
    airlines = [
        "EK",
        "EY",
        "LH",
        "VS",
        "BA",
        "SQ",
        "AA",
        "QR",
        "FZ",
        "CX",
        "NH",
        "TK",
        "QF",
        "GA",
        "BR",
        "OZ",
        "LX",
        "AF",
        "OS",
        "TG",
        "JL",
        "NZ",
        "KA",
        "HU",
        "PG",
        "MH",
        "AK",
        "AC",
        "KL",
        "A3",
        "HX",
        "AY",
        "LA",
        "D8",
        "SA",
        "WY",
        "CZ",
        "KE",
        "U2",
        "KC",
        "JQ",
        "MI",
        "D7",
        "I9",
        "DL",
        "SU",
        "WS",
        "PD",
        "AV",
        "B6",
        "JJ",
        "CI",
        "SN",
        "VN",
        "IB",
        "SK",
        "TZ",
        "BY",
        "UA",
        "EI",
        "AD",
        "HM",
        "CM",
        "AS",
        "G9"
    ]
    return airlines



@measure(JUPITER_LOGGER)
def read_in_data(dict_prod_rating_records):
    """
    This method reads in data from JUP_DB_Data_Product
    """
    cursor = db.JUP_DB_Data_Product.find({})
    counter_cursor = -1
    for c in cursor:
        if len(c) < MIN_RECORD_LENGTH:
            continue
        if c['Sub_Category'] == c['Feature']:
            continue
        counter_cursor += 1
        dict_prod_rating_records[counter_cursor] = c


@measure(JUPITER_LOGGER)
def get_list_of_core_elements(dict_prod_rating_records, dict_core_elements):
    """
    Build a dictionary of all the core_elements in the collection
    """
    for index in range(len(dict_prod_rating_records)):
        dict_rec = dict_prod_rating_records[index]
        try:
            core_elements = dict_rec['Core elements']
            list_core_elements = core_elements.split()
            for core_element in list_core_elements:
                dict_core_elements[core_element] = 1
        except:
            obj_err = error_class.ErrorObject(error_class.ErrorObject.WARNING, get_module_name(),
                                              get_arg_lists(inspect.currentframe()))
            obj_err.append_to_error_list(
                "no core elements found for category= " + dict_rec['Category'] + ", subcategory=" + dict_rec[
                    'Sub_Category'] + ", feature = " + dict_rec['Feature'])
            obj_err_main.append_to_error_object_list(obj_err)
            list_core_elements = []


@measure(JUPITER_LOGGER)
def get_list_of_features(dict_prod_rating_records, list_airlines, dict_weights_and_ratings, compartment, core_element):
    """
    This method updates the dictionary of lists "dict_weights_and_ratings" with category level dictionaries
    After this function runs, we get a dictionary of dictionaries (nested 3 times), like dict_weights_and_ratings[category][subcategory][feature]
    Each category contains some subcategories. Each subcategory contains some features. For each feature, there are ratings for each airline.
    There are no weights explicitly given at category, subcategory or feature level. So each occurrence is given a weight of 1.
    """
    for index in range(len(dict_prod_rating_records)):
        dict_rec = dict_prod_rating_records[index]
        category = dict_rec['Category']
        subcategory = dict_rec['Sub_Category']
        feature = dict_rec['Feature']
        category_weight = 1
        subcategory_weight = 1
        feature_weight = 1

        try:
            compartment_indicator = dict_rec[compartment]
            if compartment_indicator != "1":  ## compartment is not included
                continue
        except:  ## compartment not found, ignore this feature
            continue

        try:
            list_core_elements = dict_rec['Core elements']
        except:
            if core_element == 'all':
                pass
            else:
                continue

        if core_element not in list_core_elements and core_element != 'all':  ## core element not found in this feature, so go to next record
            continue

        list_ratings = []
        for airline in list_airlines:
            try:
                x = float(dict_rec[airline])  ## (raw ratings range from 0 to 10, so normalize rating by dividing by 10)
            except:
                obj_err = error_class.ErrorObject(error_class.ErrorObject.WARNING, get_module_name(),
                                                  get_arg_lists(inspect.currentframe()))
                obj_err.append_to_error_list(
                    "rating missing for airline=" + airline + ", category=" + category + ", subcategory=" + subcategory + ", feature = " + feature + "; rating assumed to be mean=" + str(
                        IMPUTED_RATING))
                obj_err_main.append_to_error_object_list(obj_err)
                x = IMPUTED_RATING  ## missing value gets mean normalized rating
            list_ratings.append(x)
        update_category(category, category_weight, subcategory, subcategory_weight, feature, feature_weight,
                        list_ratings, core_element, compartment, dict_weights_and_ratings)


@measure(JUPITER_LOGGER)
def update_category(category, category_weight, subcategory, subcategory_weight, feature, feature_weight, list_ratings,
                    core_element, compartment, dict_weights_and_ratings):
    """
    This method is helper function to get_list_of_categories: it updates the dictionary of lists "dict_weights_and_ratings"
    """
    try:
        dict_weights_and_ratings[category]
    except:
        dict_weights_and_ratings[category] = dict()
        dict_weights_and_ratings[category]['category_weight'] = category_weight
    try:
        dict_weights_and_ratings[category][subcategory]
    except:
        dict_weights_and_ratings[category][subcategory] = dict()
        dict_weights_and_ratings[category][subcategory]['subcategory_weight'] = subcategory_weight
    try:
        dict_weights_and_ratings[category][subcategory][feature]
    except:
        dict_weights_and_ratings[category][subcategory][feature] = dict()
        dict_weights_and_ratings[category][subcategory][feature]['feature_weight'] = feature_weight
        dict_weights_and_ratings[category][subcategory][feature]['list_ratings'] = list_ratings


@measure(JUPITER_LOGGER)
def compute_category_weights(dict_weights_and_ratings):
    """
    Converts the raw category weights such that sum of normalized weights equals 1
    """
    total_weight = 0.0
    for category in dict_weights_and_ratings:
        total_weight += dict_weights_and_ratings[category]['category_weight']

    for category in dict_weights_and_ratings:
        if abs(total_weight) < TOLERANCE:
            obj_err = error_class.ErrorObject(error_class.ErrorObject.WARNING, get_module_name(),
                                              get_arg_lists(inspect.currentframe()))
            obj_err.append_to_error_list(
                "category=" + category + ": total raw weight close to zero, setting normalized weight to zero")
            obj_err_main.append_to_error_object_list(obj_err)
            dict_weights_and_ratings[category]['category_weight'] = 0
        else:
            dict_weights_and_ratings[category]['category_weight'] /= total_weight


@measure(JUPITER_LOGGER)
def compute_subcategory_weights(dict_weights_and_ratings):
    """
    Converts the raw subcategory weights such that sum of normalized weights equals 1
    """
    for category in dict_weights_and_ratings:
        total_weight = 0.0
        for subcategory in dict_weights_and_ratings[category]:
            if subcategory == 'category_weight':  ## we are interested only in sucategoriess which are keys, not the key 'category_weight'
                continue
            total_weight += dict_weights_and_ratings[category][subcategory]['subcategory_weight']
        for subcategory in dict_weights_and_ratings[category]:
            if subcategory == 'category_weight':  ## we are interested only in subcategories which are keys, not the key 'category_weight'
                continue
            if abs(total_weight) < TOLERANCE:
                obj_err = error_class.ErrorObject(error_class.ErrorObject.WARNING, get_module_name(),
                                                  get_arg_lists(inspect.currentframe()))
                obj_err.append_to_error_list(
                    "category=" + category + "subcategory=" + subcategory + ": total raw weight close to zero, setting normalized weight to zero for od=" + od)
                obj_err_main.append_to_error_object_list(obj_err)
                dict_weights_and_ratings[category][subcategory]['subcategory_weight'] = 0
            else:
                dict_weights_and_ratings[category][subcategory]['subcategory_weight'] /= total_weight


@measure(JUPITER_LOGGER)
def compute_feature_weights(dict_weights_and_ratings):
    """
    Converts the raw feature weights such that sum of normalized weights equals 1
    """

    for category in dict_weights_and_ratings:
        for subcategory in dict_weights_and_ratings[category]:
            if subcategory == 'category_weight':  ## we are interested only in sucategoriess which are keys, not the key 'category_weight'
                continue
            total_weight = 0.0
            for feature in dict_weights_and_ratings[category][subcategory]:
                if feature == 'subcategory_weight':  ## we are interested only in sucategoriess which are keys, not the key 'category_weight'
                    continue
                total_weight += dict_weights_and_ratings[category][subcategory][feature]['feature_weight']
            for feature in dict_weights_and_ratings[category][subcategory]:
                if feature == 'subcategory_weight':  ## we are interested only in subcategories which are keys, not the key 'category_weight'
                    continue
                if abs(total_weight) < TOLERANCE:
                    obj_err = error_class.ErrorObject(error_class.ErrorObject.WARNING, get_module_name(),
                                                      get_arg_lists(inspect.currentframe()))
                    obj_err.append_to_error_list(
                        "category=" + category + "subcategory=" + subcategory + ", feature=" + feature + ": total raw feature weight close to zero, setting normalized weight to zero for od=" + od)
                    obj_err_main.append_to_error_object_list(obj_err)
                    dict_weights_and_ratings[category][subcategory][feature]['feature_weight'] = 0
                else:
                    dict_weights_and_ratings[category][subcategory][feature]['feature_weight'] /= total_weight


@measure(JUPITER_LOGGER)
def compute_scores(dict_weights_and_ratings, list_airlines, compartment, core_element, dict_results):
    """
    Computes the output score for each airline
    Each compnent contributes category_weight * category_weight * feature_weight * ratings[airline] for a given airline
    """
    scores = [0.0 for j in range(len(list_airlines))]
    for category in dict_weights_and_ratings:
        category_weight = dict_weights_and_ratings[category]['category_weight']
        for subcategory in dict_weights_and_ratings[category]:
            if subcategory == 'category_weight':  ##  we are interested only in subcategories which are keys, not the key 'category_weight'
                continue
            subcategory_weight = dict_weights_and_ratings[category][subcategory]['subcategory_weight']
            for feature in dict_weights_and_ratings[category][subcategory]:
                if feature == 'subcategory_weight':  ##  we are interested only in features which are keys, not the key 'subcategory_weight'
                    continue

                feature_weight = dict_weights_and_ratings[category][subcategory][feature]['feature_weight']
                list_ratings = dict_weights_and_ratings[category][subcategory][feature]['list_ratings']
                for j in range(len(list_ratings)):
                    rating = list_ratings[j]
                    scores[j] += category_weight * subcategory_weight * feature_weight * rating
    try:
        dict_results[compartment]
    except:
        dict_results[compartment] = {}
    try:
        dict_results[compartment][core_element]
    except:
        dict_results[compartment][core_element] = {}
    dict_results[compartment][core_element]['scores'] = scores


@measure(JUPITER_LOGGER)
def update_Product_Ratings_collection(list_airlines, dict_results):
    """
    delete these records from JUP_DB_Product_Ratings
    insert new records back
    """
    airline_compartment_core_element_build = []
    for compartment in dict_results:
        for core_element in dict_results[compartment]:
            for airline in list_airlines:
                airline_compartment_core_element_build.append(
                    {'compartment': compartment, 'core_element': core_element, 'airline': airline})

    query = {}
    query['$or'] = airline_compartment_core_element_build

    db.JUP_DB_Product_Ratings.remove(query)
    dict_prod_rating_rec = {}
    for compartment in dict_results:  ## if ratings not found (this really should not happen unless it is a programming error; precheck error and add to DICT_ERROR_ODS so the od won't get updated
        dict_prod_rating_rec['compartment'] = compartment
        for core_element in dict_results[compartment]:
            dict_prod_rating_rec['core_element'] = core_element
            for airline in list_airlines:
                dict_prod_rating_rec['airline'] = airline
                rating = find_score(airline, list_airlines, dict_results[compartment][core_element]['scores'])
                dict_prod_rating_rec['rating'] = rating
                dict_prod_rating_rec['last_update_date'] = "{:%Y-%m-%d}".format(datetime.now())
                dict_prod_rating_rec['_id'] = ObjectId()
                db.JUP_DB_Product_Ratings.insert(dict_prod_rating_rec)


@measure(JUPITER_LOGGER)
def find_score(airline, list_airlines, list_scores):
    """
    given an airline, find its place in list_airlines, and retrieve corresponding element from list_scores
    """
    counter = -1
    found = False
    for a in list_airlines:
        counter += 1
        if a == airline:
            found = True
            break
    if not found:
        obj_err = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, get_module_name(),
                                          get_arg_lists(inspect.currentframe()))
        obj_err.append_to_error_list("internal error: airline rating not found for airline = " + airline)
        obj_err_main.append_to_error_object_list(obj_err)
        return None
    return list_scores[counter]


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
    argument_name_list = []
    argument_value_list = []
    for k in args:
        argument_name_list.append(k)
        argument_value_list.append(values[k])
    return argument_name_list, argument_value_list


start_time = time.time()
print start_time
obj_err_main = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, "main", "")
obj_err_main.append_to_error_list("main error level")
DICT_ERROR_ODS = {}
IMPUTED_RATING = 5
MIN_RECORD_LENGTH = 60
TOLERANCE = 0.001  ## error tolerance
main()
##print obj_err_main
dict_errors = {}
dict_errors['program'] = 'competitor ratings'
dict_errors['date'] = "{:%Y-%m-%d}".format(datetime.now())
dict_errors['time'] = time.time()
dict_errors['error description'] = obj_err_main.as_string()
db.JUP_DB_Errors_All.insert(dict_errors)
print("--- %s seconds ---" % (time.time() - start_time))
