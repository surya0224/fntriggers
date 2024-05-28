"""
File Name              :   dashboard.py
Author                 :   Sai Krishna
Date Created           :   2016-12-19
Description            :   module with methods to calculate tiles for competitor analysis dashboard
MODIFICATIONS LOG
    S.No               : 1
    Date Modified      : 2017-02-10
    By                 : Shamail Mulla
    Modification Details   : Code optimisation
"""
try:
    from jupiter_AI import client, JUPITER_DB, na_value
    db = client[JUPITER_DB]
except:
    pass
import datetime
import inspect

from jupiter_AI.common import ClassErrorObject as error_class


def get_module_name():
    return inspect.stack()[1][3]


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


def get_competitor_rating(filter_competitor_dhb_scr):
    """
    Retrieves competitor ratings for a particular filter
    :param filter_competitor_dhb_scr: dictionary for filtering db documents
    :return: dictionary of competitor ratings
    """
    try:
        if 'JUP_DB_Competitor_Ratings' in db.collection_names():
            cursor = db.JUP_DB_Competitor_Ratings.find({'origin': {'$in': filter_competitor_dhb_scr['origin']},
                                                       'destination': {'$in': filter_competitor_dhb_scr['destination']}},
                                                       projection={'airline': 1, 'rating': 1, '_id': 0})
            response = dict()
            # lst_c = list(cursor)
            for i in cursor:
                # print i
                response[i['airline']] = round(i['rating'],3)
            return response
        else: # if collection to query is not present in database
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupter_AI/tiles/competitor_analysis/competitor_rating.py method: get_competitor_rating',
                                                    get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list('Collection JUP_DB_Competitor_Rating cannot be found in the database.')
                obj_error.write_error_logs(datetime.datetime.now())
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/tiles/competitor_analysis/competitor_rating.py method: get_competitor_rating',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())

if __name__ == '__main__':
    a = {
        'region': [],
        'country': [],
        'pos': [],
        'origin': ['DXB'],
        'destination': ['DOH'],
        "compartment": [],
        'fromDate': '2016-10-01',
        'toDate': '2017-03-01'
    }
    start_time = datetime.datetime.now()
    print get_competitor_rating(a)
    print (datetime.datetime.now() - start_time)