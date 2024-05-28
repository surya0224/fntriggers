"""
File Name              :   frequent_flyer_revenue.py
Author                 :   Pavan
Date Created           :   2016-12-21
Description            :   module with methods to calculate tiles for distribution and customer segment dashboard
MODIFICATIONS LOG      :
    S.No               :
    Date Modified      :
    By                 :
    Modification Details   :
"""
import inspect
import os

root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),os.path.pardir,os.path.pardir,os.path.pardir))
from jupiter_AI.network_level_params import na_value
from copy import deepcopy
from collections import defaultdict

"""
Try to break up the working into smaller functions like for query_builder
Split file into DAL and BLL, all python processing to be done in BLL
Build and fire the queries in DAL
Leave sufficient lines when a piece of logic is completed
"""


def get_module_name():
    '''+
    FUnction used to get the module name where it is called
    '''
    return inspect.stack()[1][3]


def get_arg_lists(frame):
    '''
    function used to get the list of arguments of the function
    where it is called
    '''
    args, _, _, values = inspect.getargvalues(frame)
    argument_name_list = []
    argument_value_list = []
    for k in args:
        argument_name_list.append(k)
        argument_value_list.append(values[k])
    return argument_name_list, argument_value_list



def get_frequent_flyer_revenue_share(afilter):
    data = deepcopy(defaultdict(list, afilter))
    query = dict()
    if data['region']:
        query['region'] = {'$in': data['region']}
    if data['country']:
        query['country'] = {'$in': data['country']}
    if data['pos']:
        query['pos'] = {'$in': data['pos']}
    if data['compartment']:
        query['compartment'] = {'$in': data['compartment']}
    if data['origin']:
        od_build = []
        for idx, item in enumerate(data['origin']):
            od_build.append({'origin': item, 'destination': data['destination'][idx]})
            query['$or'] = od_build
    if (data['origin'] != [] or data['destination'] != []):
        query['dep_date'] = {'$gte': data['fromDate'], '$lte': data['toDate']}
        #print query
        # ffr(frequent_flyer_revenue_)
    #     ffr_pipeline = db['JUP_DB_Sales'].aggregate([{
    #                                                         '$match': query
    #                                                    }
    #                                                    ,
    #                                                    {
    #                                                        '$group':
    #                                                            {
    #                                                                '_id':
    #                                                                    {
    #                                                                        'channel':'$channel'
    #                                                                    }
    #                                                                    ,
    #                                                                'revenue': {'$sum': '$revenue_base'}
    #                                                            }
    #                                                    }
    #                                                    ])
    #
    # else:
    #     query['dep_date'] = {'$in': [data['fromDate'], data['toDate']]}
    #     ffr_pipeline = db['JUP_DB_Cumulative_Dep_Date'].aggregate([{
    #                                                                     '$match': query
    #                                                                  }
    #                                                                  ,
    #                                                              {
    #                                                                  '$group':
    #                                                                      {
    #                                                                          '_id':
    #                                                                              {
    #                                                                                  'channel':'$channel'
    #                                                                              }
    #                                                                              ,
    #                                                                          'torevenue': {'$max': '$sale_revenue_base'},
    #                                                                          'fromrevenue': {'$min': '$sale_revenue_base'}
    #                                                                      }
    #                                                              }
    #                                                              ,
    #                                                           {
    #                                                               '$project':
    #                                                                   {
    #                                                                       'channel': '$_id.channel'
    #                                                                       ,
    #                                                                         'revenue':{'$subtract':['$torevenue','$fromrevenue']}
    #                                                                   }
    #                                                           }
    #                                                           ])
    #
    # tot_frequent_flyer_revenue = 0
    # tot_channel_revenue = 0
    # ffr_pipeline_list = list(ffr_pipeline)
    # if len(ffr_pipeline_list) != 0:
    #     for i in ffr_pipeline_list:
    #         tot_channel_revenue += i['revenue']
    #         print tot_channel_revenue
    #         if (i['_id']['channel']) == 'GDS':
    #             tot_frequent_flyer_revenue += float(i['revenue'])
    #         frequent_flyer_revenue = (tot_frequent_flyer_revenue / tot_channel_revenue) * 100
    #         response = dict()
    #         response['frequent_flyer_revenue_%'] = frequent_flyer_revenue
    return na_value

        # else:
        #     # use proper variable name for 'e1'
        #     e1 = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1, get_module_name(),
        #                                  get_arg_lists(inspect.currentframe()))
        #     e1.append_to_error_list("Expected 1 document with channels but got " + str(ffr_pipeline.count()))
        #     raise e1
        #     return NA

if __name__=='__main__':
    import time

    st = time.time()
    filter = {
              'region': ['GCC'],
              'country': ['SA'],
              'pos': ['RUH'],
              'origin': ['RUH'],
              'destination': ['CMB'],
              'compartment': ['Y'],
              'fromDate': '2016-09-10',
              'toDate': '2016-09-12'
              }

    result = get_frequent_flyer_revenue_share(filter)
    print result
    #print "time in seconds", round(time.time() - st, 3)