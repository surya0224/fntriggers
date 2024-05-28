"""
File Name              : group_pricing_DAL
Author                 : Shamail Mulla
Date Created           : 2017-01-30
Description            : This file calculates a group price using sales data

MODIFICATIONS LOG
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :
"""

import datetime
import inspect

try:
    from jupiter_AI import client
    from jupiter_AI.network_level_params import JUPITER_DB
    db = client[JUPITER_DB]
except:
    pass
from jupiter_AI.tiles.jup_common_functions_tiles import gen_collection_name as gen
from jupiter_AI.common import ClassErrorObject as error_class


result_coll_name = gen()

class GroupPricing(object):
    def get_arg_lists(self, frame):
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


    def get_revenue_data(self, query):
        '''This method calculates marginal revenue from the demand curve obtained from sales data

        :param travel_date: string in yyyy-mm-dd format
        :param dict_user_filter: dictionary of filter values to retreive a group price for
        :return: group_price
        '''
        try:
            import numpy as np
            if 'JUP_DB_Sales' in db.collection_names():
                # The pipeline currently calculates best fare for a particular date, which can be modified
                # to find the best fare for a range of dates
                # Also we can output on which date which flight (by departure time) has the best fare
                # The above can be incorporated later are coded into the pipeline but are
                # commented out right now
                ppln_group_price = [
                    {
                        '$match': query
                    }
                    ,
                    {
                        '$group':
                            {
                                # '_id':
                                #     {
                                #         'fare' : '$AIR_CHARGE',
                                #         'date' : 'dep_date',
                                #         'flight' : flight no or departure time
                                #     }
                                '_id':'$AIR_CHARGE',
                                'pax': {'$sum': '$pax'}
                            }
                    }
                    # ,
                    # {
                    #     '$project':
                    #         {
                    #           #          '_id':0,
                                #         'fare' : '$fare',
                                #         'date' : '$date',
                                #         'flight' : '$flight'
                    #         }
                    # }
                    ,
                    {
                        '$sort': {'_id':-1}
                        # '$sort': {'fare': -1}
                    }
                    ,
                    # removing negative fares
                    {
                        '$redact':
                            {
                                '$cond':
                                    {
                                        'if': {'$gt': ['$_id', 0]},
                                        'then': '$$DESCEND',
                                        'else': '$$PRUNE'
                                    }
                            }
                    }
                    ,
                    {
                        '$project':
                            {
                                '_id':0,
                                'fare': '$_id',
                                'total_revenue': {'$multiply':['$_id', '$pax']},
                                'pax': '$pax',
                                # 'fare': '$_id.fare',
                                # 'date': '$_id.date',
                                # 'flight': '$_id.flight'
                            }
                    }
                    ,
                    {
                        '$out': result_coll_name
                    }
                ]

                db.JUP_DB_Sales.aggregate(ppln_group_price, allowDiskUse=True)
                # self.plot_line_graph(self,'Fare vs Total Revenue', 'Fares', lst_fares, lst_tot_rev, 'Total Revenue')

                if result_coll_name in db.collection_names():
                    revenue_data = db.get_collection(result_coll_name)
                    if revenue_data.count > 0:
                        fares = list(revenue_data.find(projection={'fare': 1, '_id': 0}))
                        rev = list(revenue_data.find(projection={'total_revenue': 1, '_id': 0}))
                        pax = list(revenue_data.find(projection={'pax': 1, '_id': 0}))
                        revenue_data.drop()

                        # making db format data into proper list
                        for index in range(0,(len(rev))):
                            fares[index] = fares[index][u'fare']
                            rev[index] = rev[index][u'total_revenue']
                            pax[index] = pax[index][u'pax']

                        return fares, rev, pax

                    else: # in case the resultant collection is empty
                        revenue_data.drop()
                        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                            'jupter_AI/RnA/group_pricing_DAL.py method: get_price',
                                                            self.get_arg_lists(inspect.currentframe()))
                        obj_error.append_to_error_list('There is no data.')
                else: # in case resultant collection is not created
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                        'jupiter_AI/RnA/group_pricing_DAL.py method: get_price',
                                                        self.get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list('Group price collection not created in the database. Check aggregate pipeline.')
                    obj_error.write_error_logs(datetime.datetime.now())

            else: # Collection to query is not present
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupter_AI/RnA/group_pricing_DAL.py method: get_price',
                                                    self.get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list('Collection JUP_DB_Sales cannot be found in the database.')
                obj_error.write_error_logs(datetime.datetime.now())

        except Exception as error_msg:
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                                'jupter_AI/RnA/group_pricing_DAL.py method: get_price',
                                                self.get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list(str(error_msg))
            obj_error.write_error_logs(datetime.datetime.now())
