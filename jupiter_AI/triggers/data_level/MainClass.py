"""
File Name              :   MainClass.py
Author                 :   Sai Krishna
Date Created           :   2016-02-20
Description            :   Contains DataLevel class Object of a data level trigger
                           containing all common functions for a Data Level trigger

MODIFICATIONS LOG
    S.No                   : 1
    Date Modified          : 3.3.2017
    By                     : Sai
    Modification Details   : Removed the Hard Coded values of Capacity in function and obtained relevant values from
                             JUP_DB_OD_Capacity collection.
                             Created functions generate_capacity_ppln
                                               build_capacity_query
                             Constraints -
                                No Competitor Parameter in the Capacity Collection collection.

"""
import datetime
import re
import pandas as pd
import numpy as np
import inspect
import math
from collections import defaultdict
from copy import deepcopy
from jupiter_AI import mongo_client
import jupiter_AI.common.ClassErrorObject as error_class
from jupiter_AI.network_level_params import JUPITER_DB, query_month_year_builder, SYSTEM_DATE, Host_Airline_Code,JUPITER_LOGGER
from jupiter_AI.batch.JUP_AI_OD_Capacity.common_functions import get_str
from jupiter_AI.tiles.jup_common_functions_tiles import gen_collection_name
from jupiter_AI.triggers.MainClass import Trigger
from jupiter_AI.triggers.common import get_no_of_days
from jupiter_AI.BaseParametersCodes.common import get_ly_val
from jupiter_AI.logutils import measure
#db = client[JUPITER_DB]

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


class DataLevel(Trigger):
    """
    """
    @measure(JUPITER_LOGGER)
    def __init__(self, data, system_date):
        self.old_doc_data = deepcopy(data)
        self.new_doc_data = deepcopy(data)
        self.trigger_date = system_date

    #   --------------------------------- METHOD TO GET BOOKINGS DATA -----------------------------------

    @measure(JUPITER_LOGGER)
    def get_bookings_val(self, book_date_start, book_date_end, dep_date_start, dep_date_end, db, data_list,query=None):
        """
        Get bookings data for this year and last year
        :param book_date_start:
        :param book_date_end:
        :param dep_date_start:
        :param dep_date_end:
        :return:
        """
        import time
        df_main = pd.DataFrame(columns=['market'])
        df_main['market'] = data_list
        if 'JUP_DB_Manual_Triggers_Module' in db.collection_names():

            temp_collection_name = gen_collection_name()
            qry_bookings_collection = dict()
            if query:
                qry_bookings_collection = deepcopy(query)
            else:
                # qry_bookings_collection = {
                #     'pos': self.old_doc_data['pos'],
                #     'origin': self.old_doc_data['origin'],
                #     'destination': self.old_doc_data['destination'],
                #     'compartment': self.old_doc_data['compartment'],
                # }
                qry_bookings_collection = {"market_combined":{"$in":data_list}}
            # print qry_bookings_collection
            st = time.time()
            # print "end ->", book_date_end
            qry_bookings_collection.update(
                {
                    'dep_date': {
                        '$gte': dep_date_start,
                        '$lte': dep_date_end
                    },
                    'trx_date': {
                        '$gte': book_date_start,
                        '$lt': book_date_end
                    }
                }
            )
            # print "time to update: ", time.time() - st
            # print qry_bookings_collection
            ppln_bookings = [
                {
                    '$match': qry_bookings_collection
                }
                ,
                {
                    '$group':
                        {
                            '_id': {"market": "$market_combined",
                                    "dep_date": "$dep_date"},
                            'bookings_ty': {'$sum': '$book_pax.value'},
                            'bookings_ly': {'$sum': '$book_pax.value_1'},
                            'capacity_ty': {'$max': '$inventory.capacity'},
                            'capacity_ly': {'$max': '$inventory.capacity_1'}
                        }
                },
                {
                    "$project":
                        {
                            "market": "$_id.market",
                            "dep_date": "$_id.dep_date",
                            "bookings_ty": 1,
                            "bookings_ly": 1,
                            "capacity_ty": 1,
                            "capacity_ly": 1,
                            "_id": 0
                        }
                }
            ]
            # print ">>>>>>>>>qu:", ppln_bookings
            st = time.time()
            data_crsr = db.JUP_DB_Manual_Triggers_Module.aggregate(ppln_bookings, allowDiskUse=True)
            data_crsr = list(data_crsr)
            if len(data_crsr) != 0:
                df = pd.DataFrame(data_crsr)
                df.fillna(0, inplace=True)

                #df['capacity_ly'].fillna(0, inplace=True)
                #df['capacity_ty'].fillna(0, inplace=True)
                df.loc[df['capacity_ly'] == 0, 'capacity_ly'] = df.loc[df['capacity_ly'] == 0, 'capacity_ty']
                df.loc[df['capacity_ty'] == 0, 'capacity_ty'] = df.loc[df['capacity_ty'] == 0, 'capacity_ly']
                df.loc[(df['capacity_ty'] == 0) & (df['capacity_ly'] == 0), ['capacity_ty', 'capacity_ly']] = 1

                df['bookings_ty_exp'] = df['bookings_ty'] * (df['capacity_ly'] / df['capacity_ty'])
                df = df.groupby(by=['market'], as_index=False)[
                    ['bookings_ty',
                     'bookings_ty_exp',
                     'bookings_ly',
                     'capacity_ty',
                     'capacity_ly']].sum()
                df = df_main.merge(df, on='market', how='left')
                df.fillna(0, inplace=True)
            else:
                df = pd.DataFrame()
                df['market'] = data_list
                df['bookings_ty_exp'] = 0
                df['bookings_ty'] = 0
                df['bookings_ly'] = 0
                df['capacity_ty'] = 1
                df['capacity_ly'] = 1
            # print df.head()
            # print "time to aggregate: ", time.time() - st
            # st = time.time()
            # if temp_collection_name in db.collection_names():
            #     bookings_crsr = db[temp_collection_name].find(
            #         {}
            #         ,
            #         {
            #             'bookings_ty': 1,
            #             'bookings_ly': 1
            #         }
            #     )
            #     bookings_data = list(bookings_crsr)
            #     # print ">>>>>>>>>>>>>> bookings_data: ", bookings, bookings_ly
            #     db[temp_collection_name].drop()
            #     print bookings_data
            #     if len(bookings_data) == 1:
            #         bookings = bookings_data[0]['bookings_ty']
            #         bookings_ly = bookings_data[0]['bookings_ly']

                # if bookings == 0 and bookings_ly == 0:
                #     print "ERROR!!!"
                #     no_data_err = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                #                                           'jupiter_AI/triggers/data_level/MainClass.py method: get_bookings_val',
                #                                           get_arg_lists(inspect.currentframe()))
                #     no_data_err_desc = ''.join(['0 bookings for both this year and last year in the ',
                #                                 'JUP_DB_Booking_BookDate collection for the given query',
                #                                 ' ', str(qry_bookings_collection)])
                #     no_data_err.append_to_error_list(no_data_err_desc)
                #     no_data_err.write_error_logs(datetime.datetime.now())
            # else:
            #     no_temp_collection_err = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
            #                                                      'jupiter_AI/triggers/data_level/MainClass.py method: get_bookings_val',
            #                                                      get_arg_lists(inspect.currentframe()))
            #     no_temp_collection_err_desc = ''.join(['collection_name - ', temp_collection_name,
            #                                            ' Not Present in Database'])
            #     no_temp_collection_err.append_to_error_list(no_temp_collection_err_desc)
            #     no_temp_collection_err.write_error_logs(datetime.datetime.now())
        else:
            no_collection_err = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                        'jupiter_AI/triggers/data_level/MainClass.py method: get_bookings_val',
                                                        get_arg_lists(inspect.currentframe()))
            no_collection_err_desc = ''.join(['collection_name - ', 'JUP_DB_Booking_BookDate',
                                              ' Not Present in Database'])
            no_collection_err.append_to_error_list(no_collection_err_desc)
            no_collection_err.write_error_logs(datetime.datetime.now())
        # print "finding: ", time.time()-st
        if len(df[df['bookings_ty']==0])!=0 and len(df[df['bookings_ly']==0])!=0:
            # print "ERROR!!!"
            no_data_err = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                  'jupiter_AI/triggers/data_level/MainClass.py method: get_bookings_val',
                                                  get_arg_lists(inspect.currentframe()))
            no_data_err_desc = ''.join(['0 bookings for both this year and last year in the ',
                                        'JUP_DB_Booking_BookDate collection for the given query',
                                        ' ', str(qry_bookings_collection)])
            no_data_err.append_to_error_list(no_data_err_desc)
            no_data_err.write_error_logs(datetime.datetime.now())
        # return {
        #     'ty': bookings,
        #     'ly': bookings_ly
        # }
        # print " booking df-------<><><><><><><>", df
        return df

    @measure(JUPITER_LOGGER)
    def get_bookings_data(self, book_date_start, book_date_end, dep_date_start, dep_date_end, db, data_list,query=None):
        """
        :param book_date_start:
        :param book_date_end:
        :param dep_date_start:
        :param dep_date_end:
        :param query:
        :return:
        """
        import time
        st = time.time()

        final_df = self.get_bookings_val(book_date_start, book_date_end, dep_date_start, dep_date_end, db=db,
                                         data_list=data_list, query=query)
        # ty_data = ty_data.drop('bookings_ly', axis=1)
        # # print ty_data
        # book_date_start_ly = get_ly_val(book_date_start)
        # book_date_end_ly = get_ly_val(book_date_end)
        # dep_date_start_ly = get_ly_val(dep_date_start)
        # dep_date_end_ly = get_ly_val(dep_date_end)
        #
        # ly_data = self.get_bookings_val(book_date_start_ly,book_date_end_ly,dep_date_start_ly,dep_date_end_ly, data_list,query=query)
        # # if len(ly_data) != 0:
        # ly_data = ly_data.drop('bookings_ly', axis=1)
        # ly_data['bookings_ly'] = ly_data['bookings_ty']
        # ly_data.drop('bookings_ty', axis=1, inplace=True)
        # final_df = ty_data.merge(ly_data, on='market', how='left')

        # if ly data is not present then bookings_ly would be NAs so filling it with 0
        final_df.fillna(0, inplace=True)
        # else:
        #     ty_data['bookings_ly'] = 0
        #     final_df = ty_data.copy()
        # print "time to get bookings val: ", time.time() - st
        # return {
        #     'ty':ty_data['ty'],
        #     'ly':ly_data['ty']
        # }
        return final_df
    #   --------------------------------- METHOD TO GET CAPACITY DATA FOR AN OD -----------------------------------

    @measure(JUPITER_LOGGER)
    def build_capacity_query(self, dep_date_start, dep_date_end, data_list):
        """

        :param dep_date_start:
        :param dep_date_end:
        :return:
        """
        data_list = list(pd.Series(data_list).str.slice(3,9))
        qry_capacity = defaultdict(list)
        # if self.old_doc_data['origin'] and self.old_doc_data['destination']:
        qry_capacity['$and'].append({'od': {"$in": data_list}})
        qry_capacity['$and'].append({'dep_date':{'$gte': dep_date_start,
                                                 '$lte': dep_date_end}})
        return qry_capacity

    @staticmethod
    @measure(JUPITER_LOGGER)
    def generate_capacity_ppln(capacity_qry):
        """

        :param capacity_qry:
        :return:
        """
        capacity_ppln = [
            {
                '$match': capacity_qry
            }
            ,
            {
                '$sort':{'last_update_date':-1}
            }
            ,
            {
                '$group': {
                    '_id': {
                        'od': '$od',
                        'dep_date': '$dep_date'
                    },
                    'doc': {'$first': '$$ROOT'}
                }
            }
            # ,
            # {
            #     '$unwind': '$doc'
            # }
            ,
            {
                '$group':
                    {
                        '_id': {"od": "$doc.od"},
                        # 'capacity':
                        #     {
                        #         '$sum': '$doc.od_capacity'
                        #     },
                        'Y_capacity':
                            {
                                '$sum': '$doc.y_cap'
                            },
                        'J_capacity':
                            {
                                '$sum': '$doc.j_cap'
                            }
                    }
            }
            ,
            {
                '$project':
                    {
                        '_id': 0,
                        # 'capacity': '$capacity',
                        'y_capacity': '$Y_capacity',
                        'j_capacity': '$J_capacity',
                        'od': "$_id.od"
                    }
            }
        ]
        return capacity_ppln

    @measure(JUPITER_LOGGER)
    def get_capacity_data(self, dep_date_start, dep_date_end, data_list, db):
        """
        """
        df = pd.DataFrame(columns=['od'])
        df['market'] = data_list
        df['od'] = pd.Series(data_list).str.slice(3,9)
        df['y_compartment'] = 1
        df['j_compartment'] = 1
        df.loc[df['market'].str.slice(9) == "J", 'y_compartment'] = 0
        df.loc[df['market'].str.slice(9) == "Y", 'j_compartment'] = 0
        response = {
            'ty': 'NA',
            'ly': 'NA'
        }

        capacity_qry_ty = self.build_capacity_query(dep_date_start=dep_date_start,
                                                    dep_date_end=dep_date_end,
                                                    data_list=data_list)
        # print 'CAP Query ty', capacity_qry_ty
        ppln_capacity = self.generate_capacity_ppln(capacity_qry=capacity_qry_ty)

        capacity_crsr = db.JUP_DB_Host_OD_Capacity.aggregate(ppln_capacity, allowDiskUse=True)
        capacity_df = pd.DataFrame(list(capacity_crsr))
        # print "cap"
        # print capacity_df
        # print "df"
        # print df
        if len(capacity_df) != 0:
            capacity_df = df.merge(capacity_df, how='left', on='od')
            # print capacity_df
            capacity_df['capacity_ty'] = capacity_df['y_capacity'] * capacity_df['y_compartment'] + \
                                         capacity_df['j_capacity'] * capacity_df['j_compartment']
            capacity_df.fillna("NA", inplace=True)
        else:
            capacity_df = deepcopy(df)
            capacity_df['capacity_ty'] = "NA"
        capacity_df = capacity_df[['market', 'od', 'capacity_ty']]
        # print capacity_df
        # lst_capacity_data = list(capacity_crsr)
        # # print len(lst_capacity_data)
        # if len(lst_capacity_data) == 1:
        #     print self.old_doc_data
        #     try:
        #         if self.old_doc_data['compartment'] == 'Y':
        #             response['ty'] = lst_capacity_data[0]['y_capacity']
        #         elif self.old_doc_data['compartment'] == 'J':
        #             response['ty'] = lst_capacity_data[0]['j_capacity']
        #     except KeyError:
        #         response['ty'] = lst_capacity_data[0]['y_capacity'] + lst_capacity_data[0]['j_capacity']


        dep_date_start_ly = str(int(dep_date_start[:4]) - 1) + dep_date_start[4:]
        dep_date_end_ly = str(int(dep_date_end[:4]) - 1) + dep_date_end[4:]
        capacity_qry_ly = self.build_capacity_query(dep_date_start=dep_date_start_ly,
                                                    dep_date_end=dep_date_end_ly,
                                                    data_list=data_list)
        ppln_capacity_ly = self.generate_capacity_ppln(capacity_qry=capacity_qry_ly)
        capacity_crsr_ly = db.JUP_DB_Host_OD_Capacity.aggregate(ppln_capacity_ly, allowDiskUse=True)
        capacity_df_ly = pd.DataFrame(list(capacity_crsr_ly))
        # print "------->"
        # print capacity_df_ly
        # print df
        if len(capacity_df_ly) != 0:
            capacity_df_ly = df.merge(capacity_df_ly, on='od', how='left')
            capacity_df_ly['capacity_ly'] = capacity_df_ly['y_capacity'] * capacity_df_ly['y_compartment'] + \
                                         capacity_df_ly['j_capacity'] * capacity_df_ly['j_compartment']
            capacity_df_ly.fillna("NA", inplace=True)
        else:
            capacity_df_ly = deepcopy(df)
            capacity_df_ly['capacity_ly'] = "NA"
        capacity_df_ly = capacity_df_ly[['market', 'od', 'capacity_ly']]
        # print capacity_df_ly
        # lst_capacity_data_ly = list(capacity_crsr_ly)
        # if len(lst_capacity_data_ly) == 1:
        #     try:
        #         if self.old_doc_data['compartment'] == 'Y':
        #             response['ly'] = lst_capacity_data_ly[0]['y_capacity']
        #         elif self.old_doc_data['compartment'] == 'J':
        #             response['ly'] = lst_capacity_data_ly[0]['j_capacity']
        #     except KeyError:
        #         response['ly'] = lst_capacity_data[0]['y_capacity'] + lst_capacity_data[0]['j_capacity']
        capacity_df = capacity_df.merge(capacity_df_ly, how="left", on=['market', 'od'])
        # print "capacity df@m" , capacity_df.head()
        return capacity_df
    #   --------------------------------- FUNCTIONS TO GET DATA FROM SALES OR FLOWN -----------------------------------

    @measure(JUPITER_LOGGER)
    def build_qry_sales_or_flown(self,
                                 book_date_start,
                                 book_date_end,
                                 dep_date_start,
                                 dep_date_end,
                                 qry_inp):
        """
        Build the Query for either of sales or flown collections
        :return:
        """
        # print 'Query_inp',qry_inp
        # print 'depst', dep_date_start
        # print 'deped', dep_date_end
        qry_pax_collection = {
            'dep_date': {
                '$gte': dep_date_start,
                '$lte': dep_date_end
            }
            ,
            'trx_date': {
                # '$gte': book_date_start,
                '$lte': book_date_end
            }
        }
        qry_pax_collection.update(qry_inp)
        # print 'Query',qry_pax_collection
        return qry_pax_collection

    @measure(JUPITER_LOGGER)
    def generate_ppln_sales_or_flown(self,
                                     book_date_start,
                                     book_date_end,
                                     dep_date_start,
                                     dep_date_end,
                                     temp_collection_name,
                                     qry_sales_or_flown,
                                     is_sales=None):
        """

        :param temp_collection_name:
        :param book_date_start:
        :param book_date_end:
        :param dep_date_start:
        :param dep_date_end:
        :return:
        """
        if not qry_sales_or_flown:
            qry_sales_or_flown = dict()

        ppln_pax = [
            {
                '$match': qry_sales_or_flown
            }
            ,
            {
                '$group':
                    {
                        '_id': {"pos": "$pos.City",
                                "origin": "$origin.City",
                                "destination": "$destination.City",
                                "compartment": "$compartment.compartment"},
                        'pax_ty': {'$sum': '$sale_pax.value'},
                        'pax_ly': {'$sum': '$sale_pax.value_1'},
                        'revenue_ty': {'$sum': '$sale_revenue.value'},
                        'revenue_ly': {'$sum': '$sale_revenue.value_1'}
                    }
            }
            ,
            {
                "$project":
                    {
                        "_id":0,
                        "pax_ty": "$pax_ty",
                        "pax_ly": "$pax_ly",
                        "revenue_ty": "$revenue_ty",
                        "revenue_ly": "$revenue_ly",
                        "pos": "$_id.pos",
                        "origin": "$_id.origin",
                        "destination": "$_id.destination",
                        "compartment": "$_id.compartment"
                    }
            }
            # {
            #     '$out': temp_collection_name
            # }
        ]
        return ppln_pax

    @measure(JUPITER_LOGGER)
    def get_sales_flown_val(self,
                            book_date_start,
                            book_date_end,
                            dep_date_start,
                            dep_date_end,
                            data_list,
                            db,
                            today=SYSTEM_DATE,
                            parameter="revenue",
                            query=None,
                            is_vlyr=None):
        """
        :param parameter:
        :param book_date_start:
        :param book_date_end:
        :param dep_date_start:
        :param dep_date_end:
        :return:
        """
        # print today
        # regex_list = []
        # for i in data_list:
        #     # regex_list.append(re.compile("(^" + i + ")"))
        #     regex_list.append({"pos.City":i[0:3],
        #                        "origin.City": i[3:6],
        #                        "destination.City": i[6:9],
        #                       "compartment.compartment": i[-1]})
        if not query:
            query = dict()
            # if self.old_doc_data['pos']:
            #     query['pos'] = self.old_doc_data['pos']
            # if self.old_doc_data['origin']:
            #     query['origin'] = self.old_doc_data['origin']
            # if self.old_doc_data['destination']:
            #     query['destination'] = self.old_doc_data['destination']
            # if self.old_doc_data['compartment']:
            #     query['compartment'] = self.old_doc_data['compartment']
            query['market_combined'] = {"$in": data_list}

        # print book_date_start, book_date_end, dep_date_start,dep_date_end,
        # response = dict(ty=0, ly=0)
        response = pd.DataFrame()
        response['market'] = data_list
        response = response.sort_values(by='market')
        # print 'sys date', today
        if dep_date_start < today <= dep_date_end:
            qry_sales_or_flown = self.build_qry_sales_or_flown(qry_inp=query,
                                                               book_date_end=book_date_end,
                                                               book_date_start=book_date_start,
                                                               dep_date_start=today,
                                                               dep_date_end=dep_date_end)

            if is_vlyr:
                qry_sales_or_flown = {
                    'dep_date': {
                        '$gte': today,
                        '$lte': dep_date_end
                    }
                    ,
                    'trx_date': {
                        # '$gte': book_date_start,
                        '$lt': book_date_end
                    }
                }
                qry_sales_or_flown.update(query)
                sales_df = pd.DataFrame(columns='market')
                sales_df['market'] = data_list

                ppln_sales = [
                    {
                        '$match': qry_sales_or_flown
                    },
                    {
                        "$group":{
                            "_id": {
                                "pos": "$pos.City",
                                "origin": "$origin.City",
                                "destination": "$destination.City",
                                "compartment": "$compartment.compartment",
                                "dep_date": "$dep_date"
                            },
                            "pax_ty": {"$sum":"$sale_pax.value"},
                            "pax_ly": {"$sum": "$sale_pax.value_1"},
                            "revenue_ty": {"$sum": "$sale_revenue.value"},
                            "revenue_ly": {"$sum": "$sale_revenue.value_1"},
                            "capacity_ty": {"$max": "$inventory.capacity"},
                            "capacity_ly": {"$max": "$inventory.capacity_1"},
                        }
                    },
                    {
                        "$project":{
                            "pos": "$_id.pos",
                            "origin": "$_id.origin",
                            "destination": "$_id.destination",
                            "compartment": "$_id.compartment",
                            "pax_ty": "$pax_ty",
                            "pax_ly": "$pax_ly",
                            "revenue_ty": "$revenue_ty",
                            "revenue_ly": "$revenue_ly",
                            "capacity_ty": "$capacity_ty",
                            "capacity_ly": "$capacity_ly",
                            "dep_date": "$_id.dep_date"

                        }
                    }
                ]
                vlyr_crsr = db.JUP_DB_Manual_Triggers_Module.aggregate(ppln_sales, allowDiskUse=True)
                vlyr_crsr = list(vlyr_crsr)
                df = pd.DataFrame(vlyr_crsr)
                df['market'] = df['pos'] + df['origin'] + df['destination'] + df['compartment']
                df['capacity_ly'].fillna(0, inplace=True)
                df['capacity_ty'].fillna(0, inplace=True)
                df.loc[df['capacity_ly'] == 0, 'capacity_ly'] = df.loc[df['capacity_ly'] == 0, 'capacity_ty']
                df.loc[df['capacity_ty'] == 0, 'capacity_ty'] = df.loc[df['capacity_ty'] == 0, 'capacity_ly']
                df.loc[(df['capacity_ty'] == 0) & (df['capacity_ly']==0), ['capacity_ty', 'capacity_ly']] = 1
                sales_df = sales_df.merge(df, on='market', how='left')
                sales_df.fillna(0, inplace=True)
                sales_df['pax_ty'] = sales_df['pax_ty'] * (sales_df['capacity_ly']/sales_df['capacity_ty'])
                sales_df['revenue_ty'] = sales_df['revenue_ty'] * (sales_df['capacity_ly']/sales_df['capacity_ty'])
                sales_df = sales_df.groupby(by=['pos', 'origin', 'destination', 'compartment'], as_index=False)[['pax_ty',
                                                                                                                 'revenue_ty',
                                                                                                         'revenue_ly',
                                                                                                                 'pax_ly']].sum()

            else:
                qry_sales_or_flown = {
                    'dep_date': {
                        '$gte': today,
                        '$lte': dep_date_end
                    }
                    ,
                    'trx_date': {
                        '$gte': book_date_start,
                        '$lt': book_date_end
                    }
                }
                qry_sales_or_flown.update(query)
                ppln_sales = [
                    {
                        '$match': qry_sales_or_flown
                    }
                    # {   "$sort": {"trx_date":-1}}
                    ,
                    {
                        '$group':
                            {
                                '_id': {"pos": "$pos.City",
                                        "origin": "$origin.City",
                                        "destination": "$destination.City",
                                        "compartment": "$compartment.compartment"},
                                'pax_ty': {'$sum': '$sale_pax.value'},
                                'pax_ly': {'$sum': '$sale_pax.value_1'},
                                'revenue_ty': {'$sum': '$sale_revenue.value'},
                                'revenue_ly': {'$sum': '$sale_revenue.value_1'}
                            }
                    }
                    ,
                    {
                        "$project":
                            {
                                "_id": 0,
                                "pax_ty": "$pax_ty",
                                "pax_ly": "$pax_ly",
                                "revenue_ty": "$revenue_ty",
                                "revenue_ly": "$revenue_ly",
                                "pos": "$_id.pos",
                                "origin": "$_id.origin",
                                "destination": "$_id.destination",
                                "compartment": "$_id.compartment"
                            }
                    }
                    # {
                    #     '$out': temp_collection_name
                    # }
                ]


                sales_data = self.obtain_sales_or_flown_data_from_db(book_date_start=book_date_start,
                                                                     book_date_end=book_date_end,
                                                                     dep_date_start=today,
                                                                     dep_date_end=dep_date_end,
                                                                     collection_name='JUP_DB_Manual_Triggers_Module',
                                                                     parameter=parameter,
                                                                     db=db,
                                                                     qry_sales_or_flown=qry_sales_or_flown,
                                                                     data_list=data_list,
                                                                     ppln=ppln_sales)
                # print sales_data
                # today = datetime.datetime.strptime(today, "%Y-%m-%d") - datetime.timedelta(days=1)
                # today = today.strftime("%Y-%m-%d")
            qry_sales_or_flown = self.build_qry_sales_or_flown(qry_inp=query,
                                                               book_date_end=book_date_end,
                                                               book_date_start=book_date_start,
                                                               dep_date_start=dep_date_start,
                                                               dep_date_end=today)
            if is_vlyr:
                qry_sales_or_flown = {
                    'dep_date': {
                        '$gte': dep_date_start,
                        '$lt': today
                    }
                    ,
                    'trx_date': {
                        # '$gt': book_date_start,
                        '$lt': book_date_end
                    }
                }
                qry_sales_or_flown.update(query)
                flown_df = pd.DataFrame(columns=['market'])
                flown_df['market'] = data_list
                ppln_flown = [
                    {
                        '$match': qry_sales_or_flown
                    }
                    ,
                    {
                        '$group':{
                            "_id":{
                                       "pos": "$pos.City",
                                       "origin": "$origin.City",
                                       "destination": "$destination.City",
                                       "compartment": "$compartment.compartment",
                                       "dep_date": "$dep_date"
                                   },
                            'pax_ty': {'$sum': '$flown_pax.value'},
                            'pax_ly': {'$sum': '$flown_pax.value_1'},
                            'revenue_ty': {'$sum': '$flown_revenue.value'},
                            'revenue_ly': {'$sum': '$flown_revenue.value_1'},
                            "capacity_ty": {"$max": "$inventory.capacity"},
                            "capacity_ly": {"$max": "$inventory.capacity_1"},

                    }}
                    ,
                    {
                        "$project": {
                            "pos": "$_id.pos",
                            "origin": "$_id.origin",
                            "destination": "$_id.destination",
                            "compartment": "$_id.compartment",
                            "pax_ty": "$pax_ty",
                            "pax_ly": "$pax_ly",
                            "revenue_ty": "$revenue_ty",
                            "revenue_ly": "$revenue_ly",
                            "capacity_ty": "$capacity_ty",
                            "capacity_ly": "$capacity_ly",
                            "dep_date": "$_id.dep_date"

                        }
                    }
                ]
                vlyr_crsr = db.JUP_DB_Manual_Triggers_Module.aggregate(ppln_flown, allowDiskUse=True)
                vlyr_crsr = list(vlyr_crsr)
                df = pd.DataFrame(vlyr_crsr)
                df['market'] = df['pos'] + df['origin'] + df['destination'] + df['compartment']
                df['capacity_ly'].fillna(0, inplace=True)
                df['capacity_ty'].fillna(0, inplace=True)
                df.loc[df['capacity_ly'] == 0, 'capacity_ly'] = df.loc[df['capacity_ly'] == 0, 'capacity_ty']
                df.loc[df['capacity_ty'] == 0, 'capacity_ty'] = df.loc[df['capacity_ty'] == 0, 'capacity_ly']
                df.loc[(df['capacity_ty'] == 0) & (df['capacity_ly'] == 0), ['capacity_ty', 'capacity_ly']] = 1

                flown_df = flown_df.merge(df, on='market', how='left')
                flown_df.fillna(0, inplace=True)
                flown_df['pax_ty'] = flown_df['pax_ty'] * (flown_df['capacity_ly'] / flown_df['capacity_ty'])
                flown_df['revenue_ty'] = flown_df['revenue_ty'] * (flown_df['capacity_ly'] / flown_df['capacity_ty'])
                flown_df = flown_df.groupby(by=['pos', 'origin', 'destination', 'compartment'], as_index=False)[
                    ['pax_ty',
                     'revenue_ty',
                     'revenue_ly',
                     'pax_ly']].sum()

            else:
                qry_sales_or_flown = {
                    'dep_date': {
                        '$gte': dep_date_start,
                        '$lt': today
                    }
                    ,
                    'trx_date': {
                        '$gte': book_date_start,
                        '$lt': book_date_end
                    }
                }
                qry_sales_or_flown.update(query)
                ppln_flown = [
                    {
                        '$match': qry_sales_or_flown
                    }
                    ,
                    {
                        '$group':
                            {
                                '_id': {"pos": "$pos.City",
                                        "origin": "$origin.City",
                                        "destination": "$destination.City",
                                        "compartment": "$compartment.compartment"},
                                'pax_ty': {'$sum': '$flown_pax.value'},
                                'pax_ly': {'$sum': '$flown_pax.value_1'},
                                'revenue_ty': {'$sum': '$flown_revenue.value'},
                                'revenue_ly': {'$sum': '$flown_revenue.value_1'}
                            }
                    }
                    ,
                    {
                        "$project":
                            {
                                "_id": 0,
                                "pax_ty": "$pax_ty",
                                "pax_ly": "$pax_ly",
                                "revenue_ty": "$revenue_ty",
                                "revenue_ly": "$revenue_ly",
                                "pos": "$_id.pos",
                                "origin": "$_id.origin",
                                "destination": "$_id.destination",
                                "compartment": "$_id.compartment"
                            }
                    }
                    # {
                    #     '$out': temp_collection_name
                    # }
                ]


                flown_data = self.obtain_sales_or_flown_data_from_db(book_date_start=book_date_start,
                                                                     book_date_end=book_date_end,
                                                                     dep_date_start=dep_date_start,
                                                                     dep_date_end=today,
                                                                     collection_name='JUP_DB_Manual_Triggers_Module',
                                                                     parameter=parameter,
                                                                     db=db,
                                                                     qry_sales_or_flown=qry_sales_or_flown,
                                                                     data_list=data_list,
                                                                     ppln=ppln_flown)
            if is_vlyr:
                sales_df = sales_df.sort_values(by='market')
                flown_df = flown_df.sort_values(by='market')
                temp_df = sales_df.rename(columns={
                    parameter + '_ty': 'sale' + parameter + '_ty',
                    parameter + '_ly': 'sale' + parameter + '_ly',
                }).merge(flown_df.rename(columns={
                    parameter + '_ty': 'flown' + parameter + '_ty',
                    parameter + '_ly': 'flown' + parameter + '_ly',
                }), on='market', how='outer')
                temp_df['ty'] = temp_df['sale' + parameter + '_ty'] + temp_df['flown' + parameter + '_ty']
                temp_df['ly'] = temp_df['sale' + parameter + '_ly'] + temp_df['flown' + parameter + '_ly']
                # response['ty'] = sales_df[parameter + '_ty'] + flown_df[parameter + '_ty']
                # response['ly'] = sales_df[parameter + '_ly'] + flown_df[parameter + '_ly']
                # response = response.merge(temp_df, on='market', how='left')
            else:
                # print sales_data
                # print flown_data
                sales_data = sales_data.sort_values(by='market')
                flown_data = flown_data.sort_values(by='market')
                temp_df = sales_data.rename(columns={
                    parameter + '_ty': 'sale' + parameter + '_ty',
                    parameter + '_ly': 'sale' + parameter + '_ly',
                }).merge(flown_data.rename(columns={
                    parameter + '_ty': 'flown' + parameter + '_ty',
                    parameter + '_ly': 'flown' + parameter + '_ly',
                }), on='market', how='outer')
                temp_df['ty'] = temp_df['sale' + parameter + '_ty'] + temp_df['flown' + parameter + '_ty']
                temp_df['ly'] = temp_df['sale' + parameter + '_ly'] + temp_df['flown' + parameter + '_ly']
                # response['ty'] = sales_df[parameter + '_ty'] + flown_df[parameter + '_ty']
                # response['ly'] = sales_df[parameter + '_ly'] + flown_df[parameter + '_ly']
                # response = deepcopy(temp_df)
            response = response.merge(temp_df, on='market', how='left')
        elif today <= dep_date_start:
            qry_sales = self.build_qry_sales_or_flown(qry_inp=query,
                                                      book_date_end=book_date_end,
                                                      book_date_start=book_date_start,
                                                      dep_date_start=dep_date_start,
                                                      dep_date_end=dep_date_end)
            # print qry_sales
            if is_vlyr:
                qry_sales = {
                    'dep_date': {
                        '$gte': dep_date_start,
                        '$lte': dep_date_end
                    }
                    ,
                    'trx_date': {
                        # '$gte': book_date_start,
                        '$lt': book_date_end
                    }
                }
                qry_sales.update(query)
                sales_df = pd.DataFrame(columns=['market'])
                sales_df['market'] = data_list

                ppln_sales = [
                    {
                        '$match': qry_sales
                    },
                    {
                        "$group": {
                            "_id": {
                                "pos": "$pos.City",
                                "origin": "$origin.City",
                                "destination": "$destination.City",
                                "compartment": "$compartment.compartment",
                                "dep_date": "$dep_date"
                            },
                            "pax_ty": {"$sum": "$sale_pax.value"},
                            "pax_ly": {"$sum": "$sale_pax.value_1"},
                            "revenue_ty": {"$sum": "$sale_revenue.value"},
                            "revenue_ly": {"$sum": "$sale_revenue.value_1"},
                            "capacity_ty": {"$max": "$inventory.capacity"},
                            "capacity_ly": {"$max": "$inventory.capacity_1"},
                        }
                    },
                    {
                        "$project": {
                            "pos": "$_id.pos",
                            "origin": "$_id.origin",
                            "destination": "$_id.destination",
                            "compartment": "$_id.compartment",
                            "pax_ty": "$pax_ty",
                            "pax_ly": "$pax_ly",
                            "revenue_ty": "$revenue_ty",
                            "revenue_ly": "$revenue_ly",
                            "capacity_ty": "$capacity_ty",
                            "capacity_ly": "$capacity_ly",
                            "dep_date": "$_id.dep_date"

                        }
                    }
                ]
                vlyr_crsr = db.JUP_DB_Manual_Triggers_Module.aggregate(ppln_sales, allowDiskUse=True)
                vlyr_crsr = list(vlyr_crsr)
                df = pd.DataFrame(vlyr_crsr)
                df['market'] = df['pos'] + df['origin'] + df['destination'] + df['compartment']
                sales_df = sales_df.merge(df, on='market', how='left')
                sales_df.fillna(0, inplace=True)

                sales_df.loc[sales_df['capacity_ly'] == 0, 'capacity_ly'] = sales_df.loc[sales_df['capacity_ly'] == 0, 'capacity_ty']
                sales_df.loc[sales_df['capacity_ty'] == 0, 'capacity_ty'] = sales_df.loc[sales_df['capacity_ty'] == 0, 'capacity_ly']
                sales_df.loc[(sales_df['capacity_ty'] == 0) & (sales_df['capacity_ly'] == 0), ['capacity_ty', 'capacity_ly']] = 1

                # sales_df.to_csv("data_cal.csv", index=False)
                sales_df['pax_ty'] = sales_df['pax_ty'] * (sales_df['capacity_ly'] / sales_df['capacity_ty'])
                sales_df['revenue_ty'] = sales_df['revenue_ty'] * (sales_df['capacity_ly'] / sales_df['capacity_ty'])
                sales_df = sales_df.groupby(by=['pos', 'origin', 'destination', 'compartment', 'market'], as_index=False)[
                    ['pax_ty',
                     'revenue_ty',
                     'revenue_ly',
                     'pax_ly']].sum()
                sales_df = sales_df[[parameter+"_ty", parameter + "_ly", 'market']]
                # print sales_df
                sales_df = sales_df.rename(columns={parameter+"_ty":"ty", parameter+"_ly":"ly"})
                # response = deepcopy(sales_df)
                response = response.merge(sales_df, on='market', how='left')
            else:
                qry_sales = {
                    'dep_date': {
                        '$gte': dep_date_start,
                        '$lte': dep_date_end
                    }
                    ,
                    'trx_date': {
                        '$gte': book_date_start,
                        '$lt': book_date_end
                    }
                }
                qry_sales.update(query)
                ppln_sales = [
                    {
                        '$match': qry_sales
                    }
                    ,
                    {
                        '$group':
                            {
                                '_id': {"pos": "$pos.City",
                                        "origin": "$origin.City",
                                        "destination": "$destination.City",
                                        "compartment": "$compartment.compartment"},
                                'pax_ty': {'$sum': '$sale_pax.value'},
                                'pax_ly': {'$sum': '$sale_pax.value_1'},
                                'revenue_ty': {'$sum': '$sale_revenue.value'},
                                'revenue_ly': {'$sum': '$sale_revenue.value_1'}
                            }
                    }
                    ,
                    {
                        "$project":
                            {
                                "_id": 0,
                                "pax_ty": "$pax_ty",
                                "pax_ly": "$pax_ly",
                                "revenue_ty": "$revenue_ty",
                                "revenue_ly": "$revenue_ly",
                                "pos": "$_id.pos",
                                "origin": "$_id.origin",
                                "destination": "$_id.destination",
                                "compartment": "$_id.compartment"
                            }
                    }
                ]
                # print "------------------->"
                # print ppln_sales
                sales_data = self.obtain_sales_or_flown_data_from_db(book_date_start,
                                                                     book_date_end,
                                                                     dep_date_start,
                                                                     dep_date_end,
                                                                     db=db,
                                                                     collection_name='JUP_DB_Manual_Triggers_Module',
                                                                     parameter=parameter,
                                                                     qry_sales_or_flown=qry_sales,
                                                                     data_list=data_list,
                                                                     ppln=ppln_sales)
                sales_data.rename(index=str,
                                  columns={parameter + "_ty": "ty", parameter + "_ly": "ly"},
                                  inplace=True)

                # response = deepcopy(sales_data)
                response = response.merge(sales_data, on='market', how='left')
        elif today > dep_date_end:
            qry_flown = self.build_qry_sales_or_flown(qry_inp=query,
                                                      book_date_end=book_date_end,
                                                      book_date_start=book_date_start,
                                                      dep_date_start=dep_date_start,
                                                      dep_date_end=dep_date_end)
            # print 'FLOWN QUERY', qry_flown
            ppln_flown = [
                {
                    '$match': qry_flown
                }
                ,
                {
                    '$group':
                        {
                            '_id': {"pos": "$pos.City",
                                    "origin": "$origin.City",
                                    "destination": "$destination.City",
                                    "compartment": "$compartment.compartment"},
                            'pax_ty': {'$sum': '$flown_pax.value'},
                            'pax_ly': {'$sum': '$flown_pax.value_1'},
                            'revenue_ty': {'$sum': '$flown_revenue.value'},
                            'revenue_ly': {'$sum': '$flown_revenue.value_1'}
                        }
                }
                ,
                {
                    "$project":
                        {
                            "_id": 0,
                            "pax_ty": "$pax_ty",
                            "pax_ly": "$pax_ly",
                            "revenue_ty": "$revenue_ty",
                            "revenue_ly": "$revenue_ly",
                            "pos": "$_id.pos",
                            "origin": "$_id.origin",
                            "destination": "$_id.destination",
                            "compartment": "$_id.compartment"
                        }
                }
                # {
                #     '$out': temp_collection_name
                # }
            ]
            flown_data = self.obtain_sales_or_flown_data_from_db(book_date_start=book_date_start,
                                                                 book_date_end=book_date_end,
                                                                 dep_date_start=dep_date_start,
                                                                 dep_date_end=today,
                                                                 collection_name='JUP_DB_Manual_Triggers_Module',
                                                                 db=db,
                                                                 parameter=parameter,
                                                                 qry_sales_or_flown=qry_flown,
                                                                 data_list=data_list,
                                                                 ppln=ppln_flown)
            # print 'FLOWN QUERY', qry_flown
            flown_data.rename(index=str,
                              columns={parameter+"_ty": "ty", parameter+"_ly": "ly"},
                              inplace=True)
            # response = deepcopy(flown_data)
            response = response.merge(flown_data, on='market', how='left')
        return response

    @measure(JUPITER_LOGGER)
    def obtain_sales_or_flown_data_from_db(self,
                                           book_date_start,
                                           book_date_end,
                                           dep_date_start,
                                           dep_date_end,
                                           collection_name,
                                           db,
                                           data_list,
                                           parameter='revenue',
                                           ppln=None,
                                           qry_sales_or_flown=None):
        """
        Get pax data for this year and last year
        :param parameter:
        :param book_date_start:
        :param book_date_end:
        :param dep_date_start:
        :param dep_date_end:
        :param collection_name:
        :return:
        """
        df_main = pd.DataFrame(columns=['market'])
        df_main['market'] = data_list
        pax_ty = 0
        pax_ly = 0
        revenue_ty = 0
        revenue_ly = 0

        if collection_name in db.collection_names():

            temp_collection_name = gen_collection_name()


            ppln_pax = self.generate_ppln_sales_or_flown(book_date_start,
                                                         book_date_end,
                                                         dep_date_start,
                                                         dep_date_end,
                                                         temp_collection_name,
                                                         qry_sales_or_flown=qry_sales_or_flown)
            # print ppln
            crsr = db[collection_name].aggregate(ppln, allowDiskUse=True)
            df = pd.DataFrame(list(crsr))
            # print "df --->"
            # print df
            if len(df) != 0:
                df['market'] = df['pos'] + df['origin'] + df['destination'] + df['compartment']
                df = df_main.merge(df, how='left', on='market')
                df.fillna(0, inplace=True)
            else:
                df['market'] = data_list
                df['pax_ty'] = 0
                df['pax_ly'] = 0
                df['revenue_ty'] = 0
                df['revenue_ly'] = 0
            # print "--------->"
            # print df

            # if temp_collection_name in db.collection_names():
            #     pax_revenue_crsr = db[temp_collection_name].find(
            #         {}
            #         ,
            #         {
            #             'pax_ty': 1,
            #             'pax_ly': 1,
            #             'revenue_ty': 1,
            #             'revenue_ly': 1
            #         }
            #     )
            #     pax_revenue_data = list(pax_revenue_crsr)
            #     db[temp_collection_name].drop()
            #     if len(pax_revenue_data) == 1:
            #         pax_ty = pax_revenue_data[0]['pax_ty']
            #         pax_ly = pax_revenue_data[0]['pax_ly']
            #         revenue_ty = pax_revenue_data[0]['revenue_ty']
            #         revenue_ly = pax_revenue_data[0]['revenue_ly']

            if len(df[(df['pax_ty'] == 0) & (df['pax_ly'] == 0)]) != 0:
                no_data_err = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                      'jupiter_AI/triggers/data_level/MainClass.py method: obtain_sales_or_flown_data_from_db',
                                                      get_arg_lists(inspect.currentframe()))
                no_data_err_desc = ''.join(['0 pax for both this year and last year in the ',
                                            collection_name, ' collection for the given inputs'])
                no_data_err.append_to_error_list(no_data_err_desc)
                no_data_err.write_error_logs(datetime.datetime.now())
            # else:
            #     no_temp_collection_err = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
            #                                                      'jupiter_AI/triggers/data_level/MainClass.py method: obtain_target_data_from_db',
            #                                                      get_arg_lists(inspect.currentframe()))
            #     no_temp_collection_err_desc = ''.join(['collection_name - ', temp_collection_name,
            #                                            ' Not Present in Database'])
            #     no_temp_collection_err.append_to_error_list(no_temp_collection_err_desc)
            #     no_temp_collection_err.write_error_logs(datetime.datetime.now())
        else:
            no_collection_err = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                        'jupiter_AI/triggers/data_level/MainClass.py method: obtain_sales_or_flown_data_from_db',
                                                        get_arg_lists(inspect.currentframe()))
            no_collection_err_desc = ''.join(['collection_name - ', collection_name,
                                              ' Not Present in Database'])
            no_collection_err.append_to_error_list(no_collection_err_desc)
            no_collection_err.write_error_logs(datetime.datetime.now())
        if parameter == 'pax':
            # print 'DONE'
            return df[['pax_ty', 'pax_ly', 'market']]
        else:
            # return {
            #     'ty': revenue_ty,
            #     'ly': revenue_ly
            # }
            return df[['market', 'revenue_ty', 'revenue_ly']]

    @measure(JUPITER_LOGGER)
    def get_sales_flown_data(self,
                             book_date_start,
                             book_date_end,
                             dep_date_start,
                             dep_date_end,
                             data_list,
                             db,
                             parameter="revenue",
                             query=None,
                             is_vlyr=None):
        """
        :param book_date_start: 
        :param book_date_end: 
        :param dep_date_start: 
        :param dep_date_end: 
        :param parameter: 
        :param query: 
        :return: 
        """
        # print 'CURRENT DATES_flown', book_date_start, book_date_end, dep_date_start, dep_date_end
        # print "7"
        ty_data = self.get_sales_flown_val(
                             book_date_start,
                             book_date_end,
                             dep_date_start,
                             dep_date_end,
                             data_list,
                             db=db,
                             parameter=parameter,
                             query=query,
                             is_vlyr=is_vlyr)
        # print "10"
        book_date_start_ly = get_ly_val(book_date_start)
        book_date_end_ly = get_ly_val(book_date_end)
        dep_date_start_ly = get_ly_val(dep_date_start)
        dep_date_end_ly = get_ly_val(dep_date_end)
        today_ly = get_ly_val(SYSTEM_DATE)
        # print "9"
        # print is_vlyr
        # print 'PREVIOUS DATES_flown', book_date_start_ly, book_date_end_ly, dep_date_start_ly, dep_date_end_ly
        if is_vlyr:
            return ty_data
        else:
            # print "8"
            ty_data.drop('ly', axis=1, inplace=True)
            ly_data = self.get_sales_flown_val(
                                 book_date_start_ly,
                                 book_date_end_ly,
                                 dep_date_start_ly,
                                 dep_date_end_ly,
                                 db=db,
                                 data_list=data_list,
                                 today=today_ly,
                                 parameter=parameter,
                                 query=query)
            ly_data.drop('ly', axis=1, inplace=True)
            ly_data['ly'] = ly_data['ty']
            ly_data.drop('ty', axis=1, inplace=True)

            # return {
            #     'ty': ty_data['ty'],
            #     'ly': ly_data['ty']
            # }
            return ty_data.merge(ly_data, on='market', how='left')

    # --------------------------------- FUNCTIONS TO GET DATA FROM FORECAST ------------------------------------------

    @staticmethod
    @measure(JUPITER_LOGGER)
    def generate_ppln_forecast(forecast_query, temp_collection_name):
        """

        :return:
        """
        forecast_ppln = [
            {
                '$match': forecast_query
            }
            ,
            {
                '$group': {
                    '_id': {
                        'month': {'$substr':['$departureMonth', 4,-1 ]},
                        'year': {'$substr':['$departureMonth',0,4 ]}
                    }
                    ,
                    'pax': {'$sum': '$pax'},
                    'average_fare': {'$first': '$avgFare'}
                }
            }
            # ,
            # {
            #     '$out': temp_collection_name
            # }
        ]
        return forecast_ppln

    @measure(JUPITER_LOGGER)
    def build_forecast_query(self, start_month, start_year, end_month, end_year):
        """

        :return:
        """
        response = defaultdict(list)
        if self.old_doc_data['pos']:
            if self.old_doc_data['pos'] == 'DXB':
                response['$and'].append({'pos': {'$in':[self.old_doc_data['pos'],'UAE']}})
            else:
                response['$and'].append({'pos': self.old_doc_data['pos']})
        if self.old_doc_data['origin'] and self.old_doc_data['destination']:
            response['$and'].append({'od': self.old_doc_data['origin'].encode() + self.old_doc_data['destination'].encode()})
        # if self.old_doc_data['destination']:
        #     response['$and'].append({'destination': self.old_doc_data['destination'].encode()})
        if self.old_doc_data['compartment']:
            response['$and'].append({'compartment': self.old_doc_data['compartment'].encode()})

        # month_yr_combs = query_month_year_builder(stdm=start_month,
        #                                           stdy=start_year,
        #                                           endm=end_month,
        #                                           endy=end_year)
        #
        #
        # response['$and'].append({
        #     '$or': month_yr_combs
        # })
        start_yr_mnth_str = str(start_year) + get_str(month=start_month)
        end_yr_month_str = str(end_year) + get_str(month=end_month)
        response['$and'].append({
            'departureMonth': {
                '$gte': start_yr_mnth_str,
                '$lte': end_yr_month_str
            }
        })
        # print response
        # return dict(response)

    @measure(JUPITER_LOGGER)
    def obtain_forecast_data_from_db(self,
                                     dep_date_start,
                                     dep_date_end,
                                     db,
                                     parameter='revenue'):
        """

        :return:
        """
        dep_date_start_obj = datetime.datetime.strptime(
            dep_date_start,
            '%Y-%m-%d'
        )
        dep_date_end_obj = datetime.datetime.strptime(
            dep_date_end,
            '%Y-%m-%d'
        )
        sd = dep_date_start_obj.day
        sm = dep_date_start_obj.month
        sy = dep_date_start_obj.year
        ed = dep_date_end_obj.day
        em = dep_date_end_obj.month
        ey = dep_date_end_obj.year

        forecast_query = self.build_forecast_query(start_month=sm,
                                                   start_year=sy,
                                                   end_month=em,
                                                   end_year=ey)

        temp_collection_name = gen_collection_name()

        forecast_ppln = self.generate_ppln_forecast(forecast_query=forecast_query,
                                                    temp_collection_name=temp_collection_name)

        forecast_crsr = db.JUP_DB_Forecast_OD.aggregate(forecast_ppln, allowDiskUse=True)
        # forecast_crsr = db[temp_collection_name].find()
        forecast_crsr = list(forecast_crsr)
        forecast_data = defaultdict(list)
        if len(forecast_crsr) != 0:
            for doc in forecast_crsr:
                lst_data = [doc['pax'], doc['average_fare']]
                forecast_data[(int(doc['_id']['month']), int(doc['_id']['year']))] = lst_data
        # db[temp_collection_name].drop()
        # print 'forecast', forecast_data
        if forecast_data:
            for month_year in forecast_data.keys():
                no_of_days = get_no_of_days(month=int(month_year[0]), year=int(month_year[1]))
                # print 'No of Days', no_of_days
                forecast_data[month_year].append(no_of_days)
                forecast_data[month_year].append(forecast_data[month_year][0] / no_of_days)
                # print 'fcst', forecast_data
            if forecast_data.keys()[0] == (sm, sy):
                forecast_data[(sm, sy)][2] = forecast_data[(sm, sy)][2] - sd + 1
            if forecast_data.keys()[-1] == (em, ey):
                forecast_data[(em, ey)][2] = ed
            if sm == em and sy == ey:
                forecast_data[(sm, sy)][2] = forecast_data[(sm, sy)][2] - sd + 1
            # print forecast_data
            # target_data is in the following format now
            # target_data[(1,2016)] = (100,25.34, 31,3.22558)
            if parameter == 'pax':
                forecast_pax = 0
                if forecast_data.keys():
                    for month_year in forecast_data.keys():
                        val = forecast_data[month_year]
                        forecast_pax += val[2] * val[3]
                    forecast_pax = int(math.ceil(forecast_pax))

                return forecast_pax
            else:
                forecast_revenue = 0
                if forecast_data.keys():
                    for month_year in forecast_data.keys():
                        data = forecast_data[month_year]
                        forecast_revenue += data[1] * (data[2] * data[3])
                    return forecast_revenue
        else:
            return 0

    @measure(JUPITER_LOGGER)
    def get_forecast_data(self, book_date_start, book_date_end, dep_date_start, dep_date_end, db, parameter='revenue'):
        """
        Get the Forecast Pax from JUP_DB_Forecast_OD b/w the departure date range
        :param book_date_start:
        :param book_date_end:
        :param parameter:
        :param dep_date_start:
        :param dep_date_end:
        :return:
        """
        forecast_val = 0
        if dep_date_start < SYSTEM_DATE <= dep_date_end:
            forecast_contribution = self.obtain_forecast_data_from_db(dep_date_start=SYSTEM_DATE,
                                                                      dep_date_end=dep_date_end,
                                                                      db=db,
                                                                      parameter=parameter)
            # print 'fcst contri', forecast_contribution
            flown_data = self.obtain_sales_or_flown_data_from_db(book_date_start=book_date_start,
                                                                 book_date_end=book_date_end,
                                                                 dep_date_start=dep_date_start,
                                                                 dep_date_end=SYSTEM_DATE,
                                                                 db=db,
                                                                 parameter=parameter,
                                                                 collection_name='JUP_DB_Sales_Flown',
                                                                 qry_sales_or_flown={
                                                                     'pos': self.old_doc_data['pos'],
                                                                     'origin': self.old_doc_data['origin'],
                                                                     'destination': self.old_doc_data['destination'],
                                                                     'compartment': self.old_doc_data['compartment']
                                                                 })
            # print 'flown contri', flown_data
            flown_contribution = flown_data['ty']

            forecast_val = forecast_contribution + flown_contribution
        elif SYSTEM_DATE <= dep_date_start:
            forecast_val = self.obtain_forecast_data_from_db(dep_date_start=dep_date_start,
                                                             dep_date_end=dep_date_end,
                                                             db=db,
                                                             parameter=parameter)
            # print forecast_val
        else:
            flown_data = self.obtain_sales_or_flown_data_from_db(book_date_start=book_date_start,
                                                                 book_date_end=book_date_end,
                                                                 dep_date_start=dep_date_start,
                                                                 dep_date_end=SYSTEM_DATE,
                                                                 db=db,
                                                                 parameter=parameter,
                                                                 collection_name='JUP_DB_Sales_Flown')
            # print flown_data
            forecast_val = flown_data['ty']
        return forecast_val

    @measure(JUPITER_LOGGER)
    def forecast_pax_tbc(self, dep_date_start, dep_date_end):
        """

        :rtype: int
        :return: the forecast for the market in consideration in the Object and  b/w the range of departure dates
        """
        current_date = str(datetime.date.today())
        if dep_date_start < current_date < dep_date_end:
            flown_contribution = self.get_flown_pax(dep_date_start=dep_date_start,
                                                    dep_date_end=current_date)
            forecast_contribution = self.get_forecast_pax(dep_date_start=current_date,
                                                          dep_date_end=dep_date_end)
            pax_tbc = flown_contribution + forecast_contribution
        elif current_date < dep_date_start:
            pax_tbc = self.get_forecast_pax(dep_date_start=dep_date_start,
                                            dep_date_end=dep_date_end)

    @measure(JUPITER_LOGGER)
    def get_forecast_values(self, dep_date_start, dep_date_end, db, data_list, parameter):
        """
        Fetch forecast values for VTGT triggers
        :param dep_date_start:
        :param dep_date_end:
        :return:
        """
        start_month = dep_date_start[0:4] + dep_date_start[5:7]
        end_month = dep_date_end[0:4] + dep_date_end[5:7]
        sm = int(dep_date_start[5:7])
        em = int(dep_date_end[5:7])
        sy = int(dep_date_start[0:4])
        ey = int(dep_date_end[0:4])
        sd = int(dep_date_start[8:10])
        ed = int(dep_date_end[8:10])
        print dep_date_start, dep_date_end

        if 'JUP_DB_Forecast_OD' in db.collection_names():
            latest_snap = max(db.JUP_DB_Forecast_OD.distinct('snap_date'))
            crsr = db.JUP_DB_Forecast_OD.aggregate([
                {
                    '$match': {
                        'departureMonth': {'$gte': start_month, '$lte': end_month},
                        'market_combined': {'$in': data_list},
                        'snap_date': latest_snap
                    }
                },
                {
                    '$group': {
                        '_id': {
                            'market': '$market_combined',
                            'month': '$Month',
                            'year': '$Year'
                        },
                        'docs': {'$first': '$$ROOT'}
                    }
                },
                {
                    '$project': {
                        'market': '$_id.market',
                        'month': '$_id.month',
                        'year': '$_id.year',
                        'pax': '$docs.pax',
                        'revenue': '$docs.revenue',
                        '_id': 0
                    }
                }
            ], allowDiskUse=True)
            df = pd.DataFrame(list(crsr))
            if len(df) > 0:
                df['month'] = df['month'].astype('int')
                df['year'] = df['year'].astype('int')
                df['no_of_days'] = df.apply(
                    lambda row: get_no_of_days(int(row['month']), int(row['year'])), axis=1)
                df['pax_per_day'] = df['pax'] / df['no_of_days']
                df['revenue_per_day'] = df['revenue'] / df['no_of_days']
                df.loc[(df['month'] == sm) & (df['year'] == sy),
                       'no_of_days'] = df.loc[(df['month'] == sm) & (df['year'] == sy),
                                              'no_of_days'] - sd + 1

                df.loc[(df['month'] == em) & (df['year'] == ey), 'no_of_days'] = ed
                if sm == em and sy == ey:
                    df['no_of_days'] = ed - sd + 1
                # print df.head()
            else:
                df['market'] = data_list
                df['no_of_days'] = 0
                df['pax_per_day'] = 0
                df['revenue_per_day'] = 0

            if parameter == 'pax':
                df['forecast_pax'] = df['no_of_days'] * df['pax_per_day']
                grouped_df = df.groupby(by='market', as_index=False)['forecast_pax'].sum()

                return grouped_df
            else:
                df['forecast_revenue'] = df['no_of_days'] * df['revenue_per_day']
                grouped_df = df.groupby(by='market', as_index=False)['forecast_revenue'].sum()

                return grouped_df

        else:
            no_collection_err = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                        'jupiter_AI/triggers/data_level/MainClass.py method: get_forecast_values',
                                                        get_arg_lists(inspect.currentframe()))
            no_collection_err_desc = ''.join(['collection_name - JUP_DB_Forecast_OD',
                                              ' Not Present in Database'])
            no_collection_err.append_to_error_list(no_collection_err_desc)
            no_collection_err.write_error_logs(datetime.datetime.now())

    @measure(JUPITER_LOGGER)
    def get_forecast_yield_data(self, dep_date_start, dep_date_end, db, data_list):
        """
        """
        forecast_pax = self.get_forecast_values(dep_date_start=dep_date_start, dep_date_end=dep_date_end, db=db, parameter='pax',
                                          data_list=data_list)
        forecast_revenue = self.get_forecast_values(dep_date_start=dep_date_start, dep_date_end=dep_date_end, db=db,
                                              parameter='revenue', data_list=data_list)
        distance = self.get_od_distance(origin=self.old_doc_data['origin'],
                                        destination=self.old_doc_data['destination'],
                                        data_list=data_list,
                                        db=db,
                                        is_data_list=1)
        # print "target_pax: "
        # print target_pax
        # print "target_revenue: "
        # print target_revenue
        # print "distance"
        # print distance
        distance.drop("_id", axis=1, inplace=True)
        forecast_pax = forecast_pax.merge(forecast_revenue, how='left', on='market')
        forecast_pax['od'] = forecast_pax['market'].str.slice(3, 9)
        forecast_pax = forecast_pax.merge(distance, how='left', on='od')
        # if type(target_pax) in [int, float] and type(target_revenue) in [int,float] and type(distance) in [int,float] and target_pax > 0:
        #     target_yield = target_revenue * 100 / (target_pax*distance)
        #     return target_yield
        # else:
        #     return None
        forecast_pax['forecast_yield'] = forecast_pax['forecast_revenue'] * 100 / (
                forecast_pax['forecast_pax'] * forecast_pax['distance'])
        forecast_pax.fillna(0, inplace=True)
        return forecast_pax

    # --------------------------------- FUNCTIONS TO GET DATA FROM TARGET ------------------------------------------

    @measure(JUPITER_LOGGER)
    def build_target_query(self, dep_date_start, dep_date_end, data_list, db):
        """
        Generate the query to get the target data
        :return: dict
        """
        response = defaultdict(list)
        response["$and"].append({"combine_pos": {"$in": data_list}})
        # if self.old_doc_data['pos']:
        #     if self.old_doc_data['pos'] == 'DXB':
        #         response['$and'].append({'pos': {'$in':[self.old_doc_data['pos'],'UAE']}})
        #     else:
        #         response['$and'].append({'pos': self.old_doc_data['pos']})
        # if self.old_doc_data['origin']:
        #     response['$and'].append({'origin': self.old_doc_data['origin'].encode()})
        # if self.old_doc_data['destination']:
        #     response['$and'].append({'destination': self.old_doc_data['destination'].encode()})
        # if self.old_doc_data['compartment']:
        #     response['$and'].append({'compartment': self.old_doc_data['compartment'].encode()})

        start = datetime.datetime.strptime(dep_date_start, '%Y-%m-%d')
        end = datetime.datetime.strptime(dep_date_end, '%Y-%m-%d')

        months = (end.year - start.year) * 12 + end.month + 1
        li = []
        for i in xrange(start.month, months):
            year = (i - 1) / 12 + start.year
            month = (i - 1) % 12 + 1
            month_year = str(year) + format(month, '02d')
            li.append(month_year)
        # month_yr_combs = query_month_year_builder(stdm=start_month,
        #                                           stdy=start_year,
        #                                           endm=end_month,
        #                                           endy=end_year)

        response['$and'].append({
            'departureMonth': {'$in': li}
        })
        latest_snap = max(db.JUP_DB_Target_OD.distinct('snap_date'))
        response['$and'].append({'snap_date': latest_snap})

        return dict(response)

    @staticmethod
    @measure(JUPITER_LOGGER)
    def obtain_target_data_from_db(target_query, data_list, db):
        """
        Func to return target data
        :param target_query:
        :return: if data present a list of tuples (month val, year val, target pax val)
        """
        # target_data = defaultdict(list)
        target_data = pd.DataFrame(columns=['market'])
        target_data['market'] = data_list
        if 'JUP_DB_Target_OD' in db.collection_names():
            temp_collection_name = gen_collection_name()
            # print target_query
            target_ppln = [
                {
                    '$match': target_query
                }
                ,
                {
                    '$group': {
                        '_id': {
                            'month': '$month',
                            'year': '$year',
                            'market': "$combine_pos"
                        }
                        ,
                        'pax': {'$first': '$pax'},
                        'revenue': {'$first': '$revenue'}
                    }
                }
                ,
                {
                    '$project': {
                        "_id": 0,
                        "market": "$_id.market",
                        "year": "$_id.year",
                        "month": "$_id.month",
                        "pax": "$pax",
                        "revenue": "$revenue"
                    }
                }
                # ,{
                # "$out": temp_collection_name
                #           }
            ]
            # print "target_ppln ------------>"
            # print target_ppln
            # db.JUP_DB_Target_OD.aggregate(target_ppln, allowDiskUse=True)
            target_crsr = db.JUP_DB_Target_OD.aggregate(target_ppln, allowDiskUse=True)
            target_crsr = list(target_crsr)
            if len(target_crsr) != 0:
                df = pd.DataFrame(target_crsr)
                df = target_data.merge(df, how='left', on='market')
                df.fillna(0, inplace=True)
                return df[df['month'] != 0]
            else:
                return pd.DataFrame()
            # if temp_collection_name in db.collection_names():
            #     target_crsr = db[temp_collection_name].find({})
            #     lst_target_data = list(target_crsr)
            #     db[temp_collection_name].drop()
            #     if len(lst_target_data) > 0:
            #         for doc in lst_target_data:
            #             lst_data = [doc['pax'], doc['revenue']]
            #             target_data[(int(doc['_id']['month']), int(doc['_id']['year']))] = lst_data
            #     print target_data
            # else:
            #     no_temp_collection_err = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
            #                                                      'jupiter_AI/triggers/data_level/MainClass.py method: obtain_target_data_from_db',
            #                                                      get_arg_lists(inspect.currentframe()))
            #     no_temp_collection_err_desc = ''.join(['collection_name - ', temp_collection_name,
            #                                            ' Not Present in Database'])
            #     no_temp_collection_err.append_to_error_list(no_temp_collection_err_desc)
            #     no_temp_collection_err.write_error_logs(datetime.datetime.now())

        else:
            no_collection_err = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                        'jupiter_AI/triggers/data_level/MainClass.py method: obtain_target_data_from_db',
                                                        get_arg_lists(inspect.currentframe()))
            no_collection_err_desc = ''.join(['collection_name - JUP_DB_Target_OD',
                                              ' Not Present in Database'])
            no_collection_err.append_to_error_list(no_collection_err_desc)
            no_collection_err.write_error_logs(datetime.datetime.now())


    @measure(JUPITER_LOGGER)
    def get_target_data(self, dep_date_start, dep_date_end, data_list, db, parameter='revenue'):
        """

        :param parameter:
        :param dep_date_start: date in '%Y-%m-%d' format
        :param dep_date_end: date in '%Y-%m-%d' format
        :return: the target for the market in consideration in the Object and b/w the range of departure dates
        """
        dep_date_start_obj = datetime.datetime.strptime(
            dep_date_start,
            '%Y-%m-%d'
        )
        dep_date_end_obj = datetime.datetime.strptime(
            dep_date_end,
            '%Y-%m-%d'
        )
        sd = dep_date_start_obj.day
        sm = dep_date_start_obj.month
        sy = dep_date_start_obj.year
        ed = dep_date_end_obj.day
        em = dep_date_end_obj.month
        ey = dep_date_end_obj.year

        target_query = self.build_target_query(dep_date_start=dep_date_start,
                                               dep_date_end=dep_date_end,
                                               db=db,
                                               data_list=data_list)
        target_data = self.obtain_target_data_from_db(target_query, data_list, db=db)
        # print "target_data --------->"
        # print target_data.head()

        if len(target_data) != 0:
            target_data['no_of_days'] = target_data.apply(lambda row: get_no_of_days(int(row['month']), int(row['year'])), axis=1)
            target_data['pax_per_day'] = target_data['pax']/target_data['no_of_days']
            target_data['revenue_per_day'] = target_data['revenue'] / target_data['no_of_days']
            # for month_year in target_data.keys():
            #     no_of_days = get_no_of_days(month=int(month_year[0]), year=int(month_year[1]))
            #     target_data[month_year].append(no_of_days)
            #     target_data[month_year].append(target_data[month_year][0] / float(no_of_days))
            #     target_data[month_year].append(target_data[month_year][1] / float(no_of_days))
            # print "target data ------->"
            # print target_data.head()
            target_data.loc[(target_data['month'] == sm) &
                        (target_data['year'] == sy), 'no_of_days'] = target_data.loc[(target_data['month'] == sm) &
                                                                                 (target_data['year'] == sy),
                                                                                 'no_of_days'] - sd + 1

            target_data.loc[(target_data['month'] == em) &
                        (target_data['year'] == ey), 'no_of_days'] = ed


            # if target_data.keys()[0] == (sm, sy):
            #     target_data[(sm, sy)][2] = target_data[(sm, sy)][2] - sd + 1
            # if target_data.keys()[-1] == (em, ey):
            #     target_data[(em, ey)][2] = ed
            if sm == em and sy == ey:
                target_data['no_of_days'] = ed - sd + 1
        else:
            target_data['market'] = data_list
            target_data['no_of_days'] = 0
            target_data['pax_per_day'] = 0
            target_data['revenue_per_day'] = 0
        # print target_data
        # target_data is in the following format now
        # target_data[(1,2016)] = (100,10000,31,3.22558,322.558)
        if parameter == 'pax':
            target_data['target_pax'] = target_data['no_of_days'] * target_data['pax_per_day']
            grouped_df = target_data.groupby(by='market', as_index=False)['target_pax'].sum()
            # target_pax = 0
            # for month_year in target_data.keys():
            #     val = target_data[month_year]
            #     target_pax += val[2] * val[3]
            # target_pax = math.ceil(target_pax)
            return grouped_df
        else:
            target_data['target_revenue'] = target_data['no_of_days'] * target_data['revenue_per_day']
            grouped_df = target_data.groupby(by='market', as_index=False)['target_revenue'].sum()
            # target_revenue = 0
            # for month_year in target_data.keys():
            #     val = target_data[month_year]
            #     target_revenue += val[2] * val[4]
            return grouped_df

    # --------------------------------- FUNCTION TO GET YIELD DATA ------------------------------------------------

    @measure(JUPITER_LOGGER)
    def get_yield_data(self, book_date_start, book_date_end, dep_date_start, dep_date_end, db, data_list, is_vlyr=None):
        """

        :param book_date_start:
        :param book_end_date:
        :param dep_date_start:
        :param dep_date_end:
        :return:
        """
        # print "IN"
        pax_data = self.get_sales_flown_data(book_date_start=book_date_start,
                                             book_date_end=book_date_end,
                                             dep_date_start=dep_date_start,
                                             dep_date_end=dep_date_end,
                                             db=db,
                                             parameter='pax',
                                             data_list=data_list,
                                             is_vlyr=is_vlyr)

        revenue_data = self.get_sales_flown_data(book_date_start=book_date_start,
                                                 book_date_end=book_date_end,
                                                 dep_date_start=dep_date_start,
                                                 dep_date_end=dep_date_end,
                                                 db=db,
                                                 parameter='revenue',
                                                 data_list=data_list,
                                                 is_vlyr=is_vlyr)

        od_distance = self.get_od_distance(origin=self.old_doc_data['origin'],
                                           destination=self.old_doc_data['destination'],
                                           data_list=data_list,
                                           db=db,
                                           is_data_list=1)

        # pax_data['od'] = pax_data['market'].str.slice(3,9)
        revenue_data['od'] = revenue_data['market'].str.slice(3, 9)
        revenue_data = revenue_data.rename(columns={"ty":"revenue_ty",
                                                    "ly":"revenue_ly"})
        od_distance.drop("_id", axis=1, inplace=True)
        # print ">>>>>>>>>>PAX_DATA: "
        # print pax_data
        # print ">>>>>>>>>>OD_Distance: "
        # print od_distance
        # print ">>>>>Revenue:"
        # print revenue_data
        final_data = pax_data.merge(revenue_data, how='left', on='market')
        final_data = final_data.merge(od_distance, how='left', on='od')
        # print "finalData"
        # print final_data
        # final_data = final_data[final_data.notnull()]
        # final_data['yield_ty'] = None
        # final_data['yield_ly'] = None
        final_data.loc[final_data['ty'] == 0, 'revenue_ty'] = 0
        final_data.loc[final_data['ly'] == 0, 'revenue_ly'] = 0
        final_data.loc[final_data['ty'] == 0, 'ty'] = 1
        final_data.loc[final_data['ly'] == 0, 'ly'] = 1
        final_data['yield_ty'] = final_data['revenue_ty'] * 100/(final_data['distance'] * final_data['ty'])
        final_data['yield_ly'] = final_data['revenue_ly'] * 100 / (final_data['distance'] * final_data['ly'])
        final_data.fillna("NA", inplace=True)
        # if pax_data['ty'] != 0 and od_distance:
        #     yield_ty = revenue_data['ty'] * 100/ float(od_distance * pax_data['ty'])
        # else:
        #     yield_ty = None
        #
        # if pax_data['ly'] != 0 and od_distance:
        #     yield_ly = revenue_data['ly'] * 100/ float(od_distance * pax_data['ly'])
        # else:
        #     yield_ly = None

        # return {
        #     'ty': yield_ty,
        #     'ly': yield_ly
        # }
        return final_data

    @measure(JUPITER_LOGGER)
    def get_target_yield_data(self, dep_date_start, dep_date_end, db, data_list):
        """ 
        """
        target_pax = self.get_target_data(dep_date_start=dep_date_start,dep_date_end=dep_date_end,parameter='pax', db=db, data_list=data_list)
        target_revenue = self.get_target_data(dep_date_start=dep_date_start,dep_date_end=dep_date_end,parameter='revenue', db=db, data_list=data_list)
        distance = self.get_od_distance(origin = self.old_doc_data['origin'],
                                        destination = self.old_doc_data['destination'],
                                        data_list=data_list,
                                        db=db,
                                        is_data_list=1)
        # print "target_pax: "
        # print target_pax
        # print "target_revenue: "
        # print target_revenue
        # print "distance"
        # print distance
        distance.drop("_id", axis=1, inplace=True)
        target_pax = target_pax.merge(target_revenue, how='left', on='market')
        target_pax['od'] = target_pax['market'].str.slice(3,9)
        target_pax = target_pax.merge(distance, how='left', on='od')
        # if type(target_pax) in [int, float] and type(target_revenue) in [int,float] and type(distance) in [int,float] and target_pax > 0:
        #     target_yield = target_revenue * 100 / (target_pax*distance)
        #     return target_yield
        # else:
        #     return None
        target_pax['target_yield'] = target_pax['target_revenue'] * 100/ (target_pax['target_pax']*target_pax['distance'])
        target_pax.fillna("NA", inplace=True)
        return target_pax
    #   --------------------------------- FUNCTION TO GET OD DISTANCE ------------------------------------------------

    @staticmethod
    @measure(JUPITER_LOGGER)
    def get_od_distance(origin, destination, db, unit='km', data_list=None, is_data_list=0):
        """

        :param unit:
        :param origin:
        :param destination:
        :return:
        """
        if is_data_list == 0:
            distance_crsr = db.JUP_DB_OD_Distance_Master.find({
                'od': origin + destination
            })
            if distance_crsr.count() == 1:
                if unit == 'km':
                    return distance_crsr[0]['distance']
        else:
            od_list = []
            for market in data_list:
                od_list.append(market[3:9])
            distance_crsr = db.JUP_DB_OD_Distance_Master.find({
                'od':{"$in":od_list}
            })
            return pd.DataFrame(list(distance_crsr))

    #   --------------------------------- FUNCTION TO GET OD DISTANCE ------------------------------------------------

    @measure(JUPITER_LOGGER)
    def get_market_share_data(self, book_date_start, book_date_end, dep_date_start, dep_date_end):
        """

        :param book_date_start:
        :param book_date_end:
        :param dep_date_start:
        :param dep_date_end:
        :return:
        """
        pass


@measure(JUPITER_LOGGER)
def get_pos_od_compartment_combinations(db):
    """
    Get all the pos od compartment combinations for which to check relevant data level triggers
    :return:
    """
    pos_od_comp_crsr = db.JUP_DB_Pos_OD_Mapping.aggregate(
        [
            {
                '$project': {
                    '_id': 0,
                    'pos': '$pos.City',
                    'origin': '$origin.City',
                    'destination': '$destination.City',
                    'compartment': '$compartment.compartment'
                }
            },
        ]
    )

    pos_od_comp_list = []
    for doc in pos_od_comp_crsr:
        if doc['pos'] and doc['origin'] and doc['destination'] and doc['compartment']:
            pos_od_comp_list.append((doc['pos'], doc['origin'], doc['destination'], doc['compartment']))
    # print 'No of POS OD COMPARTMENT Combinations -', len(pos_od_comp_list)
    return pos_od_comp_list


if __name__ == '__main__':
    client = mongo_client()
    db= client[JUPITER_DB]

    obj = DataLevel(system_date=SYSTEM_DATE,
                    data={
                        'origin': 'RUH',
                        'destination': 'DXB',
                        'compartment': 'Y',
                        'pos': 'DXB'
                    })
    # data_list = ["SJJSJJDXBY", "MCTDXBMCTY", "DXBRUHDXBY", "DXBDXBDOHY"]
    # # res = [{'$match': {'$and': [{'pos': {'$in': ['DXB', 'UAE']}}, {'origin': 'RUH'}, {'destination': 'DXB'}, {'compartment': 'Y'}, {'$or': [{'year': 2017, 'month': 5}]}]}}, {'$group': {'pax': {'$sum': '$pax'}, '_id': {'year': '$year', 'month': '$month'}, 'revenue': {'$sum': '$revenue'}}}, {'$out': 'JUP_Temp_sai krishna_2017-07-12T12:22:33.726190_8O121T55'}]
    # res = obj.build_target_query(5,2017, 6, 2017, data_list)
    # print res
    # res = obj.obtain_target_data_from_db(res,data_list)
    # print res
    # forecast = obj.get_forecast_data(book_date_start='2017-01-1900',
    #                                  book_date_end='2017-05-13',
    #                                  dep_date_start='2017-05-01',
    #                                  dep_date_end='2017-05-30',
    #                                  parameter='pax')
    # print forecast

    # forecast = obj.get_forecast_data(book_date_start='2017-01-1900',
    #                                  book_date_end='2017-05-13',
    #                                  dep_date_start='2017-05-01',
    #                                  dep_date_end='2017-05-30',
    #                                  parameter='revenue')
    # print forecast

    sales_data = obj.get_bookings_val(dep_date_start='2017-10-01',
                                          dep_date_end='2017-10-31',
                                          book_date_start="2017-10-03",
                                          book_date_end="2017-10-10",
                                          data_list=["CMBCMBRUHY"],
                                          db=db
                                          )
    print sales_data

    # capacity = obj.get_capacity_data(dep_date_start='2017-05-01',dep_date_end='2017-05-31')
    # print capacity
    client.close()