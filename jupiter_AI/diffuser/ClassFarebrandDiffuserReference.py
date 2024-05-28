#######################################################
## Author: Prem
## First completeion date: April 2 2017
#######################################################
import jupiter_AI.common.ClassErrorObject as error_class
import datetime
import json
import time
import jupiter_AI.network_level_params as net
import inspect
import collections
import copy
import ClassDiffuserReference as diffuser_reference_class
##from jupiter_AI.network_level_params import Jupiterdb

import pymongo
from bson.objectid import ObjectId 
from datetime import datetime
from time import gmtime, strftime

#import scipy.stats as ss

start_time = time.time()
# print start_time
MONGO_CLIENT_URL = '13.92.251.7:42525'
#net.MONGO_CLI# ENT_URL
ANALYTICS_MONGO_USERNAME = 'analytics'
ANALYTICS_MONGO_PASSWORD = 'KNjSZmiaNUGLmS0Bv2'
JUPITER_DB = 'fzDB'
client = pymongo.MongoClient(MONGO_CLIENT_URL)
client.the_database.authenticate(ANALYTICS_MONGO_USERNAME, ANALYTICS_MONGO_PASSWORD, source='admin')
db = client[JUPITER_DB]
obj_err_main = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, "main", "")
obj_err_main.append_to_error_list("main error level")
DICT_ERROR_ODS={}
IMPUTED_RATING=5
MIN_RECORD_LENGTH=60
TOLERANCE=0.001         ## error tolerance
COLLECTION = 'JUP_DB_Capacity_1'


# print("--- %s seconds ---" % (time.time() - start_time))

class farebrand_diffuser_reference(diffuser_reference_class.diffuser_reference):
    def __init__(self, pos, origin, destination, compartment, ow_rt_flag, country, region):
        self.pos=pos
        self.origin=origin
        self.destination=destination
        self.compartment=compartment
        self.ow_rt_flag=ow_rt_flag
        self.country=country
        self.region=region
        self.dict_diffuser_ref_table    = self.get_diffuser_reference_table_dict_of_dicts()
        
    def __str__(self):
        return str(self.dict_diffuser_ref_table)
        
    def get_diffuser_ordering_numbers(self):
        list_diffuser_ordering_numbers=[]
        for dict_key in self.dict_diffuser_ref_table:
            list_diffuser_ordering_numbers.append(self.dict_diffuser_ref_table[dict_key]['farebrand_number'])
        return sorted(list_diffuser_ordering_numbers)
    
    def get_ordering_name(self, diffuser_ordering_number):
        return self.dict_diffuser_ref_table[diffuser_ordering_number]['farebrand']

    def get_diffuser_row_dict(self, i):
        return self.dict_diffuser_ref_table[i]


###############@@@@@@@@@@@@@@@@@@@@@@@@@@

    def get_diffuser_reference_table_dict_of_dicts(self):
        '''
        get diffuser table from most granular to least granular level
        '''
        # print 'came here'
        dict_diffuser_ref_table=self.get_diffuser_reference_table_od_level_dict_of_dicts()
        if len(dict_diffuser_ref_table)>0:
            return dict_diffuser_ref_table
        
        dict_diffuser_ref_table=self.get_diffuser_reference_table_pos_level_dict_of_dicts()
        if len(dict_diffuser_ref_table)>0:
            return dict_diffuser_ref_table
        
        dict_diffuser_ref_table=self.get_diffuser_reference_table_country_level_dict_of_dicts()
        if len(dict_diffuser_ref_table)>0:
            return dict_diffuser_ref_table
        
        dict_diffuser_ref_table=self.get_diffuser_reference_table_region_level_dict_of_dicts()
        if len(dict_diffuser_ref_table)>0:
            return dict_diffuser_ref_table
        
        dict_diffuser_ref_table=self.get_diffuser_reference_table_network_level_dict_of_dicts()
        # if len(dict_diffuser_ref_table)==0:
        #     e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
        #     e.append_to_error_list("reference table for channel diffuser doesn't exist")
        #     raise e
        return dict_diffuser_ref_table
    

    def get_diffuser_reference_table_od_level_dict_of_dicts(self):
        query={
            # 'pos': self.pos,
            # 'origin': self.origin,
            # 'destination': self.destination,
            # 'compartment': self.compartment,
            'rt_ow_indicator': {"$in" :[self.ow_rt_flag.lower(), self.ow_rt_flag.upper()]}
            }
        ##print 'query', query
        return self.build_diffuser_reference(query)

    def get_super_rbd_name(self, i):
        return ""

    def get_diffuser_reference_table_pos_level_dict_of_dicts(self):
        query={
            # 'pos': self.pos,
            # 'compartment': self.compartment,
            'rt_ow_indicator': {"$in" :[self.ow_rt_flag.lower(), self.ow_rt_flag.upper()]}
            }
        return self.build_diffuser_reference(query)

    def get_diffuser_reference_table_country_level_dict_of_dicts(self):
        query={
            # 'country': self.country,
            # 'compartment': self.compartment,
            'rt_ow_indicator': {"$in" :[self.ow_rt_flag.lower(), self.ow_rt_flag.upper()]}
            }
        return self.build_diffuser_reference(query)

    def get_diffuser_reference_table_region_level_dict_of_dicts(self):
        query={
            # 'region': self.region,
            # 'compartment': self.compartment,
            'rt_ow_indicator': {"$in" :[self.ow_rt_flag.lower(), self.ow_rt_flag.upper()]}
            }
        return self.build_diffuser_reference(query)

    def get_diffuser_reference_table_network_level_dict_of_dicts(self):
        query={
            # 'compartment': self.compartment,
            'rt_ow_indicator': {"$in" :[self.ow_rt_flag.lower(), self.ow_rt_flag.upper()]}
            }
        return self.build_diffuser_reference(query)

    def build_diffuser_reference(self, query):
        list_dict_diffuser_reference_table_raw=[]
        cursor_diffuser_reference = db.JUP_DB_ATPCO_Fare_Brand_Diffuser.find(query)
        for c in cursor_diffuser_reference:
            list_dict_diffuser_reference_table_raw.append(c)

        dict_diffuser_ref_table=dict()
        for dict_diffuser_ref_row in list_dict_diffuser_reference_table_raw:

            farebrand=dict_diffuser_ref_row['fare_brand']
            farebrand_number=farebrand
            try:
                dict_diffuser_ref_table[farebrand_number]
            except:
                dict_diffuser_ref_table[farebrand_number]=dict()

            dict_diffuser_ref_table[farebrand_number]=dict_diffuser_ref_row
            dict_diffuser_ref_table[farebrand_number]['farebrand']=dict_diffuser_ref_row['fare_brand']
            dict_diffuser_ref_table[farebrand_number]['farebrand_number']=dict_diffuser_ref_row['fare_brand']

            if self.compartment=='Y':
                if dict_diffuser_ref_row['economy_absolute'] != None:
                    dict_diffuser_ref_table[farebrand_number]['amt_or_perc']='amt'
                    dict_diffuser_ref_table[farebrand_number]['amt']=dict_diffuser_ref_row['economy_absolute']
                elif dict_diffuser_ref_row['economy_percentage'] != None:
                    dict_diffuser_ref_table[farebrand_number]['amt_or_perc']='perc'
                    dict_diffuser_ref_table[farebrand_number]['amt']=dict_diffuser_ref_row['economy_percentage']
                else:
                    e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
                    e.append_to_error_list("economy amount is neither absolute or percentage!")
                    raise e
            elif self.compartment=='J':
                if dict_diffuser_ref_row['business_absolute'] != None:
                    dict_diffuser_ref_table[farebrand_number]['amt_or_perc']='amt'
                    dict_diffuser_ref_table[farebrand_number]['amt']=dict_diffuser_ref_row['business_absolute']
                elif dict_diffuser_ref_row['business_percentage'] != None:
                    dict_diffuser_ref_table[farebrand_number]['amt_or_perc']='perc'
                    dict_diffuser_ref_table[farebrand_number]['amt']=dict_diffuser_ref_row['business_percentage']
                else:
                    e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
                    e.append_to_error_list("business amount is neither absolute or percentage!")
                    raise e
            else:
                e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
                e.append_to_error_list("compartment is neither ECONOMY nor BUSINESS!")
                raise e

            if self.ow_rt_flag=='ow':
                dict_diffuser_ref_table[farebrand_number]['o_amt_or_perc']=dict_diffuser_ref_table[farebrand_number]['amt_or_perc']
                dict_diffuser_ref_table[farebrand_number]['o_amt']=dict_diffuser_ref_table[farebrand_number]['amt'];
            if self.ow_rt_flag=='rt':
                dict_diffuser_ref_table[farebrand_number]['r_amt_or_perc']=dict_diffuser_ref_table[farebrand_number]['amt_or_perc']
                dict_diffuser_ref_table[farebrand_number]['r_amt']=dict_diffuser_ref_table[farebrand_number]['amt'];
        return dict_diffuser_ref_table
   

###############@@@@@@@@@@@@@@@@@@@@@@@@@@

    def build_diffuser_ref_table(self, query):
        list_dict_diffuser_ref_table_raw=[]
        cursor_diffuser_ref = db.JUP_DB_ATPCO_Fare_Brand_Diffuser.find(query)
        if cursor_diffuser_ref.count() == 0:
            e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
            e.append_to_error_list("No diffuser reference records in JUP_DB_ATPCO_farebrand_Diffuser!")
            raise e
        for c in cursor_diffuser_ref:
            list_dict_diffuser_ref_table_raw.append(c)

        dict_diffuser_ref_table={}
    def get_module_name(self):
        return inspect.stack()[1][3]

    def get_arg_lists(self, frame):
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

