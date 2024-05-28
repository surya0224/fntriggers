#######################################################
## Author: Prem
## First completeion date: April 2 2017
#######################################################
import jupiter_AI.common.ClassErrorObject as errorClass
import datetime
import json
import time
import jupiter_AI.network_level_params as net
import inspect
import collections
import copy
import jupiter_AI.common.ClassErrorObject as error_class
import ClassDiffuserReference as diffuser_reference_class
from jupiter_AI.network_level_params import JUPITER_DB
import pymongo

start_time = time.time()
# print start_time
MONGO_CLIENT_URL = '13.92.251.7:42525'
#net.MONGO_CLIENT_URL
ANALYTICS_MONGO_USERNAME = 'analytics'
ANALYTICS_MONGO_PASSWORD = 'KNjSZmiaNUGLmS0Bv2'
JUPITER_DB = 'fzDB'
client = pymongo.MongoClient(MONGO_CLIENT_URL)
client.the_database.authenticate(ANALYTICS_MONGO_USERNAME, ANALYTICS_MONGO_PASSWORD, source='admin')
#db = client[JUPITER_DB]

class channel_diffuser_reference(diffuser_reference_class.diffuser_reference):
    def __init__(self, pos, origin, destination, compartment, ow_rt_flag, country, region):
        self.pos=pos
        self.origin=origin
        self.destination=destination
        self.compartment=compartment
        self.ow_rt_flag=ow_rt_flag
        self.country=country
        self.region=region
        self.dict_diffuser_ref_table=self.get_diffuser_reference_table_dict_of_dicts()

    def __str__(self):
        return str(self.dict_diffuser_ref_table)
        
    def get_diffuser_ordering_numbers(self):
        list_diffuser_ordering_numbers=[]
        for dict_key in self.dict_diffuser_ref_table:
            list_diffuser_ordering_numbers.append(self.dict_diffuser_ref_table[dict_key]['channel_number'])
        return sorted(list_diffuser_ordering_numbers)
    
    def get_ordering_name(self, diffuser_ordering_number):
        return self.dict_diffuser_ref_table[diffuser_ordering_number]['channel']

    def get_diffuser_row_dict(self, diffuser_ordering_number):
        return self.dict_diffuser_ref_table[diffuser_ordering_number]

##    def get_diffuser_reference_table_dict_of_dicts():
##        ddict=dict()
##        ddict[1]={'channel':'GDS',          'channel_number': 1,    'o_amt_or_perc': 'perc', 'o_amt': 0, 'r_amt_or_perc': 'perc', 'r_amt': 0}
##        ddict[2]={'channel':'WEB',          'channel_number': 2,    'o_amt_or_perc': 'perc', 'o_amt': 2, 'r_amt_or_perc': 'perc', 'r_amt': 2}
##        ddict[3]={'channel':'OTA',          'channel_number': 3,    'o_amt_or_perc': 'perc', 'o_amt': 3, 'r_amt_or_perc': 'perc', 'r_amt': 3}
##        ddict[4]={'channel':'private',      'channel_number': 4,    'o_amt_or_perc': 'perc', 'o_amt': 4, 'r_amt_or_perc': 'perc', 'r_amt': 4}
##        ddict[5]={'channel':'FFP',          'channel_number': 5,    'o_amt_or_perc': 'amt', 'o_amt': 10, 'r_amt_or_perc': 'amt', 'r_amt': 15}
##        ddict[6]={'channel':'corporate',    'channel_number': 6,    'o_amt_or_perc': 'amt', 'o_amt': 15, 'r_amt_or_perc': 'amt', 'r_amt': 20}
##        return ddict


    def get_diffuser_reference_table_dict_of_dicts(self):
        '''
        get diffuser table from most granular to least granular level
        '''
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
        if len(dict_diffuser_ref_table)==0:
            e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, get_module_name(), get_arg_lists(inspect.currentframe()))
            e.append_to_error_list("reference table for channel diffuser doesn't exist")
            raise e
        return dict_diffuser_ref_table
        # print "@@##dict_diffuser_ref_table##@#$#$ " , dict_diffuser_ref_table



    def get_diffuser_reference_table_od_level_dict_of_dicts(self):
        query={
            # 'pos': self.pos,
            # 'origin': self.origin,
            # 'destination': self.destination,
            # 'compartment': self.compartment,
            'ow_rt_flag': {"$in" :[self.ow_rt_flag.lower(), self.ow_rt_flag.upper()]}
            }
        return self.build_diffuser_reference(query)

    def get_super_rbd_name(self, i):
        return ""

    def get_diffuser_reference_table_pos_level_dict_of_dicts(self):
        query={
            # 'pos': self.pos,
            # 'compartment': self.compartment,
            'ow_rt_flag': {"$in" :[self.ow_rt_flag.lower(), self.ow_rt_flag.upper()]}
            }
        return self.build_diffuser_reference(query)

    def get_diffuser_reference_table_country_level_dict_of_dicts(self):
        query={
            # 'country': self.country,
            # 'compartment': self.compartment,
            'ow_rt_flag': {"$in" :[self.ow_rt_flag.lower(), self.ow_rt_flag.upper()]}
            }
        return self.build_diffuser_reference(query)

    def get_diffuser_reference_table_region_level_dict_of_dicts(self):
        query={
            # 'region': self.region,
            # 'compartment': self.compartment,
            'ow_rt_flag': {"$in" :[self.ow_rt_flag.lower(), self.ow_rt_flag.upper()]}
            }
        return self.build_diffuser_reference(query)

    def get_diffuser_reference_table_network_level_dict_of_dicts(self):

        query={
            # 'compartment': self.compartment,
            'ow_rt_flag': {"$in" :[self.ow_rt_flag.lower(), self.ow_rt_flag.upper()]}
            }
        # print query
        return self.build_diffuser_reference(query)

    def build_diffuser_reference(self, query):
        list_of_dicts_diffuser_reference_table_raw=[]
        cursor_diffuser_reference = db.JUP_DB_ATPCO_Channel_Diffuser.find(query)
        if cursor_diffuser_reference.count() == 0:
            e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
            e.append_to_error_list("No diffuser reference records in JUP_DB_ATPCO_Channel_Diffuser!")
            raise e
        for c in cursor_diffuser_reference:
            list_of_dicts_diffuser_reference_table_raw.append(c)

        dict_diffuser_ref_table=dict()
        for dict_diffuser_reference_row in list_of_dicts_diffuser_reference_table_raw:

##            if check_if_pos_is_none:
##                if list_of_dicts_diffuser_reference_table_raw[dict_diffuser_reference_row]['origin'] != None:
##                    continue

            channel=dict_diffuser_reference_row['channel']
            channel_number=dict_diffuser_reference_row['channel_order']
            try:
                dict_diffuser_ref_table[channel_number]
            except:
                dict_diffuser_ref_table[channel_number]=dict()

            dict_diffuser_ref_table[channel_number]=dict_diffuser_reference_row
            dict_diffuser_ref_table[channel_number]['channel_number']=dict_diffuser_reference_row['channel_order']

            ow_rt_flag=dict_diffuser_reference_row['ow_rt_flag'].lower()
            if ow_rt_flag =='ow':
                if dict_diffuser_reference_row['amount_absolute'] != None:
                    dict_diffuser_ref_table[channel_number]['o_amt_or_perc']='amt'
                    dict_diffuser_ref_table[channel_number]['o_amt']=dict_diffuser_reference_row['amount_absolute']
                elif dict_diffuser_reference_row['amount_percentage'] != None:
                    dict_diffuser_ref_table[channel_number]['o_amt_or_perc']='perc'
                    dict_diffuser_ref_table[channel_number]['o_amt']=dict_diffuser_reference_row['amount_percentage']
                else:
                    e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
                    e.append_to_error_list("amount is neither absolute or percentage!")
                    raise e
            elif ow_rt_flag=='rt':
                if dict_diffuser_reference_row['amount_absolute'] != None:
                    dict_diffuser_ref_table[channel_number]['r_amt_or_perc']='amt'
                    dict_diffuser_ref_table[channel_number]['r_amt']=dict_diffuser_reference_row['amount_absolute']
                elif dict_diffuser_reference_row['amount_percentage'] != None:
                    dict_diffuser_ref_table[channel_number]['r_amt_or_perc']='perc'
                    dict_diffuser_ref_table[channel_number]['r_amt']=dict_diffuser_reference_row['amount_percentage']
                else:
                    e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
                    e.append_to_error_list("amount is neither absolute or percentage!")
                    raise e
            else:
                e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
                e.append_to_error_list(str(dict_diffuser_reference_row['ow_rt_flag'])+": ow_rt_flag neither O nor R!")
                raise e
        
        return dict_diffuser_ref_table
    
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

