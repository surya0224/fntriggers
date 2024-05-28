'''
Author: Premkumar Narasimhan
Date: 2nd April, 2017
Purpose:
    Calculates the RBD diffuser
Explanation:
    Derives from "diffuser" base class
Code below the line marked with ###########################@@@@ gives example of invoking the class
 The diffuser output is in the structure diffuser.list_of_dicts_diffuser_output, where "diffuser" is the base class for "rbd_diffuser"
'''

import jupiter_AI.common.ClassErrorObject as errorClass
import datetime
import json
import time
import jupiter_AI.network_level_params as net
import inspect
import collections
import copy
import jupiter_AI.common.ClassErrorObject as error_class
import ClassDiffuser as diffuserClass
import ClassRBDDiffuserReference as rbdDiffuserReferenceClass
import pymongo
from operator import itemgetter

start_time = time.time()
# print start_time
MONGO_CLIENT_URL = '13.92.251.7:42525'
#net.MONGO_CLIENT_URL
ANALYTICS_MONGO_USERNAME = 'analytics'
ANALYTICS_MONGO_PASSWORD = 'KNjSZmiaNUGLmS0Bv2'
JUPITER_DB = 'testDB'
client = pymongo.MongoClient(MONGO_CLIENT_URL)
client.the_database.authenticate(ANALYTICS_MONGO_USERNAME, ANALYTICS_MONGO_PASSWORD, source='admin')
db = client[JUPITER_DB]

class rbd_diffuser(diffuserClass.diffuser):
    def get_resultant_farebasis_code(self, dict_arguments):
##    The aim of this method is to provide the new farebasis (when the sellup/selldown is applied to the base_fare), for example MR1AE1 (base fare) might become KR1AE1 (new farebasis)
        prefix=dict_arguments['super_rbd']
        farebasis=prefix[0]+dict_arguments['farebasis'][1:]
        return farebasis

    def get_basefare_index(self, base_farebasis):
##    gets the index of the base farebasis in the diffuser_reference_table (0 means its the first record in the table etc.)
##    the base_fare_index serves as the entry point for the diffuser calculations
##    from this point, it calculates the diffuser for
##    (a) selldown (for indexes < base_fare_index)
##    (b) keeps the same fare (for index=base_fare_index)
##    (c) sellup (for indexes > base_fare_index)
        i=0
        found=False
        for ordering_number in self.obj_diffuser_reference.dict_diffuser_ref_table:
            if self.obj_diffuser_reference.dict_diffuser_ref_table[ordering_number]['rbd']==base_farebasis[0]:
                return i
            i+=1
        e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
        e.append_to_error_list("base RBD not found in diffuser reference table")
        raise e
        return None

    def get_module_name(self):
        return inspect.stack()[1][3]

    def get_arg_lists(self, frame):
##        """
##        function used to get the list of arguments of the function
##        where it is called
##        """
        args, _, _, values = inspect.getargvalues(frame)
        argument_name_list=[]
        argument_value_list=[]
        for k in args:
            argument_name_list.append(k)
            argument_value_list.append(values[k])
        return argument_name_list, argument_value_list

###########################@@@@
   ## code below this is not part of class - it is  used as an example to invoke the class
    
def get_module_name():
    return inspect.stack()[1][3]

def get_arg_lists(frame):
##    """
##    function used to get the list of arguments of the function
##    where it is called
##    """
    args, _, _, values = inspect.getargvalues(frame)
    argument_name_list=[]
    argument_value_list=[]
    for k in args:
        argument_name_list.append(k)
        argument_value_list.append(values[k])
    return argument_name_list, argument_value_list

def get_ow_rt_flag(farebasis):
    if farebasis[1]=='o' or farebasis[1]=='O':
        return 'ow'
    if farebasis[1]=='r' or farebasis[1]=='R':
        return 'rt'
    e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, get_module_name(), get_arg_lists(inspect.currentframe()))
    e.append_to_error_list("ow_rt_flag neither O nor R!")
    raise e
    
def check_farebases(list_of_dicts_farebases):
    if len(list_of_dicts_farebases)==0:
        e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, get_module_name(), get_arg_lists(inspect.currentframe()))
        e.append_to_error_list("no farebases records found as basis for channel diffuser explosion!")
        raise e
    if len(list_of_dicts_farebases)>1:
        e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, get_module_name(), get_arg_lists(inspect.currentframe()))
        e.append_to_error_list("only one farebasis can be exploded in RBD diffuser!")
        raise e
    farebases=[]
    for i in range(len(list_of_dicts_farebases)):
        pos             = list_of_dicts_farebases[i]['pos']
        origin          = list_of_dicts_farebases[i]['origin']
        destination     = list_of_dicts_farebases[i]['destination']
        compartment     = list_of_dicts_farebases[i]['compartment']
        ow_rt_flag      = get_ow_rt_flag(list_of_dicts_farebases[i]['farebasis'])
        country         = list_of_dicts_farebases[i]['country']
        region          = list_of_dicts_farebases[i]['region']
        price           = list_of_dicts_farebases[i]['price']
        farebasis       = list_of_dicts_farebases[i]['farebasis']
        if i==0:
            prv_pos             = list_of_dicts_farebases[i]['pos']
            prv_origin          = list_of_dicts_farebases[i]['origin']
            prv_destination     = list_of_dicts_farebases[i]['destination']
            prv_compartment     = list_of_dicts_farebases[i]['compartment']
            prv_ow_rt_flag      = get_ow_rt_flag(list_of_dicts_farebases[i]['farebasis'])
            prv_country         = list_of_dicts_farebases[i]['country']
            prv_region          = list_of_dicts_farebases[i]['region']
        else:
            if pos!=prv_pos or origin!=prv_origin or destination!=prv_destination or compartment!=prv_compartment or ow_rt_flag!=prv_ow_rt_flag or country!=prv_country or region!=prv_region:
                e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, get_module_name(), get_arg_lists(inspect.currentframe()))
                e.append_to_error_list("pos, origin, destination, compartment, ow_rt_flag, country, region must be same in all farebases at base of channel diffuser explosion!")
                raise e
        farebases.append([pos, origin, destination, compartment, ow_rt_flag, country, region, price, farebasis])
    return farebases


def main_func(list_of_dicts_base_farebases):
    emain = error_class.ErrorObject(0, get_module_name(), get_arg_lists(inspect.currentframe()))

    # try:
    lst_farebases = check_farebases(list_of_dicts_base_farebases)
    # except esub:
    #     emain.append_to_error_object_list(esub)
    lst_farebases = sorted(lst_farebases, key=itemgetter(7)) #7 is the index in the list of the price of that farebasis code
    nfarebases_specified = len(list_of_dicts_base_farebases)

    list_of_dicts_diffuser_output = []
    for i in range(nfarebases_specified):
        pos, origin, destination, compartment, ow_rt_flag, country, region, price, farebasis  = lst_farebases[i]
        explode_orientation="both"
        stop_rbd=None
##        if i==0: ## lowest farebasis in the list of farebasis, explode it both ways
##            explode_orientation="both"
##            ##print explode_orientation
##        else:
##            explode_orientation="sellup_only"
##            ##print explode_orientation
##        stop_rbd=None
##        if i < nfarebases_specified - 1: ## stop asks the rbd_diffuser_reference to stop exploding when it reaches this farebasis ie dont explode the "stop" farebasis and anything beyond it
##            stop_rbd=lst_farebases[i+1][8][0] ## 8 is the index in the list of the farebasis code
        
        obj_rbd_diffuser_reference   = rbdDiffuserReferenceClass.rbd_diffuser_reference(pos, origin, destination, compartment, ow_rt_flag, country, region, price, farebasis, explode_orientation, stop_rbd)
        diffuser = rbd_diffuser('relative', obj_rbd_diffuser_reference, [list_of_dicts_base_farebases[i]])
        for j in range(len(diffuser.list_of_dicts_diffuser_output)):
            #try:
            list_of_dicts_diffuser_output.append(diffuser.list_of_dicts_diffuser_output[j])
        #     except esub:
        #         emain.append_to_error_object_list(esub)
        # print emain
        return list_of_dicts_diffuser_output
    
##lst=[{"pos" : "Network", "origin" : "Network", "destination" : "Network", "compartment" : "Y", "country" : "Network", "region" : "Network", "price" : 1000, "farebasis" : "MR1AE1"}]
##lst2=main_func(lst)
##print "RBD Diffuser"
##for i in lst2:
##    print i
# print("--- %s seconds ---" % (time.time() - start_time))
