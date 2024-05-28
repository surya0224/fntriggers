'''
Author: Premkumar Narasimhan
Date: 2nd April, 2017
Purpose:
    Calculates the farebrand diffuser
Explanation:
    Derives from "diffuser" base class
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
import ClassFarebrandDiffuserReference as farebrandDiffuserReferenceClass
from jupiter_AI.network_level_params import JUPITER_DB

class farebrand_diffuser(diffuserClass.diffuser):
    def get_resultant_farebasis_code(self, dict_arguments):
        farebasis=list(dict_arguments['farebasis'])                  ## convert base_fare_basis to list of characters
        farebasis[2]=str(dict_arguments['diffuser_ordering_number'])              ## construct output farebasis code
        farebasis="".join(farebasis)                    ## convert back from list of characters to string
        return farebasis

    def get_basefare_index(self, base_farebasis):
        if len(base_farebasis)==6:
            base_fare_basis_fare_brand=int(base_farebasis[2])
        elif len(base_farebasis)==7:
            base_fare_basis_fare_brand = int(base_farebasis[3])
            # e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
            # e.append_to_error_list("base farebrand not found in diffuser_ordering_numbers")
            # raise e
        found=False
        for k in self.obj_diffuser_reference.dict_diffuser_ref_table:
            if self.obj_diffuser_reference.dict_diffuser_ref_table[k]['fare_brand']==base_fare_basis_fare_brand:
                found=True
                break
        # if not found:
        #     e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
        #     e.append_to_error_list("3rd place in farebasis not found in referrence table")
        #     raise e
        for i in range(len(self.diffuser_ordering_numbers)):
            if self.diffuser_ordering_numbers[i]==k:
                return i
        # e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
        # e.append_to_error_list("base farebrand not found in diffuser_ordering_numbers")
        # raise e

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

###########################@@@@
def get_ow_rt_flag(farebasis):
    if farebasis[1]=='o' or farebasis[1]=='O':
        return 'ow'
    if farebasis[1]=='r' or farebasis[1]=='R':
        return 'rt'
    e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            get_module_name(),
                                            get_arg_lists(inspect.currentframe()))
    e.append_to_error_list("ow_rt_flag neither O nor R!")
    raise e


def check_farebases(dict_farebases):
    pos             = dict_farebases['pos']
    origin          = dict_farebases['origin']
    destination     = dict_farebases['destination']
    compartment     = dict_farebases['compartment']
    ow_rt_flag      = get_ow_rt_flag(dict_farebases['farebasis'])
    country         = dict_farebases['country']
    region          = dict_farebases['region']
    return pos, origin, destination, compartment, ow_rt_flag, country, region
    

lst=[{'origin': 'Network', 'diffuser_ordering_number': 2, 'compartment': 'Y', 'farebasis': u'KR1AE1', 'price': 900, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
    {'origin': 'Network', 'diffuser_ordering_number': 1, 'compartment': 'Y', 'farebasis': u'ER1AE1', 'price': 800, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
    {'origin': 'Network', 'diffuser_ordering_number': 3, 'compartment': 'Y', 'farebasis': u'MR1AE1', 'price': 1000, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
    {'origin': 'Network', 'diffuser_ordering_number': 4, 'compartment': 'Y', 'farebasis': u'BR1AE1', 'price': 1150, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
    {'origin': 'Network', 'diffuser_ordering_number': 5, 'compartment': 'Y', 'farebasis': u'YR1AE1', 'price': 1300, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'}]
##lst=[{'origin': 'Network', 'diffuser_ordering_number': 2, 'compartment': 'J', 'farebasis': u'KR1AE1', 'price': 900, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
##    {'origin': 'Network', 'diffuser_ordering_number': 1, 'compartment': 'Y', 'farebasis': u'ER1AE1', 'price': 800, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'}]
def main_func(list_of_dicts_base_farebases):
    emain = error_class.ErrorObject(0, "main", "")
    nfarebases_specified = len(list_of_dicts_base_farebases)
    list_of_dicts_diffuser_output = []
    for i in range(nfarebases_specified):
        pos, origin, destination, compartment, ow_rt_flag, country, region  = check_farebases(list_of_dicts_base_farebases[i])
        obj_farebrand_diffuser_reference                = farebrandDiffuserReferenceClass.farebrand_diffuser_reference(pos, origin, destination, compartment, ow_rt_flag, country, region)
        diffuser                                        = farebrand_diffuser('absolute', obj_farebrand_diffuser_reference, [list_of_dicts_base_farebases[i]])
        list_of_dicts_diffuser_output_one_farebasis     = diffuser.list_of_dicts_diffuser_output
        for j in list_of_dicts_diffuser_output_one_farebasis:
            list_of_dicts_diffuser_output.append(j)
    return list_of_dicts_diffuser_output
    # except esub:
    #     emain.append_to_error_object_list(esub)
    # print emain
    # return list_of_dicts_diffuser_output
# start_time = time.time()
# print start_time
##lst2=main_func(lst)
##print "Farebrand Diffuser"
##for i in lst2:
##    print i
# print("--- %s seconds ---" % (time.time() - start_time))
