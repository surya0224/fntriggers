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

from jupiter_AI.network_level_params import JUPITER_DB

class diffuser_reference(object):

    def nrows(self):
        return len(dict_of_dicts_diffuser_reference_table)
    
    def get_diffuser_ordering_numbers(self):
        pass
    
    def get_ordering_name(self, i):
        pass

    def get_super_rbd_name(self, i):
        return ""

    def get_diffuser_dict(self, diffuser_ordering_number):
        return self.dict_of_dicts_diffuser_reference_rows[diffuser_ordering_number]

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

