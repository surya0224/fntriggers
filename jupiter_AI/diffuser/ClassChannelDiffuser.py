'''
Author: Premkumar Narasimhan
Date: 2nd April, 2017
Purpose:
    Calculates the Channel diffuser
Explanation:
    Derives from "diffuser" base class
## the channel diffuser is of type "absolute" - sellup/selldown is always with respect to the base_farebasis amount, and not the with respect to the next lower farebasis in the farebasis hierarchy
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
import ClassChannelDiffuserReference as channelDiffuserReferenceClass
import pymongo

start_time = time.time()
# print start_time
MONGO_CLIENT_URL = '13.92.251.7:42525'
#net.MONGO_CLIENT_URL
ANALYTICS_MONGO_USERNAME = 'analytics'
ANALYTICS_MONGO_PASSWORD = 'KNjSZmiaNUGLmS0Bv2'
JUPITER_DB = 'testDB'
client = pymongo.MongoClient(MONGO_CLIENT_URL)
client.the_database.authenticate(ANALYTICS_MONGO_USERNAME, ANALYTICS_MONGO_PASSWORD, source='admin')
#db = client[JUPITER_DB]

class channel_diffuser(diffuserClass.diffuser):

    def get_resultant_farebasis_code(self, dict_arguments):
        suffix=str(dict_arguments['ordering_name'])
        ##if suffix=='GDS':
        ##    return dict_arguments['farebasis']
        return dict_arguments['farebasis'] + '_' + suffix

    def get_basefare_index(self, base_farebasis):
        return 0

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
def get_module_name():
    return inspect.stack()[1][3]


def get_arg_lists(frame):
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


def main_func(list_of_dicts_base_farebases):
    emain = error_class.ErrorObject(0, "main", "")
    nfarebases_specified = len(list_of_dicts_base_farebases)
    list_of_dicts_diffuser_output = []
    for i in range(nfarebases_specified):
        pos, origin, destination, compartment, ow_rt_flag, country, region  = check_farebases(list_of_dicts_base_farebases[i])
        obj_channel_diffuser_reference                = channelDiffuserReferenceClass.channel_diffuser_reference(pos, origin, destination, compartment, ow_rt_flag, country, region)
        diffuser                                      = channel_diffuser('absolute', obj_channel_diffuser_reference, [list_of_dicts_base_farebases[i]])
        list_of_dicts_diffuser_output_one_farebasis   = diffuser.list_of_dicts_diffuser_output
        for j in list_of_dicts_diffuser_output_one_farebasis:
            list_of_dicts_diffuser_output.append(j)
    # print emain
    return list_of_dicts_diffuser_output

lst=[{'origin': 'Network', 'diffuser_ordering_number': 1, 'compartment': 'Y', 'farebasis': u'KR1AE1', 'price': 900, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 2, 'compartment': 'Y', 'farebasis': u'KR2AE1', 'price': 925, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 3, 'compartment': 'Y', 'farebasis': u'KR3AE1', 'price': 950, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 4, 'compartment': 'Y', 'farebasis': u'KR4AE1', 'price': 975, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 5, 'compartment': 'Y', 'farebasis': u'KR5AE1', 'price': 1000, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 6, 'compartment': 'Y', 'farebasis': u'KR6AE1', 'price': 1025, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 7, 'compartment': 'Y', 'farebasis': u'KR7AE1', 'price': 1050, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 8, 'compartment': 'Y', 'farebasis': u'KR8AE1', 'price': 1075, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 9, 'compartment': 'Y', 'farebasis': u'KR9AE1', 'price': 1100, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 1, 'compartment': 'Y', 'farebasis': u'ER1AE1', 'price': 800, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 2, 'compartment': 'Y', 'farebasis': u'ER2AE1', 'price': 825, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 3, 'compartment': 'Y', 'farebasis': u'ER3AE1', 'price': 850, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 4, 'compartment': 'Y', 'farebasis': u'ER4AE1', 'price': 875, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 5, 'compartment': 'Y', 'farebasis': u'ER5AE1', 'price': 900, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 6, 'compartment': 'Y', 'farebasis': u'ER6AE1', 'price': 925, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 7, 'compartment': 'Y', 'farebasis': u'ER7AE1', 'price': 950, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 8, 'compartment': 'Y', 'farebasis': u'ER8AE1', 'price': 975, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 9, 'compartment': 'Y', 'farebasis': u'ER9AE1', 'price': 1000, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 1, 'compartment': 'Y', 'farebasis': u'MR1AE1', 'price': 1000, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 2, 'compartment': 'Y', 'farebasis': u'MR2AE1', 'price': 1025, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 3, 'compartment': 'Y', 'farebasis': u'MR3AE1', 'price': 1050, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 4, 'compartment': 'Y', 'farebasis': u'MR4AE1', 'price': 1075, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 5, 'compartment': 'Y', 'farebasis': u'MR5AE1', 'price': 1100, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 6, 'compartment': 'Y', 'farebasis': u'MR6AE1', 'price': 1125, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 7, 'compartment': 'Y', 'farebasis': u'MR7AE1', 'price': 1150, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 8, 'compartment': 'Y', 'farebasis': u'MR8AE1', 'price': 1175, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 9, 'compartment': 'Y', 'farebasis': u'MR9AE1', 'price': 1200, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 1, 'compartment': 'Y', 'farebasis': u'BR1AE1', 'price': 1150, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 2, 'compartment': 'Y', 'farebasis': u'BR2AE1', 'price': 1175, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 3, 'compartment': 'Y', 'farebasis': u'BR3AE1', 'price': 1200, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 4, 'compartment': 'Y', 'farebasis': u'BR4AE1', 'price': 1225, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 5, 'compartment': 'Y', 'farebasis': u'BR5AE1', 'price': 1250, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 6, 'compartment': 'Y', 'farebasis': u'BR6AE1', 'price': 1275, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 7, 'compartment': 'Y', 'farebasis': u'BR7AE1', 'price': 1300, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 8, 'compartment': 'Y', 'farebasis': u'BR8AE1', 'price': 1325, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 9, 'compartment': 'Y', 'farebasis': u'BR9AE1', 'price': 1350, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 1, 'compartment': 'Y', 'farebasis': u'YR1AE1', 'price': 1300, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 2, 'compartment': 'Y', 'farebasis': u'YR2AE1', 'price': 1325, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 3, 'compartment': 'Y', 'farebasis': u'YR3AE1', 'price': 1350, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 4, 'compartment': 'Y', 'farebasis': u'YR4AE1', 'price': 1375, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 5, 'compartment': 'Y', 'farebasis': u'YR5AE1', 'price': 1400, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 6, 'compartment': 'Y', 'farebasis': u'YR6AE1', 'price': 1425, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 7, 'compartment': 'Y', 'farebasis': u'YR7AE1', 'price': 1450, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 8, 'compartment': 'Y', 'farebasis': u'YR8AE1', 'price': 1475, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'},
{'origin': 'Network', 'diffuser_ordering_number': 9, 'compartment': 'Y', 'farebasis': u'YR9AE1', 'price': 1500, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'}]

##lst=[{'origin': 'Network', 'diffuser_ordering_number': 1, 'compartment': 'Y', 'farebasis': u'KR1AE1', 'price': 900, 'country': 'Network', 'region': 'Network', 'destination': 'Network', 'pos': 'Network'}]

# start_time = time.time()
# print start_time
##lst2=main_func(lst)
##print "Channel Diffuser"
##for i in lst2:
##    print i
# print("--- %s seconds ---" % (time.time() - start_time))
