"""
Author: Abhinav Garg
Created with <3
Date Created: 2018-09-06
Code functionality:
     Listener for Rule triggers, calling function for rule trigger level analysis
Modifications log:
    1. Author:
       Exact modification made or some logic changed:
       Date of modification:
    2. Author:
       Exact modification made or some logic changed:
       Date of modification:

"""
import bson
from jupiter_AI.common import ClassErrorObject as error_class
from jupiter_AI import JUPITER_LOGGER, client, JUPITER_DB
from jupiter_AI.logutils import measure
from jupiter_AI.triggers.listener_data_level_trigger import trigger_desc_list
#from jupiter_AI.triggers.rules_triggers import rule_trigger_analysis
#from bson import ObjectId
import inspect
#db = client[JUPITER_DB]


@measure(JUPITER_LOGGER)
def analyze(id):
    '''
    Takes the id from the message in queue as input
    Does the Entire process of Analytics by calling the relevant class
    After the processing is done prints a message as to processed
    '''

    cursor = db.JUP_DB_Triggering_Event.find(
        {'_id': bson.objectid.ObjectId(id)})

    query = [i for i in cursor]

    if len(query) == 1:
        trigger_list = trigger_desc_list()
        t = query[0]['trigger_type']
        if t in trigger_list:
            print 'p = rule_trigger_analysis.' + t + str(id)
            exec 'p = rule_trigger_analysis.' + t + '(ObjectId(id))'
            exec 'p.do_analysis()'
        else:
            e1 = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                         get_module_name(),
                                         get_arg_lists(inspect.currentframe()))
            e1.append_to_error_list("The trigger_type for the id " + str(
                id) + " is not in the Triggers_List supported by jupiter")
            raise e1
    else:
        e1 = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                     get_module_name(),
                                     get_arg_lists(inspect.currentframe()))
        error_desc = "Expected 1 document in triggering_event collection for id = " + \
                     str(id) + " but got " + str(len(query))
        e1.append_to_error_list(error_desc)
        raise e1


@measure(JUPITER_LOGGER)
def get_module_name():
    '''
    FUnction used to get the module name where it is called
    '''
    return inspect.stack()[1][3]


@measure(JUPITER_LOGGER)
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
