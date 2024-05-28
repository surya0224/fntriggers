import datetime
import inspect
import os
import sys
import traceback
from copy import deepcopy

#   The below two lines of code is just to make sure that we have the dir containing the package jupiter_AI in path
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir, os.path.pardir))
sys.path.append(root_dir)
import analytics_functions as af
from jupiter_AI.common.query_pricing_data import query_competitor_pricing_data
from jupiter_AI.common import ClassErrorObject as error_class
from jupiter_AI import network_level_params as net

from jupiter_AI.network_level_params import SYSTEM_DATE


def identify_trigger(c, o, n):
    print c
    print o
    print n
    #   nested try functions are provided for exception handling
    try:
        #   Getting the list of all data change triggers from the dB
        cursor = db.JUP_DB_Trigger_Types.find({'triggering_event_type': 'data_change'})
        query = []
        for i in cursor:
            # del i['_id']
            query.append(i)
        if len(query) != 0:
            for i in query:
                #   Based on the triggering event collection
                #   Getting the unique trigger name from trigger_types collection

                if (i['triggering_event']['collection'] == c['collection'] and
                            i['triggering_event']['field'] == c['field'] and
                            i['triggering_event']['action'] == c['action']):
                    print i
                    trigger_type = i['desc']
                    print trigger_type
                    #   If a trigger name has been obtained calling the trigger_types collection
                    #   The class structure has been developed in a way so that the trigger_name is the class
                    #   Since we have defined a class for each trigger name
                    #   Through the following "exec ... " string we are calling the respective class name
                    string = 'p =' + trigger_type + '(c,o,n)'
                    print string
                    exec string
        else:
            #   Error if no documents are obtained from the trigger_types collection for data_change triggers dB
            e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                        get_module_name(),
                                        get_arg_lists(inspect.currentframe()))
            e.append_to_error_list("No docs for data_change triggers obtained from dB")
            raise e
    except error_class.ErrorObject as esub:
        #   Updating of Error in dB if any user defined Error occured in analysis
        db.JUP_DB_Error_Collection.insert_one({'date': datetime.datetime.now().strftime('%Y-%m-%d'),
                                               'time': datetime.datetime.now().strftime('%H:%M:%S.%f'),
                                               'change': c,
                                               'old_database_doc': o,
                                               'new_database_doc': n,
                                               'error_description': esub.__str__()})
        if esub.error_level >= error_class.ErrorObject.ERRORLEVEL2:
            raise esub
    except KeyError as key:
        #   Updating of Error in dB if a KeyError occurred in analysis
        tb = traceback.format_exc()
        db.JUP_DB_Error_Collection.insert_one({'date': datetime.datetime.now().strftime('%Y-%m-%d'),
                                               'time': datetime.datetime.now().strftime('%H:%M:%S.%f'),
                                               'change': c,
                                               'old_database_doc': o,
                                               'new_database_doc': n,
                                               'error_description': str(tb.encode())})


def get_module_name():
    """
    FUnction used to get the module name where it is called
    """
    return inspect.stack()[1][3]


def get_arg_lists(frame):
    """
    Function used to get the list of arguments of the function
    where it is called
    """
    args, _, _, values = inspect.getargvalues(frame)
    argument_name_list = []
    argument_value_list = []
    for k in args:
        argument_name_list.append(k)
        argument_value_list.append(values[k])
    return argument_name_list, argument_value_list


class data_change_triggers(object):
    """
    Master Class for the entire class structure of data change triggers
    """

    def __init__(self, change, old_database_doc, new_database_doc):
        """
        :type new_database_doc: dict
        :type change:dict
        :type old_database_doc: dict

        """
        self.get_all_triggers(change, old_database_doc, new_database_doc)
        self.pricing_action_id_at_trigger_time = None

    def process_trigger(self):
        """
        Function called to do the entire processing of trigger
        """
        self.errors = error_class.ErrorObject(0,
                                              self.get_module_name(),
                                              self.get_arg_lists(inspect.currentframe()))
        error_flag = False
        print error_flag
        try:
            t = af.trigger_hierarchy(self)
        except error_class.ErrorObject as esub:
            self.append_error(self.errors, esub)
            if esub.error_level <= error_class.ErrorObject.WARNING:
                error_flag = False
            elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL1:
                error_flag = True
            elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL2:
                error_flag = True
                raise esub
        if not error_flag:
            trigger_list = []
            for i in t:
                if i['triggering_event_type'] == 'data_change':
                    trigger_list.append(i)
        if not error_flag:
            try:
                trigger = self.check_if_trigger(trigger_list)
            except error_class.ErrorObject as esub:
                self.append_error(self.errors, esub)
                if esub.error_level <= error_class.ErrorObject.WARNING:
                    error_flag = False
                elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL1:
                    error_flag = True
                elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL2:
                    error_flag = True
                    raise esub
        if not error_flag:
            try:
                trigger_fired = self.check_if_trigger_fired(trigger)
                print trigger_fired
            except error_class.ErrorObject as esub:
                self.append_error(self.errors, esub)
                if esub.error_level <= error_class.ErrorObject.WARNING:
                    error_flag = False
                elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL1:
                    error_flag = True
                elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL2:
                    error_flag = True
                    raise esub
        if not error_flag:
            try:
                self.trigger_birth(trigger_fired)
            except error_class.ErrorObject as esub:
                self.append_error(self.errors, esub)
                if esub.error_level <= error_class.ErrorObject.WARNING:
                    error_flag = False
                elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL1:
                    error_flag = True
                elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL2:
                    error_flag = True
                    raise esub
        if self.errors.error_list != [] or self.errors.error_object_list != []:
            # Updates all the errors or warnings occured for the process
            db.JUP_DB_Error_Collection.insert_one({'date': datetime.datetime.now().strftime('%Y-%m-%d'),
                                                   'time': datetime.datetime.now().strftime('%H:%M:%S.%f'),
                                                   'change': self.triggering_data,
                                                   'old_database_doc': self.old_doc_data,
                                                   'new_database_doc': self.new_doc_data,
                                                   'error_description': self.errors.__str__()})

    def check_if_trigger(self, trigger_list):
        """
        Takes the trigger_list developed through hierarchy as input and checks
        Checks if there is a trigger defined for this data change
        returns the trigger if yes
        returns None if No trigger is defined
        """
        trigger_consideration = None
        if trigger_list is not []:
            for i in trigger_list:
                if (i['triggering_event']['field'] == self.triggering_data['field'] and
                            i['triggering_event']['collection'] == self.triggering_data['collection'] and
                            i['triggering_event']['action'] == self.triggering_data['action']):
                    trigger_consideration = i
            if trigger_consideration is not None:
                return trigger_consideration
            else:
                e1 = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                             self.get_module_name(),
                                             self.get_arg_lists(inspect.currentframe()))
                e1.append_to_error_list(
                    "Could not obtain trigger to be considered from list of triggers received from hierarchy")
                raise e1
        else:
            e2 = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                         self.get_module_name(),
                                         self.get_arg_lists(inspect.currentframe()))
            e2.append_to_error_list("The Trigger list after hierarchy cannot be a Null array ")
            raise e2

    def check_if_trigger_fired(self, trigger_consideration):
        """
        Takes the output of check_if_trigger as input
        If there is a trigger,checks if the trigger is being fired or not
        If the trigger is being fired,
        returns None if input is None
        returns True if trigger is fired
                False if trigger is not fired
        """
        if trigger_consideration is not None:
            p = trigger_consideration
            qq = (p['priority'] < net.MAX_TRIGGER_PRIORITY_LEVEL)
            if qq:
                diff = -(self.new_doc_data[self.triggering_data['field']]
                        - self.old_doc_data[self.triggering_data['field']])
                print 'diff',diff
                if trigger_consideration['threshold_abs/percent'] == 'absolute':
                    q = float(diff)
                elif trigger_consideration['threshold_abs/percent'] == 'percent':

                    old_val = (float(self.old_doc_data[self.triggering_data['field']]))
                    print 'old_val', old_val
                    q = float(diff * 100 / old_val)
                print q
                if q < p['lower_threshhold'] or q > p['upper_threshhold']:
                    self.trigger_type = p['desc']
                    self.priority = p['priority']
                    trigger_fired = True
                else:
                    trigger_fired = False
            else:
                trigger_fired = False
                e1 = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                             self.get_module_name(),
                                             self.get_arg_lists(inspect.currentframe()))
                e1.append_to_error_list('Max Priority Level is '
                                        + str(net.MAX_TRIGGER_PRIORITY_LEVEL) +
                                        ' but got ' + str(p['priority']))
                raise e1
        else:
            trigger_fired = False
            e1 = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                         self.get_module_name(),
                                         self.get_arg_lists(inspect.currentframe()))
            e1.append_to_error_list('trigger to be considered (input) cannot be None')
            raise e1
        return trigger_fired

    def trigger_birth(self, trigger_fired):
        """
        Takes the output of check_if_trigger_fired as input
        If trigger_fired True further process the trigger
        If trigger_fired is false dump the trigger
        """
        if trigger_fired is True:
            self.get_pricing_action_id_at_trigger_time()
            af.create_trigger(self)
            af.send_trigger_to_queue(self)
        else:
            if trigger_fired == False:
                pass

    def get_module_name(self):
        """
        FUnction used to get the module name where it is called
        """
        return inspect.stack()[1][3]

    def get_arg_lists(self, frame):
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

    def append_error(self, e, esub):
        e.append_to_error_object_list(esub)

    def get_all_triggers(self):
        pass


class competitor_change(data_change_triggers):
    def __init__(self, change, old_database_doc, new_database_doc):
        data_change_triggers.__init__(self, change, old_database_doc, new_database_doc)

    def get_pricing_action_id_at_trigger_time(self):
        change_compt_fb = {'airline': self.old_doc_data['airline'],
                           'farebasis': self.old_doc_data['farebasis']}
        cursor_1 = db.JUP_DB_Host_Pricing_Data.find({'pos': self.old_doc_data['pos'],
                                                     'origin': self.old_doc_data['origin'],
                                                     'destination': self.old_doc_data['destination'],
                                                     'compartment': self.old_doc_data['compartment'],
                                                     'circular_period_start_date':
                                                         {'$lte': SYSTEM_DATE},
                                                     'circular_period_end_date':
                                                         {'$gte': SYSTEM_DATE},
                                                     'competitor_farebasis': {'$elemMatch': change_compt_fb}})
        query_1 = []
        for i in cursor_1:
            del i['_id']
            query_1.append(i)
        if len(query_1) == 1:
            self.pricing_action_id_at_trigger_time = query_1[0]['pricing_action_id']
        elif len(query_1) == 0:
            e1 = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                         self.get_module_name(),
                                         self.get_arg_lists(inspect.currentframe()))
            e1.append_to_error_list(
                'No host fb mapped to ' + str(self.old_doc_data['farebasis']) + ' of ' + 'airline ' + str(
                    self.old_doc_data['airline']))
            raise e1
        elif len(query_1) > 1:
            e1 = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                         self.get_module_name(),
                                         self.get_arg_lists(inspect.currentframe()))
            e1.append_to_error_list(
                'More than 1 host fb mapped to ' + str(self.old_doc_data['farebasis']) + ' of ' + 'airline ' + str(
                    self.old_doc_data['airline']))
            raise e1


class competitor_price_change(competitor_change):
    def __init__(self, change, old_database_doc, new_database_doc):
        competitor_change.__init__(self, change, old_database_doc, new_database_doc)

    def get_all_triggers(self, c, o, n):
        imp_keys = ['airline', 'pos', 'origin', 'destination', 'compartment', 'farebasis']
        min_condition_satisfied = True
        for i in imp_keys:
            if o[i] == 'null' or n[i] == 'null' or o[i] == None or n[i] == None:
                min_condition_satisfied = False
        print min_condition_satisfied
        if min_condition_satisfied:
            cursor_2 = query_competitor_pricing_data({'airline': o['airline'],
                                                      'pos': o['pos'],
                                                      'origin': o['origin'],
                                                      'destination': o['destination'],
                                                      'compartment': o['compartment'],
                                                      'farebasis': o['farebasis'],
                                                      'circular_period_start_date': {'$lte': SYSTEM_DATE},
                                                      'circular_period_end_date': {'$gte': SYSTEM_DATE}})
            query = []
            for i in cursor_2:
                query.append(i)
            print len(query)
            if len(query) != 0:
                for i in query:
                    i['price'] = o['price']
                    self.old_doc_data = deepcopy(i)
                    i['price'] = n['price']
                    self.new_doc_data = deepcopy(i)
                    self.triggering_data = c
                    self.process_trigger()
        else:
            e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                        self.get_module_name(),
                                        self.get_arg_lists(inspect.currentframe()))
            e.append_to_error_list(
                'One of the parameter required for analysis is null in old_database_doc or new_database_doc')
            raise e


class competitor_market_share_change(competitor_change):
    def __init__(self, change, old_database_doc, new_database_doc):
        competitor_change.__init__(self, change, old_database_doc, new_database_doc)

    def get_all_triggers(self, c, o, n):
        imp_keys = ['airline', 'pos', 'origin', 'destination', 'compartment']
        print imp_keys
        min_condition_satisfied = True
        for i in imp_keys:
            if o[i] == 'null' or n[i] == 'null' or o[i] == None or n[i] == None:
                min_condition_satisfied = False
        print min_condition_satisfied
        if min_condition_satisfied:
            cursor_2 = query_competitor_pricing_data({'airline': o['airline'],
                                                      'pos': o['pos'],
                                                      'origin': o['origin'],
                                                      'destination': o['destination'],
                                                      'compartment': o['compartment'],
                                                      'circular_period_start_date': {'$lte': SYSTEM_DATE},
                                                      'circular_period_end_date': {'$gte': SYSTEM_DATE}})
            query = []
            for i in cursor_2:
                del i['_id']
                query.append(i)
            if len(query) != 0:
                for i in query:
                    i['market_share'] = o['market_share']
                    i['month'] = o['month']
                    i['year'] = o['year']
                    self.old_doc_data = deepcopy(i)
                    i['market_share'] = n['market_share']
                    self.new_doc_data = deepcopy(i)
                    self.triggering_data = c
                    self.process_trigger()
        else:
            e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                        self.get_module_name(),
                                        self.get_arg_lists(inspect.currentframe()))
            e.append_to_error_list(
                'One of the parameter required for analysis is null in old_database_doc or new_database_doc')
            raise e


# class competitor_rating_change(competitor_change):
#     def __init__(self, change, old_database_doc, new_database_doc):
#         competitor_change.__init__(self, change, old_database_doc, new_database_doc)
#
#     def get_all_triggers(self, c, o, n):
#         imp_keys = ['airline']
#         min_condition_satisfied = True
#         for i in imp_keys:
#             if o[i] == 'null' or n[i] == 'null' or o[i] == None or n[i] == None:
#                 min_condition_satisfied = False
#         if min_condition_satisfied:
#             cursor_2 = db.JUP_DB_Fare_Triggers.find({'airline': o['airline']})
#             query = []
#             for i in cursor_2:
#                 del i['_id']
#                 query.append(i)
#             if len(query) != 0:
#                 for i in query:
#                     i['rating'] = o['rating']
#                     self.old_doc_data = deepcopy(i)
#                     i['rating'] = n['rating']
#                     self.new_doc_data = deepcopy(i)
#                     self.triggering_data = c
#                     self.process_trigger()
#         else:
#             e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
#                                         self.get_module_name(),
#                                         self.get_arg_lists(inspect.currentframe()))
#             e.append_to_error_list(
#                 'One of the parameter required for analysis is null in old_database_doc or new_database_doc')
#             raise e


class competitor_airline_capacity_percentage_change(competitor_change):
    def __init__(self, change, old_database_doc, new_database_doc):
        competitor_change.__init__(self, change, old_database_doc, new_database_doc)

    def get_all_triggers(self, c, o, n):
        imp_keys = ['airline', 'pos', 'origin', 'destination', 'compartment']
        min_condition_satisfied = True
        for i in imp_keys:
            if o[i] == 'null' or n[i] == 'null' or o[i] == None or n[i] == None:
                min_condition_satisfied = False
        if min_condition_satisfied:
            cursor_2 = query_competitor_pricing_data({'airline': o['airline'],
                                                      'pos': o['pos'],
                                                      'origin': o['origin'],
                                                      'destination': o['destination'],
                                                      'compartment': o['compartment'],
                                                      'circular_period_start_date': {'$lte': SYSTEM_DATE},
                                                      'circular_period_end_date': {'$gte': SYSTEM_DATE}})
            query = []
            for i in cursor_2:
                query.append(i)
            if len(query) != 0:
                for i in query:
                    i['capacity'] = o['capacity']
                    self.old_doc_data = deepcopy(i)
                    i['capacity'] = n['capacity']
                    self.new_doc_data = deepcopy(i)
                    self.triggering_data = c
                    self.process_trigger()


# class competitor_exit(data_change_triggers):
#   def __init__(self,change,old_database_doc,new_database_doc):
#       competitor_change.__init__(self,change,old_database_doc,new_database_doc)
#   def get_all_triggers(self,c,o,n):
#       query = access_db(net.MONGO_CLIENT_URL,
#                       net.TRIGGER_DB_NAME,
#                       'JUP_DB_Competitor_Pricing_Data',
#                       {'origin':o['origin'],
#                       'destination':o['destination']})
#       for i in query:
#           self.old_doc_data = deepcopy(i)
#           self.new_doc_data = None
#           self.triggering_data = c
#           self.process_trigger()

# class competitor_new_entry(data_change_triggers):
#   def __init__(self,change,old_database_doc,new_database_doc):
#       competitor_change.__init__(self,change,old_database_doc,new_database_doc)
#   def get_all_triggers(self,c,o,n):
#       query = access_db(net.MONGO_CLIENT_URL,
#                       net.TRIGGER_DB_NAME,
#                       'JUP_DB_Competitor_Pricing_Data',
#                       {'origin':o['origin'],
#                       'destination':o['destination']})
#       for i in query:
#           self.old_doc_data = None
#           self.new_doc_data = deepcopy(i)
#           self.triggering_data = c
#           self.process_trigger()

class host_change(data_change_triggers):
    def __init__(self, change, old_database_doc, new_database_doc):
        data_change_triggers.__init__(self, change, old_database_doc, new_database_doc)

    def get_pricing_action_id_at_trigger_time(self):
        self.pricing_action_id_at_trigger_time = self.old_doc_data['pricing_action_id']


# class host_rating_change(host_change):
#     def __init__(self, change, old_database_doc, new_database_doc):
#         host_change.__init__(self, change, old_database_doc, new_database_doc)
#
#     def get_all_triggers(self, c, o, n):
#         cursor_2 = db.JUP_DB_Host_Pricing_Data.find({'airline': o['airline']})
#         query = []
#         for i in cursor_2:
#             del i['_id']
#             query.append(i)
#         if len(query) != 0:
#             for i in query:
#                 i['rating'] = o['rating']
#                 self.old_doc_data = deepcopy(i)
#                 i['rating'] = n['rating']
#                 self.new_doc_data = deepcopy(i)
#                 self.triggering_data = c
#                 self.process_trigger()


class host_market_share_change(host_change):
    def __init__(self, change, old_database_doc, new_database_doc):
        host_change.__init__(self, change, old_database_doc, new_database_doc)

    def get_all_triggers(self, c, o, n):
        imp_keys = ['pos', 'od', 'compartment', 'MarketingCarrier1', 'market_share']
        print imp_keys

        min_condition_satisfied = True
        print min_condition_satisfied
        for i in imp_keys:
            if o[i] == 'null' or n[i] == 'null' or o[i] == None or n[i] == None:
                min_condition_satisfied = False
        print min_condition_satisfied
        if min_condition_satisfied:
            cursor_2 = db.JUP_DB_Host_Pricing_Data.find({'pos': o['pos'],
                                                         'origin': o['od'][:3],
                                                         'destination': o['od'][3:],
                                                         'compartment': o['compartment'],
                                                         'circular_period_start_date': {'$lte': SYSTEM_DATE},
                                                         'circular_period_end_date': {'$gte': SYSTEM_DATE}})
            query = []
            for i in cursor_2:
                del i['_id']
                query.append(i)
            if len(query) != 0:
                for i in query:
                    i['market_share'] = o['market_share']
                    self.old_doc_data = deepcopy(i)
                    i['market_share'] = n['market_share']
                    self.new_doc_data = deepcopy(i)
                    self.triggering_data = c
                    self.process_trigger()
        else:
            e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                        self.get_module_name(),
                                        self.get_arg_lists(inspect.currentframe()))
            e.append_to_error_list(
                'One of the parameter required for analysis is null in old_database_doc or new_database_doc')
            raise e


class host_airline_capacity_percentage_change(host_change):
    def __init__(self, change, old_database_doc, new_database_doc):
        host_change.__init__(self, change, old_database_doc, new_database_doc)

    def get_all_triggers(self, c, o, n):
        imp_keys = ['pos', 'origin', 'destination', 'compartment']
        min_condition_satisfied = True
        for i in imp_keys:
            if o[i] == 'null' or n[i] == 'null' or o[i] == None or n[i] == None:
                min_condition_satisfied = False
        if min_condition_satisfied:
            cursor_2 = db.JUP_DB_Host_Pricing_Data.find({'pos': o['pos'],
                                                         'origin': o['origin'],
                                                         'destination': o['destination'],
                                                         'compartment': o['compartment'],
                                                         'circular_period_start_date': {'$lte': SYSTEM_DATE},
                                                         'circular_period_end_date': {'$gte': SYSTEM_DATE}})
            query = []
            for i in cursor_2:
                del i['_id']
                query.append(i)
            if len(query) != 0:
                for i in query:
                    i['capacity'] = o['capacity']
                    self.old_doc_data = deepcopy(i)
                    i['capacity'] = n['capacity']
                    self.new_doc_data = deepcopy(i)
                    self.triggering_data = c
                    self.process_trigger()
        else:
            e = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                        self.get_module_name(),
                                        self.get_arg_lists(inspect.currentframe()))
            e.append_to_error_list(
                'One of the parameter required for analysis is null in old_database_doc or new_database_doc')
            raise e

# class event_triggers(data_change_triggers):
#   def __init__(self,change,old_database_doc,new_database_doc):
#       data_change_triggers.__init__(self,change,old_database_doc,new_database_doc)
#   def get_pricing_action_id_at_trigger_time(self):
#       self.pricing_action_id_at_trigger_time = None

# class type_of_market_change(event_triggers):
#   def __init__(self,change,old_database_doc,new_database_doc):
#       event_triggers.__init__(self,change,old_database_doc,new_database_doc)
#   def get_all_triggers(self,c,o,n):
#       pass


# class condition_of_market_change(event_triggers):
#   def __init__(self,change,old_database_doc,new_database_doc):
#       event_triggers.__init__(self,change,old_database_doc,new_database_doc)
#   def get_all_triggers(self,c,o,n):
#       pass

# class yq_trigger(event_triggers):
#   def __init__(self,change,old_database_doc,new_database_doc):
#       event_triggers.__init__(self,change,old_database_doc,new_database_doc)
#   def get_all_triggers(self,c,o,n):
#       pass

# class exchange_rate_change(event_triggers):
#   def __init__(self,change,old_database_doc,new_database_doc):
#       event_triggers.__init__(self,change,old_database_doc,new_database_doc)
#   def get_all_triggers(self,c,o,n):
#       pass

# class event(event_triggers):
#   def __init__(self,change,old_database_doc,new_database_doc):
#       event_triggers.__init__(self,change,old_database_doc,new_database_doc)
#   def get_all_triggers(self,c,o,n):
#       pass

# class cost_change(event_triggers):
#   def __init__(self,change,old_database_doc,new_database_doc):
#       event_triggers.__init__(self,change,old_database_doc,new_database_doc)
#   def get_all_triggers(self,c,o,n):
#       pass
