"""
Author: Abhinav Garg
Created with <3
Date Created: 2018-09-06
Code functionality:
     Class for Rule Level Trigger Analysis, Getting Effective Sequences for all rule for which
     trigger is raised and updating Rules Workflow collection
Modifications log:
    1. Author:
       Exact modification made or some logic changed:
       Date of modification:
    2. Author:
       Exact modification made or some logic changed:
       Date of modification:

"""
from bson import ObjectId

from jupiter_AI import JUPITER_LOGGER, JUPITER_DB, client, Host_Airline_Code, today, SYSTEM_DATE, ATPCO_DB
from jupiter_AI.logutils import measure
from jupiter_AI.common import ClassErrorObject as error_class
import pandas as pd
import time
import datetime
import inspect

db = client[JUPITER_DB]
ATPCO = client[ATPCO_DB]


class trigger(object):
    @measure(JUPITER_LOGGER)
    def do_analysis(self):
        """
        :return:
        """
        error_flag = False
        try:
            self.triggering_event()
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
            self.data_through_old_doc_change()

        self.trigger_types()

        if not error_flag:
            self.check_min_interval()
            self.check_effective_dates()

        if not error_flag:
            self.get_desc()

        if not self.min_interval_factor:
            try:
                self.update_rule_level_collection()
            except error_class.ErrorObject as esub:
                self.append_error(self.errors, esub)
                if esub.error_level <= error_class.ErrorObject.WARNING:
                    error_flag = False
                elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL1:
                    error_flag = True
                elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL2:
                    error_flag = True
                    raise esub

            # print error_flag
            # print self.errors.__str__()
            # print self.errors.error_list
            # print self.errors.error_object_list
            if self.errors.error_list != [] or self.errors.error_object_list != []:
                db.JUP_DB_Error_Collection.insert_one({'triggering_event_id': self.triggering_event_id,
                                                       'error_descripetion': self.errors.__str__()})
                print 'Error Occured and Updated in dB'
            if not error_flag:
                # try:
                self.update_db()
                # except Exception:
                #     pass

    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        '''
        Instaniating method for base class of triggers
        Calling of all the functions required for Analysis is done here
        '''
        self.errors = error_class.ErrorObject(
            0, self.get_module_name(), self.get_arg_lists(
                inspect.currentframe()))
        self.triggering_event_id = triggering_event_id
        self.airline = Host_Airline_Code
        self.process_start_date = datetime.datetime.strftime(today, '%Y-%m-%d')
        # self.process_start_time = datetime.datetime.now().strftime('%H:%M:%S.%f') # to be uncommented when live
        self.process_start_time = "08:00"

    @measure(JUPITER_LOGGER)
    def triggering_event(self):
        '''
        Collects the following data relevant to trigger from collection
        '''
        cursor = db.JUP_DB_Triggering_Event.find(
            {'_id': self.triggering_event_id})
        if cursor.count() == 1:
            p = cursor[0]
            # print p
            self.triggering_event_date = p['date']
            self.triggering_event_time = p['time']
            self.trigger_id = p['trigger_id']
            self.unique_trigger_id = p['unique_trigger_id']
            self.triggering_data = p['triggering_data']
            self.cat_details = p['category_details']
            self.old_doc_data = p['old_doc_data']
            self.new_doc_data = p['new_doc_data']
            self.trigger_type = p['trigger_type']
            self.lower_threshold = p['lower_threshold']
            self.upper_threshold = p['upper_threshold']
            self.threshold_type = p['threshold_type']
            self.priority = p['priority']
            self.min_interval = p['min_interval']
            self.old_pricing_action_id_at_trigger_time = p['pricing_action_id_at_trigger_time'],
            self.is_event_trigger = p['is_event_trigger']
        else:
            e1 = error_class.ErrorObject(
                error_class.ErrorObject.ERRORLEVEL1,
                self.get_module_name(),
                self.get_arg_lists(
                    inspect.currentframe()))
            e1.append_to_error_list("Expected 1 trigger in triggering event for the id " + str(
                self.triggering_event_id) + " but got " + str(cursor.count()))
            raise e1

    @measure(JUPITER_LOGGER)
    def update_rule_level_collection(self):
        st = time.time()
        # SYSTEM_DATE = datetime.datetime.now().strftime('%Y-%m-%d')
        crsr = db.JUP_DB_Rules_Workflow_User.find({
            'tariff_code': self.tariff_code,
            'fare_rule': self.fare_rule,
            'cat_no': self.cat_no,
            'update_date': SYSTEM_DATE
        })

        print "Time taken to check whether trigger already raised for this tariff, rule and cat = ", time.time() - st
        if len(list(crsr)) == 0:
            rules_data = self.get_rules_data()
            response = dict(
                tariff_code=self.tariff_code,
                fare_rule=self.fare_rule,
                cat_no=self.cat_no,
                update_date=SYSTEM_DATE,
                rules_docs=rules_data
            )
            db.JUP_DB_Rules_Workflow_User.insert(response)
            print 'Inserted into Rules WORKFLOW User !'

    @measure(JUPITER_LOGGER)
    def get_rules_data(self):
        '''
        fetched all active sequences with corresponding table number for
        the tariff, rule id and cat combination
        :return:
        '''
        cur = ATPCO.JUP_DB_ATPCO_Record_2_Cat_All.find({'CXR_CODE': {'$eq': Host_Airline_Code},
                                                        'CAT_NO': self.cat_no,
                                                        'TARIFF': self.tariff_code,
                                                        'RULE_NO': self.fare_rule,
                                                        'DATES_EFF_1': {'$lte': today.strftime('%Y%m%d')},
                                                        'DATES_DISC_1': {'$gt': today.strftime('%Y%m%d')},
                                                        })
        rules_docs = []
        for i in cur:
            tbl = 1
            tbl_no_list = []
            ri_list = {}
            while tbl <= 200:
                try:
                    tbl_no_list.append(i["DATA_TABLE_STRING_TBL_NO_" + str(tbl)])
                    ri_list[i["DATA_TABLE_STRING_TBL_NO_" + str(tbl)]] = i[
                        "DATA_TABLE_STRING_RI_" + str(tbl)]
                    tbl += 1
                except KeyError:
                    break
            cur_1 = ATPCO[self.record_3].find({'TBL_NO': {'$in': tbl_no_list}})
            temp_list = []
            for k in cur_1:
                k["RI"] = ri_list[k["TBL_NO"]]
                k["SEQ_NO"] = i["SEQ_NO"]
                temp_list.append(k)
            i['record_3'] = temp_list
            rules_docs.append(i)
        return rules_docs

    @measure(JUPITER_LOGGER)
    def trigger_types(self):
        '''
        Takes the trigger_list developed during hierarchy as input
        Returns the document containing the parameters of the trigger
        we are dealing with now
        '''
        crsr = db.JUP_DB_Trigger_Types.find({'desc': self.trigger_type})
        data = list(crsr)
        if len(data) >= 1:
            q = data[0]
            self.pricing_type = q['pricing_type']
            self.pricing_method = q['pricing_method']
            self.dashboard = q['dashboard']
            self.active = q['active']
            self.trigger_status = q['status']
            self.triggering_event_type = q['triggering_event_type']
            self.triggering_event = q['triggering_event']
            self.frequency = q['frequency']
            self.effective_date_from = q['effective_date_from']
            self.effective_date_to = q['effective_date_to']
            self.type_of_trigger = q['type_of_trigger']
        else:
            e1 = error_class.ErrorObject(
                error_class.ErrorObject.ERRORLEVEL1,
                self.get_module_name(),
                self.get_arg_lists(
                    inspect.currentframe()))
            e1.append_to_error_list(
                "Expected 1 trigger type but got " + str(len(data)))
            raise e1

    @measure(JUPITER_LOGGER)
    def data_through_old_doc_change(self):
        '''
        Data to be collected from old_doc_data
        '''
        self.tariff_code = self.old_doc_data['tariff_code']
        self.fare_rule = self.old_doc_data['fare_rule']
        self.cat_no = self.old_doc_data['cat_no']
        self.record_3 = self.old_doc_data['record_3']

    @measure(JUPITER_LOGGER)
    def check_min_interval(self):
        '''
        Function checks if the interval b/w the last time a
        pricing action has been approved and the present time(t)
        is greater than or less than the min interval defined
        True if t <= min interval
        False if t >= min interval
        '''
        q = db.JUP_DB_Rules_Workflow.find({
            'tariff_code': self.tariff_code,
            'fare_rule': self.fare_rule,
            'cat_no': self.cat_no,
            'trigger_type': self.trigger_type,
            'status': {'$in': ['accepted', 'Autoaccepted']}
        }).sort([{'action_date': 1, 'action_time': 1}])
        if q.count() == 0:
            self.min_interval_factor = False
        else:
            last_pricing_action = q[q.count() - 1]
            last_occurence_str = last_pricing_action['action_date'] + \
                                 ' ' + last_pricing_action['action_time']
            last_occurence = datetime.datetime.strptime(
                last_occurence_str, '%Y-%m-%d %H:%M:%S.%f')
            delta_hours = (datetime.datetime.now() - last_occurence)
            if delta_hours >= datetime.timedelta(hours=self.min_interval):
                self.min_interval_factor = False
            else:
                self.min_interval_factor = True

    @measure(JUPITER_LOGGER)
    def check_effective_dates(self):
        '''
        Checks if the current date falls in the window of effective dates
        False if today in the window
        True if today violates the window
        '''
        t = datetime.datetime.now()
        effective_date_from = datetime.datetime.strptime(
            self.effective_date_from, '%d-%m-%Y')
        effective_date_to = datetime.datetime.strptime(
            self.effective_date_to, '%d-%m-%Y')
        if effective_date_from < t < effective_date_to:
            self.effective_date_factor = False
        else:
            self.effective_date_factor = True

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        '''
        Function that defines the Description for this trigger
        Defined for individual triggers later
        '''
        pass

    @measure(JUPITER_LOGGER)
    def update_db(self):
        '''
        Function to update the database
        if errors occured update errors
        else update the recommendation/trigger
        '''

        wf_crsr = db.JUP_DB_Rules_Workflow.find({
            'tariff_code': self.tariff_code,
            'fare_rule': self.fare_rule,
            'cat_no': self.cat_no,
            'update_date': SYSTEM_DATE,
            "trigger_type": self.trigger_type,
            "status": "pending",
        })

        wf_crsr = list(wf_crsr)
        if len(wf_crsr) == 0:
            del self.errors
            # print 'DONE'
            pricing_actions = self.__dict__
            print "trigger_type: ", self.trigger_type

            response = dict(
                tariff_code=self.tariff_code,
                fare_rule=self.fare_rule,
                cat_no=self.cat_no,
                update_date=SYSTEM_DATE
            )

            pricing_actions.update(response)
            pricing_actions['key'] = self.tariff_code + self.fare_rule + self.cat_no
            pricing_actions['status'] = 'pending'
            pricing_actions['action_date'] = None
            pricing_actions['action_time'] = None
            # print self.triggering_event_id
            print "-------------->"
            pricing_actions['triggering_event_id'] = str(self.triggering_event_id)
            cur = db.JUP_DB_Trigger_Types.find({'desc': self.trigger_type})
            for i in cur:
                pricing_actions['trigger_class'] = i['trigger_class']
            # print pricing_actions
            db.JUP_DB_Rules_Workflow.insert(pricing_actions)
            print "Inserted into Rules WORKFLOW ! "

    @staticmethod
    @measure(JUPITER_LOGGER)
    def get_module_name():
        return inspect.stack()[1][3]

    @staticmethod
    @measure(JUPITER_LOGGER)
    def get_arg_lists(frame):
        args, _, _, values = inspect.getargvalues(frame)
        argument_name_list = []
        argument_value_list = []
        for k in args:
            argument_name_list.append(k)
            argument_value_list.append(values[k])
        return argument_name_list, argument_value_list

    @staticmethod
    @measure(JUPITER_LOGGER)
    def append_error(e, esub):
        e.append_to_error_object_list(esub)


#   ************************************    RULE CHANGE TRIGGERS CLASSES    *******************************************


class competitor_max_stay_change(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(
            competitor_max_stay_change,
            self).__init__(triggering_event_id)

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        print self.old_doc_data['eff_from'], self.old_doc_data['eff_to']
        desc = [
            "Competitor Max Stay Change",
            '_',
            "ID: ", self.trigger_id,
            '_',
            "Carrier: ", str(self.old_doc_data['competitor'].encode()),
            '_',
            "Tariff Code: ", str(self.old_doc_data['tariff_code'].encode()),
            '_',
            "Competitor Rule ID: ", str(self.old_doc_data['comp_fare_rule'].encode()),
            '_',
            "Competitor Sequence No: ", str(self.old_doc_data['comp_seq'].encode()),
            '_',
            "Max Stay (Old): ", str(self.old_doc_data['max_stay']) + str(self.old_doc_data['unit']),
            '_',
            "Max Stay (New): ", str(self.new_doc_data['max_stay']) + str(self.new_doc_data['unit']),
            '_',
            "Effective Dates(Old): ",
                               datetime.datetime.strptime(self.old_doc_data['eff_from'], '%Y%m%d').strftime(
                                   '%d-%m-%Y') + " to " +
                               datetime.datetime.strptime(self.old_doc_data['eff_to'], '%Y%m%d').strftime('%d-%m-%Y'),
            '_',
            "Effective Dates(New): ",
                               datetime.datetime.strptime(self.new_doc_data['eff_from'], '%Y%m%d').strftime(
                                   '%d-%m-%Y') + " to " +
                               datetime.datetime.strptime(self.new_doc_data['eff_to'], '%Y%m%d').strftime('%d-%m-%Y'),
            '_',
            'Thresholds(', self.threshold_type, ')',
                               ' - ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
                               'Raised On: ' + datetime.datetime.strptime(self.triggering_event_date,
                                                                          '%Y-%m-%d').strftime('%d-%m-%Y') +
                               ' at ' + str(self.triggering_event_time)
        ]
        self.desc = ''.join(desc).encode('utf-8')


class competitor_min_stay_change(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(
            competitor_min_stay_change,
            self).__init__(triggering_event_id)

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        desc = [
            "Competitor Min Stay Change",
            '_',
            "ID: ", self.trigger_id,
            '_',
            "Carrier: ", str(self.old_doc_data['competitor'].encode()),
            '_',
            "Tariff Code: ", str(self.old_doc_data['tariff_code'].encode()),
            '_',
            "Competitor Rule ID: ", str(self.old_doc_data['comp_fare_rule'].encode()),
            '_',
            "Competitor Sequence No: ", str(self.old_doc_data['comp_seq'].encode()),
            '_',
            "Min Stay (Old): ", str(self.old_doc_data['min_stay']) + str(self.old_doc_data['unit']),
            '_',
            "Min Stay (New): ", str(self.new_doc_data['min_stay']) + str(self.new_doc_data['unit']),
            '_',
            "Effective Dates(Old): ",
                               datetime.datetime.strptime(self.old_doc_data['eff_from'], '%Y%m%d').strftime(
                                   '%d-%m-%Y') + " to " +
                               datetime.datetime.strptime(self.old_doc_data['eff_to'], '%Y%m%d').strftime('%d-%m-%Y'),
            '_',
            "Effective Dates(New): ",
                               datetime.datetime.strptime(self.new_doc_data['eff_from'], '%Y%m%d').strftime(
                                   '%d-%m-%Y') + " to " +
                               datetime.datetime.strptime(self.new_doc_data['eff_to'], '%Y%m%d').strftime('%d-%m-%Y'),
            '_',
            'Thresholds(', self.threshold_type, ')',
                               ' - ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
                               'Raised On: ' + datetime.datetime.strptime(self.triggering_event_date,
                                                                          '%Y-%m-%d').strftime('%d-%m-%Y') +
                               ' at ' + str(self.triggering_event_time)
        ]
        self.desc = ''.join(desc).encode('utf-8')


class competitor_advanced_purchase_change(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(
            competitor_advanced_purchase_change,
            self).__init__(triggering_event_id)

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        desc = [
            "Competitor Advanced Purchase Change",
            '_',
            "ID: ", self.trigger_id,
            '_',
            "Carrier: ", str(self.old_doc_data['competitor'].encode()),
            '_',
            "Tariff Code: ", str(self.old_doc_data['tariff_code'].encode()),
            '_',
            "Competitor Rule ID: ", str(self.old_doc_data['comp_fare_rule'].encode()),
            '_',
            "Competitor Sequence No: ", str(self.old_doc_data['comp_seq'].encode()),
            '_',
            "Advanced Purchase (Old): ", str(self.old_doc_data['tktg_period']) + str(self.old_doc_data['tktg_unit']),
            '_',
            "Advanced Purchase (New): ", str(self.new_doc_data['tktg_period']) + str(self.new_doc_data['tktg_unit']),
            '_',
            "Effective Dates(Old): ",
                               datetime.datetime.strptime(self.old_doc_data['eff_from'], '%Y%m%d').strftime(
                                   '%d-%m-%Y') + " to " +
                               datetime.datetime.strptime(self.old_doc_data['eff_to'], '%Y%m%d').strftime('%d-%m-%Y'),
            '_',
            "Effective Dates(New): ",
                               datetime.datetime.strptime(self.new_doc_data['eff_from'], '%Y%m%d').strftime(
                                   '%d-%m-%Y') + " to " +
                               datetime.datetime.strptime(self.new_doc_data['eff_to'], '%Y%m%d').strftime('%d-%m-%Y'),
            '_',
            'Thresholds(', self.threshold_type, ')',
                               ' - ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
                               'Raised On: ' + datetime.datetime.strptime(self.triggering_event_date,
                                                                          '%Y-%m-%d').strftime('%d-%m-%Y') +
                               ' at ' + str(self.triggering_event_time)
        ]
        self.desc = ''.join(desc).encode('utf-8')

if __name__ == "__main__":
    p = competitor_max_stay_change(ObjectId("5b912a8ae545b97499f97c59"))
    p.do_analysis()