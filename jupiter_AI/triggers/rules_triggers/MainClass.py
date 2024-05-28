"""
Author: Abhinav Garg
Created with <3
Date Created: 2018-09-05
Code functionality:
    Main Class for all Competitor Rule Change Triggers
Modifications log:
    1. Author:
       Exact modification made or some logic changed:
       Date of modification:
    2. Author:
       Exact modification made or some logic changed:
       Date of modification:

"""
from jupiter_AI.logutils import measure
from jupiter_AI.network_level_params import SYSTEM_DATE, JUPITER_LOGGER
from jupiter_AI.triggers.MainClass import Trigger
from jupiter_AI.triggers.common import generate_rule_trigger_id
import jupiter_AI.triggers.analytics_functions as af


class RuleChange(Trigger):
    """
    """
    @measure(JUPITER_LOGGER)
    def __init__(self, name, old_database_doc, new_database_doc):
        self.trigger_date = SYSTEM_DATE
        self.triggering_event = name
        self.trigger_type = name
        self.old_doc_data = old_database_doc
        self.new_doc_data = new_database_doc

    @measure(JUPITER_LOGGER)
    def do_analysis(self, db):
        """
        :return:
        """
        self.get_trigger_details(trigger_name=self.trigger_type, db=db)
        self.build_triggering_data()
        # print 'Triggering_Data', self.triggering_data
        trigger_status = self.check_trigger()
        print 'Trigger Status Checked', trigger_status
        # print 'self old doc: ', self.old_doc_data
        # print "dep_date_start: ", self.triggering_data['dep_date_start']
        # print "dep_date_end: ", self.triggering_data['dep_date_end']
        id = self.generate_rule_trigger(trigger_status=trigger_status)
        return id

    @measure(JUPITER_LOGGER)
    def build_triggering_data(self):
        '''
        :return:
        '''
        self.triggering_data = dict(competitor=self.new_doc_data['competitor'],
                                    comp_fare_rule=self.new_doc_data['comp_fare_rule'],
                                    comp_seq=self.new_doc_data['comp_seq'])
        print 'Built Triggering Data'

    @measure(JUPITER_LOGGER)
    def check_trigger(self):
        '''
        :return:
        '''
        pass

    @measure(JUPITER_LOGGER)
    def generate_rule_trigger(self, trigger_status, db):
        """
        :param trigger_status:
        :return:
        """
        if trigger_status:
            # print "old doc------>"
            # print self.old_doc_data

            self.trigger_id, self.unique_trigger_id = generate_rule_trigger_id(
                trigger_name=self.trigger_type,
                competitor=self.new_doc_data['competitor'],
                comp_fare_rule=self.new_doc_data['comp_fare_rule'],
                comp_seq=self.new_doc_data['comp_seq'])

            print "trigger type: ", self.trigger_type
            print 'Trigger Id', self.trigger_id
            # print 'Trigger Status', trigger_status
            self.cat_details = dict()
            self.cat_details['category'] = 'A'  # hard coded for now, need to build logic later to calculate this
            #   To be visited Again
            self.pricing_action_id_at_trigger_time = None
            # print self.__dict__
            # print json.dumps(self.__dict__, default=json_util.default, indent=1)
            #   Creating the Trigger
            id = af.create_trigger(self, db=db)

            return id

