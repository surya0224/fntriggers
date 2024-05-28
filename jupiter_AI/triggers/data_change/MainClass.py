"""
"""
from jupiter_AI.logutils import measure
from jupiter_AI.network_level_params import SYSTEM_DATE, JUPITER_LOGGER
from jupiter_AI.triggers.MainClass import Trigger


class DataChange(Trigger):
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
        print 'Triggering_Data', self.triggering_data
        trigger_status = self.check_trigger()
        print 'Trigger Status Checked', trigger_status
        print 'self old doc: ', self.old_doc_data
        print "dep_date_start: ", self.triggering_data['dep_date_start']
        print "dep_date_end: ", self.triggering_data['dep_date_end']
        id = self.generate_trigger_new(trigger_status=trigger_status,
                                       dep_date_start=self.triggering_data['dep_date_start'],
                                       dep_date_end=self.triggering_data['dep_date_end'],
                                       db=db)
        return id

    @measure(JUPITER_LOGGER)
    def build_triggering_data(self):
        # self.triggering_data = dict(dep_date_start=None,
        #                             dep_date_end=None)
        pass

    @measure(JUPITER_LOGGER)
    def check_trigger(self):

        '''
        :return:
        '''
        pass