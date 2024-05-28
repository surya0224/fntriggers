"""
Author: Prathyusha Gontla
End date of developement: 2017-9-20
Code functionality:
             Defined 4 types of Promotions Triggers classes. Classes are called while raising trigger in scraping code.

MODIFICATIONS LOG
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :

"""

from copy import deepcopy

from jupiter_AI.triggers.data_change.MainClass import DataChange
from jupiter_AI.triggers.common import get_start_end_dates
from jupiter_AI.network_level_params import SYSTEM_DATE, today, INF_DATE_STR, JUPITER_LOGGER
from jupiter_AI.logutils import measure
import datetime

# 3 100, 120, 80 no. of promotions trigger

class PromoRuleChangeTrigger(DataChange):

    @measure(JUPITER_LOGGER)
    def __init__(self, name, old_doc_data, new_doc_data, changed_field):

        print "entered class "
        old_doc_data['origin'] = old_doc_data['OD'][:3]
        old_doc_data['destination'] = old_doc_data['OD'][3:]
        new_doc_data['origin'] = new_doc_data['OD'][:3]
        new_doc_data['destination'] = new_doc_data['OD'][3:]
        old_doc_data['pos'] = old_doc_data['OD'][:3]
        new_doc_data['pos'] = new_doc_data['OD'][:3]

        self.old_doc_data = deepcopy(old_doc_data)
        self.new_doc_data = deepcopy(new_doc_data)
        self.changed_field = deepcopy(changed_field)

        self.triggering_event = dict(
            collection='JUP_DB_Promotions',
            field=changed_field,
            action='change'
        )

        print 'Built Triggering Event'

        print 'Class Called'
        DataChange.__init__(self, name, old_doc_data, new_doc_data)

    """
    This Class represents in analysing the trigger before sending it to the priority queue
    """

    @measure(JUPITER_LOGGER)
    def build_triggering_data(self):
        print "old_doc: ", self.old_doc_data
        print "new_doc: ", self.new_doc_data
        try:
            self.triggering_data = dict(dep_date_start=self.new_doc_data['dep_date_from'],
                                        dep_date_end=self.new_doc_data['dep_date_to'])
            if dict(dep_date_start=self.new_doc_data[''],
                    dep_date_end=self.new_doc_data['']):
                dep_date_end_object = today + datetime.timedelta(days=90)
                dep_date_end = datetime.datetime.strftime(dep_date_end_object, '%Y-%m-%d')

                self.triggering_data = dict(dep_date_start=SYSTEM_DATE,
                                            dep_date_end=dep_date_end)



        except KeyError:
            dep_date_end_object = today + datetime.timedelta(days=90)
            dep_date_end = datetime.datetime.strftime(dep_date_end_object, '%Y-%m-%d')

            self.triggering_data = dict(dep_date_start=SYSTEM_DATE,
                                        dep_date_end=dep_date_end)

        print 'Built Triggering Data'

    @measure(JUPITER_LOGGER)
    def check_trigger(self):
        self.change = '---'
        return True

class PromoDateChangeTrigger(DataChange):

    @measure(JUPITER_LOGGER)
    def __init__(self, name, old_doc_data, new_doc_data, changed_field):

        print "entered class "
        old_doc_data['origin'] = old_doc_data['OD'][:3]
        old_doc_data['destination'] = old_doc_data['OD'][3:]
        new_doc_data['origin'] = new_doc_data['OD'][:3]
        new_doc_data['destination'] = new_doc_data['OD'][3:]
        old_doc_data['pos'] = old_doc_data['OD'][:3]
        new_doc_data['pos'] = new_doc_data['OD'][:3]

        self.old_doc_data = deepcopy(old_doc_data)
        self.new_doc_data = deepcopy(new_doc_data)
        self.changed_field = deepcopy(changed_field)

        self.triggering_event = dict(
            collection='JUP_DB_Promotions',
            field=changed_field,
            action='change'
        )

        print 'Built Triggering Event'

        print 'Class Called'
        DataChange.__init__(self, name, old_doc_data, new_doc_data)

    """
    This Class represents in analysing the trigger before sending it to the priority queue
    """

    @measure(JUPITER_LOGGER)
    def build_triggering_data(self):
        print "old_doc: ", self.old_doc_data
        print "new_doc: ", self.new_doc_data
        try:
            self.triggering_data = dict(dep_date_start=self.new_doc_data['dep_date_from'],
                                        dep_date_end=self.new_doc_data['dep_date_to'])
            if dict(dep_date_start=self.new_doc_data[''],
                    dep_date_end=self.new_doc_data['']):
                dep_date_end_object = today + datetime.timedelta(days=90)
                dep_date_end = datetime.datetime.strftime(dep_date_end_object, '%Y-%m-%d')

                self.triggering_data = dict(dep_date_start=SYSTEM_DATE,
                                            dep_date_end=dep_date_end)



        except KeyError:
            dep_date_end_object = today + datetime.timedelta(days=90)
            dep_date_end = datetime.datetime.strftime(dep_date_end_object, '%Y-%m-%d')

            self.triggering_data = dict(dep_date_start=SYSTEM_DATE,
                                        dep_date_end=dep_date_end)

        print 'Built Triggering Data'

    @measure(JUPITER_LOGGER)
    def check_trigger(self):
        self.change = '---'
        return True

class PromoFareChangeTrigger(DataChange):

    @measure(JUPITER_LOGGER)
    def __init__(self, name, old_doc_data, new_doc_data, changed_field):
        print "entered class "
        old_doc_data['origin'] = old_doc_data['OD'][:3]
        old_doc_data['destination'] = old_doc_data['OD'][3:]
        new_doc_data['origin'] = new_doc_data['OD'][:3]
        new_doc_data['destination'] = new_doc_data['OD'][3:]
        old_doc_data['pos'] = old_doc_data['OD'][:3]
        new_doc_data['pos'] = new_doc_data['OD'][:3]

        self.old_doc_data = deepcopy(old_doc_data)
        self.new_doc_data = deepcopy(new_doc_data)
        self.changed_field = deepcopy(changed_field)

        self.triggering_event = dict(
            collection='JUP_DB_Promotions',
            field=changed_field,
            action='change'
        )
        print 'Built Triggering Event'
        print 'Class Called'
        DataChange.__init__(self, name, old_doc_data, new_doc_data)

    """
    This Class represents in analysing the trigger before sending it to the priority queue
    """

    @measure(JUPITER_LOGGER)
    def build_triggering_data(self):
        print "old_doc: ", self.old_doc_data
        print "new_doc: ", self.new_doc_data
        try:
            self.triggering_data = dict(dep_date_start = self.new_doc_data['dep_date_from'],
                                        dep_date_end = self.new_doc_data['dep_date_to'])
            if dict(dep_date_start = self.new_doc_data[''],
                                        dep_date_end = self.new_doc_data['']):
                dep_date_end_object = today + datetime.timedelta(days=90)
                dep_date_end = datetime.datetime.strftime(dep_date_end_object, '%Y-%m-%d')

                self.triggering_data = dict(dep_date_start=SYSTEM_DATE,
                                            dep_date_end=dep_date_end)



        except KeyError:
            dep_date_end_object = today + datetime.timedelta(days=90)
            dep_date_end = datetime.datetime.strftime(dep_date_end_object, '%Y-%m-%d')

            self.triggering_data = dict(dep_date_start= SYSTEM_DATE,
                                        dep_date_end= dep_date_end)

        print 'Built Triggering Data'

    @measure(JUPITER_LOGGER)
    def check_trigger(self):
        self.change = '---'
        return True

    # @measure(JUPITER_LOGGER)
    # def __init__(self, name, old_doc_data, new_doc_data, changed_field):
    #
    #     self.old_doc_data = deepcopy(old_doc_data)
    #     self.new_doc_data = deepcopy(new_doc_data)
    #     self.changed_field = deepcopy(changed_field)
    #
    #     self.triggering_event = dict(
    #         collection='JUP_DB_Promotions',
    #         field=changed_field,
    #         action='change'
    #     )
    #     self.old_doc_data['origin'] = self.old_doc_data['OD'][:3]
    #     self.old_doc_data['destination'] = self.old_doc_data['OD'][3:]
    #     self.new_doc_data['origin'] = self.new_doc_data['OD'][:3]
    #     self.new_doc_data['destination'] = self.new_doc_data['OD'][3:]
    #     self.old_doc_data['pos'] = self.old_doc_data['OD'][:3]
    #     self.new_doc_data['pos'] = self.new_doc_data['OD'][:3]
    #
    #     print 'Built Triggering Event'
    #     print "self.old_doc", self.old_doc_data
    #     print 'Class Called'
    #     DataChange.__init__(self, name, old_doc_data, new_doc_data)
    #
    # """
    # This Class represents in analysing the trigger before sending it to the priority queue
    # """
    #
    # @measure(JUPITER_LOGGER)
    # def build_triggering_data(self):
    #     """
    #     :return:
    #     """
    #     self.triggering_data = dict(dep_date_start = self.new_doc_data['dep_date_from'],
    #                                 dep_date_end = self.new_doc_data['dep_date_to'])
    #
    #     print 'Built Triggering Data'
    #
    # @measure(JUPITER_LOGGER)
    # def check_trigger(self):
    #     return True

class PromoNewPromotionTrigger(DataChange):

    @measure(JUPITER_LOGGER)
    def __init__(self, name, old_doc_data, new_doc_data, changed_field):
        print "entered class "
        old_doc_data['origin'] = old_doc_data['OD'][:3]
        old_doc_data['destination'] = old_doc_data['OD'][3:]
        new_doc_data['origin'] = new_doc_data['OD'][:3]
        new_doc_data['destination'] = new_doc_data['OD'][3:]
        old_doc_data['pos'] = old_doc_data['OD'][:3]
        new_doc_data['pos'] = new_doc_data['OD'][:3]

        self.old_doc_data = deepcopy(old_doc_data)
        self.new_doc_data = deepcopy(new_doc_data)
        self.changed_field = deepcopy(changed_field)

        self.triggering_event = dict(
            collection='JUP_DB_Promotions',
            field=changed_field,
            action='change'
        )
        # self.old_doc_data['origin'] = self.old_doc_data['OD'][:3]
        # self.old_doc_data['destination'] = self.old_doc_data['OD'][3:]
        # self.new_doc_data['origin'] = self.new_doc_data['OD'][:3]
        # self.new_doc_data['destination'] = self.new_doc_data['OD'][3:]
        # self.old_doc_data['pos'] = self.old_doc_data['OD'][:3]
        # self.new_doc_data['pos'] = self.new_doc_data['OD'][:3]

        print 'Built Triggering Event'

        print 'Class Called'
        DataChange.__init__(self, name, old_doc_data, new_doc_data)

    """
    This Class represents in analysing the trigger before sending it to the priority queue
    """

    @measure(JUPITER_LOGGER)
    def build_triggering_data(self):
        print "old_doc: ", self.old_doc_data
        print "new_doc: ", self.new_doc_data
        try:

            self.triggering_data = dict(dep_date_start = self.new_doc_data['dep_date_from'],
                                        dep_date_end = self.new_doc_data['dep_date_to'])
            if dict(dep_date_start = self.new_doc_data[''],
                                        dep_date_end = self.new_doc_data['']):
                dep_date_end_object = today + datetime.timedelta(days=90)
                dep_date_end = datetime.datetime.strftime(dep_date_end_object, '%Y-%m-%d')

                self.triggering_data = dict(dep_date_start=SYSTEM_DATE,
                                            dep_date_end=dep_date_end)



        except KeyError:
            dep_date_end_object = today + datetime.timedelta(days=90)
            dep_date_end = datetime.datetime.strftime(dep_date_end_object, '%Y-%m-%d')

            self.triggering_data = dict(dep_date_start= SYSTEM_DATE,
                                        dep_date_end= dep_date_end)

        print 'Built Triggering Data'

    @measure(JUPITER_LOGGER)
    def check_trigger(self):
        self.change = '---'
        return True


if __name__ == '__main__':
    old_doc_data= dict(
        origin='VIE',
        destination='DXB',
        compartment='Y',
        airline='EK'
    )
    new_doc_data= dict(
        origin='VIE',
        destination='DXB',
        compartment='Y',
        airline='EK'
    )

    old_doc_data= {
        "End_Date": "2017-09-30",
        "Url": "https://www.emirates.com/at/english/book/featured-fares/",
        "Valid from": "",
        "Maximum Stay": "",
        "Minimum Stay": "",
        "Airline": "EK",
        "Compartment": "Y",
        "Last Updated Date": "2017-07-19",
        "Fare": 499,
        "OD": "VIEDXB",
        "Currency": "EUR",
        "Valid till": "2017-07-28T19:26:36.718+0000",
        "Start_Date": "2017-07-09",
        "Last Updated Time": "19"
    }

    new_doc_data= [
        {
            "End_Date": "2017-09-30",
            "Url": "https://www.emirates.com/at/english/book/featured-fares/",
            "Valid from": "",
            "Maximum Stay": "",
            "Minimum Stay": "",
            "Airline": "EK",
            "Compartment": "Y",
            "Last Updated Date": "2017-07-19",
            "Fare": 799,
            "OD": "VIEDXB",
            "Currency": "EUR",
            "Valid till": "2017-07-28T19:26:36.718+0000",
            "Start_Date": "2017-07-09",
            "Last Updated Time": "19",
        },
        {
            "Airline": "ME",
            "OD": "DOHBEY",
            "Valid from": "2017-05-15",
            "Valid till": "2017-06-14",
            "Compartment": "Y",
            "Fare": 1795,
            "Currency": "QAR",
            "Minimum Stay": " 2 Days",
            "Maximum Stay": " 3 Months",
            "Start Date": None,
            "End Date": "2017-09-15",
            "Url": "https://www.mea.com.lb/english/plan-and-book/offers-details?offer=4972",
            "Last Updated Date": "2017-06-05",
            "Last Updated Time": "17"

        }
    ]
    obj = PromoTrigger('promotions', old_doc_data, new_doc_data)
    obj.do_analysis()