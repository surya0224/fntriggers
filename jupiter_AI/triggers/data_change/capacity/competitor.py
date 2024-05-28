"""
Author - Sai Krishna K
Date - 2017.4.9
DESC -
Competitor Capacity Change
    Competitor Capacity Data is obtained from OAG.
    The Data is obtained at Leg Level.
    This leg level data is converted into OD level data.

    It is stored in dB after conversion from leg to OD level at the following level.
        airline
        Origin
        Destination
        Compartment
        month(Departure)
        year(Departure)

        capacity
    The above data is updated by a batch program

old_doc = {
	airline,
	origin,
	destination,
	compartment,
	month,
	year,
	capacity
}
new_doc = {
	airline,
	origin,
	destination,
	compartment,
	month,
	year,
	capacity
}
name = 'competitor_capacity_change'

"""
from jupiter_AI.triggers.data_change.MainClass import DataChange
from jupiter_AI.triggers.common import get_start_end_dates
from copy import deepcopy
import jupiter_AI.common.ClassErrorObject as error_class
from jupiter_AI import JUPITER_LOGGER
from jupiter_AI.logutils import measure
import inspect
import datetime


@measure(JUPITER_LOGGER)
def get_arg_lists(frame):
    """
    function used to get the list of arguments of the function
    where it is called
    """
    args, _, _, values = inspect.getargvalues(frame)
    argument_name_list = []
    argument_value_list = []
    for k in args:
        argument_name_list.append(k)
        argument_value_list.append(values[k])
    return argument_name_list, argument_value_list


class CompCapacityChange(DataChange):
    """
    """
    @measure(JUPITER_LOGGER)
    def __init__(self, name, old_database_doc, new_database_doc):
        DataChange.__init__(self, name, old_database_doc, new_database_doc)
        self.triggering_event = dict(
            collection='JUP_DB_OD_Capacity',
            field='capacity',
            action='change'
        )
        print 'Triggering_Event', self.triggering_event

    @measure(JUPITER_LOGGER)
    def build_triggering_data(self):
        """
        :return:
        """
        dep_date_start, dep_date_end = get_start_end_dates(month=self.old_doc_data['month'],
                                                           year=self.old_doc_data['year'])
        self.triggering_data = dict(dep_date_start=dep_date_start,
                                    dep_date_end=dep_date_end)
        print 'Triggering_Data', self.triggering_data

    @measure(JUPITER_LOGGER)
    def check_trigger(self):
        if type(self.old_doc_data['capacity']) in [int, float]:
            if type(self.new_doc_data['capacity']) in [int, float]:
                print self.threshold_type
                if self.threshold_type == 'absolute':
                    self.change = (self.new_doc_data['capacity'] - self.old_doc_data['capacity'])
                elif self.threshold_type == 'percent':
                    self.change = (self.new_doc_data['capacity'] - self.old_doc_data['capacity']) * 100 / self.old_doc_data['capacity']
                print 'change', self.change
                if self.change < self.lower_threshold or self.change > self.upper_threshold:
                    return True
                else:
                    return False
            else:
                module_name = ''.join(['jupiter_AI/triggers/data_change/capacity/competitor.py ',
                                       'method: CompCapacityChange.check_trigger'])
                no_lst_yr_bookings_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                                   module_name,
                                                                   get_arg_lists(inspect.currentframe()))
                no_lst_yr_bookings_error_desc = ''.join(['Invalid value of Capacity in old doc',
                                                         str(self.old_doc_data['capacity'])])
                no_lst_yr_bookings_error.append_to_error_list(no_lst_yr_bookings_error_desc)
                no_lst_yr_bookings_error.write_error_logs(datetime.datetime.now())
                return False
        else:
            module_name = ''.join(['jupiter_AI/triggers/data_change/capacity/competitor.py ',
                                   'method: CompCapacityChange.check_trigger'])
            no_lst_yr_bookings_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                               module_name,
                                                               get_arg_lists(inspect.currentframe()))
            no_lst_yr_bookings_error_desc = ''.join(['Invalid value of Rating either in old or new doc',
                                                     str(self.new_doc_data['capacity'])])
            no_lst_yr_bookings_error.append_to_error_list(no_lst_yr_bookings_error_desc)
            no_lst_yr_bookings_error.write_error_logs(datetime.datetime.now())
            return False


class CompEntry(DataChange):
    """
    """
    @measure(JUPITER_LOGGER)
    def __init__(self, name, old_database_doc, new_database_doc):
        old_doc = deepcopy(new_database_doc)
        old_doc['capacity'] = None
        DataChange.__init__(self, name, old_doc, new_database_doc)
        self.triggering_event = dict(
            collection='JUP_DB_OD_Capacity',
            field='capacity',
            action='insert'
        )

    @measure(JUPITER_LOGGER)
    def build_triggering_data(self):
        """
        :return:
        """
        dep_date_start, dep_date_end = get_start_end_dates(month=self.old_doc_data['month'],
                                                           year=self.old_doc_data['year'])
        self.triggering_data = dict(dep_date_start=dep_date_start,
                                    dep_date_end=dep_date_end,
                                    reason='Capacity Added')

    @measure(JUPITER_LOGGER)
    def check_trigger(self):
        return True


class CompExit(DataChange):
    """
    """
    @measure(JUPITER_LOGGER)
    def __init__(self, name, old_database_doc, new_database_doc):
        new_doc = deepcopy(old_database_doc)
        new_doc['capacity'] = None
        DataChange.__init__(self, name, old_database_doc, new_doc)
        self.triggering_event = dict(
            collection='JUP_DB_OD_Capacity',
            field='capacity',
            action='delete'
        )

    @measure(JUPITER_LOGGER)
    def build_triggering_data(self):
        """
        :return:
        """
        dep_date_start, dep_date_end = get_start_end_dates(month=self.old_doc_data['month'],
                                                           year=self.old_doc_data['year'])
        self.triggering_data = dict(dep_date_start=dep_date_start,
                                    dep_date_end=dep_date_end,
                                    capacity='Capacity Removed')

    @measure(JUPITER_LOGGER)
    def check_trigger(self):
        return True


if __name__ == '__main__':
    name = 'competitor_airline_capacity_percentage_change'
    old_database_doc = dict(
        pos=None,
        origin='DXB',
        destination='DOH',
        compartment='Y',
        airline='EK',
        month=2,
        year=2017,
        capacity=15
    )
    new_database_doc = dict(
        pos=None,
        origin='DXB',
        destination='DOH',
        compartment='Y',
        airline='EK',
        month=2,
        year=2017,
        capacity=30
    )
    obj = CompCapacityChange(name=name,
                             old_database_doc=old_database_doc,
                             new_database_doc=new_database_doc)
    obj.do_analysis()

    name = 'competitor_new_entry'
    old_database_doc = None
    new_database_doc = dict(
        pos=None,
        origin='DXB',
        destination='DOH',
        compartment='Y',
        airline='EK',
        month=2,
        year=2017,
        capacity=30
    )

    obj = CompEntry(name=name,
                    old_database_doc=old_database_doc,
                    new_database_doc=new_database_doc)
    obj.do_analysis()

    name = 'competitor_exit'
    old_database_doc = dict(
        pos=None,
        origin='DXB',
        destination='DOH',
        compartment='Y',
        airline='EK',
        month=2,
        year=2017,
        capacity=30
    )
    new_database_doc = None

    obj = CompExit(name=name, old_database_doc=old_database_doc, new_database_doc=new_database_doc)
    obj.do_analysis()
