"""
Author - Sai Krishna K
Date - 2017-04-08
DESC -
Host Rating Change
Rating is calculated by a python batch program for host and other competitors at an OD level.
A change in rating is identified by comparing two snapshots of data for the following key, while running the
Competitor Ratings batch Program.
For more about this code go to jupiter_AI/batch/JUP_AI_Batch_Competitor_Rating.py

    airline(host)
    origin
    destination

    rating(parameter that is compared)

If the absolute change in the rating is identified to be below or above configured thresholds for the trigger.
The trigger is raised for Analysis.
From this point this Code takes over.
"""
import datetime
import inspect

import jupiter_AI.common.ClassErrorObject as error_class
from jupiter_AI.network_level_params import SYSTEM_DATE, INF_DATE_STR, JUPITER_LOGGER
from jupiter_AI.logutils import measure
from jupiter_AI.triggers.data_change.MainClass import DataChange


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


class HostRatingChange(DataChange):
    """
    This Class represents in analysing the trigger before sending it to the priority queue
    """
    @measure(JUPITER_LOGGER)
    def __init__(self, name, old_database_doc, new_database_doc):
        DataChange.__init__(self, name, old_database_doc, new_database_doc)
        self.triggering_event = dict(
            collection='JUP_DB_Competitor_Ratings',
            field='rating',
            action='change'
        )
        self.old_doc_data['pos'] = None
        self.new_doc_data['pos'] = None
        self.old_doc_data['compartment'] = None
        self.new_doc_data['compartment'] = None

    @measure(JUPITER_LOGGER)
    def build_triggering_data(self):
        """
        :return:
        """
        self.triggering_data = dict(dep_date_start=SYSTEM_DATE,
                                    dep_date_end=INF_DATE_STR)

    @measure(JUPITER_LOGGER)
    def check_trigger(self):
        if type(self.old_doc_data['rating']) in [int, float]:
            if type(self.new_doc_data['rating']) in [int, float]:
                if self.threshold_type == 'absolute':
                    self.change = (self.new_doc_data['rating'] - self.old_doc_data['rating'])
                else:
                    if self.old_doc_data['rating'] != 0:
                        self.change = (self.new_doc_data['rating'] - self.old_doc_data['rating']) / self.old_doc_data['rating']
                print 'change', self.change
                if self.change < self.lower_threshold or self.change > self.upper_threshold:
                    return True
                else:
                    return False
            else:
                module_name = ''.join(['jupiter_AI/triggers/data_change/rating/host.py ',
                                       'method: HostRatingChange.check_trigger'])
                no_lst_yr_bookings_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                                   module_name,
                                                                   get_arg_lists(inspect.currentframe()))
                no_lst_yr_bookings_error_desc = ''.join(['Invalid value of Rating in old doc',
                                                         str(self.old_doc_data['rating'])])
                no_lst_yr_bookings_error.append_to_error_list(no_lst_yr_bookings_error_desc)
                no_lst_yr_bookings_error.write_error_logs(datetime.datetime.now())
                return False
        else:
            module_name = ''.join(['jupiter_AI/triggers/data_change/rating/competitor.py ',
                                   'method: HostRatingChange.check_trigger'])
            no_lst_yr_bookings_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                               module_name,
                                                               get_arg_lists(inspect.currentframe()))
            no_lst_yr_bookings_error_desc = ''.join(['Invalid value of Rating either in old or new doc',
                                                     str(self.new_doc_data['rating'])])
            no_lst_yr_bookings_error.append_to_error_list(no_lst_yr_bookings_error_desc)
            no_lst_yr_bookings_error.write_error_logs(datetime.datetime.now())
            return False


if __name__ == '__main__':
    name = 'host_rating_change'
    old_doc = {
        'airline': 'FZ',
        'rating': 7,
        'origin': 'DXB',
        'destination': 'DOH',
    }
    new_doc = {
        'airline': 'FZ',
        'rating': 8,
        'origin': 'DXB',
        'destination': 'DOH'
    }
    obj = HostRatingChange(name=name,
                           old_database_doc=old_doc,
                           new_database_doc=new_doc)
    obj.do_analysis()
    print obj.__dict__
