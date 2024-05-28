"""
File Name              :   InfareFares.py
Author                 :   Sai Krishna
Date Created           :   2016-02-22
Description            :   Generation of InfareFares Trigger
Long Description for Trigger:
    Let Observation Date - '2017-02-15'
        Parameters in Comparision -
            Check for the months of departure dates

MODIFICATIONS LOG
    S.No                   :    1
    Date Modified          :    2017-02-15
    By                     :    Sai Krishna
    Modification Details   :

"""
import datetime
import inspect
import traceback
from copy import deepcopy

import jupiter_AI.common.ClassErrorObject as error_class
from jupiter_AI.network_level_params import Host_Airline_Code
from jupiter_AI.network_level_params import JUPITER_DB, today, SYSTEM_DATE,JUPITER_LOGGER
from jupiter_AI import client
#db = client[JUPITER_DB]
from jupiter_AI.triggers.GetInfareFare import get_valid_infare_fare
from jupiter_AI.triggers.common import get_start_end_dates
from jupiter_AI.triggers.data_level.MainClass import DataLevel
from jupiter_AI.logutils import measure

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


@measure(JUPITER_LOGGER)
def generate_lowest_fare_comparision_trigger(pos,
                                              origin,
                                              destination,
                                              compartment):
    """
    Main Function called to generate triggers for different sets od departure months
    Arguments:
    :pos: 3 letter str indicating the point of sale
    :origin: 3 letter str indicating the origin
    :destination: 3 letter str indicating the destination
    :compartment: a single letter str indicating the compartment
    """
    try:
        data = {
            'pos': pos,
            'origin': origin,
            'destination': destination,
            'compartment': compartment
        }

        #   Generating the trigger for First Set of departure dates into consideration (Current Month)
        month1 = today.month
        year1 = today.year
        dep_date_start1, dep_date_end1 = get_start_end_dates(month1, year1)
        trigger_obj1 = LowestFares(data, SYSTEM_DATE)
        trigger_obj1.do_analysis(dep_date_start=dep_date_start1,
                                 dep_date_end=dep_date_end1)

        #   Obtaining the next set of departure dates(Current Month + month)
        if month1 != 12:
            month2 = deepcopy(month1 + 1)
            year2 = deepcopy(year1)
        else:
            month2 = 1
            year2 = year1 + 1

        #   Generating the trigger for Second Set of departure dates into consideration (Current Month + 1)
        dep_date_start2, dep_date_end2 = get_start_end_dates(month2, year2)
        trigger_obj2 = LowestFares(data, SYSTEM_DATE)
        trigger_obj2.do_analysis(dep_date_start=dep_date_start2,
                                 dep_date_end=dep_date_end2)

        #   Obtaining the next set of departure dates(Current Month + 2)
        if month2 != 12:
            month3 = deepcopy(month2 + 1)
            year3 = deepcopy(year2)
        else:
            month3 = 1
            year3 = year1 + 1

        #   Generating the trigger for Third Set of departure dates into consideration (Current Month + 2)
        dep_date_start3, dep_date_end3 = get_start_end_dates(month3, year3)
        trigger_obj3 = LowestFares(data, SYSTEM_DATE)
        trigger_obj3.do_analysis(dep_date_start=dep_date_start3,
                                 dep_date_end=dep_date_end3)

        #   Obtaining the next set of departure dates(Current Month + 3)
        if month3 != 12:
            month4 = deepcopy(month2 + 1)
            year4 = deepcopy(year2)
        else:
            month4 = 1
            year4 = year1 + 1

        #   Generating the trigger for Fourth Set of departure dates into consideration (Current Month + 3)
        dep_date_start4, dep_date_end4 = get_start_end_dates(month4, year4)
        trigger_obj4 = LowestFares(data, SYSTEM_DATE)
        trigger_obj4.do_analysis(dep_date_start=dep_date_start4,
                                 dep_date_end=dep_date_end4)
    except Exception as error_msg:
        module_name = ''.join(['jupiter_AI/triggers/data_level/LowestFares.py ',
                               'method: generate_lowest_fare_comparision_trigger'])
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                            module_name,
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())
        traceback.print_exc()
        raise error_msg


class LowestFares(DataLevel):
    """
    """
    @measure(JUPITER_LOGGER)
    def __init__(self, data, system_date):
        """
        """
        super(LowestFares, self).__init__(data, system_date)
        self.old_doc_data = deepcopy(data)
        self.new_doc_data = deepcopy(data)
        self.trigger_date = system_date

    @measure(JUPITER_LOGGER)
    def do_analysis(self, dep_date_start, dep_date_end):
        """
        Main Function of the Class.
        Does the entire job of generating the trigger.
        :param dep_date_start: starting departure date (date str in 'YYYY-MM-DD')
        :param dep_date_end: ending departure date (date str in 'YYYY-MM-DD')
        """

        infare_condition_crsr = db.JUP_DB_Infare_Conditions.find({
            'pos': self.old_doc_data['pos'],
            'origin': self.old_doc_data['origin'],
            'destination': self.old_doc_data['destination'],
            'compartment': self.old_doc_data['compartment']
            # 'pos': 'DXB',
            # 'origin': 'DXB',
            # 'destination': 'DOH',
            # 'compartment': 'Y'
        })
        lst_infare_condition = list(infare_condition_crsr)

        print lst_infare_condition

        if lst_infare_condition:
            for condition in lst_infare_condition:
                del condition['_id']

                self.competitor = condition['airline']

                host_infare_fare = get_valid_infare_fare(airline=Host_Airline_Code,
                                                         pos=self.old_doc_data['pos'],
                                                         origin=self.old_doc_data['origin'],
                                                         destination=self.old_doc_data['destination'],
                                                         dep_date_start=dep_date_start,
                                                         dep_date_end=dep_date_end)
                if host_infare_fare:
                    host_fare = host_infare_fare['price']
                else:
                    host_fare = None

                print 'host_infare_fare ',host_infare_fare

                comp_infare_fare = get_valid_infare_fare(airline=condition['airline'],
                                                         pos=condition['pos'],
                                                         origin=condition['origin'],
                                                         destination=condition['destination'],
                                                         dep_date_start=dep_date_start,
                                                         dep_date_end=dep_date_end
                                                         )

                if comp_infare_fare:
                    comp_fare = comp_infare_fare['price']
                else:
                    comp_fare = None

                print 'comp_infare_fare', comp_infare_fare

                #   Getting the booking_changes_rolling trigger parameters from JUP_DB_Trigger_Types collection in dB
                self.get_trigger_details(trigger_name='lowest_fare_comparision')

                self.lower_threshold = condition['lower_threshold_percent']
                self.upper_threshold = condition['upper_threshold_percent']

                self.triggering_data = {
                    'dep_date_start': dep_date_start,
                    'dep_date_end': dep_date_end,
                    'condition': condition
                }
                self.triggering_data['condition']['lower_threshold'] = self.lower_threshold
                self.triggering_data['condition']['upper_threshold'] = self.upper_threshold
                #   Check whether the trigger is fired or not
                trigger_occurence = self.check_trigger(host_fare=host_fare,
                                                       comp_fare=comp_fare)
                print 'occurence', trigger_occurence

                #   Generate the trigger considering the fares applicable from dep from and dep to
                self.generate_trigger_new(trigger_occurence,
                                          dep_date_start=dep_date_start,
                                          dep_date_end=dep_date_end)

    @measure(JUPITER_LOGGER)
    def check_trigger(self, host_fare, comp_fare):
        """
        Change Parameter is calculated and checked with Thresholds
        Arguments:
        :param bookings_data_tw: dict with ty and ly booking values as key value pairs
        :param bookings_data_lw: dict with ty and ly booking values as key value pairs
        :returns a bool True if trigger is raised
                        False if trigger is not raised
        """
        if host_fare and comp_fare:
            self.old_doc_data['host_fare'] = host_fare
            self.old_doc_data['comp_fare'] = comp_fare

            self.new_doc_data['host_fare'] = host_fare
            self.new_doc_data['comp_fare'] = comp_fare

            if self.new_doc_data['comp_fare'] != 0:
                self.change = (self.old_doc_data['host_fare'] - self.old_doc_data['host_fare'])*100 / float(self.old_doc_data['host_fare'])
                print 'change', self.change
                if self.lower_threshold < self.change < self.upper_threshold :
                    return False
                else:
                    return True
            else:
                module_name = ''.join(['jupiter_AI/triggers/data_level/LowestFares.py ',
                                       'method: check_trigger'])
                no_comp_fare_val = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                                   module_name,
                                                                   get_arg_lists(inspect.currentframe()))
                no_comp_fare_val_desc = ''.join(['No lowest most frequent fare obtained from INFARE for',
                                                 str(self.competitor)])
                no_comp_fare_val.append_to_error_list(no_comp_fare_val_desc)
                no_comp_fare_val.write_error_logs(datetime.datetime.now())
                return False

if __name__ == '__main__':
    data = {
        'pos': 'DXB',
        'origin': 'DXB',
        'destination': 'DOH',
        'compartment': 'Y'
    }
    generate_lowest_fare_comparision_trigger(pos=data['pos'],
                                             origin=data['origin'],
                                             destination=data['destination'],
                                             compartment=data['compartment'])

    data = {
        'pos': 'DXB',
        'origin': 'DXB',
        'destination': 'KTM',
        'compartment': 'Y'
    }
    generate_lowest_fare_comparision_trigger(pos=data['pos'],
                                             origin=data['origin'],
                                             destination=data['destination'],
                                             compartment=data['compartment'])

    data = {
        'pos': 'DOH',
        'origin': 'DOH',
        'destination': 'BOM',
        'compartment': 'Y'
    }
    generate_lowest_fare_comparision_trigger(pos=data['pos'],
                                             origin=data['origin'],
                                             destination=data['destination'],
                                             compartment=data['compartment'])