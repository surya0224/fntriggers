"""
File Name              :   ForecastChanges
Author                 :   Sai Krishna
Date Created           :   2017-02-10
Description            :   Contains the class and relevant functions to generate forecast changes trigger.
MODIFICATIONS LOG
    S.No               :
    Date Modified      :
    By                 :
    Modification Details   :
"""
import datetime
import inspect
from copy import deepcopy
import traceback
import jupiter_AI.common.ClassErrorObject as error_class
from jupiter_AI.network_level_params import SYSTEM_DATE, today,JUPITER_LOGGER
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
def generate_forecast_vtgt_triggers(pos, origin, destination, compartment):
    """
    Generate the forecast triggers(if any) for the next three months in the OD provided.
    :param origin:
    :param destination:
    :return:
    """
    try:
        data = {
            'pos': pos,
            'origin': origin,
            'destination': destination,
            'compartment': compartment
        }
        #   considering the default booking period Year to Observation Date

        #   Generating the trigger for First Set of departure dates into consideration (Current Month)
        month1 = today.month
        year1 = today.year
        dep_date_start1, dep_date_end1 = get_start_end_dates(month1, year1)
        trigger_obj1 = ForecastChangesVTGT(data, SYSTEM_DATE)
        trigger_obj1.do_analysis(dep_date_start=dep_date_start1,
                                 dep_date_end=dep_date_end1)

        #   Obtaining the next set of departure dates(Current Month + month)
        # if month1 != 12:
        #     month2 = deepcopy(month1 + 1)
        #     year2 = deepcopy(year1)
        # else:
        #     month2 = 1
        #     year2 = year1 + 1
        # # Generating the trigger for Second Set of departure dates into consideration (Current Month + 1)
        # dep_date_start2, dep_date_end2 = get_start_end_dates(month2, year2)
        # trigger_obj2 = ForecastChangesVTGT(data, SYSTEM_DATE)
        # trigger_obj2.do_analysis(dep_date_start=dep_date_start2,
        #                          dep_date_end=dep_date_end2)
        #
        # #   Obtaining the next set of departure dates(Current Month + 2)
        # if month2 != 12:
        #     month3 = deepcopy(month2 + 1)
        #     year3 = deepcopy(year2)
        # else:
        #     month3 = 1
        #     year3 = year1 + 1
        # # Generating the trigger for Third Set of departure dates into consideration (Current Month + 2)
        # dep_date_start3, dep_date_end3 = get_start_end_dates(month3, year3)
        # trigger_obj3 = ForecastChangesVTGT(data, SYSTEM_DATE)
        # trigger_obj3.do_analysis(dep_date_start=dep_date_start3,
        #                          dep_date_end=dep_date_end3)
        #
        # #   Obtaining the next set of departure dates(Current Month + 3)
        # if month3 != 12:
        #     month4 = deepcopy(month2 + 1)
        #     year4 = deepcopy(year2)
        # else:
        #     month4 = 1
        #     year4 = year1 + 1
        # # Generating the trigger for Fourth Set of departure dates into consideration (Current Month + 3)
        # dep_date_start4, dep_date_end4 = get_start_end_dates(month4, year4)
        # trigger_obj4 = ForecastChangesVTGT(data, SYSTEM_DATE)
        # trigger_obj4.do_analysis(dep_date_start=dep_date_start4,
        #                        dep_date_end=dep_date_end4)
    except Exception as error_msg:
        print traceback.print_exc()
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                            'jupiter_AI/triggers/data_level/ForecastChangesVTGT.py method: generate_forecast_vtgt_triggers',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


class ForecastChangesVTGT(DataLevel):
    """
    Class Object defining all the functions of generating the trigger
    Logic of the Trigger.
        For a period of departure months current month and next 3 months
    """
    @measure(JUPITER_LOGGER)
    def __init__(self, data, system_date):
        """
        :param data: represents the market in consideration
         {
            'pos':'',
            'origin':'',
            'destination':'',
            'compartment':''
         }
        :param system_date:
        """
        super(ForecastChangesVTGT, self).__init__(data, system_date)
        self.old_doc_data = data
        self.new_doc_data = data
        self.trigger_date = system_date
        # self.trigger_type = 'forecast_changes'

    @measure(JUPITER_LOGGER)
    def do_analysis(self, dep_date_start, dep_date_end):
        """

        :param dep_date_start:
        :param dep_date_end:
        :return:
        """
        book_date_start = str(today.year) + '-01-01'
        book_date_end = SYSTEM_DATE

        forecast_pax = self.get_forecast_data(book_date_start=book_date_start,
                                              book_date_end=book_date_end,
                                              dep_date_start=dep_date_start,
                                              dep_date_end=dep_date_end,
                                              parameter='pax')
        # print 'forecast pax', forecast_pax

        target_pax = self.get_target_data(dep_date_start=dep_date_start,
                                          dep_date_end=dep_date_end,
                                          parameter='pax')
        # print 'target_pax', target_pax

        self.get_trigger_details(trigger_name='forecast_changes_VTGT')

        trigger_occurence = self.check_trigger(forecast_pax=forecast_pax,
                                            target_pax=target_pax)
        print 'trigger_status', trigger_occurence
        self.triggering_data = {
            'dep_date_start': dep_date_start,
            'dep_date_end': dep_date_end,
            'forecast': forecast_pax,
            'target': target_pax
        }
        # print self.triggering_data
        self.generate_trigger_new(trigger_status=trigger_occurence,
                                  dep_date_start=dep_date_start,
                                  dep_date_end=dep_date_end)

    @measure(JUPITER_LOGGER)
    def check_trigger(self, forecast_pax, target_pax):
        self.old_doc_data['target_pax'] = target_pax
        self.new_doc_data['forecast_pax'] = forecast_pax
        print ("target", self.old_doc_data['target_pax'])
        print("forecast", self.new_doc_data['forecast_pax'])
        if self.old_doc_data['target_pax'] > 0 and self.old_doc_data['forecast_pax'] > 0:
            if self.threshold_type == "percent":
                self.change = (self.new_doc_data['forecast_pax'] - self.old_doc_data['target_pax'])*100 / self.old_doc_data['target_pax']
            else:
                self.change = (self.new_doc_data['forecast_pax'] - self.old_doc_data['target_pax'])
            print 'change', self.change
            if self.change < self.lower_threshold or self.change > self.upper_threshold:
                return True
            else:
                return False
        else:
            return False


if __name__=='__main__':
    generate_forecast_vtgt_triggers(pos='DXB',
                                    origin='DXB',
                                    destination='KWI',
                                    compartment='Y')