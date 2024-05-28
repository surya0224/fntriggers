"""
File Name              :   PaxChangesVTGT.py
Author                 :   Sai Krishna
Date Created           :   2016-02-07
Description            :   Generation of a PaxChangesVTGT trigger for a market
                           Market is defined as a combination of pos, origin, destination, compartment
Long Description for Trigger:
    Let Observation Date - '2017-02-15'
        Parameters in Comparision -
            For the next 4 months of departure current month included.
            The following parameter are calculated.
                1.  Pax for the month of departure attained till date.
                2.  Target Pax for the month of departure.
                3.  Last Year Pax till Observation date for the departure month in consideration.
                4.  Total Actual Flown Pax for the departure month obtained in last year.
            From the above 4 parameters the following are obtained
                percent of target achieved till Obs Date this year
                percent of target achieved till Obs Date last year
            The percentage change on the above 2 parameters is obtained and compared with thresholds.

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
from jupiter_AI.BaseParametersCodes.common import get_ly_val
import jupiter_AI.common.ClassErrorObject as error_class
from jupiter_AI.network_level_params import today, SYSTEM_DATE, JUPITER_LOGGER
from jupiter_AI.triggers.common import get_start_end_dates, get_trigger_config_dates
from jupiter_AI.triggers.data_level.MainClass import DataLevel
from jupiter_AI import mongo_client, JUPITER_DB, Host_Airline_Hub
from jupiter_AI.logutils import measure
#db = client[JUPITER_DB]
PXVT_TRIGGERS = []


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
def generate_pax_changes_vtgt_triggers(
        pos,
        origin,
        destination,
        compartment,
        db,
        data_list,
        sig_flag=None):
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

        if sig_flag:
            dates_list = get_trigger_config_dates(db=db, sig_flag=sig_flag)
            if len(dates_list) > 0:
                for date in dates_list:
                    trigger_obj = PaxChangesVTGT(data, SYSTEM_DATE)
                    trigger_obj.do_analysis(dep_date_start=date['start'],
                                            dep_date_end=date['end'],
                                            db=db,
                                            data_list=data_list)
        else:
            # Generating the trigger for First Set of departure dates into
            # consideration (Current Month)
            month1 = today.month
            year1 = today.year
            dep_date_start1, dep_date_end1 = get_start_end_dates(month1, year1)
            trigger_obj1 = PaxChangesVTGT(data, SYSTEM_DATE)
            trigger_obj1.do_analysis(dep_date_start=SYSTEM_DATE,
                                     dep_date_end=dep_date_end1,
                                     db=db,
                                     data_list=data_list)

            # Obtaining the next set of departure dates(Current Month + month)
            if month1 != 12:
                month2 = deepcopy(month1 + 1)
                year2 = deepcopy(year1)
            else:
                month2 = 1
                year2 = year1 + 1
            # Generating the trigger for Second Set of departure dates into
            # consideration (Current Month + 1)
            dep_date_start2, dep_date_end2 = get_start_end_dates(month2, year2)
            trigger_obj2 = PaxChangesVTGT(data, SYSTEM_DATE)
            trigger_obj2.do_analysis(dep_date_start=dep_date_start2,
                                     dep_date_end=dep_date_end2,
                                     db=db,
                                     data_list=data_list)

            #   Obtaining the next set of departure dates(Current Month + 2)
            if month2 != 12:
                month3 = deepcopy(month2 + 1)
                year3 = deepcopy(year2)
            else:
                month3 = 1
                year3 = year1 + 1
            # Generating the trigger for Third Set of departure dates into
            # consideration (Current Month + 2)
            dep_date_start3, dep_date_end3 = get_start_end_dates(month3, year3)
            trigger_obj3 = PaxChangesVTGT(data, SYSTEM_DATE)
            trigger_obj3.do_analysis(dep_date_start=dep_date_start3,
                                     dep_date_end=dep_date_end3,
                                     db=db,
                                     data_list=data_list)

            #   Obtaining the next set of departure dates(Current Month + 3)
            if month3 != 12:
                month4 = deepcopy(month3 + 1)
                year4 = deepcopy(year3)
            else:
                month4 = 1
                year4 = year1 + 1
            # Generating the trigger for Fourth Set of departure dates into
            # consideration (Current Month + 3)
            dep_date_start4, dep_date_end4 = get_start_end_dates(month4, year4)
            trigger_obj4 = PaxChangesVTGT(data, SYSTEM_DATE)
            trigger_obj4.do_analysis(dep_date_start=dep_date_start4,
                                     dep_date_end=dep_date_end4,
                                     db=db,
                                     data_list=data_list)
    except Exception as error_msg:
        # print traceback.print_exc()
        obj_error = error_class.ErrorObject(
            error_class.ErrorObject.ERRORLEVEL1,
            'jupter_AI/triggers/data_level/PaxChangesVTGT.py method: generate_pax_changes_vtgt_triggers',
            get_arg_lists(
                inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())
    return PXVT_TRIGGERS


class PaxChangesVTGT(DataLevel):
    """
    Class Object that defines Pax Changes VTGT Trigger
    """

    @measure(JUPITER_LOGGER)
    def __init__(self, data, system_date):
        """
        Instantiating Function for the class
        Arguments:
            :data: dict with the following keys pos, origin, destination, compartment
            :system date: date of observation or consideration
        """
        super(PaxChangesVTGT, self).__init__(data, system_date)
        self.old_doc_data = deepcopy(data)
        self.new_doc_data = deepcopy(data)
        self.trigger_date = system_date

    @measure(JUPITER_LOGGER)
    def do_analysis(self, dep_date_start, dep_date_end, data_list, db, is_event_trigger=False):
        """
        Main Function of the Class.
        Does the entire job of generating the trigger.
        :param book_date_start:starting book date (date str in 'YYYY-MM-DD')
        :param book_date_end:ending book date (date str in 'YYYY-MM-DD')
        :param dep_date_start:starting departure date (date str in 'YYYY-MM-DD')
        :param dep_date_end:ending departure date (date str in 'YYYY-MM-DD')
        """
        #   Getting Forecast Pax data
        forecast_pax_val = self.get_forecast_values(
            dep_date_start=dep_date_start,
            dep_date_end=dep_date_end,
            data_list=data_list,
            parameter='pax',
            db=db
        )
        # print 'pax'
        # print pax_data.head()

        #   Getting the Target pax for the set of departure dates current year
        flown_pax_target_val = self.get_target_data(
            dep_date_start=dep_date_start,
            dep_date_end=dep_date_end,
            data_list=data_list,
            parameter='pax',
            db=db)
        # print 'flown_pax_target'
        # print flown_pax_target_val.head()

        self.get_trigger_details(trigger_name='pax_changes_VTGT', db=db)
        forecast_pax_val.sort_values(by='market', inplace=True)
        flown_pax_target_val.sort_values(by='market', inplace=True)
        pax_data = forecast_pax_val.merge(flown_pax_target_val, on='market', how='left')
        pax_data.dropna(inplace=True)
        # print pax_data.to_string()
        #   Check whether the trigger is fired or not
        for tup1 in pax_data.iterrows():
            try:
                self.old_doc_data['pos'] = tup1[1]['market'][0:3]
                self.old_doc_data['origin'] = tup1[1]['market'][3:6]
                self.old_doc_data['destination'] = tup1[1]['market'][6:9]
                self.old_doc_data['compartment'] = tup1[1]['market'][9:]
                self.new_doc_data = deepcopy(self.old_doc_data)
                # self.new_doc_data['forecast_pax'] = tup1[1]['forecast_pax']
                # self.old_doc_data['target_pax'] = tup1[1]['target_pax']
                trigger_occurence = self.check_trigger(
                    forecast_pax=tup1[1]['forecast_pax'],
                    target_pax=tup1[1]['target_pax'])
                # print 'occurence', trigger_occurence

                self.triggering_data = {
                    'dep_date_start': dep_date_start,
                    'dep_date_end': dep_date_end,
                    'forecast': tup1[1]['forecast_pax'],
                    'target': tup1[1]['target_pax'],
                }
                # print self.triggering_data
                # Generate the trigger considering the fares applicable from dep
                # from and dep to
                id = self.generate_trigger_new(trigger_occurence,
                                               dep_date_start=dep_date_start,
                                               dep_date_end=dep_date_end,
                                               db=db,
                                               is_event_trigger=is_event_trigger)
                PXVT_TRIGGERS.append(id)
            except Exception as e:
                db.JUP_DB_Errors.insert({"err_id": "PXVT",
                                         "error_name": str(e.__class__.__name__),
                                         "error_message": str(e.args[0]),
                                         "pos": tup1[1]['market'][0:3],
                                         "origin": tup1[1]['market'][3:6],
                                         "destination": tup1[1]['market'][6:9],
                                         "compartment": tup1[1]['market'][9:],
                                         "dep_date_start": dep_date_start,
                                         "dep_date_end": dep_date_end})

    # Old logic used for VTGT trigger which compares percentage target achieved this year
    # till date versus percentage target achieved last year.
    # This can be used to create a new trigger in future.
    @measure(JUPITER_LOGGER)
    def do_analysis_old(self, dep_date_start, dep_date_end, data_list, db, is_event_trigger=False):
        """
        Main Function of the Class.
        Does the entire job of generating the trigger.
        :param book_date_start:starting book date (date str in 'YYYY-MM-DD')
        :param book_date_end:ending book date (date str in 'YYYY-MM-DD')
        :param dep_date_start:starting departure date (date str in 'YYYY-MM-DD')
        :param dep_date_end:ending departure date (date str in 'YYYY-MM-DD')
        """
        #   Getting Pax data till today for this year and last year
        book_date_start = str(today.year) + '-01-01'
        book_date_end = SYSTEM_DATE
        dep_date_start_ly = get_ly_val(dep_date_start)
        dep_date_end_ly = get_ly_val(dep_date_end)
        book_date_start_ly = get_ly_val(book_date_start)
        book_date_end_ly = get_ly_val(book_date_end)

        pax_data = self.get_sales_flown_data(book_date_start=book_date_start,
                                             book_date_end=book_date_end,
                                             dep_date_start=dep_date_start,
                                             dep_date_end=dep_date_end,
                                             parameter='pax',
                                             db=db,
                                             data_list=data_list)
        # print 'pax'
        # print pax_data.head()

        #   Getting the Target pax for the set of departure dates current year
        flown_pax_target_val = self.get_target_data(
            dep_date_start=dep_date_start,
            dep_date_end=dep_date_end,
            data_list=data_list,
            db=db,
            parameter='pax')
        # print 'flown_pax_target'
        # print flown_pax_target_val.head()

        #   Getting Actual Flown pax for the departure period last year
        flown_pax_ly_val = self.get_sales_flown_data(
            book_date_start=book_date_start_ly,
            book_date_end=book_date_end,
            dep_date_start=dep_date_start_ly,
            dep_date_end=dep_date_end_ly,
            db=db,
            parameter='pax',
            data_list=data_list)
        # print 'flown pax last year val'
        # print flown_pax_ly_val.head()

        self.get_trigger_details(trigger_name='pax_changes_VTGT', db=db)
        pax_data.sort_values(by='market', inplace=True)
        flown_pax_target_val.sort_values(by='market', inplace=True)
        flown_pax_ly_val.sort_values(by='market', inplace=True)
        pax_data = pax_data.merge(flown_pax_target_val, on='market', how='left')
        flown_pax_ly_val = flown_pax_ly_val.rename(columns={"ty": "flown_pax_ty", "ly":"flown_pax_ly"})
        pax_data = pax_data.merge(flown_pax_ly_val, on='market', how='left')
        pax_data.dropna(inplace=True)
        #   Check whether the trigger is fired or not
        for tup1 in pax_data.iterrows():
            try:
                self.old_doc_data['pos'] = tup1[1]['market'][0:3]
                self.old_doc_data['origin'] = tup1[1]['market'][3:6]
                self.old_doc_data['destination'] = tup1[1]['market'][6:9]
                self.old_doc_data['compartment'] = tup1[1]['market'][9:]
                self.new_doc_data = deepcopy(self.old_doc_data)
                trigger_occurence = self.check_trigger(
                    pax_ly=tup1[1]['ly'],
                    pax_ty=tup1[1]['ty'],
                    flown_pax_ly=tup1[1]['flown_pax_ty'],
                    flown_pax_target_ty=tup1[1]['target_pax'])
                # print 'occurence', trigger_occurence

                self.triggering_data = {
                    'dep_date_start': dep_date_start,
                    'dep_date_end': dep_date_end,
                    'ty': {
                        'dep_date_start': dep_date_start,
                        'dep_date_end': dep_date_end,
                        'book_date_start': book_date_start,
                        'book_date_end': book_date_end,
                        'pax': tup1[1]['ty'],
                        'target': tup1[1]['target_pax']
                    },
                    'ly': {
                        'dep_date_start': dep_date_start_ly,
                        'dep_date_end': dep_date_end_ly,
                        'book_date_start': book_date_start_ly,
                        'book_date_end': book_date_end_ly,
                        'pax': tup1[1]['ly'],
                        'target': tup1[1]['target_pax']
                    }
                }
                # print self.triggering_data
                # Generate the trigger considering the fares applicable from dep
                # from and dep to
                id = self.generate_trigger_new(trigger_occurence,
                                               dep_date_start=dep_date_start,
                                               dep_date_end=dep_date_end,
                                               db=db,
                                               is_event_trigger=is_event_trigger)
                PXVT_TRIGGERS.append(id)
            except Exception as e:
                db.JUP_DB_Errors.insert({"err_id": "PXVT",
                                         "error_name": str(e.__class__.__name__),
                                         "error_message": str(e.args[0]),
                                         "pos": tup1[1]['market'][0:3],
                                         "origin": tup1[1]['market'][3:6],
                                         "destination": tup1[1]['market'][6:9],
                                         "compartment": tup1[1]['market'][9:],
                                         "dep_date_start": dep_date_start,
                                         "dep_date_end": dep_date_end})

    @measure(JUPITER_LOGGER)
    def check_trigger(self, forecast_pax, target_pax):
        self.old_doc_data['target_pax'] = target_pax
        self.new_doc_data['forecast_pax'] = forecast_pax
        # print self.old_doc_data['target_pax'], self.new_doc_data['forecast_pax']
        if self.old_doc_data['target_pax'] != 0:

            if self.threshold_type == 'percent':
                self.change = (self.new_doc_data['forecast_pax'] - self.old_doc_data[
                                  'target_pax']) * 100 / self.old_doc_data['target_pax']
            else:
                self.change = (self.new_doc_data['forecast_pax'] -
                               self.old_doc_data['target_pax'])
            # print 'change', self.change
            if self.change < self.lower_threshold or self.change > self.upper_threshold:
                return True
            else:
                return False

        else:
            return False

    @measure(JUPITER_LOGGER)
    def check_trigger_old(self, pax_ly, pax_ty, flown_pax_ly, flown_pax_target_ty):
        self.old_doc_data['pax'] = pax_ly
        self.old_doc_data['target'] = flown_pax_ly
        self.new_doc_data['pax'] = pax_ty
        self.new_doc_data['target'] = flown_pax_target_ty
        # print self.old_doc_data['target'], self.new_doc_data['target']
        if self.old_doc_data['target'] != 0 and \
           self.new_doc_data['target'] != 0 and pax_ly != "NA"\
           and pax_ty != "NA"\
           and flown_pax_ly != "NA"\
           and flown_pax_target_ty != "NA":
            self.old_doc_data['percent_acheived'] = self.old_doc_data['pax'] / \
                self.old_doc_data['target']
            self.new_doc_data['percent_acheived'] = self.new_doc_data['pax'] / \
                self.new_doc_data['target']

            if self.old_doc_data['percent_acheived'] != 0:
                if self.threshold_type == 'percent':
                    self.change = (
                        self.new_doc_data['percent_acheived'] - self.old_doc_data['percent_acheived']) * 100 / self.old_doc_data['percent_acheived']
                else:
                    self.change = (
                        self.new_doc_data['percent_acheived'] -
                        self.old_doc_data['percent_acheived'])
                # print 'change', self.change
                if self.change < self.lower_threshold or self.change > self.upper_threshold:
                    return True
                else:
                    return False
            elif self.old_doc_data['percent_acheived'] == 0 and self.new_doc_data['percent_acheived'] != 0 and pax_ly != "NA" and pax_ty != "NA" and flown_pax_ly != "NA" and flown_pax_target_ty != "NA":
                self.change = 'inf'
                return True
            else:
                return False
        else:
            return False


@measure(JUPITER_LOGGER)
def main_helper(db):
    data = {
        'pos': Host_Airline_Hub,
        'origin': Host_Airline_Hub,
        'destination': 'DOH',
        'compartment': 'Y'
    }
    import time
    import pandas as pd
    from jupiter_AI.triggers.listener_data_level_trigger import analyze
    st = time.time()
    print "Running Data Level Triggers"
    online_mrkts = db.JUP_DB_Market_Significance.aggregate([
        {"$match": {"online": True}},
        {"$sort": {"rank": 1}},
        {"$project": {"_id": 0, "market": "$market", "pax": "$pax", "revenue": "$revenue"}}
    ],
        allowDiskUse=True)

    online_mrkts_df = pd.DataFrame(list(online_mrkts))
    online_mrkts_df['pos'] = online_mrkts_df['market'].str.slice(0, 3)
    online_mrkts_df['origin'] = online_mrkts_df['market'].str.slice(3, 6)
    online_mrkts_df['destination'] = online_mrkts_df['market'].str.slice(6, 9)
    online_mrkts_df['compartment'] = online_mrkts_df['market'].str.slice(9, 10)
    online_mrkts_df['od'] = online_mrkts_df['origin'] + online_mrkts_df['destination']

    reg_mas = list(db.JUP_DB_Region_Master.find({}))
    reg_mas = pd.DataFrame(reg_mas)
    online_mrkts_df = online_mrkts_df.merge(reg_mas[['POS_CD', 'COUNTRY_NAME_TX']], left_on='pos', right_on='POS_CD',
                                            how='left').drop('POS_CD',
                                                             axis=1)

    online_mrkts_df = online_mrkts_df.rename(columns={"COUNTRY_NAME_TX": "POS_Country"})

    online_mrkts_df = online_mrkts_df.merge(reg_mas[['POS_CD', 'COUNTRY_NAME_TX']], left_on='origin', right_on='POS_CD',
                                            how='left').drop(
        'POS_CD', axis=1).rename(columns={"COUNTRY_NAME_TX": "origin_Country"})

    online_mrkts_df = online_mrkts_df.merge(reg_mas[['POS_CD', 'COUNTRY_NAME_TX']], left_on='destination',
                                            right_on='POS_CD', how='left').drop(
        'POS_CD', axis=1).rename(columns={"COUNTRY_NAME_TX": "destination_Country"})

    online_mrkts_df = online_mrkts_df[(online_mrkts_df['POS_Country'] == online_mrkts_df['origin_Country']) |
                                      (online_mrkts_df['POS_Country'] == online_mrkts_df['destination_Country'])]

    print len(online_mrkts_df[online_mrkts_df['origin_Country'] == online_mrkts_df['destination_Country']])
    online_mrkts_df = online_mrkts_df[online_mrkts_df['origin_Country'] != online_mrkts_df['destination_Country']]

    online_mrkts_df = online_mrkts_df[['market', 'pos', 'origin', 'destination', 'od', 'compartment', 'pax', 'revenue']]
    online_mrkts_df.drop_duplicates(subset='market', inplace=True)
    total_markets = len(online_mrkts_df)
    print "Number of markets where POS country == O/D country = ", total_markets

    online_mrkts_df.sort_values(by='revenue', ascending=False, inplace=True)
    online_mrkts_df.reset_index(inplace=True)
    online_mrkts_df['cumm_revenue'] = online_mrkts_df['revenue'].cumsum(axis=0)
    online_mrkts_df['cumm_rev_perc'] = online_mrkts_df['cumm_revenue'] / online_mrkts_df['revenue'].sum() * 100

    sig_rev = list(online_mrkts_df[online_mrkts_df['cumm_rev_perc'] <= 80.0]['market'].values)
    sub_sig_rev = list(online_mrkts_df[(online_mrkts_df['cumm_rev_perc'] > 80.0) &
                                       (online_mrkts_df['cumm_rev_perc'] <= 95.0)]['market'].values)

    online_mrkts_df.sort_values(by='pax', ascending=False, inplace=True)
    online_mrkts_df.reset_index(inplace=True)
    online_mrkts_df['cumm_pax'] = online_mrkts_df['pax'].cumsum(axis=0)
    online_mrkts_df['cumm_pax_perc'] = online_mrkts_df['cumm_pax'] / online_mrkts_df['pax'].sum() * 100

    sig_pax = list(online_mrkts_df[online_mrkts_df['cumm_pax_perc'] <= 80.0]['market'].values)
    sub_sig_pax = list(online_mrkts_df[(online_mrkts_df['cumm_pax_perc'] > 80.0) &
                                       (online_mrkts_df['cumm_pax_perc'] <= 95.0)]['market'].values)

    p2p = list(online_mrkts_df[((online_mrkts_df['origin'] == Host_Airline_Hub) | (online_mrkts_df['destination'] == Host_Airline_Hub) |
                                (online_mrkts_df['origin'] == 'DWC') | (online_mrkts_df['destination'] == 'DWC')) &
                               ((online_mrkts_df['origin'] == online_mrkts_df['pos']) |
                                (online_mrkts_df['destination'] == online_mrkts_df['pos']))]['market'].values)

    sig_markets_df = online_mrkts_df[online_mrkts_df['market'].isin(list(set(sig_pax + sig_rev + p2p)))]

    sub_sig_markets_df = online_mrkts_df[online_mrkts_df['market'].isin(list(set(sub_sig_pax + sub_sig_rev)))]

    non_sig_markets_df = online_mrkts_df[~online_mrkts_df['market'].isin(list(set(sig_markets_df['market'] +
                                                                                  sub_sig_markets_df['market'])))]

    sig_markets_df['sig_flag'] = 'sig'
    sub_sig_markets_df['sig_flag'] = 'sub_sig'
    non_sig_markets_df['sig_flag'] = 'non_sig'

    insert_df = pd.concat([sig_markets_df, sub_sig_markets_df])
    insert_df = insert_df[['market', 'pos', 'od', 'compartment', 'sig_flag']]

    sig_markets = len(sig_markets_df)
    sub_sig_markets = len(sub_sig_markets_df)
    non_sig_markets = len(non_sig_markets_df)

    online_mrkts = list(set(list(sig_markets_df['market'].values) +
                            list(sub_sig_markets_df['market'].values)))
    markets = []
    counter = 0
    for mrkt in ['CMBCMBDXBY']:
        if counter == 2000:
            id_list = generate_pax_changes_vtgt_triggers(data_list=markets,
                                                                pos=data['pos'],
                                                                origin=data['origin'],
                                                                destination=data['destination'],
                                                                compartment=data['compartment'],
                                                                db=db,
                                                                sig_flag='sig')
            markets = list()
            # for trigger in id_list:
            #     analyze(trigger)
            markets.append(mrkt)
            counter = 1
        else:
            markets.append(mrkt)
            counter += 1
    if counter > 0:
        id_list = generate_pax_changes_vtgt_triggers(data_list=markets,
                                                            pos=data['pos'],
                                                            origin=data['origin'],
                                                            destination=data['destination'],
                                                            compartment=data['compartment'],
                                                            db=db,
                                                            sig_flag='sig')
        for id in id_list:
            analyze(id=id, db=db)
    markets = []
    counter = 0
    for mrkt in list(sub_sig_markets_df['market'].values):
        if counter == 2000:
            id_list = generate_pax_changes_vtgt_triggers(data_list=markets,
                                                                pos=data['pos'],
                                                                origin=data['origin'],
                                                                destination=data['destination'],
                                                                compartment=data['compartment'],
                                                                db=db,
                                                                sig_flag='sub_sig')
            markets = list()
            # for trigger in id_list:
            #     analyze(trigger)
            markets.append(mrkt)
            counter = 1
        else:
            markets.append(mrkt)
            counter += 1
    if counter > 0:
        id_list = generate_pax_changes_vtgt_triggers(data_list=markets,
                                                            pos=data['pos'],
                                                            origin=data['origin'],
                                                            destination=data['destination'],
                                                            compartment=data['compartment'],
                                                            db=db,
                                                            sig_flag='sub_sig')
        # for trigger in id_list:
        #     analyze(trigger)
    print 'Total Time Taken', time.time() - st


if __name__ == '__main__':
    client = mongo_client()
    db=client[JUPITER_DB]
    main_helper(db)
    client.close()
