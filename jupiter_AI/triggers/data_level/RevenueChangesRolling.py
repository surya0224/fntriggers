"""
File Name              :   RevenueChangesRolling.py
Author                 :   Sai Krishna
Date Created           :   2016-02-07
Description            :   Generation of a RevenueChangesRolling trigger for a market
                           Market is defined as a combination of pos, origin, destination, compartment
Long Description for Trigger:
    Let Observation Date - '2017-02-15'
        Parameters in Comparision -
            1.  Revenue obtained for departure dates b/w '2017-02-01' to '2017-02-28'(this month) till
                '2017-02-15'(observation date)
            2.  Revenue obtained for departure dates b/w '2017-01-23' to '2017-02-23' till
                '2017-02-15'.
            The percentage change on the above 2 parameters is obtained and compared with thresholds.

MODIFICATIONS LOG
    S.No                   :    1
    Date Modified          :    2017-02-19
    By                     :    Sai Krishna
    Modification Details   :
        Identified get_trigger_details, obtain_fares and generate_trigger functions as common functions
        and refactored them in jupiter_AI/triggers/data_level/common.py
        Relevant imports added

"""
import datetime
import inspect
import traceback
from copy import deepcopy

import jupiter_AI.common.ClassErrorObject as error_class
from jupiter_AI.network_level_params import today, SYSTEM_DATE,JUPITER_LOGGER
from jupiter_AI.triggers.common import get_start_end_dates, get_trigger_config_dates
from jupiter_AI.triggers.data_level.MainClass import DataLevel
from jupiter_AI import mongo_client, JUPITER_DB, Host_Airline_Hub
from jupiter_AI.logutils import measure
#db = client[JUPITER_DB]
RRL_TRIGGERS = []


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
def generate_revenue_changes_rolling_triggers(pos,
                                              origin,
                                              destination,
                                              compartment,
                                              data_list,
                                              db,
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

        #   considering the default revenue period Year to Observation Date
        book_date_start = str(today.year + 1) + '-01-01'
        book_date_end = SYSTEM_DATE

        if sig_flag:
            dates_list = get_trigger_config_dates(db=db, sig_flag=sig_flag)
            print "dates_list"
            print dates_list
            if len(dates_list) > 0:
                for date in dates_list:
                    trigger_obj = RevenueChangesRolling(data, SYSTEM_DATE)
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
            trigger_obj1 = RevenueChangesRolling(data, SYSTEM_DATE)
            trigger_obj1.do_analysis(dep_date_start=SYSTEM_DATE,
                                     dep_date_end=dep_date_end1,
                                     db=db,
                                     data_list=data_list)

            #   Obtaining the next set of departure dates(Current Month + month)
            if month1 != 12:
                month2 = deepcopy(month1 + 1)
                year2 = deepcopy(year1)
            else:
                month2 = 1
                year2 = year1 + 1
            # Generating the trigger for Second Set of departure dates into consideration (Current Month + 1)
            dep_date_start2, dep_date_end2 = get_start_end_dates(month2, year2)
            trigger_obj2 = RevenueChangesRolling(data, SYSTEM_DATE)
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
            # Generating the trigger for Third Set of departure dates into consideration (Current Month + 2)
            dep_date_start3, dep_date_end3 = get_start_end_dates(month3, year3)
            trigger_obj3 = RevenueChangesRolling(data, SYSTEM_DATE)
            trigger_obj3.do_analysis(dep_date_start=dep_date_start3,
                                     dep_date_end=dep_date_end3,
                                     db=db,
                                     data_list=data_list)

            #   Obtaining the next set of departure dates(Current Month + 3)
            if month3 != 12:
                month4 = deepcopy(month2 + 1)
                year4 = deepcopy(year2)
            else:
                month4 = 1
                year4 = year1 + 1
            # Generating the trigger for Fourth Set of departure dates into consideration (Current Month + 3)
            dep_date_start4, dep_date_end4 = get_start_end_dates(month4, year4)
            trigger_obj4 = RevenueChangesRolling(data, SYSTEM_DATE)
            trigger_obj4.do_analysis(dep_date_start=dep_date_start4,
                                     dep_date_end=dep_date_end4,
                                     db=db,
                                     data_list=data_list)

    except Exception as error_msg:
        print("ERROR!!!")
        print traceback.print_exc()
        obj_error = error_class.ErrorObject(
            error_class.ErrorObject.ERRORLEVEL1,
            'jupiter_AI/triggers/data_level/RevenueChangesRolling.py method: generate_revenue_changes_rolling_triggers',
            get_arg_lists(
                inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())
    return RRL_TRIGGERS


class RevenueChangesRolling(DataLevel):
    """
    Class object that defines Revenue Changes Rolling Trigger
    Arguments:
        :data: dict with the following keys pos, origin, destination, compartment
        :system date: date of observation or consideration
    """

    @measure(JUPITER_LOGGER)
    def __init__(self, data, system_date):
        super(RevenueChangesRolling, self).__init__(data, system_date)
        self.old_doc_data = deepcopy(data)
        self.new_doc_data = deepcopy(data)
        self.trigger_date = system_date

    @measure(JUPITER_LOGGER)
    def do_analysis(self, dep_date_start, dep_date_end, data_list, db, is_event_trigger=False):
        """
        Main Function of the Class.
        Does the entire job of generating the trigger.
        :param dep_date_start: starting departure date (date str in 'YYYY-MM-DD')
        :param dep_date_end: ending departure date (date str in 'YYYY-MM-DD')
        """
        #   Obtaining revenue period consideration till Obs date
        book_end_date_obj_tw = datetime.datetime.strptime(
            self.trigger_date,
            '%Y-%m-%d'
        )
        book_end_date_tw = self.trigger_date
        book_start_date_tw = datetime.datetime.strftime(
            book_end_date_obj_tw + datetime.timedelta(days=-7), '%Y-%m-%d')

        #   Getting Revenue data till today
        revenue_data_tw = self.get_sales_flown_data(
            book_date_start=book_start_date_tw,
            book_date_end=book_end_date_tw,
            dep_date_start=dep_date_start,
            dep_date_end=dep_date_end,
            db=db,
            parameter='revenue',
            data_list=data_list)
        # print 'revenue_tw'
        revenue_data_tw.sort_values(by='market', inplace=True)
        # print revenue_data_tw.head()

        #   Triggering data represents departure date ranges and revenue date ranges under consideration
        #   Generating the triggering data for this week
        self.triggering_data = {
            'dep_date_start': dep_date_start,
            'dep_date_end': dep_date_end,
            'book_date_start': book_start_date_tw,
            'book_date_end': book_end_date_tw
        }

        #   Obtaining relevant dates for comparision
        #   revenue start date lw = year start date
        #   revenue end date lw = revenue end date this week(obs date) - 7 days
        #   Departure start date lw = Departure start date this week - 7 days
        #   Departure end date lw = Departure end date this week - 7 days
        dep_date_start_lw_obj = datetime.datetime.strptime(
            dep_date_start, '%Y-%m-%d') + datetime.timedelta(days=-7)
        dep_date_start_lw = datetime.datetime.strftime(
            dep_date_start_lw_obj, '%Y-%m-%d')
        dep_date_end_lw_obj = datetime.datetime.strptime(
            dep_date_end, '%Y-%m-%d') + datetime.timedelta(days=-7)
        dep_date_end_lw = datetime.datetime.strftime(
            dep_date_end_lw_obj, '%Y-%m-%d')
        book_end_date_obj_lw = book_end_date_obj_tw + \
            datetime.timedelta(days=-7)
        book_end_date_lw = datetime.datetime.strftime(
            book_end_date_obj_lw, '%Y-%m-%d')
        book_start_date_lw = datetime.datetime.strftime(
            book_end_date_obj_lw + datetime.timedelta(days=-7), '%Y-%m-%d')

        #   Obtaining revenue data for last weeks parameters
        revenue_data_lw = self.get_sales_flown_data(
            book_date_start=book_start_date_lw,
            book_date_end=book_end_date_lw,
            dep_date_start=dep_date_start_lw,
            dep_date_end=dep_date_end_lw,
            db=db,
            parameter='revenue',
            data_list=data_list)
        # print 'revenue_lw'
        revenue_data_lw.sort_values(by='market', inplace=True)
        # print revenue_data_lw.head()

        #   Generating triggering data for this last weeks params
        self.triggering_data.update({
            'dep_date_start_lw': dep_date_start_lw,
            'dep_date_end_lw': dep_date_end_lw,
            'book_date_start_lw': book_start_date_lw,
            'book_date_end_lw': book_end_date_lw
        }
        )

        # Getting the revenue_changes_rolling trigger parameters from
        # JUP_DB_Trigger_Types collection in dB
        self.get_trigger_details(trigger_name='revenue_changes_rolling', db=db)

        #   Check whether the trigger is fired or not
        for tup1, tup2 in zip(revenue_data_tw.sort_values(by='market').iterrows(),
                              revenue_data_lw.sort_values(by='market').iterrows()):
            try:
                self.old_doc_data['pos'] = tup1[1]['market'][0:3]
                self.old_doc_data['origin'] = tup1[1]['market'][3:6]
                self.old_doc_data['destination'] = tup1[1]['market'][6:9]
                self.old_doc_data['compartment'] = tup1[1]['market'][9:]
                self.new_doc_data = deepcopy(self.old_doc_data)
                trigger_occurence = self.check_trigger(revenue_data_tw=tup1[1],
                                                       revenue_data_lw=tup2[1])
                # print 'occurence', trigger_occurence

                # Generate the trigger considering the fares applicable from dep
                # from and dep to
                id = self.generate_trigger_new(trigger_occurence,
                                               dep_date_start=dep_date_start,
                                               dep_date_end=dep_date_end,
                                               db=db,
                                               is_event_trigger=is_event_trigger)
                RRL_TRIGGERS.append(id)
            except Exception as e:
                db.JUP_DB_Errors.insert({"err_id": "RVRL",
                                         "error_name": str(e.__class__.__name__),
                                         "error_message": str(e.args[0]),
                                         "pos": tup1[1]['market'][0:3],
                                         "origin": tup1[1]['market'][3:6],
                                         "destination": tup1[1]['market'][6:9],
                                         "compartment": tup1[1]['market'][9:],
                                         "dep_date_start": dep_date_start,
                                         "dep_date_end": dep_date_end})

    @measure(JUPITER_LOGGER)
    def check_trigger(self, revenue_data_tw, revenue_data_lw):
        """
        Change Parameter is calculated and checked with Thresholds
        Arguments:
        :param revenue_data_tw: dict with ty and ly revenue values as key value pairs
        :param revenue_data_lw: dict with ty and ly revenue values as key value pairs
        :returns a bool True if trigger is raised
                        False if trigger is not raised
        """
        self.old_doc_data['revenue'] = revenue_data_lw['ty']
        self.new_doc_data['revenue'] = revenue_data_tw['ty']
        # print self.old_doc_data['revenue'], self.new_doc_data['revenue']
        if self.old_doc_data['revenue'] != 0 and self.old_doc_data['revenue'] != "NA" and self.new_doc_data['revenue'] != "NA":
            if self.threshold_type == 'percent':
                self.change = (
                    self.new_doc_data['revenue'] - self.old_doc_data['revenue']) * 100 / self.old_doc_data['revenue']
            else:
                self.change = (
                    self.new_doc_data['revenue'] -
                    self.old_doc_data['revenue'])
            # print 'change', self.change
            if self.change < self.lower_threshold or self.change > self.upper_threshold:
                return True
            else:
                return False
        elif self.old_doc_data['revenue'] == 0 and self.new_doc_data['revenue'] != 0 and self.old_doc_data['revenue'] != "NA" and self.new_doc_data['revenue'] != "NA":
            self.change = 'inf'
            return True
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
    for mrkt in ['UAETBSDXBY']:
        if counter == 2000:
            id_list = generate_revenue_changes_rolling_triggers(data_list=markets,
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
        id_list = generate_revenue_changes_rolling_triggers(data_list=markets,
                                                            pos=data['pos'],
                                                            origin=data['origin'],
                                                            destination=data['destination'],
                                                            compartment=data['compartment'],
                                                            db=db,
                                                            sig_flag='sig')
    # markets = []
    # counter = 0
    # for mrkt in list(sub_sig_markets_df['market'].values):
    #     if counter == 2000:
    #         id_list = generate_revenue_changes_rolling_triggers(data_list=markets,
    #                                                             pos=data['pos'],
    #                                                             origin=data['origin'],
    #                                                             destination=data['destination'],
    #                                                             compartment=data['compartment'],
    #                                                             sig_flag='sub_sig')
    #         markets = list()
    #         # for trigger in id_list:
    #         #     analyze(trigger)
    #         markets.append(mrkt)
    #         counter = 1
    #     else:
    #         markets.append(mrkt)
    #         counter += 1
    # if counter > 0:
    #     id_list = generate_revenue_changes_rolling_triggers(data_list=markets,
    #                                                         pos=data['pos'],
    #                                                         origin=data['origin'],
    #                                                         destination=data['destination'],
    #                                                         compartment=data['compartment'],
    #                                                         sig_flag='sub_sig')
        # for trigger in id_list:
        #     analyze(trigger)
    print 'Total Time Taken', time.time() - st


if __name__ == '__main__':
    client =mongo_client()
    db= client[JUPITER_DB]
    main_helper(db=db)
    client.close()