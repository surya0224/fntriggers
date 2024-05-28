"""
File Name              :   RevenueChangesVLYR.py
Author                 :   Sai Krishna
Date Created           :   2016-02-21
Description            :   Generation of a RevenueChangesVLYR trigger for a market
                           Market is defined as a combination of pos, origin, destination, compartment
Long Description for Trigger:
    Let Observation Date - '2017-02-15'
        Parameters in Comparision -
            1.  Revenue obtained for departure dates b/w '2017-02-01' to '2017-02-28'(this month) till
                '2017-02-15'(observation date)
            2.  Revenue obtained for departure dates b/w '2017-02-01' to '2017-02-28' till
                this observation date last year
            1.  Capacity This YR '2017-02-01' to '2017-02-28'
            2.  Capacity Last YR for the '2017-02-01' to '2017-02-28'
            The percentage change on the above 2 parameters with capacity normalization of the second one
            is obtained and compared with thresholds.

MODIFICATIONS LOG
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :

"""
import math
import traceback
from copy import deepcopy
import inspect
import datetime
import jupiter_AI.common.ClassErrorObject as error_class
from jupiter_AI.network_level_params import today, SYSTEM_DATE,JUPITER_LOGGER
from jupiter_AI.triggers.common import get_start_end_dates, get_trigger_config_dates
from jupiter_AI.triggers.data_level.MainClass import DataLevel
from jupiter_AI import mongo_client, JUPITER_DB, Host_Airline_Hub
from jupiter_AI.logutils import measure
#db = client[JUPITER_DB]
RVLYR_TRIGGERS = []


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
def generate_revenue_changes_vlyr_triggers(
        pos, origin, destination, compartment, data_list, db, sig_flag=None):
    try:
        data = {
            'pos': pos,
            'origin': origin,
            'destination': destination,
            'compartment': compartment
        }
        book_date_start = str(today.year) + '-01-01'
        book_date_end = SYSTEM_DATE

        if sig_flag:
            dates_list = get_trigger_config_dates(db=db, sig_flag=sig_flag)
            if len(dates_list) > 0:
                for date in dates_list:
                    trigger_obj = RevenueChangesVLYR(data, SYSTEM_DATE)
                    trigger_obj.do_analysis(dep_date_start=date['start'],
                                            dep_date_end=date['end'],
                                            db=db,
                                            data_list=data_list)
        else:
            month1 = today.month
            year1 = today.year
            dep_date_start1, dep_date_end1 = get_start_end_dates(month1, year1)
            trigger_obj1 = RevenueChangesVLYR(data, SYSTEM_DATE)
            trigger_obj1.do_analysis(dep_date_start=SYSTEM_DATE,
                                     dep_date_end=dep_date_end1,
                                     db=db,
                                     data_list=data_list)

            if month1 != 12:
                month2 = deepcopy(month1 + 1)
                year2 = deepcopy(year1)
            else:
                month2 = 1
                year2 = year1 + 1
            dep_date_start2, dep_date_end2 = get_start_end_dates(month2, year2)
            trigger_obj2 = RevenueChangesVLYR(data, SYSTEM_DATE)
            trigger_obj2.do_analysis(dep_date_start=dep_date_start2,
                                     dep_date_end=dep_date_end2,
                                     db=db,
                                     data_list=data_list)

            if month2 != 12:
                month3 = deepcopy(month2 + 1)
                year3 = deepcopy(year2)
            else:
                month3 = 1
                year3 = year1 + 1
            dep_date_start3, dep_date_end3 = get_start_end_dates(month3, year3)
            trigger_obj3 = RevenueChangesVLYR(data, SYSTEM_DATE)
            trigger_obj3.do_analysis(dep_date_start=dep_date_start3,
                                     dep_date_end=dep_date_end3,
                                     db=db,
                                     data_list=data_list)

            if month3 != 12:
                month4 = deepcopy(month3 + 1)
                year4 = deepcopy(year3)
            else:
                month4 = 1
                year4 = year1 + 1
            dep_date_start4, dep_date_end4 = get_start_end_dates(month4, year4)
            trigger_obj4 = RevenueChangesVLYR(data, SYSTEM_DATE)
            trigger_obj4.do_analysis(dep_date_start=dep_date_start4,
                                     dep_date_end=dep_date_end4,
                                     db=db,
                                     data_list=data_list)
    except Exception as error_msg:
        print "ERROR!!!"
        print traceback.print_exc()
        obj_error = error_class.ErrorObject(
            error_class.ErrorObject.ERRORLEVEL1,
            'jupter_AI/triggers/data_level/RevenueChangesVLYR.py method: generate_revenue_changes_vlyr_triggers',
            get_arg_lists(
                inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())
    """

    :return:
    """
    return RVLYR_TRIGGERS

class RevenueChangesVLYR(DataLevel):
    @measure(JUPITER_LOGGER)
    def __init__(self, data, system_date):
        super(RevenueChangesVLYR, self).__init__(data, system_date)
        self.old_doc_data = deepcopy(data)
        self.new_doc_data = deepcopy(data)
        self.trigger_date = system_date

    @measure(JUPITER_LOGGER)
    def do_analysis(self, dep_date_start, dep_date_end, data_list, db, is_event_trigger=False):
        """

        :param book_date_start:
        :param book_date_end:
        :param dep_date_start:
        :param dep_date_end:
        :return:
        """
        book_date_start = str(today.year) + '-01-01'
        book_date_end = SYSTEM_DATE

        revenue_data = self.get_sales_flown_data(
            book_date_start=book_date_start,
            book_date_end=book_date_end,
            dep_date_start=dep_date_start,
            dep_date_end=dep_date_end,
            parameter='revenue',
            db=db,
            data_list=data_list,
            is_vlyr=1)
        # print 'revenue'
        # print revenue_data
        revenue_data.sort_values(by='market', inplace=True)
        # print revenue_data
        # capacity_data = self.get_capacity_data(dep_date_start=dep_date_start,
        #                                        dep_date_end=dep_date_end,
        #                                        data_list=data_list)
        # # print 'capacity'
        # capacity_data.sort_values(by='market', inplace=True)
        # print capacity_data

        self.get_trigger_details(trigger_name='revenue_changes_VLYR', db=db)
        for tup1 in revenue_data.iterrows():
            try:
                self.old_doc_data['pos'] = tup1[1]['market'][0:3]
                self.old_doc_data['origin'] = tup1[1]['market'][3:6]
                self.old_doc_data['destination'] = tup1[1]['market'][6:9]
                self.old_doc_data['compartment'] = tup1[1]['market'][9:]
                self.new_doc_data = deepcopy(self.old_doc_data)
                trigger_occurence = self.check_trigger(
                    revenue_ly=tup1[1]['ly'],
                    revenue_ty=tup1[1]['ty'])
                    # capacity_ly=tup2[1]['capacity_ly'],
                    # capacity_ty=tup2[1]['capacity_ty'])
                # print 'occurence', trigger_occurence
    # need some changes here  pax should be replace by  revenue last year and
    # revenue this year
                self.triggering_data = {
                    'dep_date_start': dep_date_start,
                    'dep_date_end': dep_date_end,
                    'ly': {
                        'revenue': tup1[1]['ly'],
                        # 'capacity': tup2[1]['capacity_ly']
                    },
                    'ty': {
                        'dep_date_start': dep_date_start,
                        'dep_date_end': dep_date_end,
                        'revenue': tup1[1]['ty'],
                        # 'capacity': tup2[1]['capacity_ty']
                    }
                }
                id = self.generate_trigger_new(trigger_occurence,
                                               dep_date_start=dep_date_start,
                                               dep_date_end=dep_date_end,
                                               db=db,
                                               is_event_trigger=is_event_trigger)
                RVLYR_TRIGGERS.append(id)
            except Exception as e:
                db.JUP_DB_Errors.insert({"err_id": "RVVL",
                                         "error_name": str(e.__class__.__name__),
                                         "error_message": str(e.args[0]),
                                         "pos": tup1[1]['market'][0:3],
                                         "origin": tup1[1]['market'][3:6],
                                         "destination": tup1[1]['market'][6:9],
                                         "compartment": tup1[1]['market'][9:],
                                         "dep_date_start": dep_date_start,
                                         "dep_date_end": dep_date_end})

    @measure(JUPITER_LOGGER)
    def check_trigger(self, revenue_ly, revenue_ty):
        self.old_doc_data['revenue'] = revenue_ly
        # self.old_doc_data['capacity'] = capacity_ly
        self.new_doc_data['revenue'] = revenue_ty
        # self.new_doc_data['capacity'] = capacity_ty
        # print self.old_doc_data['revenue'], self.new_doc_data['revenue']
        if self.old_doc_data['revenue'] > 0 and revenue_ly != "NA" and revenue_ty != "NA":
            # if self.old_doc_data['capacity'] and self.new_doc_data['capacity'] and self.new_doc_data[
            #         'capacity'] != 'NA' and self.old_doc_data['capacity'] != 'NA':
            #     self.new_doc_data['revenue_expected'] = self.new_doc_data['revenue']
            # else:
            #     self.new_doc_data['revenue_expected'] = self.new_doc_data['revenue']
            if self.threshold_type == "percent":
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
        elif self.old_doc_data['revenue'] == 0 and \
             self.new_doc_data['revenue'] != 0:
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
    for mrkt in ['UAEODSDXBY']:
        if counter == 2000:
            id_list = generate_revenue_changes_vlyr_triggers(data_list=markets,
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
        id_list = generate_revenue_changes_vlyr_triggers(data_list=markets,
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
    #         id_list = generate_revenue_changes_vlyr_triggers(data_list=markets,
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
    #     id_list = generate_revenue_changes_vlyr_triggers(data_list=markets,
    #                                                         pos=data['pos'],
    #                                                         origin=data['origin'],
    #                                                         destination=data['destination'],
    #                                                         compartment=data['compartment'],
    #                                                         sig_flag='sub_sig')
        # for trigger in id_list:
        #     analyze(trigger)
    print 'Total Time Taken', time.time() - st


if __name__ == '__main__':
    client = mongo_client()
    db = client[JUPITER_DB]
    main_helper(db=db)
    client.close()