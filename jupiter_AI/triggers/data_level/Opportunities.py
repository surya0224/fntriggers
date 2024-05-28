"""
Author: Abhinav Garg
Created with <3
Date Created: 2017-12-28
Code functionality:
    Raises triggers based on opportunity/threat in a market beyond certain threshold
    Compare market share and fair market share for every pos_od_compartment combination
    and raise trigger if the difference between the two is beyond threshold.
Modifications log:
    1. Author:
       Exact modification made or some logic changed:
       Date of modification:
    2. Author:
       Exact modification made or some logic changed:
       Date of modification:
    3. Author:
       Exact modification made or some logic changed:
       Date of modification:


"""
import datetime
import inspect
import traceback
from copy import deepcopy
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
import jupiter_AI.common.ClassErrorObject as error_class
from jupiter_AI import mongo_client
from jupiter_AI.common.calculate_market_share import calculate_market_share
from jupiter_AI.network_level_params import Host_Airline_Code, today, SYSTEM_DATE, JUPITER_DB, JUPITER_LOGGER, Host_Airline_Hub
from jupiter_AI.triggers.common import get_start_end_dates, get_threshold_values, cursor_to_df, get_trigger_config_dates
from jupiter_AI.triggers.data_level.MainClass import DataLevel
#from jupiter_AI.triggers.listener_data_level_trigger import analyze
from jupiter_AI.logutils import measure

#db = client[JUPITER_DB]
upper, lower = 0, 0
opp_triggers = []


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
def generate_ms_vs_fms_opportunities_trigger(pos,
                                             origin,
                                             destination,
                                             compartment, db):
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
        trigger_obj1 = OpportunitiesMrktShrFMS(data, SYSTEM_DATE)
        trigger_obj1.do_analysis(dep_date_start=dep_date_start1,
                                 dep_date_end=dep_date_end1, db=db)

        #   Obtaining the next set of departure dates(Current Month + month)
        # if month1 != 12:
        #     month2 = deepcopy(month1 + 1)
        #     year2 = deepcopy(year1)
        # else:
        #     month2 = 1
        #     year2 = year1 + 1
        #
        # #   Generating the trigger for Second Set of departure dates into consideration (Current Month + 1)
        # dep_date_start2, dep_date_end2 = get_start_end_dates(month2, year2)
        # trigger_obj2 = OpportunitiesMrktShrFMS(data, SYSTEM_DATE)
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
        #
        # #   Generating the trigger for Third Set of departure dates into consideration (Current Month + 2)
        # dep_date_start3, dep_date_end3 = get_start_end_dates(month3, year3)
        # trigger_obj3 = OpportunitiesMrktShrFMS(data, SYSTEM_DATE)
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
        #
        # #   Generating the trigger for Fourth Set of departure dates into consideration (Current Month + 3)
        # dep_date_start4, dep_date_end4 = get_start_end_dates(month4, year4)
        # trigger_obj4 = OpportunitiesMrktShrFMS(data, SYSTEM_DATE)
        # trigger_obj4.do_analysis(dep_date_start=dep_date_start4,
        #                          dep_date_end=dep_date_end4)
    except Exception as error_msg:
        print "ERROR!!!"
        module_name = ''.join(['jupiter_AI/triggers/data_level/OpportunitiesMrktShrFMS.py ',
                               'method: generate_ms_vs_fms_opportunities_trigger'])
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                            module_name,
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())
        traceback.print_exc()
        # raise error_msg


class OpportunitiesMrktShrFMS(DataLevel):
    @measure(JUPITER_LOGGER)
    def __init__(self, data, system_date):
        super(OpportunitiesMrktShrFMS, self).__init__(data, system_date)
        self.old_doc_data = deepcopy(data)
        self.new_doc_data = deepcopy(data)
        self.trigger_date = system_date

    @measure(JUPITER_LOGGER)
    def get_mrkt_shr_fms_values(self, dep_date_start, dep_date_end, db):
        """
        :param dep_start_date:
        :param dep_end_date:
        :return:
        """
        response = dict(ms=None, fms=None)
        market_share_data = calculate_market_share(airline=Host_Airline_Code,
                                                   pos=self.old_doc_data['pos'],
                                                   origin=self.old_doc_data['origin'],
                                                   destination=self.old_doc_data['destination'],
                                                   compartment=self.old_doc_data['compartment'],
                                                   dep_date_from=dep_date_start,
                                                   dep_date_to=dep_date_end,
                                                   db=db
                                                   )
        market_share = market_share_data['market_share']
        fms = market_share_data['fms']
        response['ms'] = market_share
        response['fms'] = fms
        return response

    @measure(JUPITER_LOGGER)
    def do_analysis(self, dep_date_start, dep_date_end, db):
        """
        :return:
        """
        # market_share_data = self.get_mrkt_shr_fms_values(dep_date_start=dep_date_start,
        #                                                  dep_date_end=dep_date_end)
        # ms = market_share_data['ms']
        # fms = market_share_data['fms']
        self.get_trigger_details(trigger_name='opportunities', db=db)
        global upper, lower
        self.triggering_data = {
            'dep_date_start': dep_date_start,
            'dep_date_end': dep_date_end,
            'lower_threshold': lower,
            'upper_threshold': upper
        }
        trigger_status = self.check_trigger()
        id = self.generate_trigger_new(trigger_status=trigger_status, dep_date_start=dep_date_start,
                                       dep_date_end=dep_date_end,
                                       db=db)
        return id

    @measure(JUPITER_LOGGER)
    def check_trigger(self):
        """
        :param ms:
        :param fms:
        :return:
        """
        # print self.old_doc_data['market_share'], self.old_doc_data['FMS']
        # response = False
        # if type() and type(fms) in [int,float]:
        #     if ms >= 0 and fms > 0:
        #         if self.threshold_type == "percent":
        #             self.change = (ms - fms) * 100 / float(fms)
        #         else:
        #             self.change = (ms - fms)
        #         if not self.lower_threshold < self.change < self.upper_threshold:
        #             response = True
        self.change = self.old_doc_data['difference']
        return True


@measure(JUPITER_LOGGER)
def raise_opportunities_trigger(db, markets, sig_flag=None):
    df = pd.DataFrame(columns=["pos",
                               "od",
                               "compartment",
                               "month",
                               "year",
                               "airline",
                               "market_share",
                               "FMS"])

    if sig_flag:
        dates_list = get_trigger_config_dates(db=db, sig_flag=sig_flag)
        if len(dates_list) > 0:
            for date in dates_list:
                if date['code_list'][0][0] == 'M':
                    print "querying relevant data"
                    cur = db.JUP_DB_Pos_OD_Compartment_new.aggregate([
                        {
                            "$match": {
                                'market': {'$in': markets},
                                "month": datetime.datetime.strptime(date['start'], "%Y-%m-%d").month,
                                'year': datetime.datetime.strptime(date['start'], "%Y-%m-%d").year,
                            }
                        },
                        {
                            "$unwind": "$top_5_comp_data"
                        },
                        {
                            "$match": {
                                "top_5_comp_data.airline": Host_Airline_Code
                            }
                        },
                        {
                            "$project": {
                                "pos": 1,
                                "od": 1,
                                "compartment": 1,
                                "month": 1,
                                "year": 1,
                                "airline": "$top_5_comp_data.airline",
                                "market_share": "$top_5_comp_data.market_share",
                                "FMS": "$top_5_comp_data.FMS",
                                "_id": 0
                            }
                        }
                    ], allowDiskUse=True)
                    print "queried"

                    temp_df = cursor_to_df(cursor=cur)
                    df = pd.concat([df, temp_df])
    else:
        next_month = today + relativedelta(months=1)
        next_to_next_month = today + relativedelta(months=2)
        next_to_next_to_next_month = today + relativedelta(months=3)
        print "querying relevant data"
        cur = db.JUP_DB_Pos_OD_Compartment_new.aggregate([
            {
                "$match": {
                    'market': {'$in': markets},
                    "$or": [
                        {"month": today.month, "year": today.year},
                        {"month": next_month.month, "year": next_month.year},
                        {"month": next_to_next_month.month, "year": next_to_next_month.year},
                        {"month": next_to_next_to_next_month.month, "year": next_to_next_to_next_month.year}
                    ]
                }
            },
            {
                "$unwind": "$top_5_comp_data"
            },
            {
                "$match": {
                    "top_5_comp_data.airline": Host_Airline_Code
                }
            },
            {
                "$project": {
                    "pos": 1,
                    "od": 1,
                    "compartment": 1,
                    "month": 1,
                    "year": 1,
                    "airline": "$top_5_comp_data.airline",
                    "market_share": "$top_5_comp_data.market_share",
                    "FMS": "$top_5_comp_data.FMS",
                    "_id": 0
                }
            }
        ], allowDiskUse=True)
        print "queried"
        df = cursor_to_df(cursor=cur)

    df.dropna(inplace=True)
    df['difference'] = df['FMS'] - df['market_share']
    df['flag'] = False
    global upper, lower
    upper, lower = get_threshold_values(trigger_type='opportunities', db=db)
    print "thresholds", lower, upper
    df.loc[(df['difference'] >= float(upper)) | (df['difference'] <= float(lower)), 'flag'] = True

    true_df = df.loc[df['flag'] == True, :]
    print "true df for opportunities trigger", true_df.head()
    print true_df.shape[0]
    global opp_triggers
    for idx, row in true_df.iterrows():
        print "------>", row['difference']
        try:
            data = dict()
            start_date, end_date = get_start_end_dates(int(row['month']), int(row['year']))
            if datetime.datetime.strptime(start_date, '%Y-%m-%d').month == today.month:
                start_date = SYSTEM_DATE
            data['pos'] = row['pos']
            data['od'] = row['od']
            data['compartment'] = row['compartment']
            data['origin'] = row['od'][:3]
            data['destination'] = row['od'][3:]
            data['month'] = row['month']
            data['year'] = row['year']
            data['airline'] = row['airline']
            data['market_share'] = row['market_share']
            data['FMS'] = row['FMS']
            data['difference'] = row['difference']
            obj = OpportunitiesMrktShrFMS(data, SYSTEM_DATE)
            id = obj.do_analysis(dep_date_start=start_date, dep_date_end=end_date, db=db)
            opp_triggers.append(id)

        except Exception as error_msg:
            print "ERROR!!!"
            db.JUP_DB_Errors.insert({"err_id": "OPPO",
                                     "error_name": str(error_msg.__class__.__name__),
                                     "error_message": str(error_msg.args[0])})
            module_name = ''.join(['jupiter_AI/triggers/data_level/OpportunitiesMrktShrFMS.py ',
                                   'method: raise_opportunities_trigger'])
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                module_name,
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list(str(error_msg))
            obj_error.write_error_logs(datetime.datetime.now())
            traceback.print_exc()

    return opp_triggers


if __name__ == '__main__':
    client =mongo_client()
    db=client[JUPITER_DB]
    # generate_ms_vs_fms_opportunities_trigger(pos='DXB',
    #                                          origin='DXB',
    #                                          destination='DOH',
    #                                          compartment='Y')
    data = {
        'pos': 'DXB',
        'origin': 'DXB',
        'destination': 'DOH',
        'compartment': 'Y'
    }
    import time

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
    for mrkt in list(sig_markets_df['market'].values):
        if counter == 2000:
            id_list = raise_opportunities_trigger(markets=markets, sig_flag='sig', db=db)
            markets = list()
            # for trigger in id_list:
            #     analyze(trigger)
            markets.append(mrkt)
            counter = 1
        else:
            markets.append(mrkt)
            counter += 1
    if counter > 0:
        id_list = raise_opportunities_trigger(markets=markets, sig_flag='sig', db=db)
    markets = []
    counter = 0
    for mrkt in list(sub_sig_markets_df['market'].values):
        if counter == 2000:
            id_list = raise_opportunities_trigger(markets=markets, sig_flag='sub_sig', db=db)
            markets = list()
            # for trigger in id_list:
            #     analyze(trigger)
            markets.append(mrkt)
            counter = 1
        else:
            markets.append(mrkt)
            counter += 1
    if counter > 0:
        id_list = raise_opportunities_trigger(markets=markets, sig_flag='sub_sig', db=db)
        # for trigger in id_list:
        #     analyze(trigger)
    client.close()
    print 'Total Time Taken', time.time() - st
