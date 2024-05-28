import inspect
import random
import time

import datetime
import pandas as pd
import numpy as np
import traceback

from bson import ObjectId
from dateutil.relativedelta import relativedelta
from jupiter_AI.logutils import measure
from jupiter_AI import mongo_client, JUPITER_DB, SYSTEM_DATE, today, JUPITER_LOGGER
#from jupiter_AI.batch.JUP_AI_Batch_Market_Characteristics1 import get_pos_od_compartment_combinations as get_pos_od_compartment_combinations_ms_collection
#from jupiter_AI.common.ClassErrorObject import ErrorObject
from jupiter_AI.triggers.common import get_start_end_dates
from jupiter_AI.triggers.data_level.BookingChangesRolling import BookingChangesRolling
from jupiter_AI.triggers.data_level.BookingChangesVLYR import BookingChangesVLYR
#from jupiter_AI.triggers.data_level.BookingChangesVTGT import BookingChangesVTGT
from jupiter_AI.triggers.data_level.BookingChangesWeekly import BookingChangesWeekly
from jupiter_AI.triggers.data_level.PaxChangesRolling import PaxChangesRolling
from jupiter_AI.triggers.data_level.PaxChangesVLYR import PaxChangesVLYR
from jupiter_AI.triggers.data_level.PaxChangesVTGT import PaxChangesVTGT
from jupiter_AI.triggers.data_level.PaxChangesWeekly import PaxChangesWeekly
from jupiter_AI.triggers.data_level.RevenueChangesRolling import RevenueChangesRolling
from jupiter_AI.triggers.data_level.RevenueChangesVLYR import RevenueChangesVLYR
from jupiter_AI.triggers.data_level.RevenueChangesVTGT import RevenueChangesVTGT
from jupiter_AI.triggers.data_level.RevenueChangesWeekly import RevenueChangesWeekly
from jupiter_AI.triggers.data_level.YieldChangesRolling import YieldChangesRolling
from jupiter_AI.triggers.data_level.YieldChangesVLYR import YieldChangesVLYR
from jupiter_AI.triggers.data_level.YieldChangesVTGT import YieldChangesVTGT
from jupiter_AI.triggers.data_level.YieldChangesWeekly import YieldChangesWeekly
from jupiter_AI.triggers.listener_data_level_trigger import analyze

#db = client[JUPITER_DB]
COLLECTION_UPDATE_LIST = ['JUP_DB_Workflow', 'JUP_DB_Workflow_OD_User']
COLLECTION_DEPENDENCY_LIST = ['JUP_DB_Manual_Triggers_Module']
START_TIME = time.time()
TRIGGERS = []
start_date = SYSTEM_DATE
start_time = time.time()
error_flag = 0


@measure(JUPITER_LOGGER)
def update_running_status(db):
    id = db.JUP_DB_Data_Status.insert(
        {
            'team': 'analytics',
            'type': 'script',
            'level': 2,
            'name': 'run_booking_triggers.py',
            'title': 'data_level_triggers',
            'stage': 'operations',
            'last_update_date': datetime.datetime.today().strftime('%Y-%m-%d'),
            'last_update_time': datetime.datetime.today().strftime('%H:%M'),
            'start_time': datetime.datetime.today().strftime('%H:%M'),
            'end_time': datetime.datetime.today().strftime('%H:%M'),
            'run_time': time.time() - START_TIME,
            'collections_updated': COLLECTION_UPDATE_LIST,
            'collections_dependencies': COLLECTION_DEPENDENCY_LIST,
            'params_updated': [
                                {
                                    'collection': '',
                                    'fields': []
                                }
                               ],
            'number_of_params': np.nan,
            'logs': {},
            'ip': '172.28.23.8',
            'port': '3354',
            'URL': '',
            'status': 'Running',
            'error': 0
        }
    )
    return id


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
def get_module_name():
    return inspect.stack()[1][3]


@measure(JUPITER_LOGGER)
def run_events_triggers(data_list, db):
    bkwk_triggers = []
    bkrl_triggers = []
    bkvl_triggers = []
    bkvt_triggers = []

    pxwk_triggers = []
    pxrl_triggers = []
    pxvl_triggers = []
    pxvt_triggers = []

    rvwk_triggers = []
    rvrl_triggers = []
    rvvl_triggers = []
    rvvt_triggers = []

    yiwk_triggers = []
    yirl_triggers = []
    yivl_triggers = []
    yivt_triggers = []

    final_df = pd.DataFrame()
    global TRIGGERS
    # print "data_list: ", data_list
    book_date_start = str(today.year) + '-01-01'
    book_date_end = SYSTEM_DATE
    market_df = pd.DataFrame()
    market_df['market'] = data_list
    market_df['od'] = market_df['market'].str.slice(3, 9)
    od_list = list(market_df['od'].values)
    for i in range(4):
        today_date = today + relativedelta(months=i)
        month = today_date.month
        year = today_date.year
        key_start = "Start_date_" + str(year)
        key_end = "End_date_" + str(year)
        # print "today_date: ", today_date
        dep_date_start, dep_date_end = get_start_end_dates(month, year)
        # print "od_list: ", od_list
        events_crsr = db.JUP_DB_Pricing_Calendar.find({"Market": {"$in": od_list}, key_start: {"$lte": dep_date_end}, key_end: {"$gte": dep_date_start}}, {"_id":0})
        events_crsr = list(events_crsr)
        # print "events_crsr: ", events_crsr
        if len(events_crsr) > 0:
            events_df = pd.DataFrame(events_crsr)
            events_df = events_df[['Holiday_Name', key_end, key_start, 'Market']]
            events_df = events_df.drop_duplicates(subset=['Holiday_Name', 'Market'])
            events_df = events_df.rename(columns={key_start: "dep_date_start", key_end: "dep_date_end", "Market": "od"})
            temp_df = market_df.merge(events_df, on='od', how='inner')
            final_df = pd.concat([final_df, temp_df])

    events_crsr = db.JUP_DB_Pricing_Calendar.find({"Market": {"$in": od_list}, "event_type": "user_defined"}, {"_id":0})
    events_crsr = list(events_crsr)
    if len(events_crsr) > 0:
        ud_events = pd.DataFrame(events_crsr)
        ud_events = ud_events.drop_duplicates(subset=['Holiday_Name', 'Market', 'start_year'])
        unique_start_year = ud_events['start_year'].unique()
        for unique_year in unique_start_year:
            key_start = "Start_date_" + str(int(unique_year))
            key_end = "End_date_" + str(int(unique_year))
            temp_df = ud_events.loc[ud_events['start_year'] == unique_year, ["Market", 'Holiday_Name', key_start, key_end]]
            temp_df = temp_df.rename(columns={key_start: "dep_date_start", key_end: "dep_date_end", 'Market': 'od'})
            temp_df = market_df.merge(temp_df, on='od', how='inner')
            final_df = pd.concat([final_df, temp_df])

    if len(final_df) > 0:
        final_df = final_df.drop_duplicates()
        final_df = final_df[final_df['dep_date_start'] >= SYSTEM_DATE]

    for idx, row in final_df.iterrows():
        data = {
            "pos": row['market'][0:3],
            "origin": row['market'][3:6],
            "destination": row['market'][6:9],
            "compartment": row['market'][-1]
        }
        try:
            bkwk = BookingChangesWeekly(data, SYSTEM_DATE)
            bkwk_triggers = bkwk_triggers + bkwk.do_analysis(dep_date_start=row['dep_date_start'],
                                             dep_date_end=row['dep_date_end'],
                                             data_list=[row['market']],
                                             db=db,
                                             is_event_trigger=True)
        except Exception:
            bkwk_triggers = bkwk_triggers + []

        try:
            bkrl = BookingChangesRolling(data, SYSTEM_DATE)
            bkrl_triggers = bkrl_triggers + bkrl.do_analysis(dep_date_start=row['dep_date_start'],
                                             dep_date_end=row['dep_date_end'],
                                             data_list=[row['market']],
                                             db=db,
                                             is_event_trigger=True)
        except Exception:
            bkrl_triggers = bkrl_triggers + []
        try:
            bkvl = BookingChangesVLYR(data, SYSTEM_DATE)
            bkvl_triggers = bkvl_triggers + bkvl.do_analysis(dep_date_start=row['dep_date_start'],
                                             dep_date_end=row['dep_date_end'],
                                             db=db,
                                             data_list=[row['market']],is_event_trigger=True)
        except Exception:
            bkvl_triggers = bkvl_triggers + []

        # try:
        #     bkvt = BookingChangesVTGT(data, SYSTEM_DATE)
        #     bkvt_triggers = bkvt_triggers + bkvt.do_analysis(dep_date_start=row['dep_date_start'],
        #                                      dep_date_end=row['dep_date_end'],
        #                                      data_list=[row['market']],is_event_trigger=True)
        # except Exception:
        #     bkvt_triggers = bkvt_triggers + []

        try:
            pxwk = PaxChangesWeekly(data, SYSTEM_DATE)
            pxwk_triggers = pxwk_triggers + pxwk.do_analysis(dep_date_start=row['dep_date_start'],
                                             dep_date_end=row['dep_date_end'],
                                             db=db,
                                             data_list=[row['market']],is_event_trigger=True)
        except Exception:
            pxwk_triggers = pxwk_triggers + []

        try:
            pxrl = PaxChangesRolling(data, SYSTEM_DATE)
            pxrl_triggers = pxrl_triggers + pxrl.do_analysis(dep_date_start=row['dep_date_start'],
                                             dep_date_end=row['dep_date_end'],
                                             db=db,
                                             data_list=[row['market']],is_event_trigger=True
                                             )
        except Exception:
            pxrl_triggers = pxrl_triggers + []

        try:
            pxvl = PaxChangesVLYR(data, SYSTEM_DATE)
            pxvl_triggers = pxvl_triggers + pxvl.do_analysis(dep_date_start=row['dep_date_start'],
                                             dep_date_end=row['dep_date_end'],
                                             data_list=[row['market']],
                                             db=db,
                                             book_date_start=book_date_start,
                                             book_date_end=book_date_end,is_event_trigger=True)
        except Exception:
            pxvl_triggers = pxvl_triggers + []

        try:
            pxvt = PaxChangesVTGT(data, SYSTEM_DATE)
            pxvt_triggers = pxvt_triggers + pxvt.do_analysis(dep_date_start=row['dep_date_start'],
                                             dep_date_end=row['dep_date_end'],
                                             db=db,
                                             data_list=[row['market']],is_event_trigger=True)
        except Exception:
            pxvt_triggers = pxvt_triggers + []

        try:
            rvwk = RevenueChangesWeekly(data, SYSTEM_DATE)
            rvwk_triggers = rvwk_triggers + rvwk.do_analysis(dep_date_start=row['dep_date_start'],
                                             dep_date_end=row['dep_date_end'],
                                             db=db,
                                             data_list=[row['market']],is_event_trigger=True)
        except Exception:
            rvwk_triggers = rvwk_triggers + []

        try:
            rvrl = RevenueChangesRolling(data, SYSTEM_DATE)
            rvrl_triggers = rvrl_triggers + rvrl.do_analysis(dep_date_start=row['dep_date_start'],
                                             dep_date_end=row['dep_date_end'],
                                             db=db,
                                             data_list=[row['market']],is_event_trigger=True)
        except Exception:
            rvrl_triggers = rvrl_triggers + []

        try:
            rvvl = RevenueChangesVLYR(data, SYSTEM_DATE)
            rvvl_triggers = rvvl_triggers + rvvl.do_analysis(dep_date_start=row['dep_date_start'],
                                             dep_date_end=row['dep_date_end'],
                                             db=db,
                                             data_list=[row['market']],is_event_trigger=True)
        except Exception:
            rvvl_triggers = rvvl_triggers + []

        try:
            rvvt = RevenueChangesVTGT(data, SYSTEM_DATE)
            rvvt_triggers = rvvt_triggers + rvvt.do_analysis(dep_date_start=row['dep_date_start'],
                                             dep_date_end=row['dep_date_end'],
                                             db=db,
                                             data_list=[row['market']],is_event_trigger=True)
        except Exception:
            rvvt_triggers = rvvt_triggers + []

        try:
            yiwk = YieldChangesWeekly(data, SYSTEM_DATE)
            yiwk_triggers = yiwk_triggers +  yiwk.do_analysis(dep_date_start=row['dep_date_start'],
                                             dep_date_end=row['dep_date_end'],
                                             db=db,
                                             data_list=[row['market']],is_event_trigger=True)
        except Exception:
            yiwk_triggers = yiwk_triggers + []

        try:
            yirl = YieldChangesRolling(data, SYSTEM_DATE)
            yirl_triggers = yirl_triggers + yirl.do_analysis(dep_date_start=row['dep_date_start'],
                                             dep_date_end=row['dep_date_end'],
                                             db=db,
                                             data_list=[row['market']],is_event_trigger=True)
        except Exception:
            yirl_triggers = yirl_triggers + []

        try:
            yivl = YieldChangesVLYR(data, SYSTEM_DATE)
            yivl_triggers = yivl_triggers + yivl.do_analysis(dep_date_start=row['dep_date_start'],
                                             dep_date_end=row['dep_date_end'],
                                             db=db,
                                             data_list=[row['market']],is_event_trigger=True)
        except Exception:
            yivl_triggers = yivl_triggers + []

        try:
            yivt = YieldChangesVTGT(data, SYSTEM_DATE)
            yivt_triggers = yivt_triggers + yivt.do_analysis(dep_date_start=row['dep_date_start'],
                                             dep_date_end=row['dep_date_end'],
                                             db=db,
                                             data_list=[row['market']],is_event_trigger=True)
        except Exception:
            yivt_triggers = yivt_triggers + []

        TRIGGERS = TRIGGERS + bkwk_triggers + bkrl_triggers + bkvt_triggers + bkvl_triggers
        TRIGGERS = TRIGGERS + pxwk_triggers + pxrl_triggers + pxvt_triggers + pxvl_triggers
        TRIGGERS = TRIGGERS + rvwk_triggers + rvrl_triggers + rvvt_triggers + rvvl_triggers
        TRIGGERS = TRIGGERS + yiwk_triggers + yirl_triggers + yivt_triggers + yivl_triggers

@measure(JUPITER_LOGGER)
def run(markets, db):
    object_id = update_running_status(db)
    global TRIGGERS
    error_flag = 0
    # try:
    # pos_od_combinations = get_pos_od_compartment_combinations()
    #  This query hits JUP_DB_POS_OD_Mapping collection which is just a permutation
    # of all the POS_OD_compartments theoretically possible
    # markets = []
    # pos_od_combinations = get_pos_od_compartment_combinations_ms_collection()
    # pos_origin_list = []
    # pos_not_origin_list = []
    # print "Getting Markets where POS == Origin . . . . . . "
    # st_segregate_markets = time.time()
    # for market in pos_od_combinations:
    #     if market[0:3] == market[3:6]:
    #         pos_origin_list.append(market)
    #     else:
    #         pos_not_origin_list.append(market)
    # markets = pos_origin_list + pos_not_origin_list
    # print "Time taken to segregate Markets into POS == Origin and POS != Origin = ", time.time() - st_segregate_markets
    # #
    # print 'No of pos/od/compartment combinations', len(markets)
    # print "No. of pos/od/compartment combinations where POS == ORIGIN = ", len(pos_origin_list)
    # temp_markets_25 = ['CMBDXBCMBY',
    #                  'CMBDXBCMBJ',
    #                  'CMBCMBDXBY',
    #                  'CMBCMBDXBJ',
    #                  'CMBMLEAMMY',
    #                  'CMBMLEAMMJ',
    #                  'AMMAMMDXBY',
    #                  'AMMAMMDXBJ',
    #                  'AMMDXBAMMY',
    #                  'AMMDXBAMMJ',
    #                  'AMMDACAMMY',
    #                  'AMMDACAMMJ',
    #                  'AMMAMMDXBY',
    #                  'AMMAMMDXBJ',
    #                  'CGPDXBCGPY',
    #                  'CGPDXBCGPJ',
    #                  'CGPEBBCGPY',
    #                  'CGPEBBCGPJ',
    #                  'CGPSLLDACY',
    #                  'CGPSLLDACJ',
    #                  'UAEKTMDXBY',
    #                  'UAEKTMDXBJ',
    #                  'UAEDACDXBY',
    #                  'UAEDACDXBJ',
    #                  'UAECGPDXBY',
    #                  'UAECGPDXBJ',
    #                  'CMBCMBKWIY',
    #                  'CMBCMBKWIJ',
    #                  'PZUPZUDXBY',
    #                  'PZUPZUDXBJ',
    #                  'BAHBAHDXBY',
    #                  'BAHBAHDXBJ',
    #                  'UAEDXBVKOY',
    #                  'UAEDXBVKOJ',
    #                  'UAEDXBGYDY',
    #                  'UAEDXBGYDJ',
    #                  'BAHMHDBAHY',
    #                  'BAHMHDBAHJ',
    #                  'BAHNJFBAHY',
    #                  'BAHNJFBAHJ',
    #                  'BAHBAHNJFY',
    #                  'BAHBAHNJFJ',
    #                  'RUHKTMRUHY',
    #                  'RUHKTMRUHJ',
    #                  'RUHKTMAHBY',
    #                  'RUHKTMAHBJ',
    #                  'RUHKTMELQY',
    #                  'RUHKTMELQJ',
    #                  'KWIBOMKWIY',
    #                  'KWIBOMKWIJ']
    st_data_level = time.time()
    # print "Generating DATA LEVEL TRIGGERS . . . . ."
    # total_markets = len(markets)
    # BATCH_SIZE = 100
    # count_batch = 0
    # run = 1
    # # db.JUP_DB_Workflow.remove({})
    # # db.JUP_DB_Workflow_OD_User.remove({})
    # # db.JUP_DB_Errors.remove({})
    # while count_batch < total_markets:
    #     temp_markets = markets[count_batch: count_batch + BATCH_SIZE]
    #     if run==1:
    #         temp_markets = temp_markets_25 + temp_markets
    run_events_triggers(db=db, data_list=markets)

    print "Time taken to generate DATA LEVEL TRIGGERS = ", time.time() - st_data_level

    TRIGGERS_ALL = TRIGGERS
    df = pd.DataFrame()
    df['Triggers'] = TRIGGERS_ALL
    df.dropna(inplace=True)
    TRIGGERS_ALL = df['Triggers'].values
    print "Total NUMBER of TRIGGERS RAISED = ", len(TRIGGERS_ALL)
    random.shuffle(TRIGGERS_ALL)
    count_analyzed = 1
    triggers_wasted = 0
    for trigger in TRIGGERS_ALL:
        # try:
        if trigger is not None:
            analyze(id=trigger, db=db)
        print "Analyzed TRIGGER NUMBER ", count_analyzed, " out of ", len(TRIGGERS_ALL)
        # except Exception as e:
        #     print traceback.print_exc()
        #     db.JUP_DB_Errors.insert({"err_id": str(trigger),
        #                              "error_name": str(e.__class__.__name__),
        #                              "error_message": str(e.args[0])})
        #     triggers_wasted += 1
        count_analyzed += 1
    print "Number of triggers wasted due to some ERROR In some function = ", triggers_wasted, " out of TOTAL TRIGGERS = ", len(TRIGGERS_ALL)
            # count_batch += BATCH_SIZE
            # run += 1

    # except Exception as error_msg:
    #     print traceback.print_exc()
    #     module_name = ''.join(
    #         ['jupiter_AI/batch/triggers/data_level_triggers/general/daily.py ', 'method: run'])
    #     obj_error = ErrorObject(ErrorObject.ERRORLEVEL1,
    #                             module_name,
    #                             get_arg_lists(inspect.currentframe()))
    #     obj_error.append_to_error_list(str(error_msg))
    #     obj_error.write_error_logs(datetime.datetime.now())
    #     error_flag = 1
    #     end_time = time.time()

    # __file__ gives the path of the module
    # "Nikunj/PycharmProjects/JUP_AI/batch/triggers/data_level_triggers/general/run_booking_triggers.py"

    print "Updating Completed Status"
    db.JUP_DB_Data_Status.update(
        {'_id': ObjectId(object_id)},
        {
            'team': 'analytics',
            'type': 'script',
            'level': 2,
            'name': 'run_booking_triggers.py',
            'title': 'data_level_triggers',
            'stage': 'operations',
            'last_update_date': datetime.datetime.today().strftime('%Y-%m-%d'),
            'last_update_time': datetime.datetime.today().strftime('%H:%M'),
            'start_time': datetime.datetime.today().strftime('%H:%M'),
            'end_time': datetime.datetime.today().strftime('%H:%M'),
            'run_time': time.time() - START_TIME,
            'collections_updated': COLLECTION_UPDATE_LIST,
            'collections_dependencies': COLLECTION_DEPENDENCY_LIST,
            'params_updated': [
                {
                    'collection': '',
                    'fields': []
                }
            ],
            'number_of_params': np.nan,
            'logs': [
                {
                    'number_of_records': np.nan
                }
            ],
            'ip': '172.28.23.8',
            'port': '3354',
            'URL': '',
            'status': 'Completed',
            'error': error_flag
        }
    )


if __name__ == '__main__':
    client = mongo_client()
    db=client[JUPITER_DB]
    markets = [[
        "TYOTYODELY", "TYOTYOBLRY", "TYOTYOMELY", "TYOTYOPUSY", "TYOTYOSYDY"]]

    run(markets=markets, db=db)

    # TEMPORARY BLOCK TO CLEAR WORKFLOW AND WORKFLOW_OD_USER
    # import sys
    # client.close()
    # sys.exit()
    # st = time.time()
    # print "Running Data Level Triggers"
    # online_mrkts = db.JUP_DB_Market_Significance.aggregate([
    #     {"$match": {"online": True}},
    #     {"$sort": {"rank": -1}},
    #     {"$group": {"_id": {"market": "$market"}}},
    #     {"$project": {"_id": 0, "market": "$_id.market"}}])
    #
    # online_mrkts = list(online_mrkts)
    #
    # counter = 0
    # trigger_group = []
    # markets = []
    # for mrkt in online_mrkts:
    #     if counter == 100:
    #         run(db=db, markets=markets)
    #         markets.append(mrkt['market'])
    #         counter = 1
    #     else:
    #         markets.append(mrkt['market'])
    #         counter += 1
    # if counter > 0:
    #     run(db=db, markets=markets)
    # print 'Total Time Taken', time.time() - st
    # client.close()