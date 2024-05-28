"""
Daily Data Level Check Batch
"""
import datetime
import inspect
import traceback
import time
from bson import ObjectId
from jupiter_AI import client, JUPITER_DB, SYSTEM_DATE, JUPITER_LOGGER
from jupiter_AI.logutils import measure
from jupiter_AI.triggers.data_change.price.CompPriceChange import raise_price_change_triggers
from jupiter_AI.triggers.data_level.Opportunities import raise_opportunities_trigger

db = client[JUPITER_DB]
import pandas as pd
from jupiter_AI.triggers.data_level.Trend_Trigger import main as generate_trend_triggers
from jupiter_AI.batch.JUP_AI_Batch_Market_Characteristics1 import get_pos_od_compartment_combinations as get_pos_od_compartment_combinations_ms_collection
from jupiter_AI.triggers.listener_data_level_trigger import analyze
from jupiter_AI.common.ClassErrorObject import ErrorObject
import numpy as np
import random

COLLECTION_UPDATE_LIST = ['JUP_DB_Workflow', 'JUP_DB_Workflow_OD_User']
COLLECTION_DEPENDENCY_LIST = ['JUP_DB_Manual_Triggers_Module']
START_TIME = time.time()
TRIGGERS = []


@measure(JUPITER_LOGGER)
def update_running_status():
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


start_date = SYSTEM_DATE
start_time = time.time()
error_flag = 0


@measure(JUPITER_LOGGER)
def run(markets):
    object_id = update_running_status()
    global TRIGGERS
    error_flag = 0
    # try:
    # pos_od_combinations = get_pos_od_compartment_combinations()
    #  This query hits JUP_DB_POS_OD_Mapping collection which is just a permutation
    # of all the POS_OD_compartments theoretically possible
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
    # print "Time taken to segregate Markets into POS == Origin and POS != Origin = ", time.time() - st_segregate_markets
    # #
    # print 'No of pos/od/compartment combinations', len(pos_od_combinations)
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
    st_pccp = time.time()
    print "Generating DATA LEVEL TRIGGERS . . . . ."
    pccp_triggers = raise_price_change_triggers()
    st_trend = time.time()
    trend_triggers = generate_trend_triggers(markets=markets)
    st_opp = time.time()
    opp_triggers = []
    opp_triggers = raise_opportunities_trigger(markets=markets)
    print "Time taken to generate PCCP TRIGGERS = ", time.time() - st_pccp
    print "Time taken to generate TREND TRIGGERS = ", time.time() - st_trend
    print "Time taken to generate Opportunites TRIGGERS = ", time.time() - st_opp

    TRIGGERS_ALL = pccp_triggers + trend_triggers + opp_triggers
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
        analyze(trigger)
        print "Analyzed TRIGGER NUMBER ", count_analyzed, " out of ", len(TRIGGERS_ALL)
        # except Exception as e:
        #     print traceback.print_exc()
        #     db.JUP_DB_Errors.insert({"err_id": str(trigger),
        #                              "error_name": str(e.__class__.__name__),
        #                              "error_message": str(e.args[0])})
        #     triggers_wasted += 1
        count_analyzed += 1
    print "Number of triggers wasted due to some ERROR In some function = ", triggers_wasted, " out of TOTAL TRIGGERS = ", len(TRIGGERS_ALL)

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

    # TEMPORARY BLOCK TO CLEAR WORKFLOW AND WORKFLOW_OD_USER
    st = time.time()
    print "Running Data Level Triggers"
    online_mrkts = db.JUP_DB_Market_Significance.aggregate([
        {"$match": {"online": True}},
        {"$sort": {"rank": -1}},
        {"$group": {"_id": {"market": "$market"}}},
        {"$project": {"_id": 0, "market": "$_id.market"}}])

    online_mrkts = list(online_mrkts)

    counter = 0
    trigger_group = []
    markets = []
    for mrkt in online_mrkts:
        if counter == 100:
            run(markets)
            markets.append(mrkt)
            counter = 1
        else:
            markets.append(mrkt)
            counter += 1
    if counter > 0:
        run(markets)
    print 'Total Time Taken', time.time() - st
