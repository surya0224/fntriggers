"""
Daily Data Level Check Batch
"""
import datetime
import inspect
import traceback
import time
import numpy as np
import random
import pandas as pd
from jupiter_AI import mongo_client, JUPITER_DB, SYSTEM_DATE, JUPITER_LOGGER
from jupiter_AI.triggers.data_level.PaxChangesWeekly import generate_pax_changes_weekly_triggers
from jupiter_AI.triggers.data_level.PaxChangesRolling import generate_pax_changes_rolling_triggers
from jupiter_AI.triggers.data_level.PaxChangesVLYR import generate_pax_changes_vlyr_triggers
from jupiter_AI.triggers.data_level.PaxChangesVTGT import generate_pax_changes_vtgt_triggers
#from jupiter_AI.batch.JUP_AI_Batch_Market_Characteristics1 import get_pos_od_compartment_combinations as get_pos_od_compartment_combinations_ms_collection
from jupiter_AI.triggers.listener_data_level_trigger import analyze
from jupiter_AI.common.ClassErrorObject import ErrorObject
from bson import ObjectId
from jupiter_AI.logutils import measure
#db = client[JUPITER_DB]
COLLECTION_UPDATE_LIST = ['JUP_DB_Workflow', 'JUP_DB_Workflow_OD_User']
COLLECTION_DEPENDENCY_LIST = ['JUP_DB_Manual_Triggers_Module']
START_TIME = time.time()
TRIGGERS = []


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


start_date = SYSTEM_DATE
start_time = time.time()
error_flag = 0


@measure(JUPITER_LOGGER)
def generate_pax_triggers(pos, origin, destination, compartment, db, data_list, sig_flag=None):
    global TRIGGERS
    try:
        print "PXWK"
        pxwk_triggers = generate_pax_changes_weekly_triggers(pos=pos,
                                                             origin=origin,
                                                             destination=destination,
                                                             compartment=compartment,
                                                             db=db,
                                                             data_list=data_list,
                                                             sig_flag=sig_flag)
        TRIGGERS = TRIGGERS + pxwk_triggers
    except Exception as error_msg:

        module_name = ''.join(
            [
                'jupiter_AI/batch/triggers/data_level_triggers/general/daily.py ',
                'method: run:pax_changes_weekly::',
                pos,
                '/',
                origin,
                '/',
                destination,
                '/',
                compartment])
        obj_error = ErrorObject(ErrorObject.ERRORLEVEL1,
                                module_name,
                                get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())

    try:
        print "PXRT"
        pxrt_triggers = generate_pax_changes_rolling_triggers(pos=pos,
                                                              origin=origin,
                                                              destination=destination,
                                                              compartment=compartment,
                                                              data_list=data_list,
                                                              db=db,
                                                              sig_flag=sig_flag)
        TRIGGERS = TRIGGERS + pxrt_triggers
    except Exception as error_msg:

        module_name = ''.join(
            [
                'jupiter_AI/batch/triggers/data_level_triggers/general/daily.py ',
                'method: run:pax_changes_rolling::',
                pos,
                '/',
                origin,
                '/',
                destination,
                '/',
                compartment])
        obj_error = ErrorObject(ErrorObject.ERRORLEVEL1,
                                module_name,
                                get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())

    try:
        print "PXVLYR"
        pxvlyr_triggers = generate_pax_changes_vlyr_triggers(pos=pos,
                                                             origin=origin,
                                                             destination=destination,
                                                             compartment=compartment,
                                                             data_list=data_list,
                                                             db=db,
                                                             sig_flag=sig_flag)
        TRIGGERS = TRIGGERS + pxvlyr_triggers
    except Exception as error_msg:

        module_name = ''.join(
            [
                'jupiter_AI/batch/triggers/data_level_triggers/general/daily.py ',
                'method: run:pax_changes_VLYR::',
                pos,
                '/',
                origin,
                '/',
                destination,
                '/',
                compartment])
        obj_error = ErrorObject(ErrorObject.ERRORLEVEL1,
                                module_name,
                                get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())

    try:
        print "PXVTGT"
        pxvtgt_triggers = generate_pax_changes_vtgt_triggers(pos=pos,
                                                             origin=origin,
                                                             destination=destination,
                                                             compartment=compartment,
                                                             db=db,
                                                             data_list=data_list,
                                                             sig_flag=sig_flag)
        TRIGGERS = TRIGGERS + pxvtgt_triggers
    except Exception as error_msg:
        module_name = ''.join(
            [
                'jupiter_AI/batch/triggers/data_level_triggers/general/daily.py ',
                'method: run:pax_changes_VTGT::',
                pos,
                '/',
                origin,
                '/',
                destination,
                '/',
                compartment])
        obj_error = ErrorObject(ErrorObject.ERRORLEVEL1,
                                module_name,
                                get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


@measure(JUPITER_LOGGER)
def run_data_level_triggers(pos, origin, destination, compartment, db, data_list, sig_flag=None):
    """
    :param pos:
    :param origin:
    :param destination:
    :param compartment:
    :return:
    """
    st_pax = time.time()
    print "Generating Pax Triggers . . . . . . ."
    generate_pax_triggers(pos=pos,
                          origin=origin,
                          destination=destination,
                          compartment=compartment,
                          db=db,
                          data_list=data_list,
                          sig_flag=sig_flag)

    print "Time taken to generate Pax Triggers = ", time.time() - st_pax


@measure(JUPITER_LOGGER)
def run(db, markets, sig_flag=None):
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
    # while count_batch < total_markets:
    #     temp_markets = markets[count_batch: count_batch + BATCH_SIZE]
    #     if run == 1:
    #         temp_markets = temp_markets_25 + temp_markets
    run_data_level_triggers(
        pos=None,
        origin=None,
        destination=None,
        compartment=None,
        db=db,
        data_list=markets,
        sig_flag=sig_flag)

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

    run(markets=[
        "DELDELBOGJ",
        "DELDELBOGY",
        "DELDELCHIJ",
        "DELDELCHIY",
        "DELDELEWRJ",
        "DELDELEWRY",
        "DELDELJEDJ",
        "DELDELJEDY",
        "DELDELJFKJ",
        "DELDELJFKY",
        "DELDELJKTJ",
        "DELDELJKTY",
        "DELDELKULJ",
        "DELDELKULY",
        "DELDELMNLJ",
        "DELDELMNLY",
        "DELDELSALJ",
        "DELDELSALY",
        "DELDELSFOJ",
        "DELDELSFOY",
        "DELDELTPEJ",
        "DELDELTPEY",
        "DELDELTYOJ",
        "DELDELTYOY",
        "DELDELWASJ",
        "DELDELWASY",
        "DELDELYHZJ",
        "DELDELYHZY",
        "DELDELYMQJ",
        "DELDELYMQY",
        "DELDELYOWJ",
        "DELDELYOWY",
        "DELDELYTOJ",
        "DELDELYTOY",
        "DELDELYVRJ",
        "DELDELYVRY",
        "DELDELYYCJ",
        "DELDELYYCY",
        "DELDELYYZJ",
        "DELDELYYZY"


    ], db=db, sig_flag='sig')
    import sys
    client.close()
    sys.exit()
    # TEMPORARY BLOCK TO CLEAR WORKFLOW AND WORKFLOW_OD_USER
