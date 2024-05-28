import inspect
import datetime
import time
from jupiter_AI.triggers.data_level.Opportunities import generate_ms_vs_fms_opportunities_trigger
from jupiter_AI.common.ClassErrorObject import ErrorObject
from jupiter_AI.triggers.data_level.MainClass import get_pos_od_compartment_combinations
from jupiter_AI import mongo_client, SYSTEM_DATE, JUPITER_LOGGER
from jupiter_AI.logutils import measure
from jupiter_AI.network_level_params import JUPITER_DB

#db = client[JUPITER_DB]


@measure(JUPITER_LOGGER)
def get_module_name():
    return inspect.stack()[1][3]


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

start_date = SYSTEM_DATE
start_time = time.time()
error_flag = 0


@measure(JUPITER_LOGGER)
def run(db):
    try:
        st = time.time()
        pos_od_combinations = get_pos_od_compartment_combinations(db=db)
        print 'No of POS_OD_Comp_Combinations - ', len(pos_od_combinations)

        print time.time() - st
        if pos_od_combinations:
            for combination in pos_od_combinations:
                pos = combination['pos']
                origin = combination['origin']
                destination = combination['destination']
                compartment = combination['compartment']

                generate_ms_vs_fms_opportunities_trigger(pos=pos,
                                                         origin=origin,
                                                         destination=destination,
                                                         compartment=compartment,
                                                         db=db)
                print "DONE - ", pos, '/', origin, '/', destination, '/', compartment

            end_time = time.time()

    except Exception as error_msg:
        print "Some error"
        module_name = ''.join(['jupiter_AI/batch/triggers/data_level_triggers/general/daily.py ',
                               'method: run'])
        obj_error = ErrorObject(ErrorObject.ERRORLEVEL1,
                                module_name,
                                get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())
        error_flag = 1
        end_time = time.time()

    db.JUP_DB_Batch_Run_Status.insert_one({"module_name": __file__,
                                           "start_date": start_date,
                                           "start_time": start_time,
                                           "end_time": end_time,
                                           "end_date": datetime.datetime.strftime(datetime.datetime.today(),
                                                                                  "%Y-%m-%d"),
                                           "running_time": end_time - start_time,
                                           "error_flag": error_flag})

if __name__ == '__main__':
    st = time.time()
    client=mongo_client()
    db=client[JUPITER_DB]
    run(db=db)
    client.close()

    # crsr = db.JUP_DB_Workflow.find({})
    # for i in crsr:
    #     print i['pos'], i['origin'], i['destination'], i['compartment']
    #     generate_ms_vs_fms_opportunities_trigger(i['pos'], i['origin'], i['destination'], i['compartment'])