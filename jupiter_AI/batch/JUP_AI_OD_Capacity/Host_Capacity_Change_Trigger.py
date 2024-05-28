"""
File Name              :   JUP_AI_Host_Capacity_Change_Trigger.py
Author                 :   Shamail Mulla
Date Created           :   2017-04-18
Description            :  This code checks if the host has changed their capacity since the last update date.

"""

from copy import deepcopy
from collections import defaultdict
import datetime
import time
import inspect

lst_od_capacity = []

try:
    from jupiter_AI import client, JUPITER_DB, JUPITER_LOGGER
    from jupiter_AI.logutils import measure
    #db = client[JUPITER_DB]
except:
    pass
from jupiter_AI import Host_Airline_Code as host
from jupiter_AI.common import ClassErrorObject as error_class
from common_functions import get_arg_lists, get_module_name, get_quarter, get_str
from jupiter_AI.tiles.jup_common_functions_tiles import gen_collection_name as gen


@measure(JUPITER_LOGGER)
def get_last_update_dates():
    try:
        if 'JUP_DB_Host_OD_Inventory' in db.collection_names():
            crsr_update_dates = db.JUP_DB_Host_OD_Inventory.distinct('last_update_date')
            lst_update_dates = list(crsr_update_dates)
            # print lst_update_dates
            if len(lst_update_dates) > 1:
                lst_update_dates = sorted(lst_update_dates, reverse=True)
                current_date = lst_update_dates[0]
                last_date = lst_update_dates[1]
                return current_date, last_date
            elif len(crsr_update_dates) == 1:
                lst_update_dates = list(crsr_update_dates)
                print 'Capacities not updated'
            else:
                print 'No documents exist'
        else:
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                                'jupter_AI/batch/JUP_AI_OD_Capacity/Host_Capacity_Change_Trigger.py method: get_last_update_dates',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('JUP_DB_OD_Capacity collection to query not present')
            obj_error.write_error_logs(datetime.datetime.now())
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/batch/JUP_AI_OD_Capacity/Host_Capacity_Change_Trigger.py method: get_last_update_dates',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


@measure(JUPITER_LOGGER)
def query_builder_od_capa(current_date, last_date):
    try:
        query = defaultdict(list)
        query['$and'].append({'$or': [{'last_update_date': current_date}, {'last_update_date': last_date}]})
        return query
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/batch/JUP_AI_OD_Capacity/Host_Capacity_Change_Trigger.py method: query_builder_od_capa',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


@measure(JUPITER_LOGGER)
def get_od_capacity_docs(current_date, last_date):
    try:
        coll_name = gen()
        if 'JUP_DB_Host_OD_Capacity' in db.collection_names():
            query = query_builder_od_capa(current_date, last_date)
            # print query

            ppln_capa_change = [
                {
                    '$match': query
                }
                ,
                {
                    '$group':
                        {
                            '_id':
                                {
                                    'od': '$od',
                                    'dep_date': '$dep_date'
                                },
                            'capa':
                                {
                                    '$push':
                                        {
                                            'j_cap': '$j_cap',
                                            'y_cap': '$y_cap',
                                            'last_update_date': '$last_update_date',
                                            'od_capacity': '$od_capacity'
                                        }
                                }
                        }
                }
                ,
                {
                    '$sort': {'capa.last_update_date': -1}
                }
                ,
                {
                    '$project':
                        {
                            'airline_capa': '$capa',
                            'od': '$_id.od',
                            'dep_date': '$_id.dep_date'
                        }
                }
                ,
                {
                    '$out': coll_name
                }
            ]
            db.JUP_DB_Host_OD_Capacity.aggregate(ppln_capa_change, allowDiskUse=True)
            crsr_capacities = db.get_collection(coll_name).find(projection={'_id': 0})
            lst_comp_capacity = list(crsr_capacities)
            db[coll_name].drop()
            return lst_comp_capacity
        else:
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                                'jupter_AI/batch/JUP_AI_OD_Capacity/Host_Capacity_Change_Trigger.py method: get_od_capacity_docs',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('JUP_DB_Host_OD_Capacity collection to query not present')
            obj_error.write_error_logs(datetime.datetime.now())
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/batch/JUP_AI_OD_Capacity/Host_Capacity_Change_Trigger.py method: get_od_capacity_docs',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


@measure(JUPITER_LOGGER)
def compare_capacities():
    from jupiter_AI.triggers.data_change.capacity.host import HostCapacityChange
    try:
        # Retrieving the last update dates for host
        current_date, last_date = get_last_update_dates()

        lst_host_capacity = get_od_capacity_docs(current_date, last_date)
        print 'Host ODs',len(lst_host_capacity)

        for capacity in lst_host_capacity:
            if len(capacity[u'airline_capa']) >= 2:
                # print '2 docs found'
                # old_capacity = capacity[u'airline_capa'][0][u'od_capacity']
                old_j_capacity = capacity[u'airline_capa'][0][u'j_cap']
                old_y_capacity = capacity[u'airline_capa'][0][u'y_cap']
                # new_capacity = capacity[u'airline_capa'][1][u'od_capacity']
                new_j_capacity = capacity[u'airline_capa'][1][u'j_cap']
                new_y_capacity = capacity[u'airline_capa'][1][u'y_cap']

                del capacity[u'airline_capa']

                if old_j_capacity != new_j_capacity:
                    print capacity
                    print host, 'J capacity for', current_date, ':', new_j_capacity, '\tcapacity for', last_date, ':', old_j_capacity
                    dict_old_doc = dict(
                        origin=capacity[u'od'][:3].encode(),
                        destination=capacity[u'od'][3:].encode(),
                        dep_date=capacity[u'dep_date'],
                        capacity=old_j_capacity,
                        compartment='J'
                    )
                    dict_new_doc = deepcopy(dict_old_doc)
                    dict_new_doc['capacity'] = new_j_capacity
                    trigger_name = 'host_airline_capacity_percentage_change'

                    # print trigger_name, 'compartment: J'

                    obj = HostCapacityChange(name=trigger_name, old_database_doc=dict_old_doc, new_database_doc=dict_new_doc)
                    obj.do_analysis()

                if old_y_capacity != new_y_capacity:
                    print capacity
                    print host, 'Y capacity for', current_date, ':', new_y_capacity, '\tcapacity for', last_date, ':', old_y_capacity
                    dict_old_doc = dict(
                        origin=capacity[u'od'][:3].encode(),
                        destination=capacity[u'od'][3:].encode(),
                        dep_date=capacity[u'dep_date'],
                        capacity=old_y_capacity,
                        compartment='Y'
                    )
                    dict_new_doc = deepcopy(dict_old_doc)
                    dict_new_doc['capacity'] = new_y_capacity
                    trigger_name = 'host_airline_capacity_percentage_change'

                    print trigger_name, 'compartment: Y'

                    obj = HostCapacityChange(name=trigger_name, old_database_doc=dict_old_doc, new_database_doc=dict_new_doc)
                    obj.do_analysis()

    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/batch/JUP_AI_OD_Capacity/Host_Capacity_Change_Trigger.py method: compare_capacities',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


if __name__ == '__main__':
    # lst = get_od_capacity_docs('2017-04-18', '2017-04-19')
    # for doc in lst:
    #     print doc
    compare_capacities()