"""
File Name              :   JUP_AI_Comp_Capacity_Change_Trigger.py
Author                 :   Shamail Mulla
Date Created           :   2017-04-18
Description            :  This code checks if the competitor has changed their capacity since the last update date.

"""

from copy import deepcopy
from collections import defaultdict
import datetime
import time
import inspect

lst_od_capacity = []

try:
    from jupiter_AI import client, JUPITER_DB
    #db = client[JUPITER_DB]
except:
    pass
from jupiter_AI.common import ClassErrorObject as error_class
from common_functions import get_arg_lists, get_module_name, get_quarter, get_str
from jupiter_AI.tiles.jup_common_functions_tiles import gen_collection_name as gen


def get_all_comp():
    try:
        if 'JUP_DB_OD_Capacity' in db.collection_names():
            crsr_airline = db.JUP_DB_OD_Capacity.distinct('airline')
            lst_comp = list(crsr_airline)

            if len(lst_comp) > 0:
                lst_competitors = []
                for comp in lst_comp:
                    lst_competitors.append(comp)

                return lst_competitors
        else:
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                                'jupter_AI/batch/JUP_AI_OD_Capacity/Competitor_Capacity_Change_Trigger.py method: get_all_comp',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('JUP_DB_OD_Capacity collection to query not present')
            obj_error.write_error_logs(datetime.datetime.now())
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/batch/JUP_AI_OD_Capacity/Competitor_Capacity_Change_Trigger.py method: get_all_comp',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


def get_last_update_dates(competitor):
    try:
        if 'JUP_DB_OD_Capacity' in db.collection_names():
            crsr_update_dates = db.JUP_DB_OD_Capacity.distinct('last_update_date', {'airline': competitor})
            lst_update_dates = list(crsr_update_dates)
            # print lst_update_dates
            if len(lst_update_dates) > 1:
                # print 'Capacity updated'
                lst_update_dates = sorted(lst_update_dates, reverse=True)
                current_date = lst_update_dates[0]
                last_date = lst_update_dates[1]
                return current_date, last_date
            else:
                # print 'Capacity not updated'
                return 0,0
        else:
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                                'jupter_AI/batch/JUP_AI_OD_Capacity/Competitor_Capacity_Change_Trigger.py method: get_last_update_dates',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('JUP_DB_OD_Capacity collection to query not present')
            obj_error.write_error_logs(datetime.datetime.now())
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/batch/JUP_AI_OD_Capacity/Competitor_Capacity_Change_Trigger.py method: get_last_update_dates',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


def query_builder_od_capa(current_date, last_date, airline):
    try:
        query = defaultdict(list)
        query['$and'].append({'$or': [{'last_update_date': current_date}, {'last_update_date': last_date}]})
        query['$and'].append({'$or': [{'airline': airline}]})
        return query
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/batch/JUP_AI_OD_Capacity/Competitor_Capacity_Change_Trigger.py method: query_builder_od_capa',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


def get_od_capacity_docs(current_date, last_date, airline):
    try:
        coll_name = gen()
        if 'JUP_DB_OD_Capacity' in db.collection_names():
            query = query_builder_od_capa(current_date, last_date, airline)
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
                                    'month': '$month',
                                    'year': '$year',
                                    'compartment': '$compartment'
                                },
                            'capa':
                                {
                                    '$push':
                                        {

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
                            'month': '$_id.month',
                            'year': '$_id.year',
                            'compartment': '$_id.compartment'
                        }
                }
                ,
                {
                    '$out': coll_name
                }
            ]
            db.JUP_DB_OD_Capacity.aggregate(ppln_capa_change, allowDiskUse=True)
            crsr_capacities = db.get_collection(coll_name).find(projection={'_id': 0})
            lst_comp_capacity = list(crsr_capacities)
            crsr_capacities.drop()
                # airline_capa_details.append(lst_comp_capacity[0])
            return lst_comp_capacity
        else:
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                                'jupter_AI/batch/JUP_AI_OD_Capacity/Competitor_Capacity_Change_Trigger.py method: get_od_capacity_docs',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('JUP_DB_OD_Capacity collection to query not present')
            obj_error.write_error_logs(datetime.datetime.now())
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/batch/JUP_AI_OD_Capacity/Competitor_Capacity_Change_Trigger.py method: get_od_capacity_docs',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


def compare_capacities():
    from jupiter_AI.triggers.data_change.capacity.competitor import CompCapacityChange
    try:
        # Step 1: Retrieve all competitors
        lst_competitors = get_all_comp()

        for comp in lst_competitors:
            # Retrieving the last update dates for each airline
            current_date, last_date = get_last_update_dates(comp)

            if current_date and last_date:
                lst_comp_capa = get_od_capacity_docs(current_date, last_date, comp)

                lst_changed_capa = []

                for capacity in lst_comp_capa:
                    if len(capacity[u'airline_capa']) >= 2:
                        old_capacity = capacity[u'airline_capa'][0][u'od_capacity']
                        new_capacity = capacity[u'airline_capa'][1][u'od_capacity']

                        del capacity[u'airline_capa']

                        if old_capacity != new_capacity:
                            # print comp, 'capacity for', current_date, ':', new_capacity, '\tcapacity for', last_date, ':', old_capacity
                            dict_old_doc = dict(
                                airline=comp.encode(),
                                origin=capacity[u'od'][:3].encode(),
                                destination=capacity[u'od'][3:].encode(),
                                month=capacity[u'month'],
                                year=capacity[u'year'],
                                capacity=old_capacity,
                                compartment=capacity[u'compartment']
                            )
                            dict_new_doc = deepcopy(dict_old_doc)
                            dict_new_doc['capacity'] = new_capacity
                            print 'New doc', new_capacity
                            print dict_new_doc
                            print 'Old doc', old_capacity
                            print dict_old_doc
                            trigger_name = 'competitor_airline_capacity_percentage_change'

                            obj = CompCapacityChange(name=trigger_name, old_database_doc=dict_old_doc, new_database_doc=dict_new_doc)
                            obj.do_analysis()

    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/batch/JUP_AI_OD_Capacity/Competitor_Capacity_Change_Trigger.py method: compare_capacities',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


if __name__ == '__main__':
    # lst = get_od_capacity_docs('2017-04-18', '2017-04-14', 'EK')
    # for doc in lst:
    #     print doc
    compare_capacities()