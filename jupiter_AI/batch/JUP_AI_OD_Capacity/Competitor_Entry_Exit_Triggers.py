"""
File Name              :   Competitor_Entry_Exit_Trigger.py
Author                 :   Shamail Mulla
Date Created           :   2017-04-19
Description            :  Monthly competitor capacities are checked to see if there are any new competitors or if any
                            competitor has existed.

"""

from copy import deepcopy
from collections import defaultdict
import datetime
import time
import inspect

lst_od_capacity = []

try:
    from jupiter_AI import client, JUPITER_DB, Host_Airline_Code

    #db = client[JUPITER_DB]
except:
    pass
from jupiter_AI.common import ClassErrorObject as error_class
from common_functions import get_arg_lists, get_module_name
from jupiter_AI.tiles.jup_common_functions_tiles import gen_collection_name as gen
from jupiter_AI.triggers.data_change.capacity.competitor import CompExit, CompEntry


def get_last_update_dates():
    try:
        if 'JUP_DB_OD_Capacity' in db.collection_names():
            crsr_update_dates = db.JUP_DB_OD_Capacity.distinct('last_update_date')
            lst_update_dates = list(crsr_update_dates)
            # print lst_update_dates
            if len(lst_update_dates) > 1:
                lst_update_dates = sorted(lst_update_dates, reverse=True)
                current_date = lst_update_dates[0]
                last_date = lst_update_dates[1]
                return current_date, last_date
            elif len(crsr_update_dates) == 1:
                print 'Capacities not updated'
            else:
                print 'No documents exist'
        else:
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                                'jupter_AI/batch/JUP_AI_OD_Capacity/Competitor_Entry_Exit_Trigger.py method: get_last_update_dates',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list('JUP_DB_OD_Capacity collection to query not present')
            obj_error.write_error_logs(datetime.datetime.now())
    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/batch/JUP_AI_OD_Capacity/Competitor_Entry_Exit_Trigger.py method: get_last_update_dates',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


def query_builder_competitors(current_date, last_date):
    query = defaultdict(list)
    query['$and'].append({'$or': [{'last_update_date': current_date}, {'last_update_date': last_date}]})
    query['$and'].append({'airline': {'$ne': Host_Airline_Code}})
    return query


def get_competitors(current_date, last_date):
    coll_name = gen()
    try:
        query = query_builder_competitors(current_date, last_date)

        ppln_competitors = [
            {
                '$match': query
            }
            ,
            {
                '$sort': {'last_update_date': -1}
            }
            ,
            {
                '$group':
                    {
                        '_id':
                            {
                                'od':'$od',
                                'compartment':'$compartment',
                                'month': '$month',
                                'year': '$year',
                                'last_update_date': '$last_update_date'
                            },
                        'competitors':
                            {
                                '$push':
                                    {
                                        'airline': '$airline'
                                    }
                            }
                    }
            }
            ,
            {
                '$group':{
                    '_id':{
                        'od':'$_id.od',
                        'compartment':'$_id.compartment',
                        'month':'$_id.month',
                        'year':'$_id.year'
                    },
                    'competitors': {
                        '$push': {
                            'update_date': '$_id.last_update_date',
                            'competitors': '$competitors'
                        }
                    }
                }
            }
            ,
            {
                '$project':
                    {
                        'od':'$_id.od',
                        'compartment':'$_id.compartment',
                        'month': '$_id.month',
                        'year': '$_id.year',
                        'competitors':'$competitors'
                    }
            }
            ,
            {
                '$out': coll_name
            }
        ]
        db.JUP_DB_OD_Capacity.aggregate(ppln_competitors, allowDiskUse=True)
        crsr_competitors = db.get_collection(coll_name).find(projection={'_id': 0})
        lst_competitors = list(crsr_competitors)
        crsr_competitors.drop()
        return lst_competitors

    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/batch/JUP_AI_OD_Capacity/Competitor_Entry_Exit_Trigger.py method: get_competitors',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


def compare_competitors():
    try:
        # Step 1: Retrieving the last update dates for each airline
        current_date, last_date = get_last_update_dates()

        # Step 2: Retrieving list of competitors for each
        lst_competitors = get_competitors(current_date, last_date)

        lst_current_update_competitors = []
        lst_last_update_competitors = []

        lst_competitor_entry = []
        lst_competitor_exit = []

        # print 'Competitor docs:',len(lst_competitors),'docs'
        for doc in lst_competitors:

            od = doc['od']
            compartment = doc['compartment']
            month = doc['month']
            year = doc['year']

            current_comp = set()
            last_comp = set()

            for comp_data in doc['competitors']:
                if comp_data['update_date'] == current_date:
                    current_comp = set(comp_data['competitors'])
                elif comp_data['update_date'] == last_date:
                    last_comp = set(comp_data['competitors'])

            entry = current_comp - last_comp
            exit = last_comp - current_comp

            for comp in entry:
                name = 'competitor_entry'
                old_doc = None
                new_doc= dict(
                    origin=od[:3],
                    destination=od[3:],
                    compartment=compartment,
                    month=month,
                    year=year,
                    airline=comp
                )
                obj = CompEntry(name=name,
                                old_database_doc=old_doc,
                                new_database_doc=new_doc)
                obj.do_analysis()

            for comp in exit:
                name = 'competitor_entry'
                new_doc = None
                old_doc = dict(
                    origin=od[:3],
                    destination=od[3:],
                    compartment=compartment,
                    month=month,
                    year=year,
                    airline=comp
                )
                obj = CompExit(name=name,
                               old_database_doc=old_doc,
                               new_database_doc=new_doc)
                obj.do_analysis()

    except Exception as error_msg:
        obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                            'jupter_AI/batch/JUP_AI_OD_Capacity/Competitor_Entry_Exit_Trigger.py method: compare_competitors',
                                            get_arg_lists(inspect.currentframe()))
        obj_error.append_to_error_list(str(error_msg))
        obj_error.write_error_logs(datetime.datetime.now())


if __name__ == '__main__':
    compare_competitors()