"""
File Name              : HostAndCompetitorPromos
Author                 : Shamail Mulla
Date Created           : 2017-03-16
Description            : This file generates triggers if competitors have more promotions than the host airline

MODIFICATIONS LOG
    S.No                   : 1
    Date Modified          : 2017-03-20
    By                     : Shamail Mulla
    Modification Details   : Modified logic to include OW/RT as a grouping parameter

"""

import datetime
import inspect
from collections import defaultdict
from copy import deepcopy
import jupiter_AI.common.ClassErrorObject as error_class
from jupiter_AI import SYSTEM_DATE, today, Host_Airline_Code as host, client, JUPITER_DB,JUPITER_LOGGER
from jupiter_AI.tiles.jup_common_functions_tiles import gen_collection_name as gen
from jupiter_AI.triggers.common import get_start_end_dates
from jupiter_AI.triggers.data_level.MainClass import DataLevel
from jupiter_AI.logutils import measure
#db = client[JUPITER_DB]
result_coll_name = gen()


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

class PromosTrigger(object):
    @measure(JUPITER_LOGGER)
    def __init__(self):
        pass

    @measure(JUPITER_LOGGER)
    def generate_trigger(self, pos, origin, destination, compartment):
        """
        Generate the forecast triggers(if any) for the next three months in the OD provided.
        :param origin:
        :param destination:
        :return:
        """
        try:
            data = {
                'pos': pos,
                'origin': origin,
                'destination': destination,
                'compartment': compartment
            }
            #   considering the default booking period Year to Observation Date

            #   Generating the trigger for First Set of departure dates into consideration (Current Month)
            print 'Today:', SYSTEM_DATE
            month = today.month
            year = today.year

            # Obtaining data for the current and next 3 months
            for i in range(0,4):
                print '\nMonth:', month, '\tYear:', year
                dep_date_start, dep_date_end = get_start_end_dates(month, year)
                trigger_obj = PromosChanges(data, SYSTEM_DATE)
                promos = trigger_obj.do_analysis(dep_date_start=dep_date_start, dep_date_end=dep_date_end)

                if month != 12:
                    month += 1
                else:
                    month = 1
                    year += 1

        except Exception as error_msg:
            # raise error_msg
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                'jupiter_AI/triggers/data_level/HostAndCompetitorPromos.py method: generate_trigger',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list(str(error_msg))
            obj_error.write_error_logs(datetime.datetime.now())


class PromosChanges(DataLevel):
    """
    Class Object defining all the functions of generating the trigger
    Logic of the Trigger.
        For a period of departure months current month and next 3 months
    """
    @measure(JUPITER_LOGGER)
    def __init__(self, data, system_date):
        """
        :param data: represents the market in consideration
         {
            'pos':'',
            'origin':'',
            'destination':'',
            'compartment':''
         }
        :param system_date:
        """
        super(PromosChanges, self).__init__(data, system_date)
        self.old_doc_data = data
        self.new_doc_data = data
        self.trigger_date = system_date

    @measure(JUPITER_LOGGER)
    def query_builder(self, dep_date_start, dep_date_end):
        '''
        The function creates a query based on the filter received from the user to be fired on the database
        :param dict_user_filter: filter to retrieve marginal revenue for a particular od and date range
        :return: query dictionary
        '''
        try:
            dict_filter = deepcopy(defaultdict(list, self.old_doc_data))
            query = defaultdict(list)

            # if self.dict_filter['pos']:
            #     query['$and'].append({'pos': {'$in': self.dict_filter['pos']}})

            if self.old_doc_data['origin'] and self.old_doc_data['destination']:
                od_build = []
                od = []
                for idx, item in enumerate(self.data['origin']):
                    od_build.append({'origin': item.encode(), 'destination': self.old_doc_data['destination'][idx].encode()})
                for i in od_build:
                    a = i['origin'] + i['destination']
                    od.append({'od': a})
                query['$and'].append({'$or': od})

            if dict_filter['compartment']:
                print 'compartment:', dict_filter['compartment']
                query['$and'].append({'compartment': {'$in': dict_filter['compartment'].encode()}})
                # query['$and'].append({'compartment': {'$in': dict_filter['compartment']}})

            # query['$and'].append({'startDepDate': {'gte': dep_date_start}})
            # query['$and'].append({'endDepDate': {'lte': dep_date_end}})
            # query['$and'].append({'$and': [{'startDepDate': {'$gte': dep_date_start}}, {'endDepDate': {'$lte': dep_date_end}}]})

            return query
        except:
            pass

    @measure(JUPITER_LOGGER)
    def do_analysis(self, dep_date_start, dep_date_end):
        print dep_date_start, dep_date_end
        query = self.query_builder(dep_date_start, dep_date_end)
        print 'Query: ',query
        if 'JUP_DB_Promos' in db.collection_names():
            print 'JUP_DB_Promos present in the database.'
        ppln_promotions = [
            # {
            #     '$match': query
            # }
            # ,
            {
                '$group':
                    {
                        '_id':
                            {
                                'airline': '$airline',
                                'od': '$od',
                                'compartment': '$compartment',
                                # 'currency': '$currency',
                                'OW/RT': '$type',
                                # 'start_dep_date': '$startDepDate',
                                # 'end_dep_date': '$endDepDate',
                            },
                        'promotions': {'$sum': 1},
                        # 'price': '$price',
                        # 'last_update_time': '$Last_updated_time',
                        # 'last_update_date': '$Last_updated_date',
                        # 'start_dep_date': '$startDepDate',
                        # 'end_dep_date': '$endDepDate',
                        # 'fare_conditions': '$fareConditions',
                        # 'exact_period': '$exact_period'
                    }

            }
            ,
            {
                '$group':
                    {
                        '_id':
                            {
                                'od': '$_id.od',
                                'compartment': '$_id.compartment',
                                # 'currency': '$_id.currency',
                                'OW/RT': '$_id.OW/RT',
                                # 'start_dep_date': '$_id.start_dep_date',
                                # 'end_dep_date': '$_id.end_dep_date',
                            },
                        'promotion_details':
                            {
                                '$push':
                                    {
                                        'airline': '$_id.airline',
                                        'count_promotions': '$promotions',
                                    }
                            }
                    }
            }
            ,
            {
                '$project':
                    {
                        '_id': 1,
                        'od': '$_id.od',
                        'compartment': '$_id.compartment',
                        'currency': '$_id.currency',
                        'OW/RT': '$_id.OW/RT',
                        'promotion_details': '$promotion_details'
                    }
            }
            ,
            {
                '$out': result_coll_name
            }
        ]
        db.JUP_DB_Promos.aggregate(ppln_promotions, allowDiskUse=True)
        crsr_promos = db.get_collection(result_coll_name).find()
        print crsr_promos.count(), 'records'
        lst_promotions = list(crsr_promos)
        db[result_coll_name].drop()

        lst_imp_competitors = []
        lst_market_promos = []

        # Debugging purposes
        # print 'Result collection:'
        # for promo in lst_promotions:
        #     print promo

        for promo in lst_promotions:
            count_host_promotions = 0
            for airline_promos in promo[u'promotion_details']:
                if airline_promos[u'airline'] == host:
                    count_host_promotions = airline_promos[u'count_promotions']
                    del airline_promos

            for airline_promos in promo[u'promotion_details']:
                if count_host_promotions < airline_promos[u'count_promotions']:
                    lst_imp_competitors.append(airline_promos)

            if len(lst_imp_competitors) > 0:
                lst_market_promos.append([{'od': promo[u'od']},
                                         {'compartment': promo[u'compartment']},
                                         {host: count_host_promotions},
                                         {'competitor promos': lst_imp_competitors}])

        for market_promos in lst_market_promos:
            print market_promos

        if len(lst_market_promos) > 0:
            self.get_trigger_details('Promos Comparision')

        # return lst_market_promos


if __name__=='__main__':
    trigger = PromosTrigger()
    trigger.generate_trigger('DMM','DMM','KTM','Y')
    # trigger = PromosChanges()
    # trigger.do_analysis('2017-03-01', '2017-03-31')
