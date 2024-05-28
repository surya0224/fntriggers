"""
File Name              : group_pricing_BLL
Author                 : Shamail Mulla
Date Created           : 2017-01-30
Description            : This file validates data and calls the DAL functions to retrieve data

MODIFICATIONS LOG
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :
"""

from group_pricing_DAL import GroupPricing
from jupiter_AI.tiles.jup_common_functions_tiles import query_month_year_builder
from copy import deepcopy
from collections import defaultdict
import datetime

class GroupBuyingPricing(object):
    min_marginal_rev = -5
    max_marginal_rev = 5

    def validate_data(self, dict_user_filter):
        '''
        If the filter values is valid
        :param dict_user_filter: Dictionary having parameters from the user
        :return: For a valid filter, returns true else false
        '''
        try:
            data_validated = True

            if dict_user_filter is None:
                data_validated = False

            return data_validated
        except:
            pass

    def query_builder(self, dict_user_filter):
        '''
        The function creates a query based on the filter received from the user to be fired on the database
        :param dict_user_filter: filter to retrieve marginal revenue for a particular od and date range
        :return: query dictionary
        '''
        try:
            if self.validate_data(dict_user_filter):
                dict_filter = deepcopy(defaultdict(list, dict_user_filter))
                query = defaultdict(list)

                if dict_filter['region']:
                    query['$and'].append({'region': {'$in': dict_user_filter['region']}})
                if dict_filter['country']:
                    query['$and'].append({'country':{'$in': dict_user_filter['country']}})
                if dict_filter['pos']:
                    query['$and'].append({'pos': {'$in': dict_user_filter['pos']}})
                if dict_filter['origin'] and dict_filter['destination']:
                    od_build = []
                    od = []
                    for idx, item in enumerate(dict_user_filter['origin']):
                        od_build.append({'origin': item, 'destination': dict_user_filter['destination'][idx]})
                    for i in od_build:
                        a = i['origin'] + i['destination']
                        od.append({'od': a})
                    query['$and'].append({'$or': od})
                from_obj = datetime.datetime.strptime(dict_user_filter['fromDate'], '%Y-%m-%d')
                to_obj = datetime.datetime.strptime(dict_user_filter['toDate'], '%Y-%m-%d')
                month_year = query_month_year_builder(from_obj.month, from_obj.year, to_obj.month, to_obj.year)
                query['$and'].append({'$or': month_year})
                # query for travel date
                return query
        except:
            pass

    def get_best_price(self, dict_user_filter, group_size):
        '''
        This function calculates and returns the best fare for a particular group
        :param dict_user_filter: filter paramters given from the user
        :param group_size: number of seats to book
        :return: best_fare for the group book
        '''
        try:
            query = self.query_builder(dict_user_filter)
            # print query
            obj_DAL = GroupPricing()
            fares, rev, pax = obj_DAL.get_revenue_data(query)

            lst_min_fares = []
            lst_min_marginal_rev = []
            # lst_min_rev = []
            marginal_rev = []
            # marginal revenue for the 1st fare is null; avoiding null error by making it 0
            marginal_rev(0)

            # calculating marginal fare from 2nd fare onwards
            for index in range(1, (len(rev))):
                marginal_rev.append((rev[index] - rev[index - 1]))
                # considering the positive marginal fares and revenue closest to 0
                if self.min_marginal_rev <= marginal_rev[index] <= self.max_marginal_rev:
                    lst_min_fares.append(fares[index])
                    lst_min_marginal_rev.append(marginal_rev[index])
                    # lst_min_rev.append(rev[index])

            # Debugging purpose
            # print 'Data:'
            # for index in range(0,len(rev)):
            #     print index, ':', fares[index], ',', pax[index], ',', rev[index], ',', marginal_rev[index]

            # Retrieving fare where marginal revenue is minimum
            min_mr_index = lst_min_marginal_rev.index(min(lst_min_marginal_rev))
            best_fare = lst_min_fares[min_mr_index]

            # Debugging purpose
            # for index in range(0, len(lst_min_marginal_rev)):
            #     print index, ':', lst_min_marginal_rev[index], ',', lst_min_fares[index]
            # print 'best fare',best_fare
            print 'Best Rate:',best_fare
            return best_fare*group_size
        except Exception as error_msg:
            print error_msg

if __name__ == '__main__':
    dict_user_filter = \
        {
            'region': [],
            'country': [],
            'pos': [],
            'origin': ['DXB'],
            'destination': ['DOH'],
            'fromDate': '2017-01-18',
            'toDate': '2017-01-18'
        }
    obj_gp = GroupBuyingPricing()
    start_time = datetime.datetime.now()
    group_size = 20
    group_price = obj_gp.get_best_price(dict_user_filter, group_size)
    end_time = datetime.datetime.now()
    print 'Price for', group_size, 'seats:',group_price
    print 'Time taken:', end_time-start_time
