"""
Author: Abhinav Garg
Created with <3
Date Created: 2017-10-22
Code functionality:
     Class for Competitor Price Change, Competitor New Entry, Competitor Exit Triggers
Modifications log:
    1. Author:
       Exact modification made or some logic changed:
       Date of modification:
    2. Author:
       Exact modification made or some logic changed:
       Date of modification:

"""

from copy import deepcopy

from jupiter_AI.triggers.data_change.MainClass import DataChange
#from jupiter_AI.triggers.common import get_start_end_dates
from jupiter_AI.network_level_params import SYSTEM_DATE, today, INF_DATE_STR, JUPITER_LOGGER
from jupiter_AI.logutils import measure
import datetime


class CompPriceChange(DataChange):
    """
    This Class represents in analysing the trigger before sending it to the priority queue
    """
    @measure(JUPITER_LOGGER)
    def __init__(self, name, old_database_doc, new_database_doc):
        print 'Class Called'
        DataChange.__init__(self, name, old_database_doc, new_database_doc)
        try:
            old_database_doc['pos']
        except Exception:
            old_database_doc['pos'] = old_database_doc['origin']
            new_database_doc['pos'] = new_database_doc['origin']

        if old_database_doc['pos'] == 'NA':
            old_database_doc['pos'] = old_database_doc['origin']
            new_database_doc['pos'] = new_database_doc['origin']
        self.triggering_event = dict(
            collection='JUP_DB_ATPCO_Fares_Rules',
            field='price',
            action='change'
        )
        print 'Built Triggering Event'

    @measure(JUPITER_LOGGER)
    def build_triggering_data(self):
        """
        :return:
        """
        # if not self.old_doc_data['dep_date_from']:
        #     dep_date_start = SYSTEM_DATE
        #     if not self.old_doc_data['dep_date_to']:
        #         dep_date_end_object = today + datetime.timedelta(days=90)
        #         dep_date_end = datetime.datetime.strftime(dep_date_end_object, '%Y-%m-%d')
        #     else:
        #         dep_date_end = self.old_doc_data['dep_date_to']
        # else:
        #     dep_date_start = self.old_doc_data['dep_date_start']
        #     dep_date_end = self.old_doc_data['dep_date_end']
        dep_date_start = self.new_doc_data['dep_from']
        dep_date_end = self.new_doc_data['dep_to']

        self.triggering_data = dict(dep_date_start=dep_date_start,
                                    dep_date_end=dep_date_end)
        print 'Built Triggering Data'

    @measure(JUPITER_LOGGER)
    def check_trigger(self):
        # print self.old_doc_data['total_fare']
        # print self.new_doc_data['total_fare']
        # if type(self.old_doc_data['total_fare']) in [int, float] and type(self.new_doc_data['total_fare']) in [int, float]:
        #     if self.threshold_type == 'percent':
        #         change = (self.new_doc_data['total_fare'] - self.old_doc_data['total_fare']) * 100 / float(
        #             self.old_doc_data['total_fare'])
        #         print change
        #         if not (self.lower_threshold < change < self.upper_threshold):
        #             return True
        #         else:
        #             return False
        #     elif self.threshold_type == 'absolute':
        #         change = self.new_doc_data['total_fare'] - self.old_doc_data['total_fare']
        #         print change
        #         if not (self.lower_threshold < change < self.upper_threshold):
        #             return True
        #         else:
        #             return False
        #     else:
        #         return False
        self.change = self.new_doc_data['pct_chng']
        return True


class CompEntry(DataChange):
    """
    """
    @measure(JUPITER_LOGGER)
    def __init__(self, name, old_database_doc, new_database_doc):
        old_doc = deepcopy(new_database_doc)
        old_doc['carrier'] = None
        old_doc['pos'] = old_doc['origin']
        new_database_doc['pos'] = new_database_doc['origin']
        DataChange.__init__(self, name, old_doc, new_database_doc)
        self.triggering_event = dict(
            collection='JUP_DB_ATPCO_Fares',
            field='farebasis',
            action='insert'
        )

    @measure(JUPITER_LOGGER)
    def build_triggering_data(self):
        """
        :return:
        """
        dep_date_start = SYSTEM_DATE
        dep_date_end = INF_DATE_STR
        self.triggering_data = dict(dep_date_start=dep_date_start,
                                    dep_date_end=dep_date_end,
                                    reason='Fares Included')

    @measure(JUPITER_LOGGER)
    def check_trigger(self):
        return True


class CompExit(DataChange):
    """
    """
    @measure(JUPITER_LOGGER)
    def __init__(self, name, old_database_doc, new_database_doc):
        print old_database_doc
        print new_database_doc
        new_doc = deepcopy(old_database_doc)
        new_doc['carrier'] = None
        old_database_doc['pos'] = old_database_doc['origin']
        new_doc['pos'] = new_doc['origin']
        DataChange.__init__(self, name, old_database_doc, new_doc)
        self.triggering_event = dict(
            collection='JUP_DB_ATPCO_Fares',
            field='farebasis',
            action='insert'
        )

    @measure(JUPITER_LOGGER)
    def build_triggering_data(self):
        """
        :return:
        """
        dep_date_start = SYSTEM_DATE
        dep_date_end = INF_DATE_STR
        self.triggering_data = dict(dep_date_start=dep_date_start,
                                    dep_date_end=dep_date_end,
                                    reason='Fares Removed')

    @measure(JUPITER_LOGGER)
    def check_trigger(self):
        return True


if __name__ == '__main__':
    old_doc = None
    new_doc = dict(
        origin='DXB',
        destination='DOH',
        compartment='Y',
        carrier='G9'
    )
    obj = CompEntry('competitor_new_entry', old_doc, new_doc)
    obj.do_analysis()

    old_doc = dict(
        origin='DXB',
        destination='DOH',
        compartment='Y',
        carrier='G9'
    )
    new_doc = None
    obj = CompExit('competitor_exit', old_doc, new_doc)
    obj.do_analysis()

    old_doc = {
        "effective_from" : "2017-04-27",
        "effective_to" : None,
        "dep_date_from" : None,
        "dep_date_to" : None,
        "sale_date_from" : None,
        "sale_date_to" : None,
        "last_ticketed_date" : None,
        "fare_basis" : "A2RTIB",
        "fare_brand" : None,
        "RBD" : "A",
        "fare_rule" : "IEGE",
        "fare_include" : True,
        "private_fare" : False,
        "tariff_code" : "4",
        "footnote" : "95",
        "rtg" : 113,
        "batch" : "9W50960",
        "Network" : None,
        "Cluster" : None,
        "region" : None,
        "country" : None,
        "pos" : None,
        "origin_country" : "IN",
        "origin_zone" : "330",
        "origin_area" : "3",
        "destination_zone" : "210",
        "destination_area" : "2",
        "destination_country" : "GB",
        "origin" : "ATQ",
        "destination" : "ABZ",
        "OD" : "ATQABZ",
        "compartment" : None,
        "oneway_return" : 2,
        "channel" : "gds",
        "carrier" : "9W",
        "fare" : 349865.0,
        "surcharge" : 0,
        "YQ" : 0.0,
        "YR" : 0,
        "taxes" : 0,
        "currency" : "INR",
        "Rule_id" : "IEGE",
        "RBD_type" : None,
        "Parent_RBD" : None,
        "RBD_hierarchy" : None,
        "derived_filed_fare" : None,
        "combine_pos_od_comp_fb" : None,
        "gfs" : "170426DTV",
        "combine_faretype" : None,
        "last_update_date" : "2017-06-23",
        "last_update_time" : 21,
        "fare_id" : "A2RTIB95",
        "category" : None,
        "total_fare" : 349865.0,
        "competitor_farebasis" : None,
        "dep_date_from_UTC" : None,
        "recommended_fare" : None,
        "surcharge1_date" : None,
        "surcharge2_date" : None,
        "surcharge3_date" : None,
        "surcharge4_date" : None,
        "surcharge1_amount" : 0,
        "surcharge2_amount" : 0,
        "surcharge3_amount" : 0,
        "surcharge4_amount" : 0,
        "surcharge_average" : 0
    }

    new_doc = {
        "effective_from" : "2017-04-27",
        "effective_to" : None,
        "dep_date_from" : None,
        "dep_date_to" : None,
        "sale_date_from" : None,
        "sale_date_to" : None,
        "last_ticketed_date" : None,
        "fare_basis" : "A2RTIB",
        "fare_brand" : None,
        "RBD" : "A",
        "fare_rule" : "IEGE",
        "fare_include" : True,
        "private_fare" : False,
        "tariff_code" : "4",
        "footnote" : "95",
        "rtg" : 113,
        "batch" : "9W50960",
        "Network" : None,
        "Cluster" : None,
        "region" : None,
        "country" : None,
        "pos" : None,
        "origin_country" : "IN",
        "origin_zone" : "330",
        "origin_area" : "3",
        "destination_zone" : "210",
        "destination_area" : "2",
        "destination_country" : "GB",
        "origin" : "ATQ",
        "destination" : "ABZ",
        "OD" : "ATQABZ",
        "compartment" : None,
        "oneway_return" : 2,
        "channel" : "gds",
        "carrier" : "9W",
        "fare" : 249865.0,
        "surcharge" : 0,
        "YQ" : 0.0,
        "YR" : 0,
        "taxes" : 0,
        "currency" : "INR",
        "Rule_id" : "IEGE",
        "RBD_type" : None,
        "Parent_RBD" : None,
        "RBD_hierarchy" : None,
        "derived_filed_fare" : None,
        "combine_pos_od_comp_fb" : None,
        "gfs" : "170426DTV",
        "combine_faretype" : None,
        "last_update_date" : "2017-06-23",
        "last_update_time" : 21,
        "fare_id" : "A2RTIB95",
        "category" : None,
        "total_fare" : 249865.0,
        "competitor_farebasis" : None,
        "dep_date_from_UTC" : None,
        "recommended_fare" : None,
        "surcharge1_date" : None,
        "surcharge2_date" : None,
        "surcharge3_date" : None,
        "surcharge4_date" : None,
        "surcharge1_amount" : 0,
        "surcharge2_amount" : 0,
        "surcharge3_amount" : 0,
        "surcharge4_amount" : 0,
        "surcharge_average" : 0
    }
    obj = CompPriceChange('competitor_price_change', old_doc, new_doc)
    obj.do_analysis()