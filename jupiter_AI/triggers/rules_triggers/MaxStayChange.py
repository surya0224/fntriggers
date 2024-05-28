"""
Author: Abhinav Garg
Created with <3
Date Created: 2018-09-05
Code functionality:
     Class for Competitor Rule Change, Competitor New Rule Addition, Competitor Rule Cancellation
Modifications log:
    1. Author:
       Exact modification made or some logic changed:
       Date of modification:
    2. Author:
       Exact modification made or some logic changed:
       Date of modification:

"""

import pandas as pd
from jupiter_AI.triggers.rules_triggers.CompRulesParams import get_new_rules_df, get_old_rules_df, get_host_rules
from jupiter_AI.triggers.rules_triggers.MainClass import RuleChange
from jupiter_AI.network_level_params import SYSTEM_DATE, JUPITER_LOGGER, JUPITER_DB
from jupiter_AI import mongo_client
from jupiter_AI.logutils import measure
import datetime

from jupiter_AI.triggers.rules_triggers.analyze_rule_trigger import analyze


class MaxStay(RuleChange):
    """
    This Class represents in analysing the trigger before sending it to the priority queue
    """
    @measure(JUPITER_LOGGER)
    def __init__(self, name, old_database_doc, new_database_doc):
        print 'Class Called'
        RuleChange.__init__(self, name, old_database_doc, new_database_doc)
        self.triggering_event = dict(
            collection='JUP_DB_ATPCO_Fares_Rules',
            field='cat_7',
            action='change'
        )
        print 'Built Triggering Event'

    @measure(JUPITER_LOGGER)
    def check_trigger(self):
        # self.change = self.new_doc_data['pct_chng']
        # change this later when thresholds are defined
        return True


@measure(JUPITER_LOGGER)
def main():
    client = mongo_client()
    new_df = get_new_rules_df(cat='007', record_3='JUP_DB_ATPCO_Record_3_Cat_07',
                              fields={'MAX_STAY': 1, 'UNIT': 1}, client=client)
    if new_df is not None:
        old_df = get_old_rules_df(new_df=new_df, record_3='JUP_DB_ATPCO_Record_3_Cat_07',
                                  fields={'MAX_STAY': 1, 'UNIT': 1}, client=client)

        df = new_df.merge(old_df, on=['CXR_CODE', 'RULE_NO', 'TARIFF', 'CAT_NO', 'SEQ_NO'], how="outer",
                          suffixes=("_new", "_old"))

        # print df[df['ACTION_new'] == '3'].head()
        df = get_host_rules(df=df, cat='cat_7', client=client)
        print df[df['ACTION_new'] == '3']

        for idx, row in df[df['ACTION_new'] == '3'].iterrows():
            for rule in row['host_rules']:
                old_doc = {'fare_rule': rule, 'tariff_code': row['TARIFF'], 'cat_no': row['CAT_NO'],
                           'max_stay': row['MAX_STAY_old'], 'unit': row['UNIT_old'], 'comp_fare_rule': row['RULE_NO'],
                           'comp_seq': row['SEQ_NO'], 'eff_from': row['DATES_EFF_1_old'],
                           'eff_to': row['DATES_DISC_1_old'], 'competitor': row['CXR_CODE'],
                           'record_3': 'JUP_DB_ATPCO_Record_3_Cat_07'}
                new_doc = {'fare_rule': rule, 'tariff_code': row['TARIFF'], 'cat_no': row['CAT_NO'],
                           'max_stay': row['MAX_STAY_new'], 'unit': row['UNIT_new'], 'comp_fare_rule': row['RULE_NO'],
                           'comp_seq': row['SEQ_NO'], 'eff_from': row['DATES_EFF_1_new'],
                           'eff_to': row['DATES_DISC_1_new'], 'competitor': row['CXR_CODE'],
                           'record_3': 'JUP_DB_ATPCO_Record_3_Cat_07'}
                obj = MaxStay(name='competitor_max_stay_change',
                              old_database_doc=old_doc,
                              new_database_doc=new_doc)
                id = obj.do_analysis()
                analyze(id)
    client.close()


if __name__ == '__main__':
    # old_doc = {'fare_rule': '01AE', 'tariff_code': '033', 'cat_no': '007', 'max_stay': '03', 'unit': 'M',
    #            'comp_fare_rule': '07AE', 'competitor': 'EK', 'comp_seq': '5000242'}
    # new_doc = {'fare_rule': '01AE', 'tariff_code': '033', 'cat_no': '007', 'max_stay': '02', 'unit': 'M',
    #            'comp_fare_rule': '07AE', 'competitor': 'EK', 'comp_seq': '5000242'}
    # obj = MaxStay(name='competitor_max_stay_change', old_database_doc=old_doc, new_database_doc=new_doc)
    # obj.do_analysis()
    main()