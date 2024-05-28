"""
File Name              :   JUP_DB_Batch_Event_Data_Level_Triggers
Author                 :   Sai Krishna
Date Created           :   2017-05-29
Description            :   For all the events n the next one year,
                           Run all the Data Level Triggers.
MODIFICATIONS LOG
    S.No               :
    Date Modified      :
    By                 :
    Modification Details:
"""
from collections import defaultdict

from jupiter_AI import client, JUPITER_DB, SYSTEM_DATE, JUPITER_LOGGER
from jupiter_AI.logutils import measure
#db = client[JUPITER_DB]

from jupiter_AI.triggers.data_level.MainClass import get_pos_od_compartment_combinations

from jupiter_AI.triggers.data_level.BookingChangesWeekly import BookingChangesWeekly
from jupiter_AI.triggers.data_level.BookingChangesRolling import BookingChangesRolling
from jupiter_AI.triggers.data_level.BookingChangesVLYR import BookingChangesVLYR
from jupiter_AI.triggers.data_level.BookingChangesVTGT import BookingChangesVTGT

from jupiter_AI.triggers.data_level.PaxChangesWeekly import PaxChangesWeekly
from jupiter_AI.triggers.data_level.PaxChangesRolling import PaxChangesRolling
from jupiter_AI.triggers.data_level.PaxChangesVLYR import PaxChangesVLYR
from jupiter_AI.triggers.data_level.PaxChangesVTGT import PaxChangesVTGT

from jupiter_AI.triggers.data_level.RevenueChangesWeekly import RevenueChangesWeekly
from jupiter_AI.triggers.data_level.RevenueChangesRolling import RevenueChangesRolling
from jupiter_AI.triggers.data_level.RevenueChangesVLYR import RevenueChangesVLYR
from jupiter_AI.triggers.data_level.RevenueChangesVTGT import RevenueChangesVTGT

from jupiter_AI.triggers.data_level.YieldChangesWeekly import YieldChangesWeekly
from jupiter_AI.triggers.data_level.YieldChangesRolling import YieldChangesRolling
from jupiter_AI.triggers.data_level.YieldChangesVLYR import YieldChangesVLYR
from jupiter_AI.triggers.data_level.YieldChangesVTGT import YieldChangesVTGT

from jupiter_AI.triggers.data_level.Opportunities import OpportunitiesMrktShrFMS
from jupiter_AI.triggers.data_level.ForecastChangesVTGT import ForecastChangesVTGT


@measure(JUPITER_LOGGER)
def get_event_dep_ranges(SYSTEM_DATE):
    """
    Get the event names and departure dates for the events
    :param SYSTEM_DATE: Today's date so as to get the events for only futuristic dates.
    :return: 
    """
    events = defaultdict(tuple)
    event_crsr = db.JUP_DB_Pricing_Calendar.find()
    for doc in event_crsr:
        pass

    return {
        ('Ramzan',2017): ('2017-05-27', '2017-06-27')
    }



@measure(JUPITER_LOGGER)
def run_datalevel_triggers(event, pos_od_comp_combination):
    pass


@measure(JUPITER_LOGGER)
def main():
    events = get_event_dep_ranges(SYSTEM_DATE)
    pos_od_comp_combinations = get_pos_od_compartment_combinations(SYSTEM_DATE)



