"""
File Name              :   JUP_AI_Data_Level_Batch.py
Author                 :   Sai Krishna
Date Created           :   2016-02-20
Description            :   Main Program to run all batch programs

MODIFICATIONS LOG
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :
"""
from jupiter_AI import client, JUPITER_DB, SYSTEM_DATE,JUPITER_LOGGER
from jupiter_AI.logutils import measure
#db = client[JUPITER_DB]
from jupiter_AI.tiles.jup_common_functions_tiles import gen_collection_name
from jupiter_AI.triggers.data_level.BookingChangesRolling import generate_booking_changes_rolling_triggers
from jupiter_AI.triggers.data_level.BookingChangesVLYR import generate_booking_changes_vlyr_triggers
from jupiter_AI.triggers.data_level.BookingChangesVTGT import generate_booking_changes_vtgt_triggers
from jupiter_AI.triggers.data_level.BookingChangesWeekly import generate_booking_changes_weekly_triggers
# from jupiter_AI.triggers.data_level.PaxChangesRolling import generate_pax_changes_rolling_triggers
# from jupiter_AI.triggers.data_level.PaxChangesVLYR import generate_pax_changes_vlyr_triggers
# from jupiter_AI.triggers.data_level.PaxChangesVTGT import generate_pax_changes_vtgt_triggers
# from jupiter_AI.triggers.data_level.PaxChangesWeekly import generate_pax_changes_weekly_triggers
# from jupiter_AI.triggers.data_level.RevenueChangesRolling import generate_revenue_changes_rolling_triggers
# from jupiter_AI.triggers.data_level.RevenueChangesVLYR import generate_revenue_changes_vlyr_triggers
# from jupiter_AI.triggers.data_level.RevenueChangesVTGT import generate_revenue_changes_vtgt_triggers
# from jupiter_AI.triggers.data_level.RevenueChangesWeekly import generate_revenue_changes_weekly_triggers
# from jupiter_AI.triggers.data_level.YieldChangesRolling import generate_yield_changes_rolling_triggers
# from jupiter_AI.triggers.data_level.YieldChangesVLYR import generate_yield_changes_vlyr_triggers
# from jupiter_AI.triggers.data_level.YieldChangesVTGT import generate_yield_changes_vtgt_triggers
# from jupiter_AI.triggers.data_level.YieldChangesWeekly import generate_yield_changes_weekly_triggers
# from jupiter_AI.triggers.data_level.ForecastChangesVTGT import generate_forecast_triggers
# from jupiter_AI.triggers.data_level.LowestFares import generate_lowest_fare_comparision_trigger
# from jupiter_AI.triggers.data_level.Opportunities import generate_ms_vs_fms_opportunities_trigger


@measure(JUPITER_LOGGER)
def get_pos_od_compartment_combinations():
    """
    :return: 
    """
    crsr = db['JUP_DB_Pos_OD_Mapping'].aggregate(
        [
            {
                '$project':
                    {
                        '_id': 0,
                        'pos': '$pos.City',
                        'origin': '$origin.City',
                        'destination': '$destination.City',
                        'compartment': '$compartment.compartment'
                    }
            }
        ]
    )
    data = list(crsr)
    return data


@measure(JUPITER_LOGGER)
def data_level_trigger_run():
    """
    :return: 
    """
    combinations = get_pos_od_compartment_combinations()
    for combination in combinations:
        pos = combination['pos']
        origin = combination['origin']
        destination = combination['destination']
        compartment = combination['compartment']

        generate_booking_changes_weekly_triggers(pos=pos,
                                                 origin=origin,
                                                 destination=destination,
                                                 compartment=compartment)
        generate_booking_changes_rolling_triggers(pos=pos,
                                                  origin=origin,
                                                  destination=destination,
                                                  compartment=compartment)
        generate_booking_changes_vlyr_triggers(pos=pos,
                                               origin=origin,
                                               destination=destination,
                                               compartment=compartment)
        generate_booking_changes_vtgt_triggers(pos=pos,
                                               origin=origin,
                                               destination=destination,
                                               compartment=compartment)