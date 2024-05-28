from jupiter_AI import mongo_client, JUPITER_DB, SYSTEM_DATE
import random
import bson
from celery import group

from jupiter_AI.batch.atpco_automation.Automation_tasks import run_booking_triggers, \
    run_events_triggers, \
    run_pax_triggers, \
    run_revenue_triggers, \
    run_yield_triggers, \
    run_opp_trend_triggers, \
    run_pccp_triggers


def main(db):
    trigger_list = list(db.execution_stats.aggregate([
        {
            "$match": {
                "task_start_date": SYSTEM_DATE,
                "group_name": "triggers_group",
                "error_class": {"$exists": True},
                "rerun": {"$exists": False}
            }

        },

        {
            "$project": {
                "trigger": "$task_name",
                "markets": "$kwargs"
            }

        }

    ]))

    triggers_group = []
    trigger_id = []

    for trigger in trigger_list:
        trigger_id.append(bson.objectid.ObjectId(trigger['_id']))

        if len(trigger['markets'])>0:
            if trigger['trigger'] == "run_pax_triggers":
                triggers_group.append(run_pax_triggers.s(markets=trigger['markets']['markets'],
                                                         sig_flag=trigger['markets']['sig_flag']))
            elif trigger['trigger'] == "run_booking_triggers":
                triggers_group.append(run_booking_triggers.s(markets=trigger['markets']['markets'],
                                                         sig_flag=trigger['markets']['sig_flag']))
            elif trigger['trigger'] == "run_revenue_triggers":
                triggers_group.append(run_revenue_triggers.s(markets=trigger['markets']['markets'],
                                                         sig_flag=trigger['markets']['sig_flag']))
            elif trigger['trigger'] == "run_yield_triggers":
                triggers_group.append(run_yield_triggers.s(markets=trigger['markets']['markets'],
                                                         sig_flag=trigger['markets']['sig_flag']))
            elif trigger['trigger'] == "run_opp_trend_triggers":
                triggers_group.append(run_opp_trend_triggers.s(markets=trigger['markets']['markets'],
                                                          sig_flag=trigger['markets']['sig_flag']))
            elif trigger['trigger'] == "run_events_triggers":
                triggers_group.append(run_events_triggers.s(markets=trigger['markets']['markets'],
                                                               sig_flag=trigger['markets']['sig_flag']))
            else:
                print "The following trigger is not defined:", trigger['trigger']

        else:
            print "appending pccp trigger.........."
            triggers_group.append(run_pccp_triggers.s())

    print "updating the rerun status in exection stats....."
    if len(trigger_id) >0:
        db.execution_stats.update_many(
            {
                "_id": {"$in": trigger_id}
            },
            {
                "$set": {"rerun": True}
            }
            #
            # {
            #     "upsert": False,
            #     "multi": True
            # }
        )
        print "updated the rerun status."

        print "Starting the triggers......"
        random.shuffle(triggers_group)
        trig_group = group(triggers_group)
        res_group = trig_group()
        trig_grp_res = res_group.get()

        print "Done triggers rerun"
    return


if __name__ == "__main__":
    client = mongo_client()
    db = client[JUPITER_DB]
    main(db)



