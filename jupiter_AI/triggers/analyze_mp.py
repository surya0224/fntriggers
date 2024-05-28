import pymongo
from jupiter_AI.triggers.listener_data_level_trigger import analyze
from multiprocessing import Pool, cpu_count
from jupiter_AI import JUPITER_DB, MONGO_CLIENT_URL, ANALYTICS_MONGO_USERNAME, ANALYTICS_MONGO_PASSWORD, MONGO_SOURCE_DB

client = pymongo.MongoClient(MONGO_CLIENT_URL)
client.the_database.authenticate(ANALYTICS_MONGO_USERNAME, ANALYTICS_MONGO_PASSWORD, source=MONGO_SOURCE_DB)
db = client[JUPITER_DB]

@profile
def analyze_trigger(crsr):
    _id = crsr['triggering_event_id']
    analyze(_id)
    db.JUP_DB_Triggers_Raised_today.update({"triggering_event_id": str(_id)},
                                           {"$set": {"is_done": 1}})
    client.close()
    return _id


if __name__ == "__main__":
    import time
    start_time = time.time()
    db = client[JUPITER_DB]
    id_list = db.JUP_DB_Triggers_Raised_today.find({'trigger_type': 'YieldChangesRolling'}, {"_id": 0, "triggering_event_id": 1}).limit(1)
    num_triggers = id_list.count()
    p = Pool(processes=cpu_count())
    x = p.map(analyze_trigger, id_list)
    print "Number of triggers received = ", num_triggers
    print "Length of returned ids = ", len(x)
    end_time = time.time()
    print "Total time taken for analysis of triggers = ", start_time - end_time
