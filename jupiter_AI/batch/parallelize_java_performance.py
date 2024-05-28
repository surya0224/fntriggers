from jupiter_AI import RABBITMQ_HOST, RABBITMQ_PASSWORD, RABBITMQ_PORT, RABBITMQ_USERNAME, client, JUPITER_DB, \
    memory_limit, JUPITER_LOGGER
from jupiter_AI.logutils import measure
from jupiter_AI.batch.atpco_automation.Automation_tasks import performance_response_from_java
from celery import group
import time

url = 'amqp://' + RABBITMQ_USERNAME + \
        ":" + RABBITMQ_PASSWORD + \
        "@" + RABBITMQ_HOST + ":" + str(RABBITMQ_PORT) + "/"

db = client[JUPITER_DB]


@measure(JUPITER_LOGGER)
def parallelize_java_performance():
    st = time.time()
    market_grp = []
    id_list = db.Temp_fzDB_tbl_001.aggregate([
        {
            '$addFields':
                {
                    'pos': {'$substr': ['$market', 0, 3]},
                    'origin': {'$substr': ['$market', 3, 3]},
                    'destination': {'$substr': ['$market', 6, 3]},
                    'compartment': {'$substr': ['$market', 9, 1]}
                }
        }, {

            '$group': {
                "_id": {
                    'pos': '$pos',
                    'dep_date_start': '$dep_date_start',
                    'dep_date_end': '$dep_date_end'
                }
            }
        }, {
            '$project': {
                'pos': '$_id.pos',
                'dep_date_start': '$_id.dep_date_start',
                'dep_date_end': '$_id.dep_date_end'
            }
        }])
    print "Got Triggers"
    id_list = list(id_list)
    for doc in id_list:
        market_grp.append(performance_response_from_java.s(pos=doc['pos'],
                                                           dep_date_start=doc['dep_date_start'],
                                                           dep_date_end=doc['dep_date_end']))

    market_group = group(market_grp)
    print "Created group of all triggers"
    res = market_group()
    res.get()
    print "Total time taken to get performance params from Java = ", time.time() - st


if __name__ == "__main__":
    parallelize_java_performance()