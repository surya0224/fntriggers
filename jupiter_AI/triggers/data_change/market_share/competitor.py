"""
Author - Sai Krishna K
Date - 2017-04-08
DESC -
Competitor Market Share Change
Market Share is obtained from PAXIS data.
A change in market share is identified by comparing two snapshots of data for the following key
    airline(except host)
    pos
    origin
    destination
    compartment
    month(departure)
    year(year)

    market_share(parameter that is compared)

If the percentage change in the market share is identified to be below or above configured thresholds for the trigger.
The trigger is raised for Analysis.

From this point this Code takes over.
"""
import traceback

from jupiter_AI import RABBITMQ_USERNAME, RABBITMQ_PASSWORD, RABBITMQ_HOST, RABBITMQ_PORT, JUPITER_LOGGER, client, JUPITER_DB
from jupiter_AI.triggers.data_change.MainClass import DataChange
from jupiter_AI.logutils import measure
from jupiter_AI.triggers.common import get_start_end_dates
import json
import rabbitpy
import pandas as pd

from jupiter_AI.triggers.listener_data_level_trigger import analyze

#db = client[JUPITER_DB]

class CompMarketShareChange(DataChange):
    """
    This Class represents in analysing the trigger before sending it to the priority queue
    """
    @measure(JUPITER_LOGGER)
    def __init__(self, name, old_database_doc, new_database_doc):
        print 'Class Called'
        DataChange.__init__(self, name, old_database_doc, new_database_doc)
        if old_database_doc['pos'] == 'NA':
            old_database_doc['pos'] = None
            new_database_doc['pos'] = None
        self.triggering_event = dict(
            collection='JUP_DB_Market_Share',
            field='market_share',
            action='change'
        )
        print 'Built Triggering Event'

    @measure(JUPITER_LOGGER)
    def build_triggering_data(self):
        """
        :return:
        """
        dep_date_start, dep_date_end = get_start_end_dates(month=self.old_doc_data['month'],
                                                           year=self.old_doc_data['year'])
        self.triggering_data = dict(dep_date_start=dep_date_start,
                                    dep_date_end=dep_date_end)
        print 'Built Triggering Data'

    @measure(JUPITER_LOGGER)
    def check_trigger(self):

        print self.old_doc_data['market_share']
        print self.new_doc_data['market_share']
        if type(self.old_doc_data['market_share']) in [int, float] and type(self.new_doc_data['market_share']) in [int,
                                                                                                                   float]:
            if self.threshold_type == 'percent':
                self.change = (self.new_doc_data['market_share'] - self.old_doc_data['market_share']) * 100 / float(
                    self.old_doc_data['market_share'])
                print self.change
                if not (self.lower_threshold < self.change < self.upper_threshold):
                    return True
                else:
                    return False
            elif self.threshold_type == 'absolute':
                self.change = self.new_doc_data['market_share'] - self.old_doc_data['market_share']
                print self.change
                if not (self.lower_threshold < self.change < self.upper_threshold):
                    return True
                else:
                    return False
            else:
                return False


@measure(JUPITER_LOGGER)
def airport_city_df():
    cur = list(db.JUP_DB_City_Airport_Mapping.find({}, {'Airport_Code':1,'City_Code':1,'_id':0}))
    mapping_df = pd.DataFrame(cur)
    mapping_df['destination'] = mapping_df['Airport_Code']
    mapping_df['pseudo_destination'] = mapping_df['City_Code']
    mapping_df.rename(columns={'Airport_Code': 'origin', 'City_Code': 'pseudo_origin'}, inplace=True)
    return mapping_df


@measure(JUPITER_LOGGER)
def host_od_list():
    cur = db.JUP_DB_Host_OD_Capacity.distinct('od')
    od_list = list()
    for i in cur:
        od_list.append(i)
    return od_list


@measure(JUPITER_LOGGER)
def run_competitor_market_share_change_trigger():
    mapping_df = airport_city_df()
    host_ods = host_od_list()
    url = 'amqp://' + RABBITMQ_USERNAME + \
          ":" + RABBITMQ_PASSWORD + \
          "@" + RABBITMQ_HOST + ":" + str(RABBITMQ_PORT) + "/"
    with rabbitpy.Connection(url) as conn:
        with conn.channel() as channel:
            #      channel.queue_delete(queue='Queue5')
            queue = rabbitpy.Queue(channel, 'competitor_market_share_change')
            print "Waiting for messages.."

            for message in queue:
                print "In competitor_market_share_change, message = "
                message.pprint(True)
                message = message.json()
                print message['old_doc']['pseudo_od']
                origin_list = list(
                    mapping_df[mapping_df['pseudo_origin'] == message['old_doc']['origin']].origin.values)
                destination_list = list(
                    mapping_df[mapping_df['pseudo_destination'] == message['old_doc']['destination']].destination.values)
                if len(origin_list) == 0:
                    origin_list = list()
                    origin_list.append(message['old_doc']['pseudo_origin'])
                if len(destination_list) == 0:
                    destination_list = list()
                    destination_list.append(message['old_doc']['pseudo_destination'])
                print "origins to do", origin_list
                print "destinations to do", destination_list
                for origin in origin_list:
                    for destination in destination_list:
                        od = origin + destination
                        print 'trigger od --- ', od
                        if od in host_ods:
                            name = message['trigger']
                            old_database_doc = dict(
                                pos=message['old_doc']['pos'],
                                origin=origin,
                                destination=destination,
                                compartment=message['old_doc']['compartment'],
                                airline=message['old_doc']['MarketingCarrier1'],
                                month=message['old_doc']['month'],
                                year=message['old_doc']['year'],
                                market_share=message['old_doc']['share']
                            )
                            new_database_doc = dict(
                                pos=message['new_doc']['pos'],
                                origin=origin,
                                destination=destination,
                                compartment=message['new_doc']['compartment'],
                                airline=message['new_doc']['MarketingCarrier1'],
                                month=message['new_doc']['month'],
                                year=message['new_doc']['year'],
                                market_share=message['new_doc']['share']
                            )
                            obj = CompMarketShareChange(name=name,
                                                        old_database_doc=old_database_doc,
                                                        new_database_doc=new_database_doc)
                            id = obj.do_analysis()
                            print id
                            try:
                                analyze(id)
                            except Exception as e:
                                print traceback.print_exc()
                                db.JUP_DB_Errors.insert({"err_id": str(id),
                                                         "error_name": str(e.__class__.__name__),
                                                         "error_message": str(e.args[0])})
                message.ack()


if __name__ == '__main__':
    run_competitor_market_share_change_trigger()
