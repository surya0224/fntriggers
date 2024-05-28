"""
Author - Sai Krishna K
Date - 2017-04-08
DESC -
Host Market Share Change
Market Share is obtained from PAXIS data.
A change in market share is identified by comparing two snapshots of data for the following key
    airline(Host)
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

import rabbitpy

from jupiter_AI import RABBITMQ_USERNAME, RABBITMQ_PASSWORD, RABBITMQ_HOST, RABBITMQ_PORT, client, JUPITER_LOGGER, JUPITER_DB
from jupiter_AI.triggers.data_change.MainClass import DataChange
from jupiter_AI.logutils import measure
from jupiter_AI.triggers.common import get_start_end_dates
from jupiter_AI.triggers.listener_data_level_trigger import analyze

#db = client[JUPITER_DB]


class HostMarketShareChange(DataChange):
    """
    This Class represents in analysing the trigger before sending it to the priority queue
    """
    @measure(JUPITER_LOGGER)
    def __init__(self, name, old_database_doc, new_database_doc):
        DataChange.__init__(self, name, old_database_doc, new_database_doc)
        self.triggering_event = dict(
            collection='',
            field='market_share',
            action='change'
        )

    @measure(JUPITER_LOGGER)
    def build_triggering_data(self):
        """
        :return:
        """
        dep_date_start, dep_date_end = get_start_end_dates(month=self.old_doc_data['month'],
                                                           year=self.old_doc_data['year'])
        self.triggering_data = dict(dep_date_start=dep_date_start,
                                    dep_date_end=dep_date_end)

    @measure(JUPITER_LOGGER)
    def check_trigger(self):
        print self.old_doc_data['market_share']
        print self.new_doc_data['market_share']
        if type(self.old_doc_data['market_share']) in [int, float] and type(self.new_doc_data['market_share']) in [int, float]:
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
def run_host_market_share_change_trigger():
    url = 'amqp://' + RABBITMQ_USERNAME + \
          ":" + RABBITMQ_PASSWORD + \
          "@" + RABBITMQ_HOST + ":" + str(RABBITMQ_PORT) + "/"
    with rabbitpy.Connection(url) as conn:
        with conn.channel() as channel:
            #      channel.queue_delete(queue='Queue5')
            queue = rabbitpy.Queue(channel, 'host_market_share_change')
            print "Waiting for messages.."

            for message in queue:
                print "In host_market_share_change, message = "
                message.pprint(True)
                message = message.json()
                print message['old_doc']['pseudo_od']
                name = message['trigger']
                old_database_doc = dict(
                    pos=message['old_doc']['pos'],
                    origin=message['old_doc']['pseudo_origin'],
                    destination=message['old_doc']['pseudo_destination'],
                    compartment=message['old_doc']['compartment'],
                    airline=message['old_doc']['MarketingCarrier1'],
                    month=message['old_doc']['month'],
                    year=message['old_doc']['year'],
                    market_share=message['old_doc']['share']
                )
                new_database_doc = dict(
                    pos=message['new_doc']['pos'],
                    origin=message['new_doc']['pseudo_origin'],
                    destination=message['new_doc']['pseudo_destination'],
                    compartment=message['new_doc']['compartment'],
                    airline=message['new_doc']['MarketingCarrier1'],
                    month=message['new_doc']['month'],
                    year=message['new_doc']['year'],
                    market_share=message['new_doc']['share']
                )
                obj = HostMarketShareChange(name=name,
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
    run_host_market_share_change_trigger()
