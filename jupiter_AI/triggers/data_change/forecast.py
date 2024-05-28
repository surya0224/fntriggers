"""
{
    'name': 'forecast_changes'
    'old_database_doc': {
        region,
        country,
        pos,
        origin,
        destination,
        compartment,
        pax,
        revenue,
        month,
        year
        }
    'new_database_doc':{
        region,
        country,
        pos,
        origin,
        destination,
        compartment,
        pax,
        revenue,
        month,
        year
        }
"""
import traceback

from jupiter_AI.triggers.listener_data_level_trigger import analyze
from jupiter_AI import Host_Airline_Code

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
from jupiter_AI.triggers.data_change.MainClass import DataChange
from jupiter_AI.triggers.common import get_start_end_dates
import rabbitpy
from jupiter_AI import JUPITER_LOGGER, RABBITMQ_USERNAME, RABBITMQ_PASSWORD, RABBITMQ_HOST, RABBITMQ_PORT, client, JUPITER_DB
from jupiter_AI.logutils import measure
#db = client[JUPITER_DB]

class ForecastChange(DataChange):
    """
    This Class represents in analysing the trigger before sending it to the priority queue
    """
    @measure(JUPITER_LOGGER)
    def __init__(self, name, old_database_doc, new_database_doc):
        DataChange.__init__(self, name, old_database_doc, new_database_doc)

        if self.old_doc_data['param'] == 'pax':
            field = 'pax'
        else:
            field = 'revenue'

        self.triggering_event = dict(
            collection='JUP_DB_Forecast_OD_All',
            field=field,
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
        if self.triggering_event['field'] == 'pax':
            self.change = self.new_doc_data['pax_variance']
        elif self.triggering_event['field'] == 'revenue':
            self.change = self.new_doc_data['revenue_variance']
        print self.change
        return True


@measure(JUPITER_LOGGER)
def run_forecast_change_trigger():
    url = 'amqp://' + RABBITMQ_USERNAME + \
          ":" + RABBITMQ_PASSWORD + \
          "@" + RABBITMQ_HOST + ":" + str(RABBITMQ_PORT) + "/"
    with rabbitpy.Connection(url) as conn:
        with conn.channel() as channel:
            #      channel.queue_delete(queue='Queue5')
            queue = rabbitpy.Queue(channel, 'data_change_forecast_trigger')
            print "Waiting for messages.."

            for message in queue:
                print "In forecast_change, message = "
                message.pprint(True)
                message = message.json()
                print message['old_doc']
                name = message['trigger']
                length_list = list()
                length_list.append(message['old_doc']['param'])
                if message['old_doc']['param'] == 'both':
                    length_list = ['pax', 'revenue']
                for i in length_list:
                    old_database_doc = dict(
                        pos=message['old_doc']['pos'],
                        origin=message['old_doc']['od'][:3],
                        destination=message['old_doc']['od'][3:],
                        compartment=message['old_doc']['compartment'],
                        airline=Host_Airline_Code,
                        month=int(message['old_doc']['departureMonth'][4:]),
                        year=int(message['old_doc']['departureMonth'][:4]),
                        pax=message['old_doc']['pax'],
                        revenue=message['old_doc']['revenue'],
                        param=i
                    )
                    new_database_doc = dict(
                        pos=message['new_doc']['pos'],
                        origin=message['new_doc']['od'][:3],
                        destination=message['new_doc']['od'][3:],
                        compartment=message['new_doc']['compartment'],
                        airline=Host_Airline_Code,
                        month=int(message['new_doc']['departureMonth'][4:]),
                        year=int(message['new_doc']['departureMonth'][:4]),
                        pax=message['new_doc']['pax'],
                        revenue=message['new_doc']['revenue'],
                        param=i,
                        revenue_variance=message['new_doc']['revenue_variance'],
                        pax_variance=message['new_doc']['pax_variance']
                    )
                    obj = ForecastChange(name=name,
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

if __name__=='__main__':
    # name = 'forecast_changes'
    # old_database_doc = dict(
    #     pos=None,
    #     origin='DXB',
    #     destination='DOH',
    #     compartment='Y',
    #     airline='FZ',
    #     month=2,
    #     year=2017,
    #     pax=15,
    #     revenue=300,
    #     param='pax'
    # )
    # new_database_doc = dict(
    #     pos=None,
    #     origin='DXB',
    #     destination='DOH',
    #     compartment='Y',
    #     airline='FZ',
    #     month=2,
    #     year=2017,
    #     pax=50,
    #     revenue=700,
    #     param='pax'
    # )
    # obj = ForecastChange(name=name,
    #                      old_database_doc=old_database_doc,
    #                      new_database_doc=new_database_doc)
    # obj.do_analysis()
    run_forecast_change_trigger()