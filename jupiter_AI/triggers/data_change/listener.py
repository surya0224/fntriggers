import json
import sys

import pika

from jupiter_AI import parameters
from jupiter_AI.triggers.data_change.forecast import ForecastChange
from jupiter_AI.triggers.data_change.market_share.competitor import CompMarketShareChange
from jupiter_AI.triggers.data_change.market_share.host import HostMarketShareChange
from jupiter_AI.triggers.data_change.price.competitor import CompEntry, CompExit, CompPriceChange
from jupiter_AI.triggers.data_change.price.promotions_trigger import PromoRuleChangeTrigger, PromoDateChangeTrigger, PromoFareChangeTrigger, PromoNewPromotionTrigger

sys.stdout.flush()


def call_relevant_classes(body):
    """
    :param body: Message Body from Rabbit MQ Queue
    :return:
    """
    print body
    name = body['name']
    old_database_doc = body['old_database_doc']
    new_database_doc = body['new_database_doc']
    changed_field = body['changed_field']
    if name == 'competitor_market_share_change':
        obj = CompMarketShareChange(name=name,
                                    old_database_doc=old_database_doc,
                                    new_database_doc=new_database_doc)
        obj.do_analysis()
    elif name == 'host_market_share_change':
        obj = HostMarketShareChange(name=name,
                                    old_database_doc=old_database_doc,
                                    new_database_doc=new_database_doc)
        obj.do_analysis()
    elif name == 'forecast_changes':
        obj = ForecastChange(name=name,
                             old_database_doc=old_database_doc,
                             new_database_doc=new_database_doc)
        obj.do_analysis()
    elif name == 'competitor_new_entry':
        obj = CompEntry(name=name,
                        old_database_doc=old_database_doc,
                        new_database_doc=new_database_doc)
        obj.do_analysis()
    elif name == 'competitor_exit':
         obj = CompExit(name=name,
                        old_database_doc=old_database_doc,
                        new_database_doc=new_database_doc)
         obj.do_analysis()
    elif name == 'competitor_price_change':
         obj = CompPriceChange(name=name,
                               old_database_doc=old_database_doc,
                               new_database_doc=new_database_doc)
         obj.do_analysis()
    elif name == 'promotions_ruleschange':
         obj = PromoRuleChangeTrigger(name=name,
                               old_database_doc=old_database_doc,
                               new_database_doc=new_database_doc,
                                      changed_field= changed_field)
         obj.do_analysis()
    elif name == 'promotions_dateschange':
        obj = PromoDateChangeTrigger(name=name,
                              old_database_doc=old_database_doc,
                              new_database_doc=new_database_doc,
                                     changed_field=changed_field)
        obj.do_analysis()
    elif name == 'promotions_fareschange':
         obj = PromoFareChangeTrigger(name=name,
                               old_database_doc=old_database_doc,
                               new_database_doc=new_database_doc,
                                      changed_field=changed_field)
         obj.do_analysis()
    elif name == 'new_promotions':
        obj = PromoNewPromotionTrigger(name=name,
                              old_database_doc=old_database_doc,
                              new_database_doc=new_database_doc,
                                       changed_field=changed_field)
        obj.do_analysis()
    else:
        print 'Trigger Name :: ', str(name), ' NOT VALID'


def print_msg(body):
    print(" [x] Received %r" % body)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue='trigger_data')


def callback(ch, method, properties, body):
    call_relevant_classes(body=json.loads(body))

channel.basic_consume(callback,
                      queue='trigger_data',
                      no_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()