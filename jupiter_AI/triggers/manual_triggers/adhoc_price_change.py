"""

"""
from jupiter_AI.triggers.trigger_classes import trigger
# from jupiter_AI.triggers.trigger_classes import
import datetime

class manual(trigger):
    def __init__(self,triggering_event_id):
        trigger.__init__(self,triggering_event_id)

    def get_host_farebasis(self):
        self.farebasis = self.old_doc_data['farebasis']


class percentage_change(manual):
    def __init__(self,triggering_event_id):
        manual.__init__(self,triggering_event_id)

    def get_price_recommendation(self):
        self.price_recommendation = self.old_doc_data['price']*(1+self.new_doc_data[percentage_change])
        self.percent_change = ''
        self.abs_change = ''
        self.ref_farebasis = ''
        self.process_end_date = datetime.datetime.now().strftime('%Y-%m-%d')
        self.processed_end_time = datetime.datetime.now().strftime('%H:%M:%S.%f')
        self.batch_price = ''


class adhoc_percentage_price_change(percentage_change):
    def __init__(self,triggering_event_id):
        percentage_change.__init__(self, triggering_event_id)

    def get_desc(self):
        self.desc = ' '.join(("Recommendation for trigger raised because an adhoc request for a percentage price change",
                             self.old_doc_data['airline'].encode()," from ",str(self.old_doc_data['price'])," to ",
                             str(self.new_doc_data['price'])," on ",str(self.triggering_event_date)," at ",str(self.triggering_event_time)))


class sales_review_request_percentage_change(percentage_change):
    def __init__(self,triggering_event_id):
        percentage_change.__init__(self,triggering_event_id)

    def get_desc(self):
        self.desc = ' '.join(("Recommendation for trigger raised because of sales request for percentage price change",
                                  self.old_doc_data['airline'].encode(), " from ", str(self.old_doc_data['price'])," to ",
                                  str(self.new_doc_data['price']), " on ", str(self.triggering_event_date), " at ",str(self.triggering_event_time)))

class absolute_change(manual):
    def __init__(self,triggering_event_id):
        manual.__init__(self,triggering_event_id)

    def get_price_recommendation(self):
        self.price_recommendation = self.old_doc_data['price']+self.new_doc_data[absolute_change]
        self.percent_change = 'null'
        self.abs_change = 'null'
        self.ref_farebasis = 'null'
        self.process_end_date = datetime.datetime.now().strftime('%Y-%m-%d')
        self.processed_end_time = datetime.datetime.now().strftime('%H:%M:%S.%f')
        self.batch_price = 'null'

class adhoc_absolute_price_change(absolute_change):
    def __init__(self,triggering_event_id):
        absolute_change.__init__(self,triggering_event_id)

    def get_desc(self):
        self.desc = ' '.join(("Recommendation for trigger raised because an adhoc request for a absolute price change",
                                  self.old_doc_data['airline'].encode(), " from ", str(self.old_doc_data['price']), " to ",
                                  str(self.new_doc_data['price']), " on ", str(self.triggering_event_date), " at ",str(self.triggering_event_time)))

class sales_review_request_absolute_change(absolute_change):
    def __init__(self,triggering_event_id):
        absolute_change.__init__(self,triggering_event_id)

    def get_desc(self):
        self.desc = ' '.join(("Recommendation for trigger raised because an sales request for absolute price change",
                                  self.old_doc_data['airline'].encode(), " from ", str(self.old_doc_data['price']), " to ",
                                  str(self.new_doc_data['price']), " on ", str(self.triggering_event_date), " at ",str(self.triggering_event_time)))

class direct(manual):
    def __init__(self,triggering_event_id):
        manual.__init__(self,triggering_event_id)

    def get_price_recommendation(self):
        self.price_recommendation = self.new_doc_data['price']
        self.percent_change = ''
        self.abs_change = ''
        self.ref_farebasis = ''
        self.process_end_date = datetime.datetime.now().strftime('%Y-%m-%d')
        self.processed_end_time = datetime.datetime.now().strftime('%H:%M:%S.%f')
        self.batch_price = ''


class farebasis_upload(direct):
    def __init__(self,triggering_event_id):
        direct.__init__(self,triggering_event_id)

    def get_desc(self):
        def get_desc(self):
            self.desc = ' '.join(("Recommendation for trigger raised because to upload fair basis",
                                      self.old_doc_data['airline'].encode(), " from ", str(self.old_doc_data['price']), " to ",
                                      str(self.new_doc_data['price']), " on ", str(self.triggering_event_date), " at ",str(self.triggering_event_time)))

class sales_review_request_upload(direct):
    def __init__(self,triggering_event_id):
        direct.__init__(self,triggering_event_id)

    def get_desc(self):
        self.desc = ' '.join(("Recommendation for trigger raised because to upload sales review request fair basis",
                                  self.old_doc_data['airline'].encode(), " from ", str(self.old_doc_data['price']), " to ",
                                  str(self.new_doc_data['price']), " on ", str(self.triggering_event_date), " at ", str(self.triggering_event_time)))

class no_price_action(manual):
    def __init__(self,triggering_event_id):
        manual.__init__(self,triggering_event_id)

    def get_price_recommendation(self):
        self.price_recommendation = ''
        self.percent_change = ''
        self.abs_change = ''
        self.ref_farebasis = ''
        self.process_end_date = datetime.datetime.now().strftime('%Y-%m-%d')
        self.processed_end_time = datetime.datetime.now().strftime('%H:%M:%S.%f')
        self.batch_price = ''

class sales_review_request_no_action(no_price_action):
    def __init(self,triggering_event_id):
        no_price_action.__init__(self,triggering_event_id)

    def get_desc(self):
        self.desc = ' '.join(("Recommendation for trigger raised because sales review request no action change",
                                  self.old_doc_data['airline'].encode(), " from ", str(self.old_doc_data['price']), " to ",
                                  str(self.new_doc_data['price']), " on ", str(self.triggering_event_date), " at ", str(self.triggering_event_time)))
