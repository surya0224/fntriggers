import datetime
import inspect
import json
import time
from copy import deepcopy
import pymongo
import numpy as np
import pandas as pd
from bson import ObjectId
from dateutil.relativedelta import relativedelta

# from jupiter_AI.batch.fbmapping_batch.JUP_DB_Batch_Fare_Ladder_Mapping_Web import map_fare_infare
from jupiter_AI import client, Host_Airline_Code, JUPITER_LOGGER, mongo_client
from jupiter_AI import network_level_params as net
from jupiter_AI.common import ClassErrorObject as error_class
from jupiter_AI.logutils import measure
from jupiter_AI.network_level_params import JUPITER_DB
from jupiter_AI.network_level_params import SYSTEM_DATE, today
from jupiter_AI.triggers.host_params_workflow_opt import main_func as get_performance_params
from jupiter_AI.triggers.mrkt_params_workflow_opt import comp_summary_python as get_mrkt_params
from jupiter_AI.triggers.mrkt_params_workflow_opt import get_lowest_filed_fare_dF, get_most_avail_dict
from jupiter_AI.triggers.recommendation_models.oligopoly_fl_ge import get_host_fares_df_all
from jupiter_AI.triggers.workflow_mrkt_level_update import get_dep_date_filters, get_config_dates
from jupiter_AI.triggers.workflow_mrkt_level_update import main_func as update_pos_od_comp_level_col, hit_java_url, \
    build_market_data_from_java_response

#db = client[JUPITER_DB]

#################################################################
##### CLASS STRUCTURE OF TRIGGERS ###############################
#################################################################

url = net.JAVA_URL


@measure(JUPITER_LOGGER)
def get_event_data(start_date, end_date, od, db):
    unique_event_list = []
    key_start = "Start_date_" + start_date[0:4]
    key_end = "End_date_" + start_date[0:4]
    events_crsr = db.JUP_DB_Pricing_Calendar.find(
        {key_end: {"$gte": start_date}, key_start: {"$lte": end_date}, "Market": od}, {"Holiday_Name": 1, "_id": 0})
    events_list = ""
    print "Events crsr count: ", events_crsr.count()
    if events_crsr.count() > 0:
        for event in events_crsr:
            if event['Holiday_Name'] not in unique_event_list:
                events_list = events_list + event['Holiday_Name'] + ", "
                unique_event_list.append(event['Holiday_Name'])

        return events_list[:-2]
    else:
        return events_list


class MyEncoder(json.JSONEncoder):
    @measure(JUPITER_LOGGER)
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(MyEncoder, self).default(obj)


class trigger(object):
    @measure(JUPITER_LOGGER)
    def do_analysis(self, db):
        """
        :return:
        """
        error_flag = False
        try:
            self.triggering_event(db=db)
        except error_class.ErrorObject as esub:
            self.append_error(self.errors, esub)
            if esub.error_level <= error_class.ErrorObject.WARNING:
                error_flag = False
            elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL1:
                error_flag = True
            elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL2:
                error_flag = True
                raise esub

        if not error_flag:
            self.data_through_old_doc_change()

        self.trigger_types(db=db)

        if not error_flag:
            self.check_min_interval(db=db)
            self.check_effective_dates()

        if not error_flag:
            # get_price_recommendation(object=self)
            self.get_desc(db)
        if not self.min_interval_factor:
            try:
                self.update_od_level_collection(db=db)
            except error_class.ErrorObject as esub:
                self.append_error(self.errors, esub)
                if esub.error_level <= error_class.ErrorObject.WARNING:
                    error_flag = False
                elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL1:
                    error_flag = True
                elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL2:
                    error_flag = True
                    raise esub

            # print error_flag
            # print self.errors.__str__()
            # print self.errors.error_list
            # print self.errors.error_object_list
            if self.errors.error_list != [] or self.errors.error_object_list != []:
                db.JUP_DB_Error_Collection.insert_one({'triggering_event_id': self.triggering_event_id,
                                                       'error_descripetion': self.errors.__str__()})
                print 'Error Occured and Updated in dB'
            if not error_flag:
                # try:
                self.update_db(db=db)
                # except Exception:
                #     pass

    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        '''
        Instaniating method for base class of triggers
        Calling of all the functions required for Analysis is done here
        '''
        self.errors = error_class.ErrorObject(
            0, self.get_module_name(), self.get_arg_lists(
                inspect.currentframe()))
        self.triggering_event_id = triggering_event_id
        self.airline = net.Host_Airline_Code
        self.process_start_date = datetime.datetime.strftime(today, '%Y-%m-%d')
        # self.process_start_time = datetime.datetime.now().strftime('%H:%M:%S.%f') # to be uncommented when live
        self.process_start_time = "08:00"

    @measure(JUPITER_LOGGER)
    def triggering_event(self, db):
        '''
        Collects the following data relevant to trigger from collection
        '''
        cursor = db.JUP_DB_Triggering_Event.find(
            {'_id': self.triggering_event_id})
        if cursor.count() == 1:
            p = cursor[0]
            # print p
            self.triggering_event_date = p['date']
            self.triggering_event_time = p['time']
            self.trigger_id = p['trigger_id']
            self.unique_trigger_id = p['unique_trigger_id']
            self.triggering_data = p['triggering_data']
            self.cat_details = p['category_details']
            self.old_doc_data = p['old_doc_data']
            self.new_doc_data = p['new_doc_data']
            self.trigger_type = p['trigger_type']
            self.lower_threshold = p['lower_threshold']
            self.upper_threshold = p['upper_threshold']
            self.threshold_type = p['threshold_type']
            self.priority = p['priority']
            self.min_interval = p['min_interval']
            self.old_pricing_action_id_at_trigger_time = p['pricing_action_id_at_trigger_time'],
            self.is_event_trigger = p['is_event_trigger']
        else:
            e1 = error_class.ErrorObject(
                error_class.ErrorObject.ERRORLEVEL1,
                self.get_module_name(),
                self.get_arg_lists(
                    inspect.currentframe()))
            e1.append_to_error_list("Expected 1 trigger in triggering event for the id " + str(
                self.triggering_event_id) + " but got " + str(cursor.count()))
            raise e1

    @measure(JUPITER_LOGGER)
    def get_region_country(self, db):
        """
        :return:
        """
        if self.pos:
            if self.pos != 'NA':
                crsr = db.JUP_DB_Region_Master.find({'POS_CD': self.pos})
                if crsr.count() >= 1:
                    self.region = crsr[0]['Region']
                    self.country = crsr[0]['COUNTRY_CD']
                else:
                    self.region = 'NA'
                    self.country = 'NA'
            else:
                self.region = 'NA'
                self.country = 'NA'

    @measure(JUPITER_LOGGER)
    def update_od_level_collection(self, db):
        # dep_date_ranges = get_dep_date_filters()
        dep_date_ranges = get_config_dates(pos=self.pos, od=self.od, compartment=self.compartment, db=db)
        extreme_start_date = min([date['start'] for date in dep_date_ranges])
        extreme_end_date = max([date['end'] for date in dep_date_ranges])
        st = time.time()
        # SYSTEM_DATE = datetime.datetime.now().strftime('%Y-%m-%d')
        crsr = db.JUP_DB_Workflow_OD_User.find({
            'pos': self.pos,
            'origin': self.origin,
            'destination': self.destination,
            'compartment': self.compartment,
            'dep_date_start': {'$gte': extreme_start_date},
            'dep_date_end': {'$lte': extreme_end_date},
            'update_date': SYSTEM_DATE
        })
        data_crsr = list(crsr)
        occuring_date_ranges = list()
        if len(data_crsr) > 0:
            for crsr_item in data_crsr:
                temp_occuring_date = {
                    'start': crsr_item['dep_date_start'],
                    'end': crsr_item['dep_date_end']}
                if temp_occuring_date not in occuring_date_ranges:
                    occuring_date_ranges.append(temp_occuring_date)
        # print "Occuring date ranges = ", occuring_date_ranges

        temp_ranges = deepcopy(dep_date_ranges)
        map(lambda d: d.pop('code_list'), temp_ranges)

        index_list = [idx for idx, temp_date in enumerate(temp_ranges) if temp_date not in occuring_date_ranges]

        dates_list_final = [temp_date for idx, temp_date in enumerate(dep_date_ranges) if idx in index_list]
        # print "Dates_list_final = ", dates_list_final
        # print "length of dates to do: ", len(dates_list_final)
        # Now, dates_list_final contains those dates for which no trigger is
        # present for this particular market and those particular dates in
        # dates_list
        print "Time taken to check whether trigger already raised for this market and all dates = ", time.time() - st
        if self.trigger_type != "manual":
            host_fares_all = pd.DataFrame()
            if len(dates_list_final) > 0:
                start_date = min([date['start'] for date in dates_list_final])
                end_date = max([date['end'] for date in dates_list_final])
                host_fares_all = get_host_fares_df_all(pos=self.pos, origin=self.origin, destination=self.destination,
                                                       compartment=self.compartment, extreme_start_date=start_date,
                                                       extreme_end_date=end_date, db=db)
            for range_ in dates_list_final:
                update_pos_od_comp_level_col(pos=self.pos,
                                             origin=self.origin,
                                             destination=self.destination,
                                             compartment=self.compartment,
                                             dep_date_start=range_['start'],
                                             dep_date_end=range_['end'],
                                             db=db,
                                             dates_code=range_['code_list'],
                                             dates_list=dates_list_final,
                                             host_fares=deepcopy(host_fares_all)
                                             )
        if self.trigger_type == "manual" or self.trigger_type == "sales_request" or self.is_event_trigger:
            # for date_ in dep_date_ranges:
            # dep_date_start = net.SYSTEM_DATE
            # dep_date_end = datetime.datetime.strftime((net.today + datetime.timedelta(days=int(30))), '%Y-%m-%d')
            crsr = db.JUP_DB_Workflow_OD_User.find({
                'pos': self.pos,
                'origin': self.origin,
                'destination': self.destination,
                'compartment': self.compartment,
                'dep_date_start': self.triggering_data['dep_date_start'],
                'dep_date_end': self.triggering_data['dep_date_end'],
                'update_date': SYSTEM_DATE
            })
            if crsr.count() == 0:
                update_pos_od_comp_level_col(
                    pos=self.pos,
                    origin=self.origin,
                    destination=self.destination,
                    compartment=self.compartment,
                    dep_date_start=self.triggering_data['dep_date_start'],
                    dep_date_end=self.triggering_data['dep_date_end'],
                    dates_list=dates_list_final,
                    db=db,
                    is_manual=1)

    # @measure(JUPITER_LOGGER)
    # def update_od_level_collection(self):
    #     dep_date_ranges = get_dep_date_filters()
    #     extreme_start_date = min([date['start'] for date in dep_date_ranges])
    #     extreme_end_date = max([date['end'] for date in dep_date_ranges])
    #     st = time.time()
    #     # SYSTEM_DATE = datetime.datetime.now().strftime('%Y-%m-%d')
    #     crsr = db.JUP_DB_Workflow_OD_User.find({
    #         'pos': self.pos,
    #         'origin': self.origin,
    #         'destination': self.destination,
    #         'compartment': self.compartment,
    #         'dep_date_start': {'$gte': extreme_start_date},
    #         'dep_date_end': {'$lte': extreme_end_date},
    #         'update_date': SYSTEM_DATE
    #     })
    #     data_crsr = list(crsr)
    #     occuring_date_ranges = list()
    #     if len(data_crsr) > 0:
    #         for crsr_item in data_crsr:
    #             temp_occuring_date = {
    #                 'start': crsr_item['dep_date_start'],
    #                 'end': crsr_item['dep_date_end']}
    #             if temp_occuring_date not in occuring_date_ranges:
    #                 occuring_date_ranges.append(temp_occuring_date)
    #     # print "Occuring date ranges = ", occuring_date_ranges
    #
    #     dates_list_final = [
    #         temp_date for temp_date in dep_date_ranges if temp_date not in occuring_date_ranges]
    #     # print "Dates_list_final = ", dates_list_final
    #     # print "length of dates to do: ", len(dates_list_final)
    #     # Now, dates_list_final contains those dates for which no trigger is
    #     # present for this particular market and those particular dates in
    #     # dates_list
    #     print "Time taken to check whether trigger already raised for this market and all dates = ", time.time() - st
    #     if self.trigger_type != "manual":
    #         host_fares_all = pd.DataFrame()
    #         if len(dates_list_final) > 0:
    #             start_date = min([date['start'] for date in dates_list_final])
    #             end_date = max([date['end'] for date in dates_list_final])
    #             host_fares_all = get_host_fares_df_all(pos=self.pos, origin=self.origin, destination=self.destination,
    #                                                    compartment=self.compartment, extreme_start_date=start_date,
    #                                                    extreme_end_date=end_date)
    #         for range_ in dates_list_final:
    #             update_pos_od_comp_level_col(pos=self.pos,
    #                                          origin=self.origin,
    #                                          destination=self.destination,
    #                                          compartment=self.compartment,
    #                                          dep_date_start=range_['start'],
    #                                          dep_date_end=range_['end'],
    #                                          dates_code=None,
    #                                          dates_list=dates_list_final,
    #                                          host_fares=deepcopy(host_fares_all)
    #                                         )
    #     if self.trigger_type == "manual" or self.trigger_type == "sales_request" or self.is_event_trigger:
    #         # for date_ in dep_date_ranges:
    #         # dep_date_start = net.SYSTEM_DATE
    #         # dep_date_end = datetime.datetime.strftime((net.today + datetime.timedelta(days=int(30))), '%Y-%m-%d')
    #         crsr = db.JUP_DB_Workflow_OD_User.find({
    #             'pos': self.pos,
    #             'origin': self.origin,
    #             'destination': self.destination,
    #             'compartment': self.compartment,
    #             'dep_date_start': self.triggering_data['dep_date_start'],
    #             'dep_date_end': self.triggering_data['dep_date_end'],
    #             'update_date': SYSTEM_DATE
    #         })
    #         if crsr.count() == 0:
    #             update_pos_od_comp_level_col(
    #                 pos=self.pos,
    #                 origin=self.origin,
    #                 destination=self.destination,
    #                 compartment=self.compartment,
    #                 dep_date_start=self.triggering_data['dep_date_start'],
    #                 dep_date_end=self.triggering_data['dep_date_end'],
    #                 dates_list=dates_list_final,
    #                 is_manual=1)

    @measure(JUPITER_LOGGER)
    def trigger_types(self, db):
        '''
        Takes the trigger_list developed during hierarchy as input
        Returns the document containing the parameters of the trigger
        we are dealing with now
        '''
        crsr = db.JUP_DB_Trigger_Types.find({'desc': self.trigger_type})
        data = list(crsr)
        if len(data) >= 1:
            q = data[0]
            self.pricing_type = q['pricing_type']
            self.pricing_method = q['pricing_method']
            self.dashboard = q['dashboard']
            self.active = q['active']
            self.trigger_status = q['status']
            self.triggering_event_type = q['triggering_event_type']
            self.triggering_event = q['triggering_event']
            self.frequency = q['frequency']
            self.effective_date_from = q['effective_date_from']
            self.effective_date_to = q['effective_date_to']
            self.type_of_trigger = q['type_of_trigger']
        else:
            e1 = error_class.ErrorObject(
                error_class.ErrorObject.ERRORLEVEL1,
                self.get_module_name(),
                self.get_arg_lists(
                    inspect.currentframe()))
            e1.append_to_error_list(
                "Expected 1 trigger type but got " + str(len(data)))
            raise e1

    @measure(JUPITER_LOGGER)
    def data_through_old_doc_change(self):
        '''
        Data to be collected from old_doc_data
        '''
        # region_country_data =
        # self.region = self.old_doc_data['region']
        # self.country = self.old_doc_data['country']
        if self.trigger_type in ['new_promotions', 'promotions_ruleschange', 'promotions_dateschange',
                                 'promotions_fareschange']:
            self.pos = self.new_doc_data['pos']
            self.origin = self.new_doc_data['origin']
            self.destination = self.new_doc_data['destination']
            self.od = self.origin + self.destination
            self.compartment = self.new_doc_data['compartment']
        else:
            self.pos = self.old_doc_data['pos']
            self.origin = self.old_doc_data['origin']
            self.destination = self.old_doc_data['destination']
            self.od = self.origin + self.destination
            self.compartment = self.old_doc_data['compartment']
        # self.effective_from = self.old_doc_data['effective_from']
        # self.effective_to = self.old_doc_data['effective_to']

    @measure(JUPITER_LOGGER)
    def check_min_interval(self, db):
        '''
        Function checks if the interval b/w the last time a
        pricing action has been approved and the present time(t)
        is greater than or less than the min interval defined
        True if t <= min interval
        False if t >= min interval
        '''
        q = db.JUP_DB_Workflow.find({
            'pos': self.pos,
            'origin': self.origin,
            'destination': self.destination,
            'compartment': self.compartment,
            # 'farebasis': self.farebasis,
            'trigger_type': self.trigger_type,
            'status': {'$in': ['accepted', 'Autoaccepted']}
        }).sort([('action_date', pymongo.ASCENDING), ('action_time', pymongo.ASCENDING)])
        if q.count() == 0:
            self.min_interval_factor = False
        else:
            last_pricing_action = q[q.count() - 1]
            last_occurence_str = last_pricing_action['action_date'] + ' ' + last_pricing_action['action_time']
            last_occurence = datetime.datetime.strptime(
                last_occurence_str, '%Y-%m-%d %H:%M:%S.%f')
            delta_hours = (datetime.datetime.now() - last_occurence)
            if delta_hours >= datetime.timedelta(hours=self.min_interval):
                self.min_interval_factor = False
            else:
                self.min_interval_factor = True

    @measure(JUPITER_LOGGER)
    def check_effective_dates(self):
        '''
        Checks if the current date falls in the window of effective dates
        False if today in the window
        True if today violates the window
        '''
        t = datetime.datetime.now()
        effective_date_from = datetime.datetime.strptime(
            self.effective_date_from, '%d-%m-%Y')
        effective_date_to = datetime.datetime.strptime(
            self.effective_date_to, '%d-%m-%Y')
        if effective_date_from < t < effective_date_to:
            self.effective_date_factor = False
        else:
            self.effective_date_factor = True

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        '''
        Function that defines the Description for this trigger
        Defined for individual triggers later
        '''
        pass

    @measure(JUPITER_LOGGER)
    def update_db(self, db):
        '''
        Function to update the database
        if errors occured update errors
        else update the recommendation/trigger
        '''
        # if self.trigger_type in ['new_promotions', 'promotions_ruleschange', 'promotions_dateschange',
        #                          'promotions_fareschange']:
        #     wf_crsr = db.JUP_DB_Workflow.find({
        #         "pos": self.pos,
        #         "origin": self.origin,
        #         "destination": self.destination,
        #         "compartment": self.compartment,
        #         # "trigger_id": self.trigger_id,
        #         "unique_trigger_id": self.unique_trigger_id,
        #         "status":"pending"
        #     })
        # else:
        wf_crsr = db.JUP_DB_Workflow.find({
            "pos": self.pos,
            "origin": self.origin,
            "destination": self.destination,
            "compartment": self.compartment,
            # "trigger_id": self.trigger_id,
            "dep_date_start": self.triggering_data['dep_date_start'],
            "dep_date_end": self.triggering_data['dep_date_end'],
            "trigger_type": self.trigger_type,
            "status": "pending",
            "update_date": SYSTEM_DATE
        })

        wf_crsr = list(wf_crsr)
        if len(wf_crsr) == 0:
            del self.errors
            self.recommendation_category = self.cat_details['category']
            self.recommendation_score = self.cat_details['score']
            self.get_region_country(db=db)
            print 'DONE'
            pricing_actions = self.__dict__
            print "trigger_type: ", self.trigger_type

            if self.trigger_type == "manual" or self.trigger_type == "sales_request" or self.trigger_type != "manual":
                # print "in if"
                if self.trigger_type in ['new_promotions', 'promotions_ruleschange', 'promotions_dateschange',
                                         'promotions_fareschange']:

                    response_ = dict(
                        pos=self.pos,
                        origin=self.origin,
                        destination=self.destination,
                        od=self.origin + self.destination,
                        compartment=self.compartment,
                        dep_date_start=self.triggering_data['dep_date_start'],
                        dep_date_end=self.triggering_data['dep_date_end'],
                        update_date=net.SYSTEM_DATE,
                        trigger_link=self.new_doc_data['Url'],
                        dates_code=None
                    )
                else:
                    response_ = dict(
                        pos=self.pos,
                        origin=self.origin,
                        destination=self.destination,
                        od=self.origin + self.destination,
                        compartment=self.compartment,
                        dep_date_start=self.triggering_data['dep_date_start'],
                        dep_date_end=self.triggering_data['dep_date_end'],
                        update_date=SYSTEM_DATE,
                        dates_code=None
                    )

                response, response_json = hit_java_url(response_)

                #             headers = {"Connection": "keep-alive",
                #                        "Content-Length": "257",
                #                        "X-Requested-With": "XMLHttpRequest",
                #                        "User-59fdb6acdad0385377492b29Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.52 Safari/536.5",
                #                        "Content-Type": "application/json",
                #                        "Accept": "*/*",
                #                        "Accept-Encoding": "gzip,deflate",
                #                        "Accept-Language": "q=0.8,en-US",
                #                        "Accept-Charset": "utf-8"
                #                        }
                #             parameters = {
                #                 "fromDate": self.triggering_data['dep_date_start'],
                #                 "toDate": self.triggering_data['dep_date_end'],
                #                 "posMap": {
                #
                #                     "cityArray": [self.pos]
                #                 },
                #                 "originMap": {
                #                     "cityArray": [self.origin]
                #                 },
                #                 "destMap": {
                #                     "cityArray": [self.destination]
                #                 },
                #                 "compMap": {
                #                     "compartmentArray": [self.compartment]
                #                 }
                #             }
                #             print "sending request..."
                #             response_mt = requests.post(
                #                 url, data=json.dumps(parameters), headers=headers)
                # #            response_json = json.loads(response_mt.text)['ManualTriggerGrid'][0]
                #             print "got response!! ", response_mt.status_code
                #             # print " ---------> ",len(json.loads(response_mt.text)['ManualTriggerGrid'])
                #             if response_mt.status_code == 200 and len(json.loads(response_mt.text)['ManualTriggerGrid']) > 0:
                #                 print "length----" ,len(json.loads(response_mt.text)['ManualTriggerGrid'])
                #                 response_json = json.loads(response_mt.text)['ManualTriggerGrid'][0]
                #                 response['pax_data'] = dict(
                #                     pax=response_json['pax'],
                #                     vlyr=response_json['paxvlyrperc'],
                #                     vtgt=response_json['paxvtgtperc']
                #                 )
                #                 response['revenue_data'] = dict(
                #                     revenue=response_json['revenue'],
                #                     vlyr=response_json['revenuevlyrperc'],
                #                     vtgt=response_json['revenuevtgtperc']
                #                 )
                #                 response['avg_fare_data'] = dict(
                #                     avg_fare=response_json['avgfare'],
                #                     vlyr=response_json['avgfarevlyr'],
                #                     vtgt=response_json['avgfarevtgt']
                #                 )
                #                 response['yield_data_compartment'] = dict(
                #                     yield_=response_json['yield'],
                #                     vlyr=response_json['yieldvlyrperc'],
                #                     vtgt=response_json['yieldvtgtperc']
                #                 )
                #                 response['seat_factor'] = dict(
                #                     leg1=response_json['bookedsf1'],
                #                     leg2=response_json['bookedsf2'],
                #                     leg1_vlyr=response_json['sf1vlyr'],  # check
                #                     leg2_vlyr=response_json['sf2vlyr']  # check
                #                 )

                if response_json:
                    host_data, comp_data, mpf_df_for_web_pricing = build_market_data_from_java_response(
                        response_json, self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                        db=db)

                    perf_params = response
                    mrkt_params = {"host": host_data, "comp": comp_data}

                    pricing_actions.update(perf_params)
                    pricing_actions['mrkt_data'] = mrkt_params
                    pricing_actions['key'] = self.pos + self.origin + \
                                             self.destination + self.compartment
                    pricing_actions['status'] = 'pending'
                    pricing_actions['action_date'] = None
                    pricing_actions['action_time'] = None
                    # print self.triggering_event_id
                    print "-------------->"
                    pricing_actions['triggering_event_id'] = str(self.triggering_event_id)
                    cur = db.JUP_DB_Trigger_Types.find({'desc': self.trigger_type})
                    for i in cur:
                        pricing_actions['trigger_class'] = i['trigger_class']
                        pricing_actions['class_category'] = i['class_category']
                    if self.is_event_trigger:
                        this_year = str(datetime.datetime.strptime(SYSTEM_DATE, '%Y-%m-%d').year)
                        eve = db.JUP_DB_Pricing_Calendar.find({'Market': self.od,
                                                               'Start_date_{}'.format(this_year): self.triggering_data[
                                                                   'dep_date_start'],
                                                               'End_date_{}'.format(this_year): self.triggering_data[
                                                                   'dep_date_end']})
                        for i in eve:
                            pricing_actions['class_category'] = i['Holiday_Name']
                    # print pricing_actions
                    res = db.JUP_DB_Workflow.insert(json.loads(json.dumps(pricing_actions, indent=1, cls=MyEncoder)))
                    print "Inserted into WORKFLOW ! "
            # print perf_params
            # print json.dumps(perf_params)
            # print mrkt_params
            # print json.dumps(mrkt_params)

            else:
                # print "in else"
                perf_params = get_performance_params(
                    pos=self.pos,
                    origin=self.origin,
                    destination=self.destination,
                    compartment=self.compartment,
                    dep_date_start=self.triggering_data['dep_date_start'],
                    dep_date_end=self.triggering_data['dep_date_end'],
                    db=db)

                # print "date_start: ", self.triggering_data['dep_date_start']
                # print "date_end: ", self.triggering_data['dep_date_end']
                mrkt_params = get_mrkt_params(
                    pos=self.pos,
                    origin=self.origin,
                    destination=self.destination,
                    compartment=self.compartment,
                    dep_date_start=self.triggering_data['dep_date_start'],
                    dep_date_end=self.triggering_data['dep_date_end'],
                    db=db,
                    date_list=[
                        {
                            "start": self.triggering_data['dep_date_start'],
                            "end": self.triggering_data['dep_date_end']}])
                # print "done params"
            # print "done if else"

    @measure(JUPITER_LOGGER)
    def get_month_year_query(self, dep_date_start, dep_date_end):
        start_obj = datetime.datetime.strptime(dep_date_start, "%Y-%m-%d")
        end_obj = datetime.datetime.strptime(dep_date_end, "%Y-%m-%d")
        query = []
        while start_obj <= end_obj:
            # print start_obj
            query.append({
                "month": start_obj.month,
                "year": start_obj.year
            })
            # query.append({
            #     "month": start_obj.month,
            #     "year": start_obj.year - 1
            # })
            start_obj = start_obj + relativedelta(months=1)
        return query

    @measure(JUPITER_LOGGER)
    def build_market_data_from_java_response(self,
                                             json_response, dep_date_start, dep_date_end, db):
        comp_data = []
        host_data = {}
        pos = json_response['pos']
        origin = json_response['origin']
        destination = json_response['destination']
        compartment = json_response['compartment']
        month_year_query = self.get_month_year_query(dep_date_start, dep_date_end)
        # top5_comp = obtain_top_5_comp([pos], [origin], [destination], [compartment], dep_date_start, dep_date_end,
        #                               date_list=[{"start": dep_date_start,
        #                                           "end": dep_date_end}])
        # comp = top5_comp
        # print "comp:   ", comp
        comp = []
        temp_ = [Host_Airline_Code]
        for i in range(1, 5):
            temp_.append(json_response['comp' + str(i) + 'carrier'])
        ratings_crsr = list(db.JUP_DB_Rating_ALL.find({"Airline": {"$in": temp_}}))
        pax_crsr = list(db.JUP_DB_Pos_OD_Compartment_new.aggregate([
            {
                "$match": {"pos": pos,
                           "od": origin + destination,
                           "compartment": compartment,
                           "$or": month_year_query
                           }
            }, {
                "$unwind": {
                    "path": "$top_5_comp_data"
                }
            }, {
                "$match": {
                    "top_5_comp_data.airline": {
                        "$in": temp_
                    }
                }
            }, {
                "$project": {
                    "_id": 0,
                    "top_5_comp_data": 1,
                    "month": 1,
                    "year": 1
                }
            }
        ]))
        # cap_crsr = list(db.JUP_DB_OD_Capacity.aggregate(
        #     [{"$match": {"od": origin + destination, "compartment": compartment, "$or": month_year_query, "airline": {"$in": temp_}}},
        #      {"$project": {"_id": 0, "airline": 1, "od_capacity": 1, "month": 1, "year": 1}}]))
        pax_dict_ty = {}
        pax_dict_ly = {}
        cap_ty = {}
        cap_ly = {}

        for comp_ in temp_:
            pax_dict_ly[comp_] = 0
            pax_dict_ty[comp_] = 0
            cap_ty[comp_] = 0
            cap_ly[comp_] = 0

        for record in pax_crsr:
            for comp_ in temp_:
                if (comp_ == record['top_5_comp_data']['airline']) and (comp_ != Host_Airline_Code):
                    if str(record['top_5_comp_data']['pax']) != 'nan':
                        pax_dict_ty[comp_] = pax_dict_ty[comp_] + record['top_5_comp_data']['pax']
                    else:
                        pax_dict_ty[comp_] = pax_dict_ty[comp_] + 0
                    if str(record['top_5_comp_data']['pax_1']) != 'nan':
                        pax_dict_ly[comp_] = pax_dict_ly[comp_] + record['top_5_comp_data']['pax_1']
                    else:
                        pax_dict_ly[comp_] = pax_dict_ly[comp_] + 0
                    if str(record['top_5_comp_data']['capacity']) != 'nan':
                        cap_ty[comp_] = cap_ty[comp_] + record['top_5_comp_data']['capacity']
                    else:
                        cap_ty[comp_] = cap_ty[comp_] + 0
                    if str(record['top_5_comp_data']['capacity_1']) != 'nan':
                        cap_ly[comp_] = cap_ly[comp_] + record['top_5_comp_data']['capacity_1']
                    else:
                        cap_ly[comp_] = cap_ly[comp_] + 0

        ratings_df = pd.DataFrame(ratings_crsr)
        comp.append((temp_, []))
        # print "comp: ---------->", comp
        # print "pax_ty: ", pax_dict_ty
        # print "pax_ly: ", pax_dict_ty
        # print "cap_ty: ", cap_ty
        # print "cap_ly: ", cap_ly
        host_data['market_share_vlyr'] = json_response['marketsharevlyr']
        host_data['market_share'] = json_response['marketshare']
        host_data['rating'] = ratings_df.loc[(
                                                     ratings_df['Airline'] == Host_Airline_Code), 'overall_rating'].values[0]
        host_data['pax'] = json_response['pax']  # check
        host_data['distributor_rating'] = ratings_df.loc[(
                                                                 ratings_df[
                                                                     'Airline'] == Host_Airline_Code), 'distributor_rating'].values[
            0]
        host_data['fms'] = json_response['fairmarketshare']
        host_data['market_rating'] = ratings_df.loc[(
                                                            ratings_df['Airline'] == Host_Airline_Code), 'market_rating'].values[0]
        host_data['capacity_rating'] = ratings_df.loc[(
                                                              ratings_df['Airline'] == Host_Airline_Code), 'capacity_rating'].values[
            0]
        host_data['market_share_vtgt'] = json_response['marketsharevtgt']
        host_data['product_rating'] = ratings_df.loc[(
                                                             ratings_df['Airline'] == Host_Airline_Code), 'product_rating'].values[0]
        host_data['pax_vlyr'] = json_response['paxvlyrperc']
        host_data['fare_rating'] = ratings_df.loc[(
                                                          ratings_df['Airline'] == Host_Airline_Code), 'fare_rating'].values[0]
        # print "built host fares"
        lowest_filed_fare = get_lowest_filed_fare_dF(pos=pos,
                                                     origin=origin,
                                                     destination=destination,
                                                     compartment=compartment,
                                                     dep_date_start=dep_date_start,
                                                     dep_date_end=dep_date_end,
                                                     db=db,
                                                     comp=comp,
                                                     date_list=[{"start": dep_date_start,
                                                       "end": dep_date_end}])
        # print lowest_filed_fare.head()

        most_avail_df = get_most_avail_dict(pos=pos,
                                            origin=origin,
                                            destination=destination,
                                            compartment=compartment,
                                            dep_date_end=dep_date_end,
                                            dep_date_start=dep_date_start,
                                            db=db,
                                            date_list=[{"start": dep_date_start,
                                                        "end": dep_date_end}])
        # most_avail_df = most_avail_df[most_avail_df['carrier'] == "FZ"].reindex().to_dict("records")[0]

        try:
            host_data['lowest_filed_fare'] = dict(
                total_fare=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                                 == Host_Airline_Code, "lowest_fare_total"].values[0],
                base_fare=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                                == Host_Airline_Code, "lowest_fare_base"].values[0],
                fare_basis=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                                 == Host_Airline_Code, "lowest_fare_fb"].values[0],
                tax=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                          == Host_Airline_Code, "lowest_fare_tax"].values[0],
                yq=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                         == Host_Airline_Code, "lowest_fare_YQ"].values[0],
                yr=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                         == Host_Airline_Code, "lowest_fare_YR"].values[0],
                surcharge=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == Host_Airline_Code, "lowest_fare_surcharge"].values[
                    0],
                currency=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == Host_Airline_Code, "currency"].values[
                    0],
                total_fare_gds=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                                     == Host_Airline_Code, "lowest_fare_total_gds"].values[0],
                base_fare_gds=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                                    == Host_Airline_Code, "lowest_fare_base_gds"].values[0],
                fare_basis_gds=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                                     == Host_Airline_Code, "lowest_fare_fb_gds"].values[0],
                tax_gds=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                              == Host_Airline_Code, "lowest_fare_tax_gds"].values[0],
                yq_gds=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                             == Host_Airline_Code, "lowest_fare_YQ_gds"].values[0],
                yr_gds=lowest_filed_fare.loc[lowest_filed_fare['carrier']
                                             == Host_Airline_Code, "lowest_fare_YR_gds"].values[0],
                surcharge_gds=
                lowest_filed_fare.loc[lowest_filed_fare['carrier'] == Host_Airline_Code, "lowest_fare_surcharge_gds"].values[
                    0],
                currency_gds=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == Host_Airline_Code, "currency_gds"].values[
                    0]

            )
        except Exception:
            host_data['lowest_filed_fare'] = dict(
                total_fare="NA",
                base_fare="NA",
                tax="NA",
                fare_basis='NA',
                yq="NA",
                yr="NA",
                surcharge="NA",
                currency="NA",
                total_fare_gds="NA",
                base_fare_gds="NA",
                tax_gds="NA",
                fare_basis_gds='NA',
                yq_gds="NA",
                yr_gds="NA",
                surcharge_gds="NA",
                currency_gds="NA",
            )
        try:
            host_data['price_movement_filed'] = dict(lowest_fare=host_data['lowest_filed_fare'],
                                                     highest_fare=dict(total_fare=lowest_filed_fare.loc[
                                                         lowest_filed_fare['carrier'] == Host_Airline_Code,
                                                         "highest_fare_total"].values[0],
                                                                       base_fare=lowest_filed_fare.loc[
                                                                           lowest_filed_fare['carrier'] == Host_Airline_Code,
                                                                           "highest_fare_base"].values[0],
                                                                       fare_basis=lowest_filed_fare.loc[
                                                                           lowest_filed_fare['carrier'] == Host_Airline_Code,
                                                                           "highest_fare_fb"].values[0],
                                                                       tax=lowest_filed_fare.loc[
                                                                           lowest_filed_fare['carrier'] == Host_Airline_Code,
                                                                           "highest_fare_tax"].values[0],
                                                                       yq=lowest_filed_fare.loc[
                                                                           lowest_filed_fare['carrier'] == Host_Airline_Code,
                                                                           "highest_fare_YQ"].values[0],
                                                                       yr=lowest_filed_fare.loc[
                                                                           lowest_filed_fare['carrier'] == Host_Airline_Code,
                                                                           "highest_fare_YR"].values[0],
                                                                       surcharge=lowest_filed_fare.loc[
                                                                           lowest_filed_fare['carrier'] == Host_Airline_Code,
                                                                           "highest_fare_surcharge"].values[0],
                                                                       currency=lowest_filed_fare.loc[
                                                                           lowest_filed_fare['carrier'] == Host_Airline_Code,
                                                                           "currency"].values[0],
                                                                       ))
        except Exception:
            host_data['price_movement_filed'] = dict(
                lowest_fare=dict(
                    surcharge="NA",
                    total_fare="NA",
                    base_fare="NA",
                    yq="NA",
                    fare_basis="NA",
                    yr="NA",
                    tax="NA",
                ),
                highest_fare=dict(
                    surcharge="NA",
                    total_fare="NA",
                    base_fare="NA",
                    yq="NA",
                    fare_basis="NA",
                    yr="NA",
                    tax="NA",
                )
            )
        try:
            host_data['most_available_fare'] = dict(
                base_fare_ow=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'most_avail_fare_base_ow'].values[0],
                frequency_ow=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'most_avail_fare_freq_ow'].values[0],
                tax_ow=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'most_avail_fare_tax_ow'].values[0],
                total_fare_ow=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'most_avail_fare_total_ow'].values[0],
                total_count_ow=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'most_avail_fare_count_ow'].values[
                    0],
                currency=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'currency'].values[0],
                base_fare_rt=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'most_avail_fare_base_rt'].values[0],
                frequency_rt=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'most_avail_fare_freq_rt'].values[0],
                tax_rt=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'most_avail_fare_tax_rt'].values[0],
                total_fare_rt=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'most_avail_fare_total_rt'].values[0],
                total_count_rt=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'most_avail_fare_count_rt'].values[
                    0],
                fare_basis_ow=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'fare_basis_ow'].values[0],
                fare_basis_rt=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'fare_basis_rt'].values[0],
                observation_date_rt=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'observation_date_rt'].values[
                    0],
                observation_date_ow=most_avail_df.loc[most_avail_df['carrier'] == Host_Airline_Code, 'observation_date_ow'].values[0]

            )
        except Exception:
            host_data['most_available_fare'] = dict(
                base_fare_ow="NA",
                frequency_ow="NA",
                tax_ow="NA",
                total_fare_ow="NA",
                total_count_ow="NA",
                currency="NA",
                base_fare_rt="NA",
                frequency_rt="NA",
                tax_rt="NA",
                total_fare_rt="NA",
                total_count_rt="NA",
                fare_basis_ow="NA",
                fare_basis_rt="NA",
                observation_date_rt="NA",
                observation_date_ow="NA"
            )

        for i in range(1, 5):
            temp = {}
            temp['market_share'] = json_response['comp' + str(i) + 'marketshare']
            temp['fms'] = json_response['comp' + str(i) + 'fairmarketshare']
            try:
                temp['distributor_rating'] = ratings_df.loc[(
                                                                    ratings_df['Airline'] == json_response[
                                                                'compcarrier' + str(
                                                                    i)]), 'distributor_rating'].values[0]
                temp['rating'] = ratings_df.loc[(
                                                        ratings_df['Airline'] == json_response[
                                                    'comp' + str(i) + 'carrier']), 'overall_rating'].values[0]
                temp['capacity_rating'] = ratings_df.loc[(
                                                                 ratings_df['Airline'] == json_response[
                                                             'comp' + str(i) + 'carrier']), 'capacity_rating'].values[0]
                temp['market_rating'] = ratings_df.loc[(
                                                               ratings_df['Airline'] == json_response[
                                                           'comp' + str(i) + 'carrier']), 'market_rating'].values[0]
                temp['fare_rating'] = ratings_df.loc[(
                                                             ratings_df['Airline'] == json_response[
                                                         'comp' + str(i) + 'carrier']), 'fare_rating'].values[0]
                temp['product_rating'] = ratings_df.loc[(
                                                                ratings_df['Airline'] == json_response[
                                                            'comp' + str(i) + 'carrier']), 'product_rating'].values[0]
            except Exception:
                temp['rating'] = 5
                temp['distributor_rating'] = 5
                temp['capacity_rating'] = 5
                temp['market_rating'] = 5
                temp['fare_rating'] = 5
                temp['product_rating'] = 5
            try:
                temp['pax'] = pax_dict_ty[json_response['comp' + str(i) + 'carrier']]  # check
            except KeyError:
                temp['pax'] = "NA"
            temp['market_share_vtgt'] = json_response['comp' +
                                                      str(i) + 'marketsharevtgt']
            # print "cap_ty: ", cap_ty
            # print "cap_ly: ", cap_ly
            try:
                cap_adj = cap_ly[json_response['comp' + str(i) + 'carrier']] / cap_ty[
                    json_response['comp' + str(i) + 'carrier']]
                temp['pax_vlyr'] = (pax_dict_ty[json_response['comp' + str(i) + 'carrier']] * cap_adj - pax_dict_ly[
                    json_response['comp' + str(i) + 'carrier']]) * 100 / pax_dict_ly[
                                       json_response['comp' + str(i) + 'carrier']]
            except Exception:
                temp['pax_vlyr'] = "NA"
            temp['airline'] = json_response['comp' + str(i) + 'carrier']
            try:
                temp['lowest_filed_fare'] = dict(
                    total_fare=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], "lowest_fare_total"].values[0],
                    base_fare=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], "lowest_fare_base"].values[0],
                    fare_basis=lowest_filed_fare.loc[
                        lowest_filed_fare['carrier'] == json_response[
                            'comp' + str(i) + 'carrier'], "lowest_fare_fb"].values[
                        0],
                    tax=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], "lowest_fare_tax"].values[0],
                    yq=lowest_filed_fare.loc[
                        lowest_filed_fare['carrier'] == json_response[
                            'comp' + str(i) + 'carrier'], "lowest_fare_YQ"].values[
                        0],
                    yr=lowest_filed_fare.loc[
                        lowest_filed_fare['carrier'] == json_response[
                            'comp' + str(i) + 'carrier'], "lowest_fare_YR"].values[
                        0],
                    surcharge=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], "lowest_fare_surcharge"].values[0],
                    currency=lowest_filed_fare.loc[
                        lowest_filed_fare['carrier'] == json_response['comp' + str(i) + 'carrier'], "currency"].values[
                        0],
                    total_fare_gds=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], "lowest_fare_total_gds"].values[0],
                    base_fare_gds=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], "lowest_fare_base_gds"].values[0],
                    fare_basis_gds=lowest_filed_fare.loc[
                        lowest_filed_fare['carrier'] == json_response[
                            'comp' + str(i) + 'carrier'], "lowest_fare_fb_gds"].values[
                        0],
                    tax_gds=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], "lowest_fare_tax_gds"].values[0],
                    yq_gds=lowest_filed_fare.loc[
                        lowest_filed_fare['carrier'] == json_response[
                            'comp' + str(i) + 'carrier'], "lowest_fare_YQ_gds"].values[
                        0],
                    yr_gds=lowest_filed_fare.loc[
                        lowest_filed_fare['carrier'] == json_response[
                            'comp' + str(i) + 'carrier'], "lowest_fare_YR_gds"].values[
                        0],
                    surcharge_gds=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], "lowest_fare_surcharge_gds"].values[0],
                    currency_gds=lowest_filed_fare.loc[
                        lowest_filed_fare['carrier'] == json_response[
                            'comp' + str(i) + 'carrier'], "currency_gds"].values[
                        0],
                )
            except Exception:
                temp['lowest_filed_fare'] = dict(
                    total_fare="NA",
                    base_fare="NA",
                    tax="NA",
                    fare_basis='NA',
                    yq="NA",
                    yr="NA",
                    surcharge="NA",
                    currency="NA",
                    total_fare_gds="NA",
                    base_fare_gds="NA",
                    tax_gds="NA",
                    fare_basis_gds='NA',
                    yq_gds="NA",
                    yr_gds="NA",
                    surcharge_gds="NA",
                    currency_gds="NA",
                )
            except IndexError:
                temp['lowest_filed_fare'] = dict(
                    total_fare="NA",
                    base_fare="NA",
                    tax="NA",
                    fare_basis='NA',
                    yq="NA",
                    yr="NA",
                    surcharge="NA",
                    currency="NA",
                    total_fare_gds="NA",
                    base_fare_gds="NA",
                    tax_gds="NA",
                    fare_basis_gds='NA',
                    yq_gds="NA",
                    yr_gds="NA",
                    surcharge_gds="NA",
                    currency_gds="NA",
                )
            try:
                temp['price_movement_filed'] = dict(
                    lowest_fare=temp['lowest_filed_fare'],
                    highest_fare=dict(
                        total_fare=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                            'comp' + str(i) + 'carrier'], "highest_fare_total"].values[0],
                        base_fare=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                            'comp' + str(i) + 'carrier'], "highest_fare_base"].values[0],
                        fare_basis=lowest_filed_fare.loc[
                            lowest_filed_fare['carrier'] == json_response[
                                'comp' + str(i) + 'carrier'], "highest_fare_fb"].values[
                            0],
                        tax=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                            'comp' + str(i) + 'carrier'], "highest_fare_tax"].values[0],
                        yq=lowest_filed_fare.loc[
                            lowest_filed_fare['carrier'] == json_response[
                                'comp' + str(i) + 'carrier'], "highest_fare_YQ"].values[
                            0],
                        yr=lowest_filed_fare.loc[
                            lowest_filed_fare['carrier'] == json_response[
                                'comp' + str(i) + 'carrier'], "highest_fare_YR"].values[
                            0],
                        surcharge=lowest_filed_fare.loc[lowest_filed_fare['carrier'] == json_response[
                            'comp' + str(i) + 'carrier'], "highest_fare_surcharge"].values[0],
                        currency=lowest_filed_fare.loc[
                            lowest_filed_fare['carrier'] == json_response[
                                'comp' + str(i) + 'carrier'], "currency"].values[
                            0],
                    )
                )
            except Exception:
                temp['price_movement_filed'] = dict(
                    lowest_fare=dict(
                        surcharge="NA",
                        total_fare="NA",
                        base_fare="NA",
                        yq="NA",
                        fare_basis="NA",
                        yr="NA",
                        tax="NA",
                    ),
                    highest_fare=dict(
                        surcharge="NA",
                        total_fare="NA",
                        base_fare="NA",
                        yq="NA",
                        fare_basis="NA",
                        yr="NA",
                        tax="NA",
                    )
                )
            except IndexError:
                temp['price_movement_filed'] = dict(
                    lowest_fare=dict(
                        surcharge="NA",
                        total_fare="NA",
                        base_fare="NA",
                        yq="NA",
                        fare_basis="NA",
                        yr="NA",
                        tax="NA",
                    ),
                    highest_fare=dict(
                        surcharge="NA",
                        total_fare="NA",
                        base_fare="NA",
                        yq="NA",
                        fare_basis="NA",
                        yr="NA",
                        tax="NA",
                    )
                )
            try:
                temp['most_available_fare'] = dict(
                    base_fare_ow=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], 'most_avail_fare_base_ow'].values[0],
                    frequency_ow=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], 'most_avail_fare_freq_ow'].values[0],
                    tax_ow=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], 'most_avail_fare_tax_ow'].values[0],
                    total_fare_ow=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], 'most_avail_fare_total_ow'].values[0],
                    total_count_ow=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], 'most_avail_fare_count_ow'].values[0],
                    currency=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], 'currency'].values[0],
                    base_fare_rt=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], 'most_avail_fare_base_rt'].values[0],
                    frequency_rt=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], 'most_avail_fare_freq_rt'].values[0],
                    tax_rt=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], 'most_avail_fare_tax_rt'].values[0],
                    total_fare_rt=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], 'most_avail_fare_total_rt'].values[0],
                    total_count_rt=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], 'most_avail_fare_count_rt'].values[0],
                    fare_basis_rt=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], 'fare_basis_rt'].values[0],
                    fare_basis_ow=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], 'fare_basis_ow'].values[0],
                    observation_date_rt=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], 'observation_date_rt'].values[0],
                    observation_date_ow=most_avail_df.loc[most_avail_df['carrier'] == json_response[
                        'comp' + str(i) + 'carrier'], 'observation_date_ow'].values[0],
                )
            except Exception:
                temp['most_available_fare'] = dict(
                    base_fare_ow="NA",
                    frequency_ow="NA",
                    tax_ow="NA",
                    total_fare_ow="NA",
                    total_count_ow="NA",
                    currency="NA",
                    base_fare_rt="NA",
                    frequency_rt="NA",
                    tax_rt="NA",
                    total_fare_rt="NA",
                    total_count_rt="NA",
                    fare_basis_ow="NA",
                    fare_basis_rt="NA",
                    observation_date_ow="NA",
                    observation_date_rt="NA"
                )
            except IndexError:
                temp['most_available_fare'] = dict(
                    base_fare_ow="NA",
                    frequency_ow="NA",
                    tax_ow="NA",
                    total_fare_ow="NA",
                    total_count_ow="NA",
                    currency="NA",
                    base_fare_rt="NA",
                    frequency_rt="NA",
                    tax_rt="NA",
                    total_fare_rt="NA",
                    total_count_rt="NA",
                    fare_basis_ow="NA",
                    fare_basis_rt="NA"
                )
            comp_data.append(temp)
        return host_data, comp_data

    @staticmethod
    @measure(JUPITER_LOGGER)
    def get_module_name():
        return inspect.stack()[1][3]

    @staticmethod
    @measure(JUPITER_LOGGER)
    def get_arg_lists(frame):
        args, _, _, values = inspect.getargvalues(frame)
        argument_name_list = []
        argument_value_list = []
        for k in args:
            argument_name_list.append(k)
            argument_value_list.append(values[k])
        return argument_name_list, argument_value_list

    @staticmethod
    @measure(JUPITER_LOGGER)
    def append_error(e, esub):
        e.append_to_error_object_list(esub)


#   ************************************    DATA CHANGE TRIGGERS CLASSES    *******************************************


class competitor_market_share_change(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(
            competitor_market_share_change,
            self).__init__(triggering_event_id)

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        from analytics_functions import get_calender_month
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        desc = ["Competitor Market Share Change",
                '_',
                "ID - ",
                self.trigger_id,
                '_',
                "Carrier - ",
                str(self.old_doc_data['airline'].encode()),
                '_',
                "Departure Month : ",
                str(get_calender_month(self.old_doc_data['month'])),
                ' ',
                str(self.old_doc_data['year']),
                '_'
                "Market Share(Prev) - ",
                str(self.old_doc_data['market_share']),
                '_',
                "Market Share(Current) - ",
                str(self.new_doc_data['market_share']),
                '_',
                'Thresholds(',
                self.threshold_type,
                ')',
                ' - ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Events - ',
                events,
                '_',
                'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)]
        self.desc = ''.join(desc).encode('utf-8')


class host_market_share_change(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(host_market_share_change, self).__init__(triggering_event_id)

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        from analytics_functions import get_calender_month
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        desc = ["Host Market Share Change",
                '_',
                "ID - ",
                self.trigger_id,
                '_',
                "Departure Month : ",
                str(get_calender_month(self.old_doc_data['month'])),
                ' ',
                str(self.old_doc_data['year']),
                '_'
                "Market Share(Prev) - ",
                str(self.old_doc_data['market_share']),
                '_',
                "Market Share(Current) - ",
                str(self.new_doc_data['market_share']),
                '_',
                'Thresholds(',
                str(self.threshold_type),
                ')',
                ' - ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Events - ',
                events,
                '_',
                'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)]
        self.desc = ''.join(desc).encode('utf-8')


class competitor_airline_capacity_percentage_change(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(competitor_airline_capacity_percentage_change,
              self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        from analytics_functions import get_calender_month
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        desc = ["Competitor Capacity Change",
                '_',
                "ID - ",
                self.trigger_id,
                '_',
                "Departure Month : ",
                str(get_calender_month(self.old_doc_data['month'])),
                ' ',
                str(self.old_doc_data['year']),
                '_'
                "Capacity(Prev) - ",
                str(self.old_doc_data['capacity']),
                '_',
                "Capacity(Current) - ",
                str(self.new_doc_data['capacity']),
                '_',
                'Thresholds(',
                self.threshold_type,
                ')',
                ' - ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Events - ',
                events,
                '_',
                'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)]
        self.desc = ''.join(desc).encode('utf-8')


class host_airline_capacity_percentage_change(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(host_airline_capacity_percentage_change,
              self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        from analytics_functions import get_calender_month
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        desc = ["Host Capacity Change",
                '_',
                "ID - ",
                self.trigger_id,
                '_',
                "Departure Month : ",
                str(get_calender_month(self.old_doc_data['month'])),
                ' ',
                str(self.old_doc_data['year']),
                '_'
                "Capacity(Prev) - ",
                str(self.old_doc_data['capacity']),
                '_',
                "Capacity(Current) - ",
                str(self.new_doc_data['capacity']),
                '_',
                'Thresholds(',
                self.threshold_type,
                ')',
                ' - ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Events - ',
                events,
                '_',
                'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)]
        self.desc = ''.join(desc).encode('utf-8')


class competitor_rating_change(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(competitor_rating_change, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        desc = ["Competitor Rating Change",
                '_',
                "ID - ",
                self.trigger_id,
                '_',
                "Carrier - ",
                self.old_doc_data['airline'].encode(),
                '_',
                "Rating(Prev) - ",
                str(self.old_doc_data['rating']),
                '_',
                "Rating(Current) - ",
                str(self.new_doc_data['rating']),
                '_',
                'Thresholds : ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Events - ',
                events,
                '_',
                'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time),
                '_',
                "Departure Period(Assumed) : ",
                self.triggering_data['dep_date_start'],
                ' to ',
                self.triggering_data['dep_date_end']]
        self.desc = ' '.join(desc).encode('utf-8')


class host_rating_change(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(host_rating_change, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        desc = ["Host Rating Change",
                '_',
                "ID - ",
                self.trigger_id,
                '_',
                "Rating(Prev) - ",
                self.old_doc_data['rating'],
                '_',
                "Rating(Current) - ",
                self.new_doc_data['rating'],
                '_',
                'Thresholds : ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time),
                '_',
                'Events - ',
                events,
                '_',
                "Departure Period(Assumed) : ",
                self.triggering_data['dep_date_start'],
                ' to ',
                self.triggering_data['dep_date_end']]
        self.desc = ' '.join(desc).encode('utf-8')


class competitor_new_entry(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(competitor_new_entry, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin+self.destination, db=db)
        desc = ["Competitor Entry",
                '_',
                "ID - ",
                self.trigger_id,
                '_',
                "Carrier - ",
                self.new_doc_data['airline'],
                '_',
                "Departure Period(Assumed) : ",
                self.triggering_data['dep_date_start'],
                ' to ',
                self.triggering_data['dep_date_end'],
                '_',
                'Events - ',
                events,
                '_',
                'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time),
                ]
        self.desc = ''.join(desc).encode('utf-8')



class competitor_exit(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(competitor_exit, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        desc = ["Competitor Exit",
                '_',
                "ID - ",
                self.trigger_id,
                '_',
                "Carrier - ",
                self.new_doc_data['airline'],
                '_',
                "Departure Period(Assumed) : ",
                self.triggering_data['dep_date_start'],
                ' to ',
                self.triggering_data['dep_date_end'],
                '_',
                'Events - ',
                events,
                '_',
                'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time),
                ]
        self.desc = ''.join(desc).encode('utf-8')


class forecast_changes(trigger):
    """
    """

    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(forecast_changes, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        from analytics_functions import get_calender_month
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        if self.old_doc_data['param'] == 'pax':
            desc = [
                "Forecast Changes",
                '_',
                "ID - ", self.trigger_id,
                '_',
                "Departure Month : ", str(get_calender_month(self.old_doc_data['month'])), ' ',
                str(self.old_doc_data['year']),
                '_'
                "Pax(Prev) - ", str(self.old_doc_data['pax']),
                '_',
                "Pax(Current) - ", str(self.new_doc_data['pax']),
                '_',
                'Thresholds(', self.threshold_type, ')',
                ' - ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Events - ',
                events,
                '_',
                'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
            ]
        elif self.old_doc_data['param'] == 'revenue':
            desc = [
                "Forecast Changes",
                '_',
                "ID - ", self.trigger_id,
                '_',
                "Departure Month : ", str(get_calender_month(self.old_doc_data['month'])), ' ',
                str(self.old_doc_data['year']),
                '_'
                "Revenue(Prev) - ", str(self.old_doc_data['revenue']),
                '_',
                "Revenue(Current) - ", str(self.new_doc_data['revenue']),
                '_',
                'Thresholds(', self.threshold_type, ')',
                ' - ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Events - ',
                events,
                '_',
                'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
            ]
        else:
            desc = [
                "Forecast Changes",
                '_',
                "ID - ", self.trigger_id,
                '_',
                "Departure Month : ", str(get_calender_month(self.old_doc_data['month'])), ' ',
                str(self.old_doc_data['year']),
                '_'
                "Pax(Prev) - ", str(self.old_doc_data['pax']),
                '_',
                "Pax(Current) - ", str(self.new_doc_data['pax']),
                '_',
                "Revenue(Prev) - ", str(self.old_doc_data['revenue']),
                '_',
                "Revenue(Current) - ", str(self.new_doc_data['revenue']),
                '_',
                'Thresholds(', self.threshold_type, ')',
                ' - ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Events - ',
                events,
                '_',
                'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
            ]
        self.desc = ''.join(desc).encode('utf-8')


class competitor_price_change(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(competitor_price_change, self).__init__(triggering_event_id)

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        try:
            new_effective_to = datetime.datetime.strptime(self.new_doc_data['effective_to'], '%Y-%m-%d').strftime(
                '%d-%m-%Y')
        except TypeError:
            new_effective_to = str(self.new_doc_data['effective_to'])

        try:
            old_effective_to = datetime.datetime.strptime(self.old_doc_data['effective_to'], '%Y-%m-%d').strftime(
                '%d-%m-%Y')

        except TypeError:
            old_effective_to = str(self.old_doc_data['effective_to'])
        desc = [
            "Competitor Price Change",
            '_',
            "ID: ", self.trigger_id,
            '_',
            "Carrier: ", str(self.old_doc_data['carrier'].encode()),
            '_',
            "Competitor OD: ", str(self.old_doc_data['Competitor_OD'].encode()),
            '_',
            # "Tariff Code - ", str(self.old_doc_data['tariff_code'].encode()),
            # '_',
            "Farebasis: ", str(self.old_doc_data['fare_basis'].encode()),
            '_',
            # "RuleId - ", str(self.old_doc_data['fare_rule'].encode()),
            # '_',
            # "Footnote - ", str(self.old_doc_data['footnote'].encode()),
            # '_',
            "Oneway Return: ", str(self.old_doc_data['oneway_return'].encode()),
            '_',
            "Base Fare(Old): ", str(self.old_doc_data['fare']),
            '_',
            "Base Fare(New): ", str(self.new_doc_data['fare']),
            '_',
            "Effective Dates(Old): ",
            datetime.datetime.strptime(self.old_doc_data['effective_from'], '%Y-%m-%d').strftime('%d-%m-%Y') + " to " +
            old_effective_to,
            '_',
            "Effective Dates(New): ",
            datetime.datetime.strptime(self.new_doc_data['effective_from'], '%Y-%m-%d').strftime('%d-%m-%Y') + " to " +
            new_effective_to,
            '_',
            "Departure Period: ",
            datetime.datetime.strptime(self.new_doc_data['dep_from'], '%Y-%m-%d').strftime('%d-%m-%Y') + " to " +
            datetime.datetime.strptime(self.new_doc_data['dep_to'], '%Y-%m-%d').strftime('%d-%m-%Y'),
            '_',
            'Percentage Change: ', str(round(self.new_doc_data['pct_chng'])),
            '_',
            'Thresholds(', self.threshold_type, ')',
            ' - ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
            'Events: ',
            events,
            '_',
            'Raised On: ' + datetime.datetime.strptime(self.triggering_event_date, '%Y-%m-%d').strftime('%d-%m-%Y') +
            ' at ' + str(self.triggering_event_time)
        ]
        self.desc = ''.join(desc).encode('utf-8')


class promotions(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(promotions, self).__init__(triggering_event_id)

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        desc = [
            "Promotions",
            '_',
            "ID: ", self.trigger_id,
            '_',
            "Carrier: ", str(self.old_doc_data['carrier'].encode()),
            '_',
            "OD: ", str(self.old_doc_data['OD'].encode()),
            '_',
            "Fare(Old): ", str(self.old_doc_data['Fare']),
            '_',
            "Fare(New): ", str(self.new_doc_data['Fare']),
            '_',
            'Thresholds(', self.threshold_type, ')',
            ' _ ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
            'Events: ',
            events,
            '_',
            'Raised On: ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
        ]
        self.desc = ''.join(desc).encode('utf-8')


#   *************************************   DATA LEVEL TRIGGERS   ********


class booking_changes_rolling(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(booking_changes_rolling, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        desc = ["Bookings Changes Rolling",
                '_',
                "ID: ", self.trigger_id,
                '_',
                'Bookings TWk: ',
                str(round((self.new_doc_data['bookings']),
                          3)),
                '_',
                "Departure Period: ",
                datetime.datetime.strptime(self.triggering_data['dep_date_start'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                ' to ',
                datetime.datetime.strptime(self.triggering_data['dep_date_end'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                '_'
                'Bookings LWk: ',
                str(round(self.old_doc_data['bookings'],
                          3)),
                '_',
                "Departure Period: ",
                datetime.datetime.strptime(self.triggering_data['dep_date_start_lw'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                ' to ',
                datetime.datetime.strptime(self.triggering_data['dep_date_end_lw'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                '_'
                'Thresholds: ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Events: ',
                events,
                '_',
                'Raised On: ' + datetime.datetime.strptime(self.triggering_event_date,
                                                           '%Y-%m-%d').strftime('%d-%m-%Y') + ' at ' + str(
                    self.triggering_event_time)]
        self.desc = ''.join(desc).encode('utf-8')


class pax_changes_rolling(trigger):

    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(pax_changes_rolling, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        desc = ["Pax Changes Rolling",
                '_',
                "ID: ", self.trigger_id,
                '_',
                'Pax LWk: ',
                str(round((self.old_doc_data['pax']),
                          3)),
                '_',
                "Departure Period: ",
                datetime.datetime.strptime(self.triggering_data['dep_date_start'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                ' to ',
                datetime.datetime.strptime(self.triggering_data['dep_date_end'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                '_',
                'Pax TWk: ',
                str(round(self.new_doc_data['pax'],
                          3)),
                '_',
                "Departure Period: ",
                datetime.datetime.strptime(self.triggering_data['dep_date_start_lw'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                ' to ',
                datetime.datetime.strptime(self.triggering_data['dep_date_end_lw'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                '_'
                'Thresholds: ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Events: ',
                events,
                '_',
                'Raised On: ' + datetime.datetime.strptime(self.triggering_event_date,
                                                           '%Y-%m-%d').strftime('%d-%m-%Y') + ' at ' + str(
                    self.triggering_event_time)]
        self.desc = ''.join(desc).encode('utf-8')


class revenue_changes_rolling(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(revenue_changes_rolling, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        desc = ["Revenue Changes Rolling",
                '_',
                "ID: ", self.trigger_id,
                '_',
                'Revenue LWk: ',
                str(round((self.old_doc_data['revenue']),
                          3)),
                '_',
                "Departure Period: ",
                datetime.datetime.strptime(self.triggering_data['dep_date_start'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                ' to ',
                datetime.datetime.strptime(self.triggering_data['dep_date_end'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                '_'
                'Revenue TWk: ',
                str(round(self.new_doc_data['revenue'],
                          3)),
                '_',
                "Departure Period: ",
                datetime.datetime.strptime(self.triggering_data['dep_date_start_lw'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                ' to ',
                datetime.datetime.strptime(self.triggering_data['dep_date_end_lw'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                '_',
                'Thresholds: ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Events: ',
                events,
                '_',
                'Raised On: ' + datetime.datetime.strptime(self.triggering_event_date,
                                                           '%Y-%m-%d').strftime('%d-%m-%Y') + ' at ' + str(
                    self.triggering_event_time)]
        self.desc = ''.join(desc).encode('utf-8')


class yield_changes_rolling(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(yield_changes_rolling, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        try:
            old_yield = str(round((self.old_doc_data['yield']),
                                  3))
        except TypeError:
            old_yield = "NA"
        try:
            new_yield = str(round((self.new_doc_data['yield']),
                                  3))
        except TypeError:
            new_yield = "NA"
        desc = ["Yield Changes Rolling",
                '_',
                "ID: ", self.trigger_id,
                '_',
                'Yield TWk: ',
                new_yield,
                '_',
                "Departure Period: ",
                datetime.datetime.strptime(self.triggering_data['dep_date_start'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                ' to ',
                datetime.datetime.strptime(self.triggering_data['dep_date_end'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                '_'
                'Yield LWk: ',
                old_yield,
                '_',
                "Departure Period: ",
                datetime.datetime.strptime(self.triggering_data['dep_date_start_lw'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                ' to ',
                datetime.datetime.strptime(self.triggering_data['dep_date_end_lw'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                '_',
                'Thresholds: ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Events: ',
                events,
                '_',
                'Raised On: ' + datetime.datetime.strptime(self.triggering_event_date,
                                                           '%Y-%m-%d').strftime('%d-%m-%Y') + ' at ' + str(
                    self.triggering_event_time)]
        self.desc = ''.join(desc).encode('utf-8')


class booking_changes_weekly(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(booking_changes_weekly, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        desc = ["Booking Changes Weekly",
                "_",
                "ID: ",
                self.trigger_id,
                '_',
                "Departure Period: ",
                datetime.datetime.strptime(self.triggering_data['dep_date_start'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                ' to ',
                datetime.datetime.strptime(self.triggering_data['dep_date_end'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                '_',
                'Bkgs LWk: ',
                str(round((self.old_doc_data['bookings']),
                          3)),
                '_',
                'Bkgs TWk: ',
                str(round(self.new_doc_data['bookings'],
                          3)),
                '_',
                'Thresholds: ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Events: ',
                events,
                '_',
                'Raised On: ' + datetime.datetime.strptime(self.triggering_event_date,
                                                           '%Y-%m-%d').strftime('%d-%m-%Y') + ' at ' + str(
                    self.triggering_event_time)]
        self.desc = ''.join(desc).encode('utf-8')


class pax_changes_weekly(trigger):

    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(pax_changes_weekly, self).__init__(triggering_event_id)
        # self.do_analysis()
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        desc = ["Pax Changes Weekly",
                "_",
                "ID: ",
                self.trigger_id,
                '_',
                "Departure Period: ",
                datetime.datetime.strptime(self.triggering_data['dep_date_start'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                ' to ',
                datetime.datetime.strptime(self.triggering_data['dep_date_end'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                '_',
                'Pax LWk: ',
                str(round((self.old_doc_data['pax']),
                          3)),
                '_',
                'Pax TWk: ',
                str(round(self.new_doc_data['pax'],
                          3)),
                '_',
                'Thresholds: ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Events: ',
                events,
                '_',
                'Raised On: ' + datetime.datetime.strptime(self.triggering_event_date,
                                                           '%Y-%m-%d').strftime('%d-%m-%Y') + ' at ' + str(
                    self.triggering_event_time)]
        self.desc = ''.join(desc).encode('utf-8')


class revenue_changes_weekly(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(revenue_changes_weekly, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        desc = ["Revenue Changes Weekly",
                "_",
                "ID: ",
                self.trigger_id,
                '_',
                "Departure Period: ",
                datetime.datetime.strptime(self.triggering_data['dep_date_start'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                ' to ',
                datetime.datetime.strptime(self.triggering_data['dep_date_end'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                '_',
                'Revenue LWk: ',
                str(round((self.old_doc_data['revenue']),
                          3)),
                '_',
                'Revenue TWk: ',
                str(round(self.new_doc_data['revenue'],
                          3)),
                '_',
                'Thresholds: ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Events: ',
                events,
                '_',
                'Raised On: ' + datetime.datetime.strptime(self.triggering_event_date,
                                                           '%Y-%m-%d').strftime('%d-%m-%Y') + ' at ' + str(
                    self.triggering_event_time)]
        self.desc = ''.join(desc).encode('utf-8')


class yield_changes_weekly(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(yield_changes_weekly, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        try:
            old_yield = str(round((self.old_doc_data['yield']),
                                  3))
        except TypeError:
            old_yield = "NA"
        try:
            new_yield = str(round((self.new_doc_data['yield']),
                                  3))
        except TypeError:
            new_yield = "NA"
        desc = ["Yield Changes Weekly",
                "_",
                "ID: ",
                self.trigger_id,
                '_',
                "Departure Period: ",
                datetime.datetime.strptime(self.triggering_data['dep_date_start'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                ' to ',
                datetime.datetime.strptime(self.triggering_data['dep_date_end'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                '_',
                'Yield LWk: ',
                old_yield,
                '_',
                'Yield TWk: ',
                new_yield,
                '_',
                'Thresholds: ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Events: ',
                events,
                '_',
                'Raised On: ' + datetime.datetime.strptime(self.triggering_event_date,
                                                           '%Y-%m-%d').strftime('%d-%m-%Y') + ' at ' + str(
                    self.triggering_event_time)]
        self.desc = ''.join(desc).encode('utf-8')


class booking_changes_VLYR(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(booking_changes_VLYR, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        desc = ["Booking Changes VLYR",
                "_",
                "ID: ",
                self.trigger_id,
                "_",
                "Departure Period: ",
                datetime.datetime.strptime(self.triggering_data['dep_date_start'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                ' to ',
                datetime.datetime.strptime(self.triggering_data['dep_date_end'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                '_',
                'Bkgs Last Year: ',
                str(self.old_doc_data['bookings']),
                '_',
                'Capacity Last Year: ',
                str(self.old_doc_data['capacity']),
                '_',
                'Capacity This Year: ',
                str(self.new_doc_data['capacity']),
                '_',
                'Expected Bkgs This Year: ',
                str(self.new_doc_data['bookings_expected']),
                '_',
                'Actual Bkgs This Year: ',
                str(self.new_doc_data['bookings']),
                '_',
                'Thresholds: ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Events: ',
                events,
                '_',
                'Raised On: ' + datetime.datetime.strptime(self.triggering_event_date,
                                                           '%Y-%m-%d').strftime('%d-%m-%Y') + ' at ' + str(
                    self.triggering_event_time)]
        self.desc = ''.join(desc).encode('utf-8')


class pax_changes_VLYR(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(pax_changes_VLYR, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        desc = ["Pax Changes VLYR",
                "_",
                "ID: ",
                self.trigger_id,
                "_",
                "Departure Period: ",
                datetime.datetime.strptime(self.triggering_data['dep_date_start'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                ' to ',
                datetime.datetime.strptime(self.triggering_data['dep_date_end'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                '_',
                'Pax Last Year: ',
                str(self.old_doc_data['pax']),
                '_',
                # 'Capacity Last Year: ',
                # str(self.old_doc_data['capacity']),
                # '_',
                # 'Capacity This Year: ',
                # str(self.new_doc_data['capacity']),
                # '_',
                # 'Expected Pax This Year: ',
                # str(self.new_doc_data['pax_expected']),
                # '_',
                'Pax This Year: ',
                str(self.new_doc_data['pax']),
                '_',
                'Thresholds: ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Events: ',
                events,
                '_',
                'Raised On: ' + datetime.datetime.strptime(self.triggering_event_date,
                                                           '%Y-%m-%d').strftime('%d-%m-%Y') + ' at ' + str(
                    self.triggering_event_time)]
        self.desc = ''.join(desc).encode('utf-8')


class revenue_changes_VLYR(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(revenue_changes_VLYR, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        desc = ["Revenue Changes VLYR",
                "_",
                "ID: ",
                self.trigger_id,
                "_",
                "Departure Period: ",
                datetime.datetime.strptime(self.triggering_data['dep_date_start'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                ' to ',
                datetime.datetime.strptime(self.triggering_data['dep_date_end'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                '_',
                'Revenue Last Year: ',
                str(self.old_doc_data['revenue']),
                '_',
                # 'Capacity Last Year: ',
                # str(self.old_doc_data['capacity']),
                # '_',
                # 'Capacity This Year: ',
                # str(self.new_doc_data['capacity']),
                # '_',
                # 'Expected Revenue This Year: ',
                # str(self.new_doc_data['revenue_expected']),
                # '_',
                'Revenue This Year: ',
                str(self.new_doc_data['revenue']),
                '_',
                'Thresholds: ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Events: ',
                events,
                '_',
                'Raised On: ' + datetime.datetime.strptime(self.triggering_event_date,
                                                           '%Y-%m-%d').strftime('%d-%m-%Y') + ' at ' + str(
                    self.triggering_event_time)]
        self.desc = ''.join(desc).encode('utf-8')


class yield_changes_VLYR(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(yield_changes_VLYR, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        try:
            old_yield = str(round((self.old_doc_data['yield']),
                                  3))
        except TypeError:
            old_yield = "NA"
        # try:
        #     exp_yield = str(round((self.new_doc_data['yield_expected']),
        #                   3))
        # except TypeError:
        #     exp_yield = "NA"
        try:
            new_yield = str(round((self.new_doc_data['yield']),
                                  3))
        except TypeError:
            new_yield = "NA"
        desc = ["Yield Changes VLYR",
                "_",
                "ID: ",
                self.trigger_id,
                "_",
                "Departure Period: ",
                datetime.datetime.strptime(self.triggering_data['dep_date_start'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                ' to ',
                datetime.datetime.strptime(self.triggering_data['dep_date_end'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                '_',
                'Yield Last Year: ',
                old_yield,
                '_',
                # 'Capacity Last Year: ',
                # str(self.old_doc_data['capacity']),
                # '_',
                # 'Capacity This Year: ',
                # str(self.new_doc_data['capacity']),
                # '_',
                # 'Expected Yield This Year: ',
                # exp_yield,
                # '_',
                'Yield This Year: ',
                new_yield,
                '_',
                'Thresholds: ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Events: ',
                events,
                '_',
                'Raised On: ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)]
        self.desc = ''.join(desc).encode('utf-8')


class booking_changes_VTGT(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(booking_changes_VTGT, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        desc = ["Booking Changes VTGT",
                "_",
                "ID: ",
                self.trigger_id,
                "_",
                "Departure Period: ",
                datetime.datetime.strptime(self.triggering_data['dep_date_start'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                ' to ',
                datetime.datetime.strptime(self.triggering_data['dep_date_end'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                '_',
                'Bkgs VTGT CY: ',
                str(round((self.old_doc_data['percent_acheived']),
                          3)),
                '_',
                'Bkgs VTGT LY: ',
                str(round(self.new_doc_data['percent_acheived'],
                          3)),
                '_',
                'Thresholds: ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Events: ',
                events,
                '_',
                'Raised On: ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)]
        self.desc = ''.join(desc).encode('utf-8')


class pax_changes_VTGT(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(pax_changes_VTGT, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        desc = ["Pax Changes VTGT",
                "_",
                "ID: ",
                self.trigger_id,
                "_",
                "Departure Period: ",
                datetime.datetime.strptime(self.triggering_data['dep_date_start'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                ' to ',
                datetime.datetime.strptime(self.triggering_data['dep_date_end'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                '_',
                'Pax Target: ',
                str(round((self.old_doc_data['target_pax']),
                          3)),
                '_',
                'Pax Forecast: ',
                str(round(self.new_doc_data['forecast_pax'],
                          3)),
                '_',
                'Thresholds: ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Events: ',
                events,
                '_',
                'Raised On: ' + datetime.datetime.strptime(self.triggering_event_date,
                                                           '%Y-%m-%d').strftime('%d-%m-%Y') + ' at ' + str(
                    self.triggering_event_time)]
        self.desc = ''.join(desc).encode('utf-8')


class revenue_changes_VTGT(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(revenue_changes_VTGT, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        desc = ["Revenue Changes VTGT",
                "_",
                "ID: ",
                self.trigger_id,
                "_",
                "Departure Period: ",
                datetime.datetime.strptime(self.triggering_data['dep_date_start'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                ' to ',
                datetime.datetime.strptime(self.triggering_data['dep_date_end'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                '_',
                'Revenue Target: ',
                str(round((self.old_doc_data['target_revenue']),
                          3)),
                '_',
                'Revenue Forecast: ',
                str(round(self.new_doc_data['forecast_revenue'],
                          3)),
                '_',
                'Thresholds: ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Events: ',
                events,
                '_',
                'Raised On: ' + datetime.datetime.strptime(self.triggering_event_date,
                                                           '%Y-%m-%d').strftime('%d-%m-%Y') + ' at ' + str(
                    self.triggering_event_time)]
        self.desc = ''.join(desc).encode('utf-8')


class yield_changes_VTGT(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(yield_changes_VTGT, self).__init__(triggering_event_id)

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        # try:
        #     old_yield = str(round((self.old_doc_data['yield']),
        #                           3))
        # except TypeError:
        #     old_yield = "NA"
        # try:
        #     target_yield = str(round((self.old_doc_data['target_yield']),
        #                              3))
        # except TypeError:
        #     target_yield = "NA"
        desc = ["Yield Changes VTGT",
                "_",
                "ID: ",
                self.trigger_id,
                "_",
                "Departure Period: ",
                datetime.datetime.strptime(self.triggering_data['dep_date_start'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                ' to ',
                datetime.datetime.strptime(self.triggering_data['dep_date_end'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                '_',
                'Yield Target: ',
                str(round((self.old_doc_data['target_yield']), 3)),
                '_',
                'Yield Forecast: ',
                str(round((self.new_doc_data['forecast_yield']), 3)),
                '_',
                'Thresholds: ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Events: ',
                events,
                '_',
                'Raised On: ' + datetime.datetime.strptime(self.triggering_event_date,
                                                           '%Y-%m-%d').strftime('%d-%m-%Y') + ' at ' + str(
                    self.triggering_event_time)]
        self.desc = ''.join(desc).encode('utf-8')


class lowest_fare_comparision(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(lowest_fare_comparision, self).__init__(triggering_event_id)

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        self.lower_threshold = self.triggering_data['condition']['lower_threshold']
        self.upper_threshold = self.triggering_data['condition']['upper_threshold']
        self.desc = ' '.join(["LFC: Lowest Fares Comparision"
                              '_',
                              'I: Internally Generated',
                              '_'
                              "Departure Period: ",
                              datetime.datetime.strptime(self.triggering_data['dep_date_start'],
                                                         '%Y-%m-%d').strftime('%d-%m-%Y'),
                              ' to ',
                              datetime.datetime.strptime(self.triggering_data['dep_date_end'],
                                                         '%Y-%m-%d').strftime('%d-%m-%Y'),
                              '_',
                              'Competitor in comparision: ',
                              self.triggering_data['condition']['airline'],
                              '_',
                              'Host Lowest Fare: ',
                              str(self.old_doc_data['host_fare']),
                              '_',
                              'Competitor Lowest Fare: ',
                              str(self.old_doc_data['comp_fare']),
                              '_',
                              'Thresholds:' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                              '_',
                              'Events - ',
                              events,
                              '_',
                              'Raised On ' + datetime.datetime.strptime(self.triggering_event_date,
                                                                        '%Y-%m-%d').strftime('%d-%m-%Y') + ' at ' + str(
                                  self.triggering_event_time)])
        # self.desc = ''.join(desc).encode('utf-8')


class opportunities(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(opportunities, self).__init__(triggering_event_id)

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        self.lower_threshold = self.triggering_data['lower_threshold']
        self.upper_threshold = self.triggering_data['upper_threshold']
        desc = ' '.join(["Opportunities (MS vs FMS)",
                         '_',
                         "ID: ", self.trigger_id,
                         '_'
                         "Departure Period: ",
                         datetime.datetime.strptime(self.triggering_data['dep_date_start'],
                                                    '%Y-%m-%d').strftime('%d-%m-%Y'),
                         ' to ',
                         datetime.datetime.strptime(self.triggering_data['dep_date_end'],
                                                    '%Y-%m-%d').strftime('%d-%m-%Y'),
                         '_',
                         'MS: ',
                         str(round(self.old_doc_data['market_share'],
                                   2)),
                         '_',
                         'FMS: ',
                         str(round(self.old_doc_data['FMS'],
                                   2)),
                         '_',
                         'Thresholds: ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                         '_',
                         'Events: ',
                         events,
                         '_',
                         'Raised On: ' + datetime.datetime.strptime(self.triggering_event_date,
                                                                    '%Y-%m-%d').strftime('%d-%m-%Y') + ' at ' + str(
                             self.triggering_event_time)])
        self.desc = desc


class forecast_changes_VTGT(trigger):
    """
    """

    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(forecast_changes_VTGT, self).__init__(triggering_event_id)
        # self.do_analysis()
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        desc = ["Forecast Changes VTGT",
                '_',
                "ID - ",
                self.trigger_id,
                '_',
                "Departure Period: ",
                datetime.datetime.strptime(self.triggering_data['dep_date_start'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                ' to ',
                datetime.datetime.strptime(self.triggering_data['dep_date_end'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                '_',
                "Forecast Pax - ",
                str(self.new_doc_data['forecast_pax']),
                '_',
                "Target Pax - ",
                str(self.old_doc_data['target_pax']),
                '_',
                'Thresholds(',
                self.threshold_type,
                ')',
                ' - ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Events - ',
                events,
                '_',
                'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)]
        self.desc = ''.join(desc).encode('utf-8')


# ***************************************** TREND TRIGGERS ********************************** #


class pax_trend(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(pax_trend, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        desc = ["Pax Trend",
                '_',
                "ID: ", self.trigger_id,
                '_',
                'Pax TY: ',
                str((self.new_doc_data['pax_ty'])),
                '_',
                "Departure Date: ",
                datetime.datetime.strptime(self.triggering_data['dep_date_start'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                '_'
                'Pax LY: ',
                str((self.old_doc_data['pax_ly'])),
                '_',
                "Transaction Period: ",
                str(self.triggering_data['trx_dates']),
                '_'
                'Thresholds: ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Events: ',
                events,
                '_',
                'Raised On: ' + datetime.datetime.strptime(self.triggering_event_date,
                                                           '%Y-%m-%d').strftime('%d-%m-%Y') + ' at ' + str(
                    self.triggering_event_time)]
        self.desc = ''.join(desc).encode('utf-8')


class market_share_trend(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(market_share_trend, self).__init__(triggering_event_id)

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        desc = ["Market Share Trend",
                '_',
                "ID: ", self.trigger_id,
                '_',
                'Market Share TY: ',
                str((self.new_doc_data['market_share_ty'])),
                '_',
                "Departure Period: ",
                datetime.datetime.strptime(self.triggering_data['dep_date_start'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                ' to ',
                datetime.datetime.strptime(self.triggering_data['dep_date_end'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                '_'
                'Market Share LY: ',
                str((self.old_doc_data['market_share_ly'])),
                '_',
                "Months: ",
                str(self.triggering_data['months']),
                '_'
                'Thresholds: ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Events: ',
                events,
                '_',
                'Raised On: ' + datetime.datetime.strptime(self.triggering_event_date,
                                                           '%Y-%m-%d').strftime('%d-%m-%Y') + ' at ' + str(
                    self.triggering_event_time)]
        self.desc = ''.join(desc).encode('utf-8')


class avg_fare_trend(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(avg_fare_trend, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        desc = ["Avg Fare Trend",
                '_',
                "ID: ", self.trigger_id,
                '_',
                'Avg Fare TY: ',
                str((self.new_doc_data['avg_fare_ty'])),
                '_',
                "Departure Date: ",
                datetime.datetime.strptime(self.triggering_data['dep_date_start'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                '_'
                'Avg Fare LY: ',
                str((self.old_doc_data['avg_fare_ly'])),
                '_',
                "Transaction Period: ",
                str(self.triggering_data['trx_dates']),
                '_'
                'Thresholds: ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Events: ',
                events,
                '_',
                'Raised On: ' + datetime.datetime.strptime(self.triggering_event_date,
                                                           '%Y-%m-%d').strftime('%d-%m-%Y') + ' at ' + str(
                    self.triggering_event_time)]
        self.desc = ''.join(desc).encode('utf-8')


class revenue_trend(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(revenue_trend, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        desc = ["Revenue Trend",
                '_',
                "ID: ", self.trigger_id,
                '_',
                'Revenue TY: ',
                str((self.new_doc_data['revenue_ty'])),
                '_',
                "Departure Date: ",
                datetime.datetime.strptime(self.triggering_data['dep_date_start'],
                                           '%Y-%m-%d').strftime('%d-%m-%Y'),
                '_'
                'Revenue LY: ',
                str((self.old_doc_data['revenue_ly'])),
                '_',
                "Transaction Period: ",
                str(self.triggering_data['trx_dates']),
                '_'
                'Thresholds: ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Events: ',
                events,
                '_',
                'Raised On: ' + datetime.datetime.strptime(self.triggering_event_date,
                                                           '%Y-%m-%d').strftime('%d-%m-%Y') + ' at ' + str(
                    self.triggering_event_time)]
        self.desc = ''.join(desc).encode('utf-8')


#   ******************************************** MANUAL TRIGGERS *********
# ***************************************** PROMOTION TRIGGERS ********************************** #

class new_promotions(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(new_promotions, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        # current_date = datetime.datetime.now().strftime('%d-%m-%Y')
        print "newdoc:", self.new_doc_data
        try:
            valid_from = datetime.datetime.strptime(self.new_doc_data['Valid_from'], "%Y-%m-%d").strftime("%d-%m-%Y")
        except:
            valid_from = ""
        desc = [
            "New Promotions Raised",
            '_',
            "ID: ", self.trigger_id,
            '_',
            'Airline: ', str((self.new_doc_data['Airline'])),
            '_',
            'OD: ', str((self.new_doc_data['OD_1'])),
            '_',
            'Promo Fare: ' + 'From ' + str(self.new_doc_data['Currency']) + ' ' + str((self.new_doc_data['Fare'])),
            '_',
            'Oneway/ Return: ', str((self.new_doc_data['Oneway/ Return'])),
            '_'
            'Compartment: ' + str((self.new_doc_data['compartment'])),
            '_',
            "Sale Period: ", valid_from + ' till ' + datetime.datetime.strptime(self.new_doc_data['Valid till'],
                                                                                '%Y-%m-%d').strftime('%d-%m-%Y'),
            '_',
            "Departure Period: ",
            datetime.datetime.strptime(self.new_doc_data['dep_date_from'], '%Y-%m-%d').strftime(
                '%d-%m-%Y') + ' to ' + datetime.datetime.strptime(self.new_doc_data['dep_date_to'],
                                                                  '%Y-%m-%d').strftime('%d-%m-%Y'),
            '_',
            'Events: ',
            events,
            '_',
            'Raised On: ' + datetime.datetime.strptime(self.triggering_event_date, '%Y-%m-%d').strftime(
                '%d-%m-%Y') + ' at ' + str(self.triggering_event_time) + ' Hrs ',
            '_'

        ]
        self.desc = ''.join(desc).encode('utf-8')


class promotions_fareschange(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(promotions_fareschange, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        try:
            valid_from = datetime.datetime.strptime(self.new_doc_data['Valid_from'], "%Y-%m-%d").strftime("%d-%m-%Y")
        except:
            valid_from = ""
        desc = [
            "Promotion Fares Changed",
            '_',
            "ID: ", self.trigger_id,
            '_',
            'Airline: ', str((self.new_doc_data['Airline'])),
            '_',
            'OD: ', str((self.new_doc_data['OD_1'])),
            '_',
            'Promo Fare: ' + 'From ' + str(self.new_doc_data['Currency']) + ' ' + str((self.new_doc_data['Fare'])),
            '_',
            'Oneway/ Return: ', str((self.new_doc_data['Oneway/ Return'])),
            '_'
            'Compartment: ' + str((self.new_doc_data['compartment'])),
            '_',
            "Sale Period: ", valid_from + ' till ' + datetime.datetime.strptime(self.new_doc_data['Valid till'],
                                                                                '%Y-%m-%d').strftime('%d-%m-%Y'),
            '_',
            "Departure Period: ",
            datetime.datetime.strptime(self.new_doc_data['dep_date_from'], '%Y-%m-%d').strftime(
                '%d-%m-%Y') + ' to ' + datetime.datetime.strptime(self.new_doc_data['dep_date_to'],
                                                                  '%Y-%m-%d').strftime('%d-%m-%Y'),
            '_',
            'Events: ',
            events,
            '_',
            'Raised On: ' + datetime.datetime.strptime(self.triggering_event_date, '%Y-%m-%d').strftime(
                '%d-%m-%Y') + ' at ' + str(self.triggering_event_time) + ' Hrs ',
            '_'
        ]
        self.desc = ''.join(desc).encode('utf-8')


class promotions_dateschange(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(promotions_dateschange, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        try:
            valid_from = datetime.datetime.strptime(self.new_doc_data['Valid_from'], "%Y-%m-%d").strftime("%d-%m-%Y")
        except:
            valid_from = ""
        desc = [
            "Promotion Dates Changed",
            '_',
            "ID: ", self.trigger_id,
            '_',
            'Airline: ', str((self.new_doc_data['Airline'])),
            '_',
            'OD: ', str((self.new_doc_data['OD_1'])),
            '_',
            'Promo Fare: ' + 'From ' + str(self.new_doc_data['Currency']) + ' ' + str((self.new_doc_data['Fare'])),
            '_',
            'Oneway/ Return: ', str((self.new_doc_data['Oneway/ Return'])),
            '_'
            'Compartment: ' + str((self.new_doc_data['compartment'])),
            '_',
            "Sale Period: ", valid_from + ' till ' + datetime.datetime.strptime(self.new_doc_data['Valid till'],
                                                                                '%Y-%m-%d').strftime('%d-%m-%Y'),
            '_',
            "Departure Period: ",
            datetime.datetime.strptime(self.triggering_data['dep_date_start'], '%Y-%m-%d').strftime(
                '%d-%m-%Y') + ' to ' + datetime.datetime.strptime(self.triggering_data['dep_date_end'],
                                                                  '%Y-%m-%d').strftime('%d-%m-%Y'),
            '_',
            'Events: ',
            events,
            '_',
            'Raised On: ' + datetime.datetime.strptime(self.triggering_event_date, '%Y-%m-%d').strftime(
                '%d-%m-%Y') + ' at ' + str(self.triggering_event_time) + ' Hrs ',
            '_'
        ]
        self.desc = ''.join(desc).encode('utf-8')


class promotions_ruleschange(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(promotions_ruleschange, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        try:
            valid_from = datetime.datetime.strptime(self.new_doc_data['Valid_from'], "%Y-%m-%d").strftime("%d-%m-%Y")
        except:
            valid_from = ""
        desc = [
            "Promotion Rules Changed",
            '_',
            "ID: ", self.trigger_id,
            '_',
            'Airline: ', str((self.new_doc_data['Airline'])),
            '_',
            'OD: ', str((self.new_doc_data['OD_1'])),
            '_',
            'Promo Fare: ' + 'From ' + str(self.new_doc_data['Currency']) + ' ' + str((self.new_doc_data['Fare'])),
            '_',
            'Oneway/ Return: ', str((self.new_doc_data['Oneway/ Return'])),
            '_'
            'Compartment: ' + str((self.new_doc_data['compartment'])),
            '_',
            "Sale Period: ", valid_from + ' till ' + datetime.datetime.strptime(self.new_doc_data['Valid till'],
                                                                                '%Y-%m-%d').strftime('%d-%m-%Y'),
            '_',
            "Departure Period: ",
            datetime.datetime.strptime(self.triggering_data['dep_date_start'], '%Y-%m-%d').strftime(
                '%d-%m-%Y') + ' to ' + datetime.datetime.strptime(self.triggering_data['dep_date_end'],
                                                                  '%Y-%m-%d').strftime('%d-%m-%Y'),
            '_',
            'Events: ',
            events,
            '_',
            'Raised On: ' + datetime.datetime.strptime(self.triggering_event_date, '%Y-%m-%d').strftime(
                '%d-%m-%Y') + ' at ' + str(self.triggering_event_time) + ' Hrs ',
            '_'
        ]
        self.desc = ''.join(desc).encode('utf-8')


#   ******************************************** MANUAL TRIGGERS ******************************************************


class manual(trigger):
    """
    """

    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(manual, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        desc = ['Manual Trigger',
                '_',
                "ID: ", self.trigger_id,
                '_',
                'Reason: ',
                self.old_doc_data['reason'],
                '_'
                "Departure Dates: ",
                str(datetime.datetime.strptime(str(self.triggering_data['dep_date_start']), "%Y-%m-%d").strftime(
                    "%d-%m-%Y")),
                ' to ',
                str(datetime.datetime.strptime(str(self.triggering_data['dep_date_end']), "%Y-%m-%d").strftime(
                    "%d-%m-%Y")),
                "_",
                'Events: ',
                events]
        self.desc = ''.join(desc).encode('utf-8')


class sales_request(trigger):
    """
    """

    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(sales_request, self).__init__(triggering_event_id)
        # Manualself.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)
        desc = [
            'Sales Request Trigger',
            '_',
            'Reason: ', self.old_doc_data['reason'],
            '_'
            "Departure Dates: ", str(self.triggering_data['dep_date_start']), ' to ',
            str(self.triggering_data['dep_date_end']),
            '_'
            'Work Package ID: ', str(self.old_doc_data['work_package_name']),
            "_",
            'Events: ',
            events
        ]
        self.desc = ''.join(desc).encode('utf-8')


class price_stability_index_change(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(price_stability_index_change, self).__init__(triggering_event_id)

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin + self.destination, db=db)

        desc = [
            "Price Stability Index Change",
            '_',
            "ID: ", self.trigger_id,
            '_',
            "Stability Index(Old): ", str(self.old_doc_data['index']),
            '_',
            "Stability Index(New): ", str(self.new_doc_data['index']),
            '_',
            "Departure Period: ",
            datetime.datetime.strptime(self.triggering_data['dep_date_start'], '%Y-%m-%d').strftime(
                '%d-%m-%Y') + " to " +
            datetime.datetime.strptime(self.triggering_data['dep_date_end'], '%Y-%m-%d').strftime('%d-%m-%Y'),
            '_',
            'Percentage Change: ', str(round(self.new_doc_data['pct_change'])),
            '_',
            'Thresholds(', self.threshold_type, ')',
            ' - ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
            'Events: ',
            events,
            '_',
            'Raised On: ' + datetime.datetime.strptime(self.triggering_event_date, '%Y-%m-%d').strftime('%d-%m-%Y') +
            ' at ' + str(self.triggering_event_time)
        ]
        self.desc = ''.join(desc).encode('utf-8')


class exchange_rate_change(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(exchange_rate_change, self).__init__(triggering_event_id)

    @measure(JUPITER_LOGGER)
    def get_desc(self, db):
        events = get_event_data(self.triggering_data['dep_date_start'], self.triggering_data['dep_date_end'],
                                od=self.origin+self.destination, db=db)

        desc = [
            "Exchange Rate Change",
            '_',
            "ID: ", self.trigger_id,
            '_',
            "Exchange Rate(Old): ", str(self.old_doc_data['rate']),
            '_',
            "Exchange Rate(New): ", str(self.new_doc_data['rate']),
            '_',
            "Departure Period: ",
            datetime.datetime.strptime(self.triggering_data['dep_date_start'], '%Y-%m-%d').strftime(
                '%d-%m-%Y') + " to " +
            datetime.datetime.strptime(self.triggering_data['dep_date_end'], '%Y-%m-%d').strftime('%d-%m-%Y'),
            '_',
            'Percentage Change: ', str(round(self.new_doc_data['pct_change'])),
            '_',
            'Thresholds(', self.threshold_type, ')',
            ' - ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
            'Events: ',
            events,
            '_',
            'Raised On: ' + datetime.datetime.strptime(self.triggering_event_date, '%Y-%m-%d').strftime('%d-%m-%Y') +
            ' at ' + str(self.triggering_event_time)
        ]
        self.desc = ''.join(desc).encode('utf-8')


if __name__ == "__main__":
    # from bson import ObjectId\
    ##BYMANISH
    client =  mongo_client()
    db=client[JUPITER_DB]

    p = pax_trend(ObjectId("5bc8a5c773311f2be469eb9c"))
    p.do_analysis(db=db)
    #temp = get_event_data("2017-10-01", "2017-10-31", "KTMDXB")
    #print temp
    client.close()
