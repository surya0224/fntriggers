import datetime
import inspect

import analytics_functions as af
from jupiter_AI import client, recommendation_lower_threshold, recommendation_upper_threshold, JUPITER_LOGGER
from jupiter_AI import network_level_params as net
from jupiter_AI.common import ClassErrorObject as error_class
from jupiter_AI.logutils import measure
from jupiter_AI.network_level_params import JUPITER_DB
from jupiter_AI.triggers.CategorizeRecommendation import Category
from jupiter_AI.triggers.workflow_parameters import update_host_workflow_params_comp,update_host_workflow_params_fb, update_market_workflow_params
#db = client[JUPITER_DB]

#################################################################
##### CLASS STRUCTURE OF TRIGGERS ###############################
#################################################################
'''
Class Structure
		trigger
			- data_change
				- competitor_change
					- #competitor_price_change
					- #competitor_market_share_change
					- #competitor_rating_change
					- competitor_airline_capacity_percent_change
					- competitor_exit
					- competitor_new_entry
				- host_change
					- #host_market_share_change
					- #host_rating_change
					- host_airline_capacity_percent_change
					- host_airline_objective_change
					- host_new_customer_segment
					- variation_to_revenue_targets
				- event_change
					- condition_of_market_change
					- type_of_market_change
					- exchange_rate_change
					- yq_trigger
					- event
					- cost_change
			- data_level
				- #booking_changes_weekly
				- #booking_changes_VLYR
				- #booking_changes_VTGT
				- #route_profitability_requirements
				- #pax_changes_weekly
				- #pax_changes_VLYR
				- #pax_changes_VTGT
				- #yield_changes_week
				- #yield_changes_VLYR
				- #yield_changes_VTGT
				- #opportunities
				- #revenue_changes_week
				- #revenue_change_VLYR
				- #revenue_changes_VTGT
				- forecast_changes
			- manual
				- percentage_change
					- adhoc_percentage_price_change
					- sales_review_request_percentage_change
				- absolute_change
					- adhoc_absolute_price_change
					- sales_review_request_absolute_change
				- direct
					- fairbasis_upload
					- sales_review_request_upload
				- no_price_action
					- sales_review_request_no_action
'''


class trigger(object):
    @measure(JUPITER_LOGGER)
    def do_analysis(self):
        """
        :return:
        """
        error_flag = False

        try:
            self.triggering_event()
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

        if not error_flag:
            try:
                self.get_host_farebasis()
            except error_class.ErrorObject as esub:
                self.append_error(self.errors, esub)
                if esub.error_level <= error_class.ErrorObject.WARNING:
                    error_flag = False
                elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL1:
                    error_flag = True
                elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL2:
                    error_flag = True
                    raise esub

        trigger_list = af.trigger_hierarchy(self)
        self.trigger_types(triggers_list=trigger_list)
        self.get_trigger_details(trigger_name=self.trigger_type)

        if not error_flag:
            self.check_min_interval()
            self.check_effective_dates()
            self.check_pricing_action()

        if not error_flag:
            # get_price_recommendation(object=self)
            self.get_desc()
        print error_flag
        print self.errors.__str__()
        print self.errors.error_list
        print self.errors.error_object_list
        if self.errors.error_list != [] or self.errors.error_object_list != []:
            db.JUP_DB_Error_Collection.insert_one({'triggering_event_id': self.triggering_event_id,
                                                   'error_descripetion': self.errors.__str__()})
            print 'Error Occured and Updated in dB'
        if not error_flag:
            self.update_db()

    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        '''
        Instaniating method for base class of triggers
        Calling of all the functions required for Analysis is done here
        '''
        self.errors = error_class.ErrorObject(0,
                                              self.get_module_name(),
                                              self.get_arg_lists(inspect.currentframe()))
        self.triggering_event_id = triggering_event_id
        self.airline = net.Host_Airline_Code
        self.process_start_date = datetime.datetime.now().strftime('%Y-%m-%d')
        self.process_start_time = datetime.datetime.now().strftime('%H:%M:%S.%f')
        # error_flag = False
        # print self.farebasis
        # print error_flag
        #
        # print error_flag
        # if not error_flag:
        #     self.competitor_pricing_data_func()
        #
        # if not error_flag:
        #     try:
        #         trigger_list = af.trigger_hierarchy(self)
        #     except error_class.ErrorObject as esub:
        #         self.append_error(self.errors, esub)
        #         if esub.error_level <= error_class.ErrorObject.WARNING:
        #             error_flag = False
        #         elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL1:
        #             error_flag = True
        #         elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL2:
        #             error_flag = True
        #             raise esub
        # print error_flag
        #
        # if not error_flag:
        #     try:
        #         self.trigger_types(trigger_list)
        #     except error_class.ErrorObject as esub:
        #         self.append_error(self.errors, esub)
        #         if esub.error_level <= error_class.ErrorObject.WARNING:
        #             error_flag = False
        #         elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL1:
        #             error_flag = True
        #         elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL2:
        #             error_flag = True
        #             raise esub
        # print error_flag
        #
        # # if not error_flag:
        # #     try:
        # #         self.pos_od_compartment_data()
        # #     except error_class.ErrorObject as esub:
        # #         self.append_error(self.errors, esub)
        # #         if esub.error_level <= error_class.ErrorObject.WARNING:
        # #             error_flag = False
        # #         elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL1:
        # #             error_flag = True
        # #         elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL2:
        # #             error_flag = True
        # #             raise esub
        # # print error_flag
        #
        # # if not error_flag:
        # #     error_flag = self.check_errors()
        # # print error_flag
        #
        # if not error_flag:
        #     self.check_min_interval()
        #     self.check_effective_dates()
        #     self.check_pricing_action()
        # print error_flag
        # # if not error_flag:
        # #     print 'For Bookings'
        # #     print self.pos,self.origin,self.destination,self.compartment,self.host_pricing_data['dep_date_from'],self.host_pricing_data['dep_date_to']
        # #     bookings_data = get_bookings_vlyr_vtgt(self.pos,
        # #                                            self.origin,
        # #                                            self.destination,
        # #                                            self.compartment,
        # #                                            dep_date_from=self.triggering_data['dep_date_start'],
        # #                                            dep_date_to=self.triggering_data['dep_date_end']
        # #                                            # dep_date_from=self.host_pricing_data['dep_date_from'],
        # #                                            # dep_date_to=self.host_pricing_data['dep_date_to']
        # #                                            )
        # #     print bookings_data
        # #     self.bookings_data = {'bookings': bookings_data['bookings'],
        # #                           'bookings_vlyr': bookings_data['bookings_vlyr'],
        # #                           'bookings_vtgt': bookings_data['bookings_vtgt']}
        #
        # print error_flag
        # print self.errors.__str__()
        # print self.errors.error_list
        # print self.errors.error_object_list
        # if self.errors.error_list != [] or self.errors.error_object_list != []:
        #     db.JUP_DB_Error_Collection.insert_one({'triggering_event_id': self.triggering_event_id,
        #                                            'error_descripetion': self.errors.__str__()})
        #     print 'Error Occured and Updated in dB'
        # if not error_flag:
        #     self.update_db()

    @measure(JUPITER_LOGGER)
    def triggering_event(self):
        '''
        Collects the following data relevant to trigger from collection
        '''
        cursor = db.JUP_DB_Triggering_Event.find({'_id': self.triggering_event_id})
        if cursor.count() == 1:
            p = cursor[0]
            print p
            self.triggering_event_date = p['date']
            self.triggering_event_time = p['time']
            self.trigger_id = p['trigger_id']
            self.triggering_data = p['triggering_data']
            self.comp_level_data = p['comp_level_data']
            self.cat_details = p['category_details']
            self.old_doc_data = p['old_doc_data']
            self.host_pricing_data = p['host_fare_doc']
            self.calc_price_recommendation = p['host_fare_doc']['recommended_fare']
            if self.calc_price_recommendation:
                if not p['host_fare_doc']['YQ']:
                    p['host_fare_doc']['YQ'] = 0
                if not p['host_fare_doc']['surcharge']:
                    p['host_fare_doc']['surcharge'] = 0
                if not p['host_fare_doc']['taxes']:
                    p['host_fare_doc']['taxes'] = 0
                self.calc_price_recommendation_base = p['host_fare_doc']['recommended_fare'] - p['host_fare_doc']['YQ'] - p['host_fare_doc']['surcharge']
                self.calc_price_recommendation_total = p['host_fare_doc']['recommended_fare'] + p['host_fare_doc']['taxes']
                self.price_recommendation_YQ = p['host_fare_doc']['YQ']
                self.price_recommendation_tax = p['host_fare_doc']['taxes']
                self.price_recommendation_surcharge = p['host_fare_doc']['surcharge']
                self.price_recommendation_YR = None
                change = (self.calc_price_recommendation_base - p['host_fare_doc']['fare'])/ float(p['host_fare_doc']['fare'])
                if recommendation_lower_threshold < change < recommendation_upper_threshold:
                    self.price_recommendation = self.calc_price_recommendation
                    self.price_recommendation_base = self.calc_price_recommendation_base
                    self.price_recommendation_total = self.calc_price_recommendation_total
                else:
                    self.price_recommendation_base = p['host_fare_doc']['fare']*(1 + (recommendation_lower_threshold/100))
                    self.price_recommendation_total = p['host_fare_doc']['total_fare']*(1 + (recommendation_lower_threshold/100))
                    self.price_recommendation = self.price_recommendation_total - p['host_fare_doc']['taxes']
            else:
                self.price_recommendation = None
                self.calc_price_recommendation = None
                self.calc_price_recommendation_base = None
                self.calc_price_recommendation_total = None
                self.price_recommendation_base = None
                self.price_recommendation_total = None
                self.price_recommendation_YQ = None
                self.price_recommendation_tax = None
                self.price_recommendation_surcharge = None
                self.price_recommendation_YR = None

            self.new_doc_data = p['new_doc_data']
            self.trigger_type = p['trigger_type']
            self.old_pricing_action_id_at_trigger_time = p['pricing_action_id_at_trigger_time']
            self.currency = self.host_pricing_data['currency']
        else:
            e1 = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                         self.get_module_name(),
                                         self.get_arg_lists(inspect.currentframe()))
            e1.append_to_error_list("Expected 1 trigger in triggering event for the id " + str(
                self.triggering_event_id) + " but got " + str(cursor.count()))
            raise e1

    @measure(JUPITER_LOGGER)
    def get_region_country(self):
        """
        :return: 
        """
        if self.pos:
            if self.pos != 'NA':
                crsr = db.JUP_DB_Region_Master.find({'POS_CD': self.pos})
                if crsr.count() == 1:
                    self.region = crsr[0]['Region']
                    self.country = crsr[0]['COUNTRY_CD']
                else:
                    self.region = 'NA'
                    self.country = 'NA'
            else:
                self.region = 'NA'
                self.country = 'NA'

    @measure(JUPITER_LOGGER)
    def get_trigger_details(self, trigger_name):
        """
        Get the details for booking_changes_weekly trigger from dB and incorporate them into object
        :returns
        """

        # trigger_crsr = db.JUP_DB_Trigger_Types.find({
        #     'desc': trigger_name
        # })
        # if trigger_crsr.count() == 1:
        #     print trigger_crsr[0]
        #     self.trigger_type = trigger_crsr[0]['desc']
        #     self.lower_threshold = trigger_crsr[0]['lower_threshhold']
        #     self.upper_threshold = trigger_crsr[0]['upper_threshhold']
        #     self.threshold_type = trigger_crsr[0]['threshold_abs/percent']
        #     self.priority = trigger_crsr[0]['priority']

        try:
            pos = self.old_doc_data['pos']
        except KeyError:
            pos = None

        try:
            origin = self.old_doc_data['origin']
        except KeyError:
            origin = None

        try:
            destination = self.old_doc_data['destination']
        except KeyError:
            destination = None

        try:
            compartment = self.old_doc_data['compartment']
        except KeyError:
            compartment = None

        db.system_js.JUP_FN_Configuration(pos, origin, destination, compartment, "temp_collection_sai", 'trigger')
        crsr = db['temp_collection_sai'].find()
        if crsr.count() > 0:
            for doc in crsr:
                doc = doc['p_config']
                if doc['Trigger_type'] == trigger_name:
                    self.trigger_type = trigger_name
                    self.lower_threshold = doc['parameters']['lower_threshold']
                    self.upper_threshold = doc['parameters']['upper_threshold']
                    self.threshold_type = doc['parameters']['threshold_percentage_or_absolute']
                    self.priority = doc['parameters']['priority']
                    self.min_interval = doc['parameters']['min_interval_to_retrigger_hrs']
        else:
            trigger_crsr = db.JUP_DB_Trigger_Types.find({
                'desc': trigger_name
            })
            if trigger_crsr.count() == 1:
                print trigger_crsr[0]
                self.trigger_type = trigger_crsr[0]['desc']
                self.lower_threshold = trigger_crsr[0]['lower_threshhold']
                self.upper_threshold = trigger_crsr[0]['upper_threshhold']
                self.threshold_type = trigger_crsr[0]['threshold_abs/percent']
                self.priority = trigger_crsr[0]['priority']
        db['temp_collection_sai'].drop()

    @measure(JUPITER_LOGGER)
    def trigger_types(self, triggers_list):
        '''
        Takes the trigger_list developed during hierarchy as input
        Returns the document containing the parameters of the trigger
        we are dealing with now
        '''
        if triggers_list != []:
            t = []
            for i in triggers_list:
                if i['desc'] == self.trigger_type:
                    t.append(i)
                else:
                    pass
            if len(t) == 1:
                q = t[0]
                self.pricing_type = q['pricing_type']
                self.pricing_method = q['pricing_method']
                self.priority = q['priority']
                self.lower_threshold = q['lower_threshhold']
                self.threshold_type = q['threshold_abs/percent']
                self.upper_threshold = q['upper_threshhold']
                self.dashboard = q['dashboard']
                self.active = q['active']
                self.trigger_status = q['status']
                self.triggering_event_type = q['triggering_event_type']
                self.triggering_event = q['triggering_event']
                self.min_interval = q['min_interval_to_retrigger_hours']
                self.frequency = q['frequency']
                self.effective_date_from = q['effective_date_from']
                self.effective_date_to = q['effective_date_to']
                self.type_of_trigger = q['type_of_trigger']
            else:
                e1 = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                             self.get_module_name(),
                                             self.get_arg_lists(inspect.currentframe()))
                e1.append_to_error_list("Expected 1 trigger type but got " + str(len(t)))
                raise e1
        else:
            e1 = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                         self.get_module_name(),
                                         self.get_arg_lists(inspect.currentframe()))
            e1.append_to_error_list("Output of hierarchy cannot be null list")
            raise e1

    @measure(JUPITER_LOGGER)
    def data_through_old_doc_change(self):
        '''
        Data to be collected from old_doc_data
        '''
        # region_country_data =
        # self.region = self.old_doc_data['region']
        # self.country = self.old_doc_data['country']
        self.pos = self.old_doc_data['pos']
        self.origin = self.old_doc_data['origin']
        self.destination = self.old_doc_data['destination']
        self.od = self.origin + self.destination
        self.compartment = self.old_doc_data['compartment']
        # self.effective_from = self.old_doc_data['effective_from']
        # self.effective_to = self.old_doc_data['effective_to']

    @measure(JUPITER_LOGGER)
    def get_host_farebasis(self):
        '''
        Function that defines how to obtain the farebasis of host
        Defined in individual classes later
        '''
        self.farebasis = self.host_pricing_data['fare_basis']

    # @measure(JUPITER_LOGGER)
    # def host_pricing_data_func(self):
    #     """
    #     Collecting the host data
    #     """
    #     qry_fares = dict(
    #         origin=self.origin.encode(),
    #         destination=self.destination.encode(),
    #         compartment=self.compartment.encode(),
    #         fare_basis=self.farebasis.encode()
    #     )
    #     qry_fares['$or'] = list()
    #
    #     qry_fares['competitor_farebasis'] = {'$ne': None}
    #     qry_fares['effective_from'] = {'$lte': SYSTEM_DATE}
    #     qry_fares['effective_to'] = {'$gte': SYSTEM_DATE}
    #
    #     qry_fares['$or'].append({
    #         '$and': [
    #             {'dep_date_from': {'$lte': self.triggering_data['dep_date_end']}},
    #             {'dep_date_end': {'$gte': self.triggering_data['dep_date_start']}}
    #         ]
    #     })
    #     qry_fares['$or'].append({
    #         '$and': [
    #             {'dep_date_from': {'$lte': self.triggering_data['dep_date_end']}},
    #             {'dep_date_end': None}
    #         ]
    #     })
    #     qry_fares['$or'].append({
    #         '$and': [
    #             {'dep_date_from': None},
    #             {'dep_date_end': {'$gte': self.triggering_data['dep_date_start']}}
    #         ]
    #     })
    #     qry_fares['$or'].append({
    #         '$and': [
    #             {'dep_date_from': None},
    #             {'dep_date_end': None}
    #         ]
    #     })
    #
    #     host_fares_data = list(db.JUP_DB_ATPCO_Fares.aggregate(
    #         [
    #             {
    #                 '$match': dict(qry_fares)
    #             }
    #         ]
    #     ))
    #     if len(host_fares_data) == 1:
    #         s = host_fares_data[0]
    #         s['base_fare'] = s['fare']
    #         s['fare'] = s['total_fare']
    #         rating_data = get_ratings_details(origin=s['origin'],
    #                                           destination=s['destination'],
    #                                           airline=Host_Airline_Code)
    #         s.update(rating_data)
    #
    #         # price_movement_data = get_price_movement(airline=Host_Airline_Code,
    #         #                                          origin=s['origin'],
    #         #                                          destination=s['destination'],
    #         #                                          compartment=s['compartment'],
    #         #                                          oneway_return=s['oneway_return'],
    #         #                                          currency=s['currency'],
    #         #                                          dep_date_start=self.triggering_data['dep_date_start'],
    #         #                                          dep_date_end=self.triggering_data['dep_date_end'],
    #         #                                          pos=s['pos'])
    #         # s.update(price_movement_data)
    #         # rating_crsr = db.JUP_DB_Product_Ratings.find({
    #         #     'airline': 'FZ',
    #         #     # 'od': self.origin + self.destination
    #         # })
    #         # if rating_crsr.count() == 1:
    #         #     s['rating'] = rating_crsr[0]['rating']
    #         # else:
    #         #     prod_rating_crsr = db.JUP_DB_Product_Ratings.find({
    #         #         'airline': 'FZ'
    #         #     })
    #         #     s['rating'] = prod_rating_crsr[0]['rating']
    #         market_share_data = calculate_market_share(airline=s['carrier'],
    #                                                    pos=s['pos'],
    #                                                    origin=s['origin'],
    #                                                    destination=s['destination'],
    #                                                    compartment=s['compartment'],
    #                                                    dep_date_from=self.triggering_data['dep_date_start'],
    #                                                    dep_date_to=self.triggering_data['dep_date_end']
    #                                                    )
    #         # s['market_share'] = market_share_data['market_share']
    #         # print s['market_share']
    #         # s['market_share_vlyr'] = market_share_data['market_share_vlyr']
    #         # s['market_share_vtgt'] = market_share_data['market_share_vtgt']
    #         # s['pax'] = market_share_data['pax']
    #         # s['avg_fare'] = market_share_data['average_fare']
    #         # s['market_average_fare'] = market_share_data['market_average_fare']
    #         s.update(market_share_data)
    #         if not (s['market_share'] and s['market_share'] != 0):
    #             self.no_host_market_share_val = True
    #             no_ms_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
    #                                                   self.get_module_name(),
    #                                                   self.get_arg_lists(inspect.currentframe()))
    #             no_ms_error.append_to_error_list('No Data in Market Share Collection')
    #             # raise no_ms_error
    #         else:
    #             self.no_host_market_share_val = False
    #         self.old_pricing_action_id_at_pricing_time = 0
    #         self.host_pricing_data = s
    #         self.currency = s['currency']
    #         self.compartment = s['compartment']
    #     else:
    #         e1 = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
    #                                      self.get_module_name(),
    #                                      self.get_arg_lists(inspect.currentframe()))
    #         e1.append_to_error_list("Expected 1 document from host data but got " + str(cursor.count()))
    #         raise e1
    #     print s

    # @measure(JUPITER_LOGGER)
    # def competitor_pricing_data_func(self):
    #     '''
    #     Collecting the competitor's data for all competitors
    #     '''
    #     cursor1 = db.JUP_DB_ATPCO_Fares.find({
    #                                           # 'pos': None,
    #                                           'origin': self.origin,
    #                                           'destination': self.destination,
    #                                           'compartment': self.compartment,
    #                                           'fare_basis': self.farebasis,
    #                                           'competitor_farebasis': {'$ne': None}})
    #     if cursor1.count() != 0:
    #         print cursor1[0]
    #         cmpt_fbs = cursor1[0]['competitor_farebasis']
    #         print cmpt_fbs
    #     else:
    #         e1 = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
    #                                      self.get_module_name(),
    #                                      self.get_arg_lists(inspect.currentframe()))
    #         e1.append_to_error_list("Expected 1 document for farebasis " + str(
    #             self.farebasis) + " from farabasis collection but got " + str(cursor1.count()))
    #         raise e1
    #     self.competitor_pricing_data = []
    #     if cmpt_fbs != []:
    #         for i in cmpt_fbs:
    #             cursor2 = get_valid_infare_fare(
    #                 airline=i['airline'],
    #                 pos=None,
    #                 origin=self.origin,
    #                 destination=self.destination,
    #                 compartment=self.compartment,
    #                 oneway_return=self.host_pricing_data['oneway_return'],
    #                 observation_date_start='2017-02-14',
    #                 observation_date_end='2017-02-14',
    #                 dep_date_start='2017-02-14',
    #                 dep_date_end='2017-02-14'
    #             )
    #
    #             data=[]
    #             data.append(cursor2)
    #             cursor2 = data
    #             if len(cursor2) == 1:
    #                 comp_market_share_data = calculate_market_share(airline=cursor2[0]['carrier'],
    #                                                                 pos=cursor2[0]['pos'],
    #                                                                 origin=cursor2[0]['origin'],
    #                                                                 destination=cursor2[0]['destination'],
    #                                                                 compartment=cursor2[0]['compartment'],
    #                                                                 dep_date_from=self.triggering_data['dep_date_start'],
    #                                                                 dep_date_to=self.triggering_data['dep_date_end']
    #                                                                 )
    #                 cursor2[0]['market_share'] = comp_market_share_data['market_share']
    #                 cursor2[0]['market_share_vlyr'] = comp_market_share_data['market_share_vlyr']
    #                 cursor2[0]['market_share_vtgt'] = comp_market_share_data['market_share_vtgt']
    #                 cursor2[0]['average_fare'] = comp_market_share_data['average_fare']
    #                 cursor2[0]['market_average_fare'] = comp_market_share_data['market_average_fare']
    #                 cursor2[0]['pax'] = comp_market_share_data['pax']
    #                 cursor2[0].update(comp_market_share_data)
    #                 price_movement_data = get_price_movement(airline=cursor2[0]['carrier'],
    #                                                          pos=cursor2[0]['pos'],
    #                                                          origin=cursor2[0]['origin'],
    #                                                          destination=cursor2[0]['destination'],
    #                                                          compartment=cursor2[0]['compartment'],
    #                                                          dep_date_start=self.triggering_data['dep_date_start'],
    #                                                          dep_date_end=self.triggering_data['dep_date_end'],
    #                                                          currency=self.host_pricing_data['currency'],
    #                                                          oneway_return=self.host_pricing_data['oneway_return'])
    #                 cursor2[0].update(price_movement_data)
    #                 self.competitor_pricing_data.append(cursor2[0])
    #                 comp_market_share_tot = sum(i['market_share'] for i in self.competitor_pricing_data if i['market_share'] != None and i['market_share'] != 'NA')
    #                 if comp_market_share_tot == 0:
    #                     self.no_comp_market_share_val = True
    #                     no_ms_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
    #                                                           self.get_module_name(),
    #                                                           self.get_arg_lists(inspect.currentframe()))
    #                     no_ms_error.append_to_error_list('No Data in Market Share Collection')
    #                     # raise no_ms_error
    #                 else:
    #                     self.no_comp_market_share_val = False
    #             else:
    #                 e1 = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
    #                                              self.get_module_name(),
    #                                              self.get_arg_lists(inspect.currentframe()))
    #                 e1.append_to_error_list("Expected 1 document for competitor data for airline " + str(
    #                     i['airline']) + " and farebasis " + str(i['farebasis']) + " but got " + str(len(cursor2)))
    #                 raise e1
    #     else:
    #         e1 = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
    #                                      self.get_module_name(),
    #                                      self.get_arg_lists(inspect.currentframe()))
    #         e1.append_to_error_list("No Competitor Farebasis Mapped to host Farebasis " + str(self.farebasis))
    #         raise e1

    # @measure(JUPITER_LOGGER)
    # def pos_od_compartment_data(self):
    #     '''
    #     Collecting Data from pos,origin,destination,compartment
    #     '''
    #     print {'pos': self.pos,
    #                                                 'origin': self.origin,
    #                                                 'destination': self.destination,
    #                                                 'compartment': self.compartment}
    #     cursor = db.JUP_DB_Pos_OD_Compartment.find({'pos': self.pos,
    #                                                 'origin': self.origin,
    #                                                 'destination': self.destination,
    #                                                 'compartment': self.compartment})
    #     if cursor.count() == 1:
    #         r = cursor[0]
    #         self.airline_objective= r['airline_objective']
    #         self.type_of_market= r['type_of_market']
    #         self.condition_of_market= r['condition_of_market']
    #         self.region = r['region']
    #         self.country = r['country']
    #         strategy_pricing = []
    #         strategy_non_pricing = []
    #         if r['strategy_id']:
    #             for doc in r['strategy_id']:
    #                 if doc['pricing']:
    #                     for strategy in doc['pricing']:
    #                         strategy_pricing.append(strategy.encode())
    #                 if doc['non_pricing']:
    #                     for strategy in doc['non_pricing']:
    #                         strategy_non_pricing.append(strategy.encode())
    #         self.strategy_details = {
    #             'action': {
    #                 'pricing': strategy_pricing,
    #                 'non_pricing': strategy_non_pricing
    #             }
    #         }
    #         if r['strategy_id'][0]['pricing']:
    #             self.strategy = r['strategy_id'][0]['pricing'][0]
    #         else:
    #             self.strategy = None
    #         # if (self.airline_objective is not None and
    #         #         self.type_of_market is not None and
    #         #         self.condition_of_market is not None):
    #         #     cursor1 = db.JUP_DB_Strategy_Master.find({'condition': self.condition_of_market,
    #         #                                               'type': self.type_of_market,
    #         #                                               'objective': self.airline_objective})
    #         #     if cursor1.count() != 1:
    #         #         self.strategy = None
    #         #     elif cursor1.count() == 1:
    #         #         self.strategy = cursor1[0]
    #         #     else:
    #         #         self.strategy = None
    #     else:
    #         e1=error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
    #                                    self.get_module_name(),
    #                                    self.get_arg_lists(inspect.currentframe()))
    #         e1.append_to_error_list("Expected 1 document for POS_OD_Compartment but got " +
    #                                 str(cursor.count()))
    #         raise e1

    @measure(JUPITER_LOGGER)
    def check_min_interval(self):
        '''
        Function checks if the interval b/w the last time a
        pricing action has been approved and the present time(t)
        is greater than or less than the min interval defined
        True if t <= min interval
        False if t >= min interval
        '''
        q = db.JUP_DB_Cum_Pricing_Actions.find({
            'pos': self.pos,
            'origin': self.origin,
            'destination': self.destination,
            'farebasis': self.farebasis,
            'trigger_type': self.trigger_type,
            'status': {'$in':['accepted','Autoaccepted']}
        })
        if q.count() == 0:
            self.min_interval_factor = False
        else:
            last_pricing_action = q[q.count() - 1]
            last_occurence_str = last_pricing_action['action_date'] + ' ' + last_pricing_action['action_time']
            last_occurence = datetime.datetime.strptime(last_occurence_str, '%Y-%m-%d %H:%M:%S.%f')
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
        effective_date_from = datetime.datetime.strptime(self.effective_date_from, '%d-%m-%Y')
        effective_date_to = datetime.datetime.strptime(self.effective_date_to, '%d-%m-%Y')
        if effective_date_from < t < effective_date_to:
            self.effective_date_factor = False
        else:
            self.effective_date_factor = True

    @measure(JUPITER_LOGGER)
    def check_pricing_action(self):
        '''
        Function checks the min interval factors and effective dates factor
        and returns if the recommendation needs to be done or not
        '''
        p = self.min_interval_factor or\
            self.pricing_method == 'not_yet_implemented'\
            or self.effective_date_factor or self.active == 'False'
        if p:
            self.is_recommendation = False
            self.price_recommendation = 'null'
            self.calculated_recommendation = 'null'
            self.percent_change = 'null'
            self.abs_change = 'null'
            self.ref_farebasis = 'null'
            self.process_end_date = datetime.datetime.now().strftime('%Y-%m-%d')
            self.processed_end_time = datetime.datetime.now().strftime('%H:%M:%S.%f')
            self.batch_price = 'null'
        else:
            if self.trigger_status == 'full_treatment':
                self.is_recommendation = True
            else:
                if self.trigger_status == 'only_notification':
                    self.is_recommendation = False

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        '''
        Function that defines the Description for this trigger
        Defined for individual triggers later
        '''
        pass

    @measure(JUPITER_LOGGER)
    def update_db(self):
        '''
        Function to update the database
        if errors occured update errors
        else update the recommendation/trigger
        '''
        del self.errors
        # if self.price_recommendation != 0:
            # fare_doc = self.get_host_doc()
            # self.farebasis = fare_doc['fare_basis']
            # self.host_pricing_data.update(fare_doc)
            # self.host_pricing_data['fare'] = self.host_pricing_data['total_fare']
        # self.trigger_id = generate_trigger_id(trigger_name=self.trigger_type)

        # cat = Category(self.__dict__)
        # cat.do_analysis()
        # cat_details = cat.__dict__
        # print cat_details
        # del cat_details['reco']
        self.recommendation_category = self.cat_details['category']
        self.recommendation_score = self.cat_details['score']

        # self.recommendation_category_details = cat_details
        pricing_actions = self.__dict__
        db.JUP_DB_Pricing_Actions.insert_one(pricing_actions)
        self.get_region_country()
        pricing_actions.update(self.comp_level_data)
        pricing_actions = update_host_workflow_params_fb(trigger_obj=pricing_actions)
        print 'DONE'
        pricing_actions['status'] = 'pending'
        pricing_actions['action_date'] = None
        pricing_actions['action_time'] = None
        print pricing_actions
        db.JUP_DB_Workflow.insert(self.__dict__)

    # @measure(JUPITER_LOGGER)
    # def check_errors(self):
    #     '''
    #     Checks some of the basic errors defined
    #     '''
    #     error_flag = False
    #     try:
    #         self.check_no_of_competitors()
    #     except error_class.ErrorObject as esub:
    #         self.append_error(self.errors, esub)
    #         if esub.error_level <= error_class.ErrorObject.WARNING:
    #             error_flag = False
    #         elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL1:
    #             error_flag = True
    #         elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL2:
    #             error_flag = True
    #             raise esub
    #
    #     if not error_flag:
    #         try:
    #             self.check_host_in_competitor_record()
    #         except error_class.ErrorObject as esub:
    #             self.append_error(self.errors, esub)
    #             if esub.error_level <= error_class.ErrorObject.WARNING:
    #                 error_flag = False
    #             elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL1:
    #                 error_flag = True
    #             elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL2:
    #                 error_flag = True
    #                 raise esub
    #
    #     if not error_flag:
    #         try:
    #             self.check_market_share()
    #         except error_class.ErrorObject as esub:
    #             self.append_error(self.errors, esub)
    #             if esub.error_level <= error_class.ErrorObject.WARNING:
    #                 error_flag = False
    #             elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL1:
    #                 error_flag = True
    #             elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL2:
    #                 error_flag = True
    #                 raise esub
    #
    #     if not error_flag:
    #         try:
    #             self.check_airline_rating()
    #         except error_class.ErrorObject as esub:
    #             self.append_error(self.errors, esub)
    #             if esub.error_level <= error_class.ErrorObject.WARNING:
    #                 error_flag = False
    #             elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL1:
    #                 error_flag = True
    #             elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL2:
    #                 error_flag = True
    #                 raise esub
    #
    #     if not error_flag:
    #         try:
    #             self.check_competitor_more_than_once()
    #         except error_class.ErrorObject as esub:
    #             self.append_error(self.errors, esub)
    #             if esub.error_level <= error_class.ErrorObject.WARNING:
    #                 error_flag = False
    #             elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL1:
    #                 error_flag = True
    #             elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL2:
    #                 error_flag = True
    #                 raise esub
    #
    #     if not error_flag:
    #         try:
    #             currency_list = db.JUP_DB_Exchange_Rate.distinct('code')
    #             self.check_currency(currency_list)
    #         except error_class.ErrorObject as esub:
    #             self.append_error(self.errors, esub)
    #             if esub.error_level <= error_class.ErrorObject.WARNING:
    #                 error_flag = False
    #             elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL1:
    #                 error_flag = True
    #             elif esub.error_level <= error_class.ErrorObject.ERRORLEVEL2:
    #                 error_flag = True
    #                 raise esub
    #
    #     return error_flag
    #
    # @measure(JUPITER_LOGGER)
    # def check_currency(self, currency_list=net.Currency_List):
    #     '''
    #     Takes the currency list supported by Jupiter as input and checks if the
    #     currency of host and competitors are in the list
    #     '''
    #     host = self.host_pricing_data
    #     competitor = self.competitor_pricing_data
    #     flag = 0
    #     e1 = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
    #                                  self.get_module_name(),
    #                                  self.get_arg_lists(inspect.currentframe()))
    #     for i in competitor:
    #         if (af.lin_search(currency_list, i['currency'])):  # if competitor currency is not in the list throws error
    #             e1.append_to_error_list(
    #                 "Currency of " + i['airline'] + " which is the competitor airline is not in the currency list ")
    #             flag = 1
    #     if (af.lin_search(currency_list, host['currency'])):  # checks host currency in the list
    #         e1.append_to_error_list(
    #             "Currency of " + self.airline + " which is the host airline is not in the currency list ")
    #         flag = 1
    #     if (flag):
    #         raise e1
    #
    # @measure(JUPITER_LOGGER)
    # def check_competitor_more_than_once(self):
    #     '''
    #     Check if one competitor is present more than once in the
    #     competitors data
    #     '''
    #     competitor = self.competitor_pricing_data
    #     flag = 0
    #     comp = []
    #     e1 = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
    #                                  self.get_module_name(),
    #                                  self.get_arg_lists(inspect.currentframe()))
    #     for i in competitor:
    #         if (
    #         af.lin_search(comp, i['carrier'])):  ###we will keep on appending comp if the airline is not present in comp
    #             comp.append(i['carrier'])
    #         else:  ### if airline is present in comp throws error
    #             e1.append_to_error_list(
    #                 "Competitor Airline " + i['carrier'] + " appears more than once in competitor pricing records")
    #             flag = 1
    #     if (flag):
    #         raise e1
    #
    # @measure(JUPITER_LOGGER)
    # def check_airline_rating(self):
    #     '''
    #     A default range of Airline raing is present
    #     Checks if the ratings of host and competitors under consideration
    #     are in the range
    #     '''
    #     host = self.host_pricing_data
    #     competitor = self.competitor_pricing_data
    #     print host
    #     print competitor
    #     flag = 0
    #     e1 = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
    #                                  self.get_module_name(),
    #                                  self.get_arg_lists(inspect.currentframe()))
    #     for i in competitor:
    #         if (i['rating'] < 0.2 or i['rating'] > 10):
    #             e1.append_to_error_list("Airline rating " + str(i['rating']) + "of " + i['carrier'] + " is not in tbe prescribed limits")
    #             flag = 1
    #     if (host['rating'] < 0.2 or host['rating'] > 10):
    #         e1.append_to_error_list("Airline rating of " + str(i['rating']) + '' + self.airline + " is not in the prescribed limits")
    #         flag = 1
    #     if (flag):
    #         raise e1
    #
    # @measure(JUPITER_LOGGER)
    # def check_market_share(self):
    #     '''
    #     Market share should nevenr be 0
    #     Sum of Market Share of host and competitor should be
    #     close to 100 i.e.
    #     [99,101]
    #     '''
    #     host = self.host_pricing_data
    #     competitor = self.competitor_pricing_data
    #     mar = 0
    #     flag = 0
    #     e1 = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
    #                                  self.get_module_name(),
    #                                  self.get_arg_lists(inspect.currentframe()))
    #     if host['market_share'] and host['market_share'] != 'NA':
    #         if host['market_share'] < 0:
    #             e1.append_to_error_list("Market share of " + self.airline + " is negative")
    #             flag = 1
    #     for i in competitor:
    #         if i['market_share']and i['market_share'] != 'NA':
    #             if (i['market_share'] < 0):
    #                 e1.append_to_error_list("Market share of " + i['airline'] + " is negative")
    #                 flag = 1
    #             mar = mar + i['market_share']
    #     if host['market_share'] and host['market_share'] != 'NA':
    #         mar = mar + host['market_share']
    #
    #     if (flag):
    #         raise e1
    #
    #     if (mar > 101 or mar < 99):  ###for now market share close to hundred means it should  be between 99% to 101%
    #         e2 = error_class.ErrorObject(error_class.ErrorObject.WARNING,
    #                                      self.get_module_name(),
    #                                      self.get_arg_lists(inspect.currentframe()))
    #         e2.append_to_error_list("Total market share not close to 100")
    #         raise e2
    #
    # @measure(JUPITER_LOGGER)
    # def check_host_in_competitor_record(self):
    #     '''
    #     Checks if host is present in the competitor's records
    #     '''
    #     flag = 0
    #     for i in self.competitor_pricing_data:
    #         if (self.airline == i['carrier']):
    #             flag = 1
    #     if (flag):
    #         e1 = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
    #                                      self.get_module_name(),
    #                                      self.get_arg_lists(inspect.currentframe()))
    #         e1.append_to_error_list("Host is in competitor Pricing Records")
    #         raise e1
    #
    # @measure(JUPITER_LOGGER)
    # def check_no_of_competitors(self):
    #     '''
    #     Checks if Number of Competitors is not 0
    #     '''
    #     if (len(self.competitor_pricing_data) == 0):
    #         e1 = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
    #                                      self.get_module_name(),
    #                                      self.get_arg_lists(inspect.currentframe()))
    #         e1.append_to_error_list("No of competitors equal to 0")
    #         raise e1

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

    @measure(JUPITER_LOGGER)
    def get_host_doc(self):
        return self.host_pricing_data

#   ************************************    DATA CHANGE TRIGGERS CLASSES    *******************************************


class competitor_market_share_change(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(competitor_market_share_change, self).__init__(triggering_event_id)

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        from analytics_functions import get_calender_month
        desc = [
            "Competitor Market Share Change",
            '_',
            "ID - ", str(self.trigger_id),
            '_',
            "Carrier - ", str(self.old_doc_data['airline'].encode()),
            '_',
            "Departure Month : ", str(get_calender_month(self.old_doc_data['month'])), ' ', str(self.old_doc_data['year']),
            '_'
            "Market Share(Prev) - ", str(self.old_doc_data['market_share']),
            '_',
            "Market Share(Current) - ", str(self.new_doc_data['market_share']),
            '_',
            'Thresholds(', self.threshold_type, ')',' - ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
            'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
        ]
        self.desc = ''.join(desc)


class host_market_share_change(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(host_market_share_change, self).__init__(triggering_event_id)

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        from analytics_functions import get_calender_month
        desc = [
            "Host Market Share Change",
            '_',
            "ID - ", self.trigger_id,
            '_',
            "Departure Month : ", str(get_calender_month(self.old_doc_data['month'])), ' ', str(self.old_doc_data['year']),
            '_'
            "Market Share(Prev) - ", str(self.old_doc_data['market_share']),
            '_',
            "Market Share(Current) - ", str(self.new_doc_data['market_share']),
            '_',
            'Thresholds(',str(self.threshold_type), ')', ' - ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
            'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
        ]
        self.desc = ''.join(desc)


class competitor_airline_capacity_percentage_change(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(competitor_airline_capacity_percentage_change, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        from analytics_functions import get_calender_month
        desc = [
            "Competitor Capacity Change",
            '_',
            "ID - ", self.trigger_id,
            '_',
            "Departure Month : ", str(get_calender_month(self.old_doc_data['month'])), ' ', str(self.old_doc_data['year']),
            '_'
            "Capacity(Prev) - ", str(self.old_doc_data['capacity']),
            '_',
            "Capacity(Current) - ", str(self.new_doc_data['capacity']),
            '_',
            'Thresholds(',self.threshold_type, ')', ' - ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
            'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
        ]
        self.desc = ''.join(desc)


class host_airline_capacity_percentage_change(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(host_airline_capacity_percentage_change, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        from analytics_functions import get_calender_month
        desc = [
            "Host Capacity Change",
            '_',
            "ID - ", self.trigger_id,
            '_',
            "Departure Month : ", str(get_calender_month(self.old_doc_data['month'])), ' ', str(self.old_doc_data['year']),
            '_'
            "Capacity(Prev) - ", str(self.old_doc_data['capacity']),
            '_',
            "Capacity(Current) - ", str(self.new_doc_data['capacity']),
            '_',
            'Thresholds(', self.threshold_type, ')', ' - ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
            'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
        ]
        self.desc = ''.join(desc)


class competitor_rating_change(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(competitor_rating_change, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        desc = [
            "Competitor Rating Change",
            '_',
            "ID - ", self.trigger_id,
            '_',
            "Carrier - ", self.old_doc_data['airline'].encode(),
            '_',
            "Rating(Prev) - ", str(self.old_doc_data['rating']),
            '_',
            "Rating(Current) - ", str(self.new_doc_data['rating']),
            '_',
            'Thresholds : ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
            'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time),
            '_',
            "Departure Period(Assumed) : ", self.triggering_data['dep_date_start'], ' to ', self.triggering_data['dep_date_end']
        ]
        self.desc = ' '.join(desc)


class host_rating_change(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(host_rating_change, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        desc = [
            "Host Rating Change",
            '_',
            "ID - ", self.trigger_id,
            '_',
            "Rating(Prev) - ", self.old_doc_data['rating'],
            '_',
            "Rating(Current) - ", self.new_doc_data['rating'],
            '_',
            'Thresholds : ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
            'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time),
            '_',
            "Departure Period(Assumed) : ", self.triggering_data['dep_date_start'], ' to ', self.triggering_data['dep_date_end']
        ]
        self.desc = ' '.join(desc)


class competitor_new_entry(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(competitor_new_entry, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        desc = [
            "Competitor Entry",
            '_',
            "ID - ", self.trigger_id,
            '_',
            "Carrier - ", self.new_doc_data['airline'],
            '_',
            "Departure Period(Assumed) : ", self.triggering_data['dep_date_start'], ' to ', self.triggering_data['dep_date_end'],
            '_',
            'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time),
        ]
        self.desc = ''.join(desc)


class competitor_exit(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(competitor_exit, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        desc = [
            "Competitor Entry",
            '_',
            "ID - ", self.trigger_id,
            '_',
            "Carrier - ", self.new_doc_data['airline'],
            '_',
            "Departure Period(Assumed) : ", self.triggering_data['dep_date_start'], ' to ', self.triggering_data['dep_date_end'],
            '_',
            'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time),
        ]
        self.desc = ''.join(desc)


class forecast_changes(trigger):
    """
    """
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(forecast_changes, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        from analytics_functions import get_calender_month
        if self.old_doc_data['param'] == 'pax':
            desc = [
                "Forecast Changes",
                '_',
                "ID - ", self.trigger_id,
                '_',
                "Departure Month : ", str(get_calender_month(self.old_doc_data['month'])), ' ', str(self.old_doc_data['year']),
                '_'
                "Pax(Prev) - ", str(self.old_doc_data['pax']),
                '_',
                "Pax(Current) - ", str(self.new_doc_data['pax']),
                '_',
                'Thresholds(', self.threshold_type, ')', ' - ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
            ]
        elif self.old_doc_data['param'] == 'revenue':
            desc = [
                "Forecast Changes",
                '_',
                "ID - ", self.trigger_id,
                '_',
                "Departure Month : ", str(get_calender_month(self.old_doc_data['month'])), ' ', str(self.old_doc_data['year']),
                '_'
                "Revenue(Prev) - ", str(self.old_doc_data['revenue']),
                '_',
                "Revenue(Current) - ", str(self.new_doc_data['revenue']),
                '_',
                'Thresholds(', self.threshold_type, ')',
                ' - ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
                '_',
                'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
            ]
        else:
            desc = [
                "Forecast Changes",
                '_',
                "ID - ", self.trigger_id,
                '_',
                "Departure Month : ", str(get_calender_month(self.old_doc_data['month'])), ' ', str(self.old_doc_data['year']),
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
                'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
            ]
        self.desc = ''.join(desc)

#   *************************************   DATA LEVEL TRIGGERS   *****************************************************


class bookings_changes_rolling(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(bookings_changes_rolling, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        desc = [
            "Bookings Changes Rolling",
            '_',
            'Bookings TWk', str(round((self.old_doc_data['bookings']), 3)),
            '_',
            "Departure Period: ", self.triggering_data['dep_date_start'], ' to ', self.triggering_data['dep_date_end'],
            '_'
            'Bookings LWk', str(round(self.new_doc_data['bookings'], 3)),
            '_',
            "Departure Period: ", self.triggering_data['dep_date_start'], ' to ', self.triggering_data['dep_date_end'],
            '_'
            'Thresholds:' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
            'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
        ]
        self.desc = ''.join(desc)


class pax_changes_rolling(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(pax_changes_rolling, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        desc = [
            "Pax Changes Rolling",
            '_',
            'Pax TWk', str(round((self.old_doc_data['pax']), 3)),
            '_',
            "Departure Period: ", self.triggering_data['dep_date_start'], ' to ', self.triggering_data['dep_date_end'],
            '_'
            'Pax LWk', str(round(self.new_doc_data['pax'], 3)),
            '_',
            "Departure Period: ", self.triggering_data['dep_date_start'], ' to ', self.triggering_data['dep_date_end'],
            '_'
            'Thresholds:' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
            'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
        ]
        self.desc = ''.join(desc)


class revenue_changes_rolling(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(revenue_changes_rolling, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        desc = [
            "Revenue Changes Rolling",
            '_',
            'Revenue TWk', str(round((self.old_doc_data['revenue']), 3)),
            '_',
            "Departure Period: ", self.triggering_data['dep_date_start'], ' to ', self.triggering_data['dep_date_end'],
            '_'
            'Revenue LWk', str(round(self.new_doc_data['revenue'], 3)),
            '_',
            "Departure Period: ", self.triggering_data['dep_date_start_lw'], ' to ', self.triggering_data['dep_date_end_lw'],
            '_'
            'Thresholds:' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
            'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
        ]
        self.desc = ''.join(desc)


class yield_changes_rolling(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(yield_changes_rolling, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        desc = [
            "Yield Changes Rolling",
            '_',
            'Yield TWk', str(round((self.old_doc_data['yield']), 3)),
            '_',
            "Departure Period: ", self.triggering_data['dep_date_start'], ' to ', self.triggering_data['dep_date_end'],
            '_'
            'Yield LWk', str(round(self.new_doc_data['yield'], 3)),
            '_',
            "Departure Period: ", self.triggering_data['dep_date_start_lw'], ' to ', self.triggering_data['dep_date_end_lw'],
            '_'
            'Thresholds:' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
            'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
        ]
        self.desc = ''.join(desc)


class booking_changes_weekly(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(booking_changes_weekly, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        desc = [
            "Booking Changes Weekly",
            "_",
            "ID - ", self.trigger_id,
            '_',
            "Departure Period: ", self.triggering_data['dep_date_start'], ' to ', self.triggering_data['dep_date_end'],
            '_',
            'Bkgs LWk', str(round((self.old_doc_data['bookings']), 3)),
            '_',
            'Bkgs TWk', str(round(self.new_doc_data['bookings'], 3)),
            '_',
            'Thresholds:' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
            'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
        ]
        self.desc = ''.join(desc)


class pax_changes_weekly(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(pax_changes_weekly, self).__init__(triggering_event_id)
        # self.do_analysis()
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        desc = [
            "Pax Changes Weekly",
            "_",
            "ID - ", self.trigger_id,
            '_',
            "Departure Period: ", self.triggering_data['dep_date_start'], ' to ', self.triggering_data['dep_date_end'],
            '_',
            'Pax LWk', str(round((self.old_doc_data['pax']), 3)),
            '_',
            'Pax TWk', str(round(self.new_doc_data['pax'], 3)),
            '_',
            'Thresholds:' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
            'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
        ]
        self.desc = ''.join(desc)


class revenue_changes_weekly(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(revenue_changes_weekly, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        desc = [
            "Revenue Changes Weekly",
            "_",
            "ID - ", self.trigger_id,
            '_',
            "Departure Period: ", self.triggering_data['dep_date_start'], ' to ', self.triggering_data['dep_date_end'],
            '_',
            'Revenue LWk', str(round((self.old_doc_data['revenue']), 3)),
            '_',
            'Revenue TWk', str(round(self.new_doc_data['revenue'], 3)),
            '_',
            'Thresholds:' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
            'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
        ]
        self.desc = ''.join(desc)


class yield_changes_weekly(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(yield_changes_weekly, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        desc = [
            "Yield Changes Weekly",
            "_",
            "ID - ", self.trigger_id,
            '_',
            "Departure Period: ", self.triggering_data['dep_date_start'], ' to ', self.triggering_data['dep_date_end'],
            '_',
            'Yield LWk', str(round((self.old_doc_data['yield']), 3)),
            '_',
            'Yield TWk', str(round(self.new_doc_data['yield'], 3)),
            '_',
            'Thresholds:' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
            'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
        ]
        self.desc = ''.join(desc)


class booking_changes_VLYR(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(booking_changes_VLYR, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        desc = [
            "Booking Changes VLYR",
            "_",
            "ID - ", self.trigger_id,
            "_",
            "Departure Period: ", self.triggering_data['dep_date_start'], ' to ', self.triggering_data['dep_date_end'],
            '_',
            'Bkgs Last Year', str(self.old_doc_data['bookings']),
            '_',
            'Capacity Last Year', str(self.old_doc_data['capacity']),
            '_',
            'Capacity This Year', str(self.new_doc_data['capacity']),
            '_',
            'Expected Bkgs This Year', str(self.new_doc_data['bookings_expected']),
            '_',
            'Actual Bkgs This Year', str(self.new_doc_data['bookings']),
            '_',
            'Thresholds:' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
            'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
        ]
        self.desc = ''.join(desc)


class pax_changes_VLYR(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(pax_changes_VLYR, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        desc = [
            "Pax Changes VLYR",
            "_",
            "ID - ", self.trigger_id,
            "_",
            "Departure Period: ", self.triggering_data['dep_date_start'], ' to ', self.triggering_data['dep_date_end'],
            '_',
            'Pax Last Year', str(self.old_doc_data['pax']),
            '_',
            'Capacity Last Year', str(self.old_doc_data['capacity']),
            '_',
            'Capacity This Year', str(self.new_doc_data['capacity']),
            '_',
            'Expected Pax This Year', str(self.new_doc_data['pax_expected']),
            '_',
            'Actual Pax This Year', str(self.new_doc_data['pax']),
            '_',
            'Thresholds:' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
            'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
        ]
        self.desc = ''.join(desc)


class revenue_changes_VLYR(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(revenue_changes_VLYR, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        desc = [
            "Revenue Changes VLYR",
            "_",
            "ID - ", self.trigger_id,
            "_",
            "Departure Period: ", self.triggering_data['dep_date_start'], ' to ', self.triggering_data['dep_date_end'],
            '_',
            'Revenue Last Year', str(self.old_doc_data['revenue']),
            '_',
            'Capacity Last Year', str(self.old_doc_data['capacity']),
            '_',
            'Capacity This Year', str(self.new_doc_data['capacity']),
            '_',
            'Expected Revenue This Year', str(self.new_doc_data['revenue_expected']),
            '_',
            'Actual Revenue This Year', str(self.new_doc_data['revenue']),
            '_',
            'Thresholds:' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
            'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
        ]
        self.desc = ''.join(desc)


class yield_changes_VLYR(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(yield_changes_VLYR, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        desc = [
            "Yield Changes VLYR",
            "_",
            "ID - ", self.trigger_id,
            "_",
            "Departure Period: ", self.triggering_data['dep_date_start'], ' to ', self.triggering_data['dep_date_end'],
            '_',
            'Yield Last Year', str(self.old_doc_data['yield']),
            '_',
            'Capacity Last Year', str(self.old_doc_data['capacity']),
            '_',
            'Capacity This Year', str(self.new_doc_data['capacity']),
            '_',
            'Expected Yield This Year', str(self.new_doc_data['yield_expected']),
            '_',
            'Actual Yield This Year', str(self.new_doc_data['yield']),
            '_',
            'Thresholds:' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
            'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
        ]
        self.desc = ''.join(desc)


class booking_changes_VTGT(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(booking_changes_VTGT, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        desc = [
            "Booking Changes VTGT",
            "_",
            "ID - ", self.trigger_id,
            "_",
            "Departure Period: ", self.triggering_data['dep_date_start'], ' to ', self.triggering_data['dep_date_end'],
            '_',
            'Bkgs VTGT CY', str(round((self.old_doc_data['percent_acheived']), 3)),
            '_',
            'Bkgs VTGT LY', str(round(self.new_doc_data['percent_acheived'], 3)),
            '_',
            'Thresholds:' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
            'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
        ]
        self.desc = ''.join(desc)


class pax_changes_VTGT(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(pax_changes_VTGT, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        desc = [
            "Pax Changes VTGT",
            "_",
            "ID - ", self.trigger_id,
            "_",
            "Departure Period: ", self.triggering_data['dep_date_start'], ' to ', self.triggering_data['dep_date_end'],
            '_',
            'Pax VTGT CY', str(round((self.old_doc_data['percent_acheived']), 3)),
            '_',
            'Pax VTGT LY', str(round(self.new_doc_data['percent_acheived'], 3)),
            '_',
            'Thresholds:' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
            'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
        ]
        self.desc = ''.join(desc)


class revenue_changes_VTGT(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(revenue_changes_VTGT, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        desc = [
            "Revenue Changes VTGT",
            "_",
            "ID - ", self.trigger_id,
            "_",
            "Departure Period: ", self.triggering_data['dep_date_start'], ' to ', self.triggering_data['dep_date_end'],
            '_',
            'Revenue VTGT CY', str(round((self.old_doc_data['percent_acheived']), 3)),
            '_',
            'Revenue VTGT LY', str(round(self.new_doc_data['percent_acheived'], 3)),
            '_',
            'Thresholds:' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
            'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
        ]
        self.desc = ''.join(desc)


class yield_changes_VTGT(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(yield_changes_VTGT, self).__init__(triggering_event_id)

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        desc = [
            "Yield Changes VTGT",
            "_",
            "ID - ",self.trigger_id,
            "_",
            "Departure Period: ", self.triggering_data['dep_date_start'], ' to ', self.triggering_data['dep_date_end'],
            '_',
            'Yield', str(round((self.old_doc_data['yield']), 3)),
            '_',
            'Target Yield', str(round(self.old_doc_data['target_yield'], 3)),
            '_',
            'Thresholds:' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
            'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
        ]
        self.desc = ''.join(desc)


class lowest_fare_comparision(trigger):
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(lowest_fare_comparision, self).__init__(triggering_event_id)

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        self.lower_threshold = self.triggering_data['condition']['lower_threshold']
        self.upper_threshold = self.triggering_data['condition']['upper_threshold']
        self.desc = ' '.join([
            "LFC: Lowest Fares Comparision"
            '_',
            'I: Internally Generated',
            '_'
            "Departure Period: ", self.triggering_data['dep_date_start'], ' to ', self.triggering_data['dep_date_end'],
            '_',
            'Competitor in comparision: ', self.triggering_data['condition']['airline'],
            '_',
            'Host Lowest Fare', str(self.old_doc_data['host_fare']),
            '_',
            'Competitor Lowest Fare', str(self.old_doc_data['comp_fare']),
            '_',
            'Thresholds:' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
            'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
        ])
        # self.desc = ''.join(desc)


class lowest_fare_changes(trigger):
    """
    """


class opportunities(trigger):
    pass


class forecast_changes_VTGT(trigger):
    """
    """
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(forecast_changes_VTGT, self).__init__(triggering_event_id)
        # self.do_analysis()
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        from analytics_functions import get_calender_month
        desc = [
            "Forecast Changes VTGT",
            '_',
            "ID - ", self.trigger_id,
            '_',
            "Departure Dates : ", str(self.triggering_data['dep_date_start']), ' to ',
            str(self.triggering_data['dep_date_end']),
            '_'
            "Forecast Pax - ", str(self.new_doc_data['forecast_pax']),
            '_',
            "Target Pax - ", str(self.old_doc_data['target_pax']),
            '_',
            'Thresholds(', self.threshold_type, ')', ' - ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
            '_',
                                                     'Raised On ' + str(self.triggering_event_date) + ' at ' + str(
                                                         self.triggering_event_time)
        ]
        self.desc = ''.join(desc)


#   ******************************************** MANUAL TRIGGERS ******************************************************


class manual(trigger):
    """
    """
    @measure(JUPITER_LOGGER)
    def __init__(self, triggering_event_id):
        super(manual, self).__init__(triggering_event_id)
        # self.get_desc()

    @measure(JUPITER_LOGGER)
    def get_desc(self):
        desc = [
            'Manual Trigger',
            '_',
            'Reason - ',self.old_doc_data['reason']
        ]
        self.desc = ''.join(desc)


# '''
# class data_change(trigger):
# @measure(JUPITER_LOGGER)
# def __init__(self, triggering_event_id):
#         trigger.__init__(self, triggering_event_id)
#
#
# class competitor_change(data_change):
# @measure(JUPITER_LOGGER)
# def __init__(self, triggering_event_id):
#         data_change.__init__(self, triggering_event_id)
#
# @measure(JUPITER_LOGGER)
# def get_host_farebasis(self):
#         self.farebasis = self.old_doc_data['host_farebasis']
#         # change_compt_fb = {'airline': self.old_doc_data['airline'],
#         #                    'farebasis': self.old_doc_data['fare_basis']}
#         # cursor = db.JUP_DB_JUP_DB_ATPCO_Fares.find({'pos': self.old_doc_data['pos'],
#         #                                            'origin': self.old_doc_data['origin'],
#         #                                            'destination': self.old_doc_data['destination'],
#         #                                            'compartment': self.old_doc_data['compartment'],
#         #                                            'effective_from':
#         #                                              {'$lte': datetime.datetime.strftime(today,'%Y-%m-%d')},
#         #                                            'effective_to':
#         #                                              {'$gte': datetime.datetime.strftime(today,'%Y-%m-%d')},
#         #                                            'competitor_farebasis': {'$elemMatch': change_compt_fb}})
#         # print cursor.count()
#         # if cursor.count() == 1:
#         #     host_fb = cursor[0]['farebasis']
#         #     self.farebasis = host_fb
#         # elif cursor.count() == 0:
#         #     e1 = error_class.ErrorObject(error_class.ErrorObject.WARNING,
#         #                                  self.get_module_name(),
#         #                                  self.get_arg_lists(inspect.currentframe()))
#         #     e1.append_to_error_list("Host Farebasis Not Mapped for airline " + str(
#         #         self.old_doc_data['airline']) + " and the Competitor Farebasis " + str(self.old_doc_data['farebasis']))
#         #     raise e1
#         #
#         # else:
#         #     e2 = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
#         #                                  self.get_module_name(),
#         #                                  self.get_arg_lists(inspect.currentframe()))
#         #     e2.append_to_error_list(
#         #         "Mapping Not ONE-ONE as multiple farebasis codes of host mapped to competitor airline " + str(
#         #             self.old_doc_data['airline']) + " and the Competitor Farebasis " + str(
#         #             self.old_doc_data['farebasis']))
#         #     raise e2
#
#
# class host_change(data_change):
# @measure(JUPITER_LOGGER)
# def __init__(self, triggering_event_id):
#         data_change.__init__(self, triggering_event_id)
#         self.get_host_farebasis()
#
# @measure(JUPITER_LOGGER)
# def get_host_farebasis(self):
#         self.farebasis = self.old_doc_data['fare_basis']
#
#
# class thesis_model_host(host_change):
# @measure(JUPITER_LOGGER)
# def __init__(self, triggering_event_id):
#         host_change.__init__(self, triggering_event_id)
#
#
# class thesis_model_competitor(competitor_change):
# @measure(JUPITER_LOGGER)
# def __init__(self, triggering_event_id):
#         competitor_change.__init__(self, triggering_event_id)
#
# #
# # class competitor_price_change(thesis_model_competitor):
# @measure(JUPITER_LOGGER)#
# def __init__(self, triggering_event_id):
# #         thesis_model_competitor.__init__(self, triggering_event_id)
# #
# @measure(JUPITER_LOGGER)#
# def get_desc(self):
# #         self.desc = ' '.join(["Recommendation for trigger raised because of a change in price of",
# #                               self.old_doc_data['airline'].encode(),
# #                               # " for the farebasis code ",self.old_doc_data['farebasis'],
# #                               " from ", str(self.old_doc_data['price']),
# #                               " to ", str(self.new_doc_data['price']), " on ", str(self.triggering_event_date),
# #                               " at ", str(self.triggering_event_time)
# #                               # ," for the departure date",str()
# #                              ]
# #                              )
# #
# #
#
#
#
# class host_market_share_change(thesis_model_host):
# @measure(JUPITER_LOGGER)
# def __init__(self, triggering_event_id):
#         thesis_model_host.__init__(self, triggering_event_id)
#         self.get_desc()
#
# @measure(JUPITER_LOGGER)
# def get_desc(self):
#         from analytics_functions import get_calender_month
#         desc = [
#             "Host Market Share Change",
#             '_',
#             "ID - ", self.trigger_id,
#             '_',
#             "Departure Month : ", str(get_calender_month(self.old_doc_data['month'])), ' ', str(self.old_doc_data['year']),
#             '_'
#             "Market Share(Prev) - ", str(self.old_doc_data['market_share']),
#             '_',
#             "Market Share(Current) - ", str(self.new_doc_data['market_share']),
#             '_',
#             'Thresholds(',self.threshold_type, ')', ' - ' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
#             '_',
#             'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
#         ]
#         self.desc = ''.join(desc)
#
#
#
#
#
# # class new_customer_segment_host(host_change):
# @measure(JUPITER_LOGGER)#
# def __init__(self,triggering_event_id):
# # 		host.__init__(self,triggering_event_id)
# @measure(JUPITER_LOGGER)#
# def get_price_recommedation(self):
# # 		pass
# @measure(JUPITER_LOGGER)#
# def get_desc(self):
# # 		pass
#
# # class host_airline_objective_change(host_change):
# @measure(JUPITER_LOGGER)#
# def __init__(self,triggering_event_id):
# # 		host.__init__(self,triggering_event_id)
# @measure(JUPITER_LOGGER)#
# def get_price_recommedation(self):
# # 		pass
# @measure(JUPITER_LOGGER)#
# def get_desc(self):
# # 		pass
#
# # class event_change(data_change):
# @measure(JUPITER_LOGGER)#
# def __init__(self,triggering_event_id):
# # 		data_change.__init__(self,triggering_event_id)
# @measure(JUPITER_LOGGER)#
# def get_host_farebasis(self):
# # 		pass
#
# # class type_of_market_change(event_change):
# @measure(JUPITER_LOGGER)#
# def __init__(self,triggering_event_id):
# # 		event_change.__init__(self,triggering_event_id)
# @measure(JUPITER_LOGGER)#
# def get_desc():
# # 		pass
#
# # class condition_of_market_change(event_change):
# @measure(JUPITER_LOGGER)#
# def __init__(self,triggering_event_id):
# # 		event_change.__init__(self,triggering_event_id)
# @measure(JUPITER_LOGGER)#
# def get_desc():
# # 		pass
#
# # class yq_trigger(event_change):
# @measure(JUPITER_LOGGER)#
# def __init__(self,triggering_event_id):
# # 		event_change.__init__(self,triggering_event_id)
# @measure(JUPITER_LOGGER)#
# def get_desc():
# # 		pass
#
# # class exchange_rate_change(event_change):
# @measure(JUPITER_LOGGER)#
# def __init__(self,triggering_event_id):
# # 		event_change.__init__(self,triggering_event_id)
# @measure(JUPITER_LOGGER)#
# def get_desc():
# # 		pass
#
# # class event(event_change):
# @measure(JUPITER_LOGGER)#
# def __init__(self,triggering_event_id):
# # 		event_change.__init__(self,triggering_event_id)
# @measure(JUPITER_LOGGER)#
# def get_desc():
# # 		pass
#
# # class cost_change(event_change):
# @measure(JUPITER_LOGGER)#
# def __init__(self,triggering_event_id):
# # 		event_change.__init__(self,triggering_event_id)
# @measure(JUPITER_LOGGER)#
# def get_desc():
# # 		pass
#
#
# class data_level(trigger):
# @measure(JUPITER_LOGGER)
# def __init__(self, triggering_event_id):
#         trigger.__init__(self, triggering_event_id)
#
# @measure(JUPITER_LOGGER)
# def get_host_doc(self):
#         host_fb, host_fl = get_host_fareladder(self.old_doc_data['pos'],
#                                                self.old_doc_data['origin'],
#                                                self.old_doc_data['destination'],
#                                                self.old_doc_data['compartment'],
#                                                self.old_doc_data['oneway_return'],
#                                                dep_date_start=self.triggering_data['dep_date_start'],
#                                                dep_date_end=self.triggering_data['dep_date_end'])
#         print host_fl
#         print self.price_recommendation
#         differences = map(lambda x: x-self.price_recommendation, host_fl)
#         zipped_list = zip(host_fb, host_fl, differences)
#         print zipped_list
#         sorted_tuple = sorted(zipped_list, key=lambda x:x[2])
#         host_fb = sorted_tuple[1][0]
#         host_fb_crsr = db.JUP_DB_ATPCO_Fares.find({
#             'pos': self.old_doc_data['pos'],
#             'origin': self.old_doc_data['origin'],
#             'destination': self.old_doc_data['destination'],
#             'compartment': self.old_doc_data['compartment'],
#             'fare_basis': host_fb,
#             'effective_from': {'$lte': SYSTEM_DATE},
#             'effective_to': {'$gte': SYSTEM_DATE},
#             # 'dep_date_from': {'$lte': self.triggering_data['tw']['departure']['to']},
#             # 'dep_date_to': {'$gte': self.triggering_data['tw']['departure']['from']}
#         })
#         if host_fb_crsr.count() == 1:
#             del host_fb_crsr[0]['_id']
#             self.old_doc_data.update(host_fb_crsr[0])
#             self.new_doc_data.update(host_fb_crsr[0])
#             self.host_pricing_data.update(host_fb_crsr[0])
#             return host_fb_crsr[0]
#         else:
#             return self.host_pricing_data
#
#
# class booking_changes_weekly(trigger):
# @measure(JUPITER_LOGGER)
# def __init__(self, triggering_event_id):
#         super(booking_changes_weekly, self).__init__(triggering_event_id)
#
# @measure(JUPITER_LOGGER)
# def get_desc(self):
#         desc = [
#             'For Departure date range [',
#             self.triggering_data['tw']['departure']['from'],
#             ',',
#             self.triggering_data['tw']['departure']['to'], ']:\n',
#             'booked from ', self.triggering_data['tw']['book']['from'],
#             ' to ', self.triggering_data['tw']['book']['to'], '\n',
#             'Bookings:', str(self.new_doc_data['bookings']), '\n',
#             'For Departure date range [',
#             self.triggering_data['lw']['departure']['from'],
#             ',',
#             self.triggering_data['lw']['departure']['to'], ']:\n',
#             'booked from ', self.triggering_data['lw']['book']['from'],
#             ' to ', self.triggering_data['lw']['book']['to'], '\n',
#             'Bookings:', str(self.old_doc_data['bookings']), '\n',
#
#             'The percentage change(this week vs last week) is beyond the thresholds defined', '\n',
#             'Lower Threshold:', str(self.lower_threshold), '\n',
#             'Upper Threshold:', str(self.upper_threshold)
#             # "Departure Period: " + self.old_doc_data
#             #
#             # Bkgs VTGT CY: -1.02
#             #
#             # Bkgs VTGT LY: 100
#             #
#             # Thresholds: -5 to 5
#             #
#             # Raised on: ddmmyyyy @ 15:30
#         ]
#         self.desc = ''.join(desc)
#         print self.desc
#
#
# class route_profitability_requirements(data_level):
# @measure(JUPITER_LOGGER)
# def __init__(self, triggering_event_id):
#         data_level.__init__(self, triggering_event_id)
#
# @measure(JUPITER_LOGGER)
# def get_host_farebasis(self):
#         self.farebasis = self.old_doc_data['fare_basis']
#
# @measure(JUPITER_LOGGER)
# def get_desc(self):
#         self.desc = ' '.join((
#                              "Recommendation for trigger raised because the value of route profitability is beyond thresholds as checked on ",
#                              str(self.triggering_event_date), " at ", str(self.triggering_event_time)))
#
#
# class pax_changes_weekly(data_level):
# @measure(JUPITER_LOGGER)
# def __init__(self, triggering_event_id):
#         data_level.__init__(self, triggering_event_id)
#
# @measure(JUPITER_LOGGER)
# def get_host_farebasis(self):
#         self.farebasis = self.old_doc_data['fare_basis']
#
# @measure(JUPITER_LOGGER)
# def get_desc(self):
#         self.desc = ' '.join((
#                              "Recommendation for trigger raised because the % change of Pax ",
#                              " this week with respect to the last week is beyond thresholds as checked on ",
#                              str(self.triggering_event_date), " at ", str(self.triggering_event_time)))
#
#
# class pax_changes_VLYR(data_level):
# @measure(JUPITER_LOGGER)
# def __init__(self, triggering_event_id):
#         data_level.__init__(self, triggering_event_id)
#
# @measure(JUPITER_LOGGER)
# def get_host_farebasis(self):
#         self.farebasis = self.old_doc_data['fare_basis']
#
# @measure(JUPITER_LOGGER)
# def get_desc(self):
#         desc = [
#             "Departure Period: ", self.triggering_data['dep_date_start'], ' to ', self.triggering_data['dep_date_end'],
#             '_',
#             'Pax Last Year', self.old_doc_data['pax'],
#             '_',
#             'Capacity Last Year', self.old_doc_data['capacity'],
#             '_',
#             'Capacity This Year', self.new_doc_data['capacity'],
#             '_',
#             'Expected Pax This Year', self.new_doc_data['pax_expected'],
#             '_',
#             'Actual Pax This Year', self.new_doc_data['pax'],
#             '_',
#             'Thresholds:' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
#             '_',
#             'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
#             # ]
#             # 'For Departure date range [',
#             # self.triggering_data['dep_date_start'],
#             # ',',
#             # self.triggering_data['dep_date_end'], ']:\n',
#             # 'As Observed on ', str(self.triggering_event_date),
#             # 'at', str(self.triggering_event_time), '\n',
#             # 'Bookings This Yr:', str(self.old_doc_data['bookings']), '\n',
#             # 'Bookings Last Yr:', str(self.new_doc_data['bookings']), '\n',
#             # 'Capacity This Yr:', str(self.old_doc_data['capacity']), '\n',
#             # 'Capacity Last Yr:', str(self.new_doc_data['capacity']), '\n',
#             # 'Expected Bookings This Yr:', str(self.new_doc_data['bookings_expected']), '\n',
#             # 'The percentage change(actual vs expected) is beyond the thresholds defined', '\n',
#             # 'Lower Threshold:', str(self.lower_threshold), '\n',
#             # 'Upper Threshold:', str(self.upper_threshold)
#         ]
#         self.desc = ''.join(desc)
#         # self.desc = ' '.join((
#         #                      "Recommendation for trigger raised because the % change of bookings for departure dates ",
#         #                      str(datetime.date.today())," to ",
#         #                      datetime.datetime.strftime(datetime.datetime.now()+datetime.timedelta(days=90),'%Y-%m-%d'),
#         #                      "this year(",str(self.new_doc_data['value']), ") with respect to the last year(",
#         #                      str(self.old_doc_data['value']), ") is beyond thresholds(",
#         #                      str(self.lower_threshold), "-", str(self.upper_threshold), ") as checked on ",
#         #                      str(self.triggering_event_date), " at ", str(self.triggering_event_time)))
#
#
# class pax_changes_VTGT(data_level):
# @measure(JUPITER_LOGGER)
# def __init__(self, triggering_event_id):
#         data_level.__init__(self, triggering_event_id)
#
# @measure(JUPITER_LOGGER)
# def get_host_farebasis(self):
#         self.farebasis = self.old_doc_data['fare_basis']
#
# @measure(JUPITER_LOGGER)
# def get_desc(self):
#         self.desc = ' '.join((
#                              "Recommendation for trigger raised because the % change of Pax with respect to the target is beyond thresholds as checked on ",
#                              str(self.triggering_event_date), " at ", str(self.triggering_event_time)))
#
#
# class yield_changes_weekly(data_level):
# @measure(JUPITER_LOGGER)
# def __init__(self, triggering_event_id):
#         data_level.__init__(self, triggering_event_id)
#
# @measure(JUPITER_LOGGER)
# def get_host_farebasis(self):
#         self.farebasis = self.old_doc_data['fare_basis']
#
# @measure(JUPITER_LOGGER)
# def get_desc(self):
#         self.desc = ' '.join((
#                              "Recommendation for trigger raised because the % change of Yield this week with respect to the last week is beyond thresholds as checked on ",
#                              str(self.triggering_event_date), " at ", str(self.triggering_event_time)))
#
#
# class yield_changes_VLYR(data_level):
# @measure(JUPITER_LOGGER)
# def __init__(self, triggering_event_id):
#         data_level.__init__(self, triggering_event_id)
#
# @measure(JUPITER_LOGGER)
# def get_host_farebasis(self):
#         self.farebasis = self.old_doc_data['fare_basis']
#
# @measure(JUPITER_LOGGER)
# def get_desc(self):
#         desc = [
#             "Departure Period: ", self.triggering_data['dep_date_start'], ' to ', self.triggering_data['dep_date_end'],
#             '_',
#             'Yield Last Year', self.old_doc_data['yield'],
#             '_',
#             'Capacity Last Year', self.old_doc_data['capacity'],
#             '_',
#             'Capacity This Year', self.new_doc_data['capacity'],
#             '_',
#             'Expected Yield This Year', self.new_doc_data['yield_expected'],
#             '_',
#             'Actual Yield This Year', self.new_doc_data['yield'],
#             '_',
#             'Thresholds:' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
#             '_',
#             'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
#             # ]
#             # 'For Departure date range [',
#             # self.triggering_data['dep_date_start'],
#             # ',',
#             # self.triggering_data['dep_date_end'], ']:\n',
#             # 'As Observed on ', str(self.triggering_event_date),
#             # 'at', str(self.triggering_event_time), '\n',
#             # 'Bookings This Yr:', str(self.old_doc_data['bookings']), '\n',
#             # 'Bookings Last Yr:', str(self.new_doc_data['bookings']), '\n',
#             # 'Capacity This Yr:', str(self.old_doc_data['capacity']), '\n',
#             # 'Capacity Last Yr:', str(self.new_doc_data['capacity']), '\n',
#             # 'Expected Bookings This Yr:', str(self.new_doc_data['bookings_expected']), '\n',
#             # 'The percentage change(actual vs expected) is beyond the thresholds defined', '\n',
#             # 'Lower Threshold:', str(self.lower_threshold), '\n',
#             # 'Upper Threshold:', str(self.upper_threshold)
#         ]
#         self.desc = ''.join(desc)
#         # self.desc = ' '.join((
#         #                      "Recommendation for trigger raised because the % change of bookings for departure dates ",
#         #                      str(datetime.date.today())," to ",
#         #                      datetime.datetime.strftime(datetime.datetime.now()+datetime.timedelta(days=90),'%Y-%m-%d'),
#         #                      "this year(",str(self.new_doc_data['value']), ") with respect to the last year(",
#         #                      str(self.old_doc_data['value']), ") is beyond thresholds(",
#         #                      str(self.lower_threshold), "-", str(self.upper_threshold), ") as checked on ",
#         #                      str(self.triggering_event_date), " at ", str(self.triggering_event_time)))
#
#
#
#
# class opportunities(data_level):
# @measure(JUPITER_LOGGER)
# def __init__(self, triggering_event_id):
#         data_level.__init__(self, triggering_event_id)
#
# @measure(JUPITER_LOGGER)
# def get_host_farebasis(self):
#         self.farebasis = self.old_doc_data['fare_basis']
#
# @measure(JUPITER_LOGGER)
# def get_desc(self):
#         self.desc = ' '.join((
#                              "Recommendation for trigger raised because the % change of market share with respect to fair market share is beyond thresholds as checked on ",
#                              str(self.triggering_event_date), " at ", str(self.triggering_event_time)))
#
#
# class revenue_changes_weekly(data_level):
# @measure(JUPITER_LOGGER)
# def __init__(self, triggering_event_id):
#         data_level.__init__(self, triggering_event_id)
#
# @measure(JUPITER_LOGGER)
# def get_host_farebasis(self):
#         self.farebasis = self.old_doc_data['fare_basis']
#
# @measure(JUPITER_LOGGER)
# def get_desc(self):
#         self.desc = ''.join(["Recommendation for trigger raised because the % change in revenenue for departures this week(",
#                              str(self.new_doc_data['value']), ") with respect to the last week(",
#                              str(self.old_doc_data['value']), ") is beyond thresholds(",
#                              str(self.lower_threshold), ",", str(self.upper_threshold), ") as checked on ",
#                              str(self.triggering_event_date), " at ", str(self.triggering_event_time)])
#
#
# class revenue_changes_VLYR(data_level):
# @measure(JUPITER_LOGGER)
# def __init__(self, triggering_event_id):
#         data_level.__init__(self, triggering_event_id)
#
# @measure(JUPITER_LOGGER)
# def get_host_farebasis(self):
#         self.farebasis = self.old_doc_data['fare_basis']
#
# @measure(JUPITER_LOGGER)
# def get_desc(self):
#         desc = [
#             "Departure Period: ", self.triggering_data['dep_date_start'], ' to ', self.triggering_data['dep_date_end'],
#             '_',
#             'Revenue Last Year', self.old_doc_data['revenue'],
#             '_',
#             'Capacity Last Year', self.old_doc_data['capacity'],
#             '_',
#             'Capacity This Year', self.new_doc_data['capacity'],
#             '_',
#             'Expected Revenue This Year', self.new_doc_data['revenue_expected'],
#             '_',
#             'Actual Revenue This Year', self.new_doc_data['revenue'],
#             '_',
#             'Thresholds:' + str(self.lower_threshold) + ' to ' + str(self.upper_threshold),
#             '_',
#             'Raised On ' + str(self.triggering_event_date) + ' at ' + str(self.triggering_event_time)
#             # ]
#             # 'For Departure date range [',
#             # self.triggering_data['dep_date_start'],
#             # ',',
#             # self.triggering_data['dep_date_end'], ']:\n',
#             # 'As Observed on ', str(self.triggering_event_date),
#             # 'at', str(self.triggering_event_time), '\n',
#             # 'Bookings This Yr:', str(self.old_doc_data['bookings']), '\n',
#             # 'Bookings Last Yr:', str(self.new_doc_data['bookings']), '\n',
#             # 'Capacity This Yr:', str(self.old_doc_data['capacity']), '\n',
#             # 'Capacity Last Yr:', str(self.new_doc_data['capacity']), '\n',
#             # 'Expected Bookings This Yr:', str(self.new_doc_data['bookings_expected']), '\n',
#             # 'The percentage change(actual vs expected) is beyond the thresholds defined', '\n',
#             # 'Lower Threshold:', str(self.lower_threshold), '\n',
#             # 'Upper Threshold:', str(self.upper_threshold)
#         ]
#         self.desc = ''.join(desc)
#         # self.desc = ' '.join((
#         #                      "Recommendation for trigger raised because the % change of bookings for departure dates ",
#         #                      str(datetime.date.today())," to ",
#         #                      datetime.datetime.strftime(datetime.datetime.now()+datetime.timedelta(days=90),'%Y-%m-%d'),
#         #                      "this year(",str(self.new_doc_data['value']), ") with respect to the last year(",
#         #                      str(self.old_doc_data['value']), ") is beyond thresholds(",
#         #                      str(self.lower_threshold), "-", str(self.upper_threshold), ") as checked on ",
#         #                      str(self.triggering_event_date), " at ", str(self.triggering_event_time)))
#
#
# class revenue_changes_VTGT(data_level):
# @measure(JUPITER_LOGGER)
# def __init__(self, triggering_event_id):
#         data_level.__init__(self, triggering_event_id)
#
# @measure(JUPITER_LOGGER)
# def get_host_farebasis(self):
#         self.farebasis = self.old_doc_data['fare_basis']
#
# @measure(JUPITER_LOGGER)
# def get_desc(self):
#         self.desc = ''.join(["Recommendation for trigger raised because the For the next 90 departure dates",
#                              "% change in actual revenue(",str(self.new_doc_data['value']),') ',
#                              " with respect to the target revenue (",
#                              str(self.old_doc_data['value']),')'," is beyond thresholds(",str(self.lower_threshold),",",
#                              str(self.upper_threshold),") as checked on ",
#                              str(self.triggering_event_date)," at ",str(self.triggering_event_time)])
#
#
# """
# class manual(trigger):
# @measure(JUPITER_LOGGER)
# def __init__(self,triggering_event_id):
#         trigger.__init__(self,triggering_event_id)
# @measure(JUPITER_LOGGER)
# def get_host_farebasis(self):
#         self.farebasis = self.old_doc_data['farebasis']
#
# class percentage_change(manual):
# @measure(JUPITER_LOGGER)
# def __init__(self,triggering_event_id):
#         manual.__init__(self,triggering_event_id)
# @measure(JUPITER_LOGGER)
# def get_price_recommendation(self):
#         self.price_recommendation = self.old_doc_data['price']*(1+self.new_doc_data[percentage_change])
#         self.percent_change = 'null'
#         self.abs_change = 'null'
#         self.ref_farebasis = 'null'
#         self.process_end_date = datetime.datetime.now().strftime('%Y-%m-%d')
#         self.processed_end_time = datetime.datetime.now().strftime('%H:%M:%S.%f')
#         self.batch_price = 'null'
#
# class adhoc_percentage_price_change(percentage_change):
# @measure(JUPITER_LOGGER)
# def __init__(self,triggeirng_event_id):
#         percentage_change.__init__(self,triggering_event_id)
# @measure(JUPITER_LOGGER)
# def get_desc(self):
#         self.desc =' '.join(("Recommendation for trigger raised because an adhoc request for a percentage price change",
# 					             self.old_doc_data['airline'].encode()," from ",str(self.old_doc_data['price'])," to ",
# 					             str(self.new_doc_data['price'])," on ",str(self.triggering_event_date)," at ",str(self.triggering_event_time)))
#
#
# class sales_review_request_percentage_change(percentage_change):
# @measure(JUPITER_LOGGER)
# def __init__(self,triggeirng_event_id):
#         percentage_change.__init__(self,triggering_event_id)
# @measure(JUPITER_LOGGER)
# def get_desc(self):
#         self.desc = ' '.join(("Recommendation for trigger raised because of sales request for percentage price change",
#                                   self.old_doc_data['airline'].encode(), " from ", str(self.old_doc_data['price'])," to ",
#                                   str(self.new_doc_data['price']), " on ", str(self.triggering_event_date), " at ",str(self.triggering_event_time))
#
# class absolute_change(manual):
# @measure(JUPITER_LOGGER)
# def __init__(self,triggering_event_id):
#         manual.__init__(self,triggering_event_id)
# @measure(JUPITER_LOGGER)
# def get_price_recommendation(self):
#         self.price_recommendation = self.old_doc_data['price']+self.new_doc_data[absolute_change]
#         self.percent_change = 'null'
#         self.abs_change = 'null'
#         self.ref_farebasis = 'null'
#         self.process_end_date = datetime.datetime.now().strftime('%Y-%m-%d')
#         self.processed_end_time = datetime.datetime.now().strftime('%H:%M:%S.%f')
#         self.batch_price = 'null'
#
# class adhoc_absolute_price_change(absolute_change):
# @measure(JUPITER_LOGGER)
# def __init__(self,triggering_event_id):
#         absolute_change.__init__(self,triggering_event_id)
# @measure(JUPITER_LOGGER)
# def get_desc(self):
#         self.desc = ' '.join(("Recommendation for trigger raised because an adhoc request for a absolute price change",
#                                   self.old_doc_data['airline'].encode(), " from ", str(self.old_doc_data['price']), " to ",
#                                   str(self.new_doc_data['price']), " on ", str(self.triggering_event_date), " at ",str(self.triggering_event_time)))
#
# class sales_review_request_absolute_change(absolute_change):
# @measure(JUPITER_LOGGER)
# def __init__(self,triggering_event_id):
#         absolute_change.__init__(self,triggering_event_id)
# @measure(JUPITER_LOGGER)
# def get_desc(self):
#         self.desc = ' '.join(("Recommendation for trigger raised because an sales request for absolute price change",
#                                   self.old_doc_data['airline'].encode(), " from ", str(self.old_doc_data['price']), " to ",
#                                   str(self.new_doc_data['price']), " on ", str(self.triggering_event_date), " at ",str(self.triggering_event_time)))
#
# class direct(manual):
# @measure(JUPITER_LOGGER)
# def __init__(self,triggering_event_id):
#         manual.__init__(self,triggering_event_id)
# @measure(JUPITER_LOGGER)
# def get_price_recommendation(self):
#         self.price_recommendation = self.new_doc_data['price']
#         self.percent_change = 'null'
#         self.abs_change = 'null'
#         self.ref_farebasis = 'null'
#         self.process_end_date = datetime.datetime.now().strftime('%Y-%m-%d')
#         self.processed_end_time = datetime.datetime.now().strftime('%H:%M:%S.%f')
#         self.batch_price = 'null'
#
# class farebasis_upload(direct):
# @measure(JUPITER_LOGGER)
# def __init__(self,triggering_event_id):
#         direct.__init__(self,triggering_event_id)
# @measure(JUPITER_LOGGER)
# def get_desc(self):
# @measure(JUPITER_LOGGER)
# def get_desc(self):
#             self.desc = ' '.join(("Recommendation for trigger raised because to upload fairbasis",
#                                       self.old_doc_data['airline'].encode(), " from ", str(self.old_doc_data['price']), " to ",
#                                       str(self.new_doc_data['price']), " on ", str(self.triggering_event_date), " at ",str(self.triggering_event_time)))
#
# class sales_review_request_upload(direct):
# @measure(JUPITER_LOGGER)
# def __init__(self,triggering_event_id):
#         direct.__init__(self,triggering_event_id)
# @measure(JUPITER_LOGGER)
# def get_desc(self):
#         self.desc = ' '.join(("Recommendation for trigger raised because to upload sales review request fairbasis",
#                                   self.old_doc_data['airline'].encode(), " from ", str(self.old_doc_data['price']), " to ",
#                                   str(self.new_doc_data['price']), " on ", str(self.triggering_event_date), " at ", str(self.triggering_event_time)))
#
# class no_price_action(manual):
# @measure(JUPITER_LOGGER)
# def __init__(self,triggering_event_id):
#         manual.__init__(self,triggering_event_id)
# @measure(JUPITER_LOGGER)
# def get_price_recommendation(self):
#         self.price_recommendation = 'null'
#         self.percent_change = 'null'
#         self.abs_change = 'null'
#         self.ref_farebasis = 'null'
#         self.process_end_date = datetime.datetime.now().strftime('%Y-%m-%d')
#         self.processed_end_time = datetime.datetime.now().strftime('%H:%M:%S.%f')
#         self.batch_price = 'null'
#
# class sales_review_request_no_action(no_price_action):
# @measure(JUPITER_LOGGER)
# def __init(self,triggering_event_id):
#         no_price_action.__init__(self,triggering_event_id)
# @measure(JUPITER_LOGGER)
# def get_desc(self):
#         self.desc = ' '.join(("Recommendation for trigger raised because sales review request no action change",
#                                   self.old_doc_data['airline'].encode(), " from ", str(self.old_doc_data['price']), " to ",
#                                   str(self.new_doc_data['price']), " on ", str(self.triggering_event_date), " at ", str(self.triggering_event_time)))
# """
# '''