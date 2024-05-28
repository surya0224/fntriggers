import inspect
import json

from copy import deepcopy

import time

import datetime

from jupiter_AI.logutils import measure
from jupiter_AI.triggers.mrkt_params_workflow_opt import comp_summary_python
from jupiter_AI.triggers.host_params_workflow_opt import main_func as host_workflow_params
# from jupiter_AI.triggers.CategorizeRecommendation import Availability
from bson import json_util
from jupiter_AI import mongo_client, JUPITER_LOGGER
import jupiter_AI.triggers.analytics_functions as af
#from jupiter_AI.network_level_params import JUPITER_DB, Host_Airline_Hub
from jupiter_AI.triggers.CategorizeRecommendation import Category
from jupiter_AI.triggers.common import generate_trigger_id
from jupiter_AI.triggers.recommendation_models.oligopoly_fl_gen import main
#db = client[JUPITER_DB]


class Trigger(object):
    """
    """

    @measure(JUPITER_LOGGER)
    def __init__(self):
        pass

    @staticmethod
    @measure(JUPITER_LOGGER)
    def get_module_name():
        return inspect.stack()[1][3]

    @staticmethod
    @measure(JUPITER_LOGGER)
    def get_arg_lists(frame):
        """
        function used to get the list of arguments of the function
        where it is called
        """
        args, _, _, values = inspect.getargvalues(frame)
        argument_name_list = []
        argument_value_list = []
        for k in args:
            argument_name_list.append(k)
            argument_value_list.append(values[k])
        return argument_name_list, argument_value_list

    # --------------------------------- FUNCTION TO GET THE RELEVANT DATA FOR

    @measure(JUPITER_LOGGER)
    def get_trigger_details(self, trigger_name, db):
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
        # print 'Name of Trigger', trigger_name
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
        st = time.time()
        # db.system_js.JUP_FN_Configuration(
        #     pos,
        #     origin,
        #     destination,
        #     compartment,
        #     "temp_collection_sai",
        #     'trigger')
        # # print 'Config function run', time.time() - st
        # st = time.time()
        # crsr = db['temp_collection_sai'].find()
        # # print 'Config temp col find', time.time() - st
        # st = time.time()
        # trigger_doc = dict()
        # if crsr.count() == 0:
        #     for doc in crsr:
        #         doc = doc['p_config']
        #         if doc['Trigger_type'] == trigger_name:
        #             trigger_doc = doc
        # if trigger_doc:
        #     self.trigger_type = trigger_name
        #     self.lower_threshold = trigger_doc['parameters']['lower_threshold']
        #     self.upper_threshold = trigger_doc['parameters']['upper_threshold']
        #     self.threshold_type = trigger_doc['parameters']['threshold_percentage_or_absolute']
        #     self.priority = trigger_doc['parameters']['priority']
        #     self.min_interval = trigger_doc['parameters']['min_interval_to_retrigger_hrs']
        # else:
        print "--------------------> Skipped FN function"
        trigger_crsr = db.JUP_DB_Trigger_Types.find({
            'desc': trigger_name
        })
        trigger_crsr = list(trigger_crsr)
        if len(trigger_crsr) >= 1:
            # print "--------> going"
            self.trigger_type = trigger_crsr[0]['desc']
            self.lower_threshold = trigger_crsr[0]['lower_threshhold']
            self.upper_threshold = trigger_crsr[0]['upper_threshhold']
            self.threshold_type = trigger_crsr[0]['threshold_abs/percent']
            self.priority = trigger_crsr[0]['priority']
            self.min_interval = trigger_crsr[0]['min_interval_to_retrigger_hours']
        # print 'Obtaining', time.time() - st
        st = time.time()
        # db['temp_collection_sai'].drop()

    # --------------------------------- FUNCTION TO GENERATE COMP LEVEL PERFOR

    @measure(JUPITER_LOGGER)
    def get_compartment_level_data(
            self,
            pos,
            origin,
            destination,
            compartment,
            dep_date_start,
            dep_date_end, db):
        """
        :return:
        """
        # from jupiter_AI.triggers.workflow_parameters import update_host_workflow_params_comp
        # from jupiter_AI.triggers.workflow_parameters import update_market_workflow_params
        # dict_inp = dict(
        #     pos=self.old_doc_data['pos'],
        #     origin=self.old_doc_data['origin'],
        #     destination=self.old_doc_data['destination'],
        #     compartment=self.old_doc_data['compartment'],
        #     trigger_type=self.trigger_type,
        #     triggering_data=self.triggering_data,
        #     currency=db.JUP_DB_ATPCO_Fares.find({'origin': self.old_doc_data['origin']}).limit(1)[0]['currency']
        # )
        # dict_inp = update_host_workflow_params_comp(dict_inp)
        # dict_inp = update_market_workflow_params(dict_inp)
        #
        # params = ['pos', 'origin', 'destination', 'compartment', 'trigger_type', 'triggering_data', 'currency']
        # for key in params:
        #     if key in dict_inp.keys():
        #         del dict_inp[key]
        # return dict_inp
        response = dict()
        host_params = host_workflow_params(
            pos,
            origin,
            destination,
            compartment,
            dep_date_start,
            dep_date_end, db=db)
        mrkt_params = comp_summary_python(
            pos,
            origin,
            destination,
            compartment,
            dep_date_start,
            dep_date_end, db=db)
        response.update(host_params)
        response.update(mrkt_params)
        return response

    # --------------------------------- FUNCTION TO OBTAIN THE LIST OF FARE TO

    @measure(JUPITER_LOGGER)
    def get_category_details(self, db):
        print "old_doc in main class: ", self.old_doc_data
        print "new_doc in main class: ", self.new_doc_data
        dict_inp = dict(
            origin=self.new_doc_data['origin'],
            destination=self.new_doc_data['destination'],
            triggering_data=self.triggering_data
        )
        cat = Category(dict_inp)
        cat.do_analysis(db=db)
        cat_details = cat.__dict__
        # print cat_details
        del cat_details['reco']
        return cat_details
        # self.recommendation_category = cat_details['category']
        # self.recommendation_score = cat_details['score']
        # self.recommendation_category_details = cat_details

    # --------------------------------- FUNCTION TO OBTAIN THE LIST OF FARE TO

    @measure(JUPITER_LOGGER)
    def obtain_fares(
            self,
            pos,
            origin,
            destination,
            compartment,
            dep_date_start,
            dep_date_end,
            db):
        """
        Obtain the fares for the market in consideration that are effective today and are applicable b/w
        the departure dates
        Arguments:
        :param pos:
        :param origin:
        :param destination:
        :param compartment:
        :param dep_date_start:
        :param dep_date_end:
        :returns list of fares for which trigger is applicable
        """

        # query = dict()
        # query['competitor_farebasis'] = {'$ne': None}
        # if pos:
        #     query['pos'] = pos
        # if origin:
        #     query['origin'] = origin
        # if destination:
        #     query['destination'] = destination
        # if compartment:
        #     query['compartment'] = compartment
        # print query
        # fares_crsr = db.JUP_DB_ATPCO_Fares.find(query)

        # print {
        #     # 'competitor_farebasis': {'$ne': None},
        #     'pos': pos,
        #     'origin': origin,
        #     'destination': destination,
        #     'compartment': compartment
        # }
        #   Checking if the fare is applicable in the departure date range
        # fare_interim_docs = []
        # for fare_doc in fares_crsr:
        #     print fare_doc
        #     if fare_doc['dep_date_from'] and fare_doc['dep_date_to']:
        #         if fare_doc['dep_date_from'] < dep_date_end or fare_doc['dep_date_to'] > dep_date_start:
        #             fare_interim_docs.append(fare_doc)
        #     elif fare_doc['dep_date_from']:
        #         if fare_doc['dep_date_from'] < dep_date_end:
        #             fare_interim_docs.append(fare_doc)
        #     elif fare_doc['dep_date_to']:
        #         if fare_doc['dep_date_to'] > dep_date_start:
        #             fare_interim_docs.append(fare_doc)
        #     else:
        #         fare_interim_docs.append(fare_doc)
        #
        # print 'Fares Interim', fare_interim_docs
        # fares = []
        # for fare_doc in fare_interim_docs:
        #     if fare_doc['effective_from'] and fare_doc['effective_to']:
        #         if fare_doc['effective_from'] <= self.trigger_date <= fare_doc['effective_to']:
        #             fares.append(fare_doc)
        #         else:
        #             pass
        #     elif fare_doc['effective_from']:
        #         if fare_doc['effective_from'] <= self.trigger_date:
        #             fares.append(fare_doc)
        #     elif fare_doc['effective_to']:
        #         if fare_doc['effective_to'] >= self.trigger_date:
        #             fares.append(fare_doc)
        #     else:
        #         fares.append(fare_doc)
        # print len(fares)
        fares = main(pos=pos,
                     origin=origin,
                     destination=destination,
                     compartment=compartment,
                     dep_date_start=dep_date_start,
                     dep_date_end=dep_date_end,
                     db=db)
        return fares

    #   --------------------------------- METHOD TO GENERATE THE DATA LEVEL TR

    @measure(JUPITER_LOGGER)
    def generate_trigger(self, trigger_status, dep_date_start, dep_date_end, db):
        """
        Collects all the fares applicable and generates the triggers.
        :param trigger_status:
        :param dep_date_start:
        :param dep_date_end:
        :return:
        """
        #   If trigger is raised then create the triggers
        #   else Do Nothing
        # print 'OK'
        self.trigger_id = generate_trigger_id(trigger_name=self.trigger_type, db=db)
        # print 'Trigger Id', self.trigger_id
        # print 'Trigger Status', trigger_status
        if trigger_status:
            # fbmapping_batch_run(dep_date_start=dep_date_start,
            #                     dep_date_end=dep_date_end)
            # # recommendation_batch()
            self.cat_details = self.get_category_details(db=db)

            if self.trigger_type == 'competitor_price_change':
                self.cat_details['category'] = 'A'

            try:
                pos = self.old_doc_data['pos']
            except KeyError:
                pos = None

            try:
                compartment = self.old_doc_data['compartment']
            except KeyError:
                compartment = None

            self.comp_level_data = self.get_compartment_level_data(
                pos=pos,
                origin=self.old_doc_data['origin'],
                destination=self.old_doc_data['destination'],
                compartment=compartment,
                dep_date_start=dep_date_start,
                dep_date_end=dep_date_end,
                db=db)

            fares = self.obtain_fares(self.old_doc_data['pos'],
                                      self.old_doc_data['origin'],
                                      self.old_doc_data['destination'],
                                      self.old_doc_data['compartment'],
                                      dep_date_start,
                                      dep_date_end,
                                      db=db)

            # print 'no_of_fares', len(fares)

            for fare in fares:
                # del fare['_id']
                # print 'fare doc', fare
                #   Updating the old document with the fare considering
                # self.old_doc_data.update(fare)
                # self.old_doc_data['host_farebasis'] = fare['fare_basis']
                self.host_fare_doc = fare
                #   Updating the new document with the fare considering
                # self.new_doc_data.update(fare)
                # self.new_doc_data['host_farebasis'] = fare['fare_basis']

                #   To be visited Again
                self.pricing_action_id_at_trigger_time = None
                # print self.__dict__
                # print json.dumps(self.__dict__, default=json_util.default,
                # indent=1)

                #   Creating the Trigger
                id = af.create_trigger(self, db=db)

                # This call creates the message to be sent into the queue and
                # sents it to the respective queue no
                af.send_trigger_to_queue(self, id=id)

    @measure(JUPITER_LOGGER)
    def generate_trigger_new(
            self,
            trigger_status,
            dep_date_start,
            dep_date_end,
            db,
            row=None,
            is_event_trigger=False):
        """
        :param trigger_status:
        :param dep_date_start:
        :param dep_date_end:
        :return:
        """
        if trigger_status:
            # print "old doc------>"
            # print self.old_doc_data
            dep_date_start = datetime.datetime.strptime(
                self.triggering_data['dep_date_start'],
                '%Y-%m-%d').strftime('%d-%m-%Y')
            dep_date_end = datetime.datetime.strptime(
                self.triggering_data['dep_date_end'],
                '%Y-%m-%d').strftime('%d-%m-%Y')
            self.trigger_id, self.unique_trigger_id = generate_trigger_id(
                trigger_name=self.trigger_type,
                pct_change=self.change,
                dep_date_start=dep_date_start,
                dep_date_end=dep_date_end,
                origin=self.old_doc_data['origin'],
                destination=self.old_doc_data['destination'],
                db=db)

            print "trigger type: ", self.trigger_type
            print 'Trigger Id', self.trigger_id
            # print 'Trigger Status', trigger_status
            self.cat_details = self.get_category_details(db=db)
            if self.trigger_type == 'competitor_price_change':
                self.cat_details['category'] = 'A'
            #   To be visited Again
            self.pricing_action_id_at_trigger_time = None
            # print self.__dict__
            # print json.dumps(self.__dict__, default=json_util.default, indent=1)
            #   Creating the Trigger
            id = af.create_trigger(self, is_event_trigger=is_event_trigger, db=db)
            # This call creates the message to be sent into the queue and sents
            # it to the respective queue no
            if self.trigger_type in ['new_promotions', 'promotions_farechange', 'promotions_dateschange', 'promotions_ruleschange']:
                print "----->", id
                af.send_trigger_to_queue(self, id=id)
            else:
                return id
