"""
__author__ = 'Sai Krishna'
__copyright__ = 'Copyright (C) 2017 Flynava Technologies'
__desc__ = ''
__version__ = '1.0'
__created_date__ = '2017-01-23'
Summary -
For Documentation Refer to
http://confluence.flynava.com/display/AN/Recommendation+Categorization

Note -
input 'recommendation' implies the entire document being uploaded in to
the pricing actions collection in the dict format.

Modifications Log -
"""
import datetime
import math
from collections import defaultdict
from jupiter_AI import mongo_client, JUPITER_LOGGER
from jupiter_AI.logutils import measure
from jupiter_AI.network_level_params import Host_Airline_Hub, SYSTEM_DATE, JUPITER_DB
from jupiter_AI.network_level_params import recommendation_lower_threshold
from jupiter_AI.network_level_params import recommendation_upper_threshold
from jupiter_AI.tiles.jup_common_functions_tiles import gen_collection_name
#db = client[JUPITER_DB]

class Factor(object):
    "Generic Class for a factor"
    @measure(JUPITER_LOGGER)
    def __init__(self, recommendation):
        self.reco = recommendation

    @measure(JUPITER_LOGGER)
    def do_analysis(self, db):
        """
            Main function of any factor class
        """
        self.check_consideration(db=db)
        self.get_weights(db=db)
        self.get_values(db)
        self.get_multiplier()

    @measure(JUPITER_LOGGER)
    def check_consideration(self, db):
        """
        Checks whether this factor is to be considered or not
        """
        pass

    @measure(JUPITER_LOGGER)
    def get_weights(self,db):
        """
        If being considered get the weights for the factors
        else return weight as 'NA'
        """
        pass

    @measure(JUPITER_LOGGER)
    def get_values(self, db):
        """
        If the factor is being considered get the relevant values
        """
        pass

    @measure(JUPITER_LOGGER)
    def get_multiplier(self):
        """
        After obtaining the relevant values
        Calculation of Multiplier
        """
        pass


class AgeOfTrigger(Factor):
    """
    """
    @measure(JUPITER_LOGGER)
    def __init__(self, recommendation):
        super(AgeOfTrigger, self).__init__(recommendation)
        self.reco = recommendation
        self.consideration = True

    @measure(JUPITER_LOGGER)
    def get_weights(self, db):
        """
            Getting the weights for the change 'recommended factor'.
        """
        if self.consideration:
            wgt_crsr = db.JUP_DB_Recommendation_Priority.find({
                'type': 'age_of_trigger'
            })
            if wgt_crsr.count() == 1:
                self.weight = wgt_crsr[0]['weightage']
            else:
                print 'ERROR'

    @measure(JUPITER_LOGGER)
    def get_values(self, db):
        """
        :return:
        """
        today_obj = datetime.datetime.now()
        trigger_date_str = self.reco['triggering_event_date']
        trigger_date_obj = datetime.datetime.strptime(trigger_date_str, '%Y-%m-%d')
        days = (today_obj - trigger_date_obj).days
        self.age_of_trigger = days

    @measure(JUPITER_LOGGER)
    def get_multiplier(self):
        """
        :return:
        """
        self.multiplier = 0
        if self.check_consideration:
            if type(self.age_of_trigger) is int:
                self.multiplier = (2/math.pi) * math.atan(self.age_of_trigger)


class ChangeRecommended(Factor):
    @measure(JUPITER_LOGGER)
    def __init__(self, recommendation):
        super(ChangeRecommended, self).__init__(recommendation)
        self.reco = recommendation

    @measure(JUPITER_LOGGER)
    def check_consideration(self, db):
        """
        Checking whether to consider this factor or not.
        """
        # If both price recommended and the old price are valid the factor
        # can be considered.
        if (type(self.reco['host_pricing_data']['total_fare']) in [int, float] and
                type(self.reco['price_recommendation']) in [int, float]):
                self.consideration = True
        else:
            self.consideration = False

    @measure(JUPITER_LOGGER)
    def get_weights(self, db):
        """
        Getting the weights for the change 'recommended factor'.
        """
        if self.consideration:
            wgt_crsr = db.JUP_DB_Recommendation_Priority.find({
                'type': 'original_change'
            })
            if wgt_crsr.count() == 1:
                self.weight = wgt_crsr[0]['weightage']
            else:
                print 'ERROR'

    @measure(JUPITER_LOGGER)
    def get_values(self, db):
        """
            Get the value of percentage change recommended over the already
            existing fare.
        """
        if self.consideration:
            old = self.reco['host_pricing_data']['total_fare']
            recommended = self.reco['price_recommendation']
            self.change_recommended = recommended - old
            self.change_recommended_perc = round((recommended - old) *100 / old ,
                                        2)

        else:
            self.change_recommended = 'NA'
            self.change_recommended_perc = 'NA'

    @measure(JUPITER_LOGGER)
    def get_multiplier(self):
        """
            Calculation of the multiplier for the 'change recommended' factor
            to be used in calculation of score.
        """
        if self.consideration:
            chng = self.change_recommended_perc
            self.upper_threshold = recommendation_upper_threshold
            self.lower_threshold = recommendation_lower_threshold
            if chng >= 0:
                self.multiplier = chng/self.upper_threshold
            else:
                self.multiplier = chng/self.lower_threshold


class Closeness(Factor):
    @measure(JUPITER_LOGGER)
    def __init__(self, recommendation):
        super(Closeness, self).__init__(recommendation)
        self.reco = recommendation

    @measure(JUPITER_LOGGER)
    def check_consideration(self, db):
        """
        """
        if self.reco['effective_date_to'] not in [None, 'NA', 'null']:
            try:
                end_date_obj = datetime.datetime.strptime(
                                    self.reco['effective_date_to'],
                                    '%Y-%m-%d')
                self.consideration = True
            except ValueError:
                self.consideration = False
        else:
            self.consideration = False

    @measure(JUPITER_LOGGER)
    def get_weights(self, db):
        crsr_wgts = db.JUP_DB_Recommendation_Priority.find({
            'type': 'expiring_soon'
        })
        if crsr_wgts.count() == 1:
            self.weight = crsr_wgts[0]['weightage']
        else:
            self.weight = 'NA'

    @measure(JUPITER_LOGGER)
    def get_values(self, db):
        """
        """
        if self.consideration:
            str_date_obj = datetime.datetime.now()
            end_date_obj = datetime.datetime.strptime(
                self.reco['effective_date_to'],
                '%Y-%m-%d'
            )
            delta = end_date_obj - str_date_obj
            self.no_of_days = delta.days
        else:
            self.no_of_days = 'NA'

    @measure(JUPITER_LOGGER)
    def get_multiplier(self):
        """
        y = 1/(ax^2 + 1)
        To parameterize a we require an intermediate point in the curve
        Experience tells us that 30 days away from expiry can be considered as
        '50%' important
        So if x = 30, y should be 0.5
        This gives us the value of a as 0.0011
        Thus making the function
        y = 1/(0.0011*x^2 + 1)
        """
        if self.consideration:
            self.multiplier = round(1/(0.0011*(self.no_of_days ^ 2) + 1), 2)
        else:
            self.multiplier = 'NA'


class SignificantOD(Factor):
    @measure(JUPITER_LOGGER)
    def __init__(self, recommendation):
        super(SignificantOD, self).__init__(recommendation)
        self.reco = recommendation

    @measure(JUPITER_LOGGER)
    def isSignificant(self, od, db):
        """
        Hard Coded as True but would code the logic
        """
        sig_od_crsr = db.JUP_DB_OD_Master.find({'OD': od})
        # print sig_od_crsr.count()

        if sig_od_crsr.count() == 1:
            if sig_od_crsr[0]['significant_flag'] == 'significant':
                return True
            else:
                return False
        else:
            return False

    @measure(JUPITER_LOGGER)
    def check_consideration(self, db):
        OD = self.reco['origin'] + self.reco['destination']
        self.consideration = self.isSignificant(od=OD, db=db)


    @measure(JUPITER_LOGGER)
    def get_weights(self, db):
        wgt_crsr = db.JUP_DB_Recommendation_Priority.find({
            'type': 'significant_od'
        })
        if wgt_crsr.count() == 1:
            self.weight = wgt_crsr[0]['weightage']
            self.priority = wgt_crsr[0]['priority_order']
        else:
            self.weight = 'NA'
            self.priority = "NA"
            print 'ERROR'

    @measure(JUPITER_LOGGER)
    def get_values(self, db):
        """
        """
        if self.consideration:
            self.values = True
        else:
            self.values = False

    @measure(JUPITER_LOGGER)
    def get_multiplier(self):
        if self.values:
            self.multiplier = 10
        else:
            self.multiplier = 1


class Event(Factor):
    """

    """
    @measure(JUPITER_LOGGER)
    def __init__(self, recommendation):
        """

        :param recommendation:
        """
        super(Event, self).__init__(recommendation)
        self.reco = recommendation
        self.consideration = True

    @measure(JUPITER_LOGGER)
    def get_weights(self, db):
        wgt_crsr = db.JUP_DB_Recommendation_Priority.find({
            'type': 'event'
        })
        # print db
        # print wgt_crsr.count()
        if wgt_crsr.count() == 1:
            self.weight = wgt_crsr[0]['weightage']
            self.priority = wgt_crsr[0]['priority_order']
        else:
            self.weight = 'NA'
            self.priority = 'NA'
            print 'ERROR'

    @measure(JUPITER_LOGGER)
    def get_values(self, db):
        """
        """
        sd = self.reco['triggering_data']['dep_date_start']
        ed = self.reco['triggering_data']['dep_date_end']
        events_crsr = db.JUP_DB_Events.find({
            'start_date': {'$lte': ed},
            'end_date': {'$gte': sd}
        })
        self.events = []
        if events_crsr.count() > 0:
            for event in events_crsr:
                self.events.append(event['name'])
            self.values = True
        else:
            self.values = False

    @measure(JUPITER_LOGGER)
    def get_multiplier(self):
        if self.values:
            self.multiplier = 10
        else:
            self.multiplier = 1


class Forecast(Factor):
    @measure(JUPITER_LOGGER)
    def __init__(self, recommendation):
        super(Forecast, self).__init__(recommendation)

    @measure(JUPITER_LOGGER)
    def get_weights(self, db):
        wgt_crsr = db.JUP_DB_Recommendation_Priority.find({
            'type': 'low_forecast'
        })
        if wgt_crsr.count() == 1:
            self.weight = wgt_crsr[0]['weightage']
        else:
            self.weight = 'NA'
            print 'ERROR'

    @measure(JUPITER_LOGGER)
    def check_consideration(self, db):
        """
        :return:
        """
        self.consideration = True

    @measure(JUPITER_LOGGER)
    def if_leg(self):
        """
        Returns a bool if the OD in consideration is a leg
        :return:
        """
        if self.reco['origin'] == 'DXB' or self.reco['destination'] == 'DXB':
            return True
        else:
            return False

    @staticmethod
    @measure(JUPITER_LOGGER)
    def build_qry_forecast(leg_origin, leg_destination, dep_date_start, dep_date_end):
        """

        :return:
        """
        qry_leg_forecast = defaultdict(list)
        qry_leg_forecast['$and'].append({
            'OD': leg_origin.encode() + leg_destination.encode()
        })

        qry_leg_forecast['$and'].append({
            'Depart_Date':
                {
                    '$gte': dep_date_start.encode(),
                    '$lte': dep_date_end.encode()
                }
        })

        return dict(qry_leg_forecast)

    @staticmethod
    @measure(JUPITER_LOGGER)
    def generate_ppln_forecast(forecast_query, temp_collection, compartment=None):
        """
        :return:
        """
        if compartment == 'Y':
            parameter_tobeused = 'Y_Fcst_in_percent'
        elif compartment == 'J':
            parameter_tobeused = 'J_Fcst_in_percent'
        else:
            parameter_tobeused = 'TL_Fcst_in_percent'
        ppln = [
            {
                '$match': forecast_query
            }
            ,
            {
                '$sort':
                    {
                        'last_update_date': -1
                    }
            }
            ,
            {
                '$group':
                    {
                        '_id':
                            {
                                'OD': '$OD',
                                'dep_date': 'Depart_Date'
                            }
                        ,
                        'data':
                            {
                                '$first': '$$ROOT'
                            }
                    }
            }
            ,
            {
                '$group': {
                    '_id': None,
                    'forecast':
                        {
                            '$sum': '$data.' + parameter_tobeused
                        }
                }
            }
            ,
            {
                '$out': temp_collection
            }
        ]
        # print 'fcst ppln', ppln
        return ppln

    @measure(JUPITER_LOGGER)
    def get_forecast_data(self, forecast_query, db, compartment=None):
        """
        Function to get obtain the overall forecast for a query
        forecast_query - dict which can be queried directly in to JUP_DB_Forecast_Leg collection
        returns forecast value(as a percentage)
        """
        forecast = 'NA'
        if 'JUP_DB_Forecast_Leg' in db.collection_names():
            temp_collection = gen_collection_name()
            forecast_ppln = self.generate_ppln_forecast(forecast_query=forecast_query,
                                                        compartment=compartment,
                                                        temp_collection=temp_collection)
            db.JUP_DB_Forecast_Leg.aggregate(forecast_ppln,
                                             allowDiskUse=True)
            if temp_collection in db.collection_names():
                forecast_data = list(db[temp_collection].find())
                # print forecast_data
                db[temp_collection].drop()
                if len(forecast_data) == 1:
                    forecast = forecast_data[0]['forecast']
                elif len(forecast_data) > 1:
                    forecast = forecast_data[0]['forecast']
        return forecast

    @measure(JUPITER_LOGGER)
    def get_values(self, db):
        """

        :return:
        """
        if self.consideration:
            origin = self.reco['origin']
            destination = self.reco['destination']
            compartment = self.reco['compartment']
            dep_date_start = self.reco['triggering_data']['dep_date_start']
            dep_date_end = self.reco['triggering_data']['dep_date_end']
            leg_flag = self.if_leg()
            # print 'leg flag', leg_flag
            if leg_flag:
                fcst_query = self.build_qry_forecast(leg_origin=origin,
                                                     leg_destination=destination,
                                                     dep_date_start=dep_date_start,
                                                     dep_date_end=dep_date_end)
                # print 'forecast_qry', fcst_query
                fcst_val = self.get_forecast_data(forecast_query=fcst_query,
                                                  compartment=compartment, db=db)
                # print 'fcst_val', fcst_val
            #   Individually Calculating for Legs and Summing up the forecasts
            else:
                leg1_origin = origin
                leg1_destination = Host_Airline_Hub
                leg1_fcst_query = self.build_qry_forecast(leg_origin=leg1_origin,
                                                          leg_destination=leg1_destination,
                                                          dep_date_start=dep_date_start,
                                                          dep_date_end=dep_date_end)
                leg1_fcst_val = self.get_forecast_data(forecast_query=leg1_fcst_query,
                                                       compartment=compartment, db=db)

                leg2_origin = Host_Airline_Hub
                leg2_destination = destination
                leg2_fcst_query = self.build_qry_forecast(leg_origin=leg2_origin,
                                                          leg_destination=leg2_destination,
                                                          dep_date_start=dep_date_start,
                                                          dep_date_end=dep_date_end)
                leg2_fcst_val = self.get_forecast_data(forecast_query=leg2_fcst_query,
                                                       compartment=compartment, db=db)

                #   Derived Factor * Minimum of two leg's forecast
                if leg1_fcst_val and leg2_fcst_val:
                    fcst_val = min(leg1_fcst_val, leg2_fcst_val)
                elif leg1_fcst_val:
                    fcst_val = leg1_fcst_val
                else:
                    fcst_val = leg2_fcst_val

            self.values = fcst_valorigin = self.reco['origin']
            destination = self.reco['destination']
            compartment = self.reco['compartment']
            dep_date_start = self.reco['triggering_data']['dep_date_start']
            dep_date_end = self.reco['triggering_data']['dep_date_end']
            leg_flag = self.if_leg()
            if leg_flag:
                fcst_query = self.build_qry_forecast(leg_origin=origin,
                                                     leg_destination=destination,
                                                     dep_date_start=dep_date_start,
                                                     dep_date_end=dep_date_end)
                fcst_val = self.get_forecast_data(forecast_query=fcst_query,
                                                  compartment=compartment, db=db)
            #   Individually Calculating for Legs and Summing up the forecasts
            else:
                leg1_origin = origin
                leg1_destination = Host_Airline_Hub
                leg1_fcst_query = self.build_qry_forecast(leg_origin=leg1_origin,
                                                          leg_destination=leg1_destination,
                                                          dep_date_start=dep_date_start,
                                                          dep_date_end=dep_date_end)
                leg1_fcst_val = self.get_forecast_data(forecast_query=leg1_fcst_query,
                                                       compartment=compartment, db=db)

                leg2_origin = Host_Airline_Hub
                leg2_destination = destination
                leg2_fcst_query = self.build_qry_forecast(leg_origin=leg2_origin,
                                                          leg_destination=leg2_destination,
                                                          dep_date_start=dep_date_start,
                                                          dep_date_end=dep_date_end)
                leg2_fcst_val = self.get_forecast_data(forecast_query=leg2_fcst_query,
                                                       compartment=compartment, db=db)

                #   Derived Factor * Minimum of two leg's forecast
                if leg1_fcst_val and leg2_fcst_val:
                    fcst_val = min(leg1_fcst_val, leg2_fcst_val)
                elif leg1_fcst_val:
                    fcst_val = leg1_fcst_val
                else:
                    fcst_val = leg2_fcst_val

            self.values = fcst_val
        else:
            self.values = 'NA'

    @measure(JUPITER_LOGGER)
    def get_multiplier(self):
        """
        Forecast_Value      Multiplier Value    Days to Departure
        90-100              0
        80-90               0.25
        70-80               0.5
        60-70               0.75
        <60                 1

        :return:
        """
        def_intervals = {
            (90, 100): 0,
            (80, 90): 0.25,
            (70, 80): 0.5,
            (60, 70): 0.75,
            (0, 60): 1
        }
        if type(self.values) in [int,float] and self.values:
            for key, value in def_intervals.items():
                if key[0] < self.values <= key[1]:
                    self.multiplier = value


class Availability(Factor):
    @measure(JUPITER_LOGGER)
    def __init__(self, recommendation):
        super(Availability, self).__init__(recommendation)
        # print recommendation
        self.reco = recommendation

    @measure(JUPITER_LOGGER)
    def get_weights(self, db):
        """
        :return:
        """
        wgt_crsr = db.JUP_DB_Recommendation_Priority.find({
            'type': 'low_forecast'
        })
        if wgt_crsr.count() == 1:
            self.weight = wgt_crsr[0]['weightage']
        else:
            self.weight = 'NA'
            # print 'ERROR'

    @measure(JUPITER_LOGGER)
    def check_consideration(self, db):
        """

        :return:
        """
        if self.if_leg():
            self.consideration = True
        else:
            self.consideration = False

    @measure(JUPITER_LOGGER)
    def if_leg(self):
        """
        Returns a bool if the OD in consideration is a leg
        :return:
        """
        # print self.reco
        if self.reco['origin'] == 'DXB' or self.reco['destination'] == 'DXB':
            return True
        else:
            return False

    @staticmethod
    @measure(JUPITER_LOGGER)
    def build_qry_availability(leg_origin, leg_destination, dep_date_start, dep_date_end):
        """

        :return:
        """
        qry_leg_availability = defaultdict(list)
        qry_leg_availability['$and'].append({
            'od': leg_origin + leg_destination
        })

        qry_leg_availability['$and'].append({
            'dep_date':
                {
                    '$gte': dep_date_start,
                    '$lte': dep_date_end
                }
        })

        return qry_leg_availability

    @staticmethod
    @measure(JUPITER_LOGGER)
    def generate_ppln_availability(availability_query, temp_collection, rbd=None, compartment=None):
        """
        :return:
        """
        if rbd:
            rbd_parameter_tobeused = str(rbd) + 'Sa'
            rbd_seats_avail_key = '$data.' + rbd_parameter_tobeused
        else:
            rbd_seats_avail_key = 0
        if compartment:
            cap_parameter_tobeused = str(compartment.lower())
        else:
            cap_parameter_tobeused = 'total'

        ppln = [
            {
                '$match': dict(availability_query)
            }
            ,
            {
                '$sort':
                    {
                        'last_update_date': -1
                    }
            }
            ,
            {
                '$group':
                    {
                        '_id':
                            {
                                'OD': '$od',
                                'dep_date': 'Depart_Date'
                            }
                        ,
                        'data':
                            {
                                '$first': '$$ROOT'
                            }
                    }
            }
            ,
            {
                '$group': {
                    '_id': None,
                    'rbd_seats_available':
                        {
                            '$sum': rbd_seats_avail_key
                        }
                    ,
                    'capacity':
                        {
                            '$sum': '$data.' + cap_parameter_tobeused + '_cap'
                        }
                    ,
                    'bookings':
                        {
                            '$sum': '$data.' + cap_parameter_tobeused + '_booking'
                        }
                }
            }
            ,
            {
                '$project':
                    {
                        '_id':0,
                        'rbd_seats_available': '$rbd_seats_available',
                        'comp_capacity': '$capacity',
                        'comp_bookings': '$bookings',
                        'comp_availability':
                            {
                                '$cond':
                                    {
                                        'if': {'$gt': ['$capacity', 0]},
                                        'then': {'$multiply': [{'$divide': [{'$subtract': ['$capacity','$bookings']},'$capacity']},100]},
                                        'else': 'NA'
                                    }
                            }
                    }
            }
            ,
            {
                '$out': temp_collection
            }
        ]
        return ppln

    @measure(JUPITER_LOGGER)
    def get_availability_data(self, availability_query, db, rbd=None, compartment=None):
        """
        Function to get obtain the overall forecast for a query
        forecast_query - dict which can be queried directly in to JUP_DB_Forecast_Leg collection
        returns forecast value(as a percentage)
        """
        if self.consideration:
            availability_data = []
            if 'JUP_DB_Inventory_OD' in db.collection_names():
                temp_collection = gen_collection_name()
                availability_ppln = self.generate_ppln_availability(availability_query=availability_query,
                                                                    rbd=rbd,
                                                                    compartment=compartment,
                                                                    temp_collection=temp_collection)
                # print availability_ppln
                db.JUP_DB_Inventory_OD.aggregate(availability_ppln,
                                                 allowDiskUse=True)
                if temp_collection in db.collection_names():
                    availability_data = list(db[temp_collection].find())
                    db[temp_collection].drop()
            return availability_data

    @measure(JUPITER_LOGGER)
    def get_values(self, db):
        """
        :return:
        """
        if self.consideration:
            origin = self.reco['origin']
            destination = self.reco['destination']
            compartment = self.reco['compartment']
            try:
                rbd = self.reco['host_pricing_data']['RBD']
            except KeyError:
                rbd = None
            dep_date_start = self.reco['triggering_data']['dep_date_start']
            dep_date_end = self.reco['triggering_data']['dep_date_end']
            leg_flag = self.if_leg()
            if leg_flag:
                availability_query = self.build_qry_availability(leg_origin=origin,
                                                     leg_destination=destination,
                                                     dep_date_start=dep_date_start,
                                                     dep_date_end=dep_date_end)
                availability_data = self.get_availability_data(availability_query=availability_query,
                                                               compartment=compartment, db=db,
                                                               rbd=rbd)
                # print availability_data
                if len(availability_data) == 1:
                    self.availability_seats = availability_data[0]['rbd_seats_available']
                    self.availability_perc = availability_data[0]['comp_availability']
                else:
                    self.availability_perc = 'NA'
                    self.availability_seats = 'NA'
        else:
            self.availability_perc = 'NA'
            self.availability_seats = 'NA'

    @measure(JUPITER_LOGGER)
    def get_multiplier(self):
        """
        Availability      Multiplier
        x(percentage)     x/100
        :return:
        """
        if self.consideration:
            if type(self.availability_perc) in[int, float]:
                self.multiplier = self.availability_perc / 100.0
            else:
                self.multiplier = 0
        else:
            self.multiplier = 0


class DeparturePeriod(Factor):
    """
    Consideration of Departure period for which the trigger has been raised
    """
    @measure(JUPITER_LOGGER)
    def __init__(self, recommendation):
        super(DeparturePeriod, self).__init__(recommendation)

    @measure(JUPITER_LOGGER)
    def do_analysis(self, db):
        """
            Main function of any factor class
        """
        self.check_consideration(db=db)
        self.get_weights(db)
        self.get_values(db)
        self.get_multiplier()

    @measure(JUPITER_LOGGER)
    def check_consideration(self, db):
        """
        Checks whether this factor is to be considered or not
        """
        if self.reco['triggering_data']['dep_date_start'] or self.reco['triggering_data']['dep_date_end']:
            self.consideration = True

    @measure(JUPITER_LOGGER)
    def get_weights(self, db):
        """
        If being considered get the weights for the factors
        else return weight as 'NA'
        """
        wgt_crsr = db.JUP_DB_Recommendation_Priority.find({
            'type': 'departure_period'
        })
        if wgt_crsr.count() == 1:
            self.weight = wgt_crsr[0]['weightage']
            self.priority = wgt_crsr[0]['priority_order']

    @measure(JUPITER_LOGGER)
    def get_values(self, db):
        """
        If the factor is being considered get the relevant values
        this month - 10
        next month - 5
        rest       - 3
        """
        if self.consideration:
            tdy_obj = datetime.datetime.strptime(SYSTEM_DATE, '%Y-%m-%d')
            if self.reco['triggering_data']['dep_date_start']:
                dsd_obj = datetime.datetime.strptime(self.reco['triggering_data']['dep_date_start'], '%Y-%m-%d')
                if tdy_obj.month == dsd_obj.month and tdy_obj.year == dsd_obj.year:
                    self.values = 'this month'
                elif tdy_obj.year == dsd_obj.year and dsd_obj.month == tdy_obj.month+1:
                    self.values = 'next month'
                else:
                    self.values = 'future months'
            else:
                self.values = 'this month'

    @measure(JUPITER_LOGGER)
    def get_multiplier(self):
        """
        After obtaining the relevant values
        Calculation of Multiplier
        """
        if self.values == 'this month':
            self.multiplier = 10
        elif self.values == 'next month':
            self.multiplier = 5
        else:
            self.multiplier = 3


class Category(object):
    "Class for identifying the category of a recommendation"
    @measure(JUPITER_LOGGER)
    def __init__(self, recommendation):
        """
        Class takes the recommendation instance as an input
        """
        self.reco = recommendation

    @measure(JUPITER_LOGGER)
    def get_category(self, score):
        """
        Range of Score is always 0-100
        80-100  :   Category A
        50-80   :   Category B
        0-50    :   Category C
        """
        if 6 < score <= 10:
            return 'A'
        elif 4 < score <= 6:
            return 'B'
        elif 0 <= score <= 4:
            return 'C'

    @measure(JUPITER_LOGGER)
    def do_analysis(self, db):
        """
        """
        score = 0
        weights_sum = 0
        self.details = dict()

        # Individual Factor Variable and Analysis
        """
        chng_factor = ChangeRecommended(self.reco)
        chng_factor.do_analysis()
        if chng_factor.consideration:
            score += chng_factor.multiplier * chng_factor.weight
            weights_sum += chng_factor.weight
        del chng_factor.reco
        self.details['change_recommended'] = chng_factor.__dict__

        closeness_factor = Closeness(self.reco)
        closeness_factor.do_analysis()
        if closeness_factor.consideration:
            score += closeness_factor.multiplier * closeness_factor.weight
            weights_sum += closeness_factor.weight
        del closeness_factor.reco
        self.details['closeness'] = closeness_factor.__dict__
        """

        sig_od_factor = SignificantOD(self.reco)
        sig_od_factor.do_analysis(db=db)
        if sig_od_factor.consideration:
            score += sig_od_factor.multiplier * sig_od_factor.weight / float(sig_od_factor.priority)
            weights_sum += sig_od_factor.weight
        del sig_od_factor.reco
        self.details['significant_OD'] = sig_od_factor.__dict__

        event_factor = Event(self.reco)
        event_factor.do_analysis(db=db)
        if event_factor.consideration:
            # print event_factor.multiplier
            # print event_factor.weight
            # print event_factor.priority
            score += event_factor.multiplier * event_factor.weight / float(event_factor.priority)
            weights_sum += event_factor.weight
        del event_factor.reco
        self.details['event'] = event_factor.__dict__

        dep_date_factor = DeparturePeriod(self.reco)
        dep_date_factor.do_analysis(db=db)
        if dep_date_factor.consideration:
            score += dep_date_factor.multiplier * dep_date_factor.weight / float(dep_date_factor.priority)
            weights_sum += dep_date_factor.weight
        del dep_date_factor.reco
        self.details['dep_date_factor'] = dep_date_factor.__dict__

        """
        forecast_factor = Forecast(self.reco)
        forecast_factor.do_analysis()
        if forecast_factor.consideration:
            score += forecast_factor.multiplier * forecast_factor.weight
            weights_sum += forecast_factor.weight
        del forecast_factor.reco
        self.details['forecast'] = forecast_factor.__dict__
        """

        self.score = round(score / weights_sum, 2)
        self.category = self.get_category(self.score)

        """
        availability_factor = Availability(self.reco)
        availability_factor.do_analysis()
        if availability_factor.consideration:
            score += availability_factor.multiplier * availability_factor.weight
            weights_sum += availability_factor.weight
        del availability_factor.reco
        self.details['availability'] = availability_factor.__dict__

        trigger_age_factor = AgeOfTrigger(self.reco)
        trigger_age_factor.do_analysis()
        if trigger_age_factor.consideration:
            score += trigger_age_factor.multiplier * trigger_age_factor.weight
            weights_sum += trigger_age_factor.weight
        del trigger_age_factor.reco
        self.details['trigger_age'] = trigger_age_factor.__dict__
        """


if __name__=='__main__':
    import json
    client = mongo_client()
    db =client[JUPITER_DB]

    recommendation = {
        # 'trigger_type': 'event',
        # 'effective_date_to': '2017-02-01',
        'origin': 'AMM',
        'destination': 'DXB',
        # 'compartment': 'Y',
        # 'price_recommendation': 1500,
        'triggering_data': {
            'dep_date_start': '2017-10-01',
            'dep_date_end': '2017-10-31'
        },
        # 'triggering_event_date': '2017-06-07'
    }
    # sig_obj = SignificantOD(recommendation=recommendation)
    # print sig_obj.isSignificant(od='DXBDOH')
    # obj = Availability(recommendation)
    # print obj.if_leg()
    # obj.do_analysis()
    # print json.dumps(obj.__dict__, indent=2)
    obj = Category(recommendation=recommendation)
    obj.do_analysis(db=db)
    cat_details = obj.__dict__
    print cat_details
    """
    Response is obtained as
        {
          "category": "A",
          "score": 83.08,
          "reco": {
            "origin": "DXB",
            "price_recommendation": 1500,
            "destination": "DOH",
            "effective_date_to": "2017-02-01",
            "trigger_type": "event",
            "host_pricing_data": {
              "fare": 1450
            }
          },
          "details": {
            "closeness": {
              "consideration": true,
              "multiplier": 0.99,
              "weight": 15,
              "no_of_days": 4
            },
            "event": {
              "consideration": true,
              "values": true,
              "weight": 10,
              "multiplier": 1
            },
            "change_recommended": {
              "weight": 25,
              "upper_threshold": 5,
              "lower_threshold": -5,
              "change_recommended": 3.0,
              "multiplier": 0.6,
              "consideration": true
            },
            "significant_OD": {
              "consideration": true,
              "values": true,
              "weight": 10,
              "multiplier": 1
            }
          }
        }
    """
    client.close()