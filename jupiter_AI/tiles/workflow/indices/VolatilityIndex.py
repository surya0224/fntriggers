"""
Author - Sai Krishna
Completed Dates -
    first version -

Calculation of Volatility Index for a recommendation
    Defined as the deviation of the host fare from the market's average fare.
Doubts -
        is volatility factor historical volatility
        Option 1(Thesis Formula) -
            1.  consider forecast data for flights for
                    origin,
                    destination,
                    compartment,
                    dep_date_start,
                    dep_date_end,
            2.  Obtain Demand Array representing demand for each flight
            3.  Calculate the Mean/Flight and standard deviation of the Demand Array
            4.  Volatility = S.D / Mean in percentages.

        Option 2(General Price Volatility)  -
            1.  Consider the price curve for this farebasis code and calculate its mean and standard deviation
            2.  Volatility = S.D / Mean

        Option 3(Based on confluence)   -
            1.  Obtain Markets' Average fare(From PAXIS or Can also be done from Infare(if lowest fares are going to be used))
            2.  Get the host average fare of the market(From Paxis or from Infare)
            3.  Volatility = (Havg  - Mavg)/Mavg
"""
import datetime

import numpy

from jupiter_AI import client,JUPITER_DB, query_month_year_builder
db = client[JUPITER_DB]


class VolatilityFactor(object):
    """
    Obtaining the Volatility Factor parameter of the fare.
    """
    def __init__(self, recommendation):
        self.reco = recommendation
        self.value = None

    def generate_forecast_query(self):
        """
        :return:
        """
        dep_date_start_obj = datetime.datetime.strptime(
            self.reco['triggering_data']['dep_date_start'],
            '%Y-%m-%d'
        )
        dep_date_end_obj = datetime.datetime.strptime(
            self.reco['triggering_data']['dep_date_end'],
            '%Y-%m-%d'
        )
        sd = dep_date_start_obj.day
        sm = dep_date_start_obj.month
        sy = dep_date_start_obj.year
        ed = dep_date_end_obj.day
        em = dep_date_end_obj.month
        ey = dep_date_end_obj.year

        mnth_yr_combinations = query_month_year_builder(stdm=sm,stdy=sy,endm=em,endy=ey)

        forecast_qry = {
            'pos': self.reco['pos'],
            'origin': self.reco['origin'],
            'destination': self.reco['destination'],
            'compartment': self.reco['compartment'],
            '$or': mnth_yr_combinations
        }
        return forecast_qry

    @staticmethod
    def generate_forecast_ppln(forecast_query):
        """
        """
        ppln_forecast =[
            {
                '$match': forecast_query
            },
            {
                '$group': {
                    '_id': {
                        'last_update_date': '$last_update_date'
                    }
                    ,
                    'pax': {'$sum': '$pax'}
                }
            },
            {
                '$group': {
                    '_id': None,
                    'pax': {
                        '$push': '$pax'
                    }
                }
            }
        ]
        return ppln_forecast

    def get_forecast_data(self):
        """
        :return:
        """
        forecast_query = self.generate_forecast_query()
        forecast_ppln = self.generate_forecast_ppln(forecast_query=forecast_query)
        forecast_data_crsr = db.JUP_DB_Forecast.aggregate(forecast_ppln)
        forecast_data = list(forecast_data_crsr)
        if len(forecast_data) == 1:
            return forecast_data[0]['pax']
        else:
            return []

    def get_volatility_val(self):
        """
        """
        volatility_factor = 'NA'
        list_forecast_pax = self.get_forecast_data()
        list_forecast_pax = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        if len(list_forecast_pax) >= 3:
            forecast_array = numpy.array(list_forecast_pax)
            mean = forecast_array.mean()
            sd = forecast_array.std()
            volatility_factor = mean / float(sd)

        return volatility_factor

if __name__ == '__main__':
    recommendation = {
        'region': 'GCC',
        'country': 'QA',
        'pos': 'DOH',
        'origin': 'DOH',
        'destination': 'BOM',
        'compartment': 'Y',
        'triggering_data': {
            'dep_date_start': '2017-02-01',
            'dep_date_end': '2017-02-28'
        }
    }
    obj = VolatilityFactor(recommendation)
    print obj.get_volatility_val()