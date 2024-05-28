""" ****WorkFlow of Code****
    -> Instance of class sendside_trigger() 'p' is created (last line of code)
    -> It calls the __init__() method, which then calls the fire_trigger() method
    -> fire_trigger calls trigger_hierarchy() method which returns a list containing all triggers to be checked
    -> there are 3 for loops in fire_trigger()
    -> first for loop runs on host pricing data.
    -> second for loop runs on the trigger list.
    -> if there is a trigger that is fired, 3rd loop goes in the host pricing data,
    and filter it, according to no. of farebasis fires multiple triggers can be fired.
    -> After trigger is fired it calls send_trigger() method which writes in the
    triggring event collection and in the pricing queue.

    Keypoints
    -> The calculate_booking change() method has been explained through comments,
    calculate_pax_change(), calculate_revenue_change() and calculate_yield_change() works similar to it.
    -> In the database all the data corresponds to a particular date. Calculations are done to find the aggregate.
    -> airline passed in opprtunities method is host airline. (for now don't know from which collection it will come?)
    booking_changes_weekly
    booking_changes_VLYR
    booking_changes_VTGT
    route_profitability_requirements
    forecast_changes
    pax_changes_week
    pax_changes_VLYR
    pax_changes_VTGT
    yield_changes_week
    yield_changes_VLYR
    yield_changes_VTGT
    opportunities
    revenue_changes_weekly
    revenue_change_VLYR
    revenue_changes_VTGT
"""

import datetime
import inspect
from copy import deepcopy

import pika

import analytics_functions as af
from jupiter_AI import network_level_params as net
from jupiter_AI.common import ClassErrorObject as errorClass
from jupiter_AI import client,JUPITER_DB, today
#db = client[JUPITER_DB]
from jupiter_AI.network_level_params import query_month_year_builder

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()


class register_data_level_trigger(object):
    def __init__(self):
        self.fire_trigger()

    def get_module_name(self):  # gives method name
        return inspect.stack()[1][3]

    def get_arg_lists(self, frame):  # gives argument list for a given method
        args, _, _, values = inspect.getargvalues(frame)
        argument_name_list = []
        argument_value_list = []
        for k in args:
            argument_name_list.append(k)
            argument_value_list.append(values[k])
        return argument_name_list, argument_value_list

    def append_error(self, e, esub):  # appends esub to 'e' error object list
        e.append_to_error_object_list(esub)

    def calculate_changes(self, trigger_name, h_data):
        """
        Calculates the value to be compared with thresholds for respective triggers
        ----------
        PARAMETERS
        ----------
        trigger_name - string     - name of the trigger            - E.g. - 'booking_changes_weekly'
        h_data       - dictionery - containing relevant query data - E.g. - {'pos':'LON','origin':'LON','destination':'BOM','compartment':'Y'....}
        h_data is host pricing data which will be used to filter the booking collection
        """
        if trigger_name == 'booking_changes_weekly':
            twd = today
            lwd = twd + datetime.timedelta(days=-7)
            nwd = twd + datetime.timedelta(days=7)
            twd = datetime.datetime.strftime(twd, '%Y-%m-%d')
            lwd = datetime.datetime.strftime(lwd, '%Y-%m-%d')
            nwd = datetime.datetime.strftime(nwd, '%Y-%m-%d')
            # print twd
            # print lwd
            # print nwd

            #   btw is booking till this week, blw is booking till last week
            btw_data = []
            blw_data = []
            btw_data_cursor = db.Ashok_JUP_DB_Booking_DepDate.find({"pos": h_data['pos'],
                                                               "origin": h_data['origin'],
                                                               "destination": h_data['destination'],
                                                               "compartment": h_data['compartment'],
                                                               "dep_date": {"$gte": twd, "$lte": nwd}})
            blw_data_cursor = db.Ashok_JUP_DB_Booking_DepDate.find({"pos": h_data['pos'],
                                                               "origin": h_data['origin'],
                                                               "destination": h_data['destination'],
                                                               "compartment": h_data['compartment'],
                                                               "dep_date": {"$gte": lwd, "$lte": twd}})
            for i in btw_data_cursor:
                del i['_id']
                btw_data.append(i)
            for i in blw_data_cursor:
                del i['_id']
                blw_data.append(i)
            if len(btw_data) != 0 and len(blw_data) != 0:
                total_blw = 0.0
                total_btw = 0.0
                for i in blw_data:
                    total_blw = total_blw + i['pax']
                for i in btw_data:
                    total_btw = total_btw + i['pax']
                #   error handling if denominator becomes zero in change_per
                if total_blw == 0:
                    e1 = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                                self.get_module_name(),
                                                self.get_arg_lists(inspect.currentframe()))
                    e1_desc = ''.join(["Total bookings till last week is 0",
                                       "in Booking collection a ",
                                       "for given POS- ",
                                       str(h_data['pos']), ",OD- ",
                                       str(h_data['origin']), str(h_data['destination']),
                                       " and Compartment- ", str(h_data['compartment']),
                                       ", per. change can't be calculated"
                                       ])
                    e1.append_to_error_list(e1_desc)
                    raise e1
                data = dict()
                data['val1'] = total_blw
                data['val2'] = total_btw
                print 'this_week',total_btw
                print 'last_week',total_blw
                change_per = ((total_btw - total_blw) * 100) / total_blw
                response = dict()
                response['change'] = change_per
                response['data'] = data
                return response
            else:
                e2 = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                            self.get_module_name(),
                                            self.get_arg_lists(inspect.currentframe()))
                e2_desc = ''.join(["No records for this week or last week bookings in Booking collection for given",
                                   " pos- ", str(h_data['pos']),
                                   ", OD-", str(h_data['origin']), str(h_data['destination']),
                                   ", compartment-", str(h_data['compartment'])])
                e2.append_to_error_list(e2_desc)
                raise e2

        if trigger_name == 'booking_changes_VLYR':
            twd = today
            enddt = twd + datetime.timedelta(days=90)
            twd = str(twd)
            enddt = str(enddt)
            # b_cursor is booking cursor

            b_data = []
            b_data_cursor = db.JUP_DB_Booking_BookDate.find({"pos": h_data['pos'],
                                                             "origin": h_data['origin'],
                                                             "destination": h_data['destination'],
                                                             "compartment": h_data['compartment'],
                                                             "book_date": {'$lte':twd},
                                                             "dep_date": {"$lte": enddt,
                                                                          "$gte": twd}})
            for i in b_data_cursor:
                del i['_id']
                b_data.append(i)

            # error handling if no. of records becomes 0

            if len(b_data) == 0:
                e1 = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                            self.get_module_name(),
                                            self.get_arg_lists(inspect.currentframe()))
                e1_desc = ''.join(["There are no. records in Booking collection",
                                   "for a given ",
                                   "pos-", str(h_data['pos']),
                                   ", od-", str(h_data['origin']), str(h_data['destination']),
                                   ", compartment-", str(h_data['compartment']),
                                   " between ", twd, " and ", enddt])
                e1.append_to_error_list(e1_desc)
                raise e1

            if len(b_data) != 0:
                #   bty is booking in this year
                total_bty = 0.0
                #   bly is booking in last year
                total_bly = 0.0
                for i in b_data:
                    if i['pax'] is not None:
                        total_bty += i['pax']
                    if i['pax_1'] is not None:
                        total_bly += i['pax_1']

                #   error handling if denominator becomes zero in change_vlyr
                if total_bly == 0:
                    e2 = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                                self.get_module_name(),
                                                self.get_arg_lists(inspect.currentframe()))
                    e2_desc = ''.join(["Last Year bookings are zero in Booking collection for given ",
                                       "pos-", str(h_data['pos']),
                                       ", od-", str(h_data['origin']), str(h_data['destination']),
                                       ", compartment", str(h_data['compartment']),
                                       "per. change can't be calculated"])
                    e2.append_to_error_list(e2_desc)
                    raise e2

            data = dict()
            data['val1'] = total_bly
            data['val2'] = total_bty
            print 'this_year',total_bty
            print 'last_year',total_bly
            if data['val1'] != 0 and data['val1']:
                change_vlyr = ((total_bty - total_bly) * 100) / total_bly
            else:
                change_vlyr = None
            response = dict()
            response['change'] = change_vlyr
            response['data'] = data
            return response

        if trigger_name == 'booking_changes_VTGT':

            twd = today
            enddt = twd + datetime.timedelta(days=90)
            tm = twd.month
            ty = twd.year
            endm = enddt.month
            endy = enddt.year
            twd = str(twd)
            enddt = str(enddt)

            month_year_query_ty = query_month_year_builder(tm, ty, endm, endy)
            month_year_query_ly = query_month_year_builder(tm,ty-1,endm,endy-1)

            b_data = []
            b_data_cursor = db.JUP_DB_Booking_BookDate.find({"pos": h_data['pos'],
                                                             "origin": h_data['origin'],
                                                             "destination": h_data['destination'],
                                                             "compartment": h_data['compartment'],
                                                             "book_date":{'$lte':today},
                                                             "dep_date": {"$lte": enddt,
                                                                          "$gte": twd}})
            for i in b_data_cursor:
                del i['_id']
                b_data.append(i)

            target_data = []
            target_data_cursor = db.JUP_DB_Target_OD.find({"pos": h_data['pos'],
                                                           "origin": h_data['origin'],
                                                           "destination": h_data['destination'],
                                                           "compartment": h_data['compartment'],
                                                           "$or": month_year_query_ty
                                                           })
            target_data_ly = []
            target_data_cursor_ly = db.JUP_DB_Target_OD.find({"pos": h_data['pos'],
                                                            "origin": h_data['origin'],
                                                            "destination": h_data['destination'],
                                                            "compartment": h_data['compartment'],
                                                            "$or": month_year_query_ly
                                                           })

            print target_data_cursor.count()
            for i in target_data_cursor:
                del i['_id']
                target_data.append(i)

            for i in target_data_cursor_ly:
                del i['_id']
                target_data_ly.append(i)

            target_val = 0

            if len(target_data) != 0:
                for i in target_data:
                    target_val += i['pax']
            else:
                e1 = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                            self.get_module_name(),
                                            self.get_arg_lists(inspect.currentframe()))
                e1_desc = ''.join(["There are no records in Target collection for given",
                                   " pos-", str(h_data['pos']),
                                   ", od-", str(h_data['origin']), str(h_data['destination']),
                                   ", compartment-", str(h_data['compartment']),
                                   " from between ", str(tm), "-", str(ty),
                                   " and ", str(endm), "-", str(endy)])
                e1.append_to_error_list(e1_desc)
                raise e1
            target_val_ly = 0
            if len(target_data_ly) != 0:
                for i in target_data_ly:
                    target_val_ly += i['pax']
            else:
                e1 = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                            self.get_module_name(),
                                            self.get_arg_lists(inspect.currentframe()))
                e1_desc = ''.join(["There are no records in Target collection for given",
                                   " pos-", str(h_data['pos']),
                                   ", od-", str(h_data['origin']), str(h_data['destination']),
                                   ", compartment-", str(h_data['compartment']),
                                   " from between ", str(tm), "-", str(ty-1),
                                   " and ", str(endm), "-", str(endy-1)])
                e1.append_to_error_list(e1_desc)
                raise e1
            # error handling if no. of records becomes 0
            if len(b_data) == 0:
                e2 = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                            self.get_module_name(),
                                            self.get_arg_lists(inspect.currentframe()))
                e2_desc = ''.join(["There are no records in Booking collection for given",
                                   " pos-", str(h_data['pos']),
                                   ", od-", str(h_data['origin']), str(h_data['destination']),
                                   ", compartment-", str(h_data['compartment']),
                                   " from between ", str(enddt),
                                   " and ", str(twd)])
                e2.append_to_error_list(e2_desc)
                raise e2

            # total bookings till today
            total_b = 0.0
            total_b_ly = 0.0
            if len(b_data) != 0:
                for i in b_data:
                    total_b = total_b + i['pax']
                    total_b_ly = total_b_ly + i['pax_1']
            print 'actual',total_b
            print 'actual_ly',total_b_ly
            print 'target',target_val
            print 'target_ly',target_val
            percent_target_ty = float(total_b)/target_val
            percent_target_ly = float(total_b_ly)/target_val_ly
            change_vtgt = ((percent_target_ty - percent_target_ly) * 100) / percent_target_ly
            data = dict()
            data['val1'] = percent_target_ly
            data['val2'] = percent_target_ty
            response = dict()
            response['change'] = change_vtgt
            response['data'] = data
            return response
        """
        if trigger_name == 'yield_changes_weekly':
            twd= today
            lwd= twd+datetime.timedelta(days=-7)
            std= twd+datetime.timedelta(days=-90)
            twd= str(twd)
            lwd= str(lwd)
            std= str(std)
            #ytw is yield till this week, ylw is yield till last week
            ytw_data= access_db(net.MONGO_CLIENT_URL,
                                    net.TRIGGER_DB_NAME,
                                    'JUP_DB_Yield_Channel',
                                    {"POS": h_data['pos'],
                                    "origin": h_data['origin'],
                                    "destination": h_data['destination'],
                                    "Class1": h_data['compartment'],
                                    "Date": {"$gte":std,"$lte":twd}})
            ylw_data= access_db(net.MONGO_CLIENT_URL,
                                    net.TRIGGER_DB_NAME,
                                    'JUP_DB_Yield_Channel',
                                    {"POS": h_data['pos'],
                                    "origin": h_data['origin'],
                                    "destination": h_data['destination'],
                                    "Class1": h_data['compartment'],
                                    "Date": {"$gte":std,"$lte":lwd}})
            if(len(ytw_data)!= 0 and len(ylw_data)!= 0):
                pax_tw = 0.0
                pax_lw = 0.0
                revenue_tw = 0.0
                revenue_lw = 0.0

                for i in ytw_data:
                    pax_tw = pax_tw + i['pax']
                    revenue_tw = revenue_tw + i['revenue']

                ytw = float(revenue_tw)/pax_tw

                for i in ylw_data:
                    pax_lw = pax_lw + i['pax']
                    revenue_lw = revenue_lw + i['revenue']

                ylw = float(revenue_lw)/pax_lw

                if(ylw==0):
                    e1=errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
                    e1.append_to_error_list("Total yield till last week is zero in Yield_Revenue collection for POS = "+h_data['pos']+", OD "+h_data['origin']+h_data['destination']+" and Compartment "+h_data['compartment']+" , per. change cant be calculated")
                    raise e1

                change_percentage = ((ytw-ylw)*100)/ylw
                return change_percentage

            else:
                e1=errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
                e1.append_to_error_list("No records for this week or last week yield in Yield_Channel collection for POS = "+h_data['pos']+", OD "+h_data['origin']+h_data['destination']+" and Compartment "+h_data['compartment'])
                raise e1
        """
        """
        if trigger_name == 'yield_changes_VLYR':
            td= today
            std= td+datetime.timedelta(days=-90)
            td= str(td)
            std= str(std)
            #ytw is yield till this week, ylw is yield till last week
            y_data= access_db(net.MONGO_CLIENT_URL,
                                net.TRIGGER_DB_NAME,
                                'JUP_DB_Yield_Channel',
                                {"POS": h_data['pos'],
                                "origin": h_data['origin'],
                                "destination": h_data['destination'],
                                "Class1": h_data['compartment'],
                                "Date": {"$gte":std,"$lte":td}})
            if len(y_data)!= 0:
                pax_ty = 0.0
                pax_ly = 0.0
                revenue_ty = 0.0
                revenue_ly = 0.0

                for i in y_data:
                    pax_ty = pax_ty + i['pax']
                    revenue_ty = revenue_ty + i['revenue']

                yty = float(revenue_ty)/pax_ty

                for i in y_data:
                    pax_ly = pax_ly + i['pax_vlyr_1']
                    revenue_ly = revenue_ly + i['revenue_vlyr_1']

                yly = float(revenue_ly)/pax_ly

                if(yly==0):
                    e1=errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
                    e1.append_to_error_list("Yield Last Year is zero in Yield_Channel collection for POS = "+h_data['pos']+", OD "+h_data['origin']+h_data['destination']+" and Compartment "+h_data['compartment']+" , perc. change cant be calculated")
                    raise e1

                change_percentage = ((yty-yly)*100)/yly
                return change_percentage

            else:
                e1=errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
                e1.append_to_error_list("No records for in Yield_Channel collection for POS = "+h_data['pos']+", OD "+h_data['origin']+h_data['destination']+" and Compartment "+h_data['compartment']+" between dates "+std+" and "+td)
                raise e1
        """
        """
        if trigger_name == 'yield_changes_VTGT':
            td= today
            std= td+datetime.timedelta(days=-90)
            td= str(td)
            std= str(std)
            #ytw is yield till this week, ylw is yield till last week
            y_data= access_db(net.MONGO_CLIENT_URL,
                                net.TRIGGER_DB_NAME,
                                'JUP_DB_Yield_Channel',
                                {"POS": h_data['pos'],
                                "origin": h_data['origin'],
                                "destination": h_data['destination'],
                                "Class1": h_data['compartment'],
                                "Date": {"$gte":std,"$lte":td}})
            if len(y_data)!= 0:
                pax_ty = 0.0
                pax_ly = 0.0
                revenue_ty = 0.0
                revenue_ly = 0.0

                for i in y_data:
                    pax_ty = pax_ty + i['pax']
                    revenue_ty = revenue_ty + i['revenue']

                y_actual = float(revenue_ty)/pax_ty

                for i in y_data:
                    pax_ly = pax_ly + i['pax_vtgt']
                    revenue_ly = revenue_ly + i['revenue_vtgt']

                y_target = float(revenue_ly)/pax_ly

                if(y_target==0):
                    e1=errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
                    e1.append_to_error_list("Target yield zero in Yield_Channel collection for POS = "+h_data['pos']+", OD "+h_data['origin']+h_data['destination']+" and Compartment "+h_data['compartment']+" , perc. change cant be calculated")
                    raise e1

                change_percentage = ((y_actual-y_target)*100)/y_target
                return change_percentage

            else:
                e1=errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
                e1.append_to_error_list("No records for in Yield_Channel collection for POS = "+h_data['pos']+", OD "+h_data['origin']+h_data['destination']+" and Compartment "+h_data['compartment']+" between dates "+std+" and "+td)
                raise e1
        """

        if trigger_name == 'revenue_changes_weekly':
            twd = today
            lwd = twd+datetime.timedelta(days=-7)
            nwd = twd+datetime.timedelta(days=7)
            twd = datetime.datetime.strftime(twd,'%Y-%m-%d')
            lwd = datetime.datetime.strftime(lwd,'%Y-%m-%d')
            nwd = datetime.datetime.strftime(nwd,'%Y-%m-%d')
            print twd,lwd,nwd
            #   ytw is yield till this week, ylw is yield till last week
            rtw_data = db.JUP_DB_Sales.find({"pos": h_data['pos'],
                                                   "origin": h_data['origin'],
                                                   "destination": h_data['destination'],
                                                   "compartment": h_data['compartment'],
                                                   "dep_date": {"$gte": twd, "$lte": nwd}})
            rlw_data = db.JUP_DB_Sales.find({"pos": h_data['pos'],
                                                   "origin": h_data['origin'],
                                                   "destination": h_data['destination'],
                                                   "compartment": h_data['compartment'],
                                                   "dep_date": {"$gte": lwd,"$lte": twd}})
            print rtw_data.count()
            print rlw_data.count()
            if rtw_data.count() != 0 and rlw_data.count() != 0:
                revenue_tw = 0.0
                revenue_lw = 0.0
                for i in rtw_data:
                    revenue_tw = revenue_tw + i['revenue_base']

                for i in rlw_data:
                    revenue_lw = revenue_lw + i['revenue_base']

                if revenue_lw == 0:
                    e1 = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                                self.get_module_name(),
                                                self.get_arg_lists(inspect.currentframe()))
                    e1_desc = ''.join([
                        "Total Revenue till last week is 0 in Sales collection for POS = ",
                        str(h_data['pos']), ", OD ", str(h_data['origin']+h_data['destination']),
                        " and Compartment ", str(h_data['compartment']), " , per. change cant be calculated"
                    ])
                    e1.append_to_error_list(e1_desc)
                    raise e1
                data = dict()
                data['val1'] = revenue_lw
                data['val2'] = revenue_tw
                change_percentage = ((revenue_tw-revenue_lw)*100)/revenue_lw
                response = dict()
                response['change'] = change_percentage
                response['data'] = data
                return response

            else:
                e2=errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
                e2_desc = ''.join([
                    "No records for this week or last week in Sales collection for pos = ",
                    str(h_data['pos']), ", od = ", str(h_data['origin']+h_data['destination']),
                    " and Compartment = ", str(h_data['compartment'])
                ])
                e2.append_to_error_list(e2_desc)
                raise e2

        if trigger_name == 'revenue_changes_VLYR':
            td = today
            endd = td+datetime.timedelta(days=90)
            td = str(td)
            endd = str(endd)
            #ytw is yield till this week, ylw is yield till last week
            r_data = []
            r_cursor = db.JUP_DB_Sales.find({"pos": h_data['pos'],
                                                   "origin": h_data['origin'],
                                                   "destination": h_data['destination'],
                                                   "compartment": h_data['compartment'],
                                                   "dep_date": {"$gte":td,"$lte":endd}})
            for i in r_cursor:
                del i['_id']
                r_data.append(i)

            if len(r_data) != 0 :
                revenue_ty = 0.0
                revenue_ly = 0.0
                for i in r_data:
                    revenue_ty = revenue_ty + i['revenue_base']
                    revenue_ly = revenue_ly + i['revenue_base_1']

                if revenue_ly == 0:
                    e1=errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                              self.get_module_name(),
                                              self.get_arg_lists(inspect.currentframe()))
                    e1_desc = ''.join(["Total Revenue in Last year is 0 in Sales collection for pos = ",
                                       str(h_data['pos']), ", OD ", str(h_data['origin']+h_data['destination']),
                                       " and Compartment ", str(h_data['compartment']),
                                       " between dates ",str(td)," and ",str(endd)])
                    e1.append_to_error_list(e1_desc)
                    raise e1
                data = dict()
                data['val1'] = revenue_ly
                data['val2'] = revenue_ty
                change_percentage = ((revenue_ty-revenue_ly)*100)/revenue_ly
                response = dict()
                response['change'] = change_percentage
                response['data'] = data
                return response

            else:
                e2=errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                          self.get_module_name(),
                                          self.get_arg_lists(inspect.currentframe()))
                e2_desc = ''.join([
                    "No records in Salescollection for pos = ",
                    str(h_data['pos']),", OD ",str(h_data['origin']+h_data['destination']),
                    " and Compartment ", str(h_data['compartment'])
                ])
                e2.append_to_error_list(e2_desc)
                raise e2

        if trigger_name == 'revenue_changes_VTGT':
            td = today
            endd = td+datetime.timedelta(days=90)
            tm = endd.month
            ty = endd.year
            stm = td.month
            sty = td.year
            td = str(td)
            e = str(endd)
            month_year_query = query_month_year_builder(stm,sty,tm,ty)
            #ytw is yield till this week, ylw is yield till last week
            r_data = []
            r_cursor = db.JUP_DB_Sales.find({"pos": h_data['pos'],
                                                   "origin": h_data['origin'],
                                                   "destination": h_data['destination'],
                                                   "compartment": h_data['compartment'],
                                                   "dep_date": {"$gte": td, "$lte": e}})

            for i in r_cursor:
                del i['_id']
                r_data.append(i)

            target_data_cursor = db.JUP_DB_Target_OD.find({"$or": month_year_query,
                                                           "pos": h_data['pos'],
                                                           "origin": h_data['origin'],
                                                           "destination": h_data['destination'],
                                                           "compartment": h_data['compartment']
                                                           })
            target_data = []
            for i in target_data_cursor:
                del i['_id']
                target_data.append(i)

            revenue_actual = 0.0
            revenue_target = 0.0
            if len(target_data) != 0:
                for i in target_data:
                    revenue_target += i['revenue']
            else:
                e1 = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                            self.get_module_name(),
                                            self.get_arg_lists(inspect.currentframe()))
                e1_desc = ''.join([
                    "No records in Target collection for pos = ", str(h_data['pos']),
                    ", OD = ", str(h_data['origin'] + h_data['destination']), " and Compartment = ",
                    str(h_data['compartment'])
                ])
                e1.append_to_error_list(e1_desc)
                raise e1

            if len(r_data) != 0:
                for i in r_data:
                    revenue_actual = revenue_actual + i['revenue_base']

                if revenue_target == 0:
                    e2 = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                                self.get_module_name(),
                                                self.get_arg_lists(inspect.currentframe()))
                    e2_desc = ''.join(["Total Target Revenue is  0 in Target_OD collection for pos = ",
                                       str(h_data['destination']), " and Compartment ", str(h_data['compartment']),
                                       "between months ", str(stm), "-", str(sty), "and", str(tm), "-", str(ty)
                                       ])
                    e2.append_to_error_list(e2_desc)
                    raise e2

                change_percentage = ((revenue_actual-revenue_target)*100)/revenue_target
                data = dict()
                data['val1'] = revenue_target
                data['val2'] = revenue_actual
                response = dict()
                response['change'] = change_percentage
                response['data'] = data
                return response

            else:
                e3 = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                            self.get_module_name(),
                                            self.get_arg_lists(inspect.currentframe()))
                e3_desc = ''.join([
                    "No records in Sales collection for pos = ", str(h_data['pos']),
                    ", OD = ", str(h_data['origin']+h_data['destination']), " and Compartment = ",
                    str(h_data['compartment'])
                ])
                e3.append_to_error_list(e3_desc)
                raise e3

        if trigger_name == 'pax_changes_weekly':
            twd = today
            lwd = twd+datetime.timedelta(days=-7)
            nwd = twd+datetime.timedelta(days=7)
            twd = str(twd)
            lwd = str(lwd)
            nwd = str(nwd)
            #   pax_tw is pax till this week, pax_lw is pax till last week
            ptw_cursor = db.Ashok_JUP_DB_Booking_DepDate.find({"pos": h_data['pos'],
                                                                "origin": h_data['origin'],
                                                                "destination": h_data['destination'],
                                                                "compartment": h_data['compartment'],
                                                                "dep_date": {"$gte": twd, "$lte": nwd}})
            plw_cursor = db.Ashok_JUP_DB_Booking_DepDate.find({"pos": h_data['pos'],
                                                               "origin": h_data['origin'],
                                                               "destination": h_data['destination'],
                                                               "compartment": h_data['compartment'],
                                                               "dep_date": {"$gte": lwd, "$lte": twd}})
            ptw_data = []
            for i in ptw_cursor:
                del i['_id']
                ptw_data.append(i)
            plw_data = []
            for i in plw_data:
                del i['_id']
                plw_data.append(i)
            if len(plw_data) != 0:
                pax_tw = 0.0
                for i in ptw_data:
                    pax_tw = pax_tw + i['pax']
                if pax_tw == 0:
                    e1 = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
                    e1.append_to_error_list("Total Pax till today is 0 in Bookings collection for POS = "+h_data['pos']+", OD "+h_data['origin']+h_data['destination']+" and Compartment "+h_data['compartment']+" , per. change cant be calculated")
                    raise e1
                else:
                    if len(plw_data) != 0:
                        pax_lw = 0
                        for i in plw_data:
                            pax_lw += i['pax']
                        if pax_lw == 0:
                            e1 = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
                            e1.append_to_error_list("Total Pax till last week is 0 in Bookings collection for POS = "+h_data['pos']+", OD "+h_data['origin']+h_data['destination']+" and Compartment "+h_data['compartment']+" , per. change cant be calculated")
                            raise e1
                        else:
                            change_percentage = ((pax_tw-pax_lw)*100)/pax_lw
                            data = dict()
                            data['val1'] = pax_lw
                            data['val2'] = pax_tw
                            response = dict()
                            response['change'] = change_percentage
                            response['data'] = data
                            return response

        if trigger_name == 'pax_changes_VTGT':
            td = today
            tm = td.month
            ty = td.year
            endd = td+datetime.timedelta(days=90)
            em = endd.month
            ey = endd.year
            td = str(td)
            endd = str(endd)
            month_year_query = query_month_year_builder(tm,ty,em,ey)
            #   pax_actual is the actual pax, pax_target is the target pax
            p_data = []
            target_data = []
            p_cursor = db.JUP_DB_Sales.find({'pos': h_data['pos'],
                                                        'origin': h_data['origin'],
                                                        'destination': h_data['destination'],
                                                        'compartment': h_data['compartment'],
                                                        'dep_date': {'$gte': td, '$lte': endd}})

            target_data_cursor = db.JUP_DB_Target_OD.find({"pos": h_data['pos'],
                                                           "origin": h_data['origin'],
                                                           "destination": h_data['destination'],
                                                           "compartment": h_data['compartment'],
                                                           "$or": month_year_query
                                                           })
            pax_actual = 0.0
            pax_target = 0.0
            for i in p_cursor:
                del i['_id']
                p_data.append(i)
            if len(p_data) != 0:
                for i in p_data:
                    pax_actual += i['pax']
            print pax_actual
            for i in target_data_cursor:
                del i['_id']
                target_data.append(i)

            if len(target_data) != 0:
                for i in target_data:
                    pax_target += i['pax']
            print pax_target
            if pax_target == 0 :
                e1 = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
                e1.append_to_error_list("Total Target Pax is zero in Yield_Revenue collection for POS = "+h_data['pos']+", OD "+h_data['origin']+h_data['destination']+" and Compartment "+h_data['compartment']+" , per. change cant be calculated")
                raise e1

            change_percentage = ((pax_actual-pax_target)*100)/pax_target
            response = dict()
            response['change'] = change_percentage
            data = dict()
            data['val1'] = pax_actual
            data['val2'] = pax_target
            response['data'] = data
            print response
            return response

        if trigger_name == 'pax_changes_VLYR':
            td = datetime.date.today()
            endd = td+datetime.timedelta(days=90)
            td = str(td)
            endd = str(endd)
            #pax_ty is pax of this year, pax_ly is pax for the last year
            p_data = []
            p_cursor = db.JUP_DB_Booking_DepDate.find({'pos': h_data['pos'],
                                                        'origin': h_data['origin'],
                                                        'destination': h_data['destination'],
                                                        'compartment': h_data['compartment'],
                                                        'dep_date': {"$gte": td, "$lte": endd}})
            for i in p_cursor:
                del i['_id']
                p_data.append(i)
            if len(p_data) != 0:
                pax_ty = 0.0
                pax_ly = 0.0
                for i in p_data:
                    pax_ty = pax_ty + i['pax']
                    pax_ly = pax_ly + i['pax_1']

                if pax_ly == 0 :
                    e1 = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                                self.get_module_name(),
                                                self.get_arg_lists(inspect.currentframe()))
                    e1.append_to_error_list("Pax Last year is zero in Yield_Revenue collection for POS = "+h_data['pos']+", OD "+h_data['origin']+h_data['destination']+" and Compartment "+h_data['compartment']+" , per. change cant be calculated")
                    raise e1

                change_percentage = ((pax_ty-pax_ly)*100)/pax_ly
                response = dict()
                response['change'] = change_percentage
                data = dict()
                data['val1'] = pax_ly
                data['val2'] = pax_ty
                response['data'] = data
                print response
                return response

            else:
                e1=errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
                e1.append_to_error_list("No records in Yield_Channel collection for POS = "+h_data['pos']+", OD "+h_data['origin']+h_data['destination']+" and Compartment "+h_data['compartment'])
                raise e1

        """
        if trigger_name == 'route_profitability_changes':
            #assuming route_profitability collection (ashok has not finalised the collection for route profitability yet)
            today= str(datetime.date.today())
            rpi_data= access_db('23.96.81.75:27017',
                                'dbschema',
                                'JUP_DB_Yield_Channel',
                                {"pos": h_data['pos'],
                                "origin": h_data['origin'],
                                "destination": h_data['destination'],
                                "Class1": h_data['compartment'],
                                "Date": today})
            if len(rpi_data) == 0:
                e1=errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
                e1.append_to_error_list("No records of route_profitability in route_profitability collection for given POS, OD and Compartment")
                raise e1
            elif len(rpi_data) == 1:
                return rpi_data[0]['route_profitability']
            else:
                e1=errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
                e1.append_to_error_list("More than 1 record of route_profitability in route_profitability collection for given POS, OD and Compartment")
                raise e1
        """
        """
        if trigger_name == 'opportunities':
            today = str(datetime.date.today())
            oag_data_h = access_db(net.MONGO_CLIENT_URL,
                                    net.TRIGGER_DB_NAME,
                                    'JUP_DB_OAG_od',
                                    {"POS": h_data['pos'],
                                    "origin": h_data['origin'],
                                    "destination": h_data['destination'],
                                    "Class1": h_data['compartment'],
                                    "carrier":net.Host_Airline_Code,
                                    "Date": today})
            oag_data = access_db(net.MONGO_CLIENT_URL,
                                    net.TRIGGER_DB_NAME,
                                    'JUP_DB_OAG_od',
                                    {"POS": h_data['pos'],
                                    "origin": h_data['origin'],
                                    "destination": h_data['destination'],
                                    "Class1": h_data['compartment'],
                                    "Date": today})
            if len(oag_data) != 0:
                if len(oag_data_h) != 0:
                    if len(oag_data_h) == 1:
                            deno= 0.0
                            for i in oag_data:
                                deno= deno+ i['pax_capacity']*i['product_rating']
                            num = oag_data_h[0]['pax_capacity']*oag_data_h[0]['product_rating']
                    else:
                        e1=errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
                        e1.append_to_error_list("There are more than 1 record in OAG_od collection for the Host for POS = "+h_data['pos']+", OD "+h_data['origin']+h_data['destination']+" and Compartment "+h_data['compartment'])
                        raise e1
                else:
                    e1=errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
                    e1.append_to_error_list("There are no. records in OAG_od collection for the Host for POS = "+h_data['pos']+", OD "+h_data['origin']+h_data['destination']+" and Compartment "+h_data['compartment'])
                    raise e1
            else:
                e1=errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
                e1.append_to_error_list("There are No records in OAG_od collection for POS = "+h_data['pos']+", OD "+h_data['origin']+h_data['destination']+" and Compartment "+h_data['compartment'])
                raise e1

            hms_data = access_db(net.MONGO_CLIENT_URL,
                                    net.TRIGGER_DB_NAME,
                                    'JUP_DB_Market_Share',
                                    {"POS": h_data['pos'],
                                    #"origin": h_data['origin'],
                                    #"destination": h_data['destination'],
                                    'od': h_data['origin']+h_data['destination'],
                                    "Class1": h_data['compartment'],
                                    "carrier":net.Host_Airline_Code,
                                    "Date": today})
            print len(hms_data)
            if len(hms_data) != 0:
                if len(hms_data) == 1:
                    hms = hms_data[0]['market_share']
                else:
                    e1=errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
                    e1.append_to_error_list("No of record for host in JUP_DB_Market_Share collection is greater than 1")
                    raise e1
            else:
                e1=errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
                e1.append_to_error_list("No records for host in JUP_DB_Market_Share collection")
                raise e1

            if deno != 0:
                fair_ms = float(num)/deno
                if fair_ms != 0:
                    change = float(hms)- (fair_ms*100)
                    #change = ((float(hms)- fair_ms)*100)/fair_ms
                    # Just take the difference
                    return change
                else:
                    e1=errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
                    e1.append_to_error_list("Calculated fair market share for given POS, OD and Compartment comes out to be zero, per. change can't be calculated")
                    raise e1
            else:
                e1=errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1, self.get_module_name(), self.get_arg_lists(inspect.currentframe()))
                e1.append_to_error_list("Sum of (pax_capacity*ratings) for all competitors is 0")
                raise e1
        """

    def fire_trigger(self):
        checked_fbs = []
        cursor = db.JUP_DB_Host_Pricing_Data.aggregate([
            {
              '$match': {
                    'competitor_farebasis': {'$ne': None}
              }
            },
            {
                '$group': {
                    '_id': {
                        'pos': '$pos',
                        'origin': '$origin',
                        'destination': '$destination',
                        'compartment': '$compartment'
                    }
                }
            }
        ])

        query = []
        for i in cursor:
            query.append(i['_id'])
        print query
        print 'num of combinations for pos,o,d,compartment in hpd is ' + str(len(query))
        for i in query:
            pos = i['pos']
            origin = i['origin']
            destination = i['destination']
            compartment = i['compartment']
            print pos,origin,destination,compartment
            # this 'e' will be the base error object, all others will be appended to this
            self.errors = errorClass.ErrorObject(0,
                                                 self.get_module_name(),
                                                 self.get_arg_lists(inspect.currentframe()))
            h_data = {"pos": pos, "origin": origin, "destination": destination, "compartment": compartment}

            self.old_doc_data = h_data
            triggers_list = af.trigger_hierarchy(self)
            triggers = []
            # j['desc'] == 'pax_changes_weekly' or j['desc'] == 'pax_changes_VLYR' or j['desc'] == 'pax_changes_VTGT'
            for j in triggers_list:
                if j['desc'] in [
                                # 'pax_changes_VTGT', 'pax_changes_weekly', 'pax_changes_VLYR',
                                #  'revenue_changes_weekly',
                                #  'revenue_changes_VLYR', 'revenue_changes_VTGT'
                                 'booking_changes_weekly',
                                 'booking_changes_VLYR', 'booking_changes_VTGT']:
                    triggers.append(j)
            print triggers
            for k in triggers:
                self.trigger_type = k['desc']
                print self.trigger_type
                self.triggering_data = k['triggering_event']
                # if any error in f_calculated, error object gets appended to base error object self.errors
                try:
                    response = self.calculate_changes(self.trigger_type, h_data)
                    if response:
                        try:
                            f_calculated = response['change']
                            data = response['data']
                        except KeyError:
                            f_calculated = None
                    else:
                        f_calculated = None
                    print f_calculated
                    if f_calculated is None:
                        e1 = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1, self.get_module_name(),
                                                    self.get_arg_lists(inspect.currentframe()))
                        e1.append_to_error_list("Calculated Change cannot be None")
                        raise e1
                    else:
                        pass
                    if k['priority'] > net.MAX_TRIGGER_PRIORITY_LEVEL:
                        e1 = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                                    self.get_module_name(),
                                                    self.get_arg_lists(inspect.currentframe()))
                        e1_desc = ''.join(["Priority of trigger i.e. ",
                                           str(k['priority']),
                                           " more than that of Maximum Supported by Jupiter(9) "])
                        e1.append_to_error_list(e1_desc)
                        raise e1
                    else:
                        pass
                    print '			' + str(f_calculated)
                    print f_calculated < k['lower_threshhold'] or f_calculated > k['upper_threshhold']
                    if f_calculated < k['lower_threshhold'] or f_calculated > k['upper_threshhold']:
                        hf = db.JUP_DB_Host_Pricing_Data.find({
                            'pos': h_data['pos'],
                            'origin': h_data['origin'],
                            'destination': h_data['destination'],
                            'compartment': h_data['compartment'],
                            'competitor_farebasis':{'$ne':None}
                        })
                        print hf.count()
                        # this loop is for firing multiple triggers on the basis of farebasis
                        for l in hf:
                            print l
                            if l['competitor_farebasis']:
                                if l['farebasis'] not in checked_fbs:
                                    checked_fbs.append(l['farebasis'])
                                    farebasis= l['farebasis']
                                    self.trigger_type = k['desc']
                                    self.priority = k['priority']
                                    self.old_doc_data= deepcopy(l)
                                    self.old_doc_data['value'] = data['val1']
                                    self.new_doc_data= deepcopy(l)
                                    self.new_doc_data['value'] = data['val2']
                                    self.triggering_data= k['triggering_event']
                                    self.pricing_action_id_at_trigger_time=l['pricing_action_id']
                                    af.create_trigger(self)
                                    # print 'SENT TO QUEUE'
                                    af.send_trigger_to_queue(self)
                                    # print len(checked_fbs)
                except errorClass.ErrorObject as esub:
                    self.append_error(self.errors, esub)
                    if esub.error_level <= errorClass.ErrorObject.WARNING:
                        error_flag = False
                    elif esub.error_level <= errorClass.ErrorObject.ERRORLEVEL1:
                        error_flag = True
                    elif esub.error_level <= errorClass.ErrorObject.ERRORLEVEL2:
                        error_flag = True
                        raise esub
                    # db.JUP_DB_Sending_Error_Collection.insert_one({'date' : datetime.datetime.now().strftime('%Y-%m-%d'),
                    #                                                'time': datetime.datetime.now().strftime('%H:%M:%S.%f'),
                    #                                                'trigger_type':self.trigger_type,
                    #                                                'error_description' : self.errors.__str__()})
                    print esub.__str__()


dummy_var = register_data_level_trigger()
