#almost complete


'''Batch program that will run daily and check if an event is about to occur in near future. If True, then trigger shall be raised.
  This check of event occurence will be true in three cases:
  1. If event start date is one year ahead of system date
  Trigger will be raised once every month for the next six months
  2. If event start date is 6 months ahead of system date
  Trigger will be raised twice every month for the next 5 months
  3. If event start date is 1 month ahead of system date
  Trigger will be raised biweekly until the end of the month
'''
from jupiter_AI.network_level_params import today, JUPITER_DB, SYSTEM_DATE
from jupiter_AI import client
from dateutil.relativedelta import relativedelta
from jupiter_AI.triggers.data_level.BookingChangesVLYR import BookingChangesVLYR
from jupiter_AI.triggers.data_level.BookingChangesWeekly import BookingChangesWeekly
from jupiter_AI.triggers.data_level.BookingChangesVTGT import BookingChangesVTGT
from jupiter_AI.triggers.data_level.PaxChangesVLYR import PaxChangesVLYR
from jupiter_AI.triggers.data_level.PaxChangesWeekly import PaxChangesWeekly
from jupiter_AI.triggers.data_level.PaxChangesVTGT import PaxChangesVTGT
from jupiter_AI.triggers.data_level.RevenueChangesWeekly import RevenueChangesWeekly
from jupiter_AI.triggers.data_level.RevenueChangesVLYR import RevenueChangesVLYR
from jupiter_AI.triggers.data_level.RevenueChangesVTGT import RevenueChangesVTGT
from jupiter_AI.triggers.data_level.YieldChangesVLYR import YieldChangesVLYR
from jupiter_AI.triggers.data_level.YieldChangesVTGT import YieldChangesVTGT
from jupiter_AI.triggers.data_level.YieldChangesWeekly import YieldChangesWeekly
import datetime
from copy import *

#db = client[JUPITER_DB]
                                                          
def get_event_start_dates():
    #
    # print ("getting event dates...")
    dates_events = []
    i = 1
    for i in range(12, 5, -1):
        dates_events.append(today + relativedelta(months=+i))
    last_date = dates_events[-1]
    while last_date >= today + relativedelta(months=+1):
        last_date = last_date - datetime.timedelta(days=15)
        dates_events.append(last_date)

    last_date = dates_events[-1]
    while last_date > today:
        if i % 2 != 0:
            last_date = last_date - datetime.timedelta(days=4)
        else:
            last_date = last_date - datetime.timedelta(days=3)
        i += 1
        dates_events.append(last_date)

    if dates_events[-1] <= today:
        del dates_events[-1]
    # print "OOOOOOOOOoooooooooo",dates_events
    return dates_events
def build_events_query(dates_events):
    or_query = []
    # create dictionary for the dates events
    for i in dates_events:
        or_query.append({"Start_date_" + str(i.year): {"$eq": i.strftime("%Y-%m-%d")}})
    # print or_query[0:3]
    return or_query
def get_events_data(query , data_list):
    #print ("getting events data...")
    # print "query!@#!@ ", query
    events_cursor = db.JUP_DB_Pricing_Calendar.find({"$or": query})
    events_list = list(events_cursor)
    # print events_list
    #print "length " , len(events_list)
    # for i in events_list:
    #     print i
    events_dict = dict()
    for i in query:
        key = i.items()[0][0]
        date_ = i.items()[0][1]["$eq"]
        # print "high " , i , key , date_
        for j in events_list:
            # caution: to be removed after getting data for last year.
            # date_2016_start = j["Start_date_2017"][4:]
            # date_2016_end = j["End_date_2017"][4:]
            # j["Start_date_2016"] = "2016" + date_2016_start
            # j["End_date_2016"] = "2016" + date_2016_end
            try:
                if j[key] == date_ and j['Market'] == data_list[0][3:9]:
                    events_dict['Event_name'] = j['Holiday_Name']
                    events_dict['dep_date_start'] = j[key]
                    events_dict['dep_date_end'] = j["End_date_" + date_[0:4]]
                    try:
                        if j["Start_date_" + str(int(date_[0:4]) - 1)] == ""  or j["End_date_" + str(int(date_[0:4]) - 1)] =="":
                            pass
                        else:
                            events_dict['dep_date_start_ly'] = j["Start_date_" + str(int(date_[0:4]) - 1)]
                            events_dict['dep_date_end_ly'] = j["End_date_" + str(int(date_[0:4]) - 1)]
                    except ValueError:
                        print ("fields are empty ")
            except KeyError:
                print("db does not contain the date")
    # print date_
    # print "event_dict%%%%%@$@@$%#%$$^%^^&*&((" , events_dict
    # print len(events_dict)

    # print (events_dict)
    return events_dict


def do_analysis_bvlyr(events_dict, data, data_list): #sampledone
    print("analysing bvlyr...################################################################")
    try:
        dep_date_start = events_dict['dep_date_start']
        dep_date_end = events_dict['dep_date_end']
        dep_date_start_ly = events_dict['dep_date_start_ly']
        dep_date_end_ly = events_dict['dep_date_end_ly']

        trigger_obj = BookingChangesVLYR(data, SYSTEM_DATE)
        book_date_end_obj = datetime.datetime.strptime(trigger_obj.trigger_date, '%Y-%m-%d')
        book_date_end = trigger_obj.trigger_date
        book_date_start = str(book_date_end_obj.year) + '-01-01'
        days_to_event = datetime.datetime.strptime(dep_date_start, "%Y-%m-%d") - datetime.datetime.strptime(book_date_end,
                                                                                                            "%Y-%m-%d")
        book_date_end_ly = (datetime.datetime.strptime(dep_date_start_ly, "%Y-%m-%d") - days_to_event).strftime("%Y-%m-%d")
        book_date_start_ly = book_date_end_ly[0:4] + '-01-01'
        trigger_obj = BookingChangesVLYR(data, SYSTEM_DATE)
        bookings_data_ty= trigger_obj.get_bookings_data(book_date_start=book_date_start,
                                               book_date_end=book_date_end,
                                               dep_date_start=dep_date_start,
                                               dep_date_end=dep_date_end,
                                               data_list=["DXBDXBKRRY"])
        bookings_data_ly= trigger_obj.get_bookings_data(book_date_start=book_date_start_ly,
                                                         book_date_end=book_date_end_ly,
                                                         dep_date_start=dep_date_start_ly,
                                                         dep_date_end=dep_date_end,
                                                         data_list=["DXBDXBKRRY"])

        capacity_data_ty = trigger_obj.get_capacity_data(dep_date_start=dep_date_start,
                                               dep_date_end=dep_date_end,
                                               data_list=data_list)
        capacity_data_ly = trigger_obj.get_capacity_data(dep_date_start=dep_date_start_ly,
                                                         dep_date_end=dep_date_end_ly,
                                                         data_list=["DXBDXBKRRY"])
        trigger_obj.get_trigger_details(trigger_name='booking_changes_VLYR')

        #   By considering the threshold is the trigger firing or not
        # bookings_data = bookings_data.sort_values(by='market')
        # capacity_data = capacity_data.sort_values(by='market')
        # st = time.time()
        for tup1, tup2, tup3, tup4 in zip(bookings_data_ty.iterrows(),
                                          capacity_data_ty.iterrows(),
                                          bookings_data_ly.iterrows(),
                                          capacity_data_ly.iterrows()):
            # print tup2
            trigger_obj.old_doc_data['origin'] = tup2[1]['od'][0:3]
            trigger_obj.old_doc_data['destination'] = tup2[1]['od'][3:]
            trigger_obj.old_doc_data['pos'] = tup2[1]['market'][0:3]
            trigger_obj.old_doc_data['compartment'] = tup2[1]['market'][9:]
            trigger_obj.new_doc_data = deepcopy(trigger_obj.old_doc_data)
            trigger_occurence = trigger_obj.check_trigger(booking_ly=tup3[1]['bookings_ty'],
                                                   booking_ty=tup1[1]['bookings_ty'],
                                                   capacity_ly=tup4[1]['capacity_ty'],
                                                   capacity_ty=tup2[1]['capacity_ty'])
            # print "time to check trigger: ", time.time() - st
            print '$$$$occurence$$$$', trigger_occurence

            trigger_obj.triggering_data = {
                'dep_date_start': dep_date_start,
                'dep_date_end': dep_date_end
            }

            # st = time.time()
            trigger_obj.generate_trigger_new(trigger_occurence,
                                      dep_date_start=dep_date_start,
                                      dep_date_end=dep_date_end)
    except KeyError:
        print("db does not conatin the key ")

def do_analysis_bwkly(events_dict, data, data_list): #done
    print("analysing bwkly...################################################################")
    dep_date_start = events_dict['dep_date_start']
    dep_date_end = events_dict['dep_date_end']
    trigger_obj = BookingChangesWeekly(data, SYSTEM_DATE)
    trigger_obj.do_analysis(dep_date_start, dep_date_end, data_list=data_list)

    # bookings_data = get_booking_data_bwkly(trigger_obj, events_dict)
    # print bookings_data
    # trigger_obj.get_trigger_details(trigger_name='booking_changes_weekly')
    # trigger_occurence = trigger_obj.check_trigger(bookings_data_lw=bookings_data['lw'],
    #                                               bookings_data_tw=bookings_data['tw'])
    # print("trigger status: ", trigger_occurence)
    # trigger_obj.triggering_data ={'dep_date_start': dep_date_start,
    #                               'dep_date_end': dep_date_end}
    # trigger_obj.generate_trigger_new(trigger_occurence,
    #                                      dep_date_start=dep_date_start,
    #                                      dep_date_end=dep_date_end)
    # print("analysing bwkly...done!")

def do_analysis_bvtgt(events_dict, data, data_list): # done doubt in flown pax data
    print("analysing bvtgt...################################################################")
    try:
        dep_date_start = events_dict['dep_date_start']
        dep_date_end = events_dict['dep_date_end']
        dep_date_start_ly = events_dict['dep_date_start_ly']
        dep_date_end_ly = events_dict['dep_date_end_ly']
        trigger_obj = BookingChangesVTGT(data, SYSTEM_DATE)
        book_date_end_obj = datetime.datetime.strptime(trigger_obj.trigger_date, '%Y-%m-%d')
        book_date_end = trigger_obj.trigger_date
        book_date_start = str(book_date_end_obj.year) + '-01-01'
        days_to_event = datetime.datetime.strptime(dep_date_start, "%Y-%m-%d") - datetime.datetime.strptime(book_date_end,
                                                                                                            "%Y-%m-%d")
        book_date_end_ly = (datetime.datetime.strptime(dep_date_start_ly, "%Y-%m-%d") - days_to_event).strftime("%Y-%m-%d")
        book_date_start_ly = book_date_end_ly[0:4] + '-01-01'
        bookings_data_ty = trigger_obj.get_bookings_data(book_date_start=book_date_start,
                                                         book_date_end=book_date_end,
                                                         dep_date_start=dep_date_start,
                                                         dep_date_end=dep_date_end,
                                                         data_list=data_list)
        bookings_data_ly = trigger_obj.get_bookings_data(book_date_start=book_date_start_ly,
                                                         book_date_end=book_date_end_ly,
                                                         dep_date_start=dep_date_start_ly,
                                                         dep_date_end=dep_date_end,
                                                         data_list=data_list)
        #   Getting the Target pax for the set of departure dates current year
        pax_target_data = trigger_obj.get_target_data(dep_date_start=dep_date_start,
                                                      dep_date_end=dep_date_end,
                                                      data_list=data_list,
                                                      parameter='pax')
        #   Getting Actual Flown pax for the departure period last year
        #may be incorrect flown pax data
        flown_pax_data = trigger_obj.get_sales_flown_data(book_date_start=book_date_start,
                                                          book_date_end=book_date_end,
                                                          dep_date_start=dep_date_start_ly,
                                                          dep_date_end=dep_date_end_ly,
                                                          data_list=data_list,
                                                          parameter='pax')
        trigger_obj.get_trigger_details(trigger_name='booking_changes_VTGT')
        for tup1, tup2, tup3, tup4 in zip(bookings_data_ly.iterrows(),
                                    flown_pax_data.iterrows(),
                                    pax_target_data.iterrows(),
                                    bookings_data_ty.iterrows()):
            trigger_obj.old_doc_data['pos'] = tup1[1]['market'][0:3]
            trigger_obj.old_doc_data['origin'] = tup1[1]['market'][3:6]
            trigger_obj.old_doc_data['destination'] = tup1[1]['market'][6:9]
            trigger_obj.old_doc_data['compartment'] = tup1[1]['market'][9:]
            #   Check whether the trigger is fired or not
            trigger_occurence = trigger_obj.check_trigger(booking_ly=tup1[1]['bookings_ty'],
                                                   booking_ty=tup4[1]['bookings_ty'],
                                                   flown_pax_ly=tup2[1]['ty'],
                                                   flown_pax_target_ty=tup3[1]['target_pax'])
            print 'occurence', trigger_occurence
            trigger_obj.triggering_data = {
            'dep_date_start': dep_date_start,
            'dep_date_end': dep_date_end,
            'book_date_start': book_date_start,
            'book_date_end': book_date_end,
            'ty': {
                'pax': tup4[1]['bookings_ty'],
                'target': tup3[1]['target_pax']
            }
            ,
            'ly': {
                'pax': tup1[1]['bookings_ty'],
                'target': tup2[1]['ty']
            }
        }
        # print trigger_obj.triggering_data
        # Generate the trigger considering the fares applicable from dep from and dep to
            trigger_obj.generate_trigger_new(trigger_occurence,
                                  dep_date_start=dep_date_start,
                                  dep_date_end=dep_date_end)
    except KeyError:
        print("db does not contain the key")

def do_analysis_pvlyr(events_dict, data, data_list):
    print("analysing pvlyr...################################################################")
    try:
        dep_date_start = events_dict['dep_date_start']
        dep_date_end = events_dict['dep_date_end']
        dep_date_start_ly = events_dict['dep_date_start_ly']
        dep_date_end_ly = events_dict['dep_date_end_ly']
        trigger_obj = PaxChangesVLYR(data, SYSTEM_DATE)
        book_date_end = trigger_obj.trigger_date
        book_date_start = str(today.year) + '-01-01'
        days_to_event = datetime.datetime.strptime(dep_date_start, "%Y-%m-%d") - datetime.datetime.strptime(book_date_end,
                                                                                                            "%Y-%m-%d")
        book_date_end_ly = (datetime.datetime.strptime(dep_date_start_ly, "%Y-%m-%d") - days_to_event).strftime("%Y-%m-%d")
        book_date_start_ly = book_date_end_ly[0:4] + '-01-01'
        pax_data_ty = trigger_obj.get_sales_flown_val(book_date_start,
                                                      book_date_end,
                                                      dep_date_start,
                                                      dep_date_end,
                                                      data_list= data_list,
                                                      today=SYSTEM_DATE,
                                                      parameter="pax")
        pax_data_ly = trigger_obj.get_sales_flown_val(book_date_start_ly,
                                                      book_date_end_ly,
                                                      dep_date_start_ly,
                                                      dep_date_end_ly,
                                                      data_list=data_list,
                                                      today=SYSTEM_DATE,
                                                      parameter="pax")
        capacity_data_ty = trigger_obj.get_capacity_data(dep_date_start, dep_date_end,data_list=data_list)
        capacity_data_ly = trigger_obj.get_capacity_data(dep_date_start_ly, dep_date_end_ly,data_list=data_list)
        trigger_obj.get_trigger_details(trigger_name='pax_changes_VLYR')
        for tup1, tup2, tup3, tup4 in zip(pax_data_ty.iterrows(),
                                          capacity_data_ty.iterrows(),
                                          pax_data_ly.iterrows(),
                                          capacity_data_ly.iterrows()):
            trigger_obj.old_doc_data['pos'] = tup1[1]['market'][0:3]
            trigger_obj.old_doc_data['origin'] = tup1[1]['market'][3:6]
            trigger_obj.old_doc_data['destination'] = tup1[1]['market'][6:9]
            trigger_obj.old_doc_data['compartment'] = tup1[1]['market'][9:]
            trigger_obj.new_doc_data = deepcopy(trigger_obj.old_doc_data)
            trigger_occurence = trigger_obj.check_trigger(pax_ly=tup3[1]['ty'],
                                                pax_ty=tup1[1]['ty'],
                                                capacity_ly=tup4[1]['capacity_ty'],
                                                capacity_ty=tup2[1]['capacity_ty'])
            trigger_obj.triggering_data = {
            'dep_date_start': dep_date_start,
            'dep_date_end': dep_date_end,
            'ly': {
                'pax': tup3[1]['ty'],
                'capacity': tup4[1]['capacity_ty']
            }
            ,
            'ty': {
                'pax': tup1[1]['ty'],
                'capacity': tup2[1]['capacity_ty']
            }
        }
            trigger_obj.generate_trigger_new(trigger_occurence,
                                         dep_date_start=dep_date_start,
                                         dep_date_end=dep_date_end)
    except KeyError:
        print("db does not contain the key ")

def do_analysis_pwkly(events_dict, data, data_list): #done
    print("analysing pwkly...################################################################")
    dep_date_start = events_dict['dep_date_start']
    dep_date_end = events_dict['dep_date_end']
    trigger_obj = PaxChangesWeekly(data, SYSTEM_DATE)
    trigger_obj.do_analysis(dep_date_start, dep_date_end,data_list= data_list)

def do_analysis_pvtgt(events_dict, data ,data_list):
    print("analysing pvtgt...################################################################")
    try:
        dep_date_start = events_dict['dep_date_start']
        dep_date_end = events_dict['dep_date_end']
        dep_date_start_ly = events_dict['dep_date_start_ly']
        dep_date_end_ly = events_dict['dep_date_end_ly']
        trigger_obj = PaxChangesVTGT(data, SYSTEM_DATE)
        book_date_end = trigger_obj.trigger_date
        book_date_start = str(today.year) + '-01-01'
        days_to_event = datetime.datetime.strptime(dep_date_start, "%Y-%m-%d") - datetime.datetime.strptime(book_date_end,
                                                                                                            "%Y-%m-%d")
        book_date_end_ly = (datetime.datetime.strptime(dep_date_start_ly, "%Y-%m-%d") - days_to_event).strftime("%Y-%m-%d")
        book_date_start_ly = str(today.year - 1) + '-01-01'

        pax_data_ty= trigger_obj.get_sales_flown_data(book_date_start=book_date_start,
                                             book_date_end=book_date_end,
                                             dep_date_start=dep_date_start,
                                             dep_date_end=dep_date_end,
                                             parameter='pax',
                                             data_list=data_list)
        pax_data_ly = trigger_obj.get_sales_flown_data(book_date_start=book_date_start_ly,
                                                    book_date_end=book_date_end_ly,
                                                    dep_date_start=dep_date_start_ly,
                                                    dep_date_end=dep_date_end_ly,
                                                    parameter='pax',
                                                    data_list=data_list)
        flown_pax_target_val = trigger_obj.get_target_data(dep_date_start=dep_date_start,
                                                    dep_date_end=dep_date_end,
                                                    data_list=data_list,
                                                    parameter='pax')
        flown_pax_ly_val = trigger_obj.get_sales_flown_data(book_date_start=book_date_start_ly,
                                                     book_date_end=book_date_end,
                                                     dep_date_start=dep_date_start_ly,
                                                     dep_date_end=dep_date_end_ly,
                                                     parameter='pax',
                                                     data_list=data_list)
        trigger_obj.get_trigger_details(trigger_name='pax_changes_VTGT')
        #   Check whether the trigger is fired or not
        for tup1, tup2, tup3, tup4 in zip(pax_data_ty.iterrows(),
                                    flown_pax_target_val.iterrows(),
                                    flown_pax_ly_val.iterrows(),
                                    pax_data_ly.iterrows()):
            trigger_obj.old_doc_data['pos'] = tup1[1]['market'][0:3]
            trigger_obj.old_doc_data['origin'] = tup1[1]['market'][3:6]
            trigger_obj.old_doc_data['destination'] = tup1[1]['market'][6:9]
            trigger_obj.old_doc_data['compartment'] = tup1[1]['market'][9:]
            trigger_obj.new_doc_data = deepcopy(trigger_obj.old_doc_data)
            trigger_occurence = trigger_obj.check_trigger(pax_ly=tup4[1]['ty'],
                                                   pax_ty=tup1[1]['ty'],
                                                   flown_pax_ly=tup3[1]['ty'],
                                                   flown_pax_target_ty=tup2[1]['target_pax'])
            print 'occurence', trigger_occurence

            trigger_obj.triggering_data = {
                'dep_date_start': dep_date_start,
                'dep_date_end': dep_date_end,
                'ty': {
                    'dep_date_start': dep_date_start,
                    'dep_date_end': dep_date_end,
                    'book_date_start': book_date_start,
                    'book_date_end': book_date_end,
                    'pax': tup1[1]['ty'],
                    'target': tup2[1]['target_pax']
                }
                ,
                'ly': {
                    'dep_date_start': dep_date_start_ly,
                    'dep_date_end': dep_date_end_ly,
                    'book_date_start': book_date_start_ly,
                    'book_date_end': book_date_end_ly,
                    'pax': tup4[1]['ty'],
                    'target': tup2[1]['target_pax']
                }
            }
            print trigger_obj.triggering_data
        # Generate the trigger considering the fares applicable from dep from and dep to
            trigger_obj.generate_trigger_new(trigger_occurence,
                                         dep_date_start=dep_date_start,
                                         dep_date_end=dep_date_end)
    except KeyError:
        print ("db does not contain the key ")

def do_analysis_rev_weekly(events_dict , data, data_list): #done
    print("analysing rev_weekly...################################################################")
    dep_date_start = events_dict['dep_date_start']
    dep_date_end = events_dict['dep_date_end']
    trigger_obj = RevenueChangesWeekly(data, SYSTEM_DATE)
    trigger_obj.do_analysis(dep_date_start, dep_date_end, data_list=data_list)

def do_analysis_rev_vlyr(events_dict , data, data_list): #done
    print("analysing rev_vlyr...################################################################")
    try:
        dep_date_start = events_dict['dep_date_start']
        dep_date_end = events_dict['dep_date_end']
        dep_date_start_ly = events_dict['dep_date_start_ly']
        dep_date_end_ly = events_dict['dep_date_end_ly']
        trigger_obj = RevenueChangesVLYR(data, SYSTEM_DATE)
        book_date_end = trigger_obj.trigger_date
        book_date_start = str(today.year) + '-01-01'
        days_to_event = datetime.datetime.strptime(dep_date_start, "%Y-%m-%d") - datetime.datetime.strptime(book_date_end,
                                                                                                            "%Y-%m-%d")
        book_date_end_ly = (datetime.datetime.strptime(dep_date_start_ly, "%Y-%m-%d") - days_to_event).strftime("%Y-%m-%d")
        book_date_start_ly = book_date_end_ly[0:4] + '-01-01'
        revenue_data_ty = trigger_obj.get_sales_flown_data(book_date_start=book_date_start,
                                                 book_date_end=book_date_end,
                                                 dep_date_start=dep_date_start,
                                                 dep_date_end=dep_date_end,
                                                 parameter='revenue',
                                                 data_list=data_list)
        revenue_data_ly = trigger_obj.get_sales_flown_data(book_date_start=book_date_start_ly,
                                                        book_date_end=book_date_end_ly,
                                                        dep_date_start=dep_date_start_ly,
                                                        dep_date_end=dep_date_end_ly,
                                                        parameter='revenue',
                                                        data_list=data_list)
        capacity_data_ty = trigger_obj.get_capacity_data(dep_date_start=dep_date_start,
                                               dep_date_end=dep_date_end,
                                               data_list=data_list)
        capacity_data_ly = trigger_obj.get_capacity_data(dep_date_start=dep_date_start_ly,
                                               dep_date_end=dep_date_end_ly,
                                               data_list=data_list)
        trigger_obj.get_trigger_details(trigger_name='revenue_changes_VLYR')
        for tup1, tup2, tup3, tup4 in zip(revenue_data_ty.iterrows(),
                                          capacity_data_ty.iterrows(),
                                          revenue_data_ly.iterrows(),
                                          capacity_data_ly.iterrows()):
            trigger_obj.old_doc_data['pos'] = tup1[1]['market'][0:3]
            trigger_obj.old_doc_data['origin'] = tup1[1]['market'][3:6]
            trigger_obj.old_doc_data['destination'] = tup1[1]['market'][6:9]
            trigger_obj.old_doc_data['compartment'] = tup1[1]['market'][9:]
            trigger_obj.new_doc_data = deepcopy(trigger_obj.old_doc_data)
            trigger_occurence = trigger_obj.check_trigger(revenue_ly=tup3[1]['ty'],
                                                   revenue_ty=tup1[1]['ty'],
                                                   capacity_ly=tup4[1]['capacity_ty'],
                                                   capacity_ty=tup2[1]['capacity_ty'])
            print 'occurence', trigger_occurence

            trigger_obj.triggering_data = {
                'dep_date_start': dep_date_start,
                'dep_date_end': dep_date_end,
                'ly': {
                    'dep_date_start': dep_date_start_ly,
                    'dep_date_end': dep_date_end_ly,
                    'revenue ': tup3[1]['ty'],
                    'capacity': tup4[1]['capacity_ty']
                }
                ,
                'ty': {
                    'dep_date_start': dep_date_start,
                    'dep_date_end': dep_date_end,
                    'revenue ': tup1[1]['ty'],
                    'capacity': tup2[1]['capacity_ty']
                }
            }
            trigger_obj.generate_trigger_new(trigger_occurence,
                                            dep_date_start=dep_date_start,
                                            dep_date_end=dep_date_end)
    except KeyError:
        print("db does not contain the key ")

def do_analysis_rev_vtgt(events_dict, data, data_list):
    print("analysing rev_vtgt...################################################################")
    try:
        dep_date_start = events_dict['dep_date_start']
        dep_date_end = events_dict['dep_date_end']
        dep_date_start_ly = events_dict['dep_date_start_ly']
        dep_date_end_ly = events_dict['dep_date_end_ly']
        trigger_obj = RevenueChangesVTGT(data, SYSTEM_DATE)
        book_date_start = str(today.year) + '-01-01'
        book_date_end = SYSTEM_DATE
        days_to_event = datetime.datetime.strptime(dep_date_start, "%Y-%m-%d") - datetime.datetime.strptime(book_date_end,
                                                                                                            "%Y-%m-%d")
        book_date_end_ly = (datetime.datetime.strptime(dep_date_start_ly, "%Y-%m-%d") - days_to_event).strftime("%Y-%m-%d")
        book_date_start_ly = book_date_end_ly[0:4] + '-01-01'
        revenue_data_ty= trigger_obj.get_sales_flown_data(book_date_start=book_date_start,
                                                 book_date_end=book_date_end,
                                                 dep_date_start=dep_date_start,
                                                 dep_date_end=dep_date_end,
                                                 parameter='revenue',
                                                 data_list=data_list)
        revenue_data_ly = trigger_obj.get_sales_flown_data(book_date_start=book_date_start_ly,
                                                        book_date_end=book_date_end_ly,
                                                        dep_date_start=dep_date_start_ly,
                                                        dep_date_end=dep_date_end_ly,
                                                        parameter='revenue',
                                                        data_list=data_list)
        #   Getting the Target pax for the set of departure dates current year
        flown_revenue_target_val = trigger_obj.get_target_data(dep_date_start=dep_date_start,
                                                        dep_date_end=dep_date_end,
                                                        parameter='revenue',
                                                        data_list=data_list)
        flown_revenue_ly_val = trigger_obj.get_sales_flown_data(book_date_start=book_date_start_ly,
                                                         book_date_end=book_date_end_ly,
                                                         dep_date_start=dep_date_start_ly,
                                                         dep_date_end=dep_date_end_ly,
                                                         parameter='revenue',
                                                         data_list=data_list)

        trigger_obj.get_trigger_details(trigger_name='revenue_changes_VTGT')
        #Check whether the trigger is fired or not
        for tup1, tup2, tup3, tup4 in zip(revenue_data_ty.iterrows(),
                                    flown_revenue_target_val.iterrows(),
                                    flown_revenue_ly_val.iterrows(),
                                    revenue_data_ly.iterrows()):
            trigger_obj.old_doc_data['pos'] = tup1[1]['market'][0:3]
            trigger_obj.old_doc_data['origin'] = tup1[1]['market'][3:6]
            trigger_obj.old_doc_data['destination'] = tup1[1]['market'][6:9]
            trigger_obj.old_doc_data['compartment'] = tup1[1]['market'][9:]
            trigger_obj.new_doc_data = deepcopy(trigger_obj.old_doc_data)
            trigger_occurence = trigger_obj.check_trigger(revenue_ly=tup4[1]['ty'],
                                                   revenue_ty=tup1[1]['ty'],
                                                   flown_revenue_val=tup3[1]['ty'],
                                                   flown_revenue_target_val=tup2[1]['target_revenue'])
            print 'occurence', trigger_occurence

            trigger_obj.triggering_data = {
                'dep_date_start': dep_date_start,
                'dep_date_end': dep_date_end,
                'ty': {
                    'dep_date_start': dep_date_start,
                    'dep_date_end': dep_date_end,
                    'book_date_start': book_date_start,
                    'book_date_end': book_date_end,
                    'revenue': tup1[1]['ty'],
                    'target': tup2[1]['target_revenue']
                }
                ,
                'ly': {
                    'dep_date_start': dep_date_start_ly,
                    'dep_date_end': dep_date_end_ly,
                    'book_date_start': book_date_start_ly,
                    'book_date_end': book_date_end_ly,
                    'revenue': tup4[1]['ty'],
                    'target': tup3[1]['ty']
                }
            }
            # Generate the trigger considering the fares applicable from dep from and dep to
            trigger_obj.generate_trigger_new(trigger_occurence,
                                      dep_date_start=dep_date_start,
                                      dep_date_end=dep_date_end)
    except:
        print (" db does not contain the key ")

def do_analysis_yield_weekly(events_dict,data, data_list):
    print("analysing yield_weekly...################################################################")
    dep_date_start = events_dict['dep_date_start']
    dep_date_end = events_dict['dep_date_end']
    trigger_obj = YieldChangesWeekly(data, SYSTEM_DATE)
    trigger_obj.do_analysis(dep_date_start, dep_date_end, data_list=data_list)

def do_analysis_yield_vtgt(events_dict , data, data_list):
    print("analysing yield_vtgt...##################################################################")
    dep_date_start = events_dict['dep_date_start']
    dep_date_end = events_dict['dep_date_end']
    book_date_start = str(today.year) + '-01-01'
    book_date_end = SYSTEM_DATE
    trigger_obj = YieldChangesVTGT(data, SYSTEM_DATE)
    yield_data = trigger_obj.get_yield_data(book_date_start=book_date_start,
                                     book_date_end=book_date_end,
                                     dep_date_start=dep_date_start,
                                     dep_date_end=dep_date_end,
                                     data_list=data_list)
    print "%%%%%yielddata", yield_data
    yield_target = trigger_obj.get_target_yield_data(dep_date_start=dep_date_start,
                                              dep_date_end=dep_date_end,
                                              data_list=data_list)
    print "%%%%%%%%%%%%%%%%%yieldtarget", yield_target
    yield_data.sort_values(by='market', inplace=True)
    yield_target.sort_values(by='market', inplace=True)
    trigger_obj.get_trigger_details(trigger_name='yield_changes_VTGT')
    for tup1, tup2 in zip(yield_data.iterrows(),
                          yield_target.iterrows()):
        trigger_obj.old_doc_data['pos'] = tup1[1]['market'][0:3]
        trigger_obj.old_doc_data['origin'] = tup1[1]['market'][3:6]
        trigger_obj.old_doc_data['destination'] = tup1[1]['market'][6:9]
        trigger_obj.old_doc_data['compartment'] = tup1[1]['market'][-1]
        trigger_obj.new_doc_data = deepcopy(trigger_obj.old_doc_data)
        trigger_occurence = trigger_obj.check_trigger(yield_val=tup1[1]['yield_ty'],
                                               yield_target_val=tup2[1]['target_yield'])
        print 'occurence', trigger_occurence
        # print "yield_val: ", yield_data['ty']
        # print "target: ", yield_target

        trigger_obj.triggering_data = {
            'dep_date_start': dep_date_start,
            'dep_date_end': dep_date_end
                                      }
        trigger_obj.generate_trigger_new(trigger_occurence,
                                  dep_date_start=dep_date_start,
                                  dep_date_end=dep_date_end)

def do_analysis_yield_vlyr(events_dict,data , data_list):
    print("analysing yield_vlyr...################################################################")
    try:
        dep_date_start = events_dict['dep_date_start']
        dep_date_end = events_dict['dep_date_end']
        dep_date_start_ly = events_dict['dep_date_start_ly']
        dep_date_end_ly = events_dict['dep_date_end_ly']
        trigger_obj = YieldChangesVLYR(data, SYSTEM_DATE)
        book_date_end_obj = datetime.datetime.strptime(trigger_obj.trigger_date, '%Y-%m-%d')
        book_date_end = trigger_obj.trigger_date
        book_date_start = str(book_date_end_obj.year) + '-01-01'
        days_to_event = datetime.datetime.strptime(dep_date_start, "%Y-%m-%d") - datetime.datetime.strptime(book_date_end,
                                                                                                            "%Y-%m-%d")
        book_date_end_ly = (datetime.datetime.strptime(dep_date_start_ly, "%Y-%m-%d") - days_to_event).strftime("%Y-%m-%d")
        book_date_start_ly = book_date_end_ly[0:4] + '-01-01'
        yield_data_ty = trigger_obj.get_yield_data(book_date_start=book_date_start,
                                         book_date_end=book_date_end,
                                         dep_date_start=dep_date_start,
                                         dep_date_end=dep_date_end,
                                         data_list=data_list)
        yield_data_ly = trigger_obj.get_yield_data(book_date_start=book_date_start_ly,
                                                book_date_end=book_date_end_ly,
                                                dep_date_start=dep_date_start_ly,
                                                dep_date_end=dep_date_end_ly,
                                                data_list=data_list)
        capacity_data_ty = trigger_obj.get_capacity_data(dep_date_start=dep_date_start,
                                               dep_date_end=dep_date_end,
                                               data_list=data_list)
        capacity_data_ly = trigger_obj.get_capacity_data(dep_date_start=dep_date_start_ly,
                                                         dep_date_end=dep_date_end_ly,
                                                         data_list=data_list)
        # final_data = yield_data.merge(capacity_data.drop('market', axis=1), how='left', on='od')
        trigger_obj.get_trigger_details(trigger_name='yield_changes_VLYR')
        for tup1, tup2, tup3, tup4 in zip(yield_data_ty.iterrows(),
                              capacity_data_ty.iterrows(),
                              yield_data_ly.iterrows(),
                              capacity_data_ly.iterrows()):
            trigger_obj.old_doc_data['pos'] = tup1[1]['market'][0:3]
            trigger_obj.old_doc_data['origin'] = tup1[1]['market'][3:6]
            trigger_obj.old_doc_data['destination'] = tup1[1]['market'][6:9]
            trigger_obj.old_doc_data['compartment'] = tup1[1]['market'][-1]
            trigger_obj.new_doc_data = deepcopy(trigger_obj.old_doc_data)
            trigger_occurence = trigger_obj.check_trigger(yield_ly=tup3[1]['yield_ty'],
                                                   yield_ty=tup1[1]['yield_ty'],
                                                   capacity_ly=tup4[1]['capacity_ty'],
                                                   capacity_ty=tup2[1]['capacity_ty'])
            print 'occurence', trigger_occurence

            trigger_obj.triggering_data = {
                'dep_date_start': dep_date_start,
                'dep_date_end': dep_date_end,
                'ly': {
                    'yield': tup3[1]['yield_ty'],
                    'capacity': tup4[1]['capacity_ty']
                }
                ,
                'ty': {
                    'dep_date_start': dep_date_start,
                    'dep_date_end': dep_date_end,
                    'yield': tup1[1]['yield_ty'],
                    'capacity': tup2[1]['capacity_ty']
                }
            }
            trigger_obj.generate_trigger_new(trigger_occurence,
                                      dep_date_start=dep_date_start,
                                      dep_date_end=dep_date_end)
    except:
        print("db does not contain the key ")
def main_func(events_dict,data,data_list):
    if len(events_dict)!=0:
        do_analysis_bvlyr(events_dict, data , data_list)
        do_analysis_bwkly(events_dict, data, data_list)
        do_analysis_bvtgt(events_dict, data, data_list)
        do_analysis_pvlyr(events_dict, data, data_list)
        do_analysis_pwkly(events_dict, data, data_list)
        do_analysis_pvtgt(events_dict, data, data_list)
        do_analysis_rev_weekly(events_dict, data, data_list)
        do_analysis_rev_vlyr(events_dict, data, data_list)
        do_analysis_rev_vtgt(events_dict, data, data_list)
        do_analysis_yield_weekly(events_dict, data, data_list)
        do_analysis_yield_vtgt(events_dict, data, data_list)
        do_analysis_yield_vlyr(events_dict, data, data_list)
    else:
        print "No EVENTS for market in data_list"

if __name__ == '__main__':
    data_list = ["DXBDXBKRRY"] #numnber of market to be inserted here
    data = {
        'pos': data_list[0:3],
        'origin': data_list[3:6],
        'destination':data_list[6:9],
        'compartment':data_list[9:9]
           }
    event_dates = get_event_start_dates()
    query1 = build_events_query(event_dates)
    events_dict1= get_events_data(query1, data_list)
    main_func(events_dict1,data, data_list)


















