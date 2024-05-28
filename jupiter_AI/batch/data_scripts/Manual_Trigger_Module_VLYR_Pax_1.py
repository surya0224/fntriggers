# coding: utf-8

# In[116]:

from copy import deepcopy
import copy
import json
import pymongo
from pymongo.errors import BulkWriteError
from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne
import collections
# import pandas as pd
import numpy as np
from datetime import datetime
import datetime as dt
import time
import timeit
from dateutil.relativedelta import relativedelta

from jupiter_AI import client, JUPITER_DB
from jupiter_AI.logutils import measure
from jupiter_AI.network_level_params import SYSTEM_DATE, JUPITER_LOGGER

# Connect mongodb db business layer
# fzDBConn = client[JUPITER_DB]

"""
global parameter pax , revenue weightage
"""
SYSTEM_DATE = "2023-06-18"

Pax_weight = 60
Revenue_weight = 40


# function for compare and update all the fares
# We are not going to use this function as per current requirements
# @measure(JUPITER_LOGGER)
# def farebasisComp(sale_farebasis, latestYR_sale_farebasis, classtype=classtype, Pax_weight=Pax_weight,
#                   Revenue_weight=Revenue_weight):
#     if (sale_farebasis != None and latestYR_sale_farebasis != None):
#         common_farebasis = latestYR_sale_farebasis[:]
#         full_sale_farebasis = copy.deepcopy(sale_farebasis)
#         # update the matchable fares last year with this year
#         for each_common_farebasis in range(len(common_farebasis)):
#             for each_sale_farebasis in range(len(sale_farebasis)):
#                 if common_farebasis[each_common_farebasis]["fare_basis"] == sale_farebasis[each_sale_farebasis][
#                     "fare_basis"]:
#                     common_farebasis[each_common_farebasis]['pax_1'] = sale_farebasis[each_sale_farebasis]['pax']
#                     common_farebasis[each_common_farebasis]['rev_1'] = sale_farebasis[each_sale_farebasis]['rev']
#                     sale_farebasis[each_sale_farebasis]["Updated"] = True
#                 else:
#                     pass
#
#         # del sale_farebasis[0]["fare_basis"]
#         if sale_farebasis != []:
#             # insert last year not matchable fare also to this year fares data with pax = 0
#             for len_sale_farebasis in range(len(sale_farebasis)):
#                 if ("Updated" not in sale_farebasis[len_sale_farebasis]):
#                     sale_farebasis[len_sale_farebasis]["pax_1"] = sale_farebasis[len_sale_farebasis]["pax"]
#                     sale_farebasis[len_sale_farebasis]["rev_1"] = sale_farebasis[len_sale_farebasis]["rev"]
#                     # swap pax and pax_1 value
#                     sale_farebasis[len_sale_farebasis]["pax"] = 0
#                     sale_farebasis[len_sale_farebasis]["rev"] = 0
#                     common_farebasis.append(sale_farebasis[len_sale_farebasis])
#                     #                 print(sale_farebasis[len_sale_farebasis])
#
#         for len_common_farebasis in range(len(common_farebasis)):
#             #         fare type include
#             if (classtype[common_farebasis[len_common_farebasis]['fare_basis'][0:1]]):
#                 common_farebasis[len_common_farebasis]['type'] = classtype[
#                     common_farebasis[len_common_farebasis]['fare_basis'][0:1]]
#             pax = common_farebasis[len_common_farebasis]['pax']
#             rev = common_farebasis[len_common_farebasis]['rev']
#             pax_1 = common_farebasis[len_common_farebasis]['pax_1']
#             rev_1 = common_farebasis[len_common_farebasis]['rev_1']
#
#             paxVLYR = 0
#             revVLYR = 0
#             effectivity = 0
#             if (rev_1 != 0 and pax_1 != 0):
#                 paxVLYR = ((pax - pax_1) / pax_1) * 100
#                 revVLYR = ((rev - rev_1) / rev_1) * 100
#             if (paxVLYR != 0 and revVLYR != 0):
#                 effectivity = ((paxVLYR * Pax_weight) + (revVLYR * Revenue_weight)) / (Pax_weight + Revenue_weight)
#                 #         add effectivity calc for reference
#             common_farebasis[len_common_farebasis]['effectivity_calc'] = effectivity
#             if (effectivity > 0):
#                 common_farebasis[len_common_farebasis]['effectivity_flag'] = True
#             else:
#                 common_farebasis[len_common_farebasis]['effectivity_flag'] = False
#                 # print(common_farebasis[len_common_farebasis]['effectivity_flag'])
#                 #         common_farebasis[len_common_farebasis] =
#         return (common_farebasis)
#
#     elif (sale_farebasis == None and latestYR_sale_farebasis != None):
#         common_farebasis = latestYR_sale_farebasis[:]
#
#         for len_common_farebasis in range(len(common_farebasis)):
#             #         fare type include
#             if (classtype[common_farebasis[len_common_farebasis]['fare_basis'][0:1]]):
#                 common_farebasis[len_common_farebasis]['type'] = classtype[
#                     common_farebasis[len_common_farebasis]['fare_basis'][0:1]]
#             pax = common_farebasis[len_common_farebasis]['pax']
#             rev = common_farebasis[len_common_farebasis]['rev']
#             pax_1 = common_farebasis[len_common_farebasis]['pax_1']
#             rev_1 = common_farebasis[len_common_farebasis]['rev_1']
#
#             paxVLYR = 0
#             revVLYR = 0
#             effectivity = 0
#             if (rev_1 != 0 and pax_1 != 0):
#                 paxVLYR = ((pax - pax_1) / pax_1) * 100
#                 revVLYR = ((rev - rev_1) / rev_1) * 100
#             if (paxVLYR != 0 and revVLYR != 0):
#                 effectivity = ((paxVLYR * Pax_weight) + (revVLYR * Revenue_weight)) / (Pax_weight + Revenue_weight)
#                 #         add effectivity calc for reference
#             common_farebasis[len_common_farebasis]['effectivity_calc'] = effectivity
#             if (effectivity > 0):
#                 common_farebasis[len_common_farebasis]['effectivity_flag'] = True
#             else:
#                 common_farebasis[len_common_farebasis]['effectivity_flag'] = False
#                 # print(common_farebasis[len_common_farebasis]['effectivity_flag'])
#                 #         common_farebasis[len_common_farebasis] =
#         return (common_farebasis)
#
#     elif (sale_farebasis != None and latestYR_sale_farebasis == None):
#         common_farebasis = []
#         # del sale_farebasis[0]["fare_basis"]
#         if sale_farebasis != []:
#             # insert last year not matchable fare also to this year fares data with pax = 0
#             for len_sale_farebasis in range(len(sale_farebasis)):
#                 if ("Updated" not in sale_farebasis[len_sale_farebasis]):
#                     sale_farebasis[len_sale_farebasis]["pax_1"] = sale_farebasis[len_sale_farebasis]["pax"]
#                     sale_farebasis[len_sale_farebasis]["rev_1"] = sale_farebasis[len_sale_farebasis]["rev"]
#                     # swap pax and pax_1 value
#                     sale_farebasis[len_sale_farebasis]["pax"] = 0
#                     sale_farebasis[len_sale_farebasis]["rev"] = 0
#                     common_farebasis.append(sale_farebasis[len_sale_farebasis])
#                     #                 print(sale_farebasis[len_sale_farebasis])
#
#         for len_common_farebasis in range(len(common_farebasis)):
#             #         fare type include
#             if (classtype[common_farebasis[len_common_farebasis]['fare_basis'][0:1]]):
#                 common_farebasis[len_common_farebasis]['type'] = classtype[
#                     common_farebasis[len_common_farebasis]['fare_basis'][0:1]]
#             pax = common_farebasis[len_common_farebasis]['pax']
#             rev = common_farebasis[len_common_farebasis]['rev']
#             pax_1 = common_farebasis[len_common_farebasis]['pax_1']
#             rev_1 = common_farebasis[len_common_farebasis]['rev_1']
#
#             paxVLYR = 0
#             revVLYR = 0
#             effectivity = 0
#             if (rev_1 != 0 and pax_1 != 0):
#                 paxVLYR = ((pax - pax_1) / pax_1) * 100
#                 revVLYR = ((rev - rev_1) / rev_1) * 100
#             if (paxVLYR != 0 and revVLYR != 0):
#                 effectivity = ((paxVLYR * Pax_weight) + (revVLYR * Revenue_weight)) / (Pax_weight + Revenue_weight)
#                 #         add effectivity calc for reference
#             common_farebasis[len_common_farebasis]['effectivity_calc'] = effectivity
#             if (effectivity > 0):
#                 common_farebasis[len_common_farebasis]['effectivity_flag'] = True
#             else:
#                 common_farebasis[len_common_farebasis]['effectivity_flag'] = False
#                 # print(common_farebasis[len_common_farebasis]['effectivity_flag'])
#                 #         common_farebasis[len_common_farebasis] =
#         return (common_farebasis)
#     else:
#         return None

@measure(JUPITER_LOGGER)
def main(pos, client):
    print pos
    fzDBConn = client[JUPITER_DB]
    # print pos
    # In[123]:
    classtype = dict()
    class_crscr = fzDBConn.JUP_DB_Booking_Class.find({"Code": {"$ne": ""}})
    for each_class_crscr in class_crscr:
        classtype[each_class_crscr['Code']] = each_class_crscr['type']

    num = 1
    updateBulk = fzDBConn.JUP_DB_Manual_Triggers_Module.initialize_unordered_bulk_op()
    dateformat = '%Y-%m-%d'
    yearformat = '%Y'
    monthformat = '%m'
    system_date = datetime.strptime(SYSTEM_DATE, '%Y-%m-%d');
    lastYearSameDate_ISO = system_date + dt.timedelta(days=-365)
    lastYearSameDate = datetime.strftime(lastYearSameDate_ISO, dateformat)

    # for net pax
    lastYearSevenDays_OLD_ISO = lastYearSameDate_ISO + dt.timedelta(days=-7)

    print "lastYearSameDate",lastYearSameDate
    prime_crscr = fzDBConn.JUP_DB_Manual_Triggers_Module.find({"pos.City":pos},
                                                              {"trx_date": 1, "dep_date": 1, "pos": 1,
                                                               "origin": 1, "destination": 1, "compartment": 1,
                                                               "book_pax": 1, "book_revenue": 1, "sale_pax": 1,
                                                               "sale_revenue": 1, "flown_pax": 1, "flown_revenue": 1,
                                                               "sale_farebasis": 1, "flown_farebasis": 1, "distance": 1})
    count = prime_crscr.count()
    for each_crscr in prime_crscr:
        try:

            # print(" S/B "+str(count)+" / "+str(num))

            #         predict the last year or next years dep_date and trx_date to update pax_1 revenue_1 value

            # print(each_crscr['pos']['City'])
            dateformat = '%Y-%m-%d'
            ISO_trx_date = datetime.strptime(each_crscr['trx_date'], dateformat)
            ISO_dep_date = datetime.strptime(each_crscr['dep_date'], dateformat)

            next_year_ISO_trx_date = ISO_trx_date + dt.timedelta(days=365)
            next_year_ISO_dep_date = ISO_dep_date + dt.timedelta(days=365)

            next_year_trx_date = next_year_ISO_trx_date.strftime(dateformat)
            next_year_dep_date = next_year_ISO_dep_date.strftime(dateformat)

            dep_month = int(next_year_ISO_dep_date.strftime("%m"))
            dep_year = int(next_year_ISO_dep_date.strftime("%Y"))

            booking_pax_1 = 0
            booking_revenue_1 = 0
            sale_pax_1 = 0
            sale_revenue_1 = 0
            flown_pax_1 = 0
            flown_revenue_1 = 0
            flown_net_pax = 0
            flown_net_revenue = 0
            market_combined = ""
            if "book_pax" in each_crscr:
                booking_pax_1 = each_crscr['book_pax']['value']
                booking_revenue_1 = each_crscr['book_revenue']['value']
            if "sale_pax" in each_crscr:
                sale_pax_1 = each_crscr['sale_pax']['value']
                sale_revenue_1 = each_crscr['sale_revenue']['value']
            if "market_combined" in each_crscr:
                market_combined = each_crscr['market_combined']
            else:
                market_combined = each_crscr['pos']['City'] + each_crscr['origin']['City'] + "" + each_crscr['destination']['City'] + each_crscr['compartment']['compartment']
            distance = 0
            if 'distance' in each_crscr:
                distance = each_crscr['distance']

            updateBulk.find({
                "trx_date":next_year_trx_date,
                'pos.Network':each_crscr['pos']['Network'],
                'pos.Cluster':each_crscr['pos']['Cluster'],
                'pos.Region':each_crscr['pos']['Region'],
                'pos.Country':each_crscr['pos']['Country'],
                'pos.City':each_crscr['pos']['City'],
                'origin.Network':each_crscr['origin']['Network'],
                'origin.Cluster':each_crscr['origin']['Cluster'],
                'origin.Region':each_crscr['origin']['Region'],
                'origin.Country':each_crscr['origin']['Country'],
                'origin.City':each_crscr['origin']['City'],
                'destination.Network':each_crscr['destination']['Network'],
                'destination.Cluster':each_crscr['destination']['Cluster'],
                'destination.Region':each_crscr['destination']['Region'],
                'destination.Country':each_crscr['destination']['Country'],
                'destination.City':each_crscr['destination']['City'],
                "compartment.compartment":each_crscr['compartment']['compartment'],
                "dep_date": next_year_dep_date,
                "dep_date_ISO" : next_year_ISO_dep_date
            }).upsert().update_one({'$set': {
                "dep_date_ISO": next_year_ISO_dep_date,
                "trx_date_ISO": next_year_ISO_trx_date,
                "trx_date": next_year_trx_date,
                "dep_date": next_year_dep_date,
                "dep_month": dep_month,
                "dep_year": dep_year,
                "pos": each_crscr['pos'],
                "od": each_crscr['origin']['City'] + "" + each_crscr['destination']['City'],
                "origin": each_crscr['origin'],
                "destination": each_crscr['destination'],
                "compartment": each_crscr['compartment'],
                "distance": distance,
                "book_pax.value_1": booking_pax_1,
                "book_revenue.value_1": booking_revenue_1,
                "sale_pax.value_1": sale_pax_1,
                "sale_revenue.value_1": sale_revenue_1,
                "market_combined":market_combined
            }})

            if (num % 1000 == 0):
                try:
                    print num
                    program_starts = time.time()
                    result1 = updateBulk.execute();
                    updateBulk = fzDBConn.JUP_DB_Manual_Triggers_Module.initialize_unordered_bulk_op();
                    now = time.time()

                except BulkWriteError as bwe:
                    print(bwe.details)
            num = num + 1
        #             '''
        except KeyError as error:
            print("key error")
            print(error)
        except Exception as error:
            print("error")
            print(error)

    try:
        updateBulk.execute();
        pass
    except:
        pass

    # ----------------------------------- Flown data -------------------------------------------#
    dateformat = '%Y-%m-%d'
    yearformat = '%Y'
    monthformat = '%m'
    lastYearSameDate_ISO = system_date + dt.timedelta(days=-364)
    lastYearSameDate = datetime.strftime(lastYearSameDate_ISO, dateformat)

    todayDate = datetime.strftime(system_date, dateformat)

    lastYearSevenDays_OLD_ISO_ = lastYearSameDate_ISO + dt.timedelta(days=-7)
    # for net pax
    lastYearSevenDays_OLD_ISO = lastYearSameDate_ISO + dt.timedelta(days=60)
    lastYearSevenDays = datetime.strftime(lastYearSevenDays_OLD_ISO, dateformat)
    lastYearSevenDays_ = datetime.strftime(lastYearSevenDays_OLD_ISO_, dateformat)
    # data  = fzDBConn.JUP_DB_Manual_Triggers_Module.find()

    if system_date.weekday() == 4:
        # '''
        # creating temp collection from for last year departure date level of pax and revenue of  last year
        num = 1
        updateBulk = fzDBConn.JUP_DB_Manual_Triggers_Module.initialize_unordered_bulk_op()

        data = fzDBConn.JUP_DB_Manual_Triggers_Module.aggregate([
            {'$match': {
                "pos.City": pos,
                 'dep_date': {'$gte': lastYearSevenDays_, '$lte': lastYearSevenDays},
                 # 'dep_date' : {'$gte' : '2021-01-01', '$lte':'2023-10-30'},
            }},
            {'$group': {
                '_id': {'dep_date': '$dep_date', 'pos': '$pos.City', 'od': '$od', 'origin': '$origin.City',
                        'destination': '$destination.City', 'compartment': '$compartment.compartment','market_combined':"$market_combined"},
                'pos': {'$first': '$pos'},
                'origin': {'$first': '$origin'},
                'destination': {'$first': '$destination'},
                'compartment': {'$first': '$compartment'},
                'pax': {'$sum': '$flown_pax.value'},
                'revenue': {'$sum': '$flown_revenue.value'},
            }},
            {'$match': {
                '$and': [
                    {'pax': {'$ne': 0}},
                    {'revenue': {'$ne': 0}}
                ]
            }},
            # {'$out': 'Temp_MPF_MT'}
        ], allowDiskUse=True)

        # cursor = fzDBConn.Temp_MPF_MT.find(no_cursor_timeout=True)
        for each in data:
            try:
                dateformat = '%Y-%m-%d'
                ISO_dep_date = datetime.strptime(each['_id']['dep_date'], dateformat)
                next_year_ISO_dep_date = ISO_dep_date + dt.timedelta(days=365)
                next_year_dep_date = next_year_ISO_dep_date.strftime(dateformat)
                dep_month = int(next_year_ISO_dep_date.strftime("%m"))
                dep_year = int(next_year_ISO_dep_date.strftime("%Y"))
                if "market_combined" in each:
                    market_combined = each['market_combined']
                else:
                    market_combined = each['pos']['City'] + each['origin']['City'] + "" + each['destination']['City'] + each['compartment']['compartment']
                # print(next_year_ISO_dep_date,next_year_dep_date,ISO_dep_date)
                updateBulk.find({
                    "pos.City": each['pos']['City'],
                    "origin.City": each['origin']['City'],
                    "destination.City": each['destination']['City'],
                    "compartment.compartment": each['compartment']['compartment'],
                    "dep_date": next_year_dep_date,
                    "dep_date_ISO": next_year_ISO_dep_date
                }).upsert().update({'$set': {
                    "dep_date_ISO": next_year_ISO_dep_date,
                    "dep_date": next_year_dep_date,
                    "dep_month": dep_month,
                    "dep_year": dep_year,
                    "pos": each['pos'],
                    "od": each['origin']['City'] + "" + each['destination']['City'],
                    "origin": each['origin'],
                    "destination": each['destination'],
                    "compartment": each['compartment'],
                    "flown_pax.value_1": each['pax'],
                    "flown_revenue.value_1": each['revenue'],
                    "market_combined": market_combined,
                }})
                if (num % 1000 == 0):
                    try:
                        print num
                        program_starts = time.time()
                        result1 = updateBulk.execute();
                        updateBulk = fzDBConn.JUP_DB_Manual_Triggers_Module.initialize_unordered_bulk_op();
                        now = time.time()
                    except BulkWriteError as bwe:
                        print(bwe.details)
                num = num + 1
            except Exception as error:
                print("error")
                print(error)
        try:
            updateBulk.execute();
            pass
        except:
            pass
    else:
        # '''
        # creating temp collection from for last year departure date level of pax and revenue of  last year
        num = 1
        updateBulk = fzDBConn.JUP_DB_Manual_Triggers_Module.initialize_unordered_bulk_op()

        data = fzDBConn.JUP_DB_Manual_Triggers_Module.aggregate([
            {'$match': {
                "pos.City": pos,
                 # 'dep_date': {'$gte': lastYearSevenDays_, '$lte': lastYearSameDate},
                'dep_date': {'$gte': '2021-01-01', '$lte': '2023-12-31'}
            }},
            {'$group': {
                '_id': {'dep_date': '$dep_date', 'pos': '$pos.City', 'od': '$od', 'origin': '$origin.City',
                        'destination': '$destination.City', 'compartment': '$compartment.compartment','market_combined':'$market_combined'},
                'pos': {'$first': '$pos'},
                'origin': {'$first': '$origin'},
                'destination': {'$first': '$destination'},
                'compartment': {'$first': '$compartment'},
                'pax': {'$sum': '$flown_pax.value'},
                'revenue': {'$sum': '$flown_revenue.value'},
            }},
            {'$match': {
                '$and': [
                    {'pax': {'$ne': 0}},
                    {'revenue': {'$ne': 0}}
                ]
            }},
            # {'$out': 'Temp_MPF_MT'}
        ], allowDiskUse=True)

        # cursor = fzDBConn.Temp_MPF_MT.find(no_cursor_timeout=True)
        for each in data:
            try:
                # print(" F " + str(num))
                dateformat = '%Y-%m-%d'
                ISO_dep_date = datetime.strptime(each['_id']['dep_date'], dateformat)
                next_year_ISO_dep_date = ISO_dep_date + dt.timedelta(days=365)
                next_year_dep_date = next_year_ISO_dep_date.strftime(dateformat)
                dep_month = int(next_year_ISO_dep_date.strftime("%m"))
                dep_year = int(next_year_ISO_dep_date.strftime("%Y"))
                if "market_combined" in each:
                    market_combined = each['market_combined']
                else:
                    market_combined = each['pos']['City'] + each['origin']['City'] + "" + each['destination']['City'] + each['compartment']['compartment']
                # print(next_year_ISO_dep_date,next_year_dep_date,ISO_dep_date)
                updateBulk.find({
                    "pos.City": each['pos']['City'],
                    "origin.City": each['origin']['City'],
                    "destination.City": each['destination']['City'],
                    "compartment.compartment": each['compartment']['compartment'],
                    "dep_date": next_year_dep_date,
                    "dep_date_ISO": next_year_ISO_dep_date

                }).upsert().update({'$set': {
                    "dep_date_ISO": next_year_ISO_dep_date,
                    "dep_date": next_year_dep_date,
                    "dep_month": dep_month,
                    "dep_year": dep_year,
                    "pos": each['pos'],
                    "od": each['origin']['City'] + "" + each['destination']['City'],
                    "origin": each['origin'],
                    "destination": each['destination'],
                    "compartment": each['compartment'],
                    "flown_pax.value_1": each['pax'],
                    "flown_revenue.value_1": each['revenue'],
                    "market_combined": market_combined,
                }})
                if (num % 1000 == 0):
                    try:
                        print num
                        program_starts = time.time()
                        result1 = updateBulk.execute();
                        updateBulk = fzDBConn.JUP_DB_Manual_Triggers_Module.initialize_unordered_bulk_op();
                        now = time.time()
                    except BulkWriteError as bwe:
                        print(bwe.details)
                num = num + 1
            except Exception as error:
                print("error")
                print(error)
        try:
            updateBulk.execute();
            pass
        except:
            pass


if __name__=='__main__':
    fzDBConn = client[JUPITER_DB]
    # pos_list = list(fzDBConn.JUP_DB_Manual_Triggers_Module.distinct('pos.City',{'pos':{'$ne' : None}, 'trx_date':"2018-04-18"}))
    pos_list = list(fzDBConn.JUP_DB_Manual_Triggers_Module.distinct('pos.City'))
    print pos_list
    for pos in pos_list:
        main(pos, client)
