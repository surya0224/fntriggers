"""
Author: Abhinav Garg
Created with <3
Date Created: 2017-08-10
File Name: market_channels.py

Creates channel characteristics from sales and flown data.

"""
from pymongo import MongoClient, UpdateOne
import pandas as pd
import math
import time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from jupiter_AI.triggers.common import cursor_to_df

from jupiter_AI import SYSTEM_DATE, mongo_client, JUPITER_DB, JUPITER_LOGGER
from jupiter_AI.logutils import measure


@measure(JUPITER_LOGGER)
def run_this_year(snap_date, db):
    print "getting sales data"
    cur_sales = db.JUP_DB_Sales.aggregate([
        {'$match': {
            '$or': [{"snap_date": snap_date},
                    {"enrole_update_date": snap_date}
                    ]
        }},
        {
            '$addFields': {"snap_date": snap_date}
        },
        {'$group': {
            '_id': {
                "pos": '$pos',
                "od": '$od',
                "origin": '$origin',
                "destination": '$destination',
                'compartment': '$compartment',
                'dep_date': '$dep_date',
                'snap_date': '$snap_date',
                'fare_basis': '$fare_basis',
                'channel': '$channel'

            },
            'pax': {'$sum': {'$cond': [{'$eq': ['$enrole_update_date', snap_date]}, {
                '$multiply': [{'$cond': [{'$eq': ['$segment_status', 'CANCELED']}, 0, '$pax']}, -1]},
                                       {'$cond': [{'$eq': ['$segment_status', 'CANCELED']}, 0, '$pax']}]}},
            'pax_1': {'$sum': {'$cond': [{'$eq': ['$enrole_update_date', snap_date]}, {
                '$multiply': [{'$cond': [{'$eq': ['$segment_status', 'CANCELED']}, 0, '$pax_1']}, -1]},
                                         {'$cond': [{'$eq': ['$segment_status', 'CANCELED']}, 0, '$pax_1']}]}},
            'revenue': {'$sum': {
                '$cond': [{'$eq': ['$enrole_update_date', snap_date]}, {'$multiply': ['$revenue', -1]}, '$revenue']}},
            'revenue_1': {'$sum': {
                '$cond': [{'$eq': ['$enrole_update_date', snap_date]}, {'$multiply': ['$revenue_1', -1]},
                          '$revenue_1']}},
            'snap_pax': {'$sum': {'$cond': [{'$eq': ['$enrole_update_date', snap_date]}, 0,
                                            {'$cond': [{'$eq': ['$segment_status', 'CANCELED']}, 0, '$pax']}]}},
            'snap_revenue': {'$sum': {'$cond': [{'$eq': ['$enrole_update_date', snap_date]}, 0, '$revenue']}},
            'AIR_CHARGE': {'$sum': {
                '$cond': [{'$eq': ["$enrole_update_date", snap_date]}, {'$multiply': ['$AIR_CHARGE', -1]},
                          '$AIR_CHARGE']}},
        }},
        {'$group': {
            '_id': {
                "pos": '$_id.pos',
                "od": '$_id.od',
                "origin": '$_id.origin',
                "destination": '$_id.destination',
                'compartment': '$_id.compartment',
                'dep_date': '$_id.dep_date',
                'snap_date': '$_id.snap_date',
                'channel': '$_id.channel'
            },
            'pax': {'$sum': '$pax'},
            'pax_1': {'$sum': '$pax_1'},
            'revenue': {'$sum': '$revenue'},
            'revenue_1': {'$sum': '$revenue_1'},
            'snap_pax': {'$sum': '$snap_pax'},
            'snap_revenue': {'$sum': '$snap_revenue'},
            'AIR_CHARGE': {'$sum': '$AIR_CHARGE'},
            'sales': {'$push': {
                'fare_basis': '$_id.fare_basis',
                'pax': '$pax',
                'pax_1': '$pax_1',
                'snap_pax': '$snap_pax',
                'snap_revenue': '$snap_revenue',
                'revenue': '$revenue',
                'revenue_1': '$revenue_1',
                'AIR_CHARGE': '$AIR_CHARGE',
            }}
        }},
        {'$project': {
            "pos": '$_id.pos',
            "od": '$_id.od',
            "origin": '$_id.origin',
            "destination": '$_id.destination',
            'compartment': '$_id.compartment',
            'dep_date': '$_id.dep_date',
            'snap_date': '$_id.snap_date',
            'channel': '$_id.channel',
            'sales_pax': '$pax',
            'sales_snap_pax': '$snap_pax',
            'sales_revenue': '$revenue',
            'sales_snap_revenue': '$snap_revenue',
            'sales_pax_1': '$pax_1',
            'sales_revenue_1': '$revenue_1',
            'sales_AIR_CHARGE': '$AIR_CHARGE',
            'sales': '$sales',
            '_id': 0
    }}
    ], allowDiskUse=True)

    df_1 = cursor_to_df(cur_sales)
    # df_1 = pd.DataFrame(list(cur_sales))

    print 'getting flown data'
    cur_flown = db.JUP_DB_Sales_Flown.aggregate([
        # {'$match': {
        #     '$or': [{"snap_date": snap_date},
        #             {"enrole_update_date": snap_date}
        #             ]
        # }},
        {
            '$addFields': {"snap_date": snap_date}
        },
        {'$group': {
            '_id': {
                "pos": '$pos',
                "od": '$od',
                "origin": '$origin',
                "destination": '$destination',
                'compartment': '$compartment',
                'dep_date': '$dep_date',
                'snap_date': '$snap_date',
                'fare_basis': '$fare_basis',
                'channel': '$channel'

            },
            'pax': {'$sum': {'$cond': [{'$eq': ['$enrole_update_date', snap_date]}, {
                '$multiply': [{'$cond': [{'$eq': ['$segment_status', 'CANCELED']}, 0, '$pax']}, -1]},
                                       {'$cond': [{'$eq': ['$segment_status', 'CANCELED']}, 0, '$pax']}]}},
            'pax_1': {'$sum': {'$cond': [{'$eq': ['$enrole_update_date', snap_date]}, {
                '$multiply': [{'$cond': [{'$eq': ['$segment_status', 'CANCELED']}, 0, '$pax_1']}, -1]},
                                         {'$cond': [{'$eq': ['$segment_status', 'CANCELED']}, 0, '$pax_1']}]}},
            'revenue': {'$sum': {
                '$cond': [{'$eq': ['$enrole_update_date', snap_date]}, {'$multiply': ['$revenue', -1]}, '$revenue']}},
            'revenue_1': {'$sum': {
                '$cond': [{'$eq': ['$enrole_update_date', snap_date]}, {'$multiply': ['$revenue_1', -1]},
                          '$revenue_1']}},
            'snap_pax': {'$sum': {'$cond': [{'$eq': ['$enrole_update_date', snap_date]}, 0,
                                            {'$cond': [{'$eq': ['$segment_status', 'CANCELED']}, 0, '$pax']}]}},
            'snap_revenue': {'$sum': {'$cond': [{'$eq': ['$enrole_update_date', snap_date]}, 0, '$revenue']}},
            'AIR_CHARGE': {'$sum': {
                '$cond': [{'$eq': ["$enrole_update_date", snap_date]}, {'$multiply': ['$AIR_CHARGE', -1]},
                          '$AIR_CHARGE']}},
        }},
        {'$group': {
            '_id': {
                "pos": '$_id.pos',
                "od": '$_id.od',
                "origin": '$_id.origin',
                "destination": '$_id.destination',
                'compartment': '$_id.compartment',
                'dep_date': '$_id.dep_date',
                'snap_date': '$_id.snap_date',
                'channel': '$_id.channel'
            },
            'pax': {'$sum': '$pax'},
            'pax_1': {'$sum': '$pax_1'},
            'revenue': {'$sum': '$revenue'},
            'revenue_1': {'$sum': '$revenue_1'},
            'snap_pax': {'$sum': '$snap_pax'},
            'snap_revenue': {'$sum': '$snap_revenue'},
            'AIR_CHARGE': {'$sum': '$AIR_CHARGE'},
            'flown': {'$push': {
                'fare_basis': '$_id.fare_basis',
                'pax': '$pax',
                'pax_1': '$pax_1',
                'snap_pax': '$snap_pax',
                'snap_revenue': '$snap_revenue',
                'revenue': '$revenue',
                'revenue_1': '$revenue_1',
                'AIR_CHARGE': '$AIR_CHARGE',
            }}
        }},
        {'$project': {
            "pos": '$_id.pos',
            "od": '$_id.od',
            "origin": '$_id.origin',
            "destination": '$_id.destination',
            'compartment': '$_id.compartment',
            'dep_date': '$_id.dep_date',
            'snap_date': '$_id.snap_date',
            'channel': '$_id.channel',
            'flown_pax': '$pax',
            'flown_snap_pax': '$snap_pax',
            'flown_revenue': '$revenue',
            'flown_snap_revenue': '$snap_revenue',
            'flown_pax_1': '$pax_1',
            'flown_revenue_1': '$revenue_1',
            'flown_AIR_CHARGE': '$AIR_CHARGE',
            'flown': '$flown',
            '_id': 0
        }}
    ], allowDiskUse=True)

    df_2=cursor_to_df(cur_flown)
    # df_2 = pd.DataFrame(list(cur_flown))

    try:
        print 'Merging'
        temp3 = pd.merge(df_1, df_2, on=['od', 'pos', 'compartment', 'channel', 'origin', 'destination',
                                         'dep_date', 'snap_date'], how='outer')
        temp3.fillna(-999, inplace=True)
        temp3['dep_month'] = temp3['dep_date'].str.slice(5, 7).astype('int')
        temp3['dep_year'] = temp3['dep_date'].str.slice(0, 4).astype('int')
        temp3['snap_month'] = temp3['snap_date'].str.slice(5, 7).astype('int')
        temp3['snap_year'] = temp3['snap_date'].str.slice(0, 4).astype('int')
        channel = temp3.to_dict("records")
        print 'Inserting'
        db.JUP_DB_Market_Characteristics_Channels.insert(channel)
        del temp3
    except KeyError:
        if len(df_1) == 0 and len(df_2) != 0:
            channel = df_2
            channel['dep_month'] = channel['dep_date'].str.slice(5, 7).astype('int')
            channel['dep_year'] = channel['dep_date'].str.slice(0, 4).astype('int')
            channel['snap_month'] = channel['snap_date'].str.slice(5, 7).astype('int')
            channel['snap_year'] = channel['snap_date'].str.slice(0, 4).astype('int')
            channel = channel.to_dict('records')
            print 'Inserting'
            db.JUP_DB_Market_Characteristics_Channels.insert(channel)
        elif len(df_2) == 0 and len(df_1) != 0:
            channel = df_1
            channel['dep_month'] = channel['dep_date'].str.slice(5, 7).astype('int')
            channel['dep_year'] = channel['dep_date'].str.slice(0, 4).astype('int')
            channel['snap_month'] = channel['snap_date'].str.slice(5, 7).astype('int')
            channel['snap_year'] = channel['snap_date'].str.slice(0, 4).astype('int')
            channel = channel.to_dict('records')
            print 'Inserting'
            db.JUP_DB_Market_Characteristics_Channels.insert(channel)
        else:
            channel = []
    del df_1
    del df_2
    del channel
    print 'Inserted channels This Year for snap:', snap_date


@measure(JUPITER_LOGGER)
def run_cap_and_last_year(snap_date, db):
    count = 1
    counti = 1
    cur_market_channel = db.JUP_DB_Market_Characteristics_Channels.find(
        # {"snap_date": {"$eq": snap_date}}
        {}
        , no_cursor_timeout=True)
    for k in cur_market_channel:
        try:
            if k['sales'] == -999:
                del [k['sales'], k['sales_pax'], k['sales_revenue'], k['sales_snap_pax'], k['sales_snap_revenue'],
                     k['sales_pax_1'], k['sales_revenue_1'], k['sales_AIR_CHARGE']]
        except KeyError:
            pass

        try:
            if k['flown'] == -999:
                del [k['flown'], k['flown_pax'], k['flown_revenue'], k['flown_snap_pax'], k['flown_snap_revenue'],
                     k['flown_pax_1'], k['flown_revenue_1'], k['flown_AIR_CHARGE']]
        except KeyError:
            pass

        cur_od_distance = db.JUP_DB_OD_Distance_Master.find({"od": k["od"]})
        for h in cur_od_distance:
            k["od_distance"] = h["distance"]

        cur_capacity = db.JUP_DB_Host_OD_Capacity.find({"dep_date": k["dep_date"], "od": k["od"]})
        for h in cur_capacity:
            if k["compartment"] == "Y":
                k["capacity"] = h["y_cap"]
                if str(h['y_cap_1']) != 'nan':
                    k['capacity_1'] = h['y_cap_1']

            elif k["compartment"] == 'J':
                k["capacity"] = h["j_cap"]
                if str(h['j_cap_1']) != 'nan':
                    k['capacity_1'] = h['j_cap_1']

            elif k["compartment"] == "F":
                k["capacity"] = h["f_cap"]
                if str(h['f_cap_1']) != 'nan':
                    k['capacity_1'] = h['f_cap_1']

        last_year_snap_date = datetime.strftime(datetime.strptime(k['snap_date'], "%Y-%m-%d") -
                                                timedelta(days=364), "%Y-%m-%d")
        last_year_dep_date = datetime.strftime(datetime.strptime(k['dep_date'], "%Y-%m-%d") -
                                               timedelta(days=364), "%Y-%m-%d")
        last_year = db.JUP_DB_Market_Characteristics_Channels.find({"snap_date": last_year_snap_date, "dep_date": last_year_dep_date, "od": k["od"],
                                             "pos": k["pos"], "compartment": k["compartment"],
                                             "channel": k["channel"]})
        for j in last_year:
            a = 0
            b = 0
            try:
                k['sales']
            except KeyError:
                k['sales'] = [{'fare_basis':-999, 'pax':-999, 'pax_1':-999, 'snap_pax':-999, 'revenue':-999, 'revenue_1':-999,
                               'snap_revenue':-999, 'AIR_CHARGE':-999}]
                a = 1
            try:
                sales_k = pd.DataFrame(k['sales'])
                sales_j = pd.DataFrame(j['sales'])
                temp = sales_k.merge(sales_j, on='fare_basis', how='outer')
                temp = temp.rename(columns={"pax_y": "pax_1_x", "pax_1_x": "tp1", "revenue_y": "revenue_1_x",
                                            "revenue_1_x": "tp2"})
                temp[['pax_x']] = temp[['pax_x']].fillna(0)
                temp[['pax_1_x']] = temp[['pax_1_x']].fillna(0)
                temp[['revenue_x']] = temp[['revenue_x']].fillna(0)
                temp[['revenue_1_x']] = temp[['revenue_1_x']].fillna(0)
                temp[['snap_pax_x']] = temp[['snap_pax_x']].fillna(0)
                temp[['snap_revenue_x']] = temp[['snap_revenue_x']].fillna(0)
                temp[['AIR_CHARGE_x']] = temp[['AIR_CHARGE_x']].fillna(0)
                temp = temp.rename(columns={"pax_x":"pax", "pax_1_x":"pax_1", "revenue_x":"revenue", "snap_pax_x":"snap_pax",
                                            "revenue_1_x":"revenue_1", "snap_revenue_x":"snap_revenue",
                                            'AIR_CHARGE_x': 'AIR_CHARGE'})
                temp.drop(['tp1', 'tp2', 'pax_1_y', 'snap_pax_y', 'revenue_1_y', 'snap_revenue_y', 'AIR_CHARGE_y'],
                          axis=1, inplace=True)
                if a == 1:
                    temp.drop([0], axis=0, inplace=True)
                k['sales_pax_1'] = temp['pax_1'].sum()
                k['sales_revenue_1'] = temp['revenue_1'].sum()
                k['sales'] = temp.to_dict('records')
                del temp
            except KeyError:
                b = 1
                pass
            except ValueError:
                b = 1
                pass

            if a == 1 and b == 1:
                del k['sales']

            c = 0
            d = 0
            try:
                k['flown']
            except KeyError:
                k['flown'] = [{'fare_basis':-999, 'pax':-999, 'pax_1':-999, 'snap_pax':-999, 'revenue':-999, 'revenue_1':-999,
                               'snap_revenue':-999, 'AIR_CHARGE':-999}]
                c = 1
            try:
                flown_k = pd.DataFrame(k['flown'])
                flown_j = pd.DataFrame(j['flown'])
                temp = flown_k.merge(flown_j, on='fare_basis', how='outer')
                temp = temp.rename(columns={"pax_y": "pax_1_x", "pax_1_x": "tp1", "revenue_y": "revenue_1_x",
                                            "revenue_1_x": "tp2"})
                temp[['pax_x']] = temp[['pax_x']].fillna(0)
                temp[['pax_1_x']] = temp[['pax_1_x']].fillna(0)
                temp[['revenue_x']] = temp[['revenue_x']].fillna(0)
                temp[['revenue_1_x']] = temp[['revenue_1_x']].fillna(0)
                temp[['snap_pax_x']] = temp[['snap_pax_x']].fillna(0)
                temp[['snap_revenue_x']] = temp[['snap_revenue_x']].fillna(0)
                temp[['AIR_CHARGE_x']] = temp[['AIR_CHARGE_x']].fillna(0)
                temp = temp.rename(columns={"pax_x": "pax", "pax_1_x": "pax_1", "revenue_x": "revenue", "snap_pax_x": "snap_pax",
                                            "revenue_1_x": "revenue_1", "snap_revenue_x": "snap_revenue",
                                            'AIR_CHARGE_x': 'AIR_CHARGE'})
                temp.drop(['tp1', 'tp2', 'pax_1_y', 'snap_pax_y', 'revenue_1_y', 'snap_revenue_y', 'AIR_CHARGE_y'],
                          axis=1, inplace=True)
                if c == 1:
                    temp.drop([0], axis=0, inplace=True)
                k['flown_pax_1'] = temp['pax_1'].sum()
                k['flown_revenue_1'] = temp['revenue_1'].sum()
                k['flown'] = temp.to_dict('records')
                del temp
            except KeyError:
                d = 1
                pass
            except ValueError:
                d = 1
                pass

            if c == 1 and d == 1:
                del k['flown']

            print count, "      ",k['_id']
            count += 1
        db.JUP_DB_Market_Characteristics_Channels.update({"_id": k["_id"]}, k)
        if counti % 1000 == 0:
            print 'Updated:', counti
        counti += 1
    cur_market_channel.close()


@measure(JUPITER_LOGGER)
def run_market_channels(client):
    db = client[JUPITER_DB]
    snap_date = SYSTEM_DATE
    st = time.time()
    print 'Removing present snaps in case of re run'
    db.JUP_DB_Market_Characteristics_Channels.remove({'snap_date': snap_date})
    print 'Removed in ', time.time() - st
    print 'Running channels This Year for snap:', snap_date
    run_this_year(snap_date=snap_date, db=db)
    print 'Time taken for this year channels', time.time() - st
    print 'Runing channels Last Year, Capacity, Target, Forecast'
    run_cap_and_last_year(snap_date=snap_date, db=db)
    print 'Ran last year, capacity, target, forecast for snap:', snap_date
    print 'Total Time Taken channels', time.time() - st


@measure(JUPITER_LOGGER)
def market_channels_adhoc(snap_date):
    st = time.time()
    print 'Running channels This Year for snap:', snap_date
    run_this_year(snap_date=snap_date)
    print 'Time taken for this year channels', time.time() - st
    print 'Runing channels Last Year, Capacity, Target, Forecast'
    run_cap_and_last_year(snap_date=snap_date)
    print 'Ran last year, capacity, target, forecast for snap:', snap_date
    print 'Total Time Taken channels', time.time() - st


if __name__ == "__main__":
    client = mongo_client()
    run_market_channels(client)
    client.close()