from jupiter_AI import client,  Host_Airline_Code, Host_Airline_Hub, JUPITER_DB, JUPITER_LOGGER
from jupiter_AI.logutils import measure
import pandas as pd
db = client[JUPITER_DB]


@measure(JUPITER_LOGGER)
def get_pax_flown_data():
    ppln = [
        {
            '$addFields':
                {
                    'quarter':
                        {
                            '$cond':
                                {
                                    'if':
                                        {
                                            '$or':
                                                [
                                                    {'$eq': ['$dep_month', 3]},
                                                    {'$eq': ['$dep_month', 2]},
                                                    {'$eq': ['$dep_month', 1]}
                                                ]
                                        },
                                    'then': 'Q1',
                                    'else':
                                        {
                                            '$cond':
                                                {
                                                    'if':
                                                        {
                                                            '$or':
                                                                [
                                                                    {'$eq': ['$dep_month', 4]},
                                                                    {'$eq': ['$dep_month', 5]},
                                                                    {'$eq': ['$dep_month', 6]}
                                                                ]
                                                        },
                                                    'then': 'Q2',
                                                    'else':
                                                        {
                                                            '$cond':
                                                                {
                                                                    'if':
                                                                        {
                                                                            '$or':
                                                                                [
                                                                                    {'$eq': ['$dep_month', 7]},
                                                                                    {'$eq': ['$dep_month', 8]},
                                                                                    {'$eq': ['$dep_month', 9]}
                                                                                ]
                                                                        },
                                                                    'then': 'Q3',
                                                                    'else': 'Q4'
                                                                }
                                                        }
                                                }
                                        }
                                }
                        }
                }
        },
        {
            "$project":{
                "od":1,
                "quarter":1,
                "flown_pax": "$flown_pax.value",
                "dep_month":1,
                "dep_year":1
            }
        },
        {
            "$group":{
                "_id":{
                    "od": "$od",
                    "quarter": "$quarter",
                    "dep_year": "$dep_year"
                },
                "total_flown_pax": {
                    "$sum": "$flown_pax"
                }
            }
        },
        {
            "$project":{
                "_id": 0,
                "od": "$_id.od",
                "quarter": "$_id.quarter",
                "year": "$_id.dep_year",
                "flown_pax": "$total_flown_pax"
            }
        }
    ]

    pax_flown_crsr = db.JUP_DB_Manual_Triggers_Module.aggregate(ppln)

    pax_flown_df = pd.DataFrame(list(pax_flown_crsr))

    return pax_flown_df


@measure(JUPITER_LOGGER)
def get_capacity_data():

    ppln = [
        {
            "$group":{
                "_id":{
                    "od": 1,
                    "quarter": 1,
                    "year": 1
                },
                "leg1_capacity": {"$sum": "$leg1_capacity"},
                "leg2_capacity": {"$sum": "$leg2_capacity"},
                "j_cap": {"$sum": "$j_cap"},
                "y_cap": {"$sum": "$y_cap"}
            }
        },{
            "$project":{
                "od": "$_id.od",
                "quarter": "$_id.quarter",
                "year": "$_id.year",
                "leg1_capacity": 1,
                "leg2_capacity": 1,
                "j_cap": 1,
                "y_cap": 1,
                "_id": 0
            }
        }
    ]

    capacity_crsr = db.JUP_DB_Host_OD_Capacity.aggregate(ppln)

    capacity_df = pd.DataFrame(list(capacity_crsr))

    return capacity_df


@measure(JUPITER_LOGGER)
def chunker(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))


@measure(JUPITER_LOGGER)
def get_derived_factors():
    counter = 0
    pax_flown_df = get_pax_flown_data()

    capacity_df = get_capacity_data()

    pax_flown_df = pax_flown_df.merge(capacity_df, on=['od', 'quarter', 'year'], how='left')

    pax_flown_df['min_cap'] = pax_flown_df[['leg1_capacity', 'leg2_capacity']].min(axis=1)

    pax_flown_df['derived_factor'] = pax_flown_df['flown_pax']/pax_flown_df['min_cap']

    pax_flown_df.loc[pax_flown_df['od'].str.contains(Host_Airline_Hub),'derived_factor'] = 1

    pax_flown_df['airline'] = "FZ"

    for chunk in chunker(pax_flown_df, 10000):
        counter += len(chunk)
        records_to_insert = chunk.to_dict("records")
        print "Done/total: " + str(counter) + "/" + str(len(pax_flown_df))
        db.JUP_DB_Derived_Factor_New.insert_many(records_to_insert)


