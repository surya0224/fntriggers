#   COMPETITOR_CONFIG
import pandas as pd
from jupiter_AI import client, JUPITER_DB
db = client[JUPITER_DB]

competitor_config_crsr = list(db.JUP_DB_Competitor_Config.aggregate([
    {
        '$project':
            {
                '_id': 0,
                'pos_level': '$pos.level',
                'pos_value': '$pos.value',
                'origin_level': '$origin.level',
                'origin_value': '$origin.value',
                'destination_value': '$destination.value',
                'destination_level': '$destination.level',
                'compartment_level': '$compartment.level',
                'compartment_value': '$compartment.value',
                'competitors': '$competitors'
            }
    }

])
)

competitor_config_df = pd.DataFrame(competitor_config_crsr)
competitor_config_df['pos'] = competitor_config_df['pos_level'] + competitor_config_df['pos_value']
competitor_config_df['origin'] = competitor_config_df['origin_level'] + competitor_config_df['origin_value']
competitor_config_df['destination'] = competitor_config_df['destination_level'] + competitor_config_df[
    'destination_value']
competitor_config_df['compartment'] = competitor_config_df['compartment_level'] + competitor_config_df[
    'compartment_value']
competitor_config_df.drop(
    ['pos_level', 'pos_value', 'origin_level', 'origin_value', 'destination_level', 'destination_value',
     'compartment_level', 'compartment_value'], axis=1, inplace=True)

total_df = pd.DataFrame(columns=['posodc', 'City'])

country_crsr = list(db.JUP_DB_Region_Master.aggregate([
    {
        '$group':
            {
                '_id': {
                    'country_value': '$COUNTRY_CD'
                },
                'City': {'$addToSet': '$POS_CD'}

            }
    },
    {
        '$project':
            {
                '_id': 0,
                'country_value': '$_id.country_value',
                'City': '$City'
            }
    }

])
)

country_df = pd.DataFrame(country_crsr)
country_df['posodc'] = country_df.apply(lambda row: 'Country' + row['country_value'], axis=1)
country_df.drop(['country_value'], axis=1, inplace=True)
total_df = pd.concat([total_df, country_df])

region_crsr = list(db.JUP_DB_Region_Master.aggregate([
    {
        '$group':
            {
                '_id': {
                    'region_value': '$Region'
                },
                'City': {'$addToSet': '$POS_CD'}

            }
    },
    {
        '$project':
            {
                '_id': 0,
                'region_value': '$_id.region_value',
                'City': '$City'
            }
    }

])
)
region_df = pd.DataFrame(region_crsr)
region_df['posodc'] = region_df.apply(lambda row: 'Region' + row['region_value'], axis=1)
region_df.drop(['region_value'], axis=1, inplace=True)
total_df = pd.concat([total_df, region_df])

cluster_crsr = list(db.JUP_DB_Region_Master.aggregate([
    {
        '$group':
            {
                '_id': {
                    'cluster_value': '$Cluster'
                },
                'City': {'$addToSet': '$POS_CD'}

            }
    },
    {
        '$project':
            {
                '_id': 0,
                'cluster_value': '$_id.cluster_value',
                'City': '$City'
            }
    }

])
)
cluster_df = pd.DataFrame(cluster_crsr)
cluster_df['posodc'] = cluster_df.apply(lambda row: 'Cluster' + row['cluster_value'], axis=1)
cluster_df.drop(['cluster_value'], axis=1, inplace=True)
total_df = pd.concat([total_df, cluster_df])

network_crsr = list(db.JUP_DB_Region_Master.aggregate([
    {
        '$group':
            {
                '_id': {
                    'network_value': '$Network'
                },
                'City': {'$addToSet': '$POS_CD'}

            }
    },
    {
        '$project':
            {
                '_id': 0,
                'network_value': '$_id.network_value',
                'City': '$City'
            }
    }

])
)

network_df = pd.DataFrame(network_crsr)
network_df['posodc'] = network_df.apply(lambda row: 'Network' + row['network_value'], axis=1)
network_df.drop(['network_value'], axis=1, inplace=True)
total_df = pd.concat([total_df, network_df])

compartment_df = pd.DataFrame([{'compartment_value': 'Y', 'City': ['Y']}, {'compartment_value': 'J', 'City': ['J']},
                               {'compartment_value': 'all', 'City': ['Y', 'J']},
                               {'compartment_value': 'All', 'City': ['Y', 'J']}])
compartment_df['posodc'] = compartment_df.apply(lambda row: 'Compartment' + row['compartment_value'], axis=1)
compartment_df.drop(['compartment_value'], axis=1, inplace=True)
total_df = pd.concat([total_df, compartment_df])

crsr_cities = db.JUP_DB_City_Airport_Mapping.find({}, {'Airport_Code': 1, '_id': 0})
crsr_cities2 = db.JUP_DB_City_Airport_Mapping.find({}, {'City_Code': 1, '_id': 0})
cities_df = pd.DataFrame(list(crsr_cities))
cities_df2 = pd.DataFrame(list(crsr_cities2))
cities_df.rename(columns={'Airport_Code': 'posod'}, inplace=True)
cities_df2.rename(columns={'City_Code': 'posod'}, inplace=True)
cities_df_all = pd.concat([cities_df, cities_df2])
cities_df_all.drop_duplicates(inplace=True)
cities_df_all['City'] = cities_df_all.apply(lambda row: [row['posod']], axis=1)
cities_df_all['posodc'] = cities_df_all.apply(lambda row: 'City' + row['posod'], axis=1)
cities_df_all.drop(['posod'], axis=1, inplace=True)
cities_df_all['City'] = cities_df_all.apply(lambda row: [row['City']], axis=1)
total_df = pd.concat([total_df, cities_df_all])

total_df.reset_index(drop=True, inplace=True)

competitor_config_df = pd.merge(competitor_config_df, total_df, how='left', left_on='pos', right_on='posodc')
competitor_config_df['pos'] = competitor_config_df['City']
competitor_config_df.drop(['City', 'posodc'], inplace=True, axis=1)
competitor_config_df = pd.merge(competitor_config_df, total_df, how='left', left_on='origin', right_on='posodc')
competitor_config_df['origin'] = competitor_config_df['City']
competitor_config_df.drop(['City', 'posodc'], inplace=True, axis=1)
competitor_config_df = pd.merge(competitor_config_df, total_df, how='left', left_on='destination', right_on='posodc')
competitor_config_df['destination'] = competitor_config_df['City']
competitor_config_df.drop(['City', 'posodc'], inplace=True, axis=1)
competitor_config_df = pd.merge(competitor_config_df, total_df, how='left', left_on='compartment', right_on='posodc')
competitor_config_df['compartment'] = competitor_config_df['City']
competitor_config_df.drop(['City', 'posodc'], inplace=True, axis=1)

temp_collection = 'temp_collection_nikunj_competitors'
db[temp_collection].insert(competitor_config_df.to_dict('records'))
competitor_crsr = list(db[temp_collection].aggregate([
    {
        '$unwind': '$pos',
    },
    {
        '$unwind': '$origin',
    },
    {
        '$unwind': '$destination',
    },
    {
        '$unwind': '$compartment',
    },
    {
        '$unwind': '$competitors',
    },
    {
        '$project':
            {
                '_id': 0,
                'pos': '$pos',
                'origin': '$origin',
                'destination': '$destination',
                'compartment': '$compartment',
                'competitors': '$competitors',
                'priority': '$priority'
            }
    }
])
)
db[temp_collection].drop()
competitor_df = pd.DataFrame(competitor_crsr)
competitor_df.drop_duplicates(inplace=True)
db[temp_collection].insert(competitor_df.to_dict('records'))
competitor_crsr = list(db[temp_collection].aggregate([
    {
        '$sort': {'priority': -1}
    },
    {
        '$group':
            {
                '_id':
                    {
                        'pos': '$pos',
                        'origin': '$origin',
                        'destination': '$destination',
                        'compartment': '$compartment'
                    },
                'competitors': {'$addToSet': '$competitors'}
            }
    },
    {
        '$project':
            {
                '_id': 0,
                'pos': '$_id.pos',
                'origin': '$_id.origin',
                'destination': '$_id.destination',
                'compartment': '$_id.compartment',
                'competitors': '$competitors'
            }
    }
])
)
db[temp_collection].drop()
competitor_df = pd.DataFrame(competitor_crsr)
