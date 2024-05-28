from jupiter_AI import mongo_client, client, JUPITER_DB, Host_Airline_Code, JUPITER_LOGGER
from jupiter_AI.logutils import measure
from jupiter_AI.network_level_params import SYSTEM_DATE, today
from jupiter_AI.triggers.recommendation_models.oligopoly_fl_ge import oligopoly_recommendation, web_pricing_caller
from copy import deepcopy
import pandas as pd

client_2 = mongo_client()
db_2 = client_2[JUPITER_DB]
HOST_AIRLINE_CODE = Host_Airline_Code
exchange_rate = {}
currency_crsr = list(db_2.JUP_DB_Exchange_Rate.find({}))
for curr in currency_crsr:
    exchange_rate[curr['code']] = curr['Reference_Rate']
print client_2.close()

@measure(JUPITER_LOGGER)
def get_pricing_models(pos, origin, destination, compartment, db):
    pos_cursor = db.JUP_DB_Region_Master.aggregate([
        {
            '$match':
                {
                    'POS_CD': pos
                }
        },
        {'$project':
            {
                '_id': 0,
                'city': '$POS_CD',
                'country': '$COUNTRY_CD',
                'region': '$Region',
                'cluster': '$Cluster',
                'network': '$Network'
            }
        }
    ])
    # print list(pos_cursor)

    pos_list = (list(pos_cursor)[0]).values()

    origin_cursor = db.JUP_DB_Region_Master.aggregate([
        {
            '$match':
                {
                    'POS_CD': origin
                }
        },
        {'$project':
            {
                '_id': 0,
                'city': '$POS_CD',
                'country': '$COUNTRY_CD',
                'region': '$Region',
                'cluster': '$Cluster',
                'network': '$Network'
            }
        }
    ])
    origin_list = (list(origin_cursor)[0]).values()
    destination_cursor = db.JUP_DB_Region_Master.aggregate([
        {
            '$match':
                {
                    'POS_CD': destination
                }
        },
        {'$project':
            {
                '_id': 0,
                'city': '$POS_CD',
                'country': '$COUNTRY_CD',
                'region': '$Region',
                'cluster': '$Cluster',
                'network': '$Network'
            }
        }
    ])
    destination_list = (list(destination_cursor)[0]).values()
    compartment_list = [compartment, 'all']
    # print destination_list, compartment_list
    pricing_models_cursor = db.JUP_DB_Pricing_Model_Markets.aggregate([
        {
            '$match':
                {
                    'pos.value': {'$in': pos_list},
                    'origin.value': {'$in': origin_list},
                    'destination.value': {'$in': destination_list},
                    'compartment.value': {'$in': compartment_list}
                }
        },
        {
            '$sort': {'priority': -1}
        },
        {
            '$limit': 1
        },
        {
            "$project":
                {
                    "_id": 0,
                    "model": 1
                }
        }
    ])
    # print list(pricing_models_cursor)
    pricing_models = list(pricing_models_cursor)[0]['model']
    # print pricing_models
    return pricing_models


@measure(JUPITER_LOGGER)
def get_models_definitions(pricing_models, db):
    models_definitions_cursor = db.JUP_DB_Pricing_Model.find({'model_code': {'$in': pricing_models}})
    models_definitions = list(models_definitions_cursor)
    return models_definitions


@measure(JUPITER_LOGGER)
def get_host_fares_filtered(host_fares_filtered, primary_filter):
    # print host_fares_filtered.to_string()
    if primary_filter['oneway_return']:
        host_fares_filtered = host_fares_filtered[
            host_fares_filtered['oneway_return'] == str(primary_filter['oneway_return'])]

    default_filters = dict((k, v) for k, v in primary_filter.iteritems() if v)
    for key, value in default_filters.items():

        if type(value) is list:
            temp_dF = pd.DataFrame()
            temp = []
            for x in value:
                temp_dF = host_fares_filtered[host_fares_filtered[key] == x]
                temp.append(temp_dF)
                temp_dF = pd.DataFrame()
            host_fares_filtered = pd.concat(temp)

        else:
            host_fares_filtered = host_fares_filtered[host_fares_filtered[key] == value]

    host_fares_filtered = host_fares_filtered.sort_values(['fare']).head(1)

    return host_fares_filtered


@measure(JUPITER_LOGGER)
def oligopoly_rec(primary_filter, pos, origin, destination, compartment, dep_date_start, dep_date_end, db,
                  dates_code=None, mpf_df=None, host_fares=None):
    # default_models_definitions = get_models_definitions(['PM00'])[0]
    default_reco_fares = oligopoly_recommendation(pos=pos, origin=origin, destination=destination,
                                                  compartment=compartment, dep_date_start=dep_date_start,
                                                  dep_date_end=dep_date_end, dates_code=dates_code, db=db,
                                                  mpf_df=mpf_df, host_fares=host_fares)

    if primary_filter is not None:
        print 'Getting filtered fares with pre select ..'
        default_dF = pd.DataFrame(default_reco_fares)
        # print default_dF[['fare', 'fare_basis']].shape, '==============='
        temp_default_dF = deepcopy(default_dF)
        default_filters = dict((k, v) for k, v in primary_filter.iteritems() if v)

        for key, value in default_filters.items():

            if type(value) is list:
                temp_dF = pd.DataFrame()
                temp = []
                default_dF = default_dF[default_dF[key].isin(value)]
                # for x in value:
                #     temp_dF = default_dF[default_dF[key] == x]
                #     temp.append(temp_dF)
                #     temp_dF = pd.DataFrame()
                # default_dF = pd.concat(temp)
                # print default_dF.to_string()

            else:
                default_dF = default_dF[default_dF[key] == value]

        default_dF['oligo_pre_select'] = True
        select_default_dF = default_dF[['fare', 'fare_basis', 'oligo_pre_select']]
        select_default_dF.drop_duplicates(inplace=True)
        # print select_default_dF.to_string()
        # print temp_default_dF.shape, select_default_dF.shape
        result = temp_default_dF.merge(select_default_dF, how='left', on=['fare', 'fare_basis'])
        # print result.shape, '------------'
        # print result[result['fare_basis'] == 'URLP8JO4'].to_string()
        result['oligo_pre_select'].fillna(False, inplace=True)
        return result
    else:
        default_reco_fares['oligo_pre_select'] = False
        return default_reco_fares


@measure(JUPITER_LOGGER)
def get_recommendation_for_primary_fare(oligopoly_fare, primary_fare, primary_criteria, pos, origin, destination,
                                        compartment, dep_date_start, dep_date_end, mpf_df=None):
    try:
        if primary_criteria['web_pricing']:
            primary_fare = web_pricing_caller(host_fares=oligopoly_fare, pos=pos, origin=origin,
                                              destination=destination, compartment=compartment,
                                              dep_date_start=dep_date_start, dep_date_end=dep_date_end,
                                              mpf_df=mpf_df)
    except:
        pass

    return primary_fare


@measure(JUPITER_LOGGER)
def main(pos, origin, destination, compartment, dep_date_start, dep_date_end, db, dates_code=None,
         mpf_df=None, host_fares=None):
    pricing_models = get_pricing_models(pos, origin, destination, compartment, db=db)
    models_definitions = get_models_definitions(pricing_models, db=db)
    print 'Got Model definitions ..'

    reco_primary_fare = pd.DataFrame()
    for model in models_definitions:

        model_effectivity = (model['eff_date_from'] < SYSTEM_DATE) and (model['eff_date_to'] > SYSTEM_DATE)

        if model_effectivity:

            try:
                primary_filter = model['primary_criteria']['filter']
            except KeyError:
                primary_filter = None

            # Oligopoly_model will run for each of the market and it will return a dataframe which will have the
            # oligopoly recommendation.
            oligopoly_fare = oligopoly_rec(primary_filter=primary_filter, pos=pos, origin=origin,
                                           destination=destination, compartment=compartment,
                                           dep_date_start=dep_date_start, dep_date_end=dep_date_end, db=db,
                                           dates_code=dates_code, mpf_df=mpf_df, host_fares=host_fares)

            reco_primary_fare = web_pricing_caller(host_fares=oligopoly_fare, pos=pos, origin=origin,
                                                   destination=destination, compartment=compartment,
                                                   dep_date_start=dep_date_start, dep_date_end=dep_date_end, db=db,
                                                   mpf_df=mpf_df)

            # if primary_filter is not None:
            #     primary_fare = get_host_fares_filtered(oligopoly_fare, primary_filter)
            #     reco_primary_fare = get_recommendation_for_primary_fare(oligopoly_fare, primary_fare,
            #                                                             model['primary_criteria'], pos, origin,
            #                                                             destination, compartment, dep_date_start,
            #                                                             dep_date_end)
            # else:
            #     reco_primary_fare = get_recommendation_for_primary_fare(oligopoly_fare,
            #                                                             host_fares.sort_values(['fare']).head(1),
            #                                                             model['primary_criteria'], pos, origin,
            #                                                             destination, compartment, dep_date_start,
            #                                                             dep_date_end)

            # df_dict = reco_primary_fare.to_dict('records')
            # db.Temp_Oligopoly_Model.insert_many(json.loads(json.dumps(df_dict, indent=1, cls=MyEncoder)))
        else:
            reco_primary_fare = oligopoly_rec(primary_filter=None, pos=pos, origin=origin,
                                              destination=destination, compartment=compartment,
                                              dep_date_start=dep_date_start, dep_date_end=dep_date_end,
                                              dates_code=dates_code, mpf_df=mpf_df, host_fares=host_fares,
                                              db=db)

            reco_primary_fare = web_pricing_caller(host_fares=reco_primary_fare, pos=pos, origin=origin,
                                                   destination=destination, compartment=compartment,
                                                   dep_date_start=dep_date_start, dep_date_end=dep_date_end,
                                                   mpf_df=mpf_df, db=db)

            print "Model not Effective"

    # reco_primary_fare.to_csv('fares.csv')
    return reco_primary_fare.to_dict('records')


if __name__ == '__main__':
    pos = 'AMM'
    origin = 'AMM'
    destination = 'DWC'
    compartment = 'Y'
    dep_date_start = '2018-04-20'
    dep_date_end = '2018-05-20'
    fares = main(pos, origin, destination, compartment, dep_date_start, dep_date_end)
    df = pd.DataFrame(fares)
    print df[df['oligo_pre_select'] == True]['RBD'].value_counts()