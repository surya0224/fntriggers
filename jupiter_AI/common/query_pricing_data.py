


def query_host_pricing_data(query=dict()):
    """
    :return:
    """
    #   today = datetime.datetime.strftime(datetime.datetime.today(),'%Y-%m-%d')
    today = '2016-08-15'
    query['circular_period_start_date'] = {'$lte': today},
    query['circular_period_start_date'] = {'$gte': today}
    apipeline = [
        {
            '$match': query
        },
        {
            '$sort': {'effective_time': -1}
        },
        {
            '$group': {
                '_id': {'farebasis': '$farebasis',
                        'airline': '$airline'},
                "airline": {'$first': '$airline'},
                "pos": {'$first': '$pos'},
                "origin": {'$first': '$origin'},
                "destination": {'$first': '$destination'},
                "compartment": {'$first': '$compartment'},
                "farebasis": {'$first': '$farebasis'},
                "segment": {'$first': '$segment'},
                "channel": {'$first': '$channel'},
                "price": {'$first': '$price'},
                "competitor_farebasis": {'$first': '$competitor_farebasis'},
                "currency": {'$first': '$currency'},
                "rating": {'$first': '$rating'},
                "effective_time": {'$first': '$effective_time'},
                "circular_period_start_date": {'$first': '$circular_period_start_date'},
                "circular_period_end_date": {'$first': '$circular_period_end_date'},
                "pricing_action_id": {'$first': '$pricing_action_id'},
                "last_update_date": {'$first': '$last_update_date'},
                "last_update_time": {'$first': '$last_update_time'}
            }
        },
        {
            '$project':
                {
                    "_id": 0,
                    "airline": '$airline',
                    "pos": '$pos',
                    "origin": '$origin',
                    "destination": '$destination',
                    "compartment": '$compartment',
                    "farebasis": '$farebasis',
                    "segment": '$segment',
                    "channel": '$channel',
                    "price": '$price',
                    "currency": '$currency',
                    "rating": '$rating',
                    "competitor_farebasis": "$competitor_farebasis",
                    "effective_time": '$effective_time',
                    "circular_period_start_date": '$circular_period_start_date',
                    "circular_period_end_date": '$circular_period_end_date',
                    "pricing_action_id": '$pricing_action_id',
                    "last_update_date": '$last_update_date',
                    "last_update_time": '$last_update_time'
                }
        }
    ]
    host_cursor = db.JUP_DB_Host_Pricing_Data.aggregate(apipeline)
    host_data = list(host_cursor)
    return host_data


def query_competitor_pricing_data(query={}):
    #   today = datetime.datetime.strftime(datetime.datetime.today(),'%Y-%m-%d')
    today = '2016-08-15'
    query['circular_period_start_date'] = {"$lte": today},
    query['circular_period_start_date'] = {'$gte': today}
    apipeline = [
        {
            '$match': query
        },
        {
            '$sort': {'effective_time': -1}
        },
        {
            '$group': {
                '_id': {'farebasis': '$farebasis',
                        'airline': '$airline'},
                "airline": {'$first': '$airline'},
                "pos": {'$first': '$pos'},
                "origin": {'$first': '$origin'},
                "destination": {'$first': '$destination'},
                "compartment": {'$first': '$compartment'},
                "farebasis": {'$first': '$farebasis'},
                "segment": {'$first': '$segment'},
                "channel": {'$first': '$channel'},
                "price": {'$first': '$price'},
                "currency": {'$first': '$currency'},
                "rating": {'$first': '$rating'},
                "effective_time": {'$first': '$effective_time'},
                "circular_period_start_date": {'$first': '$circular_period_start_date'},
                "circular_period_end_date": {'$first': '$circular_period_end_date'},
                "pricing_action_id": {'$first': '$pricing_action_id'},
                "last_update_date": {'$first': '$last_update_date'},
                "last_update_time": {'$first': '$last_update_time'}
            }
        },
        {
            '$project':
                {
                    "_id": 0,
                    "airline": '$airline',
                    "pos": '$pos',
                    "origin": '$origin',
                    "destination": '$destination',
                    "compartment": '$compartment',
                    "farebasis": '$farebasis',
                    "segment": '$segment',
                    "channel": '$channel',
                    "price": '$price',
                    "currency": '$currency',
                    "rating": '$rating',
                    "effective_time": '$effective_time',
                    "circular_period_start_date": '$circular_period_start_date',
                    "circular_period_end_date": '$circular_period_end_date',
                    "pricing_action_id": '$pricing_action_id',
                    "last_update_date": '$last_update_date',
                    "last_update_time": '$last_update_time'
                }
        }
    ]
    competitor_cursor = db.JUP_DB_Competitor_Pricing_Data.aggregate(apipeline)
    competitor_data = list(competitor_cursor)
    return competitor_data
