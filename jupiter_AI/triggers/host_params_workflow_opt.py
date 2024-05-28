from jupiter_AI import client, JUPITER_DB, SYSTEM_DATE, today, Host_Airline_Hub as hub, JUPITER_LOGGER
from jupiter_AI.BaseParametersCodes.common import get_ly_val
from jupiter_AI import mongo_client
# from jupiter_AI.triggers.common import get_no_days
import json
from calendar import monthrange
from collections import defaultdict
import datetime
import math

from jupiter_AI.logutils import measure

#db = client[JUPITER_DB]


@measure(JUPITER_LOGGER)
def get_od_capacity(origin, destination, compartment, dep_date_start, dep_date_end, db):
    """
    :return: {'ty': This year capacity value for this market and dep dates between dep_date_start and dep_date_end,
              'ly': Last year capacity value for this market and dep dates between dep_date_start - 1 yr and dep_date_end - 1yr
              }
    """
    host_od_capacity = list(db.JUP_DB_Host_OD_Capacity.aggregate(
        [
            {
                '$match': {
                    'od': origin + destination
                }
            }
            ,
            {
                '$facet': {
                    'ty':
                    # Pipeline
                        [
                            # Stage 1
                            {
                                '$match': {
                                    'dep_date': {'$gte': dep_date_start, '$lte': dep_date_end}
                                }
                            },

                            # Stage 2
                            {
                                '$sort': {
                                    'last_update_date': -1
                                }
                            },

                            # Stage 3
                            {
                                '$group': {
                                    '_id': {
                                        'od': '$od',
                                        'dep_date': '$dep_date'
                                    },
                                    'y_capacity': {'$first': '$y_cap'},
                                    'j_capacity': {'$first': '$j_cap'}
                                }
                            },

                            # Stage 4
                            {
                                '$group': {
                                    '_id': None,
                                    'y_capacity': {'$sum': '$y_capacity'},
                                    'j_capacity': {'$sum': '$j_capacity'}
                                }
                            },

                            # Stage 5
                            {
                                '$project': {
                                    'y_capacity': '$y_capacity',
                                    'j_capacity': '$j_capacity',
                                    'capacity': {'$sum': ['$y_capacity', '$j_capacity']}
                                }
                            },

                        ],
                    'ly':
                    # Pipeline
                        [
                            # Stage 1
                            {
                                '$match': {
                                    'dep_date': {'$gte': dep_date_start, '$lte': dep_date_end}
                                }
                            },

                            # Stage 2
                            {
                                '$sort': {
                                    'last_update_date': -1
                                }
                            },

                            # Stage 3
                            {
                                '$group': {
                                    '_id': {
                                        'od': '$od',
                                        'dep_date': '$dep_date'
                                    },
                                    'y_capacity': {'$first': '$y_cap'},
                                    'j_capacity': {'$first': '$j_cap'}
                                }
                            },

                            # Stage 4
                            {
                                '$group': {
                                    '_id': None,
                                    'y_capacity': {'$sum': '$y_capacity'},
                                    'j_capacity': {'$sum': '$j_capacity'}
                                }
                            },

                            # Stage 5
                            {
                                '$project': {
                                    'y_capacity': '$y_capacity',
                                    'j_capacity': '$j_capacity',
                                    'capacity': {'$sum': ['$y_capacity', '$j_capacity']}
                                }
                            },

                        ]
                }
            }

        ]
        # Created with Studio 3T, the IDE for MongoDB - https:#studio3t.com/
    ))
    response = dict(
        ty='NA',
        ly='NA'
    )

    if len(host_od_capacity) == 1:
        ty = host_od_capacity[0]['ty']
        ly = host_od_capacity[0]['ly']
        if len(ty) == 1:
            if compartment:
                if compartment == 'Y':
                    response['ty'] = ty[0]['y_capacity']
                elif compartment == 'J':
                    response['ty'] = ty[0]['j_capacity']
            else:
                response['ty'] = ty[0]['capacity']

        if len(ly) == 1:
            if compartment:
                if compartment == 'Y':
                    response['ly'] = ly[0]['y_capacity']
                elif compartment == 'J':
                    response['ly'] = ly[0]['j_capacity']
            else:
                response['ly'] = ly[0]['capacity']

    return response


@measure(JUPITER_LOGGER)
def get_no_of_days(month, year):
    """
    for a month and year returns the number of days in it
    :param month:
    :param year:
    :return:
    """
    return monthrange(year=year,
                      month=month)[1]


@measure(JUPITER_LOGGER)
def get_od_distance(od, db):
    crsr = db.JUP_DB_OD_Distance_Master.find({'od':od})
    if crsr.count() == 1:
        distance = crsr[0]['distance']
        if type(distance) not in [float, int]:
            distance = -1
    else:
        distance = -1
    return distance


@measure(JUPITER_LOGGER)
def get_data_between_case(pos, origin, destination, compartment, dep_date_start, dep_date_end, db):
    """
    """
    od_distance = get_od_distance(od=origin+destination, db=db)

    query = dict()
    if pos:
        query['pos.City'] = pos
    if origin:
        query['origin.City'] = origin
    if destination:
        query['destination.City'] = destination
    if compartment:
        query['compartment.compartment'] = compartment

    book_date_end = SYSTEM_DATE
    book_date_start = str(today.year) + '-01-01'

    SYSTEM_DATE_LY = get_ly_val(SYSTEM_DATE)

    book_date_start_ly = get_ly_val(book_date_start)
    book_date_end_ly = get_ly_val(book_date_end)

    dep_date_start_ly = get_ly_val(dep_date_start)
    dep_date_end_ly = get_ly_val(dep_date_end)

    db.JUP_DB_Manual_Triggers_Module.aggregate(
        # Pipeline
        [
            # Stage 1
            {
                '$match': query
            },

            # Stage 2
            {
                '$facet': {
                    'ty_past': [
                        {
                            '$match':
                                {
                                    'dep_date':{'$gte':dep_date_start,'$lte': SYSTEM_DATE},
                                    'trx_date':{'$lte': book_date_end, '$gte': book_date_start}
                                }
                        }
                        ,
                        {
                            '$group':
                                {
                                    '_id':{
                                        'month':'$dep_month',
                                        'year':'$dep_year'
                                    },
                                    'sales_pax':{'$sum':'$sale_pax.value'},
                                    'flown_pax':{'$sum':'$flown_pax.value'},
                                    'sales_revenue':{'$sum':'$sale_revenue.value'},
                                    'flown_revenue':{'$sum':'$flown_revenue.value'},
                                    'forecast': {'$addToSet': '$forecast'},
                                    'target': {'$addToSet': '$target'}
                                }
                        }
                        ,
                        {
                            '$addFields': {
                                'forecast': {
                                    '$cond':
                                        {
                                            'if': {'$gt': [{'$size': '$forecast'}, 0]},
                                            'then': {'$arrayElemAt': ['$forecast', 0]},
                                            'else': None
                                        }
                                }
                                ,
                                'target': {
                                    '$cond':
                                        {
                                            'if': {'$gt': [{'$size': '$target'}, 0]},
                                            'then': {'$arrayElemAt': ['$target', 0]},
                                            'else': None
                                        }
                                }
                            }
                        }
                        ,
                        {
                            '$group':
                                {
                                    '_id':None,
                                    'sales_pax':{'$sum':'$sales_pax'},
                                    'flown_pax':{'$sum':'$flown_pax'},
                                    'sales_revenue':{'$sum':'$sales_revenue'},
                                    'flown_revenue':{'$sum':'$flown_revenue'},
                                    'monthly_details':{
                                        '$push':{
                                            'month':'$_id.month',
                                            'year':'$_id.year',
                                            'target':'$target',
                                            'forecast':'$forecast'
                                        }
                                    }
                                }
                        }

                    ],
                    'ty_future': [
                        {
                            '$match':
                                {
                                    'dep_date':{'$gte': SYSTEM_DATE,'$lte': dep_date_end},
                                    'trx_date':{'$lte': book_date_end, '$gte': book_date_start}
                                }
                        }
                        ,
                        {
                            '$group':
                                {
                                    '_id':{
                                        'month':'$dep_month',
                                        'year':'$dep_year'
                                    },
                                    'sales_pax':{'$sum':'$sale_pax.value'},
                                    'flown_pax':{'$sum':'$flown_pax.value'},
                                    'sales_revenue':{'$sum':'$sale_revenue.value'},
                                    'flown_revenue':{'$sum':'$flown_revenue.value'},
                                    'forecast': {'$addToSet': '$forecast'},
                                    'target': {'$addToSet': '$target'}
                                }
                        }
                        ,
                        {
                            '$addFields': {
                                'forecast': {
                                    '$cond':
                                        {
                                            'if': {'$gt': [{'$size': '$forecast'}, 0]},
                                            'then': {'$arrayElemAt': ['$forecast', 0]},
                                            'else': None
                                        }
                                }
                                ,
                                'target': {
                                    '$cond':
                                        {
                                            'if': {'$gt': [{'$size': '$target'}, 0]},
                                            'then': {'$arrayElemAt': ['$target', 0]},
                                            'else': None
                                        }
                                }
                            }
                        }
                        ,
                        {
                            '$group':
                                {
                                    '_id':None,
                                    'sales_pax':{'$sum':'$sales_pax'},
                                    'flown_pax':{'$sum':'$flown_pax'},
                                    'sales_revenue':{'$sum':'$sales_revenue'},
                                    'flown_revenue':{'$sum':'$flown_revenue'},
                                    'monthly_details':{
                                        '$push':{
                                            'month':'$_id.month',
                                            'year':'$_id.year',
                                            'target':'$target',
                                            'forecast':'$forecast'
                                        }
                                    }
                                }
                        }
                    ],
                    'ly_past': [
                        {
                            '$match':
                                {
                                    'dep_date':{'$gte':dep_date_start,'$lte': SYSTEM_DATE},
                                    'trx_date':{'$lte': book_date_end, '$gte': book_date_start}
                                }
                        }
                        ,
                        {
                            '$group':
                                {
                                    '_id':{
                                        'month':'$dep_month',
                                        'year':'$dep_year'
                                    },
                                    'sales_pax':{'$sum':'$sale_pax.value_1'},
                                    'flown_pax':{'$sum':'$flown_pax.value_1'},
                                    'sales_revenue':{'$sum':'$sale_revenue.value_1'},
                                    'flown_revenue':{'$sum':'$flown_revenue.value_1'},
                                    'forecast': {'$addToSet': '$forecast'},
                                    'target': {'$addToSet': '$target'}
                                }
                        }
                        ,
                        {
                            '$addFields': {
                                'forecast': {
                                    '$cond':
                                        {
                                            'if': {'$gt': [{'$size': '$forecast'}, 0]},
                                            'then': {'$arrayElemAt': ['$forecast', 0]},
                                            'else': None
                                        }
                                }
                                ,
                                'target': {
                                    '$cond':
                                        {
                                            'if': {'$gt': [{'$size': '$target'}, 0]},
                                            'then': {'$arrayElemAt': ['$target', 0]},
                                            'else': None
                                        }
                                }
                            }
                        }
                        ,
                        {
                            '$group':
                                {
                                    '_id':None,
                                    'sales_pax':{'$sum':'$sales_pax'},
                                    'flown_pax':{'$sum':'$flown_pax'},
                                    'sales_revenue':{'$sum':'$sales_revenue'},
                                    'flown_revenue':{'$sum':'$flown_revenue'},
                                    'monthly_details':{
                                        '$push':{
                                            'month':'$_id.month',
                                            'year':'$_id.year',
                                            'target':'$target',
                                            'forecast':'$forecast'
                                        }
                                    }
                                }
                        }
                    ],
                    'ly_future': [
                        {
                            '$match':
                                {
                                    'dep_date':{'$gte':SYSTEM_DATE,'$lte': dep_date_end},
                                    'trx_date':{'$lte': book_date_end, '$gte': book_date_start}
                                }
                        }
                        ,
                        {
                            '$group':
                                {
                                    '_id':{
                                        'month':'$dep_month',
                                        'year':'$dep_year'
                                    },
                                    'sales_pax':{'$sum':'$sale_pax.value_1'},
                                    'flown_pax':{'$sum':'$flown_pax.value_1'},
                                    'sales_revenue':{'$sum':'$sale_revenue.value_1'},
                                    'flown_revenue':{'$sum':'$flown_revenue.value_1'},
                                    'forecast': {'$addToSet': '$forecast'},
                                    'target': {'$addToSet': '$target'}
                                }
                        }
                        ,
                        {
                            '$addFields': {
                                'forecast': {
                                    '$cond':
                                        {
                                            'if': {'$gt': [{'$size': '$forecast'}, 0]},
                                            'then': {'$arrayElemAt': ['$forecast', 0]},
                                            'else': None
                                        }
                                }
                                ,
                                'target': {
                                    '$cond':
                                        {
                                            'if': {'$gt': [{'$size': '$target'}, 0]},
                                            'then': {'$arrayElemAt': ['$target', 0]},
                                            'else': None
                                        }
                                }
                            }
                        }
                        ,
                        {
                            '$group':
                                {
                                    '_id':None,
                                    'sales_pax':{'$sum':'$sales_pax'},
                                    'flown_pax':{'$sum':'$flown_pax'},
                                    'sales_revenue':{'$sum':'$sales_revenue'},
                                    'flown_revenue':{'$sum':'$flown_revenue'},
                                    'monthly_details':{
                                        '$push':{
                                            'month':'$_id.month',
                                            'year':'$_id.year',
                                            'target':'$target',
                                            'forecast':'$forecast'
                                        }
                                    }
                                }
                        }
                    ]
                }
            },

            # Stage 3
            {
                '$out': "temp_col_saiki"
            },

        ]
    )

    crsr = db["temp_col_saiki"].find()
    if crsr.count() == 1:
        data = crsr[0]
        # print 'Output from Manual Triggers Col'
        # print data
        ty_past = data['ty_past']
        ly_past = data['ly_past']
        ty_future = data['ty_future']
        ly_future = data['ly_future']

        if len(ty_past) == 1:
            pax_past_ty = ty_past[0]['flown_pax']
            rev_past_ty = ty_past[0]['flown_revenue']
            pax_actual_past = ty_past[0]['flown_pax']
            rev_actual_past = ty_past[0]['flown_revenue']
            monthly_details = ty_past[0]['monthly_details']
            if monthly_details:
                tar_fcst_data = get_total_contribution(monthly_details, dep_date_start, dep_date_end)
                pax_target_past = tar_fcst_data['target']['pax']
                rev_target_past = tar_fcst_data['target']['revenue']
            else:
                pax_target_past = 0
                rev_target_past = 0
        else:
            pax_past_ty = 0
            rev_past_ty = 0
            pax_actual_past = 0
            rev_actual_past = 0
            pax_target_past = 0
            rev_target_past = 0


        if len(ty_future) == 1:
            pax_future_ty = ty_future[0]['sales_pax']
            rev_future_ty = ty_future[0]['sales_revenue']
            monthly_details = ty_future[0]['monthly_details']
            if monthly_details:
                tar_fcst_data = get_total_contribution(monthly_details, dep_date_start, dep_date_end)
                pax_target_future = tar_fcst_data['target']['pax']
                rev_target_future = tar_fcst_data['target']['revenue']
                pax_actual_future = tar_fcst_data['forecast']['pax']
                rev_actual_future = tar_fcst_data['forecast']['revenue']
            else:
                pax_target_future = 0
                rev_target_future = 0
                pax_actual_future = 0
                rev_actual_future = 0
        else:
            pax_future_ty = 0
            rev_future_ty = 0
            pax_target_future = 0
            rev_target_future = 0
            pax_actual_future = 0
            rev_actual_future = 0

        if len(ly_past) == 1:
            pax_past_ly = ly_past[0]['flown_pax']
            rev_past_ly = ly_past[0]['flown_revenue']
        else:
            pax_past_ly = 0
            rev_past_ly = 0

        if len(ly_future) == 1:
            pax_future_ly = ly_future[0]['sales_pax']
            rev_future_ly = ly_future[0]['sales_revenue']
        else:
            pax_future_ly = 0
            rev_future_ly = 0

        pax = pax_past_ty + pax_future_ty
        pax_ly = pax_past_ly + pax_future_ly
        rev = rev_past_ty + rev_future_ty
        rev_ly = rev_past_ly + rev_future_ly
        pax_actual = pax_actual_past + pax_actual_future
        pax_target = pax_target_past + pax_target_future
        rev_actual = rev_actual_past + rev_actual_future
        rev_target = rev_target_past + rev_target_future

        cap_data = get_od_capacity(origin, destination, compartment, dep_date_start, dep_date_end, db=db)

        return dict(
            pax = dict(
                ty=pax,
                ly=pax_ly,
                actual=pax_actual,
                target=pax_target
            ),
            revenue = dict(
                ty=rev,
                ly=rev_ly,
                actual=rev_actual,
                target=rev_target
            ),
            od_distance=od_distance,
            capacity=cap_data
        )
    else:
        return dict(
            pax=dict(
                ty=None,
                ly=None,
                actual=None,
                target=None
            ),
            revenue=dict(
                ty=None,
                ly=None,
                actual=None,
                target=None
            ),
            od_distance=od_distance,
            capacity=get_od_capacity(origin, destination, compartment, dep_date_start, dep_date_end, db=db)
        )


@measure(JUPITER_LOGGER)
def get_data_future_case(pos, origin, destination, compartment, dep_date_start, dep_date_end, db):
    """
    """

    od_distance = get_od_distance(od=origin+destination, db=db)

    query = dict()
    if pos:
        query['pos.City'] = pos
    if origin:
        query['origin.City'] = origin
    if destination:
        query['destination.City'] = destination
    if compartment:
        query['compartment.compartment'] = compartment

    book_date_end = SYSTEM_DATE
    book_date_start = str(today.year) + '-01-01'

    SYSTEM_DATE_LY = get_ly_val(SYSTEM_DATE)

    book_date_start_ly = get_ly_val(book_date_start)
    book_date_end_ly = get_ly_val(book_date_end)

    dep_date_start_ly = get_ly_val(dep_date_start)
    dep_date_end_ly = get_ly_val(dep_date_end)

    # print query
    db.JUP_DB_Manual_Triggers_Module.aggregate(
        # Pipeline
        [
            # Stage 1
            {
                '$match': query
            },

            # Stage 2
            {
                '$facet': {
                    'ty': [
                        {
                            '$match':
                                {
                                    'dep_date':{'$gte':dep_date_start,'$lte': dep_date_end},
                                    'trx_date':{'$lte': book_date_end, '$gte': book_date_start}
                                }
                        }
                        ,
                        {
                            '$group':
                                {
                                    '_id':{
                                        'month':'$dep_month',
                                        'year':'$dep_year'
                                    },
                                    'sales_pax':{'$sum':'$sale_pax.value'},
                                    'flown_pax':{'$sum':'$flown_pax.value'},
                                    'sales_revenue':{'$sum':'$sale_revenue.value'},
                                    'flown_revenue':{'$sum':'$flown_revenue.value'},
                                    'forecast': {'$addToSet': '$forecast'},
                                    'target': {'$addToSet': '$target'}
                                }
                        }
                        ,
                        {
                            '$addFields': {
                                'forecast': {
                                    '$cond':
                                        {
                                            'if': {'$gt':[{'$size':'$forecast'},0]},
                                            'then': {'$arrayElemAt':['$forecast',0]},
                                            'else':None
                                        }
                                }
                                ,
                                'target': {
                                    '$cond':
                                        {
                                            'if': {'$gt': [{'$size': '$target'}, 0]},
                                            'then': {'$arrayElemAt': ['$target', 0]},
                                            'else': None
                                        }
                                }
                            }
                        }
                        ,
                        {
                            '$group':
                                {
                                    '_id':None,
                                    'sales_pax':{'$sum':'$sales_pax'},
                                    'flown_pax':{'$sum':'$flown_pax'},
                                    'sales_revenue':{'$sum':'$sales_revenue'},
                                    'flown_revenue':{'$sum':'$flown_revenue'},
                                    'monthly_details':{
                                        '$push':{
                                            'month':'$_id.month',
                                            'year':'$_id.year',
                                            'target':'$target',
                                            'forecast':'$forecast'
                                        }
                                    }
                                }
                        }
                    ],
                    'ly': [
                        {
                            '$match':
                                {
                                    'dep_date':{'$gte':dep_date_start,'$lte': dep_date_end},
                                    'trx_date':{'$lte': book_date_end, '$gte': book_date_start}
                                }
                        }
                        ,
                        {
                            '$group':
                                {
                                    '_id':{
                                        'month':'$dep_month',
                                        'year':'$dep_year'
                                    },
                                    'sales_pax':{'$sum':'$sale_pax.value_1'},
                                    'flown_pax':{'$sum':'$flown_pax.value_1'},
                                    'sales_revenue':{'$sum':'$sale_revenue.value_1'},
                                    'flown_revenue':{'$sum':'$flown_revenue.value_1'},
                                    'forecast': {'$addToSet': '$forecast'},
                                    'target': {'$addToSet': '$target'}
                                }
                        }
                        ,
                        {
                            '$addFields': {
                                'forecast': {
                                    '$cond':
                                        {
                                            'if': {'$gt': [{'$size': '$forecast'}, 0]},
                                            'then': {'$arrayElemAt': ['$forecast', 0]},
                                            'else': None
                                        }
                                }
                                ,
                                'target': {
                                    '$cond':
                                        {
                                            'if': {'$gt': [{'$size': '$target'}, 0]},
                                            'then': {'$arrayElemAt': ['$target', 0]},
                                            'else': None
                                        }
                                }
                            }
                        }
                        ,
                        {
                            '$group':
                                {
                                    '_id':None,
                                    'sales_pax':{'$sum':'$sales_pax'},
                                    'flown_pax':{'$sum':'$flown_pax'},
                                    'sales_revenue':{'$sum':'$sales_revenue'},
                                    'flown_revenue':{'$sum':'$flown_revenue'},
                                    'monthly_details':{
                                        '$push':{
                                            'month':'$_id.month',
                                            'year':'$_id.year',
                                            'target':'$target',
                                            'forecast':'$forecast'
                                        }
                                    }
                                }
                        }
                    ]
                    # add more facets
                }
            },

            # Stage 3
            {
                '$out': "temp_col_sai_2"
            },

        ]
        # Created with Studio 3T, the IDE for MongoDB - https:#studio3t.com/
    )

    crsr = db["temp_col_sai_2"].find()
    # data = list(crsr)
    # print data
    if crsr.count() == 1:
        data = crsr[0]
        # print 'Output from Manual Triggers Col'
        # print data
        ty = data['ty']
        ly = data['ly']

        if len(ty) == 1:
            pax = ty[0]['sales_pax']
            rev = ty[0]['sales_revenue']
            monthly_details = ty[0]['monthly_details']
            if monthly_details:
                tar_fcst_data = get_total_contribution(monthly_details, dep_date_start, dep_date_end)
                pax_actual = tar_fcst_data['forecast']['pax']
                rev_actual = tar_fcst_data['forecast']['revenue']
                pax_target = tar_fcst_data['target']['pax']
                rev_target = tar_fcst_data['target']['revenue']
            else:
                pax_actual = 0
                pax_target = 0
                rev_actual = 0
                rev_target = 0
        else:
            pax = 0
            rev = 0
            pax_actual = 0
            pax_target = 0
            rev_actual = 0
            rev_target = 0

        if len(ly) == 1:
            pax_ly = ly[0]['sales_pax']
            rev_ly = ly[0]['sales_revenue']
        else:
            pax_ly = 0
            rev_ly = 0

        return dict(
            pax = dict(
                ty=pax,
                ly=pax_ly,
                actual=pax_actual,
                target=pax_target
            ),
            revenue = dict(
                ty=rev,
                ly=rev_ly,
                actual=rev_actual,
                target=rev_target
            ),
            od_distance=od_distance,
            capacity=get_od_capacity(origin, destination, compartment, dep_date_start, dep_date_end, db=db)
        )
    else:
        return dict(
            pax = dict(
                ty=None,
                ly=None,
                actual=None,
                target=None
            ),
            revenue = dict(
                ty=None,
                ly=None,
                actual=None,
                target=None
            ),
            od_distance=od_distance,
            capacity=get_od_capacity(origin, destination, compartment, dep_date_start, dep_date_end, db=db)
        )


@measure(JUPITER_LOGGER)
def get_data_past_case(pos, origin, destination, compartment, dep_date_start, dep_date_end, db):
    """
    """

    od_distance = get_od_distance(od=origin+destination, db=db)

    query = dict()
    if pos:
        query['pos.City'] = pos
    if origin:
        query['origin.City'] = origin
    if destination:
        query['destination.City'] = destination
    if compartment:
        query['compartment.compartment'] = compartment

    book_date_end = SYSTEM_DATE
    book_date_start = str(today.year) + '-01-01'

    SYSTEM_DATE_LY = get_ly_val(SYSTEM_DATE)

    book_date_start_ly = get_ly_val(book_date_start)
    book_date_end_ly = get_ly_val(book_date_end)

    dep_date_start_ly = get_ly_val(dep_date_start)
    dep_date_end_ly = get_ly_val(dep_date_end)

    db.JUP_DB_Manual_Triggers_Module.aggregate(
        # Pipeline
        [
            # Stage 1
            {
                '$match': query
            },

            # Stage 2
            {
                '$facet': {
                    'ty': [
                        {
                            '$match':
                                {
                                    'dep_date':{'$gte':dep_date_start,'$lte': dep_date_end},
                                    'trx_date':{'$lte': book_date_end, '$gte': book_date_start}
                                }
                        }
                        ,
                        {
                            '$group':
                                {
                                    '_id':{
                                        'month':'$dep_month',
                                        'year':'$dep_year'
                                    },
                                    'sales_pax':{'$sum':'$sale_pax.value'},
                                    'flown_pax':{'$sum':'$flown_pax.value'},
                                    'sales_revenue':{'$sum':'$sale_revenue.value'},
                                    'flown_revenue':{'$sum':'$flown_revenue.value'},
                                    'forecast': {'$addToSet': '$forecast'},
                                    'target': {'$addToSet': '$target'}
                                }
                        }
                        ,
                        {
                            '$addFields': {
                                'forecast': {
                                    '$cond':
                                        {
                                            'if': {'$gt': [{'$size': '$forecast'}, 0]},
                                            'then': {'$arrayElemAt': ['$forecast', 0]},
                                            'else': None
                                        }
                                }
                                ,
                                'target': {
                                    '$cond':
                                        {
                                            'if': {'$gt': [{'$size': '$target'}, 0]},
                                            'then': {'$arrayElemAt': ['$target', 0]},
                                            'else': None
                                        }
                                }
                            }
                        }
                        ,
                        {
                            '$group':
                                {
                                    '_id':None,
                                    'sales_pax':{'$sum':'$sales_pax'},
                                    'flown_pax':{'$sum':'$flown_pax'},
                                    'sales_revenue':{'$sum':'$sales_revenue'},
                                    'flown_revenue':{'$sum':'$flown_revenue'},
                                    'monthly_details':{
                                        '$push':{
                                            'month':'$_id.month',
                                            'year':'$_id.year',
                                            'target':'$target',
                                            'forecast':'$forecast'
                                        }
                                    }
                                }
                        }
                    ],
                    'ly': [
                        {
                            '$match':
                                {
                                    'dep_date':{'$gte':dep_date_start,'$lte': dep_date_end},
                                    'trx_date':{'$lte': book_date_end, '$gte': book_date_start}
                                }
                        }
                        ,
                        {
                            '$group':
                                {
                                    '_id':{
                                        'month':'$dep_month',
                                        'year':'$dep_year'
                                    },
                                    'sales_pax':{'$sum':'$sale_pax.value_1'},
                                    'flown_pax':{'$sum':'$flown_pax.value_1'},
                                    'sales_revenue':{'$sum':'$sale_revenue.value_1'},
                                    'flown_revenue':{'$sum':'$flown_revenue.value_1'},
                                    'forecast': {'$addToSet': '$forecast'},
                                    'target': {'$addToSet': '$target'}
                                }
                        }
                        ,
                        {
                            '$addFields': {
                                'forecast': {
                                    '$cond':
                                        {
                                            'if': {'$gt': [{'$size': '$forecast'}, 0]},
                                            'then': {'$arrayElemAt': ['$forecast', 0]},
                                            'else': None
                                        }
                                }
                                ,
                                'target': {
                                    '$cond':
                                        {
                                            'if': {'$gt': [{'$size': '$target'}, 0]},
                                            'then': {'$arrayElemAt': ['$target', 0]},
                                            'else': None
                                        }
                                }
                            }
                        }
                        ,
                        {
                            '$group':
                                {
                                    '_id':None,
                                    'sales_pax':{'$sum':'$sales_pax'},
                                    'flown_pax':{'$sum':'$flown_pax'},
                                    'sales_revenue':{'$sum':'$sales_revenue'},
                                    'flown_revenue':{'$sum':'$flown_revenue'},
                                    'monthly_details':{
                                        '$push':{
                                            'month':'$_id.month',
                                            'year':'$_id.year',
                                            'target':'$target',
                                            'forecast':'$forecast'
                                        }
                                    }
                                }
                        }
                    ]
                    # add more facets
                }
            },

            # Stage 3
            {
                '$out': "temp_col_sai_3"
            },

        ]
        # Created with Studio 3T, the IDE for MongoDB - https:#studio3t.com/
    )

    crsr = db["temp_col_sai_3"].find()
    if crsr.count() == 1:
        data = crsr[0]
        # print 'Output from Manual Triggers Col'
        # print data
        ty = data['ty']
        ly = data['ly']

        if len(ty) == 1:
            pax = ty[0]['flown_pax']
            rev = ty[0]['flown_revenue']
            pax_actual = ty[0]['flown_pax']
            rev_actual = ty[0]['flown_revenue']
            monthly_details = ty[0]['monthly_details']
            if monthly_details:
                tar_fcst_data = get_total_contribution(monthly_details, dep_date_start, dep_date_end)
                pax_target = tar_fcst_data['target']['pax']
                rev_target = tar_fcst_data['target']['revenue']
            else:
                pax_target = 0
                rev_target = 0
        else:
            print "got nothing in ty"
            pax = 0
            rev = 0
            pax_actual = 0
            rev_actual = 0
            pax_target = 0
            rev_target = 0

        if len(ly) == 1:
            pax_ly = ly[0]['flown_pax']
            rev_ly = ly[0]['flown_revenue']
        else:
            pax_ly = 0
            rev_ly = 0

        return dict(
            pax = dict(
                ty=pax,
                ly=pax_ly,
                actual=pax_actual,
                target=pax_target
            ),
            revenue = dict(
                ty=rev,
                ly=rev_ly,
                actual=rev_actual,
                target=rev_target
            ),
            od_distance=od_distance
        )
    else:
        return dict(
            pax = dict(
                ty=None,
                ly=None,
                actual=None,
                target=None
            ),
            revenue = dict(
                ty=None,
                ly=None,
                actual=None,
                target=None
            ),
            od_distance=od_distance,
            capacity=get_od_capacity(origin, destination, compartment, dep_date_start, dep_date_end, db=db)
        )


@measure(JUPITER_LOGGER)
def get_total_contribution(monthly_details, dep_date_start, dep_date_end):
    """
    monthly_details = [
                {
                    "month" : 5.0, 
                    "year" : 2017.0, 
                    "target" : {
                        "pax" : 3719.0, 
                        "avgFare" : 212.0, 
                        "revenue" : 788428.0
                    }, 
                    "forecast" : {
                        "pax" : 3633.0, 
                        "avgFare" : 223.0, 
                        "revenue" : 810159.0
                    }
                }
                ,
                {
                    "month" : 5.0, 
                    "year" : 2017.0, 
                    "target" : {
                        "pax" : 3719.0, 
                        "avgFare" : 212.0, 
                        "revenue" : 788428.0
                    }, 
                    "forecast" : {
                        "pax" : 3633.0, 
                        "avgFare" : 223.0, 
                        "revenue" : 810159.0
                    }
                }
            ]
    """
    forecast_data = defaultdict(list)
    target_data = defaultdict(list)

    dep_date_start_obj = datetime.datetime.strptime(
        dep_date_start,
        '%Y-%m-%d'
    )

    dep_date_end_obj = datetime.datetime.strptime(
        dep_date_end,
        '%Y-%m-%d'
    )
    sd = dep_date_start_obj.day
    sm = dep_date_start_obj.month
    sy = dep_date_start_obj.year
    ed = dep_date_end_obj.day
    em = dep_date_end_obj.month
    ey = dep_date_end_obj.year
    if monthly_details:
        for val in monthly_details:
            if val['target']:
                target_data[(val['month'], val['year'])] = [val['target']['pax'],val['target']['revenue']]
            # else:
            # 	target_data[(val['month'], val['year'])] = (0,0)

            if val['forecast']:
                forecast_data[(val['month'], val['year'])] = [val['forecast']['pax'],val['forecast']['revenue']]
        forecast_pax = 0
        forecast_rev = 0
        # print forecast_data
        if forecast_data:
            for month_year in forecast_data.keys():
                no_of_days = get_no_of_days(month=int(month_year[0]), year=int(month_year[1]))
                forecast_data[month_year].append(no_of_days)
                forecast_data[month_year].append(forecast_data[month_year][0] / no_of_days)
                forecast_data[month_year].append(forecast_data[month_year][1] / no_of_days)
            if forecast_data.keys()[0] == (sm, sy):
                forecast_data[(sm, sy)][2] = forecast_data[(sm, sy)][2] - sd + 1
            if forecast_data.keys()[-1] == (em, ey):
                forecast_data[(em, ey)][2] = ed
            if sm == em and sy == ey:
                forecast_data[(sm, sy)][2] = forecast_data[(sm, sy)][2] - sd + 1
            for key in forecast_data.keys():
                forecast_pax += forecast_data[key][2] * forecast_data[key][3]
                forecast_rev += forecast_data[key][2] * forecast_data[key][4]

        target_pax = 0
        target_rev = 0
        # print target_data
        if target_data:
            for month_year in target_data.keys():
                no_of_days = get_no_of_days(month=int(month_year[0]), year=int(month_year[1]))
                target_data[month_year].append(no_of_days)
                target_data[month_year].append(target_data[month_year][0] / no_of_days)
                target_data[month_year].append(target_data[month_year][1] / no_of_days)
            if target_data.keys()[0] == (sm, sy):
                target_data[(sm, sy)][2] = target_data[(sm, sy)][2] - sd + 1
            if target_data.keys()[-1] == (em, ey):
                target_data[(em, ey)][2] = ed
            if sm == em and sy == ey:
                target_data[(sm, sy)][2] = target_data[(sm, sy)][2] - sd + 1
            for key in target_data.keys():
                target_pax += target_data[key][2] * target_data[key][3]
                target_rev += target_data[key][2] * target_data[key][4]

        return dict(
            forecast=dict(
                pax=math.ceil(forecast_pax),
                revenue=forecast_rev
            ),
            target=dict(
                pax=math.ceil(target_pax),
                revenue=target_rev
            )
        )
    else:
        return dict(
            forecast=dict(
                pax=None,
                revenue=None
            ),
            target=dict(
                pax=None,
                revenue=None
            )
        )


@measure(JUPITER_LOGGER)
def get_workflow_params(data):
    """
        data = 
            {
                'pax':
                    {
                        'ty':23,
                        'ly':25,
                        'actual':213,
                        'target':400
                    },
                'revenue':
                    {
                        'ty':12321.32131,
                        'ly':312312.3123,
                        'actual':2313124.34,
                        'target':32131231.3123
                    },
                'capacity' = {
                    'ty': 5467,
                    'ly': 1231
                }
            }
    """
    # print 'DATA'
    # print data
    response = dict()
    response['pax_data'] = dict(
        pax='NA',
        vlyr='NA',
        vtgt='NA'
    )
    response['revenue_data'] = dict(
        revenue='NA',
        vlyr='NA',
        vtgt='NA'
    )
    response['avg_fare_data'] = dict(
        avg_fare='NA',
        vlyr='NA',
        vtgt='NA'
    )
    response['yield_data_compartment'] = dict(
        yield_='NA',
        vlyr='NA',
        vtgt='NA'
    )
    # print response
    # print response['pax_data']['vlyr']

    # PAX DATA
    if type(data['pax']['ty']) in [int,float]:
        # print 'PAX', data['pax']['ty']
        response['pax_data']['pax'] = data['pax']['ty']

    if type(data['pax']['ty']) in [int, float] and type(data['pax']['ly']) in [int, float] and data['pax']['ly'] > 0:
        if type(data['capacity']['ty']) in [int, float] and type(data['capacity']['ly']) in [int, float] and data['capacity']['ty'] > 0 and data['capacity']['ly'] > 0:
            response['pax_data']['vlyr'] = ((data['pax']['ty']*data['capacity']['ly']/data['capacity']['ty']) - data['pax']['ly']) * 100 / (float(data['pax']['ly']))
        else:
            # print 'PAX VLYR', (data['pax']['ty'] - data['pax']['ly'])*100/float(data['pax']['ly'])
            response['pax_data']['vlyr'] = (data['pax']['ty'] - data['pax']['ly'])*100/(float(data['pax']['ly']))

    if type(data['pax']['actual']) in [int, float] and type(data['pax']['target']) in [int,float] and data['pax']['target'] > 0:
        response['pax_data']['vtgt'] = ((data['pax']['actual'] - data['pax']['target'])*100/float(data['pax']['target']))

    #   REVENUE DATA
    if type(data['revenue']['ty']) in [int, float]:
        response['revenue_data']['revenue'] = data['revenue']['ty']

    if type(data['revenue']['ty']) in [int, float] and type(data['revenue']['ly']) in [int,float] and data['revenue']['ly'] > 0:
        if type(data['capacity']['ty']) in [int,float] and type(data['capacity']['ly']) in [int, float]:
            response['revenue_data']['vlyr'] = ((data['revenue']['ty']*data['capacity']['ly']/data['capacity']['ty']) - data['revenue']['ly']) * 100 / float(
                data['revenue']['ly'])
        else:
            response['revenue_data']['vlyr'] = (data['revenue']['ty'] - data['revenue']['ly']) * 100 / float(
                data['revenue']['ly'])

    if type(data['revenue']['actual']) in [int, float] and type(data['revenue']['target']) in [int, float] and data['revenue']['target'] > 0:
        response['revenue_data']['vtgt'] = (data['revenue']['actual'] - data['revenue']['target'])*100/float(data['revenue']['target'])

    #   AVG FARE DATA
    if type(data['pax']['ty']) in [int, float] and type(data['revenue']['ty']) in [int,float] and data['pax']['ty'] > 0:
        response['avg_fare_data']['avg_fare'] = data['revenue']['ty'] / float(data['pax']['ty'])

        if type(data['pax']['ly']) in [int, float] and type(data['revenue']['ly']) in [int,float] and data['pax']['ly'] > 0:
            avg_fare_ly = data['revenue']['ly'] / float(data['pax']['ly'])

            if avg_fare_ly > 0:
                response['avg_fare_data']['vlyr'] = (response['avg_fare_data']['avg_fare'] - avg_fare_ly)*100/avg_fare_ly

    if type(data['pax']['actual']) in [int, float] and type(data['pax']['target']) in [int,float] and type(data['revenue']['actual']) in [int,float] and type(data['revenue']['target']) in [int,float]:
        if data['pax']['actual'] > 0 and data['pax']['target'] > 0:
            avg_fare_actual = data['revenue']['actual'] / float(data['pax']['actual'])
            print avg_fare_actual
            avg_fare_target = data['revenue']['target'] / float(data['pax']['target'])
            if type(avg_fare_target) in [int, float] and avg_fare_target > 0:
                response['avg_fare_data']['vtgt'] = (avg_fare_actual - avg_fare_target)*100/float(avg_fare_target)

    # YIELD DATA COMPARTMENT
    if type(response['avg_fare_data']['avg_fare']) in [int, float] and data['od_distance'] > 0:
        response['yield_data_compartment']['yield_'] = response['avg_fare_data']['avg_fare']*100/float(data['od_distance'])

    if type(response['avg_fare_data']['vlyr']) in [int, float] and data['od_distance'] > 0:
        response['yield_data_compartment']['vlyr'] = response['avg_fare_data']['vlyr']

    if type(response['avg_fare_data']['vtgt']) in [int, float] and data['od_distance'] > 0:
        response['yield_data_compartment']['vtgt'] = response['avg_fare_data']['vtgt']

    return response


@measure(JUPITER_LOGGER)
def get_seat_factor_values(leg_origin, leg_destination, compartment, dep_date_start, dep_date_end, db):
    """
    """
    response = dict(
        ty='NA',
        ly='NA'
    )
    dep_date_start_ly = get_ly_val(dep_date_start)
    dep_date_end_ly = get_ly_val(dep_date_end)

    # print leg_origin + leg_destination
    crsr = db.JUP_DB_Inventory_Leg.aggregate(
        # Pipeline
        [
            # Stage 1
            {
                '$match': {
                    'od': leg_origin + leg_destination
                }
            },

            # Stage 2
            {
                '$facet': {
                    'ty': [
                        # Stage 2
                        {
                            '$match': {
                                'dep_date': {'$gte': dep_date_start, '$lte': dep_date_end}
                            }
                        },

                        # Stage 3
                        {
                            '$sort': {
                                'last_update_date': -1
                            }
                        },

                        # Stage 4
                        {
                            '$group': {
                                '_id': {
                                    'dep_date': '$dep_date',
                                    'flight': '$Flight_Number',
                                    'od': '$od'
                                },
                                'j_booking': {'$first': '$j_booking'},
                                'y_booking': {'$first': '$y_booking'},
                                'total_booking': {'$first': '$total_booking'},
                                'j_cap': {'$first': '$j_cap'},
                                'y_cap': {'$first': '$y_cap'},
                                'total_cap': {'$first': '$total_cap'}
                            }
                        },

                        # Stage 5
                        {
                            '$group': {
                                '_id': None,
                                'j_booking': {'$sum': '$j_booking'},
                                'y_booking': {'$sum': '$y_booking'},
                                'total_booking': {'$sum': '$total_booking'},

                                'j_cap': {'$sum': '$j_cap'},
                                'y_cap': {'$sum': '$y_cap'},
                                'total_cap': {'$sum': '$total_cap'}
                            }
                        },

                        # Stage 6
                        {
                            '$project': {
                                # specifications
                                '_id': 0,
                                'y_sf': {
                                    '$cond':
                                        {
                                            'if': {'$gt': ['$y_cap', 0]},
                                            'then': {'$multiply': [{'$divide': ['$y_booking', '$y_cap']}, 100]},
                                            'else': 'NA',
                                        }

                                },
                                'j_sf': {
                                    '$cond':
                                        {
                                            'if': {'$gt': ['$j_cap', 0]},
                                            'then': {'$multiply': [{'$divide': ['$j_booking', '$j_cap']}, 100]},
                                            'else': 'NA',
                                        }

                                },
                                'total_sf': {
                                    '$cond':
                                        {
                                            'if': {'$gt': ['$total_cap', 0]},
                                            'then': {'$multiply': [{'$divide': ['$total_booking', '$total_cap']}, 100]},
                                            'else': 'NA',
                                        }

                                }
                            }
                        }

                    ],
                    'ly': [
                        # Stage 2
                        {
                            '$match': {
                                'dep_date': {'$gte': dep_date_start_ly, '$lte': dep_date_end_ly}
                            }
                        },

                        # Stage 3
                        {
                            '$sort': {
                                'last_update_date': -1
                            }
                        },

                        # Stage 4
                        {
                            '$group': {
                                '_id': {
                                    'dep_date': '$dep_date',
                                    'flight': '$Flight_Number',
                                    'od': '$od'
                                },
                                'j_booking': {'$first': '$j_booking'},
                                'y_booking': {'$first': '$y_booking'},
                                'total_booking': {'$first': '$total_booking'},
                                'j_cap': {'$first': '$j_cap'},
                                'y_cap': {'$first': '$y_cap'},
                                'total_cap': {'$first': '$total_cap'}
                            }
                        },

                        # Stage 5
                        {
                            '$group': {
                                '_id': None,
                                'j_booking': {'$sum': '$j_booking'},
                                'y_booking': {'$sum': '$y_booking'},
                                'total_booking': {'$sum': '$total_booking'},

                                'j_cap': {'$sum': '$j_cap'},
                                'y_cap': {'$sum': '$y_cap'},
                                'total_cap': {'$sum': '$total_cap'}
                            }
                        },

                        # Stage 6
                        {
                            '$project': {
                                # specifications
                                '_id': 0,
                                'y_sf': {
                                    '$cond':
                                        {
                                            'if': {'$gt': ['$y_cap', 0]},
                                            'then': {'$multiply': [{'$divide': ['$y_booking', '$y_cap']}, 100]},
                                            'else': 'NA',
                                        }

                                },
                                'j_sf': {
                                    '$cond':
                                        {
                                            'if': {'$gt': ['$j_cap', 0]},
                                            'then': {'$multiply': [{'$divide': ['$j_booking', '$j_cap']}, 100]},
                                            'else': 'NA',
                                        }

                                },
                                'total_sf': {
                                    '$cond':
                                        {
                                            'if': {'$gt': ['$total_cap', 0]},
                                            'then': {'$multiply': [{'$divide': ['$total_booking', '$total_cap']}, 100]},
                                            'else': 'NA',
                                        }

                                }
                            }
                        },

                    ],
                    # add more facets
                }
            }

        ]
    )
    data = list(crsr)
    # print json.dumps(data,indent=1)
    if len(data) == 1:
        if len(data[0]['ty']) == 1:
            if compartment == 'Y':
                response['ty'] = data[0]['ty'][0]['y_sf']
            elif compartment == 'J':
                response['ty'] = data[0]['ty'][0]['j_sf']
            else:
                response['ty'] = data[0]['ty'][0]['total_sf']

        if len(data[0]['ly']) == 1:
            if compartment == 'Y':
                response['ly'] = data[0]['ly'][0]['y_sf']
            elif compartment == 'J':
                response['ly'] = data[0]['ly'][0]['j_sf']
            else:
                response['ly'] = data[0]['ly'][0]['total_sf']

    return response


    """
{ 
	"_id" : ObjectId("5937bfc8616a624d50b65159"), 
	"ty" : [
		{
			"_id" : null, 
			"sales_pax" : 629.0, 
			"flown_pax" : NumberInt(0), 
			"sales_revenue" : 62524.42336273193, 
			"flown_revenue" : NumberInt(0), 
			"monthly_details" : [
				{
					"month" : 5.0, 
					"year" : 2017.0, 
					"target" : {
						"pax" : 3719.0, 
						"avgFare" : 212.0, 
						"revenue" : 788428.0
					}, 
					"forecast" : {
						"pax" : 3633.0, 
						"avgFare" : 223.0, 
						"revenue" : 810159.0
					}
				}
			]
		}
	], 
	"ly" : [
		{
			"_id" : null, 
			"sales_pax" : 1121.0, 
			"flown_pax" : NumberInt(0), 
			"sales_revenue" : 103476.0, 
			"flown_revenue" : NumberInt(0), 
			"monthly_details" : [
				{
					"month" : 5.0, 
					"year" : 2016.0, 
					"target" : {
						"pax" : 3534.0, 
						"avgFare" : 202.0, 
						"revenue" : 749007.0
					}, 
					"forecast" : {
						"pax" : 3452.0, 
						"avgFare" : 212.0, 
						"revenue" : 769652.0
					}
				}
			]
		}
	]
}
"""

    # if __name__ == '__main__':
    # past_data =  get_data_past_case(None, 'DXB', 'DOH', 'Y', '2017-04-01', '2017-04-30')
    # bw_data =  get_data_between_case('ABC', 'ABC', 'DEF', 'Y', '2017-06-01', '2017-06-30')
    # fut_data =  get_data_future_case('ABC', 'ABC', 'DEF', 'Y', '2017-07-01', '2017-07-31')
    # print 'PAST'
    # print json.dumps(past_data,indent=1)
    # print 'WORKFLOW PAST'
    # print json.dumps(get_workflow_params(past_data),indent=1)
    #
    # print 'BETWEEN'
    # print json.dumps(bw_data,indent=1)
    # print 'WORKFLOW BETWEEN'
    # print json.dumps(get_workflow_params(bw_data),indent=1)
    #
    #
    # print 'FUT DATA'
    # print json.dumps(fut_data,indent=1)
    # print 'WORKFLOW FUTURE'
    # print json.dumps(get_workflow_params(fut_data),indent=1)

    # ***************************************	OUTPUT	*****************************************************
    """
    PAST
    {
     "pax": {
      "ly": 37144.0,
      "actual": 37992.0,
      "target": 6910.0,
      "ty": 37992.0
     },
     "od_distance": 766.0,
     "revenue": {
      "ly": 4528216.0,
      "actual": 4778725.699080229,
      "target": 1216160.0,
      "ty": 4778725.699080229
     }
    }
    WORKFLOW PAST
    {
     "avg_fare_data": {
      "avg_fare": 125.78241995894474,
      "vtgt": 12478.241995894474,
      "vlyr": 3.1766639876508402
     },
     "revenue_data": {
      "vlyr": 5.532194115303439,
      "vtgt": 292.93560872584436,
      "revenue": 4778725.699080229
     },
     "yield_data_compartment": {
      "yield_": 16.4206814567813,
      "vtgt": 1629.0133153909235,
      "vlyr": 0.4147080923826162
     },
     "pax_data": {
      "pax": 37992.0,
      "vtgt": 449.8118668596237,
      "vlyr": 2.2830066767176396
     }
    }
    BETWEEN
    {
     "pax": {
      "ly": 174.0,
      "actual": 2300.0,
      "target": 6304.0,
      "ty": 95.0
     },
     "od_distance": 766.0,
     "revenue": {
      "ly": 16712.0,
      "actual": 641700.0,
      "target": 951904.0,
      "ty": 9634.445430755615
     }
    }
    WORKFLOW BETWEEN
    {
     "avg_fare_data": {
      "avg_fare": 101.41521506058542,
      "vtgt": 27800.0,
      "vlyr": 5.590278964467824
     },
     "revenue_data": {
      "vlyr": -42.35013504813538,
      "vtgt": -32.58773994016203,
      "revenue": 9634.445430755615
     },
     "yield_data_compartment": {
      "yield_": 13.239584211564678,
      "vtgt": 3629.242819843342,
      "vlyr": 0.7298014313926663
     },
     "pax_data": {
      "pax": 95.0,
      "vtgt": -63.515228426395936,
      "vlyr": -45.40229885057471
     }
    }
    FUT DATA
    {
     "pax": {
      "ly": 103.0,
      "actual": 0.0,
      "target": 4561.0,
      "ty": 57.0
     },
     "od_distance": 766.0,
     "revenue": {
      "ly": 12857.0,
      "actual": 0,
      "target": 948688.0,
      "ty": 7475.07780456543
     }
    }
    WORKFLOW FUTURE
    {
     "avg_fare_data": {
      "avg_fare": 131.14171586956894,
      "vtgt": "NA",
      "vlyr": 5.060253049433
     },
     "revenue_data": {
      "vlyr": -41.85985996293513,
      "vtgt": -100.0,
      "revenue": 7475.07780456543
     },
     "yield_data_compartment": {
      "yield_": 17.120328442502473,
      "vtgt": "NA",
      "vlyr": 0.6606074477066579
     },
     "pax_data": {
      "pax": 57.0,
      "vtgt": -100.0,
      "vlyr": -44.66019417475728
     }
    }
    """


@measure(JUPITER_LOGGER)
def main_func(pos, origin, destination, compartment, dep_date_start, dep_date_end, db):
    """
    :param pos: 
    :param origin: 
    :param destination: 
    :param compartment: 
    :param dep_date_start: 
    :param dep_date_end: 
    :return: 
    """
    response = dict()

    # Getting the Pax/Revenue/Avg.Fare and Yield Data
    if dep_date_start < SYSTEM_DATE < dep_date_end:
        data = get_data_between_case(pos, origin, destination, compartment, dep_date_start, dep_date_end, db=db)
    elif SYSTEM_DATE <= dep_date_start:
        data = get_data_future_case(pos, origin, destination, compartment, dep_date_start, dep_date_end, db=db)
    else:
        data = get_data_past_case(pos, origin, destination, compartment, dep_date_start, dep_date_end, db=db)

    response.update(get_workflow_params(data))

    leg_flag = False
    if origin == hub or destination == hub:
        leg_flag = True

    if leg_flag:
        sf_data = get_seat_factor_values(leg_origin=origin,
                                         leg_destination=destination,
                                         compartment=compartment,
                                         dep_date_start=dep_date_start,
                                         dep_date_end=dep_date_end, db=db)
        if type(sf_data['ty']) in [int, float] and type(sf_data['ly']) in [int, float] and sf_data['ly'] > 0:
            sf_vlyr = (sf_data['ty'] - sf_data['ly']) * 100 / sf_data['ly']
        else:
            sf_vlyr = 'NA'
        response.update(dict(
            seat_factor=dict(
                leg1=sf_data['ty'],
                leg1_vlyr=sf_vlyr,
                leg2='NA',
                leg2_vlyr='NA'
            )
        ))

    else:
        leg1O = origin
        leg1D = hub
        leg2O = hub
        leg2D = destination

        leg1_sf_data = get_seat_factor_values(leg_origin=leg1O,
                                              leg_destination=leg1D,
                                              compartment=compartment,
                                              dep_date_start=dep_date_start,
                                              dep_date_end=dep_date_end, db=db)
        if type(leg1_sf_data['ty']) in [int, float] and type(leg1_sf_data['ly']) in [int, float] and leg1_sf_data['ly'] > 0:
            leg1_sf_vlyr = (leg1_sf_data['ty'] - leg1_sf_data['ly']) * 100 / leg1_sf_data['ly']
        else:
            leg1_sf_vlyr = 'NA'

        leg2_sf_data = get_seat_factor_values(leg_origin=leg2O,
                                              leg_destination=leg2D,
                                              compartment=compartment,
                                              dep_date_start=dep_date_start,
                                              dep_date_end=dep_date_end, db=db)
        if type(leg2_sf_data['ty']) in [int, float] and type(leg2_sf_data['ly']) in [int, float] and leg2_sf_data['ly'] > 0:
            leg2_sf_vlyr = (leg2_sf_data['ty'] - leg2_sf_data['ly']) * 100 / leg2_sf_data['ly']
        else:
            leg2_sf_vlyr = 'NA'

        response.update(dict(
            seat_factor=dict(
                leg1=leg1_sf_data['ty'],
                leg1_vlyr=leg1_sf_vlyr,
                leg2=leg2_sf_data['ty'],
                leg2_vlyr=leg2_sf_vlyr
            )
        ))

    return response


if __name__ == '__main__':
    import time
    st = time.time()
    client = mongo_client()
    db=client[JUPITER_DB]
    # print 'PAST LEG'
    # print main_func('DXB','DXB','DOH','Y','2017-05-01','2017-05-31')
    # print time.time() - st
    #
    # print 'BW LEG'
    # print main_func('DXB', 'DXB', 'DOH', 'Y', '2017-06-01', '2017-06-30')
    # print time.time() - st
    #
    # print 'FUTLEG'
    # print main_func('DXB', 'DXB', 'DEL', 'Y', '2017-05-01', '2017-05-31')
    # print time.time() - st
    #
    # print 'PAST OD'
    # print main_func('DOH', 'DOH', 'DAC', 'Y', '2017-04-01', '2017-04-30')
    # print time.time() - st
    #
    # print 'BW OD'
    # print main_func('DOH', 'DOH', 'DAC', 'Y', '2017-06-01', '2017-06-30')
    # print time.time() - st
    #
    # print 'FUT OD'
    # print main_func('DOH', 'DOH', 'DAC', 'Y', '2017-07-01', '2017-07-31')
    # print time.time() - st
    print get_seat_factor_values('DXB', 'AHB', 'Y', '2017-04-27', '2017-05-27', db=db)
    print "time taken to get seat factor values = ", time.time() - st
    client.close()