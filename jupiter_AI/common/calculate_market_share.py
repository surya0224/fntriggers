"""
File Name              :   calculate_market_share.py
Author                 :   K Sai Krishna
Date Created           :   2016-12-01
Description            :   For a pos, od, compartment combination
                           obtaining the YTD market share, market share vlyr,
                           average fare, market average fare
Data access layer

MODIFICATIONS LOG          :
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :
"""
import datetime
import inspect

from collections import defaultdict

from jupiter_AI import mongo_client, JUPITER_LOGGER
from jupiter_AI.logutils import measure
import jupiter_AI.common.ClassErrorObject as errorClass
from jupiter_AI.RnA.common_RnA_functions import gen_collection_name
from jupiter_AI.network_level_params import JUPITER_DB, SYSTEM_DATE, na_value, query_month_year_builder, Host_Airline_Code, Host_Airline_Hub
from jupiter_AI.triggers.data_level.MainClass import DataLevel
from jupiter_AI.BaseParametersCodes.common import get_ly_val
#db = client[JUPITER_DB]


@measure(JUPITER_LOGGER)
def get_module_name():
    '''
    FUnction used to get the module name where it is called
    '''
    return inspect.stack()[1][3]


@measure(JUPITER_LOGGER)
def get_arg_lists(frame):
    '''
    function used to get the list of arguments of the function
    where it is called
    '''
    args, _, _, values = inspect.getargvalues(frame)
    argument_name_list = []
    argument_value_list = []
    for k in args:
        argument_name_list.append(k)
        argument_value_list.append(values[k])
    return argument_name_list, argument_value_list


@measure(JUPITER_LOGGER)
def calculate_market_share_target(airline, pos, origin, destination, compartment, dep_date_from, dep_date_to, db):
    """
    First Get Host Capacity
    Then Competitor Capacity
    
    Sum them up to get the Total Capacity
    
    Get Rating Values.
    
    Calculate the fms
    """
    obj = DataLevel(
        data={
            'pos': pos,
            'origin': origin,
            'destination': destination,
            'compartment': compartment
        },
        system_date=SYSTEM_DATE
    )
    cap_data = obj.get_capacity_data(dep_date_start=dep_date_from,
                                     dep_date_end=dep_date_to,
                                     db=db)
    host_capacity = cap_data['ty']
    print 'HC',host_capacity

    dep_date_from_obj = datetime.datetime.strptime(dep_date_from,'%Y-%m-%d')
    dep_date_to_obj = datetime.datetime.strptime(dep_date_to, '%Y-%m-%d')
    month_year_combinations = query_month_year_builder(stdm=dep_date_from_obj.month,
                                                       stdy=dep_date_from_obj.year,
                                                       endm=dep_date_to_obj.month,
                                                       endy=dep_date_to_obj.year)
    query = defaultdict(list)
    query['$and'].append({'od':origin + destination})
    query['$and'].append({'$or': month_year_combinations})
    query['$and'].append({'compartment': compartment})
    if type(host_capacity) in [int,float] and host_capacity>0:
        query['$and'].append({'airline':{'$ne':Host_Airline_Code}})
    print query
    capacity_crsr = db.JUP_DB_OD_Capacity.aggregate(
    [
        {
            '$match': dict(query)
        },
        {
            '$group': {
            '_id': '$airline',
            'capacity': {'$sum': '$capacity'}
            }
        }
        ,
        {
            '$project':
                {
                    'carrier':'$_id',
                    'capacity':'$capacity'
                }
        }
    ]
    )
    capacity_data = list(capacity_crsr)
    print capacity_data

    ratings_crsr = db.JUP_DB_Competitor_Ratings.aggregate(
    [
        {
            '$match':
                {
                    'od':origin + destination
                }
        }
        ,
        {
            '$sort':
                {
                    'last_update_date':-1
                }
        }
        ,
        {
            '$group':
                {
                    '_id':
                        {
                            'carrier': '$airline'
                        },
                    'rating':{'$first':'$rating'}
                }
        }
        ,
        {
            '$project':
                {
                    'carrier': '$_id.carrier',
                    'rating': '$rating'
                }
        }
    ]
    )
    ratings_data = list(ratings_crsr)
    print ratings_data
    airline_param = 0
    total_param = 0
    if host_capacity > 0 and host_capacity!='NA':
        capacity_data.append({'carrier':Host_Airline_Code, 'capacity':host_capacity})
    # for cap_doc in capacity_data:
    #     for rat_doc in ratings_data:
    #         if cap_doc['carrier'] == rat_doc['carrier']:
    #             print cap_doc['carrier']
    #             print cap_doc['capacity']
    #             print rat_doc['rating']
    #             total_param += cap_doc['capacity'] * rat_doc['rating']
    #             if cap_doc['carrier'] == airline:
    #                 airline_param += cap_doc['capacity'] * rat_doc['rating']
    for cap_doc in capacity_data:
        total_param += cap_doc['capacity']
        if cap_doc['carrier'] == airline:
            airline_param += cap_doc['capacity']

    if total_param >0:
        fms= airline_param * 100 / float(total_param)
        return fms

# noinspection PyNoneFunctionAssignment
@measure(JUPITER_LOGGER)
def calculate_market_share_val(airline,
                               pos,
                               origin,
                               destination,
                               compartment,
                               dep_date_from,
                               dep_date_to,
                               db):
    """
    :param airline:
    :param pos:
    :param origin:
    :param destination:
    :param compartment:
    :param month:
    :param year:
    :return:
    """
    print db
    if not dep_date_from:
        dep_date_from = str(datetime.date.today().year)+'01-01'
    if not dep_date_to:
        dep_date_to = str(datetime.date.today())
    today = datetime.datetime.today()
    from_month = datetime.datetime.strptime(dep_date_from, '%Y-%m-%d').month
    from_year = datetime.datetime.strptime(dep_date_from, '%Y-%m-%d').year
    to_month = datetime.datetime.strptime(dep_date_to, '%Y-%m-%d').month
    to_year = datetime.datetime.strptime(dep_date_to, '%Y-%m-%d').year
    temp_col_name = gen_collection_name()
    month_year_combinations = query_month_year_builder(from_month,
                                                       from_year,
                                                       to_month,
                                                       to_year)
    query = dict()
    if pos:
        if pos == Host_Airline_Hub:
            query['pos'] = {'$in': [Host_Airline_Hub,'UAE']}
        else:
            query['pos'] = pos
    if origin and destination:
        query['od'] = origin+destination
    # if destination:
        # query['destination'] = destination
    if compartment:
        query['compartment'] = compartment
    if month_year_combinations:
        query['$or'] = month_year_combinations

    print query

    db.JUP_DB_Market_Share.aggregate(

    # Pipeline for aggregate
    [
        # Querying the Collection
        {
            '$match': query
        },

        # Grouping on the basis of key and obtaining aggregated
        # pax,revenue,pax last year and revenue last year values
        {
            '$group': {
                '_id':{
                    # 'pos':'$pos',
                    # 'od':'$od',
                    # 'compartment':'$compartment',
                    'airline':'$MarketingCarrier1'
                }
                ,
                'pax': {'$sum':'$pax'}
                ,
                'revenue':{'$sum':'$revenue'}
                ,
                'pax_ly':{'$sum':'$pax_1'}
                ,
                'revenue_ly':{'$sum':'$revenue_1'}
            }
        },

        #  Grouping again to obtain market level aggregates
        {
            '$group': {
                '_id': None
                #     {
                #     'pos':'$pos',
                #     'od':'$od',
                #     'compartment':'$compartment'
                # }
                ,
                'airline_level_details':{
                    '$push': {
                        'airline':'$_id.airline',
                        'pax':'$pax',
                        'revenue':'$revenue',
                        'pax_ly':'$pax_ly',
                        'revenue_ly':'$revenue_ly'
                    }
                }
                ,
                
                'mrkt_size': {'$sum':'$pax'},
                'mrkt_revenue': {'$sum':'$revenue'},
                'mrkt_size_ly': {'$sum':'$pax_ly'},
                'mrkt_revenue_ly': {'$sum':'$revenue_ly'}
            }
        },

        #  Unwinding the data again to the airline level of a market
        {
            '$unwind': '$airline_level_details'
        },

        # Calculating the required parameters market share, market share last year,
        # average fare,market average fare
        {
            '$project': {
                # 'pos':'$_id.pos',
                # 'od':'$_id.od',
                # 'compartment':'$_id.compartment',
                'airline':'$airline_level_details.airline',
                'pax':'$airline_level_details.pax',
                'pax_ly':'$airline_level_details.pax_ly',
                'mrkt_share':{
                    '$cond':{
                        'if':{'$gt':['$mrkt_size',0]},
                        'then':{'$multiply': [{'$divide':['$airline_level_details.pax','$mrkt_size']},100]},
                        'else':None
                    }
                }
                ,
                'mrkt_share_ly':{
                    '$cond':{
                        'if':{'$gt':['$mrkt_size_ly',0]},
                        'then':{'$multiply': [{'$divide':['$airline_level_details.pax_ly','$mrkt_size_ly']},100]},
                        'else':None
                    }
                }
                ,
                'average_fare':{
                    '$cond':{
                        'if':{'$gt':['$airline_level_details.pax',0]},
                        'then':{'$divide':['$airline_level_details.revenue','$airline_level_details.pax']},
                        'else':None
                    }
                }
                ,
                'mrkt_average_fare':{
                    '$cond':{
                        'if':{'$gt':['$mrkt_size',0]},
                        'then':{'$divide':['$mrkt_revenue','$mrkt_size']},
                        'else':None
                    }
                }
            }
        },

        #  Projecting or Calculating the final values
        #  market share
        #  market share vlyr
        #  average fare airline
        #  market average fare
        {
            '$project': {
                '_id': 0,
                'airline': '$airline',
                'pax': '$pax',
                'pax_ly': '$pax_ly',
                'market_share':'$mrkt_share',
                'market_share_ly':'$mrkt_share_ly',
                'market_share_vlyr':{
                    '$cond': {
                        'if': {'$gt': [{'$ifNull': ["$mrkt_share_ly", -5]}, 0]},
                        'then':{'$multiply': [{'$divide':[{'$subtract':['$mrkt_share','$mrkt_share_ly']},'$mrkt_share_ly']},100]},
                        'else':None
                    }
                }
                ,
                'pax_vlyr': {
                    '$cond': {
                        'if': {'$gt': ["$pax_ly", 0]},
                        'then':{'$multiply': [{'$divide': [{'$subtract': ['$pax', '$pax_ly']}, '$pax_ly']}, 100]},
                        'else':None
                    }
                }
                ,
                'average_fare':'$average_fare',
                'market_average_fare':'$mrkt_average_fare'
                
            }
        }
        ,
        {
            '$match': {
                'airline': airline
            }
        }
        ,
        {
            '$out': temp_col_name
        }
    ]

    )

    try:
        if temp_col_name in db.collection_names():
            crsr = db[temp_col_name].find()
            crsr_mrkt_shr = list(crsr)
            print crsr_mrkt_shr
            db[temp_col_name].drop()
            response = dict()
            if len(crsr_mrkt_shr) == 1:
                print crsr_mrkt_shr[0]
                response['pax'] = crsr_mrkt_shr[0]['pax']
                response['pax_ly'] = crsr_mrkt_shr[0]['pax_ly']
                if crsr_mrkt_shr[0]['market_share']:
                    response['market_share'] = round(crsr_mrkt_shr[0]['market_share'],2)
                else:
                    response['market_share'] = crsr_mrkt_shr[0]['market_share']
                if crsr_mrkt_shr[0]['market_share_vlyr']:
                    response['market_share_vlyr'] = round(crsr_mrkt_shr[0]['market_share_vlyr'],2)
                else:
                    response['market_share_vlyr'] = crsr_mrkt_shr[0]['market_share_vlyr']
                if crsr_mrkt_shr[0]['average_fare']:
                    response['average_fare'] = round(crsr_mrkt_shr[0]['average_fare'],0)
                else:
                    response['average_fare'] = crsr_mrkt_shr[0]['average_fare']
                if crsr_mrkt_shr[0]['market_average_fare']:
                    response['market_average_fare'] = round(crsr_mrkt_shr[0]['market_average_fare'],0)
                else:
                    response['market_average_fare'] = crsr_mrkt_shr[0]['market_average_fare']
                response['market_share_ly'] = crsr_mrkt_shr[0]['market_share_ly']
                if crsr_mrkt_shr[0]['pax_vlyr']:
                    response['pax_vlyr'] = crsr_mrkt_shr[0]['pax_vlyr']
                if response['market_share'] and response['market_share'] != 0:
                    fms = calculate_market_share_target(airline,
                                                        pos,
                                                        origin,
                                                        destination,
                                                        compartment,
                                                        dep_date_from,
                                                        dep_date_to,
                                                        db
                                                        )
                    if fms and fms != 0:
                        market_share_vtgt = (response['market_share'] - fms)*100/fms
                        response['fms'] = fms
                        response['market_share_vtgt'] = market_share_vtgt
                    else:
                        response['fms'] = None
                        response['market_share_vtgt'] = None
                else:
                    response['fms'] = None
                    response['market_share_vtgt'] = None
                return response

            elif len(crsr_mrkt_shr) > 0:
                unexpected_response_error = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                                                   get_module_name(),
                                                                   get_arg_lists(inspect.currentframe()))
                unexpected_response_error_desc = ''.join(['Expected 1 response but obtained ',
                                                          str(len(crsr_mrkt_shr)),
                                                          ' from Market Share Data for ',
                                                          'POS-', str(pos),
                                                          ' OD-', str(origin), str(destination),
                                                          ' COMPARTMENT-', str(compartment)])
                unexpected_response_error.append_to_error_list(unexpected_response_error_desc)
                raise unexpected_response_error

            else:
                no_response_error = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                                           get_module_name(),
                                                           get_arg_lists(inspect.currentframe()))
                no_response_error_desc = ''.join(['No Market Share Data for ',
                                                  'POS-', str(pos),
                                                  ' OD-', str(origin), str(destination),
                                                  ' COMPARTMENT-', str(compartment)])
                no_response_error.append_to_error_list(no_response_error_desc)
                raise no_response_error
        else:
            pass
    except errorClass.ErrorObject:
        response = dict()
        response['pax'] = na_value
        response['market_share'] = na_value
        response['market_share_vlyr'] = na_value
        response['market_share_vtgt'] = na_value
        response['average_fare'] = na_value
        response['market_average_fare'] = na_value
        response['fms'] = na_value
        response['pax_ly'] = na_value
        response['pax_vlyr'] = na_value
        return response
    finally:
        db.temp_col_name.drop()


@measure(JUPITER_LOGGER)
def calculate_market_share(airline,
                           pos,
                           origin,
                           destination,
                           compartment,
                           dep_date_from,
                           dep_date_to,
                           db):
    """
    :param airline: 
    :param pos: 
    :param origin: 
    :param destination: 
    :param compartment: 
    :param dep_date_from: 
    :param dep_date_to: 
    :return: 
    """
    ty_data = calculate_market_share_val(airline,
                                           pos,
                                           origin,
                                           destination,
                                           compartment,
                                           dep_date_from,
                                           dep_date_to,
                                           db=db)
    print ty_data
    dep_date_from_ly = get_ly_val(dep_date_from)
    dep_date_to_ly = get_ly_val(dep_date_to)

    ly_data = calculate_market_share_val(airline,
                                         pos,
                                         origin,
                                         destination,
                                         compartment,
                                         dep_date_from_ly,
                                         dep_date_to_ly,
                                         db=db)
    print ly_data
    ty_data['market_share_vlyr'] = ly_data['market_share']
    return ty_data

if __name__ == '__main__':
    client = mongo_client()
    db=client[JUPITER_DB]
    # print 'FMS', calculate_market_share_target('FZ', 'DXB', 'DXB', 'DOH', 'Y', '2017-05-01', '2017-05-31')
    # print 'FMS', calculate_market_share_target('EK', 'DXB', 'DXB', 'DOH', 'J', '2017-05-01', '2017-05-31')
    # print 'FMS', calculate_market_share('EK', 'DXB', 'DXB', 'DOH', 'Y', '2017-05-01', '2017-05-31')
    print 'MS', calculate_market_share('FZ', 'DOH', 'DXB', 'DOH', 'Y', '2017-05-01', '2017-05-31', db=db)
    client.close()