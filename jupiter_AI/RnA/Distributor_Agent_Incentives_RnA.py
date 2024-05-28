"""
File Name              :	Distributor_Agent_Incentives_RnA
Author                 :	K Sai Krishna
Date Created           :	2016-12-19
Description            :	RnA analysis for review of incentives for Distributors/Agents
Status                 :

MODIFICATIONS LOG	       :
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :
"""
import datetime
import inspect
import json
from datetime import date
from jupiter_AI import client, JUPITER_DB, Host_Airline_Code as Host, query_month_year_builder
from jupiter_AI.RnA.common_RnA_functions import gen_collection_name


def get_module_name():
    return inspect.stack()[1][3]


def get_arg_lists(frame):
    """
    function used to get the list of arguments of the function
    where it is called
    """
    args, _, _, values = inspect.getargvalues(frame)
    argument_name_list = []
    argument_value_list = []
    for k in args:
        argument_name_list.append(k)
        argument_value_list.append(values[k])
    return argument_name_list, argument_value_list


def build_query_mrkt_shr_agent_col(dict_scr_filter):
    """
    Build the query for filtering fares from Market Share Agent Collection according to the filter
    :param dict_scr_filter:
    :return:
    """
    qry_ms_agent = dict()
    qry_ms_agent['$and'] = []
    today = str(date.today())
    if dict_scr_filter['region']:
        qry_ms_agent['$and'].append({'region': {'$in': dict_scr_filter['region']}})
    if dict_scr_filter['country']:
        qry_ms_agent['$and'].append({'country': {'$in': dict_scr_filter['country']}})
    if dict_scr_filter['pos']:
        qry_ms_agent['$and'].append({'pos': {'$in': dict_scr_filter['pos']}})
    if dict_scr_filter['compartment']:
        qry_ms_agent['$and'].append({'compartment': {'$in': dict_scr_filter['compartment']}})
    if dict_scr_filter['origin']:
        od_build = []
        for idx, item in enumerate(dict_scr_filter['origin']):
            od_build.append({'origin': item,
                             'destination': dict_scr_filter['destination'][idx]})
            qry_ms_agent['$and'].append({'$or': od_build})

    from_obj = datetime.datetime.strptime(dict_scr_filter['fromDate'], '%Y-%m-%d')
    to_obj = datetime.datetime.strptime(dict_scr_filter['toDate'], '%Y-%m-%d')
    qry_ms_agent['$and'].append({'$or': query_month_year_builder(from_obj.month,
                                                                 from_obj.year,
                                                                 to_obj.month,
                                                                 to_obj.year)})
    return qry_ms_agent


def get_agent_data(dict_scr_filter):
    """

    :param dict_scr_filter:
    :return:
    """
    temp_collection_name = gen_collection_name()
    qry_ms_agent_data = build_query_mrkt_shr_agent_col(dict_scr_filter)
    ppl_agents_data = [
        {
            '$match': qry_ms_agent_data
        },
        {
            '$group': {'_id': {'agent': '$agent','region': '$region','country':'$country',
                               'pos':'$pos','od':'$od','carrier':'$MarketingCarrier1'},
                       'pax': {'$sum': '$pax'},
                       'pax_host': {
                           "$sum": {"$cond": [
                               {'$eq': ['$MarketingCarrier1', Host]},
                               '$pax',
                               0
                           ]}
                       },
                       'rev': {'$sum': '$revenue'},
                       'rev_host': {
                           "$sum": {"$cond": [
                               {'$eq': ['$MarketingCarrier1', Host]},
                               '$revenue',
                               0
                           ]}
                       }
                       }
        },
        {
            '$project': {'_id': 0,
                         'Agent': '$_id.agent',
                         'Pax': '$pax',
                         'Pax_Host': '$pax_host',
                         'Revenue':'$rev',
                         'Rev_host':'$rev_host',
                         'pos_host':'$pos_host',
                         'Market_Share_host': {'$multiply':[{'$divide':['$pax_host','$pax']},100]}
                         }
        },
        {
            '$group':{'_id': 0,
                      'tot_pax': {'$sum':'$Pax'},
                      'tot_host_pax':{'$sum':'$Pax_Host'},
                      'Agent_details': {'$push': {'Agent_Name': "$Agent", 'Pax': "$Pax", 'Pax_host':'$Pax_Host',
                                                  'Agent_Share_Host':'$Market_Share_Host'}}
            }
        },
        {
            '$unwind':'$Agent_details'
        },
        {
            '$project':{ '_id': 0,
                         'Agent_Name':'$Agent_details.Agent_Name',
                         'Tot_Pax': '$Agent_details.Pax',
                         'Pax_host': '$Agent_details.Pax_host',
                         'tot_pax':'$tot_pax',
                         'tot_host_pax':'$tot_host_pax',
                         'PI': {'$multiply':['$Agent_details.Pax',
                                             {'$subtract':[{'$divide':['$Agent_details.Pax_host','$Agent_details.Pax']},
                                                           {'$divide': ['$tot_host_pax', '$tot_pax']}]}]},

                          }

        }
        ,
        {
            '$project':
                {
                    '_id': 0,
                    'Agent_name': '$Agent_Name',
                    'PI': '$PI',
                    # 'Tot_pax':'$Tot_Pax',
                    # 'Pax_host':'$Pax_host',
                    # 'entire_pax':'$tot_pax',
                    # 'entire_host_pax':'$tot_host_pax',
                    'Friend_or_Foe': {'$cond': {
                        'if': {'$gte': ['$PI', 0]}, 'then': 'Friend', 'else': 'Foe'}}
                }
        }
        ,
        {
            '$out': temp_collection_name
        }
    ]

    db.JUP_DB_Market_Share_Last.aggregate(ppl_agents_data,
                                          allowDiskUse=True)
    cursor_agent = db[temp_collection_name].find()
    data_agent = []
    for doc in cursor_agent:
        del doc['_id']
        data_agent.append(doc)
    db[temp_collection_name].drop()
    return data_agent


def gen_agent_incentives_rna(dict_scr_filter):
    """
    :param dict_scr_filter:
    :return:
    """
    agents_data = get_agent_data(dict_scr_filter)
    if len(agents_data) == 0:
        # no_data_error = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
        #                                        get_module_name(),
        #                                        get_arg_lists(inspect.currentframe()))
        # no_data_error_desc = ''.join(['Null Response obtained from Data Base'])
        # raise no_data_error_desc
        what = ''.join(['Distributor/Agents'])
        why = ''.join(['Review the incentives provided or to be provided for Agents'])
        status_quo = ''.join([])
        action = ''.join([])
    elif len(agents_data) == 1:
        #   1 POS,OD,Compartment Combination
        what = ''.join(['Distributor/Agents'])
        why = ''.join(['Review the incentives provided or to be provided for Agents'])
        status_quo = ''.join([])
        action = ''.join([])
    elif len(agents_data) > 1:
        #   Multiple pos,od,compartment combinations
        what = ''.join(['Distributor/Agents'])
        why = ''.join(['Review the incentives provided or to be provided for Agents'])
        status_quo = ''.join([])
        action = ''.join([])
    response = dict(what=what,
                    why=why,
                    status_quo=status_quo,
                    action=action)
    return response

a = {
        'region': [],
        'country': [],
        'pos': [],
        'origin': [],
        'destination': [],
        'compartment': [],
        'fromDate': '2016-09-01',
        'toDate': '2016-12-31'
    }
k = gen_agent_incentives_rna(a)
print json.dumps(k, indent=1)