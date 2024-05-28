"""
File Name              :	Distributor_Agent_Fares_RnA
Author                 :	K Sai Krishna
Date Created           :	2016-12-19
Description            :	RnA analysis for review of Specific Fares for Distributors/Agents
Status                 :

MODIFICATIONS LOG	       :
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :
"""
import inspect
import json

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


def build_query_agent(dict_scr_filter):
    """
    Build the query for filtering fares from Market Share Agent Collection according to the filter
    :param dict_scr_filter:
    :return:
    """
    return dict()


def get_agent_data(dict_scr_filter):
    """

    :param dict_scr_filter:
    :return:
    """
    temp_collection_name = gen_collection_name()
    qry_agent_data = build_query_agent(dict_scr_filter)
    ppl_agents_data = [
        {
            '$out': temp_collection_name
        }
    ]
    # db.JUP_DB_Market_Share_Last.aggregate(ppl_agents_data,
    #                                       allowDiskUse=True)
    cursor_agent = db[temp_collection_name].find()
    data_agent = []
    for doc in cursor_agent:
        del doc['_id']
        data_agent.append(doc)
    db[temp_collection_name].drop()
    return data_agent


def gen_agent_fares_rna(dict_scr_filter):
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
        why = ''.join(['Review the Specific Fares provided or to be provided for Agents'])
        status_quo = ''.join([])
        action = ''.join([])
    elif len(agents_data) == 1:
        #   1 POS,OD,Compartment Combination
        what = ''.join(['Distributor/Agents'])
        why = ''.join(['Review the Specific Fares provided or to be provided for Agents'])
        status_quo = ''.join([])
        action = ''.join([])
    elif len(agents_data) > 1:
        #   Multiple pos,od,compartment combinations
        what = ''.join(['Distributor/Agents'])
        why = ''.join(['Review the Specific Fares provided or to be provided for Agents'])
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
k = gen_agent_fares_rna(a)
print json.dumps(k, indent=1)