"""
File Name              :   RnA_PricingEfficiency_IneffectiveFares_DAL.py
Author                 :   Shamail Mulla
Date Created           :   2016-12-15
Description            :   RnA analyzes Pricing Efficiency by retreiving the number of inaffective fares
Status                 :

MODIFICATIONS LOG         
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :
"""


# Building query for retrieving fares for the particular filter
def query_builder(user_filter):
    qry_fares = dict()
    """
    qry_fares['$and'] = []
    today = str(date.today())
    if user_filter['region']:
        qry_fares['$and'].append({'region': {'$in': user_filter['region']}})
    if user_filter['country']:
        qry_fares['$and'].append({'country': {'$in': user_filter['country']}})
    if user_filter['pos']:
        qry_fares['$and'].append({'pos': {'$in': user_filter['pos']}})
    if user_filter['compartment']:
        qry_fares['$and'].append({'compartment': {'$in': user_filter['compartment']}})
	if user_filter['fromDate']:
        qry_fares['$and'].append({'effective_from': {'$in': user_filter['fromDate']}})
	if user_filter['toDate']:
        qry_fares['$and'].append({'effective_to': {'$in': user_filter['toDate']}})
    if user_filter['origin'] and user_filter['destination']:
        qry_fares['$or'] = cf.get_od_list_leg_level(user_filter)

	qry_fares['$and'].append({'carrier': host})
	"""
    return qry_fares


def get_inaffective_fares(user_filter):
# Filtering fares and retreiving inaffective fares
	qry_fares = query_builder(user_filter)
	"""
	ppln_retreive_fares = [
        {
            '$match': qry_fares
        }
        ,
		# Fetching fares from JUP_DB_ATPCO_Fares
		{
            '$group': 
			{
                '_id': 
				{
                    'pos': '$pos',
                    'od': '$od',
                    'compartment': '$compartment'
                }
                ,
                'avg_fares': {'$avg': '$price'}
            }
        }
        ,
		{
			'$project':
            {
				'_id.none': 1,
				'od':'$_id.od',
				'compartment': '$_id.compartment',
				'avg_fare':'$avg_fares'
             }
		}
		,
		# Fetching revenue and pax target data from JUP_DB_Target_OD
        {
            '$lookup':
                {
                    'from':'JUP_DB_Target_OD',
                    'localField':'od',
                    'foreignField':'od',
                    'as': 'target_collection'
                }
        }
		,
        # Converting target collection to a list
        {
            '$unwind': '$target_collection'
        }
        ,
        # Keeping dates where,
        # i) filter from date < effective to date
        # ii) filter to date > effective from date
        {
            '$redact':
                {
                    '$cond':
                        {
                            'if':
                                {'$and':
                                     [{'$lte':['$effective_from',filter['toDate']]},
                                      {'$gte':['effective_to',filter['fromDate']]}]},
                            'then': '$$DESCEND',
                            'else': '$$PRUNE'
                        }
                }
        }
        ,
        # Projecting data for required dates
        {
            '$project':
                {
                    '_id': 0,
                    'od': '$od',
                    'pos': '$pos',
                    'compartment': '$compartment',
                    'effective_from': '$effective_from',
                    'effective_to': '$effective_to',
                    'new_effective_from':
                        {
                            '$cond':
                                {
                                    'if': {'$lt': ['$effective_from', filter['fromDate']]},
                                    'then': filter['fromDate'],
                                    'else': '$effective_from'
                                }
                        },
                    'new_effective_to':
                        {
                            '$cond':
                                {
                                    'if': {'$gt': ['$effective_to', filter['toDate']]},
                                    'then': filter['toDate'],
                                    'else': '$effective_to'
                                }
                        },
                    'capacity': '$capacity',
                    'frequency': '$frequency',
                    'target_pax': 'target_collection.'
                }
        }
        ,
        # Fetching revenue data from
	]
	"""
        result = []
        return result

#def get_pricing_efficiency(user_filter):
#    return json.dumps(get_inaffective_fares(user_filter), indent=1)

