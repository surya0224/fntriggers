"""
File Name              : JUP_DB_Batch_Categorize_Recommendation
Author                 : Sai Krishna
Date Created           : 2017-03-06
Description            : Using the CategorizeRecommendation.py code in jupiter_AI/triggers/
                         Batch program runs (probably everyday) to  categorize the recommendations(docs in
                         JUP_DB_Cum_Pricing_Actions collection) and categorizes them into categories of A,B,C.
MODIFICATIONS LOG
    S.No                   :
    Date Modified          :
    By                     :
    Modification Details   :
"""
from collections import defaultdict
from jupiter_AI import client, JUPITER_LOGGER
from jupiter_AI.logutils import measure
from jupiter_AI.network_level_params import JUPITER_DB
db = client[JUPITER_DB]
from jupiter_AI.triggers.CategorizeRecommendation import Category


@measure(JUPITER_LOGGER)
def qry_cum_pricing_actions():
    """
    :return:
    """
    qry_cum_pa = defaultdict(list)
    # qry_cum_pa['$and'].append({
    #     'status': {'$nin': ['accepted', 'approved', 'accept', None]}
    # })
    qry_cum_pa['$and'].append({
        'price_recommendation': {
            '$ne': None
        }
    })
    return dict(qry_cum_pa)


@measure(JUPITER_LOGGER)
def main():
    """
    :return:
    """
    qry_pa = qry_cum_pricing_actions()
    pa_crsr = db.JUP_DB_Workflow.find(qry_pa)
    lst_pa = list(pa_crsr)
    print lst_pa
    for doc in lst_pa:
        id = doc['_id']
        obj = Category(recommendation=doc)
        obj.do_analysis()
        obj_dict = obj.__dict__
        del obj_dict['reco']
        print obj_dict
        db.JUP_DB_Workflow.update_one(
            {
                '_id': id
            }
            ,
            {
                '$set':
                    {
                        'recommendation_category': obj_dict['category'],
                        'recommendation_category_details': obj_dict
                    }
            }
        )


main()
