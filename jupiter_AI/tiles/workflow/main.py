"""
Author      - Sai Krishna
Description -
"""

from jupiter_AI.tiles.workflow.evaluation.ExternalEvaluation import ExternalEvaluation
from jupiter_AI.tiles.workflow.evaluation.InternalEvaluation import InternalEvaluation

from jupiter_AI.tiles.workflow.PricingActions import PricingActions
from jupiter_AI.tiles.workflow.PromosOffers import PromosAndOffers
from jupiter_AI.tiles.workflow.ExpectedOutcomes import ExpectedOutcomes
from jupiter_AI.tiles.workflow.ExpectedReactions import ExpectedReactions


from jupiter_AI.tiles.workflow.indices.CollaborationIndex import CollaborationIndex
from jupiter_AI.tiles.workflow.indices.MarketElasticity import MarketElasticity
from jupiter_AI.tiles.workflow.indices.PenetrationIndex import PenetrationIndex
from jupiter_AI.tiles.workflow.indices.PriceWarIndex import PriceWar
from jupiter_AI.tiles.workflow.indices.StabilityIndex import PriceStabilityIndex
from jupiter_AI.tiles.workflow.indices.VolatilityIndex import VolatilityFactor

from jupiter_AI.tiles.workflow.successrate.SuccessRate import HistoricalSuccessRate
from jupiter_AI.tiles.workflow.successrate.SuccessProbability import SuccessProbability


def calculate_workflow_tiles(recommendation):
    """
    Calculates all the workflow tiles by calling individual tile classes
          Days Ageing
            Current (int)
            Past (int)
          Evaluation
            External (str)
            Internal (str)
          Indices
            Volatility (%)
            Stabilty (%)
          Indices
            Collaboration
            Penetration
          Indices
            Price War (bool)(Yes/No)
            Market Elasticity (float)
    Arguments:
    recommendation - dict() - containing entire document of recommendation.
    returns response as a dict containing all tiles
    """
    response = dict()

    response['indices'] = dict()

    # INDICES MARKET ELASTICITY & PRICE WAR
    market_elasticity_obj = MarketElasticity(recommendation=recommendation)
    response['indices']['market_elasticity'] = market_elasticity_obj.get_market_elasticity()

    price_war_obj = PriceWar(recommendation=recommendation)
    response['indices']['price_war'] = price_war_obj.get_price_war_bool()

    # INDICES - PRICE STABILITY INDEX & VOLATILITY FACTOR
    stability_index_obj = PriceStabilityIndex(recommendation=recommendation)
    response['indices']['price_stability_index'] = stability_index_obj.get_psi_value()

    volatility_factor_obj = VolatilityFactor(recommendation=recommendation)
    volatiltiy_val = volatility_factor_obj.get_volatility_val()
    response['indices']['volatility_factor'] = volatiltiy_val

    # INDICES COLLABORATION INDEX & MARKET PENETRATION INDEX
    collaboration_index_obj = CollaborationIndex(recommendation=recommendation)
    colaboration_index_val = collaboration_index_obj.get_collaboration_index_val()
    response['indices']['collaboration_index'] = colaboration_index_val

    market_penetration_index_obj = PenetrationIndex(recommendation=recommendation)
    penetration_index_val = market_penetration_index_obj.get_penetration_index_val()
    response['indices']['penetration_index'] = penetration_index_val

    # PRICING ACTIONS
    pricing_actions_obj = PricingActions(recommendation=recommendation)
    pricing_actions_val = pricing_actions_obj.get_pricing_actions()
    response['pricing_actions'] = pricing_actions_val

    # EVALUATION EXTERNAL and INTERNAL
    internal_eval_obj = InternalEvaluation(recommendation)
    internal_eval_val = internal_eval_obj.get_tile_val()
    response['internal_evaluation_index'] = internal_eval_val

    external_eval_obj = ExternalEvaluation(recommendation)
    external_eval_val = external_eval_obj.get_tile_val()
    response['external_evaluation_index'] = external_eval_val

    # Success Rate present and Past
    response['success_rate'] = dict()
    success_rate_obj = HistoricalSuccessRate(recommendation=recommendation)
    success_rate_val = success_rate_obj.get_success_rate_val()
    response['success_rate']['past'] = success_rate_val

    success_prob_obj = SuccessProbability(recommendation=recommendation)
    success_prob_val = success_prob_obj.get_success_probability()
    response['success_rate']['current_probability'] = success_prob_val

    # Promos n Offers
    promos_offers_obj = PromosAndOffers(recommendation=recommendation)
    promos_offers_val = promos_offers_obj.get_promos_and_offers_val()
    response['promos'] = promos_offers_val

    # Expected Outcomes predictions
    expected_outcomes_obj = ExpectedOutcomes(recommendation=recommendation)
    expected_outcomes_val = expected_outcomes_obj.get_expected_outcomes()
    response['expected_outcomes_perc_chng'] = expected_outcomes_val

    # Expected Reactions
    expected_reactions_obj = ExpectedReactions(recommendation=recommendation)
    expected_reactions_val = expected_reactions_obj.get_top5_comp_reactions()
    response['expected_reactions'] = expected_reactions_val

    # Timing
    # Days Ageing Current, Past for an OD

    return response

if __name__ == '__main__':
    recommendation = {
        'region': 'GCC',
        'country': 'QA',
        'pos': 'DOH',
        'origin': 'DOH',
        'destination': 'BOM',
        'compartment': 'Y',
        'host_pricing_data': {
            'total_fare': 450,
            'market_share': 23,
            'rbd': 'L'
        }
        ,
        'price_recommendation': 500,
        'triggering_data': {
            'dep_date_start': '2016-01-01',
            'dep_date_end': '2017-12-31'
        }
    }
    print calculate_workflow_tiles(recommendation)

# class Tile(object):
#     def __init__(self, recommendation):
#         self.reco = recommendation
#         self.value = None
#
#     def build_query(self):
#         pass
#
#     def generate_ppln(self):
#         pass
#
#     def get_data(self):
#         pass
#
#     def get_tile_val(self):
#         pass
#
#     def __str__(self):
#         print self.value


# class VolatilityFactor(Tile):
# recommendation = {
#     "origin" : "DXB",
#     "pos" : "DOH",
#     "destination" : "DOH",
#     "compartment" : "Y",
#     "country" : "QA",
#     "region" : "GCC",
#     "host_pricing_data": {
#         'departure_date_from': '2016-07-01',
#         'departure_date_to': '2016-12-31'
#         'fare': 1000,
#     },
#     "price_recommendation": 1500}
# print recommendation
# print calculate_workflow_tiles(recommendation)
