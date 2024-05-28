"""
File Name              :   dashboard.py
Author                 :   Sai Krishna
Date Created           :   2016-12-19
Description            :   module with methods to calculate tiles for KPI dashboard
MODIFICATIONS LOG      :
    S.No               :
    Date Modified      :
    By                 :
    Modification Details   :
"""

import json
import os
import sys
import time

root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),os.path.pardir, os.path.pardir,os.path.pardir))
sys.path.append(root_dir)
from jupiter_AI.tiles.kpi.significant_non_significant_od import get_significant_non_significant_ods_user
from jupiter_AI.tiles.kpi.effective_ineffective_fares import get_effective_ineffective_fares as effective_ineffective_fares
from jupiter_AI.tiles.price_biometrics.price_elasticity import pe_signal
from jupiter_AI.tiles.kpi.revenue_split import get_revenue_split_compartment
from jupiter_AI.tiles.competitor_analysis.dashboard import get_price_intelligence_quotient
from jupiter_AI.tiles.kpi.yield_rasm_seatfactor import get_tiles as yield_rasm_seatfactor
from jupiter_AI.tiles.kpi.new_products import get_new_products
from jupiter_AI.tiles.kpi.price_agility_index import get_price_agility_index


def get_tiles(filter_kpi_dhb):
    response = dict()
    response['significant_non_significant'] = get_significant_non_significant_ods_user(filter_kpi_dhb)
    response['revenue_split'] = get_revenue_split_compartment(filter_kpi_dhb)
    response['new_products'] = get_new_products(filter_kpi_dhb)
    response['effective_ineffective_fares'] = effective_ineffective_fares(filter_kpi_dhb)
    response['price_intelligence_quotient'] = get_price_intelligence_quotient(filter_kpi_dhb)
    response['yield_rasm_seatfactor'] = yield_rasm_seatfactor(filter_kpi_dhb)
    response['price_agility_index'] = get_price_agility_index(filter_kpi_dhb)
    response['break_even_seat_factor'] = 'NA'
    response['price_elasticity'] = pe_signal(filter_kpi_dhb)
    return response


if __name__ == '__main__':
    st = time.time()
    filter_kpi = {"origin": [],"destination": [], "toDate": "2017-01-17", "compartment": [], "fromDate": "2017-01-01", "country": [], "region": [], "pos": []}
    print get_tiles(filter_kpi_dhb=filter_kpi)
    print time.time() - st
