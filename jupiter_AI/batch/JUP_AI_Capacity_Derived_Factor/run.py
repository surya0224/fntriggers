"""
File Name              :   __init__.py
Author                 :   Shamail Mulla
Date Created           :   2017-04-04
Description            :  Calculates derived factor for every origin-destination combination in the hierarchy and stores
                            with priority level.
"""

from Derived_Factor_Competitor_ACR import capacity_influence as comp_df
from Derived_Factor_Host_ACR import capacity_influence_host as host_df
from Derived_Factor_Network import capacity_influence_destination_network as dest_nw_df, capacity_influence_origin_network as o_nw_df, capacity_influence_od_network as od_nw_df
import time
aggregation_level = ['A', 'C', 'R']

for origin_level in aggregation_level:
    print origin_level
    for destination_level in aggregation_level:
	print destination_level
        st=time.time()
        print st
        comp_df(o_level=origin_level, d_level=destination_level)
        print time.time() - st
        host_df(o_level=origin_level, d_level=destination_level)
        print time.time() - st
#for level in aggregation_level:
#    o_nw_df(level)

#for level in aggregation_level:
#    dest_nw_df(level)

#od_nw_df()
