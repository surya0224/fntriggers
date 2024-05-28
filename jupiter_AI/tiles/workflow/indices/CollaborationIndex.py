"""
File Name              :   CollaborationIndex.py
Author                 :   Sai Krishna
Date Created           :   2017-03-09
Description            :
Modification Log       :
"""
'''
    RBD which we are recommending
    Availability of an RBD
    For the period that we calculate in Ageing
'''


class CollaborationIndex(object):
    """
    """
    def __init__(self, recommendation):
        """
        """
        self.reco = recommendation

    def get_collaboration_index_val(self):
        """
        :return:
        """
        rbd = self.reco['host_pricing_data']['rbd']
        return 70