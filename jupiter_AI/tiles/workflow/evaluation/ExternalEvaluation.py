"""
Author - Sai Krishna
Completed Dates -
    first version -
Notes -
Not yet done
what are del ms and del f fm
"""


class ExternalEvaluation(object):
    """
    """
    def __init__(self, recommendation):
        """

        :param recommendation:
        """
        self.reco = recommendation

    def get_data(self):
        """

        # :param book_start_date:
        # :param book_end_date:
        # :param dep_start_date:
        # :param dep_end_date:
        :return:
        """
        #   change in market share
        #   change in del fare change

        self.ms_perc_change = 6
        self.del_f = 50
        self.mrkt_avg_fare_chnage = 70

        self.del_f_fm = self.del_f / float(self.mrkt_avg_fare_chnage)

    def get_tile_val(self):
        """
                    del_f/Fm = del_fare_chng
                    del_ms
                    del_fare_chng < -5       -   0
                    -5 > del_fare_chng < 5   -   1
                    del_fare_chng > 5        -   2

                    del_ms < -5       -   0
                    -5 > del_ms < 5   -   1
                    del_ms > 5        -   2
                """
        self.get_data()
        if self.del_f_fm <= -5:
            flag_fare_chng = 0
        elif -5 < self.del_f_fm <= 5:
            flag_fare_chng = 1
        else:
            flag_fare_chng = 2

        if self.ms_perc_change <= -5:
            flag_ms = 0
        elif -5 < self.ms_perc_change <= 5:
            flag_ms = 1
        else:
            flag_ms = 2

        ref_table = [['', '', ''], ['', '', ''], ['', '', '']]
        #   X   -   pax flags
        #   Y   -   revenue flags
        ref_table[0][0] = 'Average'
        ref_table[0][1] = 'Good'
        ref_table[0][2] = 'Excellent'
        ref_table[1][0] = 'Poor'
        ref_table[1][1] = 'On Target'
        ref_table[1][2] = 'Marginal'
        ref_table[2][0] = 'Poor'
        ref_table[2][1] = 'Below Average'
        ref_table[2][2] = 'On Target'

        return ref_table[flag_fare_chng][flag_ms]

if __name__ == '__main__':
    reco = {}
    obj = ExternalEvaluation(recommendation=reco)
    print obj.get_tile_val()