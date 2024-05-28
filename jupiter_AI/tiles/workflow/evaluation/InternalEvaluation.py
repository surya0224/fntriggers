"""
Author              - Sai Krishna
Completed Dates     -
    first version - 1.0
Calculation of the internal evaluation of
"""


class InternalEvaluation(object):
    """
    Obtaining the Internal Evaluation status of the fare.
    """
    def __init__(self, recommendation):
        self.reco = recommendation
        self.value = None

    def get_data(self
                 # ,
                 # book_date_start,
                 #  book_date_end,
                 #  dep_date_start,
                 #  dep_date_end
                 ):
        """
        """
        """
        self.pax_perc_change
        self.rev_perc_change
        """
        # obj = DataLevel(data={'pos': self.reco['pos'],
        #                       'origin': self.reco['origin'],
        #                       'destination': self.reco['destination'],
        #                       'compartment': self.reco['compartment']},
        #                 system_date=SYSTEM_DATE)
        # pax_data = obj.get_sales_flown_data(book_date_start=book_date_start,
        #                                     book_date_end=book_date_end,
        #                                     dep_date_start=dep_date_start,
        #                                     dep_date_end=dep_date_end,
        #                                     parameter='pax')
        #
        # revenue_data = obj.get_sales_flown_data(book_date_start=book_date_start,
        #                                         book_date_end=book_date_end,
        #                                         dep_date_start=dep_date_start,
        #                                         dep_date_end=dep_date_end,
        #                                         parameter='revenue')
        #
        # pax_target = obj.get_target_data(dep_date_start=dep_date_start,
        #                                  dep_date_end=dep_date_end,
        #                                  parameter='pax')
        #
        # revenue_target = obj.get_target_data(dep_date_start=dep_date_start,
        #                                      dep_date_end=dep_date_end,
        #                                      parameter='revenue')
        #
        # if type(pax_target) in [int,float] and type(pax_data['ty']) in [int, float] and pax_target > 0:
        #     self.pax_perc_change = round(((pax_data['ty'] - pax_target) * 100 / float(pax_target)), 2)
        # else:
        #     self.pax_perc_change = 'NA'
        #
        # if type(revenue_target) in [int, float] and type(revenue_data['ty']) in [int, float] and revenue_target > 0:
        #     self.rev_perc_change = round(((revenue_data['ty'] - revenue_target) * 100 / float(revenue_target)), 2)
        # else:
        #     self.rev_perc_change = 'NA'
        self.pax_perc_change = 3
        self.rev_perc_change = 6

    def get_tile_val(self):
        """
                    pax_vtgt < -5       -   0
                    -5 > pax_vtgt < 5   -   1
                    pax_vtgt > 5        -   2

                    revenue_vtgt < -5       -   0
                    -5 > revenue_vtgt < 5   -   1
                    revenue_vtgt > 5        -   2
        """
        self.get_data()
        # self.get_data(book_date_start=self.reco['triggering_data']['book_date_start'],
        #               book_date_end=self.reco['triggering_data']['book_date_end'],
        #               dep_date_start=self.reco['triggering_data']['dep_date_start'],
        #               dep_date_end=self.reco['triggering_data']['dep_date_end'])
        print 'pax percentage_change', self.pax_perc_change
        print 'revenue percentage change', self.rev_perc_change
        if type(self.pax_perc_change) in [int, float] and type(self.rev_perc_change) in [int, float]:
            if self.pax_perc_change <= -5:
                flag_pax = 0
            elif -5 < self.pax_perc_change <= 5:
                flag_pax = 1
            else:
                flag_pax = 2

            if self.rev_perc_change <= -5:
                flag_revenue = 0
            elif -5 < self.rev_perc_change <= 5:
                flag_revenue = 1
            else:
                flag_revenue = 2

            ref_table = [['', '', ''], ['', '', ''], ['', '', '']]
            #   X   -   pax flags
            #   Y   -   revenue flags
            ref_table[0][0] = 'Poor'
            ref_table[0][1] = 'Poor'
            ref_table[0][2] = 'Poor'
            ref_table[1][0] = 'Good'
            ref_table[1][1] = 'On Target'
            ref_table[1][2] = 'Marginal'
            ref_table[2][0] = 'Excellent'
            ref_table[2][1] = 'Good'
            ref_table[2][2] = 'Good'

            return ref_table[flag_pax][flag_revenue]
        else:
            return 'NA'

if __name__ == '__main__':
    #   TC1
    recommendation = {
        'old_doc_data': {
            'pos': 'DXB',
            'origin': 'DXB',
            'destination': 'DOH',
            'compartment': 'Y'
        },
        'triggering_data': {
            'dep_date_start': '2017-02-14',
            'dep_date_end': '2017-02-20',
            'book_date_start': '2017-01-01',
            'book_date_end': '2017-02-24'
        }
    }
    p = InternalEvaluation(recommendation=recommendation)
    print p.get_tile_val()
    print p.get_tile_val() == 'Poor'

    #   TC2
    recommendation = {
        'old_doc_data': {
            'pos': 'DXB',
            'origin': 'DXB',
            'destination': 'DOH',
            'compartment': 'Y'
        },
        'triggering_data': {
            'dep_date_start': '2018-02-14',
            'dep_date_end': '2018-02-20',
            'book_date_start': '2018-01-01',
            'book_date_end': '2018-02-24'
        }
    }
    p = InternalEvaluation(recommendation=recommendation)
    print p.get_tile_val()
    print p.get_tile_val() == 'NA'
