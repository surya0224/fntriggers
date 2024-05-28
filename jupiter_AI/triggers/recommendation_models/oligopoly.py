import datetime


def get_price_recommendation(object_):
    '''
    This function contains the Logic used for recommendation
    Oligopoly Model
    Dynamic Pricing Models for Airline by RAMBO 2002 (pg.no/69-71)

    Takes host and competitor data as inputs
    Calculates the fare till 3 decimal points
    '''
    if object_.is_recommendation:
        h = object_.host_pricing_data
        c = object_.competitor_pricing_data
        print 'host doc', h
        print 'comp doc', c
        market_share_flag = True
        if not h['market_share']:
            market_share_flag = False
        for doc in c:
            if not doc['market_share']:
                market_share_flag = False
        if market_share_flag:
            compt = (h['total_fare'] - h['tax']) * (h['market_share']/float(100)) / float(h['rating'])
            print compt
            for idx in range(len(c)):
                compt_individual = ((c[idx]['market_share']/float(100)) * (c[idx]['total_price'] - c[idx]['tax'])) / c[idx]['rating']
                print 'ind_contri', compt_individual
                compt = compt + compt_individual
            print compt
            fare = compt * h['rating']
        else:
            compt = (h['total_fare'] - h['tax']) / float(h['rating'])
            for idx in range(len(c)):
                compt_individual = (c[idx]['total_price'] - c[idx]['tax']) / c[idx]['rating']
                compt = compt + compt_individual
            fare = compt * h['rating']

        object_.price_recommendation = fare

        # if market_share_flag:
        #     p = sum(i['market_share'] for i in c)
        #     compt = 0
        #     for i in range(len(c)):
        #         c[i]['market_share_host_excluded'] = c[i]['market_share'] / p
        #         compt_individual = (c[i]['market_share_host_excluded'] * (c[i]['total_price']) - c[i]['tax']) / c[i]['rating']
        #         compt = compt + compt_individual
        #         fare = compt * h['rating']
        # else:
        #     compt = 0
        #     for comp_fare in c:
        #         compt += comp_fare['price'] / comp_fare['rating']
        #     fare = compt * h['rating']

        # change = (fare - h['fare']) * 100 / h['fare']
        # if change > recommendation_upper_threshold:
        #     object_.price_recommendation = h['fare'] * (100 + recommendation_upper_threshold) / 100
        #     object_.calculated_recommendation = fare
        # elif change < recommendation_lower_threshold:
        #     object_.price_recommendation = h['fare'] * (100 + recommendation_lower_threshold) / 100
        #     object_.calculated_recommendation = fare
        # else:
        #     object_.price_recommendation = fare
        #     object_.calculated_recommendation = fare

        object_.recommended_fare = dict(
            total_fare=object_.price_recommendation + h['tax'],
            base_fare=object_.price_recommendation - h['yq'] - h['surcharges'],
            tax=h['tax'],
            yq=h['yq'],
            surcharges=h['surcharges']
        )

        object_.percent_change = 'null'
        object_.abs_change = 'null'
        object_.ref_farebasis = 'null'
        object_.process_end_date = datetime.datetime.now().strftime('%Y-%m-%d')
        object_.processed_end_time = datetime.datetime.now().strftime('%H:%M:%S.%f')
        object_.batch_price = 'null'
    else:
        pass


class var(object):
    pass

if __name__ == '__main__':
    h = {
        'total_fare': 1000,
        'base_fare': 700,
        'tax': 140,
        'yq': 100,
        'surcharges': 60,
        'market_share': 30,
        'rating': 6.6
    }

    c = [
        {
            'airline': 'EK',
            # 'total_fare': 1500,
            'total_price': 1200,
            'base_fare': 900,
            'tax': 250,
            'yq': 150,
            'surcharges': 200,
            'market_share': 35,
            'rating': 8
        }
        ,
        {
            'airline': 'EY',
            'total_price': 1200,
            # 'total_fare': 1200,
            'base_fare': 850,
            'tax': 200,
            'yq': 50,
            'surcharges': 100,
            'market_share': 35,
            'rating': 7
        }
    ]
    obj = var()
    var.host_pricing_data = h
    var.competitor_pricing_data = c
    var.is_recommendation = True
    get_price_recommendation(var)
    print var.recommended_fare
    print var.price_recommendation
