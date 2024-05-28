from copy import deepcopy


def tweak_filter(filter_from_UI):
    response = deepcopy(filter_from_UI)
    od_list = response['od']
    del response['od']
    response['origin'] = [od[:3] for od in od_list]
    response['destination'] = [od[3:] for od in od_list]
    return response


if __name__=='__main__':
    a = {
        'region': [],
        'country': [],
        'pos': [],
        'od': ["DOHDXB"],
        'compartment': [],
        "toDate": "2017-12-31",
        "fromDate": "2016-07-01"
    }
    print tweak_filter(a)