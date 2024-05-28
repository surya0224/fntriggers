"""
"""
import datetime


def get_ly_val(date_str, method='exact', year_diff=-1):
    """
    :param date_str:
    :param method:
    :return:
    """
    assert type(year_diff) in [int],'Invalid Input Type(year_diff) ::Expected int obtained ' + str(type(year_diff))
    assert method == 'exact' or method == '-364', 'Invalid Input(method param) :: Expected exact or -364 obtained ' + str(method)
    assert date_str, 'Date should not be None'
    if date_str:
        try:
            date_str_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError as error:
            raise error
        if method == 'exact':
            ly_str_obj = date_str_obj.replace(year=date_str_obj.year + year_diff)
            ly_date_str = datetime.datetime.strftime(ly_str_obj, '%Y-%m-%d')
            return ly_date_str
        elif method == '-364':
            ly_date_obj = date_str_obj + datetime.timedelta(days=(364*year_diff))
            ly_date_str = datetime.datetime.strftime(ly_date_obj, '%Y-%m-%d')
            return ly_date_str


def get_ly_dates(sale_dates, dep_dates, method='exact'):
    """
    :param sale_dates: sale_dates['from']
                       sale_dates['to']
    :param dep_dates: dep_date['from']
                      dep_date['to']
    :param method: 'exact' represents last yr val = this yr val - 365
                   '-364'  represents last yr val = this yr val - 364
    :return: dict(
        sale_dates = dict(
            from=''
            to=''
        ),
        dep_dates = dict(
            from=''
            to=''
        )
    )
    """
    response = dict(
        sale_dates={
            'from': '',
            'to': ''
        },
        dep_dates={
            'from': '',
            'to': ''
        }
    )

    assert sale_dates, 'None input given for sale dates'
    assert dep_dates, 'None input given for dep_dates'
    try:
        sale_dates['from']
        sale_dates['to']
        dep_dates['from']
        dep_dates['to']
    except KeyError:
        raise KeyError
    response['sale_dates']['from'] = get_ly_val(date_str=sale_dates['from'], method=method)
    response['sale_dates']['to'] = get_ly_val(date_str=sale_dates['to'], method=method)
    response['dep_dates']['from'] = get_ly_val(date_str=sale_dates['from'], method=method)
    response['dep_dates']['to'] = get_ly_val(date_str=sale_dates['to'], method=method)

    return response


if __name__ == '__main__':
    print get_ly_dates(sale_dates={'from': '2017-05-01', 'to': '2017-05-01'}, dep_dates={'from': '2017-05-01', 'to': '2017-05-01'})
