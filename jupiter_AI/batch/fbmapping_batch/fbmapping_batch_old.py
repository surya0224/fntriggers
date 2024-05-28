from collections import defaultdict
import inspect
from jupiter_AI.common import ClassErrorObject as errorClass
from jupiter_AI.common.convert_currency import convert_currency
from jupiter_AI.network_level_params import *
#from jupiter_AI.triggers.GetInfareFare import *
from min_to_min import *
from jupiter_AI.batch.fbmapping_batch.fbmapping_optimizer import optimizefareladder
from jupiter_AI import client, JUPITER_DB, SYSTEM_DATE, JUPITER_LOGGER
from jupiter_AI.logutils import measure
#db = client[JUPITER_DB]


@measure(JUPITER_LOGGER)
def get_module_name():
    """
    FUnction used to get the module name where it is called
    """
    return inspect.stack()[1][3]


@measure(JUPITER_LOGGER)
def get_arg_lists(frame):
    """
    function used to get the list of arguments of the function
    where it is called
    """
    args, _, _, values = inspect.getargvalues(frame)
    argument_name_list = []
    argument_value_list = []
    for k in args:
        argument_name_list.append(k)
        argument_value_list.append(values[k])
    return argument_name_list, argument_value_list


@measure(JUPITER_LOGGER)
def get_host_fareladder(pos, origin, destination, compartment, oneway_return, dep_date_start=None, dep_date_end=None, rbd_type=None, currency=None, rules=False, footnote=False):
    """
    Function defined for obtaining the fareladder at compartment level combination
    Args:net
        pos(str)			:point of sale code
        origin(str)			:origin code
        destination(str) 	:destination code
        compartment(str)	:class code
    Returns:
        host_fbs(list)		:list of all the farebasis defined under this combination
        host_fl(list)		:list of all the fares(prices) for the above farebasis list
                             Fareladder for host in the above combination of pos,origin,destination,compartment
    """
    print pos, origin, destination, compartment, oneway_return, dep_date_start, dep_date_end, rbd_type
    fare_query = defaultdict(list)
    dep_query = defaultdict(list)
    if not dep_date_start:
        dep_date_start = SYSTEM_DATE # '2017-05-10'
    if not dep_date_end:
        dep_date_end = INF_DATE_STR # '9999-12-31'
    if dep_date_start > dep_date_end:
        print 'ERROR'

    fare_query = defaultdict(list)
    fare_query['$and'].append({'fare':{'$nin':[None,0.0,0]}})
    fare_query['$and'].append({'carrier': Host_Airline_Code})
    if pos:
        fare_query['$and'].append({'pos': pos})
    if origin:
        fare_query['$and'].append({'origin': origin})
    if destination:
        fare_query['$and'].append({'destination': destination})
    if compartment:
        fare_query['$and'].append({'compartment': compartment})
    if oneway_return:
        if oneway_return !=2:
            fare_query['$and'].append({'oneway_return': {'$in': [1, 3]}})
        else:
            fare_query['$and'].append({'oneway_return': oneway_return})
    # if rbd_type:
    #     fare_query['$and'].append({'RBD_type': rbd_type})
    if currency:
        fare_query['$and'].append({'currency': currency})
    fare_query['$and'].append(
        {
            '$or':
                [
                    {'effective_from': None},
                    {'effective_from': {'$lte': SYSTEM_DATE}}
                ]
        }
    )

    fare_query['$and'].append(
        {
            '$or':
                [
                    {'effective_to': None},
                    {'effective_to': {'$gte': SYSTEM_DATE}}
                ]
        }
    )

    dep_query['$or'].append({
        '$and': [
            {'dep_date_from': {'$lte': dep_date_end}},
            {'dep_date_end': {'$gte': dep_date_start}}
        ]
    })
    dep_query['$or'].append({
        '$and': [
            {'dep_date_from': {'$lte': dep_date_end}},
            {'dep_date_end': None}
        ]
    })
    dep_query['$or'].append({
        '$and': [
            {'dep_date_from': None},
            {'dep_date_end': {'$gte': dep_date_start}}
        ]
    })
    dep_query['$or'].append({
        '$and': [
            {'dep_date_from': None},
            {'dep_date_end': None}
        ]
    })

    fare_query['$and'].append(dict(dep_query))

    print dict(fare_query)

    host_fbs = []
    host_fl = []
    host_rules = []
    host_footnotes = []
    # d = datetime.datetime.today()
    # today = datetime.datetime.strftime(d, '%Y-%m-%d')
    host_data = db.JUP_DB_ATPCO_Fares.aggregate(
        [
            {
                '$match': dict(fare_query)
            }
        ]
    )
    host_data = list(host_data)
    print len(host_data)
    if len(host_data) > 0:
        for i in host_data:
            host_fbs.append(i['fare_basis'])
            host_footnotes.append(i['footnote'])
            host_rules.append(i['Rule_id'])
            print i['total_fare']
            print i['currency']
            # price = convert_currency(i['fare'], i['currency'])
            # print price
            if i['taxes']:
                host_fl.append(i['total_fare'] - i['taxes'])
            else:
                host_fl.append(i['total_fare'])
        if rules and footnote:
            return host_fbs, host_fl, host_rules, host_footnotes
        else:
            return host_fbs, host_fl
    else:
        e1 = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                    get_module_name(),
                                    get_arg_lists(inspect.currentframe()))
        e1.append_to_error_list("".join(["No Documents for data in Host for origin ",
                                         str(origin), ",destination ", str(destination),
                                         " and compartment ", str(compartment)]))
        raise e1


@measure(JUPITER_LOGGER)
def get_competitor_fareladder(source,
                              airline,
                              pos,
                              origin,
                              destination,
                              compartment,
                              oneway_return,
                              host_currency,
                              dep_date_start,
                              dep_date_end,
                              rbd_type=None):
    """
    Function defined to get the fareladder for competitor at compartment level combination
    Args:
        pos(str)			:point of sale code
        origin(str)			:origin code
        destination(str) 	:destination code
        compartment(str)	:class code
        airline(str)        :airline code for competitor
    Returns:
        host_fbs(list)		:list of all the farebasis defined under this combination
        host_fl(list)		:list of all the fares(prices) for the above farebasis list
                             Fareladder for host in the above combination of pos,origin,destination,compartment
    """
    if source == 'infare':
        competitor_fbs = []
        competitor_fl = []
        # d = datetime.datetime.today()
        # today = datetime.datetime.strftime(d, '%Y-%m-%d')
        print airline, pos, origin, destination, compartment, oneway_return, dep_date_start, dep_date_end, rbd_type
        competitor_data = get_valid_infare_fare(airline=airline,
                                                # pos=pos,
                                                origin=origin,
                                                destination=destination,
                                                compartment=compartment,
                                                oneway_return=oneway_return,
                                                dep_date_start=dep_date_start,
                                                dep_date_end=dep_date_end)
        print competitor_data
        data = []
        if competitor_data:
            if competitor_data['base_price'] and competitor_data['base_price'] != 'NA':
                data.append(competitor_data)
                competitor_data = data
        else:
            competitor_data = []
        print competitor_data
        if len(competitor_data) != 0:
            for i in competitor_data:
                competitor_fbs.append(i['farebasis'])
                price = convert_currency(value=i['base_price'],
                                         from_code=i['currency'],
                                         to_code=host_currency)
                competitor_fl.append(price)
            return competitor_fbs, competitor_fl
        else:
            e1 = errorClass.ErrorObject(errorClass.ErrorObject.WARNING,
                                        get_module_name(),
                                        get_arg_lists(inspect.currentframe()))
            e1.append_to_error_list("".join(["No Documents for data in Competitor for airline ",
                                             str(airline), ",origin ", str(origin),
                                             ",destination ", str(destination), " and compartment ",
                                             str(compartment)]))
            raise e1
    elif source == 'atpco':
        print pos, origin, destination, compartment, oneway_return, dep_date_start, dep_date_end, rbd_type
        fare_query = defaultdict(list)
        dep_query = defaultdict(list)
        if not dep_date_start:
            dep_date_start = SYSTEM_DATE
        if not dep_date_end:
            dep_date_end = INF_DATE_STR
        if dep_date_start > dep_date_end:
            print 'ERROR'

        fare_query = defaultdict(list)
        fare_query['$and'].append({'fare': {'$nin': [None, 0.0, 0]}})
        fare_query['$and'].append({'carrier': airline})
        if pos:
            fare_query['$and'].append({'pos': pos})
        if origin:
            fare_query['$and'].append({'origin': origin})
        if destination:
            fare_query['$and'].append({'destination': destination})
        if compartment:
            fare_query['$and'].append({'compartment': compartment})
        if oneway_return:
            if oneway_return != 2:
                fare_query['$and'].append({'oneway_return': {'$in': [1, 3]}})
            fare_query['$and'].append({'oneway_return': oneway_return})
        if rbd_type:
            fare_query['$and'].append({'RBD_type': {'$in':[ rbd_type, None]}})
        if host_currency:
            fare_query['$and'].append({'currency': host_currency})

        fare_query['$and'].append(
            {
                '$or':
                    [
                        {'effective_from': None},
                        {'effective_from': {'$lte': SYSTEM_DATE}}
                    ]
            }
        )

        fare_query['$and'].append(
            {
                '$or':
                    [
                        {'effective_to': None},
                        {'effective_to': {'$gte': SYSTEM_DATE}}
                    ]
            }
        )

        dep_query['$or'].append({
            '$and': [
                {'dep_date_from': {'$lte': dep_date_end}},
                {'dep_date_end': {'$gte': dep_date_start}}
            ]
        })
        dep_query['$or'].append({
            '$and': [
                {'dep_date_from': {'$lte': dep_date_end}},
                {'dep_date_end': None}
            ]
        })
        dep_query['$or'].append({
            '$and': [
                {'dep_date_from': None},
                {'dep_date_end': {'$gte': dep_date_start}}
            ]
        })
        dep_query['$or'].append({
            '$and': [
                {'dep_date_from': None},
                {'dep_date_end': None}
            ]
        })

        fare_query['$and'].append(dict(dep_query))

        print dict(fare_query)

        competitor_fbs = []
        competitor_fl = []
        # d = datetime.datetime.today()
        # today = datetime.datetime.strftime(d, '%Y-%m-%d')
        comp_crsr = db.JUP_DB_ATPCO_Fares.aggregate(
            [
                {
                    '$match': dict(fare_query)
                }
            ]
        )

        comp_data = list(comp_crsr)
        print len(comp_data)
        if len(comp_data) > 0:
            for i in comp_data:
                competitor_fbs.append(i['fare_basis'])
                print i['total_fare']
                print i['currency']
                # price = convert_currency(i['fare'], i['currency'])
                # print price
                if i['taxes']:
                    competitor_fl.append(i['total_fare'] - i['taxes'])
                else:
                    competitor_fl.append(i['total_fare'])
            return competitor_fbs, competitor_fl
        else:
            e1 = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                        get_module_name(),
                                        get_arg_lists(inspect.currentframe()))
            e1.append_to_error_list("".join(["No Fares for ",str(airline)," for origin ",
                                             str(origin), ",destination ", str(destination),
                                             " and compartment ", str(compartment)," in dB"]))
            # raise e1
        return competitor_fbs, competitor_fl
    else:
        error = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                       get_module_name(),
                                       get_arg_lists(inspect.currentframe()))
        error.append_to_error_list("INVALID INPUT - source :: expected either 'atpco' or 'infare' but obtained " + str(source) + ".")
        raise error


@measure(JUPITER_LOGGER)
def get_host_combinations(market_query, dep_date_start, dep_date_end):
    """
    Function to get the unique combinations of pos,origin,destination,compartment,oneway_return,channel,currency
    from host fares collection b/w the departure dates.
    Args:
        dep_date_start : start date of the departure date range
        dep_date_end   : end date of the departure date range
    dep         ded
    ---         ---
    val         val
    null        val
    val         null
    null        null
    """
    qry_fares = defaultdict(list)
    if not dep_date_start:
        dep_date_start = SYSTEM_DATE
    if not dep_date_end:
        dep_date_end = INF_DATE_STR
    if dep_date_start > dep_date_end:
        print 'ERROR'

    qry_fares['$or'].append({
        '$and': [
            {'dep_date_from': {'$lte': dep_date_end}},
            {'dep_date_end': {'$gte': dep_date_start}}
        ]
    })
    qry_fares['$or'].append({
        '$and': [
            {'dep_date_from': {'$lte': dep_date_end}},
            {'dep_date_end': None}
        ]
    })
    qry_fares['$or'].append({
        '$and': [
            {'dep_date_from': None},
            {'dep_date_end': {'$gte': dep_date_start}}
        ]
    })
    qry_fares['$or'].append({
        '$and': [
            {'dep_date_from': None},
            {'dep_date_end': None}
        ]
    })

    qry_fares['$and'].append(
        {
            '$or':
                [
                    {'effective_to': None},
                    {'effective_to': {'$gte': SYSTEM_DATE}}
                ]
        }
    )

    qry_fares.update(market_query)
    qry_fares['carrier'] = Host_Airline_Code

    print qry_fares
    fares_combinations_ppln = [
        {
            '$match': dict(qry_fares)
        }
        ,
        {
            '$group':
                {
                    '_id':
                        {
                            # 'pos': '$pos',
                            #  Removed POS as Fares are not defined at POS level
                            #  Fares are filed at an OD level.
                            'origin': '$origin',
                            'destination': '$destination',
                            'compartment': '$compartment',
                            'oneway_return': '$oneway_return',
                            'currency': '$currency'
                            # ,
                            # 'RBD_type': '$RBD_type'
                        }
                }
        }
    ]

    fares_combination_crsr = db['JUP_DB_ATPCO_Fares'].aggregate(fares_combinations_ppln)

    fares_combination_data = list()

    for combination in fares_combination_crsr:
        fares_combination_data.append(combination['_id'])

    return fares_combination_data


@measure(JUPITER_LOGGER)
def get_airlines_list(source, pos, origin, destination, compartment, dep_date_start, dep_date_end):
    """
    :param source:
    :param pos:
    :param origin:
    :param destination:
    :param compartment:
    :return:
    """
    if source == 'infare':
        infare_query = dict()
        infare_query['carrier'] = {'$ne': Host_Airline_Code}
        if pos:
            infare_query['pos'] = pos
        if origin:
            infare_query['origin'] = origin
        if destination:
            infare_query['destination'] = destination
        if compartment:
            infare_query['compartment'] = compartment
        airlines_list = db['JUP_DB_Infare_Fares'].distinct('carrier', infare_query)
        return airlines_list
    elif source == 'atpco':
        atpco_query = dict()
        atpco_query['carrier'] = {'$ne': Host_Airline_Code}
        if origin:
            atpco_query['origin'] = origin
        if destination:
            atpco_query['destination'] = destination
        if compartment:
            atpco_query['compartment'] = compartment
        airlines_list = db.JUP_DB_ATPCO_Fares.distinct('carrier', atpco_query)
        return airlines_list


@measure(JUPITER_LOGGER)
def map_fares(method='raw',
              dep_date_start=None,
              dep_date_end=None,
              comp_fare_source='atpco',
              market_query=dict()):
    """
    Main function where the entire process occurs
    Args:
        method(str)	: method used for doing the farebasis mapping
                      Default - 'mini'
                      Other values the method can take are 'med','raw','mintomin'
    Logic/Flow of Code:
        0   create a null dictionery 'mapped'
        1 	get all the combinations of pos,origin,destination,compartment of host
        2 	create null lists in mapped for each farebasis as key
            e.g. -
            mapped['FB1Ja'] = [] etc,.

        3 	get all the airlines defined for the host
        3 	for each combination obtained in step 1 obtain the host fareladder
            using the above defined function
        4 	for each combination obtained in step 2 ,for each airline obtained
            in step2 , obtain the competitor fareladder using the above defined
            function
        5 	Once both the fareladders are obtained use the fareladder mapping
            function 'optimizefareladder' defined in 'raw' module
        6   Using the output of the optimizefareladder function
            create a dictionery containing the airline and farebasis of competitor
            for which the host_farebasis is mapped and append it to mapped[host_farebasis]
            list
        7	Once the entire mapping is done
            get the list of farebasis of host using mapped.keys()
            and for each one of them query the farebasis collection and update the
            competitor_farebasis field to the obtained dictionery
            :param dep_date_start:
    :param method:
    :param dep_date_start:
    :param dep_date_end:
    :param comp_fare_source:
    :param market_query:
    :return:
    """
    # d = datetime.datetime.today()
    # today = datetime.datetime.strftime(d, '%Y-%m-%d')
    query = dict()
    if market_query:
        if market_query['pos']:
            query['pos'] = market_query['pos']
        if market_query['origin'] and market_query:
            query['origin'] = market_query['origin']
        if market_query['destination']:
            query['destination'] = market_query['destination']
        if market_query['compartment']:
            query['compartment'] = market_query['compartment']
    query['competitor_farebasis'] = None
    db.JUP_DB_ATPCO_Fares.update(query, {'$set': {'competitor_farebasis': None}}, multi=True)
    print 'NULL UPDATE DONE'
    mapped = dict()
    lst_host_combinations = get_host_combinations(market_query=query,
                                                  dep_date_start=dep_date_start,
                                                  dep_date_end=dep_date_end)

    print lst_host_combinations
    print 'NO OF HOST COMBINATIONS', len(lst_host_combinations)
    for host_combination in lst_host_combinations:
        mapped = defaultdict(list)
        host_fbs, host_fl, host_rules, host_fn = get_host_fareladder(
                                                pos=None,
                                                #   pos is inputed as None as fares are not filed for a pos level
                                                #   they are always filed at an OD level.
                                                origin=host_combination['origin'],
                                                destination=host_combination['destination'],
                                                compartment=host_combination['compartment'],
                                                oneway_return=host_combination['oneway_return'],
                                                dep_date_start=dep_date_start,
                                                dep_date_end=dep_date_end,
                                                # rbd_type=host_combination['RBD_type'],
                                                currency=host_combination['currency'],
                                                rules=True,
                                                footnote=True)

        if len(host_fbs) > 0:
            airlines = get_airlines_list(source=comp_fare_source,
                                         pos=None,
                                         #   pos is inputed as None as fares are not filed for a pos level
                                         #   they are always filed at an OD level.
                                         origin=host_combination['origin'],
                                         destination=host_combination['destination'],
                                         compartment=host_combination['compartment'],
                                         dep_date_start=dep_date_start,
                                         dep_date_end=dep_date_end)
            print 'List of Airlines', airlines
            if len(airlines) > 0:
                for airline in airlines:
                    comp_fbs, comp_fl = get_competitor_fareladder(source=comp_fare_source,
                                                                  airline=airline,
                                                                  pos=None,
                                                                  #   pos is inputed as None as fares are not
                                                                  #   filed for a pos level
                                                                  #   they are always filed at an OD level.
                                                                  origin=host_combination['origin'],
                                                                  destination=host_combination['destination'],
                                                                  compartment=host_combination['compartment'],
                                                                  host_currency=host_combination['currency'],
                                                                  oneway_return=host_combination['oneway_return'],
                                                                  dep_date_start=dep_date_start,
                                                                  dep_date_end=dep_date_end,
                                                                  # rbd_type=host_combination['RBD_type']
                                                                  )
                    if comp_fl and comp_fare_source=='atpco':
                        pass
                    # else:
                    #     comp_fbs, comp_fl = get_competitor_fareladder(source=comp_fare_source,
                    #                                                   airline=airline,
                    #                                                   pos=None,
                    #                                                   #   pos is inputed as None as fares are not
                    #                                                   #   filed for a pos level
                    #                                                   #   they are always filed at an OD level.
                    #                                                   origin=host_combination['origin'],
                    #                                                   destination=host_combination['destination'],
                    #                                                   compartment=host_combination['compartment'],
                    #                                                   host_currency=host_combination['currency'],
                    #                                                   oneway_return=host_combination['oneway_return'],
                    #                                                   dep_date_start=dep_date_start,
                    #                                                   dep_date_end=dep_date_end,
                    #                                                   # rbd_type=host_combination['RBD_type']
                    #                                                     )
                    if comp_fl:
                        print 'HOST FL', host_fl
                        print 'HOST FBS', host_fbs
                        print 'COMP FL', comp_fl
                        print 'COMP FBS', comp_fbs
                        if method == 'mintomin':
                            out = mintominfareladder(host_fl, comp_fl)
                        else:
                            out = optimizefareladder(method, host_fl, comp_fl)
                        print out
                        for index, item in enumerate(out[0]):
                            print 'INDEX', index
                            print 'ITEM', item
                            if item != -1:
                                mapped[(host_fbs[index],
                                        host_fl[index],
                                        host_rules[index],
                                        host_fn[index])].append({'airline': airline,
                                                                 'farebasis': comp_fbs[item],
                                                                 'source': comp_fare_source,
                                                                 'fare': comp_fl[item]})
            print 'MAPPED KEYS', mapped
            for l in mapped.keys():
                update_query = dict(
                    origin=host_combination['origin'],
                    destination=host_combination['destination'],
                    compartment=host_combination['compartment'],
                    currency=host_combination['currency'],
                    fare_basis=l[0],
                    carrier=Host_Airline_Code,
                    total_fare=l[1],
                    footnote=l[3],
                    Rule_id=l[2]
                )
                update_query['$and'] = [
                    {
                        '$or':
                            [
                                {'effective_from':None},
                                {'effective_from':{'$lte': SYSTEM_DATE}}
                            ]
                    }
                    ,
                    {
                        '$or':
                            [
                                {'effective_to': None},
                                {'effective_to': {'$gte': SYSTEM_DATE}}
                            ]
                    }
                ]
                print '************fb', l[0]
                # update_query = host_combination
                # print 'Update Query', update_query
                # update_query.update({
                #     'fare_basis': l[0],
                #     'carrier': Host_Airline_Code
                # })
                # del update_query['RBD_Type']
                print 'Update Query', update_query
                print db
                print 'NO OF DOCS BEING UPDATED', db.JUP_DB_ATPCO_Fares.find(dict(update_query)).count()
                db.JUP_DB_ATPCO_Fares.update(update_query,
                                             {'$set': {'competitor_farebasis': mapped[l]}})
        else:
            e1 = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                        get_module_name(),
                                        get_arg_lists(inspect.currentframe()))
            e1.append_to_error_list("".join(["No combinations of origin, destination",
                                             "compartment", " obtained  from Host Data"]))
            raise e1


if __name__ == '__main__':
    # map_fares(method='raw',
    #           dep_date_start=None,
    #           dep_date_end=None,
    #           comp_fare_source='atpco',
    #           market_query=dict(pos=None,
    #                             origin="DXB",
    #                             destination="AMM",
    #                             compartment='Y'))
    od = [
	"ADDBAH",
	"ADDDXB",
	"AHBDXB",
	"AMMDXB",
	"ASMDXB",
	"BAHDXB",
	"BEGDXB",
	"BEYDXB",
	"CGPDXB",
	"DACDXB",
	"DELDXB",
	"DOHDXB",
	"DXBADD",
	"DXBAHB",
	"DXBALA",
	"DXBAMM",
	"DXBASM",
	"DXBBAH",
	"DXBBEG",
	"DXBBEY",
	"DXBBGW",
	"DXBCGP",
	"DXBCMB",
	"DXBDEL",
	"DXBDOH",
	"DXBDYU",
	"DXBEBL",
	"DXBELQ",
	"DXBEVN",
	"DXBGYD",
	"DXBHYD",
	"DXBIEV",
	"DXBJED",
	"DXBJIB",
	"DXBJUB",
	"DXBKBL",
	"DXBKRT",
	"DXBKTM",
	"DXBMCT",
	"DXBMRV",
	"DXBMUX",
	"DXBNJF",
	"DXBOTP",
	"DXBPZU",
	"DXBSOF",
	"DXBSVX",
	"DXBTBS",
	"DXBTIF",
	"DXBZNZ",
	"DYUDXB",
	"EVNDXB",
	"GYDDXB",
	"HBEDXB",
	"HGADXB",
	"IEVDXB",
	"JEDDXB",
	"JIBDXB",
	"JUBDXB",
	"KBLDXB",
	"KRTDXB",
	"KTMDXB",
	"KWIADD",
	"KWIDXB",
	"MCTBEY",
	"MCTDXB",
	"NJFDXB",
	"ODSDXB",
	"OTPDXB",
	"PZUDXB",
	"SOFDXB",
	"SVXDXB",
	"TBSDXB",
	"AMMBAH",
	"DARDXB",
	"DXBCOK",
	"SLLDXB",
	"AJFDXB",
	"AMMDWC",
	"ASMRUH",
	"BAHAMM",
	"BAHDAC",
	"BEYYNB",
	"BGWDXB",
	"BOMDXB",
	"BOMJED",
	"CGPAHB",
	"CMBDXB",
	"COKDXB",
	"DACAHB",
	"DACDMM",
	"DACDOH",
	"DACELQ",
	"DACHAS",
	"DACJED",
	"DACRUH",
	"DMMDXB",
	"DMMKTM",
	"DOHDAC",
	"DOHKRT",
	"DWCAMM",
	"DXBASB",
	"DXBBND",
	"DXBBOM",
	"DXBBTS",
	"DXBDAC",
	"DXBDMM",
	"DXBEBB",
	"DXBFRU",
	"DXBGIZ",
	"DXBHAS",
	"DXBHBE",
	"DXBHOF",
	"DXBKHI",
	"DXBKWI",
	"DXBLYP",
	"DXBMAA",
	"DXBMED",
	"DXBMHD",
	"DXBMLE",
	"DXBRUH",
	"DXBSAW",
	"DXBSKT",
	"DXBSLL",
	"DXBTRV",
	"DXBTUU",
	"DXBUET",
	"DXBZYL",
	"EBBDXB",
	"ELQDXB",
	"ELQKHI",
	"ELQKRT",
	"FRUDXB",
	"GIZDXB",
	"HASDXB",
	"HOFDXB",
	"HYDDXB",
	"JEDBEY",
	"JEDBGW",
	"JEDHBE",
	"JEDKBL",
	"JEDKHI",
	"JEDLYP",
	"JEDMCT",
	"JEDMUX",
	"JEDSKT",
	"JEDTRV",
	"JEDUET",
	"KBLAHB",
	"KBLDMM",
	"KBLELQ",
	"KBLJED",
	"KBLMED",
	"KHIDXB",
	"KHIELQ",
	"KHIJED",
	"KHIMED",
	"KHITIF",
	"KRTDOH",
	"KTMRUH",
	"KWICMB",
	"KWIFRU",
	"KWIHBE",
	"KWIKTM",
	"KWIMLE",
	"KWISAW",
	"KWISJJ",
	"KWITBS",
	"LYPAHB",
	"LYPDXB",
	"LYPJED",
	"MAADXB",
	"MCTKRT",
	"MEDAMD",
	"MEDDXB",
	"MEDKBL",
	"MEDMUX",
	"MHDDXB",
	"MLEDXB",
	"MUXDXB",
	"MUXELQ",
	"MUXJED",
	"MUXTIF",
	"RUHDXB",
	"RUHKBL",
	"RUHKTM",
	"SAWDXB",
	"SJJDXB",
	"SJJKWI",
	"SKTAHB",
	"SKTDXB",
	"SKTELQ",
	"SKTJED",
	"TBSKWI",
	"TIFDXB",
	"TRVDXB",
	"TUUDXB",
	"UETJED",
	"YNBDXB",
	"DXBPRG",
	"MCTDOH",
	"ALADXB",
	"ASBDXB",
	"BAHGYD",
	"BNDDXB",
	"BSRDXB",
	"BTSDXB",
	"DWCDOH",
	"DXBBSR",
	"DXBDAR",
	"DXBIKA",
	"DXBKDH",
	"DXBKRR",
	"DXBKUF",
	"DXBKZN",
	"DXBLRR",
	"DXBODS",
	"DXBSJJ",
	"DXBSYZ",
	"DXBTSE",
	"DXBVKO",
	"EBLDXB",
	"GYDBAH",
	"IEVZNZ",
	"IFNDXB",
	"IKADXB",
	"KDHDXB",
	"KRRDXB",
	"KUFDXB",
	"LRRDXB",
	"MCTTBS",
	"MRVDXB",
	"PRGDXB",
	"ROVDXB",
	"TBSMCT",
	"TSEDXB",
	"UETDXB",
	"VKODXB",
	"ZNZDXB",
	"BEYDOH",
	"BEYJED",
	"DOHBEY",
	"BGWJED",
	"BOMNJF",
	"CGPMCT",
	"DOHKTM",
	"DWCKTM",
	"DXBSKP",
	"EVNKWI",
	"KRTMCT",
	"KTMDWC",
	"KWIEVN",
	"MCTAMM",
	"NJFJED",
	"SKPDXB",
	"BEYDWC",
	"BEYMCT",
	"BGWDAC",
	"DOHDWC",
	"DWCBEY",
	"DXBHGA",
	"DXBIFN",
	"KBLRUH",
	"KWIGYD",
	"KWIKRT",
	"MCTSAW",
	"SYZDXB",
	"KZNDXB",
	"ASMEBB",
	"AWZDXB",
	"BAHCGP",
	"BAHCOK",
	"BAHHYD",
	"BAHIKA",
	"BAHKTM",
	"BAHLYP",
	"BAHMHD",
	"BAHTBS",
	"BAHZYL",
	"CGPBAH",
	"CGPDOH",
	"CMBBEY",
	"CMBDOH",
	"DARMCT",
	"DOHMCT",
	"GYDKWI",
	"IEVCMB",
	"KRTKWI",
	"KTMBAH",
	"KTMDOH",
	"KTMKWI",
	"LYPMCT",
	"MCTBAH",
	"MCTKTM",
	"MCTMED",
	"MHDBAH",
	"NJFBAH",
	"SAWMCT",
	"SJJBAH",
	"TBSBAH",
	"KTMBEG",
	"BKKBEY",
	"BSRJED",
	"KWIDAC",
	"KWIIEV",
	"KWIKBL",
	"MCTDAC",
	"BSRDAC",
	"DELNJF",
	"LKONJF",
	"LYPBGW",
	"EBBMCT",
	"MCTEBB",
	"BKKDXB",
	"BKKKWI",
	"BKKMCT",
	"DXBBKK",
	"KWIBKK",
	"MCTBKK",
	"CMBPRG",
	"COKDOH",
	"IEVKTM",
	"BSRMED",
	"BKKBTS",
	"BTSCMB",
	"CMBBTS",
	"CGPKWI",
	"KWICGP",
	"IEVMLE",
	"ZYLBAH",
	"ZYLDOH",
	"ZYLDXB",
	"ZYLMCT",
	"MCTDAR",
	"ZNZMCT",
	"GYDMCT",
	"KWIMAA",
	"MCTGYD",
	"KHIRUH",
	"MCTMUX",
	"MUXMCT",
	"SKTMCT",
	"MLEIEV",
	"KBLKWI",
	"KBLHAS",
	"MCTSKT",
	"KHIGYD",
	"BKKIEV",
	"IEVBKK",
	"MCTSJJ",
	"SJJMCT",
	"MHDLYP",
	"DXBOAI",
	"OAIDXB",
	"BKKOTP",
	"BKKSLL"
    ]
    for od_val in od:
        map_fares(method='raw',
                  dep_date_start=None,
                  dep_date_end=None,
                  comp_fare_source='atpco',
                  market_query=dict(
                    pos=None,
                    origin=od_val[:3],
                    destination=od_val[3:],
                    compartment=None
                  ))

    # data = get_competitor_fareladder(source='atpco',
    #                                  airline='EK',
    #                                  pos='IST',
    #                                  origin='IST',
    #                                  destination='BKK',
    #                                  compartment=None,
    #                                  dep_date_start=None,
    #                                  dep_date_end=None,
    #                                  host_currency='AED',
    #                                  oneway_return=1)
    # print data
    # print get_competitor_fareladder(source='infare',
    #                                 airline='EK',
    #                                 host_currency='SAR',
    #                                 oneway_return=None,
    #                                 pos=None,
    #                                 origin='DXB',
    #                                 destination='DOH',
    #                                 compartment='Y',
    #                                 dep_date_start=None,
    #                                 dep_date_end=None,
    #                           # oneway_return=None
    #                         )

'''
    # query1 = []
    # cursor_combinations = db.JUP_DB_ATPCO_Fares.aggregate([
    #     # {
    #     #     '$match': {
    #     #         '$and': [
    #     #             {
    #     #                 '$or': [
    #     #                     {'effective_to': {'$eq':None}},
    #     #                     {'effective_to': {'$lte': SYSTEM_DATE}}
    #     #                 ]
    #     #             }
    #     #             ,
    #     #             {
    #     #                 '$or':[
    #     #                     {'effective_from': {'$eq':None}},
    #     #                     {'effective_from': {'$gte': SYSTEM_DATE}}
    #     #                 ]
    #     #             }
    #     #             ,
    #     #             {
    #     #                 '$or': [
    #     #                     {'dep_date_from': {'$eq': None}},
    #     #                     {'dep_date_from': {'$lte': dep_date_end}}
    #     #                 ]
    #     #             }
    #     #             ,
    #     #             {
    #     #                 '$or': [
    #     #                     {'dep_date_from': {'$eq': None}},
    #     #                     {'dep_date_to': {'$gte': dep_date_start}}
    #     #                 ]
    #     #             }
    #     #         ]
    #     #     }
    #     # },
    #     {'$group': {
    #         '_id': {
    #             'pos': '$pos',
    #             'origin': '$origin',
    #             'destination': '$destination',
    #             'compartment': '$compartment',
    #             'oneway_return': '$oneway_return',
    #             'channel': '$channel',
    #             'currency': '$currency'
    #         }
    #     }}
    #     # ,
    #     # { '$limit': 5}
    # ])
    #
    # for doc in cursor_combinations:
    #     query1.append(doc['_id'])
    # print query1
    # for i in query1:
    #     print i
    #     print i['channel']
    query2 = db.JUP_DB_Infare_Fares.distinct('carrier', {'carrier': {'$ne': Host_Airline_Code}})
    # query2 = ['EK']
    print len(query1)
    print len(query2)
    if len(query1) == 0:
        e1 = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                    get_module_name(),
                                    get_arg_lists(inspect.currentframe()))
        e1.append_to_error_list("".join(["No combinations of pos,origin, destination",
                                         "compartment", " obtained  from Host Data"]))
        raise e1
    else:
        pass

    if len(query2) == 0:
        e2 = errorClass.ErrorObject(errorClass.ErrorObject.ERRORLEVEL1,
                                    get_module_name(),
                                    get_arg_lists(inspect.currentframe()))
        e2.append_to_error_list("".join(["No Airlines defined in Competitor data"]))
        raise e2
    else:
        pass

    for i in query1:
        print i['channel']
        if i['channel'] == 'WEB':
            error_flag = False
            try:
                host_fbs, host_fl = get_host_fareladder(i['pos'], i['origin'], i['destination'], i['compartment'], oneway_return=i['oneway_return'])
                print 'host_fareladder',host_fl
            except errorClass.ErrorObject as esub:
                if esub.error_level <= errorClass.ErrorObject.WARNING:
                    error_flag = False
                elif esub.error_level <= errorClass.ErrorObject.ERRORLEVEL1:
                    error_flag = True
                elif esub.error_level <= errorClass.ErrorObject.ERRORLEVEL2:
                    error_flag = True
                    raise esub
                print esub
                # db.JUP_DB_Error_Collection.insert_one({'program': 'fbmapping_batch',
                #                                        'description': esub.__str__()})
            print error_flag
            if not error_flag:
                mapped = defaultdict(list)
                for j in query2:
                    comp_fbs = []
                    comp_fl = []
                    try:

                        comp_fbs, comp_fl = get_competitor_fareladder(j,
                                                                      i['pos'],
                                                                      i['origin'],
                                                                      i['destination'],
                                                                      i['compartment'],
                                                                      i['oneway_return'],
                                                                      i['currency'],
                                                                      dep_date_start=dep_date_start,
                                                                      dep_date_end=dep_date_end
                                                                      )
                        print 'competitor_fareladder',comp_fl
                    except errorClass.ErrorObject as esub:
                        if esub.error_level <= errorClass.ErrorObject.WARNING:
                            error_flag = False
                        elif esub.error_level <= errorClass.ErrorObject.ERRORLEVEL1:
                            error_flag = True
                        elif esub.error_level <= errorClass.ErrorObject.ERRORLEVEL2:
                            error_flag = True
                            raise esub
                        print esub
                        # db.JUP_DB_Error_Collection.insert_one({'program': 'fbmapping_batch',
                        #                                        'description': esub.__str__()})
                    if not error_flag:
                        if comp_fl:
                            print comp_fbs
                            print comp_fl
                            if method == 'mintomin':
                                out = mintominfareladder(host_fl, comp_fl)
                            else:
                                out = optimizefareladder(method, host_fl, comp_fl)
                                print out
                            for index, item in enumerate(out[0]):
                                if item != -1:
                                    mapped[(host_fbs[index],
                                            host_fl[index])].append({'airline': j,
                                                                             'farebasis': comp_fbs[item]})
            print mapped
            print error_flag
            if not error_flag:
                for l in mapped.keys():
                    crsr = db.JUP_DB_ATPCO_Fares.find({'carrier': Host_Airline_Code,
                                                       'pos': i['pos'],
                                                       'origin': i['origin'],
                                                       'destination': i['destination'],
                                                       'compartment': i['compartment'],
                                                       'effective_from': {'$lte': SYSTEM_DATE},
                                                       'effective_to': {'$gte': SYSTEM_DATE},
                                                       'fare_basis': l[0],
                                                       'total_fare': l[1]})
                    print 'HOST FARE UPDATING-----------------------------', crsr.count()
                    t = db.JUP_DB_ATPCO_Fares.update({'carrier': Host_Airline_Code,
                                                       'pos': i['pos'],
                                                       'origin': i['origin'],
                                                       'destination': i['destination'],
                                                       'compartment': i['compartment'],
                                                       'effective_from': {'$lte': SYSTEM_DATE},
                                                       'effective_to': {'$gte': SYSTEM_DATE},
                                                       'fare_basis': l[0],
                                                       'total_fare': l[1]},
                                                      {'$set': {'competitor_farebasis': mapped[l]}})
                    print 'Updated ID-----------------------------', t
                    pass



def main(dep_date_start,dep_date_end):
    try:
        try:
            # db.JUP_DB_Error_Collection.drop()
            # db.JUP_DB_ATPCO_Fares.update({}, {'$set': {'competitor_farebasis': None}},multi=True)
            map_fares(dep_date_start=dep_date_start,dep_date_end=dep_date_end)
            close_client()
        except errorClass.ErrorObject as esub:
            print esub.__str__()
            # db.JUP_DB_Error_Collection.insert_one({'program': 'fbmapping_batch',
            #                                        'description': esub.__str__()})
    except KeyError as e:
        tb = traceback.format_exc()
        print str(tb.encode())
        # db.JUP_DB_Error_Collection.insert_one({'program': 'fbmapping_batch',
        #                                        'description': str(tb.encode())})
if __name__ == '__main__':
    main(dep_date_start=None,
         dep_date_end=None)

-------------------------------------------------------------------------------------------
Testing the codes
-------------------------------------------------------------------------------------------
DONE BY 	- SAI
ON      	- 2016-09-16
REMARKS REGISTER
2016-09-16
	SAI
	Done all the tests for the errors being handled in the code
	Tests done only for this code
	Testing for dependant codes like fbmapping_optimizer and fbmapping_mintomin to be done
--------------------------------------------------------------------------------------------
TEST CASE 1
  No combinations of pos,origin,destination,compartment obtainde from Host_Pricing_Data
	Hardcoded the following between line 143,144 :: query1 = []
  Expected Behavior - 1 document uploaded into ErrorCollection
  Obtained Behavior - 1 document uploaded into ErrorCollection
	Hardcoded value removed from code
TEST DONE PASSED ON 2016-09-16::23:55

TEST CASE 2
  No Airlines defined in Competitor_Pricing_Data
	Hardcoded the following in line 154 :: query2 = []
  Expected Behavior - 1 document uploaded into ErrorCollection
  Obtained Behavior - 1 document uploaded into ErrorCollection
TEST DONE PASSED ON 2016-09-17::00:01

TEST CASE 3
  No document obtained for a fixed pos,origin,destination,compartment for Host
	Hardcoded the following between lines 50,51 :: query = []
  Expected Behavior - The number of documents uploaded into Error
						Collection is equal to the number of combinations of
						pos,origin,destination,compartment
						In this case this should be 12
  Obtained Behavior - 12 document uploaded into Error Collection
	Hardcoded value removed from code
TEST DONE PASSED ON 2016-09-17::00:26

TEST CASE 4
  No document obtained for a fixed airline,pos,origin,destination,compartment for Competitor
	Hardcoded the following between lines 88,89 :: query = []
  Expected Behavior - The number of documents uploaded into Error
						Collection is equal to the number of combinations of
						pos,origin,destination,compartment multiplied by the number of
						unique competitors we have
						In this case this should be 12*4 = 48
  Obtained Behavior - 48 document uploaded into Error Collection
	Hardcoded value removed from code
TEST DONE PASSED ON 2016-09-17::00:35
'''
