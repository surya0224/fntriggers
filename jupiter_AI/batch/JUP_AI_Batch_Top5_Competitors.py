import datetime
import pandas as pd
from jupiter_AI import mongo_client,JUPITER_LOGGER
from jupiter_AI.logutils import measure
from jupiter_AI.network_level_params import Host_Airline_Code
from jupiter_AI.network_level_params import JUPITER_DB
from jupiter_AI.network_level_params import query_month_year_builder
#db = client[JUPITER_DB]
# db = client['testDB_New']


@measure(JUPITER_LOGGER)
def get_top5_competitors():
    cursor = db.JUP_DB_OD_Master.find()
    for i in cursor:
        # print i['OD']
        apipeline = [
            {
                '$match': {
                    'od': i['OD']
                }
            },
            {
                '$group': {
                    '_id': '$MarketingCarrier1',
                    'pax': {'$sum':'$pax'}
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'airline': '$_id',
                    'pax': '$pax'
                }
            }
        ]
        cursor2 = db.JUP_DB_Market_Share.aggregate(apipeline)
        data = []
        competitors = []
        for j in cursor2:
            data.append(j)
        sorted_data = sorted(data, key=lambda k: k['pax'], reverse=True)
        if len(sorted_data) != 0:
            for idx,item in enumerate(sorted_data):
                if item['airline'] != Host_Airline_Code:
                    competitors.append(item['airline'])
                    if len(competitors) == 5:
                        break
        if len(competitors) != 0:
            db.JUP_DB_OD_Master.update({'OD': i['OD']}, {'$set': {'top5_competitors':competitors}}, upsert=True)


@measure(JUPITER_LOGGER)
def gen_sys_comp(region, country, pos, origin, destination, compartment, dep_date_start, dep_date_end, db, date_list=None):
    """
    :param pos:
    :param origin:
    :param destination:
    :param compartment:
    :param dep_date_start:
    :param dep_date_end:
    :return:
    """
    ### dep_dates should be extreme dates ####
    month_yr_combi = []
    if dep_date_start:
        sd_str = dep_date_start
        sd_obj = datetime.datetime.strptime(sd_str, '%Y-%m-%d')
        sd = sd_obj.day
        sm = sd_obj.month
        sy = sd_obj.year

        if dep_date_end:
            ed_str = dep_date_end
            ed_obj = datetime.datetime.strptime(ed_str, '%Y-%m-%d')
            ed = ed_obj.day
            em = ed_obj.month
            ey = ed_obj.year

            month_yr_combi = query_month_year_builder(stdm=sm,
                                                      stdy=sy,
                                                      endm=em,
                                                      endy=ey)
    print month_yr_combi
    ms_query = dict()
    ms_query['MarketingCarrier1'] = {'$ne': Host_Airline_Code}
    if region:
        if type(region) == list:
            ms_query['region'] = {'$in': region}
        else:
            ms_query['region'] = region
    if country:
        if type(country) == list:
            ms_query['country'] = {'$in': country}
        else:
            ms_query['country'] = country
    if pos:
        if type(pos) == list:
            ms_query['pos'] = {'$in': pos}
        else:
            ms_query['pos'] = pos
    if origin and destination:
        if type(origin) == list and type(destination) == list:
            od_list = []
            for index, org in enumerate(origin):
                od_list.append(org + destination[index])
            ms_query['od'] = {'$in': od_list}
        elif type(origin) == str and type(destination) == str:
            ms_query['od'] = origin + destination
    if compartment:
        if type(compartment) == list:
            ms_query['compartment'] = {'$in': compartment}
        else:
            ms_query['compartment'] = compartment
    if month_yr_combi:
        ms_query['$or'] = month_yr_combi

    print ms_query
    # ms_crsr = db.JUP_DB_Market_Share.aggregate(
    #     [
    #         {
    #             '$match': ms_query
    #         }
    #         ,
    #         {
    #             '$group':
    #                 {
    #                     '_id': '$MarketingCarrier1',
    #                     'pax': {'$sum': '$pax'}
    #                 }
    #         }
    #         ,
    #         {
    #             '$project':
    #                 {
    #                     '_id': 0,
    #                     'airline': '$_id',
    #                     'pax': '$pax'
    #                 }
    #         }
    #         ,
    #         {
    #             '$sort': {'pax': -1}
    #         }
    #         ,
    #         {
    #             '$limit': 7
    #         }
    #         # ,
    #         # {
    #         #     '$out': 'JUP_DB_Top5_Comp_Temp'
    #         # }
    #     ]
    # )
    print "querying..."
    ms_crsr = db.JUP_DB_Market_Share.find(ms_query, {"MarketingCarrier1": 1, "pax": 1, "month": 1, "_id": 0})
    print "got data!!!", ms_crsr.count()
    crsr_list = list(ms_crsr)
    print "to+list"
    ms_data = pd.DataFrame(crsr_list)
    print "building dataframe"

    # print "grouped and sorted"
    # ms_data = list(ms_crsr)
    list_comps = []
    # print "looping"
    if len(ms_data) > 0:
        ms_data = ms_data.groupby(by=['MarketingCarrier1', 'month'], as_index=False)['pax'].sum()
        ms_data.sort_values(by='pax', inplace=True, ascending=False)
        for date_ in date_list:
            sm_1 = int(date_['start'][5:7])
            em_1 = int(date_['end'][5:7])
            # print sm_1, em_1
            # if sm_1 == em_1:
            tmp = ms_data[(ms_data['month'] >= sm_1) & (ms_data['month'] <= em_1)]
            # print tmp
            tmp = tmp.groupby(by='MarketingCarrier1', as_index=False)['pax'].sum().sort_values(by='pax', ascending=False)
            list_comps.append(list(tmp['MarketingCarrier1']))
    else:
        for i in range(len(date_list)):
            list_comps.append([])
        # else:
        #     tmp =
    # print ms_data
    # print "length: ", len(ms_data)
    # top5_comp = []

    # for comp in ms_data:
    #     top5_comp.append(comp['airline'].encode())

    # print top5_comp
    # print list_comps
    # return top5_comp
    return list_comps


@measure(JUPITER_LOGGER)
def get_prong_val(param, value):
    if param == 'compartment':
        compartment = value
        if compartment:
            if type(compartment) == list:
                if len(compartment) == 1:
                    compartment_prong_val = compartment[0]
                else:
                    compartment_prong_val = 'all'
            else:
                compartment_prong_val = compartment
        else:
            compartment_prong_val = 'all'
        return compartment_prong_val
    else:
        if value:
            if type(value) == list:
                if len(value) == 1:
                    return value[0]
                else:
                    master_query = dict()
                    master_query['POS_CD'] = {'$in': value}
                    prong_data = list(db.JUP_DB_Region_Master_Config.aggregate(
                        [
                            {
                                '$match': master_query
                            }
                            ,
                            {
                                '$group':
                                    {
                                        '_id': None,
                                        'city': {
                                            '$addToset': '$POS_CD'
                                        },
                                        'country': {
                                            '$addToset': '$COUNTRY_CD'
                                        },
                                        'region': {
                                            '$addToset': '$Region'
                                        },
                                        'network': {
                                            '$addToset': '$network'
                                        }
                                    }
                            }
                            ,
                            {
                                '$project':
                                    {
                                        'city': '$city',
                                        'country': '$country',
                                        'region': '$region',
                                        'network': '$network'
                                    }
                            }
                        ]
                    ))
                    if len(prong_data) == 1:
                        if len(prong_data[0]['network']) == 1:
                            return prong_data[0]['network'][0]
                        if len(prong_data[0]['region']) == 1:
                            return prong_data[0]['region'][0]
                        if len(prong_data[0]['country']) == 1:
                            return prong_data[0]['country'][0]
                        if len(prong_data[0]['city']) == 1:
                            return prong_data[0]['city'][0]
                    else:
                        return 'Network'
            else:
                return value
        else:
            return 'Network'


@measure(JUPITER_LOGGER)
def obtain_top5_comp_config(pos, origin, destination, compartment, dep_date_start, dep_date_end, db):
    """
    :param pos:
    :param origin:
    :param destination:
    :param compartment:
    :param dep_date_start:
    :param dep_date_end:
    :return:
    """
    """
    db.loadServerScripts();
    var
    org = "DXB";
    var
    dest = "DOH";
    var
    pos = "DXB";
    var
    comp = "J";
    var
    col_name = "Temp_Collection_Murugan";
    JUP_FN_Configuration(pos, org, dest, comp, col_name)
    """
    competitors = []
    compartment = get_prong_val(param='compartment', value=compartment)
    pos = get_prong_val(param='pos', value=pos)
    origin = get_prong_val(param='origin', value=origin)
    destination = get_prong_val(param='destination', value=destination)
    # print pos, origin, destination, compartment
    db.system_js.JUP_FN_Configuration(pos, origin, destination, compartment, "Temp_Collection_SAI", 'competitor')
    config_crsr = db['Temp_Collection_SAI'].find()
    if config_crsr.count() == 1:
        for i in config_crsr:
            # print i
            del i['_id']
            competitors = i['competitors']
        db['Temp_Collection_SAI'].drop()
    # print competitors
    return competitors


@measure(JUPITER_LOGGER)
def obtain_top_5_comp(pos, origin, destination, compartment, dep_date_start, dep_date_end, db,
                      region=None, country=None, date_list=None):
    """
    :param pos:
    :param origin:
    :param compartment:
    :param dep_date_start:
    :param dep_date_end:
    :return:
    """
    # print 'Competitors Generating For'
    # print pos,origin,destination,compartment,dep_date_start,dep_date_end
    comp_config = obtain_top5_comp_config(pos, origin, destination, compartment, dep_date_start, dep_date_end, db=db)
    # print comp_config
    # comp_config = list()
    print 'config', comp_config
    sys_gen_top5_comp = gen_sys_comp(region, country, pos, origin, destination, compartment, dep_date_start,
                                     dep_date_end, db=db, date_list=date_list)
    print 'sys_gen', sys_gen_top5_comp
    comps = []
    for i in range(len(date_list)):
        top5_comp = ([], [])
        for comp in comp_config:
            top5_comp[0].append(comp.encode())
            top5_comp[1].append('config')

        sys_gen_top5_comp_1 = [comp_val for comp_val in sys_gen_top5_comp[i] if comp_val not in comp_config]

        for comp in sys_gen_top5_comp_1:
            if len(top5_comp[0]) == 5:
                break

            top5_comp[0].append(comp.encode())
            top5_comp[1].append('sys_gen')
        comps.append(top5_comp)
    return comps


if __name__ == '__main__':
    client = mongo_client()
    db= client[JUPITER_DB]
    # get_top5_competitors()
    # print obtain_top_5_comp(pos=['DMM'],
    #                         origin=['DMM'],
    #                         destination=['DAC'],
    #                         compartment=['Y'],
    #                         dep_date_start='2017-05-01',
    #                         dep_date_end='2017-05-31')
    # print obtain_top_5_comp(pos=['DXB'],
    #                         origin=['BKK'],
    #                         destination=['DXB'],
    #                         compartment=['Y'],
    #                         dep_date_start='2017-05-01',
    #                         dep_date_end='2017-05-31')
    # print obtain_top_5_comp(region=[],
    #                         country=[],
    #                         pos=[],
    #                         origin=['IST'],
    #                         destination=['COK'],
    #                         compartment=[],
    #                         dep_date_start='2017-06-01',
    #                         dep_date_end='2017-06-30')

    comp = obtain_top_5_comp(
        region=[],
        country=[],
        pos=["DXB"],
        origin=['DXB'],
        destination=['DEL'],
        compartment=['Y'],
        dep_date_start='2017-04-27',
        dep_date_end='2017-05-27',
        db=db,
        date_list=[{"start":'2017-04-27', "end":"2017-05-03"}, {"start": "2017-04-27", "end":"2017-05-10"} ,{"start":"2017-05-27", "end": "2017-05-29"}]
    )
    client.close()
    print comp