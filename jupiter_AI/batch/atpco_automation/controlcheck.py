from jupiter_AI import JUPITER_DB, ATPCO_DB, Host_Airline_Code, JUPITER_LOGGER, SYSTEM_DATE, today
from pandas import DataFrame
from jupiter_AI.logutils import measure
import json
import requests
import traceback
import datetime
from jupiter_AI import SECURITY_CLIENT_ID, SECURITY_CLIENT_SECRET_ID, MAIL_URL, AUTH_URL, MAIL_RECEIVERS, ENV
from dateutil.relativedelta import relativedelta


@measure(JUPITER_LOGGER)
def check_first(system_date, client, file_date):
    try:
        db = client[JUPITER_DB]
        cursor = db.JUP_DB_ATPCO_Fares_Rules.find({'carrier': Host_Airline_Code,
                                                   'fare_include': True,
                                                   "$or": [{"effective_to": None}, {"effective_to": {"$gte": system_date}}],
                                                   'file_date': file_date})
        count_1 = 0
        temp_list_1 = []
        for i in cursor:
            if i['fare'] * i['Reference_Rate'] <= 100.0:
                count_1 += 1
                temp_list_1.append({"type": "fare",
                                    "carrier": i['carrier'],
                                    "tariff_code": i['tariff_code'],
                                    "Rule_id": i['Rule_id'],
                                    "origin": i['origin'],
                                    "destination": i['destination'],
                                    "oneway_return": i['oneway_return'],
                                    "currency": i['currency'],
                                    "fare": i['fare'],
                                    "fare_basis": i['fare_basis'],
                                    "footnote": i['footnote'],
                                    "rtg": i['rtg'],
                                    "effective_from": i['effective_from'],
                                    "gfs": i['gfs']})
        if count_1 > 0:
            data_frame = DataFrame(temp_list_1)
            db.JUP_DB_ATPCO_control_check.insert_many(temp_list_1)
            failure_1_mail(data_frame)

    except Exception:
        subject_mail = "Fare Record Report"
        ein_info = traceback.format_exc()
        error_failure_mail(str(ein_info), subject_mail)


@measure(JUPITER_LOGGER)
def failure_1_mail(df):
    auth_data = {
        'client_id': SECURITY_CLIENT_ID,
        'client_secret_id': SECURITY_CLIENT_SECRET_ID
    }

    result = requests.post(AUTH_URL, data=auth_data)
    token = result.text

    mail_body = """
                        <html>
                        <head>
                        <style>
                        table {{
                            font-family: arial, sans-serif;
                            border-collapse: collapse;
                            width: 100%;
                        }}

                        td, th {{
                            border: 1px solid #dddddd;
                            text-align: left;
                            padding: 8px;
                        }}

                        tr:nth-child(even) {{
                            background-color: #dddddd;
                        }}
                        </style>
                        </head>
                        <body>
                        <p>Hello Team,</p>
                        <p><b>CONTROL CHECK STATUS</b>: BREACHED</p>
                        <p><b>Description of Condition Breached</b>: Base fare is less than 100AED or equivalent</p>
                        <table>
                        <tr>
                            <th><b>CARRIER</b></th>
                            <th><b>TARIFF_CODE</b></th>
                            <th><b>RULE_ID</b></th>
                            <th><b>ORIGIN</b></th>
                            <th><b>DESTINATION</b></th>
                            <th><b>ONEWAY_RETURN</b></th>
                            <th><b>CURRENCY</b></th>
                            <th><b>FARE</b></th>
                            <th><b>FARE_BASIS</b></th>
                            <th><b>FOOTNOTE</b></th>
                            <th><b>RTG</b></th>
                            <th><b>EFFECTIVE_FROM</b></th>
                            <th><b>GFS</b></th></p>
                            </tr>
                            {stats}
                        </table>
                        <tr>
                        <p>
                        Regards,
                        </p><p>
                        PDSS Admin
                        </p>
                        <div>
                        <hr style="background-color:#ddd">
                        <p>This e-mail was sent to you automatically from PDSS {env} Cluster.
                        Please do not respond to this message.
                        </p>
                        </div>
                        </body>
                        </html>
            """
    stats = ""

    for _, row in df.iterrows():
        stats += "<p><tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td>" \
                 "<td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr></p>".format(row["carrier"],
                                                                                           row["tariff_code"],
                                                                                           row["Rule_id"],
                                                                                           row["origin"],
                                                                                           row["destination"],
                                                                                           row["oneway_return"],
                                                                                           row["currency"],
                                                                                           row["fare"],
                                                                                           row["fare_basis"],
                                                                                           row["footnote"],
                                                                                           row["rtg"],
                                                                                           row["effective_from"],
                                                                                           row["gfs"])

    mail_body = ''.join(mail_body.format(
        stats=stats,
        env=ENV
    ).split('\n'))

    mail_data = {
        "asHtml": "true",
        "attachments": [],
        "bccAddresses": [],
        "body": mail_body,
        "ccAddresses": [],
        "emailConfigGroup": "EMAIL-SERVICE-EXTERNAL",
        "fromAddress": "no-reply@flydubai.com",
        "subject": "PDSS Control Check| {} | Fare Record Report".format(ENV),
        "toAddresses": MAIL_RECEIVERS
    }

    mail_headers = {
        "authorization": "Bearer {}".format(token),
        "Cache-Control": "no-cache",
        "client_id": "pdss",
        "Content-Type": "application/json"
    }

    mail_result = requests.post(MAIL_URL, data=json.dumps(mail_data), headers=mail_headers)

    return mail_result.text


@measure(JUPITER_LOGGER)
def check_second(system_date, client):
    try:
        client_1 = client[JUPITER_DB]
        client_2 = client[ATPCO_DB]
        temp_1 = []

        cur_1 = client_2.JUP_DB_ATPCO_Record_2_Cat_25.find({"CXR_CODE": Host_Airline_Code,
                                                            'DATES_DISC': "0999999",
                                                            'LAST_UPDATED_DATE': system_date})
        cur_1_list = list(cur_1)
        for i in cur_1_list:
            for j in range(int(i["NO_SEGS"])):
                temp_1.append(i["DATA_TABLE_STRING_TBL_NO_" + str(j + 1)])

        temp_1 = list(set(temp_1))
        count_2 = 0
        dic_1 = {}
        tab_list_1 = []

        cur_2 = client_2.JUP_DB_ATPCO_Record_3_Cat_25.find({'TBL_NO': {'$in': temp_1}})
        cur_2_list = list(cur_2)
        for j in cur_2_list:
            check = 0
            if j['FARE_CAL_PERCENT'] != "0000000":
                if int(j['FARE_CAL_PERCENT'][:3]) < 50:
                    count_2 += 1
                    check = 1

            elif j['FARE_CAL_AMT'] != "000000000" or j['FARE_CAL_AMOUNT'] != "000000000":
                if j['FARE_CAL_AMT'] != "000000000":
                    if int(j["FARE_CAL_DEC"]) != 0:
                        fare_1 = float(j['FARE_CAL_AMT'][:-int(j["FARE_CAL_DEC"])] + "." +
                                       j['FARE_CAL_AMT'][-int(j["FARE_CAL_DEC"]):])
                    else:
                        fare_1 = float(j['FARE_CAL_AMT'])

                    f_cur = client_1.JUP_DB_Exchange_Rate.find({"code": j['FARE_CAL_CUR']}).limit(1)

                    for x in f_cur:
                        fare_2 = x["Reference_Rate"] * fare_1

                        if fare_2 < 100.0:
                            count_2 += 1
                            check = 1

                if j['FARE_CAL_AMOUNT'] != "000000000":
                    if int(j["FARE_CAL_DEC1"]) != 0:
                        fare_x = float(j['FARE_CAL_AMOUNT'][:-int(j["FARE_CAL_DEC1"])] + "." +
                                       j['FARE_CAL_AMOUNT'][-int(j["FARE_CAL_DEC1"]):])
                    else:
                        fare_x = float(j['FARE_CAL_AMOUNT'])
                    f_cur = client_1.JUP_DB_Exchange_Rate.find({"code": j['FARE_CAL_CURR']}).limit(1)
                    for x in f_cur:
                        fare_3 = x["Reference_Rate"] * fare_x

                        if fare_3 < 100.0:
                            count_2 += 1
                            check = 1

            if check == 1:
                tab_list_1.append(j['TBL_NO'])
                dic_1.update({j['TBL_NO']: {'FARE_CAL_PERCENT': j['FARE_CAL_PERCENT'],
                                            'FARE_CAL_AMT': j['FARE_CAL_AMT'],
                                            'FARE_CAL_CUR': j['FARE_CAL_CUR'],
                                            'FARE_CAL_DEC': j["FARE_CAL_DEC"],
                                            'FARE_CAL_AMOUNT': j['FARE_CAL_AMOUNT'],
                                            'FARE_CAL_CURR': j['FARE_CAL_CURR'],
                                            'FARE_CAL_DEC1': j["FARE_CAL_DEC1"]}})
        if count_2 > 0:
            f_list = []
            cur_x = client_2.JUP_DB_ATPCO_Record_2_Cat_25.find({"CXR_CODE": Host_Airline_Code,
                                                                'DATES_DISC': "0999999",
                                                                'LAST_UPDATED_DATE': "2018-01-06"})
            cur_x_list = list(cur_x)
            for i in cur_x_list:
                for j in range(int(i["NO_SEGS"])):
                    if i["DATA_TABLE_STRING_TBL_NO_" + str(j + 1)] in tab_list_1:
                        f_list.append({'type': "cat25",
                                       'CAT_NO': "025",
                                       'CXR_CODE': Host_Airline_Code,
                                       'TARIFF': i['TARIFF'],
                                       'RULE_NO': i['RULE_NO'],
                                       'LOC_1': i['LOC_1'],
                                       'LOC_2': i['LOC_2'],
                                       'NO_APPL': i['NO_APPL'],
                                       'DATES_EFF_2': i['DATES_EFF_2'],
                                       'DATA_TABLE_STRING_TBL_NO_1': i["DATA_TABLE_STRING_TBL_NO_" + str(j + 1)],
                                       'SEQ_NO': i['SEQ_NO'],
                                       'FARE_CAL_PERCENT': dic_1[i["DATA_TABLE_STRING_TBL_NO_" + str(j + 1)]][
                                           'FARE_CAL_PERCENT'],
                                       'FARE_CAL_AMT': dic_1[i["DATA_TABLE_STRING_TBL_NO_" + str(j + 1)]][
                                           'FARE_CAL_AMT'],
                                       'FARE_CAL_CUR': dic_1[i["DATA_TABLE_STRING_TBL_NO_" + str(j + 1)]][
                                           'FARE_CAL_CUR'],
                                       'FARE_CAL_DEC': dic_1[i["DATA_TABLE_STRING_TBL_NO_" + str(j + 1)]][
                                           'FARE_CAL_DEC'],
                                       'FARE_CAL_AMOUNT': dic_1[i["DATA_TABLE_STRING_TBL_NO_" + str(j + 1)]][
                                           'FARE_CAL_AMOUNT'],
                                       'FARE_CAL_CURR': dic_1[i["DATA_TABLE_STRING_TBL_NO_" + str(j + 1)]][
                                           'FARE_CAL_CURR'],
                                       'FARE_CAL_DEC1': dic_1[i["DATA_TABLE_STRING_TBL_NO_" + str(j + 1)]][
                                           'FARE_CAL_DEC1']})

            df_ = DataFrame(f_list)
            client_1.JUP_DB_ATPCO_control_check.insert_many(f_list)
            failure_2_mail(df_)

    except Exception:
        subject_mail = "CAT25 FBR Report"
        ein_info = traceback.format_exc()
        error_failure_mail(str(ein_info), subject_mail)


@measure(JUPITER_LOGGER)
def failure_2_mail(df):

    auth_data = {
        'client_id': SECURITY_CLIENT_ID,
        'client_secret_id': SECURITY_CLIENT_SECRET_ID
    }

    result = requests.post(AUTH_URL, data=auth_data)
    token = result.text

    mail_body = """
                        <html>
                        <head>
                        <style>
                        table {{
                            font-family: arial, sans-serif;
                            border-collapse: collapse;
                            width: 100%;
                        }}

                        td, th {{
                            border: 1px solid #dddddd;
                            text-align: left;
                            padding: 8px;
                        }}

                        tr:nth-child(even) {{
                            background-color: #dddddd;
                        }}
                        </style>
                        </head>
                        <body>
                        <p>Hello Team,</p>
                        <p><b>CONTROL CHECK STATUS</b>: BREACHED</p>
                        <p><b>Description of Condition Breached</b>: Either the discount % percentage is
                        greater than x (50%) or specified fare
                        amount is less than 100 AED or equivalent.</p>
                        <table>
                        <tr>
                            <th><b>TYPE</b></th>
                            <th><b>CAT_NO</b></th>
                            <th><b>CXR_CODE</b></th>
                            <th><b>TARIFF</b></th>
                            <th><b>RULE_NO</b></th>
                            <th><b>LOC_1</b></th>
                            <th><b>LOC_2</b></th>
                            <th><b>NO_APPL</b></th>
                            <th><b>DATES_EFF_2</b></th>
                            <th><b>DATA_TABLE_STRING_TBL_NO_1</b></th>
                            <th><b>SEQ_NO</b></th>
                            <th><b>FARE_CAL_PERCENT</b></th>
                            <th><b>FARE_CAL_CUR</b></th>
                            <th><b>FARE_CAL_DEC</b></th>
                            <th><b>FARE_CAL_AMOUNT</b></th>
                            <th><b>FARE_CAL_CURR</b></th>
                            <th><b>FARE_CAL_DEC1</b></th>
                            </p>
                            </tr>
                            {stats}
                        </table>
                        <tr>
                        <p>
                        Regards,
                        </p><p>
                        PDSS Admin
                        </p>
                        <div>
                        <hr style="background-color:#ddd">
                        <p>This e-mail was sent to you automatically from PDSS {env} Cluster.
                        Please do not respond to this message.
                        </p>
                        </div>
                        </body>
                        </html>
            """
    stats = ""

    for _, row in df.iterrows():

        stats += "<p><tr><td>{}</td>" \
                 "<td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td>" \
                 "<td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td>" \
                        "<td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr></p>".format(row["type"],
                                                                                       row["CAT_NO"],
                                                                                       row["CXR_CODE"],
                                                                                       row["TARIFF"],
                                                                                       row["RULE_NO"],
                                                                                       row["LOC_1"],
                                                                                       row["LOC_2"],
                                                                                       row["NO_APPL"],
                                                                                       row["DATES_EFF_2"],
                                                                                       row["DATA_TABLE_STRING_TBL_NO_1"],
                                                                                       row["SEQ_NO"],
                                                                                       row["FARE_CAL_PERCENT"],
                                                                                       row["FARE_CAL_CUR"],
                                                                                       row["FARE_CAL_DEC"],
                                                                                       row["FARE_CAL_AMOUNT"],
                                                                                       row["FARE_CAL_CURR"],
                                                                                       row["FARE_CAL_DEC1"])

    mail_body = ''.join(mail_body.format(
            stats=stats,
            env=ENV
        ).split('\n'))

    mail_data = {
        "asHtml": "true",
        "attachments": [],
        "bccAddresses": [],
        "body": mail_body,
        "ccAddresses": [],
        "emailConfigGroup": "EMAIL-SERVICE-EXTERNAL",
        "fromAddress": "no-reply@flydubai.com",
        "subject": "PDSS Control Check| {} |  CAT25 FBR Report".format(ENV),
        "toAddresses": MAIL_RECEIVERS
    }

    mail_headers = {
        "authorization": "Bearer {}".format(token),
        "Cache-Control": "no-cache",
        "client_id": "pdss",
        "Content-Type": "application/json"
    }

    mail_result = requests.post(MAIL_URL, data=json.dumps(mail_data), headers=mail_headers)
    return mail_result.text


@measure(JUPITER_LOGGER)
def check_third(client):
    try:
        client_1 = client[JUPITER_DB]
        client_2 = client[ATPCO_DB]

        temp = []
        cur = client_2.JUP_DB_ATPCO_Record_2_Cat_All.find({"CXR_CODE": Host_Airline_Code,
                                                           "CAT_NO": "016",
                                                           'DATES_DISC': "0999999"})
        cur_list = list(cur)
        for i in cur_list:

            if i['RULE_NO'] != "GP09" and i['RULE_NO'] != "VA99" and i['RULE_NO'][:2] in ["01", "GP", "62", "VA"]:
                for j in range(int(i["NO_SEGS"])):
                    temp.append(i["DATA_TABLE_STRING_TBL_NO_" + str(j + 1)])

        temp = list(set(temp))
        count_3 = 0
        dlist = {}
        tab_list = []
        cur_2 = client_2.JUP_DB_ATPCO_Record_3_Cat_16.find({'TBL_NO': {'$in': temp}})
        cur_2_list = list(cur_2)
        for j in cur_2_list:
            check = 0

            if j['AMT_1'] != "0000000":
                if int(j["DEC_1"]) != 0:
                    fare = float(j['AMT_1'][:-int(j["DEC_1"])] + "." + j['AMT_1'][-int(j["DEC_1"]):])
                else:
                    fare = float(j['AMT_1'])

                f_cur = client_1.JUP_DB_Exchange_Rate.find({"code": j['CUR_1']}).limit(1)
                for x in f_cur:
                    fare2 = x["Reference_Rate"] * fare

                if fare2 > 2000.0:
                    count_3 += 1
                    check = 1

            if j['AMT_2'] != "0000000":
                if int(j["DEC_2"]) != 0:
                    fare_x = float(j['AMT_2'][:-int(j["DEC_2"])] + "." + j['AMT_2'][-int(j["DEC_2"]):])
                else:
                    fare_x = float(j['AMT_2'])
                f_cur = client_1.JUP_DB_Exchange_Rate.find({"code": j['CUR_2']}).limit(1)
                # print(f_cur)
                # print(len(list(f_cur)))
                for x in f_cur:
                    fare3 = x["Reference_Rate"] * fare_x

                if fare3 > 2000.0:
                    count_3 += 1
                    check = 1
            if check == 1:
                dlist.update({j['TBL_NO']: {'AMT_1': j['AMT_1'],
                                            'CUR_1': j['CUR_1'],
                                            'DEC_1': j["DEC_1"],
                                            'AMT_2': j['AMT_2'],
                                            'CUR_2': j['CUR_2'],
                                            'DEC_2': j["DEC_2"]}})
                tab_list.append(j['TBL_NO'])

        if count_3 > 0:
            f_list = []
            cur_x = client_2.JUP_DB_ATPCO_Record_2_Cat_All.find({"CXR_CODE": Host_Airline_Code,
                                                                 "CAT_NO": "016",
                                                                 'DATES_DISC': "0999999"})
            cur_x_list = list(cur_x)
            for i in cur_x_list:
                print("i am in")
                for j in range(int(i["NO_SEGS"])):
                    if i["DATA_TABLE_STRING_TBL_NO_" + str(j + 1)] in tab_list:
                        f_list.append({'type': "cat16",
                                       'CAT_NO': "016",
                                       'CXR_CODE': Host_Airline_Code,
                                       'TARIFF': i['TARIFF'],
                                       'RULE_NO': i['RULE_NO'],
                                       'LOC_1': i['LOC_1'],
                                       'LOC_2': i['LOC_2'],
                                       'FARE_CLASS': i['FARE_CLASS'],
                                       'NO_APPL': i['NO_APPL'],
                                       'TYPE_CODES_SEASON_TYPE': i['TYPE_CODES_SEASON_TYPE'],
                                       'TYPE_CODES_DAY_OF_WEEK_TYPE': i['TYPE_CODES_DAY_OF_WEEK_TYPE'],
                                       'OW_RT': i['OW_RT'],
                                       'DATES_EFF_2': i['DATES_EFF_2'],
                                       'RTG_NO': i['RTG_NO'],
                                       'DATA_TABLE_STRING_TBL_NO_1': i["DATA_TABLE_STRING_TBL_NO_" + str(j + 1)],
                                       'SEQ_NO': i['SEQ_NO'],
                                       'AMT_1': dlist[i["DATA_TABLE_STRING_TBL_NO_" + str(j + 1)]]['AMT_1'],
                                       'CUR_1': dlist[i["DATA_TABLE_STRING_TBL_NO_" + str(j + 1)]]['CUR_1'],
                                       'DEC_1': dlist[i["DATA_TABLE_STRING_TBL_NO_" + str(j + 1)]]['DEC_1'],
                                       'AMT_2': dlist[i["DATA_TABLE_STRING_TBL_NO_" + str(j + 1)]]['AMT_2'],
                                       'CUR_2': dlist[i["DATA_TABLE_STRING_TBL_NO_" + str(j + 1)]]['CUR_2'],
                                       'DEC_2': dlist[i["DATA_TABLE_STRING_TBL_NO_" + str(j + 1)]]['DEC_2']})

            df_ = DataFrame(f_list)
            client_1.JUP_DB_ATPCO_control_check.insert_many(f_list)
            failure_3_mail(df_)

    except Exception:
        subject_mail = "CAT16 Penalties Report"
        ein_info = traceback.format_exc()
        error_failure_mail(str(ein_info), subject_mail)


@measure(JUPITER_LOGGER)
def failure_3_mail(df):

    auth_data = {
        'client_id': SECURITY_CLIENT_ID,
        'client_secret_id': SECURITY_CLIENT_SECRET_ID
    }

    result = requests.post(AUTH_URL, data=auth_data)
    token = result.text

    mail_body = """
                        <html>
                        <head>
                        <style>
                        table {{
                            font-family: arial, sans-serif;
                            border-collapse: collapse;
                            width: 100%;
                        }}

                        td, th {{
                            border: 1px solid #dddddd;
                            text-align: left;
                            padding: 8px;
                        }}

                        tr:nth-child(even) {{
                            background-color: #dddddd;
                        }}
                        </style>
                        </head>
                        <body>
                        <p>Hello Team,</p>
                        <p><b>CONTROL CHECK STATUS</b>: BREACHED</p>
                        <p><b>Description of Condition Breached</b>: The amount exceeded the range of 0-2000 AED</p>
                        <table>
                        <tr>
                            <th><b>TYPE</b></th>
                            <th><b>CAT_NO</b></th>
                            <th><b>CXR_CODE</b></th>
                            <th><b>TARIFF</b></th>
                            <th><b>RULE_NO</b></th>
                            <th><b>LOC_1</b></th>
                            <th><b>LOC_2</b></th>
                            <th><b>FARE_CLASS</b></th>
                            <th><b>NO_APPL</b></th>
                            <th><b>TYPE_CODES_SEASON_TYPE</b></th>
                            <th><b>TYPE_CODES_DAY_OF_WEEK_TYPE</b></th>
                            <th><b>OW_RT</b></th>
                            <th><b>DATES_EFF_2</b></th>
                            <th><b>RTG_NO</b></th>
                            <th><b>DATA_TABLE_STRING_TBL_NO_1</b></th>
                            <th><b>SEQ_NO</b></th>
                            <th><b>AMT_1</b></th>
                            <th><b>CUR_1</b></th>
                            <th><b>DEC_1</b></th>
                            <th><b>AMT_2</b></th>
                            <th><b>CUR_2</b></th>
                            <th><b>DEC_2</b></th>
                            </p>
                            </tr>
                            {stats}
                        </table>
                        <tr>
                        <p>
                        Regards,
                        </p><p>
                        PDSS Admin
                        </p>
                        <div>
                        <hr style="background-color:#ddd">
                        <p>This e-mail was sent to you automatically from PDSS {env} Cluster.
                        Please do not respond to this message.
                        </p>
                        </div>
                        </body>
                        </html>
            """
    stats = ""

    for _, row in df.iterrows():

        stats += "<p><tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td>" \
                 "<td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td>" \
                 "<td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td>"\
                 "<td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr></p>".format(row["type"],
                                                                                           row["CAT_NO"],
                                                                                           row["CXR_CODE"],
                                                                                           row["TARIFF"],
                                                                                           row["RULE_NO"],
                                                                                           row["LOC_1"],
                                                                                           row["LOC_2"],
                                                                                           row["FARE_CLASS"],
                                                                                           row["NO_APPL"],
                                                                                           row["TYPE_CODES_SEASON_TYPE"],
                                                                                           row["TYPE_CODES_DAY_OF_WEEK_TYPE"],
                                                                                           row["OW_RT"],
                                                                                           row["DATES_EFF_2"],
                                                                                           row["RTG_NO"],
                                                                                           row["DATA_TABLE_STRING_TBL_NO_1"],
                                                                                           row["SEQ_NO"],
                                                                                           row["AMT_1"],
                                                                                           row["CUR_1"],
                                                                                           row["DEC_1"],
                                                                                           row["AMT_2"],
                                                                                           row["CUR_2"],
                                                                                           row["DEC_2"])

    mail_body = ''.join(mail_body.format(
            stats=stats,
            env=ENV
        ).split('\n'))

    mail_data = {
        "asHtml": "true",
        "attachments": [],
        "bccAddresses": [],
        "body": mail_body,
        "ccAddresses": [],
        "emailConfigGroup": "EMAIL-SERVICE-EXTERNAL",
        "fromAddress": "no-reply@flydubai.com",
        "subject": "PDSS Control Check| {} | CAT16 Penalties Report".format(ENV),
        "toAddresses": MAIL_RECEIVERS
    }

    mail_headers = {
        "authorization": "Bearer {}".format(token),
        "Cache-Control": "no-cache",
        "client_id": "pdss",
        "Content-Type": "application/json"
    }

    mail_result = requests.post(MAIL_URL, data=json.dumps(mail_data), headers=mail_headers)

    return mail_result.text


@measure(JUPITER_LOGGER)
def error_failure_mail(error, subject):
    auth_data = {
        'client_id': SECURITY_CLIENT_ID,
        'client_secret_id': SECURITY_CLIENT_SECRET_ID
    }

    result = requests.post(AUTH_URL, data=auth_data)
    token = result.text

    mail_body = """
                        <html>
                        <head>
                        <style>
                        table {{
                            font-family: arial, sans-serif;
                            border-collapse: collapse;
                            width: 100%;
                        }}

                        td, th {{
                            border: 1px solid #dddddd;
                            text-align: left;
                            padding: 8px;
                        }}

                        tr:nth-child(even) {{
                            background-color: #dddddd;
                        }}
                        </style>
                        </head>
                        <body>
                        <p>Hello Team,</p>
                        <p>CONTROL CHECK STATUS: FAILED </p>
                        <p>{stats}</p>
                        Regards,
                        </p><p>
                        PDSS Admin
                        </p>
                        <div>
                        <hr style="background-color:#ddd">
                        <p>This e-mail was sent to you automatically from PDSS {env} Cluster.
                        Please do not respond to this message.
                        </p>
                        </div>
                        </body>
                        </html>
            """
    stats = str(error)

    mail_body = ''.join(mail_body.format(
        timestamp=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        stats = stats,
        env=ENV
    ).split('\n'))

    mail_data = {
        "asHtml": "true",
        "attachments": [],
        "bccAddresses": [],
        "body": mail_body,
        "ccAddresses": [],
        "emailConfigGroup": "EMAIL-SERVICE-EXTERNAL",
        "fromAddress": "no-reply@flydubai.com",
        "subject": "PDSS Control Check| {} | {} | Error Alert".format(ENV,subject),
        "toAddresses": MAIL_RECEIVERS
    }

    mail_headers = {
        "authorization": "Bearer {}".format(token),
        "Cache-Control": "no-cache",
        "client_id": "pdss",
        "Content-Type": "application/json"
    }

    mail_result = requests.post(MAIL_URL, data=json.dumps(mail_data), headers=mail_headers)

    return mail_result.text


def main(client):
    file_date = (today - relativedelta(days=1)).strftime("%Y%m%d")
    check_first(SYSTEM_DATE, client, file_date)
    check_second(system_date=SYSTEM_DATE, client=client)
    check_third(client=client)







