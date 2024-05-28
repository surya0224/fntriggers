"""
Author: Abhinav Garg
Created with <3
Date Created: 2018-05-10
File Name: daily_log_mail.py

Automated Email Alerts Definitions.
Configuration and HTML for message body.

"""

import datetime
import pandas as pd
import json
import requests
from jupiter_AI import SECURITY_CLIENT_ID, SECURITY_CLIENT_SECRET_ID, MAIL_URL, AUTH_URL, SYSTEM_DATE, \
    MAIL_RECEIVERS, JUPITER_LOGGER, ENV
from jupiter_AI.logutils import measure


@measure(JUPITER_LOGGER)
def send_mail(exc_stats, subject, fares_volume, rules_volume):
    auth_data = {
        'client_id': SECURITY_CLIENT_ID,
        'client_secret_id': SECURITY_CLIENT_SECRET_ID
    }

    result = requests.post(AUTH_URL, data=auth_data)
    token = result.text

    if fares_volume is not None:
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
                <p>Hi Team,</p>
				<p>PDSS Execution Stats - {subject}</p>
                <p><b>DATE:</b> {SYSTEM_DATE}</p>
                <p><b>FARES VOLUME:</b> {fares_volume}</p>
                <p><b>RULES VOLUME:</b> {rules_volume}</p>
                <table>
                  <tr>
                    <th>GROUP</th>
                    <th>MIN-START-TIMESTAMP</th>
                    <th>MAX-END-TIMESTAMP</th>
                    <th>TIME TAKEN</th>
                  </tr>
                  {stats}
                </table>
                <br>
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

    else:
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
                <p>Hi Team,</p>
				<p>PDSS Execution Stats for {subject}</p>
                <p><b>DATE:</b> {SYSTEM_DATE}</p>
                <table>
                  <tr>
                    <th>GROUP</th>
                    <th>MIN-START-TIMESTAMP</th>
                    <th>MAX-END-TIMESTAMP</th>
                    <th>TIME TAKEN</th>
                  </tr>
                  {stats}
                </table>
                <br>
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

    for _, row in exc_stats.iterrows():
        stats += "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>".format(
            row["group_name"],
            row["start_time"],
            row["end_time"],
            row["time_taken"]
        )

    if fares_volume is not None:
        mail_body = ''.join(mail_body.format(
            subject=subject,
            SYSTEM_DATE=SYSTEM_DATE,
            fares_volume=fares_volume,
            rules_volume=rules_volume,
            stats=stats,
            env=ENV
        ).split('\n'))
    else:
        mail_body = ''.join(mail_body.format(
            subject=subject,
            SYSTEM_DATE=SYSTEM_DATE,
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
        "subject": "PDSS Analytics | {} | {} | Completion Alert".format(subject, ENV),
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
def send_mail_error_format(exc_stats, subject, fares_volume, rules_volume):
    auth_data = {
        'client_id': SECURITY_CLIENT_ID,
        'client_secret_id': SECURITY_CLIENT_SECRET_ID
    }

    result = requests.post(AUTH_URL, data=auth_data)
    token = result.text

    if fares_volume is not None:
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
                <p>Hi Team,</p>
				<p>PDSS Execution Stats - {subject}</p>
                <p><b>DATE:</b> {SYSTEM_DATE}</p>
                <p><b>FARES VOLUME:</b> {fares_volume}</p>
                <p><b>RULES VOLUME:</b> {rules_volume}</p>
                <table>
                  <tr>
                    <th>TASK_NAME</th>
                    
                    <th>ERROR_MESSAGE</th>
                    <th>Traceback</th>
                  </tr>
                  {stats}
                </table>
                <br>
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

    else:
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
                <p>Hi Team,</p>
				<p>PDSS Execution Stats for {subject}</p>
                <p><b>DATE:</b> {SYSTEM_DATE}</p>
                <table>
                  <tr>
                    <th>TASK_NAME</th>
                    
                    <th>ERROR_MESSAGE</th>
                    <th>Traceback</th>
                  </tr>
                  {stats}
                </table>
                <br>
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

    for _, row in exc_stats.iterrows():
        stats += "<tr><td>{}</td><td>{}</td><td>{}</td></tr>".format(
            row["task_name"],

            row["error_message"],
            row["error"]
        )

    if fares_volume is not None:
        mail_body = ''.join(mail_body.format(
            subject=subject,
            SYSTEM_DATE=SYSTEM_DATE,
            fares_volume=fares_volume,
            rules_volume=rules_volume,
            stats=stats,
            env=ENV
        ).split('\n'))
    else:
        mail_body = ''.join(mail_body.format(
            subject=subject,
            SYSTEM_DATE=SYSTEM_DATE,
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
        "subject": "PDSS Analytics | {} | {} | Completion Alert".format(subject, ENV),
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
def error_mail(task, task_id,error_class,error_message, hostname, error):
    count1 = db.test_fr.find({"task_start_date": datetime.datetime.now().strftime("%Y-%m-%d"), "task_name": task,"error_class": error_class, 'error_message': error_message}).count()
    if count1 < 1:
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
                <p>Hi Team,</p>
                <p>Celery Exception Occurred in:</p>
                <p><b>Task Name:</b> {task} <br> 
                <b>Task ID:</b> {task_id} <br> 
                <b>Hostname:</b> {hostname} <br> 
                <b>Timestamp:</b> {timestamp}</p>
                <p><b>Error Traceback:</b> <br>{error}</p>
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

        mail_body = ''.join(mail_body.format(
            task=task,
            task_id=task_id,
            hostname=hostname,
            timestamp=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            error=error,
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
            "subject": "PDSS Analytics | {} | {} | Error Alert".format(task.replace("_", " ").title(), ENV),
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
    else:

        pass


if __name__ == "__main__":
    from jupiter_AI import client, JUPITER_DB, SYSTEM_DATE, ATPCO_DB

    db = client[JUPITER_DB]
    crsr = list(db.execution_stats.aggregate([
        # {
        #     "$match": {
        #         # "task_name": {"$in": list(args)},
        #         # "task_start_date": SYSTEM_DATE
        #     }
        # },
        {
            "$group": {
                "_id": {
                    "group_name": "$group_name"
                },
                "start_time": {
                    "$min": "$task_start_time"
                },
                "end_time": {
                    "$max": "$task_end_time"
                },
                "completed_timestamp": {
                    "$max": "$completed_timestamp"
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "group_name": "$_id.group_name",
                "completed_timestamp": 1,
                "start_time": 1,
                "end_time": 1
            }
        },
        {
            "$sort": {
                "end_time": 1
            }
        }
    ], allowDiskUse=True))
    exc_stats = pd.DataFrame(crsr)
    exc_stats['start_time'] = pd.to_datetime(exc_stats['start_time'], format="%H:%M:%S")
    exc_stats['end_time'] = pd.to_datetime(exc_stats['end_time'], format="%H:%M:%S")
    exc_stats['time_taken'] = exc_stats['end_time'] - exc_stats['start_time']
    exc_stats['start_time'] = exc_stats['start_time'].dt.strftime("%H:%M:%S")
    exc_stats['end_time'] = exc_stats['end_time'].dt.strftime("%H:%M:%S")

    print send_mail(exc_stats, 0, 0)
