"""
File Name              : Top_10_Triggers
Author                 : Shamail Mulla
Date Created           : 2017-03-14
Description            : This file generates 10 most crucial triggers to be addressed by the analyst

MODIFICATIONS LOG
    S.No                   : 1
    Date Modified          : 2017-03-15
    By                     : Shamail Mulla
    Modification Details   : Creating PDF files and emailing it to particular user

"""
import datetime
import email
import email.mime.application
import inspect
import os
import smtplib
import traceback
from email.mime.text import MIMEText

from reportlab.lib import colors
from reportlab.lib.enums import TA_JUSTIFY
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle

import jupiter_AI.common.ClassErrorObject as error_class
from jupiter_AI import client
from jupiter_AI.network_level_params import JUPITER_DB,JUPITER_LOGGER
#db = client[JUPITER_DB]
from jupiter_AI.tiles.jup_common_functions_tiles import gen_collection_name as gen
from jupiter_AI.logutils import measure
coll_unprocessed_triggers = gen()


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


class Users():
    """
    This class gets information about users in the system
    """
    @measure(JUPITER_LOGGER)
    def __init__(self):
        # self.compartments = []
        # self.regions = []
        self.od = []
        # self.countries = []
        self.pos = []

    @measure(JUPITER_LOGGER)
    def get_user_profiles(self):
        """
        This function retrieves all users in the system and depending on their level of access, it populates pos and
        ods
        :return lst_user_profiles: list
        """
        try:
            if 'JUP_DB_User_Profile' in db.collection_names():
                crsr_user_profiles = db['JUP_DB_User_Profile'].find()
                lst_user_profiles = list(crsr_user_profiles)

                # Checking level of access for each user to populate od and pos fields
                for user in lst_user_profiles:
                    del user['_id']
                    del user[u'od']

                    if user[u'level'] == 'network':
                        del user[u'pos']
                        self.get_all_ods()
                        self.get_all_pos()
                        user[u'od'] = self.od
                        user[u'pos'] = self.pos
                    elif user[u'level'] == 'region':
                        del user[u'pos']
                        self.get_all_ods({'$or': [{'region': user[u'region']}]})
                        self.get_all_pos({'$or': [{'region': user[u'region']}]})
                        user[u'od'] = self.od
                        user[u'pos'] = self.pos
                    elif user[u'level'] == 'country':
                        del user[u'pos']
                        self.get_all_ods({'$or': [{'country': user[u'country']}]})
                        self.get_all_pos({'$or': [{'country': user[u'country']}]})
                        user[u'od'] = self.od
                        user[u'pos'] = self.pos
                    else:
                        self.get_all_ods({'$or': [{'pos': user[u'pos']}]})
                        user[u'od'] = self.od
                        user[u'pos'] = list([user[u'pos']])
            else:
                print traceback.print_exc()
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL1,
                                                    'jupter_AI/triggers/data_level/Top_10_Triggers.py method: get_user_profiles',
                                                    get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list('Collection JUP_DB_User_Profile cannot be found in the database.')
                obj_error.write_error_logs(datetime.datetime.now())

            return lst_user_profiles

        except Exception as error_msg:
            print traceback.print_exc()
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                                'jupter_AI/triggers/data_level/Top_10_Triggers.py method: get_user_profiles',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list(str(error_msg))
            obj_error.write_error_logs(datetime.datetime.now())

    @measure(JUPITER_LOGGER)
    def get_all_pos(self, query={}):
        try:
            crsr_pos = db.JUP_DB_Workflow_1.distinct('pos', query)
            self.pos.append(crsr_pos)
        except Exception as error_msg:
            print traceback.print_exc()
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                                'jupter_AI/triggers/data_level/Top_10_Triggers.py method: get_all_pos',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list(str(error_msg))
            obj_error.write_error_logs(datetime.datetime.now())

    @measure(JUPITER_LOGGER)
    def get_all_ods(self, query={}):
        try:
            crsr_od = db.JUP_DB_OD_Master.distinct('OD', query)
            self.od.append(crsr_od)
        except Exception as error_msg:
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                                'jupter_AI/triggers/data_level/Top_10_Triggers.py method: get_all_ods',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list(str(error_msg))
            obj_error.write_error_logs(datetime.datetime.now())

    # #@measur(JUPITER_LOGGER)
            # def get_all_compartments(self):
    #     crsr_compartments = db.JUP_DB_Workflow.distinct('compartment')
    #     self.compartments.append(crsr_compartments)

class EmailPriorityTriggers():
    compartments = []
    regions = []
    od = []
    countries = []
    pos = []

    @measure(JUPITER_LOGGER)
    def __init__(self):
        pass

    @measure(JUPITER_LOGGER)
    def get_unprocessed_triggers(self):
        try:
            if 'JUP_DB_Workflow_1' in db.collection_names():
                ppln_unprocessed_trigger = [
                    {
                        '$project':
                            {
                                'pos': '$pos',
                                'od': {'$concat': ['$origin','$destination']},
                                'currency': '$currency',
                                "compartment": "$compartment",
                                "recommendation_category_details": '$recommendation_category_details',
                                "trigger_id": "$trigger_id",
                                # "effective_from": "$effective_from",
                                # 'effective_from': {'$dateToString': {'format': '%d-%m-%Y', 'date': '$effective_from'}},
                                # 'effective_to': {'$dateToString': {'format': '%d-%m-%Y', 'date': '$effective_to'}},
                                'effective_from': { '$concat': [{'$substr': ['$effective_from', 8, 2]},
                                                                '-',
                                                                {'$substr': ['$effective_from', 5, 2]},
                                                                '-',
                                                                {'$substr': ['$effective_from', 0, 4]},]},
                                'effective_to': {'$concat': [{'$substr': ['$effective_to', 8, 2]},
                                                               '-',
                                                               {'$substr': ['$effective_to', 5, 2]},
                                                               '-',
                                                               {'$substr': ['$effective_to', 0, 4]}, ]},
                                'host_pricing_data': '$host_pricing_data',
                                # "effective_to": "$effective_to",
                                "country": "$country",
                                "region": "$region",
                                "status": "$status",
                                'farebasis': '$farebasis',
                                'description':'$desc',
                                'bookings_data':'$bookings_data',
                                'trigger_type':'$trigger_type',
                                # 'process_start_date': {'$dateToString': {'format': '%d-%m-%Y', 'date': '$process_start_date'}},
                                # 'process_end_date': {'$dateToString': {'format': '%d-%m-%Y', 'date': '$process_end_date'}},
                                'process_start_date': {'$concat': [{'$substr': ['$process_start_date', 8, 2]},
                                                             '-',
                                                             {'$substr': ['$process_start_date', 5, 2]},
                                                             '-',
                                                             {'$substr': ['$process_start_date', 0, 4]}, ]},
                                'process_end_date': {'$concat': [{'$substr': ['$process_end_date', 8, 2]},
                                                                   '-',
                                                                   {'$substr': ['$process_end_date', 5, 2]},
                                                                   '-',
                                                                   {'$substr': ['$process_end_date', 0, 4]}, ]},
                            }
                    }
                    ,
                    {
                        '$redact':
                            {
                                '$cond':
                                    {
                                        'if':
                                            {
                                                '$or':
                                                    [
                                                        {'$ne': ['$status', 'accepted']},
                                                        {'$ne': ['$status', 'rejected']}
                                                    ]
                                             },
                                        'then': '$$DESCEND',
                                        'else': '$$PRUNE'
                                    }
                            }
                    },
                    {
                        '$sort': {'recommendation_category_details.score': -1}
                    }
                    ,
                    {
                        '$project':
                            {
                                'pos': '$pos',  # done
                                'od': '$od',  # done
                                'currency': '$currency',  # done
                                'dep_date_from':'$host_pricing_data.dep_date_from',
                                'dep_date_to': '$host_pricing_data.dep_date_to',
                                'effective_period': {'$concat': ['$effective_from', ' - ', '$effective_to']},  # done
                                # 'travel': {'$concat': ['$host_pricing_data.dep_date_from', ' - ',
                                #                        '$host_pricing_dep.sale_date_to']},  # done
                                'sale': {'$concat': ['$host_pricing_data.sale_date_from', ' - ',
                                                     '$host_pricing_data.sale_date_to']},  # done
                                'last_ticketed_date': '$host_pricing_data.last_ticketed_date',  # done
                                'bookings_ytd': '$bookings_data.bookings',  # done
                                'bookings_vtgt': '$bookings_data.bookings_vtgt',  # done
                                'bookings_vlyr': '$bookings_data.bookings_vlyr',  # done
                                "compartment": "$compartment",  # done
                                "trigger_id": "$trigger_id",  # done
                                'market_share_ytd': '$host_pricing_data.market_share',  # done
                                'market_share_vlyr': '$host_pricing_data.market_share_vlyr',  # done
                                'market_share_vtgt': '$host_pricing_data.market_share_vtgt',  # done
                                'fms': '$host_pricing_data.fms',  # done
                                'OW/RT':
                                    {
                                        '$cond':
                                            {
                                                'if': {'$eq': ['$host_pricing_data.oneway_return', 1]},
                                                'then': 'OW',
                                                'else': 'RT'
                                            }
                                    },  # done
                                'rule_id': '$host_pricing_data.Rule_id',  # done
                                "country": "$country",  # done
                                "region": "$region",  # done
                                'farebasis': '$farebasis',  # done
                                'current_fare': '$host_pricing_data.total_fare',  # done
                                'recommended_fare': '$host_pricing_data.recommended_fare',  # done
                                'description': '$description',  # done
                                'footnote': '$host_pricing_data.footnote',
                                'trigger_type':'$trigger_type',
                                'process_end_date': '$process_end_date',
                                'process_start_date':'$process_start_date'
                            }
                    }
                    ,
                    {
                        '$out': coll_unprocessed_triggers
                    }
                ]
                db.JUP_DB_Workflow_1.aggregate(ppln_unprocessed_trigger, allowDiskUse=True)
                if coll_unprocessed_triggers in db.collection_names():
                    crsr_triggers = db.get_collection(coll_unprocessed_triggers).find()
                    if crsr_triggers.count()>0:
                        lst_unprocessed_triggers = list(crsr_triggers)
                        db[coll_unprocessed_triggers].drop()
                        # print len(lst_unprocessed_triggers), 'triggers'
                    else:
                        pass
                else:
                    obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                                        'jupter_AI/triggers/data_level/Top_10_Triggers.py method: get_unprocessed_triggers',
                                                        get_arg_lists(inspect.currentframe()))
                    obj_error.append_to_error_list('Resultant trigger collection not created in the database.')
                    obj_error.write_error_logs(datetime.datetime.now())

                return lst_unprocessed_triggers
            else:
                obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                                    'jupter_AI/triggers/data_level/Top_10_Triggers.py method: get_unprocessed_triggers',
                                                    get_arg_lists(inspect.currentframe()))
                obj_error.append_to_error_list('Collection JUP_DB_Workflow_1 not in the database.')
                obj_error.write_error_logs(datetime.datetime.now())
        except Exception as error_msg:
            print traceback.print_exc()
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                                'jupter_AI/triggers/data_level/Top_10_Triggers.py method: get_unprocessed_triggers',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list(str(error_msg))
            obj_error.write_error_logs(datetime.datetime.now())


    @measure(JUPITER_LOGGER)
    def assign_triggers_to_users(self):
        try:
            obj_users = Users()
            user_profiles = obj_users.get_user_profiles()
            unprocessed_triggers = self.get_unprocessed_triggers()
            # for trigger in unprocessed_triggers:
            #     print 'trigger od', trigger[u'od']
            # print len(unprocessed_triggers), 'triggers'
            for user_data in user_profiles:
                # print user_data[u'name'],'DATA'
                ods = user_data[u'od'][0]
                # print len(ods),'ods:', ods
                POS = user_data[u'pos'][0]
                # print len(POS),'pos:',POS
                dict_trigger_type = {}
                lst_triggers = []

                for trigger in unprocessed_triggers:
                    # print 'TRIGGER LOOP BEGINS'
                    # print trigger[u'pos']
                    # print trigger[u'od']
                    for pos in POS:
                        if pos == trigger[u'pos']:
                            # print 'pos match'
                            # print len(trigger[u'od']), 'TRIGGER ODs'
                            for od in ods:
                                if od == trigger[u'od']:
                                    # print 'od match'
                                    lst_triggers.append(trigger)

                                    if trigger[u'trigger_type'] not in dict_trigger_type:
                                        dict_trigger_type[trigger[u'trigger_type']] = 1
                                        # lst_trigger_type.append(
                                        #     {'trigger_type': trigger[u'trigger_type']}, {'trigger_count': 1})
                                    else:
                                        for key in dict_trigger_type:
                                            if trigger[u'trigger_type'] == key:
                                                dict_trigger_type[key] += 1

                del user_data[u'od']
                del user_data[u'pos']

                if len(lst_triggers)>0:
                    user_data['trigger_details'] = lst_triggers[:10]

                    min_dep_date = min(t[u'dep_date_from'] for t in lst_triggers)
                    max_dep_date = max(t[u'dep_date_to'] for t in lst_triggers)
                    dep_date_range = str(min_dep_date + ' to ' + max_dep_date)

                    min_process_start_date = min(t[u'process_start_date'] for t in lst_triggers)
                    max_process_start_date = max(t[u'process_end_date'] for t in lst_triggers)
                    process_date_range = str(min_process_start_date + ' to ' + max_process_start_date)

                    msg_trigger_type_count = ''
                    for key in dict_trigger_type:
                        # print key, dict_trigger_type[key]
                        msg_trigger_type_count += str(key)
                        msg_trigger_type_count += '\t: '
                        msg_trigger_type_count += str(dict_trigger_type[key])
                        msg_trigger_type_count += '\n'

                    if user_data[u'email_id']:
                        email_id = user_data[u'email_id']
                        del user_data[u'email_id']
                        obj_email = SendEmails()
                        obj_email.send_email(email_id, user_data, len(lst_triggers), dep_date_range, process_date_range, msg_trigger_type_count)
        except Exception as error_msg:
            print traceback.print_exc()
            raise error_msg
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                                'jupter_AI/triggers/data_level/Top_10_Triggers.py method: assign_triggers_to_users',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list(str(error_msg))
            obj_error.write_error_logs(datetime.datetime.now())

class SendEmails():
    @measure(JUPITER_LOGGER)
    def __init__(self):
        pass

    @measure(JUPITER_LOGGER)
    def create_PDF(self, pdf_body):
        try:
            import os
            dir_path = os.path.dirname(os.path.realpath(__file__))
            # file_path = '../../../' + pdf_body[u'name'] + '_top_10_triggers_' + str(datetime.datetime.now())+'.pdf'
            file_name = pdf_body[u'name'] + '_top_10_triggers_' + str(datetime.datetime.now().strftime("%d_%m_%y_%H_%M_%S"))+'.pdf'
            file_path = os.path.join(dir_path, file_name)
            doc = SimpleDocTemplate(file_path, pagesize=letter, rightMargin=72, leftMargin=72, topMargin=72, bottomMargin=18)
            elements = []
            styles = getSampleStyleSheet()
            styles.add(ParagraphStyle(name='Justify', alignment=TA_JUSTIFY))

            for trigger in pdf_body['trigger_details']:
                # print trigger
                ptext = '<font size=14>Trigger ID:%s</font>' % (trigger[u'trigger_id'])
                elements.append(Paragraph(ptext, styles["Normal"]))
                elements.append(Spacer(30, 12))

                ptext = '<font size=12>Description:</font>'
                elements.append(Paragraph(ptext, styles["Normal"]))
                elements.append(Spacer(30, 12))

                desc_list = trigger['description'].split('_')
                # print desc_list
                for str_ in desc_list:
                    ptext = '<font size=10>%s</font>' % str_.encode()
                    elements.append(Paragraph(ptext, styles["Normal"]))
                    elements.append(Spacer(30, 12))

                # print trigger['OW/RT']
                table_data = \
                [
                    # ['Description', str(trigger[u'description'])], #
                    ['Market POS/OD/Compartment',str(str(trigger[u'pos'])+ '/'+ str(trigger[u'od'])+ '/'+ str(trigger[u'compartment']))],
                    ['OW/RT', str(trigger[u'OW/RT'])], #
                    ['Fare Basis', str(trigger[u'farebasis'])], #
                    ['Rule ID', str(trigger[u'rule_id'])], #
                    ['Foot note', str(trigger[u'footnote'])],
                    ['Effective Period', str(trigger[u'effective_period'])], #
                    ['Sale Period', str(trigger[u'sale'])], #
                    ['Travel Period', str(str(trigger[u'dep_date_from'])+' to '+ str(trigger[u'dep_date_to']))], #
                    ['Last Ticketed', str(trigger[u'last_ticketed_date'])], #
                    ['Currency', str(trigger[u'currency'])], #
                    ['Current Fare', str(trigger[u'current_fare'])],
                    ['Recommended Fare', str(trigger[u'recommended_fare'])],
                    # NOT NEEDED
                    # ['Host Bookings YTD', str(trigger[u'bookings_ytd'])],
                    # ['Host Bookings VLYR', str(trigger[u'bookings_vlyr'])],
                    # ['Host Bookings VTGT', str(trigger[u'bookings_vtgt'])],
                    # ['Host Market Share YTD', str(trigger[u'market_share_ytd'])],
                    # ['Host Market Share VLYR', str(trigger[u'market_share_vlyr'])],
                    # ['Host Market Share VTGT', str(trigger[u'market_share_vtgt'])],
                    # ['FMS', str(trigger[u'fms'])]

                    # ['Market POS/OD/Comp', 'OW/RT', 'Fare Basis', 'Rule ID', 'Effective Period',
                    #  'Sale Period', 'Travel Period', 'Last Ticketed', 'Currency', 'Current Fare', 'Recommended Fare'],
                    # [str(str(trigger[u'pos'])+ '/'+ str(trigger[u'od'])+ '/'+ str(trigger[u'compartment'])),
                    #  str(trigger[u'OW/RT']), str(trigger[u'farebasis']), str(trigger[u'rule_id']),
                    #  str(trigger[u'effective_period']), str(trigger[u'sale']),
                    #  str(str(trigger[u'dep_date_from'])+' to '+ str(trigger[u'dep_date_to'])),
                    #  str(trigger[u'last_ticketed_date']), str(trigger[u'currency']), str(trigger[u'current_fare']),
                    #  str(trigger[u'recommended_fare'])]
                ]
                t = Table(table_data)
                t.setStyle(TableStyle([('INNERGRID', (0,0), (-1,-1), 0.25, colors.black),
                       ('BOX', (0,0), (-1,-1), 0.25, colors.black),]))
                elements.append(t)
                # ptext = '<font size=10>OD:%s</font>' % (trigger[u'od'])
                # elements.append(Paragraph(ptext, styles["Normal"]))
                # elements.append(PageBreak)
                elements.append(Spacer(2, 12))
            doc.build(elements)

            print file_path
            # file_path = str()+str(pdf_body[u'name'])+'_top_10_triggers.pdf'
            # dir_path = os.path.dirname(doc)
            # file_path = os.path.join(dir_path, str(pdf_body[u'name']+'_top_10_triggers.pdf'))
            return file_path
        except Exception as error_msg:
            print traceback.print_exc()
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                                'jupter_AI/triggers/data_level/Top_10_Triggers.py method: create_PDF',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list(str(error_msg))
            obj_error.write_error_logs(datetime.datetime.now())

    @measure(JUPITER_LOGGER)
    def send_email(self, email_ID, email_attachment_data, count_triggers, dep_date_range, process_date_range, msg_trigger_type_count):
        try:
            email_attachment_path = self.create_PDF(email_attachment_data)
            # attachment = open(email_attachment_path)
            print email_ID, email_attachment_path
            msg = email.MIMEMultipart.MIMEMultipart('mixed')

            msg['Subject'] = ('Top 10 Triggers for '+dep_date_range)
            msg['From'] = "alerts@flynava.com"
            msg['To'] = email_ID
            """
            Email body example:
            Hi Vanessa,
            Based on the data, system has generated the following top 10 triggers for your markets and the departure
            period <2017-02-01 to 2017-02-28> for the simulation run @ <time>
            These are the triggers / fare actions which needs to be addressed on <date>
            """
            body = MIMEText(str('Hi '+email_attachment_data[u'name']+',\nBased on the data, system has generated the following top ' +
                                str(count_triggers)+' triggers for your markets and the departure period '+ dep_date_range +
                                ' for the simulation run during '+ process_date_range +'.\n\nTrigger type details:\n' +
                                msg_trigger_type_count+'\n\nYou can view details in the attached PDF and go to workflow '+
                                'http://demo.flynava.com/modules/workflow/dss/views/dss.html to address them in detail.' +
                                '\n\nThis is a system generated alert. In case of any queries, please contact your administrator.'),
                            'plain') # , _charset="utf-8"
            # print body
            msg.attach(body)
            # print email_attachment_path
            file_ = open(email_attachment_path,'rb')
            att = email.mime.application.MIMEApplication(file_.read(), _subtype="pdf")
            file_.close()
            att.add_header('Content-Disposition', 'attachment', filename=email_attachment_path)
            msg.attach(att)
            # msg.attach(MIMEText(file(email_attachment_path).read()))
            # https://www.google.com/settings/security/lesssecureapps
            # For Gmail LEss secure apps should be enabled from the above link
            s = smtplib.SMTP('smtp.zoho.com',587)
            s.ehlo()
            s.starttls()
            s.login('alerts@flynava.com', 'alerts123')
            s.sendmail(msg['From'], msg['To'].split(","), msg.as_string())
            s.quit()
            print 'Email sent to', email_ID
            os.remove(email_attachment_path)
        except Exception as error_msg:
            print traceback.print_exc()
            obj_error = error_class.ErrorObject(error_class.ErrorObject.ERRORLEVEL2,
                                                'jupter_AI/triggers/data_level/Top_10_Triggers.py method: send_email',
                                                get_arg_lists(inspect.currentframe()))
            obj_error.append_to_error_list(str(error_msg))
            obj_error.write_error_logs(datetime.datetime.now())

obj_priority_triggers = EmailPriorityTriggers()
obj_priority_triggers.assign_triggers_to_users()

# obj_email = SendEmails()
# obj_email.create_PDF("sifjewofigjer")