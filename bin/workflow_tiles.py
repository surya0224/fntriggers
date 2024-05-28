#!/usr/bin/env python
# print "Content-Type: application/json"
print
import traceback
from jupiter_AI import client, JUPITER_DB
db = client[JUPITER_DB]
from jupiter_AI.tiles.workflow.main import calculate_workflow_tiles

try:
    # form = cgi.FieldStorage()
    # tiles_input_json = form.getvalue('tiles_input')
    # tiles_input = json.loads(tiles_input_json)

    tiles_input = {'trigger_id': 'BKTI000001', 'price_recommendation': 540}

    if 'JUP_DB_Workflow' in db.collection_names():
        rec_doc_crsr = db.JUP_DB_Workflow_1.find({
            'trigger_id': tiles_input['trigger_id']
        })
        if rec_doc_crsr.count() == 1:
            rec_doc = rec_doc_crsr[0]
            del rec_doc['_id']
            rec_doc['price_recommendation'] = tiles_input['price_recommendation']
            response = calculate_workflow_tiles(recommendation=rec_doc)

            print response
        else:
            print 'No Doc in Workflow collection for this trigger ID'
    else:
        print 'JUP_DB_Workflow collection does not exist'
except Exception as error_msg:
    traceback.print_exc()
    import traceback
    from time import strftime, localtime
    from jupiter_AI.common.mail_error_msg import send_simple_message
    from jupiter_AI import NOTIFICATION_EMAIL

    p = ''.join(['ERROR : ',
                 traceback.format_exc(),
                 ' \nTIME : ',
                 strftime("%a, %d %b %Y %H:%M:%S ", localtime()),
                 'IST'])
    send_simple_message(to=NOTIFICATION_EMAIL, subject='Workflow Scr Tiles', body=p)






