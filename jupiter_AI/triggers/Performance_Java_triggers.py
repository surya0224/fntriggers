from jupiter_AI import client, JUPITER_DB, JAVA_URL
import time
import requests
import pandas as pd
import json
import logging
#db = client[JUPITER_DB]

st = time.time()
# logging.basicConfig(filename='Performance_java_triggers.log', level=logging.DEBUG)


def hit_url(pos, dep_date_start, dep_date_end, db):
    parameters = {
    "fromDate": dep_date_start,
    "toDate": dep_date_end,
    "posMap": {

          "cityArray":[pos]
    },
    "originMap": {
    "networkArray": ['Network']
    },
    "destMap": {
    "networkArray": ['Network']
    },
    "compMap": {
    "compartmentArray": ['Y', 'J']
    }
    }

    headers = {"Connection": "keep-alive",
    "Content-Length": "257",
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.52 Safari/536.5",
    "Content-Type": "application/json",
    "Accept": "*/*",
    "Accept-Encoding": "gzip,deflate",
    "Accept-Language": "q=0.8,en-US",
    "Accept-Charset": "utf-8"
    }
    try:
        response = requests.post(JAVA_URL, data=json.dumps(parameters), headers=headers)
        a = json.loads(response.text)['ManualTriggerGrid']
        df = pd.DataFrame(a)
        logging.info("GOT RESPONSE")
        print "GOT RESPONSE"
        logging.info("Length of df = " + str(len(df)))
        logging.info("STATUS CODE = " + str(response.status_code))
        if len(df) > 0:
            logging.info("GHUSA IF MEIN")
            try:
                var = df.to_dict('records')
                db.Temp_fzDB_tbl_002.insert_many(var)
                logging.info("INSERTED RESPONSE")
                print "INSERTED RESPONSE"
            except Exception as e:
                print "INSIDE EXCEPT BLOCK"
                logging.info(str(e))
                print "ERROR = ", str(e)[:300]
        else:
            print "INSIDE ELSE BLOCK BECAUSE YOU KNOW JAVA"
            pass
    except Exception as e:
        logging.info("ERROR:"+str(e))
        logging.debug("POS DEP_DATE_START DEP_DATE_END " + str(pos) + "  " + str(dep_date_start) + "  " + str(dep_date_end))
        print "ERROR: ", str(e)


def main_helper():
    start_time = time.time()
    db = client[JUPITER_DB]
    id_list = db.Temp_fzDB_tbl_001.aggregate([
		{
			'$addFields':
				{
				     'pos': {'$substr': ['$market', 0, 3]},
				     'origin': {'$substr': ['$market', 3, 3]},
				     'destination': {'$substr': ['$market', 6, 3]},
				     'compartment': {'$substr': ['$market', 9, 1]}
				}
		},{

			'$group':{
						"_id":{
									'pos': '$pos',
									'dep_date_start': '$dep_date_start',
									'dep_date_end': '$dep_date_end'
							  }
					 }
		  },{
				'$project':{
								'pos': '$_id.pos',
								'dep_date_start': '$_id.dep_date_start',
								'dep_date_end': '$_id.dep_date_end'
							}
			}])
    p = Pool(processes=8)
    x = p.map(hit_url, id_list, chunksize=100)
    print "time_taken_completed: ", time.time()-start_time


if __name__ == '__main__':
    main_helper()
