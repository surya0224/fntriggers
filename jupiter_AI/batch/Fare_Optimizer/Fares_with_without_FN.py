from pymongo import MongoClient
import numpy as np
from datetime import datetime, timedelta

from jupiter_AI import client, JUPITER_DB, ATPCO_DB, SYSTEM_DATE, today, Host_Airline_Code, Host_Airline_Hub, JUPITER_LOGGER
from jupiter_AI.logutils import measure

FZDB = client[JUPITER_DB]
ATPCO = client[ATPCO_DB]

# HOST_Airline_CodeDB.drop_collection('expired_FN_fares')
fare_rules = FZDB.JUP_DB_ATPCO_Fares_Rules
expired_FN_fares = FZDB.expired_fares_with_without_FN
expired_FN_fares.remove({})

# today = datetime(2018, 2, 15)
# SYSTEM_DATE = datetime.strftime(today, '%Y-%m-%d')
query_date = datetime.strftime(today, "%Y%m%d")
cur_fare_rules = fare_rules.find({"carrier":Host_Airline_Code, 'effective_from': {'$lte': SYSTEM_DATE},
                                  '$or': [{'effective_to': None}, {'effective_to': {'$gte': SYSTEM_DATE}}]})
temp1_date = []
temp2_date = []

count = 1
for i in cur_fare_rules:
    print count
    j = 0
    while (j < 200):
        try:
            sale_dates_latest_tktg = datetime.strptime(i["Footnotes"]["Cat_15_FN"][j]["SALE_DATES_LATEST_TKTG"][1:],
                                                       "%y%m%d")
            temp1_date.append(sale_dates_latest_tktg)
            #         temp_tbl_no.append(i['Footnotes']['Cat_15_FN']['TBL_NO'])
            print i["Footnotes"]["Cat_15_FN"][j]["SALE_DATES_LATEST_TKTG"], ".........."
        except ValueError:
            pass
        except KeyError:
            #             print "pass key"
            break
        except IndexError:
            #             print "index break"
            break
        j += 1

    j = 0
    while (j <= 200):
        try:
            ticket_on_before = datetime.strptime(i["cat_15"][j]["SALE_DATES_LATEST_TKTG"][1:], "%y%m%d")
            temp2_date.append(ticket_on_before)
            #         temp_tbl_no.append(i['Footnotes']['Cat_15_FN']['TBL_NO'])
            print i["cat_15"][j]["SALE_DATES_LATEST_TKTG"], "#########"
        except ValueError:
            pass
        except IndexError:
            break
        except KeyError:
            break
        j += 1
    count += 1

temp1_date = np.array(temp1_date)
expired_FN_dates = temp1_date[temp1_date < (today - timedelta(days = 14))]
print len(expired_FN_dates)


@measure(JUPITER_LOGGER)
def f(x):
    return str(0)+x.strftime("%y%m%d")


f = np.vectorize(f)

result1 = f(expired_FN_dates)
print result1

temp = [unichr(i) for i in range(65,91,1)]
for i in range(1,100,1):
    temp.append(str(i))
for i in range(65,91,1):
    for j in range(65,91,1):
        temp.append(unichr(i)+unichr(j))
add_on_fn = ["1F","2F","3F","4F","5F","6F","7F","8F","9F","1T","2T","3T","4T","5T","6T","7T","8T","9T"]
temp = temp + add_on_fn
print temp

cur_expired_FN_fares = fare_rules.find({"Footnotes.Cat_15_FN.SALE_DATES_LATEST_TKTG":{"$in":list(result1)},
                                        "footnote":{"$nin":temp}, "carrier":Host_Airline_Code,
                                        'effective_from': {'$lte': SYSTEM_DATE},
                                        '$or': [{'effective_to': None}, {'effective_to': {'$gte': SYSTEM_DATE}}]
                                        })
print 'footnote expired fares:', cur_expired_FN_fares.count()
for i in cur_expired_FN_fares:
    expired_FN_fares.save(i)

temp2_date = np.array(temp2_date)
print temp2_date

expired_fares_dates = temp2_date[temp2_date < (today - timedelta(days = 14))]
print expired_fares_dates


@measure(JUPITER_LOGGER)
def f(x):
    return str(0)+x.strftime("%y%m%d")


f = np.vectorize(f)

result2 = f(expired_fares_dates)

cur_expired_fares = fare_rules.find({"cat_15.SALE_DATES_LATEST_TKTG":{"$in":list(result2)},"carrier":{"$eq":Host_Airline_Code},
                                     'effective_from': {'$lte': SYSTEM_DATE},
                                     '$or': [{'effective_to': None}, {'effective_to': {'$gte': SYSTEM_DATE}}]
                                    })
print 'non footnote expired fares', cur_expired_fares.count()

for i in cur_expired_fares:
    expired_FN_fares.save(i)
