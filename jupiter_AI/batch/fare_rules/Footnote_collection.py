"""
Author: Abhinav Garg
Date Created: 2017-07-20
File Name: Footnote_collection.py

This code populates the used, available and pruned footnotes by host for all tariffs as a batch file.

"""
from pymongo import MongoClient
from datetime import datetime, timedelta

from jupiter_AI import JUPITER_DB, mongo_client, SYSTEM_DATE, ATPCO_DB, Host_Airline_Code, Host_Airline_Hub, JUPITER_LOGGER
from jupiter_AI.logutils import measure


#@measure(JUPITER_LOGGER)
def get_used_footnotes(tariff, db):
    cur = db.JUP_DB_ATPCO_Fares_Rules.aggregate([
        {
            "$match": {
                "carrier": Host_Airline_Code,
                "tariff_code": tariff,
                "effective_from": {
                    "$lte": SYSTEM_DATE
                },
                "$or": [
                    {
                        "effective_to": None,
                    },
                    {
                        "effective_to": {"$gt": SYSTEM_DATE}
                    }
                ],
                "footnote": {
                    "$ne": "",
                    "$exists": True
                },

            }
        },
        {
            "$project": {
                "effective_from": 1,
                "effective_to": 1,
                "Footnotes": 1,
                "carrier": 1,
                "tariff_code": 1,
                "footnote": 1,

            }
        },
        {
            "$group": {
                "_id": "$footnote",
                "uniqueFootnote": {
                    "$first": "$$ROOT"
                }
            }
        },
        {
            "$project": {
                "Tariff": "$uniqueFootnote.tariff_code",
                "Footnotes_cat_14": "$uniqueFootnote.Footnotes.Cat_14_FN",
                "Footnotes_cat_15": "$uniqueFootnote.Footnotes.Cat_15_FN",
                "Footnotes_cat_3": "$uniqueFootnote.Footnotes.Cat_3_FN",
                "Footnotes_cat_11": "$uniqueFootnote.Footnotes.Cat_11_FN",
                "Footnotes_cat_23": "$uniqueFootnote.Footnotes.Cat_23_FN",
                "footnote": "$uniqueFootnote.footnote",
                "_id": 0
            }
        }
    ])

    return list(cur)


#@measure(JUPITER_LOGGER)
def insert_in_collection(tar, fn_list, system_date, client):
    db_ATPCO = client[ATPCO_DB]
    db = client[JUPITER_DB]
    final_insert = {}
    used_list = []
    print "------"
    cur_fare_rules = get_used_footnotes(tar, db)
    print len(cur_fare_rules)
    final_insert["Tariff"] = tar
    final_insert["Used"] = cur_fare_rules
    final_insert["Purged"] = []
    final_insert["Available"] = []
    if len(cur_fare_rules) != 0:
        countj = 1
        for j in cur_fare_rules:
            print countj, "......."
            try:
                cur_record_2 = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_15_FN.find(
                    {"TARIFF": {"$eq": j["tariff_code"]}, "CXR_CODE": Host_Airline_Code, "FT_NT": {"$eq": j["footnote"]},
                     "SEQ_NO": {"$eq": j["Footnotes_cat_15"][0]["SEQ_NO"]},
                     "DATA_TABLE_STRING_TBL_NO_1": {"$eq": j["Footnotes_cat_15"][0]["TBL_NO"]},
                     }).sort('DATES_DISC_1', -1)
                print cur_record_2.count()
                try:
                    if ((datetime.strptime(cur_record_2[0]["DATES_DISC"][1:], "%y%m%d") >= (
                            system_date - timedelta(days=14)))
                            and (datetime.strptime(cur_record_2[0]["DATES_DISC"][1:], "%y%m%d") < system_date)):
                        print j['footnote'], "is purged"
                        final_insert["Purged"].append(j["footnote"])

                except ValueError:
                    print j['footnote'], "is not purged"

            except KeyError:
                pass
            used_list.append(j['footnote'])
            countj += 1

        available_footnotes = set(fn_list) - set(used_list) - set(final_insert["Purged"])
        for fn in available_footnotes:
            temp_dict = {}
            temp_dict['FN'] = fn
            temp_dict['In_Use'] = 0
            final_insert["Available"].append(temp_dict)

    else:
        for fn in fn_list:
            temp_dict = {}
            temp_dict['FN'] = fn
            temp_dict['In_Use'] = 0
            final_insert["Available"].append(temp_dict)
    if len(final_insert["Purged"]) != 0:
        final_insert["Purged"] = list(set(final_insert["Purged"]))
    db.JUP_DB_ATPCO_Footnotes.insert(final_insert)


#@measure(JUPITER_LOGGER)
def generate_footnotes_collection(client):
    db = client[JUPITER_DB]
    system_date = datetime.strptime(SYSTEM_DATE, '%Y-%m-%d')
    system_date = system_date - timedelta(days=1)
    query_date = datetime.strftime(system_date, "%Y-%m-%d")
    result = db.JUP_DB_ATPCO_Footnotes.delete_many({})
    print "deleted:---", result.deleted_count

    list_alpha = ["A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z"]

    tar = db.JUP_DB_ATPCO_Fares_Rules.distinct('tariff_code', {'carrier': Host_Airline_Code})
    print 'tariff list', tar
    fn_list = []

    for i in range(9):
        fn_list.append(str(i+1))

    for i in range(9):
        for j in range(9):
            fn_list.append(str(i+1)+str(j+1))

    for i in range(26):
        fn_list.append(list_alpha[i])

    for i in range(26):
        for j in range(26):
            fn_list.append(list_alpha[i]+list_alpha[j])

    for i in range(9):
        for j in range(26):
            fn_list.append(str(i+1)+list_alpha[j])

    for i in range(26):
        for j in range(9):
            fn_list.append(list_alpha[i]+str(j+1))

    add_on_fn = ["1F","2F","3F","4F","5F","6F","7F","8F","9F","1T","2T","3T","4T","5T","6T","7T","8T","9T"]
    fn_list = list(set(fn_list) - set(add_on_fn))

    count = 0
    for i in tar:
        insert_in_collection(i, fn_list, system_date, client)
        print 'done ', i
        count += 1


if __name__ == '__main__':
    client = mongo_client()
    generate_footnotes_collection(client=client)
    client.close()

