from jupiter_AI import mongo_client, JUPITER_DB, JUPITER_LOGGER, ATPCO_DB
from jupiter_AI import SYSTEM_DATE, today
import pandas as pd
import datetime

def main(client):
    try :
        atpco_db = client[ATPCO_DB]
        db = client[JUPITER_DB]

        g16_crsr = atpco_db.JUP_DB_ATPCO_G16_Tariff_Master.find({}, {"_id": 0,
                                                                     "FARE_CLASS_TARIFF_CD": 1, "FARE_CLASS_TARIFF_NUM": 1})
        df_g16 = pd.DataFrame(list(g16_crsr))
        df_g16.drop_duplicates(inplace=True)
        df_g16 = df_g16.rename(columns = {"FARE_CLASS_TARIFF_CD": "ALPHANUMERIC", "FARE_CLASS_TARIFF_NUM": "TARIFF"})
        print df_g16.head()


        crsr = db.JUP_DB_ATPCO_Fares_Rules.aggregate([
            {"$match": {

                    "carrier": "EK",
                    "fare_include": True
                    # "effective_to": {
                    #             "$gte": SYSTEM_DATE}
                    #
                    #
                }
            },
            {
                "$project": {
                    "_id": None,
                    "OD": "$OD",
                    "TARIFF": "$tariff_code",
                    "RULE_NO": "$fare_rule",
                    "TARIFFTYPE": "$private_fare"
                }
            },

            {"$group": {
                "_id": {
                    "TARIFF": "$TARIFF",
                    "RULE_NO": "$RULE_NO",
                    "TARIFFTYPE": "$TARIFFTYPE"
                },
                "OD": {"$addToSet": "$OD"}

            }
            },


            {"$project": {
                "_id": None,
                "TARIFF": "$_id.TARIFF",
                "RULE_NO": "$_id.RULE_NO",
                #"TARIFF TYPE": "$_id.TARIFF TYPE",
                "OD": "$OD",

                "TARIFF TYPE": {

                    "$cond": [{"$eq": ["$_id.TARIFFTYPE", False]}, "PUBLIC", "PRIVATE"]
                }

            }
            }

        ])

        df = pd.DataFrame(list(crsr))
        print df.head()

        df = df.merge(df_g16, on='TARIFF', how="left")
        df.drop(['_id'], axis=1, inplace=True)
        print df.head()

        #db.Temp_Tariff_Rule_Master.insert(df.to_dict("records"))
        bulk = db.JUP_DB_Tariff_Rule_Master.initialize_ordered_bulk_op()
        for index, row in df.iterrows():
            #print row
            bulk.find({
                'TARIFF': row['TARIFF'],
                'TARIFF TYPE': row['TARIFF TYPE'],
                'RULE_NO': row['RULE_NO'],
                'ALPHANUMERIC': row['ALPHANUMERIC']
            }).upsert().update({
                '$setOnInsert': {
                    'TARIFF': row['TARIFF'],
                    'TARIFF TYPE': row['TARIFF TYPE'],
                    'RULE_NO': row['RULE_NO'],
                    'ALPHANUMERIC': row['ALPHANUMERIC']
                },

                '$set': {'OD': row['OD']}

            })

        bulk.execute()
    except Exception as error:
        print error

if __name__ == '__main__':

    client = mongo_client()
    main(client)
    client.close()

