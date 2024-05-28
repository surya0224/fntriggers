import pymongo
from pymongo import MongoClient

db_ATPCO=pymongo.MongoClient("172.29.4.5:27022")["ATPCO_prod"]
db_ATPCO.authenticate('pdssETLUser', 'pdssETL@123', source='admin')

#db_ATPCO = client.ATPCO_stg_Aug
#db_fzDB = client.fzDB_stg
record_0 = db_ATPCO.JUP_DB_ATPCO_Record_0
record_1 = db_ATPCO.JUP_DB_ATPCO_Record_1
record_2_cat_all = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_All
record_2_cat_3_fn = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_03_FN
record_2_cat_11_fn = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_11_FN
record_2_cat_14_fn = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_14_FN
record_2_cat_15_fn = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_15_FN
record_2_cat_23_fn = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_23_FN
record_2_cat_10 = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_10
record_2_cat_25 = db_ATPCO.JUP_DB_ATPCO_Record_2_Cat_25
#fare_rules = db_fzDB.JUP_DB_ATPCO_Fares_Rules

record_2_cat_all.create_index([
    ("CXR_CODE" , 1),
    ("CAT_NO", 1),
    ("FT_NT" , 1),
    ("GEN_APPL" , 1),
    ("LOC_1_AREA" , 1),
    ("LOC_1_CITY" , 1),
    ("LOC_1_COUNTRY" , 1),
    ("LOC_1_ZONE" , 1),
    ("LOC_2_AREA" , 1),
    ("LOC_2_CITY" , 1),
    ("LOC_2_COUNTRY" , 1),
    ("LOC_2_ZONE" , 1),
    ("NO_APPL" , 1),
    ("OW_RT" , 1),
    ("RTG_NO" , 1),
    ("RULE_NO" , 1),
    ("DATES_EFF_1",1),
    ("DATES_DISC_1",1),
    ("TYPE_CODES_DAY_OF_WEEK_TYPE" , 1),
    ("TYPE_CODES_FARE_TYPE" , 1),
    ("TYPE_CODES_SEASON_TYPE" , 1),
    ("TARIFF", 1)],name="fare_rules_0", background=True)
record_2_cat_all.create_index([
    ("CXR_CODE" , 1),
    ("FT_NT" , 1),
    ("LOC_1_AREA" , 1),
    ("LOC_1_CITY" , 1),
    ("LOC_1_COUNTRY" , 1),
    ("LOC_1_ZONE" , 1),
    ("LOC_2_AREA" , 1),
    ("LOC_2_CITY" , 1),
    ("LOC_2_COUNTRY" , 1),
    ("LOC_2_ZONE" , 1),
    ("NO_APPL" , 1),
    ("OW_RT" , 1),
    ("RTG_NO" , 1),
    ("RULE_NO" , 1),
    ("DATES_EFF_1",1),
    ("DATES_DISC_1",1),
    ("TYPE_CODES_DAY_OF_WEEK_TYPE" , 1),
    ("TYPE_CODES_FARE_TYPE" , 1),
    ("TYPE_CODES_SEASON_TYPE" , 1),
    ("TARIFF", 1)],name="fare_rules_2")
record_2_cat_10.create_index([
    ("CXR_CODE" , 1),
    ("FT_NT" , 1),
    ("LOC_1_AREA" , 1),
    ("LOC_1_CITY" , 1),
    ("LOC_1_COUNTRY" , 1),
    ("LOC_1_ZONE" , 1),
    ("LOC_2_AREA" , 1),
    ("LOC_2_CITY" , 1),
    ("LOC_2_COUNTRY" , 1),
    ("LOC_2_ZONE" , 1),
    ("NO_APPL" , 1),
    ("OW_RT" , 1),
    ("RTG_NO" , 1),
    ("RULE_NO" , 1),
    ("DATES_EFF_1",1),
    ("DATES_DISC_1",1),
    ("TYPE_CODES_DAY_OF_WEEK_TYPE" , 1),
    ("TYPE_CODES_FARE_TYPE" , 1),
    ("TYPE_CODES_SEASON_TYPE" , 1),
    ("TARIFF", 1)],name="fare_rules",background=True)
record_2_cat_25.create_index([
    ("CXR_CODE" , 1),
    ("FT_NT" , 1),
    ("LOC_1_AREA" , 1),
    ("LOC_1_CITY" , 1),
    ("LOC_1_COUNTRY" , 1),
    ("LOC_1_ZONE" , 1),
    ("LOC_2_AREA" , 1),
    ("LOC_2_CITY" , 1),
    ("LOC_2_COUNTRY" , 1),
    ("LOC_2_ZONE" , 1),
    ("NO_APPL" , 1),
    ("OW_RT" , 1),
    ("RTG_NO" , 1),
    ("RULE_NO" , 1),
    ("DATES_EFF_1",1),
    ("DATES_DISC_1",1),
    ("TYPE_CODES_DAY_OF_WEEK_TYPE" , 1),
    ("TYPE_CODES_FARE_TYPE" , 1),
    ("TYPE_CODES_SEASON_TYPE" , 1),
    ("TARIFF", 1)],name="fare_rules",background=True)
record_2_cat_3_fn.create_index([
    ("CXR_CODE" , 1),
    ("FT_NT" , 1),
    ("DATES_EFF_1",1),
    ("DATES_DISC_1",1),
    # ("LOC_1_AREA" , 1),
    ("LOC_1_CITY" , 1),
    ("LOC_1_COUNTRY" , 1),
    # ("LOC_1_ZONE" , 1),
    # ("LOC_2_AREA" , 1),
    ("LOC_2_CITY" , 1),
    ("LOC_2_COUNTRY" , 1),
    # ("LOC_2_ZONE" , 1),
    ("NO_APPL" , 1),
    ("OW_RT" , 1),
    ("RTG_NO" , 1),
    ("TARIFF", 1)],name="fare_rules",background=True)
record_2_cat_11_fn.create_index([
    ("CXR_CODE" , 1),
    ("FT_NT" , 1),
    ("DATES_EFF_1",1),
    ("DATES_DISC_1",1),
    # ("LOC_1_AREA" , 1),
    ("LOC_1_CITY" , 1),
    ("LOC_1_COUNTRY" , 1),
    # ("LOC_1_ZONE" , 1),
    # ("LOC_2_AREA" , 1),
    ("LOC_2_CITY" , 1),
    ("LOC_2_COUNTRY" , 1),
    # ("LOC_2_ZONE" , 1),
    ("NO_APPL" , 1),
    ("OW_RT" , 1),
    ("RTG_NO" , 1),
    ("TARIFF", 1)],name="fare_rules",background=True)
record_2_cat_14_fn.create_index([
    ("CXR_CODE" , 1),
    ("FT_NT" , 1),
    ("DATES_EFF_1",1),
    ("DATES_DISC_1",1),
    # ("LOC_1_AREA" , 1),
    ("LOC_1_CITY" , 1),
    ("LOC_1_COUNTRY" , 1),
    # ("LOC_1_ZONE" , 1),
    # ("LOC_2_AREA" , 1),
    ("LOC_2_CITY" , 1),
    ("LOC_2_COUNTRY" , 1),
    # ("LOC_2_ZONE" , 1),
    ("NO_APPL" , 1),
    ("OW_RT" , 1),
    ("RTG_NO" , 1),
    ("TARIFF", 1)],name="fare_rules",background=True)
record_2_cat_15_fn.create_index([
    ("CXR_CODE" , 1),
    ("FT_NT" , 1),
    ("DATES_EFF_1",1),
    ("DATES_DISC_1",1),
    # ("LOC_1_AREA" , 1),
    ("LOC_1_CITY" , 1),
    ("LOC_1_COUNTRY" , 1),
    # ("LOC_1_ZONE" , 1),
    # ("LOC_2_AREA" , 1),
    ("LOC_2_CITY" , 1),
    ("LOC_2_COUNTRY" , 1),
    # ("LOC_2_ZONE" , 1),
    ("NO_APPL" , 1),
    ("OW_RT" , 1),
    ("RTG_NO" , 1),
    ("TARIFF", 1)],name="fare_rules",background=True)
record_2_cat_23_fn.create_index([
    ("CXR_CODE" , 1),
    ("FT_NT" , 1),
    ("DATES_EFF_1",1),
    ("DATES_DISC_1",1),
    # ("LOC_1_AREA" , 1),
    ("LOC_1_CITY" , 1),
    ("LOC_1_COUNTRY" , 1),
    # ("LOC_1_ZONE" , 1),
    # ("LOC_2_AREA" , 1),
    ("LOC_2_CITY" , 1),
    ("LOC_2_COUNTRY" , 1),
    # ("LOC_2_ZONE" , 1),
    ("NO_APPL" , 1),
    ("OW_RT" , 1),
    ("RTG_NO" , 1),
    ("TARIFF", 1)],name="fare_rules",background=True)
record_1.create_index([
    ("FARE_CLASS", 1),
    ("CXR_CODE" , 1),
    ("FT_NT" , 1),
    ("RTG_NO", 1),
    ("DATES_EFF_1",1),
    ("DATES_DISC_1",1),
    ("LOC_1_AREA" , 1),
    ("LOC_1_CITY" , 1),
    ("LOC_1_COUNTRY" , 1),
    ("LOC_1_ZONE" , 1),
    ("LOC_2_AREA" , 1),
    ("LOC_2_CITY" , 1),
    ("LOC_2_COUNTRY" , 1),
    ("LOC_2_ZONE" , 1),
    ("OW_RT" , 1),
    ("RULE_NO" , 1),
    ("TARIFF", 1)],name="fare_rules",background=True)
record_0.create_index([
    ("CXR_CODE", 1),
    ("TARIFF", 1),
    ("DATES_EFF_1",1),
    ("DATES_DISC_1",1)],name="fare_rules",background=True)
# fare_rules.create_index([
#     ("used", 1),
#     ("carrier", 1)
# ])
# fare_rules.create_index([
#     ("used", 1),
#     ("carrier", 1),
#     ("tariff_code", 1)
# ])