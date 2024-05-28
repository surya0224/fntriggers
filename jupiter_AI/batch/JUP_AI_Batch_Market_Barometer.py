"""
"""
import json
from jupiter_AI import client, JUPITER_DB
db = client[JUPITER_DB]
from jupiter_AI.network_level_params import query_month_year_builder, today
start_date_obj = today
from_month = start_date_obj.month
from_year = start_date_obj.year
to_month = start_date_obj.month
to_year = start_date_obj.year + 1
month_year_combinations = query_month_year_builder(from_month,from_year,to_month,to_year)
month_array = [combination['month'] for combination in month_year_combinations]
year_array = [combination['year'] for combination in month_year_combinations]
for idx,month in enumerate(month_array):
    month_input = json.dumps([month])
    year_input = json.dumps([year_array[idx]])
    col_name = json.dumps('Temp_Collection_br')
    print month_input
    print year_input
    eval_str = ''.join(['JUP_FN_Market_dhb_Market_Barometer_detail(',
                        str(year_input),',',
                        str(month_input),',',
                        'null,null,null,null,null,',col_name,')'])
    print eval_str
    db.eval(eval_str)
    crsr_barometer = db.Temp_Collection_br.find()
    print crsr_barometer.count()
    for doc in crsr_barometer:
        doc['last_update_date'] = str(start_date_obj)
        doc['dep_month'] = month
        doc['dep_year'] = year_array[idx]
        doc['update_month'] = from_month
        doc['update_year'] = from_year
        db.JUP_DB_Market_Barometer.insert_one(doc)
    print 'DONE,',month,',',year_array[idx]
    print db.JUP_DB_Market_Barometer.find().count()
    db.Temp_Collection_br.drop()