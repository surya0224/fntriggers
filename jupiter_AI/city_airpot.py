import pandas as pd
from jupiter_AI import mongo_client, JUPITER_DB
from copy import deepcopy
client = mongo_client()
db = client[JUPITER_DB]

crsr= db.Temp_fzDB_tbl_003.find({}, {'_id': 0})
tax_df = pd.DataFrame(list(crsr))
mapping_df = pd.DataFrame(list(db.JUP_DB_City_Airport_Mapping.find({}, {'Airport_Code': 1,
                                                                        'City_Code': 1,
                                                                      '_id': 0})))



df = pd.DataFrame()
for index, row in tax_df.iterrows():
    if len(mapping_df[mapping_df['Airport_Code'] == row['Origin']]['City_Code'].values)>0 and \
            len(mapping_df[mapping_df['Airport_Code'] == row['Destination']]['City_Code'].values) > 0:

        if mapping_df[mapping_df['Airport_Code']==row['Origin']]['City_Code'].values[0]=='UAE':
            city = mapping_df[mapping_df['Airport_Code']==row['Destination']]['City_Code'].values[0]
            dest_airports = mapping_df[mapping_df['City_Code']==city]['Airport_Code'].values
            print dest_airports
            for airport in dest_airports:
                temp_df = pd.DataFrame(row).transpose()
                temp_df['Destination'] = airport
                df = pd.concat([df, temp_df])
            temp_df = pd.DataFrame(row).transpose()
            temp_df['Destination'] = city
        elif mapping_df[mapping_df['Airport_Code'] == row['Destination']]['City_Code'].values[0] == 'UAE':
            city = mapping_df[mapping_df['Airport_Code'] == row['Origin']]['City_Code'].values[0]
            org_airports = mapping_df[mapping_df['City_Code'] == city]['Airport_Code'].values
            print org_airports
            for airport in org_airports:
                temp_df = pd.DataFrame(row).transpose()
                temp_df['Origin'] = airport
                df = pd.concat([df, temp_df])
            temp_df = pd.DataFrame(row).transpose()
            temp_df['Origin'] = city
            df = pd.concat([df, temp_df])
        else:
            df_ = pd.DataFrame(row).transpose()
            od_df = pd.DataFrame()
            od_df['Origin'] = df_['Origin']
            od_df['Destination'] = df_['Destination']
            od_df['OD'] = od_df['Origin'] + od_df['Destination']
            #od_df = pd.DataFrame(row[''].values, columns=['OD'])
            od_df.drop(['Origin', 'Destination'], axis=1, inplace=True)
            od_df['origin'] = od_df['OD'].str.slice(0, 3)
            od_df['destination'] = od_df['OD'].str.slice(3, 6)
            od_df = od_df.merge(mapping_df.rename(columns={'Airport_Code': 'origin', 'City_Code': 'pseudo_origin'}),
                                on='origin', how='left')
            od_df['pseudo_origin'].fillna(od_df['origin'], inplace=True)
            od_df = od_df.merge(mapping_df.rename(columns={'City_Code': 'pseudo_origin'}), on='pseudo_origin', how='left')

            od_df['Airport_Code'].fillna(od_df['origin'], inplace=True)
            od_df.drop(['origin'], axis=1, inplace=True)
            od_df.rename(columns={'Airport_Code': 'origin'}, inplace=True)


            od_df = od_df.merge(mapping_df.rename(columns={'Airport_Code': 'destination', 'City_Code': 'pseudo_destination'}),
                                on='destination', how='left')
            od_df['pseudo_destination'].fillna(od_df['destination'], inplace=True)
            od_df = od_df.merge(mapping_df.rename(columns={'City_Code': 'pseudo_destination'}), on='pseudo_destination',
                                how='left')

            od_df['Airport_Code'].fillna(od_df['destination'], inplace=True)
            od_df.drop(['destination'], axis=1, inplace=True)
            od_df.rename(columns={'Airport_Code': 'destination'}, inplace=True)

            od_df['pseudo_od'] = od_df['pseudo_origin'] + od_df['pseudo_destination']
            od_df.drop(['OD'], axis=1, inplace=True)
            od_df['od'] = od_df['origin'] + od_df['destination']
            #print od_df
            #print df_
            ods = list(set(list(od_df['od'].values)+list(od_df['pseudo_od'].values)))
            print ods
            for od in ods:
                temp_df = pd.DataFrame(row).transpose()
                temp_df['Origin'] = od[:3]
                temp_df['Destination'] = od[3:]
                df = pd.concat([df, temp_df])
                #print temp_df.to_string()
            #print "DDADJABDJABJBJBJBJBJABJDA"

print df.shape
df.drop_duplicates(['Origin', 'Destination', 'Compartment', 'Currency', 'OW_RT', 'pos', 'chnl'], inplace=True)
print df.shape
print tax_df.shape
tax_df = pd.concat([df, tax_df])
tax_df.drop_duplicates(['Origin', 'Destination', 'Compartment', 'Currency', 'OW_RT', 'pos', 'chnl'], inplace=True)
print tax_df.shape
df.to_json('tax_master.json', orient='records')
#db.Tmp1.insert(tax_df.to_dict('records'))

    # else:
    #     df_ = pd.DataFrame(row).to_transpose()
    #     df_['OD'] = df_['Origin'] + df_['Destination']
    #     od_df = pd.DataFrame(row[''].values, columns=['OD'])
    #     od_df['origin'] = od_df['OD'].str.slice(0, 3)
    #     od_df['destination'] = od_df['OD'].str.slice(3, 6)
    #     od_df = od_df.merge(mapping_df.rename(columns={'Airport_Code': 'origin', 'City_Code': 'pseudo_origin'}),
    #                         on='origin', how='left')
    #     od_df['pseudo_origin'].fillna(od_df['origin'], inplace=True)
    #     od_df = od_df.merge(mapping_df.rename(columns={'City_Code': 'pseudo_origin'}), on='pseudo_origin', how='left')
    #
    #     od_df['Airport_Code'].fillna(od_df['origin'], inplace=True)
    #     od_df.drop(['origin'], axis=1, inplace=True)
    #     od_df.rename(columns={'Airport_Code': 'origin'}, inplace=True)
    #
    #
    #     od_df = od_df.merge(mapping_df.rename(columns={'Airport_Code': 'destination', 'City_Code': 'pseudo_destination'}),
    #                         on='destination', how='left')
    #     od_df['pseudo_destination'].fillna(od_df['destination'], inplace=True)
    #     od_df = od_df.merge(mapping_df.rename(columns={'City_Code': 'pseudo_destination'}), on='pseudo_destination',
    #                         how='left')
    #
    #     od_df['Airport_Code'].fillna(od_df['destination'], inplace=True)
    #     od_df.drop(['destination'], axis=1, inplace=True)
    #     od_df.rename(columns={'Airport_Code': 'destination'}, inplace=True)
    #
    #     od_df['pseudo_od'] = od_df['pseudo_origin'] + od_df['pseudo_destination']
    #     od_df.drop(['OD'], axis=1, inplace=True)
    #     print od_df

# tax_df['od'] = tax_df['Origin']+tax_df['Destination']
# print tax_df.head().to_string()
#
# list_ods = list(set(tax_df['od'].values))
#
# mapping_df = pd.DataFrame(list(db.JUP_DB_City_Airport_Mapping.find({}, {'Airport_Code': 1,
#                                                                         'City_Code': 1,
#                                                                         '_id': 0})))
# od_df = pd.DataFrame(list_ods, columns=['OD'])
# od_df['origin'] = od_df['OD'].str.slice(0, 3)
# od_df['destination'] = od_df['OD'].str.slice(3, 6)
# od_df = od_df.merge(mapping_df.rename(columns={'Airport_Code': 'origin', 'City_Code': 'pseudo_origin'}),
#                     on='origin', how='left')
# od_df['pseudo_origin'].fillna(od_df['origin'], inplace=True)
# od_df = od_df.merge(mapping_df.rename(columns={'City_Code': 'pseudo_origin'}), on='pseudo_origin', how='left')
#
# od_df['Airport_Code'].fillna(od_df['origin'], inplace=True)
# od_df.drop(['origin'], axis=1, inplace=True)
# od_df.rename(columns={'Airport_Code': 'origin'}, inplace=True)
#
#
# od_df = od_df.merge(mapping_df.rename(columns={'Airport_Code': 'destination', 'City_Code': 'pseudo_destination'}),
#                     on='destination', how='left')
# od_df['pseudo_destination'].fillna(od_df['destination'], inplace=True)
# od_df = od_df.merge(mapping_df.rename(columns={'City_Code': 'pseudo_destination'}), on='pseudo_destination',
#                     how='left')
#
# od_df['Airport_Code'].fillna(od_df['destination'], inplace=True)
# od_df.drop(['destination'], axis=1, inplace=True)
# od_df.rename(columns={'Airport_Code': 'destination'}, inplace=True)
#
# od_df['pseudo_od'] = od_df['pseudo_origin'] + od_df['pseudo_destination']
# od_df.drop(['OD'], axis=1, inplace=True)
# od_df['od'] = od_df['origin']+od_df['destination']
# od_df.drop_duplicates(subset=['pseudo_od', 'od'], inplace=True)
# od_df['flag']=False
# mask = (od_df['pseudo_origin']=='UAE') | (od_df['pseudo_destination']=='UAE')
#
# od_df['flag']=mask
# od_df = od_df[od_df['flag'] == False]
#
# print od_df
#
# tax_df = tax_df.merge(od_df, on='od', how="left")
# #tax_df['pseudo_origin'] = tax_df['pseudo_od'].str.slice(0, 3)
# #tax_df['pseudo_destination'] = tax_df['pseudo_od'].str.slice(3, )
# tax_df.drop(['origin', 'destination'], inplace=True, axis=1)
# print tax_df[tax_df['od']=="DXBLJU"].to_string()
# tax_df.drop(['od'], inplace=True, axis=1)
#
# tax_df =od_df.merge(tax_df, on=['pseudo_od'], how="left")
#
# print tax_df[tax_df['od']=="DXBLJU"].to_string()
#
#
#
#
