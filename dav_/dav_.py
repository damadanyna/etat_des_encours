import pandas as pd

arr_columns=['id','arrangement', 'reference', 'agency', 'account', 'assigne', 'type', 'created_at', 'updated_at']
arr_df = pd.read_csv('input/arrangement (25).csv', dtype=str, header=None)
arr_df.columns = arr_columns
arr_df = arr_df[['arrangement', 'reference', 'agency', 'account', 'assigne']]
arr_df =  arr_df[arr_df['assigne'] == 'OUI']
dav_df = pd.read_excel('input/DAV2.xlsx', dtype=str)
limit_df = pd.read_excel('input/LIMIT_MCBC_LIVE_FULL.xlsx', dtype=str)

print(len(dav_df))
print(len(arr_df))

arr_df.head()

neg_dav_df = dav_df[pd.to_numeric(dav_df["montant_capital"]) < 0]
len(neg_dav_df)

neg_dav_df.shape
arr_df[arr_df['reference'] == '20000000869' ]

arr_dav_df = pd.merge(dav_df, arr_df, how='inner', left_on='Numero_compte', right_on='reference')
print(len(dav_df))
print(len(arr_dav_df))

duplicate_counts = arr_dav_df["Numero_compte"].value_counts()
duplicates = duplicate_counts[duplicate_counts > 1]
print(duplicates)

limit_df.head()

filtered_arr_dav_df = arr_dav_df[~arr_dav_df["arrangement"].isin(limit_df["ACCOUNT"])]

print(len(filtered_arr_dav_df))


arr_dav_limit_df = pd.merge(arr_dav_df, limit_df, how='inner', left_on='reference', right_on='ACCOUNT')
print(len(arr_dav_df))
print(len(arr_dav_limit_df))
# 8400
# 2900
# 300

arr_dav_limit_df[arr_dav_limit_df['LIMIT.PRODUCT'] == '8400']

len(arr_dav_limit_df)

arr_dav_limit_df.to_csv("output/DAV_with_limit_df.csv", sep=';')
arr_dav_limit_df.to_excel("output/DAV_with_limit_df.xlsx")

print(f"Terminé")
