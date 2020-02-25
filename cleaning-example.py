# defines regex expressions to apply to trade data
regexINN = '(\d{8,12}|None|null|0|00)'
regexNOTDIGIT = '[^0-9]'
regexADDRESS = '(null|None)|(\b[a-zA-z]{1,3}\b)|(\d{2,})'
regex2CHAR = '(None|[a-zA-Z]{2}|\d{2})'
regexDATE = '(None|null|\d{4}-\d{2}-\d{2})'

# applying regex to the trade data
df_trade_filtered = df_trade.filter(df_trade['CONSIGNOR_NAME'].rlike(regexNOTDIGIT))\
                            .filter(df_trade['DECLARATION_NUMBER'].rlike(regexDECLARATION_NUMBER))\
                            .filter(df_trade['CUSTOMS_POST_CODE'].rlike(regex8DIGIT))\
                            .filter(df_trade['CUSTOMS_POST_NAME'].rlike(regexCUSTOMS_POST_NAME))\
                            .filter(df_trade['DATE_EXTRACTED'].rlike(regexDATE))\
                            .filter(df_trade['CONSIGNOR_INN'].rlike(regexINN))\
                            .filter(df_trade['CONSIGNOR_NAME_nosuffix'].rlike(regexNOTDIGIT))\

# save trade data to the s3 bucket
df_trade_filtered.write.save('s3://folder/subfolder/trade_data_clean.parquet')

# create a dataframe for all trade interactions with known arms exporters
# arms_inn_str refers to the list of the 59 known arms exporters, their INNs converted to str
# saves known arms exporters to s3 bucket
df_arms_traders = df_trade_filtered.filter(df_trade_filtered['CONSIGNOR_INN'].isin(arms_inn_str))
df_arms_traders.write.save('s3://folder/subfolder/arms_traders_clean.parquet')
