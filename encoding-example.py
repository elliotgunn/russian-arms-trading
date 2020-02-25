# creates a list of all CONSIGNEE_COUNTRY values for known arms dealers
temp_df = df_arms.select('CONSIGNEE_COUNTRY').distinct().collect()
arms_CONSIGNEE_COUNTRY = [temp_df[i]['CONSIGNEE_COUNTRY'] for i in range(len(temp_df))]
del(temp_df)

# creates a new dataframe for all the encoded columns
df_encoded = df_trade

# create user defined functions to apply to each column
func_CONSIGNEE_COUNTRY = F.udf(lambda x: 1 if (x in arms_CONSIGNEE_COUNTRY) else 0)

# applies udfs and removes columns not being used in the model
df_encoded = df_encoded.drop('column')\
                       .withColumn('CONSIGNEE_COUNTRY', func_CONSIGNEE_COUNTRY(df_trade['CONSIGNEE_COUNTRY']))\

#-----------------------------------------------------------------------------------------------------------#

# converts different date columns to timestamps
date_cols = ['DATE-COLUMN-1','DATE-COLUMN-2','DATE-COLUMN-3']
for n in date_cols:
    df_encoded = df_encoded.withColumn(n,
                                       F.to_date(F.unix_timestamp(F.col(n), 'yyyy-MM-dd').cast('timestamp')))

to_int = F.udf(lambda x: int(x))    
    
# seperates date columns into 3 columns for month/day/year and converts values to int
#then deletes original column
for n in date_cols:
    df_encoded = df_encoded.withColumn(n + '_day', F.dayofmonth(n)).withColumn(n + '_day', to_int(n + '_day'))
    df_encoded = df_encoded.withColumn(n + '_month', F.month(n)).withColumn(n + '_month', to_int(n + '_month'))
    df_encoded = df_encoded.withColumn(n + '_year', F.year(n)).withColumn(n + '_year', to_int(n + '_year'))
    df_encoded = df_encoded.drop(n)
    
#-----------------------------------------------------------------------------------------------------------#

# save encoded dataframe to s3
df_encoded.write.save('s3://folder/subfolder/trade_encoded.csv', format='csv', header=True)
