from iso3166 import countries

# load trade data
df_trade = spark.read.options(header=True, inferSchema=True)
    .parquet('s3://folder/subfolder/trade_data_clean.parquet')

# load known arms traders
df_arms = spark.read.options(header=True, inferSchema=True)\
               .parquet('s3://folder/subfolder/arms_traders_clean.parquet')

# get list of all consignee countries from trade data 
temp_df = df_trade.select('CONSIGNEE_COUNTRY').distinct().collect()
trade_CONSIGNEE_COUNTRY = [temp_df[i]['CONSIGNEE_COUNTRY'] for i in range(len(temp_df))]
del(temp_df)

# get list of all consignee countries of known arms dealers
temp_df = df_arms.select('CONSIGNEE_COUNTRY').distinct().collect()
arms_CONSIGNEE_COUNTRY = [temp_df[i]['CONSIGNEE_COUNTRY'] for i in range(len(temp_df))]
del(temp_df)

#-----------------------------------------------------------------------------------------------------------#

# convert both lists from ISO3166 codes to country names 
country_list = []
target_list = []

# cycle through list
for country in trade_CONSIGNEE_COUNTRY :
  if country not in ['None', 'EU']:
    country_list.append(countries.get(country).name)

# cycle through list
for country in arms_CONSIGNEE_COUNTRY:
  if country != 'None':
    target_list.append(countries.get(country).name)

#-----------------------------------------------------------------------------------------------------------#

# remove overlaps between country_list and target_list
# the remaining countries are what we need to scrutinize individually
scrutinize = [item for item in country_list if item not in target_list]

# after researching each country in the `scrutinize` list for whether they
# have active or recent historical russian military links
# they are added to the `scrutinized_positive` list
# these country names need to be converted back to ISO3166 codes
additional_list = []

# cycle through list
for country in scrutinized_positive:
  additional_list.append(countries.get(country).alpha2)
  
# add these to the original target list for the final list of 
# all countries with active or historical Russian military ties
final_list = additional_list + target

#-----------------------------------------------------------------------------------------------------------#
# the final_list is included in the feature encoding notebook

# create a function to create a new column for known russian military links
func_is_known_military_links = F.udf(lambda x: 1 if (x in final_list) else 0)
df = df.withColumn('KNOWN_MILITARY_LINKS', func_is_known_military_links('CONSIGNEE_COUNTRY'))
