import numpy as np
import pandas as pd
import dask.dataframe as dd
from sklearn.ensemble import GradientBoostingClassifier as GBC
from sklearn.model_selection import train_test_split as tts

# read in encoded dataframe from s3
df = dd.read_csv('s3://folder/subfolder/trade_encoded.csv/*.csv')

# reads df to memory
df = df.compute()

# creates a column for known arms exporters
df['KNOWN_EXPORTER'] = df['CONSIGNOR_INN'].copy().apply(lambda x: 1 if x in arms_inn_str else 0)

# creates a subset of the data to train the model with
# taking the same number of 1s as 0s to avoid overfitting to 0
df_1s = df[df['KNOWN_EXPORTER'] == 1].sample(25000)
df_0s = df[df['KNOWN_EXPORTER'] == 0].sample(250000, random_state=8)

df_train = pd.concat([df_1s, df_0s])

# sets X and y variables for a model
X = df_train.drop(columns=['CONSIGNOR_INN', 'KNOWN_EXPORTER'])
y = df_train['KNOWN_EXPORTER']

# performs a train test split
X_train, X_test, y_train, y_test = tts(X, y, test_size=0.2, train_size=0.8)

# creates a gradient boosting classifer model
mod = GBC()

# fits the model
mod.fit(X_train, y_train)

# generate some baseline predictions and appends them to the dataframe
predictions = mod.predict(df.drop(columns=['CONSIGNOR_INN', 'KNOWN_EXPORTER']))
df['PREDICTION'] = predictions

# looks at INNs which were predicted
predicted_INNs = list(df[df['PREDICTION'] == 1]['CONSIGNOR_INN'])

# creates a list of INN's not in the known 59
new_INNs = []
for inn in predicted_INNs:
    if inn not in inn_arms_exp:
        new_INNs.append(inn)
        
# converts to set, then list to avoid duplicates   
new_INNs = list(set(new_INNs))
