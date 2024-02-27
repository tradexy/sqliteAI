# open the wiki.parquet file 

import pandas as pd

df = pd.read_parquet('wiki.parquet')
# print the column names
print(df.columns)

# convert datset to csv and save it
df.to_csv('wiki.csv', index=False)