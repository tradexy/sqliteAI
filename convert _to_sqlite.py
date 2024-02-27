# open the wiki.parquet file 

import pandas as pd
import sqlite3

df = pd.read_parquet('wiki.parquet')

# create a sqlite database

conn = sqlite3.connect('wiki.db')

# convert first 10000 rows to sqlite
df.iloc[:10000].to_sql('wiki', conn, if_exists='replace')

# close the connection
conn.close()