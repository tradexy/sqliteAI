# open the wiki.db file

import sqlite3
import pandas as pd

conn = sqlite3.connect('wiki.db')

# lets loop over each row and check if makdown colum hsa more than 5000 words, if so lets grab the first 5000 words for that row and overwrite the markdown column with the first 5000 words

# load the data from sqlite to pandas dataframe
df = pd.read_sql('select * from wiki', conn)
# print the columns
print(df.columns)

# close the connection
conn.close()

# lets loop over each row and check if makdown colum hsa more than 5000 words, if so lets grab the first 5000 words for that row and overwrite the markdown column with the first 5000 words

for i, row in df.iterrows():
    if len(row['markdown'].split()) > 5000:
        print("row has more than 5000 words")
        df.loc[i, 'markdown'] = ' '.join(row['markdown'].split()[:5000])

# now lets save the dataframe back to sqlite
conn = sqlite3.connect('wiki_reduced.db')
df.to_sql('wiki', conn, if_exists='replace')
conn.close()

