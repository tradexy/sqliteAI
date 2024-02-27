
import sqlite3
import pandas as pd

conn = sqlite3.connect('wiki_embeddings.db')


df = pd.read_sql_query("SELECT * FROM wiki_embeddings", conn)

# average number of words in markdown column
print(df['markdown'].str.split().str.len().mean())