import asyncio
import sqlite3
import pandas as pd
from openai import AsyncOpenAI
import os
import json
import time


start_connection = time.time()
# Connect to the SQLite database
conn = sqlite3.connect('wiki_reduced.db')

# Load the data into a DataFrame. 
df = pd.read_sql_query("SELECT * FROM wiki", conn)
end_connection = time.time()
print(f"Connection time: {end_connection - start_connection}")

# Initialize the OpenAI client
api_key = os.getenv("OPENAI_API_KEY") or "YOUR_API_KEY"
client = AsyncOpenAI(api_key=api_key)

async def get_embedding(row):
    while True:
        try:
            # Get the markdown column value
            markdown_text = row['markdown']

            # Get the embeddings
            response = await client.embeddings.create(
                input=markdown_text,
                model="text-embedding-ada-002"
            )

            # Return the embedding
            return response.data[0].embedding
        except Exception as e:
            if "token" in str(e):
                print("Token limit exceeded, skipping this row")
                return None  # Return None if token limit is exceeded

            print(f"Error encountered: {e}. Retrying in 60 seconds...")
            await asyncio.sleep(60)


async def main():
    # Create a list of tasks
    tasks = [get_embedding(row) for _, row in df.iterrows()]

    # Run the tasks concurrently
    embeddings = await asyncio.gather(*tasks)

    # Add the embeddings as a new column in the DataFrame
    df['embeddings'] = embeddings

    # Convert the embeddings to a JSON string
    df['embeddings'] = df['embeddings'].apply(json.dumps)

    # Create a new SQLite database
    new_conn = sqlite3.connect('wiki_embeddings.db')

    # Save the DataFrame to the new SQLite database
    df.to_sql('wiki_embeddings', new_conn, if_exists='replace', index=False)

    # Close the connections
    conn.close()

start_embedding = time.time()
asyncio.run(main())
end_embedding = time.time()
print(f"Embedding time: {end_embedding - start_embedding}")