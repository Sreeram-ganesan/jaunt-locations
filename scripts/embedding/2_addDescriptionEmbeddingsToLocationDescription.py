from sentence_transformers import SentenceTransformer
import duckdb
import json
from bs4 import BeautifulSoup
import nltk
# from nltk.corpus import stopwords
import re

# Download NLTK stopwords
# nltk.download('stopwords')
# stop_words = set(stopwords.words('english'))

# Function to preprocess text
def preprocess_text(text):
    # Remove HTML tags
    if text:
        # text = BeautifulSoup(text, "html.parser").get_text()
        # Remove non-alphanumeric characters
        text = re.sub(r'\W+', ' ', text)
        # Convert to lowercase
        text = text.lower()
        # Remove stop words
        # text = ' '.join(word for word in text.split() if word not in stop_words)
    return text

# Load model
model = SentenceTransformer('all-MiniLM-L6-v2')

# Connect to DuckDB (assuming you have a 'locations' table with 'id' and 'description' columns)
conn = duckdb.connect(database='data/locations.db')

# Add a new column for embeddings if it doesn't exist
conn.execute('ALTER TABLE locations ADD COLUMN IF NOT EXISTS embedding JSON')

# Query all descriptions from the locations table
descriptions = conn.execute('SELECT contentid, description FROM locations where embedding is null').fetchall()

# Iterate through the descriptions, preprocess, and generate embeddings
#print number of descriptions
print(len(descriptions))
for row in descriptions:
    location_id = row[0]
    description = row[1]
    preprocessed_description = preprocess_text(description)
    #encode the preprocessed description to get the embedding of the description and make it comparable using cosine similarity
    if preprocessed_description is None:
        continue
    embedding = model.encode(preprocessed_description).tolist()
    # embedding = model.encode(preprocessed_description, convert_to_tensor=True, show_progress_bar=True, normalize_embeddings=True).tolist()
    # print(embedding.count)
    # embedding = model.encode(preprocessed_description).tolist()
    embedding_json = json.dumps(embedding)

    # Update the table with the embedding
    conn.execute('''
        UPDATE locations
        SET embedding = ?
        WHERE contentid = ?
    ''', (embedding_json, location_id))

# Verify the update
# result = conn.execute('SELECT id, description, embedding FROM locations').fetchall()
# for row in result:
#     print(row)

# Close the connection
conn.close()