from sentence_transformers import SentenceTransformer, util
import duckdb
import json

# join location and location_comparison table and compare the embedding of the two content_id1 and content_id2 and return the similarity score of the two embeddings
# input: location db file which has locations(contentid as unique identifier) and location_comparison table(contentid1, contentid2)
# output: similarity score of the two embeddings and update location_comparison table with the similarity score

def compare_embeddings(location_db_file):

    # Load model
    model = SentenceTransformer('all-MiniLM-L6-v2')
    conn = duckdb.connect(database=location_db_file)
    conn.execute('ALTER TABLE location_comparison ADD COLUMN IF NOT EXISTS sentence_similarity_score FLOAT')

    # Query all contentid1 and contentid2 from the location_comparison table
    content_ids = conn.execute("SELECT contentid1, contentid2 FROM location_comparison lc join locations l on contentid1 = l.contentid where lc.sentence_similarity_score is null").fetchall()
    # print no of content_ids
    print(len(content_ids))

    for row in content_ids:
        content_id1 = row[0]
        content_id2 = row[1]

        # Query the embeddings of the two content ids
        tuple1 = conn.execute('SELECT embedding FROM locations WHERE contentid = ?', (content_id1,)).fetchone()
        tuple2 = conn.execute('SELECT embedding FROM locations WHERE contentid = ?', (content_id2,)).fetchone()
        if tuple1 is not None and tuple2 is not None:
            embedding1 = tuple1[0]
            embedding2 = tuple2[0]

            if isinstance(embedding1, str):
                embedding1 = json.loads(embedding1)
            if isinstance(embedding2, str):
                embedding2 = json.loads(embedding2)

            # Calculate the similarity score
            if embedding1 is not None and embedding2 is not None:
                # embedding1 = json.loads(embedding1)
                # embedding2 = json.loads(embedding2)
                
                if embedding1 and embedding2:  # Check if embeddings are not empty
                    similarity_score = util.cos_sim(embedding1, embedding2).item()
                else:
                    similarity_score = 0.0  # Set similarity score to 0 if embeddings are empty
                
                # Update the table with the similarity score
                if content_id1 is not None and content_id2 is not None:
                    conn.execute('''
                        UPDATE location_comparison
                        SET sentence_similarity_score = ?
                        WHERE contentid1 = ? AND contentid2 = ?
                    ''', (similarity_score, content_id1, content_id2))

    # Close the connection
    conn.close()
compare_embeddings('data/locations.db')