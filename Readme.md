### Developer Wiki

#### Overview - Script 1
1. File 1_seedLocationsFromJson contains functions to interact with a DuckDB database. It includes creating a table, inserting data from a JSON file, and describing the table schema.

#### Functions

1. **CreateLocationsTableInDuckDB**
   - **Description**: Creates a `locations` table in the DuckDB database.
   - **Parameters**:
     - `dbFilePath` (string): Path to the DuckDB database file.
   - **Returns**: `error` if any issue occurs during the table creation.
   - **Usage**:
     ```go
     err := CreateLocationsTableInDuckDB("path/to/duckdb.db")
     if err != nil {
         log.Fatal(err)
     }
     ```

2. **InsertJSONIntoLocationsTableInDuckDB**
   - **Description**: Inserts data into the `locations` table from a JSON file.
   - **Parameters**:
     - `dbFilePath` (string): Path to the DuckDB database file.
     - `jsonFilePath` (string): Path to the JSON file containing the data.
   - **Returns**: `error` if any issue occurs during the data insertion.
   - **Usage**:
     ```go
     err := InsertJSONIntoLocationsTableInDuckDB("path/to/duckdb.db", "path/to/data.json")
     if err != nil {
         log.Fatal(err)
     }
     ```

3. **DescribeLocationsTableInDuckDB**
   - **Description**: Describes the schema of the `locations` table.
   - **Parameters**:
     - `dbFilePath` (string): Path to the DuckDB database file.
   - **Returns**: `error` if any issue occurs during the schema description.
   - **Usage**:
     ```go
     err := DescribeLocationsTableInDuckDB("path/to/duckdb.db")
     if err != nil {
         log.Fatal(err)
     }
     ```

#### Example Usage
```go
package main

import (
    "log"
)

func main() {
    dbFilePath := "path/to/duckdb.db"
    jsonFilePath := "path/to/data.json"

    // Create the locations table
    if err := CreateLocationsTableInDuckDB(dbFilePath); err != nil {
        log.Fatalf("Error creating locations table: %v", err)
    }

    // Insert data from JSON file
    if err := InsertJSONIntoLocationsTableInDuckDB(dbFilePath, jsonFilePath); err != nil {
        log.Fatalf("Error inserting data: %v", err)
    }

    // Describe the locations table
    if err := DescribeLocationsTableInDuckDB(dbFilePath); err != nil {
        log.Fatalf("Error describing table: %v", err)
    }
}
```

#### Notes
- Ensure DuckDB is properly installed and the database file path is correct.
- The JSON file should be formatted correctly to match the table schema.
- Handle errors appropriately in production code.

#### Overview - Script 2
This file contains functions and code to preprocess text, generate embeddings using a pre-trained SentenceTransformer model, and update a DuckDB database with these embeddings.

#### Functions

1. **preprocess_text**
   - **Description**: Preprocesses text by removing HTML tags, non-alphanumeric characters, converting to lowercase, and optionally removing stop words.
   - **Parameters**:
     - `text` (string): The text to preprocess.
   - **Returns**: The preprocessed text.
   - **Usage**:
     ```python
     clean_text = preprocess_text("<p>Example text!</p>")
     ```

#### Main Code

1. **Load Model**
   - **Description**: Loads the SentenceTransformer model `all-MiniLM-L6-v2`.
   - **Usage**:
     ```python
     model = SentenceTransformer('all-MiniLM-L6-v2')
     ```

2. **Connect to DuckDB**
   - **Description**: Connects to the DuckDB database located at `data/locations.db`.
   - **Usage**:
     ```python
     conn = duckdb.connect(database='data/locations.db')
     ```

3. **Add Embedding Column**
   - **Description**: Adds a new column `embedding` to the `locations` table if it doesn't already exist.
   - **Usage**:
     ```python
     conn.execute('ALTER TABLE locations ADD COLUMN IF NOT EXISTS embedding JSON')
     ```

4. **Query Descriptions**
   - **Description**: Queries all descriptions from the `locations` table where the `embedding` column is null.
   - **Usage**:
     ```python
     descriptions = conn.execute('SELECT contentid, description FROM locations where embedding is null').fetchall()
     ```

5. **Generate and Update Embeddings**
   - **Description**: Iterates through the descriptions, preprocesses them, generates embeddings, and updates the `locations` table with these embeddings.
   - **Usage**:
     ```python
     for row in descriptions:
         location_id = row[0]
         description = row[1]
         preprocessed_description = preprocess_text(description)
         embedding = model.encode(preprocessed_description).tolist()
         embedding_json = json.dumps(embedding)
         conn.execute('''
             UPDATE locations
             SET embedding = ?
             WHERE contentid = ?
         ''', (embedding_json, location_id))
     ```

6. **Close Connection**
   - **Description**: Closes the connection to the DuckDB database.
   - **Usage**:
     ```python
     conn.close()
     ```

#### Notes
- Ensure all required libraries (`sentence_transformers`, `duckdb`, `beautifulsoup4`, `nltk`, `re`) are installed.
- The `nltk` stopwords functionality is commented out but can be enabled if needed.
- Handle exceptions and errors appropriately in production code.

### Developer Wiki

#### Overview - Script 3
This file contains a function to compare embeddings of locations from a DuckDB database and update the similarity scores in a `location_comparison` table.

#### Functions

1. **compare_embeddings**
   - **Description**: Compares embeddings of two content IDs from the `locations` table and updates the `location_comparison` table with the similarity score.
   - **Parameters**:
     - `location_db_file` (string): Path to the DuckDB database file.
   - **Usage**:
     ```python
     compare_embeddings('data/locations.db')
     ```

#### Main Code

1. **Load Model**
   - **Description**: Loads the SentenceTransformer model `all-MiniLM-L6-v2`.
   - **Usage**:
     ```python
     model = SentenceTransformer('all-MiniLM-L6-v2')
     ```

2. **Connect to DuckDB**
   - **Description**: Connects to the DuckDB database located at `location_db_file`.
   - **Usage**:
     ```python
     conn = duckdb.connect(database=location_db_file)
     ```

3. **Add Similarity Score Column**
   - **Description**: Adds a new column `sentence_similarity_score` to the `location_comparison` table if it doesn't already exist.
   - **Usage**:
     ```python
     conn.execute('ALTER TABLE location_comparison ADD COLUMN IF NOT EXISTS sentence_similarity_score FLOAT')
     ```

4. **Query Content IDs**
   - **Description**: Queries all `contentid1` and `contentid2` from the `location_comparison` table.
   - **Usage**:
     ```python
     content_ids = conn.execute('SELECT contentid1, contentid2 FROM location_comparison').fetchall()
     ```

5. **Compare Embeddings and Update Similarity Score**
   - **Description**: Iterates through the content IDs, retrieves embeddings, calculates similarity scores, and updates the `location_comparison` table.
   - **Usage**:
     ```python
     for row in content_ids:
         content_id1 = row[0]
         content_id2 = row[1]

         tuple1 = conn.execute('SELECT embedding FROM locations WHERE contentid = ?', (content_id1,)).fetchone()
         tuple2 = conn.execute('SELECT embedding FROM locations WHERE contentid = ?', (content_id2,)).fetchone()

         if tuple1 is not None and tuple2 is not None:
             embedding1 = json.loads(tuple1[0])
             embedding2 = json.loads(tuple2[0])

             if embedding1 and embedding2:
                 similarity_score = util.cos_sim(embedding1, embedding2).item()
             else:
                 similarity_score = 0.0

             conn.execute('''
                 UPDATE location_comparison
                 SET sentence_similarity_score = ?
                 WHERE contentid1 = ? AND contentid2 = ?
             ''', (similarity_score, content_id1, content_id2))
     ```

6. **Close Connection**
   - **Description**: Closes the connection to the DuckDB database.
   - **Usage**:
     ```python
     conn.close()
     ```

#### Notes
- Ensure all required libraries (`sentence_transformers`, `duckdb`, `json`) are installed.
- Handle exceptions and errors appropriately in production code.

#### Overview - Script 4
This Go function `ArchiveAndRemoveDuplicateLocations` is designed to manage duplicate location entries in a DuckDB database. It archives duplicates into a separate table and updates the descriptions of the remaining entries based on specific conditions.

#### Functions

1. **ArchiveAndRemoveDuplicateLocations**
   - **Description**: Archives duplicate location entries from the `locations` table into a `duplicate_locations` table and updates the descriptions of the remaining entries.
   - **Parameters**:
     - `dbFilePath` (string): Path to the DuckDB database file.
   - **Returns**: 
     - `error`: Returns an error if any operation fails, otherwise returns `nil`.
   - **Usage**:
     ```go
     err := ArchiveAndRemoveDuplicateLocations("data/locations.db")
     if err != nil {
         fmt.Println("Error:", err)
     } else {
         fmt.Println("Duplicate locations archived and removed successfully.")
     }
     ```

#### Main Code

1. **Open the DuckDB Database**
   - **Description**: Opens a connection to the DuckDB database located at `dbFilePath`.
   - **Usage**:
     ```go
     db, err := sql.Open("duckdb", dbFilePath)
     if err != nil {
         return fmt.Errorf("failed to open DuckDB: %w", err)
     }
     defer db.Close()
     ```

2. **Create `duplicate_locations` Table**
   - **Description**: Creates a new table `duplicate_locations` with the same schema as `locations` but initially empty.
   - **Usage**:
     ```go
     createTableQuery := `
         CREATE TABLE IF NOT EXISTS duplicate_locations AS
         SELECT * FROM locations WHERE 1=0;
     `
     _, err = db.Exec(createTableQuery)
     if err != nil {
         return fmt.Errorf("failed to create duplicate_locations table: %w", err)
     }
     ```

3. **Insert into `duplicate_locations` Table**
   - **Description**: Inserts duplicate entries from `locations` into `duplicate_locations` based on the conditions `similarity_score > 0.85` and `distance_in_meters <= 50`.
   - **Usage**:
     ```go
     insertQuery := `
         INSERT INTO duplicate_locations
         SELECT l.*
         FROM locations l
         JOIN location_comparison lc ON l.contentid = lc.contentid2
         WHERE lc.similarity_score > 0.85 AND lc.distance_in_meters <= 50;
     `
     _, err = db.Exec(insertQuery)
     if err != nil {
         return fmt.Errorf("failed to insert into duplicate_locations table: %w", err)
     }
     ```

4. **Delete from `locations` Table**
   - **Description**: Deletes the duplicate entries from the `locations` table.
   - **Usage**:
     ```go
     deleteQuery := `
         DELETE FROM locations
         WHERE contentid IN (
             SELECT contentid
             FROM duplicate_locations
         );
     `
     _, err = db.Exec(deleteQuery)
     if err != nil {
         return fmt.Errorf("failed to delete from locations table: %w", err)
     }
     ```

5. **Update Descriptions of Remaining Locations**
   - **Description**: Updates the description of the remaining locations by combining descriptions from the `location_comparison` table.
   - **Usage**:
     ```go
     updateQuery := `
         UPDATE locations
         SET description = (
             SELECT array_to_string(array_agg(description), '|')
             FROM location_comparison
             WHERE locations.contentid = location_comparison.contentid1
         )
         WHERE locations.contentid IN (
             SELECT contentid1
             FROM location_comparison
             WHERE similarity_score > 0.85
             AND distance_in_meters <= 50
         );
     `
     _, err = db.Exec(updateQuery)
     if err != nil {
         return fmt.Errorf("failed to update descriptions: %w", err)
     }
     ```

#### Overview - Script 5
This Go code provides functions to compare locations stored in a DuckDB database. It creates a comparison table, calculates similarity scores based on titles, and calculates distances between locations using spatial functions.

#### Functions

1. **CompareLocations**
   - **Description**: Creates a new table `location_comparison` that stores the comparison of locations with the same `h3_index_8`. It includes columns for similarity score by comparing titles and closeness score by comparing `h3_index_12`.
   - **Parameters**:
     - `dbFilePath` (string): Path to the DuckDB database file.
   - **Returns**: 
     - `error`: Returns an error if any operation fails, otherwise returns `nil`.
   - **Usage**:
     ```go
     err := CompareLocations("data/locations.db")
     if err != nil {
         fmt.Println("Error:", err)
     } else {
         fmt.Println("Location comparison table created successfully.")
     }
     ```

2. **CompareLocationsInsertSimilarityScore**
   - **Description**: Inserts similarity scores into the `location_comparison` table by comparing the titles of the locations if the similarity score is null.
   - **Parameters**:
     - `dbFilePath` (string): Path to the DuckDB database file.
   - **Returns**: 
     - `error`: Returns an error if any operation fails, otherwise returns `nil`.
   - **Usage**:
     ```go
     err := CompareLocationsInsertSimilarityScore("data/locations.db")
     if err != nil {
         fmt.Println("Error:", err)
     } else {
         fmt.Println("Similarity scores inserted successfully.")
     }
     ```

3. **CompareLocationsWithHaversineDistance**
   - **Description**: Uses the SPATIAL plugin of DuckDB to calculate the distance between two points and stores it as a new column in the `location_comparison` table.
   - **Parameters**:
     - `dbFilePath` (string): Path to the DuckDB database file.
   - **Returns**: 
     - `error`: Returns an error if any operation fails, otherwise returns `nil`.
   - **Usage**:
     ```go
     err := CompareLocationsWithHaversineDistance("data/locations.db")
     if err != nil {
         fmt.Println("Error:", err)
     } else {
         fmt.Println("Distances calculated and stored successfully.")
     }
     ```

#### Main Code

1. **Open the DuckDB Database**
   - **Description**: Opens a connection to the DuckDB database located at `dbFilePath`.
   - **Usage**:
     ```go
     db, err := sql.Open("duckdb", dbFilePath)
     if err != nil {
         return fmt.Errorf("failed to open DuckDB: %w", err)
     }
     defer db.Close()
     ```

2. **Create `location_comparison` Table**
   - **Description**: Creates a new table `location_comparison` to store the comparison results of locations with the same `h3_index_8`.
   - **Usage**:
     ```go
     createTableQuery := `
         LOAD h3;LOAD SPATIAL;
         CREATE TABLE location_comparison AS
         SELECT
             l1.contentid AS contentid1,
             ARRAY[l1.title, l2.title] AS titles,
             l1.h3_index_8 AS h3_index_8_1,
             ARRAY[l1.latitude, l1.longitude] AS point1,
             ARRAY[l2.latitude, l2.longitude] AS point2,
             l1.contentid AS contentid1,
             l2.contentid AS contentid2,
             ARRAY[l1.description, l2.description] AS descriptions,
             jaro_winkler_similarity(l1.title, l2.title) AS similarity_score,
             h3_grid_distance(l1.h3_index_12, l2.h3_index_12) AS distance_in_km
         FROM
             locations l1
         JOIN
             locations l2
         ON
             l1.h3_index_8 = l2.h3_index_8
         AND
             l1.contentid < l2.contentid
         ORDER BY
             l1.h3_index_8 desc;
     `
     _, err = db.Exec(createTableQuery)
     if err != nil {
         return fmt.Errorf("failed to create location_comparison table: %w", err)
     }
     ```

3. **Insert Similarity Scores**
   - **Description**: Inserts similarity scores into the `location_comparison` table by comparing the titles of the locations if the similarity score is null.
   - **Usage**:
     ```go
     alterTableQuery := `
         ALTER TABLE location_comparison ADD COLUMN IF NOT EXISTS similarity_score FLOAT;
     `
     _, err = db.Exec(alterTableQuery)
     if err != nil {
         return fmt.Errorf("failed to alter location_comparison table: %w", err)
     }

     updateQuery := `
         UPDATE location_comparison
         SET similarity_score = jaro_winkler_similarity(l1.title, l2.title)
         FROM locations l1, locations l2
         WHERE location_comparison.similarity_score IS NULL
         AND location_comparison.contentid1 = l1.contentid
         AND location_comparison.contentid2 = l2.contentid;
     `
     _, err = db.Exec(updateQuery)
     if err != nil {
         return fmt.Errorf("failed to update similarity scores: %w", err)
     }
     ```

4. **Calculate Distances**
   - **Description**: Uses the SPATIAL plugin to calculate the distance between two points and stores it as a new column in the `location_comparison` table.
   - **Usage**:
     ```go
     query := `
         INSTALL spatial;LOAD spatial;
         ALTER TABLE location_comparison ADD COLUMN IF NOT EXISTS distance_in_meters FLOAT;
         UPDATE location_comparison
         SET distance_in_meters = ST_DISTANCE_SPHEROID(ST_POINT(l1.latitude, l1.longitude), ST_POINT(l2.latitude, l2.longitude))
         FROM locations l1, locations l2
         WHERE location_comparison.distance_in_meters IS NULL
         AND location_comparison.contentid1 = l1.contentid
         AND location_comparison.contentid2 = l2.contentid;
     `
     _, err = db.Exec(query)
     if err != nil {
         return fmt.Errorf("failed to update distances: %w", err)
     }
     ```