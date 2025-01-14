from transformers import pipeline
import duckdb
import pandas as pd

# Initialize the text classification pipeline
# Replace `text-classification` with the desired model from Hugging Face
classifier = pipeline("text-classification", model="distilbert-base-uncased", return_all_scores=True)

# Function to classify if a location belongs to NYC
def is_nyc_city(description):
    result = classifier(description)
    # Assuming binary classification with labels ["New York City", "Other"]
    for label_score in result[0]:
        if label_score["label"] == "New York City" and label_score["score"] > 0.8:
            return True
    return False

# Function to filter locations by textual description
def filter_locations_by_city(locations_df, city_column):
    return locations_df[locations_df[city_column].apply(is_nyc_city)]

# Main function
def main():
    # Connect to DuckDB
    db_path = "data/locations.db"  # Update with your database path
    conn = duckdb.connect(db_path)

    # Query the locations table
    query = """
    SELECT description, latitude, longitude
    FROM locations 
    WHERE latitude IS NULL OR longitude IS NULL;
    """
    locations_df = conn.execute(query).fetchdf()
    # Print count of locations
    print(f"Total locations: {len(locations_df)}")

    # Close the connection
    conn.close()

    # Filter locations belonging to NYC based on city description
    filtered_locations = filter_locations_by_city(locations_df, "description")
    # Print count of filtered locations
    print(f"Total filtered locations: {len(filtered_locations)}")
    # Save or display the filtered locations
    output_file = "nyc_filtered_locations.csv"
    filtered_locations.to_csv(output_file, index=False)
    print(f"Filtered locations saved to {output_file}.")

if __name__ == "__main__":
    main()
