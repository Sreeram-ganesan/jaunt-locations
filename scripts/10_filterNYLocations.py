import h3
import folium
import duckdb
import os
from folium.plugins import MarkerCluster
from transformers import AutoModelForCausalLM, AutoTokenizer
import torch

# Load the tokenizer and model
# model_name = "Qwen/Qwen-32B"  # Replace with the correct model name if different
# tokenizer = AutoTokenizer.from_pretrained(model_name)
# model = AutoModelForCausalLM.from_pretrained(model_name)
# tokenizer = AutoTokenizer.from_pretrained("Qwen/Qwen-7B", trust_remote_code=True)
# model = AutoModelForCausalLM.from_pretrained("Qwen/Qwen-7B", device_map="cpu", trust_remote_code=True).eval()

# Ensure the model is on the GPU if available
# device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
# model.to(device)
def generate_text(prompt, input_text, max_length=100):
    # Combine the prompt and input text
    full_prompt = f"{prompt}\n\n{input_text}"
    
    # Tokenize the input prompt
    inputs = tokenizer(full_prompt, return_tensors="pt").to(device)
    
    # Generate text
    outputs = model.generate(
        **inputs,
        max_length=max_length,
        num_return_sequences=1,
        no_repeat_ngram_size=2,
        early_stopping=True
    )
    
    # Decode the generated text
    generated_text = tokenizer.decode(outputs[0], skip_special_tokens=True)
    return generated_text

# Radius to cover NYC
RADIUS = 20  # Adjust radius if needed

# Function to get all H3 indexes within the radius for a given resolution
def get_h3_indexes(lat, lon, resolution, radius):
    central_h3 = h3.latlng_to_cell(lat, lon, resolution)
    return h3.grid_disk(central_h3, radius)

# Function to filter locations by latitude and longitude
def filter_locations_by_lat_lon(locations_df, lat_column, lon_column, center_lat, center_lon, resolution, radius):
    # Compute the grid disk of H3 indexes
    central_h3 = h3.latlng_to_cell(center_lat, center_lon, resolution)
    grid_disk = h3.grid_disk(central_h3, radius)

    # Filter locations based on whether their lat/lon falls into the grid disk
    def is_within_grid_disk(row):
        location_h3 = h3.latlng_to_cell(row[lat_column], row[lon_column], resolution)
        return location_h3 in grid_disk

    # Apply the filtering
    return locations_df[locations_df.apply(is_within_grid_disk, axis=1)]

# Function to create a map for filtered locations
def create_h3_map(dataframe, lat_column, lon_column, output_file):
    # Create a Folium map centered at NYC
    nyc_map = folium.Map(location=[40.7826, -73.9656], tiles = 'cartodbpositron', zoom_start=11, control_scale=True)

    # Add markers for each location
    locations = []
    for _, row in dataframe.iterrows():
        title = row["title"]
        city = row["city"]
        is_duplicate = row["is_title_duplicate"]
        colour = row["tag"]
        location = [row[lat_column], row[lon_column]]
        locations.append(location)
        popup_text = f"Title: {title}<br>Is Duplicate: {is_duplicate}"
        folium.Marker(
            location=location,
            icon=folium.Icon(color=colour),
            popup=popup_text
        ).add_to(nyc_map)
    MarkerCluster(locations).add_to(nyc_map)

    # Save the map to an HTML file
    nyc_map.save(output_file)
    print(f"Map saved to {output_file}.")
# use the filtered_locations and store the description in a csv file
def store_filtered_locations(filtered_locations, output_file, duplicate=False):
    #for each description in filtered_locations, call the generate_text method and store the description in a csv file
    # do it only for top 10 rows
    # filtered_locations = filtered_locations.head(10)
    # new_filtered_locations = filtered_locations.apply(lambda row: generate_text("Improve the following text:", row["description"]), axis=1)
    # # print top 10 rows
    # print(new_filtered_locations.head(10))
    # append to the csv file
    if duplicate:
        filtered_locations[["title","description", "latitude", "longitude", "is_title_duplicate"]].to_json(output_file, orient='records', lines=True, mode='a' if os.path.exists(output_file) else 'w')
    else:
        filtered_locations[["title","description", "latitude", "longitude", "is_title_duplicate"]].to_json(output_file, orient='records', lines=True, mode='a' if os.path.exists(output_file) else 'w')
    print(f"Filtered locations stored in {output_file}.")
# Main function
def main():
    # Connect to DuckDB
    db_path = "data/locations.db"  # Update with your database path
    conn = duckdb.connect(db_path)

    # Query the locations table
    query = """
    SELECT title, city, latitude, longitude, is_title_duplicate, 'blue' as tag
    FROM locations 
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
    """
    locations_df = conn.execute(query).fetchdf()
    # Close the connection
    conn.close()
    conn = duckdb.connect(db_path)

    # Query the locations table
    query1 = """
    SELECT title, city, latitude, longitude, TRUE AS is_title_duplicate, tag
    FROM duplicate_locations 
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND tag='red';
    """
    duplicates_df1 = conn.execute(query1).fetchdf()
    # Close the connection
    conn.close()
    conn = duckdb.connect(db_path)

    # Query the locations table
    query2 = """
    SELECT title, city, latitude, longitude, TRUE AS is_title_duplicate, tag
    FROM duplicate_locations 
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND tag='lightred';
    """
    duplicates_df2 = conn.execute(query2).fetchdf()
    conn.close()
    conn = duckdb.connect(db_path)

    # Query the locations table
    query3 = """
    SELECT title, city, latitude, longitude, TRUE AS is_title_duplicate, tag
    FROM duplicate_locations 
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND tag='darkred';
    """
    duplicates_df3 = conn.execute(query3).fetchdf()
    conn.close()

    conn = duckdb.connect(db_path)

    # Query the locations table
    query4 = """
    SELECT title, city, latitude, longitude, TRUE AS is_title_duplicate, tag
    FROM duplicate_locations 
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND tag='pink';
    """
    duplicates_df4 = conn.execute(query4).fetchdf()
    conn.close()

    conn = duckdb.connect(db_path)

    # Query the locations table
    query5 = """
    SELECT title, city, latitude, longitude, TRUE AS is_title_duplicate, tag
    FROM duplicate_locations 
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND tag='purple';
    """
    duplicates_df5 = conn.execute(query5).fetchdf()
    conn.close()


    # Filter locations for resolutions 8 and 9
    for resolution in [8]:
        filtered_duplicates1 = filter_locations_by_lat_lon(
            duplicates_df1, "latitude", "longitude", 40.7128, -74.0060, resolution, RADIUS
        )
        filtered_duplicates2 = filter_locations_by_lat_lon(
            duplicates_df2, "latitude", "longitude", 40.7128, -74.0060, resolution, RADIUS
        )
        filtered_duplicates3 = filter_locations_by_lat_lon(
            duplicates_df3, "latitude", "longitude", 40.7128, -74.0060, resolution, RADIUS
        )
        filtered_duplicates4 = filter_locations_by_lat_lon(
            duplicates_df4, "latitude", "longitude", 40.7128, -74.0060, resolution, RADIUS
        )
        filtered_duplicates5 = filter_locations_by_lat_lon(
            duplicates_df5, "latitude", "longitude", 40.7128, -74.0060, resolution, RADIUS
        )
        filtered_locations = filter_locations_by_lat_lon(
            locations_df, "latitude", "longitude", 40.7128, -74.0060, resolution, RADIUS
        )
        # store_filtered_locations(filtered_locations, f"ny-data/nyc_filtered_locations_resolution_{resolution}.csv")
        # Print the number of filtered locations
        print(f"Total filtered locations for resolution {resolution}: {len(filtered_locations)}")
        print(f"Total filtered duplicates scenario1 for resolution {resolution}: {len(filtered_duplicates1)}")
        print(f"Total filtered duplicates scenario2 for resolution {resolution}: {len(filtered_duplicates2)}")
        print(f"Total filtered duplicates scenario3 for resolution {resolution}: {len(filtered_duplicates3)}")
        print(f"Total filtered duplicates scenario4 for resolution {resolution}: {len(filtered_duplicates4)}")
        print(f"Total filtered duplicates scenario5 for resolution {resolution}: {len(filtered_duplicates5)}")

        # Create the map
        output_file = f"ny-data/nyc_filtered_locations_resolution_{resolution}.html"
        output_file_duplicates1 = f"ny-data/nyc_filtered_duplicates_scenario1_resolution_{resolution}.html"
        output_file_duplicates2 = f"ny-data/nyc_filtered_duplicates_scenario2_resolution_{resolution}.html"
        output_file_duplicates3 = f"ny-data/nyc_filtered_duplicates_scenario3_resolution_{resolution}.html"
        output_file_duplicates4 = f"ny-data/nyc_filtered_duplicates_scenario4_resolution_{resolution}.html"
        output_file_duplicates5 = f"ny-data/nyc_filtered_duplicates_scenario5_resolution_{resolution}.html"
        # output_file_including_duplicates = f"ny-data/nyc_filtered_including_duplicates_resolution_{resolution}.html"
        # Create a csv of filtered_locations
        # store_filtered_locations(filtered_locations, f"ny-data/nyc_filtered_locations_resolution_{resolution}.json")
        # store_filtered_locations(filtered_duplicates, f"ny-data/nyc_filtered_duplicates_resolution_{resolution}.json", duplicate=True)
        create_h3_map(filtered_locations, "latitude", "longitude", output_file)
        create_h3_map(filtered_duplicates1, "latitude", "longitude", output_file_duplicates1)
        create_h3_map(filtered_duplicates2, "latitude", "longitude", output_file_duplicates2)
        create_h3_map(filtered_duplicates3, "latitude", "longitude", output_file_duplicates3)
        create_h3_map(filtered_duplicates4, "latitude", "longitude", output_file_duplicates4)
        create_h3_map(filtered_duplicates5, "latitude", "longitude", output_file_duplicates5)
        # create_h3_map(filtered_locations+filtered_duplicates, "latitude", "longitude", output_file_including_duplicates)

if __name__ == "__main__":
    main()
