import h3
import folium
import duckdb
import os
import sys
from folium.plugins import MarkerCluster
import pandas as pd
from transformers import AutoModelForCausalLM, AutoTokenizer
import torch
import json
import requests
from shapely.geometry import shape, Point

# Connect to DuckDB, query the locations table, and load the data into a Pandas DataFrame, for each location latitude and longitude identify if its within the ny city boundary
# Obtain the ny city boundary in json from  - https://github.com/generalpiston/geojson-us-city-boundaries/blob/master/cities/ny/new-york.json and write a python method to identify if it falls within this boundart
# Load NYC boundary from GeoJSON
def load_nyc_boundary():
    url = "https://raw.githubusercontent.com/generalpiston/geojson-us-city-boundaries/master/cities/ny/new-york.json"
    response = requests.get(url)
    nyc_boundary_geojson = response.json()
    nyc_boundary = shape(nyc_boundary_geojson['features'][0]['geometry'])
    return nyc_boundary

def load_nyc_neighbourhood_boundaries():
    url = "https://gist.githubusercontent.com/ix4/ff7603f48283cf06fc4fb3dfb6a0635c/raw/3eae4056c9d4de99f0040b6bedbd9ba547e8d215/nyc.geojson"
    response = requests.get(url)
    try:
        load_nyc_neighbourhood_boundaries_geojsons = response.json()
        print(f"Loaded {len(load_nyc_neighbourhood_boundaries_geojsons['features'])} neighbourhood boundaries.")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return []
    return load_nyc_neighbourhood_boundaries_geojsons["features"]


# Check if a point is within the NYC boundary
def is_within_nyc_boundary(lat, lon, nyc_boundary):
    point = Point(lon, lat)
    return nyc_boundary.contains(point)

# Connect to DuckDB and load data into a DataFrame
def load_locations_from_db(db_path):
    conn = duckdb.connect(db_path)
    query = """
    SELECT DISTINCT ON (contentid) l.latitude, l.longitude
    FROM locations l
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
    """
    locations_df = conn.execute(query).fetchdf()
    conn.close()
    return locations_df

# Filter locations within NYC boundary
def filter_locations_within_nyc(locations_df, nyc_boundary):
    def check_boundary(row):
        return is_within_nyc_boundary(row['latitude'], row['longitude'], nyc_boundary)
    
    return locations_df[locations_df.apply(check_boundary, axis=1)]
def update_city_column_in_db(db_path, nyc_locations_df):
    conn = duckdb.connect(db_path)
    # content_ids = tuple(nyc_locations_df['contentid'].tolist())
    filtered_locations_content_ids = nyc_locations_df['contentid'].tolist()
    content_ids = ','.join([f"'{id}'" for id in filtered_locations_content_ids])
    query = f"UPDATE locations SET city = 'NY' WHERE contentid IN {content_ids};"
    conn.execute(query)
    conn.close()
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
RADIUS = 23  # Adjust radius if needed
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
def create_h3_map(dataframe, lat_column, lon_column, output_file, add_neighbourhood=False,neighbourhood_boundary=None):
    # Create a Folium map centered at NYC
    nyc_map = folium.Map(location=[40.7826, -73.9656], tiles='cartodbpositron', zoom_start=11, control_scale=True)

    # Add NYC boundary to the map
    if add_neighbourhood:
        folium.GeoJson(
            neighbourhood_boundary,
            name='NYC Boundary',
            style_function=lambda x: {'color': 'green', 'weight': 2, 'fillOpacity': 0.1}
        ).add_to(nyc_map)
    else:
        folium.GeoJson(
            load_nyc_boundary(),
            name='NYC Boundary',
            style_function=lambda x: {'color': 'blue', 'weight': 2, 'fillOpacity': 0.1}
        ).add_to(nyc_map)
        ny_boundaries = load_nyc_neighbourhood_boundaries()
        for index, boundary in enumerate(ny_boundaries):
            if boundary["geometry"] is not None:
                print(f"Adding boundary {index}")
                folium.GeoJson(
                    shape(boundary["geometry"]),
                    name=f"NYC Boundary - {index}",
                    style_function=lambda _: {'color': 'green', 'weight': 2, 'fillOpacity': 0.4 * (index % len(ny_boundaries)) / len(ny_boundaries)}
                ).add_to(nyc_map)

    # Add markers for each location
    locations = []
    tag_counts = {"red": 0, "lightred": 0, "darkred": 0, "pink": 0, "purple": 0, "blue": 0}
    for _, row in dataframe.iterrows():
        title = row["title"]
        contentid = row["contentid"]
        description = (row["description"] or "")[:20] + "..." if row["description"] and len(row["description"]) > 20 else (row["description"] or "")
        source_url = row["source_url"] or ""
        is_duplicate = row["is_title_duplicate"]
        colour = row["tag"]
        # Count the occurrences of each tag color
        if colour in tag_counts:
            tag_counts[colour] += 1
        location = [row[lat_column], row[lon_column]]
        locations.append(location)
        popup_text = f"Title: {title}<br>Is Duplicate: {is_duplicate}<br>Description: {description}<br>Source_URL: {source_url}<br>UniqueID: {contentid}<br>Point: {str(row[lat_column])+ ',' + str(row[lon_column])}"
        folium.Marker(
            location=location,
            icon=folium.Icon(color=colour),
            popup=popup_text
        ).add_to(nyc_map)
    
    # Print the counts
    for tag, count in tag_counts.items():
        print(f"{tag}: {count}")
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
def store_filtered_locations_to_csv(filtered_locations, output_file):
    filtered_locations.to_csv(output_file, mode='a' if os.path.exists(output_file) else 'w')
    print(f"Filtered locations stored in {output_file}.")
def store_filtered_locations_to_jsonl(filtered_locations, output_file):
    with open(output_file, 'w', encoding='ISO-8859-1') as file:
        for index, row in filtered_locations.iterrows():
            # print(row.to_json(force_ascii=False))
            file.write(row.to_json(force_ascii=False).encode('utf-8').decode('ISO-8859-1') + '\n')
    # filtered_locations.to_json(output_file, orient='records', lines=True, force_ascii=False, encoding = encoding)
    print(f"Filtered locations stored in {output_file}.")
def delete_all_location_comparisons_except_filtered_locations(filtered_locations_content_ids):
    db_path = "data/locations.db"
    conn = duckdb.connect(db_path)
    contentids = ','.join([f"'{id}'" for id in filtered_locations_content_ids])
    query2 = f"""
    DELETE FROM location_comparison
    WHERE contentid1 NOT IN ({contentids}) OR contentid2 NOT in ({contentids});
    """
    conn.execute(query2)
    conn.close()
    print(f"Deleted all locations except filtered locations.")
def delete_all_locations_except_filtered_locations(filtered_locations_content_ids):
    # delete all locations except filtered_locations in locations table
    # Connect to DuckDB
    db_path = "data/locations.db"  # Update with your database path
    conn = duckdb.connect(db_path)
    # Query the locations table to delete all locations except filtered locations ids
    # contentids = ','.join(filtered_locations_content_ids)
    contentids = ','.join([f"'{id}'" for id in filtered_locations_content_ids])
    print(contentids)
    query = f"""
    DELETE FROM locations
    WHERE contentid NOT IN ({contentids});
    """
    conn.execute(query)
    # Close the connection
def fetch_all_location_comparison_rows(filtered_locations_content_ids):
    db_path = "data/locations.db"
    conn = duckdb.connect(db_path)
    contentids = ','.join([f"'{id}'" for id in filtered_locations_content_ids])
    query = f"""
    SELECT DISTINCT ON (lc.contentid1, lc.contentid2) lc.*, s.score
    FROM location_comparison lc
    LEFT JOIN similarity s ON lc.contentid1 = s.contentid1 AND lc.contentid2 = s.contentid2
    WHERE lc.contentid2 IN ({contentids}) AND lc.distance_in_meters <= 100;
    """
    location_comparison_df = conn.execute(query).fetchdf()
    conn.close()
    return location_comparison_df
def csv_to_jsonl(input_file, output_file):
    df = pd.read_csv(input_file)
    with open(output_file, 'w', encoding='ISO-8859-1') as file:
        for index, row in df.iterrows():
            # print(row.to_json(force_ascii=False))
            file.write(row.to_json(force_ascii=False).encode('utf-8').decode('ISO-8859-1') + '\n')
    # filtered_locations.to_json(output_file, orient='records', lines=True, force_ascii=False, encoding = encoding)
    print(f"Filtered locations stored in {output_file}.")
# query from boundary_locations which has boundary_id which can be used to filter boundaries table to get the geometry of the boundary and contentids to filter locations table to get the locations within the boundary, then create a map with the locations within the boundary
def create_h3_map_for_boundary_locations():
    queryToGetBoundaryLocations = """
    select * from locations where contentid in (SELECT UNNEST(contentids)
    FROM boundary_locations where boundary_id in (select id from boundaries where names.primary = 'City of New York'));
    """
    queryToGetBoundaryLocations = """
    select * from locations where overtures_region = 'New York';
    """
    conn = duckdb.connect("data/locations.db")
    boundary_locations_df = conn.execute(queryToGetBoundaryLocations).fetchdf()
    conn.close()
    queryToGetBoundaryGeoJSON = """
    LOAD spatial;
    SELECT ST_AsGeoJSON(geometry) FROM boundaries where id in (select id from boundaries where names.primary = 'City of New York');
    """
    conn = duckdb.connect("data/locations.db")
    boundary_geojson = conn.execute(queryToGetBoundaryGeoJSON).fetchdf()
    conn.close()
    boundary_geojson = boundary_geojson.to_dict()
    # print(boundary_geojson.get('st_asgeojson(geometry)').get(0))
    # convert boundary_geojson to dictionary
    geo = json.loads(boundary_geojson.get('st_asgeojson(geometry)').get(0))
    # print(geo)
    # print(geo.get("type"))
    boundary = shape(geo)
    # print(boundary)
    create_h3_map(boundary_locations_df, "latitude", "longitude", "ny-data/nyc_filtered_including_duplicates_with_overtures_boundary_region_new.html", add_neighbourhood=True, neighbourhood_boundary=boundary)
# Main function
def main():
    # Connect to DuckDB
    # db_path = "data/locations.db"  # Update with your database path
    # conn = duckdb.connect(db_path)
    # query0 = """
    # SELECT DISTINCT ON (contentid) *
    # FROM locations
    # WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
    # """
    # all_locations_df = conn.execute(query0).fetchdf()
    # # Close the connection
    # conn.close()
    # conn = duckdb.connect(db_path)
    # query_union = """
    # SELECT DISTINCT ON (l.contentid) l.*, COALESCE(d.tag, 'blue') AS tag
    # FROM locations l
    # LEFT JOIN duplicate_locations d ON l.contentid = d.contentid
    # WHERE l.contentid NOT IN (SELECT contentid FROM duplicate_locations)
    # UNION
    # SELECT DISTINCT ON (d.contentid) d.*, d.tag
    # FROM duplicate_locations d;
    # """
    # all_locations_and_duplicates_df = conn.execute(query_union).fetchdf()
    # conn.close()
    # conn = duckdb.connect(db_path)

    # # Query the duplicate locations table
    # query = """
    # SELECT DISTINCT ON (contentid) *
    # FROM duplicate_locations 
    # WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
    # """
    # locations_df = conn.execute(query).fetchdf()
    # # Close the connection
    # conn.close()
    # conn = duckdb.connect(db_path)

    # # Query the locations table
    # # query1 = """
    # # SELECT title, city, latitude, longitude, TRUE AS is_title_duplicate, tag
    # # FROM duplicate_locations
    # # WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND tag='red';
    # # """
    # # Query to get all locations + duplicates in the same dataframe with default tag as blue for locations and tag for duplicates and duplicates should not be repeated in the final dataframe
    # query1 = """
    # SELECT DISTINCT ON (contentid) *
    # FROM duplicate_locations
    # WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND tag='red';
    # """
    # duplicates_df1 = conn.execute(query1).fetchdf()
    # # Close the connection
    # conn.close()
    # conn = duckdb.connect(db_path)

    # Query the locations table
    # query2 = """
    # SELECT title, city, latitude, longitude, TRUE AS is_title_duplicate, tag
    # FROM duplicate_locations
    # WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND tag='lightred';
    # """
    # query2 = """
    # SELECT DISTINCT ON (contentid) *
    # FROM duplicate_locations
    # WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND tag='lightred';
    # """
    # duplicates_df2 = conn.execute(query2).fetchdf()
    # conn.close()
    # conn = duckdb.connect(db_path)

    # # Query the locations table
    # # query3 = """
    # # SELECT title, city, latitude, longitude, TRUE AS is_title_duplicate, tag
    # # FROM duplicate_locations
    # # WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND tag='darkred';
    # # """
    # query3 = """
    # SELECT DISTINCT ON (contentid) *
    # FROM duplicate_locations
    # WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND tag='darkred';
    # """
    # duplicates_df3 = conn.execute(query3).fetchdf()
    # conn.close()

    # conn = duckdb.connect(db_path)

    # Query the locations table
    # query4 = """
    # SELECT title, city, latitude, longitude, TRUE AS is_title_duplicate, tag
    # FROM duplicate_locations
    # WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND tag='pink';
    # """
    # query4 = """
    # SELECT DISTINCT ON (contentid) *
    # FROM duplicate_locations
    # WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND tag='pink';
    # """
    # duplicates_df4 = conn.execute(query4).fetchdf()
    # conn.close()

    # conn = duckdb.connect(db_path)

    # # Query the locations table
    # query5 = """
    # SELECT DISTINCT ON (contentid) *
    # FROM duplicate_locations
    # WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND tag='purple';
    # """
    # duplicates_df5 = conn.execute(query5).fetchdf()
    # conn.close()


    # Filter locations for resolutions 8 and 9
    # for resolution in [8]:
    #     filtered_duplicates1 = filter_locations_by_lat_lon(
    #         duplicates_df1, "latitude", "longitude", 40.7128, -74.0060, resolution, RADIUS
    #     )
    #     filtered_duplicates2 = filter_locations_by_lat_lon(
    #         duplicates_df2, "latitude", "longitude", 40.7128, -74.0060, resolution, RADIUS
    #     )
    #     filtered_duplicates3 = filter_locations_by_lat_lon(
    #         duplicates_df3, "latitude", "longitude", 40.7128, -74.0060, resolution, RADIUS
    #     )
    #     filtered_duplicates4 = filter_locations_by_lat_lon(
    #         duplicates_df4, "latitude", "longitude", 40.7128, -74.0060, resolution, RADIUS
    #     )
    #     filtered_duplicates5 = filter_locations_by_lat_lon(
    #         duplicates_df5, "latitude", "longitude", 40.7128, -74.0060, resolution, RADIUS
    #     )
    #     filtered_locations = filter_locations_by_lat_lon(
    #         locations_df, "latitude", "longitude", 40.7128, -74.0060, resolution, RADIUS
    #     )
    #     all_filtered_locations_ny = filter_locations_by_lat_lon(
    #         all_locations_df, "latitude", "longitude", 40.7128, -74.0060, resolution, RADIUS
    #     )
    #     all_filtered_locations_ny_and_duplicates = filter_locations_by_lat_lon(
    #         all_locations_and_duplicates_df, "latitude", "longitude", 40.7128, -74.0060, resolution, RADIUS
    #     )
    #     all_location_comparison_rows_for_ny = fetch_all_location_comparison_rows(all_filtered_locations_ny["contentid"].astype(str).tolist())
    #     print(f"Total locations comparison rows for resolution {resolution}: {len(all_location_comparison_rows_for_ny)}")
    #     # store_filtered_locations_to_csv(all_location_comparison_rows_for_ny, f"ny-data/nyc_filtered_location_comparisons_resolution_{resolution}.csv")
    #     all_location_comparison_rows_for_ny_with_duplicates = fetch_all_location_comparison_rows(all_filtered_locations_ny_and_duplicates["contentid"].astype(str).tolist())
    #     print(f"Total locations and duplicates comparison rows for resolution {resolution}: {len(all_location_comparison_rows_for_ny_with_duplicates)}")
    #     # store_filtered_locations_to_csv(all_location_comparison_rows_for_ny_with_duplicates, f"ny-data/nyc_filtered_location_comparisons_with_duplicates_resolution_{resolution}.csv")
    #     # store_filtered_locations(filtered_locations, f"ny-data/nyc_filtered_locations_resolution_{resolution}.csv")
    #     # Print the number of filtered locations
    #     print(f"Total filtered locations for resolution {resolution}: {len(all_filtered_locations_ny)}")
    #     print(f"Total filtered duplicate ny locations(all scenarios) for resolution {resolution}: {len(filtered_locations)}")
    #     print(f"Total filtered duplicates with ny locations scenario1 for resolution {resolution}: {len(filtered_duplicates1)}")
    #     print(f"Total filtered duplicates with ny locations scenario2 for resolution {resolution}: {len(filtered_duplicates2)}")
    #     print(f"Total filtered duplicates with ny locations scenario3 for resolution {resolution}: {len(filtered_duplicates3)}")
    #     print(f"Total filtered duplicates with ny locations scenario4 for resolution {resolution}: {len(filtered_duplicates4)}")
    #     print(f"Total filtered duplicates with ny locations scenario5 for resolution {resolution}: {len(filtered_duplicates5)}")
    #     print(f"Total filtered locations and duplicates for resolution {resolution}: {len(all_filtered_locations_ny_and_duplicates)}")

    #     # Create the map
    #     output_file = f"ny-data/nyc_filtered_locations_resolution_{resolution}.html"
    #     output_file_duplicates1 = f"ny-data/nyc_filtered_duplicates_with_ny_locations_new_scenario1_resolution_{resolution}.html"
    #     output_file_duplicates2 = f"ny-data/nyc_filtered_duplicates_with_ny_locations_new_scenario2_resolution_{resolution}.html"
    #     output_file_duplicates3 = f"ny-data/nyc_filtered_duplicates_with_ny_locations_new_scenario3_resolution_{resolution}.html"
    #     output_file_duplicates4 = f"ny-data/nyc_filtered_duplicates_with_ny_locations_new_scenario4_resolution_{resolution}.html"
    #     output_file_duplicates5 = f"ny-data/nyc_filtered_duplicates_with_ny_locations_new_scenario5_resolution_{resolution}.html"
    #     output_file_including_duplicates = f"ny-data/nyc_filtered_including_duplicates_resolution_{resolution}.html"
    #     # create_h3_map(all_filtered_locations_ny_and_duplicates, "latitude", "longitude", output_file_including_duplicates)
    #     # output_file_including_duplicates = f"ny-data/nyc_filtered_including_duplicates_resolution_{resolution}.html"
    #     # Create a csv of filtered_locations
    #     # store_filtered_locations(filtered_locations, f"ny-data/nyc_filtered_locations_resolution_{resolution}.json")
    #     # store_filtered_locations(filtered_duplicates, f"ny-data/nyc_filtered_duplicates_resolution_{resolution}.json", duplicate=True)
    #     # create_h3_map(filtered_locations, "latitude", "longitude", output_file)
    #     # create_h3_map(filtered_duplicates1, "latitude", "longitude", output_file_duplicates1)
    #     # create_h3_map(filtered_duplicates2, "latitude", "longitude", output_file_duplicates2)
    #     # create_h3_map(filtered_duplicates3, "latitude", "longitude", output_file_duplicates3)
    #     # create_h3_map(filtered_duplicates4, "latitude", "longitude", output_file_duplicates4)
    #     # create_h3_map(filtered_duplicates5, "latitude", "longitude", output_file_duplicates5)
    #     # store_filtered_locations_to_csv(filtered_locations, f"ny-data/csv/nyc_filtered_locations_resolution_{resolution}.csv")
    #     # store_filtered_locations_to_csv(all_filtered_locations_ny_and_duplicates, f"ny-data/csv/nyc_filtered_locations_and_duplicates_resolution_{resolution}.csv")
    #     # delete_all_locations_except_filtered_locations(all_filtered_locations_ny_and_duplicates["contentid"].astype(str).tolist())
    #     # delete_all_location_comparisons_except_filtered_locations(all_filtered_locations_ny_and_duplicates["contentid"].astype(str).tolist())
    #     # store_filtered_locations_to_csv(filtered_duplicates1, f"ny-data/csv/nyc_filtered_duplicates_with_ny_locations_new_scenario1_resolution_{resolution}.csv")
    #     # store_filtered_locations_to_csv(filtered_duplicates2, f"ny-data/csv/nyc_filtered_duplicates_with_ny_locations_new_scenario2_resolution_{resolution}.csv")
    #     # store_filtered_locations_to_csv(filtered_duplicates3, f"ny-data/csv/nyc_filtered_duplicates_with_ny_locations_new_scenario3_resolution_{resolution}.csv")
    #     # store_filtered_locations_to_csv(filtered_duplicates4, f"ny-data/csv/nyc_filtered_duplicates_with_ny_locations_new_scenario4_resolution_{resolution}.csv")
    #     # store_filtered_locations_to_csv(filtered_duplicates5, f"ny-data/csv/nyc_filtered_duplicates_with_ny_locations_new_scenario5_resolution_{resolution}.csv")
    #     # create_h5_map(filtered_locations+filtered_duplicates, "latitude", "longitude", output_file_including_duplicates)
    create_h3_map_for_boundary_locations()
    # nyc_boundary = load_nyc_boundary()
    # nyc_locations_with_duplicates_by_boundary_df = filter_locations_within_nyc(all_locations_and_duplicates_df, nyc_boundary)
    # output_file_including_duplicates_with_boundary = f"ny-data/nyc_filtered_including_duplicates_resolution_with_boundary.html"
    # create_h3_map(nyc_locations_with_duplicates_by_boundary_df, "latitude", "longitude", output_file_including_duplicates_with_boundary, add_neighbourhood=False, neighbourhood_boundary=None)
    # ny_boundaries = load_nyc_neighbourhood_boundaries()
    # for index, boundary in enumerate(ny_boundaries):
    #     nyc_locations_with_duplicates_by_boundary_df_for_neighbourhood = filter_locations_within_nyc(all_locations_and_duplicates_df, shape(boundary["geometry"]))
    #     output_file_including_duplicates_with_boundary = f"ny-data/neighbourhood/nyc_filtered_including_duplicates_resolution_with_boundary_for_neighbourhood_{index}.html"
    #     print(f"Total locations in ny within ny boundary for neighbourhood {index}: {len(nyc_locations_with_duplicates_by_boundary_df_for_neighbourhood)}")
    #     if len(nyc_locations_with_duplicates_by_boundary_df_for_neighbourhood) > 0:
    #         create_h3_map(nyc_locations_with_duplicates_by_boundary_df_for_neighbourhood, "latitude", "longitude", output_file_including_duplicates_with_boundary, add_neighbourhood=True, neighbourhood_boundary=shape(boundary["geometry"]))
    #     else:
    #         print(f"No locations in ny within ny boundary for neighbourhood {index}")
    # # update_city_column_in_db(db_path, nyc_locations_with_duplicates_by_boundary_df)
    # print(f"Total locations in ny within ny boundary: {len(nyc_locations_with_duplicates_by_boundary_df)}")

if __name__ == "__main__":
    main()
