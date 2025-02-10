import json
import requests
from shapely.geometry import shape, Point
import duckdb
from folium.plugins import MarkerCluster
import folium

# Connect to DuckDB, query the locations table, and load the data into a Pandas DataFrame, for each location latitude and longitude identify if its within the ny city boundary
# Obtain the ny city boundary in json from  - https://github.com/generalpiston/geojson-us-city-boundaries/blob/master/cities/ny/new-york.json and write a python method to identify if it falls within this boundart
# Load NYC boundary from GeoJSON
def load_nyc_boundary():
    url = "https://raw.githubusercontent.com/generalpiston/geojson-us-city-boundaries/master/cities/ny/new-york.json"
    response = requests.get(url)
    nyc_boundary_geojson = response.json()
    nyc_boundary = shape(nyc_boundary_geojson['features'][0]['geometry'])
    return nyc_boundary

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
def create_h3_map(dataframe, lat_column, lon_column, output_file):
    # Create a Folium map centered at NYC
    nyc_map = folium.Map(location=[40.7826, -73.9656], tiles = 'cartodbpositron', zoom_start=11, control_scale=True)

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
#connect to duckdb and update the city column in locations with ny for the nyc_locations_df
def update_city_column_in_db(db_path, nyc_locations_df):
    conn = duckdb.connect(db_path)
    content_ids = tuple(nyc_locations_df['contentid'].tolist())
    query = f"UPDATE locations SET city = 'NY' WHERE contentid IN {content_ids};"
    conn.execute(query)
    conn.close()

# Main function to load and filter locations
def main():
    db_path = "data/locations.db"
    nyc_boundary = load_nyc_boundary()
    locations_df = load_locations_from_db(db_path)
    nyc_locations_df = filter_locations_within_nyc(locations_df, nyc_boundary)
    output_file_including_duplicates_with_boundary = f"ny-data/nyc_filtered_including_duplicates_resolution_with_boundary.html"
    create_h3_map(nyc_locations_df, "latitude", "longitude", output_file_including_duplicates_with_boundary)
    print(f"Total locations within NYC boundary: {len(nyc_locations_df)}")
    update_city_column_in_db(db_path, nyc_locations_df)
    # Further processing with nyc_locations_df

if __name__ == "__main__":
    main()