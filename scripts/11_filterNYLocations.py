import folium
import duckdb
from shapely.geometry import Point, Polygon

# Define NYC boundary as a polygon (simplified example; use a more accurate boundary for precision)
NYC_BOUNDARY_COORDS = [
    [-74.25909, 40.477399],  # Southwest corner
    [-73.700181, 40.477399],  # Southeast corner
    [-73.700181, 40.917577],  # Northeast corner
    [-74.25909, 40.917577],   # Northwest corner
    [-74.25909, 40.477399]    # Closing the loop
]
NYC_BOUNDARY_POLYGON = Polygon(NYC_BOUNDARY_COORDS)

# Function to check if a latitude and longitude is within NYC
def is_within_nyc(lat, lon):
    point = Point(lon, lat)  # Shapely uses (lon, lat)
    return NYC_BOUNDARY_POLYGON.contains(point)

# Function to filter locations within NYC
def filter_locations_by_nyc(locations_df, lat_column, lon_column):
    return locations_df[
        locations_df.apply(lambda row: is_within_nyc(row[lat_column], row[lon_column]), axis=1)
    ]

# Function to create a map for filtered locations
def create_nyc_map(dataframe, lat_column, lon_column, output_file):
    # Create a Folium map centered at NYC
    nyc_map = folium.Map(location=[40.7128, -74.0060], zoom_start=10)

    # Add markers for each location
    for _, row in dataframe.iterrows():
        folium.Marker(
            location=[row[lat_column], row[lon_column]],
            icon=folium.Icon(color="blue"),
        ).add_to(nyc_map)

    # Save the map to an HTML file
    nyc_map.save(output_file)
    print(f"Map saved to {output_file}.")

# Main function
def main():
    # Connect to DuckDB
    db_path = "data/locations.db"  # Update with your database path
    conn = duckdb.connect(db_path)

    # Query the locations table
    query = """
    SELECT contentid, title, latitude, longitude, description, city, state
    FROM locations 
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
    """
    locations_df = conn.execute(query).fetchdf()

    # Close the connection
    conn.close()

    # Filter locations within NYC
    filtered_locations = filter_locations_by_nyc(locations_df, "latitude", "longitude")

    # Print the number of filtered locations
    print(f"Total filtered locations: {len(filtered_locations)}")
    # Create the map
    # output_file = "nyc_filtered_locations_based_non_h3_index.html"
    # create_nyc_map(filtered_locations, "latitude", "longitude", output_file)

    #write a method to extract the data from databse as csv file filtered by NYC
    filtered_locations.to_csv("data/nyc_filtered_locations.csv", index=False)
if __name__ == "__main__":
    main()
