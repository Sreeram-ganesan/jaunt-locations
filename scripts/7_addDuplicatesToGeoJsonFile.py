import json
import duckdb

def read_from_db_and_update_geojson():
    # Step 1: Connect to the database
    conn = duckdb.connect(database='data/locations.db')

    # Step 2: Query the duplicate_locations table
    locations = conn.execute("SELECT title, h3_index_8, latitude, longitude, contentid FROM duplicate_locations").fetchall()

    # Step 3: Open the output_all.geojson file
    with open('data/output_all.geojson', 'r') as file:
        geojson_data = json.load(file)

    # Step 4: Insert new values into the GeoJSON file
    for location in locations:
        feature = {
            "type": "Feature",
            "properties": {
                "title": location[1],
                "marker-color": "#FFC0CB"  # Step 5: Apply pink color styling
            },
            "geometry": {
                "type": "Point",
                "coordinates": [location[3], location[2]]
            }
        }
        geojson_data['features'].append(feature)

    # Step 6: Save and close the GeoJSON file
    with open('data/output_duplicates.geojson', 'w') as file:
        json.dump(geojson_data, file, indent=4)

    # Close the database connection
    conn.close()