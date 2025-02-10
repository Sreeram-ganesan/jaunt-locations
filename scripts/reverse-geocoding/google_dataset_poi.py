import os
import requests
import json
from dotenv import load_dotenv
import duckdb
import time
import folium
import geopandas as gpd
import pandas as pd
from shapely.geometry import shape
from folium.plugins import MarkerCluster
load_dotenv()
# write a method that downloads all the poi data from google apis for new york
# and saves it to duckdb as a table
BATCH_SIZE = 1  # Adjust batch size as needed
RATE_LIMIT_DELAY = 0.1  # Delay between batches to respect rate limits (adjust as needed)
MAX_RETRIES = 3  # Maximum retries for failed requests
RADIUS = 50000  # Radius in meters for places search
API_KEY = os.getenv("GM_SAVI")
print(API_KEY)
if not API_KEY:
    raise ValueError("Error: API key not found. Please set the GOOGLE_MAPS_API_KEY environment variable.")
def download_google_poi_data():
    api_key = API_KEY
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {
        "query": "new+york+tourist+attractions",
        "key": api_key
    }
    headers = {"X-Goog-FieldMask": "*"}
    place_ids = set()
    conn = duckdb.connect('data/locations.db')
    conn.execute("CREATE TABLE IF NOT EXISTS google_poi (json_data JSON, lat DOUBLE, lon DOUBLE)")

    total_calls = 0
    retries = 0
    next_page_token = None

    while total_calls < 10:
        for _ in range(BATCH_SIZE):
            if total_calls >= 100:
                break
            try:
                response = requests.get(url, params=params, headers=headers)
                response.raise_for_status()
                data = response.json()
                for result in data.get("results", []):
                    pid = result.get("place_id")
                    if pid and pid not in place_ids:
                        place_ids.add(pid)
                        lat = float(result["geometry"]["location"]["lat"])
                        lng = float(result["geometry"]["location"]["lng"])
                        conn.execute("INSERT INTO google_poi VALUES (?, ?, ?)",
                                     (json.dumps(result), lat, lng))

                next_page_token = data.get("next_page_token")
                total_calls += 1

                if not next_page_token:
                    break
                params["pagetoken"] = next_page_token
                time.sleep(2)  # Wait for the token to become valid
            except requests.exceptions.RequestException as e:
                print(f"Request error: {e}")
                retries += 1
                if retries >= MAX_RETRIES:
                    break
                time.sleep(2**retries)

        if not next_page_token:
            break
        time.sleep(RATE_LIMIT_DELAY)

    conn.close()
def create_h3_map(dataframe, lat_column, lon_column, output_file, add_neighbourhood=True,neighbourhood_boundary=None):
    # Create a Folium map centered at NYC
    nyc_map = folium.Map(location=[40.7826, -73.9656], tiles='cartodbpositron', zoom_start=11, control_scale=True)

    # Add NYC boundary to the map
    if add_neighbourhood:
        folium.GeoJson(
            neighbourhood_boundary,
            name='NYC Boundary',
            style_function=lambda x: {'color': 'green', 'weight': 2, 'fillOpacity': 0.1}
        ).add_to(nyc_map)
    # else:
    #     folium.GeoJson(
    #         load_nyc_boundary(),
    #         name='NYC Boundary',
    #         style_function=lambda x: {'color': 'blue', 'weight': 2, 'fillOpacity': 0.1}
    #     ).add_to(nyc_map)
    #     ny_boundaries = load_nyc_neighbourhood_boundaries()
    #     for index, boundary in enumerate(ny_boundaries):
    #         if boundary["geometry"] is not None:
    #             print(f"Adding boundary {index}")
    #             folium.GeoJson(
    #                 shape(boundary["geometry"]),
    #                 name=f"NYC Boundary - {index}",
    #                 style_function=lambda _: {'color': 'green', 'weight': 2, 'fillOpacity': 0.4 * (index % len(ny_boundaries)) / len(ny_boundaries)}
    #             ).add_to(nyc_map)

    # Add markers for each location
    locations = []
    # tag_counts = {"red": 0, "lightred": 0, "darkred": 0, "pink": 0, "purple": 0, "blue": 0}
    for _, row in dataframe.iterrows():
        title = row["name"]
        contentid = row["place_id"]
        # description = (row["description"] or "")[:20] + "..." if row["description"] and len(row["description"]) > 20 else (row["description"] or "")
        source_url = "google"
        # is_duplicate = row["is_title_duplicate"]
        colour = "blue"
        # Count the occurrences of each tag color
        # if colour in tag_counts:
        #     tag_counts[colour] += 1
        location = [row[lat_column], row[lon_column]]
        locations.append(location)
        popup_text = f"Title: {title}<br>Source_URL: {source_url}<br>UniqueID: {contentid}<br>Point: {str(row[lat_column])+ ',' + str(row[lon_column])}"
        folium.Marker(
            location=location,
            icon=folium.Icon(color=colour),
            popup=popup_text
        ).add_to(nyc_map)
    
    # Print the counts
    # for tag, count in tag_counts.items():
    #     print(f"{tag}: {count}")
    MarkerCluster(locations).add_to(nyc_map)

    # Save the map to an HTML file
    nyc_map.save(output_file)
    print(f"Map saved to {output_file}.")
def load_nyc_boundary():
    url = "https://raw.githubusercontent.com/generalpiston/geojson-us-city-boundaries/master/cities/ny/new-york.json"
    response = requests.get(url)
    nyc_boundary_geojson = response.json()
    nyc_boundary = shape(nyc_boundary_geojson['features'][0]['geometry'])
    return nyc_boundary
if __name__ == "__main__":
    # download_google_poi_data()
    conn = duckdb.connect('data/locations.db')
    data = conn.execute("SELECT * FROM google_poi where place_id in (select distinct place_id from google_poi)").fetchall()
    conn.close()
    result = pd.DataFrame(data, columns=["json_data", "lat", "long", "place_id"])
    for index, row in result.iterrows():
        print(row)
        result.loc[index, "name"] = json.loads(row["json_data"])["name"]
        result.loc[index, "place_id"] = row["place_id"]
        result.loc[index, "lat"] = float(json.loads(row["json_data"])["geometry"]["location"]["lat"])
        result.loc[index, "lon"] = float(json.loads(row["json_data"])["geometry"]["location"]["lng"])
        print(result.loc[index])
    create_h3_map(result, "lat", "lon", "google_poi.html", add_neighbourhood=True, neighbourhood_boundary=load_nyc_boundary())