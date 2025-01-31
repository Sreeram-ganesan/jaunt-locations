import pandas as pd
import geopandas as gpd
import requests
import duckdb
import time
import os
import json
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
API_KEY = os.getenv("GM_SAVI")
if not API_KEY:
    raise ValueError("Error: API key not found. Please set the GOOGLE_MAPS_API_KEY environment variable.")
DATABASE_PATH = "data/locations.db"  # Path to your DuckDB database
BATCH_SIZE = 100  # Adjust batch size as needed
RATE_LIMIT_DELAY = 0.1  # Delay between batches to respect rate limits (adjust as needed)
MAX_RETRIES = 3  # Maximum retries for failed requests

# --- Helper Functions ---

import json

def parse_google_places_response(response_json):
    """
    Parses a JSON response from the Google Places API and prints details 
    one line at a time.  Handles the more complex structure of the 
    provided example.

    Args:
        response_json: A string containing the JSON response from the API.
    """
    try:
        data = json.loads(response_json)

        # No 'status' field in this example, so we assume success.
        # In a real API response, you should ALWAYS check the 'status'!

        print("--------------------")  # Separator for each place

        # Use .get() with default values throughout.
        name = data.get('name', 'N/A')
        place_id = data.get('id', 'N/A') # Use 'id' instead of 'place_id'
        formatted_address = data.get('formattedAddress', 'N/A') # Use 'formattedAddress'

        print(f"Name: {name}")
        print(f"Place ID: {place_id}")
        print(f"Formatted Address: {formatted_address}")

        location = data.get('location', {})
        latitude = location.get('latitude', 'N/A')
        longitude = location.get('longitude', 'N/A')
        print(f"Latitude: {latitude}")
        print(f"Longitude: {longitude}")

        types = data.get('types', [])
        print(f"Types: {', '.join(types) if types else 'N/A'}")

        # Address Components
        address_components = data.get('addressComponents', [])
        print("Address Components:")
        for component in address_components:
            print(f"  - {component.get('longText', 'N/A')} ({component.get('types', [])})")


        # Viewport
        viewport = data.get('viewport', {})
        low = viewport.get('low', {})
        high = viewport.get('high', {})
        print("Viewport:")
        print(f"  Low: Lat {low.get('latitude', 'N/A')}, Lng {low.get('longitude', 'N/A')}")
        print(f"  High: Lat {high.get('latitude', 'N/A')}, Lng {high.get('longitude', 'N/A')}")


        google_maps_uri = data.get('googleMapsUri', 'N/A')
        print(f"Google Maps URI: {google_maps_uri}")

        # ... (Add other fields as needed, using .get() and handling lists/dicts)

        # Example: Address Descriptor (Landmarks)
        address_descriptor = data.get('addressDescriptor', {})
        landmarks = address_descriptor.get('landmarks', [])
        print("Landmarks:")
        for landmark in landmarks:
            print(f"  - {landmark.get('displayName', {}).get('text', 'N/A')}") # Nested dict access

        # Example: Areas
        areas = data.get('areas', [])
        print("Areas:")
        for area in areas:
            print(f"  - {area.get('displayName', {}).get('text', 'N/A')}") # Nested dict access

        google_maps_links = data.get('googleMapsLinks', {})
        directions_uri = google_maps_links.get('directionsUri', 'N/A')
        place_uri = google_maps_links.get('placeUri', 'N/A')
        photos_uri = google_maps_links.get('photosUri', 'N/A')
        print(f"Google Maps Links: Directions - {directions_uri}, Place - {place_uri}, Photos - {photos_uri}")
        all_values = {
            "place_name": name,
            "place_id": place_id,
            "place_formatted_address": formatted_address,
            "place_latitude": latitude,
            "place_longitude": longitude,
            "place_types": types,
            "place_address_components": address_components,
            "place_viewport": {
                "Low": low,
                "High": high
            },
            "place_google_maps_uri": google_maps_uri,
            "place_landmarks": landmarks,
            "place_areas": areas,
            "place_google_maps_lins": {
                "place_directions": directions_uri,
                "place_uri": place_uri,
                "place_photos_uri": photos_uri
            }
        }
        return all_values


    except json.JSONDecodeError:
        print("Error: Invalid JSON response.")
    except Exception as e:  # Catch any other exceptions
        print(f"An unexpected error occurred: {e}")

# write a method to get google places information for given place_id and api_key that gives response in json
def get_place_details(place_id):
    url = f"https://places.googleapis.com/v1/places"
    params = {
        "key": API_KEY
    }
    path_params = f"/{place_id}"
    url = f"{url}{path_params}"
    print(f"Getting place details for place_id {place_id}...at url {url}")
    headers = {
        "X-Goog-FieldMask": "*"
    }

    retries = 0
    while retries < MAX_RETRIES:
        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            print(f"Place details for place_id {place_id}: {response.json()}")
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Request error for place_id {place_id}: {e}")
            retries += 1
            time.sleep(2**retries)  # Exponential backoff
            print(f"Retrying ({retries}/{MAX_RETRIES})...")
            continue
        except (KeyError, IndexError) as e:
            print(f"Error parsing JSON for place_id {place_id}: {e}")
            break

    print(f"Failed to get place details after {MAX_RETRIES} retries for place_id {place_id}")
    return None

# save response in locations table
def save_place_details_to_locations_table():
    con = duckdb.connect(DATABASE_PATH)
    query = "SELECT place_id FROM locations WHERE is_geo_coded = true and place_id is not null"
    place_ids = con.execute(query).fetchdf()['place_id'].tolist()
    print(place_ids)
    quer_to_add_column = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS place_details JSON"
    con.execute(quer_to_add_column)
    print(f"No of place_ids: {len(place_ids)}")
    for place_id in place_ids:
        print(place_id)
        print(f"Getting place details for place_id {place_id}")
        place_details = get_place_details(place_id)
        place_details = json.dumps(place_details)
        query = "UPDATE locations SET place_details = ? WHERE place_id = ?"
        print(query)
        con.execute(query, (place_details, place_id))
    con.close()
def geocode_batch(lat_longs):
    """Geocodes a batch of lat/longs using the Google Maps Geocoding API."""

    url = "https://maps.googleapis.com/maps/api/geocode/json"
    results = []

    for lat, lng, contentid in lat_longs:
        params = {
            "latlng": f"{lat},{lng}",
            "key": API_KEY,
            "language": "en"  # Set language as needed
        }

        retries = 0
        while retries < MAX_RETRIES:
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
                data = response.json()

                if data["status"] == "OK":
                    print(f"Geocoded {lat},{lng}: {data['results'][0]['formatted_address']}")
                    # add contentid to data["result"][0]
                    data["results"][0]["contentid"] = contentid
                    results.append(data["results"][0])  # Take the first result
                    break  # Exit retry loop on success
                elif data["status"] == "OVER_QUERY_LIMIT": # Handle rate limiting
                    print("Hit rate limit. Waiting...")
                    time.sleep(60) # Wait for a minute
                    retries=0 # Reset retry counter
                    continue # try again
                else:
                    print(data)
                    print(f"Geocoding failed for {lat},{lng}: {data['status']}")
                    results.append(None)  # Append None for failed lookups
                    break  # Exit retry loop
            except requests.exceptions.RequestException as e:
                print(f"Request error for {lat},{lng}: {e}")
                retries += 1
                time.sleep(2**retries) # Exponential backoff
                print(f"Retrying ({retries}/{MAX_RETRIES})...")
                continue
            except (KeyError, IndexError) as e:  # Handle cases where the JSON structure is unexpected
                print(f"Error parsing JSON for {lat},{lng}: {e}. Data: {data}")
                results.append(None)
                break

        if retries == MAX_RETRIES:
            print(f"Geocoding failed after {MAX_RETRIES} retries for {lat},{lng}")
            results.append(None)

    return results


def process_geocoding_results(results):
    """Extracts relevant information from the geocoding results."""
    processed_results = []
    for result in results:
        if result:
            print("Result: ", result)
            address_components = result.get("address_components", [])
            formatted_address = result.get("formatted_address", "")
            latitude = result.get("geometry", {}).get("location", {}).get("lat")
            longitude = result.get("geometry", {}).get("location", {}).get("lng")

            city = next((comp["long_name"] for comp in address_components if "locality" in comp["types"]), None)
            state = next((comp["long_name"] for comp in address_components if "administrative_area_level_1" in comp["types"]), None)
            country = next((comp["long_name"] for comp in address_components if "country" in comp["types"]), None)
            postcode = next((comp["long_name"] for comp in address_components if "postal_code" in comp["types"]), None)
            neighborhood = next((comp["long_name"] for comp in address_components if "neighborhood" in comp["types"]), None)
            sublocality = next((comp["long_name"] for comp in address_components if "sublocality" in comp["types"]), None)
            place_id = result.get("place_id", "")
            plus_code = result.get("plus_code", {}).get("global_code", "")
            contentid = result.get("contentid", "")
            # add point of intereset
            processed_results.append({
                "latitude": latitude,
                "longitude": longitude,
                "formatted_address": formatted_address,
                "city": city,
                "state": state,
                "country": country,
                "postcode": postcode,
                "neighborhood": neighborhood,
                "sublocality": sublocality,
                "place_id": place_id,
                "plus_code": plus_code,
                "complete_result": result,
                "contentid": contentid
            })
        else:
            processed_results.append(None)  # Keep None for failed lookups
    return processed_results



# --- Main Script ---

if __name__ == "__main__":
    is_geo_coding_enabled = os.environ.get("IS_GEO_CODING_ENABLED") == 'true'
    is_place_details_enabled = os.environ.get("IS_PLACE_DETAILS_ENABLED") == 'true'
    print_place_details = os.environ.get("PRINT_PLACE_DETAILS") == 'true'
    if is_geo_coding_enabled:
        print("Geocoding is enabled")
        try:
            # 1. Read lat/long data (replace with your actual file reading method)
            df = pd.read_csv("ny-data/csv/nyc_filtered_locations_and_duplicates_resolution_8.csv")  # Assumes CSV with latitude and longitude columns
        except FileNotFoundError:
            print("Error: CSV file not found. Please provide the correct file path.")
            exit()
        # 2. Connect to DuckDB
        con = duckdb.connect(DATABASE_PATH)

        # 3. Query locations from DuckDB
        # locations_df = con.execute("SELECT latitude, longitude FROM locations ").fetchdf()

        # 3. Process in batches
        for i in range(0, len(df), BATCH_SIZE):
            batch = df[i:i + BATCH_SIZE]
            lat_longs = list(zip(batch["latitude"], batch["longitude"], batch["contentid"]))

            # 4. Geocode the batch
            geocoding_results = geocode_batch(lat_longs)

            # 5. Process results
            processed_results = process_geocoding_results(geocoding_results)

            # 6. Insert into DuckDB
            for result in processed_results:
                if result:
                    try:
                        con.execute("ALTER TABLE locations ADD COLUMN IF NOT EXISTS city TEXT")
                        con.execute("ALTER TABLE locations ADD COLUMN IF NOT EXISTS state TEXT")
                        con.execute("ALTER TABLE locations ADD COLUMN IF NOT EXISTS country TEXT")
                        con.execute("ALTER TABLE locations ADD COLUMN IF NOT EXISTS postcode TEXT")
                        con.execute("ALTER TABLE locations ADD COLUMN IF NOT EXISTS neighborhood TEXT")
                        con.execute("ALTER TABLE locations ADD COLUMN IF NOT EXISTS is_geo_coded BOOLEAN")
                        con.execute("ALTER TABLE locations ADD COLUMN IF NOT EXISTS formatted_address TEXT")
                        con.execute("ALTER TABLE locations ADD COLUMN IF NOT EXISTS sublocality TEXT")
                        con.execute("ALTER TABLE locations ADD COLUMN IF NOT EXISTS place_id TEXT")
                        con.execute("ALTER TABLE locations ADD COLUMN IF NOT EXISTS plus_code TEXT")
                        con.execute("ALTER TABLE locations ADD COLUMN IF NOT EXISTS complete_result JSON")
                        complete_result_json = json.dumps(result['complete_result'])
                        query = f"UPDATE locations SET formatted_address = '{result['formatted_address']}', city = '{result['city']}', state = '{result['state']}', country = '{result['country']}', postcode = '{result['postcode']}', neighborhood = '{result['neighborhood']}', is_geo_coded = true, sublocality = '{result['sublocality']}', place_id = '{result['place_id']}', plus_code = '{result['plus_code']}' WHERE contentid = '{result['contentid']}'"
                        complete_result_query = f"UPDATE locations SET complete_result = '{complete_result_json}' WHERE contentid = '{result['contentid']}'"
                        print(query)
                        con.execute(query)
                        print(complete_result_query)
                        con.execute(complete_result_query)
                    except Exception as e:
                        print(f"Error inserting into database: {e}")
                        print(result)
                else:
                    print("Geocoding failed for a location.")

            print(f"Processed batch {i // BATCH_SIZE + 1} of {len(df) // BATCH_SIZE + (1 if len(df) % BATCH_SIZE > 0 else 0)}")
            time.sleep(RATE_LIMIT_DELAY) # Respect rate limits

        con.close()
        print("Geocoding and database insertion complete.")
    if is_place_details_enabled:
        print("Place details is enabled")
        save_place_details_to_locations_table()
        print("Place details saved to locations table")
    if print_place_details:
        con = duckdb.connect(DATABASE_PATH)
        places_details_df = con.execute("SELECT contentid, place_details FROM locations WHERE place_details is not null and is_geo_coded = true limit 5").fetchdf()
        for index, row in places_details_df.iterrows():
            place_details = row['place_details']
            places_parsed_values = parse_google_places_response(place_details)
            contentid = row['contentid']
            add_column_1 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS place_name TEXT"
            add_column_2 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS place_formatted_address TEXT"
            add_column_3 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS place_latitude TEXT"
            add_column_4 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS place_longitude TEXT"
            add_column_5 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS place_types TEXT"
            add_column_6 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS place_address_components TEXT"
            add_column_7 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS place_viewport TEXT"
            add_column_8 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS place_google_maps_uri TEXT"
            add_column_9 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS place_landmarks TEXT"
            add_column_10 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS place_areas TEXT"
            add_column_11 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS place_google_maps_links TEXT"
            con.execute(add_column_1)
            con.execute(add_column_2)
            con.execute(add_column_3)
            con.execute(add_column_4)
            con.execute(add_column_5)
            con.execute(add_column_6)
            con.execute(add_column_7)
            con.execute(add_column_8)
            con.execute(add_column_9)
            con.execute(add_column_10)
            con.execute(add_column_11)
            query = """
            UPDATE locations SET 
                place_name = ?, 
                place_formatted_address = ?, 
                place_latitude = ?, 
                place_longitude = ?, 
                place_types = ?, 
                place_address_components = ?, 
                place_viewport = ?, 
                place_google_maps_uri = ?, 
                place_landmarks = ?, 
                place_areas = ?, 
                place_google_maps_links = ? 
            WHERE contentid = ?
            AND is_geo_coded = true
            AND place_details is not null
            """
            con.execute(query, (
                places_parsed_values['place_name'], 
                places_parsed_values['place_formatted_address'], 
                places_parsed_values['place_latitude'], 
                places_parsed_values['place_longitude'], 
                json.dumps(places_parsed_values['place_types']), 
                json.dumps(places_parsed_values['place_address_components']), 
                json.dumps(places_parsed_values['place_viewport']), 
                places_parsed_values['place_google_maps_uri'], 
                json.dumps(places_parsed_values['place_landmarks']), 
                json.dumps(places_parsed_values['place_areas']), 
                json.dumps(places_parsed_values['place_google_maps_lins']), 
                contentid
            ))
        con.close()