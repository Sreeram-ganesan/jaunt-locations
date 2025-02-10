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
import csv

def get_place_details_os(name: str, lat: float, lon: float):
    """
    Fetches place details using the OpenStreetMap Nominatim API.

    Args:
        name (str): Name of the place.
        lat (float): Approximate latitude.
        lon (float): Approximate longitude.

    Returns:
        dict: Place details including display name, type, and address.
    """
    print(f"Getting place details for {name} at {lat}, {lon}...")
    url = "https://nominatim.openstreetmap.org/reverse"
    params = {
        "format": "json",
        "lat": lat,
        "lon": lon,
        "zoom": 10,  # Adjust zoom level for detail granularity
        "addressdetails": 1
    }

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:134.0) Gecko/20100101 Firefox/134.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-GB,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Priority": "u=0, i"
        }
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()  # Raise exception for HTTP errors
        data = response.json()
        return data
    except requests.RequestException as e:
        return {"error": str(e)}

# # Example Usage
# place_details = get_place_details("Edinburgh Castle", 55.9486, -3.1999)
# print(place_details)

def parse_google_places_response(response_json):
    """
    Parses a JSON response from the Google Places API and returns details 
    as a dictionary.

    Args:
        response_json: A string containing the JSON response from the API.

    Returns:
        dict: Parsed place details.
    """
    try:
        if isinstance(response_json, dict):
            data = response_json
        else:
            data = json.loads(response_json)

        # Top-level fields
        name = data.get("name", "N/A")
        location = data.get("location", {})
        latitude = location.get("latitude", "N/A")
        location = data.get("location", {})
        latitude = location.get("latitude", "N/A")
        longitude = location.get("longitude", "N/A")
        viewport = data.get("viewport", {})
        low = viewport.get("low", {})
        high = viewport.get("high", {})
        google_maps_uri = data.get("googleMapsUri", "N/A")
        google_maps_links = data.get("googleMapsLinks", {})
        website_uri = data.get("websiteUri", "N/A")
        rating = data.get("rating", "N/A")
        user_rating_count = data.get("userRatingCount", "N/A")
        business_status = data.get("businessStatus", "N/A")
        phone_number = data.get("nationalPhoneNumber", "N/A")
        primary_type = data.get("primaryType", "N/A")
        current_opening_hours = data.get("currentOpeningHours", {})
        reviews = data.get("reviews", [])
        photos = data.get("photos", [])
        good_for_children = data.get("goodForChildren", "N/A")
        accessibility_options = data.get("accessibilityOptions", {})
        generative_summary = data.get("generativeSummary", {})
        utc_offset_minutes = data.get("utcOffsetMinutes", "N/A")
        adr_format_address = data.get("adrFormatAddress", "N/A")
        icon_mask_base_uri = data.get("iconMaskBaseUri", "N/A")
        icon_background_color = data.get("iconBackgroundColor", "N/A")
        display_name = data.get("displayName", {})
        display_name_text = display_name.get("text", "N/A")
        display_name_language_code = display_name.get("languageCode", "N/A")
        primary_type_display_name = data.get("primaryTypeDisplayName", {})
        primary_type_display_name_text = primary_type_display_name.get("text", "N/A")
        primary_type_display_name_lang = primary_type_display_name.get("languageCode", "N/A")
        short_formatted_address = data.get("shortFormattedAddress", "N/A")
        pure_service_area_business = data.get("pureServiceAreaBusiness", "N/A")

        # Address descriptor
        address_descriptor = data.get("addressDescriptor", {})
        landmarks_full = []
        for lm in address_descriptor.get("landmarks", []):
            landmarks_full.append({
            "name": lm.get("name", "N/A"),
            "displayName": lm.get("displayName", {}),
            "types": lm.get("types", []),
            "spatialRelationship": lm.get("spatialRelationship", "N/A"),
            "straightLineDistanceMeters": lm.get("straightLineDistanceMeters", "N/A"),
            "travelDistanceMeters": lm.get("travelDistanceMeters", "N/A")
            })
        areas_full = []
        for ar in address_descriptor.get("areas", []):
            areas_full.append({
            "name": ar.get("name", "N/A"),
            "displayName": ar.get("displayName", {}),
            "containment": ar.get("containment", "N/A")
            })

        # Final aggregation
        all_values = {
            "place_name": name,
            "location": location,
            "latitude": latitude,
            "longitude": longitude,
            "viewport": {
            "low": low,
            "high": high
            },
            "google_maps_uri": google_maps_uri,
            "google_maps_links": google_maps_links,
            "website_uri": website_uri,
            "place_rating": rating,
            "user_rating_count": user_rating_count,
            "business_status": business_status,
            "phone_number": phone_number,
            "primary_type": primary_type,
            "current_opening_hours": current_opening_hours,
            "reviews": reviews,
            "photos": photos,
            "good_for_children": good_for_children,
            "accessibility_options": accessibility_options,
            "generative_summary": generative_summary,
            "utc_offset_minutes": utc_offset_minutes,
            "adr_format_address": adr_format_address,
            "icon_mask_base_uri": icon_mask_base_uri,
            "icon_background_color": icon_background_color,
            "display_name": {
            "text": display_name_text,
            "languageCode": display_name_language_code
            },
            "primary_type_display_name": {
            "text": primary_type_display_name_text,
            "languageCode": primary_type_display_name_lang
            },
            "short_formatted_address": short_formatted_address,
            "pure_service_area_business": pure_service_area_business,
            "address_descriptor": {
            "landmarks": landmarks_full,
            "areas": areas_full
            }
        }
        return all_values

    except json.JSONDecodeError:
        print("Error: Invalid JSON response.")
    except Exception as e:
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

# write a function to call places api with place name and approximate location and get place_id
def get_place_id(place_name, latitude, longitude, contentid):
    url = f"https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
    params = {
        "key": API_KEY,
        "input": place_name,
        "inputtype": "textquery",
        "locationbias": f"point:{latitude},{longitude}"
    }
    print(f"Getting place_id for place_name {place_name} at location {latitude},{longitude}...at url {url}")
    retries = 0
    while retries < MAX_RETRIES:
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            place_candidates = data.get("candidates", [])
            print(f"Place candidates for {place_name}: {place_candidates}")
            if place_candidates:
                place_id = place_candidates[0].get("place_id")
                place_name = place_candidates[0].get("name")
                print(f"Place ID for {place_name}: {place_id}")
                return place_id
            else:
                print(f"No place ID found for {place_name}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"Request error for place_name {place_name}: {e}")
            retries += 1
            time.sleep(2**retries)
            print(f"Retrying ({retries}/{MAX_RETRIES})...")
            continue
        except (KeyError, IndexError) as e:
            print(f"Error parsing JSON for place_name {place_name}: {e}")
            break

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


# write a method that takes address_components, example below:
# [{"longText": "52-11", "shortText": "52-11", "types": ["street_number"], "languageCode": "en-US"}, {"longText": "111th Street", "shortText": "111th St", "types": ["route"], "languageCode": "en"}, {"longText": "Corona", "shortText": "Corona", "types": ["neighborhood", "political"], "languageCode": "en"}, {"longText": "Queens", "shortText": "Queens", "types": ["sublocality_level_1", "sublocality", "political"], "languageCode": "en"}, {"longText": "Queens County", "shortText": "Queens County", "types": ["administrative_area_level_2", "political"], "languageCode": "en"}, {"longText": "New York", "shortText": "NY", "types": ["administrative_area_level_1", "political"], "languageCode": "en"}, {"longText": "United States", "shortText": "US", "types": ["country", "political"], "languageCode": "en"}, {"longText": "11368", "shortText": "11368", "types": ["postal_code"], "languageCode": "en-US"}]
# and returns a dictionary of format below:
# {
#     "street_number": "52-11",
#     "route": "111th Street",
#     "neighborhood": "Corona",
#     "sublocality_level_1": "Queens",
#     "administrative_area_level_2": "Queens County",
#     "administrative_area_level_1": "New York",
#     "country": "United States",
#     "postal_code": "11368",
#     "state": "NY",
#     "city": "Corona",
#     "formatted_address": "52-11 111th Street, Corona, NY 11368, United States"
# }
# */
def parse_address_components_method(address_components):
    """Parses address components from the geocoding results."""
    parsed_address = {}
    for component in address_components:
        long_text = component.get("longText", "")
        short_text = component.get("shortText", "")
        types = component.get("types", [])
        if "street_number" in types:
            parsed_address["street_number"] = long_text
        if "route" in types:
            parsed_address["route"] = long_text
        if "neighborhood" in types:
            parsed_address["neighborhood"] = long_text
        if "sublocality_level_1" in types:
            parsed_address["sublocality_level_1"] = long_text
        if "administrative_area_level_2" in types:
            parsed_address["administrative_area_level_2"] = long_text
        if "administrative_area_level_1" in types:
            parsed_address["administrative_area_level_1"] = long_text
            parsed_address["state"] = short_text
        if "colloquial_area" in types:
            parsed_address["colloquial_area"] = long_text
        if "sublocality" in types:
            parsed_address["sublocality"] = long_text
        if "natural_feature" in types:
            parsed_address["natural_feature"] = long_text
        if "airport" in types:
            parsed_address["airport"] = long_text
        if "park" in types:
            parsed_address["park"] = long_text
        if "premise" in types:
            parsed_address["premise"] = long_text
        if "subpremise" in types:
            parsed_address["subpremise"] = long_text
        if "landmark" in types:
            parsed_address["landmark"] = long_text
        if "postal_town" in types:
            parsed_address["postal_town"] = long_text
        if "administrative_area_level_3" in types:
            parsed_address["administrative_area_level_3"] = long_text
        if "administrative_area_level_4" in types:
            parsed_address["administrative_area_level_4"] = long_text
        if "administrative_area_level_5" in types:
            parsed_address["administrative_area_level_5"] = long_text
        if "administrative_area_level_6" in types:
            parsed_address["administrative_area_level_6"] = long_text
        if "room" in types:
            parsed_address["room"] = long_text
        if "floor" in types:
            parsed_address["floor"] = long_text
        if "street_address" in types:
            parsed_address["street_address"] = long_text
        if "intersection" in types:
            parsed_address["intersection"] = long_text
        if "bus_station" in types:
            parsed_address["bus_station"] = long_text
        if "train_station" in types:
            parsed_address["train_station"] = long_text
        if "transit_station" in types:
            parsed_address["transit_station"] = long_text
        if "country" in types:
            parsed_address["country"] = long_text
        if "postal_code" in types:
            parsed_address["postal_code"] = long_text
        if "locality" in types:
            parsed_address["city"] = long_text
        if "point_of_interest" in types:
            parsed_address["point_of_interest"] = long_text
    parsed_address["formatted_address"] = ", ".join([v for k, v in parsed_address.items() if k != "state"])
    return parsed_address
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
    open_street_map_enable = os.environ.get("OPEN_STREET_MAP_ENABLE") == 'true'
    is_place_details_with_name_and_lat_long_enabled = os.environ.get("IS_PLACE_DETAILS_WITH_NAME_AND_LAT_LONG_ENABLED") == 'true'
    parse_place_details_to_locations_details_table = os.environ.get("PARSE_PLACE_DETAILS_TO_LOCATIONS_DETAILS_TABLE") == 'true'
    parse_address_components_enabled = os.environ.get("PARSE_ADDRESS_COMPONENTS_TO_LOCATIONS_DETAILS_TABLE") == 'true'
    parse_google_places_response_enabled = os.environ.get("PARSE_GOOGLE_PLACES_RESPONSE_TO_LOCATIONS_DETAILS_TABLE") == 'true'
    if parse_google_places_response_enabled:
        # to parse a sample google places response like - place_details = {"name": "places/ChIJEwC599BfwokRgLMwcXHVt08", "id": "ChIJEwC599BfwokRgLMwcXHVt08", "types": ["premise"], "formattedAddress": "52-11 111th St, Corona, NY 11368, USA", "addressComponents": [{"longText": "52-11", "shortText": "52-11", "types": ["street_number"], "languageCode": "en-US"}, {"longText": "111th Street", "shortText": "111th St", "types": ["route"], "languageCode": "en"}, {"longText": "Corona", "shortText": "Corona", "types": ["neighborhood", "political"], "languageCode": "en"}, {"longText": "Queens", "shortText": "Queens", "types": ["sublocality_level_1", "sublocality", "political"], "languageCode": "en"}, {"longText": "Queens County", "shortText": "Queens County", "types": ["administrative_area_level_2", "political"], "languageCode": "en"}, {"longText": "New York", "shortText": "NY", "types": ["administrative_area_level_1", "political"], "languageCode": "en"}, {"longText": "United States", "shortText": "US", "types": ["country", "political"], "languageCode": "en"}, {"longText": "11368", "shortText": "11368", "types": ["postal_code"], "languageCode": "en-US"}], "location": {"latitude": 40.7445889, "longitude": -73.8507216}, "viewport": {"low": {"latitude": 40.743163770775546, "longitude": -73.85212968029151}, "high": {"latitude": 40.745861731358545, "longitude": -73.84943171970849}}, "googleMapsUri": "https://maps.google.com/?cid=5744294532941394816", "utcOffsetMinutes": -300, "adrFormatAddress": "<span class=\"street-address\">52-11 111th St</span>, <span class=\"locality\">Corona</span>, <span class=\"region\">NY</span> <span class=\"postal-code\">11368</span>, <span class=\"country-name\">USA</span>", "iconMaskBaseUri": "https://maps.gstatic.com/mapfiles/place_api/icons/v2/generic_pinlet", "iconBackgroundColor": "#7B9EB0", "displayName": {"text": "52-11 111th St"}, "primaryTypeDisplayName": {"text": "Building", "languageCode": "en-US"}, "primaryType": "premise", "shortFormattedAddress": "52-11 111th St, Corona", "pureServiceAreaBusiness": false, "addressDescriptor": {"landmarks": [{"name": "places/ChIJr7mhTtBfwokR9qLLLPyyLUE", "placeId": "ChIJr7mhTtBfwokR9qLLLPyyLUE", "displayName": {"text": "Terrace On The Park", "languageCode": "en"}, "types": ["establishment", "point_of_interest"], "spatialRelationship": "AROUND_THE_CORNER", "straightLineDistanceMeters": 29.478262, "travelDistanceMeters": 73.76372}, {"name": "places/ChIJr4Vyr9BfwokRwIj7AIr7bzE", "placeId": "ChIJr4Vyr9BfwokRwIj7AIr7bzE", "displayName": {"text": "Queens Zoo", "languageCode": "en"}, "types": ["establishment", "point_of_interest", "tourist_attraction", "zoo"], "straightLineDistanceMeters": 136.58391, "travelDistanceMeters": 267.67877}, {"name": "places/ChIJG2aZ1wT2wokRzz-VKP0lkJg", "placeId": "ChIJG2aZ1wT2wokRzz-VKP0lkJg", "displayName": {"text": "New York Hall of Science", "languageCode": "en"}, "types": ["establishment", "museum", "point_of_interest", "tourist_attraction"], "straightLineDistanceMeters": 318.0729, "travelDistanceMeters": 638.78284}, {"name": "places/ChIJMU3Q4tBfwokRhVWJiwGK_Yw", "placeId": "ChIJMU3Q4tBfwokRhVWJiwGK_Yw", "displayName": {"text": "Fantasy Forest Carousel Park", "languageCode": "en"}, "types": ["amusement_park", "establishment", "park", "point_of_interest", "tourist_attraction"], "straightLineDistanceMeters": 197.67273, "travelDistanceMeters": 228.85631}, {"name": "places/ChIJmbUpINFfwokRIg4fft6_jqI", "placeId": "ChIJmbUpINFfwokRIg4fft6_jqI", "displayName": {"text": "Flushing Meadows Carousel", "languageCode": "en"}, "types": ["establishment", "point_of_interest"], "straightLineDistanceMeters": 197.9763, "travelDistanceMeters": 228.85631}], "areas": [{"name": "places/ChIJ-2Yhwn9gwokRTkMRuy2ay8E", "placeId": "ChIJ-2Yhwn9gwokRTkMRuy2ay8E", "displayName": {"text": "Flushing Meadows Corona Park", "languageCode": "en"}, "containment": "OUTSKIRTS"}, {"name": "places/ChIJAZQmNsxfwokRULFner5q3VQ", "placeId": "ChIJAZQmNsxfwokRULFner5q3VQ", "displayName": {"text": "Corona", "languageCode": "en"}, "containment": "WITHIN"}, {"name": "places/ChIJK1kKR2lDwokRBXtcbIvRCUE", "placeId": "ChIJK1kKR2lDwokRBXtcbIvRCUE", "displayName": {"text": "Queens", "languageCode": "en"}, "containment": "WITHIN"}]}, "googleMapsLinks": {"directionsUri": "https://www.google.com/maps/dir//''/data=!4m7!4m6!1m1!4e2!1m2!1m1!1s0x89c25fd0f7b90013:0x4fb7d5717130b380!3e0", "placeUri": "https://maps.google.com/?cid=5744294532941394816", "photosUri": "https://www.google.com/maps/place//data=!4m3!3m2!1s0x89c25fd0f7b90013:0x4fb7d5717130b380!10e5"}}
        con = duckdb.connect(DATABASE_PATH)
        file_location = f"ny-data/csv/locations_place_details.csv"
        with open(file_location, 'a') as f:
            writer = csv.writer(f)
            writer.writerow([
                "contentid",
                "latitude",
                "longitude",
                "viewport",
                "google_maps_uri",
                "google_maps_links",
                "website_uri",
                "rating",
                "user_rating_count",
                "business_status",
                "phone_number",
                "primary_type",
                "current_opening_hours",
                "reviews",
                "photos",
                "good_for_children",
                "accessibility_options",
                "generative_summary",
                "utc_offset_minutes",
                "adr_format_address",
                "icon_mask_base_uri",
                "icon_background_color",
                "display_name",
                "primary_type_display_name",
                "short_formatted_address",
                "pure_service_area_business",
                "address_descriptor"
            ])
        locations_df = con.execute("SELECT contentid, place_details FROM locations WHERE place_details is not null and is_geo_coded = true").fetchdf()
        no_rows = 0
        for index, row in locations_df.iterrows():
            place_details = row['place_details']
            if not place_details:
                continue
            place_details = json.loads(place_details)
            print(f"Place details for contentid {row['contentid']}: {place_details}")
            parsed_place_details = parse_google_places_response(place_details)
            print(f"Parsed place details for contentid {row['contentid']}: {parsed_place_details}")

            contentid = row['contentid']
            if parsed_place_details:
                no_rows += 1
                with open(file_location, 'a') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                    contentid,
                    parsed_place_details.get("latitude", "N/A"),
                    parsed_place_details.get("longitude", "N/A"),
                    parsed_place_details.get("viewport", "N/A"),
                    parsed_place_details.get("google_maps_uri", "N/A"),
                    parsed_place_details.get("google_maps_links", "N/A"),
                    parsed_place_details.get("website_uri", "N/A"),
                    parsed_place_details.get("place_rating", "N/A"),
                    parsed_place_details.get("user_rating_count", "N/A"),
                    parsed_place_details.get("business_status", "N/A"),
                    parsed_place_details.get("phone_number", "N/A"),
                    parsed_place_details.get("primary_type", "N/A"),
                    parsed_place_details.get("current_opening_hours", "N/A"),
                    parsed_place_details.get("reviews", "N/A"),
                    parsed_place_details.get("photos", "N/A"),
                    parsed_place_details.get("good_for_children", "N/A"),
                    parsed_place_details.get("accessibility_options", "N/A"),
                    parsed_place_details.get("generative_summary", "N/A"),
                    parsed_place_details.get("utc_offset_minutes", "N/A"),
                    parsed_place_details.get("adr_format_address", "N/A"),
                    parsed_place_details.get("icon_mask_base_uri", "N/A"),
                    parsed_place_details.get("icon_background_color", "N/A"),
                    parsed_place_details.get("display_name", "N/A"),
                    parsed_place_details.get("primary_type_display_name", "N/A"),
                    parsed_place_details.get("short_formatted_address", "N/A"),
                    parsed_place_details.get("pure_service_area_business", "N/A"),
                    parsed_place_details.get("address_descriptor", "N/A")
                ])
    print(f"Total rows added: {no_rows}")
    if parse_address_components_enabled:
        con = duckdb.connect(DATABASE_PATH)
        file_location = f"ny-data/csv/locations_address_components.csv"
        lacations_df = con.execute("SELECT contentid, place_address_components FROM locations WHERE is_geo_coded = true").fetchdf()
        with open(file_location, 'a') as f:
            writer = csv.writer(f)
            writer.writerow([
                    "contentid",
                    "street_number",
                    "route",
                    "neighborhood",
                    "sublocality_level_1",
                    "administrative_area_level_2",
                    "administrative_area_level_1",
                    "colloquial_area",
                    "sublocality",
                    "natural_feature",
                    "airport",
                    "park",
                    "premise",
                    "subpremise",
                    "landmark",
                    "postal_town",
                    "administrative_area_level_3",
                    "administrative_area_level_4",
                    "administrative_area_level_5",
                    "administrative_area_level_6",
                    "room",
                    "floor",
                    "street_address",
                    "intersection",
                    "bus_station",
                    "train_station",
                    "transit_station",
                    "country",
                    "postal_code",
                    "locality",
                    "point_of_interest",
                    "formatted_address"
                ])
        for index, row in lacations_df.iterrows():
            # save to a csv file against the contentid
            address_components = row['place_address_components']
            address_components = json.loads(address_components)
            print(f"Address components for contentid {row['contentid']}: {address_components}")
            parsed_address = parse_address_components_method(address_components)
            contentid = row['contentid']
            street_number = parsed_address.get("street_number", "N/A")
            route = parsed_address.get("route", "N/A")
            neighborhood = parsed_address.get("neighborhood", "N/A")
            sublocality_level_1 = parsed_address.get("sublocality_level_1", "N/A")
            administrative_area_level_2 = parsed_address.get("administrative_area_level_2", "N/A")
            administrative_area_level_1 = parsed_address.get("administrative_area_level_1", "N/A")
            colloquial_area = parsed_address.get("colloquial_area", "N/A")
            sublocality = parsed_address.get("sublocality", "N/A")
            natural_feature = parsed_address.get("natural_feature", "N/A")
            airport = parsed_address.get("airport", "N/A")
            park = parsed_address.get("park", "N/A")
            premise = parsed_address.get("premise", "N/A")
            subpremise = parsed_address.get("subpremise", "N/A")
            landmark = parsed_address.get("landmark", "N/A")
            postal_town = parsed_address.get("postal_town", "N/A")
            administrative_area_level_3 = parsed_address.get("administrative_area_level_3", "N/A")
            administrative_area_level_4 = parsed_address.get("administrative_area_level_4", "N/A")
            administrative_area_level_5 = parsed_address.get("administrative_area_level_5", "N/A")
            administrative_area_level_6 = parsed_address.get("administrative_area_level_6", "N/A")
            room = parsed_address.get("room", "N/A")
            floor = parsed_address.get("floor", "N/A")
            street_address = parsed_address.get("street_address", "N/A")
            intersection = parsed_address.get("intersection", "N/A")
            bus_station = parsed_address.get("bus_station", "N/A")
            train_station = parsed_address.get("train_station", "N/A")
            transit_station = parsed_address.get("transit_station", "N/A")
            country = parsed_address.get("country", "N/A")
            postal_code = parsed_address.get("postal_code", "N/A")
            locality = parsed_address.get("city", "N/A")
            point_of_interest = parsed_address.get("point_of_interest", "N/A")
            formatted_address = parsed_address.get("formatted_address", "N/A")
            with open(file_location, 'a') as f:
                writer = csv.writer(f)
                writer.writerow([
                    contentid,
                    street_number,
                    route,
                    neighborhood,
                    sublocality_level_1,
                    administrative_area_level_2,
                    administrative_area_level_1,
                    colloquial_area,
                    sublocality,
                    natural_feature,
                    airport,
                    park,
                    premise,
                    subpremise,
                    landmark,
                    postal_town,
                    administrative_area_level_3,
                    administrative_area_level_4,
                    administrative_area_level_5,
                    administrative_area_level_6,
                    room,
                    floor,
                    street_address,
                    intersection,
                    bus_station,
                    train_station,
                    transit_station,
                    country,
                    postal_code,
                    locality,
                    point_of_interest,
                    formatted_address
                ])
    if is_place_details_with_name_and_lat_long_enabled:
        print("Place details with name and lat long is enabled")
        con = duckdb.connect(DATABASE_PATH)
        locations_df = con.execute("SELECT title, latitude, longitude, contentid, place_id FROM locations WHERE is_geo_coded = true").fetchdf()
        print(f"Locations to be geocoded with google places: {len(locations_df)}")
        alter_query = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS alt_place_id TEXT"
        alt_place_details = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS alt_place_details JSON"
        print(alter_query)
        print(alt_place_details)
        con.execute(alt_place_details)
        con.execute(alter_query)
        exists_query = f"ALTER TABLE locations ADD COLUMN is_place_id_same BOOLEAN"
        con.execute(exists_query)
        for index, row in locations_df.iterrows():
            title = row['title']
            latitude = row['latitude']
            longitude = row['longitude']
            contentid = row['contentid']
            placeid = row["place_id"]
            print(f"Getting place_id for {title} at {latitude}, {longitude} with existing place_id: {placeid}...")
            place_id = get_place_id(title, latitude, longitude, contentid)
            if place_id and place_id != placeid:
                query = f"UPDATE locations SET alt_place_id = '{place_id}' WHERE contentid = '{contentid}'"
                print(query)
                con.execute(query)
                # update the contentid with place details of place_id
                place_details = get_place_details(place_id)
                place_details = json.dumps(place_details)
                query = f"UPDATE locations SET alt_place_details = '{place_details}' WHERE contentid = '{contentid}'"
                print(query)
                con.execute(query)
            elif place_id and place_id == placeid:
                print(f"Place_id already exists for {title} at {latitude}, {longitude} with existing place_id: {placeid}...")
                query = f"UPDATE locations SET is_place_id_same = true WHERE contentid = '{contentid}'"
                print(query)
                con.execute(query)
    if open_street_map_enable:
        print("Open street map is enabled")
        con = duckdb.connect(DATABASE_PATH)
        locations_dg = con.execute("SELECT title, latitude, longitude, contentid FROM locations WHERE is_geo_coded = true").fetchdf()
        print(f"Locations to be geocoded with openstreet map: {len(locations_dg)}")
        for index, row in locations_dg.iterrows():
            title = row['title']
            latitude = row['latitude']
            longitude = row['longitude']
            contentid = row['contentid']
            print(f"Getting place details for {title} at {latitude}, {longitude}...")
            response_from_open_street_map = get_place_details_os(title, latitude, longitude)
            print(response_from_open_street_map)
            if response_from_open_street_map.get("error"):
                print(f"Error getting place details for {title} at {latitude}, {longitude}: {response_from_open_street_map.get('error')}")
            elif response_from_open_street_map:
                add_column_1 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS os_city TEXT"
                add_column_2 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS os_state TEXT"
                add_column_3 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS os_country TEXT"
                add_column_4 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS os_postcode TEXT"
                add_column_5 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS os_neighborhood TEXT"
                add_column_7 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS os_formatted_address TEXT"
                add_column_8 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS os_sublocality TEXT"
                add_column_10 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS os_plus_code TEXT"
                add_column_11 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS os_complete_result JSON"
                complete_result_json = json.dumps(response_from_open_street_map)
                query = f"UPDATE locations SET formatted_address = '{response_from_open_street_map['display_name']}', city = '{response_from_open_street_map['type']}', state = '{response_from_open_street_map['address'].get('state', 'N/A')}', country = '{response_from_open_street_map['address'].get('country', 'N/A')}', postcode = '{response_from_open_street_map['address'].get('postcode', 'N/A')}', neighborhood = '{response_from_open_street_map['address'].get('neighbourhood', 'N/A')}', is_geo_coded = true, sublocality = '{response_from_open_street_map['address'].get('suburb', 'N/A')}', place_id = '{response_from_open_street_map['place_id']}', plus_code = 'N/A' WHERE contentid = '{contentid}'"
                complete_result_query = f"UPDATE locations SET complete_result = '{complete_result_json}' WHERE contentid = '{contentid}'"
                print(query)
                con.execute(query)
                print(complete_result_query)
                con.execute(complete_result_query)
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
        places_details_df = con.execute("SELECT contentid, place_details FROM locations WHERE place_details is not null and is_geo_coded = true").fetchdf()
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
            add_column_9 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS place_google_maps_links TEXT"
            add_column_10 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS place_website_uri TEXT"
            add_column_11 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS place_rating TEXT"
            add_column_12 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS place_user_rating_count TEXT"
            add_column_13 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS place_business_status TEXT"
            add_column_14 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS place_phone_number TEXT"
            add_column_15 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS place_primary_type TEXT"
            add_column_16 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS place_current_opening_hours JSON"
            add_column_17 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS place_reviews JSON"
            add_column_18 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS place_photos JSON"
            add_column_19 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS place_good_for_children TEXT"
            add_column_20 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS place_accessibility_options JSON"
            add_column_21 = f"ALTER TABLE locations ADD COLUMN IF NOT EXISTS place_generative_summary JSON"
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
            con.execute(add_column_12)
            con.execute(add_column_13)
            con.execute(add_column_14)
            con.execute(add_column_15)
            con.execute(add_column_16)
            con.execute(add_column_17)
            con.execute(add_column_18)
            con.execute(add_column_19)
            con.execute(add_column_20)
            con.execute(add_column_21)
            query = """
            INSERT INTO locations (
                contentid, 
                place_name, 
                place_formatted_address, 
                place_latitude, 
                place_longitude, 
                place_types, 
                place_address_components, 
                place_viewport, 
                place_google_maps_uri, 
                place_google_maps_links, 
                place_website_uri, 
                place_rating, 
                place_user_rating_count, 
                place_business_status, 
                place_phone_number, 
                place_primary_type, 
                place_current_opening_hours, 
                place_reviews, 
                place_photos, 
                place_good_for_children, 
                place_accessibility_options, 
                place_generative_summary
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (contentid) DO UPDATE SET 
                place_name = EXCLUDED.place_name, 
                place_formatted_address = EXCLUDED.place_formatted_address, 
                place_latitude = EXCLUDED.place_latitude, 
                place_longitude = EXCLUDED.place_longitude, 
                place_types = EXCLUDED.place_types, 
                place_address_components = EXCLUDED.place_address_components, 
                place_viewport = EXCLUDED.place_viewport, 
                place_google_maps_uri = EXCLUDED.place_google_maps_uri, 
                place_google_maps_links = EXCLUDED.place_google_maps_links, 
                place_website_uri = EXCLUDED.place_website_uri, 
                place_rating = EXCLUDED.place_rating, 
                place_user_rating_count = EXCLUDED.place_user_rating_count, 
                place_business_status = EXCLUDED.place_business_status, 
                place_phone_number = EXCLUDED.place_phone_number, 
                place_primary_type = EXCLUDED.place_primary_type, 
                place_current_opening_hours = EXCLUDED.place_current_opening_hours, 
                place_reviews = EXCLUDED.place_reviews, 
                place_photos = EXCLUDED.place_photos, 
                place_good_for_children = EXCLUDED.place_good_for_children, 
                place_accessibility_options = EXCLUDED.place_accessibility_options, 
                place_generative_summary = EXCLUDED.place_generative_summary
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
                json.dumps(places_parsed_values['google_maps_links']), 
                places_parsed_values['place_website_uri'], 
                places_parsed_values['place_rating'], 
                places_parsed_values['user_rating_count'], 
                places_parsed_values['business_status'], 
                places_parsed_values['phone_number'], 
                places_parsed_values['primary_type'], 
                json.dumps(places_parsed_values['current_opening_hours']), 
                json.dumps(places_parsed_values['reviews']), 
                json.dumps(places_parsed_values['photos']), 
                places_parsed_values['good_for_children'], 
                json.dumps(places_parsed_values['accessibility_options']), 
                json.dumps(places_parsed_values['generative_summary']), 
                contentid
            ))
        con.close()
    if parse_place_details_to_locations_details_table:
        con = duckdb.connect(DATABASE_PATH)
        places_details_df = con.execute("SELECT contentid, place_id, place_details FROM locations WHERE place_details is not null and is_geo_coded = true").fetchdf()
        create_table = """
        DROP TABLE IF EXISTS locations_details;
        CREATE TABLE IF NOT EXISTS locations_details (
            contentid UUID PRIMARY KEY
        )
        """
        con.execute(create_table)
        add_column_1 = f"ALTER TABLE locations_details ADD COLUMN IF NOT EXISTS place_name TEXT"
        add_column_2 = f"ALTER TABLE locations_details ADD COLUMN IF NOT EXISTS place_formatted_address TEXT"
        add_column_3 = f"ALTER TABLE locations_details ADD COLUMN IF NOT EXISTS place_latitude TEXT"
        add_column_4 = f"ALTER TABLE locations_details ADD COLUMN IF NOT EXISTS place_longitude TEXT"
        add_column_5 = f"ALTER TABLE locations_details ADD COLUMN IF NOT EXISTS place_types TEXT"
        add_column_6 = f"ALTER TABLE locations_details ADD COLUMN IF NOT EXISTS place_address_components TEXT"
        add_column_7 = f"ALTER TABLE locations_details ADD COLUMN IF NOT EXISTS place_viewport TEXT"
        add_column_8 = f"ALTER TABLE locations_details ADD COLUMN IF NOT EXISTS place_google_maps_uri TEXT"
        add_column_9 = f"ALTER TABLE locations_details ADD COLUMN IF NOT EXISTS place_google_maps_links TEXT"
        add_column_10 = f"ALTER TABLE locations_details ADD COLUMN IF NOT EXISTS place_website_uri TEXT"
        add_column_11 = f"ALTER TABLE locations_details ADD COLUMN IF NOT EXISTS place_rating TEXT"
        add_column_12 = f"ALTER TABLE locations_details ADD COLUMN IF NOT EXISTS place_user_rating_count TEXT"
        add_column_13 = f"ALTER TABLE locations_details ADD COLUMN IF NOT EXISTS place_business_status TEXT"
        add_column_14 = f"ALTER TABLE locations_details ADD COLUMN IF NOT EXISTS place_phone_number TEXT"
        add_column_15 = f"ALTER TABLE locations_details ADD COLUMN IF NOT EXISTS place_primary_type TEXT"
        add_column_16 = f"ALTER TABLE locations_details ADD COLUMN IF NOT EXISTS place_current_opening_hours JSON"
        add_column_17 = f"ALTER TABLE locations_details ADD COLUMN IF NOT EXISTS place_reviews JSON"
        add_column_18 = f"ALTER TABLE locations_details ADD COLUMN IF NOT EXISTS place_photos JSON"
        add_column_19 = f"ALTER TABLE locations_details ADD COLUMN IF NOT EXISTS place_good_for_children TEXT"
        add_column_20 = f"ALTER TABLE locations_details ADD COLUMN IF NOT EXISTS place_accessibility_options JSON"
        add_column_21 = f"ALTER TABLE locations_details ADD COLUMN IF NOT EXISTS place_generative_summary JSON"
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
        con.execute(add_column_12)
        con.execute(add_column_13)
        con.execute(add_column_14)
        con.execute(add_column_15)
        con.execute(add_column_16)
        con.execute(add_column_17)
        con.execute(add_column_18)
        con.execute(add_column_19)
        con.execute(add_column_20)
        con.execute(add_column_21)
        for index, row in places_details_df.iterrows():
            place_details = row['place_details']
            places_parsed_values = parse_google_places_response(place_details)
            contentid = row['contentid']
            query = """
            INSERT INTO locations_details (
                contentid, 
                place_name, 
                place_formatted_address, 
                place_latitude, 
                place_longitude, 
                place_types, 
                place_address_components, 
                place_viewport, 
                place_google_maps_uri, 
                place_google_maps_links, 
                place_website_uri, 
                place_rating, 
                place_user_rating_count, 
                place_business_status, 
                place_phone_number, 
                place_primary_type, 
                place_current_opening_hours, 
                place_reviews, 
                place_photos, 
                place_good_for_children, 
                place_accessibility_options, 
                place_generative_summary
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (contentid) DO UPDATE SET 
                place_name = EXCLUDED.place_name, 
                place_formatted_address = EXCLUDED.place_formatted_address, 
                place_latitude = EXCLUDED.place_latitude, 
                place_longitude = EXCLUDED.place_longitude, 
                place_types = EXCLUDED.place_types, 
                place_address_components = EXCLUDED.place_address_components, 
                place_viewport = EXCLUDED.place_viewport, 
                place_google_maps_uri = EXCLUDED.place_google_maps_uri, 
                place_google_maps_links = EXCLUDED.place_google_maps_links, 
                place_website_uri = EXCLUDED.place_website_uri, 
                place_rating = EXCLUDED.place_rating, 
                place_user_rating_count = EXCLUDED.place_user_rating_count, 
                place_business_status = EXCLUDED.place_business_status, 
                place_phone_number = EXCLUDED.place_phone_number, 
                place_primary_type = EXCLUDED.place_primary_type, 
                place_current_opening_hours = EXCLUDED.place_current_opening_hours, 
                place_reviews = EXCLUDED.place_reviews, 
                place_photos = EXCLUDED.place_photos, 
                place_good_for_children = EXCLUDED.place_good_for_children, 
                place_accessibility_options = EXCLUDED.place_accessibility_options, 
                place_generative_summary = EXCLUDED.place_generative_summary
            """
            con.execute(query, (
                contentid,
                places_parsed_values['place_name'], 
                places_parsed_values['place_formatted_address'], 
                places_parsed_values['place_latitude'], 
                places_parsed_values['place_longitude'], 
                json.dumps(places_parsed_values['place_types']), 
                json.dumps(places_parsed_values['place_address_components']), 
                json.dumps(places_parsed_values['place_viewport']), 
                places_parsed_values['place_google_maps_uri'], 
                json.dumps(places_parsed_values['google_maps_links']), 
                places_parsed_values['place_website_uri'], 
                places_parsed_values['place_rating'], 
                places_parsed_values['user_rating_count'], 
                places_parsed_values['business_status'], 
                places_parsed_values['phone_number'], 
                places_parsed_values['primary_type'], 
                json.dumps(places_parsed_values['current_opening_hours']), 
                json.dumps(places_parsed_values['reviews']), 
                json.dumps(places_parsed_values['photos']), 
                places_parsed_values['good_for_children'], 
                json.dumps(places_parsed_values['accessibility_options']), 
                json.dumps(places_parsed_values['generative_summary'])
            ))
        con.close()