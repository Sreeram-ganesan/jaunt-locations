import requests
import time
import json
from dotenv import load_dotenv
import duckdb
import time
import geopy.distance
load_dotenv()
import os

# Your Google Places API Key
API_KEY = os.getenv("GM_SAVI")

# List of neighborhoods with their approximate central coordinates
NEIGHBORHOODS = {
    # "Harlem, Manhattan": {"lat": 40.811550, "lng": -73.946477},
    # "SoHo, Manhattan": {"lat": 40.723301, "lng": -74.002988},
    # "Williamsburg, Brooklyn": {"lat": 40.708116, "lng": -73.957070},
    # "Astoria, Queens": {"lat": 40.764357, "lng": -73.923462},
    # "Riverdale, Bronx": {"lat": 40.890834, "lng": -73.912585},
    # "St. George, Staten Island": {"lat": 40.643748, "lng": -74.073643},
    # "Central Park, Manhattan": {"lat": 40.785091, "lng": -73.968285},
    # "Coney Island, Brooklyn": {"lat": 40.574926, "lng": -73.985941},
    # "Flushing Meadows-Corona Park, Queens": {"lat": 40.745957, "lng": -73.844859},
    # "Pelham Bay Park, Bronx": {"lat": 40.850635, "lng": -73.805707},
    # "South Beach, Staten Island": {"lat": 40.580247, "lng": -74.079553},
    # "Greenwich Village, Manhattan": {"lat": 40.733572, "lng": -74.002742},
    # "Upper West Side, Manhattan": {"lat": 40.787011, "lng": -73.975368},
    # "Upper East Side, Manhattan": {"lat": 40.773565, "lng": -73.956555},
    # "Chelsea, Manhattan": {"lat": 40.746500, "lng": -74.001374},
    # "Financial District, Manhattan": {"lat": 40.707491, "lng": -74.011276},
    # "DUMBO, Brooklyn": {"lat": 40.703316, "lng": -73.989747},
    # "Park Slope, Brooklyn": {"lat": 40.672073, "lng": -73.978160},
    # "Long Island City, Queens": {"lat": 40.744679, "lng": -73.948542},
    # "Jackson Heights, Queens": {"lat": 40.755682, "lng": -73.883070},
    # "Bronx Zoo, Bronx": {"lat": 40.850593, "lng": -73.876998},
    # "Staten Island Greenbelt, Staten Island": {"lat": 40.588437, "lng": -74.141506},
    "Midtown, Manhattan": {"lat": 40.754932, "lng": -73.984016},
    "Hell's Kitchen, Manhattan": {"lat": 40.763795, "lng": -73.991441},
    "Koreatown, Manhattan": {"lat": 40.747756, "lng": -73.986982},
    "Lower East Side, Manhattan": {"lat": 40.715033, "lng": -73.984272},
    "East Village, Manhattan": {"lat": 40.726477, "lng": -73.981533},
    "Tribeca, Manhattan": {"lat": 40.716269, "lng": -74.008632},
    "Bushwick, Brooklyn": {"lat": 40.694428, "lng": -73.921286},
    "Bedford-Stuyvesant, Brooklyn": {"lat": 40.687218, "lng": -73.941773},
    "Crown Heights, Brooklyn": {"lat": 40.668103, "lng": -73.944799},
    "Sunset Park, Brooklyn": {"lat": 40.645532, "lng": -74.012385},
    "Flatbush, Brooklyn": {"lat": 40.640923, "lng": -73.963852},
    "Jamaica, Queens": {"lat": 40.702762, "lng": -73.789702},
    "Forest Hills, Queens": {"lat": 40.718157, "lng": -73.844231},
    "Woodside, Queens": {"lat": 40.745840, "lng": -73.906759},
    "Fordham, Bronx": {"lat": 40.862106, "lng": -73.894794},
    "Mott Haven, Bronx": {"lat": 40.809144, "lng": -73.922342},
    "Morris Park, Bronx": {"lat": 40.854641, "lng": -73.860980},
    "Throggs Neck, Bronx": {"lat": 40.818380, "lng": -73.819549},
    "Tottenville, Staten Island": {"lat": 40.512195, "lng": -74.251479},
    "Great Kills, Staten Island": {"lat": 40.553991, "lng": -74.151918},
    "New Dorp, Staten Island": {"lat": 40.573243, "lng": -74.117055}
}
AUTOMOTIVE_TYPE = [
    "car_dealer", "car_rental", "car_repair", "car_wash", "electric_vehicle_charging_station",
    "gas_station", "parking", "rest_stop"
]

BUSINESS_TYPE = [
    "corporate_office", "farm", "ranch"
]

CULTURE_TYPE = [
    "art_gallery", "art_studio", "auditorium", "cultural_landmark", "historical_place",
    "monument", "museum", "performing_arts_theater", "sculpture"
]

EDUCATION_TYPE = [
    "library", "preschool", "primary_school", "secondary_school", "university"
]

ENTERTAINMENT_AND_RECREATION_TYPE = [
    "adventure_sports_center", "amphitheatre", "amusement_center", "amusement_park", "aquarium",
    "banquet_hall", "barbecue_area", "botanical_garden", "bowling_alley", "casino", "childrens_camp",
    "comedy_club", "community_center", "concert_hall", "convention_center", "cultural_center",
    "cycling_park", "dance_hall", "dog_park", "event_venue", "ferris_wheel", "garden", "hiking_area",
    "historical_landmark", "internet_cafe", "karaoke", "marina", "movie_rental", "movie_theater",
    "national_park", "night_club", "observation_deck", "off_roading_area", "opera_house", "park",
    "philharmonic_hall", "picnic_ground", "planetarium", "plaza", "roller_coaster", "skateboard_park",
    "state_park", "tourist_attraction", "video_arcade", "visitor_center", "water_park", "wedding_venue",
    "wildlife_park", "wildlife_refuge", "zoo"
]

FACILITIES_TYPE = [
    "public_bath", "public_bathroom", "stable"
]

FINANCE_TYPE = [
    "accounting", "atm", "bank"
]

FOOD_AND_DRINK_TYPE = [
    "acai_shop", "afghani_restaurant", "african_restaurant", "american_restaurant", "asian_restaurant",
    "bagel_shop", "bakery", "bar", "bar_and_grill", "barbecue_restaurant", "brazilian_restaurant",
    "breakfast_restaurant", "brunch_restaurant", "buffet_restaurant", "cafe", "cafeteria", "candy_store",
    "cat_cafe", "chinese_restaurant", "chocolate_factory", "chocolate_shop", "coffee_shop", "confectionery",
    "deli", "dessert_restaurant", "dessert_shop", "diner", "dog_cafe", "donut_shop", "fast_food_restaurant",
    "fine_dining_restaurant", "food_court", "french_restaurant", "greek_restaurant", "hamburger_restaurant",
    "ice_cream_shop", "indian_restaurant", "indonesian_restaurant", "italian_restaurant", "japanese_restaurant",
    "juice_shop", "korean_restaurant", "lebanese_restaurant", "meal_delivery", "meal_takeaway",
    "mediterranean_restaurant", "mexican_restaurant", "middle_eastern_restaurant", "pizza_restaurant",
    "pub", "ramen_restaurant", "restaurant", "sandwich_shop", "seafood_restaurant", "spanish_restaurant",
    "steak_house", "sushi_restaurant", "tea_house", "thai_restaurant", "turkish_restaurant", "vegan_restaurant",
    "vegetarian_restaurant", "vietnamese_restaurant", "wine_bar"
]

GEOGRAPHICAL_AREAS_TYPE = [
    "administrative_area_level_1", "administrative_area_level_2", "country", "locality", "postal_code",
    "school_district"
]

GOVERNMENT_TYPE = [
    "city_hall", "courthouse", "embassy", "fire_station", "government_office", "local_government_office",
    "neighborhood_police_station", "police", "post_office"
]

HEALTH_AND_WELLNESS_TYPE = [
    "chiropractor", "dental_clinic", "dentist", "doctor", "drugstore", "hospital", "massage", "medical_lab",
    "pharmacy", "physiotherapist", "sauna", "skin_care_clinic", "spa", "tanning_studio", "wellness_center",
    "yoga_studio"
]

HOUSING_TYPE = [
    "apartment_building", "apartment_complex", "condominium_complex", "housing_complex"
]

LODGING_TYPE = [
    "bed_and_breakfast", "budget_japanese_inn", "campground", "camping_cabin", "cottage", "extended_stay_hotel",
    "farmstay", "guest_house", "hostel", "hotel", "inn", "japanese_inn", "lodging", "mobile_home_park", "motel",
    "private_guest_room", "resort_hotel", "rv_park"
]

NATURAL_FEATURES_TYPE = [
    "beach"
]

PLACES_OF_WORSHIP_TYPE = [
    "church", "hindu_temple", "mosque", "synagogue"
]

SERVICES_TYPE = [
    "astrologer", "barber_shop", "beautician", "beauty_salon", "body_art_service", "catering_service", "cemetery",
    "child_care_agency", "consultant", "courier_service", "electrician", "florist", "food_delivery", "foot_care",
    "funeral_home", "hair_care", "hair_salon", "insurance_agency", "laundry", "lawyer", "locksmith", "makeup_artist",
    "moving_company", "nail_salon", "painter", "plumber", "psychic", "real_estate_agency", "roofing_contractor",
    "storage", "summer_camp_organizer", "tailor", "telecommunications_service_provider", "tour_agency",
    "tourist_information_center", "travel_agency", "veterinary_care"
]

SHOPPING_TYPE = [
    "asian_grocery_store", "auto_parts_store", "bicycle_store", "book_store", "butcher_shop", "cell_phone_store",
    "clothing_store", "convenience_store", "department_store", "discount_store", "electronics_store", "food_store",
    "furniture_store", "gift_shop", "grocery_store", "hardware_store", "home_goods_store", "home_improvement_store",
    "jewelry_store", "liquor_store", "market", "pet_store", "shoe_store", "shopping_mall", "sporting_goods_store",
    "store", "supermarket", "warehouse_store", "wholesaler"
]

SPORTS_TYPE = [
    "arena", "athletic_field", "fishing_charter", "fishing_pond", "fitness_center", "golf_course", "gym",
    "ice_skating_rink", "playground", "ski_resort", "sports_activity_location", "sports_club", "sports_coaching",
    "sports_complex", "stadium", "swimming_pool"
]

TRANSPORTATION_TYPE = [
    "airport", "airstrip", "bus_station", "bus_stop", "ferry_terminal", "heliport", "international_airport",
    "light_rail_station", "park_and_ride", "subway_station", "taxi_stand", "train_station", "transit_depot",
    "transit_station", "truck_stop"
]

PLACE_TYPES = ["tourist_attraction", "museum", "park", "point_of_interest"]
# HISTORIC_PLACES = ["historic_site", "landmark", "monument", "memorial", "cemetery", "archaeological_site", "ruin"]
# ART_PLACES = ["art gallery", "artwork", "artist", "art center", "art museum", "artschool", "art studio", "sculpture"]
# GARDEN_PLACES_ESSENTIAL = ["park and garden", "botanical garden", "aquarium", "wildlife"]
# ADVENTURE_PLACES = ["amusement_park", "bowling_alley", "casino", "night_club"]
# SHOPPING_PLACES = ["shopping mall", "shopping center", "shopping district", "shopping street", "shopping plaza", "shopping village", "shopping complex", "shopping arcade", "shopping gallery", "shopping square"]

# Google Places API URL
PLACES_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"

def get_places_for_location(lat, lng, radius=1000):
    """
    Fetches places of interest around a specific location using Google Places API.
    
    :param lat: Latitude of the location
    :param lng: Longitude of the location
    :param radius: Search radius in meters
    :return: List of places with details
    """
    all_places = []
    place_types = []
    con = duckdb.connect('data/locations.db')
    # place_types.append(HISTORIC_PLACES)
    # place_types.append(ART_PLACES)
    # place_types.append(GARDEN_PLACES_ESSENTIAL)
    # place_types.append(ADVENTURE_PLACES)
    # place_types.append(PLACE_TYPES)
    place_types.append(AUTOMOTIVE_TYPE)
    place_types.append(BUSINESS_TYPE)
    place_types.append(CULTURE_TYPE)
    place_types.append(EDUCATION_TYPE)
    place_types.append(ENTERTAINMENT_AND_RECREATION_TYPE)
    place_types.append(FACILITIES_TYPE)
    place_types.append(FINANCE_TYPE)
    place_types.append(FOOD_AND_DRINK_TYPE)
    place_types.append(GEOGRAPHICAL_AREAS_TYPE)
    place_types.append(GOVERNMENT_TYPE)
    place_types.append(HEALTH_AND_WELLNESS_TYPE)
    place_types.append(HOUSING_TYPE)
    place_types.append(LODGING_TYPE)
    place_types.append(NATURAL_FEATURES_TYPE)
    place_types.append(PLACES_OF_WORSHIP_TYPE)
    place_types.append(SERVICES_TYPE)
    place_types.append(SHOPPING_TYPE)
    place_types.append(SPORTS_TYPE)
    place_types.append(TRANSPORTATION_TYPE)
    for place_type in place_types:
        params = {
            "location": f"{lat},{lng}",
            "radius": radius,
            "type": place_type,
            "key": API_KEY
        }
        print(f"Fetching places of type '{place_type}' around {lat}, {lng}...")
        while True:
            response = requests.get(PLACES_URL, params=params)
            data = response.json()
            print(f"Response from google for {lat}, {lng} is of len - {len(data)} and response - {data}")
            if "results" in data:
                for place in data["results"]:
                    place_info = {
                        "name": place.get("name"),
                        "address": place.get("vicinity"),
                        "latitude": place["geometry"]["location"]["lat"],
                        "longitude": place["geometry"]["location"]["lng"],
                        "rating": place.get("rating"),
                        "user_ratings_total": place.get("user_ratings_total"),
                        "types": place.get("types")
                    }
                    print("Inserting into db")
                    con.execute("ALTER TABLE google_poi ADD COLUMN IF NOT EXISTS place_id STRING")
                    con.execute("INSERT INTO google_poi VALUES (?, ?, ?, ?)",
                                    (json.dumps(place), lat, lng, place.get("place_id")))
                    all_places.append(place_info)

            # Handle pagination
            next_page_token = data.get("next_page_token")
            if not next_page_token:
                break  # No more pages, exit loop

            print("Fetching next page... with token", next_page_token)
            data_count = con.execute("select count(*) from google_poi").fetchall()
            count_places = [row[0] for row in data_count]
            unique_data_count = con.execute("select count(distinct place_id) from google_poi").fetchall()
            unique_places = [row[0] for row in unique_data_count]
            print(f"So far inserted into google_poi - {count_places} and unique - {unique_places}")
            time.sleep(2)  # Delay to avoid quota issues
            params["pagetoken"] = next_page_token
    con.close()
    return all_places

def get_all_places():
    """
    Fetches places of interest for all specified neighborhoods in New York City.
    
    :return: Dictionary containing results for all neighborhoods.
    """
    results = {}

    for neighborhood, coords in NEIGHBORHOODS.items():
        print(f"Fetching places for {neighborhood}...")
        neighborhood_places = get_places_for_location(coords["lat"], coords["lng"])
        
        results[neighborhood] = neighborhood_places
        
        # If fewer results than expected, perform a grid search
        if len(results[neighborhood]) < 50:  # Threshold can be adjusted
            print(f"Performing grid search for {neighborhood} due to low initial results of {len(neighborhood_places)} places")
            grid_places = perform_grid_search(coords["lat"], coords["lng"])
            neighborhood_places.extend(grid_places)
        
        results[neighborhood] = neighborhood_places

    return results

def perform_grid_search(lat, lng, grid_size=500, area_size=2000):
    """
    Performs a grid search around a central point to cover a larger area.
    
    :param lat: Central latitude
    :param lng: Central longitude
    :param grid_size: Size of each grid cell in meters
    :param area_size: Total area to cover in meters
    :return: List of places found in the grid
    """
    from geopy.distance import geodesic

    def move_point(lat, lng, dx, dy):
        """Move a point by dx meters east and dy meters north."""
        origin = geodesic(meters=dy).destination((lat, lng), 0)  # North
        destination = geodesic(meters=dx).destination((origin.latitude, origin.longitude), 90)  # East
        return destination.latitude, destination.longitude

    places = []
    steps = int(area_size / grid_size)

    for i in range(-steps, steps + 1):
        for j in range(-steps, steps + 1):
            new_lat, new_lng = move_point(lat, lng, i * grid_size, j * grid_size)
            place = get_places_for_location(new_lat, new_lng, radius=grid_size / 2)
            places.extend(place)
            print(f"Found {len(places)} places so far...")
            if len(places) >= 100:
                print(f"Found {len(places)} places, stopping grid search...")
                break
            time.sleep(1)  # To respect API rate limits

    return places

# Run the function
if __name__ == "__main__":
    places_data = get_all_places()
    # Save to JSON file
    with open("nyc/csv/nyc_places_of_interest.json", "w") as f:
        json.dump(places_data, f, indent=4)
    print("Data saved to nyc_places_of_interest.json")