# FILEPATH: Untitled-19.py
import duckdb
import json

# 1. Create the base location table in DuckDB
def create_locations_table_in_duckdb(db_file_path):
	# Connect to the DuckDB database
	conn = duckdb.connect(database='data/locations.db')
	cursor = conn.cursor()

	# Create the locations table
	query = '''
		CREATE TABLE locations (
			contentid TEXT,
			source_id TEXT,
			title TEXT,
			subtitle TEXT,
			breadcrumb TEXT,
			category TEXT,
			subcategory TEXT,
			description TEXT,
			short_description TEXT,
			keywords TEXT,
			source_url TEXT,
			address TEXT,
			address_detailed TEXT,
			zipcode TEXT,
			phone TEXT,
			latitude REAL,
			longitude REAL,
			running_hours TEXT,
			featured_image_url TEXT,
			image_urls TEXT,
			additional_info TEXT,
			primary_data INTEGER,
			city TEXT,
			country TEXT,
			state TEXT
		);
	'''

	cursor.execute(query)
	conn.commit()
	conn.close()

# 2. Insert into the locations table in DuckDB from a t_content raw data JSON file in data/ folder
def insert_json_into_locations_table_in_duckdb(db_file_path, json_file_path):
	# Connect to the DuckDB database
	conn = duckdb.connect(database='data/locations.db')
	# cursor = conn.cursor()

	# Read the JSON file
	with open(json_file_path) as file:
		json_data = json.load(file)

	# Insert JSON data into the locations table
	for data in json_data:
		query = '''
			INSERT INTO locations (
				contentid, source_id, title, subtitle, breadcrumb, category, subcategory,
				description, short_description, keywords, source_url, address,
				address_detailed, zipcode, phone, latitude, longitude, running_hours,
				featured_image_url, image_urls, additional_info, primary_data, city, country, state
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		'''
		values = (
			data['contentid'], data['source_id'], data['title'], data['subtitle'], data['breadcrumb'],
			data['category'], data['subcategory'], data['description'], data['short_description'],
			data['keywords'], data['source_url'], data['address'], data['address_detailed'],
			data['zipcode'], data['phone'], data['latitude'], data['longitude'], data['running_hours'],
			data['featured_image_url'], ','.join(data['image_urls']), json.dumps(data['additional_info']),
			int(data['primary_data']), data['city'], data['country'], data['state']
		)
		conn.execute(query, values)

	# conn.commit()
	conn.close()

# 3. Describe the schema of the locations table
def describe_locations_table_in_duckdb(db_file_path):
	# Connect to the DuckDB database
	conn = sqlite3.connect(db_file_path)
	cursor = conn.cursor()

	# Describe the locations table
	query = '''
		PRAGMA table_info(locations);
	'''

	cursor.execute(query)
	columns = cursor.fetchall()

	# Print the column names and types
	for column in columns:
		column_name = column[1]
		column_type = column[2]
		print(f"{column_name}: {column_type}")

	conn.close()

# Example usage
db_file_path = "data/locations.db"
json_file_path = "data/t_content_202501021703_new.jsonl"

# create_locations_table_in_duckdb(db_file_path)
insert_json_into_locations_table_in_duckdb(db_file_path, json_file_path)
# describe_locations_table_in_duckdb(db_file_path)
