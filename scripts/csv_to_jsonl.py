import csv
import json

# Define the input and output file paths
input_csv_file = '/home/sreeram/Projects/jaunt/ny-data/csv/nyc_filtered_locations_resolution_8.csv'
output_jsonl_file = '/home/sreeram/Projects/jaunt/ny-data/csv/nyc_filtered_locations_resolution_8.jsonl'

# Read the CSV file and convert to JSONL
with open(input_csv_file, mode='r', encoding='utf-8') as csv_file, open(output_jsonl_file, mode='w', encoding='utf-8') as jsonl_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        jsonl_file.write(json.dumps(row) + '\n')

print(f"Conversion complete. JSONL file saved to {output_jsonl_file}")