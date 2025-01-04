package main

import (
	"jaunt/scripts/dedup"
	"jaunt/scripts/seed"
	"log"
)

func main() {

	// Read the JSON file
	// locations, err := scripts.ReadJSON("data/locations.json")
	// if err != nil {
	// 	log.Fatalf("Error reading JSON: %s", err)
	// }

	// // Print the locations to verify the data
	// for _, location := range locations {
	// 	fmt.Printf("Title: %s\nSubtitle: %s\nDescription: %s\nLatitude: %f\nLongitude: %f\n\n",
	// 		location.Title, location.Subtitle, location.Description, location.Latitude, location.Longitude)
	// }
	err := seed.CreateLocationsTableInDuckDB("data/locations.db")
	if err != nil {
		log.Fatalf("Error creating locations table in DuckDB: %s", err)
	}
	err = seed.InsertJSONIntoLocationsTableInDuckDB("data/locations.db", "data/t_content_202501021703_new.jsonl")
	if err != nil {
		log.Printf("Error inserting JSON into DuckDB: %s", err)
	}
	err = seed.InsertJSONIntoLocationsTableInDuckDB("data/locations.db", "data/t_content_rawdata_202412011408.jsonl")
	if err != nil {
		log.Printf("Error inserting JSON into DuckDB: %s", err)
	}
	err = seed.UpdateLocationsWithH3Index("data/locations.db")
	if err != nil {
		log.Fatalf("Error updating locations with H3 index: %s", err)
	}
	err = seed.CompareLocations("data/locations.db")
	if err != nil {
		log.Printf("Error comparing locations: %s", err)
	}
	err = seed.CompareNewlyInsertedLocationsWithRestOfLocations("data/locations.db")
	if err != nil {
		log.Printf("Error inserting into location comparison for the newly inserted locations in location table: %s", err)
	}
	err = dedup.ArchiveAndRemoveDuplicateLocations("data/locations.db")
	if err != nil {
		log.Fatalf("Error archiving and removing duplicate locations: %s", err)
	}
	// err := scripts.StoreJSONInDuckDB("data/locations.db", "data/locations.json")
	// if err != nil {
	// 	log.Fatalf("Error storing JSON in DuckDB: %s", err)
	// }

	// log.Println("JSON data successfully stored in DuckDB")

	//store h3 index in duckdb
	// err := scripts.StoreH3IndexInDuckDB("data/locations.db")
	// if err != nil {
	// 	log.Fatalf("Error storing H3 index in DuckDB: %s", err)
	// }
	// log.Println("H3 index successfully stored in DuckDB")
	// scripts.FindDuplicateLocations("data/locations.db")
	// err := scripts.StoreH3IndexInDuckDB("data/locations.db", 8)
	// if err != nil {
	// 	log.Fatalf("Error storing H3 index in DuckDB: %s", err)
	// }

	// err := scripts.ExportComparisonResultsAsJSON("data/locations.db")
	// if err != nil {
	// 	log.Fatalf("Error exporting comparison results as JSON: %s", err)
	// }
	// err := scripts.ExportComparisonResultsWithThresholdAsJSON("data/locations.db", 0.85)
	// if err != nil {
	// 	log.Fatalf("Error exporting comparison results as JSON: %s", err)
	// }
	// err := scripts.ConvertJSONToGeoJSON("data/output.jsonl")
	// if err != nil {
	// 	log.Fatalf("Error converting JSON to GeoJSON: %s", err)
	// }
	// err := scripts.ExportJSONFromDuckDB("data/locations.db")
	// if err != nil {
	// 	log.Fatalf("Error exporting GeoJSON of duplicates: %s", err)
	// }
	// scripts.FindDuplicatesAndExportJSON("data/locations.db")
	// if err != nil {
	// 	log.Fatalf("Error finding duplicates and exporting JSON: %s", err)
	// }
	// err := scripts.CompareLocations("data/locations.db")
	// if err != nil {
	// 	log.Fatalf("Error comparing locations: %s", err)
	// }
	// err := scripts.AddWordCountColumnToLocationsTable("data/locations.db")
	// if err != nil {
	// 	log.Fatalf("Error adding word count column to locations table: %s", err)
	// }
	// err := scripts.CompareLocationsWithHaversineDistance("data/locations.db")
	// if err != nil {
	// 	log.Fatalf("Error comparing locations with Haversine distance: %s", err)
	// }
	// scripts.CompareLocationsInsertSimilarityScore("data/locations.db")
	// if err != nil {
	// 	log.Fatalf("Error comparing locations and inserting similarity score: %s", err)
	// }
	// scripts.ExportAllLocationsAsJSONFromDuckDB("data/locations.db")
	// if err != nil {
	// 	log.Fatalf("Error exporting all locations as JSON: %s", err)
	// }
	// err = scripts.ConvertAllLocationsJSONToGeoJSON("data/output_all.jsonl")
	// if err != nil {
	// 	log.Fatalf("Error converting JSON to GeoJSON: %s", err)
	// }
	// err := scripts.ListAllTablesInPostgresDB()
	// if err != nil {
	// 	log.Fatalf("Error listing all tables in Postgres DB: %s", err)
	// }

}
