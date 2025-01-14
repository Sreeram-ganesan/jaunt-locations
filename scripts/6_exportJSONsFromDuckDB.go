package scripts

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

// 1. Export all locations as JSON lines file from DuckDB and store it in data/output_all.jsonl
func ExportAllLocationsAsJSONFromDuckDB(dbFilePath string) error {
	// Open the DuckDB database
	db, err := sql.Open("duckdb", dbFilePath)
	if err != nil {
		return fmt.Errorf("failed to open DuckDB: %w", err)
	}
	defer db.Close()

	// Export the duplicates table as JSON
	exportQuery := `
		COPY (
			SELECT 
				locations.contentid,
				locations.title,
				locations.h3_index_8,
				locations.h3_index_12,
				locations.city,
				locations.latitude, 
				locations.longitude
			FROM 
				locations
			ORDER BY locations.h3_index_8 desc
		) TO 'data/output_all.jsonl' (FORMAT 'json');
	`

	_, err = db.Exec(exportQuery)
	if err != nil {
		return fmt.Errorf("failed to export JSON from DuckDB: %w", err)
	}

	return nil
}

// 2. Export duplicates as JSON lines file from DuckDB and store it in data/output.jsonl
func ExportDuplicatesAsJSONFromDuckDB(dbFilePath string) error {
	// Open the DuckDB database
	db, err := sql.Open("duckdb", dbFilePath)
	if err != nil {
		return fmt.Errorf("failed to open DuckDB: %w", err)
	}
	defer db.Close()

	// Export the duplicates table as JSON
	exportQuery := `
		COPY (
			SELECT 
				locations.contentid
				locations.title,
				locations.h3_index_8,
				locations.city,
				locations.latitude, 
				locations.longitude
			FROM 
				locations
			JOIN 
				duplicates 
			ON 
				locations.h3_index_8 = duplicates.h3_index_8
			ORDER BY locations.h3_index_8 desc
		) TO 'data/output.jsonl' (FORMAT 'jsonl');
	`

	_, err = db.Exec(exportQuery)
	if err != nil {
		return fmt.Errorf("failed to export JSON from DuckDB: %w", err)
	}

	return nil
}

// 3. convert the JSONL file to geojson features with each feature being in the format of {type: "Feature", geometry: {type: "Point", coordinates: [longitude, latitude]}, properties: {title: title}}
func ConvertJSONToGeoJSON(jsonlFilePath string) error {
	// Read the JSONL file

	jsonlData, err := os.ReadFile(jsonlFilePath)
	if err != nil {
		return fmt.Errorf("failed to read JSONL file: %w", err)
	}

	// Split the JSONL data into lines
	lines := strings.Split(string(jsonlData), "\n")
	type LocationJson struct {
		Title      string  `json:"title"`
		H3_index_8 uint64  `json:"h3_index_8"`
		City       string  `json:"city"`
		Latitude   float64 `json:"latitude"`
		Longitude  float64 `json:"longitude"`
	}

	locations := []LocationJson{}
	for _, line := range lines {
		// Check if the line is empty
		if line == "" {
			fmt.Println("empty line")
			continue
		}

		// Convert the line to []byte
		// fmt.Println(line)
		lineBytes := []byte(line)

		// Unmarshal the JSON data into a Location struct
		var location LocationJson
		err := json.Unmarshal(lineBytes, &location)
		if err != nil {
			return fmt.Errorf("failed to unmarshal JSON data: %w", err)
		}

		// Append the location to the locations slice
		locations = append(locations, location)
	}
	fmt.Println(locations)

	// Create a slice of GeoJSON features
	type Geometry struct {
		Type        string    `json:"type"`
		Coordinates []float64 `json:"coordinates"`
	}
	type Properties struct {
		Title      string `json:"title"`
		H3_index_8 uint64 `json:"h3_index_8"`
		City       string `json:"city"omitempty`
	}
	type Feature struct {
		Type       string     `json:"type"`
		Geometry   Geometry   `json:"geometry"`
		Properties Properties `json:"properties"`
	}
	type FeatureCollection struct {
		Type     string    `json:"type"`
		Features []Feature `json:"features"`
	}

	features := []Feature{}
	for _, location := range locations {
		feature := Feature{
			Type: "Feature",
			Geometry: Geometry{
				Type:        "Point",
				Coordinates: []float64{float64(location.Longitude), float64(location.Latitude)},
			},
			Properties: Properties{
				Title:      location.Title,
				H3_index_8: location.H3_index_8,
				City:       location.City,
			},
		}

		// Append the feature to the features slice
		features = append(features, feature)
	}

	featureCollection := FeatureCollection{
		Type:     "FeatureCollection",
		Features: features,
	}
	fmt.Println(featureCollection)

	geoJSONData, err := json.Marshal(featureCollection)
	if err != nil {
		return fmt.Errorf("failed to marshal GeoJSON data: %w", err)
	}

	// Write the GeoJSON data to file in data/
	dataFilePath := "data/output.geojson"
	err = os.WriteFile(dataFilePath, geoJSONData, 0644)
	if err != nil {
		return fmt.Errorf("failed to write GeoJSON data to file: %w", err)
	}

	return nil
}

// 4. findDuplicatesAndExportJSON finds all occurrences of h3_index_8 that occur more than once and stores them in a separate table, then exports the result as a JSON file
func FindDuplicatesAndExportJSON(dbFilePath string) error {
	// Open the DuckDB database
	db, err := sql.Open("duckdb", dbFilePath)
	if err != nil {
		return fmt.Errorf("failed to open DuckDB: %w", err)
	}
	defer db.Close()

	// Create a temporary table to store the duplicates
	createTableQuery := `LOAD h3;CREATE TABLE duplicates AS SELECT h3_index_8, COUNT(h3_index_8) FROM locations GROUP BY h3_index_8 HAVING count(h3_index_8) > 1 ORDER BY h3_index_8 desc;`

	_, err = db.Exec(createTableQuery)
	if err != nil {
		return fmt.Errorf("failed to create duplicates table: %w", err)
	}

	// Export the duplicates table as JSON
	exportQuery := `
		SELECT *
		FROM duplicates;
	`

	rows, err := db.Query(exportQuery)
	if err != nil {
		return fmt.Errorf("failed to export duplicates table: %w", err)
	}
	defer rows.Close()

	// Convert the rows to a slice of maps
	var duplicates []map[string]interface{}
	for rows.Next() {
		columns, err := rows.Columns()
		if err != nil {
			return fmt.Errorf("failed to get column names: %w", err)
		}

		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		err = rows.Scan(valuePtrs...)
		if err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		rowData := make(map[string]interface{})
		for i, column := range columns {
			rowData[column] = values[i]
		}

		duplicates = append(duplicates, rowData)
	}

	// Convert the duplicates slice to JSON
	jsonData, err := json.Marshal(duplicates)
	if err != nil {
		return fmt.Errorf("failed to marshal duplicates to JSON: %w", err)
	}

	// Write the JSON data to file in data/
	dataFilePath := "data/duplicates.json"
	err = ioutil.WriteFile(dataFilePath, jsonData, 0644)
	if err != nil {
		return fmt.Errorf("failed to write JSON data to file: %w", err)
	}
	return nil
}

// 5. Export the comparison results as a JSON file
func ExportComparisonResultsAsJSON(dbFilePath string) error {
	// Open the DuckDB database
	db, err := sql.Open("duckdb", dbFilePath)
	if err != nil {
		return fmt.Errorf("failed to open DuckDB: %w", err)
	}
	defer db.Close()

	// Export the comparison results as JSON
	exportQuery := `
		COPY (
			SELECT *
			FROM location_comparison
		) TO 'data/location_comparison.json' (FORMAT 'json');
	`

	_, err = db.Exec(exportQuery)
	if err != nil {
		return fmt.Errorf("failed to export comparison results as JSON: %w", err)
	}

	return nil
}

// 6. Convert all locations JSON to GeoJSON to visualize on a map
func ConvertAllLocationsJSONToGeoJSON(jsonlFilePath string) error {
	// Read the JSONL file

	jsonlData, err := os.ReadFile(jsonlFilePath)
	if err != nil {
		return fmt.Errorf("failed to read JSONL file: %w", err)
	}

	// Split the JSONL data into lines
	lines := strings.Split(string(jsonlData), "\n")
	type LocationJson struct {
		Title       string  `json:"title"`
		H3_index_8  uint64  `json:"h3_index_8"`
		H3_index_12 uint64  `json:"h3_index_12"`
		ContentId   string  `json:"contentid"`
		City        string  `json:"city"`
		Latitude    float64 `json:"latitude"`
		Longitude   float64 `json:"longitude"`
	}

	locations := []LocationJson{}
	for _, line := range lines {
		// Check if the line is empty
		if line == "" {
			fmt.Println("empty line")
			continue
		}

		// Convert the line to []byte
		// fmt.Println(line)
		lineBytes := []byte(line)

		// Unmarshal the JSON data into a Location struct
		var location LocationJson
		err := json.Unmarshal(lineBytes, &location)
		if err != nil {
			return fmt.Errorf("failed to unmarshal JSON data: %w", err)
		}

		// Append the location to the locations slice
		locations = append(locations, location)
	}
	fmt.Println(locations)

	// Create a slice of GeoJSON features
	type Geometry struct {
		Type        string    `json:"type"`
		Coordinates []float64 `json:"coordinates"`
	}
	type Properties struct {
		Title       string `json:"title"`
		H3_index_8  uint64 `json:"h3_index_8"`
		City        string `json:"city"`
		ContentId   string `json:"contentid"`
		H3_index_12 uint64 `json:"h3_index_12"`
	}
	type Feature struct {
		Type       string     `json:"type"`
		Geometry   Geometry   `json:"geometry"`
		Properties Properties `json:"properties"`
	}
	type FeatureCollection struct {
		Type     string    `json:"type"`
		Features []Feature `json:"features"`
	}

	features := []Feature{}
	for _, location := range locations {
		feature := Feature{
			Type: "Feature",
			Geometry: Geometry{
				Type:        "Point",
				Coordinates: []float64{float64(location.Longitude), float64(location.Latitude)},
			},
			Properties: Properties{
				Title:      location.Title,
				H3_index_8: location.H3_index_8,
				City:       location.City,
			},
		}

		// Append the feature to the features slice
		features = append(features, feature)
	}

	featureCollection := FeatureCollection{
		Type:     "FeatureCollection",
		Features: features,
	}
	fmt.Println(featureCollection)

	geoJSONData, err := json.Marshal(featureCollection)
	if err != nil {
		return fmt.Errorf("failed to marshal GeoJSON data: %w", err)
	}

	// Write the GeoJSON data to file in data/
	dataFilePath := "data/output_all.geojson"
	err = os.WriteFile(dataFilePath, geoJSONData, 0644)
	if err != nil {
		return fmt.Errorf("failed to write GeoJSON data to file: %w", err)
	}

	return nil
}
