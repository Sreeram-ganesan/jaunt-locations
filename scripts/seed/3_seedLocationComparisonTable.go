package seed

import (
	"database/sql"
	"fmt"
	"os"
)

// 1. Create a new table that stores the comparison of locations with same h3_index_8 and add a new column called similarity_score by comparing their titles and column called closeness_score by comparing their h3_index_12
func CompareLocations(dbFilePath string) error {
	// Open the DuckDB database
	db, err := sql.Open("duckdb", dbFilePath)
	if err != nil {
		return fmt.Errorf("failed to open DuckDB: %w", err)
	}
	defer db.Close()
	// Create a new table to store the comparison results
	queryBytes, err := os.ReadFile("sql/3_create_location_comparison.sql")
	if err != nil {
		return fmt.Errorf("failed to read query file: %w", err)
	}
	createTableQuery := string(queryBytes)
	_, err = db.Exec(createTableQuery)
	if err != nil {
		return fmt.Errorf("failed to create location_comparison table: %w", err)
	} else {
		fmt.Println("location_comparison table created successfully or already exists")
	}

	return nil
}

func CheckIfLocationComparisonIsSeeded(dbFilePath string, source string) (bool, error) {
	//check if the source is already present in location_comparison table
	db, err := sql.Open("duckdb", dbFilePath)
	if err != nil {
		return false, fmt.Errorf("failed to open DuckDB: %w", err)
	}
	defer db.Close()
	query := `
		SELECT count(*) FROM location_comparison lc join locations l on lc.contentid1 = l.contentid AND l.source_url = '%s' limit 1;
	`
	rows, err := db.Query(fmt.Sprintf(query, source))
	if err != nil {
		return false, fmt.Errorf("failed to check if source is present in location_comparison table: %w", err)
	}
	defer rows.Close()
	if rows.Next() {
		fmt.Printf("Source %s already present in location_comparison table. Skipping insertion\n", source)
		return true, nil
	}
	return false, nil
}

// 3. Identify location content_id not present in location_comparison table and insert them
func CompareNewlyInsertedLocationsWithRestOfLocations(dbFilePath string) error {
	// Open the DuckDB database
	db, err := sql.Open("duckdb", dbFilePath)
	if err != nil {
		return fmt.Errorf("failed to open DuckDB: %w", err)
	}
	defer db.Close()
	// Create a new table to store the comparison results
	// isPresent, err := CheckIfLocationComparisonIsSeeded(dbFilePath, source)
	// if err != nil {
	// 	return fmt.Errorf("failed to check if source is present in location_comparison table: %w", err)
	// }
	// if isPresent {
	// 	return nil
	// }
	// Insert into location_comparison table
	queryBytes, err := os.ReadFile("sql/4_insert_into_location_comparison.sql")
	if err != nil {
		return fmt.Errorf("failed to read query file: %w", err)
	}
	query := string(queryBytes)
	rows, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create location_comparison table: %w", err)
	}
	affectedRows, err := rows.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get affected rows: %w", err)
	}
	fmt.Printf("Number of comparison rows newly inserted or replaced into location_comparisons: %d\n", affectedRows)
	//print total rows in location_comparison
	totalRowsQuery := `
		SELECT count(*) FROM location_comparison;
	`
	totalRows, err := db.Query(totalRowsQuery)
	if err != nil {
		return fmt.Errorf("failed to get total rows in location_comparison: %w", err)
	}
	defer totalRows.Close()
	var count int
	for totalRows.Next() {
		totalRows.Scan(&count)
	}
	fmt.Printf("Total rows in location_comparison: %d\n", count)
	return nil
}

// 2. Add a method to insert similarity_score into location_comparison table by comparing the titles of the locations if null
func CompareLocationsInsertSimilarityScore(dbFilePath string) error {
	// Open the DuckDB database
	db, err := sql.Open("duckdb", dbFilePath)
	if err != nil {
		return fmt.Errorf("failed to open DuckDB: %w", err)
	}
	defer db.Close()
	//Alter the table to add a new column called similarity_score
	alterTableQuery := `
		ALTER TABLE location_comparison ADD COLUMN IF NOT EXISTS similarity_score FLOAT;
	`
	_, err = db.Exec(alterTableQuery)
	if err != nil {
		return fmt.Errorf("failed to alter location_comparison table: %w", err)
	}
	// Create a new table to store the comparison results
	createTableQuery := `
		UPDATE location_comparison
		SET similarity_score = jaro_winkler_similarity(l1.title, l2.title)
		FROM locations l1, locations l2
		WHERE location_comparison.similarity_score IS NULL
		AND location_comparison.contentid1 = l1.contentid
		AND location_comparison.contentid2 = l2.contentid;
	`
	_, err = db.Exec(createTableQuery)
	if err != nil {
		return fmt.Errorf("failed to create location_comparison table: %w", err)
	}
	return nil
}

// 3. Use SPATIAL plugin of duckdb and use ST_DISTANCE_SPHEROID function to calculate the distance between two points and store as a new column in location_comparison table
func CompareLocationsWithHaversineDistance(dbFilePath string) error {
	// Open the DuckDB database
	db, err := sql.Open("duckdb", dbFilePath)
	if err != nil {
		return fmt.Errorf("failed to open DuckDB: %w", err)
	}
	defer db.Close()
	// update below with join condition
	query := `
		INSTALL spatial;LOAD spatial;
		ALTER TABLE location_comparison ADD COLUMN IF NOT EXISTS distance_in_meters FLOAT;
		UPDATE location_comparison
		SET distance_in_meters = ST_DISTANCE_SPHEROID(ST_POINT(l1.latitude, l1.longitude), ST_POINT(l2.latitude, l2.longitude))
		FROM locations l1, locations l2
		WHERE location_comparison.distance_in_meters IS NULL
		AND location_comparison.contentid1 = l1.contentid
		AND location_comparison.contentid2 = l2.contentid;
	`
	_, err = db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create location_comparison table: %w", err)
	}
	return nil
}
