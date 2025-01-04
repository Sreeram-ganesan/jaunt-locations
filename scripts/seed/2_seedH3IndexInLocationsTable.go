package seed

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/marcboeker/go-duckdb"
)

func UpdateLocationsWithH3Index(dbFilePath string) error {
	// Open the DuckDB database
	db, err := sql.Open("duckdb", dbFilePath)
	if err != nil {
		return fmt.Errorf("failed to open DuckDB: %w", err)
	}
	defer db.Close()

	queryBytes, err := os.ReadFile("sql/2_update_h3_index_8_and_12.sql")
	if err != nil {
		return fmt.Errorf("failed to read query file: %w", err)
	}
	query := string(queryBytes)
	//run the query

	rows, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to update locations with h3_index: %w", err)
	}
	// defer rows.Close()
	affectedRows, err := rows.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get affected rows: %w", err)
	}
	fmt.Printf("Number of rows updated with h3 index 8 and 12: %d\n", affectedRows)

	return nil
}
