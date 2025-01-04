package seed

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/lib/pq"
	_ "github.com/marcboeker/go-duckdb"
)

// 1. Create the base location table in DuckDB
func CreateLocationsTableInDuckDB(dbFilePath string) error {
	// Open the DuckDB database
	db, err := sql.Open("duckdb", dbFilePath)
	if err != nil {
		return fmt.Errorf("failed to open DuckDB: %w", err)
	}
	defer db.Close()

	// Create the locations table
	queryBytes, err := os.ReadFile("sql/0_create_locations.sql")
	if err != nil {
		return fmt.Errorf("failed to read query file: %w", err)
	}
	query := string(queryBytes)

	_, err = db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create locations table: %w", err)
	} else {
		fmt.Println("Locations table created successfully or already exists")
	}

	return nil
}

// 2. Insert into the locations table in DuckDB from a t_content raw data JSON file in data/ folder
func InsertJSONIntoLocationsTableInDuckDB(dbFilePath string, jsonFilePath string) error {
	// Open the DuckDB database
	db, err := sql.Open("duckdb", dbFilePath)
	if err != nil {
		return fmt.Errorf("failed to open DuckDB: %w", err)
	}
	defer db.Close()
	// //Check if locations table is present and ignore insertion if present
	// query := `
	// 	SELECT * FROM locations limit 1;
	// `
	// rows, err := db.Query(query)
	// if err != nil {
	// 	return fmt.Errorf("failed to check if locations table is present: %w", err)
	// }
	// defer rows.Close()
	// var count int
	// for rows.Next() {
	// 	count++
	// 	if count > 0 {
	// 		break
	// 	}
	// }
	// if count > 0 {
	// 	fmt.Println("Locations table already present. Skipping insertion")
	// 	return nil
	// }
	queryBytes, err := os.ReadFile("sql/1_insert.sql")
	if err != nil {
		return fmt.Errorf("failed to read query file: %w", err)
	}
	query := fmt.Sprintf(string(queryBytes), jsonFilePath)
	//run the query
	rows, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to insert JSON into locations table: %w", err)
	} else {
		fmt.Printf("JSON at path: %s data successfully inserted or replaced into locations table\n", jsonFilePath)
	}
	//print nNumber of rows inserted
	n, err := rows.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get number of rows inserted: %w", err)
	}
	fmt.Printf("Number of rows inserted into locations table: %d\n", n)
	//return nil if no error
	return nil
}

// 5. Add a unique constraint on the contentid, title combination
func AddUniqueConstraintOnLocationsTableInDuckDB(dbFilePath string) error {
	// Open the DuckDB database
	db, err := sql.Open("duckdb", dbFilePath)
	if err != nil {
		return fmt.Errorf("failed to open DuckDB: %w", err)
	}
	defer db.Close()

	// Add unique constraint on contentid and title
	query := `
		ALTER TABLE locations ADD CONSTRAINT contentid_title_unique_constraint UNIQUE (contentid, title);
	`

	r, err := db.Exec(query)
	fmt.Printf("result of the unique constraint query: %v\n", r)
	if err != nil {
		return fmt.Errorf("failed to add unique constraint: %w", err)
	}

	return nil
}

// 3. describe the schema of the locations table
func DescribeLocationsTableInDuckDB(dbFilePath string) error {
	// Open the DuckDB database
	db, err := sql.Open("duckdb", dbFilePath)
	if err != nil {
		return fmt.Errorf("failed to open DuckDB: %w", err)
	}
	defer db.Close()

	// Describe the locations table
	query := `
		DESCRIBE locations;
	`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to describe locations table: %w", err)
	}
	defer rows.Close()

	// Print the column names and types
	for rows.Next() {
		var columnName string
		var columnType string
		err = rows.Scan(&columnName, &columnType)
		if err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		fmt.Printf("%s: %s\n", columnName, columnType)
	}

	return nil
}

// 4. Use the following db connection string to connect to postgres db and list all the tables
//
//	DB_SETTINGS = {
//	    'host': 'jauntdata.cb8c48uiq7y9.us-east-2.rds.amazonaws.com',
//	    'port': '5432',
//	    'dbname': 'postgres',
//	    'user': 'postgres',
//	    'password': '9bwSaAvMdBKgLxUlFMHe'
//	}
func ListAllTablesInPostgresDB() error {
	// Open the Postgres database
	db, err := sql.Open("postgres", "host=jauntdata.cb8c48uiq7y9.us-east-2.rds.amazonaws.com port=5432 dbname=postgres user=postgres password=9bwSaAvMdBKgLxUlFMHe sslmode=disable")
	if err != nil {
		return fmt.Errorf("failed to open Postgres: %w", err)
	}
	defer db.Close()

	// List all the tables
	query := `
			SELECT table_name
			FROM information_schema.tables
			WHERE table_schema = 'public';
		`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to list all tables: %w", err)
	}
	defer rows.Close()

	// Print the table names
	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		fmt.Println(tableName)
	}

	return nil
}
