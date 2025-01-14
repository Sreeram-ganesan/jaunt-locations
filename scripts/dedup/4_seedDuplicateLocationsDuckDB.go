package dedup

import (
	"database/sql"
	"fmt"
)

// 1. Method that uses the location_comparison table to remove duplicate location entries in the locations table with condition being the title similarity_score > 0.85 and distance_in_meters <= 50
// update the description of the location to be a combination of the the two values in descriptions array in location_comparison table and then delete the second entry
func ArchiveAndRemoveDuplicateLocations(dbFilePath string) error {
	// Open the DuckDB database
	db, err := sql.Open("duckdb", dbFilePath)
	if err != nil {
		return fmt.Errorf("failed to open DuckDB: %w", err)
	}
	defer db.Close()

	//create duplicate_locations table
	createTableQuery := `
		DROP TABLE IF EXISTS duplicate_locations;
		CREATE TABLE IF NOT EXISTS duplicate_locations AS
		SELECT * FROM locations WHERE 1=0;
	`
	_, err = db.Exec(createTableQuery)
	if err != nil {
		return fmt.Errorf("failed to create duplicate_locations table with err: %w", err)
	}

	// Insert into duplicate_locations table
	// insertQuery := `
	// 	INSERT INTO duplicate_locations (select l.* from locations l JOIN location_comparison lc ON l.contentid = lc.contentid2 AND lc.similarity_score > 0.85 OR lc.sentence_similarity_score > 0.70 AND lc.distance_in_meters <= 50);
	// `
	insertQuery := `
		ALTER TABLE duplicate_locations ADD COLUMN IF NOT EXISTS tag TEXT DEFAULT 'None';
		INSERT INTO duplicate_locations (
			contentid,
			source_id,
			title,
			subtitle,
			breadcrumb,
			category,
			subcategory,
			description,
			short_description,
			keywords,
			source_url,
			address,
			address_detailed,
			zipcode,
			phone,
			latitude,
			longitude,
			running_hours,
			featured_image_url,
			image_urls,
			additional_info,
			primary_data,
			city,
			country,
			state,
			h3_index_8,
			h3_index_12,
			word_count,
			is_title_duplicate,
			tag
		)
		select 
			contentid,
			source_id,
			title,
			subtitle,
			breadcrumb,
			category,
			subcategory,
			description,
			short_description,
			keywords,
			source_url,
			address,
			address_detailed,
			zipcode,
			phone,
			latitude,
			longitude,
			running_hours,
			featured_image_url,
			image_urls,
			additional_info,
			primary_data,
			city,
			country,
			state,
			h3_index_8,
			h3_index_12,
			0 as word_count,
			is_title_duplicate,
			'red' as tag
		from locations where contentid in (select distinct(l.contentid) from locations l JOIN location_comparison lc ON l.contentid = lc.contentid2 AND lc.similarity_score > 0.85 AND lc.distance_in_meters <= 50);
	`

	rows, err := db.Exec(insertQuery)
	if err != nil {
		return fmt.Errorf("failed to insert into duplicate_locations table: %w", err)
	}
	alternateRows, err := rows.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get affected rows: %w", err)
	}
	fmt.Printf("Number of duplicate locations inserted into duplicate_locations for scenario1: %d\n", alternateRows)
	insertQuery2 := `
		INSERT INTO duplicate_locations (
			contentid,
			source_id,
			title,
			subtitle,
			breadcrumb,
			category,
			subcategory,
			description,
			short_description,
			keywords,
			source_url,
			address,
			address_detailed,
			zipcode,
			phone,
			latitude,
			longitude,
			running_hours,
			featured_image_url,
			image_urls,
			additional_info,
			primary_data,
			city,
			country,
			state,
			h3_index_8,
			h3_index_12,
			word_count,
			is_title_duplicate,
			tag
		)
		select 
			contentid,
			source_id,
			title,
			subtitle,
			breadcrumb,
			category,
			subcategory,
			description,
			short_description,
			keywords,
			source_url,
			address,
			address_detailed,
			zipcode,
			phone,
			latitude,
			longitude,
			running_hours,
			featured_image_url,
			image_urls,
			additional_info,
			primary_data,
			city,
			country,
			state,
			h3_index_8,
			h3_index_12,
			0 as word_count,
			is_title_duplicate,
			'lightred' as tag
		from locations where contentid in (select distinct(l.contentid) from locations l JOIN location_comparison lc ON l.contentid = lc.contentid2 AND lc.similarity_score > 0.85 AND lc.distance_in_meters <= 50 JOIN similarity s on s.contentid1 = lc.contentid1 AND s.contentid2 = lc.contentid2 AND s.score > 0.70);
	`
	rows2, err := db.Exec(insertQuery2)
	if err != nil {
		return fmt.Errorf("failed to insert into duplicate_locations table: %w", err)
	}
	alternateRows2, err := rows2.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get affected rows: %w", err)
	}
	fmt.Printf("Number of duplicate locations inserted into duplicate_locations for scenario2: %d\n", alternateRows2)
	insertQuery3 := `
		INSERT INTO duplicate_locations (
			contentid,
			source_id,
			title,
			subtitle,
			breadcrumb,
			category,
			subcategory,
			description,
			short_description,
			keywords,
			source_url,
			address,
			address_detailed,
			zipcode,
			phone,
			latitude,
			longitude,
			running_hours,
			featured_image_url,
			image_urls,
			additional_info,
			primary_data,
			city,
			country,
			state,
			h3_index_8,
			h3_index_12,
			word_count,
			is_title_duplicate,
			tag
		)
		select 
			contentid,
			source_id,
			title,
			subtitle,
			breadcrumb,
			category,
			subcategory,
			description,
			short_description,
			keywords,
			source_url,
			address,
			address_detailed,
			zipcode,
			phone,
			latitude,
			longitude,
			running_hours,
			featured_image_url,
			image_urls,
			additional_info,
			primary_data,
			city,
			country,
			state,
			h3_index_8,
			h3_index_12,
			0 as word_count,
			is_title_duplicate,
			'darkred' as tag
		from locations where contentid in (select distinct lc.contentid2 from location_comparison lc JOIN similarity s ON s.contentid1 = lc.contentid1 AND s.contentid2 = lc.contentid2 AND (lc.similarity_score > 0.85 OR s.score > 0.7) AND lc.distance_in_meters <=50);
	`
	rows3, err := db.Exec(insertQuery3)
	if err != nil {
		return fmt.Errorf("failed to insert into duplicate_locations table: %w", err)
	}
	alternateRows3, err := rows3.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get affected rows: %w", err)
	}
	fmt.Printf("Number of duplicate locations inserted into duplicate_locations for scenario3: %d\n", alternateRows3)
	insertQuery4 := `
		INSERT INTO duplicate_locations (
			contentid,
			source_id,
			title,
			subtitle,
			breadcrumb,
			category,
			subcategory,
			description,
			short_description,
			keywords,
			source_url,
			address,
			address_detailed,
			zipcode,
			phone,
			latitude,
			longitude,
			running_hours,
			featured_image_url,
			image_urls,
			additional_info,
			primary_data,
			city,
			country,
			state,
			h3_index_8,
			h3_index_12,
			word_count,
			is_title_duplicate,
			tag
		)
		select 
			contentid,
			source_id,
			title,
			subtitle,
			breadcrumb,
			category,
			subcategory,
			description,
			short_description,
			keywords,
			source_url,
			address,
			address_detailed,
			zipcode,
			phone,
			latitude,
			longitude,
			running_hours,
			featured_image_url,
			image_urls,
			additional_info,
			primary_data,
			city,
			country,
			state,
			h3_index_8,
			h3_index_12,
			0 as word_count,
			is_title_duplicate,
			'pink' as tag
		from locations where contentid in (select distinct lc.contentid2 from location_comparison lc JOIN similarity s ON s.contentid1 = lc.contentid1 AND s.contentid2 = lc.contentid2 AND (lc.similarity_score > 0.85 OR s.score > 0.7) AND lc.distance_in_meters >50);
	`
	rows4, err := db.Exec(insertQuery4)
	if err != nil {
		return fmt.Errorf("failed to insert into duplicate_locations table: %w", err)
	}
	alternateRows4, err := rows4.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get affected rows: %w", err)
	}
	fmt.Printf("Number of duplicate locations inserted into duplicate_locations for scenario4: %d\n", alternateRows4)
	insertQuery5 := `
		INSERT INTO duplicate_locations (
			contentid,
			source_id,
			title,
			subtitle,
			breadcrumb,
			category,
			subcategory,
			description,
			short_description,
			keywords,
			source_url,
			address,
			address_detailed,
			zipcode,
			phone,
			latitude,
			longitude,
			running_hours,
			featured_image_url,
			image_urls,
			additional_info,
			primary_data,
			city,
			country,
			state,
			h3_index_8,
			h3_index_12,
			word_count,
			is_title_duplicate,
			tag
		)
		select 
			contentid,
			source_id,
			title,
			subtitle,
			breadcrumb,
			category,
			subcategory,
			description,
			short_description,
			keywords,
			source_url,
			address,
			address_detailed,
			zipcode,
			phone,
			latitude,
			longitude,
			running_hours,
			featured_image_url,
			image_urls,
			additional_info,
			primary_data,
			city,
			country,
			state,
			h3_index_8,
			h3_index_12,
			0 as word_count,
			is_title_duplicate,
			'purple' as tag
		from locations where contentid in ((select distinct lc.contentid2 from location_comparison lc JOIN similarity s ON s.contentid1 = lc.contentid1 AND s.contentid2 = lc.contentid2 AND (lc.similarity_score > 0.7 OR s.score > 0.6) AND lc.distance_in_meters <=50) UNION (select distinct lc.contentid2 from location_comparison lc JOIN similarity s ON s.contentid1 = lc.contentid1 AND s.contentid2 = lc.contentid2 AND (lc.similarity_score > 0.85 OR s.score > 0.7) AND lc.distance_in_meters >50));
	`
	rows5, err := db.Exec(insertQuery5)
	if err != nil {
		return fmt.Errorf("failed to insert into duplicate_locations table: %w", err)
	}
	alternateRows5, err := rows5.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get affected rows: %w", err)
	}
	fmt.Printf("Number of duplicate locations inserted into duplicate_locations for scenario5: %d\n", alternateRows5)

	// Delete from locations table
	// deleteQuery := `
	// 	Delete from locations
	// 	WHERE contentid IN (
	// 		SELECT contentid
	// 		FROM duplicate_locations
	// 	);
	// `

	// r, err := db.Exec(deleteQuery)
	// if err != nil {
	// 	return fmt.Errorf("failed to delete from locations table: %w", err)
	// }
	// noRows, _ := r.RowsAffected()
	// fmt.Printf("Number of duplicate locations soft deleted from locations: %d\n", noRows)

	// Update the description of the remaining locations
	// updateQuery := `
	// 	UPDATE locations
	// 	SET description = description || '|' || (
	// 		SELECT string_agg(descriptions, '|')
	// 		FROM location_comparison
	// 		WHERE locations.contentid = location_comparison.contentid1
	// 	)
	// 	WHERE locations.contentid IN (
	// 		SELECT contentid1
	// 		FROM location_comparison
	// 		WHERE similarity_score > 0.85
	// 		AND distance_in_meters <= 50
	// 	);
	// `

	// ro, err := db.Exec(updateQuery)
	// if err != nil {
	// 	return fmt.Errorf("failed to update descriptions: %w", err)
	// }
	// nr, _ := ro.RowsAffected()
	// fmt.Printf("Number of descriptions updated in locations: %d\n", nr)

	return nil
}
