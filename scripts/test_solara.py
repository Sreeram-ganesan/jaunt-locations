import duckdb
import solara

# Connect to DuckDB and load the locations data
con = duckdb.connect(database='data/locations.db')

# Fetch the entire locations data
locations_df = con.execute("SELECT * FROM locations").fetchdf()

# Define a Solara app to display the locations data
@solara.component
def LocationsApp():
    solara.DataFrame(locations_df)

# Run the Solara app
if __name__ == "__main__":
    solara.run(LocationsApp)