import psycopg2
# Database connection parameters
rds_url = "jauntdata.cb8c48uiq7y9.us-east-2.rds.amazonaws.com"
rds_url_2 = "dc-jaunt.cfa6488ecyrt.us-east-1.rds.amazonaws.com"
port = "5432"
dbname = "postgres"
username = "postgres"
password = "9bwSaAvMdBKgLxUlFMHe"
password_2 = "q6xItHoxiPhu"
# FUnction to query from all public table in the database the count of rows and print the table name, column names and count of rows
def connect_to_all_tables():
    try:
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(
            host=rds_url_2,
            port=port,
            dbname=dbname,
            user=username,
            password=password_2
        )
        cursor = connection.cursor()
        # Execute the query to list all tables
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """)

        # Fetch all results
        tables = cursor.fetchall()

        # Print the list of tables
        print("List of tables in the database:")
        for table in tables:
            print(table[0])
            # Print the column names and count of rows
            cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
            count = cursor.fetchone()[0]
            print(f"Table: {table[0]}, Count: {count}")
            cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table[0]}'")
            columns = cursor.fetchall()
            names_of_columns = ", ".join([column[0] for column in columns])
            print("Columns: " + names_of_columns)

    except Exception as error:
        print(f"Error connecting to the database: {error}")
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
if __name__ == "__main__":
    connect_to_all_tables()