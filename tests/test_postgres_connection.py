import psycopg2
from psycopg2 import Error

def test_connection():
    connection = None
    cursor = None
    try:
        # Connect to an existing database
        connection = psycopg2.connect(
            user="mark",
            password="V3nger!12",
            host="localhost",
            port="5432",
            database="abc"
        )

        # Create a cursor to perform database operations
        cursor = connection.cursor()
        print("PostgreSQL connection is open")

        # Execute a query
        print("Querying test_table...")
        cursor.execute("SELECT * FROM test_table;")
        
        # Fetch result
        records = cursor.fetchall()
        
        print(f"Found {len(records)} records in test_table:")
        for row in records:
            print(row)

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if connection:
            if cursor:
                cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

if __name__ == "__main__":
    test_connection()
