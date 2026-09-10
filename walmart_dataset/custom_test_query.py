import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()
connection_uri = os.getenv("DB_CONNECTION_STRING")

# Define test query
test_query = "SELECT * FROM customers LIMIT 10;"

try:
    # Establish the connection
    conn = psycopg2.connect(connection_uri)
    cursor = conn.cursor()
    print("Database connection established.\n")
    
    # Execute the single query
    cursor.execute(test_query)
    
    # Fetch all returned rows
    results = cursor.fetchall()
    
    # Print the results
    print("--- Query Results ---")
    for row in results:
        print(row)
    print("---------------------\n")
        
except Exception as e:
    print(f"Database operation failed: {e}")
        
finally:
    # Securely close the connection
    if 'conn' in locals() and conn:
        cursor.close()
        conn.close()
        print("Connection closed.")