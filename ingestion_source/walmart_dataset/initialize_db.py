import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()
connection_uri = os.getenv("DB_CONNECTION_STRING")

# Define database schema (Execute code from the parent folder)
with open('walmart_dataset/ddl/walmart_schema.sql', 'r') as sql_file:
        create_schema_query = sql_file.read()

csv_files = {
                'customers': 'customers.csv', 
                'employees': 'employees.csv',
                'orders': 'orders.csv', 
                'order_items': 'order_items.csv', 
                'products': 'products.csv', 
                'stores': 'stores.csv'
            }

try:
    # Establish the connection
    conn = psycopg2.connect(connection_uri)
    cursor = conn.cursor()
    print("Database connection established.")
    
    # Execute the schema creation
    cursor.execute(create_schema_query)
    conn.commit()
    print("Table schema verified.")
    
    # Upload the CSV data
    csv_dir = 'walmart_dataset/data/'
    
    for table_name, csv_file in csv_files.items():
        with open(f'{csv_dir}{csv_file}', 'r', encoding='utf-8') as file:
            # The COPY command maps the CSV directly into the table
            # 'HEADER' skips the first row of the CSV file
            copy_query = f"""
            COPY {table_name} 
            FROM STDIN WITH CSV HEADER
        """
        
            cursor.copy_expert(copy_query, file)
            conn.commit()
        
    print("CSV data uploaded successfully!")
        
except Exception as e:
    print(f"Database operation failed: {e}")
    # Revert changes if an error occurs to prevent partial data loading
    if 'conn' in locals() and conn:
        conn.rollback()
        
finally:
    # Securely close the connection
    if 'conn' in locals() and conn:
        cursor.close()
        conn.close()
        print("Connection closed.")