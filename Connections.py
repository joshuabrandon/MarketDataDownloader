import psycopg2
from psycopg2 import sql

def read_credentials(file_path):
    credentials = {}
    
    # Open the file in read mode
    with open(file_path, 'r') as file:
        for line in file:
            if '=' in line:
                key, value = line.strip().split('=')
                credentials[key.strip()] = value.strip()
    
    return credentials['database'], credentials['username'], credentials['password']

file_path = 'credentials.txt'
database, username, password = read_credentials(file_path)

# Define connection parameters
conn_params = {
    "dbname": database,
    "user": username,
    "password": password,
    "host": "localhost",  # or the host where your DB is hosted
    "port": "5432",       # Default Postgres port
}

# Function to connect to the PostgreSQL database
def connect_to_db():
    try:
        # Establish the connection
        conn = psycopg2.connect(**conn_params)
        print("Connection established")
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None
    
# Function to create a table
def create_table(conn, create_table_query):
    try:
        # Open a cursor to perform database operations
        cur = conn.cursor()

        # Create table query
        ##create_table_query = '''
        ##CREATE TABLE IF NOT EXISTS users (
        ##    id SERIAL PRIMARY KEY,
        ##    username VARCHAR(50) NOT NULL,
        ##    password VARCHAR(50) NOT NULL
        ##);
        ##'''
        cur.execute(create_table_query)
        conn.commit()  # Save changes to the database
        print("Table created successfully")
        
        # Close the cursor
        cur.close()
    except Exception as e:
        print(f"Error creating table: {e}")

# Function to insert data
def insert_data(conn, username, password):
    try:
        cur = conn.cursor()
        insert_query = "INSERT INTO users (username, password) VALUES (%s, %s)"
        cur.execute(insert_query, (username, password))
        conn.commit()
        print("Data inserted successfully")
        cur.close()
    except Exception as e:
        print(f"Error inserting data: {e}")

# Function to retrieve data
def fetch_data(conn):
    try:
        cur = conn.cursor()
        fetch_query = "SELECT * FROM cars"
        cur.execute(fetch_query)
        
        # Fetch all rows from the executed query
        rows = cur.fetchall()
        for row in rows:
            print(row)
        
        cur.close()
    except Exception as e:
        print(f"Error fetching data: {e}")

# Function to update data
def update_data(conn, user_id, new_password):
    try:
        cur = conn.cursor()
        update_query = "UPDATE users SET password = %s WHERE id = %s"
        cur.execute(update_query, (new_password, user_id))
        conn.commit()
        print("Data updated successfully")
        cur.close()
    except Exception as e:
        print(f"Error updating data: {e}")

# Function to delete data
def delete_data(conn, user_id):
    try:
        cur = conn.cursor()
        delete_query = "DELETE FROM users WHERE id = %s"
        cur.execute(delete_query, (user_id,))
        conn.commit()
        print("Data deleted successfully")
        cur.close()
    except Exception as e:
        print(f"Error deleting data: {e}")