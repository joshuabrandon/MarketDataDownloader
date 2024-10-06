from sqlalchemy import create_engine

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

def build_connection_string():
    user = conn_params["user"]
    password = conn_params["password"]
    host = conn_params["host"]
    port = conn_params["port"]
    database = conn_params["dbname"]
    connection_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    return connection_string

def dispose_engine(engine):
    engine.dispose()
    print("Engine closed.")

# Connection variables
connection_string = build_connection_string()
engine = create_engine(connection_string)