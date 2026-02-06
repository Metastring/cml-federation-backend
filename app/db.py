import psycopg2

DB_CONFIG = {
    "dbname": "federation_db",
    "user": "cml",
    "password": "cmlusermlc",
    "host": "localhost",
    "port": "5432"
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)


