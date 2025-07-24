import psycopg2

DB_CONFIG = {
    "dbname": "***REMOVED***",
    "user": "***REMOVED***",
    "password": "***REMOVED***",
    "host": "***REMOVED***",
    "port": "5432"
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)


