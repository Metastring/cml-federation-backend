import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ############## local ###############
# host = "localhost"
# port = 5432
# username = "postgres"
# password = "2002"
# database = "CML_Hackthon"
# database_url = f"postgresql://{username}:{password}@{host}:{port}/{database}"

# ############## GeoServer Configuration ###############
# geoserver_host = "localhost"
# geoserver_port = "8080"
# geoserver_username = "admin"
# geoserver_password = "geoserver"

####################### Staging Machine #########################
host = os.getenv("DB_HOST", "localhost")
port = int(os.getenv("DB_PORT", "5432"))
username = os.getenv("DB_USERNAME", "")
password = os.getenv("DB_PASSWORD", "")
database = os.getenv("DB_NAME", "")
database_url = f"postgresql://{username}:{password}@{host}:{port}/{database}"

# ############################ Staging GeoServer Configuration ############################

# geoserver_host = os.getenv("GEOSERVER_HOST", "")
# geoserver_port = os.getenv("GEOSERVER_PORT", "")
# geoserver_username = os.getenv("GEOSERVER_USERNAME", "")
# geoserver_password = os.getenv("GEOSERVER_PASSWORD", "")

