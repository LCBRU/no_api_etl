"""Environment Variables
"""
import os
from dotenv import load_dotenv

load_dotenv()

EDGE_USERNAME = os.environ["EDGE_USERNAME"]
EDGE_PASSWORD = os.environ["EDGE_PASSWORD"]
EDGE_BASE_URL = os.environ["EDGE_BASE_URL"]

CRFM_USERNAME = os.environ["CRFM_USERNAME"]
CRFM_PASSWORD = os.environ["CRFM_PASSWORD"]
CRFM_BASE_URL = os.environ["CRFM_BASE_URL"]
CRFM_DB_ID = os.environ["CRFM_DB_ID"]

EMAIL_FROM_ADDRESS = os.environ["EMAIL_FROM_ADDRESS"]
EMAIL_SMTP_SERVER = os.environ["EMAIL_SMTP_SERVER"]

DEFAULT_RECIPIENT = os.environ["DEFAULT_RECIPIENT"]
DATABASE_ECHO = os.environ["DATABASE_ECHO"] == 'True'

ETL_CENTRAL_CONNECTION_STRING = os.environ["ETL_CENTRAL_CONNECTION_STRING"]
ETL_DATABASES_CONNECTION_STRING = os.environ["ETL_DATABASES_CONNECTION_STRING"]
ETL_DATABASES_PREFIX = os.environ["ETL_DATABASES_PREFIX"]
ETL_DATABASES_HOST = os.environ["ETL_DATABASES_HOST"]
ETL_DATABASES_USERNAME = os.environ["ETL_DATABASES_USERNAME"]
ETL_DATABASES_PASSWORD = os.environ["ETL_DATABASES_PASSWORD"]

ETL_DOWNLOAD_USERNAME = os.environ["ETL_DOWNLOAD_USERNAME"]
ETL_DOWNLOAD_PASSWORD = os.environ["ETL_DOWNLOAD_PASSWORD"]
ETL_ENCRYPTION_PASSWORD = os.environ["ETL_ENCRYPTION_PASSWORD"]
