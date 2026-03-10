import os
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

def get_client():

    return bigquery.Client.from_service_account_json(
        credentials_path
    )