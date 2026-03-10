import os
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()


def get_client():
    project = os.getenv("BQ_PROJECT_ID", "marketing-489109")
    return bigquery.Client(project=project)