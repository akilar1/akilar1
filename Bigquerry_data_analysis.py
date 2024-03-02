import pyodbc
from google.cloud import bigquery

import pandas as pd

from google.outh2 import service_account

from pandas _profiling import ProfileReport


# Define the path to the service account JSON file
service_account_file = 'secure.json'


# Create a credentials object from the service account JSON file

credentials = service_account.Credentials.from_service_account_file(service_account_file)
# Create a BigQuery client object using the credentials

client = bigquery.Client(credentials=credentials)

# Read data from BigQuery
query = """
SELECT *
FROM flight_details"""

results = client.query(query).to_dataframe()

results=results.astype(str)

print (results.dtypes)

profile = ProfileReport(results)

profile.to_file("flights_data_analysis.html")

