# Databricks notebook source
# MAGIC %pip install azure-storage-blob

# COMMAND ----------

import requests
from azure.storage.blob import BlobServiceClient

# Retrieve the connection string from the Databricks secret scope
connection_string = dbutils.secrets.get(scope="keys", key="storage-connection-string")

# URL of the CSV file from the NHS Digital website
csv_url = 'https://files.digital.nhs.uk/77/2ED6D3/AE_2324_ECDS_open_data_csv.csv'

# File path for local storage in Databricks
csv_file_path = '/tmp/AE_2324_ECDS_open_data_csv.csv'

# Download the CSV file
response_csv = requests.get(csv_url)
with open(csv_file_path, 'wb') as file:
    file.write(response_csv.content)
print("CSV file downloaded successfully")

# Upload the CSV file to Azure Blob Storage
container_name = 'ecdsdata'
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Create a BlobClient and upload the file
blob_client_csv = blob_service_client.get_blob_client(container=container_name, blob='AE_2324_ECDS_open_data_csv.csv')
with open(csv_file_path, 'rb') as data:
    blob_client_csv.upload_blob(data, overwrite=True)
print("CSV file uploaded successfully")

