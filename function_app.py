import azure.functions as func
import logging

import os
import requests
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import zipfile
import io

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="funcKaggleDataFetch")
def funcKaggleDataFetch(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

     # Replace with your Kaggle dataset details
    dataset = 'heesoo37'  # olympics dataset
    filename = '120-years-of-olympic-history-athletes-and-results.csv'  # Example: 'State_time_series.csv'
    
    # Access Kaggle API credentials from Azure Key Vault
    credential = DefaultAzureCredential()
    secret_client = SecretClient(vault_url="https://keggleapikey.vault.azure.net/", credential=credential)
    kaggle_username = secret_client.get_secret("KaggleUsername").value
    kaggle_key = secret_client.get_secret("KaggleApiKey").value

    # Download dataset from Kaggle
    kaggle_url = f"https://www.kaggle.com/api/v1/datasets/download/{dataset}"
    headers = {'Authorization': f'Bearer {kaggle_key}'}
    response = requests.get(kaggle_url, headers=headers)

    if response.status_code == 200:
        # Unzip the file and get the specific CSV file
        zip_file = zipfile.ZipFile(io.BytesIO(response.content))
        with zip_file.open(filename) as file:
            csv_data = file.read()
        
        # Upload the CSV data to Azure Blob Storage
        blob_service_client = BlobServiceClient.from_connection_string(os.getenv("olympicsstudydatastorage"))
        blob_client = blob_service_client.get_blob_client(container="raw-data-bronze", blob=filename)
        blob_client.upload_blob(csv_data, overwrite=True)
        
        return func.HttpResponse(f"File {filename} uploaded successfully!", status_code=200)
    else:
        return func.HttpResponse(f"Failed to download data: {response.text}", status_code=response.status_code)

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )