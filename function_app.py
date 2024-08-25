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
    logging.info('Function started processing a request.')

    # Configuration for datasets to process
    datasets_config = [
        {
            'owner': 'heesoo37',
            'dataset': '120-years-of-olympic-history-athletes-and-results',
            'filenames': ['athlete_events.csv', 'noc_regions.csv']
        },
        {
            'owner': 'chadalee',
            'dataset': 'country-wise-gdp-data',
            'filenames': ['world_gdp.csv']  # Update with actual filenames if different
        },
        {
            'owner': 'chadalee',
            'dataset': 'country-wise-population-data',
            'filenames': ['world_pop.csv']  # Update with actual filenames if different
        }
    ]

    try:
        # Initialize Azure credentials
        credential = DefaultAzureCredential()
        
        # Access Kaggle API credentials from Azure Key Vault
        secret_client = SecretClient(
            vault_url="https://keggleapikey.vault.azure.net/", 
            credential=credential
        )
        kaggle_username = secret_client.get_secret("KeggleAPIUsername").value
        kaggle_key = secret_client.get_secret("KaggleApiKey").value
        logging.info("Successfully retrieved Kaggle API credentials from Key Vault.")

        # Initialize Blob Service Client
        blob_service_client = BlobServiceClient(
            account_url="https://olympicsstudydatastorage.blob.core.windows.net/", 
            credential=credential
        )
        logging.info("Successfully initialized Azure Blob Service Client.")

        # Process each dataset in the configuration
        for config in datasets_config:
            owner = config['owner']
            dataset = config['dataset']
            filenames = config['filenames']

            logging.info(f"Processing dataset: {dataset} by {owner}")

            kaggle_url = f"https://www.kaggle.com/api/v1/datasets/download/{owner}/{dataset}"
            response = requests.get(kaggle_url, auth=(kaggle_username, kaggle_key))

            # Check response status
            if response.status_code != 200:
                logging.error(f"Failed to download dataset {dataset}: HTTP {response.status_code}")
                continue

            # Check if the response is a ZIP file
            if 'zip' not in response.headers.get('Content-Type', ''):
                logging.error(f"The response for dataset {dataset} is not a ZIP file.")
                continue

            # Extract files from ZIP
            try:
                zip_file = zipfile.ZipFile(io.BytesIO(response.content))
            except zipfile.BadZipFile:
                logging.error(f"Downloaded file for dataset {dataset} is a bad ZIP file.")
                continue

            # Define the subfolder name for this dataset
            subfolder = f"{dataset.replace(' ', '_')}"

            # Process each file in the dataset
            for filename in filenames:
                if filename in zip_file.namelist():
                    with zip_file.open(filename) as file:
                        file_data = file.read()

                    # Define the blob path (subfolder/filename)
                    blob_path = f"{subfolder}/{filename}"

                    # Upload the file to Azure Blob Storage within the subfolder
                    blob_client = blob_service_client.get_blob_client(container="raw-data-bronze", blob=blob_path)
                    blob_client.upload_blob(file_data, overwrite=True)
                    logging.info(f"Successfully uploaded '{filename}' to subfolder '{subfolder}' in container 'raw-data-bronze'.")
                else:
                    logging.warning(f"File '{filename}' not found in dataset '{dataset}'.")

        logging.info("All datasets processed successfully.")
        return func.HttpResponse("Datasets processed successfully.", status_code=200)

    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)