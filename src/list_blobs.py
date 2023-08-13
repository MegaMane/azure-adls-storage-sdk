import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

source_connection_string = os.environ["STORAGE_CONNECTION_STRING"]
source_container_name = "dropzone"
source_directory_name = "recipes/"



# Connect to the source and destination storage accounts
source_blob_service_client = BlobServiceClient.from_connection_string(source_connection_string)


# Get a reference to the source container
source_container_client = source_blob_service_client.get_container_client(source_container_name)

# List blobs in the source directory
blobs = source_container_client.list_blobs(name_starts_with=source_directory_name)

# Copy each blob to the destination container
for blob in blobs:
    print(blob)
    source_blob_client = source_blob_service_client.get_blob_client(container=source_container_name, blob=blob.name)
    print(source_blob_client.url)

