import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

adls_connection_string = os.environ["STORAGE_CONNECTION_STRING"]
source_container_name = "dropzone"
destination_container_name = "dropzone"
source_directory_name = "recipes/"
destination_directory_name = "copied_recipes/"


# Connect to the source and destination storage accounts
source_blob_service_client = BlobServiceClient.from_connection_string(adls_connection_string)
destination_blob_service_client = BlobServiceClient.from_connection_string(adls_connection_string)

# Get a reference to the source container
source_container_client = source_blob_service_client.get_container_client(source_container_name)

# List blobs in the source directory
blobs = source_container_client.list_blobs(name_starts_with=source_directory_name)

# Copy each blob to the destination container
for blob in blobs:
    source_blob_client = source_blob_service_client.get_blob_client(container=source_container_name, blob=blob.name)
    print(source_blob_client.url)
    destination_blob_name = f"{destination_directory_name.rstrip('/')}/{blob.name.replace(source_directory_name, '', 1)}"
    # print(destination_blob_name)
    destination_blob_client = destination_blob_service_client.get_blob_client(container=destination_container_name,
                                                                               blob=destination_blob_name)
    print(destination_blob_client.url)
    # print(source_blob_client.url)
    destination_blob_client.start_copy_from_url(source_blob_client.url)