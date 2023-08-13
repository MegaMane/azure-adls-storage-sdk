import concurrent.futures
import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv


from time import time


def timer_func(func):
    # This function shows the execution time of
    # the function object passed
    def wrap_func(*args, **kwargs):
        t1 = time()
        result = func(*args, **kwargs)
        t2 = time()
        print(f'Function {func.__name__!r} executed in {(t2 - t1):.4f}s')
        return result

    return wrap_func


class BlobFileArchive:
    """
    BlobFileArchive is a class that handles archiving files from a source container/directory
    to a destination container/directory in Azure Blob Storage.
    """

    def __init__(self, connection_string, source_container, source_directory_name, destination_container,
                 destination_directory_name):
        """
        Initializes the BlobFileArchive object.

        Parameters:
            connection_string (str): Connection string for the Azure Blob Storage account.
            source_container (str): Name of the source container.
            source_directory_name (str): Name of the source directory within the source container.
            destination_container (str): Name of the destination container.
            destination_directory_name (str): Name of the destination directory within the destination container.
        """
        # Connect to the source and destination storage accounts
        self.source_blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self.destination_blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        self.source_container_name = source_container
        self.destination_container_name = destination_container
        self.source_directory_name = source_directory_name
        self.destination_directory_name = destination_directory_name.rstrip("/")

        # Get a reference to the source container
        self.source_container_client = self.source_blob_service_client.get_container_client(self.source_container_name)

        # List blobs in the source directory
        self.blobs = self.source_container_client.list_blobs(name_starts_with=self.source_directory_name)

    def copy_blob(self, blob):
        """
        Copies an individual blob from the source to the destination.

        Parameters:
            blob (azure.storage.blob.BlobProperties): The blob object to be copied.

        Returns:
            str: A string indicating the completion of the copying process.
        """
        source_blob_client = self.source_blob_service_client.get_blob_client(
            container=self.source_container_name, blob=blob.name)
        destination_blob_name = f"{self.destination_directory_name}/{blob.name.replace(self.source_directory_name, '', 1)}"
        destination_blob_client = self.destination_blob_service_client.get_blob_client(
            container=self.destination_container_name, blob=destination_blob_name)
        destination_blob_client.start_copy_from_url(source_blob_client.url)
        return f"Completed copying {blob.name}"

    def delete_blob(self, blob):
        """
        Deletes an individual blob from the source.

        Parameters:
            blob (azure.storage.blob.BlobProperties): The blob object to be deleted.

        Returns:
            str: A string indicating the completion of the deletion process.
        """
        source_blob_client = self.source_blob_service_client.get_blob_client(
            container=self.source_container_name, blob=blob.name)
        source_blob_client.delete_blob()
        return f"Deleted {blob.name}"

    def move_blob(self, blob):
        """
        Moves an individual blob from the source to the destination by copying and deleting.

        Parameters:
            blob (azure.storage.blob.BlobProperties): The blob object to be moved.

        Returns:
            str: A string indicating the completion of the move operation.
        """
        copy_result = self.copy_blob(blob)
        delete_result = self.delete_blob(blob)
        return f"Moved {blob.name}"

    @timer_func
    def archive_files(self):
        """
        Archives files from the source container/directory to the destination container/directory.

        This method uses multithreading to perform the move operations concurrently.
        """
        # Move blobs using multithreading
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Submit move operations to the executor
            move_futures = [executor.submit(self.move_blob, blob) for blob in self.blobs]

            # Wait for all move operations to complete
            concurrent.futures.wait(move_futures)

        empty_source_directory = self.source_blob_service_client.get_blob_client(
            container=self.source_container_name, blob=self.source_directory_name.strip("/"))
        empty_source_directory.delete_blob()

        print(f"Directory Archive Completed. Moved files from {self.source_container_name}/{self.source_directory_name} to {self.destination_container_name}/{self.destination_directory_name}/")


# Example usage
""""
blob_archiver = BlobFileArchive(
    connection_string="<redacted>",
    source_container="<source_container>",
    source_directory_name="<source/sub-directory/filepath/>",
    destination_container="<destintation_container>",
    destination_directory_name="<destination/sub-directory/filepath/>"
)
blob_archiver.archive_files()
"""

load_dotenv()

blob_archiver= BlobFileArchive(
    connection_string=os.environ["STORAGE_CONNECTION_STRING"],
    source_container="dropzone",
    source_directory_name="databricks/file_arrival_trigger/source/",
    destination_container="dropzone",
    destination_directory_name="databricks/file_arrival_trigger/destination/"
)
blob_archiver.archive_files()