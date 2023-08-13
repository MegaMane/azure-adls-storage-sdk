from azure.storage.blob import BlobServiceClient, BlobClient
import datetime
from dotenv import load_dotenv
import os

load_dotenv()

current_time = datetime.datetime.now()
time_stamp = current_time.strftime("%Y%m%d")

file_name =  f"/file_arrival_trigger/ready_{time_stamp}.txt"
print(file_name)


print("connecting to blob")
constrin = os.environ["STORAGE_CONNECTION_STRING"]
blob = BlobClient.from_connection_string(conn_str=  constrin, container_name = r"dropzone", blob_name = file_name)
exists = blob.exists()

if not exists:
    print("creating file")
    data= bytes("This is some sample text for my file.", "utf-8")
    blob.upload_blob(data)
else:
    print("file already exists")
    

