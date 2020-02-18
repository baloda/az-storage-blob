from datetime import datetime, timedelta
from blob_storage import AzBlobStorage
from argparse import ArgumentParser
from constants import *

parser = ArgumentParser(prog='azure file download/upload')
parser.add_argument('--blob_name', required=True, help="Give blob relative full path to container")
parser.add_argument('--container_name', required=True, help="Container name is required")
args = parser.parse_args()
container_name = args.blob_name
blob_name = args.blob_name

az_storage = AzBlobStorage(
    storage_account_name=storage_account_name,
    access_key=access_key,
    container_name=container_name,
    blob_name=blob_name
)

# containers = az_storage.list_containers()
blob_properties = az_storage.blob_properties()
print(blob_properties)
started_time = datetime.now()
# print("**********************************************************")

az_storage.download()
# az_storage.upload()

completed_time = datetime.now()
print(completed_time, started_time)
print(
    "total time taken {}/{} to download is".format(container_name, blob_name), completed_time-started_time
)
