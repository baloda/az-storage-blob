from blob_storage import BlobStorage


storage_account_name = "xxxxxxxxxxxxxxxxxxx"
access_key = "xxxmlzd3aPNMnXSgQwMRhIHTUu3rSs0wfEXMpw6OW0HG5ba2tz04/6dftVHdy1zNpwnTOWu7QjzH/wrpRdhFFg=="

container_name = "transcoder-poc"
blob_name="datascience-Azure.png"
filename="datascience-Azure-testing.png"

az_storage = BlobStorage(
    storage_account_name=storage_account_name,
    access_key=access_key,
    container_name=container_name,
    blob_name="datascience-Azure-testing.png"
)

# containers = az_storage.list_containers()

blob_properties = az_storage.blob_properties()
print(blob_properties)
# print("**********************************************************")
az_storage.download()
# az_storage.upload(filename)
