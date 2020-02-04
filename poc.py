from blob_stroage import BlobStorage


storage_account_name = "xxxxxxxxxxxxxxxxxxx"
access_key = "xxxmlzd3aPNMnXSgQwMRhIHTUu3rSs0wfEXMpw6OW0HG5ba2tz04/6dftVHdy1zNpwnTOWu7QjzH/wrpRdhFFg=="

az_storage = BlobStorage(
    storage_account_name=storage_account_name,
    access_key=access_key
)

containers = az_storage.list_containers()
container_name = "transcoder-poc"
az_storage.download(container_name=container_name, blob_name="datascience-Azure.png")
az_storage.upload(container_name=container_name, filename="datascience-Azure_upload.png")


print(containers)