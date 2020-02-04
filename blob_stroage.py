import os
from azure.storage.blob import BlobServiceClient
from azure.identity import ClientSecretCredential
from azure.core.exceptions import ResourceExistsError

class FileName:

    def __init__(self, filename):
        self.ext = None
        self.base_path = None
        self.filename = os.path.basename(filename)

    def parse_filename(self):
        self.base_path, self.ext = os.path.splitext(self.filename)


class LocalFile(FileName):
    local_storge_path = "/Users/dharmveer.baloda/workspace/az-transcoding-poc/downloads"
    def __init__(self, filename):
        super().__init__(filename)
        self.parse_filename()

    @property
    def absolute_path(self):
        return os.path.join(self.local_storge_path, self.filename)

# class AzureUploadFile(LocalFile):
#     def __init__(self, filename, container_name=None, blob_name=None):
#         super().__init__(filename)
#         self.container_name = container_name
#         self.blob_name = blob_name


class BlobStorage:
    URL = "https://{storage_account_name}.blob.core.windows.net/"

    def __init__(self, storage_account_name, access_key):
        self.access_key = access_key
        self.storage_account_name = storage_account_name
        self._blob_service_client = None
        self.blob_service_client()

    @property
    def resource_url(self):
        return self.URL.format(storage_account_name=self.storage_account_name)

    def blob_service_client(self):
        if not self._blob_service_client:
            self._blob_service_client = BlobServiceClient(
                account_url=self.resource_url,
                credential=self.access_key
            )
        return self._blob_service_client

    def list_containers(self):
        return [
            container
            for container in self._blob_service_client.list_containers()
        ]

    def container_client(self, container_name):
        client = None
        try:
            client = self._blob_service_client.get_container_client(
                container_name
            )
        except Exception as exception:
            raise exception
        return client

    def blob_client(self, container_name, blob_name):
        client = None
        try:
            client = self._blob_service_client.get_blob_client(
                container_name, blob_name
            )
        except Exception as exception:
            raise exception
        return client

    def delete_blob(self, container_name, blob_name):
        try:
            self.blob_client(container_name, blob_name).delete_blob()
        except Exception as exception:
            raise exception

    def download(self, container_name, blob_name, filename=None):
        filename = filename or blob_name
        blob_client = self.blob_client(container_name, blob_name)
        local_file = LocalFile(filename=filename)
        with open(local_file.absolute_path, "wb") as f:
            download_stream = blob_client.download_blob()
            f.write(download_stream.readall())

        return blob_client

    def upload(self, container_name, filename, blob_name=None):
        local_file = filename
        if not isinstance(filename, LocalFile):
            local_file = LocalFile(filename=filename)

        blob_name = blob_name or local_file.filename

        container_client = self.container_client(
            container_name=container_name
        )
        try:
            with open(local_file.absolute_path, "rb") as data:
                blob_client = container_client.upload_blob(
                    name=local_file.filename, data=data
                )

        except ResourceExistsError as identifier:
            raise identifier
        except Exception as identifier:
            raise identifier

        return self.blob_client(
            container_name=container_name,
            blob_name=local_file.filename
        )

