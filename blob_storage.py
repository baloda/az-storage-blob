import os
from azure.storage.blob import BlobServiceClient
from azure.storage.blob import BlobProperties
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
    local_storge_path = "/Users/dharmveer.baloda/workspace/az-transcoding-poc/az-storage-blob/downloads"
    def __init__(self, filename):
        super().__init__(filename)
        self.parse_filename()
        self._size = 0

    @property
    def absolute_path(self):
        return os.path.join(self.local_storge_path, self.filename)

    @property
    def size(self):
        if self._ssize:
            return self.size
        try:
            self._size = os.stat(self.path).st_size
        except Exception as exception:
            pass
        return self._size

    def delete(self):
        os.remove(self.filename)


# class AzureUploadFile(LocalFile):
#     def __init__(self, filename, container_name=None, blob_name=None):
#         super().__init__(filename)
#         self.container_name = container_name
#         self.blob_name = blob_name

class AzAuth:
    URL = "https://{storage_account_name}.blob.core.windows.net/"
    def __init__(self, storage_account_name, access_key):
        self.storage_account_name = storage_account_name
        self.access_key = access_key

    @property
    def resource_url(self):
        return self.URL.format(storage_account_name=self.storage_account_name)

class AzStorage(AzAuth):
    def __init__(self, storage_account_name, access_key):
        super().__init__(storage_account_name, access_key)
        self._blob_service_client = None

    def blob_service_client(self):
        if not self._blob_service_client:
            self._blob_service_client = BlobServiceClient(
                account_url=self.resource_url,
                credential=self.access_key
            )
        return self._blob_service_client

class AzBlobStorageContainer(AzStorage):
    def __init__(self, storage_account_name, access_key, container_name):
        super().__init__(storage_account_name, access_key)
        self.container_name = container_name

    def blob_container_client(self):
        blob_service_client= self.blob_service_client()
        return blob_service_client.get_container_client(
            self.container_name
        )

class AzBlobStorage(AzBlobStorageContainer):
    def __init__(self, storage_account_name, access_key, container_name=None, blob_name=None):
        super().__init__(storage_account_name, access_key, container_name)

        self.blob_name = blob_name
        self._blob_client = None
        self.properties = {}
        self.blob_properties()

    def blob_client(self):
        if not self._blob_client:
            try:
                self._blob_client = self.blob_service_client().get_blob_client(
                    self.container_name, self.blob_name
                )
            except Exception as exception:
                raise exception
        return self._blob_client

    def blob_properties(self):
        if not self.properties:
            try:
                self.properties = self.blob_client().get_blob_properties()
            except Exception as exception:
                pass
        return self.properties

    def delete_blob(self, container_name, blob_name):
        try:
            self.blob_client(container_name, blob_name).delete_blob()
        except Exception as exception:
            raise exception

    def to_local_file(self,filename):
        filename = filename or self.blob_name
        if not isinstance(filename, LocalFile):
            local_file = LocalFile(filename=filename)
        local_file = LocalFile(filename=filename)
        return local_file

    def download(self, filename=None):
        local_file = self.to_local_file(filename=filename)
        blob_client = self.blob_client()
        filesize = round(self.properties.get("size", 0)/1024)
        print(
            "{} {} size {} Kbps".format(
                self.container_name, self.blob_name, filesize
            )
        )
        dowloaded_filesize = 0
        with open(local_file.absolute_path, "wb") as f:
            dowloaded_filesize += blob_client.download_blob().readinto(f)

            print(
                "{} {} downloaded {}%".format(
                    self.container_name, self.blob_name, round(round(dowloaded_filesize/1024)/filesize)*100
                )
            )

    def upload(self, filename):
        local_file=self.to_local_file(filename=filename)
        try:
            print(local_file.absolute_path)
            blob_client = self.blob_client()
            with open(local_file.absolute_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            self.blob_properties()
        except ResourceExistsError as identifier:
            raise identifier
        except Exception as identifier:
            raise identifier

