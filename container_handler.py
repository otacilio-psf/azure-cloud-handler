from azure.storage.blob import ContainerClient

class Container_handler():

    def __init__(self, connection_string, container_name):
        self._connection_string = connection_string
        self._container_name = container_name
        self._container = ContainerClient.from_connection_string(self._connection_string,
                                                                 self._container_name)

    def delete_blobs(self, blobs_list):
        self._container.delete_blobs(blobs_list)
    
    def download_blob(self, blob):
        return self._container.download_blob(blob).readall()
    
    def download_blob_as_txt(self, blob):
        return self._container.download_blob(blob).readall().decode("utf-8")
    
    def upload_blob(self, blob, data):
        self._container.upload_blob(blob, data)

    def get_blobs_list(self, name_starts_with=''):
         return [i for i in self._container.list_blobs(name_starts_with=name_starts_with)]

    def get_parameter_list(self, parameter, name_starts_with=''):
        blobs_list = [i for i in self._container.list_blobs(name_starts_with=name_starts_with)]
        return [blob[parameter] for blob in blobs_list]


if __name__ == "__main__":
    connection_string = ""
    container_name = ""

    container = ContainerClient.from_connection_string(connection_string, container_name)
