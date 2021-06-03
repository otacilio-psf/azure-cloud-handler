from azure.storage.blob import BlobClient

class Blob_handler():

    def __init__(self, connection_string, container_name, blob_name):
        self._conn_str = connection_string
        self._cont_name = container_name
        self._blob_name = blob_name
        self._blob = BlobClient.from_connection_string(conn_str=self._conn_str,
                                                       container_name=self._cont_name,
                                                       blob_name=self._blob_name)

    def delete_blob(self):
        try: self._blob.delete_blob()
        except: pass
    
    def write_blob(self, data):
        self._blob.upload_blob(data)
    
    def read_blob(self):
        return self._blob.download_blob().readall().decode("utf-8")
