from azure.storage.filedatalake import FileSystemClient
import os

class ADSL2DataLoad:

    def __init__(self, storage_account_name, file_system_name, credential):
        account_url = f"https://{storage_account_name}.dfs.core.windows.net"
        self._file_system_name = file_system_name
        self._fs = FileSystemClient(account_url, file_system_name, credential=credential)

    def upload_file(self, local_file_path, remote_file_path, chunk_size=50*1024*1024):
        """
        If file exist will overwrite it
        
        :param Optional[int] chunk_size:
            The maximum chunk size for uploading a file in chunks. Defaults to 50*1024*1024, or 50MB.
        """
        file_client = self._fs.get_file_client(remote_file_path)
        file_client.create_file()
        with open(local_file_path, 'rb') as file:
            position = 0
            for part in iter(lambda: file.read(chunk_size), b''):
                length=len(part)
                file_client.append_data(data=part, offset=position, length=length)
                position += length
                file_client.flush_data(position)
        print(f"Uploaded: {remote_file_path}")

    def download_file(self):
        """
        Need create a iterator to not bring all data to the memory
        ex:
        file_client.download_file().chunks() # iterator, can I set chunk size?
        file_client.download_file(offset=None, length=None) # iterate like in upload function
        """
        pass