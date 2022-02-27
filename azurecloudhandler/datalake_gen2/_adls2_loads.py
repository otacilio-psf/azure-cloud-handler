from azure.storage.filedatalake import FileSystemClient
import os

class FileSystem:

    def __init__(self, storage_account_name, file_system_name, credential):
        account_url = f"https://{storage_account_name}.dfs.core.windows.net"
        self.name = file_system_name
        self.fs = FileSystemClient(account_url, file_system_name, credential=credential)

class ADSL2DataLoad:

    def __init__(self, file_system:FileSystem):
        self._file_system_name = file_system.name
        self._fs = file_system.fs

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

    def move_file(self, source_file_path, target_file_system:FileSystem, target_file_path):
        """
        If the target file exist will overwrite it
        """
        source_file_client = self._fs.get_file_client(source_file_path)
        target_file_client = target_file_system.fs.get_file_client(target_file_path)
        target_file_client.create_file()
        
        source_file_iterator = source_file_client.download_file(length=5*1024*1024).chunks()

        position = 0
        for part in source_file_iterator:
            length=len(part)
            target_file_client.append_data(data=part, offset=position, length=length)
            position += length
            target_file_client.flush_data(position)

        print(f"Data moved")
