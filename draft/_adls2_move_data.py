from azure.storage.filedatalake import FileSystemClient
import os

class FileSystem:

    def __init__(self, storage_account_name, file_system_name, credential):
        account_url = f"https://{storage_account_name}.dfs.core.windows.net"
        self.name = file_system_name
        self.fs = FileSystemClient(account_url, file_system_name, credential=credential)


class ADSL2DataMove:

    def __init__(self):
        pass

    def upload_file(self, source_file_system:FileSystem, source_file_path, target_file_system:FileSystem, target_file_path):
        """
        If the target file exist will overwrite it
        """
        source_file_client = source_file_system.fs.get_file_client(source_file_path)
        target_file_client = target_file_system.fs.get_file_client(target_file_path)
        target_file_client.create_file()
        
        source_file_iterator = source_file_client.download_file().chunks()

        position = 0
        for part in source_file_iterator:
            length=len(part)
            target_file_client.append_data(data=part, offset=position, length=length)
            position += length
            target_file_client.flush_data(position)

        print(f"Data moved")
