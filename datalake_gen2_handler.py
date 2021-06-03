from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential
from os import listdir, makedirs

class Datalake_handler():
    """
    connection_method: It can be through a connection string or a credential
    
    connection_method = {
        'method': 'connection_string',
        'connection_string': ''
    }
    
    or
    
    connection_method = {
        'method': 'credential',
        'tenant_id': '',
        'client_id': '',
        'client_secret': '',
        'storage_account_name': ''
    }

    container: name of container that sit the file system
    """
    def __init__(self, connection_method, container):
        if connection_method['method'] == 'credential':
            credential = ClientSecretCredential(connection_method['tenant_id'],
                                                connection_method['client_id'],
                                                connection_method['client_secret'])
            account_url = f"https://{connection_method['storage_account_name']}.dfs.core.windows.net"
            self._adls = DataLakeServiceClient(account_url=account_url, credential=credential)
        elif connection_method['method'] == 'connection_string':
            self._adls = DataLakeServiceClient.from_connection_string(connection_method['connection_string'])
        else:
            raise Exception('This method is not available')
        try:
            self._fs = self._adls.get_file_system_client(file_system=container)
        except Exception as e:
            print(e)
        self._container = container
  
    def ls(self, path=''):
        """
        return a list with PathProperties objects
        Variables: name, owner, group, permissions, last_modified, is_directory, etag, content_length
        """
        try:
            return [path for path in self._fs.get_paths(path=path)]
        except Exception as e:
            print(e)

    def print_ls(self, path=''):
        try:
            paths = self._fs.get_paths(path=path)
            print("Directory | Path")
            for path in paths:
                print(str(path.is_directory).ljust(8, " "), " |", path.name)
        except Exception as e:
            print(e)

    def create_dir(self, dir_path):
        try:
            self._fs.create_directory(dir_path)
        except Exception as e:
            print(e)

    def set_dir(self, dir_path):
        try:
            self._dir = self._fs.get_directory_client(dir_path)
        except Exception as e:
            print(e)
    
    def move_or_rename_dir(self, dir_path, new_path):
        try: 
            self._fs.get_directory_client(dir_path).rename_directory(new_path)
        except Exception as e:
            print(e)
    
    def upload_dir(self, local_dir_path, remote_dir_path, dir_scope=False):
        for root, _, files in os.walk(local_dir_path):
            for name in files:
                local_file_path = os.path.join(root, name)
                file_name = local_file_path.replace(local_dir_path + os.sep,'')
                self.upload_file(local_file_path,
                                 f'{remote_dir_path}/{file_name}',
                                 dir_scope)
    
    def download_dir(self, remote_dir_path, local_dir_path):
        files_download = [f.name for f in self.ls(remote_dir_path) if f.is_directory==False]
        for f in files_download:
            local_sub_dir = f.replace(remote_dir_path,'').replace('/'+f.split('/')[-1], '')
            self.download_file(f, f'{local_dir_path}{local_sub_dir}')
        
    def upload_file(self, local_file_path, remote_file_path, dir_scope=False):
        if dir_scope: scope = self._dir
        else: scope = self._fs
        try: 
            file_client = scope.get_file_client(remote_file_path)
            with open(local_file_path,'r') as local_file:
                file_contents = local_file.read()
                file_client.upload_data(file_contents, overwrite=True)
        except Exception as e:
            print(e)

    def download_file(self, remote_file_path, local_dir_path, dir_scope=False):
        if dir_scope: scope = self._dir
        else: scope = self._fs
        try:
            file_client = scope.get_file_client(remote_file_path)
            makedirs(f'{local_dir_path}', exist_ok=True)
            file_name = remote_file_path.split('/')[-1]
            local_file_path = f'{local_dir_path}/{file_name}'
            with open(local_file_path,'wb') as local_file:
                downloaded_bytes = file_client.download_file().readall()
                local_file.write(downloaded_bytes)
        except Exception as e:
            print(e)

    def delete_dir(self, dir_path=''):
        """
        dir_path can be the root path of the dir or a sub dir
        this function is aways dir scope
        """
        try: 
            self._dir.delete_sub_directory(dir_path)
        except Exception as e:
            print(e)

    def delete_file(self, file_path, dir_scope=False):
        if dir_scope: scope = self._dir
        else: scope = self._fs
        try:
            scope.get_file_client(file_path).delete_file()
        except Exception as e:
            print(e)

    def rename_file(self, remote_file_path, new_name, dir_scope=False):
        if dir_scope: scope = self._dir
        else: scope = self._fs
        try:
            file_client = scope.get_file_client(remote_file_path)
            file_client.rename_file(f'{self._container}/{new_name}')
        except Exception as e:
            print(e)
            
if __name__ == "__main__":
    import os

    connection_method = {
        'method': 'credential',
        'tenant_id': '',
        'client_id': '',
        'client_secret': '',
        'storage_account_name': ''
    }

    container = ''

    adlfs = Datalake_handler(connection_method, container)

    adlfs.print_ls()