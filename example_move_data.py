from azure.identity import ClientSecretCredential
from azurecloudhandler.datalake_gen2 import FileSystem, ADSL2DataLoad
import os
from dotenv import load_dotenv
load_dotenv()

credential = ClientSecretCredential(
    client_id = os.getenv("AZURE_CLIENT_ID"),
    client_secret = os.getenv("AZURE_CLIENT_SECRET"),
    tenant_id = os.getenv("AZURE_TENANT_ID")
)

credential_2 = ClientSecretCredential(
    client_id = os.getenv("AZURE_CLIENT_ID_2"),
    client_secret = os.getenv("AZURE_CLIENT_SECRET_2"),
    tenant_id = os.getenv("AZURE_TENANT_ID")
)

source_fs = FileSystem(
    storage_account_name = "otaciliofilhodev",
    file_system_name = "data-lake",
    credential = credential
)

target_fs = FileSystem(
    storage_account_name = "otaciliofilhoprod",
    file_system_name = "data-lake",
    credential = credential_2
)

dload = ADSL2DataLoad(
    file_system = source_fs
)

if __name__ == "__main__":
    
    # moving metadata.csv from dif env

    source_file_path = "metada_files/metadata.csv"
    target_file_path = source_file_path
    dload.move_file(source_file_path, target_fs, target_file_path)
