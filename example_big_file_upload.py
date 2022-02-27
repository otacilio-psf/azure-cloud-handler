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

file_system_ddlake = FileSystem(
    storage_account_name = "deepdivelake",
    file_system_name = "poc-teste",
    credential = credential
)

dload = ADSL2DataLoad(
    file_system = file_system_ddlake
)

if __name__ == "__main__":
    
    # file used for example
    # https://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2018.zip

    local_file_path = "microdados_enade_2018.txt"
    remote_file_path = "landing/enade/microdados_enade_2018.txt"
    dload.upload_file(local_file_path, remote_file_path)
