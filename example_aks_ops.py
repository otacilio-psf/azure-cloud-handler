from azure.identity import ClientSecretCredential
from azurecloudhandler.aks import KubernetesOperation
import os
import time
from dotenv import load_dotenv
load_dotenv()

credential = ClientSecretCredential(
    client_id = os.getenv("AZURE_CLIENT_ID"),
    client_secret = os.getenv("AZURE_CLIENT_SECRET"),
    tenant_id = os.getenv("AZURE_TENANT_ID")
)

subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")

kops = KubernetesOperation(
    subscription_id = subscription_id,
    credential = credential,
    rg_name = "rg-poc-aks",
    aks_name = "aks-poc"
)

if __name__ == "__main__":

    # Stopping Cluster

    kops.stop()
    status = kops.status()
    print(status)

    while status["provisioningState"] != "Succeeded":
        time.sleep(10)
        status = kops.status()
        print(status)
    
    # Starting Cluster

    kops.start()
    status = kops.status()
    print(status)

    while status["provisioningState"] != "Succeeded":
        time.sleep(10)
        status = kops.status()
        print(status)
