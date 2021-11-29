from azure.identity import ClientSecretCredential
from azurecloudhandler.aks import KubernetesCommand
import os
import time
import yaml
from dotenv import load_dotenv
load_dotenv()

credential = ClientSecretCredential(
    client_id = os.getenv("AZURE_CLIENT_ID"),
    client_secret = os.getenv("AZURE_CLIENT_SECRET"),
    tenant_id = os.getenv("AZURE_TENANT_ID")
)

subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")

kcmd = KubernetesCommand(
    subscription_id = subscription_id,
    credential = credential,
    rg_name = "rg-poc-aks",
    aks_name = "aks-poc"
)

if __name__ == "__main__":

    with open('example.yaml', 'r') as yaml_:
        y=yaml.safe_load(yaml_)

    # if any change is need 'y' will be a dict
    y_str = yaml.dump(y)

    command1 = f'cat <<EOF | kubectl replace --force -f - \n{y_str}EOF'
    kcmd.cmd(command=command1)

    command2 = f'kubectl get pods'
    kcmd.cmd(command=command2)

    command3 = f'kubectl logs teste-pod-00000000'
    kcmd.cmd(command=command3)