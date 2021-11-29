from zipfile import ZipFile
from io      import BytesIO
from base64  import b64encode
import requests
import os
import time

class KubernetesCommand():

    def __init__(self, subscription_id, credential, rg_name, aks_name):
        self._subscription_id = subscription_id
        self._token = credential.get_token("https://management.azure.com/.default").token
        self._rg_name = rg_name
        self._name = aks_name
        self._url = f"https://management.azure.com/subscriptions/{self._subscription_id}/resourceGroups/{self._rg_name}/providers/Microsoft.ContainerService/managedClusters/{self._name}"
        self._api = "?api-version=2021-05-01"
        self._headers = {'Authorization': f'Bearer {self._token}'}
        self._get_kubeconfig()

    def _get_kubeconfig(self):
        r = requests.post(self._url+"/listClusterAdminCredential"+self._api, headers=self._headers)
        self._kubeconfig = [ct["value"] for ct in r.json()["kubeconfigs"] if ct["name"] == "clusterAdmin"][0]

    def _get_context(self, context_path):
        if not context_path:
            return ""
        
        zip_obj = BytesIO()
        file_paths = []

        for root, _, files in os.walk(context_path):
            for filename in files:
                filepath = os.path.join(root, filename)
                file_paths.append(filepath)

        with ZipFile(zip_obj, 'w') as zip:
            for file in file_paths:
                zip.write(file, file.replace(context_path, ""))

        zip_obj.seek(0)
        return b64encode(zip_obj.read()).decode('ascii')

    def cmd(self, command, context=None):
        body = {
        "command": command,
        "context": self._get_context(context),
        "kubeconfig": self._kubeconfig
        }

        r = requests.post(self._url+"/runCommand"+self._api, headers=self._headers, json=body)
        cmd_id = r.headers["x-ms-request-id"]

        while True:
            time.sleep(2)
            r = requests.get(self._url+f"/commandResults/{cmd_id}"+self._api, headers=self._headers)
            if r.status_code == 200:
                print(r.json()["properties"]["logs"])
                return r.json()["properties"]["logs"]

