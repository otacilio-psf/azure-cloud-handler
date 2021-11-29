import requests

class KubernetesOperation():

    def __init__(self, subscription_id, credential, rg_name, aks_name):
        self._subscription_id = subscription_id
        self._token = credential.get_token("https://management.azure.com/.default").token
        self._rg_name = rg_name
        self._name = aks_name
        self._url = f"https://management.azure.com/subscriptions/{self._subscription_id}/resourceGroups/{self._rg_name}/providers/Microsoft.ContainerService/managedClusters/{self._name}"
        self._api = "?api-version=2021-05-01"
        self._headers = {'Authorization': f'Bearer {self._token}'}

    def _status(self):
        r = requests.get(self._url+self._api, headers=self._headers)
        if r.status_code == 200:
            return r.json()
        else:
            raise Exception({"status": r.status_code, "msg": r.text})  

    def status(self):
        r_json = self._status()
        status = {"provisioningState": r_json["properties"]["provisioningState"], "powerState": r_json["properties"]["powerState"]["code"]}
        return status
        
    def start(self):
        status = self.status()
        if status["provisioningState"] == "Succeeded" and status["powerState"] == "Stopped":
            r = requests.post(self._url+"/start"+self._api, headers=self._headers)
            print("Starting Cluster")
        else:
            print(status)

    def stop(self):
        status = self.status()
        if status["provisioningState"] == "Succeeded" and status["powerState"] == "Running":
            r = requests.post(self._url+"/stop"+self._api, headers=self._headers)
            print("Stopping Cluster")
        else:
            print(status)

