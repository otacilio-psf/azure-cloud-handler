from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
import time

class Azure_data_factory():
    """
    dict_credencials = {
        'client_id': '',
        'client_secret': '',
        'tenant_id': '',
    }

    dict_parameters = {
        'subscription_id': '',
        'resource_group_name': '',
        'data_factory_name': '',
        'pipeline_name': '',
        'pipeline_parameters': {}
    }
    """
    def __init__(self, dict_credencials, dict_parameters):
        self._client_id = dict_credencials['client_id']
        self._client_secret = dict_credencials['client_secret']
        self._tenant_id = dict_credencials['tenant_id']
        self._subscription_id = dict_parameters['subscription_id']
        self._resource_group_name = dict_parameters['resource_group_name']
        self._data_factory_name = dict_parameters['data_factory_name']
        self._pipeline_name = dict_parameters['pipeline_name']
        self._pipeline_parameters = dict_parameters['pipeline_parameters']

        self._run_response = None

        self._credentials = ClientSecretCredential(client_id=self._client_id,
                                                   client_secret=self._client_secret,
                                                   tenant_id=self._tenant_id)
        
        self._adf_client = DataFactoryManagementClient(self._credentials, self._subscription_id)


    def start_run(self):  
        self._run_response = self._adf_client.pipelines.create_run(resource_group_name= self._resource_group_name,
                                                                   factory_name=self._data_factory_name,
                                                                   pipeline_name=self._pipeline_name,
                                                                   parameters=self._pipeline_parameters)

    def monitoring_run(self):
        if self._run_response:
            pipeline_run = self._adf_client.pipeline_runs.get(self._resource_group_name,
                                                              self._data_factory_name,
                                                              self._run_response.run_id)
                                                              
            while pipeline_run.status == 'InProgress' or pipeline_run.status == 'Queued':
                print(f"Pipeline run status: {pipeline_run.status}")
                time.sleep(5)
                pipeline_run = self._adf_client.pipeline_runs.get(self._resource_group_name,
                                                                  self._data_factory_name,
                                                                  self._run_response.run_id)
            
            print(f"Pipeline run status: {pipeline_run.status}")

if __name__ == "__main__":
    dict_credencials = {
        'client_id': '',
        'client_secret': '',
        'tenant_id': '',
    }

    dict_parameters = {
        'subscription_id': '',
        'resource_group_name': '',
        'data_factory_name': '',
        'pipeline_name': '',
        'pipeline_parameters': {}
    }
    
    adf = Azure_data_factory(dict_credencials, dict_parameters)
    adf.start_run()
    adf.monitoring_run()
