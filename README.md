# Azure Cloud Handler

Library to optimally handle some resources on Azure

## libs

### Azure Kubernetes Service (aks) - doesn't yet exist in the official Azure SDK

Functionalities

* status of cluster
* start cluster
* stop cluster
* commands - kubectl

### Azure Data Lake Storage Gen2 (adls2) - datalake_gen2

Functionalities

* upload file in chunks
    * avoid memory overhead for big files
