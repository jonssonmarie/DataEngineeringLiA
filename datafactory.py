# Prerequisites in Azure Portal
# a subscription
# a resource group
# a Key Vault
# an application i Azure Active directory
"""
from azure.core.exceptions import (
    ClientAuthenticationError,
    HttpResponseError,
    ServiceRequestError,
    ResourceNotFoundError,
    AzureError
)

try:
    # do KV stuff
except ClientAuthenticationError as e:
    # Can occur if either tenant_id, client_id or client_secret is incorrect
    logger.critical("Azure SDK was not able to connect to Key Vault", e)
except HttpResponseError as e:
    # One reason is when Key Vault Name is incorrect
    logger.critical("Possible wrong Vault name given", e)
except ServiceRequestError:
    # Network error, I will let it raise to higher level
    raise
except ResourceNotFoundError:
    # Let's assume it's not big deal here, just let it go
    pass
except AzureError as e:
    # Will catch everything that is from Azure SDK, but not the two previous
    logger.critical("Azure SDK was not able to deal with my query", e)
    raise
except Exception as e:
    # Anything else that is not Azure related (network, stdlib, etc.)
    logger.critical("Unknown error I can't blame Azure for", e)
    raise
"""
import pandas as pd
from azure.common.credentials import ServicePrincipalCredentials
from azure.identity import ClientSecretCredential, DefaultAzureCredential, InteractiveBrowserCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import Factory, SecureString, LinkedServiceResource, LinkedServiceReference, \
    PipelineResource, DatasetResource, AzureStorageLinkedService, AzureBlobDataset, CopyActivity, \
    DatasetReference, BlobSink, BlobSource, RunFilterParameters, TextFormat, JsonFormat
from azure.identity import AzureCliCredential
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.storage.models import StorageAccountCreateParameters, Sku
from azure.storage.blob import BlobClient, BlobServiceClient
from azure.keyvault.secrets import SecretClient
from azure.schemaregistry import SchemaRegistryClient, SchemaFormat
from azure.core.exceptions import ClientAuthenticationError, HttpResponseError, ServiceRequestError, \
    ResourceNotFoundError, AzureError, ResourceExistsError

from datetime import datetime, timedelta
import time
import os, random, json
from dotenv import load_dotenv

load_dotenv()


def print_item(group):
    """Print an Azure object instance."""
    print("\tName: {}".format(group.name))
    print("\tId: {}".format(group.id))
    if hasattr(group, 'location'):
        print("\tLocation: {}".format(group.location))
    if hasattr(group, 'tags'):
        print("\tTags: {}".format(group.tags))
    if hasattr(group, 'properties'):
        print_properties(group.properties)


def print_properties(props):
    """Print a ResourceGroup properties instance."""
    if props and hasattr(props, 'provisioning_state') and props.provisioning_state:
        print("\tProperties:")
        print("\t\tProvisioning State: {}".format(props.provisioning_state))
    print("\n\n")


def print_activity_run_details(activity_run):
    """Print activity run details."""
    print("\n\tActivity run details\n")
    print("\tActivity run status: {}".format(activity_run.status))
    if activity_run.status == 'Succeeded':
        print("\tNumber of bytes read: {}".format(activity_run.output['dataRead']))
        print("\tNumber of bytes written: {}".format(activity_run.output['dataWritten']))
        print("\tCopy duration: {}".format(activity_run.output['copyDuration']))
    else:
        print("\tErrors: {}".format(activity_run.error['message']))


def create_storage_account(storage_client, resource_group_name, storage_account_name, location):
    # Creates a storage account
    try:
        response = storage_client.storage_accounts.begin_create(
            resource_group_name,
            storage_account_name,
            StorageAccountCreateParameters(
                sku=Sku(name="Standard_LRS"),
                kind="StorageV2",
                location=location,
                enable_https_traffic_only=True, ), )

        storage_account = response.result()
        print(f"Storage account {storage_account.name} provision state is {storage_account.provisioning_state}")
    except ResourceExistsError:
        print(f"Storage account already exist")


def retrieve_storage_account_access_key(storage_client, resource_group_name, storage_account_name):
    # Retrieve the storage account's primary access key and generate a connection string.
    try:
        keys = storage_client.storage_accounts.list_keys(resource_group_name, storage_account_name)
        print(f"Primary key for storage account: {keys.keys[0].value}")

        connection_string = f"DefaultEndpointsProtocol=https;" \
                            f"EndpointSuffix=core.windows.net;" \
                            f"AccountName={storage_account_name};" \
                            f"AccountKey={keys.keys[0].value}"

        print(f"Connection string: {connection_string}")
        return connection_string
    except ResourceNotFoundError:
        print("Not possible to create connection string, storage account is missing")


def create_blob_container(resource_group_name, storage_client, storage_account_name, container_name):
    try:
        container = storage_client.blob_containers.create(resource_group_name, storage_account_name, container_name,
                                                          {})
        print(f"Provisioned blob container {container.name}")
    except ResourceExistsError:
        print("blob container name already exist")

    # The fourth argument is a required BlobContainer object, but because we don't need any
    # special values there, so we just pass empty JSON.


def upload_file_to_blob(connection_str, container_name, path_to_file, blob_file_name):
    # Open a local file and upload its contents to Blob Storage
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_str)
        blob_client = blob_service_client.get_blob_client(container=container_name,
                                                          blob=blob_file_name)  # blob = vad filen ska heta i blob

        # with open(path_to_file, "rb") as data:
        df = pd.read_csv(path_to_file, delimiter=";")
        output = df.to_csv(index=False, encoding="utf-8")
        blob_client.upload_blob(data=output, blob_type="BlockBlob", encoding="utf-8", overwrite=True)
    except ResourceExistsError:
        print("File already exist in blob container")


def create_data_factory(adf_client, location_df, rg_name, df_name):
    try:
        df_resource = Factory(location=location_df)

        df = adf_client.factories.create_or_update(rg_name, df_name, df_resource)
        print_item(df)

        while df.provisioning_state != 'Succeeded':
            df = adf_client.factories.get(rg_name, df_name)
        time.sleep(1)
    except ResourceExistsError:
        print("Data Factory already exist")


def create_data_flow(client, resource_group_name, factory_name, data_flow_name, data_flow):
    response = client.data_flows.create_or_update(
        resource_group_name=resource_group_name,
        factory_name=factory_name,
        data_flow_name=data_flow_name,
        data_flow=data_flow)
    print("create_data_flow: ", response)


def create_storage_linked_service(storage_string, adf_client, rg_name, df_name, ls_name):
    ls_azure_storage = LinkedServiceResource(properties=AzureStorageLinkedService(connection_string=storage_string))
    # ,
    #                                                                                   type="delimitedText" verkar fungera utan
    try:
        ls = adf_client.linked_services.create_or_update(rg_name, df_name, ls_name, ls_azure_storage,
                                                         content_type='application/json')  # testar content_type: str = 'application/json', verkar fungera
        print("create_storage_linked_service:\n")
        print_item(ls)
    except ResourceExistsError:
        print("linked service exist")


def integration_run_time(adf_client, resource_group_name, factory_name, integration_runtime_name):
    response = adf_client.integration_runtimes.create_or_update(
        resource_group_name=resource_group_name,
        factory_name=factory_name,
        integration_runtime_name=integration_runtime_name,
        integration_runtime={"properties": {"description": "A selfhosted integration runtime", "type": "SelfHosted"}},
    )
    print("integration_run_time:", response)


def create_copy_activity(copy_activity, adf_client, rg_name, df_name, p_name, params_for_pipeline):
    """ Create a pipeline with the copy activity
     Note 1: To pass parameters to the pipeline, add them to the json string params_for_pipeline shown
     below in the format { “ParameterName1” : “ParameterValue1” } for each of the parameters needed in the pipeline.

     Note 2: To pass parameters to a dataflow, create a pipeline parameter to hold the parameter name/value,
     and then consume the pipeline parameter in the dataflow parameter in the format
     @pipeline().parameters.parametername."""
    try:
        p_obj = PipelineResource(activities=[copy_activity], parameters=params_for_pipeline)
        p = adf_client.pipelines.create_or_update(rg_name, df_name, p_name, p_obj)
        print_item(p)
    except ResourceExistsError:
        print("Pipeline resource copy activity already exist")


# https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/schemaregistry/azure-schemaregistry/samples/sync_samples/schema_registry.py
def register_schema(client, group_name, name, definition, format):
    print("Registering Schema...")
    schema_properties = client.register_schema(group_name, name, definition, format)
    print(f"Schema registered, returned schema id is {schema_properties.id}\n")
    print(f"Schema properties are {schema_properties}\n")
    return schema_properties


def get_schema_by_id(client, schema_id):
    print("Getting schema by id...")
    schema = client.get_schema(schema_id)
    print(f"The schema string of schema id: {schema_id} is {schema.definition}")
    print(f"Schema properties are {schema.properties}")
    return schema.definition


def monitor_pipline_run(adf_client, rg_name, df_name, run_response):
    # Monitor the pipeline run
    time.sleep(30)
    try:
        pipeline_run = adf_client.pipeline_runs.get(rg_name, df_name, run_response.run_id)
        print("\n\tPipeline run status: {}".format(pipeline_run.status))

        filter_params = RunFilterParameters(last_updated_after=datetime.now() - timedelta(1),
                                            last_updated_before=datetime.now() + timedelta(1))

        query_response = adf_client.activity_runs.query_by_pipeline_run(rg_name, df_name, pipeline_run.run_id,
                                                                        filter_params)
        print_activity_run_details(query_response.value[0])
    except ResourceExistsError:
        print("monitor resource already exist for pipeline run")


def delete_resources_adf(adf_client, df_name, storage_account_name, blob_container_name, ls_azure_storage, ls_name):
    adf_client.factories.delete(df_name, storage_account_name, blob_container_name, ls_azure_storage, ls_name)


def main():
    # Step 1: Create access to Azure
    # Azure subscription ID
    subscription_id = os.environ.get('SUBSCRIPTION_ID')
    # https://medium.com/@tophamcherie/creating-an-azure-key-vault-key-vault-secrets-2775b38979ff

    # Grabbing environment variables from the .env file
    KVUri = os.getenv('KEY_VAULT_URI')
    client_id = os.environ.get('AZURE_CLIENT_ID')
    client_secret = os.environ.get('AZURE_CLIENT_SECRET')  # 2nd way to connect
    tenant_id = os.environ.get('AZURE_TENANT_ID')
    # MI_CLIENT_ID = os.getenv('MI_CLIENT_ID')  # 3rd way to connect.
    # Managed Identity — probably the most popular credential that is used because it doesn’t
    # require password management or much administration.

    # Creating a variable to store the key vault value in
    MEDIUM_TOKEN = 'medium-token'

    # not so programmatically, only for testing right now
    # This section of code authenticates to an Azure Key Vault Using Interactive Browser Credentials
    credential = InteractiveBrowserCredential(additionally_allowed_tenants=['*'])
    client = SecretClient(vault_url=KVUri, credential=credential)

    # Lastly the secret is retrieved from the key vault using get_secret
    get_secret = client.get_secret(MEDIUM_TOKEN).value
    print(get_secret)

    # to test with Kokchun
    # Service Principal — is my favorite way to connect to the platform but it requires some additional set up.
    # This section of code authenticates to an Azure Key Vault Using the Client Secret Credentials
    """credentials = ClientSecretCredential(client_id=client_id, client_secret=client_secret, tenant_id=tenant_id)
    client = SecretClient(vault_url=KVUri, credential=credentials)"""

    # The resource group name. It must be globally unique
    rg_name = 'data_engineering_explorations'  # '<resource group name>'
    rg_location = "West Europe"

    # The data factory name. It must be globally unique.
    df_name = 'dataengexpfactory21'  # '<factory name>'

    # test senare
    # Obtain the management object for resources.
    # resource_client = ResourceManagementClient(credential, subscription_id)

    # Constants we need in multiple places: the resource group name and the region in which we provision resources.
    location_storage = 'West Europe'

    # Step 2: Provision the storage account, starting with a management object.
    storage_client = StorageManagementClient(credential, subscription_id)
    # https://learn.microsoft.com/en-us/azure/developer/python/sdk/examples/azure-sdk-example-storage?tabs=cmd

    storage_account_name = f"storageaccountdataengexp"
    create_storage_account(storage_client, rg_name, storage_account_name, location_storage)

    # Step 3: Retrieve the storage account's primary access key and generate a connection string.
    connection_string = retrieve_storage_account_access_key(storage_client, rg_name, storage_account_name)

    # Step 4: Provision the blob container in the account (this call is synchronous)
    blob_container_name = "dataengexp-blob-container-01"  # https://storageaccountdataengexp.blob.core.windows.net/dataengexp-blob-container-01/cardio_train.csv
    create_blob_container(rg_name, storage_client, storage_account_name, blob_container_name)

    # Step 5: upload file to blob container
    blob_file_name = "cardio_train.csv"
    upload_file_to_blob(connection_string, blob_container_name, "data/cardio_train.csv", blob_file_name)

    # Step 6: Create DataFactory
    location_df = "West Europe"  # "West Europe"
    adf_client = DataFactoryManagementClient(credential, subscription_id)
    create_data_factory(adf_client, location_df, rg_name, df_name)

    # Step 7: Create an Azure Storage linked service
    ls_name = 'storageLinkedService001'

    # IMPORTANT: specify the name and key of your Azure Storage account.
    storage_string = connection_string
    keys = storage_client.storage_accounts.list_keys(rg_name, storage_account_name)
    # print(f"Primary key for storage account: {keys.keys[0].value}")

    conn_string = f"DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName={storage_account_name};" \
                  f"AccountKey={keys.keys[0].value}"
    # print(f"Connection string: {conn_string}")

    create_storage_linked_service(conn_string, adf_client, rg_name, df_name, ls_name)

    ls_azure_storage = LinkedServiceResource(properties=AzureStorageLinkedService(connection_string=conn_string))

    ls = adf_client.linked_services.create_or_update(rg_name, df_name, ls_name, ls_azure_storage)
    print("Create an Azure Storage linked service:")
    print_item(ls)
    integration_run_time(adf_client, rg_name, df_name, integration_runtime_name="cardioIntegrationRuntime")

    # Step 8: Create an Azure blob dataset (input) for source
    ds_name = 'cardioIn'
    ds_ls = LinkedServiceReference(reference_name=ls_name)
    blob_input_path = "dataengexp-blob-container-01"  # blob_container_name  # '<container>/<folder path>'

    piplinecreatejsonex = {
        "properties": {
            "activities": [
                {
                    "name": "ExampleForeachActivity",
                    "type": "ForEach",
                    "typeProperties": {
                        "activities": [
                            {
                                "inputs": [
                                    {
                                        "parameters": {
                                            "MyFileName": "cardio_train.csv",
                                            "MyFolderPath": "examplecontainer",
                                        },
                                        "referenceName": "exampleDataset",
                                        "type": "DatasetReference",
                                    }
                                ],
                                "name": "ExampleCopyActivity",
                                "outputs": [
                                    {
                                        "parameters": {
                                            "MyFileName": {"type": "Expression", "value": "@item()"},
                                            "MyFolderPath": "dataengexp-blob-container-01",
                                        },
                                        "referenceName": "exampleDataset",
                                        "type": "DatasetReference",
                                    }
                                ],
                                "type": "Copy",
                                "typeProperties": {
                                    "dataIntegrationUnits": 32,
                                    "sink": {"type": "BlobSink"},
                                    "source": {"type": "BlobSource"},
                                },
                            }
                        ],
                        "isSequential": True,
                        "items": {"type": "Expression", "value": "@pipeline().parameters.OutputBlobNameList"},
                    },
                }
            ],
            "parameters": {"JobId": {"type": "String"}, "OutputBlobNameList": {"type": "Array"}},
            "policy": {"elapsedTimeMetric": {"duration": "0.00:10:00"}},
            "runDimensions": {"JobId": {"type": "Expression", "value": "@pipeline().parameters.JobId"}},
            "variables": {"TestVariableArray": {"type": "Array"}},
        }
    }

    dataset_json = {
        "properties": {
            "type": "TextFormat",
            "linkedServiceName": {
                "referenceName": "storageLinkedService001",
                "type": "LinkedServiceReference"
            },
            "activities": [{
                "typeProperties": {
                    "type": "Copy",
                    "copyBehavior": "PreserveHierarchy",
                    "translator": {
                        "type": "TabularTranslator",
                        "columnMappings": {
                            "id": "id as int",
                            "age": "age as int",
                            "gender": "gender as varchar",
                            "height": "height as int",
                            "weight": "weight as int",
                            "ap_hi": "ap_hi as int",
                            "ap_lo": "ap_lo as int",
                            "cholesterol": "cholesterol as int",
                            "gluc": "gluc as int",
                            "smoke": "smoke as int",
                            "alco": "alco as int",
                            "active": "active as int",
                            "cardio": "cardio as int"
                        }},
                    "columnDelimiter": ",",
                    "rowDelimiter": "\n",
                    "escapeChar": "\\",
                    "null_value": "\\N",
                    "encoding_name": "UTF-8",
                    "treat_empty_as_null": True,
                    "skip_line_count": 0,
                    "first_row_as_header": True
                }}],
            "schema": [
                {"name": "id",
                 "type": "int"},
                {"name": "age",
                 "type": "int"},
                {"name": "gender",
                 "type": "int"},
                {"name": "height",
                 "type": "int"},
                {"name": "ap_hi",
                 "type": "int"},
                {"name": "ap_lo",
                 "type": "int"},
                {"name": "cholesterol",
                 "type": "int"},
                {"name": "gluc",
                 "type": "int"},
                {"name": "smoke",
                 "type": "int"},
                {"name": "alco",
                 "type": "string"},
                {"name": "active",
                 "type": "int"},
                {"name": "cardio",
                 "type": "int"}
            ],
            "parameters": {"JobId": {"type": "int"}, "OutputBlobNameList": {"type": "Array"}},  # funkar ej,
            "runDimensions": {"JobId": {"type": "Expression", "value": "@pipeline().parameters.JobId"}},  # funkar ej,
        }}

    dsIn_format = TextFormat(column_delimiter=",", row_delimiter="\n",
                             quote_char="\"", null_value="\\N",
                             encoding_name="UTF-8", treat_empty_as_null=True,
                             skip_line_count=0, first_row_as_header=True)
    ds_azure_blob = DatasetResource(properties=AzureBlobDataset(linked_service_name=ds_ls, folder_path=blob_input_path,
                                                                file_name=blob_file_name,
                                                                format=dsIn_format))  # dsIn_format

    ds = adf_client.datasets.create_or_update(rg_name, df_name, ds_name, ds_azure_blob)

    print("Create an Azure blob dataset (input) for source:")
    print_item(ds)

    # Step 9: Create an Azure blob dataset (output) for sink
    ds_out_name = 'dsOut'
    output_blob_path = blob_container_name  # '<container>/<folder path>'

    dsOut_format = TextFormat(column_delimiter=",", row_delimiter="\n",
                              quote_char="\"", null_value="\\N",
                              encoding_name="UTF-8", treat_empty_as_null=True,
                              skip_line_count=0, first_row_as_header=True)
    ds_out_azure_blob = DatasetResource(properties=AzureBlobDataset(linked_service_name=ds_ls,
                                                                    folder_path=output_blob_path,
                                                                    file_name=blob_file_name,
                                                                    format=dsOut_format))  #

    ds_out = adf_client.datasets.create_or_update(rg_name, df_name, ds_out_name, ds_out_azure_blob)
    print("Create an Azure blob dataset (output) for sink:\n")
    print_item(ds_out)

    # Step 10: Creates a copy activity.
    # Create a copy activity
    act_name = 'copyBlobtoBlob'
    blob_source = BlobSource()
    blob_sink = BlobSink()
    dsInRef = DatasetReference(reference_name="cardioIn")
    dsOutRef = DatasetReference(reference_name="dsOut")

    copy_activity = CopyActivity(name=act_name, inputs=[dsInRef], outputs=[dsOutRef],
                                 source=blob_source, sink=blob_sink)

    # Step 11: Create a pipeline with the copy activity
    # https://learn.microsoft.com/en-us/rest/api/datafactory/pipelines/create-or-update?tabs=Python
    # https://learn.microsoft.com/en-us/azure/data-factory/format-delimited-text
    p_name = 'Pipeline'
    print("här startar create a pipeline with the copy activity\n")

    copy_params = {
        "properties": {
            "type": "TextFormat",
            "linkedServiceName": {
                "referenceName": "storageLinkedService001",
                "type": "LinkedServiceReference"
            },
            "activities": [{
                "name": "CopyActivity",
                "type": "Copy",
                "inputs": [
                    {
                        "referenceName": "cardioIn",  # "<source dataset name>"
                        "parameters": {
                            "path": "@pipeline().parameters.inputPath"},
                        "type": "DatasetReference"
                    }
                ],
                "outputs": [
                    {
                        "referenceName": "dsOut",  # "<sink dataset name>"
                        "parameters": {
                            "path": "@pipeline().parameters.outputPath"
                        },
                        "type": "DatasetReference"
                    }
                ],
                "userProperties": [
                    {
                        "name": "Source",
                        "value": "dataengexp-blob-container-01/cardio_train.csv"
                    },
                    {
                        "name": "Destination",
                        "value": "dataengexp-blob-container-01/cardio_train.csv"
                    }
                ],
                "typeProperties": {
                    "source": {
                        "type": "BlobSource",  # "<source type>",
                        "properties": {
                            "dataset": {
                                "referenceName": "cardioIn",
                                "type": "DatasetReference"
                            },
                        },  # <properties>
                    },
                    "sink": {
                        "type": "BlobSink",  # "<sink type>"
                        "properties": {
                                "dataset": {
                                    "referenceName": "cardioIn",
                                    "type": "DatasetReference"
                                },
                                "copyBehavior": "PreserveHierarchy", # <properties>
                            }}},
                    "translator": {
                        "type": "TabularTranslator",
                        "columnMappings": {
                            "id": "id",
                            "age": "age",
                            "gender": "gender",
                            "height": "height",
                            "weight": "weight",
                            "ap_hi": "ap_hi",
                            "ap_lo": "ap_lo",
                            "cholesterol": "cholesterol",
                            "gluc": "gluc",
                            "smoke": "smoke",
                            "alco": "alco",
                            "active": "active",
                            "cardio": "cardio"
                        }  # "<column mapping>"
                    },
                    "dataIntegrationUnits": 32,
                    "parallelCopies": 1,
                    "enableStaging": False,
                    "enableSkipIncompatibleRow": True,
                }]}}

    # sparas än så länge, ej använd
    params_for_pipeline = {
        "name": "CardioRawPipeline",
        "type": "JsonFormat",
        "properties": {
            "linkedServiceName": {
                "referenceName": "storageLinkedService001",
                "type": "LinkedServiceReference"
            },
            "activities": [{
                "typeProperties": {
                    "type": "Copy",
                    "copyBehavior": "PreserveHierarchy",
                    "translator": {
                        "type": "TabularTranslator",
                        "columnMappings": {
                            "id": "id",
                            "age": "age",
                            "gender": "gender",
                            "height": "height",
                            "weight": "weight",
                            "ap_hi": "ap_hi",
                            "ap_lo": "ap_lo",
                            "cholesterol": "cholesterol",
                            "gluc": "gluc",
                            "smoke": "smoke",
                            "alco": "alco",
                            "active": "active",
                            "cardio": "cardio"
                        }}
                }}],
            "schema": [
                {"name": "id",
                 "type": "int"},
                {"name": "age",
                 "type": "int"},
                {"name": "gender",
                 "type": "string"},
                {"name": "height",
                 "type": "string"},
                {"name": "ap_hi",
                 "type": "string"},
                {"name": "ap_lo",
                 "type": "string"},
                {"name": "cholesterol",
                 "type": "string"},
                {"name": "gluc",
                 "type": "string"},
                {"name": "smoke",
                 "type": "string"},
                {"name": "string",
                 "type": "string"},
                {"name": "active",
                 "type": "string"},
                {"name": "cardio",
                 "type": "string"}
            ]}}
    # saknar copy behavior :preserve hiarcy i creata copy activity
    create_copy_activity(copy_activity, adf_client, rg_name, df_name, p_name,
                         copy_params)  # params_for_pipeline , dataset_json - denna fungerar

    # Step 12: Create a pipeline run
    print("här startar Create a pipeline run\n")
    run_response = adf_client.pipelines.create_run(rg_name, df_name, p_name, parameters=copy_params) # dataset_json fungerar

    # Step 13: Monitor the pipeline run
    time.sleep(15)
    print("här startar Monitor the pipeline run\n")
    pipeline_run = adf_client.pipeline_runs.get(rg_name, df_name, run_response.run_id)
    print("\n\tPipeline run status: {}".format(pipeline_run.status))
    filter_params = RunFilterParameters(last_updated_after=datetime.now() - timedelta(1),
                                        last_updated_before=datetime.now() + timedelta(1))
    query_response = adf_client.activity_runs.query_by_pipeline_run(rg_name, df_name, pipeline_run.run_id,
                                                                    filter_params)
    print_activity_run_details(query_response.value[0])

    # STEP 9b: Create Data Flow
    """
    For instance, allowSchemaDrift: true, 
    in a source transformation tells the service to include all columns from the 
    source dataset in the data flow even if they aren't included in the schema projection.
    https://learn.microsoft.com/en-us/azure/data-factory/data-flow-script 
    https://learn.microsoft.com/en-us/rest/api/datafactory/data-flows/create-or-update?tabs=Python
    """
    data_flow2 = {
        "properties": {
            "description": "Sample demo data flow to convert currencies showing usage of union, "
                           "derive and conditional split transformation.",
            "type": "MappingDataFlow",
            "parameters": {
                "path": {
                    "type": "int"}},
            "typeProperties": {
                "sources": [
                    {
                        "dataset": {
                            "referenceName": "cardioIn",
                            "type": "DatasetReference"
                        },
                        "name": "cardioIn"
                    },
                    """{
                        "dataset": {
                            "referenceName": "dsOut",
                            "type": "DatasetReference"
                        },
                        "name": "dsOut"
                    }"""
                ],
                "sinks": [
                    {
                        "dataset": {
                            "referenceName": "cardioIn",
                            "type": "DatasetReference"
                        },
                        "name": "cardioInSink"
                    },
                    """{
                        "dataset": {
                            "referenceName": "dsOut",
                            "type": "DatasetReference"
                        },
                        "name": "dsOutSink"
                    }"""
                ], "scriptLines": [
                    "source(output(",
                    "id as int,",
                    "age as int,",
                    "gender as int,",
                    "height as int,",
                    "ap_hi as int,",
                    "ap_lo as int,",
                    "cholesterol as int,",
                    "gluc as int,",
                    "smoke as int,",
                    "alco as int,",
                    "active as int,",
                    "cardio as int", ")",
                    "allowSchemaDrift:True,",
                    "validateSchema: False) ~> dsIn",
                    "source(output(",
                    "id as int,",
                    "age as int,",
                    "gender as int,",
                    "height as int,",
                    "ap_hi as int,",
                    "ap_lo as int,",
                    "cholesterol as int,",
                    "gluc as int,",
                    "smoke as int,",
                    "alco as int,",
                    "active as int,",
                    "cardio as int", ")",
                    "allowSchemaDrift:True,",
                    "validateSchema: False) ~> dsOut"
                ]
            }
        }
    }
    # https://github.com/davedoesdemos/PreviewMappingDataFlow/blob/master/code/dataflow.json
    """
    {
    "name": "dataflow1",
    "properties": {
        "type": "MappingDataFlow",
        "typeProperties": {
            "sources": [
                {
                    "dataset": {
                        "referenceName": "sourcecsv",
                        "type": "DatasetReference"
                    },
                    "name": "source1",
                    "script": "source(output(\n\t\tid as string,\n\t\tfirst_name as string,\n\t\tlast_name as string,\n\t\temail as string,\n\t\tgender as string,\n\t\tip_address as string,\n\t\tcompany as string\n\t),\n\tallowSchemaDrift: true,\n\tvalidateSchema: false) ~> source1"
                }
            ],
            "sinks": [
                {
                    "dataset": {
                        "referenceName": "sinkconsolidated",
                        "type": "DatasetReference"
                    },
                    "name": "sinkconsolidated",
                    "script": "source1 sink(allowSchemaDrift: true,\n\tvalidateSchema: false,\n\tpartitionFileNames:['consolidated.csv'],\n\tpartitionBy('hash', 1),\n\ttruncate:true) ~> sinkconsolidated"
                },
                {
                    "dataset": {
                        "referenceName": "sinkaggr",
                        "type": "DatasetReference"
                    },
                    "name": "sinkaggr",
                    "script": "Aggregate1 sink(allowSchemaDrift: true,\n\tvalidateSchema: false,\n\tpartitionFileNames:['aggr.csv'],\n\tpartitionBy('hash', 1),\n\ttruncate:true) ~> sinkaggr"
                },
                {
                    "dataset": {
                        "referenceName": "sinkfemale",
                        "type": "DatasetReference"
                    },
                    "name": "sinkfemale",
                    "script": "ConditionalSplit1@Female sink(allowSchemaDrift: true,\n\tvalidateSchema: false,\n\tpartitionFileNames:['female.csv'],\n\tpartitionBy('hash', 1),\n\ttruncate:true) ~> sinkfemale"
                },
                {
                    "dataset": {
                        "referenceName": "sinkother",
                        "type": "DatasetReference"
                    },
                    "name": "sinkother",
                    "script": "ConditionalSplit1@Other sink(allowSchemaDrift: true,\n\tvalidateSchema: false,\n\tpartitionFileNames:['other.csv'],\n\tpartitionBy('hash', 1),\n\ttruncate:true) ~> sinkother"
                }
            ],
            "transformations": [
                {
                    "name": "Aggregate1",
                    "script": "source1 aggregate(groupBy(first_name),\n\tfirstnamecount = count(first_name)) ~> Aggregate1"
                },
                {
                    "name": "ConditionalSplit1",
                    "script": "source1 split(gender=='Female',\n\tdisjoint: false) ~> ConditionalSplit1@(Female, Other)"
                }
            ]
        }
    }
}
    """
    data_flow_cardio = {
        "name": "CardioDataFlow",
        "properties": {
            "description": "Sample demo data flow convert covidtrain.csv",
            "type": "MappingDataFlow",
            "formatSettings": {"type": "DelimitedTextReadSettings", "fileExtension": ".csv"},
            "typeProperties": {
                "sources": [
                    {
                        "dataset": {
                            "referenceName": "cardioIn",
                            "type": "DatasetReference"
                        },
                        "name": "cardioIn",
                        "script": "source(output(\n\t\tid as int,\n\t\tage as int,\n\t\tgender as int,\n\t\theight as int,\n\t\tap_hi as int,\n\t\tap_lo as int,\n\t\tcholesterol as int,\n\t\tgluc as int,\n\t\tsmoke as int,\n\t\talco as int,\n\t\tactive as int,\n\t\tcardio as int,\n\t\tallowSchemaDrift: true,\n\t\tvalidateSchema: false) ~> cardioIn"
                    }
                ],
                "sinks": [
                    {
                        "dataset": {
                            "referenceName": "dsOut",
                            "type": "DatasetReference"
                        },
                        "name": "dsOut",
                        "script": "cardioIn sink(allowSchemaDrift: true,\n\tvalidateSchema: false,\n\tpartitionFileNames:['dsout.csv'],\n\tpartitionBy('hash', 1),\n\ttruncate:true) ~> sinkconsolidated"
                    }],
            }}}

    """
     "scriptLines1": [
                    "source(output(",
                    "id as int,",
                    "age as int,",
                    "gender as int,",
                    "height as int,",
                    "ap_hi as int,",
                    "ap_lo as int,",
                    "cholesterol as int,",
                    "gluc as int,",
                    "smoke as int,",
                    "alco as int,",
                    "active as int,",
                    "cardio as int", ")",
                    "allowSchemaDrift:True,",
                    "validateSchema: False) ~> cardioIn",
                    "source(output(",
                    "id as int,",
                    "age as int,",
                    "gender as int,",
                    "height as int,",
                    "ap_hi as int,",
                    "ap_lo as int,",
                    "cholesterol as int,",
                    "gluc as int,",
                    "smoke as int,",
                    "alco as int,",
                    "active as int,",
                    "cardio as int", ")",
                    "allowSchemaDrift:True,",
                    "validateSchema: False) ~> dsOut"
                ],
    "sinks": [{"dataset": {"referenceName": "cardioIn", "type": "DatasetReference"}, "name": "CARDIOSink"},
                {"dataset": {"referenceName": "dsOut", "type": "DatasetReference"}, "name": "cardioInSink"}],

    sources": [{"dataset": {"referenceName": "cardioIn", "type": "DatasetReference"}, "name": "dsIn"},
                {"dataset": {"referenceName": "dsOut", "type": "DatasetReference"}, "name": "dsOut"}]

                    
    linkedservice, dataset saknas för sink, incoming stream saknas
    på set properties ska datatypen in som i python id-int, sex- varchar etc
        dsInRef = DatasetReference(reference_name="cardioIn")
    dsOutRef = DatasetReference(reference_name="dsOut")
    
                    "linkedServiceName": {
                    "referenceName": "storageLinkedService001",
                    "type": "LinkedServiceReference"},"""

    print("här startar Create data flow\n")
    create_data_flow(adf_client, rg_name, df_name, "cardio_data_flow", data_flow_cardio)  # data_flow_cardio data_flow2
    # HÄR ÄR JAG NU - kanske hoopa över och ta direkt till Synapse, isåfall skapa synapse resurse och keyvalut etc

    # Specify your Active Directory client ID, client secret, and tenant ID
    """credentials = ClientSecretCredential(client_id=os.environ.get('AZURE_CLIENT_ID'),
                                         client_secret=os.environ.get('AZURE_CLIENT_SECRET'),
                                         tenant_id=os.environ.get('AZURE_TENANT_ID'))  # ClientSecretCredential
    """
    # test
    # Obtain the management object for resources.
    # resource_client = ResourceManagementClient(credential, subscription_id)


# Start the main method
if __name__ == "__main__":
    main()
