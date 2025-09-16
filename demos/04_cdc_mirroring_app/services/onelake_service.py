import json
import io
import os
from urllib.parse import urlparse
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from config import Config




onelake_service_client = None
onelake_filesystem = None
onelake_landingzone_path = None


client_credential = ClientSecretCredential(
    tenant_id=Config.ONELAKE_TENANT_ID,
    client_id=Config.ONELAKE_CLIENT_ID,
    client_secret=Config.ONELAKE_CLIENT_SECRET
)

onelake_url_parts = urlparse(Config.ONELAKE_LANDING_ZONE)
onelake_url_segments = onelake_url_parts.path.strip("/").split("/")
onelake_account_url = f"{onelake_url_parts.scheme}://{onelake_url_parts.netloc}"
onelake_filesystem = onelake_url_segments[0]

onelake_landingzone_path = "/".join(onelake_url_segments[1:])
onelake_service_client = DataLakeServiceClient(account_url=onelake_account_url, credential=client_credential)
onelake_filesystem = onelake_service_client.get_file_system_client(file_system=onelake_filesystem)




def onelake_check_table_folder (schema, table):
    # PREPARE OneLake
    onelake_table_path = f"{onelake_landingzone_path}/{table}"
    if schema != "dbo":
        onelake_table_path = f"{onelake_landingzone_path}/{schema}.schema/{table}"
    
    onelake_table_directory_client = onelake_filesystem.get_directory_client(onelake_table_path)
    if not onelake_table_directory_client.exists():
        onelake_filesystem.create_directory(onelake_table_path)


def onelake_delete_table_folder (schema, table):
    # PREPARE OneLake
    onelake_table_path = f"{onelake_landingzone_path}/{table}"
    if schema != "dbo":
        onelake_table_path = f"{onelake_landingzone_path}/{schema}.schema/{table}"
    
    onelake_table_directory_client = onelake_filesystem.get_directory_client(onelake_table_path)
    if onelake_table_directory_client.exists():
        print("try to delete")
        print(dir(onelake_table_directory_client))
        onelake_table_directory_client.delete_directory()   




def onelake_check_and_upload_metadata_file (schema, table, unique_key):
    onelake_table_path = f"{onelake_landingzone_path}/{table}"
    if schema != "dbo":
        onelake_table_path = f"{onelake_landingzone_path}/{schema}.schema/{table}"
    onelake_table_directory_client = onelake_filesystem.get_directory_client(onelake_table_path)
    

    metadata_file = "_metadata.json"
    metadata_file_client = onelake_table_directory_client.get_file_client(metadata_file)

    if not metadata_file_client.exists():
        # Ensure the metadata is stored as a JSON file
        metadata_content = '{"KeyColumns": ' + unique_key + '}'
        data = io.BytesIO(metadata_content.encode("utf-8"))
        metadata_file_client.upload_data(data, overwrite=True)




def onelake_upload_parquet_file (schema, table, local_temporal_file, incremental_number):
        onelake_table_path = f"{onelake_landingzone_path}/{table}"
        if schema != "dbo":
            onelake_table_path = f"{onelake_landingzone_path}/{schema}.schema/{table}"
        onelake_table_directory_client = onelake_filesystem.get_directory_client(onelake_table_path)

        parquet_file = f"{incremental_number:020d}" + ".parquet"

        with open(local_temporal_file, "rb") as data:
            file_client = onelake_table_directory_client.get_file_client(parquet_file)
            file_client.upload_data(data, overwrite=True)

            return parquet_file


