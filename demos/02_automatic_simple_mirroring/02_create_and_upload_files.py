import os
import json
import pandas as pd
from urllib.parse import urlparse
from sqlalchemy import create_engine, text
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
import datetime


tables = [
    "Card",
    "CardAccount",
    "CardType",
    "Currency",
    "Customer",
    "Merchant",
    "MerchantCategory",
    "TransactionStatus",
    "TransactionType",
    "Transactions",
    "Payments"
]
config = json.load(open("00_config.json"))

client_credential = ClientSecretCredential(
    tenant_id=config["tenant_id"],
    client_id=config["client_id"],
    client_secret=config["client_secret"]
)

onelake_url_parts = urlparse(config["onelake_landing_zone"])
onelake_url_segments = onelake_url_parts.path.strip("/").split("/")
onelake_account_url = f"{onelake_url_parts.scheme}://{onelake_url_parts.netloc}"
onelake_filesystem = onelake_url_segments[0]
onelake_landingzone_path = "/".join(onelake_url_segments[1:])

onelake_service_client = DataLakeServiceClient(account_url=onelake_account_url, credential=client_credential)
onelake_filesystem = onelake_service_client.get_file_system_client(file_system=onelake_filesystem)

sql_source_engine = create_engine(f"mssql+pyodbc://{config["sql_user"]}:{config["sql_password"]}@{config["sql_server"]},{config["sql_port"]}/{config["sql_catalog_source"]}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes")


with sql_source_engine.connect() as source_conn:
    for t in tables:
        
        # PREPARE folder
        folder = f"../../results/02_automatic_simple_mirroring/{t}"
        os.makedirs(folder, exist_ok=True)

        # PREPARE OneLake
        table_path = f"{onelake_landingzone_path}/{t}"
        onelake_table_directory = onelake_filesystem.get_directory_client(table_path)
        if not onelake_table_directory.exists():
            onelake_filesystem.create_directory(table_path)



        #
        #   _METADATA.JSON
        #
        metadata_file = "_metadata.json"
        metadata_full_path = os.path.join(folder, metadata_file)
        if not os.path.isfile(metadata_full_path):
            
            # QUERY Unique keys
            keycolumns_query = f"SELECT [keyColumns] = c.[name] FROM [sys].[tables] AS t JOIN [sys].[indexes] AS i ON [t].[object_id] = [i].[object_id] JOIN [sys].[index_columns] AS ic ON [i].[object_id] = [ic].[object_id] AND [i].[index_id] = [ic].[index_id] JOIN [sys].[columns] AS c ON [ic].[object_id] = [c].[object_id] AND [ic].[column_id] = [c].[column_id] WHERE [t].[name] = '{t}' AND [i].[is_primary_key] = 1"
            keycolumns_df = pd.read_sql(keycolumns_query, source_conn)
            metadata_content = {"keyColumns": keycolumns_df["keyColumns"].tolist()}
            
            # SAVE json file to local
            with open(metadata_full_path, "w") as f:
                json.dump(metadata_content, f)

            # UPLOAD _metadata.json if it does not exists
            file_client = onelake_table_directory.get_file_client(metadata_file)
            if not file_client.exists():
                with open(metadata_full_path, "rb") as data:
                    file_client.upload_data(data, overwrite=True)

            
        
        
        ##
        ##  _CURRENT_TIMESTAMP.JSON > get current value
        ##
        # Check if _current_timestamp.json exists and read its value
        current_timestamp = None
        current_timestamp_json_file = "_current_timestamp.json"
        current_timestamp_json_path = os.path.join(folder, current_timestamp_json_file)
        if os.path.isfile(current_timestamp_json_path):
            # The file exists and we get the value
            with open(current_timestamp_json_path, "r") as f:
                ts_data = json.load(f)
                current_timestamp = ts_data["current_timestamp"]




        ##
        ##  GET NEW TIMESTAMP, but no save it. It will be saved at the end
        ##
        # GET the columns from the table
        columns_query = f"SELECT [COLUMN_NAME] FROM [INFORMATION_SCHEMA].[COLUMNS] WHERE [TABLE_NAME] = '{t}'"
        columns_df = pd.read_sql(columns_query, source_conn)
        columns = columns_df["COLUMN_NAME"].tolist()


        
        
        # GET max value of CreatedOn and/or ModifiedOn
        new_timestamp = None
        timestamp_column_used = None
        there_is_modified_on = False
        max_datetime = "MAX([CreatedOn])"
        if "ModifiedOn" in columns:
            max_datetime = f"CASE WHEN MAX([ModifiedOn]) IS NULL THEN MAX([CreatedOn]) WHEN MAX([ModifiedOn]) > MAX([CreatedOn]) THEN MAX([ModifiedOn]) ELSE MAX([CreatedOn]) END"
            there_is_modified_on = True

        max_query = f"SELECT FORMAT({max_datetime}, 'yyyy-MM-ddTHH:mm:ss.fffffff') AS [new_timestamp] FROM [dbo].[{t}]"
        max_df = pd.read_sql(max_query, source_conn)
        new_timestamp = max_df["new_timestamp"].iloc[0]
        

        


        ##
        ## GET next sequence file
        ##
        # Find the next sequence number for parquet files
        existing_files = [f for f in os.listdir(folder) if f.endswith('.parquet')]
        if existing_files:
            seq_nums = [ int(f.split('.')[0]) for f in existing_files ]
            max_seq = max(seq_nums) if seq_nums else 0
        else:
            max_seq = 0
        next_seq_num = max_seq + 1
        parquet_file = f"{str(next_seq_num).zfill(20)}.parquet"



        ##
        ##  GET data
        ##
        # PREPARE the SQL Query
        row_marker = "0"
        where = ""
        if there_is_modified_on:
            row_marker = "IIF([ModifiedOn] IS NULL, 0, 1)"
        if current_timestamp and there_is_modified_on:
            where = f" WHERE [CreatedOn] > '{current_timestamp}' OR [ModifiedOn] > '{current_timestamp}'"
        elif current_timestamp:
            where = f" WHERE [CreatedOn] > '{current_timestamp}'"
        data_query = f"SELECT *, [__rowMarker__]={row_marker} FROM [dbo].[{t}] {where}"
        data_df = pd.read_sql(data_query, source_conn)
        if data_df.shape[0] > 0:
            # SAVE parquet file to local
            parque_full_path = os.path.join(folder, parquet_file)
            data_df.to_parquet(parque_full_path, engine="pyarrow", index=False)
            # UPLOAD .parquet file
            with open(parque_full_path, "rb") as data:
                file_client = onelake_table_directory.get_file_client(parquet_file)
                file_client.upload_data(data, overwrite=True)

            print (f"{t} >> {parquet_file} >> {new_timestamp}")

            ##
            ##  UPDATE _current_timestamp.json 
            ##
            timestamp_json_file = "_current_timestamp.json"
            timestamp_json_path = os.path.join(folder, timestamp_json_file)
            with open(timestamp_json_path, "w") as f:
                json.dump({"current_timestamp": str(new_timestamp)}, f)


            