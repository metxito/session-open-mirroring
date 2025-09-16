import os
import json
import pandas as pd
from urllib.parse import urlparse
from sqlalchemy import create_engine
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient


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


sql_engine = create_engine(f"mssql+pyodbc://{config["sql_user"]}:{config["sql_password"]}@{config["sql_server"]},{config["sql_port"]}/{config["sql_catalog_source"]}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes")

with sql_engine.connect() as conn:
    for t in tables:

        # PREPARE folder
        temporal_folder = f"../../results/02_automatic_simple_mirroring/{t}"
        os.makedirs(temporal_folder, exist_ok=True)

        ##
        ##  Find the next parquet sequence number
        ##
        existing_files = [f for f in os.listdir(temporal_folder) if f.endswith('.parquet')]
        if existing_files:
            seq_nums = [
                int(f.replace('.parquet', ''))
                for f in existing_files
                if f.replace('.parquet', '').isdigit()
            ]
            next_seq = max(seq_nums) + 1 if seq_nums else 1
        else:
            next_seq = 1
        parquet_file = f"{next_seq:020d}.parquet"



        ##
        ##  Read current timestamp from _current_timestamp.json
        ##
        current_timestamp_path = os.path.join(temporal_folder, "_current_timestamp.json")
        with open(current_timestamp_path, "r") as ts_file:
            timestamp = json.load(ts_file)["current_timestamp"]

        

        ##
        ##  Get new timestamp from database
        ##
        max_createdon_query = f"SELECT MAX([CreatedOn]) AS max_createdon FROM [dbo].[{t}]"
        max_createdon_df = pd.read_sql(max_createdon_query, conn)
        new_timestamp = max_createdon_df["max_createdon"].iloc[0]

        ##          DATA
        ##
        data_query = f"SELECT *, [__rowMarker__]=0 FROM [dbo].[{t}] WHERE [CreatedOn] > '{timestamp}'"
        data_df = pd.read_sql(data_query, conn)
        
        
        if data_df.empty:
            print(f"{t.ljust(40)} no changes")
        else:
            parquet_full_path = os.path.join(temporal_folder, parquet_file)
            data_df.to_parquet(parquet_full_path, engine="pyarrow", index=False)



            table_path = f"{onelake_landingzone_path}/{t}"
            onelake_table_directory = onelake_filesystem.get_directory_client(table_path)


            if not onelake_table_directory.exists():
                onelake_filesystem.create_directory(table_path)


            #with open(parquet_full_path, "rb") as data:
            #    file_client = onelake_table_directory.get_file_client(parquet_file)
            #    file_client.upload_data(data, overwrite=True)


            
            # Save new _current_timestamp.json
            if pd.notnull(new_timestamp):
                formatted_timestamp = new_timestamp.strftime("%Y-%m-%dT%H:%M:%S.0000000")
                with open(current_timestamp_path, "w") as ts_file:
                    json.dump({"current_timestamp": formatted_timestamp}, ts_file)

            print (f"{t.ljust(40)} >> {parquet_file} >> {new_timestamp}")