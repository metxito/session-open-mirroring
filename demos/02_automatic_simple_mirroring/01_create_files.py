import os
import json
import pandas as pd
from urllib.parse import urlparse
from sqlalchemy import create_engine
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient


tables = ["CardType"]
config = json.load(open("00_config.json"))

sql_source_engine = create_engine(f"mssql+pyodbc://{config["sql_user"]}:{config["sql_password"]}@{config["sql_server"]},{config["sql_port"]}/{config["sql_catalog_source"]}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes")
with sql_source_engine.connect() as conn:
    for t in tables:



        # PREPARE folder
        folder = f"../../results/02_automatic_simple_mirroring/{t}"
        os.makedirs(folder, exist_ok=True)



        # QUERY Unique keys
        keycolumns_query = f"SELECT [keyColumns] = c.[name] FROM [sys].[tables] AS t JOIN [sys].[indexes] AS i ON [t].[object_id] = [i].[object_id] JOIN [sys].[index_columns] AS ic ON [i].[object_id] = [ic].[object_id] AND [i].[index_id] = [ic].[index_id] JOIN [sys].[columns] AS c ON [ic].[object_id] = [c].[object_id] AND [ic].[column_id] = [c].[column_id] WHERE [t].[name] = '{t}' AND [i].[is_primary_key] = 1"
        keycolumns_df = pd.read_sql(keycolumns_query, conn)
        json_data = {"keyColumns": keycolumns_df["keyColumns"].tolist()}


        # SAVE json file to local
        json_file = "_metadata.json"
        json_full_path = os.path.join(folder, json_file)
        with open(json_full_path, "w") as f:
            json.dump(json_data, f)





        # GET the columns from the table
        columns_query = f"SELECT [COLUMN_NAME] FROM [INFORMATION_SCHEMA].[COLUMNS] WHERE [TABLE_NAME] = '{t}'"
        columns_df = pd.read_sql(columns_query, conn)
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
        max_df = pd.read_sql(max_query, conn)
        new_timestamp = max_df["new_timestamp"].iloc[0]

        # SAVE to _current_timestamp.json 
        timestamp_json_file = "_current_timestamp.json"
        timestamp_json_path = os.path.join(folder, timestamp_json_file)
        with open(timestamp_json_path, "w") as f:
            json.dump({"current_timestamp": new_timestamp}, f)





        # QUERY data
        #,[__rowMarker__]=0  is not mandatory in the first insert. Actually is not recommended
        data_query = f"SELECT * FROM [dbo].[{t}]"
        data_df = pd.read_sql(data_query, conn)


        # SAVE parquet file to local
        parquet_file = f"00000000000000000001.parquet"
        parque_full_path = os.path.join(folder, parquet_file)
        data_df.to_parquet(parque_full_path, engine="pyarrow", index=False)



        print (f"Done")