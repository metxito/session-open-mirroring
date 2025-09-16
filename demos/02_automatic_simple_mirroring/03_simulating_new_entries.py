import json
import time
from sqlalchemy import create_engine, text


config = json.load(open("00_config.json"))
start_time = time.time()

while time.time() - start_time < 3600:
    sql_source_engine = create_engine(f"mssql+pyodbc://{config["sql_user"]}:{config["sql_password"]}@{config["sql_server"]},{config["sql_port"]}/{config["sql_catalog_source"]}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes")
    with sql_source_engine.connect() as source_conn:
        trans = source_conn.begin()
        try:
            #source_conn.execute(text("EXEC [dbo].[usp_insert_new_transaction]"))
            source_conn.execute(text("EXEC [dbo].[usp_insert_range_transaction] @days=1"))
            trans.commit()
        except Exception as e:
            trans.rollback()
            print(f"Error occurred: {e}")

        result = source_conn.execute(text("SELECT max([CreatedOn]) FROM [dbo].[Transactions]"))
        max_created_on = result.scalar()
        print(max_created_on)
    
    
    time.sleep(3)
