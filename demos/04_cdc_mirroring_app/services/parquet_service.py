import pandas as pd
import uuid
import sqlalchemy as sa
from sqlalchemy import text
from config import Config
import tempfile
import os


engine_source = sa.create_engine(Config.DATABASE_SOURCE_URL)

def DownloadDataIntoParquet(schema, table, query, current_lsn):
    
    tmp_filename = str(uuid.uuid4()) + ".parquet"
    tmp_dir = tempfile.gettempdir()
    tmp_parquet_full_path = os.path.join(tmp_dir, tmp_filename)

    response = {"tmp_parquet_full_path": tmp_parquet_full_path, "new_lsn": None}

    if engine_source is None:
        raise RuntimeError("SOURCE_SQL_ENGINE is not initialized. Call StartConnection() first.")

    if current_lsn == None or current_lsn == "" or current_lsn == "0x00000000000000000000":
        with engine_source.begin() as conn:
            result = conn.execute(text(f"SELECT [min_lsn]=sys.fn_cdc_get_min_lsn('{schema}_{table}')"))
            row = result.fetchone()
            if row and ("min_lsn" in row._mapping):
                current_lsn = "0x" + row._mapping["min_lsn"].hex()


    with engine_source.begin() as conn:
        result = conn.execute(text("SELECT [new_lsn]=[sys].[fn_cdc_get_max_lsn]()"))
        row = result.fetchone()
        if row and ("new_lsn" in row._mapping):
            response["new_lsn"] = "0x" + row._mapping["new_lsn"].hex()


    if current_lsn == response["new_lsn"]:
        return None


    with engine_source.connect() as conn:
        # Replace wildcards in query string
        replace_query = query.replace("{{ current_lsn }}", current_lsn).replace("{{ new_lsn }}", response["new_lsn"])

        data_df = pd.read_sql(replace_query, conn)
        if data_df.empty:
            return None

        data_df.to_parquet(tmp_parquet_full_path, engine="pyarrow", index=False)
        return response

    return None

