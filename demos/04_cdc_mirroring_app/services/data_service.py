import sqlalchemy as sa
from sqlalchemy import text
from config import Config

# Database connection config
engine_control = sa.create_engine(Config.DATABASE_CONTROL_URL)
engine_source = sa.create_engine(Config.DATABASE_SOURCE_URL)


def get_query(id):
    with engine_control.begin() as conn:
        result = conn.execute(text("SELECT * FROM [control].[v_queries] WHERE [id] = :id"), {"id": id})
        row = result.fetchone()
        if row is None:
            return None
        row_dict = dict(row._mapping)
        if "current_lsn" in row_dict and isinstance(row_dict["current_lsn"], (bytes, bytearray)):
            row_dict["current_lsn"] = "0x" + row_dict["current_lsn"].hex()
        return row_dict
    
def get_queries():
    with engine_control.begin() as conn:
        result = conn.execute(text("SELECT * FROM [control].[v_queries]"))
        rows = []
        for row in result:
            row_dict = dict(row._mapping)
            if "current_lsn" in row_dict and isinstance(row_dict["current_lsn"], (bytes, bytearray)):
                row_dict["current_lsn"] = "0x" + row_dict["current_lsn"].hex()
            rows.append(row_dict)
        return rows

def get_active_queries():
    with engine_control.begin() as conn:
        result = conn.execute(text("SELECT * FROM [control].[v_queries] WHERE [active]=1"))
        rows = []
        for row in result:
            row_dict = dict(row._mapping)
            if "current_lsn" in row_dict and isinstance(row_dict["current_lsn"], (bytes, bytearray)):
                row_dict["current_lsn"] = "0x" + row_dict["current_lsn"].hex()
            rows.append(row_dict)
        return rows


def get_connection_names():
    with engine_control.begin() as conn:
        result = conn.execute(text("SELECT DISTINCT connection_name FROM [source].[sources]"))
        return [row[0] for row in result]

def get_sources():
    with engine_control.begin() as conn:
        result = conn.execute(text("SELECT * FROM [source].[v_sources]"))
        return [dict(row._mapping) for row in result]

def get_source_columns(id):
    with engine_control.begin() as conn:
        result = conn.execute(text("SELECT c.* FROM [source].[sources] AS src JOIN [source].[columns] AS c ON src.[connection_name]=c.[connection_name] AND src.[schema]=c.[schema] AND src.[table]=c.[table] WHERE src.[id] = :id"), {"id": id})
        return [dict(row._mapping) for row in result]

def run_simulation_procedure():
    with engine_source.begin() as conn:
        result = conn.execute(text("EXEC [dbo].[usp_insert_range_transaction] @days=2"))
        row = result.fetchone()
        if row and ("MaxCreatedOn" in row._mapping):
            return row._mapping["MaxCreatedOn"]
        return None


def activate_query(id):
    with engine_control.begin() as conn:
        conn.execute(text("UPDATE [control].[queries_control] SET [active] = 1 WHERE [id] = :id"), {"id": id})

def deactivate_query(id):
    with engine_control.begin() as conn:
        conn.execute(text("UPDATE [control].[queries_control] SET [active] = 0 WHERE [id] = :id"), {"id": id})

def restart_query(id):
    with engine_control.begin() as conn:
        conn.execute(text("UPDATE [control].[queries_control] SET [next_file_sequence] = 1, [current_lsn] = 0x00000000000000000000 WHERE [id] = :id"), {"id": id})



# Update current_lsn in [control].[queries_control] for given connection_name and name
def update_current_lsn(connection_name, schema, table, new_lsn, increase_file):
    with engine_control.begin() as conn:
        conn.execute(
            text(f"""
                UPDATE [control].[queries_control]
                SET
                   [current_lsn] = {new_lsn},
                   [next_file_sequence] = [next_file_sequence] + :increase_file
                WHERE [connection_name] = :connection_name AND [schema]=:schema AND [table]=:table
            """),
            {
                "connection_name": connection_name,
                "schema": schema,
                "table": table,
                "increase_file": (1 if increase_file else 0)
            }
        )



