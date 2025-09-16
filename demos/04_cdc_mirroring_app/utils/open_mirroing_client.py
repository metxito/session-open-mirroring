from services.data_service import get_active_queries, update_current_lsn
import threading
import time
from services.parquet_service import DownloadDataIntoParquet
from services.onelake_service import onelake_check_table_folder, onelake_check_and_upload_metadata_file, onelake_upload_parquet_file


class OpenMirroringClient:
    call_state = {"running": False, "thread": None, "logs": [], "last_log_index": 0}

    @classmethod
    def start_call_loop(cls):
        if not cls.call_state["running"]:
            cls.call_state["running"] = True
            cls.call_state["thread"] = threading.Thread(target=cls.call_loop, daemon=True)
            cls.call_state["thread"].start()

    @classmethod
    def stop_call_loop(cls):
        cls.call_state["running"] = False
        if cls.call_state["thread"]:
            cls.call_state["thread"].join(timeout=1)
            cls.call_state["thread"] = None

    @classmethod
    def call_loop(cls):
        while cls.call_state["running"]:
            try:
                # 1. Get content of [control].[queries_control]
                queries = get_active_queries()
                files_to_upload = []

                for qc in queries:
                    r = DownloadDataIntoParquet(qc["schema"], qc["table"], qc["query_incremental"], qc["current_lsn"])
                    if r:
                        qc["new_lsn"] = r["new_lsn"]
                        qc["tmp_parquet_full_path"] = r["tmp_parquet_full_path"]
                        files_to_upload.append(qc)
                    
                for ftu in files_to_upload:

                    try:
                        onelake_check_table_folder (ftu["schema"], ftu["table"])
                        onelake_check_and_upload_metadata_file (ftu["schema"], ftu["table"], ftu["unique_keys"])
                        parquet_file = onelake_upload_parquet_file (ftu["schema"], ftu["table"], ftu["tmp_parquet_full_path"], ftu["next_file_sequence"])

                        update_current_lsn(ftu["connection_name"], ftu["schema"], ftu["table"], ftu["new_lsn"], True)
                        log = {
                            "time": time.strftime('%H:%M:%S'),
                            "schema": ftu["schema"],
                            "table": ftu["table"],
                            "lsn": ftu["new_lsn"],
                            "message": f"{parquet_file} uploaded"
                        }
                        cls.call_state["logs"].append(log)

                    except Exception as ex:
                        log = {
                            "time": time.strftime('%H:%M:%S'),
                            "schema": ftu["schema"],
                            "table": ftu["table"],
                            "message": str(ex)
                        }
                        cls.call_state["logs"].append(log)


            except Exception as e:
                log = {
                    "time": time.strftime('%H:%M:%S'),
                    "message": f"Call loop error: {e}"
                }
                cls.call_state["logs"].append(log)
            for _ in range(2):
                if not cls.call_state["running"]:
                    break
                time.sleep(5)

    @classmethod
    def get_call_loop_status(cls):
        running = cls.call_state["running"]
        logs = cls.call_state["logs"]
        last_index = cls.call_state.get("last_log_index", 0)
        new_logs = logs[last_index:]
        cls.call_state["last_log_index"] = len(logs)
        return {"running": running, "new_logs": new_logs}

    @classmethod
    def reset_call_loop_logs(cls):
        cls.call_state["logs"] = []
        cls.call_state["last_log_index"] = 0