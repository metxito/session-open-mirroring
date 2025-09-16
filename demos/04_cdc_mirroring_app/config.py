import json
import os

class Config:
    DEBUG = False
    TESTING = False
    CONFIG_PATH = os.path.join(os.path.dirname(__file__), "00_config.json")
    _config = {}

    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            _config = json.load(f)
    except Exception as e:
        _config = {}

    DATABASE_CONTROL_URL = _config.get("database_control_url", "")
    DATABASE_SOURCE_URL = _config.get("database_source_url", "")

    ONELAKE_TENANT_ID = _config.get("onelake_tenant_id", "")
    ONELAKE_CLIENT_NAME = _config.get("onelake_client_name", "")
    ONELAKE_CLIENT_ID = _config.get("onelake_client_id", "")
    ONELAKE_CLIENT_SECRET = _config.get("onelake_client_secret", "")
    ONELAKE_LANDING_ZONE = _config.get("onelake_landing_zone", "")
