import json
import hashlib
import uuid
from datetime import datetime
from loguru import logger

def generate_id(data: dict):
    json_data = json.dumps(data, sort_keys=True)
    md5_hash = hashlib.md5(json_data.encode('utf-8')).hexdigest()
    return md5_hash

def generate_uuid(data: dict):
    json_data = json.dumps(data, sort_keys=True)
    namespace = uuid.NAMESPACE_DNS
    return str(uuid.uuid5(namespace, json_data))

def log_align(level, label, message):
    getattr(logger, level)(f"{label:<22}: {message}")

def generate_index_ingest_agent(message):
    date_format = "%Y-%m-%d %H:%M:%S"
    created_at = datetime.strptime(message.get('created_at', ''), date_format)

    year = created_at.strftime('%Y')
    year_month = created_at.strftime('%Y%m')
    year_month_day = created_at.strftime('%Y%m%d')
    return f"index-{year_month_day}"
