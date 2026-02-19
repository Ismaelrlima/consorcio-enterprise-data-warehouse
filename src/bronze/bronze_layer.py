import json
import uuid
from datetime import datetime

def camada_bronze(raw_data: dict, source: str = 'microwork_api') -> dict:
    uuid_id = str(uuid.uuid4())
    
    bronze_record = {
        'uuid': uuid_id,
        'source': source,
        'ingestion_timestamp' : datetime.utcnow().isoformat(),
        'payload': raw_data
    }

    return bronze_record