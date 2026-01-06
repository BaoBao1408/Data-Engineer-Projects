import json
import os
from datetime import datetime

def log_failed_id(product_id, batch_index, stage, error):
    os.makedirs("errors", exist_ok=True)

    record = {
        "product_id": product_id,
        "batch": batch_index,
        "stage": stage,
        "error": str(error),
        "time": datetime.now().isoformat()
    }

    path = f"errors/failed_batch_{batch_index}.json"

    data = []
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

    data.append(record)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
